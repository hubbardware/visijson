//! Formula nodes: spreadsheet-style expressions over JSON pointer references.
//!
//! Grammar:
//!   expr   := term (('+' | '-') term)*
//!   term   := factor (('*' | '/') factor)*
//!   factor := number | pointer | '-' factor | '(' expr ')' | ident '(' args ')'
//!   args   := expr (',' expr)*
//!
//! Pointers (`/a/b`) reference other document paths. Functions: sum, count,
//! min, max (each accepts scalars and arrays). A bare pointer expression
//! yields the referenced value verbatim (any JSON type).
//!
//! On every commit, formulas whose dependencies overlap the changed paths are
//! re-evaluated in topological order; cycles reject the commit.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use serde_json::{Number, Value};

use crate::error::{Result, StoreError};
use crate::model::{
    from_value, get_at, nodes_equal, set_at, to_plain_value, walk, FormulaNode, Node, Pointer,
};

#[derive(Debug, Clone)]
pub enum Expr {
    Num(f64),
    Ref(Pointer),
    Neg(Box<Expr>),
    Bin(BinOp, Box<Expr>, Box<Expr>),
    Call(String, Vec<Expr>),
}

#[derive(Debug, Clone, Copy)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
}

// ---------------------------------------------------------------- lexer

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Num(f64),
    Ptr(String),
    Ident(String),
    Plus,
    Minus,
    Star,
    Slash,
    LParen,
    RParen,
    Comma,
}

fn lex(expr: &str) -> std::result::Result<Vec<Tok>, String> {
    let mut toks = Vec::new();
    let chars: Vec<char> = expr.chars().collect();
    let mut i = 0;
    // `/` is division only after a value; otherwise it starts a pointer.
    let mut prev_is_value = false;
    while i < chars.len() {
        let c = chars[i];
        match c {
            ' ' | '\t' | '\n' | '\r' => i += 1,
            '+' => {
                toks.push(Tok::Plus);
                prev_is_value = false;
                i += 1;
            }
            '-' => {
                toks.push(Tok::Minus);
                prev_is_value = false;
                i += 1;
            }
            '*' => {
                toks.push(Tok::Star);
                prev_is_value = false;
                i += 1;
            }
            '(' => {
                toks.push(Tok::LParen);
                prev_is_value = false;
                i += 1;
            }
            ')' => {
                toks.push(Tok::RParen);
                prev_is_value = true;
                i += 1;
            }
            ',' => {
                toks.push(Tok::Comma);
                prev_is_value = false;
                i += 1;
            }
            '/' if prev_is_value => {
                toks.push(Tok::Slash);
                prev_is_value = false;
                i += 1;
            }
            '/' => {
                let start = i;
                while i < chars.len() && !" \t\n\r+-*(),".contains(chars[i]) {
                    i += 1;
                }
                let ptr: String = chars[start..i].iter().collect();
                if ptr == "/" {
                    return Err("bare `/` is not a valid pointer".into());
                }
                toks.push(Tok::Ptr(ptr));
                prev_is_value = true;
            }
            '0'..='9' | '.' => {
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    i += 1;
                }
                let s: String = chars[start..i].iter().collect();
                let n: f64 = s.parse().map_err(|_| format!("bad number `{s}`"))?;
                toks.push(Tok::Num(n));
                prev_is_value = true;
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                toks.push(Tok::Ident(chars[start..i].iter().collect()));
                prev_is_value = false;
            }
            c => return Err(format!("unexpected character `{c}`")),
        }
    }
    Ok(toks)
}

// ---------------------------------------------------------------- parser

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn next(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn expr(&mut self) -> std::result::Result<Expr, String> {
        let mut lhs = self.term()?;
        while let Some(op) = match self.peek() {
            Some(Tok::Plus) => Some(BinOp::Add),
            Some(Tok::Minus) => Some(BinOp::Sub),
            _ => None,
        } {
            self.next();
            let rhs = self.term()?;
            lhs = Expr::Bin(op, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn term(&mut self) -> std::result::Result<Expr, String> {
        let mut lhs = self.factor()?;
        while let Some(op) = match self.peek() {
            Some(Tok::Star) => Some(BinOp::Mul),
            Some(Tok::Slash) => Some(BinOp::Div),
            _ => None,
        } {
            self.next();
            let rhs = self.factor()?;
            lhs = Expr::Bin(op, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    fn factor(&mut self) -> std::result::Result<Expr, String> {
        match self.next() {
            Some(Tok::Num(n)) => Ok(Expr::Num(n)),
            Some(Tok::Ptr(p)) => {
                let ptr = Pointer::parse(&p).map_err(|e| e.to_string())?;
                Ok(Expr::Ref(ptr))
            }
            Some(Tok::Minus) => Ok(Expr::Neg(Box::new(self.factor()?))),
            Some(Tok::LParen) => {
                let e = self.expr()?;
                match self.next() {
                    Some(Tok::RParen) => Ok(e),
                    _ => Err("expected `)`".into()),
                }
            }
            Some(Tok::Ident(name)) => {
                match self.next() {
                    Some(Tok::LParen) => {}
                    _ => return Err(format!("expected `(` after `{name}`")),
                }
                let mut args = Vec::new();
                if self.peek() != Some(&Tok::RParen) {
                    loop {
                        args.push(self.expr()?);
                        match self.next() {
                            Some(Tok::Comma) => continue,
                            Some(Tok::RParen) => break,
                            _ => return Err("expected `,` or `)`".into()),
                        }
                    }
                } else {
                    self.next();
                }
                Ok(Expr::Call(name, args))
            }
            t => Err(format!("unexpected token {t:?}")),
        }
    }
}

pub fn parse(expr: &str) -> Result<Expr> {
    let err = |msg: String| StoreError::FormulaParse {
        expr: expr.to_string(),
        msg,
    };
    let toks = lex(expr).map_err(err)?;
    let mut p = Parser { toks, pos: 0 };
    let ast = p.expr().map_err(err)?;
    if p.pos != p.toks.len() {
        return Err(StoreError::FormulaParse {
            expr: expr.to_string(),
            msg: "trailing tokens".into(),
        });
    }
    Ok(ast)
}

pub fn dependencies(ast: &Expr) -> Vec<Pointer> {
    let mut deps = Vec::new();
    fn rec(e: &Expr, out: &mut Vec<Pointer>) {
        match e {
            Expr::Num(_) => {}
            Expr::Ref(p) => out.push(p.clone()),
            Expr::Neg(a) => rec(a, out),
            Expr::Bin(_, a, b) => {
                rec(a, out);
                rec(b, out);
            }
            Expr::Call(_, args) => {
                for a in args {
                    rec(a, out);
                }
            }
        }
    }
    rec(ast, &mut deps);
    deps
}

// ---------------------------------------------------------------- eval

fn eval(ast: &Expr, root: &Arc<Node>) -> std::result::Result<Value, String> {
    match ast {
        Expr::Num(n) => num_to_value(*n),
        Expr::Ref(p) => {
            let node = get_at(root, p).ok_or_else(|| format!("missing reference {p}"))?;
            Ok(to_plain_value(&node))
        }
        Expr::Neg(a) => num_to_value(-eval_num(a, root)?),
        Expr::Bin(op, a, b) => {
            let (x, y) = (eval_num(a, root)?, eval_num(b, root)?);
            let r = match op {
                BinOp::Add => x + y,
                BinOp::Sub => x - y,
                BinOp::Mul => x * y,
                BinOp::Div => {
                    if y == 0.0 {
                        return Err("division by zero".into());
                    }
                    x / y
                }
            };
            num_to_value(r)
        }
        Expr::Call(name, args) => {
            let mut nums = Vec::new();
            for a in args {
                collect_nums(a, root, &mut nums)?;
            }
            match name.as_str() {
                "sum" => num_to_value(nums.iter().sum()),
                "count" => Ok(Value::Number(Number::from(nums.len()))),
                "min" => nums
                    .iter()
                    .cloned()
                    .fold(None::<f64>, |m, x| Some(m.map_or(x, |m| m.min(x))))
                    .map_or(Err("min() of nothing".into()), num_to_value),
                "max" => nums
                    .iter()
                    .cloned()
                    .fold(None::<f64>, |m, x| Some(m.map_or(x, |m| m.max(x))))
                    .map_or(Err("max() of nothing".into()), num_to_value),
                other => Err(format!("unknown function `{other}`")),
            }
        }
    }
}

/// Flatten an argument to numbers: arrays contribute each numeric element.
fn collect_nums(e: &Expr, root: &Arc<Node>, out: &mut Vec<f64>) -> std::result::Result<(), String> {
    let v = eval(e, root)?;
    match v {
        Value::Array(items) => {
            for item in items {
                out.push(value_num(&item)?);
            }
            Ok(())
        }
        other => {
            out.push(value_num(&other)?);
            Ok(())
        }
    }
}

fn eval_num(e: &Expr, root: &Arc<Node>) -> std::result::Result<f64, String> {
    value_num(&eval(e, root)?)
}

fn value_num(v: &Value) -> std::result::Result<f64, String> {
    match v {
        Value::Number(n) => n.as_f64().ok_or_else(|| "number out of range".into()),
        Value::String(s) if s.starts_with("#ERR") => Err(s.clone()),
        other => Err(format!("not a number: {other}")),
    }
}

fn num_to_value(n: f64) -> std::result::Result<Value, String> {
    if !n.is_finite() {
        return Err("result is not finite".into());
    }
    // Keep whole results as integers so 1 + 2 == 3, not 3.0.
    if n.fract() == 0.0 && n.abs() < i64::MAX as f64 {
        Ok(Value::Number(Number::from(n as i64)))
    } else {
        Number::from_f64(n)
            .map(Value::Number)
            .ok_or_else(|| "result is not representable".into())
    }
}

// ------------------------------------------------- commit-time re-evaluation

struct FormulaEntry {
    path: Pointer,
    expr: String,
    ast: Expr,
    deps: Vec<Pointer>,
    cached: Arc<Node>,
}

fn collect(root: &Arc<Node>) -> Result<Vec<FormulaEntry>> {
    let mut found: Vec<(Pointer, String, Arc<Node>)> = Vec::new();
    walk(root, |path, node| {
        if let Node::Formula(f) = &**node {
            found.push((path.clone(), f.expr.clone(), f.cached.clone()));
        }
    });
    let mut entries = Vec::with_capacity(found.len());
    for (path, expr, cached) in found {
        let ast = parse(&expr)?;
        let deps = dependencies(&ast);
        entries.push(FormulaEntry {
            path,
            expr,
            ast,
            deps,
            cached,
        });
    }
    Ok(entries)
}

/// Re-evaluate formulas affected by `changed` paths. Returns the new root and
/// the set of formula paths whose value actually changed. Errors with
/// `FormulaCycle` if the dependency graph is cyclic.
pub fn reevaluate(
    root: &Arc<Node>,
    changed: &BTreeSet<Pointer>,
) -> Result<(Arc<Node>, BTreeSet<Pointer>)> {
    let entries = collect(root)?;
    if entries.is_empty() {
        return Ok((root.clone(), BTreeSet::new()));
    }
    let n = entries.len();

    // edges[i] = indices i depends on
    let mut edges: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut rev: Vec<Vec<usize>> = vec![Vec::new(); n];
    for i in 0..n {
        for j in 0..n {
            if entries[i].deps.iter().any(|d| d.overlaps(&entries[j].path)) {
                if i == j {
                    return Err(StoreError::FormulaCycle(entries[i].path.to_string()));
                }
                edges[i].push(j);
                rev[j].push(i);
            }
        }
    }

    // Cycle detection + topological order (DFS, deps first).
    let mut state = vec![0u8; n]; // 0 unvisited, 1 in progress, 2 done
    let mut order: Vec<usize> = Vec::with_capacity(n);
    fn dfs(
        i: usize,
        edges: &[Vec<usize>],
        state: &mut [u8],
        order: &mut Vec<usize>,
        entries: &[FormulaEntry],
    ) -> Result<()> {
        state[i] = 1;
        for &j in &edges[i] {
            match state[j] {
                0 => dfs(j, edges, state, order, entries)?,
                1 => return Err(StoreError::FormulaCycle(entries[j].path.to_string())),
                _ => {}
            }
        }
        state[i] = 2;
        order.push(i);
        Ok(())
    }
    for i in 0..n {
        if state[i] == 0 {
            dfs(i, &edges, &mut state, &mut order, &entries)?;
        }
    }

    // Dirty = formulas whose own path or any dependency overlaps a changed
    // path, plus everything that transitively depends on a dirty formula.
    let mut dirty: HashSet<usize> = HashSet::new();
    for (i, e) in entries.iter().enumerate() {
        let touched = changed
            .iter()
            .any(|c| c.overlaps(&e.path) || e.deps.iter().any(|d| d.overlaps(c)));
        if touched {
            dirty.insert(i);
        }
    }
    let mut queue: Vec<usize> = dirty.iter().cloned().collect();
    while let Some(i) = queue.pop() {
        for &up in &rev[i] {
            if dirty.insert(up) {
                queue.push(up);
            }
        }
    }

    let mut r = root.clone();
    let mut fchanged = BTreeSet::new();
    for &i in &order {
        if !dirty.contains(&i) {
            continue;
        }
        let e = &entries[i];
        let new_cached = match eval(&e.ast, &r) {
            Ok(v) => from_value(&v),
            Err(msg) => Arc::new(Node::String(format!("#ERR: {msg}"))),
        };
        if !nodes_equal(&new_cached, &e.cached) {
            let node = Arc::new(Node::Formula(FormulaNode {
                expr: e.expr.clone(),
                cached: new_cached,
            }));
            r = set_at(&r, &e.path, node)?;
            fchanged.insert(e.path.clone());
        }
    }
    Ok((r, fchanged))
}
