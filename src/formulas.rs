//! Formula nodes: spreadsheet-style expressions over JSON pointer references.
//!
//! Grammar:
//!   expr   := term (('+' | '-') term)*
//!   term   := factor (('*' | '/') factor)*
//!   factor := number | string | pointer | '-' factor | '(' expr ')' | ident '(' args ')'
//!   args   := expr (',' expr)*
//!
//! Pointers (`/a/b`) reference other document paths. Strings are single- or
//! double-quoted with `\` escapes. Functions: sum, count, min, max (each
//! accepts scalars and arrays). A bare pointer expression yields the
//! referenced value verbatim (any JSON type).
//!
//! Query engines are ordinary functions of the form `engine(scope, 'program')`:
//! `jq(/players, '[.[] | .name]')` runs a jq program (via jaq) against the
//! plain value of the subtree at `/players`. The scope pointer doubles as the
//! formula's dependency, so re-evaluation and cycle detection need no insight
//! into the program text. The program must produce exactly one value — wrap
//! stream outputs in `[...]`. Other engines can slot in as sibling functions
//! later (`jmes(...)` is reserved for JMESPath).
//!
//! On every commit, formulas whose dependencies overlap the changed paths are
//! re-evaluated in topological order; cycles reject the commit.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;

use imbl::{OrdMap, OrdSet};
use serde_json::{Number, Value};

use crate::error::{Result, StoreError};
use crate::model::{
    from_value, get_at, nodes_equal, set_at, to_plain_value, walk, FormulaNode, Node, Pointer,
};

#[derive(Debug, Clone)]
pub enum Expr {
    Num(f64),
    Str(String),
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
    Str(String),
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
            '\'' | '"' => {
                let quote = c;
                i += 1;
                let mut s = String::new();
                loop {
                    match chars.get(i) {
                        None => return Err("unterminated string".into()),
                        Some('\\') => {
                            match chars.get(i + 1) {
                                Some(&e) => s.push(e),
                                None => return Err("unterminated string".into()),
                            }
                            i += 2;
                        }
                        Some(&e) if e == quote => {
                            i += 1;
                            break;
                        }
                        Some(&e) => {
                            s.push(e);
                            i += 1;
                        }
                    }
                }
                toks.push(Tok::Str(s));
                prev_is_value = true;
            }
            '/' => {
                let start = i;
                while i < chars.len() && !" \t\n\r+-*(),'\"".contains(chars[i]) {
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
            Some(Tok::Str(s)) => Ok(Expr::Str(s)),
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
            Expr::Num(_) | Expr::Str(_) => {}
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
        Expr::Str(s) => Ok(Value::String(s.clone())),
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
            // Query engines: engine(scope, 'program'). Dispatched before the
            // numeric functions since their arguments aren't numbers.
            if name == "jq" {
                if args.len() != 2 {
                    return Err("jq() takes (scope, 'program')".into());
                }
                let input = eval(&args[0], root)?;
                let program = match eval(&args[1], root)? {
                    Value::String(s) => s,
                    other => return Err(format!("jq() program must be a string, got {other}")),
                };
                return eval_jq(&program, input);
            }
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

/// Run a jq program (via jaq) against `input`. The program must produce
/// exactly one value; streams should be collected with `[...]`.
fn eval_jq(program: &str, input: Value) -> std::result::Result<Value, String> {
    use jaq_core::load::{Arena, File, Loader};
    use jaq_core::{Compiler, Ctx, RcIter};
    use jaq_json::Val;

    let file = File {
        code: program,
        path: (),
    };
    let loader = Loader::new(jaq_std::defs().chain(jaq_json::defs()));
    let arena = Arena::default();
    let modules = loader
        .load(&arena, file)
        .map_err(|errs| format!("jq parse: {}", load_errors(errs)))?;
    let filter = Compiler::default()
        .with_funs(jaq_std::funs().chain(jaq_json::funs()))
        .compile(modules)
        .map_err(|errs| format!("jq compile: {}", compile_errors(errs)))?;
    let inputs = RcIter::new(core::iter::empty());
    let mut out = filter.run((Ctx::new([], &inputs), Val::from(input)));
    let first = match out.next() {
        None => return Err("jq produced no output".into()),
        Some(Err(e)) => return Err(format!("jq: {e}")),
        Some(Ok(v)) => v,
    };
    if out.next().is_some() {
        return Err("jq produced multiple values; wrap the program in [...] to collect them".into());
    }
    Ok(first.into())
}

fn load_errors(errs: jaq_core::load::Errors<&str, ()>) -> String {
    errs.into_iter()
        .map(|(_, e)| format!("{e:?}"))
        .collect::<Vec<_>>()
        .join("; ")
}

fn compile_errors(errs: jaq_core::compile::Errors<&str, ()>) -> String {
    errs.into_iter()
        .flat_map(|(_, errs)| errs.into_iter().map(|(e, _)| format!("undefined {e:?}")))
        .collect::<Vec<_>>()
        .join("; ")
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

/// Everything known about one formula, parsed once when it entered the
/// index and shared by pointer until its expression text changes.
#[derive(Debug)]
pub struct FormulaInfo {
    pub expr: String,
    pub ast: Expr,
    /// Declared dependency pointers, sorted and deduplicated.
    pub deps: Vec<Pointer>,
}

/// Persistent index of formula locations and their dependency edges,
/// carried in every `Committed` like `meta::ModIndex`: O(1) clone, path-copy
/// updates. Unlike `ModIndex` it is *exact* live state — no floor and no
/// entry-cap collapse are possible, because there is no safe over-estimate;
/// size is bounded by the formulas actually in the document.
#[derive(Debug, Clone, Default)]
pub struct FormulaIndex {
    /// formula path → its parsed info.
    formulas: OrdMap<Pointer, Arc<FormulaInfo>>,
    /// dependency pointer (as declared in some formula) → the formula
    /// paths that declare it.
    rev: OrdMap<Pointer, OrdSet<Pointer>>,
}

impl FormulaIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.formulas.len()
    }

    pub fn is_empty(&self) -> bool {
        self.formulas.is_empty()
    }

    pub fn get(&self, p: &Pointer) -> Option<&Arc<FormulaInfo>> {
        self.formulas.get(p)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Pointer, &Arc<FormulaInfo>)> {
        self.formulas.iter()
    }

    /// O(1): do these two indexes share the same underlying maps? True
    /// exactly when the `update_index` that produced one from the other
    /// removed, added, and changed no formulas (a no-op update returns an
    /// untouched clone of the same persistent maps).
    pub fn same_as(&self, other: &FormulaIndex) -> bool {
        self.formulas.ptr_eq(&other.formulas) && self.rev.ptr_eq(&other.rev)
    }

    /// Would a write at `p` dirty any formula — own path or declared
    /// dependency overlapping `p`? The commit-rebase fast path declines
    /// whenever this is true for a written subtree.
    pub fn any_affected_by(&self, p: &Pointer) -> bool {
        let mut hit = false;
        overlapping(&self.formulas, p, |_, _| hit = true);
        if !hit {
            overlapping(&self.rev, p, |_, _| hit = true);
        }
        hit
    }

    /// Formulas at or under any of `scan_roots` that are new in `self`
    /// relative to `prev`, or whose parsed info changed (an identical
    /// re-write reuses the old `Arc`, so pointer identity means "same
    /// expression"). Used by write guards; O(hits) via range scans.
    pub fn written_since(
        &self,
        prev: &FormulaIndex,
        scan_roots: &[Pointer],
    ) -> Vec<(Pointer, Arc<FormulaInfo>)> {
        let mut out: Vec<(Pointer, Arc<FormulaInfo>)> = Vec::new();
        if self.same_as(prev) {
            return out;
        }
        for sr in scan_roots {
            overlapping(&self.formulas, sr, |k, info| {
                let unchanged = prev.formulas.get(k).is_some_and(|old| Arc::ptr_eq(old, info));
                if sr.is_prefix_of(k) && !unchanged && !out.iter().any(|(p, _)| p == k) {
                    out.push((k.clone(), info.clone()));
                }
            });
        }
        out
    }

    fn insert(&mut self, path: Pointer, info: Arc<FormulaInfo>) {
        for d in &info.deps {
            self.rev.entry(d.clone()).or_default().insert(path.clone());
        }
        self.formulas.insert(path, info);
    }

    fn remove(&mut self, path: &Pointer) {
        let Some(info) = self.formulas.remove(path) else {
            return;
        };
        for d in &info.deps {
            let now_empty = match self.rev.get_mut(d) {
                Some(set) => {
                    set.remove(path);
                    set.is_empty()
                }
                None => false,
            };
            if now_empty {
                self.rev.remove(d);
            }
        }
    }

    /// Formulas made dirty by a change at `c`: own path overlaps `c` (a
    /// subtree replace must dirty every formula under it) or some declared
    /// dependency overlaps `c`.
    fn affected_by(&self, c: &Pointer, out: &mut BTreeSet<Pointer>) {
        overlapping(&self.formulas, c, |k, _| {
            out.insert(k.clone());
        });
        overlapping(&self.rev, c, |_, dependents| {
            for f in dependents {
                out.insert(f.clone());
            }
        });
    }

    /// Formulas with a declared dependency overlapping formula path `f` —
    /// the reverse dependency edge.
    fn dependents_of(&self, f: &Pointer, out: &mut BTreeSet<Pointer>) {
        overlapping(&self.rev, f, |_, dependents| {
            for p in dependents {
                out.insert(p.clone());
            }
        });
    }
}

/// Entries of `map` whose key is a strict descendant of `p`. Descendants
/// are contiguous after `p` because `Pointer` orders element-wise over its
/// token vector (`["a"] < ["a","b"] < ["ab"]`), so one range scan suffices.
fn descendants<'a, V>(
    map: &'a OrdMap<Pointer, V>,
    p: &'a Pointer,
) -> impl Iterator<Item = (&'a Pointer, &'a V)> {
    map.range::<_, Pointer>((Bound::Excluded(p), Bound::Unbounded))
        .take_while(move |(k, _)| p.is_prefix_of(k))
}

/// Visit entries of `map` whose key overlaps `p`: one point-lookup per
/// prefix of `p` (root and `p` itself included) plus one descendant range
/// scan — O(depth · log n + hits) instead of a full scan.
fn overlapping<V>(map: &OrdMap<Pointer, V>, p: &Pointer, mut f: impl FnMut(&Pointer, &V)) {
    let mut prefix = Pointer::root();
    if let Some(v) = map.get(&prefix) {
        f(&prefix, v);
    }
    for tok in p.tokens() {
        prefix = prefix.push(tok.clone());
        if let Some(v) = map.get(&prefix) {
            f(&prefix, v);
        }
    }
    for (k, v) in descendants(map, p) {
        f(k, v);
    }
}

/// Parse an expression into index form (deps sorted + deduplicated).
fn parse_info(expr: &str) -> Result<FormulaInfo> {
    let ast = parse(expr)?;
    let mut deps = dependencies(&ast);
    deps.sort();
    deps.dedup();
    Ok(FormulaInfo {
        expr: expr.to_string(),
        ast,
        deps,
    })
}

/// Scan the entire tree for formulas. Used once per store open / recovery;
/// per-commit maintenance goes through `update_index`.
///
/// Lenient on parse errors: a formula that fails to parse can only come
/// from a snapshot written by something else (live commits reject them), so
/// it is skipped with a warning — its stale cached value stays inert in the
/// tree — rather than making every subsequent commit fail (the historical
/// behavior) or bricking recovery outright.
pub fn scan_index(root: &Arc<Node>) -> FormulaIndex {
    let mut found: Vec<(Pointer, String)> = Vec::new();
    walk(root, |toks, node| {
        if let Node::Formula(f) = &**node {
            let path = Pointer::from_tokens(toks.iter().map(|t| t.to_string()).collect());
            found.push((path, f.expr.clone()));
        }
    });
    let mut index = FormulaIndex::new();
    for (path, expr) in found {
        match parse_info(&expr) {
            Ok(info) => index.insert(path, Arc::new(info)),
            Err(e) => {
                tracing::warn!(path = %path, error = %e, "skipping unparseable formula in scan")
            }
        }
    }
    index
}

/// Incrementally maintain the formula index across a commit: `scan_roots`
/// are the subtree roots the ops wrote into `root` (array element inserts /
/// removes report the whole array, since siblings shift). Index entries
/// overlapping a scan root are dropped and rediscovered by rescanning just
/// that subtree — commits that never touch a formula-bearing subtree cost
/// only the size of what they wrote, not the size of the document.
/// Errors with `FormulaParse` if a formula this commit wrote fails to
/// parse — rejecting the commit, exactly as evaluation-time parsing used
/// to. A *pre-existing* unparseable formula (possible only via a foreign
/// snapshot; see `scan_index`) no longer rejects unrelated commits.
pub fn update_index(
    prev: &FormulaIndex,
    scan_roots: &[Pointer],
    root: &Arc<Node>,
) -> Result<FormulaIndex> {
    if scan_roots.is_empty() {
        return Ok(prev.clone());
    }
    let mut index = prev.clone();
    // Collect removals across all scan roots first — roots may overlap.
    let mut removed: BTreeSet<Pointer> = BTreeSet::new();
    for sr in scan_roots {
        overlapping(&prev.formulas, sr, |k, _| {
            removed.insert(k.clone());
        });
    }
    for p in &removed {
        index.remove(p);
    }
    // A dropped entry strictly above a scan root is untouched by the write
    // unless the write replaced it; re-add it if it survived. Writes below
    // a formula node are rejected, so a survivor's expression is unchanged
    // and its parsed info can be reused — the expr check is belt-and-braces.
    for p in &removed {
        if !scan_roots.iter().any(|sr| p.is_prefix_of(sr) && p != sr) {
            continue;
        }
        if let Some(Node::Formula(f)) = get_at(root, p).as_deref() {
            let info = match prev.formulas.get(p) {
                Some(old) if old.expr == f.expr => old.clone(),
                _ => Arc::new(parse_info(&f.expr)?),
            };
            index.insert(p.clone(), info);
        }
    }
    // Rescan the written subtrees. Reuse the old parsed info whenever the
    // expression text is unchanged (identical re-PUTs never reparse).
    for sr in scan_roots {
        let Some(sub) = get_at(root, sr) else {
            continue;
        };
        let mut found: Vec<(Pointer, String)> = Vec::new();
        walk(&sub, |toks, node| {
            if let Node::Formula(f) = &**node {
                let mut t = sr.tokens().to_vec();
                t.extend(toks.iter().map(|c| c.to_string()));
                found.push((Pointer::from_tokens(t), f.expr.clone()));
            }
        });
        for (path, expr) in found {
            let info = match prev.formulas.get(&path) {
                Some(old) if old.expr == expr => old.clone(),
                _ => Arc::new(parse_info(&expr)?),
            };
            index.insert(path, info);
        }
    }
    Ok(index)
}

/// Re-evaluate formulas affected by `changed` paths, using the maintained
/// dependency `index` (no tree scan, no reparse). The dirty set is seeded
/// from the changed paths, closed over reverse dependency edges, and only
/// that subgraph is sorted and evaluated — cost is proportional to what the
/// commit actually touched, not to the number of formulas in the document.
/// Returns the new root and the set of formula paths whose value actually
/// changed. Errors with `FormulaCycle` if the *dirty* subgraph is cyclic:
/// a newly formed cycle always includes a formula this commit wrote, so it
/// is always seeded and rejected; a pre-existing cycle in an untouched
/// region (possible only via a foreign snapshot) no longer rejects
/// unrelated commits.
pub fn reevaluate(
    root: &Arc<Node>,
    changed: &BTreeSet<Pointer>,
    index: &FormulaIndex,
) -> Result<(Arc<Node>, BTreeSet<Pointer>)> {
    if index.is_empty() || changed.is_empty() {
        return Ok((root.clone(), BTreeSet::new()));
    }

    // Dirty = formulas whose own path or any dependency overlaps a changed
    // path, plus everything that transitively depends on a dirty formula.
    let mut dirty: BTreeSet<Pointer> = BTreeSet::new();
    for c in changed {
        index.affected_by(c, &mut dirty);
    }
    let mut queue: Vec<Pointer> = dirty.iter().cloned().collect();
    while let Some(p) = queue.pop() {
        let mut ups = BTreeSet::new();
        index.dependents_of(&p, &mut ups);
        for up in ups {
            if dirty.insert(up.clone()) {
                queue.push(up);
            }
        }
    }
    if dirty.is_empty() {
        return Ok((root.clone(), BTreeSet::new()));
    }

    // Dependency edges within the dirty subgraph only. edge j -> i means
    // formula i depends on formula j (i re-evaluates after j).
    let paths: Vec<Pointer> = dirty.into_iter().collect(); // sorted
    let pos: HashMap<&Pointer, usize> = paths.iter().enumerate().map(|(i, p)| (p, i)).collect();
    let n = paths.len();
    let mut edges: Vec<Vec<usize>> = vec![Vec::new(); n];
    let mut indeg: Vec<usize> = vec![0; n];
    let mut seen: HashSet<(usize, usize)> = HashSet::new();
    for (i, p) in paths.iter().enumerate() {
        let Some(info) = index.get(p) else {
            debug_assert!(false, "formula index missing dirty entry {p}");
            continue;
        };
        let mut self_cycle = false;
        for d in &info.deps {
            overlapping(&index.formulas, d, |q, _| {
                let Some(&j) = pos.get(q) else { return };
                if i == j {
                    self_cycle = true;
                } else if seen.insert((j, i)) {
                    edges[j].push(i);
                    indeg[i] += 1;
                }
            });
        }
        if self_cycle {
            return Err(StoreError::FormulaCycle(p.to_string()));
        }
    }

    // Iterative topological order (Kahn's), ready set drained in ascending
    // path order so evaluation order is deterministic. Any valid order
    // yields identical values — every eval reads only its declared deps —
    // but determinism keeps replay and diagnostics predictable.
    let mut ready: std::collections::BinaryHeap<std::cmp::Reverse<usize>> = indeg
        .iter()
        .enumerate()
        .filter(|(_, &d)| d == 0)
        .map(|(i, _)| std::cmp::Reverse(i))
        .collect();
    let mut order: Vec<usize> = Vec::with_capacity(n);
    while let Some(std::cmp::Reverse(i)) = ready.pop() {
        order.push(i);
        for &j in &edges[i] {
            indeg[j] -= 1;
            if indeg[j] == 0 {
                ready.push(std::cmp::Reverse(j));
            }
        }
    }
    if order.len() < n {
        // Everything left has positive in-degree: a cycle (or downstream
        // of one). Report the lowest path for determinism.
        let p = indeg
            .iter()
            .position(|&d| d > 0)
            .map(|i| &paths[i])
            .expect("leftover node with zero in-degree");
        return Err(StoreError::FormulaCycle(p.to_string()));
    }

    // Evaluate deps-first against the progressively updated root; each
    // formula is written at most once, so the pre-commit cached value is
    // still in the tree when we compare against it.
    let mut r = root.clone();
    let mut fchanged = BTreeSet::new();
    for &i in &order {
        let path = &paths[i];
        let Some(info) = index.get(path) else {
            continue;
        };
        let Some(node) = get_at(&r, path) else {
            debug_assert!(false, "formula index points at missing node {path}");
            continue;
        };
        let Node::Formula(f) = &*node else {
            debug_assert!(false, "formula index points at non-formula {path}");
            continue;
        };
        let new_cached = match eval(&info.ast, &r) {
            Ok(v) => from_value(&v),
            Err(msg) => Arc::new(Node::String(format!("#ERR: {msg}"))),
        };
        if !nodes_equal(&new_cached, &f.cached) {
            let node = Arc::new(Node::Formula(FormulaNode {
                expr: f.expr.clone(),
                cached: new_cached,
            }));
            r = set_at(&r, path, node)?;
            fchanged.insert(path.clone());
        }
    }
    Ok((r, fchanged))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ptr(s: &str) -> Pointer {
        Pointer::parse(s).unwrap()
    }

    #[test]
    fn descendant_scan_stops_at_token_boundaries() {
        let mut map: OrdMap<Pointer, ()> = OrdMap::new();
        for k in ["/a", "/a/b", "/a/b/c", "/a/z", "/ab", "/b"] {
            map.insert(ptr(k), ());
        }
        // /ab is a sibling of /a, not a descendant — token-vector ordering
        // keeps /a's true descendants contiguous right after it.
        let hits: Vec<String> = descendants(&map, &ptr("/a"))
            .map(|(k, _)| k.to_string())
            .collect();
        assert_eq!(hits, vec!["/a/b", "/a/b/c", "/a/z"]);
        // Everything descends from the root.
        assert_eq!(descendants(&map, &Pointer::root()).count(), map.len());
    }

    #[test]
    fn update_index_maintains_both_maps() {
        let root = from_value(&json!({"a": 1, "f": {"$formula": "/a + 1"}}));
        let index = scan_index(&root);
        assert_eq!(index.len(), 1);
        assert_eq!(index.get(&ptr("/f")).unwrap().deps, vec![ptr("/a")]);
        assert!(index.rev.get(&ptr("/a")).unwrap().contains(&ptr("/f")));

        // Overwriting the formula with a plain value empties both maps.
        let root2 = from_value(&json!({"a": 1, "f": 2}));
        let index2 = update_index(&index, &[ptr("/f")], &root2).unwrap();
        assert!(index2.is_empty());
        assert!(index2.rev.is_empty(), "empty dependent sets must be pruned");

        // Replacing it with a formula on another dep moves the rev key.
        let root3 = from_value(&json!({"a": 1, "b": 2, "f": {"$formula": "/b * 2"}}));
        let index3 = update_index(&index, &[ptr("/f")], &root3).unwrap();
        assert_eq!(index3.len(), 1);
        assert!(index3.rev.get(&ptr("/a")).is_none());
        assert!(index3.rev.get(&ptr("/b")).unwrap().contains(&ptr("/f")));
    }

    #[test]
    fn unchanged_expression_reuses_parsed_info() {
        let root = from_value(&json!({"a": 1, "calc": {"f": {"$formula": "/a + 1"}}}));
        let index = scan_index(&root);
        let before = index.get(&ptr("/calc/f")).unwrap().clone();
        // Identical re-write of the subtree: same Arc, no reparse.
        let index2 = update_index(&index, &[ptr("/calc")], &root).unwrap();
        assert!(Arc::ptr_eq(&before, index2.get(&ptr("/calc/f")).unwrap()));
    }

    #[test]
    fn scan_index_skips_unparseable_formulas() {
        let root = from_value(&json!({
            "bad": {"$formula": "1 + + )", "$value": null},
            "good": {"$formula": "2 * 3", "$value": 6},
        }));
        let index = scan_index(&root);
        assert_eq!(index.len(), 1);
        assert!(index.get(&ptr("/good")).is_some());
    }

    #[test]
    fn preexisting_cycle_only_rejects_commits_that_touch_it() {
        // A committed cycle is unreachable through the API (cycle-creating
        // commits are rejected); it can only arrive via a foreign snapshot,
        // which scan_index accepts as-is.
        let root = from_value(&json!({
            "f": {"$formula": "/g + 1", "$value": 1},
            "g": {"$formula": "/f + 1", "$value": 2},
            "x": 0,
            "fx": {"$formula": "/x * 2", "$value": 99},
        }));
        let index = scan_index(&root);
        assert_eq!(index.len(), 3);

        // A change not touching the cycle re-evaluates only what it hit.
        let changed: BTreeSet<Pointer> = [ptr("/x")].into_iter().collect();
        let (_, fchanged) = reevaluate(&root, &changed, &index).unwrap();
        assert_eq!(fchanged, [ptr("/fx")].into_iter().collect());

        // A change touching the cycle is still rejected.
        let changed: BTreeSet<Pointer> = [ptr("/g")].into_iter().collect();
        let err = reevaluate(&root, &changed, &index).unwrap_err();
        assert!(matches!(err, StoreError::FormulaCycle(_)), "got {err:?}");
    }
}
