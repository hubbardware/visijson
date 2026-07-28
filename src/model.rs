//! Immutable persistent JSON tree. Every node is an `Arc<Node>`; all writes
//! produce a new root by copying only the nodes along the written path
//! (path-copy), sharing every untouched subtree with the previous version.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use serde_json::{Map, Number, Value};

use crate::error::{Result, StoreError};

/// Key that marks an object in incoming JSON as a formula node.
pub const FORMULA_KEY: &str = "$formula";
/// Key holding a formula's last evaluated value in raw serialization.
pub const FORMULA_VALUE_KEY: &str = "$value";

#[derive(Debug)]
pub enum Node {
    Null,
    Bool(bool),
    Number(Number),
    String(String),
    Array(Vec<Arc<Node>>),
    Object(BTreeMap<String, Arc<Node>>),
    Formula(FormulaNode),
}

#[derive(Debug)]
pub struct FormulaNode {
    pub expr: String,
    /// Last evaluated value (always a plain, formula-free subtree).
    pub cached: Arc<Node>,
}

pub fn empty_object() -> Arc<Node> {
    Arc::new(Node::Object(BTreeMap::new()))
}

/// Build a node tree from a JSON value. Objects of the shape
/// `{"$formula": "<expr>", "$value": <cached>}` become formula nodes.
pub fn from_value(v: &Value) -> Arc<Node> {
    match v {
        Value::Null => Arc::new(Node::Null),
        Value::Bool(b) => Arc::new(Node::Bool(*b)),
        Value::Number(n) => Arc::new(Node::Number(n.clone())),
        Value::String(s) => Arc::new(Node::String(s.clone())),
        Value::Array(items) => Arc::new(Node::Array(items.iter().map(from_value).collect())),
        Value::Object(map) => {
            if let Some(Value::String(expr)) = map.get(FORMULA_KEY) {
                let cached = map
                    .get(FORMULA_VALUE_KEY)
                    .map(from_value)
                    .unwrap_or_else(|| Arc::new(Node::Null));
                return Arc::new(Node::Formula(FormulaNode {
                    expr: expr.clone(),
                    cached,
                }));
            }
            Arc::new(Node::Object(
                map.iter().map(|(k, v)| (k.clone(), from_value(v))).collect(),
            ))
        }
    }
}

/// Serialize preserving formulas as `{"$formula": ..., "$value": ...}`.
/// This is the durable representation used by snapshots.
pub fn to_raw_value(node: &Node) -> Value {
    match node {
        Node::Null => Value::Null,
        Node::Bool(b) => Value::Bool(*b),
        Node::Number(n) => Value::Number(n.clone()),
        Node::String(s) => Value::String(s.clone()),
        Node::Array(items) => Value::Array(items.iter().map(|n| to_raw_value(n)).collect()),
        Node::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), to_raw_value(v)))
                .collect::<Map<String, Value>>(),
        ),
        Node::Formula(f) => {
            let mut m = Map::new();
            m.insert(FORMULA_KEY.into(), Value::String(f.expr.clone()));
            m.insert(FORMULA_VALUE_KEY.into(), to_raw_value(&f.cached));
            Value::Object(m)
        }
    }
}

/// Serialize with formulas replaced by their cached value — what readers see.
pub fn to_plain_value(node: &Node) -> Value {
    match node {
        Node::Formula(f) => to_plain_value(&f.cached),
        Node::Null => Value::Null,
        Node::Bool(b) => Value::Bool(*b),
        Node::Number(n) => Value::Number(n.clone()),
        Node::String(s) => Value::String(s.clone()),
        Node::Array(items) => Value::Array(items.iter().map(|n| to_plain_value(n)).collect()),
        Node::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), to_plain_value(v)))
                .collect::<Map<String, Value>>(),
        ),
    }
}

pub fn nodes_equal(a: &Arc<Node>, b: &Arc<Node>) -> bool {
    Arc::ptr_eq(a, b) || to_raw_value(a) == to_raw_value(b)
}

/// A parsed RFC 6901 JSON pointer. The empty string addresses the root.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Pointer(Vec<String>);

impl Pointer {
    pub fn root() -> Self {
        Pointer(Vec::new())
    }

    pub fn parse(s: &str) -> Result<Self> {
        if s.is_empty() {
            return Ok(Self::root());
        }
        if !s.starts_with('/') {
            return Err(StoreError::InvalidPointer(s.to_string()));
        }
        let tokens = s[1..]
            .split('/')
            .map(|t| t.replace("~1", "/").replace("~0", "~"))
            .collect();
        Ok(Pointer(tokens))
    }

    pub fn from_tokens(tokens: Vec<String>) -> Self {
        Pointer(tokens)
    }

    pub fn tokens(&self) -> &[String] {
        &self.0
    }

    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }

    pub fn push(&self, token: impl Into<String>) -> Pointer {
        let mut t = self.0.clone();
        t.push(token.into());
        Pointer(t)
    }

    pub fn parent(&self) -> Option<Pointer> {
        if self.0.is_empty() {
            None
        } else {
            Some(Pointer(self.0[..self.0.len() - 1].to_vec()))
        }
    }

    pub fn is_prefix_of(&self, other: &Pointer) -> bool {
        other.0.len() >= self.0.len() && other.0[..self.0.len()] == self.0[..]
    }

    /// True when one path is an ancestor of (or equal to) the other — the
    /// relevance test for both subscriptions and formula dependencies.
    pub fn overlaps(&self, other: &Pointer) -> bool {
        self.is_prefix_of(other) || other.is_prefix_of(self)
    }

    /// Join `other` (interpreted relative to `self`) onto this pointer.
    pub fn join(&self, other: &Pointer) -> Pointer {
        let mut t = self.0.clone();
        t.extend(other.0.iter().cloned());
        Pointer(t)
    }
}

impl fmt::Display for Pointer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for tok in &self.0 {
            write!(f, "/{}", tok.replace('~', "~0").replace('/', "~1"))?;
        }
        Ok(())
    }
}

fn parse_index(tok: &str, ptr: &Pointer) -> Result<usize> {
    tok.parse::<usize>()
        .map_err(|_| StoreError::InvalidPointer(format!("{ptr} (bad array index `{tok}`)")))
}

/// Read the node at `ptr`. Traversal descends through formula nodes into
/// their cached value, so `/total/parts/0` works even if `/total` is a formula.
pub fn get_at(root: &Arc<Node>, ptr: &Pointer) -> Option<Arc<Node>> {
    let mut cur = root.clone();
    for tok in ptr.tokens() {
        while let Node::Formula(f) = &*cur {
            let next = f.cached.clone();
            cur = next;
        }
        let next = match &*cur {
            Node::Object(map) => map.get(tok)?.clone(),
            Node::Array(items) => {
                let idx = tok.parse::<usize>().ok()?;
                items.get(idx)?.clone()
            }
            _ => return None,
        };
        cur = next;
    }
    Some(cur)
}

/// Path-copy write: returns a new root with `new` placed at `ptr`.
/// Missing intermediate object keys are created; array writes require an
/// existing index, the next index (append), or `-` (append).
pub fn set_at(root: &Arc<Node>, ptr: &Pointer, new: Arc<Node>) -> Result<Arc<Node>> {
    set_rec(root, ptr.tokens(), new, ptr, false)
}

/// Like `set_at` but array indices insert (shift right) instead of replace —
/// RFC 6902 `add` semantics.
pub fn insert_at(root: &Arc<Node>, ptr: &Pointer, new: Arc<Node>) -> Result<Arc<Node>> {
    set_rec(root, ptr.tokens(), new, ptr, true)
}

fn set_rec(
    node: &Arc<Node>,
    toks: &[String],
    new: Arc<Node>,
    full: &Pointer,
    insert: bool,
) -> Result<Arc<Node>> {
    if toks.is_empty() {
        return Ok(new);
    }
    let tok = &toks[0];
    let last = toks.len() == 1;
    match &**node {
        Node::Object(map) => {
            let child = map
                .get(tok)
                .cloned()
                .unwrap_or_else(|| Arc::new(Node::Null));
            let new_child = set_rec(&child, &toks[1..], new, full, insert)?;
            let mut m = map.clone();
            m.insert(tok.clone(), new_child);
            Ok(Arc::new(Node::Object(m)))
        }
        Node::Array(items) => {
            if tok == "-" {
                if !last {
                    return Err(StoreError::TypeMismatch {
                        path: full.to_string(),
                        msg: "`-` must be the final pointer token".into(),
                    });
                }
                let mut v = items.clone();
                v.push(new);
                return Ok(Arc::new(Node::Array(v)));
            }
            let idx = parse_index(tok, full)?;
            if last && idx == items.len() {
                let mut v = items.clone();
                v.push(new);
                return Ok(Arc::new(Node::Array(v)));
            }
            if idx >= items.len() {
                return Err(StoreError::NotFound(full.to_string()));
            }
            let mut v = items.clone();
            if last && insert {
                v.insert(idx, new);
            } else {
                v[idx] = set_rec(&items[idx], &toks[1..], new, full, insert)?;
            }
            Ok(Arc::new(Node::Array(v)))
        }
        Node::Null => {
            let child = set_rec(&Arc::new(Node::Null), &toks[1..], new, full, insert)?;
            let mut m = BTreeMap::new();
            m.insert(tok.clone(), child);
            Ok(Arc::new(Node::Object(m)))
        }
        Node::Formula(_) => Err(StoreError::TypeMismatch {
            path: full.to_string(),
            msg: "cannot write through a formula node".into(),
        }),
        _ => Err(StoreError::TypeMismatch {
            path: full.to_string(),
            msg: "cannot descend into a scalar".into(),
        }),
    }
}

/// Path-copy removal of the value at `ptr`.
pub fn remove_at(root: &Arc<Node>, ptr: &Pointer) -> Result<Arc<Node>> {
    if ptr.is_root() {
        return Ok(empty_object());
    }
    remove_rec(root, ptr.tokens(), ptr)
}

fn remove_rec(node: &Arc<Node>, toks: &[String], full: &Pointer) -> Result<Arc<Node>> {
    let tok = &toks[0];
    let last = toks.len() == 1;
    match &**node {
        Node::Object(map) => {
            if last {
                if !map.contains_key(tok) {
                    return Err(StoreError::NotFound(full.to_string()));
                }
                let mut m = map.clone();
                m.remove(tok);
                return Ok(Arc::new(Node::Object(m)));
            }
            let child = map
                .get(tok)
                .ok_or_else(|| StoreError::NotFound(full.to_string()))?;
            let new_child = remove_rec(child, &toks[1..], full)?;
            let mut m = map.clone();
            m.insert(tok.clone(), new_child);
            Ok(Arc::new(Node::Object(m)))
        }
        Node::Array(items) => {
            let idx = parse_index(tok, full)?;
            if idx >= items.len() {
                return Err(StoreError::NotFound(full.to_string()));
            }
            let mut v = items.clone();
            if last {
                v.remove(idx);
            } else {
                v[idx] = remove_rec(&items[idx], &toks[1..], full)?;
            }
            Ok(Arc::new(Node::Array(v)))
        }
        _ => Err(StoreError::NotFound(full.to_string())),
    }
}

/// Visit every node in the tree (skipping formula cached values, which are
/// derived data) with its pointer path.
pub fn walk(root: &Arc<Node>, mut visit: impl FnMut(&Pointer, &Arc<Node>)) {
    fn rec(node: &Arc<Node>, path: &Pointer, visit: &mut impl FnMut(&Pointer, &Arc<Node>)) {
        visit(path, node);
        match &**node {
            Node::Object(map) => {
                for (k, v) in map {
                    rec(v, &path.push(k.clone()), visit);
                }
            }
            Node::Array(items) => {
                for (i, v) in items.iter().enumerate() {
                    rec(v, &path.push(i.to_string()), visit);
                }
            }
            _ => {}
        }
    }
    rec(root, &Pointer::root(), &mut visit);
}
