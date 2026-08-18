//! Immutable persistent JSON tree. Every node is an `Arc<Node>`; all writes
//! produce a new root by copying only the nodes along the written path
//! (path-copy), sharing every untouched subtree with the previous version.

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

use imbl::OrdMap;
use serde::{Deserialize as _, Serialize as _};
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
    // A persistent sorted map: `clone()` is O(1) and mutation path-copies
    // O(log n) tree nodes, so path-copy writes into huge objects stay cheap
    // while iteration remains ordered by key (the diff walks rely on that).
    Object(OrdMap<String, Arc<Node>>),
    Formula(FormulaNode),
}

#[derive(Debug)]
pub struct FormulaNode {
    pub expr: String,
    /// Last evaluated value (always a plain, formula-free subtree).
    pub cached: Arc<Node>,
}

pub fn empty_object() -> Arc<Node> {
    Arc::new(Node::Object(OrdMap::new()))
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

/// `Node` serializes in **raw** form (formulas as `{"$formula", "$value"}`)
/// — byte-identical to serializing `to_raw_value`, without building a
/// `Value`. Used for the WAL, snapshots, and `?raw=true` responses.
impl serde::Serialize for Node {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::{SerializeMap, SerializeSeq};
        match self {
            Node::Null => s.serialize_unit(),
            Node::Bool(b) => s.serialize_bool(*b),
            Node::Number(n) => n.serialize(s),
            Node::String(v) => s.serialize_str(v),
            Node::Array(items) => {
                let mut seq = s.serialize_seq(Some(items.len()))?;
                for item in items {
                    seq.serialize_element(&**item)?;
                }
                seq.end()
            }
            Node::Object(map) => {
                let mut m = s.serialize_map(Some(map.len()))?;
                for (k, v) in map {
                    m.serialize_entry(k, &**v)?;
                }
                m.end()
            }
            Node::Formula(f) => {
                let mut m = s.serialize_map(Some(2))?;
                m.serialize_entry(FORMULA_KEY, &f.expr)?;
                m.serialize_entry(FORMULA_VALUE_KEY, &*f.cached)?;
                m.end()
            }
        }
    }
}

/// Deserialize JSON straight into a `Node` tree — no intermediate
/// `serde_json::Value`. Objects shaped `{"$formula": "..."}` become formula
/// nodes, mirroring `from_value`.
impl<'de> serde::Deserialize<'de> for Node {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Node, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = Node;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("any JSON value")
            }
            fn visit_bool<E>(self, v: bool) -> std::result::Result<Node, E> {
                Ok(Node::Bool(v))
            }
            fn visit_i64<E>(self, v: i64) -> std::result::Result<Node, E> {
                Ok(Node::Number(Number::from(v)))
            }
            fn visit_u64<E>(self, v: u64) -> std::result::Result<Node, E> {
                Ok(Node::Number(Number::from(v)))
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> std::result::Result<Node, E> {
                Number::from_f64(v)
                    .map(Node::Number)
                    .ok_or_else(|| E::custom("non-finite number"))
            }
            fn visit_str<E>(self, v: &str) -> std::result::Result<Node, E> {
                Ok(Node::String(v.to_owned()))
            }
            fn visit_string<E>(self, v: String) -> std::result::Result<Node, E> {
                Ok(Node::String(v))
            }
            fn visit_unit<E>(self) -> std::result::Result<Node, E> {
                Ok(Node::Null)
            }
            fn visit_none<E>(self) -> std::result::Result<Node, E> {
                Ok(Node::Null)
            }
            fn visit_some<D: serde::Deserializer<'de>>(
                self,
                d: D,
            ) -> std::result::Result<Node, D::Error> {
                Node::deserialize(d)
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Node, A::Error> {
                let mut items = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(n) = seq.next_element::<Node>()? {
                    items.push(Arc::new(n));
                }
                Ok(Node::Array(items))
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut access: A,
            ) -> std::result::Result<Node, A::Error> {
                let mut map = OrdMap::new();
                while let Some((k, v)) = access.next_entry::<String, Node>()? {
                    map.insert(k, Arc::new(v));
                }
                if let Some(expr) = map.get(FORMULA_KEY).and_then(|n| match &**n {
                    Node::String(s) => Some(s.clone()),
                    _ => None,
                }) {
                    let cached = map
                        .get(FORMULA_VALUE_KEY)
                        .cloned()
                        .unwrap_or_else(|| Arc::new(Node::Null));
                    return Ok(Node::Formula(FormulaNode { expr, cached }));
                }
                Ok(Node::Object(map))
            }
        }
        d.deserialize_any(V)
    }
}

/// Deserialize a JSON body straight into a node tree while **reconciling
/// against `hint`** — the subtree the value is about to replace. Wherever
/// the parsed content equals the hint, the hint's existing `Arc`s are
/// returned instead of fresh allocations, so a poll-style full replace
/// arrives already sharing structure with the current tree and everything
/// downstream (commit-time reconcile, diff, formula overlap) short-circuits
/// on pointer equality. The hint is purely an optimization: a mismatched or
/// stale hint just yields fresh nodes, and commit-time `reconcile` remains
/// the correctness authority. Formula shapes (`{"$formula": ...}`) parse
/// exactly as `Node`'s `Deserialize` does.
pub fn node_from_slice_reconciling(
    bytes: &[u8],
    hint: Option<&Arc<Node>>,
) -> serde_json::Result<Arc<Node>> {
    use serde::de::DeserializeSeed;
    let mut de = serde_json::Deserializer::from_slice(bytes);
    let node = ReconcileSeed { hint }.deserialize(&mut de)?;
    de.end()?;
    Ok(node)
}

struct ReconcileSeed<'a> {
    hint: Option<&'a Arc<Node>>,
}

impl<'de> serde::de::DeserializeSeed<'de> for ReconcileSeed<'_> {
    type Value = Arc<Node>;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        d: D,
    ) -> std::result::Result<Arc<Node>, D::Error> {
        d.deserialize_any(self)
    }
}

impl ReconcileSeed<'_> {
    /// Reuse the hint `Arc` when the parsed scalar equals it.
    fn scalar(self, n: Node) -> Arc<Node> {
        if let Some(h) = self.hint {
            if nodes_equal_shallow(h, &n) {
                return h.clone();
            }
        }
        Arc::new(n)
    }
}

/// Scalar-only equality (no recursion): used by the reconciling
/// deserializer to decide whether a freshly parsed leaf can reuse the
/// hint's `Arc`.
fn nodes_equal_shallow(a: &Node, b: &Node) -> bool {
    match (a, b) {
        (Node::Null, Node::Null) => true,
        (Node::Bool(x), Node::Bool(y)) => x == y,
        (Node::Number(x), Node::Number(y)) => x == y,
        (Node::String(x), Node::String(y)) => x == y,
        _ => false,
    }
}

impl<'de> serde::de::Visitor<'de> for ReconcileSeed<'_> {
    type Value = Arc<Node>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_bool<E>(self, v: bool) -> std::result::Result<Arc<Node>, E> {
        Ok(self.scalar(Node::Bool(v)))
    }
    fn visit_i64<E>(self, v: i64) -> std::result::Result<Arc<Node>, E> {
        Ok(self.scalar(Node::Number(Number::from(v))))
    }
    fn visit_u64<E>(self, v: u64) -> std::result::Result<Arc<Node>, E> {
        Ok(self.scalar(Node::Number(Number::from(v))))
    }
    fn visit_f64<E: serde::de::Error>(self, v: f64) -> std::result::Result<Arc<Node>, E> {
        let n = Number::from_f64(v).ok_or_else(|| E::custom("non-finite number"))?;
        Ok(self.scalar(Node::Number(n)))
    }
    fn visit_str<E>(self, v: &str) -> std::result::Result<Arc<Node>, E> {
        if let Some(h) = self.hint {
            if matches!(&**h, Node::String(s) if s == v) {
                return Ok(h.clone());
            }
        }
        Ok(Arc::new(Node::String(v.to_owned())))
    }
    fn visit_string<E>(self, v: String) -> std::result::Result<Arc<Node>, E> {
        Ok(self.scalar(Node::String(v)))
    }
    fn visit_unit<E>(self) -> std::result::Result<Arc<Node>, E> {
        Ok(self.scalar(Node::Null))
    }
    fn visit_none<E>(self) -> std::result::Result<Arc<Node>, E> {
        Ok(self.scalar(Node::Null))
    }
    fn visit_some<D: serde::Deserializer<'de>>(
        self,
        d: D,
    ) -> std::result::Result<Arc<Node>, D::Error> {
        use serde::de::DeserializeSeed;
        self.deserialize(d)
    }

    fn visit_seq<A: serde::de::SeqAccess<'de>>(
        self,
        mut seq: A,
    ) -> std::result::Result<Arc<Node>, A::Error> {
        let hint_items = match self.hint.map(|h| &**h) {
            Some(Node::Array(items)) => Some(items),
            _ => None,
        };
        let mut out: Vec<Arc<Node>> = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        loop {
            let child_hint = hint_items.and_then(|it| it.get(out.len()));
            match seq.next_element_seed(ReconcileSeed { hint: child_hint })? {
                Some(n) => out.push(n),
                None => break,
            }
        }
        if let (Some(h), Some(items)) = (self.hint, hint_items) {
            if items.len() == out.len() && out.iter().zip(items).all(|(a, b)| Arc::ptr_eq(a, b)) {
                return Ok(h.clone());
            }
        }
        Ok(Arc::new(Node::Array(out)))
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(
        self,
        mut access: A,
    ) -> std::result::Result<Arc<Node>, A::Error> {
        let (hint_obj, hint_formula) = match self.hint.map(|h| &**h) {
            Some(Node::Object(m)) => (Some(m), None),
            Some(Node::Formula(f)) => (None, Some(f)),
            _ => (None, None),
        };
        let mut map = OrdMap::new();
        while let Some(key) = access.next_key::<String>()? {
            // If this turns out to be a formula shape, its `$value` child
            // reconciles against the hint formula's cached subtree.
            let child_hint = if key == FORMULA_VALUE_KEY {
                hint_formula
                    .map(|f| &f.cached)
                    .or_else(|| hint_obj.and_then(|m| m.get(&key)))
            } else {
                hint_obj.and_then(|m| m.get(&key))
            };
            let node = access.next_value_seed(ReconcileSeed { hint: child_hint })?;
            map.insert(key, node);
        }
        if let Some(expr) = map.get(FORMULA_KEY).and_then(|n| match &**n {
            Node::String(s) => Some(s.clone()),
            _ => None,
        }) {
            let cached = map
                .get(FORMULA_VALUE_KEY)
                .cloned()
                .unwrap_or_else(|| Arc::new(Node::Null));
            if let (Some(h), Some(f)) = (self.hint, hint_formula) {
                if f.expr == expr && Arc::ptr_eq(&f.cached, &cached) {
                    return Ok(h.clone());
                }
            }
            return Ok(Arc::new(Node::Formula(FormulaNode { expr, cached })));
        }
        if let (Some(h), Some(hm)) = (self.hint, hint_obj) {
            if hm.len() == map.len()
                && map
                    .iter()
                    .all(|(k, v)| hm.get(k).is_some_and(|hv| Arc::ptr_eq(hv, v)))
            {
                return Ok(h.clone());
            }
        }
        Ok(Arc::new(Node::Object(map)))
    }
}

/// Serialize an `Arc<Node>` in **plain** form: formulas replaced by their
/// cached values — what readers see. The streaming twin of `to_plain_value`.
pub struct PlainView(pub Arc<Node>);

impl serde::Serialize for PlainView {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        plain_serialize(&self.0, s)
    }
}

fn plain_serialize<S: serde::Serializer>(
    node: &Node,
    s: S,
) -> std::result::Result<S::Ok, S::Error> {
    use serde::ser::{SerializeMap, SerializeSeq};
    struct P<'a>(&'a Node);
    impl serde::Serialize for P<'_> {
        fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
            plain_serialize(self.0, s)
        }
    }
    match node {
        Node::Formula(f) => plain_serialize(&f.cached, s),
        Node::Array(items) => {
            let mut seq = s.serialize_seq(Some(items.len()))?;
            for item in items {
                seq.serialize_element(&P(item))?;
            }
            seq.end()
        }
        Node::Object(map) => {
            let mut m = s.serialize_map(Some(map.len()))?;
            for (k, v) in map {
                m.serialize_entry(k, &P(v))?;
            }
            m.end()
        }
        other => other.serialize(s),
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
    if Arc::ptr_eq(a, b) {
        return true;
    }
    match (&**a, &**b) {
        (Node::Null, Node::Null) => true,
        (Node::Bool(x), Node::Bool(y)) => x == y,
        (Node::Number(x), Node::Number(y)) => x == y,
        (Node::String(x), Node::String(y)) => x == y,
        (Node::Array(x), Node::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(a, b)| nodes_equal(a, b))
        }
        // Map iteration is sorted, so equal-length maps compare by a
        // single lockstep walk — and shared subtrees exit via ptr_eq above.
        (Node::Object(x), Node::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .zip(y)
                    .all(|((ka, va), (kb, vb))| ka == kb && nodes_equal(va, vb))
        }
        (Node::Formula(x), Node::Formula(y)) => {
            x.expr == y.expr && nodes_equal(&x.cached, &y.cached)
        }
        _ => false,
    }
}

/// One step of a lockstep walk over two sorted key spaces.
pub enum MergeSide<'a> {
    /// Key only in the old/left map.
    Left(&'a String, &'a Arc<Node>),
    /// Key only in the new/right map.
    Right(&'a String, &'a Arc<Node>),
    /// Key in both.
    Both(&'a String, &'a Arc<Node>, &'a Arc<Node>),
}

/// Walk two object maps in lockstep. `OrdMap` iteration is ascending by
/// key, so one linear merge pass classifies every key as removed, added,
/// or shared — no per-key lookups.
fn merge_walk<'a>(
    a: &'a OrdMap<String, Arc<Node>>,
    b: &'a OrdMap<String, Arc<Node>>,
    mut f: impl FnMut(MergeSide<'a>),
) {
    let mut ia = a.iter().peekable();
    let mut ib = b.iter().peekable();
    loop {
        match (ia.peek(), ib.peek()) {
            (Some(&(ka, _)), Some(&(kb, _))) => match ka.cmp(kb) {
                std::cmp::Ordering::Less => {
                    let (k, v) = ia.next().unwrap();
                    f(MergeSide::Left(k, v));
                }
                std::cmp::Ordering::Greater => {
                    let (k, v) = ib.next().unwrap();
                    f(MergeSide::Right(k, v));
                }
                std::cmp::Ordering::Equal => {
                    let (k, va) = ia.next().unwrap();
                    let (_, vb) = ib.next().unwrap();
                    f(MergeSide::Both(k, va, vb));
                }
            },
            (Some(_), None) => {
                let (k, v) = ia.next().unwrap();
                f(MergeSide::Left(k, v));
            }
            (None, Some(_)) => {
                let (k, v) = ib.next().unwrap();
                f(MergeSide::Right(k, v));
            }
            (None, None) => break,
        }
    }
}

/// Rebuild `new` reusing subtrees of `old` wherever they are equal, so a
/// full-subtree replacement (poll a source, PUT the whole list) restores
/// structural sharing with the previous version instead of keeping two
/// copies. Returns `old`'s own Arc when nothing changed at all — downstream
/// `Arc::ptr_eq` checks (diffs, equality, event encoding) then short-circuit
/// exactly as if only the changed entries had been written.
pub fn reconcile(old: &Arc<Node>, new: &Arc<Node>) -> Arc<Node> {
    if Arc::ptr_eq(old, new) {
        return old.clone();
    }
    match (&**old, &**new) {
        (Node::Object(a), Node::Object(b)) => {
            let mut out = OrdMap::new();
            let mut all_old = a.len() == b.len();
            merge_walk(a, b, |side| match side {
                MergeSide::Left(_, _) => all_old = false,
                MergeSide::Right(k, vb) => {
                    all_old = false;
                    out.insert(k.clone(), vb.clone());
                }
                MergeSide::Both(k, va, vb) => {
                    let r = reconcile(va, vb);
                    if !Arc::ptr_eq(&r, va) {
                        all_old = false;
                    }
                    out.insert(k.clone(), r);
                }
            });
            if all_old {
                old.clone()
            } else {
                Arc::new(Node::Object(out))
            }
        }
        (Node::Array(a), Node::Array(b)) => {
            let mut all_old = a.len() == b.len();
            let out: Vec<Arc<Node>> = b
                .iter()
                .enumerate()
                .map(|(i, vb)| match a.get(i) {
                    Some(va) => {
                        let r = reconcile(va, vb);
                        if !Arc::ptr_eq(&r, va) {
                            all_old = false;
                        }
                        r
                    }
                    None => {
                        all_old = false;
                        vb.clone()
                    }
                })
                .collect();
            if all_old {
                old.clone()
            } else {
                Arc::new(Node::Array(out))
            }
        }
        _ => {
            if nodes_equal(old, new) {
                old.clone()
            } else {
                new.clone()
            }
        }
    }
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

    /// This pointer expressed relative to `base` (None if not under it).
    pub fn relative_to(&self, base: &Pointer) -> Option<Pointer> {
        base.is_prefix_of(self)
            .then(|| Pointer(self.0[base.0.len()..].to_vec()))
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
            let mut m = OrdMap::new();
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

/// Structural diff: record the paths where `new` actually differs from
/// `old`. Shared subtrees (`Arc::ptr_eq`) and equal scalars contribute
/// nothing, so replacing a subtree with a mostly-identical value reports
/// only the fields that really changed. Containers of different shape
/// (object vs scalar, arrays of different length) report their own path.
pub fn diff_paths(old: &Arc<Node>, new: &Arc<Node>, base: &Pointer, out: &mut Vec<Pointer>) {
    if Arc::ptr_eq(old, new) {
        return;
    }
    match (&**old, &**new) {
        (Node::Null, Node::Null) => {}
        (Node::Bool(a), Node::Bool(b)) if a == b => {}
        (Node::Number(a), Node::Number(b)) if a == b => {}
        (Node::String(a), Node::String(b)) if a == b => {}
        (Node::Object(a), Node::Object(b)) => {
            merge_walk(a, b, |side| match side {
                MergeSide::Left(k, _) | MergeSide::Right(k, _) => out.push(base.push(k.clone())),
                MergeSide::Both(k, va, vb) => diff_paths(va, vb, &base.push(k.clone()), out),
            });
        }
        (Node::Array(a), Node::Array(b)) if a.len() == b.len() => {
            for (i, (va, vb)) in a.iter().zip(b.iter()).enumerate() {
                diff_paths(va, vb, &base.push(i.to_string()), out);
            }
        }
        (Node::Formula(a), Node::Formula(b)) => {
            if a.expr != b.expr || !nodes_equal(&a.cached, &b.cached) {
                out.push(base.clone());
            }
        }
        _ => out.push(base.clone()),
    }
}

/// Apply an RFC 7396 JSON Merge Patch to `target`, returning the merged
/// node. Object members merge recursively (null members delete); anything
/// else — including formula nodes, which are leaves — replaces wholesale.
pub fn merge_value(target: Option<&Arc<Node>>, patch: &Arc<Node>) -> Arc<Node> {
    match &**patch {
        Node::Object(pm) => {
            let mut out = match target.map(|t| &**t) {
                Some(Node::Object(m)) => m.clone(),
                _ => OrdMap::new(),
            };
            for (k, v) in pm {
                if matches!(&**v, Node::Null) {
                    out.remove(k.as_str());
                } else {
                    let merged = merge_value(out.get(k.as_str()), v);
                    out.insert(k.clone(), merged);
                }
            }
            Arc::new(Node::Object(out))
        }
        _ => patch.clone(),
    }
}

/// Produce the minimal RFC 7396 merge patch that turns `old` into `new`
/// (in plain, formula-resolved terms). Returns None when they are equal.
/// Inherent RFC 7396 limits apply: arrays are replaced wholesale, and a
/// member set to null is indistinguishable from a deleted member.
pub fn merge_patch_diff(old: Option<&Arc<Node>>, new: Option<&Arc<Node>>) -> Option<Value> {
    match (old, new) {
        (None, None) => None,
        (_, None) => Some(Value::Null),
        (None, Some(n)) => Some(to_plain_value(n)),
        (Some(o), Some(n)) => {
            if Arc::ptr_eq(o, n) {
                return None;
            }
            match (&**o, &**n) {
                (Node::Object(a), Node::Object(b)) => {
                    let mut m = Map::new();
                    merge_walk(a, b, |side| match side {
                        MergeSide::Left(k, _) => {
                            m.insert(k.clone(), Value::Null);
                        }
                        MergeSide::Right(k, vb) => {
                            m.insert(k.clone(), to_plain_value(vb));
                        }
                        MergeSide::Both(k, va, vb) => {
                            if let Some(d) = merge_patch_diff(Some(va), Some(vb)) {
                                m.insert(k.clone(), d);
                            }
                        }
                    });
                    if m.is_empty() {
                        None
                    } else {
                        Some(Value::Object(m))
                    }
                }
                _ => {
                    let (po, pn) = (to_plain_value(o), to_plain_value(n));
                    if po == pn {
                        None
                    } else {
                        Some(pn)
                    }
                }
            }
        }
    }
}

/// Visit every node in the tree (skipping formula cached values, which are
/// derived data). The path arrives as a token stack — borrowed object keys,
/// owned array indices — so the traversal allocates nothing per node;
/// materialize a `Pointer` only for the nodes you actually care about.
pub fn walk<'a>(root: &'a Arc<Node>, mut visit: impl FnMut(&[Cow<'a, str>], &'a Arc<Node>)) {
    fn rec<'a>(
        node: &'a Arc<Node>,
        path: &mut Vec<Cow<'a, str>>,
        visit: &mut impl FnMut(&[Cow<'a, str>], &'a Arc<Node>),
    ) {
        visit(path, node);
        match &**node {
            Node::Object(map) => {
                for (k, v) in map {
                    path.push(Cow::Borrowed(k.as_str()));
                    rec(v, path, visit);
                    path.pop();
                }
            }
            Node::Array(items) => {
                for (i, v) in items.iter().enumerate() {
                    path.push(Cow::Owned(i.to_string()));
                    rec(v, path, visit);
                    path.pop();
                }
            }
            _ => {}
        }
    }
    rec(root, &mut Vec::new(), &mut visit);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse(v: &serde_json::Value, hint: Option<&Arc<Node>>) -> Arc<Node> {
        node_from_slice_reconciling(&serde_json::to_vec(v).unwrap(), hint).unwrap()
    }

    #[test]
    fn reconciling_parse_of_identical_body_returns_the_hint_root() {
        let old = from_value(&json!({"a": {"b": [1, 2]}, "s": "x", "n": null}));
        let new = parse(&json!({"a": {"b": [1, 2]}, "s": "x", "n": null}), Some(&old));
        assert!(Arc::ptr_eq(&old, &new));
    }

    #[test]
    fn reconciling_parse_shares_untouched_siblings() {
        let old = from_value(&json!({"keep": {"big": [1, 2, 3]}, "change": 1}));
        let new = parse(&json!({"keep": {"big": [1, 2, 3]}, "change": 2}), Some(&old));
        assert!(!Arc::ptr_eq(&old, &new));
        let (Node::Object(o), Node::Object(n)) = (&*old, &*new) else {
            panic!()
        };
        assert!(Arc::ptr_eq(o.get("keep").unwrap(), n.get("keep").unwrap()));
        assert!(!Arc::ptr_eq(o.get("change").unwrap(), n.get("change").unwrap()));
    }

    #[test]
    fn reconciling_parse_handles_hint_shape_mismatches() {
        // Hint is an object, body is an array (and vice versa): plain parse.
        let old = from_value(&json!({"a": 1}));
        let new = parse(&json!([1, 2]), Some(&old));
        assert!(matches!(&*new, Node::Array(items) if items.len() == 2));
        let old = from_value(&json!([1, 2]));
        let new = parse(&json!({"a": 1}), Some(&old));
        assert!(matches!(&*new, Node::Object(_)));
        // No hint at all matches the plain deserializer.
        let plain: Node = serde_json::from_slice(b"{\"x\":[true,null]}").unwrap();
        let seeded = parse(&json!({"x": [true, null]}), None);
        assert!(nodes_equal(&Arc::new(plain), &seeded));
    }

    #[test]
    fn reconciling_parse_preserves_formula_shapes() {
        let old = from_value(&json!({"f": {"$formula": "/a + 1", "$value": 2}, "a": 1}));
        // Identical formula reuses the whole node.
        let new = parse(&json!({"f": {"$formula": "/a + 1", "$value": 2}, "a": 1}), Some(&old));
        assert!(Arc::ptr_eq(&old, &new));
        // A changed expression yields a fresh formula node.
        let new = parse(&json!({"f": {"$formula": "/a + 2", "$value": 2}, "a": 1}), Some(&old));
        let Node::Object(n) = &*new else { panic!() };
        assert!(matches!(&**n.get("f").unwrap(), Node::Formula(f) if f.expr == "/a + 2"));
        // A formula without hint still parses as a formula.
        let fresh = parse(&json!({"$formula": "sum(/xs)"}), None);
        assert!(matches!(&*fresh, Node::Formula(f) if f.expr == "sum(/xs)"));
    }

    #[test]
    fn reconciling_parse_shares_array_prefixes() {
        let old = from_value(&json!([{"a": 1}, {"b": 2}, {"c": 3}]));
        let new = parse(&json!([{"a": 1}, {"b": 99}, {"c": 3}]), Some(&old));
        let (Node::Array(o), Node::Array(n)) = (&*old, &*new) else {
            panic!()
        };
        assert!(Arc::ptr_eq(&o[0], &n[0]));
        assert!(!Arc::ptr_eq(&o[1], &n[1]));
        assert!(Arc::ptr_eq(&o[2], &n[2]));
    }
}
