//! The typed fast lane: user types in, tree at the core.
//!
//! Two rungs:
//!
//! 1. [`to_node`] serializes any `T: Serialize` **straight into a `Node`
//!    tree** — no intermediate `serde_json::Value`. `Store::put_typed` uses
//!    it; reconciliation against the existing subtree happens in the normal
//!    commit path.
//!
//! 2. [`Document`] (usually via `#[derive(Document)]`) adds a native
//!    old-vs-new struct diff: [`TypedMount`] retains the previously synced
//!    value, and `sync()` compares the new one field-by-field at native
//!    speed, emitting ops only for what changed. Unchanged polls emit no
//!    ops at all; small drifts emit a handful of tiny ops. The tree remains
//!    the source of truth — reads, subscriptions, and formulas never see
//!    the typed layer.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use serde::ser::{self, Serialize};
use serde_json::Number;

use crate::error::Result as StoreResult;
use crate::error::StoreError;
use crate::model::{FormulaNode, Node, Pointer, FORMULA_KEY, FORMULA_VALUE_KEY};
use crate::store::{CommitInfo, Op, Store};

pub use imbl::OrdMap;
pub use visijson_derive::Document;

// ------------------------------------------------------------- to_node

/// Serialize `value` directly into a node tree. Objects shaped
/// `{"$formula": "..."}` become formula nodes, mirroring JSON ingestion.
pub fn to_node<T: Serialize + ?Sized>(value: &T) -> StoreResult<Arc<Node>> {
    value.serialize(NodeSer).map_err(|e| {
        use serde::ser::Error;
        StoreError::Serde(serde_json::Error::custom(e.0))
    })
}

#[derive(Debug)]
pub struct SerError(String);

impl std::fmt::Display for SerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for SerError {}
impl ser::Error for SerError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        SerError(msg.to_string())
    }
}

struct NodeSer;

impl ser::Serializer for NodeSer {
    type Ok = Arc<Node>;
    type Error = SerError;
    type SerializeSeq = SeqSer;
    type SerializeTuple = SeqSer;
    type SerializeTupleStruct = SeqSer;
    type SerializeTupleVariant = ser::Impossible<Arc<Node>, SerError>;
    type SerializeMap = MapSer;
    type SerializeStruct = MapSer;
    type SerializeStructVariant = ser::Impossible<Arc<Node>, SerError>;

    fn serialize_bool(self, v: bool) -> Result<Arc<Node>, SerError> {
        Ok(Arc::new(Node::Bool(v)))
    }
    fn serialize_i8(self, v: i8) -> Result<Arc<Node>, SerError> {
        self.serialize_i64(v as i64)
    }
    fn serialize_i16(self, v: i16) -> Result<Arc<Node>, SerError> {
        self.serialize_i64(v as i64)
    }
    fn serialize_i32(self, v: i32) -> Result<Arc<Node>, SerError> {
        self.serialize_i64(v as i64)
    }
    fn serialize_i64(self, v: i64) -> Result<Arc<Node>, SerError> {
        Ok(Arc::new(Node::Number(Number::from(v))))
    }
    fn serialize_u8(self, v: u8) -> Result<Arc<Node>, SerError> {
        self.serialize_u64(v as u64)
    }
    fn serialize_u16(self, v: u16) -> Result<Arc<Node>, SerError> {
        self.serialize_u64(v as u64)
    }
    fn serialize_u32(self, v: u32) -> Result<Arc<Node>, SerError> {
        self.serialize_u64(v as u64)
    }
    fn serialize_u64(self, v: u64) -> Result<Arc<Node>, SerError> {
        Ok(Arc::new(Node::Number(Number::from(v))))
    }
    fn serialize_f32(self, v: f32) -> Result<Arc<Node>, SerError> {
        self.serialize_f64(v as f64)
    }
    fn serialize_f64(self, v: f64) -> Result<Arc<Node>, SerError> {
        Number::from_f64(v)
            .map(|n| Arc::new(Node::Number(n)))
            .ok_or_else(|| ser::Error::custom("non-finite float"))
    }
    fn serialize_char(self, v: char) -> Result<Arc<Node>, SerError> {
        Ok(Arc::new(Node::String(v.to_string())))
    }
    fn serialize_str(self, v: &str) -> Result<Arc<Node>, SerError> {
        Ok(Arc::new(Node::String(v.to_owned())))
    }
    fn serialize_bytes(self, _v: &[u8]) -> Result<Arc<Node>, SerError> {
        Err(ser::Error::custom("bytes are not representable in JSON"))
    }
    fn serialize_none(self) -> Result<Arc<Node>, SerError> {
        Ok(Arc::new(Node::Null))
    }
    fn serialize_some<T: Serialize + ?Sized>(self, v: &T) -> Result<Arc<Node>, SerError> {
        v.serialize(self)
    }
    fn serialize_unit(self) -> Result<Arc<Node>, SerError> {
        Ok(Arc::new(Node::Null))
    }
    fn serialize_unit_struct(self, _name: &'static str) -> Result<Arc<Node>, SerError> {
        Ok(Arc::new(Node::Null))
    }
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _idx: u32,
        variant: &'static str,
    ) -> Result<Arc<Node>, SerError> {
        Ok(Arc::new(Node::String(variant.to_owned())))
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        v: &T,
    ) -> Result<Arc<Node>, SerError> {
        v.serialize(self)
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _idx: u32,
        variant: &'static str,
        v: &T,
    ) -> Result<Arc<Node>, SerError> {
        let mut m = OrdMap::new();
        m.insert(variant.to_owned(), v.serialize(NodeSer)?);
        Ok(Arc::new(Node::Object(m)))
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<SeqSer, SerError> {
        Ok(SeqSer {
            items: Vec::with_capacity(len.unwrap_or(0)),
        })
    }
    fn serialize_tuple(self, len: usize) -> Result<SeqSer, SerError> {
        self.serialize_seq(Some(len))
    }
    fn serialize_tuple_struct(self, _name: &'static str, len: usize) -> Result<SeqSer, SerError> {
        self.serialize_seq(Some(len))
    }
    fn serialize_tuple_variant(
        self,
        _n: &'static str,
        _i: u32,
        _v: &'static str,
        _l: usize,
    ) -> Result<Self::SerializeTupleVariant, SerError> {
        Err(ser::Error::custom("tuple variants are not supported"))
    }
    fn serialize_map(self, _len: Option<usize>) -> Result<MapSer, SerError> {
        Ok(MapSer {
            map: OrdMap::new(),
            key: None,
        })
    }
    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<MapSer, SerError> {
        self.serialize_map(None)
    }
    fn serialize_struct_variant(
        self,
        _n: &'static str,
        _i: u32,
        _v: &'static str,
        _l: usize,
    ) -> Result<Self::SerializeStructVariant, SerError> {
        Err(ser::Error::custom("struct variants are not supported"))
    }
}

pub struct SeqSer {
    items: Vec<Arc<Node>>,
}

impl ser::SerializeSeq for SeqSer {
    type Ok = Arc<Node>;
    type Error = SerError;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), SerError> {
        self.items.push(v.serialize(NodeSer)?);
        Ok(())
    }
    fn end(self) -> Result<Arc<Node>, SerError> {
        Ok(Arc::new(Node::Array(self.items)))
    }
}
impl ser::SerializeTuple for SeqSer {
    type Ok = Arc<Node>;
    type Error = SerError;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), SerError> {
        ser::SerializeSeq::serialize_element(self, v)
    }
    fn end(self) -> Result<Arc<Node>, SerError> {
        ser::SerializeSeq::end(self)
    }
}
impl ser::SerializeTupleStruct for SeqSer {
    type Ok = Arc<Node>;
    type Error = SerError;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), SerError> {
        ser::SerializeSeq::serialize_element(self, v)
    }
    fn end(self) -> Result<Arc<Node>, SerError> {
        ser::SerializeSeq::end(self)
    }
}

pub struct MapSer {
    map: OrdMap<String, Arc<Node>>,
    key: Option<String>,
}

impl MapSer {
    fn finish(self) -> Arc<Node> {
        // Same formula recognition as JSON ingestion.
        if let Some(expr) = self.map.get(FORMULA_KEY).and_then(|n| match &**n {
            Node::String(s) => Some(s.clone()),
            _ => None,
        }) {
            let cached = self
                .map
                .get(FORMULA_VALUE_KEY)
                .cloned()
                .unwrap_or_else(|| Arc::new(Node::Null));
            return Arc::new(Node::Formula(FormulaNode { expr, cached }));
        }
        Arc::new(Node::Object(self.map))
    }
}

impl ser::SerializeMap for MapSer {
    type Ok = Arc<Node>;
    type Error = SerError;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), SerError> {
        let node = key.serialize(NodeSer)?;
        self.key = Some(match &*node {
            Node::String(s) => s.clone(),
            Node::Number(n) => n.to_string(),
            Node::Bool(b) => b.to_string(),
            _ => return Err(ser::Error::custom("map key must be a string-like scalar")),
        });
        Ok(())
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, v: &T) -> Result<(), SerError> {
        let key = self
            .key
            .take()
            .expect("serialize_value before serialize_key");
        self.map.insert(key, v.serialize(NodeSer)?);
        Ok(())
    }
    fn end(self) -> Result<Arc<Node>, SerError> {
        Ok(self.finish())
    }
}
impl ser::SerializeStruct for MapSer {
    type Ok = Arc<Node>;
    type Error = SerError;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        v: &T,
    ) -> Result<(), SerError> {
        self.map.insert(key.to_owned(), v.serialize(NodeSer)?);
        Ok(())
    }
    fn end(self) -> Result<Arc<Node>, SerError> {
        Ok(self.finish())
    }
}

// ------------------------------------------------------------- Document

/// A type that can materialize itself as a node tree and diff itself
/// against a previous value, emitting minimal ops. Usually derived:
///
/// ```ignore
/// #[derive(Document, Clone)]
/// struct Player { name: String, age: i64 }
/// ```
pub trait Document {
    fn to_node(&self) -> Arc<Node>;
    /// Append ops (paths under the current `path` stack) transforming `old`
    /// into `self`.
    fn diff_into<'a>(&'a self, old: &'a Self, path: &mut DiffPath<'a>, ops: &mut Vec<Op>);
}

/// The path stack threaded through a diff: pushes borrow map keys and
/// field names, so walking an unchanged tree allocates nothing — a pointer
/// string is rendered only when a difference is found.
pub struct DiffPath<'a>(Vec<std::borrow::Cow<'a, str>>);

impl<'a> DiffPath<'a> {
    pub fn from_base(base: &'a Pointer) -> Self {
        DiffPath(
            base.tokens()
                .iter()
                .map(|t| std::borrow::Cow::Borrowed(t.as_str()))
                .collect(),
        )
    }
    pub fn push(&mut self, token: impl Into<std::borrow::Cow<'a, str>>) {
        self.0.push(token.into());
    }
    pub fn pop(&mut self) {
        self.0.pop();
    }
    /// Render as an RFC 6901 pointer string (with `~0`/`~1` escapes).
    pub fn render(&self) -> String {
        let mut s = String::new();
        for t in &self.0 {
            s.push('/');
            for c in t.chars() {
                match c {
                    '~' => s.push_str("~0"),
                    '/' => s.push_str("~1"),
                    c => s.push(c),
                }
            }
        }
        s
    }
}

fn put_op(path: &DiffPath<'_>, node: Arc<Node>) -> Op {
    Op::Put {
        path: path.render(),
        value: node,
    }
}

macro_rules! doc_scalar {
    ($($t:ty => $to:expr),* $(,)?) => {$(
        impl Document for $t {
            fn to_node(&self) -> Arc<Node> {
                #[allow(clippy::redundant_closure_call)]
                ($to)(self)
            }
            fn diff_into<'a>(&'a self, old: &'a Self, path: &mut DiffPath<'a>, ops: &mut Vec<Op>) {
                if self != old {
                    ops.push(put_op(path, self.to_node()));
                }
            }
        }
    )*};
}
doc_scalar!(
    i8 => |v: &i8| Arc::new(Node::Number(Number::from(*v as i64))),
    i16 => |v: &i16| Arc::new(Node::Number(Number::from(*v as i64))),
    i32 => |v: &i32| Arc::new(Node::Number(Number::from(*v as i64))),
    i64 => |v: &i64| Arc::new(Node::Number(Number::from(*v))),
    isize => |v: &isize| Arc::new(Node::Number(Number::from(*v as i64))),
    u8 => |v: &u8| Arc::new(Node::Number(Number::from(*v as u64))),
    u16 => |v: &u16| Arc::new(Node::Number(Number::from(*v as u64))),
    u32 => |v: &u32| Arc::new(Node::Number(Number::from(*v as u64))),
    u64 => |v: &u64| Arc::new(Node::Number(Number::from(*v))),
    usize => |v: &usize| Arc::new(Node::Number(Number::from(*v as u64))),
    f32 => |v: &f32| Number::from_f64(*v as f64).map(|n| Arc::new(Node::Number(n))).unwrap_or_else(|| Arc::new(Node::Null)),
    f64 => |v: &f64| Number::from_f64(*v).map(|n| Arc::new(Node::Number(n))).unwrap_or_else(|| Arc::new(Node::Null)),
    bool => |v: &bool| Arc::new(Node::Bool(*v)),
    String => |v: &String| Arc::new(Node::String(v.clone())),
);

/// `None` is JSON null (matching serde), so `Some -> None` writes null.
impl<T: Document> Document for Option<T> {
    fn to_node(&self) -> Arc<Node> {
        match self {
            Some(v) => v.to_node(),
            None => Arc::new(Node::Null),
        }
    }
    fn diff_into<'a>(&'a self, old: &'a Self, path: &mut DiffPath<'a>, ops: &mut Vec<Op>) {
        match (old, self) {
            (None, None) => {}
            (Some(o), Some(n)) => n.diff_into(o, path, ops),
            (None, Some(n)) => ops.push(put_op(path, n.to_node())),
            (Some(_), None) => ops.push(put_op(path, Arc::new(Node::Null))),
        }
    }
}

/// Same-length vectors diff per index; a length change replaces the array
/// wholesale (mirroring the store's array-shift semantics).
impl<T: Document> Document for Vec<T> {
    fn to_node(&self) -> Arc<Node> {
        Arc::new(Node::Array(self.iter().map(Document::to_node).collect()))
    }
    fn diff_into<'a>(&'a self, old: &'a Self, path: &mut DiffPath<'a>, ops: &mut Vec<Op>) {
        if self.len() != old.len() {
            ops.push(put_op(path, self.to_node()));
            return;
        }
        for (i, (n, o)) in self.iter().zip(old).enumerate() {
            path.push(i.to_string());
            n.diff_into(o, path, ops);
            path.pop();
        }
    }
}

/// Sorted lockstep diff over two key-sorted iterators of map entries.
fn diff_sorted_entries<'a, V: Document + 'a>(
    old: impl Iterator<Item = (&'a String, &'a V)>,
    new: impl Iterator<Item = (&'a String, &'a V)>,
    path: &mut DiffPath<'a>,
    ops: &mut Vec<Op>,
) {
    let mut io = old.peekable();
    let mut in_ = new.peekable();
    loop {
        match (io.peek(), in_.peek()) {
            (Some(&(ko, _)), Some(&(kn, _))) => match ko.cmp(kn) {
                std::cmp::Ordering::Less => {
                    path.push(ko.as_str());
                    ops.push(Op::Remove {
                        path: path.render(),
                    });
                    path.pop();
                    io.next();
                }
                std::cmp::Ordering::Greater => {
                    let (k, v) = in_.next().unwrap();
                    path.push(k.as_str());
                    ops.push(put_op(path, v.to_node()));
                    path.pop();
                }
                std::cmp::Ordering::Equal => {
                    let (_, vo) = io.next().unwrap();
                    let (k, vn) = in_.next().unwrap();
                    path.push(k.as_str());
                    vn.diff_into(vo, path, ops);
                    path.pop();
                }
            },
            (Some(_), None) => {
                let (k, _) = io.next().unwrap();
                path.push(k.as_str());
                ops.push(Op::Remove {
                    path: path.render(),
                });
                path.pop();
            }
            (None, Some(_)) => {
                let (k, v) = in_.next().unwrap();
                path.push(k.as_str());
                ops.push(put_op(path, v.to_node()));
                path.pop();
            }
            (None, None) => break,
        }
    }
}

impl<V: Document> Document for BTreeMap<String, V> {
    fn to_node(&self) -> Arc<Node> {
        Arc::new(Node::Object(
            self.iter().map(|(k, v)| (k.clone(), v.to_node())).collect(),
        ))
    }
    fn diff_into<'a>(&'a self, old: &'a Self, path: &mut DiffPath<'a>, ops: &mut Vec<Op>) {
        diff_sorted_entries(old.iter(), self.iter(), path, ops);
    }
}

impl<V: Document> Document for HashMap<String, V> {
    fn to_node(&self) -> Arc<Node> {
        Arc::new(Node::Object(
            self.iter().map(|(k, v)| (k.clone(), v.to_node())).collect(),
        ))
    }
    fn diff_into<'a>(&'a self, old: &'a Self, path: &mut DiffPath<'a>, ops: &mut Vec<Op>) {
        let mut o: Vec<_> = old.iter().collect();
        let mut n: Vec<_> = self.iter().collect();
        o.sort_by_key(|(k, _)| *k);
        n.sort_by_key(|(k, _)| *k);
        diff_sorted_entries(o.into_iter(), n.into_iter(), path, ops);
    }
}

// ------------------------------------------------------------- TypedMount

/// A typed view onto a document path. Retains the last synced value so each
/// `sync` is a native old-vs-new comparison producing minimal ops — the
/// Go-style fast path. The store itself stays untyped; this handle is the
/// only thing holding a `T`.
pub struct TypedMount<T> {
    store: Arc<Store>,
    base: Pointer,
    last: Option<T>,
}

impl<T: Document> TypedMount<T> {
    pub fn new(store: Arc<Store>, path: &str) -> StoreResult<Self> {
        Ok(TypedMount {
            store,
            base: Pointer::parse(path)?,
            last: None,
        })
    }

    /// Sync `new` as the current value at this mount's path. The first sync
    /// PUTs the whole value; later syncs diff natively against the retained
    /// copy and commit only the differences (no commit at all when equal).
    pub fn sync(&mut self, new: T) -> StoreResult<CommitInfo> {
        let ops = match &self.last {
            None => vec![Op::Put {
                path: self.base.to_string(),
                value: new.to_node(),
            }],
            Some(old) => {
                let mut ops = Vec::new();
                let mut path = DiffPath::from_base(&self.base);
                new.diff_into(old, &mut path, &mut ops);
                ops
            }
        };
        let info = if ops.is_empty() {
            CommitInfo {
                version: self.store.version(),
                changed_paths: Vec::new(),
            }
        } else {
            self.store.commit(ops, &[])?
        };
        self.last = Some(new);
        Ok(info)
    }

    /// The last value handed to `sync`, if any.
    pub fn last(&self) -> Option<&T> {
        self.last.as_ref()
    }
}
