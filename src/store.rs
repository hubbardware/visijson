//! The document store: an `ArcSwap` of the current committed root.
//!
//! Reads are lock-free (`load` the current `Committed`). Writes build a new
//! root by path-copy from the current one, re-evaluate affected formulas,
//! then attempt a CAS of the root pointer; on CAS failure the write replays
//! against the new current root. Optimistic transactions additionally record
//! a read-set that is validated before the rebased replay — any invalidated
//! read aborts with `Conflict` and the transaction closure is retried.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Result, StoreError};
use crate::formulas;
use crate::model::{
    self, empty_object, from_value, get_at, insert_at, nodes_equal, remove_at, set_at,
    to_plain_value, to_raw_value, Node, Pointer,
};
use crate::persistence::{Persistence, WalRecord};
use crate::subscriptions::{CommitEvent, Hub, Pending};

/// An immutable committed state: version + root of the persistent tree.
#[derive(Debug)]
pub struct Committed {
    pub version: u64,
    pub root: Arc<Node>,
}

/// A replayable write operation — the unit recorded in the WAL.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum Op {
    /// Set the value at `path`, creating intermediate objects as needed.
    Put { path: String, value: Value },
    /// RFC 6902 `add`: like Put, but array indices insert rather than replace.
    Insert { path: String, value: Value },
    /// Remove the value at `path`.
    Remove { path: String },
    /// Append `value` to the array at `path` (creating the array if absent).
    Append { path: String, value: Value },
}

/// A precondition for a transactional commit: the plain value at `path` must
/// equal `expected` (`None` = the path must be absent).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Condition {
    pub path: String,
    #[serde(default)]
    pub expected: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CommitInfo {
    pub version: u64,
    pub changed_paths: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// Directory for WAL + snapshots; `None` disables persistence.
    pub data_dir: Option<PathBuf>,
    /// fsync the WAL on every commit.
    pub fsync: bool,
    /// Max optimistic transaction retries before giving up.
    pub max_retries: u32,
}

impl Default for StoreConfig {
    fn default() -> Self {
        StoreConfig {
            data_dir: None,
            fsync: true,
            max_retries: 64,
        }
    }
}

pub struct Store {
    current: ArcSwap<Committed>,
    hub: Hub,
    persistence: Option<Persistence>,
    max_retries: u32,
}

/// A `Put` diff reporting more distinct paths than this collapses to the
/// put's own path — keeps changed-path lists bounded for huge replacements.
const MAX_DIFF_PATHS: usize = 64;

/// Apply ops in order against `root`, returning the new root and the set of
/// changed paths. Pure — shared by live commits and WAL replay.
pub fn apply_ops(root: &Arc<Node>, ops: &[Op]) -> Result<(Arc<Node>, BTreeSet<Pointer>)> {
    let mut r = root.clone();
    let mut changed = BTreeSet::new();
    for op in ops {
        match op {
            Op::Put { path, value } => {
                let p = Pointer::parse(path)?;
                let new = from_value(value);
                let old = get_at(&r, &p);
                r = set_at(&r, &p, new.clone())?;
                match old {
                    // Replacing an existing subtree: record only what
                    // actually differs, so rewriting a mostly-identical
                    // object doesn't flag untouched fields as changed.
                    Some(old) => {
                        let mut diff = Vec::new();
                        model::diff_paths(&old, &new, &p, &mut diff);
                        if diff.len() > MAX_DIFF_PATHS {
                            changed.insert(p);
                        } else {
                            changed.extend(diff);
                        }
                    }
                    None => {
                        changed.insert(p);
                    }
                }
            }
            Op::Insert { path, value } => {
                let p = Pointer::parse(path)?;
                let array_parent = parent_array(&r, &p);
                r = insert_at(&r, &p, from_value(value))?;
                // Inserting into an array shifts every later sibling, so the
                // whole array changed — not just the inserted index.
                changed.insert(array_parent.unwrap_or(p));
            }
            Op::Remove { path } => {
                let p = Pointer::parse(path)?;
                let array_parent = parent_array(&r, &p);
                r = remove_at(&r, &p)?;
                // Same shift logic as Insert for array element removal.
                changed.insert(array_parent.unwrap_or(p));
            }
            Op::Append { path, value } => {
                let p = Pointer::parse(path)?;
                match get_at(&r, &p) {
                    None => {
                        r = set_at(&r, &p, Arc::new(Node::Array(vec![from_value(value)])))?;
                        changed.insert(p);
                    }
                    Some(node) => match &*node {
                        Node::Array(items) => {
                            let idx = items.len();
                            r = set_at(&r, &p.push("-"), from_value(value))?;
                            changed.insert(p.push(idx.to_string()));
                        }
                        _ => {
                            return Err(StoreError::TypeMismatch {
                                path: p.to_string(),
                                msg: "append target is not an array".into(),
                            })
                        }
                    },
                }
            }
        }
    }
    Ok((r, changed))
}

/// If `p` addresses an element of an array, return the array's path.
fn parent_array(root: &Arc<Node>, p: &Pointer) -> Option<Pointer> {
    let parent = p.parent()?;
    matches!(get_at(root, &parent).as_deref(), Some(Node::Array(_))).then_some(parent)
}

fn check_conditions(root: &Arc<Node>, conditions: &[Condition]) -> Result<()> {
    for c in conditions {
        let p = Pointer::parse(&c.path)?;
        let now = get_at(root, &p).map(|n| to_plain_value(&n));
        let ok = match (&c.expected, &now) {
            (Some(e), Some(v)) => e == v,
            (None, None) => true,
            _ => false,
        };
        if !ok {
            return Err(StoreError::Conflict(format!(
                "condition failed at {}: expected {:?}, found {:?}",
                c.path, c.expected, now
            )));
        }
    }
    Ok(())
}

impl Store {
    pub fn open(cfg: StoreConfig) -> Result<Arc<Store>> {
        let (persistence, version, root) = match &cfg.data_dir {
            Some(dir) => {
                let p = Persistence::open(dir, cfg.fsync)?;
                let (version, root) = p.recover()?;
                (Some(p), version, root)
            }
            None => (None, 0, empty_object()),
        };
        let committed = Arc::new(Committed { version, root });
        Ok(Arc::new(Store {
            current: ArcSwap::from(committed.clone()),
            hub: Hub::new(committed),
            persistence,
            max_retries: cfg.max_retries,
        }))
    }

    pub fn in_memory() -> Arc<Store> {
        Store::open(StoreConfig::default()).expect("in-memory store cannot fail to open")
    }

    /// Latest committed state (lock-free).
    pub fn head(&self) -> Arc<Committed> {
        self.current.load_full()
    }

    pub fn version(&self) -> u64 {
        self.current.load().version
    }

    /// Read the plain (formula-resolved) value at `path`.
    pub fn get(&self, path: &str) -> Result<Option<Value>> {
        let p = Pointer::parse(path)?;
        Ok(get_at(&self.current.load().root, &p).map(|n| to_plain_value(&n)))
    }

    /// Read the raw value at `path` (formulas as `{"$formula", "$value"}`).
    pub fn get_raw(&self, path: &str) -> Result<Option<Value>> {
        let p = Pointer::parse(path)?;
        Ok(get_at(&self.current.load().root, &p).map(|n| to_raw_value(&n)))
    }

    pub fn put(&self, path: &str, value: Value) -> Result<CommitInfo> {
        self.commit(
            vec![Op::Put {
                path: path.into(),
                value,
            }],
            &[],
        )
    }

    pub fn remove(&self, path: &str) -> Result<CommitInfo> {
        self.commit(vec![Op::Remove { path: path.into() }], &[])
    }

    pub fn append(&self, path: &str, value: Value) -> Result<CommitInfo> {
        self.commit(
            vec![Op::Append {
                path: path.into(),
                value,
            }],
            &[],
        )
    }

    /// Commit a batch of ops with preconditions. Conditions are re-validated
    /// against the current root inside the CAS loop, so a successful commit
    /// guarantees they held at commit time. `Conflict` means a condition
    /// failed; CAS races are retried internally and never surface.
    pub fn commit(&self, ops: Vec<Op>, conditions: &[Condition]) -> Result<CommitInfo> {
        loop {
            let cur = self.current.load_full();
            check_conditions(&cur.root, conditions)?;
            match self.try_commit(&cur, &ops)? {
                Some(info) => return Ok(info),
                None => continue, // CAS lost; replay against new root
            }
        }
    }

    /// Build the new root from `cur`, re-evaluate formulas, CAS. Returns
    /// `Ok(None)` if the CAS lost the race.
    fn try_commit(&self, cur: &Arc<Committed>, ops: &[Op]) -> Result<Option<CommitInfo>> {
        let (root, changed) = apply_ops(&cur.root, ops)?;
        let (root, fchanged) = formulas::reevaluate(&root, &changed)?;
        let mut all = changed;
        all.extend(fchanged);
        let next = Arc::new(Committed {
            version: cur.version + 1,
            root,
        });
        let prev = self.current.compare_and_swap(cur, next.clone());
        if !Arc::ptr_eq(&prev, cur) {
            return Ok(None);
        }
        let changed_paths: Vec<String> = all.iter().map(|p| p.to_string()).collect();
        let pending = Pending {
            committed: next.clone(),
            changed_paths: changed_paths.clone(),
            ops: ops.to_vec(),
        };
        self.hub.finalize(pending, |p| {
            if let Some(persistence) = &self.persistence {
                let record = WalRecord {
                    version: p.committed.version,
                    ops: p.ops.clone(),
                };
                if let Err(e) = persistence.append(&record) {
                    tracing::error!(version = p.committed.version, error = %e, "WAL append failed");
                }
            }
        });
        tracing::debug!(version = next.version, changed = ?changed_paths, "commit");
        Ok(Some(CommitInfo {
            version: next.version,
            changed_paths,
        }))
    }

    /// Run an optimistic transaction. The closure reads and writes through
    /// `Txn`; at commit, the recorded read-set is validated against the
    /// current root and the buffered ops replay on top of it. On conflict the
    /// closure re-runs against a fresh snapshot. Returns the closure result,
    /// commit info, and how many retries were needed.
    pub fn transaction<T>(
        &self,
        f: impl Fn(&mut Txn) -> Result<T>,
    ) -> Result<(T, CommitInfo, u32)> {
        for attempt in 0..self.max_retries {
            let base = self.current.load_full();
            let mut txn = Txn {
                base: base.clone(),
                working: base.root.clone(),
                reads: Vec::new(),
                ops: Vec::new(),
            };
            let out = f(&mut txn)?;
            if txn.ops.is_empty() {
                return Ok((
                    out,
                    CommitInfo {
                        version: base.version,
                        changed_paths: Vec::new(),
                    },
                    attempt,
                ));
            }
            match self.commit_txn(&base, &txn.reads, &txn.ops) {
                Ok(info) => return Ok((out, info, attempt)),
                Err(StoreError::Conflict(reason)) => {
                    tracing::debug!(attempt, %reason, "transaction conflict, retrying");
                    // Back off so hot-key contention can't starve one writer
                    // through the whole retry budget.
                    if attempt >= 2 {
                        let micros = 10u64 << attempt.min(8); // 40µs .. 2.5ms
                        std::thread::sleep(std::time::Duration::from_micros(micros));
                    } else {
                        std::thread::yield_now();
                    }
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(StoreError::RetryLimit(self.max_retries))
    }

    fn commit_txn(
        &self,
        base: &Arc<Committed>,
        reads: &[(Pointer, Option<Arc<Node>>)],
        ops: &[Op],
    ) -> Result<CommitInfo> {
        loop {
            let cur = self.current.load_full();
            if !Arc::ptr_eq(&cur, base) {
                for (p, seen) in reads {
                    let now = get_at(&cur.root, p);
                    let unchanged = match (seen, &now) {
                        (None, None) => true,
                        (Some(a), Some(b)) => nodes_equal(a, b),
                        _ => false,
                    };
                    if !unchanged {
                        return Err(StoreError::Conflict(format!("read invalidated at {p}")));
                    }
                }
            }
            match self.try_commit(&cur, ops)? {
                Some(info) => return Ok(info),
                None => continue,
            }
        }
    }

    /// Consistent subscription: latest published snapshot + ordered events.
    pub fn subscribe(
        &self,
    ) -> (
        Arc<Committed>,
        tokio::sync::broadcast::Receiver<Arc<CommitEvent>>,
    ) {
        self.hub.subscribe()
    }

    pub fn hub(&self) -> &Hub {
        &self.hub
    }

    /// Write a snapshot of the latest published state and rotate the WAL.
    /// Returns the snapshotted version, or `None` if persistence is disabled.
    pub fn snapshot(&self) -> Result<Option<u64>> {
        let Some(persistence) = &self.persistence else {
            return Ok(None);
        };
        // Publication (and thus WAL append) is locked out while we write the
        // snapshot and truncate the WAL, so the rotation point is exact.
        self.hub.with_publish_lock(|published| {
            persistence.write_snapshot(published.version, &published.root)?;
            tracing::info!(version = published.version, "snapshot written");
            Ok(Some(published.version))
        })
    }
}

/// Handle passed to a transaction closure: reads record the base value for
/// commit-time validation; writes are buffered and applied to a private
/// working root so the closure observes its own effects.
pub struct Txn {
    base: Arc<Committed>,
    working: Arc<Node>,
    reads: Vec<(Pointer, Option<Arc<Node>>)>,
    ops: Vec<Op>,
}

impl Txn {
    /// Read the plain value at `path`, recording the read for validation.
    pub fn get(&mut self, path: &str) -> Result<Option<Value>> {
        let p = Pointer::parse(path)?;
        self.reads.push((p.clone(), get_at(&self.base.root, &p)));
        Ok(get_at(&self.working, &p).map(|n| to_plain_value(&n)))
    }

    pub fn put(&mut self, path: &str, value: Value) -> Result<()> {
        self.apply(Op::Put {
            path: path.into(),
            value,
        })
    }

    pub fn remove(&mut self, path: &str) -> Result<()> {
        self.apply(Op::Remove { path: path.into() })
    }

    pub fn append(&mut self, path: &str, value: Value) -> Result<()> {
        self.apply(Op::Append {
            path: path.into(),
            value,
        })
    }

    fn apply(&mut self, op: Op) -> Result<()> {
        let (working, _) = apply_ops(&self.working, std::slice::from_ref(&op))?;
        self.working = working;
        self.ops.push(op);
        Ok(())
    }

    pub fn base_version(&self) -> u64 {
        self.base.version
    }
}

/// Convenience: structural sharing check used by tests — true when the two
/// roots share the subtree at `path` by pointer identity.
pub fn shares_subtree(a: &Arc<Committed>, b: &Arc<Committed>, path: &str) -> bool {
    let p = match Pointer::parse(path) {
        Ok(p) => p,
        Err(_) => return false,
    };
    match (get_at(&a.root, &p), get_at(&b.root, &p)) {
        (Some(x), Some(y)) => Arc::ptr_eq(&x, &y),
        _ => false,
    }
}

pub use model::to_plain_value as plain;
