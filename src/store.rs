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
use crate::meta::{ModIndex, Stamp};
use crate::model::{
    self, empty_object, from_value, get_at, insert_at, nodes_equal, remove_at, set_at,
    to_plain_value, to_raw_value, Node, Pointer,
};
use crate::persistence::{Durability, Persistence, WalRecord};
use crate::stats::Stats;
use crate::subscriptions::{CommitEvent, Hub, Pending};

/// An immutable committed state: version + root of the persistent tree,
/// plus the index of formula locations in that root (maintained
/// incrementally so commits never scan the whole tree for formulas) and
/// the per-path modification index behind HTTP validators.
#[derive(Debug)]
pub struct Committed {
    pub version: u64,
    /// Wall-clock time of this commit, milliseconds since the epoch.
    /// Monotonic with respect to earlier commits even if the clock isn't.
    pub time_ms: u64,
    pub root: Arc<Node>,
    pub formulas: formulas::FormulaIndex,
    pub mods: ModIndex,
}

impl Committed {
    pub fn stamp(&self) -> Stamp {
        Stamp::new(self.version, self.time_ms)
    }
}

/// Contention backoff: yield for the first couple of retries, then sleep
/// exponentially longer (40µs .. 2.5ms) so hot-key contention can't starve
/// one writer through its whole retry budget.
fn backoff(attempt: u32) {
    if attempt >= 2 {
        let micros = 10u64 << attempt.min(8);
        std::thread::sleep(std::time::Duration::from_micros(micros));
    } else {
        std::thread::yield_now();
    }
}

/// Wall-clock milliseconds since the epoch.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A replayable write operation — the unit recorded in the WAL. Values are
/// `Node` trees directly (serde parses request/WAL JSON straight into them;
/// the wire format is unchanged raw JSON).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum Op {
    /// Set the value at `path`, creating intermediate objects as needed.
    Put { path: String, value: Arc<Node> },
    /// RFC 6902 `add`: like Put, but array indices insert rather than replace.
    Insert { path: String, value: Arc<Node> },
    /// Remove the value at `path`.
    Remove { path: String },
    /// Append `value` to the array at `path` (creating the array if absent).
    Append { path: String, value: Arc<Node> },
    /// Apply `value` as an RFC 7396 JSON Merge Patch at `path`.
    Merge { path: String, value: Arc<Node> },
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
    /// What a commit ack promises about its WAL record (see `Durability`).
    pub durability: Durability,
    /// Max optimistic transaction retries before giving up.
    pub max_retries: u32,
}

impl Default for StoreConfig {
    fn default() -> Self {
        StoreConfig {
            data_dir: None,
            durability: Durability::Sync,
            max_retries: 64,
        }
    }
}

pub struct Store {
    current: ArcSwap<Committed>,
    hub: Arc<Hub>,
    persistence: Option<Persistence>,
    max_retries: u32,
    stats: Arc<Stats>,
}

/// A `Put` diff reporting more distinct paths than this collapses to the
/// put's own path — keeps changed-path lists bounded for huge replacements.
const MAX_DIFF_PATHS: usize = 64;

/// Apply ops in order against `root`, returning the new root, the set of
/// changed paths, and the subtree roots the ops wrote (for formula-index
/// maintenance — coarser than `changed`, which is a precise diff). Pure —
/// shared by live commits and WAL replay.
pub fn apply_ops(
    root: &Arc<Node>,
    ops: &[Op],
) -> Result<(Arc<Node>, BTreeSet<Pointer>, Vec<Pointer>)> {
    let mut r = root.clone();
    let mut changed = BTreeSet::new();
    let mut scan_roots = Vec::with_capacity(ops.len());
    for op in ops {
        match op {
            Op::Put { path, value } => {
                let p = Pointer::parse(path)?;
                let old = get_at(&r, &p);
                // Reconcile a subtree replacement against what it replaces:
                // unchanged entries keep the old Arcs, so the diff below (and
                // everything downstream) short-circuits on pointer equality.
                let new = match &old {
                    Some(o) => model::reconcile(o, value),
                    None => value.clone(),
                };
                r = set_at(&r, &p, new.clone())?;
                scan_roots.push(p.clone());
                record_replacement(&mut changed, old.as_ref(), &new, p);
            }
            Op::Merge { path, value } => {
                let p = Pointer::parse(path)?;
                let old = get_at(&r, &p);
                let new = model::merge_value(old.as_ref(), value);
                let new = match &old {
                    Some(o) => model::reconcile(o, &new),
                    None => new,
                };
                r = set_at(&r, &p, new.clone())?;
                scan_roots.push(p.clone());
                record_replacement(&mut changed, old.as_ref(), &new, p);
            }
            Op::Insert { path, value } => {
                let p = Pointer::parse(path)?;
                let array_parent = parent_array(&r, &p);
                r = insert_at(&r, &p, value.clone())?;
                // Inserting into an array shifts every later sibling, so the
                // whole array changed — not just the inserted index.
                let coarse = array_parent.unwrap_or(p);
                changed.insert(coarse.clone());
                scan_roots.push(coarse);
            }
            Op::Remove { path } => {
                let p = Pointer::parse(path)?;
                let array_parent = parent_array(&r, &p);
                r = remove_at(&r, &p)?;
                // Same shift logic as Insert for array element removal.
                let coarse = array_parent.unwrap_or(p);
                changed.insert(coarse.clone());
                scan_roots.push(coarse);
            }
            Op::Append { path, value } => {
                let p = Pointer::parse(path)?;
                match get_at(&r, &p) {
                    None => {
                        r = set_at(&r, &p, Arc::new(Node::Array(vec![value.clone()])))?;
                        scan_roots.push(p.clone());
                        changed.insert(p);
                    }
                    Some(node) => match &*node {
                        Node::Array(items) => {
                            let idx = items.len();
                            r = set_at(&r, &p.push("-"), value.clone())?;
                            let elem = p.push(idx.to_string());
                            changed.insert(elem.clone());
                            scan_roots.push(elem);
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
    Ok((r, changed, scan_roots))
}

/// Record the changed paths for a subtree replacement at `p`: a structural
/// diff against the old subtree when one existed (collapsing to `p` past
/// MAX_DIFF_PATHS), or `p` itself for a brand-new subtree.
fn record_replacement(
    changed: &mut BTreeSet<Pointer>,
    old: Option<&Arc<Node>>,
    new: &Arc<Node>,
    p: Pointer,
) {
    match old {
        Some(old) => {
            let mut diff = Vec::new();
            model::diff_paths(old, new, &p, &mut diff);
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

/// Derive the smallest op list that reproduces `changed` against `root`:
/// one raw-value Put (or Remove, for paths now absent) per changed path,
/// with descendants of an already-kept ancestor pruned — the ancestor's
/// value subsumes them, and sorted order puts descendants right after it.
/// Replaying these against the previous root yields the identical tree
/// (formula caches are recomputed by replay's re-evaluation).
fn minimal_ops(root: &Arc<Node>, changed: &BTreeSet<Pointer>) -> Vec<Op> {
    let mut ops = Vec::with_capacity(changed.len());
    let mut last_kept: Option<&Pointer> = None;
    for p in changed {
        if last_kept.is_some_and(|k| k.is_prefix_of(p)) {
            continue;
        }
        last_kept = Some(p);
        ops.push(match get_at(root, p) {
            Some(n) => Op::Put {
                path: p.to_string(),
                value: n,
            },
            None => Op::Remove { path: p.to_string() },
        });
    }
    ops
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
        let stats = Arc::new(Stats::default());
        let (mut persistence, rec) = match &cfg.data_dir {
            Some(dir) => {
                let p = Persistence::open(dir, cfg.durability, stats.clone())?;
                let rec = p.recover()?;
                (Some(p), rec)
            }
            None => (None, crate::persistence::Recovered::empty()),
        };
        let committed = Arc::new(Committed {
            version: rec.version,
            time_ms: rec.time_ms,
            root: rec.root,
            formulas: rec.formulas,
            mods: rec.mods,
        });
        let hub = Arc::new(Hub::new(committed.clone(), persistence.is_some()));
        if let Some(p) = persistence.as_mut() {
            p.start_writer(hub.clone(), committed.version)?;
        }
        Ok(Arc::new(Store {
            current: ArcSwap::from(committed),
            hub,
            persistence,
            max_retries: cfg.max_retries,
            stats,
        }))
    }

    /// Point-in-time operational counters.
    pub fn stats(&self) -> crate::stats::StatsSnapshot {
        self.stats.snapshot()
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

    /// Read the node at `path` — no serialization; serialize it plain with
    /// `model::PlainView` or raw via `Node`'s own `Serialize`.
    pub fn get_node(&self, path: &str) -> Result<Option<Arc<Node>>> {
        let p = Pointer::parse(path)?;
        Ok(get_at(&self.current.load().root, &p))
    }

    pub fn put(&self, path: &str, value: Value) -> Result<CommitInfo> {
        self.put_node(path, from_value(&value))
    }

    pub fn put_node(&self, path: &str, node: Arc<Node>) -> Result<CommitInfo> {
        self.commit(
            vec![Op::Put {
                path: path.into(),
                value: node,
            }],
            &[],
        )
    }

    /// Serialize any `T: Serialize` straight into a node tree (no
    /// intermediate `serde_json::Value`) and PUT it at `path`.
    pub fn put_typed<T: serde::Serialize>(&self, path: &str, value: &T) -> Result<CommitInfo> {
        self.put_node(path, crate::typed::to_node(value)?)
    }

    pub fn remove(&self, path: &str) -> Result<CommitInfo> {
        self.commit(vec![Op::Remove { path: path.into() }], &[])
    }

    pub fn append(&self, path: &str, value: Value) -> Result<CommitInfo> {
        self.append_node(path, from_value(&value))
    }

    pub fn append_node(&self, path: &str, node: Arc<Node>) -> Result<CommitInfo> {
        self.commit(
            vec![Op::Append {
                path: path.into(),
                value: node,
            }],
            &[],
        )
    }

    /// Apply an RFC 7396 JSON Merge Patch at `path`.
    pub fn merge(&self, path: &str, patch: Value) -> Result<CommitInfo> {
        self.merge_node(path, from_value(&patch))
    }

    pub fn merge_node(&self, path: &str, patch: Arc<Node>) -> Result<CommitInfo> {
        self.commit(
            vec![Op::Merge {
                path: path.into(),
                value: patch,
            }],
            &[],
        )
    }

    /// Commit a batch of ops with preconditions. Conditions are re-validated
    /// against the current root inside the CAS loop, so a successful commit
    /// guarantees they held at commit time. `Conflict` means a condition
    /// failed; CAS races are retried internally and never surface.
    pub fn commit(&self, ops: Vec<Op>, conditions: &[Condition]) -> Result<CommitInfo> {
        for attempt in 0..self.max_retries {
            let cur = self.current.load_full();
            check_conditions(&cur.root, conditions)?;
            // Commit time never runs backwards, whatever the wall clock does.
            let time_ms = now_ms().max(cur.time_ms);
            match self.try_commit(&cur, &ops, time_ms)? {
                Some(info) => return Ok(info),
                None => backoff(attempt), // CAS lost; replay against new root
            }
        }
        Err(StoreError::RetryLimit(self.max_retries))
    }

    /// When was the subtree at `path` last modified? Over-estimates rather
    /// than under-estimates for paths the modification index has aged out,
    /// so it is always safe as an HTTP validator.
    pub fn last_modified(&self, path: &str) -> Result<Stamp> {
        let p = Pointer::parse(path)?;
        Ok(self.current.load().mods.last_modified(&p))
    }

    /// Build the new root from `cur`, re-evaluate formulas, CAS. Returns
    /// `Ok(None)` if the CAS lost the race.
    fn try_commit(
        &self,
        cur: &Arc<Committed>,
        ops: &[Op],
        time_ms: u64,
    ) -> Result<Option<CommitInfo>> {
        // A broken WAL fail-stops writes before any state changes hands —
        // a commit that could never recover must not CAS in.
        self.hub.check_writable()?;
        if let Some(persistence) = &self.persistence {
            persistence.check_writable()?;
        }
        let (root, changed, scan_roots) = apply_ops(&cur.root, ops)?;
        let formulas = formulas::update_index(&cur.formulas, &scan_roots, &root)?;
        let (root, fchanged) = formulas::reevaluate(&root, &changed, &formulas)?;
        // A commit that changed nothing (e.g. re-PUTting an identical
        // fetch) is elided entirely: no version bump, no WAL, no event.
        if changed.is_empty() && fchanged.is_empty() {
            Stats::bump(&self.stats.elided);
            return Ok(Some(CommitInfo {
                version: cur.version,
                changed_paths: Vec::new(),
            }));
        }
        // WAL the effect, not the request: a whole-subtree PUT that changed
        // three fields records three small ops, not the whole subtree.
        let wal_ops = minimal_ops(&root, &changed);
        let mut all = changed;
        all.extend(fchanged);
        let stamp = Stamp::new(cur.version + 1, time_ms);
        let next = Arc::new(Committed {
            version: stamp.version,
            time_ms,
            root,
            formulas,
            mods: cur.mods.record(stamp, all.iter()),
        });
        let prev = self.current.compare_and_swap(cur, next.clone());
        if !Arc::ptr_eq(&prev, cur) {
            Stats::bump(&self.stats.cas_losses);
            return Ok(None);
        }
        Stats::bump(&self.stats.commits);
        let changed_paths = Arc::new(all.iter().map(|p| p.to_string()).collect::<Vec<_>>());
        let pending = Pending {
            committed: next.clone(),
            changed_paths: changed_paths.clone(),
            ops: wal_ops,
        };
        // The sink hands the record (and its event) to the WAL writer in
        // version order; broadcast happens once the batch is durable. For
        // in-memory stores the sink is a no-op and the Hub broadcasts.
        self.hub.finalize(pending, |p, event| {
            if let Some(persistence) = &self.persistence {
                let record = WalRecord {
                    version: p.committed.version,
                    time_ms: p.committed.time_ms,
                    ops: p.ops.clone(),
                };
                persistence.enqueue(&record, event.clone()).inspect_err(|e| {
                    tracing::error!(version = p.committed.version, error = %e,
                        "WAL enqueue failed; store is fail-stopped for writes");
                })?;
            }
            Ok(())
        })?;
        // Group commit: block until our record reaches the configured
        // durability level (batch-mates wake together off one fsync).
        if let Some(persistence) = &self.persistence {
            persistence.wait_durable(next.version)?;
        }
        tracing::debug!(version = next.version, changed = ?changed_paths, "commit");
        Ok(Some(CommitInfo {
            version: next.version,
            changed_paths: changed_paths.as_ref().clone(),
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
                    backoff(attempt);
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
            let time_ms = now_ms().max(cur.time_ms);
            match self.try_commit(&cur, ops, time_ms)? {
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

    /// Write a snapshot and retire the WAL segments it supersedes. Returns
    /// the snapshotted version, or `None` if persistence is disabled.
    ///
    /// Commits are never stalled for the duration of the document: the
    /// publication lock is held only to pin the latest enqueued state and
    /// mark a segment rotation at exactly that boundary; serializing the
    /// (immutable, `Arc`-pinned) tree, fsync, and rename all run outside
    /// it, and sealed segments are deleted only after the rename lands — a
    /// crash at any point recovers to the same state.
    pub fn snapshot(&self) -> Result<Option<u64>> {
        let Some(persistence) = &self.persistence else {
            return Ok(None);
        };
        let _one_at_a_time = persistence.snapshot_guard();
        let t = std::time::Instant::now();
        let (pinned, rotation) = self.hub.latest_enqueued(|enqueued| {
            let rotation = persistence.request_rotation()?;
            Ok::<_, StoreError>((enqueued.clone(), rotation))
        })?;
        let sealed = rotation
            .recv()
            .map_err(|_| StoreError::WalBroken("WAL writer thread exited".into()))??;
        persistence.write_snapshot(pinned.version, pinned.time_ms, &pinned.root)?;
        persistence.prune(&sealed);
        Stats::bump(&self.stats.snapshots);
        Stats::add(&self.stats.snapshot_micros, t.elapsed().as_micros() as u64);
        tracing::info!(version = pinned.version, "snapshot written");
        Ok(Some(pinned.version))
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
            value: from_value(&value),
        })
    }

    pub fn remove(&mut self, path: &str) -> Result<()> {
        self.apply(Op::Remove { path: path.into() })
    }

    pub fn append(&mut self, path: &str, value: Value) -> Result<()> {
        self.apply(Op::Append {
            path: path.into(),
            value: from_value(&value),
        })
    }

    fn apply(&mut self, op: Op) -> Result<()> {
        let (working, _, _) = apply_ops(&self.working, std::slice::from_ref(&op))?;
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
