//! Ordered change-event publication and path subscriptions.
//!
//! Commits CAS the root lock-free, so two commits may race to publish their
//! events. The `Hub` restores order: each finalized commit is buffered by
//! version and flushed strictly in version order, under a mutex, into a
//! `tokio::sync::broadcast` channel. The same mutex guards `published`, the
//! latest flushed state — `subscribe()` therefore returns a snapshot plus a
//! receiver that will observe exactly the events after that snapshot, gap-free
//! and in order.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::broadcast;

use crate::model::Pointer;
use crate::store::{Committed, Op};

/// One committed change, delivered to subscribers in version order.
#[derive(Debug, Clone)]
pub struct CommitEvent {
    pub version: u64,
    pub changed_paths: Arc<Vec<String>>,
    pub committed: Arc<Committed>,
}

/// A commit that has CASed in but not yet been published.
pub struct Pending {
    pub committed: Arc<Committed>,
    pub changed_paths: Vec<String>,
    pub ops: Vec<Op>,
}

pub struct Ordered {
    next_version: u64,
    published: Arc<Committed>,
    buffer: BTreeMap<u64, Pending>,
}

pub struct Hub {
    tx: broadcast::Sender<Arc<CommitEvent>>,
    ordered: Mutex<Ordered>,
}

impl Hub {
    pub fn new(initial: Arc<Committed>) -> Self {
        let (tx, _) = broadcast::channel(1024);
        Hub {
            tx,
            ordered: Mutex::new(Ordered {
                next_version: initial.version + 1,
                published: initial,
                buffer: BTreeMap::new(),
            }),
        }
    }

    /// Enqueue a CASed commit and flush everything now contiguous, calling
    /// `sink` (WAL append) for each flushed commit before broadcasting it.
    pub fn finalize(&self, pending: Pending, mut sink: impl FnMut(&Pending)) {
        let mut g = self.ordered.lock().unwrap();
        g.buffer.insert(pending.committed.version, pending);
        loop {
            let next = g.next_version;
            let Some(p) = g.buffer.remove(&next) else {
                break;
            };
            sink(&p);
            let event = Arc::new(CommitEvent {
                version: p.committed.version,
                changed_paths: Arc::new(p.changed_paths),
                committed: p.committed.clone(),
            });
            // Send fails only when there are no subscribers; that's fine.
            let _ = self.tx.send(event);
            g.published = p.committed;
            g.next_version += 1;
        }
    }

    /// Consistent subscription: the returned snapshot is the latest published
    /// state, and the receiver sees every event with a greater version.
    pub fn subscribe(&self) -> (Arc<Committed>, broadcast::Receiver<Arc<CommitEvent>>) {
        let g = self.ordered.lock().unwrap();
        (g.published.clone(), self.tx.subscribe())
    }

    /// Latest published state (may trail `Store::head` by in-flight commits).
    pub fn published(&self) -> Arc<Committed> {
        self.ordered.lock().unwrap().published.clone()
    }

    /// Hold the publication lock across `f` — used by snapshotting so the WAL
    /// cannot advance while the snapshot file and WAL rotation are written.
    pub fn with_publish_lock<T>(&self, f: impl FnOnce(&Arc<Committed>) -> T) -> T {
        let g: MutexGuard<'_, Ordered> = self.ordered.lock().unwrap();
        f(&g.published)
    }
}

/// Is a change at `changed` relevant to a subscriber watching `sub`?
/// True when either path is an ancestor of (or equal to) the other.
pub fn is_relevant(sub: &Pointer, changed: &str) -> bool {
    match Pointer::parse(changed) {
        Ok(c) => sub.overlaps(&c),
        Err(_) => false,
    }
}
