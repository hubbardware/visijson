//! Ordered change-event publication and path subscriptions.
//!
//! Commits CAS the root lock-free, so two commits may race to publish their
//! events. The `Hub` restores order: each finalized commit is buffered by
//! version and flushed strictly in version order, under a mutex, into a
//! `tokio::sync::broadcast` channel. The same mutex guards `published`, the
//! latest flushed state — `subscribe()` therefore returns a snapshot plus a
//! receiver that will observe exactly the events after that snapshot, gap-free
//! and in order.
//!
//! `finalize` does not return until the caller's own commit has been flushed
//! (its WAL record written by whichever thread drained it), so a successful
//! commit is durable to the WAL's configured strength before it acks. A sink
//! failure poisons the hub: the WAL has (or may have) a hole, records after it
//! would be unreachable to recovery, so every subsequent write fails with
//! `WalBroken` instead of silently losing data.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use tokio::sync::broadcast;

use crate::error::{Result, StoreError};
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
    pub changed_paths: Arc<Vec<String>>,
    pub ops: Vec<Op>,
}

pub struct Ordered {
    next_version: u64,
    published: Arc<Committed>,
    /// Latest state handed to the sink in version order — for a persistent
    /// store this can run ahead of `published`, which waits for durability.
    enqueued: Arc<Committed>,
    buffer: BTreeMap<u64, Pending>,
    /// Set when a sink (WAL enqueue/append) failed: the log has a hole, so
    /// no later record could be recovered. Every subsequent write fails fast.
    poisoned: Option<String>,
}

pub struct Hub {
    tx: broadcast::Sender<Arc<CommitEvent>>,
    ordered: Mutex<Ordered>,
    /// When true (persistent store), `finalize` does not broadcast; the WAL
    /// writer calls `publish_batch` once a batch reaches its durability
    /// level, so subscribers never observe a commit a crash could take back.
    deferred_publish: bool,
    /// Signals waiters whenever `next_version` advances or the hub poisons.
    flushed: Condvar,
    /// Lock-free mirror of `Ordered::poisoned` for the pre-commit check.
    broken: AtomicBool,
}

impl Hub {
    pub fn new(initial: Arc<Committed>, deferred_publish: bool) -> Self {
        let (tx, _) = broadcast::channel(1024);
        Hub {
            tx,
            ordered: Mutex::new(Ordered {
                next_version: initial.version + 1,
                published: initial.clone(),
                enqueued: initial,
                buffer: BTreeMap::new(),
                poisoned: None,
            }),
            deferred_publish,
            flushed: Condvar::new(),
            broken: AtomicBool::new(false),
        }
    }

    /// Cheap pre-commit check: refuse new writes once the WAL is broken, so
    /// a commit is rejected before it CASes state that could never recover.
    pub fn check_writable(&self) -> Result<()> {
        if self.broken.load(AtomicOrdering::Relaxed) {
            let g = self.ordered.lock().unwrap();
            if let Some(msg) = &g.poisoned {
                return Err(StoreError::WalBroken(msg.clone()));
            }
        }
        Ok(())
    }

    /// Enqueue a CASed commit and flush everything now contiguous, calling
    /// `sink` for each flushed commit in strict version order. For a
    /// persistent store the sink hands the record to the WAL writer (and
    /// broadcast is deferred to `publish_batch`); for an in-memory store
    /// the sink is a no-op and the event broadcasts here. Returns only once
    /// this commit has been flushed through the sink (possibly by another
    /// thread draining the buffer); a failed sink poisons the hub and every
    /// in-flight and future commit fails.
    pub fn finalize(
        &self,
        pending: Pending,
        mut sink: impl FnMut(&Pending, &Arc<CommitEvent>) -> Result<()>,
    ) -> Result<()> {
        let version = pending.committed.version;
        let mut g = self.ordered.lock().unwrap();
        if let Some(msg) = &g.poisoned {
            return Err(StoreError::WalBroken(msg.clone()));
        }
        g.buffer.insert(version, pending);
        loop {
            let next = g.next_version;
            let Some(p) = g.buffer.remove(&next) else {
                break;
            };
            let event = Arc::new(CommitEvent {
                version: p.committed.version,
                changed_paths: p.changed_paths.clone(),
                committed: p.committed.clone(),
            });
            if let Err(e) = sink(&p, &event) {
                g.poisoned = Some(e.to_string());
                self.broken.store(true, AtomicOrdering::Relaxed);
                self.flushed.notify_all();
                // Whether the failed record was ours or an earlier one, our
                // commit cannot be durable behind a hole in the log.
                return Err(e);
            }
            if !self.deferred_publish {
                // Send fails only when there are no subscribers; that's fine.
                let _ = self.tx.send(event);
                g.published = p.committed.clone();
            }
            g.enqueued = p.committed;
            g.next_version += 1;
        }
        self.flushed.notify_all();
        // If our version is still buffered, an earlier CAS winner hasn't
        // finalized yet; it (or a successor) will flush our record — wait
        // for that before acking, so ack always implies sunk in order.
        while g.next_version <= version {
            if let Some(msg) = &g.poisoned {
                return Err(StoreError::WalBroken(msg.clone()));
            }
            g = self.flushed.wait(g).unwrap();
        }
        Ok(())
    }

    /// Broadcast a durable batch, in order, and advance `published`. Called
    /// by the WAL writer thread; the lock makes each event atomic with
    /// `subscribe`, preserving the gap-free guarantee.
    pub fn publish_batch(&self, events: impl IntoIterator<Item = Arc<CommitEvent>>) {
        let mut g = self.ordered.lock().unwrap();
        for event in events {
            g.published = event.committed.clone();
            let _ = self.tx.send(event);
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

    /// Run `f` with the latest *enqueued* state while holding the
    /// publication lock — used by snapshotting to pin a version and mark a
    /// WAL rotation behind it atomically. Keep `f` fast: nothing can
    /// finalize or publish while it runs.
    pub fn latest_enqueued<T>(&self, f: impl FnOnce(&Arc<Committed>) -> T) -> T {
        let g: MutexGuard<'_, Ordered> = self.ordered.lock().unwrap();
        f(&g.enqueued)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formulas::FormulaIndex;
    use crate::meta::{ModIndex, Stamp};
    use crate::model::empty_object;

    fn committed(version: u64) -> Arc<Committed> {
        Arc::new(Committed {
            version,
            time_ms: 0,
            root: empty_object(),
            formulas: FormulaIndex::new(),
            mods: ModIndex::new(Stamp::default()),
        })
    }

    fn pending(version: u64) -> Pending {
        Pending {
            committed: committed(version),
            changed_paths: Arc::new(vec![format!("/v{version}")]),
            ops: Vec::new(),
        }
    }

    fn io_err() -> StoreError {
        StoreError::Io(std::io::Error::other("disk full"))
    }

    #[test]
    fn out_of_order_finalize_blocks_until_flushed_in_order() {
        let hub = Arc::new(Hub::new(committed(0), false));
        let (snap, mut rx) = hub.subscribe();
        assert_eq!(snap.version, 0);

        // Version 2 arrives first; its finalize must not return (and must
        // not ack) until version 1 has been flushed ahead of it.
        let h2 = hub.clone();
        let t = std::thread::spawn(move || h2.finalize(pending(2), |_, _| Ok(())));
        hub.finalize(pending(1), |_, _| Ok(())).unwrap();
        t.join().unwrap().unwrap();

        assert_eq!(rx.try_recv().unwrap().version, 1);
        assert_eq!(rx.try_recv().unwrap().version, 2);
        assert_eq!(hub.published().version, 2);
    }

    #[test]
    fn failed_sink_poisons_the_hub() {
        let hub = Hub::new(committed(0), false);
        let err = hub.finalize(pending(1), |_, _| Err(io_err())).unwrap_err();
        assert!(matches!(err, StoreError::Io(_)), "got {err:?}");
        // Every later commit fails fast, before and inside finalize.
        assert!(matches!(hub.check_writable(), Err(StoreError::WalBroken(_))));
        let err = hub.finalize(pending(2), |_, _| Ok(())).unwrap_err();
        assert!(matches!(err, StoreError::WalBroken(_)), "got {err:?}");
        // Nothing was published for the failed commit.
        assert_eq!(hub.published().version, 0);
    }

    #[test]
    fn waiter_behind_failed_append_is_woken_with_error() {
        let hub = Arc::new(Hub::new(committed(0), false));
        // Version 2 waits for version 1; version 1's append fails, which
        // must wake the waiter with an error, not strand it (its record
        // sits behind a hole in the log, so it can never be durable).
        let h2 = hub.clone();
        let t = std::thread::spawn(move || h2.finalize(pending(2), |_, _| Ok(())));
        let err = hub.finalize(pending(1), |_, _| Err(io_err())).unwrap_err();
        assert!(matches!(err, StoreError::Io(_)), "got {err:?}");
        let err = t.join().unwrap().unwrap_err();
        assert!(matches!(err, StoreError::WalBroken(_)), "got {err:?}");
    }

    #[test]
    fn deferred_publish_broadcasts_only_via_publish_batch() {
        let hub = Arc::new(Hub::new(committed(0), true));
        let (_, mut rx) = hub.subscribe();
        let mut sunk = Vec::new();
        hub.finalize(pending(1), |_, event| {
            sunk.push(event.clone());
            Ok(())
        })
        .unwrap();
        // Sunk in order but not yet published: subscribers see nothing and
        // the published snapshot hasn't advanced (durability pending).
        assert_eq!(sunk.len(), 1);
        assert!(rx.try_recv().is_err());
        assert_eq!(hub.published().version, 0);
        assert_eq!(hub.latest_enqueued(|c| c.version), 1);

        hub.publish_batch(sunk);
        assert_eq!(rx.try_recv().unwrap().version, 1);
        assert_eq!(hub.published().version, 1);
    }
}
