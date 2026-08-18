//! Lightweight operational counters — relaxed atomics bumped on the commit
//! and persistence paths, cheap enough to be always-on. `Store::stats()`
//! returns a point-in-time snapshot; deltas between snapshots are what the
//! write benchmark (and any monitoring) work from. The interesting derived
//! signals: `cas_losses / commits` (contention), `wal_fsyncs / wal_records`
//! (group-commit batching — 1.0 means no batching).

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Stats {
    /// Commits that CASed in and produced a version.
    pub(crate) commits: AtomicU64,
    /// Commits elided because they changed nothing.
    pub(crate) elided: AtomicU64,
    /// CAS races lost (each one redid the whole commit preparation, unless
    /// it rebased).
    pub(crate) cas_losses: AtomicU64,
    /// CAS losses recovered by splicing the prepared subtrees onto the new
    /// head instead of replaying the whole preparation.
    pub(crate) rebases: AtomicU64,
    /// WAL records appended.
    pub(crate) wal_records: AtomicU64,
    /// WAL fsync calls issued.
    pub(crate) wal_fsyncs: AtomicU64,
    /// Snapshots written.
    pub(crate) snapshots: AtomicU64,
    /// Total wall-clock microseconds spent writing snapshots.
    pub(crate) snapshot_micros: AtomicU64,
}

impl Stats {
    pub(crate) fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            commits: self.commits.load(Ordering::Relaxed),
            elided: self.elided.load(Ordering::Relaxed),
            cas_losses: self.cas_losses.load(Ordering::Relaxed),
            rebases: self.rebases.load(Ordering::Relaxed),
            wal_records: self.wal_records.load(Ordering::Relaxed),
            wal_fsyncs: self.wal_fsyncs.load(Ordering::Relaxed),
            snapshots: self.snapshots.load(Ordering::Relaxed),
            snapshot_micros: self.snapshot_micros.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time copy of the counters.
#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct StatsSnapshot {
    pub commits: u64,
    pub elided: u64,
    pub cas_losses: u64,
    pub rebases: u64,
    pub wal_records: u64,
    pub wal_fsyncs: u64,
    pub snapshots: u64,
    pub snapshot_micros: u64,
}
