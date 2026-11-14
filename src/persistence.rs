//! Durability: an append-only, segmented WAL of committed ops plus periodic
//! snapshots, written by a dedicated writer thread with group commit.
//!
//! - WAL: one JSON line per commit: `{"version": N, "ops": [...]}`, in strict
//!   version order. Commits enqueue their record under the Hub's publication
//!   lock (so channel order == version order) and the writer thread drains
//!   the channel in batches: write + flush every queued record, then one
//!   fsync for the whole batch — concurrent writers share fsyncs, and batch
//!   size grows naturally with load. Records live in numbered segments
//!   (`wal/000001.log`); a legacy single `wal.log` is still replayed first.
//! - Acks: `wait_durable` blocks a commit until its record is written
//!   (`Durability::Async`/`NoFsync`) or fsynced (`Durability::Sync`).
//!   Subscription events are broadcast by the writer thread after a batch
//!   reaches its durability level, so subscribers never observe a commit
//!   that a crash could take back.
//! - Failure: a write/fsync error poisons the writer — every waiting and
//!   subsequent commit fails with `WalBroken` and nothing past the failure
//!   is broadcast. Restart recovers to the last durable record.
//! - Snapshot: `snapshot.json` = `{"version": N, "root": <raw tree>}`, written
//!   to a temp file and atomically renamed. The publication lock is held only
//!   long enough to pin the state and enqueue a segment rotation; sealed
//!   segments are deleted after the rename lands.
//! - Recovery: load the snapshot (if any), then replay WAL records with
//!   version > snapshot version — legacy `wal.log` first, then segments in
//!   order — re-running each commit's ops and formula re-evaluation. A torn
//!   final line (crash mid-append) is tolerated.

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{mpsc, Arc, Condvar, Mutex, MutexGuard};

use serde::{Deserialize, Serialize};

use crate::error::{Result, StoreError};
use crate::formulas;
use crate::meta::{ModIndex, ModIndexDump, Stamp};
use crate::model::{empty_object, Node};
use crate::stats::Stats;
use crate::store::{apply_ops, Op};
use crate::subscriptions::{CommitEvent, Hub};

/// What a successful `commit()` promises about its WAL record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Ack after the record is written *and* fsynced. Group commit shares
    /// one fsync among concurrent writers. Survives power loss.
    Sync,
    /// Ack after the record is written to the file. Fsync still runs per
    /// batch, but acks don't wait for it. Survives a process crash; power
    /// loss can drop the last batch or two.
    Async,
    /// Ack after the record is written; never fsync. The OS flushes when it
    /// pleases.
    NoFsync,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WalRecord {
    pub version: u64,
    /// Commit wall-clock time (epoch millis). Defaulted for records
    /// written before timestamps existed.
    #[serde(default)]
    pub time_ms: u64,
    pub ops: Vec<Op>,
    /// The principal that made the commit — an audit trail; replay
    /// ignores it. Absent for anonymous/library commits and old records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
}

#[derive(Deserialize)]
struct SnapshotFile {
    version: u64,
    #[serde(default)]
    time_ms: u64,
    root: Node,
    /// Exact per-path validators as of `version` — absent in snapshots
    /// from before the dump existed, in which case recovery falls back to
    /// a floor (safe: over-estimates only cost refetches).
    #[serde(default)]
    mods: Option<ModIndexDump>,
}

#[derive(Serialize)]
struct SnapshotOut<'a> {
    version: u64,
    time_ms: u64,
    root: &'a Node,
    mods: ModIndexDump,
}

/// Everything rebuilt from disk at open.
pub struct Recovered {
    pub version: u64,
    pub time_ms: u64,
    pub root: Arc<Node>,
    pub formulas: formulas::FormulaIndex,
    pub mods: ModIndex,
}

impl Recovered {
    /// A fresh, empty store (persistence disabled).
    pub fn empty() -> Self {
        Recovered {
            version: 0,
            time_ms: 0,
            root: empty_object(),
            formulas: formulas::FormulaIndex::new(),
            mods: ModIndex::new(Stamp::default()),
        }
    }
}

enum WalMsg {
    Record {
        line: String,
        version: u64,
        event: Arc<CommitEvent>,
    },
    /// Seal the current segment and start a new one; reply with every file
    /// sealed so far (safe to delete once a snapshot covering them lands).
    Rotate {
        reply: mpsc::Sender<Result<Vec<PathBuf>>>,
    },
}

#[derive(Debug)]
struct WalProgress {
    /// Highest version written (and flushed) to a segment file.
    written: u64,
    /// Highest version at this store's durability level (== `written` for
    /// `NoFsync`/`Async` acks; advances after the batch fsync for `Sync`).
    durable: u64,
    error: Option<String>,
}

struct WalShared {
    progress: Mutex<WalProgress>,
    advanced: Condvar,
    broken: AtomicBool,
}

impl WalShared {
    fn fail(&self, msg: String) {
        let mut p = self.progress.lock().unwrap();
        p.error.get_or_insert(msg);
        self.broken.store(true, AtomicOrdering::Relaxed);
        self.advanced.notify_all();
    }

    fn broken_error(&self) -> StoreError {
        let p = self.progress.lock().unwrap();
        StoreError::WalBroken(
            p.error
                .clone()
                .unwrap_or_else(|| "WAL writer unavailable".into()),
        )
    }
}

pub struct Persistence {
    legacy_wal_path: PathBuf,
    seg_dir: PathBuf,
    snapshot_path: PathBuf,
    durability: Durability,
    stats: Arc<Stats>,
    shared: Arc<WalShared>,
    tx: Option<mpsc::Sender<WalMsg>>,
    writer: Option<std::thread::JoinHandle<()>>,
    /// Serializes snapshots (rotation + write + prune) against each other,
    /// and remembers the last snapshotted version so an idle store's
    /// periodic tick skips the whole write instead of re-serializing an
    /// identical snapshot (and churning an empty WAL segment) forever.
    last_snapshot: Mutex<u64>,
    /// Exclusive advisory lock on the data dir, held for the process lifetime.
    /// Two processes sharing a dir would interleave WAL versions and prune
    /// each other's segments on snapshot rotation.
    _lock: File,
}

impl Persistence {
    pub fn open(dir: &Path, durability: Durability, stats: Arc<Stats>) -> Result<Self> {
        use fs2::FileExt;
        fs::create_dir_all(dir)?;
        let lock = OpenOptions::new()
            .create(true)
            .write(true)
            .open(dir.join("LOCK"))?;
        lock.try_lock_exclusive().map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!(
                    "data dir {} is already locked by another visijson process",
                    dir.display()
                ),
            )
        })?;
        Ok(Persistence {
            legacy_wal_path: dir.join("wal.log"),
            seg_dir: dir.join("wal"),
            snapshot_path: dir.join("snapshot.json"),
            durability,
            stats,
            shared: Arc::new(WalShared {
                progress: Mutex::new(WalProgress {
                    written: 0,
                    durable: 0,
                    error: None,
                }),
                advanced: Condvar::new(),
                broken: AtomicBool::new(false),
            }),
            tx: None,
            writer: None,
            last_snapshot: Mutex::new(0),
            _lock: lock,
        })
    }

    /// Rebuild state: snapshot (if present) + WAL replay (legacy `wal.log`
    /// first, then segments in order). The formula index is scanned once
    /// from the snapshot, then maintained incrementally through replay; the
    /// modification index starts at the snapshot as its floor and is
    /// sharpened by every replayed commit. Call before `start_writer`.
    pub fn recover(&self) -> Result<Recovered> {
        let (mut version, mut time_ms, mut root, snap_mods) = match fs::read(&self.snapshot_path) {
            Ok(bytes) => {
                let snap: SnapshotFile = serde_json::from_slice(&bytes)?;
                tracing::info!(version = snap.version, "recovered snapshot");
                (snap.version, snap.time_ms, Arc::new(snap.root), snap.mods)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (0, 0, empty_object(), None),
            Err(e) => return Err(e.into()),
        };
        // Seed the idle-skip watermark: the on-disk snapshot already covers
        // exactly this version.
        *self.last_snapshot.lock().unwrap() = version;
        let mut index = formulas::scan_index(&root);
        // Exact validators from the snapshot when present (client ETags
        // stay valid across the restart); floor otherwise. Replay sharpens
        // either starting point.
        let mut mods = snap_mods
            .map(|d| ModIndex::from_dump(&d))
            .unwrap_or_else(|| ModIndex::new(Stamp::new(version, time_ms)));

        let mut files = Vec::new();
        if self.legacy_wal_path.exists() {
            files.push(self.legacy_wal_path.clone());
        }
        files.extend(list_segments(&self.seg_dir)?);

        let mut replayed = 0u64;
        'files: for path in files {
            let file = match File::open(&path) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            for line in BufReader::new(file).lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let record: WalRecord = match serde_json::from_str(&line) {
                    Ok(r) => r,
                    Err(e) => {
                        // A torn tail from a crash mid-append is expected;
                        // stop here (a later file couldn't be contiguous).
                        tracing::warn!(error = %e, file = %path.display(),
                            "stopping WAL replay at unparseable record");
                        break 'files;
                    }
                };
                if record.version <= version {
                    continue; // predates the snapshot
                }
                if record.version != version + 1 {
                    tracing::warn!(
                        expected = version + 1,
                        found = record.version,
                        "WAL gap; stopping replay"
                    );
                    break 'files;
                }
                let (r, changed, scan_roots) = apply_ops(&root, &record.ops)?;
                index = formulas::update_index(&index, &scan_roots, &r)?;
                let (r, fchanged) = formulas::reevaluate(&r, &changed, &index)?;
                root = r;
                version = record.version;
                time_ms = record.time_ms.max(time_ms);
                let stamp = Stamp::new(version, time_ms);
                mods = mods.record(stamp, changed.iter().chain(fchanged.iter()));
                replayed += 1;
            }
        }
        if replayed > 0 {
            tracing::info!(replayed, version, "replayed WAL");
        }
        Ok(Recovered {
            version,
            time_ms,
            root,
            formulas: index,
            mods,
        })
    }

    /// Spawn the WAL writer thread. `base_version` is the recovered version
    /// — everything at or below it is already on disk by definition.
    pub fn start_writer(&mut self, hub: Arc<Hub>, base_version: u64) -> Result<()> {
        fs::create_dir_all(&self.seg_dir)?;
        {
            let mut p = self.shared.progress.lock().unwrap();
            p.written = base_version;
            p.durable = base_version;
        }
        // Everything already on disk is sealed history for the next snapshot.
        let mut sealed = Vec::new();
        if self.legacy_wal_path.exists() {
            sealed.push(self.legacy_wal_path.clone());
        }
        let existing = list_segments(&self.seg_dir)?;
        let seg_no = next_segment_number(&existing);
        sealed.extend(existing);

        let (tx, rx) = mpsc::channel();
        let ctx = WriterCtx {
            rx,
            seg_dir: self.seg_dir.clone(),
            seg_no,
            sealed,
            durability: self.durability,
            stats: self.stats.clone(),
            shared: self.shared.clone(),
            hub,
        };
        let handle = std::thread::Builder::new()
            .name("visijson-wal".into())
            .spawn(move || writer_loop(ctx))?;
        self.tx = Some(tx);
        self.writer = Some(handle);
        Ok(())
    }

    /// Fail fast before a commit CASes: is the WAL still accepting records?
    pub fn check_writable(&self) -> Result<()> {
        if self.shared.broken.load(AtomicOrdering::Relaxed) {
            return Err(self.shared.broken_error());
        }
        Ok(())
    }

    /// Hand a commit's record (and its subscriber event) to the writer.
    /// Must be called under the Hub's publication lock, in version order.
    pub fn enqueue(&self, record: &WalRecord, event: Arc<CommitEvent>) -> Result<()> {
        if self.shared.broken.load(AtomicOrdering::Relaxed) {
            return Err(self.shared.broken_error());
        }
        let line = serde_json::to_string(record)?;
        self.tx
            .as_ref()
            .expect("writer started")
            .send(WalMsg::Record {
                line,
                version: record.version,
                event,
            })
            .map_err(|_| StoreError::WalBroken("WAL writer thread exited".into()))
    }

    /// Block until `version` reaches this store's durability level. This is
    /// what makes commit ack imply written (`Async`/`NoFsync`) or fsynced
    /// (`Sync`) — concurrent commits in the same batch wake together.
    pub fn wait_durable(&self, version: u64) -> Result<()> {
        let need_fsync = self.durability == Durability::Sync;
        let mut p = self.shared.progress.lock().unwrap();
        loop {
            let level = if need_fsync { p.durable } else { p.written };
            if level >= version {
                return Ok(());
            }
            if let Some(msg) = &p.error {
                return Err(StoreError::WalBroken(msg.clone()));
            }
            p = self.shared.advanced.wait(p).unwrap();
        }
    }

    /// Enqueue a segment rotation; the returned channel yields the sealed
    /// files once the writer has processed everything ahead of the marker.
    /// Must be called under the Hub's publication lock so the boundary is
    /// exact; receive the reply *after* releasing it.
    pub fn request_rotation(&self) -> Result<mpsc::Receiver<Result<Vec<PathBuf>>>> {
        let (reply, rx) = mpsc::channel();
        self.tx
            .as_ref()
            .expect("writer started")
            .send(WalMsg::Rotate { reply })
            .map_err(|_| StoreError::WalBroken("WAL writer thread exited".into()))?;
        Ok(rx)
    }

    /// Serialize whole-snapshot operations against each other.
    pub fn snapshot_guard(&self) -> MutexGuard<'_, u64> {
        self.last_snapshot.lock().unwrap()
    }

    /// Atomically write a snapshot at `version` (temp file + rename). Safe
    /// outside any lock: the caller pins the tree by `Arc` and prunes
    /// sealed segments only after this returns.
    pub fn write_snapshot(
        &self,
        version: u64,
        time_ms: u64,
        root: &Arc<Node>,
        mods: &ModIndex,
    ) -> Result<()> {
        let snap = SnapshotOut {
            version,
            time_ms,
            root,
            mods: mods.dump(),
        };
        let tmp = self.snapshot_path.with_extension("json.tmp");
        {
            let file = File::create(&tmp)?;
            let mut w = BufWriter::new(file);
            serde_json::to_writer(&mut w, &snap)?;
            w.flush()?;
            w.get_ref().sync_all()?;
        }
        fs::rename(&tmp, &self.snapshot_path)?;
        Ok(())
    }

    /// Delete files superseded by a snapshot. Missing files are fine (a
    /// previous prune may have raced a failed snapshot's retry).
    pub fn prune(&self, sealed: &[PathBuf]) {
        for p in sealed {
            match fs::remove_file(p) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => tracing::warn!(file = %p.display(), error = %e, "prune failed"),
            }
        }
    }
}

impl Drop for Persistence {
    fn drop(&mut self) {
        // Close the channel so the writer drains outstanding records and
        // exits; join so everything is written before the dir is reusable.
        self.tx.take();
        if let Some(w) = self.writer.take() {
            let _ = w.join();
        }
    }
}

fn segment_path(seg_dir: &Path, n: u64) -> PathBuf {
    seg_dir.join(format!("{n:06}.log"))
}

/// Segment files in replay order (zero-padded names sort correctly).
fn list_segments(seg_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut v = Vec::new();
    match fs::read_dir(seg_dir) {
        Ok(rd) => {
            for entry in rd {
                let p = entry?.path();
                if p.extension().and_then(|e| e.to_str()) == Some("log") {
                    v.push(p);
                }
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    v.sort();
    Ok(v)
}

/// Segment numbers never reuse, even after pruning.
fn next_segment_number(existing: &[PathBuf]) -> u64 {
    existing
        .iter()
        .filter_map(|p| p.file_stem()?.to_str()?.parse::<u64>().ok())
        .max()
        .map_or(1, |n| n + 1)
}

struct WriterCtx {
    rx: mpsc::Receiver<WalMsg>,
    seg_dir: PathBuf,
    seg_no: u64,
    /// Files fully behind the current segment — what a snapshot may delete.
    sealed: Vec<PathBuf>,
    durability: Durability,
    stats: Arc<Stats>,
    shared: Arc<WalShared>,
    hub: Arc<Hub>,
}

fn open_segment(seg_dir: &Path, n: u64) -> std::io::Result<BufWriter<File>> {
    let file = OpenOptions::new()
        .create_new(true)
        .append(true)
        .open(segment_path(seg_dir, n))?;
    Ok(BufWriter::new(file))
}

/// The WAL writer: drains the channel in arrival order (== version order),
/// batching every record that queued up while the previous batch was being
/// written or fsynced — group commit with no artificial delay. Broadcast to
/// subscribers happens here, after the batch reaches the store's durability
/// level, so events never run ahead of what a restart would recover.
fn writer_loop(mut ctx: WriterCtx) {
    let mut current = match open_segment(&ctx.seg_dir, ctx.seg_no) {
        Ok(f) => f,
        Err(e) => {
            ctx.shared.fail(e.to_string());
            return drain_broken(&ctx.rx, &ctx.shared);
        }
    };
    loop {
        let first = match ctx.rx.recv() {
            Ok(m) => m,
            Err(_) => return, // channel closed; all batches already flushed
        };
        // Gather the batch: everything queued now, stopping at a rotation
        // marker so the segment boundary stays exact.
        let mut records = Vec::new();
        let mut rotate = None;
        match first {
            WalMsg::Record {
                line,
                version,
                event,
            } => records.push((line, version, event)),
            WalMsg::Rotate { reply } => rotate = Some(reply),
        }
        while rotate.is_none() {
            match ctx.rx.try_recv() {
                Ok(WalMsg::Record {
                    line,
                    version,
                    event,
                }) => records.push((line, version, event)),
                Ok(WalMsg::Rotate { reply }) => rotate = Some(reply),
                Err(_) => break,
            }
        }

        if let Some((_, last_version, _)) = records.last() {
            let last_version = *last_version;
            if let Err(e) = write_batch(&mut current, &records, &ctx, last_version) {
                ctx.shared.fail(e.to_string());
                tracing::error!(error = %e, "WAL write failed; store is fail-stopped for writes");
                if let Some(reply) = rotate {
                    let _ = reply.send(Err(ctx.shared.broken_error()));
                }
                return drain_broken(&ctx.rx, &ctx.shared);
            }
            // Broadcast only after the batch is durable to this store's level.
            ctx.hub
                .publish_batch(records.iter().map(|(_, _, e)| e.clone()));
        }

        if let Some(reply) = rotate {
            let result = (|| -> Result<Vec<PathBuf>> {
                current.flush()?;
                ctx.sealed.push(segment_path(&ctx.seg_dir, ctx.seg_no));
                ctx.seg_no += 1;
                current = open_segment(&ctx.seg_dir, ctx.seg_no)?;
                // Report only files that still exist — earlier prunes may
                // have removed some after a snapshot that then failed.
                ctx.sealed.retain(|p| p.exists());
                Ok(ctx.sealed.clone())
            })();
            match result {
                Ok(sealed) => {
                    let _ = reply.send(Ok(sealed));
                }
                Err(e) => {
                    ctx.shared.fail(e.to_string());
                    let _ = reply.send(Err(ctx.shared.broken_error()));
                    return drain_broken(&ctx.rx, &ctx.shared);
                }
            }
        }
    }
}

fn write_batch(
    w: &mut BufWriter<File>,
    records: &[(String, u64, Arc<CommitEvent>)],
    ctx: &WriterCtx,
    last_version: u64,
) -> Result<()> {
    for (line, _, _) in records {
        writeln!(w, "{line}")?;
    }
    w.flush()?;
    Stats::add(&ctx.stats.wal_records, records.len() as u64);
    {
        // Written: Async/NoFsync acks release here, while the fsync (if
        // any) is still ahead of us.
        let mut p = ctx.shared.progress.lock().unwrap();
        p.written = last_version;
        if ctx.durability == Durability::NoFsync {
            p.durable = last_version;
        }
        ctx.shared.advanced.notify_all();
    }
    if ctx.durability != Durability::NoFsync {
        w.get_ref().sync_data()?;
        Stats::bump(&ctx.stats.wal_fsyncs);
        let mut p = ctx.shared.progress.lock().unwrap();
        p.durable = last_version;
        ctx.shared.advanced.notify_all();
    }
    Ok(())
}

/// After a failure: keep consuming so enqueuers never block on a full pipe
/// (mpsc is unbounded, but rotation replies must still be answered), until
/// the store drops the channel.
fn drain_broken(rx: &mpsc::Receiver<WalMsg>, shared: &Arc<WalShared>) {
    while let Ok(msg) = rx.recv() {
        if let WalMsg::Rotate { reply } = msg {
            let _ = reply.send(Err(shared.broken_error()));
        }
    }
}
