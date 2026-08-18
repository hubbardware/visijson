//! Durability: an append-only WAL of committed ops plus periodic snapshots.
//!
//! - WAL: one JSON line per commit: `{"version": N, "ops": [...]}`. Appended
//!   in strict version order (the Hub's publication lock guarantees this),
//!   flushed and optionally fsynced per commit.
//! - Snapshot: `snapshot.json` = `{"version": N, "root": <raw tree>}`, written
//!   to a temp file and atomically renamed, after which the WAL is truncated.
//! - Recovery: load the snapshot (if any), then replay WAL records with
//!   version > snapshot version, re-running each commit's ops and formula
//!   re-evaluation. A torn final line (crash mid-append) is tolerated.

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::formulas;
use crate::meta::{ModIndex, Stamp};
use crate::model::{empty_object, Node};
use crate::store::{apply_ops, Op};

#[derive(Debug, Serialize, Deserialize)]
pub struct WalRecord {
    pub version: u64,
    /// Commit wall-clock time (epoch millis). Defaulted for records
    /// written before timestamps existed.
    #[serde(default)]
    pub time_ms: u64,
    pub ops: Vec<Op>,
}

#[derive(Deserialize)]
struct SnapshotFile {
    version: u64,
    #[serde(default)]
    time_ms: u64,
    root: Node,
}

#[derive(Serialize)]
struct SnapshotOut<'a> {
    version: u64,
    time_ms: u64,
    root: &'a Node,
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

pub struct Persistence {
    wal_path: PathBuf,
    snapshot_path: PathBuf,
    wal: Mutex<BufWriter<File>>,
    fsync: bool,
    /// Exclusive advisory lock on the data dir, held for the process lifetime.
    /// Two processes sharing a dir would interleave WAL versions and truncate
    /// each other's records on snapshot rotation.
    _lock: File,
}

impl Persistence {
    pub fn open(dir: &Path, fsync: bool) -> Result<Self> {
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
        let wal_path = dir.join("wal.log");
        let snapshot_path = dir.join("snapshot.json");
        let file = OpenOptions::new().create(true).append(true).open(&wal_path)?;
        Ok(Persistence {
            wal_path,
            snapshot_path,
            wal: Mutex::new(BufWriter::new(file)),
            fsync,
            _lock: lock,
        })
    }

    /// Rebuild state: snapshot (if present) + WAL replay. The formula
    /// index is scanned once from the snapshot, then maintained
    /// incrementally through replay; the modification index starts at the
    /// snapshot as its floor and is sharpened by every replayed commit, so
    /// recently-written paths keep exact validators across a restart.
    pub fn recover(&self) -> Result<Recovered> {
        let (mut version, mut time_ms, mut root) = match fs::read(&self.snapshot_path) {
            Ok(bytes) => {
                let snap: SnapshotFile = serde_json::from_slice(&bytes)?;
                tracing::info!(version = snap.version, "recovered snapshot");
                (snap.version, snap.time_ms, Arc::new(snap.root))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (0, 0, empty_object()),
            Err(e) => return Err(e.into()),
        };
        let mut index = formulas::scan_index(&root);
        let mut mods = ModIndex::new(Stamp::new(version, time_ms));

        let file = match File::open(&self.wal_path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Recovered {
                    version,
                    time_ms,
                    root,
                    formulas: index,
                    mods,
                })
            }
            Err(e) => return Err(e.into()),
        };
        let mut replayed = 0u64;
        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let record: WalRecord = match serde_json::from_str(&line) {
                Ok(r) => r,
                Err(e) => {
                    // A torn tail from a crash mid-append is expected; stop here.
                    tracing::warn!(error = %e, "stopping WAL replay at unparseable record");
                    break;
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
                break;
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

    /// Append one commit record. Caller (the Hub) guarantees version order.
    pub fn append(&self, record: &WalRecord) -> Result<()> {
        let line = serde_json::to_string(record)?;
        let mut w = self.wal.lock().unwrap();
        writeln!(w, "{line}")?;
        w.flush()?;
        if self.fsync {
            w.get_ref().sync_data()?;
        }
        Ok(())
    }

    /// Atomically write a snapshot at `version`, then truncate the WAL.
    /// Must be called with WAL appends locked out (the Hub's publish lock),
    /// so no record newer than `version` can be lost to the truncation.
    pub fn write_snapshot(&self, version: u64, time_ms: u64, root: &Arc<Node>) -> Result<()> {
        let snap = SnapshotOut {
            version,
            time_ms,
            root,
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

        // Rotate the WAL: everything up to `version` is now in the snapshot.
        let mut w = self.wal.lock().unwrap();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.wal_path)?;
        *w = BufWriter::new(file);
        Ok(())
    }
}
