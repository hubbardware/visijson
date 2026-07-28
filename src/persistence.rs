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
use serde_json::Value;

use crate::error::Result;
use crate::formulas;
use crate::model::{empty_object, from_value, to_raw_value, Node};
use crate::store::{apply_ops, Op};

#[derive(Debug, Serialize, Deserialize)]
pub struct WalRecord {
    pub version: u64,
    pub ops: Vec<Op>,
}

#[derive(Serialize, Deserialize)]
struct SnapshotFile {
    version: u64,
    root: Value,
}

pub struct Persistence {
    wal_path: PathBuf,
    snapshot_path: PathBuf,
    wal: Mutex<BufWriter<File>>,
    fsync: bool,
}

impl Persistence {
    pub fn open(dir: &Path, fsync: bool) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let wal_path = dir.join("wal.log");
        let snapshot_path = dir.join("snapshot.json");
        let file = OpenOptions::new().create(true).append(true).open(&wal_path)?;
        Ok(Persistence {
            wal_path,
            snapshot_path,
            wal: Mutex::new(BufWriter::new(file)),
            fsync,
        })
    }

    /// Rebuild state: snapshot (if present) + WAL replay.
    pub fn recover(&self) -> Result<(u64, Arc<Node>)> {
        let (mut version, mut root) = match fs::read(&self.snapshot_path) {
            Ok(bytes) => {
                let snap: SnapshotFile = serde_json::from_slice(&bytes)?;
                tracing::info!(version = snap.version, "recovered snapshot");
                (snap.version, from_value(&snap.root))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (0, empty_object()),
            Err(e) => return Err(e.into()),
        };

        let file = match File::open(&self.wal_path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((version, root)),
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
            let (r, changed) = apply_ops(&root, &record.ops)?;
            let (r, _) = formulas::reevaluate(&r, &changed)?;
            root = r;
            version = record.version;
            replayed += 1;
        }
        if replayed > 0 {
            tracing::info!(replayed, version, "replayed WAL");
        }
        Ok((version, root))
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
    pub fn write_snapshot(&self, version: u64, root: &Arc<Node>) -> Result<()> {
        let snap = SnapshotFile {
            version,
            root: to_raw_value(root),
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
