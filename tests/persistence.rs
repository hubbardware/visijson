//! Crash recovery: WAL replay, snapshot + WAL, torn tail tolerance, and
//! formula state surviving restart.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde_json::json;
use tempfile::TempDir;
use visijson::store::{Store, StoreConfig};
use visijson::Durability;

fn open(dir: &Path) -> std::sync::Arc<Store> {
    open_with(dir, Durability::NoFsync) // tests simulate process death, not power loss
}

fn open_with(dir: &Path, durability: Durability) -> std::sync::Arc<Store> {
    Store::open(StoreConfig {
        data_dir: Some(dir.to_path_buf()),
        durability,
        ..StoreConfig::default()
    })
    .unwrap()
}

/// All WAL content in replay order: legacy `wal.log` then segments.
fn read_wal(dir: &Path) -> String {
    let mut out = std::fs::read_to_string(dir.join("wal.log")).unwrap_or_default();
    for seg in segment_paths(dir) {
        out.push_str(&std::fs::read_to_string(seg).unwrap_or_default());
    }
    out
}

fn segment_paths(dir: &Path) -> Vec<PathBuf> {
    let mut segs: Vec<PathBuf> = std::fs::read_dir(dir.join("wal"))
        .map(|rd| rd.filter_map(|e| Some(e.ok()?.path())).collect())
        .unwrap_or_default();
    segs.sort();
    segs
}

#[test]
fn wal_replay_restores_state_without_snapshot() {
    let dir = TempDir::new().unwrap();
    {
        let store = open(dir.path());
        store.put("/a", json!(1)).unwrap();
        store.put("/b/c", json!([1, 2])).unwrap();
        store.append("/b/c", json!(3)).unwrap();
        store.remove("/a").unwrap();
        assert_eq!(store.version(), 4);
    } // dropped without ever snapshotting — recovery is pure WAL replay

    let store = open(dir.path());
    assert_eq!(store.version(), 4);
    assert_eq!(store.get("/a").unwrap(), None);
    assert_eq!(store.get("/b/c").unwrap(), Some(json!([1, 2, 3])));

    // And the recovered store keeps working with correct versioning.
    let info = store.put("/d", json!(true)).unwrap();
    assert_eq!(info.version, 5);
}

#[test]
fn snapshot_plus_wal_recovery() {
    let dir = TempDir::new().unwrap();
    {
        let store = open(dir.path());
        store.put("/x", json!(1)).unwrap();
        store.put("/y", json!(2)).unwrap();
        assert_eq!(store.snapshot().unwrap(), Some(2));
        // Post-snapshot commits land in the fresh WAL only.
        store.put("/z", json!(3)).unwrap();
        store.put("/x", json!(10)).unwrap();
    }

    let store = open(dir.path());
    assert_eq!(store.version(), 4);
    assert_eq!(store.get("/x").unwrap(), Some(json!(10)));
    assert_eq!(store.get("/y").unwrap(), Some(json!(2)));
    assert_eq!(store.get("/z").unwrap(), Some(json!(3)));
}

#[test]
fn snapshot_alone_recovery_after_wal_rotation() {
    let dir = TempDir::new().unwrap();
    {
        let store = open(dir.path());
        store.put("/only", json!("snapshotted")).unwrap();
        store.snapshot().unwrap();
    }
    // WAL was rotated to empty; recovery must come entirely from the snapshot.
    let store = open(dir.path());
    assert_eq!(store.version(), 1);
    assert_eq!(store.get("/only").unwrap(), Some(json!("snapshotted")));
}

#[test]
fn torn_wal_tail_is_tolerated() {
    let dir = TempDir::new().unwrap();
    {
        let store = open(dir.path());
        store.put("/a", json!(1)).unwrap();
        store.put("/b", json!(2)).unwrap();
    }
    // Simulate a crash mid-append: garbage half-record at the tail of the
    // last WAL segment.
    let last = segment_paths(dir.path()).pop().expect("a WAL segment");
    let mut f = OpenOptions::new().append(true).open(last).unwrap();
    f.write_all(b"{\"version\":3,\"ops\":[{\"op\":\"put\",\"pa").unwrap();
    drop(f);

    let store = open(dir.path());
    // Replay stops cleanly at the torn record; the two good commits survive.
    assert_eq!(store.version(), 2);
    assert_eq!(store.get("/a").unwrap(), Some(json!(1)));
    assert_eq!(store.get("/b").unwrap(), Some(json!(2)));
    // New commits continue from the recovered version.
    assert_eq!(store.put("/c", json!(3)).unwrap().version, 3);
}

#[test]
fn formulas_survive_recovery_and_keep_recomputing() {
    let dir = TempDir::new().unwrap();
    {
        let store = open(dir.path());
        store.put("/a", json!(2)).unwrap();
        store.put("/f", json!({"$formula": "/a * 3"})).unwrap();
        assert_eq!(store.get("/f").unwrap(), Some(json!(6)));
        store.snapshot().unwrap();
        store.put("/a", json!(4)).unwrap(); // post-snapshot, WAL-only
        assert_eq!(store.get("/f").unwrap(), Some(json!(12)));
    }

    let store = open(dir.path());
    assert_eq!(store.get("/f").unwrap(), Some(json!(12)));
    assert_eq!(store.get_raw("/f").unwrap().unwrap()["$formula"], json!("/a * 3"));
    // Still live after recovery.
    store.put("/a", json!(10)).unwrap();
    assert_eq!(store.get("/f").unwrap(), Some(json!(30)));
}

#[test]
fn recovery_is_deterministic_across_repeated_restarts() {
    let dir = TempDir::new().unwrap();
    {
        let store = open(dir.path());
        for i in 0..10i64 {
            store.put(&format!("/k{i}"), json!(i)).unwrap();
        }
        store.snapshot().unwrap();
        for i in 10..20i64 {
            store.put(&format!("/k{i}"), json!(i)).unwrap();
        }
    }
    let first = {
        let store = open(dir.path());
        (store.version(), store.get("").unwrap())
    };
    let second = {
        let store = open(dir.path());
        (store.version(), store.get("").unwrap())
    };
    assert_eq!(first, second);
    assert_eq!(first.0, 20);
}

#[test]
fn wal_records_effects_not_whole_requests() {
    let dir = TempDir::new().unwrap();
    {
        let store = open(dir.path());
        store
            .put(
                "/things",
                json!({"keep": {"blob": "GIANT-UNCHANGED-PAYLOAD"}, "n": 1}),
            )
            .unwrap();
        // Poll-style full replace: only /things/n actually changed.
        store
            .put(
                "/things",
                json!({"keep": {"blob": "GIANT-UNCHANGED-PAYLOAD"}, "n": 2}),
            )
            .unwrap();
        // Identical full replace: elided — no WAL record at all.
        let v = store.version();
        store
            .put(
                "/things",
                json!({"keep": {"blob": "GIANT-UNCHANGED-PAYLOAD"}, "n": 2}),
            )
            .unwrap();
        assert_eq!(store.version(), v);
    }

    let wal = read_wal(dir.path());
    let lines: Vec<&str> = wal.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(lines.len(), 2, "elided commit must not append: {wal}");
    // The second record is the effect (just /things/n), not the request.
    assert!(lines[1].contains("/things/n"), "got {}", lines[1]);
    assert!(
        !lines[1].contains("GIANT-UNCHANGED-PAYLOAD"),
        "unchanged data leaked into the WAL: {}",
        lines[1]
    );

    // Replaying minimal records reproduces the exact state.
    let store = open(dir.path());
    assert_eq!(store.version(), 2);
    assert_eq!(
        store.get("/things").unwrap(),
        Some(json!({"keep": {"blob": "GIANT-UNCHANGED-PAYLOAD"}, "n": 2}))
    );
}

#[test]
fn commit_ack_implies_wal_record_on_disk() {
    let dir = TempDir::new().unwrap();
    let store = open(dir.path());
    // Racing writers CAS in any order, but a commit must not return until
    // its own WAL record is written — the instant `put` acks version V,
    // V's record must be readable from the files.
    std::thread::scope(|s| {
        for t in 0..8 {
            let store = store.clone();
            let dir = dir.path();
            s.spawn(move || {
                for i in 0..25 {
                    let v = store.put(&format!("/t{t}/i{i}"), json!(i)).unwrap().version;
                    assert!(
                        read_wal(dir).contains(&format!("\"version\":{v}")),
                        "commit acked v{v} but the WAL on disk lacks its record"
                    );
                }
            });
        }
    });
}

#[test]
fn group_commit_batches_fsyncs_across_concurrent_writers() {
    let dir = TempDir::new().unwrap();
    let store = open_with(dir.path(), Durability::Sync);
    std::thread::scope(|s| {
        for t in 0..8 {
            let store = store.clone();
            s.spawn(move || {
                for i in 0..25 {
                    store.put(&format!("/t{t}/i{i}"), json!(i)).unwrap();
                }
            });
        }
    });
    let stats = store.stats();
    assert_eq!(stats.wal_records, 200);
    // Writers queue while the previous batch fsyncs, so batches must have
    // formed — strictly fewer fsyncs than records.
    assert!(
        stats.wal_fsyncs < stats.wal_records,
        "no batching: {} fsyncs for {} records",
        stats.wal_fsyncs,
        stats.wal_records
    );
    // And every record survives a restart.
    drop(store);
    let store = open(dir.path());
    assert_eq!(store.version(), 200);
}

#[test]
fn all_durability_modes_recover_identically() {
    for durability in [Durability::Sync, Durability::Async, Durability::NoFsync] {
        let dir = TempDir::new().unwrap();
        {
            let store = open_with(dir.path(), durability);
            store.put("/a", json!(1)).unwrap();
            store.put("/b/c", json!([1, 2])).unwrap();
            store.snapshot().unwrap();
            store.put("/a", json!(2)).unwrap();
        }
        let store = open(dir.path());
        assert_eq!(store.version(), 3, "{durability:?}");
        assert_eq!(store.get("/a").unwrap(), Some(json!(2)), "{durability:?}");
        assert_eq!(store.get("/b/c").unwrap(), Some(json!([1, 2])), "{durability:?}");
    }
}

#[test]
fn legacy_single_file_wal_is_still_replayed() {
    let dir = TempDir::new().unwrap();
    std::fs::write(
        dir.path().join("wal.log"),
        concat!(
            "{\"version\":1,\"time_ms\":5,\"ops\":[{\"op\":\"put\",\"path\":\"/old\",\"value\":1}]}\n",
            "{\"version\":2,\"time_ms\":6,\"ops\":[{\"op\":\"put\",\"path\":\"/old2\",\"value\":2}]}\n",
        ),
    )
    .unwrap();
    let store = open(dir.path());
    assert_eq!(store.version(), 2);
    assert_eq!(store.get("/old").unwrap(), Some(json!(1)));
    // New commits land in segments and replay after the legacy file.
    store.put("/new", json!(3)).unwrap();
    drop(store);
    let store = open(dir.path());
    assert_eq!(store.version(), 3);
    assert_eq!(store.get("/new").unwrap(), Some(json!(3)));
    // A snapshot retires the legacy file along with sealed segments.
    store.snapshot().unwrap();
    assert!(!dir.path().join("wal.log").exists());
    drop(store);
    let store = open(dir.path());
    assert_eq!(store.version(), 3);
}

#[test]
fn snapshots_under_concurrent_writes_lose_nothing() {
    let dir = TempDir::new().unwrap();
    let store = open(dir.path());
    std::thread::scope(|s| {
        for t in 0..4 {
            let store = store.clone();
            s.spawn(move || {
                for i in 0..50 {
                    store.put(&format!("/t{t}/i{i}"), json!(i)).unwrap();
                }
            });
        }
        let store = store.clone();
        s.spawn(move || {
            for _ in 0..10 {
                store.snapshot().unwrap();
            }
        });
    });
    let final_version = store.version();
    assert_eq!(final_version, 200);
    drop(store);
    let store = open(dir.path());
    assert_eq!(store.version(), final_version);
    for t in 0..4 {
        for i in 0..50 {
            assert_eq!(
                store.get(&format!("/t{t}/i{i}")).unwrap(),
                Some(json!(i)),
                "/t{t}/i{i} lost across snapshot + restart"
            );
        }
    }
}

#[test]
fn foreign_snapshot_cycle_only_rejects_commits_that_touch_it() {
    let dir = TempDir::new().unwrap();
    // A formula cycle can't be committed through the API; simulate a
    // snapshot written by something else that contains one.
    std::fs::write(
        dir.path().join("snapshot.json"),
        serde_json::to_string(&json!({
            "version": 1,
            "time_ms": 0,
            "root": {
                "f": {"$formula": "/g + 1", "$value": 1},
                "g": {"$formula": "/f + 1", "$value": 2},
                "x": 0
            }
        }))
        .unwrap(),
    )
    .unwrap();

    let store = open(dir.path());
    // Commits that don't touch the cycle proceed.
    store.put("/x", json!(5)).unwrap();
    assert_eq!(store.get("/x").unwrap(), Some(json!(5)));
    // Re-writing a member of the cycle is still rejected.
    let err = store.put("/f", json!({"$formula": "/g + 1"})).unwrap_err();
    assert!(
        matches!(err, visijson::error::StoreError::FormulaCycle(_)),
        "got {err:?}"
    );
    // Replacing a member with a plain value legitimately breaks the cycle.
    store.put("/g", json!(0)).unwrap();
    assert_eq!(store.get("/f").unwrap(), Some(json!(1)));
}

#[test]
fn commit_timestamps_and_validators_survive_restart() {
    let dir = TempDir::new().unwrap();
    let (v_at_write, t_at_write) = {
        let store = open(dir.path());
        store.put("/early", json!(1)).unwrap();
        store.put("/late", json!(1)).unwrap();
        let s = store.last_modified("/late").unwrap();
        assert!(s.time_ms > 1_700_000_000_000, "no wall clock: {s:?}");
        (s.version, s.time_ms)
    };

    // WAL replay restores the timestamps and re-sharpens the per-path
    // modification index, so ETags stay stable across a restart.
    let store = open(dir.path());
    let s = store.last_modified("/late").unwrap();
    assert_eq!((s.version, s.time_ms), (v_at_write, t_at_write));
    // /early is older than /late and must stay distinguishable.
    assert!(store.last_modified("/early").unwrap().version < s.version);

    // Snapshot + restart: the index resets to a floor, which must still be
    // a safe (never older than truth) answer.
    store.snapshot().unwrap();
    let v = store.version();
    drop(store);
    let store = open(dir.path());
    let s = store.last_modified("/early").unwrap();
    assert_eq!(s.version, v, "floor should be the snapshot version");
    assert!(s.time_ms >= t_at_write);

    // And new writes after recovery are exact again.
    store.put("/early", json!(2)).unwrap();
    assert_eq!(store.last_modified("/early").unwrap().version, v + 1);
    assert_eq!(store.last_modified("/late").unwrap().version, v);
}
