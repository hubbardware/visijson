//! Crash recovery: WAL replay, snapshot + WAL, torn tail tolerance, and
//! formula state surviving restart.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use visijson::store::{Store, StoreConfig};
use serde_json::json;
use tempfile::TempDir;

fn open(dir: &Path) -> std::sync::Arc<Store> {
    Store::open(StoreConfig {
        data_dir: Some(dir.to_path_buf()),
        fsync: false, // tests simulate process death, not power loss
        ..StoreConfig::default()
    })
    .unwrap()
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
    // Simulate a crash mid-append: garbage half-record at the WAL tail.
    let mut f = OpenOptions::new()
        .append(true)
        .open(dir.path().join("wal.log"))
        .unwrap();
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

    let wal = std::fs::read_to_string(dir.path().join("wal.log")).unwrap();
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
