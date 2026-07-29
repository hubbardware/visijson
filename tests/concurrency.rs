//! Concurrency: many writers racing on the same and disjoint paths, with
//! CAS-retried commits and optimistic transaction retries.

use std::sync::Arc;
use std::thread;

use jsondocstore::store::Store;
use serde_json::json;

#[test]
fn concurrent_transactional_increments_lose_no_updates() {
    let store = Store::in_memory();
    store.put("/counter", json!(0)).unwrap();

    const THREADS: usize = 8;
    const INCREMENTS: usize = 50;

    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                let mut retries = 0u32;
                for _ in 0..INCREMENTS {
                    let (_, _, r) = store
                        .transaction(|txn| {
                            let cur = txn
                                .get("/counter")?
                                .and_then(|v| v.as_i64())
                                .unwrap_or(0);
                            txn.put("/counter", json!(cur + 1))?;
                            Ok(())
                        })
                        .expect("transaction should eventually succeed");
                    retries += r;
                }
                retries
            })
        })
        .collect();

    // The invariant is the count: every increment must land exactly once.
    // (Retry counts vary with scheduling — the deterministic proof of the
    // conflict/retry path is read_modify_write_conflict_forces_retry.)
    let total_retries: u32 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    assert_eq!(
        store.get("/counter").unwrap(),
        Some(json!((THREADS * INCREMENTS) as i64))
    );
    println!("total retries under contention: {total_retries}");
}

#[test]
fn concurrent_disjoint_writes_all_land() {
    let store = Store::in_memory();
    const THREADS: usize = 8;
    const WRITES: usize = 30;

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                for i in 0..WRITES {
                    store.put(&format!("/t{t}/k{i}"), json!(i)).unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    for t in 0..THREADS {
        for i in 0..WRITES {
            assert_eq!(
                store.get(&format!("/t{t}/k{i}")).unwrap(),
                Some(json!(i)),
                "lost write at /t{t}/k{i}"
            );
        }
    }
    // Every commit got a distinct version.
    assert_eq!(store.version(), (THREADS * WRITES) as u64);
}

#[test]
fn concurrent_appends_lose_no_elements() {
    let store = Store::in_memory();
    store.put("/list", json!([])).unwrap();
    const THREADS: usize = 6;
    const APPENDS: usize = 40;

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                for i in 0..APPENDS {
                    store.append("/list", json!(format!("{t}-{i}"))).unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let list = store.get("/list").unwrap().unwrap();
    let items = list.as_array().unwrap();
    assert_eq!(items.len(), THREADS * APPENDS);
    // Per-thread order is preserved (each thread's appends are sequential).
    for t in 0..THREADS {
        let mine: Vec<&str> = items
            .iter()
            .filter_map(|v| v.as_str())
            .filter(|s| s.starts_with(&format!("{t}-")))
            .collect();
        let expected: Vec<String> = (0..APPENDS).map(|i| format!("{t}-{i}")).collect();
        assert_eq!(mine, expected);
    }
}

#[test]
fn readers_never_see_torn_state() {
    // A writer commits {a: n, b: n} pairs; readers must always observe a == b.
    let store = Store::in_memory();
    store.put("/pair", json!({"a": 0, "b": 0})).unwrap();

    let writer = {
        let store = Arc::clone(&store);
        thread::spawn(move || {
            for n in 1..=500i64 {
                store.put("/pair", json!({"a": n, "b": n})).unwrap();
            }
        })
    };
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                for _ in 0..2000 {
                    let v = store.get("/pair").unwrap().unwrap();
                    assert_eq!(v["a"], v["b"], "torn read: {v}");
                }
            })
        })
        .collect();
    writer.join().unwrap();
    for r in readers {
        r.join().unwrap();
    }
}
