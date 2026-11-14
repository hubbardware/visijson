//! Optimistic transactions: read-set validation, forced conflicts and
//! retries, conditions, and retry-limit exhaustion.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use serde_json::json;
use visijson::error::StoreError;
use visijson::store::{Condition, Op, Store, StoreConfig};

#[test]
fn read_modify_write_conflict_forces_retry() {
    let store = Store::in_memory();
    store.put("/n", json!(10)).unwrap();

    let attempts = AtomicU32::new(0);
    let interfered = AtomicU32::new(0);
    let (_, info, retries) = store
        .transaction(|txn| {
            let n = txn.get("/n")?.and_then(|v| v.as_i64()).unwrap();
            // On the first attempt only, sneak in a conflicting external
            // write between our read and our commit.
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                store.put("/n", json!(n + 100)).unwrap();
                interfered.fetch_add(1, Ordering::SeqCst);
            }
            txn.put("/n", json!(n + 1))?;
            Ok(())
        })
        .unwrap();

    assert_eq!(retries, 1, "first attempt must have been invalidated");
    assert_eq!(interfered.load(Ordering::SeqCst), 1);
    // The retry re-read n=110 and wrote 111 — the external write was not lost.
    assert_eq!(store.get("/n").unwrap(), Some(json!(111)));
    assert_eq!(info.changed_paths, vec!["/n"]);
}

#[test]
fn unrelated_writes_do_not_conflict() {
    let store = Store::in_memory();
    store.put("/a", json!(1)).unwrap();
    store.put("/b", json!(2)).unwrap();

    let first = AtomicU32::new(0);
    let (_, _, retries) = store
        .transaction(|txn| {
            let a = txn.get("/a")?.unwrap();
            if first.fetch_add(1, Ordering::SeqCst) == 0 {
                // Concurrent write to a path we never read: must not abort us.
                store.put("/b", json!(99)).unwrap();
            }
            txn.put("/a2", a)?;
            Ok(())
        })
        .unwrap();
    assert_eq!(retries, 0, "write to unread path must not invalidate txn");
    assert_eq!(store.get("/a2").unwrap(), Some(json!(1)));
    assert_eq!(store.get("/b").unwrap(), Some(json!(99)));
}

#[test]
fn txn_sees_its_own_writes() {
    let store = Store::in_memory();
    let (seen, _, _) = store
        .transaction(|txn| {
            txn.put("/x", json!(5))?;
            txn.get("/x")
        })
        .unwrap();
    assert_eq!(seen, Some(json!(5)));
}

#[test]
fn retry_limit_surfaces_as_error() {
    let store = Store::open(StoreConfig {
        max_retries: 3,
        ..StoreConfig::default()
    })
    .unwrap();
    store.put("/hot", json!(0)).unwrap();

    // Sabotage every attempt so the transaction can never validate.
    let result = store.transaction(|txn| {
        let n = txn.get("/hot")?.and_then(|v| v.as_i64()).unwrap();
        store.put("/hot", json!(n + 1000)).unwrap();
        txn.put("/hot", json!(n + 1))?;
        Ok(())
    });
    match result {
        Err(StoreError::RetryLimit(3)) => {}
        other => panic!("expected RetryLimit(3), got {other:?}"),
    }
}

#[test]
fn conditions_guard_commits() {
    let store = Store::in_memory();
    store.put("/state", json!("draft")).unwrap();

    // Passing condition
    store
        .commit(
            vec![Op::Put {
                path: "/state".into(),
                value: visijson::model::from_value(&json!("published")),
            }],
            &[Condition::value("/state", Some(json!("draft")))],
        )
        .unwrap();
    assert_eq!(store.get("/state").unwrap(), Some(json!("published")));

    // Failing condition: value moved on
    let err = store
        .commit(
            vec![Op::Put {
                path: "/state".into(),
                value: visijson::model::from_value(&json!("archived")),
            }],
            &[Condition::value("/state", Some(json!("draft")))],
        )
        .unwrap_err();
    assert!(matches!(err, StoreError::Conflict(_)));
    assert_eq!(store.get("/state").unwrap(), Some(json!("published")));

    // Absence condition
    let err = store
        .commit(
            vec![Op::Put {
                path: "/state".into(),
                value: visijson::model::from_value(&json!("x")),
            }],
            &[Condition::value("/state", None)],
        )
        .unwrap_err();
    assert!(matches!(err, StoreError::Conflict(_)));
}

#[test]
fn two_racing_transactions_serialize_correctly() {
    let store = Store::in_memory();
    store.put("/bal", json!(100)).unwrap();
    let barrier = Arc::new(Barrier::new(2));

    let handles: Vec<_> = [-30i64, -50i64]
        .into_iter()
        .map(|delta| {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let waited = AtomicU32::new(0);
                store
                    .transaction(|txn| {
                        let bal = txn.get("/bal")?.and_then(|v| v.as_i64()).unwrap();
                        // Rendezvous once so both first attempts read the
                        // same base; retries must not re-wait.
                        if waited.fetch_add(1, Ordering::SeqCst) == 0 {
                            barrier.wait();
                        }
                        txn.put("/bal", json!(bal + delta))?;
                        Ok(())
                    })
                    .unwrap()
            })
        })
        .collect();

    let mut total_retries = 0;
    for h in handles {
        let (_, _, retries) = h.join().unwrap();
        total_retries += retries;
    }
    assert_eq!(store.get("/bal").unwrap(), Some(json!(20)));
    assert!(total_retries >= 1, "one of the two must have retried");
}
