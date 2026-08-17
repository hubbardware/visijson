//! Subscription semantics at the store level: consistent initial snapshot,
//! strictly ordered gap-free events, and path relevance filtering.

use std::sync::Arc;

use visijson::model::Pointer;
use visijson::store::Store;
use visijson::subscriptions::is_relevant;
use serde_json::json;

#[tokio::test]
async fn events_arrive_in_version_order_without_gaps() {
    let store = Store::in_memory();
    store.put("/n", json!(0)).unwrap();
    let (snapshot, mut rx) = store.subscribe();
    assert_eq!(snapshot.version, 1);

    for i in 1..=20i64 {
        store.put("/n", json!(i)).unwrap();
    }

    for expected in (snapshot.version + 1..).take(20) {
        let ev = rx.recv().await.unwrap();
        assert_eq!(ev.version, expected, "gap or reorder in event stream");
    }
}

#[tokio::test]
async fn snapshot_and_stream_are_consistent_under_concurrent_writes() {
    // Subscribe while writers are hammering the store; the first event after
    // the snapshot must be exactly snapshot.version + 1 — no gaps, no dupes.
    let store = Store::in_memory();
    store.put("/n", json!(0)).unwrap();

    let writer = {
        let store = Arc::clone(&store);
        tokio::task::spawn_blocking(move || {
            for i in 0..300i64 {
                store.put("/n", json!(i)).unwrap();
            }
        })
    };

    for _ in 0..20 {
        let (snapshot, mut rx) = store.subscribe();
        // The next relevant event (writers touch /n every commit) must be
        // contiguous with the snapshot.
        if let Ok(ev) =
            tokio::time::timeout(std::time::Duration::from_millis(200), rx.recv()).await
        {
            let ev = ev.unwrap();
            assert_eq!(
                ev.version,
                snapshot.version + 1,
                "snapshot v{} not contiguous with first event v{}",
                snapshot.version,
                ev.version
            );
        }
    }
    writer.await.unwrap();
}

#[tokio::test]
async fn concurrent_writers_produce_a_single_ordered_stream() {
    let store = Store::in_memory();
    let (snapshot, mut rx) = store.subscribe();
    assert_eq!(snapshot.version, 0);

    const THREADS: usize = 6;
    const WRITES: usize = 25;
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let store = Arc::clone(&store);
        handles.push(tokio::task::spawn_blocking(move || {
            for i in 0..WRITES {
                store.put(&format!("/t{t}"), json!(i)).unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let total = (THREADS * WRITES) as u64;
    for expected in 1..=total {
        let ev = rx.recv().await.unwrap();
        assert_eq!(ev.version, expected, "event stream must be gap-free");
        // The event carries the exact committed state for its version.
        assert_eq!(ev.committed.version, ev.version);
    }
}

#[test]
fn relevance_is_prefix_symmetric() {
    let sub = Pointer::parse("/a/b").unwrap();
    assert!(is_relevant(&sub, "/a/b"));
    assert!(is_relevant(&sub, "/a/b/c")); // change below the subscription
    assert!(is_relevant(&sub, "/a")); // change above the subscription
    assert!(is_relevant(&sub, "")); // root replacement affects everyone
    assert!(!is_relevant(&sub, "/a/x"));
    assert!(!is_relevant(&sub, "/other"));

    let root_sub = Pointer::root();
    assert!(is_relevant(&root_sub, "/anything/at/all"));
}

#[tokio::test]
async fn formula_changes_are_delivered_as_changed_paths() {
    let store = Store::in_memory();
    store.put("/a", json!(1)).unwrap();
    store.put("/total", json!({"$formula": "/a * 10"})).unwrap();

    let (_, mut rx) = store.subscribe();
    store.put("/a", json!(5)).unwrap();
    let ev = rx.recv().await.unwrap();
    assert_eq!(
        *ev.changed_paths,
        vec!["/a".to_string(), "/total".to_string()]
    );
    // A subscriber watching only /total would match via changed_paths.
    let sub = Pointer::parse("/total").unwrap();
    assert!(ev.changed_paths.iter().any(|c| is_relevant(&sub, c)));
}
