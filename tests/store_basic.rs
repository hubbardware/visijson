//! Basic store semantics: pointers, path-copy writes, structural sharing,
//! changed-path recording.

use jsondocstore::store::{shares_subtree, Store};
use serde_json::json;

#[test]
fn put_get_roundtrip() {
    let store = Store::in_memory();
    store.put("/a/b/c", json!(42)).unwrap();
    assert_eq!(store.get("/a/b/c").unwrap(), Some(json!(42)));
    assert_eq!(store.get("/a").unwrap(), Some(json!({"b": {"c": 42}})));
    assert_eq!(store.get("").unwrap(), Some(json!({"a": {"b": {"c": 42}}})));
    assert_eq!(store.get("/missing").unwrap(), None);
}

#[test]
fn versions_increment_per_commit() {
    let store = Store::in_memory();
    assert_eq!(store.version(), 0);
    let c1 = store.put("/x", json!(1)).unwrap();
    let c2 = store.put("/y", json!(2)).unwrap();
    assert_eq!(c1.version, 1);
    assert_eq!(c2.version, 2);
    assert_eq!(store.version(), 2);
}

#[test]
fn changed_paths_recorded() {
    let store = Store::in_memory();
    let info = store.put("/a/b", json!(1)).unwrap();
    assert_eq!(info.changed_paths, vec!["/a/b"]);

    store.put("/list", json!([1, 2])).unwrap();
    let info = store.append("/list", json!(3)).unwrap();
    assert_eq!(info.changed_paths, vec!["/list/2"]);
    assert_eq!(store.get("/list").unwrap(), Some(json!([1, 2, 3])));

    let info = store.remove("/a/b").unwrap();
    assert_eq!(info.changed_paths, vec!["/a/b"]);
    assert_eq!(store.get("/a").unwrap(), Some(json!({})));
}

#[test]
fn append_creates_array() {
    let store = Store::in_memory();
    store.append("/log", json!("first")).unwrap();
    store.append("/log", json!("second")).unwrap();
    assert_eq!(store.get("/log").unwrap(), Some(json!(["first", "second"])));
}

#[test]
fn array_index_writes() {
    let store = Store::in_memory();
    store.put("/arr", json!([1, 2, 3])).unwrap();
    store.put("/arr/1", json!(20)).unwrap();
    assert_eq!(store.get("/arr").unwrap(), Some(json!([1, 20, 3])));
    // next-index write appends
    store.put("/arr/3", json!(4)).unwrap();
    assert_eq!(store.get("/arr").unwrap(), Some(json!([1, 20, 3, 4])));
    // out of range fails
    assert!(store.put("/arr/10", json!(0)).is_err());
}

#[test]
fn escaped_pointer_tokens() {
    let store = Store::in_memory();
    store.put("/a~1b/x~0y", json!("escaped")).unwrap();
    assert_eq!(
        store.get("").unwrap(),
        Some(json!({"a/b": {"x~y": "escaped"}}))
    );
    assert_eq!(store.get("/a~1b/x~0y").unwrap(), Some(json!("escaped")));
}

#[test]
fn unchanged_subtrees_are_shared_between_versions() {
    let store = Store::in_memory();
    store.put("/left", json!({"deep": [1, 2, 3]})).unwrap();
    store.put("/right", json!({"other": true})).unwrap();
    let before = store.head();
    store.put("/right/other", json!(false)).unwrap();
    let after = store.head();
    // Path-copy: /left untouched, so both versions share the same Arc.
    assert!(shares_subtree(&before, &after, "/left"));
    assert!(!shares_subtree(&before, &after, "/right"));
}

#[test]
fn old_snapshots_remain_readable_after_new_commits() {
    let store = Store::in_memory();
    store.put("/v", json!(1)).unwrap();
    let old = store.head();
    store.put("/v", json!(2)).unwrap();
    // The old immutable root still reads its own value.
    let ptr = jsondocstore::model::Pointer::parse("/v").unwrap();
    let node = jsondocstore::model::get_at(&old.root, &ptr).unwrap();
    assert_eq!(jsondocstore::model::to_plain_value(&node), json!(1));
    assert_eq!(store.get("/v").unwrap(), Some(json!(2)));
}
