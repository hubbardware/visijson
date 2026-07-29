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
fn put_records_only_genuinely_changed_paths() {
    let store = Store::in_memory();
    store
        .put("/info", json!({"time": "t1", "uptime": 1, "started": "t0"}))
        .unwrap();

    // Rewrite the whole object with one field identical: `started` must NOT
    // be reported as changed.
    let info = store
        .put("/info", json!({"time": "t2", "uptime": 2, "started": "t0"}))
        .unwrap();
    assert_eq!(info.changed_paths, vec!["/info/time", "/info/uptime"]);

    // Fully identical rewrite → nothing changed (version still advances).
    let info = store
        .put("/info", json!({"time": "t2", "uptime": 2, "started": "t0"}))
        .unwrap();
    assert!(info.changed_paths.is_empty(), "got {:?}", info.changed_paths);
    assert_eq!(info.version, 3);

    // Added and removed keys are each reported.
    let info = store
        .put("/info", json!({"time": "t2", "uptime": 2, "extra": true}))
        .unwrap();
    assert_eq!(info.changed_paths, vec!["/info/extra", "/info/started"]);

    // Same-length arrays diff per index; changed shape reports the array.
    store.put("/arr", json!([1, 2, 3])).unwrap();
    let info = store.put("/arr", json!([1, 9, 3])).unwrap();
    assert_eq!(info.changed_paths, vec!["/arr/1"]);
    let info = store.put("/arr", json!([1, 9])).unwrap();
    assert_eq!(info.changed_paths, vec!["/arr"]);

    // Type change reports the path itself.
    let info = store.put("/arr", json!("now a string")).unwrap();
    assert_eq!(info.changed_paths, vec!["/arr"]);
}

#[test]
fn array_insert_and_remove_report_the_whole_array() {
    use jsondocstore::store::Op;
    let store = Store::in_memory();
    store.put("/l", json!([1, 2, 3])).unwrap();

    // Insert shifts later siblings, so the array itself is the changed path.
    let info = store
        .commit(
            vec![Op::Insert {
                path: "/l/1".into(),
                value: json!(9),
            }],
            &[],
        )
        .unwrap();
    assert_eq!(info.changed_paths, vec!["/l"]);
    assert_eq!(store.get("/l").unwrap(), Some(json!([1, 9, 2, 3])));

    let info = store.remove("/l/0").unwrap();
    assert_eq!(info.changed_paths, vec!["/l"]);
    assert_eq!(store.get("/l").unwrap(), Some(json!([9, 2, 3])));

    // Object key removal stays precise.
    store.put("/o", json!({"a": 1, "b": 2})).unwrap();
    let info = store.remove("/o/a").unwrap();
    assert_eq!(info.changed_paths, vec!["/o/a"]);
}

#[test]
fn merge_op_follows_rfc7396() {
    let store = Store::in_memory();
    store
        .put("", json!({"title": "Goodbye!", "author": {"givenName": "John", "familyName": "Doe"},
                        "tags": ["example", "sample"], "content": "This will be unchanged"}))
        .unwrap();

    // The example straight from RFC 7396 §1.
    let info = store
        .merge(
            "",
            json!({"title": "Hello!", "phoneNumber": "+01-123-456-7890",
                   "author": {"familyName": null}, "tags": ["example"]}),
        )
        .unwrap();
    assert_eq!(
        store.get("").unwrap(),
        Some(json!({"title": "Hello!", "author": {"givenName": "John"},
                    "tags": ["example"], "content": "This will be unchanged",
                    "phoneNumber": "+01-123-456-7890"}))
    );
    assert_eq!(
        info.changed_paths,
        vec!["/author/familyName", "/phoneNumber", "/tags", "/title"]
    );

    // Non-object patch replaces wholesale; merge into a scalar treats it as {}.
    store.merge("/tags", json!("just a string")).unwrap();
    assert_eq!(store.get("/tags").unwrap(), Some(json!("just a string")));
    store.merge("/tags", json!({"now": "object"})).unwrap();
    assert_eq!(store.get("/tags").unwrap(), Some(json!({"now": "object"})));
}

#[test]
fn identical_put_does_not_wake_formulas() {
    let store = Store::in_memory();
    store.put("/a", json!(5)).unwrap();
    store.put("/f", json!({"$formula": "/a * 2"})).unwrap();
    // Re-putting the same value produces no changed paths, so the dependent
    // formula must not appear in changed paths either.
    let info = store.put("/a", json!(5)).unwrap();
    assert!(info.changed_paths.is_empty(), "got {:?}", info.changed_paths);
    assert_eq!(store.get("/f").unwrap(), Some(json!(10)));
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
