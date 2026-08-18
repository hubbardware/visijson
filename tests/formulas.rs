//! Formula correctness: evaluation, dependency-driven re-evaluation in
//! topological order, cycle rejection, and error propagation.

use visijson::error::StoreError;
use visijson::store::Store;
use serde_json::json;

fn formula(expr: &str) -> serde_json::Value {
    json!({ "$formula": expr })
}

#[test]
fn basic_arithmetic_over_references() {
    let store = Store::in_memory();
    store.put("/a", json!(2)).unwrap();
    store.put("/b", json!(3)).unwrap();
    store.put("/sum", formula("/a + /b")).unwrap();
    store.put("/expr", formula("(/a + /b) * 2 - 1")).unwrap();
    store.put("/div", formula("/b / 2")).unwrap();

    assert_eq!(store.get("/sum").unwrap(), Some(json!(5)));
    assert_eq!(store.get("/expr").unwrap(), Some(json!(9)));
    assert_eq!(store.get("/div").unwrap(), Some(json!(1.5)));
}

#[test]
fn formulas_reevaluate_when_dependencies_change() {
    let store = Store::in_memory();
    store.put("/price", json!(10)).unwrap();
    store.put("/qty", json!(4)).unwrap();
    store.put("/total", formula("/price * /qty")).unwrap();
    assert_eq!(store.get("/total").unwrap(), Some(json!(40)));

    let info = store.put("/qty", json!(5)).unwrap();
    assert_eq!(store.get("/total").unwrap(), Some(json!(50)));
    // The commit records both the written path and the recomputed formula.
    assert_eq!(info.changed_paths, vec!["/qty", "/total"]);
}

#[test]
fn chained_formulas_evaluate_in_topological_order() {
    let store = Store::in_memory();
    store.put("/x", json!(1)).unwrap();
    // Insert the chain in reverse order so naive evaluation order would fail.
    store.put("/d", formula("/c + 1")).unwrap();
    store.put("/c", formula("/b + 1")).unwrap();
    store.put("/b", formula("/x + 1")).unwrap();

    assert_eq!(store.get("/b").unwrap(), Some(json!(2)));
    assert_eq!(store.get("/c").unwrap(), Some(json!(3)));
    assert_eq!(store.get("/d").unwrap(), Some(json!(4)));

    // One change to the base ripples through the whole chain in one commit.
    let info = store.put("/x", json!(10)).unwrap();
    assert_eq!(store.get("/d").unwrap(), Some(json!(13)));
    assert_eq!(info.changed_paths, vec!["/b", "/c", "/d", "/x"]);
}

#[test]
fn formula_referencing_ancestor_subtree() {
    let store = Store::in_memory();
    store.put("/order/items", json!([10, 20, 30])).unwrap();
    store.put("/order/total", formula("sum(/order/items)")).unwrap();
    assert_eq!(store.get("/order/total").unwrap(), Some(json!(60)));

    // Changing one element re-evaluates (dependency is an ancestor overlap).
    store.put("/order/items/1", json!(25)).unwrap();
    assert_eq!(store.get("/order/total").unwrap(), Some(json!(65)));

    // Appending also re-evaluates.
    store.append("/order/items", json!(5)).unwrap();
    assert_eq!(store.get("/order/total").unwrap(), Some(json!(70)));
}

#[test]
fn aggregate_functions() {
    let store = Store::in_memory();
    store.put("/vals", json!([4, 1, 7, 2])).unwrap();
    store.put("/min", formula("min(/vals)")).unwrap();
    store.put("/max", formula("max(/vals)")).unwrap();
    store.put("/count", formula("count(/vals)")).unwrap();
    store.put("/mixed", formula("sum(/vals, 10)")).unwrap();
    assert_eq!(store.get("/min").unwrap(), Some(json!(1)));
    assert_eq!(store.get("/max").unwrap(), Some(json!(7)));
    assert_eq!(store.get("/count").unwrap(), Some(json!(4)));
    assert_eq!(store.get("/mixed").unwrap(), Some(json!(24)));
}

#[test]
fn bare_reference_copies_any_json_type() {
    let store = Store::in_memory();
    store.put("/src", json!({"nested": [1, "two"]})).unwrap();
    store.put("/mirror", formula("/src")).unwrap();
    assert_eq!(
        store.get("/mirror").unwrap(),
        Some(json!({"nested": [1, "two"]}))
    );
    store.put("/src/nested/0", json!(100)).unwrap();
    assert_eq!(
        store.get("/mirror").unwrap(),
        Some(json!({"nested": [100, "two"]}))
    );
}

#[test]
fn formula_on_formula_uses_cached_value() {
    let store = Store::in_memory();
    store.put("/a", json!(3)).unwrap();
    store.put("/sq", formula("/a * /a")).unwrap();
    store.put("/cube", formula("/sq * /a")).unwrap();
    assert_eq!(store.get("/cube").unwrap(), Some(json!(27)));
    store.put("/a", json!(2)).unwrap();
    assert_eq!(store.get("/sq").unwrap(), Some(json!(4)));
    assert_eq!(store.get("/cube").unwrap(), Some(json!(8)));
}

#[test]
fn two_formula_cycle_rejects_the_commit() {
    let store = Store::in_memory();
    store.put("/f", formula("/g + 1")).unwrap();
    // Committing /g completes the cycle f -> g -> f and must be rejected.
    let err = store.put("/g", formula("/f + 1")).unwrap_err();
    assert!(matches!(err, StoreError::FormulaCycle(_)), "got {err:?}");
    // The rejected commit left no trace.
    assert_eq!(store.get("/g").unwrap(), None);
}

#[test]
fn self_cycle_rejects_the_commit() {
    let store = Store::in_memory();
    let err = store.put("/loop", formula("/loop + 1")).unwrap_err();
    assert!(matches!(err, StoreError::FormulaCycle(_)));
}

#[test]
fn missing_reference_yields_error_value_then_recovers() {
    let store = Store::in_memory();
    store.put("/f", formula("/nope + 1")).unwrap();
    let v = store.get("/f").unwrap().unwrap();
    assert!(
        v.as_str().unwrap().starts_with("#ERR"),
        "expected error value, got {v}"
    );
    // Supplying the dependency heals the formula.
    store.put("/nope", json!(41)).unwrap();
    assert_eq!(store.get("/f").unwrap(), Some(json!(42)));
}

#[test]
fn division_by_zero_yields_error_value() {
    let store = Store::in_memory();
    store.put("/d", json!(0)).unwrap();
    store.put("/f", formula("10 / /d")).unwrap();
    let v = store.get("/f").unwrap().unwrap();
    assert!(v.as_str().unwrap().contains("division by zero"), "got {v}");
    store.put("/d", json!(4)).unwrap();
    assert_eq!(store.get("/f").unwrap(), Some(json!(2.5)));
}

#[test]
fn parse_error_rejects_the_commit() {
    let store = Store::in_memory();
    let err = store.put("/bad", formula("1 + + 2)")).unwrap_err();
    assert!(matches!(err, StoreError::FormulaParse { .. }), "got {err:?}");
    assert_eq!(store.get("/bad").unwrap(), None);
}

#[test]
fn unrelated_formula_is_not_reevaluated() {
    let store = Store::in_memory();
    store.put("/a", json!(1)).unwrap();
    store.put("/fa", formula("/a + 1")).unwrap();
    store.put("/b", json!(1)).unwrap();
    // Changing /b must not list /fa among changed paths.
    let info = store.put("/b", json!(2)).unwrap();
    assert_eq!(info.changed_paths, vec!["/b"]);
}

#[test]
fn deep_formula_chain_evaluates_without_recursion() {
    let store = Store::in_memory();
    store.put("/base", json!(0)).unwrap();
    // One Put creating a 5k-deep dependency chain — the topological sort
    // must not recurse over it.
    let mut chain = serde_json::Map::new();
    chain.insert("f0".into(), formula("/base + 1"));
    for i in 1..5000 {
        chain.insert(format!("f{i}"), formula(&format!("/chain/f{} + 1", i - 1)));
    }
    store.put("/chain", serde_json::Value::Object(chain)).unwrap();
    assert_eq!(store.get("/chain/f4999").unwrap(), Some(json!(5000)));

    // One base write ripples the whole chain in a single commit.
    store.put("/base", json!(10)).unwrap();
    assert_eq!(store.get("/chain/f4999").unwrap(), Some(json!(5010)));
}

#[test]
fn raw_read_exposes_expression_and_cache() {
    let store = Store::in_memory();
    store.put("/a", json!(1)).unwrap();
    store.put("/f", formula("/a + 1")).unwrap();
    let raw = store.get_raw("/f").unwrap().unwrap();
    assert_eq!(raw["$formula"], json!("/a + 1"));
    assert_eq!(raw["$value"], json!(2));
}

// ---------------------------------------------------------------- jq engine

#[test]
fn jq_extracts_player_names_as_array() {
    let store = Store::in_memory();
    store
        .put(
            "/players",
            json!({
                "u1": {"name": "alice", "age": 30, "level": 7},
                "u2": {"name": "bob", "age": 25, "level": 3},
            }),
        )
        .unwrap();

    // Bare values, 1-field objects, n-field objects, keyed by uid.
    store.put("/names", formula("jq(/players, '[.[] | .name]')")).unwrap();
    store
        .put("/cards", formula("jq(/players, '[.[] | {name, age}]')"))
        .unwrap();
    store
        .put("/by_uid", formula("jq(/players, 'map_values({name})')"))
        .unwrap();

    assert_eq!(store.get("/names").unwrap(), Some(json!(["alice", "bob"])));
    assert_eq!(
        store.get("/cards").unwrap(),
        Some(json!([
            {"name": "alice", "age": 30},
            {"name": "bob", "age": 25},
        ]))
    );
    assert_eq!(
        store.get("/by_uid").unwrap(),
        Some(json!({"u1": {"name": "alice"}, "u2": {"name": "bob"}}))
    );
}

#[test]
fn jq_reevaluates_when_scope_changes_only() {
    let store = Store::in_memory();
    store.put("/players/u1/name", json!("alice")).unwrap();
    store.put("/other", json!(0)).unwrap();
    store.put("/names", formula("jq(/players, '[.[] | .name]')")).unwrap();
    assert_eq!(store.get("/names").unwrap(), Some(json!(["alice"])));

    // A new player under the scope re-evaluates the view.
    let info = store.put("/players/u2/name", json!("bob")).unwrap();
    assert!(info.changed_paths.contains(&"/names".to_string()));
    assert_eq!(store.get("/names").unwrap(), Some(json!(["alice", "bob"])));

    // Writes outside the scope leave the formula untouched.
    let info = store.put("/other", json!(1)).unwrap();
    assert_eq!(info.changed_paths, vec!["/other"]);
}

#[test]
fn jq_composes_with_numeric_functions() {
    let store = Store::in_memory();
    store
        .put("/players", json!({"u1": {"age": 30}, "u2": {"age": 20}}))
        .unwrap();
    store
        .put("/avg_age", formula("sum(jq(/players, '[.[] | .age]')) / count(jq(/players, '[.[] | .age]'))"))
        .unwrap();
    assert_eq!(store.get("/avg_age").unwrap(), Some(json!(25)));
}

#[test]
fn jq_errors_surface_and_heal() {
    let store = Store::in_memory();
    // Missing scope → #ERR that heals when the scope appears.
    store.put("/names", formula("jq(/players, '[.[] | .name]')")).unwrap();
    let v = store.get("/names").unwrap().unwrap();
    assert!(v.as_str().unwrap().starts_with("#ERR"), "got {v}");
    store.put("/players/u1/name", json!("alice")).unwrap();
    assert_eq!(store.get("/names").unwrap(), Some(json!(["alice"])));

    // A stream of outputs is rejected with a hint, not silently truncated.
    store.put("/players/u2/name", json!("bob")).unwrap();
    store.put("/stream", formula("jq(/players, '.[] | .name')")).unwrap();
    let v = store.get("/stream").unwrap().unwrap();
    assert!(v.as_str().unwrap().contains("[...]"), "got {v}");

    // Broken programs are a parse-time #ERR, not a rejected commit.
    store.put("/broken", formula("jq(/players, '][')")).unwrap();
    let v = store.get("/broken").unwrap().unwrap();
    assert!(v.as_str().unwrap().starts_with("#ERR"), "got {v}");
}

#[test]
fn jq_view_depending_on_own_scope_is_a_cycle() {
    let store = Store::in_memory();
    store.put("/players/u1/name", json!("alice")).unwrap();
    // A formula inside its own scope must be rejected as a cycle.
    let err = store
        .put("/players/view", formula("jq(/players, '[.[]]')"))
        .unwrap_err();
    assert!(matches!(err, StoreError::FormulaCycle(_)), "got {err:?}");
}

// ------------------------------------------------- formula index maintenance

#[test]
fn formula_in_array_survives_sibling_insert_shift() {
    use visijson::store::Op;
    let store = Store::in_memory();
    store.put("/x", json!(1)).unwrap();
    store.put("/arr", json!([{"$formula": "/x + 1"}, "plain"])).unwrap();
    assert_eq!(store.get("/arr/0").unwrap(), Some(json!(2)));

    // Insert at the front: the formula shifts to /arr/1. The index must
    // follow it, so a dependency change still finds and updates it.
    store
        .commit(
            vec![Op::Insert {
                path: "/arr/0".into(),
                value: visijson::model::from_value(&json!("first")),
            }],
            &[],
        )
        .unwrap();
    assert_eq!(store.get("/arr/1").unwrap(), Some(json!(2)));
    store.put("/x", json!(10)).unwrap();
    assert_eq!(store.get("/arr/1").unwrap(), Some(json!(11)));
}

#[test]
fn formulas_created_and_destroyed_by_deep_subtree_writes() {
    let store = Store::in_memory();
    // Formula born inside a larger object write, not written directly.
    store
        .put("/data", json!({"a": {"f": {"$formula": "/data/a/x * 2"}, "x": 3}}))
        .unwrap();
    assert_eq!(store.get("/data/a/f").unwrap(), Some(json!(6)));
    store.put("/data/a/x", json!(5)).unwrap();
    assert_eq!(store.get("/data/a/f").unwrap(), Some(json!(10)));

    // Overwriting the subtree destroys the formula; later dependency
    // changes must not resurrect it or flag it as changed.
    store.put("/data", json!({"a": {"x": 7}})).unwrap();
    assert_eq!(store.get("/data/a/f").unwrap(), None);
    let info = store.put("/data/a/x", json!(8)).unwrap();
    assert_eq!(info.changed_paths, vec!["/data/a/x"]);
}

#[test]
fn merge_patch_can_create_and_delete_formulas() {
    let store = Store::in_memory();
    store.put("/x", json!(2)).unwrap();
    store
        .merge("", json!({"calc": {"$formula": "/x * 10"}}))
        .unwrap();
    assert_eq!(store.get("/calc").unwrap(), Some(json!(20)));

    store.merge("", json!({"calc": null})).unwrap();
    assert_eq!(store.get("/calc").unwrap(), None);
    let info = store.put("/x", json!(3)).unwrap();
    assert_eq!(info.changed_paths, vec!["/x"]);
}
