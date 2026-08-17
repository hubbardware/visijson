//! The typed fast lane: `Serialize` → Node without Value, and
//! `#[derive(Document)]` + `TypedMount` native diffing.

use std::collections::BTreeMap;

use jsondocstore::store::Store;
use jsondocstore::typed::{to_node, Document, TypedMount};
use serde_json::json;

#[derive(Document, Clone)]
struct Player {
    name: String,
    age: i64,
    active: bool,
    score: Option<f64>,
    tags: Vec<String>,
    meta: BTreeMap<String, i64>,
}

fn player(name: &str, age: i64) -> Player {
    Player {
        name: name.into(),
        age,
        active: true,
        score: Some(1.5),
        tags: vec!["a".into()],
        meta: BTreeMap::from([("x".into(), 1)]),
    }
}

#[derive(Document, Clone)]
struct World {
    players: BTreeMap<String, Player>,
    tick: u64,
}

#[test]
fn put_typed_serializes_straight_to_nodes() {
    #[derive(serde::Serialize)]
    struct Config {
        name: String,
        retries: u32,
        endpoints: Vec<String>,
    }
    let store = Store::in_memory();
    store
        .put_typed(
            "/config",
            &Config {
                name: "prod".into(),
                retries: 3,
                endpoints: vec!["a".into(), "b".into()],
            },
        )
        .unwrap();
    assert_eq!(
        store.get("/config").unwrap(),
        Some(json!({"name": "prod", "retries": 3, "endpoints": ["a", "b"]}))
    );
}

#[test]
fn to_node_recognizes_formula_shapes() {
    #[derive(serde::Serialize)]
    struct WithFormula {
        #[serde(rename = "$formula")]
        formula: String,
    }
    let n = to_node(&WithFormula {
        formula: "/a + 1".into(),
    })
    .unwrap();
    assert!(matches!(
        &*n,
        jsondocstore::model::Node::Formula(f) if f.expr == "/a + 1"
    ));
}

#[test]
fn typed_mount_syncs_with_native_diffs() {
    let store = Store::in_memory();
    let mut mount: TypedMount<World> = TypedMount::new(store.clone(), "/world").unwrap();

    let mut world = World {
        players: BTreeMap::from([
            ("u1".into(), player("alice", 30)),
            ("u2".into(), player("bob", 25)),
        ]),
        tick: 1,
    };

    // First sync PUTs the whole value.
    let info = mount.sync(world.clone()).unwrap();
    assert_eq!(info.version, 1);
    assert_eq!(
        store.get("/world/players/u1/name").unwrap(),
        Some(json!("alice"))
    );

    // Identical sync: no ops at all, no commit, no version bump.
    let info = mount.sync(world.clone()).unwrap();
    assert!(info.changed_paths.is_empty());
    assert_eq!(store.version(), 1);

    // One field drifts → exactly one changed path.
    world.players.get_mut("u1").unwrap().age = 31;
    world.tick = 2;
    let info = mount.sync(world.clone()).unwrap();
    assert_eq!(
        info.changed_paths,
        vec!["/world/players/u1/age", "/world/tick"]
    );
    assert_eq!(store.get("/world/players/u1/age").unwrap(), Some(json!(31)));

    // New player appears, one disappears → put + remove.
    world.players.insert("u3".into(), player("carol", 41));
    world.players.remove("u2");
    let info = mount.sync(world.clone()).unwrap();
    assert_eq!(
        info.changed_paths,
        vec!["/world/players/u2", "/world/players/u3"]
    );
    assert_eq!(store.get("/world/players/u2").unwrap(), None);
    assert_eq!(
        store.get("/world/players/u3/name").unwrap(),
        Some(json!("carol"))
    );

    // Option transitions: Some -> None writes null (serde convention).
    world.players.get_mut("u1").unwrap().score = None;
    let info = mount.sync(world.clone()).unwrap();
    assert_eq!(info.changed_paths, vec!["/world/players/u1/score"]);
    assert_eq!(
        store.get("/world/players/u1/score").unwrap(),
        Some(json!(null))
    );

    // Vec length change replaces the array wholesale (shift semantics).
    world.players.get_mut("u1").unwrap().tags.push("b".into());
    let info = mount.sync(world).unwrap();
    assert_eq!(info.changed_paths, vec!["/world/players/u1/tags"]);
}

#[test]
fn typed_mount_composes_with_subscriptions_and_formulas() {
    let store = Store::in_memory();
    store
        .put("/avg", json!({"$formula": "sum(jq(/world/players, '[.[] | .age]')) / count(jq(/world/players, '[.[] | .age]'))"}))
        .unwrap();

    let mut mount: TypedMount<World> = TypedMount::new(store.clone(), "/world").unwrap();
    let mut world = World {
        players: BTreeMap::from([
            ("u1".into(), player("alice", 30)),
            ("u2".into(), player("bob", 20)),
        ]),
        tick: 1,
    };
    mount.sync(world.clone()).unwrap();
    assert_eq!(store.get("/avg").unwrap(), Some(json!(25)));

    // A typed sync drives formula re-evaluation like any other commit.
    world.players.get_mut("u2").unwrap().age = 40;
    let info = mount.sync(world).unwrap();
    assert_eq!(store.get("/avg").unwrap(), Some(json!(35)));
    assert!(info.changed_paths.contains(&"/avg".to_string()));
}
