//! Rough diff/commit timings at production-ish shape: many objects with
//! many fields. Run with `cargo run --release --example bench_diff`.

use std::collections::BTreeMap;
use std::time::Instant;

use jsondocstore::store::Store;
use jsondocstore::typed::{Document, TypedMount};
use serde_json::{json, Map, Value};

#[derive(Document, Clone, serde::Serialize)]
struct BPlayer {
    name: String,
    age: i64,
    level: i64,
    fields: BTreeMap<String, i64>,
}

type BWorld = BTreeMap<String, BPlayer>;

fn main() {
    const PLAYERS: usize = 20_000;
    const FIELDS: usize = 110;

    let mut players = Map::new();
    for p in 0..PLAYERS {
        let mut fields = Map::new();
        for f in 0..FIELDS {
            fields.insert(format!("field_{f:03}"), json!(p * FIELDS + f));
        }
        players.insert(format!("u{p:05}"), Value::Object(fields));
    }
    let players = Value::Object(players);

    let store = Store::in_memory();
    let t = Instant::now();
    store.put("/players", players.clone()).unwrap();
    println!("initial PUT of {PLAYERS} x {FIELDS}:      {:?}", t.elapsed());

    // Worst case for the diff: replace the whole subtree with a freshly
    // parsed tree (zero structural sharing, so ptr_eq never short-circuits)
    // that differs in exactly 3 fields of one player.
    let mut changed = players.clone();
    let obj = changed["u10000"].as_object_mut().unwrap();
    obj.insert("field_000".into(), json!("renamed"));
    obj.insert("field_055".into(), json!("renamed"));
    obj.remove("field_109");

    let t = Instant::now();
    let info = store.put("/players", changed.clone()).unwrap();
    println!("full-replace PUT, 3 fields changed:  {:?}", t.elapsed());
    println!("changed paths reported: {:?}", info.changed_paths);

    // Poll-style: the source didn't change, we PUT the identical list again.
    // The commit is elided — parse + compare only, no WAL, no version bump.
    let t = Instant::now();
    let info = store.put("/players", changed).unwrap();
    println!(
        "identical full-replace (elided):     {:?} ({} changed paths)",
        t.elapsed(),
        info.changed_paths.len()
    );

    // Common case: a single-field write (path-copy, diff of one leaf).
    let t = Instant::now();
    store.put("/players/u00042/field_007", json!(1)).unwrap();
    println!("single-field PUT:                    {:?}", t.elapsed());

    // Where does single-field commit time go? Time the formula sweep alone.
    let head = store.head();
    let changed: std::collections::BTreeSet<_> =
        [jsondocstore::model::Pointer::parse("/players/u00042/field_007").unwrap()]
            .into_iter()
            .collect();
    let t = Instant::now();
    jsondocstore::formulas::reevaluate(&head.root, &changed, &head.formulas).unwrap();
    println!("formulas::reevaluate (no formulas):  {:?}", t.elapsed());

    // The one-time cost the index replaces: a full-tree formula scan.
    let t = Instant::now();
    let index = jsondocstore::formulas::scan_index(&head.root);
    println!("full scan_index (open/recovery only): {:?} ({} formulas)", t.elapsed(), index.len());

    // Typed fast lane: native struct diffing via #[derive(Document)].
    // Clones happen before the timers — a real poller hands over freshly
    // fetched data, so sync() itself is what we measure.
    let world: BWorld = (0..PLAYERS)
        .map(|p| {
            (
                format!("u{p:05}"),
                BPlayer {
                    name: format!("player{p}"),
                    age: (p % 80) as i64,
                    level: (p % 99) as i64,
                    fields: (0..FIELDS - 3)
                        .map(|f| (format!("field_{f:03}"), (p * FIELDS + f) as i64))
                        .collect(),
                },
            )
        })
        .collect();
    let tstore = Store::in_memory();
    let mut mount: TypedMount<BWorld> = TypedMount::new(tstore.clone(), "/players").unwrap();

    let first = world.clone();
    let t = Instant::now();
    mount.sync(first).unwrap();
    println!("typed initial sync:                  {:?}", t.elapsed());

    let identical = world.clone();
    let t = Instant::now();
    let info = mount.sync(identical).unwrap();
    println!(
        "typed identical sync (native diff):  {:?} ({} changed paths)",
        t.elapsed(),
        info.changed_paths.len()
    );

    let mut drifted = world.clone();
    let p = drifted.get_mut("u10000").unwrap();
    p.name = "renamed".into();
    p.level = 100;
    p.fields.insert("field_055".into(), -1);
    let t = Instant::now();
    let info = mount.sync(drifted).unwrap();
    println!(
        "typed 3-field drift sync:            {:?} ({:?})",
        t.elapsed(),
        info.changed_paths
    );

    // Middle ground: full-replace via structures. put_typed serializes the
    // struct straight to a tree (no JSON text, no Value) and the commit
    // reconciles — but with no retained copy, the whole tree is built and
    // compared every time.
    let cur = mount.last().unwrap().clone();
    let t = Instant::now();
    let info = tstore.put_typed("/players", &cur).unwrap();
    println!(
        "put_typed identical full-replace:    {:?} ({} changed paths)",
        t.elapsed(),
        info.changed_paths.len()
    );

    let mut d2 = cur.clone();
    let p = d2.get_mut("u00500").unwrap();
    p.name = "again".into();
    p.age = 999;
    p.fields.insert("field_010".into(), -2);
    let t = Instant::now();
    let info = tstore.put_typed("/players", &d2).unwrap();
    println!(
        "put_typed 3-field drift replace:     {:?} ({:?})",
        t.elapsed(),
        info.changed_paths
    );
}
