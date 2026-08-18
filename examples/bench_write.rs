//! Write-throughput sweep: writers × durability × workload, reporting
//! CAS losses and fsync batching from `Store::stats()`. Run with
//! `cargo run --release --example bench_write`.
//!
//! Every concurrent commit contends on the single root CAS, so "contention"
//! is the writer count itself; keys are disjoint (and values always change)
//! so no commit is elided. The `formulas` workload pre-installs 500 formulas
//! and every write dirties exactly one of them.

use std::sync::Arc;
use std::time::Instant;

use serde_json::json;
use tempfile::TempDir;
use visijson::store::{Store, StoreConfig};
use visijson::Durability;

fn open(durability: &str) -> (Arc<Store>, Option<TempDir>) {
    match durability {
        "mem" => (Store::in_memory(), None),
        other => {
            let dir = TempDir::new().unwrap();
            let store = Store::open(StoreConfig {
                data_dir: Some(dir.path().to_path_buf()),
                durability: match other {
                    "fsync" => Durability::Sync,
                    "async" => Durability::Async,
                    _ => Durability::NoFsync,
                },
                ..StoreConfig::default()
            })
            .unwrap();
            (store, Some(dir))
        }
    }
}

fn run(label: &str, store: &Arc<Store>, writers: usize, ops_per: usize, formulas: bool) {
    let before = store.stats();
    let t = Instant::now();
    std::thread::scope(|s| {
        for w in 0..writers {
            let store = store.clone();
            s.spawn(move || {
                for i in 0..ops_per {
                    let path = if formulas {
                        format!("/data/k{:03}", (w * ops_per + i) % 500)
                    } else {
                        format!("/w{w}/k{:03}", i % 100)
                    };
                    store.put(&path, json!(w * ops_per + i)).unwrap();
                }
            });
        }
    });
    let secs = t.elapsed().as_secs_f64();
    let after = store.stats();
    let ops = (writers * ops_per) as f64;
    let records = after.wal_records - before.wal_records;
    let fsyncs = after.wal_fsyncs - before.wal_fsyncs;
    let batching = if fsyncs > 0 {
        format!("{:.1} rec/fsync", records as f64 / fsyncs as f64)
    } else {
        "-".into()
    };
    println!(
        "{label:22} writers={writers:<2} {:>9.0} ops/s  p50≈{:>7.1}µs  cas_losses={:<5} wal={records:<5} fsyncs={fsyncs:<5} {batching}",
        ops / secs,
        secs * 1e6 * writers as f64 / ops,
        after.cas_losses - before.cas_losses,
    );
}

fn main() {
    // BENCH_ONLY=sweep|formulas|mixed runs a single section.
    let only = std::env::var("BENCH_ONLY").ok();
    let section = |name: &str| only.as_deref().is_none_or(|o| o == name);

    if section("sweep") {
        for durability in ["mem", "nofsync", "async", "fsync"] {
            let ops_per = if durability == "fsync" { 100 } else { 5000 };
            for writers in [1, 2, 4, 8] {
                let (store, _dir) = open(durability);
                run(durability, &store, writers, ops_per, false);
            }
        }
    }
    if section("formulas") {
        // Formula-heavy: 500 formulas, every write dirties exactly one.
        for writers in [1, 8] {
            let (store, _dir) = open("mem");
            let mut calc = serde_json::Map::new();
            for j in 0..500 {
                calc.insert(
                    format!("f{j:03}"),
                    json!({"$formula": format!("/data/k{j:03} + 1")}),
                );
            }
            store.put("/calc", serde_json::Value::Object(calc)).unwrap();
            run("mem + 500 formulas", &store, writers, 5000, true);
        }
    }
    if section("mixed") {
        // Mixed workload — the rebase target: one poll-style replacer of a
        // big subtree racing small writers. Each lost CAS costs the replacer
        // a full re-prepare unless it can splice its already-built subtree
        // onto the winner's root — without rebase it can starve outright.
        for rebase in [false, true] {
            mixed(rebase);
        }
    }
}

fn mixed(rebase: bool) {
    use std::sync::atomic::{AtomicBool, Ordering};

    let store = Store::open(StoreConfig {
        rebase_on_conflict: rebase,
        ..StoreConfig::default()
    })
    .unwrap();
    const PLAYERS: usize = 2000;
    const FIELDS: usize = 20;
    let mut players = serde_json::Map::new();
    for p in 0..PLAYERS {
        let mut fields = serde_json::Map::new();
        for f in 0..FIELDS {
            fields.insert(format!("f{f:02}"), json!(p * FIELDS + f));
        }
        players.insert(format!("u{p:04}"), serde_json::Value::Object(fields));
    }
    let big = serde_json::Value::Object(players);
    store.put("/big", big.clone()).unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let before = store.stats();
    let mut replace_time = std::time::Duration::ZERO;
    let (mut done, mut failed) = (0u32, 0u32);
    std::thread::scope(|s| {
        for w in 0..4 {
            let store = store.clone();
            let stop = stop.clone();
            s.spawn(move || {
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    // Small commits essentially can't exhaust retries, but
                    // never panic inside the scope — that would hang it.
                    let _ = store.put(&format!("/w{w}/k{:02}", i % 50), json!(i));
                    i += 1;
                }
            });
        }
        // Time-bounded: without rebase the replacer can starve to
        // RetryLimit — count that instead of waiting forever.
        let window = Instant::now();
        while done + failed < 20 && window.elapsed() < std::time::Duration::from_secs(15) {
            let mut b = big.clone();
            let obj = b["u1000"].as_object_mut().unwrap();
            obj.insert("f00".into(), json!(format!("drift{}", done + failed)));
            let t = Instant::now();
            match store.put("/big", b) {
                Ok(_) => {
                    replace_time += t.elapsed();
                    done += 1;
                }
                Err(_) => failed += 1, // RetryLimit: starved out
            }
        }
        stop.store(true, Ordering::Relaxed);
    });
    let after = store.stats();
    println!(
        "mixed rebase={rebase:<5} big replaces: {done} ok / {failed} starved, avg {:>8.1?}  cas_losses={:<6} rebases={}",
        replace_time / done.max(1),
        after.cas_losses - before.cas_losses,
        after.rebases - before.rebases,
    );
}
