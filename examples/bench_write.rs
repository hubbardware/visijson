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
    for durability in ["mem", "nofsync", "async", "fsync"] {
        let ops_per = if durability == "fsync" { 100 } else { 5000 };
        for writers in [1, 2, 4, 8] {
            let (store, _dir) = open(durability);
            run(durability, &store, writers, ops_per, false);
        }
    }
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
