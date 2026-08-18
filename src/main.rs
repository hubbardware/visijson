use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use visijson::api;
use visijson::store::{Store, StoreConfig};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // DATA_DIR=none disables persistence entirely.
    let data_dir = match std::env::var("DATA_DIR").as_deref() {
        Ok("none") => None,
        Ok(dir) => Some(PathBuf::from(dir)),
        Err(_) => Some(PathBuf::from("./data")),
    };
    // DURABILITY=sync|async|nofsync — what a write ack promises (default sync).
    let durability = match std::env::var("DURABILITY").as_deref() {
        Ok("async") => visijson::Durability::Async,
        Ok("nofsync") | Ok("none") => visijson::Durability::NoFsync,
        _ => visijson::Durability::Sync,
    };
    let cfg = StoreConfig {
        data_dir,
        durability,
        ..StoreConfig::default()
    };
    let store = Store::open(cfg)?;
    tracing::info!(version = store.version(), "store opened");

    let snapshot_secs: u64 = std::env::var("SNAPSHOT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    if snapshot_secs > 0 {
        let snap_store: Arc<Store> = store.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(snapshot_secs));
            interval.tick().await; // first tick is immediate; skip it
            loop {
                interval.tick().await;
                if let Err(e) = snap_store.snapshot() {
                    tracing::error!(error = %e, "periodic snapshot failed");
                }
            }
        });
    }

    // Liveness heartbeat at /info — its own in-memory store, a sibling of
    // /doc, so the churn never reaches document subscribers or the WAL.
    // Once per second by default (0 disables).
    let info = Store::in_memory();
    let info_tick_secs: u64 = std::env::var("INFO_TICK_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    if info_tick_secs > 0 {
        api::spawn_info_ticker(info.clone(), Duration::from_secs(info_tick_secs));
    }

    let addr = std::env::var("ADDR").unwrap_or_else(|_| "127.0.0.1:8080".into());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "listening");
    axum::serve(listener, api::router(store, info)).await?;
    Ok(())
}
