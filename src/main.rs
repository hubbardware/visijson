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
    // REBASE=0 disables the lost-CAS rebase fast path (on by default).
    let rebase_on_conflict = !matches!(
        std::env::var("REBASE").as_deref(),
        Ok("0") | Ok("false") | Ok("off")
    );
    let cfg = StoreConfig {
        data_dir: data_dir.clone(),
        durability,
        rebase_on_conflict,
        ..StoreConfig::default()
    };
    let store = Store::open(cfg)?;

    // AUTH=on enables users and permissions (off by default: everyone is
    // unrestricted). Identity data lives in its own store under
    // DATA_DIR/sys, always fsync-durable — a lost session or grant
    // revocation must never come back after a crash.
    let auth_on = matches!(std::env::var("AUTH").as_deref(), Ok("on") | Ok("1") | Ok("true"));
    let sys = auth_on
        .then(|| {
            Store::open(StoreConfig {
                data_dir: data_dir.as_ref().map(|d| d.join("sys")),
                ..StoreConfig::default()
            })
        })
        .transpose()?;
    let auth = match &sys {
        Some(sys) => {
            let env_secs = |name: &str, default: u64| {
                std::env::var(name)
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(default)
            };
            let cfg = visijson::auth::AuthConfig {
                session_ttl_ms: env_secs("SESSION_TTL_SECS", 7 * 24 * 3600) * 1000,
                cookie_secure: matches!(std::env::var("COOKIE_SECURE").as_deref(), Ok("1") | Ok("true")),
                ..Default::default()
            };
            let (auth, bootstrap) = visijson::auth::Auth::open(sys.clone(), cfg)?;
            if let Some(key) = bootstrap {
                // Shown exactly once: only its hash is stored.
                tracing::warn!(
                    principal = %key.principal,
                    "no principals yet — bootstrapped an admin. Its API key (shown once): {}",
                    key.key
                );
            }
            let sweeper = auth.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(600));
                loop {
                    interval.tick().await;
                    let a = sweeper.clone();
                    if let Ok(Err(e)) = tokio::task::spawn_blocking(move || a.sweep()).await {
                        tracing::warn!(error = %e, "session sweep failed");
                    }
                }
            });
            tracing::info!("auth enabled");
            Some(auth)
        }
        None => None,
    };
    tracing::info!(version = store.version(), "store opened");

    let snapshot_secs: u64 = std::env::var("SNAPSHOT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    if snapshot_secs > 0 {
        let snap_stores: Vec<Arc<Store>> = std::iter::once(store.clone()).chain(sys.clone()).collect();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(snapshot_secs));
            interval.tick().await; // first tick is immediate; skip it
            loop {
                interval.tick().await;
                for s in &snap_stores {
                    // Serializing a large document is CPU-bound; keep it off
                    // the async workers.
                    let s = s.clone();
                    match tokio::task::spawn_blocking(move || s.snapshot()).await {
                        Ok(Ok(_)) => {}
                        Ok(Err(e)) => tracing::error!(error = %e, "periodic snapshot failed"),
                        Err(e) => tracing::error!(error = %e, "snapshot task failed"),
                    }
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
        api::spawn_info_ticker(info.clone(), store.clone(), Duration::from_secs(info_tick_secs));
    }

    let addr = std::env::var("ADDR").unwrap_or_else(|_| "127.0.0.1:8080".into());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "listening");
    axum::serve(listener, api::router_with_auth(store, info, auth)).await?;
    Ok(())
}
