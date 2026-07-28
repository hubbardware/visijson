//! End-to-end API tests over a real TCP listener: REST verbs, transactions,
//! and WebSocket subscriptions (initial snapshot + ordered change events).

use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use jsondocstore::api::router;
use jsondocstore::store::Store;
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;

async fn serve() -> (String, Arc<Store>) {
    let store = Store::in_memory();
    let app = router(store.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), store)
}

#[tokio::test]
async fn rest_crud_roundtrip() {
    let (base, _store) = serve().await;
    let client = reqwest::Client::new();

    // PUT
    let resp = client
        .put(format!("{base}/doc/users/alice"))
        .json(&json!({"age": 30, "tags": ["admin"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let info: Value = resp.json().await.unwrap();
    assert_eq!(info["version"], 1);
    assert_eq!(info["changed_paths"], json!(["/users/alice"]));

    // GET
    let v: Value = client
        .get(format!("{base}/doc/users/alice/age"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v, json!(30));

    // GET missing → 404
    let resp = client.get(format!("{base}/doc/nope")).send().await.unwrap();
    assert_eq!(resp.status(), 404);

    // POST appends
    let resp = client
        .post(format!("{base}/doc/users/alice/tags"))
        .json(&json!("ops"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = client
        .get(format!("{base}/doc/users/alice/tags"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v, json!(["admin", "ops"]));

    // DELETE
    let resp = client
        .delete(format!("{base}/doc/users/alice/age"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let resp = client
        .get(format!("{base}/doc/users/alice/age"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    // GET whole document
    let v: Value = client.get(format!("{base}/doc")).send().await.unwrap().json().await.unwrap();
    assert_eq!(v, json!({"users": {"alice": {"tags": ["admin", "ops"]}}}));
}

#[tokio::test]
async fn patch_add_replace_remove_test() {
    let (base, _store) = serve().await;
    let client = reqwest::Client::new();
    client
        .put(format!("{base}/doc"))
        .json(&json!({"list": [1, 3], "keep": true, "drop": 1}))
        .send()
        .await
        .unwrap();

    let patch = json!([
        {"op": "add", "path": "/list/1", "value": 2},
        {"op": "replace", "path": "/keep", "value": false},
        {"op": "remove", "path": "/drop"},
        {"op": "test", "path": "/list/0", "value": 1},
    ]);
    let resp = client
        .patch(format!("{base}/doc"))
        .json(&patch)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = client.get(format!("{base}/doc")).send().await.unwrap().json().await.unwrap();
    assert_eq!(v, json!({"list": [1, 2, 3], "keep": false}));

    // Failing `test` op → 409 and no change
    let patch = json!([
        {"op": "test", "path": "/keep", "value": true},
        {"op": "replace", "path": "/keep", "value": "never"},
    ]);
    let resp = client
        .patch(format!("{base}/doc"))
        .json(&patch)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);
    let v: Value = client.get(format!("{base}/doc/keep")).send().await.unwrap().json().await.unwrap();
    assert_eq!(v, json!(false));
}

#[tokio::test]
async fn txn_endpoint_applies_atomically_with_conditions() {
    let (base, _store) = serve().await;
    let client = reqwest::Client::new();
    client
        .put(format!("{base}/doc/bal"))
        .json(&json!(100))
        .send()
        .await
        .unwrap();

    let txn = json!({
        "ops": [
            {"op": "put", "path": "/bal", "value": 70},
            {"op": "append", "path": "/ledger", "value": {"delta": -30}}
        ],
        "conditions": [{"path": "/bal", "expected": 100}]
    });
    let resp = client.post(format!("{base}/txn")).json(&txn).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    // Same conditional txn again: /bal is now 70, so it must 409.
    let resp = client.post(format!("{base}/txn")).json(&txn).send().await.unwrap();
    assert_eq!(resp.status(), 409);

    let v: Value = client.get(format!("{base}/doc")).send().await.unwrap().json().await.unwrap();
    assert_eq!(v, json!({"bal": 70, "ledger": [{"delta": -30}]}));
}

#[tokio::test]
async fn formulas_over_rest() {
    let (base, _store) = serve().await;
    let client = reqwest::Client::new();
    client.put(format!("{base}/doc/a")).json(&json!(6)).send().await.unwrap();
    client
        .put(format!("{base}/doc/double"))
        .json(&json!({"$formula": "/a * 2"}))
        .send()
        .await
        .unwrap();

    let v: Value = client.get(format!("{base}/doc/double")).send().await.unwrap().json().await.unwrap();
    assert_eq!(v, json!(12));

    let raw: Value = client
        .get(format!("{base}/doc/double?raw=true"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(raw["$formula"], json!("/a * 2"));

    // Introducing a cycle over REST → 422
    let resp = client
        .put(format!("{base}/doc/cycle"))
        .json(&json!({"$formula": "/cycle + 1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 422);
}

#[tokio::test]
async fn browsers_get_the_embedded_ui_api_clients_get_json() {
    let (base, store) = serve().await;
    store.put("/a", json!(1)).unwrap();
    let client = reqwest::Client::new();

    // Browser-style Accept → HTML app, even for paths that don't exist yet.
    for path in ["/doc", "/doc/a", "/doc/not/yet/created"] {
        let resp = client
            .get(format!("{base}{path}"))
            .header("accept", "text/html,application/xhtml+xml,*/*;q=0.8")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "GET {path}");
        let ct = resp.headers()["content-type"].to_str().unwrap().to_string();
        assert!(ct.starts_with("text/html"), "GET {path} → {ct}");
        let body = resp.text().await.unwrap();
        assert!(body.contains("jsondocstore"), "UI page not served for {path}");
    }

    // JSON Accept (and curl's */*) still get data + real 404s.
    let v: Value = client
        .get(format!("{base}/doc/a"))
        .header("accept", "application/json")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v, json!(1));
    let resp = client
        .get(format!("{base}/doc/a"))
        .header("accept", "*/*")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.json::<Value>().await.unwrap(), json!(1));

    // ?raw=true from a browser stays JSON (address-bar escape hatch).
    let resp = client
        .get(format!("{base}/doc/a?raw=true"))
        .header("accept", "text/html,*/*;q=0.8")
        .send()
        .await
        .unwrap();
    assert!(resp.headers()["content-type"]
        .to_str()
        .unwrap()
        .starts_with("application/json"));
}

#[tokio::test]
async fn info_ticker_heartbeats() {
    let (_base, store) = serve().await;
    jsondocstore::api::spawn_info_ticker(store.clone(), std::time::Duration::from_millis(20));

    tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    let info = store.get("/info").unwrap().expect("/info should exist");
    let time = info["time"].as_str().unwrap();
    // RFC 3339 shape: 2026-07-28T12:34:56Z
    assert_eq!(time.len(), 20, "unexpected time format: {time}");
    assert!(time.ends_with('Z') && time.contains('T'));
    assert!(time.starts_with("20"), "implausible year in {time}");
    assert!(info["epoch_ms"].as_u64().unwrap() > 1_700_000_000_000);
    assert!(info["uptime_secs"].is_u64());
    assert_eq!(info["started"].as_str().unwrap().len(), 20);

    // It keeps ticking: version advances and subscribers see /info changes.
    let v1 = store.version();
    let (_, mut rx) = store.subscribe();
    let ev = rx.recv().await.unwrap();
    assert!(ev.changed_paths.iter().any(|p| p == "/info"));
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    assert!(store.version() > v1, "ticker stopped advancing versions");
}

#[tokio::test]
async fn websocket_snapshot_then_ordered_filtered_changes() {
    let (base, store) = serve().await;
    let ws_url = format!("{}/ws?path=/watched", base.replace("http", "ws"));

    store.put("/watched", json!({"n": 0})).unwrap();
    store.put("/ignored", json!(0)).unwrap();

    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.unwrap();

    // 1. Initial snapshot reflects current state.
    let msg: Value = match ws.next().await.unwrap().unwrap() {
        Message::Text(t) => serde_json::from_str(&t).unwrap(),
        m => panic!("expected text, got {m:?}"),
    };
    assert_eq!(msg["type"], "snapshot");
    assert_eq!(msg["value"], json!({"n": 0}));
    let snap_version = msg["version"].as_u64().unwrap();

    // 2. Mixed relevant and irrelevant writes.
    store.put("/ignored", json!(1)).unwrap(); // must NOT be delivered
    store.put("/watched/n", json!(1)).unwrap();
    store.put("/ignored", json!(2)).unwrap(); // must NOT be delivered
    store.put("/watched/n", json!(2)).unwrap();

    let mut seen = Vec::new();
    while seen.len() < 2 {
        let msg: Value = match ws.next().await.unwrap().unwrap() {
            Message::Text(t) => serde_json::from_str(&t).unwrap(),
            Message::Ping(_) => continue,
            m => panic!("expected text, got {m:?}"),
        };
        assert_eq!(msg["type"], "change");
        assert_eq!(msg["changed_paths"], json!(["/watched/n"]));
        seen.push(msg);
    }

    // 3. Ordering: versions strictly increase and start after the snapshot.
    let v1 = seen[0]["version"].as_u64().unwrap();
    let v2 = seen[1]["version"].as_u64().unwrap();
    assert!(v1 > snap_version);
    assert!(v2 > v1);
    assert_eq!(seen[0]["value"], json!({"n": 1}));
    assert_eq!(seen[1]["value"], json!({"n": 2}));

    ws.send(Message::Close(None)).await.unwrap();
}

#[tokio::test]
async fn websocket_subscribers_watching_different_paths() {
    let (base, store) = serve().await;
    store.put("/a", json!(0)).unwrap();
    store.put("/b", json!(0)).unwrap();

    let ws = |p: &str| {
        let url = format!("{}/ws?path={}", base.replace("http", "ws"), p);
        async move { tokio_tungstenite::connect_async(url).await.unwrap().0 }
    };
    let mut ws_a = ws("/a").await;
    let mut ws_b = ws("/b").await;

    // Drain both snapshots.
    for s in [&mut ws_a, &mut ws_b] {
        let msg = s.next().await.unwrap().unwrap();
        let v: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        assert_eq!(v["type"], "snapshot");
    }

    store.put("/a", json!(1)).unwrap();
    store.put("/b", json!(2)).unwrap();

    async fn next<S>(s: &mut tokio_tungstenite::WebSocketStream<S>) -> Value
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let msg = s.next().await.unwrap().unwrap();
        serde_json::from_str::<Value>(msg.to_text().unwrap()).unwrap()
    }
    let a_msg = next(&mut ws_a).await;
    assert_eq!(a_msg["changed_paths"], json!(["/a"]));
    assert_eq!(a_msg["value"], json!(1));
    let b_msg = next(&mut ws_b).await;
    assert_eq!(b_msg["changed_paths"], json!(["/b"]));
    assert_eq!(b_msg["value"], json!(2));
}
