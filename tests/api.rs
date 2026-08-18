//! End-to-end API tests over a real TCP listener: REST verbs, transactions,
//! and WebSocket subscriptions (initial snapshot + ordered change events).

use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use visijson::api::router;
use visijson::store::Store;
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

async fn serve() -> (String, Arc<Store>, Arc<Store>) {
    let store = Store::in_memory();
    let info = Store::in_memory();
    let app = router(store.clone(), info.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), store, info)
}

#[tokio::test]
async fn rest_crud_roundtrip() {
    let (base, _store, _info) = serve().await;
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
    let (base, _store, _info) = serve().await;
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
    let (base, _store, _info) = serve().await;
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
    let (base, _store, _info) = serve().await;
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
    let (base, store, _info) = serve().await;
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
        assert!(body.contains("visijson"), "UI page not served for {path}");
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
async fn conditional_writes_if_match_and_if_unmodified_since() {
    let (base, _store, _info) = serve().await;
    let client = reqwest::Client::new();

    // Seed and grab the ETag (== last-modified version of that subtree).
    client
        .put(format!("{base}/doc/a"))
        .json(&json!(1))
        .send()
        .await
        .unwrap();
    let etag = client
        .get(format!("{base}/doc/a"))
        .send()
        .await
        .unwrap()
        .headers()["etag"]
        .to_str()
        .unwrap()
        .to_string();

    // Matching If-Match proceeds…
    let resp = client
        .put(format!("{base}/doc/a"))
        .header("if-match", &etag)
        .json(&json!(2))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // …and the now-stale tag earns 412, leaving the value untouched.
    let resp = client
        .put(format!("{base}/doc/a"))
        .header("if-match", &etag)
        .json(&json!(3))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 412);
    let v: Value = client
        .get(format!("{base}/doc/a"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v, json!(2));

    // `*` imposes no version constraint.
    let resp = client
        .put(format!("{base}/doc/a"))
        .header("if-match", "*")
        .json(&json!(4))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // If-Unmodified-Since in the past: 412. In the future: proceeds.
    let resp = client
        .delete(format!("{base}/doc/a"))
        .header("if-unmodified-since", "Mon, 01 Jan 2001 00:00:00 GMT")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 412);
    let resp = client
        .delete(format!("{base}/doc/a"))
        .header("if-unmodified-since", "Fri, 01 Jan 2100 00:00:00 GMT")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // PATCH (merge flavor) honors the validators too.
    client
        .put(format!("{base}/doc/obj"))
        .json(&json!({"x": 1}))
        .send()
        .await
        .unwrap();
    let etag = client
        .get(format!("{base}/doc/obj"))
        .send()
        .await
        .unwrap()
        .headers()["etag"]
        .to_str()
        .unwrap()
        .to_string();
    let resp = client
        .patch(format!("{base}/doc/obj"))
        .header("if-match", &etag)
        .json(&json!({"y": 2}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let resp = client
        .patch(format!("{base}/doc/obj"))
        .header("if-match", "\"1\"")
        .json(&json!({"z": 3}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 412);

    // /txn speaks validator conditions directly.
    let resp = client
        .post(format!("{base}/txn"))
        .json(&json!({
            "ops": [{"op": "put", "path": "/obj/x", "value": 9}],
            "conditions": [{"path": "/obj", "unmodified_version": 1}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 412);
}

#[tokio::test]
async fn info_ticker_heartbeats_beside_the_doc() {
    let (base, store, info) = serve().await;
    store.put("/seed", json!(1)).unwrap(); // doc version the ticker reports
    visijson::api::spawn_info_ticker(
        info.clone(),
        store.clone(),
        std::time::Duration::from_millis(20),
    );

    tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    let v = info.get("").unwrap().expect("info root should exist");
    let time = v["time"].as_str().unwrap();
    // RFC 3339 shape: 2026-07-28T12:34:56Z
    assert_eq!(time.len(), 20, "unexpected time format: {time}");
    assert!(time.ends_with('Z') && time.contains('T'));
    assert!(time.starts_with("20"), "implausible year in {time}");
    assert!(v["epoch_ms"].as_u64().unwrap() > 1_700_000_000_000);
    assert!(v["uptime_secs"].is_u64());
    assert_eq!(v["started"].as_str().unwrap().len(), 20);
    // /version folded into /info: the doc store's version rides the tick.
    assert_eq!(v["doc"]["version"].as_u64(), Some(store.version()));

    // Served over HTTP as a sibling of /doc: GET /info/time is the probe.
    let client = reqwest::Client::new();
    let t: Value = client
        .get(format!("{base}/info/time"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(t.as_str().unwrap().ends_with('Z'));

    // Read-only over HTTP: writes are not routed.
    let resp = client
        .put(format!("{base}/info/time"))
        .json(&json!("nope"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 405);

    // It keeps ticking, with only genuinely changed fields reported —
    // `started` is constant after the first tick and must never be flagged.
    let v1 = info.version();
    let (_, mut rx) = info.subscribe();
    let ev = rx.recv().await.unwrap();
    assert!(
        ev.changed_paths.iter().any(|p| p == "/time" || p == "/epoch_ms"),
        "got {:?}",
        ev.changed_paths
    );
    assert!(
        !ev.changed_paths.iter().any(|p| p == "/started" || p.is_empty()),
        "unchanged fields flagged: {:?}",
        ev.changed_paths
    );
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    assert!(info.version() > v1, "ticker stopped advancing versions");

    // The heartbeat is subscribable at /info like any document path...
    let ws_url = format!("{}/info", base.replace("http", "ws"));
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.unwrap();
    let snap: Value = match ws.next().await.unwrap().unwrap() {
        Message::Text(t) => serde_json::from_str(&t).unwrap(),
        m => panic!("expected text, got {m:?}"),
    };
    assert_eq!(snap["type"], "snapshot");
    assert!(snap["value"]["time"].is_string());
    let change: Value = match ws.next().await.unwrap().unwrap() {
        Message::Text(t) => serde_json::from_str(&t).unwrap(),
        m => panic!("expected text, got {m:?}"),
    };
    assert_eq!(change["type"], "change");

    // ...and the document store never saw any of the churn (still at the
    // seed commit's version).
    assert_eq!(store.version(), 1, "ticker leaked into the doc store");
}

#[tokio::test]
async fn websocket_snapshot_then_ordered_filtered_changes() {
    let (base, store, _info) = serve().await;
    let ws_url = format!("{}/doc/watched", base.replace("http", "ws"));

    store.put("/watched", json!({"n": 0})).unwrap();
    store.put("/ignored", json!(0)).unwrap();

    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.unwrap();

    // 1. Initial snapshot reflects current state.
    let msg: Value = match ws.next().await.unwrap().unwrap() {
        Message::Text(t) => serde_json::from_str(&t).unwrap(),
        m => panic!("expected text, got {m:?}"),
    };
    assert_eq!(msg["type"], "snapshot");
    assert_eq!(msg["exists"], json!(true));
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
        seen.push(msg);
    }

    // 3. Ordering: versions strictly increase and start after the snapshot.
    let v1 = seen[0]["version"].as_u64().unwrap();
    let v2 = seen[1]["version"].as_u64().unwrap();
    assert!(v1 > snap_version);
    assert!(v2 > v1);
    // Deltas are RFC 6902 patches relative to the subscription root, and
    // carry only the changed path's new value — never siblings.
    assert_eq!(
        seen[0]["patch"],
        json!([{"op": "replace", "path": "/n", "value": 1}])
    );
    assert_eq!(
        seen[1]["patch"],
        json!([{"op": "replace", "path": "/n", "value": 2}])
    );

    ws.send(Message::Close(None)).await.unwrap();
}

#[tokio::test]
async fn websocket_subscribers_watching_different_paths() {
    let (base, store, _info) = serve().await;
    store.put("/a", json!(0)).unwrap();
    store.put("/b", json!(0)).unwrap();

    let ws = |p: &str| {
        let url = format!("{}/doc{}", base.replace("http", "ws"), p);
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
    // Subscribed directly at /a and /b, so the patch path is the root: "".
    let a_msg = next(&mut ws_a).await;
    assert_eq!(a_msg["patch"], json!([{"op": "replace", "path": "", "value": 1}]));
    let b_msg = next(&mut ws_b).await;
    assert_eq!(b_msg["patch"], json!([{"op": "replace", "path": "", "value": 2}]));
}

#[tokio::test]
async fn websocket_json_patch_add_and_remove_ops() {
    let (base, store, _info) = serve().await;
    store.put("/w", json!({"keep": 1, "gone": 2})).unwrap();
    let ws_url = format!("{}/doc/w", base.replace("http", "ws"));
    let (mut ws, _) = tokio_tungstenite::connect_async(&ws_url).await.unwrap();
    ws.next().await; // snapshot

    store.put("/w/fresh", json!("new")).unwrap();
    store.remove("/w/gone").unwrap();
    store.append("/w/list", json!(1)).unwrap(); // creates the array
    store.append("/w/list", json!(2)).unwrap(); // appends → RFC 6902 add

    let mut patches = Vec::new();
    while patches.len() < 4 {
        let msg = ws.next().await.unwrap().unwrap();
        let v: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        patches.push(v["patch"].clone());
    }
    assert_eq!(patches[0], json!([{"op": "add", "path": "/fresh", "value": "new"}]));
    assert_eq!(patches[1], json!([{"op": "remove", "path": "/gone"}]));
    assert_eq!(patches[2], json!([{"op": "add", "path": "/list", "value": [1]}]));
    assert_eq!(patches[3], json!([{"op": "add", "path": "/list/1", "value": 2}]));
}

#[tokio::test]
async fn websocket_merge_patch_format() {
    let (base, store, _info) = serve().await;
    store
        .put("/w", json!({"a": 1, "b": {"x": 1, "y": 2}, "gone": true}))
        .unwrap();
    let ws_url = format!("{}/doc/w", base.replace("http", "ws"));
    let mut req = ws_url.clone().into_client_request().unwrap();
    req.headers_mut()
        .insert("sec-websocket-protocol", "merge-patch".parse().unwrap());
    let (mut ws, resp) = tokio_tungstenite::connect_async(req).await.unwrap();
    // The handshake echoes the selected subprotocol.
    assert_eq!(
        resp.headers()["sec-websocket-protocol"].to_str().unwrap(),
        "merge-patch"
    );
    ws.next().await; // snapshot

    // One commit touching a nested field, deleting a key, and leaving the
    // rest alone → minimal RFC 7396 merge patch.
    store
        .put("/w", json!({"a": 1, "b": {"x": 9, "y": 2}}))
        .unwrap();
    let msg = ws.next().await.unwrap().unwrap();
    let v: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
    assert_eq!(v["type"], "change");
    assert_eq!(v["merge"], json!({"b": {"x": 9}, "gone": null}));

    // Offering only unknown subprotocols is rejected before upgrade.
    let mut bad = ws_url.into_client_request().unwrap();
    bad.headers_mut()
        .insert("sec-websocket-protocol", "bogus".parse().unwrap());
    assert!(tokio_tungstenite::connect_async(bad).await.is_err());
}

/// Pull the next real SSE event (id + JSON data) off a streaming response,
/// skipping keep-alive comment frames.
/// Next SSE frame as (id, event name, payload). Every data frame is named
/// (`event: snapshot` or `event: change`).
async fn next_sse(resp: &mut reqwest::Response, buf: &mut String) -> (u64, String, Value) {
    loop {
        if let Some(pos) = buf.find("\n\n") {
            let frame = buf[..pos].to_string();
            buf.replace_range(..pos + 2, "");
            let mut id = None;
            let mut name = None;
            let mut data = None;
            for line in frame.lines() {
                if let Some(v) = line.strip_prefix("id:") {
                    id = Some(v.trim().parse::<u64>().unwrap());
                } else if let Some(v) = line.strip_prefix("event:") {
                    name = Some(v.trim().to_string());
                } else if let Some(v) = line.strip_prefix("data:") {
                    data = Some(serde_json::from_str::<Value>(v.trim()).unwrap());
                }
            }
            if let (Some(i), Some(d)) = (id, data) {
                return (i, name.expect("data frame without an event name"), d);
            }
            continue; // comment-only keep-alive frame
        }
        let chunk = resp.chunk().await.unwrap().expect("SSE stream ended");
        buf.push_str(std::str::from_utf8(&chunk).unwrap());
    }
}

#[tokio::test]
async fn sse_snapshot_then_filtered_changes() {
    let (base, store, _info) = serve().await;
    store.put("/w", json!({"n": 0})).unwrap();
    store.put("/ignored", json!(0)).unwrap();

    let client = reqwest::Client::new();
    let mut resp = client
        .get(format!("{base}/doc/w"))
        .header("accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(resp.headers()["content-type"]
        .to_str()
        .unwrap()
        .starts_with("text/event-stream"));

    let mut buf = String::new();
    let (snap_id, kind, snap) = next_sse(&mut resp, &mut buf).await;
    assert_eq!(kind, "snapshot");
    assert_eq!(snap["type"], "snapshot");
    assert_eq!(snap["value"], json!({"n": 0}));
    assert_eq!(snap_id, snap["version"].as_u64().unwrap());

    store.put("/ignored", json!(1)).unwrap(); // must NOT be delivered
    store.put("/w/n", json!(1)).unwrap();
    store.put("/w/n", json!(2)).unwrap();

    let (id1, kind, ev1) = next_sse(&mut resp, &mut buf).await;
    assert_eq!(kind, "change");
    assert_eq!(ev1["patch"], json!([{"op": "replace", "path": "/n", "value": 1}]));
    assert!(id1 > snap_id);
    let (id2, _, ev2) = next_sse(&mut resp, &mut buf).await;
    assert_eq!(ev2["patch"], json!([{"op": "replace", "path": "/n", "value": 2}]));
    assert!(id2 > id1);
}

#[tokio::test]
async fn sse_reconnect_resumes_without_snapshot_when_current() {
    let (base, store, _info) = serve().await;
    store.put("/w", json!({"n": 0})).unwrap();
    let client = reqwest::Client::new();

    // First connect: normal snapshot; remember its id.
    let mut resp = client
        .get(format!("{base}/doc/w"))
        .header("accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    let mut buf = String::new();
    let (seen_id, kind, _) = next_sse(&mut resp, &mut buf).await;
    assert_eq!(kind, "snapshot");
    drop(resp); // connection lost

    // Unrelated churn elsewhere advances the head but not /w.
    store.put("/elsewhere", json!(1)).unwrap();
    store.put("/elsewhere", json!(2)).unwrap();

    // Reconnect with Last-Event-ID: /w is untouched since, so no snapshot
    // — the very first frame is the next live change.
    let mut resp = client
        .get(format!("{base}/doc/w"))
        .header("accept", "text/event-stream")
        .header("last-event-id", seen_id.to_string())
        .send()
        .await
        .unwrap();
    store.put("/w/n", json!(1)).unwrap();
    let mut buf = String::new();
    let (id, kind, ev) = next_sse(&mut resp, &mut buf).await;
    assert_eq!(kind, "change", "resumable reconnect must not resend a snapshot");
    assert_eq!(ev["patch"], json!([{"op": "replace", "path": "/n", "value": 1}]));
    assert!(id > seen_id);
    drop(resp);

    // Reconnect after /w itself changed: falls back to a resync snapshot.
    let mut resp = client
        .get(format!("{base}/doc/w"))
        .header("accept", "text/event-stream")
        .header("last-event-id", seen_id.to_string())
        .send()
        .await
        .unwrap();
    let mut buf = String::new();
    let (_, kind, snap) = next_sse(&mut resp, &mut buf).await;
    assert_eq!(kind, "snapshot");
    assert_eq!(snap["resync"], json!(true));
    assert_eq!(snap["value"], json!({"n": 1}));

    // A fresh connect (no Last-Event-ID) is a plain snapshot, not a resync.
    let mut resp = client
        .get(format!("{base}/doc/w"))
        .header("accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    let mut buf = String::new();
    let (_, kind, snap) = next_sse(&mut resp, &mut buf).await;
    assert_eq!(kind, "snapshot");
    assert_eq!(snap.get("resync"), None);
}

#[tokio::test]
async fn conditional_gets_with_etag_and_if_modified_since() {
    let (base, store, _info) = serve().await;
    store.put("/a", json!({"n": 1})).unwrap();
    store.put("/b", json!({"n": 1})).unwrap();
    let client = reqwest::Client::new();

    // A read carries validators.
    let resp = client.get(format!("{base}/doc/a")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let etag = resp.headers()["etag"].to_str().unwrap().to_string();
    let last_mod = resp.headers()["last-modified"].to_str().unwrap().to_string();
    assert_eq!(resp.headers()["cache-control"], "no-cache");
    assert!(last_mod.ends_with(" GMT"), "got {last_mod}");

    // Re-reading with that ETag → 304, no body.
    let resp = client
        .get(format!("{base}/doc/a"))
        .header("if-none-match", &etag)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 304);
    assert_eq!(resp.headers()["etag"], etag.as_str());
    assert!(resp.text().await.unwrap().is_empty());

    // Weak comparison: W/"x" matches "x".
    let resp = client
        .get(format!("{base}/doc/a"))
        .header("if-none-match", format!("W/{etag}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 304);

    // A write to an unrelated path must NOT invalidate /a.
    store.put("/b/n", json!(2)).unwrap();
    let resp = client
        .get(format!("{base}/doc/a"))
        .header("if-none-match", &etag)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 304, "sibling write invalidated /a");

    // Writing inside /a does invalidate it, with a new ETag.
    store.put("/a/n", json!(2)).unwrap();
    let resp = client
        .get(format!("{base}/doc/a"))
        .header("if-none-match", &etag)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let etag2 = resp.headers()["etag"].to_str().unwrap().to_string();
    assert_ne!(etag2, etag);

    // ?raw=true is a different representation, so a different ETag.
    let resp = client
        .get(format!("{base}/doc/a?raw=true"))
        .send()
        .await
        .unwrap();
    assert_ne!(resp.headers()["etag"].to_str().unwrap(), etag2);
    // ...and the plain ETag must not satisfy a raw request.
    let resp = client
        .get(format!("{base}/doc/a?raw=true"))
        .header("if-none-match", &etag2)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // If-Modified-Since, using the date the server itself reported.
    let resp = client.get(format!("{base}/doc/a")).send().await.unwrap();
    let last_mod = resp.headers()["last-modified"].to_str().unwrap().to_string();
    let resp = client
        .get(format!("{base}/doc/a"))
        .header("if-modified-since", &last_mod)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 304);
    let resp = client
        .get(format!("{base}/doc/a"))
        .header("if-modified-since", "Sun, 06 Nov 1994 08:49:37 GMT")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    // An unparseable date is ignored rather than trusted.
    let resp = client
        .get(format!("{base}/doc/a"))
        .header("if-modified-since", "not a date")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // A missing path is a 404, never a 304.
    let resp = client
        .get(format!("{base}/doc/gone"))
        .header("if-none-match", "*")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn snapshot_distinguishes_absent_from_null() {
    let (base, store, _info) = serve().await;
    let client = reqwest::Client::new();

    // A plain GET of a missing path is still a 404 …
    let resp = client.get(format!("{base}/doc/nothing")).send().await.unwrap();
    assert_eq!(resp.status(), 404);

    // … but a subscription succeeds and says so explicitly.
    let mut resp = client
        .get(format!("{base}/doc/nothing"))
        .header("accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    let mut buf = String::new();
    let (_, kind, snap) = next_sse(&mut resp, &mut buf).await;
    assert_eq!(kind, "snapshot");
    assert_eq!(snap["exists"], json!(false));
    assert_eq!(snap["value"], json!(null));

    // Watch-then-create: the birth of the path arrives as an add.
    store.put("/nothing", json!({"a": 1})).unwrap();
    let (_, kind, ev) = next_sse(&mut resp, &mut buf).await;
    assert_eq!(kind, "change");
    assert_eq!(ev["patch"], json!([{"op": "add", "path": "", "value": {"a": 1}}]));

    // A path explicitly set to JSON null exists.
    store.put("/reallynull", json!(null)).unwrap();
    let mut resp = client
        .get(format!("{base}/doc/reallynull"))
        .header("accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    let mut buf = String::new();
    let (_, _, snap) = next_sse(&mut resp, &mut buf).await;
    assert_eq!(snap["exists"], json!(true));
    assert_eq!(snap["value"], json!(null));
}

#[tokio::test]
async fn sse_merge_patch_via_accept_parameter() {
    let (base, store, _info) = serve().await;
    store.put("/w", json!({"a": 1, "gone": true})).unwrap();
    let client = reqwest::Client::new();

    let mut resp = client
        .get(format!("{base}/doc/w"))
        .header("accept", "text/event-stream;patch=merge-patch")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let mut buf = String::new();
    let (_, kind, snap) = next_sse(&mut resp, &mut buf).await;
    assert_eq!(kind, "snapshot");
    assert_eq!(snap["type"], "snapshot");

    store.put("/w", json!({"a": 2})).unwrap();
    let (_, kind, ev) = next_sse(&mut resp, &mut buf).await;
    assert_eq!(kind, "change");
    assert_eq!(ev["merge"], json!({"a": 2, "gone": null}));

    // Unknown patch parameter → 400 before any stream starts.
    let resp = client
        .get(format!("{base}/doc/w"))
        .header("accept", "text/event-stream;patch=bogus")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn rest_merge_patch() {
    let (base, _store, _info) = serve().await;
    let client = reqwest::Client::new();
    client
        .put(format!("{base}/doc/users"))
        .json(&json!({"alice": {"age": 30, "tags": ["admin"]}, "bob": {"age": 44}}))
        .send()
        .await
        .unwrap();

    // Explicit RFC 7396 content type: merge nested, delete via null.
    let resp = client
        .patch(format!("{base}/doc/users"))
        .header("content-type", "application/merge-patch+json")
        .body(r#"{"alice": {"age": 31}, "bob": null, "carol": {"new": true}}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let info: Value = resp.json().await.unwrap();
    // Changed paths are diffed precisely — alice/tags untouched.
    assert_eq!(
        info["changed_paths"],
        json!(["/users/alice/age", "/users/bob", "/users/carol"])
    );
    let v: Value = client
        .get(format!("{base}/doc/users"))
        .header("accept", "application/json")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        v,
        json!({"alice": {"age": 31, "tags": ["admin"]}, "carol": {"new": true}})
    );

    // Sniffed: plain application/json with an object body is a merge patch.
    let resp = client
        .patch(format!("{base}/doc/users"))
        .json(&json!({"carol": {"new": false}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = client
        .get(format!("{base}/doc/users/carol/new"))
        .header("accept", "application/json")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(v, json!(false));
}
