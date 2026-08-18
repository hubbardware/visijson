//! HTTP + WebSocket API.
//!
//! REST (document paths are JSON pointers):
//!   GET    /doc[/{path}]?raw=true   read (plain by default, raw shows formulas)
//!   PUT    /doc[/{path}]            set value
//!   POST   /doc[/{path}]            append to array
//!   PATCH  /doc[/{path}]            RFC 6902 subset: add/replace/remove/test
//!   DELETE /doc/{path}              remove
//!   POST   /txn                     {"ops": [...], "conditions": [...]}
//!   GET    /version                 current committed version
//!   GET    /info[/{path}]           server liveness metadata — a read-only
//!                                   sibling namespace backed by its own
//!                                   in-memory store, so its once-a-second
//!                                   churn never touches /doc subscribers
//!                                   or the WAL
//!
//! GET on a document path is fully content-negotiated: a WebSocket upgrade
//! subscribes to that path (snapshot, then ordered changes — event format
//! chosen by `Sec-WebSocket-Protocol: json-patch` (default) or
//! `merge-patch`); `Accept: text/event-stream` subscribes over SSE (same
//! events; format via an Accept parameter, `text/event-stream;patch=
//! merge-patch`); an Accept preferring text/html gets the embedded UI;
//! everything else gets JSON.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::StoreError;
use crate::meta::Stamp;
use crate::model::{from_value, get_at, to_plain_value, Node, PlainView, Pointer};
use crate::store::{Condition, Op, Store};
use crate::subscriptions::CommitEvent;

pub fn router(doc: Arc<Store>, info: Arc<Store>) -> Router {
    Router::new()
        .route(
            "/doc",
            get(get_root)
                .put(put_root)
                .post(post_root)
                .patch(patch_root)
                .delete(delete_root),
        )
        .route(
            "/doc/*path",
            get(get_path)
                .put(put_path)
                .post(post_path)
                .patch(patch_path)
                .delete(delete_path),
        )
        .route("/txn", post(txn_handler))
        .route("/version", get(version_handler))
        .with_state(doc)
        // /info is a sibling namespace over its own store, GET-only (reads,
        // the UI, and WebSocket subscriptions all negotiate off GET; writes
        // 405). Same handlers, different state.
        .merge(
            Router::new()
                .route("/info", get(get_root))
                .route("/info/*path", get(get_path))
                .with_state(info),
        )
}

struct ApiError(StoreError);

impl From<StoreError> for ApiError {
    fn from(e: StoreError) -> Self {
        ApiError(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            StoreError::NotFound(_) => StatusCode::NOT_FOUND,
            StoreError::Conflict(_) | StoreError::RetryLimit(_) => StatusCode::CONFLICT,
            StoreError::FormulaCycle(_) => StatusCode::UNPROCESSABLE_ENTITY,
            StoreError::InvalidPointer(_)
            | StoreError::TypeMismatch { .. }
            | StoreError::FormulaParse { .. }
            | StoreError::Unsupported(_) => StatusCode::BAD_REQUEST,
            StoreError::WalBroken(_) => StatusCode::SERVICE_UNAVAILABLE,
            StoreError::Io(_) | StoreError::Serde(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(json!({ "error": self.0.to_string() }))).into_response()
    }
}

type ApiResult = Result<Response, ApiError>;

fn pointer_of(rest: &str) -> String {
    if rest.is_empty() {
        String::new()
    } else {
        format!("/{rest}")
    }
}

#[derive(Deserialize, Default)]
struct GetQuery {
    #[serde(default)]
    raw: bool,
}

// ------------------------------------------------------------------ GET

/// The embedded live UI, compiled into the binary.
const UI_HTML: &str = include_str!("ui.html");

/// Browsers announce `text/html` ahead of anything else in Accept; API
/// clients (curl's `*/*`, fetch with `application/json`) do not.
fn wants_html(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|accept| match (accept.find("text/html"), accept.find("application/json")) {
            (Some(h), Some(j)) => h < j,
            (Some(_), None) => true,
            _ => false,
        })
        .unwrap_or(false)
}

async fn get_root(
    State(store): State<Arc<Store>>,
    Query(q): Query<GetQuery>,
    headers: HeaderMap,
    ws: Option<WebSocketUpgrade>,
) -> ApiResult {
    match ws {
        Some(ws) => subscribe(ws, store, String::new(), &headers),
        None => read(store, String::new(), q, headers).await,
    }
}

async fn get_path(
    State(store): State<Arc<Store>>,
    Path(path): Path<String>,
    Query(q): Query<GetQuery>,
    headers: HeaderMap,
    ws: Option<WebSocketUpgrade>,
) -> ApiResult {
    match ws {
        Some(ws) => subscribe(ws, store, pointer_of(&path), &headers),
        None => read(store, pointer_of(&path), q, headers).await,
    }
}

async fn read(store: Arc<Store>, ptr: String, q: GetQuery, headers: HeaderMap) -> ApiResult {
    // `Accept: text/event-stream` subscribes over SSE instead of reading.
    if let Some(format) = sse_request(&headers) {
        return subscribe_sse(store, ptr, format?);
    }
    // Browsers get the live UI for any path — even one that doesn't exist
    // yet, so you can navigate somewhere and create it. `?raw=true` stays
    // JSON as an escape hatch for looking at formulas from the address bar.
    if !q.raw && wants_html(&headers) {
        return Ok(Html(UI_HTML).into_response());
    }
    let Some(node) = store.get_node(&ptr)? else {
        return Err(ApiError(StoreError::NotFound(ptr)));
    };
    // Cache validators: the subtree's last-modified commit. `no-cache`
    // means "reuse only after revalidating" — right for live data.
    let stamp = store.last_modified(&ptr)?;
    let etag = etag_for(stamp, q.raw);
    let validators = [
        (header::ETAG, etag.clone()),
        (header::LAST_MODIFIED, http_date(stamp.time_ms)),
        (header::CACHE_CONTROL, "no-cache".to_string()),
    ];
    if is_not_modified(&headers, &etag, stamp.time_ms) {
        return Ok((StatusCode::NOT_MODIFIED, validators).into_response());
    }
    // Serialize straight from the tree — no intermediate Value.
    if q.raw {
        Ok((validators, Json(node)).into_response())
    } else {
        Ok((validators, Json(PlainView(node))).into_response())
    }
}

// ------------------------------------------------------------------ PUT

// Bodies deserialize straight into `Node` trees (no intermediate Value).

async fn put_root(State(store): State<Arc<Store>>, Json(node): Json<Node>) -> ApiResult {
    Ok(Json(store.put_node("", Arc::new(node))?).into_response())
}

async fn put_path(
    State(store): State<Arc<Store>>,
    Path(path): Path<String>,
    Json(node): Json<Node>,
) -> ApiResult {
    Ok(Json(store.put_node(&pointer_of(&path), Arc::new(node))?).into_response())
}

// ------------------------------------------------------------------ POST (append)

async fn post_root(State(store): State<Arc<Store>>, Json(node): Json<Node>) -> ApiResult {
    Ok(Json(store.append_node("", Arc::new(node))?).into_response())
}

async fn post_path(
    State(store): State<Arc<Store>>,
    Path(path): Path<String>,
    Json(node): Json<Node>,
) -> ApiResult {
    Ok(Json(store.append_node(&pointer_of(&path), Arc::new(node))?).into_response())
}

// ------------------------------------------------------------------ DELETE

async fn delete_root(State(store): State<Arc<Store>>) -> ApiResult {
    Ok(Json(store.remove("")?).into_response())
}

async fn delete_path(State(store): State<Arc<Store>>, Path(path): Path<String>) -> ApiResult {
    Ok(Json(store.remove(&pointer_of(&path))?).into_response())
}

// ------------------------------------------------------------------ PATCH

#[derive(Deserialize)]
struct PatchOp {
    op: String,
    path: String,
    #[serde(default)]
    value: Option<Value>,
}

fn patch_to_ops(prefix: &str, patch: Vec<PatchOp>) -> Result<(Vec<Op>, Vec<Condition>), StoreError> {
    let mut ops = Vec::new();
    let mut conditions = Vec::new();
    for p in patch {
        let full = format!("{prefix}{}", p.path);
        match p.op.as_str() {
            "add" => ops.push(Op::Insert {
                path: full,
                value: from_value(&p.value.ok_or_else(|| {
                    StoreError::Unsupported("`add` requires a value".into())
                })?),
            }),
            "replace" => ops.push(Op::Put {
                path: full,
                value: from_value(&p.value.ok_or_else(|| {
                    StoreError::Unsupported("`replace` requires a value".into())
                })?),
            }),
            "remove" => ops.push(Op::Remove { path: full }),
            "test" => conditions.push(Condition {
                path: full,
                expected: p.value,
            }),
            other => {
                return Err(StoreError::Unsupported(format!(
                    "patch op `{other}` not supported (use add/replace/remove/test)"
                )))
            }
        }
    }
    Ok((ops, conditions))
}

async fn patch_root(
    State(store): State<Arc<Store>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> ApiResult {
    apply_patch(store, String::new(), headers, body).await
}

async fn patch_path(
    State(store): State<Arc<Store>>,
    Path(path): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> ApiResult {
    apply_patch(store, pointer_of(&path), headers, body).await
}

/// PATCH speaks both standards. `application/json-patch+json` (RFC 6902) and
/// `application/merge-patch+json` (RFC 7396) select explicitly; a plain JSON
/// content type is sniffed — an array must be a JSON Patch, anything else is
/// a merge patch.
async fn apply_patch(
    store: Arc<Store>,
    prefix: String,
    headers: HeaderMap,
    body: Value,
) -> ApiResult {
    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let is_json_patch = ct.starts_with("application/json-patch+json")
        || (!ct.starts_with("application/merge-patch+json") && body.is_array());
    if is_json_patch {
        let patch: Vec<PatchOp> = serde_json::from_value(body).map_err(StoreError::Serde)?;
        let (ops, conditions) = patch_to_ops(&prefix, patch)?;
        Ok(Json(store.commit(ops, &conditions)?).into_response())
    } else {
        Ok(Json(store.merge(&prefix, body)?).into_response())
    }
}

// ------------------------------------------------------------------ /txn

#[derive(Deserialize)]
struct TxnRequest {
    #[serde(default)]
    ops: Vec<Op>,
    #[serde(default)]
    conditions: Vec<Condition>,
}

async fn txn_handler(State(store): State<Arc<Store>>, Json(req): Json<TxnRequest>) -> ApiResult {
    Ok(Json(store.commit(req.ops, &req.conditions)?).into_response())
}

// ------------------------------------------------------------------ /version

async fn version_handler(State(store): State<Arc<Store>>) -> ApiResult {
    Ok(Json(json!({ "version": store.version() })).into_response())
}

// ------------------------------------------------------------------ /info ticker

/// Spawn the liveness ticker: rewrites the root of the (dedicated, in-memory)
/// info store with wall-clock time and uptime every `period`. Subscribers of
/// `/info` get a heartbeat; the document store never sees the churn.
pub fn spawn_info_ticker(store: Arc<Store>, period: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let started = SystemTime::now();
        let started_iso = rfc3339(started);
        let mut interval = tokio::time::interval(period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            let now = SystemTime::now();
            let epoch_ms = now
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let uptime = now.duration_since(started).unwrap_or_default().as_secs();
            let info = json!({
                "time": rfc3339(now),
                "epoch_ms": epoch_ms,
                "uptime_secs": uptime,
                "started": started_iso,
            });
            if let Err(e) = store.put("", info) {
                tracing::warn!(error = %e, "info ticker write failed");
            }
        }
    })
}

// ------------------------------------------------------------------ dates
//
// Civil-date math per Howard Hinnant's algorithms, so no date crate.

/// (year, month, day) for a count of days since the epoch.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Inverse of `civil_from_days`.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// UTC RFC 3339 timestamp.
fn rfc3339(t: SystemTime) -> String {
    let secs = t
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

const DOW: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// An HTTP-date (RFC 9110 IMF-fixdate): `Sun, 06 Nov 1994 08:49:37 GMT`.
fn http_date(time_ms: u64) -> String {
    let secs = (time_ms / 1000) as i64;
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (y, m, d) = civil_from_days(days);
    // 1970-01-01 was a Thursday.
    let dow = DOW[(days + 4).rem_euclid(7) as usize];
    let mon = MONTHS[(m - 1) as usize];
    format!("{dow}, {d:02} {mon} {y:04} {h:02}:{mi:02}:{s:02} GMT")
}

/// Parse an IMF-fixdate into epoch seconds. Obsolete formats (RFC 850,
/// asctime) return None, which simply skips the conditional check and
/// serves a full response — safe, never a false 304.
fn parse_http_date(s: &str) -> Option<i64> {
    let f: Vec<&str> = s.split_whitespace().collect();
    if f.len() != 6 || f[5] != "GMT" {
        return None;
    }
    let d: i64 = f[1].parse().ok()?;
    let m = MONTHS.iter().position(|m| *m == f[2])? as i64 + 1;
    let y: i64 = f[3].parse().ok()?;
    let mut hms = f[4].split(':');
    let h: i64 = hms.next()?.parse().ok()?;
    let mi: i64 = hms.next()?.parse().ok()?;
    let sec: i64 = hms.next()?.parse().ok()?;
    if hms.next().is_some() || !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    Some(days_from_civil(y, m, d) * 86_400 + h * 3600 + mi * 60 + sec)
}

// ------------------------------------------------------- HTTP validators

/// The strong validator for a read: the version of the last commit that
/// touched this subtree, qualified by the representation (`?raw=true`
/// renders formulas differently from the same version's plain view).
fn etag_for(stamp: Stamp, raw: bool) -> String {
    if raw {
        format!("\"{}-raw\"", stamp.version)
    } else {
        format!("\"{}\"", stamp.version)
    }
}

/// Does a conditional request let us answer 304? Per RFC 9110,
/// `If-None-Match` wins outright when present; `If-Modified-Since` is only
/// consulted in its absence.
fn is_not_modified(headers: &HeaderMap, etag: &str, time_ms: u64) -> bool {
    if let Some(inm) = headers.get(header::IF_NONE_MATCH).and_then(|v| v.to_str().ok()) {
        let inm = inm.trim();
        if inm == "*" {
            return true;
        }
        // Weak comparison: `W/"x"` and `"x"` are a match.
        return inm
            .split(',')
            .map(|t| t.trim().strip_prefix("W/").unwrap_or(t.trim()))
            .any(|t| t == etag);
    }
    if let Some(ims) = headers
        .get(header::IF_MODIFIED_SINCE)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_http_date)
    {
        // Second granularity, per the header's own resolution.
        return (time_ms / 1000) as i64 <= ims;
    }
    false
}

// ------------------------------------------------------------------ subscriptions

/// Wire format for change events (WebSocket and SSE alike).
#[derive(Clone, Copy)]
enum PatchFormat {
    /// RFC 6902 JSON Patch, paths relative to the subscription root.
    JsonPatch,
    /// RFC 7396 JSON Merge Patch against the subscribed subtree.
    MergePatch,
}

/// Subprotocols a subscriber may offer via `Sec-WebSocket-Protocol`.
const WS_PROTOCOLS: [&str; 2] = ["json-patch", "merge-patch"];

/// A WebSocket upgrade on a document path subscribes to that path. The event
/// format is the first recognized subprotocol the client offered (echoed back
/// in the handshake); offering none means json-patch, offering only unknown
/// ones is a 400.
fn subscribe(ws: WebSocketUpgrade, store: Arc<Store>, path: String, headers: &HeaderMap) -> ApiResult {
    let ptr = Pointer::parse(&path)?;
    let offered: Vec<&str> = headers
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.split(',').map(str::trim).filter(|s| !s.is_empty()).collect())
        .unwrap_or_default();
    let format = match offered.iter().find(|p| WS_PROTOCOLS.contains(p)) {
        Some(&"merge-patch") => PatchFormat::MergePatch,
        Some(_) => PatchFormat::JsonPatch,
        None if offered.is_empty() => PatchFormat::JsonPatch,
        None => {
            return Err(ApiError(StoreError::Unsupported(format!(
                "subprotocol {:?} (offer json-patch or merge-patch)",
                offered.join(", ")
            ))))
        }
    };
    Ok(ws
        .protocols(WS_PROTOCOLS)
        .on_upgrade(move |socket| handle_ws(socket, store, ptr, format)))
}

/// The snapshot message opening (or resyncing) any subscription transport.
/// `exists` distinguishes an absent path (`value` is null as a placeholder)
/// from a path explicitly set to JSON null — subscribing to a path that
/// doesn't exist yet is supported, and its creation arrives as a change.
fn snapshot_msg(version: u64, root: &Arc<Node>, ptr: &Pointer, resync: bool) -> Value {
    let node = get_at(root, ptr);
    let mut msg = json!({
        "type": "snapshot",
        "version": version,
        "path": ptr.to_string(),
        "exists": node.is_some(),
        "value": node.map(|n| to_plain_value(&n)).unwrap_or(Value::Null),
    });
    if resync {
        msg["resync"] = json!(true);
    }
    msg
}

/// Encode one commit as a change message in the subscriber's chosen format.
/// None = nothing relevant to this subscription.
fn change_msg(
    format: PatchFormat,
    sub: &Pointer,
    old_root: &Arc<Node>,
    ev: &CommitEvent,
) -> Option<Value> {
    match format {
        PatchFormat::JsonPatch => json_patch_event(sub, old_root, ev),
        PatchFormat::MergePatch => merge_patch_event(sub, old_root, ev),
    }
}

/// Encode a change event as an RFC 6902 JSON Patch relative to `sub`.
/// `old_root` is the tree as of the previous event (or snapshot), letting us
/// distinguish add/replace/remove and skip no-ops. None = nothing relevant.
fn json_patch_event(sub: &Pointer, old_root: &Arc<crate::model::Node>, ev: &CommitEvent) -> Option<Value> {
    let mut targets = std::collections::BTreeSet::new();
    for c in ev.changed_paths.iter() {
        let Ok(c) = Pointer::parse(c) else { continue };
        if !sub.overlaps(&c) {
            continue;
        }
        // Clamp to the subscription root when an ancestor changed.
        targets.insert(if sub.is_prefix_of(&c) { c } else { sub.clone() });
    }
    let mut patch = Vec::new();
    for t in &targets {
        let rel = t.relative_to(sub).expect("target is always under sub");
        let (old, new) = (get_at(old_root, t), get_at(&ev.committed.root, t));
        match (old, new) {
            (None, Some(n)) => patch.push(json!({
                "op": "add", "path": rel.to_string(), "value": to_plain_value(&n),
            })),
            (Some(o), Some(n)) => {
                if !crate::model::nodes_equal(&o, &n) {
                    patch.push(json!({
                        "op": "replace", "path": rel.to_string(), "value": to_plain_value(&n),
                    }));
                }
            }
            (Some(_), None) => patch.push(json!({"op": "remove", "path": rel.to_string()})),
            (None, None) => {}
        }
    }
    if patch.is_empty() {
        return None;
    }
    Some(json!({"type": "change", "version": ev.version, "patch": patch}))
}

/// Encode a change event as an RFC 7396 JSON Merge Patch for the subtree.
fn merge_patch_event(sub: &Pointer, old_root: &Arc<crate::model::Node>, ev: &CommitEvent) -> Option<Value> {
    let relevant = ev.changed_paths.iter().any(|c| {
        Pointer::parse(c).map(|c| sub.overlaps(&c)).unwrap_or(false)
    });
    if !relevant {
        return None;
    }
    let old = get_at(old_root, sub);
    let new = get_at(&ev.committed.root, sub);
    let merge = crate::model::merge_patch_diff(old.as_ref(), new.as_ref())?;
    Some(json!({"type": "change", "version": ev.version, "merge": merge}))
}

async fn handle_ws(mut socket: WebSocket, store: Arc<Store>, ptr: Pointer, format: PatchFormat) {
    let (snapshot, mut rx) = store.subscribe();
    let mut last_root = snapshot.root.clone();
    let hello = snapshot_msg(snapshot.version, &snapshot.root, &ptr, false);
    if socket.send(Message::Text(hello.to_string())).await.is_err() {
        return;
    }
    loop {
        tokio::select! {
            event = rx.recv() => match event {
                Ok(ev) => {
                    let msg = change_msg(format, &ptr, &last_root, &ev);
                    last_root = ev.committed.root.clone();
                    let Some(msg) = msg else { continue };
                    if socket.send(Message::Text(msg.to_string())).await.is_err() {
                        return;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    // Fell behind the broadcast buffer: resync with a fresh
                    // snapshot rather than deliver a gapped stream.
                    tracing::warn!(missed = n, "subscriber lagged; resyncing");
                    let (snap, new_rx) = store.subscribe();
                    rx = new_rx;
                    last_root = snap.root.clone();
                    let msg = snapshot_msg(snap.version, &snap.root, &ptr, true);
                    if socket.send(Message::Text(msg.to_string())).await.is_err() {
                        return;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            },
            msg = socket.recv() => match msg {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return,
                Some(Ok(_)) => {} // ignore pings/client chatter
            },
        }
    }
}

// ------------------------------------------------------------------ SSE

/// Did the Accept header ask for `text/event-stream`, and if so in which
/// patch format? The format rides an Accept media-type parameter —
/// `text/event-stream;patch=merge-patch` — because `EventSource` can't set
/// custom headers (it always sends exactly `Accept: text/event-stream`,
/// which gets the json-patch default). Returns None when SSE wasn't asked
/// for, Some(Err) for an unknown format.
fn sse_request(headers: &HeaderMap) -> Option<Result<PatchFormat, StoreError>> {
    let accept = headers.get(header::ACCEPT)?.to_str().ok()?;
    for range in accept.split(',') {
        let mut parts = range.trim().split(';');
        let media_type = parts.next().unwrap_or("").trim();
        if !media_type.eq_ignore_ascii_case("text/event-stream") {
            continue;
        }
        for param in parts {
            if let Some(v) = param.trim().strip_prefix("patch=") {
                return Some(match v.trim().trim_matches('"') {
                    "json-patch" => Ok(PatchFormat::JsonPatch),
                    "merge-patch" => Ok(PatchFormat::MergePatch),
                    other => Err(StoreError::Unsupported(format!(
                        "patch format `{other}` (use json-patch or merge-patch)"
                    ))),
                });
            }
        }
        return Some(Ok(PatchFormat::JsonPatch));
    }
    None
}

/// Subscribe over Server-Sent Events: one unnamed `data:` frame per message,
/// identical payloads to the WebSocket transport, with the commit version as
/// the SSE event id. Lag resyncs with a fresh snapshot, exactly like the
/// WebSocket path.
fn subscribe_sse(store: Arc<Store>, path: String, format: PatchFormat) -> ApiResult {
    let ptr = Pointer::parse(&path)?;
    let (snapshot, rx) = store.subscribe();
    let first = Event::default()
        .id(snapshot.version.to_string())
        .data(snapshot_msg(snapshot.version, &snapshot.root, &ptr, false).to_string());

    struct Ctx {
        store: Arc<Store>,
        ptr: Pointer,
        format: PatchFormat,
        rx: tokio::sync::broadcast::Receiver<Arc<CommitEvent>>,
        last_root: Arc<Node>,
    }
    let ctx = Ctx {
        store,
        ptr,
        format,
        rx,
        last_root: snapshot.root.clone(),
    };
    let stream = futures_util::stream::unfold((ctx, Some(first)), |(mut ctx, queued)| async move {
        if let Some(ev) = queued {
            return Some((Ok::<_, std::convert::Infallible>(ev), (ctx, None)));
        }
        loop {
            match ctx.rx.recv().await {
                Ok(ev) => {
                    let msg = change_msg(ctx.format, &ctx.ptr, &ctx.last_root, &ev);
                    ctx.last_root = ev.committed.root.clone();
                    if let Some(msg) = msg {
                        let e = Event::default().id(ev.version.to_string()).data(msg.to_string());
                        return Some((Ok(e), (ctx, None)));
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(missed = n, "SSE subscriber lagged; resyncing");
                    let (snap, rx) = ctx.store.subscribe();
                    ctx.rx = rx;
                    ctx.last_root = snap.root.clone();
                    let e = Event::default()
                        .id(snap.version.to_string())
                        .data(snapshot_msg(snap.version, &snap.root, &ctx.ptr, true).to_string());
                    return Some((Ok(e), (ctx, None)));
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
            }
        }
    });
    Ok(Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response())
}
