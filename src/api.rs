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
//!   GET    /ws?path=/a/b            subscribe: snapshot, then ordered changes

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::StoreError;
use crate::model::{get_at, to_plain_value, Pointer};
use crate::store::{Condition, Op, Store};
use crate::subscriptions::CommitEvent;

pub fn router(store: Arc<Store>) -> Router {
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
        .route("/ws", get(ws_handler))
        .with_state(store)
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
) -> ApiResult {
    read(store, String::new(), q, headers).await
}

async fn get_path(
    State(store): State<Arc<Store>>,
    Path(path): Path<String>,
    Query(q): Query<GetQuery>,
    headers: HeaderMap,
) -> ApiResult {
    read(store, pointer_of(&path), q, headers).await
}

async fn read(store: Arc<Store>, ptr: String, q: GetQuery, headers: HeaderMap) -> ApiResult {
    // Browsers get the live UI for any path — even one that doesn't exist
    // yet, so you can navigate somewhere and create it. `?raw=true` stays
    // JSON as an escape hatch for looking at formulas from the address bar.
    if !q.raw && wants_html(&headers) {
        return Ok(Html(UI_HTML).into_response());
    }
    let value = if q.raw {
        store.get_raw(&ptr)?
    } else {
        store.get(&ptr)?
    };
    match value {
        Some(v) => Ok(Json(v).into_response()),
        None => Err(ApiError(StoreError::NotFound(ptr))),
    }
}

// ------------------------------------------------------------------ PUT

async fn put_root(State(store): State<Arc<Store>>, Json(value): Json<Value>) -> ApiResult {
    Ok(Json(store.put("", value)?).into_response())
}

async fn put_path(
    State(store): State<Arc<Store>>,
    Path(path): Path<String>,
    Json(value): Json<Value>,
) -> ApiResult {
    Ok(Json(store.put(&pointer_of(&path), value)?).into_response())
}

// ------------------------------------------------------------------ POST (append)

async fn post_root(State(store): State<Arc<Store>>, Json(value): Json<Value>) -> ApiResult {
    Ok(Json(store.append("", value)?).into_response())
}

async fn post_path(
    State(store): State<Arc<Store>>,
    Path(path): Path<String>,
    Json(value): Json<Value>,
) -> ApiResult {
    Ok(Json(store.append(&pointer_of(&path), value)?).into_response())
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
                value: p.value.ok_or_else(|| {
                    StoreError::Unsupported("`add` requires a value".into())
                })?,
            }),
            "replace" => ops.push(Op::Put {
                path: full,
                value: p.value.ok_or_else(|| {
                    StoreError::Unsupported("`replace` requires a value".into())
                })?,
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

/// Spawn the liveness ticker: writes wall-clock time and uptime to `/info`
/// every `period`. Doubles as a heartbeat for subscribers watching the root.
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
            if let Err(e) = store.put("/info", info) {
                tracing::warn!(error = %e, "info ticker write failed");
            }
        }
    })
}

/// UTC RFC 3339 timestamp without pulling in a date crate
/// (civil-from-days per Howard Hinnant's algorithm).
fn rfc3339(t: SystemTime) -> String {
    let secs = t
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

// ------------------------------------------------------------------ /ws

#[derive(Deserialize, Default)]
struct WsQuery {
    #[serde(default)]
    path: String,
    #[serde(default)]
    format: Option<String>,
}

/// Wire format for change events.
#[derive(Clone, Copy)]
enum WsFormat {
    /// RFC 6902 JSON Patch, paths relative to the subscription root.
    JsonPatch,
    /// RFC 7396 JSON Merge Patch against the subscribed subtree.
    MergePatch,
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(q): Query<WsQuery>,
    State(store): State<Arc<Store>>,
) -> Response {
    let ptr = match Pointer::parse(&q.path) {
        Ok(p) => p,
        Err(e) => return ApiError(e).into_response(),
    };
    let format = match q.format.as_deref() {
        None | Some("json-patch") => WsFormat::JsonPatch,
        Some("merge-patch") => WsFormat::MergePatch,
        Some(other) => {
            return ApiError(StoreError::Unsupported(format!(
                "format `{other}` (use json-patch or merge-patch)"
            )))
            .into_response()
        }
    };
    ws.on_upgrade(move |socket| handle_ws(socket, store, ptr, format))
}

fn value_at(root: &Arc<crate::model::Node>, ptr: &Pointer) -> Value {
    get_at(root, ptr)
        .map(|n| to_plain_value(&n))
        .unwrap_or(Value::Null)
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

async fn handle_ws(mut socket: WebSocket, store: Arc<Store>, ptr: Pointer, format: WsFormat) {
    let (snapshot, mut rx) = store.subscribe();
    let mut last_root = snapshot.root.clone();
    let hello = json!({
        "type": "snapshot",
        "version": snapshot.version,
        "path": ptr.to_string(),
        "value": value_at(&snapshot.root, &ptr),
    });
    if socket.send(Message::Text(hello.to_string())).await.is_err() {
        return;
    }
    loop {
        tokio::select! {
            event = rx.recv() => match event {
                Ok(ev) => {
                    let msg = match format {
                        WsFormat::JsonPatch => json_patch_event(&ptr, &last_root, &ev),
                        WsFormat::MergePatch => merge_patch_event(&ptr, &last_root, &ev),
                    };
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
                    let msg = json!({
                        "type": "snapshot",
                        "version": snap.version,
                        "path": ptr.to_string(),
                        "value": value_at(&snap.root, &ptr),
                        "resync": true,
                    });
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
