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

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::StoreError;
use crate::model::{get_at, to_plain_value, Pointer};
use crate::store::{Condition, Op, Store};
use crate::subscriptions::is_relevant;

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

async fn get_root(State(store): State<Arc<Store>>, Query(q): Query<GetQuery>) -> ApiResult {
    read(store, String::new(), q).await
}

async fn get_path(
    State(store): State<Arc<Store>>,
    Path(path): Path<String>,
    Query(q): Query<GetQuery>,
) -> ApiResult {
    read(store, pointer_of(&path), q).await
}

async fn read(store: Arc<Store>, ptr: String, q: GetQuery) -> ApiResult {
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

async fn patch_root(State(store): State<Arc<Store>>, Json(patch): Json<Vec<PatchOp>>) -> ApiResult {
    apply_patch(store, String::new(), patch).await
}

async fn patch_path(
    State(store): State<Arc<Store>>,
    Path(path): Path<String>,
    Json(patch): Json<Vec<PatchOp>>,
) -> ApiResult {
    apply_patch(store, pointer_of(&path), patch).await
}

async fn apply_patch(store: Arc<Store>, prefix: String, patch: Vec<PatchOp>) -> ApiResult {
    let (ops, conditions) = patch_to_ops(&prefix, patch)?;
    Ok(Json(store.commit(ops, &conditions)?).into_response())
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

// ------------------------------------------------------------------ /ws

#[derive(Deserialize, Default)]
struct WsQuery {
    #[serde(default)]
    path: String,
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
    ws.on_upgrade(move |socket| handle_ws(socket, store, ptr))
}

fn value_at(root: &Arc<crate::model::Node>, ptr: &Pointer) -> Value {
    get_at(root, ptr)
        .map(|n| to_plain_value(&n))
        .unwrap_or(Value::Null)
}

async fn handle_ws(mut socket: WebSocket, store: Arc<Store>, ptr: Pointer) {
    let (snapshot, mut rx) = store.subscribe();
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
                    let relevant: Vec<&String> = ev
                        .changed_paths
                        .iter()
                        .filter(|c| is_relevant(&ptr, c))
                        .collect();
                    if relevant.is_empty() {
                        continue;
                    }
                    let msg = json!({
                        "type": "change",
                        "version": ev.version,
                        "changed_paths": relevant,
                        "value": value_at(&ev.committed.root, &ptr),
                    });
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
