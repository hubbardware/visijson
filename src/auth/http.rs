//! HTTP surface of auth: the middleware that turns whatever credential a
//! request carries into a `Caller`, and the `/auth/...` endpoints.
//!
//! Credential precedence: `Authorization: Bearer` (session `vjs_…` or API
//! key `vjk_…`; invalid = 401), then the `vj_session` cookie (invalid =
//! anonymous, so a stale cookie never blocks logging in again), then a
//! single-use `?ticket=` on GET (for `EventSource`, which can't send
//! headers), else anonymous.
//!
//!   POST   /auth/login                 {username, password} → session (cookie + token)
//!   POST   /auth/logout
//!   GET    /auth/whoami                identity + the grants that apply
//!   POST   /auth/ticket                single-use stream ticket (30s)
//!   PUT    /auth/password              {current, new} — own password
//!   GET    /auth/keys                  own credentials (no secrets)
//!   POST   /auth/keys                  {name, scope?, expires_in_secs?} → key, shown once
//!   DELETE /auth/keys/{cid}
//!   GET    /auth/principals            ┐
//!   POST   /auth/principals            │ admin on `/`
//!   GET|PATCH|DELETE /auth/principals/{pid}
//!   PUT    /auth/principals/{pid}/password
//!   GET|POST /auth/principals/{pid}/keys
//!   DELETE /auth/principals/{pid}/credentials/{cid} ┘
//!   GET    /auth/grants                grants on paths you administer
//!   POST   /auth/grants                {subject, path, perms} — admin on path
//!   DELETE /auth/grants/{gid}          admin on the grant's path

use std::sync::Arc;

use axum::extract::{Path, Request, State};
use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Extension, Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use super::{Auth, KeyRequest, NewKey, Perm, PermSet, Source};
use crate::api::error_response;
use crate::auth::acl::{parse_scope, Caller};
use crate::error::StoreError;
use crate::model::Pointer;

pub const SESSION_COOKIE: &str = "vj_session";
/// Cookie-authenticated unsafe requests must carry this header. Cross-site
/// pages can't set custom headers without a CORS preflight, which this
/// server never grants — so it is a complete CSRF defense.
pub const CSRF_HEADER: &str = "x-visijson-request";

type AuthState = Option<Arc<Auth>>;

/// Handler error: renders like every other API error.
struct HttpError(StoreError);

impl From<StoreError> for HttpError {
    fn from(e: StoreError) -> Self {
        HttpError(e)
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        error_response(self.0)
    }
}

type R = Result<Response, HttpError>;

fn fail(e: StoreError) -> Response {
    error_response(e)
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, StoreError> + Send + 'static,
) -> Result<T, StoreError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| StoreError::Io(std::io::Error::other(format!("auth task: {e}"))))?
}

fn cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|kv| kv.trim().split_once('='))
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v.to_string())
}

fn query_param(query: Option<&str>, name: &str) -> Option<String> {
    query?.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == name).then(|| v.to_string())
    })
}

async fn identify(
    auth: &Arc<Auth>,
    headers: &HeaderMap,
    ticket: Option<String>,
) -> Result<Caller, StoreError> {
    if let Some(authz) = headers.get(header::AUTHORIZATION) {
        let token = authz
            .to_str()
            .ok()
            .and_then(|v| v.strip_prefix("Bearer "))
            .ok_or_else(|| StoreError::Unauthorized("use `Authorization: Bearer <token>`".into()))?;
        return auth.authenticate(token.trim(), false);
    }
    if let Some(token) = cookie(headers, SESSION_COOKIE) {
        if let Ok(caller) = auth.authenticate(&token, true) {
            return Ok(caller);
        }
    }
    if let Some(ticket) = ticket {
        let auth = auth.clone();
        return tokio::task::spawn_blocking(move || auth.redeem_ticket(&ticket))
            .await
            .map_err(|e| StoreError::Io(std::io::Error::other(e.to_string())))?;
    }
    Ok(auth.anonymous())
}

/// Cookie credentials ride along on cross-site requests, so they get two
/// extra rules: unsafe methods need the CSRF header, and WebSocket upgrades
/// (which carry no CORS protection at all) need a same-origin `Origin`.
fn cookie_rules(caller: &Caller, req: &Request) -> Result<(), StoreError> {
    if !caller.via_cookie() {
        return Ok(());
    }
    let headers = req.headers();
    let safe = matches!(*req.method(), Method::GET | Method::HEAD | Method::OPTIONS);
    if !safe && !headers.contains_key(CSRF_HEADER) {
        return Err(StoreError::Forbidden(format!(
            "cookie-authenticated writes must send the `{CSRF_HEADER}` header"
        )));
    }
    let upgrade = headers
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
    if upgrade {
        if let Some(origin) = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) {
            let host = headers.get(header::HOST).and_then(|v| v.to_str().ok()).unwrap_or("");
            let origin_host = origin.split_once("://").map(|(_, h)| h).unwrap_or(origin);
            if origin_host != host {
                return Err(StoreError::Forbidden("cross-origin WebSocket upgrade".into()));
            }
        }
    }
    Ok(())
}

/// The middleware: resolve the caller and put it in request extensions.
/// With auth disabled every request is `Caller::unrestricted()`.
pub async fn authenticate(State(auth): State<AuthState>, mut req: Request, next: Next) -> Response {
    let caller = match &auth {
        None => Caller::unrestricted(),
        Some(auth) => {
            // Tickets are for streams only: GET, never a write.
            let ticket = (req.method() == Method::GET)
                .then(|| query_param(req.uri().query(), "ticket"))
                .flatten();
            let headers = req.headers().clone();
            match identify(auth, &headers, ticket).await {
                Ok(c) => c,
                Err(e) => return fail(e),
            }
        }
    };
    if let Err(e) = cookie_rules(&caller, &req) {
        return fail(e);
    }
    req.extensions_mut().insert(caller);
    next.run(req).await
}

pub fn routes(auth: AuthState) -> Router {
    Router::new()
        .route("/auth/login", post(login))
        .route("/auth/logout", post(logout))
        .route("/auth/whoami", get(whoami))
        .route("/auth/ticket", post(ticket))
        .route("/auth/password", put(change_password))
        .route("/auth/keys", get(own_keys).post(mint_own_key))
        .route("/auth/keys/:cid", delete(delete_own_key))
        .route("/auth/principals", get(list_principals).post(create_principal))
        .route(
            "/auth/principals/:pid",
            get(get_principal).patch(patch_principal).delete(delete_principal),
        )
        .route("/auth/principals/:pid/password", put(admin_set_password))
        .route("/auth/principals/:pid/keys", get(admin_keys).post(admin_mint_key))
        .route("/auth/principals/:pid/credentials/:cid", delete(admin_delete_credential))
        .route("/auth/grants", get(list_grants).post(create_grant))
        .route("/auth/grants/:gid", delete(delete_grant))
        .with_state(auth)
}

fn enabled(auth: AuthState) -> Result<Arc<Auth>, StoreError> {
    auth.ok_or_else(|| StoreError::NotFound("auth is disabled on this server".into()))
}

fn signed_in(caller: &Caller) -> Result<String, StoreError> {
    caller
        .principal()
        .map(String::from)
        .ok_or_else(|| caller.deny("sign in first".into()))
}

fn require_root_admin(caller: &Caller) -> Result<(), StoreError> {
    caller.require(Perm::Admin, &Pointer::root())
}

fn session_cookie(auth: &Auth, token: &str, max_age_secs: u64) -> String {
    format!(
        "{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={max_age_secs}{}",
        if auth.cfg.cookie_secure { "; Secure" } else { "" }
    )
}

// ------------------------------------------------------------ sessions

#[derive(Deserialize)]
struct LoginBody {
    username: String,
    password: String,
}

async fn login(State(auth): State<AuthState>, Json(body): Json<LoginBody>) -> R {
    let auth = enabled(auth)?;
    let a = auth.clone();
    let (token, caller) = blocking(move || a.login(&body.username, &body.password)).await?;
    let max_age = auth.cfg.session_ttl_ms / 1000;
    Ok((
        [(header::SET_COOKIE, session_cookie(&auth, &token, max_age))],
        Json(json!({
            "token": token,
            "principal": caller.principal(),
            "expires_ms": caller.expires_ms(),
        })),
    )
        .into_response())
}

async fn logout(State(auth): State<AuthState>, Extension(caller): Extension<Caller>) -> R {
    let auth = enabled(auth)?;
    let a = auth.clone();
    blocking(move || a.logout(&caller)).await?;
    Ok((
        [(header::SET_COOKIE, session_cookie(&auth, "", 0))],
        Json(json!({"ok": true})),
    )
        .into_response())
}

async fn whoami(State(auth): State<AuthState>, Extension(caller): Extension<Caller>) -> R {
    if auth.is_none() {
        return Ok(Json(json!({"auth_enabled": false})).into_response());
    }
    let inner = caller.inner();
    let via = match caller.source() {
        Source::Unrestricted => "unrestricted",
        Source::Anonymous => "anonymous",
        Source::Session { .. } => "session",
        Source::ApiKey { .. } => "api_key",
    };
    let grants: Vec<Value> = caller
        .grants()
        .into_iter()
        .map(|(id, g)| json!({"id": id, "subject": g.subject, "path": g.path, "perms": g.perms}))
        .collect();
    let scope = caller.scope().map(|s| {
        s.iter()
            .map(|(p, ps)| json!({"path": p.to_string(), "perms": ps.names()}))
            .collect::<Vec<_>>()
    });
    Ok(Json(json!({
        "auth_enabled": true,
        "anonymous": caller.is_anonymous(),
        "principal": caller.principal(),
        "display": inner.display,
        "groups": inner.groups,
        "via": via,
        "expires_ms": caller.expires_ms(),
        "scope": scope,
        "grants": grants,
    }))
    .into_response())
}

async fn ticket(State(auth): State<AuthState>, Extension(caller): Extension<Caller>) -> R {
    let auth = enabled(auth)?;
    let (ticket, expires_ms) = blocking(move || auth.mint_ticket(&caller)).await?;
    Ok(Json(json!({"ticket": ticket, "expires_ms": expires_ms})).into_response())
}

#[derive(Deserialize)]
struct PasswordChange {
    current: String,
    new: String,
}

async fn change_password(
    State(auth): State<AuthState>,
    Extension(caller): Extension<Caller>,
    Json(body): Json<PasswordChange>,
) -> R {
    let auth = enabled(auth)?;
    let pid = signed_in(&caller)?;
    blocking(move || auth.change_password(&pid, &body.current, &body.new)).await?;
    Ok(Json(json!({"ok": true})).into_response())
}

// ------------------------------------------------------------ own keys

async fn own_keys(State(auth): State<AuthState>, Extension(caller): Extension<Caller>) -> R {
    let auth = enabled(auth)?;
    let pid = signed_in(&caller)?;
    Ok(Json(json!({"credentials": auth.credentials(&pid)?})).into_response())
}

/// A scoped caller may only mint keys at most as wide as its own scope —
/// otherwise a narrow key could mint itself a wide one.
fn check_scope_within(caller: &Caller, req: &KeyRequest) -> Result<(), StoreError> {
    let Some(have) = caller.scope() else {
        return Ok(());
    };
    let Some(want) = &req.scope else {
        return Err(StoreError::Forbidden("a scoped key can only mint scoped keys".into()));
    };
    for (p, ps) in parse_scope(want)?.iter() {
        let covered = have
            .iter()
            .any(|(hp, hps)| hp.is_prefix_of(p) && ps.within(*hps));
        if !covered {
            return Err(StoreError::Forbidden(format!("scope at {p} exceeds your own")));
        }
    }
    Ok(())
}

async fn mint_own_key(
    State(auth): State<AuthState>,
    Extension(caller): Extension<Caller>,
    Json(req): Json<KeyRequest>,
) -> R {
    let auth = enabled(auth)?;
    let pid = signed_in(&caller)?;
    check_scope_within(&caller, &req)?;
    let key: NewKey = blocking(move || auth.create_api_key(&pid, req)).await?;
    Ok((StatusCode::CREATED, Json(key)).into_response())
}

async fn delete_own_key(
    State(auth): State<AuthState>,
    Extension(caller): Extension<Caller>,
    Path(cid): Path<String>,
) -> R {
    let auth = enabled(auth)?;
    let pid = signed_in(&caller)?;
    blocking(move || auth.delete_credential(&pid, &cid)).await?;
    Ok(Json(json!({"ok": true})).into_response())
}

// ------------------------------------------------------------ principals (admin)

#[derive(Deserialize)]
struct NewPrincipal {
    display: String,
    #[serde(default = "default_kind")]
    kind: String,
    #[serde(default)]
    groups: Vec<String>,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
}

fn default_kind() -> String {
    "human".into()
}

async fn list_principals(State(auth): State<AuthState>, Extension(caller): Extension<Caller>) -> R {
    let auth = enabled(auth)?;
    require_root_admin(&caller)?;
    let list: Vec<Value> = auth
        .principals()
        ?
        .into_iter()
        .map(|(id, p)| json!({"id": id, "kind": p.kind, "display": p.display, "status": p.status, "groups": p.groups}))
        .collect();
    Ok(Json(json!({"principals": list})).into_response())
}

async fn create_principal(
    State(auth): State<AuthState>,
    Extension(caller): Extension<Caller>,
    Json(body): Json<NewPrincipal>,
) -> R {
    let auth = enabled(auth)?;
    require_root_admin(&caller)?;
    let pid = blocking(move || {
        let pw = match (&body.username, &body.password) {
            (Some(u), Some(p)) => Some((u.as_str(), p.as_str())),
            (None, None) => None,
            _ => {
                return Err(StoreError::Unsupported(
                    "username and password go together".into(),
                ))
            }
        };
        auth.create_principal(&body.kind, &body.display, body.groups.clone(), pw)
    })
    .await?;
    Ok((StatusCode::CREATED, Json(json!({"id": pid}))).into_response())
}

async fn get_principal(
    State(auth): State<AuthState>,
    Extension(caller): Extension<Caller>,
    Path(pid): Path<String>,
) -> R {
    let auth = enabled(auth)?;
    require_root_admin(&caller)?;
    let p = auth
        .principal(&pid)
        ?
        .ok_or_else(|| StoreError::NotFound(pid.clone()))?;
    Ok(Json(json!({
        "id": pid, "kind": p.kind, "display": p.display, "status": p.status, "groups": p.groups,
        "credentials": auth.credentials(&pid)?,
    }))
    .into_response())
}

#[derive(Deserialize)]
struct PrincipalPatch {
    display: Option<String>,
    status: Option<String>,
    groups: Option<Vec<String>>,
}

async fn patch_principal(
    State(auth): State<AuthState>,
    Extension(caller): Extension<Caller>,
    Path(pid): Path<String>,
    Json(body): Json<PrincipalPatch>,
) -> R {
    let auth = enabled(auth)?;
    require_root_admin(&caller)?;
    let rec = blocking(move || auth.update_principal(&pid, body.display, body.status, body.groups)).await?;
    Ok(Json(rec).into_response())
}

async fn delete_principal(
    State(auth): State<AuthState>,
    Extension(caller): Extension<Caller>,
    Path(pid): Path<String>,
) -> R {
    let auth = enabled(auth)?;
    require_root_admin(&caller)?;
    blocking(move || auth.delete_principal(&pid)).await?;
    Ok(Json(json!({"ok": true})).into_response())
}

#[derive(Deserialize)]
struct SetPassword {
    username: String,
    password: String,
}

async fn admin_set_password(
    State(auth): State<AuthState>,
    Extension(caller): Extension<Caller>,
    Path(pid): Path<String>,
    Json(body): Json<SetPassword>,
) -> R {
    let auth = enabled(auth)?;
    require_root_admin(&caller)?;
    blocking(move || auth.set_password(&pid, &body.username, &body.password)).await?;
    Ok(Json(json!({"ok": true})).into_response())
}

async fn admin_keys(
    State(auth): State<AuthState>,
    Extension(caller): Extension<Caller>,
    Path(pid): Path<String>,
) -> R {
    let auth = enabled(auth)?;
    require_root_admin(&caller)?;
    Ok(Json(json!({"credentials": auth.credentials(&pid)?})).into_response())
}

async fn admin_mint_key(
    State(auth): State<AuthState>,
    Extension(caller): Extension<Caller>,
    Path(pid): Path<String>,
    Json(req): Json<KeyRequest>,
) -> R {
    let auth = enabled(auth)?;
    require_root_admin(&caller)?;
    check_scope_within(&caller, &req)?;
    let key = blocking(move || auth.create_api_key(&pid, req)).await?;
    Ok((StatusCode::CREATED, Json(key)).into_response())
}

async fn admin_delete_credential(
    State(auth): State<AuthState>,
    Extension(caller): Extension<Caller>,
    Path((pid, cid)): Path<(String, String)>,
) -> R {
    let auth = enabled(auth)?;
    require_root_admin(&caller)?;
    blocking(move || auth.delete_credential(&pid, &cid)).await?;
    Ok(Json(json!({"ok": true})).into_response())
}

// ------------------------------------------------------------ grants

#[derive(Deserialize)]
struct NewGrant {
    subject: String,
    path: String,
    perms: Vec<String>,
}

async fn list_grants(State(auth): State<AuthState>, Extension(caller): Extension<Caller>) -> R {
    let auth = enabled(auth)?;
    let acl = auth.acl();
    let list: Vec<Value> = acl
        .grants
        .iter()
        .filter(|(_, g)| {
            Pointer::parse(&g.path).is_ok_and(|p| caller.allows(Perm::Admin, &p))
        })
        .map(|(id, g)| json!({"id": id, "subject": g.subject, "path": g.path, "perms": g.perms, "created_by": g.created_by}))
        .collect();
    Ok(Json(json!({"grants": list})).into_response())
}

/// Admin on a path delegates: you can grant anything at or under it.
async fn create_grant(
    State(auth): State<AuthState>,
    Extension(caller): Extension<Caller>,
    Json(body): Json<NewGrant>,
) -> R {
    let auth = enabled(auth)?;
    let path = Pointer::parse(&body.path)?;
    caller.require(Perm::Admin, &path)?;
    PermSet::parse(&body.perms)?;
    let by = caller.principal().map(String::from);
    let gid = blocking(move || auth.add_grant(&body.subject, &body.path, &body.perms, by.as_deref())).await?;
    Ok((StatusCode::CREATED, Json(json!({"id": gid}))).into_response())
}

async fn delete_grant(
    State(auth): State<AuthState>,
    Extension(caller): Extension<Caller>,
    Path(gid): Path<String>,
) -> R {
    let auth = enabled(auth)?;
    let g = auth
        .grant(&gid)
        ?
        .ok_or_else(|| StoreError::NotFound(gid.clone()))?;
    let path = Pointer::parse(&g.path)?;
    caller.require(Perm::Admin, &path)?;
    blocking(move || auth.delete_grant(&gid)).await?;
    Ok(Json(json!({"ok": true})).into_response())
}
