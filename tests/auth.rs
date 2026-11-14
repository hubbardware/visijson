//! Users and permissions end to end (TODO #6): credentials → callers,
//! effect-based write authorization, definer-rights formulas, side-channel
//! rules, cookie CSRF/Origin rules, tickets, and live-stream revocation.

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use visijson::api::router_with_auth;
use visijson::auth::{Auth, AuthConfig};
use visijson::store::Store;

struct Server {
    base: String,
    doc: Arc<Store>,
    auth: Arc<Auth>,
    /// The bootstrap admin's API key.
    admin: String,
    http: reqwest::Client,
}

async fn serve() -> Server {
    let doc = Store::in_memory();
    let info = Store::in_memory();
    let (auth, key) = Auth::open(Store::in_memory(), AuthConfig::default()).unwrap();
    let admin = key.expect("empty sys store bootstraps an admin").key;
    let app = router_with_auth(doc.clone(), info, Some(auth.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Server {
        base: format!("http://{addr}"),
        doc,
        auth,
        admin,
        http: reqwest::Client::new(),
    }
}

impl Server {
    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// Create a user with a password and return (principal id, session token).
    async fn user(&self, name: &str) -> (String, String) {
        let resp = self
            .http
            .post(self.url("/auth/principals"))
            .bearer_auth(&self.admin)
            .json(&json!({"display": name, "username": name, "password": "correct horse"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 201);
        let pid = resp.json::<Value>().await.unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();
        let token = self.login(name, "correct horse").await;
        (pid, token)
    }

    async fn login(&self, name: &str, password: &str) -> String {
        let resp = self
            .http
            .post(self.url("/auth/login"))
            .json(&json!({"username": name, "password": password}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "login as {name}");
        resp.json::<Value>().await.unwrap()["token"]
            .as_str()
            .unwrap()
            .to_string()
    }

    async fn grant(&self, subject: &str, path: &str, perms: &[&str]) -> String {
        let resp = self
            .http
            .post(self.url("/auth/grants"))
            .bearer_auth(&self.admin)
            .json(&json!({"subject": subject, "path": path, "perms": perms}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 201);
        resp.json::<Value>().await.unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string()
    }

    async fn put(&self, token: &str, path: &str, body: Value) -> reqwest::StatusCode {
        self.http
            .put(self.url(&format!("/doc{path}")))
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .unwrap()
            .status()
    }

    async fn get(&self, token: Option<&str>, path: &str) -> reqwest::Response {
        let mut req = self.http.get(self.url(path));
        if let Some(t) = token {
            req = req.bearer_auth(t);
        }
        req.send().await.unwrap()
    }
}

#[tokio::test]
async fn disabled_auth_reports_itself_and_allows_everything() {
    let doc = Store::in_memory();
    let app = visijson::api::router(doc.clone(), Store::in_memory());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let http = reqwest::Client::new();
    let who: Value = http
        .get(format!("http://{addr}/auth/whoami"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(who, json!({"auth_enabled": false}));
    let resp = http
        .put(format!("http://{addr}/doc/x"))
        .json(&json!(1))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    // Auth endpoints that need it say it's off.
    let resp = http
        .post(format!("http://{addr}/auth/login"))
        .json(&json!({"username": "a", "password": "b"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn anonymous_is_401_until_a_public_grant_exists() {
    let s = serve().await;
    s.doc.put("/public/news", json!("hi")).unwrap();
    s.doc.put("/private", json!("secret")).unwrap();

    let resp = s.get(None, "/doc/public/news").await;
    assert_eq!(resp.status(), 401);
    assert!(resp.headers().contains_key("www-authenticate"));
    // Unreadable paths are 401/403 even when absent: a 404 would reveal
    // existence.
    assert_eq!(s.get(None, "/doc/nope").await.status(), 401);

    s.grant("group:anonymous", "/public", &["read"]).await;
    assert_eq!(s.get(None, "/doc/public/news").await.status(), 200);
    assert_eq!(s.get(None, "/doc/private").await.status(), 401);
    assert_eq!(
        s.get(None, "/doc").await.status(),
        401,
        "no holes: root needs a root grant"
    );
    // /info stays public regardless.
    assert_ne!(s.get(None, "/info").await.status(), 401);
    // The UI shell is served to anyone; data still needs a grant.
    let resp = s
        .http
        .get(s.url("/doc/private"))
        .header("accept", "text/html")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn login_whoami_logout() {
    let s = serve().await;
    let (pid, token) = s.user("alice").await;
    s.grant(&format!("principal:{pid}"), "/teamA", &["read", "write"])
        .await;

    let who: Value = s
        .get(Some(&token), "/auth/whoami")
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(who["principal"], json!(pid));
    assert_eq!(who["via"], "session");
    assert_eq!(who["anonymous"], false);
    assert_eq!(who["grants"][0]["path"], "/teamA");

    // Wrong password and unknown user are indistinguishable.
    for (u, p) in [("alice", "wrong password"), ("nobody", "correct horse")] {
        let resp = s
            .http
            .post(s.url("/auth/login"))
            .json(&json!({"username": u, "password": p}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401);
        assert_eq!(
            resp.json::<Value>().await.unwrap()["error"],
            "authentication required: invalid username or password"
        );
    }
    // Usernames are case-insensitive and unique.
    s.login("ALICE", "correct horse").await;
    let resp = s
        .http
        .post(s.url("/auth/principals"))
        .bearer_auth(&s.admin)
        .json(&json!({"display": "x", "username": "Alice", "password": "something long"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);

    let resp = s
        .http
        .post(s.url("/auth/logout"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        s.get(Some(&token), "/auth/whoami").await.status(),
        401,
        "session is gone"
    );
}

#[tokio::test]
async fn writes_authorize_on_the_effect_not_the_url() {
    let s = serve().await;
    s.doc
        .put("", json!({"teamA": {"score": 1}, "teamB": {"score": 1}}))
        .unwrap();
    let (pid, token) = s.user("syncer").await;
    let me = format!("principal:{pid}");
    s.grant(&me, "/teamA", &["write"]).await;

    // Write-only on /teamA, no read on /: a full-root PUT is refused even
    // if it only changes /teamA — otherwise accept/deny would be an oracle
    // for the values of /teamB.
    let full = json!({"teamA": {"score": 2}, "teamB": {"score": 1}});
    assert_eq!(s.put(&token, "", full.clone()).await, 403);

    // With read on the root, the poll-style full replace is fine as long
    // as its *effect* stays inside /teamA...
    s.grant(&me, "", &["read"]).await;
    assert_eq!(s.put(&token, "", full).await, 200);
    assert_eq!(s.doc.get("/teamA/score").unwrap(), Some(json!(2)));
    // ...and refused once it reaches into /teamB.
    let reaching = json!({"teamA": {"score": 2}, "teamB": {"score": 99}});
    assert_eq!(s.put(&token, "", reaching).await, 403);
    assert_eq!(s.doc.get("/teamB/score").unwrap(), Some(json!(1)));

    // Every write verb goes through the same guard.
    let resp = s
        .http
        .delete(s.url("/doc/teamB"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    let resp = s
        .http
        .patch(s.url("/doc/teamA"))
        .bearer_auth(&token)
        .json(&json!({"score": 5}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let resp = s
        .http
        .post(s.url("/txn"))
        .bearer_auth(&token)
        .json(&json!({"ops": [{"op": "put", "path": "/teamB/score", "value": 0}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
}

#[tokio::test]
async fn conditions_and_formula_listing_need_read() {
    let s = serve().await;
    s.doc.put("/secret", json!(42)).unwrap();
    s.doc.put("/mine/f", json!({"$formula": "1 + 1"})).unwrap();
    s.doc
        .put("/hidden/f", json!({"$formula": "2 + 2"}))
        .unwrap();
    let (pid, token) = s.user("bob").await;
    s.grant(&format!("principal:{pid}"), "/mine", &["read", "write"])
        .await;

    // A test op on /secret would reveal its value.
    let resp = s
        .http
        .post(s.url("/txn"))
        .bearer_auth(&token)
        .json(&json!({
            "ops": [{"op": "put", "path": "/mine/x", "value": 1}],
            "conditions": [{"path": "/secret", "expected": 42}],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);

    let listed: Value = s.get(Some(&token), "/formulas").await.json().await.unwrap();
    let paths: Vec<&str> = listed["formulas"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["path"].as_str().unwrap())
        .collect();
    assert_eq!(paths, vec!["/mine/f"]);
}

#[tokio::test]
async fn formulas_need_formula_perm_and_read_on_their_dependencies() {
    let s = serve().await;
    s.doc
        .put(
            "",
            json!({"raw": {"a": 2, "b": 3}, "salary": 100, "views": {}}),
        )
        .unwrap();
    let (pid, token) = s.user("analyst").await;
    let me = format!("principal:{pid}");
    s.grant(&me, "/views", &["read", "write"]).await;

    let sum = json!({"$formula": "/raw/a + /raw/b"});
    assert_eq!(
        s.put(&token, "/views/total", sum.clone()).await,
        403,
        "write alone can't define formulas"
    );

    s.grant(&me, "/views", &["formula"]).await;
    assert_eq!(
        s.put(&token, "/views/total", sum.clone()).await,
        403,
        "can't read /raw yet"
    );
    let peek = json!({"$formula": "/salary"});
    assert_eq!(s.put(&token, "/views/peek", peek).await, 403);

    s.grant(&me, "/raw", &["read"]).await;
    assert_eq!(s.put(&token, "/views/total", sum).await, 200);
    assert_eq!(s.doc.get("/views/total").unwrap(), Some(json!(5)));

    // Definer's rights: a reader of /views sees the computed value without
    // any access to /raw.
    let (rpid, rtoken) = s.user("reader").await;
    s.grant(&format!("principal:{rpid}"), "/views", &["read"])
        .await;
    let v: Value = s
        .get(Some(&rtoken), "/doc/views/total")
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(v, json!(5));
    assert_eq!(s.get(Some(&rtoken), "/doc/raw").await.status(), 403);

    // A plain writer re-PUTting a subtree that already holds an unchanged
    // formula doesn't need `formula` — nothing about the formula changed.
    let (wpid, wtoken) = s.user("writer").await;
    s.grant(&format!("principal:{wpid}"), "/views", &["read", "write"])
        .await;
    let raw: Value = s
        .http
        .get(s.url("/doc/views?raw=true"))
        .bearer_auth(&wtoken)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mut body = raw.clone();
    body["note"] = json!("hello");
    assert_eq!(s.put(&wtoken, "/views", body).await, 200);
    // But changing the expression is a formula write.
    let mut body = raw;
    body["total"] = json!({"$formula": "/raw/a * 10"});
    assert_eq!(s.put(&wtoken, "/views", body).await, 403);
}

#[tokio::test]
async fn api_keys_scope_down_never_up() {
    let s = serve().await;
    s.doc
        .put("", json!({"public": {"n": 1}, "other": 1}))
        .unwrap();
    let (pid, token) = s.user("carol").await;
    s.grant(&format!("principal:{pid}"), "", &["read", "write"])
        .await;

    let resp = s
        .http
        .post(s.url("/auth/keys"))
        .bearer_auth(&token)
        .json(&json!({"name": "dashboard", "scope": [{"path": "/public", "perms": ["read"]}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let key = resp.json::<Value>().await.unwrap();
    let k = key["key"].as_str().unwrap().to_string();
    assert!(k.starts_with("vjk_"));

    assert_eq!(s.get(Some(&k), "/doc/public/n").await.status(), 200);
    assert_eq!(s.get(Some(&k), "/doc/other").await.status(), 403);
    assert_eq!(
        s.put(&k, "/public/n", json!(2)).await,
        403,
        "scope is read-only"
    );

    // A scoped key can't mint itself something wider.
    let resp = s
        .http
        .post(s.url("/auth/keys"))
        .bearer_auth(&k)
        .json(&json!({"name": "wider"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);

    // Listing never exposes secrets; deleting revokes.
    let listed: Value = s
        .get(Some(&token), "/auth/keys")
        .await
        .json()
        .await
        .unwrap();
    assert!(!listed.to_string().contains(&k[k.len() - 20..]));
    let cid = key["credential"].as_str().unwrap();
    let resp = s
        .http
        .delete(s.url(&format!("/auth/keys/{cid}")))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(s.get(Some(&k), "/doc/public/n").await.status(), 401);

    // Garbage tokens are a hard 401, not a silent anonymous.
    assert_eq!(
        s.get(Some("vjk_nope_nope"), "/doc/public").await.status(),
        401
    );
}

#[tokio::test]
async fn cookie_sessions_need_the_csrf_header_and_stale_cookies_fall_back() {
    let s = serve().await;
    let (pid, _) = s.user("dave").await;
    s.grant(&format!("principal:{pid}"), "", &["read", "write"])
        .await;
    let resp = s
        .http
        .post(s.url("/auth/login"))
        .json(&json!({"username": "dave", "password": "correct horse"}))
        .send()
        .await
        .unwrap();
    let set_cookie = resp.headers()["set-cookie"].to_str().unwrap().to_string();
    assert!(set_cookie.contains("HttpOnly") && set_cookie.contains("SameSite=Strict"));
    let cookie = set_cookie.split(';').next().unwrap().to_string();

    let put = |with_header: bool| {
        let mut req = s
            .http
            .put(s.url("/doc/x"))
            .header("cookie", &cookie)
            .json(&json!(1));
        if with_header {
            req = req.header("x-visijson-request", "1");
        }
        req.send()
    };
    assert_eq!(put(false).await.unwrap().status(), 403);
    assert_eq!(put(true).await.unwrap().status(), 200);
    // Reads by cookie need no header.
    let resp = s
        .http
        .get(s.url("/doc/x"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // A stale cookie degrades to anonymous rather than blocking re-login.
    let resp = s
        .http
        .post(s.url("/auth/login"))
        .header("cookie", "vj_session=vjs_stale")
        .json(&json!({"username": "dave", "password": "correct horse"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn cookie_websocket_upgrade_requires_same_origin() {
    let s = serve().await;
    let (pid, token) = s.user("erin").await;
    s.grant(&format!("principal:{pid}"), "", &["read"]).await;
    let ws_url = s.base.replace("http://", "ws://") + "/doc";
    let host = s.base.trim_start_matches("http://").to_string();

    let mut req = ws_url.as_str().into_client_request().unwrap();
    req.headers_mut()
        .insert("cookie", format!("vj_session={token}").parse().unwrap());
    req.headers_mut()
        .insert("origin", "http://evil.example".parse().unwrap());
    assert!(tokio_tungstenite::connect_async(req).await.is_err());

    let mut req = ws_url.as_str().into_client_request().unwrap();
    req.headers_mut()
        .insert("cookie", format!("vj_session={token}").parse().unwrap());
    req.headers_mut()
        .insert("origin", format!("http://{host}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    let Some(Ok(Message::Text(t))) = ws.next().await else {
        panic!("no snapshot")
    };
    assert_eq!(
        serde_json::from_str::<Value>(&t).unwrap()["type"],
        "snapshot"
    );
}

async fn next_frame(resp: &mut reqwest::Response, buf: &mut String) -> Option<String> {
    loop {
        if let Some(pos) = buf.find("\n\n") {
            let frame = buf[..pos].to_string();
            buf.replace_range(..pos + 2, "");
            if frame.lines().any(|l| l.starts_with("data:")) {
                return Some(frame);
            }
            continue;
        }
        let chunk = tokio::time::timeout(Duration::from_secs(5), resp.chunk())
            .await
            .expect("stream stalled")
            .unwrap()?;
        buf.push_str(std::str::from_utf8(&chunk).unwrap());
    }
}

#[tokio::test]
async fn tickets_are_single_use_stream_credentials() {
    let s = serve().await;
    s.doc.put("/w", json!(0)).unwrap();
    let (pid, token) = s.user("frank").await;
    s.grant(&format!("principal:{pid}"), "/w", &["read"]).await;
    let t: Value = s
        .http
        .post(s.url("/auth/ticket"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let ticket = t["ticket"].as_str().unwrap();
    let url = s.url(&format!("/doc/w?ticket={ticket}"));
    let mut resp = s
        .http
        .get(&url)
        .header("accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let mut buf = String::new();
    assert!(next_frame(&mut resp, &mut buf)
        .await
        .unwrap()
        .contains("event: snapshot"));
    let again = s
        .http
        .get(&url)
        .header("accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(again.status(), 401, "a ticket authorizes one connection");
    // Anonymous can't mint tickets.
    let resp = s.http.post(s.url("/auth/ticket")).send().await.unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn revoking_a_grant_closes_live_streams() {
    let s = serve().await;
    s.doc.put("/w", json!(0)).unwrap();
    let (pid, token) = s.user("gina").await;
    let gid = s.grant(&format!("principal:{pid}"), "/w", &["read"]).await;

    // SSE
    let mut sse = s
        .http
        .get(s.url("/doc/w"))
        .bearer_auth(&token)
        .header("accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    let mut buf = String::new();
    next_frame(&mut sse, &mut buf).await.unwrap();

    // WebSocket
    let mut req = (s.base.replace("http://", "ws://") + "/doc/w")
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    ws.next().await.unwrap().unwrap(); // snapshot

    // Still live: a change arrives on both.
    s.doc.put("/w", json!(1)).unwrap();
    assert!(next_frame(&mut sse, &mut buf)
        .await
        .unwrap()
        .contains("event: change"));
    ws.next().await.unwrap().unwrap();

    // Unrelated sys churn (another login) re-validates but keeps them open.
    s.user("unrelated").await;
    s.doc.put("/w", json!(2)).unwrap();
    assert!(next_frame(&mut sse, &mut buf)
        .await
        .unwrap()
        .contains("event: change"));
    ws.next().await.unwrap().unwrap();

    let resp = s
        .http
        .delete(s.url(&format!("/auth/grants/{gid}")))
        .bearer_auth(&s.admin)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        next_frame(&mut sse, &mut buf).await,
        None,
        "SSE stream ends"
    );
    match tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap()
    {
        Some(Ok(Message::Close(Some(f)))) => assert_eq!(f.reason, "access revoked"),
        other => panic!("expected a close frame, got {other:?}"),
    }
}

#[tokio::test]
async fn disabling_or_deleting_a_principal_cuts_it_off() {
    let s = serve().await;
    let (pid, token) = s.user("hank").await;
    s.grant(&format!("principal:{pid}"), "", &["read"]).await;
    s.doc.put("/x", json!(1)).unwrap();
    assert_eq!(s.get(Some(&token), "/doc/x").await.status(), 200);

    let resp = s
        .http
        .patch(s.url(&format!("/auth/principals/{pid}")))
        .bearer_auth(&s.admin)
        .json(&json!({"status": "disabled"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(s.get(Some(&token), "/doc/x").await.status(), 401);

    // Non-admins can't manage principals.
    let (_, other) = s.user("ivy").await;
    let resp = s
        .http
        .delete(s.url(&format!("/auth/principals/{pid}")))
        .bearer_auth(&other)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);

    let resp = s
        .http
        .delete(s.url(&format!("/auth/principals/{pid}")))
        .bearer_auth(&s.admin)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    // Everything attached went with it: lookup, sessions, grants.
    let sys = s.auth.sys();
    assert_eq!(sys.get("/lookup/password/hank").unwrap(), None);
    let grants = sys.get("/grants").unwrap().unwrap();
    assert!(!grants.to_string().contains(&pid));
    let sessions = sys.get("/sessions").unwrap().unwrap();
    assert!(!sessions.to_string().contains(&pid));
}

#[tokio::test]
async fn path_admins_delegate_within_their_subtree() {
    let s = serve().await;
    let (lead, lead_token) = s.user("lead").await;
    let (member, member_token) = s.user("member").await;
    s.grant(&format!("principal:{lead}"), "/teamA", &["admin"])
        .await;
    s.doc.put("/teamA/x", json!(1)).unwrap();

    let grant = |path: &str| {
        s.http
            .post(s.url("/auth/grants"))
            .bearer_auth(&lead_token)
            .json(
                &json!({"subject": format!("principal:{member}"), "path": path, "perms": ["read"]}),
            )
            .send()
    };
    assert_eq!(grant("/teamA/x").await.unwrap().status(), 201);
    assert_eq!(grant("/teamB").await.unwrap().status(), 403);
    assert_eq!(
        s.get(Some(&member_token), "/doc/teamA/x").await.status(),
        200
    );
    // The lead sees only grants under what they administer.
    let listed: Value = s
        .get(Some(&lead_token), "/auth/grants")
        .await
        .json()
        .await
        .unwrap();
    assert!(listed["grants"]
        .as_array()
        .unwrap()
        .iter()
        .all(|g| g["path"].as_str().unwrap().starts_with("/teamA")));
}

#[test]
fn wal_records_the_actor() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(visijson::StoreConfig {
        data_dir: Some(dir.path().to_path_buf()),
        ..Default::default()
    })
    .unwrap();
    store
        .commit_with(
            vec![visijson::Op::Put {
                path: "/a".into(),
                value: visijson::model::from_value(&json!(1)),
            }],
            &[],
            visijson::CommitCtx {
                actor: Some("p_test"),
                guard: None,
            },
        )
        .unwrap();
    store.put("/b", json!(2)).unwrap();
    let mut wal = String::new();
    for e in std::fs::read_dir(dir.path().join("wal")).unwrap() {
        wal.push_str(&std::fs::read_to_string(e.unwrap().path()).unwrap());
    }
    let records: Vec<Value> = wal
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(records[0]["actor"], "p_test");
    assert!(
        records[1].get("actor").is_none(),
        "anonymous library commits record no actor"
    );
}
