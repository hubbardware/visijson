//! Users and permissions (TODO #6): principals with many credentials,
//! server-side sessions, API keys, single-use stream tickets, and per-path
//! grants — all kept in a dedicated `sys` store that is never routed
//! directly, so identity data gets the same durability, CAS, and
//! subscriptions as documents.
//!
//! Sys store layout (every secret is stored hashed):
//!
//! ```text
//! /principals/{pid}                {kind, display, status, groups, created_ms}
//! /credentials/{pid}/{cid}         password {username, hash} | api_key {key_id, name, hash, scope?, expires_ms?}
//! /lookup/password/{username}      {principal, credential}
//! /lookup/api_key/{key_id}         {principal, credential}
//! /sessions/{sha256(token)}        {principal, credential, created_ms, expires_ms}
//! /tickets/{sha256(ticket)}        {source, scope?, expires_ms}
//! /grants/{gid}                    {subject, path, perms, created_ms, created_by}
//! ```
//!
//! Uniqueness (usernames, key ids) is enforced with absent-path conditions
//! on the lookup entries, committed atomically with the credential.

pub mod acl;
pub mod http;

use std::sync::Arc;

use arc_swap::ArcSwap;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};

use acl::CallerInner;
pub use acl::{Acl, Caller, GrantRec, Perm, PermSet, Scope, ScopeEntry, Source};

use crate::error::{Result, StoreError};
use crate::model::{from_value, Pointer};
use crate::store::{now_ms, Condition, Op, Store};

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub session_ttl_ms: u64,
    pub ticket_ttl_ms: u64,
    /// Mark the session cookie `Secure` (set when served over TLS).
    pub cookie_secure: bool,
}

impl Default for AuthConfig {
    fn default() -> Self {
        AuthConfig {
            session_ttl_ms: 7 * 24 * 3600 * 1000,
            ticket_ttl_ms: 30 * 1000,
            cookie_secure: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrincipalRec {
    /// `human` or `service`.
    pub kind: String,
    pub display: String,
    /// `active` or `disabled`.
    pub status: String,
    #[serde(default)]
    pub groups: Vec<String>,
    #[serde(default)]
    pub created_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CredentialRec {
    Password {
        username: String,
        hash: String,
        created_ms: u64,
    },
    ApiKey {
        key_id: String,
        name: String,
        hash: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scope: Option<Vec<ScopeEntry>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expires_ms: Option<u64>,
        created_ms: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LookupRec {
    principal: String,
    credential: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionRec {
    principal: String,
    credential: String,
    created_ms: u64,
    expires_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum SourceRec {
    Session {
        id: String,
    },
    ApiKey {
        principal: String,
        credential: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TicketRec {
    source: SourceRec,
    expires_ms: u64,
}

/// A freshly minted API key: the secret is only ever visible here.
#[derive(Debug, Clone, Serialize)]
pub struct NewKey {
    pub key: String,
    pub key_id: String,
    pub credential: String,
    pub principal: String,
}

/// Options for minting an API key.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct KeyRequest {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub scope: Option<Vec<ScopeEntry>>,
    #[serde(default)]
    pub expires_in_secs: Option<u64>,
}

pub struct Auth {
    sys: Arc<Store>,
    acl: ArcSwap<Acl>,
    pub cfg: AuthConfig,
}

// ------------------------------------------------------------ crypto bits

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn random_bytes<const N: usize>() -> [u8; N] {
    let mut b = [0u8; N];
    getrandom::getrandom(&mut b).expect("OS random number generator unavailable");
    b
}

fn random_hex<const N: usize>() -> String {
    hex(&random_bytes::<N>())
}

pub(crate) fn sha256_hex(s: &str) -> String {
    use sha2::{Digest, Sha256};
    hex(&Sha256::digest(s.as_bytes()))
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn hash_password(password: &str) -> Result<String> {
    use argon2::password_hash::{PasswordHasher, SaltString};
    let salt = SaltString::encode_b64(&random_bytes::<16>())
        .map_err(|e| StoreError::Unsupported(format!("salt: {e}")))?;
    argon2::Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| StoreError::Unsupported(format!("password hash: {e}")))
}

fn verify_password(password: &str, hash: &str) -> bool {
    use argon2::password_hash::{PasswordHash, PasswordVerifier};
    PasswordHash::new(hash).is_ok_and(|h| {
        argon2::Argon2::default()
            .verify_password(password.as_bytes(), &h)
            .is_ok()
    })
}

/// A valid hash of a random password: unknown usernames still pay one
/// argon2 verification, so login timing doesn't reveal which exist.
fn dummy_hash() -> &'static str {
    static H: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    H.get_or_init(|| hash_password(&random_hex::<16>()).expect("argon2 hashing works"))
}

// ------------------------------------------------------------ sys paths

/// Build a sys-store pointer from raw tokens (escaping handled by Pointer).
fn sp(tokens: &[&str]) -> String {
    Pointer::from_tokens(tokens.iter().map(|t| t.to_string()).collect()).to_string()
}

fn put_op<T: Serialize>(path: String, v: &T) -> Result<Op> {
    Ok(Op::Put {
        path,
        value: from_value(&serde_json::to_value(v)?),
    })
}

fn normalize_username(u: &str) -> Result<String> {
    let u = u.trim().to_lowercase();
    if u.is_empty() || u.len() > 128 {
        return Err(StoreError::Unsupported(
            "username must be 1–128 characters".into(),
        ));
    }
    Ok(u)
}

fn check_password_policy(pw: &str) -> Result<()> {
    if pw.chars().count() < 8 {
        return Err(StoreError::Unsupported(
            "password must be at least 8 characters".into(),
        ));
    }
    Ok(())
}

fn bad_credentials() -> StoreError {
    StoreError::Unauthorized("invalid or expired credential".into())
}

impl Auth {
    /// Wrap a sys store. If it holds no principals yet, bootstrap one admin
    /// (admin on the whole document) and return its one-time API key — the
    /// caller logs it; it is never retrievable again.
    pub fn open(sys: Arc<Store>, cfg: AuthConfig) -> Result<(Arc<Auth>, Option<NewKey>)> {
        let auth = Arc::new(Auth {
            acl: ArcSwap::from_pointee(Acl::default()),
            sys,
            cfg,
        });
        let empty = auth
            .sys
            .get("/principals")?
            .as_ref()
            .and_then(Value::as_object)
            .is_none_or(|m| m.is_empty());
        let bootstrap = if empty {
            let pid = auth.create_principal("human", "admin", Vec::new(), None)?;
            auth.add_grant(&format!("principal:{pid}"), "", &["admin".into()], None)?;
            Some(auth.create_api_key(
                &pid,
                KeyRequest {
                    name: "bootstrap".into(),
                    ..Default::default()
                },
            )?)
        } else {
            None
        };
        Ok((auth, bootstrap))
    }

    pub fn sys(&self) -> &Arc<Store> {
        &self.sys
    }

    fn get<T: DeserializeOwned>(&self, path: &str) -> Result<Option<T>> {
        match self.sys.get(path)? {
            Some(v) => Ok(Some(serde_json::from_value(v)?)),
            None => Ok(None),
        }
    }

    fn children<T: DeserializeOwned>(&self, path: &str) -> Result<Vec<(String, T)>> {
        let Some(Value::Object(m)) = self.sys.get(path)? else {
            return Ok(Vec::new());
        };
        m.into_iter()
            .map(|(k, v)| Ok((k, serde_json::from_value(v)?)))
            .collect()
    }

    /// The ACL as of the sys store's current head, rebuilt only when the
    /// sys version has moved (grant churn is admin-rate; requests are not).
    pub fn acl(&self) -> Arc<Acl> {
        let version = self.sys.version();
        let cur = self.acl.load_full();
        if cur.version == version && version != 0 {
            return cur;
        }
        let grants = self.children::<GrantRec>("/grants").unwrap_or_else(|e| {
            tracing::error!(error = %e, "reading grants failed; denying all");
            Vec::new()
        });
        let next = Arc::new(Acl::build(version, grants));
        self.acl.store(next.clone());
        next
    }

    // -------------------------------------------------------- resolution

    pub fn anonymous(&self) -> Caller {
        Caller::new(CallerInner {
            source: Source::Anonymous,
            via_cookie: false,
            principal: None,
            display: None,
            groups: Vec::new(),
            subjects: vec!["group:anonymous".into()],
            scope: None,
            expires_ms: None,
            acl: self.acl(),
        })
    }

    /// Resolve a bearer/cookie token (`vjs_…` session or `vjk_…` API key).
    pub fn authenticate(&self, token: &str, via_cookie: bool) -> Result<Caller> {
        let source = if token.starts_with("vjs_") {
            Source::Session {
                id: sha256_hex(token),
            }
        } else if let Some(rest) = token.strip_prefix("vjk_") {
            let (key_id, secret) = rest.split_once('_').ok_or_else(bad_credentials)?;
            let lookup: LookupRec = self
                .get(&sp(&["lookup", "api_key", key_id]))?
                .ok_or_else(bad_credentials)?;
            let cred: CredentialRec = self
                .get(&sp(&["credentials", &lookup.principal, &lookup.credential]))?
                .ok_or_else(bad_credentials)?;
            let CredentialRec::ApiKey { hash, .. } = cred else {
                return Err(bad_credentials());
            };
            if !ct_eq(sha256_hex(secret).as_bytes(), hash.as_bytes()) {
                return Err(bad_credentials());
            }
            Source::ApiKey {
                principal: lookup.principal,
                credential: lookup.credential,
            }
        } else {
            return Err(bad_credentials());
        };
        self.resolve(&source, via_cookie)
    }

    /// Build a `Caller` for `source` against the current sys state — the
    /// same path serves first authentication and stream re-validation.
    pub fn resolve(&self, source: &Source, via_cookie: bool) -> Result<Caller> {
        let now = now_ms();
        let (principal, scope, expires_ms) = match source {
            Source::Unrestricted => return Ok(Caller::unrestricted()),
            Source::Anonymous => return Ok(self.anonymous()),
            Source::Session { id } => {
                let s: SessionRec = self
                    .get(&sp(&["sessions", id]))?
                    .ok_or_else(bad_credentials)?;
                if s.expires_ms <= now {
                    return Err(bad_credentials());
                }
                // A session dies with the credential that opened it.
                if self
                    .sys
                    .get_node(&sp(&["credentials", &s.principal, &s.credential]))?
                    .is_none()
                {
                    return Err(bad_credentials());
                }
                (s.principal, None, Some(s.expires_ms))
            }
            Source::ApiKey {
                principal,
                credential,
            } => {
                let cred: CredentialRec = self
                    .get(&sp(&["credentials", principal, credential]))?
                    .ok_or_else(bad_credentials)?;
                let CredentialRec::ApiKey {
                    scope, expires_ms, ..
                } = cred
                else {
                    return Err(bad_credentials());
                };
                if expires_ms.is_some_and(|e| e <= now) {
                    return Err(bad_credentials());
                }
                let scope = scope.as_deref().map(acl::parse_scope).transpose()?;
                (principal.clone(), scope, expires_ms)
            }
        };
        let rec: PrincipalRec = self
            .get(&sp(&["principals", &principal]))?
            .ok_or_else(bad_credentials)?;
        if rec.status != "active" {
            return Err(StoreError::Unauthorized("account disabled".into()));
        }
        let mut subjects = vec![
            format!("principal:{principal}"),
            "group:authenticated".into(),
            "group:anonymous".into(),
        ];
        subjects.extend(rec.groups.iter().map(|g| format!("group:{g}")));
        Ok(Caller::new(CallerInner {
            source: source.clone(),
            via_cookie,
            principal: Some(principal),
            display: Some(rec.display),
            groups: rec.groups,
            subjects,
            scope,
            expires_ms,
            acl: self.acl(),
        }))
    }

    /// Re-validate a caller against current sys state (for live streams):
    /// `None` once its credential, account, or session is gone or expired.
    pub fn revalidate(&self, caller: &Caller) -> Option<Caller> {
        self.resolve(caller.source(), caller.via_cookie()).ok()
    }

    // -------------------------------------------------------- sessions

    /// Username + password → a new session token. Blocking (argon2 + an
    /// fsynced sys commit).
    pub fn login(&self, username: &str, password: &str) -> Result<(String, Caller)> {
        let username = normalize_username(username).map_err(|_| bad_credentials())?;
        let lookup: Option<LookupRec> = self.get(&sp(&["lookup", "password", &username]))?;
        let cred = match &lookup {
            Some(l) => {
                self.get::<CredentialRec>(&sp(&["credentials", &l.principal, &l.credential]))?
            }
            None => None,
        };
        let hash = match &cred {
            Some(CredentialRec::Password { hash, .. }) => hash.as_str(),
            _ => dummy_hash(),
        };
        let ok = verify_password(password, hash);
        let (Some(lookup), true) = (lookup, ok && cred.is_some()) else {
            return Err(StoreError::Unauthorized(
                "invalid username or password".into(),
            ));
        };
        let token = format!("vjs_{}", random_hex::<32>());
        let id = sha256_hex(&token);
        let now = now_ms();
        let rec = SessionRec {
            principal: lookup.principal,
            credential: lookup.credential,
            created_ms: now,
            expires_ms: now + self.cfg.session_ttl_ms,
        };
        self.sys
            .commit(vec![put_op(sp(&["sessions", &id]), &rec)?], &[])?;
        let caller = self.resolve(&Source::Session { id }, false)?;
        Ok((token, caller))
    }

    /// End the caller's session (no-op for other credential kinds).
    pub fn logout(&self, caller: &Caller) -> Result<()> {
        if let Source::Session { id } = caller.source() {
            self.sys.remove(&sp(&["sessions", id]))?;
        }
        Ok(())
    }

    /// Mint a single-use ticket for one stream connection (`?ticket=` —
    /// `EventSource` can't send headers). It resolves to the minting
    /// caller's own credential, so revoking that credential kills streams
    /// opened with the ticket, and a key's scope carries over.
    pub fn mint_ticket(&self, caller: &Caller) -> Result<(String, u64)> {
        let source = match caller.source() {
            Source::Session { id } => SourceRec::Session { id: id.clone() },
            Source::ApiKey {
                principal,
                credential,
            } => SourceRec::ApiKey {
                principal: principal.clone(),
                credential: credential.clone(),
            },
            _ => return Err(caller.deny("tickets require a credential".into())),
        };
        let ticket = format!("vjt_{}", random_hex::<32>());
        let expires_ms = now_ms() + self.cfg.ticket_ttl_ms;
        let rec = TicketRec { source, expires_ms };
        self.sys.commit(
            vec![put_op(sp(&["tickets", &sha256_hex(&ticket)]), &rec)?],
            &[],
        )?;
        Ok((ticket, expires_ms))
    }

    /// Redeem (and burn) a ticket. Blocking. The conditional remove makes
    /// single use atomic: two racing redemptions can't both succeed.
    pub fn redeem_ticket(&self, ticket: &str) -> Result<Caller> {
        let path = sp(&["tickets", &sha256_hex(ticket)]);
        let raw = self.sys.get(&path)?.ok_or_else(bad_credentials)?;
        let rec: TicketRec = serde_json::from_value(raw.clone())?;
        self.sys
            .commit(
                vec![Op::Remove { path: path.clone() }],
                &[Condition::value(path, Some(raw))],
            )
            .map_err(|_| bad_credentials())?;
        if rec.expires_ms <= now_ms() {
            return Err(bad_credentials());
        }
        let source = match rec.source {
            SourceRec::Session { id } => Source::Session { id },
            SourceRec::ApiKey {
                principal,
                credential,
            } => Source::ApiKey {
                principal,
                credential,
            },
        };
        self.resolve(&source, false)
    }

    /// Drop expired sessions and tickets. Run periodically; resolution
    /// already rejects expired entries, this only reclaims space.
    pub fn sweep(&self) -> Result<usize> {
        let now = now_ms();
        let mut ops = Vec::new();
        for (id, s) in self.children::<SessionRec>("/sessions")? {
            if s.expires_ms <= now {
                ops.push(Op::Remove {
                    path: sp(&["sessions", &id]),
                });
            }
        }
        for (id, t) in self.children::<TicketRec>("/tickets")? {
            if t.expires_ms <= now {
                ops.push(Op::Remove {
                    path: sp(&["tickets", &id]),
                });
            }
        }
        let n = ops.len();
        if n > 0 {
            self.sys.commit(ops, &[])?;
        }
        Ok(n)
    }

    // -------------------------------------------------------- principals

    pub fn create_principal(
        &self,
        kind: &str,
        display: &str,
        groups: Vec<String>,
        password: Option<(&str, &str)>,
    ) -> Result<String> {
        if kind != "human" && kind != "service" {
            return Err(StoreError::Unsupported(
                "kind must be `human` or `service`".into(),
            ));
        }
        let pid = format!("p_{}", random_hex::<8>());
        let rec = PrincipalRec {
            kind: kind.into(),
            display: display.into(),
            status: "active".into(),
            groups,
            created_ms: now_ms(),
        };
        let mut ops = vec![put_op(sp(&["principals", &pid]), &rec)?];
        let mut conds = Vec::new();
        if let Some((username, password)) = password {
            let (o, c) = self.password_ops(&pid, username, password)?;
            ops.extend(o);
            conds.extend(c);
        }
        self.sys.commit(ops, &conds).map_err(|e| match e {
            StoreError::Conflict(_) => StoreError::Conflict("username already taken".into()),
            e => e,
        })?;
        Ok(pid)
    }

    pub fn principal(&self, pid: &str) -> Result<Option<PrincipalRec>> {
        self.get(&sp(&["principals", pid]))
    }

    pub fn principals(&self) -> Result<Vec<(String, PrincipalRec)>> {
        self.children("/principals")
    }

    pub fn update_principal(
        &self,
        pid: &str,
        display: Option<String>,
        status: Option<String>,
        groups: Option<Vec<String>>,
    ) -> Result<PrincipalRec> {
        // Read-modify-write guarded by the record's version: a racing
        // update or delete fails the commit instead of being clobbered.
        let path = sp(&["principals", pid]);
        let seen = self.sys.last_modified(&path)?.version;
        let mut rec = self
            .principal(pid)?
            .ok_or_else(|| StoreError::NotFound(pid.into()))?;
        if let Some(d) = display {
            rec.display = d;
        }
        if let Some(s) = status {
            if s != "active" && s != "disabled" {
                return Err(StoreError::Unsupported(
                    "status must be `active` or `disabled`".into(),
                ));
            }
            rec.status = s;
        }
        if let Some(g) = groups {
            rec.groups = g;
        }
        self.sys.commit(
            vec![put_op(path.clone(), &rec)?],
            &[Condition::unmodified_since_version(path, seen)],
        )?;
        Ok(rec)
    }

    /// Remove a principal with everything attached: credentials, their
    /// lookups, sessions, and grants naming it — one atomic sys commit.
    pub fn delete_principal(&self, pid: &str) -> Result<()> {
        if self.principal(pid)?.is_none() {
            return Err(StoreError::NotFound(pid.into()));
        }
        let mut ops = vec![Op::Remove {
            path: sp(&["principals", pid]),
        }];
        for (_, cred) in self.children::<CredentialRec>(&sp(&["credentials", pid]))? {
            ops.push(Op::Remove {
                path: lookup_path(&cred),
            });
        }
        if self.sys.get_node(&sp(&["credentials", pid]))?.is_some() {
            ops.push(Op::Remove {
                path: sp(&["credentials", pid]),
            });
        }
        for (id, s) in self.children::<SessionRec>("/sessions")? {
            if s.principal == pid {
                ops.push(Op::Remove {
                    path: sp(&["sessions", &id]),
                });
            }
        }
        let subject = format!("principal:{pid}");
        for (id, g) in self.children::<GrantRec>("/grants")? {
            if g.subject == subject {
                ops.push(Op::Remove {
                    path: sp(&["grants", &id]),
                });
            }
        }
        self.sys.commit(ops, &[])?;
        Ok(())
    }

    // -------------------------------------------------------- credentials

    /// Ops (and uniqueness conditions) that set `pid`'s password credential,
    /// replacing any existing one (and its username lookup).
    fn password_ops(
        &self,
        pid: &str,
        username: &str,
        password: &str,
    ) -> Result<(Vec<Op>, Vec<Condition>)> {
        let username = normalize_username(username)?;
        check_password_policy(password)?;
        let mut ops = Vec::new();
        let mut conds = Vec::new();
        let mut cid = format!("c_{}", random_hex::<8>());
        let mut same_username = false;
        for (id, cred) in self.children::<CredentialRec>(&sp(&["credentials", pid]))? {
            if let CredentialRec::Password { username: old, .. } = &cred {
                cid = id; // keep the id: sessions opened by it survive a change
                same_username = *old == username;
                if !same_username {
                    ops.push(Op::Remove {
                        path: lookup_path(&cred),
                    });
                }
            }
        }
        let lookup = sp(&["lookup", "password", &username]);
        if !same_username {
            conds.push(Condition::value(lookup.clone(), None));
        }
        ops.push(put_op(
            sp(&["credentials", pid, &cid]),
            &CredentialRec::Password {
                username,
                hash: hash_password(password)?,
                created_ms: now_ms(),
            },
        )?);
        ops.push(put_op(
            lookup,
            &LookupRec {
                principal: pid.into(),
                credential: cid,
            },
        )?);
        Ok((ops, conds))
    }

    pub fn set_password(&self, pid: &str, username: &str, password: &str) -> Result<()> {
        if self.principal(pid)?.is_none() {
            return Err(StoreError::NotFound(pid.into()));
        }
        let (ops, conds) = self.password_ops(pid, username, password)?;
        self.sys.commit(ops, &conds).map_err(|e| match e {
            StoreError::Conflict(_) => StoreError::Conflict("username already taken".into()),
            e => e,
        })?;
        Ok(())
    }

    /// Self-service password change: requires the current password.
    pub fn change_password(&self, pid: &str, current: &str, new: &str) -> Result<()> {
        let existing = self
            .children::<CredentialRec>(&sp(&["credentials", pid]))?
            .into_iter()
            .find_map(|(_, c)| match c {
                CredentialRec::Password { username, hash, .. } => Some((username, hash)),
                _ => None,
            });
        let Some((username, hash)) = existing else {
            return Err(StoreError::NotFound("no password credential".into()));
        };
        if !verify_password(current, &hash) {
            return Err(StoreError::Forbidden(
                "current password is incorrect".into(),
            ));
        }
        self.set_password(pid, &username, new)
    }

    pub fn create_api_key(&self, pid: &str, req: KeyRequest) -> Result<NewKey> {
        if self.principal(pid)?.is_none() {
            return Err(StoreError::NotFound(pid.into()));
        }
        if let Some(s) = &req.scope {
            acl::parse_scope(s)?; // validate before storing
        }
        let key_id = random_hex::<8>();
        let secret = random_hex::<32>();
        let cid = format!("c_{}", random_hex::<8>());
        let now = now_ms();
        let cred = CredentialRec::ApiKey {
            key_id: key_id.clone(),
            name: req.name,
            hash: sha256_hex(&secret),
            scope: req.scope,
            expires_ms: req.expires_in_secs.map(|s| now + s * 1000),
            created_ms: now,
        };
        let lookup = sp(&["lookup", "api_key", &key_id]);
        self.sys.commit(
            vec![
                put_op(sp(&["credentials", pid, &cid]), &cred)?,
                put_op(
                    lookup.clone(),
                    &LookupRec {
                        principal: pid.into(),
                        credential: cid.clone(),
                    },
                )?,
            ],
            &[Condition::value(lookup, None)],
        )?;
        Ok(NewKey {
            key: format!("vjk_{key_id}_{secret}"),
            key_id,
            credential: cid,
            principal: pid.into(),
        })
    }

    /// `pid`'s credentials with secrets stripped.
    pub fn credentials(&self, pid: &str) -> Result<Vec<Value>> {
        Ok(self
            .children::<CredentialRec>(&sp(&["credentials", pid]))?
            .into_iter()
            .map(|(id, c)| match c {
                CredentialRec::Password {
                    username,
                    created_ms,
                    ..
                } => json!({
                    "id": id, "kind": "password", "username": username, "created_ms": created_ms,
                }),
                CredentialRec::ApiKey {
                    key_id,
                    name,
                    scope,
                    expires_ms,
                    created_ms,
                    ..
                } => json!({
                    "id": id, "kind": "api_key", "key_id": key_id, "name": name,
                    "scope": scope, "expires_ms": expires_ms, "created_ms": created_ms,
                }),
            })
            .collect())
    }

    /// Remove one of `pid`'s credentials (and its lookup). Sessions opened
    /// with it die on their next resolution; live streams on their next
    /// re-validation.
    pub fn delete_credential(&self, pid: &str, cid: &str) -> Result<()> {
        let path = sp(&["credentials", pid, cid]);
        let cred: CredentialRec = self
            .get(&path)?
            .ok_or_else(|| StoreError::NotFound(cid.into()))?;
        self.sys.commit(
            vec![
                Op::Remove { path },
                Op::Remove {
                    path: lookup_path(&cred),
                },
            ],
            &[],
        )?;
        Ok(())
    }

    // -------------------------------------------------------- grants

    pub fn add_grant(
        &self,
        subject: &str,
        path: &str,
        perms: &[String],
        created_by: Option<&str>,
    ) -> Result<String> {
        let valid_subject = subject
            .strip_prefix("principal:")
            .or_else(|| subject.strip_prefix("group:"))
            .is_some_and(|s| !s.is_empty());
        if !valid_subject {
            return Err(StoreError::Unsupported(
                "subject must be `principal:<id>` or `group:<name>`".into(),
            ));
        }
        Pointer::parse(path)?;
        if PermSet::parse(perms)? == PermSet::default() {
            return Err(StoreError::Unsupported(
                "a grant needs at least one permission".into(),
            ));
        }
        let gid = format!("g_{}", random_hex::<8>());
        let rec = GrantRec {
            subject: subject.into(),
            path: path.into(),
            perms: perms.to_vec(),
            created_ms: now_ms(),
            created_by: created_by.map(String::from),
        };
        self.sys
            .commit(vec![put_op(sp(&["grants", &gid]), &rec)?], &[])?;
        Ok(gid)
    }

    pub fn grant(&self, gid: &str) -> Result<Option<GrantRec>> {
        self.get(&sp(&["grants", gid]))
    }

    pub fn delete_grant(&self, gid: &str) -> Result<()> {
        if self.grant(gid)?.is_none() {
            return Err(StoreError::NotFound(gid.into()));
        }
        self.sys.remove(&sp(&["grants", gid]))?;
        Ok(())
    }
}

fn lookup_path(cred: &CredentialRec) -> String {
    match cred {
        CredentialRec::Password { username, .. } => sp(&["lookup", "password", username]),
        CredentialRec::ApiKey { key_id, .. } => sp(&["lookup", "api_key", key_id]),
    }
}
