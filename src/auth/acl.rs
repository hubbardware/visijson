//! Permissions, grants, and the per-request `Caller` that checks them.
//!
//! A grant is `subject × path × perms` and covers the whole subtree at
//! `path` — there are no deny entries and no holes (TODO #6, rule 3), so
//! "may P read X" is a walk over P's subjects asking whether any grant path
//! is a prefix of X. The `Acl` is an immutable view built from the sys
//! store, swapped whenever the sys store's version moves.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::{Result, StoreError};
use crate::model::Pointer;
use crate::store::{CommitCtx, WriteEffect, WriteGuard};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Perm {
    Read,
    Write,
    /// Create or change formulas (jq programs run inside the commit path,
    /// so this is separate from plain `write`).
    Formula,
    /// Manage grants at/under the path; on the root, manage principals too.
    /// Implies every other permission.
    Admin,
}

impl Perm {
    const ALL: [Perm; 4] = [Perm::Read, Perm::Write, Perm::Formula, Perm::Admin];

    fn bit(self) -> u8 {
        match self {
            Perm::Read => 1,
            Perm::Write => 2,
            Perm::Formula => 4,
            Perm::Admin => 8,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Perm::Read => "read",
            Perm::Write => "write",
            Perm::Formula => "formula",
            Perm::Admin => "admin",
        }
    }

    pub fn parse(s: &str) -> Result<Perm> {
        Perm::ALL
            .into_iter()
            .find(|p| p.name() == s)
            .ok_or_else(|| {
                StoreError::Unsupported(format!(
                    "unknown permission `{s}` (use read, write, formula, admin)"
                ))
            })
    }
}

/// A set of permissions; `admin` implies all the others.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PermSet(u8);

impl PermSet {
    pub fn has(self, p: Perm) -> bool {
        self.0 & (p.bit() | Perm::Admin.bit()) != 0
    }

    pub fn parse(names: &[String]) -> Result<PermSet> {
        let mut bits = 0;
        for n in names {
            bits |= Perm::parse(n)?.bit();
        }
        Ok(PermSet(bits))
    }

    pub fn names(self) -> Vec<String> {
        Perm::ALL
            .into_iter()
            .filter(|p| self.0 & p.bit() != 0)
            .map(|p| p.name().to_string())
            .collect()
    }

    /// Is every permission in `self` also granted by `other`?
    pub fn within(self, other: PermSet) -> bool {
        Perm::ALL
            .into_iter()
            .filter(|p| self.0 & p.bit() != 0)
            .all(|p| other.has(p))
    }
}

/// One `(path, perms)` entry, as stored for grants and API-key scopes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScopeEntry {
    pub path: String,
    pub perms: Vec<String>,
}

/// A parsed scope: an API key (or a ticket minted from one) may only use
/// permissions some entry covers — scope ∩ grants, never wider.
pub type Scope = Arc<Vec<(Pointer, PermSet)>>;

pub fn parse_scope(entries: &[ScopeEntry]) -> Result<Scope> {
    entries
        .iter()
        .map(|e| Ok((Pointer::parse(&e.path)?, PermSet::parse(&e.perms)?)))
        .collect::<Result<Vec<_>>>()
        .map(Arc::new)
}

/// Stored grant record (`/grants/{id}` in the sys store).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantRec {
    /// `principal:<id>` or `group:<name>`.
    pub subject: String,
    pub path: String,
    pub perms: Vec<String>,
    #[serde(default)]
    pub created_ms: u64,
    #[serde(default)]
    pub created_by: Option<String>,
}

/// Every grant in force at one sys-store version, indexed by subject.
#[derive(Debug, Default)]
pub struct Acl {
    pub version: u64,
    by_subject: HashMap<String, Vec<(Pointer, PermSet)>>,
    /// `(grant id, record)` in id order — for listings.
    pub grants: Vec<(String, GrantRec)>,
}

impl Acl {
    pub fn build(version: u64, grants: Vec<(String, GrantRec)>) -> Acl {
        let mut by_subject: HashMap<String, Vec<(Pointer, PermSet)>> = HashMap::new();
        for (id, g) in &grants {
            // Invalid records can't enter through the API; skip rather than
            // fail every request if one ever appears.
            match (Pointer::parse(&g.path), PermSet::parse(&g.perms)) {
                (Ok(p), Ok(ps)) => by_subject.entry(g.subject.clone()).or_default().push((p, ps)),
                _ => tracing::warn!(grant = %id, "skipping malformed grant"),
            }
        }
        Acl {
            version,
            by_subject,
            grants,
        }
    }

    pub fn allows(&self, subjects: &[String], perm: Perm, ptr: &Pointer) -> bool {
        subjects.iter().any(|s| {
            self.by_subject.get(s).is_some_and(|gs| {
                gs.iter()
                    .any(|(p, ps)| ps.has(perm) && p.is_prefix_of(ptr))
            })
        })
    }

    /// Grants that apply to any of `subjects`.
    pub fn grants_for<'a>(
        &'a self,
        subjects: &'a [String],
    ) -> impl Iterator<Item = &'a (String, GrantRec)> + 'a {
        self.grants.iter().filter(|(_, g)| subjects.contains(&g.subject))
    }
}

/// How a request authenticated — also what a live stream re-validates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    /// Auth disabled (or a public namespace): every check passes.
    Unrestricted,
    Anonymous,
    /// A session, by the sha256 of its token.
    Session { id: String },
    ApiKey { principal: String, credential: String },
}

/// The authenticated party behind one request (or one live stream). Cheap
/// to clone; immutable — re-validation builds a fresh one.
#[derive(Clone)]
pub struct Caller(Arc<CallerInner>);

pub(crate) struct CallerInner {
    pub source: Source,
    /// Authenticated by cookie (CSRF/Origin rules apply).
    pub via_cookie: bool,
    pub principal: Option<String>,
    pub display: Option<String>,
    pub groups: Vec<String>,
    /// Grant subjects this caller acts as (principal, groups, pseudo-groups).
    pub subjects: Vec<String>,
    pub scope: Option<Scope>,
    /// When the credential stops being valid (epoch ms).
    pub expires_ms: Option<u64>,
    pub acl: Arc<Acl>,
}

impl Caller {
    pub fn unrestricted() -> Caller {
        Caller(Arc::new(CallerInner {
            source: Source::Unrestricted,
            via_cookie: false,
            principal: None,
            display: None,
            groups: Vec::new(),
            subjects: Vec::new(),
            scope: None,
            expires_ms: None,
            acl: Arc::new(Acl::default()),
        }))
    }

    pub(crate) fn new(inner: CallerInner) -> Caller {
        Caller(Arc::new(inner))
    }

    pub(crate) fn inner(&self) -> &CallerInner {
        &self.0
    }

    pub fn source(&self) -> &Source {
        &self.0.source
    }

    pub fn is_unrestricted(&self) -> bool {
        self.0.source == Source::Unrestricted
    }

    pub fn is_anonymous(&self) -> bool {
        self.0.source == Source::Anonymous
    }

    pub fn via_cookie(&self) -> bool {
        self.0.via_cookie
    }

    pub fn principal(&self) -> Option<&str> {
        self.0.principal.as_deref()
    }

    pub fn expires_ms(&self) -> Option<u64> {
        self.0.expires_ms
    }

    pub fn scope(&self) -> Option<&Scope> {
        self.0.scope.as_ref()
    }

    pub fn allows(&self, perm: Perm, ptr: &Pointer) -> bool {
        if self.is_unrestricted() {
            return true;
        }
        let granted = self.0.acl.allows(&self.0.subjects, perm, ptr);
        granted
            && self.0.scope.as_ref().is_none_or(|s| {
                s.iter().any(|(p, ps)| ps.has(perm) && p.is_prefix_of(ptr))
            })
    }

    /// The error for a denied check: 401 for the anonymous caller (log in
    /// and try again), 403 for an authenticated one.
    pub fn deny(&self, msg: String) -> StoreError {
        if self.is_anonymous() {
            StoreError::Unauthorized(msg)
        } else {
            StoreError::Forbidden(msg)
        }
    }

    pub fn require(&self, perm: Perm, ptr: &Pointer) -> Result<()> {
        if self.allows(perm, ptr) {
            Ok(())
        } else {
            let at = if ptr.is_root() { "/".to_string() } else { ptr.to_string() };
            Err(self.deny(format!("{} access to {at} denied", perm.name())))
        }
    }

    /// The commit context for this caller: actor for the WAL, and itself as
    /// the write guard (none at all when unrestricted — zero cost with auth
    /// off).
    pub fn ctx(&self) -> CommitCtx<'_> {
        if self.is_unrestricted() {
            return CommitCtx::default();
        }
        CommitCtx {
            actor: Some(self.0.principal.as_deref().unwrap_or("anonymous")),
            guard: Some(self),
        }
    }

    /// Grants that apply to this caller, for `whoami`.
    pub fn grants(&self) -> Vec<&(String, GrantRec)> {
        self.0.acl.grants_for(&self.0.subjects).collect()
    }
}

/// Writes authorize on the *effect* (TODO #6, rule 1): write on every
/// changed path, `formula` on every formula written plus read on each of
/// its dependencies (definer's rights), and — the oracle guard — each op's
/// target must be writable or at least readable, so accept/deny of a
/// guessed full replace can't leak values the caller can't see.
impl WriteGuard for Caller {
    fn check(&self, effect: &WriteEffect<'_>) -> Result<()> {
        for op in effect.ops {
            let target = Pointer::parse(op_path(op))?;
            if !self.allows(Perm::Write, &target) && !self.allows(Perm::Read, &target) {
                return Err(self.require(Perm::Write, &target).unwrap_err());
            }
        }
        for c in effect.changed {
            self.require(Perm::Write, c)?;
        }
        for (path, info) in &effect.formulas_written {
            self.require(Perm::Formula, path)?;
            for dep in &info.deps {
                if !self.allows(Perm::Read, dep) {
                    return Err(self.deny(format!(
                        "formula at {path} reads {dep}, which you cannot read"
                    )));
                }
            }
        }
        Ok(())
    }
}

fn op_path(op: &crate::store::Op) -> &str {
    use crate::store::Op;
    match op {
        Op::Put { path, .. }
        | Op::Insert { path, .. }
        | Op::Remove { path }
        | Op::Append { path, .. }
        | Op::Merge { path, .. } => path,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acl(grants: &[(&str, &str, &[&str])]) -> Arc<Acl> {
        Arc::new(Acl::build(
            1,
            grants
                .iter()
                .enumerate()
                .map(|(i, (s, p, ps))| {
                    (
                        format!("g{i}"),
                        GrantRec {
                            subject: s.to_string(),
                            path: p.to_string(),
                            perms: ps.iter().map(|s| s.to_string()).collect(),
                            created_ms: 0,
                            created_by: None,
                        },
                    )
                })
                .collect(),
        ))
    }

    fn caller(acl: Arc<Acl>, subjects: &[&str], scope: Option<Scope>) -> Caller {
        Caller::new(CallerInner {
            source: Source::Session { id: "s".into() },
            via_cookie: false,
            principal: Some("p_1".into()),
            display: None,
            groups: Vec::new(),
            subjects: subjects.iter().map(|s| s.to_string()).collect(),
            scope,
            expires_ms: None,
            acl,
        })
    }

    fn ptr(s: &str) -> Pointer {
        Pointer::parse(s).unwrap()
    }

    #[test]
    fn grants_cover_subtrees_and_admin_implies_all() {
        let a = acl(&[
            ("principal:p_1", "/teamA", &["read", "write"]),
            ("group:ops", "/ops", &["admin"]),
        ]);
        let c = caller(a, &["principal:p_1", "group:ops"], None);
        assert!(c.allows(Perm::Write, &ptr("/teamA/score")));
        assert!(c.allows(Perm::Read, &ptr("/teamA")));
        assert!(!c.allows(Perm::Read, &ptr("/teamB")));
        assert!(!c.allows(Perm::Read, &ptr("")), "a subtree grant never covers its ancestors");
        assert!(!c.allows(Perm::Formula, &ptr("/teamA/x")));
        assert!(c.allows(Perm::Formula, &ptr("/ops/deep/x")));
        // Prefix is per token, not per character.
        assert!(!c.allows(Perm::Read, &ptr("/teamAB")));
    }

    #[test]
    fn scope_narrows_never_widens() {
        let a = acl(&[("principal:p_1", "", &["read", "write"])]);
        let scope = Arc::new(vec![(ptr("/public"), PermSet::parse(&["read".into(), "admin".into()]).unwrap())]);
        let c = caller(a, &["principal:p_1"], Some(scope));
        assert!(c.allows(Perm::Read, &ptr("/public/x")));
        assert!(c.allows(Perm::Write, &ptr("/public/x")), "admin in scope admits write the grant has");
        assert!(!c.allows(Perm::Admin, &ptr("/public")), "scope can't add what grants lack");
        assert!(!c.allows(Perm::Read, &ptr("/private")));
    }

    #[test]
    fn permset_within() {
        let rw = PermSet::parse(&["read".into(), "write".into()]).unwrap();
        let r = PermSet::parse(&["read".into()]).unwrap();
        let admin = PermSet::parse(&["admin".into()]).unwrap();
        assert!(r.within(rw));
        assert!(!rw.within(r));
        assert!(rw.within(admin));
        assert!(PermSet::parse(&["bogus".into()]).is_err());
    }
}
