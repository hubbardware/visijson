# TODO

## 1. Isolate document data from metadata at the URL root

Top-level URL paths should be namespaces: `/doc`, `/info`, `/views`, and
room for more.

**Done (2026-08-06):** `/info` is now a sibling of `/doc`, backed by its own
in-memory `Store` — full subscription semantics for free, read-only over
HTTP, and the ticker churn no longer touches doc subscribers or the WAL.
The document root is purely user data.

**Done (2026-08-18):** `/version` folded into `/info` — the liveness
ticker now reports the doc store's head as `/info/doc/{version,time_ms}`
(lagging a live commit by at most one tick; the authoritative version for
a write is its commit response, ETag, or subscription event), and the
stray `GET /version` route is gone.

Still open:
- `/views` namespace arrives with #2.

## 2. Materialized views under `/views` — deferred (2026-08-18)

**Decision: do nothing yet.** Views already work today as plain formulas
(`jq(...)` anywhere in the doc, placement by convention), and pre-#6
there is one trusted writer and nothing to protect from anyone. The
`/views` mount (design below, fully settled and kept for that day) is
really an *ownership* feature — shielding consumer overlays from data
owners' full replaces — and ownership should be decided once, alongside
#6. The rules-based alternative ("data writes preserve formulas" /
sticky reconcile) was considered and rejected: it breaks "what you PUT
is what you GET", resurrects formulas producers deleted from their own
schemas (fixable only with ownership metadata — i.e. users again), and
doesn't address cycle geometry, which forbids in-scope placement
regardless. `GET /formulas` — a discovery listing (path + expression)
straight off the `FormulaIndex` — shipped 2026-08-18; the TUI's formula
markers ride it.

A connection should be able to create its own view of the data and read it
under `/views/...` — like a materialized view, with all the existing
subscription semantics (consistent snapshot, then gap-free version-ordered
patches). Views are **read-only** through the API.

**Direction decided (2026-08-06):** the definition language is the formula
language. Query engines are formula functions of the form
`engine(scope, 'program')` — the scope pointer *is* the dependency, so
re-evaluation and cycle detection never inspect the program text.
`jq(/players, '…')` (via jaq) is implemented; `jmes(...)` is reserved so
JMESPath (guaranteed-terminating) can slot in beside it later. Update
timing is *within* the commit, like all formulas. Prefer keyed
(`map_values`) view shapes over arrays — stable keys keep subscription
diffs small.

**Design settled (2026-08-18) — ownership and placement:**
- **Named, persistent until deleted, global. No users, no sessions.**
  Session-tied views fight the architecture: formulas live in the
  committed tree (durable, subscribable, evaluated within the commit),
  while sessions are ephemeral — they'd either churn the WAL on every
  connect/disconnect (SSE auto-reconnect would thrash) or need an
  in-memory overlay evaluated outside the pipeline, forking the formula
  machinery. And HTTP "sessions" mean minting tokens + liveness + GC —
  a half-built identity system; if we build identity, build it once as
  #6. Sharing is also the point: one leaderboard evaluation serving
  every dashboard beats N private copies. Scratch views = unique names
  deleted when done; if leaks ever matter, add TTL leases (expiry
  without identity). Ownership/ACLs (`who defines`, `who reads`, the
  jq-CPU concern below) layer on with #6 as plain metadata + per-path
  ACLs.
- **A view is *mechanically* just a formula** — zero new evaluation
  machinery. `/views` is placement policy, and it earns its existence:
  (a) data writers stomp formulas by design — Put/Merge reconcile whole
  subtrees, so a consumer formula inside producer data dies on the next
  full replace (`formulas_created_and_destroyed_by_deep_subtree_writes`
  pins this); (b) cycle geometry forbids the "natural" spot — a view
  can't live inside its own scope (`jq_view_depending_on_own_scope_is_
  a_cycle`), so views need a region outside the data; (c) `/doc` reads
  and exports stay pure data; (d) the namespace is where the rules
  attach: definition writes shape-checked (must be a formula), values
  read-only, `GET /views` an enumerable registry, one ACL boundary
  later.
- **Write model (2026-08-18):** `/views` accepts *declare* (create-only
  PUT, shape-checked: must be a formula), *delete*, and later *renew*
  (TTL lease) — no in-place modify, no plain data. Redefinition =
  delete + declare; allowing PUT-over-existing later is trivial if
  wanted (it's just a formula write).
- **Dependency semantics — free, by construction:** view→view and doc
  formula→view both already work (formula-on-formula reads the cached
  value, one dependency graph, topo order within the commit, so a data
  write updates chained views atomically); cycles across namespaces are
  rejected by the existing detection (422). Deleting a view that others
  reference yields the standard `#ERR: missing reference` (non-fatal,
  heals if redeclared). Known limit: a view cannot scope the whole
  root (`jq(/, …)` overlaps `$views` itself → cycle) — scope specific
  subtrees.
- **Implementation wrinkle to solve at build time:** formulas resolve
  pointers against the store root and existing doc formulas use paths
  like `/players`, so the doc can't be nested under a key — and for
  view-referencing formulas (`/views/top10`) to resolve without
  rewriting pointers inside expressions, the reserved top-level key
  should literally be named `views` (client paths == tree paths; cost:
  doc data can't use a top-level `views` key). `/doc` routes hide it;
  root-level full replaces are special-cased to preserve it.

Still open:
- jq programs are re-compiled on each evaluation (jaq's compiled `Filter`
  borrows its `Arena`/`Loader`, so caching it is a lifetime puzzle). The
  formula-AST half of this note was fixed by #7; jq compile caching
  remains.
- ~~Get under the ~300ms full-replace floor~~ — done 2026-08-06, all three
  rungs: (1) `Node` deserializes straight from JSON (no `Value`) and ops
  carry `Arc<Node>`; (2) `typed::to_node` + `Store::put_typed` serialize
  any `T: Serialize` directly into trees; (3) `#[derive(Document)]` +
  `TypedMount` retain the last synced struct and diff natively — unchanged
  20k × 110 sync ≈ 44ms and commits nothing; 3-field drift ≈ 35ms + 3 ops.
  The REST-polling half — a reconciling *deserializer* comparing against
  the old tree while parsing the request body — done 2026-08-18
  (`model::node_from_slice_reconciling`, used by the PUT handlers with
  the current subtree as hint): an almost-unchanged poll body arrives
  pre-shared, collapsing the commit-side reconcile+diff from ~218ms to
  ~15ms on the 20k×110 shape (parse pays ~+90ms for hint lookups; net
  ~1.4× total, and the contended portion inside the CAS loop shrinks
  ~14×, which also makes lost races and rebases far cheaper for big
  replaces).
- ~~Path-copy cloning the 20k-entry parent map dominated single-field
  PUTs (~1.2ms)~~ — done 2026-08-06: `Node::Object` is now `imbl::OrdMap`
  (persistent B-tree: O(log n) path-copy, still sorted iteration).
  Single-field PUT at 20k × 110 fields: ~26µs (`examples/bench_diff.rs`).
- jq is Turing-ish (recursion); a hostile/buggy program can burn CPU inside
  the commit path. Fine single-user; revisit with permissions (#6).

## 3. Type out the SSE `event:` field

**Done (2026-08-18):** SSE frames are named — `event: snapshot` /
`event: change` — so clients `addEventListener` per type instead of
switching on the payload's `type` field (which stays, for parity with the
WebSocket messages; the two must grow new names in lockstep). Resyncs are
deliberately `snapshot`, not a separate name: a client listening for
snapshots must never miss one, and the payload's `"resync": true` flag
distinguishes them. Breaking change for `onmessage` consumers (named
events don't fire it) — accepted, pre-users.

**`Last-Event-ID` resume (rungs 1+3) done (2026-08-18):** an SSE
reconnect carries the last `id:` the client saw — a commit version V. If
the modification index proves the subscribed subtree is untouched since V
(`last_modified(ptr).version <= V`), the stream resumes with **no
snapshot at all** — zero retention, and it kills the reconnect snapshot
herd for quiet subtrees. Otherwise the reconnect gets a fresh snapshot
marked `"resync": true` (a fresh connect's snapshot stays unmarked).
Over-estimation only ever downgrades a resume to a resync.

Still open, purely an optimization (add only if reconnect-under-heavy-
writes shows up as a measured cost):
- **Rung 2 — coalesced catch-up.** Retain recent roots (small
  `VecDeque<(version, Arc<Committed>)>` in the Hub, trimmed by count/age
  — near-zero extra memory, the broadcast ring already pins the last
  ~1024 `Arc<Committed>`s) and, when root@V is retained, send one
  coalesced `change` patch diffing root@V vs current at ptr instead of a
  resync snapshot. Watch the retention bound: a big replace pins its
  displaced subtree until it ages out.

## 4. UI: grey out edit controls where the user can't write

The embedded UI shows edit/delete affordances everywhere, including
read-only namespaces — on `/info` clicking one just earns a 405. Grey them
out (or hide them) wherever the current user lacks write permission:
`/info` today, and per-path once users/permissions exist (#6). The server
needs to tell the page its rights — e.g. capability info in the snapshot
message or an OPTIONS/Allow probe — rather than the UI hardcoding
namespace names.

## 5. Conditional requests: ETags / If-Modified-Since

**Done (2026-08-08):** GETs carry `ETag` (last-modified version for that
subtree), `Last-Modified`, and `Cache-Control: no-cache`; `If-None-Match`
(weak comparison, `*`) and `If-Modified-Since` answer 304. Commits are
timestamped (clamped monotonic) and the timestamps persist in WAL records
and snapshots. `meta::ModIndex` answers per-path last-modified in
O(depth) via `own`/`sub` stamps, is carried in `Committed` as a persistent
map, and always over-estimates so a stale 304 is impossible.

**Done (2026-08-18), the write half:**
- `If-Match` / `If-Unmodified-Since` on all write verbs (PUT/POST/DELETE/
  PATCH, both patch flavors): `Condition` grew a validator flavor
  (`unmodified_version` / `unmodified_ms`, also reachable via `/txn`)
  checked against the mod index inside the commit loop — so it holds at
  commit time, including across a rebase — failing with 412
  (`StoreError::PreconditionFailed`). The ETag is the version, `*` and
  lists per RFC 9110 (weakly). Over-estimation stays safe: a validator can
  spuriously fail (refetch + retry), never spuriously pass.
- The mod index now rides the snapshot (`ModIndex::dump`/`from_dump`),
  so a restart keeps exact per-path validators and client ETags stay
  valid; pre-dump snapshots still fall back to the floor. Bounded by
  `MAX_ENTRIES`.
- The collapse path has a unit test (`collapse_keeps_answers_safe_never_
  stale`) pinning the over-estimate invariant.

Still open (cosmetic / revisit-if-hit):
- If a workload hits the `MAX_ENTRIES` collapse routinely, consider
  pruning by age instead of wholesale collapse.
- ~~The embedded UI's static HTML could carry its own ETag~~ — done
  2026-08-18: content-hash ETag + `no-cache`, revalidations answer 304.

## 6. Permissions / users

**Design settled (2026-11-05).** Single tenant — multi-tenancy (orgs with
fully separate data) is explicitly out of scope; one tree, path grants.

**Identity model: a principal with many credentials.** Authorization only
ever sees the principal; how it authenticated is a detail of the request.
- `Principal { id, kind: human | service, display, status: active |
  disabled, groups }`. Service accounts are principals with only keys/mTLS.
- Credential kinds, many per principal: `password` (username + argon2id),
  `api_key` (`vjk_<id>_<secret>`, sha256-hashed, shown once, optional
  scope + expiry), `oidc` (issuer, subject — Google, any OIDC provider),
  `oauth` (non-OIDC providers like Facebook via an adapter), `passkey`
  (WebAuthn), `mtls` (CA + SAN, or SPKI fingerprint).
- **Key external identities on `(issuer, subject)`, never on email** —
  auto-merging by email is the classic account-takeover hole. Linking a new
  credential only happens from an already-authenticated session; unlinking
  must leave at least one credential.
- **We mint our own sessions**: provider tokens are used once at login and
  discarded. Sessions are opaque, server-side, revocable (`vjs_…`);
  browsers get an HttpOnly SameSite=Strict cookie, programs a Bearer token.
- API keys can only narrow their principal's rights (scope ∩ grants),
  never widen them.

**Request pipeline.** An axum middleware resolves whichever credential the
request carries (Bearer session/key, cookie, single-use `?ticket=`) into a
`Caller` placed in request extensions; no credential = the built-in
`anonymous` principal. Handlers never know the method. Browser specifics:
`EventSource` can't set headers → cookie, or a 30s single-use ticket from
`POST /auth/ticket`; cookie-authenticated WebSocket upgrades must pass an
`Origin` check (CSWSH); cookie-authenticated writes require the
`X-VisiJson-Request` header (cross-origin pages can't set it without a CORS
preflight we never grant). Anonymous denial is 401, authenticated denial
403.

**Storage: a third `Store` (`sys`), never routed directly.** Principals,
credential lookups, sessions, tickets, grants. Durability, CAS, uniqueness
(conditions on lookup keys) for free. Persisted under `DATA_DIR/sys`.
Secrets stored hashed only.

**Authorization: per-path grants.** `(principal | group) × path × {read,
write, formula, admin}`; a grant covers its whole subtree. Pseudo-groups
`anonymous` (everyone) and `authenticated` (anyone logged in). The ACL view
is rebuilt from the sys store lazily, keyed by sys version.
1. **Writes authorize on the effect.** The store's `WriteGuard` hook runs
   inside the commit loop after the diff, before the CAS (so it holds
   across rebase), and checks `write` on every *changed path* — not the
   request URL. A poll-style full PUT at `/` that only changes
   `/teamA/score` passes for a holder of write on `/teamA`. Diff collapse
   (`MAX_DIFF_PATHS`) only coarsens paths → spurious *deny*, never allow.
   Formula re-evaluation changes are exempt. **Oracle guard:** an op whose
   target the caller can't write must at least be readable — otherwise
   accept/deny of a guessed full replace would leak values. Elided
   (no-change) commits are covered by the same op-level check.
2. **Formulas run with the definer's rights.** A computed value is
   evaluated once and shared, so it cannot depend on the reader. Creating
   or changing a formula needs `formula` on its path and `read` on every
   dependency (the jq scope pointer included). This is what makes views
   (#2) useful: publish `/leaderboard`, hide `/raw_scores`. Phase 4 adds
   recorded ownership so a definer losing access turns the formula `#ERR`.
3. **No holes (v1).** Reading P requires a grant on P or an ancestor —
   no per-subscriber redaction, so the shared event stream survives.
   `GET /auth/whoami` lists your grants so clients can navigate.
4. **Side channels:** `/txn` conditions and `If-Match` need read (they
   reveal values/timing); `GET /formulas` is filtered by read; ETags and
   versions leak activity timing (accepted, documented).
5. `formula` is separate from `write`: jq burns CPU inside the commit.
6. **Revocation of live streams:** WS/SSE tasks watch the sys store and
   re-validate their caller on every sys commit and at session expiry,
   closing the stream when the credential or the grant is gone.

**Also:** every WAL record carries `actor` (audit trail; later a per-path
last-writer beside `ModIndex`). Bootstrap: with auth on and no principals,
startup mints a one-time admin API key and logs it. `AUTH=off` (default)
keeps today's behavior — everyone unrestricted, zero cost. The library
`Store` API stays unauthenticated; authorization is opt-in via
`Store::commit_with`. Deleting a user removes their credentials and
personal data. Later: per-principal quotas (write rate, doc size, jq CPU);
federation (`remote()` formulas) authenticates server-to-server as
service principals over mTLS / API keys.

**Phases 1–2 done (2026-11-05):** `src/auth/` (`mod.rs` sys-store
identity ops, `acl.rs` perms/grants/`Caller`/`WriteGuard`, `http.rs`
middleware + `/auth/*`), `Store::commit_with` + `WriteGuard`/`WriteEffect`
hook, `actor` in WAL records, UI sign-in dialog + CSRF header, TUI
`--token`. Pinned by `tests/auth.rs`. Known gaps, small:
- API-key `last_used` isn't tracked (a sys write per request would churn
  the WAL; revisit with a coarse, batched stamp).
- Every sys commit (incl. each login) re-validates every live stream —
  cheap reads, but O(streams); index streams by principal if it shows.
- The TUI always subscribes at the root, so a user with only subtree
  grants gets 403 — it needs a start-path argument.
- No CLI for admin recovery if the bootstrap key is lost (today: stop the
  server, delete `DATA_DIR/sys`, restart → fresh bootstrap — which also
  drops all users).

**Phasing:**
1. Principals, passwords, sessions, API keys, tickets, the `sys` store, the
   middleware, `AUTH` flag, bootstrap key, `actor` in the WAL.
2. Path grants, effect-based write checks, read checks on every surface,
   subscription revocation.
3. OIDC (Google, generic), Facebook adapter, passkeys, account linking.
4. Formula ownership + definer-rights revocation, `/views` (#2), UI/TUI
   greyout (#4), mTLS.

---

Items 7–10 came out of a 2026-08-17 trace of the concurrency, durability,
and formula paths (prompted by comparing notes with an external
"SuperJsonCalc" design aimed at write throughput). 7 and 8 are defects, not
optimizations. Full assessment:
`~/.claude/plans/i-worked-with-another-melodic-comet.md`.

## 7. Formula evaluation is O(F²) per commit attempt

**Done (2026-08-17):** `Committed.formulas` is now `formulas::FormulaIndex`
— two `imbl::OrdMap`s (formula path → `Arc<FormulaInfo>` with the parsed
AST and deduped deps; dep pointer → dependent formula paths). Overlap
queries are `depth` ancestor point-lookups + one `OrdMap::range` descendant
scan (token-vector `Ord` keeps descendants contiguous). `reevaluate` seeds
the dirty set from changed paths, BFSes the closure over reverse edges,
builds the graph over the dirty subgraph only, and topo-sorts with
iterative Kahn (no recursion). Expressions parse once, in `update_index`,
when a formula enters or changes — identical re-PUTs reuse the old
`Arc<FormulaInfo>`. Bench (20k×110 doc + 1k formulas): unrelated
single-field PUT 3.25ms → 25µs; `reevaluate` with 0 dirty 2.6ms → 1.9µs.

Documented narrowings (both only reachable via a foreign snapshot — live
commits reject cycles and parse errors at creation): a *pre-existing*
cycle or unparseable formula in an untouched region no longer rejects
unrelated commits (`scan_index` skips unparseable formulas with a warning
at recovery). Newly formed cycles are still rejected — any new cycle
includes a formula this commit wrote, so it is always seeded. Pinned by
`preexisting_cycle_only_rejects_commits_that_touch_it` (unit) and
`foreign_snapshot_cycle_only_rejects_commits_that_touch_it` (integration).

## 8. Durability defects

**Done (2026-08-18):**
- `Hub::finalize` now returns `Result` and blocks (condvar) until the
  caller's own record has been flushed — by whichever thread drained it —
  so a successful `commit()` always means the record is written (and
  fsynced, when fsync is on). Pinned by
  `commit_ack_implies_wal_record_on_disk` (8 threads × 25 commits, WAL
  re-read from disk the instant each ack returns) and Hub unit tests.
- A failed WAL append **poisons the hub**: the failing commit and every
  waiter behind the hole get an error, nothing past the hole is broadcast,
  and all subsequent writes fail fast with `WalBroken` (HTTP 503) —
  checked before the CAS, so a commit that could never recover never
  enters memory. Reads still serve; restart recovers to the last durable
  record.
- `Store::commit` now uses the same backoff ladder as `transaction`
  (shared `backoff()`) and caps at `max_retries` → `RetryLimit`.

Still open (moved under #9): the fsync itself is still inline, per commit,
inside the publication mutex — group commit / a dedicated writer thread is
the throughput half.

## 9. Write throughput: get I/O off the critical path

**Done (2026-08-18)**, in three commits:
- `Store::stats()` counters + `examples/bench_write.rs` (writers ×
  durability × formula workload; reports CAS losses and records/fsync).
- Dedicated WAL writer thread with group commit: records enqueue under
  the publication mutex (channel order == version order), the writer
  batches everything queued during the previous write into one fsync, and
  broadcasts via `Hub::publish_batch` only after a batch reaches its
  durability level. `Durability::{Sync, Async, NoFsync}` (default Sync;
  `DURABILITY` env var); ack-implies-durable and #8's fail-stop preserved.
  Snapshots pin the enqueued `Committed` + enqueue a segment rotation
  under the lock, serialize/rename outside it, prune sealed segments
  after; WAL is now `wal/NNNNNN.log` segments, legacy `wal.log` replayed
  first and retired by the next snapshot.
- `spawn_blocking` around all eight write handlers and the snapshot tick.

Measured (8 writers): Sync 270 → 1041 ops/s (4 rec/fsync; p50 30ms →
7.7ms), Async 2080 ops/s (8 rec/fsync). Known costs / future tuning:
- NoFsync pays ~5µs/commit for the writer-thread handoff (single-writer
  151k → 85k ops/s). Could special-case an inline write path if it matters.
- Single-writer Async is still fsync-bound (~260 ops/s): serial commits
  queue behind the previous batch's fsync inside the one writer thread.
  A two-stage pipeline (fsync on its own thread) would fix it.
- Batches form from wake-up stragglers (4 rec/fsync at 8 writers, not 8);
  a tiny gather window before writing would trade p50 for throughput.

## 10. Commit rebase on CAS loss

**Done (2026-08-18)** via `StoreConfig.rebase_on_conflict`, default **on**
since 2026-08-18 (`REBASE=0` env disables) — the starvation evidence below
settled the default; the whole test suite now runs with it enabled. When a Put/Merge-only
commit loses the CAS, `Store::try_rebase` re-checks conditions against the
winner's head and takes the fast path iff every scan-root subtree is
`Arc::ptr_eq` between our base and the head (structural sharing makes
"untouched" a free pointer comparison), our prepared formula index is
ptr-identical to the base's (we touched no formulas, `FormulaIndex::
same_as`), and no head formula is `any_affected_by` a scan root (covers a
winner *adding* a formula reaching into our subtree). Then it splices the
already-built subtree `Arc`s onto the head via `set_at` and reuses the
computed changed paths and minimal WAL ops. Excluded cases fall back to
full replay silently; `Append`/`Insert`/`Remove` are excluded up front
(array positions derive from the base root). Transactions never rebase
(their read-set was validated against the base).

Proven by `rebased_commits_equal_wal_replay`: 4 threads × 200 randomized
Put/Merge (disjoint + contended + formula-covered regions) with rebase
on, then WAL replay into a fresh store must be `nodes_equal` with the
live head — replay is an independent implementation of the same
semantics. Stress-ran 10× in release.

**Why it defaults on** (bench_write `mixed` section): one poll-style
2000×20 replacer racing 4 small writers
— with rebase off the replacer **starves outright** (20/20 replaces
exhaust the 64-retry cap; 10.5k CAS losses, zero successes; before #8's
cap this spun forever), with rebase on it's 20/20 at ~7.8ms each.
`Store::stats().rebases` tracks recoveries.

## 11. TUI client alongside the embedded web UI

**v1 done (2026-08-18):** `tui/` (`visijson-tui`), HTTP-only ratatui
client. Tree browser with fold/unfold and `ƒ` formula markers (from a
raw read per snapshot), detail pane, live updates over SSE (named
events, RFC 6902 patches applied to a local mirror, auto-reconnect with
`Last-Event-ID` resume), and editing: `e` edits values or formulas
(`=expr` declares a formula, bare words become strings), `a` adds
object keys / appends to arrays, `d` deletes — every edit and delete is
`If-Match`-guarded off a fresh raw read, so concurrent changes surface
as a 412 conflict message instead of a lost update. `--check` gives a
headless connect-and-print smoke mode. Verified end-to-end through a
sized pty: rendering, formula markers, and mid-session SSE updates
re-rendering recomputed values.

Remaining polish (small, as wanted):
- ~~Formula markers can go stale between snapshots~~ — improved
  2026-08-18: markers now come from `GET /formulas` (tiny, off the
  index, no raw-tree fetch) and refresh on change events with a 1s
  debounce; residual staleness is at most that debounce.
- Capability-driven greyout arrives with #4/#6, same as the web UI.
- Array element insert-at-index (only append today); copy/yank.

Original sketch:
A terminal UI (ratatui or similar) speaking the same HTTP API as the
embedded web editor (`src/ui.html`): tree browser with expand/collapse,
inline value editing, spreadsheet-style formula editing (mirroring the
web UI's `$formula` affordances), and live updates by subscribing to
document paths.

Design notes:
- **Live updates come free**: subscribe via SSE (`Accept:
  text/event-stream`) or WebSocket on the document URL — same
  snapshot-then-gap-free-patches contract the web UI uses. RFC 6902
  patches (or merge-patch via the `merge-patch` subprotocol /
  `patch=merge-patch` Accept parameter) apply to a local mirror of the
  subtree.
- **Conditional writes**: use ETags/`If-None-Match` for cheap refresh;
  once `If-Match` on writes lands (#5's open half), the TUI should use it
  for edit conflict detection.
- Probably a separate binary (`src/bin/` or a workspace crate, e.g.
  `visijson-tui`) so the server binary stays dependency-lean —
  ratatui/crossterm shouldn't bloat the store.
- Interacts with #4/#6: like the web UI, it should grey out or hide edit
  affordances where the user can't write (read-only `/info`, per-path
  ACLs later), driven by server-reported capabilities rather than the
  client hardcoding namespace names.
- Patch application against the local mirror is new client-side code
  (the store's `typed` module is the in-process fast lane, not a wire
  client); if a reusable client-side helper emerges, consider sharing it
  with future client work rather than burying it in the TUI.

**Decided (2026-08-18): HTTP only** — it works against any running
server and keeps the client story honest. (In-process data-dir opening
for offline snapshot/WAL inspection was considered and dropped; if that
need arises it's a separate inspection tool, and it would have to respect
the `LOCK` file semantics — a second process on the same dir fails to
start by design.)
