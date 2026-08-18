# TODO

## 1. Isolate document data from metadata at the URL root

Top-level URL paths should be namespaces: `/doc`, `/info`, `/views`, and
room for more.

**Done (2026-08-06):** `/info` is now a sibling of `/doc`, backed by its own
in-memory `Store` — full subscription semantics for free, read-only over
HTTP, and the ticker churn no longer touches doc subscribers or the WAL.
The document root is purely user data.

Still open:
- `/version` probably folds into `/info` (it reports the *doc* store's
  version, so it would need to live in the info store as data, or `/info`
  grows a computed member).
- `/views` namespace arrives with #2.

## 2. Materialized views under `/views`

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

Still open:
- The `/views` namespace itself: a view today is just a formula stored in
  the document; `/views` should mount view definitions outside user data,
  read-only, probably per-connection or named.
- Lifetime: tied to the creating connection, or persistent until deleted?
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
  Still open if REST polling (not library mode) needs the same: a
  reconciling *deserializer* that compares against the old tree while
  parsing the request body.
- ~~Path-copy cloning the 20k-entry parent map dominated single-field
  PUTs (~1.2ms)~~ — done 2026-08-06: `Node::Object` is now `imbl::OrdMap`
  (persistent B-tree: O(log n) path-copy, still sorted iteration).
  Single-field PUT at 20k × 110 fields: ~26µs (`examples/bench_diff.rs`).
- jq is Turing-ish (recursion); a hostile/buggy program can burn CPU inside
  the commit path. Fine single-user; revisit with permissions (#6).

## 3. Type out the SSE `event:` field

SSE frames are currently unnamed (so `EventSource.onmessage` just works)
and the payload carries `"type": "snapshot" | "change"` inside the JSON,
mirroring the WebSocket messages. Later: formalize a taxonomy of named
`event:` types (`snapshot`, `change`, maybe `error`/`resync`) so clients
can `addEventListener` per type, document it, and keep the WS `type` field
and SSE `event:` names in sync.

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

Still open:
- `If-Match` / `If-Unmodified-Since` on writes. The `conditions` machinery
  can express it (compare-and-set on a version) once a path's version is
  addressable as a condition target — today conditions compare *values*.
- The mod index isn't persisted, so a restart from a snapshot resets every
  path to the floor and invalidates client caches once. Persisting it (or
  a digest of it) alongside the snapshot would avoid that.
- `MAX_ENTRIES` (200k) collapse is untested at scale; if a workload hits it
  routinely, consider pruning by age instead of wholesale collapse.
- The embedded UI and SSE/WS paths don't use validators (they don't need
  to — they're live), but the UI's static HTML could carry its own ETag.

## 6. Permissions / users

Eventually needed. No design yet — auth on the HTTP layer, per-path ACLs on
the store, or both. Interacts with #2 (who can create/see a view), #4 (the
UI reflecting the user's rights), and #1 (metadata namespace probably
shouldn't be writable by clients at all).

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

- **WAL fsync runs inline inside the publication mutex** (subscriptions.rs:68
  → persistence.rs:192-201), on a tokio worker, one fsync per commit, no
  batching. Move it to a dedicated writer thread fed by a channel, with
  group commit. Keep the channel send *inside* the mutex so WAL order ==
  version order still holds; only the I/O leaves. `Durability::{Sync,
  Async, NoFsync}`, default `Sync` — concurrent writers share one fsync but
  a commit still returns only once its own record is durable, preserving
  the ack-implies-written guarantee and fail-stop from #8.
- **Snapshots hold the publication lock across whole-document serialize +
  `sync_all` + rename + WAL truncate**, stalling every commit *and every new
  subscriber connection* (`Hub::subscribe` shares the mutex) for a duration
  proportional to document size. Take the `Arc` under the lock, enqueue a
  WAL rotate, serialize outside; needs WAL segmentation (`wal/000001.log`,
  keeping legacy `wal.log` readable) so old segments drop after the rename.
- **Every write handler calls `Store::commit` from `async fn`**
  (api.rs:199-330), and the snapshot ticker from a bare `tokio::spawn` — a
  400ms PUT blocks a tokio worker today, before fsync is even involved.
  `spawn_blocking` is the cheapest latency win available.

Prerequisite for all of it: **we have no write-throughput measurement.**
`examples/bench_diff.rs` is single-threaded and in-memory, so fsync never
runs. Need stats counters (commits, CAS losses, fsyncs vs records, snapshot
micros) and `examples/bench_write.rs` sweeping writers × durability ×
formula count × contention.

## 10. Commit rebase on CAS loss

A CAS loser discards *everything* — path-copy, `reconcile`, `diff_paths`,
formula work, `minimal_ops`, mod index — and redoes it. For a poll-style
20k-object full replace that is ~400ms per lost race, which matters
precisely in the mixed workload (a big periodic sync alongside small
concurrent writes).

Instead of partitioning the tree by "owner" threads: when we lose to
exactly one commit, and our ops are `Put`/`Merge` only, and the winner
didn't touch our subtree, and no formulas are implicated on either side
(#7's `FormulaIndex` queries make that check cheap), splice the
already-built subtree `Arc` onto the new root via `set_at` and reuse the
computed changed paths. Microseconds instead of a full replay. The
"winner didn't touch it" check can be `Arc::ptr_eq` on the node at each of
our scan roots between the base root and the winner's root — structural
sharing means an untouched subtree is literally the same pointer, which is
both free and impossible to over-approximate (idea 2026-08-17).

Flag-gated, off by default until a randomized test proves rebased trees
equal full WAL replay (`nodes_equal`) — WAL replay is an independent
implementation of the same semantics, so it makes a strong oracle. Note the
excluded cases are load-bearing: `Append`/`Insert`/`Remove` derive array
positions from the base root, and a winner that *adds* a formula reaching
into our subtree must invalidate us even though its changed path doesn't
overlap ours.
