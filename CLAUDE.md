# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```sh
cargo build
cargo test                        # full suite (unit + integration)
cargo test --test api             # one integration test file (tests/api.rs)
cargo test formula_cycle          # tests matching a name substring
cargo run                         # serve on 127.0.0.1:8080, data in ./data
DATA_DIR=none cargo run           # pure in-memory (no WAL/snapshots)
ADDR=0.0.0.0:9000 SNAPSHOT_SECS=10 DATA_DIR=/path cargo run
```

Env vars (read in `src/main.rs`): `DATA_DIR` (`none` disables persistence), `ADDR`, `SNAPSHOT_SECS` (0 disables periodic snapshots), `INFO_TICK_SECS` (0 disables the `/info` liveness ticker), `RUST_LOG`.

Top-level URL paths are namespaces, each its own `Store`: `/doc` is user data; `/info` is read-only liveness metadata on a dedicated in-memory store (so its once-a-second churn never reaches doc subscribers or the WAL). `api::router(doc, info)` takes both.

The data dir holds an exclusive advisory `LOCK` file — a second process on the same dir fails to start. If tests or manual runs behave oddly (e.g. data "disappearing"), check for a stray `cargo run` server still holding `./data`.

## Architecture

In-memory JSON document store: an immutable persistent tree of `Arc<Node>` with lock-free reads and CAS-committed writes. The core invariant everything hangs on: **every commit produces a whole new root by path-copy** (untouched subtrees shared by pointer), and `ArcSwap<Committed>` holds the current `{version, root, formulas}`. Objects are `imbl::OrdMap` (persistent sorted map): O(1) clone, O(log n) path-copy, and **sorted iteration — the lockstep merge-walk diffs in `model.rs` depend on that ordering**, so don't swap in a hash map.

### The commit pipeline (spans store.rs → formulas.rs → subscriptions.rs → persistence.rs)

1. `Store::commit` loads the current root, checks conditions, and calls `apply_ops` (pure — also reused verbatim by WAL replay in `persistence.rs`). It computes precise changed paths via structural diff (`model::diff_paths`, collapsing past `MAX_DIFF_PATHS`; array insert/remove marks the whole array because siblings shift). Put/Merge **reconcile** the incoming subtree against what it replaces (`model::reconcile`), restoring structural sharing for unchanged entries. A commit whose diff is empty is **elided** — no version bump, no WAL, no event. The WAL records **minimal derived ops** (`store::minimal_ops` — the effect, not the request), so poll-style full replaces write only what changed.
2. `formulas::update_index` maintains the formula-location index carried in `Committed` (entries overlapping a written subtree are dropped and that subtree rescanned — never the whole tree; a full `scan_index` runs only at open/recovery). `formulas::reevaluate` then re-runs indexed formulas whose dependencies overlap the changed paths, in topological order; a dependency cycle rejects the whole commit. Formula-free commits pay ~nothing here.
3. CAS the root pointer. Losers replay from step 1. Transaction commits (`Store::transaction`) additionally validate the recorded read-set before replaying and surface `Conflict` to retry the closure (with backoff).
4. `Hub::finalize` (subscriptions.rs) restores order after racy CASes: commits buffer by version and flush strictly sequentially under a mutex — the sink appends to the WAL, then the event broadcasts. So **WAL order == broadcast order == version order**, and `Hub::subscribe` returns a published snapshot plus a receiver guaranteed gap-free events after it. This is the consistency guarantee WebSocket subscribers rely on.
5. Snapshots (`Store::snapshot`) run inside `Hub::with_publish_lock`, freezing WAL appends so the snapshot version is an exact WAL rotation point. Recovery = snapshot + replay of newer WAL records, tolerating a torn final line and version gaps.

### Other cross-cutting points

- **Two value representations** (`model.rs`): `to_plain_value` resolves formulas to their computed values (what GET returns); `to_raw_value` keeps `{"$formula", "$value"}` objects (what `?raw=true`, snapshots, and the WAL use). Writing an object containing `"$formula"` creates a formula node.
- **`api.rs`** maps REST verbs onto store `Op`s: PATCH content-negotiates RFC 6902 JSON Patch (array body) vs RFC 7396 Merge Patch (object body); `/txn` takes ops + conditions (409 on conflict). GET on a document path is fully negotiated: a WebSocket upgrade subscribes to that path, `Accept: text/event-stream` subscribes over SSE, an Accept preferring text/html gets the embedded single-file UI (`src/ui.html` via `include_str!` — no build step, edit it directly), everything else gets JSON. There is no separate `/ws` or events endpoint.
- **Subscription events** (WebSocket and SSE share the same encoding helpers in `api.rs`) are computed per-subscriber by diffing consecutive committed roots at the subscription path, as RFC 6902 patches by default or RFC 7396 merge patches on request — via the `merge-patch` WebSocket subprotocol (`Sec-WebSocket-Protocol`) or the SSE Accept parameter `text/event-stream;patch=merge-patch`. SSE uses commit versions as event ids.
- **HTTP validators**: `Committed` carries `time_ms` and a `meta::ModIndex` (persistent `OrdMap`, updated per commit alongside the formula index). `ModIndex` stores `own`/`sub` stamps per path so `last_modified(P)` = one lookup + one per depth level, no descendant scan. It is never persisted — recovery seeds a *floor* from the snapshot and replay sharpens it. **The invariant: answers may be newer than the truth, never older** (a stale 304 would be a correctness bug; a needless refetch is just cost). Commit timestamps are clamped monotonic and stored in WAL records and snapshots.
- **Formula evaluation errors** (missing ref, divide by zero, bad jq program) become `"#ERR: ..."` values and heal on the next relevant commit; only cycles reject a commit (HTTP 422).
- **Query engines are formula functions**: `jq(/scope, 'program')` (via the jaq crates) evaluates a jq program against the subtree at `/scope`; the scope pointer is the dependency, so dirtiness/cycles never inspect program text. Programs must yield exactly one value. `jmes(...)` is reserved for a future JMESPath engine — add engines as new function names in `formulas.rs::eval`, never as new node types.

The repo is a two-crate workspace: the store, plus `derive/` (`jsondocstore-derive`, the `#[derive(Document)]` proc macro re-exported via `typed`). `Op` values are `Arc<Node>` (serde parses request/WAL JSON straight into trees — `Node` has hand-written `Serialize`/`Deserialize`, raw form; `model::PlainView` streams the formula-resolved form). The `typed` module is the library fast lane: `to_node` (any `Serialize` → tree, no `Value`), and `Document`/`TypedMount` (retained-copy native struct diffing → minimal ops). `typed::DiffPath` is a borrow-based token stack — like `model::walk`, paths materialize only at changes; keep it that way, per-node `Pointer` allocation was a measured 7× regression.

Planned directions (URL namespaces, materialized views, permissions) are in `TODO.md` — check it before proposing structural changes.

Integration tests in `tests/` lean on `Store::in_memory()` and `shares_subtree` (structural-sharing assertions); persistence tests use `tempfile`. The README's "Consistency model" section is the authoritative prose description of the ordering guarantees — keep it in sync if you change the pipeline.
