# VisiJson

*A spreadsheet for JSON — cells are pointers, formulas recalculate on commit,
and everyone watching sees the change.*

An in-memory JSON document store built on **immutable persistent tree nodes**
(`Arc<Node>`) with **atomic root swaps** (`ArcSwap<Committed>`). Every write is
a path-copy that shares all untouched subtrees with the previous version, and
commits land via a CAS on the root pointer — readers are lock-free and always
see a fully consistent snapshot.

Features:

- **JSON pointer addressing** (RFC 6901, including `~0`/`~1` escapes and `-`
  array append)
- **REST API** — GET / PUT / POST (append) / DELETE, PATCH speaking both
  RFC 6902 JSON Patch (`add`/`replace`/`remove`/`test`) and RFC 7396 JSON
  Merge Patch, plus a `/txn` batch endpoint
- **Optimistic transactions** — read-set validation with automatic retry
  (Rust closure API) and conditional commits (`test` ops / `conditions`) over
  REST
- **WebSocket subscriptions by path** — upgrade the document URL itself
  (`ws://…/doc/users/alice`); no separate endpoint. A consistent initial
  snapshot is followed by gap-free, strictly version-ordered **standard
  patch** events: RFC 6902 JSON Patch by default (paths relative to the
  subscription root, directly applicable with any JSON Patch library), or
  RFC 7396 JSON Merge Patch via the `merge-patch` WebSocket subprotocol
  (`Sec-WebSocket-Protocol`). Only changed paths are shipped, never untouched
  siblings; every commit records its precisely-diffed changed paths
- **Server-Sent Events** — the same subscription over plain HTTP: request any
  document URL with `Accept: text/event-stream` (exactly what the browser
  `EventSource` API sends). Identical snapshot/change payloads, commit
  versions as SSE event ids, automatic resync on lag; merge-patch format via
  an Accept parameter (`text/event-stream;patch=merge-patch`)
- **Formula nodes** — spreadsheet-style expressions over pointer references
  with dependency tracking, topological re-evaluation, and cycle detection
  (cycles reject the commit); embedded **jq** (`jq(/scope, 'program')` via
  jaq) for projections and reshaping — extract `[.[] | .name]` slices or
  `map_values({name, age})` keyed views of a subtree
- **HTTP caching** — every read carries an `ETag` (the version of the last
  commit that touched *that subtree*) and `Last-Modified`; `If-None-Match`
  and `If-Modified-Since` answer `304`. A write elsewhere in the document
  does not invalidate your path
- **Durability** — append-only WAL (one JSON line per commit) + periodic
  snapshots with atomic rotation; recovery on startup replays snapshot + WAL,
  tolerating a torn WAL tail

## Layout

| Module | Responsibility |
|---|---|
| `model` | Immutable `Node` tree, `Pointer`, path-copy `get/set/insert/remove` |
| `store` | `ArcSwap` root, CAS commits, ops, conditions, optimistic transactions |
| `api` | axum REST + WebSocket handlers, embedded UI, `/info` ticker |
| `subscriptions` | Ordered event publication (`Hub`), path relevance |
| `formulas` | Expression parser/evaluator, dependency graph, topo re-eval |
| `persistence` | WAL append/replay, snapshot write/rotate, recovery |

## Live web UI

Every document URL is fully content-negotiated: a WebSocket upgrade
subscribes, `Accept: text/event-stream` subscribes over SSE, an `Accept`
preferring `text/html` (a browser) gets an embedded single-file UI, and
everything else gets JSON (`?raw=true` forces JSON from the address bar).
So opening `http://localhost:8080/doc` or `/doc/any/sub/path` in a browser
serves the UI, which then subscribes to the same URL over WebSocket and:

- renders the value as a navigable JSON tree (keys link deeper, breadcrumbs
  link up), live-updating with per-row flashes on every relevant commit
- lets you click any scalar to edit it in place (PUT to that pointer), or
  edit the whole subtree as raw JSON — formulas round-trip intact
- shows connection status, current version, and a change-event log
- reconnects automatically with backoff, resyncing via a fresh snapshot

The server also maintains liveness metadata at `/info` — a **sibling** of
`/doc`, backed by its own in-memory store — wall-clock time (RFC 3339),
epoch millis, uptime, and start time, rewritten once per second
(`INFO_TICK_SECS`, `0` disables). It gets the same content negotiation:
open `/info` in a browser for a live heartbeat, `GET /info/time` as a
liveness probe, or subscribe to `ws://…/info`. It is read-only over HTTP,
and because it is a separate store, document subscribers never see the
ticker churn and it never touches the WAL — leverage it if you want it,
ignore it if you don't.

## Running

```sh
cargo run                       # data in ./data, listens on 127.0.0.1:8080
DATA_DIR=none cargo run         # pure in-memory
ADDR=0.0.0.0:9000 SNAPSHOT_SECS=10 DATA_DIR=/var/lib/jds cargo run
```

## API tour

```sh
# Write and read
curl -X PUT localhost:8080/doc/users/alice -d '{"age": 30}' -H 'content-type: application/json'
curl localhost:8080/doc/users/alice/age            # → 30

# Append to an array (created on first append)
curl -X POST localhost:8080/doc/log -d '"started"' -H 'content-type: application/json'

# RFC 6902 JSON Patch with a guard (array body, or content-type
# application/json-patch+json)
curl -X PATCH localhost:8080/doc -H 'content-type: application/json' -d '[
  {"op": "test", "path": "/users/alice/age", "value": 30},
  {"op": "replace", "path": "/users/alice/age", "value": 31}
]'

# RFC 7396 JSON Merge Patch (object body, or content-type
# application/merge-patch+json); null deletes a member
curl -X PATCH localhost:8080/doc/users -H 'content-type: application/merge-patch+json' \
  -d '{"alice": {"age": 32}, "bob": null}'

# Conditional transaction (409 on conflict — retry client-side)
curl -X POST localhost:8080/txn -H 'content-type: application/json' -d '{
  "ops": [{"op": "put", "path": "/bal", "value": 70}],
  "conditions": [{"path": "/bal", "expected": 100}]
}'

# Formulas: write an object with "$formula"; reads return the computed value
curl -X PUT localhost:8080/doc/total -d '{"$formula": "sum(/cart/prices) * 1.08"}' -H 'content-type: application/json'
curl localhost:8080/doc/total                      # computed number
curl 'localhost:8080/doc/total?raw=true'           # {"$formula": ..., "$value": ...}

# Subscribe by upgrading the document URL itself: snapshot first, then
# RFC 6902 patches relative to the subscription root — pipe them into any
# JSON Patch library as-is
websocat ws://localhost:8080/doc/users/alice
# → {"type":"snapshot","version":3,"path":"/users/alice","exists":true,"value":{...}}
# (subscribing to a path that doesn't exist yet is fine: the snapshot says
#  "exists":false, and the path's creation arrives as an add event)
# → {"type":"change","version":4,"patch":[{"op":"replace","path":"/age","value":31}]}
# → {"type":"change","version":5,"patch":[{"op":"remove","path":"/nick"}]}

# Or RFC 7396 merge patches (arrays replace wholesale; null means delete) —
# the format is a WebSocket subprotocol, e.g. new WebSocket(url, "merge-patch")
websocat --protocol merge-patch ws://localhost:8080/doc/users/alice
# → {"type":"change","version":6,"merge":{"age":33,"nick":null}}

# Same subscription over SSE — new EventSource("/doc/users/alice") in a
# browser, or curl. Commit versions double as SSE event ids.
curl -N -H 'accept: text/event-stream' localhost:8080/doc/users/alice
# → id: 3
# → data: {"type":"snapshot","version":3,"path":"/users/alice","exists":true,"value":{...}}
# →
# → id: 4
# → data: {"type":"change","version":4,"patch":[...]}

# Merge-patch format rides an Accept parameter (EventSource can't set custom
# headers, so it always gets the json-patch default)
curl -N -H 'accept: text/event-stream;patch=merge-patch' localhost:8080/doc/users/alice
```

Formula grammar: `+ - * /`, parentheses, unary minus, numeric and string
(`'...'` / `"..."`) literals, pointer references (`/a/b`), and
`sum` / `count` / `min` / `max` (scalars and arrays). A bare pointer
expression mirrors any JSON value. Evaluation errors (missing ref,
divide-by-zero) surface as `"#ERR: ..."` values and heal when the dependency
is fixed; dependency cycles reject the writing commit (HTTP 422).

Query engines are formula functions of the form `engine(scope, 'program')`.
`jq(...)` runs a jq program (via [jaq]) against the plain value of the
subtree at `scope`; the scope pointer doubles as the formula's dependency,
so it re-evaluates exactly when that subtree changes. The program must
produce a single value — wrap streams in `[...]`. More engines can slot in
as sibling functions later (`jmes(...)` is reserved for JMESPath).

[jaq]: https://github.com/01mf02/jaq

```sh
# All player names, as a bare array
curl -X PUT localhost:8080/doc/names -H 'content-type: application/json' \
  -d "{\"\$formula\": \"jq(/players, '[.[] | .name]')\"}"

# A keyed projection — uid → {name, level}. Prefer this shape for live
# views: object keys are stable, so subscription diffs stay tiny (a name
# change is `replace /u7/name`, never an array reshuffle).
curl -X PUT localhost:8080/doc/roster -H 'content-type: application/json' \
  -d "{\"\$formula\": \"jq(/players, 'map_values({name, level})')\"}"

# Engines compose with the numeric functions
#   sum(jq(/players, '[.[] | .age]')) / count(jq(/players, '[.[] | .age]'))
```

## Using it as a library

The common types re-export at the crate root:

```rust
use visijson::{Store, StoreConfig, Op, Condition, Document, TypedMount};
```

**Open a store** — `Store::in_memory()` for no persistence, or:

```rust
let store = Store::open(StoreConfig {
    data_dir: Some("/var/lib/jds".into()),  // None disables persistence
    fsync: true,                            // per-commit WAL fsync
    max_retries: 64,                        // transaction retry budget
})?;                                        // recovers snapshot + WAL
store.snapshot()?;                          // write a snapshot, rotate the WAL
```

**Read** — lock-free against the current root:

```rust
store.get("/players/u1/age")?;   // Option<Value>, formulas resolved
store.get_raw("/players/u1")?;   // Option<Value>, formulas as {"$formula","$value"}
store.get_node("/players")?;     // Option<Arc<Node>> — zero-copy handle
store.head();                    // Arc<Committed>: an immutable point-in-time
                                 // snapshot of the whole document, valid forever
store.version();                 // u64
```

**Write** — each call is one atomic commit returning
`CommitInfo { version, changed_paths }`:

```rust
store.put("/a/b", json!(1))?;                 // Value in
store.put_node("/a", node)?;                  // Arc<Node> in
store.put_typed("/config", &config)?;         // any T: Serialize, no Value
store.append("/log", json!("entry"))?;        // array append (creates the array)
store.merge("/users", json!({"bob": null}))?; // RFC 7396 merge patch
store.remove("/a/b")?;
store.commit(ops, &conditions)?;              // batch of Ops + preconditions
```

**Optimistic transactions** — read-set validation with automatic retry:

```rust
let (out, info, retries) = store.transaction(|txn| {
    let bal = txn.get("/bal")?.and_then(|v| v.as_i64()).unwrap_or(0);
    txn.put("/bal", json!(bal - 30))?;
    Ok(bal)
})?;
```

**Subscribe** — a consistent snapshot plus every commit after it,
version-ordered and gap-free:

```rust
let (snapshot, mut rx) = store.subscribe();
while let Ok(ev) = rx.recv().await {
    // ev: Arc<CommitEvent> { version, changed_paths, committed }
}
```

**Embed the HTTP server** (optional): `api::router(doc, info)` is an axum
`Router`; `api::spawn_info_ticker(info, period)` runs the heartbeat.

Lower-level tree tools live in `visijson::model`: `Pointer`,
path-copy `get_at`/`set_at`/`insert_at`/`remove_at` on any root,
`diff_paths`, `merge_patch_diff`, `reconcile`, `walk`, and `PlainView`
(streaming formula-resolved serialization — pairs with `get_node`).

### The typed fast lane

Typed data gets a fast path in — while the tree stays the single source of
truth (reads, subscriptions, formulas, and the WAL never see the typed
layer). For poll-style syncing of big collections, derive `Document` and
use a `TypedMount`: it retains the last synced value and diffs each new one
against it **natively** — generated field-by-field comparisons, no
serialization of anything unchanged — committing only minimal ops:

```rust
#[derive(Document, Clone)]
struct Player { name: String, age: i64, level: i64 }

let mut mount: TypedMount<BTreeMap<String, Player>> =
    TypedMount::new(store.clone(), "/players")?;
mount.sync(fetch_players()?)?; // first sync: one PUT of everything
mount.sync(fetch_players()?)?; // after: native diff → minimal ops (or none)
```

At 20k entries × 110 fields (`examples/bench_diff.rs`), an unchanged sync
costs ~44ms of pure comparison and commits nothing; a 3-field drift commits
3 tiny ops — versus ~330–440ms for a full-replace PUT of the same data,
typed or not, because a full replace must rebuild and re-compare the whole
tree. Field types: integers, floats, `bool`, `String`, `Option` (`None` ⇢
JSON null), `Vec` (same length diffs per index, length changes replace
wholesale), string-keyed `BTreeMap`/`HashMap`, and nested
`#[derive(Document)]` structs.

## Conditional requests

Reads are cacheable and precisely invalidated:

```sh
curl -i localhost:8080/doc/players/u1
# → ETag: "742"
# → Last-Modified: Sat, 08 Aug 2026 23:50:08 GMT
# → Cache-Control: no-cache

curl -i -H 'if-none-match: "742"' localhost:8080/doc/players/u1
# → 304 Not Modified            (and stays 304 when /players/u2 changes)
```

The ETag is the version of the last commit whose changed paths **overlap**
that subtree — the same relevance rule subscriptions use — so a write to a
sibling leaves your cached copy valid. `?raw=true` is a distinct
representation with its own ETag. `If-None-Match` takes precedence over
`If-Modified-Since` (RFC 9110), weak comparison applies, and an
unparseable date is ignored rather than trusted.

Commit timestamps are persisted in the WAL and in snapshots, so
`Last-Modified` survives restarts. The per-path index behind ETags is
rebuilt on recovery — from the snapshot version as a floor, sharpened by
every replayed commit — and always errs newer than the truth, so a stale
`304` is impossible; the cost of imprecision is only a needless refetch.
The same applies if the index hits its size cap and collapses to
whole-store validators.

## Consistency model

- A commit: load current root → apply ops by path-copy → re-evaluate affected
  formulas topologically → CAS the root. CAS losers replay against the new
  root; transaction commits additionally validate their read-set and abort
  with a conflict (retrying the closure) if any read was invalidated.
- Commits publish through an ordering stage that hands records to a dedicated
  WAL writer thread strictly by version: WAL order == broadcast order ==
  version order, and subscribers get a snapshot plus exactly the events after
  it — no gaps, no reordering. The writer batches whatever queued up during
  the previous write into one fsync (group commit), so concurrent writers
  share fsyncs and throughput scales with load.
- A commit does not return until its own WAL record has been written
  (`Durability::Async`/`NoFsync`) or fsynced (`Durability::Sync`, the
  default) — ack implies durable to the configured strength, and events are
  broadcast only after their batch reaches that level, so subscribers never
  observe a commit a crash could take back. If a WAL write ever fails, the
  store fail-stops: the failing commit and everything queued behind it
  error, and every subsequent write is rejected up front rather than
  silently lost; restart recovers to the last durable record.
- Snapshots never stall commits: the publication lock is held only to pin a
  version and mark a WAL segment rotation at that exact boundary; the
  document serializes outside the lock and superseded segments are deleted
  after the snapshot lands. Recovery = snapshot + replay of newer WAL
  records (legacy `wal.log` first, then `wal/` segments in order).

## Tests

`cargo test` covers concurrency (racing increments, disjoint writes, torn-read
detection), transaction retries and conditions, formula correctness and
cycles, subscription snapshot/ordering guarantees, crash recovery (WAL-only,
snapshot+WAL, torn tail), and end-to-end REST/WebSocket behavior.
