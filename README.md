# jsondocstore

An in-memory JSON document store built on **immutable persistent tree nodes**
(`Arc<Node>`) with **atomic root swaps** (`ArcSwap<Committed>`). Every write is
a path-copy that shares all untouched subtrees with the previous version, and
commits land via a CAS on the root pointer — readers are lock-free and always
see a fully consistent snapshot.

Features:

- **JSON pointer addressing** (RFC 6901, including `~0`/`~1` escapes and `-`
  array append)
- **REST API** — GET / PUT / POST (append) / PATCH (RFC 6902 subset:
  `add`/`replace`/`remove`/`test`) / DELETE, plus a `/txn` batch endpoint
- **Optimistic transactions** — read-set validation with automatic retry
  (Rust closure API) and conditional commits (`test` ops / `conditions`) over
  REST
- **WebSocket subscriptions by path** — consistent initial snapshot followed
  by gap-free, strictly version-ordered **delta** events (only the changed
  paths and their new values, never untouched siblings), filtered by path
  relevance; every commit records its precisely-diffed changed paths
- **Formula nodes** — spreadsheet-style expressions over pointer references
  with dependency tracking, topological re-evaluation, and cycle detection
  (cycles reject the commit)
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

Open any document path in a browser — `http://localhost:8080/doc` or
`/doc/any/sub/path` — and the server content-negotiates an embedded
single-file UI (API clients still get JSON; `?raw=true` forces JSON from the
address bar). The page subscribes to that subtree over the WebSocket and:

- renders the value as a navigable JSON tree (keys link deeper, breadcrumbs
  link up), live-updating with per-row flashes on every relevant commit
- lets you click any scalar to edit it in place (PUT to that pointer), or
  edit the whole subtree as raw JSON — formulas round-trip intact
- shows connection status, current version, and a change-event log
- reconnects automatically with backoff, resyncing via a fresh snapshot

The server also maintains a liveness node at `/info` — wall-clock time
(RFC 3339), epoch millis, uptime, and start time — rewritten once per second
(`INFO_TICK_SECS`, `0` disables). Watching `/doc` in a browser therefore
shows a heartbeat; `GET /doc/info/time` works as a liveness probe.

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

# JSON Patch with a guard
curl -X PATCH localhost:8080/doc -H 'content-type: application/json' -d '[
  {"op": "test", "path": "/users/alice/age", "value": 30},
  {"op": "replace", "path": "/users/alice/age", "value": 31}
]'

# Conditional transaction (409 on conflict — retry client-side)
curl -X POST localhost:8080/txn -H 'content-type: application/json' -d '{
  "ops": [{"op": "put", "path": "/bal", "value": 70}],
  "conditions": [{"path": "/bal", "expected": 100}]
}'

# Formulas: write an object with "$formula"; reads return the computed value
curl -X PUT localhost:8080/doc/total -d '{"$formula": "sum(/cart/prices) * 1.08"}' -H 'content-type: application/json'
curl localhost:8080/doc/total                      # computed number
curl 'localhost:8080/doc/total?raw=true'           # {"$formula": ..., "$value": ...}

# Subscribe (snapshot first, then ordered delta events)
websocat 'ws://localhost:8080/ws?path=/users/alice'
# → {"type":"snapshot","version":3,"path":"/users/alice","value":{...}}
# → {"type":"change","version":4,"changes":[{"path":"/users/alice/age","value":31}]}
# → {"type":"change","version":5,"changes":[{"path":"/users/alice/nick","removed":true}]}
```

Formula grammar: `+ - * /`, parentheses, unary minus, numeric literals,
pointer references (`/a/b`), and `sum` / `count` / `min` / `max` (scalars and
arrays). A bare pointer expression mirrors any JSON value. Evaluation errors
(missing ref, divide-by-zero) surface as `"#ERR: ..."` values and heal when
the dependency is fixed; dependency cycles reject the writing commit (HTTP
422).

## Consistency model

- A commit: load current root → apply ops by path-copy → re-evaluate affected
  formulas topologically → CAS the root. CAS losers replay against the new
  root; transaction commits additionally validate their read-set and abort
  with a conflict (retrying the closure) if any read was invalidated.
- Commits publish through an ordering stage that flushes strictly by version:
  WAL append and event broadcast happen in the same order, and subscribers
  get a snapshot plus exactly the events after it — no gaps, no reordering.
- Snapshots are written under the publication lock, so the WAL rotation point
  is exact; recovery = snapshot + replay of newer WAL records.

## Tests

`cargo test` covers concurrency (racing increments, disjoint writes, torn-read
detection), transaction retries and conditions, formula correctness and
cycles, subscription snapshot/ordering guarantees, crash recovery (WAL-only,
snapshot+WAL, torn tail), and end-to-end REST/WebSocket behavior.
