# 03. Execution

## 1. Selection and planning

`atlatl run [--env E] [--replication R…] [--stream ID…] [--tag T…] [--instance 'R[KEY]'…] [--with-deps | --no-deps] [--dry-run]`

1. **Resolve the selection** to a set of replications. `--stream` narrows a replication to a subset of its streams. The subset is still committed atomically.
   `--instance <replication>[<key>]` (the name the plan and report give a `for_each` instance) selects that instance only; with several, only the named instances run, and only their replications are selected ([11 §3](11-extensibility.md#3-larger-pipeline-graphs)).
2. **Dependency closure.**
   - **Within a replication:** REST queue producers ([07 §7](07-rest-api.md#7-iteration-queues-and-endpoint-dependencies)).
     - With `--with-deps` (the default), a producer that isn't selected is added in **feed-only** mode: it fetches and emits, but writes nothing and produces no materialization.
     - With `--no-deps`, the run fails at plan time and names the missing producer. A consumer never silently no-ops.
   - **Between replications:** `after: [replication…]`. The dependent replication runs only if every replication it names succeeded in this run. If an `after` target isn't selected, it's ignored (ordering, not data).
3. **Discovery.** Wildcard `tables:` streams are expanded against the source catalog into **members**, sorted by name. Zero matches is an error unless `on_no_match: warn`.
4. **Build the plan** (a DAG of typed steps, [11 §4](11-extensibility.md#3-larger-pipeline-graphs)). A typical replication plans as:
   - ingest: per member, `extract → stage → validate`, then one `apply` step per stream inside the replication's single commit;
   - multi-hop (API → lake → Postgres): the lake hop's steps, then a lake commit, then the serve hop's steps, then its commit.
5. **Static checks:**
   - connection capabilities fit the mode (for example, `cdc` needs a `ChangeSource`);
   - PK and `update_key` columns exist;
   - read-only connections are never targets;
   - every `atomic_group` has a single target ([§2](#2-atomicity)).

`atlatl plan` runs steps 1–5 and prints the plan as JSON, with no side effects beyond discovery. `atlatl run --dry-run` runs everything but the commit ([§11](#11-dry-run)).

## 2. Atomicity

| Unit | Rule |
|---|---|
| **Replication** | Succeeds or fails **as a unit**. All of its streams' writes to a target commit in **one transaction** at the end, together with their state (cursors, sync values, rejects). If anything fails, nothing from that replication is committed on that target |
| **Multi-hop replication** | Each hop commits in order, upstream first (lake, then serving). If a later hop fails, the replication is **failed**, the earlier hop's commit stays, and the next run's serve hop catches up from its cursor ([08](08-cdc.md)). Sibling hops (fan-out) still run, and the hops below a failed one are `not_started`. A `replication_retries` retry re-runs only the hops not yet committed. The report marks each hop `committed` / `failed` / `not_started` |
| **`for_each` instance** | Each instance (`<replication>[<key>]`) is its own replication for atomicity, scheduling, retries and `--skip-succeeded`; `after` on the replication waits for every instance ([11 §3](11-extensibility.md#3-larger-pipeline-graphs)) |
| **Run** | A set of replications, **independent** of each other. One failing doesn't affect the others, except for dependents through `after` |
| **`atomic_group`** (opt-in) | Several replications that commit in **one transaction** or not at all. They must share one target connection, which is validated at plan time. For mixed targets, use `after` ordering, which gives no atomicity |

Consequences:
- There are no checkpoints and no partial resume. A failed or killed replication re-runs from its last committed state (cursor or `sync` values) the next time. A long REST backfill is split across runs instead, with `for_each.max_items` and `except_landed` ([07 §7](07-rest-api.md#7-iteration-queues-and-endpoint-dependencies)): each run commits its batch.
- **Chunking** ([04 §7](04-ingest.md#7-chunking)) bounds memory only. Chunks are applied one after another inside the single transaction.
- **A plain DuckDB file target** has no snapshots: the replication's transaction is a DuckDB transaction, and the report's `lake_snapshot` stays unset ([06 §10](06-connectors.md#10-duckdb-files)).
- **D1** can't hold a transaction across HTTP requests. Its applier uses an idempotent window, and its cursor advances last ([08 §6.3](08-cdc.md#63-d1)). This is the one documented exception to single-transaction commits.

## 3. Scheduling

- **Across replications:** `--concurrency N` (default `engine.replication_concurrency`, 2). A replication starts when its `after` dependencies have succeeded, a slot is free and its locks are acquired.
  - The scheduling unit is a replication, or a whole `atomic_group`, whose members run and commit as one unit and share one outcome.
  - A dependent of a replication that failed, was killed or was skipped is itself `skipped`, with code `dependency_failed`. `up_to_date` and `--skip-succeeded` skips count as success for `after`.
  - `lock_group` locks are taken for the unit's whole run, before its first attempt. A lock timeout fails the unit with `conflict`.
  - `replication_retries` re-runs a unit only after a retryable-class failure (transient, throttled, decode, conflict, timeout). `data`, `config` and `internal` failures are deterministic and not retried.
  - A per-replication `timeout` cancels the attempt, which is reported `killed` with code `timeout`.
  - Results stream out as each unit finishes, for per-replication materializations.
- **Within a replication:** extracts of members and streams run concurrently, up to `engine.extract_concurrency` (default 4) and per-connection caps (`connections.<c>.max_concurrency`). Applies run in the replication's commit transaction in declared order (upserts parent-first, deletes child-first, [08 §7](08-cdc.md#7-ordering)).
- **Determinism:** among ready work, file order (replication, then stream, then member) breaks ties.

## 4. Isolation between replications

- **Connections:** each replication gets its own source and target sessions from per-connection pools, with a health check on checkout. A dropped connection fails only the attempt that was using it, and the retry gets a fresh one.
- **Staging:** names are scoped to the run and the replication ([04 §3](04-ingest.md#3-staging)), so concurrent runs never collide.
- **Variables:** replication and stream `vars` exist only in their own template context.
- **Panics:** a panic inside a replication is caught at the replication boundary and reported as `internal` for that replication. Its transaction is abandoned, and the process exits non-zero ([§10](#10-exit-codes)).

## 5. Error taxonomy

Every error is classified. The class drives retry, rate limiting and reporting. Connectors map their native errors into these classes, and unknown errors default to `internal` (not retried).

| Class | Examples | Default handling |
|---|---|---|
| `transient` | connection reset, EOF mid-read, TLS handshake failure, idle-session drop, 5xx, DNS blip | retry with backoff |
| `throttled` | HTTP 429 or a vendor-specific throttle (the `throttled` vendor's 409 with "slow down"), D1 rate limit, Redshift Data API `ThrottlingException` | retry, honouring `Retry-After` and similar headers, **and** slow the connection's rate limiter ([07 §8](07-rest-api.md#8-rate-limiting)) |
| `decode` | invalid JSON, a truncated body, a wrong or missing Content-Type, a CSV parse error, invalid UTF-8, JMESPath applied to a non-object | **retryable**, at the granularity the user chose (§6.1) |
| `conflict` | DuckLake commit conflict, Postgres serialization failure or deadlock, lock timeout | retry the **commit step only**, keeping the staging |
| `timeout` | a request or statement deadline | retry |
| `auth` | 401/403, expired token, bad credentials | refresh credentials once, then fail |
| `data` | a failed cast into the target type, NULL or duplicate PKs, a table-drift violation, a source schema violation, rejects over threshold | fail the replication (no retry). The report includes samples |
| `config` | an invalid reference, a missing column, an unresolved template | fail at plan time where possible |
| `killed` | SIGTERM, SIGINT or `--timeout` | none ([§9](#9-kill-behaviour)) |
| `internal` | a bug or panic | fail, with a backtrace in the report |

**Decode failures are classified data, never crashes.** A body that can't be decoded (say a plain-text 409 with no Content-Type) goes through the rules and retries like any other response:
- **Capture first.** A response is captured as `status`, `headers` and raw `text`/bytes before anything else. Decoding is lazy and never throws out of the pipeline: a failure yields `response.json = null` and `response.decode_error = {kind, message, offset}`.
- **Rules come before decoding.** REST rules ([07 §10](07-rest-api.md#10-response-rules-and-decoding)) see the response before any decode is required, so they can match on status and text.
- **Throttle-like failures slow the connection down.** A decode failure on a throttle-like status (409/429/503), or on a body matching the connection's `throttle_patterns`, is classified `throttled`, and the shared rate limiter backs off.
- **Repeated failures count as throttling.** Repeated decode failures within a window (`decode_failures_as_throttle: 3/60s`) are treated as throttling, because some vendors answer overload with a 200 carrying an HTML error page.
- **Non-HTTP sources are covered too.** A torn gzip member, a truncated S3 object or a CSV record split mid-transfer is also `decode`.

## 6. Retry

atlatl owns retries. Dagster retries are only a coarse backstop.

```yaml
retry:                           # project → replication → stream/endpoint; deep-merged
  max_attempts: 5                # total attempts per unit, including the first
  backoff: exponential_jitter    # none | constant | linear | exponential | exponential_jitter
  base: 2s
  max: 5m
  retry_on: [transient, throttled, decode, conflict, timeout]
  budget: 30m                    # stop retrying once this much wall time has been spent in retries
replication_retries: 0           # re-run the whole replication from scratch after it fails (off by default: re-extracting burns vendor quota)
```

| Level | What reruns |
|---|---|
| **Request / query / object** | one HTTP request (pagination and iteration state are kept), one SQL statement or Redshift Data API statement, or one S3 object read |
| **Commit** | the commit transaction only, against the retained staging (`conflict`) |
| **Replication** | the entire replication, from its last committed state. Only when `replication_retries > 0` |

- The waits are `base · 2^(n-1)` for exponential and `× U[1, 1.5)` for jitter, capped at `max`.
- `Retry-After`, `RateLimit-Reset` and `X-RateLimit-Reset` override the computed wait for `throttled`.
- Every attempt is recorded in the report with its class, message and wait time.

### 6.1 Decode-failure granularity

Each stream or endpoint chooses how decode failures are retried:

```yaml
on_decode_error:
  retry: request        # request (default) | row | none
  reject_threshold: {ratio: 0, min_count: 1}   # row mode only: max rejects before the replication fails
```

- **`request`** re-issues the whole request, page, query or object and decodes it again, up to `max_attempts`. If it's still failing, the replication fails with `decode`.
- **`row`** keeps the rows that decoded and handles the others individually:
  1. The request that produced a bad row is refetched, up to `max_attempts`. The row is re-decoded each time.
  2. Rows that still fail become **rejects** (§7).
  3. If rejects exceed `reject_threshold` (default: any reject), the replication fails with `data`. Otherwise it commits, with the rejects recorded.
     - The count is this run's decode rejects, still-failing retries of older ones included. It trips when `count ≥ min_count` and `count / (staged rows + count) > ratio`. Skip rejects (`record_skips`) don't count; `max_skipped` guards those.
- **`none`** fails on the first decode error.

Casting failures into target types (for example `TRY_CAST` returning NULL for a non-NULL value) follow the same `row` / `none` choice, through `on_cast_error`. `on_cast_error: {set_null: true}` downgrades them to NULL with a counted warning.

## 7. Rejects

Rejects are rows that still fail decoding or casting after their retries. They are **not** a dead end. atlatl retries them on later runs, so that a fix on the upstream side lands without a full reload.

```sql
-- in the lake
CREATE TABLE _atlatl.rejects (
  stream_id     VARCHAR,
  member        VARCHAR,
  reject_key    VARCHAR,     -- stable identity: record PK if decodable, else hash(request descriptor, row offset)
  request       JSON,        -- replayable request descriptor (REST: method/url/params/body; files: object + offset; DB: query + key)
  raw           VARCHAR,     -- raw payload excerpt (redacted)
  error_class   VARCHAR,
  error         VARCHAR,
  first_seen_run VARCHAR, last_seen_run VARCHAR,
  attempts      INTEGER,
  first_seen_at TIMESTAMPTZ, last_seen_at TIMESTAMPTZ
);
```

- **Recording.** New rejects, and updates to existing ones, are written **in the replication's commit transaction**. They only persist when the replication commits, which means when they stay under `reject_threshold`. If the replication fails, the run report carries the reject samples, and nothing is persisted, because the next run re-reads everything anyway.
- **Retrying.** At the start of each run, a stream's open rejects are retried before, or alongside, the normal extract:
  - **REST:** the stored request descriptors are re-issued, through the same auth, rate limiter and rules, after the normal extract (so with `dedup: first` a fresh copy of the row wins).
  - **Files and DB:** a full re-read covers them naturally. For incremental sources, the rows are re-selected by key where the source supports it.
  - A reject that now decodes lands like any other row, and its reject entry is **deleted in the same transaction**.
  - One that still fails with a different `error_class` or `error` is updated (error, `attempts`, `last_seen_*`), and that update is a write: the replication commits.
  - One that still fails with the same class and error text isn't a write by itself. Its `attempts` and `last_seen_*` are refreshed **when the replication commits** for another reason (a row, DDL, state, hook or another reject changed). Otherwise nothing commits, its counters stay as they were, and the stream reports `up_to_date` ([04 §1](04-ingest.md#1-replication-lifecycle)). So `attempts` counts the commits that saw it failing, not every run. It still counts toward `reject_threshold` (§6.1).
- **Ageing.** `on_decode_error.reject_ttl` (a duration such as `7d`; default none, so nothing expires) sits next to `reject_threshold`: on a stream (project → replication → stream, deep-merged) or on a REST endpoint, the stream's winning.
  - A reject **expires** once its `first_seen_at` is more than the ttl before the run reads its open rejects (the age runs from when it was first seen, not from its last failure, so a reject that keeps failing still expires).
  - An expired reject isn't retried. It is **deleted in the replication's commit**, and the deletion is a write: the replication commits even if nothing else changed.
  - If this run's normal extract hits the same row again, it is recorded afresh (`first_seen_*` now, `attempts` 1, counted in `rejects.new`), so a permanently broken row is retried for at most one ttl at a time.
  - The member report counts expiries in `rejects.expired`; a dry run reports the would-expire count there and deletes nothing.
- **Reporting.** The report (`rejects.new`, `resolved`, `expired`, `open`) and Dagster metadata (`atlatl/rejects`) show the reject counts per stream, the open count also on an `up_to_date` run.
- **Managing** (`atlatl rejects`, addressed like `atlatl state` ([05 §5](05-state.md#5-cli)): `<stream>`, `<stream>@<key>` or `--instance <key>` for a `for_each` instance, a bare id covering every instance; `--env` resolves the state lake as for `state`):
  - `rejects list [--stream S] [--json]`: open rejects per stream and member, with key, error class and error, attempts, first and last seen (time and run) and the request descriptor, redacted. Text by default.
  - `rejects retry <stream>…`: a run restricted to the selected REST streams' open rejects. Their stored requests are re-issued now (no normal extract, no queue producers); rows that decode land and their rejects are deleted, in one lake commit per replication. The rows are merged by primary key (appended without one), never mirrored: a `full-refresh` target keeps its other rows. `reject_threshold` isn't applied (the retry sees only the rejects, and a still-failing one stays open), `sync` values don't move, hooks aren't run (a member warning says so), and `then:` hops aren't run (they pick the rows up on the next run). Expiries apply as in a run. It prints each member's `resolved`, `still_failing`, `expired` and `open`; the run is recorded in `_atlatl.runs` as `rejects retry`. Non-REST streams are a `config` error (`rejects_retry_unsupported`).
  - `rejects drop <stream> [--member M] [--key K …]` or `rejects drop --all`: deletes the selected rejects in one commit, logged in `_atlatl.runs` as `rejects drop`. A stream or `--all` must be named.

## 8. Locks

- **Per-target-table lock.** A replication takes exclusive locks on every `(connection, table)` it will write, from the start of its commit until the commit ends. The default backend is a **Postgres advisory lock**: on the DuckLake catalog database for lake targets, or on the serving Postgres itself.
  - The call is `pg_try_advisory_lock(hashtextextended('atlatl:<conn>:<schema>.<table>', 0))` on a dedicated session.
  - Locks are taken in sorted order to avoid deadlocks.
  - Locks are released when the session ends, so a killed process never leaves a stale lock.
- **Timeout.** Acquisition waits up to `lock_timeout` (default 30m), then fails with `conflict`.
- **Lock groups.** `lock_group: <name>` on replications takes a group-wide lock (key `atlatl-group:<name>`, which can't collide with a table key) for their whole run. Group keys sort before table keys, so they are taken first. It can replace a Dagster `no_concurrent_runs` tag, or sit under one as a second layer.
- **Other writers.** dbt and the maintenance jobs don't take atlatl's locks. For DuckLake, the conflict retry at commit covers concurrent commits by other writers.
- **Plain DuckDB files.** Locks are in-process; replications into the same file in one run take them as for a lake. Across processes DuckDB's own file lock applies: atlatl and dbt run one after the other, and a file another process holds fails the attach (`transient`, `duckdb_locked`; [06 §10](06-connectors.md#10-duckdb-files)).

## 9. Kill behaviour

Interruption doesn't need to be graceful. Killing atlatl at any moment is safe, because every commit is atomic and state only advances inside commits.

- On SIGTERM or SIGINT, atlatl **exits within about a second**:
  - it does not drain in-flight work, run a grace period or wait for commits in progress;
  - open transactions roll back when their sessions drop;
  - in-flight Redshift Data API statements get a best-effort `CancelStatement`.
- **Best-effort report.** Before exiting, atlatl tries to write the report. Replications in flight get `killed`, those not started get `not_started`, and those already committed keep `succeeded`. SIGKILL skips this, and then the absence of a report is itself the signal.
- The exit code is 130 (SIGINT) or 143 (SIGTERM).
- **Leftovers.** Artefacts outside a transaction, such as Redshift UNLOAD prefixes, are run-scoped. `atlatl gc` and an S3 lifecycle rule clean them up. There's no cleanup on the kill path.
- `--timeout` (run) and `timeout` (per replication) behave the same way when they expire: the affected work is killed and reported `killed`.

## 10. Exit codes

| Code | Meaning |
|---|---|
| 0 | every selected replication succeeded (including legitimately empty ones) |
| 1 | one or more replications failed; the report says which. Replications that succeeded are committed |
| 2 | config, validation or plan error; nothing ran |
| 3 | internal error (a panic, a report write failure), whether or not replications succeeded |
| 130 / 143 | killed by SIGINT / SIGTERM |

A panic always produces a non-zero code. When present, the report is the machine-readable truth; the exit code only summarises.

## 11. Dry run

`atlatl run --dry-run [--diff-samples N]` previews an ingest run: what a real run would change on each lake target, with nothing committed.

- **Same work as a run, up to the commit.** Selection, `for_each` instances, discovery, extract, staging, the invariants ([04 §9](04-ingest.md#9-invariants-checked-before-apply)), source schemas, `on_empty` / `guard`, table-drift planning and the new `update_key` / `sync` values all run exactly as they would. A violation fails the replication with the class a run would report, and the exit code follows ([§10](#10-exit-codes)).
- **Then a read-only diff per member** of the staged rows (cast into the target's column types, as the apply would) against the target as the commit would find it (after its table-drift DDL: added columns read as NULL). It counts `inserts`, `updates`, `unchanged`, and `deletes` for the modes that delete (`full-refresh`), with the same change guard as the apply (run-varying metadata columns aren't compared, [04 §8](04-ingest.md#8-metadata-columns)). A chunked stream is compared chunk by chunk and summed; its deletes are the target keys absent from every chunk.
- **Samples:** up to `--diff-samples` (default 5) per op and member, ordered by key: `{op: insert | delete, key}` and `{op: update, key, changed, staged, target}`, where `staged` and `target` hold the changed columns' values.
- **Keyless streams** (an append, a `snapshot`, a `full-refresh` without a `primary_key`) report counts only, with `counts_only` saying why: the target's row count and the staged count are all that can be compared.
- **Nothing is written.** Lakes are attached read-only, and a lake catalog that doesn't exist yet isn't created: it is diffed as an empty lake (every staged row is an insert, the table's `CREATE TABLE` is in `ddl`) with a run warning `dry_run_no_catalog` naming it ([06 §2](06-connectors.md#2-ducklake)); a catalog that exists but can't be reached is an error as for a run. No transaction is opened on a target, so there is no lake snapshot; nothing is written to `_atlatl` (state, rejects, run history): if the state lake has no `_atlatl` yet, empty in-memory stand-ins are read instead of creating it; no table lock is taken; cursors don't advance. Hooks aren't run (a member warning says so). Remote side effects are limited to reads, except Redshift UNLOADs, whose prefixes are removed afterwards as usual.
- **Output.** The run report ([10 §4](10-observability.md#4-run-report)) has `dry_run: true` and a `diff` per member, and stdout gets one JSON object shaped like `cdc bootstrap --dry-run`'s: `{dry_run: true, run_id, status, exit_code, warnings, replications: [{replication, dry_run: true, status, error, streams: [...]}]}`, a stream entry per member with `stream`, `member`, `target`, `mode`, `staged`, `target_rows`, the four counts, `counts_only`, `ddl` (what the commit would run first), `state_before`, `state_after`, `chunks`, `schema`, `rejects` and `samples`. A short text summary goes to stderr. A replication is `up_to_date` when the run would change nothing, `succeeded` otherwise; neither committed anything.
- **Scope.** Only replications the lake runner applies: REST, files, Delta, Postgres (not `mode: cdc`), Redshift and lake → lake into a lake target. Selecting a lake → serving or Postgres logical-replication replication is a `config` error (`dry_run_unsupported`) that points at `atlatl cdc bootstrap <replication> --dry-run`. A multi-hop replication dry-runs its lake hop only: its `then:` hops read that hop's commit, so they are listed with `not_run: true` and a run warning (`dry_run_hops`).
