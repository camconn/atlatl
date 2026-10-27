# 01. Architecture

## 1. Shape

atlatl is **one static binary**, `atlatl`, that runs to completion per invocation. Dagster calls it as a subprocess through a thin Python package ([09](09-dagster.md)). An in-process library (a pyo3 binding) is *later*. The subprocess boundary is the v1 contract, because it gives memory isolation and hard-kill semantics for free.

```
                    ┌────────────────────────── atlatl process ──────────────────────────┐
 atlatl.yaml ──►    │  config loader ─► planner ─► executor (tokio)                      │
 replications/*.yaml   │                     │            │                                 │
                    │                     │     ┌──────┴──────────── replications ────┐  │
                    │                     │     │  source ─► [Arrow batches] ─► stage  │  │
                    │                     │     │                  │                   │  │
                    │                     │     │           DuckDB engine (in-proc)    │  │──► DuckLake
                    │                     │     │                  │                   │  │    (PG catalog + S3)
                    │                     │     │  lake read ─► [Arrow] ─► applier ────┼──┼──► Postgres / SQLite /
                    │                     │     └──────────────────────────────────────┘  │    D1 / Turso
                    │   state store · lock manager · rate limiters · retries · rejects     │
                    │   events (NDJSON) ─────────────────────────────────────────────────┼──► stdout / file
                    │   run report (JSON) ───────────────────────────────────────────────┼──► --report path
                    └─────────────────────────────────────────────────────────────────────┘
```

## 2. The engine: DuckDB in-process

DuckDB is linked **dynamically against the official `libduckdb` from the DuckDB GitHub release**, through the `duckdb` crate, pinned exactly to `=1.10506.1` (DuckDB 1.5.6). The build fetches it (`DUCKDB_DOWNLOAD_LIB=1`), or takes a local copy from `DUCKDB_LIB_DIR`. DuckDB is never compiled from source (`.github/workflows/release.yml`). It is atlatl's working engine, used for:
- staging and typing incoming data;
- `MERGE`/`INSERT` into DuckLake;
- reading DuckLake, including snapshot-pinned reads and `table_changes`;
- net-change collapse and diffing for CDC;
- reading files (CSV, Parquet, JSON) locally and from S3;
- reading Postgres through the `postgres` extension where that is the simplest path.

Rules:
- **One DuckDB database instance per process.** It is in-memory, with `temp_directory` for spill. Every replication gets its own `Connection` from that instance (plus extra connections for concurrent extracts), so the memory limit is global across the process rather than per stream.
- **Budget before work.** `memory_limit`, `threads`, `temp_directory`, `max_temp_directory_size` and `preserve_insertion_order=false` are passed to `duckdb_open_ext` in a `duckdb_config`, together with `autoinstall_known_extensions=false`, `autoload_known_extensions=false`, `allow_unsigned_extensions=false`, `allow_community_extensions=false` and `extension_directory`, so they are in force before any `ATTACH` or load ([§5](#5-memory-model)).
  - The spill directory is a per-process subdirectory, `<temp_directory>/atlatl-<pid>`, so concurrent processes never share DuckDB temp files.
  - The extension directory is an explicit engine option, else `$ATLATL_EXTENSION_DIR`, else `<exe>/../share/atlatl/extensions`.
- **ATTACH once.** Extensions (`ducklake`, `postgres`, `httpfs`, `aws`, and `delta` from M2) load from a **pre-staged extension directory** (DuckDB's layout, `<dir>/v1.5.6/<platform>/<name>.duckdb_extension`) shipped with the binary, with no network `INSTALL` at runtime. Unsigned and community extensions are refused. The extensions exchange C++ objects with the host, so host and extensions must come from the same official build and version (`scripts/smoke-test-prefix.py`). `delta` is the largest (80 MB; about 25 MiB compressed) and brings the wheel close to PyPI's 100 MB per-file limit (size check in `.github/workflows/release.yml`).
- **The library is found next to the binary, never in system paths.** The rpath is `$ORIGIN/../lib/atlatl` (`@executable_path/…` on macOS), plus `target/` paths in development. A binary without one silently picked up a different distro `libduckdb` (rpath checks in `.github/workflows/release.yml`). The DuckDB and extension versions are pinned and must match the ones dbt uses (dbt-duckdb), because both write the same catalog. A version-mismatch check runs at startup: `duckdb_library_version()` must be the version the crate pin encodes (`v1.5.6`), otherwise `config` (`duckdb_version_mismatch`).
- **Blocking isolation.** DuckDB calls are synchronous. They run on `spawn_blocking` behind a semaphore sized to `max(threads, 2)` (at least two, so a stream and a call its consumer makes can't deadlock at `threads: 1`), never on tokio worker threads.
  - `Engine::with_conn(|conn| …)` runs a closure on a pooled connection; a connection returns to the pool only after a successful call. A `Session` leases one connection for work spanning calls (staging temp tables, the commit transaction).
- **UTC.** The engine runs `SET GLOBAL TimeZone = 'UTC'` right after open, before any work. ICU is built into the official library, but its default zone is the host's, which silently changes `date_trunc`, `current_date` and timestamp rendering ([spike S3 §5](../docs/implementation/spike-s3.md#5-other-facts)).
- **Built-in extensions.** The official library includes `core_functions`, `parquet`, `json`, `icu` and `autocomplete`. The instance is opened with `autoinstall_known_extensions=false` and `autoload_known_extensions=false`: a missing extension is an error, never a download.
- **Streaming reads use atlatl's own FFI reader.** duckdb-rs `Statement::stream_arrow` treats an error mid-stream as end-of-stream, which would silently truncate an apply. The engine streams with `duckdb_execute_prepared_streaming` / `duckdb_stream_fetch_chunk` and checks `duckdb_result_error` when the stream ends (`atlatl-engine/tests/engine.rs` `stream_surfaces_mid_stream_error`).
  - duckdb-rs doesn't expose a `Connection`'s raw handle, so a stream runs on its own raw connection from the shared database. It sees attached catalogs, secrets and committed data, but not another session's temp tables or open transaction.
  - Batches go over a bounded channel, and each holds its size in the process-wide `channel_bytes` budget until the consumer drops it. A mid-stream error is always the stream's last item, and a killed or timed-out stream ends with that error, never silently. Dropping the stream interrupts the query.
- **Timeouts.** A DuckDB query that exceeds its statement timeout is interrupted with the `duckdb_interrupt` C API (`InterruptHandle`, driven by a tokio timer), and re-interrupted every 25 ms until the call returns, since an interrupt only stops the statement running at that moment. The result is `timeout` (`statement_timeout`); an interrupt from a cancellation token (the engine's kill token, or a per-call one) is `killed`. The connection stays usable. On a kill signal the process simply exits, and open transactions are abandoned ([03 §9](03-execution.md#9-kill-behaviour)).
- **Transactions are real.** A replication's lake writes are one `BEGIN … COMMIT` on one connection, so every stream's DDL, data changes, state and rejects land in **one DuckLake snapshot** ([04](04-ingest.md), [05](05-state.md)).

## 3. Data paths

Every stream uses one of three paths. The planner picks one per stream, based on connector capabilities.

| Path | Used for | Flow |
|---|---|---|
| **Engine-native** | File/S3 sources (CSV, Parquet, JSON); Redshift UNLOAD read-back; Postgres sources (optional); DuckLake → DuckLake | DuckDB reads the source itself (`read_parquet('s3://…')`, `postgres_scan`, `read_csv`) straight into staging. There's no Rust data plane, so it is the fastest path with the fewest copies |
| **Rust-fed** | REST APIs, D1/SQLite/Turso as sources, future SDK/external sources | A Rust source yields Arrow `RecordBatch`es (or NDJSON spooled to a run-scoped temp file) over a **bounded** channel, and they are appended into a DuckDB temp table. Backpressure comes from the channel's byte budget |
| **Lake-out** | CDC and serving loads | A DuckDB query (snapshot-pinned or `table_changes`) streams Arrow batches over a bounded channel to a Rust **applier** (tokio-postgres binary COPY, rusqlite, D1 HTTP, libsql) |

API responses are staged as raw JSON records, and typing happens in SQL over the complete staged set ([04 §5](04-ingest.md#5-typing)). That replaces sampled inference.

## 4. Crate layout (Cargo workspace, one binary)

```
crates/
  atlatl-cli        # bin: clap commands, signal handling, exit codes
  atlatl-core       # config model + loader, plan IR, executor, error taxonomy, retry, locks, events, report
  atlatl-engine     # DuckDB wrapper: instance/budget, connections, interrupt, Arrow ingest, lake ops
  atlatl-ingest     # lake writes: staging, validation, table drift/adoption, mode SQL, chunk relations, metadata columns
  atlatl-state      # StateStore trait + backends (lake, target-db)
  atlatl-http       # REST API engine (auth, rate limit, pagination, rules, decoders)
  atlatl-cdc        # change-feed reader, net-change collapse, diff apply, retention guard
  atlatl-conn-ducklake | -postgres | -redshift | -files | -sqlite (SQLite, D1 and Turso) | -clickhouse
  atlatl-testkit    # fixtures, containers, HTTP mocks, fault injection
python/dagster-atlatl
```

- Connectors register through a **compile-time registry** keyed by `type:`, and each connector crate sits behind a cargo feature. The released binary enables every feature.
- There is no dynamic plugin ABI and no external-process source. Every source is Rust core plus YAML config ([11](11-extensibility.md)).

Candidate crates (to confirm during implementation):

| Concern | Crate |
|---|---|
| Async runtime | `tokio`, `tokio-util` |
| DuckDB, Arrow | `duckdb =1.10506.1` (feature `appender-arrow`; official libduckdb linked dynamically), `arrow` 58 |
| Postgres / Redshift wire | `tokio-postgres`, `postgres-types`, `rustls` + `tokio-postgres-rustls`, `deadpool-postgres` |
| AWS (S3, STS, Redshift Data API, Secrets Manager) | hand-rolled SigV4 over `reqwest` in `atlatl-conn-redshift` (Data API, STS, S3 list/get/delete); no AWS SDK crates (disk and build time). Secrets Manager is still open |
| HTTP | `reqwest` (rustls with ring, OS trust store via rustls-platform-verifier; clients start from `atlatl_core::tls::http_client`), `governor` (GCRA rate limiting) |
| Templates / expressions | `minijinja` |
| JMESPath | `jmespath` (jmespath.rs, with custom functions) |
| JSON | `serde_json` with `arbitrary_precision` (exact numbers) |
| YAML + schema | `serde_yaml_ng` or `serde_norway`, `schemars` (JSON Schema emitted for editor validation) |
| SQLite, libSQL | `rusqlite` (system `libsqlite3`), `libsql` (remote mode) |
| Archives | `zip`, `flate2`, `zstd` |
| CLI, errors | `clap`, `thiserror` |
| Logs, traces | `tracing`, `tracing-subscriber` (JSON), `opentelemetry-otlp` (feature-gated, off by default) |

## 5. Memory model

Three things must be capped together: atlatl's own heap, DuckDB's parquet/S3 buffers, and DuckDB's thread count (from the cgroup, not the host core count). atlatl uses one budget:

```yaml
engine:
  memory: auto          # auto = cgroup memory.max (or host RAM) × 0.85
  threads: auto         # auto = cgroup cpu.max quota (not host cores)
  duckdb_share: 0.6     # optional: fraction of `memory` for DuckDB memory_limit, replacing the headroom formula
  memory_mode: high     # high (default): faster, larger reserve | low: periodic allocator purges, smaller reserve
  temp_directory: /tmp/atlatl/spill
  max_temp_directory_size: 50GB
  channel_bytes: 256MB  # total in-flight Arrow/NDJSON across all replications
```

- DuckDB gets `memory_limit = memory − channel_bytes − fixed − threads × per_thread`, clamped to at least 128 MiB (the report flags `clamped`). A clamped budget no longer guarantees RSS within `memory`. `fixed` and `per_thread` are the reserve for RSS outside `memory_limit`, and depend on `memory_mode` (below). They cover DuckDB's out-of-limit allocations (mostly DuckLake's delete bookkeeping while a chunk runs), allocator pages not yet returned, HTTP bodies and the process baseline. They are engine parameters (`BudgetParams`), not config keys. `channel_bytes` bounds Rust buffers.
- **Allocator release.** DuckDB doesn't return freed memory to the OS promptly: jemalloc keeps dirty pages, and glibc keeps the heap fragmented by DuckLake deletes. Across chunks this grew to 470–920 MiB outside `memory_limit` ([S3 §6.2](../docs/implementation/spike-s3.md#62-where-the-memory-outside-memory_limit-went-c4-2026-09-29)). `atlatl_engine::alloc::release` purges all jemalloc arenas and runs `malloc_trim(0)`; both are looked up at runtime and skipped where libduckdb's jemalloc or glibc isn't present. What the engine does with it depends on `engine.memory_mode`:

  | `memory_mode` | Release | Reserve (`fixed` + `threads ×` `per_thread`) | 1 / 2 / 4 threads |
  |---|---|---|---|
  | `high` (default) | between chunks, and after every engine call of at least 1 s (so after `COMMIT`) | 550 + 50 MiB | 600 / 650 / 750 MiB |
  | `low` | the same, plus every 250 ms while any engine call or stream is in flight; at 2+ threads DuckDB's `allocator_bulk_deallocation_flush_threshold = 4MiB` as well | 220 + 25 MiB | 245 / 270 / 320 MiB |

  `high` is the default because it is the faster one. `low` trades merge time for about 200–370 MiB less reserve, which a tight pod can give to `memory_limit` or to `threads`. Measured with the S3 harness (a chunked 10M-row merge in one transaction, `memory_limit` 819 MiB, uncapped, two runs each; RSS outside `memory_limit` / merge time):

  | `threads` | `high` | `low` | `low` vs `high` |
  |---|---|---|---|
  | 1 | 383–405 MiB / 194–270 s | 188–200 MiB / 204–209 s | −200 MiB, time within noise |
  | 2 | 468–536 MiB / 124–130 s | 159–170 MiB / 154 s | −330 MiB, +20% |
  | 4 | 513–535 MiB / 94–101 s | 204–210 MiB / 117–118 s | −310 MiB, +20% |

  Each reserve covers its mode's worst case by at least 15% (`high`: 48% / 21% / 40%; `low`: 22% / 59% / 52%, counting the capped runs). The 250 ms interval: at 1 thread, where it is the only purge during a chunk, 1 s left 216 MiB outside and 100 ms 144 MiB at about 13% more time than 250 ms; at 2 threads 100 ms–1 s all landed at 139–166 MiB. The bulk flush still helps on top of the purge (206 → 159–166 MiB at 2 threads, 271 → 204–207 at 4) for 15–20% more time; at 1 thread it only slows the merge, so it isn't set there.
- With `memory: auto`, a 2 GiB pod at 2 threads gets a `memory_limit` of about 834 MiB (`high`) or 1,214 MiB (`low`), and a 3 GiB pod at 4 threads about 1,605 or 2,035 MiB. The harness merge (1.1M-row chunks) needs more than about 310 MiB at 2 threads and 390 MiB at 4. It runs within an explicit `engine.memory` of 1.5 GiB at 1, 2 and 4 threads in both modes, under a `MemoryMax` cap of the same size (S3 §6.3).
- An explicit `duckdb_share` (in (0, 1]) replaces the formula with `memory × duckdb_share`. There is no default share.
- `channel_bytes` defaults to 256 MB, capped at `memory / 8`.
- `memory: auto` is the tightest `memory.max` (v2) or `memory.limit_in_bytes` (v1) on the process's cgroup path, else `/proc/meminfo` `MemTotal` (on macOS, the `hw.memsize` sysctl), × 0.85. `threads: auto` is `ceil(quota / period)` from `cpu.max` (v2) or `cpu.cfs_quota_us` / `cpu.cfs_period_us` (v1), else `available_parallelism`. A per-replication `engine:` override wins over the project's.
- The resolved budget (values, their sources, `memory_mode` and its reserve, and the parameters) is `atlatl_engine::Budget`; the run report's `engine` carries `memory_limit`, `threads` and `memory_mode` ([10](10-observability.md)).
- `threads` bounds DuckDB threads. Because per-thread parquet and httpfs buffers scale with thread count, this is the lever for the memory DuckDB can't cap.
- **Measured headroom (S3 §6):** with the official build (jemalloc), RSS outside `memory_limit` is roughly independent of the limit. With allocator release it no longer grows across chunks. What remains scales with the chunk's size, so it is fixed per replication, and grows little with threads. `duckdb_share` is therefore not a fixed fraction, hence the formula above. Re-calibrate the constants under cgroup caps with the S3 harness at the pod sizes actually used.
- Large merges are bounded by **chunking** ([04 §7](04-ingest.md#7-chunking)), not by hoping the spill suffices.
- The report records DuckDB's peak `memory_usage` and the process peak RSS for each replication, so tuning is data-driven.
- Explicit overrides are optional (`engine:` at project level or per replication). By default everything is inferred from the container limits. One process runs one Dagster op (a group of replications), so the budget is effectively per op.

## 6. Process lifecycle

1. **Parse CLI.** Install signal handlers. Initialise logging and events.
2. **Load config.** Resolve the environment, render load-time templates, validate against the schema, and resolve the selection (`--replication`, `--stream`, `--tag`).
3. **Plan.** Expand wildcards through discovery, build the plan (a DAG of steps), and run static checks (for example that queue producers are included). `atlatl plan` stops here and prints the plan as JSON.
4. **Engine up.** Create the DuckDB instance with its budget, then load extensions and ATTACH the lake if any replication needs it.
5. **Execute.** Run replications under the scheduler, with each replication committing atomically ([03](03-execution.md)).
6. **Finish.** Write the run report, flush events, and exit with a code that reflects the outcome ([03 §9](03-execution.md#10-exit-codes)).

## 7. Extension seams (stable from v1)

These interfaces are designed wider than v1 needs, so that future sources and larger replications slot in ([11](11-extensibility.md)):

| Seam | v1 implementations | Future |
|---|---|---|
| `Source` (discover, partition, read → engine-native SQL or an Arrow stream) | Postgres, Redshift, files, REST, DuckLake, ClickHouse (HTTP, Parquet) | MySQL, Kafka, SDK sources via the external protocol |
| `ChangeSource` (read changes since a cursor) | DuckLake `table_changes` | Postgres logical replication, Turso `turso_cdc`, MySQL binlog/VStream |
| `Sink` / `Applier` (stage, apply batch, commit with cursor) | DuckLake, plain DuckDB files (ingest only), Postgres, SQLite, D1, Turso | MySQL, ClickHouse, OpenSearch |
| `StateStore`, `LockManager` | lake tables; target-DB tables; PG advisory locks | other catalogs, lease locks |
| `Authenticator`, `Paginator`, `Decoder` (REST) | static/query key, bearer, basic, OAuth2 CC; cursor, page, offset, link, expression; JSON, JSONL, CSV | HMAC signing, SigV4, per-tenant credentials, XML, binary/file downloads |
| **Plan IR**: a DAG of typed steps | `extract → stage → validate` per member, then one commit per replication | multi-step DAGs, fan-out to multiple targets, cross-stream transactions |
