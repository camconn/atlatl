# 12. Testing

The testing goal is confidence that atlatl is reliable on the failure modes that hurt: dropped connections, throttling, decode failures, interruption, commit conflicts, empty sources, NULL keys and snapshot expiry.

## 1. Unit tests (per crate)

- **Config:**
  - parsing, deep-merge precedence, strict template errors, environment resolution;
  - the `read_only` target rule;
  - JSON Schema round-trip.
- **Planner:**
  - wildcard ordering, zero-match handling;
  - queue producer closure (`--with-deps` / `--no-deps`);
  - `after` cycles (a `config` error).
- **Error taxonomy:** each connector's error-mapping table (a native error to its class).
- **Retry and backoff:** wait sequences, header overrides, budgets.
- **REST engine** (against an in-process HTTP mock):
  - every paginator, including no-progress detection;
  - rule ordering and defaults;
  - decode-failure classification: non-JSON 409, no Content-Type, HTML 200, a truncated body;
  - the limiter's `min_interval` shared across setup, iteration and retries;
  - OAuth refresh on expiry and on 401;
  - null/empty parameter omission;
  - exact numbers through queues.
- **Net-change computation** (property tests, `proptest`): for random sequences of inserts, updates, deletes, delete+insert of an unchanged row, full rewrites and compaction (rowid changes), `apply(target@C, diff(C,E)) == project(lake@E)`.
- **Typing:** full-set inference rules, `naming: clean_csv` header cleaning, and cast-failure sampling.
- **Source schemas:** new/missing fields, type/format/null/domain violations, the tolerance, observe vs enforce, and `schema infer` from an all-text table.

## 2. Integration tests (containers, `atlatl-testkit`)

The environment is Postgres **12** and **16** (DuckLake catalog + serving), RustFS (S3), DuckLake attached to both, and SQLite files. D1 is tested against a local HTTP fake that implements the `/query` contract and its limits, and later against a real D1 behind a flag. Turso uses `sqld` locally.

Scenarios:
- Every mode (`full-refresh` with and without a PK, `incremental` with a PK / `update_key` / `window` / neither, `snapshot`) against **pre-existing tables with legacy-loader schemas** (int32 `integer`, `decimal(24,6)`, all-text, `_legacy_loaded_at`), asserting the schema is unchanged.
- `on_empty` mirror, skip and guard.
- NULL and duplicate PK failures.
- Table-drift policies, including `new_columns: fail` on positionally coupled tables.
- Replication atomicity: a failure in one stream rolls back the whole replication; independent replications in the same run still commit; `atomic_group` commits or rolls back together; a multi-hop replication keeps its lake hop when the serve hop fails.
- Decode granularity: `request` vs `row`; rejects recorded, retried on the next run, and resolved once the upstream is fixed; `reject_threshold` enforcement.
- CDC:
  - the bootstrap `adopt` diff;
  - incremental windows;
  - hard and soft deletes;
  - FK-ordered streams in one replication transaction;
  - a dbt `delete+insert` rewrite producing no writes when nothing changed;
  - the retention gap → `cursor_expired`, and `resync`;
  - cursor compare-and-set against a concurrent applier.
- The Redshift UNLOAD path is tested with a fake: SQL assertions plus Parquet written to RustFS by the test. It runs against real Redshift in the staging environment only.

## 3. Fault injection

| Fault | How | Expected |
|---|---|---|
| Source connection drop mid-extract | toxiproxy between atlatl and PG/"Redshift" | only the affected request/query retries; other replications unaffected, no cascade |
| S3 object read stall or truncation | RustFS + toxiproxy | per-object retry (`transient`/`decode`), no whole-request deadline failure |
| DuckLake commit conflict | a concurrent writer committing to the same table during apply | apply retried against staging, one snapshot total |
| SIGTERM / SIGKILL during extract / stage / commit | a signal at controlled points (test hooks) | exits within about a second; nothing half-applied; state unchanged or fully advanced; best-effort report on SIGTERM; exit 143 |
| SIGKILL mid-commit | kill -9 | on restart the target matches either the pre-commit or post-commit state, never between |
| API throttling | the mock returns 409 text/plain, 429 with `Retry-After`, or HTML 200 | backoff, limiter pause, eventual success, or classified failure after the budget |
| Panic inside a replication | a test-only panic hook | that replication fails `internal` and rolls back, independent replications commit, exit code 3 |
| Memory ceiling | a cgroup-limited container running the chunked 66M-row-shaped fixture (synthetic) | completes within budget, and peak RSS is reported |

## 4. Parity harness (migration)

This gates each cut-over.

- **Ingest:** atlatl writes to a shadow schema (`br_x__shadow`) alongside the legacy loader, and a DuckDB diff job compares shadow vs live on:
  - row counts;
  - the PK set;
  - per-column hashes;
  - **column names, order and types** (`information_schema.columns`).

  A cut-over requires N clean days (N is an open question). Before the first of them, `atlatl run --dry-run` against the live tables gives the one-off diff (inserts, updates, deletes with samples) that the review explains row cause by row cause.
- **CDC:** `atlatl cdc bootstrap --mode adopt --dry-run` against the live Postgres gives the diff between lake@S and what the legacy loader wrote. The count of expected differences (for example rows a strict `>` watermark skipped) is reviewed before the switch.
- **REST:** recorded cassettes of each vendor spec are replayed through atlatl, and the extracted records are compared with the legacy-landed rows from the same day.

## 5. CI

- Cargo: `fmt`, `clippy -D warnings`, unit tests, integration tests (docker), and `cargo deny`.
- Python: `ruff`, `pyright`, and pytest for `dagster-atlatl` (subprocess protocol, streamed materializations, termination forwarding, partial failure), and for `dbt-atlatl` (`python/dbt-atlatl`, in the `dbt-compat` CI job since it needs the binary): real `dbt` subprocesses at the consumer's pins, checking that `build`/`run` run atlatl first and other commands don't, that atlatl follows `--target`, that a failed run stops dbt before any model, and that the generated sources stay current.
- Tests pin the same DuckDB and extension versions as the release, and a compatibility test runs the dbt-duckdb version in the consumer repo's lock file against what atlatl writes: `python/dbt-compat` (`uv run pytest`, needing the `atlatl` binary). atlatl lands raw tables, dbt builds on them, and atlatl reads what dbt wrote back, for a plain DuckDB file and for a DuckLake with a DuckDB-file catalog. Its pins move with the consumer's lock and with atlatl's `duckdb` crate.
- ClickHouse tests run against `clickhouse/clickhouse-server` (`atlatl_testkit::clickhouse`, or `ATLATL_TEST_CLICKHOUSE_URL`).

### Live-internet tests

Tests that reach the internet (today the FRED checks in `atlatl-cli/tests/fred_ports.rs`) never run by default, not even under a plain `--ignored`. They sit behind the `atlatl-cli` `network` feature: `#[cfg_attr(not(feature = "network"), ignore = "…")]`, and the body returns early without the feature, so `--ignored` alone doesn't reach the network either. `cargo test-network` (an alias in `.cargo/config.toml` for `cargo test -p atlatl-cli --features network --test fred_ports`) runs them. Recorders that rewrite committed cassettes also keep `#[ignore]`, so they need both: `cargo test-network -- --ignored record_fred_cassettes`. Tests that start no container must also pass with networking disabled (`unshare -rn`); container tests only use the host's loopback.
