# Testing

What runs by default, what doesn't, and how to reach each tier, both through the cargo aliases and by hand.
The policy behind it is [spec/12-testing.md](spec/12-testing.md) (§5 for CI and live-internet tests).

## Quick reference

| What | Command | Needs |
|---|---|---|
| Everything offline (what CI's `test` job runs) | `cargo test --workspace --all-targets --locked` | Docker, `extensions/` |
| The FRED ports, offline | `cargo test -p atlatl-cli --test fred_ports` | Docker, `extensions/` |
| The FRED ports, plus the live FRED tests | `cargo test-network` | internet; `FRED_API_KEY` for the keyed test |
| Live Redshift | `cargo test-redshift` | `REDSHIFT_*`, a reachable cluster |
| Re-record the FRED cassettes | `cargo test-network -- --ignored record_fred_cassettes` | internet |
| Re-record the dogfood cassettes | `cargo test -p atlatl-cli --test dogfood -- --ignored record_cassettes` (also `--test dogfood_ports`) | nothing (in-process emulators) |
| The memory-budget calibration | `cargo test -p atlatl-ingest --test memory -- --ignored chunked_merge_stays_within_the_budget` | time, RAM |
| Python | `uv run --no-sync pytest` in `python/<package>` | `uv`; most need the `atlatl` binary |

Before the first run:

```sh
scripts/fetch-duckdb-extensions.sh     # DuckDB extensions into extensions/<duckdb version>/
docker version                         # the integration tests start containers through the docker CLI
```

libduckdb itself is downloaded at build time into `target/duckdb-download/` (`DUCKDB_DOWNLOAD_LIB` in
`.cargo/config.toml`); set `DUCKDB_LIB_DIR` to link a local copy instead. Binaries find it through an rpath, never
the system library path.

## Tiers

### Default: offline

A plain `cargo test` never leaves the machine. It runs against:

- **containers** started by `atlatl-testkit` with the `docker` CLI: Postgres 12 and 16, RustFS (S3), SFTP,
  ClickHouse, libsql-server and toxiproxy. They're shared per test binary, publish on random loopback ports, and are
  removed by a reaper process when the test process exits, even if it was killed;
- **in-process fakes** (the Redshift Data API, D1, the dogfood vendor emulators);
- **committed cassettes** for every REST API.

Tests that start no container must also pass with networking off:

```sh
unshare -rn cargo test -p atlatl-cli --test fred_ports api_test_and_schema_check_replay_every_endpoint_offline
```

### `#[ignore]`d by default

| Test | Why it's ignored | Run with |
|---|---|---|
| `dogfood::record_cassettes`, `dogfood_ports::record_cassettes` | rewrites committed cassettes | `--ignored record_cassettes` |
| `memory::chunked_merge_stays_within_the_budget` | 10M-row merge, minutes | `--ignored`; tune with `ATLATL_MEMTEST_{ROWS,CHUNKS,MEMORY_MIB,THREADS,SHARE,MODE}` (`MODE` is `high` or `low`) |
| `fred_ports::record_fred_cassettes` | live, and rewrites committed cassettes and `examples/fred/data/` | `network` feature **and** `--ignored` |
| `fred_ports::live_*` | live | `network` feature |
| `redshift_wire` and `atlatl-conn-redshift/tests/live.rs` live tests | live Redshift | `network` **and** `redshift` features |

### Live tests: the double gate

A live test is gated twice:

```rust
#[cfg_attr(not(feature = "network"), ignore = "calls … (needs --features network)")]
fn live_fred_endpoints() {
    if !network() {          // cfg!(feature = "network")
        return;
    }
```

So `--ignored` / `--include-ignored` alone **doesn't** reach the internet: the test runs, sees the feature off, and
returns early (it passes, having done nothing). Only the feature turns it on. What each invocation does to a live
test:

| Invocation | Live test |
|---|---|
| `cargo test` | `ignored` |
| `cargo test -- --include-ignored` | runs, returns at once, `ok` |
| `cargo test --features network` (with `-p`) | runs for real |
| `cargo test --features network -- --ignored` | also runs the recorders, which rewrite committed files |

A test that also needs a secret skips (with a `skipped: …` line on stderr, and `ok`) when the secret is missing.
Pass `-- --nocapture` to see that line; a quick `ok` can mean "skipped".

The features are per crate, so name the package: `-p atlatl-cli --features network`, or
`--features atlatl-cli/network`.

| Feature | Crate | Turns on |
|---|---|---|
| `network` | `atlatl-cli` | FRED tests in `fred_ports.rs` |
| `network` + `redshift` | `atlatl-cli` | live `redshift_wire.rs` tests |
| `network` + `redshift` | `atlatl-conn-redshift` | `tests/live.rs` |

## Aliases and what they expand to

Defined in [`.cargo/config.toml`](.cargo/config.toml). Arguments after the alias are appended, so filters and
`-- …` work as usual.

```sh
cargo test-network
# = cargo test -p atlatl-cli --features network --test fred_ports

cargo test-redshift
# = cargo test -p atlatl-conn-redshift -p atlatl-cli \
#     --features atlatl-conn-redshift/network,atlatl-conn-redshift/redshift,atlatl-cli/network,atlatl-cli/redshift \
#     --test live --test redshift_wire
```

Common narrowings:

```sh
cargo test-network live_                          # only the live tests, none of the offline ones
cargo test-network live_fred_api -- --nocapture   # the keyed JSON API test, showing a skip if there's no key
cargo test-network -- --ignored record_fred_cassettes
cargo test-redshift --test live                   # just the connector's live tests
```

`cargo test-network` runs the whole of `fred_ports.rs`, the offline tests included, so it needs Docker too. It
doesn't include other live tests that may be added elsewhere; run those with
`cargo test -p <crate> --features network`.

## Secrets and `.env`

Secrets live in a gitignored `.env` at the repo root; [`.env.example`](.env.example) lists them. Neither atlatl nor
cargo reads it. The live tests that need a secret read it themselves: the environment first, then a
`KEY=value` line in `.env`.

| Variable | Read by | Without it |
|---|---|---|
| `FRED_API_KEY` | `fred_ports::live_fred_api_unrate_matches_the_graph_csv` | skips |
| `REDSHIFT_HOST`, `_PORT` (default 5439), `_DATABASE`, `_USER`, `_PASSWORD` | `redshift_wire`, `atlatl-conn-redshift/tests/live.rs` | fails |
| `REDSHIFT_SAMPLE_TABLE` (optional, `database.schema.table`) | `atlatl-conn-redshift/tests/live.rs` | that check is skipped |

The tests' own `.env` reader takes `KEY=value` literally: no quotes, no `export`, no expansion. To get the same
values into a shell (for `atlatl` itself, say):

```sh
set -a; . ./.env; set +a
```

That runs `.env` as shell, so a value with `$`, `;`, spaces or backticks must be single-quoted there, and then
the tests' reader sees the quotes. Keep such values out of `.env`, or export them yourself.

Live Redshift creates and drops `atlatl_test_*` schemas in the connected database. Never point it at production.

## Pointing tests at existing services

Instead of starting containers:

| Service | Variables |
|---|---|
| Postgres 12 / 16 | `ATLATL_TEST_PG12_DSN`, `ATLATL_TEST_PG16_DSN` |
| S3 | `ATLATL_TEST_S3_ENDPOINT`, `ATLATL_TEST_S3_KEY`, `ATLATL_TEST_S3_SECRET` |
| ClickHouse | `ATLATL_TEST_CLICKHOUSE_URL`, `_USER` (default `default`), `_PASSWORD` |

Postgres containers built with custom server files (`PostgresBuilder`) are always started, never replaced by a DSN.
`ATLATL_EXTENSION_DIR` defaults to the nearest `extensions/` holding the pinned DuckDB version.

## Cassettes

REST tests replay committed cassettes (`apis/<api>/cassettes/*.json`). `ATLATL_CASSETTE=<name>` replays a session
and `ATLATL_CASSETTE_RECORD=<name>` records one. The `fred_ports`, `dogfood*` and `aries_example` harnesses clear
both from the inherited environment, so a value in your shell doesn't leak into them.

Offline FRED runs also point `HTTP(S)_PROXY` at `127.0.0.1:9`, so a request that misses the cassette fails instead
of reaching FRED.

Recording redacts registered secrets, sensitive query parameters (`api_key=****`), auth headers and cookies.
Still, read a new cassette before committing it. `fred_ports` asserts that committed FRED cassettes hold no cookies.
The keyed FRED JSON API test has no cassette and is live only.

## Python

Each package under `python/` is its own uv project:

```sh
cd python/<package>
uv sync --locked --no-install-project      # dbt-compat: uv sync --locked
uv run --no-sync ruff check && uv run --no-sync ruff format --check
uv run --no-sync pyright
uv run --no-sync pytest
```

| Package | Binary | Without it |
|---|---|---|
| `dagster-atlatl` | `ATLATL_REAL_BIN` (default `target/debug/atlatl`) for the end-to-end tests; the rest use a fake | those tests skip |
| `dbt-atlatl` | `ATLATL_BIN` (default `target/debug/atlatl`) | the tests that run dbt fail |
| `dbt-compat` | `ATLATL_BIN` (default `target/debug/atlatl`) | fail |

Build the binary with `cargo build -p atlatl-cli`. `ATLATL_EXTENSION_DIR` defaults to `extensions/`.
`dbt-compat` isn't a package (`package = false`): it pins dbt-core, dbt-duckdb and duckdb to the consumer's lock and
checks that dbt reads what atlatl writes and atlatl reads what dbt writes.

## CI

| Workflow | Job | Runs |
|---|---|---|
| [`ci.yml`](.github/workflows/ci.yml), on push to `master` and PRs | `fmt`, `clippy`, `msrv`, `deny` | checks |
| | `test` | `cargo test --workspace --all-targets --locked` (offline, Docker on the runner) |
| | `python` | `dagster-atlatl` |
| | `dbt-compat` | builds `atlatl-cli`, then `dbt-compat` and `dbt-atlatl` |
| [`network.yml`](.github/workflows/network.yml), on push to `master` and manual | `fred` | `cargo test-network` with the `FRED_API_KEY` secret; fails at once if the secret isn't set |
| [`release.yml`](.github/workflows/release.yml) | per platform | smoke-tests the built wheels and tarballs |

Live Redshift never runs in CI. Never on pull requests either: secrets aren't passed to runs from forks.

To run the network workflow by hand: `gh workflow run network.yml`, then `gh run watch`.
