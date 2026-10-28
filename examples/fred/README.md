# FRED ports

Public-data analogues of the consumer's M8 ports (spec 13 §2): keyless FRED graph CSVs moved port → parity → cut-over, with local infrastructure only.

| | Replications | Shape |
|---|---|---|
| M8.1a REST | `fred_dff`, `fred_dff_daily`, `fred_series` (`apis/fred.yaml`) | single endpoint; half-year `range()` windows over `cosd`/`coed`, then the stored `sync.floor`; `for_each` over a literal list of series; a reviewed source schema per stream (`schemas/fred/`) |
| M8.2a Postgres | `appdb` | a series catalogue and `fred.obs_*` tables (enums, three `numeric` precisions, a view), sorted wildcard discovery, full refresh by key, mirrored emptiness |
| M8.3a Redshift | `rates_1d` + `rates_1h`, `gdp_1d` + `gdp_1h` | two families, each a daily merge and a frequent delta writing the same tables (one op, one `lock_group`): `update_key` and `window` pushed into the UNLOAD, `svv_columns` precision, UNLOAD → Parquet + manifest, chunked by file group |
| JSON API | `fred_api_unrate` (`apis/fred_api.yaml`, `FRED_API_KEY`) | key in the query string (`auth: query`, redacted as `****`), offset paging (`total_from: count`), FRED's `.` for a missing value turned into null by the records path |
| M8.4a files | `bulk_dgs10`, `bulk_rates`, `bulk_gdp` | a CSV over HTTPS, a zip in S3 and a tarball on disk; member selection, `naming: clean_csv`, `null_values` (FRED's graph CSV leaves a missing value empty, its API writes `.`), `_legacy_loaded_at` kept through `metadata.loaded_at` |

The cassettes in `apis/fred/cassettes/` and the series in `data/` were recorded from FRED together. The tests seed the Postgres app DB, the warehouse fake and the bulk files from `data/`.

## Run it

From this directory, with `atlatl` built (`cargo build -p atlatl-cli`; the binary is `../../target/debug/atlatl`) and the checkout's DuckDB extensions:

```sh
export ATLATL_EXTENSION_DIR=$PWD/../../extensions
mkdir -p .lake/data .lake-parity/data          # the lake (FRED_LAKE) and an empty scratch lake (FRED_PARITY_LAKE)
atlatl validate

# Offline, from the cassettes.
atlatl api test fred series --replay fred_series_run1 --items 4
atlatl schema check fred/series --from-cassette fred_series_run1 --items 4
ATLATL_CASSETTE=fred_dff_daily_run1 atlatl run --replication fred_dff_daily     # the full walk
ATLATL_CASSETTE=fred_dff_daily_run2 atlatl run --replication fred_dff_daily     # from the stored floor
atlatl state show --stream fred/dff_windows
ATLATL_CASSETTE=fred_series_run1 atlatl run --replication fred_series --dry-run      # the diff a run would apply; commits nothing

# Live (a few requests to fred.stlouisfed.org).
atlatl api test fred dff
atlatl run --replication fred_series
atlatl run --replication bulk_dgs10
FRED_API_KEY=… atlatl api test fred_api unrate                  # the keyed JSON API
```

The Postgres, Redshift and bulk-archive replications need their local services, which the tests start (Docker for Postgres and RustFS; the Redshift Data API fake runs in the test):

```sh
cargo test -p atlatl-cli --test fred_ports                         # offline
cargo test-network                                                 # + live_fred_endpoints (2 requests) and, with FRED_API_KEY, the JSON API (~12)
cargo test-network -- --ignored record_fred_cassettes              # re-records, 13 requests
```

Tests that call fred.stlouisfed.org only run with the `atlatl-cli` `network` feature, which `cargo test-network` turns on (spec 12 §5); a plain `cargo test` or `--ignored` never reaches the internet. Re-recording also needs `--ignored`, since it rewrites the committed cassettes and `data/`.

## Parity and cut-over

A stand-in legacy loader writes the lake tables first, as the previous tool would have: text or legacy-style types (`decimal(24,6)`, `DOUBLE`), its own column order, `_legacy_loaded_at`, missing values kept as text, nothing after its pause, and one missed revision. Then each test:

1. runs the port with `atlatl run --dry-run` against the legacy tables (spec 03 §11): staged and validated as a run would, diffed by key with the port's values cast to the legacy column types, nothing committed; it requires a named cause for every insert, update and delete it samples (`--diff-samples` covers all of them);
2. seeds `update_key` cursors with `atlatl state init <stream> --from-target [--member …]`;
3. runs the port onto the legacy tables, which are adopted with the same table ids and column order;
4. dry-runs again, which is clean, and reruns, which commits nothing and reports `up_to_date` (for `metadata.loaded_at` streams, unchanged rows keep their stamp).

A dry run with `--env parity`, whose lake is empty, shows the `CREATE TABLE` a fresh table would get (source precision, enums as text).

## What FRED can't show

The keyless graph CSV has no key, no paging and no request chaining; the JSON API adds a query-string key and offset paging, live only (no cassette). Queue chains (`record_queue`, `paged_queue`, `queue_chain`), OAuth, HMAC signing and the other paginators stay covered only by the synthetic cassettes in `crates/atlatl-cli/tests/dogfood_ports.rs`. FRED's CDN also stalls or resets clients whose User-Agent it doesn't know, so `apis/fred.yaml` and the `fred_files` connection (`headers:`) send a curl-style one, the same shape as the `monthly` port's browser headers.

Source: FRED, Federal Reserve Bank of St. Louis (fred.stlouisfed.org). DFF and DGS10 are from the Board of Governors of the Federal Reserve System, UNRATE from the U.S. Bureau of Labor Statistics and GDPC1 from the U.S. Bureau of Economic Analysis; all are public domain.
