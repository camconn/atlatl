# dbt-atlatl

A [dbt-duckdb](https://github.com/duckdb/dbt-duckdb) plugin for [atlatl](https://github.com/camconn/atlatl):
`dbt build` runs your atlatl replications into dbt's DuckDB file just before dbt opens it, so one
command pulls the raw data and builds on it. The wheel bundles the `atlatl` binary, libduckdb and
the DuckDB extensions it loads; there's nothing else to install.

```sh
pip install dbt-atlatl
```

Linux (x86_64, aarch64; glibc 2.28+) and macOS (arm64).

## Use

Add the plugin to the DuckDB output in `profiles.yml`:

```yaml
analytics:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: warehouse.duckdb
      plugins:
        - module: dbt_atlatl
          alias: atlatl
          config:
            project: atlatl          # the atlatl project, relative to the dbt project
            replications: [vendor]   # optional: also streams, tags, vars, env, timeout
```

Point the atlatl project's target at the same database through the dbt profile, so the path is
written once and `dbt build --target prod` loads prod's file:

```yaml
# atlatl/atlatl.yaml
version: 1
state: {lake: wh}
connections:
  wh: {type: duckdb, dbt: {profile: analytics}}
```

- atlatl runs on `dbt build` and `dbt run` only (configurable with `commands`), not on `parse`,
  `ls`, `compile` or `test`. `DBT_ATLATL_SKIP=1` skips it for one invocation.
- Its log, including progress lines during long extracts, goes to dbt's log as it runs. A failed
  run fails dbt before any model runs.

Generate dbt `sources:` for the tables atlatl lands (and check them in CI with `--check`):

```sh
python -m dbt_atlatl sources --project atlatl --out models/_atlatl__sources.yml --profiles-dir .
```

Reference: [spec 13, dbt integration](https://github.com/camconn/atlatl/blob/master/spec/13-dbt.md)
and [dbt profile connections](https://github.com/camconn/atlatl/blob/master/spec/06-connectors.md#12-connections-from-a-dbt-profile).

## Development

No Rust build is needed: the tests use `ATLATL_BIN` (default `target/debug/atlatl`) and the
extensions from `scripts/fetch-duckdb-extensions.sh`.

```sh
uv sync --no-install-project
uv run --no-sync ruff check && uv run --no-sync ruff format --check
uv run --no-sync pyright
uv run --no-sync pytest
```

## License

AGPL-3.0-or-later. The wheel's third-party notices (DuckDB, its extensions and the Rust
dependencies) are installed as `share/atlatl/LICENSE-THIRD-PARTY`.
