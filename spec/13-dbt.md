# 13. dbt integration

`dbt-atlatl` lets a dbt-duckdb project pull its raw tables with atlatl as part of `dbt build`, in place of a separate extract step. It is a dbt-duckdb plugin plus a sources generator. atlatl still owns config and execution; the package only decides when to run it, and turns the plan into dbt `sources:`.

## 1. Why a plugin

DuckDB lets one process write a database file, and dbt-duckdb holds it from its first connection until the process exits. So atlatl can't run from inside a dbt build (a hook, a macro or a Python model) against the file dbt has open: the attach fails as `duckdb_locked` ([06 §10](06-connectors.md#10-duckdb-files)).

dbt-duckdb builds its plugins (`profiles.yml` → `plugins:`) when it creates its environment, before its first `duckdb.connect`. A plugin's `initialize` is therefore the last moment another process can write the file, and that's where `dbt-atlatl` runs atlatl, to completion, before dbt opens it.

## 2. Configuration

```yaml
# profiles.yml
analytics:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('WAREHOUSE_DUCKDB_PATH', 'warehouse.duckdb') }}"
      plugins:
        - module: dbt_atlatl
          alias: atlatl
          config:
            project: atlatl                 # relative to the dbt project dir
            replications: [vendor]       # also: streams, tags, vars, env, timeout, concurrency
```

```yaml
# atlatl/atlatl.yaml
version: 1
state: {lake: wh}                           # run records and cursors in the same file
connections:
  wh: {type: duckdb, dbt: {profile: analytics}}
```

- The atlatl target names the dbt target ([06 §12](06-connectors.md#12-connections-from-a-dbt-profile)). The plugin sets `DBT_PROFILES_DIR` and `DBT_TARGET` from the invocation, so `dbt build --target prod` lands rows in prod's file. A connection can use `path: "{{ env.DBT_DUCKDB_PATH }}"` instead: the plugin also sets that, to the absolute path dbt is about to open.
- atlatl runs with dbt's working directory and environment, so relative paths and `env_var`s resolve the same way for both.
- `commands` (default `[build, run]`) lists the dbt subcommands that run atlatl. `parse`, `ls`, `compile`, `test`, `show` and the rest open connections too, but don't need fresh data. `DBT_ATLATL_SKIP=1` skips the run for one invocation.
- dbt-duckdb merges the profile's `settings:` into every plugin's config, so the plugin ignores keys it doesn't know rather than rejecting them.
- The binary is `ATLATL_BIN`, else the plugin's `binary`, else the one the wheel installed, else `atlatl` on `PATH`. The extension directory is `extension_dir`, else `ATLATL_EXTENSION_DIR`, else the wheel's.

atlatl's log goes to dbt's log line by line as atlatl writes it (the plugin runs it with `--log-format text`), so a long extract's periodic progress lines show while dbt waits.

## 3. Failure

The plugin raises unless the run report's `status` is `succeeded`. dbt-duckdb then fails to open its connection, so no model runs on a partial ingest. The error carries each replication's status and error message, or the last lines of atlatl's log when there was no report (a `config_error` before the run).

A `--select` that touches no source still runs the configured replications. Narrow them with the plugin's `replications`, `streams` or `tags`, or skip with `DBT_ATLATL_SKIP`.

## 4. Sources

```sh
python -m dbt_atlatl sources --project atlatl --out models/bronze/_atlatl__sources.yml
python -m dbt_atlatl sources --project atlatl --out models/bronze/_atlatl__sources.yml --check   # CI
```

dbt parses sources before the plugin runs, so they're generated ahead of time from `atlatl plan --static --json` and committed, and `--check` keeps them current.

- There is one source per schema, named after it (`--prefix` prepends to the name), with one table per stream object written to a `duckdb` target (`--connection` limits which). Each table's `meta.atlatl` carries `replication`, `stream`, `asset_key` and `mode`, and its `description` is the stream's.
- `database` is omitted, so it's dbt's own database; set `--database` otherwise. A three-part object drops its catalog, which is atlatl's attach alias.
- Objects templated on `member.*` are rendered for each listed member. A wildcard stream's members are only known at run time, so it's skipped with a warning. So are feed-only streams, and a second stream writing the same table.
- `--profiles-dir` and `--target` are passed to the plan as `DBT_PROFILES_DIR` and `DBT_TARGET`, for `dbt:` connections.

## 5. Packaging

`dbt-atlatl` is a platform wheel built like `dagster-atlatl` ([09 §1](09-dagster.md#1-packaging)): the same binary, libduckdb, libsqlite3 and extensions under the same prefix layout, from the same staged `atlatl.data`. maturin won't put a console script in a wheel that ships a binary, so the generator is `python -m dbt_atlatl`. The package depends on `dbt-duckdb`; its dev pins follow the consumer's lock, as `python/dbt-compat` does.

dbt-duckdb's own `duckdb` and atlatl's libduckdb are separate libraries. They must stay on the same DuckDB storage version, or one can't open the other's file ([12](12-testing.md): `dbt-compat` checks this).
