# 09. Dagster integration

`dagster-atlatl` is a thin Python package. atlatl owns config, execution and results. The package turns atlatl's plan into asset specs, and its run report into materializations.

## 1. Packaging

- There are three artefacts from one release replication:
  - `dagster-atlatl`, platform wheels that **bundle the `atlatl` binary** (maturin `bindings = "bin"`);
  - `dbt-atlatl`, platform wheels with the same binary and data and the dbt-duckdb plugin instead ([13](13-dbt.md)). Each job builds it from the staged `atlatl.data` and checks that its binary and libraries are byte-identical to `dagster-atlatl`'s;
  - a standalone `atlatl` binary tarball for non-Python use.
- Wheels are built for `manylinux_2_28` `x86_64` and `aarch64`, and `macosx` `arm64`. Package and binary versions match exactly.
  - `ATLATL_BIN` overrides the bundled binary for local builds.
  - There is no runtime download, so the Dockerfile warm-up step goes away.
- `libduckdb` (the official release build) and the DuckDB extensions (`ducklake`, `postgres`, `httpfs`, `aws`) ship **inside the wheel**, pinned to the DuckDB version. The extension directory can be overridden with `ATLATL_EXTENSION_DIR`. On Linux the wheel also ships `libsqlite3`, which the binary links dynamically ([06 §6](06-connectors.md#6-sqlite-target)); macOS uses the system one.
- **Layout.** This is one prefix tree, identical to the standalone tarball ([spike S6](../docs/implementation/spike-s6.md)):
  - `bin/atlatl` is the real binary (the wheel's `.data/scripts`, with no Python launcher);
  - `lib/atlatl/libduckdb.{so,dylib}` sits in its own subdirectory, so it never shadows a system `libduckdb`;
  - `lib/atlatl/libsqlite3.so.0` (Linux only) is the build image's SQLite, copied with symlinks resolved: AlmaLinux 8's 3.26 in `manylinux_2_28` (the applier needs 3.24). The binary finds it through the same rpath. From the host it needs only glibc and `libz.so.1` (RHEL's build links zlib, but SQLite imports no zlib symbols). macOS links the system `/usr/lib/libsqlite3.dylib` instead: every macOS ships it (3.32 or later from macOS 11), while Homebrew's is built for the runner's macOS release rather than the 11.0 deployment target and would need its install name rewritten;
  - `share/atlatl/extensions/v1.5.6/<platform>/` holds the extensions.

  The binary's rpath is `$ORIGIN/../lib/atlatl`. Its default extension directory is `<exe>/../share/atlatl/extensions`.
- **Build.** maturin runs with `--auditwheel skip`: its repair step would move the library into `<pkg>.libs/` and wrap the binary in a Python launcher. `auditwheel show` runs as a check instead.
  - Linux wheels build in `manylinux_2_28`, which is the official extensions' baseline, or with `--zig` against glibc 2.28. The container installs `sqlite-devel`, and the job pins both the link and the staged copy to `/usr/lib64` (`SQLITE3_LIB_DIR`, `SQLITE3_NO_PKG_CONFIG=1`, `SQLITE_LIB` for `scripts/stage-wheel-data.sh`, which otherwise stages what `ldconfig` resolves): the image's `PKG_CONFIG_PATH` would pick its private `/opt/_internal/sqlite3`, whose SONAME is `libsqlite3.so` and which adds an absolute rpath. With the repair step skipped nothing is grafted twice, and `auditwheel show` stays a report.
  - macOS arm64 builds on a macOS runner, since there's no SDK cross-link, and thins the universal dylib with `lipo -thin arm64`. It builds with `SQLITE3_NO_PKG_CONFIG=1`, so `libsqlite3-sys` links the SDK's `libsqlite3.tbd` and never a Homebrew keg.
- **Pipeline** (`.github/workflows/release.yml`): one build per platform produces the wheel, and the tarball `atlatl-<ver>-<platform>.tar.gz` is repackaged from that wheel, so both carry the same bytes. Linux x86_64 and aarch64 build natively in the `manylinux_2_28` containers (the aarch64 job on an arm runner). Release builds set `RUSTFLAGS` so the only rpath is `$ORIGIN/../lib/atlatl` (`@executable_path/../lib/atlatl` on macOS). Each job checks the rpath, the glibc ceiling (2.28), the bundled libsqlite3's own dependencies (glibc and `libz.so.1` only; on macOS, that the binary links nothing but `@rpath/libduckdb.dylib` and OS libraries, including `/usr/lib/libsqlite3.dylib`) and the wheel's data entries, then smoke-tests the installed wheel and the unpacked tarball: `atlatl --version`, that `bin/atlatl` resolves `libsqlite3.so.0` to `lib/atlatl/` (`LD_TRACE_LOADED_OBJECTS`), the bundled libduckdb loading every bundled extension with installs and autoloading off (through the C API until `atlatl` has an extension check of its own), and a lake → SQLite serving replication (`cdc bootstrap --mode reload`, then a `run` applying one change), all offline. `delta` is not bundled until S7.
- **Sizes:** 66 MiB (x86_64) and 59 MiB (aarch64) compressed. The installed size is about 190 MB. The Linux libsqlite3 adds about 0.6 MiB compressed (1.1 MB installed).
- **Publishing:** with trusted publishing (OIDC) only, to two registries. `vars.RELEASE_PUBLISH` picks the targets for a tag push (`none`, the default; `pypi`, `crates` or `all`), and a manual run's `publish` input does the same. The build has no dependency on internal-only services.
  - **PyPI** (the `pypi` environment): `dbt-atlatl` only for now; `dagster-atlatl` wheels are built and smoke-tested but not published.
  - **crates.io** (the `crates-io` environment): `atlatl-cli`, which installs the `atlatl` command, and the 15 `atlatl-*` library crates it depends on, all at the workspace version and published one at a time in dependency order by `scripts/publish-crates.py`, which skips versions already on crates.io (so a rerun resumes after a failure) and waits out crates.io's 429s (new crates: a burst of about five, then about one every ten minutes). `atlatl-testkit` is dev-only and unpublished. The prepare job fails if an internal dependency's version isn't the workspace version. Publishing is through trusted publishing (OIDC) only; no API token is stored. crates.io can only attach a trusted publisher to a crate that exists, so a new crate's first version is published by hand with a one-off token, after which the crate gets the trusted publisher and "require trusted publishing". From source, `cargo install atlatl-cli` needs `DUCKDB_DOWNLOAD_LIB=1` (or a libduckdb), and the binary needs that libduckdb and the extensions at run time; the wheels and tarballs ship both.
- **Public-release hygiene** (required before a first public publish):
  - Examples and tests must contain no org-specific config, hostnames or account ids. The consumer's replication config lives in the consumer repo, not in this one.
  - A license file, plus third-party notices for the bundled DuckDB and extensions and the Rust dependencies (`cargo about` / `cargo deny` license checks). DuckDB and the extensions are MIT, but they statically link Apache-2.0 components with NOTICE files (AWS SDK for C++, s2n, Thrift, parquet-format), plus libpq (PostgreSQL licence), libcurl and OpenSSL. All of these licence texts and NOTICEs must ship (`licenses/`, `scripts/fetch-license-texts.sh`), plus SQLite's public-domain dedication for the Linux `libsqlite3` (`licenses/sqlite/LICENSE.md`). They're checked in under `licenses/` (`scripts/fetch-license-texts.sh`, pinned to the audited versions), and `scripts/third-party-licenses.sh` combines them with the `cargo about` output into `LICENSE-THIRD-PARTY`, shipped as `share/atlatl/LICENSE-THIRD-PARTY` in the wheel and at the tarball root.
  - A wheel size budget. The binary plus extensions is expected to be tens of MB, and the bundle must stay under PyPI's per-file limit.

## 2. Component

```yaml
# defs/atlatl_ingest/defs.yaml
type: dagster_atlatl.AtlatlProjectComponent
attributes:
  project: "{{ context.project_root }}/atlatl"
  environment_from: DBT_PROFILE          # dev | prod; passed as --env
  replications: ["*"]                       # or a list; lets migration move one replication at a time
```

- **At definition load**, the component runs `atlatl plan --json --static`. That command does no network discovery and no secrets resolution, only config parsing and validation. It builds one `multi_asset` per **op** from the result:
  - **op** = the replication's `op:` value, defaulting to the replication file name. Replications that share an op run in one subprocess, and they stay independent there ([03 §2](03-execution.md#2-atomicity)). Existing op names are kept (`warehouse_est_consensus`, `queue_chain_history`, …);
  - **assets** = one per stream, keyed by `asset.key`, with `group`, `deps`, `description`, `kinds` (`{"atlatl", <target type>}`), `tags` and `code_version` = the stream config hash. A `then:` hop's streams are assets of the same op, and their target is the hop's target, which sets `kinds`, `dagster/storage_kind` and `dagster/table_name`. A `for_each` replication has one asset per stream, whatever its instance count, and runs whole (atlatl's `--instance` is for manual runs);
  - `can_subset=True`.
- **Environments:** the asset graph is identical in every environment, and the environment only selects connections at run time. That's the same property the current `_resolve_connections` provides, without the rewrite.
- A **Python entry point** covers the bulk-statement pattern (Python pre-processing, then an atlatl load):
  ```python
  @atlatl_assets(project=..., replication="queue_chain_bulk")
  def bulk_load(context, atlatl: AtlatlResource):
      download_bulk_csvs(...)                          # until atlatl-native signed-URL files land (06 §5)
      yield from atlatl.run(context, vars={"bulk_dir": str(BULK_DIR)})
  ```

## 3. Execution

1. **Selection.** The selected asset keys are mapped to replications and stream ids. `--stream` is passed only when the selection is a strict subset of the op's streams, and then lists every selected stream (atlatl narrows every selected replication to the listed streams).
2. **Launch** one process per op:
   ```
   atlatl --log-format json run --env <env> --replication <r>… --stream <id>… --events fd:<N> \
     --report <tmp>/report.json --external-run-id <dagster run id> [--skip-succeeded] [--var k=v…]
   ```
   `<N>` is the write end of the events pipe, inherited at the parent's descriptor number (`pass_fds`), so it is usually not 3. `--env` is omitted when neither `env` nor the `environment_from` variable is set, leaving the choice to atlatl ([02 §2.1](02-configuration.md#21-environments)). `--var` carries the `vars` of `AtlatlResource.run`.
   atlatl adds queue producers in feed-only mode itself ([03 §1](03-execution.md#1-selection-and-planning)), so subsetting `record_queue` `ratings` works.
3. **Events** arrive as NDJSON on the pipe ([10 §3](10-observability.md#3-event-stream)) and are forwarded to `context.log` at a level derived from the type: `stream.progress` is debug; `retrying`, `throttle`, `schema.drift` and `warning` are warnings; a non-successful `replication.completed` is an error. Logs go to stderr as JSON lines and are forwarded at their own level.
4. **Materializations are streamed per replication.** On each `replication.completed` event, one `MaterializeResult` is yielded straight away for each selected stream whose status is `succeeded` or `up_to_date` (so a failed multi-hop replication still materializes its committed lake-side streams). It isn't held until the process exits, so a later replication failing doesn't erase earlier successes.
   - **`for_each` instances** (entries with `instance: {of, key}`) map to their replication's assets. An asset materializes once per step, so these are held until the process exits. Then each stream yields one `MaterializeResult` over the instances that committed it, with rows summed, members named `<key>/<member>`, and `atlatl/instances` (status per key). A failed instance adds to the `Failure` raised in step 5.
5. **Finish.** The report is read once the process exits.
   - If any selected replication failed, the component raises a `dagster.Failure` listing the failed replications, error classes and messages, **after** yielding all successes.
   - For a multi-hop replication whose lake hop committed but whose serve hop failed, the lake-side assets are materialized and the serving assets are failed.
   - The exit codes 2 and 3 map to a `Failure` with the config or internal error.
6. **Termination.** When Dagster interrupts the op (`DagsterExecutionInterruptedError`, run termination), the component sends SIGTERM to the child and then SIGKILL after 5 s. atlatl exits within about a second, and any replication in flight rolls back ([03 §9](03-execution.md#9-kill-behaviour)).

## 4. Materialization metadata

| Key | Value |
|---|---|
| `dagster/row_count` | Rows written: inserted + updated (+ deleted for CDC) |
| `dagster/table_name` | Fully resolved `<connection>.<schema>.<table>` (the member's `object`, prefixed with the target connection when it has fewer than three parts). Omitted for wildcard streams, which list their members in `atlatl/members` |
| `dagster/storage_kind` | `ducklake` / `postgres` / … |
| `atlatl/rows` | `{extracted, inserted, updated, deleted, unchanged}` |
| `atlatl/members` | For wildcard streams, a per-table breakdown (rows, duration, snapshot) |
| `atlatl/cursor` | Before and after (watermark, sync values, or snapshot id) |
| `atlatl/lake_snapshot` | The DuckLake snapshot created (ingest) |
| `atlatl/cdc` | `{from_snapshot, to_snapshot, snapshots_behind, seconds_behind}` |
| `atlatl/rejects` | `{new, resolved, expired, open}` summed over members ([03 §7](03-execution.md#7-rejects)), when any is non-zero; an `up_to_date` stream with open rejects shows it too |
| `atlatl/duration_s`, `atlatl/attempts`, `atlatl/throttle_wait_s` | Per stream |
| asset check `atlatl_schema` | One result per stream with a source schema: passed, or `WARN` / `ERROR` severity with the drift details and samples ([04 §10](04-ingest.md#10-source-schemas)). It is attached to the materialization, or yielded alone for a failed stream. No result when the report has no `schema` section for the stream (e.g. it failed before validation) |
| `dagster/column_schema` | Optional (`include_column_schema: true`), read from lake metadata. No extra CLI call is needed |

## 5. Retries and partial failure

- **atlatl owns retries** ([03 §6](03-execution.md#6-retry)). `op_retry_policy` / `run_retry_tags` remain only as a coarse backstop.
- **Retries never redo successful replications.** `--external-run-id` plus `--skip-succeeded` makes atlatl consult `_atlatl.runs` and skip replications that already succeeded under the same Dagster run id. The component passes `--skip-succeeded` whenever `context.retry_number > 0`.
  - Retrying a warehouse op or the `throttled` op then re-runs only the replications that failed. That also removes the reason the `throttled` replication files were split to save quota.

## 6. Contracts to preserve

The consumer's asset keys, groups, op names and job references must be reproduced byte for byte. The notable ones:
- keys derived today from wildcard patterns (`br/warehouse/est_v2/est_core_consf`) are spelled out explicitly;
- default-derived keys (`target/app/metric`, `target/br/{orgs,rounds}`) are spelled out explicitly;
- `load_metric` selects `target/app/metric` exactly;
- `load_org_event` selects by `key_substring("org_event/load")`.

Fixes that are consumer-side decisions, outside atlatl scope. atlatl can express either the current or the fixed graph:
- the broken dbt source links (`br/warehouse/est_v2/{est_core,est_plus,est_sym}`, `fin_v1/fin_sym`, `br/paged_queue/*`);
- the phantom deps derived from stream names;
- the metric load depending on the phantom `au/metric_feed` instead of `metric_feed`.

## 7. Testing hooks

- `atlatl plan --json --static` is the stable artefact to test against. The consumer's `tests/test_warehouse_replication_split.py` becomes a test on the resolved plan:
  - which asset keys exist, and which op and group each has;
  - that no two replications write the same table outside a shared `lock_group`.
- `dagster-atlatl` ships `assert_asset_keys(plan, expected)` helpers.
