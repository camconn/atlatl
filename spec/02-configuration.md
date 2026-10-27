# 02. Configuration

Config is declarative YAML that can be reviewed in git. Terms: replication, stream, connection, source/target, object, mode (`full-refresh` / `incremental` / `snapshot` / `cdc`), primary_key, update_key, api spec, endpoint, `from`, member, run, source schema, rejects. Three principles:

1. **No silent behaviour.** Unknown keys, unresolved template variables, and wildcards that match nothing are errors. The last is configurable per stream as `on_no_match: warn`.
2. **Explicit identity.** Every stream has a stable `id`. Asset keys are declared, never derived from pattern strings.
3. **No process-wide side effects.** There is no `os.Setenv` equivalent. Stream-scoped values are template variables.

## 1. Project layout

```
atlatl.yaml                 # project: environments, engine, connections, state, defaults
replications/**/*.yaml      # one or more replications per file
apis/*.yaml                 # REST API specs (07), referenced by connections
schemas/**/*.yaml           # source schemas (04 §10), matched by stream id
```

`atlatl --project <dir>` sets the project root. The default is the current directory, or `ATLATL_PROJECT`.

## 2. `atlatl.yaml`

```yaml
version: 1

environments:
  names: [dev, prod]
  default: dev
  select_from_env: DBT_PROFILE        # env var holding the environment name; --env overrides it
  base: prod                          # the environment connection base definitions describe (§2.1)
  dev:
    allow_base: [record_queue]        # connections dev may use unchanged from base

engine:                               # see 01 §5
  memory: auto
  threads: auto
  memory_mode: high                   # high (default): faster; low: less RSS outside DuckDB's memory_limit, slower
  temp_directory: /tmp/atlatl/spill

state:                                # see 05
  lake: lake                          # connection that holds ingest state tables (schema _atlatl)
  locks: lake.catalog                 # advisory locks on the DuckLake catalog Postgres

defaults:                             # project-wide stream defaults (lowest precedence)
  retry: {max_attempts: 5, backoff: exponential_jitter, base: 2s, max: 5m}
  on_empty: mirror

connections:
  lake:
    type: ducklake
    alias: catalog                    # attach alias; matches dbt's profiles.yml
    catalog:
      type: postgres
      host: catalog-db.example
      database: ducklake_meta
      user: catalog
      password: {secret: env:PG_PASSWORD}
      sslmode: require
    data_path: s3://example-lake/lake/ducklake/
    storage: {type: s3, provider: credential_chain, region: us-east-1}
    environments:
      dev:
        catalog: {host: localhost, port: 15432, user: postgres, password: password, sslmode: disable}
        data_path: "{{ env.DUCKLAKE_DATA_PATH }}"
        storage: {type: local}        # or s3 with endpoint: localhost:9000, url_style: path, use_ssl: false (RustFS)

  app_pg:
    type: postgres
    host: app-db.example
    database: postgres
    user: app
    password: {secret: env:PG_PASSWORD}
    sslmode: require
    environments:
      dev: {host: localhost, user: postgres, password: password, sslmode: disable}

  appdb:
    type: postgres
    host: appdb.example
    database: appdb
    user: app
    password: {secret: env:PG_PASSWORD}
    sslmode: require
    read_only: true                   # enforced: this connection may never be a target
    # no dev override: dev reads prod (explicit, unlike today's implicit fall-through)
    changes:                          # logical-replication CDC into the lake (08 §10.1)
      production: true                # slots only from environments.base; refused from dev
      slots: {allow_create: true}     # may create its slot and publication (default false)

  warehouse:
    type: redshift
    access: data_api
    data_api: {workgroup: analytics, database: warehouse_…, auth: iam, region: us-east-1}
    read_only: true
    unload:
      bucket: "{{ env.STORAGE_BUCKET }}"
      prefix: atlatl_unload/
      iam_role: arn:aws:iam::123456789012:role/warehouse-unload

  record_queue:
    type: rest
    spec: apis/record_queue.yaml
    secrets: {api_key: {secret: env:RECORD_QUEUE_API_KEY}}
```

### 2.1 Environments

- The active environment comes from `--env`, then the `select_from_env` variable, then `default`.
- A connection's `environments.<name>` block is **deep-merged** over its base definition.
- A connection with no block for the active environment uses its base definition. That is legal only if:
  - the active environment is `environments.base`, the environment the base definitions describe (usually `prod`);
  - or the connection is `read_only: true`;
  - or it is explicitly listed in `environments.<name>.allow_base: [conn…]`.

  Without `base`, every environment needs a block or an exemption. The rule prevents what happens today, where a dev run of a bulk-file asset silently writes to the prod lake.
- `atlatl plan --env dev` prints the resolved connections with secrets redacted, so switching can be checked in CI.

### 2.2 Secrets

A secret value can be written as:
- a literal (discouraged; lint warning);
- a template (`"{{ env.X }}"`);
- a secret reference: `{secret: env:NAME}`, `{secret: file:/path}`, or `{secret: aws-sm:<secret-id>[#json_key]}`.

`aws-sm:` calls Secrets Manager `GetSecretValue` (SigV4 over HTTPS; no AWS SDK). The id is a name or ARN; `#json_key` picks one key of a JSON-object secret. The region is the ARN's, else `AWS_REGION`/`AWS_DEFAULT_REGION`; the endpoint can be overridden with `AWS_ENDPOINT_URL_SECRETS_MANAGER` or `AWS_ENDPOINT_URL`. Credentials come, first found, from `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`[/`AWS_SESSION_TOKEN`], web identity (`AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN`, via STS), the ECS/EKS container credentials endpoint, or the static keys of `AWS_PROFILE` in the shared credentials file; EC2 instance metadata is not used. Each secret is fetched once per process, and 5xx and throttling responses are retried.

Every resolved secret is registered with the redactor ([10 §5](10-observability.md#5-redaction)), and secrets are never written to state, events or reports.

## 3. Replications

A replication has one source connection and one target connection, plus defaults and an **ordered list** of streams. It is the **atomic unit**: its streams commit together or not at all ([03 §2](03-execution.md#2-atomicity)). Replications in one run are independent of each other. By default a replication file maps to one Dagster op, and `op:` groups several replications into one op, run in one process ([09](09-dagster.md)).

```yaml
replications:
  warehouse_hourly_est_consensus:
    source: warehouse
    target: lake
    asset: {group: warehouse_hourly}
    lock_group: warehouse             # shared with the daily replications (see 03 §8)
    defaults:
      mode: incremental
      object: "br.{{ member.table }}"
      window: "cons_start >= current_date - 45"
    streams:
      - id: warehouse_hourly/est_v2/est_core_consf
        asset: {key: br/warehouse_hourly/est_v2/est_core_consf}
        from: {tables: "est_v2.est_core_cons_*f"}
        primary_key: [sym_id, item_code, period_end, cons_start]
      - id: warehouse_hourly/est_v2/est_core_cons_rec
        asset: {key: br/warehouse_hourly/est_v2/est_core_cons_rec}
        from: {table: est_v2.est_core_cons_rec}
        primary_key: [sym_id, item_code, cons_start]
```

Precedence, lowest to highest: project `defaults` → replication `defaults` → stream. Maps are deep-merged, and lists and scalars replace. A stream can clear an inherited value with `null`.

### 3.1 Replication-level keys

| Key | Meaning |
|---|---|
| `source`, `target` | Connection names. DuckLake is always one of the two ([01](01-architecture.md)) |
| `defaults`, `streams` | Stream defaults, and the ordered stream list (§4) |
| `op` | Dagster op name (`[A-Za-z0-9_]+`). Replications with the same `op` run in one subprocess and stay independent, so they must agree on `engine` overrides. The default is the file stem (`replications/warehouse_est.yaml` → `warehouse_est`) |
| `after` | Replications that must succeed earlier in the same run before this one starts |
| `atomic_group` | Opt-in: replications with the same group commit in one transaction or not at all. They must share one target ([03 §2](03-execution.md#2-atomicity)) |
| `lock_group` | A shared exclusive lock across replications (for example the warehouse's 1h and 1d replications) ([03 §8](03-execution.md#8-locks)) |
| `retry`, `replication_retries`, `timeout` | [03 §6](03-execution.md#6-retry). `replication_retries` defaults to 0. `retry` is merged over the project's `defaults.retry` for commit retries, and inherited by the streams beneath their `defaults.retry` |
| `asset` | Shorthand for `defaults.asset` (typically `{group: …}`), merged beneath it |
| `lock_timeout` | Lock acquisition timeout for this replication (default `state.lock_timeout`, else 30m; [03 §8](03-execution.md#8-locks)) |
| `engine` | Overrides for this replication's process: `memory`, `threads`, `duckdb_share`, `temp_directory`, `max_temp_directory_size`, `memory_mode`. Memory and threads are inferred from the container by default, and `memory_mode` is `high` ([01 §5](01-architecture.md#5-memory-model)) |
| `then` | Multi-hop: a list of hops `{target, streams, defaults, asset, then}` that run after this replication commits, for example REST → lake → Postgres. Each hop reads the lake its upstream hop wrote, so the upstream target must be ducklake. Several entries give fan-out, and a hop's own `then` chains further. Hop streams inherit the project `defaults` and the replication's `asset`/`retry` shorthand, but not the replication's `defaults`. Each hop commits on its own, upstream first ([03 §2](03-execution.md#2-atomicity), [11 §3](11-extensibility.md#3-larger-pipeline-graphs)). Not combinable with `atomic_group` |
| `for_each` | Per-tenant or per-partition fan-out: `{over: [items…] \| {query, connection}, as, key}`. It instantiates the replication once per item, as `<replication>[<key>]`. The item is the template variable `as` (plan phase and REST requests), and `key` names the identifying field of map items. Each instance is atomic and independent, with its own state ([11 §3](11-extensibility.md#3-larger-pipeline-graphs)). Not combinable with `then`, `atomic_group` or `cdc` streams |
| `source_snapshot` | Postgres sources: `per_member` (default) reads each member in its own snapshot; `shared` reads every member under one exported snapshot, at the cost of holding back the server's xmin horizon while staging. Not combinable with `chunk` ([06 §3.2](06-connectors.md#32-source)) |

```yaml
replications:
  windowed:
    source: windowed
    target: lake
    streams: [{id: windowed/events, from: {endpoint: events}, object: br.events, mode: incremental, primary_key: [symbol, year, quarter]}]
    then:                                        # multi-hop into serving
      - target: app_pg
        streams: [{id: serve/events, from: {table: br.events}, object: app.events, mode: cdc, primary_key: [symbol, year, quarter]}]
  signed_accounts:
    source: signed
    target: lake
    for_each: {over: {query: "SELECT user_id, secret_ref FROM catalog.au.signed_users", connection: lake}, as: user, key: user_id}
    streams: [{id: signed/accounts, from: {endpoint: accounts}, object: br.accounts, mode: incremental, primary_key: [user_id, id]}]
```

## 4. Stream reference

| Key | Applies to | Meaning |
|---|---|---|
| `id` | all | **Required.** Stable, unique project-wide (`[a-z0-9_./-]+`). It keys state, results and default asset identity. A rename is an explicit migration (`atlatl state move`) |
| `enabled` | all | default `true` |
| `tags` | all | selection labels (`--tag`) |
| `from` | all | **Source selector**, a map with exactly one of the keys below |
| `from.table` | DB, lake, delta | `schema.table`; for a `delta` source, the table's path under the connection `root` ([06 §9](06-connectors.md#9-later-connectors-designed-for-not-built-in-v1)) |
| `from.version` | delta | Read this Delta table version instead of the head |
| `from.tables` | DB | glob pattern (`appdb.*`, `est_v2.est_core_cons_*f`), with `exclude:`, `include_views: false` (default), and `on_no_match: error` (default) \| `warn`. Members expand in **sorted order** |
| `from.query` | DB | SQL |
| `from.endpoint` | REST | an endpoint name in the API spec |
| `from.files` | files | `{url, glob, format, archive: {member}, csv: {…}}` ([06 §5](06-connectors.md#5-files)) |
| `object` | all | Target table (a string), templated with `member.schema`, `member.table`, `stream.id`. It is usually set once in replication `defaults` for wildcard replications |
| `mode` | all | `full-refresh` \| `incremental` \| `snapshot` ([04](04-ingest.md)); `cdc` for lake → serving ([08](08-cdc.md)), or postgres → lake when the source has `changes:` ([08 §10.1](08-cdc.md#101-postgres-logical-replication-m6)) |
| `primary_key` | incremental, full-refresh, cdc | Column list. With `incremental` it means upsert, and without it `incremental` appends. Null PKs fail by default (`null_keys: fail \| skip \| null_safe`). On a postgres `from.tables` stream without it, each member uses its own primary key constraint ([06 §3.2](06-connectors.md#32-source)) |
| `members` | `from.tables` | Per-member overrides, keyed by member name or glob: `{"appdb.audit_*": {primary_key: [ts, seq]}}`. An exact name wins over a glob, then globs in the order written. A member's `primary_key` here wins over the stream's and over the source's own |
| `update_key` | incremental | Watermark column: a string, or `{column, lookback: "2h" \| 0, inclusive: true, nulls: exclude}` ([04 §4.3](04-ingest.md#43-incremental-with-an-update_key-watermark)) |
| `window` (alias `where`) | incremental | Source filter (SQL, templated), e.g. the warehouse's hourly windows |
| `bind` | clickhouse `from.query` | Lists spliced into `from.query` as `{{ bind.<name> }}`: `{<name>: {query, connection, batch}}`, listed when the member is extracted ([04 §12](04-ingest.md#12-bind-lists)) |
| `chunk` | all | `{by: column \| files, size, column}` ([04 §7](04-ingest.md#7-chunking)) |
| `columns` | all | per-column settings (below) |
| `deletes` | cdc | `{mode: soft \| hard \| ignore, column, value, restore}`; the default is soft ([08 §6.0](08-cdc.md#60-delete-modes)). For postgres → lake the default is `hard`, soft deletes set `_atlatl_op = 'delete'`, and `column`/`value` don't apply |
| `history` | cdc (postgres → lake) | `true` appends every netted change (`_atlatl_op`, `_atlatl_synced_at`, `_atlatl_lsn`) instead of upserting by key; default `false` ([08 §10.1](08-cdc.md#101-postgres-logical-replication-m6)) |
| `on_decode_error`, `on_cast_error` | ingest | `{retry: request \| row \| none, reject_threshold, reject_ttl, set_null}`: the retry granularity. `reject_ttl` (a duration, default none) expires old rejects ([03 §7](03-execution.md#7-rejects)). `set_null: true` (`on_cast_error` only) turns a failed cast into NULL with a counted warning. Row mode records rejects ([03 §6.1](03-execution.md#61-decode-failure-granularity)) |
| `on_empty` | full-refresh | `mirror` (default) \| `skip` \| `fail` |
| `guard` | full-refresh | `{min_rows, max_shrink: 0.5}`, a sibling of `on_empty` ([04 §4.1](04-ingest.md#41-full-refresh)) |
| `infer` | ingest | `text` (default for REST/files) \| `safe` ([04 §5](04-ingest.md#5-typing)) |
| `schema` | ingest | Source schema: expected fields and types, enforced every run; `schemas/<stream id>.yaml` (default) or inline ([04 §10](04-ingest.md#10-source-schemas)) |
| `table_drift` | all | `{new_columns: add \| ignore \| fail, type_changes: fail \| widen, missing_columns: warn \| fail}` |
| `metadata` | ingest | metadata columns, e.g. `{loaded_at: {name: _legacy_loaded_at, type: epoch_s_int}}` ([04 §8](04-ingest.md#8-metadata-columns)) |
| `hooks` | all | `{pre_apply: [sql…], post_apply: [sql…]}` on the target session, inside the replication's commit, templated with `staging`, `target`. This is an escape hatch; prefer `columns` |
| `after` | all | Streams in this replication to apply first (FK order) ([08 §7](08-cdc.md#7-ordering)) |
| `retry` | all | Override request/query-level retry defaults ([03 §6](03-execution.md#6-retry)) |
| `asset` | all | `{key, group, deps, description, kinds, tags}`, consumed by `dagster-atlatl` (the op is set at replication level) ([09](09-dagster.md)). `key` defaults to the stream id; `kinds` defaults to `[atlatl, <target type>]` |
| `vars` | all | Stream-scoped template variables, rendered in the load phase and available as `vars.*` / `stream.vars.*` in the plan phase, scoped to the stream rather than the process. `--var k=v` overrides one for a run, as a string, in the plan phase and in what reads `vars` at run time (REST expressions, `from.files`) |

```yaml
- id: org_event/hub
  from: {table: au.org_event}
  object: app.org_event
```

### 4.1 `columns`

```yaml
columns:
  include: [object_id, org_id, type, date, updated_at]            # whitelist
  exclude: [_internal]                                            # alternative to include
  "*": {type: text}                                               # default for unlisted columns (lake target)
  fiscal_year: {type: integer}                                    # lake type for a new table / cast
  type: {target_type: app.event_type}                             # serving-DB native type; cast in the apply
  rating_current:
    transform: "trim(both '_' from regexp_replace(upper(rating_current), '[^A-Z0-9]+', '_', 'g'))"
    target_type: app.rating
  unlinked_names: {target_type: "text[]"}
  dealId: {source_name: "dealId"}                                 # explicit name mapping when needed
```

- `type` is the lake-side (DuckDB) type. It is used when atlatl creates a table or adds a column, and as the cast target during staging.
- `transform` is a DuckDB SQL expression, evaluated in the engine before apply. It can reference other source columns.
- `target_type` is the native type on a serving target. The applier casts to it (`$col::app.periodicity`) inside the apply statement. This replaces every `pre_sql` enum and array recast.
- Whenever a table already exists, **its schema is authoritative** ([04 §6](04-ingest.md#6-existing-tables-adoption-and-table-drift)).

## 5. Templating

- **Language:** minijinja (`{{ … }}`), with `undefined` behaviour set to **strict**. A missing variable is an error at the phase where it is rendered, never silently removed.
- **Phases.** Each templated field declares its phase, and only that phase's namespace is available:

| Phase | Namespace | Fields |
|---|---|---|
| load | `env`, `env(name, default)`, `project_root`, `environment` | connections, `vars`, API spec paths |
| hook | the plan namespace, plus `staging` and `target` | `hooks.pre_apply` / `post_apply` (rendered at apply) |
| plan | the above, plus `member.{schema,table,name}`, `stream.{id,vars}`, `run.{id,started_at}` | `object`, `window`, `from.query`, hooks |
| request | the above, plus `state`, `secrets`, `response`, `record`, `queue`, `sync` | REST spec fields ([07](07-rest-api.md)) |

- **Typed connection fields** (ports, counts, durations, sizes, flags) take templates too, quoted as YAML strings: `port: "{{ env.PG_PORT }}"`. The rendered text is read as a plain scalar of the field's type in the load phase, so `"15432"` is an integer; a result that doesn't fit (`"5432x"`, `70000` for a port) is a `config` error at the template's span. Static checks, which don't render, accept a template there.
- Expressions such as conditions, stop conditions and `over:` use minijinja's **expression** syntax, with short-circuiting and typed comparisons. Custom functions are listed in [07 §9](07-rest-api.md#9-expression-functions).
- Query languages with braces of their own (JMESPath, SQL) are **never** template-rendered implicitly. Variables go into them as bound data ([07 §6](07-rest-api.md#6-records)).

## 6. Validation and tooling

- `atlatl validate` checks schema validity, reference integrity (connections, endpoints, `after`, queue producers), environment completeness, and uniqueness of ids and asset keys.
- `atlatl json-schema <project|replications|source-schema|plan|report|events>` prints the JSON Schemas for editor integration and consumers. `config` is an alias for `project`. `report` and `events` arrive with M0.15.
- `atlatl plan --static` doesn't select an environment, render templates or read secrets. It checks every environment's connection structure and base rule. Every stream needs an explicit `mode`: there is no default.
- Until the REST engine (M3), API specs are read only for dependency analysis (`endpoints.*.{emit, for_each.over.queue, depends_on}`). A queue producer must be read by a stream in the same replication.
- `atlatl plan [--env] [--replication …] [--instance …] --json` prints the fully resolved plan. With `--discover` it expands postgres wildcards against the source catalog, listing each member's target and resolved primary key (other sources' wildcards still need a run); otherwise it lists patterns.
