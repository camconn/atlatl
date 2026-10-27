# 06. Connectors

## 1. Model

A connector is a named `type:` that implements one or more capability traits ([11 §2](11-extensibility.md#2-traits)):

| Capability | Meaning |
|---|---|
| `discover` | list objects and columns (wildcards, validation) |
| `read.native` | DuckDB can read it directly (engine-native path) |
| `read.stream` | the Rust source yields Arrow batches |
| `write.lake` | an ingest target (DuckLake only in v1) |
| `apply` | a serving target: staged batch apply + cursor in one commit |
| `changes` | a CDC source |
| `lock` | provides advisory locks |

Connection-level settings every connector accepts:
- `max_concurrency`;
- `read_only`;
- `retry` (defaults for its operations);
- `timeouts {connect, statement, idle}`;
- `environments`.

| Connector | v1 capabilities | Later |
|---|---|---|
| `ducklake` | discover, read.native, write.lake, changes, lock (via its PG catalog) | other catalog types |
| `postgres` | discover, read.native / read.stream, apply, lock | `changes` (logical replication), RDS IAM auth |
| `redshift` | discover, read.native (UNLOAD → Parquet) | |
| `files` (local, s3, https, sftp) | discover (glob), read.native | manifests, Delta |
| `rest` | read.stream ([07](07-rest-api.md)) | |
| `sqlite` | apply | read.stream |
| `duckdb` | write.lake (a plain file, no snapshots), and `for_each.over.query` / `bind` lists ([§10](#10-duckdb-files)) | read.native |
| `clickhouse` | read.stream (HTTP(S), Parquet) ([§11](#11-clickhouse-source)) | CDC sink |
| `d1` | apply | read.stream |
| `turso` | apply | `changes` (`turso_cdc`), embedded replicas |
| `delta` | — | read.native (**M2**, to migrate the Delta `br` tables) |
| `mysql` (incl. PlanetScale) | — | apply, `changes` (binlog / VStream) |
| `clickhouse`, `kafka`, `opensearch` | — | apply (CDC sinks) |

## 2. DuckLake

```yaml
type: ducklake
alias: catalog
catalog: {type: postgres, host, port, database, user, password, sslmode, iam_auth: false}   # or {type: duckdb, path} for local dev/tests
data_path: s3://example-lake/lake/ducklake/
storage:
  type: s3                      # s3 | local
  provider: credential_chain    # credential_chain | config | assume_role
  region: us-east-1
  endpoint: localhost:9000      # S3-compatible stores (RustFS/MinIO): host:port, no scheme
  url_style: path
  use_ssl: false
  key_id / secret / session_token      # for provider: config
  role_arn                             # for provider: assume_role (atlatl calls STS and refreshes)
data_inlining_row_limit: 10
```

**Attach** (once per process; the extensions come from the pre-staged directory, [01 §2](01-architecture.md#2-the-engine-duckdb-in-process)):

```sql
LOAD ducklake; LOAD postgres; LOAD httpfs; LOAD aws;
CREATE OR REPLACE SECRET "atlatl_lake_catalog" (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1', SCOPE 's3://example-lake/');
ATTACH 'ducklake:postgres:host=''…'' port=''5432'' dbname=''…'' user=''…'' password=''…'' sslmode=''require'' connect_timeout=''30'' application_name=''atlatl''' AS "catalog" (DATA_PATH '<data_path>', DATA_INLINING_ROW_LIMIT 10);
```

- **Extensions.** `ducklake` always; `postgres` for a postgres catalog; `httpfs` and `aws` when the data path or storage is S3.
- **Correct quoting.** Every DSN value is single-quoted as a libpq keyword value (`\` and `'` backslash-escaped), and the whole target is an escaped SQL literal. The alias must be a plain identifier (`[A-Za-z_][A-Za-z0-9_]*`). A `type: duckdb` catalog attaches `ducklake:<path>`.
- **Redaction.** The password is registered with the redactor raw, libpq-escaped, and as it appears inside the SQL literal. ATTACH errors echo the DSN (and `duckdb_databases().path` shows it too), so they are always redacted. A wrong password maps to `auth`.
- **`DATA_INLINING_ROW_LIMIT`** is passed on ATTACH when configured. It is a per-attach setting: DuckLake does not persist it in the catalog.
- **S3-compatible stores.** The secret is generated with `ENDPOINT` as `host:port` and explicit `URL_STYLE` / `USE_SSL`. A scheme in `endpoint` is parsed and removed, and it sets `USE_SSL` (`http://` → false, `https://` → true; an explicit `use_ssl` that disagrees is a `config` error). That fixes the RustFS gap: dev can use `data_path: s3://example-lake-dev/…` against the compose RustFS.
  - The secret is `atlatl_lake_<alias>`, scoped to `s3://<data_path bucket>/`. `provider: credential_chain` is the default; `config` needs `key_id` and `secret` (`session_token` optional); `assume_role` is a `config` error until it is implemented. No `storage:` with an `s3://` data path means `credential_chain`; `storage: {type: local}` creates no secret.
- **Data path check.** After ATTACH, the `data_path` stored in the catalog (`ducklake_options(alias)`, option `data_path`) is compared with the configured one, **byte for byte** (a trailing slash counts). A mismatch is a `config` error (code `data_path_mismatch`) that shows both values, and the lake is detached again.
  - DuckLake checks too, but only after normalising trailing slashes: `…/data` and `…/data//` both match a stored `…/data/`. A real difference fails in ATTACH with `Invalid Configuration Error: DATA_PATH parameter "<configured>" does not match existing data path in the catalog "<stored>"`, which maps to the same error.
  - DuckLake stores a new catalog's data path with a trailing slash, so configure it with one.
- **Local paths.** DuckDB creates a missing `type: duckdb` catalog file but not its directory; that ATTACH fails with `Cannot open file "<path>": No such file or directory` (or `Permission denied`), which maps to `config` rather than the `transient` of other IO errors. A missing local data directory is created by DuckLake on the first write.
- **Creating the catalog.** A run's ATTACH creates a catalog that doesn't exist yet: the DuckDB file, or the DuckLake metadata tables in the Postgres database (the database itself must exist). A dry run ([03 §11](03-execution.md#11-dry-run)) never does: it attaches with `READ_ONLY, CREATE_IF_NOT_EXISTS false`.
  - A missing `type: duckdb` file (its directory exists) is checked before ATTACH, since DuckDB can't open a missing file read-only; a file or Postgres database without DuckLake metadata fails that ATTACH with `… does not exist - and creating a new DuckLake is explicitly disabled`. Either way the catalog is missing, which isn't an error: an empty lake with an in-memory catalog is attached under the same alias instead (its data path is in the run's temp directory and is never written), and the run gets a `dry_run_no_catalog` warning naming the catalog.
  - Anything else fails as it would for a run: a missing catalog directory (`config`), a missing Postgres database, auth or network errors.
- **Version check.** The catalog's format version (`ducklake_options` option `version`; `1.0` for DuckDB 1.5.6's ducklake) must equal the one the bundled extension writes, else `config` (`ducklake_version_mismatch`). An older catalog already fails in ATTACH (`DuckLake catalog version mismatch: catalog version is 0.3, but the extension requires version 1.0`, mapped the same way); atlatl never sets `AUTOMATIC_MIGRATION`. Matching dbt's pinned version is a release-time check (the same DuckDB pin), not a runtime one.
- **Snapshot helpers:** the current snapshot (`ducklake_current_snapshot`), whether a snapshot is retained, its `snapshot_time`, the oldest retained snapshot (`min(snapshot_id)` of `ducklake_snapshots`), and the current `table_id` of `schema.table` (`ducklake_table_info` joined to the catalog's `ducklake_schema`).
- **Reads:** snapshot-pinned with `FROM catalog.s.t AT (VERSION => n)` whenever a replication needs consistency across queries.
- **Writes:** explicit transactions. DuckLake's own commit retry (`ducklake_max_retry_count`, default 10; `ducklake_retry_wait_ms`, `ducklake_retry_backoff`) **stays on**: it resolves concurrent commits racing for the next snapshot id, and with it off even commits to disjoint tables fail ([spike S3 §3](../docs/implementation/spike-s3.md#3-commit-conflicts)).
  - What DuckLake still reports is a logical conflict, which is detected per table (not per row): any delete, update or ALTER on a table another transaction also changed. It surfaces as `TransactionContext Error: Failed to commit … Transaction conflict - attempting to … table with index "<table_id>" …`, which maps to `conflict` (code `commit_conflict`); the message names the table (``table `schema.table` (index "<table_id>")``, looked up in the catalog after the rollback, keeping the bare id if the lookup fails).
  - atlatl retries it at the commit step by re-running the whole apply transaction against the retained staging.
- **Lock backend:** advisory locks on the catalog Postgres ([03 §6](03-execution.md#8-locks)); in-process locks for a `type: duckdb` catalog.
- **Maintenance awareness:** see [08 §5](08-cdc.md#5-retention-guard).
- **RDS IAM auth** for the catalog is **v1.x**. It mints a token with `aws-sdk-rds`'s auth token generator and re-attaches before the token expires, as `resources/ducklake.py` does.

## 3. Postgres

### 3.1 Connection

- An unset `sslmode` means `require`: plaintext is never a silent default. Local setups say `sslmode: disable`.
- `host, port, database, user, password, sslmode (disable|require|verify-full), sslrootcert, application_name, statement_timeout, keepalives {idle: 30s, interval: 10s, count: 3}`.
- TLS uses rustls:
  - `require` means TLS without verification, which matches libpq/lib/pq;
  - `verify-full` checks the chain and the hostname against `sslrootcert` (a PEM path, e.g. the RDS CA bundle), or the OS trust store (rustls-platform-verifier) when it is unset;
  - a certificate failure, or a server without TLS under `require`, is a `config` error (not retried).
  - the OS trust store is the only default for every TLS client (Postgres, HTTP, Turso): images need a CA bundle (`ca-certificates`; distroless `static`/`cc` ship one, `scratch` doesn't). On Linux `SSL_CERT_FILE` / `SSL_CERT_DIR` point it elsewhere.
- TCP keepalives are **on by default**, and dead sockets are detected on checkout. That addresses the idle-drop class of failures.
- The session pool has a max size and a checkout timeout (`timeout` class). Checkout drops idle sessions that are closed, and pings those idle for more than a few seconds.

### 3.2 Source

- **Discovery:** `pg_catalog` (`pg_class` with `relkind`), in a read-only catalog session. Base tables (plain and partitioned; partitions are read through their parent, never listed) by default; `include_views: true` adds views and matviews. System schemas are never listed. A pattern without a schema means `public`; a glob in the schema part searches every schema. Members are sorted byte-wise, independent of the server collation. Names containing `.` are skipped.
- **Primary keys of wildcard members.** Each discovered member resolves its own key, first found:
  1. a `members` override on the stream: `members: {<member or glob>: {primary_key: [...]}}`. An exact name wins over a glob, and globs are tried in the order written;
  2. the stream's `primary_key`, which then applies to every member (and turns discovery off);
  3. the member's own primary key constraint (`pg_index.indisprimary`, in key order), read in the catalog session. Views, and tables without one, have none.

  A member with no key uses the no-PK behaviour of its mode (for example, `full-refresh` replaces the table). A discovered key column that `columns` leaves out is a `config` error that names the member. `atlatl plan --discover` lists each member with its key and where it came from (`members`, `stream`, `source`); without `--discover`, the plan shows the stream's `primary_key` and its `members` overrides (`member_primary_keys`). `members` keys are part of the state `config_hash` (05 §4).
- **Read:**
  - The default is engine-native: DuckDB `postgres` extension, `ATTACH … (TYPE postgres, READ_ONLY)` once per unit (detached after it), then `CREATE TEMP TABLE stg AS SELECT <cols> FROM pg.<schema>.<table> WHERE <window/cursor>`. This uses binary COPY underneath, with filter pushdown. `window` is DuckDB SQL here. The attach carries the connection's TLS, keepalives, connect and statement timeouts (`options=-c statement_timeout=…`).
  - `from.query` is Postgres SQL, run through `postgres_query`.
  - **Read-only is enforced for every postgres source**, whatever `read_only` says: the attach refuses engine-side writes, its scans run in server-side `READ ONLY` transactions (so `postgres_execute` or a writing function fails), and the catalog session sets `default_transaction_read_only`.
  - Columns the scanner can't read exactly are converted in Postgres: the member is then read through `postgres_query` with a server-side projection. These are arrays and composites (`to_json(c)::text`), `numeric` without a typmod or wider than DECIMAL(38) (`c::text`; the scanner would read DOUBLE), and geometric types (`c::text`). `columns.include`/`exclude` narrow the projection before anything is read.
  - **Cursor pushdown for converted members.** `postgres_query` gets no filter pushdown, so the `update_key` predicate is added to the server-side `WHERE` when that is safe:
    - the cursor column itself is read natively (not converted);
    - its type compares in Postgres as it does in the engine: `int2/4/8`, `float4/8`, `numeric`, `date`, `time`, `timestamp`, `timestamptz`, or `text`/`varchar` compared `COLLATE "C"` (byte-wise, like DuckDB);
    - the watermark goes in as a quoted literal cast to that base type, and a `lookback` as a quoted `interval` (dates and timestamps only).

    Anything else (a converted cursor column, `bpchar`, `uuid`, enums, a text lookback) keeps the engine-side filter. `window` is DuckDB SQL, so it always applies engine-side on these members. Natively scanned members need none of this, because the scanner pushes the engine's filters itself.
  - The alternative is `read.stream`: tokio-postgres `COPY (SELECT …) TO STDOUT (FORMAT binary)` → Arrow. Use it when a type isn't supported by the extension, or when a cursor predicate needs server-side evaluation the extension can't push down. (Not built.)
  - **Snapshots.** By default (`source_snapshot: per_member`) a replication's members are read one after another in autocommit, so each member is its own Postgres snapshot; they still commit to the lake together. A writer that commits between two members' reads can therefore land in one member and not the other.
  - `source_snapshot: shared` on the replication reads every member under one snapshot:
    - when the unit starts, a session of its own opens `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` and exports its snapshot (`pg_export_snapshot()`);
    - each member's staging read runs in an engine transaction that first adopts it (`CALL postgres_execute(<attach>, 'SET TRANSACTION SNAPSHOT …')`). The extension opens that transaction as `REPEATABLE READ READ ONLY`, and its parallel scan workers import the snapshot of that transaction in turn, so native scans, `postgres_query` and `from.query` all see the same state;
    - the exporting transaction commits once every member is staged, before table locks and the lake commit.

    Holding the snapshot open pins the server's **xmin horizon** for as long as staging takes. `VACUUM` can't remove rows that died after it (on every table, not only the source's), so bloat and, in the worst case, transaction-ID wraparound pressure grow with staging time. Replicas with `hot_standby_feedback` pass the pin on to the primary. Use `shared` when members must agree with each other (foreign keys across tables, totals), and keep those replications small and fast. `idle_in_transaction_session_timeout` on the server must exceed the staging time; if the exporting session dies, adopting the snapshot fails as `transient` (`snapshot_lost`) and the replication retries. `shared` can't be combined with `chunk` (chunks re-read the source inside the commit) and applies to postgres sources only.
- **Errors:** catalog queries map by SQLSTATE (03 §5). Extension errors carry libpq text only and are mapped by message: bad password or role → `auth`; missing database, relation or privilege, and attempted writes → `config`; `statement_timeout` → `timeout`; a refused or dropped connection, or a shared snapshot that is gone (`invalid snapshot identifier`) → `transient`.
- **Type notes for adoption** (matching what legacy loaders wrote to `br.appdb_*`):
  - PG enums → VARCHAR;
  - PG arrays and jsonb → JSON-text VARCHAR, using `to_json()` rather than DuckDB's list-to-VARCHAR formatting, which differs;
  - `numeric(p,s)` → the existing lake column type; for a new table, `DECIMAL(p,s)` when p ≤ 38, else exact text (VARCHAR);
  - `timestamptz` → TIMESTAMPTZ (microseconds); domains read as their text form.

  The adoption parity check ([12 §4](12-testing.md#4-parity-harness-migration)) verifies these per table.

### 3.3 Target (serving applier)

Full apply semantics are in [08 §6](08-cdc.md#6-target-appliers). Connector-level facts:
- **Staging** is a session `CREATE TEMP TABLE` (`pg_temp`), which is invisible to Atlas and to other sessions. It is loaded with binary `COPY FROM STDIN`.
- **Upsert** is `INSERT … ON CONFLICT (pk) DO UPDATE … WHERE (tgt.*) IS DISTINCT FROM (excluded.*)`. **PG 12 compatible**: prod is Aurora PG 12, which has no `MERGE`. The planner verifies that a unique index exists on the PK columns (`pg_index`).
- **Casts:** `columns.<c>.target_type` renders as `stg.c::<type>` in the apply `SELECT`. Enums, `text[]`, jsonb and timestamptz are all declared, with no hand-written `ALTER … USING`.
- **Transaction:**
  - `BEGIN`, then `SET LOCAL lock_timeout`, `statement_timeout` and `idle_in_transaction_session_timeout`, then the apply statements, then the cursor upsert, then `COMMIT`;
  - `serialization_failure` / `deadlock_detected` → `conflict`, retried.
- **Tests** run against **both PG 12 and PG 16** ([12](12-testing.md)).

## 4. Redshift

```yaml
type: redshift
access: data_api                # data_api (default) | wire (PG protocol fallback)
data_api:
  workgroup: analytics          # Redshift Serverless workgroup (or cluster_identifier for provisioned)
  database: warehouse_…
  auth: iam                     # iam (the pod role; temporary credentials via the Data API) | secret_arn
  secret_arn: …                 # when auth: secret_arn
  region: us-east-1
  poll: {wait: 20s, initial: 500ms, max: 10s}   # DescribeStatement long poll (WaitTimeSeconds ≤ 30, 0s = off); backoff when long poll is refused
  endpoint: …                   # optional Data API endpoint override (VPC endpoint, local fake)
  key_id, secret, session_token # optional static caller keys; default: env keys, then IRSA web identity
wire:                           # only for access: wire
  host, port, database, user, password, sslmode, keepalives
unload:                         # optional with access: wire (absent: members are exported over the wire)
  bucket: "{{ env.STORAGE_BUCKET }}"
  prefix: atlatl_unload/
  iam_role: arn:aws:iam::…:role/warehouse-unload        # used by Redshift for UNLOAD
  read_role: same | <arn> | none                         # role atlatl assumes to read the files back
  max_file_size: 256MB          # MAXFILESIZE in MB (bytes / 10^6), clamped to 5–6200
  storage: {…}                  # optional, as for DuckLake: region, endpoint, url_style, use_ssl, static keys
```

**Data API execution** (the default). Every statement (discovery, precision lookup, UNLOAD) is run over the Data API's awsJson1.1 protocol (`POST /`, `X-Amz-Target: RedshiftData.<Op>`) with a hand-rolled SigV4 signer on reqwest; atlatl doesn't link the AWS SDK (`atlatl-conn-redshift`):
- `ExecuteStatement` (`WorkgroupName` or `ClusterIdentifier`, `Database`, `Sql`, `SecretArn` for `auth: secret_arn`, `StatementName = atlatl:<run>:<stream>:<member>` truncated to 500 chars, `WithEvent: false`, and a random `ClientToken` reused across that call's request-level retries, so a retried submit is idempotent), then `DescribeStatement` with `WaitTimeSeconds = poll.wait` (a long poll that returns early on a terminal state) until the statement is `FINISHED`, `FAILED` or `ABORTED`. On `ActiveWaitingRequestsExceededException` it falls back to plain polling with capped backoff.
- Small results (discovery, precision lookup) are read with `GetStatementResult` (paginated). UNLOAD returns no rows.
- **No socket is held** for the life of a long statement. The idle-session drops behind long warehouse streams can't happen, and a pod restart doesn't kill an in-flight UNLOAD's connection.
- **Kill and timeout:** a best-effort `CancelStatement` on the statement id ([03 §9](03-execution.md#9-kill-behaviour)). `CancelStatement` is limited to **3 TPS per account**, so cancels go through a process-wide rate limiter and aren't retried more than once.
- **Errors:**
  - throttling (`ThrottlingException`, `ActiveStatementsExceededException`, `ActiveSessionsExceededException`, `ActiveWaitingRequestsExceededException`) → `throttled`;
  - `ExecuteStatementException`, `InternalServerException` and `DatabaseConnectionException` (HTTP 500), and `FAILED` with a transient Redshift error (a serverless capacity or connection error) → `transient`;
  - SQL errors, `ValidationException`, and a `FAILED` UNLOAD over existing files → `config` / `data` / `internal`;
  - expired credentials → `auth`.
- **Limits to respect** ([spike S8 §1](../docs/implementation/spike-s8.md#1-data-api-facts)):
  - SQL text ≤ 200 KB;
  - ≤ 500 active statements per workgroup (the connection's `max_concurrency` stays far below that);
  - a 24 h statement lifetime, enforced as the task `timeout`;
  - results kept for 24 h;
  - account-wide TPS quotas: `ExecuteStatement` 30, `DescribeStatement` 100, `GetStatementResult` 20, `CancelStatement` 3.
- **IAM needed by the pod role** (exact policy in [spike S8 §2.1](../docs/implementation/spike-s8.md#21-iam-exactly)):
  - `redshift-data:ExecuteStatement` on the workgroup ARN (`arn:aws:redshift-serverless:<region>:<acct>:workgroup/<workgroup-id>`);
  - `redshift-data:DescribeStatement`, `GetStatementResult` (which also covers V2) and `CancelStatement` on `*`, conditioned on `redshift-data:statement-owner-iam-userid`;
  - `redshift-serverless:GetCredentials` on the workgroup;
  - `sts:AssumeRole` on the UNLOAD read-back role (or direct S3 read on the prefix).
- The UNLOAD role trusts `redshift.amazonaws.com` and `redshift-serverless.amazonaws.com`, is associated with the namespace, and needs `s3:PutObject`, `s3:GetObject`, `s3:ListBucket` and `s3:GetBucketLocation` on the prefix (plus KMS for SSE-KMS).

**Extract flow for each member:**
1. **Columns and precision** come from `svv_columns` (`numeric_precision`, `numeric_scale`). They're used for new tables and for validating casts into existing ones.
2. **UNLOAD:**
   ```sql
   UNLOAD ('SELECT <cols> FROM "est_v2"."est_core_cons_af" WHERE <window/cursor/chunk>')
   TO 's3://<bucket>/atlatl_unload/<run_id>/<stream>/<member>/a<attempt>/part_'
   IAM_ROLE '<iam_role>' FORMAT PARQUET MANIFEST VERBOSE MAXFILESIZE 256 MB PARALLEL ON;
   ```
   - The inner SQL is escaped correctly (doubled `'`, and cursor literals rendered for Redshift types). An outer `LIMIT` isn't allowed, so it is nested.
   - Parquet keeps decimal and timestamp types, so there's no CSV round trip and no `\N` marker. **`timestamptz` loses its zone** in Parquet. Columns that `svv_columns` reports as `timestamp with time zone` are read as UTC and cast back to `TIMESTAMPTZ`.
   - The prefix is run- **and attempt**-scoped. Concurrent runs can't collide, and a retry never meets a failed attempt's partial files: UNLOAD fails if files would be overwritten, and atlatl uses neither `ALLOWOVERWRITE` nor `CLEANPATH`.
   - For Parquet, `MAXFILESIZE` is rounded down to a multiple of 32 MB.
3. **Read-back.** atlatl assumes `read_role` with STS, creates a DuckDB secret scoped to the run prefix (refreshed before expiry), reads the manifest (`<prefix>manifest`), and runs `read_parquet([...files])` into staging. It's chunked by file group when `chunk: {by: files}` is set.
   - The row count read back must equal the manifest's `meta.record_count`; a mismatch is `decode`.
   - A zero-row UNLOAD may write no data files, and possibly no manifest ([spike S8](../docs/implementation/spike-s8.md), to be verified). Either case is an empty result with the `svv_columns` schema, not an error.
   - DuckDB opens objects on demand with ranged GETs, so there are no long-held response bodies and no whole-request timeouts. The failure mode behind `HTTP_TIMEOUT: 30m` is gone.
   - A failed or corrupt object is retried per object (`transient` / `decode`).
4. **Cleanup.** The prefix is deleted after commit. A bucket lifecycle rule on `atlatl_unload/` (for example 3 days) is recommended as a backstop, and `atlatl gc` removes orphaned prefixes.

**Wire fallback** (`access: wire`) is for environments without Data API access, such as local proxies like `redshift-proxy.sh`.
- It runs the same SQL over tokio-postgres `simple_query`.
- With `unload`, a connection is held only for the duration of each UNLOAD. Keepalives are on, and every member gets a fresh pooled connection with a liveness check.
- Statement timeouts cancel the query with `pg_cancel_backend` from a side connection. Redshift's `pg_cancel_backend` returns an integer, Postgres's a boolean; both are read.

**Wire export** (`access: wire` without `unload`). No S3 or IAM is needed:
- The member's select (columns, `window` and the `update_key` bound pushed down, as for UNLOAD) is streamed over the extended protocol with **binary** results. Redshift's text format rounds `float8` values computed on the cluster to 15 significant digits whatever `extra_float_digits` says (leader-node constants do honour it), so text would lose precision; binary carries the exact value. Each value is rendered to canonical text (shortest round-trip floats, exact numerics, ISO dates and `+00` timestamps), appended as an all-`VARCHAR` Arrow batch to a temp table, and cast per batch into a temp table typed from `svv_columns`, so a bad value fails at the batch that carries it. Types without a text-compatible binary form are a `config` error.
- **Resume.** Rows are ordered by the member's primary key (`ASC NULLS LAST`) and handed over only at key-group boundaries. A read that fails `transient` (a dropped socket, a terminated backend) keeps the complete groups it received and re-issues the select with `key > last` (lexicographic, NULL-aware), on a fresh session, after the replication's `retry` backoff. `max_attempts` counts consecutive failures without progress, so a long read that keeps dropping still finishes. A member with no key among its columns isn't ordered; a dropped read truncates what was staged and starts over, counting every attempt.
- **Stalls.** A terminated Serverless backend can leave the client socket open and silent. Once rows are flowing, no data for `stall` (default 5 min; time spent appending doesn't count) is a `transient` drop (`export_stalled`): the backend is cancelled best-effort and the read resumes. Before the first row only the statement timeout applies, since a large ordered select can take minutes to start.
- Kills and the statement timeout cancel the backend and aren't retried. `chunk: {by: files}` needs `unload`; other chunking works on the staged rows. Resumes are reported as query-level retries on the member.

**Cross-database names.** A member named `database.schema.table` is read from the connected database with three-part SQL (a datashare consumer database, say). Its columns come from `svv_all_columns` (filtered by `database_name`; precision is also parsed from type names such as `numeric(18,4)`), and wildcards from `svv_all_tables` (`table_type` `TABLE`/`VIEW`). The database part of a pattern can't be a glob, and discovered names keep it.

**Wildcards:** `svv_tables`, base tables by default (views with `from.include_views: true`), filtered by schema equality on the server when the schema part has no glob, then matched (`*`, `?`) and sorted byte-wise on the client, so the order doesn't depend on the server collation. Names containing `.` are skipped; `from.exclude` takes names or globs.

**Implementation decisions (M5, until spike S8 is run):**
- **Credentials.** The Data API caller uses `data_api.key_id`/`secret`/`session_token`, else `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`, else IRSA (`AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN` → STS `AssumeRoleWithWebIdentity`). The read-back base is `unload.storage` keys, else the caller's. `read_role` defaults to `same`: the base assumes `iam_role` with a signed STS `AssumeRole` (1 h sessions, refreshed 10 min before expiry). `none` reads with the base itself. Instance metadata and EKS Pod Identity aren't supported.
- **Escaping.** Redshift treats `\` as an escape in string literals, so literals double both `'` and `\`. The UNLOAD select is embedded with the same rule.
- **Prefixes.** `<unload.prefix><run>/<stream>/<member>/a<attempt>/part_`, with each path segment reduced to `[A-Za-z0-9._-]` (`/` → `_`). A statement that fails `transient`/`throttled` is retried at query level into `a<attempt>r<try>/`. The manifest is `…/part_manifest`. Cleanup after commit deletes the whole member prefix (every attempt).
- **Status mapping.** `ABORTED` that atlatl didn't request is `transient` (`statement_aborted`). A kill or the statement timeout sends one `CancelStatement` (process-wide 3 TPS spacing, at most one retry) and fails `killed` / `timeout`. `FAILED` texts are classified by substring (spike S8 §3 #11 will replace the guesses): a non-empty UNLOAD destination is `internal` (`unload_prefix_not_empty`); S3 slow-downs `throttled`; capacity and connection errors `transient`; missing objects, permissions and syntax `config`; conversions and overflows `data`; anything else `internal`.
- **Read-back.** The manifest is fetched with a signed S3 GET. No manifest is a zero-row result only if the statement reported 0 (or unknown) rows; otherwise, as is a statement row count that differs from the manifest's `meta.record_count`, it is `decode`. Each object is appended into the staging table in its own DuckDB transaction and retried per object; an object whose row count differs from its manifest `record_count` rolls back as `decode`. The projection casts every column to its `svv_columns` type (`numeric(p,s)` → `DECIMAL(p,s)`; Redshift's bare `numeric` is `(18,0)`), and `timestamp with time zone` is restored with `timezone('UTC', CAST(c AS TIMESTAMP))`. `varbyte`, `geometry`, `geography` and `hllsketch` columns are a `config` error (not unloadable to Parquet). A zero-row read creates the staging table from the `svv_columns` types.
- **Runner.**
  - A member's columns (`columns.include`/`exclude`), `window` and `update_key` bound are pushed into the UNLOAD select, so nothing is filtered engine-side. The bound is rendered for Redshift: the stored DuckDB type becomes the Redshift cast target.
  - The UNLOAD is staged per object into `<staging>_raw`, which then goes through the usual staging and validation.
  - With `chunk: {by: files, size}`, each file group's `read_parquet` select is one chunk relation, read inside the commit.
  - `from.query` is not supported for Redshift yet, because its columns can't be typed from `svv_columns`.
  - UNLOAD prefixes are deleted once the unit ends, whether it committed or failed, but not when it was killed.
  - Dropping a waiting statement's future (the runner abandons a killed unit) spawns its `CancelStatement`.
  - Query and object retries follow the replication's `retry`; the statement timeout and wire sessions follow the connection's `timeouts.statement` and `max_concurrency`.
- **`atlatl gc`.** It lists `<unload.prefix>` with ListObjectsV2, groups by run id, and returns run prefixes whose newest object is older than `--older-than`, skipping runs it is told are active. It removes them with per-object `DELETE`s. `atlatl gc` does this for every `redshift` connection with `unload`, resolved in the base environment, honouring `--older-than` and `--dry-run`.
- **Wire.** Sessions come from the Postgres pool, pinged on every checkout (default port 5439, `sslmode: require`). The same catalog SQL (`svv_columns`, `svv_tables`) and UNLOAD run through `simple_query`; a kill or timeout cancels via `pg_cancel_backend(<pid>)` on a side connection and discards the session. Without `unload`, members go through the wire export above (`atlatl-conn-redshift::export`, tested on Postgres 16 with dropped, reset and frozen connections, and live with `--features network,redshift`).

## 5. Files

```yaml
from:
  files:
    url: "s3://bucket/path/{{ vars.date }}/*.csv.gz"   # local path, file://, s3://, https://, sftp://
    format: csv                                         # csv | parquet | json | jsonl (auto from extension)
    archive: {member: "US_FIN_INCOME_STATEMENT.csv"}    # zip/tar member selection (name or glob); never unions members
    csv: {header: true, delimiter: ",", quote: "\"", null_values: [], encoding: utf-8}
    union_by_name: false
    manifest: false                                     # url is a Redshift UNLOAD manifest
    hive_partitioning: false                            # k=v path segments become columns
```

- The read is engine-native (`read_csv` / `read_parquet` / `read_json`). Explicit `columns` become DuckDB `columns={…}`, and `"*": text` becomes `all_varchar=true`.
  - CSV and JSON always stage as text (`infer: safe` runs later, over the staged set). Declared CSV types become `types={…}`; Parquet keeps its own types, and a declared type there is a `CAST` in the staging select.
  - CSV sets `skip=0`, so the sniffer never drops leading rows.
  - JSON numbers pass through DuckDB's double parser, so more than ~17 significant digits aren't exact (REST keeps them exact, §5.2 of 04).
- **Globs** (`url` + `glob`, or a glob in `url`) expand in sorted order. Zero matches is a `config` error; `on_no_match: warn` gives an empty set with a warning. Several files with different columns are an error unless `union_by_name: true`.
- **HTTP(S) URLs** are downloaded with retry and resume into the run temp dir, then read locally, because signed URLs can expire mid-read.
  - Resume uses `Range` with `If-Range` (strong ETag, else Last-Modified); a `200` restarts the body. A body shorter than `Content-Length` is `transient` and retried from where it stopped.
  - HTTP statuses map through the error taxonomy (404 → `config`, 401/403 → `auth`, 429 → `throttled`, 5xx → `transient`). Messages drop the query string, which holds the signature.
  - The connection's `headers` (`type: files`, `headers: {User-Agent: …}`) go with every download, for servers that turn away unknown clients.
- **SFTP** (`sftp://[user@]host[:port]/path`, pure-Rust `russh` + `russh-sftp`) downloads every matched file into the run temp dir, mirroring the remote path (so `k=v` segments survive), then reads the local copies; archives, compression sniffing and `on_no_match` work as for local files.
  - The connection holds the server and credentials; the URL's host, port and user override it, and `sftp:///path` takes the host from the connection:
    ```yaml
    type: files
    sftp:
      host: sftp.example           # port: 22
      user: "{{ env.SFTP_USERNAME }}"
      password: {secret: env:SFTP_PASSWORD}
      private_key: {secret: env:SFTP_PRIVATE_KEY}          # OpenSSH/PEM/PuTTY text, or base64 of it
      private_key_passphrase: {secret: env:KEY_PASS}
      host_key: ["SHA256:…"]       # as `ssh-keygen -lf` prints; else known_hosts
      known_hosts: /etc/atlatl/known_hosts                 # default ~/.ssh/known_hosts
      insecure_skip_host_key_check: false                  # tests only
      connect_timeout: 30s
    ```
  - The server key is always verified unless `insecure_skip_host_key_check: true`: against `host_key` fingerprints when set, else the known_hosts file. A rejected key is `auth` (code `host_key`); host certificates are not supported.
  - Auth tries the private key, then the password. A refused login is `auth` and not retried.
  - Globs expand in Rust over `readdir` from the first path segment with a glob character (`*` stays in one segment, `**` recurses), sorted by path; symlinks are followed. A missing file or glob base directory is zero matches.
  - Connects, listings and downloads retry under the download retry policy (as HTTPS), each retry on a fresh connection. A download resumes from the partial file's length with an offset read; a file whose size changed since the first attempt restarts from zero. A short read or dropped connection is `transient`, a file that vanishes after listing `config`, a permission error `auth`.
- **Archives** are extracted by member (the `zip` and `tar` crates; tar may be gzip/zstd-compressed), and only the selected members are read. A pattern without `/` also matches the member's file name. An archive without `archive.member` is a `config` error. S3 archives are read whole into the run temp dir first.
  - Selecting several members with different schemas is an error unless `union_by_name: true`.
  - This removes the reason the consumer's bulk-statement asset unzips by hand.
- **Compression** (gzip, zstd) is detected by magic bytes for local, downloaded and extracted files, and by extension for S3 objects.
  - Local compressed files are decoded once in full before the read: DuckDB returns a prefix of a torn gzip/zstd stream without error. A torn stream, truncated Parquet or CSV/JSON parse error is `decode`; a missing file is `config`.
- **Null handling:** `null_values` defaults to empty, so nothing is nulled implicitly (DuckDB's default reads empty fields as NULL; atlatl passes `force_not_null` for every column). Adopting the bulk-statement tables sets `null_values: ["NULL"]` (`nullstr`) to match the legacy output; quoted `"NULL"` is nulled too. REST's CSV decoder takes the same key but defaults to `[""]` ([07 §10](07-rest-api.md#10-response-rules-and-decoding)).
- **Header naming:** `naming: clean_csv` for adopted tables ([04 §5.3](04-ingest.md#53-column-names)). It is applied to the raw header cells, so duplicates number as `ticker1`, not as DuckDB does (`ticker_1`).
- **S3 manifests:** `manifest: true` makes `url` a Redshift UNLOAD manifest (`{"entries": [{"url", "meta": {"content_length", "record_count"}}]}`) is the file list, read as `read_parquet([...])`. `chunk: {by: files, size}` groups consecutive entries up to about `size` bytes. When every entry has `record_count`, a staged count that differs is `decode`.
- **Hive-style paths:** `url`, `glob` and `archive.member` render with `vars.*` (`…/date={{ vars.date }}/*.parquet`). `hive_partitioning: true` adds the `k=v` segments as `VARCHAR` columns; it is off unless asked for.
- **v1.x:**
  - `urls_from:` a REST queue, which makes `queue_chain`'s signed bulk links fully native.

## 6. SQLite (target)

All three SQLite-family targets live in `atlatl-conn-sqlite` and share the statement generator, the type mapping, `_atlatl_cursors` and the applier; only the transport differs. The window logic is `atlatl-run`'s `serve_lite` ([08 §6.2](08-cdc.md#62-sqlite-and-turso)).

```yaml
app_sqlite: {type: sqlite, path: /data/app.db, busy_timeout: 5s, create_tables: true}
```

- **Library: `rusqlite` without `bundled`, linked against the system `libsqlite3`** (no C compiled by atlatl; the workspace rule). The applier needs SQLite **3.24** or later (`ON CONFLICT … DO UPDATE`), checked when the connection opens; it uses no `RETURNING` (3.35), `UPDATE … FROM` (3.33) or `IS DISTINCT FROM` (3.39), so the manylinux_2_28 baseline's 3.26 is enough.
  - **rusqlite stays on 0.40**, without its `modern_sqlite` feature. 0.41 dropped that feature and calls `sqlite3_set_clientdata` (3.44), `sqlite3_error_offset` (3.38) and `sqlite3_changes64` (3.37) unconditionally, so it can't link against 3.26, or load on the macOS releases the release targets (11 ships 3.32). Moving past 0.40 needs a newer SQLite shipped with the binary.
  - *Rejected:* DuckDB's prebuilt `sqlite` extension. It bundles SQLite and needs no system library, but writes go through DuckDB's binder (no `ON CONFLICT … DO UPDATE … WHERE` guard into an attached SQLite file, no change counts, no control over the cursor compare-and-set), and it can't serve D1 or Turso. `libsql`'s local mode compiles its own SQLite fork.
  - **Packaging:** the binary links `libsqlite3.so.0` dynamically. It isn't on the manylinux allow-list, so the Linux wheel and tarball **bundle the build image's copy** (AlmaLinux 8's 3.26 in `manylinux_2_28`) in `lib/atlatl/` next to `libduckdb`, found through the same `$ORIGIN/../lib/atlatl` rpath; SQLite is public domain. macOS links the system `/usr/lib/libsqlite3.dylib`, which every macOS ships ([09 §1](09-dagster.md#1-packaging)). Development builds use the host's library; CI's Ubuntu runners need `libsqlite3-dev` at build time.
- The connection runs `PRAGMA journal_mode = WAL; synchronous = NORMAL; foreign_keys = ON` and `busy_timeout` (default 5 s). With foreign keys on, parent-first upserts and child-first hard deletes ([08 §7](08-cdc.md#7-ordering)) are enforced, not assumed. `SQLITE_BUSY`/`LOCKED` → `conflict`; constraint, trigger `RAISE`, type and size failures → `data`; other SQL errors → `config`.
- **Apply** is one `BEGIN IMMEDIATE` transaction with every stream and its cursor. There is no staging table: each chunk of rows is a multi-row `VALUES` statement (at most 500 rows, and `SQLITE_LIMIT_VARIABLE_NUMBER / ncols`):
  - `INSERT INTO t (<cols>) VALUES (…), … ON CONFLICT (<pk>) DO UPDATE SET c = excluded.c, … WHERE (t.c, …) IS NOT (excluded.c, …)`. Values go through column affinity before the guard compares them, so an integer bound into a `REAL` column is unchanged on re-apply. With a managed soft-delete column (`restore: true`, not served) the `SET` adds `deleted_at = NULL` (`= 0` for `value: true`) and the guard compares `t.deleted_at IS NOT NULL` with `0`.
  - Inserts are counted by a `SELECT count(*) … WHERE (<pk>) IN (VALUES …)` of the same chunk before the upsert, whose change count is inserts plus guarded updates; `rows_unchanged = sent − inserted − updated` (08 §11).
  - Deletes: `DELETE FROM t WHERE (<pk>) IN (VALUES …)` (hard), or `UPDATE t SET deleted_at = ? WHERE (<pk>) IN (VALUES …) AND deleted_at IS NULL`, one statement per distinct stamp (`snapshot_time`), `now`, or `= 1 … IS NOT TRUE` (`value: true`).
  - The `_atlatl_cursors` compare-and-set ([08 §6.2](08-cdc.md#62-sqlite-and-turso)) runs last; any error rolls the whole apply back.
- **Type mapping.** Defaults by Arrow type; `columns.<c>.target_type` names another storage mapping: `integer | real | text | blob | json | epoch_s | epoch_ms`.
  - integers and bool → INTEGER (0/1); a `UBIGINT` above 2⁶³−1 is a `data` error;
  - floats → REAL (NaN → NULL, as SQLite stores it);
  - **decimals → TEXT** with their scale (`12.3456`, lossless), or `real` by opt-in;
  - timestamps → ISO-8601 TEXT, fixed width per unit so text order is time order: `2023-11-14T22:13:20.123456Z` (µs; `Z` only for zoned types, naive timestamps get no suffix); dates `YYYY-MM-DD`; times `HH:MM:SS.ffffff`. `epoch_s` / `epoch_ms` store INTEGER seconds or milliseconds (timestamps and dates);
  - strings → TEXT; binary → BLOB;
  - lists, structs and maps → compact JSON TEXT, field order kept; inside JSON, decimals and timestamps are strings (lossless) and binary is base64. A map with non-string keys becomes `[[k, v], …]`;
  - other types (intervals, durations, unions) have no mapping: a `config` error naming the column, fixed with a `transform`.
  - Soft-delete stamps (`snapshot_time`, `now`) and `_atlatl_cursors` times use the same ISO text; a stamp column declared `INTEGER`/`REAL` is a `config` error.
- **`create_tables`** (default `true` on every SQLite-family connection): a missing served table is created from the lake projection's Arrow schema, `CREATE TABLE IF NOT EXISTS t (<col> <INTEGER|REAL|TEXT|BLOB> [NOT NULL on the PK], …, [deleted_at TEXT | gone INTEGER NOT NULL DEFAULT 0], PRIMARY KEY (<pk>))`. With `create_tables: false` a missing table is a `config` error. atlatl never alters an existing table.
- **Plan-time checks** (`PRAGMA table_info` / `index_list` / `index_info`): every served column exists; the primary key or a non-partial unique index covers exactly `primary_key` (the upsert's `ON CONFLICT` target); the soft-delete column exists. Failures are `config`.
- **Objects** are `table` or `main.table`; any other schema is a `config` error.
- **Bootstrap** (`atlatl cdc bootstrap`, [08 §9](08-cdc.md#9-bootstrap-adoption-and-resync)) can't attach the target in DuckDB, so atlatl reads the target back (paged on D1 and Turso), maps lake@S through the same type mapping plus the target columns' affinity, and diffs by primary key in memory; `reload` upserts every row, and the change guard still writes only real differences. The same diff serves `on_gap: resync`. It holds one table's keys and compared columns in memory, which suits serving-sized tables.

## 7. D1 (target)

```yaml
app_d1:
  type: d1
  account_id: 0123abcd…
  database: app                    # or database_id: <uuid>
  api_token: {secret: env:CLOUDFLARE_D1_TOKEN}
  statements_per_request: 1        # >1 sends a `batch` body (S11)
  max_concurrency: 4
  retry: {max_attempts: 5}
```

- **HTTP:** the Cloudflare REST API, via reqwest with a bearer token. Statements go to `POST {endpoint}/accounts/{account_id}/d1/database/{id}/raw` as `{"sql", "params"}` (or `{"batch": [{"sql", "params"}, …]}` when `statements_per_request > 1`); `/raw` returns `{columns, rows}` arrays plus `meta.changes`. `endpoint` defaults to `https://api.cloudflare.com/client/v4` (overridable for a fake or a proxy). `database:` is resolved to its uuid through the *paginated* `GET …/d1/database?name=…&page=…&per_page=100`, matching the name exactly. The body shapes are those of the Cloudflare API reference; the S11 runbook re-checks them against a real database.
- **Parameters** are JSON: integers within ±(2⁵³−1) as numbers and larger ones as strings (INTEGER affinity stores them exactly; JavaScript numbers would not), non-finite reals as `null`. The REST API cannot bind BLOBs, so a `blob` column is a plan-time `config` error.
- **Limits** are enforced client-side:
  - at most 100 bound parameters per statement: upsert chunks are `floor(100 / ncols)` rows, key lists `floor(99 / npk)`;
  - at most 100 KB of SQL per statement (`data` error before sending);
  - at most 2 MB per row (`data` error naming the key);
  - a table with more than 100 served columns fails at plan time (`inline_literals` is not built);
  - the 30 s query duration is D1's; the request timeout is `timeouts.statement` (default 60 s).
- **Transactions.** None across requests, so apply is **idempotent per window** ([08 §6.3](08-cdc.md#63-d1)): mark `pending`, send replayable batches, flip the cursor last. This is the documented exception to single-transaction commits ([03 §2](03-execution.md#2-atomicity)). Whether one `batch` body is atomic is S11; nothing depends on it, and the default of one statement per request assumes nothing.
- **Retry** (per request, `retry:` of the connection): 429 → `throttled` honouring `Retry-After`; 5xx and network errors → `transient`; a non-JSON body → `decode`; 401/403 → `auth`. SQL errors (`errors[].message`) are `data` for constraint and type failures, else `config`, and are not retried. A retried request may re-run statements that already applied; every statement is replayable, and a final cursor flip that finds its work already done (a lost response) re-reads the cursor instead of reporting a conflict.
- **Concurrency:** within one stream's upserts or deletes, up to `max_concurrency` (default 4) requests are in flight; phases and streams stay ordered. There is no rate limiter beyond `Retry-After`.
- Types, `create_tables`, checks and bootstrap follow SQLite (§6). D1 enforces foreign keys.

## 8. Turso / libSQL (target)

```yaml
app_turso: {type: turso, url: libsql://app-org.turso.io, auth_token: {secret: env:TURSO_TOKEN}, transaction_rows: 10000}
```

- **Client:** the crates.io `libsql` crate in remote mode only (`default-features = false, features = ["remote"]`): Hrana over HTTP(S) with an auth token, and interactive transactions over a Hrana stream. It compiles no C of its own (its local mode would build the libSQL fork). Its `tls` feature is off because it pins rustls 0.22; `atlatl-conn-sqlite::https` is the connector instead, on the workspace rustls (ring, OS trust store), offering HTTP/1.1 only. libsql still brings hyper 0.14 (and h2 0.3, never negotiated); a reqwest-based Hrana client is the fallback if that ever matters. `libsql://` means `https://`; `http://` is for a local `sqld`.
- Errors: HTTP statuses map as for D1 (429 `throttled`, 5xx `transient`, 401/403 `auth`); `SQLITE_CONSTRAINT*`/`MISMATCH`/`TOOBIG` → `data`, `SQLITE_BUSY`/`LOCKED` → `conflict`, other SQL errors → `config`. Every call has the `timeouts.statement` deadline (default 60 s).
- **Transactions.** A window of at most `transaction_rows` rows (upserts plus deletes, default 10 000) is one `BEGIN IMMEDIATE` transaction including the cursor, like SQLite. A larger window uses the D1 scheme ([08 §6.3](08-cdc.md#63-d1)): the `pending` mark commits on its own, then the rows go in transactions of about `transaction_rows` rows (whole statements), and the cursor flips in the last one.
- Statements are as for SQLite, up to 500 rows each; types, `create_tables`, checks and bootstrap follow §6. Foreign keys are not switched on (sqld's default is off).
- **Tests** run against `sqld` (`ghcr.io/tursodatabase/libsql-server`, one server per test). Auth tokens (JWTs) are not exercised locally.
- **Later:** embedded replicas; `turso_cdc` as a `changes` source ([08 §10](08-cdc.md#10-future-cdc-sources-into-the-lake)).

## 9. Later connectors (designed for, not built in v1)

| Connector | Notes |
|---|---|
| `delta` (**M2**, `atlatl-conn-delta`) | DuckDB `delta` extension (`delta_scan`, delta-kernel-rs 0.21) for bulk migration of Delta `br` tables into DuckLake. Connection: `{type: delta, root: s3://bucket/br/ \| /local/path, storage: {…}}` (`storage` as for DuckLake, §2; an `s3://` root without it uses `credential_chain`). A delta connection is never a target. Streams select `from.table: <path under root>` (e.g. `throttled/releases`), optionally `from.version: n`; modes `full-refresh` and `incremental` into the lake only. Every run discovers the head version and reads through a pin, `ATTACH '<uri>' AS atlatl_delta_<stream>_<member> (TYPE delta, READ_ONLY, VERSION n)`, so staging, chunks and commit retries see the same data; `delta_scan` has no version parameter and there is no timestamp travel. **Always `READ_ONLY`**: the default attach is read-write and `INSERT` into it crashes the process. The head version comes from globbing `_delta_log/` (commits and checkpoints). New lake tables take the Delta-derived DuckDB types unchanged (S7 type map; `timestamp` → `TIMESTAMPTZ`, `timestamp_ntz` → `TIMESTAMP`); existing tables stay authoritative (04 §6). `update_key` bounds are cast to the column's Delta type so file stats apply; `window:` is pushed into the pinned read. Partition and file-stat predicates skip files, but `IN` lists and expressions don't. Deletion vectors, checkpoints and schema evolution are read correctly. The extension has no CDF reader, so `incremental` means `update_key` with stats pushdown, or the table version as the cursor. Delta CDF (via `deltalake`) only if a live Delta → X feed is needed during migration (`atlatl-conn-delta/tests/delta.rs`) |
| `mysql` / PlanetScale | `mysql_async`; `INSERT … ON DUPLICATE KEY UPDATE`; connection limits; `changes` via binlog (MySQL) or VStream gRPC (PlanetScale) |
| `postgres` `changes` (**M6, built**: `atlatl-conn-pgrepl`) | pgoutput logical replication into the lake, enabled per connection with `changes: {production, slots: {allow_create, temporary}, protocol}`. Slots are **owned**: one slot and one publication per replication, `atlatl_<replication>`, because every slot decodes the whole WAL. There's a `decommission` command (drops both), slot lag (bytes behind `pg_current_wal_lsn()`, `catalog_xmin` age) goes in every report, and a connection marked `production: true` refuses to create or use a slot outside `environments.base` (`config`, `slot_refused`); creating or altering needs `slots.allow_create: true`. See the abandoned-slot incident in the consumer repo's `transactions.md`. Decoding: our own client on `postgres-protocol` with the connector's TLS; protocol v1 (text) on PG 12/13 and v2 with streaming and binary values on PG 14+, chosen by `changes.protocol` (`atlatl-conn-pgrepl/tests/pg/protocol.rs`, [08 §10.1](08-cdc.md#101-postgres-logical-replication-m6)) |
| `clickhouse` (target) | HTTP insert (Arrow/Parquet), `ReplacingMergeTree` or partition replace; CDC sink. The source is built ([§11](#11-clickhouse-source)) |
| `kafka` | `rdkafka`, Avro with a Schema Registry (no auto-register), last-change-per-key collapse, publish-window filter, `--advance-cursor-only` |
| `opensearch` | bulk upsert/delete by id, SigV4 signing, per-field byte truncation |
| `sftp`, `s3` manifests | file transports for SFTP-delivered vendor files, billing-data exports and a reference-data capture feed |

## 10. DuckDB files

```yaml
seed: {type: duckdb, path: "{{ project_root }}/seed.duckdb"}
warehouse: {type: duckdb, path: "{{ env.WAREHOUSE_DUCKDB_PATH }}", alias: warehouse}
```

A local DuckDB database file. It is never a source of streams (`duckdb-role`); it is either:

- **An ingest target.** Any source the lake runner reads (REST, files, Delta, Redshift, Postgres, DuckLake, ClickHouse) can write into it with `full-refresh`, `incremental` or `snapshot`, exactly as into a lake. It is for consumers that keep their own DuckDB file (a dbt-duckdb project with `path:` set to it) rather than a DuckLake. Such a connection can name the dbt target instead of the path ([§12](#12-connections-from-a-dbt-profile)), and dbt can run atlatl itself just before it opens the file ([13](13-dbt.md)).
  - **Attach.** A run attaches the file read-write under `alias` (default: the connection name) when a planned replication targets it or it is the state store; a dry run attaches it `READ_ONLY`. The file must exist: a missing one is a `config` error, never a fresh empty database, so a failed download upstream can't silently restart a consumer's history.
  - **No snapshots.** A commit is one DuckDB transaction. The report's `lake_snapshot` is unset, and a commit that wrote is the change ([03 §2](03-execution.md#2-atomicity)). It can't be a CDC source or a `then:` hop's source.
  - **State.** `state.lake` may name it; the `_atlatl` tables then live in the file ([05](05-state.md)).
  - **Locks.** In-process only ([03 §8](03-execution.md#8-locks)). DuckDB holds an exclusive file lock while attached, so atlatl and anything else that writes the file (dbt) must run one after the other. A file locked by another process fails the attach as `transient` with code `duckdb_locked`.
  - **Conflicts.** Concurrent replications into the same file can race on `CREATE SCHEMA`/`CREATE TABLE`; DuckDB's "Catalog write-write conflict" is a `conflict` and the commit is retried.
- **A read-only list source** for `for_each.over.query` items (endpoint or replication level, [07 §7](07-rest-api.md#7-iteration-queues-and-endpoint-dependencies), [11 §3](11-extensibility.md#3-larger-pipeline-graphs)) and `bind` lists ([04 §12](04-ingest.md#12-bind-lists)), for example a member list with per-member credentials marked `secret`.
  - When the run doesn't write the file, each listing opens it `READ_ONLY` in its own DuckDB database with extension autoloading off, and closes it after the query. A missing file is a `config` error; a file held by a writer elsewhere fails the open (DuckDB's file lock) rather than reading a half-written state.
  - When the run writes it, the listing runs on the run's own connection, inside the file (`USE <alias>` for the query), so a query reads the same either way and sees what earlier replications in the run committed.
- `duckdb` connections are exempt from the environment base rule ([02 §2.1](02-configuration.md#21-environments)): the path is local, and is usually templated per environment.

## 11. ClickHouse source

```yaml
ch:
  type: clickhouse
  dsn: {secret: env:CLICKHOUSE_URI}     # clickhousedb://user:password@host:8443/database
  # or: url: https://host:8443, user, password, database (default `default`)
  settings: {max_threads: "8"}          # extra query settings, sent with every query
  timeouts: {connect: 60s, statement: 10m}
```

- **Transport.** The HTTP(S) interface, via `reqwest` with rustls (01 §4). The query is the POST body; credentials go in `X-ClickHouse-User` / `X-ClickHouse-Key`, never in the URL. A `clickhousedb://` or `clickhouse://` DSN (as `clickhouse_connect` takes it) uses TLS on ports 443 and 8443 or with `secure=true`.
- **Read-only by construction.** Every query carries `readonly=2`, so the server refuses writes (and DDL) whatever the SQL says; settings may still change. A connection's `settings` can't set `readonly`, `wait_end_of_query`, `default_format`, `database` or `max_execution_time` (`config`): atlatl owns those.
- **Results.** `default_format=Parquet` and `wait_end_of_query=1`: the server finishes the query before it sends a byte, so a failure is an HTTP status with `X-ClickHouse-Exception-Code`, never a truncated body. The body streams to a file in a directory of the member's own under the run's workdir, the member reads it with `read_parquet`, and the directory is removed once the unit commits or rolls back. Server defaults for Parquet types differ (ClickHouse Cloud sends `Date` as a `UInt16` day count), so every query also sets `output_format_parquet_date_as_uint16=0`, `output_format_parquet_datetime_as_uint32=0` and `output_format_parquet_string_as_string=1`; a connection's `settings` come after and can override them. The types then map directly: `Date` → `DATE`, `DateTime` → `TIMESTAMPTZ`, `Decimal` → `DECIMAL`, `LowCardinality(String)` → `VARCHAR`, `Nullable(T)` → nullable `T`. An empty result keeps its schema. A body that isn't Parquet (a query with its own `FORMAT`) is `decode`.
- **Streams.** `from.query` or `from.table` (`SELECT *`; `database.table`, split at the first dot, so a table name may contain dots but a database name may not), modes `full-refresh` and `incremental` into a lake or DuckDB file. No `update_key`: incremental filters belong in `from.query`. `bind` lists splice run-time lists into the query ([04 §12](04-ingest.md#12-bind-lists)); a batched list runs one query, and one file, per batch, and all of them stage and commit together.
- **Errors.** By exception code first, since ClickHouse answers most query mistakes with HTTP 500: authentication codes are `auth`; `TIMEOUT_EXCEEDED`/`SOCKET_TIMEOUT` are `timeout`; `TOO_MANY_SIMULTANEOUS_QUERIES` is `throttled`; network, memory-limit, replica and cancellation codes are `transient`; every other code is `config` (for example syntax errors, unknown tables, `READONLY`). An error without a code falls back to the HTTP status (03 §5). The code is kept as `clickhouse_<n>`.
- **Retries and timeouts.** Each query retries under the replication's `retry`. `timeouts.statement` (default 10m) is sent as `max_execution_time`, and the request gets a minute more; `timeouts.connect` defaults to 60s.

## 12. Connections from a dbt profile

A `duckdb`, `postgres` or `redshift` connection can take its settings from a dbt profile target instead of repeating them, so a dbt project and the atlatl project beside it describe a database once. It's opt-in per connection; atlatl never loads profiles on its own.

```yaml
wh:
  type: duckdb
  dbt: {profile: analytics}               # target: DBT_TARGET, else the profile's `target`
  environments:
    prod: {dbt: {target: prod}}
```

- **Lookup.**
  - The file is `dbt.profiles_dir/profiles.yml` (relative to the project root), else `$DBT_PROFILES_DIR`, else `./profiles.yml` when the working directory has one, else `~/.dbt/profiles.yml`: dbt's order.
  - The target is `dbt.target`, else `$DBT_TARGET`, else the profile's `target` (rendered).
  - profiles.yml is read as dbt reads it, including YAML merge keys (`<<: *defaults`, a map or a list of maps; explicit keys win, earlier maps win). atlatl's own files still reject them.
  - The dbt-duckdb plugin ([13](13-dbt.md)) sets `DBT_PROFILES_DIR` and `DBT_TARGET` from the dbt invocation, so atlatl reads the same target dbt is about to open. The lookup reads the process environment even for `plan --static`; the connection's values don't.
- **Mapping.** The target's `type` must equal the connection's.
  - `duckdb`: `path`. `:memory:` and MotherDuck paths are `config` errors. A relative path stays relative to the working directory, as it is for dbt.
  - `postgres`: `host`, `port`, `user`, `password` (or `pass`), `dbname` (or `database`) → `database`, `sslmode`, `sslrootcert`.
  - `redshift`: the same keys, under `wire`. Only password authentication maps; `method: iam` is a `config` error.
  - Everything else (`schema`, `threads`, `extensions`, `settings`, `attach`, `plugins`, …) is dbt's and is dropped.
- **Merging.** The mapped fields are the connection's base. Its own keys, and then its `environments.<env>` block, merge over them ([02 §2.1](02-configuration.md#21-environments)), so an override can change `dbt.target` or any mapped field.
- **Templates.** dbt's `{{ env_var('X', 'default') }}` is rewritten to atlatl's `{{ env('X', 'default') }}`, and its type-hint filters (`as_number`, `as_bool`, `as_text`, `as_native`) are dropped. The values then render and resolve like any connection's: a templated `password` is a secret, and is redacted. A literal password in profiles.yml gets the `literal-secret` warning.
- **Errors** are `config` with the code `dbt`, at the connection's `dbt` key, naming the profiles and targets that do exist. Mapped values keep their profiles.yml spans, so a bad field is reported at its line there.
