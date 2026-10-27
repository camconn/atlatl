# 11. Extensibility and future work

v1 is small. This doc defines the seams that keep it small **without** closing off:
- future sources, most of which are hand-written Python today and land in Delta;
- future sinks;
- larger, more intricate pipeline graphs.

## 1. Principles

- **Rust only, single binary.** Connectors are compiled in behind cargo features. There is no dynamic plugin ABI and no subprocess or external-code source. Supporting a source means porting what it needs to Rust.
- **Vendors are YAML only.** A new REST vendor is an API spec. A capability YAML can't express becomes a core Rust feature (an auth type, paginator, decoder or transport) that YAML then configures. There is no per-vendor Rust code.
- **Shared behaviour lives in the core.** The core owns:
  - status and failure policy;
  - rate limiting;
  - redaction;
  - decoders (JSON, CSV, archives);
  - retry and error taxonomy;
  - rejects;
  - state;
  - locks;
  - reporting.

  Together these cover more than two-thirds of the surveyed sources.
- **Out of reach until ported:** scraping-style sources (yfinance, yt-dlp, the podcast chain) stay in Dagster Python. They move only once the capability they need exists in the core.

## 2. Traits

The shapes below are sketches. The exact signatures get settled during implementation.

```rust
trait Source {
    fn capabilities(&self) -> SourceCaps;                                   // native | stream, discover, pushdown
    async fn discover(&self, sel: &Selector) -> Result<Vec<Member>>;
    async fn plan(&self, member: &Member, req: &ReadRequest) -> Result<Vec<ReadPartition>>;  // chunks
    async fn read(&self, part: ReadPartition, cx: &StepCx) -> Result<ReadOutput>;
}
enum ReadOutput {
    Native(NativeScan),                  // SQL/table function DuckDB executes (read_parquet, postgres_scan, delta_scan, …)
    Stream(BoxStream<Result<RecordBatch>>),
    Json(BoxStream<Result<JsonRecord>>), // staged as raw JSON, typed in SQL
}

trait ChangeSource {                      // DuckLake table_changes; later pgoutput, turso_cdc, binlog
    async fn head(&self) -> Result<Position>;
    async fn changes(&self, from: &Position, to: &Position, proj: &Projection) -> Result<ChangeSet>;
    async fn check_retained(&self, from: &Position) -> Result<Retention>;
}

trait Applier {                           // Postgres, SQLite, D1, Turso; later MySQL, ClickHouse, Kafka, OpenSearch
    fn capabilities(&self) -> ApplyCaps;  // transactional, max_batch, supports_cursor_cas, …
    async fn begin(&self, window: &Window) -> Result<Box<dyn ApplyTxn>>;
}
trait ApplyTxn {                          // one per replication (per target) — all streams apply inside it
    async fn upserts(&mut self, stream: &StreamId, batches: BoxStream<RecordBatch>) -> Result<Counts>;
    async fn deletes(&mut self, stream: &StreamId, keys: BoxStream<RecordBatch>) -> Result<Counts>;
    async fn commit(self: Box<Self>, cursors: Vec<CursorCas>) -> Result<()>;
}

trait StateStore { … }   trait LockManager { … }
trait Authenticator { … } trait Paginator { … } trait Decoder { … } trait Transport { … }   // REST/file seams
```

- `ChangeSource` for pgoutput is **forward-only**. `changes(from, to)` streams from the slot's `confirmed_flush_lsn`, and asking for an older `from` silently starts there instead. The trait therefore also needs `confirm(&Position)`, called after the lake commit, and `check_retained` compares the slot's confirmed position with the stored cursor (`atlatl-conn-pgrepl/tests/pg/resume.rs`).
- A connector declares its **error mapping** into the taxonomy ([03 §5](03-execution.md#5-error-taxonomy)). Unmapped errors are `internal`.
- The **Kafka, ClickHouse and OpenSearch** sinks fit `Applier` directly. The Python code for them today already consumes Delta change feeds with append, partition-replace, upsert-by-id and last-change-per-key semantics.

## 3. Larger pipeline graphs

**The plan IR** is a DAG of typed **steps** from day one. The early milestones only build simple shapes: `Extract(member) → Stage → Validate`, then one `Commit` per replication and target. The IR also carries edges for `after`, queues and multi-hop.

**In scope.** (a)–(c) are built (M8.6); (d) is built in the executor ([03 §2](03-execution.md#2-atomicity)).

| Shape | How it maps | Motivation |
|---|---|---|
| (a) Multi-hop | `then:` on a replication ([02 §3.1](02-configuration.md#31-replication-level-keys)): a lake `Commit`, then serve steps, then a serving `Commit`. Each hop is atomic, and the lake hop stays committed if a later hop fails ([03 §2](03-execution.md#2-atomicity)) | land `br` and serve it in one run |
| (b) Fan-out | several `then:` entries below one hop. Every entry reads what its upstream hop committed, so one extract feeds the lake and any number of serving tables | one vendor call feeding the lake and a hot serving table |
| (c) Per-tenant / per-partition fan-out | `for_each:` on a replication instantiates independent atomic instances | `signed` per user, Dagster partitions |
| (d) Cross-replication transactions | `atomic_group` on replications that share a target: one transaction | replications that must land together |

Each shape is a new step kind or edge type. The scheduler, retry, locks and reporting don't change.

**Multi-hop and fan-out (a, b).**
- **Plan.** A multi-hop replication is still one replication and one scheduling unit. Its plan lists `hops` (`{hop, parent, source, target, streams}`), and each stream carries its `hop`. Every hop has its own `apply` steps and its own commit step (`commit` for hop 0, `commit:<n>` after that). A hop's extracts depend on the commit of its nearest planned upstream hop.
- **Targets.** A hop's source is its upstream hop's target, and that target must be `ducklake`. The hop target can be any connection a lake-sourced replication can write, so each hop runs through the runner for its own source → target pair. That pair is currently a serving `cdc` target or a lake → lake copy, and the new serving appliers slot in unchanged. A serving hop needs `atlatl cdc bootstrap <replication>` like any serving replication; bootstrap covers every lake → postgres hop of the replication.
- **No direct staging-to-serving path.** A serving hop reads the change feed of what the lake hop committed; it never reads the lake hop's staging tables. "One staged extract feeding several targets" therefore means one extract, one lake commit and then any number of hops. Writing a serving table without the lake would break the rule that DuckLake is one end of every replication ([01](01-architecture.md)); the one exception is ingest into a plain DuckDB file ([06 §10](06-connectors.md#10-duckdb-files)), which is a lake-like target with no change feed.
- **Execution.** Hops run in plan order (depth-first, so parents come before children), and each one commits on its own.
  - If a hop fails, its descendants are `not_started`, while its siblings and their subtrees still run.
  - The replication fails with its first failed hop's error, and each failed hop records its own error.
  - A `replication_retries` retry re-runs only the hops that haven't committed in this run.
  - `hop.committed` is emitted after each hop commits ([10 §3](10-observability.md#3-event-stream)).
- **Static checks.**
  - an unknown or read-only hop target;
  - a hop whose upstream target isn't the lake;
  - `then` combined with `atomic_group` (the hops commit separately);
  - a stream's `after` naming a stream in another hop;
  - data-flow cycles across hops, where a hop reads a table that it or a downstream hop writes (literal table names only);
  - the usual per-stream checks, run against each hop's own source and target.

  A hop stream reading a table that its upstream hop doesn't write gets the warning `then-source`.
- **Selection.** `--stream` narrows every hop. A hop with no selected streams is skipped, and the remaining hops wait only on planned upstream hops.

**Per-tenant fan-out (c).** `for_each: {over: [items…] | {query, connection}, as: <name>, key: <field>}`.
- **Items.** Scalar items are their own instance key. Map items need `key`, which names the field that identifies the instance.
  - A literal list expands when the plan is built.
  - A query runs on a lake (in the run's engine) or a postgres connection when the run starts, and the replication is then replanned with one instance per row. A row with one column is a scalar item; a row with several columns is a map.
  - Static plans (the `dagster-atlatl` input) never expand. They list the replication once, with its `for_each`.
- **Instances.** Each instance is its own replication, `<replication>[<key>]`, carrying `instance: {of, key, as, item}`. It is an independent atomic unit for scheduling, `replication_retries`, `--skip-succeeded` and the report.
  - `after: [<replication>]` waits for every instance.
  - The item is bound under the `as` name in plan-phase templates (`object`, `window`, `from.*`) and in REST request templates.
- **State.** An instance stores its watermarks, `sync` values and rejects under `<stream id>@<key>`, so the instances don't share cursors. `atlatl state` addresses it as `<stream>@<key>` or `<stream> --instance <key>`; a bare stream id covers every instance ([05 §5](05-state.md#5-cli)).
- **Selection.** `--instance <replication>[<key>]` (repeatable; `plan` and `run`) runs only the named instances, and only their replications are selected. With `--replication`, each instance's replication must be among them. It is checked like the other selection flags:
  - a malformed name, an unknown replication, or one without `for_each` is a plan-time `config` error;
  - so is a key missing from a literal list;
  - a query iteration is listed when the run starts, and a key missing from its rows fails that replication (`config`, `unknown_instance`, naming the listed keys) without running any of its instances.

  `validate` checks the whole project and takes no selection. `dagster-atlatl` never passes `--instance`: a `for_each` replication is one set of assets, and a retry skips the instances that succeeded (`--skip-succeeded`).
- **Empty or failed listings.** A query that returns no rows leaves the replication `up_to_date`. A query that fails fails only that replication.
- **Static checks.**
  - `for_each` can't be combined with `atomic_group` or `then`, or with `cdc` streams (serving cursors and replication slots are kept per stream);
  - `as` must be an identifier that isn't a template root;
  - a literal list must be non-empty and have unique keys;
  - a query connection must be a lake, postgres or duckdb connection;
  - a `full-refresh` stream must template its `object` with the item, because otherwise every instance would replace the others' rows.

**Not planned:** continuous or streaming execution, distributed or multi-pod runs, and non-Rust extension mechanisms. atlatl stays a CLI that runs to completion.

## 4. Backlog

Open work, with what blocks it:

| Item | Spec | Status |
|---|---|---|
| Redshift Data API against real Serverless: UNLOAD + MANIFEST format, error texts, IAM, memory at 66M rows | [06 §4](06-connectors.md#4-redshift) | Blocked on a non-production AWS account; runbook in [spike S8](../docs/implementation/spike-s8.md). Built and tested against `redshift_fake` |
| D1 REST `/query` multi-statement atomicity and body shapes | [08 §6.3](08-cdc.md#63-d1) | Blocked on a scratch Cloudflare account; runbook in [spike S11](../docs/implementation/spike-s11.md). The idempotent apply is correct either way |
| Delta capture-style incremental with deletes (only files added since a version; drop rows the lake removed) | [04](04-ingest.md), [06 §9](06-connectors.md#9-later-connectors-designed-for-not-built-in-v1) | Not started |
| Templated enums and template-aware editor JSON Schemas | [02 §5](02-configuration.md#5-templating) | Not started |
| Consumer cut-overs (REST vendors, `appdb`, `warehouse`, bulk files) | [12 §4](12-testing.md#4-parity-harness-migration) | atlatl side done; parity and cut-over need staging/prod |
| Publishing beyond PyPI: the `atlatl-cli` crate (and the `atlatl-*` crates it needs) on crates.io with `cargo binstall` metadata, and a GitHub Release per tag with the standalone tarballs, `SHA256SUMS`, `LICENSE-THIRD-PARTY` and provenance attestations. `dbt-atlatl` from PyPI already installs the `atlatl` binary | [09 §1](09-dagster.md#1-packaging) | Deferred: PyPI `dbt-atlatl` only for now. A plain `cargo install` would also need libduckdb (`DUCKDB_DOWNLOAD_LIB=1`) and the extensions fetched |
| Progress for the other sources and appliers: Postgres, Redshift, ClickHouse, files, Delta and DuckLake extracts, CDC serving. Today only REST streams report it (status line, periodic log line, `stream.progress` with `http`) | [10 §2](10-observability.md#2-logs), [10 §3](10-observability.md#3-event-stream) | Not started. REST's `progress::Board` and `Live` counters are the pattern: rows/bytes/chunks per member, totals where the source knows them |

Further core extension points, added as sources need them:

| Extension point | Spec |
|---|---|
| Per-request status/failure policy, skip guard | [07 §10](07-rest-api.md#10-response-rules-and-decoding) (ships with the REST engine) |
| Shared rate limiter honouring headers and budgets | [07 §8](07-rest-api.md#8-rate-limiting) (ships with the REST engine) |
| Non-JSON decoders: CSV, zip/tgz members, gzip JSONL, XML | [07 §10](07-rest-api.md#10-response-rules-and-decoding), [06 §5](06-connectors.md#5-files) |
| Secret-typed variables, universal redaction | [02 §2.2](02-configuration.md#22-secrets), [10 §5](10-observability.md#5-redaction) (v1) |
| Auth: OAuth2 password grant, body-signalled expiry, RDS IAM, SigV4 | [07 §3](07-rest-api.md#3-authentication) |
| Request batching/multiplexing | [07 §7](07-rest-api.md#7-iteration-queues-and-endpoint-dependencies) `for_each.batch` |
| Cursor in a POST body, adaptive window split on result caps | [07 §5](07-rest-api.md#5-pagination) `body_cursor` |
| Source schemas, raw-capture mode, snapshot-only streams | [04 §10](04-ingest.md#10-source-schemas), [07 §6](07-rest-api.md#6-records), [07 §11](07-rest-api.md#11-sync-state) (ship with the REST engine) |
| Binary payloads (BLOB column or blob sink), a vendor binary-format decoder, body repair, async submit/poll jobs | `Decoder` / `Paginator` / `Transport` core features |
| Verify/reconcile mode (source keyset vs target, gaps as structured output) | a generic reconciliation job |

## 5. Out of scope

- A web UI, a scheduler, an agent platform.
- General-purpose transformation (dbt).
- Reverse-ETL into SaaS APIs (CRM, billing, SES). These have side effects and ID-dependent branching, so they stay in Python.
- Non-Rust sources and plugin mechanisms (see §1).
