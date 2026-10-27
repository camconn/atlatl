# 04. Ingest semantics (writes into DuckLake)

This doc covers writes whose target is the lake. Writes into serving databases are CDC/apply, covered in [08](08-cdc.md). A plain DuckDB file target ([06 §10](06-connectors.md#10-duckdb-files)) is written the same way: the commit is one DuckDB transaction instead of one DuckLake snapshot.

## 1. Replication lifecycle

```
per member:  extract ─► stage ─► validate ┐
per member:  extract ─► stage ─► validate ┼─► commit (one DuckLake transaction for the whole replication) ─► cleanup
per member:  extract ─► stage ─► validate ┘        table-drift DDL + data changes for every stream + state + rejects
```

1. **Extract.** Each stream or member produces rows into staging, either engine-native or Rust-fed ([01 §3](01-architecture.md#3-data-paths)). Extracts run concurrently.
2. **Stage.** Rows land in DuckDB `TEMP` tables (§3).
3. **Validate** against the invariants in §9 and the stream's source schema (§10). A violation is a `data` error, raised before anything touches the target.
4. **Commit.** Once every stream is staged and valid, one transaction runs on one connection:
   ```sql
   BEGIN;
     -- for each stream, in declared order:
     --   table-drift DDL (ALTER TABLE … ADD COLUMN), if allowed (§6)
     --   data change for the mode (§4), chunk by chunk if chunked (§7)
     -- state upserts into catalog._atlatl.stream_state (05)
     -- rejects inserts/updates/deletes (03 §7)
   COMMIT;
   ```
   - The transaction produces **one DuckLake snapshot** for the whole replication ([03 §2](03-execution.md#2-atomicity)).
   - **Nothing to write, nothing committed.** When no stream changes a row, runs DDL or a hook, moves its state value or changes a reject (a new one, a changed error, a resolution or an expiry; a still-failing one with the same error doesn't count, [03 §7](03-execution.md#7-rejects)), the transaction rolls back instead of committing: no snapshot, and the replication reports `up_to_date` ([05 §4](05-state.md#4-semantics)). In a replication that does commit, a stream that wrote nothing reports `up_to_date` itself.
   - A `conflict` on COMMIT is retried against the same staging ([03 §6](03-execution.md#6-retry)).
5. **Cleanup.** Staging is dropped, and so are durable extract artefacts such as UNLOAD prefixes.

A replication can declare `hooks.pre_apply` / `hooks.post_apply` SQL. It runs *inside* the transaction, templated with `{{ staging }}` and `{{ target }}`, and it re-runs on every commit retry.

## 2. Guarantees

- One transaction per replication. `full-refresh` keeps the table and its id (§4.1).
- Run-scoped staging names (§3).
- The budget is set at engine start ([01 §5](01-architecture.md#5-memory-model)); hooks run inside the replication's commit.
- Merges update only rows whose payload changed, so change feeds and file rewrites stay small.
- NULL PKs fail by default (`null_keys`).
- `on_empty: mirror` by default for every source type, with a shrink guard.
- The `update_key` value is stored transactionally, with `lookback` and `inclusive` (§4.3).
- An existing table's schema is authoritative. New tables are typed from the full staged set (§5).

## 3. Staging

- The table is a DuckDB `TEMP` table named `stg_<run_id8>_<stream_seq>_<member_seq>`. It lives in the process's in-memory `temp` catalog, spills to `temp_directory`, and is never visible in the lake.
- The engine-native path uses `CREATE TEMP TABLE stg AS SELECT … FROM read_parquet(…)` or `postgres_scan(…)`, applying projections and casts inline.
- The Rust-fed path uses the Appender with Arrow batches. REST records are appended as `(ordinal BIGINT, record JSON)` and projected in SQL (§5.2).
- **Staging is built before the commit transaction begins**, in autocommit, where it can spill to `temp_directory`. TEMP tables created or dropped *inside* an open transaction are held in memory until it ends and never spill ([spike S3 §4](../docs/implementation/spike-s3.md#4-memory-of-a-chunked-10m-row-merge-in-one-transaction)), so the commit transaction creates no data-sized TEMP tables.
- A commit retry re-runs the whole apply transaction against the retained staging (a DuckDB transaction that failed to commit is aborted, not re-committable). A request/query retry replaces only the affected rows or partition. A `replication_retries` re-run recreates everything.

## 4. Modes

Ingest modes are `full-refresh`, `incremental`, `snapshot` for ingest, plus atlatl's `cdc` for serving ([08](08-cdc.md)).

### 4.1 `full-refresh`

After commit, the target contains exactly the source rows.

- **Without a `primary_key`:**
  ```sql
  DELETE FROM tgt;
  INSERT INTO tgt (<target column order>) SELECT <casts> FROM stg;
  ```
- **With a `primary_key`** (a diff-replace, recommended for `appdb.*`, where the tables are small and churn is rare):
  ```sql
  DELETE FROM tgt WHERE NOT EXISTS (SELECT 1 FROM stg WHERE stg.pk = tgt.pk);
  MERGE INTO tgt USING stg ON tgt.pk = stg.pk
    WHEN MATCHED AND (tgt.c1, …) IS DISTINCT FROM (stg.c1, …) THEN UPDATE SET …
    WHEN NOT MATCHED THEN INSERT …;
  ```
  An hourly re-sync of an unchanged table then commits nothing at all: no snapshot, and `up_to_date` (§1).
- **Empty source:**
  - `on_empty: mirror` (the default) empties the target.
  - `skip` leaves it untouched and reports `skipped_empty`.
  - `fail` raises `data`.
  - `guard: {min_rows: N, max_shrink: 0.5}` turns a suspicious drop into a `data` failure instead of a silent wipe. That covers vendor outages that return `[]`.
- **Missing table:** it is created from the resolved types (§5), inside the same transaction.

### 4.2 `incremental` with a `primary_key` (upsert)

```sql
MERGE INTO tgt USING (SELECT <casts> FROM stg) s
  ON tgt.k1 = s.k1 AND …
  WHEN MATCHED AND (tgt.<non-pk…>) IS DISTINCT FROM (s.<non-pk…>) THEN UPDATE SET …
  WHEN NOT MATCHED THEN INSERT (<cols>) VALUES (…);
```

- Target columns that are absent from the source are left alone on update and default to NULL on insert.
- `null_keys: fail` (the default) makes a NULL in any PK column a `data` error. `skip` drops those rows with a counted warning. `null_safe` joins with `IS NOT DISTINCT FROM` (slower, and explicit).
- `duplicates: fail` (the default) makes duplicate PKs in staging a `data` error. `last` / `first` keep one row per key using `order_by:`. A REST endpoint's dedup already happens during extraction ([07 §6](07-rest-api.md#6-records)).

### 4.3 `incremental` with an `update_key` (watermark)

`update_key` works with or without a `primary_key`. It is a string (the column name), or a map when you need options:

```yaml
update_key: publication_date
update_key: {column: publication_date, lookback: 0, inclusive: true, nulls: exclude}
```

- **Source filter:** `column >= stored_value - lookback` when `inclusive: true` (the default), otherwise `>`. Upsert semantics make overlap harmless, so rows sharing the max timestamp or committing late aren't skipped.
- **New value:** `max(column)` over the **staged** rows. It is written in the replication's commit and is never read back from the target.
- **First run:** with no stored value, a full extract runs. For adoption, `atlatl state init <stream> --from-target` seeds the value from `max(column)` of the existing lake table, as an explicit and logged step.
- **Nulls:** `nulls: exclude` never selects rows whose `update_key` is NULL. That matches today's behaviour, where the warehouse's estimate actuals rely on the daily full merge for the roughly 26% of rows with NULL `publication_date`. `include` adds `OR column IS NULL`.
- **Missing column:** a missing `update_key` column in the source or target is a `config` error.

### 4.4 `window`

`window:` is a templated SQL predicate pushed into the source query, for example `cons_start >= current_date - 45`. It's combined with `incremental` + `primary_key` and has no stored state. This is the warehouse's hourly pattern. `where` is accepted as an alias.

### 4.5 `incremental` without a `primary_key`, and `snapshot`

- `incremental` without a `primary_key` is an append (`INSERT` only). With an `update_key`, only new rows are appended.
- `snapshot` is an append with a required `metadata.loaded_at`.

### 4.6 Backfill and range overrides

`atlatl run --stream S --range 2015-01-01..2020-01-01` overrides the `update_key` value, window or REST iteration range for that run only ([07 §11](07-rest-api.md#11-sync-state)).

- The stored `update_key` value is advanced only if the range's end is beyond it.
- `--reset-state` is explicit and logged.

## 5. Typing

### 5.1 General rules

1. **An existing target column's type always wins** (§6). Staged values are cast to it.
   - A value that is non-NULL before the cast and NULL after `TRY_CAST` is a `data` error. The report carries up to 20 samples.
   - `on_cast_error: {set_null: true}` downgrades that to a counted warning (a bare `null` would clear the setting under the merge rules).
2. **Declared `columns.<c>.type` wins for new tables and new columns.**
3. **Otherwise the type is inferred:**
   - **DB sources:** from source metadata, mapped to DuckDB types with the source precision and scale. For Redshift this means reading `svv_columns` / `pg_table_def`, to keep precision.
   - **Files and REST:** the default is `infer: text`. Every column that isn't declared is `VARCHAR`. That is failure-proof, and dbt does the typing. The *expected* types live in a **source schema** (§10), which detects upstream changes without affecting storage.
   - `infer: safe` is opt-in per stream. It infers from the **full staged set**, never a sample, in SQL:
     - integers → `BIGINT` (never 32-bit);
     - decimals → `DECIMAL(38, max_scale)` if they fit, else `DOUBLE`;
     - timestamps and dates only if **every** non-null value parses with one ISO-8601 layout;
     - booleans only from JSON booleans;
     - leading-zero numerics stay `VARCHAR`;
     - everything else is `VARCHAR`.

### 5.2 REST/JSON shape

- Each record is one row, and its top-level keys become columns.
- **Nested objects and arrays** are stored as **JSON text** (`VARCHAR`) columns, not flattened. That matches today's `br` tables, where for example the `paged_queue` `ag` model reads `org->>'$.vendorIds.v2Id'`.
  - `flatten: N` opts into `a__b` columns , for new tables only.
- Column order:
  - for an existing table, the table's order;
  - for a new table, first-seen key order across records, followed by declared columns not yet seen.
- Numbers stay **exact** end to end (`serde_json` `arbitrary_precision` → DuckDB `DECIMAL`/`VARCHAR`), with no float64 round trip.

### 5.3 Column names

- The default is `naming: source`: names are kept verbatim (`dealId`, `Code`, `Tag`).
- `naming: lower` / `snake` are opt-in for new tables.
- `naming: clean_csv` reproduces a common legacy CSV header cleaning, for adopting tables loaded that way:
  1. NFD-strip accents;
  2. replace every `[^\p{L}\p{N}_]` run with `_`, trimming underscores at the ends;
  3. prefix `_` if the name starts with a digit;
  4. dedupe as `name1`, `name2`, …;
  5. lowercase.
- Matching staged columns to existing columns is exact first, then case-insensitive. An ambiguous match is a `config` error.

## 6. Existing tables: adoption and table drift

atlatl **adopts** tables another loader created. It never recreates them.

- The **target schema is authoritative**: its column set, order and types. Existing column order is never changed. This matters because `metric_warehouse_est_v2` unions 12 `br` tables *positionally*.
- `table_drift.new_columns`:
  - `add` (the default) runs `ALTER TABLE ADD COLUMN`, appended at the end, inside the replication's commit;
  - `ignore` drops the column with a warning;
  - `fail` raises a `data` error.

  Positionally coupled tables (the est_v2 families) should set `fail`, so a new vendor column can't silently break `ag`. See the coverage matrix.
- `table_drift.type_changes`:
  - `fail` (the default) raises when the source type can't be cast losslessly into the existing column;
  - `widen` allows the promotions DuckLake's `ALTER COLUMN TYPE` accepts: wider integers (`INTEGER → BIGINT`), `FLOAT → DOUBLE`, and `DECIMAL(p,s) → DECIMAL(p',s')` with s' ≥ s and no fewer integer digits. DuckLake refuses anything → `VARCHAR` ("only widening type promotions are allowed"), so that stays a `fail`.
  - Under `fail`, a type difference isn't rejected by type alone. Staged values are cast into the existing column type, and any non-NULL value that doesn't survive `TRY_CAST` is a `data` error (`cast_failed`) with samples (§5.1).
- `table_drift.missing_columns`: a target column absent from the source is left NULL on insert and untouched on update. The policy is `warn` (the default) or `fail`.
- Legacy-loader artefacts that must be adopted as-is:
  - `integer` (int32) and `decimal(24,6)`-style columns;
  - all-`text` `br` tables;
  - `_legacy_loaded_at` (epoch seconds, `INTEGER`) on bulk-file tables.

## 7. Chunking

Chunking **bounds memory only**. Chunks are applied one after another inside the replication's **single** commit transaction ([03 §2](03-execution.md#2-atomicity)). DuckLake writes each chunk's data files as it goes, so transaction state stays flat ([spike S3 §4](../docs/implementation/spike-s3.md#4-memory-of-a-chunked-10m-row-merge-in-one-transaction)). There are no per-chunk commits and no resume: a failure or kill rolls the whole replication back, and the next run starts over.

- **A chunk is merged straight from its pinned source relation**, not from a per-chunk TEMP table (which would be held until commit): `MERGE … USING (<chunk relation>)`, where the relation is a group of UNLOADed parquet files, `catalog.s.t AT (VERSION => n) WHERE hash(pk) % N = k`, or a Postgres scan with the chunk predicate.
- The invariants (§9) run once, before the commit and outside it, as queries over the whole pinned source relation, so they cover every chunk at once: the row count (against a manifest's `record_count`: `decode`), NULL keys (`null_keys: skip` filters every chunk relation instead), duplicate keys across chunks, casts into an adopted table, `on_empty` / `guard`, and the new `update_key` watermark. `duplicates: first|last` and `on_cast_error.set_null` need a staged set and are a `config` error with `chunk`.
- Pinning (files by path, a lake snapshot) makes a commit retry re-read identical data.
- Measured: a 10M-row target with 5.5M updates and 1M inserts in 10 chunks, under `memory_limit=300MB`, peaked at 721 MB RSS; the unchunked MERGE ran out of memory.

```yaml
chunk: {by: files, size: 2GB}                      # Redshift UNLOAD / files sources: apply per group of files
chunk: {by: hash, columns: [sym_id], buckets: 16}  # key-partitioned extracts: WHERE hash(sym_id) % 16 = k
chunk: {by: range, column: date, step: 1y}         # ordered column ranges
```

- `incremental` + `primary_key` with chunks is correct as long as a PK never spans two chunks, or does so idempotently. `hash` and `range` guarantee this. `files` relies on the source PK being unique, which the pre-commit invariants check across all chunks.
- `full-refresh` with a PK can be chunked. Each chunk is upserted, the keys seen are recorded in a temp table, and the keys not seen are deleted at the end, all in the same transaction.
- The motivating case is the 66M-row `fe_advanced_conh_af` daily merge. It uses parallel UNLOAD to Parquet with `MAXFILESIZE`, then applies file groups one after another under a fixed DuckDB budget.

## 8. Metadata columns

These are off by default. Each is enabled per stream or replication.

```yaml
metadata:
  loaded_at: {name: _legacy_loaded_at, type: epoch_s_int}   # epoch_s_int | timestamptz (default name _atlatl_loaded_at)
  run_id:    {name: _atlatl_run_id}
  source:    {name: _atlatl_source}                        # file URL / endpoint URL
```

- The value is constant per replication run (the replication's start time), which matches `_legacy_loaded_at` semantics.
- `loaded_at` and `run_id` change every run, so the change guard (§4.2) doesn't compare them: an unchanged row keeps the stamp of the run that last wrote it, and a new or changed row gets this run's. A rerun with nothing new is then a real no-op (§1). `source` is compared, since it is the same every run for the same member. `snapshot` (an append) stamps every row it appends.
- Metadata columns are appended at the end of **new** tables. For existing tables they're written only if the column already exists, and otherwise follow `new_columns` policy.
- The bulk statements (`queue_chain_bulk`) set `loaded_at: {name: _legacy_loaded_at, type: epoch_s_int}` until the dbt `ids` list stops referencing it.

## 9. Invariants checked before apply

1. The staged row count equals the rows produced by extract (catches lost batches). Rejects are counted separately ([03 §7](03-execution.md#7-rejects)).
2. There are no NULL PK values (unless `null_keys` says otherwise).
3. There are no duplicate PKs (unless `duplicates` says otherwise).
4. Every PK, `update_key` and `columns.include` column exists.
5. An `on_empty` / `guard` violation fails.

The report records row counts:
- `rows_extracted`, `rows_staged`, `rows_inserted`, `rows_updated`, `rows_deleted`, `rows_unchanged`;
- the DuckLake snapshot id created (or `null` if nothing changed).

## 10. Source schemas

A source schema declares what a source is *expected* to send: fields, logical types, formats, nullability and optional domains. It is checked on every run against the staged data. It is **independent of storage**: `br` stays all-text, and the source schema detects upstream drift, such as a vendor renaming a field, switching a number to a formatted string, or starting to send nulls, before dbt breaks on it.

Source schemas apply to any source. They matter most for REST and files, where there's no source catalog to compare against. For DB sources they are compared against the source's catalog types.

```yaml
# schemas/throttled/releases.yaml (or inline under the stream key `schema:`)
schema:
  mode: enforce              # enforce (default whenever a source schema exists) | observe (report only, for drafting)
  fields:
    Code:         {type: string, nullable: false}
    Date:         {type: timestamp, format: "%Y-%m-%dT%H:%M:%S"}
    Tag:          {type: string}
    ActualValue:  {type: decimal}
    Importance:   {type: integer, values: [1, 2, 3]}
    Revenue:      {type: string}                      # formatted "253.15B": deliberately string
  # each policy is ignore | warn | fail; all default to fail under enforce
  on_new_field: fail          # a field seen that the source schema doesn't list
  on_missing_field: fail      # a declared field absent from every record this run
  on_type_violation: fail     # a non-null value that doesn't parse as the declared type/format
  on_null_violation: fail     # null in a nullable: false field
  on_domain_violation: fail   # a value outside `values`
  tolerance: {ratio: 0, min_count: 1}   # violations below this are counted but not acted on
```

- **Logical types:** `string`, `integer`, `decimal`, `float`, `boolean`, `date`, `timestamp`, `timestamptz`, `json` (object or array). Each can take a `format` (strftime for temporal types; `plain` / `thousands_sep` for numbers).
- **Evaluation:** in DuckDB, over the staged records, before apply. `TRY_CAST` / `try_strptime` counts per field, `json_keys` for field sets. It costs one aggregate pass over staging.
  - Fields are the top-level keys of JSON-staged records (REST, `(ordinal, record)`), or the columns of columnar staging (files, DB, `br` tables, minus atlatl's own columns).
  - A missing key counts as null. A field absent from every record is only a missing field, not also null violations. With zero staged records `on_missing_field` isn't evaluated.
  - JSON strings are parsed like text, so `"42"` is an `integer`; typed JSON values and typed columns must fit (`1.0` is not an `integer`, `false` is not a `decimal`). `string` accepts any scalar. `json` needs an object or array. `date` without a `format` needs `YYYY-MM-DD`; `timestamptz` without a `format` needs an offset or `Z`. `boolean` accepts `true/false/t/f/1/0`. `values` compare numerically for numeric types, else as text.
  - `ignore` drops that finding from the report. `tolerance` applies per field and kind: a violation acts only when `count >= min_count` (default 1) and `count >= ratio × records` (default 0); below it, it is still listed with its count. Under `observe`, `fail` becomes `warn`.
  - The report section (`schema`, [10 §4](10-observability.md#4-run-report)) has `status` (`violated` when a `warn`/`fail` policy acted), `severity` (`error` when a `fail` did), `records`, `new_fields`, `missing_fields` and `violations [{field, kind, count, samples [{ordinal, value}]}]` (up to 3 samples, redacted). It is written for every evaluated member, passing or not.
- **Outcomes:**
  - A `fail` policy that trips makes the replication fail with class `data` (`schema_violation`), before anything is written.
  - A key column missing from the source (§9 item 4, `missing_column`) is usually a renamed field: when the stream has a source schema, it is evaluated first, and its violation (with the new and missing fields) is the error reported.
  - `warn` records the drift in the report ([10 §4](10-observability.md#4-run-report)) and emits a `schema.drift` event.
  - `dagster-atlatl` surfaces both as an asset check (`atlatl_schema`, with severity `WARN` or `ERROR`) with samples ([09 §4](09-dagster.md#4-materialization-metadata)).
- **Storage interplay:**
  - New fields are still added as `VARCHAR` columns per `table_drift.new_columns` (§6). The source schema only decides whether that is also a warning or a failure.
  - Later, `store: typed` can use source schema types when creating *new* tables.
- **Tooling:**
  - `atlatl schema infer <stream> [--from-table | --from-api-test | --from-cassette <name|path>] [--write [--force]]` drafts a source schema (`mode: observe`) from the existing `br` table (all-text values are profiled for the narrowest type that fits every value), a live `api test` sample, or a recorded cassette, for review. Narrowest first: `boolean` (`true`/`false` only), `integer`, `decimal`, `float`, `date`, `timestamptz`, `timestamp`, `json`, else `string`; digit strings with leading zeros stay `string`. `nullable: false` is drafted when no nulls were seen; `values` and formats are never guessed. `--write` writes `schemas/<stream id>.yaml`.
  - `atlatl schema check <stream> [--from-table | --from-api-test | --from-cassette …] [--json]` validates the current table or a sample against its source schema without a run, printing the report section; it exits 1 when a `fail` policy trips.
- **Defaults:** a stream **with** a source schema is enforced: every policy defaults to `fail`, and individual policies can be relaxed to `warn` or `ignore`. `mode: observe` exists only for drafting a new source schema, and `atlatl validate` warns about it. A stream **without** a source schema is unchecked.
- **Milestone:** source schemas (enforce and observe) and `schema infer` ship with the REST engine (M3). Each ported vendor gets a reviewed source schema as part of its cut-over.

## 12. Bind lists

A source query often needs a list only known at run time, such as the instruments a consumer's
dbt models currently cover. `bind` lists one per stream, splices it into `from.query`, and keeps
the stream one atomic unit (unlike `for_each`, which makes an instance per item):

```yaml
- id: ch/eod_close
  mode: full-refresh                # re-pulled whole every run, so corrected upstream bars land
  object: br.eod_close
  primary_key: [series_id, date]
  bind:
    series: {query: "SELECT series_id FROM ag.scoped_series", connection: warehouse, batch: 500}
  from:
    query: >
      SELECT series_id, toDate(timestamp) AS date, argMax(close, timestamp) AS eod_close
      FROM default.aggregate_eod
      WHERE series_id IN ({{ bind.series }}) AND close > 0
      GROUP BY series_id, date
```

- **Sources.** ClickHouse streams with `from.query` (`bind`).
- **Listing.** Each `bind.<name>` runs `query` on `connection` (a lake, postgres or duckdb connection, as for `for_each.over`) when the member is extracted, so it sees what earlier replications in the run committed and what dbt wrote before the run. The SQL is never templated. Each row must have one column.
- **Splicing.** The plan renders `{{ bind.<name> }}` as the marker `/*atlatl:bind:<name>*/` (so `atlatl plan` shows where lists go); the runner replaces it with the values as ClickHouse literals, comma-separated: numbers bare, strings single-quoted with `\` escapes, `NULL`, `true`/`false`. Plan checks reject a list `from.query` doesn't use and a `bind.<name>` it uses but doesn't define; a non-scalar value is `config` at run time.
- **Batching.** One list may set `batch: n`: the query then runs once per `n` values, and every batch's result stages and commits together (one commit, so a full refresh still replaces the whole table). Other lists are spliced whole into every batch.
- **Empty lists.** An empty list renders as `NULL`, so the query still runs once and returns its schema. `IN (NULL)` matches nothing, but `NOT IN (NULL)` matches every row, so write exclusions accordingly. The member warns `bind_empty`, and `on_empty` decides what a full refresh with no rows does to the target (default `mirror`: it empties it).

