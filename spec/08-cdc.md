# 08. CDC: DuckLake → serving databases

This is the headline capability. It replaces the `updated_at > max(target.updated_at)` pseudo-CDC in `load/*.yaml` with snapshot-based change apply.

## 1. Principles

1. **The cursor is a DuckLake snapshot id.** It is stored **in the target** and committed in the same transaction as the changes it covers. That gives exactly-once apply wherever the target has transactions.
2. **Changes are keyed on the business PK, never on `rowid`.** Rowids change under dbt's `delete+insert` and under compaction (`ducklake_rewrite_data_files`).
3. **Only real changes are written.** A row is written only if its projected payload differs between the window's start and end. This prevents table rebuilds and delete+insert churn from rewriting the serving DB, which also helps Postgres bloat.
4. **Gaps are never silent.** If the change history needed is gone (snapshot expiry), atlatl fails or explicitly resyncs.
5. **The target schema is owned by someone else** (Atlas for Postgres). atlatl validates it but doesn't migrate it.

## 2. Configuration

```yaml
replications:
  org_event:
    source: lake
    target: app_pg
    asset: {group: org_event}
    defaults:
      mode: cdc
      deletes: {mode: soft, column: deleted_at}   # soft (default) | hard | ignore; see §6.0
    streams:
      - id: org_event/hub
        from: {table: au.org_event}
        object: app.org_event
        primary_key: [object_id]
        columns:
          include: [object_id, org_id, type, date, updated_at]
          type: {target_type: app.event_type}
        asset: {key: org_event/load/app/org_event, deps: [org_event]}
      - id: org_event/rating
        after: [org_event/hub]
        from: {table: au.org_event_rating}
        object: app.org_event_rating
        primary_key: [event_id]
        columns:
          include: [event_id, instrument_id, source_name, author, time, rating_change,
                    rating_current, rating_prior, target_action, target_current,
                    target_prior, target_currency, updated_at]
          rating_change: {target_type: app.rating_change}
          rating_current:
            transform: "trim(both '_' from regexp_replace(upper(rating_current), '[^A-Z0-9]+', '_', 'g'))"
            target_type: app.rating
          rating_prior:
            transform: "trim(both '_' from regexp_replace(upper(rating_prior), '[^A-Z0-9]+', '_', 'g'))"
            target_type: app.rating
      # … listing, report, funding shards
```

## 3. Window selection

For each CDC replication at start:

1. **E** (the end) is `SELECT id FROM ducklake_current_snapshot('catalog')`, pinned **once per replication**, so every stream (hub and shards) sees the same lake state.
2. **C** (the start) is the stored cursor. The window is `(C, E]`. If `C = E`, there is nothing to do (`status: up_to_date`).
3. **Guards** run before reading: retention (§5) and table identity. If the source table's DuckLake `table_id` changed since C (it was dropped and recreated), the result is `resync_required`. The cursor stores the `table_id` (from `ducklake_table_info`). This guard is a correctness requirement: across a recreate, the change feed resolves the name to the new table and returns only its rows, so every dropped key would be missed (`atlatl-run/tests/serve_shapes.rs` `recreated_table_needs_resync`).
4. **Bounds:** if `max_snapshots` or `max_changes` would be exceeded, E is lowered to the largest snapshot that fits. The rest is picked up in the next window (§8).

## 4. Net-change computation

atlatl diffs **table states** instead of interpreting `change_type` rows one by one. The change feed is used only to find which keys were touched. This makes the result independent of how the writer expressed the change: delete+insert, update pre/postimages, full table rewrite, or compaction.

```sql
-- 1. Keys touched in the window (bounds are inclusive; projection pushdown: PK columns only)
CREATE TEMP TABLE touched AS
  SELECT DISTINCT <pk> FROM ducklake_table_changes('catalog', 'au', 'x', C + 1, E);

-- 2. Before / after images of those keys, projected to the served columns (+ transforms).
--    <proj@C> reads with C's schema: columns added after C are projected as NULL AS <col>.
CREATE TEMP TABLE before AS SELECT * FROM (SELECT <proj@C> FROM catalog.au.x AT (VERSION => C)) b
  SEMI JOIN touched USING (<pk>) [WHERE <where>];
CREATE TEMP TABLE after  AS SELECT * FROM (SELECT <proj> FROM catalog.au.x AT (VERSION => E)) a
  SEMI JOIN touched USING (<pk>) [WHERE <where>];

-- 3. Net changes
SELECT 'upsert' AS op, a.* FROM after a LEFT JOIN before b USING (<pk>)
  WHERE b.<pk1> IS NULL OR (a.<cmp…>) IS DISTINCT FROM (b.<cmp…>)
UNION ALL
SELECT 'delete' AS op, b.<pk…> FROM before b ANTI JOIN after a USING (<pk>);
```

- `<cmp…>` is the projected columns minus `diff.ignore_columns`. For example, ignoring `updated_at` on `au.funding_participant`, which stamps `CURRENT_TIMESTAMP` on every build, stops the whole table re-pushing each time. When other columns do change, ignored columns are still written.
- **Full rewrite.** When most keys are touched (for example a `table`-materialised model, or the planned mass `UPDATE` in `migration_plan.md`), `touched` covers the whole table. The result is still exact, and it costs one full-table diff inside DuckDB. Diffing is chunked by PK hash when `touched` exceeds `diff.chunk_rows`.
- **Schema.** `AT (VERSION => C)` reads with the schema as of C, and referencing a column added later is a binder error. atlatl reads C's columns (`DESCRIBE … AT (VERSION => C)`) and projects `NULL AS <col>` for served columns added after C. Columns dropped after C are an error if they are in `include`. The feed's own output columns follow E's schema.
- **Only the touched keys come from the feed.** Its per-row `snapshot_id` was wrong, before DuckDB v1.5.6's ducklake, for deletes that DuckLake folds into an earlier delete file (`atlatl-cdc/tests/feed.rs` `delete_stamps_from_a_fresh_attach_when_deletes_share_a_file`, which now pins the fixed behaviour), so no step depends on it.
- **Filters.** `where:` (for example a publish window) is applied to both images, so a row leaving the filter becomes a delete. `deletes: ignore` suppresses that.
- **Validation.** NULL or duplicate PKs in `after` are `data` errors.

## 5. Retention guard

`ducklake_expire_snapshots` (and `CHECKPOINT`, which calls it) can delete the snapshots at or after C that both `table_changes` and `AT (VERSION => C)` need. Prod currently keeps snapshots forever, so this doesn't bite today. The guard still ships in v1, because it is cheap and a silent gap would be the worst failure mode. The tooling that coordinates with maintenance is **later**.

- **Check (v1).** Before reading, verify that snapshot C still exists (`SELECT count(*) FROM ducklake_snapshots('catalog') WHERE snapshot_id = C`). An expired C fails reads with `No snapshot found at version C`, the same text as for a future snapshot, so the explicit check comes first and the text is only a backstop. If C is gone, the result depends on `on_gap`:
  - `fail` (the default) gives a `data` error, `cursor_expired`, with the cursor, the oldest retained snapshot and a suggested command;
  - `resync` runs a bootstrap `adopt` at E (§9), which is expensive but correct. It is recorded as `resynced` in the report.
- **Lag metrics (v1).** Every CDC report entry includes `snapshots_behind` and `seconds_behind` (E's commit time minus C's).
- **Consumer floor (later).** This becomes needed only once snapshot expiry is enabled.
  - Each committed apply also upserts `catalog._atlatl.consumers(consumer_id, source_table, cursor_snapshot, cursor_time)`.
  - `atlatl lake retention-floor` prints the oldest cursor across all consumers.
  - `atlatl lake expire [--dry-run]` expires snapshots up to `min(now − policy, floor)` and never passes the floor.

## 6. Target appliers

The net changes (upserts, then deletes) stream out of DuckDB as Arrow batches into the target applier.

### 6.0 Delete modes

Soft and hard deletes are both first-class. **Soft is the default and ships first (M1)**, and hard follows (**M1.x**).

```yaml
deletes:
  mode: soft            # soft (default) | hard | ignore
  column: deleted_at    # target column: timestamp/timestamptz (or boolean with value: true)
  value: snapshot_time  # snapshot_time (commit time of the snapshot that deleted the row; see below) | now | true
  restore: true         # re-inserting a soft-deleted key clears the column
```

| Situation | `soft` | `hard` |
|---|---|---|
| A key is deleted in the lake | `UPDATE … SET <column> = <value> WHERE pk = … AND <column> IS NULL`. The other columns keep their last values. The operation is idempotent: an already-deleted row keeps its original timestamp | `DELETE … WHERE pk = …`, applied child-first (§7) |
| A deleted key reappears in the lake | the upsert writes the row and sets `<column> = NULL` (`restore`) | a plain insert |
| The soft column is also a served source column (the lake carries its own `deleted_at`, as the funding shard and the `metric` feed do) | the upsert writes the lake's value. Lake-side deletes still set the column. `restore` is implied by the lake value being NULL | n/a |
| A row leaves a `where:` filter | treated as a delete, following `mode` | treated as a delete |
| `ignore` | lake deletes are dropped (they're counted in the report) | |

- `snapshot_time` is found by probing, because the change feed can't give it: a key leaving `where:` is an UPDATE in the feed, with no delete at all, and the feed's delete labels were wrong before DuckDB v1.5.6 (§4). For the deleted keys only, take the first snapshot in `(C, E]` whose `changes` touch the table and in which the key is absent (`AT (VERSION => s)`), and stamp that snapshot's `snapshot_time` (`atlatl-cdc/tests/feed.rs` `delete_stamps_from_a_fresh_attach_when_deletes_share_a_file`).
- At plan time the applier checks that the soft column exists on the target and has a compatible type. A missing column is a `config` error.
- Soft deletes never violate FKs, because the rows stay. Group ordering still applies, so that restores and inserts land parent-first.
- With `value: true` (a boolean column) a row counts as not deleted unless the column is `true`: the delete is `… SET <column> = true WHERE … AND <column> IS NOT TRUE`, and `restore` sets it to `false`.

### 6.1 Postgres (v1)

```sql
BEGIN;
SET LOCAL lock_timeout = '30s';
SET LOCAL statement_timeout = '<per stream>';

CREATE TEMP TABLE _atlatl_up_<n> (<cols in wire types: text for enums, text[] for lists, …>) ON COMMIT DROP;
COPY _atlatl_up_<n> (<cols>) FROM STDIN (FORMAT binary);

INSERT INTO app.t AS t (<cols>)
SELECT <col or col::target_type …> FROM _atlatl_up_<n>
ON CONFLICT (<pk>) DO UPDATE SET <c = EXCLUDED.c …>
  WHERE (t.<c…>) IS DISTINCT FROM (EXCLUDED.<c…>);

CREATE TEMP TABLE _atlatl_del_<n> (<pk>, _ts timestamptz) ON COMMIT DROP;
COPY _atlatl_del_<n> FROM STDIN (FORMAT binary);
UPDATE app.t t SET deleted_at = d._ts FROM _atlatl_del_<n> d                 -- deletes: soft (default)
  WHERE t.<pk> = d.<pk> AND t.deleted_at IS NULL;
-- DELETE FROM app.t t USING _atlatl_del_<n> d WHERE t.<pk> = d.<pk>;         -- deletes: hard

UPDATE atlatl.cursors SET snapshot_id = $E, snapshot_time = $t, source_table_id = $id, …, updated_at = now()
  WHERE stream_id = $1 AND snapshot_id = $C;    -- compare-and-set: 0 rows updated => concurrent writer => abort
-- first apply (no cursor yet): INSERT … ON CONFLICT (stream_id) DO NOTHING, where 0 rows also aborts
COMMIT;
```

- Aurora PG 12 compatible (no `MERGE`), verified on PG 12.22 and 16.14 (`atlatl-conn-postgres/tests/pg/apply.rs`). The planner verifies a unique index on the PK columns.
- Enums are staged as `text` and cast in the apply `SELECT`. An unknown label fails the transaction with SQLSTATE `22P02` (`data`).
- The upsert ends with `RETURNING (xmax = 0) AS inserted`, which counts inserts and updates. Rows the guard filters return nothing, so `rows_unchanged = staged − inserted − updated`.
- Columns not in `include` are never touched. Target defaults (such as `inserted_at DEFAULT now()`) apply on insert.
- `target_type` casts replace every `pre_sql` recast in `load/*.yaml`.
- With `restore: true` and a soft column that is not a served source column, the upsert's `DO UPDATE SET` also sets `deleted_at = NULL`, and the `IS DISTINCT FROM` guard includes it.
- Source-side tombstones, such as those of `au.metric_feed`, are plain data and are upserted like any other column.
- The cursor compare-and-set, plus the table lock ([03 §6](03-execution.md#8-locks)), makes concurrent appliers safe.
- **Staging types.** A column is staged in the target column's own type when that is a built-in type the Arrow type encodes into directly (so jsonb, `text[]`, timestamptz and numeric go straight in). Otherwise (enums, domains, or any column with a `target_type`) it is staged in the Arrow type's natural wire type (`text` for strings, `text[]` for lists of strings, …) and cast in the apply `SELECT`: to the `target_type`, or else to the column's type.
- The transaction also runs `SET LOCAL TimeZone = 'UTC'`, so a timestamptz stamp lands in a `timestamp` column as UTC. The guard compares `json` columns as `jsonb`, because `json` has no equality operator.
- `ensure_state_schema` runs once per run, before `BEGIN`, in its own transaction.

### 6.2 SQLite and Turso

These use the same logical steps in one transaction ([06 §6](06-connectors.md#6-sqlite-target)):
- chunked multi-row `VALUES` statements in place of staging tables;
- `INSERT … ON CONFLICT (pk) DO UPDATE SET … WHERE (t.c, …) IS NOT (excluded.c, …)`;
- `DELETE … WHERE (pk) IN (VALUES …)`, or the soft-delete `UPDATE`;
- the `_atlatl_cursors` compare-and-set.

`_atlatl_cursors` has the columns of `atlatl.cursors` (05 §3), times as ISO-8601 text, plus `pending_snapshot INTEGER` and `pending_fingerprint TEXT` (§6.3). atlatl creates it on first use; there is no Atlas. The compare-and-set is `UPDATE … WHERE stream_id = ? AND snapshot_id = C AND (pending_snapshot IS NULL OR pending_snapshot = E)`, and on a first apply `INSERT … ON CONFLICT (stream_id) DO NOTHING`; zero rows changed is `conflict` (`cursor_moved`) and rolls back. A CDC apply never moves a cursor past another window's pending mark; a bootstrap clears the mark.

Turso windows above `transaction_rows` fall back to the D1 scheme.

### 6.3 D1

D1 has no transactions across HTTP requests. It is the one exception to single-transaction commits ([03 §2](03-execution.md#2-atomicity)), so apply is **idempotent per window**:

1. Record `pending = {E, fingerprint}` for every stream of the unit: `UPDATE _atlatl_cursors SET pending_snapshot = E, pending_fingerprint = f WHERE stream_id = ? AND snapshot_id = C AND (pending_snapshot IS NULL OR (pending_snapshot = E AND pending_fingerprint = f))`. Zero rows is `conflict`: another applier moved the cursor, or a different window is pending. The fingerprint is a hash of the stream's `config_hash`, C, E and the source `table_id`.
2. Send upsert and delete batches, parent-first then child-first as in §7. Each batch is replayable, because it sets final values per PK for the fixed window (and a soft-delete stamp only lands on rows not yet stamped).
3. Set `snapshot_id = E` and clear `pending` as the last statement, matching the marked fingerprint. If that reports zero rows because a retried request already applied it (a lost response), atlatl re-reads the cursor and accepts `snapshot_id = E` with no pending mark.

A crash between steps 1 and 3 leaves `pending` set. The next run re-applies the same window (the same E) before doing anything new: a pending mark pins E for the whole unit (bounds are not re-evaluated; streams already at E are skipped; two different pending E are `pending_mismatch`), and a fingerprint that no longer matches (the definition or the source table changed) is a `config` error, `pending_mismatch`, resolved by `atlatl cdc bootstrap --mode adopt`. Re-applying exactly (C, E] matters: a wider window would miss keys whose value at the new end equals their value at C, leaving the partial write of the failed run in place. If the head moved meanwhile, the run ends `partial_caught_up` and the next run continues. Batch sizing and limits are in [06 §7](06-connectors.md#7-d1-target).

## 7. Ordering

- A CDC replication applies **all its streams in one target transaction**, including every stream's cursor, because the replication is the atomic unit ([03 §2](03-execution.md#2-atomicity)). FKs are consistent at every commit, and no reader sees shards without their hub.
- Stream-level `after:` orders **upserts** parent-first (hub before shards) and **hard deletes** child-first (shards before hub). The executor schedules this: every stream's upserts in `after` order, then every stream's deletes in reverse order. The applier rejects an upsert after any delete in the same transaction.
- Across replications, `after` orders runs but gives no shared transaction, unless they share an `atomic_group` on the same target. Which streams belong together in one replication is the consumer's config decision.

## 8. Bounded runs

`max_changes` (default 1 000 000), `max_snapshots`, and `max_duration` (for example `4m` for a 5-minute schedule).

- A run applies **one** window, (C, E'], lowering E to E' so the bounds hold (§3), in the replication's single commit. `max_changes` is checked by counting feed rows for a candidate window and bisecting over the snapshots that touch the table; per-snapshot label counts are unreliable. If E' < the head, the run ends with `status: partial_caught_up`, and the next run continues from E'.
- Dagster can therefore drive CDC on a short schedule or from a sensor.

## 9. Bootstrap, adoption and resync

`atlatl cdc bootstrap <replication> --mode adopt|reload [--at-snapshot S] [--dry-run]`

- **`adopt`** (the default for existing populated tables):
  1. Pin S.
  2. DuckDB attaches the target Postgres read-only and computes `diff(lake@S projected, target)` on PK plus the compared columns, cast to comparable types.
  3. Only the differences are applied, then `cursor = S`.

  `--dry-run` reports the diff counts and samples without writing. That doubles as the Phase 0 parity check.
- **`reload`**: upsert every row of lake@S and delete target rows missing from lake@S, then `cursor = S`. It never truncates, because FKs from other tables may point at the rows.
- Both are chunked by PK hash for memory, but applied in **one** target transaction together with the cursor. There's no resume: a failure or kill means a full re-run.
- **Resync** after a retention gap is `adopt` at E.
- `--advance-cursor-only` sets `cursor = E` without applying. It's explicit, logged and dangerous, and is meant for after a manual backfill (the Kafka publisher's `SKIP_PUBLISH` pattern).

## 10. Future CDC sources (into the lake)

These are designed through the `ChangeSource` trait ([11](11-extensibility.md)). None is in v1.

| Source | Mechanism | Applied to the lake as |
|---|---|---|
| Postgres (`appdb`, myapp) — **built (M6, `atlatl-conn-pgrepl`), see §10.1** | pgoutput logical replication through atlatl's own replication client and decoder, built on `postgres-protocol` (`atlatl-conn-pgrepl/tests/pg/protocol.rs`). The protocol is negotiated by server version (`changes.protocol`, §10.1): **v1 with text values** on PG 12/13 (Aurora prod rejects `binary`, `messages`, `streaming` and `proto_version 2`), and **v2 with `streaming 'on'` and binary values** on PG 14+. Binary values are rendered to the server's text form, so either way values are staged as text with per-column unchanged-TOAST flags, then cast by the Relation message's type OID and typmod. **One owned slot and one publication per replication** (`atlatl_<replication>`), because every slot decodes the whole WAL; explicit create/decommission, slot lag and `catalog_xmin` age reported every run, prod slots refused from dev, and a `temporary` slot for tests. PG 12 has no `max_slot_wal_keep_size`, so a stuck slot retains WAL without limit | upsert by PK, or append in history mode, with `_atlatl_op`, `_atlatl_synced_at` and `_atlatl_lsn`. The cursor is the commit `end_lsn` in `stream_state` (kind `lsn`), advanced in the lake commit, and the slot's confirmed flush is sent after the commit. Redelivery of unconfirmed transactions is at-least-once, so transactions with `end_lsn <= cursor` are skipped. `check_retained` fails as `data` if `confirmed_flush_lsn > cursor`, because the server silently starts there. Empty transactions (PG < 15) and idle keepalive `wal_end`s are confirmed so a quiet slot doesn't pin WAL. Bounded runs stop at the `pg_current_wal_lsn()` read after the backfill. Under v1 a transaction arrives whole at commit; under v2 a large one arrives in stream blocks before it commits. Either way it streams into staging, never into memory |
| Turso | Poll `turso_cdc` (`change_id` cursor, before/after images), enabled by `PRAGMA capture_data_changes_conn('full')` | same |
| MySQL / PlanetScale | binlog (MySQL) / VStream gRPC (PlanetScale MySQL) / logical replication (PlanetScale Postgres) | same |
| D1, Redshift, REST | No change stream. `update_key` watermarks, windows or `sync` cursors ([04](04-ingest.md), [07](07-rest-api.md)) | `incremental` |

### 10.1 Postgres logical replication (M6)

**Config.** The source connection opts in with a `changes:` block, which environments can override like any other key:

```yaml
connections:
  appdb_pg:
    type: postgres
    host: prod-appdb
    read_only: true
    changes:
      production: true            # this definition points at a production server
      slots: {allow_create: true} # may create its slot and publication (default false)
      # slots: {temporary: true}  # session-scoped slot for tests: every run resyncs
      # protocol: auto            # auto (default) | v1 | v2
replications:
  appdb_cdc:
    source: appdb_pg
    target: lake
    defaults: {mode: cdc, max_duration: 10m}
    streams:
      - {id: appdb/users, from: {table: public.users}, object: br.appdb_users}
      - {id: appdb/events, from: {table: public.events}, object: br.appdb_events, history: true}
```

- `mode: cdc` from a postgres source into the lake needs the `changes:` block and `from.table` (a plain table; partitioned tables are not supported yet). `primary_key` defaults to the table's primary key, which must be part of its replica identity (or the table uses `REPLICA IDENTITY FULL`; `NOTHING` is refused). `object` must be a literal `schema.table`.
- `max_changes` and `max_duration` bound the window (the smallest across the replication's streams). A window always ends on a transaction boundary: `max_changes` is checked after each commit, and `max_duration` drops a transaction still open.
- `deletes.mode`: `hard` (default) deletes the lake row, `soft` sets `_atlatl_op = 'delete'` and keeps the row, `ignore` skips deletes and truncates. `deletes.column`/`value` don't apply.
- `history: true` appends every netted change as a row instead of upserting. The current version of a key is its row with the highest `_atlatl_lsn`.
- `window`, `diff`, `on_gap` and `max_snapshots` don't apply.
- `changes.protocol`: `auto` (default) picks pgoutput v2 on PG 14+ and v1 before; `v1` forces text values without streaming; `v2` requires PG 14+ (`config` before any slot is created otherwise).

**Slots and publications.** One slot and one publication per replication, both named `atlatl_<replication>` (lowercased, other characters mapped to `_`, at most 63 bytes, with a hash suffix when truncated). `validate` rejects two replications on one connection that map to the same name. The publication is created before the slot, because decoding WAL from before a publication existed fails. Each run reconciles its tables with `ALTER PUBLICATION … SET TABLE`. Creating or altering either needs `slots.allow_create: true`; otherwise an existing slot and publication are used as they are. `atlatl cdc decommission <replication> [--force] [--env E]` drops both (refused while a walsender holds the slot unless forced); the lake keeps its data and cursors, so the next run fails as `data` (`slot_missing`) until `atlatl state reset`.

**Refusing production slots from other environments.** A connection marked `changes.production: true` may only create or use a slot when the active environment is `environments.base` (or the project has no environments). Otherwise the run fails as `config` (`slot_refused`) before connecting. A dev run of a read-only prod source (legal through `read_only` or `allow_base`) is the case this catches. A dev override pointing at a dev server sets `changes: {production: false}`.

**Backfill.** A stream without a cursor (first run, a new stream, or after `state reset`) is copied in full with `COPY … TO STDOUT` (text format, the same output GUCs as the walsender) under `REPEATABLE READ`, staged as a truncate followed by inserts, and then the stream continues from the slot. The copy starts after the slot's position is fixed and `head` is read after the copy, so replaying the slot's changes over the copy converges on the state at `head`. There is no exported snapshot: one code path covers the first run and streams added later.

**Protocol v2.** Options `proto_version '2', streaming 'on'`, plus `binary 'true'` when every published column's type has a binary decoder: built-in scalars, text types, json/jsonb, uuid, the date/time types, interval, numeric, enums, domains over these, and arrays of any of them. A column of another type (`inet`, `money`, a PostGIS type, …) turns binary off for the window, with an info log. If a Relation message brings such a type mid-window (a column added and dropped again since, so the catalog check didn't see it), the window drops its streamed changes (the backfill stays) and restarts from the slot's confirmed position with text values; with a temporary slot, which wouldn't survive the reconnect, it fails as `transient` (`binary_type`) instead. Binary values are rendered exactly as the server's text output under the walsender's GUCs (UTC, ISO dates, `postgres` intervals, `extra_float_digits = 3`, hex bytea), so keys, staging and casts are unchanged.
- **Streamed transactions.** A transaction over `logical_decoding_work_mem` arrives as Stream Start/Stop blocks before its commit, each change tagged with its (sub)transaction's xid. Each (sub)transaction stages under its own ordinal with provisional sequence numbers above every committed change. Stream Abort drops the subtransaction's rows (or the whole transaction's), and Stream Commit renumbers the rows after the last committed change, so staging is in commit order again and netting and truncates work unchanged.
- **Open at the end of a window.** A streamed transaction that hasn't committed when the window stops is dropped from staging. Its commit is after the confirmed position, so the server streams it again from the start on a later run.
- The server doesn't stream a transaction that is already aborted when it's decoded (PG 16 checks up front; PG 14 notices on its first catalog lookup), so a whole-transaction Stream Abort only arrives when the abort races the decoding.

**Netting.** Per stream and window, changes net to the last one per key after the last truncate. A delete then an insert is an upsert; an insert then a delete is nothing. An update that changes the key is a delete of the old key and an upsert of the new. Unchanged TOAST columns (`u`) take the key's latest value earlier in the window (since its last insert or key change), else the lake row of the key the chain started from. A key-changing update carries them from the old key. Values are cast in DuckDB to the lake column's type, or, for a new column, to the mapped type (following DuckDB's postgres scanner: `numeric(p,s)` → `DECIMAL(p,s)` up to 38 digits, else `DOUBLE`; `jsonb` → `JSON`; `bytea` → `BLOB`; one-dimensional numeric and boolean arrays → lists; unmapped types → `VARCHAR`). New source columns are added to the lake table.

**Truncate.** Applied, not failed: the truncate is ordered in the WAL, so the lake table is emptied (hard), all rows marked deleted (soft), or a delete row appended per live key (history), and the changes after it apply on top. The same path performs a backfill resync.

**Apply and confirm.** One lake transaction per replication holds every stream's changes and each stream's `lsn` cursor (`{"lsn": "0/16B3748", "slot": "atlatl_appdb_cdc"}`), including streams with no changes. The cursor advances even when no published table changed, so empty transactions and idle keepalives keep the slot from pinning WAL; such a run commits only state. Only after COMMIT is the lowest stored cursor sent as the standby status. If the replication connection dropped during a long commit, it reconnects to confirm. A crash between the commit and the confirm redelivers transactions that are then skipped by LSN, so the lake applies each transaction exactly once.

**Runner.** `atlatl run` takes the replication's target table locks, stages the window, applies it on the blocking pool, confirms, then reports. A replication runs all its streams even when `--stream`/`--tag` select fewer (one slot covers them; a warning is logged), can't join an `atomic_group`, and bounds its window to half its `timeout` when that is shorter than `max_duration`. A kill while streaming drops the window unconfirmed; a kill during the commit lets it finish, and the unconfirmed transactions are skipped next run. The status is `up_to_date` when the window had no changes and nothing was backfilled.

**Errors.** `data`: `slot_missing` (cursors but no slot), `cursor_gap` (slot confirmed past a cursor, or a cursor on a table the publication didn't cover). `config`: `changes_disabled`, `slot_refused`, `slot_absent` / `publication_absent` / `publication_mismatch` (would need `allow_create`), `slot_foreign` (the name is taken by a non-pgoutput slot or another database). `conflict`: `slot_active` (another walsender holds the slot for over 10 s) and SQLSTATE `55006`.

**Report.** Each run reports the window (`from`, `head`, `upto`, transactions, skipped transactions, changes, unchanged-TOAST changes, the stop reason, backfilled streams), per-stream counts, and the slot's stats: `lag_bytes` (`pg_current_wal_lsn() - confirmed_flush_lsn`), `retained_bytes` (from `restart_lsn`), and `catalog_xmin_age`.

## 11. Reporting

A CDC replication reports, per stream:
- `window: {from_snapshot, to_snapshot, from_time, to_time}`;
- `touched_keys`, `upserts_sent`, `rows_inserted`, `rows_updated`, `rows_unchanged` (filtered by the `IS DISTINCT FROM` guard), `rows_deleted`;
- `snapshots_behind`, `seconds_behind`;
- `resynced: bool`.
