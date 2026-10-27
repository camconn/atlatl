# 05. State

State is atlatl's persisted memory between runs. It covers:
- `update_key` watermark values;
- REST `sync` values;
- CDC snapshot cursors;
- rejects awaiting retry ([03 §7](03-execution.md#7-rejects));
- run records.

The rule: **state advances in the same transaction as the data it describes.** Where that is impossible (D1), apply is made idempotent and the cursor moves only after the final write ([08 §6](08-cdc.md#6-target-appliers)).

## 1. Where state lives

| State | Store | Why |
|---|---|---|
| Ingest `update_key` values and REST `sync` values (lake targets) | **In the lake**: `catalog._atlatl.stream_state`, a DuckLake table | Written in the replication's single DuckLake commit, so data and state land in one snapshot, exactly once |
| Rejects | **In the lake**: `catalog._atlatl.rejects` | Same commit as the data ([03 §7](03-execution.md#7-rejects)) |
| CDC cursors (serving targets) | **In the target**: `atlatl.cursors` on Postgres (a dedicated schema, outside Atlas-managed `dagster`/`app`), and `_atlatl_cursors` on SQLite/D1/Turso | Committed in the target transaction that applies the changes |
| CDC consumer registry (**later**) | `catalog._atlatl.consumers` in the lake | So lake maintenance can see the oldest snapshot any consumer still needs, once snapshot expiry is enabled ([08 §5](08-cdc.md#5-retention-guard)) |
| Run records | `catalog._atlatl.runs` / `run_replications` in the lake (append-only, in its own small commit per replication outcome); the run report file | Audit, history, and `--skip-succeeded` for Dagster retries |
| Locks | Postgres advisory locks (not tables) | [03 §6](03-execution.md#8-locks) |

- `state.lake` may also name a plain DuckDB file target ([06 §10](06-connectors.md#10-duckdb-files)): the same `_atlatl` tables then live in that file and commit in its DuckDB transaction; `lake_snapshot` is null there.
- `StateStore` is a trait ([11](11-extensibility.md)). A future non-lake target (for example ingesting into ClickHouse) supplies its own implementation, or uses a lake-hosted store with an idempotent apply.
- DuckLake data inlining keeps these tiny tables in the catalog database rather than as small Parquet files. A startup check sets or verifies `DATA_INLINING_ROW_LIMIT` for the `_atlatl` schema.

## 2. Schema (lake)

```sql
CREATE TABLE _atlatl.stream_state (
  stream_id      VARCHAR,     -- config id
  member         VARCHAR,     -- '' for single-member streams; 'schema.table' for wildcard members
  kind           VARCHAR,     -- 'update_key' | 'sync' | 'lsn'
  value          JSON,        -- typed payload, see below
  config_hash    VARCHAR,     -- hash of the stream config that produced it
  run_id         VARCHAR,     -- the run that last changed `value` (§4)
  lake_snapshot  BIGINT,      -- snapshot this state was committed in (informational)
  updated_at     TIMESTAMPTZ  -- when `value` last changed
);
-- logical key (stream_id, member, kind); upserted with MERGE

CREATE TABLE _atlatl.runs (         -- one row per run or manual state action
  run_id VARCHAR, external_run_id VARCHAR, env VARCHAR,
  kind VARCHAR,                      -- 'run' | 'state set' | 'state reset' | …
  status VARCHAR, plan_hash VARCHAR, atlatl_version VARCHAR, detail JSON, recorded_at TIMESTAMPTZ
);

CREATE TABLE _atlatl.run_replications (   -- one row per replication outcome; `--skip-succeeded` reads it
  run_id VARCHAR, external_run_id VARCHAR, replication VARCHAR,
  status VARCHAR,                    -- report status (succeeded | up_to_date | failed | …)
  attempts INTEGER, lake_snapshot BIGINT, error_class VARCHAR, error VARCHAR, recorded_at TIMESTAMPTZ
);

CREATE TABLE _atlatl.consumers (   -- later: only needed once snapshot expiry is enabled
  consumer_id    VARCHAR,     -- '<target conn>/<stream id>'
  source_table   VARCHAR,     -- 'au.org_event'
  cursor_snapshot BIGINT,     -- last applied snapshot
  cursor_time    TIMESTAMPTZ,
  updated_at     TIMESTAMPTZ
);
```

`value` payloads:
- `update_key`: `{"column": "publication_date", "type": "DATE", "value": "2025-05-01"}`. Values are exact and typed, never float-cast.
- `sync` (REST): `{"<key>": <json>, …}`, the values an endpoint declared in `sync:` ([07 §11](07-rest-api.md#11-sync-state)).
- `lsn` (postgres logical replication): `{"lsn": "0/16B3748", "slot": "atlatl_<replication>"}`, the commit `end_lsn` applied up to, written in the same lake commit as the changes; the slot is confirmed only afterwards ([08 §10.1](08-cdc.md#101-postgres-logical-replication-m6)).

## 3. Schema (Postgres target)

```sql
CREATE SCHEMA IF NOT EXISTS atlatl;
CREATE TABLE IF NOT EXISTS atlatl.cursors (
  stream_id        text PRIMARY KEY,
  source_table     text NOT NULL,
  source_table_id  bigint NOT NULL,        -- DuckLake table_id at snapshot_id (identity guard, 08 §3)
  snapshot_id      bigint NOT NULL,        -- last applied DuckLake snapshot (inclusive)
  snapshot_time    timestamptz,
  mode             text NOT NULL,          -- 'cdc' | 'bootstrap'
  config_hash      text NOT NULL,
  updated_at       timestamptz NOT NULL DEFAULT now()
);
```

- atlatl creates the `atlatl` schema on first use, unless the connection sets `manage_state_schema: false`, in which case Atlas creates it. Either way atlatl checks the table has every column above, and fails with `config` if not.
- Atlas's `atlas.hcl` must exclude it. That's an open question, since the team may prefer to manage it in Atlas.

## 4. Semantics

- **Read:** at replication start, inside the lake transaction's snapshot for lake state, or the target's for CDC state.
- **Write:** only inside the replication's commit. A failed or killed replication leaves state untouched.
- **Only when the value moves.** An ingest row (`update_key`, `sync`) is upserted only if its new `value` differs from the stored one. `run_id`, `lake_snapshot` and `updated_at` are therefore those of the run that last moved the value, not of the last run, and an unchanged rerun writes no state and commits nothing ([04 §1](04-ingest.md#1-replication-lifecycle)). A value that moves with no row changing (a `sync` floor stepping forward, a rewound watermark catching up) still commits.
- **Config drift:** `config_hash` covers the fields that change what stored state means: source query, window, `update_key`, PK (and `members` keys, when set). If it changes, the replication fails with `config` ("cursor was produced by a different definition"). The fix is an explicit `atlatl state reset|accept`, because silently continuing could skip data.
- **Member lifecycle:** a wildcard member that stops appearing keeps its state, and it is reported as `member_missing`. A new member starts with no state.

## 5. CLI

| Command | Effect |
|---|---|
| `atlatl state show [--stream S]` | Print state (JSON); a stream's `for_each` instance states are listed with it |
| `atlatl state set <stream> [--member M] --cursor <value>` | Rewind or advance a cursor. Logged in `_atlatl.runs` as a manual action |
| `atlatl state reset <stream>` | Delete state (the next run is a full extract or bootstrap) |
| `atlatl state accept <stream>` | Re-stamp `config_hash` after a reviewed config change |
| `atlatl state init <stream> --from-target [--member M …]` | Seed a cursor from `max(cursor column)` of the existing target (adopting a table another loader wrote). A `from.tables` stream names the members to seed, since discovery needs the source; each is seeded from its own target table |
| `atlatl state move <old-id> <new-id>` | Rename a stream id without losing state |

**`for_each` instances** keep state under `<stream>@<key>` ([11 §3](11-extensibility.md#3-larger-pipeline-graphs)). Every command takes that id in place of the stream, or `--instance <key>` (a global flag of `atlatl state`):
- a bare stream id means the stream and every instance for `show`, `reset` and `accept`;
- `move <old> <new>` renames the stream's state and each `<old>@<key>` to `<new>@<key>`, while `--instance` (or `<old>@<key> <new>@<key>`) moves one instance;
- `set` and `init` write one row, so a `for_each` stream needs the instance there. Against a literal list, the key must be one of the list's, which `accept` checks too. `init` can't seed an instance of a `for_each.over.query` replication, whose items are only listed by a run.

All of these take the stream's table lock.
