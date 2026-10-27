# Spike S3: DuckLake transactions

Date: 2026-09-28. Proof: the S3 spike (code since removed; its checks are now tests: §1 in `atlatl-ingest` `src/modes/tests.rs` and `atlatl-state` `src/tests.rs`, §2 in `atlatl-cdc` `tests/feed.rs`, §3 in `atlatl-conn-ducklake` `tests/conflicts.rs` and `atlatl-state` `tests/concurrent.rs`, and the §4/§6 memory harness in `atlatl-ingest` `tests/memory.rs`, `#[ignore]`d). DuckDB 1.5.3 and ducklake `e6a3bd0a`, loaded from the bundled extension directory (`scripts/smoke-test-prefix.py`). Two catalogs were used: a DuckDB-file catalog on local disk, and a Postgres 16 catalog in a container.

## Verdict

- **Atomicity: confirmed.** One `BEGIN … COMMIT` spanning DDL, MERGE and writes to several tables lands as **one snapshot**. `DELETE` + `INSERT` keeps the table id. An unchanged merge commits nothing.
- **Conflicts: understood, and the spec changes.** DuckLake's own commit retry must stay **on**. Logical conflicts are detected per table and surface as `TransactionContext Error`.
- **Chunked memory: confirmed, with a design constraint.** DuckLake writes data files during the transaction, so transaction state stays flat. However, **TEMP tables created or dropped inside the commit transaction are held until commit and do not spill**. Chunked applies must merge each chunk straight from its source relation, not from a per-chunk TEMP table (§4).

## 1. One snapshot per transaction

```sql
BEGIN;
ALTER TABLE lake.br.t ADD COLUMN extra INTEGER;
MERGE INTO lake.br.t AS tgt USING stg AS s ON tgt.id = s.id
  WHEN MATCHED AND (tgt.v, tgt.extra) IS DISTINCT FROM (s.v, s.extra) THEN UPDATE SET v = s.v, extra = s.extra
  WHEN NOT MATCHED THEN INSERT (id, v, extra) VALUES (s.id, s.v, s.extra);
MERGE INTO lake._atlatl.stream_state … ;      -- a second table (the state row)
COMMIT;
```

- Snapshots go 4 → 5. Snapshot 5's `changes` are `{tables_altered=[3], tables_inserted_into=[3, 4], tables_deleted_from=[3]}`.
- The snapshot is invisible inside the transaction (`ducklake_snapshots` still reports 4).
- `MERGE … WHEN MATCHED AND (…) IS DISTINCT FROM (…)` works. An UPDATE is recorded as a delete plus an insert.
- `CREATE SCHEMA` / `CREATE TABLE` of `_atlatl.*` can precede it in the same session.

| Case | Snapshots |
|---|---|
| a MERGE that changes nothing (guard filters every row) | none |
| `BEGIN; COMMIT;` | none |
| `ROLLBACK` after inserting 1,000 rows | none. The rows are invisible, and the data files written during the transaction are removed (none orphaned) |

## 2. Table identity

| Statement | `ducklake_table.table_id` |
|---|---|
| `BEGIN; DELETE FROM t; INSERT INTO t …; COMMIT;` | **unchanged** (2 → 2) |
| `CREATE OR REPLACE TABLE t AS …` | new id (2 → 4). The old entry gets `end_snapshot` and the snapshot records `tables_dropped` + `tables_created` |

So 04 §4.1's `full-refresh` (DELETE + INSERT, or diff-replace) keeps identity, and 08 §3's table-identity guard is right to treat a new `table_id` as `resync_required`.

## 3. Commit conflicts

Two independent engine instances, standing in for two processes, attached one Postgres catalog. Each opened a transaction, both did their writes, A committed, then B committed:

| B's commit, after A committed | `ducklake_max_retry_count = 0` | default (`10`) |
|---|---|---|
| A inserts x, B inserts x | fails | **commits** |
| A updates x, B updates y | fails | **commits** |
| A updates x, B updates **other rows** of x | fails | fails: `…attempting to insert into table with index "23" - but another transaction has deleted from it` |
| A merges x, B merges other rows of x | fails | fails (the same) |
| A deletes from x, B inserts into x | fails | fails (`…has deleted from it`) |
| A alters x, B inserts into x | fails | fails (`…has altered it`) |
| A inserts x, B deletes all + inserts x | fails | fails (`attempting to delete from table … has inserted into it`) |

- **With retries off, every concurrent commit fails**, even on disjoint tables. The error is `Failed to flush changes into DuckLake: … duplicate key value violates unique constraint "ducklake_snapshot_pkey"`. The two commits race for the next snapshot id, and DuckLake's retry is what resolves that race, by re-reading the latest snapshot and re-checking.
  - **So 06 §2 is wrong** to set DuckLake's retries "conservatively so atlatl's retry is in control". They stay on (the default is `ducklake_max_retry_count=10`, `ducklake_retry_wait_ms=100`, `ducklake_retry_backoff=1.5`; the names are verified). atlatl retries only what DuckLake reports as a logical conflict.
- **Logical conflicts are per table, not per row.** Two transactions that delete from (or update) the same table always conflict, even on disjoint rows. Appends don't conflict with appends. ALTER conflicts with any write. A dbt `delete+insert` on a table atlatl is writing will conflict.
- **What the error looks like:**
  ```
  TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction.
  Transaction conflict - attempting to <insert into|delete from> table with index "<table_id>" - but another transaction has <deleted from|inserted into|altered> it
  ```
  The message names the `table_id`, not the table. The error mapping ([03 §5](../../spec/03-execution.md#5-error-taxonomy)):
  - `TransactionContext Error` + `Transaction conflict` → `conflict`, code `commit_conflict`, with the id resolved to a name through the catalog for the message;
  - `Failed to flush changes into DuckLake` + `ducklake_snapshot_pkey` → `conflict` (DuckLake retries exhausted).
- Detection is fast: about 70–110 ms per failed commit against a local Postgres.
- **`_atlatl.stream_state` is a shared hot table.** Every state write is a MERGE, and advancing an existing cursor rewrites its row. So two replications committing at the same time conflict on it even when their data tables are disjoint, both across processes and between two connections of one engine (`--concurrency`). First cursors (inserts only) don't conflict. The commit-level retry resolves it; `atlatl-state` `tests/concurrent.rs` checks both cases.
- **Retrying.** A DuckDB transaction that failed to commit is aborted. The commit-level retry ([03 §6](../../spec/03-execution.md#6-retry)) therefore re-runs the whole apply transaction (`BEGIN`, DDL, merges, state, `COMMIT`) against the retained staging. It does not re-commit the old one.

## 4. Memory of a chunked 10M-row merge in one transaction

Target `lake.big`: 10M rows × `(BIGINT, INT, VARCHAR(32), VARCHAR(32), DATE)`, 666 MB of parquet. Source: 11M rows, which is 5.5M changed rows, 4.5M unchanged rows (filtered by the `IS DISTINCT FROM` guard) and 1M new rows. There are 10 chunks by `hash(id) % 10`, the settings are `threads=4` and `preserve_insertion_order=false`, and the catalog is a DuckDB-file catalog with local data. One snapshot every time.

| Variant | `memory_limit=1GB` | `memory_limit=300MB` |
|---|---|---|
| One unchunked MERGE | ok, **peak RSS 2,025 MB** | **OOM** |
| Chunked; each chunk `CREATE TEMP TABLE stg AS …` → MERGE → `DROP` **inside** the transaction | ok, peak RSS 1,247–1,304 MB; **DuckDB memory grows 123 → 590 MB** (`IN_MEMORY_TABLE`) | **OOM** |
| All chunks staged as TEMP tables **before** `BEGIN`, then merged in one transaction | ok, peak RSS 1,641 MB. Staging spilled to `temp_directory` (744 MB at the 300 MB limit) | OOM during the merge |
| **Chunked; MERGE straight from the chunk's source relation** (`USING (SELECT … WHERE hash(id) % 10 = k)`) | ok, peak RSS 1,312 MB; DuckDB memory **flat at about 0 MB** between chunks | **ok, peak RSS 721 MB** |

What this shows:
1. **DuckLake writes data files as the transaction goes.** Parquet files in the data path went 1 → 5 before `COMMIT` in §1, and 6 → 24 across the chunks here. Transaction-local DuckLake state doesn't accumulate: in the direct variant, tracked memory returns to about 0 after each chunk.
2. **TEMP tables inside the commit transaction are the problem.** A table dropped inside an open transaction is kept until the transaction ends (MVCC, in case of rollback), and that memory neither spills nor frees. Ten 1.1M-row chunks cost about 52 MB each, so a 66M-row full merge would need gigabytes and fail under any fixed budget.
3. **RSS exceeds `memory_limit`.** It ran 2.4× at 300 MB and 1.3× at 1 GB: parquet writers, per-thread buffers and other allocations outside the buffer manager. That supports the `duckdb_share` headroom in 01 §5, which the engine milestone (M0.9) should size from measurements.

**Design consequence, spec change to 04 §3 and §7:**
- **No data-sized TEMP tables are created inside the commit transaction.** Unchunked streams are staged before `BEGIN`, in autocommit, where the staging can spill. Retries of the commit reuse that staging.
- **Chunked streams are applied from their source relation**, chunk by chunk, inside the transaction: `MERGE … USING (<pinned chunk relation>)`, for example `read_parquet(<file group>)`, `catalog.s.t AT (VERSION => n) WHERE hash(pk) % N = k`, or a Postgres scan with the chunk predicate.
  - The chunk's invariants (row count, NULL and duplicate PKs) are checked by a query over the same relation just before its MERGE. An extra read of the chunk replaces the staged copy.
  - The source must be pinned (UNLOAD files by path, a lake snapshot), so that a commit retry re-reads identical data.
- The chunked `full-refresh`-with-PK "keys seen" set is PK-only and small, but it still lives until commit. Estimate it before relying on it for very large tables, or compute the deletes with an anti-join against the pinned source instead.

## 5. Other facts

- **ICU is required** *(with the bundled build only; the official libduckdb has ICU built in, see §6)*. The bundled build has no ICU.
  - Without it, `current_date` doesn't exist and `TIMESTAMPTZ → DATE` casts fail ("Unimplemented type for cast").
  - Worse, several functions silently use the **host's local time zone**: `date_trunc('day', now())` gave `00:00:00-04`, and `to_timestamp(…)` gave `-05`.
  - The `icu` extension (about 20 MB per platform) is added to the bundled set. The engine runs `LOAD icu; SET TimeZone = 'UTC'` at startup, which makes timestamp semantics deterministic and matches 07 §9 ("`now()` is always UTC").
- `DATA_INLINING_ROW_LIMIT` defaults to 10 (`ducklake_default_data_inlining_row_limit`). Small writes (state rows, small tables) are inlined into the catalog. Conflict messages then mention "inlined data", and the rules are the same.
- DuckLake settings exist: `ducklake_max_retry_count`, `ducklake_retry_wait_ms`, `ducklake_retry_backoff`, `ducklake_default_data_inlining_row_limit` and `ducklake_write_deletion_vectors` (false).

## 6. Addendum: official libduckdb (jemalloc)

The semantics (§1–§3, including the conflict matrix) were re-run against the official dynamically linked libduckdb (`.github/workflows/release.yml`) and are unchanged. Memory changed, because the official build uses jemalloc. The workload is the same 10M-row merge as in §4:

| Variant | `memory_limit=1GB` | `300MB` | `500MB` |
|---|---|---|---|
| One unchunked MERGE | ok, RSS 1,447 MB | **OOM** (2/2 repeats; one earlier run passed, so it's flaky at this edge) | ok, RSS 1,014 MB |
| Chunked, TEMP staging inside the txn | ok, RSS 1,912 MB | OOM | ok, RSS 1,172 MB |
| **Chunked, MERGE straight from the chunk relation** | — | **ok, 2/2**, RSS 1,306–1,477 MB | ok, RSS 1,525 MB |

Under a real cgroup cap (`systemd-run --scope -p MemoryMax=… -p MemorySwapMax=0`), with direct chunks and `memory_limit=300MB`:

| `MemoryMax` | `threads` | Result |
|---|---|---|
| 1500M | 4 | ok, peak RSS 936 MB |
| 1000M | 4 | **OOM-killed** (137) |
| 800M | 2 | **OOM-killed** |
| 800M | 1 | ok, peak RSS 755 MB, 217 s |

What changes relative to §4:
- **§4's design conclusion stands.** The only variant that completes at a 300 MB DuckDB limit is direct per-chunk merges from the source relation. TEMP staging inside the transaction still accumulates until commit.
- **But `memory_limit` bounds only DuckDB's buffer manager.** Memory outside it (jemalloc caches, per-thread parquet writer and scan buffers, the httpfs/parquet metadata) came to **roughly 450–700 MB on top of a 300 MB limit, and grows with `threads`**. This RSS is real: the kernel's cgroup OOM killer counts it.
  - Lowering `allocator_flush_threshold` to 16 MB with background threads did **not** help. All three variants then failed with DuckDB-level OOM at 300 MB.
- **For the engine (M0.9), `duckdb_share = 0.6` of the container limit is too optimistic at small budgets.** The budget should be `memory_limit = container − fixed overhead − threads × per-thread overhead`. Calibrate it with this harness (`cargo test -p atlatl-ingest --release --test memory -- --ignored --nocapture`, which runs the real chunked apply on an engine sized by `Budget::compute` and fails if peak RSS exceeds `engine.memory`) under cgroup caps at the pod sizes actually used (the `large` / `xlarge` profiles). Keep `threads` low for memory-bound merges; `threads` is the main lever (01 §5 already says so).
- The earlier ICU finding (§5) no longer applies: the official build has ICU. `SET TimeZone = 'UTC'` is still required.
- **Re-measured through the real apply path (2026-09-29, `atlatl-ingest` `tests/memory.rs`).** 10M rows, 10 hash chunks merged from a pinned parquet source in one transaction, `threads = 2`, `engine.memory = 1536 MiB`, so `BudgetParams::default()` gives `memory_limit = 794 MiB`. Uncapped peak RSS was **1,618 MiB** (merge 177 s), and under `MemoryMax=1536M` the process was **OOM-killed**. About 820 MiB lived outside `memory_limit`, against the 550 MiB reserved (`fixed_overhead` 400 + 2 × 75; the 192 MiB `channel_bytes` share goes unused by a merge). With `memory: auto` the 0.85 fraction covers the gap; an explicit `engine.memory` does not.

### 6.1 `BudgetParams` calibration (2026-09-29)

*Superseded by §6.2: the memory measured here was allocator retention, now released, and the overheads are lower.*

Same harness and workload (10M rows, 10 hash chunks, one transaction, `TMPDIR` on disk). "Outside" is peak RSS (`VmHWM`) minus `memory_limit`. Setup (building the target and source) peaks at about 120 MiB, so every peak below is the merge. Timings are noisy, because the host was shared with other builds.

| `threads` | `engine.memory` | `memory_limit` | Variant | Peak RSS | Outside | Under `MemoryMax` |
|---|---|---|---|---|---|---|
| 1 | 1536 | 869 | | 1,350 | 481 | |
| 1 | 2048 | 792 | | 1,252 | 460 | |
| 2 | 1536 | 794 | 3 repeats | 1,581–1,743 | 787–949 | 1536M: **OOM-killed** (137) |
| 2 | 1536 | 794 | 5 chunks | 1,570 | 776 | |
| 2 | 1536 | 794 | each chunk its own transaction | 1,371 | 577 | |
| 2 | 1536 | 794 | 5M rows | 1,398 | 604 | |
| 2 | 1536 | 794 | `allocator_background_threads`, `allocator_flush_threshold = 16MB` | 1,481 | 687 | |
| 2 | 2560 | 1754 | | 2,591 | 837 | |
| 2 | 2048 | 692 | 2 repeats | 1,262–1,525 | 570–833 | 2048M: ok, peak RSS 1,267–1,482 |
| 4 | 1536 | 644 | | 1,462 | 818 | |
| 4 | 2048 | 492 | | 1,707 | **1,215** | 2048M: ok, peak RSS 1,526 |
| 4 | 2560 | 904 | | 1,735 | 831 | 2560M: ok |
| 2 | 1536 | 244 / 307 | new defaults / `duckdb_share = 0.2` | DuckDB OOM | | |
| 4 | 2048 | 392 | new defaults | DuckDB OOM | | 2048M: DuckDB OOM |

- **Fit.** The worst case per thread count is 481 / 949 / 1,215 MiB at 1 / 2 / 4 threads. Run-to-run variance (577–949 MiB at 2 threads) is larger than the thread effect, so the fixed/per-thread split is loose. The overhead measured at the first chunk, before anything accumulates, is about 140 / 200 / 290 MiB, roughly 90 + 50 × threads. The rest builds up across chunks. `fixed_overhead = 800 MiB` and `per_thread_overhead = 150 MiB` reserve 950 / 1,100 / 1,400 MiB, so each worst case is covered by at least 15% (16% at 2 threads, 15% at 4).
- **Outside doesn't scale with `memory_limit`:** 837 MiB at 1,754 MiB and 787–949 MiB at 794 MiB (2 threads). An additive reserve is therefore the right shape, and `duckdb_share` is not.
- **The harness default moved from 1536 to 2048 MiB.** At 1536 MiB and 2 threads, the new defaults leave a 244 MiB limit. A 1.1M-row chunk needs more than 307 MiB at 2 threads (20 chunks fail too) and more than 392 MiB at 4. No safe reserve fits this workload in 1536 MiB until the retention below is fixed. With 2048 MiB it passes at 1 and 2 threads, uncapped and under `MemoryMax=2048M -p MemorySwapMax=0`. At 4 threads it needs 2560 MiB, and passes under a 2560M cap.
- **Too small an `engine.memory`** still clamps `memory_limit` to 128 MiB and sets `clamped` (01 §5), rather than failing as a config error. The run may then exceed `engine.memory`.

**C.4 leads** (cheap diagnostics collected alongside; not investigated further):
- **RSS climbs chunk by chunk while DuckDB's tracked memory is flat.** Between chunks, `duckdb_memory()` totals 4 MiB (`ART_INDEX` 2 MiB, `IN_MEMORY_TABLE` 1 MiB, nothing else and no spill). RSS after each chunk still roughly doubles between chunk 0 and chunk 9 (350–710 → 940–1,580 MiB at a 492–869 MiB limit), and the peak rises with it. Whatever holds this memory is outside the buffer manager and isn't DuckLake transaction state: committing each chunk separately grows the same way (465 → 1,270 MiB), and `TRANSACTION` is 1 MiB.
- **It is not lazily freed pages.** `LazyFree` (`MADV_FREE`d jemalloc pages, from `smaps_rollup`) peaks at 150–590 MiB, but RSS minus `LazyFree` still reaches 1,230–1,680 MiB. The kernel also OOM-kills the 1536M cgroup run with the lake on disk, so the earlier OOM wasn't tmpfs pages charged to the cgroup either.
- **Part of it is dirty retention that decays.** Five idle seconds after `COMMIT`, RSS falls by up to 640 MiB, but by almost nothing in some runs (for example 1,579 → 1,156 and 1,592 → 1,246). Another 550–1,250 MiB stays resident with nothing tracked. That looks like jemalloc retention or fragmentation across DuckDB's per-thread arenas, or allocations DuckDB never counts (parquet writer and reader buffers, the DuckLake metadata cache). Next steps: jemalloc `stats.print` or heap profiling of the harness, and a glibc-malloc build for comparison.
- **The allocator knobs help only a little:** `allocator_background_threads = true` with `allocator_flush_threshold = 16MB` gave 687 MiB outside against 787–949.
- **Fewer, larger chunks don't change the outside share:** 5 chunks gave 776 MiB against 787–949 MiB for 10. **1 thread about halves it** (460–481 MiB). **Half the rows** gives about two thirds (604 MiB). So it scales with the data processed and with threads, not with the number of chunks.
- The harness prints all of this. Every run logs per-chunk peak RSS, RSS, `LazyFree` and tracked memory, 200 ms samples of RSS against `duckdb_memory()`, the tags at `COMMIT`, RSS after `COMMIT` and 5 s later, and the cgroup `memory.peak`. The knobs are `ATLATL_MEMTEST_SHARE` (a fixed `memory_limit`), `_SQL` (settings) and `_TXN_PER_CHUNK=1`.

### 6.2 Where the memory outside `memory_limit` went (C.4, 2026-09-29)

Same harness. The official libduckdb v1.5.3 (`14eca11bd9`) is used, with its bundled jemalloc `5.3.0-196-ga25b9b8b` and DuckLake `e6a3bd0a`. The host runs glibc 2.41. Allocator counters come from `atlatl_engine::alloc::stats()`: jemalloc through the `duckdb_je_mallctl` that libduckdb exports, and glibc through `mallinfo2`. The Rust heap stays at about 0 throughout, measured with a counting global allocator that has since been removed.

**Causes.** Between chunks, `duckdb_memory()` tracks 4 MiB and live allocations total about 5 MiB of jemalloc and 5–14 MiB of glibc. RSS still climbs chunk by chunk. Three things account for it:

1. **jemalloc dirty pages that nothing purges.** DuckDB's buffer-managed blocks come from jemalloc, configured with `dirty_decay_ms:1000,muzzy_decay_ms:1000,narenas:16` and no background thread. Decay runs only when the same arena allocates again, so an arena that goes quiet keeps its freed pages. DuckDB's own flushes rarely fire:
   - `Allocator::ThreadFlush` runs only on worker threads, only when the thread's peak exceeds `allocator_flush_threshold`, and only for that thread's own arena. It is skipped entirely when `allocator_background_threads` is on.
   - `FlushAll` (purge all arenas) runs after a bulk deallocation larger than `allocator_bulk_deallocation_flush_threshold`, 512 MiB by default.

   Between chunks, jemalloc held 200–800 MiB dirty against about 5 MiB allocated. Some of it decayed to muzzy pages: those are `MADV_FREE`d, so they no longer count in jemalloc's resident figure, but they stay in RSS as `LazyFree` until the kernel reclaims them. The same pattern appears without atlatl or DuckLake (C API, official library, `threads = 2`, default limit):

   ```sql
   CREATE TABLE t AS SELECT i, md5(i::VARCHAR) AS s FROM range(3000000) r(i);
   CREATE TABLE u AS SELECT s, count(*) AS n FROM t GROUP BY s ORDER BY s;
   DROP TABLE u; DROP TABLE t;
   ```

   After this, `duckdb_memory()` shows 0 MiB and jemalloc 0 MiB allocated, yet 500–580 MiB is dirty and RSS is 560–650 MiB (from 32 MiB at open). RSS stays there across 5 repeats and after 10 idle seconds (400 MiB with the next query's activity). At `memory_limit = 300MB` it is 380–400 MiB. Of the DuckDB settings:
   - `allocator_flush_threshold = 1MB` does nothing, alone or with a 1 MB bulk threshold;
   - `allocator_background_threads = true` brings RSS down to 48 MiB only after 10 idle seconds, not between statements;
   - `mallctl("arena.4096.purge")` brings it down to 43–46 MiB immediately.
2. **glibc heap retention from DuckLake deletes.** Everything not buffer-managed comes from glibc `malloc` (`operator new`). A DuckLake `DELETE`, which a `MERGE` update also performs, leaves glibc holding its freed memory. Over 10 `DELETE FROM lake.big WHERE hash(id) % 10 = k` on a 3M-row DuckLake table (autocommit), glibc free space grows 33 → 582 MiB and RSS 156 → 664 MiB, while glibc in-use stays at 5 MiB and tracked memory at 4–8 MiB. None of the other statements grow the glibc free space: the same `DELETE`s and chunked `MERGE` on a native DuckDB table, `COPY … TO` parquet per chunk, and DuckLake `INSERT` per chunk all hold it at 5–14 MiB. It is fragmentation, not heap-top slack: `GLIBC_TUNABLES` `mmap_threshold` 128 KiB or 1 MiB, `trim_threshold` 1 MiB and `arena_max=1` all still grow it, to 344–654 MiB. `malloc_trim(0)` returns it: RSS drops to 79–105 MiB. In the harness it adds 35–70 MiB of RSS per 1.1M-row chunk (glibc free 70 → 730 MiB over 10 chunks at 10M rows).
3. **Untracked memory while a chunk runs.** glibc in-use peaks at 155 MiB (3M rows) and 405 MiB (10M rows, 1.1M-row chunks) *during* a chunk, then falls to 5–14 MiB. That is transient, outside `memory_limit`, and scales with the chunk's size, not with threads. The delete bookkeeping in (2) is the likely owner. jemalloc dirty pages also build up within a single chunk, 150–800 MiB. The process baseline is about 70 MiB: libduckdb's text plus the heaps at rest.

Ruled out, besides §6.1's list:
- the Rust heap;
- `MALLOC_CONF`, which the bundled jemalloc doesn't read (DuckDB formats its own `malloc_conf`);
- glibc tunables;
- `allocator_background_threads`: 815 MiB peak against 769–827 MiB at 3M rows, because decayed pages turn muzzy and stay in RSS.

**Fix (ours).**
- `atlatl_engine::alloc::release()` purges every jemalloc arena (`arena.4096.purge`, dirty and muzzy) and runs `malloc_trim(0)`. Both symbols are looked up with `dlsym` at first use, only on Linux/glibc; anything missing is skipped. The call takes 15–210 ms, and the time grows with what it frees (0.3–1.2 GiB here). The engine calls it:
  - between chunks: one call in `atlatl-run` `lake.rs` after each chunk's apply;
  - after any engine call or stream that ran for at least 1 s. That covers `COMMIT`, staging and long exports.
- At 2 or more threads the engine opens DuckDB with `allocator_bulk_deallocation_flush_threshold = 4MiB`, so DuckDB purges during a chunk as well. 16 MiB and 64 MiB behaved like the default. At 1 thread it gained nothing (380 vs 386 MiB) and slowed the merge from 230 s to 299–309 s, so it isn't set there.
- `alloc::set_enabled(false)` turns the release off, for measurement only.

**Before/after (10M rows, 10 chunks, `engine.memory` 2048 MiB, `duckdb_share` 0.4 → `memory_limit` 819 MiB, uncapped).** Each cell is peak RSS / outside / merge time in MiB and seconds; ranges are over two runs. Timings are noisy because the host is shared.

| `threads` | Before (DuckDB defaults, no release) | Release only | C.4 config (bulk flush at 2+ threads; replaced by `memory_mode`, §6.3) | Plus `alloc::release` every 250 ms (C.4 measurement; became `memory_mode: low`) |
|---|---|---|---|---|
| 1 | 1,291 / 472 / 232 | 1,199 / 380 / 230 (the C.4 config at 1 thread) | with the bulk flush: 1,205 / 386 / 299–309 | 952 / 133 / 318 (with the bulk flush) |
| 2 | 1,602 / 783 / 182 | 1,310 / 491 / 204 | 1,041–1,066 / 222–247 / 180–184 | 944 / 124 / 193 (with the bulk flush) |
| 4 | 1,742 / 923 / 122 | 1,342 / 523 / 112 | 1,036–1,038 / 217–219 / 141–152 | 1,055 / 235 / 126 (without it) |

- RSS after `COMMIT` drops from 1,160 / 1,541 / 1,452 MiB (1 / 2 / 4 threads; 828–1,159 MiB five idle seconds later) to 65–73 MiB.
- The first chunk's peak drops only a little (985 → 907–914 MiB at 2 threads), so the fix removes the accumulation across chunks, not the in-chunk cost of (3).
- **What remains** is (3): up to about 400 MiB of untracked in-flight glibc memory plus dirty pages within one chunk. At 1 thread the jemalloc part stays large, because DuckDB has no worker thread to flush it. A purge every 250 ms during calls would cut the remainder to 124–133 MiB at 1–2 threads. At 4 threads it matches the bulk flush's memory (235 MiB) without the bulk flush's slowdown (126 s against 141–152 s). The user chose to make it opt-in: `engine.memory_mode: low` (§6.3).

**`BudgetParams` (C.4, superseded by the per-mode reserves in §6.3).** The C.4 worst cases were 386 / 247 / 237 MiB at 1 / 2 / 4 threads, including the capped runs below. The largest is at 1 thread, so the reserve is mostly fixed. `fixed_overhead = 400 MiB` and `per_thread_overhead = 50 MiB` reserve 450 / 500 / 600 MiB: a 16% margin at 1 thread, and 2× or more at 2 and 4. The old defaults reserved 950 / 1,100 / 1,400 MiB. The harness default is back to `engine.memory = 1536 MiB` (2 threads → `memory_limit` 844 MiB):

| `threads` | `memory_limit` | Under `MemoryMax=1536M -p MemorySwapMax=0` | Peak RSS | Outside | Merge | RSS after `COMMIT` |
|---|---|---|---|---|---|---|
| 1 | 894 | ok | 1,175 | 281 | 350 s | 81 |
| 2 | 844 | ok | 1,065 | 221 | 231 s | 84 |
| 4 | 744 | ok | 981 | 237 | 190 s | 144 |

The cgroup's `memory.peak` reads 1,536 MiB in every run. That figure includes the page cache of the parquet files written and read, which the kernel reclaims under the cap, along with `LazyFree` pages (up to 640 MiB at 1 thread). The worst cases feeding `BudgetParams` are therefore 386 / 247 / 237 MiB.

**Upstream bugs (not filed):** DuckDB's jemalloc keeps freed memory resident after queries, and DuckLake `DELETE`/`MERGE` bookkeeping is untracked and fragments the glibc heap. If upstream fixes either, revisit `alloc::release` and `memory_mode`.

### 6.3 `engine.memory_mode` (C.5, 2026-09-30)

The user chose speed as the default, with a switch to low memory. The C.4 bulk flush slowed 4-thread merges by 25–35%, so it moved out of the default:

- **`high`** (default): `alloc::release()` between chunks and after engine calls of at least 1 s; DuckDB's allocator settings left at their defaults.
- **`low`**: the same, plus `alloc::release()` every 250 ms while an engine call or stream is in flight, and `allocator_bulk_deallocation_flush_threshold = 4MiB` at 2 or more threads.

Same harness (10M rows, 10 chunks, one transaction, `TMPDIR` on disk). Cells are RSS outside `memory_limit` / merge time, in MiB and seconds; two runs each.

**Uncapped**, `engine.memory` 2048 MiB, `duckdb_share` 0.4 (`memory_limit` 819 MiB):

| `threads` | `high` | `low` | `low` without the bulk flush |
|---|---|---|---|
| 1 | 405 / 270, 383 / 194 | 188 / 204, 200 / 209 | (not set at 1 thread) |
| 2 | 536 / 124, 468 / 130 | 159 / 154, 166 / 154 | 206 / 129 |
| 4 | 513 / 94, 520 / 101 | 204 / 117, 207 / 118 | 271 / 102 |

The bulk flush still pays for itself in `low`: without it, 2 and 4 threads keep 23–25% more outside the limit.

**Purge interval** (`low`, uncapped; `ATLATL_MEMTEST_PURGE_MS`):

| interval | 1 thread | 2 threads |
|---|---|---|
| 100 ms | 144 / 235 | 139 / 189 |
| 250 ms | 188–200 / 204–209 | 159–166 / 154 |
| 500 ms | | 158 / 169 |
| 1000 ms | 216 / 221 | 144 / 192 |

At 1 thread the periodic purge is the only purge during a chunk, and 100 ms saves about 50 MiB for about 13% more time. At 2 threads the interval hardly matters (the bulk flush does most of the work). 250 ms is the compromise.

**Capped** at the harness default: `engine.memory` 1536 MiB with no `duckdb_share`, under `systemd-run --user --scope -p MemoryMax=1536M -p MemorySwapMax=0`. All six runs pass.

| mode | `threads` | `memory_limit` | Peak RSS | Outside | Merge | RSS after `COMMIT` |
|---|---|---|---|---|---|---|
| `high` | 1 | 744 | 1,145 | 401 | 313 s | 94 |
| `high` | 2 | 694 | 1,155 | 461 | 217 s | 98 |
| `high` | 4 | 594 | 1,129 | 535 | 159 s | 141 |
| `low` | 1 | 1,099 | 1,295 | 196 | 309 s | 102 |
| `low` | 2 | 1,074 | 1,244 | 170 | 227 s | 106 |
| `low` | 4 | 1,024 | 1,234 | 210 | 180 s | 117 |

Under a fixed cap, `low`'s smaller reserve gives DuckDB about 350–430 MiB more `memory_limit`, which recovers most of its purge cost: 5% slower at 2 threads and 13% at 4, against 20% uncapped.

**`BudgetParams`**, worst case over all runs per mode, with at least 15% margin:

| mode | worst outside (1 / 2 / 4 threads) | reserve | reserved (1 / 2 / 4) | margin |
|---|---|---|---|---|
| `high` | 405 / 536 / 535 | 550 + 50 MiB/thread | 600 / 650 / 750 | 48% / 21% / 40% |
| `low` | 200 / 170 / 210 | 220 + 25 MiB/thread | 245 / 270 / 320 | 22% / 59% / 52% |

`high`'s reserve is larger than C.4's 400 + 50 because it drops the bulk flush. The `min_memory_limit` clamp (128 MiB) is unchanged.

