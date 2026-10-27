# Spike S11: D1 multi-statement atomicity

Date: 2026-09-29. No proof code: nothing here was run. The source is the Cloudflare D1 docs ([Worker binding API](https://developers.cloudflare.com/d1/worker-api/d1-database/), [REST API](https://developers.cloudflare.com/d1/rest-api/)).

## Verdict

**Blocked (REST path).**
- A real answer needs a D1 database in a Cloudflare account. Local miniflare/wrangler D1 is plain SQLite and says nothing about the hosted REST endpoint.
- Nothing has been created in any account.
- This gates only the D1 target (M8.5). The 08 §6.3 design is idempotent per window, so it stays correct whatever the answer; only batch sizing depends on it.

## Documented facts

| Path | Atomic? | Source |
|---|---|---|
| Worker binding `DB.batch([stmts])` | **Yes.** "Batched statements are SQL transactions. If a statement in the sequence fails, … it aborts or rolls back the entire sequence." Statements run sequentially and non-concurrently. | Worker API, `batch()` |
| Worker binding `DB.exec(sql)` | Not documented as atomic. It runs "one or more queries directly" and returns a `count`. | Worker API, `exec()` |
| REST `POST …/d1/database/{id}/query` (and `/raw`) with several `;`-separated statements, or a batch body | **Undocumented.** | REST API |
| Interactive `BEGIN … COMMIT` | Not available; D1 runs in auto-commit. | Worker API, `batch()` |

## Runbook (needs a scratch account and a token scoped to one throwaway database)

1. Create a scratch D1 database and `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`.
2. `POST /query` with `sql: "INSERT INTO t VALUES (1,'a'); INSERT INTO t VALUES (1,'dup'); INSERT INTO t VALUES (2,'b');"`. Then `SELECT count(*)`: 0 means atomic, 1 means a statement-by-statement prefix.
3. Repeat step 2 with the batch body form (an array of `{sql, params}`) if the API accepts it, and with `/raw`.
4. Measure the largest body and statement count accepted before `SQLITE_TOOBIG` or a 413, for batch sizing (06 §7).
5. Delete the database.

## Consequences for design

- **If REST batches are atomic:** one request per window chunk can carry the upserts, the deletes and the `pending → snapshot_id` cursor flip. The pending marker then only matters across requests.
- **If not:** keep 08 §6.3 as written. Every batch is replayable, and the cursor's final statement is its own request.
- **Either way:** a thin Worker proxy using `DB.batch()` is the documented atomic path, if exactly-once per batch is ever needed without idempotency.
