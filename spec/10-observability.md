# 10. Observability

## 1. Outputs

| Output | Stream | Audience |
|---|---|---|
| Logs | stderr. The format is `--log-format text` (default on a TTY) or `json` (default otherwise) | humans, log aggregation |
| Events | NDJSON to `--events <fd:N \| path \| ->` | `dagster-atlatl`, tooling |
| Run report | JSON to `--report <path>`, written atomically (tmp + rename) before exit | the source of truth for results |
| Traces and metrics | OTLP, only when `--otlp <endpoint>` / `OTEL_EXPORTER_OTLP_ENDPOINT` is set (feature-gated) | observability stack |

**No telemetry.** Nothing is sent anywhere unless configured.

## 2. Logs

- Built on `tracing`. Spans cover run → replication → stream → step → request/statement, with fields `run_id`, `replication`, `stream_id`, `member`, `attempt`.
- Levels: `error`, `warn`, `info` (replication lifecycle and summaries), `debug` (SQL and requests with redacted bodies), `trace` (full payloads, redacted).
- SQL is logged at debug, with literal values redacted if they came from secrets.
- **Progress.** While REST streams extract, a run reports their progress. On a terminal (stderr a TTY, text logs) it's one status line under the logs, redrawn every second like cargo's and cut to the terminal's width, one segment per stream: `vendor/symbols  8,412/21,976 items  792 req/min  0 retries  0 429s  eta 17m`. Log lines clear it, print and redraw it. Otherwise it's an INFO line per stream with the same fields (`items_done`, `items_total`, `requests`, `requests_per_min`, `retries`, `throttle_wait_ms`, `eta_s`), when an extract starts and then every `--progress-interval` (`ATLATL_PROGRESS_INTERVAL`, default `30s`). `stream.progress` events follow the same cadence on a terminal or not, and an interval of `0` turns all of it off, status line included. The total is absent for a plain endpoint (pages aren't known ahead), and the ETA with it. Counts update per request, items per finished item.

## 3. Event stream

There is one JSON object per line, and every event carries `{"v": 1, "ts": "...", "run_id": "...", "type": "..."}`.

| `type` | Payload |
|---|---|
| `run.started` | `env`, `selection`, `plan_hash`, `atlatl_version`, `duckdb_version` |
| `replication.started` | `replication`, `op`, `streams` |
| `stream.progress` | `replication`, `stream_id`, `member`, `phase` (extract/stage/validate/commit), `rows_so_far`, `bytes_so_far`, `chunk` (at most one per second per stream). A REST stream's extract emits one when it starts and then every `--progress-interval` (§2), `chunk` being items done, with `http`: `items_done`, `items_total?`, `requests`, `requests_per_min`, `retries`, `throttle_wait_ms`, `status_counts`, `eta_s?` |
| `retrying` | `replication`, `stream_id?`, `member?`, `level` (request/query/object/commit/replication), `attempt`, `error_class`, `message`, `wait_ms` |
| `hop.committed` | `replication`, `hop`, `target`, `lake_snapshot?` (for multi-hop replications) |
| `replication.completed` | the replication's full report entry (§4): `replication`, `status`, `hops`, `error`, and per-stream `asset_key`, rows, state before and after, rejects |
| `throttle` | `connection`, `wait_ms`, `reason` |
| `schema.drift` | `replication`, `stream_id`, `member?`, `mode`, `new_fields`, `missing_fields`, `violations` (per field: kind, count, samples) |
| `warning` | `replication?`, `stream_id?`, `code`, `message` |
| `run.completed` | `status`, `exit_code`, `counts {succeeded, failed, skipped, killed, not_started}` (`up_to_date` counts as succeeded), `report_path` |

Optional (`?`) fields are omitted when absent. The event schema is versioned (`atlatl json-schema events`), and consumers must ignore unknown fields.

## 4. Run report

```json
{
  "v": 1,
  "run_id": "01J…",
  "external_run_id": "dagster-run-uuid",
  "atlatl_version": "0.1.0",
  "env": "prod",
  "plan_hash": "…",
  "started_at": "…", "finished_at": "…",
  "status": "partial_failure",               // succeeded | partial_failure | failed | killed | config_error | internal_error
  "exit_code": 1,
  "killed_by": null,                         // sigint | sigterm | timeout
  "error": null,                             // run-level, for config_error / internal_error
  "warnings": [],                            // run-level (plan warnings)
  "dry_run": false,                          // `run --dry-run` (03 §11): nothing committed; members carry `diff`
  "engine": {"duckdb_version": "1.x", "memory_limit": "…", "threads": 4, "memory_mode": "high", "peak_rss_bytes": 0, "peak_duckdb_bytes": 0},
  "replications": [
    {
      "replication": "warehouse_est_consensus",
      "op": "warehouse_est_consensus",
      "status": "succeeded",                  // succeeded | failed | skipped | killed | not_started | up_to_date (nothing to commit)
      "started_at": "…",
      "duration_ms": 0,
      "attempts": 1,                          // replication-level attempts (replication_retries)
      "hops": [{"target": "lake", "status": "committed", "lake_snapshot": 12345}],   // multi-hop only; committed | failed | not_started, plus `error` on a failed hop
      // "instance": {"of": "signed_accounts", "key": "42"} on a for_each instance, else absent
      "error": null,                          // {class, code, message, retryable, samples[], backtrace} on failure; code "panic" for a caught panic
      "streams": [
        {
          "stream_id": "warehouse/est_v2/est_core_consf",
          "asset_key": "br/warehouse/est_v2/est_core_consf",
          "status": "succeeded",              // succeeded | failed | feed_only | up_to_date (wrote nothing) | rolled_back (nothing committed)
          "duration_ms": 0,
          "members": [
            {
              "member": "est_v2.est_core_cons_af",
              "object": "catalog.br.est_core_cons_af",
              "duration_ms": 0,
              "retries": [{"level": "request", "attempt": 1, "class": "transient", "message": "…", "wait_ms": 2000}],
              "rows": {"extracted": 0, "staged": 0, "inserted": 0, "updated": 0, "deleted": 0, "unchanged": 0, "rejected": 0},
              "bytes": {"read": 0},
              "state": {"before": null, "after": null},     // update_key / sync / CDC snapshot
              "lake_snapshot": 12345,           // DuckLake snapshot created, null if nothing changed
              "chunks": 8,
              "cdc": null,                      // 08 §11: {window, touched_keys, upserts_sent, rows_*, deletes_ignored, snapshots_behind, seconds_behind, resynced}
              "http": null,                     // REST: {requests, retries, throttle_wait_ms, status_counts {"200": n}, items_deferred? (07 §7 max_items), items_held? (07 §7 except_skipped), skipped_items [{key, reason}]}
              "schema": null,                   // {status: ok|violated, mode, severity: warn|error|null, records, new_fields, missing_fields, violations [{field, kind: type|null|domain, count, samples [{ordinal, value}]}]} (04 §10)
              "rejects": {"new": 0, "resolved": 0, "expired": 0, "open": 0, "samples": []},
              "warnings": [],
              "diff": null                      // --dry-run only (03 §11): {mode, target_rows, inserts, updates, deletes, unchanged, counts_only, ddl [], samples [{op: insert|update|delete, key, changed?, staged?, target?}]}
            }
          ]
        }
      ]
    }
  ]
}
```

An ingest replication is `up_to_date` when its commit would have written nothing (04 §1): no row changed, no DDL ran, no state value moved and no reject changed (a reject failing again with the same error isn't a change, [03 §7](03-execution.md#7-rejects)); nothing is committed then. `rejects.open` is still reported. In a dry run, `up_to_date` and `succeeded` mean the run would change nothing or something; `rows.inserted` / `updated` / `deleted` stay 0 there (nothing was written) and the would-be counts are in `diff`.

The report is the contract with `dagster-atlatl`, and it's covered by a JSON Schema (`atlatl json-schema report`). It's written on every exit path except SIGKILL. On SIGTERM/SIGINT it is best-effort ([03 §9](03-execution.md#9-kill-behaviour)).

`status` and `exit_code` are derived from the replications ([03 §10](03-execution.md#10-exit-codes)): all succeeded, up to date or skipped is `succeeded` (0); otherwise `partial_failure` if any succeeded (or a failed multi-hop replication committed a hop), else `failed` (1). A replication error with code `panic` makes it 3. `killed_by: sigint|sigterm` gives 130/143; a `--timeout` kill exits 1.

## 5. Redaction

- Every resolved secret value, including OAuth tokens and per-item secrets, is registered with a process-wide redactor.
- The redactor replaces those values with `****` in logs, events, reports, error messages and cassettes.
- URL query parameters named `api_key`, `apikey`, `token`, `c`, `key`, `secret`, `password`, `signature` (a configurable list) are always redacted. The same applies to the `Authorization`, `Cookie` and `X-Api-Key` headers.
- The DuckLake catalog DSN password is redacted wherever an error echoes the connection string.

## 6. Metrics (OTLP, optional)

Counters and histograms:
- `atlatl.rows{stream,op}`;
- `atlatl.replication.duration`;
- `atlatl.http.requests{connection,status}`;
- `atlatl.http.throttle_wait`;
- `atlatl.retries{class}`;
- `atlatl.cdc.snapshots_behind{stream}`;
- `atlatl.engine.memory`.

Traces mirror the log spans.
