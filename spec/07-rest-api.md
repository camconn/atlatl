# 07. REST API engine

Vendors are defined **only** in YAML API specs. A capability that YAML can't express (a new auth scheme, paginator or decoder) is added to the Rust core and exposed as YAML config, never as per-vendor code. Footguns the format avoids by design:

- defaults that replace rather than merge;
- a hidden 2 req/s default rate;
- setup calls that aren't spaced against the main requests;
- throwing on non-JSON bodies;
- brace templating inside JMESPath;
- OAuth tokens never refreshed;
- float64 queues;
- infinite pagination loops.

## 1. Example

The throttled vendor in atlatl form. It shows setup, a shared throttle, range iteration, text-matching rules and sync state.

```yaml
# apis/throttled.yaml
name: throttled
base_url: https://api.throttled.example
defaults:
  params: {key: "{{ secrets.api_key }}", fmt: json}  # merged into every request, setup included
rate_limit:
  rps: 0.2
  min_interval: 250ms          # the vendor 409s any request within ~100ms of the previous one
  scope: connection            # shared by all endpoints, setup and retries
throttle_patterns: ["slow down"]
rules:
  - when: 'response.status == 409 or "slow down" in response.text'
    action: retry
    class: throttled
    backoff: {type: exponential_jitter, base: 5s}
    max_attempts: 5

endpoints:
  releases:
    for_each:
      over: 'range(coalesce(sync.floor, date("2015-01-01")), date_add(now(), 12, "month"), "3mo")'
      as: window_start
    request:
      path: /releases
      params:
        start: '{{ window_start | date_format("%Y-%m-%d") }}'
        end: '{{ window_start | date_add(3, "month") | date_add(-1, "day") | date_format("%Y-%m-%d") }}'
    records: {path: "[]", primary_key: [Code, Tag, Date]}
    sync:
      floor: {value: 'date_trunc(date_add(now(), -6, "month"), "quarter")', aggregate: set}   # next run re-walks from ~2 quarters back

  symbols:
    setup:
      - name: regions
        request: {path: /regions}
        records: {path: "[].Region"}
        collect: regions               # state.regions = list of all extracted values
    for_each: {over: state.regions, as: region}
    request: {path: "/symbols/region/{{ region | urlencode }}"}
    records: {path: "[]", primary_key: [Code]}
```

## 2. Spec structure

| Key | Meaning |
|---|---|
| `name` | spec name |
| `base_url` | joined with `request.path`; `request.url` overrides it with an absolute URL |
| `auth` | §3 |
| `defaults` | `headers`, `params`, `body`, `timeout`, `rules`, `records`. **Deep-merged** into every endpoint, setup and auth request. Maps merge per key, an endpoint value of `null` removes a key, and `defaults.rules` are evaluated right after each endpoint's (and setup step's) own rules |
| `rate_limit`, `throttle_patterns`, `decode_failures_as_throttle` (`3/60s` or `{count, window}`) | §8, §10 |
| `max_body` | response size cap (default `256MB`); `decode.max_body` overrides it per endpoint |
| `rules` | spec-wide rules, evaluated after endpoint rules and before the built-in defaults (§10) |
| `state` | initial values for the `state` namespace |
| `endpoints` | map from name to endpoint |

Endpoint keys: `description`, `setup`, `for_each`, `request`, `paginate`, `decode`, `records`, `fields`, `raw`, `emit`, `rules`, `sync`, `history`, `depends_on`, `auth`, `on_decode_error`, `max_skipped`, `record_skips`. Unknown keys are errors, located at file:line.

## 3. Authentication

| `auth.type` | Fields | Notes |
|---|---|---|
| `none` | | |
| `headers` | `headers: {…}` | Static header(s); `paged_queue`'s `api_key` |
| `query` | `params: {…}` | Key in the query string; `queue_chain`, `record_queue`, `windowed`, `throttled`. Values are redacted everywhere ([10 §5](10-observability.md#5-redaction)) |
| `bearer` | `token` | |
| `basic` | `username`, `password` | Key as the username (a market-feed vendor) |
| `oauth2` | `flow: client_credentials \| password \| refresh_token`, `token_url`, `client_id`, `client_secret`, `scopes`, `client_auth: basic \| body` (default `basic`), `username`/`password` (password flow), `refresh_token` (refresh flow), `extra: {…}` | Tokens are **refreshed proactively** at `expires_in − 60s` and re-acquired once on 401 (a refresh token from any grant is used first). `reauth_when:` is an expression for vendors that signal expiry in a 200 body (one reference-data vendor). Tokens are cached in memory only |
| `hmac` (**v1.x**) | `algorithm: sha256 \| sha512`, `key`, `params`/`headers` (added before signing; `timestamp` is epoch seconds), `canonical` (template over `method`, `path`, `query`, `url`, `body`, `body_json`, `timestamp`), `encoding: base64 \| hex`, `placement: {header: X} \| {param: x}` | `signed` request signing: `canonical: '{{ {"content": body_json, "path": path, "query": query} \| tojson(sort_keys=true) }}'` |
| `sigv4` (later) | `service`, `region`, credentials chain | AWS APIs, OpenSearch sink |

- `auth` can be overridden per endpoint, and `auth: none` disables it for that endpoint.
- **Per-iteration credentials**: `for_each` items can carry fields marked `secret: true`, such as `signed`'s per-user `userSecret`. They're available as `secrets.item.*`, registered with the redactor (so masked in logs, errors and cassettes), and never landed. OAuth2 fields may use them (`client_id: "{{ secrets.item.client_id }}"`): tokens are cached per rendered credential set (token URL, client id and secret, username/password, refresh token, `extra`), so each item gets and reuses its own token on a shared connection.

## 4. Requests

```yaml
request:
  method: GET                 # GET|POST|PUT|PATCH|DELETE
  path: /entities/{{ entity_id | urlencode }}/reports
  params: {page_size: 1000, next_page: "{{ page.cursor }}"}
  headers: {User-Agent: "Mozilla/5.0 …", Accept: application/json}
  body: {json: {…}} | {form: {…}} | {text: "…"}
  timeout: 60s
```

- **Omission.** A parameter, header or body field that renders to `null` **or `""`** is omitted. `keep_empty: true` keeps `""`. That removes the need for the `oauth_cursor` trick of splicing the cursor into the URL.
- **Encoding.** Every interpolated value in `path` is URL-encoded unless it is piped through `| safe`. Values in `params` are always encoded.
- **Typing.** A template that is the whole value keeps its type (a number, bool, list or map) in `params` and `body`.

## 5. Pagination

Pagination uses typed strategies, plus an expression fallback:

```yaml
paginate: {type: cursor, param: next_page, from: "next_page"}           # value from response body (JMESPath); stops when null/""
paginate: {type: cursor, param: cursor_next, from: "data.cursor.next"}  # oauth_cursor
paginate: {type: page, param: pageNumber, start: 1, stop_when: 'response.json.pagination.pageNumber >= response.json.pagination.numPages'}  # paged_queue
paginate: {type: offset, param: offset, limit_param: limit, limit: 1000, total_from: "pagination.total"}  # signed activities
paginate: {type: next_url, from: "next_url"}                            # a next-URL vendor
paginate: {type: link_header}                                           # RFC 8288
paginate: {type: body_cursor, path: "$.from", step: 100, cap: 10000, on_cap: split_window}   # v1.x: capped search APIs
paginate: {type: expression, next: {state_key: "<expr>"}, stop_when: "<expr>"}
```

Every strategy has three guards:
- `max_pages` (default 10 000; exceeding it is an error);
- **no-progress detection**: if the next request is identical to the previous one, it's a `config` error, so an infinite loop is impossible;
- `stop_on_empty`, which stops when the records path yields zero records for a page (before `filter` and dedup). The default is `true` for `page`, `offset` and `expression`, and `false` for `cursor`, `next_url` and `link_header`, which end on their own signal (a page whose rows a JMESPath filter drops entirely must not end an `oauth_cursor` cursor walk).

All strategies also take `stop_when`, evaluated after each page. Request templates see `page.{number, cursor, offset, limit, page, url}` plus the `expression` strategy's keys (`null` before the first value). `page` defaults to `start: 1, step: 1`; `offset` stops at `total_from` or on a short page; `next_url` and `link_header` targets (`rel: next` by default) are resolved against the current URL and replace it, request params included, with auth re-applied.

## 6. Records

```yaml
decode: {format: json}                   # json | jsonl | csv | xml (v1.x) | text | bytes; see §10
                                         # csv also takes null_values (default [""])
records:
  path: "entities[?reg_no!=null].{id: id, reg_no: reg_no, code: code, name: name}"   # JMESPath over the decoded body
  filter: 'record.fiscal_period in ["Q1","Q2","Q3","Q4"]'                         # optional expression
  primary_key: [id]
  dedup: first                           # first | last | none (by PK, across the whole run)
fields:                                  # enrichment: column -> expression
  entity_id: response.json.entity.id
  reg_no: response.json.entity.reg_no
  uid: item.uid                          # the current for_each item
raw: false                               # true = land the whole response as one row (see below)
```

- **JMESPath is never template-rendered.** It runs on the decoded body. Values from `state`, the `item` or `response` enter only through `fields`/`filter` expressions.
- The dialect is standard JMESPath (jmespath.rs), including `merge()` and `to_string()`. Extra functions: `lower`, `upper`.
- JMESPath objects are key-sorted. Plain paths (`[]`, `data.items`, `entities[]`) are evaluated directly and keep the vendor's key order. `null` array elements yield no record.
- A path that yields a single object produces one record. Scalars produce a `data` error unless `records.scalar_as: value` (a `{value: x}` wrapper).
- `fields` run after extraction and before `filter` and dedup, so a PK can include enriched columns (`queue_chain`'s `report_id`). A field that is a plain path (`response.json.entity.id`, `item.uid`, `record`) is copied exactly; a computed expression goes through minijinja's i64/i128/f64.
- **Exact numbers:** bodies are parsed into a JSON value that keeps each number's source text (`1.10`, 30-digit ids), all the way into staging. `serde_json/arbitrary_precision` isn't used: features unify across the workspace, and it breaks serde's internally tagged enums elsewhere. JMESPath numbers are f64 internally, but values it passes through (field access, projections, filters, multiselect, `merge`, `to_string`) keep their exact text; computed values (`sum`, arithmetic) don't.
- **Typing:** columns are stored as text by default. Expected types and formats are declared in the stream's source schema ([04 §10](04-ingest.md#10-source-schemas)), which reports or fails on upstream drift.
- **`raw: true`** lands `{fetched_at, request_key, status, response_json}` as one row per response. It's the schemaless "decode in dbt" mode that `signed` captures use.

## 7. Iteration, queues and endpoint dependencies

```yaml
for_each:
  over: [NORTH, SOUTH]                                   # literal list
  over: 'range(date("1996-01-01"), date_add(now(), 1, "month"), "1mo")'
  over: state.regions                                    # from setup
  over: {queue: entities}                                # from another endpoint's emit
  over: {queue: symbol_ids, unique: true, except_landed: [usid]}   # only ids not yet in the target
  over: {query: "SELECT isin FROM catalog.au.instrument", connection: lake, except_landed: [isin]}   # v1.x
  as: entity_id                                          # variable name; items can be maps (item.symbol)
  concurrency: 4                                         # items in flight (default 1); see §8
  max_items: 12000                                       # per run; or an expression such as `vars.batch`
  except_skipped: {ttl: 7d}                              # items a rule skipped sit out this long (needs record_skips)
  batch: {size: 100, as: items}                          # v1.x: request multiplexing (batch lookup APIs, `record_queue` batch)
emit:
  entities: record.id                                    # push to queue `entities` (one item per record)
  instruments: record                                       # whole record
```

- **Queues** are materialised as DuckDB temp tables holding exact JSON values, in insertion order. `unique: true` deduplicates them, and `except_landed` (as for `query` iteration, below) then drops the items already in the consuming stream's target. That's this run's producer output against earlier runs' rows, so a new key is fetched in the run that first lists it, which `query` over the producer's target can't do (it sees only committed rows).
  - A consumer starts after its producer has finished, in v1. Streaming consumption (`consume: live`) is later.
  - Values keep full precision, with no float64 round trip.
  - An `emit` expression sees `record`, `item` (and the `as` name), `state`, `sync` and the plan namespace, not `response`. A `null` value emits nothing; a map with a null field is not null, so guard it (`'{"id": record.id} if record.id is not none else none'`). Emits follow run-wide dedup: a duplicate record emits nothing, and under `dedup: last` records emit when the endpoint finishes.
  - A queue belongs to one replication in one run (`q_<run_id8>_<hash(replication)>_<queue>`). Consuming a queue whose producer hasn't been extracted yet is a `config` error, never an empty iteration.
- **`max_items`** caps the items one run fetches, counted after `unique` and `except_landed`, in iteration order (before `batch` groups them). The stream commits normally, the member reports `http.items_deferred` with an `items_deferred` warning (and `warning` event), and the replication still succeeds. With `except_landed`, each run takes the next batch, so a long backfill commits in pieces: a crash costs one batch, not the whole backfill. Without it every run would take the same first items, and `atlatl validate` warns. An integer, or an expression evaluated like `over`: `max_items: vars.batch` with stream `vars: {batch: 12000}` lets `--var batch=500` change it for one run. Items a later run doesn't reach keep their open `skipped` rejects; they aren't replayed meanwhile.
- **`except_skipped: {ttl}`** keeps keys a rule skipped from costing a request every run, and from starving `max_items`: an item a `skip` rule (§10) dropped never lands, so `except_landed` alone keeps it first in line. It needs `record_skips: true` and can't be combined with `batch`. An item whose open `skipped` reject was last seen within `ttl` is left out (reported as `http.items_held`), and so is its replay. Once the TTL has passed it's retried, after the run's new items, so it's the first to be cut by `max_items`. A retry that's skipped again is committed even though its error is unchanged, which restarts the TTL. `atlatl validate` warns about `max_items` on an endpoint with a `skip` rule and no `except_skipped`.
- **Items.** `for_each.over` is evaluated once, after `setup`. Each item is one endpoint run (its own pagination); items run in order, `concurrency` at a time, and their records are staged in item order. Dedup by primary key spans all items. `batch: {size, as}` groups consecutive items into lists, bound as the `as` name (and `item`); each list is one run.
- **Dependencies** are inferred from `emit` and `over.queue`. `depends_on` adds explicit ones.
  - Selecting a consumer without its producer pulls the producer in as **feed-only** (it fetches and emits but doesn't write), unless `--no-deps`, which fails at plan time ([03 §1](03-execution.md#1-selection-and-planning)).
  - `record_queue` `ratings`, `queue_chain` `line_items` and `paged_queue` `rounds` can therefore be materialised alone.
- **Iteration isolation:** a failure in one item is handled by the rules (§10). Only a `fail` action or an exhausted budget fails the endpoint, and with it the replication ([03 §2](03-execution.md#2-atomicity)).
- **`query` iteration** (**v1.x**, the highest-value primitive for future sources) runs SQL against a lake or DB connection. Worklist logic, such as "keys not yet landed", can then live in SQL or dbt.
  - `connection` is a lake (the query runs in the replication's engine session, so any attached catalog can be joined) a Postgres connection (queried directly; a live plan resolves it along with the replication's own connections), or a `duckdb` file (opened read-only in its own database, or queried inside the file when the run writes it; [06 §10](06-connectors.md#10-duckdb-files)). The SQL is never templated.
  - A row with one column yields that value as the item; a row with more yields a map (`item.isin`). Values are exact.
  - `except_landed: [col…]` drops items whose values for those columns (compared as text) are already in the stream's target table. A missing target (the first run) excludes nothing, so the anti-join doesn't need the table to exist, and it works across connections. A scalar item is compared to a single column. The stream should be `incremental` (or `snapshot`): under `full-refresh` each run would replace the target with only that run's new keys.

## 8. Rate limiting

```yaml
rate_limit:
  rps: 1                      # sustained rate (GCRA via `governor`)
  burst: 1
  min_interval: 0ms           # hard minimum spacing between request *starts*
  max_in_flight: 1
  scope: connection           # connection (default) | endpoint
  budget: {requests: 20000, per: run}     # optional hard cap; exceeding is a `config`-class failure
  respect_headers: true       # Retry-After, RateLimit-*, X-RateLimit-Remaining/Reset (delta or epoch)
  adaptive: {on_throttle: halve, recover_after: 60s}
```

- **One limiter per connection by default.** Setup requests, auth requests, retries, all endpoints and all iterations draw from it. The `throttled` vendor's back-to-back setup/first-request 409 therefore can't happen.
- **No hidden default rate.** An absent `rate_limit` means `max_in_flight: 1` with no rps cap, and `atlatl validate` warns about it.
- **Concurrency is the endpoint's, not the limiter's.** `max_in_flight` and `rps` only cap requests; a `for_each` endpoint still runs `for_each.concurrency` items at a time (default 1), so with one item in flight its rate is bounded by latency (about 1/latency per second), whatever the limit allows. To reach `rps` set `concurrency` to about `rps × latency` (and `max_in_flight` at least that). `atlatl validate` warns when a `for_each` endpoint leaves `concurrency` unset while `rate_limit` allows more than one request at a time or more than 1 rps.
- **Throttle feedback.** A `throttled`-class response pauses the whole limiter until the server's reset hint (or the backoff) expires. `adaptive` halves the effective rate on each throttle (down to 1/64; it scales `min_interval` when there's no `rps`) and doubles it back after each `recover_after` without one. `X-RateLimit-Remaining: 0` triggers a proactive wait.
- Waits are visible in events and reported per endpoint (`throttle_wait_ms`).

## 9. Expression functions

Expressions and templates use minijinja ([02 §5](02-configuration.md#5-templating)). Comparisons short-circuit and work on strings, dates and numbers.

| Function / filter | Semantics |
|---|---|
| `now()` | current time, **always UTC** |
| `date("2015-01-01")`, `date_parse(s, fmt)` | parse (strftime or `"auto"` ISO-8601) |
| `date_format(t, fmt)` | strftime |
| `date_add(t, n, unit)` | unit ∈ second, minute, hour, day, week, month, quarter, year (plural and any case accepted). Month and year arithmetic **clamps** to the month end (Jan 31 + 1 month = Feb 28/29; Feb 29 + 1 year = Feb 28). |
| `date_trunc(t, unit)` | |
| `range(start, end[, step])` | ints or dates; strings are parsed as dates. **End-inclusive** (`t <= end` as instants); `start > end` gives `[]`; the default step is `1d` (`1` for ints; a negative int step counts down). Date steps are `N` ≥ 1 followed by `h`/`hour`, `min`/`minute`, `d`/`day`, `w`/`week`, `mo`/`month`/`M`, `q`/`quarter` or `y`/`year`, case-insensitive, with an optional trailing `s`. A bare `m` is a config error, since it is ambiguous between minute and month. Month, quarter and year steps are calendar steps computed **from `start`** (`start + k·step`, clamped to the month end), so they never drift. |
| `coalesce(a, b, …)` | first non-null (`""` counts as non-null); `first_nonempty(…)` for the non-empty variant |
| `x in y` | substring or list membership (replaces `contains`) |
| `urlencode`, `tojson`, `fromjson`, `lower`, `upper`, `default` | |
| `hmac(alg, key, msg)`, `sha256`, `base64`, `hex` | for signers (**v1.x**). `hmac` and `sha256` return bytes; `base64` and `hex` encode bytes or strings |

Other semantics: missing JSON keys read as `none` and attribute access on `none` is `none`, so `coalesce(response.json.data.cursor.next, "")` works on sparse bodies; unknown roots and unknown `secrets.*`, `sync.*` and `state.*` keys are config errors before the first request. `none` interpolates as `""`, maps and lists as compact JSON, dates as RFC 3339. `default` also replaces `none`. `tojson` takes `sort_keys` and `indent`, doesn't HTML-escape, and keeps exact numbers.

`now()` is UTC. There are no second steps.

## 10. Response rules and decoding

Every response goes through this replication:

```
receive ─► capture {status, headers, bytes, text, elapsed} ─► rules ─► decode ─► records ─► fields ─► dedup ─► stage
                                                       ▲                 │
                                                       └── decode_error ─┘  (decoding is lazy, so a rule reading response.json or decode_error sees the result)
```

- **Capture first.** The body is read with a size cap (`max_body: 256MB`) and a timeout. A transport error mid-body (reset, truncation) is `transient`. Transport errors (no response) skip the rules and are retried like built-in 2. A body over the cap is a `data` error (`body_too_large`).
- **Rules see the raw response.** `response.status`, `response.headers` (keys lowercased), `response.text`, `response.json` (lazily decoded; `null` if decoding failed) and `response.decode_error`.
  - A missing or wrong `Content-Type` never causes a throw.
  - `decode.content_type: sniff | trust | ignore` (default `sniff`: try the declared format, whatever the header says).
- **Decode failures are errors that can be retried and rate-limited:**
  - If decoding fails and no rule matched the decode error, the response is classified `decode` ([03 §4](03-execution.md#5-error-taxonomy)). That means retry with backoff under the endpoint's attempt budget.
  - If the status is 409, 429 or 503, or the text matches `throttle_patterns`, it is classified `throttled` instead, and the shared limiter backs off.
  - More than `decode_failures_as_throttle` (default 3 in 60 s) on one connection is treated as throttling. This covers HTML "please slow down" pages with status 200.
  - The endpoint's `on_decode_error` picks the granularity ([03 §6.1](03-execution.md#61-decode-failure-granularity)):
    - `request` (the default) re-issues the whole request;
    - `row` keeps the records that decoded, refetches the request for the ones that didn't, and records rows that still fail as rejects. Rejects are retried on later runs ([03 §7](03-execution.md#7-rejects)). `reject_threshold` and `reject_ttl` can be set here too; the stream's `on_decode_error` wins.
      - The request is refetched under the attempt budget first (including after the failures were re-classed as throttling). The last response is then decoded row by row: JSONL lines and CSV records fail individually (the row index is the line or record number), and a JSON body that still doesn't parse is one reject for the whole response.
      - Such a page ends its item's pagination, because its cursor can't be read.
      - The stored request descriptor (method, URL, headers, body, item, row) is replayable and secret-free: secret values are sealed as `{{secrets.<name>}}` markers and restored on replay. The reject key is a hash of the descriptor and row.
- **Rules** (first match wins; order is endpoint → spec → built-in defaults):

  ```yaml
  rules:
    - {when: "response.status == 404", action: skip}                            # this item has no data
    - {when: "response.status == 429 or response.status >= 500", action: retry, max_attempts: 5, backoff: {type: exponential, base: 2s}}
    - {when: '"token expired" in response.text', action: reauth}
  ```

  | Action | Effect |
  |---|---|
  | `retry` | Re-send after a backoff (per-rule `backoff`, `max_attempts`, `class`; defaults from the connection's `retry`). Auth is refreshed before re-sending if the class is `auth`. A `throttled` retry pauses the shared limiter for the server's hint or the backoff instead of sleeping. Without `class`, 409/429/503 and `throttle_patterns` matches are `throttled`, and other statuses take their HTTP class (`transient` for anything that would be `config`) |
  | `skip` | End the current iteration item (and its pagination) with zero further records. Counted as `skipped` in the report, with the reason |
  | `ignore` | Process the body normally, despite the status |
  | `reauth` | Refresh credentials and retry once |
  | `stop` | Finish the endpoint cleanly after in-flight requests complete |
  | `fail` | Fail the endpoint with `message` |

- **Built-in defaults** are appended after user rules, and `atlatl api explain` prints them:
  1. 429 → retry, `throttled`, honouring headers, 5 attempts.
  2. 408, 5xx, transport → retry, `transient`, 5 attempts.
  3. 401 → `reauth` once, then fail with `auth`.
  4. Decode failure on a status < 400, a 409/429/503, or a body matching `throttle_patterns` → retry, `decode` (or `throttled` per the rules above). A 404 HTML page therefore fails under 5 rather than being retried. The same rule applies when an `ignore`d response doesn't decode.
  5. Other ≥ 400 → fail.
- **Skip guard:** `max_skipped: {ratio: 0.2, min_items: 50}`. If too many items skip, the endpoint fails, so a revoked key can't quietly produce an empty table.
- **Skipped items** (from a `skip` rule) are reported with their request key and reason. With `record_skips: true` they're also written to `_atlatl.rejects` (class `skipped`) and retried on later runs, like decode rejects ([03 §7](03-execution.md#7-rejects)). A skip reject is keyed by its item: if the item is iterated again it resolves (or stays open) with that run's outcome; otherwise the item is replayed on its own.
- **CSV** (`format: csv`): the first record is the header, and each later record becomes an object of strings keyed by it (a UTF-8 BOM is dropped).
  - `decode.null_values` lists the field values that decode as `null`; it defaults to `[""]`, so an empty field is `null`, and `null_values: []` keeps every field a string. Matching is exact, per field, as in the files connector ([06 §5](06-connectors.md#5-files)).
  - Quoted and unquoted fields are not told apart (the CSV reader unquotes before atlatl sees the value): `""` is an empty field, and `"NA"` matches `NA`. The same holds for files, where DuckDB's `nullstr` also matches quoted values.
  - The default differs from files, which default to `[]`. In an API's CSV an empty field means "no value" (a missing observation), just as an absent key does in JSON, and REST records are compared and typed as JSON values: with empty strings, a nullable `decimal` in the source schema would fail, and every endpoint needed a `fields:` rewrite to `none`. Files are often adopted as-is from tables whose loader kept empty strings, so they null nothing unless asked.
  - `atlatl api explain` prints the effective `null_values` for CSV endpoints. In `on_decode_error: row` mode the same rule applies per record.
- **Body repair** (later): `decode.repair: [{regex, replace}]` for vendors that return malformed JSON (one reference vendor).

## 11. Sync state

```yaml
sync:
  floor: {value: 'date_trunc(date_add(now(), -6, "month"), "quarter")', aggregate: set}   # evaluated once at the end of the run
  max_updated: {value: record.updated_at, aggregate: max}                                   # folded over every record of the run
  since: {value: response.json.meta.last_updated, aggregate: last}                          # folded over responses
history: replayable            # replayable (default) | snapshot_only
```

`aggregate` takes one of these values:
- `set`: evaluated once at the end of the run, with no record context;
- `max` / `min` / `last` / `first`: folded over every record (for a `record.*` value) or every response (for a `response.*` value).

A fold over zero inputs leaves the stored value unchanged. `null` values aren't inputs. `max`/`min` compare numbers numerically and everything else by its text (ISO dates sort correctly).

A value whose expression reads `response` but not `record` is folded over responses; any other non-`set` value is folded over records (after filter and run-wide dedup). `set` sees `sync` (the stored values), `state` and the plan namespace.

- `sync` values are persisted per stream in `_atlatl.stream_state`, **in the same commit** as the data ([05](05-state.md)). Templates read them as `sync.<name>` on the next run, and they are `null` on the first run.
- This replaces the daily full re-walks:
  - `throttled` would walk from about 2 quarters back instead of from 2015;
  - `monthly` from the last month or two instead of from 1996;
  - `windowed` from a rolling window.

  Each spec keeps a `coalesce(sync.x, <floor>)` so the first run backfills.
- `--range a..b` overrides iteration bounds for a backfill ([04 §4.6](04-ingest.md#46-backfill-and-range-overrides)).
  - It replaces the start and end of every `range()` call in `for_each.over` (either side may be empty to keep the spec's bound; the step is kept). It applies to selected streams, not feed-only producers, and the report warns (`range_unused`) for an endpoint whose `over` doesn't call `range()`.
  - Under `--range`, `max`/`min` folds are combined with the stored value, so a backfill never moves them back, and `first`/`last` folds are not stored. `set` values are evaluated as usual.
- `history: snapshot_only` rejects `--range` (a `config` error), for live snapshots such as `signed` positions or a live venue-status feed.

## 12. Setup

`setup:` is a list of requests that run once before iteration. They share the connection's limiter and rules. Each can paginate, and it can write to `state` through `collect` (list), `set` (last value) or `max`/`min`.

## 13. Tooling

- `atlatl api test <connection> <endpoint> [--pages 2] [--items 2] [--var k=v] [--item JSON] [--item-secret k=v] [--json]` executes against the live API **without writing**. It prints each request and response (redacted, sensitive headers dropped, bodies truncated), the extracted records and the would-be field types (as `schema infer` would draft them). `--pages` limits plain endpoints; endpoints with `for_each` or `setup` run through the iteration driver and `--items` limits the items (each run to completion). `--item` replaces `for_each.over`; `--item-secret` binds `secrets.item.*`. `<connection>` may also be an API spec (path, file stem or `name`) whose templates use no `secrets.*`.
- **Cassettes.** `--record <name|path>` records the session to `apis/<api>/cassettes/<name>.json` (a bare name) or a path; `--replay <name|path>` plays it back with no network. Cassettes are transport-level (auth token requests included) and back the golden tests in [12](12-testing.md); the runner and tests use them through `ClientOptions::cassette` (`Cassette::replay(path)`).
  - **Redaction** happens when the file is written, so tokens registered after their response (OAuth access tokens) are masked too: registered secrets and sensitive query parameters become `****`, `Authorization`/`Cookie`/`X-Api-Key`-style request headers, the spec's `headers`-auth headers and HMAC signature headers are dropped, and `Set-Cookie` is masked.
  - **Matching** is deterministic: method, the redacted URL with sorted query pairs (minus `match.ignore_query`, set with `--ignore-param`, for volatile values such as HMAC timestamps) and the redacted body. Request headers are never matched. Each interaction is used once, in recorded order among equal keys; a request with none left fails with `cassette_miss` (class `config`).
  - Replay doesn't read secrets: each connection secret becomes a registered placeholder, so it matches the recorded `****`.
  - **Clock.** `now()` is pinned for the whole session: to the recording's start (millisecond precision), which is saved as `recorded_at`, and on replay to `recorded_at`. Clock-derived requests (`range()` windows, `now()` params, sync floors) replay identically on any later day, so a ported spec is cassette-tested as written. HMAC `timestamp`s come from the same clock.
  - **Pacing.** Replay paces on a virtual clock: `rps`, `min_interval`, throttle pauses and retry backoff are computed, and reported (`rate_wait_ms`, `throttle_wait_ms`, retry `wait_ms`), as live, without sleeping.
- `atlatl api explain <connection|spec> [<endpoint>] [--json]` prints the effective merged config: defaults, auth (templates, redacted), limiter, pagination, decode, records, rules in evaluation order (including built-ins) and dependencies (`depends_on`, consumed queues).

## 14. Coverage of current specs

| Current spec feature | atlatl |
|---|---|
| Static header (`paged_queue`), key-in-query (`queue_chain`, `record_queue`, `windowed`, `throttled`) | `auth: headers` / `auth: query`, or `defaults.params`; both are now inherited by every endpoint |
| OAuth2 client credentials with scopes (`oauth_cursor`) | `auth: oauth2`, with refresh on expiry |
| `defaults.state` | spec `state` |
| Endpoint `state` initial values (page cursors) | handled by `paginate` strategies |
| `pagination.next_state` + `stop_condition` | a `paginate` type; `expression` as the fallback |
| Cursor spliced into the URL (`oauth_cursor`) | `paginate: {type: cursor}` + null/empty omission |
| `iterate.over` list / `range()` / `state.x` / `queue.x` | `for_each.over` |
| `iterate.concurrency`, `request.concurrency` | `for_each.concurrency`, `rate_limit.max_in_flight` |
| `request.rate` | `rate_limit.rps` (connection-scoped) + `min_interval` |
| `request.headers` (`monthly` UA) | `request.headers` / `defaults.headers` |
| `setup` + `processors → state` with `aggregation: collect` | `setup` + `collect` |
| `depends_on` (`paged_queue`) | `depends_on` / inferred |
| `records.jmespath` incl. filters, projections, `merge()`, `[]` | `records.path` (standard JMESPath) |
| `records.primary_key` | `records.primary_key` + `dedup` |
| `processors → record.*` | `fields` |
| `processors → queue.*` | `emit` |
| `rules` retry/continue with `backoff` / `backoff_base` / `jitter` / `message` | `rules` with `retry` / `skip` / `ignore` / `fail`, typed backoff, `message` |
| `response.format: json` | `decode.format` |
| Functions `now`, `date_add`, `date_parse`, `date_format`, `range`, `coalesce`, `contains` | §9 |
