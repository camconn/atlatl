# Aries club scrape

Pulls every club member's Aries (finance.dev) account into a local DuckLake, keeping each response exactly as Aries returned it.

| Table | Endpoint | Shape |
|---|---|---|
| `br.aries_accounts` | `GET /v2/users/me/accounts` | one row per trading account (live and SIM), tagged with `member` |
| `br.aries_orders_history_raw` | `GET /v2/accounts/{id}/orders/history` | the full history every run: 90-day windows from `history_start`, `nextCursor` pages within each |
| `br.aries_orders_active_raw` | `GET /v2/accounts/{id}/orders` | open orders of any age plus today's terminal ones |
| `br.aries_positions_raw` | `GET /v1/accounts/{id}/positions` | per-position today/unrealized/realized P&L |
| `br.aries_balance_raw` | `GET /v2/accounts/{id}/balance` | cash, buying power, today's P&L |

Raw tables hold `{fetched_at, request_key, status, response_json}`, one row per response page, and only ever append. `request_key` is the request URL, so it names the account. Aries has no REST P&L history or fills, so P&L over time comes from these daily snapshots: run once a day after the close.

## Members

Each member creates a personal-token client (Client Center / Manage Account) and gives you its `client_id` (`ARI-PERSONAL-TK.…`) and `client_secret`. Aries ignores scopes, so **any of these can place orders**: keep `seed.duckdb` private, and members can revoke their client at any time.

```sh
duckdb seed.duckdb < seed.sql
duckdb seed.duckdb "INSERT INTO members VALUES ('alice', 'ARI-PERSONAL-TK.…', '…')"
```

The secrets are read through `for_each.secret`: redacted in logs and errors, never landed. Each member gets their own OAuth2 token.

## Run it

From this directory, with `atlatl` built (`cargo build -p atlatl-cli`; the binary is `../../target/debug/atlatl`):

```sh
export ATLATL_EXTENSION_DIR=$PWD/../../extensions
mkdir -p .lake/data
atlatl validate
atlatl run
```

`ARIES_SEED`, `ARIES_LAKE` and `ARIES_BASE_URL` override the seed file, the lake directory and `https://api.aries.com`. Change `history_start` in `replications/aries.yaml` to reach further back.

Read the lake from DuckDB 1.5.x:

```sql
ATTACH 'ducklake:.lake/meta.ducklake' AS aries (DATA_PATH '.lake/data/');
SELECT member, trading_account_id FROM aries.br.aries_accounts;
```

`cargo test -p atlatl-cli --test aries_example` runs this project against an in-process Aries fake.
