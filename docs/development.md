# Development

Requires Rust 1.90 (pinned in `rust-toolchain.toml`) and Docker for the integration tests, which run against local
Postgres and RustFS containers. libduckdb is downloaded at build time.

```sh
scripts/fetch-duckdb-extensions.sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
```

For Python, see [`python/dagster-atlatl/README.md`](../python/dagster-atlatl/README.md).

## Live-network tests

The default test run is offline: containers on localhost and replayed cassettes. Tests that reach the internet
only run behind the `atlatl-cli` `network` feature ([spec/12-testing.md](../spec/12-testing.md) §5), via
`cargo test-network`. See [`examples/fred/README.md`](../examples/fred/README.md).

Local secrets live in a gitignored `.env` at the repo root; [`.env.example`](../.env.example) lists them. Neither
atlatl nor cargo reads `.env`, so load it into the shell first (or use direnv):

```sh
cp .env.example .env      # then fill in the values
set -a; . ./.env; set +a
cargo test-network
```

| Variable | Used for |
|---|---|
| `FRED_API_KEY` | A free [FRED API key](https://fred.stlouisfed.org/docs/api/api_key.html), reserved for live tests against FRED's JSON API. The current FRED examples use the keyless graph CSVs and don't need it. |

Replays need no key, but check a new cassette for secrets before committing it.

## Implementation notes

[`implementation/`](implementation/) holds spike findings the code and spec cite (`spike S3 §6.2` etc.),
including runbooks for the spikes still open (S8, S11).
