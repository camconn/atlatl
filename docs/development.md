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

See [`TESTING.md`](../TESTING.md) for every test tier, the features and aliases, and their manual invocations.

The default test run is offline: containers on localhost and replayed cassettes. Tests that reach the internet
only run behind the `atlatl-cli` `network` feature ([spec/12-testing.md](../spec/12-testing.md) §5), via
`cargo test-network`. See [`examples/fred/README.md`](../examples/fred/README.md). CI runs them on pushes to
`master` (and on demand) in [`network.yml`](../.github/workflows/network.yml), with `FRED_API_KEY` as a repository
secret; never on pull requests.

Local secrets live in a gitignored `.env` at the repo root; [`.env.example`](../.env.example) lists them. Neither
atlatl nor cargo reads `.env`, so load it into the shell first (or use direnv):

```sh
cp .env.example .env      # then fill in the values
set -a; . ./.env; set +a
cargo test-network
```

| Variable | Used for |
|---|---|
| `FRED_API_KEY` | A free [FRED API key](https://fred.stlouisfed.org/docs/api/api_key.html) for `live_fred_api_unrate_matches_the_graph_csv` (FRED's JSON API, `examples/fred/apis/fred_api.yaml`), which skips without it. The test also reads it from `.env`. The graph-CSV tests are keyless. |

Replays need no key, but check a new cassette for secrets before committing it.

## Implementation notes

[`implementation/`](implementation/) holds spike findings the code and spec cite (`spike S3 §6.2` etc.),
including runbooks for the spikes still open (S8, S11).
