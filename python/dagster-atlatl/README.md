# dagster-atlatl

Dagster integration for [atlatl](../../README.md). The wheel bundles the `atlatl` binary and the
DuckDB extensions it loads; see `spec/09-dagster.md`.

Development (no Rust build needed; point `ATLATL_BIN` at a local binary):

```sh
uv sync --no-install-project
uv run --no-sync ruff check && uv run --no-sync ruff format --check
uv run --no-sync pyright
uv run --no-sync pytest
```

Release wheels are built by `.github/workflows/release.yml`: `scripts/stage-wheel-data.sh` stages
libduckdb, libsqlite3 (Linux), the extensions and `LICENSE-THIRD-PARTY` into `atlatl.data/data/`, then
`maturin build --profile dist --auditwheel skip` packs them with the binary.
