# atlatl-conn-ducklake

DuckLake and plain DuckDB files as targets and sources: attaching catalogs, S3 secrets, data-path and format-version checks, snapshot helpers and lock backends.

Part of [atlatl](https://github.com/camconn/atlatl): an internal piece of the `atlatl` command line ([`atlatl-cli`](https://crates.io/crates/atlatl-cli)). It's published so the CLI can be, and its API isn't stable: it can change in any release.

Spec: [DuckLake](https://github.com/camconn/atlatl/blob/master/spec/06-connectors.md#2-ducklake), [DuckDB files](https://github.com/camconn/atlatl/blob/master/spec/06-connectors.md#10-duckdb-files).
