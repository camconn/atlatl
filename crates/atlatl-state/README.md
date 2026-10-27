# atlatl-state

The state store: stream cursors and sync values, run records and rejects, kept in an `_atlatl` schema in the target DuckLake or DuckDB file. Writes join the replication's transaction, so state commits with the data it describes.

Part of [atlatl](https://github.com/camconn/atlatl): an internal piece of the `atlatl` command line ([`atlatl-cli`](https://crates.io/crates/atlatl-cli)). It's published so the CLI can be, and its API isn't stable: it can change in any release.

Spec: [State](https://github.com/camconn/atlatl/blob/master/spec/05-state.md).
