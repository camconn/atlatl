# atlatl-conn-delta

Delta Lake tables as a source, read through DuckDB's `delta` extension and pinned to one table version per run.

Part of [atlatl](https://github.com/camconn/atlatl): an internal piece of the `atlatl` command line ([`atlatl-cli`](https://crates.io/crates/atlatl-cli)). It's published so the CLI can be, and its API isn't stable: it can change in any release.

Spec: [Connectors](https://github.com/camconn/atlatl/blob/master/spec/06-connectors.md#9-later-connectors-designed-for-not-built-in-v1).
