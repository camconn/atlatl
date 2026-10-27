# atlatl-conn-clickhouse

ClickHouse as a read-only source over its HTTP(S) interface: every query runs `readonly`, and results stream back as Parquet for the engine to read.

Part of [atlatl](https://github.com/camconn/atlatl): an internal piece of the `atlatl` command line ([`atlatl-cli`](https://crates.io/crates/atlatl-cli)). It's published so the CLI can be, and its API isn't stable: it can change in any release.

Spec: [ClickHouse](https://github.com/camconn/atlatl/blob/master/spec/06-connectors.md#11-clickhouse-source).
