# atlatl-engine

The in-process DuckDB engine every replication runs on: pooled sessions on a bounded thread pool, memory and thread budgets sized from cgroups or the host, statement timeouts and kills, Arrow batch streaming, and the bundled extension directory.

Part of [atlatl](https://github.com/camconn/atlatl): an internal piece of the `atlatl` command line ([`atlatl-cli`](https://crates.io/crates/atlatl-cli)). It's published so the CLI can be, and its API isn't stable: it can change in any release.

Spec: [Architecture](https://github.com/camconn/atlatl/blob/master/spec/01-architecture.md).
