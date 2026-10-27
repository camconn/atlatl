# atlatl-ingest

Writing into the lake: staging, validation, table drift and adoption of existing tables, source-schema checks, chunking, and the write modes (`full-refresh`, `incremental`, `snapshot`).

Part of [atlatl](https://github.com/camconn/atlatl): an internal piece of the `atlatl` command line ([`atlatl-cli`](https://crates.io/crates/atlatl-cli)). It's published so the CLI can be, and its API isn't stable: it can change in any release.

Spec: [Ingest semantics](https://github.com/camconn/atlatl/blob/master/spec/04-ingest.md).
