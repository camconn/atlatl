# atlatl-conn-postgres

Postgres as a source (catalog discovery, then reads through DuckDB's `postgres` extension in read-only sessions) and as a serving target (changes loaded with binary `COPY`, then applied as idempotent, cursor-guarded upserts and deletes).

Part of [atlatl](https://github.com/camconn/atlatl): an internal piece of the `atlatl` command line ([`atlatl-cli`](https://crates.io/crates/atlatl-cli)). It's published so the CLI can be, and its API isn't stable: it can change in any release.

Spec: [Postgres](https://github.com/camconn/atlatl/blob/master/spec/06-connectors.md#3-postgres).
