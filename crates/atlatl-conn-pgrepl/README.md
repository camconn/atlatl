# atlatl-conn-pgrepl

Postgres logical replication into the lake: owned slots and publications, its own `pgoutput` client and decoder (v1, or v2 with streaming and binary values on Postgres 14+), and bounded windows applied by primary key with the LSN cursor, confirming the slot only after the commit.

Part of [atlatl](https://github.com/camconn/atlatl): an internal piece of the `atlatl` command line ([`atlatl-cli`](https://crates.io/crates/atlatl-cli)). It's published so the CLI can be, and its API isn't stable: it can change in any release.

Spec: [CDC sources](https://github.com/camconn/atlatl/blob/master/spec/08-cdc.md#10-future-cdc-sources-into-the-lake).
