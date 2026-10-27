# atlatl-cdc

Reads changes out of DuckLake for serving targets: the net change between two snapshots on the business key (the change feed only says which keys to look at), and the snapshot that deleted each removed key.

Part of [atlatl](https://github.com/camconn/atlatl): an internal piece of the `atlatl` command line ([`atlatl-cli`](https://crates.io/crates/atlatl-cli)). It's published so the CLI can be, and its API isn't stable: it can change in any release.

Spec: [CDC](https://github.com/camconn/atlatl/blob/master/spec/08-cdc.md).
