# atlatl-conn-redshift

Redshift as a source, over the Data API (hand-rolled SigV4, no AWS SDK) or the Postgres wire protocol, with exact column types from `svv_columns` and large reads through `UNLOAD` to Parquet on S3.

Part of [atlatl](https://github.com/camconn/atlatl): an internal piece of the `atlatl` command line ([`atlatl-cli`](https://crates.io/crates/atlatl-cli)). It's published so the CLI can be, and its API isn't stable: it can change in any release.

Spec: [Redshift](https://github.com/camconn/atlatl/blob/master/spec/06-connectors.md#4-redshift).
