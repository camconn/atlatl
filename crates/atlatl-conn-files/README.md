# atlatl-conn-files

File sources: local paths, S3, HTTP(S) and SFTP, with compression detected by magic bytes, archive members, Hive-style paths and Redshift `UNLOAD` manifests.

Part of [atlatl](https://github.com/camconn/atlatl): an internal piece of the `atlatl` command line ([`atlatl-cli`](https://crates.io/crates/atlatl-cli)). It's published so the CLI can be, and its API isn't stable: it can change in any release.

Spec: [Files](https://github.com/camconn/atlatl/blob/master/spec/06-connectors.md#5-files).
