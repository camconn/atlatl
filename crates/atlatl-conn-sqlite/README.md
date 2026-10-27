# atlatl-conn-sqlite

Serving targets in the SQLite family: local SQLite files, Cloudflare D1 and Turso (libSQL), sharing one change applier.

Part of [atlatl](https://github.com/camconn/atlatl): an internal piece of the `atlatl` command line ([`atlatl-cli`](https://crates.io/crates/atlatl-cli)). It's published so the CLI can be, and its API isn't stable: it can change in any release.

Spec: [SQLite](https://github.com/camconn/atlatl/blob/master/spec/06-connectors.md#6-sqlite-target), [D1](https://github.com/camconn/atlatl/blob/master/spec/06-connectors.md#7-d1-target), [Turso](https://github.com/camconn/atlatl/blob/master/spec/06-connectors.md#8-turso--libsql-target).
