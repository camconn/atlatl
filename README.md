# Atlatl

`atlatl` is an opinionated data extraction and upload tool: a single Rust binary built around an in-process DuckDB
engine and [DuckLake](https://ducklake.select). It does two things:

1. **Pull** data into the lake.
2. **Push** lake tables into serving databases, driven by DuckLake's change feed.

It runs as a CLI, one invocation to completion, and integrates with dbt and Dagster.

## Features

- Atomic replications, kill-safe commits, correct exit codes
- Typed errors with retry + backoff
- Inspectable, rewindable state
- CDC out of DuckLake: exactly-once apply, hard + soft deletes, adoption of existing tables, retention guard
- Source schemas with drift detection
- No license server, no telemetry, no closed code

| Sources | Ingest targets | Serving targets |
|---|---|---|
| DuckLake, Postgres (incl. logical replication), Redshift, ClickHouse, Delta Lake, files (local, S3, HTTP(S), SFTP), REST APIs (declarative YAML) | DuckLake, DuckDB file | Postgres, SQLite, D1, Turso |

## Usage

```sh
atlatl validate
atlatl plan --replication <name>
atlatl run --replication <name>
```

See [docs/usage.md](docs/usage.md) and [`examples/`](examples/).

## Docs

- [docs/](docs/): usage and development
- [spec/](spec/README.md): specification and design

## License

© 2026 Cameron Conn. Licensed under the GNU Affero General Public License, version 3 or later. See `LICENSE.md`.

## Naming

`atlatl` is named after the [atlatl](https://en.wikipedia.org/wiki/Atlatl). It is also named twice after the worst
city in the Southern US.
