# atlatl-cli

The `atlatl` command: pull data into [DuckLake](https://ducklake.select) or a DuckDB file, and push
lake tables into serving databases, from declarative YAML. Sources include REST APIs, Postgres
(including logical replication), Redshift, ClickHouse, Delta Lake and files (local, S3, HTTP(S),
SFTP); serving targets are Postgres, SQLite, Cloudflare D1 and Turso.

```sh
atlatl validate
atlatl plan --replication <name>
atlatl run --replication <name>
```

## Install

The easiest way is the [`dbt-atlatl`](https://pypi.org/project/dbt-atlatl/) wheel, which installs
the `atlatl` binary together with the libduckdb and DuckDB extensions it needs:

```sh
pipx install dbt-atlatl    # or: uv tool install dbt-atlatl
```

From source, `cargo install atlatl-cli` needs the official libduckdb (`DUCKDB_DOWNLOAD_LIB=1`
fetches it) and the DuckDB extensions atlatl loads, in an extension directory
(`--extension-dir` or `ATLATL_EXTENSION_DIR`); it never installs extensions at run time.

See the [repository](https://github.com/camconn/atlatl) for usage, examples and the [spec](https://github.com/camconn/atlatl/blob/master/spec/README.md).
