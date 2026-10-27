# atlatl-testkit

Test fixtures for atlatl's crates: Postgres, S3 (RustFS), SFTP, ClickHouse, libSQL and toxiproxy
containers, local DuckLake catalogs, fake D1 and Redshift servers, and temporary projects.
Containers start through the `docker` CLI and are removed when the test process exits; set
`ATLATL_TEST_*` variables to point tests at existing services instead.

Dev-only and not published.
