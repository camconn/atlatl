# atlatl: design spec

atlatl is a single-binary Rust (tokio) data-movement tool built around an in-process DuckDB engine and DuckLake as the source of truth:

1. **Ingest**: pull external sources (REST APIs, Postgres, Redshift, ClickHouse, files, Delta, Postgres logical replication) into DuckLake or a DuckDB file.
2. **Serve**: push DuckLake tables into serving databases (Postgres, SQLite, D1, Turso), with **DuckLake's change feed** as the primary mechanism.

This directory is the specification and design. End-user docs and implementation notes are in [`docs/`](../docs/).

## Documents

| # | Doc | What it covers |
|---|---|---|
| 01 | [Architecture](01-architecture.md) | Process model, engine, data plane, crate layout, memory, extension seams |
| 02 | [Configuration](02-configuration.md) | Project layout, environments, connections, replications/streams, templating, secrets |
| 03 | [Execution](03-execution.md) | Planning, scheduling, atomicity, retries, errors, rejects, kill behaviour, locks, exit codes |
| 04 | [Ingest semantics](04-ingest.md) | Write modes into DuckLake, staging, typing, source schemas, table drift, chunking, rejects, adoption of existing tables |
| 05 | [State](05-state.md) | Cursors and watermarks, where state lives, transactional commit, run records |
| 06 | [Connectors](06-connectors.md) | DuckLake, Postgres, Redshift, files, SQLite, D1, Turso, and later ones |
| 07 | [REST API engine](07-rest-api.md) | Declarative HTTP source: auth, requests, pagination, iteration, queues, rules, decoding |
| 08 | [CDC](08-cdc.md) | DuckLake → serving change apply, bootstrap and adoption, retention guard; future CDC sources |
| 09 | [Dagster integration](09-dagster.md) | `dagster-atlatl`, asset identity, results contract, environment selection |
| 10 | [Observability](10-observability.md) | Logs, event stream, run report schema, OTLP, redaction |
| 11 | [Extensibility and future work](11-extensibility.md) | Plugin seams, future sources, larger pipeline graphs, backlog |
| 12 | [Testing](12-testing.md) | Unit, integration, fault injection, parity harness |
| 13 | [dbt integration](13-dbt.md) | `dbt-atlatl`: the dbt-duckdb plugin that runs atlatl first, and dbt sources from the plan |

## Goals

- **Reliability first**:
  - each replication succeeds or fails as a unit, and independent replications in one run don't affect each other;
  - atlatl owns retries: typed errors with backoff, and decode failures retried per request or per row as configured;
  - kill-safe: every commit is atomic, so killing the process at any point leaves no partial writes;
  - correct exit codes;
  - a structured results report.
- **Bounded memory.** DuckDB runs in-process with its budget set before any work starts. Engine buffers are bounded as well.
- **CDC out of DuckLake** with exactly-once apply, hard and soft deletes, and a retention guard.
- **Persisted state** (API sync cursors, watermarks, CDC cursors), inspectable and rewindable.
- **No license server, no telemetry, no closed code.**
- **Room to grow.** v1 is deliberately small. The seams for future sources, targets and larger pipeline graphs exist from day one ([11](11-extensibility.md)):
  - multi-hop replications;
  - fan-out;
  - per-tenant fan-out;
  - cross-replication transactions.

## Non-goals

- A web UI, an agent/platform model, or a scheduler. Dagster schedules and orchestrates.
- Transforms beyond per-column casts and expressions. dbt owns transforms.
- ODBC; schema migration of PKs, FKs and indexes on serving targets. Atlas owns Postgres DDL.
- A long-running daemon, or streaming, continuous or distributed execution. atlatl is a CLI that runs to completion per invocation.
- Non-Rust extension mechanisms (plugins, subprocess sources). Every source is Rust core plus YAML config.

## Glossary

| Term | Meaning |
|---|---|
| **Project** | A directory with `atlatl.yaml` plus replication files |
| **Environment** | A named profile (`dev`, `prod`) that selects connection settings |
| **Connection** | A named, typed endpoint (DuckLake, Postgres, REST API, …) |
| **Replication** | A named set of streams sharing a source, a target and defaults. It is the **atomic unit**: all its streams commit together or not at all. Usually one vendor asset into `br`, or one `au` asset (plus its shards) into serving. |
| **Stream** | One logical dataset within a replication, with a stable `id`. It is the unit of state and asset identity |
| **Member** | One concrete table a wildcard stream expands to. A non-wildcard stream has exactly one member |
| **Task** | An internal scheduling unit (extracting one member, applying one stream). It isn't user-visible for commit purposes |
| **Run** | One `atlatl run` invocation: a set of replications that are independent unless grouped with `atomic_group`. It produces an event stream and a run report |
| **Cursor** | The persisted position of a stream: an `update_key` watermark, an API `sync` value, or a DuckLake snapshot id |
| **Rejects** | Rows that still fail decoding or casting after retries. They're recorded in the lake and retried on later runs |
| **Source schema** | The declared expected upstream shape of a stream, enforced every run |
| **Staging** | Run-scoped scratch storage (a DuckDB temp table, a Postgres session temp table) |
| **Lake** | The DuckLake catalog + data path attached in-process |
