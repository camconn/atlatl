# Usage

## Project layout

A project is a directory containing `atlatl.yaml` (environments, connections and state) and
`replications/**/*.yaml`. REST sources add `apis/*.yaml`; source schemas live in `schemas/`.
Full reference: [spec/02-configuration.md](../spec/02-configuration.md). Working projects are in
[`examples/`](../examples/).

## Commands

| Command | What it does |
|---|---|
| `atlatl validate` | Check config for every environment without running anything |
| `atlatl plan` | Resolve a selection (`--replication`, `--stream`, `--tag`) and print the plan |
| `atlatl run` | Run the selected replications (`--dry-run` diffs against the targets without committing) |
| `atlatl state show\|set\|reset\|accept\|init\|move` | Inspect or change stream cursors |
| `atlatl cdc bootstrap\|decommission` | Adopt or reload a serving table; drop a replication slot |
| `atlatl api test\|explain` | Try out a REST spec (redacted, with cassettes) |
| `atlatl schema infer\|check` | Manage source schemas |
| `atlatl json-schema <kind>` | Print the JSON Schema for config, plans, reports or events |
| `atlatl gc` | Remove leftovers from killed runs |

## Secrets

Config references secrets as `{secret: env:NAME}` (or `file:` / `aws-sm:`), never as literal values
([spec/02-configuration.md](../spec/02-configuration.md) §2.2). Resolved secrets are redacted from logs,
errors and recorded cassettes.

## Dagster

[`python/dagster-atlatl`](../python/dagster-atlatl) ships a wheel that bundles the binary and its DuckDB
extensions ([spec/09-dagster.md](../spec/09-dagster.md)).

## dbt

[`python/dbt-atlatl`](../python/dbt-atlatl) is a dbt-duckdb plugin: `dbt build` runs atlatl into
dbt's DuckDB file before dbt opens it, and `python -m dbt_atlatl sources` writes dbt sources from
the plan. A `duckdb`, `postgres` or `redshift` connection can take its settings from a dbt profile
target with `dbt: {profile: <name>}` ([spec/06-connectors.md](../spec/06-connectors.md) §12,
[spec/13-dbt.md](../spec/13-dbt.md)).
