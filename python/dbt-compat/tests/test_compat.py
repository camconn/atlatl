"""atlatl and the consumer's dbt-duckdb take turns on the same files (spec 12 §3).

atlatl lands raw tables, dbt builds on them, and atlatl then reads what dbt wrote, for both a
plain DuckDB file and a DuckLake with a DuckDB-file catalog. Needs the `atlatl` binary
(`ATLATL_BIN`, default `target/debug/atlatl`: `cargo build -p atlatl-cli`) and the bundled DuckDB
extensions (`scripts/fetch-duckdb-extensions.sh`).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

REPO = Path(__file__).resolve().parents[3]
BIN = Path(os.environ.get("ATLATL_BIN", REPO / "target" / "debug" / "atlatl"))
EXTENSIONS = Path(os.environ.get("ATLATL_EXTENSION_DIR", REPO / "extensions"))


def write(p: Path, text: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)


def atlatl(project: Path, *args: str) -> dict[str, object]:
    if not BIN.is_file():
        pytest.fail(f"no atlatl binary at {BIN}; run `cargo build -p atlatl-cli` or set ATLATL_BIN")
    report = project / "report.json"
    out = subprocess.run(
        [
            str(BIN),
            "--project",
            str(project),
            "run",
            "--extension-dir",
            str(EXTENSIONS),
            "--report",
            str(report),
            *args,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    detail = report.read_text() if report.exists() else out.stderr + out.stdout
    assert out.returncode == 0, detail
    result: dict[str, object] = json.loads(report.read_text())
    return result


def dbt(project: Path, *args: str) -> None:
    # Its own process, as in production: dbt-duckdb holds the file open until its process exits.
    exe = Path(sys.executable).with_name("dbt")
    out = subprocess.run(
        [str(exe), *args, "--project-dir", str(project), "--profiles-dir", str(project)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert out.returncode == 0, out.stdout + out.stderr


def dbt_project(root: Path, profile: str, models: dict[str, str]) -> Path:
    d = root / "dbt"
    write(
        d / "dbt_project.yml",
        "name: compat\nversion: '1.0.0'\nprofile: compat\nmodel-paths: [models]\n",
    )
    write(d / "profiles.yml", profile)
    for name, sql in models.items():
        write(d / "models" / f"{name}.sql", sql)
    return d


def settings() -> str:
    # Offline: extensions come from atlatl's bundled directory, never the network.
    return (
        f"      settings:\n"
        f"        extension_directory: '{EXTENSIONS}'\n"
        f"        autoinstall_known_extensions: false\n"
    )


def test_plain_duckdb_file(tmp_path: Path) -> None:
    db = tmp_path / "warehouse.duckdb"
    # The file already exists, written by dbt's own DuckDB.
    duckdb.connect(str(db)).close()
    proj = tmp_path / "atlatl"
    write(
        proj / "atlatl.yaml",
        f"version: 1\nstate: {{lake: wh}}\nconnections:\n"
        f"  wh: {{type: duckdb, path: '{db}'}}\n  files: {{type: files}}\n",
    )
    write(
        proj / "replications" / "prices.yaml",
        "replications:\n  prices:\n    source: files\n    target: wh\n    streams:\n"
        "      - {id: f/prices, mode: full-refresh, from: {files: {url: in, glob: '*.csv'}},"
        " object: br.prices, primary_key: [ticker], columns: {px: {type: DOUBLE}}}\n",
    )
    write(proj / "in" / "prices.csv", "ticker,px\nAAA,1.5\nBBB,2\n")
    atlatl(proj)

    d = dbt_project(
        tmp_path,
        f"compat:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n"
        f"      path: '{db}'\n      threads: 1\n{settings()}",
        {
            "ag_prices": "{{ config(materialized='table', schema='ag') }}\n"
            "SELECT ticker, px * 2 AS px2 FROM br.prices"
        },
    )
    dbt(d, "build")

    # atlatl reopens the file dbt last wrote, and dbt rebuilds on the new rows.
    write(proj / "in" / "prices.csv", "ticker,px\nAAA,3\nCCC,4\n")
    report = atlatl(proj)
    assert report["status"] == "succeeded", report
    dbt(d, "build")
    with duckdb.connect(str(db), read_only=True) as c:
        got = c.sql("SELECT ticker, px2 FROM main_ag.ag_prices ORDER BY 1").fetchall()
        runs = c.sql("SELECT count(*) FROM _atlatl.runs").fetchone()
    assert got == [("AAA", 6.0), ("CCC", 8.0)]
    assert runs == (2,), "atlatl's run records live in the same file"


def test_ducklake_with_a_duckdb_catalog(tmp_path: Path) -> None:
    catalog = tmp_path / "catalog.duckdb"
    data = tmp_path / "data"
    proj = tmp_path / "atlatl"
    write(
        proj / "atlatl.yaml",
        "version: 1\nstate: {lake: lake}\nconnections:\n"
        f"  lake: {{type: ducklake, alias: catalog, catalog: {{type: duckdb, path: '{catalog}'}},"
        f" data_path: '{data}/', storage: {{type: local}}}}\n"
        "  files: {type: files}\n",
    )
    write(
        proj / "replications" / "lake.yaml",
        "replications:\n  prices:\n    source: files\n    target: lake\n    streams:\n"
        "      - {id: f/prices, mode: full-refresh, from: {files: {url: in, glob: '*.csv'}},"
        " object: br.prices, primary_key: [ticker], columns: {px: {type: DOUBLE}}}\n"
        "  copy_au:\n    source: lake\n    target: lake\n    streams:\n"
        "      - {id: l/au, mode: full-refresh, from: {table: main_au.prices2},"
        " object: br.prices2_copy, primary_key: [ticker]}\n",
    )
    write(proj / "in" / "prices.csv", "ticker,px\nAAA,1.5\nBBB,2\n")
    atlatl(proj, "--replication", "prices")

    d = dbt_project(
        tmp_path,
        "compat:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n"
        "      path: ':memory:'\n      threads: 1\n"
        "      attach:\n"
        f"        - path: 'ducklake:{catalog}'\n"
        "          alias: catalog\n"
        f"          options: {{data_path: '{data}/'}}\n"
        f"{settings()}",
        {
            "prices2": "{{ config(materialized='table', database='catalog', schema='au') }}\n"
            "SELECT ticker, px * 2 AS px2 FROM catalog.br.prices"
        },
    )
    dbt(d, "build")

    # atlatl reads the table dbt-duckdb committed to the lake (dbt's default schema naming
    # prefixes custom schemas with the target's, so `au` lands as `main_au`).
    report = atlatl(proj, "--replication", "copy_au")
    assert report["status"] == "succeeded", report
    c = duckdb.connect()
    c.execute(f"SET extension_directory = '{EXTENSIONS}'")
    c.execute(f"ATTACH 'ducklake:{catalog}' AS catalog (DATA_PATH '{data}/', READ_ONLY)")
    got = c.sql("SELECT ticker, px2 FROM catalog.br.prices2_copy ORDER BY 1").fetchall()
    assert got == [("AAA", 3.0), ("BBB", 4.0)]
