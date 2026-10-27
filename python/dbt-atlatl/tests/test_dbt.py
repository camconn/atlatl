"""dbt-duckdb runs atlatl through the plugin, then builds on what it landed.

Needs the `atlatl` binary (`ATLATL_BIN`, default `target/debug/atlatl`: `cargo build -p
atlatl-cli`) and the bundled DuckDB extensions (`scripts/fetch-duckdb-extensions.sh`).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from dbt_atlatl._cli import main as cli

REPO = Path(__file__).resolve().parents[3]
BIN = Path(os.environ.get("ATLATL_BIN", REPO / "target" / "debug" / "atlatl"))
EXTENSIONS = Path(os.environ.get("ATLATL_EXTENSION_DIR", REPO / "extensions"))


def write(p: Path, text: str) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    if not BIN.is_file():
        pytest.fail(f"no atlatl binary at {BIN}; run `cargo build -p atlatl-cli` or set ATLATL_BIN")
    monkeypatch.setenv("ATLATL_BIN", str(BIN))
    monkeypatch.setenv("ATLATL_EXTENSION_DIR", str(EXTENSIONS))
    monkeypatch.delenv("DBT_ATLATL_SKIP", raising=False)
    for db in ("wh", "other"):
        duckdb.connect(str(tmp_path / f"{db}.duckdb")).close()

    proj = tmp_path / "dbt"
    write(
        proj / "atlatl" / "atlatl.yaml",
        "version: 1\nstate: {lake: wh}\nconnections:\n"
        "  wh: {type: duckdb, dbt: {profile: consumer}}\n  files: {type: files}\n",
    )
    write(
        proj / "atlatl" / "replications" / "prices.yaml",
        "replications:\n  prices:\n    source: files\n    target: wh\n    streams:\n"
        "      - {id: f/prices, mode: full-refresh, from: {files: {url: in, glob: '*.csv'}},"
        " object: br.prices, primary_key: [ticker], columns: {px: {type: DOUBLE}}}\n",
    )
    write(proj / "atlatl" / "in" / "prices.csv", "ticker,px\nAAA,1.5\nBBB,2\n")
    write(
        proj / "dbt_project.yml",
        "name: consumer\nversion: '1.0.0'\nprofile: consumer\nmodel-paths: [models]\n",
    )
    # `other` inherits from `dev` through a YAML merge key, as real profiles often do.
    outputs = (
        "    dev: &dev\n      type: duckdb\n"
        "      path: \"{{ env_var('WH_DIR', '..') }}/wh.duckdb\"\n      threads: 1\n"
        "      settings:\n"
        f"        extension_directory: '{EXTENSIONS}'\n"
        "        autoinstall_known_extensions: false\n"
        "      plugins:\n"
        "        - module: dbt_atlatl\n          alias: atlatl\n"
        "          config: {project: atlatl, replications: [prices]}\n"
        "    other:\n      <<: *dev\n"
        "      path: \"{{ env_var('WH_DIR', '..') }}/other.duckdb\"\n"
    )
    write(proj / "profiles.yml", f"consumer:\n  target: dev\n  outputs:\n{outputs}")
    write(
        proj / "models" / "ag_prices.sql",
        "{{ config(materialized='table') }}\n"
        "SELECT ticker, px * 2 AS px2 FROM {{ source('br', 'prices') }}",
    )
    assert (
        cli(
            [
                "sources",
                "--project",
                str(proj / "atlatl"),
                "--out",
                str(proj / "models" / "_atlatl.yml"),
                "--profiles-dir",
                str(proj),
            ]
        )
        == 0
    )
    return proj


def dbt(project: Path, *args: str, ok: bool = True) -> subprocess.CompletedProcess[str]:
    # From the project dir: the profile's relative DuckDB path resolves against it.
    out = subprocess.run(
        [str(Path(sys.executable).with_name("dbt")), *args, "--profiles-dir", "."],
        cwd=project,
        # dbt imports the plugin; the project is not installed into the dev venv (no Rust build).
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")},
        capture_output=True,
        text=True,
        check=False,
    )
    assert (out.returncode == 0) == ok, out.stdout + out.stderr
    return out


def query(db: Path, sql: str) -> list[tuple[object, ...]]:
    with duckdb.connect(str(db), read_only=True) as c:
        return c.sql(sql).fetchall()


def test_build_runs_atlatl_first(project: Path) -> None:
    db = project.parent / "wh.duckdb"
    out = dbt(project, "build")
    log = out.stdout + (project / "logs" / "dbt.log").read_text()
    assert "replication succeeded" in log, "atlatl's own log lines reach dbt's"
    assert query(db, "SELECT ticker, px2 FROM main.ag_prices ORDER BY 1") == [
        ("AAA", 3.0),
        ("BBB", 4.0),
    ]
    assert query(db, "SELECT count(*) FROM _atlatl.runs") == [(1,)]

    write(project / "atlatl" / "in" / "prices.csv", "ticker,px\nAAA,3\nCCC,4\n")
    dbt(project, "run")
    assert query(db, "SELECT ticker, px2 FROM main.ag_prices ORDER BY 1") == [
        ("AAA", 6.0),
        ("CCC", 8.0),
    ]
    assert query(db, "SELECT count(*) FROM _atlatl.runs") == [(2,)]


def test_other_commands_and_skip_env_do_not_run_atlatl(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = project.parent / "wh.duckdb"
    dbt(project, "build")
    for cmd in (("parse",), ("ls",), ("compile",), ("show", "--inline", "SELECT 1"), ("test",)):
        dbt(project, *cmd)
    monkeypatch.setenv("DBT_ATLATL_SKIP", "1")
    dbt(project, "build")
    assert query(db, "SELECT count(*) FROM _atlatl.runs") == [(1,)]


def test_failed_atlatl_run_fails_dbt_before_any_model(project: Path) -> None:
    db = project.parent / "wh.duckdb"
    write(project / "atlatl" / "in" / "prices.csv", "ticker,px\nAAA,not-a-number\n")
    out = dbt(project, "build", ok=False)
    assert "atlatl run failed" in out.stdout + out.stderr
    assert query(
        db, "SELECT count(*) FROM information_schema.tables WHERE table_name = 'ag_prices'"
    ) == [(0,)]


def test_atlatl_follows_the_dbt_target(project: Path) -> None:
    dbt(project, "build", "--target", "other")
    other, wh = project.parent / "other.duckdb", project.parent / "wh.duckdb"
    assert query(other, "SELECT count(*) FROM main.ag_prices") == [(2,)]
    assert query(other, "SELECT count(*) FROM _atlatl.runs") == [(1,)]
    assert query(
        wh, "SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'br'"
    ) == [(0,)]


def test_duckdb_path_env_without_a_dbt_connection(project: Path) -> None:
    write(
        project / "atlatl" / "atlatl.yaml",
        "version: 1\nstate: {lake: wh}\nconnections:\n"
        "  wh: {type: duckdb, path: '{{ env.DBT_DUCKDB_PATH }}'}\n  files: {type: files}\n",
    )
    dbt(project, "build")
    assert query(project.parent / "wh.duckdb", "SELECT count(*) FROM main.ag_prices") == [(2,)]


def test_sources_check(project: Path) -> None:
    out = project / "models" / "_atlatl.yml"
    args = ["sources", "--project", str(project / "atlatl"), "--out", str(out), "--check"]
    args += ["--profiles-dir", str(project)]
    assert cli(args) == 0
    out.write_text(out.read_text().replace("prices", "stale"))
    assert cli(args) == 1
