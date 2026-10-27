from __future__ import annotations

import json
import os
import stat
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from dagster_atlatl import AtlatlPlan

TESTS = Path(__file__).parent
FIXTURES = TESTS / "fixtures"
PROJECT = FIXTURES / "project"
PLAN_JSON = FIXTURES / "plan.json"
REPO = TESTS.parents[2]


@dataclass
class Fake:
    root: Path
    bin: Path

    def scenario(self, **sc: Any) -> None:
        (self.root / "scenario.json").write_text(json.dumps(sc))

    def calls(self) -> list[list[str]]:
        p = self.root / "calls.jsonl"
        return [json.loads(line) for line in p.read_text().splitlines()] if p.exists() else []

    def signals(self) -> list[str]:
        p = self.root / "signals"
        return p.read_text().split() if p.exists() else []

    @property
    def hung(self) -> Path:
        return self.root / "hung"


@pytest.fixture
def fake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Fake:
    root = tmp_path / "fake"
    root.mkdir()
    exe = root / "atlatl"
    exe.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{TESTS / "fake_atlatl.py"}" "$@"\n')
    exe.chmod(exe.stat().st_mode | stat.S_IXUSR)
    env = {
        "ATLATL_BIN": str(exe),
        "FAKE_ATLATL_PLAN": str(PLAN_JSON),
        "FAKE_ATLATL_SCENARIO": str(root / "scenario.json"),
        "FAKE_ATLATL_CALLS": str(root / "calls.jsonl"),
        "FAKE_ATLATL_SIGNALS": str(root / "signals"),
        "FAKE_ATLATL_STATE": str(root / "state.json"),
    }
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    f = Fake(root, exe)
    f.scenario()
    return f


@pytest.fixture
def plan() -> AtlatlPlan:
    return AtlatlPlan.from_json(json.loads(PLAN_JSON.read_text()))


def shapes_plan_json() -> dict[str, Any]:
    """The fixture plan with `sales/lines` served by a `then:` hop and `refunds` a `for_each`."""
    data: dict[str, Any] = json.loads(PLAN_JSON.read_text())
    lake = {"name": "lake", "type": "ducklake"}
    pg = {"name": "app_pg", "type": "postgres"}
    for r in data["replications"]:
        if r["name"] == "orders":
            r["hops"] = [
                {
                    "hop": 0,
                    "parent": None,
                    "source": lake,
                    "target": lake,
                    "streams": ["sales/orders"],
                },
                {"hop": 1, "parent": 0, "source": lake, "target": pg, "streams": ["sales/lines"]},
            ]
            r["streams"][1]["hop"] = 1
            r["streams"][1]["asset"]["kinds"] = ["atlatl", "postgres"]
        if r["name"] == "refunds":
            r["for_each"] = {"over": ["a", "b"], "as": "tenant", "key": None}
    return data


@pytest.fixture
def shapes_plan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fake: Fake) -> AtlatlPlan:
    data = shapes_plan_json()
    path = tmp_path / "shapes_plan.json"
    path.write_text(json.dumps(data))
    monkeypatch.setenv("FAKE_ATLATL_PLAN", str(path))
    return AtlatlPlan.from_json(data)


@pytest.fixture
def real_atlatl() -> Path:
    exe = Path(os.environ.get("ATLATL_REAL_BIN", REPO / "target" / "debug" / "atlatl"))
    if not exe.is_file():
        pytest.skip(f"{exe} not built (cargo build -p atlatl-cli)")
    return exe
