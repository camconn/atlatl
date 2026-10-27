from __future__ import annotations

import json
import shutil
import textwrap
from pathlib import Path

import dagster as dg
import pytest
from conftest import PLAN_JSON, PROJECT
from dagster.components.testing import create_defs_folder_sandbox

from dagster_atlatl import (
    SCHEMA_CHECK,
    AtlatlPlan,
    AtlatlPlanError,
    AtlatlProjectComponent,
    assert_asset_keys,
    build_atlatl_assets,
    build_atlatl_defs,
    load_plan,
)

KEYS = {
    "br/sales/orders": {"op": "sales", "group": "sales", "replication": "orders"},
    "sales/lines": {"op": "sales", "group": "sales", "replication": "orders"},
    "sales/refunds": {"op": "sales", "group": None, "replication": "refunds"},
    "hr/people": {"op": "hr", "group": "hr", "replication": "people"},
}


def by_op(assets: list[dg.AssetsDefinition]) -> dict[str, dg.AssetsDefinition]:
    return {a.node_def.name: a for a in assets}


def test_real_plan_builds_asset_specs(real_atlatl: Path) -> None:
    plan = load_plan(PROJECT, binary=real_atlatl)
    ops = by_op(build_atlatl_assets(plan, binary=real_atlatl))
    assert set(ops) == {"sales", "hr"}
    sales = ops["sales"]
    assert sales.can_subset
    assert {k.to_user_string() for k in sales.keys} == {
        "br/sales/orders",
        "sales/lines",
        "sales/refunds",
    }
    orders = sales.specs_by_key[dg.AssetKey(["br", "sales", "orders"])]
    assert orders.group_name == "sales"
    assert orders.description == "Orders and their lines"
    assert [d.asset_key for d in orders.deps] == [dg.AssetKey(["br", "orders"])]
    assert orders.kinds == {"atlatl", "ducklake"}
    assert orders.tags["tier"] == "gold"
    stream = plan.replications[[r.name for r in plan.replications].index("orders")].streams[0]
    assert orders.code_version == stream.asset.code_version
    assert orders.skippable
    assert orders.metadata["atlatl/replication"] == "orders"
    assert orders.metadata["atlatl/stream_id"] == "sales/orders"
    refunds = sales.specs_by_key[dg.AssetKey(["sales", "refunds"])]
    assert refunds.group_name == "default"
    assert not list(sales.check_specs)
    people = ops["hr"]
    assert [c.key for c in people.check_specs] == [
        dg.AssetCheckKey(dg.AssetKey(["hr", "people"]), SCHEMA_CHECK)
    ]


def test_fixture_plan_matches_real_binary(real_atlatl: Path, plan: AtlatlPlan) -> None:
    real = load_plan(PROJECT, binary=real_atlatl)
    assert real.replications == plan.replications


def test_plan_errors_surface(real_atlatl: Path, tmp_path: Path) -> None:
    shutil.copytree(PROJECT, tmp_path / "p")
    f = tmp_path / "p" / "replications" / "hr.yaml"
    f.write_text(f.read_text().replace("object:", "objekt:"))
    with pytest.raises(AtlatlPlanError, match=r"config error\)[\s\S]*objekt"):
        load_plan(tmp_path / "p", binary=real_atlatl)


def test_unparseable_plan(tmp_path: Path) -> None:
    exe = tmp_path / "atlatl"
    exe.write_text("#!/bin/sh\necho not-json\n")
    exe.chmod(0o755)
    with pytest.raises(AtlatlPlanError, match="invalid JSON"):
        load_plan(tmp_path, binary=exe)


def test_op_grouping(plan: AtlatlPlan) -> None:
    assert {op: [r.name for r in reps] for op, reps in plan.ops().items()} == {
        "hr": ["people"],
        "sales": ["orders", "refunds"],
    }


def test_replication_globs(plan: AtlatlPlan) -> None:
    ops = by_op(build_atlatl_assets(plan, replications=["ord*", "people"]))
    assert {k.to_user_string() for k in ops["sales"].keys} == {"br/sales/orders", "sales/lines"}
    assert set(ops) == {"sales", "hr"}
    with pytest.raises(AtlatlPlanError, match="matched nothing: nope"):
        build_atlatl_assets(plan, replications=["*", "nope"])


def test_build_defs_without_components(plan: AtlatlPlan) -> None:
    defs = build_atlatl_defs(plan, replications=["refunds"])
    keys = {k.to_user_string() for k in defs.resolve_asset_graph().get_all_asset_keys()}
    assert keys == {"sales/refunds"}


def test_assert_asset_keys(plan: AtlatlPlan) -> None:
    assert_asset_keys(plan, KEYS)
    assert_asset_keys(plan, list(KEYS))
    wrong = {**KEYS, "hr/people": {"op": "people"}, "gone": {}}
    del wrong["sales/lines"]
    with pytest.raises(AssertionError) as e:
        assert_asset_keys(plan, wrong)
    msg = str(e.value)
    assert "missing: gone" in msg
    assert "unexpected: sales/lines" in msg
    assert "hr/people: op is 'hr', expected 'people'" in msg


def test_component_loads_real_plan(real_atlatl: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATLATL_BIN", str(real_atlatl))
    with create_defs_folder_sandbox() as sandbox:
        defs_path = sandbox.defs_folder_path / "atlatl_ingest"
        defs_path.mkdir()
        (defs_path / "defs.yaml").write_text(
            textwrap.dedent(f"""
                type: dagster_atlatl.AtlatlProjectComponent
                attributes:
                  project: "{PROJECT}"
                  environment_from: DBT_PROFILE
                  replications: ["orders", "peo*"]
            """)
        )
        with sandbox.load_component_and_build_defs(defs_path=defs_path) as (component, defs):
            assert isinstance(component, AtlatlProjectComponent)
            assert component.environment_from == "DBT_PROFILE"
            graph = defs.resolve_asset_graph()
            assert {k.to_user_string() for k in graph.get_all_asset_keys()} >= {
                "br/sales/orders",
                "sales/lines",
                "hr/people",
            }
            assert dg.AssetKey(["sales", "refunds"]) not in graph.get_all_asset_keys()
            assert graph.get(dg.AssetKey(["hr", "people"])).group_name == "hr"


def test_component_relative_project_and_plan_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATLATL_BIN", "/bin/false")
    with create_defs_folder_sandbox() as sandbox:
        defs_path = sandbox.defs_folder_path / "atlatl_ingest"
        defs_path.mkdir()
        (defs_path / "defs.yaml").write_text(
            "type: dagster_atlatl.AtlatlProjectComponent\nattributes:\n  project: atlatl\n"
        )
        with (
            pytest.raises(Exception, match="atlatl_ingest") as e,
            sandbox.load_component_and_build_defs(defs_path=defs_path),
        ):
            pass
        cause = e.value.__cause__
        assert isinstance(cause, AtlatlPlanError)
        assert f"for {defs_path / 'atlatl'} failed" in str(cause)


def test_fixture_is_static() -> None:
    assert json.loads(PLAN_JSON.read_text())["static"] is True


def test_hop_streams_carry_their_hop_target(shapes_plan: AtlatlPlan) -> None:
    orders = next(r for r in shapes_plan.replications if r.name == "orders")
    assert [(h.hop, h.parent, h.target.name) for h in orders.hops] == [
        (0, None, "lake"),
        (1, 0, "app_pg"),
    ]
    lines = orders.streams[1]
    assert (lines.hop, orders.target_of(lines).type) == (1, "postgres")
    assert next(r for r in shapes_plan.replications if r.name == "refunds").for_each
    sales = by_op(build_atlatl_assets(shapes_plan))["sales"]
    spec = sales.specs_by_key[dg.AssetKey(["sales", "lines"])]
    assert (spec.metadata["atlatl/target"], spec.metadata["atlatl/target_type"]) == (
        "app_pg",
        "postgres",
    )
    assert spec.kinds == {"atlatl", "postgres"}
    orders_spec = sales.specs_by_key[dg.AssetKey(["br", "sales", "orders"])]
    assert orders_spec.metadata["atlatl/target"] == "lake"
