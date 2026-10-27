# No `from __future__ import annotations`: Dagster inspects compute fn annotations.

import _thread
import logging
import threading
import time
from collections.abc import Generator, Iterator
from pathlib import Path
from typing import Any

import dagster as dg
import pytest
from conftest import Fake

from dagster_atlatl import (
    SCHEMA_CHECK,
    AtlatlPlan,
    AtlatlResource,
    atlatl_assets,
    build_atlatl_assets,
)
from dagster_atlatl._process import AtlatlProcess, Event, Item

ORDERS = dg.AssetKey(["br", "sales", "orders"])
LINES = dg.AssetKey(["sales", "lines"])
REFUNDS = dg.AssetKey(["sales", "refunds"])
PEOPLE = dg.AssetKey(["hr", "people"])


def sales_op(plan: AtlatlPlan, **kw: Any) -> dg.AssetsDefinition:
    ops = {a.node_def.name: a for a in build_atlatl_assets(plan, **kw)}
    return ops["sales"]


def ephemeral() -> dg.DagsterInstance:
    return dg.DagsterInstance.ephemeral()  # pyright: ignore[reportUnknownMemberType]


def run(
    assets: list[dg.AssetsDefinition],
    selection: list[dg.AssetKey] | None = None,
    resources: dict[str, Any] | None = None,
) -> tuple[dg.ExecuteInProcessResult, dg.DagsterInstance]:
    instance = ephemeral()
    result = dg.materialize(
        assets, selection=selection, instance=instance, raise_on_error=False, resources=resources
    )
    return result, instance


def materialized(result: dg.ExecuteInProcessResult) -> dict[dg.AssetKey, dict[str, Any]]:
    ms = [e.materialization for e in result.get_asset_materialization_events()]
    return {m.asset_key: {k: v.value for k, v in m.metadata.items()} for m in ms}


def step_failure(result: dg.ExecuteInProcessResult) -> str:
    [event] = [e for e in result.all_events if e.is_step_failure]
    err = event.step_failure_data.error
    assert err is not None
    return err.message + "".join(c.message for c in [err.cause] if c is not None)


def flag(argv: list[str], name: str) -> list[str]:
    return [argv[i + 1] for i, a in enumerate(argv) if a == name]


def test_success_materializes_each_stream(
    fake: Fake, plan: AtlatlPlan, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DBT_PROFILE", "dev")
    ops = build_atlatl_assets(plan, resource=AtlatlResource(environment_from="DBT_PROFILE"))
    result, _ = run(ops, selection=[ORDERS, LINES, REFUNDS])
    assert result.success
    got = materialized(result)
    assert set(got) == {ORDERS, LINES, REFUNDS}
    md = got[ORDERS]
    assert md["dagster/row_count"] == 9
    assert md["dagster/table_name"] == "catalog.out.orders"
    assert md["dagster/storage_kind"] == "ducklake"
    assert md["atlatl/rows"] == {
        "extracted": 10,
        "inserted": 7,
        "updated": 2,
        "deleted": 0,
        "unchanged": 1,
    }
    assert md["atlatl/cursor"] == {"before": None, "after": {"watermark": "2026-09-01"}}
    assert md["atlatl/lake_snapshot"] == 42
    assert md["atlatl/duration_s"] == 1.5
    assert md["atlatl/attempts"] == 1
    [argv] = fake.calls()
    assert argv[argv.index("run") - 4 : argv.index("run")] == [
        "--project",
        "/fixture/project",
        "--log-format",
        "json",
    ]
    assert flag(argv, "--env") == ["dev"]
    assert flag(argv, "--replication") == ["orders", "refunds"]
    assert flag(argv, "--stream") == []
    assert flag(argv, "--external-run-id") == [result.run_id]
    assert flag(argv, "--events")[0].startswith("fd:")
    assert "--skip-succeeded" not in argv


def test_subset_maps_keys_to_streams(fake: Fake, plan: AtlatlPlan) -> None:
    result, _ = run([sales_op(plan)], selection=[ORDERS])
    assert result.success
    assert set(materialized(result)) == {ORDERS}
    [argv] = fake.calls()
    assert flag(argv, "--replication") == ["orders"]
    assert flag(argv, "--stream") == ["sales/orders"]

    result, _ = run([sales_op(plan)], selection=[ORDERS, REFUNDS])
    assert set(materialized(result)) == {ORDERS, REFUNDS}
    argv = fake.calls()[1]
    assert flag(argv, "--replication") == ["orders", "refunds"]
    assert flag(argv, "--stream") == ["sales/orders", "sales/refunds"]


def test_success_is_streamed_before_a_later_failure(fake: Fake, plan: AtlatlPlan) -> None:
    gate = fake.root / "gate"
    fake.scenario(
        gate=str(gate),
        replications={
            "refunds": {
                "status": "failed",
                "error": {"class": "auth", "code": "http_401", "message": "bad token"},
            }
        },
    )
    seen: list[dg.AssetKey] = []

    @atlatl_assets(project=plan, replication=["orders", "refunds"])
    def sales(context: dg.AssetExecutionContext, atlatl: AtlatlResource) -> Iterator[Any]:
        for r in atlatl.run(context):
            if isinstance(r, dg.MaterializeResult) and r.asset_key is not None:
                seen.append(dg.AssetKey.from_coercible(r.asset_key))
                gate.touch()
            yield r

    result, _ = run([sales], resources={"atlatl": AtlatlResource()})
    assert not result.success
    assert seen == [ORDERS, LINES]
    assert set(materialized(result)) == {ORDERS, LINES}
    msg = step_failure(result)
    assert "1 atlatl replication(s) failed" in msg
    assert "refunds (failed): auth [http_401]: bad token" in msg


def test_multi_hop_partial_success(fake: Fake, plan: AtlatlPlan) -> None:
    fake.scenario(
        replications={
            "orders": {
                "status": "failed",
                "error": {"class": "transient", "message": "serve hop failed"},
                "hops": [
                    {"target": "lake", "status": "committed", "lake_snapshot": 7},
                    {"target": "pg", "status": "failed", "lake_snapshot": None},
                ],
                "streams": {"sales/orders": "succeeded", "sales/lines": "failed"},
            }
        }
    )
    result, _ = run([sales_op(plan)], selection=[ORDERS, LINES])
    assert not result.success
    assert set(materialized(result)) == {ORDERS}
    assert "orders (failed): transient: serve hop failed" in step_failure(result)


@pytest.mark.parametrize(
    ("exit_code", "expected", "materializes"),
    [
        (2, "atlatl config error (exit 2): config: unknown connection `nope`", set[dg.AssetKey]()),
        (3, "atlatl internal error (exit 3): internal [panic]: boom", {ORDERS, LINES, REFUNDS}),
    ],
)
def test_config_and_internal_errors(
    fake: Fake,
    plan: AtlatlPlan,
    exit_code: int,
    expected: str,
    materializes: set[dg.AssetKey],
) -> None:
    fake.scenario(exit=exit_code)
    result, _ = run([sales_op(plan)])
    assert not result.success
    assert expected in step_failure(result)
    assert set(materialized(result)) == materializes


def test_missing_report(fake: Fake, plan: AtlatlPlan) -> None:
    fake.scenario(exit=2, no_report=True)
    result, _ = run([sales_op(plan)])
    msg = step_failure(result)
    assert "atlatl config error (exit 2): config error: unknown connection `nope`" in msg


def test_events_and_stderr_reach_the_log(fake: Fake, plan: AtlatlPlan) -> None:
    fake.scenario(
        stderr=["hello from atlatl"],
        warnings=["plan warning"],
        replications={
            "orders": {"retry": True},
            "refunds": {"status": "failed", "error": {"class": "data", "message": "bad rows"}},
        },
    )
    result, instance = run([sales_op(plan)])
    assert not result.success
    logs = [(e.level, e.user_message) for e in instance.all_logs(result.run_id)]

    def at(level: int, needle: str) -> bool:
        return any(lv == level and needle in m for lv, m in logs)

    assert at(logging.INFO, "hello from atlatl")
    assert at(logging.INFO, "atlatl run 01FAKE started")
    assert at(logging.INFO, "replication orders started: sales/orders, sales/lines")
    assert at(logging.DEBUG, "orders/sales/orders extract: 10 rows")
    assert at(logging.WARNING, "retrying request (attempt 1) in 10ms after transient")
    assert at(logging.INFO, "replication orders succeeded")
    assert at(logging.ERROR, "replication refunds failed in 2s: data: bad rows")
    assert at(logging.WARNING, "[fake] plan warning")
    assert at(logging.WARNING, "atlatl run partial_failure (exit 1)")


def test_retry_passes_skip_succeeded(fake: Fake, plan: AtlatlPlan) -> None:
    fake.scenario(
        attempts=[
            {"replications": {"refunds": {"status": "failed", "error": {"class": "transient"}}}},
            {},
        ]
    )
    op = sales_op(plan, retry_policy=dg.RetryPolicy(max_retries=1))
    result, _ = run([op])
    assert result.success
    first, second = fake.calls()
    assert "--skip-succeeded" not in first
    assert "--skip-succeeded" in second
    assert flag(first, "--external-run-id") == flag(second, "--external-run-id") == [result.run_id]
    keys = [e.asset_key for e in result.get_asset_materialization_events()]
    assert sorted(k.to_user_string() for k in keys if k) == [
        "br/sales/orders",
        "sales/lines",
        "sales/refunds",
    ]


def test_vars_are_passed(fake: Fake, plan: AtlatlPlan) -> None:
    @atlatl_assets(project=plan, replication="refunds", name="refunds_load")
    def refunds(context: dg.AssetExecutionContext, atlatl: AtlatlResource) -> Iterator[Any]:
        yield from atlatl.run(context, vars={"bulk_dir": "/tmp/bulk"})

    result, _ = run([refunds], resources={"atlatl": AtlatlResource(env="prod")})
    assert result.success
    [argv] = fake.calls()
    assert flag(argv, "--var") == ["bulk_dir=/tmp/bulk"]
    assert flag(argv, "--env") == ["prod"]


def hr_op(plan: AtlatlPlan) -> dg.AssetsDefinition:
    return {a.node_def.name: a for a in build_atlatl_assets(plan)}["hr"]


def check(result: dg.ExecuteInProcessResult) -> dg.AssetCheckEvaluation:
    [ev] = result.get_asset_check_evaluations()
    return ev


def schema(**kw: Any) -> dict[str, Any]:
    return {
        "status": "ok",
        "mode": "enforce",
        "severity": None,
        "new_fields": [],
        "missing_fields": [],
        "violations": [],
        **kw,
    }


def test_schema_check_passes(fake: Fake, plan: AtlatlPlan) -> None:
    fake.scenario(replications={"people": {"schema": schema()}})
    result, _ = run([hr_op(plan)])
    assert result.success
    ev = check(result)
    assert ev.check_name == SCHEMA_CHECK
    assert ev.passed


def test_schema_check_warns(fake: Fake, plan: AtlatlPlan) -> None:
    drift = schema(status="violated", severity="warn", new_fields=["email"])
    fake.scenario(replications={"people": {"schema": drift}})
    result, _ = run([hr_op(plan)])
    assert result.success
    ev = check(result)
    assert not ev.passed
    assert ev.severity == dg.AssetCheckSeverity.WARN
    assert ev.description == "src.people: new fields email"
    assert set(materialized(result)) == {PEOPLE}


def test_schema_check_errors_on_a_failed_replication(fake: Fake, plan: AtlatlPlan) -> None:
    violation = {"field": "id", "kind": "type", "count": 3, "samples": ["x"]}
    bad = schema(status="violated", severity="error", violations=[violation])
    fake.scenario(
        replications={
            "people": {
                "status": "failed",
                "schema": bad,
                "error": {"class": "data", "code": "schema_violation", "message": "id"},
            }
        }
    )
    result, _ = run([hr_op(plan)])
    assert not result.success
    ev = check(result)
    assert not ev.passed
    assert ev.severity == dg.AssetCheckSeverity.ERROR
    assert ev.description == "src.people: type violations on id (3)"
    assert materialized(result) == {}


def spawn(fake: Fake, grace: float) -> tuple[AtlatlProcess, Generator[Item, None, None]]:
    report = fake.root / "report.json"
    proc = AtlatlProcess(
        [str(fake.bin), "run", "--events", "fd:{events_fd}", "--report", str(report)],
        terminate_grace_s=grace,
    )
    return proc, proc.__iter__()


def wait_for(path: Path) -> None:
    deadline = time.monotonic() + 10
    while not path.exists():
        assert time.monotonic() < deadline, f"{path} never appeared"
        time.sleep(0.02)


def first_event(items: Iterator[Item]) -> Event:
    for item in items:
        if isinstance(item, Event):
            return item
    raise AssertionError("no event")


def test_close_sends_sigterm(fake: Fake) -> None:
    fake.scenario(hang=True)
    proc, items = spawn(fake, grace=5)
    assert first_event(items).body["type"] == "run.started"
    wait_for(fake.hung)
    items.close()
    assert fake.signals() == ["TERM"]
    assert proc.returncode == 143


def test_sigkill_after_grace(fake: Fake) -> None:
    fake.scenario(hang=True, ignore_term=True)
    proc, items = spawn(fake, grace=0.3)
    first_event(items)
    wait_for(fake.hung)
    t = time.monotonic()
    items.close()
    assert 0.3 <= time.monotonic() - t < 5
    assert fake.signals() == ["TERM"]
    assert proc.returncode == -9


def test_interrupt_during_materialize_kills_the_child(fake: Fake, plan: AtlatlPlan) -> None:
    fake.scenario(hang=True, ignore_term=True)
    op = sales_op(plan, resource=AtlatlResource(terminate_grace_s=0.2))

    def interrupt() -> None:
        wait_for(fake.hung)
        _thread.interrupt_main()

    threading.Thread(target=interrupt, daemon=True).start()
    with pytest.raises((KeyboardInterrupt, dg.DagsterExecutionInterruptedError)):
        dg.materialize([op], instance=ephemeral())
    assert fake.signals() == ["TERM"]
    assert not Path(fake.root / "report.json").exists()


def test_serving_hop_assets_use_their_hop_target(fake: Fake, shapes_plan: AtlatlPlan) -> None:
    result, _ = run([sales_op(shapes_plan)], selection=[ORDERS, LINES])
    assert result.success
    got = materialized(result)
    assert got[LINES]["dagster/storage_kind"] == "postgres"
    assert got[ORDERS]["dagster/storage_kind"] == "ducklake"


def test_for_each_instances_materialize_each_asset_once(
    fake: Fake, shapes_plan: AtlatlPlan
) -> None:
    fake.scenario(instances={"refunds": ["a", "b"]})
    result, _ = run([sales_op(shapes_plan)], selection=[REFUNDS])
    assert result.success
    [argv] = fake.calls()
    assert flag(argv, "--replication") == ["refunds"]
    assert len(result.get_asset_materialization_events()) == 1
    meta = materialized(result)[REFUNDS]
    assert meta["dagster/row_count"] == 18
    assert meta["atlatl/instances"] == {"a": "succeeded", "b": "succeeded"}
    members: list[dict[str, Any]] = meta["atlatl/members"]
    assert [m["member"] for m in members] == ["a/src.refunds", "b/src.refunds"]


def test_a_failed_for_each_instance_fails_the_step_after_the_rest_materialize(
    fake: Fake, shapes_plan: AtlatlPlan
) -> None:
    fake.scenario(
        instances={"refunds": ["a", "b"]},
        replications={
            "refunds[b]": {"status": "failed", "error": {"class": "data", "message": "dupes"}}
        },
    )
    result, _ = run([sales_op(shapes_plan)], selection=[REFUNDS])
    assert not result.success
    md = materialized(result)[REFUNDS]
    assert md["dagster/row_count"] == 9
    assert md["atlatl/instances"] == {"a": "succeeded", "b": "rolled_back"}
    assert "refunds[b] (failed): data: dupes" in step_failure(result)
