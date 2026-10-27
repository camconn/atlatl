from __future__ import annotations

import json
from typing import Any

import dagster as dg

from dagster_atlatl._process import LogLine, parse_log_line
from dagster_atlatl._results import describe_event, schema_check, stream_metadata


def values(md: dict[str, Any]) -> dict[str, Any]:
    return {k: v.value for k, v in md.items()}


def member(name: str, obj: str | None, **kw: Any) -> dict[str, Any]:
    return {
        "member": name,
        "object": obj,
        "duration_ms": 250,
        "rows": {"extracted": 5, "inserted": 3, "updated": 1, "deleted": 1, "unchanged": 0},
        "state": {"before": 1, "after": 2},
        "lake_snapshot": None,
        **kw,
    }


def test_wildcard_stream_lists_members() -> None:
    stream = {
        "stream_id": "fs/basic",
        "duration_ms": 3000,
        "members": [
            member("est.a", "catalog.br.a", lake_snapshot=10),
            member("est.b", "br.b", lake_snapshot=11),
        ],
    }
    md = values(stream_metadata({"attempts": 2}, stream, target="lake", target_type="ducklake"))
    assert "dagster/table_name" not in md
    assert md["dagster/row_count"] == 10
    assert md["atlatl/lake_snapshot"] == 11
    assert md["atlatl/attempts"] == 2
    assert md["atlatl/cursor"] == {
        "est.a": {"before": 1, "after": 2},
        "est.b": {"before": 1, "after": 2},
    }
    assert [m["table_name"] for m in md["atlatl/members"]] == ["catalog.br.a", "lake.br.b"]
    assert md["atlatl/members"][0]["duration_s"] == 0.25


def test_cdc_http_and_hop_snapshot() -> None:
    cdc = {
        "window": {"from_snapshot": 3, "to_snapshot": 9},
        "snapshots_behind": 0,
        "seconds_behind": 1.5,
        "touched_keys": 4,
    }
    stream = {"members": [member("m", "dagster.t", cdc=cdc, http={"throttle_wait_ms": 1500})]}
    rep = {"hops": [{"status": "committed", "lake_snapshot": 77}]}
    md = values(stream_metadata(rep, stream, target="pg", target_type="postgres"))
    assert md["dagster/table_name"] == "pg.dagster.t"
    assert md["atlatl/cdc"] == {
        "from_snapshot": 3,
        "to_snapshot": 9,
        "snapshots_behind": 0,
        "seconds_behind": 1.5,
    }
    assert md["atlatl/throttle_wait_s"] == 1.5
    assert md["atlatl/lake_snapshot"] == 77


def test_rejects_show_when_up_to_date() -> None:
    quiet = {"extracted": 2, "inserted": 0, "updated": 0, "deleted": 0, "unchanged": 2}
    stream = {
        "status": "up_to_date",
        "members": [
            member("m", "br.feed", rows=quiet, rejects={"new": 0, "resolved": 0, "open": 1}),
            member("n", "br.feed2", rows=quiet, rejects={"new": 0, "expired": 2, "open": 0}),
        ],
    }
    md = values(stream_metadata({}, stream, target="lake", target_type="ducklake"))
    assert md["dagster/row_count"] == 0
    assert md["atlatl/rejects"] == {"new": 0, "resolved": 0, "expired": 2, "open": 1}
    stream["members"] = [member("m", "br.feed", rejects={"new": 0, "resolved": 0, "open": 0})]
    md = values(stream_metadata({}, stream, target="lake", target_type="ducklake"))
    assert "atlatl/rejects" not in md


def test_schema_check_without_reports() -> None:
    assert schema_check(dg.AssetKey("a"), {"members": [member("m", None)]}) is None


def test_log_lines() -> None:
    line = {"level": "WARN", "fields": {"message": "slow", "replication": "r"}, "target": "x"}
    assert parse_log_line(json.dumps(line)) == LogLine("warning", "slow replication=r")
    assert parse_log_line(json.dumps({"level": "TRACE", "fields": {}})).level == "debug"
    assert parse_log_line("plain text") == LogLine("info", "plain text")
    assert parse_log_line("[1, 2]") == LogLine("info", "[1, 2]")


def test_unknown_events_are_debug() -> None:
    assert describe_event({"type": "future.thing", "x": 1})[0] == "debug"
    level, msg = describe_event({"type": "throttle", "connection": "api", "wait_ms": 5})
    assert level == "warning"
    assert "throttled on api for 5ms" in msg
