"""Report entries (spec 10 §4) → Dagster metadata, check results and log lines."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

from dagster import AssetCheckResult, AssetCheckSeverity, AssetKey, MetadataValue

Json = Mapping[str, Any]

SCHEMA_CHECK = "atlatl_schema"
SUCCESS = frozenset({"succeeded", "up_to_date"})
STREAM_COMMITTED = frozenset({"succeeded", "up_to_date"})
_ROW_FIELDS = ("extracted", "inserted", "updated", "deleted", "unchanged")


def obj(v: Any) -> Json:
    return cast(Json, v) if isinstance(v, Mapping) else {}


def objs(v: Any) -> list[Json]:
    return [obj(x) for x in cast(list[Any], v)] if isinstance(v, list) else []


def strs(v: Any) -> list[str]:
    return [str(x) for x in cast(list[Any], v)] if isinstance(v, list) else []


def _int(v: Any) -> int:
    try:
        return int(v or 0)
    except (TypeError, ValueError):
        return 0


def _rows(members: Sequence[Json]) -> dict[str, int]:
    out: dict[str, int] = dict.fromkeys(_ROW_FIELDS, 0)
    for m in members:
        rows = obj(m.get("rows"))
        for k in _ROW_FIELDS:
            out[k] += _int(rows.get(k))
    return out


def _table_name(o: str, target: str) -> str:
    return o if o.count(".") >= 2 else f"{target}.{o}"


def stream_metadata(
    replication: Json, stream: Json, *, target: str, target_type: str
) -> dict[str, Any]:
    """The materialization metadata of spec 09 §4."""
    members = objs(stream.get("members"))
    rows = _rows(members)
    written = rows["inserted"] + rows["updated"] + rows["deleted"]
    md: dict[str, Any] = {
        "dagster/row_count": MetadataValue.int(written),
        "dagster/storage_kind": MetadataValue.text(target_type),
        "atlatl/rows": MetadataValue.json(rows),
        "atlatl/duration_s": MetadataValue.float(_int(stream.get("duration_ms")) / 1000),
        "atlatl/attempts": MetadataValue.int(_int(replication.get("attempts"))),
    }
    objects = [str(m["object"]) for m in members if m.get("object")]
    if len(members) == 1 and objects:
        md["dagster/table_name"] = MetadataValue.text(_table_name(objects[0], target))
    if len(members) > 1:
        md["atlatl/members"] = MetadataValue.json(
            [
                {
                    "member": m.get("member"),
                    "table_name": _table_name(str(m["object"]), target)
                    if m.get("object")
                    else None,
                    "rows": _rows([m]),
                    "duration_s": _int(m.get("duration_ms")) / 1000,
                    "lake_snapshot": m.get("lake_snapshot"),
                }
                for m in members
            ]
        )
    if members:
        states = [obj(m.get("state")) for m in members]
        cursor: Any = (
            {"before": states[0].get("before"), "after": states[0].get("after")}
            if len(members) == 1
            else {
                str(m.get("member")): {"before": st.get("before"), "after": st.get("after")}
                for m, st in zip(members, states, strict=True)
            }
        )
        md["atlatl/cursor"] = MetadataValue.json(cursor)
    snapshot = _lake_snapshot(replication, members)
    if snapshot is not None:
        md["atlatl/lake_snapshot"] = MetadataValue.int(snapshot)
    cdc = next((obj(m["cdc"]) for m in members if m.get("cdc")), None)
    if cdc:
        window = obj(cdc.get("window"))
        md["atlatl/cdc"] = MetadataValue.json(
            {
                "from_snapshot": window.get("from_snapshot"),
                "to_snapshot": window.get("to_snapshot"),
                "snapshots_behind": cdc.get("snapshots_behind"),
                "seconds_behind": cdc.get("seconds_behind"),
            }
        )
    rejects = {
        k: sum(_int(obj(m.get("rejects")).get(k)) for m in members)
        for k in ("new", "resolved", "expired", "open")
    }
    if any(rejects.values()):
        md["atlatl/rejects"] = MetadataValue.json(rejects)
    http = [obj(m.get("http")) for m in members if m.get("http")]
    if http:
        wait = sum(_int(h.get("throttle_wait_ms")) for h in http)
        md["atlatl/throttle_wait_s"] = MetadataValue.float(wait / 1000)
    return md


def _lake_snapshot(replication: Json, members: Sequence[Json]) -> int | None:
    snaps = [_int(m["lake_snapshot"]) for m in members if m.get("lake_snapshot") is not None]
    if snaps:
        return max(snaps)
    for h in objs(replication.get("hops")):
        if h.get("status") == "committed" and h.get("lake_snapshot") is not None:
            return _int(h["lake_snapshot"])
    return None


def schema_check(key: AssetKey, stream: Json) -> AssetCheckResult | None:
    """`atlatl_schema` from the members' `schema` sections; `None` when none was evaluated."""
    reports = [
        (str(m.get("member")), obj(m["schema"]))
        for m in objs(stream.get("members"))
        if m.get("schema")
    ]
    if not reports:
        return None
    details = [
        {
            "member": n,
            "status": s.get("status"),
            "mode": s.get("mode"),
            "severity": s.get("severity"),
            "new_fields": strs(s.get("new_fields")),
            "missing_fields": strs(s.get("missing_fields")),
            "violations": objs(s.get("violations")),
        }
        for n, s in reports
    ]
    md: dict[str, Any] = {"atlatl/schema": MetadataValue.json(details)}
    violated = [(n, s) for n, s in reports if s.get("status") == "violated" or s.get("severity")]
    if not violated:
        return AssetCheckResult(asset_key=key, check_name=SCHEMA_CHECK, passed=True, metadata=md)
    error = any(s.get("severity") == "error" for _, s in violated)
    summary: list[str] = []
    for n, s in violated:
        parts: list[str] = []
        if new := strs(s.get("new_fields")):
            parts.append(f"new fields {', '.join(new)}")
        if missing := strs(s.get("missing_fields")):
            parts.append(f"missing fields {', '.join(missing)}")
        parts.extend(
            f"{v.get('kind')} violations on {v.get('field')} ({v.get('count')})"
            for v in objs(s.get("violations"))
        )
        summary.append(f"{n}: {'; '.join(parts) or 'violated'}")
    return AssetCheckResult(
        asset_key=key,
        check_name=SCHEMA_CHECK,
        passed=False,
        severity=AssetCheckSeverity.ERROR if error else AssetCheckSeverity.WARN,
        description="\n".join(summary),
        metadata=md,
    )


def describe_error(err: Json | None) -> str:
    if not err:
        return "no error recorded"
    code = f" [{err['code']}]" if err.get("code") else ""
    return f"{err.get('class') or 'unknown'}{code}: {err.get('message') or ''}"


def _where(*parts: Any) -> str:
    return "/".join(str(x) for x in parts if x)


def describe_event(ev: Json) -> tuple[str, str]:
    """(level, message) for `context.log`."""
    t = str(ev.get("type", "?"))
    rep = ev.get("replication")
    if t == "run.started":
        return "info", (
            f"atlatl run {ev.get('run_id')} started (env={ev.get('env')}, "
            f"atlatl {ev.get('atlatl_version')}, plan {ev.get('plan_hash')})"
        )
    if t == "replication.started":
        return "info", f"replication {rep} started: {', '.join(strs(ev.get('streams')))}"
    if t == "stream.progress":
        return "debug", (
            f"{_where(rep, ev.get('stream_id'), ev.get('member'))} {ev.get('phase')}: "
            f"{ev.get('rows_so_far')} rows, chunk {ev.get('chunk')}"
        )
    if t == "retrying":
        return "warning", (
            f"{_where(rep, ev.get('stream_id'), ev.get('member'))}: retrying {ev.get('level')} "
            f"(attempt {ev.get('attempt')}) in {ev.get('wait_ms')}ms after "
            f"{ev.get('error_class')}: {ev.get('message')}"
        )
    if t == "hop.committed":
        snap = ev.get("lake_snapshot")
        suffix = f", lake snapshot {snap}" if snap is not None else ""
        return "info", f"{rep}: hop {ev.get('hop')} ({ev.get('target')}) committed{suffix}"
    if t == "replication.completed":
        status = ev.get("status")
        msg = f"replication {rep} {status} in {_int(ev.get('duration_ms')) / 1000:g}s"
        if status in SUCCESS or status == "skipped":
            return "info", msg
        return "error", f"{msg}: {describe_error(obj(ev.get('error')))}"
    if t == "throttle":
        return "warning", (
            f"throttled on {ev.get('connection')} for {ev.get('wait_ms')}ms: {ev.get('reason')}"
        )
    if t == "schema.drift":
        return "warning", (
            f"{_where(rep, ev.get('stream_id'), ev.get('member'))}: schema drift "
            f"({ev.get('mode')}): new {strs(ev.get('new_fields'))}, "
            f"missing {strs(ev.get('missing_fields'))}, "
            f"{len(objs(ev.get('violations')))} violated fields"
        )
    if t == "warning":
        where = _where(rep, ev.get("stream_id"))
        return "warning", f"{where + ': ' if where else ''}[{ev.get('code')}] {ev.get('message')}"
    if t == "run.completed":
        level = "info" if ev.get("status") == "succeeded" else "warning"
        return level, (
            f"atlatl run {ev.get('status')} (exit {ev.get('exit_code')}): "
            f"{dict(obj(ev.get('counts')))}"
        )
    return "debug", f"atlatl event {t}: {dict(ev)}"
