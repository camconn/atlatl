"""Assertions over the static plan (spec 09 §7)."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import cast

from dagster_atlatl._plan import AtlatlPlan, load_plan

_FIELDS = ("op", "group", "replication")


def asset_index(plan: AtlatlPlan) -> dict[str, dict[str, str | None]]:
    """Asset key → `{op, group, replication}` for every non-feed-only stream."""
    return {
        s.asset.key: {"op": r.op, "group": s.asset.group, "replication": r.name}
        for r in plan.replications
        for s in r.streams
        if not s.feed_only
    }


def assert_asset_keys(
    plan: AtlatlPlan | str | Path,
    expected: Iterable[str] | Mapping[str, Mapping[str, str | None]],
) -> None:
    """The plan's asset keys are exactly `expected`.

    With a mapping, each value may also pin `op`, `group` and `replication`.
    """
    p = plan if isinstance(plan, AtlatlPlan) else load_plan(plan)
    got = asset_index(p)
    want: dict[str, Mapping[str, str | None]] = (
        dict(cast(Mapping[str, Mapping[str, str | None]], expected))
        if isinstance(expected, Mapping)
        else {k: {} for k in expected}
    )
    problems: list[str] = []
    problems += [f"missing: {k}" for k in sorted(set(want) - set(got))]
    problems += [f"unexpected: {k}" for k in sorted(set(got) - set(want))]
    for k in sorted(set(want) & set(got)):
        for f, v in want[k].items():
            if f not in _FIELDS:
                problems.append(f"{k}: unknown field {f!r} (expected one of {', '.join(_FIELDS)})")
            elif got[k][f] != v:
                problems.append(f"{k}: {f} is {got[k][f]!r}, expected {v!r}")
    if problems:
        raise AssertionError("atlatl asset keys differ:\n  " + "\n  ".join(problems))
