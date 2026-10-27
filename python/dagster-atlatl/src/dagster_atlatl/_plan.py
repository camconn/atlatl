"""The static plan (`atlatl plan --static --json`) as seen by Dagster."""

from __future__ import annotations

import fnmatch
import json
import subprocess
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dagster_atlatl._binary import binary_path

PLAN_TIMEOUT_S = 120


class AtlatlPlanError(RuntimeError):
    """`atlatl plan` failed, or its output (or a selection over it) is unusable."""


@dataclass(frozen=True)
class Connection:
    name: str
    type: str


@dataclass(frozen=True)
class AssetInfo:
    key: str
    key_path: tuple[str, ...]
    group: str | None
    deps: tuple[str, ...]
    description: str | None
    kinds: tuple[str, ...]
    tags: Mapping[str, str]
    code_version: str


@dataclass(frozen=True)
class StreamInfo:
    id: str
    role: str
    asset: AssetInfo
    source_schema: str | None
    hop: int = 0

    @property
    def feed_only(self) -> bool:
        return self.role == "feed_only"


@dataclass(frozen=True)
class HopInfo:
    """One hop of a multi-hop (`then:`) replication; hop 0 is the replication itself."""

    hop: int
    parent: int | None
    source: Connection
    target: Connection


@dataclass(frozen=True)
class ReplicationInfo:
    name: str
    op: str
    file: str
    source: Connection
    target: Connection
    streams: tuple[StreamInfo, ...]
    hops: tuple[HopInfo, ...] = ()
    for_each: bool = False
    """Runs as independent instances, reported as `<name>[<key>]` entries (11 §3 (c))."""

    def target_of(self, stream: StreamInfo) -> Connection:
        """The connection a stream writes: its hop's target."""
        return self.hops[stream.hop].target if stream.hop < len(self.hops) else self.target


@dataclass(frozen=True)
class AtlatlPlan:
    project_root: Path
    plan_hash: str
    replications: tuple[ReplicationInfo, ...]
    warnings: tuple[str, ...] = ()
    raw: Mapping[str, Any] = field(default_factory=dict[str, Any], repr=False, compare=False)

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> AtlatlPlan:
        try:
            return cls(
                project_root=Path(data["project_root"]),
                plan_hash=str(data["plan_hash"]),
                replications=tuple(_replication(r) for r in data["replications"]),
                warnings=tuple(f"{w['code']}: {w['message']}" for w in data.get("warnings", [])),
                raw=data,
            )
        except (KeyError, TypeError, ValueError) as e:
            raise AtlatlPlanError(f"unexpected `atlatl plan --json` output: {e!r}") from e

    def ops(self) -> dict[str, list[ReplicationInfo]]:
        """Replications grouped by op, in plan order."""
        out: dict[str, list[ReplicationInfo]] = {}
        for r in self.replications:
            out.setdefault(r.op, []).append(r)
        return out

    def select(self, patterns: Iterable[str]) -> AtlatlPlan:
        """Replications whose name matches any glob. Every pattern must match something."""
        pats = list(patterns)
        names = [r.name for r in self.replications]
        unmatched = [p for p in pats if not any(fnmatch.fnmatchcase(n, p) for n in names)]
        if unmatched:
            raise AtlatlPlanError(
                f"replication patterns matched nothing: {', '.join(unmatched)} "
                f"(replications: {', '.join(names) or 'none'})"
            )
        keep = tuple(
            r for r in self.replications if any(fnmatch.fnmatchcase(r.name, p) for p in pats)
        )
        return AtlatlPlan(self.project_root, self.plan_hash, keep, self.warnings, self.raw)


def _conn(c: Mapping[str, Any]) -> Connection:
    return Connection(c["name"], c["type"])


def _replication(r: Mapping[str, Any]) -> ReplicationInfo:
    return ReplicationInfo(
        name=r["name"],
        op=r["op"],
        file=r.get("file", ""),
        source=_conn(r["source"]),
        target=_conn(r["target"]),
        streams=tuple(_stream(s) for s in r["streams"]),
        hops=tuple(
            HopInfo(int(h["hop"]), h.get("parent"), _conn(h["source"]), _conn(h["target"]))
            for h in r.get("hops", [])
        ),
        for_each=r.get("for_each") is not None,
    )


def _stream(s: Mapping[str, Any]) -> StreamInfo:
    a = s["asset"]
    return StreamInfo(
        id=s["id"],
        role=s.get("role", "selected"),
        asset=AssetInfo(
            key=a["key"],
            key_path=tuple(a["key_path"]),
            group=a.get("group"),
            deps=tuple(a.get("deps", [])),
            description=a.get("description"),
            kinds=tuple(a.get("kinds", [])),
            tags=dict(a.get("tags", {})),
            code_version=a["code_version"],
        ),
        source_schema=s.get("source_schema"),
        hop=int(s.get("hop", 0)),
    )


def load_plan(project: str | Path, *, binary: str | Path | None = None) -> AtlatlPlan:
    """Runs `atlatl plan --static --json`: config only, no environment, no secrets."""
    exe = Path(binary) if binary else binary_path()
    cmd = [
        str(exe),
        "--project",
        str(project),
        "--log-format",
        "json",
        "plan",
        "--static",
        "--json",
    ]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=PLAN_TIMEOUT_S, check=False)
    except (OSError, subprocess.SubprocessError) as e:
        raise AtlatlPlanError(f"running `{' '.join(cmd)}` failed: {e}") from e
    if p.returncode != 0:
        kind = {2: "config error", 3: "internal error"}.get(p.returncode, f"exit {p.returncode}")
        raise AtlatlPlanError(
            f"`atlatl plan --static` for {project} failed ({kind}):\n{p.stderr.strip()}"
        )
    try:
        data = json.loads(p.stdout)
    except json.JSONDecodeError as e:
        raise AtlatlPlanError(f"`atlatl plan --json` printed invalid JSON: {e}") from e
    return AtlatlPlan.from_json(data)
