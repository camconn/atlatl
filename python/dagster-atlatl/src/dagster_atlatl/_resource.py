"""`AtlatlResource`: selection → `atlatl run` → streamed materializations (spec 09 §3)."""

from __future__ import annotations

import json
import os
import tempfile
from collections import deque
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from dagster import (
    AssetCheckResult,
    AssetExecutionContext,
    AssetKey,
    ConfigurableResource,
    Failure,
    MaterializeResult,
    MetadataValue,
)

from dagster_atlatl._binary import binary_path
from dagster_atlatl._process import EVENTS_FD_PLACEHOLDER, AtlatlProcess, Event
from dagster_atlatl._results import (
    SCHEMA_CHECK,
    STREAM_COMMITTED,
    SUCCESS,
    Json,
    describe_error,
    describe_event,
    obj,
    objs,
    schema_check,
    stream_metadata,
)

MD_PROJECT = "atlatl/project"
MD_REPLICATION = "atlatl/replication"
MD_STREAM = "atlatl/stream_id"
MD_TARGET = "atlatl/target"
MD_TARGET_TYPE = "atlatl/target_type"

EXIT_CONFIG = 2
EXIT_INTERNAL = 3
_TAIL = 20

AtlatlResult = MaterializeResult[Any] | AssetCheckResult


class AtlatlResource(ConfigurableResource):  # pyright: ignore[reportMissingTypeArgument]
    """Runs atlatl for the selected assets of an atlatl multi-asset.

    `project` defaults to the project the assets were built from. The environment is `env`,
    else the value of the `environment_from` variable, else atlatl's own default.
    """

    project: str | None = None
    env: str | None = None
    environment_from: str | None = None
    terminate_grace_s: float = 5.0
    binary: str | None = None

    def resolved_env(self) -> str | None:
        if self.env:
            return self.env
        if self.environment_from:
            return os.environ.get(self.environment_from) or None
        return None

    def run(
        self, context: AssetExecutionContext, *, vars: Mapping[str, str] | None = None
    ) -> Iterator[AtlatlResult]:
        """Yields a `MaterializeResult` per stream as each replication completes, then raises
        `dagster.Failure` if any selected replication failed."""
        return _Run(context, self, dict(vars or {})).execute()


def _md(spec_md: Mapping[str, Any], key: str) -> str | None:
    v: Any = spec_md.get(key)
    if isinstance(v, MetadataValue):
        v = cast(Any, v).value
    return None if v is None else str(v)


@dataclass
class _Stream:
    key: AssetKey
    replication: str
    target: str
    target_type: str
    check: bool


@dataclass
class _Run:
    context: AssetExecutionContext
    res: AtlatlResource
    vars: dict[str, str]
    streams: dict[str, _Stream] = field(default_factory=dict[str, _Stream])
    replications: list[str] = field(default_factory=list[str])
    done: set[str] = field(default_factory=set[str])
    """Report entries handled (replications, or `for_each` instances `<name>[<key>]`)."""
    seen: set[str] = field(default_factory=set[str])
    """Selected replications with at least one report entry."""
    instances: dict[str, list[Json]] = field(default_factory=dict[str, list[Json]])
    """`for_each` instance entries by replication, materialized once the run is over."""
    failures: list[dict[str, Any]] = field(default_factory=list[dict[str, Any]])
    tail: deque[str] = field(default_factory=lambda: deque[str](maxlen=_TAIL))

    def selection(self) -> tuple[str, list[str], list[str]]:
        """(project, replications, stream ids); stream ids are empty when nothing is subset."""
        ad = self.context.assets_def
        selected = self.context.selected_asset_keys
        checks = {
            k.asset_key for k in self.context.selected_asset_check_keys if k.name == SCHEMA_CHECK
        }
        all_by_rep: dict[str, set[str]] = {}
        projects: set[str] = set()
        for key, spec in ad.specs_by_key.items():
            rep, sid = _md(spec.metadata, MD_REPLICATION), _md(spec.metadata, MD_STREAM)
            if rep is None or sid is None:
                raise Failure(f"asset {key.to_user_string()} was not built by dagster-atlatl")
            all_by_rep.setdefault(rep, set()).add(sid)
            if (p := _md(spec.metadata, MD_PROJECT)) is not None:
                projects.add(p)
            if key in selected:
                self.streams[sid] = _Stream(
                    key=key,
                    replication=rep,
                    target=_md(spec.metadata, MD_TARGET) or "",
                    target_type=_md(spec.metadata, MD_TARGET_TYPE) or "",
                    check=key in checks,
                )
                if rep not in self.replications:
                    self.replications.append(rep)
        project = self.res.project
        if project is None:
            if len(projects) != 1:
                raise Failure(
                    "AtlatlResource.project is unset and the assets name no single project"
                )
            project = projects.pop()
        subset = any(
            {sid for sid, s in self.streams.items() if s.replication == r} != all_by_rep[r]
            for r in self.replications
        )
        return project, self.replications, sorted(self.streams) if subset else []

    def argv(self, project: str, reps: list[str], streams: list[str], report: Path) -> list[str]:
        exe = self.res.binary or str(binary_path())
        argv = [exe, "--project", project, "--log-format", "json", "run"]
        if env := self.res.resolved_env():
            argv += ["--env", env]
        for r in reps:
            argv += ["--replication", r]
        for s in streams:
            argv += ["--stream", s]
        argv += [
            "--events",
            f"fd:{EVENTS_FD_PLACEHOLDER}",
            "--report",
            str(report),
            "--external-run-id",
            self.context.run.run_id,
        ]
        if self.context.retry_number > 0:
            argv.append("--skip-succeeded")
        for k, v in sorted(self.vars.items()):
            argv += ["--var", f"{k}={v}"]
        return argv

    def execute(self) -> Iterator[AtlatlResult]:
        log = self.context.log
        project, reps, streams = self.selection()
        if not reps:
            return
        with tempfile.TemporaryDirectory(prefix="dagster-atlatl-") as tmp:
            report_path = Path(tmp) / "report.json"
            argv = self.argv(project, reps, streams, report_path)
            log.info(f"running {' '.join(argv)}")
            proc = AtlatlProcess(
                argv, terminate_grace_s=self.res.terminate_grace_s, on_terminate=log.warning
            )
            for item in proc:
                if isinstance(item, Event):
                    level, msg = describe_event(item.body)
                    getattr(log, level)(msg)
                    if item.body.get("type") == "replication.completed":
                        yield from self.completed(item.body)
                else:
                    self.tail.append(item.message)
                    getattr(log, item.level)(item.message)
            code = proc.returncode
            report = _read_report(report_path)
        yield from self.finish(code, report)

    def completed(self, rep: Json) -> Iterator[AtlatlResult]:
        entry = str(rep.get("replication"))
        instance = obj(rep.get("instance"))
        name = str(instance.get("of") or entry)
        if entry in self.done or name not in self.replications:
            return
        self.done.add(entry)
        self.seen.add(name)
        status = rep.get("status")
        if instance:
            self.instances.setdefault(name, []).append(rep)
        else:
            yield from self.materialize(rep, objs(rep.get("streams")))
        if status not in SUCCESS and status != "skipped":
            err = obj(rep.get("error"))
            self.failures.append(
                {
                    "replication": entry,
                    "status": status,
                    "error_class": err.get("class"),
                    "error": describe_error(err),
                }
            )

    def materialize(
        self, rep: Json, streams: list[Json], extra: Mapping[str, dict[str, Any]] | None = None
    ) -> Iterator[AtlatlResult]:
        for s in streams:
            sid = str(s.get("stream_id"))
            st = self.streams.get(sid)
            if st is None:
                continue
            check = schema_check(st.key, s) if st.check else None
            if s.get("status") in STREAM_COMMITTED:
                md = stream_metadata(rep, s, target=st.target, target_type=st.target_type)
                md.update((extra or {}).get(sid, {}))
                yield MaterializeResult(
                    asset_key=st.key, metadata=md, check_results=[check] if check else None
                )
            elif check is not None:
                yield check

    def instances_done(self) -> Iterator[AtlatlResult]:
        """One materialization per stream over the `for_each` instances that committed it; an
        asset materializes once per step, and the instance count is only known at the end."""
        for reps in self.instances.values():
            parts: dict[str, list[tuple[str, Json]]] = {}
            for r in reps:
                key = str(obj(r.get("instance")).get("key"))
                for s in objs(r.get("streams")):
                    parts.setdefault(str(s.get("stream_id")), []).append((key, s))
            merged: list[Json] = []
            extra: dict[str, dict[str, Any]] = {}
            for sid, ps in parts.items():
                committed = [(k, s) for k, s in ps if s.get("status") in STREAM_COMMITTED]
                merged.append(
                    {
                        "stream_id": sid,
                        "status": "succeeded" if committed else "rolled_back",
                        "duration_ms": sum(int(s.get("duration_ms") or 0) for _, s in ps),
                        "members": [
                            {**m, "member": f"{k}/{m.get('member')}"}
                            for k, s in (committed or ps)
                            for m in objs(s.get("members"))
                        ],
                    }
                )
                extra[sid] = {
                    "atlatl/instances": MetadataValue.json({k: s.get("status") for k, s in ps})
                }
            attempts = max((int(r.get("attempts") or 0) for r in reps), default=0)
            yield from self.materialize({"attempts": attempts, "hops": []}, merged, extra)
        self.instances.clear()

    def finish(self, code: int | None, report: Json | None) -> Iterator[AtlatlResult]:
        if report is not None:
            for rep in objs(report.get("replications")):
                yield from self.completed(rep)
        yield from self.instances_done()
        missing = [r for r in self.replications if r not in self.seen]
        run_err = obj(report.get("error")) if report else None
        tail = "\n".join(self.tail)
        md: dict[str, Any] = {"atlatl/exit_code": MetadataValue.int(-1 if code is None else code)}
        if report is not None:
            md["atlatl/run_id"] = MetadataValue.text(str(report.get("run_id")))
        if self.failures:
            md["atlatl/failed"] = MetadataValue.json(self.failures)
        if code in (EXIT_CONFIG, EXIT_INTERNAL) or run_err:
            kind = "config" if code == EXIT_CONFIG else "internal"
            detail = describe_error(run_err) if run_err else tail or "no details"
            raise Failure(
                description=f"atlatl {kind} error (exit {code}): {detail}{self._failed_lines()}",
                metadata=md,
                allow_retries=code != EXIT_CONFIG,
            )
        if report is None:
            raise Failure(
                description=f"atlatl exited {code} without writing a report\n{tail}".rstrip(),
                metadata=md,
            )
        if self.failures or missing:
            lines = self._failed_lines()
            if missing:
                lines += "".join(f"\n  {r}: not in the report" for r in missing)
            n = len(self.failures) + len(missing)
            raise Failure(description=f"{n} atlatl replication(s) failed:{lines}", metadata=md)
        if code != 0:
            raise Failure(description=f"atlatl exited {code}\n{tail}".rstrip(), metadata=md)

    def _failed_lines(self) -> str:
        return "".join(
            f"\n  {f['replication']} ({f['status']}): {f['error']}" for f in self.failures
        )


def _read_report(path: Path) -> Json | None:
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return obj(data) or None
