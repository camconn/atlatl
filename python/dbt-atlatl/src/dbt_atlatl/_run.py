"""Running `atlatl run` against the DuckDB file dbt-duckdb is about to open."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from dbt_atlatl import _binary
from dbt_atlatl._binary import AtlatlBinaryError

SKIP_ENV = "DBT_ATLATL_SKIP"
PATH_ENV = "DBT_DUCKDB_PATH"
"""Set for atlatl: dbt's DuckDB file, absolute, for `path: "{{ env.DBT_DUCKDB_PATH }}"`."""

DEFAULT_COMMANDS = ("build", "run")
STDERR_TAIL = 50
STOP_GRACE_S = 10.0


class AtlatlRunError(RuntimeError):
    """atlatl is misconfigured, missing, or its run did not succeed."""


def _strs(config: Mapping[str, Any], key: str) -> tuple[str, ...]:
    v = config.get(key)
    if v is None:
        return ()
    if isinstance(v, str):
        return (v,)
    if isinstance(v, list):
        items = cast(list[object], v)
        if all(isinstance(x, str) for x in items):
            return tuple(cast(list[str], items))
    raise AtlatlRunError(f"dbt_atlatl plugin config `{key}` must be a string or list of strings")


def _opt_str(config: Mapping[str, Any], key: str) -> str | None:
    v = config.get(key)
    if v is None or isinstance(v, str):
        return v
    raise AtlatlRunError(f"dbt_atlatl plugin config `{key}` must be a string")


@dataclass(frozen=True)
class RunConfig:
    """The plugin's `config:` in profiles.yml.

    dbt-duckdb merges the profile's `settings:` into every plugin config, so unknown keys are
    ignored rather than rejected.
    """

    project: Path
    env: str | None = None
    replications: tuple[str, ...] = ()
    streams: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    vars: Mapping[str, str] = field(default_factory=dict[str, str])
    binary: str | None = None
    extension_dir: str | None = None
    timeout: str | None = None
    concurrency: int | None = None
    commands: tuple[str, ...] = DEFAULT_COMMANDS

    @classmethod
    def from_plugin_config(cls, config: Mapping[str, Any], base: Path) -> RunConfig:
        project = _opt_str(config, "project")
        if not project:
            raise AtlatlRunError(
                "dbt_atlatl plugin config needs `project`: the atlatl project root"
            )
        raw_vars: object = config.get("vars") or {}
        if not isinstance(raw_vars, Mapping):
            raise AtlatlRunError("dbt_atlatl plugin config `vars` must be a mapping")
        vars_ = cast(Mapping[object, object], raw_vars)
        concurrency = config.get("concurrency")
        if concurrency is not None and (
            isinstance(concurrency, bool) or not isinstance(concurrency, int)
        ):
            raise AtlatlRunError("dbt_atlatl plugin config `concurrency` must be an integer")
        commands = _strs(config, "commands") if "commands" in config else DEFAULT_COMMANDS
        return cls(
            project=(base / project).resolve(),
            env=_opt_str(config, "env"),
            replications=_strs(config, "replications"),
            streams=_strs(config, "streams"),
            tags=_strs(config, "tags"),
            vars={str(k): str(v) for k, v in vars_.items()},
            binary=_opt_str(config, "binary"),
            extension_dir=_opt_str(config, "extension_dir"),
            timeout=_opt_str(config, "timeout"),
            concurrency=concurrency,
            commands=commands,
        )


def binary_path(configured: str | None = None) -> Path:
    try:
        return _binary.binary_path(configured)
    except AtlatlBinaryError as e:
        raise AtlatlRunError(str(e)) from e


def command(cfg: RunConfig, report: Path) -> list[str]:
    cmd = [
        str(binary_path(cfg.binary)),
        "--project",
        str(cfg.project),
        "--log-format",
        "text",
        "run",
    ]
    cmd += ["--report", str(report)]
    if cfg.env:
        cmd += ["--env", cfg.env]
    for flag, values in (
        ("--replication", cfg.replications),
        ("--stream", cfg.streams),
        ("--tag", cfg.tags),
    ):
        for v in values:
            cmd += [flag, v]
    for k, v in cfg.vars.items():
        cmd += ["--var", f"{k}={v}"]
    ext = cfg.extension_dir or _binary.extension_dir()
    if ext:
        cmd += ["--extension-dir", str(ext)]
    if cfg.timeout:
        cmd += ["--timeout", cfg.timeout]
    if cfg.concurrency is not None:
        cmd += ["--concurrency", str(cfg.concurrency)]
    return cmd


def summarize(report: Mapping[str, Any]) -> list[str]:
    lines: list[str] = []
    for r in report.get("replications", []):
        streams = ", ".join(f"{s['stream_id']}={s['status']}" for s in r.get("streams", []))
        line = f"{r['replication']}: {r['status']}" + (f" ({streams})" if streams else "")
        if r.get("error"):
            line += f": {r['error'].get('message', r['error'])}"
        lines.append(line)
    return lines


def stop(p: subprocess.Popen[str], grace: float = STOP_GRACE_S) -> None:
    if p.poll() is not None:
        return
    p.terminate()
    try:
        p.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait()


def run(
    cfg: RunConfig,
    duckdb_path: Path,
    log: Callable[[str], None] = lambda _: None,
    extra_env: Mapping[str, str] | None = None,
) -> Mapping[str, Any]:
    """Runs atlatl to completion; raises unless the run report says `succeeded`."""
    with tempfile.TemporaryDirectory(prefix="dbt-atlatl-") as tmp:
        report_path = Path(tmp) / "report.json"
        cmd = command(cfg, report_path)
        env = {**os.environ, **(extra_env or {}), PATH_ENV: str(duckdb_path)}
        log(f"running {' '.join(cmd)}")
        # atlatl's log (its periodic progress lines included) goes to dbt's log as it's written;
        # the tail is kept for the error when there's no report.
        tail: deque[str] = deque(maxlen=STDERR_TAIL)
        try:
            with subprocess.Popen(
                cmd,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                errors="replace",
            ) as p:
                assert p.stderr is not None
                try:
                    for raw in p.stderr:
                        line = raw.rstrip("\n")
                        if line:
                            tail.append(line)
                            log(line)
                    returncode = p.wait()
                except BaseException:
                    # Don't leave atlatl writing the file dbt is about to open: SIGTERM is its
                    # graceful kill (03 §9); SIGKILL if it doesn't stop.
                    stop(p)
                    raise
        except OSError as e:
            raise AtlatlRunError(f"running atlatl failed: {e}") from e
        report: Mapping[str, Any] = (
            json.loads(report_path.read_text()) if report_path.is_file() else {}
        )
    for line in summarize(report):
        log(line)
    if returncode != 0 or report.get("status") != "succeeded":
        status = report.get("status", f"exit {returncode}")
        detail = report.get("error") or "\n".join(summarize(report)) or "\n".join(tail)
        raise AtlatlRunError(f"atlatl run {status}:\n{detail}")
    return report
