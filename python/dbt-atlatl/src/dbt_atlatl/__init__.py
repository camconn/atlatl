"""dbt-duckdb plugin: runs atlatl into dbt's DuckDB file before dbt-duckdb opens it.

dbt-duckdb builds its plugins before its first `duckdb.connect`, and DuckDB holds an exclusive
lock on the file once connected, so `initialize` is the last point where another process can write
it. In profiles.yml:

    plugins:
      - module: dbt_atlatl
        alias: atlatl
        config: {project: atlatl, replications: [vendor]}
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.events.logging import AdapterLogger

from dbt_atlatl._run import SKIP_ENV, AtlatlRunError, RunConfig, run

__all__ = ["AtlatlRunError", "Plugin", "RunConfig"]

logger = AdapterLogger("atlatl")


@dataclass(frozen=True)
class Invocation:
    which: str | None = None
    project_dir: Path = field(default_factory=Path.cwd)
    profiles_dir: str | None = None
    target: str | None = None

    def env(self) -> dict[str, str]:
        """What a `dbt:` connection needs to pick dbt's own profiles.yml and target."""
        out: dict[str, str] = {}
        if self.profiles_dir:
            out["DBT_PROFILES_DIR"] = str(Path(self.profiles_dir).resolve())
        if self.target:
            out["DBT_TARGET"] = self.target
        return out


def _invocation() -> Invocation:
    """This dbt invocation's flags, when dbt has set them."""
    try:
        from dbt.flags import get_flags

        flags = cast(object, get_flags())
    except Exception:
        return Invocation()

    def s(name: str) -> str | None:
        v = getattr(flags, name, None)
        return str(v) if isinstance(v, str | Path) else None

    project_dir = s("PROJECT_DIR")
    return Invocation(
        which=s("WHICH"),
        project_dir=Path(project_dir) if project_dir else Path.cwd(),
        profiles_dir=s("PROFILES_DIR"),
        target=s("TARGET"),
    )


def _info(msg: str) -> None:
    logger.info(msg)


class Plugin(BasePlugin):
    def initialize(self, plugin_config: dict[str, Any]) -> None:
        inv = _invocation()
        self.config = RunConfig.from_plugin_config(plugin_config, inv.project_dir)
        self.report: dict[str, Any] | None = None
        if os.environ.get(SKIP_ENV, "") not in ("", "0", "false"):
            logger.info(f"atlatl: skipped ({SKIP_ENV} is set)")
            return
        if inv.which is not None and inv.which not in self.config.commands:
            logger.debug(
                f"atlatl: skipped for `dbt {inv.which}` (runs on {', '.join(self.config.commands)})"
            )
            return
        path = self.creds.path if self.creds is not None else None
        if not path or path == ":memory:" or path.startswith(("md:", "motherduck:")):
            raise AtlatlRunError(f"dbt_atlatl needs a local DuckDB file `path`, not {path!r}")
        # DuckDB resolves a relative path against the working directory, so atlatl must too.
        self.report = dict(run(self.config, Path(path).resolve(), _info, inv.env()))
