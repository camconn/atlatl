"""`AtlatlProjectComponent` (spec 09 §2)."""

# No `from __future__ import annotations`: `Resolvable` reads the field types at runtime.

from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

from dagster import Component, ComponentLoadContext, Definitions, Resolvable

from dagster_atlatl._assets import build_atlatl_assets
from dagster_atlatl._plan import load_plan
from dagster_atlatl._resource import AtlatlResource


@dataclass
class AtlatlProjectComponent(Component, Resolvable):
    """Assets for every stream of an atlatl project, one multi-asset per op.

    ```yaml
    type: dagster_atlatl.AtlatlProjectComponent
    attributes:
      project: "{{ context.project_root }}/atlatl"
      environment_from: DBT_PROFILE
      replications: ["*"]
    ```
    """

    project: str
    environment_from: str | None = None
    env: str | None = None
    replications: Sequence[str] = field(default_factory=lambda: ["*"])
    terminate_grace_s: float = 5.0

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        root = Path(self.project)
        if not root.is_absolute():
            root = context.path / root
        plan = load_plan(root)
        res = AtlatlResource(
            project=str(plan.project_root),
            env=self.env,
            environment_from=self.environment_from,
            terminate_grace_s=self.terminate_grace_s,
        )
        return Definitions(
            assets=build_atlatl_assets(plan, replications=self.replications, resource=res)
        )
