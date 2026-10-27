"""Plan → asset specs and one `multi_asset` per op (spec 09 §2)."""

# No `from __future__ import annotations`: Dagster inspects the compute fn's context annotation.

from collections.abc import Callable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

from dagster import (
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    AssetSpec,
    Definitions,
    RetryPolicy,
    multi_asset,
)

from dagster_atlatl._plan import AtlatlPlan, AtlatlPlanError, ReplicationInfo, load_plan
from dagster_atlatl._resource import (
    MD_PROJECT,
    MD_REPLICATION,
    MD_STREAM,
    MD_TARGET,
    MD_TARGET_TYPE,
    AtlatlResource,
    AtlatlResult,
)
from dagster_atlatl._results import SCHEMA_CHECK


def asset_specs(plan: AtlatlPlan, replications: Sequence[ReplicationInfo]) -> list[AssetSpec]:
    return [
        AssetSpec(
            key=AssetKey(list(s.asset.key_path)),
            deps=[AssetKey.from_user_string(d) for d in s.asset.deps],
            description=s.asset.description,
            group_name=s.asset.group,
            kinds=set(s.asset.kinds),
            tags=dict(s.asset.tags),
            code_version=s.asset.code_version,
            skippable=True,
            metadata={
                MD_PROJECT: str(plan.project_root),
                MD_REPLICATION: r.name,
                MD_STREAM: s.id,
                MD_TARGET: r.target_of(s).name,
                MD_TARGET_TYPE: r.target_of(s).type,
            },
        )
        for r in replications
        for s in r.streams
        if not s.feed_only
    ]


def check_specs(replications: Sequence[ReplicationInfo]) -> list[AssetCheckSpec]:
    return [
        AssetCheckSpec(
            SCHEMA_CHECK,
            asset=AssetKey(list(s.asset.key_path)),
            description=f"Source schema ({s.source_schema}) checked by atlatl on every run",
        )
        for r in replications
        for s in r.streams
        if s.source_schema and not s.feed_only
    ]


def _plan(project: str | Path | AtlatlPlan, binary: str | Path | None) -> AtlatlPlan:
    return project if isinstance(project, AtlatlPlan) else load_plan(project, binary=binary)


def atlatl_assets(
    *,
    project: str | Path | AtlatlPlan,
    replication: str | Sequence[str],
    name: str | None = None,
    op_tags: Mapping[str, Any] | None = None,
    retry_policy: RetryPolicy | None = None,
    pool: str | None = None,
    binary: str | Path | None = None,
) -> Callable[[Callable[..., Iterator[Any]]], AssetsDefinition]:
    """A multi-asset over the named replications (globs allowed), for Python-assisted loads.

    The decorated function runs whatever it needs, then `yield from atlatl.run(context, ...)`.
    """
    plan = _plan(project, binary).select(
        [replication] if isinstance(replication, str) else replication
    )
    reps = list(plan.replications)

    def decorator(fn: Callable[..., Iterator[Any]]) -> AssetsDefinition:
        return multi_asset(
            name=name or fn.__name__,
            specs=asset_specs(plan, reps),
            check_specs=check_specs(reps),
            can_subset=True,
            op_tags=op_tags,
            retry_policy=retry_policy,
            pool=pool,
        )(fn)

    return decorator


def build_atlatl_assets(
    project: str | Path | AtlatlPlan,
    *,
    replications: Sequence[str] = ("*",),
    resource: AtlatlResource | None = None,
    binary: str | Path | None = None,
    op_tags: Mapping[str, Any] | None = None,
    retry_policy: RetryPolicy | None = None,
) -> list[AssetsDefinition]:
    """One `multi_asset` per op over the replications matching `replications`."""
    plan = _plan(project, binary).select(replications)
    res = resource or AtlatlResource(
        project=str(plan.project_root), binary=str(binary) if binary else None
    )
    out: list[AssetsDefinition] = []
    for op, reps in plan.ops().items():
        specs = asset_specs(plan, reps)
        if not specs:
            continue

        def compute(context: AssetExecutionContext) -> Iterator[AtlatlResult]:
            yield from res.run(context)

        out.append(
            multi_asset(
                name=op,
                specs=specs,
                check_specs=check_specs(reps),
                can_subset=True,
                op_tags=op_tags,
                retry_policy=retry_policy,
            )(compute)
        )
    if not out:
        raise AtlatlPlanError(f"no assets in {plan.project_root} for {list(replications)}")
    return out


def build_atlatl_defs(
    project: str | Path | AtlatlPlan,
    *,
    replications: Sequence[str] = ("*",),
    env: str | None = None,
    environment_from: str | None = None,
    terminate_grace_s: float = 5.0,
    binary: str | Path | None = None,
    op_tags: Mapping[str, Any] | None = None,
    retry_policy: RetryPolicy | None = None,
) -> Definitions:
    """`Definitions` for an atlatl project, without the Components machinery."""
    plan = _plan(project, binary)
    res = AtlatlResource(
        project=str(plan.project_root),
        env=env,
        environment_from=environment_from,
        terminate_grace_s=terminate_grace_s,
        binary=str(binary) if binary else None,
    )
    return Definitions(
        assets=build_atlatl_assets(
            plan,
            replications=replications,
            resource=res,
            op_tags=op_tags,
            retry_policy=retry_policy,
        )
    )
