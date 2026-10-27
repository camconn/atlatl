//! Static checks (spec 03 §1 step 5, 02 §6): everything that can be verified from config alone.

use indexmap::IndexMap;

use crate::config::apis::ApiSpec;
use crate::config::connection::Connection;
use crate::config::diag::{ConfigError, Diagnostics};
use crate::config::env::{Resolve, all_envs, check_environments, resolve_all};
use crate::config::graph::find_cycle;
use crate::config::load::{Project, Replication, Stream, normalize_rel};
use crate::config::model::{
    ChunkBy, ConnectionKind, Duplicates, ForEachOver, Mode, SchemaMode, SourceSnapshot,
};
use crate::config::node::{Node, Span};
use crate::template::{Phase, Templates};

/// The connection type of each connection, from its base definition (overrides can't change it).
pub fn connection_types(p: &Project) -> IndexMap<String, String> {
    p.connections
        .values()
        .filter_map(|c| Some((c.name.clone(), c.node.get("type")?.as_str()?.to_owned())))
        .collect()
}

pub fn asset_key(s: &Stream) -> String {
    s.config
        .asset
        .as_ref()
        .and_then(|a| a.key.as_ref())
        .map(|k| k.value.clone())
        .unwrap_or_else(|| s.id.value.clone())
}

fn asset_key_span(s: &Stream) -> Span {
    s.config
        .asset
        .as_ref()
        .and_then(|a| a.key.as_ref())
        .map(|k| k.span.clone())
        .unwrap_or_else(|| s.id.span.clone())
}

/// The API spec behind a REST connection.
pub fn spec_for<'a>(p: &'a Project, conn: &str) -> Option<&'a ApiSpec> {
    let c = p.connections.get(conn)?;
    let spec = c.node.get("spec")?.as_str()?;
    p.api_specs.get(&normalize_rel(spec))
}

/// Runs every static check. Connection structure and the environment base rule are checked
/// for every environment.
pub fn check_project(p: &Project, templates: &Templates, diags: &mut Diagnostics) {
    check_environments(p, diags);
    let mut per_env: Vec<(Option<String>, IndexMap<String, Connection>)> = Vec::new();
    for env in all_envs(p) {
        let conns = resolve_all(p, env.as_deref(), &Resolve::Static, templates, diags);
        per_env.push((env, conns));
    }
    let types = connection_types(p);
    check_state(p, &types, diags);
    check_replications(p, &types, &per_env, templates, diags);
    diags.dedup();
}

fn check_state(p: &Project, types: &IndexMap<String, String>, diags: &mut Diagnostics) {
    let Some(state) = &p.state else { return };
    match types.get(&state.lake.value).map(String::as_str) {
        Some("ducklake" | "duckdb") => {}
        Some(t) => diags.error(
            "state",
            ConfigError::new(format!(
                "`state.lake` must be a ducklake or duckdb connection, `{}` is {t}",
                state.lake.value
            ))
            .at(state.lake.span.clone()),
        ),
        None => diags.error(
            "state",
            ConfigError::new(format!(
                "`state.lake` names unknown connection `{}`",
                state.lake.value
            ))
            .at(state.lake.span.clone()),
        ),
    }
    if let Some(locks) = &state.locks {
        let (conn, part) = match locks.value.split_once('.') {
            Some((c, rest)) => (c, Some(rest)),
            None => (locks.value.as_str(), None),
        };
        let ok = match (types.get(conn).map(String::as_str), part) {
            (Some("postgres"), None) => true,
            (Some("ducklake"), Some("catalog")) => {
                p.connections[conn]
                    .node
                    .get("catalog")
                    .and_then(|c| c.get("type"))
                    .and_then(Node::as_str)
                    == Some("postgres")
            }
            _ => false,
        };
        if !ok {
            diags.error(
                "state",
                ConfigError::new(format!(
                    "`state.locks: {}` must name a postgres connection, or `<ducklake conn>.catalog` with a postgres catalog",
                    locks.value
                ))
                .at(locks.span.clone()),
            );
        }
    }
}

fn check_replications(
    p: &Project,
    types: &IndexMap<String, String>,
    per_env: &[(Option<String>, IndexMap<String, Connection>)],
    templates: &Templates,
    diags: &mut Diagnostics,
) {
    let names: Vec<&str> = p
        .replications
        .iter()
        .map(|r| r.name.value.as_str())
        .collect();
    for r in &p.replications {
        check_replication(p, r, types, per_env, templates, diags);
        for a in &r.after {
            if a.value == r.name.value {
                diags.error(
                    "after",
                    ConfigError::new(format!("replication `{}` lists itself in `after`", a.value))
                        .at(a.span.clone()),
                );
            } else if !names.contains(&a.value.as_str()) {
                diags.error(
                    "after",
                    ConfigError::new(format!("`after` names unknown replication `{}`", a.value))
                        .at(a.span.clone()),
                );
            }
        }
    }
    let deps = |n: &str| {
        p.replication(n)
            .map(|r| r.after.iter().map(|a| a.value.clone()).collect::<Vec<_>>())
            .unwrap_or_default()
    };
    if let Some(cycle) = find_cycle(&names, deps) {
        let r = p.replication(&cycle[0]).expect("known");
        diags.error(
            "after-cycle",
            ConfigError::new(format!("replication `after` cycle: {}", cycle.join(" -> ")))
                .at(r.name.span.clone()),
        );
    }

    let mut groups: IndexMap<&str, Vec<&Replication>> = IndexMap::new();
    for r in &p.replications {
        if let Some(g) = &r.atomic_group {
            groups.entry(g.value.as_str()).or_default().push(r);
        }
    }
    for (g, reps) in &groups {
        let first = reps[0];
        for r in &reps[1..] {
            if r.target.value != first.target.value {
                diags.error(
                    "atomic-group",
                    ConfigError::new(format!(
                        "atomic_group `{g}` spans targets `{}` and `{}`; an atomic group must share one target (use `after` for ordering across targets)",
                        first.target.value, r.target.value
                    ))
                    .at(r.target.span.clone())
                    .related("other target", first.target.span.clone()),
                );
            }
        }
    }

    let mut ops: IndexMap<&str, &Replication> = IndexMap::new();
    for r in &p.replications {
        match ops.get(r.op.value.as_str()) {
            Some(first) if first.engine != r.engine => diags.error(
                "op-engine",
                ConfigError::new(format!(
                    "replications `{}` and `{}` share op `{}` (one process) but set different `engine` overrides",
                    first.name.value, r.name.value, r.op.value
                ))
                .at(r.name.span.clone()),
            ),
            Some(_) => {}
            None => {
                ops.insert(&r.op.value, r);
            }
        }
    }

    // Two replications writing one table need a shared lock_group (03 §8, 13 §4).
    let mut writers: IndexMap<(String, String), &Replication> = IndexMap::new();
    for r in &p.replications {
        for s in &r.streams {
            let Some(o) = &s.config.object else { continue };
            let target = &r.hop_of(s).target;
            let key = (target.value.clone(), o.value.clone());
            match writers.get(&key) {
                Some(prev) if prev.name.value != r.name.value => {
                    let shared = matches!((&prev.lock_group, &r.lock_group), (Some(a), Some(b)) if a.value == b.value);
                    if !shared {
                        diags.warn(
                            "shared-target",
                            ConfigError::new(format!(
                                "replications `{}` and `{}` both write `{}` on `{}` without a shared `lock_group`",
                                prev.name.value, r.name.value, o.value, target.value
                            ))
                            .at(o.span.clone()),
                        );
                    }
                }
                Some(_) => {}
                None => {
                    writers.insert(key, r);
                }
            }
        }
    }

    let mut slots: IndexMap<(String, String), &Replication> = IndexMap::new();
    for r in &p.replications {
        let pg_changes = types.get(&r.source.value).map(String::as_str) == Some("postgres")
            && types.get(&r.target.value).map(String::as_str) == Some("ducklake")
            && r.streams
                .iter()
                .filter(|s| s.hop == 0)
                .any(|s| s.config.mode.as_ref().map(|m| m.value) == Some(Mode::Cdc));
        if !pg_changes {
            continue;
        }
        let slot = crate::config::model::PgChanges::slot_name(&r.name.value);
        if let Some(prev) = slots.get(&(r.source.value.clone(), slot.clone())) {
            diags.error(
                "slot-name",
                ConfigError::new(format!(
                    "replications `{}` and `{}` on `{}` both map to slot `{slot}`; rename one",
                    prev.name.value, r.name.value, r.source.value
                ))
                .at(r.name.span.clone()),
            );
        } else {
            slots.insert((r.source.value.clone(), slot), r);
        }
    }

    let mut keys: IndexMap<String, Span> = IndexMap::new();
    for r in &p.replications {
        for s in &r.streams {
            let k = asset_key(s);
            if let Some(prev) = keys.get(&k) {
                diags.error(
                    "duplicate-asset",
                    ConfigError::new(format!("asset key `{k}` is used by more than one stream"))
                        .at(asset_key_span(s))
                        .related("first used", prev.clone()),
                );
            } else {
                keys.insert(k, asset_key_span(s));
            }
        }
    }
}

fn check_replication(
    p: &Project,
    r: &Replication,
    types: &IndexMap<String, String>,
    per_env: &[(Option<String>, IndexMap<String, Connection>)],
    templates: &Templates,
    diags: &mut Diagnostics,
) {
    let src = types.get(&r.source.value).map(String::as_str);
    let tgt = types.get(&r.target.value).map(String::as_str);
    for (role, name, t) in [("source", &r.source, src), ("target", &r.target, tgt)] {
        if t.is_none() {
            diags.error(
                "reference",
                ConfigError::new(format!(
                    "{role} `{}` is not a defined connection",
                    name.value
                ))
                .at(name.span.clone()),
            );
        }
    }
    let (Some(src), Some(tgt)) = (src, tgt) else {
        return;
    };
    if src == "duckdb" {
        diags.error(
            "duckdb-role",
            ConfigError::new(format!(
                "source `{}` is a duckdb connection, which is an ingest target or feeds `for_each.over.query` and `bind`",
                r.source.value
            ))
            .at(r.source.span.clone()),
        );
        return;
    }
    if src != "ducklake" && !is_lake_target(tgt) {
        diags.error(
            "lake-endpoint",
            ConfigError::new(format!(
                "replication `{}`: DuckLake must be the source, or DuckLake or DuckDB the target (got {src} → {tgt})",
                r.name.value
            ))
            .at(r.name.span.clone()),
        );
    }
    for (env, conns) in per_env {
        if let Some(c) = conns.get(&r.target.value)
            && !c.can_be_target()
        {
            let in_env = env
                .as_deref()
                .map(|e| format!(" in `{e}`"))
                .unwrap_or_default();
            diags.error(
                "read-only-target",
                ConfigError::new(format!(
                    "target `{}` is read_only{in_env} and can never be written",
                    r.target.value
                ))
                .at(r.target.span.clone()),
            );
        }
    }
    if let Some(ConnectionKind::Postgres(_)) = per_env
        .first()
        .and_then(|(_, c)| c.get(&r.target.value))
        .map(|c| &c.kind)
        && src != "ducklake"
    {
        diags.error(
            "target-type",
            ConfigError::new(format!(
                "postgres target `{}` requires a ducklake source (mode: cdc)",
                r.target.value
            ))
            .at(r.target.span.clone()),
        );
    }
    if src == "postgres" && tgt == "ducklake" {
        let cdc = |s: &&Stream| s.config.mode.as_ref().map(|m| m.value) == Some(Mode::Cdc);
        let own: Vec<&Stream> = r.streams.iter().filter(|s| s.hop == 0).collect();
        if own.iter().any(cdc) && !own.iter().all(cdc) {
            diags.error(
                "mode",
                ConfigError::new(format!(
                    "replication `{}` can't mix `mode: cdc` (logical replication) with ingest modes from postgres; split it",
                    r.name.value
                ))
                .at(r.name.span.clone()),
            );
        }
    }

    let ids: Vec<&str> = r.streams.iter().map(|s| s.id.value.as_str()).collect();
    for s in &r.streams {
        if s.hop == 0 {
            check_stream(p, r, s, src, tgt, types, templates, diags);
        }
        for a in s.config.after.iter().flatten() {
            let other_hop = r
                .streams
                .iter()
                .any(|o| o.id.value == a.value && o.hop != s.hop);
            if other_hop {
                diags.error(
                    "after",
                    ConfigError::new(format!(
                        "stream `{}` lists `{}` in `after`, which is in a different hop; hops are already ordered upstream first",
                        s.id.value, a.value
                    ))
                    .at(a.span.clone()),
                );
            } else if a.value == s.id.value {
                diags.error(
                    "after",
                    ConfigError::new(format!("stream `{}` lists itself in `after`", a.value))
                        .at(a.span.clone()),
                );
            } else if !ids.contains(&a.value.as_str()) {
                diags.error(
                    "after",
                    ConfigError::new(format!(
                        "`after` names `{}`, which is not a stream in replication `{}`",
                        a.value, r.name.value
                    ))
                    .at(a.span.clone()),
                );
            }
        }
    }
    let deps = |id: &str| {
        r.streams
            .iter()
            .find(|s| s.id.value == id)
            .map(|s| {
                s.config
                    .after
                    .iter()
                    .flatten()
                    .map(|a| a.value.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    };
    if let Some(cycle) = find_cycle(&ids, deps) {
        diags.error(
            "after-cycle",
            ConfigError::new(format!("stream `after` cycle: {}", cycle.join(" -> ")))
                .at(r.name.span.clone()),
        );
    }

    if src == "rest"
        && let Some(spec) = spec_for(p, &r.source.value)
    {
        for s in &r.streams {
            let Some(ep) = s.config.from.as_ref().and_then(|f| f.endpoint.clone()) else {
                continue;
            };
            for prod in spec.producers_of(&ep) {
                let has = r.streams.iter().any(|o| {
                    o.config.from.as_ref().and_then(|f| f.endpoint.as_deref())
                        == Some(prod.as_str())
                });
                if !has {
                    diags.error(
                        "queue-producer",
                        ConfigError::new(format!(
                            "stream `{}` (endpoint `{ep}`) depends on endpoint `{prod}`, which no stream in replication `{}` reads",
                            s.id.value, r.name.value
                        ))
                        .at(s.id.span.clone()),
                    );
                }
            }
        }
    }
    check_hops(p, r, types, per_env, templates, diags);
    check_for_each(p, r, types, templates, diags);
    check_source_snapshot(r, types, diags);
}

/// Targets the lake runner writes: DuckLake, or a plain DuckDB file (06 §10).
pub fn is_lake_target(t: &str) -> bool {
    matches!(t, "ducklake" | "duckdb")
}

fn err(diags: &mut Diagnostics, code: &'static str, msg: String, span: &Span) {
    diags.error(code, ConfigError::new(msg).at(span.clone()));
}

#[allow(clippy::too_many_arguments)]
fn check_stream(
    p: &Project,
    r: &Replication,
    s: &Stream,
    src: &str,
    tgt: &str,
    types: &IndexMap<String, String>,
    templates: &Templates,
    diags: &mut Diagnostics,
) {
    let c = &s.config;
    let id = &s.id.value;
    let Some(mode) = c.mode.as_ref() else {
        err(
            diags,
            "mode",
            format!("stream `{id}` has no `mode` (full-refresh | incremental | snapshot | cdc)"),
            &s.id.span,
        );
        return;
    };
    let m = mode.value;

    match &c.from {
        None => err(
            diags,
            "from",
            format!("stream `{id}` has no `from` selector"),
            &s.id.span,
        ),
        Some(f) => {
            let set: Vec<&str> = [
                ("table", f.table.is_some()),
                ("tables", f.tables.is_some()),
                ("query", f.query.is_some()),
                ("endpoint", f.endpoint.is_some()),
                ("files", f.files.is_some()),
            ]
            .into_iter()
            .filter_map(|(k, on)| on.then_some(k))
            .collect();
            if set.len() != 1 {
                err(
                    diags,
                    "from",
                    format!(
                        "`from` needs exactly one of table, tables, query, endpoint, files (found: {})",
                        if set.is_empty() {
                            "none".into()
                        } else {
                            set.join(", ")
                        }
                    ),
                    &f.span,
                );
            } else {
                let sel = set[0];
                let allowed: &[&str] = match src {
                    "ducklake" | "postgres" | "redshift" => &["table", "tables", "query"],
                    "clickhouse" => &["table", "query"],
                    "rest" => &["endpoint"],
                    "files" => &["files"],
                    "delta" => &["table"],
                    _ => &[],
                };
                if !allowed.contains(&sel) {
                    err(
                        diags,
                        "from",
                        format!(
                            "`from.{sel}` does not fit a {src} source (use: {})",
                            allowed.join(", ")
                        ),
                        &f.span,
                    );
                }
                if sel != "tables" && (f.exclude.is_some() || f.include_views.is_some()) {
                    err(
                        diags,
                        "from",
                        "`exclude` and `include_views` only apply to `from.tables`".into(),
                        &f.span,
                    );
                }
                if !matches!(sel, "tables" | "files") && f.on_no_match.is_some() {
                    err(
                        diags,
                        "from",
                        "`on_no_match` only applies to `from.tables` and `from.files`".into(),
                        &f.span,
                    );
                }
                if f.version.is_some() && src != "delta" {
                    err(
                        diags,
                        "from",
                        format!(
                            "`from.version` only applies to delta sources (`{}` is {src})",
                            r.hop_of(s).source.value
                        ),
                        &f.span,
                    );
                }
                if sel == "tables" && src == "ducklake" && m == Mode::Cdc {
                    err(
                        diags,
                        "from",
                        "cdc streams read one table (`from.table`), not a pattern".into(),
                        &f.span,
                    );
                }
                if m == Mode::Cdc && sel != "table" {
                    err(
                        diags,
                        "from",
                        format!("cdc streams read one table (`from.table`), not `from.{sel}`"),
                        &f.span,
                    );
                }
                if let Some(ep) = &f.endpoint
                    && src == "rest"
                {
                    match spec_for(p, &r.source.value) {
                        Some(spec) if !spec.endpoints.contains_key(ep) => err(
                            diags,
                            "endpoint",
                            format!("endpoint `{ep}` is not defined in API spec `{}`", spec.path),
                            &f.span,
                        ),
                        _ => {}
                    }
                }
            }
        }
    }

    if m.is_ingest() && !is_lake_target(tgt) {
        err(
            diags,
            "mode",
            format!(
                "mode `{}` writes into the lake; target `{}` is {tgt} (use mode: cdc)",
                m.as_str(),
                r.hop_of(s).target.value
            ),
            &mode.span,
        );
    }
    if src == "clickhouse" {
        if !matches!(m, Mode::FullRefresh | Mode::Incremental) {
            err(
                diags,
                "mode",
                format!(
                    "a clickhouse source supports full-refresh | incremental (stream `{id}` is {})",
                    m.as_str()
                ),
                &mode.span,
            );
        }
        if let Some(uk) = &c.update_key {
            err(
                diags,
                "update-key",
                format!(
                    "stream `{id}`: clickhouse sources don't take `update_key` (filter in `from.query`)"
                ),
                &uk.span,
            );
        }
    }
    check_bind(s, src, types, templates, diags);
    if src == "delta" && !matches!(m, Mode::FullRefresh | Mode::Incremental) {
        err(
            diags,
            "mode",
            format!(
                "a delta source supports full-refresh | incremental into the lake (stream `{id}` is {})",
                m.as_str()
            ),
            &mode.span,
        );
    }
    let pg_changes = m == Mode::Cdc && src == "postgres" && tgt == "ducklake";
    if pg_changes {
        check_pg_changes(p, r, s, &mode.span, diags);
    } else if c.history.is_some() {
        err(
            diags,
            "mode-key",
            format!("`history` only applies to postgres `changes` streams (stream `{id}`)"),
            &mode.span,
        );
    }
    if m == Mode::Cdc && !pg_changes {
        if src != "ducklake" {
            err(
                diags,
                "mode",
                format!(
                    "mode `cdc` needs a ducklake source (a change feed) or a postgres source with `changes:` into the lake; `{}` is {src}",
                    r.hop_of(s).source.value
                ),
                &mode.span,
            );
        }
        if !matches!(tgt, "postgres" | "sqlite" | "d1" | "turso") {
            err(
                diags,
                "mode",
                format!(
                    "mode `cdc` needs a serving target that can apply changes; `{}` is {tgt}",
                    r.hop_of(s).target.value
                ),
                &mode.span,
            );
        }
        if c.primary_key.is_none() {
            err(
                diags,
                "primary-key",
                format!("cdc stream `{id}` needs `primary_key`"),
                &mode.span,
            );
        }
    }
    if let Some(pk) = &c.primary_key
        && pk.value.is_empty()
    {
        err(
            diags,
            "primary-key",
            "`primary_key` is empty".into(),
            &pk.span,
        );
    }
    if c.object.is_none() {
        err(
            diags,
            "object",
            format!("stream `{id}` has no `object` (target table)"),
            &s.id.span,
        );
    }

    let only = |diags: &mut Diagnostics, key: &str, present: Option<&Span>, modes: &[Mode]| {
        if let Some(sp) = present
            && !modes.contains(&m)
        {
            let names: Vec<_> = modes.iter().map(|x| x.as_str()).collect();
            err(
                diags,
                "mode-key",
                format!(
                    "`{key}` only applies to mode {} (stream `{id}` is {})",
                    names.join(" | "),
                    m.as_str()
                ),
                sp,
            );
        }
    };
    only(
        diags,
        "update_key",
        c.update_key.as_ref().map(|x| &x.span),
        &[Mode::Incremental],
    );
    only(
        diags,
        "window",
        c.window.as_ref().map(|x| &x.span),
        &[Mode::Incremental, Mode::Cdc],
    );
    only(
        diags,
        "on_empty",
        c.on_empty.as_ref().map(|x| &x.span),
        &[Mode::FullRefresh],
    );
    only(
        diags,
        "deletes",
        c.deletes.as_ref().map(|x| &x.span),
        &[Mode::Cdc],
    );
    let cdc_only = [
        ("diff", c.diff.is_some()),
        ("on_gap", c.on_gap.is_some()),
        ("max_changes", c.max_changes.is_some()),
        ("max_snapshots", c.max_snapshots.is_some()),
        ("max_duration", c.max_duration.is_some()),
    ];
    for (k, on) in cdc_only {
        if on && m != Mode::Cdc {
            err(
                diags,
                "mode-key",
                format!(
                    "`{k}` only applies to mode cdc (stream `{id}` is {})",
                    m.as_str()
                ),
                &mode.span,
            );
        }
    }
    if c.guard.is_some() && m != Mode::FullRefresh {
        err(
            diags,
            "mode-key",
            format!(
                "`guard` only applies to mode full-refresh (stream `{id}` is {})",
                m.as_str()
            ),
            &mode.span,
        );
    }
    if m == Mode::Snapshot
        && c.metadata
            .as_ref()
            .and_then(|x| x.value.loaded_at.as_ref())
            .is_none()
    {
        err(
            diags,
            "snapshot",
            format!("snapshot stream `{id}` needs `metadata.loaded_at`"),
            &mode.span,
        );
    }
    if matches!(c.duplicates, Some(Duplicates::First | Duplicates::Last))
        && c.order_by.as_ref().is_none_or(|o| o.is_empty())
    {
        err(
            diags,
            "duplicates",
            format!("`duplicates: first|last` on `{id}` needs `order_by`"),
            &s.id.span,
        );
    }
    if let Some(ch) = &c.chunk {
        let bad = match ch.by {
            ChunkBy::Hash => (ch.columns.as_ref().is_none_or(|v| v.is_empty())
                || ch.buckets.is_none_or(|b| b < 2))
            .then_some("`chunk: {by: hash}` needs `columns` and `buckets` ≥ 2"),
            ChunkBy::Range => (ch.column.is_none() || ch.step.is_none())
                .then_some("`chunk: {by: range}` needs `column` and `step`"),
            ChunkBy::Files => (!matches!(src, "redshift" | "files"))
                .then_some("`chunk: {by: files}` applies to redshift (UNLOAD) and files sources"),
        };
        if let Some(msg) = bad {
            err(diags, "chunk", format!("{msg} (stream `{id}`)"), &s.id.span);
        }
    }
    if let Some(rc) = &c.retry
        && let Err(e) = crate::retry::RetryPolicy::from_config(rc)
    {
        err(diags, "retry", format!("stream `{id}`: {e}"), &s.id.span);
    }
    if let Some(doc) = &s.schema
        && doc.schema.value.mode == Some(SchemaMode::Observe)
    {
        diags.warn(
            "schema-observe",
            ConfigError::new(format!(
                "source schema for `{id}` is in observe mode (drafting only)"
            ))
            .at(doc.schema.span.clone()),
        );
    }
    let key = asset_key(s);
    if key.split('/').any(str::is_empty) {
        err(
            diags,
            "asset-key",
            format!("asset key `{key}` has an empty segment"),
            &asset_key_span(s),
        );
    }

    if let Some(ms) = &c.members {
        if c.from.as_ref().is_none_or(|f| f.tables.is_none()) {
            err(
                diags,
                "members",
                format!("`members` applies to `from.tables` streams (stream `{id}`)"),
                &s.id.span,
            );
        }
        for (name, mc) in ms {
            if mc.primary_key.as_ref().is_some_and(Vec::is_empty) {
                err(
                    diags,
                    "primary-key",
                    format!("stream `{id}`: `members.\"{name}\".primary_key` is empty"),
                    &s.id.span,
                );
            }
        }
    }

    check_templates(r, s, templates, diags);
}

/// `source_snapshot` (06 §3.2): postgres sources only; `shared` reads each member once, so it
/// can't be combined with `chunk` (chunks re-read the source inside the commit).
fn check_source_snapshot(
    r: &Replication,
    types: &IndexMap<String, String>,
    diags: &mut Diagnostics,
) {
    let Some(snap) = &r.source_snapshot else {
        return;
    };
    let name = &r.name.value;
    if types.get(&r.source.value).map(String::as_str) != Some("postgres") {
        err(
            diags,
            "source-snapshot",
            format!(
                "replication `{name}`: `source_snapshot` applies to postgres sources; `{}` isn't one",
                r.source.value
            ),
            &snap.span,
        );
    }
    if snap.value == SourceSnapshot::Shared {
        for s in r
            .streams
            .iter()
            .filter(|s| s.hop == 0 && s.config.chunk.is_some())
        {
            err(
                diags,
                "source-snapshot",
                format!(
                    "replication `{name}`: `source_snapshot: shared` can't be combined with `chunk` (stream `{}`)",
                    s.id.value
                ),
                &snap.span,
            );
        }
    }
}

/// `mode: cdc` from a postgres source into the lake: logical replication (08 §10).
fn check_pg_changes(p: &Project, r: &Replication, s: &Stream, at: &Span, diags: &mut Diagnostics) {
    let c = &s.config;
    let id = &s.id.value;
    let declared = p
        .connections
        .get(&r.source.value)
        .and_then(|c| c.node.get("changes"))
        .is_some();
    if !declared {
        err(
            diags,
            "changes",
            format!(
                "stream `{id}` reads changes from postgres `{}`, which has no `changes:` block (logical replication must be enabled on the connection)",
                r.source.value
            ),
            at,
        );
    }
    let unsupported = [
        ("window", c.window.is_some()),
        ("diff", c.diff.is_some()),
        ("on_gap", c.on_gap.is_some()),
        ("max_snapshots", c.max_snapshots.is_some()),
    ];
    for (k, on) in unsupported {
        if on {
            err(
                diags,
                "mode-key",
                format!("`{k}` does not apply to postgres `changes` streams (stream `{id}`)"),
                at,
            );
        }
    }
    if let Some(d) = &c.deletes
        && (d.value.column.is_some() || d.value.value.is_some())
    {
        err(
            diags,
            "deletes",
            format!(
                "postgres `changes` streams mark soft deletes in `_atlatl_op`; `deletes.column`/`value` are not supported (stream `{id}`)"
            ),
            &d.span,
        );
    }
}

fn check_templates(r: &Replication, s: &Stream, templates: &Templates, diags: &mut Diagnostics) {
    let c = &s.config;
    let vars: Vec<String> = c
        .vars
        .as_ref()
        .map(|v| v.keys().cloned().collect())
        .unwrap_or_default();
    let item: Vec<&str> = r
        .for_each
        .iter()
        .map(|f| f.value.as_name.as_str())
        .collect();
    let mut check = |src: &str, phase: Phase, span: &Span| {
        if let Err(e) = templates.check_with(src, phase, Some(&vars), &item) {
            diags.error("template", ConfigError::new(e.message).at(span.clone()));
        }
    };
    if let Some(o) = &c.object {
        check(&o.value, Phase::Plan, &o.span);
    }
    if let Some(w) = &c.window {
        check(&w.value, Phase::Plan, &w.span);
    }
    if let Some(f) = &c.from {
        for v in [&f.table, &f.tables, &f.query].into_iter().flatten() {
            check(v, Phase::Plan, &f.span);
        }
        if let Some(files) = &f.files {
            check(&files.url, Phase::Plan, &f.span);
        }
    }
    if let Some(h) = &c.hooks {
        for sql in h
            .pre_apply
            .iter()
            .flatten()
            .chain(h.post_apply.iter().flatten())
        {
            check(&sql.value, Phase::Hook, &sql.span);
        }
    }
    if let Some(vars_node) = s.node.get("vars") {
        vars_node.visit_strings(&mut |v, span| {
            if let Err(e) = templates.check(v, Phase::Load, None) {
                diags.error("template", ConfigError::new(e.message).at(span.clone()));
            }
        });
    }
}

/// `then:` hops (11 §3 (a)/(b)): each reads the lake its upstream hop wrote and commits on its own.
fn check_hops(
    p: &Project,
    r: &Replication,
    types: &IndexMap<String, String>,
    per_env: &[(Option<String>, IndexMap<String, Connection>)],
    templates: &Templates,
    diags: &mut Diagnostics,
) {
    if !r.is_multi_hop() {
        return;
    }
    if let Some(g) = &r.atomic_group {
        err(
            diags,
            "then",
            format!(
                "replication `{}` has `then` hops and `atomic_group: {}`; hops commit one after another, so a multi-hop replication can't share one transaction (order with `after` instead)",
                r.name.value, g.value
            ),
            &g.span,
        );
    }
    for (i, h) in r.hops.iter().enumerate().skip(1) {
        let Some(tgt) = types.get(&h.target.value).map(String::as_str) else {
            err(
                diags,
                "reference",
                format!(
                    "`then` target `{}` is not a defined connection",
                    h.target.value
                ),
                &h.target.span,
            );
            continue;
        };
        let Some(src) = types.get(&h.source.value).map(String::as_str) else {
            continue;
        };
        if src != "ducklake" {
            err(
                diags,
                "then",
                format!(
                    "`then` hop to `{}` would read `{}`, which is {src}; a hop reads the lake its upstream hop wrote, so the upstream target must be ducklake",
                    h.target.value, h.source.value
                ),
                &h.target.span,
            );
            continue;
        }
        for (env, conns) in per_env {
            if let Some(c) = conns.get(&h.target.value)
                && !c.can_be_target()
            {
                let in_env = env
                    .as_deref()
                    .map(|e| format!(" in `{e}`"))
                    .unwrap_or_default();
                err(
                    diags,
                    "read-only-target",
                    format!(
                        "`then` target `{}` is read_only{in_env} and can never be written",
                        h.target.value
                    ),
                    &h.target.span,
                );
            }
        }
        for s in r.streams.iter().filter(|s| s.hop == i) {
            check_stream(p, r, s, src, tgt, types, templates, diags);
        }
    }
    check_hop_reads(r, diags);
}

/// `conn:schema.table`, for literal (untemplated) names only.
fn table_node(conn: &str, table: &str) -> Option<String> {
    if crate::template::is_template(table) {
        return None;
    }
    let parts: Vec<&str> = table.split('.').collect();
    let st = match parts.as_slice() {
        [.., s, t] => format!("{s}.{t}"),
        [t] => format!("main.{t}"),
        [] => return None,
    };
    Some(format!("{conn}:{}", st.to_ascii_lowercase()))
}

/// Data-flow cycles across a replication's hops (a hop reading a table it or a downstream hop
/// writes), and hop streams reading tables their upstream hop doesn't write.
fn check_hop_reads(r: &Replication, diags: &mut Diagnostics) {
    let edge = |s: &Stream| -> Option<(String, String)> {
        let h = r.hop_of(s);
        let read = table_node(&h.source.value, s.config.from.as_ref()?.table.as_deref()?)?;
        let wrote = table_node(&h.target.value, &s.config.object.as_ref()?.value)?;
        Some((wrote, read))
    };
    let edges: Vec<(String, String)> = r.streams.iter().filter_map(edge).collect();
    let mut nodes: Vec<&str> = edges
        .iter()
        .flat_map(|(a, b)| [a.as_str(), b.as_str()])
        .collect();
    nodes.sort_unstable();
    nodes.dedup();
    let deps = |n: &str| {
        edges
            .iter()
            .filter(|(w, _)| w == n)
            .map(|(_, rd)| rd.clone())
            .collect::<Vec<_>>()
    };
    if let Some(cycle) = find_cycle(&nodes, deps) {
        diags.error(
            "then-cycle",
            ConfigError::new(format!(
                "replication `{}`: hops read what they (or a downstream hop) write: {}",
                r.name.value,
                cycle.join(" <- ")
            ))
            .at(r.name.span.clone()),
        );
    }
    for s in r.streams.iter().filter(|s| s.hop > 0) {
        let h = r.hop_of(s);
        let Some(read) = s
            .config
            .from
            .as_ref()
            .and_then(|f| f.table.as_deref())
            .and_then(|t| table_node(&h.source.value, t))
        else {
            continue;
        };
        let parent = h.parent.unwrap_or_default();
        let written: Option<Vec<String>> = r
            .streams
            .iter()
            .filter(|o| o.hop == parent)
            .map(|o| {
                o.config
                    .object
                    .as_ref()
                    .and_then(|ob| table_node(&h.source.value, &ob.value))
            })
            .collect();
        if let Some(w) = written
            && !w.contains(&read)
        {
            diags.warn(
                "then-source",
                ConfigError::new(format!(
                    "`then` stream `{}` reads a table its upstream hop doesn't write; it serves whatever that table holds",
                    s.id.value
                ))
                .at(s.id.span.clone()),
            );
        }
    }
}

/// Names a `for_each` item can't be bound as: every template root of 02 §5 and 07 §9.
const RESERVED_ROOTS: &[&str] = &[
    "env",
    "project_root",
    "environment",
    "member",
    "stream",
    "run",
    "vars",
    "staging",
    "target",
    "item",
    "state",
    "secrets",
    "response",
    "record",
    "queue",
    "sync",
];

/// Replication `for_each` (11 §3 (c)): independent, atomic instances of an ingest replication.
fn check_for_each(
    _p: &Project,
    r: &Replication,
    types: &IndexMap<String, String>,
    templates: &Templates,
    diags: &mut Diagnostics,
) {
    let Some(fe) = &r.for_each else { return };
    let (f, at) = (&fe.value, &fe.span);
    let name = &r.name.value;
    if let Some(g) = &r.atomic_group {
        err(
            diags,
            "for-each",
            format!(
                "replication `{name}` has `for_each`, whose instances are independent units, so it can't join `atomic_group: {}`",
                g.value
            ),
            &g.span,
        );
    }
    if r.is_multi_hop() {
        err(
            diags,
            "for-each",
            format!(
                "replication `{name}`: `for_each` with `then` hops isn't supported (serving hops keep one cursor per stream)"
            ),
            at,
        );
    }
    if let Some(s) = r
        .streams
        .iter()
        .find(|s| s.config.mode.as_ref().map(|m| m.value) == Some(Mode::Cdc))
    {
        err(
            diags,
            "for-each",
            format!(
                "stream `{}` is cdc; `for_each` instantiates ingest replications into the lake (cursors and slots are per stream)",
                s.id.value
            ),
            &s.id.span,
        );
    }
    let ident = f
        .as_name
        .bytes()
        .next()
        .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && f.as_name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_');
    if !ident || RESERVED_ROOTS.contains(&f.as_name.as_str()) {
        err(
            diags,
            "for-each",
            format!(
                "`for_each.as: {}` must be an identifier that isn't a template root ({})",
                f.as_name,
                RESERVED_ROOTS.join(", ")
            ),
            at,
        );
    }
    match &f.over {
        ForEachOver::List(items) => {
            if items.is_empty() {
                err(
                    diags,
                    "for-each",
                    format!("replication `{name}`: `for_each.over` is an empty list"),
                    at,
                );
            }
            let mut seen: Vec<String> = Vec::new();
            for it in items {
                match super::instance_key(it, f.key.as_deref()) {
                    Ok(k) if seen.contains(&k) => err(
                        diags,
                        "for-each",
                        format!("replication `{name}`: `for_each` key `{k}` repeats"),
                        at,
                    ),
                    Ok(k) => seen.push(k),
                    Err(e) => err(diags, "for-each", format!("replication `{name}`: {e}"), at),
                }
            }
        }
        ForEachOver::Query(q) => {
            if q.query.trim().is_empty() {
                err(
                    diags,
                    "for-each",
                    "`for_each.over.query` is empty".into(),
                    at,
                );
            }
            match types.get(&q.connection).map(String::as_str) {
                Some("ducklake" | "postgres" | "duckdb") => {}
                Some(t) => err(
                    diags,
                    "for-each",
                    format!(
                        "`for_each.over.connection: {}` must be a lake, postgres or duckdb connection, not {t}",
                        q.connection
                    ),
                    at,
                ),
                None => err(
                    diags,
                    "reference",
                    format!(
                        "`for_each.over.connection: {}` is not a defined connection",
                        q.connection
                    ),
                    at,
                ),
            }
        }
    }
    for s in &r.streams {
        let (Some(m), Some(o)) = (&s.config.mode, &s.config.object) else {
            continue;
        };
        if m.value == Mode::FullRefresh
            && !templates
                .roots_used(&o.value)
                .iter()
                .any(|v| v == &f.as_name)
        {
            err(
                diags,
                "for-each-object",
                format!(
                    "full-refresh stream `{}` writes `{}` for every `for_each` instance, each replacing the last; use `{{{{ {} }}}}` in `object`, or incremental with a primary key",
                    s.id.value, o.value, f.as_name
                ),
                &o.span,
            );
        }
    }
}

/// `bind` lists (04 §12): clickhouse `from.query` streams only.
fn check_bind(
    s: &Stream,
    src: &str,
    types: &IndexMap<String, String>,
    templates: &Templates,
    diags: &mut Diagnostics,
) {
    let Some(b) = &s.config.bind else { return };
    let id = &s.id.value;
    let at = &s.id.span;
    if src != "clickhouse" {
        err(
            diags,
            "bind",
            format!("stream `{id}`: `bind` applies to clickhouse sources, not {src}"),
            at,
        );
        return;
    }
    let Some(query) = s.config.from.as_ref().and_then(|f| f.query.as_ref()) else {
        err(
            diags,
            "bind",
            format!("stream `{id}`: `bind` lists are spliced into `from.query`, which it lacks"),
            at,
        );
        return;
    };
    let used: Vec<String> = templates
        .paths_used(query)
        .into_iter()
        .filter_map(|p| p.strip_prefix("bind.").map(str::to_owned))
        .collect();
    for name in b.keys().filter(|n| !used.contains(n)) {
        err(
            diags,
            "bind",
            format!(
                "stream `{id}`: `bind.{name}` is not used in `from.query` (splice it in as {{{{ bind.{name} }}}})"
            ),
            at,
        );
    }
    for name in used.iter().filter(|n| !b.contains_key(*n)) {
        err(
            diags,
            "bind",
            format!("stream `{id}`: `from.query` uses `bind.{name}`, which `bind` doesn't define"),
            at,
        );
    }
    for (name, cfg) in b {
        let ident = name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        if !ident {
            err(
                diags,
                "bind",
                format!("stream `{id}`: bind name `{name}` must be an identifier"),
                at,
            );
        }
        if cfg.query.trim().is_empty() {
            err(
                diags,
                "bind",
                format!("stream `{id}`: `bind.{name}.query` is empty"),
                at,
            );
        }
        match types.get(&cfg.connection).map(String::as_str) {
            Some("ducklake" | "postgres" | "duckdb") => {}
            Some(t) => err(
                diags,
                "bind",
                format!(
                    "stream `{id}`: `bind.{name}.connection: {}` must be a lake, postgres or duckdb connection, not {t}",
                    cfg.connection
                ),
                at,
            ),
            None => err(
                diags,
                "reference",
                format!(
                    "stream `{id}`: `bind.{name}.connection: {}` is not a defined connection",
                    cfg.connection
                ),
                at,
            ),
        }
        if cfg.batch == Some(0) {
            err(
                diags,
                "bind",
                format!("stream `{id}`: `bind.{name}.batch` must be at least 1"),
                at,
            );
        }
    }
    let batched: Vec<&str> = b
        .iter()
        .filter(|(_, c)| c.batch.is_some())
        .map(|(n, _)| n.as_str())
        .collect();
    if batched.len() > 1 {
        err(
            diags,
            "bind",
            format!(
                "stream `{id}`: only one bind list can be batched (found {})",
                batched.join(", ")
            ),
            at,
        );
    }
}
