//! `atlatl state show|set|reset|accept|init|move` (05 §5). A `for_each` instance's state is
//! addressed as `<stream>@<key>` or `<stream> --instance <key>`; a bare stream id of a `for_each`
//! replication covers every instance for `show`, `reset`, `accept` and `move`.

use std::path::PathBuf;
use std::sync::Arc;

use atlatl_core::config::load::Project;
use atlatl_core::config::model::{ConnectionKind, ForEachOver};
use atlatl_core::error::{AtlatlError, Result};
use atlatl_core::plan::{self, PlanRequest, state_config_hash};
use atlatl_core::redact::Redactor;
use atlatl_core::template::{Context, EnvSource, Templates};
use atlatl_engine::budget::{Budget, BudgetParams, Detected};
use atlatl_engine::{Engine, EngineOptions};
use atlatl_state::{LakeState, RunRecord, StateKind, StateRow};
use serde_json::{Value, json};

use crate::lake::split_object;
use crate::runtime::resolve_connection;

#[derive(Clone, Debug)]
pub enum StateOp {
    Show {
        stream: Option<String>,
        instance: Option<String>,
    },
    Set {
        stream: String,
        instance: Option<String>,
        member: String,
        cursor: String,
    },
    Reset {
        stream: String,
        instance: Option<String>,
        member: Option<String>,
    },
    Accept {
        stream: String,
        instance: Option<String>,
    },
    Init {
        stream: String,
        instance: Option<String>,
        /// Wildcard members to seed (`schema.table`), each from its own target table.
        members: Vec<String>,
    },
    Move {
        old: String,
        new: String,
        instance: Option<String>,
    },
}

/// A state address: a stream id, and the `for_each` instance key if one is named.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Address {
    pub(crate) stream: String,
    pub(crate) key: Option<String>,
}

impl Address {
    /// `<stream>@<key>` (stream ids never contain `@`), and/or `--instance <key>`.
    pub(crate) fn parse(id: &str, instance: Option<&str>) -> Result<Self> {
        let (stream, key) = match id.split_once('@') {
            Some((s, k)) => (s, Some(k)),
            None => (id, None),
        };
        let key = match (key, instance) {
            (Some(a), Some(b)) if a != b => {
                return Err(AtlatlError::config(format!(
                    "`{id}` names instance `{a}` but --instance is `{b}`"
                )));
            }
            (k, i) => k.or(i),
        };
        if stream.is_empty() || key.is_some_and(str::is_empty) {
            return Err(AtlatlError::config(format!(
                "`{id}`: expected `<stream>` or `<stream>@<instance key>`"
            )));
        }
        Ok(Self {
            stream: stream.to_owned(),
            key: key.map(str::to_owned),
        })
    }

    fn state_id(&self) -> String {
        match &self.key {
            Some(k) => format!("{}@{k}", self.stream),
            None => self.stream.clone(),
        }
    }

    /// State ids this address covers among `ids`: exactly one instance's, or the stream's own
    /// and every instance's.
    pub(crate) fn covers(&self, id: &str) -> bool {
        match &self.key {
            Some(_) => id == self.state_id(),
            None => {
                id == self.stream
                    || id
                        .strip_prefix(self.stream.as_str())
                        .is_some_and(|r| r.starts_with('@'))
            }
        }
    }
}

/// Checks an address against the config: an instance key needs a `for_each` replication (and,
/// for a literal list, one of its keys when `known` is asked), and a `for_each` stream needs a
/// key when `one` state row is meant.
pub(crate) fn check(project: &Project, a: &Address, known: bool, one: bool) -> Result<()> {
    let Some((rep, _)) = project.stream(&a.stream) else {
        return Err(AtlatlError::config(format!(
            "no stream `{}` in the project",
            a.stream
        )));
    };
    let fe = rep.for_each.as_ref().map(|f| &f.value);
    match (&a.key, fe) {
        (Some(k), None) => Err(AtlatlError::config(format!(
            "stream `{}` is not in a `for_each` replication, so it has no instance `{k}`",
            a.stream
        ))),
        (Some(k), Some(f)) if known => match &f.over {
            ForEachOver::List(items) => {
                let keys: Vec<String> = items
                    .iter()
                    .filter_map(|it| plan::instance_key(it, f.key.as_deref()).ok())
                    .collect();
                if keys.contains(k) {
                    Ok(())
                } else {
                    Err(AtlatlError::config(format!(
                        "replication `{}` has no instance `{k}` (its instances: {})",
                        rep.name.value,
                        keys.join(", ")
                    )))
                }
            }
            ForEachOver::Query(_) => Ok(()),
        },
        (None, Some(_)) if one => Err(AtlatlError::config(format!(
            "stream `{}` runs once per `for_each` instance of `{}`: name one with --instance <key> or `{}@<key>`",
            a.stream, rep.name.value, a.stream
        ))),
        _ => Ok(()),
    }
}

impl StateOp {
    fn name(&self) -> &'static str {
        match self {
            StateOp::Show { .. } => "state show",
            StateOp::Set { .. } => "state set",
            StateOp::Reset { .. } => "state reset",
            StateOp::Accept { .. } => "state accept",
            StateOp::Init { .. } => "state init",
            StateOp::Move { .. } => "state move",
        }
    }
}

pub struct StateArgs {
    pub env: Option<String>,
    pub extension_dir: Option<PathBuf>,
}

/// Runs one state command; returns what to print (JSON). Every change is logged in `_atlatl.runs`.
pub async fn state(
    project: Arc<Project>,
    args: StateArgs,
    vars: Arc<dyn EnvSource>,
    op: StateOp,
) -> Result<Value> {
    let (engine, lake, st) = open_store(&project, &args, vars.clone()).await?;
    let lake_name = project
        .state
        .as_ref()
        .map(|s| s.lake.value.clone())
        .unwrap_or_default();
    let stream_cfg = |id: &str| {
        project
            .stream(id)
            .map(|(_, s)| s)
            .ok_or_else(|| AtlatlError::config(format!("no stream `{id}` in the project")))
    };
    // Distinct state ids an address covers (its instances' included).
    let covered = |a: &Address| {
        let (st, a) = (st.clone(), a.clone());
        let engine = &engine;
        async move {
            let rows = engine.with_conn(move |c| st.list(c, None)).await?;
            let mut ids: Vec<String> = rows
                .into_iter()
                .map(|r| r.stream_id)
                .filter(|id| a.covers(id))
                .collect();
            ids.dedup();
            Ok::<_, AtlatlError>(ids)
        }
    };
    let out = match op.clone() {
        StateOp::Show { stream, instance } => {
            let a = match stream {
                Some(s) => Some(Address::parse(&s, instance.as_deref())?),
                None if instance.is_some() => {
                    return Err(AtlatlError::config("--instance needs --stream"));
                }
                None => None,
            };
            let rows = engine.with_conn(move |c| st.list(c, None)).await?;
            let rows: Vec<StateRow> = rows
                .into_iter()
                .filter(|r| a.as_ref().is_none_or(|a| a.covers(&r.stream_id)))
                .collect();
            return Ok(serde_json::to_value(rows).unwrap_or_default());
        }
        StateOp::Reset {
            stream,
            instance,
            member,
        } => {
            let a = Address::parse(&stream, instance.as_deref())?;
            let mut n = 0;
            for id in covered(&a).await? {
                let (st2, m) = (st.clone(), member.clone());
                n += engine
                    .with_conn(move |c| st2.reset(c, &id, m.as_deref()))
                    .await?;
            }
            log(&engine, st, &op, &args, json!({"removed": n})).await?;
            json!({"removed": n})
        }
        StateOp::Accept { stream, instance } => {
            let a = Address::parse(&stream, instance.as_deref())?;
            check(&project, &a, true, false)?;
            let hash = state_config_hash(stream_cfg(&a.stream)?);
            let mut n = 0;
            let mut ids = Vec::new();
            for id in covered(&a).await? {
                let (st2, h, i) = (st.clone(), hash.clone(), id.clone());
                n += engine.with_conn(move |c| st2.accept(c, &i, &h)).await?;
                ids.push(id);
            }
            if n == 0 {
                return Err(AtlatlError::config("no state to accept"));
            }
            let detail = json!({"config_hash": hash, "rows": n, "state_ids": ids});
            log(&engine, st, &op, &args, detail.clone()).await?;
            detail
        }
        StateOp::Move { old, new, instance } => {
            let (o, n) = (
                Address::parse(&old, instance.as_deref())?,
                Address::parse(&new, instance.as_deref())?,
            );
            if o.key.is_some() != n.key.is_some() {
                return Err(AtlatlError::config(format!(
                    "`{old}` → `{new}`: move an instance to an instance, or a stream (with its instances) to a stream"
                )));
            }
            stream_cfg(&n.stream)?;
            check(&project, &n, false, false)?;
            let mut rows = 0;
            let mut moved = serde_json::Map::new();
            for id in covered(&o).await? {
                let to = match id.strip_prefix(o.stream.as_str()) {
                    Some(rest) if o.key.is_none() => format!("{}{rest}", n.stream),
                    _ => n.state_id(),
                };
                let (st2, from, t) = (st.clone(), id.clone(), to.clone());
                rows += engine.with_conn(move |c| st2.rename(c, &from, &t)).await?;
                moved.insert(id, Value::String(to));
            }
            let detail = json!({"rows": rows, "moved": moved});
            log(&engine, st, &op, &args, detail.clone()).await?;
            detail
        }
        StateOp::Set {
            stream,
            instance,
            member,
            cursor,
        } => {
            let a = Address::parse(&stream, instance.as_deref())?;
            check(&project, &a, true, true)?;
            let s = stream_cfg(&a.stream)?;
            let hash = state_config_hash(s);
            let column = s.config.update_key.as_ref().map(|u| u.value.column.clone());
            let st2 = st.clone();
            let (sid, mem) = (a.state_id(), member.clone());
            let row = engine
                .with_conn(move |c| {
                    let prev = st2.get(c, &sid, &mem, StateKind::UpdateKey)?;
                    let value = match (&prev, column) {
                        (Some(p), _) => {
                            let mut v = p.value.clone();
                            v["value"] = Value::String(cursor);
                            v
                        }
                        (None, Some(col)) => {
                            json!({"column": col, "type": Value::Null, "value": cursor})
                        }
                        (None, None) => {
                            return Err(AtlatlError::config(format!(
                                "stream `{sid}` has no `update_key` cursor to set"
                            )));
                        }
                    };
                    let row = StateRow {
                        stream_id: sid,
                        member: mem,
                        kind: StateKind::UpdateKey,
                        value,
                        config_hash: hash,
                        run_id: atlatl_core::ids::ulid(),
                        lake_snapshot: None,
                        updated_at: None,
                    };
                    st2.put(c, &row)?;
                    Ok((prev.map(|p| p.value), row.value))
                })
                .await?;
            let detail = json!({"before": row.0, "after": row.1});
            log(&engine, st, &op, &args, detail.clone()).await?;
            detail
        }
        StateOp::Init {
            stream,
            instance,
            members,
        } => {
            let a = Address::parse(&stream, instance.as_deref())?;
            check(&project, &a, true, true)?;
            let s = stream_cfg(&a.stream)?;
            let uk = s
                .config
                .update_key
                .as_ref()
                .map(|u| u.value.column.clone())
                .ok_or_else(|| {
                    AtlatlError::config(format!("stream `{}` has no `update_key`", a.stream))
                })?;
            let req = PlanRequest {
                env: args.env.clone(),
                streams: vec![a.stream.clone()],
                with_deps: false,
                ..PlanRequest::all()
            };
            let p = plan::build(&project, &req, vars.clone(), Redactor::global())
                .map_err(|d| {
                    AtlatlError::config(
                        d.errors()
                            .map(|e| e.error.to_string())
                            .collect::<Vec<_>>()
                            .join("; "),
                    )
                })?
                .0;
            let (rep, sp) = p
                .replications
                .iter()
                .filter(|r| {
                    a.key.is_none() || r.instance.as_ref().map(|i| &i.key) == a.key.as_ref()
                })
                .find_map(|r| r.streams.iter().find(|x| x.id == a.stream).map(|x| (r, x)))
                .ok_or_else(|| {
                    AtlatlError::config(format!(
                        "`{}` isn't planned: `state init` on a `for_each.over.query` instance needs a run to list its items",
                        a.state_id()
                    ))
                })?;
            // `(state member, target object)`: wildcard members are named by the caller, since
            // discovery needs the source.
            let targets: Vec<(String, String)> = match sp.members.as_slice() {
                [m] if m.pattern => {
                    if members.is_empty() {
                        return Err(AtlatlError::config(format!(
                            "stream `{}` reads `{}`: name the members to seed with `--member`",
                            a.stream, m.name
                        )));
                    }
                    let exclude = s
                        .config
                        .from
                        .as_ref()
                        .and_then(|f| f.exclude.clone())
                        .unwrap_or_default();
                    members
                        .iter()
                        .map(|name| {
                            if !member_matches(name, &m.name, &exclude) {
                                return Err(AtlatlError::config(format!(
                                    "`{name}` is not a member of `{}`",
                                    m.name
                                )));
                            }
                            Ok((
                                name.clone(),
                                render_member(
                                    &project,
                                    p.env.as_deref(),
                                    &vars,
                                    s,
                                    sp,
                                    rep.instance.as_ref(),
                                    name,
                                )?
                                .0,
                            ))
                        })
                        .collect::<Result<_>>()?
                }
                [m] if members.is_empty() && m.object.is_some() => {
                    vec![(String::new(), m.object.clone().unwrap_or_default())]
                }
                [_] => {
                    return Err(AtlatlError::config(
                        "`--member` is for streams that read `from.tables`",
                    ));
                }
                _ => {
                    return Err(AtlatlError::config(
                        "`state init --from-target` needs a single-member stream",
                    ));
                }
            };
            let target = if rep.target.name == lake_name {
                lake.clone()
            } else {
                attach_lake(
                    &engine,
                    &project,
                    &rep.target.name,
                    args.env.as_deref(),
                    vars.clone(),
                )
                .await?
            };
            let hash = state_config_hash(s);
            let mut seeded = serde_json::Map::new();
            for (member, object) in &targets {
                let (schema, table) = split_object(object);
                let fq = format!(
                    "{}.{}.{}",
                    atlatl_ingest::sql::ident(&target.alias),
                    atlatl_ingest::sql::ident(&schema),
                    atlatl_ingest::sql::ident(&table)
                );
                let (st2, sid, mem, h, uk) = (
                    st.clone(),
                    sp.state_id().to_owned(),
                    member.clone(),
                    hash.clone(),
                    uk.clone(),
                );
                let v = engine
                    .with_conn(move |c| {
                        let v = st2.seed_from_target(c, &fq, &uk)?.ok_or_else(|| {
                            AtlatlError::config(format!("{fq} has no non-null `{uk}` to seed from"))
                        })?;
                        let row = StateRow {
                            stream_id: sid,
                            member: mem,
                            kind: StateKind::UpdateKey,
                            value: v.clone(),
                            config_hash: h,
                            run_id: atlatl_core::ids::ulid(),
                            lake_snapshot: None,
                            updated_at: None,
                        };
                        st2.put(c, &row)?;
                        Ok(v)
                    })
                    .await?;
                seeded.insert(member.clone(), v);
            }
            let detail = match (targets.as_slice(), seeded.values().next()) {
                ([(m, _)], Some(v)) if m.is_empty() => json!({"after": v}),
                _ => json!({"after": seeded}),
            };
            log(&engine, st, &op, &args, detail.clone()).await?;
            detail
        }
    };
    Ok(out)
}

/// The engine, the state lake (resolved for `--env`) and its bootstrapped store.
pub(crate) async fn open_store(
    project: &Project,
    args: &StateArgs,
    vars: Arc<dyn EnvSource>,
) -> Result<(Engine, atlatl_conn_ducklake::Lake, LakeState)> {
    let st_cfg = project
        .state
        .as_ref()
        .ok_or_else(|| AtlatlError::config("the project has no `state.lake`"))?;
    let budget = Budget::compute(
        &project.engine,
        None,
        &Detected::detect(),
        &BudgetParams::default(),
    )?;
    let engine = Engine::open(EngineOptions {
        budget,
        extension_dir: args.extension_dir.clone(),
        pool_size: None,
    })?;
    let lake = attach_lake(
        &engine,
        project,
        &st_cfg.lake.value,
        args.env.as_deref(),
        vars,
    )
    .await?;
    let st = LakeState::new(lake.alias.clone());
    let st2 = st.clone();
    engine.with_conn(move |c| st2.bootstrap(c)).await?;
    Ok((engine, lake, st))
}

/// `name` is a member `from.tables: <pattern>` can expand to (07 §4 glob, byte-wise).
fn member_matches(name: &str, pattern: &str, exclude: &[String]) -> bool {
    use atlatl_conn_redshift::catalog::glob_match;
    let (ns, nt) = split_object(name);
    let shape = if pattern.contains('.') {
        let (ps, pt) = split_object(pattern);
        glob_match(&ns, &ps) && glob_match(&nt, &pt)
    } else {
        glob_match(&nt, pattern)
    };
    shape && !exclude.iter().any(|e| name == e || glob_match(name, e))
}

/// A wildcard member's rendered target and window, as the runner renders them after discovery.
pub(crate) fn render_member(
    project: &Project,
    env: Option<&str>,
    vars: &Arc<dyn EnvSource>,
    s: &atlatl_core::config::load::Stream,
    sp: &plan::StreamPlan,
    inst: Option<&plan::InstancePlan>,
    name: &str,
) -> Result<(String, Option<String>)> {
    let (schema, table) = split_object(name);
    let stream_vars = s
        .config
        .vars
        .as_ref()
        .map(|v| serde_json::to_value(v).unwrap_or_default())
        .unwrap_or(json!({}));
    let ctx = Context::load(vars.clone(), &project.root, env)
        .with(
            "member",
            minijinja::Value::from_pairs([
                ("name", name.to_owned()),
                ("schema", schema),
                ("table", table),
            ]),
        )
        .with("vars", atlatl_core::template::to_value(&stream_vars))
        .with(
            "stream",
            minijinja::Value::from_pairs([
                ("id", minijinja::Value::from(sp.id.clone())),
                ("vars", atlatl_core::template::to_value(&stream_vars)),
            ]),
        )
        .with(
            "run",
            minijinja::Value::from_pairs([
                ("id", atlatl_core::ids::ulid()),
                (
                    "started_at",
                    atlatl_core::ids::rfc3339(std::time::SystemTime::now()),
                ),
            ]),
        );
    let ctx = match inst {
        Some(i) => ctx.with(&i.as_name, atlatl_core::template::to_value(&i.item)),
        None => ctx,
    };
    let t = Templates::new();
    let r = |src: &str| {
        t.render(src, &ctx)
            .map_err(|e| AtlatlError::config(e.message))
    };
    Ok((
        r(&sp.object)?,
        s.config.window.as_ref().map(|w| r(&w.value)).transpose()?,
    ))
}

async fn attach_lake(
    engine: &Engine,
    p: &Project,
    name: &str,
    env: Option<&str>,
    vars: Arc<dyn EnvSource>,
) -> Result<atlatl_conn_ducklake::Lake> {
    let c = resolve_connection(p, name, env, vars)?;
    match &c.kind {
        ConnectionKind::Ducklake(d) => atlatl_conn_ducklake::attach(engine, name, d).await,
        ConnectionKind::Duckdb(d) => {
            atlatl_conn_ducklake::plain::attach(engine, name, d, false).await
        }
        _ => Err(AtlatlError::config(format!(
            "`{name}` is not a ducklake or duckdb connection"
        ))),
    }
}

async fn log(
    engine: &Engine,
    st: LakeState,
    op: &StateOp,
    args: &StateArgs,
    detail: Value,
) -> Result<()> {
    let rec = action_record(op.name(), args.env.clone(), format!("{op:?}"), detail);
    engine.with_conn(move |c| st.record_run(c, &rec)).await
}

/// A manual action's `_atlatl.runs` row.
pub(crate) fn action_record(
    kind: &str,
    env: Option<String>,
    op: String,
    detail: Value,
) -> RunRecord {
    RunRecord {
        run_id: atlatl_core::ids::ulid(),
        external_run_id: None,
        env,
        kind: kind.into(),
        status: "finished".into(),
        plan_hash: None,
        atlatl_version: env!("CARGO_PKG_VERSION").into(),
        detail: json!({"op": op, "result": detail}),
    }
}
