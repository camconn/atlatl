//! Replication `for_each` (11 §3 (c)): a query iteration expands into independent, atomic
//! instances when the run starts (literal lists expand at plan time). A replication still
//! unexpanded at run time found no items, couldn't list them, or lacks an `--instance` asked for.

use std::sync::Arc;

use atlatl_core::config::model::{ForEachOver, ForEachQuery};
use atlatl_core::error::{AtlatlError, Result};
use atlatl_core::obs::report::ReportHandle;
use atlatl_core::plan::{self, PlanRequest, ReplicationPlan, instance_key};
use atlatl_core::redact::Redactor;
use indexmap::IndexMap;

use crate::run::Output;
use crate::runtime::Runtime;

/// Lists the items of every `for_each.over.query` replication in the plan and replans with one
/// instance per item. A replication whose items can't be listed stays in the plan and fails alone.
pub async fn expand(rt: &mut Runtime, req: &PlanRequest) -> Result<()> {
    let pending: Vec<(String, ForEachQuery, Option<String>)> = rt
        .plan
        .replications
        .iter()
        .filter_map(|r| {
            let f = r.for_each.as_ref()?;
            match &f.over {
                ForEachOver::Query(q) => Some((r.name.clone(), q.clone(), f.key.clone())),
                ForEachOver::List(_) => None,
            }
        })
        .collect();
    if pending.is_empty() {
        return Ok(());
    }
    let mut items: IndexMap<String, Vec<serde_json::Value>> = IndexMap::new();
    let mut session = rt.engine.session()?;
    for (name, q, key) in pending {
        let listed = crate::rest::query_items(rt, &mut session, &q.query, &q.connection, &[], None)
            .await
            .map(|v| v.iter().map(|j| j.to_serde()).collect::<Vec<_>>())
            .and_then(|v| checked(&name, v, key.as_deref()))
            .and_then(|v| selected(&name, v, key.as_deref(), &req.instances));
        match listed {
            Ok(v) => {
                items.insert(name, v);
            }
            Err(e) => {
                rt.for_each_errors.insert(name, e);
            }
        }
    }
    drop(session);
    let req = PlanRequest {
        run_id: Some(rt.run_id.clone()),
        ..req.clone()
    };
    let plan = plan::build_with(
        &rt.project,
        &req,
        rt.vars.clone(),
        Redactor::global(),
        &items,
    )
    .map_err(|d| {
        AtlatlError::config(
            d.errors()
                .map(|e| e.error.to_string())
                .collect::<Vec<_>>()
                .join("; "),
        )
    })?
    .0;
    let engine = rt.report.snapshot().engine;
    let report = ReportHandle::from_plan(&plan, rt.external_run_id.clone());
    report.update(|r| {
        r.run_id.clone_from(&rt.run_id);
        r.engine = engine;
    });
    rt.plan = Arc::new(plan);
    rt.report = report;
    Ok(())
}

/// Every item has a key and no key repeats.
fn checked(
    name: &str,
    items: Vec<serde_json::Value>,
    key: Option<&str>,
) -> Result<Vec<serde_json::Value>> {
    let mut seen = std::collections::HashSet::new();
    for it in &items {
        let k = instance_key(it, key)
            .map_err(|e| AtlatlError::config(format!("replication `{name}`: {e}")))?;
        if !seen.insert(k.clone()) {
            return Err(AtlatlError::data(format!(
                "replication `{name}`: `for_each` key `{k}` repeats in the query's rows"
            ))
            .code("for_each_duplicate_key"));
        }
    }
    Ok(items)
}

/// `--instance` keys of this replication must be among its listed items.
fn selected(
    name: &str,
    items: Vec<serde_json::Value>,
    key: Option<&str>,
    instances: &[String],
) -> Result<Vec<serde_json::Value>> {
    let keys: Vec<String> = items
        .iter()
        .filter_map(|it| instance_key(it, key).ok())
        .collect();
    let missing: Vec<&str> = instances
        .iter()
        .filter_map(|i| plan::parse_instance(i))
        .filter(|(r, k)| *r == name && !keys.iter().any(|x| x == k))
        .map(|(_, k)| k)
        .collect();
    if missing.is_empty() {
        return Ok(items);
    }
    Err(AtlatlError::config(format!(
        "--instance: replication `{name}` has no instance {} (its query listed: {})",
        missing
            .iter()
            .map(|k| format!("`{k}`"))
            .collect::<Vec<_>>()
            .join(", "),
        if keys.is_empty() {
            "nothing".to_owned()
        } else {
            keys.join(", ")
        }
    ))
    .code("unknown_instance"))
}

/// A `for_each` replication left unexpanded: its listing error, else nothing to do.
pub fn unexpanded(rt: &Runtime, rep: &ReplicationPlan) -> Result<Output> {
    rt.report.start_replication(&rep.name);
    match rt.for_each_errors.get(&rep.name) {
        Some(e) => Err(e.clone()),
        None => Ok(Output {
            up_to_date: true,
            lake_snapshot: None,
        }),
    }
}
