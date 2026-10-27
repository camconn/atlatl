//! Multi-hop replications (`then:`, 11 §3 (a)/(b), 03 §2): hops run upstream first, each through
//! the runner for its own source → target and committed on its own. A failed hop leaves the hops
//! before it committed and skips its descendants; sibling hops still run.

use atlatl_core::error::{AtlatlError, ErrorClass, Result};
use atlatl_core::obs::events::{EventBody, HopCommitted};
use atlatl_core::obs::report::{HopStatus, ReportError, StreamStatus};
use atlatl_core::plan::ReplicationPlan;
use tokio_util::sync::CancellationToken;

use crate::run::{Output, dispatch};
use crate::runtime::Runtime;

pub async fn run_unit(
    rt: &Runtime,
    rep: &ReplicationPlan,
    attempt: u32,
    cancel: CancellationToken,
) -> Result<Output> {
    rt.report.start_replication(&rep.name);
    // A hop committed by an earlier attempt of this run (`replication_retries`) isn't redone.
    let before: Vec<(HopStatus, Option<u64>)> = rt
        .report
        .replication(&rep.name, |r| {
            r.hops.iter().map(|h| (h.status, h.lake_snapshot)).collect()
        })
        .unwrap_or_default();
    let committed = |i: usize| before.get(i).is_some_and(|h| h.0 == HopStatus::Committed);
    let mut state: Vec<HopStatus> = (0..rep.hops.len())
        .map(|i| {
            if committed(i) {
                HopStatus::Committed
            } else {
                HopStatus::NotStarted
            }
        })
        .collect();
    let mut first_err: Option<AtlatlError> = None;
    let mut up_to_date = true;
    let mut lake_snapshot = before
        .first()
        .filter(|_| committed(0))
        .and_then(|h| h.1)
        .map(|s| s as i64);
    for (i, hop) in rep.hops.iter().enumerate() {
        if hop.streams.is_empty() || state[i] == HopStatus::Committed {
            up_to_date &= state[i] != HopStatus::Committed;
            continue;
        }
        if blocked(rep, &state, i) {
            continue;
        }
        let plan = rep.hop_plan(i);
        let r = match atlatl_core::fault::hit_async("hop.start", &format!("{}#{i}", rep.name)).await
        {
            Ok(()) => dispatch(rt, &[&plan], attempt, cancel.clone()).await,
            Err(e) => Err(e),
        };
        rt.report.replication(&rep.name, |x| x.attempts = attempt);
        match r {
            Ok(o) => {
                state[i] = HopStatus::Committed;
                up_to_date &= o.up_to_date;
                if i == 0 {
                    lake_snapshot = o.lake_snapshot;
                }
                let status = if o.up_to_date {
                    StreamStatus::UpToDate
                } else {
                    StreamStatus::Succeeded
                };
                settle(
                    rt,
                    rep,
                    i,
                    HopStatus::Committed,
                    status,
                    o.lake_snapshot,
                    None,
                );
                rt.events.emit(EventBody::HopCommitted(HopCommitted {
                    replication: rep.name.clone(),
                    hop: i as u32,
                    target: hop.target.name.clone(),
                    lake_snapshot: o.lake_snapshot.map(|s| s as u64),
                }));
            }
            Err(e) => {
                state[i] = HopStatus::Failed;
                settle(
                    rt,
                    rep,
                    i,
                    HopStatus::Failed,
                    StreamStatus::Failed,
                    None,
                    Some(&e),
                );
                if e.class == ErrorClass::Killed {
                    return Err(e);
                }
                first_err.get_or_insert(e);
            }
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(Output {
            up_to_date,
            lake_snapshot,
        }),
    }
}

/// An upstream hop that ran in this attempt failed (or was itself blocked).
fn blocked(rep: &ReplicationPlan, state: &[HopStatus], hop: usize) -> bool {
    let mut p = rep.hops[hop].parent;
    while let Some(x) = p {
        if !rep.hops[x].streams.is_empty() && state[x] != HopStatus::Committed {
            return true;
        }
        p = rep.hops[x].parent;
    }
    false
}

fn settle(
    rt: &Runtime,
    rep: &ReplicationPlan,
    hop: usize,
    status: HopStatus,
    streams: StreamStatus,
    lake_snapshot: Option<i64>,
    error: Option<&AtlatlError>,
) {
    rt.report.replication(&rep.name, |r| {
        if let Some(h) = r.hops.get_mut(hop) {
            h.status = status;
            h.lake_snapshot = lake_snapshot.map(|s| s as u64);
            h.error = error.map(ReportError::from);
        }
        for id in &rep.hops[hop].streams {
            // A committed hop keeps the runner's per-stream `up_to_date`.
            if let Some(s) = r.stream_mut(id)
                && !(streams == StreamStatus::Succeeded && s.status == StreamStatus::UpToDate)
            {
                s.status = streams;
            }
        }
    });
}
