//! Orphaned UNLOAD prefixes (`<base><run_id>/…`) left by killed runs, for `atlatl gc` (03 §9).

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use atlatl_core::error::Result;

use crate::s3::S3Client;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StalePrefix {
    pub bucket: String,
    /// `<base><run_id>/`.
    pub prefix: String,
    pub run_id: String,
    pub objects: u64,
    pub bytes: u64,
    pub newest: Option<SystemTime>,
}

/// Run prefixes under `base` whose newest object is at least `older_than` old, sorted. Runs in
/// `active` are never returned.
pub async fn collect(
    s3: &S3Client,
    bucket: &str,
    base: &str,
    older_than: Duration,
    now: SystemTime,
    active: &[String],
) -> Result<Vec<StalePrefix>> {
    let mut runs: BTreeMap<String, StalePrefix> = BTreeMap::new();
    for o in s3.list(bucket, base).await? {
        let Some(rest) = o.key.strip_prefix(base) else {
            continue;
        };
        let Some((run, _)) = rest.split_once('/') else {
            continue;
        };
        if run.is_empty() {
            continue;
        }
        let e = runs.entry(run.to_owned()).or_insert_with(|| StalePrefix {
            bucket: bucket.to_owned(),
            prefix: format!("{base}{run}/"),
            run_id: run.to_owned(),
            objects: 0,
            bytes: 0,
            newest: None,
        });
        e.objects += 1;
        e.bytes += o.size;
        e.newest = e.newest.max(o.last_modified);
    }
    Ok(runs
        .into_values()
        .filter(|p| !active.iter().any(|a| crate::unload::segment(a) == p.run_id))
        .filter(|p| {
            p.newest
                .is_some_and(|t| now.duration_since(t).unwrap_or_default() >= older_than)
        })
        .collect())
}

/// Deletes the prefixes; returns the number of objects removed.
pub async fn remove(s3: &S3Client, stale: &[StalePrefix]) -> Result<u64> {
    let mut n = 0;
    for p in stale {
        n += s3.delete_prefix(&p.bucket, &p.prefix).await?;
    }
    Ok(n)
}
