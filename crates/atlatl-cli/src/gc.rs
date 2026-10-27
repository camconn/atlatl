//! `atlatl gc`: removes run-scoped leftovers a killed run couldn't clean up (03 §9). The kill path
//! never cleans; this and bucket lifecycle rules do.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// Run temp dirs (`atlatl-run-*`) and DuckDB spill files (`duckdb_temp_*`) older than `older_than`.
pub fn collect(dir: &Path, older_than: Duration, now: SystemTime) -> std::io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e),
    };
    for e in rd {
        let e = e?;
        let name = e.file_name().to_string_lossy().into_owned();
        if !(name.starts_with("atlatl-run-") || name.starts_with("duckdb_temp_")) {
            continue;
        }
        let age = now
            .duration_since(e.metadata()?.modified()?)
            .unwrap_or_default();
        if age >= older_than {
            out.push(e.path());
        }
    }
    out.sort();
    Ok(out)
}

pub fn remove(paths: &[PathBuf]) -> std::io::Result<u64> {
    let mut n = 0;
    for p in paths {
        if p.is_dir() {
            std::fs::remove_dir_all(p)?;
        } else {
            std::fs::remove_file(p)?;
        }
        n += 1;
    }
    Ok(n)
}

/// Stale UNLOAD prefixes (`<unload.prefix><run_id>/`) of every redshift connection with `unload`,
/// removed unless `dry_run`. Connections resolve in the base environment.
pub fn redshift(
    p: &atlatl_core::config::load::Project,
    older_than: Duration,
    dry_run: bool,
) -> Result<u64, String> {
    use atlatl_core::config::model::ConnectionKind;
    let names: Vec<String> = p
        .connections
        .values()
        .filter(|c| c.node.get("type").and_then(|t| t.as_str()) == Some("redshift"))
        .map(|c| c.name.clone())
        .collect();
    if names.is_empty() {
        return Ok(0);
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    rt.block_on(async {
        let mut n = 0;
        for name in names {
            let vars = std::sync::Arc::new(atlatl_core::template::ProcessEnv);
            let c = atlatl_run::runtime::resolve_connection(p, &name, None, vars)
                .map_err(|e| e.to_string())?;
            let ConnectionKind::Redshift(r) = &c.kind else {
                continue;
            };
            if r.unload.is_none() {
                continue;
            }
            let rs =
                atlatl_conn_redshift::Redshift::new(r, atlatl_conn_redshift::Options::default())
                    .map_err(|e| format!("{name}: {e}"))?;
            let stale = rs
                .stale_prefixes(older_than, &[])
                .await
                .map_err(|e| format!("{name}: {e}"))?;
            for s in &stale {
                println!(
                    "{}s3://{}/{} ({name}: {} objects, {} bytes)",
                    if dry_run {
                        "would remove "
                    } else {
                        "removing "
                    },
                    s.bucket,
                    s.prefix,
                    s.objects,
                    s.bytes
                );
            }
            if !dry_run {
                n += rs
                    .remove_prefixes(&stale)
                    .await
                    .map_err(|e| format!("{name}: {e}"))?;
            }
        }
        Ok(n)
    })
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::*;

    #[test]
    fn only_old_atlatl_artifacts() {
        let d = tempfile::tempdir().expect("tmp");
        let old = SystemTime::now() - Duration::from_secs(3 * 86_400);
        for name in [
            "atlatl-run-01OLD",
            "duckdb_temp_storage-1.tmp",
            "unrelated",
            "atlatl-run-01NEW",
        ] {
            let p = d.path().join(name);
            if name.contains('.') || name == "unrelated" {
                File::create(&p).expect("file");
            } else {
                std::fs::create_dir(&p).expect("dir");
                File::create(p.join("x")).expect("inner");
            }
            if name != "atlatl-run-01NEW" {
                File::options()
                    .write(true)
                    .open(&p)
                    .or_else(|_| File::open(&p))
                    .expect("open")
                    .set_modified(old)
                    .expect("mtime");
            }
        }
        let found =
            collect(d.path(), Duration::from_secs(86_400), SystemTime::now()).expect("collect");
        let names: Vec<String> = found
            .iter()
            .map(|p| p.file_name().expect("name").to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, ["atlatl-run-01OLD", "duckdb_temp_storage-1.tmp"]);
        assert_eq!(remove(&found).expect("remove"), 2);
        assert!(d.path().join("atlatl-run-01NEW").exists() && d.path().join("unrelated").exists());
        assert!(
            collect(&d.path().join("missing"), Duration::ZERO, SystemTime::now())
                .expect("missing")
                .is_empty()
        );
    }
}
