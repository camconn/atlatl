//! Memory and thread budget (spec 01 §5): container limits, config overrides, DuckDB's share.

use std::fs;
use std::path::{Path, PathBuf};

use atlatl_core::AtlatlError;
use atlatl_core::config::model::{EngineConfig, EngineOverrides, MemoryMode};
use atlatl_core::config::types::Auto;
use atlatl_core::error::Result;
use serde::Serialize;

const MIB: u64 = 1 << 20;
/// `memory_mode: low` releases freed allocator memory this often while an engine call runs.
pub const PURGE_EVERY_MS: u64 = 250;
/// cgroup v1 reports "unlimited" as a page-rounded i64::MAX.
const V1_UNLIMITED: u64 = 1 << 60;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    CgroupV2,
    CgroupV1,
    Host,
    Config,
}

/// What the container (or host) provides.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Detected {
    pub memory: Option<u64>,
    pub memory_source: Source,
    pub cpus: u32,
    pub cpu_source: Source,
}

impl Detected {
    pub fn detect() -> Self {
        let host = std::thread::available_parallelism().map_or(1, |n| n.get() as u32);
        let mut d = Self::from_root(Path::new("/"), host);
        if d.memory.is_none() {
            d.memory = os_memory();
        }
        d
    }

    /// Reads `<root>/proc/self/cgroup`, `<root>/proc/meminfo` and `<root>/sys/fs/cgroup/…`.
    pub fn from_root(root: &Path, host_cpus: u32) -> Self {
        let host_mem = host_memory(root);
        let membership = read(&root.join("proc/self/cgroup")).unwrap_or_default();
        let cg = root.join("sys/fs/cgroup");
        let (mem, cpu, source) = if cg.join("cgroup.controllers").is_file() {
            let path = membership
                .lines()
                .find_map(|l| l.strip_prefix("0::"))
                .unwrap_or("/");
            let dirs = ancestors(&cg, path);
            let mem = dirs
                .iter()
                .filter_map(|d| read(&d.join("memory.max")).and_then(|s| s.trim().parse().ok()))
                .min();
            let cpu = dirs
                .iter()
                .filter_map(|d| read(&d.join("cpu.max")).and_then(|s| parse_cpu_max(&s)))
                .reduce(f64::min);
            (mem, cpu, Source::CgroupV2)
        } else {
            let v1_path = |ctl: &str| {
                membership.lines().find_map(|l| {
                    let mut it = l.splitn(3, ':');
                    let (_, ctls, path) = (it.next()?, it.next()?, it.next()?);
                    ctls.split(',').any(|c| c == ctl).then_some(path)
                })
            };
            let mem = v1_path("memory").and_then(|p| {
                ancestors(&cg.join("memory"), p)
                    .iter()
                    .filter_map(|d| {
                        read(&d.join("memory.limit_in_bytes")).and_then(|s| s.trim().parse().ok())
                    })
                    .filter(|&v: &u64| v < V1_UNLIMITED)
                    .min()
            });
            let cpu = v1_path("cpu").and_then(|p| {
                ["cpu,cpuacct", "cpu", "cpuacct,cpu"]
                    .iter()
                    .map(|m| cg.join(m))
                    .find(|m| m.is_dir())
                    .and_then(|m| {
                        ancestors(&m, p)
                            .iter()
                            .filter_map(|d| {
                                let q = read(&d.join("cpu.cfs_quota_us"))?
                                    .trim()
                                    .parse::<i64>()
                                    .ok()?;
                                let per = read(&d.join("cpu.cfs_period_us"))?
                                    .trim()
                                    .parse::<i64>()
                                    .ok()?;
                                (q > 0 && per > 0).then(|| q as f64 / per as f64)
                            })
                            .reduce(f64::min)
                    })
            });
            (mem, cpu, Source::CgroupV1)
        };

        let (memory, memory_source) = match (mem, host_mem) {
            (Some(m), Some(h)) if h <= m => (Some(h), Source::Host),
            (Some(m), _) => (Some(m), source),
            (None, h) => (h, Source::Host),
        };
        let (cpus, cpu_source) = match cpu {
            Some(q) if (q.ceil() as u32) < host_cpus => ((q.ceil() as u32).max(1), source),
            _ => (host_cpus.max(1), Source::Host),
        };
        Self {
            memory,
            memory_source,
            cpus,
            cpu_source,
        }
    }
}

/// Physical memory where there's no `/proc` (macOS: `hw.memsize`).
#[cfg(target_os = "macos")]
fn os_memory() -> Option<u64> {
    let mut v: u64 = 0;
    let mut len = std::mem::size_of::<u64>();
    // SAFETY: `hw.memsize` is a u64; `v` and `len` describe that buffer.
    let r = unsafe {
        libc::sysctlbyname(
            c"hw.memsize".as_ptr(),
            (&raw mut v).cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    (r == 0 && v > 0).then_some(v)
}

#[cfg(not(target_os = "macos"))]
fn os_memory() -> Option<u64> {
    None
}

fn read(p: &Path) -> Option<String> {
    fs::read_to_string(p).ok()
}

fn host_memory(root: &Path) -> Option<u64> {
    read(&root.join("proc/meminfo"))?.lines().find_map(|l| {
        let kb: u64 = l
            .strip_prefix("MemTotal:")?
            .trim()
            .strip_suffix("kB")?
            .trim()
            .parse()
            .ok()?;
        Some(kb * 1024)
    })
}

fn parse_cpu_max(s: &str) -> Option<f64> {
    let mut it = s.split_whitespace();
    let quota = it.next()?;
    let period: f64 = it.next().map_or(Some(100_000.0), |p| p.parse().ok())?;
    let quota: f64 = quota.parse().ok()?;
    (quota > 0.0 && period > 0.0).then_some(quota / period)
}

/// `mount/path` up to `mount`. Inside a container's cgroup namespace the recorded path may not
/// exist under the mount, in which case only the mount root is read.
fn ancestors(mount: &Path, path: &str) -> Vec<PathBuf> {
    let mut leaf = mount.to_path_buf();
    for part in path.split('/').filter(|p| !p.is_empty() && *p != "..") {
        leaf.push(part);
    }
    if !leaf.is_dir() {
        return vec![mount.to_path_buf()];
    }
    let mut out = vec![];
    let mut d = leaf.as_path();
    loop {
        out.push(d.to_path_buf());
        if d == mount {
            break;
        }
        match d.parent() {
            Some(p) if p.starts_with(mount) => d = p,
            _ => break,
        }
    }
    out
}

/// RSS a memory mode reserves outside DuckDB's `memory_limit`: `fixed + threads × per_thread`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct Overhead {
    pub fixed: u64,
    pub per_thread: u64,
}

impl Overhead {
    pub fn reserve(&self, threads: u32) -> u64 {
        self.fixed + u64::from(threads) * self.per_thread
    }
}

/// Calibrated with the S3 harness (a chunked 10M-row merge; spike S3 §6.2): each mode's
/// overhead covers its worst RSS outside `memory_limit` at 1, 2 and 4 threads by at least 15%.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct BudgetParams {
    /// `memory: auto` takes this fraction of the detected limit.
    pub auto_memory_fraction: f64,
    /// `memory_mode: high`.
    pub high: Overhead,
    /// `memory_mode: low`.
    pub low: Overhead,
    pub min_memory_limit: u64,
    /// `channel_bytes` when unset, capped at `memory / 8`.
    pub default_channel_bytes: u64,
}

impl BudgetParams {
    pub fn overhead(&self, mode: MemoryMode) -> Overhead {
        match mode {
            MemoryMode::High => self.high,
            MemoryMode::Low => self.low,
        }
    }
}

impl Default for BudgetParams {
    fn default() -> Self {
        Self {
            auto_memory_fraction: 0.85,
            high: Overhead {
                fixed: 550 * MIB,
                per_thread: 50 * MIB,
            },
            low: Overhead {
                fixed: 220 * MIB,
                per_thread: 25 * MIB,
            },
            min_memory_limit: 128 * MIB,
            default_channel_bytes: 256 * MIB,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LimitRule {
    /// `memory − channel_bytes − overhead.fixed − threads × overhead.per_thread`.
    Headroom,
    /// `memory × duckdb_share`, set explicitly.
    Share,
}

/// The engine's resolved budget; serialised into the run report.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Budget {
    pub memory: u64,
    pub memory_source: Source,
    pub threads: u32,
    pub threads_source: Source,
    pub memory_mode: MemoryMode,
    /// The mode's reserve outside `memory_limit` (unused under [`LimitRule::Share`]).
    pub overhead: Overhead,
    /// Periodic [`crate::alloc::release`] during engine calls (`low` only).
    pub purge_every_ms: Option<u64>,
    /// DuckDB `memory_limit`.
    pub memory_limit: u64,
    pub memory_limit_rule: LimitRule,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duckdb_share: Option<f64>,
    /// Raised to `min_memory_limit`: the budget is too small for the thread count.
    pub clamped: bool,
    pub channel_bytes: u64,
    /// This process's spill directory (a per-process subdirectory of the configured one).
    pub temp_directory: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_temp_directory_size: Option<u64>,
    pub params: BudgetParams,
}

impl Budget {
    /// `ov` (a replication's `engine:`) wins over `cfg` (the project's).
    pub fn compute(
        cfg: &EngineConfig,
        ov: Option<&EngineOverrides>,
        det: &Detected,
        p: &BudgetParams,
    ) -> Result<Self> {
        let memory_cfg = ov.and_then(|o| o.memory).or(cfg.memory).unwrap_or_default();
        let threads_cfg = ov
            .and_then(|o| o.threads)
            .or(cfg.threads)
            .unwrap_or_default();
        let share = ov.and_then(|o| o.duckdb_share).or(cfg.duckdb_share);
        let memory_mode = ov
            .and_then(|o| o.memory_mode)
            .or(cfg.memory_mode)
            .unwrap_or_default();
        let overhead = p.overhead(memory_mode);

        let (memory, memory_source) = match memory_cfg {
            Auto::Value(b) => (b.0, Source::Config),
            Auto::Auto => {
                let m = det.memory.ok_or_else(|| {
                    AtlatlError::config(
                        "cannot detect the memory limit (no cgroup limit, /proc/meminfo or hw.memsize); set engine.memory",
                    )
                })?;
                (
                    (m as f64 * p.auto_memory_fraction) as u64,
                    det.memory_source,
                )
            }
        };
        if memory == 0 {
            return Err(AtlatlError::config("engine.memory must be positive"));
        }
        let (threads, threads_source) = match threads_cfg {
            Auto::Value(0) => return Err(AtlatlError::config("engine.threads must be at least 1")),
            Auto::Value(t) => (t, Source::Config),
            Auto::Auto => (det.cpus.max(1), det.cpu_source),
        };
        if let Some(s) = share
            && !(s > 0.0 && s <= 1.0)
        {
            return Err(AtlatlError::config(format!(
                "engine.duckdb_share must be in (0, 1], got {s}"
            )));
        }
        let channel_bytes = cfg
            .channel_bytes
            .map_or_else(|| p.default_channel_bytes.min(memory / 8), |b| b.0);

        let (raw, rule) = match share {
            Some(s) => ((memory as f64 * s) as u64, LimitRule::Share),
            None => (
                memory
                    .saturating_sub(channel_bytes)
                    .saturating_sub(overhead.reserve(threads)),
                LimitRule::Headroom,
            ),
        };
        let clamped = raw < p.min_memory_limit;
        let memory_limit = raw.max(p.min_memory_limit);

        let base = ov
            .and_then(|o| o.temp_directory.as_ref())
            .or(cfg.temp_directory.as_ref())
            .map_or_else(
                || std::env::temp_dir().join("atlatl").join("spill"),
                PathBuf::from,
            );
        Ok(Self {
            memory,
            memory_source,
            threads,
            threads_source,
            memory_mode,
            overhead,
            purge_every_ms: (memory_mode == MemoryMode::Low).then_some(PURGE_EVERY_MS),
            memory_limit,
            memory_limit_rule: rule,
            duckdb_share: share,
            clamped,
            channel_bytes,
            temp_directory: base.join(format!("atlatl-{}", std::process::id())),
            max_temp_directory_size: ov
                .and_then(|o| o.max_temp_directory_size)
                .or(cfg.max_temp_directory_size)
                .map(|b| b.0),
            params: p.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use atlatl_core::config::types::ByteSize;

    use super::*;

    const GIB: u64 = 1 << 30;

    struct Root(tempfile::TempDir);

    impl Root {
        fn new() -> Self {
            let r = Self(tempfile::tempdir().expect("tempdir"));
            r.put(
                "proc/meminfo",
                "MemTotal:       16384000 kB\nMemFree:  1 kB\n",
            );
            r
        }
        fn put(&self, rel: &str, text: &str) -> &Self {
            let p = self.0.path().join(rel);
            fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
            fs::write(p, text).expect("write");
            self
        }
        fn detect(&self, host_cpus: u32) -> Detected {
            Detected::from_root(self.0.path(), host_cpus)
        }
    }

    const HOST_MEM: u64 = 16_384_000 * 1024;

    #[test]
    fn v2_container_root() {
        let r = Root::new();
        r.put("proc/self/cgroup", "0::/\n")
            .put("sys/fs/cgroup/cgroup.controllers", "cpu memory")
            .put("sys/fs/cgroup/memory.max", "2147483648\n")
            .put("sys/fs/cgroup/cpu.max", "150000 100000\n");
        assert_eq!(
            r.detect(16),
            Detected {
                memory: Some(2 * GIB),
                memory_source: Source::CgroupV2,
                cpus: 2,
                cpu_source: Source::CgroupV2,
            }
        );
    }

    #[test]
    fn v2_nested_takes_tightest_ancestor() {
        let r = Root::new();
        r.put("proc/self/cgroup", "0::/kubepods/pod1/c1\n")
            .put("sys/fs/cgroup/cgroup.controllers", "")
            .put("sys/fs/cgroup/kubepods/memory.max", "4294967296")
            .put("sys/fs/cgroup/kubepods/pod1/memory.max", "1073741824")
            .put("sys/fs/cgroup/kubepods/pod1/c1/memory.max", "max")
            .put("sys/fs/cgroup/kubepods/pod1/cpu.max", "max 100000")
            .put("sys/fs/cgroup/kubepods/pod1/c1/cpu.max", "400000 100000");
        let d = r.detect(32);
        assert_eq!((d.memory, d.memory_source), (Some(GIB), Source::CgroupV2));
        assert_eq!((d.cpus, d.cpu_source), (4, Source::CgroupV2));
    }

    #[test]
    fn v2_unlimited_falls_back_to_host() {
        let r = Root::new();
        r.put("proc/self/cgroup", "0::/user.slice\n")
            .put("sys/fs/cgroup/cgroup.controllers", "")
            .put("sys/fs/cgroup/user.slice/memory.max", "max\n")
            .put("sys/fs/cgroup/user.slice/cpu.max", "max 100000\n");
        let d = r.detect(8);
        assert_eq!((d.memory, d.memory_source), (Some(HOST_MEM), Source::Host));
        assert_eq!((d.cpus, d.cpu_source), (8, Source::Host));
    }

    #[test]
    fn v2_limit_above_host_ram_is_host() {
        let r = Root::new();
        r.put("proc/self/cgroup", "0::/\n")
            .put("sys/fs/cgroup/cgroup.controllers", "")
            .put("sys/fs/cgroup/memory.max", &(64 * GIB).to_string())
            .put("sys/fs/cgroup/cpu.max", "3200000 100000");
        let d = r.detect(4);
        assert_eq!((d.memory, d.memory_source), (Some(HOST_MEM), Source::Host));
        assert_eq!((d.cpus, d.cpu_source), (4, Source::Host));
    }

    #[test]
    fn v2_namespace_path_missing_reads_mount_root() {
        let r = Root::new();
        r.put("proc/self/cgroup", "0::/../../host/path\n")
            .put("sys/fs/cgroup/cgroup.controllers", "")
            .put("sys/fs/cgroup/memory.max", "536870912")
            .put("sys/fs/cgroup/cpu.max", "50000 100000");
        let d = r.detect(8);
        assert_eq!(d.memory, Some(512 * MIB));
        assert_eq!(d.cpus, 1);
    }

    #[test]
    fn v1_limits() {
        let r = Root::new();
        r.put(
            "proc/self/cgroup",
            "12:memory:/docker/abc\n11:cpu,cpuacct:/docker/abc\n1:name=systemd:/docker/abc\n",
        )
        .put(
            "sys/fs/cgroup/memory/docker/abc/memory.limit_in_bytes",
            "3221225472\n",
        )
        .put(
            "sys/fs/cgroup/memory/memory.limit_in_bytes",
            "9223372036854771712\n",
        )
        .put(
            "sys/fs/cgroup/cpu,cpuacct/docker/abc/cpu.cfs_quota_us",
            "250000\n",
        )
        .put(
            "sys/fs/cgroup/cpu,cpuacct/docker/abc/cpu.cfs_period_us",
            "100000\n",
        );
        assert_eq!(
            r.detect(16),
            Detected {
                memory: Some(3 * GIB),
                memory_source: Source::CgroupV1,
                cpus: 3,
                cpu_source: Source::CgroupV1,
            }
        );
    }

    #[test]
    fn v1_unlimited() {
        let r = Root::new();
        r.put("proc/self/cgroup", "4:memory:/\n3:cpu,cpuacct:/\n")
            .put(
                "sys/fs/cgroup/memory/memory.limit_in_bytes",
                "9223372036854771712",
            )
            .put("sys/fs/cgroup/cpu,cpuacct/cpu.cfs_quota_us", "-1")
            .put("sys/fs/cgroup/cpu,cpuacct/cpu.cfs_period_us", "100000");
        let d = r.detect(6);
        assert_eq!((d.memory, d.memory_source), (Some(HOST_MEM), Source::Host));
        assert_eq!((d.cpus, d.cpu_source), (6, Source::Host));
    }

    #[test]
    fn nothing_readable() {
        let r = Root(tempfile::tempdir().expect("tempdir"));
        let d = r.detect(0);
        assert_eq!(d.memory, None);
        assert_eq!(d.cpus, 1);
        let e = Budget::compute(&EngineConfig::default(), None, &d, &BudgetParams::default())
            .expect_err("no memory");
        assert_eq!(e.class, atlatl_core::ErrorClass::Config);
    }

    #[test]
    fn detect_on_this_host() {
        let d = Detected::detect();
        assert!(d.memory.is_some_and(|m| m > 0));
        assert!(d.cpus >= 1);
    }

    fn det(mem: u64, cpus: u32) -> Detected {
        Detected {
            memory: Some(mem),
            memory_source: Source::CgroupV2,
            cpus,
            cpu_source: Source::CgroupV2,
        }
    }

    #[test]
    fn headroom_formula() {
        let p = BudgetParams::default();
        let b =
            Budget::compute(&EngineConfig::default(), None, &det(8 * GIB, 4), &p).expect("budget");
        let memory = (8.0 * GIB as f64 * 0.85) as u64;
        assert_eq!(b.memory, memory);
        assert_eq!(b.threads, 4);
        assert_eq!(b.channel_bytes, 256 * MIB);
        assert_eq!(b.memory_limit, memory - 256 * MIB - p.high.reserve(4));
        assert_eq!(b.memory_limit_rule, LimitRule::Headroom);
        assert_eq!((b.memory_mode, b.overhead), (MemoryMode::High, p.high));
        assert_eq!(b.purge_every_ms, None);
        assert!(!b.clamped);
        assert!(
            b.temp_directory
                .ends_with(format!("atlatl-{}", std::process::id()))
        );
    }

    #[test]
    fn small_budget_caps_channel_and_clamps() {
        let p = BudgetParams::default();
        let b =
            Budget::compute(&EngineConfig::default(), None, &det(GIB / 2, 4), &p).expect("budget");
        assert_eq!(b.channel_bytes, b.memory / 8);
        assert!(b.clamped);
        assert_eq!(b.memory_limit, p.min_memory_limit);
    }

    /// Worst peak RSS minus `memory_limit` for the harness's 10M-row merge, per mode and thread
    /// count (spike S3 §6.2).
    const MEASURED_OUTSIDE_MIB: [(MemoryMode, [(u32, u64); 3]); 2] = [
        (MemoryMode::High, [(1, 405), (2, 536), (4, 520)]),
        (MemoryMode::Low, [(1, 200), (2, 166), (4, 207)]),
    ];

    #[test]
    fn each_mode_covers_its_measured_overhead_with_margin() {
        let p = BudgetParams::default();
        for (mode, measured) in MEASURED_OUTSIDE_MIB {
            for (t, mib) in measured {
                let reserved = p.overhead(mode).reserve(t);
                assert!(
                    reserved * 100 >= mib * MIB * 115,
                    "{mode:?}, {t} threads: {} MiB reserved for {mib} MiB measured",
                    reserved / MIB
                );
            }
        }
        for t in [1, 2, 4, 8] {
            assert!(p.low.reserve(t) < p.high.reserve(t), "{t} threads");
        }
    }

    #[test]
    fn memory_mode_selects_the_overhead() {
        let p = BudgetParams::default();
        let cfg = EngineConfig {
            memory: Some(Auto::Value(ByteSize(2 * GIB))),
            threads: Some(Auto::Value(2)),
            channel_bytes: Some(ByteSize(0)),
            memory_mode: Some(MemoryMode::Low),
            ..Default::default()
        };
        let b = Budget::compute(&cfg, None, &det(GIB, 1), &p).expect("budget");
        assert_eq!((b.memory_mode, b.overhead), (MemoryMode::Low, p.low));
        assert_eq!(b.memory_limit, 2 * GIB - p.low.reserve(2));
        assert_eq!(b.purge_every_ms, Some(PURGE_EVERY_MS));

        let ov = EngineOverrides {
            memory_mode: Some(MemoryMode::High),
            ..Default::default()
        };
        let b = Budget::compute(&cfg, Some(&ov), &det(GIB, 1), &p).expect("budget");
        assert_eq!((b.memory_mode, b.overhead), (MemoryMode::High, p.high));
        assert_eq!(b.memory_limit, 2 * GIB - p.high.reserve(2));
        assert_eq!(b.purge_every_ms, None);

        let ov = EngineOverrides {
            memory_mode: Some(MemoryMode::Low),
            ..Default::default()
        };
        let b = Budget::compute(&EngineConfig::default(), Some(&ov), &det(4 * GIB, 4), &p)
            .expect("budget");
        assert_eq!(b.memory_mode, MemoryMode::Low);
    }

    #[test]
    fn memory_mode_parses_from_project_and_replication_config() {
        let lake = atlatl_testkit::LocalLake::file().expect("lake");
        let stream = "streams: [{id: g/a, mode: incremental, from: {table: a}, object: o}]";
        let load = |engine: &str, rep_engine: &str| {
            let p = atlatl_testkit::TempProject::new()
                .and_then(|p| p.project(&lake.connection_yaml("lake", "lake"), engine))
                .and_then(|p| {
                    p.replication(
                        "a",
                        &format!(
                            "replications:\n  a: {{source: lake, target: lake, {rep_engine}{stream}}}\n"
                        ),
                    )
                })
                .expect("project");
            let (project, diags) = atlatl_core::config::load::load(p.path());
            let errors: Vec<String> = diags.errors().map(|d| format!("{d:?}")).collect();
            (project, errors)
        };

        let (p, errors) = load("engine: {memory_mode: low}\n", "");
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(p.engine.memory_mode, Some(MemoryMode::Low));
        let (p, errors) = load("", "engine: {memory_mode: high}, ");
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(p.engine.memory_mode, None);
        let r = p.replication("a").expect("replication");
        assert_eq!(
            r.engine.as_ref().and_then(|e| e.memory_mode),
            Some(MemoryMode::High)
        );

        for (engine, rep_engine, want) in [
            ("engine: {memory_mode: medium}\n", "", "medium"),
            ("engine: {memory_modes: low}\n", "", "memory_modes"),
            ("", "engine: {memory_mode: fast}, ", "fast"),
        ] {
            let (_, errors) = load(engine, rep_engine);
            assert!(
                errors.iter().any(|e| e.contains(want)),
                "{want}: {errors:?}"
            );
        }
    }

    #[test]
    fn small_pods_keep_a_usable_limit() {
        let p = BudgetParams::default();
        let auto = |mode| EngineConfig {
            memory_mode: Some(mode),
            ..Default::default()
        };
        for (mode, pod2, pod3) in [(MemoryMode::High, 800, 1550), (MemoryMode::Low, 1200, 2000)] {
            let b = Budget::compute(&auto(mode), None, &det(2 * GIB, 2), &p).expect("budget");
            assert!(!b.clamped);
            assert!(
                b.memory_limit >= pod2 * MIB,
                "{mode:?}: {}",
                b.memory_limit / MIB
            );
            let b = Budget::compute(&auto(mode), None, &det(3 * GIB, 4), &p).expect("budget");
            assert!(!b.clamped);
            assert!(
                b.memory_limit >= pod3 * MIB,
                "{mode:?}: {}",
                b.memory_limit / MIB
            );
        }

        // The memory harness's default (S3 §6.2): its 1.1M-row chunks need more than ~310 MiB at
        // 2 threads and ~390 MiB at 4.
        for mode in [MemoryMode::High, MemoryMode::Low] {
            for t in [1, 2, 4] {
                let cfg = EngineConfig {
                    memory: Some(Auto::Value(ByteSize(1536 * MIB))),
                    threads: Some(Auto::Value(t)),
                    memory_mode: Some(mode),
                    ..Default::default()
                };
                let b = Budget::compute(&cfg, None, &det(GIB, 1), &p).expect("budget");
                assert_eq!(
                    b.memory_limit,
                    (1536 - 192) * MIB - p.overhead(mode).reserve(t)
                );
                assert!(
                    b.memory_limit >= 450 * MIB,
                    "{mode:?} {t}: {}",
                    b.memory_limit / MIB
                );
            }
        }
    }

    #[test]
    fn explicit_memory_below_the_overheads_clamps_to_the_floor() {
        let p = BudgetParams::default();
        let tight = |memory: u64, threads: u32| EngineConfig {
            memory: Some(Auto::Value(ByteSize(memory))),
            threads: Some(Auto::Value(threads)),
            channel_bytes: Some(ByteSize(0)),
            ..Default::default()
        };
        let edge = p.high.reserve(2) + p.min_memory_limit;
        let b = Budget::compute(&tight(edge, 2), None, &det(GIB, 1), &p).expect("budget");
        assert_eq!((b.memory_limit, b.clamped), (p.min_memory_limit, false));
        let b = Budget::compute(&tight(edge - 1, 2), None, &det(GIB, 1), &p).expect("budget");
        assert_eq!((b.memory_limit, b.clamped), (p.min_memory_limit, true));
        let b = Budget::compute(&tight(512 * MIB, 4), None, &det(GIB, 1), &p).expect("budget");
        assert_eq!((b.memory_limit, b.clamped), (p.min_memory_limit, true));
    }

    #[test]
    fn explicit_config_and_overrides() {
        let cfg = EngineConfig {
            memory: Some(Auto::Value(ByteSize(4 * GIB))),
            threads: Some(Auto::Value(8)),
            channel_bytes: Some(ByteSize(64 * MIB)),
            temp_directory: Some("/spill".into()),
            max_temp_directory_size: Some(ByteSize(10 * GIB)),
            ..Default::default()
        };
        let p = BudgetParams::default();
        let b = Budget::compute(&cfg, None, &det(GIB, 1), &p).expect("budget");
        assert_eq!((b.memory, b.memory_source), (4 * GIB, Source::Config));
        assert_eq!((b.threads, b.threads_source), (8, Source::Config));
        assert_eq!(b.memory_limit, 4 * GIB - 64 * MIB - p.high.reserve(8));
        assert!(b.temp_directory.starts_with("/spill"));
        assert_eq!(b.max_temp_directory_size, Some(10 * GIB));

        let ov = EngineOverrides {
            memory: Some(Auto::Auto),
            threads: Some(Auto::Value(2)),
            duckdb_share: Some(0.5),
            temp_directory: Some("/other".into()),
            max_temp_directory_size: None,
            memory_mode: None,
        };
        let b = Budget::compute(&cfg, Some(&ov), &det(2 * GIB, 1), &p).expect("budget");
        assert!(b.temp_directory.starts_with("/other"));
        assert_eq!(b.max_temp_directory_size, Some(10 * GIB));
        assert_eq!(b.memory, (2.0 * GIB as f64 * 0.85) as u64);
        assert_eq!(b.threads, 2);
        assert_eq!(b.memory_limit_rule, LimitRule::Share);
        assert_eq!(b.memory_limit, (b.memory as f64 * 0.5) as u64);
    }

    #[test]
    fn custom_params() {
        let p = BudgetParams {
            high: Overhead {
                fixed: 100 * MIB,
                per_thread: 10 * MIB,
            },
            low: Overhead {
                fixed: 0,
                per_thread: 0,
            },
            default_channel_bytes: 0,
            auto_memory_fraction: 1.0,
            min_memory_limit: MIB,
        };
        let b = Budget::compute(&EngineConfig::default(), None, &det(GIB, 2), &p).expect("budget");
        assert_eq!(b.memory_limit, GIB - 120 * MIB);
    }

    #[test]
    fn invalid_values() {
        let p = BudgetParams::default();
        for cfg in [
            EngineConfig {
                duckdb_share: Some(0.0),
                ..Default::default()
            },
            EngineConfig {
                duckdb_share: Some(1.5),
                ..Default::default()
            },
            EngineConfig {
                threads: Some(Auto::Value(0)),
                ..Default::default()
            },
            EngineConfig {
                memory: Some(Auto::Value(ByteSize(0))),
                ..Default::default()
            },
        ] {
            let e = Budget::compute(&cfg, None, &det(GIB, 1), &p).expect_err("invalid");
            assert_eq!(e.class, atlatl_core::ErrorClass::Config, "{cfg:?}");
        }
    }

    #[test]
    fn serialises_for_report() {
        let b = Budget::compute(
            &EngineConfig::default(),
            None,
            &det(8 * GIB, 4),
            &BudgetParams::default(),
        )
        .expect("budget");
        let v = serde_json::to_value(&b).expect("json");
        assert_eq!(v["memory_source"], "cgroup_v2");
        assert_eq!(v["memory_limit_rule"], "headroom");
        assert!(v.get("duckdb_share").is_none());
        assert_eq!(v["memory_mode"], "high");
        assert_eq!(v["overhead"]["fixed"], BudgetParams::default().high.fixed);
        assert!(v["purge_every_ms"].is_null());
        assert_eq!(
            v["params"]["low"]["per_thread"],
            BudgetParams::default().low.per_thread
        );
    }
}
