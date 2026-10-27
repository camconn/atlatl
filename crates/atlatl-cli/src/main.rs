//! The `atlatl` command line.

mod api;
mod gc;
mod schema_cmd;
mod signals;

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use atlatl_core::config::diag::{Diagnostics, SourceMap};
use atlatl_core::config::load::{self, Project};
use atlatl_core::config::model::{ProjectFile, ReplicationsFile, SchemaFile};
use atlatl_core::error::{AtlatlError, ErrorClass};
use atlatl_core::obs::report::KillReason;
use atlatl_core::obs::{EventSink, EventTarget, LogFormat};
use atlatl_core::plan::{self, Plan, PlanRequest, check_project};
use atlatl_core::redact::Redactor;
use atlatl_core::template::{ProcessEnv, Templates};
use clap::{Args, Parser, Subcommand, ValueEnum};

/// Exit codes (spec 03 §10).
const EXIT_CONFIG: u8 = 2;
const EXIT_INTERNAL: u8 = 3;

#[derive(Parser)]
#[command(
    name = "atlatl",
    version,
    about = "Data movement into and out of DuckLake"
)]
struct Cli {
    /// Project root (default: current directory).
    #[arg(long, global = true, env = "ATLATL_PROJECT")]
    project: Option<PathBuf>,
    #[command(flatten)]
    obs: ObsArgs,
    #[command(subcommand)]
    command: Command,
}

#[derive(Args)]
struct ObsArgs {
    /// Log format on stderr [default: text on a TTY, json otherwise].
    #[arg(long, global = true, value_enum)]
    log_format: Option<LogFormatArg>,
    /// NDJSON event stream: `fd:N`, a path, or `-` for stdout.
    #[arg(long, global = true, value_name = "fd:N|PATH|-")]
    events: Option<EventTarget>,
    /// Write the run report here (atomically) before exit.
    #[arg(long, global = true, value_name = "PATH")]
    report: Option<PathBuf>,
}

#[derive(Clone, Copy, ValueEnum)]
enum LogFormatArg {
    Text,
    Json,
}

impl ObsArgs {
    fn init_logging(&self) {
        let format = self
            .log_format
            .map(|f| match f {
                LogFormatArg::Text => LogFormat::Text,
                LogFormatArg::Json => LogFormat::Json,
            })
            .unwrap_or_else(LogFormat::detect);
        atlatl_core::obs::log::init(Some(format));
        if format == LogFormat::Text && std::io::IsTerminal::is_terminal(&std::io::stderr()) {
            atlatl_core::obs::status::enable();
        }
    }

    /// Open before anything else opens files, so an inherited `fd:N` is still the caller's.
    fn event_sink(&self, run_id: &str) -> Result<EventSink, String> {
        EventSink::open(self.events.as_ref(), run_id).map_err(|e| {
            let t = self
                .events
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default();
            format!("cannot open events target `{t}`: {e}")
        })
    }

    fn report_path(&self) -> Option<&std::path::Path> {
        self.report.as_deref()
    }
}

#[derive(Subcommand)]
enum Command {
    /// Run the selected replications.
    Run(RunCmd),
    /// Resolve the selection and print the plan.
    Plan(PlanArgs),
    /// Inspect or change stream state (cursors) in the state lake.
    State {
        #[arg(long, global = true)]
        env: Option<String>,
        #[arg(long, global = true, env = "ATLATL_EXTENSION_DIR")]
        extension_dir: Option<PathBuf>,
        /// A `for_each` instance key: its state is `<stream>@<key>` (which also works as the id).
        #[arg(long, global = true, value_name = "KEY")]
        instance: Option<String>,
        #[command(subcommand)]
        op: StateCmd,
    },
    /// Open rejects (03 §7): list, retry or drop them.
    Rejects {
        #[arg(long, global = true)]
        env: Option<String>,
        #[arg(long, global = true, env = "ATLATL_EXTENSION_DIR")]
        extension_dir: Option<PathBuf>,
        /// A `for_each` instance key: its rejects are under `<stream>@<key>` (which also works).
        #[arg(long, global = true, value_name = "KEY")]
        instance: Option<String>,
        #[command(subcommand)]
        op: RejectsCmd,
    },
    /// Postgres logical-replication slots (06 §9).
    Cdc {
        #[command(subcommand)]
        op: CdcCmd,
    },
    /// REST tooling: `api test` (redacted, with cassettes) and `api explain`.
    Api {
        #[command(subcommand)]
        op: api::ApiCmd,
    },
    /// Source schemas: `schema infer` and `schema check`.
    Schema {
        #[command(subcommand)]
        op: schema_cmd::SchemaCmd,
    },
    /// Check config without running anything (every environment).
    Validate {
        /// Diagnostics as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Print a JSON Schema.
    JsonSchema {
        #[arg(value_enum)]
        kind: SchemaKind,
    },
    /// Remove run-scoped leftovers (run temp dirs, DuckDB spill files, Redshift UNLOAD prefixes) that a killed run left.
    Gc {
        /// Only artefacts at least this old (e.g. `24h`, `3d`).
        #[arg(long, default_value = "24h")]
        older_than: String,
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
enum CdcCmd {
    /// Adopt or reload a serving table at a lake snapshot and set its cursor (08 §9).
    Bootstrap {
        replication: String,
        #[arg(long, value_enum, default_value = "adopt")]
        mode: BootModeArg,
        /// Default: the lake head.
        #[arg(long)]
        at_snapshot: Option<i64>,
        /// Report diff counts and samples without writing.
        #[arg(long)]
        dry_run: bool,
        /// Set `cursor = S` without applying anything (after a manual backfill).
        #[arg(long)]
        advance_cursor_only: bool,
        #[arg(long)]
        env: Option<String>,
        #[arg(long, env = "ATLATL_EXTENSION_DIR")]
        extension_dir: Option<PathBuf>,
    },
    /// Drop a replication's slot and publication; the lake keeps its data and cursors.
    Decommission {
        replication: String,
        /// Terminate a walsender still holding the slot.
        #[arg(long)]
        force: bool,
        #[arg(long)]
        env: Option<String>,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum BootModeArg {
    Adopt,
    Reload,
}

#[derive(Args)]
struct PlanArgs {
    #[arg(long)]
    env: Option<String>,
    #[arg(long = "replication", value_name = "NAME")]
    replications: Vec<String>,
    #[arg(long = "stream", value_name = "ID")]
    streams: Vec<String>,
    #[arg(long = "tag", value_name = "TAG")]
    tags: Vec<String>,
    /// Only this `for_each` instance, named as in the plan (`--instance 'tenants[acme]'`).
    #[arg(long = "instance", value_name = "REPLICATION[KEY]")]
    instances: Vec<String>,
    /// Override a stream variable for this run (`--var bulk_dir=/tmp/x`).
    #[arg(long = "var", value_name = "K=V", value_parser = parse_var)]
    vars: Vec<(String, String)>,
    /// Add unselected queue producers as feed-only (default).
    #[arg(long, overrides_with = "no_deps")]
    with_deps: bool,
    /// Fail instead of adding feed-only producers.
    #[arg(long)]
    no_deps: bool,
    /// No environment, templates or secrets: config only.
    #[arg(long = "static")]
    static_plan: bool,
    /// Expand wildcards against the source catalog.
    #[arg(long)]
    discover: bool,
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct RunCmd {
    #[arg(long)]
    env: Option<String>,
    #[arg(long = "replication", value_name = "NAME")]
    replications: Vec<String>,
    #[arg(long = "stream", value_name = "ID")]
    streams: Vec<String>,
    #[arg(long = "tag", value_name = "TAG")]
    tags: Vec<String>,
    /// Only this `for_each` instance, named as in the plan (`--instance 'tenants[acme]'`).
    #[arg(long = "instance", value_name = "REPLICATION[KEY]")]
    instances: Vec<String>,
    #[arg(long = "var", value_name = "K=V", value_parser = parse_var)]
    vars: Vec<(String, String)>,
    /// Fail instead of adding feed-only queue producers.
    #[arg(long)]
    no_deps: bool,
    /// Replications run concurrently [default: engine.replication_concurrency, 2].
    #[arg(long)]
    concurrency: Option<usize>,
    /// Kill the run after this long (e.g. `4m`); in-flight work is reported `killed`.
    #[arg(long)]
    timeout: Option<String>,
    /// Report progress (a log line off a terminal, and a `stream.progress` event) this often;
    /// `0` turns progress reporting off, the terminal status line included [default: 30s]
    #[arg(long, value_name = "DURATION", env = atlatl_run::progress::INTERVAL_ENV)]
    progress_interval: Option<String>,
    /// Caller's run id (Dagster); keys `--skip-succeeded`.
    #[arg(long)]
    external_run_id: Option<String>,
    /// Skip replications that already succeeded under `--external-run-id`.
    #[arg(long)]
    skip_succeeded: bool,
    /// Backfill: override REST iteration bounds for this run (`2015-01-01..2020-01-01`).
    #[arg(long, value_name = "START..END")]
    range: Option<String>,
    /// Extract, stage, validate and diff every selected ingest stream against its target, then
    /// roll back: nothing is committed. Prints the diff as JSON.
    #[arg(long)]
    dry_run: bool,
    /// With `--dry-run`: sample keys per op (insert, update, delete) and member [default: 5].
    #[arg(long, value_name = "N", requires = "dry_run")]
    diff_samples: Option<usize>,
    #[arg(long, env = "ATLATL_EXTENSION_DIR")]
    extension_dir: Option<PathBuf>,
}

#[derive(Subcommand)]
enum StateCmd {
    /// Print state as JSON.
    Show {
        #[arg(long)]
        stream: Option<String>,
    },
    /// Rewind or advance an `update_key` cursor.
    Set {
        stream: String,
        #[arg(long, default_value = "")]
        member: String,
        #[arg(long)]
        cursor: String,
    },
    /// Delete state; the next run is a full extract.
    Reset {
        stream: String,
        #[arg(long)]
        member: Option<String>,
    },
    /// Re-stamp `config_hash` after a reviewed config change.
    Accept { stream: String },
    /// Seed the cursor from `max(update_key)` of the existing target.
    Init {
        stream: String,
        #[arg(long, required = true)]
        from_target: bool,
        /// A member of a `from.tables` stream to seed (`schema.table`, repeatable), each from
        /// its own target table.
        #[arg(long = "member")]
        members: Vec<String>,
    },
    /// Rename a stream id without losing state.
    Move { old: String, new: String },
}

#[derive(Subcommand)]
enum RejectsCmd {
    /// Open rejects per stream and member (text, or `--json`).
    List {
        #[arg(long)]
        stream: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Re-issue the stored requests of these streams' open rejects now; the rows that decode
    /// land (and their rejects are deleted) in one lake commit. Prints the outcome as JSON.
    Retry {
        #[arg(required = true)]
        streams: Vec<String>,
    },
    /// Delete rejects: a stream's (or one member's, or only `--key`s), or every stream's with `--all`.
    Drop {
        stream: Option<String>,
        #[arg(long)]
        member: Option<String>,
        #[arg(long = "key", value_name = "REJECT_KEY")]
        keys: Vec<String>,
        #[arg(long, conflicts_with = "stream")]
        all: bool,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum SchemaKind {
    /// `atlatl.yaml` (alias: config).
    #[value(alias = "config")]
    Project,
    /// `replications/**/*.yaml`.
    Replications,
    /// `schemas/**/*.yaml`.
    SourceSchema,
    /// `atlatl plan --json` output.
    Plan,
    /// The run report (`--report`).
    Report,
    /// One line of the event stream (`--events`).
    Events,
}

fn parse_var(s: &str) -> Result<(String, String), String> {
    s.split_once('=')
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .filter(|(k, _)| !k.is_empty())
        .ok_or_else(|| format!("expected K=V, got `{s}`"))
}

fn render(sources: &SourceMap, d: &Diagnostics) -> String {
    d.items
        .iter()
        .map(|i| sources.render(i))
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn load_project(
    root: &std::path::Path,
) -> Result<(Project, Diagnostics), (SourceMap, Diagnostics)> {
    let (p, d) = load::load(root);
    if d.has_errors() {
        Err((p.sources, d))
    } else {
        Ok((p, d))
    }
}

fn print_plan(plan: &Plan) {
    let env = plan
        .env
        .as_deref()
        .map(|e| format!(" env={e}"))
        .unwrap_or_default();
    println!(
        "plan {}{env} ({} replications)",
        plan.plan_hash,
        plan.replications.len()
    );
    for r in &plan.replications {
        let after = if r.after.is_empty() {
            String::new()
        } else {
            format!(" after {}", r.after.join(", "))
        };
        println!(
            "  {} [op {}] {} → {}{after}",
            r.name, r.op, r.source.name, r.target.name
        );
        for s in &r.streams {
            let role = if s.role == plan::StreamRole::FeedOnly {
                " (feed-only)"
            } else {
                ""
            };
            let obj = s
                .members
                .iter()
                .filter_map(|m| m.object.clone())
                .collect::<Vec<_>>();
            let obj = if obj.is_empty() {
                s.object.clone()
            } else {
                obj.join(", ")
            };
            let hop = match r.hops.get(s.hop) {
                Some(h) if s.hop > 0 => {
                    format!(" (hop {}: {} → {})", s.hop, h.source.name, h.target.name)
                }
                _ => String::new(),
            };
            let pk = s
                .primary_key
                .as_ref()
                .map(|k| format!(" pk ({})", k.join(", ")))
                .unwrap_or_default();
            println!(
                "    {} {} → {obj} [{}]{pk}{role}{hop}",
                s.id, s.mode, s.asset.key
            );
            for (m, k) in &s.member_primary_keys {
                println!("      members.{m}: pk ({})", k.join(", "));
            }
            let discovered = s.from.tables.is_some();
            for m in s.members.iter().filter(|m| discovered && !m.pattern) {
                match (&m.primary_key, &m.primary_key_from) {
                    (Some(k), Some(from)) => {
                        println!("      {}: pk ({}) from {from}", m.name, k.join(", "))
                    }
                    _ => println!("      {}: no primary key", m.name),
                }
            }
        }
    }
    for w in &plan.warnings {
        println!("warning[{}]: {}", w.code, w.message);
    }
}

fn run(cli: Cli) -> Result<ExitCode, String> {
    let root = cli.project.clone().unwrap_or_else(|| PathBuf::from("."));
    match cli.command {
        Command::Run(a) => {
            atlatl_core::fault::init().map_err(|e| format!("{}: {e}", atlatl_core::fault::ENV))?;
            let run_id = atlatl_core::ids::ulid();
            let events = cli.obs.event_sink(&run_id)?;
            let timeout = a
                .timeout
                .as_deref()
                .map(atlatl_core::config::types::Dur::parse)
                .transpose()?
                .map(|d| d.0);
            let progress_interval = a
                .progress_interval
                .as_deref()
                .map(|s| match s.trim() {
                    "0" => Ok(std::time::Duration::ZERO),
                    s => atlatl_core::config::types::Dur::parse(s).map(|d| d.0),
                })
                .transpose()?;
            let (p, _) = match load_project(&root) {
                Ok(x) => x,
                Err((sources, d)) => {
                    eprintln!("{}", render(&sources, &d));
                    return Ok(ExitCode::from(EXIT_CONFIG));
                }
            };
            let args = atlatl_run::RunArgs {
                plan: PlanRequest {
                    env: a.env,
                    vars: a.vars.into_iter().collect(),
                    run_id: Some(run_id),
                    replications: a.replications,
                    streams: a.streams,
                    tags: a.tags,
                    instances: a.instances,
                    with_deps: !a.no_deps,
                    static_plan: false,
                    discover: false,
                    range: a.range,
                },
                external_run_id: a.external_run_id,
                skip_succeeded: a.skip_succeeded,
                concurrency: a.concurrency,
                events,
                report: cli.obs.report_path().map(std::path::Path::to_path_buf),
                extension_dir: a.extension_dir,
                dry_run: a.dry_run.then(|| atlatl_run::dry::DryRun {
                    samples: a.diff_samples.unwrap_or(atlatl_run::dry::DEFAULT_SAMPLES),
                }),
                retry_rejects: false,
                progress_interval,
            };
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            let r = rt.block_on(run_cmd(p, args, timeout));
            // Dropping the runtime waits for blocking tasks; after a kill, engine work that
            // ignores the interrupt must not hold the exit (03 §9).
            rt.shutdown_timeout(std::time::Duration::from_millis(100));
            r
        }
        Command::State {
            env,
            extension_dir,
            instance,
            op,
        } => {
            use atlatl_run::state_cmd::{StateArgs, StateOp};
            let (p, _) = match load_project(&root) {
                Ok(x) => x,
                Err((sources, d)) => {
                    eprintln!("{}", render(&sources, &d));
                    return Ok(ExitCode::from(EXIT_CONFIG));
                }
            };
            let op = match op {
                StateCmd::Show { stream } => StateOp::Show { stream, instance },
                StateCmd::Set {
                    stream,
                    member,
                    cursor,
                } => StateOp::Set {
                    stream,
                    instance,
                    member,
                    cursor,
                },
                StateCmd::Reset { stream, member } => StateOp::Reset {
                    stream,
                    instance,
                    member,
                },
                StateCmd::Accept { stream } => StateOp::Accept { stream, instance },
                StateCmd::Init {
                    stream, members, ..
                } => StateOp::Init {
                    stream,
                    instance,
                    members,
                },
                StateCmd::Move { old, new } => StateOp::Move { old, new, instance },
            };
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            let args = StateArgs { env, extension_dir };
            match rt.block_on(atlatl_run::state_cmd::state(
                Arc::new(p),
                args,
                Arc::new(ProcessEnv),
                op,
            )) {
                Ok(v) => {
                    let s = serde_json::to_string_pretty(&v).map_err(|e| e.to_string())?;
                    println!("{}", atlatl_core::redact::redact(&s));
                    Ok(ExitCode::SUCCESS)
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    Ok(ExitCode::from(if e.class == ErrorClass::Config {
                        EXIT_CONFIG
                    } else {
                        EXIT_INTERNAL
                    }))
                }
            }
        }
        Command::Rejects {
            env,
            extension_dir,
            instance,
            op,
        } => {
            use atlatl_run::rejects_cmd::{self, RejectsOp};
            let (p, _) = match load_project(&root) {
                Ok(x) => x,
                Err((sources, d)) => {
                    eprintln!("{}", render(&sources, &d));
                    return Ok(ExitCode::from(EXIT_CONFIG));
                }
            };
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            let fail = |e: AtlatlError| {
                eprintln!("error: {e}");
                Ok(ExitCode::from(if e.class == ErrorClass::Config {
                    EXIT_CONFIG
                } else {
                    EXIT_INTERNAL
                }))
            };
            let (op, text) = match op {
                RejectsCmd::Retry { streams } => {
                    let run_id = atlatl_core::ids::ulid();
                    let events = cli.obs.event_sink(&run_id)?;
                    let mut plan =
                        match rejects_cmd::retry_request(&p, env, &streams, instance.as_deref()) {
                            Ok(r) => r,
                            Err(e) => return fail(e),
                        };
                    plan.run_id = Some(run_id);
                    let args = atlatl_run::RunArgs {
                        plan,
                        events,
                        report: cli.obs.report_path().map(std::path::Path::to_path_buf),
                        extension_dir,
                        retry_rejects: true,
                        ..Default::default()
                    };
                    let r = rt.block_on(run_cmd(p, args, None));
                    rt.shutdown_timeout(std::time::Duration::from_millis(100));
                    return r;
                }
                RejectsCmd::List { stream, json } => (RejectsOp::List { stream, instance }, !json),
                RejectsCmd::Drop {
                    stream,
                    member,
                    keys,
                    all,
                } => (
                    RejectsOp::Drop {
                        stream,
                        instance,
                        member,
                        keys,
                        all,
                    },
                    false,
                ),
            };
            let args = atlatl_run::state_cmd::StateArgs { env, extension_dir };
            match rt.block_on(rejects_cmd::rejects(
                Arc::new(p),
                args,
                Arc::new(ProcessEnv),
                op,
            )) {
                Ok(v) => {
                    let s = if text {
                        rejects_cmd::list_text(&v)
                    } else {
                        format!(
                            "{}\n",
                            serde_json::to_string_pretty(&v).map_err(|e| e.to_string())?
                        )
                    };
                    print!("{}", atlatl_core::redact::redact(&s));
                    Ok(ExitCode::SUCCESS)
                }
                Err(e) => fail(e),
            }
        }
        Command::Gc {
            older_than,
            dry_run,
        } => {
            let age = atlatl_core::config::types::Dur::parse(&older_than)?.0;
            let (p, _) = load::load(&root);
            let dir = PathBuf::from(
                p.engine
                    .temp_directory
                    .clone()
                    .unwrap_or_else(|| "/tmp/atlatl/spill".into()),
            );
            let found =
                gc::collect(&dir, age, std::time::SystemTime::now()).map_err(|e| e.to_string())?;
            for f in &found {
                println!(
                    "{}{}",
                    if dry_run {
                        "would remove "
                    } else {
                        "removing "
                    },
                    f.display()
                );
            }
            if !dry_run {
                gc::remove(&found).map_err(|e| e.to_string())?;
            }
            gc::redshift(&p, age, dry_run)?;
            Ok(ExitCode::SUCCESS)
        }
        Command::JsonSchema { kind } => {
            let s = match kind {
                SchemaKind::Project => schemars::schema_for!(ProjectFile),
                SchemaKind::Replications => schemars::schema_for!(ReplicationsFile),
                SchemaKind::SourceSchema => schemars::schema_for!(SchemaFile),
                SchemaKind::Plan => schemars::schema_for!(Plan),
                SchemaKind::Report => atlatl_core::obs::report::schema(),
                SchemaKind::Events => atlatl_core::obs::events::schema(),
            };
            println!(
                "{}",
                serde_json::to_string_pretty(&s).map_err(|e| e.to_string())?
            );
            Ok(ExitCode::SUCCESS)
        }
        Command::Cdc {
            op:
                CdcCmd::Bootstrap {
                    replication,
                    mode,
                    at_snapshot,
                    dry_run,
                    advance_cursor_only,
                    env,
                    extension_dir,
                },
        } => {
            let (p, _) = match load_project(&root) {
                Ok(x) => x,
                Err((sources, d)) => {
                    eprintln!("{}", render(&sources, &d));
                    return Ok(ExitCode::from(EXIT_CONFIG));
                }
            };
            let args = atlatl_run::serve::BootstrapArgs {
                replication,
                env,
                mode: match mode {
                    BootModeArg::Adopt => atlatl_cdc::bootstrap::Mode::Adopt,
                    BootModeArg::Reload => atlatl_cdc::bootstrap::Mode::Reload,
                },
                at_snapshot,
                dry_run,
                advance_cursor_only,
                extension_dir,
            };
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            match rt.block_on(atlatl_run::serve::bootstrap_cmd(
                Arc::new(p),
                args,
                Arc::new(ProcessEnv),
            )) {
                Ok(v) => {
                    let s = serde_json::to_string_pretty(&v).map_err(|e| e.to_string())?;
                    println!("{}", atlatl_core::redact::redact(&s));
                    Ok(ExitCode::SUCCESS)
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    Ok(ExitCode::from(if e.class == ErrorClass::Config {
                        EXIT_CONFIG
                    } else {
                        EXIT_INTERNAL
                    }))
                }
            }
        }
        Command::Cdc {
            op:
                CdcCmd::Decommission {
                    replication,
                    force,
                    env,
                },
        } => {
            let (p, _) = match load_project(&root) {
                Ok(x) => x,
                Err((sources, d)) => {
                    eprintln!("{}", render(&sources, &d));
                    return Ok(ExitCode::from(EXIT_CONFIG));
                }
            };
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            let r = rt.block_on(atlatl_run::pgcdc::decommission(
                &p,
                &replication,
                env.as_deref(),
                force,
                Arc::new(ProcessEnv),
            ));
            match r {
                Ok(d) => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&d).map_err(|e| e.to_string())?
                    );
                    Ok(ExitCode::SUCCESS)
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    Ok(ExitCode::from(if e.class == ErrorClass::Config {
                        EXIT_CONFIG
                    } else {
                        EXIT_INTERNAL
                    }))
                }
            }
        }
        Command::Api { op } => api::run(&root, op),
        Command::Schema { op } => schema_cmd::run(&root, op),
        Command::Validate { json } => {
            let (p, mut d) = load::load(&root);
            if !d.has_errors() {
                check_project(&p, &Templates::new(), &mut d);
                api::check_specs(&p, &mut d);
            }
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&d.to_json()).map_err(|e| e.to_string())?
                );
            } else if !d.items.is_empty() {
                eprintln!("{}", render(&p.sources, &d));
            }
            if d.has_errors() {
                return Ok(ExitCode::from(EXIT_CONFIG));
            }
            if !json {
                let n: usize = p.replications.iter().map(|r| r.streams.len()).sum();
                println!("ok: {} replications, {n} streams", p.replications.len());
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Plan(a) => {
            let (p, _) = match load_project(&root) {
                Ok(x) => x,
                Err((sources, d)) => {
                    eprintln!("{}", render(&sources, &d));
                    return Ok(ExitCode::from(EXIT_CONFIG));
                }
            };
            let req = PlanRequest {
                env: a.env,
                vars: a.vars.into_iter().collect(),
                run_id: None,
                replications: a.replications,
                streams: a.streams,
                tags: a.tags,
                instances: a.instances,
                with_deps: !a.no_deps,
                static_plan: a.static_plan,
                discover: a.discover,
                range: None,
            };
            match plan::build(&p, &req, Arc::new(ProcessEnv), Redactor::global()) {
                Ok((mut plan, _)) => {
                    if !a.static_plan
                        && let Err(e) = atlatl_run::run::check_rest_specs(&p, &plan)
                    {
                        eprintln!("error: {e}");
                        return Ok(ExitCode::from(EXIT_CONFIG));
                    }
                    if a.discover {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .map_err(|e| e.to_string())?;
                        if let Err(e) = rt.block_on(atlatl_run::pgsrc::discover_plan(
                            &p,
                            &mut plan,
                            Arc::new(ProcessEnv),
                        )) {
                            eprintln!("error: {e}");
                            return Ok(ExitCode::from(if e.class == ErrorClass::Config {
                                EXIT_CONFIG
                            } else {
                                EXIT_INTERNAL
                            }));
                        }
                    }
                    if a.json {
                        let s = serde_json::to_string_pretty(&plan).map_err(|e| e.to_string())?;
                        println!("{}", atlatl_core::redact::redact(&s));
                    } else {
                        print_plan(&plan);
                    }
                    Ok(ExitCode::SUCCESS)
                }
                Err(d) => {
                    eprintln!("{}", render(&p.sources, &d));
                    Ok(ExitCode::from(EXIT_CONFIG))
                }
            }
        }
    }
}

async fn run_cmd(
    p: Project,
    args: atlatl_run::RunArgs,
    timeout: Option<std::time::Duration>,
) -> Result<ExitCode, String> {
    let kill = signals::Kill::new();
    kill.install().map_err(|e| e.to_string())?;
    if let Some(t) = timeout {
        let k = kill.clone();
        tokio::spawn(async move {
            tokio::time::sleep(t).await;
            k.trigger(KillReason::Timeout);
        });
    }
    let (k1, k2) = (kill.clone(), kill.clone());
    let (dry, retry) = (args.dry_run.is_some(), args.retry_rejects);
    let r = atlatl_run::run(
        Arc::new(p),
        args,
        Arc::new(ProcessEnv),
        kill.token.clone(),
        move || k1.reason(),
        move |h, path| k2.set_report(h.clone(), path.map(std::path::Path::to_path_buf)),
    )
    .await;
    kill.finished();
    match r {
        Ok(report) => {
            if dry {
                let v = atlatl_run::dry::render(&report);
                let s = serde_json::to_string_pretty(&v).map_err(|e| e.to_string())?;
                println!("{}", atlatl_core::redact::redact(&s));
                eprint!(
                    "{}",
                    atlatl_core::redact::redact(&atlatl_run::dry::summary(&report))
                );
            }
            if retry {
                let v = atlatl_run::rejects_cmd::render_retry(&report);
                let s = serde_json::to_string_pretty(&v).map_err(|e| e.to_string())?;
                println!("{}", atlatl_core::redact::redact(&s));
            }
            Ok(ExitCode::from(match kill.reason() {
                Some(r @ (KillReason::Sigint | KillReason::Sigterm)) => signals::exit_code(r),
                _ => report.exit_code,
            }))
        }
        Err(e) => {
            eprintln!("error: {e}");
            Ok(ExitCode::from(if e.class == ErrorClass::Config {
                EXIT_CONFIG
            } else {
                EXIT_INTERNAL
            }))
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    cli.obs.init_logging();
    let code = match std::panic::catch_unwind(|| run(cli)) {
        Ok(Ok(code)) => code,
        Ok(Err(e)) => {
            eprintln!("error: {e}");
            ExitCode::from(EXIT_INTERNAL)
        }
        Err(_) => ExitCode::from(EXIT_INTERNAL),
    };
    let _ = std::io::stdout().flush();
    code
}
