//! `atlatl api test` and `atlatl api explain` (07 §13), and the REST sampling behind
//! `atlatl schema infer|check --from-api-test|--from-cassette`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use atlatl_core::config::diag::Diagnostics;
use atlatl_core::config::load::Project;
use atlatl_core::config::model::{ConnectionKind, RestConn};
use atlatl_core::config::types::SecretValue;
use atlatl_core::redact::redact;
use atlatl_core::template::{EnvSource, ProcessEnv};
use atlatl_http::cassette::{self, Interaction};
use atlatl_http::json::JValue;
use atlatl_http::{ApiClient, ApiSpec, Cassette, ClientOptions, DriveOptions, Driver, RunVars};
use clap::{Args, Subcommand};
use indexmap::IndexMap;
use serde_json::{Value, json};

#[derive(Subcommand)]
pub enum ApiCmd {
    /// Run an endpoint against the live API (or a cassette) without writing anything; prints the
    /// redacted requests, responses, records and would-be field types.
    Test(TestArgs),
    /// Print the effective config of an API (spec overview) or one endpoint: defaults, auth
    /// (redacted), limiter, pagination, records, rules (built-ins included) and dependencies.
    Explain {
        /// A REST connection, or an API spec (`apis/` path, file stem or spec `name`).
        api: String,
        endpoint: Option<String>,
        #[arg(long)]
        env: Option<String>,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Args, Clone, Default)]
pub struct SampleArgs {
    #[arg(long)]
    pub env: Option<String>,
    /// Bound as `vars.K` (and `stream.vars.K`).
    #[arg(long = "var", value_name = "K=V", value_parser = crate::parse_var)]
    pub vars: Vec<(String, String)>,
    /// `for_each` items (JSON), replacing `over` (repeatable).
    #[arg(long = "item", value_name = "JSON")]
    pub items: Vec<String>,
    /// A per-item secret, bound as `secrets.item.K` (repeatable).
    #[arg(long = "item-secret", value_name = "K=V", value_parser = crate::parse_var)]
    pub item_secrets: Vec<(String, String)>,
    /// Stop after this many pages (endpoints without `for_each`/`setup`).
    #[arg(long, default_value_t = 2)]
    pub pages: u64,
    /// Stop after this many `for_each` items (each run to completion).
    #[arg(long = "items", default_value_t = 2)]
    pub max_items: u64,
    /// Record the session to a cassette: a name (`apis/<api>/cassettes/<name>.json`) or a path.
    #[arg(long, value_name = "NAME|PATH", conflicts_with = "replay")]
    pub record: Option<String>,
    /// Replay a cassette instead of calling the API.
    #[arg(long, value_name = "NAME|PATH")]
    pub replay: Option<String>,
    /// Query parameters left out of cassette matching (with `--record`).
    #[arg(long = "ignore-param", value_name = "NAME")]
    pub ignore_params: Vec<String>,
}

#[derive(Args)]
pub struct TestArgs {
    /// A REST connection (or an API spec that uses no `secrets.*`).
    pub api: String,
    pub endpoint: String,
    #[command(flatten)]
    pub sample: SampleArgs,
    /// Body characters printed per response.
    #[arg(long, default_value_t = 2000)]
    pub body_chars: usize,
    /// Records printed.
    #[arg(long, default_value_t = 20)]
    pub records: usize,
    #[arg(long)]
    pub json: bool,
}

/// An API spec and, for a connection, its resolved secrets.
pub struct Target {
    pub spec: Arc<ApiSpec>,
    /// Path relative to the project root.
    pub spec_path: String,
    pub secrets: IndexMap<String, String>,
}

impl Target {
    /// `apis/monthly.yaml` → `monthly`, `apis/v/monthly.yaml` → `v/monthly` (the cassette directory).
    pub fn api_dir(&self) -> String {
        cassette::api_dir(&self.spec_path)
    }
}

/// Full parse of every discovered API spec (the project loader reads only its dependencies).
pub fn check_specs(p: &Project, d: &mut Diagnostics) {
    for spec in p.api_specs.values() {
        match ApiSpec::load_config(&p.root, &spec.path) {
            Ok(s) => s.lints().into_iter().for_each(|w| d.warn("api-spec", w)),
            Err(e) => d.error("api-spec", e),
        }
    }
}

fn load_spec(root: &Path, rel: &str) -> Result<ApiSpec, String> {
    ApiSpec::load(root, rel).map_err(|e| e.message)
}

/// How connection secrets are obtained.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Secrets {
    /// Resolved (env, files, `aws-sm:`).
    Resolve,
    /// Not read; each becomes a distinct placeholder, registered (so masked) like a real value.
    /// For cassette replay, whose recordings hold `****` wherever a secret went.
    Placeholder,
    /// Not read, left out (`api explain`).
    Skip,
}

pub fn target(
    p: &Project,
    api: &str,
    env: Option<&str>,
    secrets: Secrets,
) -> Result<Target, String> {
    let resolve = secrets == Secrets::Resolve;
    let placeholders = secrets == Secrets::Placeholder;
    if let Some(raw) = p.connections.get(api) {
        let conn = if resolve {
            atlatl_run::runtime::resolve_connection(p, api, env, Arc::new(ProcessEnv))
                .map_err(|e| e.to_string())?
        } else {
            let mut d = atlatl_core::config::Diagnostics::default();
            atlatl_core::config::env::resolve_connection(
                p,
                raw,
                env,
                &atlatl_core::config::env::Resolve::Static,
                &atlatl_core::template::Templates::new(),
                &mut d,
            )
            .ok_or_else(|| format!("connection `{api}` failed to resolve"))?
        };
        let ConnectionKind::Rest(RestConn { spec, secrets }) = &conn.kind else {
            return Err(format!("connection `{api}` is not a REST connection"));
        };
        let secrets = secrets
            .iter()
            .filter_map(|(k, v)| match (v, resolve) {
                (SecretValue::Literal(s), true) => Some((k.clone(), s.clone())),
                _ if resolve => None,
                _ if !placeholders => None,
                _ => Some((k.clone(), format!("atlatl-replay-secret-{k}"))),
            })
            .collect();
        return Ok(Target {
            spec: Arc::new(load_spec(&p.root, &spec.value)?),
            spec_path: spec.value.clone(),
            secrets,
        });
    }
    let found = p.api_specs.iter().find(|(path, s)| {
        path.as_str() == api
            || Path::new(path.as_str())
                .file_stem()
                .and_then(|x| x.to_str())
                == Some(api)
            || s.name.as_deref() == Some(api)
    });
    match found {
        Some((path, _)) => Ok(Target {
            spec: Arc::new(load_spec(&p.root, path)?),
            spec_path: path.clone(),
            secrets: IndexMap::new(),
        }),
        None => Err(format!(
            "`{api}` is neither a REST connection nor an API spec (specs: {})",
            p.api_specs.keys().cloned().collect::<Vec<_>>().join(", ")
        )),
    }
}

/// A cassette argument: a bare name under `apis/<api>/cassettes/`, else a path.
pub fn cassette_path(root: &Path, spec_path: &str, arg: &str) -> PathBuf {
    cassette::resolve(root, spec_path, arg)
}

pub struct Sample {
    pub records: Vec<atlatl_http::Record>,
    pub cassette: Cassette,
    pub saved: Option<PathBuf>,
    pub pages: u64,
    /// `for_each` items run.
    pub items: u64,
    /// Retries, skips and stops (plain endpoints).
    pub decisions: Vec<String>,
    pub error: Option<atlatl_core::AtlatlError>,
}

/// Runs up to `pages` chunks of an endpoint. Errors after the client starts are returned in
/// [`Sample::error`], so what was exchanged can still be printed.
pub async fn sample(
    p: &Project,
    api: &str,
    endpoint: &str,
    a: &SampleArgs,
    stream_vars: Option<Value>,
) -> Result<(Target, Sample), String> {
    let mode = if a.replay.is_some() {
        Secrets::Placeholder
    } else {
        Secrets::Resolve
    };
    let t = target(p, api, a.env.as_deref(), mode)?;
    let cas = match (&a.record, &a.replay) {
        (_, Some(r)) => {
            Cassette::replay(cassette_path(&p.root, &t.spec_path, r)).map_err(|e| e.to_string())?
        }
        (Some(r), None) => Cassette::record(cassette_path(&p.root, &t.spec_path, r))
            .api(t.api_dir())
            .ignore_query(
                a.ignore_params
                    .iter()
                    .cloned()
                    .chain(cassette::volatile_params(&t.spec)),
            ),
        (None, None) => Cassette::memory(),
    };
    let env: Arc<dyn EnvSource> = Arc::new(ProcessEnv);
    let opts = ClientOptions {
        env,
        cassette: Some(cas.clone()),
        ..Default::default()
    };
    let client =
        ApiClient::new(t.spec.clone(), t.secrets.clone(), opts).map_err(|e| e.to_string())?;
    let mut vars_json: serde_json::Map<String, Value> = match stream_vars {
        Some(Value::Object(m)) => m,
        _ => serde_json::Map::new(),
    };
    for (k, v) in &a.vars {
        vars_json.insert(k.clone(), Value::String(v.clone()));
    }
    let vars_json = Value::Object(vars_json);
    let mut vars = RunVars::default();
    vars.globals.insert("vars".into(), JValue::from(&vars_json));
    vars.globals.insert(
        "stream".into(),
        JValue::from(&json!({"id": format!("api-test/{endpoint}"), "vars": vars_json})),
    );
    vars.globals.insert(
        "run".into(),
        JValue::from(&json!({"id": "api-test", "started_at": atlatl_core::ids::rfc3339(std::time::SystemTime::now())})),
    );
    vars.item_secrets = a.item_secrets.iter().cloned().collect();
    let items = if a.items.is_empty() {
        None
    } else {
        Some(
            a.items
                .iter()
                .map(|s| {
                    serde_json::from_str::<Value>(s)
                        .map(|v| JValue::from(&v))
                        .map_err(|e| format!("--item `{s}`: {e}"))
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
    };
    let mut out = Sample {
        records: Vec::new(),
        cassette: cas.clone(),
        saved: None,
        pages: 0,
        items: 0,
        decisions: Vec::new(),
        error: None,
    };
    let ep = t
        .spec
        .endpoint(endpoint)
        .ok_or_else(|| format!("API spec `{}` has no endpoint `{endpoint}`", t.spec_path))?;
    let driven = ep.for_each.is_some() || !ep.setup.is_empty() || items.is_some();
    let mut stats: Option<atlatl_http::StatsHandle> = None;
    let run = async {
        if driven {
            let mut d = Driver::start(
                &client,
                endpoint,
                DriveOptions {
                    vars,
                    items,
                    ..Default::default()
                },
            )
            .await?;
            while out.items < a.max_items {
                match d.next().await? {
                    Some(c) => {
                        out.items += 1;
                        out.records.extend(c.records);
                    }
                    None => break,
                }
            }
        } else {
            let mut r = client.endpoint(endpoint, vars)?;
            stats = Some(r.stats());
            while out.pages < a.pages {
                match r.next_page().await? {
                    Some(recs) => {
                        out.records.extend(recs);
                        out.pages = r.stats().get().pages;
                    }
                    None => break,
                }
            }
        }
        Ok::<_, atlatl_core::AtlatlError>(())
    };
    if let Err(e) = run.await {
        out.error = Some(e);
    }
    if let Some(st) = stats.map(|h| h.get()) {
        out.decisions.extend(st.attempts.iter().map(|a| {
            format!(
                "retry attempt {} ({}, waited {} ms): {}",
                a.attempt, a.class, a.wait_ms, a.message
            )
        }));
        out.decisions.extend(
            st.skips
                .iter()
                .map(|k| format!("skipped {}: {}", k.request_key, k.reason)),
        );
        if st.stopped {
            out.decisions.push("stopped by a `stop` rule".into());
        }
    }
    out.saved = cas.save().map_err(|e| e.to_string())?;
    Ok((t, out))
}

fn truncate(s: &str, n: usize) -> String {
    let cut: String = s.chars().take(n).collect();
    if cut.len() < s.len() {
        format!("{cut}… ({} bytes)", s.len())
    } else {
        cut
    }
}

fn print_interaction(i: &Interaction, body_chars: usize) {
    let q = &i.request;
    println!("→ {} {}", q.method, q.url);
    for (k, v) in &q.headers {
        println!("    {k}: {v}");
    }
    if let Some(b) = &q.body {
        println!("    {}", truncate(b, body_chars));
    }
    let r = &i.response;
    let ms = r
        .elapsed_ms
        .map(|m| format!(" ({m} ms)"))
        .unwrap_or_default();
    println!("← {}{ms}", r.status);
    for (k, v) in &r.headers {
        println!("    {k}: {v}");
    }
    match (&r.body, &r.body_base64) {
        (Some(b), _) => println!("    {}", truncate(b, body_chars)),
        (None, Some(b)) => println!("    <binary, {} base64 chars>", b.len()),
        _ => {}
    }
}

/// Would-be field types (as `schema infer` would draft them).
pub fn infer_fields(
    records: &[atlatl_http::Record],
) -> Result<atlatl_ingest::schema::Inferred, String> {
    let c = duckdb::Connection::open_in_memory().map_err(|e| e.to_string())?;
    atlatl_ingest::schema::stage_json(
        &c,
        "sample",
        records.iter().map(|r| (r.ordinal, r.record.to_string())),
    )
    .map_err(|e| e.to_string())?;
    atlatl_ingest::schema::infer(&c, &atlatl_ingest::schema::Staged::records("sample"))
        .map_err(|e| e.to_string())
}

pub fn run(root: &Path, cmd: ApiCmd) -> Result<std::process::ExitCode, String> {
    let (p, _) = crate::load_project(root).map_err(|(s, d)| crate::render(&s, &d))?;
    match cmd {
        ApiCmd::Explain {
            api,
            endpoint,
            env,
            json,
        } => {
            let t = target(&p, &api, env.as_deref(), Secrets::Skip)?;
            let c = ApiClient::new(t.spec.clone(), t.secrets.clone(), ClientOptions::default())
                .map_err(|e| e.to_string())?;
            let v = c.explain(endpoint.as_deref()).map_err(|e| e.to_string())?;
            if json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&v).map_err(|e| e.to_string())?
                );
            } else {
                print!("{}", atlatl_http::explain::render(&v));
            }
            Ok(std::process::ExitCode::SUCCESS)
        }
        ApiCmd::Test(a) => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            let (_, s) = rt.block_on(sample(&p, &a.api, &a.endpoint, &a.sample, None))?;
            let inferred = infer_fields(&s.records).ok();
            if a.json {
                let v = json!({
                    "interactions": s.cassette.interactions(),
                    "pages": s.pages,
                    "items": s.items,
                    "decisions": s.decisions,
                    "records": s.records.iter().take(a.records).map(|r| json!({"ordinal": r.ordinal, "record": serde_json::from_str::<Value>(&r.record.to_string()).unwrap_or(Value::Null)})).collect::<Vec<_>>(),
                    "record_count": s.records.len(),
                    "fields": inferred.as_ref().map(|i| i.fields.iter().map(|f| json!({"name": f.name, "type": atlatl_ingest::schema::type_name(f.kind), "nulls": f.nulls})).collect::<Vec<_>>()),
                    "cassette": s.saved.as_ref().map(|p| p.display().to_string()),
                    "error": s.error.as_ref().map(|e| json!({"class": e.class, "code": e.code, "message": e.message})),
                });
                // Per value: redacting the rendered text lets a sensitive query parameter's mask
                // swallow a URL's closing quote.
                let v = atlatl_core::obs::redacted_json(&v).map_err(|e| e.to_string())?;
                let text = serde_json::to_string_pretty(&v).map_err(|e| e.to_string())?;
                println!("{text}");
            } else {
                if s.cassette.is_replay() {
                    println!(
                        "replayed {} of {} recorded interactions",
                        s.cassette.interactions().len() - s.cassette.unused(),
                        s.cassette.interactions().len()
                    );
                } else {
                    for i in s.cassette.interactions() {
                        print_interaction(&i, a.body_chars);
                    }
                }
                let items = if s.items > 0 {
                    format!(", {} items", s.items)
                } else {
                    String::new()
                };
                println!(
                    "\n{} records over {} pages{items}",
                    s.records.len(),
                    s.pages
                );
                for r in s.records.iter().take(a.records) {
                    println!(
                        "  [{}] {}",
                        r.ordinal,
                        redact(&truncate(&r.record.to_string(), a.body_chars))
                    );
                }
                if s.records.len() > a.records {
                    println!("  … {} more", s.records.len() - a.records);
                }
                if let Some(i) = &inferred
                    && !i.fields.is_empty()
                {
                    println!("\nfields (would-be types):");
                    for f in &i.fields {
                        let null = if f.nulls > 0 {
                            format!(", {} nulls", f.nulls)
                        } else {
                            String::new()
                        };
                        println!(
                            "  {}: {}{null}",
                            f.name,
                            atlatl_ingest::schema::type_name(f.kind)
                        );
                    }
                }
                if !s.decisions.is_empty() {
                    println!("\nrule decisions:");
                    for d in &s.decisions {
                        println!("  {}", redact(d));
                    }
                }
                if let Some(pth) = &s.saved {
                    println!("\ncassette written to {}", pth.display());
                }
                if let Some(e) = &s.error {
                    eprintln!("\nerror: {e}");
                }
            }
            Ok(if s.error.is_some() {
                std::process::ExitCode::from(1)
            } else {
                std::process::ExitCode::SUCCESS
            })
        }
    }
}
