//! Logs to stderr via `tracing` (spec 10 §2). Every formatted line passes through the global
//! redactor before it is written.

use std::io::{self, IsTerminal, Write};
use std::str::FromStr;

use serde_json::Value;
use tracing::{Metadata, Span, Subscriber};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::util::SubscriberInitExt;

pub const LOG_ENV: &str = "ATLATL_LOG";
const DEFAULT_FILTER: &str = "info";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogFormat {
    Text,
    Json,
}

impl LogFormat {
    /// Text on a TTY, JSON otherwise.
    pub fn detect() -> Self {
        if io::stderr().is_terminal() {
            Self::Text
        } else {
            Self::Json
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Json => "json",
        }
    }
}

impl FromStr for LogFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "text" => Ok(Self::Text),
            "json" => Ok(Self::Json),
            _ => Err(format!("unknown log format `{s}` (expected text or json)")),
        }
    }
}

/// The filter from `ATLATL_LOG`, else `RUST_LOG`, else `info`. An invalid spec falls back to
/// `info` and returns the parse error.
pub fn env_filter() -> (EnvFilter, Option<String>) {
    let spec = [LOG_ENV, "RUST_LOG"]
        .iter()
        .filter_map(|k| std::env::var(k).ok())
        .find(|v| !v.trim().is_empty());
    match spec {
        None => (EnvFilter::new(DEFAULT_FILTER), None),
        Some(s) => match EnvFilter::try_new(&s) {
            Ok(f) => (f, None),
            Err(e) => (
                EnvFilter::new(DEFAULT_FILTER),
                Some(format!("invalid log filter `{s}`: {e}")),
            ),
        },
    }
}

/// A subscriber writing `format` lines to `writer`, redacted.
pub fn subscriber<W>(
    format: LogFormat,
    filter: EnvFilter,
    writer: W,
    ansi: bool,
) -> Box<dyn Subscriber + Send + Sync>
where
    W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    let b = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(RedactingMakeWriter::new(writer, format));
    match format {
        LogFormat::Text => Box::new(b.with_ansi(ansi).finish()),
        LogFormat::Json => Box::new(
            b.json()
                .with_current_span(false)
                .with_span_list(true)
                .finish(),
        ),
    }
}

/// Installs the global stderr subscriber (and the `log` bridge). `None` detects the format.
/// Returns false if a global subscriber was already set.
pub fn init(format: Option<LogFormat>) -> bool {
    let format = format.unwrap_or_else(LogFormat::detect);
    let ansi = format == LogFormat::Text && io::stderr().is_terminal();
    let (filter, bad) = env_filter();
    let ok = subscriber(format, filter, io::stderr, ansi)
        .try_init()
        .is_ok();
    if let Some(e) = bad {
        tracing::warn!("{e}");
    }
    ok
}

#[derive(Clone, Debug)]
pub struct RedactingMakeWriter<M> {
    inner: M,
    json: bool,
}

impl<M> RedactingMakeWriter<M> {
    pub fn new(inner: M, format: LogFormat) -> Self {
        Self {
            inner,
            json: format == LogFormat::Json,
        }
    }
}

impl<'a, M: MakeWriter<'a>> MakeWriter<'a> for RedactingMakeWriter<M> {
    type Writer = RedactingWriter<M::Writer>;

    fn make_writer(&'a self) -> Self::Writer {
        RedactingWriter::new(self.inner.make_writer(), self.json)
    }

    fn make_writer_for(&'a self, meta: &Metadata<'_>) -> Self::Writer {
        RedactingWriter::new(self.inner.make_writer_for(meta), self.json)
    }
}

/// Buffers one formatted event and writes it redacted on drop, so a secret split across
/// `write` calls is still masked.
pub struct RedactingWriter<W: Write> {
    inner: W,
    buf: Vec<u8>,
    json: bool,
}

impl<W: Write> RedactingWriter<W> {
    fn new(inner: W, json: bool) -> Self {
        Self {
            inner,
            buf: Vec::new(),
            json,
        }
    }
}

impl<W: Write> Write for RedactingWriter<W> {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(b);
        Ok(b.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<W: Write> Drop for RedactingWriter<W> {
    fn drop(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        let out = redact_output(&String::from_utf8_lossy(&self.buf), self.json);
        super::status::around(|| {
            let _ = self.inner.write_all(out.as_bytes());
            let _ = self.inner.flush();
        });
    }
}

/// Redacts formatted log output. JSON lines are redacted per string value, so secrets that
/// JSON escaping would alter are still caught.
pub fn redact_output(s: &str, json: bool) -> String {
    if !json {
        return crate::redact::redact(s).into_owned();
    }
    let mut out = String::with_capacity(s.len());
    for line in s.split_inclusive('\n') {
        let (body, nl) = match line.strip_suffix('\n') {
            Some(b) => (b, "\n"),
            None => (line, ""),
        };
        match serde_json::from_str::<Value>(body) {
            Ok(mut v) => {
                super::redact_value(&mut v);
                out.push_str(&v.to_string());
            }
            Err(_) => out.push_str(&crate::redact::redact(body)),
        }
        out.push_str(nl);
    }
    out
}

pub fn run_span(run_id: &str) -> Span {
    tracing::info_span!("run", run_id = %run_id)
}

/// `attempt` is the replication-level attempt (`replication_retries`), from 1.
pub fn replication_span(replication: &str, attempt: u32) -> Span {
    tracing::info_span!("replication", replication = %replication, attempt)
}

pub fn stream_span(stream_id: &str) -> Span {
    tracing::info_span!("stream", stream_id = %stream_id)
}

pub fn member_span(member: &str) -> Span {
    tracing::info_span!("member", member = %member)
}

/// A step (extract/stage/validate/apply/commit) or a request/statement, at retry `attempt`.
pub fn step_span(step: &str, attempt: u32) -> Span {
    tracing::info_span!("step", step = %step, attempt)
}
