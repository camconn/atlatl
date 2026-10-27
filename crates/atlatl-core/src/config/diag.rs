use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::sync::{Arc, RwLock};

use super::node::Span;

/// A config error, located where possible.
#[derive(Clone, Debug, PartialEq)]
pub struct ConfigError {
    pub message: String,
    pub span: Option<Span>,
    /// Secondary locations, e.g. the first definition of a duplicate.
    pub related: Vec<(String, Span)>,
}

impl ConfigError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            span: None,
            related: Vec::new(),
        }
    }

    /// Attaches `span` unless a more specific one is already set.
    pub fn at(mut self, span: Span) -> Self {
        if self.span.is_none() {
            self.span = Some(span);
        }
        self
    }

    pub fn at_opt(self, span: Option<&Span>) -> Self {
        match span {
            Some(s) => self.at(s.clone()),
            None => self,
        }
    }

    pub fn related(mut self, note: impl Into<String>, span: Span) -> Self {
        self.related.push((note.into(), span));
        self
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.span {
            Some(s) => write!(f, "{s}: {}", self.message)?,
            None => write!(f, "{}", self.message)?,
        }
        for (note, s) in &self.related {
            write!(f, " ({note}: {s})")?;
        }
        Ok(())
    }
}

impl std::error::Error for ConfigError {}

impl serde::de::Error for ConfigError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self::new(msg.to_string())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Error,
    Warning,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Diagnostic {
    pub severity: Severity,
    pub code: &'static str,
    pub error: ConfigError,
}

/// Errors and warnings collected while loading; loading continues past independent errors so
/// `validate` reports as many as possible in one pass.
#[derive(Clone, Debug, Default)]
pub struct Diagnostics {
    pub items: Vec<Diagnostic>,
}

impl Diagnostics {
    pub fn error(&mut self, code: &'static str, e: ConfigError) {
        self.items.push(Diagnostic {
            severity: Severity::Error,
            code,
            error: e,
        });
    }

    pub fn warn(&mut self, code: &'static str, e: ConfigError) {
        self.items.push(Diagnostic {
            severity: Severity::Warning,
            code,
            error: e,
        });
    }

    pub fn has_errors(&self) -> bool {
        self.items.iter().any(|d| d.severity == Severity::Error)
    }

    pub fn errors(&self) -> impl Iterator<Item = &Diagnostic> {
        self.items.iter().filter(|d| d.severity == Severity::Error)
    }

    pub fn warnings(&self) -> impl Iterator<Item = &Diagnostic> {
        self.items
            .iter()
            .filter(|d| d.severity == Severity::Warning)
    }

    /// Drops repeats (the same check run once per environment).
    pub fn dedup(&mut self) {
        let mut seen: Vec<Diagnostic> = Vec::new();
        self.items.retain(|d| {
            if seen.contains(d) {
                false
            } else {
                seen.push(d.clone());
                true
            }
        });
    }

    pub fn extend(&mut self, other: Diagnostics) {
        self.items.extend(other.items);
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::Value::Array(
            self.items
                .iter()
                .map(|d| {
                    serde_json::json!({
                        "severity": d.severity,
                        "code": d.code,
                        "message": d.error.message,
                        "file": d.error.span.as_ref().map(|s| s.file.display().to_string()),
                        "line": d.error.span.as_ref().map(|s| s.line),
                        "col": d.error.span.as_ref().map(|s| s.col),
                        "related": d.error.related.iter().map(|(n, s)| serde_json::json!({"note": n, "at": s.to_string()})).collect::<Vec<_>>(),
                    })
                })
                .collect(),
        )
    }
}

/// Source texts by file, for rendering snippets under diagnostics.
#[derive(Clone, Debug, Default)]
pub struct SourceMap {
    files: Arc<RwLock<HashMap<Arc<Path>, Arc<str>>>>,
}

impl SourceMap {
    pub fn insert(&self, path: Arc<Path>, text: Arc<str>) {
        self.files
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(path, text);
    }

    pub fn line(&self, span: &Span) -> Option<String> {
        let files = self.files.read().unwrap_or_else(|e| e.into_inner());
        let text = files.get(&span.file)?;
        text.lines()
            .nth(span.line.checked_sub(1)? as usize)
            .map(str::to_owned)
    }

    /// `error[code]: message` followed by a rustc-style location and snippet.
    pub fn render(&self, d: &Diagnostic) -> String {
        let label = match d.severity {
            Severity::Error => "error",
            Severity::Warning => "warning",
        };
        let mut out = format!("{label}[{}]: {}", d.code, d.error.message);
        if let Some(span) = &d.error.span {
            out.push_str(&format!("\n  --> {span}"));
            if let Some(line) = self.line(span) {
                let caret = " ".repeat(span.col.saturating_sub(1) as usize);
                out.push_str(&format!("\n   |\n   | {line}\n   | {caret}^"));
            }
        }
        for (note, s) in &d.error.related {
            out.push_str(&format!("\n   = {note}: {s}"));
        }
        out
    }
}
