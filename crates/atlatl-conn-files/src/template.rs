use atlatl_core::config::model::FilesSelector;
use atlatl_core::error::{AtlatlError, Result};
use atlatl_core::template::{Context, Templates, to_value};
use indexmap::IndexMap;

/// Renders `url`, `glob` and `archive.member` with `vars.*` bound (Hive-style paths such as
/// `s3://b/t/date={{ vars.date }}/*.parquet`).
pub fn render_selector(
    sel: &FilesSelector,
    templates: &Templates,
    ctx: &Context,
    vars: Option<&IndexMap<String, serde_json::Value>>,
) -> Result<FilesSelector> {
    let ctx = ctx.clone().with(
        "vars",
        vars.map(to_value)
            .unwrap_or_else(|| to_value(IndexMap::<String, String>::new())),
    );
    let r = |s: &str, what: &str| {
        templates
            .render(s, &ctx)
            .map_err(|e| AtlatlError::config(format!("from.files.{what}: {e}")))
    };
    let mut out = sel.clone();
    out.url = r(&sel.url, "url")?;
    if let Some(g) = &sel.glob {
        out.glob = Some(r(g, "glob")?);
    }
    if let Some(a) = &mut out.archive {
        a.member = r(&a.member, "archive.member")?;
    }
    Ok(out)
}
