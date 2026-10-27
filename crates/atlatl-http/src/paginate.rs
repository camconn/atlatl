//! Pagination strategies (07 §5). `max_pages`, `stop_on_empty` and `stop_when` are enforced here;
//! no-progress detection is done by the runner on the rendered requests.

use atlatl_core::AtlatlError;
use reqwest::Url;

use crate::expr::{Engine, Scope};
use crate::json::{JValue, Map};
use crate::records::Query;
use crate::request::Prepared;
use crate::response::ResponseView;
use crate::spec::{Paginate, PaginateKind};

#[derive(Debug)]
pub struct Pager {
    spec: Option<Paginate>,
    from: Option<Query>,
    total_from: Option<Query>,
    /// 1-based number of the page about to be requested.
    number: u64,
    page: i64,
    offset: u64,
    cursor: Option<String>,
    url: Option<Url>,
    vars: Map,
}

fn text(v: JValue) -> Option<String> {
    match v {
        JValue::Null => None,
        JValue::String(s) if s.is_empty() => None,
        JValue::String(s) => Some(s),
        other => Some(other.to_string()),
    }
}

/// The `rel` target of an RFC 8288 `Link` header.
pub fn link_target(header: &str, rel: &str) -> Option<String> {
    let mut rest = header;
    while let Some(start) = rest.find('<') {
        let end = rest[start..].find('>')? + start;
        let target = &rest[start + 1..end];
        let tail = &rest[end + 1..];
        let params_end = tail.find('<').unwrap_or(tail.len());
        let params = &tail[..params_end];
        let hit = params.split(';').any(|p| {
            let p = p.trim().trim_end_matches(',').trim();
            p.split_once('=').is_some_and(|(k, v)| {
                k.trim().eq_ignore_ascii_case("rel")
                    && v.trim()
                        .trim_matches('"')
                        .split_whitespace()
                        .any(|r| r.eq_ignore_ascii_case(rel))
            })
        });
        if hit {
            return Some(target.to_owned());
        }
        rest = &tail[params_end..];
    }
    None
}

impl Pager {
    pub fn new(spec: Option<&Paginate>) -> Result<Self, AtlatlError> {
        let (from, total_from, page, offset) = match spec.map(|p| &p.kind) {
            Some(PaginateKind::Cursor { from, .. } | PaginateKind::NextUrl { from }) => {
                (Some(Query::compile(from)?), None, 0, 0)
            }
            Some(PaginateKind::Offset {
                total_from, start, ..
            }) => (
                None,
                total_from.as_ref().map(Query::compile).transpose()?,
                0,
                *start,
            ),
            Some(PaginateKind::Page { start, .. }) => (None, None, *start, 0),
            _ => (None, None, 0, 0),
        };
        Ok(Self {
            spec: spec.cloned(),
            from,
            total_from,
            number: 1,
            page,
            offset,
            cursor: None,
            url: None,
            vars: Map::new(),
        })
    }

    /// `page` in request templates.
    pub fn namespace(&self) -> JValue {
        let mut m = self.vars.clone();
        m.insert("number".into(), self.number.into());
        m.insert(
            "cursor".into(),
            self.cursor.clone().map(Into::into).unwrap_or_default(),
        );
        m.insert("offset".into(), self.offset.into());
        if let Some(PaginateKind::Page { .. }) = self.spec.as_ref().map(|p| &p.kind) {
            m.insert("page".into(), self.page.into());
        }
        if let Some(PaginateKind::Offset { limit, .. }) = self.spec.as_ref().map(|p| &p.kind) {
            m.insert("limit".into(), (*limit).into());
        }
        m.insert(
            "url".into(),
            self.url
                .as_ref()
                .map(|u| u.as_str().into())
                .unwrap_or_default(),
        );
        JValue::Object(m)
    }

    pub fn apply(&self, req: &mut Prepared) {
        let Some(p) = &self.spec else { return };
        match &p.kind {
            PaginateKind::Cursor { param, .. } => {
                if let Some(c) = &self.cursor {
                    req.set_param(param, Some(c));
                }
            }
            PaginateKind::Page { param, .. } => req.set_param(param, Some(&self.page.to_string())),
            PaginateKind::Offset {
                param,
                limit_param,
                limit,
                ..
            } => {
                req.set_param(param, Some(&self.offset.to_string()));
                if let Some(l) = limit_param {
                    req.set_param(l, Some(&limit.to_string()));
                }
            }
            PaginateKind::NextUrl { .. } | PaginateKind::LinkHeader { .. } => {
                if let Some(u) = &self.url {
                    req.url = u.clone();
                }
            }
            PaginateKind::Expression { .. } => {}
        }
    }

    /// Moves to the next page after `view` (whose path yielded `count` records). `scope` holds
    /// `response` and `page`. Returns `false` when pagination is done.
    pub fn advance(
        &mut self,
        view: &ResponseView,
        req_url: &Url,
        count: usize,
        engine: &Engine,
        scope: &Scope,
    ) -> Result<bool, AtlatlError> {
        let Some(p) = self.spec.clone() else {
            return Ok(false);
        };
        if p.stop_on_empty && count == 0 {
            return Ok(false);
        }
        if let Some(w) = &p.stop_when
            && engine.eval_bool(w, scope).map_err(|e| {
                AtlatlError::config(format!("stop_when at {}: {}", w.span, e.message))
            })?
        {
            return Ok(false);
        }
        let null = JValue::Null;
        let body = match view.decoded() {
            Ok(b) => &**b,
            Err(_) => &null,
        };
        let more = match &p.kind {
            PaginateKind::Cursor { .. } => {
                let q = self.from.as_ref().expect("cursor query");
                self.cursor = text(q.search(body)?);
                self.cursor.is_some()
            }
            PaginateKind::Page { step, .. } => {
                self.page += step;
                true
            }
            PaginateKind::Offset { limit, .. } => {
                self.offset += limit;
                match &self.total_from {
                    Some(q) => {
                        let total = q.search(body)?;
                        let total = total
                            .as_u64()
                            .or_else(|| total.as_str().and_then(|s| s.parse().ok()));
                        total.is_some_and(|t| self.offset < t)
                    }
                    None => count as u64 >= *limit,
                }
            }
            PaginateKind::NextUrl { .. } => {
                let q = self.from.as_ref().expect("next_url query");
                self.url = text(q.search(body)?)
                    .map(|u| req_url.join(&u))
                    .transpose()
                    .map_err(|e| AtlatlError::data(format!("invalid next_url: {e}")))?;
                self.url.is_some()
            }
            PaginateKind::LinkHeader { rel } => {
                self.url = view
                    .cap
                    .header("link")
                    .and_then(|h| link_target(h, rel))
                    .map(|u| req_url.join(&u))
                    .transpose()
                    .map_err(|e| AtlatlError::data(format!("invalid Link header target: {e}")))?;
                self.url.is_some()
            }
            PaginateKind::Expression { next } => {
                let mut vars = self.vars.clone();
                for (k, e) in next {
                    let v = engine.eval_json(e, scope).map_err(|err| {
                        AtlatlError::config(format!("paginate.next at {}: {}", e.span, err.message))
                    })?;
                    vars.insert(k.clone(), v);
                }
                self.vars = vars;
                true
            }
        };
        if !more {
            return Ok(false);
        }
        self.number += 1;
        if self.number > p.max_pages {
            return Err(AtlatlError::config(format!(
                "pagination exceeded max_pages ({}) at {}",
                p.max_pages, p.span
            ))
            .code("max_pages"));
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn link_header_parsing() {
        let h = r#"<https://api.x.com/items?page=2>; rel="next", <https://api.x.com/items?page=9>; rel="last""#;
        assert_eq!(
            link_target(h, "next").as_deref(),
            Some("https://api.x.com/items?page=2")
        );
        assert_eq!(
            link_target(h, "last").as_deref(),
            Some("https://api.x.com/items?page=9")
        );
        assert_eq!(link_target(h, "prev"), None);
        assert_eq!(
            link_target(r#"</p?a=1,2>; rel="prev next""#, "next").as_deref(),
            Some("/p?a=1,2")
        );
        assert_eq!(link_target("garbage", "next"), None);
    }
}
