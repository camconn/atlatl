//! Just enough XML for STS and S3 responses: flat tag lookup and entity decoding.

/// Inner text of every `<name>…</name>`, in order (no attributes, no nesting of the same tag).
pub fn all<'a>(doc: &'a str, name: &str) -> Vec<&'a str> {
    let (open, close) = (format!("<{name}>"), format!("</{name}>"));
    let mut out = Vec::new();
    let mut rest = doc;
    while let Some(i) = rest.find(&open) {
        let after = &rest[i + open.len()..];
        let Some(j) = after.find(&close) else { break };
        out.push(&after[..j]);
        rest = &after[j + close.len()..];
    }
    out
}

pub fn first<'a>(doc: &'a str, name: &str) -> Option<&'a str> {
    all(doc, name).into_iter().next()
}

pub fn text(doc: &str, name: &str) -> Option<String> {
    first(doc, name).map(unescape)
}

pub fn unescape(s: &str) -> String {
    if !s.contains('&') {
        return s.to_owned();
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find('&') {
        out.push_str(&rest[..i]);
        let tail = &rest[i..];
        let Some(end) = tail.find(';') else {
            out.push_str(tail);
            return out;
        };
        let ent = &tail[1..end];
        let ch = match ent {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            _ => ent
                .strip_prefix("#x")
                .and_then(|h| u32::from_str_radix(h, 16).ok())
                .or_else(|| ent.strip_prefix('#').and_then(|d| d.parse().ok()))
                .and_then(char::from_u32),
        };
        match ch {
            Some(c) => out.push(c),
            None => out.push_str(&tail[..=end]),
        }
        rest = &tail[end + 1..];
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tags() {
        let d = "<R><Contents><Key>a&amp;b</Key></Contents><Contents><Key>c&#x2F;d&#47;</Key></Contents><IsTruncated>false</IsTruncated></R>";
        assert_eq!(
            all(d, "Key").into_iter().map(unescape).collect::<Vec<_>>(),
            ["a&b", "c/d/"]
        );
        assert_eq!(text(d, "IsTruncated").as_deref(), Some("false"));
        assert_eq!(text(d, "Nope"), None);
        assert_eq!(unescape("x &bogus; y&"), "x &bogus; y&");
    }
}
