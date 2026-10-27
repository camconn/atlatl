use atlatl_core::error::{AtlatlError, ErrorClass, classify_http_status};
use atlatl_core::redact::redact;

/// Classifies a DuckDB read error: parse/decompression failures are `decode`, missing files
/// `config`, HTTP failures per the status taxonomy.
pub fn classify_duck(msg: &str) -> ErrorClass {
    if let Some(status) = http_status(msg) {
        return match status {
            404 => ErrorClass::Config,
            s => classify_http_status(s).unwrap_or(ErrorClass::Transient),
        };
    }
    let decode = [
        "CSV Error",
        "Error when sniffing file",
        "Malformed JSON",
        "No magic bytes found",
        "Invalid Parquet",
        "gzip",
        "GZIP",
        "zstd",
        "ZSTD",
        "Invalid unicode",
        "not valid UTF",
    ];
    if decode.iter().any(|d| msg.contains(d)) {
        return ErrorClass::Decode;
    }
    let config = [
        "No files found that match the pattern",
        "No such file or directory",
        "Cannot open file",
    ];
    if config.iter().any(|d| msg.contains(d))
        || msg.starts_with("Binder Error")
        || msg.starts_with("Catalog Error")
        || msg.starts_with("Parser Error")
    {
        return ErrorClass::Config;
    }
    if msg.starts_with("Conversion Error") || msg.starts_with("Constraint Error") {
        return ErrorClass::Data;
    }
    if msg.starts_with("HTTP Error") || msg.contains("Could not establish connection") {
        return ErrorClass::Transient;
    }
    ErrorClass::Internal
}

fn http_status(msg: &str) -> Option<u16> {
    let i = msg.find("(HTTP ")?;
    msg[i + 6..].get(..3)?.parse().ok()
}

pub fn duck(e: duckdb::Error) -> AtlatlError {
    let msg = redact(&e.to_string()).into_owned();
    AtlatlError::new(classify_duck(&msg), msg)
}

/// Re-classifies an error from `atlatl_ingest` helpers, which don't know file-reader messages.
pub fn reclassify(e: AtlatlError) -> AtlatlError {
    let class = classify_duck(&e.message);
    if class == ErrorClass::Internal {
        e
    } else {
        AtlatlError { class, ..e }
    }
}

pub fn io_at(path: &std::path::Path, e: std::io::Error) -> AtlatlError {
    let class = match e.kind() {
        std::io::ErrorKind::NotFound => ErrorClass::Config,
        std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::InvalidData => ErrorClass::Decode,
        _ => atlatl_core::error::classify_io(&e),
    };
    AtlatlError::new(class, format!("{}: {e}", path.display())).source(e)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classes() {
        let c = |m: &str| classify_duck(m);
        assert_eq!(
            c("IO Error: No files found that match the pattern \"x\""),
            ErrorClass::Config
        );
        assert_eq!(
            c("Invalid Input Error: CSV Error on Line: 3"),
            ErrorClass::Decode
        );
        assert_eq!(
            c("Invalid Input Error: Malformed JSON in file"),
            ErrorClass::Decode
        );
        assert_eq!(
            c("Invalid Input Error: No magic bytes found at end of file 'x'"),
            ErrorClass::Decode
        );
        assert_eq!(
            c("HTTP Error: HTTP GET error reading 's3://b/k' in region '' (HTTP 404 Not Found)"),
            ErrorClass::Config
        );
        assert_eq!(
            c("HTTP Error: HTTP GET error on 's3://b/k' (HTTP 403 Forbidden)"),
            ErrorClass::Auth
        );
        assert_eq!(
            c("HTTP Error: HTTP GET error on 's3://b/k' (HTTP 503 Slow Down)"),
            ErrorClass::Transient
        );
        assert_eq!(c("Binder Error: bad"), ErrorClass::Config);
        assert_eq!(c("Something else"), ErrorClass::Internal);
    }
}
