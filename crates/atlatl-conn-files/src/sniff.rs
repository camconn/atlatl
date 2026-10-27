use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;

use atlatl_core::config::model::FileFormat;
use atlatl_core::error::{AtlatlError, Result};

use crate::error::io_at;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compression {
    None,
    Gzip,
    Zstd,
}

impl Compression {
    pub fn as_duckdb(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Gzip => "gzip",
            Compression::Zstd => "zstd",
        }
    }

    pub fn from_magic(head: &[u8]) -> Self {
        match head {
            [0x1f, 0x8b, ..] => Compression::Gzip,
            [0x28, 0xb5, 0x2f, 0xfd, ..] => Compression::Zstd,
            _ => Compression::None,
        }
    }

    /// For objects we can't sniff cheaply (S3).
    pub fn from_name(name: &str) -> Self {
        let n = name.to_ascii_lowercase();
        if n.ends_with(".gz") || n.ends_with(".gzip") {
            Compression::Gzip
        } else if n.ends_with(".zst") || n.ends_with(".zstd") {
            Compression::Zstd
        } else {
            Compression::None
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArchiveKind {
    Zip,
    Tar,
}

pub fn head(path: &Path, n: usize) -> Result<Vec<u8>> {
    let mut f = File::open(path).map_err(|e| io_at(path, e))?;
    let mut buf = vec![0; n];
    let mut read = 0;
    while read < n {
        match f.read(&mut buf[read..]).map_err(|e| io_at(path, e))? {
            0 => break,
            k => read += k,
        }
    }
    buf.truncate(read);
    Ok(buf)
}

pub fn compression(path: &Path) -> Result<Compression> {
    Ok(Compression::from_magic(&head(path, 4)?))
}

/// Zip by magic; tar by the `ustar` marker, after gzip/zstd decompression.
pub fn archive_kind(path: &Path) -> Result<Option<ArchiveKind>> {
    let h = head(path, 4)?;
    if h.starts_with(b"PK\x03\x04") || h.starts_with(b"PK\x05\x06") {
        return Ok(Some(ArchiveKind::Zip));
    }
    let mut r = decoder(path, Compression::from_magic(&h))?;
    let mut block = vec![0; 263];
    let n = read_full(&mut r, &mut block).map_err(|e| io_at(path, e))?;
    Ok((n >= 262 && &block[257..262] == b"ustar").then_some(ArchiveKind::Tar))
}

fn read_full(r: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match r.read(&mut buf[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
    }
    Ok(n)
}

pub fn decoder(path: &Path, c: Compression) -> Result<Box<dyn Read>> {
    let f = BufReader::new(File::open(path).map_err(|e| io_at(path, e))?);
    Ok(match c {
        Compression::None => Box::new(f),
        Compression::Gzip => Box::new(flate2::read::MultiGzDecoder::new(f)),
        Compression::Zstd => {
            Box::new(zstd::stream::read::Decoder::new(f).map_err(|e| io_at(path, e))?)
        }
    })
}

/// Decodes the whole stream. DuckDB returns a prefix of a torn gzip/zstd stream without error,
/// so local compressed files are checked before they're read.
pub fn verify(path: &Path, c: Compression) -> Result<()> {
    if c == Compression::None {
        return Ok(());
    }
    let mut r = decoder(path, c)?;
    io::copy(&mut r, &mut io::sink()).map_err(|e| {
        AtlatlError::new(
            atlatl_core::error::ErrorClass::Decode,
            format!(
                "{}: corrupt or truncated {} stream: {e}",
                path.display(),
                c.as_duckdb()
            ),
        )
    })?;
    Ok(())
}

/// Format from the file name, ignoring a compression suffix.
pub fn format_from_name(name: &str) -> Option<FileFormat> {
    let n = name.to_ascii_lowercase();
    let n = [".gz", ".gzip", ".zst", ".zstd"]
        .iter()
        .find_map(|s| n.strip_suffix(s))
        .unwrap_or(&n);
    let ext = n.rsplit_once('.')?.1;
    Some(match ext {
        "csv" | "tsv" | "txt" => FileFormat::Csv,
        "parquet" | "pq" => FileFormat::Parquet,
        "json" => FileFormat::Json,
        "jsonl" | "ndjson" => FileFormat::Jsonl,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert_eq!(format_from_name("a/B.CSV.gz"), Some(FileFormat::Csv));
        assert_eq!(format_from_name("x.parquet"), Some(FileFormat::Parquet));
        assert_eq!(format_from_name("x.ndjson.zst"), Some(FileFormat::Jsonl));
        assert_eq!(format_from_name("x.json"), Some(FileFormat::Json));
        assert_eq!(format_from_name("0000_part_00"), None);
        assert_eq!(Compression::from_name("a.csv.GZ"), Compression::Gzip);
        assert_eq!(Compression::from_name("a.zst"), Compression::Zstd);
        assert_eq!(Compression::from_name("a.csv"), Compression::None);
        assert_eq!(Compression::from_magic(&[0x1f, 0x8b, 8]), Compression::Gzip);
        assert_eq!(
            Compression::from_magic(&[0x28, 0xb5, 0x2f, 0xfd]),
            Compression::Zstd
        );
        assert_eq!(Compression::from_magic(b"id,"), Compression::None);
    }
}
