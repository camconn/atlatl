use std::fs::{self, File};
use std::io;
use std::path::{Component, Path, PathBuf};

use atlatl_core::error::{AtlatlError, ErrorClass, Result};
use globset::{GlobBuilder, GlobMatcher};

use crate::error::io_at;
use crate::sniff::{self, ArchiveKind};

/// A member name or glob. A pattern without `/` also matches a member's file name.
pub struct MemberMatcher {
    glob: GlobMatcher,
    by_name: bool,
}

impl MemberMatcher {
    pub fn new(pattern: &str) -> Result<Self> {
        let glob = GlobBuilder::new(pattern)
            .literal_separator(true)
            .build()
            .map_err(|e| AtlatlError::config(format!("archive.member `{pattern}`: {e}")))?
            .compile_matcher();
        Ok(Self {
            glob,
            by_name: !pattern.contains('/'),
        })
    }

    pub fn is_match(&self, member: &str) -> bool {
        self.glob.is_match(member)
            || (self.by_name
                && member
                    .rsplit('/')
                    .next()
                    .is_some_and(|n| self.glob.is_match(n)))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Extracted {
    pub member: String,
    pub path: PathBuf,
}

/// Member file names in archive order.
pub fn members(path: &Path, kind: ArchiveKind) -> Result<Vec<String>> {
    let mut out = Vec::new();
    match kind {
        ArchiveKind::Zip => {
            let mut z = open_zip(path)?;
            for i in 0..z.len() {
                let f = z.by_index(i).map_err(|e| zip_err(path, e))?;
                if f.is_file() {
                    out.push(f.name().map_err(|e| zip_err(path, e))?.into_owned());
                }
            }
        }
        ArchiveKind::Tar => {
            let mut t = open_tar(path)?;
            for e in t.entries().map_err(|e| io_at(path, e))? {
                let e = e.map_err(|e| io_at(path, e))?;
                if e.header().entry_type().is_file() {
                    out.push(
                        e.path()
                            .map_err(|e| io_at(path, e))?
                            .to_string_lossy()
                            .into_owned(),
                    );
                }
            }
        }
    }
    Ok(out)
}

/// Extracts the matching members under `dest`, sorted by member name.
pub fn extract(
    path: &Path,
    kind: ArchiveKind,
    m: &MemberMatcher,
    dest: &Path,
) -> Result<Vec<Extracted>> {
    fs::create_dir_all(dest).map_err(|e| io_at(dest, e))?;
    let mut out = Vec::new();
    match kind {
        ArchiveKind::Zip => {
            let mut z = open_zip(path)?;
            for i in 0..z.len() {
                let mut f = z.by_index(i).map_err(|e| zip_err(path, e))?;
                if !f.is_file() {
                    continue;
                }
                let name = f.name().map_err(|e| zip_err(path, e))?.into_owned();
                if !m.is_match(&name) {
                    continue;
                }
                let rel = f.enclosed_name().ok_or_else(|| unsafe_name(path, &name))?;
                let target = dest.join(rel);
                write_member(&mut f, &target).map_err(|e| {
                    AtlatlError::new(
                        ErrorClass::Decode,
                        format!("{}: member `{name}`: {e}", path.display()),
                    )
                })?;
                out.push(Extracted {
                    member: name,
                    path: target,
                });
            }
        }
        ArchiveKind::Tar => {
            let mut t = open_tar(path)?;
            for e in t.entries().map_err(|e| io_at(path, e))? {
                let mut e = e.map_err(|e| io_at(path, e))?;
                let name = e
                    .path()
                    .map_err(|e| io_at(path, e))?
                    .to_string_lossy()
                    .into_owned();
                if !e.header().entry_type().is_file() || !m.is_match(&name) {
                    continue;
                }
                let rel = enclosed(&name).ok_or_else(|| unsafe_name(path, &name))?;
                let target = dest.join(rel);
                write_member(&mut e, &target).map_err(|err| io_at(path, err))?;
                out.push(Extracted {
                    member: name,
                    path: target,
                });
            }
            // Reach the gzip/zstd trailer so a torn stream is caught.
            io::copy(&mut t.into_inner(), &mut io::sink()).map_err(|e| io_at(path, e))?;
        }
    }
    out.sort_by(|a, b| a.member.cmp(&b.member));
    Ok(out)
}

fn write_member(r: &mut impl io::Read, target: &Path) -> io::Result<()> {
    if let Some(p) = target.parent() {
        fs::create_dir_all(p)?;
    }
    let mut f = File::create(target)?;
    io::copy(r, &mut f)?;
    Ok(())
}

fn enclosed(name: &str) -> Option<PathBuf> {
    let mut out = PathBuf::new();
    for c in Path::new(name).components() {
        match c {
            Component::Normal(p) => out.push(p),
            Component::CurDir => {}
            _ => return None,
        }
    }
    (!out.as_os_str().is_empty()).then_some(out)
}

fn unsafe_name(path: &Path, name: &str) -> AtlatlError {
    AtlatlError::new(
        ErrorClass::Decode,
        format!("{}: member `{name}` escapes the archive", path.display()),
    )
}

fn open_zip(path: &Path) -> Result<zip::ZipArchive<File>> {
    let f = File::open(path).map_err(|e| io_at(path, e))?;
    zip::ZipArchive::new(f).map_err(|e| zip_err(path, e))
}

fn open_tar(path: &Path) -> Result<tar::Archive<Box<dyn io::Read>>> {
    let c = sniff::compression(path)?;
    Ok(tar::Archive::new(sniff::decoder(path, c)?))
}

fn zip_err(path: &Path, e: zip::result::ZipError) -> AtlatlError {
    let class = match &e {
        zip::result::ZipError::Io(io) if io.kind() == io::ErrorKind::NotFound => ErrorClass::Config,
        _ => ErrorClass::Decode,
    };
    AtlatlError::new(class, format!("{}: {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching() {
        let m = MemberMatcher::new("US_*.csv").expect("glob");
        assert!(m.is_match("US_FIN.csv"));
        assert!(m.is_match("dir/US_FIN.csv"));
        assert!(!m.is_match("US_FIN.txt"));
        let m = MemberMatcher::new("dir/*.csv").expect("glob");
        assert!(m.is_match("dir/a.csv"));
        assert!(!m.is_match("other/dir/a.csv"));
        let m = MemberMatcher::new("exact.csv").expect("glob");
        assert!(m.is_match("exact.csv"));
        assert!(!m.is_match("exact.csv.bak"));
    }

    #[test]
    fn enclosed_names() {
        assert_eq!(enclosed("a/./b.csv"), Some(PathBuf::from("a/b.csv")));
        assert_eq!(enclosed("../x"), None);
        assert_eq!(enclosed("/etc/passwd"), None);
    }
}
