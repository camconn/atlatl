use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use atlatl_core::config::model::{
    CAST_NULLED, ChunkBy, ChunkConfig, ColumnsConfig, CsvOptions, FileFormat, FilesConn,
    FilesSelector, Naming, OnNoMatch, SftpConfig, StorageConfig, StreamConfig,
};
use atlatl_core::error::{AtlatlError, ErrorClass, Result};
use atlatl_ingest::drift;
use atlatl_ingest::sql::{ident, literal};
use atlatl_ingest::staging::{self, Column};
use duckdb::Connection;

use crate::archive::{self, MemberMatcher};
use crate::error::{duck, io_at, reclassify};
use crate::http::{Download, Downloader, display_url};
use crate::manifest;
use crate::s3;
use crate::sftp::{self, SftpClient, SftpTarget, SftpUrl};
use crate::sniff::{self, Compression};

/// Everything a files read needs, detached from the config tree.
#[derive(Clone, Debug)]
pub struct FilesRead {
    pub selector: FilesSelector,
    /// Base for relative local paths (`FilesConn.root`).
    pub root: Option<PathBuf>,
    pub storage: Option<StorageConfig>,
    /// Server and credentials for `sftp://` URLs (`FilesConn.sftp`).
    pub sftp: Option<SftpConfig>,
    pub columns: Option<ColumnsConfig>,
    pub naming: Naming,
    pub on_no_match: OnNoMatch,
    /// `url` is a Redshift UNLOAD manifest; its entries are the files.
    pub manifest: bool,
    /// Adds `k=v` path segments as columns.
    pub hive_partitioning: bool,
    /// `on_cast_error.set_null`: a declared column's value that doesn't fit reads as NULL.
    pub set_null: bool,
}

impl FilesRead {
    pub fn new(selector: FilesSelector) -> Self {
        Self {
            selector,
            root: None,
            storage: None,
            sftp: None,
            columns: None,
            naming: Naming::Source,
            on_no_match: OnNoMatch::Error,
            manifest: false,
            hive_partitioning: false,
            set_null: false,
        }
    }

    pub fn from_stream(stream: &StreamConfig, conn: Option<&FilesConn>) -> Result<Self> {
        let from = stream
            .from
            .as_ref()
            .ok_or_else(|| AtlatlError::config("stream has no `from`"))?;
        let sel = from
            .files
            .clone()
            .ok_or_else(|| AtlatlError::config("`from.files` is required for a files source"))?;
        let mut r = Self::new(sel);
        r.root = conn.and_then(|c| c.root.as_ref()).map(PathBuf::from);
        r.storage = conn.and_then(|c| c.storage.clone());
        r.sftp = conn.and_then(|c| c.sftp.clone());
        r.columns = stream.columns.clone();
        r.naming = stream.naming.unwrap_or(Naming::Source);
        r.on_no_match = from.on_no_match.unwrap_or(OnNoMatch::Error);
        r.manifest = r.selector.manifest.unwrap_or(false);
        r.hive_partitioning = r.selector.hive_partitioning.unwrap_or(false);
        r.set_null = stream.set_null();
        Ok(r)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Location {
    /// A path or glob.
    Local(String),
    /// An `s3://` URL or glob.
    S3(String),
    Https(String),
    /// An `sftp://` path or glob, listed and downloaded by [`FilesSource::fetch`].
    Sftp(SftpUrl),
}

impl Location {
    pub fn parse(url: &str, glob: Option<&str>, root: Option<&Path>) -> Result<Self> {
        let join = |base: &str| match glob {
            Some(g) => format!(
                "{}/{}",
                base.trim_end_matches('/'),
                g.trim_start_matches('/')
            ),
            None => base.to_owned(),
        };
        if url.starts_with("https://") || url.starts_with("http://") {
            if glob.is_some() {
                return Err(AtlatlError::config(format!(
                    "from.files.glob can't be used with an HTTP URL ({})",
                    display_url(url)
                )));
            }
            return Ok(Location::Https(url.to_owned()));
        }
        if url.starts_with("sftp://") {
            return Ok(Location::Sftp(SftpUrl::parse(&join(url))?));
        }
        if url.starts_with("s3://") {
            if s3::bucket(url).is_none() {
                return Err(AtlatlError::config(format!("`{url}` has no bucket")));
            }
            return Ok(Location::S3(join(url)));
        }
        let path = if let Some(p) = url.strip_prefix("file://") {
            p
        } else if let Some((scheme, _)) = url.split_once("://") {
            return Err(AtlatlError::config(format!(
                "from.files.url: unsupported scheme `{scheme}://` (local path, file://, s3://, https://, sftp://)"
            )));
        } else {
            url
        };
        let p = Path::new(path);
        let full = match root {
            Some(r) if p.is_relative() => r.join(p),
            _ => p.to_owned(),
        };
        Ok(Location::Local(join(&full.to_string_lossy())))
    }

    fn shown(&self) -> String {
        match self {
            Location::Local(p) | Location::S3(p) => p.clone(),
            Location::Https(u) => display_url(u).to_owned(),
            Location::Sftp(u) => u.display(),
        }
    }
}

/// A resolved files source. HTTPS and SFTP sources need [`FilesSource::fetch`] before discovery.
#[derive(Debug)]
pub struct FilesSource {
    read: FilesRead,
    loc: Location,
    fetched: Option<PathBuf>,
    /// SFTP: the matched files, downloaded.
    remote: Option<Vec<FileEntry>>,
    downloads: Vec<Download>,
}

/// One file DuckDB reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileEntry {
    /// For reporting: the URL or path, or `<archive>!<member>`.
    pub name: String,
    /// What DuckDB reads: a local path or `s3://` URL.
    pub path: String,
    pub size: Option<u64>,
    /// From a VERBOSE manifest.
    pub record_count: Option<u64>,
    pub compression: Compression,
}

impl FilesSource {
    pub fn new(read: FilesRead) -> Result<Self> {
        let loc = Location::parse(
            &read.selector.url,
            read.selector.glob.as_deref(),
            read.root.as_deref(),
        )?;
        if read.manifest && read.selector.archive.is_some() {
            return Err(AtlatlError::config(
                "a manifest source can't select archive members",
            ));
        }
        Ok(Self {
            read,
            loc,
            fetched: None,
            remote: None,
            downloads: Vec::new(),
        })
    }

    pub fn location(&self) -> &Location {
        &self.loc
    }

    pub fn needs_fetch(&self) -> bool {
        match self.loc {
            Location::Https(_) => self.fetched.is_none(),
            Location::Sftp(_) => self.remote.is_none(),
            _ => false,
        }
    }

    /// Per-file downloads of the last [`FilesSource::fetch`].
    pub fn downloads(&self) -> &[Download] {
        &self.downloads
    }

    /// Downloads an HTTPS source (or manifest) into `workdir`, or lists and downloads every
    /// file an SFTP path or glob matches (retrying with `dl`'s policy); a no-op for other
    /// locations. For SFTP the result sums the per-file [`FilesSource::downloads`], with `path`
    /// the download directory.
    pub async fn fetch(&mut self, dl: &Downloader, workdir: &Path) -> Result<Option<Download>> {
        if let Location::Sftp(u) = &self.loc {
            return self.fetch_sftp(u.clone(), dl, workdir).await.map(Some);
        }
        let Location::Https(url) = &self.loc else {
            return Ok(None);
        };
        let name = display_url(url)
            .rsplit('/')
            .next()
            .filter(|n| !n.is_empty())
            .map(sanitize)
            .unwrap_or_else(|| "download".into());
        let d = dl
            .download(url, &workdir.join("download").join(name))
            .await?;
        self.fetched = Some(d.path.clone());
        self.downloads = vec![d.clone()];
        Ok(Some(d))
    }

    async fn fetch_sftp(
        &mut self,
        u: SftpUrl,
        dl: &Downloader,
        workdir: &Path,
    ) -> Result<Download> {
        let target = SftpTarget::new(&u, self.read.sftp.as_ref())?;
        let shown = |p: &str| {
            let port = if target.port == 22 {
                String::new()
            } else {
                format!(":{}", target.port)
            };
            format!("sftp://{}{port}{p}", target.host)
        };
        let dir = workdir.join("download").join("sftp");
        let client = SftpClient::new(target.clone(), dl.policy().clone());
        let res = async {
            let files = client.list(&u.path).await?;
            let mut entries = Vec::with_capacity(files.len());
            let mut downloads = Vec::with_capacity(files.len());
            for f in &files {
                let d = client.download(f, &sftp::local_path(&dir, &f.path)).await?;
                entries.push(FileEntry {
                    name: shown(&f.path),
                    path: d.path.to_string_lossy().into_owned(),
                    size: Some(d.bytes),
                    record_count: None,
                    compression: Compression::None,
                });
                downloads.push(d);
            }
            Ok::<_, AtlatlError>((entries, downloads))
        }
        .await;
        client.close().await;
        let (entries, downloads) = res?;
        let total = Download {
            path: dir,
            bytes: downloads.iter().map(|d| d.bytes).sum(),
            attempts: downloads.iter().map(|d| d.attempts).sum(),
            resumed: downloads.iter().map(|d| d.resumed).sum(),
            retries: downloads.iter().flat_map(|d| d.retries.clone()).collect(),
        };
        self.remote = Some(entries);
        self.downloads = downloads;
        Ok(total)
    }

    /// Lists, downloads (S3 archives) and extracts what the selector matches, checks
    /// compressed streams, and probes the schema. `workdir` is run-scoped scratch space.
    pub fn discover(&self, c: &Connection, workdir: &Path) -> Result<FileSet> {
        let sel = &self.read.selector;
        let mut buckets = BTreeSet::new();
        if let Location::S3(u) = &self.loc {
            buckets.extend(s3::bucket(u).map(str::to_owned));
        }
        self.prepare_s3(c, &buckets)?;

        let mut listed = if self.read.manifest {
            let entries = self.manifest_entries(c)?;
            let more: BTreeSet<String> = entries
                .iter()
                .filter_map(|e| s3::bucket(&e.path).map(str::to_owned))
                .filter(|b| !buckets.contains(b))
                .collect();
            self.prepare_s3(c, &more)?;
            entries
        } else {
            match &self.loc {
                Location::Local(p) | Location::S3(p) => list(c, p)?,
                Location::Https(u) => {
                    let p = self.fetched.as_ref().ok_or_else(|| {
                        AtlatlError::internal(format!(
                            "{} was not fetched before discovery",
                            display_url(u)
                        ))
                    })?;
                    let size = std::fs::metadata(p).map_err(|e| io_at(p, e))?.len();
                    vec![FileEntry {
                        name: display_url(u).to_owned(),
                        path: p.to_string_lossy().into_owned(),
                        size: Some(size),
                        record_count: None,
                        compression: Compression::None,
                    }]
                }
                Location::Sftp(_) => self.sftp_files()?.to_vec(),
            }
        };

        if let Some(a) = &sel.archive {
            let m = MemberMatcher::new(&a.member)?;
            let mut out = Vec::new();
            for (i, f) in listed.iter().enumerate() {
                let local =
                    self.local_archive(c, f, &workdir.join("archives").join(i.to_string()))?;
                let kind = sniff::archive_kind(&local)?.ok_or_else(|| {
                    AtlatlError::config(format!(
                        "{} is not a zip or tar archive, but archive.member is set",
                        f.name
                    ))
                })?;
                let got = archive::extract(
                    &local,
                    kind,
                    &m,
                    &workdir.join("extract").join(i.to_string()),
                )?;
                if got.is_empty() && self.read.on_no_match == OnNoMatch::Error {
                    let members = archive::members(&local, kind)?;
                    return Err(AtlatlError::config(format!(
                        "no member of {} matches `{}` (members: {})",
                        f.name,
                        a.member,
                        members.join(", ")
                    )));
                }
                for x in got {
                    let size = std::fs::metadata(&x.path)
                        .map_err(|e| io_at(&x.path, e))?
                        .len();
                    out.push(FileEntry {
                        name: format!("{}!{}", f.name, x.member),
                        path: x.path.to_string_lossy().into_owned(),
                        size: Some(size),
                        record_count: None,
                        compression: Compression::None,
                    });
                }
            }
            listed = out;
        }

        let mut warnings = Vec::new();
        if listed.is_empty() {
            let msg = format!("no files match `{}`", self.loc.shown());
            match self.read.on_no_match {
                OnNoMatch::Error => return Err(AtlatlError::config(msg)),
                OnNoMatch::Warn => warnings.push(msg),
            }
        }

        for f in &mut listed {
            if f.path.starts_with("s3://") {
                f.compression = Compression::from_name(&f.path);
                continue;
            }
            let p = Path::new(&f.path);
            if sel.archive.is_none()
                && let Some(k) = sniff::archive_kind(p)?
            {
                return Err(AtlatlError::config(format!(
                    "{} is a {} archive; set from.files.archive.member",
                    f.name,
                    match k {
                        sniff::ArchiveKind::Zip => "zip",
                        sniff::ArchiveKind::Tar => "tar",
                    }
                )));
            }
            f.compression = sniff::compression(p)?;
            sniff::verify(p, f.compression)?;
        }

        let format = match sel.format {
            Some(f) => f,
            None => detect_format(&listed, self.read.manifest)?,
        };
        let compression = match format {
            FileFormat::Parquet => Compression::None,
            _ => {
                let set: BTreeSet<_> = listed.iter().map(|f| f.compression.as_duckdb()).collect();
                if set.len() > 1 {
                    return Err(AtlatlError::config(format!(
                        "matched files mix compressions ({}); select them separately",
                        set.into_iter().collect::<Vec<_>>().join(", ")
                    )));
                }
                listed
                    .first()
                    .map(|f| f.compression)
                    .unwrap_or(Compression::None)
            }
        };

        let mut fs = FileSet {
            files: listed,
            format,
            warnings,
            compression,
            csv: csv_options(sel.csv.as_ref())?,
            union_by_name: sel.union_by_name.unwrap_or(false),
            hive: self.read.hive_partitioning,
            hive_keys: Vec::new(),
            source: Vec::new(),
            aliases: Vec::new(),
            declared: Vec::new(),
            set_null: self.read.set_null,
        };
        if fs.files.is_empty() {
            return Ok(fs);
        }
        fs.probe(
            c,
            self.read.naming,
            self.read.columns.as_ref(),
            !self.read.manifest,
        )?;
        Ok(fs)
    }

    fn prepare_s3(&self, c: &Connection, buckets: &BTreeSet<String>) -> Result<()> {
        if buckets.is_empty() {
            return Ok(());
        }
        let st = self.read.storage.as_ref();
        for ext in s3::extensions(st) {
            c.execute_batch(&format!("LOAD {ext}")).map_err(|e| {
                AtlatlError::config(format!("S3 reads need the `{ext}` extension: {e}"))
            })?;
        }
        // Secrets are engine-wide: replications sharing a bucket in one process would otherwise
        // race `CREATE OR REPLACE SECRET` into a catalog write-write conflict.
        static SECRETS: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _g = SECRETS.lock().unwrap_or_else(|e| e.into_inner());
        for b in buckets {
            let name = format!(
                "atlatl_files_{}",
                b.chars()
                    .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                    .collect::<String>()
            );
            if let Some(sql) = s3::secret_sql(&name, st, b)? {
                c.execute_batch(&sql).map_err(duck)?;
            }
        }
        Ok(())
    }

    fn manifest_entries(&self, c: &Connection) -> Result<Vec<FileEntry>> {
        let (text, origin) = match &self.loc {
            Location::Local(p) => (
                std::fs::read_to_string(p).map_err(|e| io_at(Path::new(p), e))?,
                p.clone(),
            ),
            Location::S3(u) => {
                let t: String = c
                    .query_row(
                        &format!("SELECT content FROM read_text({})", literal(u)),
                        [],
                        |r| r.get(0),
                    )
                    .map_err(duck)?;
                (t, u.clone())
            }
            Location::Https(u) => {
                let p = self
                    .fetched
                    .as_ref()
                    .ok_or_else(|| AtlatlError::internal("manifest was not fetched"))?;
                (
                    std::fs::read_to_string(p).map_err(|e| io_at(p, e))?,
                    display_url(u).to_owned(),
                )
            }
            Location::Sftp(u) => {
                let f = self.sftp_files()?.first().ok_or_else(|| {
                    AtlatlError::config(format!("manifest {} not found", u.display()))
                })?;
                (
                    std::fs::read_to_string(&f.path).map_err(|e| io_at(Path::new(&f.path), e))?,
                    f.name.clone(),
                )
            }
        };
        manifest::parse(&text, &origin)?
            .into_iter()
            .map(|e| {
                if e.url.starts_with("https://") || e.url.starts_with("http://") {
                    return Err(AtlatlError::config(format!(
                        "manifest {origin}: HTTP entries are not supported ({})",
                        display_url(&e.url)
                    )));
                }
                let path = e.url.strip_prefix("file://").unwrap_or(&e.url).to_owned();
                Ok(FileEntry {
                    name: e.url.clone(),
                    path,
                    size: e.content_length,
                    record_count: e.record_count,
                    compression: Compression::None,
                })
            })
            .collect()
    }

    fn sftp_files(&self) -> Result<&[FileEntry]> {
        self.remote.as_deref().ok_or_else(|| {
            AtlatlError::internal(format!(
                "{} was not fetched before discovery",
                self.loc.shown()
            ))
        })
    }

    /// A local copy of an archive; S3 objects are read whole through DuckDB.
    fn local_archive(&self, c: &Connection, f: &FileEntry, dir: &Path) -> Result<PathBuf> {
        if !f.path.starts_with("s3://") {
            return Ok(PathBuf::from(&f.path));
        }
        let bytes: Vec<u8> = c
            .query_row(
                &format!("SELECT content FROM read_blob({})", literal(&f.path)),
                [],
                |r| r.get(0),
            )
            .map_err(duck)?;
        std::fs::create_dir_all(dir).map_err(|e| io_at(dir, e))?;
        let name = f.path.rsplit('/').next().map(sanitize).unwrap_or_default();
        let p = dir.join(if name.is_empty() {
            "archive".into()
        } else {
            name
        });
        std::fs::write(&p, bytes).map_err(|e| io_at(&p, e))?;
        Ok(p)
    }
}

fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn list(c: &Connection, pattern: &str) -> Result<Vec<FileEntry>> {
    let sql = format!(
        "SELECT filename, size FROM read_blob({}) ORDER BY filename",
        literal(pattern)
    );
    let mut s = c.prepare(&sql).map_err(duck)?;
    let rows = s.query_map([], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, Option<i64>>(1)?))
    });
    let rows: Vec<(String, Option<i64>)> = match rows {
        Ok(it) => it.collect::<std::result::Result<_, _>>(),
        Err(e) => Err(e),
    }
    .or_else(|e| {
        let m = e.to_string();
        if m.contains("No files found") {
            Ok(Vec::new())
        } else {
            Err(duck(e))
        }
    })?;
    Ok(rows
        .into_iter()
        .map(|(f, size)| FileEntry {
            name: f.clone(),
            path: f,
            size: size.map(|s| s as u64),
            record_count: None,
            compression: Compression::None,
        })
        .collect())
}

fn detect_format(files: &[FileEntry], manifest: bool) -> Result<FileFormat> {
    let mut found: Option<(FileFormat, &str)> = None;
    for f in files {
        let member = f.name.rsplit('!').next().unwrap_or(&f.name);
        let x = match sniff::format_from_name(member) {
            Some(x) => x,
            None if manifest => FileFormat::Parquet,
            None => {
                return Err(AtlatlError::config(format!(
                    "can't infer the format of {}; set from.files.format",
                    f.name
                )));
            }
        };
        match found {
            Some((y, other)) if y != x => {
                return Err(AtlatlError::config(format!(
                    "matched files mix formats ({other} is {y:?}, {} is {x:?}); set from.files.format or narrow the selection",
                    f.name
                )));
            }
            Some(_) => {}
            None => found = Some((x, &f.name)),
        }
    }
    Ok(found.map(|(x, _)| x).unwrap_or(if manifest {
        FileFormat::Parquet
    } else {
        FileFormat::Csv
    }))
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Csv {
    header: bool,
    delim: Option<String>,
    quote: Option<String>,
    null_values: Vec<String>,
    encoding: Option<&'static str>,
}

fn csv_options(o: Option<&CsvOptions>) -> Result<Csv> {
    let Some(o) = o else {
        return Ok(Csv {
            header: true,
            ..Default::default()
        });
    };
    let encoding = match o
        .encoding
        .as_deref()
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        None | Some("utf-8" | "utf8") => None,
        Some("utf-16" | "utf16") => Some("utf-16"),
        Some("latin-1" | "latin1" | "iso-8859-1") => Some("latin-1"),
        Some(e) => {
            return Err(AtlatlError::config(format!(
                "csv.encoding `{e}` is not supported (utf-8, utf-16, latin-1)"
            )));
        }
    };
    Ok(Csv {
        header: o.header.unwrap_or(true),
        delim: o.delimiter.clone(),
        quote: o.quote.clone(),
        null_values: o.null_values.clone().unwrap_or_default(),
        encoding,
    })
}

/// The files a source resolved to, and how to read them.
#[derive(Clone, Debug)]
pub struct FileSet {
    /// Sorted by name (manifests keep manifest order).
    pub files: Vec<FileEntry>,
    pub format: FileFormat,
    /// `on_no_match: warn` notices.
    pub warnings: Vec<String>,
    compression: Compression,
    csv: Csv,
    union_by_name: bool,
    hive: bool,
    hive_keys: Vec<String>,
    source: Vec<Column>,
    aliases: Vec<String>,
    /// (source index, DuckDB type) for declared `columns.<c>.type`.
    declared: Vec<(usize, String)>,
    set_null: bool,
}

/// One `chunk: {by: files}` group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chunk {
    pub files: Vec<FileEntry>,
    pub bytes: u64,
    pub expected_rows: Option<u64>,
    /// `SELECT … FROM read_…([files])`, to stage or merge from directly.
    pub select: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Probe,
    /// CSV with `header=false`: the first row is the raw header.
    Header,
    Read,
}

impl FileSet {
    pub fn names(&self) -> Vec<String> {
        self.files.iter().map(|f| f.name.clone()).collect()
    }

    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }

    /// Source column names as the reader produces them, and the staged names.
    pub fn columns(&self) -> Vec<(String, String)> {
        self.source
            .iter()
            .zip(&self.aliases)
            .map(|(s, a)| (s.name.clone(), a.clone()))
            .collect()
    }

    /// Sum of the manifest `record_count`s, if every entry has one.
    pub fn expected_rows(&self) -> Option<u64> {
        expected(&self.files)
    }

    pub fn select(&self) -> Result<String> {
        self.select_for(&self.files)
    }

    /// The staged names of declared columns whose values that don't fit read as NULL, each
    /// flagged in a [`CAST_NULLED`] column (`on_cast_error.set_null`).
    pub fn nulled(&self) -> Vec<String> {
        if !self.set_null {
            return Vec::new();
        }
        (0..self.source.len())
            .filter(|&i| self.declared(i).is_some_and(|(from, t)| from != t))
            .map(|i| self.aliases[i].clone())
            .collect()
    }

    /// A declared column's (read type, declared type).
    fn declared(&self, i: usize) -> Option<(&str, &str)> {
        let (_, t) = self.declared.iter().find(|(j, _)| *j == i)?;
        let col = &self.source[i];
        let from = if self.format == FileFormat::Parquet && !self.hive_keys.contains(&col.name) {
            col.kind.as_str()
        } else {
            "VARCHAR"
        };
        Some((from, t.as_str()))
    }

    /// Stages every file into `CREATE TEMP TABLE <name>`; returns the row count.
    pub fn stage(&self, c: &Connection, name: &str) -> Result<u64> {
        let n = staging::stage_query(c, name, &self.select()?).map_err(reclassify)?;
        if let Some(want) = self.expected_rows()
            && want != n
        {
            return Err(AtlatlError::new(
                ErrorClass::Decode,
                format!("staged {n} rows but the manifest lists {want}"),
            ));
        }
        Ok(n)
    }

    /// `chunk: {by: files, size}`: consecutive groups of about `size` bytes.
    pub fn chunks(&self, cfg: &ChunkConfig) -> Result<Vec<Chunk>> {
        if cfg.by != ChunkBy::Files {
            return Err(AtlatlError::config("files sources chunk only `by: files`"));
        }
        let size = cfg
            .size
            .ok_or_else(|| AtlatlError::config("chunk.size is required for `by: files`"))?
            .0;
        let sizes: Vec<u64> = self.files.iter().map(|f| f.size.unwrap_or(0)).collect();
        manifest::group_by_size(&sizes, size)
            .into_iter()
            .map(|r| {
                let files = self.files[r.clone()].to_vec();
                Ok(Chunk {
                    bytes: sizes[r].iter().sum(),
                    expected_rows: expected(&files),
                    select: self.select_for(&files)?,
                    files,
                })
            })
            .collect()
    }

    fn select_for(&self, files: &[FileEntry]) -> Result<String> {
        if files.is_empty() {
            return Err(AtlatlError::config("no files to read"));
        }
        let mut flags = Vec::new();
        let projection: Vec<String> = self
            .source
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let src = ident(&col.name);
                let alias = &self.aliases[i];
                let expr = match self.declared(i) {
                    Some((from, t)) if self.set_null => {
                        let (e, flag) = drift::nulling_cast(&src, from, t);
                        if let Some(f) = flag {
                            flags.push(format!(
                                "{f} AS {}",
                                ident(&format!("{CAST_NULLED}{alias}"))
                            ));
                        }
                        e
                    }
                    Some((from, t)) => drift::exact_cast(&src, from, t, alias),
                    None => src.clone(),
                };
                if expr == src && *alias == col.name {
                    src
                } else {
                    format!("{expr} AS {}", ident(alias))
                }
            })
            .collect::<Vec<_>>()
            .into_iter()
            .chain(flags)
            .collect();
        Ok(format!(
            "SELECT {} FROM {}",
            projection.join(", "),
            self.reader(files, Phase::Read)
        ))
    }

    fn reader(&self, files: &[FileEntry], phase: Phase) -> String {
        let list = files
            .iter()
            .map(|f| literal(&f.path))
            .collect::<Vec<_>>()
            .join(", ");
        let mut opts: Vec<String> = Vec::new();
        let text = self.format != FileFormat::Parquet;
        if text {
            let comp = match (self.compression, self.format) {
                (Compression::None, FileFormat::Csv) => "none",
                (Compression::None, _) => "uncompressed",
                (c, _) => c.as_duckdb(),
            };
            opts.push(format!("compression={}", literal(comp)));
        }
        if self.union_by_name {
            opts.push("union_by_name=true".into());
        }
        opts.push(format!("hive_partitioning={}", self.hive));
        if self.hive {
            opts.push("hive_types_autocast=false".into());
        }
        let hive = |c: &Column| self.hive_keys.contains(&c.name);
        let f = match self.format {
            FileFormat::Csv => {
                let c = &self.csv;
                opts.push(format!("header={}", c.header && phase != Phase::Header));
                // The sniffer otherwise may skip leading rows it doesn't like.
                opts.push("skip=0".into());
                opts.push("all_varchar=true".into());
                if let Some(d) = &c.delim {
                    opts.push(format!("delim={}", literal(d)));
                }
                if let Some(q) = &c.quote {
                    opts.push(format!("quote={}", literal(q)));
                }
                if let Some(e) = c.encoding {
                    opts.push(format!("encoding={}", literal(e)));
                }
                if phase == Phase::Read {
                    if c.null_values.is_empty() {
                        let cols: Vec<String> = self
                            .source
                            .iter()
                            .filter(|c| !hive(c))
                            .map(|c| literal(&c.name))
                            .collect();
                        opts.push(format!("force_not_null=[{}]", cols.join(", ")));
                    } else {
                        let n: Vec<String> = c.null_values.iter().map(|v| literal(v)).collect();
                        opts.push(format!("nullstr=[{}]", n.join(", ")));
                    }
                }
                "read_csv"
            }
            FileFormat::Json | FileFormat::Jsonl => {
                let fmt = if self.format == FileFormat::Jsonl {
                    "newline_delimited"
                } else {
                    "auto"
                };
                opts.push(format!("format={}", literal(fmt)));
                if phase == Phase::Read {
                    let cols: Vec<String> = self
                        .source
                        .iter()
                        .filter(|c| !hive(c))
                        .map(|c| format!("{}: 'VARCHAR'", literal(&c.name)))
                        .collect();
                    opts.push(format!("columns={{{}}}", cols.join(", ")));
                }
                "read_json"
            }
            FileFormat::Parquet => "read_parquet",
        };
        format!("{f}([{list}], {})", opts.join(", "))
    }

    fn describe(&self, c: &Connection, files: &[FileEntry]) -> Result<Vec<Column>> {
        let rel = format!("(SELECT * FROM {})", self.reader(files, Phase::Probe));
        staging::describe(c, &rel).map_err(reclassify)
    }

    fn probe(
        &mut self,
        c: &Connection,
        naming: Naming,
        columns: Option<&ColumnsConfig>,
        check_schemas: bool,
    ) -> Result<()> {
        if self.hive {
            self.hive_keys = self
                .files
                .first()
                .map(|f| hive_keys(&f.path))
                .unwrap_or_default();
        }
        self.source = self.describe(c, &self.files)?;
        if check_schemas && !self.union_by_name && self.files.len() > 1 {
            let first = self.describe(c, &self.files[..1])?;
            for f in &self.files[1..] {
                let other = self.describe(c, std::slice::from_ref(f))?;
                let same = if self.format == FileFormat::Parquet {
                    other == first
                } else {
                    other
                        .iter()
                        .map(|c| &c.name)
                        .eq(first.iter().map(|c| &c.name))
                };
                if !same {
                    return Err(AtlatlError::config(format!(
                        "{} and {} have different columns ({} vs {}); set from.files.union_by_name: true to union them by name",
                        self.files[0].name,
                        f.name,
                        show(&first),
                        show(&other)
                    )));
                }
            }
        }
        let names: Vec<&str> = self.source.iter().map(|c| c.name.as_str()).collect();
        self.aliases = match naming {
            Naming::Source => names.iter().map(|s| s.to_string()).collect(),
            Naming::Lower => names.iter().map(|s| s.to_lowercase()).collect(),
            Naming::Snake => names
                .iter()
                .map(|s| atlatl_ingest::naming::snake(s))
                .collect(),
            Naming::CleanCsv => {
                let raw = self.raw_headers(c)?;
                let raw: Vec<&str> = match &raw {
                    Some(r) => r.iter().map(String::as_str).collect(),
                    None => names.clone(),
                };
                atlatl_ingest::naming::clean_csv(&raw)
            }
        };
        let mut seen = BTreeSet::new();
        for (a, s) in self.aliases.iter().zip(&names) {
            if !seen.insert(a.as_str()) {
                return Err(AtlatlError::config(format!(
                    "column `{s}` maps to `{a}`, which another column already uses"
                )));
            }
        }
        self.declared.clear();
        if let Some(cols) = columns {
            for (key, cfg) in &cols.columns {
                let Some(t) = cfg.kind.as_deref() else {
                    continue;
                };
                if key == "*" {
                    if is_text(t) {
                        continue;
                    }
                    return Err(AtlatlError::config(format!(
                        "columns.\"*\".type `{t}`: files sources only support text for `*`; declare columns individually"
                    )));
                }
                let idx = match &cfg.source_name {
                    Some(s) => names.iter().position(|n| n == s),
                    None => self.aliases.iter().position(|a| a == key).or_else(|| {
                        self.aliases
                            .iter()
                            .position(|a| a.eq_ignore_ascii_case(key))
                    }),
                };
                if let Some(i) = idx
                    && !(is_text(t) && self.format != FileFormat::Parquet)
                {
                    self.declared
                        .push((i, drift::canonical(c, t).map_err(reclassify)?));
                }
            }
        }
        Ok(())
    }
}

impl FileSet {
    /// CSV header cells as written; DuckDB renames duplicates (`x_1`), `clean_csv` numbers them (`x1`).
    fn raw_headers(&self, c: &Connection) -> Result<Option<Vec<String>>> {
        if self.format != FileFormat::Csv || !self.csv.header || self.union_by_name {
            return Ok(None);
        }
        let sql = format!(
            "SELECT * FROM {} LIMIT 1",
            self.reader(&self.files[..1], Phase::Header)
        );
        let mut st = c.prepare(&sql).map_err(duck)?;
        let mut rows = st.query([]).map_err(duck)?;
        let Some(r) = rows.next().map_err(duck)? else {
            return Ok(None);
        };
        let n = r.as_ref().column_count();
        let mut cells = Vec::with_capacity(n + self.hive_keys.len());
        for i in 0..n {
            let v: Option<String> = r.get(i).map_err(duck)?;
            cells.push(v.unwrap_or_default());
        }
        let data_cols = self.source.len() - self.hive_keys.len().min(self.source.len());
        let n_cells = n - self.hive_keys.len().min(n);
        if n_cells != data_cols {
            return Ok(None);
        }
        cells.truncate(n_cells);
        cells.extend(self.hive_keys.iter().cloned());
        Ok(Some(cells))
    }
}

fn is_text(t: &str) -> bool {
    matches!(
        t.to_ascii_lowercase().as_str(),
        "text" | "varchar" | "string"
    )
}

fn expected(files: &[FileEntry]) -> Option<u64> {
    files.iter().map(|f| f.record_count).sum()
}

fn show(cols: &[Column]) -> String {
    cols.iter()
        .map(|c| format!("{} {}", c.name, c.kind))
        .collect::<Vec<_>>()
        .join(", ")
}

fn hive_keys(path: &str) -> Vec<String> {
    let mut segs: Vec<&str> = path.split('/').collect();
    segs.pop();
    segs.into_iter()
        .filter_map(|s| s.split_once('=').map(|(k, _)| k.to_owned()))
        .filter(|k| !k.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locations() {
        let root = Path::new("/data");
        assert_eq!(
            Location::parse("x/a.csv", None, Some(root)).expect("loc"),
            Location::Local("/data/x/a.csv".into())
        );
        assert_eq!(
            Location::parse("file:///tmp/d/", Some("*.csv"), Some(root)).expect("loc"),
            Location::Local("/tmp/d/*.csv".into())
        );
        assert_eq!(
            Location::parse("s3://b/p", Some("*.gz"), None).expect("loc"),
            Location::S3("s3://b/p/*.gz".into())
        );
        assert!(matches!(
            Location::parse("https://h/x.zip?sig=1", None, None).expect("loc"),
            Location::Https(_)
        ));
        let e = Location::parse("https://h/x", Some("*"), None).expect_err("glob");
        assert_eq!(e.class, ErrorClass::Config);
        assert_eq!(
            Location::parse("sftp://u@h:2222/in/", Some("*.csv"), Some(root)).expect("loc"),
            Location::Sftp(SftpUrl {
                user: Some("u".into()),
                host: Some("h".into()),
                port: Some(2222),
                path: "/in/*.csv".into()
            })
        );
        let e = Location::parse("ftp://h/x", None, None).expect_err("scheme");
        assert!(e.message.contains("ftp://"));
        assert!(Location::parse("s3:///x", None, None).is_err());
    }

    #[test]
    fn hive() {
        assert_eq!(
            hive_keys("s3://b/t/date=2024-01-01/region=us/f.parquet"),
            ["date", "region"]
        );
        assert!(hive_keys("/a/b=c.csv").is_empty());
    }

    #[test]
    fn csv_encoding() {
        let o = |e: &str| CsvOptions {
            header: None,
            delimiter: None,
            quote: None,
            null_values: None,
            encoding: Some(e.into()),
        };
        assert_eq!(csv_options(Some(&o("UTF-8"))).expect("csv").encoding, None);
        assert_eq!(
            csv_options(Some(&o("latin1"))).expect("csv").encoding,
            Some("latin-1")
        );
        assert!(csv_options(Some(&o("ebcdic"))).is_err());
    }
}
