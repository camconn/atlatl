//! SFTP transport (06 §5): files are listed (globs expand in Rust over `readdir`) and downloaded
//! into the run temp dir with retry and offset resume, then read like local files.

use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use atlatl_core::config::model::SftpConfig;
use atlatl_core::config::types::SecretValue;
use atlatl_core::error::{AtlatlError, ErrorClass, Result, classify_io};
use atlatl_core::retry::{AttemptRecord, RandomJitter, RetryLevel, RetryPolicy, retry};
use base64::Engine;
use globset::GlobBuilder;
use russh::client;
use russh::keys::{HashAlg, PrivateKey, PrivateKeyWithHashAlg, PublicKey, PublicKeyOrCertificate};
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::StatusCode;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::http::Download;

/// `sftp://[user@]host[:port]/path`; `path` is absolute and may hold a glob. The host may be
/// omitted (`sftp:///path`) when the connection sets `sftp.host`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SftpUrl {
    pub user: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub path: String,
}

impl SftpUrl {
    pub fn parse(url: &str) -> Result<Self> {
        let rest = url
            .strip_prefix("sftp://")
            .ok_or_else(|| AtlatlError::config(format!("`{url}` is not an sftp:// URL")))?;
        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i..]),
            None => (rest, "/"),
        };
        let (user, hostport) = match authority.rsplit_once('@') {
            Some((u, h)) => (Some(u), h),
            None => (None, authority),
        };
        if user.is_some_and(|u| u.contains(':')) {
            return Err(AtlatlError::config(
                "sftp:// URLs can't carry a password; set the connection's sftp.password",
            ));
        }
        let (host, port) = match hostport.rsplit_once(':') {
            Some((h, p)) if !p.contains(']') => {
                let port = p.parse::<u16>().map_err(|_| {
                    AtlatlError::config(format!("sftp://{hostport}: bad port `{p}`"))
                })?;
                (h, Some(port))
            }
            _ => (hostport, None),
        };
        let host = host.trim_start_matches('[').trim_end_matches(']');
        Ok(Self {
            user: user.filter(|u| !u.is_empty()).map(str::to_owned),
            host: (!host.is_empty()).then(|| host.to_owned()),
            port,
            path: path.to_owned(),
        })
    }

    /// For messages and file names: no user.
    pub fn display(&self) -> String {
        let port = self.port.map(|p| format!(":{p}")).unwrap_or_default();
        format!(
            "sftp://{}{port}{}",
            self.host.as_deref().unwrap_or(""),
            self.path
        )
    }
}

#[derive(Clone, Debug)]
enum HostCheck {
    Insecure,
    Fingerprints(Vec<String>),
    KnownHosts(PathBuf),
}

/// A resolved server, credentials and host-key policy.
#[derive(Clone)]
pub struct SftpTarget {
    pub host: String,
    pub port: u16,
    pub user: String,
    password: Option<String>,
    key: Option<Arc<PrivateKey>>,
    check: HostCheck,
    connect_timeout: Duration,
}

impl std::fmt::Debug for SftpTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SftpTarget")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("check", &self.check)
            .finish_non_exhaustive()
    }
}

fn resolved(v: Option<&SecretValue>, what: &str) -> Result<Option<String>> {
    match v {
        None => Ok(None),
        Some(SecretValue::Literal(s)) => {
            atlatl_core::redact::register(s);
            Ok(Some(s.clone()))
        }
        Some(SecretValue::Ref(_)) => Err(AtlatlError::internal(format!(
            "sftp.{what} was not resolved before reading"
        ))),
    }
}

/// Key text as given, or decoded from base64 (keeps newlines intact through env vars).
fn key_text(raw: &str) -> Result<String> {
    let t = raw.trim();
    if t.contains("-----BEGIN") || t.starts_with("PuTTY-User-Key-File-") {
        return Ok(t.to_owned());
    }
    let compact: String = t.chars().filter(|c| !c.is_whitespace()).collect();
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(compact)
        .map_err(|_| {
            AtlatlError::config("sftp.private_key is neither a PEM/OpenSSH key nor base64 of one")
        })?;
    let s = String::from_utf8(bytes)
        .map_err(|_| AtlatlError::config("sftp.private_key: base64 does not decode to text"))?;
    atlatl_core::redact::register(s.trim());
    Ok(s)
}

impl SftpTarget {
    pub fn new(url: &SftpUrl, cfg: Option<&SftpConfig>) -> Result<Self> {
        let d = SftpConfig::default();
        let cfg = cfg.unwrap_or(&d);
        let host = url
            .host
            .clone()
            .or_else(|| cfg.host.clone())
            .ok_or_else(|| {
                AtlatlError::config(format!(
                    "{}: no host (put it in the URL or the connection's sftp.host)",
                    url.display()
                ))
            })?;
        let user = url
            .user
            .clone()
            .or_else(|| cfg.user.clone())
            .ok_or_else(|| {
                AtlatlError::config(format!(
                    "{}: no user (set the connection's sftp.user)",
                    url.display()
                ))
            })?;
        let password = resolved(cfg.password.as_ref(), "password")?;
        let passphrase = resolved(
            cfg.private_key_passphrase.as_ref(),
            "private_key_passphrase",
        )?;
        let key = match resolved(cfg.private_key.as_ref(), "private_key")? {
            Some(raw) => {
                let k = russh::keys::decode_secret_key(&key_text(&raw)?, passphrase.as_deref())
                    .map_err(|e| AtlatlError::config(format!("sftp.private_key: {e}")))?;
                Some(Arc::new(k))
            }
            None => None,
        };
        if password.is_none() && key.is_none() {
            return Err(AtlatlError::config(format!(
                "{}: set sftp.password or sftp.private_key",
                url.display()
            )));
        }
        let check = if cfg.insecure_skip_host_key_check == Some(true) {
            HostCheck::Insecure
        } else if let Some(fps) = cfg.host_key.as_ref().filter(|v| !v.is_empty()) {
            HostCheck::Fingerprints(fps.clone())
        } else if let Some(p) = &cfg.known_hosts {
            HostCheck::KnownHosts(PathBuf::from(p))
        } else {
            let home = std::env::var_os("HOME").ok_or_else(|| {
                AtlatlError::config(
                    "sftp: no host_key or known_hosts set, and no $HOME for ~/.ssh/known_hosts",
                )
            })?;
            HostCheck::KnownHosts(Path::new(&home).join(".ssh").join("known_hosts"))
        };
        Ok(Self {
            host,
            port: url.port.or(cfg.port).unwrap_or(22),
            user,
            password,
            key,
            check,
            connect_timeout: cfg
                .connect_timeout
                .map(|d| d.0)
                .unwrap_or(Duration::from_secs(30)),
        })
    }

    fn at(&self) -> String {
        if self.port == 22 {
            format!("sftp://{}", self.host)
        } else {
            format!("sftp://{}:{}", self.host, self.port)
        }
    }

    fn verify(&self, key: &PublicKey) -> std::result::Result<(), String> {
        let fp = key.fingerprint(HashAlg::Sha256).to_string();
        let alg = key.algorithm();
        match &self.check {
            HostCheck::Insecure => Ok(()),
            HostCheck::Fingerprints(want) => {
                let norm = |s: &str| {
                    s.trim()
                        .trim_start_matches("SHA256:")
                        .trim_end_matches('=')
                        .to_owned()
                };
                if want.iter().any(|w| norm(w) == norm(&fp)) {
                    Ok(())
                } else {
                    Err(format!("server key {alg} {fp} is not in sftp.host_key"))
                }
            }
            HostCheck::KnownHosts(p) => {
                match russh::keys::check_known_hosts_path(&self.host, self.port, key, p) {
                    Ok(true) => Ok(()),
                    Ok(false) => Err(format!(
                        "server key {alg} {fp} is not in {} (add it, or set sftp.host_key)",
                        p.display()
                    )),
                    Err(russh::keys::Error::KeyChanged { line }) => Err(format!(
                        "server key {alg} {fp} does not match {} line {line}",
                        p.display()
                    )),
                    Err(e) => Err(format!("reading {}: {e}", p.display())),
                }
            }
        }
    }
}

struct Handler {
    target: Arc<SftpTarget>,
    rejected: Arc<StdMutex<Option<String>>>,
}

impl client::Handler for Handler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        k: &PublicKeyOrCertificate,
    ) -> std::result::Result<bool, Self::Error> {
        let res = match k {
            PublicKeyOrCertificate::PublicKey { key, .. } => self.target.verify(key),
            PublicKeyOrCertificate::Certificate(_)
                if matches!(self.target.check, HostCheck::Insecure) =>
            {
                Ok(())
            }
            PublicKeyOrCertificate::Certificate(_) => {
                Err("the server presented a host certificate, which isn't supported".into())
            }
        };
        match res {
            Ok(()) => Ok(true),
            Err(m) => {
                if let Ok(mut r) = self.rejected.lock() {
                    *r = Some(m);
                }
                Ok(false)
            }
        }
    }
}

struct Conn {
    handle: client::Handle<Handler>,
    sftp: SftpSession,
}

/// A remote file matched by a URL or glob.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteFile {
    pub path: String,
    pub size: u64,
}

/// One SSH connection, reopened after a failed attempt.
pub struct SftpClient {
    target: Arc<SftpTarget>,
    policy: RetryPolicy,
    conn: Mutex<Option<Arc<Conn>>>,
    retries: StdMutex<Vec<String>>,
}

fn ssh_err(at: &str, e: russh::Error) -> AtlatlError {
    use russh::Error as E;
    let class = match &e {
        E::IO(io) => match classify_io(io) {
            ErrorClass::Internal => ErrorClass::Transient,
            c => c,
        },
        E::ConnectionTimeout | E::KeepaliveTimeout | E::InactivityTimeout => ErrorClass::Timeout,
        E::Keys(_) | E::CouldNotReadKey | E::NoCommonAlgo { .. } | E::UnknownAlgo | E::Version => {
            ErrorClass::Config
        }
        E::NotAuthenticated | E::NoAuthMethod => ErrorClass::Auth,
        _ => ErrorClass::Transient,
    };
    AtlatlError::new(class, format!("{at}: {e}")).source(e)
}

fn sftp_err(at: &str, e: russh_sftp::client::error::Error) -> AtlatlError {
    use russh_sftp::client::error::Error as E;
    let class = match &e {
        E::Status(s) => match s.status_code {
            StatusCode::NoSuchFile => ErrorClass::Config,
            StatusCode::PermissionDenied => ErrorClass::Auth,
            StatusCode::OpUnsupported | StatusCode::BadMessage => ErrorClass::Internal,
            _ => ErrorClass::Transient,
        },
        E::Timeout => ErrorClass::Timeout,
        E::Limited(_) => ErrorClass::Config,
        E::UnexpectedPacket => ErrorClass::Internal,
        E::IO(_) | E::UnexpectedBehavior(_) => ErrorClass::Transient,
    };
    AtlatlError::new(class, format!("{at}: {e}")).source(e)
}

fn not_found(e: &russh_sftp::client::error::Error) -> bool {
    matches!(e, russh_sftp::client::error::Error::Status(s) if s.status_code == StatusCode::NoSuchFile)
}

fn read_err(at: &str, e: std::io::Error) -> AtlatlError {
    let class = match e.kind() {
        std::io::ErrorKind::TimedOut => ErrorClass::Timeout,
        _ => ErrorClass::Transient,
    };
    AtlatlError::new(class, format!("{at}: {e}")).source(e)
}

fn has_glob(s: &str) -> bool {
    s.contains(['*', '?', '[', '{'])
}

/// `/a/b/*/x-*.csv` → (`/a/b`, `*/x-*.csv`).
fn split_glob(path: &str) -> (String, Option<String>) {
    let segs: Vec<&str> = path.split('/').collect();
    let Some(i) = segs.iter().position(|s| has_glob(s)) else {
        return (path.to_owned(), None);
    };
    let base = segs[..i].join("/");
    let base = if base.is_empty() { "/".into() } else { base };
    (base, Some(segs[i..].join("/")))
}

fn join(dir: &str, name: &str) -> String {
    if dir.ends_with('/') {
        format!("{dir}{name}")
    } else {
        format!("{dir}/{name}")
    }
}

/// Where a remote file lands under `dir`: the remote path mirrored, so `k=v` segments survive.
pub fn local_path(dir: &Path, remote: &str) -> PathBuf {
    let mut p = dir.to_owned();
    for seg in remote.split('/').filter(|s| !s.is_empty()) {
        p.push(match seg {
            "." | ".." => "_",
            s => s,
        });
    }
    p
}

impl SftpClient {
    pub fn new(target: SftpTarget, policy: RetryPolicy) -> Self {
        Self {
            target: Arc::new(target),
            policy,
            conn: Mutex::new(None),
            retries: StdMutex::new(Vec::new()),
        }
    }

    pub fn target(&self) -> &SftpTarget {
        &self.target
    }

    async fn session(&self) -> Result<Arc<Conn>> {
        let mut g = self.conn.lock().await;
        if let Some(c) = g.as_ref() {
            return Ok(c.clone());
        }
        let c = Arc::new(self.connect().await?);
        *g = Some(c.clone());
        Ok(c)
    }

    async fn reset(&self) {
        if let Some(c) = self.conn.lock().await.take() {
            let _ = c
                .handle
                .disconnect(russh::Disconnect::ByApplication, "", "en")
                .await;
        }
    }

    /// Disconnects; later calls reconnect.
    pub async fn close(&self) {
        self.reset().await;
    }

    async fn connect(&self) -> Result<Conn> {
        let at = self.target.at();
        let t = self.target.connect_timeout;
        match tokio::time::timeout(t, self.connect_inner(&at)).await {
            Ok(r) => r,
            Err(_) => Err(AtlatlError::new(
                ErrorClass::Timeout,
                format!("{at}: connect timed out after {t:?}"),
            )),
        }
    }

    async fn connect_inner(&self, at: &str) -> Result<Conn> {
        let tg = &self.target;
        let cfg = client::Config {
            keepalive_interval: Some(Duration::from_secs(15)),
            keepalive_max: 3,
            nodelay: true,
            ..Default::default()
        };
        let rejected = Arc::new(StdMutex::new(None));
        let h = Handler {
            target: tg.clone(),
            rejected: rejected.clone(),
        };
        let mut handle = match client::connect(Arc::new(cfg), (tg.host.as_str(), tg.port), h).await
        {
            Ok(x) => x,
            Err(russh::Error::UnknownKey) => {
                let why = rejected
                    .lock()
                    .ok()
                    .and_then(|r| r.clone())
                    .unwrap_or_else(|| "unknown server key".into());
                return Err(AtlatlError::new(
                    ErrorClass::Auth,
                    format!("{at}: host key rejected: {why}"),
                )
                .code("host_key"));
            }
            Err(e) => return Err(ssh_err(at, e)),
        };
        let mut ok = false;
        let mut tried = Vec::new();
        if let Some(k) = &tg.key {
            let hash = if k.algorithm().is_rsa() {
                handle
                    .best_supported_rsa_hash()
                    .await
                    .map_err(|e| ssh_err(at, e))?
                    .flatten()
            } else {
                None
            };
            ok = handle
                .authenticate_publickey(&tg.user, PrivateKeyWithHashAlg::new(k.clone(), hash))
                .await
                .map_err(|e| ssh_err(at, e))?
                .success();
            tried.push("private key");
        }
        if !ok && let Some(pw) = &tg.password {
            ok = handle
                .authenticate_password(&tg.user, pw)
                .await
                .map_err(|e| ssh_err(at, e))?
                .success();
            tried.push("password");
        }
        if !ok {
            return Err(AtlatlError::new(
                ErrorClass::Auth,
                format!(
                    "{at}: authentication failed for user `{}` ({})",
                    tg.user,
                    tried.join(", ")
                ),
            ));
        }
        let ch = handle
            .channel_open_session()
            .await
            .map_err(|e| ssh_err(at, e))?;
        ch.request_subsystem(true, "sftp")
            .await
            .map_err(|e| ssh_err(at, e))?;
        let sftp = SftpSession::new(ch.into_stream())
            .await
            .map_err(|e| sftp_err(at, e))?;
        sftp.set_timeout(30);
        Ok(Conn { handle, sftp })
    }

    /// Runs `op` under the retry policy on a live connection, reconnecting after each failure.
    async fn retried<T, F, Fut>(&self, what: &str, op: F) -> Result<(T, u32)>
    where
        F: Fn(Arc<Conn>) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let attempts = StdMutex::new(0u32);
        let (op, counter) = (&op, &attempts);
        let mut jitter = RandomJitter::new();
        let res = retry(
            &self.policy,
            RetryLevel::Request,
            &mut jitter,
            |a: &AttemptRecord| {
                if let Ok(mut r) = self.retries.lock() {
                    r.push(format!(
                        "{what}: attempt {}: {} ({})",
                        a.attempt, a.message, a.class
                    ));
                }
            },
            move |_| async move {
                if let Ok(mut n) = counter.lock() {
                    *n += 1;
                }
                let r = match self.session().await {
                    Ok(c) => op(c).await,
                    Err(e) => Err(e),
                };
                if r.is_err() {
                    self.reset().await;
                }
                r
            },
        )
        .await;
        let n = attempts.into_inner().unwrap_or(0);
        match res {
            Ok(v) => Ok((v, n)),
            Err(e) => {
                let mut err = e.error.clone();
                err.message = format!("{e}");
                Err(err)
            }
        }
    }

    fn take_retries(&self) -> Vec<String> {
        self.retries
            .lock()
            .map(|mut r| std::mem::take(&mut *r))
            .unwrap_or_default()
    }

    /// Files matching `path` (a file or a glob), sorted by path. A missing file or glob base
    /// directory is an empty list.
    pub async fn list(&self, path: &str) -> Result<Vec<RemoteFile>> {
        let at = format!("{}{path}", self.target.at());
        let (base, pat) = split_glob(path);
        let (files, _) = match pat {
            None => {
                self.retried("list", |c| {
                    let at = at.clone();
                    let base = base.clone();
                    async move {
                        match c.sftp.metadata(base.as_str()).await {
                            Ok(m) if m.file_type().is_dir() => Err(AtlatlError::config(format!(
                                "{at} is a directory; add a glob (from.files.glob)"
                            ))),
                            Ok(m) => Ok(vec![RemoteFile {
                                path: base,
                                size: m.len(),
                            }]),
                            Err(e) if not_found(&e) => Ok(Vec::new()),
                            Err(e) => Err(sftp_err(&at, e)),
                        }
                    }
                })
                .await?
            }
            Some(pat) => {
                let m = GlobBuilder::new(&pat)
                    .literal_separator(true)
                    .build()
                    .map_err(|e| AtlatlError::config(format!("{at}: bad glob: {e}")))?
                    .compile_matcher();
                let depth = if pat.contains("**") {
                    64
                } else {
                    pat.split('/').count()
                };
                self.retried("list", |c| {
                    walk(c, at.clone(), base.clone(), m.clone(), depth)
                })
                .await?
            }
        };
        self.take_retries();
        Ok(files)
    }

    /// Downloads `f` to `dest` through `<dest>.part`, resuming from the part's length after a
    /// failure. A file whose size changed between attempts restarts from zero.
    pub async fn download(&self, f: &RemoteFile, dest: &Path) -> Result<Download> {
        if let Some(p) = dest.parent() {
            tokio::fs::create_dir_all(p)
                .await
                .map_err(|e| crate::error::io_at(p, e))?;
        }
        let part = dest.with_extension(match dest.extension() {
            Some(e) => format!("{}.part", e.to_string_lossy()),
            None => "part".into(),
        });
        let _ = tokio::fs::remove_file(&part).await;
        let at = format!("{}{}", self.target.at(), f.path);
        let seen_size: StdMutex<Option<u64>> = StdMutex::new(None);
        let resumed = StdMutex::new(0u32);
        let res = self
            .retried(&f.path, |c| {
                let (at, part, remote) = (at.clone(), part.clone(), f.path.clone());
                let (seen_size, resumed) = (&seen_size, &resumed);
                async move {
                    let mut rf = c
                        .sftp
                        .open(remote.as_str())
                        .await
                        .map_err(|e| sftp_err(&at, e))?;
                    let size = rf.metadata().await.map_err(|e| sftp_err(&at, e))?.len();
                    let mut have = tokio::fs::metadata(&part)
                        .await
                        .map(|m| m.len())
                        .unwrap_or(0);
                    {
                        let mut s = seen_size
                            .lock()
                            .map_err(|_| AtlatlError::internal("download state poisoned"))?;
                        if s.is_some_and(|s| s != size) || have > size {
                            have = 0;
                        }
                        *s = Some(size);
                    }
                    if have > 0 {
                        rf.seek(SeekFrom::Start(have))
                            .await
                            .map_err(|e| read_err(&at, e))?;
                        if let Ok(mut r) = resumed.lock() {
                            *r += 1;
                        }
                    }
                    let mut out = tokio::fs::OpenOptions::new()
                        .create(true)
                        .write(true)
                        .append(have > 0)
                        .truncate(have == 0)
                        .open(&part)
                        .await
                        .map_err(|e| crate::error::io_at(&part, e))?;
                    let mut n = have;
                    let mut buf = vec![0u8; 256 * 1024];
                    let body = async {
                        loop {
                            let k = rf.read(&mut buf).await.map_err(|e| read_err(&at, e))?;
                            if k == 0 {
                                break;
                            }
                            out.write_all(&buf[..k])
                                .await
                                .map_err(|e| crate::error::io_at(&part, e))?;
                            n += k as u64;
                        }
                        Ok::<_, AtlatlError>(())
                    }
                    .await;
                    out.flush()
                        .await
                        .map_err(|e| crate::error::io_at(&part, e))?;
                    out.sync_all()
                        .await
                        .map_err(|e| crate::error::io_at(&part, e))?;
                    body?;
                    if n != size {
                        return Err(AtlatlError::transient(format!(
                            "{at}: read ended at {n} of {size} bytes"
                        )));
                    }
                    Ok(n)
                }
            })
            .await;
        let retries = self.take_retries();
        match res {
            Ok((bytes, attempts)) => {
                tokio::fs::rename(&part, dest)
                    .await
                    .map_err(|e| crate::error::io_at(dest, e))?;
                Ok(Download {
                    path: dest.to_owned(),
                    bytes,
                    attempts,
                    resumed: resumed.into_inner().unwrap_or(0),
                    retries,
                })
            }
            Err(e) => {
                let _ = tokio::fs::remove_file(&part).await;
                Err(e)
            }
        }
    }
}

async fn walk(
    c: Arc<Conn>,
    at: String,
    base: String,
    m: globset::GlobMatcher,
    depth: usize,
) -> Result<Vec<RemoteFile>> {
    let mut out = Vec::new();
    let mut stack = vec![(base, String::new(), 1usize)];
    while let Some((dir, rel, d)) = stack.pop() {
        let entries = match c.sftp.read_dir(dir.as_str()).await {
            Ok(e) => e,
            Err(e) if not_found(&e) && rel.is_empty() => return Ok(Vec::new()),
            Err(e) => return Err(sftp_err(&format!("{at} (listing {dir})"), e)),
        };
        for e in entries {
            let name = e.file_name();
            if name == "." || name == ".." {
                continue;
            }
            let full = join(&dir, &name);
            let r = if rel.is_empty() {
                name
            } else {
                format!("{rel}/{name}")
            };
            let mut md = e.metadata();
            if md.file_type().is_symlink() {
                match c.sftp.metadata(full.as_str()).await {
                    Ok(x) => md = x,
                    Err(e) if not_found(&e) => continue,
                    Err(e) => return Err(sftp_err(&at, e)),
                }
            }
            if md.file_type().is_dir() {
                if d < depth {
                    stack.push((full, r, d + 1));
                }
            } else if m.is_match(&r) {
                out.push(RemoteFile {
                    path: full,
                    size: md.len(),
                });
            }
        }
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls() {
        let u = SftpUrl::parse("sftp://me@h.example:2222/a/b/*.csv").expect("url");
        assert_eq!(
            u,
            SftpUrl {
                user: Some("me".into()),
                host: Some("h.example".into()),
                port: Some(2222),
                path: "/a/b/*.csv".into()
            }
        );
        assert_eq!(u.display(), "sftp://h.example:2222/a/b/*.csv");
        let u = SftpUrl::parse("sftp:///x.csv").expect("url");
        assert_eq!((u.host, u.path.as_str()), (None, "/x.csv"));
        let u = SftpUrl::parse("sftp://[::1]:22/x").expect("url");
        assert_eq!((u.host.as_deref(), u.port), (Some("::1"), Some(22)));
        assert!(SftpUrl::parse("sftp://u:pw@h/x").is_err());
        assert!(SftpUrl::parse("sftp://h:port/x").is_err());
    }

    #[test]
    fn globs() {
        assert_eq!(split_glob("/a/b.csv"), ("/a/b.csv".into(), None));
        assert_eq!(
            split_glob("/a/b/*/x-*.csv"),
            ("/a/b".into(), Some("*/x-*.csv".into()))
        );
        assert_eq!(split_glob("/*.csv"), ("/".into(), Some("*.csv".into())));
        assert_eq!(
            local_path(Path::new("/w"), "/d/k=v/../f.csv"),
            Path::new("/w/d/k=v/_/f.csv")
        );
    }

    #[test]
    fn targets() {
        let cfg: SftpConfig = serde_json::from_value(serde_json::json!({
            "host": "conf", "user": "u", "password": "pw", "host_key": ["SHA256:abc"]
        }))
        .expect("cfg");
        let t = SftpTarget::new(&SftpUrl::parse("sftp:///x").expect("url"), Some(&cfg))
            .expect("target");
        assert_eq!(
            (t.host.as_str(), t.port, t.user.as_str()),
            ("conf", 22, "u")
        );
        let t = SftpTarget::new(
            &SftpUrl::parse("sftp://o@other:23/x").expect("url"),
            Some(&cfg),
        )
        .expect("target");
        assert_eq!(
            (t.host.as_str(), t.port, t.user.as_str()),
            ("other", 23, "o")
        );
        let e = SftpTarget::new(&SftpUrl::parse("sftp://h/x").expect("url"), None)
            .expect_err("no user");
        assert_eq!(e.class, ErrorClass::Config);
        let no_auth: SftpConfig =
            serde_json::from_value(serde_json::json!({"user": "u"})).expect("cfg");
        let e = SftpTarget::new(&SftpUrl::parse("sftp://h/x").expect("url"), Some(&no_auth))
            .expect_err("no auth");
        assert!(e.message.contains("sftp.password"), "{e}");
        let bad_key: SftpConfig = serde_json::from_value(serde_json::json!({
            "user": "u", "private_key": "not a key!"
        }))
        .expect("cfg");
        let e = SftpTarget::new(&SftpUrl::parse("sftp://h/x").expect("url"), Some(&bad_key))
            .expect_err("bad key");
        assert_eq!(e.class, ErrorClass::Config);
    }
}
