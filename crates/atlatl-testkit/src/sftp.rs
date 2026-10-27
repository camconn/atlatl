//! An SFTP server (atmoz/sftp) with a password user and authorized ed25519 and RSA keys. The
//! user is chrooted to its home, so remote paths look like `/data/...`.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::time::Duration;

use crate::container::Container;

pub const SFTP_IMAGE: &str = "atmoz/sftp:alpine";

#[derive(Debug)]
pub struct Sftp {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    /// OpenSSH-format ed25519 private key authorized for `user`.
    pub ed25519_key: String,
    /// PEM (PKCS#1) RSA private key authorized for `user`.
    pub rsa_key: String,
    /// `SHA256:…` fingerprints of the server's host keys.
    pub host_key_fingerprints: Vec<String>,
    /// known_hosts lines for `[host]:port`.
    pub known_hosts: String,
    container: Container,
}

const USER: &str = "atlatl";
const PASSWORD: &str = "atlatl-sftp-pw";

fn sh(c: &Container, script: &str) -> Result<String, String> {
    c.exec(&["sh", "-c", script])
}

impl Sftp {
    fn start() -> Result<Self, String> {
        let spec = format!("{USER}:{PASSWORD}:1001:100:data");
        let c = Container::start(SFTP_IMAGE, 22, &[], &[], &[&spec])?;
        let addr = format!("{}:{}", c.host, c.port);
        c.wait_for("sftp", Duration::from_secs(60), || banner(&addr))?;
        sh(
            &c,
            &format!(
                "set -e; ssh-keygen -q -t ed25519 -N '' -f /tmp/id_ed25519; \
                 ssh-keygen -q -t rsa -b 2048 -m PEM -N '' -f /tmp/id_rsa; \
                 mkdir -p /home/{USER}/.ssh; cat /tmp/id_ed25519.pub /tmp/id_rsa.pub > /home/{USER}/.ssh/authorized_keys; \
                 chown -R {USER} /home/{USER}/.ssh; chmod 700 /home/{USER}/.ssh; chmod 600 /home/{USER}/.ssh/authorized_keys"
            ),
        )?;
        let ed25519_key = sh(&c, "cat /tmp/id_ed25519")?;
        let rsa_key = sh(&c, "cat /tmp/id_rsa")?;
        let mut host_key_fingerprints = Vec::new();
        let mut known_hosts = String::new();
        for alg in ["ed25519", "rsa"] {
            let pubkey = format!("/etc/ssh/ssh_host_{alg}_key.pub");
            let fp = sh(&c, &format!("ssh-keygen -lf {pubkey}"))?;
            host_key_fingerprints.push(
                fp.split_whitespace()
                    .nth(1)
                    .ok_or_else(|| format!("bad fingerprint line {fp}"))?
                    .to_owned(),
            );
            let line = sh(&c, &format!("cat {pubkey}"))?;
            let mut f = line.split_whitespace();
            let (Some(t), Some(k)) = (f.next(), f.next()) else {
                return Err(format!("bad host key {line}"));
            };
            known_hosts.push_str(&format!("[{}]:{} {t} {k}\n", c.host, c.port));
        }
        Ok(Self {
            host: c.host.clone(),
            port: c.port,
            user: USER.into(),
            password: PASSWORD.into(),
            ed25519_key,
            rsa_key,
            host_key_fingerprints,
            known_hosts,
            container: c,
        })
    }

    /// Writes `body` at `path` inside the chroot (e.g. `/data/x/a.csv`), creating directories.
    pub fn put(&self, path: &str, body: &[u8]) -> Result<(), String> {
        let full = format!("/home/{USER}/{}", path.trim_start_matches('/'));
        let mut child = Command::new("docker")
            .args([
                "exec",
                "-i",
                &self.container.id,
                "sh",
                "-c",
                r#"mkdir -p "$(dirname "$1")" && cat > "$1""#,
                "sh",
                &full,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("docker exec: {e}"))?;
        child
            .stdin
            .take()
            .ok_or("no stdin")?
            .write_all(body)
            .map_err(|e| format!("docker exec: {e}"))?;
        let out = child
            .wait_with_output()
            .map_err(|e| format!("docker exec: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "put {path}: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        Ok(())
    }

    /// A new empty directory under `/data`, as the remote path.
    pub fn fresh_dir(&self) -> Result<String, String> {
        let p = format!("/data/{}", crate::unique("d"));
        sh(&self.container, &format!("mkdir -p /home/{USER}{p}"))?;
        Ok(p)
    }
}

fn banner(addr: &str) -> bool {
    let Ok(mut s) = TcpStream::connect(addr) else {
        return false;
    };
    let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
    let mut b = [0u8; 4];
    s.read_exact(&mut b).is_ok() && &b == b"SSH-"
}

static SFTP: OnceLock<Result<Sftp, String>> = OnceLock::new();

/// A shared SFTP server. Panics with the reason if it can't start.
pub fn sftp() -> &'static Sftp {
    SFTP.get_or_init(Sftp::start)
        .as_ref()
        .unwrap_or_else(|e| panic!("sftp: {e}"))
}
