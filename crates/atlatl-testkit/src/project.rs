//! Temp project directories for config, plan and run tests.

use std::path::Path;

pub struct TempProject {
    pub dir: tempfile::TempDir,
}

impl TempProject {
    pub fn new() -> std::io::Result<Self> {
        Ok(Self {
            dir: tempfile::tempdir()?,
        })
    }

    pub fn path(&self) -> &Path {
        self.dir.path()
    }

    /// Writes `rel` (creating parents) and returns `self` for chaining.
    pub fn file(self, rel: &str, text: &str) -> std::io::Result<Self> {
        let p = self.dir.path().join(rel);
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(p, text)?;
        Ok(self)
    }

    /// `atlatl.yaml` with `version: 1`, the given `connections:` body, and optional extra sections.
    pub fn project(self, connections: &str, extra: &str) -> std::io::Result<Self> {
        self.file(
            "atlatl.yaml",
            &format!("version: 1\n{extra}connections:\n{connections}"),
        )
    }

    pub fn replication(self, name: &str, yaml: &str) -> std::io::Result<Self> {
        self.file(&format!("replications/{name}.yaml"), yaml)
    }
}
