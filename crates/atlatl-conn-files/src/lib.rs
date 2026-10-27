//! Files connector (06 §5): local, `file://`, `s3://`, `https://` and `sftp://` sources read engine-native
//! into DuckDB staging, with archive member selection, magic-byte compression detection, Redshift
//! UNLOAD manifests and Hive-style paths.

pub mod archive;
pub mod error;
pub mod http;
pub mod manifest;
pub mod s3;
pub mod sftp;
pub mod sniff;
mod source;
pub mod template;

pub use http::{Download, Downloader};
pub use sftp::{SftpClient, SftpTarget, SftpUrl};
pub use sniff::Compression;
pub use source::{Chunk, FileEntry, FileSet, FilesRead, FilesSource, Location};
pub use template::render_selector;
