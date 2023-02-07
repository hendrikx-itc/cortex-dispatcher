use std::path::{Path, PathBuf};

use regex::Regex;

#[cfg(target_os = "linux")]
use inotify::WatchMask;

use chrono::prelude::{DateTime, Utc};

use crate::base_types;

use serde_derive::{Deserialize, Serialize};

use regex;
use serde_regex;

fn default_false() -> bool {
    false
}

fn default_true() -> bool {
    true
}

trait FileFilter {
    fn file_matches<P: AsRef<Path>>(&self, path: P) -> bool;
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RegexFilter {
    #[serde(with = "serde_regex")]
    pattern: Regex,
}

impl FileFilter for RegexFilter {
    fn file_matches<P: AsRef<Path>>(&self, path: P) -> bool {
        let file_name_result = path.as_ref().file_name();

        file_name_result.map_or_else(
            || false,
            |file_name| self.pattern.is_match(file_name.to_str().unwrap()),
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Filter {
    Regex(RegexFilter),
    All,
}

impl Filter {
    pub fn file_matches<P: AsRef<Path>>(&self, path: P) -> bool {
        match self {
            Filter::Regex(r) => r.file_matches(path),
            Filter::All => true,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Connection {
    pub source: String,
    pub target: String,
    pub filter: Option<Filter>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum FileSystemEvent {
    Access,
    Attrib,
    CloseWrite,
    CloseNoWrite,
    Create,
    Delete,
    DeleteSelf,
    Modify,
    MoveSelf,
    MovedFrom,
    MovedTo,
    Open,
    AllEvents,
}

#[cfg(target_os = "linux")]
impl FileSystemEvent {
    pub fn watch_mask(&self) -> WatchMask {
        match self {
            Self::Access => WatchMask::ACCESS,
            Self::Attrib => WatchMask::ATTRIB,
            Self::CloseWrite => WatchMask::CLOSE_WRITE,
            Self::CloseNoWrite => WatchMask::CLOSE_NOWRITE,
            Self::Create => WatchMask::CREATE,
            Self::Delete => WatchMask::DELETE,
            Self::DeleteSelf => WatchMask::DELETE_SELF,
            Self::Modify => WatchMask::MODIFY,
            Self::MoveSelf => WatchMask::MOVE_SELF,
            Self::MovedFrom => WatchMask::MOVED_FROM,
            Self::MovedTo => WatchMask::MOVED_TO,
            Self::Open => WatchMask::OPEN,
            Self::AllEvents => WatchMask::ALL_EVENTS,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileComparison {
    pub size: bool,
    pub modified: bool,
    pub hash: bool,
}

impl FileComparison {
    pub fn equal(
        &self,
        file_info: &base_types::FileInfo,
        size: u64,
        modified: DateTime<Utc>,
        hash: Option<String>,
    ) -> bool {
        if self.size && (file_info.size as u64) != size {
            return false;
        }

        if self.modified && file_info.modified != modified {
            return false;
        }

        if self.hash && file_info.hash != hash {
            return false;
        }

        true
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Deduplication {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "check")]
    Check(FileComparison),
}

fn default_directory_source_deduplication() -> Deduplication {
    Deduplication::Check(FileComparison {
        size: false,
        modified: false,
        hash: true,
    })
}

/// A local directory where files can be placed that will be picked up for
/// dispatching.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DirectorySource {
    /// The name of this source
    pub name: String,
    /// The directory to monitor for new files.
    pub directory: PathBuf,
    /// Set to true to recursively descend in subdirectories.
    #[serde(default = "default_true")]
    pub recursive: bool,
    /// A list of file system events that can trigger the intake of files in
    /// this source.
    pub events: Vec<FileSystemEvent>,
    /// A filter to ingest only certain files in the source directory.
    pub filter: Option<Filter>,
    /// Set to a file comparison method to prevent the same file from being
    /// dispatched multiple times, or None if no deduplication must be
    /// performed.
    #[serde(default = "default_directory_source_deduplication")]
    pub deduplication: Deduplication,
    /// Set to true to remove the source file after ingestion
    #[serde(default = "default_true")]
    pub delete: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RabbitMQNotify {
    pub message_template: String,
    pub address: String,
    pub exchange: String,
    pub routing_key: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Notify {
    #[serde(rename = "rabbitmq")]
    RabbitMQ(RabbitMQNotify),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum LocalTargetMethod {
    Copy,
    Symlink,
    Hardlink,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DirectoryTarget {
    pub name: String,
    pub directory: PathBuf,
    #[serde(default = "default_local_target_method")]
    pub method: LocalTargetMethod,
    pub overwrite: bool,
    pub notify: Option<Notify>,
    pub permissions: u32,
}

fn default_local_target_method() -> LocalTargetMethod {
    LocalTargetMethod::Hardlink
}

fn default_sftp_source_deduplication() -> Deduplication {
    Deduplication::Check(FileComparison {
        size: true,
        modified: true,
        hash: false,
    })
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SftpSource {
    pub name: String,
    pub address: String,
    pub username: String,
    pub password: Option<String>,
    pub key_file: Option<PathBuf>,
    #[serde(default = "default_thread_count")]
    pub thread_count: usize,
    #[serde(default = "default_false")]
    pub compress: bool,
    #[serde(default = "default_sftp_source_deduplication")]
    pub deduplication: Deduplication,
}

/// Default Sftp downloader thread count
fn default_thread_count() -> usize {
    1
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Storage {
    pub directory: PathBuf,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CommandQueue {
    pub address: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Postgresql {
    pub url: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HttpServer {
    pub address: std::net::SocketAddr,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Settings {
    pub storage: Storage,
    pub command_queue: CommandQueue,
    #[serde(default = "default_directory_sources")]
    pub directory_sources: Vec<DirectorySource>,
    #[serde(default = "default_directory_targets")]
    pub directory_targets: Vec<DirectoryTarget>,
    pub sftp_sources: Vec<SftpSource>,
    pub connections: Vec<Connection>,
    pub postgresql: Postgresql,
    pub http_server: HttpServer,
    #[serde(default = "default_scan_interval")]
    pub scan_interval: u64,
}

/// Default directory scan (sweep) interval
fn default_scan_interval() -> u64 {
    60_000
}

fn default_directory_sources() -> Vec<DirectorySource> {
    vec![]
}

fn default_directory_targets() -> Vec<DirectoryTarget> {
    vec![]
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            storage: Storage {
                directory: PathBuf::from("/cortex/storage"),
            },
            command_queue: CommandQueue {
                address: "127.0.0.1:5672".parse().unwrap(),
            },
            directory_sources: vec![DirectorySource {
                name: "mixed-directory".to_string(),
                directory: PathBuf::from("/cortex/incoming"),
                events: vec![FileSystemEvent::MovedTo, FileSystemEvent::CloseWrite],
                filter: None,
                recursive: true,
                deduplication: Deduplication::Check(FileComparison {
                    size: false,
                    modified: false,
                    hash: true,
                }),
                delete: true,
            }],
            directory_targets: vec![DirectoryTarget {
                name: "red".to_string(),
                directory: PathBuf::from("/cortex/storage/red-consumer"),
                method: LocalTargetMethod::Hardlink,
                overwrite: true,
                notify: Some(Notify::RabbitMQ(RabbitMQNotify {
                    message_template: "".to_string(),
                    address: "127.0.0.1:5672".parse().unwrap(),
                    exchange: "".to_string(),
                    routing_key: "red-consumer".to_string(),
                })),
                permissions: 100,
            }],
            sftp_sources: vec![
                SftpSource {
                    name: "red".to_string(),
                    address: "127.0.0.1:22".parse().unwrap(),
                    username: "cortex".to_string(),
                    password: Some("password".to_string()),
                    key_file: None,
                    compress: false,
                    thread_count: 4,
                    deduplication: Deduplication::Check(FileComparison {
                        size: true,
                        modified: true,
                        hash: false,
                    }),
                },
                SftpSource {
                    name: "blue".to_string(),
                    address: "127.0.0.1:22".parse().unwrap(),
                    username: "cortex".to_string(),
                    password: Some("password".to_string()),
                    key_file: None,
                    compress: false,
                    thread_count: 4,
                    deduplication: Deduplication::Check(FileComparison {
                        size: true,
                        modified: true,
                        hash: false,
                    }),
                },
            ],
            connections: vec![],
            postgresql: Postgresql {
                url: "postgresql://postgres:password@127.0.0.1:5432/cortex".to_string(),
            },
            http_server: HttpServer {
                address: "0.0.0.0:56008".parse().unwrap(),
            },
            scan_interval: 60_000,
        }
    }
}
