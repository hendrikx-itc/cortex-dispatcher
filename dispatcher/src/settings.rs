use std::path::PathBuf;
use std::net::SocketAddr;

use regex::Regex;
use crate::event::FileEvent;

extern crate regex;
extern crate serde_regex;


trait EventFilter {
    fn event_matches(&self, file_event: &FileEvent) -> bool;
}

#[derive(Debug, Deserialize, Clone)]
pub struct RegexFilter {
    #[serde(with = "serde_regex")]
    regex: Regex
}

impl EventFilter for RegexFilter {
    fn event_matches(&self, file_event: &FileEvent) -> bool {
        let file_name_result = file_event.path.file_name();

        file_name_result.map_or_else(|| false, |file_name| {
            self.regex.is_match(file_name.to_str().unwrap())
        })
    }
}

#[derive(Debug, Deserialize, Clone)]
pub enum Filter {
    Regex(RegexFilter),
    All
}

impl Filter {
    pub fn event_matches(&self, file_event: &FileEvent) -> bool {
        match self {
            Filter::Regex(r) => r.event_matches(file_event),
            Filter::All => true
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct Connection {
    pub source: String,
    pub target: String,
    pub filter: Filter
}

#[derive(Debug, Deserialize, Clone)]
pub struct DirectorySource {
    pub name: String,
    pub directory: PathBuf,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DirectoryTarget {
    pub name: String,
    pub directory: PathBuf,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SftpSource {
    pub name: String,
    pub address: String,
    pub username: String,
    #[serde(default = "default_thread_count")]
    pub thread_count: usize,
    #[serde(default = "default_compress")]
    pub compress: bool
}

/// Default Sftp downloader thread count
fn default_thread_count() -> usize {
    1
}

/// Default Sftp compression setting
fn default_compress() -> bool {
    false
}

#[derive(Debug, Deserialize, Clone)]
pub struct Storage {
    pub directory: PathBuf,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CommandQueue {
    pub address: SocketAddr
}

#[derive(Debug, Deserialize, Clone)]
pub struct PrometheusPush {
    pub gateway: String,
    pub interval: u64
}

#[derive(Debug, Deserialize, Clone)]
pub struct Postgresql {
    pub url: String
}

#[derive(Debug, Deserialize, Clone)]
pub struct HttpServer {
    pub address: std::net::SocketAddr
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub storage: Storage,
    pub command_queue: CommandQueue,
    pub directory_sources: Vec<DirectorySource>,
    pub directory_targets: Vec<DirectoryTarget>,
    pub sftp_sources: Vec<SftpSource>,
    pub connections: Vec<Connection>,
    pub prometheus_push: Option<PrometheusPush>,
    pub postgresql: Postgresql,
    pub http_server: HttpServer
}
