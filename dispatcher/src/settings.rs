use regex::Regex;

extern crate regex;
extern crate serde_regex;

#[derive(Debug, Deserialize, Clone)]
pub struct DataTarget {
    #[serde(with = "serde_regex")]
    pub regex: Regex,
    pub directory: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DirectorySource {
    pub name: String,
    pub directory: String,
    pub targets: Vec<DataTarget>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SftpSource {
    pub name: String,
    pub address: String,
    pub username: String,
    #[serde(default = "default_thread_count")]
    pub thread_count: usize,
}

/// Default Sftp scanner thread count
fn default_thread_count() -> usize {
    1
}

#[derive(Debug, Deserialize, Clone)]
pub struct Storage {
    pub directory: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CommandQueue {
    pub address: String,
    pub queue_name: String
}

#[derive(Debug, Deserialize, Clone)]
pub struct Prometheus {
    pub push_gateway: String,
    pub push_interval: u64
}

#[derive(Debug, Deserialize, Clone)]
pub struct Postgresql {
    pub url: String
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub storage: Storage,
    pub command_queue: CommandQueue,
    pub directory_sources: Vec<DirectorySource>,
    pub sftp_sources: Vec<SftpSource>,
    pub prometheus: Prometheus,
    pub postgresql: Postgresql
}
