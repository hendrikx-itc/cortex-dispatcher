use regex::Regex;

extern crate regex;
extern crate serde_regex;

#[derive(Debug, Deserialize, Clone)]
pub struct CommandQueue {
    pub address: std::net::SocketAddr,
    pub queue_name: String
}

#[derive(Debug, Deserialize, Clone)]
pub struct SftpSource {
    pub name: String,
    pub address: String,
    pub username: String,
    #[serde(with = "serde_regex")]
    pub regex: Regex,
    pub directory: String,
    pub scan_interval: u64
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
    pub command_queue: CommandQueue,
    pub sftp_sources: Vec<SftpSource>,
    pub prometheus: Prometheus,
    pub postgresql: Postgresql
}
