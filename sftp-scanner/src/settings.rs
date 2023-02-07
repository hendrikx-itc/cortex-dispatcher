use regex::Regex;
use std::path::PathBuf;

use regex;
use serde_regex;
use serde_derive::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CommandQueue {
    pub address: String,
    pub queue_name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SftpSource {
    pub name: String,
    pub address: String,
    pub username: String,
    pub password: Option<String>,
    pub key_file: Option<PathBuf>,
    #[serde(with = "serde_regex")]
    pub regex: Regex,
    pub directory: String,
    #[serde(default = "default_false")]
    pub deduplicate: bool,
    #[serde(default = "default_false")]
    pub remove: bool,
    pub scan_interval: u64,
    #[serde(default = "default_false")]
    pub recurse: bool,
}

fn default_false() -> bool {
    false
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
    pub command_queue: CommandQueue,
    pub sftp_sources: Vec<SftpSource>,
    pub postgresql: Postgresql,
    pub http_server: HttpServer,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            command_queue: CommandQueue {
                address: "127.0.0.1:5672".parse().unwrap(),
                queue_name: "cortex-dispatcher".to_string(),
            },
            sftp_sources: vec![
                SftpSource {
                    name: "red".to_string(),
                    address: "127.0.0.1:22".parse().unwrap(),
                    username: "cortex".to_string(),
                    password: Some("password".to_string()),
                    key_file: None,
                    regex: Regex::new("^.*\\.xml$").unwrap(),
                    directory: "upload/red".to_string(),
                    deduplicate: false,
                    remove: true,
                    scan_interval: 3000,
                    recurse: false,
                },
                SftpSource {
                    name: "blue".to_string(),
                    address: "127.0.0.1:22".parse().unwrap(),
                    username: "cortex".to_string(),
                    password: Some("password".to_string()),
                    key_file: None,
                    regex: Regex::new("^.*\\.xml$").unwrap(),
                    directory: "upload/blue".to_string(),
                    deduplicate: false,
                    remove: true,
                    scan_interval: 2000,
                    recurse: true,
                },
            ],
            postgresql: Postgresql {
                url: "postgresql://postgres:password@127.0.0.1:5432/cortex".to_string(),
            },
            http_server: HttpServer {
                address: "0.0.0.0:56008".parse().unwrap(),
            },
        }
    }
}
