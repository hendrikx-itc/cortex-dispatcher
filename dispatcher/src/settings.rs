use std::path::PathBuf;

use regex::Regex;

use inotify::WatchMask;

extern crate regex;
extern crate serde_regex;

trait FileFilter {
    fn file_matches(&self, path: &PathBuf) -> bool;
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RegexFilter {
    #[serde(with = "serde_regex")]
    pattern: Regex,
}

impl FileFilter for RegexFilter {
    fn file_matches(&self, path: &PathBuf) -> bool {
        let file_name_result = path.file_name();

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
    pub fn file_matches(&self, path: &PathBuf) -> bool {
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
    AllEvents
}

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
            Self::AllEvents => WatchMask::ALL_EVENTS
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DirectorySource {
    pub name: String,
    pub directory: PathBuf,
    pub recursive: bool,
    pub events: Vec<FileSystemEvent>,
    pub filter: Option<Filter>
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
    Hardlink
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DirectoryTarget {
    pub name: String,
    pub directory: PathBuf,
    #[serde(default = "default_local_target_method")]
    pub method: LocalTargetMethod,
    pub notify: Option<Notify>,
    pub permissions: u32
}

fn default_local_target_method() -> LocalTargetMethod {
    LocalTargetMethod::Hardlink
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
}

/// Default Sftp downloader thread count
fn default_thread_count() -> usize {
    1
}

fn default_false() -> bool {
    false
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
pub struct PrometheusPush {
    pub gateway: String,
    pub interval: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Postgresql {
    pub url: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HttpServer {
    pub address: std::net::SocketAddr,
    pub static_content_path: PathBuf,
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
    pub prometheus_push: Option<PrometheusPush>,
    pub postgresql: Postgresql,
    pub http_server: HttpServer,
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
                address: "127.0.0.1:5672".parse().unwrap()
            },
            directory_sources: vec![DirectorySource {
                name: "mixed-directory".to_string(),
                directory: PathBuf::from("/cortex/incoming"),
                events: vec![FileSystemEvent::MovedTo, FileSystemEvent::CloseWrite],
                filter: None,
                recursive: true
            }],
            directory_targets: vec![DirectoryTarget {
                name: "red".to_string(),
                directory: PathBuf::from("/cortex/storage/red-consumer"),
                method: LocalTargetMethod::Hardlink,
                notify: Some(Notify::RabbitMQ(RabbitMQNotify {
                    message_template: "".to_string(),
                    address: "127.0.0.1:5672".parse().unwrap(),
                    exchange: "".to_string(),
                    routing_key: "red-consumer".to_string(),
                })),
                permissions: 100
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
                },
                SftpSource {
                    name: "blue".to_string(),
                    address: "127.0.0.1:22".parse().unwrap(),
                    username: "cortex".to_string(),
                    password: Some("password".to_string()),
                    key_file: None,
                    compress: false,
                    thread_count: 4,
                },
            ],
            connections: vec![],
            prometheus_push: None,
            postgresql: Postgresql {
                url: "postgresql://postgres:password@127.0.0.1:5432/cortex".to_string(),
            },
            http_server: HttpServer {
                address: "0.0.0.0:56008".parse().unwrap(),
                static_content_path: PathBuf::from("static-web"),
            },
        }
    }
}
