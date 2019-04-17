use regex::Regex;

extern crate regex;
extern crate serde_regex;

#[derive(Debug, Deserialize, Clone)]
pub struct CommandQueue {
    pub address: String,
    pub queue_name: String
}

#[derive(Debug, Deserialize, Clone)]
pub struct SftpSource {
    pub name: String,
    pub address: String,
    pub username: String,
    #[serde(with = "serde_regex")]
    pub regex: Regex,
    pub directory: String
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub command_queue: CommandQueue,
    pub sftp_sources: Vec<SftpSource>
}
