use regex::Regex;

extern crate regex;
extern crate serde_regex;


#[derive(Debug, Deserialize, Clone)]
pub struct DataTarget {
    #[serde(with = "serde_regex")]
    pub regex: Regex,
    pub directory: String
}

#[derive(Debug, Deserialize, Clone)]
pub struct DirectorySource {
    pub name: String,
    pub directory: String,
    pub targets: Vec<DataTarget>
}

#[derive(Debug, Deserialize, Clone)]
pub struct SftpSource {
    pub name: String,
    pub address: String,
    pub username: String
}

/// Default Sftp scan interval in milliseconds
fn default_interval() -> u64 {
    return 60000;
}

#[derive(Debug, Deserialize, Clone)]
pub struct SftpScanner {
    pub name: String,
    pub sftp_source: String,
    pub directory: String,
    #[serde(with = "serde_regex")]
    pub regex: Regex,
    #[serde(default = "default_interval")]
    pub interval: u64
}

#[derive(Debug, Deserialize, Clone)]
pub struct SftpDownloader {
    pub name: String,
    pub sftp_source: String,
    pub local_directory: String,
    #[serde(default = "default_false")]
    pub remove_after_download: bool,
    #[serde(default = "default_thread_count")]
    pub thread_count: usize,
}

/// A generic default value function that returns false
fn default_false() -> bool {
    return false;
}

/// Default Sftp scanner thread count
fn default_thread_count() -> usize {
    return 1;
}

#[derive(Debug, Deserialize, Clone)]
pub struct Storage {
    pub directory: String
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub storage: Storage,
    pub directory_sources: Vec<DirectorySource>,
    pub sftp_sources: Vec<SftpSource>,
    pub sftp_scanners: Vec<SftpScanner>,
    pub sftp_downloaders: Vec<SftpDownloader>
}
