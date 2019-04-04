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

#[derive(Debug, Deserialize, Clone)]
pub struct SftpScanner {
    pub name: String,
    pub sftp_source: String,
    pub directory: String,
    #[serde(with = "serde_regex")]
    pub regex: Regex,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SftpDownloader {
    pub name: String,
    pub sftp_source: String,
    pub local_directory: String,
    pub thread_count: usize,
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
