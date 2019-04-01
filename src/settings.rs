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
    #[serde(rename = "sftp-source")]
    pub sftp_source: String,
    pub directory: String,
    #[serde(with = "serde_regex")]
    pub regex: Regex,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SftpDownloader {
    pub name: String,
    #[serde(rename = "sftp-source")]
    pub sftp_source: String,
    #[serde(rename = "local-directory")]
    pub local_directory: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Storage {
    pub directory: String
}

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub storage: Storage,
    #[serde(rename = "directory-sources")]
    pub directory_sources: Vec<DirectorySource>,
    #[serde(rename = "sftp-sources")]
    pub sftp_sources: Vec<SftpSource>,
    #[serde(rename = "sftp-scanners")]
    pub sftp_scanners: Vec<SftpScanner>,
    #[serde(rename = "sftp-downloaders")]
    pub sftp_downloaders: Vec<SftpDownloader>
}
