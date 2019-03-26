use regex::Regex;

extern crate regex;
extern crate serde_regex;


#[derive(Debug, Deserialize)]
pub struct DataTarget {
    #[serde(with = "serde_regex")]
    pub regex: Regex,
    pub directory: String
}

#[derive(Debug, Deserialize)]
pub struct DirectorySource {
    pub name: String,
    pub directory: String,
    pub targets: Vec<DataTarget>
}

#[derive(Debug, Deserialize)]
pub struct SftpSource {
    pub name: String,
    pub host: String,
    pub username: String,
    pub directory: String,
    #[serde(with = "serde_regex")]
    pub regex: Regex,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    #[serde(rename = "directory-sources")]
    pub directory_sources: Vec<DirectorySource>,
    #[serde(rename = "sftp-sources")]
    pub sftp_sources: Vec<SftpSource>
}
