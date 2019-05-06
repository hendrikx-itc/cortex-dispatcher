use std::fmt;

#[macro_use]
extern crate serde_derive;

extern crate chrono;
use chrono::prelude::*;

extern crate log;

pub mod sftp_connection;

pub use self::sftp_connection::SftpConnection;


/// The set of commands that can be sent over the command queue
#[derive(Debug, Deserialize, Clone, Serialize)]
pub struct SftpDownload {
    pub created: DateTime<Utc>,
    pub size: Option<u64>,
    pub sftp_source: String,
    pub path: String
}

#[derive(Debug, Deserialize, Clone, Serialize)]
pub struct HttpDownload {
    pub created: DateTime<Utc>,
    pub size: Option<u64>,
    pub url: String
}


impl fmt::Display for SftpDownload {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.size {
            Some(s) => write!(f, "SftpDownload({}, {}, {}, {})", self.created, s, self.sftp_source, self.path),
            None => write!(f, "SftpDownload({}, {}, {})", self.created, self.sftp_source, self.path)
        }
    }
}


impl fmt::Display for HttpDownload {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.size {
            Some(s) => write!(f, "HttpDownload({}, {}, {})", self.created, s, self.url),
            None => write!(f, "HttpDownload({}, {})", self.created, self.url),
        }
    }
}
