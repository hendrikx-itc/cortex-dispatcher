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
pub enum Command {
    SftpDownload {
        created: DateTime<Utc>,
        size: Option<u64>,
        sftp_source: String, path: String
    },
    HttpDownload {
        created: DateTime<Utc>,
        size: Option<u64>,
        url: String
    }
}


impl fmt::Display for Command {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
            Command::SftpDownload { created, size, sftp_source, path } => {
                match size {
                    Some(s) => write!(f, "SftpDownload({}, {}, {}, {})", created, s, sftp_source, path),
                    None => write!(f, "SftpDownload({}, {}, {})", created, sftp_source, path)
                }
            },
            Command::HttpDownload { created, size, url } => {
                match size {
                    Some(s) => write!(f, "HttpDownload({}, {}, {})", created, s, url),
                    None => write!(f, "HttpDownload({}, {})", created, url),
                }
            }
		}
    }
}
