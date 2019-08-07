use std::fmt;
use std::thread;

#[macro_use]
extern crate serde_derive;

extern crate chrono;
use chrono::prelude::*;

#[macro_use]
extern crate error_chain;

extern crate log;

use log::{info, error};

pub mod sftp_connection;

pub use self::sftp_connection::SftpConnection;


/// The set of commands that can be sent over the command queue
#[derive(Debug, Deserialize, Clone, Serialize)]
pub struct SftpDownload {
    pub created: DateTime<Utc>,
    pub size: Option<u64>,
    pub sftp_source: String,
    pub path: String,
    pub remove: bool
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


/// Wait for a thread to finish, log error or success, ignoring the success value.
pub fn wait_for<T>(join_handle: thread::JoinHandle<T>, thread_name: &str) {
    let join_result = join_handle.join();

    match join_result {
        Ok(_) => {
            info!("{} thread stopped", thread_name);
        }
        Err(e) => {
            error!("{} thread stopped with error: {:?}", thread_name, e);
        }
    }
}
