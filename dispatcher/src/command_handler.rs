use crate::sftp_downloader::SftpDownloadDispatcher;

use failure::Error;
use futures::future::{Future, ok};

pub struct CommandHandler {
    pub sftp_download_dispatcher: SftpDownloadDispatcher
}

impl CommandHandler {
    pub fn sftp_download(&mut self, sftp_source: String, size: Option<u64>, path: String) -> Box<Future<Item=bool, Error=Error>> {
        info!("sftp_download: {}", path);
        self.sftp_download_dispatcher.dispatch_download(&sftp_source, size, path.clone())
    }

    pub fn http_download(&mut self, _size: Option<u64>, path: String) -> Box<Future<Item = bool, Error = Error>> {
        info!("http_download {}", path);
        Box::new(ok(true))
    }
}
