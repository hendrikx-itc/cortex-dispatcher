use crate::sftp_downloader::SftpDownloadDispatcher;

pub struct CommandHandler {
    pub sftp_download_dispatcher: SftpDownloadDispatcher
}

impl CommandHandler {
    pub fn sftp_download(&mut self, sftp_source: String, size: Option<u64>, path: String) {
        self.sftp_download_dispatcher.dispatch_download(&sftp_source, size, path.clone());
        info!("sftp_download: {}", path);
    }

    pub fn http_download(&mut self, size: Option<u64>, path: String) {
        info!("http_download {}", path);
    }
}
