use crate::sftp_downloader::SftpDownloadDispatcher;

pub struct CommandHandler {
    pub sftp_download_dispatcher: SftpDownloadDispatcher
}

impl CommandHandler {
    pub fn sftp_download(&mut self, path: String) {
        self.sftp_download_dispatcher.dispatch_download(&String::from("local-test"), path.clone());
        info!("sftp_download: {}", path);
    }

    pub fn http_download(&mut self, path: String) {
        info!("http_download {}", path);
    }
}
