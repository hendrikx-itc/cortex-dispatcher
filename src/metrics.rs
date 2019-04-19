use prometheus::{IntCounter};

lazy_static! {
    pub static ref FILE_DOWNLOAD_COUNTER: IntCounter = register_int_counter!(
        "file_download_total",
        "Total number of files downloaded"
    )
    .unwrap();

    pub static ref BYTES_DOWNLOADED_COUNTER: IntCounter = register_int_counter!(
        "bytes_download_total",
        "Total number of files downloaded"
    )
    .unwrap();
}
