use lazy_static::lazy_static;
use prometheus::{register_int_counter_vec, IntCounterVec};

lazy_static! {
    pub static ref FILE_DOWNLOAD_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "file_download_total",
        "Total number of files downloaded",
        &["source"]
    )
    .unwrap();
    pub static ref BYTES_DOWNLOADED_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "bytes_download_total",
        "Total number of files downloaded",
        &["source"]
    )
    .unwrap();
    pub static ref MESSAGES_RECEIVED_COUNTER: IntCounterVec = register_int_counter_vec!(
        "messages_received_total",
        "Total number of messages received",
        &["source"]
    )
    .unwrap();
}
