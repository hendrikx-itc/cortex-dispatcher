use prometheus::IntCounterVec;

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
    pub static ref SWEEP_ENCOUNTERED_COUNTER: IntCounterVec = register_int_counter_vec!(
        "sweep_encountered_total",
        "Total number of files encountered during sweep",
        &["source"]
    )
    .unwrap();
    pub static ref SWEEP_RE_MATCHING_COUNTER: IntCounterVec = register_int_counter_vec!(
        "sweep_re_matching_total",
        "Total number of files encountered during sweep that matched the regular expression",
        &["source"]
    )
    .unwrap();
    pub static ref SWEEP_NEW_COUNTER: IntCounterVec = register_int_counter_vec!(
        "sweep_new_total",
        "Total number of files encountered during sweep that matched the regular expression and are not yet ingested",
        &["source"]
    )
    .unwrap();
}
