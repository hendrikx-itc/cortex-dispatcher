use prometheus::{IntCounterVec};

lazy_static! {
    pub static ref DIR_SCAN_COUNTER: IntCounterVec = register_int_counter_vec!(
        "dir_scan_total",
        "Total number of source scans",
        &["source"]
    )
    .unwrap();

    pub static ref DIR_SCAN_DURATION: IntCounterVec = register_int_counter_vec!(
        "dir_scan_duration",
        "Total time spent scanning a source",
        &["source"]
    )
    .unwrap();
}
