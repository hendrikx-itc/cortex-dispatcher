use prometheus::{IntCounter};

lazy_static! {
    pub static ref DIR_SCAN_COUNTER: IntCounter = register_int_counter!(
        "dir_scan_total",
        "Total number of directory scans"
    )
    .unwrap();
}
