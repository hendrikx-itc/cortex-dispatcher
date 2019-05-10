use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct FileEvent {
    pub source_name: String,
    pub path: PathBuf
}
