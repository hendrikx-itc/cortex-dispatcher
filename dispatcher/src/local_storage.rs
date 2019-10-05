use std::fs::hard_link;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct LocalStorage {
    directory: PathBuf,
}

impl LocalStorage {
    pub fn new<P: AsRef<Path>>(directory: P) -> LocalStorage {
        LocalStorage {
            directory: directory.as_ref().to_path_buf(),
        }
    }

    pub fn local_path<P: AsRef<Path>>(
        &self,
        source_name: &str,
        file_path: P,
        prefix: P,
    ) -> Result<PathBuf, String> {
        let strip_result = file_path.as_ref().strip_prefix(prefix);

        let relative_file_path = match strip_result {
            Ok(path) => path,
            Err(e) => return Err(format!("Error stripping file path: {}", e)),
        };

        Ok(self.directory.join(source_name).join(relative_file_path))
    }

    /// Store file in local storage. The file will be hardlinked from the
    /// specified file_path and will be stored in a directory with the name of
    /// the source. The prefix will be stripped from the file path.
    pub fn hard_link<P>(&self, source_name: &str, file_path: P, prefix: P) -> Result<PathBuf, String>
    where
        P: AsRef<Path>,
    {
        debug!("Hard link prefix: {}", prefix.as_ref().to_string_lossy());
        let source_path_str = file_path.as_ref().to_string_lossy();
        let local_path = match self.local_path(source_name, &file_path, &prefix) {
            Ok(path) => path,
            Err(e) => return Err(e)
        };

        let local_path_parent = local_path.parent().unwrap();

        if !local_path_parent.exists() {
            let local_path_parent_str = local_path_parent.to_string_lossy();
            let create_dir_result = std::fs::create_dir_all(local_path_parent);

            match create_dir_result {
                Ok(_) => info!("Created containing directory '{}'", local_path_parent_str),
                Err(e) => {
                    return Err(format!("Error creating containing directory '{}': {}", local_path_parent_str, e))
                }
            }
        }

        let target_path_str = local_path.to_string_lossy();

        let link_result = hard_link(&file_path, &local_path);

        match link_result {
            Ok(()) => Ok(local_path),
            Err(e) => Err(format!(
                "[E?????] Error hardlinking '{}' to '{}': {}",
                &source_path_str, &target_path_str, &e
            )),
        }
    }
}
