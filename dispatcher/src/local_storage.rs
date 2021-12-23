use std::error;
use std::fmt;
use std::fs::hard_link;
use std::path::{Path, PathBuf};
use std::convert::TryFrom;
use std::time::{SystemTime, UNIX_EPOCH};

use postgres::tls::{MakeTlsConnect, TlsConnect};
use tokio_postgres::Socket;

use chrono::{Utc, DateTime, NaiveDateTime};

use crate::persistence::{Persistence, PersistenceError};

#[derive(Debug, Clone)]
pub struct LocalStorage<T> 
where
    T: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send + postgres::tls::MakeTlsConnect<postgres::Socket>,
    T::TlsConnect: Send,
    T::Stream: Send + Sync,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    directory: PathBuf,
    persistence: Persistence<T>
}

#[derive(Debug, Clone)]
pub struct LocalStorageError {
    message: String
}

impl fmt::Display for LocalStorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.message)
    }
}

impl error::Error for LocalStorageError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}

impl From<PersistenceError> for LocalStorageError {
    fn from(e: PersistenceError) -> Self {
        LocalStorageError { message: format!("{}", e) }
    }
}

impl From<std::io::Error> for LocalStorageError {
    fn from(e: std::io::Error) -> Self {
        LocalStorageError { message: format!("{}", e) }
    }
}

impl<T> LocalStorage<T>
where
    T: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send + postgres::tls::MakeTlsConnect<postgres::Socket>,
    T::TlsConnect: Send,
    T::Stream: Send + Sync,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub fn new<P: AsRef<Path>>(directory: P, persistence: Persistence<T>) -> LocalStorage<T> {
        LocalStorage {
            directory: directory.as_ref().to_path_buf(),
            persistence: persistence
        }
    }

    pub fn local_path<P: AsRef<Path>>(
        &self,
        source_name: &str,
        file_path: P,
        prefix: P,
    ) -> Result<PathBuf, LocalStorageError> {
        if file_path.as_ref().starts_with(&prefix) {
            let strip_result = file_path.as_ref().strip_prefix(&prefix);

            let relative_file_path = match strip_result {
                Ok(path) => path,
                Err(e) => return Err(LocalStorageError { message: format!("Error stripping file path: {}", e) }),
            };
    
            Ok(self.directory.join(source_name).join(relative_file_path))
        } else {
            Ok(self.directory.join(source_name).join(file_path))
        }
    }

    pub async fn in_storage<P>(&self, source_name: &str, file_path: P, prefix: P) -> Result<bool, LocalStorageError>
    where
        P: AsRef<Path>,
    {
        let local_path = match self.local_path(source_name, &file_path, &prefix) {
            Ok(path) => path,
            Err(e) => return Err(e)
        };

        let local_path_str = local_path.to_string_lossy();

        let file_result = &self.persistence.get_file(source_name, &local_path_str).await?;

        match file_result {
            None => Ok(false),
            Some(_f) => Ok(true)
        }
    }

    /// Store file in local storage. The file will be hardlinked from the
    /// specified file_path and will be stored in a directory with the name of
    /// the source. The prefix will be stripped from the file path.
    pub async fn hard_link<P>(&self, source_name: &str, file_path: P, prefix: P) -> Result<(i64, PathBuf), LocalStorageError>
    where
        P: AsRef<Path>,
    {
        debug!("Hard link prefix: {}", prefix.as_ref().to_string_lossy());
        let source_path_str = file_path.as_ref().to_string_lossy();
        let local_path = match self.local_path(source_name, &file_path, &prefix) {
            Ok(path) => path,
            Err(e) => return Err(e)
        };
        let local_path_str = local_path.to_string_lossy();

        if let Some(local_path_parent) = local_path.parent() {
            if !local_path_parent.exists() {
                let local_path_parent_str = local_path_parent.to_string_lossy();
                let create_dir_result = std::fs::create_dir_all(local_path_parent);
    
                match create_dir_result {
                    Ok(_) => info!("Created containing directory '{}'", local_path_parent_str),
                    Err(e) => {
                        return Err(LocalStorageError { message: format!("Error creating containing directory '{}': {}", local_path_parent_str, e) })
                    }
                }
            } else {
                if local_path.is_file() {
                    self.persistence.remove_file(source_name, &local_path_str).await?;
    
                    // Remove existing file before creating new hardlink
                    std::fs::remove_file(&local_path)?;
                }
            }        
        };

        let link_result = hard_link(&file_path, &local_path);

        match link_result {
            Ok(()) => {
                let metadata = std::fs::metadata(&local_path)?;
                let modified = system_time_to_date_time(metadata.modified()?);
                let size = match i64::try_from(metadata.len()) {
                    Ok(s) => s,
                    Err(e) => return Err(LocalStorageError{ message: format!("Error converting file size to i64: {}", e) })
                };

                let file_id = self.persistence.insert_file(source_name, &local_path_str, &modified, size, None).await?;

                debug!("Stored '{}' to '{}'", &source_path_str, &local_path_str);

                Ok((file_id, local_path))
            }
            Err(e) => Err(LocalStorageError{ message: format!(
                "[E?????] Error hardlinking '{}' to '{}': {}",
                &source_path_str, &local_path_str, &e
            )}),
        }
    }
}

fn system_time_to_date_time(t: SystemTime) -> DateTime<Utc> {
    let (sec, nsec) = match t.duration_since(UNIX_EPOCH) {
        Ok(dur) => (dur.as_secs() as i64, dur.subsec_nanos()),
        Err(e) => {
            let dur = e.duration();
            let (sec, nsec) = (dur.as_secs() as i64, dur.subsec_nanos());
            if nsec == 0 {
                (-sec, 0)
            } else {
                (-sec - 1, 1_000_000_000 - nsec)
            }
        },
    };

	DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(sec, nsec), Utc)
}

