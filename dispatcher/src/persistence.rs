use std::fmt;
use std::error;
use std::path::PathBuf;

use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::Socket;
use chrono::prelude::*;

#[derive(Debug)]
pub struct PersistenceError {
    pub source: Option<Box<dyn error::Error + 'static + Send + Sync>>,
    pub message: String
}

impl fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(s) = &self.source {
            write!(f, "{}: {}", self.message, s)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

impl error::Error for PersistenceError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match &self.source {
            Some(s) => Some(s.as_ref()),
            None => None
        }
    }
}

pub struct FileInfo {
    source: String,
    path: PathBuf,
    modified: DateTime<Utc>,
    size: i64,
    hash: Option<String>
}

#[derive(Clone, Debug)]
pub struct Persistence<T>
where
    T: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send + postgres::tls::MakeTlsConnect<postgres::Socket>,
    T::TlsConnect: Send,
    T::Stream: Send + Sync,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    conn_pool: bb8::Pool<PostgresConnectionManager<T>>,
}

impl<T> Persistence<T>
where
    T: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send,
    T::TlsConnect: Send,
    T::Stream: Send + Sync,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub async fn new(connection_manager: bb8_postgres::PostgresConnectionManager<T>) -> Result<Persistence<T>, String> {
        let pool = bb8::Pool::builder()
            .build(connection_manager)
            .await
            .map_err(|e| format!("Error connecting to database: {}", e))?;

        Ok(Persistence { conn_pool: pool })
    }

    pub async fn insert_sftp_download(&self, source: &str, path: &str, size: i64) -> Result<i64,PersistenceError> {
        let client = self.conn_pool.get()
            .await
            .map_err(|e| PersistenceError { source: Some(Box::new(e)), message: "Could not get database connection".into() } )?;

        let execute_result = client.query_one(
            "insert into dispatcher.sftp_download (source, path, size) values ($1, $2, $3) returning id",
            &[&source, &path, &size]
        ).await;

        match execute_result {
            Ok(row) => Ok(row.get(0)),
            Err(e) => {
                Err(PersistenceError {
                    source: Some(Box::new(e)),
                    message: String::from("Error inserting download record into database")
                })
            }
        }
    }

    pub async fn set_sftp_download_file(&self, id: i64, file_id: i64) -> Result<(), PersistenceError> {
        let client = self.conn_pool.get()
            .await
            .map_err(|e| PersistenceError { source: Some(Box::new(e)), message: "Could not get database connection".into() } )?;

        let execute_result = client.execute(
            "update dispatcher.sftp_download set file_id = $2 where id = $1",
            &[&id, &file_id]
        ).await;

        match execute_result {
            Ok(_) => Ok(()),
            Err(e) => {
                Err(PersistenceError{
                    source: Some(Box::new(e)),
                    message: String::from("Error updating sftp_download record into database")
                })
            }
        }
    }

    pub async fn delete_sftp_download_file(&self, id: i64) -> Result<(), PersistenceError> {
        let client = self.conn_pool.get()
            .await
            .map_err(|e| PersistenceError { source: Some(Box::new(e)), message: "Could not get database connection".into() } )?;

        let execute_result = client.execute(
            "delete from dispatcher.sftp_download where id = $1",
            &[&id]
        ).await;

        match execute_result {
            Ok(_) => Ok(()),
            Err(e) => {
                Err(PersistenceError{
                    source: Some(Box::new(e)),
                    message: String::from("Error deleting sftp_download record from database")
                })
            }
        }
    }

    pub async fn insert_file(&self, source: &str, path: &str, modified: &DateTime<Utc>, size: i64, hash: Option<String>) -> Result<i64,PersistenceError> {
        let client = self.conn_pool.get()
            .await
            .map_err(|e| PersistenceError { source: Some(Box::new(e)), message: "Could not get database connection".into() } )?;

        let insert_result = client.query_one(
            "insert into dispatcher.file (source, path, modified, size, hash) values ($1, $2, $3, $4, $5) returning id",
            &[&source, &path, &modified, &size, &hash]
        ).await;

        match insert_result {
            Ok(row) => Ok(row.get(0)),
            Err(e) => Err(PersistenceError{
                source: Some(Box::new(e)),
                message: String::from("Error inserting file record into database")
            })
        }
    }

    pub async fn remove_file(&self, source: &str, path: &str) -> Result<(),PersistenceError> {
        let client = self.conn_pool.get()
            .await
            .map_err(|e| PersistenceError { source: Some(Box::new(e)), message: "Could not get database connection".into() } )?;

        let insert_result = client.execute(
            "delete from dispatcher.file where source = $1 and path = $2",
            &[&source, &path]
        ).await;

        match insert_result {
            Ok(_) => Ok(()),
            Err(e) => Err(PersistenceError{
                source: Some(Box::new(e)),
                message: String::from("Error deleting file record from database")
            })
        }
    }

    pub async fn get_file(&self, source: &str, path: &str) -> Result<Option<FileInfo>,PersistenceError> {
        let client = self.conn_pool.get()
            .await
            .map_err(|e| PersistenceError { source: Some(Box::new(e)), message: "Could not get database connection".into() } )?;

        let query_result = client.query(
            "select source, path, modified, size, hash from dispatcher.file where source = $1 and path = $2",
            &[&source, &path]
        ).await;

        match query_result {
            Ok(rows) => {
                if rows.is_empty() {
                    Ok(None)
                } else if rows.len() == 1 {
                    let row = &rows[0];
                    let ps: String = row.get(1);
                    let p = PathBuf::from(ps);
    
                    Ok(Some(
                        FileInfo {
                            source: row.get(0),
                            path: p,
                            modified: row.get(2),
                            size: row.get(3),
                            hash: row.get(4)
                        }
                    ))
                } else {
                    Err(PersistenceError{
                        source: None,
                        message: String::from("More than one file matching criteria")
                    })
                }
            },
            Err(e) => Err(PersistenceError{
                source: Some(Box::new(e)),
                message: String::from("Error reading file record from database")
            })
        }
    }

    pub async fn insert_dispatched(&self, dest: &str, file_id: i64) -> Result<(), PersistenceError> {
        let client = self.conn_pool.get()
            .await
            .map_err(|e| PersistenceError { source: Some(Box::new(e)), message: "Could not get database connection".into() } )?;

        let insert_result = client.execute(
            "insert into dispatcher.dispatched (file_id, target, timestamp) values ($1, $2, now())",
            &[&file_id, &dest]
        ).await;

        match insert_result {
            Ok(_) => Ok(()),
            Err(e) => Err(PersistenceError{
                source: Some(Box::new(e)),
                message: String::from("Error inserting dispatched record into database")
            })
        }
    }
}
