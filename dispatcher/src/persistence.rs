use std::fmt;
use std::error;
use std::path::PathBuf;

use postgres::tls::{MakeTlsConnect, TlsConnect};
use r2d2;
use r2d2_postgres::PostgresConnectionManager;
use bb8;
use bb8_postgres;
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

pub trait Persistence {
    fn insert_sftp_download(&self, source: &str, path: &str, size: i64) -> Result<i64, PersistenceError>;
    fn delete_sftp_download_file(&self, id: i64) -> Result<(), PersistenceError>;
    fn set_sftp_download_file(&self, id: i64, file_id: i64) -> Result<(), PersistenceError>;
    fn insert_file(&self, source: &str, path: &str, modified: &DateTime<Utc>, size: i64, hash: Option<String>) -> Result<i64,PersistenceError>;
    fn remove_file(&self, source: &str, path: &str) -> Result<(),PersistenceError>;
    fn get_file(&self, source: &str, path: &str) -> Result<Option<FileInfo>,PersistenceError>;
    fn insert_dispatched(&self, dest: &str, file_id: i64) -> Result<(), PersistenceError>;
}

#[derive(Clone)]
pub struct PostgresPersistence<T>
where
    T: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send,
    T::TlsConnect: Send,
    T::Stream: Send,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    conn_pool: r2d2::Pool<PostgresConnectionManager<T>>,
}

impl<T> PostgresPersistence<T>
where
    T: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send,
    T::TlsConnect: Send,
    T::Stream: Send,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub fn new(connection_manager: PostgresConnectionManager<T>) -> PostgresPersistence<T> {
        let pool = r2d2::Pool::new(connection_manager).unwrap();

        PostgresPersistence { conn_pool: pool }
    }
}

impl<T> Persistence for PostgresPersistence<T>
where
    T: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send,
    T::TlsConnect: Send,
    T::Stream: Send,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    fn insert_sftp_download(&self, source: &str, path: &str, size: i64) -> Result<i64,PersistenceError> {
        let mut client = self.conn_pool.get().unwrap();

        let execute_result = client.query_one(
            "insert into dispatcher.sftp_download (source, path, size) values ($1, $2, $3) returning id",
            &[&source, &path, &size]
        );

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

    fn set_sftp_download_file(&self, id: i64, file_id: i64) -> Result<(), PersistenceError> {
        let mut client = self.conn_pool.get().unwrap();

        let execute_result = client.execute(
            "update dispatcher.sftp_download set file_id = $2 where id = $1",
            &[&id, &file_id]
        );

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

    fn delete_sftp_download_file(&self, id: i64) -> Result<(), PersistenceError> {
        let mut client = self.conn_pool.get().unwrap();

        let execute_result = client.execute(
            "delete from dispatcher.sftp_download where id = $1",
            &[&id]
        );

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

    fn insert_file(&self, source: &str, path: &str, modified: &DateTime<Utc>, size: i64, hash: Option<String>) -> Result<i64,PersistenceError> {
        let mut client = self.conn_pool.get().unwrap();

        let insert_result = client.query_one(
            "insert into dispatcher.file (source, path, modified, size, hash) values ($1, $2, $3, $4, $5) returning id",
            &[&source, &path, &modified, &size, &hash]
        );

        match insert_result {
            Ok(row) => Ok(row.get(0)),
            Err(e) => Err(PersistenceError{
                source: Some(Box::new(e)),
                message: String::from("Error inserting file record into database")
            })
        }
    }

    fn remove_file(&self, source: &str, path: &str) -> Result<(),PersistenceError> {
        let mut client = self.conn_pool.get().unwrap();

        let insert_result = client.execute(
            "delete from dispatcher.file where source = $1 and path = $2",
            &[&source, &path]
        );

        match insert_result {
            Ok(_) => Ok(()),
            Err(e) => Err(PersistenceError{
                source: Some(Box::new(e)),
                message: String::from("Error deleting file record from database")
            })
        }
    }

    fn get_file(&self, source: &str, path: &str) -> Result<Option<FileInfo>,PersistenceError> {
        let mut client = self.conn_pool.get().unwrap();

        let query_result = client.query(
            "select source, path, modified, size, hash from dispatcher.file where source = $1 and path = $2",
            &[&source, &path]
        );

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

    fn insert_dispatched(&self, dest: &str, file_id: i64) -> Result<(), PersistenceError> {
        let get_result = self.conn_pool.get_timeout(std::time::Duration::from_millis(10_000));

        let mut client = match get_result {
            Ok(c) => c,
            Err(e) => {
                let message = format!("Error getting PostgreSQL conection from pool: {}", &e);
                error!("{}", &message);

                return Err(PersistenceError{
                    source: Some(Box::new(e)),
                    message: message 
                })
            }
        };

        let insert_result = client.query_one(
            "insert into dispatcher.dispatched (file_id, target, timestamp) values ($1, $2, now())",
            &[&file_id, &dest]
        );

        match insert_result {
            Ok(row) => Ok(()),
            Err(e) => Err(PersistenceError{
                source: Some(Box::new(e)),
                message: String::from("Error inserting dispatched record into database")
            })
        }
    }
}


#[derive(Clone)]
pub struct PostgresAsyncPersistence<T>
where
    T: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send,
    T::TlsConnect: Send,
    T::Stream: Send + Sync,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    conn_pool: bb8::Pool<bb8_postgres::PostgresConnectionManager<T>>,
}

impl<T> PostgresAsyncPersistence<T>
where
    T: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send,
    T::TlsConnect: Send,
    T::Stream: Send + Sync,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
{
    pub async fn new(connection_manager: bb8_postgres::PostgresConnectionManager<T>) -> PostgresAsyncPersistence<T> {
        let pool = bb8::Pool::builder().build(connection_manager).await.unwrap();

        PostgresAsyncPersistence { conn_pool: pool }
    }

    pub async fn insert_dispatched(&self, dest: &str, file_id: i64) -> Result<(), PersistenceError> {
        let get_result = self.conn_pool.get().await;

        let client = match get_result {
            Ok(c) => c,
            Err(e) => {
                let message = format!("Error getting PostgreSQL conection from pool: {}", &e);
                error!("{}", &message);

                return Err(PersistenceError{
                    source: Some(Box::new(e)),
                    message: message 
                })
            }
        };

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