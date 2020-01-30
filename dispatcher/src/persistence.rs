use postgres::tls::{MakeTlsConnect, TlsConnect};
use r2d2;
use r2d2_postgres::PostgresConnectionManager;
use tokio_postgres::Socket;

pub trait Persistence {
    fn insert_sftp_download(&self, source: &str, path: &str, size: i64);
    fn delete_sftp_download_file(&self, id: i64);
    fn set_sftp_download_file(&self, id: i64, file_id: i64);
    fn insert_file(&self, source: &str, path: &str, size: i64, hash: Option<String>) -> Result<i64,String>;
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
    fn insert_sftp_download(&self, source: &str, path: &str, size: i64) {
        let mut client = self.conn_pool.get().unwrap();

        let execute_result = client.execute(
            "insert into dispatcher.sftp_download (source, path, size) values ($1, $2, $3)",
            &[&source, &path, &size]
        );

        match execute_result {
            Ok(_) => {}
            Err(e) => {
                error!("Error inserting download record into database: {}", e);
            }
        }
    }

    fn set_sftp_download_file(&self, id: i64, file_id: i64) {
        let mut client = self.conn_pool.get().unwrap();

        let execute_result = client.execute(
            "update dispatcher.sftp_download set file_id = $2 where id = $1",
            &[&id, &file_id]
        );

        match execute_result {
            Ok(_) => {}
            Err(e) => {
                error!("Error updating sftp_download record into database: {}", e);
            }
        }
    }

    fn delete_sftp_download_file(&self, id: i64) {
        let mut client = self.conn_pool.get().unwrap();

        let execute_result = client.execute(
            "delete from dispatcher.sftp_download where id = $1",
            &[&id]
        );

        match execute_result {
            Ok(_) => {}
            Err(e) => {
                error!("Error deleting sftp_download record from database: {}", e);
            }
        }
    }

    fn insert_file(&self, source: &str, path: &str, size: i64, hash: Option<String>) -> Result<i64,String> {
        let mut client = self.conn_pool.get().unwrap();

        let insert_result = client.query_one(
            "insert into dispatcher.file (source, path, size, hash) values ($1, $2, $3, $4) returning id",
            &[&source, &path, &size, &hash]
        );

        match insert_result {
            Ok(row) => {
                Ok(row.get(0))
            }
            Err(e) => {
                Err(format!("Error inserting file record into database: {}", e))
            }
        }
    }

}
