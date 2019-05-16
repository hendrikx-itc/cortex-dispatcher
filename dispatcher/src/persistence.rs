use tokio_postgres::Socket;
use postgres::tls::{MakeTlsConnect, TlsConnect};
use r2d2;
use r2d2_postgres::PostgresConnectionManager;

pub trait Persistence {
	fn store(&self, source: &str, path: &str, size: i64, hash: &str);
}

#[derive(Clone)]
pub struct PostgresPersistence<T> where
    T: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send,
    T::TlsConnect: Send,
    T::Stream: Send,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send, {

	conn_pool: r2d2::Pool<PostgresConnectionManager<T>>
}

impl<T> PostgresPersistence<T>
where
    T: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send,
    T::TlsConnect: Send,
    T::Stream: Send,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send, {
	pub fn new(connection_manager: PostgresConnectionManager<T>) -> PostgresPersistence<T> {
		let pool = r2d2::Pool::new(connection_manager).unwrap();

		PostgresPersistence {
			conn_pool: pool
		}
	}
}

impl<T> Persistence for PostgresPersistence<T> where
    T: MakeTlsConnect<Socket> + Clone + 'static + Sync + Send,
    T::TlsConnect: Send,
    T::Stream: Send,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send, {
	fn store(&self, source: &str, path: &str, size: i64, hash: &str) {
		let mut client = self.conn_pool.get().unwrap();

		let execute_result = client.execute(
			"insert into dispatcher.sftp_download (remote, path, size, hash) values ($1, $2, $3, $4)",
			&[&source, &path, &size, &hash]
		);

		match execute_result {
			Ok(_) => {},
			Err(e) => {
				error!("Error inserting download record into database: {}", e);
			}
		}
	}
}
