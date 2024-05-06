use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time;

use serde_derive::{Deserialize, Serialize};
use ssh2::Session;

use error_chain::error_chain;

use log::{debug, error, info};

error_chain! {
    errors {
        DisconnectedError
        ConnectInterrupted
        AuthorizationError
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SftpConfig {
    pub address: String,
    pub username: String,
    pub password: Option<String>,
    pub key_file: Option<PathBuf>,
    pub compress: bool,
}

impl SftpConfig {
    pub fn connect(&self) -> Result<Session> {
        let tcp = TcpStream::connect(&self.address)
            .map_err(|e| Error::with_chain(e, "TCP connect failed"))?;

        let mut session =
            Session::new().map_err(|e| Error::with_chain(e, "Session setup failed"))?;

        session.set_compress(self.compress);
        session.set_tcp_stream(tcp);
        let handshake_result = session.handshake();

        match handshake_result {
            Ok(()) => debug!("SSH handshake succeeded"),
            Err(e) => {
                let msg = format!("SSH handshake failed: {}", &e);
                return Err(Error::with_chain(e, msg));
            }
        }

        let auth_result = match &self.key_file {
            Some(key_file_path) => {
                info!("Authorizing using key {}", &key_file_path.to_string_lossy());
                session.userauth_pubkey_file(&self.username, None, key_file_path.as_path(), None)
            }
            None => match &self.password {
                Some(pw) => {
                    info!("Authorizing using password");
                    session.userauth_password(&self.username, &pw)
                }
                None => {
                    info!("Authorizing using ssh agent");
                    session.userauth_agent(&self.username)
                }
            },
        };

        auth_result.map_err(|e| Error::with_chain(e, "SSH authorization failed"))?;

        debug!("SSH authorization succeeded");

        Ok(session)
    }

    pub fn connect_loop(&self, stop: Arc<AtomicBool>) -> Result<Session> {
        while !stop.load(Ordering::Relaxed) {
            let conn_result = self.connect();

            match conn_result {
                Ok(c) => return Ok(c),
                Err(e) => error!("Could not connect: {}", e),
            }

            thread::sleep(time::Duration::from_millis(1000));
        }

        Err(ErrorKind::ConnectInterrupted.into())
    }
}
