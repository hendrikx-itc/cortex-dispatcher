use std::net::TcpStream;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use ssh2::{Session, Sftp};

use owning_ref::OwningHandle;

use log::{info, debug};

pub struct SftpConnection {
    _tcp: TcpStream,
    pub sftp: OwningHandle<Box<Session>, Box<Sftp<'static>>>,
}

#[derive(Debug)]
pub struct SftpError {
    description: String
}

impl SftpError {
    pub fn new(description: String) -> SftpError {
        SftpError { description }
    }
}

impl fmt::Display for SftpError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SftpError: {}", self.description)
    }
}

impl std::error::Error for SftpError {
}

impl SftpConnection {
    pub fn new(address: &str, username: &str, password: Option<String>, key_file: Option<PathBuf>, compress: bool) -> Result<SftpConnection, SftpError> {
        let tcp_connect_result = TcpStream::connect(address);

        let tcp = match tcp_connect_result {
            Ok(v) => v,
            Err(e) => return Err(SftpError::new(e.to_string()))
        };

        let mut session = Box::new(Session::new().unwrap());
        session.set_compress(compress);
        let handshake_result = session.handshake(&tcp);

        match handshake_result {
            Ok(()) => debug!("SSH handshake succeeded"),
            Err(e) => return Err(SftpError::new(e.to_string()))
        }

        let auth_result = match key_file {
            Some(key_file_path) => {
                debug!("key file: {}", &key_file_path.to_str().unwrap());
                session.userauth_pubkey_file(username, None, key_file_path.as_path(), None)
            },
            None => match password {
                Some(pw) => session.userauth_password(username, &pw),
                None => session.userauth_agent(username)
            }
        };

        info!("authorizing using ssh agent");

        match auth_result {
            Ok(()) => debug!("SSH authorization succeeded"),
            Err(e) => return Err(SftpError::new(e.to_string()))
        }

        // OwningHandle is needed to store a value and a reference to that value in the same struct
        let sftp =
            OwningHandle::new_with_fn(session, unsafe { |s| Box::new((*s).sftp().unwrap()) });

        Ok(SftpConnection {_tcp: tcp, sftp})
    }
}

pub struct SftpConnectionManager {
    _conn: Option<Arc<Box<SftpConnection>>>,
    address: String,
    username: String,
    password: Option<String>,
    key_file: Option<PathBuf>,
    compress: bool
}

impl SftpConnectionManager
{
    pub fn new(address: String, username: String, password: Option<String>, key_file: Option<PathBuf>, compress: bool) -> SftpConnectionManager {
        SftpConnectionManager {
            _conn: None,
            address: address,
            username: username,
            password: password,
            key_file: key_file,
            compress: compress
        }
    }

    pub fn connect(&self) -> Result<SftpConnection, SftpError> {
        SftpConnection::new(
            &self.address.clone(),
            &self.username.clone(),
            self.password.clone(),
            self.key_file.clone(),
            self.compress,
        )
    }

    pub fn get(&mut self) -> Arc<Box<SftpConnection>> {
        match &self._conn {
            None => {
                let conn = Arc::new(Box::new(self.connect().unwrap()));
                self._conn = Some(conn.clone());

                conn.clone()
            },
            Some(conn) => {
                conn.clone()
            }
        }
    }

    pub fn reset(&mut self) {
        self._conn = None;
    }
}
