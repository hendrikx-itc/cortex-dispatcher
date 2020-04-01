use std::net::TcpStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time;

use ssh2::{Session, Sftp};

use owning_ref::OwningHandle;

use log::{info, debug, error};

error_chain! {
    errors {
        DisconnectedError
        ConnectInterrupted
        AuthorizationError
    }
}

pub struct SftpConnection {
    _tcp: TcpStream,
    pub sftp: OwningHandle<Box<Session>, Box<Sftp>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SftpConfig {
    pub address: String,
    pub username: String,
    pub password: Option<String>,
    pub key_file: Option<PathBuf>,
    pub compress: bool
}

impl SftpConnection {
    pub fn connect(config: SftpConfig) -> Result<SftpConnection> {
        let tcp_connect_result = TcpStream::connect(config.address);

        let tcp = match tcp_connect_result {
            Ok(v) => v,
            Err(e) => return Err(Error::with_chain(e, "TCP connect failed"))
        };

        let mut session = Box::new(Session::new().unwrap());
        session.set_compress(config.compress);
        let handshake_result = session.handshake();

        match handshake_result {
            Ok(()) => debug!("SSH handshake succeeded"),
            Err(e) => return Err(Error::with_chain(e, "SSH handshake failed"))
        }

        let auth_result = match config.key_file {
            Some(key_file_path) => {
                info!("Authorizing using key {}", &key_file_path.to_str().unwrap());
                session.userauth_pubkey_file(&config.username, None, key_file_path.as_path(), None)
            },
            None => match config.password {
                Some(pw) => {
                    info!("Authorizing using password");
                    session.userauth_password(&config.username, &pw)
                },
                None => {
                    info!("Authorizing using ssh agent");
                    session.userauth_agent(&config.username)
                }
            }
        };

        match auth_result {
            Ok(()) => debug!("SSH authorization succeeded"),
            Err(e) => return Err(Error::with_chain(e, "SSH authorization failed"))
        }

        // OwningHandle is needed to store a value and a reference to that value in the same struct
        let sftp = OwningHandle::new_with_fn(session, unsafe { |s| Box::new((*s).sftp().unwrap()) });

        Ok(SftpConnection {_tcp: tcp, sftp})
    }

    pub fn connect_loop(config: SftpConfig, stop: Arc<AtomicBool>) -> Result<SftpConnection> {
        while !stop.load(Ordering::Relaxed) {
            let conn_result = SftpConnection::connect(config.clone());

            match conn_result {
                Ok(c) => return Ok(c),
                Err(e) => error!("Could not connect: {}", e),
            }

            thread::sleep(time::Duration::from_millis(1000));
        }

        Err(ErrorKind::ConnectInterrupted.into())
    }
}
