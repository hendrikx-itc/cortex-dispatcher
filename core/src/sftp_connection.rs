use std::net::TcpStream;
use std::fmt;

use ssh2::{Session, Sftp};

use owning_ref::OwningHandle;

use log::{info, debug};

pub struct SftpConnection {
    tcp: TcpStream,
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
    pub fn new(address: &str, username: &str) -> Result<SftpConnection, SftpError> {
        let tcp_connect_result = TcpStream::connect(address);

        let tcp = match tcp_connect_result {
            Ok(v) => v,
            Err(e) => return Err(SftpError::new(e.to_string()))
        };

        let mut session = Box::new(Session::new().unwrap());
        session.set_compress(true);
        let handshake_result = session.handshake(&tcp);

        match handshake_result {
            Ok(()) => debug!("SSH handshake succeeded"),
            Err(e) => return Err(SftpError::new(e.to_string()))
        }

        let auth_result = session
            .userauth_agent(username);

        info!("authorizing using ssh agent");

        match auth_result {
            Ok(()) => debug!("SSH authorization succeeded"),
            Err(e) => return Err(SftpError::new(e.to_string()))
        }

        // OwningHandle is needed to store a value and a reference to that value in the same struct
        let sftp =
            OwningHandle::new_with_fn(session, unsafe { |s| Box::new((*s).sftp().unwrap()) });

        Ok(SftpConnection {tcp, sftp})
    }
}
