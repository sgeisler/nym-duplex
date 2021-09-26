//! A probably not standard compliant SOCKS5 implementation

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct SocksRequest {
    pub fqdn: String,
    pub port: u16,
}

#[derive(Debug)]
pub enum SocksError {
    UnsupportedDestination,
    IoError(tokio::io::Error),
    ProtocolError(&'static str),
    ConnectionDropped,
}

async fn authenticate(socket: &mut TcpStream) -> Result<(), SocksError> {
    if socket.read_u8().await? != 5 {
        return Err(SocksError::ProtocolError("Wrong version"));
    }

    let methods_len = socket.read_u8().await?;
    let mut methods = Vec::with_capacity(methods_len as usize);
    for _ in 0..methods_len {
        methods.push(socket.read_u8().await?);
    }

    if !methods.contains(&0) {
        return Err(SocksError::ProtocolError(
            "NO AUTHENTICATION REQUIRED not supported",
        ));
    }

    // respond with version 5
    socket.write_u8(5).await?;
    // choose no authentication
    socket.write_u8(0).await?;

    Ok(())
}

/// Read the SOCKS request sent in the beginning. It contains instructions where to connect to.
/// This implementation is probably not standard compliant.
pub async fn receive_request(socket: &mut TcpStream) -> Result<SocksRequest, SocksError> {
    authenticate(socket).await?;

    if socket.read_u8().await? != 5 {
        return Err(SocksError::ProtocolError("Wrong version"));
    }

    if socket.read_u8().await? != 1 {
        return Err(SocksError::ProtocolError(
            "Only connect requests are supported",
        ));
    }

    if socket.read_u8().await? != 0 {
        return Err(SocksError::ProtocolError("RSV!=0"));
    }

    if socket.read_u8().await? != 3 {
        return Err(SocksError::ProtocolError("Only fqdns are supported"));
    }

    let len = socket.read_u8().await?;
    let mut addr_bytes = vec![0u8; len as usize];
    socket.read_exact(&mut addr_bytes).await?;
    let addr = String::from_utf8(addr_bytes)
        .map_err(|_| SocksError::ProtocolError("Invalid unicode as fqdn"))?;

    let port =
        tokio_byteorder::AsyncReadBytesExt::read_u16::<tokio_byteorder::BigEndian>(socket).await?;
    // Our response is kinda bs except that it says it was successful (which might actually be the case)
    socket.write_all(&[5, 0, 0, 1, 10, 0, 0, 1]).await?;
    tokio_byteorder::AsyncWriteBytesExt::write_u16::<tokio_byteorder::BigEndian>(socket, port)
        .await?;
    Ok(SocksRequest { fqdn: addr, port })
}

impl From<tokio::io::Error> for SocksError {
    fn from(e: tokio::io::Error) -> Self {
        SocksError::IoError(e)
    }
}
