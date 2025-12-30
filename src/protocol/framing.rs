use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{PgWireError, Result};

#[derive(Debug, Clone)]
pub struct BackendMessage {
    pub tag: u8,
    pub payload: Bytes, // payload excludes the 4-byte length field
}

pub async fn read_backend_message<R: AsyncRead + Unpin>(rd: &mut R) -> Result<BackendMessage> {
    let mut hdr = [0u8; 5];
    rd.read_exact(&mut hdr).await?;
    let tag = hdr[0];
    let len = i32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
    if len < 4 {
        return Err(PgWireError::Protocol(format!(
            "invalid backend message length: {len}"
        )));
    }
    let payload_len = len - 4;
    let mut buf = vec![0u8; payload_len];
    rd.read_exact(&mut buf).await?;
    Ok(BackendMessage {
        tag,
        payload: Bytes::from(buf),
    })
}

pub async fn write_ssl_request<W: AsyncWrite + Unpin>(wr: &mut W) -> Result<()> {
    let mut buf = [0u8; 8];
    buf[0..4].copy_from_slice(&(8i32).to_be_bytes());
    buf[4..8].copy_from_slice(&(80877103i32).to_be_bytes());
    wr.write_all(&buf).await?;
    wr.flush().await?;
    Ok(())
}

pub async fn write_startup_message<W: AsyncWrite + Unpin>(
    wr: &mut W,
    protocol_version: i32,
    params: &[(&str, &str)],
) -> Result<()> {
    let mut buf = BytesMut::with_capacity(256);
    buf.put_i32(0); // length placeholder
    buf.put_i32(protocol_version);

    for (k, v) in params {
        buf.extend_from_slice(k.as_bytes());
        buf.put_u8(0);
        buf.extend_from_slice(v.as_bytes());
        buf.put_u8(0);
    }
    buf.put_u8(0); // terminator

    let len = buf.len() as i32;
    buf[0..4].copy_from_slice(&len.to_be_bytes());

    wr.write_all(&buf).await?;
    wr.flush().await?;
    Ok(())
}

pub async fn write_query<W: AsyncWrite + Unpin>(wr: &mut W, sql: &str) -> Result<()> {
    let mut buf = BytesMut::with_capacity(sql.len() + 64);
    buf.put_u8(b'Q');
    buf.put_i32(0);
    buf.extend_from_slice(sql.as_bytes());
    buf.put_u8(0);

    let len = (buf.len() - 1) as i32;
    buf[1..5].copy_from_slice(&len.to_be_bytes());

    wr.write_all(&buf).await?;
    wr.flush().await?;
    Ok(())
}

pub async fn write_password_message<W: AsyncWrite + Unpin>(
    wr: &mut W,
    payload: &[u8],
) -> Result<()> {
    let mut buf = BytesMut::with_capacity(payload.len() + 16);
    buf.put_u8(b'p');
    buf.put_i32(0);
    buf.extend_from_slice(payload);

    let len = (buf.len() - 1) as i32;
    buf[1..5].copy_from_slice(&len.to_be_bytes());

    wr.write_all(&buf).await?;
    wr.flush().await?;
    Ok(())
}

pub async fn write_copy_data<W: AsyncWrite + Unpin>(wr: &mut W, payload: &[u8]) -> Result<()> {
    let mut buf = BytesMut::with_capacity(payload.len() + 16);
    buf.put_u8(b'd');
    buf.put_i32(0);
    buf.extend_from_slice(payload);

    let len = (buf.len() - 1) as i32;
    buf[1..5].copy_from_slice(&len.to_be_bytes());

    wr.write_all(&buf).await?;
    wr.flush().await?;
    Ok(())
}

pub async fn write_copy_done<W: AsyncWrite + Unpin>(wr: &mut W) -> Result<()> {
    let mut buf = BytesMut::with_capacity(5);
    buf.put_u8(b'c'); // CopyDone
    buf.put_i32(4); // length includes itself; CopyDone has no payload
    wr.write_all(&buf).await?;
    wr.flush().await?;
    Ok(())
}
