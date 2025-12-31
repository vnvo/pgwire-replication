use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{PgWireError, Result};

/// Maximum backend message size (1GB) - prevents memory exhaustion from malformed length fields
/// This is more than enough.
pub const MAX_MESSAGE_SIZE: usize = 1024 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendMessage {
    pub tag: u8,
    pub payload: Bytes, // payload excludes the 4-byte length field
}

impl BackendMessage {
    /// Returns true if this is an ErrorResponse ('E')
    #[inline]
    pub fn is_error(&self) -> bool {
        self.tag == b'E'
    }

    /// Returns true if this is a ReadyForQuery ('Z')
    #[inline]
    pub fn is_ready_for_query(&self) -> bool {
        self.tag == b'Z'
    }

    /// Returns true if this is CopyBothResponse ('W')
    #[inline]
    pub fn is_copy_both_response(&self) -> bool {
        self.tag == b'W'
    }

    /// Returns true if this is CopyData ('d')
    #[inline]
    pub fn is_copy_data(&self) -> bool {
        self.tag == b'd'
    }

    /// Returns true if this is AuthenticationRequest ('R')
    #[inline]
    pub fn is_auth_request(&self) -> bool {
        self.tag == b'R'
    }
}

pub async fn read_backend_message<R: AsyncRead + Unpin>(rd: &mut R) -> Result<BackendMessage> {
    let mut hdr = [0u8; 5];
    rd.read_exact(&mut hdr).await?;
    let tag = hdr[0];
    let len = i32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]);

    if len < 4 {
        return Err(PgWireError::Protocol(format!(
            "invalid backend message length: {len}"
        )));
    }

    let payload_len = (len - 4) as usize;

    if payload_len > MAX_MESSAGE_SIZE {
        return Err(PgWireError::Protocol(format!(
            "backend message too large: {payload_len} bytes (max {MAX_MESSAGE_SIZE})"
        )));
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn read_backend_message_parses_valid_message() {
        // Tag 'Z' (ReadyForQuery), length=5 (4 + 1 byte payload), payload='I' (idle)
        let data = [b'Z', 0, 0, 0, 5, b'I'];
        let mut cursor = Cursor::new(&data[..]);

        let msg = read_backend_message(&mut cursor).await.unwrap();
        assert_eq!(msg.tag, b'Z');
        assert_eq!(&msg.payload[..], b"I");
        assert!(msg.is_ready_for_query());
    }

    #[tokio::test]
    async fn read_backend_message_handles_empty_payload() {
        // Tag 'N' (NoticeResponse placeholder), length=4 (no payload)
        let data = [b'N', 0, 0, 0, 4];
        let mut cursor = Cursor::new(&data[..]);

        let msg = read_backend_message(&mut cursor).await.unwrap();
        assert_eq!(msg.tag, b'N');
        assert!(msg.payload.is_empty());
    }

    #[tokio::test]
    async fn read_backend_message_rejects_invalid_length() {
        // length < 4 is invalid
        let data = [b'Z', 0, 0, 0, 3];
        let mut cursor = Cursor::new(&data[..]);

        let err = read_backend_message(&mut cursor).await.unwrap_err();
        assert!(err.to_string().contains("invalid backend message length"));
    }

    #[tokio::test]
    async fn read_backend_message_rejects_oversized_message() {
        // length = MAX_MESSAGE_SIZE + 5 (over limit)
        let huge_len = (MAX_MESSAGE_SIZE as i32) + 5;
        let data = [
            b'Z',
            (huge_len >> 24) as u8,
            (huge_len >> 16) as u8,
            (huge_len >> 8) as u8,
            huge_len as u8,
        ];
        let mut cursor = Cursor::new(&data[..]);

        let err = read_backend_message(&mut cursor).await.unwrap_err();
        assert!(err.to_string().contains("too large"));
    }

    #[tokio::test]
    async fn write_ssl_request_produces_valid_bytes() {
        let mut buf = Vec::new();
        write_ssl_request(&mut buf).await.unwrap();

        assert_eq!(buf.len(), 8);
        // length = 8
        assert_eq!(&buf[0..4], &8i32.to_be_bytes());
        // SSL request code = 80877103
        assert_eq!(&buf[4..8], &80877103i32.to_be_bytes());
    }

    #[tokio::test]
    async fn write_startup_message_includes_params() {
        let mut buf = Vec::new();
        let params = [("user", "postgres"), ("database", "test")];
        write_startup_message(&mut buf, 196608, &params)
            .await
            .unwrap();

        // Should contain the parameter strings
        let s = String::from_utf8_lossy(&buf);
        assert!(s.contains("user"));
        assert!(s.contains("postgres"));
        assert!(s.contains("database"));
        assert!(s.contains("test"));

        // Length field should be at start
        let len = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        assert_eq!(len, buf.len());
    }

    #[tokio::test]
    async fn write_query_produces_valid_message() {
        let mut buf = Vec::new();
        write_query(&mut buf, "SELECT 1").await.unwrap();

        // Should start with 'Q'
        assert_eq!(buf[0], b'Q');

        // Length should be correct (excludes tag byte)
        let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        assert_eq!(len, buf.len() - 1);

        // Should contain the SQL
        assert!(buf[5..].starts_with(b"SELECT 1"));

        // Should be null-terminated
        assert_eq!(buf[buf.len() - 1], 0);
    }

    #[tokio::test]
    async fn write_password_message_produces_valid_message() {
        let mut buf = Vec::new();
        write_password_message(&mut buf, b"secret").await.unwrap();

        assert_eq!(buf[0], b'p');
        let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        assert_eq!(len, buf.len() - 1);
        assert_eq!(&buf[5..], b"secret");
    }

    #[tokio::test]
    async fn write_copy_data_produces_valid_message() {
        let mut buf = Vec::new();
        write_copy_data(&mut buf, b"payload").await.unwrap();

        assert_eq!(buf[0], b'd');
        let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        assert_eq!(len, buf.len() - 1);
        assert_eq!(&buf[5..], b"payload");
    }

    #[tokio::test]
    async fn write_copy_done_produces_valid_message() {
        let mut buf = Vec::new();
        write_copy_done(&mut buf).await.unwrap();

        assert_eq!(buf.len(), 5);
        assert_eq!(buf[0], b'c');
        // Length = 4 (just the length field itself, no payload)
        assert_eq!(&buf[1..5], &4i32.to_be_bytes());
    }

    #[test]
    fn backend_message_helper_methods() {
        let error = BackendMessage {
            tag: b'E',
            payload: Bytes::new(),
        };
        assert!(error.is_error());
        assert!(!error.is_ready_for_query());

        let ready = BackendMessage {
            tag: b'Z',
            payload: Bytes::from_static(b"I"),
        };
        assert!(ready.is_ready_for_query());
        assert!(!ready.is_error());

        let copy_both = BackendMessage {
            tag: b'W',
            payload: Bytes::new(),
        };
        assert!(copy_both.is_copy_both_response());

        let copy_data = BackendMessage {
            tag: b'd',
            payload: Bytes::new(),
        };
        assert!(copy_data.is_copy_data());

        let auth = BackendMessage {
            tag: b'R',
            payload: Bytes::new(),
        };
        assert!(auth.is_auth_request());
    }
}
