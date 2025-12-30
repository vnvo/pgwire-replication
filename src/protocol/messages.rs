use bytes::Buf;

use crate::error::{PgWireError, Result};

pub fn parse_error_response(payload: &[u8]) -> String {
    // fields: (code_byte, cstring) ... 0
    let mut b = payload;
    let mut msg = None;
    let mut sqlstate = None;

    while !b.is_empty() {
        let code = b[0];
        b = &b[1..];
        if code == 0 {
            break;
        }
        if let Some(pos) = b.iter().position(|&x| x == 0) {
            let s = String::from_utf8_lossy(&b[..pos]).to_string();
            if code == b'M' {
                msg = Some(s);
            } else if code == b'C' {
                sqlstate = Some(s);
            }
            b = &b[pos + 1..];
        } else {
            break;
        }
    }

    match (msg, sqlstate) {
        (Some(m), Some(c)) => format!("{m} (SQLSTATE {c})"),
        (Some(m), None) => m,
        _ => "unknown server error".to_string(),
    }
}

pub fn parse_auth_request(payload: &[u8]) -> Result<(i32, &[u8])> {
    if payload.len() < 4 {
        return Err(PgWireError::Protocol("auth request too short".into()));
    }
    let mut b = payload;
    let code = b.get_i32();
    Ok((code, b))
}

#[cfg(test)]
mod tests {
    use super::parse_error_response;

    #[test]
    fn parse_error_prefers_message() {
        // 'M' "hello" \0 'C' "12345" \0 \0
        let payload = [
            b'M', b'h', b'e', b'l', b'l', b'o', 0, b'C', b'1', b'2', b'3', b'4', b'5', 0, 0,
        ];
        let s = parse_error_response(&payload);
        assert!(s.contains("hello"));
        assert!(s.contains("SQLSTATE"));
    }
}
