#[cfg(feature = "scram")]
use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
#[cfg(feature = "scram")]
use hmac::{Hmac, Mac};
#[cfg(feature = "scram")]
use rand::RngCore;
#[cfg(feature = "scram")]
use sha2::{Digest, Sha256};

use crate::error::{PgWireError, Result};

#[cfg(feature = "scram")]
type HmacSha256 = Hmac<Sha256>;

#[cfg(feature = "scram")]
#[derive(Debug, Clone)]
pub struct ScramClient {
    pub client_nonce_b64: String,
    pub client_first_bare: String,
    pub client_first: String,
}

#[cfg(feature = "scram")]
impl ScramClient {
    pub fn new(username: &str) -> ScramClient {
        let mut nonce = [0u8; 18];
        rand::rng().fill_bytes(&mut nonce);
        let nonce_b64 = B64.encode(nonce);

        let user = sasl_escape_username(username);
        let client_first_bare = format!("n={user},r={nonce_b64}");
        let client_first = format!("n,,{client_first_bare}");

        ScramClient {
            client_nonce_b64: nonce_b64,
            client_first_bare,
            client_first,
        }
    }

    pub fn parse_server_first(server_first: &str) -> Result<(String, String, u32)> {
        let mut r = None;
        let mut s = None;
        let mut i = None;
        for part in server_first.split(',') {
            if let Some(v) = part.strip_prefix("r=") {
                r = Some(v.to_string());
            } else if let Some(v) = part.strip_prefix("s=") {
                s = Some(v.to_string());
            } else if let Some(v) = part.strip_prefix("i=") {
                i = v.parse::<u32>().ok();
            }
        }
        Ok((
            r.ok_or_else(|| PgWireError::Auth("SCRAM missing r".into()))?,
            s.ok_or_else(|| PgWireError::Auth("SCRAM missing s".into()))?,
            i.ok_or_else(|| PgWireError::Auth("SCRAM missing i".into()))?,
        ))
    }

    pub fn client_final(
        &self,
        password: &str,
        server_first: &str,
    ) -> Result<(
        String,
        String,  /* auth_message */
        Vec<u8>, /* salted_password */
    )> {
        let (rnonce, salt_b64, iters) = Self::parse_server_first(server_first)?;
        if !rnonce.starts_with(&self.client_nonce_b64) {
            return Err(PgWireError::Auth("SCRAM nonce mismatch".into()));
        }
        let salt = B64
            .decode(salt_b64.as_bytes())
            .map_err(|e| PgWireError::Auth(format!("bad SCRAM salt b64: {e}")))?;

        let channel_binding = "biws"; // base64("n,,")
        let client_final_wo_proof = format!("c={channel_binding},r={rnonce}");
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, server_first, client_final_wo_proof
        );

        let salted_password = hi_sha256(password.as_bytes(), &salt, iters);
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = Sha256::digest(&client_key);

        let client_sig = hmac_sha256(stored_key.as_slice(), auth_message.as_bytes());
        let proof = xor_bytes(&client_key, &client_sig);
        let proof_b64 = B64.encode(proof);

        let client_final = format!("{client_final_wo_proof},p={proof_b64}");
        Ok((client_final, auth_message, salted_password))
    }

    pub fn verify_server_final(
        server_final: &str,
        salted_password: &[u8],
        auth_message: &str,
    ) -> Result<()> {
        let v = server_final
            .split(',')
            .find_map(|p| p.strip_prefix("v="))
            .ok_or_else(|| PgWireError::Auth("SCRAM final missing v".into()))?;
        let server_sig = B64
            .decode(v.trim().as_bytes())
            .map_err(|e| PgWireError::Auth(format!("bad server signature b64: {e}")))?;

        let server_key = hmac_sha256(salted_password, b"Server Key");
        let expected = hmac_sha256(&server_key, auth_message.as_bytes());
        if server_sig != expected {
            return Err(PgWireError::Auth("SCRAM server signature mismatch".into()));
        }
        Ok(())
    }
}

#[cfg(feature = "scram")]
fn sasl_escape_username(u: &str) -> String {
    u.replace('=', "=3D").replace(',', "=2C")
}

#[cfg(feature = "scram")]
fn hi_sha256(password: &[u8], salt: &[u8], iters: u32) -> Vec<u8> {
    // RFC5802 Hi(): U1 = HMAC(p, salt + INT(1)), U2 = HMAC(p, U1) ... XOR
    let mut s1 = Vec::with_capacity(salt.len() + 4);
    s1.extend_from_slice(salt);
    s1.extend_from_slice(&1u32.to_be_bytes());

    let mut u = hmac_sha256(password, &s1);
    let mut out = u.clone();

    for _ in 1..iters {
        u = hmac_sha256(password, &u);
        for (o, ui) in out.iter_mut().zip(u.iter()) {
            *o ^= *ui;
        }
    }
    out
}

#[cfg(feature = "scram")]
fn hmac_sha256(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac key");
    mac.update(msg);
    mac.finalize().into_bytes().to_vec()
}

#[cfg(feature = "scram")]
fn xor_bytes(a: &[u8], b: &[u8]) -> Vec<u8> {
    a.iter().zip(b.iter()).map(|(x, y)| x ^ y).collect()
}

#[cfg(test)]
#[cfg(feature = "scram")]
mod tests {
    use super::ScramClient;

    #[test]
    fn scram_builds_first_message() {
        let c = ScramClient::new("user");
        assert!(c.client_first.starts_with("n,,n=user,r="));
    }

    #[test]
    fn parse_server_first() {
        let (r, s, i) = ScramClient::parse_server_first("r=abc,s=Zm9v,i=4096").unwrap();
        assert_eq!(r, "abc");
        assert_eq!(s, "Zm9v");
        assert_eq!(i, 4096);
    }
}
