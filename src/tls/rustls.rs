#[cfg(feature = "tls-rustls")]
use std::{fs::File, io::BufReader, sync::Arc};

#[cfg(feature = "tls-rustls")]
use tokio::net::TcpStream;

#[cfg(feature = "tls-rustls")]
use tokio_rustls::{TlsConnector, client::TlsStream};

#[cfg(feature = "tls-rustls")]
use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};

use crate::config::{SslMode, TlsConfig};
use crate::error::{PgWireError, Result};
use crate::protocol::framing::write_ssl_request;

#[cfg(feature = "tls-rustls")]
pub enum MaybeTlsStream {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

#[cfg(feature = "tls-rustls")]
pub async fn maybe_upgrade_to_tls(
    mut tcp: TcpStream,
    tls: &TlsConfig,
    host: &str,
) -> Result<MaybeTlsStream> {
    match tls.mode {
        SslMode::Disable => return Ok(MaybeTlsStream::Plain(tcp)),
        SslMode::Prefer | SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull => {}
    }

    write_ssl_request(&mut tcp).await?;
    let mut resp = [0u8; 1];
    use tokio::io::AsyncReadExt;
    tcp.read_exact(&mut resp).await?;

    if resp[0] != b'S' {
        // Server refused TLS
        match tls.mode {
            SslMode::Prefer => return Ok(MaybeTlsStream::Plain(tcp)),
            _ => {
                return Err(PgWireError::Tls(
                    "server does not support TLS (SSLRequest rejected)".into(),
                ));
            }
        }
    }

    let verify = matches!(tls.mode, SslMode::VerifyCa | SslMode::VerifyFull);
    let verify_hostname = matches!(tls.mode, SslMode::VerifyFull);

    let cfg = build_rustls_config(tls, verify, verify_hostname, host)?;
    let connector = TlsConnector::from(Arc::new(cfg));

    let sni = tls.sni_hostname.clone().unwrap_or_else(|| host.to_string());
    let server_name = ServerName::try_from(sni)
        .map_err(|e| PgWireError::Tls(format!("invalid SNI hostname: {e}")))?;

    let tls_stream = connector
        .connect(server_name, tcp)
        .await
        .map_err(|e| PgWireError::Tls(format!("tls handshake failed: {e}")))?;

    Ok(MaybeTlsStream::Tls(tls_stream))
}

#[cfg(feature = "tls-rustls")]
fn build_rustls_config(
    tls: &TlsConfig,
    verify: bool,
    verify_hostname: bool,
    host: &str,
) -> Result<ClientConfig> {
    let mut roots = RootCertStore::empty();

    if let Some(path) = &tls.ca_pem_path {
        let f = File::open(path).map_err(|e| PgWireError::Tls(format!("open CA pem: {e}")))?;
        let mut rd = BufReader::new(f);
        let certs = rustls_pemfile::certs(&mut rd)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| PgWireError::Tls(format!("parse CA pem: {e}")))?;
        roots.add_parsable_certificates(certs);
    } else {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    let mut cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    if !verify {
        cfg.dangerous().set_certificate_verifier(Arc::new(NoVerify {
            verify_hostname,
            host: host.to_string(),
        }));
    }

    Ok(cfg)
}

#[cfg(feature = "tls-rustls")]
#[derive(Debug)]
struct NoVerify {
    verify_hostname: bool,
    host: String,
}

#[cfg(feature = "tls-rustls")]
impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        if self.verify_hostname {
            // if someone chooses VerifyFull but also disables cert verify, at least enforce SNI match.
            match server_name {
                rustls::pki_types::ServerName::DnsName(dns) => {
                    if dns.as_ref() != self.host.as_str() {
                        return Err(rustls::Error::General("hostname mismatch".into()));
                    }
                }
                _ => return Err(rustls::Error::General("unsupported server name".into())),
            }
        }
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA256,
        ]
    }
}
