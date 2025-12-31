#![cfg(feature = "tls-rustls")]

use std::{fs::File, io::BufReader, sync::Arc};

use rustls::{ClientConfig, RootCertStore};
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, client::TlsStream};

use crate::config::{SslMode, TlsConfig};
use crate::error::{PgWireError, Result};
use crate::protocol::framing::write_ssl_request;

pub enum MaybeTlsStream {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

pub async fn maybe_upgrade_to_tls(
    mut tcp: TcpStream,
    tls: &TlsConfig,
    host: &str,
) -> Result<MaybeTlsStream> {
    match tls.mode {
        SslMode::Disable => return Ok(MaybeTlsStream::Plain(tcp)),
        SslMode::Prefer | SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull => {}
    }

    // pgwire TLS negotiation
    write_ssl_request(&mut tcp).await?;
    let mut resp = [0u8; 1];
    use tokio::io::AsyncReadExt;
    tcp.read_exact(&mut resp).await?;

    if resp[0] != b'S' {
        // Server refused TLS
        return match tls.mode {
            SslMode::Prefer => Ok(MaybeTlsStream::Plain(tcp)),
            _ => Err(PgWireError::Tls(
                "server does not support TLS (SSLRequest rejected)".into(),
            )),
        };
    }

    // Verification semantics:
    // - VerifyFull: verify chain + hostname
    // - VerifyCa: verify chain, ignore hostname mismatch
    // - Prefer/Require: do not verify (dangerous)
    let verify_chain = matches!(tls.mode, SslMode::VerifyCa | SslMode::VerifyFull);
    let verify_hostname = matches!(tls.mode, SslMode::VerifyFull);

    let cfg = build_rustls_config(tls, verify_chain, verify_hostname, host)?;
    let connector = TlsConnector::from(Arc::new(cfg));

    let sni = tls.sni_hostname.as_deref().unwrap_or(host);
    let server_name = rustls::pki_types::ServerName::try_from(sni.to_string())
        .map_err(|_| PgWireError::Tls(format!("invalid SNI hostname '{sni}'")))?;

    let tls_stream = connector
        .connect(server_name, tcp)
        .await
        .map_err(|e| PgWireError::Tls(format!("tls handshake failed: {e}")))?;

    Ok(MaybeTlsStream::Tls(tls_stream))
}

fn build_rustls_config(
    tls: &TlsConfig,
    verify_chain: bool,
    verify_hostname: bool,
    host: &str,
) -> Result<ClientConfig> {
    use rustls::pki_types::CertificateDer;

    // ---- mTLS config validation ----
    let has_cert = tls.client_cert_pem_path.is_some();
    let has_key = tls.client_key_pem_path.is_some();
    if has_cert ^ has_key {
        return Err(PgWireError::Tls(format!(
            "TLS config error: mTLS requires both client_cert_pem_path and client_key_pem_path (got cert={has_cert} key={has_key})"
        )));
    }

    // Operator hint: VerifyFull + IP literal host is a common failure mode.
    if verify_hostname && host.parse::<std::net::IpAddr>().is_ok() && tls.sni_hostname.is_none() {
        return Err(PgWireError::Tls(format!(
            "TLS config error: VerifyFull enabled but host '{host}' is an IP address. \
             Hint: use a DNS name matching the certificate, or set tls.sni_hostname to that DNS name, or use VerifyCa."
        )));
    }

    // ---- Root store (CA) ----
    let mut roots = RootCertStore::empty();

    if let Some(path) = &tls.ca_pem_path {
        let f = File::open(path).map_err(|e| {
            PgWireError::Tls(format!(
                "TLS config error: failed to open CA PEM {}: {e}",
                path.display()
            ))
        })?;
        let mut rd = BufReader::new(f);

        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut rd)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| {
                PgWireError::Tls(format!(
                    "TLS config error: failed to parse CA PEM {}: {e}",
                    path.display()
                ))
            })?
            .into_iter()
            .map(|c| CertificateDer::from(c.into_owned()))
            .collect();

        let (added, _ignored) = roots.add_parsable_certificates(certs);
        if added == 0 {
            return Err(PgWireError::Tls(format!(
                "TLS config error: no valid CA certificates found in {}",
                path.display()
            )));
        }
    } else {
        // Mozilla roots (webpki-roots)
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    // rustls verifier builder needs Arc<RootCertStore>
    let roots_arc = Arc::new(roots.clone());

    // ---- Base config builder ----
    let builder = ClientConfig::builder().with_root_certificates(roots);

    // ---- Client auth (mTLS) ----
    let mut cfg: ClientConfig = if has_cert {
        let cert_path = tls.client_cert_pem_path.as_ref().unwrap();
        let key_path = tls.client_key_pem_path.as_ref().unwrap();

        let cert_chain = load_cert_chain(cert_path)?;
        let key = load_private_key(key_path)?;

        builder
            .with_client_auth_cert(cert_chain, key)
            .map_err(|e| {
                PgWireError::Tls(format!("TLS config error: invalid client cert/key: {e}"))
            })?
    } else {
        builder.with_no_client_auth()
    };

    // ---- Verification policy ----
    if !verify_chain {
        // Prefer/Require: no verification
        let effective_host = tls.sni_hostname.as_deref().unwrap_or(host).to_string();
        cfg.dangerous()
            .set_certificate_verifier(Arc::new(NoVerifyAll {
                verify_hostname,
                host: effective_host,
            }));
        return Ok(cfg);
    }

    if verify_chain && !verify_hostname {
        // VerifyCa: verify chain but ignore hostname mismatch
        let inner = rustls::client::WebPkiServerVerifier::builder(roots_arc)
            .build()
            .map_err(|e| PgWireError::Tls(format!("TLS config error: build verifier: {e}")))?;

        cfg.dangerous()
            .set_certificate_verifier(Arc::new(VerifyChainNoHostname { inner }));
    }

    // VerifyFull: rustls default verifier already verifies chain + hostname.
    Ok(cfg)
}

fn load_cert_chain(
    path: &std::path::Path,
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    use rustls::pki_types::CertificateDer;

    let f = File::open(path).map_err(|e| {
        PgWireError::Tls(format!(
            "TLS config error: failed to open client certificate PEM {}: {e}",
            path.display()
        ))
    })?;
    let mut rd = BufReader::new(f);

    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut rd)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| {
            PgWireError::Tls(format!(
                "TLS config error: failed to parse client certificate PEM {}: {e}",
                path.display()
            ))
        })?
        .into_iter()
        .map(|c| CertificateDer::from(c.into_owned()))
        .collect();

    if certs.is_empty() {
        return Err(PgWireError::Tls(format!(
            "TLS config error: no certificates found in client certificate PEM {}",
            path.display()
        )));
    }

    Ok(certs)
}

fn load_private_key(path: &std::path::Path) -> Result<rustls::pki_types::PrivateKeyDer<'static>> {
    use rustls::pki_types::PrivateKeyDer;

    // Try PKCS#8 first
    let f = File::open(path).map_err(|e| {
        PgWireError::Tls(format!(
            "TLS config error: failed to open private key PEM {}: {e}",
            path.display()
        ))
    })?;
    let mut rd = BufReader::new(f);

    let mut keys: Vec<PrivateKeyDer<'static>> = rustls_pemfile::pkcs8_private_keys(&mut rd)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| {
            PgWireError::Tls(format!(
                "TLS config error: failed to parse PKCS#8 private key PEM {}: {e}",
                path.display()
            ))
        })?
        .into_iter()
        .map(PrivateKeyDer::from)
        .collect();

    if keys.is_empty() {
        // Re-open and try RSA (PKCS#1)
        let f = File::open(path).map_err(|e| {
            PgWireError::Tls(format!(
                "TLS config error: failed to re-open private key PEM {}: {e}",
                path.display()
            ))
        })?;
        let mut rd = BufReader::new(f);

        keys = rustls_pemfile::rsa_private_keys(&mut rd)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| {
                PgWireError::Tls(format!(
                    "TLS config error: failed to parse RSA private key PEM {}: {e}",
                    path.display()
                ))
            })?
            .into_iter()
            .map(PrivateKeyDer::from)
            .collect();
    }

    if keys.is_empty() {
        return Err(PgWireError::Tls(format!(
            "TLS config error: no private keys found in {} (expected PKCS#8 or RSA). \
             Hint: provide a PEM-encoded PKCS#8 private key for best compatibility.",
            path.display()
        )));
    }
    if keys.len() > 1 {
        return Err(PgWireError::Tls(format!(
            "TLS config error: multiple private keys found in {}. Please provide exactly one.",
            path.display()
        )));
    }

    Ok(keys.remove(0))
}

// ---------------- Verifiers ----------------

#[derive(Debug)]
struct NoVerifyAll {
    verify_hostname: bool,
    host: String,
}

impl rustls::client::danger::ServerCertVerifier for NoVerifyAll {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        if self.verify_hostname {
            match server_name {
                rustls::pki_types::ServerName::DnsName(dns) => {
                    if dns.as_ref() != self.host.as_str() {
                        return Err(rustls::Error::InvalidCertificate(
                            rustls::CertificateError::NotValidForName,
                        ));
                    }
                }
                _ => {
                    return Err(rustls::Error::InvalidCertificate(
                        rustls::CertificateError::NotValidForName,
                    ));
                }
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

#[derive(Debug)]
struct VerifyChainNoHostname {
    inner: Arc<dyn rustls::client::danger::ServerCertVerifier>,
}

impl rustls::client::danger::ServerCertVerifier for VerifyChainNoHostname {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        server_name: &rustls::pki_types::ServerName<'_>,
        ocsp: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        match self
            .inner
            .verify_server_cert(end_entity, intermediates, server_name, ocsp, now)
        {
            Ok(ok) => Ok(ok),
            Err(rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidForName)) => {
                // VerifyCa: ignore hostname mismatch, keep chain validation.
                Ok(rustls::client::danger::ServerCertVerified::assertion())
            }
            Err(e) => Err(e),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}
