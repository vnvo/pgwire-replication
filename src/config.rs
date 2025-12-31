use std::path::PathBuf;
use std::time::Duration;

use crate::lsn::Lsn;

#[derive(Debug, Clone)]
pub enum SslMode {
    Disable,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub mode: SslMode,
    
    /// Path to a PEM file containing one or more CA certificates.
    /// If None and Verify* is used, use webpki-roots (system roots equivalent).
    pub ca_pem_path: Option<PathBuf>,

    /// Override SNI / DNS name used for hostname verification.
    pub sni_hostname: Option<String>,

    /// Optional client certificate chain (PEM). Enables mutual TLS.
    pub client_cert_pem_path: Option<PathBuf>,

    /// Optional client private key (PEM). Enables mutual TLS.
    pub client_key_pem_path: Option<PathBuf>,    
}

#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub host: String,
    pub port: u16,

    pub user: String,
    pub password: String,
    pub database: String,

    pub tls: TlsConfig,

    pub slot: String,
    pub publication: String,

    pub start_lsn: Lsn,

    /// Optional “replay/range” bound: stop once committed end LSN >= stop_at_lsn.
    /// Note: only meaningful if WAL is available covering the range.
    pub stop_at_lsn: Option<Lsn>,

    /// How often to send standby status updates (feedback).
    pub status_interval: Duration,

    /// If no server messages arrive within this interval, treat it as an error.
    pub idle_timeout: Duration,

    /// Bounded buffer size (#events) between replication worker and consumer.
    pub buffer_events: usize,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 5432,
            user: "postgres".into(),
            password: "postgres".into(),
            database: "postgres".into(),
            tls: TlsConfig {
                mode: SslMode::Disable,
                ca_pem_path: None,
                sni_hostname: None,
                client_cert_pem_path: None,
                client_key_pem_path: None,
            },

            slot: "slot".into(),
            publication: "pub".into(),
            start_lsn: Lsn(0),
            stop_at_lsn: None,

            status_interval: Duration::from_secs(1),
            idle_timeout: Duration::from_secs(30),
            buffer_events: 8192,
        }
    }
}
