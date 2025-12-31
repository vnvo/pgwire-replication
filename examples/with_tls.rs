#![cfg(feature = "examples")]

#[path = "common.rs"]
mod common;

use pgwire_replication::{
    ReplicationClient, ReplicationConfig, SslMode, TlsConfig, client::ReplicationEvent,
};
use std::path::PathBuf;

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Connection parameters
    let host = env("PGHOST", "localhost");
    let port: u16 = env("PGPORT", "5432").parse()?;
    let user = env("PGUSER", "postgres");
    let password = env("PGPASSWORD", "postgres");
    let database = env("PGDATABASE", "postgres");
    let slot = env("PGSLOT", "example_slot_tls");
    let publication = env("PGPUBLICATION", "example_pub_tls");

    // TLS parameters
    let ca_pem_path = PathBuf::from(env("PGTLS_CA", "/etc/ssl/certs/ca-certificates.crt"));
    let sni_hostname = env("PGTLS_SNI", &host);

    // Control plane (still plain SQL via tokio-postgres; TLS there is optional and independent)
    let sql = common::connect_control_plane(&host, port, &user, &password, &database).await?;
    let start_lsn = common::ensure_slot_and_get_start_lsn(&sql, &slot, &publication).await?;

    println!("starting TLS replication from start_lsn={start_lsn}");

    let cfg = ReplicationConfig {
        host: host.into(),
        port,
        user: user.into(),
        password: password.into(),
        database: database.into(),

        tls: TlsConfig {
            mode: SslMode::VerifyFull,
            ca_pem_path: Some(ca_pem_path),
            sni_hostname: Some(sni_hostname),
        },

        slot: slot.into(),
        publication: publication.into(),
        start_lsn,
        stop_at_lsn: None,

        status_interval: std::time::Duration::from_secs(1),
        idle_timeout: std::time::Duration::from_secs(30),
        buffer_events: 8192,
    };

    let mut repl = ReplicationClient::connect(cfg).await?;

    loop {
        match repl.recv().await? {
            ReplicationEvent::XLogData { wal_end, data, .. } => {
                println!("XLogData wal_end={wal_end} bytes={}", data.len());
                repl.update_applied_lsn(wal_end);
            }
            ReplicationEvent::KeepAlive {
                wal_end,
                reply_requested,
                ..
            } => {
                println!("KeepAlive wal_end={wal_end} reply_requested={reply_requested}");
            }
            ReplicationEvent::StoppedAt { reached } => {
                println!("StoppedAt reached={reached}");
                break;
            }
        }
    }

    Ok(())
}
