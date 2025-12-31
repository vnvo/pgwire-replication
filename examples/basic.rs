/// START_LSN="0/16B6C50" cargo run --example basic
use pgwire_replication::{
    Lsn, ReplicationClient, ReplicationConfig, SslMode, TlsConfig, client::ReplicationEvent,
};

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let host = env("PGHOST", "127.0.0.1");
    let port: u16 = env("PGPORT", "5432").parse()?;
    let user = env("PGUSER", "postgres");
    let password = env("PGPASSWORD", "postgres");
    let database = env("PGDATABASE", "postgres");
    let slot = env("PGSLOT", "my_slot");
    let publication = env("PGPUBLICATION", "my_pub");

    // Provide explicitly, e.g. START_LSN=0/16B6C50
    let start_lsn = Lsn::parse(&env("START_LSN", "0/0")).unwrap();

    let cfg = ReplicationConfig {
        host: host.into(),
        port,
        user: user.into(),
        password: password.into(),
        database: database.into(),
        tls: TlsConfig {
            mode: SslMode::Disable,
            ca_pem_path: None,
            sni_hostname: None,
            client_cert_pem_path: None,
            client_key_pem_path: None,
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
