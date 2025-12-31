#![cfg(feature = "integration-tests")]

use anyhow::{Context, Result};
use pgwire_replication::{
    Lsn, ReplicationClient, ReplicationConfig, SslMode, TlsConfig, client::ReplicationEvent,
};
use std::time::{Duration, Instant};
use testcontainers::ContainerRequest;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt, core::IntoContainerPort, core::WaitFor};
use tokio::io::AsyncBufReadExt;
use tokio::task;
use tokio_postgres::NoTls;
use tracing::{debug, info, warn};

fn init_tracing() {
    // RUST_LOG=info,pgwire_replication=debug cargo test ...
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_test_writer()
        .try_init();
}

fn postgres_image(host_port: u16) -> ContainerRequest<GenericImage> {
    GenericImage::new("postgres", "16-alpine")
        .with_wait_for(WaitFor::message_on_stdout(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_DB", "postgres")
        // Logical replication requirements
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=10",
            "-c",
            "max_wal_senders=10",
            // Keep WAL around a bit longer for “seek” within a short test
            "-c",
            "wal_keep_size=256MB",
        ])
        .with_mapped_port(host_port, 5432.tcp())
}

async fn follow_container_logs(container: &testcontainers::ContainerAsync<GenericImage>) {
    // container log followers (helpful when this fails in CI)
    {
        let mut out = container.stdout(true);
        task::spawn(async move {
            let mut line = String::new();
            loop {
                line.clear();
                match out.read_line(&mut line).await {
                    Ok(0) => break,
                    Ok(_) => {
                        let l = line.trim_end();
                        if !l.is_empty() {
                            info!(target: "container:stdout", "{l}");
                        }
                    }
                    Err(e) => {
                        warn!(target: "container:stdout", "stdout follower error: {e}");
                        break;
                    }
                }
            }
        });
    }

    {
        let mut err = container.stderr(true);
        task::spawn(async move {
            let mut line = String::new();
            loop {
                line.clear();
                match err.read_line(&mut line).await {
                    Ok(0) => break,
                    Ok(_) => {
                        let l = line.trim_end();
                        if !l.is_empty() {
                            info!(target: "container:stderr", "{l}");
                        }
                    }
                    Err(e) => {
                        warn!(target: "container:stderr", "stderr follower error: {e}");
                        break;
                    }
                }
            }
        });
    }
}

async fn connect_pg(port: u16) -> Result<tokio_postgres::Client> {
    let dsn = format!("host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres");
    let (client, conn) = tokio_postgres::connect(&dsn, NoTls)
        .await
        .context("connect control-plane postgres")?;

    tokio::spawn(async move {
        if let Err(e) = conn.await {
            warn!("control-plane connection error: {e}");
        }
    });

    Ok(client)
}

async fn wait_for_pg_ready(port: u16, timeout: Duration) -> Result<tokio_postgres::Client> {
    let start = Instant::now();
    loop {
        match connect_pg(port).await {
            Ok(c) => return Ok(c),
            Err(e) => {
                if start.elapsed() > timeout {
                    return Err(e).context("postgres did not become ready in time");
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

async fn current_wal_lsn(client: &tokio_postgres::Client) -> Result<Lsn> {
    let row = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .context("read pg_current_wal_lsn")?;
    let lsn_str: String = row.get(0);
    Lsn::parse(&lsn_str).context(format!("parse lsn: {lsn_str}"))
}

async fn setup_publication_and_slot(client: &tokio_postgres::Client) -> Result<()> {
    client
        .batch_execute("CREATE TABLE IF NOT EXISTS t(id INT PRIMARY KEY, v TEXT);")
        .await
        .context("create table")?;

    client
        .batch_execute("DROP PUBLICATION IF EXISTS pub1;")
        .await
        .context("drop publication")?;

    client
        .batch_execute("CREATE PUBLICATION pub1 FOR TABLE t;")
        .await
        .context("create publication")?;

    // Drop slot if exists
    client
        .batch_execute(
            "SELECT pg_drop_replication_slot('slot1')
             WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='slot1');",
        )
        .await
        .context("drop slot if exists")?;

    client
        .batch_execute("SELECT * FROM pg_create_logical_replication_slot('slot1','pgoutput');")
        .await
        .context("create logical slot")?;

    Ok(())
}

async fn start_repl(
    host_port: u16,
    start_lsn: Lsn,
    stop_at_lsn: Option<Lsn>,
) -> Result<ReplicationClient> {
    ReplicationClient::connect(ReplicationConfig {
        host: "127.0.0.1".into(),
        port: host_port,
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
        slot: "slot1".into(),
        publication: "pub1".into(),
        start_lsn,
        stop_at_lsn,
        status_interval: Duration::from_secs(1),
        idle_timeout: Duration::from_secs(15),
        buffer_events: 2048,
    })
    .await
    .context("connect replication client")
}

async fn recv_until_xlog(
    client: &mut ReplicationClient,
    timeout: Duration,
) -> Result<(Lsn, usize)> {
    let deadline = Instant::now() + timeout;
    let mut keepalives = 0usize;

    while Instant::now() < deadline {
        let ev = client.recv().await.context("recv replication event")?;
        match ev {
            ReplicationEvent::XLogData { wal_end, data, .. } => {
                debug!("received XLogData wal_end={wal_end} bytes={}", data.len());
                client.update_applied_lsn(wal_end);
                return Ok((wal_end, keepalives));
            }
            ReplicationEvent::KeepAlive {
                wal_end,
                reply_requested,
                ..
            } => {
                keepalives += 1;
                debug!("received KeepAlive wal_end={wal_end} reply_requested={reply_requested}");
            }
            ReplicationEvent::StoppedAt { reached } => {
                // Treat “stop” without prior XLogData as unexpected for this helper
                anyhow::bail!("stopped unexpectedly at {reached} without observing XLogData");
            }
        }
    }

    anyhow::bail!("timeout waiting for XLogData");
}

async fn recv_keepalive(client: &mut ReplicationClient, timeout: Duration) -> Result<Lsn> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let ev = client.recv().await.context("recv replication event")?;
        match ev {
            ReplicationEvent::KeepAlive { wal_end, .. } => return Ok(wal_end),
            ReplicationEvent::XLogData { wal_end, .. } => {
                // Update feedback, but keep waiting for an actual keepalive
                client.update_applied_lsn(wal_end);
            }
            ReplicationEvent::StoppedAt { reached } => {
                anyhow::bail!("stopped unexpectedly at {reached}")
            }
        }
    }
    anyhow::bail!("timeout waiting for KeepAlive");
}

async fn recv_stopped_at(client: &mut ReplicationClient, timeout: Duration) -> Result<Lsn> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let ev = client.recv().await.context("recv replication event")?;
        match ev {
            ReplicationEvent::StoppedAt { reached } => return Ok(reached),
            ReplicationEvent::XLogData { wal_end, .. } => client.update_applied_lsn(wal_end),
            ReplicationEvent::KeepAlive { wal_end, .. } => {
                debug!("keepalive while waiting stop reached wal_end={wal_end}")
            }
        }
    }
    anyhow::bail!("timeout waiting for StoppedAt");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_replication_e2e_keepalive_seek_and_bounded_replay() -> Result<()> {
    init_tracing();

    let host_port: u16 = std::env::var("PG_ITEST_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(55432);

    info!("starting postgres container on host port {host_port}");
    let image = postgres_image(host_port);
    let container = image.start().await.expect("start postgres");
    info!("container id={}", container.id());

    follow_container_logs(&container).await;

    let client = wait_for_pg_ready(host_port, Duration::from_secs(30)).await?;
    setup_publication_and_slot(&client).await?;

    // Baseline LSN (start position)
    let base_lsn = current_wal_lsn(&client).await?;
    info!("base LSN: {base_lsn}");

    // 1) Start replication from base_lsn and validate we receive KeepAlive while idle.
    let mut repl = start_repl(host_port, base_lsn, None).await?;
    info!("replication connected from base_lsn={base_lsn}");

    // Wait for a keepalive (important: proves idle handling and prevents idle timeout regressions)
    let ka_wal_end = recv_keepalive(&mut repl, Duration::from_secs(10)).await?;
    info!("observed keepalive wal_end={ka_wal_end}");

    // 2) Emit first change and ensure we receive WAL data; record LSN for “seek”
    client
        .execute(
            "INSERT INTO t(id,v) VALUES (1,'hello') \
             ON CONFLICT (id) DO UPDATE SET v=excluded.v",
            &[],
        )
        .await
        .context("insert tx1")?;

    let (wal_end_after_tx1, ka_count) = recv_until_xlog(&mut repl, Duration::from_secs(10)).await?;
    let sql_lsn_after_tx1 = current_wal_lsn(&client).await?;
    info!(
        "tx1 observed: repl_wal_end={wal_end_after_tx1} sql_wal_lsn={sql_lsn_after_tx1} (keepalives while waiting: {ka_count})"
    );

    // Stop first replication client (simulates source restart)
    repl.stop();
    let _ = repl.join().await; // best-effort; worker may already have exited

    // 3) Emit second change (while replication is disconnected)
    client
        .execute(
            "INSERT INTO t(id,v) VALUES (2,'world') \
             ON CONFLICT (id) DO UPDATE SET v=excluded.v",
            &[],
        )
        .await
        .context("insert tx2")?;

    // 4) “Seek” by reconnecting from lsn_after_tx1 and observe WAL advances
    let mut repl2 = start_repl(host_port, sql_lsn_after_tx1, None).await?;
    info!("replication reconnected from sql_lsn_after_tx1={sql_lsn_after_tx1}");

    let (wal_end_after_tx2, _) = recv_until_xlog(&mut repl2, Duration::from_secs(10)).await?;
    let sql_lsn_after_tx2 = current_wal_lsn(&client).await?;

    info!("tx2 observed: repl_wal_end={wal_end_after_tx2} sql_wal_lsn={sql_lsn_after_tx2}");

    anyhow::ensure!(
        sql_lsn_after_tx2 > sql_lsn_after_tx1,
        "expected SQL wal lsn to advance; got tx2 sql_wal_lsn={sql_lsn_after_tx2} <= tx1 sql_wal_lsn={sql_lsn_after_tx1}"
    );

    repl2.stop();
    let _ = repl2.join().await;

    // 5) Bounded replay: connect from lsn_after_tx1 but stop at lsn_after_tx2
    // This is the basis for a future “replay up to checkpoint” feature in Deltaforge.
    let mut repl3 = start_repl(host_port, sql_lsn_after_tx1, Some(sql_lsn_after_tx2)).await?;
    let _ = recv_keepalive(&mut repl3, Duration::from_secs(10)).await?; // assert we receive at least one keep alive
    info!("bounded replay connected start={sql_lsn_after_tx1} stop_at={sql_lsn_after_tx2}");

    let reached = recv_stopped_at(&mut repl3, Duration::from_secs(15)).await?;
    info!("bounded replay stopped at reached={reached}");

    anyhow::ensure!(
        reached >= sql_lsn_after_tx2,
        "expected reached >= stop_at; reached={reached}, stop_at={sql_lsn_after_tx2}"
    );

    repl3.stop();
    let _ = repl3.join().await;

    info!("E2E test completed successfully");
    Ok(())
}
