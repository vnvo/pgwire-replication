#![cfg(feature = "integration-tests")]

//! Integration tests for pgwire-replication core functionality.
//!
//! Run with:
//! ```bash
//! cargo test --features integration-tests -- --nocapture
//! ```
//!
//! Override port with PG_ITEST_PORT=55432 if needed.

use anyhow::{Context, Result};
use bytes::Bytes;
use pgwire_replication::{
    Lsn, ReplicationClient, ReplicationConfig, TlsConfig, client::ReplicationEvent,
};
use std::time::{Duration, Instant};
use testcontainers::ContainerRequest;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt, core::IntoContainerPort, core::WaitFor};
use tokio::io::AsyncBufReadExt;
use tokio::task;
use tokio_postgres::NoTls;
use tracing::{debug, info, warn};

// ============================================================================
// Test Infrastructure
// ============================================================================

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_test_writer()
        .try_init();
}

fn get_available_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("get local addr")
        .port()
}

fn postgres_image(host_port: u16) -> ContainerRequest<GenericImage> {
    GenericImage::new("postgres", "16-alpine")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_DB", "postgres")
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=10",
            "-c",
            "max_wal_senders=10",
            "-c",
            "wal_keep_size=256MB",
        ])
        .with_mapped_port(host_port, 5432.tcp())
}

async fn follow_container_logs(container: &testcontainers::ContainerAsync<GenericImage>) {
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

// ============================================================================
// Postgres Helpers
// ============================================================================

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

async fn setup_publication_and_slot(
    client: &tokio_postgres::Client,
    slot: &str,
    publication: &str,
) -> Result<()> {
    client
        .batch_execute("CREATE TABLE IF NOT EXISTS t(id INT PRIMARY KEY, v TEXT);")
        .await
        .context("create table")?;

    client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {publication};"))
        .await
        .context("drop publication")?;

    client
        .batch_execute(&format!("CREATE PUBLICATION {publication} FOR TABLE t;"))
        .await
        .context("create publication")?;

    client
        .batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{slot}')
             WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='{slot}');"
        ))
        .await
        .context("drop slot if exists")?;

    client
        .batch_execute(&format!(
            "SELECT * FROM pg_create_logical_replication_slot('{slot}','pgoutput');"
        ))
        .await
        .context("create logical slot")?;

    Ok(())
}

// ============================================================================
// Replication Helpers
// ============================================================================

fn replication_config(
    host_port: u16,
    slot: &str,
    publication: &str,
    start_lsn: Lsn,
    stop_at_lsn: Option<Lsn>,
) -> ReplicationConfig {
    ReplicationConfig {
        host: "127.0.0.1".into(),
        port: host_port,
        user: "postgres".into(),
        password: "postgres".into(),
        database: "postgres".into(),
        tls: TlsConfig::disabled(),
        slot: slot.into(),
        publication: publication.into(),
        start_lsn,
        stop_at_lsn,
        status_interval: Duration::from_secs(1),
        idle_timeout: Duration::from_secs(15),
        buffer_events: 2048,
    }
}

async fn start_repl(
    host_port: u16,
    start_lsn: Lsn,
    stop_at_lsn: Option<Lsn>,
) -> Result<ReplicationClient> {
    ReplicationClient::connect(replication_config(
        host_port,
        "slot1",
        "pub1",
        start_lsn,
        stop_at_lsn,
    ))
    .await
    .context("connect replication client")
}

/// Receive events until XLogData arrives. Returns (wal_end, payload, keepalive_count).
async fn recv_until_xlog(
    client: &mut ReplicationClient,
    timeout: Duration,
) -> Result<(Lsn, Bytes, usize)> {
    let deadline = Instant::now() + timeout;
    let mut keepalives = 0usize;

    while Instant::now() < deadline {
        let ev = client.recv().await.context("recv replication event")?;
        match ev {
            ReplicationEvent::XLogData { wal_end, data, .. } => {
                debug!("received XLogData wal_end={wal_end} bytes={}", data.len());
                client.update_applied_lsn(wal_end);
                return Ok((wal_end, data, keepalives));
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
                anyhow::bail!("stopped unexpectedly at {reached} without observing XLogData");
            }
        }
    }

    anyhow::bail!("timeout waiting for XLogData");
}

/// Drain all available XLogData events until idle (keepalive) or timeout.
/// Returns count of XLogData events received.
async fn drain_xlog_events(
    client: &mut ReplicationClient,
    idle_timeout: Duration,
) -> Result<usize> {
    let mut count = 0usize;

    loop {
        match tokio::time::timeout(idle_timeout, client.recv()).await {
            Ok(Ok(ReplicationEvent::XLogData { wal_end, .. })) => {
                client.update_applied_lsn(wal_end);
                count += 1;
            }
            Ok(Ok(ReplicationEvent::KeepAlive { .. })) => {
                // Idle - we've caught up
                break;
            }
            Ok(Ok(ReplicationEvent::StoppedAt { .. })) => break,
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => break, // timeout - assume caught up
        }
    }

    Ok(count)
}

async fn recv_keepalive(client: &mut ReplicationClient, timeout: Duration) -> Result<Lsn> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let ev = client.recv().await.context("recv replication event")?;
        match ev {
            ReplicationEvent::KeepAlive { wal_end, .. } => return Ok(wal_end),
            ReplicationEvent::XLogData { wal_end, .. } => {
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

// ============================================================================
// Tests
// ============================================================================

/// Core E2E test covering:
/// - Keepalive handling while idle
/// - INSERT/UPDATE/DELETE replication
/// - Seek (reconnect from known LSN)
/// - Bounded replay (stop_at_lsn)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_replication_e2e() -> Result<()> {
    init_tracing();

    let host_port: u16 = std::env::var("PG_ITEST_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(get_available_port);

    info!("starting postgres container on host port {host_port}");
    let image = postgres_image(host_port);
    let container = image.start().await.expect("start postgres");
    info!("container id={}", container.id());

    follow_container_logs(&container).await;

    let client = wait_for_pg_ready(host_port, Duration::from_secs(30)).await?;
    setup_publication_and_slot(&client, "slot1", "pub1").await?;

    // Clean slate
    client.execute("DELETE FROM t", &[]).await?;

    let base_lsn = current_wal_lsn(&client).await?;
    info!("base LSN: {base_lsn}");

    // -------------------------------------------------------------------------
    // Phase 1: Keepalive handling while idle
    // -------------------------------------------------------------------------
    let mut repl = start_repl(host_port, base_lsn, None).await?;
    info!("replication connected from base_lsn={base_lsn}");

    let ka_wal_end = recv_keepalive(&mut repl, Duration::from_secs(10)).await?;
    info!("phase 1: observed keepalive wal_end={ka_wal_end}");

    // -------------------------------------------------------------------------
    // Phase 2: INSERT replication
    // -------------------------------------------------------------------------
    client
        .execute("INSERT INTO t(id, v) VALUES (1, 'hello')", &[])
        .await
        .context("insert")?;

    let (wal_end_insert, data, _) = recv_until_xlog(&mut repl, Duration::from_secs(10)).await?;
    info!(
        "phase 2: INSERT observed wal_end={wal_end_insert} payload_bytes={}",
        data.len()
    );
    anyhow::ensure!(
        !data.is_empty(),
        "expected non-empty pgoutput payload for INSERT"
    );

    // -------------------------------------------------------------------------
    // Phase 3: UPDATE replication
    // -------------------------------------------------------------------------
    client
        .execute("UPDATE t SET v = 'updated' WHERE id = 1", &[])
        .await
        .context("update")?;

    let (_wal_end_update, data, _) = recv_until_xlog(&mut repl, Duration::from_secs(10)).await?;
    info!("phase 3: UPDATE observed payload_bytes={}", data.len());
    anyhow::ensure!(
        !data.is_empty(),
        "expected non-empty pgoutput payload for UPDATE"
    );
    // Note: wal_end in XLogData can be 0/0 for messages within a transaction.
    // Only commit messages reliably have the actual LSN.

    // -------------------------------------------------------------------------
    // Phase 4: DELETE replication
    // -------------------------------------------------------------------------
    client
        .execute("DELETE FROM t WHERE id = 1", &[])
        .await
        .context("delete")?;

    let (_wal_end_delete, data, _) = recv_until_xlog(&mut repl, Duration::from_secs(10)).await?;
    info!("phase 4: DELETE observed payload_bytes={}", data.len());
    anyhow::ensure!(
        !data.is_empty(),
        "expected non-empty pgoutput payload for DELETE"
    );

    let lsn_after_delete = current_wal_lsn(&client).await?;

    // Stop first replication session
    repl.stop();
    let _ = repl.join().await;

    // -------------------------------------------------------------------------
    // Phase 5: Seek - reconnect from known LSN
    // -------------------------------------------------------------------------
    // Insert while disconnected
    client
        .execute("INSERT INTO t(id, v) VALUES (2, 'world')", &[])
        .await
        .context("insert while disconnected")?;

    // Reconnect from lsn_after_delete - should see the new insert
    let mut repl2 = start_repl(host_port, lsn_after_delete, None).await?;
    info!("phase 5: reconnected from lsn_after_delete={lsn_after_delete}");

    let (wal_end_reconnect, _, _) = recv_until_xlog(&mut repl2, Duration::from_secs(10)).await?;
    let lsn_after_reconnect = current_wal_lsn(&client).await?;
    info!("phase 5: seek verified wal_end={wal_end_reconnect} sql_lsn={lsn_after_reconnect}");

    anyhow::ensure!(
        lsn_after_reconnect > lsn_after_delete,
        "expected LSN to advance after reconnect insert"
    );

    repl2.stop();
    let _ = repl2.join().await;

    // -------------------------------------------------------------------------
    // Phase 6: Bounded replay (stop_at_lsn)
    // -------------------------------------------------------------------------
    let stop_target = lsn_after_reconnect;
    let mut repl3 = start_repl(host_port, lsn_after_delete, Some(stop_target)).await?;
    info!("phase 6: bounded replay start={lsn_after_delete} stop_at={stop_target}");

    let reached = recv_stopped_at(&mut repl3, Duration::from_secs(15)).await?;
    info!("phase 6: bounded replay stopped at reached={reached}");

    anyhow::ensure!(
        reached >= stop_target,
        "expected reached >= stop_at; reached={reached}, stop_at={stop_target}"
    );

    repl3.stop();
    let _ = repl3.join().await;

    info!("E2E test completed successfully");
    Ok(())
}

/// Test batch inserts - ensures we handle rapid WAL production correctly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_replication_batch_insert() -> Result<()> {
    init_tracing();

    let host_port: u16 = std::env::var("PG_ITEST_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(get_available_port);

    info!("starting postgres container on host port {host_port}");
    let image = postgres_image(host_port);
    let container = image.start().await.expect("start postgres");

    follow_container_logs(&container).await;

    let client = wait_for_pg_ready(host_port, Duration::from_secs(30)).await?;
    setup_publication_and_slot(&client, "slot_batch", "pub_batch").await?;

    client.execute("DELETE FROM t", &[]).await?;

    let base_lsn = current_wal_lsn(&client).await?;

    let mut repl = ReplicationClient::connect(replication_config(
        host_port,
        "slot_batch",
        "pub_batch",
        base_lsn,
        None,
    ))
    .await?;

    // Wait for connection to stabilize
    recv_keepalive(&mut repl, Duration::from_secs(10)).await?;

    // Batch insert 100 rows
    const BATCH_SIZE: i32 = 100;
    for i in 0..BATCH_SIZE {
        client
            .execute(
                "INSERT INTO t(id, v) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET v = excluded.v",
                &[&i, &format!("batch_val_{i}")],
            )
            .await?;
    }
    info!("inserted {BATCH_SIZE} rows");

    // Wait for at least one XLogData event (proves replication is working)
    let (first_wal_end, first_data, _) =
        recv_until_xlog(&mut repl, Duration::from_secs(10)).await?;
    info!(
        "first batch event: wal_end={first_wal_end} bytes={}",
        first_data.len()
    );

    // Drain remaining XLogData events
    let remaining = drain_xlog_events(&mut repl, Duration::from_secs(2)).await?;
    let total = 1 + remaining;
    info!("received {total} total XLogData events from batch insert");

    // We should receive at least some events (exact count depends on transaction batching)
    anyhow::ensure!(
        total > 0,
        "expected at least one XLogData event from batch insert"
    );

    repl.stop();
    let _ = repl.join().await;

    info!("batch insert test completed successfully");
    Ok(())
}

/// Test error handling: connecting with a nonexistent slot should fail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_replication_invalid_slot_error() -> Result<()> {
    init_tracing();

    let host_port: u16 = std::env::var("PG_ITEST_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(get_available_port);

    info!("starting postgres container on host port {host_port}");
    let image = postgres_image(host_port);
    let container = image.start().await.expect("start postgres");

    follow_container_logs(&container).await;

    let client = wait_for_pg_ready(host_port, Duration::from_secs(30)).await?;

    // Create publication but NOT the slot
    client
        .batch_execute("CREATE TABLE IF NOT EXISTS t(id INT PRIMARY KEY, v TEXT);")
        .await?;
    client
        .batch_execute("DROP PUBLICATION IF EXISTS pub_noexist;")
        .await?;
    client
        .batch_execute("CREATE PUBLICATION pub_noexist FOR TABLE t;")
        .await?;

    let base_lsn = current_wal_lsn(&client).await?;

    // Attempt to connect with nonexistent slot
    let result = ReplicationClient::connect(replication_config(
        host_port,
        "nonexistent_slot_xyz",
        "pub_noexist",
        base_lsn,
        None,
    ))
    .await;

    match result {
        Ok(mut repl) => {
            // Connection might succeed but first recv should fail
            let recv_result = repl.recv().await;
            anyhow::ensure!(
                recv_result.is_err(),
                "expected error when using nonexistent slot, got: {:?}",
                recv_result
            );
            info!("invalid slot error surfaced on recv (as expected)");
        }
        Err(e) => {
            info!("invalid slot error surfaced on connect (as expected): {e}");
        }
    }

    info!("invalid slot error test completed successfully");
    Ok(())
}

/// Test multi-table publication (verifies we handle multiple relations).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_replication_multi_table() -> Result<()> {
    init_tracing();

    let host_port: u16 = std::env::var("PG_ITEST_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(get_available_port);

    info!("starting postgres container on host port {host_port}");
    let image = postgres_image(host_port);
    let container = image.start().await.expect("start postgres");

    follow_container_logs(&container).await;

    let client = wait_for_pg_ready(host_port, Duration::from_secs(30)).await?;

    // Create two tables
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS t1(id INT PRIMARY KEY, v TEXT);
             CREATE TABLE IF NOT EXISTS t2(id INT PRIMARY KEY, v TEXT);",
        )
        .await?;

    client
        .batch_execute("DROP PUBLICATION IF EXISTS pub_multi;")
        .await?;
    client
        .batch_execute("CREATE PUBLICATION pub_multi FOR TABLE t1, t2;")
        .await?;

    client
        .batch_execute(
            "SELECT pg_drop_replication_slot('slot_multi')
             WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='slot_multi');",
        )
        .await?;
    client
        .batch_execute("SELECT * FROM pg_create_logical_replication_slot('slot_multi','pgoutput');")
        .await?;

    client.execute("DELETE FROM t1", &[]).await?;
    client.execute("DELETE FROM t2", &[]).await?;

    let base_lsn = current_wal_lsn(&client).await?;

    let mut repl = ReplicationClient::connect(replication_config(
        host_port,
        "slot_multi",
        "pub_multi",
        base_lsn,
        None,
    ))
    .await?;

    recv_keepalive(&mut repl, Duration::from_secs(10)).await?;

    // Insert into both tables
    client
        .execute("INSERT INTO t1(id, v) VALUES (1, 'table1_row')", &[])
        .await?;
    let (_wal_t1, data_t1, _) = recv_until_xlog(&mut repl, Duration::from_secs(10)).await?;
    info!("t1 insert: bytes={}", data_t1.len());

    client
        .execute("INSERT INTO t2(id, v) VALUES (1, 'table2_row')", &[])
        .await?;
    let (_wal_t2, data_t2, _) = recv_until_xlog(&mut repl, Duration::from_secs(10)).await?;
    info!("t2 insert: bytes={}", data_t2.len());

    anyhow::ensure!(!data_t1.is_empty(), "expected payload for t1 insert");
    anyhow::ensure!(!data_t2.is_empty(), "expected payload for t2 insert");
    // Note: wal_end ordering not checked - can be 0/0 for messages within transactions

    repl.stop();
    let _ = repl.join().await;

    info!("multi-table test completed successfully");
    Ok(())
}

// ============================================================================
// SCRAM-SHA-256 Authentication Test
// ============================================================================

/// Test SCRAM-SHA-256 authentication (the default in PostgreSQL 14+).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "scram")]
async fn postgres_replication_scram_auth() -> Result<()> {
    init_tracing();
    let host_port = get_available_port();

    // Create pg_hba.conf that requires scram-sha-256
    let hba_content = r#"
# TYPE  DATABASE        USER            ADDRESS                 METHOD
local   all             all                                     scram-sha-256
host    all             all             0.0.0.0/0               scram-sha-256
host    all             all             ::/0                    scram-sha-256
host    replication     all             0.0.0.0/0               scram-sha-256
host    replication     all             ::/0                    scram-sha-256
"#;

    let temp_dir = tempfile::tempdir()?;
    let hba_path = temp_dir.path().join("pg_hba.conf");
    std::fs::write(&hba_path, hba_content)?;

    info!("starting postgres container with SCRAM auth on port {host_port}");

    let image = GenericImage::new("postgres", "16-alpine")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "scram_test_password")
        .with_env_var("POSTGRES_USER", "scram_user")
        .with_env_var("POSTGRES_DB", "postgres")
        .with_env_var("POSTGRES_HOST_AUTH_METHOD", "scram-sha-256")
        .with_env_var(
            "POSTGRES_INITDB_ARGS",
            "--auth-host=scram-sha-256 --auth-local=scram-sha-256",
        )
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=10",
            "-c",
            "max_wal_senders=10",
            "-c",
            "password_encryption=scram-sha-256",
        ])
        .with_mapped_port(host_port, 5432.tcp());

    let container = image.start().await.expect("start postgres with SCRAM");
    follow_container_logs(&container).await;

    // Connect with tokio-postgres first to set up slot (it handles SCRAM internally)
    let dsn = format!(
        "host=127.0.0.1 port={host_port} user=scram_user password=scram_test_password dbname=postgres"
    );

    let client = loop {
        match tokio_postgres::connect(&dsn, NoTls).await {
            Ok((client, conn)) => {
                tokio::spawn(async move {
                    let _ = conn.await;
                });
                break client;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    };

    // Setup replication
    client
        .batch_execute("CREATE TABLE IF NOT EXISTS t(id INT PRIMARY KEY, v TEXT);")
        .await?;
    client
        .batch_execute("DROP PUBLICATION IF EXISTS pub_scram;")
        .await?;
    client
        .batch_execute("CREATE PUBLICATION pub_scram FOR TABLE t;")
        .await?;
    client
        .batch_execute(
            "SELECT pg_drop_replication_slot('slot_scram')
             WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='slot_scram');",
        )
        .await?;
    client
        .batch_execute("SELECT * FROM pg_create_logical_replication_slot('slot_scram','pgoutput');")
        .await?;

    let base_lsn = current_wal_lsn(&client).await?;
    info!("SCRAM test: base_lsn={base_lsn}");

    // Now connect with our replication client using SCRAM
    let config = ReplicationConfig {
        host: "127.0.0.1".into(),
        port: host_port,
        user: "scram_user".into(),
        password: "scram_test_password".into(),
        database: "postgres".into(),
        tls: TlsConfig::disabled(),
        slot: "slot_scram".into(),
        publication: "pub_scram".into(),
        start_lsn: base_lsn,
        stop_at_lsn: None,
        status_interval: Duration::from_secs(1),
        idle_timeout: Duration::from_secs(15),
        buffer_events: 1024,
    };

    let mut repl = ReplicationClient::connect(config)
        .await
        .context("connect with SCRAM auth")?;

    info!("SCRAM auth successful, waiting for keepalive");

    // Verify connection works
    let ka = recv_keepalive(&mut repl, Duration::from_secs(10)).await?;
    info!("SCRAM test: received keepalive wal_end={ka}");

    // Test actual replication
    client
        .execute("INSERT INTO t(id, v) VALUES (1, 'scram_test')", &[])
        .await?;

    let (wal_end, data, _) = recv_until_xlog(&mut repl, Duration::from_secs(10)).await?;
    info!(
        "SCRAM test: received XLogData wal_end={wal_end} bytes={}",
        data.len()
    );

    anyhow::ensure!(
        !data.is_empty(),
        "expected payload from SCRAM-authenticated replication"
    );

    repl.stop();
    let _ = repl.join().await;

    info!("SCRAM authentication test completed successfully");
    Ok(())
}

// ============================================================================
// TLS Connection Test
// ============================================================================

/// Test TLS connection with certificate verification.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "tls-rustls")]
async fn postgres_replication_tls() -> Result<()> {
    use std::process::Command;

    init_tracing();

    let host_port = get_available_port();

    // Create temp directory for certificates
    let cert_dir = tempfile::tempdir()?;
    let cert_path = cert_dir.path();

    // Generate X.509v3 CA certificate (rustls requires v3 with proper extensions)
    let ca_key = cert_path.join("ca.key");
    let ca_cert = cert_path.join("ca.crt");

    let status = Command::new("openssl")
        .args([
            "req",
            "-new",
            "-x509",
            "-days",
            "1",
            "-nodes",
            "-newkey",
            "rsa:2048",
            "-keyout",
            ca_key.to_str().unwrap(),
            "-out",
            ca_cert.to_str().unwrap(),
            "-subj",
            "/CN=Test-CA",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ])
        .status()
        .context("generate CA cert")?;
    anyhow::ensure!(status.success(), "openssl CA generation failed");

    // Server key and CSR
    let server_key = cert_path.join("server.key");
    let server_csr = cert_path.join("server.csr");
    let server_cert = cert_path.join("server.crt");

    let status = Command::new("openssl")
        .args([
            "req",
            "-new",
            "-nodes",
            "-newkey",
            "rsa:2048",
            "-keyout",
            server_key.to_str().unwrap(),
            "-out",
            server_csr.to_str().unwrap(),
            "-subj",
            "/CN=localhost",
        ])
        .status()
        .context("generate server key/CSR")?;
    anyhow::ensure!(status.success(), "openssl server key generation failed");

    // Create extensions file for server cert (X.509v3)
    let ext_file = cert_path.join("server.ext");
    std::fs::write(
        &ext_file,
        "basicConstraints=CA:FALSE\n\
         keyUsage=critical,digitalSignature,keyEncipherment\n\
         extendedKeyUsage=serverAuth\n\
         subjectAltName=DNS:localhost,IP:127.0.0.1\n",
    )?;

    // Sign server cert with CA (with extensions for X.509v3)
    let status = Command::new("openssl")
        .args([
            "x509",
            "-req",
            "-days",
            "1",
            "-in",
            server_csr.to_str().unwrap(),
            "-CA",
            ca_cert.to_str().unwrap(),
            "-CAkey",
            ca_key.to_str().unwrap(),
            "-CAcreateserial",
            "-out",
            server_cert.to_str().unwrap(),
            "-extfile",
            ext_file.to_str().unwrap(),
        ])
        .status()
        .context("sign server cert")?;
    anyhow::ensure!(status.success(), "openssl signing failed");

    info!("generated TLS certificates in {}", cert_path.display());

    // Start container with certs mounted
    let image = GenericImage::new("postgres", "16-alpine")
        .with_wait_for(WaitFor::message_on_stdout(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_DB", "postgres")
        .with_mount(testcontainers::core::Mount::bind_mount(
            cert_path.to_str().unwrap(),
            "/certs",
        ))
        .with_cmd([
            "sh",
            "-c",
            "chown postgres:postgres /certs/* && chmod 600 /certs/server.key && \
             exec /usr/local/bin/docker-entrypoint.sh postgres \
                -c wal_level=logical \
                -c max_replication_slots=10 \
                -c max_wal_senders=10 \
                -c ssl=on \
                -c ssl_cert_file=/certs/server.crt \
                -c ssl_key_file=/certs/server.key \
                -c ssl_ca_file=/certs/ca.crt",
        ])
        .with_mapped_port(host_port, 5432.tcp());

    info!("starting postgres container with TLS on port {host_port}");
    let container = image.start().await.expect("start postgres with TLS");
    follow_container_logs(&container).await;

    // Wait for postgres with TLS
    let client = wait_for_pg_ready(host_port, Duration::from_secs(30)).await?;

    // Setup replication
    client
        .batch_execute("CREATE TABLE IF NOT EXISTS t(id INT PRIMARY KEY, v TEXT);")
        .await?;
    client
        .batch_execute("DROP PUBLICATION IF EXISTS pub_tls;")
        .await?;
    client
        .batch_execute("CREATE PUBLICATION pub_tls FOR TABLE t;")
        .await?;
    client
        .batch_execute(
            "SELECT pg_drop_replication_slot('slot_tls')
             WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='slot_tls');",
        )
        .await?;
    client
        .batch_execute("SELECT * FROM pg_create_logical_replication_slot('slot_tls','pgoutput');")
        .await?;

    let base_lsn = current_wal_lsn(&client).await?;
    info!("TLS test: base_lsn={base_lsn}");

    // Connect with TLS (verify-ca mode since we're using localhost)
    let tls_config = TlsConfig::verify_ca(Some(ca_cert.clone()));

    let config = ReplicationConfig {
        host: "127.0.0.1".into(),
        port: host_port,
        user: "postgres".into(),
        password: "postgres".into(),
        database: "postgres".into(),
        tls: tls_config,
        slot: "slot_tls".into(),
        publication: "pub_tls".into(),
        start_lsn: base_lsn,
        stop_at_lsn: None,
        status_interval: Duration::from_secs(1),
        idle_timeout: Duration::from_secs(15),
        buffer_events: 1024,
    };

    info!("TLS connection successful, waiting for keepalive");

    let mut repl = ReplicationClient::connect(config)
        .await
        .context("connect with TLS")?;

    // Verify connection works
    let ka = recv_keepalive(&mut repl, Duration::from_secs(10)).await?;
    info!("TLS test: received keepalive wal_end={ka}");

    // Test actual replication over TLS
    client
        .execute("INSERT INTO t(id, v) VALUES (1, 'tls_test')", &[])
        .await?;

    let (wal_end, data, _) = recv_until_xlog(&mut repl, Duration::from_secs(10)).await?;
    info!(
        "TLS test: received XLogData wal_end={wal_end} bytes={}",
        data.len()
    );

    anyhow::ensure!(
        !data.is_empty(),
        "expected payload from TLS-encrypted replication"
    );

    repl.stop();
    let _ = repl.join().await;

    info!("TLS connection test completed successfully");
    Ok(())
}

/// Test TLS with require mode (no verification - just encryption).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "tls-rustls")]
async fn postgres_replication_tls_require_mode() -> Result<()> {
    use std::process::Command;

    init_tracing();

    let host_port = get_available_port();

    // Create temp directory for certificates
    let cert_dir = tempfile::tempdir()?;
    let cert_path = cert_dir.path();

    // Generate self-signed server certificate (no CA needed for require mode)
    let server_key = cert_path.join("server.key");
    let server_cert = cert_path.join("server.crt");

    let status = Command::new("openssl")
        .args([
            "req",
            "-new",
            "-x509",
            "-days",
            "1",
            "-nodes",
            "-newkey",
            "rsa:2048",
            "-keyout",
            server_key.to_str().unwrap(),
            "-out",
            server_cert.to_str().unwrap(),
            "-subj",
            "/CN=localhost",
        ])
        .status()
        .context("generate self-signed cert")?;
    anyhow::ensure!(status.success(), "openssl generation failed");

    info!("generated self-signed cert in {}", cert_path.display());

    let image = GenericImage::new("postgres", "16-alpine")
        .with_wait_for(WaitFor::message_on_stdout(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_DB", "postgres")
        .with_mount(testcontainers::core::Mount::bind_mount(
            cert_path.to_str().unwrap(),
            "/certs",
        ))
        .with_cmd([
            "sh",
            "-c",
            "chown postgres:postgres /certs/* && chmod 600 /certs/server.key && \
             exec /usr/local/bin/docker-entrypoint.sh postgres \
                -c wal_level=logical \
                -c max_replication_slots=10 \
                -c ssl=on \
                -c ssl_cert_file=/certs/server.crt \
                -c ssl_key_file=/certs/server.key",
        ])
        .with_mapped_port(host_port, 5432.tcp());

    info!("starting postgres with TLS (require mode) on port {host_port}");
    let container = image.start().await.expect("start postgres");
    follow_container_logs(&container).await;

    let client = wait_for_pg_ready(host_port, Duration::from_secs(30)).await?;

    // Setup
    client
        .batch_execute("CREATE TABLE IF NOT EXISTS t(id INT PRIMARY KEY, v TEXT);")
        .await?;
    client
        .batch_execute("DROP PUBLICATION IF EXISTS pub_tls_req;")
        .await?;
    client
        .batch_execute("CREATE PUBLICATION pub_tls_req FOR TABLE t;")
        .await?;
    client
        .batch_execute(
            "SELECT pg_drop_replication_slot('slot_tls_req')
             WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='slot_tls_req');",
        )
        .await?;
    client
        .batch_execute(
            "SELECT * FROM pg_create_logical_replication_slot('slot_tls_req','pgoutput');",
        )
        .await?;

    let base_lsn = current_wal_lsn(&client).await?;

    // Connect with TLS require mode (no verification)
    let config = ReplicationConfig {
        host: "127.0.0.1".into(),
        port: host_port,
        user: "postgres".into(),
        password: "postgres".into(),
        database: "postgres".into(),
        tls: TlsConfig::require(),
        slot: "slot_tls_req".into(),
        publication: "pub_tls_req".into(),
        start_lsn: base_lsn,
        stop_at_lsn: None,
        status_interval: Duration::from_secs(1),
        idle_timeout: Duration::from_secs(15),
        buffer_events: 1024,
    };

    let mut repl = ReplicationClient::connect(config)
        .await
        .context("connect with TLS require mode")?;

    let ka = recv_keepalive(&mut repl, Duration::from_secs(10)).await?;
    info!("TLS require mode: received keepalive wal_end={ka}");

    // Verify replication works
    client
        .execute("INSERT INTO t(id, v) VALUES (1, 'tls_require')", &[])
        .await?;
    let (wal_end, data, _) = recv_until_xlog(&mut repl, Duration::from_secs(10)).await?;

    anyhow::ensure!(!data.is_empty(), "expected payload");
    info!("TLS require mode: replication working, wal_end={wal_end}");

    repl.stop();
    let _ = repl.join().await;

    info!("TLS require mode test completed successfully");
    Ok(())
}

/// Test that TLS verification fails with wrong CA.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[cfg(feature = "tls-rustls")]
async fn postgres_replication_tls_wrong_ca_fails() -> Result<()> {
    use std::process::Command;

    init_tracing();

    let host_port = get_available_port();
    let cert_dir = tempfile::tempdir()?;
    let cert_path = cert_dir.path();

    // Generate server cert
    let server_key = cert_path.join("server.key");
    let server_cert = cert_path.join("server.crt");

    Command::new("openssl")
        .args([
            "req",
            "-new",
            "-x509",
            "-days",
            "1",
            "-nodes",
            "-newkey",
            "rsa:2048",
            "-keyout",
            server_key.to_str().unwrap(),
            "-out",
            server_cert.to_str().unwrap(),
            "-subj",
            "/CN=localhost",
        ])
        .status()?;

    // Generate a DIFFERENT CA (that didn't sign the server cert)
    let wrong_ca = cert_path.join("wrong_ca.crt");
    Command::new("openssl")
        .args([
            "req",
            "-new",
            "-x509",
            "-days",
            "1",
            "-nodes",
            "-newkey",
            "rsa:2048",
            "-keyout",
            cert_path.join("wrong_ca.key").to_str().unwrap(),
            "-out",
            wrong_ca.to_str().unwrap(),
            "-subj",
            "/CN=Wrong-CA",
        ])
        .status()?;

    let image = GenericImage::new("postgres", "16-alpine")
        .with_wait_for(WaitFor::message_on_stdout(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_DB", "postgres")
        .with_mount(testcontainers::core::Mount::bind_mount(
            cert_path.to_str().unwrap(),
            "/certs",
        ))
        .with_cmd([
            "sh",
            "-c",
            "chown postgres:postgres /certs/* && chmod 600 /certs/server.key && \
             exec /usr/local/bin/docker-entrypoint.sh postgres \
                -c wal_level=logical \
                -c max_replication_slots=10 \
                -c ssl=on \
                -c ssl_cert_file=/certs/server.crt \
                -c ssl_key_file=/certs/server.key",
        ])
        .with_mapped_port(host_port, 5432.tcp());

    let container = image.start().await.expect("start postgres");
    follow_container_logs(&container).await;

    let client = wait_for_pg_ready(host_port, Duration::from_secs(30)).await?;

    client
        .batch_execute("CREATE TABLE IF NOT EXISTS t(id INT PRIMARY KEY, v TEXT);")
        .await?;
    client
        .batch_execute("DROP PUBLICATION IF EXISTS pub_wrong_ca;")
        .await?;
    client
        .batch_execute("CREATE PUBLICATION pub_wrong_ca FOR TABLE t;")
        .await?;
    client
        .batch_execute(
            "SELECT pg_drop_replication_slot('slot_wrong_ca')
             WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='slot_wrong_ca');",
        )
        .await?;
    client
        .batch_execute(
            "SELECT * FROM pg_create_logical_replication_slot('slot_wrong_ca','pgoutput');",
        )
        .await?;

    let base_lsn = current_wal_lsn(&client).await?;

    // Try to connect with wrong CA - should fail verification
    let config = ReplicationConfig {
        host: "127.0.0.1".into(),
        port: host_port,
        user: "postgres".into(),
        password: "postgres".into(),
        database: "postgres".into(),
        tls: TlsConfig::verify_ca(Some(wrong_ca)), // Wrong CA!
        slot: "slot_wrong_ca".into(),
        publication: "pub_wrong_ca".into(),
        start_lsn: base_lsn,
        stop_at_lsn: None,
        status_interval: Duration::from_secs(1),
        idle_timeout: Duration::from_secs(15),
        buffer_events: 1024,
    };

    let result = ReplicationClient::connect(config).await;

    match result {
        Ok(mut repl) => {
            // Connection might succeed but first operation should fail
            let recv_result = repl.recv().await;
            anyhow::ensure!(
                recv_result.is_err(),
                "expected TLS verification to fail with wrong CA"
            );
            info!("TLS verification failed as expected (on recv)");
        }
        Err(e) => {
            info!("TLS verification failed as expected: {e}");
            anyhow::ensure!(
                e.to_string().to_lowercase().contains("tls")
                    || e.to_string().to_lowercase().contains("certificate")
                    || e.to_string().to_lowercase().contains("ssl")
                    || e.to_string().to_lowercase().contains("verify"),
                "expected TLS-related error, got: {e}"
            );
        }
    }

    info!("TLS wrong CA test completed successfully");
    Ok(())
}
