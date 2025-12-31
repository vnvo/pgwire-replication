use crate::config::ReplicationConfig;
use crate::error::{PgWireError, Result};
use crate::lsn::Lsn;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

#[cfg(not(feature = "tls-rustls"))]
use crate::config::SslMode;

use super::worker::{ReplicationEvent, ReplicationEventReceiver, WorkerState};

/// PostgreSQL logical replication client.
///
/// This client spawns a background worker task that maintains the replication
/// connection and streams events to the consumer via a bounded channel.
///
/// # Example
///
/// ```ignore
/// use pgwire_replication::client::ReplicationClient;
/// use pgwire_replication::config::ReplicationConfig;
///
/// let config = ReplicationConfig::new(
///     "localhost", "postgres", "password", "mydb", "my_slot", "my_pub"
/// );
///
/// let mut client = ReplicationClient::connect(config).await?;
///
/// loop {
///     match client.recv().await? {
///         ReplicationEvent::XLogData { data, wal_end, .. } => {
///             // Process the change event
///             process_change(&data);
///             // Report progress
///             client.update_applied_lsn(wal_end);
///         }
///         ReplicationEvent::KeepAlive { .. } => {
///             // Server heartbeat, automatically handled
///         }
///         ReplicationEvent::StoppedAt { reached } => {
///             println!("Reached stop LSN: {reached}");
///             break;
///         }
///     }
/// }
/// ```
///
/// # Shutdown
///
/// The client can be stopped gracefully by calling [`stop()`](Self::stop),
/// or it will be stopped automatically when dropped.
pub struct ReplicationClient {
    rx: ReplicationEventReceiver,
    applied_tx: watch::Sender<Lsn>,
    stop_tx: watch::Sender<bool>,
    join: Option<JoinHandle<std::result::Result<(), PgWireError>>>,
}

impl ReplicationClient {
    /// Connect to PostgreSQL and start streaming replication events.
    ///
    /// This establishes a TCP connection (optionally upgrading to TLS),
    /// authenticates, and starts the replication stream. Events are buffered
    /// in a channel of size `config.buffer_events`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - TCP connection fails
    /// - TLS handshake fails (when enabled)
    /// - Authentication fails
    /// - Replication slot doesn't exist
    /// - Publication doesn't exist
    pub async fn connect(cfg: ReplicationConfig) -> Result<Self> {
        let (tx, rx) = mpsc::channel(cfg.buffer_events);
        let (applied_tx, applied_rx) = watch::channel(cfg.start_lsn);
        let (stop_tx, stop_rx) = watch::channel(false);

        let join = tokio::spawn(async move {
            let mut worker = WorkerState::new(cfg.clone(), applied_rx, stop_rx, tx);
            let res = run_worker(&mut worker, &cfg).await;
            if let Err(ref e) = res {
                tracing::error!("replication worker terminated with error: {e}");
            }
            res
        });

        Ok(Self {
            rx,
            applied_tx,
            stop_tx,
            join: Some(join),
        })
    }

    /// Receive the next replication event.
    ///
    /// This method blocks until an event is available or an error occurs.
    /// If the worker task has terminated, this will return the error that
    /// caused the termination.
    ///
    /// # Errors
    ///
    /// - Server errors (e.g., slot dropped, connection lost)
    /// - Protocol errors
    /// - Worker panic
    pub async fn recv(&mut self) -> Result<ReplicationEvent> {
        match self.rx.recv().await {
            Some(Ok(ev)) => Ok(ev),
            Some(Err(e)) => Err(e),
            None => self.handle_worker_shutdown().await,
        }
    }

    /// Handle the case where the worker channel is closed.
    async fn handle_worker_shutdown(&mut self) -> Result<ReplicationEvent> {
        let join = self
            .join
            .take()
            .ok_or_else(|| PgWireError::Internal("replication worker already joined".into()))?;

        match join.await {
            Ok(Ok(())) => Err(PgWireError::Internal(
                "replication worker exited cleanly but channel closed".into(),
            )),
            Ok(Err(e)) => Err(e),
            Err(join_err) => Err(PgWireError::Task(format!(
                "replication worker panicked: {join_err}"
            ))),
        }
    }

    /// Update the applied/committed LSN reported to the server.
    ///
    /// This LSN is sent to PostgreSQL in periodic status updates, allowing
    /// the server to release WAL segments. Call this after successfully
    /// processing and persisting events (e.g., at transaction commit boundaries).
    ///
    /// # Note
    ///
    /// The update is asynchronous - the next status update will include this LSN.
    /// Status updates are sent at the interval configured in `status_interval`.
    #[inline]
    pub fn update_applied_lsn(&self, lsn: Lsn) {
        // Ignore send errors - worker may have stopped
        let _ = self.applied_tx.send(lsn);
    }

    /// Request the worker to stop gracefully.
    ///
    /// After calling this, [`recv()`](Self::recv) will return remaining buffered
    /// events, then an error indicating the stream has ended.
    ///
    /// This sends a CopyDone message to the server to cleanly terminate
    /// the replication stream.
    #[inline]
    pub fn stop(&self) {
        let _ = self.stop_tx.send(true);
    }

    /// Returns `true` if the worker task is still running.
    pub fn is_running(&self) -> bool {
        self.join
            .as_ref()
            .map(|j| !j.is_finished())
            .unwrap_or(false)
    }

    /// Wait for the worker task to complete and return its result.
    ///
    /// This consumes the client. Use this for diagnostics or to ensure
    /// clean shutdown after calling [`stop()`](Self::stop).
    pub async fn join(mut self) -> Result<()> {
        let join = self
            .join
            .take()
            .ok_or_else(|| PgWireError::Task("worker already joined".into()))?;

        match join.await {
            Ok(inner) => inner,
            Err(e) => Err(PgWireError::Task(format!("join error: {e}"))),
        }
    }
}

impl Drop for ReplicationClient {
    fn drop(&mut self) {
        // Signal worker to stop (best-effort cleanup)
        let _ = self.stop_tx.send(true);

        // Abort the worker task if still running
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }
}

/// Run the replication worker on a connection.
async fn run_worker(worker: &mut WorkerState, cfg: &ReplicationConfig) -> Result<()> {
    let tcp = TcpStream::connect((cfg.host.as_str(), cfg.port)).await?;
    tcp.set_nodelay(true)?;

    #[cfg(feature = "tls-rustls")]
    {
        use crate::tls::rustls::{MaybeTlsStream, maybe_upgrade_to_tls};
        let upgraded = maybe_upgrade_to_tls(tcp, &cfg.tls, &cfg.host).await?;
        match upgraded {
            MaybeTlsStream::Plain(mut s) => worker.run_on_stream(&mut s).await,
            MaybeTlsStream::Tls(mut s) => worker.run_on_stream(s.as_mut()).await,
        }
    }

    #[cfg(not(feature = "tls-rustls"))]
    {
        if !matches!(cfg.tls.mode, SslMode::Disable) {
            return Err(PgWireError::Tls("tls-rustls feature not enabled".into()));
        }
        let mut s = tcp;
        worker.run_on_stream(&mut s).await
    }
}
