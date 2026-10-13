use crate::config::ReplicationConfig;
use crate::error::{PgWireError, Result};
use crate::lsn::Lsn;

use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use std::sync::Arc;

#[cfg(not(feature = "tls-rustls"))]
use crate::config::SslMode;

use super::metrics::ReplicationMetrics;
use super::worker::{
    GateCommand, QueryRow, ReplicationEvent, ReplicationEventReceiver, SharedProgress, WorkerState,
};

/// The IDENTIFY_SYSTEM answer of a replication session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentifySystem {
    /// The cluster's system identifier (decimal text).
    pub system_id: String,
    /// The server's current timeline.
    pub timeline: u32,
    /// The current WAL flush location.
    pub xlogpos: Lsn,
    /// The session's database (`None` for a physical session).
    pub dbname: Option<String>,
}

/// The TIMELINE_HISTORY answer: the history file of a timeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimelineHistory {
    /// The history file's name, such as `00000002.history`.
    pub filename: String,
    /// The history file's contents.
    pub content: bytes::Bytes,
}

/// PostgreSQL logical replication client.
///
/// This client spawns a background worker task that maintains the replication
/// connection and streams events to the consumer via a bounded channel.
///
/// # Example
///
/// ```no_run
/// use pgwire_replication::client::{ReplicationClient, ReplicationEvent};
/// use pgwire_replication::config::ReplicationConfig;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let config = ReplicationConfig::new(
///         "localhost",
///         "postgres",
///         "password",
///         "mydb",
///         "my_slot",
///         "my_pub",
///     );
///
///     let mut client = ReplicationClient::connect(config).await?;
///
///     while let Some(ev) = client.recv().await? {
///         match ev {
///             ReplicationEvent::XLogData { data, wal_end, .. } => {
///                 process_change(&data);
///                 client.update_applied_lsn(wal_end);
///             }
///             ReplicationEvent::KeepAlive { .. } => {}
///             ReplicationEvent::StoppedAt { reached } => {
///                 println!("Reached stop LSN: {reached}");
///                 break;
///             }
///             _ => {}
///         }
///     }
///
///     Ok(())
/// }
///
/// fn process_change(_data: &bytes::Bytes) {
///     // user-defined
/// }
/// ```
pub struct ReplicationClient {
    rx: ReplicationEventReceiver,
    progress: Arc<SharedProgress>,
    stop_tx: watch::Sender<bool>,
    metrics: Arc<ReplicationMetrics>,
    join: Option<JoinHandle<std::result::Result<(), PgWireError>>>,
    /// Becomes `true` once the server accepted START_REPLICATION.
    started: watch::Receiver<bool>,
    /// Commands to a gated session before START_REPLICATION. `None`: not
    /// gated, or already started.
    gate: Option<mpsc::Sender<GateCommand>>,
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
    /// - Unix socket does not exist (when host starts with `/`)
    /// - TLS requested with Unix socket connection
    pub async fn connect(cfg: ReplicationConfig) -> Result<Self> {
        Self::spawn(cfg, None)
    }

    /// Connect and authenticate like [`connect`](Self::connect), but do not
    /// start replication: the session waits for
    /// [`simple_query`](Self::simple_query) (and the IDENTIFY_SYSTEM /
    /// TIMELINE_HISTORY helpers) until [`start`](Self::start) sends
    /// START_REPLICATION. Everything runs on the one authenticated session, so
    /// what is read before the start describes the server the stream then
    /// opens on. Dropping the client before `start` ends the session.
    pub async fn connect_gated(cfg: ReplicationConfig) -> Result<Self> {
        let (gate_tx, gate_rx) = mpsc::channel(1);
        let mut client = Self::spawn(cfg, Some(gate_rx))?;
        client.gate = Some(gate_tx);
        Ok(client)
    }

    fn spawn(cfg: ReplicationConfig, gate: Option<mpsc::Receiver<GateCommand>>) -> Result<Self> {
        let (tx, rx) = mpsc::channel(cfg.buffer_events);

        // Progress is shared via atomics: cheap, monotonic, no async backpressure.
        let progress = Arc::new(SharedProgress::with_timing(
            cfg.ack_lsn.unwrap_or(cfg.start_lsn),
            cfg.feedback_observer.is_some(),
        ));

        let (stop_tx, stop_rx) = watch::channel(false);
        let (started_tx, started_rx) = watch::channel(false);

        let metrics = Arc::new(ReplicationMetrics::default());

        let progress_for_worker = Arc::clone(&progress);
        let metrics_for_worker = Arc::clone(&metrics);
        let cfg_for_worker = cfg.clone();

        let join = tokio::spawn(async move {
            let mut worker = WorkerState::new(
                cfg_for_worker,
                progress_for_worker,
                stop_rx,
                tx,
                metrics_for_worker,
            );
            worker.notify_started(started_tx);
            if let Some(gate) = gate {
                worker.gate_before_start(gate);
            }
            let res = run_worker(&mut worker, &cfg).await;
            if let Err(ref e) = res {
                tracing::error!("replication worker terminated with error: {e}");
            }
            res
        });

        Ok(Self {
            rx,
            progress,
            stop_tx,
            metrics,
            join: Some(join),
            started: started_rx,
            gate: None,
        })
    }

    /// Run `sql` with the simple query protocol on a gated session before
    /// [`start`](Self::start); returns its rows (text format). A server error
    /// leaves the session usable.
    pub async fn simple_query(&mut self, sql: &str) -> Result<Vec<QueryRow>> {
        let gate = self.gate.as_ref().ok_or_else(|| {
            PgWireError::Internal("simple_query needs a gated session that has not started".into())
        })?;
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let sent = gate
            .send(GateCommand::Query {
                sql: sql.to_string(),
                reply: reply_tx,
            })
            .await;
        if sent.is_ok() {
            if let Ok(result) = reply_rx.await {
                return result;
            }
        }
        // The worker ended (connect or authentication failed, or the session
        // broke): report why.
        self.gate = None;
        match self.handle_worker_shutdown().await {
            Err(e) => Err(e),
            Ok(_) => Err(PgWireError::Internal(
                "replication session ended before the query ran".into(),
            )),
        }
    }

    /// IDENTIFY_SYSTEM on a gated session.
    pub async fn identify_system(&mut self) -> Result<IdentifySystem> {
        let rows = self.simple_query("IDENTIFY_SYSTEM").await?;
        let row = single_row(rows, 4, "IDENTIFY_SYSTEM")?;
        let text = |i: usize| -> Result<String> {
            row[i]
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .ok_or_else(|| {
                    PgWireError::Protocol(format!("IDENTIFY_SYSTEM: column {i} is NULL"))
                })
        };
        let timeline = text(1)?.parse::<u32>().map_err(|e| {
            PgWireError::Protocol(format!("IDENTIFY_SYSTEM: invalid timeline: {e}"))
        })?;
        let xlogpos = Lsn::parse(&text(2)?)
            .map_err(|e| PgWireError::Protocol(format!("IDENTIFY_SYSTEM: invalid xlogpos: {e}")))?;
        Ok(IdentifySystem {
            system_id: text(0)?,
            timeline,
            xlogpos,
            dbname: text(3).ok(),
        })
    }

    /// TIMELINE_HISTORY for `timeline` on a gated session (the server has no
    /// history file for timeline 1).
    pub async fn timeline_history(&mut self, timeline: u32) -> Result<TimelineHistory> {
        let rows = self
            .simple_query(&format!("TIMELINE_HISTORY {timeline}"))
            .await?;
        let mut row = single_row(rows, 2, "TIMELINE_HISTORY")?;
        let null =
            |i: usize| PgWireError::Protocol(format!("TIMELINE_HISTORY: column {i} is NULL"));
        let content = row[1].take().ok_or_else(|| null(1))?;
        let filename = row[0].take().ok_or_else(|| null(0))?;
        Ok(TimelineHistory {
            filename: String::from_utf8_lossy(&filename).into_owned(),
            content,
        })
    }

    /// Send START_REPLICATION on a gated session; then
    /// [`wait_started`](Self::wait_started) reports whether the server
    /// accepted it.
    pub async fn start(&mut self) -> Result<()> {
        let gate = self.gate.take().ok_or_else(|| {
            PgWireError::Internal("start needs a gated session that has not started".into())
        })?;
        // A closed gate means the worker already ended; wait_started reports
        // its error.
        let _ = gate.send(GateCommand::Start).await;
        Ok(())
    }

    /// Wait until the server has accepted START_REPLICATION: from then on the
    /// replication slot is held by this session, and no other session can
    /// acquire or advance it. [`connect`](Self::connect) returns before that,
    /// as connecting, authentication and START_REPLICATION run in the
    /// background worker. Returns the worker's error if it ended first.
    pub async fn wait_started(&mut self) -> Result<()> {
        if self.started.wait_for(|started| *started).await.is_ok() {
            return Ok(());
        }
        match self.handle_worker_shutdown().await {
            Err(e) => Err(e),
            Ok(_) => Err(PgWireError::Internal(
                "replication worker ended before replication started".into(),
            )),
        }
    }

    /// Receive the next replication event.
    ///
    /// - `Ok(Some(event))` => received an event
    /// - `Ok(None)`        => replication ended normally (stop requested or stop_at_lsn reached)
    /// - `Err(e)`          => replication ended abnormally
    pub async fn recv(&mut self) -> Result<Option<ReplicationEvent>> {
        match self.rx.recv().await {
            Some(Ok(ev)) => Ok(Some(ev)),
            Some(Err(e)) => Err(e),
            None => self.handle_worker_shutdown().await,
        }
    }

    async fn handle_worker_shutdown(&mut self) -> Result<Option<ReplicationEvent>> {
        let join = self
            .join
            .take()
            .ok_or_else(|| PgWireError::Internal("replication worker already joined".into()))?;

        match join.await {
            Ok(Ok(())) => Ok(None),
            Ok(Err(e)) => Err(e),
            Err(join_err) => Err(PgWireError::Task(format!(
                "replication worker panicked: {join_err}"
            ))),
        }
    }

    /// Update the applied/durable LSN reported to the server.
    ///
    /// Semantics: call this only once you have durably persisted all events up to `lsn`.
    /// This update is monotonic and cheap. A raised LSN wakes the worker, which sends it
    /// once `status_interval` has passed since its last standby status update, also on an
    /// idle stream; updates in between coalesce into one status carrying the newest LSN.
    #[inline]
    pub fn update_applied_lsn(&self, lsn: Lsn) {
        self.progress.update_applied(lsn);
    }

    /// When the oldest applied-position raise not yet reported to the server
    /// happened, with a feedback observer configured; `None` otherwise or
    /// when everything raised was reported. Readable after the session ended,
    /// so a caller replacing a failed session can carry it over with
    /// [`carry_feedback_pending`](Self::carry_feedback_pending).
    pub fn feedback_pending_since(&self) -> Option<std::time::Instant> {
        self.progress.pending_since()
    }

    /// Carry over a pending start from a session this one replaces (from its
    /// [`feedback_pending_since`](Self::feedback_pending_since)): the next
    /// update that advances the reported position reports its wait from
    /// there, or from an older start of this session. No effect without a
    /// feedback observer.
    pub fn carry_feedback_pending(&self, since: std::time::Instant) {
        self.progress.carry_pending(since);
    }

    /// Returns a handle to the live replication metrics.
    ///
    /// The returned `Arc` shares the same counters the background worker
    /// updates, so reads reflect current progress. Cheap to clone and call
    /// repeatedly; nothing here blocks the worker.
    #[inline]
    pub fn metrics(&self) -> Arc<ReplicationMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Request the worker to stop gracefully.
    ///
    /// After calling this, [`recv()`](Self::recv) will return remaining buffered
    /// events, then `Ok(None)` once the worker exits cleanly.
    ///
    /// This sends a CopyDone message to the server to cleanly terminate
    /// the replication stream.
    #[inline]
    pub fn stop(&self) {
        let _ = self.stop_tx.send(true);
    }

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

    /// Abort the worker task immediately.
    ///
    /// This is a hard cancel and does not send CopyDone.
    /// Prefer `stop()`/`shutdown()` for graceful termination.
    pub fn abort(&mut self) {
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }

    /// Request a graceful stop and wait for the worker to exit.
    pub async fn shutdown(&mut self) -> Result<()> {
        self.stop();

        // Drain events until the worker closes the channel.
        while let Some(msg) = self.rx.recv().await {
            match msg {
                Ok(_ev) => {} //discard; caller can drain themselves if they need events
                Err(e) => return Err(e),
            }
        }

        self.join_mut().await
    }

    /// Wait for the worker task to complete and return its result.
    async fn join_mut(&mut self) -> Result<()> {
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
        let _ = self.stop_tx.send(true);

        // We cannot .await here. Prefer to detach a join in the background
        // so the worker can exit cleanly without being aborted.
        if let Some(join) = self.join.take() {
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    handle.spawn(async move {
                        let _ = join.await;
                    });
                }
                Err(_) => {
                    // No Tokio runtime available (dropping outside async context).
                    // Fall back to abort to avoid a potentially unbounded leaked task.
                    tracing::debug!(
                        "dropping ReplicationClient outside a Tokio runtime; aborting worker task"
                    );
                    join.abort();
                }
            }
        }
    }
}

/// The one row of a replication command's answer, with `columns` columns.
fn single_row(rows: Vec<QueryRow>, columns: usize, command: &str) -> Result<QueryRow> {
    let mut rows = rows.into_iter();
    match (rows.next(), rows.next()) {
        (Some(row), None) if row.len() >= columns => Ok(row),
        _ => Err(PgWireError::Protocol(format!(
            "{command}: expected one row of {columns} columns"
        ))),
    }
}

async fn run_worker(worker: &mut WorkerState, cfg: &ReplicationConfig) -> Result<()> {
    #[cfg(unix)]
    if cfg.is_unix_socket() {
        if cfg.tls.mode.requires_tls() {
            return Err(PgWireError::Tls(
                "TLS is not supported over Unix domain sockets".into(),
            ));
        }

        let path = cfg.unix_socket_path();
        let mut stream = UnixStream::connect(&path).await.map_err(|e| {
            PgWireError::Io(std::sync::Arc::new(std::io::Error::new(
                e.kind(),
                format!("failed to connect to Unix socket {}: {e}", path.display()),
            )))
        })?;

        return worker.run_on_stream(&mut stream).await;
    }

    let tcp = TcpStream::connect((cfg.host.as_str(), cfg.port)).await?;
    tcp.set_nodelay(true)?;

    #[cfg(feature = "tls-rustls")]
    {
        use crate::tls::rustls::{maybe_upgrade_to_tls, MaybeTlsStream};
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
