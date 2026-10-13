use bytes::Bytes;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, BufReader};
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use super::metrics::ReplicationMetrics;
use super::progress::ProgressCell;
use crate::config::ReplicationConfig;
use crate::error::{PgWireError, Result};
use crate::lsn::Lsn;
use crate::protocol::framing::{
    read_backend_message, write_copy_data, write_copy_done, write_password_message, write_query,
    write_startup_message, MessageReader,
};
use crate::protocol::messages::{parse_auth_request, parse_error_response};
use crate::protocol::replication::{
    encode_standby_status_update, parse_copy_data, ReplicationCopyData, PG_EPOCH_MICROS,
};

/// Shared replication progress updated by the consumer and read by the worker.
///
/// The applied LSN lives in a lock-free [`ProgressCell`] so progress updates
/// are cheap and monotonic without async backpressure. A raise also wakes an
/// idle worker, which sends it promptly instead of at its next wakeup. Only the
/// first raise after the worker last looked bumps the watch generation; later
/// ones coalesce into it and cost one more atomic load.
pub struct SharedProgress {
    cell: ProgressCell,
    changed: watch::Sender<u64>,
}

/// Clock origin for pending starts (nanoseconds after it, plus one), shared
/// by every session so a start carried from an earlier one stays comparable.
/// tokio's clock: the system clock, paused under test. Set an hour back so a
/// clock read on another (paused) runtime never falls before it.
fn clock_origin() -> Instant {
    static ORIGIN: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    *ORIGIN.get_or_init(|| {
        let now = Instant::now();
        now.checked_sub(std::time::Duration::from_secs(3600))
            .unwrap_or(now)
    })
}

impl SharedProgress {
    #[cfg(test)]
    pub fn new(start: Lsn) -> Self {
        Self::with_timing(start, false)
    }

    /// `timed`: keep when the oldest unreported raise happened, for a
    /// feedback observer.
    pub fn with_timing(start: Lsn, timed: bool) -> Self {
        Self {
            cell: ProgressCell::new(start.as_u64(), timed),
            changed: watch::Sender::new(0),
        }
    }

    fn clock(&self) -> u64 {
        clock_origin().elapsed().as_nanos() as u64 + 1
    }

    fn instant(&self, clock: u64) -> Instant {
        clock_origin() + std::time::Duration::from_nanos(clock - 1)
    }

    /// When the oldest raise not yet reported to the server happened.
    pub fn pending_since(&self) -> Option<std::time::Instant> {
        match self.cell.pending_since() {
            0 => None,
            clock => Some(self.instant(clock).into_std()),
        }
    }

    /// Carry over the pending start of an earlier session (the older wins).
    pub fn carry_pending(&self, since: std::time::Instant) {
        let since = Instant::from_std(since);
        let clock = since.saturating_duration_since(clock_origin()).as_nanos() as u64 + 1;
        self.cell.carry_pending(clock);
    }

    #[inline]
    pub fn load_applied(&self) -> Lsn {
        Lsn::from_u64(self.cell.load())
    }

    /// Monotonic update: if `lsn` is lower than the currently stored applied LSN,
    /// this is a no-op.
    #[inline]
    pub fn update_applied(&self, lsn: Lsn) {
        if self.cell.raise(lsn.as_u64(), || self.clock()) {
            self.changed.send_modify(|g| *g = g.wrapping_add(1));
        }
    }

    /// Wakes when the applied LSN is raised.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    /// The worker is about to read the applied LSN: the next raise wakes it
    /// again. Returns the applied LSN read after clearing the signal.
    fn take_signal(&self) -> Lsn {
        Lsn::from_u64(self.cell.take_signal())
    }
}

/// Events emitted by the replication worker.
#[derive(Debug, Clone)]
pub enum ReplicationEvent {
    /// Server heartbeat message.
    KeepAlive {
        /// Current server WAL end position
        wal_end: Lsn,
        /// Whether server requested a reply (already handled internally)
        reply_requested: bool,
        /// Server timestamp (microseconds since 2000-01-01)
        server_time_micros: i64,
    },

    /// Start of a transaction (pgoutput Begin message).
    Begin {
        final_lsn: Lsn,
        xid: u32,
        commit_time_micros: i64,
    },

    /// WAL data containing transaction changes.
    XLogData {
        /// WAL position where this data starts
        wal_start: Lsn,
        /// WAL end position (may be 0 for mid-transaction messages)
        wal_end: Lsn,
        /// Server timestamp (microseconds since 2000-01-01)
        server_time_micros: i64,
        /// pgoutput-encoded change data
        data: Bytes,
    },

    /// End of a transaction (pgoutput Commit message).
    Commit {
        lsn: Lsn,
        end_lsn: Lsn,
        commit_time_micros: i64,
    },

    /// Logical decoding message emitted via `pg_logical_emit_message()`.
    ///
    /// Transactional messages are delivered only after the enclosing
    /// transaction commits. Non-transactional messages are delivered
    /// immediately.
    Message {
        /// Whether the message was emitted inside a transaction.
        transactional: bool,
        /// LSN of the message in the WAL.
        lsn: Lsn,
        /// Application-defined message prefix (e.g. `"myapp.checkpoint"`).
        prefix: String,
        /// Raw message content bytes.
        content: Bytes,
    },

    /// Emitted when `stop_at_lsn` has been reached.
    ///
    /// After this event, no more events will be emitted and the
    /// replication stream will be closed.
    StoppedAt {
        /// The LSN that triggered the stop condition
        reached: Lsn,
    },
}

/// Channel receiver type for replication events.
pub type ReplicationEventReceiver =
    mpsc::Receiver<std::result::Result<ReplicationEvent, PgWireError>>;

/// Internal worker state.
pub struct WorkerState {
    cfg: ReplicationConfig,
    progress: Arc<SharedProgress>,
    stop_rx: watch::Receiver<bool>,
    out: mpsc::Sender<std::result::Result<ReplicationEvent, PgWireError>>,
    metrics: Arc<ReplicationMetrics>,
    /// Notified once the server accepted START_REPLICATION.
    started: Option<tokio::sync::oneshot::Sender<()>>,
    /// The applied LSN in the last status update written to the server, if any.
    last_sent: Option<Lsn>,
}

impl WorkerState {
    pub fn new(
        cfg: ReplicationConfig,
        progress: Arc<SharedProgress>,
        stop_rx: watch::Receiver<bool>,
        out: mpsc::Sender<std::result::Result<ReplicationEvent, PgWireError>>,
        metrics: Arc<ReplicationMetrics>,
    ) -> Self {
        Self {
            cfg,
            progress,
            stop_rx,
            out,
            metrics,
            started: None,
            last_sent: None,
        }
    }

    /// Notify `started` once the server accepts START_REPLICATION.
    pub(crate) fn notify_started(&mut self, started: tokio::sync::oneshot::Sender<()>) {
        self.started = Some(started);
    }

    /// Run the replication protocol on the given stream.
    pub async fn run_on_stream<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
    ) -> Result<()> {
        // Wrap in a 128KB read buffer to batch multiple WAL messages into fewer
        // recv() syscalls. BufReader delegates AsyncWrite to the inner stream,
        // so writes (standby status replies, etc.) are unaffected.
        let mut stream = BufReader::with_capacity(128 * 1024, stream);
        self.startup(&mut stream).await?;
        self.authenticate(&mut stream).await?;
        self.start_replication(&mut stream).await?;
        if let Some(started) = self.started.take() {
            let _ = started.send(());
        }
        self.stream_loop(&mut stream).await
    }

    /// Send startup message with replication parameters.
    async fn startup<S: AsyncWrite + Unpin>(&self, stream: &mut S) -> Result<()> {
        let mut params = vec![
            ("user", self.cfg.user.as_str()),
            ("database", self.cfg.database.as_str()),
            ("replication", "database"),
            ("client_encoding", "UTF8"),
            ("application_name", "pgwire-replication"),
        ];
        // PostgreSQL honors `options` on replication connections the same as on
        // ordinary ones, so this lets callers pin session GUCs (e.g. `DateStyle`,
        // `IntervalStyle`) the way they already can via a normal libpq `options`
        // parameter — only sent when the caller actually set one.
        if let Some(options) = self.cfg.options.as_deref() {
            params.push(("options", options));
        }
        write_startup_message(stream, 196608, &params).await
    }

    /// Start the logical replication stream.
    async fn start_replication<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
    ) -> Result<()> {
        let binary_opt = if self.cfg.binary {
            ", binary 'true'"
        } else {
            ""
        };
        let sql = format!(
            "START_REPLICATION SLOT {} LOGICAL {} \
            (proto_version '1', publication_names '{}', messages 'true'{})",
            self.cfg.slot,
            self.cfg.start_lsn,
            self.cfg.publication.to_option_value(),
            binary_opt,
        );
        write_query(stream, &sql).await?;

        // Wait for CopyBothResponse
        loop {
            let msg = read_backend_message(stream).await?;
            match msg.tag {
                b'W' => return Ok(()), // CopyBothResponse - ready to stream
                b'E' => return Err(PgWireError::Server(parse_error_response(&msg.payload))),
                b'N' | b'S' | b'K' => continue, // Notice, ParameterStatus, BackendKeyData
                _ => continue,
            }
        }
    }

    /// Main replication streaming loop.
    ///
    /// Uses a two-phase approach for throughput:
    /// 1. **Drain phase**: while the BufReader has buffered data, read messages
    ///    in a tight loop without `select!` or timeout overhead.
    /// 2. **Wait phase**: when the buffer is empty, fall back to `select!` with
    ///    timeout + stop signal to handle idle keepalives and graceful shutdown.
    ///
    /// Reads use [`MessageReader`], which preserves partial-read state across
    /// dropped futures so the wait-phase `select!` is cancellation-safe.
    async fn stream_loop<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut BufReader<S>,
    ) -> Result<()> {
        let mut last_status_sent = Instant::now() - self.cfg.status_interval;
        let mut last_applied = self.progress.load_applied();
        // Wakes the wait phase when the consumer raises the applied LSN.
        let mut applied_rx = self.progress.subscribe();
        // Cancellation-safe message reader, partial reads survive dropped futures.
        let mut reader = MessageReader::new();
        // How many messages to process in the tight loop before checking
        // stop signal and sending periodic status feedback.
        const DRAIN_BATCH: usize = 256;

        loop {
            // Update applied LSN from client
            let current_applied = self.progress.load_applied();
            if current_applied != last_applied {
                last_applied = current_applied;
            }

            // Send periodic status feedback
            if last_status_sent.elapsed() >= self.cfg.status_interval {
                self.send_feedback(stream, last_applied, false).await?;
                last_status_sent = Instant::now();
            }

            // ── Drain phase: tight loop while BufReader has buffered data ──
            // The BufReader has a 128KB internal buffer. When the kernel delivers
            // a large TCP segment, many WAL messages are available without syscalls.
            // Read them in a tight loop to avoid select!/timeout overhead per message.
            let mut drained = 0usize;
            while stream.buffer().len() >= 5 && drained < DRAIN_BATCH {
                let msg = reader.read(stream).await?;
                drained += 1;
                if msg.tag == b'E' {
                    return Err(PgWireError::Server(parse_error_response(&msg.payload)));
                }
                if msg.tag == b'd'
                    && self
                        .handle_copy_data(
                            stream,
                            msg.payload,
                            &mut last_applied,
                            &mut last_status_sent,
                        )
                        .await?
                {
                    return Ok(());
                }
            }

            // If we drained messages, loop back to check stop/status before
            // potentially blocking on the next read.
            if drained > 0 {
                // Check stop signal without blocking
                if self.stop_rx.has_changed().unwrap_or(false) && *self.stop_rx.borrow() {
                    let _ = write_copy_done(stream).await;
                    return Ok(());
                }
                continue;
            }

            // ── Wait phase: buffer empty, need to wait for socket data ──
            //
            // Both `stop_rx.changed()` and the timeout can drop the read future
            // mid-message. `MessageReader::read` is cancellation-safe — partial
            // header/payload state lives on `reader` and is preserved across the
            // drop, so the next iteration resumes the read without losing bytes.
            //
            // An applied LSN newer than the last one sent is pending: it is sent
            // once `status_interval` has passed since the last status update.
            // Otherwise a raise wakes the loop. Marking the watch seen and
            // clearing the signal before reading the LSN means a raise after
            // this point still wakes it.
            applied_rx.mark_unchanged();
            let applied = self.progress.take_signal();
            let pending = self.last_sent.is_none_or(|sent| applied > sent);
            let feedback_due = last_status_sent + self.cfg.status_interval;
            let msg = tokio::select! {
                biased;

                _ = self.stop_rx.changed() => {
                    if *self.stop_rx.borrow() {
                        let _ = write_copy_done(stream).await;
                        return Ok(());
                    }
                    continue;
                }

                _ = tokio::time::sleep_until(feedback_due), if pending => {
                    let applied = self.progress.load_applied();
                    last_applied = applied;
                    self.send_feedback(stream, applied, false).await?;
                    last_status_sent = Instant::now();
                    continue;
                }

                // The sender lives in `self.progress`, so this never closes.
                _ = applied_rx.changed(), if !pending => continue,

                msg_result = tokio::time::timeout(
                    self.cfg.idle_wakeup_interval,
                    reader.read(stream),
                ) => {
                    match msg_result {
                        Ok(res) => res?,
                        Err(_) => {
                            let applied = self.progress.load_applied();
                            last_applied = applied;
                            self.send_feedback(stream, applied, false).await?;
                            last_status_sent = Instant::now();
                            continue;
                        }
                    }
                }
            };

            if msg.tag == b'E' {
                return Err(PgWireError::Server(parse_error_response(&msg.payload)));
            }
            if msg.tag == b'd'
                && self
                    .handle_copy_data(
                        stream,
                        msg.payload,
                        &mut last_applied,
                        &mut last_status_sent,
                    )
                    .await?
            {
                return Ok(());
            }
        }
    }

    /// Handle a CopyData message. Returns true if we should stop.
    async fn handle_copy_data<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut BufReader<S>,
        payload: Bytes,
        last_applied: &mut Lsn,
        last_status_sent: &mut Instant,
    ) -> Result<bool> {
        let cd = parse_copy_data(payload)?;

        match cd {
            ReplicationCopyData::KeepAlive {
                wal_end,
                server_time_micros,
                reply_requested,
            } => {
                self.metrics.set_last_wal_end(wal_end);

                // Respond immediately if server requests it
                if reply_requested {
                    let applied = self.progress.load_applied();
                    *last_applied = applied;
                    self.send_feedback(stream, applied, true).await?;
                    *last_status_sent = Instant::now();
                    self.metrics.record_keepalive_reply();
                }

                if self
                    .send_event_backpressured(
                        stream,
                        Ok(ReplicationEvent::KeepAlive {
                            wal_end,
                            reply_requested,
                            server_time_micros,
                        }),
                        last_status_sent,
                    )
                    .await?
                {
                    return Ok(true);
                }

                Ok(false)
            }
            ReplicationCopyData::XLogData {
                wal_start,
                wal_end,
                server_time_micros,
                data,
            } => {
                self.metrics.set_last_wal_end(wal_end);

                // If the payload is a pgoutput Begin/Commit message, emit only the boundary event.
                if let Some(boundary_ev) = parse_pgoutput_boundary(&data)? {
                    let reached_lsn = match boundary_ev {
                        ReplicationEvent::Begin { final_lsn, .. } => final_lsn,
                        ReplicationEvent::Commit { end_lsn, .. } => end_lsn,
                        _ => wal_end, // should never happen if parser only returns Begin/Commit
                    };

                    if self
                        .send_event_backpressured(stream, Ok(boundary_ev), last_status_sent)
                        .await?
                    {
                        return Ok(true);
                    }

                    // Stop condition (prefer boundary LSN semantics when available)
                    if let Some(stop_lsn) = self.cfg.stop_at_lsn {
                        if reached_lsn >= stop_lsn {
                            if self
                                .send_event_backpressured(
                                    stream,
                                    Ok(ReplicationEvent::StoppedAt {
                                        reached: reached_lsn,
                                    }),
                                    last_status_sent,
                                )
                                .await?
                            {
                                return Ok(true);
                            }
                            let _ = write_copy_done(stream).await;
                            return Ok(true); // should stop.
                        }
                    }

                    return Ok(false);
                }
                // Otherwise, emit raw payload
                // Check stop condition
                if let Some(stop_lsn) = self.cfg.stop_at_lsn {
                    if wal_end >= stop_lsn {
                        // Send final event, then stop signal
                        if self
                            .send_event_backpressured(
                                stream,
                                Ok(ReplicationEvent::XLogData {
                                    wal_start,
                                    wal_end,
                                    server_time_micros,
                                    data,
                                }),
                                last_status_sent,
                            )
                            .await?
                        {
                            return Ok(true);
                        }

                        if self
                            .send_event_backpressured(
                                stream,
                                Ok(ReplicationEvent::StoppedAt { reached: wal_end }),
                                last_status_sent,
                            )
                            .await?
                        {
                            return Ok(true);
                        }

                        let _ = write_copy_done(stream).await;
                        return Ok(true);
                    }
                }

                if self
                    .send_event_backpressured(
                        stream,
                        Ok(ReplicationEvent::XLogData {
                            wal_start,
                            wal_end,
                            server_time_micros,
                            data,
                        }),
                        last_status_sent,
                    )
                    .await?
                {
                    return Ok(true);
                }

                Ok(false)
            }
        }
    }

    /// Forward an event to the consumer, keeping standby feedback alive under
    /// backpressure.
    ///
    /// Fast path: if the channel has capacity, send immediately — no await, no
    /// stall. When the channel is full we must **not** block silently on
    /// `send().await`: that would park the worker and starve standby-status
    /// feedback until `wal_sender_timeout` fires and the server resets the
    /// connection. Instead we wait for capacity while continuing to emit
    /// feedback every `status_interval`, so the server keeps seeing us alive.
    /// This does not falsely acknowledge data — feedback reports the unchanged
    /// applied LSN — and real backpressure still propagates via TCP flow
    /// control (we stop reading the socket while parked here).
    ///
    /// Returns `Ok(true)` if a stop was requested while waiting (the caller
    /// should unwind), or `Ok(false)` once the event is delivered or the
    /// channel is closed.
    async fn send_event_backpressured<S: AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
        event: std::result::Result<ReplicationEvent, PgWireError>,
        last_status_sent: &mut Instant,
    ) -> Result<bool> {
        // Fast path: capacity available.
        match self.out.try_reserve() {
            Ok(permit) => {
                permit.send(event);
                self.metrics.record_event_forwarded();
                return Ok(false);
            }
            Err(mpsc::error::TrySendError::Closed(())) => {
                tracing::debug!("event channel closed, client may have disconnected");
                return Ok(false);
            }
            // Channel full: fall through to the backpressure wait loop. `event`
            // is untouched (try_reserve carries no value), so we still own it.
            Err(mpsc::error::TrySendError::Full(())) => {}
        }

        // Counted here, at the start, so an ongoing stall is visible even while
        // the worker is still parked below awaiting capacity.
        self.metrics.record_stall_begin();
        let stall_start = Instant::now();
        loop {
            let deadline = *last_status_sent + self.cfg.status_interval;
            tokio::select! {
                biased;

                _ = self.stop_rx.changed() => {
                    if *self.stop_rx.borrow() {
                        let _ = write_copy_done(stream).await;
                        return Ok(true);
                    }
                    continue;
                }

                permit = self.out.reserve() => {
                    self.metrics
                        .record_stall_end(stall_start.elapsed().as_micros() as u64);
                    match permit {
                        Ok(permit) => {
                            permit.send(event);
                            self.metrics.record_event_forwarded();
                        }
                        Err(_closed) => {
                            tracing::debug!("event channel closed while backpressured");
                        }
                    }
                    return Ok(false);
                }

                _ = tokio::time::sleep_until(deadline) => {}
            }
            // The status interval elapsed while parked.
            let applied = self.progress.load_applied();
            self.send_feedback(stream, applied, false).await?;
            *last_status_sent = Instant::now();
        }
    }

    /// Handle PostgreSQL authentication exchange.
    async fn authenticate<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
    ) -> Result<()> {
        loop {
            let msg = read_backend_message(stream).await?;
            match msg.tag {
                b'R' => {
                    let (code, rest) = parse_auth_request(&msg.payload)?;
                    self.handle_auth_request(stream, code, rest).await?;
                }
                b'E' => return Err(PgWireError::Server(parse_error_response(&msg.payload))),
                b'S' | b'K' => {}      // ParameterStatus, BackendKeyData - ignore
                b'Z' => return Ok(()), // ReadyForQuery - auth complete
                _ => {}
            }
        }
    }

    /// Handle a specific authentication request.
    async fn handle_auth_request<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
        code: i32,
        data: &[u8],
    ) -> Result<()> {
        match code {
            0 => Ok(()), // AuthenticationOk
            3 => {
                // Cleartext password
                let mut payload = Vec::from(self.cfg.password.as_bytes());
                payload.push(0);
                write_password_message(stream, &payload).await
            }
            10 => {
                // SASL (SCRAM-SHA-256)
                self.auth_scram(stream, data).await
            }
            #[cfg(feature = "md5")]
            5 => {
                // MD5 password
                if data.len() != 4 {
                    return Err(PgWireError::Protocol(
                        "MD5 auth: expected 4-byte salt".into(),
                    ));
                }
                let mut salt = [0u8; 4];
                salt.copy_from_slice(&data[..4]);

                let hash = postgres_md5(&self.cfg.password, &self.cfg.user, &salt);
                let mut payload = hash.into_bytes();
                payload.push(0);
                write_password_message(stream, &payload).await
            }
            _ => Err(PgWireError::Auth(format!(
                "unsupported auth method code: {code}"
            ))),
        }
    }

    /// Perform SCRAM-SHA-256 authentication.
    async fn auth_scram<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
        mechanisms_data: &[u8],
    ) -> Result<()> {
        // Parse offered mechanisms
        let mechanisms = parse_sasl_mechanisms(mechanisms_data);

        if !mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
            return Err(PgWireError::Auth(format!(
                "server doesn't offer SCRAM-SHA-256, available: {mechanisms:?}"
            )));
        }

        #[cfg(not(feature = "scram"))]
        return Err(PgWireError::Auth(
            "SCRAM authentication required but 'scram' feature not enabled".into(),
        ));

        #[cfg(feature = "scram")]
        {
            use crate::auth::scram::ScramClient;

            let scram = ScramClient::new(&self.cfg.user);

            // Send SASLInitialResponse
            let mut init = Vec::new();
            init.extend_from_slice(b"SCRAM-SHA-256\0");
            init.extend_from_slice(&(scram.client_first.len() as i32).to_be_bytes());
            init.extend_from_slice(scram.client_first.as_bytes());
            write_password_message(stream, &init).await?;

            // Receive AuthenticationSASLContinue (code 11)
            let server_first = read_auth_data(stream, 11).await?;
            let server_first_str = String::from_utf8_lossy(&server_first);

            // Compute and send client-final
            let (client_final, auth_message, salted_password) =
                scram.client_final(&self.cfg.password, &server_first_str)?;
            write_password_message(stream, client_final.as_bytes()).await?;

            // Receive and verify AuthenticationSASLFinal (code 12)
            let server_final = read_auth_data(stream, 12).await?;
            let server_final_str = String::from_utf8_lossy(&server_final);
            ScramClient::verify_server_final(&server_final_str, &salted_password, &auth_message)?;

            Ok(())
        }
    }

    /// Send standby status update to server.
    ///
    /// The position is recorded as sent only once the write succeeded, so a
    /// failed send leaves it pending.
    async fn send_feedback<S: AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
        applied: Lsn,
        reply_requested: bool,
    ) -> Result<()> {
        // Timed only when this write advances the reported position and
        // someone observes it.
        let loaded_at = (self.cfg.feedback_observer.is_some()
            && self.last_sent.is_none_or(|sent| applied > sent))
        .then(|| self.progress.clock());
        let client_time = current_pg_timestamp();
        let payload = encode_standby_status_update(applied, client_time, reply_requested);
        write_copy_data(stream, &payload).await?;
        self.last_sent = Some(applied);
        self.metrics.record_feedback_sent(applied);
        if let (Some(at), Some(observer)) = (loaded_at, &self.cfg.feedback_observer) {
            let since = self.progress.cell.take_pending(applied.as_u64(), at);
            if since != 0 {
                observer.observe(crate::FeedbackSent {
                    pending_for: self.progress.instant(since).elapsed(),
                });
            }
        }
        Ok(())
    }
}

/// Parse SASL mechanism list from auth data.
fn parse_sasl_mechanisms(data: &[u8]) -> Vec<String> {
    let mut mechanisms = Vec::new();
    let mut remaining = data;

    while !remaining.is_empty() {
        if let Some(pos) = remaining.iter().position(|&x| x == 0) {
            if pos == 0 {
                break; // Empty string terminates list
            }
            mechanisms.push(String::from_utf8_lossy(&remaining[..pos]).to_string());
            remaining = &remaining[pos + 1..];
        } else {
            break;
        }
    }

    mechanisms
}

fn parse_pgoutput_boundary(data: &Bytes) -> Result<Option<ReplicationEvent>> {
    if data.is_empty() {
        return Ok(None);
    }

    let tag = data[0];
    let mut p = &data[1..];

    fn take_i8(p: &mut &[u8]) -> Result<i8> {
        if p.is_empty() {
            return Err(PgWireError::Protocol("pgoutput: truncated i8".into()));
        }
        let v = p[0] as i8;
        *p = &p[1..];
        Ok(v)
    }

    fn take_i32(p: &mut &[u8]) -> Result<i32> {
        if p.len() < 4 {
            return Err(PgWireError::Protocol("pgoutput: truncated i32".into()));
        }
        let (head, tail) = p.split_at(4);
        *p = tail;
        Ok(i32::from_be_bytes(head.try_into().unwrap()))
    }

    fn take_i64(p: &mut &[u8]) -> Result<i64> {
        if p.len() < 8 {
            return Err(PgWireError::Protocol("pgoutput: truncated i64".into()));
        }
        let (head, tail) = p.split_at(8);
        *p = tail;
        Ok(i64::from_be_bytes(head.try_into().unwrap()))
    }

    match tag {
        b'B' => {
            let final_lsn = Lsn::from_u64(take_i64(&mut p)? as u64);
            let commit_time_micros = take_i64(&mut p)?;
            let xid = take_i32(&mut p)? as u32;

            Ok(Some(ReplicationEvent::Begin {
                final_lsn,
                commit_time_micros,
                xid,
            }))
        }
        b'C' => {
            let _flags = take_i8(&mut p)?;
            let lsn = Lsn::from_u64(take_i64(&mut p)? as u64); // should be safe
            let end_lsn = Lsn::from_u64(take_i64(&mut p)? as u64);
            let commit_time_micros = take_i64(&mut p)?;

            Ok(Some(ReplicationEvent::Commit {
                lsn,
                end_lsn,
                commit_time_micros,
            }))
        }
        b'M' => {
            // Logical decoding message (pg_logical_emit_message)
            // Wire: flags(1) + lsn(8) + prefix(null-terminated) + content_len(4) + content(n)
            let flags = take_i8(&mut p)?;
            let transactional = (flags & 1) != 0;
            let lsn = Lsn::from_u64(take_i64(&mut p)? as u64);

            // Read null-terminated prefix string
            let prefix_end = p.iter().position(|&b| b == 0).ok_or_else(|| {
                PgWireError::Protocol("pgoutput Message: missing null terminator for prefix".into())
            })?;
            let prefix = String::from_utf8_lossy(&p[..prefix_end]).into_owned();
            p = &p[prefix_end + 1..]; // advance past null byte

            let content_len = take_i32(&mut p)? as usize;
            if p.len() < content_len {
                return Err(PgWireError::Protocol(format!(
                    "pgoutput Message: expected {} content bytes, got {}",
                    content_len,
                    p.len()
                )));
            }
            let content = Bytes::copy_from_slice(&p[..content_len]);

            Ok(Some(ReplicationEvent::Message {
                transactional,
                lsn,
                prefix,
                content,
            }))
        }
        _ => Ok(None),
    }
}

/// Read authentication response data for a specific auth code.
async fn read_auth_data<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    expected_code: i32,
) -> Result<Vec<u8>> {
    loop {
        let msg = read_backend_message(stream).await?;
        match msg.tag {
            b'R' => {
                let (code, data) = parse_auth_request(&msg.payload)?;
                if code == expected_code {
                    return Ok(data.to_vec());
                }
                return Err(PgWireError::Auth(format!(
                    "unexpected auth code {code}, expected {expected_code}"
                )));
            }
            b'E' => return Err(PgWireError::Server(parse_error_response(&msg.payload))),
            _ => {} // Skip other messages
        }
    }
}

/// Get current time as PostgreSQL timestamp (microseconds since 2000-01-01).
fn current_pg_timestamp() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    let unix_micros = (now.as_secs() as i64) * 1_000_000 + (now.subsec_micros() as i64);
    unix_micros - PG_EPOCH_MICROS
}

/// Compute PostgreSQL MD5 password hash.
#[cfg(feature = "md5")]
fn postgres_md5(password: &str, user: &str, salt: &[u8; 4]) -> String {
    fn md5_hex(data: &[u8]) -> String {
        format!("{:x}", md5::compute(data))
    }

    // First hash: md5(password + username)
    let inner = md5_hex(format!("{password}{user}").as_bytes());

    // Second hash: md5(inner_hash + salt)
    let mut outer_input = inner.into_bytes();
    outer_input.extend_from_slice(salt);

    format!("md5{}", md5_hex(&outer_input))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sasl_mechanisms_single() {
        let data = b"SCRAM-SHA-256\0\0";
        let mechs = parse_sasl_mechanisms(data);
        assert_eq!(mechs, vec!["SCRAM-SHA-256"]);
    }

    #[test]
    fn parse_sasl_mechanisms_multiple() {
        let data = b"SCRAM-SHA-256\0SCRAM-SHA-256-PLUS\0\0";
        let mechs = parse_sasl_mechanisms(data);
        assert_eq!(mechs, vec!["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"]);
    }

    #[test]
    fn parse_sasl_mechanisms_empty() {
        let mechs = parse_sasl_mechanisms(b"\0");
        assert!(mechs.is_empty());
    }

    #[test]
    #[cfg(feature = "md5")]
    fn postgres_md5_known_value() {
        // Test vector: user="md5_user", password="md5_pass", salt=[0x01, 0x02, 0x03, 0x04]
        // Can verify with: SELECT 'md5' || md5(md5('md5_passmd5_user') || E'\\x01020304');
        let hash = postgres_md5("md5_pass", "md5_user", &[0x01, 0x02, 0x03, 0x04]);
        assert!(hash.starts_with("md5"));
        assert_eq!(hash.len(), 35); // "md5" + 32 hex chars
    }

    #[test]
    fn current_pg_timestamp_is_positive() {
        // Any time after 2000-01-01 should be positive
        let ts = current_pg_timestamp();
        assert!(ts > 0);
    }

    /// Build a CopyData('d') frame wrapping an XLogData('w') replication message
    /// carrying a non-boundary pgoutput payload (so the worker forwards it as a
    /// raw `XLogData` event).
    fn xlog_frame(wal_end: u64, data: &[u8]) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.push(b'w');
        payload.extend_from_slice(&(wal_end as i64).to_be_bytes()); // wal_start
        payload.extend_from_slice(&(wal_end as i64).to_be_bytes()); // wal_end
        payload.extend_from_slice(&0i64.to_be_bytes()); // server_time
        payload.extend_from_slice(data);

        let mut frame = Vec::new();
        frame.push(b'd');
        frame.extend_from_slice(&((payload.len() + 4) as i32).to_be_bytes());
        frame.extend_from_slice(&payload);
        frame
    }

    /// Build a `WorkerState` wired to in-memory channels for unit testing.
    /// Returns the worker plus the stop sender and event receiver (held so the
    /// channels stay open for the duration of the test).
    fn test_worker(
        cfg: ReplicationConfig,
    ) -> (
        WorkerState,
        watch::Sender<bool>,
        mpsc::Receiver<std::result::Result<ReplicationEvent, PgWireError>>,
    ) {
        let progress = Arc::new(SharedProgress::new(cfg.ack_lsn.unwrap_or(cfg.start_lsn)));
        let (stop_tx, stop_rx) = watch::channel(false);
        let (tx, rx) = mpsc::channel(16);
        let metrics = Arc::new(ReplicationMetrics::default());
        let worker = WorkerState::new(cfg, progress, stop_rx, tx, metrics);
        (worker, stop_tx, rx)
    }

    /// The `options` startup parameter is how `libpq`/`PGOPTIONS` lets a caller
    /// pin session GUCs (e.g. `DateStyle`, `IntervalStyle`) on a connection.
    /// PostgreSQL honors it on replication connections too, so it must be
    /// forwarded into the startup message when the caller sets it, and left
    /// out entirely otherwise (server defaults apply).
    #[tokio::test]
    async fn startup_includes_options_when_set() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let cfg = ReplicationConfig::new("h", "u", "p", "db", "slot", "pub")
            .with_options("-c datestyle=ISO,MDY -c intervalstyle=iso_8601");
        let (worker, _stop_tx, _rx) = test_worker(cfg);
        let (mut worker_end, mut server) = tokio::io::duplex(64 * 1024);

        worker.startup(&mut worker_end).await.unwrap();
        worker_end.shutdown().await.unwrap();

        let mut sent = Vec::new();
        server.read_to_end(&mut sent).await.unwrap();
        let sent = String::from_utf8_lossy(&sent);

        assert!(
            sent.contains("options"),
            "startup message missing 'options' key: {sent:?}"
        );
        assert!(
            sent.contains("-c datestyle=ISO,MDY -c intervalstyle=iso_8601"),
            "startup message missing options value: {sent:?}"
        );
    }

    #[tokio::test]
    async fn startup_omits_options_when_unset() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (worker, _stop_tx, _rx) = test_worker(ReplicationConfig::default());
        let (mut worker_end, mut server) = tokio::io::duplex(64 * 1024);

        worker.startup(&mut worker_end).await.unwrap();
        worker_end.shutdown().await.unwrap();

        let mut sent = Vec::new();
        server.read_to_end(&mut sent).await.unwrap();
        let sent = String::from_utf8_lossy(&sent);

        assert!(
            !sent.contains("options"),
            "startup message should omit 'options' when unset: {sent:?}"
        );
    }

    #[tokio::test]
    async fn start_replication_returns_ok_on_copy_both_response() {
        use std::time::Duration;
        use tokio::io::AsyncWriteExt;

        let (worker, _stop_tx, _rx) = test_worker(ReplicationConfig::default());
        let (mut worker_end, mut server) = tokio::io::duplex(64 * 1024);

        // Server accepts the stream: CopyBothResponse ('W', empty payload).
        server.write_all(&[b'W', 0, 0, 0, 4]).await.unwrap();

        let res = tokio::time::timeout(
            Duration::from_secs(5),
            worker.start_replication(&mut worker_end),
        )
        .await;
        assert!(matches!(res, Ok(Ok(()))), "expected Ok on 'W', got {res:?}");
    }

    #[tokio::test]
    async fn start_replication_surfaces_server_error_response() {
        use std::time::Duration;
        use tokio::io::AsyncWriteExt;

        let (worker, _stop_tx, _rx) = test_worker(ReplicationConfig::default());
        let (mut worker_end, mut server) = tokio::io::duplex(64 * 1024);

        // Server rejects the stream: ErrorResponse ('E') with an empty field list.
        server.write_all(&[b'E', 0, 0, 0, 5, 0]).await.unwrap();

        let res = tokio::time::timeout(
            Duration::from_secs(5),
            worker.start_replication(&mut worker_end),
        )
        .await;
        assert!(
            matches!(res, Ok(Err(PgWireError::Server(_)))),
            "expected Server error on 'E', got {res:?}"
        );
    }

    /// Regression test for the keepalive/feedback starvation bug
    /// (spiceai/spiceai#11616): when the event channel fills and the consumer
    /// never drains, the worker must keep emitting standby-status feedback so
    /// the server does not hit `wal_sender_timeout` and reset the connection.
    #[tokio::test]
    async fn feedback_continues_under_backpressure() {
        use std::time::Duration;
        use tokio::io::AsyncWriteExt;

        let cfg = ReplicationConfig {
            status_interval: Duration::from_millis(40),
            idle_wakeup_interval: Duration::from_secs(10),
            buffer_events: 2,
            ..Default::default()
        };

        let progress = Arc::new(SharedProgress::new(Lsn(0)));
        let (stop_tx, stop_rx) = watch::channel(false);
        // Receiver is held but never drained, so the channel stays full.
        let (tx, _rx_never_drained) = mpsc::channel(cfg.buffer_events);
        let metrics = Arc::new(ReplicationMetrics::default());
        let mut worker = WorkerState::new(cfg, progress, stop_rx, tx, Arc::clone(&metrics));

        let (worker_end, mut test_end) = tokio::io::duplex(64 * 1024);

        let worker_task = tokio::spawn(async move {
            let mut buf = BufReader::with_capacity(1024, worker_end);
            let _ = worker.stream_loop(&mut buf).await;
        });

        // Feed more messages than the 2-slot channel can hold; the worker
        // forwards two, then parks awaiting capacity on the third.
        for i in 1..=5u64 {
            test_end.write_all(&xlog_frame(i, b"Idummy")).await.unwrap();
        }
        test_end.flush().await.unwrap();

        // A feedback message is a CopyData('d') whose payload is a
        // StandbyStatusUpdate('r'). Assert several arrive while the channel is
        // full — i.e. feedback survives backpressure, not just the initial one.
        let mut reader = MessageReader::new();
        let mut feedback_count = 0;
        let deadline = Instant::now() + Duration::from_millis(500);
        while feedback_count < 3 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, reader.read(&mut test_end)).await {
                Ok(Ok(msg)) if msg.tag == b'd' && msg.payload.first() == Some(&b'r') => {
                    feedback_count += 1;
                }
                Ok(Ok(_)) => continue,
                _ => break,
            }
        }

        assert!(
            feedback_count >= 3,
            "expected repeated feedback under backpressure, got {feedback_count}"
        );
        assert!(
            metrics.stall_count() >= 1,
            "expected at least one stall to be recorded"
        );
        assert!(
            metrics.feedback_sent() >= 3,
            "feedback_sent metric should track sends, got {}",
            metrics.feedback_sent()
        );

        let _ = stop_tx.send(true);
        let _ = worker_task.await;
    }

    /// Feedback under backpressure must be *rate-limited* to `status_interval`,
    /// not emitted on every loop iteration. Guards the stall-loop deadline math:
    /// a wrong sign (deadline in the past) would busy-loop and flood the server.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn feedback_under_backpressure_is_rate_limited() {
        use std::time::Duration;
        use tokio::io::AsyncWriteExt;

        let interval = Duration::from_millis(50);
        let cfg = ReplicationConfig {
            status_interval: interval,
            idle_wakeup_interval: Duration::from_secs(10),
            buffer_events: 2,
            ..Default::default()
        };

        let progress = Arc::new(SharedProgress::new(Lsn(0)));
        let (stop_tx, stop_rx) = watch::channel(false);
        let (tx, _rx_never_drained) = mpsc::channel(cfg.buffer_events);
        let metrics = Arc::new(ReplicationMetrics::default());
        let mut worker = WorkerState::new(cfg, progress, stop_rx, tx, Arc::clone(&metrics));

        let (worker_end, mut test_end) = tokio::io::duplex(64 * 1024);
        let worker_task = tokio::spawn(async move {
            let mut buf = BufReader::with_capacity(1024, worker_end);
            let _ = worker.stream_loop(&mut buf).await;
        });

        for i in 1..=5u64 {
            test_end.write_all(&xlog_frame(i, b"Idummy")).await.unwrap();
        }
        test_end.flush().await.unwrap();

        // Over a fixed window, correct behavior emits roughly window/interval
        // feedbacks (~7 here). A deadline that fires every iteration would emit
        // far more, so a generous upper bound catches the flooding regression
        // while the lower bound confirms feedback keeps flowing.
        let window = Duration::from_millis(350);
        let end = Instant::now() + window;
        let mut reader = MessageReader::new();
        let mut feedback = 0usize;
        loop {
            let remaining = end.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, reader.read(&mut test_end)).await {
                Ok(Ok(msg)) if msg.tag == b'd' && msg.payload.first() == Some(&b'r') => {
                    feedback += 1;
                }
                Ok(Ok(_)) => {}
                _ => break,
            }
        }

        assert!(
            feedback >= 2,
            "feedback should keep flowing under backpressure, got {feedback}"
        );
        assert!(
            feedback <= 25,
            "feedback must be rate-limited to ~status_interval, got {feedback}"
        );

        let _ = stop_tx.send(true);
        let _ = worker_task.await;
    }

    // ── Change-driven standby status updates ──────────────────────────────

    /// A KeepAlive ('k') CopyData frame.
    fn keepalive_frame(wal_end: u64, reply_requested: bool) -> Vec<u8> {
        let mut payload = vec![b'k'];
        payload.extend_from_slice(&(wal_end as i64).to_be_bytes());
        payload.extend_from_slice(&0i64.to_be_bytes());
        payload.push(u8::from(reply_requested));
        let mut frame = vec![b'd'];
        frame.extend_from_slice(&((payload.len() + 4) as i32).to_be_bytes());
        frame.extend_from_slice(&payload);
        frame
    }

    /// A status update read by the scripted server.
    #[derive(Debug, PartialEq)]
    struct Status {
        applied: u64,
        reply: bool,
    }

    /// The scripted server end of a worker's stream.
    struct Server {
        end: tokio::io::DuplexStream,
        reader: MessageReader,
    }

    impl Server {
        /// The next standby status update, skipping anything else.
        async fn status(&mut self) -> Status {
            loop {
                let msg = self.reader.read(&mut self.end).await.expect("stream open");
                if msg.tag == b'd' && msg.payload.first() == Some(&b'r') {
                    let p = &msg.payload;
                    return Status {
                        applied: u64::from_be_bytes(p[1..9].try_into().unwrap()),
                        reply: p[33] == 1,
                    };
                }
            }
        }

        /// No status update arrives within `within`.
        async fn quiet(&mut self, within: std::time::Duration) -> bool {
            tokio::time::timeout(within, self.status()).await.is_err()
        }

        async fn send(&mut self, frame: &[u8]) {
            use tokio::io::AsyncWriteExt;
            self.end.write_all(frame).await.unwrap();
            self.end.flush().await.unwrap();
        }
    }

    struct Running {
        /// When the worker was started (tokio's clock).
        started: Instant,
        server: Server,
        task: tokio::task::JoinHandle<Result<()>>,
        stop: watch::Sender<bool>,
        _events: mpsc::Receiver<std::result::Result<ReplicationEvent, PgWireError>>,
    }

    fn feedback_cfg() -> ReplicationConfig {
        ReplicationConfig {
            status_interval: std::time::Duration::from_secs(1),
            idle_wakeup_interval: std::time::Duration::from_secs(10),
            ..Default::default()
        }
    }

    /// Run `stream_loop` against a scripted server over a duplex of `capacity`
    /// bytes.
    fn run_stream(progress: &Arc<SharedProgress>, capacity: usize) -> Running {
        run_stream_with(feedback_cfg(), progress, capacity)
    }

    fn run_stream_with(
        cfg: ReplicationConfig,
        progress: &Arc<SharedProgress>,
        capacity: usize,
    ) -> Running {
        let (stop, stop_rx) = watch::channel(false);
        let (tx, events) = mpsc::channel(16);
        let mut worker = WorkerState::new(
            cfg,
            Arc::clone(progress),
            stop_rx,
            tx,
            Arc::new(ReplicationMetrics::default()),
        );
        let (worker_end, end) = tokio::io::duplex(capacity);
        let task = tokio::spawn(async move {
            let mut buf = BufReader::with_capacity(1024, worker_end);
            worker.stream_loop(&mut buf).await
        });
        Running {
            started: Instant::now(),
            server: Server {
                end,
                reader: MessageReader::new(),
            },
            task,
            stop,
            _events: events,
        }
    }

    const SECOND: std::time::Duration = std::time::Duration::from_secs(1);

    #[tokio::test(start_paused = true)]
    async fn an_update_is_sent_within_the_status_interval() {
        let progress = Arc::new(SharedProgress::new(Lsn(0)));
        let mut r = run_stream(&progress, 64 * 1024);
        assert_eq!(
            r.server.status().await,
            Status {
                applied: 0,
                reply: false
            }
        );
        // Idle with nothing new: no status before the idle wakeup.
        assert!(r.server.quiet(3 * SECOND).await);

        // The interval since the last status has passed: sent at once.
        let t = Instant::now();
        progress.update_applied(Lsn(100));
        assert_eq!(
            r.server.status().await,
            Status {
                applied: 100,
                reply: false
            }
        );
        assert!(t.elapsed() < SECOND);

        // The next one waits for the interval, no longer.
        progress.update_applied(Lsn(200));
        let t = Instant::now();
        assert_eq!(
            r.server.status().await,
            Status {
                applied: 200,
                reply: false
            }
        );
        assert!(t.elapsed() <= SECOND, "took {:?}", t.elapsed());
        r.task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn coalesced_updates_send_the_newest_once() {
        let progress = Arc::new(SharedProgress::new(Lsn(0)));
        let mut r = run_stream(&progress, 64 * 1024);
        assert_eq!(r.server.status().await.applied, 0);
        let t = Instant::now();
        for lsn in 1..=100 {
            progress.update_applied(Lsn(lsn));
        }
        assert_eq!(
            r.server.status().await,
            Status {
                applied: 100,
                reply: false
            }
        );
        assert!(t.elapsed() <= SECOND);
        // Nothing else until the idle wakeup.
        assert!(r.server.quiet(8 * SECOND).await);
        r.task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn an_update_during_a_send_stays_pending() {
        let progress = Arc::new(SharedProgress::new(Lsn(0)));
        // Too small for one status message: the first send blocks mid-write.
        let mut r = run_stream(&progress, 8);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        progress.update_applied(Lsn(300));
        // The blocked send carries the position loaded before the update...
        assert_eq!(r.server.status().await.applied, 0);
        // ...and the update follows within the interval.
        let t = Instant::now();
        assert_eq!(r.server.status().await.applied, 300);
        assert!(t.elapsed() <= SECOND);
        r.task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_new_session_starts_with_the_newest_position() {
        let progress = Arc::new(SharedProgress::new(Lsn(0)));
        progress.update_applied(Lsn(500));
        let mut r = run_stream(&progress, 64 * 1024);
        assert_eq!(r.server.status().await.applied, 500);
        r.task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_send_leaves_the_position_for_the_next_session() {
        let progress = Arc::new(SharedProgress::new(Lsn(0)));
        let mut r = run_stream(&progress, 64 * 1024);
        assert_eq!(r.server.status().await.applied, 0);
        progress.update_applied(Lsn(700));
        // The server goes away before the pending status is written.
        drop(r.server);
        let ended = tokio::time::timeout(2 * SECOND, r.task).await.unwrap();
        assert!(ended.unwrap().is_err(), "the failed send ends the session");
        let mut next = run_stream(&progress, 64 * 1024);
        assert_eq!(next.server.status().await.applied, 700);
        next.task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_keepalive_requesting_a_reply_is_answered_at_once() {
        let progress = Arc::new(SharedProgress::new(Lsn(40)));
        let mut r = run_stream(&progress, 64 * 1024);
        assert_eq!(r.server.status().await.applied, 40);
        // No local change, inside the status interval.
        let t = Instant::now();
        r.server.send(&keepalive_frame(90, true)).await;
        assert_eq!(
            r.server.status().await,
            Status {
                applied: 40,
                reply: true
            }
        );
        assert!(t.elapsed() < SECOND);
        r.task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_completely_idle_connection_keeps_sending_status() {
        let progress = Arc::new(SharedProgress::new(Lsn(10)));
        let mut r = run_stream(&progress, 64 * 1024);
        let t = Instant::now();
        let mut statuses = 0;
        while t.elapsed() < std::time::Duration::from_secs(65) {
            assert_eq!(r.server.status().await.applied, 10);
            statuses += 1;
        }
        // One at start, then one per idle wakeup (10 s): not none, which
        // would let wal_sender_timeout disconnect, and not one per interval.
        assert!((6..=8).contains(&statuses), "{statuses} statuses in 65 s");
        assert!(!r.task.is_finished());
        r.task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn stopping_with_the_timer_armed_ends_at_once() {
        let progress = Arc::new(SharedProgress::new(Lsn(0)));
        let mut r = run_stream(&progress, 64 * 1024);
        assert_eq!(r.server.status().await.applied, 0);
        progress.update_applied(Lsn(900)); // pending until the interval passes
        let t = Instant::now();
        r.stop.send(true).unwrap();
        let ended = tokio::time::timeout(SECOND, r.task)
            .await
            .expect("the worker ends without waiting for the timer");
        assert!(ended.unwrap().is_ok());
        assert!(t.elapsed() < SECOND);
    }

    /// Raises from another thread race the worker clearing its signal and
    /// reading the position: whatever the interleaving, the newest position of
    /// every burst is sent within the status interval once the burst ends.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn racing_raises_never_strand_the_newest_position() {
        use std::time::Duration;
        let interval = Duration::from_millis(50);
        let cfg = ReplicationConfig {
            status_interval: interval,
            idle_wakeup_interval: Duration::from_secs(60),
            ..Default::default()
        };
        let progress = Arc::new(SharedProgress::new(Lsn(0)));
        let mut r = run_stream_with(cfg, &progress, 64 * 1024);
        let mut newest = 0u64;
        for burst in 0..50u64 {
            let p = Arc::clone(&progress);
            let from = newest;
            let len = 1 + (burst * 7919) % 5000;
            newest = std::thread::spawn(move || {
                for lsn in from + 1..=from + len {
                    p.update_applied(Lsn(lsn));
                }
                from + len
            })
            .join()
            .unwrap();
            let ended = Instant::now();
            loop {
                let status = tokio::time::timeout(4 * interval, r.server.status())
                    .await
                    .unwrap_or_else(|_| {
                        panic!("burst {burst}: {newest} not sent within {:?}", 4 * interval)
                    });
                if status.applied == newest {
                    break;
                }
                assert!(status.applied < newest);
            }
            assert!(ended.elapsed() <= 4 * interval);
        }
        r.task.abort();
    }

    // ── Feedback observer ─────────────────────────────────────────────────

    type Seen = Arc<std::sync::Mutex<Vec<std::time::Duration>>>;

    fn observed_cfg() -> (ReplicationConfig, Seen) {
        let seen: Seen = Arc::default();
        let sink = Arc::clone(&seen);
        let cfg = feedback_cfg().with_feedback_observer(crate::FeedbackObserver::new(move |s| {
            sink.lock().unwrap().push(s.pending_for)
        }));
        (cfg, seen)
    }

    fn seen(s: &Seen) -> Vec<std::time::Duration> {
        s.lock().unwrap().clone()
    }

    #[tokio::test(start_paused = true)]
    async fn the_observer_reports_how_long_a_position_waited() {
        let (cfg, s) = observed_cfg();
        let progress = Arc::new(SharedProgress::with_timing(Lsn(0), true));
        let mut r = run_stream_with(cfg, &progress, 64 * 1024);
        assert_eq!(r.server.status().await.applied, 0);
        assert!(
            seen(&s).is_empty(),
            "nothing was pending at the first status"
        );
        assert!(r.server.quiet(3 * SECOND).await);

        // Due at once: no wait.
        progress.update_applied(Lsn(100));
        assert_eq!(r.server.status().await.applied, 100);
        // Waits for the interval.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        progress.update_applied(Lsn(200));
        assert_eq!(r.server.status().await.applied, 200);
        assert_eq!(
            seen(&s),
            vec![
                std::time::Duration::ZERO,
                std::time::Duration::from_millis(900)
            ]
        );
        r.task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn coalesced_raises_report_the_oldest_wait() {
        let (cfg, s) = observed_cfg();
        let progress = Arc::new(SharedProgress::with_timing(Lsn(0), true));
        let mut r = run_stream_with(cfg, &progress, 64 * 1024);
        assert_eq!(r.server.status().await.applied, 0);
        for (at_ms, lsn) in [(100, 1), (500, 2), (900, 3)] {
            tokio::time::sleep_until(r.started + std::time::Duration::from_millis(at_ms)).await;
            progress.update_applied(Lsn(lsn));
        }
        assert_eq!(r.server.status().await.applied, 3);
        assert_eq!(seen(&s), vec![std::time::Duration::from_millis(900)]);
        r.task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn no_observation_when_the_position_does_not_advance() {
        let (cfg, s) = observed_cfg();
        let progress = Arc::new(SharedProgress::with_timing(Lsn(40), true));
        let mut r = run_stream_with(cfg, &progress, 64 * 1024);
        assert_eq!(r.server.status().await.applied, 40);
        r.server.send(&keepalive_frame(90, true)).await;
        assert!(r.server.status().await.reply);
        progress.update_applied(Lsn(40)); // not a raise
        for _ in 0..3 {
            r.server.status().await; // idle wakeups
        }
        assert!(seen(&s).is_empty());
        assert_eq!(progress.pending_since(), None);
        r.task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_write_carries_its_wait_into_the_next_session() {
        let (cfg, s) = observed_cfg();
        let progress = Arc::new(SharedProgress::with_timing(Lsn(0), true));
        let mut r = run_stream_with(cfg.clone(), &progress, 64 * 1024);
        assert_eq!(r.server.status().await.applied, 0);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        progress.update_applied(Lsn(700));
        drop(r.server);
        assert!(r.task.await.unwrap().is_err(), "the write fails");
        let since = progress.pending_since().expect("still pending");

        // The replacement session starts from the acknowledged position and
        // carries the pending start over.
        tokio::time::sleep(2 * SECOND).await;
        let next = Arc::new(SharedProgress::with_timing(Lsn(700), true));
        next.carry_pending(since);
        let mut r = run_stream_with(cfg, &next, 64 * 1024);
        assert_eq!(r.server.status().await.applied, 700);
        let waited = seen(&s);
        assert_eq!(waited.len(), 1);
        assert!(waited[0] >= 2 * SECOND, "{waited:?}");
        r.task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn nothing_is_timed_without_an_observer() {
        let progress = Arc::new(SharedProgress::with_timing(Lsn(0), false));
        progress.update_applied(Lsn(5));
        assert_eq!(progress.pending_since(), None);
    }

    #[tokio::test(start_paused = true)]
    async fn a_raise_during_a_write_keeps_a_pending_start() {
        let (cfg, s) = observed_cfg();
        let progress = Arc::new(SharedProgress::with_timing(Lsn(0), true));
        progress.update_applied(Lsn(1));
        // Too small for one status message: the first write blocks.
        let mut r = run_stream_with(cfg, &progress, 8);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        progress.update_applied(Lsn(2));
        assert_eq!(r.server.status().await.applied, 1);
        assert_eq!(r.server.status().await.applied, 2);
        assert_eq!(seen(&s).len(), 2, "both waits are reported");
        r.task.abort();
    }

    /// A write that repeats the reported position (an idle wakeup) completing
    /// while a newer raise is pending does not report that raise; the write
    /// that carries it does, once.
    #[tokio::test(start_paused = true)]
    async fn a_repeated_position_does_not_report_a_pending_raise() {
        let (cfg, s) = observed_cfg();
        let progress = Arc::new(SharedProgress::with_timing(Lsn(40), true));
        // Too small for one status message: writes block until read.
        let mut r = run_stream_with(cfg, &progress, 8);
        assert_eq!(r.server.status().await.applied, 40);
        // The idle wakeup (10 s) writes 40 again and blocks unread; raise
        // while it is in flight.
        tokio::time::sleep_until(r.started + std::time::Duration::from_millis(10_500)).await;
        progress.update_applied(Lsn(50));
        assert_eq!(r.server.status().await.applied, 40);
        assert!(
            seen(&s).is_empty(),
            "the repeated position reported nothing"
        );
        assert_eq!(r.server.status().await.applied, 50);
        assert_eq!(seen(&s).len(), 1);
        r.task.abort();
    }
}
