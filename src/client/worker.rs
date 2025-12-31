use bytes::Bytes;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

use crate::config::ReplicationConfig;
use crate::error::{PgWireError, Result};
use crate::lsn::Lsn;
use crate::protocol::framing::{
    read_backend_message, write_copy_data, write_query, write_startup_message,
};
use crate::protocol::messages::{parse_auth_request, parse_error_response};
use crate::protocol::replication::{
    ReplicationCopyData, encode_standby_status_update, parse_copy_data,
};

use tokio::io::{AsyncRead, AsyncWrite};

#[derive(Debug, Clone)]
pub enum ReplicationEvent {
    KeepAlive {
        wal_end: Lsn,
        reply_requested: bool,
        server_time_micros: i64,
    },
    XLogData {
        wal_start: Lsn,
        wal_end: Lsn,
        server_time_micros: i64,
        data: Bytes,
    },

    /// Emitted when the worker decides it has reached stop_at_lsn (commit boundary best-effort).
    /// In v0.1, this is evaluated by WAL end LSN; we can use pgoutput Commit end LSN later.
    StoppedAt { reached: Lsn },
}

pub type ReplicationEventReceiver =
    mpsc::Receiver<std::result::Result<ReplicationEvent, PgWireError>>;

pub struct WorkerState {
    cfg: ReplicationConfig,
    applied_rx: watch::Receiver<Lsn>,
    stop_rx: watch::Receiver<bool>,
    out: mpsc::Sender<std::result::Result<ReplicationEvent, PgWireError>>,
}

impl WorkerState {
    pub fn new(
        cfg: ReplicationConfig,
        applied_rx: watch::Receiver<Lsn>,
        stop_rx: watch::Receiver<bool>,
        out: mpsc::Sender<std::result::Result<ReplicationEvent, PgWireError>>,
    ) -> Self {
        Self {
            cfg,
            applied_rx,
            stop_rx,
            out,
        }
    }

    pub async fn run_on_stream<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
    ) -> Result<()> {
        // Startup (protocol 3.0 = 196608)
        let replication_param = "database";
        let params = [
            ("user", self.cfg.user.as_str()),
            ("database", self.cfg.database.as_str()),
            ("replication", replication_param),
            ("client_encoding", "UTF8"),
            ("application_name", "pgwire-replication"),
        ];
        write_startup_message(stream, 196608, &params).await?;

        // Auth
        self.authenticate(stream).await?;

        // START_REPLICATION ... LOGICAL ...
        let sql = format!(
            "START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names '{}')",
            self.cfg.slot,
            self.cfg.start_lsn,
            self.cfg.publication.replace('\'', "''"),
        );
        write_query(stream, &sql).await?;

        // Wait for CopyBothResponse ('W')
        loop {
            let msg = read_backend_message(stream).await?;
            match msg.tag {
                b'W' => break,
                b'E' => return Err(PgWireError::Server(parse_error_response(&msg.payload))),
                b'N' | b'S' | b'K' => continue,
                _ => continue,
            }
        }

        let mut last_status_sent = Instant::now() - self.cfg.status_interval;
        let mut last_applied = *self.applied_rx.borrow();

        loop {
            if *self.stop_rx.borrow() {
                let _ = crate::protocol::framing::write_copy_done(stream).await;
                return Ok(());
            }

            // Periodic feedback using latest applied LSN
            let current_applied = *self.applied_rx.borrow();
            if current_applied != last_applied {
                last_applied = current_applied;
            }
            if last_status_sent.elapsed() >= self.cfg.status_interval {
                self.send_feedback(stream, last_applied, false).await?;
                last_status_sent = Instant::now();
            }

            // Read next message with idle timeout
            let msg = tokio::time::timeout(self.cfg.idle_timeout, read_backend_message(stream))
                .await
                .map_err(|_| PgWireError::Protocol("replication idle timeout".into()))??;

            match msg.tag {
                b'd' => {
                    let cd = parse_copy_data(msg.payload)?;
                    match cd {
                        ReplicationCopyData::KeepAlive {
                            wal_end,
                            server_time_micros,
                            reply_requested,
                        } => {
                            // If server requests reply, respond immediately with latest applied LSN.
                            if reply_requested {
                                self.send_feedback(stream, last_applied, true).await?;
                                last_status_sent = Instant::now();
                            }
                            let _ = self
                                .out
                                .send(Ok(ReplicationEvent::KeepAlive {
                                    wal_end,
                                    reply_requested,
                                    server_time_micros,
                                }))
                                .await;
                        }
                        ReplicationCopyData::XLogData {
                            wal_start,
                            wal_end,
                            server_time_micros,
                            data,
                        } => {
                            // Optional stop condition: best-effort based on WAL end LSN
                            if let Some(stop) = self.cfg.stop_at_lsn
                                && wal_end >= stop
                            {
                                let _ = self
                                    .out
                                    .send(Ok(ReplicationEvent::XLogData {
                                        wal_start,
                                        wal_end,
                                        server_time_micros,
                                        data,
                                    }))
                                    .await;
                                let _ = self
                                    .out
                                    .send(Ok(ReplicationEvent::StoppedAt { reached: wal_end }))
                                    .await;
                                let _ = crate::protocol::framing::write_copy_done(stream).await;
                                return Ok(());
                            }

                            let _ = self
                                .out
                                .send(Ok(ReplicationEvent::XLogData {
                                    wal_start,
                                    wal_end,
                                    server_time_micros,
                                    data,
                                }))
                                .await;
                        }
                    }
                }
                b'E' => {
                    let err = PgWireError::Server(parse_error_response(&msg.payload));
                    let _ = self.out.send(Err(err)).await;
                    return Ok(());
                }
                _ => {
                    // In CopyBoth mode, we generally expect CopyData; other tags are unusual.
                }
            }
        }
    }

    async fn authenticate<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
    ) -> Result<()> {
        loop {
            let msg = read_backend_message(stream).await?;
            match msg.tag {
                b'R' => {
                    let (code, rest) = parse_auth_request(&msg.payload)?;
                    match code {
                        0 => { /* AuthenticationOk */ }
                        3 => {
                            // cleartext password
                            let mut p = Vec::from(self.cfg.password.as_bytes());
                            p.push(0);
                            crate::protocol::framing::write_password_message(stream, &p).await?;
                        }
                        10 => {
                            self.auth_scram(stream, rest).await?;
                        }
                        #[cfg(feature = "md5")]
                        5 => {
                            if rest.len() != 4 {
                                return Err(PgWireError::Protocol("md5 auth salt missing".into()));
                            }
                            let mut salt = [0u8; 4];
                            salt.copy_from_slice(&rest[..4]);
                            let md5 = postgres_md5(&self.cfg.password, &self.cfg.user, &salt);
                            let mut p = md5.into_bytes();
                            p.push(0);
                            crate::protocol::framing::write_password_message(stream, &p).await?;
                        }
                        _ => {
                            return Err(PgWireError::Auth(format!(
                                "unsupported auth method: {code}"
                            )));
                        }
                    }
                }
                b'E' => return Err(PgWireError::Server(parse_error_response(&msg.payload))),
                b'S' | b'K' => {}      // ParameterStatus, BackendKeyData
                b'Z' => return Ok(()), // ReadyForQuery
                _ => {}
            }
        }
    }

    async fn auth_scram<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
        mechanisms: &[u8],
    ) -> Result<()> {
        // Parse offered mechanisms (cstring list)
        let mut b = mechanisms;
        let mut offered = Vec::new();
        while !b.is_empty() {
            if let Some(pos) = b.iter().position(|&x| x == 0) {
                if pos == 0 {
                    break;
                }
                offered.push(String::from_utf8_lossy(&b[..pos]).to_string());
                b = &b[pos + 1..];
            } else {
                break;
            }
        }

        if !offered.iter().any(|m| m == "SCRAM-SHA-256") {
            return Err(PgWireError::Auth(format!(
                "server does not offer SCRAM-SHA-256: {offered:?}"
            )));
        }

        #[cfg(not(feature = "scram"))]
        return Err(PgWireError::Auth("SCRAM feature disabled".into()));

        #[cfg(feature = "scram")]
        {
            use crate::auth::scram::ScramClient;

            let scram = ScramClient::new(&self.cfg.user);

            // SASLInitialResponse is sent as a PasswordMessage ('p'):
            // mechanism\0 + int32(len) + bytes
            let mut init = Vec::new();
            init.extend_from_slice(b"SCRAM-SHA-256");
            init.push(0);
            init.extend_from_slice(&(scram.client_first.len() as i32).to_be_bytes());
            init.extend_from_slice(scram.client_first.as_bytes());
            crate::protocol::framing::write_password_message(stream, &init).await?;

            // Receive AuthenticationSASLContinue (code 11)
            let server_first = read_auth_blob(stream, 11).await?;
            let server_first_str = String::from_utf8_lossy(&server_first).to_string();

            let (client_final, auth_message, salted_password) =
                scram.client_final(&self.cfg.password, &server_first_str)?;
            crate::protocol::framing::write_password_message(stream, client_final.as_bytes())
                .await?;

            // Receive AuthenticationSASLFinal (code 12)
            let server_final = read_auth_blob(stream, 12).await?;
            let server_final_str = String::from_utf8_lossy(&server_final).to_string();
            ScramClient::verify_server_final(&server_final_str, &salted_password, &auth_message)?;

            Ok(())
        }
    }

    async fn send_feedback<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        stream: &mut S,
        applied: Lsn,
        reply_requested: bool,
    ) -> Result<()> {
        let now = pg_epoch_micros();
        let payload = encode_standby_status_update(applied, now, reply_requested);
        write_copy_data(stream, &payload).await
    }
}

async fn read_auth_blob<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    want_code: i32,
) -> Result<Vec<u8>> {
    loop {
        let msg = read_backend_message(stream).await?;
        match msg.tag {
            b'R' => {
                let (code, rest) = parse_auth_request(&msg.payload)?;
                if code == want_code {
                    return Ok(rest.to_vec());
                }
                return Err(PgWireError::Auth(format!(
                    "unexpected auth code {code}, expected {want_code}"
                )));
            }
            b'E' => return Err(PgWireError::Server(parse_error_response(&msg.payload))),
            _ => {}
        }
    }
}

fn pg_epoch_micros() -> i64 {
    // Postgres epoch = 2000-01-01T00:00:00Z
    // Use system time; only used in status updates.
    use std::time::{SystemTime, UNIX_EPOCH};
    const PG_EPOCH_UNIX: i64 = 946684800; // 2000-01-01 in unix seconds
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let unix_micros = (now.as_secs() as i64) * 1_000_000 + (now.subsec_micros() as i64);
    unix_micros - (PG_EPOCH_UNIX * 1_000_000)
}

#[cfg(feature = "md5")]
fn postgres_md5(password: &str, user: &str, salt: &[u8; 4]) -> String {
    fn md5_hex(bytes: &[u8]) -> String {
        let digest = md5::compute(bytes);
        format!("{:x}", digest)
    }
    let inner = md5_hex(format!("{password}{user}").as_bytes());
    let mut outer = Vec::with_capacity(inner.len() + 4);
    outer.extend_from_slice(inner.as_bytes());
    outer.extend_from_slice(salt);
    format!("md5{}", md5_hex(&outer))
}
