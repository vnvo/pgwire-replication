use crate::config::ReplicationConfig;
use crate::error::{PgWireError, Result};
use crate::lsn::Lsn;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

#[cfg(not(feature = "tls-rustls"))]
use crate::config::SslMode;

use super::worker::{ReplicationEvent, ReplicationEventReceiver, WorkerState};

pub struct ReplicationClient {
    rx: ReplicationEventReceiver,
    applied_tx: watch::Sender<Lsn>,
    stop_tx: watch::Sender<bool>,
    join: Option<JoinHandle<std::result::Result<(), PgWireError>>>,
}

impl ReplicationClient {
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

    /// Receive next replication event (or error).
    pub async fn recv(&mut self) -> Result<ReplicationEvent> {
        match self.rx.recv().await {
            Some(Ok(ev)) => Ok(ev),
            Some(Err(e)) => Err(e),
            None => {
                // worker is gone; await join to find out why
                let join = self.join.take().ok_or_else(|| {
                    PgWireError::Internal(
                        "replication worker channel closed (join already consumed)".into(),
                    )
                })?;

                match join.await {
                    Ok(Ok(())) => Err(PgWireError::Internal(
                        "replication worker exited unexpectedly".into(),
                    )),
                    Ok(Err(e)) => Err(e),
                    Err(join_err) => Err(PgWireError::Task(format!(
                        "replication worker panicked: {join_err}"
                    ))),
                }
            }
        }
    }

    /// Update the applied/committed LSN that will be reported in standby status updates.
    /// This should typically be advanced at transaction COMMIT boundaries in the parent (controller) pipeline/checkpoint policy.
    pub fn update_applied_lsn(&self, lsn: Lsn) {
        let _ = self.applied_tx.send(lsn);
    }

    /// Ask the worker to stop. After calling stop(), recv() will return an error once drained.
    pub fn stop(&self) {
        let _ = self.stop_tx.send(true);
    }

    /// Join the worker task (useful for diagnostics).
    pub async fn join(mut self) -> std::result::Result<(), PgWireError> {
        let join = self
            .join
            .take()
            .ok_or_else(|| PgWireError::Task("join already consumed".into()))?;
        match join.await {
            Ok(inner) => inner,
            Err(e) => Err(PgWireError::Task(format!("join error: {e}"))),
        }
    }
}

async fn run_worker(worker: &mut WorkerState, cfg: &ReplicationConfig) -> Result<()> {
    let tcp = TcpStream::connect((cfg.host.as_str(), cfg.port)).await?;
    tcp.set_nodelay(true)?;

    // TLS upgrade if enabled (feature-gated)
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
            return Err(PgWireError::Tls("tls-rustls feature disabled".into()));
        }
        let mut s = tcp;
        worker.run_on_stream(&mut s).await
    }
}
