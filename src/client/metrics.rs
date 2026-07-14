//! Observability counters for the replication worker.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::lsn::Lsn;

/// Pull-based metrics for a replication stream.
///
/// Shared between the background worker (which updates the counters) and the
/// consumer (which reads them via [`ReplicationClient::metrics`]). All updates
/// use relaxed atomics, so the worker's hot path never blocks and reads are
/// cheap. Counters are monotonic for the lifetime of the connection.
///
/// [`ReplicationClient::metrics`]: crate::client::ReplicationClient::metrics
#[derive(Debug, Default)]
pub struct ReplicationMetrics {
    events_forwarded: AtomicU64,
    feedback_sent: AtomicU64,
    keepalive_replies: AtomicU64,
    stall_count: AtomicU64,
    stall_micros_total: AtomicU64,
    last_applied_lsn: AtomicU64,
    last_wal_end: AtomicU64,
}

impl ReplicationMetrics {
    /// Replication events successfully forwarded to the consumer channel.
    #[inline]
    pub fn events_forwarded(&self) -> u64 {
        self.events_forwarded.load(Ordering::Relaxed)
    }

    /// Standby-status (feedback) messages sent to the server.
    #[inline]
    pub fn feedback_sent(&self) -> u64 {
        self.feedback_sent.load(Ordering::Relaxed)
    }

    /// Feedback messages sent specifically in reply to a reply-requested
    /// keepalive. A subset of [`feedback_sent`](Self::feedback_sent).
    #[inline]
    pub fn keepalive_replies(&self) -> u64 {
        self.keepalive_replies.load(Ordering::Relaxed)
    }

    /// Number of times forwarding an event had to wait for channel capacity
    /// (i.e. the consumer applied backpressure).
    #[inline]
    pub fn stall_count(&self) -> u64 {
        self.stall_count.load(Ordering::Relaxed)
    }

    /// Cumulative time, in microseconds, the worker spent waiting for channel
    /// capacity while backpressured. Feedback continues throughout this time.
    #[inline]
    pub fn stall_micros_total(&self) -> u64 {
        self.stall_micros_total.load(Ordering::Relaxed)
    }

    /// Most recent applied LSN reported to the server via feedback.
    #[inline]
    pub fn last_applied_lsn(&self) -> Lsn {
        Lsn::from_u64(self.last_applied_lsn.load(Ordering::Relaxed))
    }

    /// Most recent server WAL-end position observed on an incoming message.
    #[inline]
    pub fn last_wal_end(&self) -> Lsn {
        Lsn::from_u64(self.last_wal_end.load(Ordering::Relaxed))
    }

    // ── worker-side updates (crate-internal) ──

    #[inline]
    pub(crate) fn record_event_forwarded(&self) {
        self.events_forwarded.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn record_feedback_sent(&self, applied: Lsn) {
        self.feedback_sent.fetch_add(1, Ordering::Relaxed);
        self.last_applied_lsn
            .store(applied.as_u64(), Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn record_keepalive_reply(&self) {
        self.keepalive_replies.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a backpressure stall has begun. Counted at the start (not
    /// the end) so an in-progress stall is observable while the worker is still
    /// parked awaiting capacity.
    #[inline]
    pub(crate) fn record_stall_begin(&self) {
        self.stall_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record the duration of a stall once capacity is regained.
    #[inline]
    pub(crate) fn record_stall_end(&self, micros: u64) {
        self.stall_micros_total.fetch_add(micros, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn set_last_wal_end(&self, wal_end: Lsn) {
        self.last_wal_end.store(wal_end.as_u64(), Ordering::Relaxed);
    }
}
