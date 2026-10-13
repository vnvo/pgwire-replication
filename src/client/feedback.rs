//! Observing standby status feedback: how long an acknowledged position
//! waited before the server was told about it.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// A standby status update that told the server about a newer applied
/// position, reported to a [`FeedbackObserver`] after the write succeeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct FeedbackSent {
    /// From the oldest raise of the applied position that this update is the
    /// first to report, until the update was written. Raises coalesced into
    /// one update keep the oldest, so this is how long an acknowledged
    /// position was withheld from the server, including across a failed
    /// write or a new session that carried it over
    /// ([`ReplicationClient::carry_feedback_pending`](crate::ReplicationClient::carry_feedback_pending)).
    pub pending_for: Duration,
}

/// Called after each successful standby status update that advanced the
/// position reported to the server; never for one that repeated it. Runs on
/// the replication worker, so it must be cheap and must not block.
#[derive(Clone)]
pub struct FeedbackObserver(Arc<dyn Fn(FeedbackSent) + Send + Sync>);

impl FeedbackObserver {
    pub fn new(observer: impl Fn(FeedbackSent) + Send + Sync + 'static) -> Self {
        Self(Arc::new(observer))
    }

    pub(crate) fn observe(&self, sent: FeedbackSent) {
        (self.0)(sent)
    }
}

impl fmt::Debug for FeedbackObserver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FeedbackObserver(..)")
    }
}

/// Two observers are equal when they are the same callback.
impl PartialEq for FeedbackObserver {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for FeedbackObserver {}

// The callback is only ever called by the replication worker
// (`observe` is crate-private), so a reference held across a
// `catch_unwind` boundary cannot observe state that a panic left broken.
// This keeps `ReplicationConfig` unwind safe, as it was before observers.
impl std::panic::UnwindSafe for FeedbackObserver {}
impl std::panic::RefUnwindSafe for FeedbackObserver {}
