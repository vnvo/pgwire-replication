//! Error types for pgwire-replication.
//!
//! All errors in this crate are represented by [`PgWireError`], which covers:
//! - I/O errors (network, file system)
//! - Protocol errors (malformed messages, unexpected responses)
//! - Server errors (PostgreSQL error responses)
//! - Authentication errors (wrong password, unsupported method)
//! - TLS errors (handshake failure, certificate issues)
//! - Task errors (worker panics, unexpected termination)
//! - Message-size errors (a frame larger than the configured limit)

use std::sync::Arc;
use thiserror::Error;

/// Error type for all pgwire-replication operations.
///
/// This enum is `#[non_exhaustive]`: match on it with a `_` arm so that new
/// variants can be added without a breaking release.
#[derive(Debug, Error, Clone)]
#[non_exhaustive]
pub enum PgWireError {
    /// I/O error (network, file system).
    ///
    /// Wraps `std::io::Error` in an `Arc` to preserve `ErrorKind` while
    /// keeping `PgWireError` `Clone`. Use `.kind()` via `Arc`'s `Deref`.
    #[error("io error: {0}")]
    Io(Arc<std::io::Error>),

    /// Protocol error - malformed message or unexpected response.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Server error - PostgreSQL returned an error response.
    ///
    /// The message typically includes SQLSTATE code.
    #[error("server error: {0}")]
    Server(String),

    /// Authentication error - wrong credentials or unsupported method.
    #[error("authentication error: {0}")]
    Auth(String),

    /// TLS error - handshake failure, certificate validation, etc.
    #[error("tls error: {0}")]
    Tls(String),

    /// Task error - worker panicked or terminated unexpectedly.
    #[error("task error: {0}")]
    Task(String),

    /// Internal error - bug in the library.
    #[error("internal error: {0}")]
    Internal(String),

    /// A backend message announced a payload larger than the configured
    /// limit, so it was refused before any of it was buffered.
    ///
    /// Distinct from [`Protocol`](Self::Protocol) so a caller can tell a
    /// legitimate frame over its own
    /// [`max_message_size`](crate::config::ReplicationConfig::max_message_size)
    /// apart from malformed framing, and report which bound was exceeded
    /// without parsing the message text. Non-transient: the refused payload
    /// is never consumed, so the connection cannot be resumed.
    #[error("backend message too large: {payload_bytes} bytes (max {limit_bytes})")]
    MessageTooLarge {
        /// Payload size the server announced, in bytes.
        payload_bytes: usize,
        /// Limit that refused it, in bytes.
        limit_bytes: usize,
    },
}

impl PgWireError {
    /// Returns `true` if this is an I/O error.
    #[inline]
    pub fn is_io(&self) -> bool {
        matches!(self, PgWireError::Io(_))
    }

    /// Returns `true` if this is a server error.
    #[inline]
    pub fn is_server(&self) -> bool {
        matches!(self, PgWireError::Server(_))
    }

    /// Returns `true` if this is an authentication error.
    #[inline]
    pub fn is_auth(&self) -> bool {
        matches!(self, PgWireError::Auth(_))
    }

    /// Returns `true` if this is a TLS error.
    #[inline]
    pub fn is_tls(&self) -> bool {
        matches!(self, PgWireError::Tls(_))
    }

    /// Returns `true` if this is a message-size error.
    #[inline]
    pub fn is_message_too_large(&self) -> bool {
        matches!(self, PgWireError::MessageTooLarge { .. })
    }

    /// Returns `true` if this error is likely transient and retryable.
    ///
    /// Transient errors include I/O errors and task errors. Non-transient
    /// errors (auth, server, protocol) typically require configuration changes.
    pub fn is_transient(&self) -> bool {
        matches!(self, PgWireError::Io(_) | PgWireError::Task(_))
    }
}

impl From<std::io::Error> for PgWireError {
    fn from(err: std::io::Error) -> Self {
        PgWireError::Io(Arc::new(err))
    }
}

/// Result type alias for pgwire-replication operations.
pub type Result<T> = std::result::Result<T, PgWireError>;
