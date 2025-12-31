//! PostgreSQL wire protocol implementation.
//!
//! This module provides low-level primitives for:
//! - Reading and writing PostgreSQL frontend/backend messages ([`framing`])
//! - Parsing authentication and error responses ([`messages`])
//! - Handling streaming replication protocol messages ([`replication`])
//!
//! # Wire Protocol Overview
//!
//! PostgreSQL uses a message-based protocol where each message consists of:
//! - 1 byte: message type tag
//! - 4 bytes: message length (including these 4 bytes)
//! - N bytes: message payload
//!
//! Exception: Startup and SSL request messages omit the type tag.
//!
//! # Replication Protocol
//!
//! During logical replication, the server sends CopyData messages containing
//! either `XLogData` (WAL changes) or `KeepAlive` (heartbeats). The client
//! responds with `StandbyStatusUpdate` messages to report replay progress.

pub mod framing;
pub mod messages;
pub mod replication;

pub use framing::BackendMessage;
pub use messages::{ErrorFields, parse_auth_request, parse_error_response};
pub use replication::{
    PG_EPOCH_MICROS, ReplicationCopyData, encode_standby_status_update, parse_copy_data,
    pg_to_unix_timestamp, unix_to_pg_timestamp,
};
