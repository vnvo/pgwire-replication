//! PostgreSQL logical replication client.
//!
//! This module provides the main interface for consuming logical replication
//! events from PostgreSQL.
//!
//! # Overview
//!
//! The client establishes a replication connection to PostgreSQL and streams
//! change events (inserts, updates, deletes) from the configured publication.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────┐     channel      ┌─────────────────┐
//! │                 │◄────────────────│                 │
//! │  Your App       │  ReplicationEvent│  Worker Task    │
//! │                 │─────────────────►│                 │
//! │                 │   applied_lsn    │                 │
//! └─────────────────┘                  └────────┬────────┘
//!                                               │
//!                                               │ TCP/TLS
//!                                               ▼
//!                                      ┌─────────────────┐
//!                                      │   PostgreSQL    │
//!                                      │  (pgoutput)     │
//!                                      └─────────────────┘
//! ```
//!
//! # Example
//!
//! ```ignore
//! use pgwire_replication::client::ReplicationClient;
//! use pgwire_replication::config::ReplicationConfig;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = ReplicationConfig::new(
//!         "localhost",
//!         "replicator",
//!         "password",
//!         "mydb",
//!         "my_slot",
//!         "my_publication",
//!     );
//!
//!     let mut client = ReplicationClient::connect(config).await?;
//!
//!     loop {
//!         let event = client.recv().await?;
//!         match event {
//!             ReplicationEvent::XLogData { data, wal_end, .. } => {
//!                 // Parse and process pgoutput data
//!                 println!("Received {} bytes at {}", data.len(), wal_end);
//!
//!                 // Report progress to allow WAL cleanup
//!                 client.update_applied_lsn(wal_end);
//!             }
//!             ReplicationEvent::KeepAlive { wal_end, .. } => {
//!                 println!("Server heartbeat at {}", wal_end);
//!             }
//!             ReplicationEvent::StoppedAt { reached } => {
//!                 println!("Reached stop LSN {}", reached);
//!                 break;
//!             }
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```

mod tokio_client;
mod worker;

pub use tokio_client::ReplicationClient;
pub use worker::{ReplicationEvent, ReplicationEventReceiver};
