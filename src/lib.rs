#![warn(
    clippy::all,
    clippy::cargo,
    clippy::perf,
    clippy::style,
    clippy::correctness,
    clippy::suspicious
)]
#![allow(
    clippy::module_name_repetitions,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::multiple_crate_versions
)]

pub mod auth;
pub mod client;
pub mod config;
pub mod error;
pub mod lsn;
pub mod protocol;
pub mod tls;

pub use client::{ReplicationClient, ReplicationEvent, ReplicationEventReceiver};
pub use config::{ReplicationConfig, SslMode, TlsConfig};
pub use error::{PgWireError, Result};
pub use lsn::Lsn;
