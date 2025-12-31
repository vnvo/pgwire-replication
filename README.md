# pgwire-replication

[![Crates.io](https://img.shields.io/crates/v/pgwire-replication.svg)](https://crates.io/crates/pgwire-replication)
[![Documentation](https://docs.rs/pgwire-replication/badge.svg)](https://docs.rs/pgwire-replication)
[![License](https://img.shields.io/crates/l/pgwire-replication.svg)](LICENSE)

A low-level, high-performance PostgreSQL logical replication client implemented directly on top of the PostgreSQL wire protocol (pgwire).

This crate is designed for **CDC, change streaming, and WAL replay systems** that require explicit control over replication state, deterministic restart behavior, and minimal runtime overhead.

`pgwire-replication` intentionally avoids `libpq`, `tokio-postgres`, and other higher-level PostgreSQL clients for the replication path. It interacts with a Postgres instance directly and relies on `START_REPLICATION ... LOGICAL ...` and the built-in `pgoutput` output plugin.


`pgwire-replication` exists to provide:
- a **direct pgwire implementation** for logical replication
- explicit, user-controlled LSN start and stop semantics
- predictable feedback and backpressure behavior
- clean integration into async systems and coordinators

This crate was originally extracted from the Deltaforge CDC project and is maintained independently.

## Installation

Add to your `Cargo.toml`:
```toml
[dependencies]
pgwire-replication = "0.1"
```

Or with specific features:
```toml
[dependencies]
pgwire-replication = { version = "0.1", default-features = false, features = ["tls-rustls"] }
```

## Requirements

- Rust 1.89 or later
- PostgreSQL 15+ with logical replication enabled

## Features

- Logical replication using the PostgreSQL wire protocol
- `pgoutput` logical decoding support (transport-level)
- Explicit LSN seek (`start_lsn`)
- Bounded replay (`stop_at_lsn`)
- Periodic standby status updates
- Keepalive handling
- Tokio-based async client
- SCRAM-SHA-256 and MD5 authentication
- TLS/mTLS support (via rustls)
- Designed for checkpoint and replay-based systems

## Non-goals

This crate intentionally does **not** provide:

- A general-purpose SQL client
- Automatic checkpoint persistence
- Exactly-once semantics
- Schema management or DDL interpretation
- Full `pgoutput` decoding into rows or events

These responsibilities belong in higher layers.


## Basic usage

```rust
use pgwire_replication::{ReplicationClient, ReplicationEvent};

let mut repl = ReplicationClient::connect(config).await?;

while let Ok(event) = repl.recv().await {
    match event {
        ReplicationEvent::XLogData { wal_end, data, .. } => {
            process(data);
            repl.update_applied_lsn(wal_end);
        }
        ReplicationEvent::KeepAlive { .. } => {}
        ReplicationEvent::StoppedAt { reached } => break,
    }
}
```

Check the **Quick Start** and **Examples** for more detailed use cases.

## Seek and Replay Semantics

`pgwire-replication` is built around explicit WAL position control.
LSNs (Log Sequence Numbers) are treated as first-class inputs and outputs and are never hidden behind opaque offsets.

### Starting from an LSN (Seek)
Every replication session begins at an explicit LSN:

```rust
ReplicationConfig {
    start_lsn: Lsn,
    ..
}
```

This enables:
- resuming replication after a crash
- replaying WAL from a known checkpoint
- controlled historical backfills

The provided LSN is sent verbatim to PostgreSQL via `START_REPLICATION`.

### Bounded Replay (Start -> Stop)
Replication can be bounded using `stop_at_lsn`:
```rust
ReplicationConfig {
    start_lsn,
    stop_at_lsn: Some(stop_lsn),
    ..
}
```

When configured:
- replication starts at start_lsn
- WAL is streamed until the stop LSN is reached
- a ReplicationEvent::StoppedAt { reached } event is emitted
- the replication connection is terminated cleanly using CopyDone

This enables:
- deterministic WAL replay
- offline backfills
- "replay up to checkpoint" workflows
- controlled reprocessing in recovery scenarios


## Progress Tracking and Feedback
Progress is **not** auto-committed.
Instead, the consumer explicitly reports progress:
```rust
repl.update_applied_lsn(lsn);
```

This allows callers to control:
- durability boundaries
- batching behavior
- exactly-once or at-least-once semantics (implemented externally)

Standby status updates are sent periodically using the latest applied LSN.


## Important Notes on LSN Semantics
- PostgreSQL does not guarantee that every logical replication message advances the WAL end position.
- Small or fast transactions may share the same WAL position.
- LSNs should be treated as monotonic but not dense.

Today, bounded replay is evaluated using WAL positions observed during streaming.
Future versions may expose commit-boundary LSNs derived from pgoutput decoding for stronger replay guarantees.


## TLS support

TLS is optional and uses `rustls`.
TLS configuration is provided explicitly via `ReplicationConfig` and does not rely on system OpenSSL.


## Quick start

Control plane (publication/slot creation) is typically done using a proper "Postgres client" (Your choice).
This crate handles only the replication plane.

```rust
use pgwire_replication::{
    client::ReplicationEvent, Lsn, ReplicationClient, ReplicationConfig, SslMode, TlsConfig,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Control plane (publication/slot creation) is typically done using a Postgres client.
    // This crate implements the replication plane only.

    // Use a real LSN:
    // - from your checkpoint store, or
    // - from SQL (pg_current_wal_lsn / slot confirmed_flush_lsn), or
    // - from a previous run.
    let start_lsn = Lsn::parse("0/16B6C50")?;

    let cfg = ReplicationConfig {
        host: "127.0.0.1".into(),
        port: 5432,
        user: "postgres".into(),
        password: "postgres".into(),
        database: "postgres".into(),
        tls: TlsConfig::disabled(),
        slot: "my_slot".into(),
        publication: "my_pub".into(),
        start_lsn,
        stop_at_lsn: None,

        status_interval: std::time::Duration::from_secs(1),
        idle_timeout: std::time::Duration::from_secs(30),
        buffer_events: 8192,
    };

    let mut client = ReplicationClient::connect(cfg).await?;

    loop {
        let ev = client.recv().await?;
        match ev {
            ReplicationEvent::XLogData { wal_end, data, .. } => {
                // Process pgoutput payload (bytes)
                println!("XLogData wal_end={wal_end} bytes={}", data.len());

                // Report progress so WAL can be released and feedback remains correct.
                client.update_applied_lsn(wal_end);
            }
            ReplicationEvent::KeepAlive { wal_end, reply_requested, .. } => {
                println!("KeepAlive wal_end={wal_end} reply_requested={reply_requested}");
            }
            ReplicationEvent::StoppedAt { reached } => {
                println!("StoppedAt reached={reached}");
                break;
            }
        }
    }

    Ok(())
}
```

## Examples

Examples that use the control-plane SQL client (`tokio-postgres`) require the `examples` feature.

### Replication plane only: `examples/basic.rs`
```bash
START_LSN="0/16B6C50" cargo run --example basic
```

### Control-plane + streaming: `examples/checkpointed.rs`
```bash
cargo run --example checkpointed
```

### Bounded replay: `examples/bounded_replay.rs`
```bash
cargo run --example bounded_replay
```
### With TLS enabled: `examples/with_tls.rs`
```bash
PGHOST=db.example.com \
PGPORT=5432 \
PGUSER=repl_user \
PGPASSWORD=secret \
PGDATABASE=postgres \
PGSLOT=example_slot_tls \
PGPUBLICATION=example_pub_tls \
PGTLS_CA=/path/to/ca.pem \
PGTLS_SNI=db.example.com \
cargo run --example with_tls
```

### Enabling mTLS : `examples/with_mtls.rs`
Inject the fake dns record, if you need to:
```bash
sudo sh -c 'echo "127.0.0.1 db.example.com" >> /etc/hosts'
```
and then:
```bash
PGHOST=db.example.com \
PGPORT=5432 \
PGUSER=repl_user \
PGPASSWORD=secret \
PGDATABASE=postgres \
PGSLOT=example_slot_mtls \
PGPUBLICATION=example_pub_mtls \
PGTLS_CA=/etc/ssl/ca.pem \
PGTLS_CLIENT_CERT=/etc/ssl/client.crt.pem \
PGTLS_CLIENT_KEY=/etc/ssl/client.key.pem \
PGTLS_SNI=db.example.com \
cargo run --example with_mtls
```
- `PGUSER/PGPASSWORD` are used for control-plane setup (publication/slot).
- `REPL_USER/REPL_PASSWORD` are used for the replication stream.
- If PGHOST is an **IP address**, you must set `PGTLS_SNI` to a DNS name on the cert.
- Client key should be **PKCS#8 PEM** for best compatibility.
- `VerifyCa` can be used instead of `VerifyFull` if hostname validation is not possible.


# Testing

Integration tests use Docker via `testcontainers` and are gated behind a feature flag:
```bash
cargo test --features integration-tests -- --nocapture
```

# License

Licensed under either of:

- Apache License, Version 2.0
- MIT License

at your option.
