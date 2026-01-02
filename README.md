# pgwire-replication

[![CI](https://github.com/vnvo/pgwire-replication/actions/workflows/ci.yml/badge.svg)](https://github.com/vnvo/pgwire-replication/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/pgwire-replication.svg)](https://crates.io/crates/pgwire-replication)
[![docs.rs](https://docs.rs/pgwire-replication/badge.svg)](https://docs.rs/pgwire-replication)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](https://github.com/vnvo/pgwire-replication#license)
[![MSRV](https://img.shields.io/badge/MSRV-1.89-blue.svg)](Cargo.toml)

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

- Rust 1.75 or later
- PostgreSQL 15+ with logical replication enabled (older versions will probably work too)

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

while let Some(event) = repl.recv().await? {
    match event {
        ReplicationEvent::XLogData { wal_end, data, .. } => {
            process(data);
            repl.update_applied_lsn(wal_end);
        }
        ReplicationEvent::KeepAlive { .. } => {}
        ReplicationEvent::StoppedAt { reached } => break,
    }
}
// Clean end-of-stream
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
- After StoppedAt is emitted, the stream ends cleanly and recv() returns Ok(None)
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

## Idle behavior

PostgreSQL logical replication may remain silent for extended periods when no WAL is generated.
This is normal.

`idle_wakeup_interval` does not indicate failure. It bounds how long the client may block
waiting for server messages before waking up to send a standby status update and continue waiting.
While the system is idle, the effective feedback cadence is bounded by
`idle_wakeup_interval`, not `status_interval`.

## Shutdown

- `stop()` requests a graceful stop (sends `CopyDone`). The client will continue to yield any buffered events and then `recv()` returns `Ok(None)`.
- `shutdown().await` is a convenience method that calls `stop()`, drains remaining events, and awaits the worker task result.
- `abort()` cancels the worker task immediately (hard stop; does not send `CopyDone`).

Dropping `ReplicationClient` requests a best-effort graceful stop. When dropped inside a Tokio runtime, the worker is detached and allowed to finish cleanly; when dropped outside a runtime, the worker may be aborted to avoid leaking a task.

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

        status_interval: std::time::Duration::from_secs(10),
        idle_wakeup_interval: std::time::Duration::from_secs(10),
        buffer_events: 8192,
    };

    let mut client = ReplicationClient::connect(cfg).await?;

    loop {
        match client.recv().await {
            Ok(Some(ev)) => match ev {
                ReplicationEvent::XLogData { wal_end, data, .. } => {
                    println!("XLogData wal_end={wal_end} bytes={}", data.len());
                    client.update_applied_lsn(wal_end);
                }
                ReplicationEvent::KeepAlive {
                    wal_end,
                    reply_requested,
                    ..
                } => {
                    println!("KeepAlive wal_end={wal_end} reply_requested={reply_requested}");
                }
                ReplicationEvent::StoppedAt { reached } => {
                    println!("StoppedAt reached={reached}");
                    // break is optional; the stream should end shortly anyway
                    break;
                }
            },
            Ok(None) => {
                println!("Replication ended cleanly");
                break;
            }
            Err(e) => {
                eprintln!("Replication failed: {e}");
                return Err(e.into());
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
