# pgwire-replication

[![CI](https://github.com/vnvo/pgwire-replication/actions/workflows/ci.yml/badge.svg)](https://github.com/vnvo/pgwire-replication/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/pgwire-replication.svg)](https://crates.io/crates/pgwire-replication)
[![docs.rs](https://docs.rs/pgwire-replication/badge.svg)](https://docs.rs/pgwire-replication)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](https://github.com/vnvo/pgwire-replication#license)
[![MSRV](https://img.shields.io/badge/MSRV-1.88-blue.svg)](Cargo.toml)

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
pgwire-replication = "0.5.0"
```

Or with specific features:

```toml
[dependencies]
pgwire-replication = { version = "0.5.0", default-features = false, features = ["tls-rustls"] }
```

## Requirements

- Rust 1.88 or later
- PostgreSQL 15+ with logical replication enabled (older versions will probably work too)

## Features

- Logical replication using the PostgreSQL wire protocol
- `pgoutput` logical decoding support (transport-level)
- Explicit LSN seek (`start_lsn`)
- `pg_logical_emit_message()` support
- Bounded replay (`stop_at_lsn`)
- Change-driven standby status updates: an acknowledged LSN reaches the server within `status_interval`, also on an idle stream
- Initial acknowledged position (`ack_lsn`) independent of the start position
- Backpressure-safe feedback: keepalives keep flowing even when a slow consumer fills the event buffer, avoiding `wal_sender_timeout` resets
- Multiple publications per replication slot
- Optional `pgoutput` binary output format (PG 14+)
- Pull-based replication metrics (`ReplicationClient::metrics()`) and a feedback observer reporting how long acknowledged positions waited
- Configurable per-message size limit (`max_message_size`), refused with a typed `MessageTooLarge` error
- Gated sessions: run `IDENTIFY_SYSTEM`, `TIMELINE_HISTORY` or SQL on the replication connection before `START_REPLICATION`
- Start confirmation (`wait_started`): know when the server accepted `START_REPLICATION` and the slot is held
- Keepalive handling
- Tokio-based async client
- SCRAM-SHA-256 authentication (MD5 behind the `md5` feature)
- TLS/mTLS support (via rustls)
- Connection-level configs via the startup `options` parameter (e.g. pin `TimeZone`/`IntervalStyle`/`DateStyle` for deterministic decoded output)
- Configurable startup `application_name`
- Unix domain socket connections (libpq-compatible: host starts with `/`)
- Designed for checkpoint and replay-based systems

## Non-goals

This crate intentionally does **not** provide:

- A general-purpose SQL client (a gated session can run a few queries on the replication connection before it starts streaming, nothing more)
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
        ReplicationEvent::Begin { .. } | ReplicationEvent::Commit { .. } => {}
        ReplicationEvent::Message { prefix, content, .. } => {
            // User-defined message from pg_logical_emit_message()
            handle_message(prefix, content);
        }
        ReplicationEvent::StoppedAt { .. } => break,
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
let cfg = ReplicationConfig::new(host, user, password, database, slot, publication)
    .with_start_lsn(start_lsn);
```

This enables:

- resuming replication after a crash
- replaying WAL from a known checkpoint
- controlled historical backfills

The provided LSN is sent verbatim to PostgreSQL via `START_REPLICATION`.

### Bounded Replay (Start -> Stop)

Replication can be bounded using `stop_at_lsn`:

```rust
let cfg = ReplicationConfig::new(host, user, password, database, slot, publication)
    .with_start_lsn(start_lsn)
    .with_stop_lsn(stop_lsn);
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
Calling `update_applied_lsn` indicates that **all WAL up to `lsn` has been durably persisted by the consumer** (for example, flushed to disk or a message queue).

This allows callers to control:

- durability boundaries
- batching behavior
- exactly-once or at-least-once semantics (implemented externally)

Updates are **monotonic**: reporting an older LSN is a no-op.
Standby status updates are sent asynchronously by the worker using the
latest applied LSN: a raised LSN within `status_interval` of the last update,
also on an idle stream, and at once when the server requests a reply.
For CDC pipelines, progress should typically be reported at **transaction commit boundaries**, not for every message.

### Initial acknowledged position

Until the first `update_applied_lsn`, the worker reports `start_lsn` as flushed and
applied. A consumer that resumes reading from a position it has not made durable
(for example, replaying from a slot's `restart_lsn` after a crash) sets `ack_lsn` to
its durable position instead, so the slot's `confirmed_flush_lsn` never advances
past what it could recover from:

```rust
let cfg = ReplicationConfig::new(host, user, password, database, slot, publication)
    .with_start_lsn(read_from)
    .with_ack_lsn(durable);
```

## Backpressure

The event channel between the worker and your consumer is bounded (`buffer_events`).
If the consumer falls behind and the channel fills, the worker does **not** block
silently: it keeps sending standby-status feedback every `status_interval` while
waiting for capacity, so PostgreSQL never trips `wal_sender_timeout` and resets the
stream (which would force a full replay from `confirmed_flush`). Real backpressure
still propagates to the server through TCP flow control, and feedback reports only
the applied LSN, so nothing is acknowledged that the consumer has not durably
processed.

## Message size limit

`max_message_size` bounds the largest backend message payload the worker accepts
(default and maximum: 1 GiB). The buffer is sized from the length the server
announces, so this caps the memory one message can take:

```rust
let cfg = ReplicationConfig::new(host, user, password, database, slot, publication)
    .with_max_message_size(64 * 1024 * 1024);
```

A larger message is refused before any of it is buffered, with
`PgWireError::MessageTooLarge { payload_bytes, limit_bytes }`. The refusal is
terminal: the payload may carry WAL changes, so it is never skipped, and the stream
ends. Each row change arrives as one message, so a single wide row must fit. The
limit bounds one message, not a transaction, the event buffer or total memory.
It also bounds each row of a gated session's queries.

## Gated sessions and start confirmation

`connect` returns before the server has accepted `START_REPLICATION`: connecting,
authentication and the start run in the background worker. `wait_started` returns
once the server accepted it, when the slot is held by this session, or the
worker's error if it ended first:

```rust
let mut client = ReplicationClient::connect(cfg).await?;
client.wait_started().await?;
```

`connect_gated` authenticates and then waits. On that one session you can run
replication commands or SQL with the simple query protocol, then start streaming.
What you read before the start describes the server the stream opens on, with no
window for a failover in between:

```rust
let mut client = ReplicationClient::connect_gated(cfg).await?;
let system = client.identify_system().await?; // system_id, timeline, xlogpos, dbname
if system.timeline > 1 {
    let history = client.timeline_history(system.timeline).await?;
    // history.filename, history.content
}
let rows = client.simple_query("SELECT pg_is_in_recovery()").await?; // text values, None for NULL
client.start().await?;
client.wait_started().await?;
```

A server error from a query leaves the session usable. Dropping the client before
`start` ends the session without starting replication.

## Startup parameters

- `with_options("-c datestyle=ISO,MDY -c intervalstyle=iso_8601")` sets the startup
  `options` parameter, like `libpq`'s `options` / `PGOPTIONS`, to pin session GUCs
  so decoded text values render deterministically.
- `with_application_name("my-cdc-7f3a")` sets the `application_name` shown in
  `pg_stat_activity` and `pg_stat_replication` (default `pgwire-replication`), so
  a consumer can tell its own walsender from other sessions.

## Multiple publications

A single slot can subscribe to more than one publication. Pass a single name or a
collection to `new()` / `unix()`:

```rust
// one publication
ReplicationConfig::new(host, user, password, database, slot, "orders");

// several publications on the same slot
ReplicationConfig::new(host, user, password, database, slot, ["orders", "customers"]);
```

The publication set is bound once at `START_REPLICATION` and is fixed for the
connection's lifetime; to change it, reconnect with a new config.

## Binary output

By default `pgoutput` emits column values in text format. Set `binary` to request
PostgreSQL's binary wire format (PostgreSQL 14+):

```rust
ReplicationConfig::new(host, user, password, database, slot, publication)
    .with_binary(true);
```

The library forwards `XLogData` payloads unchanged, so your decoder must handle
binary values. Any column type without a binary send function makes the walsender
error and close the stream, so leave this off unless you control both ends.

## Observability

`ReplicationClient::metrics()` returns an `Arc<ReplicationMetrics>` with pull-based
counters you can scrape into your own metrics system:

```rust
let metrics = client.metrics();
// ... later, from any task ...
println!(
    "events={} feedback={} stalls={} stall_us={}",
    metrics.events_forwarded(),
    metrics.feedback_sent(),
    metrics.stall_count(),
    metrics.stall_micros_total(),
);
```

Counters include events forwarded, feedback sent, keepalive replies, stall count and
cumulative stall duration, and the last applied / server WAL-end LSNs. Reads never
block the worker.

A feedback observer reports how long each acknowledged position waited before a
standby status update told the server about it:

```rust
use pgwire_replication::FeedbackObserver;

let cfg = cfg.with_feedback_observer(FeedbackObserver::new(|sent| {
    record_feedback_delay(sent.pending_for); // runs on the worker: keep it cheap
}));
```

It is called after each successful update that advanced the reported position.
Coalesced acknowledgments report the oldest wait. If a session fails before
reporting, `feedback_pending_since()` on the old client and
`carry_feedback_pending()` on its replacement keep the wait across sessions.
Without an observer, no clock is read.

## Idle behavior

PostgreSQL logical replication may remain silent for extended periods when no WAL is generated.
This is normal.

`idle_wakeup_interval` does not indicate failure. It bounds how long the client may block
waiting for server messages before waking up to send a standby status update and continue waiting.
While the system is idle and nothing new is acknowledged, the feedback cadence
is bounded by `idle_wakeup_interval`; a newly applied LSN is still sent within
`status_interval`.

## Logical Decoding Messages

PostgreSQL's `pg_logical_emit_message()` lets applications write custom messages
into the WAL stream. These are surfaced as `ReplicationEvent::Message` events:
```rust
ReplicationEvent::Message {
    transactional: bool,  // true if emitted inside a transaction
    lsn: Lsn,            // WAL position of the message
    prefix: String,       // application-defined prefix
    content: Bytes,       // raw message payload
}
```

NB: Messages are always enabled in the pgoutput protocol options.

**Non-transactional** messages (`SELECT pg_logical_emit_message(false, ...)`)
are delivered immediately and are not tied to any transaction boundary.

**Transactional** messages (`SELECT pg_logical_emit_message(true, ...)`)
are delivered only after the enclosing transaction commits, appearing between
`Begin` and `Commit` events.

Use cases include:

- application-level checkpoint markers
- out-of-band coordination signals
- schema migration fencing
- custom CDC control messages

## Unix Domain Sockets

On Unix systems, `pgwire-replication` supports connecting via Unix domain sockets.
Following libpq convention, set `host` to the socket directory path:
```rust
let config = ReplicationConfig::unix(
    "/var/run/postgresql",  // socket directory
    5432,                    // port (used to form .s.PGSQL.5432)
    "replicator",
    "secret",
    "mydb",
    "my_slot",
    "my_pub",
);
```

Or equivalently, since any `host` starting with `/` is treated as a socket
directory, pass one to `new(...)`:
```rust
let config = ReplicationConfig::new(
    "/var/run/postgresql", // host as socket directory
    "replicator",
    "secret",
    "mydb",
    "my_slot",
    "my_pub",
)
.with_port(5432);
```

Any `host` starting with `/` is treated as a Unix socket directory.
The actual socket file used is `{host}/.s.PGSQL.{port}`.

TLS is not supported over Unix sockets (not needed). Requesting a TLS mode other 
than `Disable` with a Unix socket host will return an error.

## Shutdown

- `stop()` requests a graceful stop (sends `CopyDone`). The client will continue to yield any buffered events and then `recv()` returns `Ok(None)`.
- `shutdown().await` is a convenience method that calls `stop()`, drains remaining events, and awaits the worker task result.
- `abort()` cancels the worker task immediately (hard stop; does not send `CopyDone`).

Dropping `ReplicationClient` requests a best-effort graceful stop. When dropped inside a Tokio runtime, the worker is detached and allowed to finish cleanly; when dropped outside a runtime, the worker may be aborted to avoid leaking a task.

## Important Notes on LSN Semantics

PostgreSQL logical replication delivers complete, committed transactions in commit order. This has important implications for LSN handling:

- **Commit LSNs are strictly monotonically increasing** across the replication stream.
- **Within a single transaction**, event LSNs are monotonically increasing.
- **Across transactions**, event LSNs are *not* monotonic. Concurrent transactions interleave their writes in WAL, so a later transaction in the stream may contain events with lower LSNs than the previous transaction.
- **The tuple `(commit_lsn, event_lsn)` provides a total ordering** suitable for checkpointing and replay.
- **LSNs are not dense**: LSNs are byte offsets into the WAL, not sequential counters. Gaps between consecutive events are normal.

For CDC pipelines, progress tracking should typically be based on commit boundaries rather than individual event LSNs.

LSNs are formatted exactly as PostgreSQL displays them:

- uppercase hexadecimal
- `X/Y` format
- up to 8 hex digits per part
- leading zeros omitted

Examples: `0/0`, `0/16B6C50`, `16/B374D848`

Parsing accepts both padded and unpadded forms for compatibility.

## Errors

All errors are `PgWireError`, which is `#[non_exhaustive]`: match it with a `_` arm.
Predicates such as `is_io()`, `is_server()`, `is_auth()`, `is_tls()`,
`is_message_too_large()` and `is_transient()` classify an error without matching on
variants.

## Upgrading from 0.4

- `PgWireError` is now `#[non_exhaustive]`, so an exhaustive `match` needs a `_` arm.
- New `PgWireError::MessageTooLarge` variant. A refused oversized message is terminal.
- New `ReplicationConfig` fields (`ack_lsn`, `application_name`, `max_message_size`,
  `feedback_observer`) and builders. `ReplicationConfig` is `#[non_exhaustive]`, so
  code that uses `new()` / `unix()` and the `with_*` builders is unaffected.
- New `ReplicationClient` APIs: `wait_started`, `connect_gated`, `simple_query`,
  `identify_system`, `timeline_history`, `start`, `feedback_pending_since` and
  `carry_feedback_pending`.
- Standby status updates are change-driven: an acknowledged LSN is sent within
  `status_interval`, also on an idle stream, where it previously waited for the
  next idle wakeup or server keepalive.

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

    // ReplicationConfig is #[non_exhaustive]: build it with new()/unix() and the
    // with_* methods. A single publication name is accepted directly; pass an
    // array (e.g. ["pub_a", "pub_b"]) to subscribe to several at once.
    let cfg = ReplicationConfig::new(
        "127.0.0.1",
        "postgres",
        "postgres",
        "postgres",
        "my_slot",
        "my_pub",
    )
    .with_tls(TlsConfig::disabled())
    .with_start_lsn(start_lsn)
    .with_status_interval(std::time::Duration::from_secs(10))
    .with_wakeup_interval(std::time::Duration::from_secs(10));

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
                ReplicationEvent::Begin { .. } | ReplicationEvent::Commit { .. } => {}
                ReplicationEvent::Message { prefix, content, .. } => {
                    println!("Message prefix={prefix:?} bytes={}", content.len());
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

Examples connect to `127.0.0.1:5432` as `postgres`/`postgres` unless `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`, `PGSLOT` and `PGPUBLICATION` say otherwise (`control_and_stream` uses these defaults as fixed values). Those that create the publication and slot use `tokio-postgres`, a dev-dependency, so no extra feature is needed.

### Replication plane only: `examples/basic.rs`

```bash
START_LSN="0/16B6C50" cargo run --example basic
```

### Control-plane + streaming: `examples/control_and_stream.rs`

```bash
cargo run --example control_and_stream
```

### Checkpoint boundaries: `examples/checkpointed.rs`

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

The `loom` model of the progress signalling runs under its own cfg:

```bash
RUSTFLAGS="--cfg pgwire_loom" CARGO_TARGET_DIR=target/loom cargo test --release --lib loom_model
```

# License

Licensed under either of:

- Apache License, Version 2.0
- MIT License

at your option.
