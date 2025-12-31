# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

---

## [0.1.0] – 2025-12-31

### Added
- Initial release of `pgwire-replication`
- Logical replication over PostgreSQL wire protocol (pgwire)
- Explicit replication start via `start_lsn`
- Bounded replay via `stop_at_lsn`
- Periodic standby status updates (feedback)
- Keepalive handling
- Clean replication shutdown using `CopyDone`
- Tokio-based async replication client
- Optional TLS and mTLS support using `rustls`
- Integration tests using Docker (`testcontainers`)
- End-to-end tests covering:
  - keepalive handling
  - LSN seek behavior
  - bounded replay semantics
  - clean shutdown behavior

### Notes
- pgoutput payloads are currently treated as opaque bytes.
- Replay bounds are evaluated using WAL positions observed during streaming.
- Commit-boundary semantics may be added in a future release.

---

## [Unreleased]

### Planned
- Commit-boundary LSN extraction from pgoutput
- Stronger replay guarantees based on commit end LSN
- Additional pgoutput message parsing helpers
- Fuzz testing for pgwire framing
