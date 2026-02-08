# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

---

## [0.2.0] - 2026-02-08

### Added
- Support for `pg_logical_emit_message()` via new `ReplicationEvent::Message` variant
  - Messages are always enabled (`messages 'true'` in pgoutput protocol options); zero overhead when unused
  - Both transactional and non-transactional messages are supported
  - Events include `prefix` (application-defined string), `content` (raw bytes), `lsn`, and `transactional` flag
- Unix domain socket connections: set `host` to a socket directory path (e.g. `/var/run/postgresql`)
  - Follows libpq convention: any `host` starting with `/` is treated as a socket directory
  - Socket path is `{host}/.s.PGSQL.{port}`
  - `ReplicationConfig::unix()` convenience constructor
  - `ReplicationConfig::is_unix_socket()` and `ReplicationConfig::unix_socket_path()` helpers

### Changed
- `START_REPLICATION` now includes `messages 'true'` in pgoutput options (previously only `proto_version` and `publication_names`)

---

## [0.1.2] - 2025-01-17
### Fixed
- Corrected documentation on LSN monotonicity semantics (event LSNs are not monotonic across transactions; `(commit_lsn, event_lsn)` tuple provides total ordering)

---

## [0.1.1] - 2025-01-09

### Fixed
- `stop()` is now immediately responsive regardless of `idle_wakeup_interval`
  - Previously, the worker only checked the stop signal at the top of the stream loop, meaning `stop()` could take up to `idle_wakeup_interval` to take effect if blocked waiting for Postgres messages
  - Now uses `tokio::select!` to concurrently watch for stop signals while waiting, ensuring prompt shutdown

---

## [0.1.0] - 2025-12-31

### Added
- Initial release of `pgwire-replication`
- Tokio-based async PostgreSQL logical replication client
- PostgreSQL wire protocol (pgwire) implementation
- SCRAM-SHA-256 authentication (default)
- MD5 authentication (optional, feature-gated)
- TLS support via rustls with modes: disable, require, verify-ca, verify-full
- Mutual TLS (mTLS) client certificate support
- Explicit replication start via `start_lsn`
- Bounded replay via `stop_at_lsn`
- Periodic standby status updates (feedback)
- Keepalive handling with automatic replies
- Clean shutdown using `CopyDone`
- Integration tests using Docker (`testcontainers`)

### Notes
- pgoutput payloads are returned as opaque bytes (no parsing yet)
- Replay bounds use WAL positions observed during streaming

---

### Planned
- Commit-boundary LSN extraction from pgoutput
- Stronger replay guarantees based on commit end LSN
- Additional pgoutput message parsing helpers
- Fuzz testing for pgwire framing


[Unreleased]: https://github.com/vnvo/pgwire-replication/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/vnvo/pgwire-replication/releases/tag/v0.2.0
[0.1.2]: https://github.com/vnvo/pgwire-replication/releases/tag/v0.1.2
[0.1.1]: https://github.com/vnvo/pgwire-replication/releases/tag/v0.1.1
[0.1.0]: https://github.com/vnvo/pgwire-replication/releases/tag/v0.1.0
