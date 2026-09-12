# Changelog

All notable changes are documented here.

## [0.3.4] - 2026-09-12

### Added

- SQLite store integration suite (`tests/sqlite_store.rs`, 9 tests, fully
  local in a tempdir): durability across full store drop/reopen, id
  sequencing resumption without overwriting rows, topic/timestamp filters
  on disk, binary payload integrity, retention cleanup, persisted-event
  shape stability, plus end-to-end at-least-once redelivery through
  `PersistentBus` (late-subscriber replay, offset-bounded `replay_since`,
  failed persistence never delivering, restart simulation).
- Postgres store integration suite (`tests/postgres_store.rs`, 9 tests)
  against a real Postgres spun up per run with testcontainers:
  idempotent migration, append/load round trips, timestamp offsets,
  wildcard topic replay, persistence across a pool reconnect
  (server-side durability), store-error surfacing, and
  publish/consume/replay redelivery including concurrent publishers.

### CI

- New `integration` job running the sqlite suite (no services) and the
  postgres suite (testcontainers).

## [0.3.1] - 2026-09-11

### Fixed

- 22-gate quality audit pass: documentation completeness
  (README badges, REQUIREMENTS/THREAT-MODEL coverage) and
  feature-gated test hygiene.


All notable changes to this project are documented here. Format: [Keep a
Changelog](https://keepachangelog.com/) — versions follow [semver](https://semver.org).

## [0.2.0] - 2026-09-05

### Added
- Initial public release.
