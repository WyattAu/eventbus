# Changelog

All notable changes are documented here.

## [0.4.0] - 2026-10-10

### Added

- Bounded delivery: `EventBus::subscribe_bounded` gives a subscriber a
  queue of at most `capacity` envelopes and a dedicated worker task, so a
  slow callback no longer delays the publisher. `publish` already spawned
  each callback but then awaited every handle before returning, which left
  publisher latency equal to the slowest subscriber's latency.
- `OverflowPolicy` (`DropOldest`, `DropNewest`, `Reject`): what a full queue
  does is now an explicit per-subscriber decision instead of an emergent
  consequence of load. Defaults to `DropOldest`.
- `SubscriberStats` via `EventBus::stats`: queue depth, delivered, dropped,
  rejected, and panicked counters. Queue depth is the early signal — a
  backlog that stays above zero under sustained publishing means the
  callback cannot keep up, well before `dropped` proves data was lost.
- `EventBus::publish_report` / `publish_report_with_metadata`, returning
  `DeliveryReport` (matched / enqueued / dropped / rejected) so a producer
  can react to loss instead of inferring it later from a consumer's backlog.

### Fixed

- Panicking callbacks are counted in `SubscriberStats::panicked` instead of
  being silently swallowed: the awaited publish path discarded the
  `JoinError` with `let _ = handle.await`, so a crashing subscriber was
  indistinguishable from a healthy one. A bounded subscriber's worker also
  survives a panicking callback rather than unwinding and stopping
  delivery for that subscriber.

### Unchanged

- `subscribe`, `subscribe_sync`, `publish`, `publish_sequential`, and the
  persistence APIs are untouched; this release is additive.

## [0.3.5] - 2026-09-12

### Added

- `tests/config_matrix.rs` (3 tests): behavior-observable coverage for the
  public config knobs — `EventEnvelope::with_metadata` (metadata delivered
  verbatim through `publish_with_metadata` and preserved through
  `PersistentBus::replay`, with the no-metadata default contrasted) and the
  subscription topic pattern (exact vs `*` vs `**` delivery sets on the
  same publishes).

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
