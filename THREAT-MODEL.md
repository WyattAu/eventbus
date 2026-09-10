# Threat Model — eventbus

Reference: STRIDE. Scope: the crate's public API surface (`EventBus`,
`PersistentBus`, `EventEnvelope`, `EventStore` implementations, wildcard
subscriptions). Trust boundaries: (1) topics and payloads entering
`publish`, (2) wildcard patterns entering `subscribe`/`topic_matches`,
(3) the SQLite/Postgres store files (shared disk), (4) the dependency tree.

## Assets

| ID | Asset | Example |
|----|-------|---------|
| A1 | Availability of the bus (subscribers keep receiving) | Flooded payload or pathological topic pattern stalls dispatch |
| A2 | Durability of persisted events | Replay/recovery loses or duplicates events |
| A3 | Confidentiality of payloads in storage | Events readable from the SQLite file by another process |

## STRIDE Analysis

| # | Threat | Category | Surface | Mitigation | Verifying test |
|---|--------|----------|---------|------------|----------------|
| T1 | Malformed topic/payload panics during publish or matching | DoS | `EventBus::publish`, `topic_matches` | Errors are `Result`; wildcard matching is a bounded linear scan over pattern segments, no recursion; fuzz target `fuzz_envelope` exercises arbitrary envelope bytes without panics | `fuzz_envelope.rs` (fuzz/), `test_wildcard_subscription`, `test_double_star_only`, `topic_filtering` |
| T2 | Persisted events tampered with on disk | Tampering | `SqliteEventStore` | **Not mitigated** — SQLite file integrity only (page checksums); no per-event MAC/signature. A writer with file access can rewrite history. Documented residual risk: the store path is the trust boundary | `id_sequencing_across_reopen`, `persistent_bus_publish_and_replay` (detect *loss*, not forgery) |
| T3 | Replay/duplicate delivery after recovery | Replay | `PersistentBus::replay_since` | Monotonic event IDs persisted across reopen (`MAX(id)` seeding); consumers dedup by ID since replay resumes strictly after the last seen ID | `id_sequencing_across_reopen`, `persistent_bus_replay_since`, `replay_since` |
| T4 | Publisher impersonation / unauthorized topic writes | Spoofing | `EventBus::publish` | **Not mitigated** — any caller may publish to any topic; there is no ACL or publisher identity. Documented residual risk: authorization belongs above the bus | Code review; no identity field exists on `publish` (API surface audit) |
| T5 | Unbounded memory (in-memory store, slow subscribers) | DoS | `InMemoryStore`, in-memory subscriber queues | **Not mitigated** — `InMemoryStore` grows without bound; subscriber callbacks are awaited inline (no internal queue to fill, but a slow callback stalls its bus). `cleanup(before_ms)` exists for the SQLite store but nothing enforces calling it | `cleanup_removes_old`, `in_memory_store_append_and_load_all` (correctness only); growth bounds documented as caller responsibility |
| T6 | Payload confidentiality in storage | Info disclosure | SQLite store | **Not mitigated** — payloads are plaintext bytes in the events table. Documented: encrypt at the storage layer if events are sensitive | Code review |
| T7 | Event forgery via metadata injection | Tampering | `EventEnvelope::with_metadata` | Metadata is typed `serde_json::Value` and never interpreted by the bus; consumers must validate anything security-relevant they read from it | `test_with_metadata`, `publish_with_metadata` |

## Repudiation

Partially mitigated: the SQLite-backed store is an append-only trail with
monotonic IDs and timestamps, so *that* an event was published is
attributable to the process — but there is no publisher identity (T4), so
attribution among in-process publishers is impossible.

## Out of Scope

- Transport security between processes: the bus is in-process; distribution
  is the caller's job.
- Consumer-side idempotency: replay semantics mean consumers must be
  idempotent or dedup by event ID.
- SQLite/Postgres server hardening (feature `postgres` delegates to the DB).

## Residual Risks

- **R1 (Medium, accepted):** No publisher authentication or per-topic
  authorization (T4). In-process trust domain assumed, same rationale as the
  standard library's channels.
- **R2 (Medium, accepted):** `InMemoryStore` and per-topic history are
  unbounded (T5); a long-lived process with chatty topics grows until OOM.
  Callers must use `cleanup` or a persistent store with retention.
- **R3 (Low, accepted):** Persisted events are unauthenticated at rest (T2)
  and plaintext (T6). Filesystem permissions are the control.
- **R4 (Low, accepted):** Wildcard pattern `**` matching is linear per
  subscription per publish; thousands of overlapping patterns degrade
  publish latency (O(subscribers) per event). No pattern-count limit.
