# Requirements — eventbus

Numbered, testable requirements. Every requirement maps to at least one named
test or doc-comment contract; security-relevant items cite THREAT-MODEL.md rows.

Scope: Durable event bus — in-memory topic fan-out with optional Postgres/SQLite persistence

## Functional

| ID | Requirement | Priority |
|----|-------------|----------|
| REQ-EB-001 | Publish → subscribe fan-out delivers to all current subscribers; subscription is atomic (no half-attached state) | MUST |
| REQ-EB-002 | Durable backends persist events before ack and replay unacknowledged events on restart | MUST |
| REQ-EB-003 | Backends are feature-gated (`postgres`, `sqlite`); default build is in-memory only | MUST |

## Security

| ID | Requirement | Priority |
|----|-------------|----------|
| REQ-EB-100 | Payloads are opaque bytes; persistence layers never interpret or execute them | MUST |
| REQ-EB-101 | SQL backends use parameterized queries exclusively (no string-built SQL) | MUST |

## Observability & API hygiene

| ID | Requirement | Priority |
|----|-------------|----------|
| REQ-EB-900 | All fallible public APIs return typed errors; production `unwrap`/`expect` is denied or explicitly justified with an invariant comment | MUST |
| REQ-EB-901 | Public items carry doc comments with runnable examples where practical | SHOULD |

Reviewed: 2026-09-11
