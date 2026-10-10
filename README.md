# typed-eventbus

[![docs.rs](https://docs.rs/eventbus-kit/badge.svg)](https://docs.rs/eventbus-kit)
[![crates.io](https://img.shields.io/crates/v/eventbus-kit.svg)](https://crates.io/crates/eventbus-kit)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)

Async event bus for Rust — typed pub/sub with optional persistence, replay, and wildcard subscriptions.

## Features

- **Typed events** — Generic `EventBus<T>` for type-safe payloads
- **Wildcard subscriptions** — `*` matches one segment, `**` matches multiple
- **DashMap-backed** — Lock-free concurrent reads and writes
- **Bounded delivery** — Per-subscriber queues with an explicit overflow
  policy, so a slow consumer cannot slow the publisher down
- **Observable** — Queue depth, drops, rejections, and panics per subscriber
- **Metadata support** — Attach arbitrary JSON metadata to events
- **Zero allocation topics** — Efficient pattern matching on `&str`

## Quick Start

```rust
use eventbus::EventBus;

let bus = EventBus::new();

// Subscribe with a wildcard pattern
bus.subscribe("orders.*", |event| {
    println!("Order event: {} on {}", event.payload, event.topic);
});

// Publish events
bus.publish("orders.created", "order #123".to_string()).unwrap();
```

## Wildcard Patterns

| Pattern | `orders.created` | `orders.created.v2` | `payments.refund` |
|---------|:----------------:|:-------------------:|:-----------------:|
| `orders.*` | yes | no | no |
| `orders.**` | yes | yes | no |
| `*` | yes | no | no |
| `**` | yes | yes | yes |

- `*` matches exactly one segment (between `.` separators)
- `**` matches zero or more segments
- Exact segments match literally

## Bounded Subscribers

`subscribe` spawns each callback and then **waits for all of them** before
`publish` returns, so publisher latency is the slowest subscriber's
latency. That is the right choice when a caller genuinely needs every
handler to have run, and the wrong one for a consumer that must never be
able to hold up a producer.

`subscribe_bounded` hands the callback a queue with a worker instead. The
publisher never waits, and what happens when the queue fills is a choice
you make per subscriber:

| Policy | On a full queue | Use for |
|---|---|---|
| `DropOldest` | evict the oldest, keep the freshest | log shipping, analytics, cache warming |
| `DropNewest` | discard the incoming event | audit trails, replication journals |
| `Reject` | refuse it and tell the producer | events that must not go missing |

```rust
use typed_eventbus::{EventBus, OverflowPolicy};

let bus: EventBus<String> = EventBus::new();
let sub = bus.subscribe_bounded("orders.**", 128, OverflowPolicy::DropOldest, |event| {
    Box::pin(async move { println!("{}", event.payload) })
}).await;

bus.publish_report("orders.created", "order #123".into()).await.unwrap();

let stats = bus.stats(sub.id).unwrap();
println!("depth {} dropped {}", stats.queue_depth(), stats.dropped());
```

A `DropOldest` subscriber is a legitimate consumer to run — but if
`queue_depth` sits above zero under steady load, that subscriber cannot
keep up, and `dropped` is already counting real losses. Both are exported
as plain counters so they can be scraped as metrics.

## Unsubscribing

```rust
use eventbus::EventBus;

let bus = EventBus::new();

let sub = bus.subscribe("orders.*", |event| {
    println!("Got: {}", event.payload);
});

// Later, unsubscribe using the handle
bus.unsubscribe(sub.id);
```

## Typed Events

```rust
use eventbus::{EventBus, TypedEvent};

#[derive(Debug, Clone)]
struct OrderCreated {
    order_id: String,
    amount: f64,
}

impl std::fmt::Display for OrderCreated {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OrderCreated({})", self.order_id)
    }
}

let bus = EventBus::new();
bus.subscribe("orders.*", |event| {
    println!("Payload: {}", event.payload);
});

let event = TypedEvent::new("orders.created", OrderCreated {
    order_id: "123".into(),
    amount: 99.99,
});

bus.publish(event.topic().clone(), event.into_payload()).unwrap();
```

## License

Licensed under either of [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at your option.
