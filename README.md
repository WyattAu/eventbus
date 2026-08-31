# typed-eventbus

Async event bus for Rust — typed pub/sub with optional persistence, replay, and wildcard subscriptions.

## Features

- **Typed events** — Generic `EventBus<T>` for type-safe payloads
- **Wildcard subscriptions** — `*` matches one segment, `**` matches multiple
- **DashMap-backed** — Lock-free concurrent reads and writes
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
