#![forbid(unsafe_code)]
//! Async event bus for Rust.
//!
//! `eventbus` provides typed pub/sub messaging with wildcard subscription support.
//!
//! # Quick Start
//!
//! ```rust
//! use eventbus::EventBus;
//!
//! let bus = EventBus::new();
//!
//! bus.subscribe("orders.*", |event| {
//!     println!("Got order event: {}", event.payload);
//! });
//!
//! bus.publish("orders.created", "order #123".to_string()).unwrap();
//! ```

pub mod bus;
pub mod envelope;
pub mod error;
pub mod event;
pub mod subscription;

pub use bus::EventBus;
pub use envelope::EventEnvelope;
pub use error::{EventBusError, Result};
pub use event::{Event, TypedEvent};
pub use subscription::{topic_matches, Subscription};
