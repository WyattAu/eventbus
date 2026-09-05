#![forbid(unsafe_code)]
#![deny(missing_docs)]
//! Async event bus for Rust.
//!
//! `eventbus` provides typed pub/sub messaging with wildcard subscription support.
//!
//! # Quick Start
//!
//! ```rust
//! use typed_eventbus::EventBus;
//!
//! let rt = tokio::runtime::Runtime::new().unwrap();
//! rt.block_on(async {
//!     let bus = EventBus::new();
//!
//!     bus.subscribe("orders.*", |event| Box::pin(async move {
//!         println!("Got order event: {}", event.payload);
//!     })).await;
//!
//!     bus.publish("orders.created", "order #123".to_string()).await.unwrap();
//! });
//! ```

/// Event bus implementation.
pub mod bus;
/// Event envelope type.
pub mod envelope;
/// Error types.
pub mod error;
/// Event types.
pub mod event;
/// SQLite-backed event persistence.
#[cfg(feature = "sqlite")]
pub mod persistence;
/// Persistent bus decorator.
pub mod persistent_bus;
/// Event store trait and implementations.
pub mod store;
/// Subscription management.
pub mod subscription;

pub use bus::EventBus;
pub use envelope::EventEnvelope;
pub use error::{EventBusError, Result};
pub use event::{Event, TypedEvent};
pub use persistent_bus::PersistentBus;
#[cfg(feature = "postgres")]
pub use store::PostgresStore;
pub use store::{EventStore, InMemoryStore};
pub use subscription::{Subscription, topic_matches};
