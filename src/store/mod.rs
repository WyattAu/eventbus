use crate::envelope::EventEnvelope;
use crate::error::Result;
use async_trait::async_trait;

/// A trait for persisting and retrieving events.
///
/// Implementations provide durable storage (e.g. in-memory, Postgres)
/// so events can be replayed after the fact.
#[async_trait]
pub trait EventStore<T: Clone + Send + Sync + 'static>: Send + Sync {
    /// Append an event envelope to the store.
    async fn append(&self, envelope: &EventEnvelope<T>) -> Result<()>;

    /// Load all events with a timestamp >= `since` (epoch millis).
    async fn load_since(&self, since: i64) -> Result<Vec<EventEnvelope<T>>>;

    /// Load events whose topic matches the given pattern.
    async fn load_by_topic(&self, topic_pattern: &str) -> Result<Vec<EventEnvelope<T>>>;

    /// Load all stored events in timestamp order.
    async fn load_all(&self) -> Result<Vec<EventEnvelope<T>>>;
}

/// In-memory event store for testing and development.
pub mod memory;

/// Postgres-backed event store (requires the `postgres` feature).
#[cfg(feature = "postgres")]
pub mod postgres;

pub use memory::InMemoryStore;
#[cfg(feature = "postgres")]
pub use postgres::PostgresStore;
