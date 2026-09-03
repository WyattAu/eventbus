use std::sync::Arc;

use uuid::Uuid;

use crate::bus::EventBus;
use crate::envelope::{EventEnvelope, EventMetadata};
use crate::error::Result;
use crate::store::EventStore;
use crate::subscription::Subscription;

/// A decorator that adds persistence and replay to an `EventBus`.
///
/// Events are persisted to the underlying `EventStore` before being
/// delivered to in-memory subscribers. Stored events can be replayed
/// later via [`replay`](Self::replay) or [`replay_since`](Self::replay_since).
pub struct PersistentBus<T: Clone + Send + Sync + 'static> {
    bus: EventBus<T>,
    store: Arc<dyn EventStore<T>>,
}

impl<T: Clone + Send + Sync + 'static> PersistentBus<T> {
    /// Create a new `PersistentBus` wrapping the given bus and store.
    pub fn new(bus: EventBus<T>, store: Arc<dyn EventStore<T>>) -> Self {
        Self { bus, store }
    }

    /// Publish an event — persists to store, then delivers to subscribers.
    pub async fn publish(&self, topic: impl Into<Arc<str>>, payload: T) -> Result<usize> {
        let topic: Arc<str> = topic.into();
        let envelope = EventEnvelope {
            id: Uuid::new_v4(),
            topic,
            payload,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
            metadata: EventMetadata::new(),
        };
        self.store.append(&envelope).await?;
        self.bus.publish_with_envelope(envelope).await
    }

    /// Subscribe to a topic pattern with a synchronous callback.
    pub async fn subscribe(
        &self,
        topic_pattern: impl Into<Arc<str>>,
        callback: impl Fn(Arc<EventEnvelope<T>>) + Send + Sync + 'static,
    ) -> Subscription {
        self.bus.subscribe_sync(topic_pattern, callback).await
    }

    /// Replay all events matching a pattern to existing subscribers.
    pub async fn replay(&self, topic_pattern: &str) -> Result<usize> {
        let events = self.store.load_by_topic(topic_pattern).await?;
        let count = events.len();
        for event in events {
            self.bus.publish_with_envelope(event).await?;
        }
        Ok(count)
    }

    /// Replay events since a given timestamp (epoch millis).
    pub async fn replay_since(&self, since: i64) -> Result<usize> {
        let events = self.store.load_since(since).await?;
        let count = events.len();
        for event in events {
            self.bus.publish_with_envelope(event).await?;
        }
        Ok(count)
    }

    /// Returns the number of active subscribers on the inner bus.
    pub fn subscriber_count(&self) -> usize {
        self.bus.subscription_count()
    }
}
