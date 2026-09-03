use std::sync::Mutex;

use async_trait::async_trait;

use crate::envelope::EventEnvelope;
use crate::error::Result;
use crate::store::EventStore;
use crate::subscription::topic_matches;

/// An in-memory event store backed by a `Vec`.
///
/// Useful for testing and development. Not durable — all data is lost on drop.
pub struct InMemoryStore<T: Clone + Send + Sync + 'static> {
    entries: Mutex<Vec<EventEnvelope<T>>>,
}

impl<T: Clone + Send + Sync + 'static> InMemoryStore<T> {
    /// Creates a new empty `InMemoryStore`.
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
        }
    }
}

impl<T: Clone + Send + Sync + 'static> Default for InMemoryStore<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<T: Clone + Send + Sync + 'static> EventStore<T> for InMemoryStore<T> {
    async fn append(&self, envelope: &EventEnvelope<T>) -> Result<()> {
        self.entries.lock().unwrap().push(envelope.clone());
        Ok(())
    }

    async fn load_since(&self, since: i64) -> Result<Vec<EventEnvelope<T>>> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.timestamp >= since)
            .cloned()
            .collect())
    }

    async fn load_by_topic(&self, topic_pattern: &str) -> Result<Vec<EventEnvelope<T>>> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .iter()
            .filter(|e| topic_matches(topic_pattern, &e.topic))
            .cloned()
            .collect())
    }

    async fn load_all(&self) -> Result<Vec<EventEnvelope<T>>> {
        Ok(self.entries.lock().unwrap().clone())
    }
}
