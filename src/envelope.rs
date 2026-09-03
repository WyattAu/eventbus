use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Metadata attached to every event.
pub type EventMetadata = std::collections::HashMap<String, serde_json::Value>;

/// An envelope wrapping an event payload with routing and identification info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope<T: Clone + Send + Sync + 'static> {
    /// Unique identifier for this event instance.
    pub id: Uuid,
    /// The topic the event was published to. `Arc<str>` avoids cloning the topic
    /// string on every `envelope.clone()` (one allocation per publish, then
    /// atomic refcount bumps for each subscriber).
    pub topic: Arc<str>,
    /// The event payload.
    pub payload: T,
    /// Timestamp when the event was created (UTC epoch millis).
    pub timestamp: i64,
    /// Arbitrary metadata.
    pub metadata: EventMetadata,
}

impl<T: Clone + Send + Sync + 'static> EventEnvelope<T> {
    /// Creates a new `EventEnvelope` with a generated ID and current timestamp.
    ///
    /// `topic` accepts `String`, `&str`, `Arc<str>`, `Cow<'_, str>` etc. via
    /// `Into<Arc<str>>` — no extra allocation when already `Arc<str>`.
    pub fn new(topic: impl Into<Arc<str>>, payload: T) -> Self {
        Self {
            id: Uuid::new_v4(),
            topic: topic.into(),
            payload,
            timestamp: chrono_now_millis(),
            metadata: EventMetadata::new(),
        }
    }

    /// Builder-style setter for metadata.
    pub fn with_metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }
}

impl<T: Clone + Send + Sync + fmt::Display + 'static> fmt::Display for EventEnvelope<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.topic, self.payload)
    }
}

fn chrono_now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestPayload {
        message: String,
    }

    #[test]
    fn test_envelope_creation() {
        let payload = TestPayload {
            message: "hello".into(),
        };
        let envelope = EventEnvelope::new("test.topic", payload.clone());

        assert_eq!(envelope.topic.as_ref(), "test.topic");
        assert_eq!(envelope.payload, payload);
        assert!(!envelope.id.is_nil());
        assert!(envelope.timestamp > 0);
    }

    #[test]
    fn test_with_metadata() {
        let payload = TestPayload {
            message: "hello".into(),
        };
        let envelope = EventEnvelope::new("test.topic", payload)
            .with_metadata("source", serde_json::json!("test"))
            .with_metadata("version", serde_json::json!(1));

        assert_eq!(envelope.metadata.len(), 2);
        assert_eq!(
            envelope.metadata.get("source").unwrap(),
            &serde_json::json!("test")
        );
    }
}
