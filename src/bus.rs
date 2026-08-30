use std::sync::Arc;

use dashmap::DashMap;
use uuid::Uuid;

use crate::envelope::{EventEnvelope, EventMetadata};
use crate::error::Result;
use crate::subscription::Subscription;

/// A callback function that handles events.
pub type EventCallback<T> = Arc<dyn Fn(EventEnvelope<T>) + Send + Sync>;

/// An async event bus supporting typed pub/sub with wildcard subscriptions.
pub struct EventBus<T: Clone + Send + Sync + 'static> {
    subscriptions: DashMap<Uuid, SubscriptionEntry<T>>,
    /// Tracks topic -> subscriber IDs for fast lookup.
    topic_index: DashMap<String, Vec<Uuid>>,
}

struct SubscriptionEntry<T: Clone + Send + Sync + 'static> {
    subscription: Subscription,
    callback: EventCallback<T>,
}

impl<T: Clone + Send + Sync + 'static> EventBus<T> {
    /// Creates a new empty `EventBus`.
    pub fn new() -> Self {
        Self {
            subscriptions: DashMap::new(),
            topic_index: DashMap::new(),
        }
    }

    /// Publishes an event to all matching subscribers.
    ///
    /// Returns the number of subscribers notified.
    pub fn publish(&self, topic: impl Into<String>, payload: T) -> Result<usize> {
        self.publish_with_metadata(topic, payload, EventMetadata::new())
    }

    /// Publishes an event with custom metadata.
    pub fn publish_with_metadata(
        &self,
        topic: impl Into<String>,
        payload: T,
        metadata: EventMetadata,
    ) -> Result<usize> {
        let topic = topic.into();
        let envelope = EventEnvelope {
            id: Uuid::new_v4(),
            topic: topic.clone(),
            payload,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
            metadata,
        };

        let mut notified = 0;

        for entry in self.subscriptions.iter() {
            let sub = entry.value();
            if sub.subscription.matches(&topic) {
                (sub.callback)(envelope.clone());
                notified += 1;
            }
        }

        Ok(notified)
    }

    /// Subscribes to a topic pattern with a callback.
    ///
    /// Returns a `Subscription` handle that can be used to unsubscribe.
    pub fn subscribe(
        &self,
        topic_pattern: impl Into<String>,
        callback: impl Fn(EventEnvelope<T>) + Send + Sync + 'static,
    ) -> Subscription {
        let subscription = Subscription::new(topic_pattern);
        let sub_id = subscription.id;

        self.subscriptions.insert(
            sub_id,
            SubscriptionEntry {
                subscription: subscription.clone(),
                callback: Arc::new(callback),
            },
        );

        // Update topic index for future optimizations
        self.topic_index
            .entry(subscription.topic_pattern.clone())
            .or_default()
            .push(sub_id);

        subscription
    }

    /// Unsubscribes by subscription ID.
    ///
    /// Returns true if the subscription was found and removed.
    pub fn unsubscribe(&self, subscription_id: Uuid) -> bool {
        if let Some(mut entry) = self.subscriptions.remove(&subscription_id) {
            entry.1.subscription.deactivate();

            // Clean up topic index
            let topic = entry.1.subscription.topic_pattern.clone();
            if let Some(mut ids) = self.topic_index.get_mut(&topic) {
                ids.retain(|id| *id != subscription_id);
                if ids.is_empty() {
                    drop(ids);
                    self.topic_index.remove(&topic);
                }
            }
            true
        } else {
            false
        }
    }

    /// Returns the number of active subscriptions.
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Returns true if any subscriber matches the given topic.
    pub fn has_subscribers_for(&self, topic: &str) -> bool {
        self.subscriptions
            .iter()
            .any(|entry| entry.value().subscription.matches(topic))
    }
}

impl<T: Clone + Send + Sync + 'static> Default for EventBus<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + Send + Sync + 'static> std::fmt::Debug for EventBus<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventBus")
            .field("subscriptions", &self.subscriptions.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_publish_subscribe() {
        let bus = EventBus::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        bus.subscribe("orders.created", move |_event| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        let notified = bus.publish("orders.created", "payload".to_string()).unwrap();
        assert_eq!(notified, 1);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_wildcard_subscription() {
        let bus = EventBus::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        bus.subscribe("orders.*", move |_event| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        bus.publish("orders.created", "1".to_string()).unwrap();
        bus.publish("orders.cancelled", "2".to_string()).unwrap();
        bus.publish("payments.created", "3".to_string()).unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_unsubscribe() {
        let bus = EventBus::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let sub = bus.subscribe("orders.*", move |_event| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        bus.publish("orders.created", "1".to_string()).unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        assert!(bus.unsubscribe(sub.id));
        bus.publish("orders.created", "2".to_string()).unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1); // not incremented
    }

    #[test]
    fn test_multiple_subscribers() {
        let bus = EventBus::new();
        let counter1 = Arc::new(AtomicUsize::new(0));
        let counter2 = Arc::new(AtomicUsize::new(0));
        let c1 = counter1.clone();
        let c2 = counter2.clone();

        bus.subscribe("orders.*", move |_| {
            c1.fetch_add(1, Ordering::SeqCst);
        });
        bus.subscribe("orders.*", move |_| {
            c2.fetch_add(1, Ordering::SeqCst);
        });

        bus.publish("orders.created", "1".to_string()).unwrap();

        assert_eq!(counter1.load(Ordering::SeqCst), 1);
        assert_eq!(counter2.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_subscription_count() {
        let bus = EventBus::<String>::new();
        assert_eq!(bus.subscription_count(), 0);

        let _sub1 = bus.subscribe("a.*", |_| {});
        let _sub2 = bus.subscribe("b.*", |_| {});
        assert_eq!(bus.subscription_count(), 2);
    }
}
