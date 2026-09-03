use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use dashmap::DashMap;
use uuid::Uuid;

use crate::envelope::{EventEnvelope, EventMetadata};
use crate::error::Result;
use crate::subscription::Subscription;

/// An async callback that handles events. Envelope is shared via `Arc` to avoid
/// per-subscriber heap clones of the payload / metadata.
pub type EventCallback<T> =
    Arc<dyn Fn(Arc<EventEnvelope<T>>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// An async event bus supporting typed pub/sub with wildcard subscriptions.
pub struct EventBus<T: Clone + Send + Sync + 'static> {
    subscriptions: DashMap<Uuid, SubscriptionEntry<T>>,
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
        }
    }

    /// Publishes an event to all matching subscribers concurrently.
    ///
    /// Each matching subscriber is spawned as a separate tokio task.
    /// Returns the number of subscribers notified.
    pub async fn publish(&self, topic: impl Into<Arc<str>>, payload: T) -> Result<usize> {
        self.publish_with_metadata(topic, payload, EventMetadata::new())
            .await
    }

    /// Publishes an event with custom metadata concurrently.
    pub async fn publish_with_metadata(
        &self,
        topic: impl Into<Arc<str>>,
        payload: T,
        metadata: EventMetadata,
    ) -> Result<usize> {
        let envelope = Arc::new(EventEnvelope {
            id: Uuid::new_v4(),
            topic: topic.into(),
            payload,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
            metadata,
        });

        let matching: Vec<EventCallback<T>> = self
            .subscriptions
            .iter()
            .filter(|entry| entry.value().subscription.matches(&envelope.topic))
            .map(|entry| Arc::clone(&entry.value().callback))
            .collect();

        let notified = matching.len();
        let mut handles = Vec::with_capacity(notified);

        for callback in matching {
            let env = Arc::clone(&envelope);
            handles.push(tokio::spawn(async move {
                (callback)(env).await;
            }));
        }

        for handle in handles {
            let _ = handle.await;
        }

        Ok(notified)
    }

    /// Publishes a pre-built envelope to all matching subscribers concurrently.
    ///
    /// This is used by `PersistentBus` to re-deliver stored events.
    pub async fn publish_with_envelope(&self, envelope: EventEnvelope<T>) -> Result<usize> {
        let envelope = Arc::new(envelope);

        let matching: Vec<EventCallback<T>> = self
            .subscriptions
            .iter()
            .filter(|entry| entry.value().subscription.matches(&envelope.topic))
            .map(|entry| Arc::clone(&entry.value().callback))
            .collect();

        let notified = matching.len();
        let mut handles = Vec::with_capacity(notified);

        for callback in matching {
            let env = Arc::clone(&envelope);
            handles.push(tokio::spawn(async move {
                (callback)(env).await;
            }));
        }

        for handle in handles {
            let _ = handle.await;
        }

        Ok(notified)
    }

    /// Publishes an event to all matching subscribers sequentially.
    ///
    /// Each subscriber is awaited before moving to the next.
    /// Returns the number of subscribers notified.
    pub async fn publish_sequential(&self, topic: impl Into<Arc<str>>, payload: T) -> Result<usize> {
        self.publish_sequential_with_metadata(topic, payload, EventMetadata::new())
            .await
    }

    /// Publishes an event with custom metadata sequentially.
    pub async fn publish_sequential_with_metadata(
        &self,
        topic: impl Into<Arc<str>>,
        payload: T,
        metadata: EventMetadata,
    ) -> Result<usize> {
        let envelope = Arc::new(EventEnvelope {
            id: Uuid::new_v4(),
            topic: topic.into(),
            payload,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64,
            metadata,
        });

        let matching: Vec<EventCallback<T>> = self
            .subscriptions
            .iter()
            .filter(|entry| entry.value().subscription.matches(&envelope.topic))
            .map(|entry| Arc::clone(&entry.value().callback))
            .collect();

        let notified = matching.len();

        for callback in matching {
            (callback)(Arc::clone(&envelope)).await;
        }

        Ok(notified)
    }

    /// Subscribes to a topic pattern with an async callback.
    ///
    /// Returns a `Subscription` handle that can be used to unsubscribe.
    pub async fn subscribe(
        &self,
        topic_pattern: impl Into<Arc<str>>,
        callback: impl Fn(Arc<EventEnvelope<T>>) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync
        + 'static,
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

        subscription
    }

    /// Subscribes to a topic pattern with a synchronous callback.
    ///
    /// The synchronous callback is wrapped in an async adapter.
    /// Returns a `Subscription` handle that can be used to unsubscribe.
    pub async fn subscribe_sync(
        &self,
        topic_pattern: impl Into<Arc<str>>,
        callback: impl Fn(Arc<EventEnvelope<T>>) + Send + Sync + 'static,
    ) -> Subscription {
        let subscription = Subscription::new(topic_pattern);
        let sub_id = subscription.id;

        self.subscriptions.insert(
            sub_id,
            SubscriptionEntry {
                subscription: subscription.clone(),
                callback: Arc::new(move |env| {
                    callback(env);
                    Box::pin(async {})
                }),
            },
        );

        subscription
    }

    /// Unsubscribes by subscription ID.
    ///
    /// Returns true if the subscription was found and removed.
    pub fn unsubscribe(&self, subscription_id: Uuid) -> bool {
        if let Some(mut entry) = self.subscriptions.remove(&subscription_id) {
            entry.1.subscription.deactivate();
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

    #[tokio::test]
    async fn test_publish_subscribe() {
        let bus = EventBus::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        bus.subscribe_sync("orders.created", move |_event| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        })
        .await;

        let notified = bus
            .publish("orders.created", "payload".to_string())
            .await
            .unwrap();
        assert_eq!(notified, 1);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_wildcard_subscription() {
        let bus = EventBus::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        bus.subscribe_sync("orders.*", move |_event| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        })
        .await;

        bus.publish("orders.created", "1".to_string())
            .await
            .unwrap();
        bus.publish("orders.cancelled", "2".to_string())
            .await
            .unwrap();
        bus.publish("payments.created", "3".to_string())
            .await
            .unwrap();

        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_unsubscribe() {
        let bus = EventBus::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let sub = bus
            .subscribe_sync("orders.*", move |_event| {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            })
            .await;

        bus.publish("orders.created", "1".to_string())
            .await
            .unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        assert!(bus.unsubscribe(sub.id));
        bus.publish("orders.created", "2".to_string())
            .await
            .unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let bus = EventBus::new();
        let counter1 = Arc::new(AtomicUsize::new(0));
        let counter2 = Arc::new(AtomicUsize::new(0));
        let c1 = counter1.clone();
        let c2 = counter2.clone();

        bus.subscribe_sync("orders.*", move |_| {
            c1.fetch_add(1, Ordering::SeqCst);
        })
        .await;
        bus.subscribe_sync("orders.*", move |_| {
            c2.fetch_add(1, Ordering::SeqCst);
        })
        .await;

        bus.publish("orders.created", "1".to_string())
            .await
            .unwrap();

        assert_eq!(counter1.load(Ordering::SeqCst), 1);
        assert_eq!(counter2.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_subscription_count() {
        let bus = EventBus::<String>::new();
        assert_eq!(bus.subscription_count(), 0);

        let _sub1 = bus.subscribe_sync("a.*", |_| {}).await;
        let _sub2 = bus.subscribe_sync("b.*", |_| {}).await;
        assert_eq!(bus.subscription_count(), 2);
    }

    #[tokio::test]
    async fn test_async_callback() {
        let bus = EventBus::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        bus.subscribe("orders.*", move |_event| {
            let counter = counter_clone.clone();
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
            }) as Pin<Box<dyn Future<Output = ()> + Send>>
        })
        .await;

        let notified = bus
            .publish("orders.created", "payload".to_string())
            .await
            .unwrap();
        assert_eq!(notified, 1);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_publish_sequential() {
        let bus = EventBus::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        bus.subscribe_sync("orders.*", move |_event| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        })
        .await;

        let notified = bus
            .publish_sequential("orders.created", "payload".to_string())
            .await
            .unwrap();
        assert_eq!(notified, 1);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }
}
