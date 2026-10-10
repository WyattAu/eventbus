use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use dashmap::DashMap;
use uuid::Uuid;

use crate::backpressure::{DeliveryReport, Enqueue, OverflowPolicy, SubscriberStats};
use crate::envelope::{EventEnvelope, EventMetadata};
use crate::error::Result;
use crate::queue::{BoundedQueue, spawn_worker};
use crate::subscription::Subscription;

/// An async callback that handles events. Envelope is shared via `Arc` to avoid
/// per-subscriber heap clones of the payload / metadata.
pub type EventCallback<T> =
    Arc<dyn Fn(Arc<EventEnvelope<T>>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// An async event bus supporting typed pub/sub with wildcard subscriptions.
pub struct EventBus<T: Clone + Send + Sync + 'static> {
    subscriptions: DashMap<Uuid, SubscriptionEntry<T>>,
}

/// How a subscriber receives envelopes.
///
/// `Awaited` is the original behaviour: the publisher spawns the callback and
/// waits for it, so publisher latency is the slowest subscriber's latency.
/// `Bounded` keeps the same callbacks but hands them a queue with a worker,
/// which decouples the two.
#[derive(Clone)]
enum Delivery<T: Clone + Send + Sync + 'static> {
    Awaited(EventCallback<T>),
    Bounded(Arc<BoundedQueue<T>>),
}

struct SubscriptionEntry<T: Clone + Send + Sync + 'static> {
    subscription: Subscription,
    delivery: Delivery<T>,
}

/// Fans an envelope out according to each subscriber's delivery mode.
///
/// `Awaited` subscribers are spawned and awaited together, preserving the
/// existing publish contract; `Bounded` subscribers are enqueued and never
/// waited on. Returns the number of matching subscribers.
async fn dispatch<T: Clone + Send + Sync + 'static>(
    deliveries: Vec<Delivery<T>>,
    envelope: &Arc<EventEnvelope<T>>,
    sequential: bool,
) -> usize {
    let mut handles = Vec::new();
    let mut notified = 0;

    for delivery in deliveries {
        match delivery {
            Delivery::Awaited(callback) => {
                notified += 1;
                if sequential {
                    callback(Arc::clone(envelope)).await;
                } else {
                    let env = Arc::clone(envelope);
                    handles.push(tokio::spawn(async move {
                        callback(env).await;
                    }));
                }
            }
            Delivery::Bounded(queue) => {
                notified += 1;
                queue.push(Arc::clone(envelope));
            }
        }
    }

    for handle in handles {
        let _ = handle.await;
    }
    notified
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

        let matching = self.matching_deliveries(&envelope.topic);
        Ok(dispatch(matching, &envelope, false).await)
    }

    /// Publishes a pre-built envelope to all matching subscribers concurrently.
    ///
    /// This is used by `PersistentBus` to re-deliver stored events.
    pub async fn publish_with_envelope(&self, envelope: EventEnvelope<T>) -> Result<usize> {
        let envelope = Arc::new(envelope);

        let matching = self.matching_deliveries(&envelope.topic);
        Ok(dispatch(matching, &envelope, false).await)
    }

    /// Publishes an event to all matching subscribers sequentially.
    ///
    /// Each subscriber is awaited before moving to the next.
    /// Returns the number of subscribers notified.
    pub async fn publish_sequential(
        &self,
        topic: impl Into<Arc<str>>,
        payload: T,
    ) -> Result<usize> {
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

        let matching = self.matching_deliveries(&envelope.topic);
        Ok(dispatch(matching, &envelope, true).await)
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
                delivery: Delivery::Awaited(Arc::new(callback)),
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
                delivery: Delivery::Awaited(Arc::new(move |env| {
                    callback(env);
                    Box::pin(async {})
                })),
            },
        );

        subscription
    }

    /// Unsubscribes by subscription ID.
    ///
    /// Returns true if the subscription was found and removed.
    pub fn unsubscribe(&self, subscription_id: Uuid) -> bool {
        if let Some((_, mut entry)) = self.subscriptions.remove(&subscription_id) {
            entry.subscription.deactivate();
            if let Delivery::Bounded(queue) = &entry.delivery {
                queue.close();
            }
            true
        } else {
            false
        }
    }

    /// Subscribes with a bounded queue so the publisher is never blocked.
    ///
    /// Unlike [`Self::subscribe`], a slow callback cannot slow the publisher
    /// down: the envelope goes into a queue of at most `capacity` items and a
    /// worker drains it. What happens when the queue is full is decided by
    /// `policy`, not by whichever subscriber happens to be slowest.
    ///
    /// Use [`Self::stats`] to watch the queue depth, drops, and panics.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use typed_eventbus::{EventBus, OverflowPolicy};
    /// # async fn run() {
    /// let bus: EventBus<String> = EventBus::new();
    /// bus.subscribe_bounded("orders.**", 128, OverflowPolicy::DropOldest, |event| {
    ///     Box::pin(async move { let _ = event.payload; })
    /// })
    /// .await;
    /// # }
    /// ```
    pub async fn subscribe_bounded(
        &self,
        topic_pattern: impl Into<Arc<str>>,
        capacity: usize,
        policy: OverflowPolicy,
        callback: impl Fn(Arc<EventEnvelope<T>>) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync
        + 'static,
    ) -> Subscription {
        let subscription = Subscription::new(topic_pattern);
        let stats = Arc::new(SubscriberStats::new());
        let queue = Arc::new(BoundedQueue::new(capacity, policy, Arc::clone(&stats)));

        self.subscriptions.insert(
            subscription.id,
            SubscriptionEntry {
                subscription: subscription.clone(),
                delivery: Delivery::Bounded(Arc::clone(&queue)),
            },
        );
        spawn_worker(queue, Arc::new(callback));
        subscription
    }

    /// Counters for a bounded subscriber.
    ///
    /// Returns `None` for an unknown id or for a subscriber registered with
    /// [`Self::subscribe`], which has no queue.
    #[must_use]
    pub fn stats(&self, subscription_id: uuid::Uuid) -> Option<Arc<SubscriberStats>> {
        let entry = self.subscriptions.get(&subscription_id)?;
        match &entry.delivery {
            Delivery::Bounded(queue) => Some(queue.stats()),
            Delivery::Awaited(_) => None,
        }
    }

    /// Publishes and reports exactly what each subscriber's queue did.
    ///
    /// [`Self::publish`] reports only how many subscribers matched. Use this
    /// when the producer needs to know whether anything was lost — the
    /// difference between a producer that reacts and one that discovers the
    /// backlog later, in a consumer's queue depth.
    pub async fn publish_report(
        &self,
        topic: impl Into<Arc<str>>,
        payload: T,
    ) -> Result<DeliveryReport> {
        self.publish_report_with_metadata(topic, payload, EventMetadata::new())
            .await
    }

    /// Publishes with custom metadata and reports delivery outcomes.
    pub async fn publish_report_with_metadata(
        &self,
        topic: impl Into<Arc<str>>,
        payload: T,
        metadata: EventMetadata,
    ) -> Result<DeliveryReport> {
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

        let mut report = DeliveryReport::default();
        for delivery in self.matching_deliveries(&envelope.topic) {
            report.matched += 1;
            match delivery {
                Delivery::Bounded(queue) => match queue.push(Arc::clone(&envelope)) {
                    Enqueue::Delivered => report.enqueued += 1,
                    Enqueue::DroppedOldest | Enqueue::DroppedNewest => report.dropped += 1,
                    Enqueue::Rejected => report.rejected += 1,
                },
                Delivery::Awaited(callback) => {
                    // Must actually deliver: reporting a subscriber as
                    // enqueued that never received the envelope would be a
                    // silent drop wearing a success report.
                    callback(Arc::clone(&envelope)).await;
                    report.enqueued += 1;
                }
            }
        }
        Ok(report)
    }

    fn matching_deliveries(&self, topic: &str) -> Vec<Delivery<T>> {
        self.subscriptions
            .iter()
            .filter(|entry| entry.value().subscription.matches(topic))
            .map(|entry| entry.value().delivery.clone())
            .collect()
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
    #![allow(clippy::unwrap_used)]
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
