use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use typed_eventbus::store::{EventStore, InMemoryStore};
use typed_eventbus::{EventBus, PersistentBus};

#[tokio::test]
async fn in_memory_store_append_and_load_all() {
    let store = InMemoryStore::<String>::new();
    let env = typed_eventbus::EventEnvelope::new("test.topic", "hello".to_string());
    store.append(&env).await.unwrap();

    let all = store.load_all().await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].topic.as_ref(), "test.topic");
    assert_eq!(all[0].payload, "hello");
}

#[tokio::test]
async fn in_memory_store_load_since() {
    let store = InMemoryStore::<i32>::new();
    let mut e1 = typed_eventbus::EventEnvelope::new("a", 1i32);
    let mut e2 = typed_eventbus::EventEnvelope::new("b", 2i32);
    e1.timestamp = 100;
    e2.timestamp = 200;

    store.append(&e1).await.unwrap();
    store.append(&e2).await.unwrap();

    let since_150 = store.load_since(150).await.unwrap();
    assert_eq!(since_150.len(), 1);
    assert_eq!(since_150[0].payload, 2);

    let since_0 = store.load_since(0).await.unwrap();
    assert_eq!(since_0.len(), 2);
}

#[tokio::test]
async fn in_memory_store_load_by_topic() {
    let store = InMemoryStore::<&str>::new();
    store
        .append(&typed_eventbus::EventEnvelope::new("orders.created", "a"))
        .await
        .unwrap();
    store
        .append(&typed_eventbus::EventEnvelope::new("orders.cancelled", "b"))
        .await
        .unwrap();
    store
        .append(&typed_eventbus::EventEnvelope::new("payments.created", "c"))
        .await
        .unwrap();

    let orders = store.load_by_topic("orders.*").await.unwrap();
    assert_eq!(orders.len(), 2);

    let exact = store.load_by_topic("orders.created").await.unwrap();
    assert_eq!(exact.len(), 1);
    assert_eq!(exact[0].payload, "a");

    let all = store.load_by_topic("**").await.unwrap();
    assert_eq!(all.len(), 3);
}

#[tokio::test]
async fn persistent_bus_publish_and_replay() {
    let store = Arc::new(InMemoryStore::<String>::new());
    let bus = EventBus::<String>::new();
    let pbus = PersistentBus::new(bus, store.clone());

    pbus.publish("orders.created", "order-1".to_string())
        .await
        .unwrap();
    pbus.publish("orders.cancelled", "order-2".to_string())
        .await
        .unwrap();

    assert_eq!(store.load_all().await.unwrap().len(), 2);

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();
    pbus.subscribe("orders.*", move |_env| {
        counter_clone.fetch_add(1, Ordering::SeqCst);
    })
    .await;

    let replayed = pbus.replay("orders.*").await.unwrap();
    assert_eq!(replayed, 2);
    assert_eq!(counter.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn persistent_bus_replay_since() {
    let store = Arc::new(InMemoryStore::<String>::new());
    let bus = EventBus::<String>::new();
    let pbus = PersistentBus::new(bus, store.clone());

    let mut e1 = typed_eventbus::EventEnvelope::new("orders.created", "a".to_string());
    let mut e2 = typed_eventbus::EventEnvelope::new("orders.cancelled", "b".to_string());
    e1.timestamp = 100;
    e2.timestamp = 200;
    store.append(&e1).await.unwrap();
    store.append(&e2).await.unwrap();

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();
    pbus.subscribe("orders.*", move |_env| {
        counter_clone.fetch_add(1, Ordering::SeqCst);
    })
    .await;

    let replayed = pbus.replay_since(150).await.unwrap();
    assert_eq!(replayed, 1);
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn persistent_bus_topic_filtering() {
    let store = Arc::new(InMemoryStore::<String>::new());
    let bus = EventBus::<String>::new();
    let pbus = PersistentBus::new(bus, store.clone());

    pbus.publish("orders.created", "a".to_string())
        .await
        .unwrap();
    pbus.publish("payments.created", "b".to_string())
        .await
        .unwrap();
    pbus.publish("orders.cancelled", "c".to_string())
        .await
        .unwrap();

    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();
    pbus.subscribe("orders.*", move |_env| {
        counter_clone.fetch_add(1, Ordering::SeqCst);
    })
    .await;

    let replayed = pbus.replay("orders.*").await.unwrap();
    assert_eq!(replayed, 2);
    assert_eq!(counter.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn persistent_bus_subscriber_count() {
    let store = Arc::new(InMemoryStore::<String>::new());
    let bus = EventBus::<String>::new();
    let pbus = PersistentBus::new(bus, store);

    assert_eq!(pbus.subscriber_count(), 0);
    pbus.subscribe("a.*", |_| {}).await;
    pbus.subscribe("b.*", |_| {}).await;
    assert_eq!(pbus.subscriber_count(), 2);
}
