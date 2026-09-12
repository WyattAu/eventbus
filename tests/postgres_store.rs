// Tests talk to a real Postgres in docker; unwrap/expect is the test signal.
#![allow(clippy::unwrap_used, clippy::expect_used)]
#![cfg(feature = "postgres")]

//! Postgres store integration tests against a real Postgres server spun up
//! per run with testcontainers (docker required).
//!
//! ```sh
//! cargo test --features postgres --test postgres_store
//! ```
//!
//! Proves the full durable path on Postgres: migration, append/load via
//! [`PostgresStore`], timestamp offsets, wildcard topic replay, and
//! at-least-once redelivery through [`PersistentBus`] — the behaviors the
//! in-memory unit tests cannot exercise.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use typed_eventbus::store::EventStore;
use typed_eventbus::{EventBus, EventBusError, PersistentBus, PostgresStore};

async fn spawn_postgres() -> (testcontainers::ContainerAsync<Postgres>, sqlx::PgPool) {
    let container = Postgres::default().start().await.unwrap();
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .unwrap();
    (container, pool)
}

// ---------------------------------------------------------------------------
// Store CRUD over the wire
// ---------------------------------------------------------------------------

#[tokio::test]
async fn migrate_is_idempotent() {
    let (_container, pool) = spawn_postgres().await;
    PostgresStore::migrate(&pool).await.unwrap();
    PostgresStore::migrate(&pool).await.unwrap();
    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'eventbus_store'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(n, 1);
}

#[tokio::test]
async fn append_and_load_roundtrip() {
    let (_container, pool) = spawn_postgres().await;
    PostgresStore::migrate(&pool).await.unwrap();
    let store = PostgresStore::open(pool).await.unwrap();

    let e1 = typed_eventbus::EventEnvelope::new("orders.created", serde_json::json!({"id": 1}));
    let e2 = typed_eventbus::EventEnvelope::new("orders.created", serde_json::json!({"id": 2}));
    store.append(&e1).await.unwrap();
    store.append(&e2).await.unwrap();

    let all: Vec<typed_eventbus::EventEnvelope<serde_json::Value>> =
        store.load_all().await.unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].payload, serde_json::json!({"id": 1}));
    assert_eq!(all[1].topic.as_ref(), "orders.created");
}

#[tokio::test]
async fn load_since_filters_by_timestamp() {
    let (_container, pool) = spawn_postgres().await;
    PostgresStore::migrate(&pool).await.unwrap();
    let store = PostgresStore::open(pool).await.unwrap();

    let first = typed_eventbus::EventEnvelope::new("t", 1i64);
    store.append(&first).await.unwrap();
    let cutoff = first.timestamp;

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    store
        .append(&typed_eventbus::EventEnvelope::new("t", 2i64))
        .await
        .unwrap();

    let since: Vec<typed_eventbus::EventEnvelope<i64>> =
        store.load_since(cutoff + 1).await.unwrap();
    assert_eq!(since.len(), 1);
    assert_eq!(since[0].payload, 2);
    let all_since_zero: Vec<typed_eventbus::EventEnvelope<i64>> =
        store.load_since(0).await.unwrap();
    assert_eq!(all_since_zero.len(), 2);
}

#[tokio::test]
async fn load_by_topic_supports_wildcard_patterns() {
    let (_container, pool) = spawn_postgres().await;
    PostgresStore::migrate(&pool).await.unwrap();
    let store = PostgresStore::open(pool).await.unwrap();

    store
        .append(&typed_eventbus::EventEnvelope::new("orders.created", 1i64))
        .await
        .unwrap();
    store
        .append(&typed_eventbus::EventEnvelope::new(
            "orders.cancelled",
            2i64,
        ))
        .await
        .unwrap();
    store
        .append(&typed_eventbus::EventEnvelope::new(
            "payments.settled",
            3i64,
        ))
        .await
        .unwrap();

    let orders: Vec<typed_eventbus::EventEnvelope<i64>> =
        store.load_by_topic("orders.**").await.unwrap();
    assert_eq!(orders.len(), 2);
    let exact: Vec<typed_eventbus::EventEnvelope<i64>> =
        store.load_by_topic("payments.settled").await.unwrap();
    assert_eq!(exact.len(), 1);
}

#[tokio::test]
async fn append_persists_across_pool_reconnect() {
    // A brand-new store instance over a NEW pool to the same database sees
    // previously appended events (durability on the server, not in-process).
    let (container, pool) = spawn_postgres().await;
    PostgresStore::migrate(&pool).await.unwrap();
    {
        let store = PostgresStore::open(pool.clone()).await.unwrap();
        store
            .append(&typed_eventbus::EventEnvelope::new(
                "durable",
                "x".to_string(),
            ))
            .await
            .unwrap();
    }
    pool.close().await;

    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    let pool2 = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .unwrap();
    let store2 = PostgresStore::open(pool2).await.unwrap();
    let all: Vec<typed_eventbus::EventEnvelope<String>> = store2.load_all().await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].payload, "x");
}

#[tokio::test]
async fn store_errors_surface_as_eventbus_store_errors() {
    let (_container, pool) = spawn_postgres().await;
    // No migration: the table does not exist — append must fail with a
    // Store error rather than panicking.
    let store = PostgresStore::open(pool).await.unwrap();
    let err = store
        .append(&typed_eventbus::EventEnvelope::new("t", 1i64))
        .await
        .unwrap_err();
    assert!(matches!(err, EventBusError::Store(_)), "got: {err:?}");
}

// ---------------------------------------------------------------------------
// PersistentBus on Postgres: publish / consume / redelivery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn persistent_bus_publish_consume_and_replay() {
    let (_container, pool) = spawn_postgres().await;
    PostgresStore::migrate(&pool).await.unwrap();
    let store = Arc::new(PostgresStore::open(pool).await.unwrap());

    let bus = Arc::new(PersistentBus::<String>::new(EventBus::new(), store.clone()));

    let received = Arc::new(AtomicUsize::new(0));
    let rx = received.clone();
    bus.subscribe("events.**", move |_| {
        rx.fetch_add(1, Ordering::SeqCst);
    })
    .await;

    // Live delivery while subscribed.
    bus.publish("events.a", "one".to_string()).await.unwrap();
    bus.publish("events.b", "two".to_string()).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        received.load(Ordering::SeqCst),
        2,
        "live delivery to subscribers"
    );

    // Publish one event with nobody subscribed (drop the bus, new bus over
    // the same store simulates a crash/restart window).
    drop(bus);
    store
        .append(&typed_eventbus::EventEnvelope::new(
            "events.c",
            "three".to_string(),
        ))
        .await
        .unwrap();

    // New process: fresh bus over the same store; replay re-delivers all 3.
    let bus2 = PersistentBus::<String>::new(EventBus::new(), store);
    let redelivered = Arc::new(AtomicUsize::new(0));
    let rx2 = redelivered.clone();
    bus2.subscribe("events.**", move |_| {
        rx2.fetch_add(1, Ordering::SeqCst);
    })
    .await;
    let n = bus2.replay("events.**").await.unwrap();
    assert_eq!(n, 3, "all persisted events replay after restart");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        redelivered.load(Ordering::SeqCst),
        3,
        "subscriber consumed the full replay"
    );
}

#[tokio::test]
async fn concurrent_publishers_persist_every_event() {
    let (_container, pool) = spawn_postgres().await;
    PostgresStore::migrate(&pool).await.unwrap();
    let store = Arc::new(PostgresStore::open(pool).await.unwrap());
    let bus = Arc::new(PersistentBus::<String>::new(EventBus::new(), store));

    let mut handles = Vec::new();
    for w in 0..8u32 {
        let bus = bus.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..5u32 {
                bus.publish("load.test", format!("w{w}-e{i}"))
                    .await
                    .unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let total = bus.replay("load.test").await.unwrap();
    assert_eq!(
        total, 40,
        "every publish from every concurrent publisher persisted"
    );
}
