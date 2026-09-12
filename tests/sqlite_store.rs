// Tests exercise durable storage; unwrap/expect is the test signal.
#![allow(clippy::unwrap_used, clippy::expect_used)]
#![cfg(feature = "sqlite")]

//! SQLite store integration tests — fully local, no services required.
//!
//! The in-module unit tests cover an in-memory DB; these exercise the
//! on-disk path in a tempdir: durability across a full store drop/reopen,
//! id sequencing resumption, binary payload integrity, topic and timestamp
//! filters, and retention cleanup. Plus the at-least-once redelivery story
//! of [`PersistentBus`] end to end: persist → subscriber attaches later →
//! replay re-delivers the backlog, and a failed persist never delivers.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use typed_eventbus::persistence::{PersistedEvent, SqliteStore};
use typed_eventbus::store::{EventStore, InMemoryStore};
use typed_eventbus::{EventBus, EventEnvelope, PersistentBus};

fn temp_store(tag: &str) -> (tempfile::TempDir, SqliteStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = SqliteStore::new(dir.path().join(format!("{tag}.db"))).unwrap();
    (dir, store)
}

// ---------------------------------------------------------------------------
// Durability across drop/reopen
// ---------------------------------------------------------------------------

#[test]
fn events_survive_full_store_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("events.db");

    {
        let store = SqliteStore::new(&db).unwrap();
        store.store("orders.created", b"order-1").unwrap();
        store.store("orders.created", b"order-2").unwrap();
        store.store("payments.settled", b"pay-1").unwrap();
    }

    let store = SqliteStore::new(&db).unwrap();
    assert_eq!(store.get_events("orders.created", 0).unwrap().len(), 2);
    assert_eq!(store.get_events("payments.settled", 0).unwrap().len(), 1);
    let latest = store.get_latest("orders.created").unwrap().unwrap();
    assert_eq!(latest.payload, b"order-2");
}

#[test]
fn id_sequencing_resumes_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("seq.db");

    let last_id = {
        let store = SqliteStore::new(&db).unwrap();
        let mut last = 0;
        for i in 0..5 {
            last = store.store("t", format!("p{i}").as_bytes()).unwrap();
        }
        last
    };
    assert_eq!(last_id, 5);

    let store = SqliteStore::new(&db).unwrap();
    assert_eq!(
        store.store("t", b"p5").unwrap(),
        6,
        "next id continues past the reopened max"
    );
}

#[test]
fn reopened_store_does_not_overwrite_existing_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("ids.db");
    {
        let store = SqliteStore::new(&db).unwrap();
        store.store("t", b"a").unwrap();
    }
    let store = SqliteStore::new(&db).unwrap();
    let id = store.store("t", b"b").unwrap();
    assert_eq!(id, 2);
    let events = store.get_events("t", 0).unwrap();
    assert_eq!(
        events.len(),
        2,
        "both rows present — the reused counter wrote a fresh row"
    );
    assert_eq!(events[0].payload, b"a");
    assert_eq!(events[1].payload, b"b");
}

// ---------------------------------------------------------------------------
// Filters, payloads, retention
// ---------------------------------------------------------------------------

#[test]
fn topic_and_timestamp_filters_on_disk_db() {
    let (_dir, store) = temp_store("filters");
    store.store("orders.created", b"1").unwrap();
    store.store("orders.cancelled", b"2").unwrap();
    store.store("orders.created", b"3").unwrap();

    let created = store.get_events("orders.created", 0).unwrap();
    assert_eq!(created.len(), 2);
    assert!(
        created
            .windows(2)
            .all(|w| w[0].timestamp_ms <= w[1].timestamp_ms)
    );

    // Future cutoff excludes everything.
    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 10_000;
    assert!(
        store
            .get_events("orders.created", future)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn binary_payloads_round_trip_intact() {
    let (_dir, store) = temp_store("binary");
    let blob: Vec<u8> = (0..=255u8).cycle().take(4096).collect();
    store.store("blobs", &blob).unwrap();
    let got = store.get_latest("blobs").unwrap().unwrap();
    assert_eq!(got.payload.len(), 4096);
    assert_eq!(got.payload, blob);
}

#[test]
fn cleanup_before_timestamp_removes_only_old_rows() {
    let (_dir, store) = temp_store("cleanup");
    store.store("t", b"old-1").unwrap();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let deleted = store.cleanup(now_ms + 1_000).unwrap();
    assert_eq!(deleted, 1);
    assert!(store.get_events("t", 0).unwrap().is_empty());

    // The store stays writable after cleanup and keeps its id sequence.
    let id = store.store("t", b"new").unwrap();
    assert_eq!(id, 2);
    assert_eq!(store.get_events("t", 0).unwrap()[0].payload, b"new");
}

#[test]
fn persisted_event_shape_is_stable() {
    let (_dir, store) = temp_store("shape");
    let id = store.store("shape.topic", b"payload").unwrap();
    let got: PersistedEvent = store.get_events("shape.topic", 0).unwrap().remove(0);
    assert_eq!(got.id, id);
    assert_eq!(got.topic, "shape.topic");
    assert!(
        got.timestamp_ms > 1_600_000_000_000,
        "timestamp is epoch millis"
    );
}

// ---------------------------------------------------------------------------
// At-least-once redelivery through PersistentBus
// ---------------------------------------------------------------------------

#[tokio::test]
async fn late_subscriber_receives_history_via_replay() {
    let bus = Arc::new(PersistentBus::new(
        EventBus::new(),
        Arc::new(InMemoryStore::<String>::new()),
    ));

    // Publish before anyone is listening.
    bus.publish("orders.created", "o1".into()).await.unwrap();
    bus.publish("orders.created", "o2".into()).await.unwrap();

    // A subscriber attaches afterwards; replay re-delivers the backlog.
    let received = Arc::new(AtomicUsize::new(0));
    let rx = received.clone();
    bus.subscribe("orders.**", move |_| {
        rx.fetch_add(1, Ordering::SeqCst);
    })
    .await;

    let redelivered = bus.replay("orders.**").await.unwrap();
    assert_eq!(redelivered, 2, "both historical events are redelivered");
    assert_eq!(
        received.load(Ordering::SeqCst),
        2,
        "subscriber saw the full backlog"
    );
}

#[tokio::test]
async fn replay_since_delivers_only_events_after_the_offset() {
    let bus = PersistentBus::new(EventBus::new(), Arc::new(InMemoryStore::<i64>::new()));

    bus.publish("t", 1).await.unwrap();
    // Timestamps are epoch millis; gap the publishes so the offset lands
    // strictly between event 1 and event 2.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let offset = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    bus.publish("t", 2).await.unwrap();
    bus.publish("t", 3).await.unwrap();

    let received = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let rx = received.clone();
    let log = seen.clone();
    bus.subscribe("t", move |env| {
        rx.fetch_add(1, Ordering::SeqCst);
        log.lock().unwrap().push(env.payload);
    })
    .await;

    assert_eq!(
        bus.replay_since(offset).await.unwrap(),
        2,
        "only events at/after the offset"
    );
    let mut got = seen.lock().unwrap().clone();
    got.sort();
    assert_eq!(
        got,
        vec![2, 3],
        "event 1 (before the offset) is not redelivered"
    );
    assert_eq!(received.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn failed_persistence_never_delivers() {
    // If the store rejects the append, the event must not reach
    // subscribers — persistence precedes delivery.
    struct RejectingStore;

    #[async_trait::async_trait]
    impl EventStore<String> for RejectingStore {
        async fn append(&self, _e: &EventEnvelope<String>) -> typed_eventbus::Result<()> {
            Err(typed_eventbus::EventBusError::Store("disk full".into()))
        }
        async fn load_since(&self, _s: i64) -> typed_eventbus::Result<Vec<EventEnvelope<String>>> {
            Ok(vec![])
        }
        async fn load_by_topic(
            &self,
            _p: &str,
        ) -> typed_eventbus::Result<Vec<EventEnvelope<String>>> {
            Ok(vec![])
        }
        async fn load_all(&self) -> typed_eventbus::Result<Vec<EventEnvelope<String>>> {
            Ok(vec![])
        }
    }

    let bus = PersistentBus::new(EventBus::new(), Arc::new(RejectingStore));
    let received = Arc::new(AtomicUsize::new(0));
    let rx = received.clone();
    bus.subscribe("t", move |_| {
        rx.fetch_add(1, Ordering::SeqCst);
    })
    .await;

    let result = bus.publish("t", "gone".into()).await;
    assert!(result.is_err(), "store failure must fail the publish");
    assert_eq!(
        received.load(Ordering::SeqCst),
        0,
        "no delivery when persistence fails"
    );
}

#[tokio::test]
async fn replay_redelivers_after_restart_simulation() {
    // Simulate process restart: a fresh bus over a store that already has
    // history re-delivers to its new subscribers.
    let store = Arc::new(InMemoryStore::<String>::new());
    {
        let old_bus = PersistentBus::new(EventBus::new(), store.clone());
        old_bus.publish("jobs.enqueued", "j1".into()).await.unwrap();
        old_bus.publish("jobs.enqueued", "j2".into()).await.unwrap();
        old_bus
            .publish("jobs.done", "j1-done".into())
            .await
            .unwrap();
    }

    let new_bus = PersistentBus::new(EventBus::new(), store);
    let received = Arc::new(AtomicUsize::new(0));
    let rx = received.clone();
    new_bus
        .subscribe("jobs.enqueued", move |_| {
            rx.fetch_add(1, Ordering::SeqCst);
        })
        .await;

    let n = new_bus.replay("jobs.enqueued").await.unwrap();
    assert_eq!(n, 2);
    assert_eq!(
        received.load(Ordering::SeqCst),
        2,
        "only the matching topic replays"
    );
}
