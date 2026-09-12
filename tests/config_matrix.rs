// Config-knob behavior matrix: every public config knob on the bus must
// observably change what subscribers receive — default vs configured must
// differ. All tests are fully deterministic (no clocks, no sleeps).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use typed_eventbus::envelope::EventMetadata;
use typed_eventbus::store::{EventStore, InMemoryStore};
use typed_eventbus::{EventBus, EventEnvelope, PersistentBus};

type Payload = String;

async fn capture_envelopes(
    bus: &EventBus<Payload>,
    pattern: &str,
) -> Arc<std::sync::Mutex<Vec<EventEnvelope<Payload>>>> {
    let captured: Arc<std::sync::Mutex<Vec<EventEnvelope<Payload>>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = captured.clone();
    bus.subscribe_sync(pattern, move |envelope| {
        sink.lock().unwrap().push((*envelope).clone());
    })
    .await;
    captured
}

// ---------------------------------------------------------------------------
// knob: EventEnvelope::with_metadata — delivery must carry configured
// metadata; the default (no metadata) delivers an empty map
// ---------------------------------------------------------------------------

#[tokio::test]
async fn knob_with_metadata_reaches_subscribers_through_the_bus() {
    let bus: EventBus<Payload> = EventBus::new();
    let configured = capture_envelopes(&bus, "orders.*").await;

    let mut meta = EventMetadata::new();
    meta.insert("tenant".into(), serde_json::json!("acme"));
    meta.insert("attempt".into(), serde_json::json!(2));

    let notified = bus
        .publish_with_metadata("orders.created", "payload-1".to_string(), meta)
        .await
        .unwrap();
    assert_eq!(notified, 1);

    let got = configured.lock().unwrap().pop().expect("delivery");
    assert_eq!(
        got.metadata.get("tenant"),
        Some(&serde_json::json!("acme")),
        "configured metadata must be delivered verbatim"
    );
    assert_eq!(got.metadata.get("attempt"), Some(&serde_json::json!(2)));

    // Default contrast: publishing without metadata delivers an empty map.
    let notified = bus
        .publish("orders.created", "payload-2".to_string())
        .await
        .unwrap();
    assert_eq!(notified, 1);
    let got = configured.lock().unwrap().pop().expect("delivery");
    assert!(
        got.metadata.is_empty(),
        "default publish must carry no metadata"
    );
}

#[tokio::test]
async fn knob_with_metadata_survives_persistent_replay() {
    // The builder setter on a hand-built envelope (the replay path used by
    // PersistentBus) must deliver the metadata to live subscribers.
    let bus: EventBus<Payload> = EventBus::new();
    let captured = capture_envelopes(&bus, "replay.**").await;

    let store = Arc::new(InMemoryStore::new());
    let persistent = PersistentBus::new(bus, store.clone());

    let envelope = EventEnvelope::new("replay.backfill", "stored".to_string())
        .with_metadata("origin", serde_json::json!("migration"));
    EventStore::append(store.as_ref(), &envelope).await.unwrap();

    persistent.replay("replay.**").await.unwrap();

    let got = captured.lock().unwrap().pop().expect("replayed delivery");
    assert_eq!(
        got.metadata.get("origin"),
        Some(&serde_json::json!("migration")),
        "replay must preserve envelope metadata set via with_metadata"
    );
    assert_eq!(got.payload, "stored");
}

// ---------------------------------------------------------------------------
// knob: subscription topic pattern — the pattern decides the delivery set
// ---------------------------------------------------------------------------

#[tokio::test]
async fn knob_topic_pattern_changes_the_delivery_set() {
    let bus: EventBus<Payload> = EventBus::new();
    let exact = capture_envelopes(&bus, "orders.created").await;
    let single_wild = capture_envelopes(&bus, "orders.*").await;
    let deep_wild = capture_envelopes(&bus, "orders.**").await;

    // One publish to a deep topic: only the `**` pattern matches.
    let notified = bus
        .publish("orders.created.v2", "deep".to_string())
        .await
        .unwrap();
    assert_eq!(
        notified, 1,
        "exactly one subscriber must match a deep topic"
    );

    assert_eq!(
        exact.lock().unwrap().len(),
        0,
        "exact pattern must not match"
    );
    assert_eq!(
        single_wild.lock().unwrap().len(),
        0,
        "* must match exactly one segment"
    );
    assert_eq!(
        deep_wild.lock().unwrap().len(),
        1,
        "** must match across segments"
    );

    // One publish to a shallow topic: exact + both wildcards match.
    let notified = bus
        .publish("orders.created", "shallow".to_string())
        .await
        .unwrap();
    assert_eq!(notified, 3);

    assert_eq!(exact.lock().unwrap().len(), 1);
    assert_eq!(single_wild.lock().unwrap().len(), 1);
    assert_eq!(deep_wild.lock().unwrap().len(), 2);
}
