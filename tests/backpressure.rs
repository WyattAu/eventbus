//! Bounded-queue delivery: backpressure policy, isolation, and observability.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use typed_eventbus::{EventBus, OverflowPolicy};

#[tokio::test]
async fn bounded_subscriber_receives_events() {
    let bus: EventBus<String> = EventBus::new();
    let seen = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&seen);

    bus.subscribe_bounded("orders.**", 16, OverflowPolicy::DropOldest, move |_event| {
        let counter = Arc::clone(&counter);
        Box::pin(async move {
            counter.fetch_add(1, Ordering::SeqCst);
        })
    })
    .await;

    bus.publish("orders.created", "a".to_string())
        .await
        .unwrap();
    bus.publish("orders.shipped.v2", "b".to_string())
        .await
        .unwrap();
    bus.publish("payments.created", "c".to_string())
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(seen.load(Ordering::SeqCst), 2, "wildcard match failed");
}

#[tokio::test]
async fn slow_callback_does_not_delay_publisher() {
    let bus: EventBus<String> = EventBus::new();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));

    let held = Arc::clone(&gate);
    bus.subscribe_bounded(
        "orders.**",
        1024,
        OverflowPolicy::DropOldest,
        move |_event| {
            let held = Arc::clone(&held);
            Box::pin(async move {
                let _permit = held.acquire().await;
            })
        },
    )
    .await;

    let started = Instant::now();
    for i in 0..200 {
        bus.publish("orders.created", format!("e{i}"))
            .await
            .unwrap();
    }
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_millis(500),
        "publish waited on the slow callback: {elapsed:?}"
    );
    assert_eq!(
        bus.publish("orders.created", "x".to_string())
            .await
            .unwrap(),
        1
    );
    gate.add_permits(1);
}

#[tokio::test]
async fn awaited_subscriber_still_blocks_publisher() {
    // Guards the contrast that motivates bounded delivery: the original
    // publish path waits for every subscriber. If this ever stops being true
    // the two modes have converged and the distinction is pointless.
    let bus: EventBus<String> = EventBus::new();
    bus.subscribe("orders.**", |_event| {
        Box::pin(async { tokio::time::sleep(Duration::from_millis(300)).await })
    })
    .await;

    let started = Instant::now();
    bus.publish("orders.created", "a".to_string())
        .await
        .unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(250),
        "awaited subscriber no longer blocks publish; the regression test is stale"
    );
}

#[tokio::test]
async fn drop_oldest_keeps_the_freshest_events() {
    let bus: EventBus<String> = EventBus::new();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    let held = Arc::clone(&gate);
    let sender = tx.clone();
    bus.subscribe_bounded("orders.**", 2, OverflowPolicy::DropOldest, move |event| {
        let held = Arc::clone(&held);
        let sender = sender.clone();
        Box::pin(async move {
            let _permit = held.acquire().await;
            let _ = sender.send(event.payload.clone());
        })
    })
    .await;

    let sub = bus
        .subscribe_bounded("orders.**", 2, OverflowPolicy::DropOldest, |event| {
            Box::pin(async move {
                let _ = event.payload.clone();
            })
        })
        .await;
    bus.unsubscribe(sub.id);

    bus.publish("orders.created", "e0".to_string())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await; // worker parks on e0

    for i in 1..5 {
        bus.publish("orders.created", format!("e{i}"))
            .await
            .unwrap();
    }
    gate.add_permits(3);

    let mut seen = Vec::new();
    while let Some(e) = rx.recv().await {
        seen.push(e);
        if seen.len() == 3 {
            break;
        }
    }
    assert_eq!(seen, vec!["e0", "e3", "e4"], "queue kept the oldest events");
}

#[tokio::test]
async fn drop_newest_keeps_the_oldest_queued() {
    let bus: EventBus<String> = EventBus::new();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    let held = Arc::clone(&gate);
    let sender = tx.clone();
    bus.subscribe_bounded("orders.**", 2, OverflowPolicy::DropNewest, move |event| {
        let held = Arc::clone(&held);
        let sender = sender.clone();
        Box::pin(async move {
            let _permit = held.acquire().await;
            let _ = sender.send(event.payload.clone());
        })
    })
    .await;

    bus.publish("orders.created", "e0".to_string())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    for i in 1..5 {
        bus.publish("orders.created", format!("e{i}"))
            .await
            .unwrap();
    }
    gate.add_permits(3);

    let mut seen = Vec::new();
    while let Some(e) = rx.recv().await {
        seen.push(e);
        if seen.len() == 3 {
            break;
        }
    }
    assert_eq!(seen, vec!["e0", "e1", "e2"], "DropNewest must keep history");
}

#[tokio::test]
async fn reject_policy_reports_backpressure_to_the_producer() {
    let bus: EventBus<String> = EventBus::new();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));

    let held = Arc::clone(&gate);
    bus.subscribe_bounded("orders.**", 1, OverflowPolicy::Reject, move |_event| {
        let held = Arc::clone(&held);
        Box::pin(async move {
            let _permit = held.acquire().await;
        })
    })
    .await;

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut rejected = 0;
    for i in 0..6 {
        let report = bus
            .publish_report("orders.created", format!("e{i}"))
            .await
            .unwrap();
        assert_eq!(report.matched, 1);
        if report.rejected == 1 {
            rejected += 1;
        }
    }
    assert!(
        rejected >= 3,
        "Reject did not report backpressure: {rejected}"
    );
    gate.add_permits(1);
}

#[tokio::test]
async fn panicking_callback_is_counted_and_delivery_continues() {
    let bus: EventBus<String> = EventBus::new();
    let sub = bus
        .subscribe_bounded("orders.**", 8, OverflowPolicy::DropOldest, |event| {
            Box::pin(async move {
                assert_ne!(event.payload, "boom", "callback exploded");
            })
        })
        .await;

    for payload in ["boom", "boom", "fine"] {
        bus.publish("orders.created", payload.to_string())
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(250)).await;

    let stats = bus.stats(sub.id).expect("bounded subscriber has stats");
    assert_eq!(stats.panicked(), 2, "panics were silently swallowed");
    assert_eq!(stats.delivered(), 1, "worker stopped after a panic");
}

#[tokio::test]
async fn queue_depth_is_observable_while_a_callback_is_blocked() {
    let bus: EventBus<String> = EventBus::new();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));

    let held = Arc::clone(&gate);
    let sub = bus
        .subscribe_bounded("orders.**", 4, OverflowPolicy::DropOldest, move |_event| {
            let held = Arc::clone(&held);
            Box::pin(async move {
                let _permit = held.acquire().await;
            })
        })
        .await;

    for i in 0..3 {
        bus.publish("orders.created", format!("e{i}"))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(120)).await;

    let stats = bus.stats(sub.id).unwrap();
    assert!(
        stats.queue_depth() > 0,
        "a backlog that cannot be seen is the failure this kit exists to fix"
    );
    gate.add_permits(8);
}

#[tokio::test]
async fn unsubscribe_stops_the_worker() {
    let bus: EventBus<String> = EventBus::new();
    let seen = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&seen);

    let sub = bus
        .subscribe_bounded("orders.**", 16, OverflowPolicy::DropOldest, move |_event| {
            let counter = Arc::clone(&counter);
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
            })
        })
        .await;

    bus.publish("orders.created", "a".to_string())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(seen.load(Ordering::SeqCst), 1);

    assert!(bus.unsubscribe(sub.id));
    bus.publish("orders.created", "b".to_string())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;

    assert_eq!(
        seen.load(Ordering::SeqCst),
        1,
        "worker ran after unsubscribe"
    );
    assert!(bus.stats(sub.id).is_none());
}

#[tokio::test]
async fn awaited_and_bounded_subscribers_coexist() {
    let bus: EventBus<String> = EventBus::new();
    let sync_seen = Arc::new(AtomicUsize::new(0));
    let async_seen = Arc::new(AtomicUsize::new(0));

    let a = Arc::clone(&sync_seen);
    bus.subscribe("orders.**", move |_event| {
        let a = Arc::clone(&a);
        Box::pin(async move {
            a.fetch_add(1, Ordering::SeqCst);
        })
    })
    .await;

    let b = Arc::clone(&async_seen);
    let sub = bus
        .subscribe_bounded("orders.**", 16, OverflowPolicy::DropOldest, move |_event| {
            let b = Arc::clone(&b);
            Box::pin(async move {
                b.fetch_add(1, Ordering::SeqCst);
            })
        })
        .await;

    let report = bus
        .publish_report("orders.created", "x".to_string())
        .await
        .unwrap();
    assert_eq!(report.matched, 2);
    assert!(report.is_lossless());

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(sync_seen.load(Ordering::SeqCst), 1);
    assert_eq!(async_seen.load(Ordering::SeqCst), 1);
    assert!(bus.stats(sub.id).is_some());
}

#[tokio::test]
async fn awaited_subscriber_has_no_queue_stats() {
    let bus: EventBus<String> = EventBus::new();
    let sub = bus
        .subscribe("orders.**", |_event| Box::pin(async {}))
        .await;
    assert!(
        bus.stats(sub.id).is_none(),
        "an unbounded subscriber must not report queue state"
    );
}
