use criterion::{Criterion, criterion_group, criterion_main};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use typed_eventbus::EventBus;
use typed_eventbus::subscription::topic_matches;

fn bench_event_bus_creation(c: &mut Criterion) {
    c.bench_function("event_bus_creation", |b| {
        b.iter(|| EventBus::<String>::new());
    });
}

fn bench_subscribe_single(c: &mut Criterion) {
    c.bench_function("subscribe_single", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.iter_with_setup(
            || EventBus::<String>::new(),
            |bus| {
                rt.block_on(async {
                    let _sub = bus.subscribe_sync("orders.created", |_| {}).await;
                });
            },
        );
    });
}

fn bench_subscribe_multiple(c: &mut Criterion) {
    c.bench_function("subscribe_multiple", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        b.iter_with_setup(
            || {
                let bus = EventBus::<String>::new();
                rt.block_on(async {
                    let _s1 = bus.subscribe_sync("orders.*", |_| {}).await;
                    let _s2 = bus.subscribe_sync("payments.*", |_| {}).await;
                    let _s3 = bus.subscribe_sync("users.*", |_| {}).await;
                });
                bus
            },
            |bus| {
                let _ = bus.subscription_count();
            },
        );
    });
}

fn bench_publish_single_subscriber(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let bus = EventBus::<String>::new();
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();
    rt.block_on(async {
        bus.subscribe_sync("orders.created", move |_event| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        })
        .await;
    });

    c.bench_function("publish_single_subscriber", |b| {
        b.iter(|| {
            rt.block_on(async {
                bus.publish("orders.created", "payload".into())
                    .await
                    .unwrap();
            });
        });
    });
}

fn bench_publish_multiple_subscribers(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let bus = EventBus::<String>::new();
    let counter = Arc::new(AtomicUsize::new(0));
    let c1 = counter.clone();
    let c2 = counter.clone();
    let c3 = counter.clone();
    rt.block_on(async {
        let _s1 = bus
            .subscribe_sync("orders.*", move |_| {
                c1.fetch_add(1, Ordering::SeqCst);
            })
            .await;
        let _s2 = bus
            .subscribe_sync("orders.created", move |_| {
                c2.fetch_add(1, Ordering::SeqCst);
            })
            .await;
        let _s3 = bus
            .subscribe_sync("orders.*", move |_| {
                c3.fetch_add(1, Ordering::SeqCst);
            })
            .await;
    });

    c.bench_function("publish_multiple_subscribers", |b| {
        b.iter(|| {
            rt.block_on(async {
                bus.publish("orders.created", "payload".into())
                    .await
                    .unwrap();
            });
        });
    });
}

fn bench_publish_wildcard(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let bus = EventBus::<String>::new();
    let counter = Arc::new(AtomicUsize::new(0));
    let c1 = counter.clone();
    rt.block_on(async {
        let _sub = bus
            .subscribe_sync("events.**", move |_| {
                c1.fetch_add(1, Ordering::SeqCst);
            })
            .await;
    });

    c.bench_function("publish_wildcard", |b| {
        b.iter(|| {
            rt.block_on(async {
                bus.publish("events.a.b.c.d", "payload".into())
                    .await
                    .unwrap();
            });
        });
    });
}

fn bench_topic_matches_single_segment(c: &mut Criterion) {
    c.bench_function("topic_matches_single_segment", |b| {
        b.iter(|| topic_matches("orders.*", "orders.created"));
    });
}

fn bench_topic_matches_multi_segment(c: &mut Criterion) {
    c.bench_function("topic_matches_multi_segment", |b| {
        b.iter(|| topic_matches("orders.**", "orders.a.b.c.d.e"));
    });
}

fn bench_topic_matches_exact(c: &mut Criterion) {
    c.bench_function("topic_matches_exact", |b| {
        b.iter(|| topic_matches("orders.created", "orders.created"));
    });
}

fn bench_topic_matches_no_match(c: &mut Criterion) {
    c.bench_function("topic_matches_no_match", |b| {
        b.iter(|| topic_matches("orders.*", "payments.created"));
    });
}

criterion_group!(
    benches,
    bench_event_bus_creation,
    bench_subscribe_single,
    bench_subscribe_multiple,
    bench_publish_single_subscriber,
    bench_publish_multiple_subscribers,
    bench_publish_wildcard,
    bench_topic_matches_single_segment,
    bench_topic_matches_multi_segment,
    bench_topic_matches_exact,
    bench_topic_matches_no_match,
);
criterion_main!(benches);
