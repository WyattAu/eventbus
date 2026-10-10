//! The bounded queue and the per-subscriber worker task.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use crate::backpressure::{Enqueue, OverflowPolicy, SubscriberStats};
use crate::bus::EventCallback;
use crate::envelope::EventEnvelope;

pub(crate) struct BoundedQueue<T: Clone + Send + Sync + 'static> {
    queue: Mutex<VecDeque<Arc<EventEnvelope<T>>>>,
    capacity: usize,
    policy: OverflowPolicy,
    stats: Arc<SubscriberStats>,
    notify: Notify,
    closed: AtomicBool,
}

impl<T: Clone + Send + Sync + 'static> BoundedQueue<T> {
    pub(crate) fn new(
        capacity: usize,
        policy: OverflowPolicy,
        stats: Arc<SubscriberStats>,
    ) -> Self {
        Self {
            queue: Mutex::new(VecDeque::with_capacity(capacity.min(1024))),
            capacity: capacity.max(1),
            policy,
            stats,
            notify: Notify::new(),
            closed: AtomicBool::new(false),
        }
    }

    /// Offers an envelope, applying the overflow policy. Never blocks.
    pub(crate) fn push(&self, envelope: Arc<EventEnvelope<T>>) -> Enqueue {
        let Ok(mut queue) = self.queue.lock() else {
            self.stats.record_dropped();
            return Enqueue::Rejected;
        };
        let outcome = if queue.len() >= self.capacity {
            match self.policy {
                OverflowPolicy::DropOldest => {
                    queue.pop_front();
                    queue.push_back(envelope);
                    Enqueue::DroppedOldest
                }
                OverflowPolicy::DropNewest => Enqueue::DroppedNewest,
                OverflowPolicy::Reject => Enqueue::Rejected,
            }
        } else {
            queue.push_back(envelope);
            Enqueue::Delivered
        };
        self.stats.record_queue_depth(queue.len() as u64);
        drop(queue);

        match outcome {
            Enqueue::DroppedOldest | Enqueue::DroppedNewest => self.stats.record_dropped(),
            Enqueue::Rejected => self.stats.record_rejected(),
            Enqueue::Delivered => {}
        }
        // A stored permit makes this race-free: if the worker is parked it
        // wakes now, and if it is not parked the permit is held for its next
        // await, so a wakeup can never be lost.
        self.notify.notify_one();
        outcome
    }

    fn pop(&self) -> Option<Arc<EventEnvelope<T>>> {
        let mut queue = self.queue.lock().ok()?;
        let envelope = queue.pop_front();
        self.stats.record_queue_depth(queue.len() as u64);
        envelope
    }

    fn is_empty(&self) -> bool {
        self.queue.lock().is_ok_and(|q| q.is_empty())
    }

    /// Signals the worker to drain and exit.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.notify.notify_one();
    }

    pub(crate) fn stats(&self) -> Arc<SubscriberStats> {
        Arc::clone(&self.stats)
    }
}

/// Runs one subscriber's callbacks, one envelope at a time, off the
/// publisher's task.
pub(crate) fn spawn_worker<T: Clone + Send + Sync + 'static>(
    queue: Arc<BoundedQueue<T>>,
    callback: EventCallback<T>,
) {
    tokio::spawn(async move {
        loop {
            if queue.closed.load(Ordering::Relaxed) && queue.is_empty() {
                return;
            }
            let Some(envelope) = queue.pop() else {
                queue.notify.notified().await;
                continue;
            };
            let callback = Arc::clone(&callback);
            // Spawning turns a panicking callback into a `JoinError` instead
            // of unwinding through the worker and killing it, which would
            // silently stop delivery for this subscriber forever.
            match tokio::spawn(async move { callback(envelope).await }).await {
                Ok(()) => queue.stats.record_delivered(),
                Err(err) if err.is_panic() => queue.stats.record_panic(),
                Err(_) => queue.stats.record_delivered(),
            }
        }
    });
}
