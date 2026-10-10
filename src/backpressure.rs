//! Bounded delivery: overflow policy, per-subscriber state, outcome reporting.
//!
//! The reactive/actor literature is consistent that backpressure must be a
//! deliberate choice rather than an emergent consequence of load. These types
//! make that choice explicit: a bounded subscriber declares how much slack it
//! can absorb, what happens when that slack runs out, and reports what it lost.

use std::sync::atomic::{AtomicU64, Ordering};

/// What a bounded subscriber's queue does when it is full.
///
/// Every option keeps the publisher non-blocking. Blocking the publisher
/// would put one subscriber's backlog on the producer's critical path, which
/// is the coupling this module exists to remove.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Evict the oldest queued envelope to make room.
    ///
    /// Keeps the freshest view and loses history. Right for log shipping,
    /// analytics, and cache warming: a consumer that is behind is better
    /// served by current state than by a backlog it will never catch up on.
    DropOldest,
    /// Discard the incoming envelope.
    ///
    /// Preserves what is queued and goes stale. Right for consumers where
    /// each event matters individually and order matters more than
    /// freshness.
    DropNewest,
    /// Refuse the envelope and tell the producer.
    ///
    /// Nothing is lost silently: the producer observes the rejection in its
    /// [`DeliveryReport`] and can write to a durable outbox, retry, or fail.
    Reject,
}

impl OverflowPolicy {
    /// Stable lowercase name, for metrics labels and logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DropOldest => "drop_oldest",
            Self::DropNewest => "drop_newest",
            Self::Reject => "reject",
        }
    }
}

impl Default for OverflowPolicy {
    /// `DropOldest`: a queue that grows without bound is not a
    /// backpressure policy, it is a delayed out-of-memory crash.
    fn default() -> Self {
        Self::DropOldest
    }
}

/// What one bounded queue did with an offered envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Enqueue {
    /// Accepted; the envelope is queued for the callback.
    Delivered,
    /// An older queued envelope was evicted to make room.
    DroppedOldest,
    /// The offered envelope was discarded because the queue was full.
    DroppedNewest,
    /// The envelope was refused and the producer was told.
    Rejected,
}

/// Counters for one bounded subscriber.
///
/// A lagging subscriber used to be invisible: it simply made every publish
/// slower. These numbers make it observable, which is the precondition for
/// tuning capacity instead of guessing.
#[derive(Debug, Default)]
pub struct SubscriberStats {
    delivered: AtomicU64,
    dropped: AtomicU64,
    rejected: AtomicU64,
    panicked: AtomicU64,
    queue_depth: AtomicU64,
}

impl SubscriberStats {
    /// Creates zeroed counters.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Envelopes handed to the callback without panicking.
    #[must_use]
    pub fn delivered(&self) -> u64 {
        self.delivered.load(Ordering::Relaxed)
    }

    /// Envelopes lost to capacity: evicted or discarded.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Envelopes refused under [`OverflowPolicy::Reject`].
    #[must_use]
    pub fn rejected(&self) -> u64 {
        self.rejected.load(Ordering::Relaxed)
    }

    /// Callbacks that panicked.
    ///
    /// Before this counter existed a panicking callback was silently
    /// swallowed by the discarded `JoinError`, so a crashing subscriber
    /// looked identical to a healthy one.
    #[must_use]
    pub fn panicked(&self) -> u64 {
        self.panicked.load(Ordering::Relaxed)
    }

    /// Envelopes currently waiting in the queue.
    ///
    /// A depth that stays above zero under sustained publishing means the
    /// callback cannot keep up. Alert on this well before `dropped` proves
    /// data has already been lost.
    #[must_use]
    pub fn queue_depth(&self) -> u64 {
        self.queue_depth.load(Ordering::Relaxed)
    }

    pub(crate) fn record_delivered(&self) {
        self.delivered.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_dropped(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_rejected(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_panic(&self) {
        self.panicked.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_queue_depth(&self, depth: u64) {
        self.queue_depth.store(depth, Ordering::Relaxed);
    }
}

/// What one publish did across the subscribers that matched.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DeliveryReport {
    /// Subscribers whose pattern matched the topic.
    pub matched: usize,
    /// Envelopes accepted for delivery.
    pub enqueued: usize,
    /// Envelopes lost to capacity.
    pub dropped: usize,
    /// Envelopes refused by a subscriber using [`OverflowPolicy::Reject`].
    pub rejected: usize,
}

impl DeliveryReport {
    /// True when no subscriber lost the envelope.
    #[must_use]
    pub fn is_lossless(&self) -> bool {
        self.dropped == 0 && self.rejected == 0
    }
}
