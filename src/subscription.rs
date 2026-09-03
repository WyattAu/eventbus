use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A subscription handle returned when subscribing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    /// Unique ID for this subscription.
    pub id: Uuid,
    /// The topic pattern (supports `*` and `**` wildcards). `Arc<str>` shares
    /// the pattern string across the DashMap entry and the Subscription handle.
    pub topic_pattern: Arc<str>,
    /// Whether this subscription is currently active.
    pub active: bool,
}

impl Subscription {
    /// Creates a new active subscription. Accepts `String`, `&str`, or `Arc<str>`.
    pub fn new(topic_pattern: impl Into<Arc<str>>) -> Self {
        Self {
            id: Uuid::new_v4(),
            topic_pattern: topic_pattern.into(),
            active: true,
        }
    }

    /// Deactivates this subscription.
    pub fn deactivate(&mut self) {
        self.active = false;
    }

    /// Returns true if this subscription matches a given topic.
    pub fn matches(&self, topic: &str) -> bool {
        if !self.active {
            return false;
        }
        topic_matches(&self.topic_pattern, topic)
    }
}

impl fmt::Display for Subscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Subscription({} on '{}' {})",
            self.id,
            self.topic_pattern,
            if self.active { "active" } else { "inactive" }
        )
    }
}

/// Checks if a topic matches a pattern with wildcard support.
///
/// - `*` matches a single segment (between `.` separators)
/// - `**` matches zero or more segments
///
/// # Examples
///
/// ```text
/// topic_matches("orders.*", "orders.created")       => true
/// topic_matches("orders.*", "orders.created.v2")    => false
/// topic_matches("orders.**", "orders.created.v2")   => true
/// topic_matches("*", "orders")                      => true
/// topic_matches("**", "anything.goes.here")         => true
/// ```
pub fn topic_matches(pattern: &str, topic: &str) -> bool {
    let pattern_parts: Vec<&str> = pattern.split('.').collect();
    let topic_parts: Vec<&str> = topic.split('.').collect();
    matches_recursive(&pattern_parts, &topic_parts)
}

fn matches_recursive(pattern: &[&str], topic: &[&str]) -> bool {
    if pattern.is_empty() {
        return topic.is_empty();
    }

    if pattern[0] == "**" {
        // ** can match zero or more segments
        // Try matching ** against 0, 1, 2, ... segments
        for i in 0..=topic.len() {
            if matches_recursive(&pattern[1..], &topic[i..]) {
                return true;
            }
        }
        return false;
    }

    if topic.is_empty() {
        return false;
    }

    if pattern[0] == "*" || pattern[0] == topic[0] {
        return matches_recursive(&pattern[1..], &topic[1..]);
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_matches() {
        let sub = Subscription::new("orders.*");
        assert!(sub.matches("orders.created"));
        assert!(!sub.matches("orders.created.v2"));
    }

    #[test]
    fn test_inactive_subscription() {
        let mut sub = Subscription::new("orders.*");
        assert!(sub.matches("orders.created"));
        sub.deactivate();
        assert!(!sub.matches("orders.created"));
    }

    #[test]
    fn test_single_wildcard() {
        assert!(topic_matches("orders.*", "orders.created"));
        assert!(!topic_matches("orders.*", "orders.created.v2"));
        assert!(!topic_matches("orders.*", "payments.created"));
    }

    #[test]
    fn test_double_wildcard() {
        assert!(topic_matches("orders.**", "orders.created"));
        assert!(topic_matches("orders.**", "orders.created.v2"));
        assert!(topic_matches("orders.**", "orders.a.b.c"));
        assert!(!topic_matches("orders.**", "payments.created"));
    }

    #[test]
    fn test_star_only() {
        assert!(topic_matches("*", "anything"));
        assert!(!topic_matches("*", "two.parts"));
    }

    #[test]
    fn test_double_star_only() {
        assert!(topic_matches("**", "anything"));
        assert!(topic_matches("**", "multi.part.topic"));
        assert!(topic_matches("**", ""));
    }

    #[test]
    fn test_exact_match() {
        assert!(topic_matches("orders.created", "orders.created"));
        assert!(!topic_matches("orders.created", "orders.cancelled"));
    }
}
