use std::fmt;

use serde::{Deserialize, Serialize};



/// A trait that all events must implement.
pub trait Event: Clone + Send + Sync + fmt::Display + 'static {
    /// Returns the type name of this event (e.g. "order.created").
    fn type_name(&self) -> &'static str;

    /// Returns the default topic for this event type.
    fn topic(&self) -> String;
}

/// A typed event that can be published on the bus.
///
/// Wraps a payload with a topic and implements the `Event` trait.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypedEvent<T: Clone + Send + Sync + fmt::Display + 'static> {
    topic: String,
    payload: T,
}

impl<T: Clone + Send + Sync + fmt::Display + 'static> TypedEvent<T> {
    /// Creates a new `TypedEvent`.
    pub fn new(topic: impl Into<String>, payload: T) -> Self {
        Self {
            topic: topic.into(),
            payload,
        }
    }

    /// Returns a reference to the payload.
    pub fn payload(&self) -> &T {
        &self.payload
    }

    /// Returns the topic.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Consumes the event and returns the payload.
    pub fn into_payload(self) -> T {
        self.payload
    }
}

impl<T: Clone + Send + Sync + fmt::Display + 'static> Event for TypedEvent<T> {
    fn type_name(&self) -> &'static str {
        std::any::type_name::<T>()
    }

    fn topic(&self) -> String {
        self.topic.clone()
    }
}

impl<T: Clone + Send + Sync + fmt::Display + 'static> fmt::Display for TypedEvent<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TypedEvent({} on {})", self.type_name(), self.topic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct OrderCreated {
        order_id: String,
        amount: f64,
    }

    impl std::fmt::Display for OrderCreated {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "OrderCreated({})", self.order_id)
        }
    }

    #[test]
    fn test_typed_event() {
        let event = TypedEvent::new(
            "orders.created",
            OrderCreated {
                order_id: "123".into(),
                amount: 99.99,
            },
        );

        assert_eq!(event.topic(), "orders.created");
        assert_eq!(event.payload().order_id, "123");
    }

    #[test]
    fn test_into_payload() {
        let payload = OrderCreated {
            order_id: "456".into(),
            amount: 49.99,
        };
        let event = TypedEvent::new("orders.created", payload.clone());
        assert_eq!(event.into_payload(), payload);
    }
}
