/// Errors that can occur in event bus operations.
#[derive(Debug, thiserror::Error)]
pub enum EventBusError {
    /// A serialization error occurred. `Cow` avoids allocation when the
    /// message is a static string.
    #[error("Serialization error: {0}")]
    Serialization(std::borrow::Cow<'static, str>),

    /// A subscription error occurred.
    #[error("Subscription error: {0}")]
    Subscription(std::borrow::Cow<'static, str>),

    /// The event bus has been closed.
    #[error("Event bus is closed")]
    Closed,

    /// A subscriber panicked while handling an event.
    #[error("Subscriber panicked: {0}")]
    SubscriberPanicked(std::borrow::Cow<'static, str>),

    /// A persistence/store error occurred.
    #[error("Store error: {0}")]
    Store(std::borrow::Cow<'static, str>),
}

/// A convenience result type for event bus operations.
pub type Result<T> = std::result::Result<T, EventBusError>;

impl From<serde_json::Error> for EventBusError {
    fn from(e: serde_json::Error) -> Self {
        EventBusError::Serialization(e.to_string().into())
    }
}
