/// Errors that can occur in event bus operations.
#[derive(Debug, thiserror::Error)]
pub enum EventBusError {
    /// A serialization error occurred.
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// A subscription error occurred.
    #[error("Subscription error: {0}")]
    Subscription(String),

    /// The event bus has been closed.
    #[error("Event bus is closed")]
    Closed,
}

/// A convenience result type for event bus operations.
pub type Result<T> = std::result::Result<T, EventBusError>;

impl From<serde_json::Error> for EventBusError {
    fn from(e: serde_json::Error) -> Self {
        EventBusError::Serialization(e.to_string())
    }
}
