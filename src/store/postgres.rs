use async_trait::async_trait;
use sqlx::PgPool;

use crate::envelope::EventEnvelope;
use crate::error::{EventBusError, Result};
use crate::store::EventStore;
use crate::subscription::topic_matches;

/// A Postgres-backed event store.
///
/// Requires the `postgres` feature and a running Postgres instance.
pub struct PostgresStore {
    pool: PgPool,
}

#[derive(sqlx::FromRow)]
struct StoredEvent {
    id: uuid::Uuid,
    topic: String,
    payload: serde_json::Value,
    timestamp: i64,
    metadata: serde_json::Value,
}

impl PostgresStore {
    /// Create a new `PostgresStore` from an existing connection pool.
    pub async fn open(pool: PgPool) -> Result<Self> {
        Ok(Self { pool })
    }

    /// Run the schema migration to create the `event_store` table.
    pub async fn migrate(pool: &PgPool) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS event_store (
                id UUID PRIMARY KEY,
                topic TEXT NOT NULL,
                payload JSONB NOT NULL,
                timestamp BIGINT NOT NULL,
                metadata JSONB NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS idx_event_topic ON event_store (topic);
            CREATE INDEX IF NOT EXISTS idx_event_timestamp ON event_store (timestamp);",
        )
        .execute(pool)
        .await
        .map_err(|e| EventBusError::Store(format!("migration: {e}").into()))?;
        Ok(())
    }
}

#[async_trait]
impl<T> EventStore<T> for PostgresStore
where
    T: Clone + Send + Sync + serde::Serialize + for<'de> serde::Deserialize<'de> + 'static,
{
    async fn append(&self, envelope: &EventEnvelope<T>) -> Result<()> {
        let payload = serde_json::to_value(&envelope.payload)
            .map_err(|e| EventBusError::Store(format!("serialize payload: {e}").into()))?;
        let metadata = serde_json::to_value(&envelope.metadata)
            .map_err(|e| EventBusError::Store(format!("serialize metadata: {e}").into()))?;

        sqlx::query(
            "INSERT INTO event_store (id, topic, payload, timestamp, metadata)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(envelope.id)
        .bind(envelope.topic.as_ref())
        .bind(payload)
        .bind(envelope.timestamp)
        .bind(metadata)
        .execute(&self.pool)
        .await
        .map_err(|e| EventBusError::Store(format!("append: {e}").into()))?;

        Ok(())
    }

    async fn load_since(&self, since: i64) -> Result<Vec<EventEnvelope<T>>> {
        let rows: Vec<StoredEvent> = sqlx::query_as(
            "SELECT id, topic, payload, timestamp, metadata FROM event_store
             WHERE timestamp >= $1 ORDER BY timestamp ASC",
        )
        .bind(since)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| EventBusError::Store(format!("load_since: {e}").into()))?;

        rows.into_iter().map(deserialize_envelope).collect()
    }

    async fn load_by_topic(&self, topic_pattern: &str) -> Result<Vec<EventEnvelope<T>>> {
        let all = self.load_all().await?;
        Ok(all
            .into_iter()
            .filter(|e| topic_matches(topic_pattern, &e.topic))
            .collect())
    }

    async fn load_all(&self) -> Result<Vec<EventEnvelope<T>>> {
        let rows: Vec<StoredEvent> = sqlx::query_as(
            "SELECT id, topic, payload, timestamp, metadata FROM event_store ORDER BY timestamp ASC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| EventBusError::Store(format!("load_all: {e}").into()))?;

        rows.into_iter().map(deserialize_envelope).collect()
    }
}

fn deserialize_envelope<T: Clone + Send + Sync + for<'de> serde::Deserialize<'de> + 'static>(
    row: StoredEvent,
) -> Result<EventEnvelope<T>> {
    let payload: T = serde_json::from_value(row.payload)
        .map_err(|e| EventBusError::Store(format!("deserialize payload: {e}").into()))?;
    let metadata = serde_json::from_value(row.metadata)
        .map_err(|e| EventBusError::Store(format!("deserialize metadata: {e}").into()))?;

    Ok(EventEnvelope {
        id: row.id,
        topic: row.topic.into(),
        payload,
        timestamp: row.timestamp,
        metadata,
    })
}
