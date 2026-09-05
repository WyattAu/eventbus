//! SQLite-backed event persistence.

use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Mutex;

/// A persisted event stored in SQLite.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedEvent {
    /// Unique event identifier.
    pub id: u64,
    /// The event topic.
    pub topic: String,
    /// Serialized event payload.
    pub payload: Vec<u8>,
    /// Timestamp in milliseconds since Unix epoch.
    pub timestamp_ms: u64,
}

/// SQLite-backed event store for durable event persistence.
pub struct SqliteStore {
    conn: Mutex<Connection>,
    next_id: std::sync::atomic::AtomicU64,
}

impl SqliteStore {
    /// Open or create a SQLite database at the given path.
    pub fn new(path: impl AsRef<Path>) -> Result<Self, rusqlite::Error> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY,
                topic TEXT NOT NULL,
                payload BLOB NOT NULL,
                timestamp_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_topic ON events(topic);
            CREATE INDEX IF NOT EXISTS idx_timestamp ON events(timestamp_ms);",
        )?;
        let max_id: u64 = conn
            .query_row("SELECT COALESCE(MAX(id), 0) FROM events", [], |row| {
                row.get(0)
            })
            .unwrap_or(0);
        Ok(Self {
            conn: Mutex::new(conn),
            next_id: std::sync::atomic::AtomicU64::new(max_id + 1),
        })
    }

    /// Create an in-memory SQLite store for testing.
    pub fn in_memory() -> Result<Self, rusqlite::Error> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY,
                topic TEXT NOT NULL,
                payload BLOB NOT NULL,
                timestamp_ms INTEGER NOT NULL
            );",
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
            next_id: std::sync::atomic::AtomicU64::new(1),
        })
    }

    /// Store an event and return its assigned id.
    pub fn store(&self, topic: &str, payload: &[u8]) -> Result<u64, rusqlite::Error> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO events (id, topic, payload, timestamp_ms) VALUES (?1, ?2, ?3, ?4)",
            params![id, topic, payload, now_ms],
        )?;
        Ok(id)
    }

    /// Retrieve events for a topic with a timestamp at or after `since_ms`.
    pub fn get_events(
        &self,
        topic: &str,
        since_ms: u64,
    ) -> Result<Vec<PersistedEvent>, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, topic, payload, timestamp_ms FROM events WHERE topic = ?1 AND timestamp_ms >= ?2 ORDER BY timestamp_ms"
        )?;
        let rows = stmt.query_map(params![topic, since_ms], |row| {
            Ok(PersistedEvent {
                id: row.get(0)?,
                topic: row.get(1)?,
                payload: row.get(2)?,
                timestamp_ms: row.get(3)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>()
    }

    /// Get the most recent event for a topic, if any.
    pub fn get_latest(&self, topic: &str) -> Result<Option<PersistedEvent>, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let result = conn.query_row(
            "SELECT id, topic, payload, timestamp_ms FROM events WHERE topic = ?1 ORDER BY timestamp_ms DESC, id DESC LIMIT 1",
            params![topic],
            |row| Ok(PersistedEvent {
                id: row.get(0)?,
                topic: row.get(1)?,
                payload: row.get(2)?,
                timestamp_ms: row.get(3)?,
            }),
        );
        match result {
            Ok(event) => Ok(Some(event)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Delete events older than `before_ms`. Returns the number of deleted rows.
    pub fn cleanup(&self, before_ms: u64) -> Result<u64, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let deleted = conn.execute(
            "DELETE FROM events WHERE timestamp_ms < ?1",
            params![before_ms],
        )?;
        Ok(deleted as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_and_retrieve() {
        let store = SqliteStore::in_memory().unwrap();
        let id = store.store("orders.created", b"order-1").unwrap();
        assert_eq!(id, 1);
        let events = store.get_events("orders.created", 0).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload, b"order-1");
    }

    #[test]
    fn topic_filtering() {
        let store = SqliteStore::in_memory().unwrap();
        store.store("a", b"1").unwrap();
        store.store("b", b"2").unwrap();
        store.store("a", b"3").unwrap();
        let a_events = store.get_events("a", 0).unwrap();
        assert_eq!(a_events.len(), 2);
        let b_events = store.get_events("b", 0).unwrap();
        assert_eq!(b_events.len(), 1);
    }

    #[test]
    fn timestamp_filtering() {
        let store = SqliteStore::in_memory().unwrap();
        store.store("t", b"1").unwrap();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let events = store.get_events("t", now_ms + 1_000_000).unwrap();
        assert!(events.is_empty());
    }

    #[test]
    fn get_latest() {
        let store = SqliteStore::in_memory().unwrap();
        store.store("topic", b"first").unwrap();
        store.store("topic", b"second").unwrap();
        let latest = store.get_latest("topic").unwrap().unwrap();
        assert_eq!(latest.payload, b"second");
    }

    #[test]
    fn get_latest_empty() {
        let store = SqliteStore::in_memory().unwrap();
        assert!(store.get_latest("nope").unwrap().is_none());
    }

    #[test]
    fn cleanup_removes_old() {
        let store = SqliteStore::in_memory().unwrap();
        store.store("t", b"a").unwrap();
        let deleted = store.cleanup(i64::MAX as u64).unwrap();
        assert_eq!(deleted, 1);
        assert!(store.get_events("t", 0).unwrap().is_empty());
    }

    #[test]
    fn id_sequencing_across_reopen() {
        let dir = std::env::temp_dir().join("eventbus_test_reopen");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("test.db");

        {
            let store = SqliteStore::new(&db_path).unwrap();
            store.store("t", b"1").unwrap();
            store.store("t", b"2").unwrap();
        }

        {
            let store = SqliteStore::new(&db_path).unwrap();
            let id = store.store("t", b"3").unwrap();
            assert_eq!(id, 3);
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
