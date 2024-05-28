use rusqlite::{params, Connection, OptionalExtension, Result};

pub struct Subscription {
    pub subscriber_id: String,
    pub last_event_id: u64,
}

pub fn setup_db() -> Result<Connection> {
    let conn = Connection::open("subscriptions.db")?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS subscriptions (
            subscriber_id TEXT PRIMARY KEY,
            last_event_id INTEGER NOT NULL
        )",
        [],
    )?;

    Ok(conn)
}

pub fn load_subscription(conn: &Connection, subscriber_id: &str) -> Result<Option<u64>> {
    let mut stmt =
        conn.prepare("SELECT last_event_id FROM subscriptions WHERE subscriber_id = ?1")?;
    stmt.query_row([subscriber_id], |row| row.get(0)).optional()
}

pub fn update_subscription(
    conn: &Connection,
    subscriber_id: &str,
    last_event_id: u64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO subscriptions (subscriber_id, last_event_id)
         VALUES (?1, ?2)
         ON CONFLICT(subscriber_id) DO UPDATE SET last_event_id = excluded.last_event_id",
        params![subscriber_id, last_event_id],
    )?;

    Ok(())
}
