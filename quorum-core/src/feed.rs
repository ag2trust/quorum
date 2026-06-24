//! The broadcast feed (replaces the GitHub-issue hub).
//!
//! Messages are appended with a monotonic `seq` and a TTL. Agents read deltas relative to a
//! per-(agent, topic) cursor — cheap polling, never the whole tail. Delivery is at-least-once:
//! `read` is a pure read by default and only advances the cursor when the caller passes
//! `ack_through` (what it durably handled last poll), so a crash mid-poll re-delivers.

use crate::error::{QuorumError, Result};
use crate::sweep::SWEEP_LIMIT;
use rusqlite::{params, Connection, OptionalExtension, Row, TransactionBehavior};
use serde::Serialize;

/// The default topic when none is given.
pub const DEFAULT_TOPIC: &str = "hub";

/// Valid message kinds.
pub const KINDS: &[&str] = &["info", "request", "claim", "done", "hello", "critical"];

/// A feed message.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct Message {
    pub seq: i64,
    pub ts: i64,
    pub author: String,
    pub topic: String,
    pub kind: String,
    pub body: String,
    pub refs: Option<String>,
    pub expires_at: i64,
}

/// Result of a [`post`].
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct PostResult {
    pub seq: i64,
    pub expires_at: i64,
}

const COLS: &str = "seq, ts, author, topic, kind, body, refs, expires_at";

fn row_to_msg(r: &Row) -> rusqlite::Result<Message> {
    Ok(Message {
        seq: r.get(0)?,
        ts: r.get(1)?,
        author: r.get(2)?,
        topic: r.get(3)?,
        kind: r.get(4)?,
        body: r.get(5)?,
        refs: r.get(6)?,
        expires_at: r.get(7)?,
    })
}

fn begin(conn: &mut Connection) -> Result<rusqlite::Transaction<'_>> {
    Ok(conn.transaction_with_behavior(TransactionBehavior::Immediate)?)
}

/// Append a message. `ttl` is seconds-from-now until it logically expires.
#[allow(clippy::too_many_arguments)]
pub fn post(
    conn: &mut Connection,
    author: &str,
    kind: &str,
    topic: Option<&str>,
    body: &str,
    refs: Option<&str>,
    ttl: i64,
    now: i64,
) -> Result<PostResult> {
    if !KINDS.contains(&kind) {
        return Err(QuorumError::Usage(format!("invalid kind: {kind}")));
    }
    let topic = topic.unwrap_or(DEFAULT_TOPIC);
    let expires_at = now + ttl;
    let tx = begin(conn)?;
    crate::agents::touch(&tx, author, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    tx.execute(
        "INSERT INTO messages(ts, author, topic, kind, body, refs, expires_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![now, author, topic, kind, body, refs, expires_at],
    )?;
    let seq = tx.last_insert_rowid();
    tx.commit()?;
    Ok(PostResult { seq, expires_at })
}

/// Read unexpired messages on `topic` with `seq > cursor`, oldest first.
///
/// Two modes: without `ack_through` it is a **pure read** (no lock, no presence bump). With
/// `ack_through` it first advances the cursor **monotonically** (`MAX(last_seq, ack)`) inside
/// a write transaction (bumping presence), then returns messages after the new cursor.
pub fn read(
    conn: &mut Connection,
    agent: &str,
    topic: Option<&str>,
    ack_through: Option<i64>,
    limit: i64,
    now: i64,
) -> Result<Vec<Message>> {
    let topic = topic.unwrap_or(DEFAULT_TOPIC);
    let cursor = match ack_through {
        Some(ack) => {
            let tx = begin(conn)?;
            crate::agents::touch(&tx, agent, now)?;
            crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
            tx.execute(
                "INSERT INTO cursors(agent_id, topic, last_seq) VALUES (?1, ?2, ?3)
                 ON CONFLICT(agent_id, topic)
                 DO UPDATE SET last_seq = MAX(last_seq, excluded.last_seq)",
                params![agent, topic, ack],
            )?;
            let c: i64 = tx.query_row(
                "SELECT last_seq FROM cursors WHERE agent_id=?1 AND topic=?2",
                params![agent, topic],
                |r| r.get(0),
            )?;
            tx.commit()?;
            c
        }
        None => conn
            .query_row(
                "SELECT last_seq FROM cursors WHERE agent_id=?1 AND topic=?2",
                params![agent, topic],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0),
    };
    let mut stmt = conn.prepare(&format!(
        "SELECT {COLS} FROM messages
         WHERE topic=?1 AND seq > ?2 AND expires_at > ?3 ORDER BY seq ASC LIMIT ?4"
    ))?;
    let msgs = stmt
        .query_map(params![topic, cursor, now, limit], row_to_msg)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(msgs)
}

/// Non-cursor read for one-off inspection: unexpired messages on `topic` with `seq > since`.
pub fn peek(
    conn: &Connection,
    topic: Option<&str>,
    since: Option<i64>,
    limit: i64,
    now: i64,
) -> Result<Vec<Message>> {
    let topic = topic.unwrap_or(DEFAULT_TOPIC);
    let since = since.unwrap_or(0);
    let mut stmt = conn.prepare(&format!(
        "SELECT {COLS} FROM messages
         WHERE topic=?1 AND seq > ?2 AND expires_at > ?3 ORDER BY seq ASC LIMIT ?4"
    ))?;
    let msgs = stmt
        .query_map(params![topic, since, now, limit], row_to_msg)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(msgs)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_tmp() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let c = crate::db::open(&dir.path().join("q.db")).unwrap();
        (dir, c)
    }

    fn post_info(c: &mut Connection, author: &str, body: &str, now: i64) -> i64 {
        post(c, author, "info", None, body, None, 1000, now)
            .unwrap()
            .seq
    }

    #[test]
    fn post_returns_increasing_seq() {
        let (_d, mut c) = open_tmp();
        let s1 = post_info(&mut c, "A", "one", 100);
        let s2 = post_info(&mut c, "A", "two", 100);
        assert!(s2 > s1);
    }

    #[test]
    fn post_rejects_invalid_kind() {
        let (_d, mut c) = open_tmp();
        assert!(post(&mut c, "A", "shout", None, "x", None, 1000, 100).is_err());
    }

    #[test]
    fn read_without_ack_is_pure_and_repeatable() {
        let (_d, mut c) = open_tmp();
        post_info(&mut c, "A", "m1", 100);
        post_info(&mut c, "A", "m2", 100);
        let first = read(&mut c, "B", None, None, 10, 100).unwrap();
        assert_eq!(first.len(), 2);
        // no ack → cursor unchanged → same messages again
        let second = read(&mut c, "B", None, None, 10, 100).unwrap();
        assert_eq!(second.len(), 2);
    }

    #[test]
    fn ack_advances_cursor() {
        let (_d, mut c) = open_tmp();
        let s1 = post_info(&mut c, "A", "m1", 100);
        post_info(&mut c, "A", "m2", 100);
        // ack through the first message → next read returns only m2
        let after = read(&mut c, "B", None, Some(s1), 10, 100).unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].body, "m2");
    }

    #[test]
    fn cursor_is_monotonic() {
        let (_d, mut c) = open_tmp();
        let s1 = post_info(&mut c, "A", "m1", 100);
        let s2 = post_info(&mut c, "A", "m2", 100);
        read(&mut c, "B", None, Some(s2), 10, 100).unwrap(); // ack to m2
                                                             // a lower ack must NOT move the cursor backward
        let after = read(&mut c, "B", None, Some(s1), 10, 100).unwrap();
        assert!(after.is_empty(), "cursor regressed below s2");
    }

    #[test]
    fn read_filters_expired() {
        let (_d, mut c) = open_tmp();
        post(&mut c, "A", "info", None, "soon", None, 10, 100).unwrap(); // expires 110
        assert_eq!(read(&mut c, "B", None, None, 10, 105).unwrap().len(), 1);
        assert_eq!(read(&mut c, "B", None, None, 10, 200).unwrap().len(), 0);
    }

    #[test]
    fn body_roundtrips_byte_exact() {
        let (_d, mut c) = open_tmp();
        let body = "héllo \"world\"\n`$x`\n";
        post(&mut c, "A", "info", None, body, None, 1000, 100).unwrap();
        let msgs = peek(&c, None, None, 10, 100).unwrap();
        assert_eq!(msgs[0].body, body);
    }
}
