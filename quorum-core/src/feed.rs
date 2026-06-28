//! The broadcast feed (replaces the GitHub-issue hub).
//!
//! Messages are appended with a monotonic `seq` and a TTL. Agents read deltas relative to a
//! per-(agent, topic) cursor — cheap polling, never the whole tail. Delivery is at-least-once:
//! `read` is a pure read by default and only advances the cursor when the caller passes
//! `ack_through` (what it durably handled last poll), so a crash mid-poll re-delivers.

use crate::db::begin_immediate;
use crate::error::{QuorumError, Result};
use crate::sweep::SWEEP_LIMIT;
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::Serialize;

/// The default topic when none is given.
pub const DEFAULT_TOPIC: &str = "hub";

/// Default message TTL when the CLI's `--ttl` is omitted. 48h matches the typical end-of-day
/// + next-morning round-trip on the hub.
pub const DEFAULT_MESSAGE_TTL_SECS: i64 = 48 * 3600;

/// Default page size for `read`/`peek` when the caller omits `--limit`.
pub const DEFAULT_READ_LIMIT: i64 = 100;

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
    /// Direct-message recipient. `None` = broadcast (visible to everyone).
    pub recipient: Option<String>,
}

/// Result of a [`post`].
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct PostResult {
    pub seq: i64,
    pub expires_at: i64,
}

/// Filter applied by [`read`] over the recipient column. Cursor/TTL/at-least-once semantics
/// are unaffected — this is a view, not a separate channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadFilter {
    /// Default: broadcasts (recipient IS NULL) plus direct-to-me.
    All,
    /// Only direct-to-me (recipient = agent).
    Direct,
    /// Only broadcasts (recipient IS NULL).
    Broadcasts,
}

const COLS: &str = "seq, ts, author, topic, kind, body, refs, expires_at, recipient";

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
        recipient: r.get(8)?,
    })
}

/// Append a message. `ttl` is seconds-from-now until it logically expires. `recipient =
/// Some(agent)` marks it as a direct message to that agent; `None` is a broadcast.
///
/// `system` (#94) marks a critical broadcast as "system must-see" so workers on
/// `--scope minimal` syncs receive its full body. Non-critical/direct messages may
/// also carry `system=true` but it has no observable effect today — the scope filter
/// only applies to critical broadcasts. `false` preserves the legacy shape.
#[allow(clippy::too_many_arguments)]
pub fn post(
    conn: &mut Connection,
    author: &str,
    kind: &str,
    topic: Option<&str>,
    body: &str,
    refs: Option<&str>,
    recipient: Option<&str>,
    system: bool,
    ttl: i64,
    now: i64,
) -> Result<PostResult> {
    if !KINDS.contains(&kind) {
        return Err(QuorumError::Usage(format!("invalid kind: {kind}")));
    }
    let topic = topic.unwrap_or(DEFAULT_TOPIC);
    let expires_at = now + ttl;
    let tx = begin_immediate(conn)?;
    crate::agents::touch(&tx, author, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    let system_i: i64 = i64::from(system);
    tx.execute(
        "INSERT INTO messages(ts, author, topic, kind, body, refs, expires_at, recipient, system)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![now, author, topic, kind, body, refs, expires_at, recipient, system_i],
    )?;
    let seq = tx.last_insert_rowid();
    tx.commit()?;
    Ok(PostResult { seq, expires_at })
}

/// Read unexpired messages on `topic` with `seq > cursor`, oldest first, filtered by
/// `filter`.
///
/// Two modes: without `ack_through` it is a **pure read** (no lock, no presence bump). With
/// `ack_through` it first advances the cursor **monotonically** (`MAX(last_seq, ack)`) inside
/// a write transaction (bumping presence), then returns messages after the new cursor.
///
/// `filter` is a view over the recipient column: it changes *which* rows the caller sees but
/// does not change cursor/TTL/at-least-once semantics — the cursor advances on ack regardless
/// of which filter the caller used.
pub fn read(
    conn: &mut Connection,
    agent: &str,
    topic: Option<&str>,
    ack_through: Option<i64>,
    filter: ReadFilter,
    limit: i64,
    now: i64,
) -> Result<Vec<Message>> {
    let topic = topic.unwrap_or(DEFAULT_TOPIC);
    let cursor = match ack_through {
        Some(ack) => {
            let tx = begin_immediate(conn)?;
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
    let recipient_clause = match filter {
        ReadFilter::All => "AND (recipient IS NULL OR recipient = ?5)",
        ReadFilter::Direct => "AND recipient = ?5",
        ReadFilter::Broadcasts => "AND recipient IS NULL",
    };
    let mut stmt = conn.prepare(&format!(
        "SELECT {COLS} FROM messages
         WHERE topic=?1 AND seq > ?2 AND expires_at > ?3 {recipient_clause}
         ORDER BY seq ASC LIMIT ?4"
    ))?;
    let msgs = match filter {
        ReadFilter::Broadcasts => stmt
            .query_map(params![topic, cursor, now, limit], row_to_msg)?
            .collect::<rusqlite::Result<Vec<_>>>()?,
        ReadFilter::All | ReadFilter::Direct => stmt
            .query_map(params![topic, cursor, now, limit, agent], row_to_msg)?
            .collect::<rusqlite::Result<Vec<_>>>()?,
    };
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
        post(c, author, "info", None, body, None, None, false, 1000, now)
            .unwrap()
            .seq
    }

    fn post_to(c: &mut Connection, author: &str, to: &str, body: &str, now: i64) -> i64 {
        post(
            c,
            author,
            "info",
            None,
            body,
            None,
            Some(to),
            false,
            1000,
            now,
        )
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
        assert!(post(&mut c, "A", "shout", None, "x", None, None, false, 1000, 100).is_err());
    }

    #[test]
    fn read_without_ack_is_pure_and_repeatable() {
        let (_d, mut c) = open_tmp();
        post_info(&mut c, "A", "m1", 100);
        post_info(&mut c, "A", "m2", 100);
        let first = read(&mut c, "B", None, None, ReadFilter::All, 10, 100).unwrap();
        assert_eq!(first.len(), 2);
        // no ack → cursor unchanged → same messages again
        let second = read(&mut c, "B", None, None, ReadFilter::All, 10, 100).unwrap();
        assert_eq!(second.len(), 2);
    }

    #[test]
    fn ack_advances_cursor() {
        let (_d, mut c) = open_tmp();
        let s1 = post_info(&mut c, "A", "m1", 100);
        post_info(&mut c, "A", "m2", 100);
        // ack through the first message → next read returns only m2
        let after = read(&mut c, "B", None, Some(s1), ReadFilter::All, 10, 100).unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].body, "m2");
    }

    #[test]
    fn cursor_is_monotonic() {
        let (_d, mut c) = open_tmp();
        let s1 = post_info(&mut c, "A", "m1", 100);
        let s2 = post_info(&mut c, "A", "m2", 100);
        read(&mut c, "B", None, Some(s2), ReadFilter::All, 10, 100).unwrap(); // ack to m2
                                                                              // a lower ack must NOT move the cursor backward
        let after = read(&mut c, "B", None, Some(s1), ReadFilter::All, 10, 100).unwrap();
        assert!(after.is_empty(), "cursor regressed below s2");
    }

    #[test]
    fn read_filters_expired() {
        let (_d, mut c) = open_tmp();
        post(
            &mut c, "A", "info", None, "soon", None, None, false, 10, 100,
        )
        .unwrap(); // expires 110
        assert_eq!(
            read(&mut c, "B", None, None, ReadFilter::All, 10, 105)
                .unwrap()
                .len(),
            1
        );
        // exact boundary: dead iff expires_at <= now, so now==110 is invisible
        assert_eq!(
            read(&mut c, "B", None, None, ReadFilter::All, 10, 109)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            read(&mut c, "B", None, None, ReadFilter::All, 10, 110)
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            read(&mut c, "B", None, None, ReadFilter::All, 10, 200)
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn body_roundtrips_byte_exact() {
        let (_d, mut c) = open_tmp();
        let body = "héllo \"world\"\n`$x`\n";
        post(
            &mut c, "A", "info", None, body, None, None, false, 1000, 100,
        )
        .unwrap();
        let msgs = peek(&c, None, None, 10, 100).unwrap();
        assert_eq!(msgs[0].body, body);
    }

    // -- Directed messages (issue #5) ----------------------------------------------------

    #[test]
    fn broadcast_has_null_recipient_direct_carries_it() {
        let (_d, mut c) = open_tmp();
        post_info(&mut c, "A", "to-everyone", 100);
        post_to(&mut c, "A", "B", "hey B", 100);
        let msgs = peek(&c, None, None, 10, 100).unwrap();
        assert_eq!(msgs.len(), 2);
        assert!(msgs[0].recipient.is_none());
        assert_eq!(msgs[1].recipient.as_deref(), Some("B"));
    }

    #[test]
    fn default_read_returns_broadcasts_and_direct_to_self() {
        let (_d, mut c) = open_tmp();
        post_info(&mut c, "A", "bcast", 100);
        post_to(&mut c, "A", "B", "hi B", 100);
        post_to(&mut c, "A", "C", "hi C", 100);

        let for_b = read(&mut c, "B", None, None, ReadFilter::All, 10, 100).unwrap();
        let bodies: Vec<_> = for_b.iter().map(|m| m.body.as_str()).collect();
        assert_eq!(bodies, ["bcast", "hi B"]);

        let for_c = read(&mut c, "C", None, None, ReadFilter::All, 10, 100).unwrap();
        let bodies: Vec<_> = for_c.iter().map(|m| m.body.as_str()).collect();
        assert_eq!(bodies, ["bcast", "hi C"]);
    }

    #[test]
    fn read_direct_filter_returns_only_direct_to_self() {
        let (_d, mut c) = open_tmp();
        post_info(&mut c, "A", "bcast", 100);
        post_to(&mut c, "A", "B", "hi B", 100);
        post_to(&mut c, "A", "C", "hi C", 100);

        let direct = read(&mut c, "B", None, None, ReadFilter::Direct, 10, 100).unwrap();
        let bodies: Vec<_> = direct.iter().map(|m| m.body.as_str()).collect();
        assert_eq!(bodies, ["hi B"]);
    }

    #[test]
    fn read_broadcasts_filter_returns_only_broadcasts() {
        let (_d, mut c) = open_tmp();
        post_info(&mut c, "A", "b1", 100);
        post_to(&mut c, "A", "B", "hi B", 100);
        post_info(&mut c, "A", "b2", 100);

        let bcasts = read(&mut c, "B", None, None, ReadFilter::Broadcasts, 10, 100).unwrap();
        let bodies: Vec<_> = bcasts.iter().map(|m| m.body.as_str()).collect();
        assert_eq!(bodies, ["b1", "b2"]);
    }

    #[test]
    fn direct_to_self_invisible_to_others_as_direct() {
        let (_d, mut c) = open_tmp();
        post_to(&mut c, "A", "B", "for B only", 100);
        let direct_c = read(&mut c, "C", None, None, ReadFilter::Direct, 10, 100).unwrap();
        assert!(direct_c.is_empty(), "C must not see direct-to-B as direct");
        let default_c = read(&mut c, "C", None, None, ReadFilter::All, 10, 100).unwrap();
        assert!(default_c.is_empty(), "C must not see direct-to-B at all");
    }

    #[test]
    fn cursor_advances_regardless_of_filter() {
        let (_d, mut c) = open_tmp();
        let s1 = post_info(&mut c, "A", "bcast1", 100);
        let _s2 = post_to(&mut c, "A", "B", "to B", 100);
        // Ack via the broadcasts-only filter through s1; cursor is per-(agent,topic),
        // so the next default read sees only entries with seq > s1.
        let after = read(&mut c, "B", None, Some(s1), ReadFilter::Broadcasts, 10, 100).unwrap();
        assert!(
            after.is_empty(),
            "broadcasts-only after acking past the only broadcast"
        );
        let next = read(&mut c, "B", None, None, ReadFilter::All, 10, 100).unwrap();
        let bodies: Vec<_> = next.iter().map(|m| m.body.as_str()).collect();
        assert_eq!(bodies, ["to B"]);
    }
}
