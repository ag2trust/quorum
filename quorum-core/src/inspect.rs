//! Read-only diagnostic queries for messages, events, and errors.
//!
//! Every function here opens NO write transaction, performs no touch/ack/sweep, and returns
//! rows including expired ones (with a derived `state` field). Callers wrap the result with
//! `as_of` and retention caveats.

use crate::error::Result;
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::Serialize;

// ── Messages ────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct InspectMessage {
    pub seq: i64,
    pub ts: i64,
    pub author: String,
    pub topic: String,
    pub kind: String,
    pub body: String,
    pub refs: Option<String>,
    pub expires_at: i64,
    pub recipient: Option<String>,
    pub state: &'static str,
}

fn row_to_inspect_msg(r: &Row, now: i64) -> rusqlite::Result<InspectMessage> {
    let expires_at: i64 = r.get(7)?;
    Ok(InspectMessage {
        seq: r.get(0)?,
        ts: r.get(1)?,
        author: r.get(2)?,
        topic: r.get(3)?,
        kind: r.get(4)?,
        body: r.get(5)?,
        refs: r.get(6)?,
        expires_at,
        recipient: r.get(8)?,
        state: if expires_at > now { "live" } else { "expired" },
    })
}

const MSG_COLS: &str = "seq, ts, author, topic, kind, body, refs, expires_at, recipient";

/// Single message by seq (including expired).
pub fn get_message(conn: &Connection, seq: i64, now: i64) -> Result<Option<InspectMessage>> {
    let msg = conn
        .query_row(
            &format!("SELECT {MSG_COLS} FROM messages WHERE seq = ?1"),
            params![seq],
            |r| row_to_inspect_msg(r, now),
        )
        .optional()?;
    Ok(msg)
}

#[derive(Debug, Default)]
pub struct MessagesFilter<'a> {
    pub author: Option<&'a str>,
    pub recipient: Option<&'a str>,
    pub kind: Option<&'a str>,
    pub topic: Option<&'a str>,
    pub refs_contains: Option<&'a str>,
    pub state: Option<&'a str>,
    pub since_seq: Option<i64>,
    pub before_seq: Option<i64>,
    pub since_ts: Option<i64>,
    pub before_ts: Option<i64>,
    pub limit: i64,
}

/// Filtered message listing (including expired rows, with derived state).
pub fn list_messages(
    conn: &Connection,
    f: &MessagesFilter,
    now: i64,
) -> Result<Vec<InspectMessage>> {
    let mut clauses = Vec::new();
    let mut bind_vals: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    let mut idx = 0usize;

    macro_rules! bind {
        ($clause:expr, $val:expr) => {{
            idx += 1;
            clauses.push(format!($clause, idx));
            bind_vals.push(Box::new($val));
        }};
    }

    if let Some(v) = f.author {
        bind!("author = ?{}", v.to_string());
    }
    if let Some(v) = f.recipient {
        bind!("recipient = ?{}", v.to_string());
    }
    if let Some(v) = f.kind {
        bind!("kind = ?{}", v.to_string());
    }
    if let Some(v) = f.topic {
        bind!("topic = ?{}", v.to_string());
    }
    if let Some(v) = f.refs_contains {
        bind!("refs LIKE '%' || ?{} || '%'", v.to_string());
    }
    match f.state {
        Some("live") => {
            bind!("expires_at > ?{}", now);
        }
        Some("expired") => {
            bind!("expires_at <= ?{}", now);
        }
        _ => {}
    }
    if let Some(v) = f.since_seq {
        bind!("seq > ?{}", v);
    }
    if let Some(v) = f.before_seq {
        bind!("seq < ?{}", v);
    }
    if let Some(v) = f.since_ts {
        bind!("ts >= ?{}", v);
    }
    if let Some(v) = f.before_ts {
        bind!("ts < ?{}", v);
    }

    idx += 1;
    let limit_idx = idx;
    bind_vals.push(Box::new(f.limit));

    let where_clause = if clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", clauses.join(" AND "))
    };

    let sql = format!(
        "SELECT {MSG_COLS} FROM messages {where_clause} ORDER BY seq ASC LIMIT ?{limit_idx}"
    );

    let params: Vec<&dyn rusqlite::ToSql> = bind_vals.iter().map(|b| b.as_ref()).collect();
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(params.as_slice(), |r| row_to_inspect_msg(r, now))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

// ── Events ──────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct InspectEvent {
    pub seq: i64,
    pub ts: i64,
    pub kind: String,
    pub subject: String,
    pub body: String,
    pub expires_at: i64,
    pub state: &'static str,
}

fn row_to_inspect_event(r: &Row, now: i64) -> rusqlite::Result<InspectEvent> {
    let expires_at: i64 = r.get(5)?;
    Ok(InspectEvent {
        seq: r.get(0)?,
        ts: r.get(1)?,
        kind: r.get(2)?,
        subject: r.get(3)?,
        body: r.get(4)?,
        expires_at,
        state: if expires_at > now { "live" } else { "expired" },
    })
}

const EVT_COLS: &str = "seq, ts, kind, subject, body, expires_at";

/// Single event by seq (including expired).
pub fn get_event(conn: &Connection, seq: i64, now: i64) -> Result<Option<InspectEvent>> {
    let evt = conn
        .query_row(
            &format!("SELECT {EVT_COLS} FROM events WHERE seq = ?1"),
            params![seq],
            |r| row_to_inspect_event(r, now),
        )
        .optional()?;
    Ok(evt)
}

#[derive(Debug, Default)]
pub struct EventsFilter<'a> {
    pub kind: Option<&'a str>,
    pub subject: Option<&'a str>,
    pub state: Option<&'a str>,
    pub since_seq: Option<i64>,
    pub before_seq: Option<i64>,
    pub since_ts: Option<i64>,
    pub before_ts: Option<i64>,
    pub limit: i64,
}

/// Filtered event listing (including expired rows).
pub fn list_events(conn: &Connection, f: &EventsFilter, now: i64) -> Result<Vec<InspectEvent>> {
    let mut clauses = Vec::new();
    let mut bind_vals: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    let mut idx = 0usize;

    macro_rules! bind {
        ($clause:expr, $val:expr) => {{
            idx += 1;
            clauses.push(format!($clause, idx));
            bind_vals.push(Box::new($val));
        }};
    }

    if let Some(v) = f.kind {
        bind!("kind = ?{}", v.to_string());
    }
    if let Some(v) = f.subject {
        bind!("subject = ?{}", v.to_string());
    }
    match f.state {
        Some("live") => {
            bind!("expires_at > ?{}", now);
        }
        Some("expired") => {
            bind!("expires_at <= ?{}", now);
        }
        _ => {}
    }
    if let Some(v) = f.since_seq {
        bind!("seq > ?{}", v);
    }
    if let Some(v) = f.before_seq {
        bind!("seq < ?{}", v);
    }
    if let Some(v) = f.since_ts {
        bind!("ts >= ?{}", v);
    }
    if let Some(v) = f.before_ts {
        bind!("ts < ?{}", v);
    }

    idx += 1;
    let limit_idx = idx;
    bind_vals.push(Box::new(f.limit));

    let where_clause = if clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", clauses.join(" AND "))
    };

    let sql =
        format!("SELECT {EVT_COLS} FROM events {where_clause} ORDER BY seq ASC LIMIT ?{limit_idx}");

    let params: Vec<&dyn rusqlite::ToSql> = bind_vals.iter().map(|b| b.as_ref()).collect();
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(params.as_slice(), |r| row_to_inspect_event(r, now))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

// ── Errors ──────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct InspectError {
    pub id: i64,
    pub ts: i64,
    pub source: String,
    pub detail: String,
    pub expires_at: i64,
    pub state: &'static str,
}

fn row_to_inspect_error(r: &Row, now: i64) -> rusqlite::Result<InspectError> {
    let expires_at: i64 = r.get(4)?;
    Ok(InspectError {
        id: r.get(0)?,
        ts: r.get(1)?,
        source: r.get(2)?,
        detail: r.get(3)?,
        expires_at,
        state: if expires_at > now { "live" } else { "expired" },
    })
}

const ERR_COLS: &str = "id, ts, source, detail, expires_at";

/// Single error by id (including expired).
pub fn get_error(conn: &Connection, id: i64, now: i64) -> Result<Option<InspectError>> {
    let err = conn
        .query_row(
            &format!("SELECT {ERR_COLS} FROM errors WHERE id = ?1"),
            params![id],
            |r| row_to_inspect_error(r, now),
        )
        .optional()?;
    Ok(err)
}

#[derive(Debug, Default)]
pub struct ErrorsFilter<'a> {
    pub source: Option<&'a str>,
    pub state: Option<&'a str>,
    pub since_id: Option<i64>,
    pub before_id: Option<i64>,
    pub since_ts: Option<i64>,
    pub before_ts: Option<i64>,
    pub limit: i64,
}

/// Filtered error listing (including expired rows).
pub fn list_errors(conn: &Connection, f: &ErrorsFilter, now: i64) -> Result<Vec<InspectError>> {
    let mut clauses = Vec::new();
    let mut bind_vals: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    let mut idx = 0usize;

    macro_rules! bind {
        ($clause:expr, $val:expr) => {{
            idx += 1;
            clauses.push(format!($clause, idx));
            bind_vals.push(Box::new($val));
        }};
    }

    if let Some(v) = f.source {
        bind!("source = ?{}", v.to_string());
    }
    match f.state {
        Some("live") => {
            bind!("expires_at > ?{}", now);
        }
        Some("expired") => {
            bind!("expires_at <= ?{}", now);
        }
        _ => {}
    }
    if let Some(v) = f.since_id {
        bind!("id > ?{}", v);
    }
    if let Some(v) = f.before_id {
        bind!("id < ?{}", v);
    }
    if let Some(v) = f.since_ts {
        bind!("ts >= ?{}", v);
    }
    if let Some(v) = f.before_ts {
        bind!("ts < ?{}", v);
    }

    idx += 1;
    let limit_idx = idx;
    bind_vals.push(Box::new(f.limit));

    let where_clause = if clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", clauses.join(" AND "))
    };

    let sql =
        format!("SELECT {ERR_COLS} FROM errors {where_clause} ORDER BY id ASC LIMIT ?{limit_idx}");

    let params: Vec<&dyn rusqlite::ToSql> = bind_vals.iter().map(|b| b.as_ref()).collect();
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(params.as_slice(), |r| row_to_inspect_error(r, now))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

// ── Retention notes ─────────────────────────────────────────────────────────

pub const MESSAGES_RETENTION: &str =
    "Messages expire per TTL (default 48h). Expired rows may be swept at any time. \
     Absence does not prove a message was never sent.";

pub const EVENTS_RETENTION: &str =
    "Events expire per TTL (default 24h). Expired rows may be swept at any time. \
     Absence does not prove an event never occurred.";

pub const ERRORS_RETENTION: &str =
    "Errors expire per TTL (default 7d). Expired rows may be swept at any time. \
     Absence does not prove no error occurred.";

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::TransactionBehavior;

    fn open_tmp() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let c = crate::db::open(&dir.path().join("q.db")).unwrap();
        (dir, c)
    }

    fn insert_msg(c: &mut Connection, author: &str, body: &str, now: i64) -> i64 {
        crate::feed::post(c, author, "info", None, body, None, None, 1000, now)
            .unwrap()
            .seq
    }

    fn insert_event(c: &mut Connection, kind: &str, subject: &str, body: &str, now: i64) -> i64 {
        let tx = c
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let seq = crate::events::emit(&tx, kind, subject, body, now).unwrap();
        tx.commit().unwrap();
        seq
    }

    fn insert_error(c: &Connection, source: &str, detail: &str, now: i64) {
        crate::errlog::log_error(c, now, source, detail);
    }

    // ── get_message ──

    #[test]
    fn get_message_found() {
        let (_d, mut c) = open_tmp();
        let seq = insert_msg(&mut c, "Alice", "hello", 100);
        let msg = get_message(&c, seq, 100).unwrap().unwrap();
        assert_eq!(msg.author, "Alice");
        assert_eq!(msg.body, "hello");
        assert_eq!(msg.state, "live");
    }

    #[test]
    fn get_message_not_found() {
        let (_d, c) = open_tmp();
        assert!(get_message(&c, 999, 100).unwrap().is_none());
    }

    #[test]
    fn get_message_shows_expired() {
        let (_d, mut c) = open_tmp();
        let seq = insert_msg(&mut c, "Alice", "old", 100);
        let msg = get_message(&c, seq, 1200).unwrap().unwrap();
        assert_eq!(msg.state, "expired");
    }

    // ── list_messages ──

    #[test]
    fn list_messages_filters_by_author() {
        let (_d, mut c) = open_tmp();
        insert_msg(&mut c, "Alice", "a1", 100);
        insert_msg(&mut c, "Bob", "b1", 100);
        let f = MessagesFilter {
            author: Some("Alice"),
            limit: 50,
            ..Default::default()
        };
        let msgs = list_messages(&c, &f, 100).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].author, "Alice");
    }

    #[test]
    fn list_messages_includes_expired() {
        let (_d, mut c) = open_tmp();
        insert_msg(&mut c, "Alice", "old", 100);
        let f = MessagesFilter {
            limit: 50,
            ..Default::default()
        };
        let msgs = list_messages(&c, &f, 1200).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].state, "expired");
    }

    #[test]
    fn list_messages_state_filter_live() {
        let (_d, mut c) = open_tmp();
        insert_msg(&mut c, "Alice", "live-one", 100);
        // second message already expired
        crate::feed::post(&mut c, "Bob", "info", None, "old", None, None, 10, 100).unwrap();
        let f = MessagesFilter {
            state: Some("live"),
            limit: 50,
            ..Default::default()
        };
        let msgs = list_messages(&c, &f, 200).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].body, "live-one");
    }

    #[test]
    fn list_messages_state_filter_expired() {
        let (_d, mut c) = open_tmp();
        insert_msg(&mut c, "Alice", "live-one", 100);
        crate::feed::post(&mut c, "Bob", "info", None, "old", None, None, 10, 100).unwrap();
        let f = MessagesFilter {
            state: Some("expired"),
            limit: 50,
            ..Default::default()
        };
        let msgs = list_messages(&c, &f, 200).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].body, "old");
    }

    #[test]
    fn list_messages_bounded_by_limit() {
        let (_d, mut c) = open_tmp();
        for i in 0..5 {
            insert_msg(&mut c, "A", &format!("m{i}"), 100);
        }
        let f = MessagesFilter {
            limit: 3,
            ..Default::default()
        };
        let msgs = list_messages(&c, &f, 100).unwrap();
        assert_eq!(msgs.len(), 3);
    }

    #[test]
    fn list_messages_refs_contains() {
        let (_d, mut c) = open_tmp();
        crate::feed::post(
            &mut c,
            "A",
            "info",
            None,
            "with-ref",
            Some(r#"{"pr":42}"#),
            None,
            1000,
            100,
        )
        .unwrap();
        insert_msg(&mut c, "B", "no-ref", 100);
        let f = MessagesFilter {
            refs_contains: Some("pr"),
            limit: 50,
            ..Default::default()
        };
        let msgs = list_messages(&c, &f, 100).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].body, "with-ref");
    }

    #[test]
    fn list_messages_time_range() {
        let (_d, mut c) = open_tmp();
        insert_msg(&mut c, "A", "early", 100);
        insert_msg(&mut c, "A", "mid", 200);
        insert_msg(&mut c, "A", "late", 300);
        let f = MessagesFilter {
            since_ts: Some(150),
            before_ts: Some(250),
            limit: 50,
            ..Default::default()
        };
        let msgs = list_messages(&c, &f, 300).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].body, "mid");
    }

    #[test]
    fn list_messages_empty_result() {
        let (_d, c) = open_tmp();
        let f = MessagesFilter {
            limit: 50,
            ..Default::default()
        };
        let msgs = list_messages(&c, &f, 100).unwrap();
        assert!(msgs.is_empty());
    }

    // ── get_event ──

    #[test]
    fn get_event_found() {
        let (_d, mut c) = open_tmp();
        let seq = insert_event(&mut c, "task_created", "task#1", "created", 100);
        let evt = get_event(&c, seq, 100).unwrap().unwrap();
        assert_eq!(evt.kind, "task_created");
        assert_eq!(evt.state, "live");
    }

    #[test]
    fn get_event_not_found() {
        let (_d, c) = open_tmp();
        assert!(get_event(&c, 999, 100).unwrap().is_none());
    }

    #[test]
    fn get_event_shows_expired() {
        let (_d, mut c) = open_tmp();
        let seq = insert_event(&mut c, "task_created", "task#1", "created", 100);
        let evt = get_event(&c, seq, 100 + 24 * 3600 + 1).unwrap().unwrap();
        assert_eq!(evt.state, "expired");
    }

    // ── list_events ──

    #[test]
    fn list_events_filters_by_kind() {
        let (_d, mut c) = open_tmp();
        insert_event(&mut c, "task_created", "task#1", "a", 100);
        insert_event(&mut c, "task_claimed", "task#1", "b", 100);
        let f = EventsFilter {
            kind: Some("task_created"),
            limit: 50,
            ..Default::default()
        };
        let evts = list_events(&c, &f, 100).unwrap();
        assert_eq!(evts.len(), 1);
        assert_eq!(evts[0].kind, "task_created");
    }

    #[test]
    fn list_events_filters_by_subject() {
        let (_d, mut c) = open_tmp();
        insert_event(&mut c, "task_created", "task#1", "a", 100);
        insert_event(&mut c, "task_created", "task#2", "b", 100);
        let f = EventsFilter {
            subject: Some("task#1"),
            limit: 50,
            ..Default::default()
        };
        let evts = list_events(&c, &f, 100).unwrap();
        assert_eq!(evts.len(), 1);
        assert_eq!(evts[0].subject, "task#1");
    }

    #[test]
    fn list_events_includes_expired() {
        let (_d, mut c) = open_tmp();
        insert_event(&mut c, "task_created", "task#1", "a", 100);
        let f = EventsFilter {
            limit: 50,
            ..Default::default()
        };
        let evts = list_events(&c, &f, 100 + 24 * 3600 + 1).unwrap();
        assert_eq!(evts.len(), 1);
        assert_eq!(evts[0].state, "expired");
    }

    #[test]
    fn list_events_bounded_by_limit() {
        let (_d, mut c) = open_tmp();
        for i in 0..5 {
            insert_event(&mut c, "task_created", &format!("task#{i}"), "x", 100);
        }
        let f = EventsFilter {
            limit: 3,
            ..Default::default()
        };
        let evts = list_events(&c, &f, 100).unwrap();
        assert_eq!(evts.len(), 3);
    }

    #[test]
    fn list_events_seq_range() {
        let (_d, mut c) = open_tmp();
        let s1 = insert_event(&mut c, "task_created", "task#1", "a", 100);
        let _s2 = insert_event(&mut c, "task_claimed", "task#1", "b", 100);
        let s3 = insert_event(&mut c, "task_done", "task#1", "c", 100);
        let f = EventsFilter {
            since_seq: Some(s1),
            before_seq: Some(s3),
            limit: 50,
            ..Default::default()
        };
        let evts = list_events(&c, &f, 100).unwrap();
        assert_eq!(evts.len(), 1);
        assert_eq!(evts[0].kind, "task_claimed");
    }

    // ── get_error ──

    #[test]
    fn get_error_found() {
        let (_d, c) = open_tmp();
        insert_error(&c, "claim", "database busy", 100);
        let err = get_error(&c, 1, 100).unwrap().unwrap();
        assert_eq!(err.source, "claim");
        assert_eq!(err.detail, "database busy");
        assert_eq!(err.state, "live");
    }

    #[test]
    fn get_error_not_found() {
        let (_d, c) = open_tmp();
        assert!(get_error(&c, 999, 100).unwrap().is_none());
    }

    #[test]
    fn get_error_shows_expired() {
        let (_d, c) = open_tmp();
        insert_error(&c, "claim", "busy", 100);
        let err = get_error(&c, 1, 100 + 7 * 24 * 3600 + 1).unwrap().unwrap();
        assert_eq!(err.state, "expired");
    }

    // ── list_errors ──

    #[test]
    fn list_errors_filters_by_source() {
        let (_d, c) = open_tmp();
        insert_error(&c, "claim", "busy", 100);
        insert_error(&c, "sync", "timeout", 100);
        let f = ErrorsFilter {
            source: Some("claim"),
            limit: 50,
            ..Default::default()
        };
        let errs = list_errors(&c, &f, 100).unwrap();
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].source, "claim");
    }

    #[test]
    fn list_errors_includes_expired() {
        let (_d, c) = open_tmp();
        insert_error(&c, "claim", "busy", 100);
        let f = ErrorsFilter {
            limit: 50,
            ..Default::default()
        };
        let errs = list_errors(&c, &f, 100 + 7 * 24 * 3600 + 1).unwrap();
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].state, "expired");
    }

    #[test]
    fn list_errors_bounded_by_limit() {
        let (_d, c) = open_tmp();
        for _ in 0..5 {
            insert_error(&c, "claim", "busy", 100);
        }
        let f = ErrorsFilter {
            limit: 3,
            ..Default::default()
        };
        let errs = list_errors(&c, &f, 100).unwrap();
        assert_eq!(errs.len(), 3);
    }

    #[test]
    fn list_errors_time_range() {
        let (_d, c) = open_tmp();
        insert_error(&c, "claim", "early", 100);
        insert_error(&c, "claim", "mid", 200);
        insert_error(&c, "claim", "late", 300);
        let f = ErrorsFilter {
            since_ts: Some(150),
            before_ts: Some(250),
            limit: 50,
            ..Default::default()
        };
        let errs = list_errors(&c, &f, 300).unwrap();
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].detail, "mid");
    }
}
