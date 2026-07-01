//! Daemon mailbox: agent-pushed control events consumed by the daemon tick loop.
//!
//! Each agent CLI invocation (`done`/`task-update`/`message`) writes one row; the daemon
//! polls unconsumed rows each tick and marks them consumed after acting. See spec §12.

use crate::clock;
use crate::db::begin_immediate;
use crate::error::Result;
use rusqlite::{params, Connection, Row};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MailboxKind {
    Done,
    TaskUpdate,
    Message,
}

impl MailboxKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Done => "done",
            Self::TaskUpdate => "task_update",
            Self::Message => "message",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "done" => Some(Self::Done),
            "task_update" => Some(Self::TaskUpdate),
            "message" => Some(Self::Message),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MailboxRow {
    pub agent: String,
    pub kind: MailboxKind,
    pub task_id: Option<i64>,
    pub pr: Option<i64>,
    pub verdict: Option<String>,
    pub feedback: Option<String>,
    pub note: Option<String>,
    pub to_agent: Option<String>,
    pub payload: Option<String>,
}

fn row_from_sql(r: &Row<'_>) -> rusqlite::Result<(i64, MailboxRow)> {
    let kind_str: String = r.get(2)?;
    let kind = MailboxKind::from_str(&kind_str).unwrap_or(MailboxKind::Message);
    Ok((
        r.get(0)?,
        MailboxRow {
            agent: r.get(1)?,
            kind,
            task_id: r.get(3)?,
            pr: r.get(4)?,
            verdict: r.get(5)?,
            feedback: r.get(6)?,
            note: r.get(7)?,
            to_agent: r.get(8)?,
            payload: r.get(9)?,
        },
    ))
}

pub fn append(conn: &mut Connection, row: &MailboxRow) -> Result<i64> {
    let now = clock::now();
    let tx = begin_immediate(conn)?;
    let id = {
        tx.query_row(
            "INSERT INTO mailbox (agent, kind, task_id, pr, verdict, feedback, note, to_agent, payload, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
             RETURNING id",
            params![
                row.agent,
                row.kind.as_str(),
                row.task_id,
                row.pr,
                row.verdict,
                row.feedback,
                row.note,
                row.to_agent,
                row.payload,
                now,
            ],
            |r| r.get(0),
        )?
    };
    tx.commit()?;
    Ok(id)
}

pub fn poll_unconsumed(conn: &Connection) -> Result<Vec<(i64, MailboxRow)>> {
    let mut stmt = conn.prepare(
        "SELECT id, agent, kind, task_id, pr, verdict, feedback, note, to_agent, payload
         FROM mailbox
         WHERE consumed_at IS NULL
         ORDER BY id",
    )?;
    let rows = stmt.query_map([], row_from_sql)?;
    let mut result = Vec::new();
    for r in rows {
        result.push(r?);
    }
    Ok(result)
}

pub fn mark_consumed(conn: &mut Connection, id: i64) -> Result<()> {
    let now = clock::now();
    let tx = begin_immediate(conn)?;
    tx.execute(
        "UPDATE mailbox SET consumed_at = ?1 WHERE id = ?2",
        params![now, id],
    )?;
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;

    fn test_conn() -> (Connection, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let conn = db::open(&dir.path().join("q.db")).unwrap();
        (conn, dir)
    }

    #[test]
    fn append_poll_mark_consumed() {
        let (mut conn, _dir) = test_conn();
        let row = MailboxRow {
            agent: "TestAgent".into(),
            kind: MailboxKind::Done,
            task_id: Some(42),
            pr: Some(100),
            verdict: None,
            feedback: None,
            note: None,
            to_agent: None,
            payload: None,
        };
        let id = append(&mut conn, &row).unwrap();
        assert!(id > 0);

        let unconsumed = poll_unconsumed(&conn).unwrap();
        assert_eq!(unconsumed.len(), 1);
        assert_eq!(unconsumed[0].0, id);
        assert_eq!(unconsumed[0].1.agent, "TestAgent");
        assert_eq!(unconsumed[0].1.kind, MailboxKind::Done);
        assert_eq!(unconsumed[0].1.task_id, Some(42));
        assert_eq!(unconsumed[0].1.pr, Some(100));

        mark_consumed(&mut conn, id).unwrap();
        let unconsumed = poll_unconsumed(&conn).unwrap();
        assert!(unconsumed.is_empty());
    }

    #[test]
    fn poll_returns_multiple_ordered_by_id() {
        let (mut conn, _dir) = test_conn();
        let row1 = MailboxRow {
            agent: "A".into(),
            kind: MailboxKind::Done,
            task_id: Some(1),
            pr: None,
            verdict: None,
            feedback: None,
            note: None,
            to_agent: None,
            payload: None,
        };
        let row2 = MailboxRow {
            agent: "B".into(),
            kind: MailboxKind::Message,
            task_id: None,
            pr: None,
            verdict: None,
            feedback: None,
            note: Some("hello".into()),
            to_agent: Some("A".into()),
            payload: None,
        };
        let id1 = append(&mut conn, &row1).unwrap();
        let id2 = append(&mut conn, &row2).unwrap();
        assert!(id2 > id1);

        let unconsumed = poll_unconsumed(&conn).unwrap();
        assert_eq!(unconsumed.len(), 2);
        assert_eq!(unconsumed[0].0, id1);
        assert_eq!(unconsumed[1].0, id2);
    }

    #[test]
    fn mark_consumed_is_selective() {
        let (mut conn, _dir) = test_conn();
        let row = MailboxRow {
            agent: "X".into(),
            kind: MailboxKind::TaskUpdate,
            task_id: Some(5),
            pr: None,
            verdict: None,
            feedback: None,
            note: None,
            to_agent: None,
            payload: None,
        };
        let id1 = append(&mut conn, &row).unwrap();
        let id2 = append(&mut conn, &row).unwrap();

        mark_consumed(&mut conn, id1).unwrap();
        let unconsumed = poll_unconsumed(&conn).unwrap();
        assert_eq!(unconsumed.len(), 1);
        assert_eq!(unconsumed[0].0, id2);
    }
}
