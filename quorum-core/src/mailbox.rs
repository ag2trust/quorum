//! Daemon mailbox: agent-pushed control events consumed by the daemon tick loop.
//!
//! Each agent CLI invocation (`done`/`task-update`/`message`) writes one row; the daemon
//! polls unconsumed rows each tick and marks them consumed after acting. See spec §12.

use crate::clock;
use crate::db::begin_immediate;
use crate::error::Result;
use rusqlite::{params, types::Type, Connection, Row};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MailboxKind {
    Done,
    Message,
    TaskUpdate,
    Kill,
    ReviewDraft,
}

impl MailboxKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Done => "done",
            Self::Message => "message",
            Self::TaskUpdate => "task_update",
            Self::Kill => "kill",
            Self::ReviewDraft => "review_draft",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "done" => Some(Self::Done),
            "message" => Some(Self::Message),
            "task_update" => Some(Self::TaskUpdate),
            "kill" => Some(Self::Kill),
            "review_draft" => Some(Self::ReviewDraft),
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
    let kind = MailboxKind::from_str(&kind_str).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            2,
            Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("unknown mailbox kind {kind_str:?}"),
            )),
        )
    })?;
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

/// True if `agent` has at least one unconsumed row of `kind` waiting for the
/// daemon. Lets a caller tell "this agent already signalled" apart from "this
/// agent's task status has not caught up yet": the row lands at CLI-invocation
/// time, the status only changes when the daemon drains it.
pub fn has_unconsumed(
    conn: &Connection,
    agent: &str,
    kind: MailboxKind,
    task_id: i64,
) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM mailbox
         WHERE agent = ?1 AND kind = ?2 AND task_id = ?3 AND consumed_at IS NULL)",
        params![agent, kind.as_str(), task_id],
        |r| r.get(0),
    )?)
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

/// Consume all unconsumed mailbox rows for a given agent name. Returns the
/// number of rows consumed. Used at agent-spawn time to drain stale rows
/// and prevent phantom verdicts from name reuse.
pub fn consume_all_for_agent(conn: &mut Connection, agent: &str) -> Result<usize> {
    let now = clock::now();
    let tx = begin_immediate(conn)?;
    let count = tx.execute(
        "UPDATE mailbox SET consumed_at = ?1 WHERE agent = ?2 AND consumed_at IS NULL",
        params![now, agent],
    )?;
    tx.commit()?;
    Ok(count)
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
    fn poll_returns_multiple_done_rows_ordered_by_id() {
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
            kind: MailboxKind::Done,
            task_id: Some(2),
            pr: None,
            verdict: None,
            feedback: None,
            note: None,
            to_agent: None,
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
            kind: MailboxKind::Done,
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

    #[test]
    fn message_kind_round_trips() {
        let (mut conn, _dir) = test_conn();
        let row = MailboxRow {
            agent: "Sender".into(),
            kind: MailboxKind::Message,
            task_id: None,
            pr: None,
            verdict: None,
            feedback: None,
            note: None,
            to_agent: Some("Target".into()),
            payload: Some("hello from sender".into()),
        };
        let id = append(&mut conn, &row).unwrap();

        let unconsumed = poll_unconsumed(&conn).unwrap();
        assert_eq!(unconsumed.len(), 1);
        assert_eq!(unconsumed[0].0, id);
        assert_eq!(unconsumed[0].1.kind, MailboxKind::Message);
        assert_eq!(unconsumed[0].1.to_agent.as_deref(), Some("Target"));
        assert_eq!(
            unconsumed[0].1.payload.as_deref(),
            Some("hello from sender")
        );
    }

    #[test]
    fn kill_kind_round_trips() {
        let (mut conn, _dir) = test_conn();
        let row = MailboxRow {
            agent: "Admin".into(),
            kind: MailboxKind::Kill,
            task_id: None,
            pr: None,
            verdict: None,
            feedback: None,
            note: Some("zombie worker".into()),
            to_agent: Some("Zombie".into()),
            payload: None,
        };
        let id = append(&mut conn, &row).unwrap();

        let unconsumed = poll_unconsumed(&conn).unwrap();
        assert_eq!(unconsumed.len(), 1);
        assert_eq!(unconsumed[0].0, id);
        assert_eq!(unconsumed[0].1.kind, MailboxKind::Kill);
        assert_eq!(unconsumed[0].1.agent, "Admin");
        assert_eq!(unconsumed[0].1.to_agent.as_deref(), Some("Zombie"));
        assert_eq!(unconsumed[0].1.note.as_deref(), Some("zombie worker"));
    }

    #[test]
    fn task_update_kind_round_trips() {
        let (mut conn, _dir) = test_conn();
        let row = MailboxRow {
            agent: "Worker1".into(),
            kind: MailboxKind::TaskUpdate,
            task_id: Some(42),
            pr: None,
            verdict: None,
            feedback: None,
            note: Some("blocked".into()),
            to_agent: None,
            payload: None,
        };
        let id = append(&mut conn, &row).unwrap();

        let unconsumed = poll_unconsumed(&conn).unwrap();
        assert_eq!(unconsumed.len(), 1);
        assert_eq!(unconsumed[0].0, id);
        assert_eq!(unconsumed[0].1.kind, MailboxKind::TaskUpdate);
        assert_eq!(unconsumed[0].1.agent, "Worker1");
        assert_eq!(unconsumed[0].1.task_id, Some(42));
        assert_eq!(unconsumed[0].1.note.as_deref(), Some("blocked"));
    }

    #[test]
    fn review_draft_kind_round_trips_without_becoming_done() {
        let (mut conn, _dir) = test_conn();
        let row = MailboxRow {
            agent: "Reviewer1".into(),
            kind: MailboxKind::ReviewDraft,
            task_id: Some(42),
            pr: Some(77),
            verdict: None,
            feedback: Some("Need another analysis turn for the blocking path.".into()),
            note: None,
            to_agent: None,
            payload: Some("{\"blocking\":1}".into()),
        };
        append(&mut conn, &row).unwrap();

        let unconsumed = poll_unconsumed(&conn).unwrap();
        assert_eq!(unconsumed.len(), 1);
        assert_eq!(unconsumed[0].1.kind, MailboxKind::ReviewDraft);
        assert_ne!(unconsumed[0].1.kind, MailboxKind::Done);
    }

    #[test]
    fn unknown_mailbox_kind_fails_closed_instead_of_decoding_as_done() {
        let (conn, _dir) = test_conn();
        conn.execute(
            "INSERT INTO mailbox(agent,kind,created_at) VALUES ('legacy','unknown_kind',1)",
            [],
        )
        .unwrap();

        let err = poll_unconsumed(&conn).unwrap_err();
        assert!(err.to_string().contains("unknown mailbox kind"), "{err}");
    }

    #[test]
    fn poll_returns_all_kinds_ordered() {
        let (mut conn, _dir) = test_conn();
        let r1 = MailboxRow {
            agent: "A".into(),
            kind: MailboxKind::TaskUpdate,
            task_id: Some(1),
            pr: None,
            verdict: None,
            feedback: None,
            note: Some("needs-info".into()),
            to_agent: None,
            payload: None,
        };
        let r2 = MailboxRow {
            agent: "B".into(),
            kind: MailboxKind::Message,
            task_id: None,
            pr: None,
            verdict: None,
            feedback: None,
            note: None,
            to_agent: Some("C".into()),
            payload: Some("ping".into()),
        };
        let r3 = MailboxRow {
            agent: "D".into(),
            kind: MailboxKind::Done,
            task_id: Some(2),
            pr: Some(99),
            verdict: None,
            feedback: None,
            note: None,
            to_agent: None,
            payload: None,
        };
        append(&mut conn, &r1).unwrap();
        append(&mut conn, &r2).unwrap();
        append(&mut conn, &r3).unwrap();

        let unconsumed = poll_unconsumed(&conn).unwrap();
        assert_eq!(unconsumed.len(), 3);
        assert_eq!(unconsumed[0].1.kind, MailboxKind::TaskUpdate);
        assert_eq!(unconsumed[1].1.kind, MailboxKind::Message);
        assert_eq!(unconsumed[2].1.kind, MailboxKind::Done);
    }

    #[test]
    fn consume_all_for_agent_drains_only_that_agent() {
        let (mut conn, _dir) = test_conn();
        let row_a = MailboxRow {
            agent: "Alpha".into(),
            kind: MailboxKind::Done,
            task_id: Some(1),
            pr: None,
            verdict: None,
            feedback: None,
            note: None,
            to_agent: None,
            payload: None,
        };
        let row_b = MailboxRow {
            agent: "Beta".into(),
            kind: MailboxKind::Done,
            task_id: Some(2),
            pr: None,
            verdict: None,
            feedback: None,
            note: None,
            to_agent: None,
            payload: None,
        };
        append(&mut conn, &row_a).unwrap();
        append(&mut conn, &row_a).unwrap();
        append(&mut conn, &row_b).unwrap();

        let count = consume_all_for_agent(&mut conn, "Alpha").unwrap();
        assert_eq!(count, 2);

        let unconsumed = poll_unconsumed(&conn).unwrap();
        assert_eq!(unconsumed.len(), 1);
        assert_eq!(unconsumed[0].1.agent, "Beta");
    }

    #[test]
    fn has_unconsumed_matches_agent_and_kind_until_consumed() {
        let (mut conn, _dir) = test_conn();
        let done = MailboxRow {
            agent: "Spool".into(),
            kind: MailboxKind::Done,
            task_id: Some(218),
            pr: Some(439),
            verdict: None,
            feedback: None,
            note: None,
            to_agent: None,
            payload: None,
        };
        let id = append(&mut conn, &done).unwrap();

        assert!(has_unconsumed(&conn, "Spool", MailboxKind::Done, 218).unwrap());
        assert!(
            !has_unconsumed(&conn, "Other", MailboxKind::Done, 218).unwrap(),
            "another agent's row must not count"
        );
        assert!(
            !has_unconsumed(&conn, "Spool", MailboxKind::TaskUpdate, 218).unwrap(),
            "another kind must not count"
        );
        assert!(
            !has_unconsumed(&conn, "Spool", MailboxKind::Done, 219).unwrap(),
            "another task must not count"
        );

        mark_consumed(&mut conn, id).unwrap();
        assert!(
            !has_unconsumed(&conn, "Spool", MailboxKind::Done, 218).unwrap(),
            "consumed row must not count"
        );
    }

    /// T6: if mark_consumed is skipped (simulating failure), poll_unconsumed
    /// returns the same row on the next poll — the replay path used by the
    /// daemon when consume_mailbox_row returns false.
    #[test]
    fn unconsumed_row_replays_on_next_poll() {
        let (mut conn, _dir) = test_conn();
        let row = MailboxRow {
            agent: "Worker".into(),
            kind: MailboxKind::Done,
            task_id: Some(1),
            pr: Some(10),
            verdict: None,
            feedback: None,
            note: None,
            to_agent: None,
            payload: None,
        };
        let id = append(&mut conn, &row).unwrap();

        let first = poll_unconsumed(&conn).unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].0, id);

        // Simulate consume failure: skip mark_consumed.
        // Second poll must return the same row (at-least-once delivery).
        let second = poll_unconsumed(&conn).unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].0, id, "same row must be returned on replay");

        // After successful consume, row disappears.
        mark_consumed(&mut conn, id).unwrap();
        let third = poll_unconsumed(&conn).unwrap();
        assert!(third.is_empty());
    }

    /// T6: mark_consumed is idempotent — calling it twice on the same row
    /// does not error (the row is already consumed, UPDATE is a no-op).
    #[test]
    fn mark_consumed_idempotent() {
        let (mut conn, _dir) = test_conn();
        let row = MailboxRow {
            agent: "X".into(),
            kind: MailboxKind::Done,
            task_id: Some(1),
            pr: None,
            verdict: None,
            feedback: None,
            note: None,
            to_agent: None,
            payload: None,
        };
        let id = append(&mut conn, &row).unwrap();

        mark_consumed(&mut conn, id).unwrap();
        // Second call must not fail.
        mark_consumed(&mut conn, id).unwrap();

        let unconsumed = poll_unconsumed(&conn).unwrap();
        assert!(unconsumed.is_empty());
    }
}
