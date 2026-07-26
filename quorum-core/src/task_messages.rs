//! Run-targeted task messages with per-recipient delivery tracking.
//!
//! Messages are scoped to a task and addressed by agent run ID (from `agent_runs`), never
//! reusable agent name. A send creates one message row plus one delivery row per recipient.
//! Delivery rows are durable history (not TTL'd); messages are swept like the broadcast feed.

use crate::db::begin_immediate;
use crate::error::{QuorumError, Result};
use rusqlite::{params, Connection, Row};
use serde::Serialize;

/// Default TTL for task messages (48h, same as the broadcast feed).
pub const DEFAULT_TTL_SECS: i64 = 48 * 3600;

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct TaskMessage {
    pub id: i64,
    pub task_id: i64,
    pub sender_run_id: i64,
    pub sender_agent: String,
    pub kind: String,
    pub target_run_id: Option<i64>,
    pub body: String,
    pub recipient_count: i64,
    pub created_at: i64,
    pub expires_at: i64,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct Delivery {
    pub id: i64,
    pub message_id: i64,
    pub recipient_run_id: i64,
    pub recipient_agent: String,
    pub status: String,
    pub attempts: i64,
    pub last_attempt_at: Option<i64>,
    pub last_error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct DeliverySummary {
    pub delivery_id: i64,
    pub recipient_run_id: i64,
    pub recipient_agent: String,
}

#[derive(Debug, Serialize)]
pub enum SendResult {
    Sent {
        message_id: i64,
        recipient_count: i64,
        deliveries: Vec<DeliverySummary>,
        expires_at: i64,
    },
    TargetNotFound {
        agent: String,
    },
    TargetAmbiguous {
        agent: String,
        run_ids: Vec<i64>,
    },
}

impl SendResult {
    pub fn is_sent(&self) -> bool {
        matches!(self, SendResult::Sent { .. })
    }
}

struct ActiveRun {
    run_id: i64,
    agent_name: String,
}

fn validate_body(body: &str) -> Result<()> {
    if body.contains('\0') {
        return Err(QuorumError::BadInput("embedded NUL in message body".into()));
    }
    Ok(())
}

/// Snapshot active worker/R1/R2 runs for a task, excluding the sender's run.
fn snapshot_audience(
    conn: &Connection,
    task_id: i64,
    exclude_run_id: i64,
) -> Result<Vec<ActiveRun>> {
    let mut stmt = conn.prepare(
        "SELECT id, agent_name FROM agent_runs
         WHERE task_id = ?1 AND ended_at IS NULL AND id != ?2",
    )?;
    let runs = stmt
        .query_map(params![task_id, exclude_run_id], |r| {
            Ok(ActiveRun {
                run_id: r.get(0)?,
                agent_name: r.get(1)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(runs)
}

/// Resolve a target agent name to active run(s) on a task.
fn resolve_target(conn: &Connection, task_id: i64, target_agent: &str) -> Result<Vec<ActiveRun>> {
    let mut stmt = conn.prepare(
        "SELECT id, agent_name FROM agent_runs
         WHERE task_id = ?1 AND agent_name = ?2 AND ended_at IS NULL",
    )?;
    let runs = stmt
        .query_map(params![task_id, target_agent], |r| {
            Ok(ActiveRun {
                run_id: r.get(0)?,
                agent_name: r.get(1)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(runs)
}

#[allow(clippy::too_many_arguments)]
fn insert_message(
    conn: &Connection,
    task_id: i64,
    sender_run_id: i64,
    sender_agent: &str,
    kind: &str,
    target_run_id: Option<i64>,
    body: &str,
    recipient_count: i64,
    now: i64,
    expires_at: i64,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO task_messages(task_id, sender_run_id, sender_agent, kind,
             target_run_id, body, recipient_count, created_at, expires_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            task_id,
            sender_run_id,
            sender_agent,
            kind,
            target_run_id,
            body,
            recipient_count,
            now,
            expires_at
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

fn insert_delivery(
    conn: &Connection,
    message_id: i64,
    recipient_run_id: i64,
    recipient_agent: &str,
    now: i64,
) -> Result<i64> {
    conn.execute(
        "INSERT INTO task_message_deliveries(message_id, recipient_run_id,
             recipient_agent, status, attempts, created_at, updated_at)
         VALUES (?1, ?2, ?3, 'queued', 0, ?4, ?4)",
        params![message_id, recipient_run_id, recipient_agent, now],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Send a direct message to a named agent on a task.
#[allow(clippy::too_many_arguments)]
pub fn send_direct(
    conn: &mut Connection,
    task_id: i64,
    sender_run_id: i64,
    sender_agent: &str,
    target_agent: &str,
    body: &str,
    ttl: i64,
    now: i64,
) -> Result<SendResult> {
    validate_body(body)?;
    let tx = begin_immediate(conn)?;
    let targets = resolve_target(&tx, task_id, target_agent)?;
    match targets.len() {
        0 => {
            tx.commit()?;
            Ok(SendResult::TargetNotFound {
                agent: target_agent.to_string(),
            })
        }
        1 => {
            let target = &targets[0];
            let expires_at = now + ttl;
            let message_id = insert_message(
                &tx,
                task_id,
                sender_run_id,
                sender_agent,
                "direct",
                Some(target.run_id),
                body,
                1,
                now,
                expires_at,
            )?;
            let delivery_id =
                insert_delivery(&tx, message_id, target.run_id, &target.agent_name, now)?;
            tx.commit()?;
            Ok(SendResult::Sent {
                message_id,
                recipient_count: 1,
                deliveries: vec![DeliverySummary {
                    delivery_id,
                    recipient_run_id: target.run_id,
                    recipient_agent: target.agent_name.clone(),
                }],
                expires_at,
            })
        }
        _ => {
            let run_ids: Vec<i64> = targets.iter().map(|t| t.run_id).collect();
            tx.commit()?;
            Ok(SendResult::TargetAmbiguous {
                agent: target_agent.to_string(),
                run_ids,
            })
        }
    }
}

/// Send a broadcast to all eligible (worker/R1/R2) runs on a task.
///
/// Zero recipients is a clean success (recipient_count=0, no delivery rows).
pub fn send_broadcast(
    conn: &mut Connection,
    task_id: i64,
    sender_run_id: i64,
    sender_agent: &str,
    body: &str,
    ttl: i64,
    now: i64,
) -> Result<SendResult> {
    validate_body(body)?;
    let tx = begin_immediate(conn)?;
    let audience = snapshot_audience(&tx, task_id, sender_run_id)?;
    let expires_at = now + ttl;
    let recipient_count = audience.len() as i64;
    let message_id = insert_message(
        &tx,
        task_id,
        sender_run_id,
        sender_agent,
        "broadcast",
        None,
        body,
        recipient_count,
        now,
        expires_at,
    )?;
    let mut deliveries = Vec::with_capacity(audience.len());
    for run in &audience {
        let delivery_id = insert_delivery(&tx, message_id, run.run_id, &run.agent_name, now)?;
        deliveries.push(DeliverySummary {
            delivery_id,
            recipient_run_id: run.run_id,
            recipient_agent: run.agent_name.clone(),
        });
    }
    tx.commit()?;
    Ok(SendResult::Sent {
        message_id,
        recipient_count,
        deliveries,
        expires_at,
    })
}

// -- Query functions -----------------------------------------------------------------------

fn row_to_message(r: &Row) -> rusqlite::Result<TaskMessage> {
    Ok(TaskMessage {
        id: r.get(0)?,
        task_id: r.get(1)?,
        sender_run_id: r.get(2)?,
        sender_agent: r.get(3)?,
        kind: r.get(4)?,
        target_run_id: r.get(5)?,
        body: r.get(6)?,
        recipient_count: r.get(7)?,
        created_at: r.get(8)?,
        expires_at: r.get(9)?,
    })
}

fn row_to_delivery(r: &Row) -> rusqlite::Result<Delivery> {
    Ok(Delivery {
        id: r.get(0)?,
        message_id: r.get(1)?,
        recipient_run_id: r.get(2)?,
        recipient_agent: r.get(3)?,
        status: r.get(4)?,
        attempts: r.get(5)?,
        last_attempt_at: r.get(6)?,
        last_error: r.get(7)?,
        created_at: r.get(8)?,
        updated_at: r.get(9)?,
    })
}

const MSG_COLS: &str = "id, task_id, sender_run_id, sender_agent, kind, target_run_id, body, \
     recipient_count, created_at, expires_at";

const DEL_COLS: &str = "id, message_id, recipient_run_id, recipient_agent, status, attempts, \
     last_attempt_at, last_error, created_at, updated_at";

/// Unexpired messages for a task, newest first.
pub fn list_for_task(conn: &Connection, task_id: i64, now: i64) -> Result<Vec<TaskMessage>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {MSG_COLS} FROM task_messages
         WHERE task_id = ?1 AND expires_at > ?2
         ORDER BY id DESC"
    ))?;
    let msgs = stmt
        .query_map(params![task_id, now], row_to_message)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(msgs)
}

/// Single message by id (regardless of expiry, for inspection).
pub fn get_message(conn: &Connection, message_id: i64) -> Result<Option<TaskMessage>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {MSG_COLS} FROM task_messages WHERE id = ?1"
    ))?;
    let msg = stmt
        .query_row(params![message_id], row_to_message)
        .optional()?;
    Ok(msg)
}

/// All deliveries for a message.
pub fn deliveries_for_message(conn: &Connection, message_id: i64) -> Result<Vec<Delivery>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {DEL_COLS} FROM task_message_deliveries
         WHERE message_id = ?1 ORDER BY id ASC"
    ))?;
    let dels = stmt
        .query_map(params![message_id], row_to_delivery)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(dels)
}

/// Queued deliveries for a specific run, filtered to unexpired messages.
pub fn pending_for_run(conn: &Connection, run_id: i64, now: i64) -> Result<Vec<Delivery>> {
    let mut stmt = conn.prepare(
        "SELECT d.id, d.message_id, d.recipient_run_id, d.recipient_agent,
                d.status, d.attempts, d.last_attempt_at, d.last_error,
                d.created_at, d.updated_at
         FROM task_message_deliveries d
         JOIN task_messages m ON m.id = d.message_id
         WHERE d.recipient_run_id = ?1 AND d.status = 'queued' AND m.expires_at > ?2
         ORDER BY d.id ASC",
    )?;
    let dels = stmt
        .query_map(params![run_id, now], row_to_delivery)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(dels)
}

// -- Delivery state transitions ------------------------------------------------------------

/// Mark a delivery as successfully delivered.
pub fn mark_delivered(conn: &Connection, delivery_id: i64, now: i64) -> Result<bool> {
    let n = conn.execute(
        "UPDATE task_message_deliveries
         SET status = 'delivered', attempts = attempts + 1,
             last_attempt_at = ?1, updated_at = ?1
         WHERE id = ?2 AND status = 'queued'",
        params![now, delivery_id],
    )?;
    Ok(n > 0)
}

/// Mark a delivery as undeliverable (target exited, process died, etc.).
pub fn mark_undeliverable(
    conn: &Connection,
    delivery_id: i64,
    error: &str,
    now: i64,
) -> Result<bool> {
    let n = conn.execute(
        "UPDATE task_message_deliveries
         SET status = 'undeliverable', attempts = attempts + 1,
             last_attempt_at = ?1, last_error = ?2, updated_at = ?1
         WHERE id = ?3 AND status = 'queued'",
        params![now, error, delivery_id],
    )?;
    Ok(n > 0)
}

/// Transition queued deliveries to expired for messages past their TTL.
/// Must be called before sweeping task_messages so delivery rows capture the
/// terminal state before their parent is deleted.
pub fn expire_stale_deliveries(conn: &Connection, now: i64, limit: usize) -> Result<usize> {
    let n = conn.execute(
        "UPDATE task_message_deliveries SET status = 'expired', updated_at = ?1
         WHERE status = 'queued' AND message_id IN (
             SELECT id FROM task_messages WHERE expires_at <= ?1 LIMIT ?2
         )",
        params![now, limit as i64],
    )?;
    Ok(n)
}

use rusqlite::OptionalExtension;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_runs;

    fn open_tmp() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let c = crate::db::open(&dir.path().join("q.db")).unwrap();
        (dir, c)
    }

    /// Seed a task + two agent runs (worker + reviewer) and return
    /// (task_id, worker_run_id, reviewer_run_id).
    fn seed_task_with_runs(c: &mut Connection) -> (i64, i64, i64) {
        let task_id =
            crate::tasks::create(c, "boss", "test-task", None, 0, None, None, None, None, 100)
                .unwrap();
        let w = agent_runs::insert(
            c, task_id, "Alice", "worker", "opus-46", "high", "claude", 100,
        )
        .unwrap();
        let r = agent_runs::insert(
            c, task_id, "Bob", "reviewer", "opus-46", "high", "claude", 100,
        )
        .unwrap();
        (task_id, w, r)
    }

    // -- Basic send / receive ----------------------------------------------------------

    #[test]
    fn direct_send_creates_message_and_delivery() {
        let (_d, mut c) = open_tmp();
        let (tid, w_run, r_run) = seed_task_with_runs(&mut c);

        let result = send_direct(&mut c, tid, w_run, "Alice", "Bob", "hey", 1000, 200).unwrap();
        match &result {
            SendResult::Sent {
                message_id,
                recipient_count,
                deliveries,
                ..
            } => {
                assert_eq!(*recipient_count, 1);
                assert_eq!(deliveries.len(), 1);
                assert_eq!(deliveries[0].recipient_run_id, r_run);
                assert_eq!(deliveries[0].recipient_agent, "Bob");
                let msg = get_message(&c, *message_id).unwrap().unwrap();
                assert_eq!(msg.kind, "direct");
                assert_eq!(msg.target_run_id, Some(r_run));
                assert_eq!(msg.body, "hey");
            }
            _ => panic!("expected Sent"),
        }
    }

    #[test]
    fn broadcast_sends_to_all_except_sender() {
        let (_d, mut c) = open_tmp();
        let (tid, w_run, r_run) = seed_task_with_runs(&mut c);

        let result = send_broadcast(&mut c, tid, w_run, "Alice", "attention", 1000, 200).unwrap();
        match &result {
            SendResult::Sent {
                recipient_count,
                deliveries,
                ..
            } => {
                assert_eq!(*recipient_count, 1);
                assert_eq!(deliveries[0].recipient_run_id, r_run);
            }
            _ => panic!("expected Sent"),
        }
    }

    #[test]
    fn broadcast_includes_r2_runs() {
        let (_d, mut c) = open_tmp();
        let (tid, w_run, _r1_run) = seed_task_with_runs(&mut c);
        let r2_run =
            agent_runs::insert_r2(&c, tid, "Carol", "opus-46", "high", "claude", 100).unwrap();

        let result = send_broadcast(&mut c, tid, w_run, "Alice", "all hands", 1000, 200).unwrap();
        match &result {
            SendResult::Sent {
                recipient_count,
                deliveries,
                ..
            } => {
                assert_eq!(*recipient_count, 2);
                let run_ids: Vec<i64> = deliveries.iter().map(|d| d.recipient_run_id).collect();
                assert!(run_ids.contains(&_r1_run));
                assert!(run_ids.contains(&r2_run));
            }
            _ => panic!("expected Sent"),
        }
    }

    #[test]
    fn broadcast_excludes_ended_runs() {
        let (_d, mut c) = open_tmp();
        let (tid, w_run, r_run) = seed_task_with_runs(&mut c);
        agent_runs::close(&c, r_run, 150, "done").unwrap();

        let result = send_broadcast(&mut c, tid, w_run, "Alice", "hello", 1000, 200).unwrap();
        match &result {
            SendResult::Sent {
                recipient_count, ..
            } => {
                assert_eq!(*recipient_count, 0);
            }
            _ => panic!("expected Sent"),
        }
    }

    // -- Edge cases: target resolution -------------------------------------------------

    #[test]
    fn direct_to_missing_agent_returns_target_not_found() {
        let (_d, mut c) = open_tmp();
        let (tid, w_run, _) = seed_task_with_runs(&mut c);

        let result = send_direct(&mut c, tid, w_run, "Alice", "Nobody", "hi", 1000, 200).unwrap();
        assert!(matches!(result, SendResult::TargetNotFound { .. }));
    }

    #[test]
    fn direct_to_ambiguous_agent_returns_ambiguous() {
        let (_d, mut c) = open_tmp();
        let (tid, w_run, _) = seed_task_with_runs(&mut c);
        // Two active runs with the same agent name (unusual but possible)
        agent_runs::insert(&c, tid, "Bob", "reviewer", "opus-46", "high", "claude", 100).unwrap();

        let result = send_direct(&mut c, tid, w_run, "Alice", "Bob", "hi", 1000, 200).unwrap();
        match &result {
            SendResult::TargetAmbiguous { agent, run_ids } => {
                assert_eq!(agent, "Bob");
                assert_eq!(run_ids.len(), 2);
            }
            _ => panic!("expected TargetAmbiguous"),
        }
    }

    #[test]
    fn direct_to_ended_agent_returns_not_found() {
        let (_d, mut c) = open_tmp();
        let (tid, w_run, r_run) = seed_task_with_runs(&mut c);
        agent_runs::close(&c, r_run, 150, "done").unwrap();

        let result = send_direct(&mut c, tid, w_run, "Alice", "Bob", "hi", 1000, 200).unwrap();
        assert!(matches!(result, SendResult::TargetNotFound { .. }));
    }

    // -- Zero-recipient broadcast ------------------------------------------------------

    #[test]
    fn broadcast_zero_recipients_succeeds_with_zero_count() {
        let (_d, mut c) = open_tmp();
        let task_id = crate::tasks::create(
            &mut c, "boss", "lonely", None, 0, None, None, None, None, 100,
        )
        .unwrap();
        let solo = agent_runs::insert(
            &c, task_id, "Solo", "worker", "opus-46", "high", "claude", 100,
        )
        .unwrap();

        let result = send_broadcast(&mut c, task_id, solo, "Solo", "echo", 1000, 200).unwrap();
        match &result {
            SendResult::Sent {
                recipient_count,
                deliveries,
                ..
            } => {
                assert_eq!(*recipient_count, 0);
                assert!(deliveries.is_empty());
            }
            _ => panic!("expected Sent"),
        }
    }

    // -- Delivery state transitions ----------------------------------------------------

    #[test]
    fn mark_delivered_transitions_queued() {
        let (_d, mut c) = open_tmp();
        let (tid, w_run, _) = seed_task_with_runs(&mut c);
        let result = send_direct(&mut c, tid, w_run, "Alice", "Bob", "x", 1000, 200).unwrap();
        let del_id = match &result {
            SendResult::Sent { deliveries, .. } => deliveries[0].delivery_id,
            _ => panic!("expected Sent"),
        };

        assert!(mark_delivered(&c, del_id, 300).unwrap());
        let dels = deliveries_for_message(&c, 1).unwrap();
        assert_eq!(dels[0].status, "delivered");
        assert_eq!(dels[0].attempts, 1);
        assert_eq!(dels[0].last_attempt_at, Some(300));
    }

    #[test]
    fn mark_delivered_is_idempotent_on_already_delivered() {
        let (_d, mut c) = open_tmp();
        let (tid, w_run, _) = seed_task_with_runs(&mut c);
        let result = send_direct(&mut c, tid, w_run, "Alice", "Bob", "x", 1000, 200).unwrap();
        let del_id = match &result {
            SendResult::Sent { deliveries, .. } => deliveries[0].delivery_id,
            _ => panic!("expected Sent"),
        };

        assert!(mark_delivered(&c, del_id, 300).unwrap());
        assert!(!mark_delivered(&c, del_id, 400).unwrap());
    }

    #[test]
    fn mark_undeliverable_records_error() {
        let (_d, mut c) = open_tmp();
        let (tid, w_run, _) = seed_task_with_runs(&mut c);
        let result = send_direct(&mut c, tid, w_run, "Alice", "Bob", "x", 1000, 200).unwrap();
        let del_id = match &result {
            SendResult::Sent { deliveries, .. } => deliveries[0].delivery_id,
            _ => panic!("expected Sent"),
        };

        assert!(mark_undeliverable(&c, del_id, "process exited", 300).unwrap());
        let dels = deliveries_for_message(&c, 1).unwrap();
        assert_eq!(dels[0].status, "undeliverable");
        assert_eq!(dels[0].last_error.as_deref(), Some("process exited"));
    }

    // -- Expiry -----------------------------------------------------------------------

    #[test]
    fn expire_stale_marks_queued_deliveries_expired() {
        let (_d, mut c) = open_tmp();
        let (tid, w_run, _) = seed_task_with_runs(&mut c);
        send_direct(&mut c, tid, w_run, "Alice", "Bob", "short-lived", 10, 200).unwrap();

        let count = expire_stale_deliveries(&c, 210, 100).unwrap();
        assert_eq!(count, 1);
        let dels = deliveries_for_message(&c, 1).unwrap();
        assert_eq!(dels[0].status, "expired");
    }

    #[test]
    fn expire_stale_skips_already_delivered() {
        let (_d, mut c) = open_tmp();
        let (tid, w_run, _) = seed_task_with_runs(&mut c);
        let result = send_direct(
            &mut c,
            tid,
            w_run,
            "Alice",
            "Bob",
            "delivered-first",
            10,
            200,
        )
        .unwrap();
        let del_id = match &result {
            SendResult::Sent { deliveries, .. } => deliveries[0].delivery_id,
            _ => panic!("expected Sent"),
        };
        mark_delivered(&c, del_id, 205).unwrap();

        let count = expire_stale_deliveries(&c, 210, 100).unwrap();
        assert_eq!(count, 0);
        let dels = deliveries_for_message(&c, 1).unwrap();
        assert_eq!(dels[0].status, "delivered");
    }

    #[test]
    fn list_for_task_filters_expired_messages() {
        let (_d, mut c) = open_tmp();
        let (tid, w_run, _) = seed_task_with_runs(&mut c);
        send_direct(&mut c, tid, w_run, "Alice", "Bob", "expires-soon", 10, 200).unwrap();
        send_direct(&mut c, tid, w_run, "Alice", "Bob", "lives-long", 1000, 200).unwrap();

        let msgs = list_for_task(&c, tid, 210).unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].body, "lives-long");
    }

    #[test]
    fn pending_for_run_filters_expired() {
        let (_d, mut c) = open_tmp();
        let (tid, w_run, r_run) = seed_task_with_runs(&mut c);
        send_direct(&mut c, tid, w_run, "Alice", "Bob", "short", 10, 200).unwrap();
        send_direct(&mut c, tid, w_run, "Alice", "Bob", "long", 1000, 200).unwrap();

        let pending = pending_for_run(&c, r_run, 210).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].message_id, 2);
    }

    // -- Text safety -------------------------------------------------------------------

    #[test]
    fn body_roundtrips_byte_exact() {
        let (_d, mut c) = open_tmp();
        let (tid, w_run, _) = seed_task_with_runs(&mut c);
        let body = "héllo \"world\"\n`$x`\n";
        send_direct(&mut c, tid, w_run, "Alice", "Bob", body, 1000, 200).unwrap();
        let msg = get_message(&c, 1).unwrap().unwrap();
        assert_eq!(msg.body, body);
    }

    #[test]
    fn rejects_nul_in_body() {
        let (_d, mut c) = open_tmp();
        let (tid, w_run, _) = seed_task_with_runs(&mut c);
        let result = send_direct(&mut c, tid, w_run, "Alice", "Bob", "a\0b", 1000, 200);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().exit_code(), 2);
    }

    #[test]
    fn rejects_nul_in_broadcast_body() {
        let (_d, mut c) = open_tmp();
        let (tid, w_run, _) = seed_task_with_runs(&mut c);
        let result = send_broadcast(&mut c, tid, w_run, "Alice", "a\0b", 1000, 200);
        assert!(result.is_err());
    }

    // -- Concurrent sends (race safety) ------------------------------------------------

    #[test]
    fn concurrent_sends_all_succeed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");
        {
            let mut c = crate::db::open(&path).unwrap();
            let (tid, w_run, r_run) = seed_task_with_runs(&mut c);
            // Pre-seed is done; now N threads each open a fresh connection and send.
            let _ = (tid, w_run, r_run);
        }

        let handles: Vec<_> = (0..8)
            .map(|i| {
                let p = path.clone();
                std::thread::spawn(move || {
                    let mut c = crate::db::open(&p).unwrap();
                    // Each thread sends a direct message. All should succeed because
                    // there's no uniqueness constraint across sends.
                    let result = send_direct(
                        &mut c,
                        1, // task_id from seed
                        1, // w_run from seed
                        "Alice",
                        "Bob",
                        &format!("msg-{i}"),
                        1000,
                        200 + i,
                    );
                    result.unwrap()
                })
            })
            .collect();

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert!(results.iter().all(|r| r.is_sent()));

        let c = crate::db::open(&path).unwrap();
        let msgs = list_for_task(&c, 1, 300).unwrap();
        assert_eq!(msgs.len(), 8, "all 8 concurrent sends must persist");
    }

    #[test]
    fn concurrent_broadcasts_snapshot_independently() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");
        {
            let mut c = crate::db::open(&path).unwrap();
            let tid =
                crate::tasks::create(&mut c, "boss", "t", None, 0, None, None, None, None, 100)
                    .unwrap();
            agent_runs::insert(&c, tid, "W1", "worker", "m", "h", "claude", 100).unwrap();
            agent_runs::insert(&c, tid, "R1", "reviewer", "m", "h", "claude", 100).unwrap();
            agent_runs::insert_r2(&c, tid, "R2", "m", "h", "claude", 100).unwrap();
        }

        let handles: Vec<_> = (0..4)
            .map(|i| {
                let p = path.clone();
                std::thread::spawn(move || {
                    let mut c = crate::db::open(&p).unwrap();
                    send_broadcast(&mut c, 1, 1, "W1", &format!("bcast-{i}"), 1000, 200 + i)
                        .unwrap()
                })
            })
            .collect();

        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        for r in &results {
            match r {
                SendResult::Sent {
                    recipient_count, ..
                } => assert_eq!(*recipient_count, 2),
                _ => panic!("expected Sent"),
            }
        }

        let c = crate::db::open(&path).unwrap();
        let del_count: i64 = c
            .query_row("SELECT count(*) FROM task_message_deliveries", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(
            del_count, 8,
            "4 broadcasts × 2 recipients each = 8 deliveries"
        );
    }

    // -- get_message / deliveries_for_message ------------------------------------------

    #[test]
    fn get_message_returns_none_for_missing() {
        let (_d, c) = open_tmp();
        assert!(get_message(&c, 999).unwrap().is_none());
    }

    #[test]
    fn deliveries_for_message_returns_all() {
        let (_d, mut c) = open_tmp();
        let tid = crate::tasks::create(&mut c, "boss", "t", None, 0, None, None, None, None, 100)
            .unwrap();
        let w = agent_runs::insert(&c, tid, "W", "worker", "m", "h", "claude", 100).unwrap();
        agent_runs::insert(&c, tid, "R1", "reviewer", "m", "h", "claude", 100).unwrap();
        agent_runs::insert(&c, tid, "R2", "reviewer", "m", "h", "claude", 100).unwrap();

        let result = send_broadcast(&mut c, tid, w, "W", "hi all", 1000, 200).unwrap();
        let msg_id = match &result {
            SendResult::Sent { message_id, .. } => *message_id,
            _ => panic!("expected Sent"),
        };

        let dels = deliveries_for_message(&c, msg_id).unwrap();
        assert_eq!(dels.len(), 2);
        assert!(dels.iter().all(|d| d.status == "queued"));
    }
}
