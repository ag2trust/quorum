//! Best-effort logging of *abnormal* failures to the `errors` table.
//!
//! Only genuine failures go here (DB errors, post-timeout BUSY, bad input, migration
//! refusal) — never a normal lost-race / not-holder (those are expected operation). Logging
//! is best-effort: a failure to log must never mask or replace the original error.
//!
//! [`log_lifecycle_diagnostic`] extends this for daemon-managed lifecycle failures: when the
//! daemon's own `fire_event` is rejected, the exact diagnostic is persisted to `errors`,
//! `task_notes`, and an owner alert in `messages` — so `task-get` and `quorum status`
//! surface the evidence after terminal output is gone.

use rusqlite::{params, Connection};

/// Logged errors are reclaimed after this long. Default; Phase 6 config overrides.
pub const ERROR_TTL_SECS: i64 = 7 * 24 * 3600;

/// Append an error row. Best-effort: any failure here is swallowed.
pub fn log_error(conn: &Connection, now: i64, source: &str, detail: &str) {
    let _ = conn.execute(
        "INSERT INTO errors(ts, source, detail, expires_at) VALUES (?1,?2,?3,?4)",
        params![now, source, detail, now + ERROR_TTL_SECS],
    );
}

/// Context for a daemon-managed lifecycle failure diagnostic.
pub struct LifecycleDiagnostic<'a> {
    pub task_id: i64,
    pub event: &'a str,
    pub error: &'a str,
    pub actor: &'a str,
    pub status: &'a str,
    pub assignee: Option<&'a str>,
    pub author: Option<&'a str>,
    pub reviewer: Option<&'a str>,
    pub pr: Option<&'a str>,
    pub run_id: Option<i64>,
    pub recovery_attempts: i64,
}

/// Persist a managed lifecycle failure diagnostic to errors + task_notes + owner alert.
///
/// Called from the daemon's `fire_event` when `apply_event` returns an error for a
/// daemon-managed transition (which is always abnormal — the daemon believed it owned
/// the lifecycle). Best-effort: failures here are swallowed.
pub fn log_lifecycle_diagnostic(conn: &mut Connection, now: i64, d: &LifecycleDiagnostic) {
    let Ok(tx) = crate::db::begin_immediate(conn) else {
        return;
    };

    let detail = format!(
        concat!(
            "{{\"task\":{},\"event\":\"{}\",\"error\":\"{}\",",
            "\"actor\":\"{}\",\"status\":\"{}\",",
            "\"assignee\":{},\"author\":{},\"reviewer\":{},",
            "\"pr\":{},\"run_id\":{},\"recovery_attempts\":{}}}"
        ),
        d.task_id,
        json_escape(d.event),
        json_escape(d.error),
        json_escape(d.actor),
        json_escape(d.status),
        opt_json(d.assignee),
        opt_json(d.author),
        opt_json(d.reviewer),
        opt_json(d.pr),
        d.run_id.map_or("null".to_string(), |v| v.to_string()),
        d.recovery_attempts,
    );

    // 1. errors row
    let _ = tx.execute(
        "INSERT INTO errors(ts, source, detail, expires_at) VALUES (?1,?2,?3,?4)",
        params![now, "lifecycle", &detail, now + ERROR_TTL_SECS],
    );

    // 2. task_notes row (survives indefinitely with the task — no TTL)
    let note_body = format!(
        "lifecycle failure: event={} error={} actor={} status={} run_id={}",
        d.event,
        d.error,
        d.actor,
        d.status,
        d.run_id.map_or("none".to_string(), |v| v.to_string()),
    );
    let _ = tx.execute(
        "INSERT INTO task_notes(task_id, ts, agent, body) VALUES (?1, ?2, ?3, ?4)",
        params![d.task_id, now, "system", &note_body],
    );

    // 3. owner alert (visible in `quorum status` ALERTS section)
    let alert_body = format!(
        "task #{}: lifecycle failure: {} rejected ({}) — actor={}, status={}, pr={}, recovery {}/3",
        d.task_id,
        d.event,
        d.error,
        d.actor,
        d.status,
        d.pr.unwrap_or("none"),
        d.recovery_attempts,
    );
    let expires_at = now + crate::feed::DEFAULT_MESSAGE_TTL_SECS;
    let _ = tx.execute(
        "INSERT INTO messages(ts, author, topic, kind, body, refs, expires_at, recipient) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            now,
            "system",
            crate::feed::DEFAULT_TOPIC,
            "alert",
            &alert_body,
            format!("task:{}", d.task_id),
            expires_at,
            "owner"
        ],
    );

    // 4. event (visible in `quorum log`)
    let _ = crate::events::emit(
        &tx,
        "lifecycle_failure",
        &format!("task#{}", d.task_id),
        &format!("{} rejected: {}", d.event, d.error),
        now,
    );

    let _ = tx.commit();
}

/// All-in-one: gather task context from the DB and persist the diagnostic.
/// Used by the daemon's `fire_event` which has only (db_path, agent, task_id, event, error).
pub fn persist_lifecycle_diagnostic(
    conn: &mut Connection,
    now: i64,
    actor: &str,
    task_id: i64,
    event_desc: &str,
    error_msg: &str,
) {
    #[allow(clippy::type_complexity)]
    let task_ctx: Option<(
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        i64,
    )> = conn
        .query_row(
            "SELECT status, assignee, author, reviewer, refs, recovery_attempts \
             FROM tasks WHERE id=?1",
            params![task_id],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                ))
            },
        )
        .ok();

    let (status, assignee, author, reviewer, refs, recovery_attempts) =
        task_ctx.unwrap_or_else(|| ("unknown".to_string(), None, None, None, None, 0));

    let pr = refs
        .as_deref()
        .and_then(|r| r.split(',').find(|s| s.starts_with("pr:")).map(|s| &s[3..]));

    let run_id: Option<i64> = conn
        .query_row(
            "SELECT id FROM agent_runs \
             WHERE task_id=?1 AND agent_name=?2 AND ended_at IS NULL \
             ORDER BY id DESC LIMIT 1",
            params![task_id, actor],
            |r| r.get(0),
        )
        .ok();

    let diag = LifecycleDiagnostic {
        task_id,
        event: event_desc,
        error: error_msg,
        actor,
        status: &status,
        assignee: assignee.as_deref(),
        author: author.as_deref(),
        reviewer: reviewer.as_deref(),
        pr,
        run_id,
        recovery_attempts,
    };
    log_lifecycle_diagnostic(conn, now, &diag);
}

fn json_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn opt_json(s: Option<&str>) -> String {
    match s {
        Some(v) => format!("\"{}\"", json_escape(v)),
        None => "null".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_error_inserts_row() {
        let dir = tempfile::tempdir().unwrap();
        let c = crate::db::open(&dir.path().join("q.db")).unwrap();
        log_error(&c, 100, "claim", "database busy after timeout");
        let (source, detail, expires): (String, String, i64) = c
            .query_row("SELECT source, detail, expires_at FROM errors", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .unwrap();
        assert_eq!(source, "claim");
        assert_eq!(detail, "database busy after timeout");
        assert_eq!(expires, 100 + ERROR_TTL_SECS);
    }

    #[test]
    fn lifecycle_diagnostic_persists_all_artifacts() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = crate::db::open(&dir.path().join("q.db")).unwrap();
        let now = 1000i64;

        // Create a task with context (assignee, author, reviewer, refs with PR).
        let task_id = crate::tasks::create(
            &mut conn,
            "boss",
            "test task",
            None,
            0,
            None,
            None,
            None,
            None,
            now,
        )
        .unwrap();
        // Set fields that fire_event would have populated.
        conn.execute(
            "UPDATE tasks SET status='working', assignee='worker-1', author='worker-1', \
             reviewer='reviewer-1', refs='pr:42,branch:feat', recovery_attempts=2 \
             WHERE id=?1",
            params![task_id],
        )
        .unwrap();
        // Insert an active agent_run.
        crate::agent_runs::insert(
            &conn, task_id, "worker-1", "worker", "opus-46", "high", "claude", now,
        )
        .unwrap();

        // Fire the diagnostic.
        persist_lifecycle_diagnostic(
            &mut conn,
            now,
            "worker-1",
            task_id,
            "SignaledDone { pr: \"42\" }",
            "not the current holder",
        );

        // 1. errors row exists with source="lifecycle" and structured JSON detail.
        let (source, detail): (String, String) = conn
            .query_row(
                "SELECT source, detail FROM errors WHERE source='lifecycle'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(source, "lifecycle");
        assert!(detail.contains("\"task\":"));
        assert!(detail.contains("\"error\":\"not the current holder\""));
        assert!(detail.contains("\"status\":\"working\""));
        assert!(detail.contains("\"pr\":\"42\""));
        assert!(detail.contains("\"recovery_attempts\":2"));

        // 2. task_notes row with the failure detail.
        let note_body: String = conn
            .query_row(
                "SELECT body FROM task_notes WHERE task_id=?1 AND agent='system'",
                params![task_id],
                |r| r.get(0),
            )
            .unwrap();
        assert!(note_body.contains("lifecycle failure:"));
        assert!(note_body.contains("not the current holder"));
        assert!(note_body.contains("worker-1"));

        // 3. owner alert in messages.
        let alert: String = conn
            .query_row(
                "SELECT body FROM messages WHERE kind='alert' AND recipient='owner'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(alert.contains("lifecycle failure:"));
        assert!(alert.contains("not the current holder"));
        assert!(alert.contains("pr=42"));
        assert!(alert.contains("recovery 2/3"));

        // 4. event row.
        let (kind, body): (String, String) = conn
            .query_row(
                "SELECT kind, body FROM events WHERE kind='lifecycle_failure'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(kind, "lifecycle_failure");
        assert!(body.contains("rejected"));
        assert!(body.contains("not the current holder"));
    }

    #[test]
    fn lifecycle_diagnostic_works_without_task_context() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = crate::db::open(&dir.path().join("q.db")).unwrap();
        let now = 2000i64;

        // Call with a non-existent task — should not panic, still persists what it can.
        persist_lifecycle_diagnostic(
            &mut conn,
            now,
            "ghost-agent",
            9999,
            "VerdictApprove",
            "join error: task panicked",
        );

        // errors row still persisted with status="unknown".
        let detail: String = conn
            .query_row(
                "SELECT detail FROM errors WHERE source='lifecycle'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(detail.contains("\"status\":\"unknown\""));
        assert!(detail.contains("\"task\":9999"));
    }
}
