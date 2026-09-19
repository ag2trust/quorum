//! Durable, prospective-only blocker reassessment checkpoints.
//!
//! A checkpoint does not record a review verdict and never transitions task
//! lifecycle. It binds one positive review draft to the exact task, PR head,
//! review role, reviewer, and managed run, then completes that mailbox row
//! with bounded JSON guidance for the same reviewer turn.

use crate::db::{begin_immediate, map_sql_err};
use crate::error::{QuorumError, Result};
use rusqlite::{params, Connection, OptionalExtension};

pub const MAX_DRAFT_FEEDBACK_BYTES: usize = 8 * 1024;
pub const MAX_RESPONSE_JSON_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewDraftAuthority {
    pub task_id: i64,
    pub pr_number: i64,
    pub head_sha: String,
    pub review_role: String,
    pub reviewer_agent: String,
    pub agent_run_id: i64,
    pub blocking_count: u32,
    pub draft_feedback: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordOutcome {
    Created { checkpoint_id: i64 },
    Existing { checkpoint_id: i64 },
}

impl ReviewDraftAuthority {
    fn validate(&self) -> Result<()> {
        if self.task_id <= 0
            || self.pr_number <= 0
            || self.agent_run_id <= 0
            || self.blocking_count == 0
        {
            return Err(QuorumError::Usage(
                "review draft authority requires positive relationship ids and blockers".into(),
            ));
        }
        if !matches!(self.review_role.as_str(), "r1" | "r2") {
            return Err(QuorumError::Usage("invalid review draft role".into()));
        }
        if self.reviewer_agent.is_empty() || self.reviewer_agent.contains('\0') {
            return Err(QuorumError::Usage("invalid review draft reviewer".into()));
        }
        if self.head_sha.len() != 40 || !self.head_sha.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(QuorumError::Usage("invalid review draft head SHA".into()));
        }
        if self.draft_feedback.trim().is_empty()
            || self.draft_feedback.contains('\0')
            || self.draft_feedback.len() > MAX_DRAFT_FEEDBACK_BYTES
        {
            return Err(QuorumError::Usage(
                "invalid bounded review draft feedback".into(),
            ));
        }
        Ok(())
    }
}

/// Atomically record the first checkpoint for a reviewed head and complete the
/// originating mailbox row with its response. A repeated draft receives the
/// first response and cannot create a second checkpoint.
pub fn record_and_respond(
    conn: &mut Connection,
    mailbox_id: i64,
    authority: &ReviewDraftAuthority,
    response_json: &str,
    now: i64,
) -> Result<RecordOutcome> {
    authority.validate()?;
    validate_response_json(response_json)?;
    if mailbox_id <= 0 || now < 0 {
        return Err(QuorumError::Usage(
            "invalid review draft mailbox relationship".into(),
        ));
    }

    let tx = begin_immediate(conn)?;
    validate_mailbox_row(&tx, mailbox_id, authority)?;
    validate_live_authority(&tx, authority)?;

    let existing = tx
        .query_row(
            "SELECT id,response_json FROM review_blocker_reassessments
             WHERE task_id=?1 AND pr_number=?2 AND head_sha=?3 AND review_role=?4",
            params![
                authority.task_id,
                authority.pr_number,
                authority.head_sha,
                authority.review_role,
            ],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()
        .map_err(map_sql_err)?;

    let (outcome, response) = if let Some((checkpoint_id, response)) = existing {
        (RecordOutcome::Existing { checkpoint_id }, response)
    } else {
        let checkpoint_id = tx
            .query_row(
                "INSERT INTO review_blocker_reassessments(
                     task_id,pr_number,head_sha,review_role,reviewer_agent,
                     agent_run_id,draft_mailbox_id,blocking_count,draft_feedback,
                     response_json,created_at)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
                 RETURNING id",
                params![
                    authority.task_id,
                    authority.pr_number,
                    authority.head_sha,
                    authority.review_role,
                    authority.reviewer_agent,
                    authority.agent_run_id,
                    mailbox_id,
                    i64::from(authority.blocking_count),
                    authority.draft_feedback,
                    response_json,
                    now,
                ],
                |row| row.get(0),
            )
            .map_err(map_sql_err)?;
        (
            RecordOutcome::Created { checkpoint_id },
            response_json.to_string(),
        )
    };

    let updated = tx
        .execute(
            "UPDATE mailbox SET note=?1,consumed_at=?2
             WHERE id=?3 AND kind='review_draft' AND consumed_at IS NULL",
            params![response, now, mailbox_id],
        )
        .map_err(map_sql_err)?;
    if updated != 1 {
        return Err(QuorumError::Usage(
            "review draft mailbox row lost response authority".into(),
        ));
    }
    tx.commit().map_err(map_sql_err)?;
    Ok(outcome)
}

/// Whether the exact managed review run completed its mandatory positive-
/// blocker checkpoint for this PR head and role.
pub fn exists_for_final_changes(
    conn: &Connection,
    task_id: i64,
    pr_number: i64,
    head_sha: &str,
    review_role: &str,
    reviewer_agent: &str,
    agent_run_id: i64,
) -> Result<bool> {
    if task_id <= 0
        || pr_number <= 0
        || agent_run_id <= 0
        || !matches!(review_role, "r1" | "r2")
        || head_sha.len() != 40
        || !head_sha.bytes().all(|byte| byte.is_ascii_hexdigit())
        || reviewer_agent.is_empty()
        || reviewer_agent.contains('\0')
    {
        return Ok(false);
    }
    conn.query_row(
        "SELECT 1 FROM review_blocker_reassessments
         WHERE task_id=?1 AND pr_number=?2 AND head_sha=?3 AND review_role=?4
           AND reviewer_agent=?5 AND agent_run_id=?6",
        params![
            task_id,
            pr_number,
            head_sha,
            review_role,
            reviewer_agent,
            agent_run_id,
        ],
        |_| Ok(()),
    )
    .optional()
    .map(|row| row.is_some())
    .map_err(map_sql_err)
}

fn validate_response_json(response_json: &str) -> Result<()> {
    if response_json.is_empty()
        || response_json.contains('\0')
        || response_json.len() > MAX_RESPONSE_JSON_BYTES
        || !serde_json::from_str::<serde_json::Value>(response_json)
            .is_ok_and(|value| value.is_object())
    {
        return Err(QuorumError::Usage(
            "invalid bounded review draft response".into(),
        ));
    }
    Ok(())
}

fn validate_mailbox_row(
    conn: &Connection,
    mailbox_id: i64,
    authority: &ReviewDraftAuthority,
) -> Result<()> {
    let valid = conn
        .query_row(
            "SELECT 1 FROM mailbox
             WHERE id=?1 AND kind='review_draft' AND consumed_at IS NULL
               AND agent=?2 AND task_id=?3 AND pr=?4",
            params![
                mailbox_id,
                authority.reviewer_agent,
                authority.task_id,
                authority.pr_number,
            ],
            |_| Ok(()),
        )
        .optional()
        .map_err(map_sql_err)?
        .is_some();
    if !valid {
        return Err(QuorumError::Usage(
            "review draft mailbox row does not match live authority".into(),
        ));
    }
    Ok(())
}

fn validate_live_authority(conn: &Connection, authority: &ReviewDraftAuthority) -> Result<()> {
    let task_valid = conn
        .query_row(
            "SELECT 1 FROM tasks
             WHERE id=?1 AND status='in-review' AND reviewer=?2",
            params![authority.task_id, authority.reviewer_agent],
            |_| Ok(()),
        )
        .optional()
        .map_err(map_sql_err)?
        .is_some();
    let expected_sub_role = (authority.review_role == "r2").then_some("r2");
    let run_valid = conn
        .query_row(
            "SELECT 1 FROM agent_runs
             WHERE id=?1 AND task_id=?2 AND agent_name=?3 AND role='reviewer'
               AND ((?4 IS NULL AND sub_role IS NULL) OR sub_role=?4)
               AND review_pr=?5 AND review_head_sha=?6 AND ended_at IS NULL",
            params![
                authority.agent_run_id,
                authority.task_id,
                authority.reviewer_agent,
                expected_sub_role,
                authority.pr_number,
                authority.head_sha,
            ],
            |_| Ok(()),
        )
        .optional()
        .map_err(map_sql_err)?
        .is_some();
    if !task_valid || !run_valid {
        return Err(QuorumError::Usage(
            "review draft no longer has live reviewer authority".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db, mailbox};

    fn fixture() -> (tempfile::TempDir, Connection, i64) {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = db::open(&dir.path().join("review-reassessment.db")).unwrap();
        conn.execute(
            "INSERT INTO tasks(id,title,status,created_by,reviewer,created_at,updated_at)
             VALUES (7,'review','in-review','owner','R1',1,1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO agent_runs(
                 id,task_id,agent_name,role,model,effort,spawned_at,sub_role,
                 review_pr,review_head_sha)
             VALUES (9,7,'R1','reviewer','model','high',1,NULL,42,
                     '0123456789abcdef0123456789abcdef01234567')",
            [],
        )
        .unwrap();
        let mailbox_id = mailbox::append(
            &mut conn,
            &mailbox::MailboxRow {
                agent: "R1".into(),
                kind: mailbox::MailboxKind::ReviewDraft,
                task_id: Some(7),
                pr: Some(42),
                verdict: None,
                feedback: Some("two possible blockers".into()),
                note: None,
                to_agent: None,
                payload: Some("{\"blocking\":2}".into()),
            },
        )
        .unwrap();
        (dir, conn, mailbox_id)
    }

    fn authority() -> ReviewDraftAuthority {
        ReviewDraftAuthority {
            task_id: 7,
            pr_number: 42,
            head_sha: "0123456789abcdef0123456789abcdef01234567".into(),
            review_role: "r1".into(),
            reviewer_agent: "R1".into(),
            agent_run_id: 9,
            blocking_count: 2,
            draft_feedback: "two possible blockers".into(),
        }
    }

    #[test]
    fn first_checkpoint_responds_without_changing_task_lifecycle() {
        let (_dir, mut conn, mailbox_id) = fixture();
        let before: (String, Option<String>, i64) = conn
            .query_row(
                "SELECT status,reviewer,rework_round FROM tasks WHERE id=7",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        let response = r#"{"accepted":true,"guidance":"reassess"}"#;

        assert!(matches!(
            record_and_respond(&mut conn, mailbox_id, &authority(), response, 10).unwrap(),
            RecordOutcome::Created { .. }
        ));
        assert_eq!(
            conn.query_row(
                "SELECT status,reviewer,rework_round FROM tasks WHERE id=7",
                [],
                |row| Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, i64>(2)?
                ))
            )
            .unwrap(),
            before
        );
        assert_eq!(
            conn.query_row(
                "SELECT note FROM mailbox WHERE id=?1 AND consumed_at IS NOT NULL",
                [mailbox_id],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
            response
        );
        assert!(exists_for_final_changes(
            &conn,
            7,
            42,
            "0123456789abcdef0123456789abcdef01234567",
            "r1",
            "R1",
            9,
        )
        .unwrap());
        assert!(!exists_for_final_changes(
            &conn,
            7,
            42,
            "1123456789abcdef0123456789abcdef01234567",
            "r1",
            "R1",
            9,
        )
        .unwrap());
    }

    #[test]
    fn duplicate_draft_for_same_head_reuses_first_checkpoint_response() {
        let (_dir, mut conn, first_mailbox_id) = fixture();
        let first_response = r#"{"accepted":true,"guidance":"first"}"#;
        record_and_respond(
            &mut conn,
            first_mailbox_id,
            &authority(),
            first_response,
            10,
        )
        .unwrap();
        let second_mailbox_id = mailbox::append(
            &mut conn,
            &mailbox::MailboxRow {
                agent: "R1".into(),
                kind: mailbox::MailboxKind::ReviewDraft,
                task_id: Some(7),
                pr: Some(42),
                verdict: None,
                feedback: Some("duplicate".into()),
                note: None,
                to_agent: None,
                payload: Some("{\"blocking\":1}".into()),
            },
        )
        .unwrap();
        let mut duplicate = authority();
        duplicate.blocking_count = 1;
        duplicate.draft_feedback = "duplicate".into();

        assert!(matches!(
            record_and_respond(
                &mut conn,
                second_mailbox_id,
                &duplicate,
                r#"{"accepted":true,"guidance":"second"}"#,
                11,
            )
            .unwrap(),
            RecordOutcome::Existing { .. }
        ));
        assert_eq!(
            conn.query_row(
                "SELECT note FROM mailbox WHERE id=?1",
                [second_mailbox_id],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
            first_response
        );
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM review_blocker_reassessments",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
    }

    #[test]
    fn stale_authority_leaves_mailbox_pending_and_creates_no_checkpoint() {
        let (_dir, mut conn, mailbox_id) = fixture();
        conn.execute("UPDATE tasks SET status='rework' WHERE id=7", [])
            .unwrap();

        assert!(record_and_respond(
            &mut conn,
            mailbox_id,
            &authority(),
            r#"{"accepted":true,"guidance":"reassess"}"#,
            10,
        )
        .is_err());
        assert_eq!(
            conn.query_row(
                "SELECT consumed_at IS NULL FROM mailbox WHERE id=?1",
                [mailbox_id],
                |row| row.get::<_, bool>(0)
            )
            .unwrap(),
            true
        );
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM review_blocker_reassessments",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
    }
}
