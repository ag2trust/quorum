//! Bounded, read-only inspection of durable review follow-up snapshots.
//!
//! Every stored value is reconstructed through the closed follow-up domain types.
//! JSON arrays and text are bounded before they are copied or parsed, and aggregate
//! reads reject partial or internally inconsistent batches. This module deliberately
//! exposes no update, disposition, reassessment, or lifecycle API.

use crate::db::map_sql_err;
use crate::error::{QuorumError, Result};
use crate::review_followups::{
    ReviewFollowupArtifact, ReviewFollowupBatch, MAX_FOLLOWUP_ARTIFACTS,
    MAX_FOLLOWUP_ARTIFACT_JSON_BYTES, MAX_FOLLOWUP_TEXT_BYTES,
};
use rusqlite::types::ValueRef;
use rusqlite::{params, Connection, Row};
use std::collections::HashSet;

/// Maximum number of complete follow-up batches returned by one list call.
pub const MAX_FOLLOWUP_BATCHES_PER_READ: usize = 128;

const CLOSED_VALUE_MAX_BYTES: usize = 64;

/// One complete, inspection-ready stored batch and its validated artifacts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReviewFollowupBatchInspection {
    batch: ReviewFollowupBatch,
    artifacts: Vec<ReviewFollowupArtifact>,
}

impl ReviewFollowupBatchInspection {
    fn new(batch: ReviewFollowupBatch, artifacts: Vec<ReviewFollowupArtifact>) -> Result<Self> {
        if artifacts.len() != batch.artifact_count() {
            return Err(QuorumError::Usage(
                "stored follow-up batch artifact count is inconsistent".into(),
            ));
        }

        let mut ordinals = HashSet::with_capacity(artifacts.len());
        for artifact in &artifacts {
            if artifact.pr_number() != batch.pr_number() {
                return Err(QuorumError::Usage(
                    "stored follow-up artifact belongs to another batch".into(),
                ));
            }
            if !ordinals.insert(artifact.ordinal()) {
                return Err(QuorumError::Usage(
                    "stored follow-up batch has duplicate artifact ordinals".into(),
                ));
            }
        }

        Ok(Self { batch, artifacts })
    }

    pub fn batch(&self) -> &ReviewFollowupBatch {
        &self.batch
    }

    pub fn artifacts(&self) -> &[ReviewFollowupArtifact] {
        &self.artifacts
    }
}

/// Read one complete batch by PR number.
///
/// Absence is returned as `None`. A present batch is returned only after its
/// complete artifact set has been bounded and reconstructed through closed types.
pub fn get_batch(
    conn: &Connection,
    pr_number: i64,
) -> Result<Option<ReviewFollowupBatchInspection>> {
    if pr_number <= 0 {
        return Err(QuorumError::Usage(
            "follow-up batch PR number must be positive".into(),
        ));
    }

    with_read_snapshot(conn, |snapshot| {
        let batch = read_batch(snapshot, pr_number)?;
        batch
            .map(|batch| inspection_for_batch(snapshot, batch))
            .transpose()
    })
}

/// List complete batches in deterministic newest-first order.
///
/// `limit` must be between one and [`MAX_FOLLOWUP_BATCHES_PER_READ`]. Each
/// returned batch independently enforces the artifact cap, so this API cannot
/// turn corrupt stored counts into an unbounded read.
pub fn list_batches(conn: &Connection, limit: usize) -> Result<Vec<ReviewFollowupBatchInspection>> {
    if !(1..=MAX_FOLLOWUP_BATCHES_PER_READ).contains(&limit) {
        return Err(QuorumError::Usage(format!(
            "follow-up batch read limit must be between 1 and {MAX_FOLLOWUP_BATCHES_PER_READ}"
        )));
    }
    let sql_limit = i64::try_from(limit)
        .map_err(|_| QuorumError::Usage("follow-up batch read limit is invalid".into()))?;
    with_read_snapshot(conn, |snapshot| {
        let mut stmt = snapshot.prepare(
            "SELECT pr_number,task_id,graph_id,source_task_id,collector_version,
                    artifact_count,state,created_at,updated_at
             FROM review_followup_batches
             ORDER BY created_at DESC,pr_number DESC LIMIT ?1",
        )?;
        let mut rows = stmt.query([sql_limit])?;
        let mut batches = Vec::with_capacity(limit);
        while let Some(row) = rows.next()? {
            batches.push(batch_from_row(row)?);
        }
        drop(rows);
        drop(stmt);

        batches
            .into_iter()
            .map(|batch| inspection_for_batch(snapshot, batch))
            .collect()
    })
}

/// Read one artifact by its durable ID, reconstructed through closed domain types.
pub fn get_artifact(conn: &Connection, artifact_id: i64) -> Result<Option<ReviewFollowupArtifact>> {
    if artifact_id <= 0 {
        return Err(QuorumError::Usage(
            "follow-up artifact id must be positive".into(),
        ));
    }
    let mut stmt = conn.prepare(
        "SELECT id,pr_number,ordinal,technical_impact,scope_relationship,concern,
                non_blocking_reason,affected_behavior,desired_outcome,
                verification_expectations,evidence_ids,disposition,
                disposition_reason,linked_task_id,created_task_id,created_at,updated_at
         FROM review_followup_artifacts WHERE id=?1",
    )?;
    let mut rows = stmt.query([artifact_id])?;
    let artifact = rows.next()?.map(artifact_from_row).transpose()?;
    Ok(artifact)
}

fn read_batch(conn: &Connection, pr_number: i64) -> Result<Option<ReviewFollowupBatch>> {
    let mut stmt = conn.prepare(
        "SELECT pr_number,task_id,graph_id,source_task_id,collector_version,
                artifact_count,state,created_at,updated_at
         FROM review_followup_batches WHERE pr_number=?1",
    )?;
    let mut rows = stmt.query([pr_number])?;
    rows.next()?.map(batch_from_row).transpose()
}

/// Run one bounded aggregate read against one WAL snapshot.
///
/// `unchecked_transaction` preserves the public `&Connection` read surface while
/// still issuing a short `BEGIN DEFERRED`. The first SELECT establishes the
/// snapshot; commit ends it before the inspection value is returned. If the
/// caller already owns a transaction, its existing snapshot is reused.
fn with_read_snapshot<T>(
    conn: &Connection,
    read: impl FnOnce(&Connection) -> Result<T>,
) -> Result<T> {
    if !conn.is_autocommit() {
        return read(conn);
    }
    let transaction = conn.unchecked_transaction().map_err(map_sql_err)?;
    let value = read(&transaction)?;
    transaction.commit().map_err(map_sql_err)?;
    Ok(value)
}

fn inspection_for_batch(
    conn: &Connection,
    batch: ReviewFollowupBatch,
) -> Result<ReviewFollowupBatchInspection> {
    let artifacts = read_artifacts_for_pr(conn, batch.pr_number())?;
    ReviewFollowupBatchInspection::new(batch, artifacts)
}

fn read_artifacts_for_pr(conn: &Connection, pr_number: i64) -> Result<Vec<ReviewFollowupArtifact>> {
    // Read one extra row so corrupt storage fails loudly rather than becoming a
    // plausible-looking partial inspection result.
    let row_limit = i64::try_from(MAX_FOLLOWUP_ARTIFACTS + 1)
        .map_err(|_| QuorumError::Usage("follow-up artifact read limit is invalid".into()))?;
    let mut stmt = conn.prepare(
        "SELECT id,pr_number,ordinal,technical_impact,scope_relationship,concern,
                non_blocking_reason,affected_behavior,desired_outcome,
                verification_expectations,evidence_ids,disposition,
                disposition_reason,linked_task_id,created_task_id,created_at,updated_at
         FROM review_followup_artifacts
         WHERE pr_number=?1 ORDER BY ordinal,id LIMIT ?2",
    )?;
    let mut rows = stmt.query(params![pr_number, row_limit])?;
    let mut artifacts = Vec::with_capacity(MAX_FOLLOWUP_ARTIFACTS);
    while let Some(row) = rows.next()? {
        if artifacts.len() == MAX_FOLLOWUP_ARTIFACTS {
            return Err(QuorumError::Usage(
                "stored follow-up batch exceeds the artifact read bound".into(),
            ));
        }
        artifacts.push(artifact_from_row(row)?);
    }
    Ok(artifacts)
}

fn batch_from_row(row: &Row<'_>) -> Result<ReviewFollowupBatch> {
    ReviewFollowupBatch::from_stored(
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        required_text(row, 4, "collector version", MAX_FOLLOWUP_TEXT_BYTES)?,
        row.get(5)?,
        &required_text(row, 6, "batch state", CLOSED_VALUE_MAX_BYTES)?,
        row.get(7)?,
        row.get(8)?,
    )
}

fn artifact_from_row(row: &Row<'_>) -> Result<ReviewFollowupArtifact> {
    let technical_impact = required_text(row, 3, "technical impact", CLOSED_VALUE_MAX_BYTES)?;
    let scope_relationship = required_text(row, 4, "scope relationship", CLOSED_VALUE_MAX_BYTES)?;
    let concern = required_text(row, 5, "concern", MAX_FOLLOWUP_TEXT_BYTES)?;
    let non_blocking_reason =
        required_text(row, 6, "non-blocking reason", MAX_FOLLOWUP_TEXT_BYTES)?;
    let affected_behavior = required_text(row, 7, "affected behavior", MAX_FOLLOWUP_TEXT_BYTES)?;
    let desired_outcome = required_text(row, 8, "desired outcome", MAX_FOLLOWUP_TEXT_BYTES)?;
    let verification = required_text(
        row,
        9,
        "verification expectations JSON",
        MAX_FOLLOWUP_ARTIFACT_JSON_BYTES,
    )?;
    let evidence = required_text(
        row,
        10,
        "evidence IDs JSON",
        MAX_FOLLOWUP_ARTIFACT_JSON_BYTES,
    )?;
    let disposition = optional_text(row, 11, "disposition", CLOSED_VALUE_MAX_BYTES)?;
    let disposition_reason = optional_text(row, 12, "disposition reason", MAX_FOLLOWUP_TEXT_BYTES)?;

    ReviewFollowupArtifact::from_stored(
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        &technical_impact,
        &scope_relationship,
        concern,
        non_blocking_reason,
        affected_behavior,
        desired_outcome,
        &verification,
        &evidence,
        disposition.as_deref(),
        disposition_reason,
        row.get(13)?,
        row.get(14)?,
        row.get(15)?,
        row.get(16)?,
    )
}

fn required_text(row: &Row<'_>, index: usize, field: &str, max: usize) -> Result<String> {
    match row.get_ref(index)? {
        ValueRef::Text(bytes) => bounded_utf8(bytes, field, max).map(str::to_owned),
        _ => Err(QuorumError::Usage(format!(
            "stored follow-up {field} is not text"
        ))),
    }
}

fn optional_text(row: &Row<'_>, index: usize, field: &str, max: usize) -> Result<Option<String>> {
    match row.get_ref(index)? {
        ValueRef::Null => Ok(None),
        ValueRef::Text(bytes) => bounded_utf8(bytes, field, max).map(str::to_owned).map(Some),
        _ => Err(QuorumError::Usage(format!(
            "stored follow-up {field} is neither text nor null"
        ))),
    }
}

fn bounded_utf8<'a>(bytes: &'a [u8], field: &str, max: usize) -> Result<&'a str> {
    if bytes.len() > max || bytes.contains(&0) {
        return Err(QuorumError::Usage(format!(
            "stored follow-up {field} exceeds its read bound"
        )));
    }
    std::str::from_utf8(bytes)
        .map_err(|_| QuorumError::Usage(format!("stored follow-up {field} is not valid UTF-8")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::review_followup_writes::{
        insert_batch_if_absent, NewReviewFollowupArtifact, NewReviewFollowupBatch,
    };
    use crate::review_followups::{
        FollowupBatchState, FollowupDisposition, ScopeRelationship, TechnicalImpact,
        MAX_FOLLOWUP_EVIDENCE_IDS, MAX_VERIFICATION_EXPECTATIONS,
    };
    use rusqlite::Connection;
    use tempfile::TempDir;

    fn database() -> (TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let conn = crate::db::open(&dir.path().join("followup-reads.db")).unwrap();
        conn.pragma_update(None, "foreign_keys", true).unwrap();
        conn.execute_batch(
            "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at) VALUES
                 (1,'source','done','owner',1,1),
                 (2,'linked','open','owner',1,1);",
        )
        .unwrap();
        (dir, conn)
    }

    fn artifact(pr_number: i64, ordinal: i64, created_at: i64) -> NewReviewFollowupArtifact {
        NewReviewFollowupArtifact::new(
            pr_number,
            ordinal,
            TechnicalImpact::Major,
            ScopeRelationship::OutOfScope,
            format!("concern {ordinal}"),
            "safe outside this change".into(),
            "review collection".into(),
            "reject invalid input".into(),
            r#"["focused test passes"]"#,
            r#"[{"kind":"review_comment","id":42}]"#,
            created_at,
        )
        .unwrap()
    }

    fn insert_batch(conn: &mut Connection, pr_number: i64, artifact_count: usize, at: i64) {
        let artifacts = (0..artifact_count)
            .map(|ordinal| artifact(pr_number, ordinal as i64, at))
            .collect();
        let batch = NewReviewFollowupBatch::new(
            pr_number,
            1,
            None,
            1,
            "followups-v1".into(),
            artifacts,
            at,
        )
        .unwrap();
        insert_batch_if_absent(conn, &batch).unwrap();
    }

    fn total_changes(conn: &Connection) -> i64 {
        conn.query_row("SELECT total_changes()", [], |row| row.get(0))
            .unwrap()
    }

    #[test]
    fn reads_complete_closed_batches_and_artifacts_from_wal_sqlite() {
        let (_dir, mut conn) = database();
        insert_batch(&mut conn, 101, 2, 100);
        insert_batch(&mut conn, 102, 0, 200);
        let artifact_id: i64 = conn
            .query_row(
                "SELECT id FROM review_followup_artifacts WHERE pr_number=101 AND ordinal=0",
                [],
                |row| row.get(0),
            )
            .unwrap();
        conn.execute(
            "UPDATE review_followup_artifacts
             SET disposition='linked',disposition_reason='already tracked',
                 linked_task_id=2,updated_at=150
             WHERE id=?1",
            [artifact_id],
        )
        .unwrap();

        let inspection = get_batch(&conn, 101).unwrap().unwrap();
        assert_eq!(inspection.batch().pr_number(), 101);
        assert_eq!(inspection.batch().state(), FollowupBatchState::Collected);
        assert_eq!(inspection.artifacts().len(), 2);
        assert_eq!(
            inspection.artifacts()[0].technical_impact(),
            TechnicalImpact::Major
        );
        assert_eq!(
            inspection.artifacts()[0].scope_relationship(),
            ScopeRelationship::OutOfScope
        );
        assert_eq!(
            inspection.artifacts()[0]
                .verification_expectations()
                .as_slice(),
            &["focused test passes".to_string()]
        );
        assert_eq!(
            inspection.artifacts()[0].evidence_ids().as_slice()[0].id(),
            42
        );
        let disposition = inspection.artifacts()[0].disposition().unwrap();
        assert_eq!(disposition.kind(), FollowupDisposition::Linked);
        assert_eq!(disposition.reason(), "already tracked");
        assert_eq!(disposition.linked_task_id(), Some(2));
        assert_eq!(
            get_artifact(&conn, artifact_id).unwrap().unwrap().id(),
            Some(artifact_id)
        );

        let listed = list_batches(&conn, 1).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].batch().pr_number(), 102);
        assert!(listed[0].artifacts().is_empty());
        assert!(get_batch(&conn, 999).unwrap().is_none());
        assert_eq!(
            conn.query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
                .unwrap()
                .to_ascii_lowercase(),
            "wal"
        );
    }

    #[test]
    fn reads_do_not_change_followups_or_existing_review_analytics() {
        let (_dir, mut conn) = database();
        insert_batch(&mut conn, 103, 1, 100);
        conn.execute_batch(
            "INSERT INTO review_findings(
                 pr_number,task_id,reviewer,kind,text,source_endpoint,created_at)
             VALUES (103,1,'reviewer','suggestion','legacy analytics','pulls',90);
             INSERT INTO review_collection_runs(
                 pr_number,task_id,status,collector_model,collector_version,
                 findings_count,followup_count,attempted_at,completed_at)
             VALUES (103,1,'success','model','analytics-v1',1,0,90,91);",
        )
        .unwrap();
        let before_changes = total_changes(&conn);
        let before: (String, String, i64) = conn
            .query_row(
                "SELECT f.text,r.collector_version,r.followup_count
                 FROM review_findings f JOIN review_collection_runs r USING(pr_number)
                 WHERE f.pr_number=103",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();

        let inspected = get_batch(&conn, 103).unwrap().unwrap();
        assert_eq!(inspected.artifacts().len(), 1);
        assert_eq!(list_batches(&conn, 10).unwrap().len(), 1);
        assert_eq!(total_changes(&conn), before_changes);
        assert_eq!(
            conn.query_row(
                "SELECT f.text,r.collector_version,r.followup_count
                 FROM review_findings f JOIN review_collection_runs r USING(pr_number)
                 WHERE f.pr_number=103",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap(),
            before
        );
        assert!(
            conn.is_autocommit(),
            "aggregate read left a transaction open"
        );
    }

    #[test]
    fn aggregate_read_uses_one_snapshot_across_concurrent_atomic_application() {
        let (dir, mut writer) = database();
        insert_batch(&mut writer, 108, 1, 100);
        let reader = crate::db::open(&dir.path().join("followup-reads.db")).unwrap();

        // Establish the reader's snapshot with the batch row, atomically advance
        // both durable rows on another WAL connection, then finish the aggregate
        // read. The inspection must contain the complete pre-application state,
        // never the impossible collected+disposed combination.
        let inspection = with_read_snapshot(&reader, |snapshot| {
            let batch = read_batch(snapshot, 108)?.unwrap();
            let transaction = crate::db::begin_immediate(&mut writer)?;
            transaction.execute(
                "UPDATE review_followup_artifacts
                 SET disposition='linked',disposition_reason='already tracked',
                     linked_task_id=2,updated_at=200
                 WHERE pr_number=108",
                [],
            )?;
            transaction.execute(
                "UPDATE review_followup_batches
                 SET state='resolved',updated_at=200 WHERE pr_number=108",
                [],
            )?;
            transaction.commit()?;
            inspection_for_batch(snapshot, batch)
        })
        .unwrap();

        assert_eq!(inspection.batch().state(), FollowupBatchState::Collected);
        assert!(inspection.artifacts()[0].disposition().is_none());
        assert!(reader.is_autocommit());

        let after = get_batch(&reader, 108).unwrap().unwrap();
        assert_eq!(after.batch().state(), FollowupBatchState::Resolved);
        assert_eq!(
            after.artifacts()[0].disposition().unwrap().kind(),
            FollowupDisposition::Linked
        );
    }

    #[test]
    fn malformed_and_oversized_json_arrays_fail_at_read_boundary() {
        let cases = [
            ("verification_expectations", "not-json".to_string()),
            ("evidence_ids", "not-json".to_string()),
            (
                "verification_expectations",
                serde_json::to_string(&vec!["verify"; MAX_VERIFICATION_EXPECTATIONS + 1]).unwrap(),
            ),
            (
                "evidence_ids",
                serde_json::to_string(
                    &(1..=(MAX_FOLLOWUP_EVIDENCE_IDS + 1))
                        .map(|id| serde_json::json!({"kind":"review","id":id}))
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
            ),
            (
                "verification_expectations",
                "x".repeat(MAX_FOLLOWUP_ARTIFACT_JSON_BYTES + 1),
            ),
        ];

        for (column, invalid) in cases {
            let (_dir, mut conn) = database();
            insert_batch(&mut conn, 104, 1, 100);
            conn.execute(
                &format!("UPDATE review_followup_artifacts SET {column}=?1 WHERE pr_number=104"),
                [invalid],
            )
            .unwrap();
            let before_changes = total_changes(&conn);

            assert!(get_batch(&conn, 104).is_err(), "accepted invalid {column}");
            assert_eq!(total_changes(&conn), before_changes);
        }
    }

    #[test]
    fn oversized_text_and_inconsistent_or_oversized_batches_fail_at_read_boundary() {
        let (_dir, mut conn) = database();
        insert_batch(&mut conn, 105, 1, 100);
        conn.execute(
            "UPDATE review_followup_artifacts SET concern=?1 WHERE pr_number=105",
            ["x".repeat(MAX_FOLLOWUP_TEXT_BYTES + 1)],
        )
        .unwrap();
        assert!(get_batch(&conn, 105).is_err());

        let (_dir, mut conn) = database();
        insert_batch(&mut conn, 106, 1, 100);
        conn.execute(
            "UPDATE review_followup_batches SET artifact_count=2 WHERE pr_number=106",
            [],
        )
        .unwrap();
        assert!(get_batch(&conn, 106).is_err());

        let (_dir, mut conn) = database();
        insert_batch(&mut conn, 107, MAX_FOLLOWUP_ARTIFACTS, 100);
        conn.execute(
            "INSERT INTO review_followup_artifacts(
                 pr_number,ordinal,technical_impact,scope_relationship,concern,
                 non_blocking_reason,affected_behavior,desired_outcome,
                 verification_expectations,evidence_ids,created_at,updated_at)
             VALUES (107,32,'major','out_of_scope','extra','reason','behavior','outcome',
                     '[\"verify\"]','[{\"kind\":\"review\",\"id\":1}]',100,100)",
            [],
        )
        .unwrap();
        assert!(get_batch(&conn, 107).is_err());
    }

    #[test]
    fn caller_limits_and_invalid_lookup_ids_are_rejected() {
        let (_dir, conn) = database();
        assert!(list_batches(&conn, 0).is_err());
        assert!(list_batches(&conn, MAX_FOLLOWUP_BATCHES_PER_READ + 1).is_err());
        assert!(get_batch(&conn, 0).is_err());
        assert!(get_artifact(&conn, 0).is_err());
    }
}
