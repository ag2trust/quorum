//! Atomic, absent-only persistence for immutable review follow-up snapshots.
//!
//! This is deliberately an insertion-only boundary. Callers first construct the
//! closed [`NewReviewFollowupArtifact`] and [`NewReviewFollowupBatch`] values;
//! construction parses and bounds every JSON array and validates all cross-row
//! relationships before a write transaction begins. There is no artifact update,
//! delete, or reassessment API in this module.

use crate::db::{begin_immediate, map_sql_err};
use crate::error::{QuorumError, Result};
use crate::review_followups::{
    FollowupBatchState, FollowupEvidenceIds, ReviewFollowupArtifact, ReviewFollowupBatch,
    ScopeRelationship, TechnicalImpact, VerificationExpectations,
};
use rusqlite::{params, Connection};
use std::collections::HashSet;

/// A validated, undisposed artifact that may participate in one new immutable batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewReviewFollowupArtifact {
    artifact: ReviewFollowupArtifact,
    verification_expectations_json: String,
    evidence_ids_json: String,
}

impl NewReviewFollowupArtifact {
    /// Parse raw JSON arrays and construct a closed artifact value at the write boundary.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pr_number: i64,
        ordinal: i64,
        technical_impact: TechnicalImpact,
        scope_relationship: ScopeRelationship,
        concern: String,
        non_blocking_reason: String,
        affected_behavior: String,
        desired_outcome: String,
        verification_expectations_json: &str,
        evidence_ids_json: &str,
        created_at: i64,
    ) -> Result<Self> {
        let artifact = ReviewFollowupArtifact::new(
            None,
            pr_number,
            ordinal,
            technical_impact,
            scope_relationship,
            concern,
            non_blocking_reason,
            affected_behavior,
            desired_outcome,
            VerificationExpectations::from_json(verification_expectations_json)?,
            FollowupEvidenceIds::from_json(evidence_ids_json)?,
            None,
            created_at,
            created_at,
        )?;
        Self::try_from_artifact(artifact)
    }

    /// Convert a domain artifact into an insertion value, rejecting stored or disposed rows.
    ///
    /// Re-encoding and parsing the arrays here is intentional: every core write boundary
    /// independently enforces the JSON shape and byte/count limits even when its caller
    /// already holds a validated domain value.
    pub fn try_from_artifact(artifact: ReviewFollowupArtifact) -> Result<Self> {
        if artifact.id().is_some() {
            return Err(QuorumError::Usage(
                "new follow-up artifact must not have a stored id".into(),
            ));
        }
        if artifact.disposition().is_some() {
            return Err(QuorumError::Usage(
                "new follow-up artifact must be undisposed".into(),
            ));
        }
        if artifact.updated_at() != artifact.created_at() {
            return Err(QuorumError::Usage(
                "new follow-up artifact timestamps must match".into(),
            ));
        }

        let verification_expectations_json = artifact.verification_expectations().to_json()?;
        let evidence_ids_json = artifact.evidence_ids().to_json()?;
        VerificationExpectations::from_json(&verification_expectations_json)?;
        FollowupEvidenceIds::from_json(&evidence_ids_json)?;

        Ok(Self {
            artifact,
            verification_expectations_json,
            evidence_ids_json,
        })
    }

    pub fn artifact(&self) -> &ReviewFollowupArtifact {
        &self.artifact
    }
}

/// One complete, validated batch snapshot ready for absent-only insertion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewReviewFollowupBatch {
    batch: ReviewFollowupBatch,
    artifacts: Vec<NewReviewFollowupArtifact>,
}

impl NewReviewFollowupBatch {
    /// Construct one collected snapshot. Artifact count is derived, never caller-supplied.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pr_number: i64,
        task_id: i64,
        graph_id: Option<i64>,
        source_task_id: i64,
        collector_version: String,
        artifacts: Vec<NewReviewFollowupArtifact>,
        created_at: i64,
    ) -> Result<Self> {
        let batch = ReviewFollowupBatch::new(
            pr_number,
            task_id,
            graph_id,
            source_task_id,
            collector_version,
            artifacts.len(),
            FollowupBatchState::Collected,
            created_at,
            created_at,
        )?;

        let mut ordinals = HashSet::with_capacity(artifacts.len());
        for value in &artifacts {
            let artifact = value.artifact();
            if artifact.pr_number() != pr_number {
                return Err(QuorumError::Usage(
                    "follow-up artifact belongs to another batch".into(),
                ));
            }
            if !ordinals.insert(artifact.ordinal()) {
                return Err(QuorumError::Usage(
                    "follow-up batch contains a duplicate artifact ordinal".into(),
                ));
            }
        }

        Ok(Self { batch, artifacts })
    }

    pub fn batch(&self) -> &ReviewFollowupBatch {
        &self.batch
    }

    pub fn artifacts(&self) -> &[NewReviewFollowupArtifact] {
        &self.artifacts
    }
}

/// Result of an absent-only immutable batch insertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertReviewFollowupBatchOutcome {
    Inserted,
    AlreadyExists,
}

/// Atomically insert a complete immutable batch when its PR has no batch yet.
///
/// A duplicate PR is a clean negative result. The transaction does not insert,
/// replace, or update any artifact in that case, so the first snapshot remains
/// byte-for-byte authoritative even under racing writers.
pub fn insert_batch_if_absent(
    conn: &mut Connection,
    value: &NewReviewFollowupBatch,
) -> Result<InsertReviewFollowupBatchOutcome> {
    let batch = value.batch();
    let artifact_count = i64::try_from(batch.artifact_count())
        .map_err(|_| QuorumError::Usage("follow-up artifact count is not representable".into()))?;
    let tx = begin_immediate(conn)?;
    let inserted = tx
        .execute(
            "INSERT INTO review_followup_batches(
                 pr_number,task_id,graph_id,source_task_id,collector_version,
                 artifact_count,state,created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)
             ON CONFLICT(pr_number) DO NOTHING",
            params![
                batch.pr_number(),
                batch.task_id(),
                batch.graph_id(),
                batch.source_task_id(),
                batch.collector_version(),
                artifact_count,
                batch.state().as_str(),
                batch.created_at(),
                batch.updated_at(),
            ],
        )
        .map_err(map_sql_err)?;

    if inserted == 0 {
        tx.commit().map_err(map_sql_err)?;
        return Ok(InsertReviewFollowupBatchOutcome::AlreadyExists);
    }

    for value in value.artifacts() {
        let artifact = value.artifact();
        tx.execute(
            "INSERT INTO review_followup_artifacts(
                 pr_number,ordinal,technical_impact,scope_relationship,concern,
                 non_blocking_reason,affected_behavior,desired_outcome,
                 verification_expectations,evidence_ids,disposition,
                 disposition_reason,linked_task_id,created_task_id,created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,NULL,NULL,NULL,NULL,?11,?12)",
            params![
                artifact.pr_number(),
                artifact.ordinal(),
                artifact.technical_impact().as_str(),
                artifact.scope_relationship().as_str(),
                artifact.concern(),
                artifact.non_blocking_reason(),
                artifact.affected_behavior(),
                artifact.desired_outcome(),
                value.verification_expectations_json,
                value.evidence_ids_json,
                artifact.created_at(),
                artifact.updated_at(),
            ],
        )
        .map_err(map_sql_err)?;
    }

    tx.commit().map_err(map_sql_err)?;
    Ok(InsertReviewFollowupBatchOutcome::Inserted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::review_followups::{
        ArtifactDisposition, FollowupDisposition, MAX_FOLLOWUP_ARTIFACTS,
        MAX_FOLLOWUP_ARTIFACT_JSON_BYTES, MAX_FOLLOWUP_EVIDENCE_IDS, MAX_FOLLOWUP_TEXT_BYTES,
        MAX_VERIFICATION_EXPECTATIONS,
    };
    use tempfile::TempDir;

    fn database() -> (TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let conn = crate::db::open(&dir.path().join("followup-writes.db")).unwrap();
        conn.pragma_update(None, "foreign_keys", true).unwrap();
        conn.execute_batch(
            "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at) VALUES
                 (1,'source','done','owner',1,1),
                 (2,'other','done','owner',1,1),
                 (3,'created','open','owner',1,1);",
        )
        .unwrap();
        (dir, conn)
    }

    fn artifact(pr_number: i64, ordinal: i64, concern: &str) -> NewReviewFollowupArtifact {
        NewReviewFollowupArtifact::new(
            pr_number,
            ordinal,
            TechnicalImpact::Major,
            ScopeRelationship::OutOfScope,
            concern.into(),
            "safe outside this change".into(),
            "review collection".into(),
            "reject invalid input".into(),
            r#"["focused test passes"]"#,
            r#"[{"kind":"review_comment","id":42}]"#,
            100,
        )
        .unwrap()
    }

    fn batch(
        pr_number: i64,
        task_id: i64,
        collector_version: &str,
        artifacts: Vec<NewReviewFollowupArtifact>,
    ) -> NewReviewFollowupBatch {
        NewReviewFollowupBatch::new(
            pr_number,
            task_id,
            None,
            task_id,
            collector_version.into(),
            artifacts,
            100,
        )
        .unwrap()
    }

    fn row_count(conn: &Connection, table: &str) -> i64 {
        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
    }

    #[test]
    fn inserts_bounded_closed_batch_and_artifacts_against_wal_sqlite() {
        let (_dir, mut conn) = database();
        let value = batch(
            101,
            1,
            "followups-v1",
            vec![artifact(101, 0, "first"), artifact(101, 1, "second")],
        );

        assert_eq!(
            insert_batch_if_absent(&mut conn, &value).unwrap(),
            InsertReviewFollowupBatchOutcome::Inserted
        );
        assert_eq!(row_count(&conn, "review_followup_batches"), 1);
        assert_eq!(row_count(&conn, "review_followup_artifacts"), 2);
        assert_eq!(
            conn.query_row(
                "SELECT task_id,source_task_id,collector_version,artifact_count,state,
                        created_at,updated_at
                 FROM review_followup_batches WHERE pr_number=101",
                [],
                |row| Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                )),
            )
            .unwrap(),
            (1, 1, "followups-v1".into(), 2, "collected".into(), 100, 100)
        );
        assert_eq!(
            conn.query_row("PRAGMA journal_mode", [], |row| row.get::<_, String>(0))
                .unwrap()
                .to_ascii_lowercase(),
            "wal"
        );
    }

    #[test]
    fn zero_and_maximum_artifact_batches_are_accepted_but_oversized_batch_is_rejected() {
        let (_dir, mut conn) = database();
        let empty = batch(107, 1, "followups-v1", vec![]);
        let maximum_artifacts = (0..MAX_FOLLOWUP_ARTIFACTS)
            .map(|ordinal| artifact(108, ordinal as i64, &format!("concern {ordinal}")))
            .collect::<Vec<_>>();
        let maximum = batch(108, 1, "followups-v1", maximum_artifacts);

        assert_eq!(
            insert_batch_if_absent(&mut conn, &empty).unwrap(),
            InsertReviewFollowupBatchOutcome::Inserted
        );
        assert_eq!(
            insert_batch_if_absent(&mut conn, &maximum).unwrap(),
            InsertReviewFollowupBatchOutcome::Inserted
        );

        let oversized_artifacts = (0..=MAX_FOLLOWUP_ARTIFACTS)
            .map(|ordinal| artifact(109, ordinal as i64, &format!("concern {ordinal}")))
            .collect::<Vec<_>>();
        assert!(NewReviewFollowupBatch::new(
            109,
            1,
            None,
            1,
            "followups-v1".into(),
            oversized_artifacts,
            100,
        )
        .is_err());
        assert_eq!(row_count(&conn, "review_followup_batches"), 2);
        assert_eq!(row_count(&conn, "review_followup_artifacts"), 32);
    }

    #[test]
    fn second_batch_insert_cannot_replace_or_mutate_first_snapshot() {
        let (_dir, mut conn) = database();
        let first = batch(
            102,
            1,
            "followups-v1",
            vec![artifact(102, 0, "original concern")],
        );
        let replacement = batch(
            102,
            2,
            "followups-v2",
            vec![
                artifact(102, 0, "replacement concern"),
                artifact(102, 1, "new concern"),
            ],
        );

        assert_eq!(
            insert_batch_if_absent(&mut conn, &first).unwrap(),
            InsertReviewFollowupBatchOutcome::Inserted
        );
        let before: String = conn
            .query_row(
                "SELECT json_object(
                    'batch',json_object(
                        'task_id',b.task_id,'source_task_id',b.source_task_id,
                        'collector_version',b.collector_version,
                        'artifact_count',b.artifact_count,'state',b.state,
                        'created_at',b.created_at,'updated_at',b.updated_at),
                    'artifacts',(
                        SELECT json_group_array(json(a.row_json))
                        FROM (
                            SELECT json_object(
                                'id',id,'ordinal',ordinal,'technical_impact',technical_impact,
                                'scope_relationship',scope_relationship,'concern',concern,
                                'non_blocking_reason',non_blocking_reason,
                                'affected_behavior',affected_behavior,
                                'desired_outcome',desired_outcome,
                                'verification_expectations',verification_expectations,
                                'evidence_ids',evidence_ids,'disposition',disposition,
                                'disposition_reason',disposition_reason,
                                'linked_task_id',linked_task_id,'created_task_id',created_task_id,
                                'created_at',created_at,'updated_at',updated_at) AS row_json
                            FROM review_followup_artifacts
                            WHERE pr_number=b.pr_number ORDER BY ordinal
                        ) AS a
                    )
                 )
                 FROM review_followup_batches AS b WHERE pr_number=102",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(
            insert_batch_if_absent(&mut conn, &replacement).unwrap(),
            InsertReviewFollowupBatchOutcome::AlreadyExists
        );
        let after: String = conn
            .query_row(
                "SELECT json_object(
                    'batch',json_object(
                        'task_id',b.task_id,'source_task_id',b.source_task_id,
                        'collector_version',b.collector_version,
                        'artifact_count',b.artifact_count,'state',b.state,
                        'created_at',b.created_at,'updated_at',b.updated_at),
                    'artifacts',(
                        SELECT json_group_array(json(a.row_json))
                        FROM (
                            SELECT json_object(
                                'id',id,'ordinal',ordinal,'technical_impact',technical_impact,
                                'scope_relationship',scope_relationship,'concern',concern,
                                'non_blocking_reason',non_blocking_reason,
                                'affected_behavior',affected_behavior,
                                'desired_outcome',desired_outcome,
                                'verification_expectations',verification_expectations,
                                'evidence_ids',evidence_ids,'disposition',disposition,
                                'disposition_reason',disposition_reason,
                                'linked_task_id',linked_task_id,'created_task_id',created_task_id,
                                'created_at',created_at,'updated_at',updated_at) AS row_json
                            FROM review_followup_artifacts
                            WHERE pr_number=b.pr_number ORDER BY ordinal
                        ) AS a
                    )
                 )
                 FROM review_followup_batches AS b WHERE pr_number=102",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(after, before, "duplicate insert mutated the first snapshot");
        assert!(after.contains("original concern"));
        assert!(!after.contains("replacement concern"));
        assert_eq!(row_count(&conn, "review_followup_artifacts"), 1);
    }

    #[test]
    fn duplicate_ordinal_is_rejected_before_any_write() {
        let (_dir, conn) = database();
        let result = NewReviewFollowupBatch::new(
            103,
            1,
            None,
            1,
            "followups-v1".into(),
            vec![artifact(103, 0, "first"), artifact(103, 0, "duplicate")],
            100,
        );

        assert!(matches!(result, Err(QuorumError::Usage(_))));
        assert_eq!(row_count(&conn, "review_followup_batches"), 0);
        assert_eq!(row_count(&conn, "review_followup_artifacts"), 0);
    }

    #[test]
    fn malformed_and_oversized_json_and_text_are_rejected_at_write_boundary() {
        let (_dir, conn) = database();
        let make = |concern: String, verification: &str, evidence: &str| {
            NewReviewFollowupArtifact::new(
                104,
                0,
                TechnicalImpact::Major,
                ScopeRelationship::DesignDebt,
                concern,
                "reason".into(),
                "behavior".into(),
                "outcome".into(),
                verification,
                evidence,
                100,
            )
        };

        assert!(make(
            "concern".into(),
            "not-json",
            r#"[{"kind":"review","id":1}]"#
        )
        .is_err());
        assert!(make("concern".into(), r#"["verify"]"#, "not-json").is_err());
        assert!(make(
            "concern".into(),
            &"x".repeat(MAX_FOLLOWUP_ARTIFACT_JSON_BYTES + 1),
            r#"[{"kind":"review","id":1}]"#,
        )
        .is_err());
        assert!(make(
            "x".repeat(MAX_FOLLOWUP_TEXT_BYTES + 1),
            r#"["verify"]"#,
            r#"[{"kind":"review","id":1}]"#,
        )
        .is_err());
        let too_many_expectations =
            serde_json::to_string(&vec!["verify"; MAX_VERIFICATION_EXPECTATIONS + 1]).unwrap();
        assert!(make(
            "concern".into(),
            &too_many_expectations,
            r#"[{"kind":"review","id":1}]"#,
        )
        .is_err());
        let too_many_evidence = serde_json::to_string(
            &(1..=(MAX_FOLLOWUP_EVIDENCE_IDS + 1))
                .map(|id| serde_json::json!({"kind":"review","id":id}))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert!(make("concern".into(), r#"["verify"]"#, &too_many_evidence,).is_err());
        assert_eq!(row_count(&conn, "review_followup_batches"), 0);
        assert_eq!(row_count(&conn, "review_followup_artifacts"), 0);
    }

    #[test]
    fn disposition_relationships_and_disposed_artifacts_are_rejected() {
        let (_dir, conn) = database();
        assert!(ArtifactDisposition::new(
            FollowupDisposition::Created,
            "new work".into(),
            Some(2),
            None,
        )
        .is_err());

        let disposed = ReviewFollowupArtifact::new(
            None,
            105,
            0,
            TechnicalImpact::Minor,
            ScopeRelationship::FutureRequirement,
            "concern".into(),
            "reason".into(),
            "behavior".into(),
            "outcome".into(),
            VerificationExpectations::new(vec!["verify".into()]).unwrap(),
            FollowupEvidenceIds::from_json(r#"[{"kind":"review","id":1}]"#).unwrap(),
            Some(
                ArtifactDisposition::new(
                    FollowupDisposition::Created,
                    "new work".into(),
                    None,
                    Some(3),
                )
                .unwrap(),
            ),
            100,
            100,
        )
        .unwrap();
        assert!(NewReviewFollowupArtifact::try_from_artifact(disposed).is_err());
        assert_eq!(row_count(&conn, "review_followup_batches"), 0);
        assert_eq!(row_count(&conn, "review_followup_artifacts"), 0);
    }

    #[test]
    fn artifact_failure_rolls_back_the_batch_and_prior_artifacts() {
        let (_dir, mut conn) = database();
        conn.execute_batch(
            "CREATE TRIGGER reject_second_followup_artifact
             BEFORE INSERT ON review_followup_artifacts
             WHEN NEW.ordinal = 1
             BEGIN
                 SELECT RAISE(ABORT, 'injected artifact failure');
             END;",
        )
        .unwrap();
        let value = batch(
            106,
            1,
            "followups-v1",
            vec![artifact(106, 0, "first"), artifact(106, 1, "second")],
        );

        assert!(insert_batch_if_absent(&mut conn, &value).is_err());
        assert_eq!(row_count(&conn, "review_followup_batches"), 0);
        assert_eq!(row_count(&conn, "review_followup_artifacts"), 0);
    }
}
