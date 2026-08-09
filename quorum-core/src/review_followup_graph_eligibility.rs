//! Short, read-only eligibility classification for terminal Task Graphs.
//!
//! Generated children never become assessment scopes here. This module folds
//! their immutable follow-up artifacts into the accepted terminal graph and
//! returns only a classification; it creates no assessment or membership rows.

use crate::decomposition::MAX_CHILDREN;
use crate::error::{QuorumError, Result};
use crate::review_followups::{MAX_FOLLOWUP_ARTIFACTS, MAX_FOLLOWUP_TEXT_BYTES};
use rusqlite::{params, Connection};
use std::collections::{HashMap, HashSet};

/// Maximum artifact membership a valid graph can expose in one assessment.
pub const MAX_GRAPH_FOLLOWUP_ARTIFACTS: usize = MAX_CHILDREN * MAX_FOLLOWUP_ARTIFACTS;

/// The dormant assessment disposition of one graph scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphAssessmentEligibility {
    /// The graph is absent, non-terminal, has no merged child, lacks durable
    /// delivery identity, or has already entered/resolved assessment.
    Ineligible,
    /// The graph is terminal and merge-backed, but at least one merged child
    /// lacks a complete successful interpretation for the requested generation.
    WaitingInterpretation,
    /// Every merged child has a complete immutable zero-artifact batch.
    PermanentlySkipped,
    /// The terminal graph owns these unresolved generated-child artifacts.
    Eligible(GraphAssessmentScope),
}

/// Exact graph authority and artifact membership discovered by one SQLite read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphAssessmentScope {
    graph_id: i64,
    source_task_id: i64,
    artifact_ids: Vec<i64>,
}

impl GraphAssessmentScope {
    pub fn graph_id(&self) -> i64 {
        self.graph_id
    }

    pub fn source_task_id(&self) -> i64 {
        self.source_task_id
    }

    pub fn artifact_ids(&self) -> &[i64] {
        &self.artifact_ids
    }
}

#[derive(Debug)]
struct ChildRead {
    status: String,
    pr_number: Option<i64>,
    interpretation_complete: bool,
    batch_state: Option<String>,
    expected_artifacts: Option<usize>,
    actual_artifacts: usize,
}

/// Classify one Task Graph for a specific collector generation.
///
/// One bounded SELECT owns the complete SQLite snapshot. It reads only the
/// graph's accepted plan revision and at most the maximum valid graph artifact
/// membership plus one overflow row. No transaction remains open after return.
pub fn classify_graph_assessment(
    conn: &Connection,
    graph_id: i64,
    collector_version: &str,
) -> Result<GraphAssessmentEligibility> {
    if graph_id <= 0 {
        return Err(QuorumError::Usage(
            "follow-up graph scope id must be positive".into(),
        ));
    }
    if collector_version.trim().is_empty()
        || collector_version.contains('\0')
        || collector_version.len() > MAX_FOLLOWUP_TEXT_BYTES
    {
        return Err(QuorumError::Usage(
            "invalid bounded follow-up collector version".into(),
        ));
    }

    // A valid graph has at most MAX_CHILDREN batches of at most
    // MAX_FOLLOWUP_ARTIFACTS rows. Reading one extra row makes corrupt storage
    // loud without allowing it to make this eligibility pass unbounded.
    let row_limit = i64::try_from(MAX_GRAPH_FOLLOWUP_ARTIFACTS + 1)
        .map_err(|_| QuorumError::Usage("follow-up graph read limit is invalid".into()))?;
    let mut statement = conn.prepare(
        "WITH graph_scope AS (
             SELECT graph.id,graph.source_task_id,graph.state,
                    graph.accepted_plan_revision,source.status AS source_status
             FROM task_decompositions graph
             JOIN tasks source ON source.id=graph.source_task_id
             WHERE graph.id=?1
         ), accepted_members AS (
             SELECT member.graph_id,member.task_id,child.status,
                    CASE WHEN json_valid(child.refs) THEN
                      CASE WHEN json_type(child.refs,'$.pr')='integer'
                              AND json_extract(child.refs,'$.pr')>0
                           THEN json_extract(child.refs,'$.pr') END
                    END AS pr_number
             FROM task_graph_members member
             JOIN graph_scope graph ON graph.id=member.graph_id
              AND member.plan_revision=graph.accepted_plan_revision
             JOIN tasks child ON child.id=member.task_id
         )
         SELECT graph.id,graph.source_task_id,graph.state,graph.source_status,
                member.task_id,member.status,member.pr_number,
                run.task_id,run.status,run.collector_version,
                batch.task_id,batch.graph_id,batch.source_task_id,
                batch.collector_version,batch.artifact_count,batch.state,
                artifact.id,artifact.disposition,
                assessment.id,membership.assessment_id
         FROM graph_scope graph
         LEFT JOIN accepted_members member ON member.graph_id=graph.id
         LEFT JOIN review_collection_runs run
           ON member.status='done' AND run.pr_number=member.pr_number
         LEFT JOIN review_followup_batches batch
           ON member.status='done' AND batch.pr_number=member.pr_number
         LEFT JOIN review_followup_artifacts artifact ON artifact.pr_number=batch.pr_number
         LEFT JOIN review_followup_assessments assessment
           ON assessment.scope_kind='graph' AND assessment.scope_id=graph.id
         LEFT JOIN review_followup_assessment_artifacts membership
           ON membership.artifact_id=artifact.id
         ORDER BY member.task_id,artifact.id
         LIMIT ?2",
    )?;
    let mut rows = statement.query(params![graph_id, row_limit])?;
    let mut row_count = 0usize;
    let mut graph: Option<(i64, String, String)> = None;
    let mut children: HashMap<i64, ChildRead> = HashMap::new();
    let mut artifact_ids = HashSet::new();
    let mut unresolved_ids = HashSet::new();
    let mut assessment_exists = false;
    let mut membership_exists = false;

    while let Some(row) = rows.next()? {
        row_count += 1;
        if row_count > MAX_GRAPH_FOLLOWUP_ARTIFACTS {
            return Err(QuorumError::Usage(
                "stored follow-up graph exceeds the artifact read bound".into(),
            ));
        }

        let source_task_id: i64 = row.get(1)?;
        let graph_state: String = row.get(2)?;
        let source_status: String = row.get(3)?;
        graph.get_or_insert((source_task_id, graph_state, source_status));
        assessment_exists |= row.get::<_, Option<i64>>(18)?.is_some();
        membership_exists |= row.get::<_, Option<i64>>(19)?.is_some();

        let Some(child_id) = row.get::<_, Option<i64>>(4)? else {
            continue;
        };
        let child_status: String = row.get(5)?;
        let pr_number: Option<i64> = row.get(6)?;
        let run_task_id: Option<i64> = row.get(7)?;
        let run_status: Option<String> = row.get(8)?;
        let run_version: Option<String> = row.get(9)?;
        let batch_task_id: Option<i64> = row.get(10)?;
        let batch_graph_id: Option<i64> = row.get(11)?;
        let batch_source_task_id: Option<i64> = row.get(12)?;
        let batch_version: Option<String> = row.get(13)?;
        let batch_count: Option<i64> = row.get(14)?;
        let batch_state: Option<String> = row.get(15)?;
        let expected_artifacts = batch_count
            .map(|count| {
                usize::try_from(count).map_err(|_| {
                    QuorumError::Usage("stored follow-up artifact count is invalid".into())
                })
            })
            .transpose()?;
        if expected_artifacts.is_some_and(|count| count > MAX_FOLLOWUP_ARTIFACTS) {
            return Err(QuorumError::Usage(
                "stored follow-up batch exceeds the artifact read bound".into(),
            ));
        }
        let interpretation_complete = pr_number.is_some()
            && run_task_id == Some(child_id)
            && run_status.as_deref() == Some("success")
            && run_version.as_deref() == Some(collector_version)
            && batch_task_id == Some(child_id)
            && batch_graph_id == Some(graph_id)
            && batch_source_task_id == Some(source_task_id)
            && batch_version.as_deref() == Some(collector_version)
            && expected_artifacts.is_some();

        let child = children.entry(child_id).or_insert_with(|| ChildRead {
            status: child_status,
            pr_number,
            interpretation_complete,
            batch_state,
            expected_artifacts,
            actual_artifacts: 0,
        });
        let artifact_id: Option<i64> = row.get(16)?;
        if let Some(artifact_id) = artifact_id {
            if artifact_id <= 0 || !artifact_ids.insert(artifact_id) {
                return Err(QuorumError::Usage(
                    "stored follow-up graph has invalid artifact membership".into(),
                ));
            }
            child.actual_artifacts += 1;
            if row.get::<_, Option<String>>(17)?.is_none() {
                unresolved_ids.insert(artifact_id);
            }
        }
    }
    drop(rows);
    drop(statement);

    let Some((source_task_id, graph_state, source_status)) = graph else {
        return Ok(GraphAssessmentEligibility::Ineligible);
    };
    if children.is_empty()
        || !matches!(graph_state.as_str(), "completed" | "cancelled")
        || (graph_state == "completed" && source_status != "done")
        || (graph_state == "cancelled" && source_status != "cancelled")
    {
        return Ok(GraphAssessmentEligibility::Ineligible);
    }

    let done_children = children
        .values()
        .filter(|child| child.status == "done")
        .count();
    let done_child_missing_pr = children
        .values()
        .any(|child| child.status == "done" && child.pr_number.is_none());
    if done_children == 0
        || done_child_missing_pr
        || (graph_state == "completed" && done_children != children.len())
    {
        return Ok(GraphAssessmentEligibility::Ineligible);
    }

    let merged_children = children
        .values()
        .filter(|child| child.status == "done")
        .collect::<Vec<_>>();
    if merged_children.iter().any(|child| {
        !child.interpretation_complete || child.expected_artifacts != Some(child.actual_artifacts)
    }) {
        return Ok(GraphAssessmentEligibility::WaitingInterpretation);
    }

    let expected_artifact_count = merged_children
        .iter()
        .map(|child| child.expected_artifacts.unwrap_or(0))
        .sum::<usize>();
    if expected_artifact_count == 0 {
        return Ok(GraphAssessmentEligibility::PermanentlySkipped);
    }

    // Non-collected batches and already-owned artifacts are lifecycle evidence,
    // not fresh eligibility. A valid atomic application leaves no partial mix.
    if assessment_exists
        || membership_exists
        || merged_children
            .iter()
            .any(|child| child.batch_state.as_deref() != Some("collected"))
        || unresolved_ids.len() != expected_artifact_count
    {
        return Ok(GraphAssessmentEligibility::Ineligible);
    }

    let mut artifact_ids = unresolved_ids.into_iter().collect::<Vec<_>>();
    artifact_ids.sort_unstable();
    Ok(GraphAssessmentEligibility::Eligible(GraphAssessmentScope {
        graph_id,
        source_task_id,
        artifact_ids,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::review_followup_writes::{
        insert_batch_if_absent, NewReviewFollowupArtifact, NewReviewFollowupBatch,
    };
    use crate::review_followups::{ScopeRelationship, TechnicalImpact};
    use rusqlite::params;
    use tempfile::TempDir;

    const CURRENT_VERSION: &str = "followups-current";

    #[derive(Clone, Copy)]
    struct ChildSpec {
        status: &'static str,
        pr_number: Option<i64>,
    }

    fn database() -> (TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let connection = crate::db::open(&dir.path().join("graph-eligibility.db")).unwrap();
        (dir, connection)
    }

    fn graph(connection: &Connection, state: &str, children: &[ChildSpec]) -> (i64, i64, Vec<i64>) {
        let source_status = match state {
            "completed" => "done",
            "cancelled" => "cancelled",
            _ => "decomposed",
        };
        connection
            .execute(
                "INSERT INTO tasks(title,status,created_by,created_at,updated_at)
                 VALUES ('source',?1,'owner',1,1)",
                [source_status],
            )
            .unwrap();
        let source_id = connection.last_insert_rowid();
        connection
            .execute(
                "INSERT INTO task_decompositions(
                     source_task_id,state,active,freeze_active,planned_source_revision,
                     plan_revision,accepted_plan_revision,created_at,updated_at)
                 VALUES (?1,?2,?3,0,1,1,1,1,1)",
                params![source_id, state, i64::from(state == "active")],
            )
            .unwrap();
        let graph_id = connection.last_insert_rowid();
        let mut child_ids = Vec::new();
        for (ordinal, child) in children.iter().enumerate() {
            let refs = child.pr_number.map(|pr| format!(r#"{{"pr":{pr}}}"#));
            connection
                .execute(
                    "INSERT INTO tasks(title,status,created_by,created_at,updated_at,refs)
                     VALUES (?1,?2,'owner',1,1,?3)",
                    params![format!("child {ordinal}"), child.status, refs],
                )
                .unwrap();
            let child_id = connection.last_insert_rowid();
            connection
                .execute(
                    "INSERT INTO task_graph_members(
                         graph_id,task_id,local_key,plan_revision,active)
                     VALUES (?1,?2,?3,1,?4)",
                    params![
                        graph_id,
                        child_id,
                        format!("child-{ordinal}"),
                        i64::from(state != "cancelled")
                    ],
                )
                .unwrap();
            child_ids.push(child_id);
        }
        (graph_id, source_id, child_ids)
    }

    fn artifact(pr_number: i64, ordinal: i64) -> NewReviewFollowupArtifact {
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
            10,
        )
        .unwrap()
    }

    fn interpreted(
        connection: &mut Connection,
        graph_id: i64,
        source_id: i64,
        child_id: i64,
        pr_number: i64,
        artifact_count: usize,
        version: &str,
    ) {
        connection
            .execute(
                "INSERT INTO review_collection_runs(
                     pr_number,task_id,status,collector_model,collector_version,
                     findings_count,followup_count,attempted_at,completed_at)
                 VALUES (?1,?2,'success','model',?3,0,?4,10,11)",
                params![pr_number, child_id, version, artifact_count as i64],
            )
            .unwrap();
        let artifacts = (0..artifact_count)
            .map(|ordinal| artifact(pr_number, ordinal as i64))
            .collect();
        let batch = NewReviewFollowupBatch::new(
            pr_number,
            child_id,
            Some(graph_id),
            source_id,
            version.into(),
            artifacts,
            10,
        )
        .unwrap();
        insert_batch_if_absent(connection, &batch).unwrap();
    }

    fn eligible_scope(result: GraphAssessmentEligibility) -> GraphAssessmentScope {
        match result {
            GraphAssessmentEligibility::Eligible(scope) => scope,
            other => panic!("expected eligible graph scope, got {other:?}"),
        }
    }

    #[test]
    fn completed_graph_folds_generated_child_artifacts_into_one_graph_scope() {
        let (_dir, mut connection) = database();
        let specs = [
            ChildSpec {
                status: "done",
                pr_number: Some(101),
            },
            ChildSpec {
                status: "done",
                pr_number: Some(102),
            },
        ];
        let (graph_id, source_id, child_ids) = graph(&connection, "completed", &specs);
        interpreted(
            &mut connection,
            graph_id,
            source_id,
            child_ids[0],
            101,
            1,
            CURRENT_VERSION,
        );
        interpreted(
            &mut connection,
            graph_id,
            source_id,
            child_ids[1],
            102,
            2,
            CURRENT_VERSION,
        );
        let changes_before: i64 = connection
            .query_row("SELECT total_changes()", [], |row| row.get(0))
            .unwrap();

        let scope = eligible_scope(
            classify_graph_assessment(&connection, graph_id, CURRENT_VERSION).unwrap(),
        );

        assert_eq!(scope.graph_id(), graph_id);
        assert_eq!(scope.source_task_id(), source_id);
        assert_eq!(scope.artifact_ids().len(), 3);
        assert_eq!(
            connection
                .query_row("SELECT total_changes()", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            changes_before,
            "eligibility classification must be read-only"
        );
        assert!(connection.is_autocommit());
        assert_eq!(
            connection
                .query_row(
                    "SELECT count(*) FROM review_followup_assessments",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0,
            "generated children must not create independent assessment jobs"
        );
    }

    #[test]
    fn cancelled_graph_with_merged_subset_is_eligible_but_without_merge_is_not() {
        let (_dir, mut connection) = database();
        let specs = [
            ChildSpec {
                status: "done",
                pr_number: Some(111),
            },
            ChildSpec {
                status: "cancelled",
                pr_number: None,
            },
        ];
        let (graph_id, source_id, child_ids) = graph(&connection, "cancelled", &specs);
        interpreted(
            &mut connection,
            graph_id,
            source_id,
            child_ids[0],
            111,
            1,
            CURRENT_VERSION,
        );
        assert!(matches!(
            classify_graph_assessment(&connection, graph_id, CURRENT_VERSION).unwrap(),
            GraphAssessmentEligibility::Eligible(_)
        ));

        let (_dir, connection) = database();
        let (graph_id, _, _) = graph(
            &connection,
            "cancelled",
            &[ChildSpec {
                status: "cancelled",
                pr_number: None,
            }],
        );
        assert_eq!(
            classify_graph_assessment(&connection, graph_id, CURRENT_VERSION).unwrap(),
            GraphAssessmentEligibility::Ineligible
        );
    }

    #[test]
    fn active_and_missing_pr_graphs_are_ineligible() {
        let (_dir, connection) = database();
        let (active_graph, _, _) = graph(
            &connection,
            "active",
            &[ChildSpec {
                status: "done",
                pr_number: Some(121),
            }],
        );
        assert_eq!(
            classify_graph_assessment(&connection, active_graph, CURRENT_VERSION).unwrap(),
            GraphAssessmentEligibility::Ineligible
        );

        let (_dir, connection) = database();
        let (missing_pr_graph, _, _) = graph(
            &connection,
            "completed",
            &[ChildSpec {
                status: "done",
                pr_number: None,
            }],
        );
        assert_eq!(
            classify_graph_assessment(&connection, missing_pr_graph, CURRENT_VERSION).unwrap(),
            GraphAssessmentEligibility::Ineligible
        );
    }

    #[test]
    fn terminal_graph_waits_for_complete_current_generation_interpretation() {
        for setup in ["missing-run", "failed-run", "old-run", "missing-batch"] {
            let (_dir, mut connection) = database();
            let (graph_id, source_id, child_ids) = graph(
                &connection,
                "completed",
                &[ChildSpec {
                    status: "done",
                    pr_number: Some(131),
                }],
            );
            match setup {
                "missing-run" => {}
                "failed-run" => {
                    connection
                        .execute(
                            "INSERT INTO review_collection_runs(
                             pr_number,task_id,status,error,collector_model,collector_version,
                             attempted_at)
                         VALUES (131,?1,'failed','failure','model',?2,10)",
                            params![child_ids[0], CURRENT_VERSION],
                        )
                        .unwrap();
                }
                "old-run" => interpreted(
                    &mut connection,
                    graph_id,
                    source_id,
                    child_ids[0],
                    131,
                    1,
                    "followups-old",
                ),
                "missing-batch" => {
                    connection
                        .execute(
                            "INSERT INTO review_collection_runs(
                             pr_number,task_id,status,collector_model,collector_version,
                             findings_count,followup_count,attempted_at,completed_at)
                         VALUES (131,?1,'success','model',?2,0,1,10,11)",
                            params![child_ids[0], CURRENT_VERSION],
                        )
                        .unwrap();
                }
                _ => unreachable!(),
            }

            assert_eq!(
                classify_graph_assessment(&connection, graph_id, CURRENT_VERSION).unwrap(),
                GraphAssessmentEligibility::WaitingInterpretation,
                "case {setup}"
            );
        }
    }

    #[test]
    fn zero_artifact_graph_is_permanently_skipped() {
        let (_dir, mut connection) = database();
        let (graph_id, source_id, child_ids) = graph(
            &connection,
            "completed",
            &[ChildSpec {
                status: "done",
                pr_number: Some(141),
            }],
        );
        interpreted(
            &mut connection,
            graph_id,
            source_id,
            child_ids[0],
            141,
            0,
            CURRENT_VERSION,
        );

        assert_eq!(
            classify_graph_assessment(&connection, graph_id, CURRENT_VERSION).unwrap(),
            GraphAssessmentEligibility::PermanentlySkipped
        );
        connection
            .execute(
                "UPDATE review_collection_runs SET attempted_at=20,completed_at=21
                 WHERE pr_number=141",
                [],
            )
            .unwrap();
        assert_eq!(
            classify_graph_assessment(&connection, graph_id, CURRENT_VERSION).unwrap(),
            GraphAssessmentEligibility::PermanentlySkipped,
            "later interpretation audit data cannot make an immutable zero batch eligible"
        );
    }

    #[test]
    fn resolved_artifact_graph_is_not_freshly_eligible() {
        let (_dir, mut connection) = database();
        let (graph_id, source_id, child_ids) = graph(
            &connection,
            "completed",
            &[ChildSpec {
                status: "done",
                pr_number: Some(151),
            }],
        );
        interpreted(
            &mut connection,
            graph_id,
            source_id,
            child_ids[0],
            151,
            1,
            CURRENT_VERSION,
        );
        connection
            .execute(
                "UPDATE review_followup_artifacts
                 SET disposition='dismissed',disposition_reason='resolved',updated_at=20
                 WHERE pr_number=151",
                [],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE review_followup_batches SET state='resolved',updated_at=20
                 WHERE pr_number=151",
                [],
            )
            .unwrap();

        assert_eq!(
            classify_graph_assessment(&connection, graph_id, CURRENT_VERSION).unwrap(),
            GraphAssessmentEligibility::Ineligible
        );
    }
}
