//! Durable, daemon-owned GitHub issue intents for review follow-ups.
//!
//! The planner may only propose a closed assessment. This module validates
//! complete artifact coverage and stores issue intents atomically. It performs
//! no network calls: the daemon creates or discovers GitHub issues, records the
//! result, then atomically resolves the assessment and contributing batches.

use crate::db::{begin_immediate, map_sql_err};
use crate::error::{QuorumError, Result};
use crate::review_followups::{MAX_FOLLOWUP_ARTIFACTS, MAX_FOLLOWUP_TEXT_BYTES};
use rusqlite::{params, Connection, Transaction};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub const MAX_CREATED_ISSUES: usize = 8;
pub const MAX_EXISTING_ISSUES: usize = 128;
pub const MAX_ISSUE_ATTEMPTS: i64 = 3;
pub const MAX_ISSUE_BODY_BYTES: usize = 64 * 1024;
pub const REVIEW_FOLLOWUP_LABEL: &str = "review-followup";

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExistingIssue {
    pub number: i64,
    pub url: String,
    pub title: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IssueType {
    Bug,
    Enhancement,
    Documentation,
    Tests,
    Cleanup,
}

impl IssueType {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Bug => "bug",
            Self::Enhancement => "enhancement",
            Self::Documentation => "documentation",
            Self::Tests => "tests",
            Self::Cleanup => "cleanup",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProposedIssue {
    pub title: String,
    pub issue_type: IssueType,
    pub observable_outcome: String,
    pub acceptance_criteria: Vec<String>,
    pub source_constraints: Vec<String>,
    pub verification_expectations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "decision", rename_all = "snake_case", deny_unknown_fields)]
pub enum IssueDecision {
    Create {
        artifact_ids: Vec<i64>,
        reason: String,
        issue: ProposedIssue,
    },
    Link {
        artifact_ids: Vec<i64>,
        reason: String,
        existing_issue_number: i64,
        existing_issue_url: String,
    },
    Dismiss {
        artifact_ids: Vec<i64>,
        reason: String,
        category: DismissCategory,
    },
    Defer {
        artifact_ids: Vec<i64>,
        reason: String,
        required_decision: String,
    },
}

impl IssueDecision {
    fn artifact_ids(&self) -> &[i64] {
        match self {
            Self::Create { artifact_ids, .. }
            | Self::Link { artifact_ids, .. }
            | Self::Dismiss { artifact_ids, .. }
            | Self::Defer { artifact_ids, .. } => artifact_ids,
        }
    }

    fn reason(&self) -> &str {
        match self {
            Self::Create { reason, .. }
            | Self::Link { reason, .. }
            | Self::Dismiss { reason, .. }
            | Self::Defer { reason, .. } => reason,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DismissCategory {
    Invalid,
    Obsolete,
    AlreadyResolved,
    OutOfProduct,
}

impl DismissCategory {
    fn as_str(self) -> &'static str {
        match self {
            Self::Invalid => "invalid",
            Self::Obsolete => "obsolete",
            Self::AlreadyResolved => "already_resolved",
            Self::OutOfProduct => "out_of_product",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FollowupIssuePlan {
    pub outcome: AssessmentOutcome,
    pub decisions: Vec<IssueDecision>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AssessmentOutcome {
    Assessment,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssueIntent {
    pub id: i64,
    pub assessment_id: i64,
    pub ordinal: i64,
    pub title: String,
    pub body: String,
    pub labels: Vec<String>,
    pub idempotency_marker: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageOutcome {
    Staged,
    AlreadyStaged,
}

pub fn parse_and_validate_plan(
    json: &str,
    expected_artifact_ids: &[i64],
    allowed_issues: &[ExistingIssue],
) -> Result<FollowupIssuePlan> {
    if json.is_empty() || json.len() > MAX_ISSUE_BODY_BYTES || json.contains('\0') {
        return Err(QuorumError::Usage(
            "follow-up issue assessment exceeds its bounded response size".into(),
        ));
    }
    let plan: FollowupIssuePlan = serde_json::from_str(json).map_err(|error| {
        QuorumError::Usage(format!("invalid follow-up issue assessment: {error}"))
    })?;
    validate_plan(&plan, expected_artifact_ids, allowed_issues)?;
    Ok(plan)
}

pub fn validate_plan(
    plan: &FollowupIssuePlan,
    expected_artifact_ids: &[i64],
    allowed_issues: &[ExistingIssue],
) -> Result<()> {
    if plan.decisions.is_empty() || plan.decisions.len() > MAX_FOLLOWUP_ARTIFACTS {
        return Err(usage("follow-up assessment must contain 1..=32 decisions"));
    }
    let expected = expected_artifact_ids
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    if expected.len() != expected_artifact_ids.len() || expected.iter().any(|id| *id <= 0) {
        return Err(usage("expected follow-up artifact membership is invalid"));
    }
    let allowed_issues = allowed_issues
        .iter()
        .map(|issue| (issue.number, issue.url.as_str()))
        .collect::<HashSet<_>>();
    let mut observed = HashSet::new();
    let mut create_count = 0usize;
    for decision in &plan.decisions {
        validate_text("decision reason", decision.reason())?;
        let ids = decision.artifact_ids();
        if ids.is_empty() || ids.len() > MAX_FOLLOWUP_ARTIFACTS {
            return Err(usage("each decision must contain 1..=32 artifact ids"));
        }
        for artifact_id in ids {
            if *artifact_id <= 0 || !observed.insert(*artifact_id) {
                return Err(usage("every follow-up artifact must appear exactly once"));
            }
        }
        match decision {
            IssueDecision::Create { issue, .. } => {
                create_count += 1;
                validate_issue(issue)?;
            }
            IssueDecision::Link {
                existing_issue_number,
                existing_issue_url,
                ..
            } => {
                if *existing_issue_number <= 0
                    || !allowed_issues
                        .contains(&(*existing_issue_number, existing_issue_url.as_str()))
                    || !valid_issue_url(existing_issue_url)
                {
                    return Err(usage(
                        "linked issue must be a positive issue from the supplied inventory",
                    ));
                }
            }
            IssueDecision::Dismiss { .. } => {}
            IssueDecision::Defer {
                required_decision, ..
            } => validate_text("required decision", required_decision)?,
        }
    }
    if create_count > MAX_CREATED_ISSUES {
        return Err(usage("one assessment may create at most eight issues"));
    }
    if observed != expected {
        return Err(usage(
            "assessment decisions must cover the complete immutable artifact membership",
        ));
    }
    Ok(())
}

fn validate_issue(issue: &ProposedIssue) -> Result<()> {
    validate_text("issue title", &issue.title)?;
    validate_text("observable outcome", &issue.observable_outcome)?;
    validate_list("acceptance criteria", &issue.acceptance_criteria)?;
    validate_list("source constraints", &issue.source_constraints)?;
    validate_list(
        "verification expectations",
        &issue.verification_expectations,
    )
}

fn validate_list(field: &str, values: &[String]) -> Result<()> {
    if values.is_empty() || values.len() > 8 {
        return Err(usage(&format!("{field} must contain 1..=8 entries")));
    }
    for value in values {
        validate_text(field, value)?;
    }
    Ok(())
}

fn validate_text(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() || value.contains('\0') || value.len() > MAX_FOLLOWUP_TEXT_BYTES {
        return Err(usage(&format!("invalid bounded follow-up {field}")));
    }
    Ok(())
}

fn valid_issue_url(value: &str) -> bool {
    value.starts_with("https://github.com/")
        && value.contains("/issues/")
        && value.len() <= MAX_FOLLOWUP_TEXT_BYTES
        && !value.contains('\0')
}

/// Atomically stage the complete planner assessment. Replaying after a commit
/// is a clean idempotent success; a partial or different stored plan is loud.
pub fn stage_plan(
    conn: &mut Connection,
    assessment_id: i64,
    plan: &FollowupIssuePlan,
    allowed_issues: &[ExistingIssue],
    now: i64,
) -> Result<StageOutcome> {
    if assessment_id <= 0 || now < 0 {
        return Err(usage("invalid follow-up issue assessment relationship"));
    }
    let tx = begin_immediate(conn)?;
    let membership = assessment_membership(&tx, assessment_id)?;
    validate_plan(plan, &membership, allowed_issues)?;
    let plan_json = serde_json::to_string(plan)
        .map_err(|error| QuorumError::Io(format!("serialize follow-up issue plan: {error}")))?;
    let existing: i64 = tx.query_row(
        "SELECT count(*) FROM review_followup_issue_intents WHERE assessment_id=?1",
        [assessment_id],
        |row| row.get(0),
    )?;
    if existing > 0 {
        let stored_plan: Option<String> = tx.query_row(
            "SELECT min(plan_json) FROM review_followup_issue_intents WHERE assessment_id=?1",
            [assessment_id],
            |row| row.get(0),
        )?;
        let distinct_plans: i64 = tx.query_row(
            "SELECT count(DISTINCT plan_json) FROM review_followup_issue_intents WHERE assessment_id=?1",
            [assessment_id],
            |row| row.get(0),
        )?;
        if existing as usize == plan.decisions.len()
            && intent_membership(&tx, assessment_id)? == membership
            && distinct_plans == 1
            && stored_plan.as_deref() == Some(plan_json.as_str())
        {
            tx.commit().map_err(map_sql_err)?;
            return Ok(StageOutcome::AlreadyStaged);
        }
        return Err(usage(
            "stored follow-up issue plan is partial or inconsistent",
        ));
    }
    let authoritative: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM review_followup_assessments
         WHERE id=?1 AND state='planning' AND active=1 AND membership_sealed=1)",
        [assessment_id],
        |row| row.get(0),
    )?;
    if !authoritative {
        return Err(usage(
            "follow-up issue assessment no longer owns planning authority",
        ));
    }

    for (ordinal, decision) in plan.decisions.iter().enumerate() {
        let marker = format!("quorum-review-followup:{assessment_id}:{ordinal}");
        let (kind, state, reason, title, body, labels, issue_number, issue_url, category, required) =
            decision_columns(&tx, assessment_id, ordinal, decision, &marker)?;
        tx.execute(
            "INSERT INTO review_followup_issue_intents(
                 assessment_id,ordinal,decision,state,reason,title,body,labels_json,
                 issue_number,issue_url,dismiss_category,required_decision,
                 plan_json,idempotency_marker,created_at,updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?15)",
            params![
                assessment_id,
                ordinal as i64,
                kind,
                state,
                reason,
                title,
                body,
                labels,
                issue_number,
                issue_url,
                category,
                required,
                plan_json,
                marker,
                now,
            ],
        )?;
        let intent_id = tx.last_insert_rowid();
        for artifact_id in decision.artifact_ids() {
            tx.execute(
                "INSERT INTO review_followup_issue_intent_artifacts(intent_id,artifact_id)
                 VALUES (?1,?2)",
                params![intent_id, artifact_id],
            )?;
        }
    }
    tx.commit().map_err(map_sql_err)?;
    Ok(StageOutcome::Staged)
}

#[allow(clippy::type_complexity)]
fn decision_columns(
    tx: &Transaction<'_>,
    assessment_id: i64,
    ordinal: usize,
    decision: &IssueDecision,
    marker: &str,
) -> Result<(
    &'static str,
    &'static str,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<&'static str>,
    Option<String>,
)> {
    Ok(match decision {
        IssueDecision::Create {
            artifact_ids,
            reason,
            issue,
        } => {
            let labels = serde_json::to_string(&vec![
                REVIEW_FOLLOWUP_LABEL,
                issue.issue_type.as_label(),
                priority_label(tx, artifact_ids)?,
            ])
            .map_err(|error| QuorumError::Io(format!("serialize issue labels: {error}")))?;
            let body = issue_body(tx, assessment_id, ordinal, artifact_ids, issue, marker)?;
            (
                "create",
                "pending",
                reason.clone(),
                Some(issue.title.clone()),
                Some(body),
                Some(labels),
                None,
                None,
                None,
                None,
            )
        }
        IssueDecision::Link {
            reason,
            existing_issue_number,
            existing_issue_url,
            ..
        } => (
            "link",
            "completed",
            reason.clone(),
            None,
            None,
            None,
            Some(*existing_issue_number),
            Some(existing_issue_url.clone()),
            None,
            None,
        ),
        IssueDecision::Dismiss {
            reason, category, ..
        } => (
            "dismiss",
            "completed",
            reason.clone(),
            None,
            None,
            None,
            None,
            None,
            Some(category.as_str()),
            None,
        ),
        IssueDecision::Defer {
            reason,
            required_decision,
            ..
        } => (
            "defer",
            "completed",
            reason.clone(),
            None,
            None,
            None,
            None,
            None,
            None,
            Some(required_decision.clone()),
        ),
    })
}

fn priority_label(tx: &Transaction<'_>, artifact_ids: &[i64]) -> Result<&'static str> {
    let mut highest = 0u8;
    for artifact_id in artifact_ids {
        let impact: String = tx.query_row(
            "SELECT technical_impact FROM review_followup_artifacts WHERE id=?1",
            [artifact_id],
            |row| row.get(0),
        )?;
        highest = highest.max(match impact.as_str() {
            "critical" => 4,
            "major" => 3,
            "minor" => 2,
            "nit" => 1,
            _ => return Err(usage("stored follow-up impact is invalid")),
        });
    }
    Ok(match highest {
        3 | 4 => "priority:high",
        2 => "priority:medium",
        _ => "priority:low",
    })
}

fn issue_body(
    tx: &Transaction<'_>,
    assessment_id: i64,
    ordinal: usize,
    artifact_ids: &[i64],
    issue: &ProposedIssue,
    marker: &str,
) -> Result<String> {
    let mut provenance = Vec::new();
    for artifact_id in artifact_ids {
        let (pr, concern, affected, desired): (i64, String, String, String) = tx.query_row(
            "SELECT pr_number,concern,affected_behavior,desired_outcome
             FROM review_followup_artifacts WHERE id=?1 AND disposition IS NULL",
            [artifact_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        provenance.push(format!(
            "- PR #{pr}, artifact `{artifact_id}`\n  - Concern: {concern}\n  - Affected behavior: {affected}\n  - Desired outcome: {desired}"
        ));
    }
    let bullets = |values: &[String]| {
        values
            .iter()
            .map(|value| format!("- {value}"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let body = format!(
        "## Observable outcome\n\n{}\n\n## Acceptance criteria\n\n{}\n\n## Source constraints\n\n{}\n\n## Verification\n\n{}\n\n## Review provenance\n\n{}\n\n<!-- {} -->\n<!-- assessment:{} decision:{} -->",
        issue.observable_outcome,
        bullets(&issue.acceptance_criteria),
        bullets(&issue.source_constraints),
        bullets(&issue.verification_expectations),
        provenance.join("\n"),
        marker,
        assessment_id,
        ordinal,
    );
    if body.len() > MAX_ISSUE_BODY_BYTES {
        return Err(usage("materialized follow-up issue body exceeds 64 KiB"));
    }
    Ok(body)
}

pub fn pending_issue_intents(
    conn: &Connection,
    limit: usize,
    now: i64,
) -> Result<Vec<IssueIntent>> {
    if limit == 0 || limit > MAX_CREATED_ISSUES {
        return Err(usage("pending issue intent limit must be 1..=8"));
    }
    if now < 0 {
        return Err(usage("pending issue intent time cannot be negative"));
    }
    let mut stmt = conn.prepare(
        "SELECT intent.id,intent.assessment_id,intent.ordinal,intent.title,intent.body,
                intent.labels_json,intent.idempotency_marker
         FROM review_followup_issue_intents intent
         JOIN review_followup_assessments assessment ON assessment.id=intent.assessment_id
         WHERE intent.decision='create' AND intent.state IN ('pending','backoff')
           AND intent.attempts < 3
           AND assessment.state='planning' AND assessment.active=1
           AND (intent.last_attempt_at IS NULL
                OR intent.last_attempt_at + intent.attempts * 60 <= ?2)
         ORDER BY intent.id LIMIT ?1",
    )?;
    let rows = stmt.query_map(params![limit as i64, now], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, String>(6)?,
        ))
    })?;
    rows.map(|row| {
        let (id, assessment_id, ordinal, title, body, labels, marker) = row?;
        let labels: Vec<String> = serde_json::from_str(&labels)
            .map_err(|error| usage(&format!("stored issue labels are invalid: {error}")))?;
        Ok(IssueIntent {
            id,
            assessment_id,
            ordinal,
            title,
            body,
            labels,
            idempotency_marker: marker,
        })
    })
    .collect()
}

pub fn complete_issue_intent(
    conn: &mut Connection,
    intent_id: i64,
    issue_number: i64,
    issue_url: &str,
    now: i64,
) -> Result<bool> {
    if intent_id <= 0 || issue_number <= 0 || now < 0 || !valid_issue_url(issue_url) {
        return Err(usage("invalid created follow-up issue result"));
    }
    let tx = begin_immediate(conn)?;
    let updated = tx.execute(
        "UPDATE review_followup_issue_intents
         SET state='completed',issue_number=?2,issue_url=?3,updated_at=?4
         WHERE id=?1 AND decision='create' AND state IN ('pending','backoff')",
        params![intent_id, issue_number, issue_url, now],
    )?;
    tx.commit().map_err(map_sql_err)?;
    Ok(updated == 1)
}

pub fn record_issue_failure(
    conn: &mut Connection,
    intent_id: i64,
    error: &str,
    now: i64,
) -> Result<bool> {
    if intent_id <= 0 || now < 0 {
        return Err(usage("invalid follow-up issue failure relationship"));
    }
    validate_text("issue failure", error)?;
    let tx = begin_immediate(conn)?;
    let updated = tx.execute(
        "UPDATE review_followup_issue_intents
         SET attempts=attempts+1,
             state=CASE WHEN attempts+1>=3 THEN 'held' ELSE 'backoff' END,
             last_attempt_at=?3,last_error=?2,updated_at=?3
         WHERE id=?1 AND decision='create' AND state IN ('pending','backoff')
           AND attempts < 3",
        params![intent_id, error, now],
    )?;
    if updated == 1 {
        let hold_summary = truncate_utf8(error, 2048);
        tx.execute(
            "UPDATE review_followup_assessments
             SET state='held',active=0,hold_code='issue-materialization',
                 hold_summary=?2,updated_at=?3
             WHERE id=(SELECT assessment_id FROM review_followup_issue_intents WHERE id=?1)
               AND state='planning' AND active=1
               AND EXISTS(SELECT 1 FROM review_followup_issue_intents
                          WHERE id=?1 AND state='held')",
            params![intent_id, hold_summary, now],
        )?;
    }
    tx.commit().map_err(map_sql_err)?;
    Ok(updated == 1)
}

fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut boundary = max_bytes;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    &value[..boundary]
}

pub fn finalize_assessment(conn: &mut Connection, assessment_id: i64, now: i64) -> Result<bool> {
    if assessment_id <= 0 || now < 0 {
        return Err(usage("invalid follow-up issue finalization relationship"));
    }
    let tx = begin_immediate(conn)?;
    let complete: bool = tx.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM review_followup_assessments a
             WHERE a.id=?1 AND a.state='planning' AND a.active=1
               AND a.membership_sealed=1
               AND NOT EXISTS(SELECT 1 FROM review_followup_issue_intents i
                              WHERE i.assessment_id=a.id AND i.state!='completed')
               AND (SELECT count(*) FROM review_followup_assessment_artifacts m
                    WHERE m.assessment_id=a.id) =
                   (SELECT count(*) FROM review_followup_issue_intent_artifacts ia
                    JOIN review_followup_issue_intents i ON i.id=ia.intent_id
                    WHERE i.assessment_id=a.id)
         )",
        [assessment_id],
        |row| row.get(0),
    )?;
    if !complete {
        tx.commit().map_err(map_sql_err)?;
        return Ok(false);
    }
    tx.execute(
        "UPDATE review_followup_batches SET state='resolved',updated_at=?2
         WHERE pr_number IN (
             SELECT DISTINCT artifact.pr_number
             FROM review_followup_issue_intents intent
             JOIN review_followup_issue_intent_artifacts member ON member.intent_id=intent.id
             JOIN review_followup_artifacts artifact ON artifact.id=member.artifact_id
             WHERE intent.assessment_id=?1
         )",
        params![assessment_id, now],
    )?;
    let updated = tx.execute(
        "UPDATE review_followup_assessments
         SET state='completed',active=0,hold_code=NULL,hold_summary=NULL,updated_at=?2
         WHERE id=?1 AND state='planning' AND active=1",
        params![assessment_id, now],
    )?;
    tx.commit().map_err(map_sql_err)?;
    Ok(updated == 1)
}

fn assessment_membership(tx: &Transaction<'_>, assessment_id: i64) -> Result<Vec<i64>> {
    let scope_kind: String = tx.query_row(
        "SELECT scope_kind FROM review_followup_assessments WHERE id=?1",
        [assessment_id],
        |row| row.get(0),
    )?;
    let max_membership = match scope_kind.as_str() {
        "task" => MAX_FOLLOWUP_ARTIFACTS,
        "graph" => crate::review_followup_graph_eligibility::MAX_GRAPH_FOLLOWUP_ARTIFACTS,
        _ => return Err(usage("stored follow-up assessment scope is invalid")),
    };
    let mut stmt = tx.prepare(
        "SELECT m.artifact_id
         FROM review_followup_assessment_artifacts m
         JOIN review_followup_artifacts artifact ON artifact.id=m.artifact_id
         WHERE m.assessment_id=?1 AND artifact.disposition IS NULL
         ORDER BY m.artifact_id LIMIT ?2",
    )?;
    let ids = stmt
        .query_map(params![assessment_id, (max_membership + 1) as i64], |row| {
            row.get::<_, i64>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if ids.is_empty() || ids.len() > max_membership {
        return Err(usage("follow-up issue assessment membership is invalid"));
    }
    Ok(ids)
}

fn intent_membership(tx: &Transaction<'_>, assessment_id: i64) -> Result<Vec<i64>> {
    let mut stmt = tx.prepare(
        "SELECT member.artifact_id
         FROM review_followup_issue_intents intent
         JOIN review_followup_issue_intent_artifacts member ON member.intent_id=intent.id
         WHERE intent.assessment_id=?1 ORDER BY member.artifact_id",
    )?;
    let ids = stmt
        .query_map([assessment_id], |row| row.get::<_, i64>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(ids)
}

fn usage(message: &str) -> QuorumError {
    QuorumError::Usage(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (tempfile::TempDir, Connection, i64, Vec<i64>) {
        let dir = tempfile::tempdir().unwrap();
        let conn = crate::db::open(&dir.path().join("followup-issues.db")).unwrap();
        conn.execute(
            "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at)
             VALUES (1,'source','done','owner',1,1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO review_followup_batches(
                 pr_number,task_id,source_task_id,collector_version,artifact_count,state,created_at,updated_at)
             VALUES (42,1,1,'v1',2,'assessing',1,1)",
            [],
        )
        .unwrap();
        for (ordinal, impact) in [(0, "major"), (1, "minor")] {
            conn.execute(
                "INSERT INTO review_followup_artifacts(
                     pr_number,ordinal,technical_impact,scope_relationship,concern,
                     non_blocking_reason,affected_behavior,desired_outcome,
                     verification_expectations,evidence_ids,created_at,updated_at)
                 VALUES (42,?1,?2,'out_of_scope','failure','safe to defer','behavior',
                         'desired','[\"verify\"]','[{\"kind\":\"review\",\"id\":1}]',1,1)",
                params![ordinal, impact],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO review_followup_assessments(
                 id,target,scope_kind,scope_id,source_task_id,state,active,membership_sealed,
                 created_at,updated_at)
             VALUES (7,'followup:task:1','task',1,1,'pending',0,0,1,1)",
            [],
        )
        .unwrap();
        let ids = {
            let mut stmt = conn
                .prepare("SELECT id FROM review_followup_artifacts ORDER BY id")
                .unwrap();
            stmt.query_map([], |row| row.get::<_, i64>(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        for id in &ids {
            conn.execute(
                "INSERT INTO review_followup_assessment_artifacts(assessment_id,artifact_id)
                 VALUES (7,?1)",
                [id],
            )
            .unwrap();
        }
        conn.execute(
            "UPDATE review_followup_assessments
             SET membership_sealed=1,state='planning',active=1 WHERE id=7",
            [],
        )
        .unwrap();
        (dir, conn, 7, ids)
    }

    fn plan(ids: &[i64]) -> FollowupIssuePlan {
        FollowupIssuePlan {
            outcome: AssessmentOutcome::Assessment,
            decisions: vec![IssueDecision::Create {
                artifact_ids: ids.to_vec(),
                reason: "same root concern".into(),
                issue: ProposedIssue {
                    title: "Make shutdown behavior explicit".into(),
                    issue_type: IssueType::Bug,
                    observable_outcome: "Shutdown preserves pending work".into(),
                    acceptance_criteria: vec!["Pending work completes".into()],
                    source_constraints: vec!["Preserve current API".into()],
                    verification_expectations: vec!["Concurrent shutdown test".into()],
                },
            }],
        }
    }

    fn existing_issue(number: i64) -> ExistingIssue {
        ExistingIssue {
            number,
            url: format!("https://github.com/o/r/issues/{number}"),
            title: "Existing tracking issue".into(),
        }
    }

    #[test]
    fn validation_requires_exact_membership_and_inventory_links() {
        let ids = [10, 11];
        assert!(validate_plan(&plan(&ids), &ids, &[]).is_ok());
        assert!(validate_plan(&plan(&ids[..1]), &ids, &[]).is_err());
        let linked = FollowupIssuePlan {
            outcome: AssessmentOutcome::Assessment,
            decisions: vec![IssueDecision::Link {
                artifact_ids: ids.to_vec(),
                reason: "already tracked".into(),
                existing_issue_number: 9,
                existing_issue_url: "https://github.com/o/r/issues/9".into(),
            }],
        };
        assert!(validate_plan(&linked, &ids, &[]).is_err());
        assert!(validate_plan(&linked, &ids, &[existing_issue(9)]).is_ok());
        assert!(validate_plan(
            &linked,
            &ids,
            &[ExistingIssue {
                number: 9,
                url: "https://github.com/other/repo/issues/9".into(),
                title: "Wrong repository".into(),
            }]
        )
        .is_err());
    }

    #[test]
    fn stage_complete_and_finalize_is_replay_safe() {
        let (_dir, mut conn, assessment_id, ids) = fixture();
        let plan = plan(&ids);
        assert_eq!(
            stage_plan(&mut conn, assessment_id, &plan, &[], 2).unwrap(),
            StageOutcome::Staged
        );
        assert_eq!(
            stage_plan(&mut conn, assessment_id, &plan, &[], 3).unwrap(),
            StageOutcome::AlreadyStaged
        );
        let pending = pending_issue_intents(&conn, 8, 3).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(
            pending[0].labels,
            ["review-followup", "bug", "priority:high"]
        );
        assert!(pending[0].body.contains("quorum-review-followup:7:0"));
        assert!(!finalize_assessment(&mut conn, assessment_id, 4).unwrap());
        assert!(complete_issue_intent(
            &mut conn,
            pending[0].id,
            123,
            "https://github.com/o/r/issues/123",
            5,
        )
        .unwrap());
        assert!(finalize_assessment(&mut conn, assessment_id, 6).unwrap());
        assert!(!finalize_assessment(&mut conn, assessment_id, 7).unwrap());
        assert_eq!(
            conn.query_row(
                "SELECT state FROM review_followup_batches WHERE pr_number=42",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
            "resolved"
        );
        assert_eq!(
            conn.query_row(
                "SELECT state || ':' || active FROM review_followup_assessments WHERE id=7",
                [],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
            "completed:0"
        );
    }

    #[test]
    fn replay_rejects_a_different_plan_with_the_same_shape_and_membership() {
        let (_dir, mut conn, assessment_id, ids) = fixture();
        let original = plan(&ids);
        stage_plan(&mut conn, assessment_id, &original, &[], 2).unwrap();
        let mut changed = plan(&ids);
        let IssueDecision::Create { reason, .. } = &mut changed.decisions[0] else {
            unreachable!()
        };
        *reason = "a different grouping rationale".into();
        let error = stage_plan(&mut conn, assessment_id, &changed, &[], 3).unwrap_err();
        assert!(error.to_string().contains("partial or inconsistent"));
    }

    #[test]
    fn link_only_plan_completes_without_an_issue_outbox_call() {
        let (_dir, mut conn, assessment_id, ids) = fixture();
        let issue = existing_issue(9);
        let plan = FollowupIssuePlan {
            outcome: AssessmentOutcome::Assessment,
            decisions: vec![IssueDecision::Link {
                artifact_ids: ids,
                reason: "the existing issue owns the same outcome".into(),
                existing_issue_number: issue.number,
                existing_issue_url: issue.url.clone(),
            }],
        };
        stage_plan(&mut conn, assessment_id, &plan, &[issue], 2).unwrap();
        assert!(pending_issue_intents(&conn, 1, 2).unwrap().is_empty());
        assert!(finalize_assessment(&mut conn, assessment_id, 3).unwrap());
    }

    #[test]
    fn external_failures_back_off_and_hold_at_the_bound() {
        let (_dir, mut conn, assessment_id, ids) = fixture();
        stage_plan(&mut conn, assessment_id, &plan(&ids), &[], 2).unwrap();
        let intent = pending_issue_intents(&conn, 1, 2).unwrap().remove(0);

        assert!(record_issue_failure(&mut conn, intent.id, "network one", 10).unwrap());
        assert!(pending_issue_intents(&conn, 1, 69).unwrap().is_empty());
        assert_eq!(pending_issue_intents(&conn, 1, 70).unwrap().len(), 1);
        assert!(record_issue_failure(&mut conn, intent.id, "network two", 70).unwrap());
        assert!(pending_issue_intents(&conn, 1, 189).unwrap().is_empty());
        assert_eq!(pending_issue_intents(&conn, 1, 190).unwrap().len(), 1);
        assert!(record_issue_failure(&mut conn, intent.id, "network three", 190).unwrap());
        assert!(pending_issue_intents(&conn, 1, i64::MAX)
            .unwrap()
            .is_empty());
        assert_eq!(
            conn.query_row(
                "SELECT state || ':' || attempts FROM review_followup_issue_intents WHERE id=?1",
                [intent.id],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
            "held:3"
        );
        assert_eq!(
            conn.query_row(
                "SELECT state || ':' || active || ':' || hold_code
                 FROM review_followup_assessments WHERE id=?1",
                [assessment_id],
                |row| row.get::<_, String>(0)
            )
            .unwrap(),
            "held:0:issue-materialization"
        );
        assert!(!finalize_assessment(&mut conn, assessment_id, 191).unwrap());
    }
}
