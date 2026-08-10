//! Automatic post-merge review-quality analytics (#93 → #125).
//!
//! After a PR merges, the daemon spawns a cheap Haiku-class classifier to read the
//! finished PR (metadata, submitted reviews, inline + conversation comments, commits,
//! CI status, and quorum-side task metadata) and extract structured findings:
//! blockers vs advisory, addressed vs unaddressed, author pushbacks accepted or
//! overridden, all pinned to concrete GitHub record ids as evidence. Rows land in
//! `review_findings`; every attempt (success or failure) leaves a row in
//! `review_collection_runs`.
//!
//! Idempotency: `replace_for_pr` is delete-then-insert scoped to the PR, and
//! `record_run` is UPSERT keyed on `pr_number`. Re-running the collector for the
//! same PR overwrites the prior result — no duplicates, no partial states.
//!
//! Lifecycle boundary: this module reads and stores analytics only. Nothing here
//! transitions tasks, votes verdicts, or posts to GitHub. Collection failures are
//! recorded (loud) and returned to the caller; they never unwind a merge.

use crate::clock;
use crate::db::begin_immediate;
use crate::error::Result;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

/// A single reviewer-raised finding extracted from a PR's review record.
///
/// Field semantics are prompt-anchored: the classifier prompt lists the exact
/// vocabulary for `kind`, `addressed_status`, `pushback_accepted`, `severity`, and
/// `source_endpoint`; the DB CHECK constraints pin the two closed enums that
/// existed pre-v24. New fields (addressed_status, evidence_ids, collector_model,
/// collector_version) are validation-lite at the DB layer so a future prompt
/// revision doesn't require a migration to add a new vocabulary term.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewFinding {
    #[serde(skip_deserializing)]
    pub id: i64,
    #[serde(default)]
    pub pr_number: i64,
    #[serde(default)]
    pub task_id: Option<i64>,
    pub reviewer: String,
    pub kind: String,
    #[serde(default)]
    pub author_pushback: bool,
    #[serde(default)]
    pub pushback_accepted: Option<bool>,
    #[serde(default)]
    pub severity: Option<String>,
    pub text: String,
    #[serde(default = "default_source")]
    pub source_endpoint: String,
    /// v24: 'addressed' | 'unaddressed' | 'partial' | 'unclear' | 'withdrawn'.
    /// A blocking finding that merged as `unaddressed` is a review-quality
    /// signal; the collector must never mutate the merge outcome regardless.
    #[serde(default)]
    pub addressed_status: Option<String>,
    /// v24: JSON array of `{"kind": "review"|"review_comment"|"issue_comment", "id": <gh_id>}`
    /// records tying this finding to concrete GitHub rows.
    #[serde(default)]
    pub evidence: Vec<EvidenceId>,
    /// v24: stamped by the collector wrapper before write; classifier response omits.
    #[serde(default, skip_deserializing)]
    pub collector_model: Option<String>,
    #[serde(default, skip_deserializing)]
    pub collector_version: Option<String>,
}

/// Provenance pointer from a stored finding back to a specific GitHub record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceId {
    pub kind: String,
    pub id: i64,
}

fn default_source() -> String {
    "pulls".to_string()
}

/// Classifier response schema: strict `{"findings":[...]}` envelope.
#[derive(Debug, Deserialize)]
pub struct InterpreterResponse {
    pub findings: Vec<ReviewFinding>,
}

/// Replace all findings for a PR (idempotent re-run). Also carries the collector
/// provenance stamps into each row for later filter-by-generation.
pub fn replace_for_pr(
    conn: &mut Connection,
    pr_number: i64,
    findings: &[ReviewFinding],
) -> Result<()> {
    let now = clock::now();
    let tx = begin_immediate(conn)?;
    replace_for_pr_inner(&tx, pr_number, findings, now)?;
    tx.commit()?;
    Ok(())
}

fn replace_for_pr_inner(
    conn: &Connection,
    pr_number: i64,
    findings: &[ReviewFinding],
    now: i64,
) -> Result<()> {
    conn.execute(
        "DELETE FROM review_findings WHERE pr_number = ?1",
        params![pr_number],
    )?;
    for f in findings {
        let evidence_json = if f.evidence.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&f.evidence).unwrap_or_else(|_| "[]".to_string()))
        };
        conn.execute(
            "INSERT INTO review_findings
                (pr_number, task_id, reviewer, kind, author_pushback, pushback_accepted,
                 severity, text, source_endpoint, created_at,
                 addressed_status, evidence_ids, collector_model, collector_version)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                pr_number,
                f.task_id,
                f.reviewer,
                f.kind,
                f.author_pushback as i64,
                f.pushback_accepted.map(|b| b as i64),
                f.severity,
                f.text,
                f.source_endpoint,
                now,
                f.addressed_status,
                evidence_json,
                f.collector_model,
                f.collector_version,
            ],
        )?;
    }
    Ok(())
}

/// List all findings for a PR (ordered by insertion id).
pub fn list_for_pr(conn: &Connection, pr_number: i64) -> Result<Vec<ReviewFinding>> {
    let mut stmt = conn.prepare(
        "SELECT id, pr_number, task_id, reviewer, kind, author_pushback, pushback_accepted,
                severity, text, source_endpoint,
                addressed_status, evidence_ids, collector_model, collector_version
         FROM review_findings WHERE pr_number = ?1 ORDER BY id",
    )?;
    let rows = stmt.query_map(params![pr_number], |r| {
        let evidence_json: Option<String> = r.get(11)?;
        let evidence: Vec<EvidenceId> = evidence_json
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();
        Ok(ReviewFinding {
            id: r.get(0)?,
            pr_number: r.get(1)?,
            task_id: r.get(2)?,
            reviewer: r.get(3)?,
            kind: r.get(4)?,
            author_pushback: r.get::<_, i64>(5)? != 0,
            pushback_accepted: r.get::<_, Option<i64>>(6)?.map(|v| v != 0),
            severity: r.get(7)?,
            text: r.get(8)?,
            source_endpoint: r.get(9)?,
            addressed_status: r.get(10)?,
            evidence,
            collector_model: r.get(12)?,
            collector_version: r.get(13)?,
        })
    })?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

/// Collection-run outcome for a single PR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunStatus {
    Success,
    Failed,
}

impl RunStatus {
    fn as_str(&self) -> &'static str {
        match self {
            RunStatus::Success => "success",
            RunStatus::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone)]
pub struct CollectionRun {
    pub pr_number: i64,
    pub task_id: Option<i64>,
    pub status: RunStatus,
    pub error: Option<String>,
    pub collector_model: String,
    /// Launch identity used only to validate a managed assignment link. The
    /// durable assignment row remains the canonical source for these fields.
    pub collector_provider: Option<String>,
    pub collector_runner: Option<String>,
    pub collector_effort: Option<String>,
    pub collector_version: String,
    pub findings_count: i64,
    pub attempted_at: i64,
    pub completed_at: Option<i64>,
    /// Durable routing decision for the collector responsibility, when managed.
    pub role_assignment_id: Option<i64>,
}

/// UPSERT a run record for a PR — the primary-key REPLACE keeps at most one row
/// per PR, so retries overwrite instead of accumulating history rows.
pub fn record_run(conn: &Connection, run: &CollectionRun) -> Result<()> {
    record_run_inner(conn, run)
}

const COLLECTION_RUN_INSERT: &str = "INSERT INTO review_collection_runs(
        pr_number,task_id,status,error,collector_model,collector_version,
        findings_count,attempted_at,completed_at,role_assignment_id)
    SELECT :pr_number,:task_id,:status,:error,:collector_model,:collector_version,
        :findings_count,:attempted_at,:completed_at,:quorum_assignment_id
    /* quorum-role-assignment-guard */
      AND (:quorum_assignment_id IS NULL OR EXISTS(
          SELECT 1 FROM role_assignments AS collector_assignment
          WHERE collector_assignment.id=:quorum_assignment_id
            AND collector_assignment.pr_number=:pr_number
            AND collector_assignment.review_stage IS NULL
            AND collector_assignment.complexity IS NULL
      ))
    ON CONFLICT(pr_number) DO UPDATE SET
        task_id=excluded.task_id,
        status=excluded.status,
        error=excluded.error,
        collector_model=excluded.collector_model,
        collector_version=excluded.collector_version,
        findings_count=excluded.findings_count,
        attempted_at=excluded.attempted_at,
        completed_at=excluded.completed_at,
        role_assignment_id=excluded.role_assignment_id";

fn record_run_inner(conn: &Connection, run: &CollectionRun) -> Result<()> {
    let responsibility = format!("collector:pr:{}", run.pr_number);
    let context = crate::role_assignments::EvidenceAssignmentContext {
        role_assignment_id: run.role_assignment_id,
        task_id: run.task_id,
        responsibility_key: &responsibility,
        role: "collector",
        provider: run.collector_provider.as_deref().unwrap_or_default(),
        runner: run.collector_runner.as_deref().unwrap_or_default(),
        model: &run.collector_model,
        effort: run.collector_effort.as_deref().unwrap_or_default(),
    };
    let status = run.status.as_str();
    crate::role_assignments::guarded_evidence_insert(
        conn,
        "collector run",
        &context,
        COLLECTION_RUN_INSERT,
        &[
            (":pr_number", &run.pr_number),
            (":task_id", &run.task_id),
            (":status", &status),
            (":error", &run.error),
            (":collector_model", &run.collector_model),
            (":collector_version", &run.collector_version),
            (":findings_count", &run.findings_count),
            (":attempted_at", &run.attempted_at),
            (":completed_at", &run.completed_at),
        ],
    )
}

/// Replace a collector's findings and persist its successful run as one
/// canonical evidence write. A mismatched assignment aborts the guarded run
/// insert and rolls the findings replacement back with it.
pub fn replace_for_pr_and_record_run(
    conn: &mut Connection,
    pr_number: i64,
    findings: &[ReviewFinding],
    run: &CollectionRun,
) -> Result<()> {
    if run.pr_number != pr_number {
        return Err(crate::role_assignments::evidence_mismatch("collector PR"));
    }
    let now = clock::now();
    let tx = begin_immediate(conn)?;
    replace_for_pr_inner(&tx, pr_number, findings, now)?;
    record_run_inner(&tx, run)?;
    tx.commit()?;
    Ok(())
}

/// Read the run record for a PR (None if never collected).
pub fn get_run(conn: &Connection, pr_number: i64) -> Result<Option<CollectionRun>> {
    let mut stmt = conn.prepare(
        "SELECT r.pr_number,r.task_id,r.status,r.error,r.collector_model,
                r.collector_version,r.findings_count,r.attempted_at,r.completed_at,
                r.role_assignment_id,a.provider,a.runner,a.effort
         FROM review_collection_runs r
         LEFT JOIN role_assignments a ON a.id=r.role_assignment_id
         WHERE r.pr_number = ?1",
    )?;
    let mut rows = stmt.query(params![pr_number])?;
    if let Some(row) = rows.next()? {
        let status_s: String = row.get(2)?;
        let status = match status_s.as_str() {
            "success" => RunStatus::Success,
            _ => RunStatus::Failed,
        };
        Ok(Some(CollectionRun {
            pr_number: row.get(0)?,
            task_id: row.get(1)?,
            status,
            error: row.get(3)?,
            collector_model: row.get(4)?,
            collector_provider: row.get(10)?,
            collector_runner: row.get(11)?,
            collector_effort: row.get(12)?,
            collector_version: row.get(5)?,
            findings_count: row.get(6)?,
            attempted_at: row.get(7)?,
            completed_at: row.get(8)?,
            role_assignment_id: row.get(9)?,
        }))
    } else {
        Ok(None)
    }
}

/// Deterministic, bounded classifier input assembled by the collector before the
/// model ever runs. All GitHub payloads are raw JSON as returned by `gh api`; the
/// classifier never issues its own HTTP calls.
#[derive(Debug, Clone, Default, Serialize)]
pub struct CollectorInputs {
    pub pr_number: i64,
    pub pr_metadata_json: String,
    pub reviews_json: String,
    pub review_comments_json: String,
    pub issue_comments_json: String,
    /// Deterministic evidence index captured before bounded prompt truncation.
    /// `None` keeps hand-built/legacy inputs compatible with validation that
    /// derives IDs directly from their JSON payloads.
    #[serde(skip)]
    pub fetched_evidence: Option<Vec<EvidenceId>>,
    pub commits_json: String,
    pub checks_summary: String,
    pub diff_stat: String,
    pub task_context: TaskContext,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct TaskContext {
    pub task_id: Option<i64>,
    pub author: Option<String>,
    pub reviewer: Option<String>,
    pub rework_round: i64,
    pub review_only: bool,
    pub agent_runs: Vec<AgentRunSummary>,
    pub verdicts: Vec<VerdictSummary>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AgentRunSummary {
    pub agent: String,
    pub role: String,
    pub sub_role: Option<String>,
    pub model: String,
    pub effort: String,
    pub end_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct VerdictSummary {
    pub reviewer: String,
    pub verdict: String,
    pub blocking_count: i64,
    pub head_sha: String,
    pub created_at: i64,
}

/// Build the Haiku classifier prompt from the assembled inputs. Every extraction
/// requirement (blocking/advisory, addressed/unaddressed, pushbacks with
/// accepted/overridden/unclear, and evidence ids) is stated inline so a single
/// bounded pass yields the full analytics record.
pub fn build_collector_prompt(inputs: &CollectorInputs) -> String {
    let task_json =
        serde_json::to_string_pretty(&inputs.task_context).unwrap_or_else(|_| "{}".to_string());
    format!(
        r#"You are a post-merge review-analytics classifier. This PR is already merged; your
output is retrospective only. You do NOT judge the code, participate in the review, or
change any lifecycle. Every finding you emit must cite concrete GitHub record ids.

## PR #{pr}

### Metadata
{meta}

### Diff stat
{diff}

### Submitted reviews (pulls/{pr}/reviews)
{reviews}

### Inline review comments (pulls/{pr}/comments — includes reply threads via `in_reply_to_id`)
{review_comments}

### Conversation comments (issues/{pr}/comments)
{issue_comments}

### Commits (pulls/{pr}/commits)
{commits}

### CI checks summary
{checks}

### Quorum task context
{task}

## Extract

For every reviewer-raised finding on this PR, emit one entry. Include findings that
the author addressed AND findings that merged unaddressed — both are analytics data.

Fields per finding:

1. `reviewer` — GitHub login of the commenter who raised it.
2. `kind` — `"blocking"` (reviewer required a change: "must", "needs to", "blocker",
   the reviewer's verdict was `REQUEST_CHANGES`, or the quorum verdict listed it as
   blocking) OR `"suggestion"` (non-blocking: "nit", "consider", "optional").
3. `author_pushback` — `true` if the PR author pushed back on this finding
   (disagreed, called it intentional, declined to change, argued the point).
4. `pushback_accepted` — only meaningful when `author_pushback=true`:
   `true` if the reviewer accepted the pushback (dropped the objection, said "fair
   point", approved despite it), `false` if the reviewer maintained/overrode it,
   `null` if the resolution is unclear from the record.
5. `severity` — `"critical" | "major" | "minor" | "nit" | null`.
6. `text` — one-sentence summary of the finding.
7. `source_endpoint` — `"pulls"` (from a review or inline comment) or
   `"issues"` (from a conversation comment).
8. `addressed_status` — one of:
   - `"addressed"` if the author fixed it before merge (later commits fix the code,
     author replies "fixed in <sha>", or the reviewer confirms it resolved),
   - `"unaddressed"` if it merged without a fix and the author did not push back,
   - `"partial"` if some parts were fixed and others were left,
   - `"unclear"` if the record does not let you tell,
   - `"withdrawn"` if the reviewer independently retracted the finding without
     accepting author pushback.
9. `evidence` — array of `{{"kind":"review"|"review_comment"|"issue_comment","id":<int>}}`
   objects. Include at least one; use the GitHub numeric `id` field from the raw
   comment/review JSON above. Threaded replies count as separate evidence rows.

Also emit a `followup_artifacts` entry for each concrete, evidence-backed
non-blocking concern that remains valid after merge. Every artifact must share
at least one evidence row with a `suggestion` finding above. Do not emit an
artifact for a finding that was fixed, withdrawn, or accepted as invalid.

Fields per follow-up artifact:

1. `technical_impact` — `"critical" | "major" | "minor" | "nit"`.
2. `scope_relationship` — `"pre_existing" | "out_of_scope" |
   "threat_model_expansion" | "defense_in_depth" | "future_requirement" |
   "design_debt"`.
3. `concern` — the concrete failure under stated assumptions.
4. `non_blocking_reason` — why the merged PR did not need to resolve it.
5. `affected_behavior` — the affected product behavior.
6. `desired_outcome` — an observable future outcome, not a vague improvement.
7. `verification_expectations` — one through eight concrete checks.
8. `evidence` — one or more unique GitHub evidence rows, all present above.

## Output

Respond with ONLY a JSON object:
```json
{{"findings":[
  {{"reviewer":"...","kind":"blocking","author_pushback":false,"pushback_accepted":null,
   "severity":"major","text":"...","source_endpoint":"pulls",
   "addressed_status":"addressed",
   "evidence":[{{"kind":"review_comment","id":12345}}]}}
],"followup_artifacts":[
  {{"technical_impact":"major","scope_relationship":"out_of_scope",
   "concern":"Concrete failure under the stated assumptions",
   "non_blocking_reason":"Why the merged PR did not need to resolve it",
   "affected_behavior":"Product behavior affected by the concern",
   "desired_outcome":"Observable future outcome",
   "verification_expectations":["Evidence that proves the outcome"],
   "evidence":[{{"kind":"review_comment","id":12345}}]}}
]}}
```

If the PR has no substantive reviewer findings (only bot noise, "LGTM", or merge
notifications), return `{{"findings":[],"followup_artifacts":[]}}`. A PR may have
findings and still have zero valid artifacts. Do NOT invent findings or artifacts.
Do NOT wrap the JSON in prose."#,
        pr = inputs.pr_number,
        meta = inputs.pr_metadata_json,
        diff = inputs.diff_stat,
        reviews = inputs.reviews_json,
        review_comments = inputs.review_comments_json,
        issue_comments = inputs.issue_comments_json,
        commits = inputs.commits_json,
        checks = inputs.checks_summary,
        task = task_json,
    )
}

/// Legacy alias — the pre-v2 prompt still ships for callers that only have the
/// two comment endpoints and no task context. Kept lossy on purpose: the new
/// pipeline should build a full `CollectorInputs`.
pub fn build_prompt(
    pulls_comments_json: &str,
    issues_comments_json: &str,
    pr_number: i64,
) -> String {
    let inputs = CollectorInputs {
        pr_number,
        pr_metadata_json: "{}".into(),
        reviews_json: "[]".into(),
        review_comments_json: pulls_comments_json.to_string(),
        issue_comments_json: issues_comments_json.to_string(),
        fetched_evidence: None,
        commits_json: "[]".into(),
        checks_summary: "unknown".into(),
        diff_stat: "unknown".into(),
        task_context: TaskContext::default(),
    };
    build_collector_prompt(&inputs)
}

/// Parse the classifier response into structured findings, stamping the runtime
/// context (pr, task, model, version). Returns `None` on unparseable output —
/// the caller reports a collection failure rather than swallowing silently.
pub fn parse_response(
    text: &str,
    pr_number: i64,
    task_id: Option<i64>,
) -> Option<Vec<ReviewFinding>> {
    parse_response_with_provenance(text, pr_number, task_id, None, None)
}

/// Same as [`parse_response`] but also stamps `collector_model` / `collector_version`
/// on every emitted finding.
pub fn parse_response_with_provenance(
    text: &str,
    pr_number: i64,
    task_id: Option<i64>,
    collector_model: Option<&str>,
    collector_version: Option<&str>,
) -> Option<Vec<ReviewFinding>> {
    let json_text = extract_json(text)?;
    let mut resp: InterpreterResponse = serde_json::from_str(json_text).ok()?;
    for f in &mut resp.findings {
        f.pr_number = pr_number;
        f.task_id = task_id;
        f.collector_model = collector_model.map(|s| s.to_string());
        f.collector_version = collector_version.map(|s| s.to_string());
    }
    Some(resp.findings)
}

fn extract_json(text: &str) -> Option<&str> {
    let trimmed = text.trim();
    if trimmed.starts_with('{') {
        return Some(trimmed);
    }
    if let Some(start) = trimmed.find("```json") {
        let after = &trimmed[start + 7..];
        if let Some(end) = after.find("```") {
            return Some(after[..end].trim());
        }
    }
    if let Some(start) = trimmed.find("```") {
        let after = &trimmed[start + 3..];
        if let Some(end) = after.find("```") {
            let inner = after[..end].trim();
            if inner.starts_with('{') {
                return Some(inner);
            }
        }
    }
    None
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

    fn mk(kind: &str, source: &str) -> ReviewFinding {
        ReviewFinding {
            id: 0,
            pr_number: 0,
            task_id: None,
            reviewer: "alice".into(),
            kind: kind.into(),
            author_pushback: false,
            pushback_accepted: None,
            severity: None,
            text: "sample".into(),
            source_endpoint: source.into(),
            addressed_status: None,
            evidence: vec![],
            collector_model: None,
            collector_version: None,
        }
    }

    #[test]
    fn replace_and_list_roundtrip() {
        let (mut conn, _dir) = test_conn();
        let findings = vec![
            ReviewFinding {
                addressed_status: Some("addressed".into()),
                evidence: vec![EvidenceId {
                    kind: "review_comment".into(),
                    id: 111,
                }],
                collector_model: Some("haiku".into()),
                collector_version: Some("v1".into()),
                ..mk("blocking", "pulls")
            },
            ReviewFinding {
                author_pushback: true,
                pushback_accepted: Some(true),
                severity: Some("nit".into()),
                addressed_status: Some("unclear".into()),
                evidence: vec![EvidenceId {
                    kind: "issue_comment".into(),
                    id: 222,
                }],
                ..mk("suggestion", "issues")
            },
        ];
        replace_for_pr(&mut conn, 100, &findings).unwrap();
        let got = list_for_pr(&conn, 100).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].kind, "blocking");
        assert_eq!(got[0].addressed_status.as_deref(), Some("addressed"));
        assert_eq!(got[0].evidence.len(), 1);
        assert_eq!(got[0].evidence[0].id, 111);
        assert_eq!(got[0].collector_model.as_deref(), Some("haiku"));
        assert_eq!(got[1].kind, "suggestion");
        assert!(got[1].author_pushback);
        assert_eq!(got[1].pushback_accepted, Some(true));
        assert_eq!(got[1].evidence[0].kind, "issue_comment");
    }

    #[test]
    fn replace_is_idempotent() {
        let (mut conn, _dir) = test_conn();
        let f1 = vec![mk("blocking", "pulls")];
        replace_for_pr(&mut conn, 100, &f1).unwrap();
        assert_eq!(list_for_pr(&conn, 100).unwrap().len(), 1);
        let mut f2 = mk("suggestion", "pulls");
        f2.text = "new".into();
        replace_for_pr(&mut conn, 100, &[f2]).unwrap();
        let got = list_for_pr(&conn, 100).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].text, "new");
    }

    #[test]
    fn parse_response_full_schema() {
        let response = r#"{"findings":[
            {"reviewer":"r1","kind":"blocking","author_pushback":false,"pushback_accepted":null,
             "severity":"major","text":"Missing error handling","source_endpoint":"pulls",
             "addressed_status":"addressed",
             "evidence":[{"kind":"review_comment","id":10}]},
            {"reviewer":"r1","kind":"suggestion","author_pushback":false,"pushback_accepted":null,
             "severity":"nit","text":"Rename var","source_endpoint":"pulls",
             "addressed_status":"unaddressed",
             "evidence":[{"kind":"review_comment","id":11}]},
            {"reviewer":"r1","kind":"blocking","author_pushback":true,"pushback_accepted":true,
             "severity":"minor","text":"Prefer helper","source_endpoint":"issues",
             "addressed_status":"unaddressed",
             "evidence":[{"kind":"issue_comment","id":12}]},
            {"reviewer":"r1","kind":"blocking","author_pushback":true,"pushback_accepted":false,
             "severity":"critical","text":"Race in claim path","source_endpoint":"issues",
             "addressed_status":"addressed",
             "evidence":[{"kind":"issue_comment","id":13},{"kind":"review","id":99}]}
        ]}"#;
        let findings =
            parse_response_with_provenance(response, 200, Some(50), Some("haiku"), Some("v1"))
                .unwrap();
        assert_eq!(findings.len(), 4);

        // Blocking, addressed
        assert_eq!(findings[0].kind, "blocking");
        assert_eq!(findings[0].addressed_status.as_deref(), Some("addressed"));
        assert_eq!(findings[0].evidence[0].kind, "review_comment");
        assert_eq!(findings[0].evidence[0].id, 10);
        assert_eq!(findings[0].collector_model.as_deref(), Some("haiku"));
        assert_eq!(findings[0].collector_version.as_deref(), Some("v1"));

        // Suggestion, unaddressed (advisory left behind)
        assert_eq!(findings[1].kind, "suggestion");
        assert_eq!(findings[1].addressed_status.as_deref(), Some("unaddressed"));

        // Pushback accepted: reviewer dropped objection
        assert!(findings[2].author_pushback);
        assert_eq!(findings[2].pushback_accepted, Some(true));

        // Pushback overridden: reviewer insisted
        assert!(findings[3].author_pushback);
        assert_eq!(findings[3].pushback_accepted, Some(false));
        assert_eq!(findings[3].evidence.len(), 2);

        for f in &findings {
            assert_eq!(f.pr_number, 200);
            assert_eq!(f.task_id, Some(50));
        }
    }

    #[test]
    fn parse_response_with_fences() {
        let response = "prelude\n```json\n{\"findings\":[{\"reviewer\":\"a\",\"kind\":\"suggestion\",\"text\":\"nit\",\"source_endpoint\":\"pulls\"}]}\n```";
        let findings = parse_response(response, 1, None).unwrap();
        assert_eq!(findings.len(), 1);
    }

    #[test]
    fn parse_response_empty() {
        let findings = parse_response(r#"{"findings":[]}"#, 1, None).unwrap();
        assert!(findings.is_empty());
    }

    #[test]
    fn parse_response_invalid() {
        assert!(parse_response("not json", 1, None).is_none());
    }

    #[test]
    fn parse_response_backfills_absent_addressed_and_evidence() {
        // Pre-v24 shape (no addressed_status, no evidence) must still parse so
        // manual runs of an older prompt don't hard-fail — fields default to
        // None / empty vec.
        let response = r#"{"findings":[
            {"reviewer":"r","kind":"blocking","text":"x","source_endpoint":"pulls"}
        ]}"#;
        let findings = parse_response(response, 1, None).unwrap();
        assert_eq!(findings.len(), 1);
        assert!(findings[0].addressed_status.is_none());
        assert!(findings[0].evidence.is_empty());
    }

    #[test]
    fn record_and_get_run_success() {
        let (conn, _dir) = test_conn();
        let now = clock::now();
        conn.execute(
            "INSERT INTO role_assignments(
                 id,responsibility_key,task_id,pr_number,role,profile_id,provider,runner,model,effort,
                 pool_key,policy_generation,created_at)
             VALUES (77,'collector:pr:500',80,500,'collector','collector-profile','claude',
                     'claude','haiku','high','collector','g1',?1)",
            [now],
        )
        .unwrap();
        let run = CollectionRun {
            pr_number: 500,
            task_id: Some(80),
            status: RunStatus::Success,
            error: None,
            collector_model: "haiku".into(),
            collector_provider: Some("claude".into()),
            collector_runner: Some("claude".into()),
            collector_effort: Some("high".into()),
            collector_version: "v1".into(),
            findings_count: 3,
            attempted_at: now,
            completed_at: Some(now + 1),
            role_assignment_id: Some(77),
        };
        record_run(&conn, &run).unwrap();
        let got = get_run(&conn, 500).unwrap().unwrap();
        assert_eq!(got.status, RunStatus::Success);
        assert_eq!(got.findings_count, 3);
        assert_eq!(got.error, None);
        assert_eq!(got.role_assignment_id, Some(77));
        assert_eq!(got.collector_provider.as_deref(), Some("claude"));
        assert_eq!(got.collector_runner.as_deref(), Some("claude"));
        assert_eq!(got.collector_effort.as_deref(), Some("high"));
    }

    #[test]
    fn mismatched_collector_assignment_rolls_back_findings_and_run_atomically() {
        let (mut conn, _dir) = test_conn();
        conn.execute(
            "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at)
             VALUES (90,'merged','done','owner',1,1)",
            [],
        )
        .unwrap();
        let old = ReviewFinding {
            pr_number: 700,
            task_id: Some(90),
            text: "old finding".into(),
            ..mk("suggestion", "pulls")
        };
        replace_for_pr(&mut conn, 700, &[old]).unwrap();
        let old_run = CollectionRun {
            pr_number: 700,
            task_id: Some(90),
            status: RunStatus::Failed,
            error: Some("old failure".into()),
            collector_model: "haiku".into(),
            collector_provider: None,
            collector_runner: None,
            collector_effort: None,
            collector_version: "v1".into(),
            findings_count: 0,
            attempted_at: 1,
            completed_at: Some(2),
            role_assignment_id: None,
        };
        record_run(&conn, &old_run).unwrap();
        conn.execute(
            "INSERT INTO role_assignments(
                 id,responsibility_key,task_id,pr_number,role,profile_id,provider,runner,model,effort,
                 pool_key,policy_generation,created_at)
             VALUES (88,'collector:pr:700',90,700,'collector','profile','codex','claude',
                     'haiku','high','collector','g1',1)",
            [],
        )
        .unwrap();
        let replacement = ReviewFinding {
            pr_number: 700,
            task_id: Some(90),
            text: "replacement".into(),
            ..mk("blocking", "pulls")
        };
        let routed_run = CollectionRun {
            status: RunStatus::Success,
            error: None,
            findings_count: 1,
            attempted_at: 3,
            completed_at: Some(4),
            role_assignment_id: Some(88),
            collector_provider: Some("codex".into()),
            collector_runner: Some("codex".into()),
            collector_effort: Some("high".into()),
            ..old_run
        };

        assert!(
            replace_for_pr_and_record_run(&mut conn, 700, &[replacement], &routed_run).is_err()
        );
        let findings = list_for_pr(&conn, 700).unwrap();
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].text, "old finding");
        let stored = get_run(&conn, 700).unwrap().unwrap();
        assert_eq!(stored.status, RunStatus::Failed);
        assert_eq!(stored.error.as_deref(), Some("old failure"));
        assert_eq!(stored.role_assignment_id, None);
        let task_status: String = conn
            .query_row("SELECT status FROM tasks WHERE id=90", [], |row| row.get(0))
            .unwrap();
        assert_eq!(task_status, "done");
    }

    #[test]
    fn record_run_replaces_on_retry() {
        let (conn, _dir) = test_conn();
        let now = clock::now();
        let failed = CollectionRun {
            pr_number: 600,
            task_id: Some(90),
            status: RunStatus::Failed,
            error: Some("gh api rate-limited".into()),
            collector_model: "haiku".into(),
            collector_provider: None,
            collector_runner: None,
            collector_effort: None,
            collector_version: "v1".into(),
            findings_count: 0,
            attempted_at: now,
            completed_at: None,
            role_assignment_id: None,
        };
        record_run(&conn, &failed).unwrap();
        let got = get_run(&conn, 600).unwrap().unwrap();
        assert_eq!(got.status, RunStatus::Failed);
        assert_eq!(got.error.as_deref(), Some("gh api rate-limited"));

        let success = CollectionRun {
            findings_count: 2,
            status: RunStatus::Success,
            error: None,
            completed_at: Some(now + 5),
            attempted_at: now + 4,
            ..failed.clone()
        };
        record_run(&conn, &success).unwrap();
        let got = get_run(&conn, 600).unwrap().unwrap();
        assert_eq!(got.status, RunStatus::Success);
        assert_eq!(got.findings_count, 2);
        assert_eq!(got.error, None);
        assert_eq!(got.completed_at, Some(now + 5));

        // Only one row per PR — verify via count.
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM review_collection_runs WHERE pr_number=?1",
                params![600i64],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn build_collector_prompt_includes_all_sections() {
        let inputs = CollectorInputs {
            pr_number: 42,
            pr_metadata_json: r#"{"title":"hello"}"#.into(),
            reviews_json: r#"[{"id":1,"state":"APPROVED"}]"#.into(),
            review_comments_json: r#"[{"id":10,"in_reply_to_id":null}]"#.into(),
            issue_comments_json: r#"[{"id":20,"body":"nit"}]"#.into(),
            fetched_evidence: None,
            commits_json: r#"[{"sha":"deadbeef"}]"#.into(),
            checks_summary: "all-passed".into(),
            diff_stat: "3 files, 20 additions".into(),
            task_context: TaskContext {
                task_id: Some(7),
                author: Some("Alpha".into()),
                reviewer: Some("Bravo".into()),
                rework_round: 1,
                review_only: false,
                agent_runs: vec![AgentRunSummary {
                    agent: "Alpha".into(),
                    role: "worker".into(),
                    sub_role: None,
                    model: "opus".into(),
                    effort: "high".into(),
                    end_reason: Some("done".into()),
                }],
                verdicts: vec![VerdictSummary {
                    reviewer: "Bravo".into(),
                    verdict: "approved".into(),
                    blocking_count: 0,
                    head_sha: "abc123".into(),
                    created_at: 999,
                }],
            },
        };
        let prompt = build_collector_prompt(&inputs);
        // Every section title present — the prompt is the contract with the model.
        assert!(prompt.contains("PR #42"));
        assert!(prompt.contains("Submitted reviews"));
        assert!(prompt.contains("Inline review comments"));
        assert!(prompt.contains("in_reply_to_id"));
        assert!(prompt.contains("Conversation comments"));
        assert!(prompt.contains("Commits"));
        assert!(prompt.contains("CI checks summary"));
        assert!(prompt.contains("Quorum task context"));
        assert!(prompt.contains("all-passed"));
        assert!(prompt.contains("deadbeef"));
        assert!(prompt.contains("Alpha"));
        assert!(prompt.contains("addressed_status"));
        assert!(prompt.contains("withdrawn"));
        assert!(prompt.contains("evidence"));
        assert!(prompt.contains("followup_artifacts"));
        assert!(prompt.contains("technical_impact"));
        assert!(prompt.contains("scope_relationship"));
    }
}
