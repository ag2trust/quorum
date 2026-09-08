//! Performance report queries for `quorum perf`. Read-only — no writes, no mutations.
//!
//! Computes aggregate metrics from the `tasks` table (terminal tasks only: done, failed,
//! cancelled). Model/effort resolved from `agent_runs` (earliest worker spawn per task),
//! falling back to caller-supplied defaults for orphan tasks. Complexity derived from
//! `complexity:*` labels. The separate facts surface below deliberately has a
//! stricter, delivery-evidence-based inclusion policy; it does not affect the
//! legacy aggregate report.

use crate::db::map_sql_err;
use crate::error::Result;
use rusqlite::OptionalExtension;
use rusqlite::{params_from_iter, Connection};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PerfCut {
    Default,
    Complexity,
    Reviewer,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct PerfReport {
    pub rows: Vec<PerfRow>,
}

#[derive(Debug, Serialize, PartialEq)]
pub struct PerfRow {
    pub model: String,
    pub effort: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub complexity: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reviewer: Option<String>,
    pub n_tasks: i64,
    pub first_pass_pct: f64,
    pub avg_rework: f64,
    pub fail_pct: f64,
    pub median_wall_mins: f64,
    pub avg_reviewer_secs: f64,
    pub rubber_stamp_count: i64,
    pub approve_rate_pct: f64,
    pub avg_blocking: f64,
    pub total_cost_usd: f64,
}

/// Read the prospective-only boundary from the `perf_watermark` table.
/// Returns `None` on pre-v27 databases (table absent or empty).
pub fn read_watermark(conn: &Connection) -> Result<Option<i64>> {
    let val: Option<i64> = conn
        .query_row(
            "SELECT watermark FROM perf_watermark WHERE id = 1",
            [],
            |r| r.get(0),
        )
        .optional()?;
    Ok(val)
}

fn extract_label_value(labels_json: Option<&str>, prefix: &str) -> Option<String> {
    let s = labels_json?;
    let v: serde_json::Value = serde_json::from_str(s).ok()?;
    let arr = v.as_array()?;
    for item in arr {
        if let Some(t) = item.as_str() {
            if let Some(rest) = t.strip_prefix(prefix) {
                if !rest.is_empty() {
                    return Some(rest.to_string());
                }
            }
        }
    }
    None
}

fn extract_complexity(labels_json: Option<&str>) -> String {
    extract_label_value(labels_json, "complexity:").unwrap_or_else(|| "untagged".to_string())
}

struct TaskRow {
    id: i64,
    status: String,
    labels: Option<String>,
    model: String,
    effort: String,
    rework_round: i64,
    created_at: i64,
    updated_at: i64,
    reviewer: Option<String>,
}

fn load_terminal_tasks(
    conn: &Connection,
    default_model: &str,
    default_effort: &str,
    since: Option<i64>,
) -> Result<Vec<TaskRow>> {
    let since_val = since.unwrap_or(0);
    let mut stmt = conn.prepare(
        "SELECT t.id, t.status, t.labels, t.rework_round, t.created_at, t.updated_at, t.reviewer, \
                COALESCE(ar.model, ?1), COALESCE(ar.effort, ?2) \
         FROM tasks t \
         LEFT JOIN ( \
             SELECT task_id, model, effort, \
                    ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY spawned_at ASC) AS rn \
             FROM agent_runs WHERE role = 'worker' \
         ) ar ON ar.task_id = t.id AND ar.rn = 1 \
         WHERE t.status IN ('done', 'failed', 'cancelled') \
           AND t.updated_at >= ?3",
    )?;
    let rows = stmt
        .query_map(
            rusqlite::params![default_model, default_effort, since_val],
            |r| {
                Ok(TaskRow {
                    id: r.get(0)?,
                    status: r.get(1)?,
                    labels: r.get(2)?,
                    rework_round: r.get(3)?,
                    created_at: r.get(4)?,
                    updated_at: r.get(5)?,
                    reviewer: r.get(6)?,
                    model: r.get(7)?,
                    effort: r.get(8)?,
                })
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn load_reviewer_durations(conn: &Connection) -> Result<HashMap<i64, Vec<i64>>> {
    let mut stmt = conn.prepare(
        "SELECT task_id, ended_at - spawned_at \
         FROM agent_runs \
         WHERE role = 'reviewer' AND ended_at IS NOT NULL AND sub_role IS NULL",
    )?;
    let mut map: HashMap<i64, Vec<i64>> = HashMap::new();
    let rows = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))?;
    for row in rows {
        let (task_id, duration) = row?;
        map.entry(task_id).or_default().push(duration);
    }
    Ok(map)
}

fn load_approval_stats(conn: &Connection) -> Result<HashMap<i64, (String, i64)>> {
    let mut stmt = conn.prepare("SELECT task_id, verdict, blocking_count FROM approvals")?;
    let mut map = HashMap::new();
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, i64>(2)?,
        ))
    })?;
    for row in rows {
        let (task_id, verdict, blocking) = row?;
        map.insert(task_id, (verdict, blocking));
    }
    Ok(map)
}

fn load_cost_by_task(conn: &Connection) -> Result<HashMap<i64, f64>> {
    let mut stmt = conn.prepare(
        "SELECT task_id, SUM(cost_usd) FROM journal WHERE task_id IS NOT NULL GROUP BY task_id",
    )?;
    let mut map = HashMap::new();
    let rows = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, f64>(1)?)))?;
    for row in rows {
        let (task_id, cost) = row?;
        map.insert(task_id, cost);
    }
    Ok(map)
}

struct AuxData {
    reviewer_durations: HashMap<i64, Vec<i64>>,
    approvals: HashMap<i64, (String, i64)>,
    costs: HashMap<i64, f64>,
}

fn load_aux_data(conn: &Connection) -> Result<AuxData> {
    Ok(AuxData {
        reviewer_durations: load_reviewer_durations(conn)?,
        approvals: load_approval_stats(conn)?,
        costs: load_cost_by_task(conn)?,
    })
}

fn median(sorted: &[f64]) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let n = sorted.len();
    if n.is_multiple_of(2) {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    } else {
        sorted[n / 2]
    }
}

type GroupKey = (String, String, Option<String>, Option<String>);

struct GroupAccum {
    total: i64,
    first_pass: i64,
    rework_sum: i64,
    failed: i64,
    wall_mins: Vec<f64>,
    reviewer_durations: Vec<i64>,
    n_approved: i64,
    n_with_approval: i64,
    blocking_sum: i64,
    cost_usd_sum: f64,
}

impl GroupAccum {
    fn new() -> Self {
        Self {
            total: 0,
            first_pass: 0,
            rework_sum: 0,
            failed: 0,
            wall_mins: Vec::new(),
            reviewer_durations: Vec::new(),
            n_approved: 0,
            n_with_approval: 0,
            blocking_sum: 0,
            cost_usd_sum: 0.0,
        }
    }

    fn add(&mut self, task: &TaskRow, aux: &AuxData) {
        self.total += 1;
        if task.status == "done" && task.rework_round == 0 {
            self.first_pass += 1;
        }
        self.rework_sum += task.rework_round;
        if task.status == "failed" || task.status == "cancelled" {
            self.failed += 1;
        }
        let wall_secs = (task.updated_at - task.created_at).max(0) as f64;
        self.wall_mins.push(wall_secs / 60.0);

        if let Some(durs) = aux.reviewer_durations.get(&task.id) {
            self.reviewer_durations.extend(durs);
        }
        if let Some((verdict, blocking)) = aux.approvals.get(&task.id) {
            self.n_with_approval += 1;
            if verdict == "approved" {
                self.n_approved += 1;
            }
            self.blocking_sum += blocking;
        }
        if let Some(&cost) = aux.costs.get(&task.id) {
            self.cost_usd_sum += cost;
        }
    }

    fn into_row(
        mut self,
        model: String,
        effort: String,
        complexity: Option<String>,
        reviewer: Option<String>,
    ) -> PerfRow {
        self.wall_mins
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let n = self.total as f64;
        let n_reviews = self.reviewer_durations.len() as f64;
        let n_appr = self.n_with_approval as f64;
        PerfRow {
            model,
            effort,
            complexity,
            reviewer,
            n_tasks: self.total,
            first_pass_pct: if n > 0.0 {
                (self.first_pass as f64 / n) * 100.0
            } else {
                0.0
            },
            avg_rework: if n > 0.0 {
                self.rework_sum as f64 / n
            } else {
                0.0
            },
            fail_pct: if n > 0.0 {
                (self.failed as f64 / n) * 100.0
            } else {
                0.0
            },
            median_wall_mins: median(&self.wall_mins),
            avg_reviewer_secs: if n_reviews > 0.0 {
                self.reviewer_durations.iter().sum::<i64>() as f64 / n_reviews
            } else {
                0.0
            },
            rubber_stamp_count: self.reviewer_durations.iter().filter(|&&d| d < 120).count() as i64,
            approve_rate_pct: if n_appr > 0.0 {
                (self.n_approved as f64 / n_appr) * 100.0
            } else {
                0.0
            },
            avg_blocking: if n_appr > 0.0 {
                self.blocking_sum as f64 / n_appr
            } else {
                0.0
            },
            total_cost_usd: self.cost_usd_sum,
        }
    }
}

pub fn perf(
    conn: &Connection,
    cut: PerfCut,
    default_model: &str,
    default_effort: &str,
) -> Result<PerfReport> {
    perf_with(conn, cut, default_model, default_effort, false)
}

pub fn perf_with(
    conn: &Connection,
    cut: PerfCut,
    default_model: &str,
    default_effort: &str,
    include_all: bool,
) -> Result<PerfReport> {
    let since = if include_all {
        None
    } else {
        read_watermark(conn)?
    };
    let tasks = load_terminal_tasks(conn, default_model, default_effort, since)?;
    if tasks.is_empty() {
        return Ok(PerfReport { rows: vec![] });
    }

    let aux = load_aux_data(conn)?;
    let mut groups: BTreeMap<GroupKey, GroupAccum> = BTreeMap::new();

    for task in &tasks {
        let (model, effort, complexity, reviewer_col) = match cut {
            PerfCut::Default => (task.model.clone(), task.effort.clone(), None, None),
            PerfCut::Complexity => {
                let cx = extract_complexity(task.labels.as_deref());
                (task.model.clone(), task.effort.clone(), Some(cx), None)
            }
            PerfCut::Reviewer => {
                let rev = task.reviewer.clone().unwrap_or_else(|| "none".to_string());
                (task.model.clone(), task.effort.clone(), None, Some(rev))
            }
        };

        let key = (model, effort, complexity, reviewer_col);
        groups
            .entry(key)
            .or_insert_with(GroupAccum::new)
            .add(task, &aux);
    }

    let rows = groups
        .into_iter()
        .map(|((model, effort, cx, rev), acc)| acc.into_row(model, effort, cx, rev))
        .collect();

    Ok(PerfReport { rows })
}

// ── facts report scaffold (perf-facts-v1) ──────────────────────────────────
//
// Read-only fact surface. Types declare every fact field up front. A JSON null
// with a false coverage flag is distinguishable from a measured zero or from a
// fact that was deliberately found to be unavailable.

pub const FACTS_VERSION: &str = "perf-facts-v1";

/// Cap on returned intents. Additional candidates become excluded-truncated.
pub const MAX_INTENTS: usize = 10_000;

/// Cap on contributing task ids per intent — enrichment/lineage siblings clip
/// their evidence to this bound.
pub const MAX_CONTRIBUTING_TASKS_PER_INTENT: usize = 64;

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct FactsReport {
    pub facts_version: &'static str,
    pub cohort: CohortDefinition,
    pub query_limits: QueryLimits,
    pub counts: CohortCounts,
    pub coverage: CoverageSummary,
    pub excluded_reasons: BTreeMap<String, i64>,
    pub intents: Vec<IntentFacts>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct CohortDefinition {
    pub prospective_only: bool,
    pub watermark: Option<i64>,
    pub include_all: bool,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct QueryLimits {
    pub max_intents: usize,
    pub max_contributing_tasks_per_intent: usize,
}

#[derive(Debug, Serialize, PartialEq, Eq, Default)]
pub struct CohortCounts {
    pub candidate: i64,
    pub included: i64,
    pub excluded: i64,
}

/// Per-field covered/uncovered tally across the returned intents. Keyed by the
/// same names `IntentCoverage` exposes, ordered for deterministic output.
#[derive(Debug, Serialize, PartialEq, Eq, Default)]
pub struct CoverageSummary {
    pub fields: BTreeMap<String, FieldCoverage>,
}

#[derive(Debug, Serialize, PartialEq, Eq, Default)]
pub struct FieldCoverage {
    pub covered: i64,
    pub uncovered: i64,
}

/// One row per managed intent. Inclusion is resolved exclusively from durable
/// task, graph, and merge evidence; excluded candidates remain visible with a
/// bounded reason code rather than disappearing from the report.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct IntentFacts {
    pub intent_id: String,
    pub contributing_task_ids: Vec<i64>,
    pub included: bool,
    pub reason: InclusionReason,

    // ── evidence fields (populated by sibling tasks) ─────────────────────
    pub lineage_root_task_id: Option<i64>,
    pub lineage_evidence: Option<serde_json::Value>,
    pub terminal_outcome: Option<String>,
    pub terminal_evidence: Option<serde_json::Value>,
    pub merge_provenance: Option<String>,
    pub complexity: Option<String>,
    pub complexity_provenance: Option<String>,
    pub config_evidence: Option<serde_json::Value>,
    pub final_worker: Option<String>,
    pub contributing_attempts: Option<serde_json::Value>,
    pub role_tokens_usd: Option<serde_json::Value>,
    pub active_model_secs: Option<i64>,
    pub wall_secs: Option<i64>,
    pub rework_count: Option<i64>,
    pub recovery_count: Option<i64>,
    pub replan_count: Option<i64>,
    pub incident_count: Option<i64>,
    pub review_quality: Option<serde_json::Value>,

    pub coverage: IntentCoverage,
}

/// Per-field coverage flags. Same names as `IntentFacts` evidence fields so a
/// generic covered/uncovered summary can iterate without reflection.
#[derive(Debug, Serialize, PartialEq, Eq, Default, Clone, Copy)]
pub struct IntentCoverage {
    pub lineage: bool,
    pub terminal: bool,
    pub merge_provenance: bool,
    pub complexity: bool,
    pub config: bool,
    pub final_worker: bool,
    pub contributing_attempts: bool,
    pub role_tokens_usd: bool,
    pub active_model_secs: bool,
    pub wall_secs: bool,
    pub rework: bool,
    pub recovery: bool,
    pub replan: bool,
    pub incident: bool,
    pub review_quality: bool,
}

impl IntentCoverage {
    fn iter_named(&self) -> [(&'static str, bool); 15] {
        [
            ("lineage", self.lineage),
            ("terminal", self.terminal),
            ("merge_provenance", self.merge_provenance),
            ("complexity", self.complexity),
            ("config", self.config),
            ("final_worker", self.final_worker),
            ("contributing_attempts", self.contributing_attempts),
            ("role_tokens_usd", self.role_tokens_usd),
            ("active_model_secs", self.active_model_secs),
            ("wall_secs", self.wall_secs),
            ("rework", self.rework),
            ("recovery", self.recovery),
            ("replan", self.replan),
            ("incident", self.incident),
            ("review_quality", self.review_quality),
        ]
    }
}

/// Bounded inclusion/exclusion reason codes. No free-form-only eligibility.
#[derive(Debug, Serialize, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "kebab-case")]
pub enum InclusionReason {
    IncludedVerifiedMerge,
    IncludedIrrecoverableFailure,
    ExcludedDuplicate,
    ExcludedIntakeDeclined,
    ExcludedHousekeepingCancellation,
    ExcludedReviewOnly,
    ExcludedManualCompletion,
    ExcludedUnknownDelivery,
    ExcludedPreWatermark,
    ExcludedTruncated,
    ExcludedNonTerminal,
}

impl InclusionReason {
    pub fn is_included(&self) -> bool {
        matches!(
            self,
            Self::IncludedVerifiedMerge | Self::IncludedIrrecoverableFailure
        )
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::IncludedVerifiedMerge => "included-verified-merge",
            Self::IncludedIrrecoverableFailure => "included-irrecoverable-failure",
            Self::ExcludedDuplicate => "excluded-duplicate",
            Self::ExcludedIntakeDeclined => "excluded-intake-declined",
            Self::ExcludedHousekeepingCancellation => "excluded-housekeeping-cancellation",
            Self::ExcludedReviewOnly => "excluded-review-only",
            Self::ExcludedManualCompletion => "excluded-manual-completion",
            Self::ExcludedUnknownDelivery => "excluded-unknown-delivery",
            Self::ExcludedPreWatermark => "excluded-pre-watermark",
            Self::ExcludedTruncated => "excluded-truncated",
            Self::ExcludedNonTerminal => "excluded-non-terminal",
        }
    }
}

/// The durable fields used to resolve one facts candidate. Raw refs never
/// leave this reader; they are inspected only through fixed, fail-closed
/// predicates below.
#[derive(Debug, Clone)]
struct FactsTaskRow {
    id: i64,
    status: String,
    review_only: bool,
    completion_provenance: Option<String>,
    refs: Option<String>,
}

/// Load bounded facts candidates in deterministic order. Unlike the legacy
/// aggregate, facts must make nonterminal and review-only exclusions visible,
/// so this intentionally reads every task status.
fn load_facts_candidate_tasks(
    conn: &Connection,
    since: Option<i64>,
    limit: usize,
) -> Result<Vec<FactsTaskRow>> {
    let since_val = since.unwrap_or(0);
    let mut stmt = conn.prepare(
        "SELECT id,status,review_only,completion_provenance,refs \
         FROM tasks \
         WHERE updated_at >= ?1 \
         ORDER BY id ASC \
         LIMIT ?2",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![since_val, limit as i64], |r| {
            Ok(FactsTaskRow {
                id: r.get(0)?,
                status: r.get(1)?,
                review_only: r.get::<_, i64>(2)? != 0,
                completion_provenance: r.get(3)?,
                refs: r.get(4)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Exact raw candidate count. The returned rows are capped separately; any
/// overflow is explicitly reported as `excluded-truncated`.
fn count_candidates(conn: &Connection, since: Option<i64>) -> Result<i64> {
    let since_val = since.unwrap_or(0);
    let count = conn.query_row(
        "SELECT COUNT(*) FROM tasks WHERE updated_at >= ?1",
        rusqlite::params![since_val],
        |r| r.get(0),
    )?;
    Ok(count)
}

fn new_intent_facts(
    root_task_id: i64,
    contributing_task_ids: Vec<i64>,
    reason: InclusionReason,
) -> IntentFacts {
    IntentFacts {
        intent_id: format!("intent-{root_task_id}"),
        contributing_task_ids,
        included: reason.is_included(),
        reason,
        lineage_root_task_id: None,
        lineage_evidence: None,
        terminal_outcome: None,
        terminal_evidence: None,
        merge_provenance: None,
        complexity: None,
        complexity_provenance: None,
        config_evidence: None,
        final_worker: None,
        contributing_attempts: None,
        role_tokens_usd: None,
        active_model_secs: None,
        wall_secs: None,
        rework_count: None,
        recovery_count: None,
        replan_count: None,
        incident_count: None,
        review_quality: None,
        coverage: IntentCoverage::default(),
    }
}

#[derive(Debug)]
struct ResolvedIntent {
    reason: InclusionReason,
    terminal_outcome: Option<String>,
    terminal_evidence: Option<serde_json::Value>,
    merge_provenance: Option<String>,
}

impl ResolvedIntent {
    fn terminal(reason: InclusionReason, outcome: &str, rows: &[&FactsTaskRow]) -> Self {
        Self {
            reason,
            terminal_outcome: Some(outcome.to_string()),
            terminal_evidence: Some(terminal_evidence(rows, &[])),
            merge_provenance: None,
        }
    }

    fn nonterminal() -> Self {
        Self::nonterminal_with_reason(InclusionReason::ExcludedNonTerminal)
    }

    fn nonterminal_with_reason(reason: InclusionReason) -> Self {
        Self {
            reason,
            terminal_outcome: None,
            terminal_evidence: None,
            merge_provenance: None,
        }
    }
}

/// Extract a refs object only when it is a well-formed JSON object. A malformed
/// or scalar retained value is unknown evidence, never a basis for a positive
/// classification.
fn refs_object(row: &FactsTaskRow) -> Option<serde_json::Map<String, serde_json::Value>> {
    serde_json::from_str::<serde_json::Value>(row.refs.as_deref()?)
        .ok()?
        .as_object()
        .cloned()
}

/// A daemon merge identifier is intentionally not reader-validated as a
/// hexadecimal Git SHA: the lifecycle writer accepts any non-empty, NUL-free
/// immutable identifier for enterprise and test providers too.
fn valid_merge_identifier(identifier: &str) -> bool {
    !identifier.is_empty() && !identifier.contains('\0')
}

fn valid_merge_commit_sha(row: &FactsTaskRow) -> Option<String> {
    if row.status != "done" || row.completion_provenance.as_deref() != Some("merged") {
        return None;
    }
    let value = refs_object(row)?;
    let sha = value.get("merge_commit_sha")?.as_str()?;
    // Match the daemon writer's persisted contract: GitHub normally supplies
    // a hexadecimal SHA, but the lifecycle primitive intentionally accepts
    // any non-empty, NUL-free immutable merge identifier (including test and
    // enterprise-provider identifiers). Do not invent a stricter reader-only
    // format that would discard a valid daemon completion.
    valid_merge_identifier(sha).then(|| sha.to_string())
}

fn has_duplicate_cancellation_evidence(row: &FactsTaskRow) -> bool {
    row.status == "cancelled"
        && refs_object(row)
            .and_then(|refs| refs.get("cx_dup_of").cloned())
            .and_then(|value| value.as_array().cloned())
            .is_some_and(|ids| {
                !ids.is_empty() && ids.iter().all(|id| id.as_i64().is_some_and(|id| id > 0))
            })
}

fn has_intake_decline_cancellation_evidence(row: &FactsTaskRow) -> bool {
    if row.status != "cancelled" {
        return false;
    }
    let Some(refs) = refs_object(row) else {
        return false;
    };
    refs.get("cx_ready").and_then(serde_json::Value::as_bool) == Some(false)
        && refs
            .get("cx_not_ready_reason")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|reason| !reason.trim().is_empty() && !reason.contains('\0'))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParkDisposition {
    NotParked,
    Retryable,
    DeliveryUnknown,
    Malformed,
}

/// This mirrors the durable portion of `task-retry` admission without making
/// a lifecycle decision. A parked row with a supported resume target remains
/// active work for facts purposes; a merged-PR park is not credited because it
/// lacks daemon-owned merged completion and a merge commit witness.
fn parked_disposition(row: &FactsTaskRow) -> ParkDisposition {
    if row.status != "failed" {
        return ParkDisposition::NotParked;
    }
    let Some(refs) = refs_object(row) else {
        return ParkDisposition::NotParked;
    };
    // `task-retry` uses SQLite `json_extract(...)=1`, which admits the
    // retained JSON numeric representation as well as JSON true. Mirror that
    // exact lifecycle-compatible truth set; a string such as "1" remains
    // malformed evidence and cannot grant a continuation here.
    let parked = refs
        .get("daemon_parked")
        .is_some_and(|value| value.as_bool() == Some(true) || value.as_f64() == Some(1.0));
    if !parked {
        return ParkDisposition::NotParked;
    }
    if refs
        .get("daemon_publication_failure_kind")
        .and_then(serde_json::Value::as_str)
        == Some("pr-merged")
    {
        return ParkDisposition::DeliveryUnknown;
    }
    match refs
        .get("daemon_resume_status")
        .and_then(serde_json::Value::as_str)
    {
        Some("open" | "working" | "rework" | "in-review" | "merging") => ParkDisposition::Retryable,
        _ => ParkDisposition::Malformed,
    }
}

fn has_requested_continuation(row: &FactsTaskRow) -> bool {
    let Some(refs) = refs_object(row) else {
        return false;
    };
    // `runner_retry` is canonical when present; its neutral requested=false
    // state must suppress stale legacy Codex bits. Reuse the daemon's shared
    // compatibility predicate rather than OR-ing the two representations.
    crate::runner_state::retry_requested(&serde_json::Value::Object(refs.clone()))
        // Both daemon remediation markers are admitted only from `rework`.
        // In particular, `rework_approved_merge` can retain its marker after
        // the lifecycle turns a merge conflict at the rework cap into
        // `failed`; that stale retained fact is not an available recovery.
        || (row.status == "rework"
            && (refs
                .get("daemon_rework_retry_requested")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
                || refs
                    .get("ci_remediation_requested")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true)))
}

/// A cancelled dependency makes every parked retry/continuation path
/// unsatisfiable. Reuse the lifecycle's exact guard under the facts snapshot
/// rather than approximating its JSON dependency traversal here.
fn load_unsatisfiable_parked_task_ids(
    conn: &Connection,
    candidate_tasks: &[FactsTaskRow],
) -> Result<HashSet<i64>> {
    let mut unsatisfiable = HashSet::new();
    for row in candidate_tasks {
        if parked_disposition(row) == ParkDisposition::Retryable
            && !crate::tasks::cancelled_dep_ids(conn, row.id)?.is_empty()
        {
            unsatisfiable.insert(row.id);
        }
    }
    Ok(unsatisfiable)
}

fn has_supported_recovery(
    row: &FactsTaskRow,
    recoverable_graph_task_ids: &HashSet<i64>,
    unsatisfiable_parked_task_ids: &HashSet<i64>,
) -> bool {
    !unsatisfiable_parked_task_ids.contains(&row.id)
        && (recoverable_graph_task_ids.contains(&row.id)
            || has_requested_continuation(row)
            || parked_disposition(row) == ParkDisposition::Retryable)
}

fn terminal_evidence(rows: &[&FactsTaskRow], merge_commit_shas: &[String]) -> serde_json::Value {
    let tasks: Vec<serde_json::Value> = rows
        .iter()
        .map(|row| {
            serde_json::json!({
                "task_id": row.id,
                "status": row.status,
                "completion_provenance": row.completion_provenance,
            })
        })
        .collect();
    let mut evidence = serde_json::json!({ "tasks": tasks });
    if !merge_commit_shas.is_empty() {
        evidence["merge_commit_shas"] = serde_json::json!(merge_commit_shas);
    }
    evidence
}

/// Preserve a terminal status only when every collapsed row agrees. A graph
/// intent with mixed terminal statuses is durable but ambiguous, so facts must
/// retain `unknown` rather than borrowing an outcome from an arbitrary row.
fn shared_terminal_outcome(rows: &[&FactsTaskRow]) -> String {
    let Some(outcome) = rows.first().map(|row| row.status.as_str()) else {
        return "unknown".to_string();
    };
    if matches!(outcome, "done" | "failed" | "cancelled")
        && rows.iter().all(|row| row.status == outcome)
    {
        outcome.to_string()
    } else {
        "unknown".to_string()
    }
}

/// Return only merge witnesses that the immutable explicit-adoption ledger
/// has already correlated to both sides of a one-to-one recovery pair. The
/// nested task ref alone is deliberately insufficient: `build_lineage_snapshot`
/// populated these pairs only after validating accepted graph membership,
/// daemon-owned merged completions, and the matching ledger entry.
fn recovery_merge_witnesses(lineage: &LineageSnapshot) -> HashMap<i64, String> {
    let mut witnesses = HashMap::new();
    for pairs in lineage.original_to_recoveries.values() {
        let [pair] = pairs.as_slice() else {
            continue;
        };
        let Some(merged_head_sha) = pair
            .merged_head_sha
            .as_deref()
            .filter(|sha| valid_merge_identifier(sha))
        else {
            continue;
        };
        // The one durable delivery belongs to the adopted original and the
        // completed recovery task; both collapse into the same intent.
        witnesses.insert(pair.original_task_id, merged_head_sha.to_string());
        witnesses.insert(pair.recovery_task_id, merged_head_sha.to_string());
    }
    witnesses
}

fn verified_merge_witness(
    row: &FactsTaskRow,
    recovery_merge_witnesses: &HashMap<i64, String>,
) -> Option<String> {
    valid_merge_commit_sha(row).or_else(|| recovery_merge_witnesses.get(&row.id).cloned())
}

fn dedup_merge_witnesses(witnesses: Vec<String>) -> Vec<String> {
    let mut distinct = Vec::with_capacity(witnesses.len());
    for witness in witnesses {
        if !distinct.contains(&witness) {
            distinct.push(witness);
        }
    }
    distinct
}

/// Resolve one collapsed intent from bounded, durable state only. The order is
/// intentional: exclusions that establish non-delivery take precedence over a
/// terminal-looking status, and ambiguous evidence is never promoted to a
/// delivered or failed result.
fn resolve_intent(
    rows: &[&FactsTaskRow],
    recoverable_graph_task_ids: &HashSet<i64>,
    unsatisfiable_parked_task_ids: &HashSet<i64>,
    completed_graph_source_task_ids: &HashSet<i64>,
    recovery_merge_witnesses: &HashMap<i64, String>,
) -> ResolvedIntent {
    debug_assert!(!rows.is_empty());

    if rows.iter().any(|row| row.review_only) {
        if rows.iter().any(|row| {
            !matches!(row.status.as_str(), "done" | "failed" | "cancelled")
                || has_supported_recovery(
                    row,
                    recoverable_graph_task_ids,
                    unsatisfiable_parked_task_ids,
                )
        }) {
            return ResolvedIntent::nonterminal_with_reason(InclusionReason::ExcludedReviewOnly);
        }
        return ResolvedIntent::terminal(
            InclusionReason::ExcludedReviewOnly,
            &shared_terminal_outcome(rows),
            rows,
        );
    }
    if rows
        .iter()
        .any(|row| has_duplicate_cancellation_evidence(row))
    {
        return ResolvedIntent::terminal(InclusionReason::ExcludedDuplicate, "cancelled", rows);
    }
    if rows
        .iter()
        .any(|row| has_intake_decline_cancellation_evidence(row))
    {
        return ResolvedIntent::terminal(
            InclusionReason::ExcludedIntakeDeclined,
            "cancelled",
            rows,
        );
    }
    if rows.iter().any(|row| row.status == "cancelled") {
        return ResolvedIntent::terminal(
            InclusionReason::ExcludedHousekeepingCancellation,
            "cancelled",
            rows,
        );
    }
    if rows
        .iter()
        .any(|row| row.completion_provenance.as_deref() == Some("manual"))
    {
        return ResolvedIntent::terminal(InclusionReason::ExcludedManualCompletion, "done", rows);
    }
    if rows.iter().any(|row| {
        !matches!(row.status.as_str(), "done" | "failed")
            || has_supported_recovery(
                row,
                recoverable_graph_task_ids,
                unsatisfiable_parked_task_ids,
            )
    }) {
        return ResolvedIntent::nonterminal();
    }
    if rows.iter().any(|row| {
        matches!(
            parked_disposition(row),
            ParkDisposition::DeliveryUnknown | ParkDisposition::Malformed
        )
    }) {
        return ResolvedIntent::terminal(InclusionReason::ExcludedUnknownDelivery, "unknown", rows);
    }
    // A merged-completion marker on anything other than a done row is
    // contradictory retained evidence. It cannot establish either delivery or
    // failure, so preserve the ambiguity rather than guessing from status.
    if rows
        .iter()
        .any(|row| row.status != "done" && row.completion_provenance.as_deref() == Some("merged"))
    {
        return ResolvedIntent::terminal(InclusionReason::ExcludedUnknownDelivery, "unknown", rows);
    }
    if rows.iter().any(|row| row.status == "failed") {
        return ResolvedIntent::terminal(
            InclusionReason::IncludedIrrecoverableFailure,
            "failed",
            rows,
        );
    }

    let merge_commit_shas: Option<Vec<String>> = rows
        .iter()
        .filter(|row| {
            // A completed decomposition source is a daemon-owned aggregate,
            // not a delivery-bearing task. Its `done` status is the durable
            // graph-completion witness; each accepted child still needs its
            // own merged completion and merge identifier below.
            !(completed_graph_source_task_ids.contains(&row.id)
                && row.status == "done"
                && row.completion_provenance.is_none())
        })
        .map(|row| verified_merge_witness(row, recovery_merge_witnesses))
        .collect::<Option<Vec<_>>>()
        .map(dedup_merge_witnesses);
    if let Some(merge_commit_shas) = merge_commit_shas.filter(|shas| !shas.is_empty()) {
        return ResolvedIntent {
            reason: InclusionReason::IncludedVerifiedMerge,
            terminal_outcome: Some("done".to_string()),
            terminal_evidence: Some(terminal_evidence(rows, &merge_commit_shas)),
            merge_provenance: Some("merged".to_string()),
        };
    }

    ResolvedIntent::terminal(InclusionReason::ExcludedUnknownDelivery, "unknown", rows)
}

/// Membership row: a task_id that is a generated child of a source task
/// through an active graph member row. Carries the graph_id for evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
struct GraphChildInfo {
    task_id: i64,
    graph_id: i64,
    source_task_id: i64,
}

/// Recovery adoption row. Its fields are emitted as evidence only after the
/// reader has matched the immutable accepted-member relation, daemon-owned
/// completion provenance, and the daemon's explicit-adoption ledger entry.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RecoveryPairInfo {
    recovery_task_id: i64,
    original_task_id: i64,
    pr_number: Option<i64>,
    merged_head_sha: Option<String>,
}

/// Durable lineage relations captured under the same WAL snapshot as the
/// cohort reads. Absent from either map ⇒ no collapse. Relations are confined
/// to the capped facts cohort and its necessary graph roots; they are never
/// derived from titles, labels, matching PRs, or `continue_pr`.
struct LineageSnapshot {
    /// Every active graph child → the source task that owns its graph.
    child_to_source: HashMap<i64, i64>,
    /// Every accepted recovery task → the original it delivered against.
    recovery_to_original: HashMap<i64, RecoveryPairInfo>,
    /// Source task id → its active graph children (for evidence).
    source_to_children: BTreeMap<i64, Vec<GraphChildInfo>>,
    /// Original task id → all recovery pairs claiming that original.
    original_to_recoveries: BTreeMap<i64, Vec<RecoveryPairInfo>>,
}

/// Every lineage query uses a small parameter batch rather than a full task
/// history scan. The caps below also bound rows held in the facts snapshot:
/// each accepted graph has at most `MAX_CHILDREN` members, while seeing two
/// recovery mappings for one original is enough to reject it as ambiguous.
const LINEAGE_ID_BATCH: usize = 256;
const MAX_GRAPH_MEMBERS_PER_LINEAGE_ROOT: usize = crate::decomposition::MAX_CHILDREN;
const MAX_RECOVERY_MAPPINGS_PER_ORIGINAL: usize = 2;

fn sql_placeholders(len: usize) -> String {
    std::iter::repeat_n("?", len).collect::<Vec<_>>().join(",")
}

/// Look up only cohort tasks that are accepted generated children, to find
/// the roots that may need sibling evidence. `task_id` is UNIQUE in the
/// membership schema; the LIMIT is nevertheless retained as a hard reader
/// bound if a legacy/corrupt database violates that contract.
fn load_graph_member_roots(conn: &Connection, task_ids: &[i64]) -> Result<Vec<GraphChildInfo>> {
    let mut out = Vec::new();
    for batch in task_ids.chunks(LINEAGE_ID_BATCH) {
        let placeholders = sql_placeholders(batch.len());
        let sql = format!(
            "SELECT member.task_id,member.graph_id,graph.source_task_id \
             FROM task_graph_members member \
             JOIN task_decompositions graph ON graph.id=member.graph_id \
             WHERE member.task_id IN ({placeholders}) \
               AND member.active=1 \
               AND member.plan_revision=graph.accepted_plan_revision \
               AND (graph.active=1 OR graph.state='completed') \
             ORDER BY member.task_id \
             LIMIT ?"
        );
        let mut statement = conn.prepare(&sql)?;
        let mut params = batch.to_vec();
        params.push(batch.len() as i64);
        let rows = statement.query_map(params_from_iter(params), |row| {
            Ok(GraphChildInfo {
                task_id: row.get(0)?,
                graph_id: row.get(1)?,
                source_task_id: row.get(2)?,
            })
        })?;
        out.extend(rows.collect::<rusqlite::Result<Vec<_>>>()?);
    }
    Ok(out)
}

/// Read accepted members only for roots derived from the capped cohort. A
/// source with more than the daemon's declared child cap is malformed and is
/// excluded by the caller; the extra row makes that condition observable
/// without allocating its complete history.
fn load_graph_members_for_roots(
    conn: &Connection,
    root_ids: &[i64],
) -> Result<Vec<GraphChildInfo>> {
    let mut out = Vec::new();
    for batch in root_ids.chunks(LINEAGE_ID_BATCH) {
        let placeholders = sql_placeholders(batch.len());
        let sql = format!(
            "SELECT member.task_id,member.graph_id,graph.source_task_id \
             FROM task_decompositions graph \
             JOIN task_graph_members member ON member.graph_id=graph.id \
             WHERE graph.source_task_id IN ({placeholders}) \
               AND member.active=1 \
               AND member.plan_revision=graph.accepted_plan_revision \
               AND (graph.active=1 OR graph.state='completed') \
             ORDER BY graph.source_task_id,member.task_id \
             LIMIT ?"
        );
        let mut statement = conn.prepare(&sql)?;
        let mut params = batch.to_vec();
        params.push(
            batch
                .len()
                .saturating_mul(MAX_GRAPH_MEMBERS_PER_LINEAGE_ROOT + 1) as i64,
        );
        let rows = statement.query_map(params_from_iter(params), |row| {
            Ok(GraphChildInfo {
                task_id: row.get(0)?,
                graph_id: row.get(1)?,
                source_task_id: row.get(2)?,
            })
        })?;
        out.extend(rows.collect::<rusqlite::Result<Vec<_>>>()?);
    }
    Ok(out)
}

/// Read only recovery ledger rows whose original task is in the capped
/// cohort or an accepted member of one of its roots. Mutable task refs are
/// corroborating fields, never authority: a row is accepted only when the
/// original is an accepted generated child, both tasks have daemon-owned
/// merged completion, and the immutable daemon-written explicit-adoption
/// ledger names the exact same pair and delivery evidence.
fn load_recovery_deliveries(
    conn: &Connection,
    original_ids: &[i64],
) -> Result<Vec<RecoveryPairInfo>> {
    let mut out = Vec::new();
    for batch in original_ids.chunks(LINEAGE_ID_BATCH) {
        let placeholders = sql_placeholders(batch.len());
        let sql = format!(
            "SELECT original.id, \
                    json_extract(original.refs,'$.recovery_delivery.recovery_task'), \
                    json_extract(original.refs,'$.recovery_delivery.pr'), \
                    json_extract(original.refs,'$.recovery_delivery.merged_head_sha') \
             FROM tasks original \
             JOIN task_graph_members member ON member.task_id=original.id \
             JOIN task_decompositions graph ON graph.id=member.graph_id \
             JOIN tasks recovery \
               ON recovery.id=json_extract(original.refs,'$.recovery_delivery.recovery_task') \
             JOIN decomposition_attempts adoption \
               ON adoption.graph_id=graph.id \
              AND adoption.source_revision=graph.planned_source_revision \
              AND adoption.kind='recovery' \
              AND adoption.reason_code='explicit-delivery-adoption' \
             WHERE original.id IN ({placeholders}) \
               AND original.status='done' \
               AND original.completion_provenance='merged' \
               AND recovery.status='done' \
               AND recovery.completion_provenance='merged' \
               AND member.active=1 \
               AND member.plan_revision=graph.accepted_plan_revision \
               AND (graph.active=1 OR graph.state='completed') \
               AND json_valid(original.refs) \
               AND json_type(original.refs,'$.recovery_delivery.source_task')='integer' \
               AND json_extract(original.refs,'$.recovery_delivery.source_task')=original.id \
               AND json_type(original.refs,'$.recovery_delivery.recovery_task')='integer' \
               AND json_extract(original.refs,'$.recovery_delivery.recovery_task')!=original.id \
               AND json_type(original.refs,'$.recovery_delivery.pr')='integer' \
               AND json_type(original.refs,'$.recovery_delivery.merged_head_sha')='text' \
               AND json_valid(adoption.summary) \
               AND json_type(adoption.summary,'$.authority')='text' \
               AND json_extract(adoption.summary,'$.authority')='explicit-operator' \
               AND json_type(adoption.summary,'$.original_child')='integer' \
               AND json_extract(adoption.summary,'$.original_child')=original.id \
               AND json_type(adoption.summary,'$.recovery_task')='integer' \
               AND json_extract(adoption.summary,'$.recovery_task')=
                   json_extract(original.refs,'$.recovery_delivery.recovery_task') \
               AND json_type(adoption.summary,'$.decomposition_source')='integer' \
               AND json_extract(adoption.summary,'$.decomposition_source')=graph.source_task_id \
               AND json_type(adoption.summary,'$.pr')='integer' \
               AND json_extract(adoption.summary,'$.pr')=
                   json_extract(original.refs,'$.recovery_delivery.pr') \
               AND json_type(adoption.summary,'$.merged_head_sha')='text' \
               AND json_extract(adoption.summary,'$.merged_head_sha')=
                   json_extract(original.refs,'$.recovery_delivery.merged_head_sha') \
             ORDER BY original.id,adoption.id \
             LIMIT ?"
        );
        let mut statement = conn.prepare(&sql)?;
        let mut params = batch.to_vec();
        params.push(
            batch
                .len()
                .saturating_mul(MAX_RECOVERY_MAPPINGS_PER_ORIGINAL) as i64,
        );
        let rows = statement.query_map(params_from_iter(params), |row| {
            Ok(RecoveryPairInfo {
                original_task_id: row.get(0)?,
                recovery_task_id: row.get(1)?,
                pr_number: Some(row.get(2)?),
                merged_head_sha: Some(row.get(3)?),
            })
        })?;
        out.extend(rows.collect::<rusqlite::Result<Vec<_>>>()?);
    }
    Ok(out)
}

fn build_lineage_snapshot(conn: &Connection, cohort_ids: &[i64]) -> Result<LineageSnapshot> {
    // The first query discovers roots for cohort children. It is keyed by
    // `task_graph_members.task_id` (UNIQUE), so completed-graph retained
    // members cannot turn this into a history traversal.
    let direct_members = load_graph_member_roots(conn, cohort_ids)?;
    let mut root_ids: BTreeSet<i64> = cohort_ids.iter().copied().collect();
    root_ids.extend(direct_members.iter().map(|member| member.source_task_id));
    let root_ids: Vec<i64> = root_ids.into_iter().collect();

    // Then read only members of those roots. Invalid over-cap roots are
    // discarded wholesale, so a truncated/corrupt relation can never become
    // a partial, guessed lineage mapping.
    let root_members = load_graph_members_for_roots(conn, &root_ids)?;
    let mut source_to_children: BTreeMap<i64, Vec<GraphChildInfo>> = BTreeMap::new();
    for member in root_members {
        source_to_children
            .entry(member.source_task_id)
            .or_default()
            .push(member);
    }
    source_to_children.retain(|_, members| {
        if members.len() > MAX_GRAPH_MEMBERS_PER_LINEAGE_ROOT {
            return false;
        }
        members.sort_by_key(|member| member.task_id);
        true
    });
    let child_to_source: HashMap<i64, i64> = source_to_children
        .iter()
        .flat_map(|(&source, members)| members.iter().map(move |member| (member.task_id, source)))
        .collect();

    // A recovery relation can affect a cohort original or a sibling of one
    // of its roots, but nothing else. This caps the ledger read to the facts
    // cohort plus at most MAX_CHILDREN per derived root.
    let mut recovery_original_ids: BTreeSet<i64> = cohort_ids.iter().copied().collect();
    recovery_original_ids.extend(
        source_to_children
            .values()
            .flat_map(|members| members.iter().map(|member| member.task_id)),
    );
    let recovery_original_ids: Vec<i64> = recovery_original_ids.into_iter().collect();
    let pairs = load_recovery_deliveries(conn, &recovery_original_ids)?;

    // A recovery/original must be one-to-one. The ledger normally enforces
    // this by construction; if retained or malformed data presents a
    // duplicate/conflict, reject every implicated mapping instead of letting
    // insertion order choose a winner.
    let mut original_counts: HashMap<i64, usize> = HashMap::new();
    let mut recovery_counts: HashMap<i64, usize> = HashMap::new();
    for pair in &pairs {
        *original_counts.entry(pair.original_task_id).or_default() += 1;
        *recovery_counts.entry(pair.recovery_task_id).or_default() += 1;
    }
    let mut recovery_to_original = HashMap::new();
    let mut original_to_recoveries: BTreeMap<i64, Vec<RecoveryPairInfo>> = BTreeMap::new();
    for pair in pairs {
        if original_counts[&pair.original_task_id] != 1
            || recovery_counts[&pair.recovery_task_id] != 1
        {
            continue;
        }
        recovery_to_original.insert(pair.recovery_task_id, pair.clone());
        original_to_recoveries
            .entry(pair.original_task_id)
            .or_default()
            .push(pair);
    }

    Ok(LineageSnapshot {
        child_to_source,
        recovery_to_original,
        source_to_children,
        original_to_recoveries,
    })
}

/// Find candidate tasks with a durable graph continuation path. Active and
/// blocked graphs are live by their `active` sentinel. A held, pre-
/// materialization source is also nonterminal only when it passes the exact
/// bounded `task-retry` eligibility check; other held graphs stay terminal.
fn load_recoverable_graph_task_ids(
    conn: &Connection,
    task_ids: &[i64],
    now: i64,
) -> Result<HashSet<i64>> {
    let mut recoverable = HashSet::new();
    for batch in task_ids.chunks(LINEAGE_ID_BATCH) {
        let placeholders = sql_placeholders(batch.len());
        let sql = format!(
            "SELECT m.task_id \
             FROM task_graph_members m \
             JOIN task_decompositions d ON d.id=m.graph_id \
             WHERE m.task_id IN ({placeholders}) \
               AND m.active=1 AND d.active=1 \
             UNION \
             SELECT d.source_task_id \
             FROM task_decompositions d \
             WHERE d.source_task_id IN ({placeholders}) AND d.active=1 \
             LIMIT ?"
        );
        let mut params = batch.to_vec();
        params.extend_from_slice(batch);
        params.push(batch.len().saturating_mul(2) as i64);
        let mut statement = conn.prepare(&sql)?;
        let rows = statement.query_map(params_from_iter(params), |row| row.get::<_, i64>(0))?;
        recoverable.extend(rows.collect::<rusqlite::Result<Vec<_>>>()?);
    }
    // Reuse the lifecycle predicate rather than approximating its hold code,
    // attempt-history, and retry-budget evidence in this read-only report.
    // The facts cohort is capped, and the predicate itself examines one graph
    // with bounded attempt history, so this remains a bounded scan.
    for &task_id in task_ids {
        if crate::decomposition::exhausted_planning_retry_is_eligible(conn, task_id, now)? {
            recoverable.insert(task_id);
        }
    }
    Ok(recoverable)
}

/// A completed graph source is a durable aggregate completion, not an
/// independent merge. Restrict this exception to sources with accepted live
/// membership so a retained `done` row never becomes delivery evidence alone.
fn load_completed_graph_source_task_ids(
    conn: &Connection,
    task_ids: &[i64],
) -> Result<HashSet<i64>> {
    let mut completed = HashSet::new();
    for batch in task_ids.chunks(LINEAGE_ID_BATCH) {
        let placeholders = sql_placeholders(batch.len());
        let sql = format!(
            "SELECT d.source_task_id \
             FROM task_decompositions d \
             WHERE d.source_task_id IN ({placeholders}) \
               AND d.state='completed' AND d.active=0 AND d.freeze_active=0 \
               AND EXISTS (SELECT 1 FROM task_graph_members m \
                           WHERE m.graph_id=d.id AND m.active=1 \
                             AND m.plan_revision=d.accepted_plan_revision)"
        );
        let mut statement = conn.prepare(&sql)?;
        let rows =
            statement.query_map(params_from_iter(batch.iter()), |row| row.get::<_, i64>(0))?;
        completed.extend(rows.collect::<rusqlite::Result<Vec<_>>>()?);
    }
    Ok(completed)
}

/// Walk (recovery → original) then (child → source) until we reach a task
/// that is neither a recovery target nor a graph child. Cycle-guarded with
/// a bounded step count so a pathological refs blob cannot spin forever.
fn canonical_root(task_id: i64, lineage: &LineageSnapshot) -> i64 {
    let mut current = task_id;
    let mut seen: HashSet<i64> = HashSet::new();
    for _ in 0..8 {
        if !seen.insert(current) {
            break;
        }
        if let Some(pair) = lineage.recovery_to_original.get(&current) {
            current = pair.original_task_id;
            continue;
        }
        if let Some(&src) = lineage.child_to_source.get(&current) {
            current = src;
            continue;
        }
        break;
    }
    current
}

/// A completed graph aggregate can establish delivery only after every
/// accepted child is present in the bounded facts cohort. The lineage scan
/// reads this small, daemon-capped member set even when one child lies just
/// beyond the raw task cap, so never infer that the in-cap subset is complete.
fn completed_graph_has_omitted_member(
    root: i64,
    cohort_tasks: &HashMap<i64, FactsTaskRow>,
    lineage: &LineageSnapshot,
) -> bool {
    lineage
        .source_to_children
        .get(&root)
        .is_some_and(|children| {
            children
                .iter()
                .any(|child| !cohort_tasks.contains_key(&child.task_id))
        })
}

/// Build the JSON evidence blob for a collapsed intent's lineage. `members`
/// is the sorted list of terminal-cohort task ids that fold into `root`;
/// every claim is backed by a durable relation captured in `lineage`.
///
/// Returns `None` when no durable collapse relation exists for `root`
/// (no active graph children and no accepted recovery pairs touching the
/// intent). Consumers must treat that as an explicit coverage gap — a
/// standalone task, a shared-PR/continue_pr link without provenance, and a
/// row whose `$.recovery_delivery` failed the daemon-owned adoption gate
/// are all indistinguishable to this reader and none of them establishes
/// lineage.
fn build_lineage_evidence(
    root: i64,
    members: &[i64],
    lineage: &LineageSnapshot,
) -> Option<serde_json::Value> {
    let children = lineage.source_to_children.get(&root);

    // Gather all recovery pairs whose original is either the root itself
    // or one of the root's graph children — these are the pairs that
    // fold into this intent.
    let mut relevant_originals: BTreeSet<i64> = BTreeSet::new();
    if lineage.original_to_recoveries.contains_key(&root) {
        relevant_originals.insert(root);
    }
    if let Some(cs) = children {
        for c in cs {
            if lineage.original_to_recoveries.contains_key(&c.task_id) {
                relevant_originals.insert(c.task_id);
            }
        }
    }
    let mut recovery_pairs: Vec<&RecoveryPairInfo> = Vec::new();
    for orig in &relevant_originals {
        if let Some(rs) = lineage.original_to_recoveries.get(orig) {
            for p in rs {
                recovery_pairs.push(p);
            }
        }
    }

    let has_children = children.map(|v| !v.is_empty()).unwrap_or(false);
    let has_recovery = !recovery_pairs.is_empty();

    let kind = match (has_children, has_recovery) {
        (true, true) => "decomposed+recovery",
        (true, false) => "decomposed",
        (false, true) => "recovery",
        // Standalone / ambiguous / malformed provenance is an explicit
        // coverage gap, not lineage evidence. Never fabricate a root.
        (false, false) => return None,
    };

    let mut obj = serde_json::Map::new();
    obj.insert("kind".to_string(), serde_json::Value::String(kind.into()));
    obj.insert("root_task_id".to_string(), serde_json::json!(root));
    obj.insert("member_task_ids".to_string(), serde_json::json!(members));

    if let Some(cs) = children {
        if !cs.is_empty() {
            let graph_ids: BTreeSet<i64> = cs.iter().map(|c| c.graph_id).collect();
            let child_ids: Vec<i64> = cs.iter().map(|c| c.task_id).collect();
            obj.insert("source_task_id".to_string(), serde_json::json!(root));
            obj.insert(
                "graph_ids".to_string(),
                serde_json::json!(graph_ids.into_iter().collect::<Vec<i64>>()),
            );
            obj.insert(
                "generated_child_task_ids".to_string(),
                serde_json::json!(child_ids),
            );
        }
    }

    if !recovery_pairs.is_empty() {
        let pairs_json: Vec<serde_json::Value> = recovery_pairs
            .iter()
            .map(|p| {
                serde_json::json!({
                    "original_task_id": p.original_task_id,
                    "recovery_task_id": p.recovery_task_id,
                    "pr_number": p.pr_number,
                    "merged_head_sha": p.merged_head_sha,
                })
            })
            .collect();
        obj.insert(
            "recovery_pairs".to_string(),
            serde_json::Value::Array(pairs_json),
        );
    }

    Some(serde_json::Value::Object(obj))
}

/// Materialized snapshot of the cohort reads plus durable lineage
/// relations. Captured under one WAL read transaction so counts, ids,
/// graph memberships, and recovery-delivery provenance all reflect the
/// same database state.
struct CohortSnapshot {
    watermark: Option<i64>,
    candidate_count: i64,
    candidate_tasks: Vec<FactsTaskRow>,
    lineage: LineageSnapshot,
    recoverable_graph_task_ids: HashSet<i64>,
    unsatisfiable_parked_task_ids: HashSet<i64>,
    completed_graph_source_task_ids: HashSet<i64>,
}

/// Take the watermark, aggregate counts, and bounded candidate scan under
/// a single WAL read snapshot so their totals are internally consistent —
/// even if a daemon lifecycle write commits between conceptual steps. If the
/// caller already owns a transaction, its existing snapshot is reused
/// instead of nesting a second.
fn read_cohort_snapshot(conn: &Connection, include_all: bool) -> Result<CohortSnapshot> {
    let now = crate::clock::now();
    let read = |c: &Connection| -> Result<CohortSnapshot> {
        let watermark = read_watermark(c)?;
        let since = if include_all { None } else { watermark };
        // First SELECT establishes the snapshot; subsequent reads see it.
        let candidate_count = count_candidates(c, since)?;
        let candidate_tasks = load_facts_candidate_tasks(c, since, MAX_INTENTS + 1)?;
        // The sentinel row detects truncation but is not part of the facts
        // cohort, so lineage reads never expand beyond MAX_INTENTS ids.
        let capped_ids: Vec<i64> = candidate_tasks
            .iter()
            .take(MAX_INTENTS)
            .map(|task| task.id)
            .collect();
        let lineage = build_lineage_snapshot(c, &capped_ids)?;
        let recoverable_graph_task_ids = load_recoverable_graph_task_ids(c, &capped_ids, now)?;
        let unsatisfiable_parked_task_ids = load_unsatisfiable_parked_task_ids(
            c,
            &candidate_tasks[..candidate_tasks.len().min(MAX_INTENTS)],
        )?;
        let completed_graph_source_task_ids = load_completed_graph_source_task_ids(c, &capped_ids)?;
        Ok(CohortSnapshot {
            watermark,
            candidate_count,
            candidate_tasks,
            lineage,
            recoverable_graph_task_ids,
            unsatisfiable_parked_task_ids,
            completed_graph_source_task_ids,
        })
    };
    if !conn.is_autocommit() {
        return read(conn);
    }
    let tx = conn.unchecked_transaction().map_err(map_sql_err)?;
    let snap = read(&tx)?;
    tx.commit().map_err(map_sql_err)?;
    Ok(snap)
}

/// Read-only facts surface for `quorum perf`. Returns a deterministic,
/// bounded `FactsReport` with one resolved intent per candidate lineage. A
/// terminal task is included only for a daemon-owned merged completion with a
/// valid merge commit witness, or for a durable, irrecoverable failure. Every
/// other candidate remains visible as an excluded intent with a bounded code.
///
/// Cohort selection is prospective by default via `perf_watermark`;
/// `include_all` bypasses that boundary. All DB reads happen inside one
/// short WAL read snapshot that ends before report construction — no
/// transaction is held across allocation or serialization work. Performs
/// no writes.
pub fn perf_facts(conn: &Connection, include_all: bool) -> Result<FactsReport> {
    // Gather the three interdependent reads under one WAL snapshot, then let
    // the transaction end before we build the report. Report construction
    // touches no DB state.
    let snap = read_cohort_snapshot(conn, include_all)?;
    let CohortSnapshot {
        watermark,
        candidate_count,
        candidate_tasks,
        lineage,
        recoverable_graph_task_ids,
        unsatisfiable_parked_task_ids,
        completed_graph_source_task_ids,
    } = snap;

    let cohort = CohortDefinition {
        prospective_only: !include_all,
        watermark,
        include_all,
    };
    let query_limits = QueryLimits {
        max_intents: MAX_INTENTS,
        max_contributing_tasks_per_intent: MAX_CONTRIBUTING_TASKS_PER_INTENT,
    };
    // Extract only immutable-ledger-correlated recovery delivery witnesses
    // after the short read snapshot has ended. Bare nested refs never enter
    // terminal classification.
    let recovery_merge_witnesses = recovery_merge_witnesses(&lineage);

    let truncated = candidate_tasks.len() > MAX_INTENTS;

    // Collapse capped cohort tasks into their canonical intent roots via
    // durable lineage (graph membership, recovery-delivery provenance).
    // Group by root_id in BTreeMap ordering for deterministic output.
    let capped_tasks: Vec<FactsTaskRow> = candidate_tasks.into_iter().take(MAX_INTENTS).collect();
    let tasks_by_id: HashMap<i64, FactsTaskRow> = capped_tasks
        .iter()
        .cloned()
        .map(|task| (task.id, task))
        .collect();
    let mut groups: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    for task in &capped_tasks {
        let root = canonical_root(task.id, &lineage);
        groups.entry(root).or_default().push(task.id);
    }

    let mut intents: Vec<IntentFacts> = Vec::with_capacity(groups.len());
    for (root, mut members) in groups {
        members.sort();
        let members_for_evidence = members.clone();
        let evidence = build_lineage_evidence(root, &members_for_evidence, &lineage);
        let member_rows: Vec<&FactsTaskRow> = members
            .iter()
            .map(|id| {
                tasks_by_id
                    .get(id)
                    .expect("lineage groups are derived from capped candidate ids")
            })
            .collect();
        let resolved = if completed_graph_source_task_ids.contains(&root)
            && completed_graph_has_omitted_member(root, &tasks_by_id, &lineage)
        {
            // The source is a completed aggregate, but a durable accepted
            // child is outside this facts cohort. Classify the partial graph
            // explicitly as truncated/unknown rather than crediting only its
            // in-cap merged children.
            let reason = if truncated {
                InclusionReason::ExcludedTruncated
            } else {
                // A child excluded by the prospective watermark is not a
                // cap overflow, but it still leaves delivery incomplete.
                InclusionReason::ExcludedUnknownDelivery
            };
            ResolvedIntent::terminal(reason, "unknown", &member_rows)
        } else {
            resolve_intent(
                &member_rows,
                &recoverable_graph_task_ids,
                &unsatisfiable_parked_task_ids,
                &completed_graph_source_task_ids,
                &recovery_merge_witnesses,
            )
        };
        // Clip contributing task ids to the per-intent bound.
        let contributing: Vec<i64> = members
            .into_iter()
            .take(MAX_CONTRIBUTING_TASKS_PER_INTENT)
            .collect();
        let mut intent = new_intent_facts(root, contributing, resolved.reason);
        intent.terminal_outcome = resolved.terminal_outcome;
        intent.terminal_evidence = resolved.terminal_evidence;
        intent.merge_provenance = resolved.merge_provenance;
        intent.coverage.terminal = intent.terminal_evidence.is_some();
        intent.coverage.merge_provenance = intent.merge_provenance.is_some();
        // Populate lineage only when a durable collapse relation exists.
        // Standalone / shared-PR-only / malformed provenance leaves
        // lineage_evidence null and coverage.lineage false — an explicit
        // coverage gap, distinguishable from a measured value.
        if let Some(ev) = evidence {
            intent.lineage_root_task_id = Some(root);
            intent.lineage_evidence = Some(ev);
            intent.coverage.lineage = true;
        }
        intents.push(intent);
    }

    let mut excluded_reasons: BTreeMap<String, i64> = BTreeMap::new();
    for intent in &intents {
        if !intent.included {
            *excluded_reasons
                .entry(intent.reason.as_str().to_string())
                .or_default() += 1;
        }
    }
    if truncated {
        // Bounded aggregate lets us report the exact truncated excess without
        // loading the overflow tail. Those raw candidates cannot be resolved,
        // so they are explicitly excluded rather than silently omitted.
        let overflow = (candidate_count - MAX_INTENTS as i64).max(0);
        if overflow > 0 {
            *excluded_reasons
                .entry(InclusionReason::ExcludedTruncated.as_str().to_string())
                .or_default() += overflow;
        }
    }

    // These counts are derived from the resolved per-intent flags. Raw rows
    // beyond the cap are surfaced as `excluded-truncated`, preserving the
    // `included + excluded == candidate` invariant without an unbounded read.
    let included_count = intents.iter().filter(|intent| intent.included).count() as i64;
    let overflow = (candidate_count - MAX_INTENTS as i64).max(0);
    let excluded_count = intents.iter().filter(|intent| !intent.included).count() as i64 + overflow;
    let resolved_candidate_count = intents.len() as i64 + overflow;

    // Coverage summary: iterate each intent's flags and tally covered vs
    // uncovered per named field. Order comes from `IntentCoverage::iter_named`
    // (via BTreeMap insertion), producing deterministic output.
    let mut coverage = CoverageSummary::default();
    for intent in &intents {
        for (name, covered) in intent.coverage.iter_named() {
            let entry = coverage.fields.entry(name.to_string()).or_default();
            if covered {
                entry.covered += 1;
            } else {
                entry.uncovered += 1;
            }
        }
    }
    // Ensure the map contains every declared field even when there are no
    // intents, so consumers can rely on a stable schema.
    if intents.is_empty() {
        for (name, _) in IntentCoverage::default().iter_named() {
            coverage.fields.entry(name.to_string()).or_default();
        }
    }

    Ok(FactsReport {
        facts_version: FACTS_VERSION,
        cohort,
        query_limits,
        counts: CohortCounts {
            candidate: resolved_candidate_count,
            included: included_count,
            excluded: excluded_count,
        },
        coverage,
        excluded_reasons,
        intents,
    })
}

pub fn render_table(report: &PerfReport) {
    if report.rows.is_empty() {
        println!("No terminal tasks found.");
        return;
    }

    let has_complexity = report.rows.iter().any(|r| r.complexity.is_some());
    let has_reviewer = report.rows.iter().any(|r| r.reviewer.is_some());

    let mut header = format!("{:<14} {:<8}", "MODEL", "EFFORT");
    if has_complexity {
        header.push_str(&format!(" {:<12}", "COMPLEXITY"));
    }
    if has_reviewer {
        header.push_str(&format!(" {:<18}", "REVIEWER"));
    }
    header.push_str(&format!(
        " {:>7} {:>10} {:>9} {:>7} {:>11} {:>10} {:>7} {:>8} {:>9} {:>10}",
        "N",
        "1st_PASS",
        "AVG_RWK",
        "FAIL",
        "MED_MINS",
        "AVG_REV_S",
        "RUBBER",
        "APR_RT",
        "AVG_BLK",
        "COST_USD"
    ));
    println!("{header}");
    println!("{}", "-".repeat(header.len()));

    for r in &report.rows {
        let mut line = format!("{:<14} {:<8}", r.model, r.effort);
        if has_complexity {
            line.push_str(&format!(" {:<12}", r.complexity.as_deref().unwrap_or("")));
        }
        if has_reviewer {
            line.push_str(&format!(" {:<18}", r.reviewer.as_deref().unwrap_or("")));
        }
        line.push_str(&format!(
            " {:>7} {:>9.1}% {:>9.2} {:>6.1}% {:>11.1} {:>10.0} {:>7} {:>7.1}% {:>9.2} {:>10.2}",
            r.n_tasks,
            r.first_pass_pct,
            r.avg_rework,
            r.fail_pct,
            r.median_wall_mins,
            r.avg_reviewer_secs,
            r.rubber_stamp_count,
            r.approve_rate_pct,
            r.avg_blocking,
            r.total_cost_usd
        ));
        println!("{line}");
    }
}

// ── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const DM: &str = "fallback-model";
    const DE: &str = "fallback-effort";

    fn open_tmp() -> (tempfile::TempDir, rusqlite::Connection) {
        let dir = tempfile::tempdir().unwrap();
        let c = crate::db::open(&dir.path().join("q.db")).unwrap();
        // Reset watermark to 0 so aggregation tests see all seeded tasks
        // (their hardcoded timestamps predate the live migration watermark).
        c.execute("UPDATE perf_watermark SET watermark = 0 WHERE id = 1", [])
            .unwrap();
        (dir, c)
    }

    fn seed_task(
        conn: &mut rusqlite::Connection,
        status: &str,
        labels: Option<&str>,
        rework_round: i64,
        reviewer: Option<&str>,
        created_at: i64,
        updated_at: i64,
    ) -> i64 {
        let tx = crate::db::begin_immediate(conn).unwrap();
        tx.execute(
            "INSERT INTO tasks(title, body, status, priority, labels, assignee, created_by, \
             created_at, updated_at, refs, depends_on, author, reviewer, rework_round, review_only) \
             VALUES ('test', NULL, ?1, 0, ?2, NULL, 'boss', ?3, ?4, NULL, NULL, 'worker', ?5, ?6, 0)",
            rusqlite::params![status, labels, created_at, updated_at, reviewer, rework_round],
        )
        .unwrap();
        let id = tx.last_insert_rowid();
        tx.commit().unwrap();
        id
    }

    fn seed_run(conn: &Connection, task_id: i64, model: &str, effort: &str, spawned_at: i64) {
        crate::agent_runs::insert(
            conn, task_id, "agent", "worker", model, effort, "claude", spawned_at,
        )
        .unwrap();
    }

    fn seed_reviewer_run(
        conn: &Connection,
        task_id: i64,
        agent: &str,
        spawned_at: i64,
        ended_at: i64,
    ) {
        let run_id = crate::agent_runs::insert(
            conn, task_id, agent, "reviewer", "opus-46", "high", "claude", spawned_at,
        )
        .unwrap();
        crate::agent_runs::close(conn, run_id, ended_at, "done").unwrap();
    }

    fn seed_approval(conn: &Connection, task_id: i64, verdict: &str, blocking_count: i64) {
        conn.execute(
            "INSERT OR REPLACE INTO approvals \
             (pr_number, task_id, author, reviewer, verdict, blocking_count, approved_head_sha, created_at) \
             VALUES (?1, ?2, 'worker', 'reviewer', ?3, ?4, 'abc123', 1000)",
            rusqlite::params![task_id * 100, task_id, verdict, blocking_count],
        )
        .unwrap();
    }

    fn seed_journal_cost(conn: &mut Connection, task_id: i64, agent: &str, cost_usd: f64) {
        let tx = crate::db::begin_immediate(conn).unwrap();
        tx.execute(
            "INSERT INTO journal \
             (agent, role, task_id, session_id, phase, cost_tokens, cost_usd, updated_at) \
             VALUES (?1, 'worker', ?2, 'sess-1', 'working', 0, ?3, 1000)",
            rusqlite::params![agent, task_id, cost_usd],
        )
        .unwrap();
        tx.commit().unwrap();
    }

    #[test]
    fn empty_db_returns_empty_report() {
        let (_d, c) = open_tmp();
        let r = perf(&c, PerfCut::Default, DM, DE).unwrap();
        assert!(r.rows.is_empty());
    }

    #[test]
    fn model_effort_from_agent_runs() {
        let (_d, mut c) = open_tmp();
        let tid = seed_task(&mut c, "done", None, 0, Some("rev-1"), 1000, 1600);
        seed_run(&c, tid, "claude-opus-4-6", "high", 1001);

        let r = perf(&c, PerfCut::Default, DM, DE).unwrap();
        assert_eq!(r.rows.len(), 1);
        let row = &r.rows[0];
        assert_eq!(row.model, "claude-opus-4-6");
        assert_eq!(row.effort, "high");
        assert_eq!(row.n_tasks, 1);
        assert!((row.first_pass_pct - 100.0).abs() < 0.01);
        assert!((row.median_wall_mins - 10.0).abs() < 0.01);
    }

    #[test]
    fn orphan_task_uses_defaults() {
        let (_d, mut c) = open_tmp();
        seed_task(&mut c, "done", None, 0, None, 1000, 1600);

        let r = perf(&c, PerfCut::Default, DM, DE).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].model, DM);
        assert_eq!(r.rows[0].effort, DE);
    }

    #[test]
    fn earliest_worker_run_wins() {
        let (_d, mut c) = open_tmp();
        let tid = seed_task(&mut c, "done", None, 1, None, 1000, 2000);
        seed_run(&c, tid, "claude-opus-4-6", "medium", 1100);
        seed_run(&c, tid, "claude-sonnet-5", "high", 1050);

        let r = perf(&c, PerfCut::Default, DM, DE).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(
            r.rows[0].model, "claude-sonnet-5",
            "earlier spawn should win"
        );
        assert_eq!(r.rows[0].effort, "high");
    }

    #[test]
    fn reviewer_run_ignored() {
        let (_d, mut c) = open_tmp();
        let tid = seed_task(&mut c, "done", None, 0, None, 1000, 1600);
        // Only a reviewer run — should fall back to defaults
        crate::agent_runs::insert(
            &c,
            tid,
            "rev",
            "reviewer",
            "claude-opus-4-8",
            "max",
            "claude",
            1001,
        )
        .unwrap();

        let r = perf(&c, PerfCut::Default, DM, DE).unwrap();
        assert_eq!(r.rows[0].model, DM);
        assert_eq!(r.rows[0].effort, DE);
    }

    #[test]
    fn mixed_outcomes_correct_aggregates() {
        let (_d, mut c) = open_tmp();

        let t1 = seed_task(&mut c, "done", None, 0, Some("R"), 1000, 1600);
        seed_run(&c, t1, "opus-47", "medium", 1001);
        let t2 = seed_task(&mut c, "done", None, 2, Some("R"), 1000, 2200);
        seed_run(&c, t2, "opus-47", "medium", 1001);
        let t3 = seed_task(&mut c, "failed", None, 1, Some("R"), 1000, 1300);
        seed_run(&c, t3, "opus-47", "medium", 1001);
        let t4 = seed_task(&mut c, "cancelled", None, 0, None, 1000, 1060);
        seed_run(&c, t4, "opus-47", "medium", 1001);

        let r = perf(&c, PerfCut::Default, DM, DE).unwrap();
        assert_eq!(r.rows.len(), 1);
        let row = &r.rows[0];
        assert_eq!(row.model, "opus-47");
        assert_eq!(row.effort, "medium");
        assert_eq!(row.n_tasks, 4);
        assert!((row.first_pass_pct - 25.0).abs() < 0.01);
        assert!((row.avg_rework - 0.75).abs() < 0.01);
        assert!((row.fail_pct - 50.0).abs() < 0.01);
        assert!((row.median_wall_mins - 7.5).abs() < 0.01);
    }

    #[test]
    fn complexity_cut_splits_by_label() {
        let (_d, mut c) = open_tmp();
        let t1 = seed_task(
            &mut c,
            "done",
            Some(r#"["complexity:simple"]"#),
            0,
            None,
            1000,
            1600,
        );
        seed_run(&c, t1, "opus-46", "high", 1001);
        let t2 = seed_task(
            &mut c,
            "done",
            Some(r#"["complexity:complex"]"#),
            1,
            None,
            1000,
            2200,
        );
        seed_run(&c, t2, "opus-46", "high", 1001);
        let t3 = seed_task(&mut c, "done", None, 0, None, 1000, 1300);
        seed_run(&c, t3, "opus-46", "high", 1001);

        let r = perf(&c, PerfCut::Complexity, DM, DE).unwrap();
        assert_eq!(r.rows.len(), 3);
        let simple = r
            .rows
            .iter()
            .find(|r| r.complexity.as_deref() == Some("simple"))
            .unwrap();
        assert_eq!(simple.n_tasks, 1);
        assert!((simple.first_pass_pct - 100.0).abs() < 0.01);

        let complex = r
            .rows
            .iter()
            .find(|r| r.complexity.as_deref() == Some("complex"))
            .unwrap();
        assert_eq!(complex.n_tasks, 1);
        assert!((complex.first_pass_pct - 0.0).abs() < 0.01);

        let untagged = r
            .rows
            .iter()
            .find(|r| r.complexity.as_deref() == Some("untagged"))
            .unwrap();
        assert_eq!(untagged.n_tasks, 1);
    }

    #[test]
    fn reviewer_cut_splits_by_reviewer() {
        let (_d, mut c) = open_tmp();
        let t1 = seed_task(&mut c, "done", None, 0, Some("alice"), 1000, 1600);
        seed_run(&c, t1, "opus-46", "high", 1001);
        let t2 = seed_task(&mut c, "done", None, 1, Some("alice"), 1000, 2200);
        seed_run(&c, t2, "opus-46", "high", 1001);
        let t3 = seed_task(&mut c, "done", None, 0, Some("bob"), 1000, 1300);
        seed_run(&c, t3, "opus-46", "high", 1001);
        let t4 = seed_task(&mut c, "failed", None, 0, None, 1000, 1060);
        seed_run(&c, t4, "opus-46", "high", 1001);

        let r = perf(&c, PerfCut::Reviewer, DM, DE).unwrap();
        assert_eq!(r.rows.len(), 3);

        let alice = r
            .rows
            .iter()
            .find(|r| r.reviewer.as_deref() == Some("alice"))
            .unwrap();
        assert_eq!(alice.n_tasks, 2);
        assert!((alice.first_pass_pct - 50.0).abs() < 0.01);

        let bob = r
            .rows
            .iter()
            .find(|r| r.reviewer.as_deref() == Some("bob"))
            .unwrap();
        assert_eq!(bob.n_tasks, 1);

        let none = r
            .rows
            .iter()
            .find(|r| r.reviewer.as_deref() == Some("none"))
            .unwrap();
        assert_eq!(none.n_tasks, 1);
    }

    #[test]
    fn multiple_model_effort_combos() {
        let (_d, mut c) = open_tmp();
        let t1 = seed_task(&mut c, "done", None, 0, None, 1000, 1600);
        seed_run(&c, t1, "opus-46", "high", 1001);
        let t2 = seed_task(&mut c, "done", None, 0, None, 1000, 1300);
        seed_run(&c, t2, "sonnet-5", "medium", 1001);
        // Orphan — falls back to defaults
        seed_task(&mut c, "done", None, 0, None, 1000, 1120);

        let r = perf(&c, PerfCut::Default, DM, DE).unwrap();
        assert_eq!(r.rows.len(), 3);

        let opus = r.rows.iter().find(|r| r.model == "opus-46").unwrap();
        assert_eq!(opus.effort, "high");

        let sonnet = r.rows.iter().find(|r| r.model == "sonnet-5").unwrap();
        assert_eq!(sonnet.effort, "medium");

        let fallback = r.rows.iter().find(|r| r.model == DM).unwrap();
        assert_eq!(fallback.effort, DE);
    }

    #[test]
    fn no_unknown_in_output() {
        let (_d, mut c) = open_tmp();
        // Mix: one with agent_runs, one orphan
        let t1 = seed_task(&mut c, "done", None, 0, None, 1000, 1600);
        seed_run(&c, t1, "claude-opus-4-6", "medium", 1001);
        seed_task(&mut c, "done", None, 0, None, 1000, 1300);

        let r = perf(&c, PerfCut::Default, DM, DE).unwrap();
        for row in &r.rows {
            assert_ne!(row.model, "unknown", "model must never be 'unknown'");
            assert_ne!(row.effort, "unknown", "effort must never be 'unknown'");
        }
    }

    #[test]
    fn non_terminal_tasks_excluded() {
        let (_d, mut c) = open_tmp();
        // Terminal
        let tid = seed_task(&mut c, "done", None, 0, None, 1000, 1600);
        seed_run(&c, tid, "opus-46", "high", 1001);
        // Non-terminal — must be excluded
        let tx = crate::db::begin_immediate(&mut c).unwrap();
        tx.execute(
            "INSERT INTO tasks(title, body, status, priority, labels, assignee, created_by, \
             created_at, updated_at, refs, depends_on, author, reviewer, rework_round, review_only) \
             VALUES ('wip', NULL, 'working', 0, NULL, 'A', 'boss', 1000, 1600, NULL, NULL, 'A', NULL, 0, 0)",
            [],
        ).unwrap();
        tx.execute(
            "INSERT INTO tasks(title, body, status, priority, labels, assignee, created_by, \
             created_at, updated_at, refs, depends_on, author, reviewer, rework_round, review_only) \
             VALUES ('open', NULL, 'open', 0, NULL, NULL, 'boss', 1000, 1600, NULL, NULL, NULL, NULL, 0, 0)",
            [],
        ).unwrap();
        tx.commit().unwrap();

        let r = perf(&c, PerfCut::Default, DM, DE).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].n_tasks, 1);
    }

    #[test]
    fn median_odd_count() {
        assert!((median(&[1.0, 3.0, 5.0]) - 3.0).abs() < 0.001);
    }

    #[test]
    fn median_even_count() {
        assert!((median(&[1.0, 2.0, 3.0, 4.0]) - 2.5).abs() < 0.001);
    }

    #[test]
    fn median_single() {
        assert!((median(&[7.0]) - 7.0).abs() < 0.001);
    }

    #[test]
    fn median_empty() {
        assert!((median(&[]) - 0.0).abs() < 0.001);
    }

    #[test]
    fn reviewer_duration_and_rubber_stamp() {
        let (_d, mut c) = open_tmp();
        let t1 = seed_task(&mut c, "done", None, 0, Some("rev-a"), 1000, 1600);
        seed_run(&c, t1, "opus-46", "high", 1001);
        seed_reviewer_run(&c, t1, "rev-a", 1500, 1500 + 300); // 300s

        let t2 = seed_task(&mut c, "done", None, 0, Some("rev-a"), 2000, 2600);
        seed_run(&c, t2, "opus-46", "high", 2001);
        seed_reviewer_run(&c, t2, "rev-a", 2500, 2500 + 60); // 60s — rubber stamp

        let r = perf(&c, PerfCut::Default, DM, DE).unwrap();
        assert_eq!(r.rows.len(), 1);
        let row = &r.rows[0];
        assert!(
            (row.avg_reviewer_secs - 180.0).abs() < 0.01,
            "avg of 300+60 = 180"
        );
        assert_eq!(row.rubber_stamp_count, 1, "one review under 120s");
    }

    #[test]
    fn no_reviewer_runs_zero_defaults() {
        let (_d, mut c) = open_tmp();
        seed_task(&mut c, "done", None, 0, None, 1000, 1600);

        let r = perf(&c, PerfCut::Default, DM, DE).unwrap();
        assert!((r.rows[0].avg_reviewer_secs - 0.0).abs() < 0.01);
        assert_eq!(r.rows[0].rubber_stamp_count, 0);
    }

    #[test]
    fn approval_stats_approve_rate_and_blocking() {
        let (_d, mut c) = open_tmp();
        let t1 = seed_task(&mut c, "done", None, 0, None, 1000, 1600);
        seed_run(&c, t1, "opus-46", "high", 1001);
        seed_approval(&c, t1, "approved", 0);

        let t2 = seed_task(&mut c, "done", None, 1, None, 2000, 2600);
        seed_run(&c, t2, "opus-46", "high", 2001);
        seed_approval(&c, t2, "changes", 3);

        let t3 = seed_task(&mut c, "done", None, 0, None, 3000, 3600);
        seed_run(&c, t3, "opus-46", "high", 3001);
        seed_approval(&c, t3, "approved", 0);

        let r = perf(&c, PerfCut::Default, DM, DE).unwrap();
        let row = &r.rows[0];
        // 2 approved out of 3 with approvals
        assert!((row.approve_rate_pct - 66.666).abs() < 0.01);
        // avg blocking: (0+3+0)/3 = 1.0
        assert!((row.avg_blocking - 1.0).abs() < 0.01);
    }

    #[test]
    fn no_approvals_zero_defaults() {
        let (_d, mut c) = open_tmp();
        let t1 = seed_task(&mut c, "done", None, 0, None, 1000, 1600);
        seed_run(&c, t1, "opus-46", "high", 1001);

        let r = perf(&c, PerfCut::Default, DM, DE).unwrap();
        assert!((r.rows[0].approve_rate_pct - 0.0).abs() < 0.01);
        assert!((r.rows[0].avg_blocking - 0.0).abs() < 0.01);
    }

    #[test]
    fn cost_aggregates_from_journal() {
        let (_d, mut c) = open_tmp();
        let t1 = seed_task(&mut c, "done", None, 0, None, 1000, 1600);
        seed_run(&c, t1, "opus-46", "high", 1001);
        seed_journal_cost(&mut c, t1, "w1", 1.50);

        let t2 = seed_task(&mut c, "done", None, 0, None, 2000, 2600);
        seed_run(&c, t2, "opus-46", "high", 2001);
        seed_journal_cost(&mut c, t2, "w2", 0.75);

        let r = perf(&c, PerfCut::Default, DM, DE).unwrap();
        assert!((r.rows[0].total_cost_usd - 2.25).abs() < 0.01);
    }

    #[test]
    fn reviewer_cut_with_review_metrics() {
        let (_d, mut c) = open_tmp();
        let t1 = seed_task(&mut c, "done", None, 0, Some("alice"), 1000, 1600);
        seed_run(&c, t1, "opus-46", "high", 1001);
        seed_reviewer_run(&c, t1, "alice", 1500, 1500 + 400); // 400s
        seed_approval(&c, t1, "approved", 0);

        let t2 = seed_task(&mut c, "done", None, 0, Some("bob"), 2000, 2600);
        seed_run(&c, t2, "opus-46", "high", 2001);
        seed_reviewer_run(&c, t2, "bob", 2500, 2500 + 31); // 31s rubber stamp
        seed_approval(&c, t2, "approved", 0);

        let r = perf(&c, PerfCut::Reviewer, DM, DE).unwrap();

        let alice = r
            .rows
            .iter()
            .find(|r| r.reviewer.as_deref() == Some("alice"))
            .unwrap();
        assert!((alice.avg_reviewer_secs - 400.0).abs() < 0.01);
        assert_eq!(alice.rubber_stamp_count, 0);
        assert!((alice.approve_rate_pct - 100.0).abs() < 0.01);

        let bob = r
            .rows
            .iter()
            .find(|r| r.reviewer.as_deref() == Some("bob"))
            .unwrap();
        assert!((bob.avg_reviewer_secs - 31.0).abs() < 0.01);
        assert_eq!(bob.rubber_stamp_count, 1);
    }

    // ── watermark boundary tests (#158) ─────────────────────────────────

    #[test]
    fn watermark_excludes_historical_tasks() {
        let (_d, mut c) = open_tmp();
        // Set watermark to 5000: only tasks with updated_at >= 5000 are eligible.
        c.execute(
            "UPDATE perf_watermark SET watermark = 5000 WHERE id = 1",
            [],
        )
        .unwrap();

        // Historical task (updated_at=1600 < 5000) — must be excluded.
        let t1 = seed_task(&mut c, "done", None, 0, None, 1000, 1600);
        seed_run(&c, t1, "opus-46", "high", 1001);
        // Post-rollout task (updated_at=6000 >= 5000) — must be included.
        let t2 = seed_task(&mut c, "done", None, 0, None, 5000, 6000);
        seed_run(&c, t2, "opus-46", "high", 5001);

        let r = perf(&c, PerfCut::Default, DM, DE).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].n_tasks, 1, "only post-watermark task counted");
    }

    #[test]
    fn watermark_all_flag_includes_historical() {
        let (_d, mut c) = open_tmp();
        c.execute(
            "UPDATE perf_watermark SET watermark = 5000 WHERE id = 1",
            [],
        )
        .unwrap();

        let t1 = seed_task(&mut c, "done", None, 0, None, 1000, 1600);
        seed_run(&c, t1, "opus-46", "high", 1001);
        let t2 = seed_task(&mut c, "done", None, 0, None, 5000, 6000);
        seed_run(&c, t2, "opus-46", "high", 5001);

        let r = perf_with(&c, PerfCut::Default, DM, DE, true).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0].n_tasks, 2, "--all must include both tasks");
    }

    /// Negative-path: if the watermark boundary filter were removed, this
    /// test would fail — historical tasks would reappear in the default report.
    #[test]
    fn watermark_negative_path_regression_guard() {
        let (_d, mut c) = open_tmp();
        c.execute(
            "UPDATE perf_watermark SET watermark = 9000 WHERE id = 1",
            [],
        )
        .unwrap();

        // All tasks are historical (updated_at < 9000).
        seed_task(&mut c, "done", None, 0, None, 1000, 1600);
        seed_task(&mut c, "failed", None, 0, None, 2000, 3000);
        seed_task(&mut c, "cancelled", None, 0, None, 4000, 5000);

        let r = perf(&c, PerfCut::Default, DM, DE).unwrap();
        assert!(
            r.rows.is_empty(),
            "default perf must return no rows when all tasks predate the watermark"
        );
    }

    #[test]
    fn watermark_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");
        {
            let c = crate::db::open(&path).unwrap();
            let wm = read_watermark(&c).unwrap();
            assert!(wm.is_some(), "watermark must exist after migration");
            assert!(wm.unwrap() > 0, "watermark must be a real timestamp");
        }
        // Reopen — watermark persists.
        let c = crate::db::open(&path).unwrap();
        let wm = read_watermark(&c).unwrap();
        assert!(wm.is_some(), "watermark must survive reopen");
        assert!(wm.unwrap() > 0);
    }

    // ── facts scaffold tests (perf-facts-v1) ────────────────────────────

    /// Snapshot per-table row counts and PRAGMA data_version, so a "no writes"
    /// assertion catches both row-level changes and hidden schema/pragma
    /// mutations without depending on log strings.
    fn snapshot_db_state(conn: &Connection) -> (BTreeMap<String, i64>, i64) {
        let mut stmt = conn
            .prepare(
                "SELECT name FROM sqlite_master \
                 WHERE type = 'table' AND name NOT LIKE 'sqlite_%' \
                 ORDER BY name",
            )
            .unwrap();
        let tables: Vec<String> = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        let mut counts = BTreeMap::new();
        for t in tables {
            let n: i64 = conn
                .query_row(&format!("SELECT COUNT(*) FROM \"{t}\""), [], |r| r.get(0))
                .unwrap();
            counts.insert(t, n);
        }
        let data_version: i64 = conn
            .query_row("PRAGMA data_version", [], |r| r.get(0))
            .unwrap();
        (counts, data_version)
    }

    fn seed_ordinary(conn: &mut Connection, updated_at: i64) -> i64 {
        let id = seed_task(conn, "open", None, 0, None, 1000, updated_at);
        crate::tasks::close_after_merge_with_merge_commit_sha(
            conn,
            id,
            "merged for facts fixture",
            &format!("{id:040x}"),
            updated_at,
        )
        .unwrap();
        id
    }

    fn seed_review_only(conn: &mut Connection, updated_at: i64) -> i64 {
        let tx = crate::db::begin_immediate(conn).unwrap();
        tx.execute(
            "INSERT INTO tasks(title, body, status, priority, labels, assignee, created_by, \
             created_at, updated_at, refs, depends_on, author, reviewer, rework_round, review_only) \
             VALUES ('rev', NULL, 'done', 0, NULL, NULL, 'boss', 1000, ?1, NULL, NULL, 'worker', NULL, 0, 1)",
            rusqlite::params![updated_at],
        )
        .unwrap();
        let id = tx.last_insert_rowid();
        tx.commit().unwrap();
        id
    }

    #[test]
    fn facts_version_is_v1() {
        let (_d, c) = open_tmp();
        let r = perf_facts(&c, false).unwrap();
        assert_eq!(r.facts_version, "perf-facts-v1");
    }

    #[test]
    fn facts_empty_report_declares_full_coverage_schema() {
        let (_d, c) = open_tmp();
        let r = perf_facts(&c, false).unwrap();
        assert_eq!(r.counts, CohortCounts::default());
        assert!(r.intents.is_empty());
        // Every declared coverage field is present even with no intents, so
        // consumers can rely on a stable field schema.
        let expected: Vec<&str> = IntentCoverage::default()
            .iter_named()
            .iter()
            .map(|(n, _)| *n)
            .collect();
        for name in expected {
            assert!(
                r.coverage.fields.contains_key(name),
                "coverage field {name} must be present in empty report"
            );
        }
    }

    #[test]
    fn facts_intent_identity_one_per_ordinary_task() {
        let (_d, mut c) = open_tmp();
        let t1 = seed_ordinary(&mut c, 1600);
        let t2 = seed_ordinary(&mut c, 1700);

        let r = perf_facts(&c, false).unwrap();
        assert_eq!(r.intents.len(), 2);
        assert_eq!(r.counts.candidate, 2);
        assert_eq!(r.counts.included, 2);
        assert_eq!(r.counts.excluded, 0);

        let ids: Vec<i64> = r
            .intents
            .iter()
            .map(|i| i.contributing_task_ids[0])
            .collect();
        assert_eq!(ids, vec![t1, t2]);
        assert_eq!(r.intents[0].intent_id, format!("intent-{t1}"));
        assert_eq!(r.intents[1].intent_id, format!("intent-{t2}"));
        for intent in &r.intents {
            assert!(intent.included);
            assert_eq!(intent.reason, InclusionReason::IncludedVerifiedMerge);
            assert_eq!(intent.contributing_task_ids.len(), 1);
        }
    }

    #[test]
    fn facts_same_task_rework_stays_one_intent() {
        // Ordinary same-task rework increments rework_round on the same row —
        // there is still only one task, hence one intent.
        let (_d, mut c) = open_tmp();
        let tid = seed_task(&mut c, "done", None, 3, None, 1000, 1600);
        seed_run(&c, tid, "opus-46", "high", 1001);

        let r = perf_facts(&c, false).unwrap();
        assert_eq!(r.intents.len(), 1);
        assert_eq!(r.intents[0].contributing_task_ids, vec![tid]);
    }

    #[test]
    fn facts_sibling_owned_evidence_null_and_coverage_false() {
        // A standalone task carries no durable collapse relation — its
        // lineage_evidence must remain JSON null with coverage.lineage=false
        // so consumers can distinguish an unavailable lineage from a
        // measured one. Every other evidence field is null with its coverage
        // flag false as well, awaiting sibling enrichment.
        let (_d, mut c) = open_tmp();
        let tid = seed_ordinary(&mut c, 1600);
        seed_run(&c, tid, "opus-46", "high", 1001);
        seed_approval(&c, tid, "approved", 0);

        let r = perf_facts(&c, false).unwrap();
        assert_eq!(r.intents.len(), 1);
        let i = &r.intents[0];
        // Standalone lineage is an explicit coverage gap.
        assert!(i.lineage_root_task_id.is_none());
        assert!(i.lineage_evidence.is_none());
        assert!(
            !i.coverage.lineage,
            "standalone lineage must remain an explicit coverage gap"
        );
        // Terminal evidence comes from the durable merged completion. The
        // remaining enrichment fields retain their explicit null coverage gaps.
        assert_eq!(i.terminal_outcome.as_deref(), Some("done"));
        assert!(i.terminal_evidence.is_some());
        assert_eq!(i.merge_provenance.as_deref(), Some("merged"));
        assert!(i.complexity.is_none());
        assert!(i.complexity_provenance.is_none());
        assert!(i.config_evidence.is_none());
        assert!(i.final_worker.is_none());
        assert!(i.contributing_attempts.is_none());
        assert!(i.role_tokens_usd.is_none());
        assert!(i.active_model_secs.is_none());
        assert!(i.wall_secs.is_none());
        assert!(i.rework_count.is_none());
        assert!(i.recovery_count.is_none());
        assert!(i.replan_count.is_none());
        assert!(i.incident_count.is_none());
        assert!(i.review_quality.is_none());
        // Terminal and merge-provenance are resolved here; all enrichment
        // siblings remain uncovered.
        for (name, covered) in i.coverage.iter_named() {
            assert_eq!(
                covered,
                matches!(name, "terminal" | "merge_provenance"),
                "coverage.{name}"
            );
        }
        // Coverage summary reflects the two resolved fields precisely.
        for (name, fc) in &r.coverage.fields {
            let expected_covered =
                i64::from(matches!(name.as_str(), "terminal" | "merge_provenance"));
            assert_eq!(fc.covered, expected_covered, "field {name} covered count");
            assert_eq!(
                fc.uncovered,
                1 - expected_covered,
                "field {name} uncovered count"
            );
        }
        // Serialization proves null lineage is on the wire — enrichment
        // consumers cannot mistake it for a measured value.
        let wire = serde_json::to_value(i).unwrap();
        assert_eq!(
            wire.get("lineage_evidence").unwrap(),
            &serde_json::Value::Null
        );
        assert_eq!(
            wire.get("lineage_root_task_id").unwrap(),
            &serde_json::Value::Null
        );
    }

    #[test]
    fn facts_json_null_distinct_from_measured_zero() {
        // Serialize a scaffold intent and one with an explicit measured zero,
        // verifying null is distinguishable from a real 0.
        let (_d, mut c) = open_tmp();
        seed_ordinary(&mut c, 1600);
        let r = perf_facts(&c, false).unwrap();
        let unpop = serde_json::to_value(&r.intents[0]).unwrap();
        assert_eq!(unpop.get("rework_count").unwrap(), &serde_json::Value::Null);

        let mut zeroed = r.intents[0].clone_for_test();
        zeroed.rework_count = Some(0);
        zeroed.coverage.rework = true;
        let pop = serde_json::to_value(&zeroed).unwrap();
        assert_eq!(pop.get("rework_count").unwrap(), &serde_json::json!(0));
    }

    #[test]
    fn facts_review_only_excluded_by_reason_code() {
        let (_d, mut c) = open_tmp();
        seed_ordinary(&mut c, 1600);
        seed_review_only(&mut c, 1700);

        let r = perf_facts(&c, false).unwrap();
        assert_eq!(r.counts.candidate, 2);
        assert_eq!(r.counts.included, 1);
        assert_eq!(r.counts.excluded, 1);
        assert_eq!(
            r.excluded_reasons.get("excluded-review-only").copied(),
            Some(1)
        );
    }

    #[test]
    fn facts_prospective_by_default_include_all_bypasses() {
        let (_d, mut c) = open_tmp();
        c.execute(
            "UPDATE perf_watermark SET watermark = 5000 WHERE id = 1",
            [],
        )
        .unwrap();
        seed_ordinary(&mut c, 1600); // historical, pre-watermark
        seed_ordinary(&mut c, 6000); // post-watermark

        let default = perf_facts(&c, false).unwrap();
        assert!(default.cohort.prospective_only);
        assert!(!default.cohort.include_all);
        assert_eq!(default.cohort.watermark, Some(5000));
        assert_eq!(default.counts.candidate, 1);
        assert_eq!(default.counts.included, 1);
        assert_eq!(
            default.intents[0].reason,
            InclusionReason::IncludedVerifiedMerge
        );

        let all = perf_facts(&c, true).unwrap();
        assert!(!all.cohort.prospective_only);
        assert!(all.cohort.include_all);
        assert_eq!(all.cohort.watermark, Some(5000));
        assert_eq!(all.counts.candidate, 2);
        assert_eq!(all.counts.included, 2);
        for intent in &all.intents {
            assert_eq!(intent.reason, InclusionReason::IncludedVerifiedMerge);
        }
    }

    #[test]
    fn facts_deterministic_across_repeated_runs() {
        let (_d, mut c) = open_tmp();
        seed_ordinary(&mut c, 1600);
        seed_ordinary(&mut c, 1700);
        seed_review_only(&mut c, 1800);

        let r1 = perf_facts(&c, false).unwrap();
        let r2 = perf_facts(&c, false).unwrap();
        let r3 = perf_facts(&c, false).unwrap();
        assert_eq!(r1, r2);
        assert_eq!(r2, r3);
    }

    #[test]
    fn facts_no_writes_against_real_sqlite() {
        let (_d, mut c) = open_tmp();
        seed_ordinary(&mut c, 1600);
        seed_ordinary(&mut c, 1700);
        seed_review_only(&mut c, 1800);
        // Extra data across auxiliary tables so the snapshot covers non-tasks
        // state as well.
        let tid = seed_ordinary(&mut c, 1900);
        seed_run(&c, tid, "opus-46", "high", 1901);
        seed_approval(&c, tid, "approved", 0);
        seed_reviewer_run(&c, tid, "r", 1902, 1950);
        seed_journal_cost(&mut c, tid, "w", 1.25);

        let before = snapshot_db_state(&c);
        // Default and include_all — both must be pure reads.
        let _ = perf_facts(&c, false).unwrap();
        let _ = perf_facts(&c, true).unwrap();
        let after = snapshot_db_state(&c);
        assert_eq!(before, after, "perf_facts must not write to any table");
    }

    #[test]
    fn facts_verified_merge_includes_terminal_evidence_and_real_provenance() {
        let (_d, mut c) = open_tmp();
        let task_id = seed_ordinary(&mut c, 1600);
        let expected_sha = format!("{task_id:040x}");

        let before = snapshot_db_state(&c);
        let report = perf_facts(&c, false).unwrap();
        let after = snapshot_db_state(&c);

        assert_eq!(before, after, "facts reads must not mutate SQLite state");
        assert_eq!(
            report.counts,
            CohortCounts {
                candidate: 1,
                included: 1,
                excluded: 0
            }
        );
        let intent = &report.intents[0];
        assert!(intent.included);
        assert_eq!(intent.reason, InclusionReason::IncludedVerifiedMerge);
        assert_eq!(intent.terminal_outcome.as_deref(), Some("done"));
        assert_eq!(intent.merge_provenance.as_deref(), Some("merged"));
        assert!(intent.coverage.terminal);
        assert!(intent.coverage.merge_provenance);
        assert_eq!(
            intent.terminal_evidence.as_ref().unwrap()["merge_commit_shas"],
            serde_json::json!([expected_sha])
        );
    }

    #[test]
    fn facts_excludes_cancellation_manual_close_and_review_without_reducing_delivery() {
        let (_d, mut c) = open_tmp();
        let delivered = seed_ordinary(&mut c, 1600);
        let cancelled = seed_task(&mut c, "cancelled", None, 0, None, 1000, 1700);
        let duplicate = seed_task(&mut c, "cancelled", None, 0, None, 1000, 1710);
        set_refs(
            &c,
            duplicate,
            &serde_json::json!({ "cx_dup_of": [delivered] }).to_string(),
        );
        let intake_declined = seed_task(&mut c, "cancelled", None, 0, None, 1000, 1720);
        set_refs(
            &c,
            intake_declined,
            r#"{"cx_ready":false,"cx_not_ready_reason":"missing owner decision"}"#,
        );
        let manual = seed_task(&mut c, "open", None, 0, None, 1000, 1800);
        crate::tasks::close_manual(&mut c, "owner", manual, "resolved elsewhere", 1800).unwrap();
        let review_only = seed_review_only(&mut c, 1900);

        let report = perf_facts(&c, false).unwrap();
        assert_eq!(
            report.counts,
            CohortCounts {
                candidate: 6,
                included: 1,
                excluded: 5
            }
        );
        assert_eq!(
            report
                .excluded_reasons
                .get(InclusionReason::ExcludedHousekeepingCancellation.as_str()),
            Some(&1)
        );
        assert_eq!(
            report
                .excluded_reasons
                .get(InclusionReason::ExcludedDuplicate.as_str()),
            Some(&1)
        );
        assert_eq!(
            report
                .excluded_reasons
                .get(InclusionReason::ExcludedIntakeDeclined.as_str()),
            Some(&1)
        );
        assert_eq!(
            report
                .excluded_reasons
                .get(InclusionReason::ExcludedManualCompletion.as_str()),
            Some(&1)
        );
        assert_eq!(
            report
                .excluded_reasons
                .get(InclusionReason::ExcludedReviewOnly.as_str()),
            Some(&1)
        );
        let by_task: HashMap<i64, &IntentFacts> = report
            .intents
            .iter()
            .map(|intent| (intent.contributing_task_ids[0], intent))
            .collect();
        assert!(by_task[&delivered].included);
        assert_eq!(
            by_task[&cancelled].reason,
            InclusionReason::ExcludedHousekeepingCancellation
        );
        assert_eq!(
            by_task[&duplicate].reason,
            InclusionReason::ExcludedDuplicate
        );
        assert_eq!(
            by_task[&intake_declined].reason,
            InclusionReason::ExcludedIntakeDeclined
        );
        assert_eq!(
            by_task[&manual].reason,
            InclusionReason::ExcludedManualCompletion
        );
        assert_eq!(
            by_task[&review_only].reason,
            InclusionReason::ExcludedReviewOnly
        );
        assert!(by_task[&manual].merge_provenance.is_none());
    }

    #[test]
    fn facts_includes_only_irrecoverable_failed_terminal() {
        let (_d, mut c) = open_tmp();
        let failed = seed_task(&mut c, "failed", None, 0, None, 1000, 1600);

        let report = perf_facts(&c, false).unwrap();
        assert_eq!(
            report.counts,
            CohortCounts {
                candidate: 1,
                included: 1,
                excluded: 0
            }
        );
        let intent = &report.intents[0];
        assert_eq!(intent.contributing_task_ids, vec![failed]);
        assert!(intent.included);
        assert_eq!(intent.reason, InclusionReason::IncludedIrrecoverableFailure);
        assert_eq!(intent.terminal_outcome.as_deref(), Some("failed"));
        assert!(intent.terminal_evidence.is_some());
        assert!(intent.merge_provenance.is_none());
        assert!(intent.coverage.terminal);
        assert!(!intent.coverage.merge_provenance);
    }

    #[test]
    fn facts_missing_or_contradictory_delivery_evidence_is_unknown() {
        let (_d, mut c) = open_tmp();
        let missing = seed_task(&mut c, "done", None, 0, None, 1000, 1600);
        let contradictory = seed_task(&mut c, "failed", None, 0, None, 1000, 1610);
        c.execute(
            "UPDATE tasks SET completion_provenance='merged', \
             refs=json_object('merge_commit_sha','merge-42') WHERE id=?1",
            [contradictory],
        )
        .unwrap();

        let report = perf_facts(&c, false).unwrap();
        assert_eq!(
            report.counts,
            CohortCounts {
                candidate: 2,
                included: 0,
                excluded: 2
            }
        );
        for intent in &report.intents {
            assert!(
                matches!(intent.contributing_task_ids[0], id if id == missing || id == contradictory)
            );
            assert!(!intent.included);
            assert_eq!(intent.reason, InclusionReason::ExcludedUnknownDelivery);
            assert_eq!(intent.terminal_outcome.as_deref(), Some("unknown"));
            assert!(intent.terminal_evidence.is_some());
            assert!(intent.merge_provenance.is_none());
        }
    }

    #[test]
    fn facts_excludes_active_and_retryable_work_as_nonterminal() {
        let (_d, mut c) = open_tmp();
        let open = seed_task(&mut c, "open", None, 0, None, 1000, 1600);
        let working = seed_task(&mut c, "working", None, 0, None, 1000, 1610);
        let in_review = seed_task(&mut c, "in-review", None, 0, None, 1000, 1620);
        let rework = seed_task(&mut c, "rework", None, 0, None, 1000, 1630);
        let decomposed = seed_task(&mut c, "decomposed", None, 0, None, 1000, 1640);
        seed_decomposition(&c, decomposed, 1);
        let parked = seed_task(&mut c, "failed", None, 0, None, 1000, 1650);
        set_refs(
            &c,
            parked,
            r#"{"daemon_parked":true,"daemon_resume_status":"open"}"#,
        );

        let report = perf_facts(&c, false).unwrap();
        assert_eq!(
            report.counts,
            CohortCounts {
                candidate: 6,
                included: 0,
                excluded: 6
            }
        );
        assert_eq!(
            report
                .excluded_reasons
                .get(InclusionReason::ExcludedNonTerminal.as_str()),
            Some(&6)
        );
        let expected = [open, working, in_review, rework, decomposed, parked];
        for intent in &report.intents {
            assert!(expected.contains(&intent.contributing_task_ids[0]));
            assert!(!intent.included);
            assert_eq!(intent.reason, InclusionReason::ExcludedNonTerminal);
            assert!(intent.terminal_outcome.is_none());
            assert!(intent.terminal_evidence.is_none());
            assert!(!intent.coverage.terminal);
        }
    }

    #[test]
    fn facts_excludes_numeric_parked_failure_accepted_by_task_retry() {
        let (_d, mut c) = open_tmp();
        let parked = seed_task(&mut c, "failed", None, 0, None, 1000, 1650);
        // SQLite's `json_extract(...)=1` task-retry predicate deliberately
        // accepts this retained numeric encoding, not just JSON true.
        set_refs(
            &c,
            parked,
            r#"{"daemon_parked":1,"daemon_resume_status":"open"}"#,
        );

        let report = perf_facts(&c, false).unwrap();
        assert_eq!(
            report.counts,
            CohortCounts {
                candidate: 1,
                included: 0,
                excluded: 1,
            }
        );
        let intent = &report.intents[0];
        assert_eq!(intent.contributing_task_ids, vec![parked]);
        assert!(!intent.included);
        assert_eq!(intent.reason, InclusionReason::ExcludedNonTerminal);
        assert!(intent.terminal_outcome.is_none());
        assert!(intent.terminal_evidence.is_none());

        // Prove the same persisted row remains admitted by the lifecycle,
        // rather than merely duplicating a look-alike JSON predicate here.
        assert!(crate::tasks::retry_parked(
            &mut c,
            parked,
            "retry-owner",
            true,
            crate::clock::now(),
        )
        .unwrap()
        .is_some());
    }

    #[test]
    fn facts_includes_parked_failure_with_cancelled_dependency() {
        let (_d, mut c) = open_tmp();
        let cancelled_dependency = seed_task(&mut c, "cancelled", None, 0, None, 1000, 1600);
        let parked = seed_task(&mut c, "failed", None, 0, None, 1000, 1650);
        c.execute(
            "UPDATE tasks SET depends_on=?2,refs=?3 WHERE id=?1",
            rusqlite::params![
                parked,
                format!("[{cancelled_dependency}]"),
                r#"{"daemon_parked":1,"daemon_resume_status":"open"}"#,
            ],
        )
        .unwrap();

        // Prove the exact lifecycle recovery path refuses this durable
        // dependency state before facts classifies it as irrecoverable.
        assert!(crate::tasks::retry_parked(
            &mut c,
            parked,
            "retry-owner",
            true,
            crate::clock::now(),
        )
        .unwrap()
        .is_none());

        let report = perf_facts(&c, false).unwrap();
        assert_eq!(
            report.counts,
            CohortCounts {
                candidate: 2,
                included: 1,
                excluded: 1,
            }
        );
        let by_task: HashMap<i64, &IntentFacts> = report
            .intents
            .iter()
            .map(|intent| (intent.contributing_task_ids[0], intent))
            .collect();
        let intent = by_task[&parked];
        assert!(intent.included);
        assert_eq!(intent.reason, InclusionReason::IncludedIrrecoverableFailure);
        assert_eq!(intent.terminal_outcome.as_deref(), Some("failed"));
        assert!(intent.terminal_evidence.is_some());
        assert!(intent.merge_provenance.is_none());
        assert_eq!(
            by_task[&cancelled_dependency].reason,
            InclusionReason::ExcludedHousekeepingCancellation
        );
    }

    #[test]
    fn facts_keeps_retryable_review_only_park_nonterminal() {
        let (_d, mut c) = open_tmp();
        let review = seed_review_only(&mut c, 1650);
        c.execute(
            "UPDATE tasks SET status='failed',refs=?2 WHERE id=?1",
            rusqlite::params![
                review,
                r#"{"daemon_parked":true,"daemon_resume_status":"in-review"}"#
            ],
        )
        .unwrap();

        let report = perf_facts(&c, false).unwrap();
        assert_eq!(
            report.counts,
            CohortCounts {
                candidate: 1,
                included: 0,
                excluded: 1,
            }
        );
        let intent = &report.intents[0];
        assert_eq!(intent.contributing_task_ids, vec![review]);
        assert!(!intent.included);
        assert_eq!(intent.reason, InclusionReason::ExcludedReviewOnly);
        assert!(intent.terminal_outcome.is_none());
        assert!(intent.terminal_evidence.is_none());
        assert!(intent.merge_provenance.is_none());

        // This is the lifecycle-compatible pre-review-CI park form, not a
        // look-alike status: retry restores it to the review phase.
        let retried =
            crate::tasks::retry_parked(&mut c, review, "retry-owner", true, crate::clock::now())
                .unwrap()
                .unwrap();
        assert_eq!(retried.status, "in-review");
    }

    #[test]
    fn facts_preserves_durable_terminal_review_only_outcomes() {
        let (_d, mut c) = open_tmp();
        let failed = seed_review_only(&mut c, 1650);
        let cancelled = seed_review_only(&mut c, 1660);
        c.execute(
            "UPDATE tasks SET status='in-review' WHERE id IN (?1,?2)",
            rusqlite::params![failed, cancelled],
        )
        .unwrap();
        assert_eq!(
            crate::tasks::apply_event(
                &mut c,
                "daemon",
                failed,
                &crate::lifecycle::Event::PrFoundClosed,
                1670,
            )
            .unwrap()
            .task
            .status,
            "failed"
        );
        assert_eq!(
            crate::tasks::apply_event(
                &mut c,
                "daemon",
                cancelled,
                &crate::lifecycle::Event::Cancelled {
                    by: "test-owner".to_string(),
                },
                1680,
            )
            .unwrap()
            .task
            .status,
            "cancelled"
        );

        let report = perf_facts(&c, false).unwrap();
        assert_eq!(
            report.counts,
            CohortCounts {
                candidate: 2,
                included: 0,
                excluded: 2,
            }
        );
        let by_task: HashMap<i64, &IntentFacts> = report
            .intents
            .iter()
            .map(|intent| (intent.contributing_task_ids[0], intent))
            .collect();
        for (task_id, outcome) in [(failed, "failed"), (cancelled, "cancelled")] {
            let intent = by_task[&task_id];
            assert!(!intent.included);
            assert_eq!(intent.reason, InclusionReason::ExcludedReviewOnly);
            assert_eq!(intent.terminal_outcome.as_deref(), Some(outcome));
            assert_eq!(
                intent.terminal_evidence.as_ref().unwrap()["tasks"][0]["status"],
                serde_json::json!(outcome)
            );
            assert!(intent.merge_provenance.is_none());
        }
    }

    #[test]
    fn facts_includes_merge_conflict_rework_cap_failure_with_stale_retry_marker() {
        let (_d, mut c) = open_tmp();
        let task = seed_task(
            &mut c,
            "merging",
            None,
            i64::from(crate::lifecycle::REWORK_CAP),
            Some("reviewer"),
            1000,
            1650,
        );
        set_refs(&c, task, r#"{"pr":419,"daemon_merge_retry":"attempting"}"#);

        // This durable path intentionally retains the marker after the
        // transition reaches `failed`; only `rework` is lifecycle-admitted
        // for its remediation retry.
        let transition = crate::tasks::rework_approved_merge(
            &mut c,
            task,
            419,
            "merge conflict at rework cap",
            1660,
        )
        .unwrap();
        assert_eq!(transition.task.status, "failed");
        let refs: serde_json::Value =
            serde_json::from_str(transition.task.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["daemon_rework_retry_requested"], true);

        let report = perf_facts(&c, false).unwrap();
        assert_eq!(
            report.counts,
            CohortCounts {
                candidate: 1,
                included: 1,
                excluded: 0,
            }
        );
        let intent = &report.intents[0];
        assert_eq!(intent.contributing_task_ids, vec![task]);
        assert!(intent.included);
        assert_eq!(intent.reason, InclusionReason::IncludedIrrecoverableFailure);
        assert_eq!(intent.terminal_outcome.as_deref(), Some("failed"));
        assert_eq!(
            intent.terminal_evidence.as_ref().unwrap()["tasks"][0]["status"],
            serde_json::json!("failed")
        );
        assert!(intent.merge_provenance.is_none());
    }

    #[test]
    fn facts_completed_decomposition_uses_merged_child_provenance() {
        let (_d, mut c) = open_tmp();
        let source = seed_task(&mut c, "decomposed", None, 0, None, 1000, 1600);
        let first_child = seed_task(&mut c, "open", None, 0, None, 1000, 1610);
        let final_child = seed_task(&mut c, "open", None, 0, None, 1000, 1620);
        let graph = seed_decomposition(&c, source, 1);
        seed_graph_member(&c, graph, first_child, "first", 1);
        seed_graph_member(&c, graph, final_child, "final", 1);

        let first_sha = format!("{first_child:040x}");
        let final_sha = format!("{final_child:040x}");
        assert!(crate::tasks::close_after_merge_with_merge_commit_sha(
            &mut c,
            first_child,
            "first merged child",
            &first_sha,
            1630,
        )
        .unwrap());
        assert!(crate::tasks::close_after_merge_with_merge_commit_sha(
            &mut c,
            final_child,
            "final merged child",
            &final_sha,
            1640,
        )
        .unwrap());

        let completed: (String, i64, String, Option<String>) = c
            .query_row(
                "SELECT d.state,d.active,t.status,t.completion_provenance
                 FROM task_decompositions d JOIN tasks t ON t.id=d.source_task_id
                 WHERE d.id=?1",
                [graph],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(completed, ("completed".into(), 0, "done".into(), None));

        let report = perf_facts(&c, false).unwrap();
        assert_eq!(
            report.counts,
            CohortCounts {
                candidate: 1,
                included: 1,
                excluded: 0,
            }
        );
        let intent = &report.intents[0];
        assert!(intent.included);
        assert_eq!(intent.reason, InclusionReason::IncludedVerifiedMerge);
        assert_eq!(intent.terminal_outcome.as_deref(), Some("done"));
        assert_eq!(intent.merge_provenance.as_deref(), Some("merged"));
        assert_eq!(
            intent.terminal_evidence.as_ref().unwrap()["merge_commit_shas"],
            serde_json::json!([first_sha, final_sha])
        );
    }

    #[test]
    fn facts_completed_graph_with_overflow_member_is_not_credited_partially() {
        let (_d, mut c) = open_tmp();
        let source = seed_task(&mut c, "decomposed", None, 0, None, 1000, 1600);
        let merged_child = seed_task(&mut c, "open", None, 0, None, 1000, 1610);

        // Fill the raw facts cohort through MAX_INTENTS. The accepted manual
        // child below then lands exactly in the overflow tail.
        let tx = crate::db::begin_immediate(&mut c).unwrap();
        for _ in 0..(MAX_INTENTS - 2) {
            tx.execute(
                "INSERT INTO tasks(title,status,created_by,created_at,updated_at) \
                 VALUES ('filler','open','boss',1000,1620)",
                [],
            )
            .unwrap();
        }
        tx.commit().unwrap();
        let manual_child = seed_task(&mut c, "open", None, 0, None, 1000, 1630);

        let graph = seed_decomposition(&c, source, 1);
        seed_graph_member(&c, graph, merged_child, "merged", 1);
        seed_graph_member(&c, graph, manual_child, "manual-overflow", 1);
        assert!(crate::tasks::close_manual(
            &mut c,
            "manual-owner",
            manual_child,
            "manual delivery fixture",
            1640,
        )
        .unwrap()
        .is_some());
        let merged_sha = format!("{merged_child:040x}");
        assert!(crate::tasks::close_after_merge_with_merge_commit_sha(
            &mut c,
            merged_child,
            "merged child",
            &merged_sha,
            1650,
        )
        .unwrap());

        let completion: (String, i64, String, Option<String>) = c
            .query_row(
                "SELECT d.state,d.active,t.status,t.completion_provenance
                 FROM task_decompositions d JOIN tasks t ON t.id=d.source_task_id
                 WHERE d.id=?1",
                [graph],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(completion, ("completed".into(), 0, "done".into(), None));
        let manual_provenance: String = c
            .query_row(
                "SELECT completion_provenance FROM tasks WHERE id=?1",
                [manual_child],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            manual_provenance,
            crate::tasks::COMPLETION_PROVENANCE_MANUAL
        );

        let report = perf_facts(&c, false).unwrap();
        assert_eq!(
            report.counts,
            CohortCounts {
                candidate: MAX_INTENTS as i64,
                included: 0,
                excluded: MAX_INTENTS as i64,
            }
        );
        let graph_intent = report
            .intents
            .iter()
            .find(|intent| intent.intent_id == format!("intent-{source}"))
            .unwrap();
        assert_eq!(
            graph_intent.contributing_task_ids,
            vec![source, merged_child]
        );
        assert!(!graph_intent.included);
        assert_eq!(graph_intent.reason, InclusionReason::ExcludedTruncated);
        assert_eq!(graph_intent.terminal_outcome.as_deref(), Some("unknown"));
        assert!(graph_intent.terminal_evidence.is_some());
        assert!(graph_intent.merge_provenance.is_none());
        assert_eq!(
            graph_intent.lineage_evidence.as_ref().unwrap()["generated_child_task_ids"],
            serde_json::json!([merged_child, manual_child])
        );
        // One exclusion is the incomplete graph and one is its omitted raw
        // candidate; neither is silently promoted to verified delivery.
        assert_eq!(
            report
                .excluded_reasons
                .get(InclusionReason::ExcludedTruncated.as_str()),
            Some(&2)
        );
    }

    #[test]
    fn facts_includes_completed_explicit_recovery_graph_with_ledger_witness() {
        let (_d, mut c) = open_tmp();
        const PR: i64 = 526;
        const ORIGINAL_HEAD: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        const RECOVERY_HEAD: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

        let source = seed_task(&mut c, "decomposed", None, 0, None, 1, 10);
        let original = seed_task(&mut c, "failed", None, 0, None, 2, 20);
        let graph = seed_decomposition(&c, source, 1);
        seed_graph_member(&c, graph, original, "failed-child", 1);
        set_refs(&c, original, &serde_json::json!({ "pr": PR }).to_string());
        c.execute(
            "INSERT INTO pr_targets(task_id,pr_number,head_ref,head_sha,is_fork,resolved_at) \
             VALUES (?1,?2,'daemon/recovery',?3,0,8)",
            rusqlite::params![original, PR, ORIGINAL_HEAD],
        )
        .unwrap();

        // This recovery intentionally has no top-level merge_commit_sha. The
        // durable explicit-adoption ledger, written below by the lifecycle,
        // is the only recovery merge witness the facts reader may use.
        let recovery = seed_task(&mut c, "done", None, 0, None, 9, 40);
        set_refs(
            &c,
            recovery,
            &serde_json::json!({ "pr": PR, "source_task": original }).to_string(),
        );
        c.execute(
            "UPDATE tasks SET completion_provenance=?2,continue_pr=?3 WHERE id=?1",
            rusqlite::params![recovery, crate::tasks::COMPLETION_PROVENANCE_MERGED, PR],
        )
        .unwrap();
        c.execute(
            "INSERT INTO pr_targets(task_id,pr_number,head_ref,head_sha,is_fork,resolved_at) \
             VALUES (?1,?2,'daemon/recovery',?3,0,25)",
            rusqlite::params![recovery, PR, RECOVERY_HEAD],
        )
        .unwrap();

        c.execute(
            "INSERT INTO role_assignments(
                 responsibility_key,task_id,pr_number,role,review_stage,complexity,
                 profile_id,provider,runner,model,effort,pool_key,policy_generation,created_at)
             VALUES (?1,?2,NULL,'worker',NULL,'M','worker','codex','codex','sol','high',
                     'worker','test',9)",
            rusqlite::params![format!("worker:task:{recovery}:revision:1"), recovery],
        )
        .unwrap();
        let worker_assignment = c.last_insert_rowid();
        c.execute(
            "INSERT INTO agent_runs(task_id,agent_name,role,model,effort,provider,
                 role_assignment_id,spawned_at,ended_at,end_reason)
             VALUES (?1,'worker','worker','sol','high','codex',?2,10,20,'completed')",
            rusqlite::params![recovery, worker_assignment],
        )
        .unwrap();
        c.execute(
            "INSERT INTO role_assignments(
                 responsibility_key,task_id,pr_number,role,review_stage,complexity,
                 profile_id,provider,runner,model,effort,pool_key,policy_generation,created_at)
             VALUES (?1,?2,?3,'reviewer','r1','M','reviewer','codex','codex','sol','high',
                     'reviewer','test',26)",
            rusqlite::params![format!("reviewer:task:{recovery}:r1"), recovery, PR],
        )
        .unwrap();
        let reviewer_assignment = c.last_insert_rowid();
        c.execute(
            "INSERT INTO agent_runs(task_id,agent_name,role,model,effort,provider,
                 role_assignment_id,spawned_at,ended_at,end_reason,review_cap_run_id,
                 review_pr,review_head_sha)
             VALUES (?1,'reviewer','reviewer','sol','high','codex',?2,26,35,
                     'verdict:approved','review-cap',?3,?4)",
            rusqlite::params![recovery, reviewer_assignment, PR, RECOVERY_HEAD],
        )
        .unwrap();
        c.execute(
            "INSERT INTO r2_sampling_decisions(pr_number,head_sha,task_id,required,created_at)
             VALUES (?1,?2,?3,0,25)",
            rusqlite::params![PR, RECOVERY_HEAD, recovery],
        )
        .unwrap();

        assert!(crate::decomposition::adopt_explicit_recovery_delivery(
            &mut c,
            &crate::decomposition::ExplicitRecoveryAdoption {
                original_child_id: original,
                recovery_task_id: recovery,
                authorized_by: "test-operator",
                now: 50,
            },
        )
        .unwrap());
        let completion: (String, i64, String, Option<String>) = c
            .query_row(
                "SELECT d.state,d.active,t.status,t.completion_provenance
                 FROM task_decompositions d JOIN tasks t ON t.id=d.source_task_id
                 WHERE d.id=?1",
                [graph],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(completion, ("completed".into(), 0, "done".into(), None));
        let has_top_level_witness: Option<String> = c
            .query_row(
                "SELECT json_extract(refs,'$.merge_commit_sha') FROM tasks WHERE id=?1",
                [original],
                |row| row.get(0),
            )
            .unwrap();
        assert!(has_top_level_witness.is_none());

        let report = perf_facts(&c, false).unwrap();
        assert_eq!(
            report.counts,
            CohortCounts {
                candidate: 1,
                included: 1,
                excluded: 0,
            }
        );
        let intent = &report.intents[0];
        assert_eq!(
            intent.contributing_task_ids,
            vec![source, original, recovery]
        );
        assert!(intent.included);
        assert_eq!(intent.reason, InclusionReason::IncludedVerifiedMerge);
        assert_eq!(intent.terminal_outcome.as_deref(), Some("done"));
        assert_eq!(intent.merge_provenance.as_deref(), Some("merged"));
        assert_eq!(
            intent.terminal_evidence.as_ref().unwrap()["merge_commit_shas"],
            serde_json::json!([RECOVERY_HEAD])
        );
    }

    #[test]
    fn facts_excludes_retry_eligible_held_planning_source_as_nonterminal() {
        let (_d, mut c) = open_tmp();
        let source = seed_task(&mut c, "open", None, 0, None, 1000, 1600);
        let graph = crate::decomposition::begin_planning(
            &mut c,
            &crate::decomposition::BeginPlanning {
                source_task_id: source,
                expected_revision: 1,
                provider: "codex",
                model: "test-model",
                frozen_base_sha: "0123456789abcdef0123456789abcdef01234567",
                now: 1610,
            },
        )
        .unwrap()
        .unwrap();
        assert!(crate::decomposition::record_attempt(
            &mut c,
            graph,
            "provider",
            "test-timeout",
            "first bounded planner timeout",
            1620,
        )
        .unwrap()
        .is_some());
        assert!(crate::decomposition::reacquire_freeze(&mut c, graph, 1630).unwrap());
        assert!(crate::decomposition::set_frozen_phase(
            &mut c,
            graph,
            "freeze-requested",
            "planning",
            None,
            1640,
        )
        .unwrap());
        assert!(crate::decomposition::record_attempt(
            &mut c,
            graph,
            "provider",
            "test-timeout",
            "second bounded planner timeout",
            1650,
        )
        .unwrap()
        .is_some());
        assert!(crate::decomposition::exhausted_planning_retry_is_eligible(
            &c,
            source,
            crate::clock::now(),
        )
        .unwrap());

        let report = perf_facts(&c, false).unwrap();
        assert_eq!(
            report.counts,
            CohortCounts {
                candidate: 1,
                included: 0,
                excluded: 1,
            }
        );
        let intent = &report.intents[0];
        assert_eq!(intent.contributing_task_ids, vec![source]);
        assert!(!intent.included);
        assert_eq!(intent.reason, InclusionReason::ExcludedNonTerminal);
        assert!(intent.terminal_outcome.is_none());
        assert!(intent.terminal_evidence.is_none());
    }

    #[test]
    fn facts_includes_non_retryable_held_failure_with_neutral_canonical_retry() {
        let (_d, mut c) = open_tmp();
        let source = seed_task(&mut c, "open", None, 0, None, 1000, 1600);
        let refs = r#"{"runner_retry":{"provider":"codex","model":"gpt-5","effort":"high","prompt":"finish","turn_kind":"rework","continuation_id":"thread-new","requested":false},"codex_retry_requested":true}"#;
        set_refs(&c, source, refs);
        let graph = crate::decomposition::begin_planning(
            &mut c,
            &crate::decomposition::BeginPlanning {
                source_task_id: source,
                expected_revision: 1,
                provider: "codex",
                model: "test-model",
                frozen_base_sha: "0123456789abcdef0123456789abcdef01234567",
                now: 1610,
            },
        )
        .unwrap()
        .unwrap();

        // Exhaust the initial generation, then exhaust each lifecycle-issued
        // operator retry. Planning never rewrites the canonical retry record.
        let mut now = 1620;
        for generation in 0..=crate::decomposition::MAX_OPERATOR_RETRIES {
            if generation > 0 {
                assert!(crate::decomposition::reacquire_freeze(&mut c, graph, now).unwrap());
                now += 10;
                assert!(crate::decomposition::set_frozen_phase(
                    &mut c,
                    graph,
                    "freeze-requested",
                    "planning",
                    None,
                    now,
                )
                .unwrap());
                now += 10;
            }
            assert!(crate::decomposition::record_attempt(
                &mut c,
                graph,
                "provider",
                "test-timeout",
                "bounded planner timeout",
                now,
            )
            .unwrap()
            .is_some());
            now += 10;
            assert!(crate::decomposition::reacquire_freeze(&mut c, graph, now).unwrap());
            now += 10;
            assert!(crate::decomposition::set_frozen_phase(
                &mut c,
                graph,
                "freeze-requested",
                "planning",
                None,
                now,
            )
            .unwrap());
            now += 10;
            assert!(crate::decomposition::record_attempt(
                &mut c,
                graph,
                "provider",
                "test-timeout",
                "bounded planner timeout",
                now,
            )
            .unwrap()
            .is_some());
            now += 10;
            if generation < crate::decomposition::MAX_OPERATOR_RETRIES {
                assert!(matches!(
                    crate::decomposition::retry_exhausted_planning(
                        &mut c,
                        source,
                        "operator",
                        now,
                    )
                    .unwrap(),
                    crate::decomposition::PlanningRetryOutcome::Retried { .. }
                ));
                now += 10;
            }
        }
        assert!(!crate::decomposition::exhausted_planning_retry_is_eligible(
            &c,
            source,
            crate::clock::now(),
        )
        .unwrap());
        assert_eq!(
            crate::decomposition::retry_exhausted_planning(&mut c, source, "operator", now)
                .unwrap(),
            crate::decomposition::PlanningRetryOutcome::RetryCapExhausted {
                retry_count: crate::decomposition::MAX_OPERATOR_RETRIES,
            }
        );
        let persisted_refs: String = c
            .query_row("SELECT refs FROM tasks WHERE id=?1", [source], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(persisted_refs, refs);

        let report = perf_facts(&c, false).unwrap();
        assert_eq!(
            report.counts,
            CohortCounts {
                candidate: 1,
                included: 1,
                excluded: 0,
            }
        );
        let intent = &report.intents[0];
        assert_eq!(intent.contributing_task_ids, vec![source]);
        assert!(intent.included);
        assert_eq!(intent.reason, InclusionReason::IncludedIrrecoverableFailure);
        assert_eq!(intent.terminal_outcome.as_deref(), Some("failed"));
        assert!(intent.terminal_evidence.is_some());
        assert!(intent.merge_provenance.is_none());
    }

    #[test]
    fn facts_output_is_bounded_by_max_intents() {
        // Verify the API bound is present and respected; use a small local
        // cap by asserting via query_limits — the truncation path itself is a
        // pure LIMIT + counter and does not need MAX_INTENTS-sized fixtures.
        let (_d, c) = open_tmp();
        let r = perf_facts(&c, false).unwrap();
        assert_eq!(r.query_limits.max_intents, MAX_INTENTS);
        assert_eq!(
            r.query_limits.max_contributing_tasks_per_intent,
            MAX_CONTRIBUTING_TASKS_PER_INTENT
        );
        assert!(
            r.intents.len() <= MAX_INTENTS,
            "returned intents must not exceed MAX_INTENTS"
        );
    }

    /// Regression: review-only rows earlier in id order must never consume
    /// the bounded intent scan and silently omit later ordinary tasks. Prior
    /// implementation LIMIT'd on the raw terminal set before filtering
    /// review_only in memory, which could displace ordinary tasks near the
    /// sentinel. Exercised at the SQL level with a small explicit limit so
    /// the ordering pathology is directly observable without a
    /// MAX_INTENTS-sized fixture.
    #[test]
    fn facts_overflow_ordering_review_only_does_not_displace_ordinary() {
        let (_d, mut c) = open_tmp();
        // Five review-only rows with the lowest ids — under a raw-terminal
        // ORDER BY id LIMIT 3, these would fill the scan and hide the
        // ordinary rows entirely.
        let r1 = seed_review_only(&mut c, 1601);
        let r2 = seed_review_only(&mut c, 1602);
        let r3 = seed_review_only(&mut c, 1603);
        let r4 = seed_review_only(&mut c, 1604);
        let r5 = seed_review_only(&mut c, 1605);
        // Three ordinary rows follow.
        let o1 = seed_ordinary(&mut c, 1700);
        let o2 = seed_ordinary(&mut c, 1701);
        let o3 = seed_ordinary(&mut c, 1702);
        assert!(
            r5 < o1,
            "seed order must place review-only ids before ordinary"
        );

        // End-to-end accounting: candidate = 5 + 3, all three ordinary rows
        // are included, while every review-only row remains visible with its
        // bounded exclusion reason.
        let r = perf_facts(&c, false).unwrap();
        assert_eq!(r.counts.candidate, 8);
        assert_eq!(r.counts.included, 3);
        assert_eq!(r.counts.excluded, 5);
        assert_eq!(
            r.excluded_reasons.get("excluded-review-only").copied(),
            Some(5)
        );
        // No spurious truncated tally when we are well under MAX_INTENTS.
        assert!(!r.excluded_reasons.contains_key("excluded-truncated"));
        let included_ids: Vec<i64> = r
            .intents
            .iter()
            .filter(|intent| intent.included)
            .map(|i| i.contributing_task_ids[0])
            .collect();
        assert_eq!(included_ids, vec![o1, o2, o3]);
        // Silence unused-binding warnings for the review-only ids.
        let _ = (r1, r2, r3, r4);
    }

    /// Two-connection WAL regression: the three cohort reads (watermark,
    /// aggregate, bounded id scan) must land in one snapshot so an
    /// intervening daemon lifecycle write cannot produce internally
    /// impossible candidate/included/excluded totals.
    ///
    /// Reader connection opens an unchecked transaction; a second connection
    /// commits a new terminal task in between; `perf_facts` reuses the
    /// caller's snapshot (its `is_autocommit()` path) and must see the
    /// pre-write cohort. After the reader's snapshot ends, a fresh call
    /// must observe the new task.
    #[test]
    fn facts_snapshot_isolates_from_intervening_lifecycle_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");
        // Reader and writer connections against the same WAL database.
        let reader = crate::db::open(&path).unwrap();
        let mut writer = crate::db::open(&path).unwrap();
        // Reset watermark so both connections observe every seeded task.
        reader
            .execute("UPDATE perf_watermark SET watermark = 0 WHERE id = 1", [])
            .unwrap();

        // Pre-seed two ordinary terminal tasks via the writer.
        seed_ordinary(&mut writer, 1600);
        seed_ordinary(&mut writer, 1700);

        // Reader opens a read snapshot; the first SELECT below pins it.
        let tx = reader.unchecked_transaction().unwrap();
        assert!(!tx.is_autocommit(), "read txn must be non-autocommit");

        // Intervening lifecycle write from the writer connection while the
        // reader's snapshot is held. Under WAL, the writer sees its own
        // commit but the reader's snapshot must remain unchanged.
        let injected_before = perf_facts(&tx, false).unwrap();
        assert_eq!(
            injected_before.counts.candidate, 2,
            "snapshot must see two pre-write candidates"
        );
        seed_ordinary(&mut writer, 1800);

        // Second call inside the same snapshot must return the same totals
        // as the first — the caller txn is reused, no new snapshot is
        // established, and the mid-flight write is invisible.
        let after_injected_write = perf_facts(&tx, false).unwrap();
        assert_eq!(
            after_injected_write.counts.candidate, 2,
            "snapshot must not observe the write committed on another connection"
        );
        assert_eq!(after_injected_write.counts.included, 2);
        assert_eq!(after_injected_write.counts.excluded, 0);
        assert_eq!(
            after_injected_write.counts.included + after_injected_write.counts.excluded,
            after_injected_write.counts.candidate,
            "included + excluded must equal candidate"
        );
        assert_eq!(
            after_injected_write.intents.len() as i64,
            after_injected_write.counts.included,
            "intent count must match included accounting"
        );

        // End the read snapshot.
        tx.commit().unwrap();

        // A fresh call outside any caller txn opens its own snapshot and
        // now observes the intervening write.
        let fresh = perf_facts(&reader, false).unwrap();
        assert_eq!(fresh.counts.candidate, 3);
        assert_eq!(fresh.counts.included, 3);
    }

    #[test]
    fn facts_inclusion_reason_is_bounded_enum_not_free_form() {
        // Serializing a code goes through the kebab-case enum discriminants
        // — no free-form text can appear in the wire form.
        for r in [
            InclusionReason::IncludedVerifiedMerge,
            InclusionReason::IncludedIrrecoverableFailure,
            InclusionReason::ExcludedDuplicate,
            InclusionReason::ExcludedIntakeDeclined,
            InclusionReason::ExcludedHousekeepingCancellation,
            InclusionReason::ExcludedReviewOnly,
            InclusionReason::ExcludedManualCompletion,
            InclusionReason::ExcludedUnknownDelivery,
            InclusionReason::ExcludedPreWatermark,
            InclusionReason::ExcludedTruncated,
            InclusionReason::ExcludedNonTerminal,
        ] {
            let v = serde_json::to_value(r).unwrap();
            let s = v.as_str().expect("reason must serialize as a string");
            assert_eq!(s, r.as_str(), "wire form and as_str() must agree");
        }
    }

    // ── decomposition and recovery collapse (test proofs #4 and #5) ─────
    //
    // Sanitized fixture matrix, kept in tests, not production code:
    //
    //   proof #4 — decomposed graph:
    //     source S (terminal ordinary) + generated children c1, c2 (both
    //     linked as active graph_members of S's task_decompositions row).
    //     Expectation: exactly one top-level intent whose contributing_task_ids
    //     are [S, c1, c2] and whose lineage_evidence names the graph. The
    //     children never appear as independent intents. If S itself is
    //     non-terminal, the intent is still one and its members are only the
    //     terminal children.
    //
    //   proof #5a — exact recovery adoption:
    //     original X is an accepted generated child, its immutable daemon
    //     recovery ledger names Y, and both have merged completion provenance.
    //     Y collapses through X into the source intent. Mutable delivery refs
    //     corroborate that ledger evidence but cannot establish it alone.
    //
    //   proof #5b — shared-PR / continuation without provenance:
    //     tasks A and B share a PR reference (refs.$.pr = 42) or a
    //     continuation link (refs.$.continue_pr = 42); NEITHER carries
    //     refs.$.recovery_delivery. They stay as two independent intents
    //     whose lineage evidence is JSON null (coverage.lineage=false).
    //     Title/labels/matching PR/continue_pr never trigger collapse.
    //
    //   proof #5c — forged refs without exact adoption:
    //     a normal task may carry a self-consistent recovery_delivery blob
    //     and merged completion provenance, but stays independent unless the
    //     accepted-member and exact ledger predicates also hold.

    fn seed_decomposition(conn: &Connection, source_task_id: i64, plan_revision: i64) -> i64 {
        conn.execute(
            "INSERT INTO task_decompositions(source_task_id,state,active,freeze_active,\
                 planned_source_revision,plan_revision,accepted_plan_revision,created_at,updated_at)\
             VALUES (?1,'active',1,0,?2,?2,?2,1,1)",
            rusqlite::params![source_task_id, plan_revision],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn seed_graph_member(
        conn: &Connection,
        graph_id: i64,
        task_id: i64,
        local_key: &str,
        plan_revision: i64,
    ) {
        conn.execute(
            "INSERT INTO task_graph_members(graph_id,task_id,local_key,plan_revision,active)\
             VALUES (?1,?2,?3,?4,1)",
            rusqlite::params![graph_id, task_id, local_key, plan_revision],
        )
        .unwrap();
    }

    fn set_refs(conn: &Connection, task_id: i64, refs_json: &str) {
        conn.execute(
            "UPDATE tasks SET refs = ?2 WHERE id = ?1",
            rusqlite::params![task_id, refs_json],
        )
        .unwrap();
    }

    /// Seed the durable evidence written by the daemon's explicit recovery
    /// adoption: accepted generated-child membership is supplied separately,
    /// both completions are daemon-merged, and the immutable recovery ledger
    /// agrees exactly with the persisted delivery fields.
    fn record_explicit_recovery_adoption(
        conn: &Connection,
        graph_id: i64,
        original_task_id: i64,
        recovery_task_id: i64,
    ) {
        let (source_task_id, source_revision, refs): (i64, i64, String) = conn
            .query_row(
                "SELECT source_task_id,planned_source_revision,refs
                 FROM task_decompositions JOIN tasks ON tasks.id=?2
                 WHERE task_decompositions.id=?1",
                rusqlite::params![graph_id, original_task_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        let refs: serde_json::Value = serde_json::from_str(&refs).unwrap();
        let pr = refs["recovery_delivery"]["pr"].as_i64().unwrap();
        let merged_head_sha = refs["recovery_delivery"]["merged_head_sha"]
            .as_str()
            .unwrap();
        conn.execute(
            "UPDATE tasks SET status='done',completion_provenance='merged'
             WHERE id IN (?1,?2)",
            rusqlite::params![original_task_id, recovery_task_id],
        )
        .unwrap();
        let ordinal: i64 = conn
            .query_row(
                "SELECT COALESCE(MAX(ordinal),0)+1 FROM decomposition_attempts
                 WHERE graph_id=?1 AND source_revision=?2 AND kind='recovery'",
                rusqlite::params![graph_id, source_revision],
                |row| row.get(0),
            )
            .unwrap();
        let summary = serde_json::json!({
            "authority": "explicit-operator",
            "authorized_by": "test-operator",
            "decomposition_source": source_task_id,
            "original_child": original_task_id,
            "recovery_task": recovery_task_id,
            "pr": pr,
            "merged_head_sha": merged_head_sha,
        })
        .to_string();
        conn.execute(
            "INSERT INTO decomposition_attempts(graph_id,source_revision,kind,ordinal,
                 retry_generation,reason_code,summary,created_at)
             VALUES (?1,?2,'recovery',?3,0,'explicit-delivery-adoption',?4,1800)",
            rusqlite::params![graph_id, source_revision, ordinal, summary],
        )
        .unwrap();
    }

    #[test]
    fn facts_decomposed_source_and_children_collapse_to_one_intent() {
        // Proof #4. Source S plus two generated children c1, c2 form one
        // top-level intent. Neither child appears as an independent top-level
        // intent. The decomposition parent (S) is folded into the collapsed
        // intent — never emitted as its own separate row.
        let (_d, mut c) = open_tmp();
        let s = seed_ordinary(&mut c, 1600);
        let c1 = seed_ordinary(&mut c, 1650);
        let c2 = seed_ordinary(&mut c, 1700);
        let graph_id = seed_decomposition(&c, s, 1);
        seed_graph_member(&c, graph_id, c1, "child-a", 1);
        seed_graph_member(&c, graph_id, c2, "child-b", 1);

        let r = perf_facts(&c, false).unwrap();
        assert_eq!(r.intents.len(), 1, "one top-level intent for the graph");
        let intent = &r.intents[0];
        assert_eq!(intent.intent_id, format!("intent-{s}"));
        assert_eq!(intent.contributing_task_ids, vec![s, c1, c2]);
        assert_eq!(intent.lineage_root_task_id, Some(s));
        assert!(intent.coverage.lineage);
        let ev = intent.lineage_evidence.as_ref().unwrap();
        assert_eq!(ev.get("kind").unwrap(), &serde_json::json!("decomposed"));
        assert_eq!(ev.get("source_task_id").unwrap(), &serde_json::json!(s));
        assert_eq!(
            ev.get("generated_child_task_ids").unwrap(),
            &serde_json::json!([c1, c2])
        );
        // The graph remains active in this fixture, so the collapsed intent
        // is explicitly nonterminal rather than a guessed delivery.
        assert_eq!(r.counts.candidate, 1);
        assert_eq!(r.counts.included, 0);
        assert_eq!(r.counts.excluded, 1);
        assert_eq!(intent.reason, InclusionReason::ExcludedNonTerminal);
        // No child id ever surfaces as an independent intent_id.
        for i in &r.intents {
            assert_ne!(i.intent_id, format!("intent-{c1}"));
            assert_ne!(i.intent_id, format!("intent-{c2}"));
        }
    }

    #[test]
    fn facts_decomposition_parent_never_emitted_when_only_children_terminal() {
        // Source S is still open (non-terminal); its two children are done.
        // The decomposition parent must never be emitted as an independent
        // row. Result: exactly one collapsed intent rooted at S, with the
        // two terminal children as contributing detail.
        let (_d, mut c) = open_tmp();
        let s = {
            let tx = crate::db::begin_immediate(&mut c).unwrap();
            tx.execute(
                "INSERT INTO tasks(title, body, status, priority, labels, assignee, created_by, \
                 created_at, updated_at, refs, depends_on, author, reviewer, rework_round, review_only) \
                 VALUES ('src', NULL, 'working', 0, NULL, 'A', 'boss', 1000, 1600, NULL, NULL, 'A', NULL, 0, 0)",
                [],
            ).unwrap();
            let id = tx.last_insert_rowid();
            tx.commit().unwrap();
            id
        };
        let c1 = seed_ordinary(&mut c, 1650);
        let c2 = seed_ordinary(&mut c, 1700);
        let graph_id = seed_decomposition(&c, s, 1);
        seed_graph_member(&c, graph_id, c1, "child-a", 1);
        seed_graph_member(&c, graph_id, c2, "child-b", 1);

        let r = perf_facts(&c, false).unwrap();
        assert_eq!(r.intents.len(), 1);
        let intent = &r.intents[0];
        assert_eq!(intent.intent_id, format!("intent-{s}"));
        // Facts surfaces the still-working source as nonterminal while the
        // children remain folded under its durable graph root.
        assert_eq!(intent.contributing_task_ids, vec![s, c1, c2]);
        assert_eq!(intent.lineage_root_task_id, Some(s));
        assert!(!intent.included);
        assert_eq!(intent.reason, InclusionReason::ExcludedNonTerminal);
    }

    #[test]
    fn facts_exact_recovery_collapses_while_shared_pr_does_not() {
        // Proof #5. Two lineages, both terminal, side by side:
        //   (A) exact durable recovery adoption — X carries
        //       refs.$.recovery_delivery = {source_task: X, recovery_task: Y}.
        //       Y collapses into X's intent.
        //   (B) shared-PR / continue_pr without provenance — A and B share
        //       refs.$.pr / refs.$.continue_pr but neither carries
        //       refs.$.recovery_delivery. They stay independent.
        let (_d, mut c) = open_tmp();

        // Lineage A: exact recovery adoption. The original is an accepted
        // generated child, and the daemon-owned recovery ledger agrees with
        // the merged delivery fields — refs alone are never enough.
        let source_s = seed_ordinary(&mut c, 1550);
        let orig_x = seed_ordinary(&mut c, 1600);
        let recovery_y = seed_ordinary(&mut c, 1650);
        let graph_id = seed_decomposition(&c, source_s, 1);
        seed_graph_member(&c, graph_id, orig_x, "child-x", 1);
        let x_refs = serde_json::json!({
            "recovery_delivery": {
                "source_task": orig_x,
                "recovery_task": recovery_y,
                "pr": 42,
                "merged_head_sha": "abc123",
                "adopted_at": 1650,
            }
        })
        .to_string();
        set_refs(&c, orig_x, &x_refs);
        record_explicit_recovery_adoption(&c, graph_id, orig_x, recovery_y);

        // Lineage B: shared PR / continue_pr but no recovery_delivery.
        let shared_a = seed_ordinary(&mut c, 1700);
        let shared_b = seed_ordinary(&mut c, 1750);
        let a_refs = serde_json::json!({ "pr": 99 }).to_string();
        let b_refs = serde_json::json!({ "continue_pr": 99, "pr": 99 }).to_string();
        set_refs(&c, shared_a, &a_refs);
        set_refs(&c, shared_b, &b_refs);

        let r = perf_facts(&c, false).unwrap();
        // Expected intents: one collapsed (source_s + orig_x + recovery_y), and two
        // independent (shared_a, shared_b) — total 3.
        assert_eq!(r.intents.len(), 3, "recovery folds, shared-PR does not");
        assert_eq!(r.counts.candidate, 3);
        assert_eq!(r.counts.included, 0);
        assert_eq!(r.counts.excluded, 3);

        // Find each intent by intent_id.
        let by_id: HashMap<&str, &IntentFacts> = r
            .intents
            .iter()
            .map(|i| (i.intent_id.as_str(), i))
            .collect();

        // The collapsed recovery intent.
        let collapsed_id = format!("intent-{source_s}");
        let collapsed = by_id.get(collapsed_id.as_str()).unwrap();
        assert_eq!(
            collapsed.contributing_task_ids,
            vec![source_s, orig_x, recovery_y]
        );
        assert_eq!(collapsed.lineage_root_task_id, Some(source_s));
        assert!(collapsed.coverage.lineage);
        let ev = collapsed.lineage_evidence.as_ref().unwrap();
        assert_eq!(
            ev.get("kind").unwrap(),
            &serde_json::json!("decomposed+recovery")
        );
        let pairs = ev.get("recovery_pairs").unwrap().as_array().unwrap();
        assert_eq!(pairs.len(), 1);
        assert_eq!(
            pairs[0].get("original_task_id").unwrap(),
            &serde_json::json!(orig_x)
        );
        assert_eq!(
            pairs[0].get("recovery_task_id").unwrap(),
            &serde_json::json!(recovery_y)
        );
        assert_eq!(pairs[0].get("pr_number").unwrap(), &serde_json::json!(42));
        assert_eq!(
            pairs[0].get("merged_head_sha").unwrap(),
            &serde_json::json!("abc123")
        );

        // The shared-PR / continuation tasks stay independent — never fold.
        let shared_a_intent = by_id.get(format!("intent-{shared_a}").as_str()).unwrap();
        let shared_b_intent = by_id.get(format!("intent-{shared_b}").as_str()).unwrap();
        assert_eq!(shared_a_intent.contributing_task_ids, vec![shared_a]);
        assert_eq!(shared_b_intent.contributing_task_ids, vec![shared_b]);
        // Their lineage is an explicit coverage gap — never inferred from a
        // shared PR or a continue_pr link.
        assert!(shared_a_intent.lineage_evidence.is_none());
        assert!(shared_a_intent.lineage_root_task_id.is_none());
        assert!(!shared_a_intent.coverage.lineage);
        assert!(shared_b_intent.lineage_evidence.is_none());
        assert!(shared_b_intent.lineage_root_task_id.is_none());
        assert!(!shared_b_intent.coverage.lineage);

        // The recovery task Y never appears as its own top-level intent.
        assert!(
            !by_id.contains_key(format!("intent-{recovery_y}").as_str()),
            "recovery task must not surface as a top-level intent"
        );
    }

    #[test]
    fn facts_recovery_delivery_ignored_when_provenance_malformed() {
        // A refs.$.recovery_delivery whose source_task disagrees with the row
        // it lives on, or whose recovery_task is missing, must NOT be used
        // to collapse anything — partial provenance is treated as unknown,
        // never guessed. The exact ledger predicate includes these fields,
        // so a malformed row cannot become a recovery mapping.
        let (_d, mut c) = open_tmp();
        let a = seed_ordinary(&mut c, 1600);
        let b = seed_ordinary(&mut c, 1650);
        // Row `a` claims a source_task other than itself — inconsistent.
        let bogus = serde_json::json!({
            "recovery_delivery": {
                "source_task": 999_999,
                "recovery_task": b,
            }
        })
        .to_string();
        set_refs(&c, a, &bogus);
        // A malformed blob has no matching immutable adoption ledger entry,
        // so it is rejected as unknown lineage rather than guessed.

        let r = perf_facts(&c, false).unwrap();
        // Neither task collapses — two independent top-level intents, both
        // with an explicit lineage coverage gap.
        assert_eq!(r.intents.len(), 2);
        let ids: Vec<i64> = r
            .intents
            .iter()
            .map(|i| i.contributing_task_ids[0])
            .collect();
        assert_eq!(ids, vec![a, b]);
        for intent in &r.intents {
            assert!(
                intent.lineage_evidence.is_none(),
                "malformed provenance must not surface as lineage"
            );
            assert!(intent.lineage_root_task_id.is_none());
            assert!(!intent.coverage.lineage);
        }
    }

    /// Negative-path proof for finding 1: ordinary daemon-merged completion
    /// is not recovery adoption. An agent may write a self-consistent
    /// `$.recovery_delivery` object before normal merge finalization, so the
    /// immutable accepted-child relation plus explicit adoption ledger are
    /// both required to make the mutable refs relevant at all.
    #[test]
    fn facts_ordinary_merged_recovery_forgery_does_not_collapse() {
        let (_d, mut c) = open_tmp();
        let forger = seed_ordinary(&mut c, 1600);
        let victim = seed_ordinary(&mut c, 1650);
        // Self-consistent agent-writable shape plus the same daemon-owned
        // merged completion a normal task receives. This must still not
        // induce a collapse because neither task is an accepted graph child
        // with a matching explicit adoption ledger entry.
        let forged = serde_json::json!({
            "recovery_delivery": {
                "source_task": forger,
                "recovery_task": victim,
                "pr": 7,
                "merged_head_sha": "deadbeef",
            }
        })
        .to_string();
        set_refs(&c, forger, &forged);
        c.execute(
            "UPDATE tasks SET completion_provenance='merged' WHERE id IN (?1,?2)",
            rusqlite::params![forger, victim],
        )
        .unwrap();
        let provenance: Option<String> = c
            .query_row(
                "SELECT completion_provenance FROM tasks WHERE id = ?1",
                rusqlite::params![forger],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(provenance.as_deref(), Some("merged"));

        let r = perf_facts(&c, false).unwrap();
        assert_eq!(
            r.intents.len(),
            2,
            "ordinary merged refs cannot forge recovery adoption"
        );
        let ids: Vec<i64> = r
            .intents
            .iter()
            .map(|i| i.contributing_task_ids[0])
            .collect();
        assert_eq!(ids, vec![forger, victim]);
        for intent in &r.intents {
            assert!(intent.lineage_evidence.is_none());
            assert!(intent.lineage_root_task_id.is_none());
            assert!(!intent.coverage.lineage);
        }
    }

    /// The gate rejects `completion_provenance = 'manual'`; merged
    /// completion is necessary but the exact adoption relation is also
    /// required.
    #[test]
    fn facts_recovery_delivery_manual_provenance_does_not_collapse() {
        let (_d, mut c) = open_tmp();
        let a = seed_ordinary(&mut c, 1600);
        let b = seed_ordinary(&mut c, 1650);
        let refs_a = serde_json::json!({
            "recovery_delivery": { "source_task": a, "recovery_task": b }
        })
        .to_string();
        set_refs(&c, a, &refs_a);
        c.execute(
            "UPDATE tasks SET status='done', completion_provenance='manual' WHERE id=?1",
            rusqlite::params![a],
        )
        .unwrap();

        let r = perf_facts(&c, false).unwrap();
        assert_eq!(r.intents.len(), 2);
        for intent in &r.intents {
            assert!(intent.lineage_evidence.is_none());
            assert!(!intent.coverage.lineage);
        }
    }

    #[test]
    fn facts_recovery_of_graph_child_folds_into_source_intent() {
        // Chained lineage: recovery Y → original child X → source S.
        // Both X and Y (and S itself) collapse into a single intent rooted
        // at S. All members surface as attributable detail; none appear
        // independently.
        let (_d, mut c) = open_tmp();
        let s = seed_ordinary(&mut c, 1600);
        let x = seed_ordinary(&mut c, 1650); // generated child of S
        let y = seed_ordinary(&mut c, 1700); // recovery for X
        let graph_id = seed_decomposition(&c, s, 1);
        seed_graph_member(&c, graph_id, x, "child-a", 1);
        let x_refs = serde_json::json!({
            "recovery_delivery": {
                "source_task": x,
                "recovery_task": y,
                "pr": 7,
                "merged_head_sha": "deadbeef",
            }
        })
        .to_string();
        set_refs(&c, x, &x_refs);
        record_explicit_recovery_adoption(&c, graph_id, x, y);

        let r = perf_facts(&c, false).unwrap();
        assert_eq!(r.intents.len(), 1);
        let intent = &r.intents[0];
        assert_eq!(intent.intent_id, format!("intent-{s}"));
        assert_eq!(intent.contributing_task_ids, vec![s, x, y]);
        let ev = intent.lineage_evidence.as_ref().unwrap();
        assert_eq!(
            ev.get("kind").unwrap(),
            &serde_json::json!("decomposed+recovery")
        );
        // Recovery pairs describe X→Y even though the root is S.
        let pairs = ev.get("recovery_pairs").unwrap().as_array().unwrap();
        assert_eq!(pairs.len(), 1);
        assert_eq!(
            pairs[0].get("original_task_id").unwrap(),
            &serde_json::json!(x)
        );
        assert_eq!(
            pairs[0].get("recovery_task_id").unwrap(),
            &serde_json::json!(y)
        );
    }

    #[test]
    fn facts_duplicate_or_conflicting_recovery_mappings_fail_closed() {
        // Three otherwise-valid daemon ledger entries all name recovery Y:
        // two duplicate X→Y entries and one conflicting Z→Y entry. The
        // reader must not select a last writer; Y stays independent with an
        // explicit lineage coverage gap, while the valid decomposition alone
        // still groups its generated children under S.
        let (_d, mut c) = open_tmp();
        let s = seed_ordinary(&mut c, 1600);
        let x = seed_ordinary(&mut c, 1650);
        let z = seed_ordinary(&mut c, 1700);
        let y = seed_ordinary(&mut c, 1750);
        let graph_id = seed_decomposition(&c, s, 1);
        seed_graph_member(&c, graph_id, x, "child-x", 1);
        seed_graph_member(&c, graph_id, z, "child-z", 1);
        for (original, pr, sha) in [(x, 7, "head-x"), (z, 8, "head-z")] {
            set_refs(
                &c,
                original,
                &serde_json::json!({
                    "recovery_delivery": {
                        "source_task": original,
                        "recovery_task": y,
                        "pr": pr,
                        "merged_head_sha": sha,
                    }
                })
                .to_string(),
            );
        }
        record_explicit_recovery_adoption(&c, graph_id, x, y);
        record_explicit_recovery_adoption(&c, graph_id, x, y);
        record_explicit_recovery_adoption(&c, graph_id, z, y);

        let r = perf_facts(&c, false).unwrap();
        assert_eq!(r.intents.len(), 2);
        let by_id: HashMap<&str, &IntentFacts> = r
            .intents
            .iter()
            .map(|intent| (intent.intent_id.as_str(), intent))
            .collect();
        let decomposed = by_id.get(format!("intent-{s}").as_str()).unwrap();
        assert_eq!(decomposed.contributing_task_ids, vec![s, x, z]);
        assert!(decomposed.coverage.lineage);
        assert!(
            decomposed
                .lineage_evidence
                .as_ref()
                .unwrap()
                .get("recovery_pairs")
                .is_none(),
            "ambiguous recovery rows must not leak into decomposition evidence"
        );
        let independent = by_id.get(format!("intent-{y}").as_str()).unwrap();
        assert_eq!(independent.contributing_task_ids, vec![y]);
        assert!(independent.lineage_root_task_id.is_none());
        assert!(independent.lineage_evidence.is_none());
        assert!(!independent.coverage.lineage);
    }

    #[test]
    fn facts_lineage_selection_is_scoped_and_member_capped() {
        // Real SQLite regression for both bounds: unrelated retained graphs
        // never enter a snapshot rooted at `current_source`, and a malformed
        // root can contribute only MAX_CHILDREN + 1 probe rows before being
        // rejected rather than materialized in full.
        let (_d, mut c) = open_tmp();
        let current_source = seed_ordinary(&mut c, 1600);
        let current_a = seed_ordinary(&mut c, 1650);
        let current_b = seed_ordinary(&mut c, 1700);
        let current_recovery = seed_ordinary(&mut c, 1750);
        let current_graph = seed_decomposition(&c, current_source, 1);
        seed_graph_member(&c, current_graph, current_a, "current-a", 1);
        seed_graph_member(&c, current_graph, current_b, "current-b", 1);
        set_refs(
            &c,
            current_a,
            &serde_json::json!({
                "recovery_delivery": {
                    "source_task": current_a,
                    "recovery_task": current_recovery,
                    "pr": 7,
                    "merged_head_sha": "current-head",
                }
            })
            .to_string(),
        );
        record_explicit_recovery_adoption(&c, current_graph, current_a, current_recovery);
        c.execute(
            "UPDATE task_decompositions SET state='completed',active=0 WHERE id=?1",
            rusqlite::params![current_graph],
        )
        .unwrap();

        let historical_source = seed_ordinary(&mut c, 100);
        let historical_graph = seed_decomposition(&c, historical_source, 1);
        c.execute(
            "UPDATE task_decompositions SET state='completed',active=0 WHERE id=?1",
            rusqlite::params![historical_graph],
        )
        .unwrap();
        let mut historical_children = Vec::new();
        for index in 0..(MAX_GRAPH_MEMBERS_PER_LINEAGE_ROOT + 3) {
            let child = seed_ordinary(&mut c, 100 + index as i64);
            seed_graph_member(
                &c,
                historical_graph,
                child,
                &format!("historical-{index}"),
                1,
            );
            historical_children.push(child);
        }
        let historical_recovery = seed_ordinary(&mut c, 200);
        set_refs(
            &c,
            historical_children[0],
            &serde_json::json!({
                "recovery_delivery": {
                    "source_task": historical_children[0],
                    "recovery_task": historical_recovery,
                    "pr": 8,
                    "merged_head_sha": "historical-head",
                }
            })
            .to_string(),
        );
        record_explicit_recovery_adoption(
            &c,
            historical_graph,
            historical_children[0],
            historical_recovery,
        );

        let scoped = build_lineage_snapshot(&c, &[current_a]).unwrap();
        assert_eq!(scoped.child_to_source.len(), 2);
        assert_eq!(
            scoped.child_to_source.get(&current_a),
            Some(&current_source)
        );
        assert_eq!(
            scoped.child_to_source.get(&current_b),
            Some(&current_source)
        );
        assert_eq!(scoped.source_to_children.len(), 1);
        assert_eq!(scoped.recovery_to_original.len(), 1);
        assert_eq!(
            scoped
                .recovery_to_original
                .get(&current_recovery)
                .unwrap()
                .original_task_id,
            current_a
        );
        assert!(!scoped
            .recovery_to_original
            .contains_key(&historical_recovery));
        assert!(!scoped
            .child_to_source
            .values()
            .any(|&id| id == historical_source));

        let capped = load_graph_members_for_roots(&c, &[historical_source]).unwrap();
        assert_eq!(
            capped.len(),
            MAX_GRAPH_MEMBERS_PER_LINEAGE_ROOT + 1,
            "the extra row is only an over-cap probe, never an unbounded history read"
        );
    }

    #[test]
    fn facts_collapse_paths_do_not_write() {
        // Real-SQLite proof that the collapse read paths (graph members and
        // refs.$.recovery_delivery scans) leave the database unchanged.
        let (_d, mut c) = open_tmp();
        let s = seed_ordinary(&mut c, 1600);
        let x = seed_ordinary(&mut c, 1650);
        let y = seed_ordinary(&mut c, 1700);
        let graph_id = seed_decomposition(&c, s, 1);
        seed_graph_member(&c, graph_id, x, "child-a", 1);
        let x_refs = serde_json::json!({
            "recovery_delivery": {
                "source_task": x,
                "recovery_task": y,
                "pr": 7,
                "merged_head_sha": "deadbeef"
            }
        })
        .to_string();
        set_refs(&c, x, &x_refs);
        record_explicit_recovery_adoption(&c, graph_id, x, y);

        let before = snapshot_db_state(&c);
        let _ = perf_facts(&c, false).unwrap();
        let _ = perf_facts(&c, true).unwrap();
        let after = snapshot_db_state(&c);
        assert_eq!(before, after, "collapse reads must not write");
    }

    // Helper for cloning an IntentFacts in tests (Serialize/Deserialize is
    // not derived because Deserialize is not needed elsewhere).
    impl IntentFacts {
        fn clone_for_test(&self) -> Self {
            Self {
                intent_id: self.intent_id.clone(),
                contributing_task_ids: self.contributing_task_ids.clone(),
                included: self.included,
                reason: self.reason,
                lineage_root_task_id: self.lineage_root_task_id,
                lineage_evidence: self.lineage_evidence.clone(),
                terminal_outcome: self.terminal_outcome.clone(),
                terminal_evidence: self.terminal_evidence.clone(),
                merge_provenance: self.merge_provenance.clone(),
                complexity: self.complexity.clone(),
                complexity_provenance: self.complexity_provenance.clone(),
                config_evidence: self.config_evidence.clone(),
                final_worker: self.final_worker.clone(),
                contributing_attempts: self.contributing_attempts.clone(),
                role_tokens_usd: self.role_tokens_usd.clone(),
                active_model_secs: self.active_model_secs,
                wall_secs: self.wall_secs,
                rework_count: self.rework_count,
                recovery_count: self.recovery_count,
                replan_count: self.replan_count,
                incident_count: self.incident_count,
                review_quality: self.review_quality.clone(),
                coverage: self.coverage,
            }
        }
    }

    #[test]
    fn watermark_boundary_is_inclusive() {
        let (_d, mut c) = open_tmp();
        c.execute(
            "UPDATE perf_watermark SET watermark = 1600 WHERE id = 1",
            [],
        )
        .unwrap();

        // Task exactly at the boundary (updated_at == watermark).
        let t1 = seed_task(&mut c, "done", None, 0, None, 1000, 1600);
        seed_run(&c, t1, "opus-46", "high", 1001);

        let r = perf(&c, PerfCut::Default, DM, DE).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(
            r.rows[0].n_tasks, 1,
            "task at exact boundary must be included"
        );
    }
}
