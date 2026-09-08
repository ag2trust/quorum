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

    // ── durable evidence fields ─────────────────────────────────────────
    pub lineage_root_task_id: Option<i64>,
    pub lineage_evidence: Option<serde_json::Value>,
    pub terminal_outcome: Option<String>,
    pub terminal_evidence: Option<serde_json::Value>,
    pub merge_provenance: Option<String>,
    pub complexity: Option<serde_json::Value>,
    pub complexity_provenance: Option<serde_json::Value>,
    pub config_evidence: Option<serde_json::Value>,
    pub final_worker: Option<serde_json::Value>,
    pub contributing_attempts: Option<serde_json::Value>,
    /// Raw normalized durable token buckets grouped by role. The nested
    /// `provisional_effective_token_total` is deliberately not a cost or
    /// billing definition; provider totals/cost stay null when not durable.
    pub role_tokens_usd: Option<serde_json::Value>,
    pub active_model_secs: Option<i64>,
    pub wall_secs: Option<i64>,
    pub rework_count: Option<i64>,
    pub recovery_count: Option<i64>,
    pub replan_count: Option<i64>,
    pub provider_failure_count: Option<i64>,
    pub abnormal_runner_ending_count: Option<i64>,
    pub collector_failure_count: Option<i64>,
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
    pub provider_failure: bool,
    pub abnormal_runner_ending: bool,
    pub collector_failure: bool,
    pub incident: bool,
    pub review_quality: bool,
}

impl IntentCoverage {
    fn iter_named(&self) -> [(&'static str, bool); 18] {
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
            ("provider_failure", self.provider_failure),
            ("abnormal_runner_ending", self.abnormal_runner_ending),
            ("collector_failure", self.collector_failure),
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
    created_at: i64,
    updated_at: i64,
    rework_round: i64,
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
        "SELECT id,status,review_only,created_at,updated_at,rework_round,
                completion_provenance,refs \
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
                created_at: r.get(3)?,
                updated_at: r.get(4)?,
                rework_round: r.get(5)?,
                completion_provenance: r.get(6)?,
                refs: r.get(7)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Load only derived lineage roots that are outside the bounded cohort. Their
/// classifier refs belong to the canonical intent even when the source itself
/// predates the prospective watermark; this is a bounded primary-key lookup,
/// never a second task-history scan.
fn load_facts_tasks_by_id(
    conn: &Connection,
    task_ids: &[i64],
) -> Result<HashMap<i64, FactsTaskRow>> {
    let mut rows = HashMap::new();
    for batch in task_ids.chunks(LINEAGE_ID_BATCH) {
        let placeholders = sql_placeholders(batch.len());
        let sql = format!(
            "SELECT id,status,review_only,created_at,updated_at,rework_round,
                    completion_provenance,refs
             FROM tasks WHERE id IN ({placeholders})"
        );
        let mut statement = conn.prepare(&sql)?;
        let batch_rows = statement
            .query_map(params_from_iter(batch.iter()), |row| {
                Ok(FactsTaskRow {
                    id: row.get(0)?,
                    status: row.get(1)?,
                    review_only: row.get::<_, i64>(2)? != 0,
                    created_at: row.get(3)?,
                    updated_at: row.get(4)?,
                    rework_round: row.get(5)?,
                    completion_provenance: row.get(6)?,
                    refs: row.get(7)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.extend(batch_rows.into_iter().map(|row| (row.id, row)));
    }
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
        provider_failure_count: None,
        abnormal_runner_ending_count: None,
        collector_failure_count: None,
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
    root_tasks: HashMap<i64, FactsTaskRow>,
    attribution: AttributionSnapshot,
    capped_attribution_task_ids: HashSet<i64>,
    recoverable_graph_task_ids: HashSet<i64>,
    unsatisfiable_parked_task_ids: HashSet<i64>,
    completed_graph_source_task_ids: HashSet<i64>,
}

// Attribution is deliberately capped both globally and per task. Facts are a
// reporting surface, not an unbounded replay of a task's complete execution
// history. The global cap bounds retained managed attempts to 65,536 and
// planner attempts to the same number under the per-task cap below.
const MAX_ATTRIBUTION_TASKS_PER_SNAPSHOT: usize = 1_024;
const MAX_ATTRIBUTION_ATTEMPTS_PER_TASK: usize = 64;
const MAX_TOKEN_USAGE_ROWS_PER_TASK: usize = 128;
const MAX_PLANNING_INCIDENT_ROWS_PER_TASK: usize = 64;
const MAX_REVIEW_COLLECTION_ROWS_PER_TASK: usize = 64;
const MAX_REVIEW_FINDINGS_PER_TASK: usize = 256;

#[derive(Debug, Clone)]
struct AssignmentFact {
    id: i64,
    responsibility_key: String,
    task_id: Option<i64>,
    pr_number: Option<i64>,
    role: String,
    review_stage: Option<String>,
    profile_id: String,
    provider: String,
    runner: String,
    model: String,
    effort: String,
    pool_key: String,
    policy_generation: String,
}

#[derive(Debug, Clone)]
struct RoutingAttemptFact {
    id: i64,
    profile_id: String,
    provider: String,
    runner: String,
    model: String,
    effort: String,
    pool_key: String,
    policy_generation: String,
}

#[derive(Debug, Clone)]
struct ManagedAttemptFact {
    task_id: i64,
    agent_run_id: i64,
    agent_name: String,
    role: String,
    model: String,
    provider: String,
    effort: String,
    spawned_at: i64,
    ended_at: Option<i64>,
    end_reason: Option<String>,
    task_status: String,
    assignment: AssignmentFact,
    routing_attempt: Option<RoutingAttemptFact>,
    configured_profile_id: Option<String>,
}

#[derive(Debug, Clone)]
struct PlannerAttemptFact {
    task_id: i64,
    graph_id: i64,
    run_id: String,
    agent_name: String,
    assignment: AssignmentFact,
}

/// A task-bound runner interval. Unlike attribution/configuration evidence,
/// time remains attributable when a legacy row lacks a role assignment: the
/// immutable `agent_runs.task_id` link is itself sufficient evidence.
#[derive(Debug, Clone)]
struct ActiveRunFact {
    id: i64,
    role: String,
    spawned_at: i64,
    ended_at: Option<i64>,
    end_reason: Option<String>,
}

#[derive(Debug, Clone)]
struct TokenUsageFact {
    agent_run_id: Option<i64>,
    purpose: String,
    usage: crate::token_usage::TokenUsage,
}

#[derive(Debug, Clone, Copy, Default)]
struct PlanningIncidentFact {
    replan_count: i64,
    provider_failure_count: i64,
}

#[derive(Debug, Clone)]
struct ReviewCollectionFact {
    pr_number: i64,
    status: String,
    findings_count: i64,
}

#[derive(Debug, Clone)]
struct ReviewFindingFact {
    pr_number: i64,
    kind: String,
    author_pushback: bool,
    pushback_accepted: Option<bool>,
    addressed_status: Option<String>,
}

#[derive(Debug, Default)]
struct AttributionSnapshot {
    managed_by_task: HashMap<i64, Vec<ManagedAttemptFact>>,
    latest_worker_run_by_task: HashMap<i64, i64>,
    incomplete_managed_task_ids: HashSet<i64>,
    planners_by_task: HashMap<i64, Vec<PlannerAttemptFact>>,
    active_runs_by_task: HashMap<i64, Vec<ActiveRunFact>>,
    capped_active_run_task_ids: HashSet<i64>,
    token_usage_by_task: HashMap<i64, Vec<TokenUsageFact>>,
    capped_token_usage_task_ids: HashSet<i64>,
    planning_incidents_by_task: HashMap<i64, PlanningIncidentFact>,
    incomplete_planning_incident_task_ids: HashSet<i64>,
    capped_planning_incident_task_ids: HashSet<i64>,
    review_collections_by_task: HashMap<i64, Vec<ReviewCollectionFact>>,
    capped_review_collection_task_ids: HashSet<i64>,
    review_findings_by_task: HashMap<i64, Vec<ReviewFindingFact>>,
    capped_review_finding_task_ids: HashSet<i64>,
    // These roles execute outside `agent_runs`. Their durable terminal
    // evidence still proves a model invocation, so it participates in
    // token/timing completeness even though it has no managed interval id.
    non_managed_invocations_by_task: HashMap<i64, BTreeMap<String, i64>>,
    capped_non_managed_invocation_task_ids: HashSet<i64>,
    // A future durable producer reason may prove that an invocation happened
    // without saying which role produced it. Keep its aggregate explicitly
    // unknown rather than crediting a partial role-token total.
    incomplete_non_managed_invocation_task_ids: HashSet<i64>,
}

fn note_non_managed_invocation(snapshot: &mut AttributionSnapshot, task_id: i64, purpose: &str) {
    let count = snapshot
        .non_managed_invocations_by_task
        .entry(task_id)
        .or_default()
        .entry(purpose.to_string())
        .or_default();
    if !add_metric(count, 1) {
        snapshot
            .capped_non_managed_invocation_task_ids
            .insert(task_id);
    }
}

fn present_text(value: &str) -> bool {
    !value.is_empty() && !value.contains('\0')
}

fn complete_assignment(assignment: &AssignmentFact) -> bool {
    assignment.task_id.is_some()
        && [
            assignment.responsibility_key.as_str(),
            assignment.role.as_str(),
            assignment.profile_id.as_str(),
            assignment.provider.as_str(),
            assignment.runner.as_str(),
            assignment.model.as_str(),
            assignment.effort.as_str(),
            assignment.pool_key.as_str(),
            assignment.policy_generation.as_str(),
        ]
        .into_iter()
        .all(present_text)
        && assignment.review_stage.as_deref().is_none_or(present_text)
}

fn complete_routing_attempt(attempt: &RoutingAttemptFact) -> bool {
    [
        attempt.profile_id.as_str(),
        attempt.provider.as_str(),
        attempt.runner.as_str(),
        attempt.model.as_str(),
        attempt.effort.as_str(),
        attempt.pool_key.as_str(),
        attempt.policy_generation.as_str(),
    ]
    .into_iter()
    .all(present_text)
}

fn complete_managed_attempt(attempt: &ManagedAttemptFact) -> bool {
    attempt.assignment.task_id == Some(attempt.task_id)
        && attempt.assignment.role == attempt.role
        && complete_assignment(&attempt.assignment)
        && [
            attempt.agent_name.as_str(),
            attempt.model.as_str(),
            attempt.provider.as_str(),
            attempt.effort.as_str(),
        ]
        .into_iter()
        .all(present_text)
}

fn managed_attempt_value(attempt: &ManagedAttemptFact) -> serde_json::Value {
    serde_json::json!({
        "task_id": attempt.task_id,
        "attempt_id": attempt.agent_run_id,
        "role_assignment_id": attempt.assignment.id,
        "routing_attempt_id": attempt.routing_attempt.as_ref().map(|route| route.id),
        "agent": present_text(&attempt.agent_name).then_some(attempt.agent_name.as_str()),
        "role": attempt.role,
        "review_stage": attempt.assignment.review_stage,
        "model": present_text(&attempt.model).then_some(attempt.model.as_str()),
        "provider": present_text(&attempt.provider).then_some(attempt.provider.as_str()),
        "effort": present_text(&attempt.effort).then_some(attempt.effort.as_str()),
        "spawned_at": attempt.spawned_at,
        "ended_at": attempt.ended_at,
        "end_reason": attempt.end_reason,
    })
}

fn planner_attempt_value(attempt: &PlannerAttemptFact) -> serde_json::Value {
    serde_json::json!({
        "task_id": attempt.task_id,
        "graph_id": attempt.graph_id,
        "attempt_id": attempt.run_id,
        "role_assignment_id": attempt.assignment.id,
        "agent": attempt.agent_name,
        "role": "planner",
        "model": attempt.assignment.model,
        "provider": attempt.assignment.provider,
        "effort": attempt.assignment.effort,
    })
}

fn assignment_config_value(
    assignment: &AssignmentFact,
    routing_attempt: Option<&RoutingAttemptFact>,
) -> serde_json::Value {
    let (
        source,
        routing_attempt_id,
        profile_id,
        provider,
        runner,
        model,
        effort,
        pool_key,
        policy_generation,
    ) = match routing_attempt {
        Some(route) => (
            "routing-attempt",
            Some(route.id),
            route.profile_id.as_str(),
            route.provider.as_str(),
            route.runner.as_str(),
            route.model.as_str(),
            route.effort.as_str(),
            route.pool_key.as_str(),
            route.policy_generation.as_str(),
        ),
        None => (
            "role-assignment",
            None,
            assignment.profile_id.as_str(),
            assignment.provider.as_str(),
            assignment.runner.as_str(),
            assignment.model.as_str(),
            assignment.effort.as_str(),
            assignment.pool_key.as_str(),
            assignment.policy_generation.as_str(),
        ),
    };
    serde_json::json!({
        "source": source,
        "role_assignment_id": assignment.id,
        "routing_attempt_id": routing_attempt_id,
        "responsibility_key": assignment.responsibility_key,
        "task_id": assignment.task_id,
        "pr_number": assignment.pr_number,
        "role": assignment.role,
        "review_stage": assignment.review_stage,
        "profile_id": profile_id,
        "provider": provider,
        "runner": runner,
        "model": model,
        "effort": effort,
        "pool_key": pool_key,
        "policy_generation": policy_generation,
    })
}

/// Configuration evidence must identify the exact durable route. An original
/// run is covered by its immutable role assignment; a fallback requires its
/// matching immutable routing-attempt row. In particular, never fill a
/// missing historical fallback route from today's configuration.
fn managed_config_value(attempt: &ManagedAttemptFact) -> Option<serde_json::Value> {
    if let Some(route) = attempt
        .routing_attempt
        .as_ref()
        .filter(|route| complete_routing_attempt(route))
    {
        return Some(assignment_config_value(&attempt.assignment, Some(route)));
    }
    attempt
        .configured_profile_id
        .is_none()
        .then(|| assignment_config_value(&attempt.assignment, None))
}

fn planner_config_value(attempt: &PlannerAttemptFact) -> serde_json::Value {
    assignment_config_value(&attempt.assignment, None)
}

fn is_final_submitting_worker(attempt: &ManagedAttemptFact) -> bool {
    matches!(
        attempt.end_reason.as_deref(),
        Some("submitted" | "awaiting_merge" | "completed" | "merged")
    ) || (attempt.ended_at.is_none()
        && matches!(attempt.task_status.as_str(), "in-review" | "merging"))
}

fn load_attribution_snapshot(conn: &Connection, task_ids: &[i64]) -> Result<AttributionSnapshot> {
    let mut snapshot = AttributionSnapshot::default();
    for batch in task_ids.chunks(LINEAGE_ID_BATCH) {
        let placeholders = sql_placeholders(batch.len());
        let sql = format!(
            "SELECT task_id,agent_run_id,agent_name,role,model,provider,effort,spawned_at,ended_at,
                    end_reason,task_status,configured_profile_id,
                    assignment_id,responsibility_key,assignment_task_id,pr_number,
                    assignment_role,review_stage,assignment_profile_id,assignment_provider,
                    assignment_runner,assignment_model,assignment_effort,assignment_pool_key,
                    assignment_policy_generation,
                    routing_attempt_id,routing_profile_id,routing_provider,routing_runner,
                    routing_model,routing_effort,routing_pool_key,routing_policy_generation
             FROM (
                 SELECT ar.task_id,ar.id AS agent_run_id,ar.agent_name,ar.role,ar.model,
                        ar.provider,ar.effort,ar.spawned_at,ar.ended_at,ar.end_reason,
                        task.status AS task_status,
                        ar.configured_profile_id,
                        assignment.id AS assignment_id,
                        assignment.responsibility_key,
                        assignment.task_id AS assignment_task_id,
                        assignment.pr_number,
                        assignment.role AS assignment_role,
                        assignment.review_stage,
                        assignment.profile_id AS assignment_profile_id,
                        assignment.provider AS assignment_provider,
                        assignment.runner AS assignment_runner,
                        assignment.model AS assignment_model,
                        assignment.effort AS assignment_effort,
                        assignment.pool_key AS assignment_pool_key,
                        assignment.policy_generation AS assignment_policy_generation,
                        route.id AS routing_attempt_id,
                        route.profile_id AS routing_profile_id,
                        route.provider AS routing_provider,
                        route.runner AS routing_runner,
                        route.model AS routing_model,
                        route.effort AS routing_effort,
                        route.pool_key AS routing_pool_key,
                        route.policy_generation AS routing_policy_generation,
                        ROW_NUMBER() OVER (PARTITION BY ar.task_id ORDER BY ar.id DESC) AS row_num
                 FROM agent_runs ar
                 JOIN tasks task ON task.id=ar.task_id
                 JOIN role_assignments assignment
                   ON assignment.id=ar.role_assignment_id
                  AND assignment.task_id=ar.task_id
                  AND assignment.role=ar.role
                 LEFT JOIN routing_attempts route
                   ON route.role_assignment_id=assignment.id
                  AND route.profile_id=COALESCE(ar.configured_profile_id, assignment.profile_id)
                 WHERE ar.task_id IN ({placeholders})
                   AND ar.role IN ('worker','reviewer')
             )
             WHERE row_num <= ?
             ORDER BY task_id,agent_run_id"
        );
        let mut params = batch.to_vec();
        params.push(MAX_ATTRIBUTION_ATTEMPTS_PER_TASK as i64);
        let mut statement = conn.prepare(&sql)?;
        let rows = statement
            .query_map(params_from_iter(params), |row| {
                let assignment = AssignmentFact {
                    id: row.get(12)?,
                    responsibility_key: row.get(13)?,
                    task_id: row.get(14)?,
                    pr_number: row.get(15)?,
                    role: row.get(16)?,
                    review_stage: row.get(17)?,
                    profile_id: row.get(18)?,
                    provider: row.get(19)?,
                    runner: row.get(20)?,
                    model: row.get(21)?,
                    effort: row.get(22)?,
                    pool_key: row.get(23)?,
                    policy_generation: row.get(24)?,
                };
                let routing_attempt = match row.get::<_, Option<i64>>(25)? {
                    Some(id) => Some(RoutingAttemptFact {
                        id,
                        profile_id: row.get(26)?,
                        provider: row.get(27)?,
                        runner: row.get(28)?,
                        model: row.get(29)?,
                        effort: row.get(30)?,
                        pool_key: row.get(31)?,
                        policy_generation: row.get(32)?,
                    }),
                    None => None,
                };
                Ok(ManagedAttemptFact {
                    task_id: row.get(0)?,
                    agent_run_id: row.get(1)?,
                    agent_name: row.get(2)?,
                    role: row.get(3)?,
                    model: row.get(4)?,
                    provider: row.get(5)?,
                    effort: row.get(6)?,
                    spawned_at: row.get(7)?,
                    ended_at: row.get(8)?,
                    end_reason: row.get(9)?,
                    task_status: row.get(10)?,
                    configured_profile_id: row.get(11)?,
                    assignment,
                    routing_attempt,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for attempt in rows {
            // An agent run records what actually executed. Do not borrow a
            // provider/model/effort from its assignment when the execution
            // evidence is incomplete. Retain the bounded attempt instead so
            // its JSON nulls and coverage gap remain visible to consumers.
            if !complete_managed_attempt(&attempt) {
                snapshot.incomplete_managed_task_ids.insert(attempt.task_id);
            }
            snapshot
                .managed_by_task
                .entry(attempt.task_id)
                .or_default()
                .push(attempt);
        }

        // An agent run with no exact durable assignment cannot be rendered as
        // an attributed attempt. It still makes the compound attribution and
        // configuration evidence incomplete; do not silently report the
        // remaining linked runs as complete.
        let incomplete_sql = format!(
            "SELECT task_id FROM (
                 SELECT ar.task_id,
                        CASE WHEN assignment.id IS NULL THEN 1 ELSE 0 END AS missing_assignment,
                        ROW_NUMBER() OVER (PARTITION BY ar.task_id ORDER BY ar.id DESC) AS row_num
                 FROM agent_runs ar
                 LEFT JOIN role_assignments assignment
                   ON assignment.id=ar.role_assignment_id
                  AND assignment.task_id=ar.task_id
                  AND assignment.role=ar.role
                 WHERE ar.task_id IN ({placeholders})
                   AND ar.role IN ('worker','reviewer')
             )
             WHERE row_num <= ? AND missing_assignment=1"
        );
        let mut params = batch.to_vec();
        params.push(MAX_ATTRIBUTION_ATTEMPTS_PER_TASK as i64);
        let mut statement = conn.prepare(&incomplete_sql)?;
        let incomplete_task_ids = statement
            .query_map(params_from_iter(params), |row| row.get::<_, i64>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        snapshot
            .incomplete_managed_task_ids
            .extend(incomplete_task_ids);

        let latest_sql = format!(
            "SELECT task_id,MAX(id) FROM agent_runs
             WHERE task_id IN ({placeholders}) AND role='worker'
             GROUP BY task_id"
        );
        let mut statement = conn.prepare(&latest_sql)?;
        let latest = statement
            .query_map(params_from_iter(batch.iter()), |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        snapshot.latest_worker_run_by_task.extend(latest);

        let planner_sql = format!(
            "SELECT task_id,graph_id,run_id,agent_name,
                    assignment_id,responsibility_key,assignment_task_id,pr_number,
                    assignment_role,review_stage,assignment_profile_id,assignment_provider,
                    assignment_runner,assignment_model,assignment_effort,assignment_pool_key,
                    assignment_policy_generation
             FROM (
                 SELECT graph.source_task_id AS task_id,graph.id AS graph_id,
                        submission.run_id,capability.agent AS agent_name,
                        assignment.id AS assignment_id,
                        assignment.responsibility_key,
                        assignment.task_id AS assignment_task_id,
                        assignment.pr_number,
                        assignment.role AS assignment_role,
                        assignment.review_stage,
                        assignment.profile_id AS assignment_profile_id,
                        assignment.provider AS assignment_provider,
                        assignment.runner AS assignment_runner,
                        assignment.model AS assignment_model,
                        assignment.effort AS assignment_effort,
                        assignment.pool_key AS assignment_pool_key,
                        assignment.policy_generation AS assignment_policy_generation,
                        ROW_NUMBER() OVER (
                            PARTITION BY graph.source_task_id ORDER BY submission.run_id DESC
                        ) AS row_num
                 FROM task_decompositions graph
                 JOIN planner_submissions submission ON submission.graph_id=graph.id
                 JOIN run_capabilities capability
                   ON capability.run_id=submission.run_id
                  AND capability.task_id=graph.source_task_id
                  AND capability.role='planner'
                 JOIN role_assignments assignment
                   ON assignment.id=graph.planner_assignment_id
                  AND assignment.task_id=graph.source_task_id
                  AND assignment.role='planner'
                 WHERE graph.source_task_id IN ({placeholders})
             )
             WHERE row_num <= ?
             ORDER BY task_id,run_id"
        );
        let mut params = batch.to_vec();
        params.push(MAX_ATTRIBUTION_ATTEMPTS_PER_TASK as i64);
        let mut statement = conn.prepare(&planner_sql)?;
        let planners = statement
            .query_map(params_from_iter(params), |row| {
                Ok(PlannerAttemptFact {
                    task_id: row.get(0)?,
                    graph_id: row.get(1)?,
                    run_id: row.get(2)?,
                    agent_name: row.get(3)?,
                    assignment: AssignmentFact {
                        id: row.get(4)?,
                        responsibility_key: row.get(5)?,
                        task_id: row.get(6)?,
                        pr_number: row.get(7)?,
                        role: row.get(8)?,
                        review_stage: row.get(9)?,
                        profile_id: row.get(10)?,
                        provider: row.get(11)?,
                        runner: row.get(12)?,
                        model: row.get(13)?,
                        effort: row.get(14)?,
                        pool_key: row.get(15)?,
                        policy_generation: row.get(16)?,
                    },
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for planner in planners {
            if planner.assignment.task_id != Some(planner.task_id)
                || planner.assignment.role != "planner"
                || !complete_assignment(&planner.assignment)
                || !present_text(&planner.run_id)
                || !present_text(&planner.agent_name)
            {
                continue;
            }
            snapshot
                .planners_by_task
                .entry(planner.task_id)
                .or_default()
                .push(planner);
        }
    }
    load_active_run_facts(conn, task_ids, &mut snapshot)?;
    load_token_usage_facts(conn, task_ids, &mut snapshot)?;
    load_non_managed_invocation_facts(conn, task_ids, &mut snapshot)?;
    load_planning_incident_facts(conn, task_ids, &mut snapshot)?;
    load_review_quality_facts(conn, task_ids, &mut snapshot)?;
    Ok(snapshot)
}

/// Load only a bounded prefix plus one overflow probe for each task. A prefix
/// is useful only when it is complete, so a probe makes the whole timing fact
/// explicitly unknown rather than reporting a partial duration.
fn load_active_run_facts(
    conn: &Connection,
    task_ids: &[i64],
    snapshot: &mut AttributionSnapshot,
) -> Result<()> {
    // Do not rank a task's complete history merely to discard its tail. Each
    // indexed task lookup stops at the bounded prefix plus one overflow probe;
    // ordering is immaterial to token matching, duration sums, and incidents.
    let mut statement = conn.prepare(
        "SELECT id,role,spawned_at,ended_at,end_reason
         FROM agent_runs
         WHERE task_id=?1 AND role IN ('worker','reviewer')
         LIMIT ?2",
    )?;
    for &task_id in task_ids {
        let rows = statement
            .query_map(
                [task_id, (MAX_ATTRIBUTION_ATTEMPTS_PER_TASK + 1) as i64],
                |row| {
                    Ok(ActiveRunFact {
                        id: row.get(0)?,
                        role: row.get(1)?,
                        spawned_at: row.get(2)?,
                        ended_at: row.get(3)?,
                        end_reason: row.get(4)?,
                    })
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if rows.len() > MAX_ATTRIBUTION_ATTEMPTS_PER_TASK {
            snapshot.capped_active_run_task_ids.insert(task_id);
            continue;
        }
        snapshot.active_runs_by_task.insert(task_id, rows);
    }
    Ok(())
}

fn load_token_usage_facts(
    conn: &Connection,
    task_ids: &[i64],
    snapshot: &mut AttributionSnapshot,
) -> Result<()> {
    let mut statement = conn.prepare(
        "SELECT usage.agent_run_id,usage.purpose,usage.uncached_input_tokens,
                usage.cached_input_tokens,usage.cache_write_input_tokens,
                usage.output_tokens,usage.reasoning_tokens
         FROM token_usage_run_tasks mapping
         JOIN token_usage_runs usage ON usage.id=mapping.run_id
         WHERE mapping.task_id=?1
         LIMIT ?2",
    )?;
    for &task_id in task_ids {
        let rows = statement
            .query_map(
                [task_id, (MAX_TOKEN_USAGE_ROWS_PER_TASK + 1) as i64],
                |row| {
                    Ok(TokenUsageFact {
                        agent_run_id: row.get(0)?,
                        purpose: row.get(1)?,
                        usage: crate::token_usage::TokenUsage {
                            uncached_input_tokens: row.get(2)?,
                            cached_input_tokens: row.get(3)?,
                            cache_write_input_tokens: row.get(4)?,
                            output_tokens: row.get(5)?,
                            reasoning_tokens: row.get(6)?,
                        },
                    })
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if rows.len() > MAX_TOKEN_USAGE_ROWS_PER_TASK {
            snapshot.capped_token_usage_task_ids.insert(task_id);
            continue;
        }
        snapshot.token_usage_by_task.insert(task_id, rows);
    }
    Ok(())
}

/// Planner and decomposition-classifier outcomes are durable invocation
/// evidence without an `agent_runs` row. Read each source task through a
/// bounded prefix/probe so their missing telemetry cannot certify a partial
/// aggregate, while a long retained planner history cannot extend this WAL
/// snapshot.
fn load_non_managed_invocation_facts(
    conn: &Connection,
    task_ids: &[i64],
    snapshot: &mut AttributionSnapshot,
) -> Result<()> {
    let mut planners = conn.prepare(
        "SELECT submission.run_id
         FROM task_decompositions AS graph
         JOIN planner_submissions AS submission ON submission.graph_id=graph.id
         WHERE graph.source_task_id=?1
         LIMIT ?2",
    )?;
    let mut classifier = conn.prepare(
        "SELECT 1
         FROM task_decompositions
         WHERE source_task_id=?1 AND accepted_classifications_json IS NOT NULL
         LIMIT 1",
    )?;
    // Proposal rejections retain the classifier turn after a retry clears the
    // current accepted classification. Provider failures retain the role that
    // was reaped with best-effort telemetry. Include both in completeness so a
    // later successful turn cannot certify a partial token aggregate.
    let mut retained_attempts = conn.prepare(
        "SELECT attempt.kind,attempt.reason_code
         FROM task_decompositions AS graph
         JOIN decomposition_attempts AS attempt ON attempt.graph_id=graph.id
         WHERE graph.source_task_id=?1
           AND attempt.kind IN ('proposal','provider','blocker')
         LIMIT ?2",
    )?;
    for &task_id in task_ids {
        let planner_rows = planners
            .query_map(
                [task_id, (MAX_ATTRIBUTION_ATTEMPTS_PER_TASK + 1) as i64],
                |row| row.get::<_, String>(0),
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if planner_rows.len() > MAX_ATTRIBUTION_ATTEMPTS_PER_TASK {
            snapshot
                .capped_non_managed_invocation_task_ids
                .insert(task_id);
        } else {
            for _ in planner_rows {
                note_non_managed_invocation(snapshot, task_id, "planner");
            }
        }
        if classifier.exists([task_id])? {
            note_non_managed_invocation(snapshot, task_id, "classifier");
        }
        let attempts = retained_attempts
            .query_map(
                [task_id, (MAX_ATTRIBUTION_ATTEMPTS_PER_TASK + 1) as i64],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if attempts.len() > MAX_ATTRIBUTION_ATTEMPTS_PER_TASK {
            snapshot
                .capped_non_managed_invocation_task_ids
                .insert(task_id);
            continue;
        }
        for (kind, reason_code) in attempts {
            match kind.as_str() {
                // `reject_decomposition_proposal` writes a proposal attempt
                // only after its classifier turn. It remains after retries
                // discard `accepted_classifications_json`.
                "proposal" => note_non_managed_invocation(snapshot, task_id, "classifier"),
                "provider" => match reason_code.as_str() {
                    // These failures occur after the corresponding model
                    // process is reaped, so any best-effort token snapshot is
                    // required before the role aggregate is complete.
                    "planner-provider" => note_non_managed_invocation(snapshot, task_id, "planner"),
                    "classifier-provider" => {
                        note_non_managed_invocation(snapshot, task_id, "classifier")
                    }
                    // Arbiter terminal outcomes have a paired durable verdict
                    // row, which `load_planning_incident_facts` accounts for.
                    // The remaining listed failures happen before a provider
                    // invocation is started.
                    "arbiter-provider"
                    | "planner-prompt"
                    | "frozen-view"
                    | "planner-spawn"
                    | "classifier-spawn"
                    | "arbiter-frozen-view"
                    | "arbiter-spawn" => {}
                    // Do not guess a role for a future producer value.
                    _ => {
                        snapshot
                            .incomplete_non_managed_invocation_task_ids
                            .insert(task_id);
                    }
                },
                // Planner blockers are terminal model responses. The two
                // named exceptions are recorded after an arbiter/materialize
                // path already accounted for elsewhere.
                "blocker"
                    if !matches!(
                        reason_code.as_str(),
                        "arbiter-reject-source" | "materialization-authority-lost"
                    ) =>
                {
                    note_non_managed_invocation(snapshot, task_id, "planner")
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn load_planning_incident_facts(
    conn: &Connection,
    task_ids: &[i64],
    snapshot: &mut AttributionSnapshot,
) -> Result<()> {
    // The task_decompositions counters are current retry budgets. They reset
    // when planning is retried, whereas decomposition_attempts retains every
    // proposal/provider event across those generations.
    let mut statement = conn.prepare(
        "SELECT attempt.kind
         FROM task_decompositions AS graph
         JOIN decomposition_attempts AS attempt ON attempt.graph_id=graph.id
         WHERE graph.source_task_id=?1
           AND attempt.kind IN ('proposal','provider','verdict')
         LIMIT ?2",
    )?;
    for &task_id in task_ids {
        let rows = statement
            .query_map(
                [task_id, (MAX_PLANNING_INCIDENT_ROWS_PER_TASK + 1) as i64],
                |row| row.get::<_, String>(0),
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if rows.len() > MAX_PLANNING_INCIDENT_ROWS_PER_TASK {
            snapshot.capped_planning_incident_task_ids.insert(task_id);
            snapshot
                .capped_non_managed_invocation_task_ids
                .insert(task_id);
            continue;
        }
        for kind in rows {
            // Every durable verdict attempt is emitted by the arbiter after a
            // provider turn. Its interval is not persisted in agent_runs.
            if kind == "verdict" {
                note_non_managed_invocation(snapshot, task_id, "arbiter");
                continue;
            }
            let current = snapshot
                .planning_incidents_by_task
                .entry(task_id)
                .or_default();
            let counter = match kind.as_str() {
                "proposal" => &mut current.replan_count,
                "provider" => &mut current.provider_failure_count,
                _ => {
                    snapshot
                        .incomplete_planning_incident_task_ids
                        .insert(task_id);
                    continue;
                }
            };
            if !add_metric(counter, 1) {
                snapshot
                    .incomplete_planning_incident_task_ids
                    .insert(task_id);
            }
        }
    }
    Ok(())
}

fn load_review_quality_facts(
    conn: &Connection,
    task_ids: &[i64],
    snapshot: &mut AttributionSnapshot,
) -> Result<()> {
    let mut collections_statement = conn.prepare(
        "SELECT pr_number,status,findings_count
         FROM review_collection_runs
         WHERE task_id=?1
         LIMIT ?2",
    )?;
    let mut findings_statement = conn.prepare(
        "SELECT pr_number,kind,author_pushback,pushback_accepted,addressed_status
         FROM review_findings
         WHERE task_id=?1
         LIMIT ?2",
    )?;
    for &task_id in task_ids {
        let collections = collections_statement
            .query_map(
                [task_id, (MAX_REVIEW_COLLECTION_ROWS_PER_TASK + 1) as i64],
                |row| {
                    Ok(ReviewCollectionFact {
                        pr_number: row.get(0)?,
                        status: row.get(1)?,
                        findings_count: row.get(2)?,
                    })
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if collections.len() > MAX_REVIEW_COLLECTION_ROWS_PER_TASK {
            snapshot.capped_review_collection_task_ids.insert(task_id);
            snapshot
                .capped_non_managed_invocation_task_ids
                .insert(task_id);
        } else {
            for _ in &collections {
                // A canonical collector result still proves one invocation;
                // its independent telemetry must be present before token
                // coverage can be certified.
                note_non_managed_invocation(snapshot, task_id, "collector");
            }
            snapshot
                .review_collections_by_task
                .insert(task_id, collections);
        }

        let findings = findings_statement
            .query_map(
                [task_id, (MAX_REVIEW_FINDINGS_PER_TASK + 1) as i64],
                |row| {
                    Ok(ReviewFindingFact {
                        pr_number: row.get(0)?,
                        kind: row.get(1)?,
                        author_pushback: row.get::<_, i64>(2)? != 0,
                        pushback_accepted: row.get::<_, Option<i64>>(3)?.map(|value| value != 0),
                        addressed_status: row.get(4)?,
                    })
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if findings.len() > MAX_REVIEW_FINDINGS_PER_TASK {
            snapshot.capped_review_finding_task_ids.insert(task_id);
        } else {
            snapshot.review_findings_by_task.insert(task_id, findings);
        }
    }
    Ok(())
}

fn root_complexity_facts(
    row: Option<&FactsTaskRow>,
) -> Option<(serde_json::Value, serde_json::Value)> {
    let row = row?;
    // Reuse the classifier's persisted v2 readiness contract rather than
    // trusting an isolated `cx_est` value or any legacy complexity label.
    if !crate::tasks::classification_is_complete(&row.refs) {
        return None;
    }
    let refs = refs_object(row)?;
    let cx_est = refs.get("cx_est")?.as_i64()?;
    let cx_by = refs
        .get("cx_by")?
        .as_str()
        .filter(|value| present_text(value))?;
    let cx_size = refs.get("cx_size")?.as_str()?;
    let cx_ready = refs.get("cx_ready")?.as_bool()?;
    let cx_not_ready_reason = refs.get("cx_not_ready_reason")?.clone();
    let mut provenance = serde_json::json!({
        "task_id": row.id,
        "cx_by": cx_by,
    });
    if let Some(size_reason) = refs
        .get("cx_size_reason")
        .and_then(serde_json::Value::as_str)
        .filter(|value| present_text(value))
    {
        provenance["cx_size_reason"] = serde_json::json!(size_reason);
    }
    Some((
        serde_json::json!({
            "cx_est": cx_est,
            "cx_size": cx_size,
            "cx_ready": cx_ready,
            "cx_not_ready_reason": cx_not_ready_reason,
        }),
        provenance,
    ))
}

/// Attribution uses the full small durable lineage relation, while the public
/// contributing-task list remains capped to the prospective cohort. A graph
/// source can predate the watermark, so omitting it here would lose the
/// planner/worker/reviewer facts that belong to the collapsed intent.
fn attribution_members_for_intent(
    root: i64,
    cohort_members: &[i64],
    lineage: &LineageSnapshot,
) -> Vec<i64> {
    let mut members: BTreeSet<i64> = cohort_members.iter().copied().collect();
    members.insert(root);
    if let Some(children) = lineage.source_to_children.get(&root) {
        members.extend(children.iter().map(|child| child.task_id));
        for child in children {
            if let Some(recoveries) = lineage.original_to_recoveries.get(&child.task_id) {
                members.extend(recoveries.iter().map(|pair| pair.recovery_task_id));
            }
        }
    }
    if let Some(recoveries) = lineage.original_to_recoveries.get(&root) {
        members.extend(recoveries.iter().map(|pair| pair.recovery_task_id));
    }
    // Lineage construction already bounds each graph to MAX_CHILDREN and
    // rejects ambiguous recovery mappings. Keep this internal set complete;
    // only the public contributing-task list is capped for output size.
    members.into_iter().collect()
}

/// A global attribution cap makes the omitted lineage member unknown, not
/// absent. Do not emit a subset of an intent's role/configuration evidence as
/// if it were complete; JSON null plus false coverage is the explicit gap.
fn clear_capped_attribution(intent: &mut IntentFacts) {
    intent.final_worker = None;
    intent.contributing_attempts = None;
    intent.config_evidence = None;
    intent.role_tokens_usd = None;
    intent.active_model_secs = None;
    intent.wall_secs = None;
    intent.rework_count = None;
    intent.recovery_count = None;
    intent.replan_count = None;
    intent.provider_failure_count = None;
    intent.abnormal_runner_ending_count = None;
    intent.collector_failure_count = None;
    intent.incident_count = None;
    intent.review_quality = None;
    intent.coverage.final_worker = false;
    intent.coverage.contributing_attempts = false;
    intent.coverage.config = false;
    intent.coverage.role_tokens_usd = false;
    intent.coverage.active_model_secs = false;
    intent.coverage.wall_secs = false;
    intent.coverage.rework = false;
    intent.coverage.recovery = false;
    intent.coverage.replan = false;
    intent.coverage.provider_failure = false;
    intent.coverage.abnormal_runner_ending = false;
    intent.coverage.collector_failure = false;
    intent.coverage.incident = false;
    intent.coverage.review_quality = false;
}

fn populate_attribution(
    intent: &mut IntentFacts,
    members: &[i64],
    attribution: &AttributionSnapshot,
) {
    let mut workers = Vec::new();
    let mut reviewers = Vec::new();
    let mut planners = Vec::new();
    let mut final_workers: Vec<&ManagedAttemptFact> = Vec::new();
    let mut config_inputs: BTreeMap<(i64, Option<i64>), serde_json::Value> = BTreeMap::new();
    let mut config_complete = !members
        .iter()
        .any(|task_id| attribution.incomplete_managed_task_ids.contains(task_id));
    let mut attempts_complete = config_complete;
    let mut has_contribution = false;

    for &task_id in members {
        if let Some(runs) = attribution.managed_by_task.get(&task_id) {
            for run in runs {
                has_contribution = true;
                if !complete_managed_attempt(run) {
                    attempts_complete = false;
                    config_complete = false;
                }
                let attempt = managed_attempt_value(run);
                match run.role.as_str() {
                    "worker" => {
                        if attribution.latest_worker_run_by_task.get(&task_id)
                            == Some(&run.agent_run_id)
                            && complete_managed_attempt(run)
                            && is_final_submitting_worker(run)
                        {
                            final_workers.push(run);
                        }
                        workers.push(attempt);
                    }
                    "reviewer" => reviewers.push(attempt),
                    _ => continue,
                }
                if let Some(input) = managed_config_value(run) {
                    config_inputs.insert(
                        (
                            run.assignment.id,
                            run.routing_attempt.as_ref().map(|route| route.id),
                        ),
                        input,
                    );
                } else {
                    config_complete = false;
                }
            }
        }
        if let Some(attempts) = attribution.planners_by_task.get(&task_id) {
            for planner in attempts {
                has_contribution = true;
                planners.push(planner_attempt_value(planner));
                config_inputs
                    .entry((planner.assignment.id, None))
                    .or_insert_with(|| planner_config_value(planner));
            }
        }
    }

    if has_contribution {
        intent.contributing_attempts = Some(serde_json::json!({
            "worker": workers,
            "reviewer": reviewers,
            "planner": planners,
        }));
        intent.coverage.contributing_attempts = attempts_complete;
    }
    // A final submitting worker is the final managed worker turn for a task
    // that durably submitted, awaits merge, completed, or merged. Across a
    // collapsed intent, the latest such turn is the final submission; rework
    // and fallback turns remain visible in `contributing_attempts` rather
    // than replacing it.
    if let Some(final_worker) = final_workers
        .into_iter()
        .max_by_key(|run| (run.ended_at.unwrap_or(run.spawned_at), run.agent_run_id))
    {
        intent.final_worker = Some(managed_attempt_value(final_worker));
        intent.coverage.final_worker = true;
    }
    if has_contribution && config_complete && !config_inputs.is_empty() {
        intent.config_evidence = Some(serde_json::json!({
            "policy_fingerprint_inputs": config_inputs.into_values().collect::<Vec<_>>(),
        }));
        intent.coverage.config = true;
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct TokenTotals {
    uncached_input_tokens: i64,
    cached_input_tokens: i64,
    cache_write_input_tokens: i64,
    output_tokens: i64,
    reasoning_tokens: i64,
}

impl TokenTotals {
    fn add_usage(&mut self, usage: crate::token_usage::TokenUsage) -> bool {
        if [
            usage.uncached_input_tokens,
            usage.cached_input_tokens,
            usage.cache_write_input_tokens,
            usage.output_tokens,
            usage.reasoning_tokens,
        ]
        .into_iter()
        .any(|value| value < 0)
        {
            return false;
        }
        let Some(uncached_input_tokens) = self
            .uncached_input_tokens
            .checked_add(usage.uncached_input_tokens)
        else {
            return false;
        };
        let Some(cached_input_tokens) = self
            .cached_input_tokens
            .checked_add(usage.cached_input_tokens)
        else {
            return false;
        };
        let Some(cache_write_input_tokens) = self
            .cache_write_input_tokens
            .checked_add(usage.cache_write_input_tokens)
        else {
            return false;
        };
        let Some(output_tokens) = self.output_tokens.checked_add(usage.output_tokens) else {
            return false;
        };
        let Some(reasoning_tokens) = self.reasoning_tokens.checked_add(usage.reasoning_tokens)
        else {
            return false;
        };
        *self = Self {
            uncached_input_tokens,
            cached_input_tokens,
            cache_write_input_tokens,
            output_tokens,
            reasoning_tokens,
        };
        true
    }

    /// This is intentionally a transparent, provisional sum rather than a
    /// cost estimate or a claim about provider billing. Providers differ on
    /// whether reasoning and cache-write buckets overlap other counters; raw
    /// buckets stay alongside it so a later definition can change safely.
    fn provisional_effective_token_total(self) -> Option<i64> {
        self.uncached_input_tokens
            .checked_add(self.cached_input_tokens)?
            .checked_add(self.cache_write_input_tokens)?
            .checked_add(self.output_tokens)?
            .checked_add(self.reasoning_tokens)
    }

    fn json_value(self) -> Option<serde_json::Value> {
        Some(serde_json::json!({
            "uncached_input_tokens": self.uncached_input_tokens,
            "cached_input_tokens": self.cached_input_tokens,
            "cache_write_input_tokens": self.cache_write_input_tokens,
            "output_tokens": self.output_tokens,
            "reasoning_tokens": self.reasoning_tokens,
            "provisional_effective_token_total": self.provisional_effective_token_total()?,
            // The durable token schema intentionally contains no provider
            // total/cost columns. Keep that absence explicit; do not derive
            // historical cost from the transient journal.
            "provider_reported_total_tokens": serde_json::Value::Null,
            "provider_reported_cost_usd": serde_json::Value::Null,
            "coverage": {
                "uncached_input_tokens": true,
                "cached_input_tokens": true,
                "cache_write_input_tokens": true,
                "output_tokens": true,
                "reasoning_tokens": true,
                "provisional_effective_token_total": true,
                "provider_reported_total_tokens": false,
                "provider_reported_cost_usd": false,
            },
        }))
    }
}

fn supported_token_purpose(purpose: &str) -> bool {
    matches!(
        purpose,
        "worker" | "reviewer" | "classifier" | "collector" | "planner" | "arbiter"
    )
}

fn has_durable_classifier_invocation(task: &FactsTaskRow) -> bool {
    if !crate::tasks::classification_is_complete(&task.refs) {
        return false;
    }
    let Some(refs) = refs_object(task) else {
        return false;
    };
    refs.get("cx_by")
        .and_then(serde_json::Value::as_str)
        .is_some_and(present_text)
}

fn add_expected_invocation(
    expected: &mut BTreeMap<String, i64>,
    purpose: &str,
    count: i64,
) -> bool {
    add_metric(expected.entry(purpose.to_string()).or_default(), count)
}

fn populate_token_facts(
    intent: &mut IntentFacts,
    members: &[i64],
    task_evidence_by_id: &HashMap<i64, FactsTaskRow>,
    attribution: &AttributionSnapshot,
) {
    let mut role_totals: BTreeMap<String, TokenTotals> = BTreeMap::new();
    let mut total = TokenTotals::default();
    let mut expected_managed_runs = HashSet::new();
    let mut observed_managed_runs = HashSet::new();
    let mut expected_non_managed = BTreeMap::new();
    let mut observed_non_managed = BTreeMap::new();
    let mut complete = true;
    let mut has_usage = false;

    for &task_id in members {
        if attribution.capped_token_usage_task_ids.contains(&task_id) {
            complete = false;
        }
        if attribution.capped_active_run_task_ids.contains(&task_id) {
            complete = false;
        }
        if attribution
            .capped_non_managed_invocation_task_ids
            .contains(&task_id)
            || attribution
                .incomplete_non_managed_invocation_task_ids
                .contains(&task_id)
        {
            complete = false;
        }
        if task_evidence_by_id
            .get(&task_id)
            .is_some_and(has_durable_classifier_invocation)
            && !add_expected_invocation(&mut expected_non_managed, "classifier", 1)
        {
            complete = false;
        }
        if let Some(invocations) = attribution.non_managed_invocations_by_task.get(&task_id) {
            for (purpose, count) in invocations {
                if !add_expected_invocation(&mut expected_non_managed, purpose, *count) {
                    complete = false;
                }
            }
        }
        if let Some(runs) = attribution.active_runs_by_task.get(&task_id) {
            expected_managed_runs.extend(runs.iter().map(|run| run.id));
        }
        for usage in attribution
            .token_usage_by_task
            .get(&task_id)
            .into_iter()
            .flatten()
        {
            has_usage = true;
            if !supported_token_purpose(&usage.purpose) {
                complete = false;
                continue;
            }
            if !total.add_usage(usage.usage)
                || !role_totals
                    .entry(usage.purpose.clone())
                    .or_default()
                    .add_usage(usage.usage)
            {
                complete = false;
            }
            match usage.agent_run_id {
                Some(run_id) => {
                    let matching_run = attribution
                        .active_runs_by_task
                        .get(&task_id)
                        .into_iter()
                        .flatten()
                        .find(|run| run.id == run_id);
                    if matching_run.is_some_and(|run| run.role == usage.purpose) {
                        observed_managed_runs.insert(run_id);
                    } else {
                        complete = false;
                    }
                }
                None if matches!(usage.purpose.as_str(), "worker" | "reviewer") => {
                    // Managed worker/reviewer telemetry has a durable run id.
                    complete = false;
                }
                None => {
                    if !add_metric(
                        observed_non_managed
                            .entry(usage.purpose.clone())
                            .or_default(),
                        1,
                    ) {
                        complete = false;
                    }
                }
            }
        }
    }

    if !has_usage
        || observed_managed_runs != expected_managed_runs
        || expected_non_managed
            .iter()
            .any(|(purpose, expected)| observed_non_managed.get(purpose).unwrap_or(&0) < expected)
    {
        complete = false;
    }
    if !complete {
        return;
    }
    let Some(provisional_effective_token_total) = total.provisional_effective_token_total() else {
        return;
    };
    let mut roles = serde_json::Map::new();
    for (role, totals) in role_totals {
        let Some(value) = totals.json_value() else {
            return;
        };
        roles.insert(role, value);
    }
    intent.role_tokens_usd = Some(serde_json::json!({
        "roles": roles,
        "provisional_effective_token_total": provisional_effective_token_total,
        "provider_reported_total_tokens": serde_json::Value::Null,
        "provider_reported_cost_usd": serde_json::Value::Null,
        "coverage": {
            "uncached_input_tokens": true,
            "cached_input_tokens": true,
            "cache_write_input_tokens": true,
            "output_tokens": true,
            "reasoning_tokens": true,
            "provisional_effective_token_total": true,
            "provider_reported_total_tokens": false,
            "provider_reported_cost_usd": false,
        },
    }));
    intent.coverage.role_tokens_usd = true;
}

fn populate_timing_facts(
    intent: &mut IntentFacts,
    members: &[i64],
    task_evidence_by_id: &HashMap<i64, FactsTaskRow>,
    attribution: &AttributionSnapshot,
) {
    let mut earliest_created_at: Option<i64> = None;
    let mut latest_updated_at: Option<i64> = None;
    let mut wall_complete = true;
    let mut active_model_secs = 0_i64;
    let mut active_complete = true;
    let mut has_active_interval = false;

    for &task_id in members {
        let Some(task) = task_evidence_by_id.get(&task_id) else {
            wall_complete = false;
            active_complete = false;
            continue;
        };
        if task.created_at < 0 || task.updated_at < task.created_at {
            wall_complete = false;
        } else {
            earliest_created_at = Some(
                earliest_created_at.map_or(task.created_at, |value| value.min(task.created_at)),
            );
            latest_updated_at =
                Some(latest_updated_at.map_or(task.updated_at, |value| value.max(task.updated_at)));
        }
        if attribution.capped_active_run_task_ids.contains(&task_id) {
            active_complete = false;
        }
        if attribution
            .capped_non_managed_invocation_task_ids
            .contains(&task_id)
            || attribution
                .incomplete_non_managed_invocation_task_ids
                .contains(&task_id)
            || attribution
                .non_managed_invocations_by_task
                .get(&task_id)
                .is_some_and(|invocations| !invocations.is_empty())
            || has_durable_classifier_invocation(task)
        {
            // Planner/classifier/arbiter/collector work has no durable
            // invocation interval. Never fill that gap with queue, task, or
            // worker/reviewer wall time.
            active_complete = false;
        }
        if attribution
            .token_usage_by_task
            .get(&task_id)
            .into_iter()
            .flatten()
            .any(|usage| {
                usage.agent_run_id.is_none()
                    && matches!(
                        usage.purpose.as_str(),
                        "planner" | "classifier" | "arbiter" | "collector"
                    )
            })
        {
            // Telemetry itself is durable proof of a non-managed model turn,
            // but carries no interval from which active time can be derived.
            active_complete = false;
        }
        for run in attribution
            .active_runs_by_task
            .get(&task_id)
            .into_iter()
            .flatten()
        {
            let Some(ended_at) = run.ended_at else {
                active_complete = false;
                continue;
            };
            let Some(duration) = ended_at.checked_sub(run.spawned_at) else {
                active_complete = false;
                continue;
            };
            if duration < 0 {
                active_complete = false;
                continue;
            }
            let Some(sum) = active_model_secs.checked_add(duration) else {
                active_complete = false;
                continue;
            };
            active_model_secs = sum;
            has_active_interval = true;
        }
    }

    if wall_complete {
        if let (Some(first), Some(last)) = (earliest_created_at, latest_updated_at) {
            intent.wall_secs = last.checked_sub(first);
            intent.coverage.wall_secs = intent.wall_secs.is_some();
        }
    }
    if active_complete && has_active_interval {
        intent.active_model_secs = Some(active_model_secs);
        intent.coverage.active_model_secs = true;
    }
}

enum RunnerEnding {
    Normal,
    Abnormal,
    Unknown,
}

// Keep this list synchronized with the literal end reasons emitted by managed
// worker/reviewer teardown paths. A known controlled teardown is not a runner
// incident merely because it occurs after a stale head, completed review, or
// policy handoff. Unknown is deliberately reserved for persisted text outside
// this producer vocabulary.
const NORMAL_RUNNER_ENDINGS: &[&str] = &[
    "submitted",
    "awaiting_merge",
    "completed",
    "merged",
    "done",
    "approved",
    "in-review",
    "merging",
    "turn-completed",
    "verdict:approved",
    "r2-pending",
    "r2-provision-unavailable",
    "r2-graph-held",
    "r2-ci-pending",
    "r2-ci-failed",
    "r2-no-branch",
    "codex_rereview",
    "ownership_transferred",
    "drain",
    "shutdown",
    "cancelled",
    "stale-sha",
    "stale-authority",
    "merge-metadata-unavailable",
    "approval-no-author",
    "graph-blocker",
    "remediation_lease_unavailable",
    "rework_cap",
    "parked",
    "external",
    "pr_closed",
];

// These literals are source-defined managed teardown failures. They are
// durable abnormal runner endings, unlike a future literal which must keep
// abnormal-ending coverage explicitly unknown.
const ABNORMAL_RUNNER_ENDINGS: &[&str] = &[
    "failed",
    "crashed",
    "agent_failed",
    "idle_reaped",
    "idle",
    "killed",
    "provider_blocked",
    "provision-failed",
    "journal-handoff-failed",
    "terminal_handoff_failed",
    "fallback-route-unavailable",
    "fallback_launch_failed",
    "attachment-failed",
    "r2-spawn-error",
    "verdict:none",
    "daemon_push_failed",
    "daemon_push_rejected",
    "error_retries",
];

fn classify_runner_ending(reason: &str) -> RunnerEnding {
    if NORMAL_RUNNER_ENDINGS.contains(&reason) || reason.starts_with("verdict:changes") {
        RunnerEnding::Normal
    } else if ABNORMAL_RUNNER_ENDINGS.contains(&reason) {
        RunnerEnding::Abnormal
    } else {
        RunnerEnding::Unknown
    }
}

fn add_metric(total: &mut i64, value: i64) -> bool {
    if value < 0 {
        return false;
    }
    let Some(next) = total.checked_add(value) else {
        return false;
    };
    *total = next;
    true
}

fn populate_incident_facts(
    intent: &mut IntentFacts,
    members: &[i64],
    task_evidence_by_id: &HashMap<i64, FactsTaskRow>,
    attribution: &AttributionSnapshot,
) {
    let mut rework_count = 0;
    let mut replan_count = 0;
    let mut provider_failure_count = 0;
    let mut abnormal_runner_ending_count = 0;
    let mut rework_counts_complete = true;
    let mut planning_counts_complete = true;
    let mut abnormal_counts_complete = true;

    for &task_id in members {
        let Some(task) = task_evidence_by_id.get(&task_id) else {
            rework_counts_complete = false;
            planning_counts_complete = false;
            abnormal_counts_complete = false;
            continue;
        };
        rework_counts_complete &= add_metric(&mut rework_count, task.rework_round);

        if attribution
            .incomplete_planning_incident_task_ids
            .contains(&task_id)
            || attribution
                .capped_planning_incident_task_ids
                .contains(&task_id)
        {
            planning_counts_complete = false;
        }
        let planning = attribution
            .planning_incidents_by_task
            .get(&task_id)
            .copied()
            .unwrap_or_default();
        planning_counts_complete &= add_metric(&mut replan_count, planning.replan_count);
        planning_counts_complete &=
            add_metric(&mut provider_failure_count, planning.provider_failure_count);

        if attribution.capped_active_run_task_ids.contains(&task_id) {
            abnormal_counts_complete = false;
        }
        for run in attribution
            .active_runs_by_task
            .get(&task_id)
            .into_iter()
            .flatten()
        {
            let Some(reason) = run.end_reason.as_deref() else {
                abnormal_counts_complete = false;
                continue;
            };
            match classify_runner_ending(reason) {
                RunnerEnding::Normal => {}
                RunnerEnding::Abnormal => {
                    abnormal_counts_complete &= add_metric(&mut abnormal_runner_ending_count, 1);
                }
                RunnerEnding::Unknown => abnormal_counts_complete = false,
            }
        }
    }

    if rework_counts_complete {
        intent.rework_count = Some(rework_count);
        intent.coverage.rework = true;
    }
    if planning_counts_complete {
        intent.replan_count = Some(replan_count);
        intent.provider_failure_count = Some(provider_failure_count);
        intent.coverage.replan = true;
        intent.coverage.provider_failure = true;
    }
    if abnormal_counts_complete {
        intent.abnormal_runner_ending_count = Some(abnormal_runner_ending_count);
        intent.coverage.abnormal_runner_ending = true;
    }

    // `tasks.recovery_attempts` is a resettable budget, not append-only
    // recovery history. The recovery entries in decomposition_attempts cover
    // only explicit delivery adoption and cannot establish all automatic
    // crash/lease recoveries, so this aggregate deliberately remains a gap.
    // Likewise review_collection_runs retains one overwriteable canonical PR
    // result, not collector-attempt history. Neither may become a covered zero
    // merely because a later success replaced a prior failure.
    if let (
        Some(rework),
        Some(recovery),
        Some(replan),
        Some(provider_failures),
        Some(abnormal_endings),
        Some(collector_failures),
    ) = (
        intent.rework_count,
        intent.recovery_count,
        intent.replan_count,
        intent.provider_failure_count,
        intent.abnormal_runner_ending_count,
        intent.collector_failure_count,
    ) {
        intent.incident_count = rework
            .checked_add(recovery)
            .and_then(|value| value.checked_add(replan))
            .and_then(|value| value.checked_add(provider_failures))
            .and_then(|value| value.checked_add(abnormal_endings))
            .and_then(|value| value.checked_add(collector_failures));
        intent.coverage.incident = intent.incident_count.is_some();
    }
}

fn populate_review_quality_facts(
    intent: &mut IntentFacts,
    members: &[i64],
    attribution: &AttributionSnapshot,
) {
    let mut collections = Vec::new();
    let mut findings = Vec::new();
    for &task_id in members {
        if attribution
            .capped_review_collection_task_ids
            .contains(&task_id)
            || attribution
                .capped_review_finding_task_ids
                .contains(&task_id)
        {
            return;
        }
        collections.extend(
            attribution
                .review_collections_by_task
                .get(&task_id)
                .into_iter()
                .flatten(),
        );
        findings.extend(
            attribution
                .review_findings_by_task
                .get(&task_id)
                .into_iter()
                .flatten(),
        );
    }
    if collections.is_empty() || collections.iter().any(|run| run.status != "success") {
        return;
    }

    let mut expected_by_pr = BTreeMap::new();
    for run in collections {
        if run.findings_count < 0 {
            return;
        }
        if expected_by_pr
            .insert(run.pr_number, run.findings_count)
            .is_some()
        {
            return;
        }
    }
    let mut actual_by_pr: BTreeMap<i64, i64> =
        expected_by_pr.keys().copied().map(|pr| (pr, 0)).collect();
    for finding in &findings {
        let Some(count) = actual_by_pr.get_mut(&finding.pr_number) else {
            return;
        };
        if !add_metric(count, 1) {
            return;
        }
    }
    if expected_by_pr
        .iter()
        .any(|(pr, expected)| actual_by_pr.get(pr).copied().unwrap_or(0) != *expected)
    {
        return;
    }

    let mut blocking_count = 0;
    let mut suggestion_count = 0;
    let mut author_pushback_count = 0;
    let mut pushback_accepted_count = 0;
    let mut pushback_overridden_count = 0;
    let mut pushback_unknown_count = 0;
    let mut dispositions = BTreeMap::from([
        ("addressed", 0_i64),
        ("unaddressed", 0_i64),
        ("partial", 0_i64),
        ("unclear", 0_i64),
        ("withdrawn", 0_i64),
    ]);
    let mut disposition_unknown_count = 0;
    for finding in findings {
        match finding.kind.as_str() {
            "blocking" => blocking_count += 1,
            "suggestion" => suggestion_count += 1,
            _ => return,
        }
        if finding.author_pushback {
            author_pushback_count += 1;
            match finding.pushback_accepted {
                Some(true) => pushback_accepted_count += 1,
                Some(false) => pushback_overridden_count += 1,
                None => pushback_unknown_count += 1,
            }
        }
        match finding.addressed_status.as_deref() {
            Some(status) if dispositions.contains_key(status) => {
                *dispositions.get_mut(status).expect("checked key") += 1;
            }
            None => disposition_unknown_count += 1,
            Some(_) => return,
        }
    }
    intent.review_quality = Some(serde_json::json!({
        "finding_count": blocking_count + suggestion_count,
        "finding_kinds": {
            "blocking": blocking_count,
            "suggestion": suggestion_count,
        },
        "disposition_counts": dispositions,
        "disposition_unknown_count": disposition_unknown_count,
        "pushback": {
            "raised_count": author_pushback_count,
            "accepted_count": pushback_accepted_count,
            "overridden_count": pushback_overridden_count,
            "unknown_count": pushback_unknown_count,
        },
    }));
    intent.coverage.review_quality = true;
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
        // Include all bounded durable lineage members as well as cohort rows:
        // a child can be inside a prospective cohort while its source or a
        // sibling/recovery predates the watermark, and attribution belongs to
        // the collapsed intent rather than only its public cohort members.
        let mut attribution_task_ids: BTreeSet<i64> = capped_ids.iter().copied().collect();
        attribution_task_ids.extend(lineage.source_to_children.keys().copied());
        attribution_task_ids.extend(
            lineage
                .source_to_children
                .values()
                .flat_map(|children| children.iter().map(|child| child.task_id)),
        );
        attribution_task_ids.extend(
            lineage
                .recovery_to_original
                .values()
                .flat_map(|pair| [pair.original_task_id, pair.recovery_task_id]),
        );
        let capped_attribution_task_ids: HashSet<i64> = attribution_task_ids
            .iter()
            .skip(MAX_ATTRIBUTION_TASKS_PER_SNAPSHOT)
            .copied()
            .collect();
        let attribution_task_ids: Vec<i64> = attribution_task_ids
            .into_iter()
            .take(MAX_ATTRIBUTION_TASKS_PER_SNAPSHOT)
            .collect();
        // These task rows underpin wall-clock and incident facts too. Load the
        // same bounded attribution membership rather than only graph roots so
        // a collapsed recovery or sibling is never silently omitted.
        let root_tasks = load_facts_tasks_by_id(c, &attribution_task_ids)?;
        let attribution = load_attribution_snapshot(c, &attribution_task_ids)?;
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
            root_tasks,
            attribution,
            capped_attribution_task_ids,
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
        root_tasks,
        attribution,
        capped_attribution_task_ids,
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
    let mut task_evidence_by_id = root_tasks;
    task_evidence_by_id.extend(tasks_by_id.iter().map(|(&id, row)| (id, row.clone())));
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
        if let Some((complexity, provenance)) =
            root_complexity_facts(task_evidence_by_id.get(&root))
        {
            intent.complexity = Some(complexity);
            intent.complexity_provenance = Some(provenance);
            intent.coverage.complexity = true;
        }
        let attribution_members =
            attribution_members_for_intent(root, &members_for_evidence, &lineage);
        populate_attribution(&mut intent, &attribution_members, &attribution);
        populate_token_facts(
            &mut intent,
            &attribution_members,
            &task_evidence_by_id,
            &attribution,
        );
        populate_timing_facts(
            &mut intent,
            &attribution_members,
            &task_evidence_by_id,
            &attribution,
        );
        populate_incident_facts(
            &mut intent,
            &attribution_members,
            &task_evidence_by_id,
            &attribution,
        );
        populate_review_quality_facts(&mut intent, &attribution_members, &attribution);
        if attribution_members
            .iter()
            .any(|task_id| capped_attribution_task_ids.contains(task_id))
        {
            clear_capped_attribution(&mut intent);
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

    fn seed_assignment(
        conn: &Connection,
        task_id: i64,
        role: &str,
        review_stage: Option<&str>,
        pr_number: Option<i64>,
        profile_id: &str,
    ) -> i64 {
        conn.execute(
            "INSERT INTO role_assignments(
                 responsibility_key,task_id,pr_number,role,review_stage,complexity,
                 profile_id,provider,runner,model,effort,pool_key,policy_generation,created_at)
             VALUES (?1,?2,?3,?4,?5,'M',?6,'codex','codex','configured-default','high',
                     ?4,'policy-test',1)",
            rusqlite::params![
                format!("{role}:task:{task_id}:{profile_id}"),
                task_id,
                pr_number,
                role,
                review_stage,
                profile_id,
            ],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn seed_routing_attempt(
        conn: &Connection,
        assignment_id: i64,
        responsibility_key: &str,
        profile_id: &str,
        provider: &str,
        model: &str,
        effort: &str,
    ) -> i64 {
        conn.execute(
            "INSERT INTO routing_attempts(
                 role_assignment_id,responsibility_key,profile_id,provider,runner,model,effort,
                 pool_key,policy_generation,failure_disposition,recorded_at)
             VALUES (?1,?2,?3,?4,?4,?5,?6,'worker','policy-test',NULL,1)",
            rusqlite::params![
                assignment_id,
                responsibility_key,
                profile_id,
                provider,
                model,
                effort,
            ],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    #[allow(clippy::too_many_arguments)]
    fn seed_attributed_run(
        conn: &Connection,
        task_id: i64,
        agent: &str,
        role: &str,
        model: &str,
        provider: &str,
        effort: &str,
        assignment_id: i64,
        configured_profile_id: Option<&str>,
        spawned_at: i64,
        ended_at: i64,
        end_reason: &str,
    ) -> i64 {
        conn.execute(
            "INSERT INTO agent_runs(
                 task_id,agent_name,role,model,effort,provider,role_assignment_id,spawned_at,
                 ended_at,end_reason,configured_profile_id,configured_provider,configured_model,
                 configured_effort)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?6,?4,?5)",
            rusqlite::params![
                task_id,
                agent,
                role,
                model,
                effort,
                provider,
                assignment_id,
                spawned_at,
                ended_at,
                end_reason,
                configured_profile_id,
            ],
        )
        .unwrap();
        conn.last_insert_rowid()
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
    fn facts_attributes_final_worker_and_all_role_attempts_without_defaults() {
        // Real SQLite evidence covers three worker turns on one task: an
        // earlier rework submission, a fallback failure, and the final
        // submitting fallback. The final worker must not be inferred from the
        // first run or from the assignment's configured default.
        let (_d, mut c) = open_tmp();
        let task_id = seed_ordinary(&mut c, 1600);
        c.execute(
            "UPDATE tasks SET labels='[\"complexity:1\"]',refs=?2 WHERE id=?1",
            rusqlite::params![
                task_id,
                serde_json::json!({
                    "merge_commit_sha": format!("{task_id:040x}"),
                    "cx_est": 4,
                    "cx_size": "M",
                    "cx_size_reason": "durable classifier rationale",
                    "cx_ready": true,
                    "cx_not_ready_reason": null,
                    "cx_by": "classifier-test:v3",
                })
                .to_string(),
            ],
        )
        .unwrap();

        let worker_assignment = seed_assignment(&c, task_id, "worker", None, None, "primary");
        let responsibility_key = format!("worker:task:{task_id}:primary");
        let fallback_route = seed_routing_attempt(
            &c,
            worker_assignment,
            &responsibility_key,
            "fallback",
            "claude",
            "claude-fallback",
            "medium",
        );
        let final_route = seed_routing_attempt(
            &c,
            worker_assignment,
            &responsibility_key,
            "final",
            "grok",
            "grok-final",
            "max",
        );
        let first_run = seed_attributed_run(
            &c,
            task_id,
            "rework-worker",
            "worker",
            "claude-initial",
            "claude",
            "high",
            worker_assignment,
            None,
            10,
            20,
            "completed",
        );
        let fallback_run = seed_attributed_run(
            &c,
            task_id,
            "fallback-worker",
            "worker",
            "claude-fallback",
            "claude",
            "medium",
            worker_assignment,
            Some("fallback"),
            30,
            40,
            "failed",
        );
        let final_run = seed_attributed_run(
            &c,
            task_id,
            "final-worker",
            "worker",
            "grok-final",
            "grok",
            "max",
            worker_assignment,
            Some("final"),
            50,
            60,
            "merged",
        );

        let reviewer_assignment =
            seed_assignment(&c, task_id, "reviewer", Some("r1"), Some(77), "reviewer");
        let reviewer_run = seed_attributed_run(
            &c,
            task_id,
            "reviewer-1",
            "reviewer",
            "review-model",
            "claude",
            "high",
            reviewer_assignment,
            None,
            61,
            70,
            "verdict:approved",
        );

        let planner_assignment = seed_assignment(&c, task_id, "planner", None, None, "planner");
        c.execute(
            "INSERT INTO task_decompositions(
                 source_task_id,state,active,freeze_active,planned_source_revision,
                 planner_provider,planner_model,planner_assignment_id,created_at,updated_at)
             VALUES (?1,'completed',0,0,1,'codex','configured-default',?2,1,1)",
            rusqlite::params![task_id, planner_assignment],
        )
        .unwrap();
        let graph_id = c.last_insert_rowid();
        c.execute(
            "INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at)
             VALUES ('planner-run',?1,'planner-1','planner',1)",
            [task_id],
        )
        .unwrap();
        c.execute(
            "INSERT INTO planner_submissions(run_id,graph_id,response_json,rejections,accepted_at)
             VALUES ('planner-run',?1,'[]',0,2)",
            [graph_id],
        )
        .unwrap();

        let before = snapshot_db_state(&c);
        let report = perf_facts(&c, false).unwrap();
        let after = snapshot_db_state(&c);
        assert_eq!(before, after, "facts enrichment must not write");
        let intent = report
            .intents
            .iter()
            .find(|intent| intent.intent_id == format!("intent-{task_id}"))
            .unwrap();

        assert_eq!(
            intent.final_worker.as_ref().unwrap(),
            &serde_json::json!({
                "task_id": task_id,
                "attempt_id": final_run,
                "role_assignment_id": worker_assignment,
                "routing_attempt_id": final_route,
                "agent": "final-worker",
                "role": "worker",
                "review_stage": null,
                "model": "grok-final",
                "provider": "grok",
                "effort": "max",
                "spawned_at": 50,
                "ended_at": 60,
                "end_reason": "merged",
            }),
        );
        let attempts = intent.contributing_attempts.as_ref().unwrap();
        assert_eq!(attempts["worker"].as_array().unwrap().len(), 3);
        assert_eq!(attempts["worker"][0]["attempt_id"], first_run);
        assert_eq!(attempts["worker"][1]["attempt_id"], fallback_run);
        assert_eq!(attempts["worker"][1]["routing_attempt_id"], fallback_route);
        assert_eq!(attempts["worker"][1]["model"], "claude-fallback");
        assert_eq!(attempts["worker"][1]["provider"], "claude");
        assert_eq!(attempts["worker"][1]["effort"], "medium");
        assert_eq!(attempts["worker"][2]["attempt_id"], final_run);
        assert_eq!(attempts["reviewer"][0]["attempt_id"], reviewer_run);
        assert_eq!(attempts["reviewer"][0]["role"], "reviewer");
        assert_eq!(attempts["reviewer"][0]["model"], "review-model");
        assert_eq!(attempts["reviewer"][0]["provider"], "claude");
        assert_eq!(attempts["reviewer"][0]["effort"], "high");
        assert_eq!(attempts["planner"][0]["attempt_id"], "planner-run");
        assert_eq!(attempts["planner"][0]["role"], "planner");
        assert_eq!(attempts["planner"][0]["model"], "configured-default");
        assert_eq!(attempts["planner"][0]["provider"], "codex");
        assert_eq!(attempts["planner"][0]["effort"], "high");
        assert_eq!(intent.complexity.as_ref().unwrap()["cx_est"], 4);
        assert_eq!(
            intent.complexity_provenance.as_ref().unwrap()["cx_by"],
            "classifier-test:v3"
        );
        assert_eq!(
            intent.complexity_provenance.as_ref().unwrap()["task_id"],
            task_id
        );
        let config_inputs = intent.config_evidence.as_ref().unwrap()["policy_fingerprint_inputs"]
            .as_array()
            .unwrap();
        assert!(config_inputs.iter().any(|input| {
            input["routing_attempt_id"] == final_route
                && input["profile_id"] == "final"
                && input["provider"] == "grok"
                && input["policy_generation"] == "policy-test"
        }));
        assert!(intent.coverage.complexity);
        assert!(intent.coverage.config);
        assert!(intent.coverage.final_worker);
        assert!(intent.coverage.contributing_attempts);
    }

    #[test]
    fn facts_missing_classifier_and_managed_run_evidence_stays_uncovered() {
        let (_d, mut c) = open_tmp();
        let task_id = seed_ordinary(&mut c, 1600);
        // A configured assignment is not a substitute for a complete managed
        // run. This malformed historical run has no model/effort, so facts
        // must not fill it from the assignment. The label is deliberately
        // ignored by facts classification.
        c.execute(
            "UPDATE tasks SET labels='[\"complexity:5\"]' WHERE id=?1",
            [task_id],
        )
        .unwrap();
        let assignment = seed_assignment(&c, task_id, "worker", None, None, "configured-only");
        seed_attributed_run(
            &c,
            task_id,
            "missing-model-worker",
            "worker",
            "",
            "codex",
            "",
            assignment,
            None,
            1,
            2,
            "merged",
        );

        let report = perf_facts(&c, false).unwrap();
        let intent = &report.intents[0];
        assert!(intent.complexity.is_none());
        assert!(intent.complexity_provenance.is_none());
        assert!(intent.final_worker.is_none());
        let attempts = intent.contributing_attempts.as_ref().unwrap();
        assert_eq!(attempts["worker"][0]["agent"], "missing-model-worker");
        assert_eq!(attempts["worker"][0]["model"], serde_json::Value::Null);
        assert_eq!(attempts["worker"][0]["effort"], serde_json::Value::Null);
        assert!(intent.config_evidence.is_none());
        assert!(!intent.coverage.complexity);
        assert!(!intent.coverage.final_worker);
        assert!(!intent.coverage.contributing_attempts);
        assert!(!intent.coverage.config);
        assert!(
            !intent.coverage.lineage,
            "standalone lineage is never inferred"
        );
        let wire = serde_json::to_value(intent).unwrap();
        for field in [
            "complexity",
            "complexity_provenance",
            "final_worker",
            "config_evidence",
            "lineage_evidence",
        ] {
            assert_eq!(wire[field], serde_json::Value::Null, "{field}");
        }
    }

    #[test]
    fn facts_final_worker_accepts_submission_lifecycle_evidence() {
        // The lifecycle closes workers as submitted or awaiting_merge before
        // task completion; a still-open worker on an in-review task is also
        // a durable submission. All three must be eligible final workers.
        let (_d, mut c) = open_tmp();
        let submitted_task = seed_ordinary(&mut c, 1600);
        let submitted_assignment =
            seed_assignment(&c, submitted_task, "worker", None, None, "submitted");
        let submitted_run = seed_attributed_run(
            &c,
            submitted_task,
            "submitted-worker",
            "worker",
            "submitted-model",
            "codex",
            "high",
            submitted_assignment,
            None,
            10,
            20,
            "submitted",
        );

        let awaiting_task = seed_ordinary(&mut c, 1700);
        let awaiting_assignment =
            seed_assignment(&c, awaiting_task, "worker", None, None, "awaiting");
        let awaiting_run = seed_attributed_run(
            &c,
            awaiting_task,
            "awaiting-worker",
            "worker",
            "awaiting-model",
            "codex",
            "high",
            awaiting_assignment,
            None,
            30,
            40,
            "awaiting_merge",
        );

        let live_task = seed_task(&mut c, "in-review", None, 0, None, 1000, 1800);
        let live_assignment = seed_assignment(&c, live_task, "worker", None, None, "live");
        let live_run = seed_attributed_run(
            &c,
            live_task,
            "live-submitted-worker",
            "worker",
            "live-model",
            "codex",
            "high",
            live_assignment,
            None,
            50,
            51,
            "ignored-after-open",
        );
        c.execute(
            "UPDATE agent_runs SET ended_at=NULL,end_reason=NULL WHERE id=?1",
            [live_run],
        )
        .unwrap();

        let before = snapshot_db_state(&c);
        let report = perf_facts(&c, false).unwrap();
        assert_eq!(before, snapshot_db_state(&c), "facts must not write");
        for (task_id, run_id, agent) in [
            (submitted_task, submitted_run, "submitted-worker"),
            (awaiting_task, awaiting_run, "awaiting-worker"),
            (live_task, live_run, "live-submitted-worker"),
        ] {
            let intent = report
                .intents
                .iter()
                .find(|intent| intent.intent_id == format!("intent-{task_id}"))
                .unwrap();
            assert_eq!(intent.final_worker.as_ref().unwrap()["attempt_id"], run_id);
            assert_eq!(intent.final_worker.as_ref().unwrap()["agent"], agent);
            assert!(intent.coverage.final_worker);
        }
    }

    #[test]
    fn facts_mixed_incomplete_managed_runs_keep_nulls_and_coverage_gaps() {
        // A complete earlier rework and an incomplete later fallback belong to
        // one intent. The latter must not disappear and make the compound
        // attempt/config evidence look complete.
        let (_d, mut c) = open_tmp();
        let task_id = seed_ordinary(&mut c, 1600);
        let assignment = seed_assignment(&c, task_id, "worker", None, None, "primary");
        let valid_run = seed_attributed_run(
            &c,
            task_id,
            "first-worker",
            "worker",
            "first-model",
            "codex",
            "high",
            assignment,
            None,
            10,
            20,
            "completed",
        );
        let incomplete_run = seed_attributed_run(
            &c,
            task_id,
            "missing-model-worker",
            "worker",
            "",
            "codex",
            "",
            assignment,
            None,
            30,
            40,
            "merged",
        );

        let before = snapshot_db_state(&c);
        let report = perf_facts(&c, false).unwrap();
        assert_eq!(before, snapshot_db_state(&c), "facts must not write");
        let intent = &report.intents[0];
        let attempts = intent.contributing_attempts.as_ref().unwrap()["worker"]
            .as_array()
            .unwrap();
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0]["attempt_id"], valid_run);
        assert_eq!(attempts[1]["attempt_id"], incomplete_run);
        assert_eq!(attempts[1]["model"], serde_json::Value::Null);
        assert_eq!(attempts[1]["effort"], serde_json::Value::Null);
        assert!(intent.final_worker.is_none());
        assert!(intent.config_evidence.is_none());
        assert!(!intent.coverage.final_worker);
        assert!(!intent.coverage.contributing_attempts);
        assert!(!intent.coverage.config);
    }

    #[test]
    fn facts_pre_watermark_graph_root_keeps_attribution_out_of_public_cohort() {
        // The source predates the prospective watermark, but its planner,
        // worker, reviewer, and policy evidence belongs to the child intent.
        // The public contributing ids remain cohort-only.
        let (_d, mut c) = open_tmp();
        c.execute("UPDATE perf_watermark SET watermark=1600 WHERE id=1", [])
            .unwrap();
        let source = seed_task(&mut c, "decomposed", None, 0, None, 1000, 1500);
        let child = seed_ordinary(&mut c, 1700);
        let graph_id = seed_decomposition(&c, source, 1);
        seed_graph_member(&c, graph_id, child, "current-child", 1);

        let worker_assignment = seed_assignment(&c, source, "worker", None, None, "source");
        let worker_run = seed_attributed_run(
            &c,
            source,
            "source-worker",
            "worker",
            "source-worker-model",
            "codex",
            "high",
            worker_assignment,
            None,
            10,
            20,
            "submitted",
        );
        let reviewer_assignment =
            seed_assignment(&c, source, "reviewer", Some("r1"), Some(88), "source-r1");
        let reviewer_run = seed_attributed_run(
            &c,
            source,
            "source-reviewer",
            "reviewer",
            "source-review-model",
            "codex",
            "high",
            reviewer_assignment,
            None,
            21,
            30,
            "verdict:approved",
        );
        let planner_assignment = seed_assignment(&c, source, "planner", None, None, "source-plan");
        c.execute(
            "UPDATE task_decompositions SET planner_assignment_id=?2 WHERE id=?1",
            rusqlite::params![graph_id, planner_assignment],
        )
        .unwrap();
        c.execute(
            "INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at)
             VALUES ('pre-watermark-planner',?1,'source-planner','planner',1)",
            [source],
        )
        .unwrap();
        c.execute(
            "INSERT INTO planner_submissions(run_id,graph_id,response_json,rejections,accepted_at)
             VALUES ('pre-watermark-planner',?1,'[]',0,2)",
            [graph_id],
        )
        .unwrap();

        let before = snapshot_db_state(&c);
        let report = perf_facts(&c, false).unwrap();
        assert_eq!(before, snapshot_db_state(&c), "facts must not write");
        assert_eq!(report.counts.candidate, 1);
        let intent = report
            .intents
            .iter()
            .find(|intent| intent.intent_id == format!("intent-{source}"))
            .unwrap();
        assert_eq!(intent.contributing_task_ids, vec![child]);
        let attempts = intent.contributing_attempts.as_ref().unwrap();
        assert_eq!(attempts["worker"][0]["attempt_id"], worker_run);
        assert_eq!(attempts["reviewer"][0]["attempt_id"], reviewer_run);
        assert_eq!(
            attempts["planner"][0]["attempt_id"],
            "pre-watermark-planner"
        );
        assert!(intent.coverage.contributing_attempts);
        assert!(intent.coverage.config);
        assert_eq!(
            intent.final_worker.as_ref().unwrap()["attempt_id"],
            worker_run
        );
    }

    #[test]
    fn facts_global_attribution_cap_marks_expanded_intent_uncovered() {
        // Reach the global cap through real bounded graph expansion: every
        // prospective child loads its pre-watermark source plus all accepted
        // siblings. The final root has durable worker evidence, but two of
        // its children fall beyond the snapshot cap, so its attribution must
        // be explicitly unavailable rather than a partial subset.
        let (_d, mut c) = open_tmp();
        c.execute("UPDATE perf_watermark SET watermark=1500 WHERE id=1", [])
            .unwrap();
        let graph_count =
            MAX_ATTRIBUTION_TASKS_PER_SNAPSHOT / (crate::decomposition::MAX_CHILDREN + 1) + 1;
        let mut first_root = None;
        let mut cap_affected_root = None;
        for graph_index in 0..graph_count {
            let source = seed_task(&mut c, "decomposed", None, 0, None, 1000, 1000);
            let graph_id = seed_decomposition(&c, source, 1);
            for child_index in 0..crate::decomposition::MAX_CHILDREN {
                let child = seed_ordinary(&mut c, 1600);
                seed_graph_member(
                    &c,
                    graph_id,
                    child,
                    &format!("child-{graph_index}-{child_index}"),
                    1,
                );
            }
            c.execute(
                "UPDATE task_decompositions SET state='completed',active=0 WHERE id=?1",
                [graph_id],
            )
            .unwrap();
            if graph_index == 0 {
                first_root = Some(source);
            }
            if graph_index + 1 == graph_count {
                cap_affected_root = Some(source);
            }
        }
        let first_root = first_root.unwrap();
        let cap_affected_root = cap_affected_root.unwrap();
        let first_assignment = seed_assignment(&c, first_root, "worker", None, None, "first");
        let first_run = seed_attributed_run(
            &c,
            first_root,
            "first-root-worker",
            "worker",
            "first-root-model",
            "codex",
            "high",
            first_assignment,
            None,
            10,
            20,
            "submitted",
        );
        let capped_assignment =
            seed_assignment(&c, cap_affected_root, "worker", None, None, "capped");
        seed_attributed_run(
            &c,
            cap_affected_root,
            "capped-root-worker",
            "worker",
            "capped-root-model",
            "codex",
            "high",
            capped_assignment,
            None,
            30,
            40,
            "submitted",
        );

        let before = snapshot_db_state(&c);
        let snapshot = read_cohort_snapshot(&c, false).unwrap();
        assert_eq!(snapshot.capped_attribution_task_ids.len(), 2);
        let report = perf_facts(&c, false).unwrap();
        assert_eq!(before, snapshot_db_state(&c), "facts must not write");
        assert_eq!(report.intents.len(), graph_count);
        let covered = report
            .intents
            .iter()
            .find(|intent| intent.intent_id == format!("intent-{first_root}"))
            .unwrap();
        assert_eq!(
            covered.final_worker.as_ref().unwrap()["attempt_id"],
            first_run
        );
        assert!(covered.coverage.final_worker);
        assert!(covered.coverage.contributing_attempts);
        assert!(covered.coverage.config);

        let capped = report
            .intents
            .iter()
            .find(|intent| intent.intent_id == format!("intent-{cap_affected_root}"))
            .unwrap();
        assert!(capped.final_worker.is_none());
        assert!(capped.contributing_attempts.is_none());
        assert!(capped.config_evidence.is_none());
        assert!(!capped.coverage.final_worker);
        assert!(!capped.coverage.contributing_attempts);
        assert!(!capped.coverage.config);
    }

    #[test]
    fn facts_metric_coverage_uses_only_durable_evidence() {
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
        // Token and active-model evidence is absent/incomplete. Task timestamps,
        // rework, and the retained planning-attempt ledger can still measure
        // zero; resettable recovery and collector histories cannot.
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
        assert_eq!(i.wall_secs, Some(600));
        assert_eq!(i.rework_count, Some(0));
        assert!(i.recovery_count.is_none());
        assert_eq!(i.replan_count, Some(0));
        assert_eq!(i.provider_failure_count, Some(0));
        assert!(i.abnormal_runner_ending_count.is_none());
        assert!(i.collector_failure_count.is_none());
        assert!(i.incident_count.is_none());
        assert!(i.review_quality.is_none());
        // The unmanaged open run means an abnormal-ending count and total
        // incident count cannot be fabricated as zero.
        for (name, covered) in i.coverage.iter_named() {
            assert_eq!(
                covered,
                matches!(
                    name,
                    "terminal"
                        | "merge_provenance"
                        | "wall_secs"
                        | "rework"
                        | "replan"
                        | "provider_failure"
                ),
                "coverage.{name}"
            );
        }
        // Coverage summary reflects every independently measured fact.
        for (name, fc) in &r.coverage.fields {
            let expected_covered = i64::from(matches!(
                name.as_str(),
                "terminal"
                    | "merge_provenance"
                    | "wall_secs"
                    | "rework"
                    | "replan"
                    | "provider_failure"
            ));
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
        // A missing active-model interval stays null, unlike an explicit
        // measured zero. This is the JSON contract consumers use for gaps.
        let (_d, mut c) = open_tmp();
        seed_ordinary(&mut c, 1600);
        let r = perf_facts(&c, false).unwrap();
        let unpop = serde_json::to_value(&r.intents[0]).unwrap();
        assert_eq!(
            unpop.get("active_model_secs").unwrap(),
            &serde_json::Value::Null
        );

        let mut zeroed = r.intents[0].clone_for_test();
        zeroed.active_model_secs = Some(0);
        zeroed.coverage.active_model_secs = true;
        let pop = serde_json::to_value(&zeroed).unwrap();
        assert_eq!(pop.get("active_model_secs").unwrap(), &serde_json::json!(0));
    }

    #[test]
    fn facts_aggregate_rework_fallback_tokens_time_and_retained_incidents() {
        // One task can retry in-place. Its first failed route and its fallback
        // route are both part of the same intent; neither queue time nor the
        // gap between routes is relabelled as model time.
        let (_d, mut c) = open_tmp();
        let task_id = seed_ordinary(&mut c, 1_600);
        c.execute("UPDATE tasks SET rework_round=1 WHERE id=?1", [task_id])
            .unwrap();
        let assignment = seed_assignment(&c, task_id, "worker", None, None, "primary");
        let first_run = seed_attributed_run(
            &c,
            task_id,
            "first-worker",
            "worker",
            "gpt-primary",
            "codex",
            "high",
            assignment,
            None,
            1_010,
            1_025,
            "fallback-route-unavailable",
        );
        let fallback_profile = "fallback";
        seed_routing_attempt(
            &c,
            assignment,
            &format!("worker:task:{task_id}:primary"),
            fallback_profile,
            "codex",
            "gpt-fallback",
            "medium",
        );
        let fallback_run = seed_attributed_run(
            &c,
            task_id,
            "fallback-worker",
            "worker",
            "gpt-fallback",
            "codex",
            "medium",
            assignment,
            Some(fallback_profile),
            1_200,
            1_230,
            "completed",
        );
        for (run_id, usage) in [
            (
                first_run,
                crate::token_usage::TokenUsage {
                    uncached_input_tokens: 1,
                    cached_input_tokens: 10,
                    cache_write_input_tokens: 100,
                    output_tokens: 1_000,
                    reasoning_tokens: 10_000,
                },
            ),
            (
                fallback_run,
                crate::token_usage::TokenUsage {
                    uncached_input_tokens: 2,
                    cached_input_tokens: 20,
                    cache_write_input_tokens: 200,
                    output_tokens: 2_000,
                    reasoning_tokens: 20_000,
                },
            ),
        ] {
            crate::token_usage::record(
                &mut c,
                Some(run_id),
                "worker",
                &[task_id],
                None,
                "codex",
                "test-model",
                "high",
                usage,
                250,
            )
            .unwrap();
        }
        c.execute(
            "INSERT INTO task_decompositions(
                 source_task_id,state,planned_source_revision,proposal_attempts,
                 provider_failures,created_at,updated_at)
             VALUES (?1,'completed',1,0,0,1,2)",
            [task_id],
        )
        .unwrap();
        let graph_id = c.last_insert_rowid();
        for (kind, count) in [("proposal", 3_i64), ("provider", 4_i64)] {
            for ordinal in 1..=count {
                let retry_generation = (ordinal - 1) / 2;
                c.execute(
                    "INSERT INTO decomposition_attempts(
                         graph_id,source_revision,kind,ordinal,retry_generation,
                         reason_code,summary,created_at)
                     VALUES (?1,1,?2,?3,?4,'retained-attempt','test fixture',?3)",
                    rusqlite::params![graph_id, kind, ordinal, retry_generation],
                )
                .unwrap();
            }
        }

        let before = snapshot_db_state(&c);
        let report = perf_facts(&c, false).unwrap();
        assert_eq!(before, snapshot_db_state(&c), "facts must remain read-only");
        let intent = &report.intents[0];
        assert_eq!(intent.intent_id, format!("intent-{task_id}"));
        assert_eq!(intent.contributing_task_ids, vec![task_id]);

        let tokens = intent.role_tokens_usd.as_ref().unwrap();
        assert_eq!(tokens["roles"]["worker"]["uncached_input_tokens"], 3);
        assert_eq!(tokens["roles"]["worker"]["cached_input_tokens"], 30);
        assert_eq!(tokens["roles"]["worker"]["cache_write_input_tokens"], 300);
        assert_eq!(tokens["roles"]["worker"]["output_tokens"], 3_000);
        assert_eq!(tokens["roles"]["worker"]["reasoning_tokens"], 30_000);
        assert_eq!(
            tokens["roles"]["worker"]["provisional_effective_token_total"],
            33_333
        );
        assert!(tokens["provider_reported_total_tokens"].is_null());
        assert!(tokens["provider_reported_cost_usd"].is_null());
        assert_eq!(tokens["coverage"]["provider_reported_total_tokens"], false);
        assert_eq!(tokens["coverage"]["provider_reported_cost_usd"], false);
        assert!(intent.coverage.role_tokens_usd);

        assert_eq!(intent.active_model_secs, Some(45));
        assert_eq!(intent.wall_secs, Some(600));
        assert!(intent.coverage.active_model_secs);
        assert!(intent.coverage.wall_secs);
        assert_ne!(intent.active_model_secs, intent.wall_secs);
        assert_eq!(intent.rework_count, Some(1));
        assert!(intent.recovery_count.is_none());
        assert_eq!(intent.replan_count, Some(3));
        assert_eq!(intent.provider_failure_count, Some(4));
        assert_eq!(intent.abnormal_runner_ending_count, Some(1));
        assert!(intent.collector_failure_count.is_none());
        assert!(intent.incident_count.is_none());
        assert!(intent.coverage.rework);
        assert!(!intent.coverage.recovery);
        assert!(intent.coverage.replan);
        assert!(intent.coverage.provider_failure);
        assert!(intent.coverage.abnormal_runner_ending);
        assert!(!intent.coverage.collector_failure);
        assert!(!intent.coverage.incident);
    }

    #[test]
    fn facts_retained_planning_attempts_survive_budget_reset() {
        // retry_exhausted_planning resets the live budget but retains every
        // attempt row. Exercise that real SQLite path, then make the source
        // terminal so facts must credit the retained provider failures.
        let (_d, mut c) = open_tmp();
        let task_id = seed_task(&mut c, "failed", None, 0, None, 1_000, 1_600);
        c.execute(
            "INSERT INTO task_decompositions(
             source_task_id,state,active,freeze_active,planned_source_revision,
                 proposal_attempts,provider_failures,operator_retry_count,hold_code,
                 created_at,updated_at)
             VALUES (?1,'held',0,0,1,0,2,0,'provider-attempts-exhausted',1100,1200)",
            [task_id],
        )
        .unwrap();
        let graph_id = c.last_insert_rowid();
        for ordinal in 1..=2_i64 {
            c.execute(
                "INSERT INTO decomposition_attempts(
                     graph_id,source_revision,kind,ordinal,retry_generation,
                     reason_code,summary,created_at)
                 VALUES (?1,1,'provider',?2,0,'provider-failure','fixture',?2)",
                rusqlite::params![graph_id, ordinal],
            )
            .unwrap();
        }
        assert!(matches!(
            crate::decomposition::retry_exhausted_planning(&mut c, task_id, "operator", 1_700)
                .unwrap(),
            crate::decomposition::PlanningRetryOutcome::Retried { .. }
        ));
        assert_eq!(
            c.query_row(
                "SELECT provider_failures FROM task_decompositions WHERE id=?1",
                [graph_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
            0,
            "planning retry resets the current provider budget"
        );
        crate::tasks::close_after_merge_with_merge_commit_sha(
            &mut c,
            task_id,
            "merged fixture",
            &format!("{task_id:040x}"),
            1_800,
        )
        .unwrap();

        let before = snapshot_db_state(&c);
        let report = perf_facts(&c, false).unwrap();
        assert_eq!(before, snapshot_db_state(&c), "facts must remain read-only");
        let intent = &report.intents[0];
        assert_eq!(intent.replan_count, Some(0));
        assert!(intent.coverage.replan);
        assert_eq!(intent.provider_failure_count, Some(2));
        assert!(intent.coverage.provider_failure);
    }

    #[test]
    fn facts_recovery_lineage_aggregates_attempt_metrics_into_root_intent() {
        // Exact recovery adoption collapses a failed generated child and its
        // recovery delivery. Their model work must be credited once to the
        // source intent, never emitted as unrelated attempts.
        let (_d, mut c) = open_tmp();
        let source = seed_ordinary(&mut c, 1_600);
        let original = seed_ordinary(&mut c, 1_650);
        let recovery = seed_ordinary(&mut c, 1_700);
        let graph_id = seed_decomposition(&c, source, 1);
        seed_graph_member(&c, graph_id, original, "child", 1);
        set_refs(
            &c,
            original,
            &serde_json::json!({
                "recovery_delivery": {
                    "source_task": original,
                    "recovery_task": recovery,
                    "pr": 7,
                    "merged_head_sha": "recovered-head",
                }
            })
            .to_string(),
        );
        record_explicit_recovery_adoption(&c, graph_id, original, recovery);
        c.execute("UPDATE tasks SET rework_round=1 WHERE id=?1", [original])
            .unwrap();
        let original_assignment = seed_assignment(&c, original, "worker", None, None, "original");
        let original_run = seed_attributed_run(
            &c,
            original,
            "original-worker",
            "worker",
            "gpt-original",
            "codex",
            "high",
            original_assignment,
            None,
            10,
            30,
            "fallback-route-unavailable",
        );
        let recovery_assignment = seed_assignment(&c, recovery, "worker", None, None, "recovery");
        let recovery_run = seed_attributed_run(
            &c,
            recovery,
            "recovery-worker",
            "worker",
            "gpt-recovery",
            "codex",
            "high",
            recovery_assignment,
            None,
            50,
            80,
            "completed",
        );
        for (run_id, task_id, uncached_input_tokens) in
            [(original_run, original, 11), (recovery_run, recovery, 22)]
        {
            crate::token_usage::record(
                &mut c,
                Some(run_id),
                "worker",
                &[task_id],
                None,
                "codex",
                "test-model",
                "high",
                crate::token_usage::TokenUsage {
                    uncached_input_tokens,
                    ..Default::default()
                },
                100,
            )
            .unwrap();
        }

        let before = snapshot_db_state(&c);
        let report = perf_facts(&c, false).unwrap();
        assert_eq!(before, snapshot_db_state(&c), "facts must remain read-only");
        assert_eq!(report.intents.len(), 1);
        let intent = &report.intents[0];
        assert_eq!(intent.intent_id, format!("intent-{source}"));
        assert_eq!(
            intent.contributing_task_ids,
            vec![source, original, recovery]
        );
        assert_eq!(intent.active_model_secs, Some(50));
        assert_eq!(
            intent.role_tokens_usd.as_ref().unwrap()["roles"]["worker"]["uncached_input_tokens"],
            33
        );
        assert_eq!(intent.rework_count, Some(1));
        assert!(intent.recovery_count.is_none());
        assert!(!intent.coverage.recovery);
        assert_eq!(intent.abnormal_runner_ending_count, Some(1));
        assert!(intent.incident_count.is_none());
        assert!(!intent.coverage.incident);
    }

    #[test]
    fn facts_controlled_reviewer_endings_do_not_hide_abnormal_closes() {
        // A stale-head rework and an R2 handoff are controlled reviewer
        // teardowns; the no-verdict reviewer exit is a known abnormal close.
        // Exercise all three with durable agent_runs rows on real SQLite so a
        // completed later intent reports the measured abnormal count rather
        // than an unknown gap.
        let (_d, mut c) = open_tmp();
        let task_id = seed_ordinary(&mut c, 1_600);
        let mut no_verdict_run = None;
        for (ordinal, agent, reason) in [
            (0, "stale-head-reviewer", "stale-sha"),
            (1, "stale-authority-reviewer", "stale-authority"),
            (2, "r2-handoff-reviewer", "r2-no-branch"),
            (3, "metadata-reviewer", "merge-metadata-unavailable"),
            (4, "no-verdict-reviewer", "verdict:none"),
        ] {
            let reviewer_run = crate::agent_runs::insert_reviewer_with_launch(
                &c,
                task_id,
                agent,
                "gpt-review",
                "high",
                "codex",
                None,
                1_100 + ordinal,
                None,
                &format!("reviewer-cap-{ordinal}"),
                99,
                &format!("{task_id:040x}"),
            )
            .unwrap()
            .unwrap();
            crate::agent_runs::close(&c, reviewer_run, 1_130 + ordinal, reason).unwrap();
            if reason == "verdict:none" {
                no_verdict_run = Some(reviewer_run);
            }
        }
        let reviewer_run = no_verdict_run.unwrap();

        let before = snapshot_db_state(&c);
        let report = perf_facts(&c, false).unwrap();
        assert_eq!(before, snapshot_db_state(&c), "facts must remain read-only");
        let intent = &report.intents[0];
        assert_eq!(intent.abnormal_runner_ending_count, Some(1));
        assert!(intent.coverage.abnormal_runner_ending);

        c.execute(
            "UPDATE agent_runs SET end_reason='future-unrecognized-reason' WHERE id=?1",
            [reviewer_run],
        )
        .unwrap();
        let before = snapshot_db_state(&c);
        let report = perf_facts(&c, false).unwrap();
        assert_eq!(before, snapshot_db_state(&c), "facts must remain read-only");
        let intent = &report.intents[0];
        assert!(intent.abnormal_runner_ending_count.is_none());
        assert!(!intent.coverage.abnormal_runner_ending);
    }

    #[test]
    fn known_runner_ending_vocabulary_is_closed_and_future_text_is_a_gap() {
        for reason in NORMAL_RUNNER_ENDINGS {
            assert!(matches!(
                classify_runner_ending(reason),
                RunnerEnding::Normal
            ));
        }
        for reason in ABNORMAL_RUNNER_ENDINGS {
            assert!(matches!(
                classify_runner_ending(reason),
                RunnerEnding::Abnormal
            ));
        }
        assert!(matches!(
            classify_runner_ending("verdict:changes:rerun"),
            RunnerEnding::Normal
        ));
        assert!(matches!(
            classify_runner_ending("future-unrecognized-reason"),
            RunnerEnding::Unknown
        ));
    }

    #[test]
    fn facts_resettable_recovery_and_collector_history_stay_uncovered() {
        // Use the actual lifecycle reset and collector UPSERT paths on a real
        // temporary SQLite database. A later clean handoff/collection must
        // not turn overwritten prior incidents into measured zeroes.
        let (_d, mut c) = open_tmp();
        let refs = serde_json::json!({
            "cx_est": 3,
            "cx_size": "M",
            "cx_size_reason": "facts fixture",
            "cx_ready": true,
            "cx_not_ready_reason": null,
            "cx_by": "test:v2",
        })
        .to_string();
        let task_id = crate::tasks::create(
            &mut c,
            "owner",
            "resettable incident fixture",
            None,
            0,
            None,
            Some(&refs),
            None,
            None,
            1_000,
        )
        .unwrap();
        c.execute(
            "UPDATE tasks SET recovery_attempts=2 WHERE id=?1",
            [task_id],
        )
        .unwrap();
        crate::tasks::claim(&mut c, "worker", Some(task_id), &[], 3_600, 1_001).unwrap();
        crate::tasks::apply_event(
            &mut c,
            "worker",
            task_id,
            &crate::lifecycle::Event::SignaledDone { pr: "99".into() },
            1_002,
        )
        .unwrap();
        crate::tasks::close_after_merge_with_merge_commit_sha(
            &mut c,
            task_id,
            "merged fixture",
            &format!("{task_id:040x}"),
            1_600,
        )
        .unwrap();
        assert_eq!(
            c.query_row(
                "SELECT recovery_attempts FROM tasks WHERE id=?1",
                [task_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
            0,
            "normal handoff resets the current recovery budget"
        );

        let failed_collection = crate::review_findings::CollectionRun {
            pr_number: 99,
            task_id: Some(task_id),
            status: crate::review_findings::RunStatus::Failed,
            error: Some("collector unavailable".into()),
            collector_model: "test".into(),
            collector_provider: None,
            collector_runner: None,
            collector_effort: None,
            collector_version: "v1".into(),
            findings_count: 0,
            attempted_at: 1_700,
            completed_at: None,
            role_assignment_id: None,
        };
        crate::review_findings::record_run(&c, &failed_collection).unwrap();
        let successful_collection = crate::review_findings::CollectionRun {
            status: crate::review_findings::RunStatus::Success,
            error: None,
            findings_count: 0,
            attempted_at: 1_701,
            completed_at: Some(1_702),
            ..failed_collection
        };
        crate::review_findings::record_run(&c, &successful_collection).unwrap();
        assert_eq!(
            c.query_row(
                "SELECT COUNT(*) FROM review_collection_runs WHERE pr_number=99",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
            1,
            "collector retry overwrites its canonical PR row"
        );
        assert_eq!(
            c.query_row(
                "SELECT status FROM review_collection_runs WHERE pr_number=99",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
            "success"
        );

        let before = snapshot_db_state(&c);
        let report = perf_facts(&c, false).unwrap();
        assert_eq!(before, snapshot_db_state(&c), "facts must remain read-only");
        let intent = &report.intents[0];
        assert!(intent.recovery_count.is_none());
        assert!(!intent.coverage.recovery);
        assert!(intent.collector_failure_count.is_none());
        assert!(!intent.coverage.collector_failure);
        assert!(intent.incident_count.is_none());
        assert!(!intent.coverage.incident);
    }

    #[test]
    fn facts_planner_invocation_requires_telemetry_and_interval_coverage() {
        // A submit_plan row is durable proof that the source planner ran, but
        // planner telemetry is best-effort and planners have no agent_runs
        // interval. Worker evidence must not make either partial aggregate
        // look complete.
        let (_d, mut c) = open_tmp();
        let task_id = seed_ordinary(&mut c, 1_600);
        let worker_assignment = seed_assignment(&c, task_id, "worker", None, None, "worker");
        let worker_run = seed_attributed_run(
            &c,
            task_id,
            "worker",
            "worker",
            "gpt-worker",
            "codex",
            "high",
            worker_assignment,
            None,
            10,
            30,
            "merged",
        );
        crate::token_usage::record(
            &mut c,
            Some(worker_run),
            "worker",
            &[task_id],
            None,
            "codex",
            "gpt-worker",
            "high",
            crate::token_usage::TokenUsage {
                uncached_input_tokens: 7,
                ..Default::default()
            },
            100,
        )
        .unwrap();

        let planner_assignment = seed_assignment(&c, task_id, "planner", None, None, "planner");
        c.execute(
            "INSERT INTO task_decompositions(
                 source_task_id,state,active,freeze_active,planned_source_revision,
                 planner_provider,planner_model,planner_assignment_id,created_at,updated_at)
             VALUES (?1,'completed',0,0,1,'codex','gpt-planner',?2,1,1)",
            rusqlite::params![task_id, planner_assignment],
        )
        .unwrap();
        let graph_id = c.last_insert_rowid();
        c.execute(
            "INSERT INTO run_capabilities(run_id,task_id,agent,role,created_at)
             VALUES ('planner-coverage-run',?1,'planner','planner',1)",
            [task_id],
        )
        .unwrap();
        c.execute(
            "INSERT INTO planner_submissions(run_id,graph_id,response_json,rejections,accepted_at)
             VALUES ('planner-coverage-run',?1,'[]',0,2)",
            [graph_id],
        )
        .unwrap();

        let before = snapshot_db_state(&c);
        let report = perf_facts(&c, false).unwrap();
        assert_eq!(before, snapshot_db_state(&c), "facts must remain read-only");
        let intent = &report.intents[0];
        assert!(intent.role_tokens_usd.is_none());
        assert!(!intent.coverage.role_tokens_usd);
        assert!(intent.active_model_secs.is_none());
        assert!(!intent.coverage.active_model_secs);
        assert_eq!(intent.wall_secs, Some(600));
        assert!(intent.coverage.wall_secs);

        crate::token_usage::record(
            &mut c,
            None,
            "planner",
            &[task_id],
            None,
            "codex",
            "gpt-planner",
            "high",
            crate::token_usage::TokenUsage {
                uncached_input_tokens: 11,
                ..Default::default()
            },
            101,
        )
        .unwrap();
        let before = snapshot_db_state(&c);
        let report = perf_facts(&c, false).unwrap();
        assert_eq!(before, snapshot_db_state(&c), "facts must remain read-only");
        let intent = &report.intents[0];
        assert_eq!(
            intent.role_tokens_usd.as_ref().unwrap()["roles"]["planner"]["uncached_input_tokens"],
            11
        );
        assert!(intent.coverage.role_tokens_usd);
        // Adding a planner snapshot closes only the token gap. It cannot
        // fabricate the planner's active interval from the worker or wall.
        assert!(intent.active_model_secs.is_none());
        assert!(!intent.coverage.active_model_secs);
    }

    #[test]
    fn facts_retained_classifier_and_provider_attempts_require_every_token_snapshot() {
        // Retained planning attempts are the only durable trace after a retry
        // clears current classifier state or a failed provider turn produces
        // no submission. One later/partial snapshot must not certify either
        // role-token aggregate or active-model time.
        let (_d, mut c) = open_tmp();
        let classifier_retry_task = seed_ordinary(&mut c, 1_600);
        let planner_failed_task = seed_ordinary(&mut c, 1_700);
        let classifier_failed_task = seed_ordinary(&mut c, 1_800);

        let seed_worker_usage = |conn: &mut Connection, task_id: i64| {
            let assignment = seed_assignment(conn, task_id, "worker", None, None, "worker");
            let run = seed_attributed_run(
                conn,
                task_id,
                "worker",
                "worker",
                "gpt-worker",
                "codex",
                "high",
                assignment,
                None,
                10,
                30,
                "merged",
            );
            crate::token_usage::record(
                conn,
                Some(run),
                "worker",
                &[task_id],
                None,
                "codex",
                "gpt-worker",
                "high",
                crate::token_usage::TokenUsage {
                    uncached_input_tokens: 7,
                    ..Default::default()
                },
                100,
            )
            .unwrap();
        };
        seed_worker_usage(&mut c, classifier_retry_task);
        seed_worker_usage(&mut c, planner_failed_task);
        seed_worker_usage(&mut c, classifier_failed_task);

        // The retained proposal is the rejected first classifier invocation;
        // the current accepted batch is a second invocation. Only the latter
        // has telemetry, so the classifier subtotal is deliberately unknown.
        let classifier_retry_graph = seed_decomposition(&c, classifier_retry_task, 1);
        c.execute(
            "UPDATE task_decompositions
             SET active=0,accepted_classifications_json='[]' WHERE id=?1",
            [classifier_retry_graph],
        )
        .unwrap();
        seed_decomposition_attempt(
            &c,
            classifier_retry_graph,
            "proposal",
            1,
            "classifier-rejected",
        );
        crate::token_usage::record(
            &mut c,
            None,
            "classifier",
            &[classifier_retry_task],
            None,
            "codex",
            "gpt-classifier",
            "high",
            crate::token_usage::TokenUsage {
                uncached_input_tokens: 11,
                ..Default::default()
            },
            101,
        )
        .unwrap();

        // Provider-failure attempts are recorded after their model process is
        // reaped. Neither failure below has a best-effort snapshot, even
        // though the managed worker evidence is complete.
        let planner_failed_graph = seed_decomposition(&c, planner_failed_task, 1);
        c.execute(
            "UPDATE task_decompositions SET active=0 WHERE id=?1",
            [planner_failed_graph],
        )
        .unwrap();
        seed_decomposition_attempt(&c, planner_failed_graph, "provider", 1, "planner-provider");
        let classifier_failed_graph = seed_decomposition(&c, classifier_failed_task, 1);
        c.execute(
            "UPDATE task_decompositions SET active=0 WHERE id=?1",
            [classifier_failed_graph],
        )
        .unwrap();
        seed_decomposition_attempt(
            &c,
            classifier_failed_graph,
            "provider",
            1,
            "classifier-provider",
        );

        let before = snapshot_db_state(&c);
        let report = perf_facts(&c, false).unwrap();
        assert_eq!(before, snapshot_db_state(&c), "facts must remain read-only");
        let intent_for = |task_id| {
            report
                .intents
                .iter()
                .find(|intent| intent.intent_id == format!("intent-{task_id}"))
                .unwrap()
        };
        for task_id in [
            classifier_retry_task,
            planner_failed_task,
            classifier_failed_task,
        ] {
            let intent = intent_for(task_id);
            assert!(intent.role_tokens_usd.is_none(), "task {task_id}");
            assert!(!intent.coverage.role_tokens_usd, "task {task_id}");
            assert!(intent.active_model_secs.is_none(), "task {task_id}");
            assert!(!intent.coverage.active_model_secs, "task {task_id}");
        }
    }

    #[test]
    fn planner_invocation_probe_uses_graph_index_with_unrelated_history() {
        // The facts snapshot asks by source task, then joins submissions by
        // graph id. Retained planner rows for unrelated graphs must not turn
        // that bounded prefix/probe into a full planner-history scan.
        let (_d, mut c) = open_tmp();
        let source_task = seed_ordinary(&mut c, 1_600);
        let source_graph = seed_decomposition(&c, source_task, 1);
        c.execute(
            "INSERT INTO planner_submissions(run_id,graph_id,response_json,rejections,accepted_at)
             VALUES ('planner-current',?1,'[]',0,1)",
            [source_graph],
        )
        .unwrap();
        for ordinal in 0..64 {
            c.execute(
                "INSERT INTO planner_submissions(run_id,graph_id,response_json,rejections,accepted_at)
                 VALUES (?1,?2,'[]',0,1)",
                rusqlite::params![format!("retained-unrelated-{ordinal}"), 10_000 + ordinal],
            )
            .unwrap();
        }

        let details = c
            .prepare(
                "EXPLAIN QUERY PLAN
                 SELECT submission.run_id
                 FROM task_decompositions AS graph
                 JOIN planner_submissions AS submission ON submission.graph_id=graph.id
                 WHERE graph.source_task_id=?1
                 LIMIT ?2",
            )
            .unwrap()
            .query_map(
                rusqlite::params![source_task, (MAX_ATTRIBUTION_ATTEMPTS_PER_TASK + 1) as i64],
                |row| row.get::<_, String>(3),
            )
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert!(
            details
                .iter()
                .any(|detail| detail.contains("planner_submissions_graph_id")),
            "planner prefix/probe must use graph index: {details:?}"
        );

        let before = snapshot_db_state(&c);
        let report = perf_facts(&c, false).unwrap();
        assert_eq!(before, snapshot_db_state(&c), "facts must remain read-only");
        let intent = report
            .intents
            .iter()
            .find(|intent| intent.contributing_task_ids == vec![source_task])
            .unwrap();
        assert!(intent.role_tokens_usd.is_none());
        assert!(!intent.coverage.role_tokens_usd);
    }

    #[test]
    fn facts_per_task_prefix_probes_mark_every_overflow_uncovered() {
        // Each fixture exceeds its loader by exactly one row. The facts reader
        // needs only that bounded prefix and its probe to fail closed; it must
        // not rank or materialize the retained tail to discover overflow.
        let (_d, mut c) = open_tmp();
        let active_task = seed_ordinary(&mut c, 1_600);
        let token_task = seed_ordinary(&mut c, 1_700);
        let planning_task = seed_ordinary(&mut c, 1_800);
        let collection_task = seed_ordinary(&mut c, 1_900);
        let finding_task = seed_ordinary(&mut c, 2_000);

        for ordinal in 0..=MAX_ATTRIBUTION_ATTEMPTS_PER_TASK {
            c.execute(
                "INSERT INTO agent_runs(
                     task_id,agent_name,role,model,effort,provider,spawned_at,ended_at,end_reason)
                 VALUES (?1,?2,'worker','gpt','high','codex',?3,?4,'done')",
                rusqlite::params![
                    active_task,
                    format!("overflow-worker-{ordinal}"),
                    ordinal as i64,
                    ordinal as i64 + 1,
                ],
            )
            .unwrap();
        }
        for ordinal in 0..=MAX_TOKEN_USAGE_ROWS_PER_TASK {
            crate::token_usage::record(
                &mut c,
                None,
                "classifier",
                &[token_task],
                None,
                "codex",
                "gpt",
                "high",
                crate::token_usage::TokenUsage {
                    uncached_input_tokens: ordinal as i64,
                    ..Default::default()
                },
                ordinal as i64,
            )
            .unwrap();
        }

        c.execute(
            "INSERT INTO task_decompositions(
                 source_task_id,state,active,freeze_active,planned_source_revision,created_at,updated_at)
             VALUES (?1,'completed',0,0,1,1,1)",
            [planning_task],
        )
        .unwrap();
        let graph_id = c.last_insert_rowid();
        for ordinal in 0..=MAX_PLANNING_INCIDENT_ROWS_PER_TASK {
            c.execute(
                "INSERT INTO decomposition_attempts(
                     graph_id,source_revision,kind,ordinal,retry_generation,
                     reason_code,summary,created_at)
                 VALUES (?1,1,'proposal',?2,0,'overflow','fixture',?2)",
                rusqlite::params![graph_id, ordinal as i64],
            )
            .unwrap();
        }
        for ordinal in 0..=MAX_REVIEW_COLLECTION_ROWS_PER_TASK {
            c.execute(
                "INSERT INTO review_collection_runs(
                     pr_number,task_id,status,collector_model,collector_version,
                     findings_count,attempted_at,completed_at)
                 VALUES (?1,?2,'success','test','v1',0,1,1)",
                rusqlite::params![10_000 + ordinal as i64, collection_task],
            )
            .unwrap();
        }
        for ordinal in 0..=MAX_REVIEW_FINDINGS_PER_TASK {
            c.execute(
                "INSERT INTO review_findings(
                     pr_number,task_id,reviewer,kind,author_pushback,text,source_endpoint,created_at)
                 VALUES (?1,?2,'r','suggestion',0,'overflow','pulls',1)",
                rusqlite::params![20_000 + ordinal as i64, finding_task],
            )
            .unwrap();
        }

        let before = snapshot_db_state(&c);
        let report = perf_facts(&c, false).unwrap();
        assert_eq!(before, snapshot_db_state(&c), "facts must remain read-only");
        let intent_for = |task_id| {
            report
                .intents
                .iter()
                .find(|intent| intent.intent_id == format!("intent-{task_id}"))
                .unwrap()
        };
        assert!(intent_for(active_task).active_model_secs.is_none());
        assert!(!intent_for(active_task).coverage.active_model_secs);
        assert!(intent_for(token_task).role_tokens_usd.is_none());
        assert!(!intent_for(token_task).coverage.role_tokens_usd);
        assert!(intent_for(planning_task).replan_count.is_none());
        assert!(!intent_for(planning_task).coverage.replan);
        assert!(intent_for(collection_task).review_quality.is_none());
        assert!(!intent_for(collection_task).coverage.review_quality);
        assert!(intent_for(finding_task).review_quality.is_none());
        assert!(!intent_for(finding_task).coverage.review_quality);
    }

    #[test]
    fn facts_missing_token_and_active_timing_evidence_are_coverage_gaps() {
        let (_d, mut c) = open_tmp();
        let task_id = seed_ordinary(&mut c, 1_600);
        // An unclosed managed run proves neither a completed active interval
        // nor a durable token record. Neither fact may become a zero.
        let assignment = seed_assignment(&c, task_id, "worker", None, None, "primary");
        c.execute(
            "INSERT INTO agent_runs(
                 task_id,agent_name,role,model,effort,provider,role_assignment_id,spawned_at)
             VALUES (?1,'still-running','worker','gpt','high','codex',?2,10)",
            rusqlite::params![task_id, assignment],
        )
        .unwrap();

        let before = snapshot_db_state(&c);
        let report = perf_facts(&c, false).unwrap();
        assert_eq!(before, snapshot_db_state(&c), "facts must remain read-only");
        let intent = &report.intents[0];
        assert!(intent.role_tokens_usd.is_none());
        assert!(!intent.coverage.role_tokens_usd);
        assert!(intent.active_model_secs.is_none());
        assert!(!intent.coverage.active_model_secs);
        // Task lifecycle timestamps are a different, available wall-clock
        // fact; their presence must not fill the active-model gap.
        assert_eq!(intent.wall_secs, Some(600));
        assert!(intent.coverage.wall_secs);
        assert!(intent.abnormal_runner_ending_count.is_none());
        assert!(!intent.coverage.abnormal_runner_ending);
        assert!(intent.incident_count.is_none());
        assert!(!intent.coverage.incident);
    }

    #[test]
    fn facts_review_quality_uses_successful_durable_collection() {
        let (_d, mut c) = open_tmp();
        let task_id = seed_ordinary(&mut c, 1_600);
        c.execute(
            "INSERT INTO review_collection_runs(
                 pr_number,task_id,status,collector_model,collector_version,
                 findings_count,attempted_at,completed_at)
             VALUES (77,?1,'success','test','v1',2,10,11)",
            [task_id],
        )
        .unwrap();
        c.execute(
            "INSERT INTO review_findings(
                 pr_number,task_id,reviewer,kind,author_pushback,pushback_accepted,
                 text,source_endpoint,created_at,addressed_status)
             VALUES
                 (77,?1,'r1','blocking',1,0,'must fix','pulls',10,'addressed'),
                 (77,?1,'r2','suggestion',0,NULL,'consider','issues',10,'unaddressed')",
            [task_id],
        )
        .unwrap();

        let before = snapshot_db_state(&c);
        let report = perf_facts(&c, false).unwrap();
        assert_eq!(before, snapshot_db_state(&c), "facts must remain read-only");
        let intent = &report.intents[0];
        let quality = intent.review_quality.as_ref().unwrap();
        assert_eq!(quality["finding_count"], 2);
        assert_eq!(quality["finding_kinds"]["blocking"], 1);
        assert_eq!(quality["finding_kinds"]["suggestion"], 1);
        assert_eq!(quality["disposition_counts"]["addressed"], 1);
        assert_eq!(quality["disposition_counts"]["unaddressed"], 1);
        assert_eq!(quality["disposition_unknown_count"], 0);
        assert_eq!(quality["pushback"]["raised_count"], 1);
        assert_eq!(quality["pushback"]["overridden_count"], 1);
        assert!(intent.coverage.review_quality);
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
        crate::tasks::close_manual(&mut c, "owner", manual, "resolved elsewhere", None, 1800)
            .unwrap();
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
            None,
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

    fn seed_decomposition_attempt(
        conn: &Connection,
        graph_id: i64,
        kind: &str,
        ordinal: i64,
        reason_code: &str,
    ) {
        conn.execute(
            "INSERT INTO decomposition_attempts(
                 graph_id,source_revision,kind,ordinal,retry_generation,
                 reason_code,summary,created_at)
             VALUES (?1,1,?2,?3,0,?4,'fixture',1)",
            rusqlite::params![graph_id, kind, ordinal, reason_code],
        )
        .unwrap();
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
                provider_failure_count: self.provider_failure_count,
                abnormal_runner_ending_count: self.abnormal_runner_ending_count,
                collector_failure_count: self.collector_failure_count,
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
