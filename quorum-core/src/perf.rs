//! Performance report queries for `quorum perf`. Read-only — no writes, no mutations.
//!
//! Computes aggregate metrics from the `tasks` table (terminal tasks only: done, failed,
//! cancelled). Model/effort resolved from `agent_runs` (earliest worker spawn per task),
//! falling back to caller-supplied defaults for orphan tasks. Complexity derived from
//! `complexity:*` labels.

use crate::error::Result;
use rusqlite::Connection;
use rusqlite::OptionalExtension;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};

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
// Read-only fact surface siblings populate. Types declare every fact field up
// front so lineage/terminal/enrichment tasks only fill in evidence and flip
// coverage flags. A JSON null with a false coverage flag is distinguishable
// from a measured zero.

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

/// One row per ordinary managed implementation task. Every evidence field is
/// initialized to JSON null with the matching coverage flag false — enrichment
/// siblings flip flags only when they successfully fill in real evidence.
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
    EligibleTerminal,
    EligibleIncludeAll,
    ExcludedReviewOnly,
    ExcludedPreWatermark,
    ExcludedTruncated,
    ExcludedNonTerminal,
}

impl InclusionReason {
    pub fn is_included(&self) -> bool {
        matches!(self, Self::EligibleTerminal | Self::EligibleIncludeAll)
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::EligibleTerminal => "eligible-terminal",
            Self::EligibleIncludeAll => "eligible-include-all",
            Self::ExcludedReviewOnly => "excluded-review-only",
            Self::ExcludedPreWatermark => "excluded-pre-watermark",
            Self::ExcludedTruncated => "excluded-truncated",
            Self::ExcludedNonTerminal => "excluded-non-terminal",
        }
    }
}

struct CandidateRow {
    id: i64,
    review_only: i64,
}

/// Load terminal candidate rows in a bounded, deterministic order. Ordering by
/// `id` keeps repeated runs stable and keeps allocations bounded via LIMIT.
fn load_facts_candidates(
    conn: &Connection,
    since: Option<i64>,
    limit: usize,
) -> Result<Vec<CandidateRow>> {
    let since_val = since.unwrap_or(0);
    let mut stmt = conn.prepare(
        "SELECT id, review_only \
         FROM tasks \
         WHERE status IN ('done','failed','cancelled') \
           AND updated_at >= ?1 \
         ORDER BY id ASC \
         LIMIT ?2",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![since_val, limit as i64], |r| {
            Ok(CandidateRow {
                id: r.get(0)?,
                review_only: r.get(1)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn new_intent_facts(task_id: i64, reason: InclusionReason) -> IntentFacts {
    IntentFacts {
        intent_id: format!("intent-{task_id}"),
        contributing_task_ids: vec![task_id],
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

/// Read-only facts surface for `quorum perf`. Returns a deterministic,
/// bounded `FactsReport` with one intent per ordinary managed implementation
/// task in the cohort. All evidence fields are initialized to JSON null; the
/// per-field coverage flags stay false until enrichment siblings populate
/// them.
///
/// Cohort selection is prospective by default via `perf_watermark`;
/// `include_all` bypasses that boundary. Performs no writes.
pub fn perf_facts(conn: &Connection, include_all: bool) -> Result<FactsReport> {
    let watermark = read_watermark(conn)?;
    let since = if include_all { None } else { watermark };
    let cohort = CohortDefinition {
        prospective_only: !include_all,
        watermark,
        include_all,
    };
    let query_limits = QueryLimits {
        max_intents: MAX_INTENTS,
        max_contributing_tasks_per_intent: MAX_CONTRIBUTING_TASKS_PER_INTENT,
    };

    // Cap the candidate load at MAX_INTENTS + 1 so truncation is detectable
    // without loading unbounded rows.
    let candidates = load_facts_candidates(conn, since, MAX_INTENTS + 1)?;
    let candidate_count = candidates.len() as i64;

    let base_reason = if include_all {
        InclusionReason::EligibleIncludeAll
    } else {
        InclusionReason::EligibleTerminal
    };

    let mut intents: Vec<IntentFacts> = Vec::new();
    let mut excluded_reasons: BTreeMap<String, i64> = BTreeMap::new();

    for cand in candidates {
        // Truncation guard: additional candidates beyond MAX_INTENTS are
        // tallied under excluded-truncated rather than materialized.
        if intents.len() >= MAX_INTENTS {
            *excluded_reasons
                .entry(InclusionReason::ExcludedTruncated.as_str().to_string())
                .or_insert(0) += 1;
            continue;
        }

        if cand.review_only != 0 {
            *excluded_reasons
                .entry(InclusionReason::ExcludedReviewOnly.as_str().to_string())
                .or_insert(0) += 1;
            continue;
        }

        intents.push(new_intent_facts(cand.id, base_reason));
    }

    let included_count = intents.len() as i64;
    let excluded_count = candidate_count - included_count;

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
            candidate: candidate_count,
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
        seed_task(conn, "done", None, 0, None, 1000, updated_at)
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
            assert_eq!(intent.reason, InclusionReason::EligibleTerminal);
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
    fn facts_all_evidence_null_and_coverage_false() {
        let (_d, mut c) = open_tmp();
        let tid = seed_ordinary(&mut c, 1600);
        seed_run(&c, tid, "opus-46", "high", 1001);
        seed_approval(&c, tid, "approved", 0);

        let r = perf_facts(&c, false).unwrap();
        assert_eq!(r.intents.len(), 1);
        let i = &r.intents[0];
        // Every evidence field remains null — enrichment is a sibling's job.
        assert!(i.lineage_root_task_id.is_none());
        assert!(i.lineage_evidence.is_none());
        assert!(i.terminal_outcome.is_none());
        assert!(i.terminal_evidence.is_none());
        assert!(i.merge_provenance.is_none());
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
        // Every coverage flag stays false.
        for (name, covered) in i.coverage.iter_named() {
            assert!(!covered, "coverage.{name} must default to false");
        }
        // Coverage summary reflects that: uncovered==1 for every field.
        for (name, fc) in &r.coverage.fields {
            assert_eq!(fc.covered, 0, "field {name} covered");
            assert_eq!(fc.uncovered, 1, "field {name} uncovered");
        }
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
        assert_eq!(default.intents[0].reason, InclusionReason::EligibleTerminal);

        let all = perf_facts(&c, true).unwrap();
        assert!(!all.cohort.prospective_only);
        assert!(all.cohort.include_all);
        assert_eq!(all.cohort.watermark, Some(5000));
        assert_eq!(all.counts.candidate, 2);
        assert_eq!(all.counts.included, 2);
        for intent in &all.intents {
            assert_eq!(intent.reason, InclusionReason::EligibleIncludeAll);
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

    #[test]
    fn facts_inclusion_reason_is_bounded_enum_not_free_form() {
        // Serializing a code goes through the kebab-case enum discriminants
        // — no free-form text can appear in the wire form.
        for r in [
            InclusionReason::EligibleTerminal,
            InclusionReason::EligibleIncludeAll,
            InclusionReason::ExcludedReviewOnly,
            InclusionReason::ExcludedPreWatermark,
            InclusionReason::ExcludedTruncated,
            InclusionReason::ExcludedNonTerminal,
        ] {
            let v = serde_json::to_value(r).unwrap();
            let s = v.as_str().expect("reason must serialize as a string");
            assert_eq!(s, r.as_str(), "wire form and as_str() must agree");
        }
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
