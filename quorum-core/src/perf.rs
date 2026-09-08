//! Performance report queries for `quorum perf`. Read-only — no writes, no mutations.
//!
//! Computes aggregate metrics from the `tasks` table (terminal tasks only: done, failed,
//! cancelled). Model/effort resolved from `agent_runs` (earliest worker spawn per task),
//! falling back to caller-supplied defaults for orphan tasks. Complexity derived from
//! `complexity:*` labels.

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

/// Load bounded ordinary (`review_only = 0`) implementation intent ids in a
/// deterministic order. Filtering at the SQL layer keeps the sentinel LIMIT
/// dedicated to intent capacity — review-only rows can never squeeze ordinary
/// tasks out of the returned set.
fn load_ordinary_intent_ids(
    conn: &Connection,
    since: Option<i64>,
    limit: usize,
) -> Result<Vec<i64>> {
    let since_val = since.unwrap_or(0);
    let mut stmt = conn.prepare(
        "SELECT id \
         FROM tasks \
         WHERE status IN ('done','failed','cancelled') \
           AND updated_at >= ?1 \
           AND review_only = 0 \
         ORDER BY id ASC \
         LIMIT ?2",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![since_val, limit as i64], |r| r.get(0))?
        .collect::<rusqlite::Result<Vec<i64>>>()?;
    Ok(rows)
}

/// Aggregate counts over the terminal cohort partitioned by `review_only`.
/// One bounded query, one row read — no per-row allocation.
struct CandidateCounts {
    ordinary: i64,
    review_only: i64,
}

fn count_candidates(conn: &Connection, since: Option<i64>) -> Result<CandidateCounts> {
    let since_val = since.unwrap_or(0);
    let (ordinary, review_only) = conn.query_row(
        "SELECT \
             SUM(CASE WHEN review_only = 0 THEN 1 ELSE 0 END), \
             SUM(CASE WHEN review_only = 1 THEN 1 ELSE 0 END) \
         FROM tasks \
         WHERE status IN ('done','failed','cancelled') \
           AND updated_at >= ?1",
        rusqlite::params![since_val],
        |r| {
            Ok((
                r.get::<_, Option<i64>>(0)?.unwrap_or(0),
                r.get::<_, Option<i64>>(1)?.unwrap_or(0),
            ))
        },
    )?;
    Ok(CandidateCounts {
        ordinary,
        review_only,
    })
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
    counts: CandidateCounts,
    ordinary_ids: Vec<i64>,
    lineage: LineageSnapshot,
}

/// Take the watermark, aggregate counts, and bounded ordinary-id scan under
/// a single WAL read snapshot so their totals are internally consistent —
/// even if a daemon lifecycle write commits between conceptual steps. If the
/// caller already owns a transaction, its existing snapshot is reused
/// instead of nesting a second.
fn read_cohort_snapshot(conn: &Connection, include_all: bool) -> Result<CohortSnapshot> {
    let read = |c: &Connection| -> Result<CohortSnapshot> {
        let watermark = read_watermark(c)?;
        let since = if include_all { None } else { watermark };
        // First SELECT establishes the snapshot; subsequent reads see it.
        let counts = count_candidates(c, since)?;
        let ordinary_ids = load_ordinary_intent_ids(c, since, MAX_INTENTS + 1)?;
        // The sentinel row detects truncation but is not part of the facts
        // cohort, so lineage reads never expand beyond MAX_INTENTS ids.
        let lineage =
            build_lineage_snapshot(c, &ordinary_ids[..ordinary_ids.len().min(MAX_INTENTS)])?;
        Ok(CohortSnapshot {
            watermark,
            counts,
            ordinary_ids,
            lineage,
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
/// bounded `FactsReport` with one intent per ordinary managed implementation
/// task in the cohort. All evidence fields are initialized to JSON null; the
/// per-field coverage flags stay false until enrichment siblings populate
/// them.
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
        counts,
        ordinary_ids,
        lineage,
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

    let candidate_count = counts.ordinary + counts.review_only;
    let truncated = ordinary_ids.len() > MAX_INTENTS;

    let base_reason = if include_all {
        InclusionReason::EligibleIncludeAll
    } else {
        InclusionReason::EligibleTerminal
    };

    // Collapse ordinary cohort tasks into their canonical intent roots via
    // durable lineage (graph membership, recovery-delivery provenance).
    // Group by root_id in BTreeMap ordering for deterministic output.
    let capped_ids: Vec<i64> = ordinary_ids.iter().take(MAX_INTENTS).copied().collect();
    let mut groups: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
    for id in &capped_ids {
        let root = canonical_root(*id, &lineage);
        groups.entry(root).or_default().push(*id);
    }

    let mut intents: Vec<IntentFacts> = Vec::with_capacity(groups.len());
    for (root, mut members) in groups {
        members.sort();
        let members_for_evidence = members.clone();
        let evidence = build_lineage_evidence(root, &members_for_evidence, &lineage);
        // Clip contributing task ids to the per-intent bound.
        let contributing: Vec<i64> = members
            .into_iter()
            .take(MAX_CONTRIBUTING_TASKS_PER_INTENT)
            .collect();
        let mut intent = new_intent_facts(root, contributing, base_reason);
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
    if counts.review_only > 0 {
        excluded_reasons.insert(
            InclusionReason::ExcludedReviewOnly.as_str().to_string(),
            counts.review_only,
        );
    }
    if truncated {
        // Bounded aggregate lets us report the exact truncated excess without
        // loading the overflow tail.
        let overflow = (counts.ordinary - MAX_INTENTS as i64).max(0);
        if overflow > 0 {
            excluded_reasons.insert(
                InclusionReason::ExcludedTruncated.as_str().to_string(),
                overflow,
            );
        }
    }

    // `included` counts terminal cohort tasks that contributed to some
    // emitted intent (either as root or as folded detail). With collapse it
    // may exceed `intents.len()` — the intent count is available separately
    // via `intents.len()`. For non-collapsed cohorts this preserves the
    // historical `included == intents.len()` identity.
    let included_count = capped_ids.len() as i64;
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
        // Every other evidence field remains null — enrichment is a sibling's job.
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
        // Every coverage flag stays false — nothing is covered on a standalone
        // intent until sibling enrichment lands.
        for (name, covered) in i.coverage.iter_named() {
            assert!(!covered, "coverage.{name} must default to false");
        }
        // Coverage summary reflects that: every field is uncovered==1.
        for (name, fc) in &r.coverage.fields {
            assert_eq!(fc.covered, 0, "field {name} covered count");
            assert_eq!(fc.uncovered, 1, "field {name} uncovered count");
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

        // Direct SQL guard: fetching ordinary intents with a sentinel of 3
        // must return exactly the three ordinary ids, in stable order.
        let ids = load_ordinary_intent_ids(&c, None, 3).unwrap();
        assert_eq!(ids, vec![o1, o2, o3]);

        // End-to-end accounting: candidate = 5 + 3, all three ordinary rows
        // are included, review-only rows are tallied by reason code.
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
        // All three terminal tasks contributed; nothing was excluded as
        // "children folded away".
        assert_eq!(r.counts.candidate, 3);
        assert_eq!(r.counts.included, 3);
        assert_eq!(r.counts.excluded, 0);
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
        // S itself is not in the terminal cohort — but children still fold to S.
        assert_eq!(intent.contributing_task_ids, vec![c1, c2]);
        assert_eq!(intent.lineage_root_task_id, Some(s));
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
        assert_eq!(r.counts.candidate, 5);
        assert_eq!(r.counts.included, 5);
        assert_eq!(r.counts.excluded, 0);

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
