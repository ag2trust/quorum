//! Performance report queries for `quorum perf`. Read-only — no writes, no mutations.
//!
//! Computes aggregate metrics from the `tasks` table (terminal tasks only: done, failed,
//! cancelled). Model/effort resolved from `agent_runs` (earliest worker spawn per task),
//! falling back to caller-supplied defaults for orphan tasks. Complexity derived from
//! `complexity:*` labels.

use crate::error::Result;
use rusqlite::Connection;
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
) -> Result<Vec<TaskRow>> {
    let mut stmt = conn.prepare(
        "SELECT t.id, t.status, t.labels, t.rework_round, t.created_at, t.updated_at, t.reviewer, \
                COALESCE(ar.model, ?1), COALESCE(ar.effort, ?2) \
         FROM tasks t \
         LEFT JOIN ( \
             SELECT task_id, model, effort, \
                    ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY spawned_at ASC) AS rn \
             FROM agent_runs WHERE role = 'worker' \
         ) ar ON ar.task_id = t.id AND ar.rn = 1 \
         WHERE t.status IN ('done', 'failed', 'cancelled')",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![default_model, default_effort], |r| {
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
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn load_reviewer_durations(conn: &Connection) -> Result<HashMap<i64, Vec<i64>>> {
    let mut stmt = conn.prepare(
        "SELECT task_id, ended_at - spawned_at \
         FROM agent_runs \
         WHERE role = 'reviewer' AND ended_at IS NOT NULL",
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
    let tasks = load_terminal_tasks(conn, default_model, default_effort)?;
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
        crate::agent_runs::insert(conn, task_id, "agent", "worker", model, effort, spawned_at)
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
            conn, task_id, agent, "reviewer", "opus-46", "high", spawned_at,
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
        crate::agent_runs::insert(&c, tid, "rev", "reviewer", "claude-opus-4-8", "max", 1001)
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
}
