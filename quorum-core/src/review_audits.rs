//! R2 review-audit capture: one row per second-reviewer (R2) pass on a PR.
//! Stratified sampling and persistence for adversarial audit of R1 reviews.

use crate::error::Result;
use rusqlite::{params, Connection};
use serde::Serialize;
use std::collections::HashMap;

/// One R2 audit row.
#[derive(Debug, Clone, Serialize)]
pub struct ReviewAudit {
    pub task_id: i64,
    pub pr_number: i64,
    pub r1_run_id: i64,
    pub r2_run_id: i64,
    pub r1_reviewer: String,
    pub r2_reviewer: String,
    pub model: String,
    pub effort: String,
    pub cx_bucket: String,
    pub missed_count: i64,
    pub overcaught_count: i64,
    pub r1_verdict: String,
    pub r2_verdict: Option<String>,
    pub created_at: i64,
}

/// Insert a completed R2 audit row.
pub fn insert(conn: &Connection, audit: &ReviewAudit) -> Result<i64> {
    conn.execute(
        "INSERT INTO review_audits \
         (task_id, pr_number, r1_run_id, r2_run_id, r1_reviewer, r2_reviewer, \
          model, effort, cx_bucket, missed_count, overcaught_count, \
          r1_verdict, r2_verdict, created_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
        params![
            audit.task_id,
            audit.pr_number,
            audit.r1_run_id,
            audit.r2_run_id,
            audit.r1_reviewer,
            audit.r2_reviewer,
            audit.model,
            audit.effort,
            audit.cx_bucket,
            audit.missed_count,
            audit.overcaught_count,
            audit.r1_verdict,
            audit.r2_verdict,
            audit.created_at,
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Stratum key for sampling: (model, effort, cx_bucket).
pub type Stratum = (String, String, String);

/// Count completed R2 audits per stratum.
pub fn stratum_counts(conn: &Connection) -> Result<HashMap<Stratum, i64>> {
    let mut stmt = conn.prepare(
        "SELECT model, effort, cx_bucket, COUNT(*) FROM review_audits GROUP BY model, effort, cx_bucket",
    )?;
    let mut map = HashMap::new();
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, i64>(3)?,
        ))
    })?;
    for row in rows {
        let (model, effort, cx, count) = row?;
        map.insert((model, effort, cx), count);
    }
    Ok(map)
}

/// Decide whether to spawn R2 for a given stratum, given current counts.
/// Returns true if the stratum is under the coverage target, or with
/// probability `steady_state_p` if at/over target.
/// `seed` is used for deterministic pseudo-random in the steady-state case.
pub fn should_sample(
    counts: &HashMap<Stratum, i64>,
    stratum: &Stratum,
    target_per_stratum: i64,
    steady_state_p: f64,
    seed: u64,
) -> bool {
    let current = counts.get(stratum).copied().unwrap_or(0);
    if current < target_per_stratum {
        return true;
    }
    // ponytail: simple hash-based pseudo-random, no external RNG crate needed
    let hash = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    let frac = (hash % 10000) as f64 / 10000.0;
    frac < steady_state_p
}

/// Extract complexity bucket from task labels JSON (mirrors perf.rs logic).
pub fn extract_cx_bucket(labels_json: Option<&str>) -> String {
    let Some(s) = labels_json else {
        return "untagged".to_string();
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(s) else {
        return "untagged".to_string();
    };
    let Some(arr) = v.as_array() else {
        return "untagged".to_string();
    };
    for item in arr {
        if let Some(t) = item.as_str() {
            if let Some(rest) = t.strip_prefix("complexity:") {
                if !rest.is_empty() {
                    return rest.to_string();
                }
            }
        }
    }
    "untagged".to_string()
}

/// Query the stratum key for a given task: (model, effort, cx_bucket).
/// Falls back to the provided defaults if no worker agent_run exists.
pub fn task_stratum(
    conn: &Connection,
    task_id: i64,
    default_model: &str,
    default_effort: &str,
) -> Result<Stratum> {
    let (model, effort): (String, String) = conn
        .query_row(
            "SELECT COALESCE(ar.model, ?1), COALESCE(ar.effort, ?2) \
             FROM tasks t \
             LEFT JOIN ( \
                 SELECT task_id, model, effort, \
                        ROW_NUMBER() OVER (PARTITION BY task_id ORDER BY spawned_at ASC) AS rn \
                 FROM agent_runs WHERE role = 'worker' \
             ) ar ON ar.task_id = t.id AND ar.rn = 1 \
             WHERE t.id = ?3",
            params![default_model, default_effort, task_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap_or_else(|_| (default_model.to_string(), default_effort.to_string()));

    let labels: Option<String> = conn
        .query_row(
            "SELECT labels FROM tasks WHERE id = ?1",
            params![task_id],
            |r| r.get(0),
        )
        .unwrap_or(None);
    let cx = extract_cx_bucket(labels.as_deref());
    Ok((model, effort, cx))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_tmp() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let c = crate::db::open(&dir.path().join("q.db")).unwrap();
        (dir, c)
    }

    #[test]
    fn insert_and_query_round_trip() {
        let (_d, c) = open_tmp();
        let audit = ReviewAudit {
            task_id: 1,
            pr_number: 42,
            r1_run_id: 10,
            r2_run_id: 20,
            r1_reviewer: "Alice".into(),
            r2_reviewer: "Bob".into(),
            model: "claude-opus-4-6".into(),
            effort: "high".into(),
            cx_bucket: "simple".into(),
            missed_count: 2,
            overcaught_count: 1,
            r1_verdict: "approved".into(),
            r2_verdict: Some("changes".into()),
            created_at: 1000,
        };
        let id = insert(&c, &audit).unwrap();
        assert!(id > 0);

        let counts = stratum_counts(&c).unwrap();
        let key = ("claude-opus-4-6".into(), "high".into(), "simple".into());
        assert_eq!(counts.get(&key), Some(&1));
    }

    #[test]
    fn stratum_counts_groups_correctly() {
        let (_d, c) = open_tmp();
        let mut audit = ReviewAudit {
            task_id: 1,
            pr_number: 42,
            r1_run_id: 10,
            r2_run_id: 20,
            r1_reviewer: "A".into(),
            r2_reviewer: "B".into(),
            model: "opus".into(),
            effort: "high".into(),
            cx_bucket: "simple".into(),
            missed_count: 0,
            overcaught_count: 0,
            r1_verdict: "approved".into(),
            r2_verdict: None,
            created_at: 1000,
        };
        insert(&c, &audit).unwrap();
        audit.task_id = 2;
        audit.pr_number = 43;
        insert(&c, &audit).unwrap();
        audit.task_id = 3;
        audit.pr_number = 44;
        audit.cx_bucket = "complex".into();
        insert(&c, &audit).unwrap();

        let counts = stratum_counts(&c).unwrap();
        assert_eq!(
            counts.get(&("opus".into(), "high".into(), "simple".into())),
            Some(&2)
        );
        assert_eq!(
            counts.get(&("opus".into(), "high".into(), "complex".into())),
            Some(&1)
        );
    }

    #[test]
    fn should_sample_under_target_always_true() {
        let counts = HashMap::new();
        let stratum = ("opus".into(), "high".into(), "simple".into());
        for seed in 0..100 {
            assert!(should_sample(&counts, &stratum, 5, 0.10, seed));
        }
    }

    #[test]
    fn should_sample_at_target_uses_probability() {
        let mut counts = HashMap::new();
        let stratum: Stratum = ("opus".into(), "high".into(), "simple".into());
        counts.insert(stratum.clone(), 5);

        let sampled: usize = (0..10000)
            .filter(|&seed| should_sample(&counts, &stratum, 5, 0.10, seed))
            .count();
        // With p=0.10, expect ~1000 out of 10000. Allow wide margin for hash distribution.
        assert!(
            sampled > 500 && sampled < 2000,
            "expected ~1000 samples, got {sampled}"
        );
    }

    #[test]
    fn should_sample_zero_probability_never_samples() {
        let mut counts = HashMap::new();
        let stratum: Stratum = ("opus".into(), "high".into(), "simple".into());
        counts.insert(stratum.clone(), 10);

        let sampled: usize = (0..1000)
            .filter(|&seed| should_sample(&counts, &stratum, 5, 0.0, seed))
            .count();
        assert_eq!(sampled, 0);
    }

    #[test]
    fn extract_cx_bucket_variants() {
        assert_eq!(extract_cx_bucket(None), "untagged");
        assert_eq!(extract_cx_bucket(Some("")), "untagged");
        assert_eq!(extract_cx_bucket(Some("[]")), "untagged");
        assert_eq!(
            extract_cx_bucket(Some(r#"["complexity:simple"]"#)),
            "simple"
        );
        assert_eq!(
            extract_cx_bucket(Some(r#"["kind:bug","complexity:complex"]"#)),
            "complex"
        );
        assert_eq!(extract_cx_bucket(Some(r#"["kind:feature"]"#)), "untagged");
    }
}
