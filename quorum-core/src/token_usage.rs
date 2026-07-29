//! Durable token accounting for managed and daemon-internal model invocations.

use crate::error::Result;
use rusqlite::{params, Connection};
use serde::Serialize;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct TokenUsage {
    pub uncached_input_tokens: i64,
    pub cached_input_tokens: i64,
    pub cache_write_input_tokens: i64,
    pub output_tokens: i64,
    pub reasoning_tokens: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UsageRun {
    pub id: i64,
    pub agent_run_id: Option<i64>,
    pub purpose: String,
    pub pr_number: Option<i64>,
    pub provider: String,
    pub model: String,
    pub effort: String,
    pub usage: TokenUsage,
    pub recorded_at: i64,
    pub task_ids: Vec<i64>,
}

#[allow(clippy::too_many_arguments)]
pub fn record(
    conn: &mut Connection,
    agent_run_id: Option<i64>,
    purpose: &str,
    task_ids: &[i64],
    pr_number: Option<i64>,
    provider: &str,
    model: &str,
    effort: &str,
    usage: TokenUsage,
    recorded_at: i64,
) -> Result<i64> {
    let tx = crate::db::begin_immediate(conn)?;
    // Token telemetry is also written by daemon-internal classifier and
    // collector turns, which do not pass through another command mutation.
    // Reclaim one bounded retention batch in this transaction so those
    // telemetry-only write paths cannot grow without bound.
    crate::sweep::sweep_token_usage_on_write(&tx, recorded_at)?;
    tx.execute(
        "INSERT INTO token_usage_runs
            (agent_run_id, purpose, pr_number, provider, model, effort,
             uncached_input_tokens, cached_input_tokens, cache_write_input_tokens,
             output_tokens, reasoning_tokens, recorded_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
         ON CONFLICT(agent_run_id) DO UPDATE SET
             purpose=excluded.purpose,
             pr_number=excluded.pr_number,
             provider=excluded.provider,
             model=excluded.model,
             effort=excluded.effort,
             uncached_input_tokens=excluded.uncached_input_tokens,
             cached_input_tokens=excluded.cached_input_tokens,
             cache_write_input_tokens=excluded.cache_write_input_tokens,
             output_tokens=excluded.output_tokens,
             reasoning_tokens=excluded.reasoning_tokens,
             recorded_at=excluded.recorded_at",
        params![
            agent_run_id,
            purpose,
            pr_number,
            provider,
            model,
            effort,
            usage.uncached_input_tokens,
            usage.cached_input_tokens,
            usage.cache_write_input_tokens,
            usage.output_tokens,
            usage.reasoning_tokens,
            recorded_at,
        ],
    )?;
    let run_id = match agent_run_id {
        Some(agent_run_id) => tx.query_row(
            "SELECT id FROM token_usage_runs WHERE agent_run_id = ?1",
            [agent_run_id],
            |row| row.get(0),
        )?,
        None => tx.last_insert_rowid(),
    };
    for task_id in task_ids {
        tx.execute(
            "INSERT OR IGNORE INTO token_usage_run_tasks (run_id, task_id) VALUES (?1, ?2)",
            params![run_id, task_id],
        )?;
    }
    tx.commit()?;
    Ok(run_id)
}

pub fn for_task(conn: &Connection, task_id: i64) -> Result<Vec<UsageRun>> {
    let mut stmt = conn.prepare(
        "SELECT r.id, r.agent_run_id, r.purpose, r.pr_number, r.provider, r.model,
                r.effort, r.uncached_input_tokens, r.cached_input_tokens,
                r.cache_write_input_tokens, r.output_tokens, r.reasoning_tokens,
                r.recorded_at
         FROM token_usage_runs r
         JOIN token_usage_run_tasks t ON t.run_id = r.id
         WHERE t.task_id = ?1
         ORDER BY r.id",
    )?;
    let mut rows = stmt.query([task_id])?;
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        let id = row.get(0)?;
        let mut tasks = conn.prepare(
            "SELECT task_id FROM token_usage_run_tasks WHERE run_id = ?1 ORDER BY task_id",
        )?;
        let task_ids = tasks
            .query_map([id], |task| task.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        result.push(UsageRun {
            id,
            agent_run_id: row.get(1)?,
            purpose: row.get(2)?,
            pr_number: row.get(3)?,
            provider: row.get(4)?,
            model: row.get(5)?,
            effort: row.get(6)?,
            usage: TokenUsage {
                uncached_input_tokens: row.get(7)?,
                cached_input_tokens: row.get(8)?,
                cache_write_input_tokens: row.get(9)?,
                output_tokens: row.get(10)?,
                reasoning_tokens: row.get(11)?,
            },
            recorded_at: row.get(12)?,
            task_ids,
        });
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_usage_survives_run_close_with_breakdown_intact() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = crate::db::open(&dir.path().join("q.db")).unwrap();
        let run_id =
            crate::agent_runs::insert(&conn, 7, "A", "worker", "gpt", "high", "codex", 1).unwrap();
        crate::agent_runs::close(&conn, run_id, 2, "done").unwrap();
        let observed = TokenUsage {
            uncached_input_tokens: 82_265,
            cached_input_tokens: 1_294_080,
            cache_write_input_tokens: 0,
            output_tokens: 6_691,
            reasoning_tokens: 3_518,
        };
        record(
            &mut conn,
            Some(run_id),
            "worker",
            &[7],
            Some(450),
            "codex",
            "gpt-5.6-sol",
            "high",
            observed,
            3,
        )
        .unwrap();

        let stored = for_task(&conn, 7).unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].usage, observed);
        assert_eq!(stored[0].usage.uncached_input_tokens, 82_265);
        assert_eq!(stored[0].usage.cached_input_tokens, 1_294_080);
        let ended_at: i64 = conn
            .query_row(
                "SELECT ended_at FROM agent_runs WHERE id=?1",
                [run_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ended_at, 2);
    }

    #[test]
    fn classifier_and_collector_are_attributable_without_agent_runs() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = crate::db::open(&dir.path().join("q.db")).unwrap();
        let usage = TokenUsage {
            uncached_input_tokens: 10,
            cached_input_tokens: 20,
            output_tokens: 3,
            reasoning_tokens: 2,
            ..Default::default()
        };
        record(
            &mut conn,
            None,
            "classifier",
            &[7, 8],
            None,
            "claude",
            "haiku",
            "low",
            usage,
            1,
        )
        .unwrap();
        record(
            &mut conn,
            None,
            "collector",
            &[7],
            Some(450),
            "codex",
            "gpt",
            "medium",
            usage,
            2,
        )
        .unwrap();

        let purposes: Vec<_> = for_task(&conn, 7)
            .unwrap()
            .into_iter()
            .map(|run| run.purpose)
            .collect();
        assert_eq!(purposes, ["classifier", "collector"]);
        assert!(for_task(&conn, 8)
            .unwrap()
            .iter()
            .any(|run| run.purpose == "classifier"));
    }

    #[test]
    fn telemetry_only_writes_reclaim_expired_runs_and_mappings_in_bounded_batches() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = crate::db::open(&dir.path().join("q.db")).unwrap();
        let now = 10_000_000;
        let expired_at = now - crate::sweep::TOKEN_USAGE_RETENTION_SECS;

        for task_id in 1..=(crate::sweep::SWEEP_LIMIT as i64 + 1) {
            record(
                &mut conn,
                None,
                "classifier",
                &[task_id],
                None,
                "codex",
                "gpt-5.6-terra",
                "medium",
                TokenUsage::default(),
                expired_at,
            )
            .unwrap();
        }

        record(
            &mut conn,
            None,
            "classifier",
            &[999],
            None,
            "codex",
            "gpt-5.6-terra",
            "medium",
            TokenUsage::default(),
            now,
        )
        .unwrap();

        let expired_remaining: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM token_usage_runs WHERE recorded_at <= ?1",
                [expired_at],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(expired_remaining, 1, "one bounded batch must be reclaimed");
        let orphaned_mappings: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM token_usage_run_tasks t
                 WHERE NOT EXISTS (SELECT 1 FROM token_usage_runs r WHERE r.id=t.run_id)",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            orphaned_mappings, 0,
            "reclaimed runs must take their mappings"
        );

        record(
            &mut conn,
            None,
            "collector",
            &[1000],
            Some(464),
            "codex",
            "gpt-5.6-terra",
            "medium",
            TokenUsage::default(),
            now,
        )
        .unwrap();

        let expired_remaining: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM token_usage_runs WHERE recorded_at <= ?1",
                [expired_at],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            expired_remaining, 0,
            "later telemetry-only writes drain the backlog"
        );
        let remaining_mappings: i64 = conn
            .query_row("SELECT COUNT(*) FROM token_usage_run_tasks", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(
            remaining_mappings, 2,
            "only the two live writes remain mapped"
        );
    }
}
