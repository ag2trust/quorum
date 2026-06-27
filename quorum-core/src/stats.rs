//! Read-only health snapshot for `quorum status`. Every count applies the same logical
//! `expires_at > now` / presence read-filter as the rest of the system, so a snapshot never
//! reports expired rows or stale-as-online agents.
//!
//! Issue #77 enriched the snapshot into an operator dashboard:
//! - `agents` — per-online-agent view with derived tier + current task + last-seen age.
//! - `queue_by_tier` — open-task count grouped by `tier:*` label (untiered + review bucket).
//! - `recent_messages` — last 5 messages (from/kind/age/preview).
//! - `claim_ttls` — active claims with time-to-expiry.
//! - `throughput` — closed-last-hour + oldest-done-awaiting-review (catches review-loop stalls).

use crate::error::Result;
use rusqlite::{params, Connection};
use serde::Serialize;

/// How many recent messages to surface on `status`. Bounded to keep the output cheap.
pub const RECENT_MSG_LIMIT: i64 = 5;
/// Body-preview length per recent message. Beyond this is truncated with an ellipsis.
pub const MSG_PREVIEW_CHARS: usize = 80;
/// A `done` task older than this is "stuck awaiting review" — surfaces stalled review loops.
pub const DONE_STUCK_THRESHOLD_SECS: i64 = 30 * 60;

/// Per-status task count.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct StatusCount {
    pub status: String,
    pub count: i64,
}

/// A recent error row.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ErrorRow {
    pub ts: i64,
    pub source: String,
    pub detail: String,
}

/// One online agent — what tier they appear to operate at and what they're doing right now.
/// Tier is derived from the active claim's task labels (`tier:*`); `unknown` when the agent
/// holds no active task. The agent table has no tier column by design (agents announce tier
/// via `--match-label tier:X` on sync; tier is a property of the work they pick, not them).
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct AgentView {
    pub id: String,
    pub tier: String,
    /// `Some({id,title})` if holding an active task; `None` = idle.
    pub current_task: Option<AgentCurrentTask>,
    /// Seconds since `last_seen`.
    pub last_seen_age_secs: i64,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct AgentCurrentTask {
    pub id: i64,
    pub title: String,
}

/// Open-task count grouped by required tier label. `tier` is either a `tier:*` value
/// (e.g. `tier:opus-47`), `untiered` (open tasks with no `tier:` label), or `review`
/// (open `kind:review` tasks — they're tier-exempt and routed separately, see #73 fix).
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct TierQueueCount {
    pub tier: String,
    pub open: i64,
}

/// A recent feed message — last N rows, oldest-first within the window.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct RecentMessage {
    pub seq: i64,
    pub ts: i64,
    pub age_secs: i64,
    pub author: String,
    pub kind: String,
    pub body_preview: String,
}

/// An active claim with time-to-expiry. Negative `expires_in_secs` means already-lapsed
/// (the reaper will clean it on the next sweep); flag in the renderer.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct ClaimTtl {
    pub target: String,
    pub holder: String,
    pub expires_in_secs: i64,
}

/// Throughput / queue-health metrics — surfaces review-loop stalls early.
#[derive(Debug, Serialize, PartialEq, Eq, Default)]
pub struct Throughput {
    /// Tasks transitioned to `closed` in the last hour (proxy for review-loop velocity).
    pub closed_last_hour: i64,
    /// Tasks currently `done` (submitted, awaiting review verdict).
    pub done_awaiting_review: i64,
    /// Age in seconds of the oldest `done`-status task (i.e. the worst review-loop stall).
    /// `None` when no `done` tasks exist.
    pub oldest_done_awaiting_review_secs: Option<i64>,
    /// Count of `done`-status tasks older than [`DONE_STUCK_THRESHOLD_SECS`].
    pub done_stuck_count: i64,
}

/// A point-in-time snapshot of the store.
#[derive(Debug, Serialize, PartialEq, Eq, Default)]
pub struct Stats {
    pub agents_total: i64,
    pub agents_online: i64,
    pub messages_live: i64,
    pub claims_active: i64,
    pub tasks: Vec<StatusCount>,
    pub errors_live: i64,
    pub last_errors: Vec<ErrorRow>,
    /// Issue #77: per-online-agent view (tier + current task + last_seen age).
    pub agents: Vec<AgentView>,
    /// Issue #77: open-task count grouped by required tier.
    pub queue_by_tier: Vec<TierQueueCount>,
    /// Issue #77: last RECENT_MSG_LIMIT feed messages.
    pub recent_messages: Vec<RecentMessage>,
    /// Issue #77: active claims with time-to-expiry.
    pub claim_ttls: Vec<ClaimTtl>,
    /// Issue #77: throughput / review-loop-stall metrics.
    pub throughput: Throughput,
}

/// Gather a snapshot. Read-only.
pub fn stats(conn: &Connection, now: i64, online_window: i64) -> Result<Stats> {
    let one = |sql: &str, p: &[&dyn rusqlite::ToSql]| -> Result<i64> {
        Ok(conn.query_row(sql, p, |r| r.get(0))?)
    };

    let agents_total = one("SELECT count(*) FROM agents", &[])?;
    let agents_online = one(
        "SELECT count(*) FROM agents WHERE (?1 - last_seen) < ?2",
        &[&now, &online_window],
    )?;
    let messages_live = one(
        "SELECT count(*) FROM messages WHERE expires_at > ?1",
        &[&now],
    )?;
    let claims_active = one(
        "SELECT count(*) FROM claims WHERE active=1 AND expires_at > ?1",
        &[&now],
    )?;
    let errors_live = one("SELECT count(*) FROM errors WHERE expires_at > ?1", &[&now])?;

    let mut tstmt =
        conn.prepare("SELECT status, count(*) FROM tasks GROUP BY status ORDER BY status")?;
    let tasks = tstmt
        .query_map([], |r| {
            Ok(StatusCount {
                status: r.get(0)?,
                count: r.get(1)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut estmt = conn.prepare(
        "SELECT ts, source, detail FROM errors WHERE expires_at > ?1 ORDER BY id DESC LIMIT 5",
    )?;
    let last_errors = estmt
        .query_map(params![now], |r| {
            Ok(ErrorRow {
                ts: r.get(0)?,
                source: r.get(1)?,
                detail: r.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let agents = online_agents_view(conn, now, online_window)?;
    let queue_by_tier = queue_by_tier(conn)?;
    let recent_messages = recent_messages(conn, now)?;
    let claim_ttls = claim_ttls(conn, now)?;
    let throughput = throughput(conn, now)?;

    Ok(Stats {
        agents_total,
        agents_online,
        messages_live,
        claims_active,
        tasks,
        errors_live,
        last_errors,
        agents,
        queue_by_tier,
        recent_messages,
        claim_ttls,
        throughput,
    })
}

/// Per-online-agent view. Tier derived from their active task's labels — see
/// [`extract_tier_from_labels`]. Sorted by tier ascending, then id ascending — deterministic
/// so the watch loop's output is stable frame-to-frame.
fn online_agents_view(conn: &Connection, now: i64, online_window: i64) -> Result<Vec<AgentView>> {
    let mut stmt = conn.prepare(
        "SELECT a.id, a.last_seen, t.id, t.title, t.labels
         FROM agents a
         LEFT JOIN claims c
           ON c.holder = a.id
          AND c.active = 1
          AND c.expires_at > ?1
          AND c.target LIKE 'task#%'
         LEFT JOIN tasks t
           ON t.id = CAST(SUBSTR(c.target, 6) AS INTEGER)
         WHERE (?1 - a.last_seen) < ?2
         ORDER BY a.id ASC",
    )?;
    let mut views: Vec<AgentView> = stmt
        .query_map(params![now, online_window], |r| {
            let id: String = r.get(0)?;
            let last_seen: i64 = r.get(1)?;
            let task_id: Option<i64> = r.get(2)?;
            let task_title: Option<String> = r.get(3)?;
            let labels: Option<String> = r.get(4)?;
            let current_task = task_id
                .zip(task_title)
                .map(|(id, title)| AgentCurrentTask { id, title });
            let tier = extract_tier_from_labels(labels.as_deref());
            Ok(AgentView {
                id,
                tier,
                current_task,
                last_seen_age_secs: (now - last_seen).max(0),
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    // Stable display order: by tier then id.
    views.sort_by(|a, b| a.tier.cmp(&b.tier).then_with(|| a.id.cmp(&b.id)));
    Ok(views)
}

/// Parse `tier:*` out of a JSON-array labels string. Returns the first matching label
/// verbatim (e.g. `tier:opus-47`), or `unknown` when none / no labels / unparseable.
/// Keeps the SQL path tier-agnostic — tier vocabulary lives in agent/CTO conventions.
pub fn extract_tier_from_labels(labels_json: Option<&str>) -> String {
    let s = match labels_json {
        Some(s) => s,
        None => return "unknown".to_string(),
    };
    let v: serde_json::Value = match serde_json::from_str(s) {
        Ok(v) => v,
        Err(_) => return "unknown".to_string(),
    };
    let arr = match v.as_array() {
        Some(a) => a,
        None => return "unknown".to_string(),
    };
    for item in arr {
        if let Some(t) = item.as_str() {
            if let Some(rest) = t.strip_prefix("tier:") {
                if !rest.is_empty() {
                    return t.to_string();
                }
            }
        }
    }
    "unknown".to_string()
}

/// Open-task count grouped by required tier. Uses [`extract_tier_from_labels`] over each
/// open task row in app-space (the labels are JSON text in a TEXT column — pure SQL grouping
/// would need a JSON SQL extension we don't depend on). `kind:review` open tasks land in a
/// distinct `review` bucket because they're tier-exempt at the matcher (#73 fix).
fn queue_by_tier(conn: &Connection) -> Result<Vec<TierQueueCount>> {
    let mut stmt = conn.prepare("SELECT labels FROM tasks WHERE status='open'")?;
    let labels_rows = stmt
        .query_map([], |r| {
            let l: Option<String> = r.get(0)?;
            Ok(l)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut counts: std::collections::BTreeMap<String, i64> = std::collections::BTreeMap::new();
    for labels in labels_rows {
        let bucket = if has_label(labels.as_deref(), "kind:review") {
            "review".to_string()
        } else {
            let t = extract_tier_from_labels(labels.as_deref());
            if t == "unknown" {
                "untiered".to_string()
            } else {
                t
            }
        };
        *counts.entry(bucket).or_insert(0) += 1;
    }
    Ok(counts
        .into_iter()
        .map(|(tier, open)| TierQueueCount { tier, open })
        .collect())
}

/// Quick "does this labels JSON contain a given label string" helper.
pub fn has_label(labels_json: Option<&str>, target: &str) -> bool {
    let s = match labels_json {
        Some(s) => s,
        None => return false,
    };
    let v: serde_json::Value = match serde_json::from_str(s) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let arr = match v.as_array() {
        Some(a) => a,
        None => return false,
    };
    arr.iter().any(|x| x.as_str() == Some(target))
}

/// Last [`RECENT_MSG_LIMIT`] feed messages (newest first), with a bounded body preview.
fn recent_messages(conn: &Connection, now: i64) -> Result<Vec<RecentMessage>> {
    let mut stmt = conn.prepare(
        "SELECT seq, ts, author, kind, body
         FROM messages
         WHERE expires_at > ?1
         ORDER BY seq DESC
         LIMIT ?2",
    )?;
    let rows = stmt
        .query_map(params![now, RECENT_MSG_LIMIT], |r| {
            let body: String = r.get(4)?;
            let preview: String = body
                .chars()
                .take(MSG_PREVIEW_CHARS)
                .collect::<String>()
                .replace(['\n', '\r'], " ");
            let trimmed = if body.chars().count() > MSG_PREVIEW_CHARS {
                format!("{preview}…")
            } else {
                preview
            };
            let ts: i64 = r.get(1)?;
            Ok(RecentMessage {
                seq: r.get(0)?,
                ts,
                age_secs: (now - ts).max(0),
                author: r.get(2)?,
                kind: r.get(3)?,
                body_preview: trimmed,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Active claims with time-to-expiry, ordered soonest-to-expire first (the dashboard's
/// most actionable angle — what's about to lapse).
fn claim_ttls(conn: &Connection, now: i64) -> Result<Vec<ClaimTtl>> {
    let mut stmt = conn.prepare(
        "SELECT target, holder, expires_at
         FROM claims
         WHERE active=1 AND expires_at > ?1
         ORDER BY expires_at ASC",
    )?;
    let rows = stmt
        .query_map(params![now], |r| {
            let expires_at: i64 = r.get(2)?;
            Ok(ClaimTtl {
                target: r.get(0)?,
                holder: r.get(1)?,
                expires_in_secs: expires_at - now,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Throughput / review-loop-stall metrics.
fn throughput(conn: &Connection, now: i64) -> Result<Throughput> {
    let hour_ago = now - 3600;
    let closed_last_hour: i64 = conn.query_row(
        "SELECT count(*) FROM tasks WHERE status='closed' AND updated_at > ?1",
        params![hour_ago],
        |r| r.get(0),
    )?;
    let done_awaiting_review: i64 =
        conn.query_row("SELECT count(*) FROM tasks WHERE status='done'", [], |r| {
            r.get(0)
        })?;
    let oldest_done_ts: Option<i64> = conn
        .query_row(
            "SELECT MIN(updated_at) FROM tasks WHERE status='done'",
            [],
            |r| r.get(0),
        )
        .ok();
    let oldest_done_awaiting_review_secs = oldest_done_ts.map(|ts| (now - ts).max(0));
    let stuck_threshold = now - DONE_STUCK_THRESHOLD_SECS;
    let done_stuck_count: i64 = conn.query_row(
        "SELECT count(*) FROM tasks WHERE status='done' AND updated_at < ?1",
        params![stuck_threshold],
        |r| r.get(0),
    )?;
    Ok(Throughput {
        closed_last_hour,
        done_awaiting_review,
        oldest_done_awaiting_review_secs,
        done_stuck_count,
    })
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
    fn counts_exclude_expired_and_stale() {
        let (_d, mut c) = open_tmp();
        crate::feed::post(&mut c, "A", "info", None, "live", None, None, 1000, 100).unwrap();
        crate::feed::post(&mut c, "A", "info", None, "dead", None, None, 5, 100).unwrap();
        crate::claims::claim(&mut c, "A", "pr#1", 1000, 100).unwrap();
        crate::tasks::create(&mut c, "A", "t", None, 0, None, None, None, 100).unwrap();

        let s = stats(&c, 500, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(s.messages_live, 1);
        assert_eq!(s.claims_active, 1);
        assert_eq!(s.agents_total, 1);
        assert_eq!(s.agents_online, 0);
        assert_eq!(
            s.tasks,
            vec![StatusCount {
                status: "open".into(),
                count: 1
            }]
        );
        assert_eq!(s.errors_live, 0);
    }

    // --- Issue #77 dashboard fields ----------------------------------------

    #[test]
    fn extract_tier_finds_tier_label() {
        assert_eq!(
            extract_tier_from_labels(Some(r#"["foo","tier:opus-47","bar"]"#)),
            "tier:opus-47"
        );
        assert_eq!(
            extract_tier_from_labels(Some(r#"["foo","bar"]"#)),
            "unknown"
        );
        assert_eq!(extract_tier_from_labels(None), "unknown");
        assert_eq!(extract_tier_from_labels(Some("not json")), "unknown");
        assert_eq!(extract_tier_from_labels(Some(r#"["tier:"]"#)), "unknown");
    }

    #[test]
    fn has_label_matches_exactly() {
        assert!(has_label(
            Some(r#"["kind:review","tier:opus-47"]"#),
            "kind:review"
        ));
        assert!(!has_label(Some(r#"["kind:bug"]"#), "kind:review"));
        assert!(!has_label(None, "kind:review"));
    }

    #[test]
    fn agents_view_includes_tier_from_current_task() {
        let (_d, mut c) = open_tmp();
        // Two tasks at different tiers.
        let t46 = crate::tasks::create(
            &mut c,
            "boss",
            "t46",
            None,
            0,
            Some("[\"tier:opus-46\"]"),
            None,
            None,
            100,
        )
        .unwrap();
        let t47 = crate::tasks::create(
            &mut c,
            "boss",
            "t47",
            None,
            0,
            Some("[\"tier:opus-47\"]"),
            None,
            None,
            100,
        )
        .unwrap();
        // Two agents claim them.
        crate::tasks::claim(&mut c, "Alice", Some(t46), &[], 1000, 100).unwrap();
        crate::tasks::claim(&mut c, "Bob", Some(t47), &[], 1000, 100).unwrap();

        let s = stats(&c, 200, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        // Both online, both have a current task with a tier.
        let by_id: std::collections::HashMap<_, _> =
            s.agents.iter().map(|a| (a.id.as_str(), a)).collect();
        assert_eq!(by_id["Alice"].tier, "tier:opus-46");
        assert_eq!(by_id["Alice"].current_task.as_ref().unwrap().id, t46);
        assert_eq!(by_id["Bob"].tier, "tier:opus-47");
        assert_eq!(by_id["Bob"].current_task.as_ref().unwrap().id, t47);
    }

    #[test]
    fn agents_view_unknown_tier_when_idle() {
        let (_d, mut c) = open_tmp();
        // Agent posts a message (touches presence) but holds no task.
        crate::feed::post(&mut c, "Idle", "info", None, "hi", None, None, 1000, 100).unwrap();
        let s = stats(&c, 200, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        let a = s.agents.iter().find(|a| a.id == "Idle").unwrap();
        assert_eq!(a.tier, "unknown");
        assert!(a.current_task.is_none());
    }

    #[test]
    fn queue_by_tier_buckets_correctly() {
        let (_d, mut c) = open_tmp();
        crate::tasks::create(
            &mut c,
            "boss",
            "a",
            None,
            0,
            Some("[\"tier:opus-47\"]"),
            None,
            None,
            100,
        )
        .unwrap();
        crate::tasks::create(
            &mut c,
            "boss",
            "b",
            None,
            0,
            Some("[\"tier:opus-47\"]"),
            None,
            None,
            100,
        )
        .unwrap();
        crate::tasks::create(
            &mut c,
            "boss",
            "c",
            None,
            0,
            Some("[\"tier:opus-46\"]"),
            None,
            None,
            100,
        )
        .unwrap();
        crate::tasks::create(&mut c, "boss", "d", None, 0, None, None, None, 100).unwrap();
        crate::tasks::create(
            &mut c,
            "boss",
            "r",
            None,
            1000,
            Some("[\"kind:review\"]"),
            None,
            None,
            100,
        )
        .unwrap();

        let s = stats(&c, 200, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        let by_tier: std::collections::HashMap<_, _> = s
            .queue_by_tier
            .iter()
            .map(|q| (q.tier.as_str(), q.open))
            .collect();
        assert_eq!(by_tier.get("tier:opus-47"), Some(&2));
        assert_eq!(by_tier.get("tier:opus-46"), Some(&1));
        assert_eq!(by_tier.get("untiered"), Some(&1));
        assert_eq!(by_tier.get("review"), Some(&1));
    }

    #[test]
    fn recent_messages_limit_and_preview() {
        let (_d, mut c) = open_tmp();
        for i in 0..(RECENT_MSG_LIMIT + 3) {
            crate::feed::post(
                &mut c,
                "A",
                "info",
                None,
                &format!("msg-{i}"),
                None,
                None,
                1000,
                100 + i,
            )
            .unwrap();
        }
        let long_body = "x".repeat(MSG_PREVIEW_CHARS + 50);
        crate::feed::post(&mut c, "A", "info", None, &long_body, None, None, 1000, 200).unwrap();

        let s = stats(&c, 300, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(s.recent_messages.len() as i64, RECENT_MSG_LIMIT);
        // Newest first (the long body).
        assert!(
            s.recent_messages[0].body_preview.ends_with('…'),
            "long body must be truncated with ellipsis, got: {}",
            s.recent_messages[0].body_preview
        );
        assert!(s.recent_messages[0].body_preview.chars().count() == MSG_PREVIEW_CHARS + 1);
        // +1 for ellipsis
    }

    #[test]
    fn claim_ttls_orders_soonest_first() {
        let (_d, mut c) = open_tmp();
        // Different holders so `agents::touch` auto-renewal of in-flight leases (which
        // happens inside every write — see CLAUDE.md "Leases auto-renew on touch") doesn't
        // cross-renew across our test claims and re-order the expected times.
        crate::claims::claim(&mut c, "A", "pr#1", 100, 1000).unwrap(); // expires 1100
        crate::claims::claim(&mut c, "B", "pr#2", 1000, 1000).unwrap(); // expires 2000
        crate::claims::claim(&mut c, "C", "pr#3", 500, 1000).unwrap(); // expires 1500

        let s = stats(&c, 1050, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        let order: Vec<_> = s.claim_ttls.iter().map(|x| x.target.as_str()).collect();
        assert_eq!(order, vec!["pr#1", "pr#3", "pr#2"]);
        assert!(s.claim_ttls[0].expires_in_secs > 0);
    }

    fn done(status: &str) -> crate::tasks::TaskUpdate<'_> {
        crate::tasks::TaskUpdate {
            status: Some(status),
            body: None,
            refs: None,
            verdict: None,
        }
    }

    #[test]
    fn throughput_counts_oldest_done_awaiting_review() {
        let (_d, mut c) = open_tmp();
        // Two tasks, both driven to `done` at different times. No closed transitions:
        // closing requires the review-verdict path (approve/changes), which is exercised
        // in tasks::tests. Stats only reads the resulting state — pin that here.
        let t1 =
            crate::tasks::create(&mut c, "boss", "t1", None, 0, None, None, None, 100).unwrap();
        let t2 =
            crate::tasks::create(&mut c, "boss", "t2", None, 0, None, None, None, 200).unwrap();
        let t3_open =
            crate::tasks::create(&mut c, "boss", "t3", None, 0, None, None, None, 300).unwrap();
        crate::tasks::claim(&mut c, "Alice", Some(t1), &[], 1000, 400).unwrap();
        crate::tasks::update(&mut c, "Alice", t1, &done("done"), 400).unwrap();
        crate::tasks::claim(&mut c, "Bob", Some(t2), &[], 1000, 500).unwrap();
        crate::tasks::update(&mut c, "Bob", t2, &done("done"), 500).unwrap();

        let now = 600;
        let s = stats(&c, now, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(s.throughput.done_awaiting_review, 2);
        // t1 went done at 400; now=600 → age 200s. t1 is older than t2.
        assert_eq!(s.throughput.oldest_done_awaiting_review_secs, Some(200));
        // 200s < DONE_STUCK_THRESHOLD_SECS (30 min) → not stuck.
        assert_eq!(s.throughput.done_stuck_count, 0);
        assert!(crate::tasks::get(&c, t3_open).unwrap().is_some());
    }

    #[test]
    fn throughput_done_stuck_flagged_after_threshold() {
        let (_d, mut c) = open_tmp();
        let t =
            crate::tasks::create(&mut c, "boss", "stuck", None, 0, None, None, None, 100).unwrap();
        crate::tasks::claim(&mut c, "Alice", Some(t), &[], 10000, 100).unwrap();
        crate::tasks::update(&mut c, "Alice", t, &done("done"), 100).unwrap();
        // Now far enough in the future for it to be stuck.
        let now = 100 + DONE_STUCK_THRESHOLD_SECS + 60;
        let s = stats(&c, now, crate::agents::ONLINE_WINDOW_SECS).unwrap();
        assert_eq!(s.throughput.done_stuck_count, 1);
        assert!(s.throughput.oldest_done_awaiting_review_secs.unwrap() > DONE_STUCK_THRESHOLD_SECS);
    }
}
