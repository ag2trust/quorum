//! The shared work queue.
//!
//! Tasks move through five states: `open → claimed → done → closed`, plus terminal
//! `cancelled`. The agent's write footprint per task is exactly two calls — `task-claim`
//! (`open → claimed`) then `task-update --status done` (`claimed → done`). The reviewer
//! (issue #10) drives `done → closed` (terminal) or `done → open` (rework); this module
//! exposes `closed` as a valid value but does not own that transition.
//!
//! Claiming is atomic via a guarded `UPDATE ... WHERE status='open' RETURNING`, so two agents
//! can never claim the same task. A claim also takes a **renewable lease** on `task#<id>`
//! (reusing the `claims` table): the assignee `renew`s on long work, and a lapsed lease lets
//! the sweep-on-write reaper return the task to `open` (see `sweep::reap_lapsed_tasks`).
//! `done` tasks are physically reclaimed by the sweeper after a TTL.

use crate::db::begin_immediate;
use crate::error::{QuorumError, Result};
use crate::sweep::SWEEP_LIMIT;
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::Serialize;

/// Valid task statuses. `open`/`claimed` are system-driven (claim/release/reaper); `done` is
/// the executor's only settable status; `closed`/`cancelled` are terminal (review #10 / cancel).
pub const STATUSES: &[&str] = &["open", "claimed", "done", "closed", "cancelled"];

/// Default task-claim lease TTL when `--ttl` is omitted. The assignee renews on long work;
/// a lapsed lease lets the reaper return the task to `open`.
pub const DEFAULT_LEASE_TTL_SECS: i64 = 3600;

/// Label that marks a task as a review task (issue #10). The claim path uses this to enforce
/// "no self-review" (an agent whose `orig` equals the caller is filtered out) and reviews
/// don't auto-spawn reviews. Stored inside the `labels` JSON array.
pub const REVIEW_LABEL: &str = "kind:review";

/// Default sticky-reopen window (issue #10). When a reviewer's `changes` verdict reopens a
/// task, only the original assignee may claim it for this many seconds; after, anyone may.
/// Eligibility-only — does not change priority. The issue spec pins "Default W = 30m".
pub const STICKY_WINDOW_SECS: i64 = 1800;

/// Shortened sticky-reopen window for high-priority tasks (issue #88). When a reviewer's
/// `changes` verdict reopens a task whose `priority >= CRITICAL_PRIORITY_THRESHOLD`, the
/// sticky window collapses to this many seconds — long enough that an active original
/// author still gets a slight preference, short enough that a disengaged author doesn't
/// lock the task out from other idle eligible agents. Picked at 60s because the work-loop
/// tick is currently ~5 min — well under one tick, so a sticky author who is between ticks
/// does NOT block the queue, but an actively-running author (turn-of-conversation
/// reviewer→author) typically still gets the first claim.
pub const STICKY_WINDOW_SECS_CRITICAL: i64 = 60;

/// Priority threshold at and above which a task is treated as critical for sticky-fallback
/// (issue #88). Tasks created with `priority >= CRITICAL_PRIORITY_THRESHOLD` get the
/// shortened sticky window on `changes`-verdict reopens. Set comfortably above realistic
/// CTO-assigned priorities (typically <200) and below the auto-spawned-review priority
/// (`REVIEW_PRIORITY = 1000`) — but reviews never go through the `changes`-verdict reopen
/// path (verdicts target the original task T, not the review R), so the overlap is moot.
/// The CTO opts a task INTO fast-fallback by creating it with `--priority 900` or higher.
pub const CRITICAL_PRIORITY_THRESHOLD: i64 = 900;

// Compile-time invariant: the critical sticky window must be a small fraction of the
// default — if these ever drift to comparable values, the "fast-fallback" property the
// CASE in apply_verdict is supposed to deliver silently disappears.
const _: () = assert!(STICKY_WINDOW_SECS_CRITICAL < STICKY_WINDOW_SECS / 10);

/// Priority assigned to an auto-spawned review task (issue #10). Reviews precede new work
/// by sorting above any realistic executor-priority — the issue spec says "priority HIGH
/// (so reviews precede new work — automatic)." Hardcoded in v1 because making it tunable
/// invites "let me lower it to clear my queue" anti-patterns; can move to config later
/// (forward-only) if a real need surfaces.
pub const REVIEW_PRIORITY: i64 = 1000;

/// Label that marks a reopened task carrying a reviewer's `changes` action items (#10).
/// Additive to the task's existing labels; deduped so multiple review rounds don't
/// accumulate duplicate `"rework"` entries.
pub const REWORK_LABEL: &str = "rework";

/// True iff a labels JSON value contains `"kind:review"`. Mirrors the SQL `labels LIKE
/// '%"kind:review"%'` pattern used by the claim selector so the Rust-side check and the
/// SQL-side check agree on what counts as a review task.
fn labels_contain_review(labels: &Option<String>) -> bool {
    labels
        .as_deref()
        .is_some_and(|s| s.contains(&format!("\"{REVIEW_LABEL}\"")))
}

/// The lease target string for a task id — the key under which a task's renewable claim-lease
/// lives in the shared `claims` table.
pub fn lease_target(id: i64) -> String {
    format!("task#{id}")
}

/// Deactivate any live lease on a task within an existing transaction. Idempotent.
fn deactivate_lease(tx: &rusqlite::Transaction, id: i64, now: i64) -> Result<()> {
    tx.execute(
        "UPDATE claims SET active=0 WHERE target=?1 AND active=1 AND expires_at > ?2",
        params![lease_target(id), now],
    )?;
    Ok(())
}

/// A task row.
///
/// `depends_on` is a JSON array of task ids this task waits on; `None` = no deps. The value
/// is validated at create-time (must be a JSON array of i64), so reads NEVER fault on bad
/// JSON.
///
/// `ready` is **derived** (true iff every listed dep exists and is `closed`) — populated by
/// [`get`], [`list`], and [`update`]; [`claim`] always sees `true` (a non-ready task can never
/// be claimed). A task with no deps is always ready.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct Task {
    pub id: i64,
    pub title: String,
    pub body: Option<String>,
    pub status: String,
    pub priority: i64,
    pub labels: Option<String>,
    pub assignee: Option<String>,
    pub created_by: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub refs: Option<String>,
    pub depends_on: Option<String>,
    /// Unix-ts sticky-reopen window end. `> now` ⇒ only `assignee` may claim. NULL = no
    /// window. Set by the reviewer's `changes` verdict (issue #10); cleared on a sticky-orig
    /// re-claim or any `release`/`cancel`. Eligibility-only — does not change priority.
    pub sticky_until: Option<i64>,
    /// Review-task only: the original executor whose `done` spawned this review (issue #10).
    /// `claim` filters review tasks whose `orig` equals the caller — no self-review.
    pub orig: Option<String>,
    pub ready: bool,
}

/// A token-efficient summary view of a [`Task`] for queue scans (`task-list --brief`). Drops
/// the full `body` — the one large field an agent doesn't need until it picks a task up — plus
/// the timestamps/refs a scan doesn't read. The full task is always one `task-get <id>` away.
/// Fields match the spec's summary set: id, title, labels, priority, status, assignee, ready,
/// depends_on (#86).
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct TaskBrief {
    pub id: i64,
    pub title: String,
    pub labels: Option<String>,
    pub priority: i64,
    pub status: String,
    pub assignee: Option<String>,
    pub ready: bool,
    pub depends_on: Option<String>,
}

impl From<&Task> for TaskBrief {
    fn from(t: &Task) -> Self {
        TaskBrief {
            id: t.id,
            title: t.title.clone(),
            labels: t.labels.clone(),
            priority: t.priority,
            status: t.status.clone(),
            assignee: t.assignee.clone(),
            ready: t.ready,
            depends_on: t.depends_on.clone(),
        }
    }
}

/// Compact projection of a task — used by write-command success responses
/// (`task-claim`, `task-update`, `task-release`, `task-cancel`) to omit the multi-KB
/// `body` field and notes history that the caller already produced. The full record
/// stays one `task-get <id>` away. **Issue #64** (write-side twin of #57's read-side
/// `TaskBrief`).
///
/// Locked field set: `{id, status, assignee, refs}` + two optional context fields the
/// caller would otherwise need an extra read for:
/// - `lease_expires_at` — included when the response is from a command that maintains a
///   live lease (i.e. `task-claim`). Omitted for release/cancel (lease is dead) and for
///   `task-update` (lease unchanged; `task-get` if you need it).
/// - `note_id` — included on `task-update --note-*` so the caller can identify the
///   breadcrumb they just appended without a re-read.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct TaskCompact {
    pub id: i64,
    pub status: String,
    /// Always serialized (even `null`) — the caller's most-asked question after a write
    /// is "who is this assigned to now?", and an absent field is harder to parse than
    /// an explicit `null`. Mirrors the spec's "{id, status, assignee, refs,
    /// lease_expires_at}" literal field set.
    pub assignee: Option<String>,
    /// Always serialized — same reasoning as `assignee`; structured refs are commonly
    /// checked after a `done` (e.g. `refs.pr`).
    pub refs: Option<String>,
    /// Lease expiry, included only by commands that maintain a live lease
    /// (`task-claim`). Omitted (skipped) otherwise — `release`/`cancel` killed it; for
    /// `task-update` the lease is unchanged and an extra query just to fill this field
    /// would defeat the compact-response point.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_expires_at: Option<i64>,
    /// Id of the breadcrumb appended on `task-update --note-*`. Omitted on other calls.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note_id: Option<i64>,
    /// Recommended branch name for this task in its project (issue #98). Populated by
    /// `task-claim` once the claim transition succeeds; omitted by other commands.
    /// **Recommendation, not mandate** — the agent may override locally, but defaulting
    /// to this name keeps anti-collision centralized (quorum is the only registry of
    /// in-use names).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_branch: Option<String>,
    /// Recommended worktree directory for this task in its project (issue #98). Per
    /// project convention: `.claude/worktrees/<basename>` for ag2trust,
    /// `~/dev/quorum-wt/<basename>` for quorum. Omitted by non-claim commands.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_worktree: Option<String>,
    /// `true` when this task already had a branch allocated (e.g., a reopened/rework
    /// task being re-claimed) → reuse the existing branch and PR. `false` on a fresh
    /// allocation. Omitted entirely when the command did not consult the allocator.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch_exists: Option<bool>,
}

impl From<&Task> for TaskCompact {
    fn from(t: &Task) -> Self {
        TaskCompact {
            id: t.id,
            status: t.status.clone(),
            assignee: t.assignee.clone(),
            refs: t.refs.clone(),
            lease_expires_at: None,
            note_id: None,
            suggested_branch: None,
            suggested_worktree: None,
            branch_exists: None,
        }
    }
}

/// One append-only breadcrumb attached to a task. Ordered by `id` (= insertion order).
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct Note {
    pub id: i64,
    pub ts: i64,
    pub agent: String,
    pub body: String,
}

/// A task plus its notes, returned by [`get_with_notes`] (used by the CLI `task-get`). The
/// `Task` fields are inlined into the JSON output so it stays a single flat object plus a
/// `notes` array — easy for an agent to read in one go.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct TaskDetail {
    #[serde(flatten)]
    pub task: Task,
    pub notes: Vec<Note>,
}

/// Fields a [`update`] may change. `None` leaves the field untouched.
///
/// Note: there is no `assignee` field — reassignment is intentionally not a `task-update`
/// operation under the lease model (it would leave the `task#<id>` lease with the old holder,
/// so the new assignee couldn't renew and the reaper could reclaim under them). Hand-off is
/// `task-release` (→ `open`) followed by a fresh `task-claim`.
///
/// `verdict` (issue #10) is the reviewer's call on a review task being marked `done`. Valid
/// only when (a) the task carries `kind:review` AND (b) `status` is `Some("done")` — the
/// validator rejects every other combination as a usage error. `Some("approve")` chains the
/// original task to `closed`; `Some("changes")` reopens the original with the `rework` label
/// and a sticky window. `None` is rejected on a review-task `done` (verdict is mandatory
/// there) and ignored elsewhere.
#[derive(Default)]
pub struct TaskUpdate<'a> {
    pub status: Option<&'a str>,
    pub body: Option<&'a str>,
    pub refs: Option<&'a str>,
    pub verdict: Option<&'a str>,
}

const COLS: &str =
    "id, title, body, status, priority, labels, assignee, created_by, created_at, updated_at, refs, depends_on, sticky_until, orig";

fn row_to_task(r: &Row) -> rusqlite::Result<Task> {
    // `ready` is set false by default and filled in by the caller (get/list/update/claim).
    // Materializing it requires another query, which the raw row reader can't do.
    Ok(Task {
        id: r.get(0)?,
        title: r.get(1)?,
        body: r.get(2)?,
        status: r.get(3)?,
        priority: r.get(4)?,
        labels: r.get(5)?,
        assignee: r.get(6)?,
        created_by: r.get(7)?,
        created_at: r.get(8)?,
        updated_at: r.get(9)?,
        refs: r.get(10)?,
        depends_on: r.get(11)?,
        sticky_until: r.get(12)?,
        orig: r.get(13)?,
        ready: false,
    })
}

/// Validate `--depends-on` at the boundary: parse as a JSON array of i64 and return a clean
/// usage error if it doesn't. Storing unvalidated JSON would let one bad row poison every
/// `task-list`/`task-get`/`task-cancel` call (json_each errors propagate from `compute_ready`)
/// — including the cancel that would otherwise let an operator recover. Reject early; never
/// store bad JSON.
fn validate_depends_on(s: &str) -> Result<()> {
    serde_json::from_str::<Vec<i64>>(s).map_err(|e| {
        QuorumError::Usage(format!(
            "--depends-on must be a JSON array of task ids (e.g. '[1,3]'): {e}"
        ))
    })?;
    Ok(())
}

/// Compute whether a task's deps are all `closed`. `None`/empty deps = ready.
///
/// Issue #2 alignment: a dependent unblocks only when every dep is **`closed`** (reviewed +
/// finalized per #9/#10), not `done` (submitted/unreviewed). A missing or `cancelled` dep is
/// treated as unmet — never satisfied — so the dependent stays unclaimable.
///
/// Safe-by-construction: bad JSON can never reach this function because [`validate_depends_on`]
/// rejects malformed input in [`create`].
pub fn compute_ready(conn: &Connection, depends_on: &Option<String>) -> Result<bool> {
    let Some(json) = depends_on.as_deref() else {
        return Ok(true);
    };
    let unmet: i64 = conn.query_row(
        "SELECT count(*) FROM json_each(?1)
         WHERE NOT EXISTS (
             SELECT 1 FROM tasks d WHERE d.id = json_each.value AND d.status = 'closed'
         )",
        params![json],
        |r| r.get(0),
    )?;
    Ok(unmet == 0)
}

/// Create a new `open` task. Returns its id.
///
/// `depends_on` is a JSON array of task ids this task waits on (e.g. `"[1,3]"`); `None` =
/// no deps. Validated at the boundary as a JSON array of i64 — malformed input is rejected
/// with a usage error (exit 2) BEFORE the row lands, so no read path can be poisoned.
#[allow(clippy::too_many_arguments)]
pub fn create(
    conn: &mut Connection,
    created_by: &str,
    title: &str,
    body: Option<&str>,
    priority: i64,
    labels: Option<&str>,
    refs: Option<&str>,
    depends_on: Option<&str>,
    now: i64,
) -> Result<i64> {
    // Reject malformed depends_on at the boundary. See validate_depends_on for why.
    if let Some(s) = depends_on {
        validate_depends_on(s)?;
    }
    let tx = begin_immediate(conn)?;
    crate::agents::touch(&tx, created_by, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    tx.execute(
        "INSERT INTO tasks(title, body, status, priority, labels, assignee, created_by, created_at, updated_at, refs, depends_on)
         VALUES (?1, ?2, 'open', ?3, ?4, NULL, ?5, ?6, ?6, ?7, ?8)",
        params![title, body, priority, labels, created_by, now, refs, depends_on],
    )?;
    let id = tx.last_insert_rowid();
    let body_str = match labels {
        Some(l) => format!("created (prio {priority}, labels {l})"),
        None => format!("created (prio {priority})"),
    };
    crate::events::emit(&tx, "task_created", &lease_target(id), &body_str, now)?;
    tx.commit()?;
    Ok(id)
}

/// Atomically claim a task and take a renewable `ttl`-second lease on it. With `task_id`,
/// claims that specific open task; without, claims the highest-priority open task whose
/// `labels` contain every label in `match_labels` (empty slice = no label filter, current
/// behaviour). Returns `None` if nothing claimable (already taken / none open / no match).
///
/// The guarded `UPDATE ... WHERE status='open'` is the single-winner gate; the lease row
/// (`target='task#<id>'`) is written in the same transaction so claim + lease are atomic. The
/// lease lets a lost agent's task be reaped back to `open` (see `sweep::reap_lapsed_tasks`).
///
/// `match_labels` is intentionally AND-only and exact-match (no expression grammar) — quorum
/// stays agnostic to what a label means; pattern is `%"<label>"%` against the JSON-array
/// `labels` column, identical to [`list`].
pub fn claim(
    conn: &mut Connection,
    agent: &str,
    task_id: Option<i64>,
    match_labels: &[&str],
    ttl: i64,
    now: i64,
) -> Result<Option<Task>> {
    let tx = begin_immediate(conn)?;
    crate::agents::touch(&tx, agent, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    // #2 dep-gate clause: claimable only when every dep id is `closed` (or no deps).
    // Gate on `closed` (reviewed + finalized per #9/#10), NOT `done` (submitted/unreviewed) —
    // don't let dependents build on unreviewed work. Composed with the match-label filter as
    // additional ANDs; both are pure-narrowing.
    const DEP_READY_CLAUSE: &str = "(depends_on IS NULL OR NOT EXISTS (
        SELECT 1 FROM json_each(depends_on) je
        WHERE NOT EXISTS (
            SELECT 1 FROM tasks d WHERE d.id = je.value AND d.status = 'closed'
        )
    ))";
    // #10 self-review block: a review task (labels contain "kind:review") whose `orig` equals
    // the caller is invisible to that caller. Reject-at-claim, not reject-at-update — else an
    // orig could claim and hold the lease (denying everyone else) until they noticed. Composes
    // as another AND clause; ?1 is the agent param shared with the UPDATE/SELECT.
    const SELF_REVIEW_BLOCK_CLAUSE: &str =
        "(labels IS NULL OR labels NOT LIKE '%\"kind:review\"%' OR orig IS NULL OR orig != ?1)";
    // #10 sticky-reopen gate: a task in its sticky window is claimable only by its assignee
    // (the original executor whose `changes`-verdict reopen set the window). After expiry,
    // anyone — eligibility narrows for `now < sticky_until` only. ?2 is the `now` param.
    const STICKY_CLAUSE: &str = "(sticky_until IS NULL OR sticky_until <= ?2 OR assignee = ?1)";
    let mut task = match task_id {
        Some(id) => tx
            .query_row(
                // Even with an explicit --task-id, all three gates apply: dep-ready, no
                // self-review, sticky eligibility. An agent that truly needs to bypass the
                // dep gate can wait or cancel the dep; the other two are mechanism for the
                // #10 contract and have no bypass.
                &format!(
                    "UPDATE tasks SET status='claimed', assignee=?1, updated_at=?2
                     WHERE id=?3 AND status='open' AND {DEP_READY_CLAUSE}
                       AND {SELF_REVIEW_BLOCK_CLAUSE} AND {STICKY_CLAUSE}
                     RETURNING {COLS}"
                ),
                params![agent, now, id],
                row_to_task,
            )
            .optional()?,
        None => {
            // Build the open-task selector with the dep-ready / self-review / sticky
            // clauses, plus a label match that handles both tiered and untiered reviews:
            // - Untiered review tasks (legacy `["kind:review"]` only) are tier-exempt
            //   so they remain visible to every tier-filtered claim (#73).
            // - Tiered review tasks (e.g. `["kind:review","tier:opus-47"]`) go through
            //   normal tier matching so a weaker-tier agent can't claim them (#105).
            // Patterns are bound as parameters; only the placeholder count is
            // interpolated (no value reaches SQL as a string).
            const REVIEW_UNTIERED_EXEMPT: &str =
                "(labels LIKE '%\"kind:review\"%' AND labels NOT LIKE '%\"tier:%')";
            let mut selector = format!(
                "SELECT id FROM tasks WHERE status='open' AND {DEP_READY_CLAUSE}
                 AND {SELF_REVIEW_BLOCK_CLAUSE} AND {STICKY_CLAUSE}"
            );
            if !match_labels.is_empty() {
                use std::fmt::Write as _;
                // (untiered-review OR (label1 AND label2 AND ...))
                let _ = write!(selector, " AND ({REVIEW_UNTIERED_EXEMPT} OR (");
                for i in 0..match_labels.len() {
                    if i > 0 {
                        selector.push_str(" AND ");
                    }
                    // Params are 1-indexed; ?1 = agent, ?2 = now, so label params start at ?3.
                    let _ = write!(selector, "labels LIKE ?{}", i + 3);
                }
                selector.push_str("))");
            }
            selector.push_str(" ORDER BY priority DESC, id ASC LIMIT 1");
            let sql = format!(
                "UPDATE tasks SET status='claimed', assignee=?1, updated_at=?2
                 WHERE id=({selector}) RETURNING {COLS}"
            );
            let label_pats: Vec<String> =
                match_labels.iter().map(|l| format!("%\"{l}\"%")).collect();
            let mut bind: Vec<&dyn rusqlite::ToSql> = vec![&agent, &now];
            for p in &label_pats {
                bind.push(p);
            }
            tx.query_row(&sql, &bind[..], row_to_task).optional()?
        }
    };
    if let Some(t) = &mut task {
        // A claim only fires when the row was claimable, so deps are by construction met.
        t.ready = true;
        // Take the lease in the same txn. Reap a dead lease first (a reopened task may carry a
        // stale row), then insert active. Only the single status-winner reaches here, so the
        // partial unique index won't collide.
        let target = lease_target(t.id);
        tx.execute(
            "UPDATE claims SET active=0 WHERE target=?1 AND active=1 AND expires_at <= ?2",
            params![target, now],
        )?;
        tx.execute(
            "INSERT INTO claims(target, holder, ts, expires_at, active) VALUES (?1,?2,?3,?4,1)",
            params![target, agent, now, now + ttl],
        )?;
        crate::events::emit(
            &tx,
            "task_claimed",
            &target,
            &format!("taken by {agent}"),
            now,
        )?;
    }
    tx.commit()?;
    Ok(task)
}

/// Update a task. Fails loud ([`QuorumError::NotHolder`]) if `agent` is not the assignee of a
/// **`claimed`** task — a terminal (`done`/`cancelled`/`closed`) or `open` task cannot be
/// updated, so a settled task can never be resurrected (issue #21).
///
/// **Non-review task (executor's path):** the only status an agent may set is `done`
/// (`claimed → done`); no agent-set intermediate state. Setting `done` deactivates the
/// task's lease AND (issue #10) **atomically auto-spawns a review task** in the same
/// transaction — submitting work guarantees a review is queued, with no race window between
/// the two. The spawn skips when the source task is itself a `kind:review` task (no recursion,
/// per the issue's "reviews don't spawn reviews" rule).
///
/// **Review task (reviewer's path, issue #10):** marking `done` REQUIRES `--verdict
/// approve|changes`. Both verdicts deactivate the review lease and chain a state change to
/// the original task **in the same transaction**:
/// - `approve` → original task → `closed` (terminal).
/// - `changes` → original task → `open` + `rework` label + assignee=`orig` + sticky window.
///
/// Other transitions: `claimed` is system-driven (claim/reaper). `open` releases a claimed
/// task (assignee-only). `cancelled` is a terminal won't-do (creator OR assignee).
pub fn update(
    conn: &mut Connection,
    agent: &str,
    id: i64,
    fields: &TaskUpdate,
    now: i64,
) -> Result<Task> {
    // -- Up-front argument validation (cheap, before we take the write lock). --------------
    if let Some(s) = fields.status {
        if !STATUSES.contains(&s) {
            return Err(QuorumError::Usage(format!("invalid status: {s}")));
        }
        if s == "claimed" || s == "closed" {
            return Err(QuorumError::Usage(format!(
                "task-update cannot set status '{s}'; 'claimed' is set by task-claim, \
                 'closed' is set by review automation"
            )));
        }
    }
    if let Some(v) = fields.verdict {
        if v != "approve" && v != "changes" {
            return Err(QuorumError::Usage(format!(
                "--verdict must be 'approve' or 'changes' (got '{v}')"
            )));
        }
        if fields.status != Some("done") {
            return Err(QuorumError::Usage(
                "--verdict is only valid with --status done".into(),
            ));
        }
    }

    let tx = begin_immediate(conn)?;
    crate::agents::touch(&tx, agent, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;

    // -- Status-specific UPDATE with appropriate guards. -----------------------------------
    // Each status transition has its own SQL because the permission guard differs:
    //   open       → assignee-only, from claimed (release semantics)
    //   cancelled  → creator OR assignee, from non-terminal (cancel semantics)
    //   done       → assignee-only, from claimed
    //   no status  → assignee-only, from claimed (metadata-only update)
    let n = match fields.status {
        Some("open") => {
            // Release: assignee guard, status=claimed. Nulls assignee + sticky.
            tx.execute(
                "UPDATE tasks SET
                    status='open', assignee=NULL, sticky_until=NULL,
                    body  = COALESCE(?3, body),
                    refs  = COALESCE(?4, refs),
                    updated_at = ?5
                 WHERE id=?1 AND assignee=?6 AND status='claimed'",
                params![id, "open", fields.body, fields.refs, now, agent],
            )?
        }
        Some("cancelled") => {
            // Cancel: creator OR assignee, non-terminal status.
            tx.execute(
                "UPDATE tasks SET
                    status='cancelled', sticky_until=NULL,
                    body  = COALESCE(?3, body),
                    refs  = COALESCE(?4, refs),
                    updated_at = ?5
                 WHERE id=?1 AND (created_by=?6 OR assignee=?6)
                       AND status NOT IN ('cancelled', 'closed')",
                params![id, "cancelled", fields.body, fields.refs, now, agent],
            )?
        }
        _ => {
            // done or metadata-only: assignee guard, status=claimed.
            tx.execute(
                "UPDATE tasks SET
                    status   = COALESCE(?2, status),
                    body     = COALESCE(?3, body),
                    refs     = COALESCE(?4, refs),
                    updated_at = ?5
                 WHERE id=?1 AND assignee=?6 AND status='claimed'",
                params![id, fields.status, fields.body, fields.refs, now, agent],
            )?
        }
    };
    if n == 0 {
        tx.commit()?;
        return Err(QuorumError::NotHolder);
    }

    // -- Post-update side-effects per status. ----------------------------------------------
    if fields.status == Some("open") {
        deactivate_lease(&tx, id, now)?;
        crate::events::emit(
            &tx,
            "task_released",
            &lease_target(id),
            &format!("released by {agent}"),
            now,
        )?;
    } else if fields.status == Some("cancelled") {
        deactivate_lease(&tx, id, now)?;
        crate::events::emit(
            &tx,
            "task_cancelled",
            &lease_target(id),
            &format!("cancelled by {agent}"),
            now,
        )?;
    } else if fields.status == Some("done") {
        deactivate_lease(&tx, id, now)?;

        let (title, labels, refs_str, orig_opt): (
            String,
            Option<String>,
            Option<String>,
            Option<String>,
        ) = tx.query_row(
            "SELECT title, labels, refs, orig FROM tasks WHERE id=?1",
            params![id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )?;
        let is_review = labels_contain_review(&labels);

        if is_review {
            let Some(verdict) = fields.verdict else {
                let _ = tx.rollback();
                return Err(QuorumError::Usage(
                    "review task requires --verdict (approve|changes) on --status done".into(),
                ));
            };
            let Some(orig) = orig_opt else {
                let _ = tx.rollback();
                return Err(QuorumError::Usage(
                    "review task is missing `orig` — cannot resolve verdict target".into(),
                ));
            };
            let target_t = extract_review_of(&refs_str).ok_or_else(|| {
                let _ = ();
                QuorumError::Usage(
                    "review task is missing refs.review_of — cannot resolve verdict target".into(),
                )
            })?;
            apply_verdict(&tx, verdict, target_t, agent, &orig, now)?;
        } else {
            if fields.verdict.is_some() {
                let _ = tx.rollback();
                return Err(QuorumError::Usage(
                    "--verdict is only valid on a review task (labels contain kind:review)".into(),
                ));
            }
            crate::events::emit(
                &tx,
                "task_done",
                &lease_target(id),
                &format!("done by {agent}"),
                now,
            )?;
            spawn_review(&tx, id, &title, &refs_str, &labels, agent, now)?;
        }
    }

    // Final task fetch + ready re-compute (deps may have shifted if a chained `closed` landed
    // above, e.g. on the approve path the target task is now `closed`; downstream tasks
    // depending on it become claimable — surface that via `ready` on the returned row).
    let mut task = tx.query_row(
        &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
        params![id],
        row_to_task,
    )?;
    task.ready = compute_ready(&tx, &task.depends_on)?;
    tx.commit()?;
    Ok(task)
}

/// Pull `refs.review_of` (a task id) out of a review task's refs JSON. Returns `None` if the
/// refs are missing, malformed, or lack `review_of`. Used only by the verdict path.
fn extract_review_of(refs_str: &Option<String>) -> Option<i64> {
    let s = refs_str.as_deref()?;
    let v: serde_json::Value = serde_json::from_str(s).ok()?;
    v.get("review_of").and_then(|x| x.as_i64())
}

/// Auto-spawn the review task that pairs with a non-review task's `done`. Same transaction
/// as the source `done` so a crash between them is impossible — the contract is "submit ⇒
/// review queued, atomically." Skip when the source is itself a review task (no recursion,
/// enforced one level up by the `is_review` branch).
fn spawn_review(
    tx: &rusqlite::Transaction,
    source_id: i64,
    source_title: &str,
    source_refs: &Option<String>,
    source_labels: &Option<String>,
    orig_agent: &str,
    now: i64,
) -> Result<()> {
    // Compose the review task's refs. `review_of` points back at the source so the verdict
    // path can resolve T from R alone. Inherit `pr` if the source carried one (a human
    // breadcrumb; nothing in quorum branches on it).
    let pr_value: Option<serde_json::Value> = source_refs
        .as_deref()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
        .and_then(|v| v.get("pr").cloned());
    let review_refs = {
        let mut obj = serde_json::Map::new();
        obj.insert(
            "review_of".to_string(),
            serde_json::Value::Number(source_id.into()),
        );
        if let Some(pr) = pr_value {
            obj.insert("pr".to_string(), pr);
        }
        serde_json::Value::Object(obj).to_string()
    };
    // #105: inherit the source task's tier label so the review routes to a
    // same-or-higher-tier agent (a weaker-tier reviewer shouldn't review harder work).
    let review_labels = {
        let mut lbls = vec![REVIEW_LABEL.to_string()];
        if let Some(s) = source_labels.as_deref() {
            if let Ok(arr) = serde_json::from_str::<Vec<String>>(s) {
                for l in &arr {
                    if l.starts_with("tier:") {
                        lbls.push(l.clone());
                    }
                }
            }
        }
        serde_json::to_string(&lbls).unwrap()
    };
    let review_title = format!("review: {source_title}");
    // `created_by` records the executor whose `done` triggered the spawn — useful in a
    // roster trace for "who is this review chained from."
    tx.execute(
        "INSERT INTO tasks(title, body, status, priority, labels, assignee, created_by,
                           created_at, updated_at, refs, depends_on, sticky_until, orig)
         VALUES (?1, NULL, 'open', ?2, ?3, NULL, ?4, ?5, ?5, ?6, NULL, NULL, ?7)",
        params![
            review_title,
            REVIEW_PRIORITY,
            review_labels,
            orig_agent,
            now,
            review_refs,
            orig_agent,
        ],
    )?;
    let review_id = tx.last_insert_rowid();
    crate::events::emit(
        tx,
        "review_spawned",
        &lease_target(review_id),
        &format!("for task#{source_id} by {orig_agent}"),
        now,
    )?;
    Ok(())
}

/// Apply the reviewer's verdict to the original task. Inside the same transaction as the
/// review task's `done`, so the chained state change either commits as one with the verdict
/// or rolls back as one — there is never a `done` review without its consequence.
fn apply_verdict(
    tx: &rusqlite::Transaction,
    verdict: &str,
    target_t: i64,
    reviewer: &str,
    orig: &str,
    now: i64,
) -> Result<()> {
    match verdict {
        "approve" => {
            // T → closed (terminal). Any sticky carried over from a prior `changes` round is
            // wiped — terminal state has no eligibility window.
            let n = tx.execute(
                "UPDATE tasks SET status='closed', sticky_until=NULL, updated_at=?1
                 WHERE id=?2 AND status='done'",
                params![now, target_t],
            )?;
            if n == 0 {
                let _ = ();
                return Err(QuorumError::Usage(format!(
                    "verdict 'approve' requires target task#{target_t} to be in 'done' \
                     (someone changed it under us)"
                )));
            }
            crate::events::emit(
                tx,
                "task_closed",
                &lease_target(target_t),
                &format!("approved by {reviewer}"),
                now,
            )?;
        }
        "changes" => {
            // T → open + dedup-append "rework" label. Assignee + sticky window depend on
            // whether T is a review task (#115): implementation rework is sticky-to-orig
            // (bounce back to the code author); a re-review reopens to the pool (any
            // eligible non-author reviewer can claim).
            //
            // Issue #88: the sticky window length is row-conditional on `priority`. For
            // `priority >= CRITICAL_PRIORITY_THRESHOLD` we use `STICKY_WINDOW_SECS_CRITICAL`
            // (≈60s — one fleet beat) so an idle eligible non-orig agent can claim a
            // master-blocking reopen without waiting out the default 30-minute window. The
            // CASE is in-SQL so we avoid an extra SELECT round-trip just to read the
            // priority of the row we're already UPDATEing.
            //
            // #115: kind:review targets get assignee=NULL, sticky_until=NULL — handled
            // inline via SQL CASE on the labels column.
            let target_labels: Option<String> = tx.query_row(
                "SELECT labels FROM tasks WHERE id=?1",
                params![target_t],
                |r| r.get(0),
            )?;
            let target_is_review = labels_contain_review(&target_labels);
            let n = tx.execute(
                "UPDATE tasks SET
                    status = 'open',
                    assignee = CASE
                        WHEN labels LIKE '%\"kind:review\"%' THEN NULL
                        ELSE ?1
                    END,
                    sticky_until = CASE
                        WHEN labels LIKE '%\"kind:review\"%' THEN NULL
                        ELSE ?2 + CASE
                            WHEN priority >= ?6 THEN ?7
                            ELSE ?8
                        END
                    END,
                    labels = CASE
                        WHEN labels IS NULL OR labels NOT LIKE '%\"rework\"%'
                        THEN json_insert(COALESCE(labels, '[]'), '$[#]', ?3)
                        ELSE labels
                    END,
                    updated_at = ?4
                 WHERE id = ?5 AND status = 'done'",
                params![
                    orig,
                    now,
                    REWORK_LABEL,
                    now,
                    target_t,
                    CRITICAL_PRIORITY_THRESHOLD,
                    STICKY_WINDOW_SECS_CRITICAL,
                    STICKY_WINDOW_SECS
                ],
            )?;
            if n == 0 {
                let _ = ();
                return Err(QuorumError::Usage(format!(
                    "verdict 'changes' requires target task#{target_t} to be in 'done' \
                     (someone changed it under us)"
                )));
            }
            let event_detail = if target_is_review {
                format!("changes requested by {reviewer} (reopened to pool)")
            } else {
                format!("changes requested by {reviewer} (sticky to {orig})")
            };
            crate::events::emit(
                tx,
                "task_reopened",
                &lease_target(target_t),
                &event_detail,
                now,
            )?;
        }
        _ => unreachable!("verdict pre-validated upstream"),
    }
    Ok(())
}

/// List tasks, optionally filtered by status, label, and/or assignee. Read-only.
pub fn list(
    conn: &Connection,
    status: Option<&str>,
    label: Option<&str>,
    assignee: Option<&str>,
) -> Result<Vec<Task>> {
    // Dynamic but fully parameterized — no value is interpolated into SQL.
    let label_pat = label.map(|l| format!("%\"{l}\"%"));
    let mut sql = format!("SELECT {COLS} FROM tasks WHERE 1=1");
    if status.is_some() {
        sql.push_str(" AND status=:status");
    }
    if label_pat.is_some() {
        sql.push_str(" AND labels LIKE :label");
    }
    if assignee.is_some() {
        sql.push_str(" AND assignee=:assignee");
    }
    sql.push_str(" ORDER BY priority DESC, id ASC");

    let mut stmt = conn.prepare(&sql)?;
    let params: Vec<(&str, &dyn rusqlite::ToSql)> = {
        let mut v: Vec<(&str, &dyn rusqlite::ToSql)> = Vec::new();
        if let Some(s) = &status {
            v.push((":status", s));
        }
        if let Some(p) = &label_pat {
            v.push((":label", p));
        }
        if let Some(a) = &assignee {
            v.push((":assignee", a));
        }
        v
    };
    let mut tasks: Vec<Task> = stmt
        .query_map(&params[..], row_to_task)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for t in &mut tasks {
        t.ready = compute_ready(conn, &t.depends_on)?;
    }
    Ok(tasks)
}

/// Fetch a single task by id. `ready` is filled per the dependency rule (#2).
pub fn get(conn: &Connection, id: i64) -> Result<Option<Task>> {
    let mut task = conn
        .query_row(
            &format!("SELECT {COLS} FROM tasks WHERE id=?1"),
            params![id],
            row_to_task,
        )
        .optional()?;
    if let Some(t) = &mut task {
        t.ready = compute_ready(conn, &t.depends_on)?;
    }
    Ok(task)
}

/// Release sticky tasks held by a retired agent (#115). Sets assignee=NULL +
/// sticky_until=NULL on any open task assigned to `agent` with an active sticky window,
/// so the STICKY_CLAUSE frees them for the next eligible claimer.
pub fn release_sticky_for_agent(conn: &Connection, agent: &str, now: i64) -> Result<()> {
    conn.execute(
        "UPDATE tasks SET assignee = NULL, sticky_until = NULL, updated_at = ?3
         WHERE assignee = ?1 AND sticky_until IS NOT NULL AND sticky_until > ?2
           AND status = 'open'",
        params![agent, now, now],
    )?;
    Ok(())
}

/// Fetch a single task and its append-only note history (oldest first).
pub fn get_with_notes(conn: &Connection, id: i64) -> Result<Option<TaskDetail>> {
    let Some(task) = get(conn, id)? else {
        return Ok(None);
    };
    let notes = notes_for(conn, id)?;
    Ok(Some(TaskDetail { task, notes }))
}

/// Append a breadcrumb to `task_id`. Returns the new note's id, or `None` if the task does
/// not exist (mapped to a clean exit 1 by the CLI, matching `task-get` semantics). Notes are
/// append-only — there is no edit/delete. Any agent may add a note: notes are public
/// context for whoever picks the task up next, so the assignee guard from [`update`] does
/// not apply.
pub fn add_note(
    conn: &mut Connection,
    agent: &str,
    task_id: i64,
    body: &str,
    now: i64,
) -> Result<Option<i64>> {
    let tx = begin_immediate(conn)?;
    crate::agents::touch(&tx, agent, now)?;
    crate::sweep::sweep_on_write(&tx, now, SWEEP_LIMIT)?;
    // Verify the task exists *inside the transaction* so it can't disappear between the
    // check and the INSERT. SQLite has no FK enforced here (v1 invariant: no FKs), so this
    // is the load-bearing existence check.
    let exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM tasks WHERE id=?1)",
        params![task_id],
        |r| r.get(0),
    )?;
    if !exists {
        tx.commit()?;
        return Ok(None);
    }
    tx.execute(
        "INSERT INTO task_notes(task_id, ts, agent, body) VALUES (?1, ?2, ?3, ?4)",
        params![task_id, now, agent, body],
    )?;
    let id = tx.last_insert_rowid();
    tx.commit()?;
    Ok(Some(id))
}

fn notes_for(conn: &Connection, task_id: i64) -> Result<Vec<Note>> {
    let mut stmt = conn
        .prepare("SELECT id, ts, agent, body FROM task_notes WHERE task_id=?1 ORDER BY id ASC")?;
    let notes = stmt
        .query_map(params![task_id], |r| {
            Ok(Note {
                id: r.get(0)?,
                ts: r.get(1)?,
                agent: r.get(2)?,
                body: r.get(3)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(notes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_tmp() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let c = crate::db::open(&dir.path().join("q.db")).unwrap();
        (dir, c)
    }

    const TTL: i64 = 3600;

    fn release(conn: &mut Connection, agent: &str, id: i64, now: i64) -> Result<Task> {
        update(
            conn,
            agent,
            id,
            &TaskUpdate {
                status: Some("open"),
                body: None,
                refs: None,
                verdict: None,
            },
            now,
        )
    }

    fn cancel(conn: &mut Connection, agent: &str, id: i64, now: i64) -> Result<Task> {
        update(
            conn,
            agent,
            id,
            &TaskUpdate {
                status: Some("cancelled"),
                body: None,
                refs: None,
                verdict: None,
            },
            now,
        )
    }

    /// Is there a live (active, unexpired) lease on this task?
    fn has_live_lease(c: &Connection, id: i64, now: i64) -> bool {
        c.query_row(
            "SELECT count(*) FROM claims WHERE target=?1 AND active=1 AND expires_at > ?2",
            params![lease_target(id), now],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
            > 0
    }

    #[test]
    fn create_then_claim_sets_assignee_and_lease() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "fix bug", None, 0, None, None, None, 1000).unwrap();
        let t = claim(&mut c, "A", Some(id), &[], TTL, 1000)
            .unwrap()
            .unwrap();
        assert_eq!(t.status, "claimed");
        assert_eq!(t.assignee.as_deref(), Some("A"));
        // The claim took a renewable lease on task#<id>.
        assert!(has_live_lease(&c, id, 1000));
    }

    #[test]
    fn second_claim_of_same_task_is_none() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        assert!(claim(&mut c, "A", Some(id), &[], TTL, 1000)
            .unwrap()
            .is_some());
        assert!(claim(&mut c, "B", Some(id), &[], TTL, 1000)
            .unwrap()
            .is_none());
    }

    #[test]
    fn claim_without_id_picks_highest_priority() {
        let (_d, mut c) = open_tmp();
        create(&mut c, "boss", "low", None, 1, None, None, None, 1000).unwrap();
        create(&mut c, "boss", "high", None, 9, None, None, None, 1000).unwrap();
        let t = claim(&mut c, "A", None, &[], TTL, 1000).unwrap().unwrap();
        assert_eq!(t.title, "high");
    }

    #[test]
    fn claim_nothing_open_is_none() {
        let (_d, mut c) = open_tmp();
        assert!(claim(&mut c, "A", None, &[], TTL, 1000).unwrap().is_none());
    }

    // -- --match-label (issue #1) -----------------------------------------------------------

    #[test]
    fn match_label_filters_to_matching_task() {
        let (_d, mut c) = open_tmp();
        // Two open tasks; the higher-priority one lacks the requested label.
        create(
            &mut c,
            "boss",
            "high-no-label",
            None,
            9,
            Some(r#"["tier:opus-46"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        let want = create(
            &mut c,
            "boss",
            "low-with-label",
            None,
            1,
            Some(r#"["tier:opus-47","lang:rust"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        let t = claim(&mut c, "A", None, &["tier:opus-47"], TTL, 1000)
            .unwrap()
            .unwrap();
        assert_eq!(t.id, want);
        assert_eq!(t.title, "low-with-label");
    }

    #[test]
    fn match_label_no_match_is_none_not_error() {
        let (_d, mut c) = open_tmp();
        create(
            &mut c,
            "boss",
            "rust",
            None,
            5,
            Some(r#"["lang:rust"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        // Requesting a label nothing carries → clean None (caller exits 1), not an error.
        assert!(claim(&mut c, "A", None, &["lang:python"], TTL, 1000)
            .unwrap()
            .is_none());
    }

    #[test]
    fn match_label_is_and_across_repeats() {
        let (_d, mut c) = open_tmp();
        // Only one task carries BOTH labels; AND must skip the singletons.
        create(
            &mut c,
            "boss",
            "rust-only",
            None,
            9,
            Some(r#"["lang:rust"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        create(
            &mut c,
            "boss",
            "tier-only",
            None,
            9,
            Some(r#"["tier:opus-47"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        let want = create(
            &mut c,
            "boss",
            "rust-and-tier",
            None,
            5,
            Some(r#"["lang:rust","tier:opus-47"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        let t = claim(&mut c, "A", None, &["lang:rust", "tier:opus-47"], TTL, 1000)
            .unwrap()
            .unwrap();
        assert_eq!(t.id, want);
    }

    #[test]
    fn match_label_picks_highest_priority_among_matches() {
        let (_d, mut c) = open_tmp();
        // Three matching tasks at different priorities; claim must pick the top.
        create(
            &mut c,
            "boss",
            "low",
            None,
            1,
            Some(r#"["k"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        let want = create(
            &mut c,
            "boss",
            "high",
            None,
            9,
            Some(r#"["k"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        create(
            &mut c,
            "boss",
            "mid",
            None,
            5,
            Some(r#"["k"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        let t = claim(&mut c, "A", None, &["k"], TTL, 1000)
            .unwrap()
            .unwrap();
        assert_eq!(t.id, want);
    }

    #[test]
    fn claim_treats_untiered_review_as_tier_exempt() {
        // #73: an UNTIERED `kind:review` task (only `["kind:review"]`, no tier
        // label) remains tier-exempt so legacy reviews are still claimable.
        // #105 narrows: tiered reviews go through normal matching (see
        // `claim_tiered_review_obeys_tier_matching`).
        let (_d, mut c) = open_tmp();
        let _user = create(
            &mut c,
            "boss",
            "user-work",
            None,
            50,
            Some(r#"["tier:opus-47"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        let review = create(
            &mut c,
            "boss",
            "review-pending",
            None,
            1000,
            Some(r#"["kind:review"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        let t = claim(&mut c, "agent-X", None, &["tier:opus-47"], TTL, 1000)
            .unwrap()
            .expect("tier-filtered claim should surface the review task");
        assert_eq!(
            t.id, review,
            "priority-1000 review task must win the tier-filtered claim over the priority-50 user-work task",
        );
        assert!(has_live_lease(&c, review, 1000));
    }

    #[test]
    fn claim_tier_exempt_does_not_widen_non_review_matching() {
        // The tier-exempt OR must NOT let a non-review task in a different tier
        // through the filter — only `kind:review` gets the bypass.
        let (_d, mut c) = open_tmp();
        let _foreign = create(
            &mut c,
            "boss",
            "foreign",
            None,
            100,
            Some(r#"["tier:opus-46"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        let got = claim(&mut c, "agent-X", None, &["tier:opus-47"], TTL, 1000).unwrap();
        assert!(
            got.is_none(),
            "non-review task in a different tier must NOT be claimable through the tier-exempt branch",
        );
    }

    #[test]
    fn claim_kind_review_still_blocks_self_review() {
        // Defense-in-depth: the tier-exempt branch must compose with the
        // self-review block (#10) — an `orig` agent must NOT be able to claim
        // their own review task via the tier-filtered path.
        let (_d, mut c) = open_tmp();
        // Manually insert a `kind:review` task whose `orig` is "author-A".
        let id: i64 = c.query_row(
            "INSERT INTO tasks (title, created_by, priority, labels, orig, status, created_at, updated_at)
             VALUES ('review-of-A', 'system', 1000, '[\"kind:review\"]', 'author-A', 'open', 1000, 1000)
             RETURNING id",
            [],
            |r| r.get(0),
        ).unwrap();
        // author-A passes a tier filter — must still be blocked by self-review.
        let got = claim(&mut c, "author-A", None, &["tier:opus-47"], TTL, 1000).unwrap();
        assert!(
            got.is_none(),
            "author of the orig task must NOT be able to claim their own review, even via the tier-exempt branch",
        );
        // A different agent can claim it.
        let got_b = claim(&mut c, "agent-B", None, &["tier:opus-47"], TTL, 1000)
            .unwrap()
            .expect("a non-author with a tier filter should still claim the review");
        assert_eq!(got_b.id, id);
    }

    #[test]
    fn match_label_takes_lease_just_like_unfiltered_claim() {
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c,
            "boss",
            "x",
            None,
            0,
            Some(r#"["k"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        let t = claim(&mut c, "A", None, &["k"], TTL, 1000)
            .unwrap()
            .unwrap();
        assert_eq!(t.id, id);
        assert!(has_live_lease(&c, id, 1000));
    }

    #[test]
    fn update_by_assignee_sets_done_and_drops_lease() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        let t = update(
            &mut c,
            "A",
            id,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            1100,
        )
        .unwrap();
        assert_eq!(t.status, "done");
        assert_eq!(t.updated_at, 1100);
        // Submitting `done` drops the lease.
        assert!(!has_live_lease(&c, id, 1100));
    }

    #[test]
    fn update_by_nonassignee_fails_loud() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        let err = update(
            &mut c,
            "B",
            id,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            1100,
        )
        .unwrap_err();
        assert!(matches!(err, QuorumError::NotHolder));
    }

    #[test]
    fn update_rejects_invalid_status() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        let err = update(
            &mut c,
            "A",
            id,
            &TaskUpdate {
                status: Some("frobnicate"),
                ..Default::default()
            },
            1100,
        )
        .unwrap_err();
        assert!(matches!(err, QuorumError::Usage(_)));
    }

    #[test]
    fn update_rejects_system_only_statuses() {
        // `claimed` and `closed` are system-driven (claim/review automation), never agent-set.
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        for bad in ["claimed", "closed"] {
            let err = update(
                &mut c,
                "A",
                id,
                &TaskUpdate {
                    status: Some(bad),
                    ..Default::default()
                },
                1100,
            )
            .unwrap_err();
            assert!(
                matches!(err, QuorumError::Usage(_)),
                "status {bad} must be rejected"
            );
        }
    }

    #[test]
    fn update_cannot_resurrect_terminal_task() {
        // Issue #21: task-update guarded only the assignee, never the status. A terminal task
        // retains its assignee (cancel keeps it; done keeps it), so `WHERE assignee=A` matched
        // and the task could be moved back to `done` — a "resurrection". The fix adds
        // `AND status='claimed'`: the only legal agent transition is `claimed → done`, so a
        // settled row matches zero rows → loud NotHolder (exit 1) and is left untouched.
        let (_d, mut c) = open_tmp();

        // (a) cancelled → done must be rejected (the exact repro in the issue).
        let cancelled = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(cancelled), &[], TTL, 1000).unwrap();
        cancel(&mut c, "A", cancelled, 1100).unwrap(); // status=cancelled, assignee still A
        let err = update(
            &mut c,
            "A",
            cancelled,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            1200,
        )
        .unwrap_err();
        assert!(matches!(err, QuorumError::NotHolder));
        // Status (and updated_at) unchanged — the rejected update mutated nothing.
        let still = get(&c, cancelled).unwrap().unwrap();
        assert_eq!(still.status, "cancelled");
        assert_eq!(still.updated_at, 1100);

        // (b) done → done re-fire must be rejected (no duplicate task_done, no done-TTL reset).
        let done = create(&mut c, "boss", "y", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(done), &[], TTL, 1000).unwrap();
        update(
            &mut c,
            "A",
            done,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            1100,
        )
        .unwrap();
        let err = update(
            &mut c,
            "A",
            done,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            1200,
        )
        .unwrap_err();
        assert!(matches!(err, QuorumError::NotHolder));
        let still = get(&c, done).unwrap().unwrap();
        assert_eq!(still.status, "done");
        assert_eq!(still.updated_at, 1100); // the rejected re-fire did not bump updated_at
    }

    #[test]
    fn release_returns_task_to_open_and_drops_lease() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        let t = release(&mut c, "A", id, 1100).unwrap();
        assert_eq!(t.status, "open");
        assert!(t.assignee.is_none());
        assert!(!has_live_lease(&c, id, 1100));
        // Released task is re-claimable by anyone.
        assert!(claim(&mut c, "B", Some(id), &[], TTL, 1200)
            .unwrap()
            .is_some());
    }

    #[test]
    fn release_by_nonassignee_fails_loud() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        assert!(matches!(
            release(&mut c, "B", id, 1100).unwrap_err(),
            QuorumError::NotHolder
        ));
    }

    #[test]
    fn cancel_allowed_for_creator_or_assignee() {
        let (_d, mut c) = open_tmp();
        // Creator can cancel an open task.
        let id1 = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        assert_eq!(
            cancel(&mut c, "boss", id1, 1100).unwrap().status,
            "cancelled"
        );
        // Assignee can cancel a claimed task (and the lease is dropped).
        let id2 = create(&mut c, "boss", "y", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id2), &[], TTL, 1000).unwrap();
        assert_eq!(cancel(&mut c, "A", id2, 1100).unwrap().status, "cancelled");
        assert!(!has_live_lease(&c, id2, 1100));
    }

    #[test]
    fn cancel_rejected_for_stranger_and_terminal() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        // A bystander (neither creator nor assignee) cannot cancel.
        assert!(matches!(
            cancel(&mut c, "C", id, 1100).unwrap_err(),
            QuorumError::NotHolder
        ));
        // Already terminal → cannot cancel again.
        cancel(&mut c, "boss", id, 1100).unwrap();
        assert!(matches!(
            cancel(&mut c, "boss", id, 1200).unwrap_err(),
            QuorumError::NotHolder
        ));
    }

    #[test]
    fn list_filters_by_status_and_label() {
        let (_d, mut c) = open_tmp();
        create(
            &mut c,
            "boss",
            "a",
            None,
            0,
            Some("[\"ui\"]"),
            None,
            None,
            1000,
        )
        .unwrap();
        create(
            &mut c,
            "boss",
            "b",
            None,
            0,
            Some("[\"api\"]"),
            None,
            None,
            1000,
        )
        .unwrap();
        assert_eq!(list(&c, Some("open"), None, None).unwrap().len(), 2);
        assert_eq!(list(&c, None, Some("ui"), None).unwrap().len(), 1);
        assert_eq!(list(&c, Some("done"), None, None).unwrap().len(), 0);
    }

    #[test]
    fn task_compact_projects_write_summary_and_omits_body() {
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c,
            "boss",
            "do-thing",
            Some("multi-KB-body-here"),
            5,
            Some(r#"["rust"]"#),
            Some(r#"{"pr":2459}"#),
            None,
            100,
        )
        .unwrap();
        claim(&mut c, "A", Some(id), &[], 1000, 100).unwrap();
        let task = get(&c, id).unwrap().unwrap();
        let compact = TaskCompact::from(&task);
        // Locked field set per #64: id + status + assignee + refs surface; body and the
        // descriptive fields (title/labels/priority) do NOT.
        assert_eq!(compact.id, id);
        assert_eq!(compact.status, "claimed");
        assert_eq!(compact.assignee.as_deref(), Some("A"));
        assert_eq!(compact.refs.as_deref(), Some(r#"{"pr":2459}"#));
        // Lease/note are caller-filled context (None on a bare `From<&Task>`).
        assert!(compact.lease_expires_at.is_none());
        assert!(compact.note_id.is_none());
        // Serialized JSON: no `body`, no `title`, no `labels`, no `priority`, no `notes`.
        // (assignee + refs always present per the spec; lease_expires_at + note_id are
        // omit-empty when None.)
        let json = serde_json::to_string(&compact).unwrap();
        assert!(!json.contains("\"body\""), "body must not surface: {json}");
        assert!(
            !json.contains("\"title\""),
            "title must not surface: {json}"
        );
        assert!(
            !json.contains("\"labels\""),
            "labels must not surface: {json}"
        );
        assert!(
            !json.contains("\"priority\""),
            "priority must not surface: {json}"
        );
        assert!(
            !json.contains("\"notes\""),
            "notes must not surface: {json}"
        );
        assert!(
            !json.contains("\"lease_expires_at\""),
            "lease omitted when None: {json}"
        );
        assert!(
            !json.contains("\"note_id\""),
            "note_id omitted when None: {json}"
        );
        assert!(json.contains("\"id\":"));
        assert!(json.contains("\"status\":\"claimed\""));
        assert!(json.contains("\"assignee\":\"A\""));
        assert!(json.contains("\"refs\":\"{\\\"pr\\\":2459}\""));
    }

    #[test]
    fn task_compact_assignee_serializes_null_after_release() {
        // After a release (or any path that nulls assignee), the field must serialize
        // as `null`, not be omitted — callers care a lot about "who is this assigned to
        // now" and an absent field is harder to parse than an explicit null.
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 100).unwrap();
        claim(&mut c, "A", Some(id), &[], 1000, 100).unwrap();
        let task = release(&mut c, "A", id, 200).unwrap();
        let compact = TaskCompact::from(&task);
        let json = serde_json::to_string(&compact).unwrap();
        assert!(
            json.contains("\"assignee\":null"),
            "assignee must serialize as null when None: {json}"
        );
    }

    #[test]
    fn task_brief_projects_summary_and_omits_body() {
        let (_d, mut c) = open_tmp();
        let id = create(
            &mut c,
            "boss",
            "title here",
            Some("a long body the queue scan should not pay for"),
            5,
            Some("[\"ui\"]"),
            None,
            None,
            1000,
        )
        .unwrap();
        let task = get(&c, id).unwrap().unwrap();
        let brief = TaskBrief::from(&task);
        // Carries the summary fields verbatim, including derived `ready`.
        assert_eq!(brief.id, id);
        assert_eq!(brief.title, "title here");
        assert_eq!(brief.priority, 5);
        assert_eq!(brief.labels.as_deref(), Some("[\"ui\"]"));
        assert_eq!(brief.status, "open");
        assert_eq!(brief.assignee, None);
        assert!(brief.ready); // no deps => ready
        assert_eq!(brief.depends_on, None); // no deps
                                            // The JSON is exactly the 8 summary fields (#86 added depends_on) — body (and
                                            // other non-summary fields) are gone, which is the whole point of --brief.
        let json = serde_json::to_value(&brief).unwrap();
        let obj = json.as_object().unwrap();
        assert_eq!(obj.len(), 8, "brief must serialize exactly 8 fields");
        for k in [
            "id",
            "title",
            "labels",
            "priority",
            "status",
            "assignee",
            "ready",
            "depends_on",
        ] {
            assert!(obj.contains_key(k), "brief missing summary field {k}");
        }
        for k in ["body", "created_by", "created_at", "updated_at", "refs"] {
            assert!(!obj.contains_key(k), "brief must omit {k}");
        }
    }

    #[test]
    fn get_returns_task_or_none() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        assert!(get(&c, id).unwrap().is_some());
        assert!(get(&c, 9999).unwrap().is_none());
    }

    // -- task dependencies (issue #2) -------------------------------------------------------

    /// Force a task to `closed` directly in the DB — there is no public `close()` helper yet
    /// (that's the review automation, issue #10). Bypasses the executor/reviewer split so the
    /// dep-gate tests can simulate the resolved state without staging the whole #10 machinery.
    fn force_close(c: &Connection, id: i64) {
        c.execute("UPDATE tasks SET status='closed' WHERE id=?1", params![id])
            .unwrap();
    }

    #[test]
    fn create_rejects_malformed_depends_on_before_storing() {
        // Cobble-x7M's blocking finding on #18 v1: unvalidated --depends-on poisons every
        // subsequent task-list/task-get/task-cancel via compute_ready -> json_each. Fix:
        // reject malformed JSON at the boundary, never store it.
        let (_d, mut c) = open_tmp();
        // Forgot the brackets (Cobble's repro). Must be a clean Usage error (exit 2).
        let err = create(
            &mut c,
            "boss",
            "bad",
            None,
            0,
            None,
            None,
            Some("1,2"),
            1000,
        )
        .unwrap_err();
        assert!(matches!(err, QuorumError::Usage(_)), "got {err:?}");
        // And nothing was inserted — list is empty, queue not poisoned.
        assert_eq!(list(&c, None, None, None).unwrap().len(), 0);
        // Other malformed shapes — string, object, array-of-strings — all rejected.
        for bad in &["garbage", "{}", r#"["one"]"#, "[1, 2,"] {
            assert!(matches!(
                create(&mut c, "boss", "x", None, 0, None, None, Some(bad), 1000).unwrap_err(),
                QuorumError::Usage(_)
            ));
        }
        // Empty array and well-formed deps still accepted.
        assert!(create(
            &mut c,
            "boss",
            "ok-empty",
            None,
            0,
            None,
            None,
            Some("[]"),
            1000
        )
        .is_ok());
        assert!(create(
            &mut c,
            "boss",
            "ok-one",
            None,
            0,
            None,
            None,
            Some("[1]"),
            1000
        )
        .is_ok());
    }

    #[test]
    fn auto_pick_skips_task_with_unmet_dep() {
        let (_d, mut c) = open_tmp();
        let dep = create(&mut c, "boss", "dep", None, 5, None, None, None, 1000).unwrap();
        let dependent = create(
            &mut c,
            "boss",
            "dependent",
            None,
            5,
            None,
            None,
            Some(&format!("[{dep}]")),
            1000,
        )
        .unwrap();
        // Auto-pick: only `dep` is eligible. The dependent must be invisible to auto-pick
        // until its dep is `closed`.
        let first = claim(&mut c, "A", None, &[], TTL, 1000).unwrap().unwrap();
        assert_eq!(first.id, dep);
        // dep is now `claimed`, dependent stays gated.
        assert!(claim(&mut c, "B", None, &[], TTL, 1000).unwrap().is_none());
        // Forcing dep → done is *not enough* per the #9/#10 alignment: gate is on `closed`.
        c.execute("UPDATE tasks SET status='done' WHERE id=?1", params![dep])
            .unwrap();
        assert!(claim(&mut c, "B", None, &[], TTL, 1000).unwrap().is_none());
        // Once dep is `closed`, the dependent becomes claimable.
        force_close(&c, dep);
        let unblocked = claim(&mut c, "B", None, &[], TTL, 1000).unwrap().unwrap();
        assert_eq!(unblocked.id, dependent);
    }

    #[test]
    fn explicit_task_id_also_respects_dep_gate() {
        let (_d, mut c) = open_tmp();
        let dep = create(&mut c, "boss", "dep", None, 0, None, None, None, 1000).unwrap();
        let dependent = create(
            &mut c,
            "boss",
            "dependent",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            1000,
        )
        .unwrap();
        assert!(claim(&mut c, "A", Some(dependent), &[], TTL, 1000)
            .unwrap()
            .is_none());
        force_close(&c, dep);
        assert!(claim(&mut c, "A", Some(dependent), &[], TTL, 1000)
            .unwrap()
            .is_some());
    }

    #[test]
    fn missing_dep_id_blocks_forever() {
        let (_d, mut c) = open_tmp();
        let dependent = create(
            &mut c,
            "boss",
            "dependent",
            None,
            0,
            None,
            None,
            Some("[9999]"),
            1000,
        )
        .unwrap();
        assert!(claim(&mut c, "A", None, &[], TTL, 1000).unwrap().is_none());
        assert!(claim(&mut c, "A", Some(dependent), &[], TTL, 1000)
            .unwrap()
            .is_none());
    }

    #[test]
    fn cancelled_dep_blocks_dependent() {
        let (_d, mut c) = open_tmp();
        let dep = create(&mut c, "boss", "dep", None, 0, None, None, None, 1000).unwrap();
        let dependent = create(
            &mut c,
            "boss",
            "dependent",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            1000,
        )
        .unwrap();
        cancel(&mut c, "boss", dep, 1100).unwrap();
        assert!(claim(&mut c, "A", None, &[], TTL, 1100).unwrap().is_none());
        assert!(claim(&mut c, "A", Some(dependent), &[], TTL, 1100)
            .unwrap()
            .is_none());
    }

    #[test]
    fn multiple_deps_all_must_be_closed() {
        let (_d, mut c) = open_tmp();
        let d1 = create(&mut c, "boss", "d1", None, 0, None, None, None, 1000).unwrap();
        let d2 = create(&mut c, "boss", "d2", None, 0, None, None, None, 1000).unwrap();
        let dependent = create(
            &mut c,
            "boss",
            "dependent",
            None,
            9,
            None,
            None,
            Some(&format!("[{d1},{d2}]")),
            1000,
        )
        .unwrap();
        force_close(&c, d1);
        assert!(claim(&mut c, "A", Some(dependent), &[], TTL, 1000)
            .unwrap()
            .is_none());
        force_close(&c, d2);
        assert!(claim(&mut c, "A", Some(dependent), &[], TTL, 1000)
            .unwrap()
            .is_some());
    }

    #[test]
    fn get_surfaces_depends_on_and_ready() {
        let (_d, mut c) = open_tmp();
        let dep = create(&mut c, "boss", "dep", None, 0, None, None, None, 1000).unwrap();
        let dependent = create(
            &mut c,
            "boss",
            "dependent",
            None,
            0,
            None,
            None,
            Some(&format!("[{dep}]")),
            1000,
        )
        .unwrap();
        let solo = get(&c, dep).unwrap().unwrap();
        assert!(solo.depends_on.is_none());
        assert!(solo.ready);
        let t = get(&c, dependent).unwrap().unwrap();
        assert_eq!(t.depends_on.as_deref(), Some(format!("[{dep}]").as_str()));
        assert!(!t.ready);
        force_close(&c, dep);
        let t = get(&c, dependent).unwrap().unwrap();
        assert!(t.ready);
    }

    #[test]
    fn dep_gate_composes_with_match_label_filter() {
        // Two AND clauses (dep-ready + label-match) co-exist in the same auto-pick selector.
        // Smoke-test the composition: only a task matching BOTH label AND ready-deps wins.
        let (_d, mut c) = open_tmp();
        let dep = create(&mut c, "boss", "dep", None, 0, None, None, None, 1000).unwrap();
        // (a) labeled but gated by an unmet dep
        create(
            &mut c,
            "boss",
            "labeled-gated",
            None,
            9,
            Some(r#"["tier:opus-47"]"#),
            None,
            Some(&format!("[{dep}]")),
            1000,
        )
        .unwrap();
        // (b) ready but wrong label
        create(
            &mut c,
            "boss",
            "ready-no-label",
            None,
            9,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        // (c) labeled AND ready
        let want = create(
            &mut c,
            "boss",
            "labeled-and-ready",
            None,
            1,
            Some(r#"["tier:opus-47"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        let t = claim(&mut c, "A", None, &["tier:opus-47"], TTL, 1000)
            .unwrap()
            .unwrap();
        assert_eq!(t.id, want, "claim must pick the labeled+ready task");
    }

    // -- Task notes (issue #3) -----------------------------------------------------------

    #[test]
    fn add_note_appends_and_get_with_notes_returns_in_order() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        let n1 = add_note(&mut c, "A", id, "first crumb", 1100)
            .unwrap()
            .unwrap();
        let n2 = add_note(&mut c, "B", id, "second crumb", 1200)
            .unwrap()
            .unwrap();
        assert!(n2 > n1);

        let detail = get_with_notes(&c, id).unwrap().unwrap();
        assert_eq!(detail.notes.len(), 2);
        assert_eq!(detail.notes[0].agent, "A");
        assert_eq!(detail.notes[0].body, "first crumb");
        assert_eq!(detail.notes[0].ts, 1100);
        assert_eq!(detail.notes[1].agent, "B");
        assert_eq!(detail.notes[1].body, "second crumb");
        assert_eq!(detail.notes[1].ts, 1200);
    }

    #[test]
    fn add_note_on_missing_task_returns_none() {
        let (_d, mut c) = open_tmp();
        assert!(add_note(&mut c, "A", 9999, "into the void", 1000)
            .unwrap()
            .is_none());
        // and nothing was inserted
        let n: i64 = c
            .query_row("SELECT count(*) FROM task_notes", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn notes_preserve_byte_exact_body() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        let body = "héllo \"world\"\n`$x`\nmultiline\n";
        add_note(&mut c, "A", id, body, 1100).unwrap().unwrap();
        let detail = get_with_notes(&c, id).unwrap().unwrap();
        assert_eq!(detail.notes[0].body, body);
    }

    #[test]
    fn anyone_can_add_a_note_no_assignee_guard() {
        // Notes are public breadcrumbs — the assignee restriction from `update` does NOT
        // apply, so a non-assignee agent can append. (Differentiates the contract clearly:
        // `update` mutates the task; `add_note` annotates it.)
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(id), &[], TTL, 1000).unwrap();
        // B is not the assignee but can still leave a note
        let nid = add_note(&mut c, "B", id, "from a watcher", 1100)
            .unwrap()
            .unwrap();
        assert!(nid > 0);
    }

    #[test]
    fn notes_are_not_swept_when_other_rows_expire() {
        // Notes have no expires_at column — the sweeper's TTL pass cannot evict them.
        // Verify by running a sweep at a far-future `now` and seeing the note persist.
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        add_note(&mut c, "A", id, "durable", 1100).unwrap().unwrap();
        crate::sweep::sweep_all(&c, 9_999_999).unwrap();
        let detail = get_with_notes(&c, id).unwrap().unwrap();
        assert_eq!(detail.notes.len(), 1);
        assert_eq!(detail.notes[0].body, "durable");
    }

    #[test]
    fn task_without_notes_yields_empty_notes_array() {
        let (_d, mut c) = open_tmp();
        let id = create(&mut c, "boss", "x", None, 0, None, None, None, 1000).unwrap();
        let detail = get_with_notes(&c, id).unwrap().unwrap();
        assert!(detail.notes.is_empty());
    }

    #[test]
    fn get_with_notes_on_missing_task_is_none() {
        let (_d, c) = open_tmp();
        assert!(get_with_notes(&c, 9999).unwrap().is_none());
    }

    // -- Review-as-task: claim-side filters (issue #10, Phase 1) ---------------------------
    //
    // Phase 1 only wires the claim-side gates that READ `kind:review` / `orig` / `sticky_until`.
    // Phase 2 will land the auto-spawn + verdict that WRITE those fields. To exercise the
    // gates standalone, these tests stamp the relevant fields directly via SQL — mirroring
    // the existing `force_close` shortcut used by the dep-gate tests above.

    /// Mark a task as a review task spawned by `orig_agent`. Mirrors the auto-spawn that
    /// Phase 2 will add (label `kind:review` + the new `orig` column).
    fn force_review_task(c: &Connection, id: i64, orig_agent: &str) {
        c.execute(
            "UPDATE tasks SET labels = ?1, orig = ?2 WHERE id = ?3",
            params![r#"["kind:review"]"#, orig_agent, id],
        )
        .unwrap();
    }

    /// Stamp a sticky-reopen window on a task. Mirrors what the Phase 2 `changes` verdict
    /// will do (set status=open, assignee=orig, sticky_until=now+W).
    fn force_sticky_reopen(c: &Connection, id: i64, orig_agent: &str, sticky_until: i64) {
        c.execute(
            "UPDATE tasks SET status='open', assignee=?1, sticky_until=?2 WHERE id=?3",
            params![orig_agent, sticky_until, id],
        )
        .unwrap();
    }

    #[test]
    fn claim_blocks_orig_from_own_review_task_auto_pick() {
        // Auto-pick must skip a review task whose `orig` equals the caller — even if it's the
        // only open task and has higher priority. The orig sees `None` (clean exit 1), not an
        // error.
        let (_d, mut c) = open_tmp();
        let rid = create(
            &mut c,
            "boss",
            "review of A's work",
            None,
            9,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        force_review_task(&c, rid, "A");
        // A tries to auto-pick: nothing claimable (the only open task is their own review).
        assert!(claim(&mut c, "A", None, &[], TTL, 1000).unwrap().is_none());
        // Sanity: a non-orig (B) sees and claims it.
        let t = claim(&mut c, "B", None, &[], TTL, 1000).unwrap().unwrap();
        assert_eq!(t.id, rid);
        assert_eq!(t.assignee.as_deref(), Some("B"));
        assert_eq!(t.orig.as_deref(), Some("A"));
    }

    #[test]
    fn claim_blocks_orig_from_own_review_task_explicit_id() {
        // Even with an explicit --task-id, the orig is rejected (no bypass). Returns None,
        // which the CLI maps to a clean exit 1 — the dominant lost-race shape, not an error.
        let (_d, mut c) = open_tmp();
        let rid = create(
            &mut c,
            "boss",
            "review of A's work",
            None,
            0,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        force_review_task(&c, rid, "A");
        assert!(claim(&mut c, "A", Some(rid), &[], TTL, 1000)
            .unwrap()
            .is_none());
        // And a non-orig with the explicit id can still claim.
        assert!(claim(&mut c, "B", Some(rid), &[], TTL, 1000)
            .unwrap()
            .is_some());
    }

    #[test]
    fn claim_allows_non_orig_on_review_task() {
        // Non-orig sees and claims a review task normally — the self-review block targets
        // exactly the orig, nobody else.
        let (_d, mut c) = open_tmp();
        let rid = create(
            &mut c,
            "boss",
            "review of A's work",
            None,
            0,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        force_review_task(&c, rid, "A");
        let t = claim(&mut c, "C", None, &[], TTL, 1000).unwrap().unwrap();
        assert_eq!(t.id, rid);
        assert_eq!(t.assignee.as_deref(), Some("C"));
    }

    #[test]
    fn claim_blocked_during_sticky_window_for_non_orig() {
        // A reopened task in its sticky window is invisible to non-orig (auto-pick) and
        // rejected with `None` for non-orig via explicit --task-id. The orig may still claim.
        let (_d, mut c) = open_tmp();
        let tid = create(&mut c, "boss", "T", None, 9, None, None, None, 1000).unwrap();
        // Sticky window from t=1000 to t=2000; caller A is orig.
        force_sticky_reopen(&c, tid, "A", 2000);
        // Auto-pick at t=1500 by non-orig B: nothing visible (the only open task is sticky to A).
        assert!(claim(&mut c, "B", None, &[], TTL, 1500).unwrap().is_none());
        // Explicit --task-id by B is also rejected.
        assert!(claim(&mut c, "B", Some(tid), &[], TTL, 1500)
            .unwrap()
            .is_none());
    }

    #[test]
    fn claim_allowed_during_sticky_window_for_orig() {
        // The orig (assignee) may claim during the sticky window — that's the whole point of
        // "orig has the context, sticky-reserves them the first crack."
        let (_d, mut c) = open_tmp();
        let tid = create(&mut c, "boss", "T", None, 9, None, None, None, 1000).unwrap();
        force_sticky_reopen(&c, tid, "A", 2000);
        let t = claim(&mut c, "A", None, &[], TTL, 1500).unwrap().unwrap();
        assert_eq!(t.id, tid);
        assert_eq!(t.assignee.as_deref(), Some("A"));
        assert_eq!(t.status, "claimed");
        assert_eq!(t.sticky_until, Some(2000));
    }

    #[test]
    fn claim_allowed_after_sticky_window_expires_for_anyone() {
        // Once `now >= sticky_until`, the gate lifts — any agent (including a brand-new C)
        // may claim. Eligibility-only filter; priority is unchanged so the row still sorts
        // by priority DESC, id ASC in auto-pick.
        let (_d, mut c) = open_tmp();
        let tid = create(&mut c, "boss", "T", None, 9, None, None, None, 1000).unwrap();
        force_sticky_reopen(&c, tid, "A", 2000);
        // At t=2001, sticky_until <= now → gate lifts.
        let t = claim(&mut c, "C", None, &[], TTL, 2001).unwrap().unwrap();
        assert_eq!(t.id, tid);
        assert_eq!(t.assignee.as_deref(), Some("C"));
    }

    #[test]
    fn claim_at_exact_sticky_boundary_is_allowed_for_anyone() {
        // Spec boundary alignment with the project's "expiry is `expires_at > now`" rule
        // (see CLAUDE.md gotcha "expiry boundary must be consistent everywhere"). The
        // sticky predicate uses `<= now` to be DEAD, so `now == sticky_until` is DEAD →
        // anyone may claim. The reaper boundary uses `<=`; we match it.
        let (_d, mut c) = open_tmp();
        let tid = create(&mut c, "boss", "T", None, 0, None, None, None, 1000).unwrap();
        force_sticky_reopen(&c, tid, "A", 2000);
        let t = claim(&mut c, "C", None, &[], TTL, 2000).unwrap().unwrap();
        assert_eq!(t.id, tid);
    }

    // -- Review-as-task: auto-spawn + verdict (issue #10, Phase 2) -------------------------
    //
    // These pin the WRITE side of review-as-task: marking a non-review task `done` MUST
    // auto-spawn a review task in the same txn; a reviewer's `--verdict approve|changes` on
    // the spawned review MUST chain the original task's state change atomically. Phase 1's
    // claim-side gates already filter on the columns these write; the spawn/verdict path is
    // what fills them.

    /// Find the most-recently inserted review task (label `kind:review`, highest id). Used
    /// to look up the auto-spawned R from a test that just submitted T's `done`.
    fn last_review_task(c: &Connection) -> Task {
        list(c, Some("open"), Some(REVIEW_LABEL), None)
            .unwrap()
            .into_iter()
            .max_by_key(|t| t.id)
            .expect("expected an auto-spawned review task")
    }

    #[test]
    fn done_auto_spawns_review_with_orig_and_refs_review_of() {
        let (_d, mut c) = open_tmp();
        let tid = create(&mut c, "boss", "fix bug", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(tid), &[], TTL, 1000).unwrap();
        update(
            &mut c,
            "A",
            tid,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            1100,
        )
        .unwrap();
        let r = last_review_task(&c);
        // Title is review-prefixed; review_of points at T; orig is the executor.
        assert_eq!(r.title, "review: fix bug");
        assert_eq!(r.status, "open");
        assert_eq!(r.priority, REVIEW_PRIORITY);
        assert_eq!(r.orig.as_deref(), Some("A"));
        assert!(r.labels.as_deref().unwrap().contains(REVIEW_LABEL));
        let refs: serde_json::Value = serde_json::from_str(r.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["review_of"].as_i64(), Some(tid));
    }

    #[test]
    fn done_on_review_task_does_not_recurse_spawn() {
        // Reviews don't spawn reviews — the source-is-review check inside `update` skips the
        // auto-spawn for `kind:review`-labeled tasks. The reviewer marks R done with a
        // verdict; no R-of-R appears.
        let (_d, mut c) = open_tmp();
        let tid = create(&mut c, "boss", "T", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(tid), &[], TTL, 1000).unwrap();
        update(
            &mut c,
            "A",
            tid,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            1100,
        )
        .unwrap();
        let r = last_review_task(&c);
        claim(&mut c, "B", Some(r.id), &[], TTL, 1200).unwrap();
        update(
            &mut c,
            "B",
            r.id,
            &TaskUpdate {
                status: Some("done"),
                verdict: Some("approve"),
                ..Default::default()
            },
            1300,
        )
        .unwrap();
        // Only one review task ever existed — no recursive R-of-R.
        assert_eq!(
            list(&c, None, Some(REVIEW_LABEL), None).unwrap().len(),
            1,
            "review must not auto-spawn its own review"
        );
    }

    #[test]
    fn auto_spawn_inherits_pr_ref_when_source_had_one() {
        let (_d, mut c) = open_tmp();
        let tid = create(
            &mut c,
            "boss",
            "fix bug",
            None,
            0,
            None,
            Some(r#"{"pr":2459}"#),
            None,
            1000,
        )
        .unwrap();
        claim(&mut c, "A", Some(tid), &[], TTL, 1000).unwrap();
        update(
            &mut c,
            "A",
            tid,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            1100,
        )
        .unwrap();
        let r = last_review_task(&c);
        let refs: serde_json::Value = serde_json::from_str(r.refs.as_deref().unwrap()).unwrap();
        assert_eq!(refs["review_of"].as_i64(), Some(tid));
        // pr breadcrumb travels with the review for human navigation.
        assert_eq!(refs["pr"].as_i64(), Some(2459));
    }

    #[test]
    fn auto_spawn_inherits_tier_label_from_source() {
        // #105: a tiered task's review must carry the same tier label so
        // the review routes to a same-or-higher-tier agent.
        let (_d, mut c) = open_tmp();
        let tid = create(
            &mut c,
            "boss",
            "hard work",
            None,
            0,
            Some(r#"["tier:opus-47","area:core"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        claim(&mut c, "A", Some(tid), &[], TTL, 1000).unwrap();
        update(
            &mut c,
            "A",
            tid,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            1100,
        )
        .unwrap();
        let r = last_review_task(&c);
        let labels: Vec<String> = serde_json::from_str(r.labels.as_deref().unwrap()).unwrap();
        assert!(
            labels.contains(&"kind:review".to_string()),
            "review must carry kind:review",
        );
        assert!(
            labels.contains(&"tier:opus-47".to_string()),
            "review must inherit tier:opus-47 from source",
        );
        assert!(
            !labels.contains(&"area:core".to_string()),
            "non-tier labels must NOT be inherited",
        );
    }

    #[test]
    fn auto_spawn_no_tier_when_source_untiered() {
        // #105 backward compat: an untiered source produces an untiered review
        // (only `kind:review`) — same as before the change.
        let (_d, mut c) = open_tmp();
        let tid = create(&mut c, "boss", "easy", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(tid), &[], TTL, 1000).unwrap();
        update(
            &mut c,
            "A",
            tid,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            1100,
        )
        .unwrap();
        let r = last_review_task(&c);
        let labels: Vec<String> = serde_json::from_str(r.labels.as_deref().unwrap()).unwrap();
        assert_eq!(labels, vec!["kind:review"]);
    }

    #[test]
    fn claim_tiered_review_obeys_tier_matching() {
        // #105: a review task that carries a tier label is NOT tier-exempt —
        // a weaker-tier agent must not claim it.
        let (_d, mut c) = open_tmp();
        let _review = create(
            &mut c,
            "boss",
            "review-47",
            None,
            1000,
            Some(r#"["kind:review","tier:opus-47"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        // A tier:opus-46 agent should NOT see this review.
        let got = claim(&mut c, "weak-agent", None, &["tier:opus-46"], TTL, 1000).unwrap();
        assert!(
            got.is_none(),
            "tier:opus-46 agent must NOT claim a tier:opus-47 review task",
        );
        // A tier:opus-47 agent SHOULD claim it.
        let got = claim(&mut c, "strong-agent", None, &["tier:opus-47"], TTL, 1000)
            .unwrap()
            .expect("tier:opus-47 agent should claim a tier:opus-47 review");
        assert_eq!(got.id, _review);
    }

    #[test]
    fn verdict_approve_closes_target_task_atomically() {
        let (_d, mut c) = open_tmp();
        let tid = create(&mut c, "boss", "T", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(tid), &[], TTL, 1000).unwrap();
        update(
            &mut c,
            "A",
            tid,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            1100,
        )
        .unwrap();
        let r = last_review_task(&c);
        claim(&mut c, "B", Some(r.id), &[], TTL, 1200).unwrap();
        let r_after = update(
            &mut c,
            "B",
            r.id,
            &TaskUpdate {
                status: Some("done"),
                verdict: Some("approve"),
                ..Default::default()
            },
            1300,
        )
        .unwrap();
        assert_eq!(r_after.status, "done");
        // Original task is now `closed` (terminal).
        let t = get(&c, tid).unwrap().unwrap();
        assert_eq!(t.status, "closed");
        // No sticky carried over.
        assert!(t.sticky_until.is_none());
    }

    #[test]
    fn verdict_changes_reopens_target_with_rework_label_and_sticky() {
        let (_d, mut c) = open_tmp();
        let tid = create(&mut c, "boss", "T", None, 5, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(tid), &[], TTL, 1000).unwrap();
        update(
            &mut c,
            "A",
            tid,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            1100,
        )
        .unwrap();
        let r = last_review_task(&c);
        claim(&mut c, "B", Some(r.id), &[], TTL, 1200).unwrap();
        update(
            &mut c,
            "B",
            r.id,
            &TaskUpdate {
                status: Some("done"),
                verdict: Some("changes"),
                ..Default::default()
            },
            1300,
        )
        .unwrap();
        let t = get(&c, tid).unwrap().unwrap();
        assert_eq!(t.status, "open");
        assert_eq!(t.assignee.as_deref(), Some("A"));
        assert_eq!(t.sticky_until, Some(1300 + STICKY_WINDOW_SECS));
        assert!(t.labels.as_deref().unwrap().contains(REWORK_LABEL));
        // Priority unchanged — eligibility-only, no priority bump.
        assert_eq!(t.priority, 5);
    }

    #[test]
    fn verdict_changes_dedupes_rework_label_across_rounds() {
        // Multiple `changes` rounds for the same T must not accumulate duplicate "rework"
        // entries in the labels JSON. The dedup CASE-WHEN in apply_verdict pins this.
        let (_d, mut c) = open_tmp();
        let tid = create(
            &mut c,
            "boss",
            "T",
            None,
            0,
            Some(r#"["other"]"#),
            None,
            None,
            1000,
        )
        .unwrap();
        // Round 1: A → done → B reviews → changes → T reopened with rework.
        claim(&mut c, "A", Some(tid), &[], TTL, 1000).unwrap();
        update(
            &mut c,
            "A",
            tid,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            1100,
        )
        .unwrap();
        let r1 = last_review_task(&c);
        claim(&mut c, "B", Some(r1.id), &[], TTL, 1200).unwrap();
        update(
            &mut c,
            "B",
            r1.id,
            &TaskUpdate {
                status: Some("done"),
                verdict: Some("changes"),
                ..Default::default()
            },
            1300,
        )
        .unwrap();
        // Round 2: A (still in sticky) re-claims, redos, done → B reviews → changes again.
        // Pass current "now" past sticky_until so A can claim freely; A is orig either way.
        claim(&mut c, "A", Some(tid), &[], TTL, 4000).unwrap();
        update(
            &mut c,
            "A",
            tid,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            4100,
        )
        .unwrap();
        let r2 = last_review_task(&c);
        assert_ne!(r1.id, r2.id, "second round must spawn a fresh review");
        claim(&mut c, "B", Some(r2.id), &[], TTL, 4200).unwrap();
        update(
            &mut c,
            "B",
            r2.id,
            &TaskUpdate {
                status: Some("done"),
                verdict: Some("changes"),
                ..Default::default()
            },
            4300,
        )
        .unwrap();
        // T's labels: exactly one "rework", "other" preserved.
        let t = get(&c, tid).unwrap().unwrap();
        let labels_json: serde_json::Value =
            serde_json::from_str(t.labels.as_deref().unwrap()).unwrap();
        let arr = labels_json.as_array().unwrap();
        let rework_count = arr
            .iter()
            .filter(|v| v.as_str() == Some(REWORK_LABEL))
            .count();
        assert_eq!(rework_count, 1, "rework must appear exactly once");
        assert!(arr.iter().any(|v| v.as_str() == Some("other")));
    }

    #[test]
    fn verdict_changes_critical_priority_uses_shorter_sticky_window() {
        // Issue #88: a task created with priority >= CRITICAL_PRIORITY_THRESHOLD gets
        // STICKY_WINDOW_SECS_CRITICAL on `changes`-verdict reopen, not the default
        // STICKY_WINDOW_SECS. The CASE in apply_verdict picks the window per-row from
        // `priority`; this test pins both branches.
        let (_d, mut c) = open_tmp();
        let tid_crit = create(
            &mut c,
            "boss",
            "T-critical",
            None,
            CRITICAL_PRIORITY_THRESHOLD, // exactly at threshold: gets shorter window
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        let tid_normal = create(
            &mut c,
            "boss",
            "T-normal",
            None,
            CRITICAL_PRIORITY_THRESHOLD - 1, // one below threshold: gets default window
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        for tid in [tid_crit, tid_normal] {
            claim(&mut c, "A", Some(tid), &[], TTL, 1000).unwrap();
            update(
                &mut c,
                "A",
                tid,
                &TaskUpdate {
                    status: Some("done"),
                    ..Default::default()
                },
                1100,
            )
            .unwrap();
            let r = last_review_task(&c);
            claim(&mut c, "B", Some(r.id), &[], TTL, 1200).unwrap();
            update(
                &mut c,
                "B",
                r.id,
                &TaskUpdate {
                    status: Some("done"),
                    verdict: Some("changes"),
                    ..Default::default()
                },
                1300,
            )
            .unwrap();
        }
        let t_crit = get(&c, tid_crit).unwrap().unwrap();
        let t_normal = get(&c, tid_normal).unwrap().unwrap();
        assert_eq!(
            t_crit.sticky_until,
            Some(1300 + STICKY_WINDOW_SECS_CRITICAL),
            "critical task must use STICKY_WINDOW_SECS_CRITICAL"
        );
        assert_eq!(
            t_normal.sticky_until,
            Some(1300 + STICKY_WINDOW_SECS),
            "non-critical task must use STICKY_WINDOW_SECS"
        );
        // The compile-time `const _: () = assert!(STICKY_WINDOW_SECS_CRITICAL <
        // STICKY_WINDOW_SECS / 10)` pins the "shorter window is meaningfully shorter"
        // property; nothing to assert at runtime here.
    }

    #[test]
    fn verdict_changes_critical_sticky_falls_back_to_non_orig_after_short_window() {
        // The acceptance test for issue #88: a critical CHANGES_REQUESTED reopen can be
        // claimed by a non-author idle agent without waiting out a multi-minute sticky
        // window. End-to-end: spawn T (critical), claim by A, done, review by B, changes
        // verdict, then verify a non-A agent C can claim T after just past
        // STICKY_WINDOW_SECS_CRITICAL — well before the default STICKY_WINDOW_SECS would
        // have expired.
        let (_d, mut c) = open_tmp();
        let tid = create(
            &mut c,
            "boss",
            "T",
            None,
            CRITICAL_PRIORITY_THRESHOLD,
            None,
            None,
            None,
            1000,
        )
        .unwrap();
        claim(&mut c, "A", Some(tid), &[], TTL, 1000).unwrap();
        update(
            &mut c,
            "A",
            tid,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            1100,
        )
        .unwrap();
        let r = last_review_task(&c);
        claim(&mut c, "B", Some(r.id), &[], TTL, 1200).unwrap();
        update(
            &mut c,
            "B",
            r.id,
            &TaskUpdate {
                status: Some("done"),
                verdict: Some("changes"),
                ..Default::default()
            },
            1300,
        )
        .unwrap();
        // Within the shortened sticky window: only A (orig) may claim.
        assert!(
            claim(
                &mut c,
                "C",
                None,
                &[],
                TTL,
                1300 + STICKY_WINDOW_SECS_CRITICAL / 2
            )
            .unwrap()
            .is_none(),
            "non-orig C must be blocked during the shortened sticky window"
        );
        // Past the shortened sticky window (but WELL before STICKY_WINDOW_SECS would have
        // expired): C may claim. This is the fast-fallback property.
        let now_after = 1300 + STICKY_WINDOW_SECS_CRITICAL + 1;
        assert!(
            now_after < 1300 + STICKY_WINDOW_SECS,
            "fast-fallback claim must happen well before the default window expires"
        );
        let t = claim(&mut c, "C", None, &[], TTL, now_after)
            .unwrap()
            .unwrap();
        assert_eq!(t.id, tid);
        assert_eq!(t.assignee.as_deref(), Some("C"));
    }

    #[test]
    fn verdict_changes_on_review_target_reopens_to_pool() {
        // #115 Bug 2: a `changes` verdict targeting a kind:review task must reopen it
        // to the pool (assignee=NULL, sticky_until=NULL) instead of sticky-to-orig.
        let (_d, mut c) = open_tmp();
        // Set up target T: a kind:review task already in "done" status. Direct SQL
        // because the normal API can't create a done review without a full chain.
        let review_labels = format!(r#"["{REVIEW_LABEL}"]"#);
        c.execute(
            "INSERT INTO tasks(title, body, status, priority, labels, assignee, created_by,
                               created_at, updated_at, refs, depends_on, sticky_until, orig)
             VALUES ('review: fix bug', NULL, 'done', 0, ?1, 'OldReviewer', 'boss',
                     1000, 1100, NULL, NULL, NULL, 'OrigAuthor')",
            params![review_labels],
        )
        .unwrap();
        let target_id = c.last_insert_rowid();
        // Create review R that targets T via review_of.
        let review_refs = format!(r#"{{"review_of":{target_id}}}"#);
        let rid = create(
            &mut c,
            "OrigAuthor",
            "review: review: fix bug",
            None,
            REVIEW_PRIORITY,
            Some(&review_labels),
            Some(&review_refs),
            None,
            1000,
        )
        .unwrap();
        // Manually set orig on R (create() doesn't support orig).
        c.execute(
            "UPDATE tasks SET orig = 'OrigAuthor' WHERE id = ?1",
            params![rid],
        )
        .unwrap();
        claim(&mut c, "B", Some(rid), &[], TTL, 1200).unwrap();
        update(
            &mut c,
            "B",
            rid,
            &TaskUpdate {
                status: Some("done"),
                verdict: Some("changes"),
                ..Default::default()
            },
            1300,
        )
        .unwrap();
        let t = get(&c, target_id).unwrap().unwrap();
        assert_eq!(t.status, "open");
        assert_eq!(
            t.assignee, None,
            "review target must reopen to the pool, not sticky to orig"
        );
        assert_eq!(
            t.sticky_until, None,
            "review target must have no sticky window"
        );
        assert!(
            t.labels.as_deref().unwrap().contains(REWORK_LABEL),
            "rework label should still be added"
        );
    }

    #[test]
    fn verdict_required_on_review_task_done() {
        // A review task marked `done` without a verdict is a usage error AND the half-applied
        // status change is rolled back — the review stays `claimed` (no state drift).
        let (_d, mut c) = open_tmp();
        let tid = create(&mut c, "boss", "T", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(tid), &[], TTL, 1000).unwrap();
        update(
            &mut c,
            "A",
            tid,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            1100,
        )
        .unwrap();
        let r = last_review_task(&c);
        claim(&mut c, "B", Some(r.id), &[], TTL, 1200).unwrap();
        let err = update(
            &mut c,
            "B",
            r.id,
            &TaskUpdate {
                status: Some("done"),
                // no verdict
                ..Default::default()
            },
            1300,
        )
        .unwrap_err();
        assert!(matches!(err, QuorumError::Usage(ref m) if m.contains("verdict")));
        // R rolled back to its prior status (claimed by B).
        let r_after = get(&c, r.id).unwrap().unwrap();
        assert_eq!(r_after.status, "claimed");
        // T untouched.
        let t = get(&c, tid).unwrap().unwrap();
        assert_eq!(t.status, "done");
    }

    #[test]
    fn verdict_forbidden_on_non_review_task() {
        // A non-review task marked done with a verdict is a usage error; rollback again so
        // the regular `done` half doesn't half-apply.
        let (_d, mut c) = open_tmp();
        let tid = create(&mut c, "boss", "T", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(tid), &[], TTL, 1000).unwrap();
        let err = update(
            &mut c,
            "A",
            tid,
            &TaskUpdate {
                status: Some("done"),
                verdict: Some("approve"),
                ..Default::default()
            },
            1100,
        )
        .unwrap_err();
        assert!(matches!(err, QuorumError::Usage(ref m) if m.contains("kind:review")));
        let t = get(&c, tid).unwrap().unwrap();
        assert_eq!(
            t.status, "claimed",
            "non-review verdict must roll back to claimed"
        );
    }

    #[test]
    fn verdict_invalid_value_is_usage_error_pre_txn() {
        // Bad verdict value rejected up-front (before begin_immediate) — no write hit.
        let (_d, mut c) = open_tmp();
        let tid = create(&mut c, "boss", "T", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(tid), &[], TTL, 1000).unwrap();
        let err = update(
            &mut c,
            "A",
            tid,
            &TaskUpdate {
                status: Some("done"),
                verdict: Some("maybe"),
                ..Default::default()
            },
            1100,
        )
        .unwrap_err();
        assert!(matches!(err, QuorumError::Usage(ref m) if m.contains("verdict")));
    }

    #[test]
    fn verdict_without_status_done_is_usage_error_pre_txn() {
        let (_d, mut c) = open_tmp();
        let tid = create(&mut c, "boss", "T", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(tid), &[], TTL, 1000).unwrap();
        let err = update(
            &mut c,
            "A",
            tid,
            &TaskUpdate {
                verdict: Some("approve"),
                ..Default::default()
            },
            1100,
        )
        .unwrap_err();
        assert!(
            matches!(err, QuorumError::Usage(ref m) if m.contains("only valid with --status done"))
        );
    }

    #[test]
    fn spawn_then_approve_emits_events_for_both() {
        // Acceptance: spawn / verdict / reopen emit events (#6).
        let (_d, mut c) = open_tmp();
        let tid = create(&mut c, "boss", "T", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(tid), &[], TTL, 1000).unwrap();
        update(
            &mut c,
            "A",
            tid,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            1100,
        )
        .unwrap();
        let r = last_review_task(&c);
        // review_spawned event on the review's lease target.
        let spawn_events =
            crate::events::list(&c, 0, Some(&lease_target(r.id)), 100, 1100).unwrap();
        assert!(
            spawn_events.iter().any(|e| e.kind == "review_spawned"),
            "review_spawned event must fire"
        );
        // Approve → task_closed on T.
        claim(&mut c, "B", Some(r.id), &[], TTL, 1200).unwrap();
        update(
            &mut c,
            "B",
            r.id,
            &TaskUpdate {
                status: Some("done"),
                verdict: Some("approve"),
                ..Default::default()
            },
            1300,
        )
        .unwrap();
        let t_events = crate::events::list(&c, 0, Some(&lease_target(tid)), 100, 1300).unwrap();
        assert!(t_events.iter().any(|e| e.kind == "task_closed"));
    }

    #[test]
    fn spawn_then_changes_emits_task_reopened() {
        let (_d, mut c) = open_tmp();
        let tid = create(&mut c, "boss", "T", None, 0, None, None, None, 1000).unwrap();
        claim(&mut c, "A", Some(tid), &[], TTL, 1000).unwrap();
        update(
            &mut c,
            "A",
            tid,
            &TaskUpdate {
                status: Some("done"),
                ..Default::default()
            },
            1100,
        )
        .unwrap();
        let r = last_review_task(&c);
        claim(&mut c, "B", Some(r.id), &[], TTL, 1200).unwrap();
        update(
            &mut c,
            "B",
            r.id,
            &TaskUpdate {
                status: Some("done"),
                verdict: Some("changes"),
                ..Default::default()
            },
            1300,
        )
        .unwrap();
        let t_events = crate::events::list(&c, 0, Some(&lease_target(tid)), 100, 1300).unwrap();
        assert!(t_events.iter().any(|e| e.kind == "task_reopened"));
    }

    #[test]
    fn release_clears_sticky_until() {
        // If the sticky-orig releases (gives up) during the window, sticky_until is cleared
        // so the task is immediately claimable by anyone — no 30-min dead window.
        let (_d, mut c) = open_tmp();
        let tid = create(&mut c, "boss", "T", None, 0, None, None, None, 1000).unwrap();
        // Stage: simulate a post-changes sticky-reopen state directly, then have A claim &
        // release it to verify sticky_until is wiped.
        force_sticky_reopen(&c, tid, "A", 2000);
        claim(&mut c, "A", Some(tid), &[], TTL, 1500).unwrap();
        release(&mut c, "A", tid, 1600).unwrap();
        let t = get(&c, tid).unwrap().unwrap();
        assert!(t.sticky_until.is_none(), "release must clear sticky_until");
        // C (non-orig) can immediately claim despite the original window being unexpired.
        let claimed = claim(&mut c, "C", None, &[], TTL, 1700).unwrap().unwrap();
        assert_eq!(claimed.id, tid);
    }

    #[test]
    fn cancel_clears_sticky_until() {
        let (_d, mut c) = open_tmp();
        let tid = create(&mut c, "boss", "T", None, 0, None, None, None, 1000).unwrap();
        force_sticky_reopen(&c, tid, "A", 2000);
        cancel(&mut c, "boss", tid, 1500).unwrap();
        let t = get(&c, tid).unwrap().unwrap();
        assert!(t.sticky_until.is_none(), "cancel must clear sticky_until");
        assert_eq!(t.status, "cancelled");
    }

    #[test]
    fn sticky_gate_composes_with_dep_gate_and_label_filter() {
        // All three claim-side gates (dep-ready, self-review block, sticky window) are pure
        // AND-ed in the same selector. Smoke-test the composition: a task with an unmet dep
        // AND a sticky window must remain unclaimable by anyone until BOTH gates clear.
        let (_d, mut c) = open_tmp();
        let dep = create(&mut c, "boss", "dep", None, 0, None, None, None, 1000).unwrap();
        let dependent = create(
            &mut c,
            "boss",
            "dependent",
            None,
            5,
            None,
            None,
            Some(&format!("[{dep}]")),
            1000,
        )
        .unwrap();
        // Stamp the dependent as sticky-reopened to A through t=2000.
        force_sticky_reopen(&c, dependent, "A", 2000);
        // A is orig + within sticky window, but the dep is unmet → still blocked.
        assert!(claim(&mut c, "A", Some(dependent), &[], TTL, 1500)
            .unwrap()
            .is_none());
        // After force-closing the dep, A can claim (orig in sticky); B still cannot.
        c.execute("UPDATE tasks SET status='closed' WHERE id=?1", params![dep])
            .unwrap();
        assert!(claim(&mut c, "B", Some(dependent), &[], TTL, 1500)
            .unwrap()
            .is_none());
        assert!(claim(&mut c, "A", Some(dependent), &[], TTL, 1500)
            .unwrap()
            .is_some());
    }
}
