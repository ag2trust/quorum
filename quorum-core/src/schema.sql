-- Quorum schema (SCHEMA_VERSION = 39). All statements idempotent (IF NOT EXISTS) so the
-- migration is safe to run on every open. See docs/2026-06-23-quorum-design.md §Data model.

CREATE TABLE IF NOT EXISTS agents (
    id            TEXT PRIMARY KEY,
    first_seen    INTEGER NOT NULL,
    last_seen     INTEGER NOT NULL,
    tier          TEXT,
    -- Agent-retirement state machine (issue #97): 'active' | 'retiring' | 'retired'.
    -- Transitions are computed inside `sync::tick` from the load_score budget.
    -- Session reset (issue #125): when a returning agent's offline gap >= ONLINE_WINDOW,
    -- `touch` resets first_seen + retire_status + retired_at — reused names start fresh.
    retire_status TEXT NOT NULL DEFAULT 'active',
    -- Unix-ts the agent reached 'retired' (NULL until then). Surfaced in `quorum status`
    -- under the retired-agents list so the owner sees capacity dropping in real time.
    retired_at    INTEGER
);

CREATE TABLE IF NOT EXISTS messages (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          INTEGER NOT NULL,
    author      TEXT NOT NULL,
    topic       TEXT NOT NULL,
    kind        TEXT NOT NULL,
    body        TEXT NOT NULL,
    refs        TEXT,
    expires_at  INTEGER NOT NULL,
    recipient   TEXT
);
CREATE INDEX IF NOT EXISTS messages_topic_seq ON messages(topic, seq);
CREATE INDEX IF NOT EXISTS messages_expires  ON messages(expires_at);

CREATE TABLE IF NOT EXISTS cursors (
    agent_id    TEXT NOT NULL,
    topic       TEXT NOT NULL,
    last_seq    INTEGER NOT NULL,
    PRIMARY KEY (agent_id, topic)
);

CREATE TABLE IF NOT EXISTS claims (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    target      TEXT NOT NULL,
    holder      TEXT NOT NULL,
    ts          INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL,
    active      INTEGER NOT NULL DEFAULT 0
);
-- The load-bearing invariant: at most one active claim per target.
CREATE UNIQUE INDEX IF NOT EXISTS claims_one_active ON claims(target) WHERE active = 1;
CREATE INDEX IF NOT EXISTS claims_expires ON claims(expires_at);

CREATE TABLE IF NOT EXISTS tasks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    title       TEXT NOT NULL,
    body        TEXT,
    status      TEXT NOT NULL,
    priority    INTEGER NOT NULL DEFAULT 0,
    labels      TEXT,
    assignee    TEXT,
    created_by  TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    refs        TEXT,
    -- JSON array of task ids this task depends on; NULL = no deps. Claim auto-pick + explicit
    -- --task-id both gate on every listed dep being status='done'.
    -- Validated at create-time, so reads never fault on bad JSON.
    depends_on  TEXT,
    -- Sticky-reopen window (issue #10): set on a `changes`-driven reopen; while sticky_until
    -- > now, only `assignee` may claim. NULL = no window. Eligibility filter only — does not
    -- change priority. Cleared by release/cancel/successful sticky-claim.
    sticky_until INTEGER,
    -- Review-task only: the original executor whose `done` spawned this review (issue #10).
    -- The claim path filters review tasks where orig == caller so an agent cannot review its
    -- own work — "no self-review" as mechanism, not policy. NULL on non-review tasks.
    orig         TEXT,
    -- v20 lifecycle columns: durable identity + rework tracking for single-task model.
    author       TEXT,
    reviewer     TEXT,
    rework_round INTEGER NOT NULL DEFAULT 0,
    review_only  INTEGER NOT NULL DEFAULT 0,
    -- v29: durable crash-recovery budget. Incremented each time a managed worker
    -- dies and the task reopens; reset when the task reaches a meaningful lifecycle
    -- handoff (in-review via submit/rework-push). Cancels the task when exhausted.
    recovery_attempts INTEGER NOT NULL DEFAULT 0,
    -- v35: optimistic concurrency authority for edits and planning input. The v38
    -- repair migration adds these columns to split-lineage databases that missed them.
    revision     INTEGER NOT NULL DEFAULT 1,
    edit_count   INTEGER NOT NULL DEFAULT 0,
    -- v35: authoritative existing-PR implementation intent. Unlike refs.pr, this field is
    -- creator-selected only through --continue-pr and controls daemon provisioning.
    continue_pr INTEGER CHECK (continue_pr IS NULL OR continue_pr > 0)
);
CREATE INDEX IF NOT EXISTS tasks_status_priority ON tasks(status, priority DESC);
-- Bounded REVIEWING projection: each status is read newest-first, then at most two
-- REVIEWING_TASK_LIMIT-sized candidate sets are merged for global ordering.
CREATE INDEX IF NOT EXISTS tasks_reviewing_newest
    ON tasks(status, updated_at DESC, id DESC);
-- v36: durable candidate representation for corrupt terminal retry authority.
-- These partial indexes contain only rows reconciliation/status need to inspect,
-- so proving the healthy steady state does not scan the unbounded terminal task
-- history. Separate orderings keep both LIMIT queries bounded without sorting
-- the entire candidate set.
CREATE INDEX IF NOT EXISTS tasks_terminal_retry_id
    ON tasks(id)
    WHERE status IN ('done','failed','cancelled')
      AND json_valid(refs)
      AND (
          json_type(refs, '$.daemon_rework_retry_requested')='true'
          OR json_type(refs, '$.daemon_parked_head_check')='true'
          OR (
              status IN ('done','cancelled')
              AND (
                  json_type(refs, '$.daemon_parked') IS NOT NULL
                  OR json_type(refs, '$.daemon_resume_status') IS NOT NULL
              )
          )
          OR (
              status='failed'
              AND json_type(refs, '$.daemon_resume_status') IS NOT NULL
              AND COALESCE(json_extract(refs, '$.daemon_parked'), 0) != 1
          )
      );
CREATE INDEX IF NOT EXISTS tasks_terminal_retry_recent
    ON tasks(updated_at DESC, id DESC)
    WHERE status IN ('done','failed','cancelled')
      AND json_valid(refs)
      AND (
          json_type(refs, '$.daemon_rework_retry_requested')='true'
          OR json_type(refs, '$.daemon_parked_head_check')='true'
          OR (
              status IN ('done','cancelled')
              AND (
                  json_type(refs, '$.daemon_parked') IS NOT NULL
                  OR json_type(refs, '$.daemon_resume_status') IS NOT NULL
              )
          )
          OR (
              status='failed'
              AND json_type(refs, '$.daemon_resume_status') IS NOT NULL
              AND COALESCE(json_extract(refs, '$.daemon_parked'), 0) != 1
          )
      );

-- v35: one bounded decomposition aggregate per source task. `active` and
-- `freeze_active` are explicit partial-index sentinels, never inferred from
-- text state, so racing writers lose at SQLite's uniqueness boundary.
CREATE TABLE IF NOT EXISTS task_decompositions (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    source_task_id          INTEGER NOT NULL UNIQUE REFERENCES tasks(id),
    state                   TEXT NOT NULL CHECK(state IN (
                                'freeze-requested','draining','planning','validating',
                                'preclassifying','provider-backoff','held','active',
                                'blocked','completed','cancelled')),
    active                  INTEGER NOT NULL DEFAULT 0 CHECK(active IN (0,1)),
    freeze_active           INTEGER NOT NULL DEFAULT 0 CHECK(freeze_active IN (0,1)),
    planned_source_revision INTEGER NOT NULL,
    plan_revision           INTEGER NOT NULL DEFAULT 1,
    proposal_attempts       INTEGER NOT NULL DEFAULT 0,
    provider_failures       INTEGER NOT NULL DEFAULT 0,
    planner_provider        TEXT,
    planner_model           TEXT,
    planner_session_id      TEXT,
    frozen_base_sha         TEXT,
    accepted_proposal_json  TEXT CHECK(accepted_proposal_json IS NULL OR length(CAST(accepted_proposal_json AS BLOB)) <= 65536),
    accepted_plan_revision  INTEGER,
    hold_code               TEXT,
    hold_summary            TEXT,
    created_at              INTEGER NOT NULL,
    updated_at              INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_task_graph
    ON task_decompositions(active) WHERE active = 1;
CREATE UNIQUE INDEX IF NOT EXISTS one_planning_freeze
    ON task_decompositions(freeze_active) WHERE freeze_active = 1;

-- v37: short-lived daemon reviewer reservations serialize external reviewer
-- provisioning against acquisition of the repository planning freeze.
CREATE TABLE IF NOT EXISTS reviewer_provision_reservations (
    task_id       INTEGER PRIMARY KEY REFERENCES tasks(id),
    token         TEXT NOT NULL UNIQUE,
    role          TEXT NOT NULL CHECK(role IN ('r1','r2')),
    created_at    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS task_graph_members (
    graph_id      INTEGER NOT NULL REFERENCES task_decompositions(id),
    task_id       INTEGER NOT NULL UNIQUE REFERENCES tasks(id),
    local_key     TEXT NOT NULL,
    plan_revision INTEGER NOT NULL,
    active        INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    PRIMARY KEY (graph_id, plan_revision, local_key)
);
CREATE INDEX IF NOT EXISTS task_graph_members_graph_active
    ON task_graph_members(graph_id, active);

CREATE TABLE IF NOT EXISTS decomposition_attempts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    graph_id         INTEGER NOT NULL REFERENCES task_decompositions(id),
    source_revision  INTEGER NOT NULL,
    kind             TEXT NOT NULL CHECK(kind IN ('proposal','provider','blocker','recovery')),
    ordinal          INTEGER NOT NULL,
    reason_code      TEXT NOT NULL,
    summary          TEXT NOT NULL,
    created_at       INTEGER NOT NULL,
    UNIQUE (graph_id, source_revision, kind, ordinal)
);

CREATE TABLE IF NOT EXISTS decomposition_cleanup (
    graph_id       INTEGER NOT NULL REFERENCES task_decompositions(id),
    task_id        INTEGER NOT NULL REFERENCES tasks(id),
    artifact_kind  TEXT NOT NULL,
    artifact_ref   TEXT NOT NULL,
    state          TEXT NOT NULL DEFAULT 'pending'
                       CHECK(state IN ('pending','running','done','exhausted')),
    attempts       INTEGER NOT NULL DEFAULT 0,
    last_error     TEXT,
    updated_at     INTEGER NOT NULL,
    PRIMARY KEY (graph_id, task_id, artifact_kind, artifact_ref)
);

CREATE TABLE IF NOT EXISTS errors (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          INTEGER NOT NULL,
    source      TEXT NOT NULL,
    detail      TEXT NOT NULL,
    expires_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS errors_expires ON errors(expires_at);

-- Automatic state-change events. Distinct from `messages` so the agent-to-agent feed isn't
-- drowned by routine state ticks. Auto-emitted from inside each mutator's transaction so an
-- event never disagrees with the state it describes (Atomic > all). TTL'd like everything
-- else; not part of the messaging channel — read via `quorum log`.
CREATE TABLE IF NOT EXISTS events (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          INTEGER NOT NULL,
    kind        TEXT NOT NULL,
    subject     TEXT NOT NULL,
    body        TEXT NOT NULL,
    expires_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS events_subject_seq ON events(subject, seq);
CREATE INDEX IF NOT EXISTS events_expires    ON events(expires_at);

-- Append-only breadcrumbs attached to a task. No edit/delete in v1 (read-only history).
-- Sweeper does NOT TTL these — they're durable context for the next picker-upper after the
-- assignee is lost. Ordered by `id` (= monotonic insertion order); ts/agent are reported.
CREATE TABLE IF NOT EXISTS task_notes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     INTEGER NOT NULL,
    ts          INTEGER NOT NULL,
    agent       TEXT NOT NULL,
    body        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS task_notes_task_id ON task_notes(task_id, id);

-- Non-expiring control state — the emergency stop primitive (issue #6). One row per scope:
-- `global` (every agent halts) or `agent:<id>` (only that agent). Both can coexist; an agent
-- is stopped if either applies to it. **No expires_at column** — stops live until someone
-- explicitly `resume`s them, by design. The sweeper does NOT touch this table.
CREATE TABLE IF NOT EXISTS control (
    scope   TEXT PRIMARY KEY,
    reason  TEXT NOT NULL,
    by      TEXT NOT NULL,
    since   INTEGER NOT NULL
);

-- Non-expiring pinned notices (issue #78) — durable standing state every agent sees on
-- every sync, regardless of cursor or TTL. Parallels `control`: no `expires_at` column,
-- sweep does NOT touch this table. Removal is explicit via `unpin`.
CREATE TABLE IF NOT EXISTS pinned (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts      INTEGER NOT NULL,
    author  TEXT NOT NULL,
    body    TEXT NOT NULL
);

-- Per-task branch allocations (issue #98). One row = the recommended branch +
-- worktree path for an agent claiming this task. Lifetime matches the task itself:
-- persists across release/reopen so a rework re-claim returns the SAME branch
-- (no reconstruction, no guessing).
--
-- UNIQUE(task_id): one allocation per task — caller will INSERT OR IGNORE then
-- SELECT, so a fresh task allocates and a re-claim reuses without a TOCTOU race.
-- UNIQUE(branch): centralized anti-collision — quorum is the single registry of
-- in-use names, so two tasks can never share a branch even if their titles slugify
-- identically.
--
-- Sweeper does NOT TTL these (no expires_at) — same posture as task_notes:
-- durable context for the next picker-upper after the original assignee is lost.
CREATE TABLE IF NOT EXISTS task_branches (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id      INTEGER NOT NULL UNIQUE,
    branch       TEXT NOT NULL UNIQUE,
    worktree     TEXT NOT NULL,
    allocated_by TEXT NOT NULL,
    allocated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS task_branches_task ON task_branches(task_id);

-- Optional Claude Code PostToolUse activity hook (issue #101) — EXPERIMENTAL,
-- stats-only, opt-in. MUST NOT affect any existing workflow (claim-based
-- presence, sign-off, retirement load-score remain authoritative).
--
-- `agent_sessions`: bridge Claude session UUIDs to the agent's creative name.
-- Written by `quorum session-register` at hub-onboard time; read by
-- `quorum activity` to resolve `session_id → agent_name` before recording.
-- TTL'd: a stale session row falls out of the read-filter, fail-open
-- (activity record stores session_id but `agent_name=NULL` → counted as
-- "unresolved" in the stats surface).
CREATE TABLE IF NOT EXISTS agent_sessions (
    session_id    TEXT PRIMARY KEY,
    agent_name    TEXT NOT NULL,
    registered_at INTEGER NOT NULL,
    expires_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS agent_sessions_expires ON agent_sessions(expires_at);

-- Daemon mailbox (§12 IPC): agent-pushed control events consumed by the daemon's
-- tick loop. Each CLI invocation of `quorum submit/task-update/message` writes one row;
-- the daemon polls `consumed_at IS NULL` each tick and marks rows consumed after acting.
-- Not TTL'd — consumed rows are GC'd by sweep (bounded by `consumed_at IS NOT NULL`).
CREATE TABLE IF NOT EXISTS mailbox (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    agent       TEXT NOT NULL,
    kind        TEXT NOT NULL,
    task_id     INTEGER,
    pr          INTEGER,
    verdict     TEXT,
    feedback    TEXT,
    note        TEXT,
    to_agent    TEXT,
    payload     TEXT,
    created_at  INTEGER NOT NULL,
    consumed_at INTEGER
);
CREATE INDEX IF NOT EXISTS mailbox_unconsumed ON mailbox(consumed_at) WHERE consumed_at IS NULL;

-- Daemon journal: one row per in-flight agent (worker or reviewer). The daemon upserts
-- on every lifecycle transition so a restart can resurrect agents via `--resume`. Keyed
-- by agent name (one process per name at any time). Deleted on terminal transitions.
CREATE TABLE IF NOT EXISTS journal (
    agent           TEXT PRIMARY KEY,
    role            TEXT NOT NULL,
    task_id         INTEGER,
    session_id      TEXT NOT NULL,
    worktree        TEXT,
    branch          TEXT,
    phase           TEXT NOT NULL,
    expected_signal TEXT,
    cost_tokens     INTEGER NOT NULL DEFAULT 0,
    -- M5 agent state reaction: 'blocked', 'failed', 'needs-info', or free-text note.
    -- NULL = normal (no reaction signaled). Set by the daemon when processing a
    -- task_update mailbox row; cleared on terminal transitions.
    agent_state     TEXT,
    -- M6 logging: cumulative USD cost (session-level high-water mark from stream-json).
    cost_usd        REAL NOT NULL DEFAULT 0.0,
    -- M6 logging: filesystem path to this agent's session log directory.
    log_dir         TEXT,
    -- M7 crash recovery: process group ID for stale-process cleanup on restart.
    pid             INTEGER,
    -- M7 crash recovery: PR number (workers in awaiting-review phase).
    pr              INTEGER,
    -- M7 crash recovery: rework round counter for limit enforcement across restarts.
    rework_count    INTEGER NOT NULL DEFAULT 0,
    updated_at      INTEGER NOT NULL
);

-- #228 durable approval record: an attested review verdict persisted against the
-- PR/task. A `--self-update-drain` restart that lands between approval and merge
-- must be able to reconstruct "merge this PR" from durable state alone. Recovery
-- merges iff both r1 and r2 records are well-formed attested approvals for the
-- same head SHA, reviewer != author, and the SHA still matches the PR's live head.
-- Keyed by (pr_number, review_role): one verdict per reviewer role per PR.
-- Deleted on merge / demote / reject.
CREATE TABLE IF NOT EXISTS approvals (
    pr_number         INTEGER NOT NULL,
    review_role       TEXT NOT NULL DEFAULT 'r1',
    task_id           INTEGER NOT NULL,
    author            TEXT NOT NULL,
    reviewer          TEXT NOT NULL,
    verdict           TEXT NOT NULL,
    blocking_count    INTEGER NOT NULL,
    approved_head_sha TEXT NOT NULL,
    created_at        INTEGER NOT NULL,
    PRIMARY KEY (pr_number, review_role)
);
CREATE INDEX IF NOT EXISTS approvals_task ON approvals(task_id);

-- Durable reviewer-provision attempt tracking (#190). Bounded retry budget
-- per (task, PR, role). A new head_sha resets the counter (different diff =
-- fresh budget). Survives daemon restarts unlike the in-memory tracker.
CREATE TABLE IF NOT EXISTS reviewer_provision_attempts (
    task_id         INTEGER NOT NULL,
    pr_number       INTEGER NOT NULL,
    role            TEXT NOT NULL,
    head_sha        TEXT NOT NULL DEFAULT '',
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_attempt_at INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (task_id, pr_number, role)
);

-- Single-daemon-per-DB guard. At most one row (id=1). The daemon acquires this
-- on startup, refreshes heartbeat_at on every tick, and deletes on clean shutdown.
-- A second daemon checks this row to decide whether to take over (stale/dead) or exit.
CREATE TABLE IF NOT EXISTS daemon_lock (
    id            INTEGER PRIMARY KEY CHECK (id = 1),
    pid           INTEGER NOT NULL,
    heartbeat_at  INTEGER NOT NULL
);

-- Agent-performance capture: one row per daemon-spawned agent process (worker
-- or reviewer). Records the RESOLVED model+effort (after label→model mapping +
-- daemon defaults) so query surfaces can cut by what actually ran, not what was
-- requested. Rows are opened at spawn and closed at teardown/terminal.
-- Not TTL'd — durable historical data for performance analysis.
CREATE TABLE IF NOT EXISTS agent_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     INTEGER NOT NULL,
    agent_name  TEXT NOT NULL,
    role        TEXT NOT NULL CHECK(role IN ('worker','reviewer')),
    model       TEXT NOT NULL,
    effort      TEXT NOT NULL,
    spawned_at  INTEGER NOT NULL,
    ended_at    INTEGER,
    end_reason  TEXT,
    -- v23: NULL = normal R1/worker run. 'r2' = R2 adversarial audit pass.
    -- Perf queries filter `sub_role IS NULL` to exclude R2 from R1 stats.
    sub_role    TEXT,
    -- v31: resolved provider for this run ('claude' or 'codex'). NULL for
    -- pre-existing rows (implies Claude). Recovery must use this, not re-resolve.
    provider    TEXT
);
CREATE INDEX IF NOT EXISTS agent_runs_task ON agent_runs(task_id);

-- `activity_events`: one row per Claude PostToolUse hook firing. `agent_name`
-- is resolved at insert time (NULL if the session isn't registered).
-- Stats-only; never read by claim/routing/sign-off code paths. TTL'd.
CREATE TABLE IF NOT EXISTS activity_events (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          INTEGER NOT NULL,
    session_id  TEXT NOT NULL,
    agent_name  TEXT,
    tool        TEXT NOT NULL,
    expires_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS activity_events_agent_ts ON activity_events(agent_name, ts DESC);
CREATE INDEX IF NOT EXISTS activity_events_expires  ON activity_events(expires_at);

-- Durable PR target persistence (#201). Stores the authoritative PR target
-- tuple (head ref, head SHA, fork status) from `gh pr view` so remediation
-- and recovery can reuse it when GitHub is temporarily unavailable. Without
-- this, review-only tasks (where orphan_worker_branch returns None) are
-- stranded on GitHub outages. Keyed by task_id (one target per task); the
-- pr_number is stored for validation (a persisted target for a different PR
-- is stale). Not TTL'd — durable like task_branches.
CREATE TABLE IF NOT EXISTS pr_targets (
    task_id     INTEGER PRIMARY KEY,
    pr_number   INTEGER NOT NULL,
    head_ref    TEXT NOT NULL,
    head_sha    TEXT NOT NULL,
    is_fork     INTEGER NOT NULL DEFAULT 0,
    resolved_at INTEGER NOT NULL
);

-- Review-comment interpreter output (#93): structured findings parsed from PR
-- comment threads by a cheap Haiku pass. Captures blocking findings, non-blocking
-- suggestions, and author rebuttals that would otherwise be lost as free text.
-- Not TTL'd — durable historical data for review quality analysis.
CREATE TABLE IF NOT EXISTS review_findings (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    pr_number           INTEGER NOT NULL,
    task_id             INTEGER,
    reviewer            TEXT NOT NULL,
    kind                TEXT NOT NULL CHECK(kind IN ('blocking', 'suggestion')),
    author_pushback     INTEGER NOT NULL DEFAULT 0,
    pushback_accepted   INTEGER,
    severity            TEXT,
    text                TEXT NOT NULL,
    source_endpoint     TEXT NOT NULL CHECK(source_endpoint IN ('pulls', 'issues')),
    created_at          INTEGER NOT NULL,
    -- v24 collector fields (#125). Nullable so pre-v24 rows keep round-tripping.
    -- addressed_status: 'addressed' | 'unaddressed' | 'partial' | 'unclear'.
    -- Distinguishes "the author fixed this before merge" from a merged-anyway
    -- blocking find (which is a review-quality signal, not a lifecycle problem).
    addressed_status    TEXT,
    -- JSON array of {"kind":"review"|"review_comment"|"issue_comment","id":<gh_id>}
    -- tying the classification to concrete GitHub records — no prose-only findings.
    evidence_ids        TEXT,
    -- Provenance so re-interpretation replaces old rows cleanly, and analytics can
    -- filter by which collector generation produced them.
    collector_model     TEXT,
    collector_version   TEXT
);
CREATE INDEX IF NOT EXISTS review_findings_pr ON review_findings(pr_number);
CREATE INDEX IF NOT EXISTS review_findings_task ON review_findings(task_id);

-- Post-merge collector run record (#125). One row per PR — a retry replaces
-- the prior row via UPSERT so re-running is idempotent. Failed rows are the
-- observable surface for collector failures (they must never touch the merged
-- task's lifecycle); the manual `quorum review-interpret` command retries by
-- overwriting this row plus the findings for the PR. Not TTL'd — durable audit.
CREATE TABLE IF NOT EXISTS review_collection_runs (
    pr_number           INTEGER PRIMARY KEY,
    task_id             INTEGER,
    status              TEXT NOT NULL CHECK(status IN ('success', 'failed')),
    error               TEXT,
    collector_model     TEXT NOT NULL,
    collector_version   TEXT NOT NULL,
    findings_count      INTEGER NOT NULL DEFAULT 0,
    attempted_at        INTEGER NOT NULL,
    completed_at        INTEGER
);
CREATE INDEX IF NOT EXISTS review_collection_runs_task ON review_collection_runs(task_id);

-- v24 (#127): durable post-merge collector retry queue. Each row is a pending
-- collector invocation — the daemon enqueues one at MergeSucceeded and drains
-- one job per tick with a bounded retry budget (attempts cap + linear backoff).
-- Deleted on success. Historical terminal tasks are NOT backfilled (#157).
-- Complements `review_collection_runs` (an audit record per PR) by providing
-- durable retry state that survives daemon crashes; failure is observable via
-- `last_error` but never mutates the completed task's `done` state.
CREATE TABLE IF NOT EXISTS review_interpret_jobs (
    pr_number           INTEGER PRIMARY KEY,
    task_id             INTEGER NOT NULL,
    repo                TEXT,
    interpreter_version TEXT NOT NULL,
    attempts            INTEGER NOT NULL DEFAULT 0,
    last_attempt_at     INTEGER,
    last_error          TEXT,
    created_at          INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS review_interpret_jobs_task ON review_interpret_jobs(task_id);

-- R2 review-audit capture (#92): one row per adversarial pre-merge second
-- reviewer (R2) pass on an R1-approved PR. Stratified-sampled on
-- (model, effort, cx_bucket). R2 replaces R1 as the pre-merge gate — its
-- verdict drives lifecycle (approve → merge, changes → rework). The audit
-- row is a durable stratum-coverage record, not a shadow ledger. Not TTL'd.
CREATE TABLE IF NOT EXISTS review_audits (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id           INTEGER NOT NULL,
    pr_number         INTEGER NOT NULL,
    r1_run_id         INTEGER NOT NULL,
    r2_run_id         INTEGER NOT NULL,
    r1_reviewer       TEXT NOT NULL,
    r2_reviewer       TEXT NOT NULL,
    model             TEXT NOT NULL,
    effort            TEXT NOT NULL,
    cx_bucket         TEXT NOT NULL,
    missed_count      INTEGER NOT NULL DEFAULT 0,
    overcaught_count  INTEGER NOT NULL DEFAULT 0,
    r1_verdict        TEXT NOT NULL,
    r2_verdict        TEXT,
    created_at        INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS review_audits_task ON review_audits(task_id);
CREATE INDEX IF NOT EXISTS review_audits_stratum ON review_audits(model, effort, cx_bucket);

-- Daemon-owned R2 sampling decisions.  Task refs are agent-writable metadata,
-- so they must never authorize bypassing the pre-merge R2 gate.  A decision is
-- immutable for one PR head and survives rework/restart without being exposed
-- through the task-update surface.
CREATE TABLE IF NOT EXISTS r2_sampling_decisions (
    pr_number  INTEGER NOT NULL,
    head_sha   TEXT NOT NULL,
    task_id    INTEGER NOT NULL,
    required   INTEGER NOT NULL CHECK(required IN (0, 1)),
    created_at INTEGER NOT NULL,
    PRIMARY KEY (pr_number, head_sha)
);
CREATE INDEX IF NOT EXISTS r2_sampling_decisions_task ON r2_sampling_decisions(task_id);

-- Run-targeted task messages (#131): durable, non-interrupting messages scoped
-- to a task. Each send creates one message row plus per-recipient delivery rows
-- keyed by agent_runs.id (run ID), never reusable agent name. Messages are
-- TTL'd (swept like the broadcast feed); delivery rows are durable history.
CREATE TABLE IF NOT EXISTS task_messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         INTEGER NOT NULL,
    sender_run_id   INTEGER NOT NULL,
    sender_agent    TEXT NOT NULL,
    kind            TEXT NOT NULL CHECK(kind IN ('direct', 'broadcast')),
    target_run_id   INTEGER,
    body            TEXT NOT NULL,
    recipient_count INTEGER NOT NULL DEFAULT 0,
    created_at      INTEGER NOT NULL,
    expires_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS task_messages_task ON task_messages(task_id);
CREATE INDEX IF NOT EXISTS task_messages_expires ON task_messages(expires_at);

-- Per-recipient delivery tracking keyed by run ID. Immutable once terminal
-- (delivered/undeliverable/expired). Not TTL'd — durable inspectable history.
-- Absence vs expiry is distinguishable: no row = not in audience; row with
-- status='expired' = was in audience but message expired before delivery.
CREATE TABLE IF NOT EXISTS task_message_deliveries (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id        INTEGER NOT NULL,
    recipient_run_id  INTEGER NOT NULL,
    recipient_agent   TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'queued'
                      CHECK(status IN ('queued', 'delivered', 'undeliverable', 'expired')),
    attempts          INTEGER NOT NULL DEFAULT 0,
    last_attempt_at   INTEGER,
    last_error        TEXT,
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS task_message_deliveries_msg ON task_message_deliveries(message_id);
CREATE INDEX IF NOT EXISTS task_message_deliveries_run ON task_message_deliveries(recipient_run_id, status);

-- v26: daemon-issued run capabilities (#130) — per-run identity binding for
-- managed submit/report operations. The daemon creates a row at worker/reviewer
-- spawn; submit validates run_id before processing. Revoked on agent death or
-- task terminal transition. Not TTL'd — durable audit trail.
CREATE TABLE IF NOT EXISTS run_capabilities (
    run_id      TEXT PRIMARY KEY,
    task_id     INTEGER NOT NULL,
    agent       TEXT NOT NULL,
    role        TEXT NOT NULL CHECK(role IN ('worker','reviewer')),
    created_at  INTEGER NOT NULL,
    revoked_at  INTEGER
);
CREATE INDEX IF NOT EXISTS run_capabilities_agent ON run_capabilities(agent) WHERE revoked_at IS NULL;

-- v27: prospective-only boundary for PR-interaction performance analytics
-- (#158). Single row (id=1) recording the unix timestamp at which
-- `quorum perf` results become eligible. Tasks with `updated_at` before
-- this watermark are excluded from the default performance report.
-- Set once during the v27 migration and never updated — it marks the
-- point of feature rollout and survives daemon restarts / binary upgrades.
-- `quorum perf --all` bypasses the watermark for historical analysis.
CREATE TABLE IF NOT EXISTS perf_watermark (
    id        INTEGER PRIMARY KEY CHECK (id = 1),
    watermark INTEGER NOT NULL
);
