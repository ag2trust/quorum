-- Quorum schema (SCHEMA_VERSION = 12). All statements idempotent (IF NOT EXISTS) so the
-- migration is safe to run on every open. See docs/2026-06-23-quorum-design.md §Data model.

CREATE TABLE IF NOT EXISTS agents (
    id            TEXT PRIMARY KEY,
    first_seen    INTEGER NOT NULL,
    last_seen     INTEGER NOT NULL,
    tier          TEXT,
    -- Agent-retirement state machine (issue #97): 'active' | 'retiring' | 'retired'.
    -- Transitions are computed inside `sync::tick` from the load_score budget; once an
    -- agent reaches 'retired' it stays there for this DB's lifetime (a fresh session
    -- name is a fresh agent row).
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
    -- --task-id both gate on every listed dep being status='closed' (reviewed + finalized).
    -- Validated at create-time, so reads never fault on bad JSON.
    depends_on  TEXT,
    -- Sticky-reopen window (issue #10): set on a `changes`-driven reopen; while sticky_until
    -- > now, only `assignee` may claim. NULL = no window. Eligibility filter only — does not
    -- change priority. Cleared by release/cancel/successful sticky-claim.
    sticky_until INTEGER,
    -- Review-task only: the original executor whose `done` spawned this review (issue #10).
    -- The claim path filters review tasks where orig == caller so an agent cannot review its
    -- own work — "no self-review" as mechanism, not policy. NULL on non-review tasks.
    orig         TEXT
);
CREATE INDEX IF NOT EXISTS tasks_status_priority ON tasks(status, priority DESC);

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

-- Per-(task, project) branch allocations (issue #98). One row = the recommended
-- branch + worktree path for an agent claiming this task in this project. Lifetime
-- matches the task itself: persists across release/reopen so a rework re-claim
-- returns the SAME branch (no reconstruction, no guessing).
--
-- UNIQUE(task_id, repo): one allocation per (task, project) — caller will INSERT
-- OR IGNORE then SELECT, so a fresh task allocates and a re-claim reuses without
-- a TOCTOU race.
-- UNIQUE(repo, branch): centralized anti-collision — quorum is the single
-- registry of in-use names, so two tasks in the same project can never share a
-- branch even if their titles slugify identically.
--
-- Sweeper does NOT TTL these (no expires_at) — same posture as task_notes:
-- durable context for the next picker-upper after the original assignee is lost.
CREATE TABLE IF NOT EXISTS task_branches (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id      INTEGER NOT NULL,
    repo         TEXT NOT NULL,
    branch       TEXT NOT NULL,
    worktree     TEXT NOT NULL,
    allocated_by TEXT NOT NULL,
    allocated_at INTEGER NOT NULL,
    UNIQUE(task_id, repo),
    UNIQUE(repo, branch)
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
-- tick loop. Each CLI invocation of `quorum done/task-update/message` writes one row;
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
    updated_at      INTEGER NOT NULL
);

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
