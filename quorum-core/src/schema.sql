-- Quorum schema (SCHEMA_VERSION = 6). All statements idempotent (IF NOT EXISTS) so the
-- migration is safe to run on every open. See docs/2026-06-23-quorum-design.md §Data model.

CREATE TABLE IF NOT EXISTS agents (
    id          TEXT PRIMARY KEY,
    first_seen  INTEGER NOT NULL,
    last_seen   INTEGER NOT NULL
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
