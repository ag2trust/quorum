-- Quorum schema (SCHEMA_VERSION = 1). All statements idempotent (IF NOT EXISTS) so the
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
    refs        TEXT
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
