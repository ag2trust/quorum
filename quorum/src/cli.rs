//! Command-line surface (clap). clap handles `--help`/usage errors itself, exiting 2 —
//! which matches our usage-error exit code.

use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

/// CLI mirror of [`quorum_core::sync::SyncScope`]. Derives `ValueEnum` so clap parses
/// `--scope minimal|coordinator` from the command line. Defaults to `Minimal` so a
/// worker that just types `quorum sync --agent X` gets the cheap-by-default bucket
/// (#94 acceptance — "default for workers").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum SyncScopeArg {
    /// Workers (default). Critical broadcasts limited to `system=1` only.
    #[default]
    Minimal,
    /// Coordinators (opt-in). All critical broadcasts delivered in full.
    Coordinator,
}

impl From<SyncScopeArg> for quorum_core::sync::SyncScope {
    fn from(a: SyncScopeArg) -> Self {
        match a {
            SyncScopeArg::Minimal => quorum_core::sync::SyncScope::Minimal,
            SyncScopeArg::Coordinator => quorum_core::sync::SyncScope::Coordinator,
        }
    }
}

#[derive(Parser)]
#[command(
    name = "quorum",
    version,
    about = "Local agent coordination (by agents, for agents)",
    // We define our own `help` subcommand below (the agent cheat-sheet, recovery-safe).
    // Without this, clap auto-generates a generic `help` that would collide with ours.
    disable_help_subcommand = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Create ~/.quorum/, the database, and run migrations (idempotent).
    Init,
    /// Wipe ALL state — drop the database and recreate a clean schema. Requires `--yes`.
    Reset {
        /// Confirm the destructive wipe. Without it, `reset` refuses (exit 2) — no accidental wipe.
        #[arg(long)]
        yes: bool,
    },
    /// Create a new open task. Body (free text) via --body-stdin or --body-file.
    TaskCreate {
        #[arg(long = "created-by")]
        created_by: String,
        #[arg(long)]
        title: String,
        #[arg(long)]
        priority: Option<i64>,
        /// JSON array of labels, e.g. '["ui","p1"]'.
        #[arg(long)]
        labels: Option<String>,
        /// JSON of external refs, e.g. '{"pr":2459}'.
        #[arg(long)]
        refs: Option<String>,
        /// JSON array of task ids this task depends on, e.g. '[1,3]'. Claim (auto-pick AND
        /// explicit --task-id) skips this task until every listed dep is `closed` (reviewed
        /// + finalized). Validated as a JSON array of ints at create — malformed exits 2.
        #[arg(long = "depends-on")]
        depends_on: Option<String>,
        #[arg(long = "body-stdin")]
        body_stdin: bool,
        #[arg(long = "body-file")]
        body_file: Option<PathBuf>,
    },
    /// Atomically claim a task (a specific --task-id, or the highest-priority open task), taking
    /// a renewable lease. A lapsed lease returns the task to `open` (reaper).
    ///
    /// `--match-label <L>` (repeatable, AND) restricts the auto-pick to tasks whose `labels`
    /// contain every supplied label — useful for capability/tier matching. Mutually exclusive
    /// with `--task-id` (an explicit id is already a more specific selector).
    TaskClaim {
        #[arg(long)]
        agent: String,
        #[arg(long = "task-id", conflicts_with = "match_label")]
        task_id: Option<i64>,
        /// Restrict the auto-pick to tasks whose labels contain this label. Repeatable = AND.
        #[arg(long = "match-label")]
        match_label: Vec<String>,
        /// Lease duration, e.g. 45m, 1h, 30s, or bare seconds. Defaults to the config lease TTL.
        #[arg(long)]
        ttl: Option<String>,
    },
    /// Update a task: transition status, set refs/body, or append a note. The single
    /// task-transition command — replaces the former `task-release` and `task-cancel`.
    ///
    /// Valid `--status` values and their guards:
    ///   `done`      — assignee-only, from `claimed`. Auto-spawns a review task.
    ///   `open`      — assignee-only, from `claimed` (release/give-up semantics).
    ///   `cancelled` — creator OR assignee, from non-terminal (won't-do).
    ///   Omitted     — metadata-only update (body/refs/note), assignee guard.
    ///
    /// `--note-stdin`/`--note-file` appends a breadcrumb to the task's note history. Notes
    /// have **no assignee guard** (any agent can leave one) and can be combined with the
    /// other field updates in the same call.
    ///
    /// `--verdict approve|changes` (issue #10) is the reviewer's decision when marking a
    /// `kind:review` task `done`. **Required** on review tasks; **forbidden** on non-review
    /// tasks. `approve` chains the original task to `closed`; `changes` reopens the original
    /// with the `rework` label and a sticky window (only the original assignee may claim
    /// during the window, then anyone).
    TaskUpdate {
        #[arg(long)]
        agent: String,
        #[arg(long = "task-id")]
        task_id: i64,
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        refs: Option<String>,
        #[arg(long = "body-stdin", conflicts_with = "note_stdin")]
        body_stdin: bool,
        #[arg(long = "body-file")]
        body_file: Option<PathBuf>,
        /// Read a free-text note from stdin and append it to the task's history.
        #[arg(long = "note-stdin")]
        note_stdin: bool,
        /// Read a free-text note from a file and append it to the task's history.
        #[arg(long = "note-file")]
        note_file: Option<PathBuf>,
        /// Reviewer's verdict on a `kind:review` task being marked done. One of: approve,
        /// changes. Required on review tasks, rejected on non-review.
        #[arg(long)]
        verdict: Option<String>,
    },
    /// List tasks, optionally filtered by status/label/assignee. `--brief` returns summary rows
    /// (no body) for a token-cheap queue scan; the full body is one `task-get <id>` away.
    TaskList {
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        label: Option<String>,
        #[arg(long)]
        assignee: Option<String>,
        /// Summary rows only — id, title, labels, priority, status, assignee, ready (omits body).
        #[arg(long)]
        brief: bool,
    },
    /// Fetch a single task by id.
    TaskGet {
        #[arg(long = "task-id")]
        task_id: i64,
    },
    /// Post a message to the feed. Body (free text) via --body-stdin or --body-file.
    /// `--to <agent>` marks it as a direct message to that agent; omitted = broadcast.
    Post {
        #[arg(long)]
        agent: String,
        /// One of: info, request, claim, done, hello, critical.
        #[arg(long)]
        kind: String,
        #[arg(long)]
        topic: Option<String>,
        /// Direct-message recipient. Omit for a broadcast.
        #[arg(long = "to")]
        to: Option<String>,
        /// Message TTL, e.g. 48h, 2h, 30m. Defaults to 48h.
        #[arg(long)]
        ttl: Option<String>,
        /// JSON of external refs.
        #[arg(long)]
        refs: Option<String>,
        #[arg(long = "body-stdin")]
        body_stdin: bool,
        #[arg(long = "body-file")]
        body_file: Option<PathBuf>,
        /// Mark this message as "system must-see" so workers on `sync --scope minimal`
        /// receive its full body (#94). Orthogonal to `--kind critical`: a typical
        /// `quorum stop` / master-CI-red broadcast pairs `--kind critical` with this
        /// flag. Without it, a `--kind critical` broadcast is delivered only to
        /// `--scope coordinator` syncs in full; workers see it via the broadcast count.
        #[arg(long = "system-critical")]
        system_critical: bool,
    },
    /// Read messages from the feed. With `--agent`, returns new messages since your cursor
    /// (broadcasts + direct-to-you); `--ack-through` advances the cursor. Without `--agent`,
    /// returns unexpired messages without touching any cursor (replaces the former `peek`
    /// command). `--direct` / `--broadcasts` filter the feed; `--since` sets a seq floor
    /// for agent-less reads.
    Read {
        #[arg(long)]
        agent: Option<String>,
        #[arg(long)]
        topic: Option<String>,
        #[arg(long = "ack-through")]
        ack_through: Option<i64>,
        #[arg(long)]
        limit: Option<i64>,
        /// Show only direct-to-you messages (requires --agent).
        #[arg(long, conflicts_with = "broadcasts")]
        direct: bool,
        /// Show only broadcasts (no recipient).
        #[arg(long)]
        broadcasts: bool,
        /// Seq floor for agent-less reads: return messages with seq > since.
        #[arg(long)]
        since: Option<i64>,
    },
    /// Read the auto-emitted state-change event log (separate from the message feed).
    /// `--since <seq>` returns events strictly after a seq; `--refs <subject>` filters by
    /// the entity (e.g. `task#42`, `pr#2459`).
    Log {
        #[arg(long)]
        since: Option<i64>,
        #[arg(long = "refs")]
        refs: Option<String>,
        #[arg(long)]
        limit: Option<i64>,
    },
    /// Emergency stop — halt every agent (`global`, default) or a specific one
    /// (`--agent <id>`). Stopped agents are expected to do no work but keep cheap-polling
    /// for the matching `resume`. Reason via --reason-stdin or --reason-file (never a flag).
    /// Non-expiring: the row lives until you `quorum resume` it. Re-issuing on the same
    /// scope replaces reason+by+since (idempotent).
    Stop {
        /// Target a specific agent (omit for a global stop).
        #[arg(long)]
        agent: Option<String>,
        /// Who is issuing this stop.
        #[arg(long)]
        by: String,
        #[arg(long = "reason-stdin")]
        reason_stdin: bool,
        #[arg(long = "reason-file")]
        reason_file: Option<PathBuf>,
    },
    /// Clear a stop. Omit `--agent` to clear the global stop; pass `--agent <id>` to clear
    /// that targeted stop. Emits a `stop_cleared` event (subject = `global` or `agent:<id>`)
    /// so a halted agent's next poll learns the halt is over. Exit 1 if no stop was set on
    /// that scope (clean "didn't get it", not an error).
    Resume {
        #[arg(long)]
        agent: Option<String>,
        /// Who is clearing this stop.
        #[arg(long)]
        by: String,
    },
    /// List every active stop (global and per-agent). Read-only.
    Stops,
    /// Post a pinned standing notice (issue #78). Non-expiring, cursor-independent —
    /// surfaced in EVERY agent's `sync.pinned` until explicitly unpinned. Body (free
    /// text) via --body-stdin or --body-file.
    Pin {
        #[arg(long)]
        agent: String,
        #[arg(long = "body-stdin")]
        body_stdin: bool,
        #[arg(long = "body-file")]
        body_file: Option<PathBuf>,
    },
    /// Remove a pinned notice by id. Creator-only — `--agent` must match the original
    /// author. Exit 1 (clean "didn't") if the id doesn't exist or isn't yours.
    Unpin {
        #[arg(long)]
        agent: String,
        #[arg(long)]
        id: i64,
    },
    /// List every active pinned notice (oldest first). Read-only.
    Pins,
    /// Single-call agent tick — the "compass." Returns one JSON payload with everything the
    /// agent needs to orient: `current_task` (or `next_task` if idle), unread direct +
    /// critical messages, a broadcast `count` + critical bodies, and a scoped event log.
    /// State-adaptive XOR — `current_task` ⇔ `next_task`, never both. Omit-empty so a quiet
    /// tick is near-empty JSON.
    ///
    /// Auto-acks the message cursor as a side effect (use `read --ack-through` explicitly if
    /// you need strict at-least-once instead of at-most-once). `current_task`/`next_task`
    /// bodies are omitted — fetch once with `task-get`.
    ///
    /// `--match-label <L>` (repeatable, AND) restricts the `next_task` pick to tasks whose
    /// `labels` contain every supplied label — capability/tier matching. Does NOT affect
    /// `current_task` (you keep what you hold).
    Sync {
        #[arg(long)]
        agent: String,
        /// Restrict the auto-picked `next_task` to tasks whose labels contain this label.
        /// Repeatable = AND. Does not affect `current_task`.
        #[arg(long = "match-label")]
        match_label: Vec<String>,
        /// Subscription level (#94). `minimal` (default for workers) delivers only
        /// system-must-see critical broadcasts in full; the coordination critical
        /// chatter flows through `broadcasts.count` instead — zero body bytes per
        /// tick. `coordinator` (CTO / dispatchers) keeps the full critical feed.
        /// Directs + counts + scoped log are unaffected by scope.
        #[arg(long, value_enum, default_value_t = SyncScopeArg::default())]
        scope: SyncScopeArg,
    },
    /// Health snapshot. --json for machine output; --watch to refresh continuously.
    /// --agents lists known agents with derived online/offline presence (replaces the
    /// former `roster` command).
    Status {
        #[arg(long)]
        json: bool,
        #[arg(long)]
        watch: bool,
        /// List agent presence (online/offline). Replaces the former `roster` command.
        #[arg(long)]
        agents: bool,
    },
    /// Reclaim all expired rows and checkpoint the WAL.
    Sweep,
    /// EXPERIMENTAL (issue #101) — register a Claude session UUID → agent name
    /// for the optional PostToolUse activity hook. Stats-only; no workflow
    /// impact. Idempotent; re-register extends the session TTL (48h).
    SessionRegister {
        #[arg(long)]
        agent: String,
        #[arg(long)]
        session: String,
    },
    /// EXPERIMENTAL (issue #101) — record one tool-use event for the activity
    /// stats surface. Resolves `--session` → agent name via the
    /// `session-register` mapping; fail-open (unregistered session is still
    /// recorded with `agent_name = NULL` → counted as "unknown" in stats).
    /// Designed for `~/.claude/settings.json` PostToolUse hook invocation.
    Activity {
        #[arg(long)]
        session: String,
        #[arg(long)]
        tool: String,
    },
    /// Print a one-screen cheat-sheet of all commands (for agents to re-orient).
    /// `help-agent` is kept as a back-compat alias.
    #[command(name = "help", alias = "help-agent")]
    Help,
}
