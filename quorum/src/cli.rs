//! Command-line surface (clap). clap handles `--help`/usage errors itself, exiting 2 —
//! which matches our usage-error exit code.

use clap::{Parser, Subcommand};
use std::path::PathBuf;

const fn long_version() -> &'static str {
    concat!(
        env!("CARGO_PKG_VERSION"),
        " (",
        env!("QUORUM_GIT_SHA_SHORT"),
        ")",
    )
}

pub fn short_version() -> &'static str {
    env!("QUORUM_GIT_DESCRIBE")
}

#[derive(Parser)]
#[command(
    name = "quorum",
    version = long_version(),
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
#[allow(clippy::large_enum_variant)]
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
        /// Create as a review-only task (starts in in-review). The PR number is stored in refs.pr.
        #[arg(long = "review-pr")]
        review_pr: Option<i64>,
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
    ///   `done`      — assignee-only, from `claimed`. Sets terminal status only;
    ///                  review spawning happens via `quorum done`, not task-update.
    ///   `open`      — assignee-only, from `claimed` (release/give-up semantics).
    ///   `cancelled` — creator OR assignee, from non-terminal (won't-do).
    ///   Omitted     — metadata-only update (body/refs/note). Assignee guard if
    ///                  claimed; creator guard if unclaimed (open + no assignee).
    ///
    /// `--note-stdin`/`--note-file` appends a breadcrumb to the task's note history. Notes
    /// have **no assignee guard** (any agent can leave one) and can be combined with the
    /// other field updates in the same call.
    ///
    /// `--verdict approve|changes` is the reviewer's verdict on an in-review task.
    /// `approve` transitions to merging; `changes` transitions to rework.
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
        /// Reviewer's verdict on an in-review task. One of: approve, changes.
        #[arg(long)]
        verdict: Option<String>,
        /// Count of BLOCKING findings in your review (#206). `--verdict approve`
        /// requires `--blocking 0` — any blocking finding requires
        /// `--verdict changes`.
        #[arg(long)]
        blocking: Option<u32>,
        /// Replace this task's dependency list. JSON array of task ids, e.g. '[1,3]'.
        /// Pass '[]' to clear all dependencies. Creator or assignee guard; blocked on
        /// terminal tasks (done/failed).
        #[arg(long = "depends-on")]
        depends_on: Option<String>,
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
    /// Send a message to another daemon-managed agent. The daemon delivers
    /// the payload as a turn when the target agent is idle. Body (free text)
    /// via --body-stdin or --body-file.
    Message {
        /// Sender agent name.
        #[arg(long)]
        from: String,
        /// Target agent name.
        #[arg(long)]
        to: String,
        #[arg(long = "body-stdin")]
        body_stdin: bool,
        #[arg(long = "body-file")]
        body_file: Option<PathBuf>,
    },
    /// Signal a non-terminal agent state to the daemon. The daemon tracks
    /// the state and surfaces it via `quorum status`.
    React {
        #[arg(long)]
        agent: String,
        #[arg(long = "task-id")]
        task_id: i64,
        /// One of: blocked, failed, needs-info, note.
        #[arg(long)]
        state: String,
    },
    /// Signal task completion (worker) or emit a review verdict (reviewer).
    /// Writes a mailbox row for the daemon to consume.
    Done {
        #[arg(long)]
        agent: String,
        #[arg(long)]
        pr: Option<i64>,
        #[arg(long)]
        summary: Option<String>,
        /// Review verdict: approved or changes.
        #[arg(long)]
        verdict: Option<String>,
        /// Review feedback (required with --verdict changes — it becomes the
        /// worker's rework instructions).
        #[arg(long)]
        feedback: Option<String>,
        /// Count of BLOCKING findings in your review (#206). `--verdict approved`
        /// requires `--blocking 0` — any blocking finding requires
        /// `--verdict changes`.
        #[arg(long)]
        blocking: Option<u32>,
    },
    /// Launch the agent-manager daemon. Spawns and drives Claude Code agents as
    /// persistent stdin-fed processes, polls the mailbox, and shuts down on Ctrl-C.
    Serve {
        /// Path to a TOML config file. Default: ~/.quorum/serve/<owner>__<repo>.toml
        /// if present. CLI flags override config-file values.
        #[arg(long)]
        config: Option<String>,
        /// Maximum concurrent worker agents (default: 4).
        #[arg(long)]
        cap: Option<usize>,
        /// Path to the git repo the daemon manages.
        #[arg(long)]
        repo_dir: Option<String>,
        /// Base directory for agent worktrees.
        #[arg(long)]
        worktree_base: Option<String>,
        /// Path to the agent names file (one name per line).
        /// When absent, names are auto-generated.
        #[arg(long)]
        names_file: Option<String>,
        /// Override the agent binary (default: "claude").
        #[arg(long)]
        agent_bin: Option<String>,
        /// Model to pass to spawned agents (default: sonnet).
        #[arg(long)]
        model: Option<String>,
        /// Effort level to pass to spawned agents (default: high).
        #[arg(long)]
        effort: Option<String>,
        /// Path to a file containing a GitHub token for merging PRs.
        /// Read at merge time; never passed to agent processes.
        #[arg(long)]
        merge_token_file: Option<String>,
        /// Override the merge command (for testing). Replaces `{pr}` with the PR number.
        /// When set, bypasses `gh pr merge` and runs this shell command instead.
        #[arg(long, hide = true)]
        merge_cmd: Option<String>,
        /// Override the checks command (for testing). Replaces `{pr}` with the PR number.
        /// stdout first line: "ready"|"pending"|"failed"; if "failed", subsequent lines
        /// are failing check names.
        #[arg(long, hide = true)]
        merge_checks_cmd: Option<String>,
        /// Override the mergeability check command (for testing). Replaces `{pr}` with the
        /// PR number. stdout: "conflicting" => PR has conflicts; anything else => mergeable.
        #[arg(long, hide = true)]
        merge_mergeability_cmd: Option<String>,
        /// Seconds to wait for required status checks before merging (default: 900).
        #[arg(long)]
        merge_checks_timeout_secs: Option<u64>,
        /// Poll interval for status checks in seconds (default: 30).
        #[arg(long, hide = true)]
        merge_checks_poll_secs: Option<u64>,
        /// Disable --bare mode for spawned agents. By default, agents are
        /// spawned with --bare so they do not inherit operator-local hooks,
        /// plugins, memory, or MCP config. Pass this flag to let agents use
        /// the operator's full Claude config.
        #[arg(long)]
        no_bare_agent: bool,
        /// Max tokens (input+output) per single turn. Exceeding kills the agent.
        #[arg(long)]
        max_turn_tokens: Option<i64>,
        /// Max cumulative tokens (input+output) per task. Exceeding kills the agent.
        #[arg(long)]
        max_task_tokens: Option<i64>,
        /// Max USD cost per single turn (delta from session-cumulative total_cost_usd).
        #[arg(long)]
        max_turn_cost_usd: Option<f64>,
        /// Max cumulative USD cost per task.
        #[arg(long)]
        max_task_cost_usd: Option<f64>,
        /// Max wall-clock seconds per single turn.
        #[arg(long)]
        max_turn_wall_secs: Option<u64>,
        /// Max wall-clock seconds per task (across all turns).
        #[arg(long)]
        max_task_wall_secs: Option<u64>,
        /// Max seconds a worker/reviewer may sit idle between turns before
        /// the watchdog kills it (default: 300). Catches zombies.
        #[arg(long)]
        idle_timeout_secs: Option<u64>,
        /// Comma-separated tool allowlist for spawned agents (overrides built-in default).
        #[arg(long)]
        allowed_tools: Option<String>,
        /// Directory for per-agent session logs (stream.jsonl, transcript.md, meta.json).
        /// Defaults to {quorum_home}/logs when omitted.
        #[arg(long)]
        log_dir: Option<String>,
        /// Enable self-update drain mode. When the daemon merges a PR for its
        /// own repo (or detects the base branch advancing via git ls-remote), it drains
        /// in-flight agents and exits 75 so a supervisor can rebuild and relaunch.
        #[arg(long)]
        self_update_drain: bool,
        /// Seconds to wait for in-flight agents to finish before force-killing
        /// them during a drain (default: 900).
        #[arg(long)]
        drain_timeout_secs: Option<u64>,
        /// Owner/name of the daemon's own repo (e.g. "ag2trust/quorum").
        /// Default: derived from --repo-dir's origin remote via `gh repo view`.
        #[arg(long)]
        self_repo: Option<String>,
        /// Interval between git ls-remote sha polls in seconds (default: 60).
        #[arg(long, hide = true)]
        sha_poll_interval_secs: Option<u64>,
        /// Repo this daemon manages (e.g. "ag2trust/quorum"). Required
        /// (via flag or config file). The daemon opens this repo's per-repo DB
        /// and sets QUORUM_REPO for all spawned workers/reviewers.
        #[arg(long)]
        repo: Option<String>,
        /// Base branch name for sha-polling, worktree provisioning, and merge
        /// targeting. Use "master" for repos whose trunk is master (default: main).
        #[arg(long)]
        base_branch: Option<String>,
        /// Path to a sentinel file. When set, serve polls for this file's
        /// existence every tick and initiates shutdown when it disappears.
        /// Used by test fixtures to self-terminate when the parent test
        /// process dies and its tempdir is cleaned up.
        #[arg(long, hide = true)]
        exit_when_gone: Option<String>,
        /// Enable the doctor agent — a one-shot troubleshooter spawned for
        /// tasks stalled with no active worker/reviewer. Default: off.
        #[arg(long)]
        doctor_enabled: bool,
    },
    /// Stream rendered events from an agent's session log. Resolves the agent's
    /// latest session log directory via the journal. Default: print events so far
    /// and exit. `--follow` keeps tailing until Ctrl-C.
    Tail {
        /// Agent name whose session log to tail.
        agent: String,
        /// Keep following new events (like `tail -f`).
        #[arg(long, short = 'f')]
        follow: bool,
        /// Emit raw JSONL lines instead of rendered text.
        #[arg(long)]
        raw: bool,
    },
    /// Performance report: model × effort aggregates over terminal tasks.
    /// Read-only, no mutations.
    Perf {
        /// Cut dimension: `complexity` or `reviewer`.
        #[arg(long)]
        by: Option<String>,
        /// Emit JSON instead of a table.
        #[arg(long)]
        json: bool,
    },
    /// Classify tasks: assign complexity scores, shape-lint flags, type tags,
    /// and duplicate-of hints. Primarily driven by the daemon; this command is
    /// for manual backfill of historical tasks.
    Classify {
        /// Backfill all tasks (any status) that lack a complexity score.
        #[arg(long)]
        backfill: bool,
        /// Override the agent binary (default: "claude").
        #[arg(long)]
        agent_bin: Option<String>,
        /// Disable --bare for the classifier agent so it inherits operator
        /// credentials — required on subscription-auth (logged-in) machines,
        /// same as `serve --no-bare-agent`.
        #[arg(long)]
        no_bare_agent: bool,
    },
    /// Hard-terminate a daemon-managed agent. Writes a kill request to the
    /// mailbox; the daemon consumes it and SIGTERM→SIGKILL the child process,
    /// releases the slot, and runs the post-mortem ladder on any held task.
    /// Exit 0 = kill delivered to mailbox. The daemon acts on it at next tick.
    Kill {
        /// Target agent name to kill.
        #[arg(long)]
        agent: String,
        /// Who is requesting the kill.
        #[arg(long)]
        by: String,
        #[arg(long = "reason-stdin")]
        reason_stdin: bool,
        #[arg(long = "reason-file")]
        reason_file: Option<PathBuf>,
    },
    /// Print a one-screen cheat-sheet of all commands (for agents to re-orient).
    /// `help-agent` is kept as a back-compat alias.
    #[command(name = "help", alias = "help-agent")]
    Help,
}
