//! Command-line surface (clap). clap handles `--help`/usage errors itself, exiting 2 —
//! which matches our usage-error exit code.

use clap::{Parser, Subcommand};
use std::path::PathBuf;

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
    /// List known agents with derived online/offline presence.
    Roster,
    /// Atomically claim a target (e.g. pr#2459) for a TTL lease.
    Claim {
        #[arg(long)]
        agent: String,
        #[arg(long)]
        target: String,
        /// Lease duration, e.g. 45m, 1h, 30s, or bare seconds.
        #[arg(long)]
        ttl: String,
    },
    /// Release a claim you hold (by --target or --claim-id).
    Release {
        #[arg(long)]
        agent: String,
        #[arg(long)]
        target: Option<String>,
        #[arg(long = "claim-id")]
        claim_id: Option<i64>,
    },
    /// Extend a claim's lease (must be the active holder).
    Renew {
        #[arg(long)]
        agent: String,
        #[arg(long = "claim-id")]
        claim_id: i64,
        #[arg(long)]
        ttl: String,
    },
    /// List active claims, optionally filtered to one target.
    Claims {
        #[arg(long)]
        target: Option<String>,
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
        #[arg(long = "body-stdin")]
        body_stdin: bool,
        #[arg(long = "body-file")]
        body_file: Option<PathBuf>,
    },
    /// Atomically claim a task (a specific --task-id, or the highest-priority open task), taking
    /// a renewable lease. A lapsed lease returns the task to `open` (reaper).
    TaskClaim {
        #[arg(long)]
        agent: String,
        #[arg(long = "task-id")]
        task_id: Option<i64>,
        /// Lease duration, e.g. 45m, 1h, 30s, or bare seconds. Defaults to the config lease TTL.
        #[arg(long)]
        ttl: Option<String>,
    },
    /// Submit a task as `done` (review pending). Only the assignee may, and only `done`.
    /// (Hand-off is `task-release` then a fresh `task-claim`, not reassignment.)
    TaskUpdate {
        #[arg(long)]
        agent: String,
        #[arg(long = "task-id")]
        task_id: i64,
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        refs: Option<String>,
        #[arg(long = "body-stdin")]
        body_stdin: bool,
        #[arg(long = "body-file")]
        body_file: Option<PathBuf>,
    },
    /// Release a task you hold back to `open` (give-up). Assignee-only.
    TaskRelease {
        #[arg(long)]
        agent: String,
        #[arg(long = "task-id")]
        task_id: i64,
    },
    /// Extend your lease on a claimed task (must be the active holder).
    TaskRenew {
        #[arg(long)]
        agent: String,
        #[arg(long = "task-id")]
        task_id: i64,
        /// Lease duration, e.g. 45m, 1h. Defaults to the config lease TTL.
        #[arg(long)]
        ttl: Option<String>,
    },
    /// Cancel a task (terminal won't-do). Creator OR assignee may cancel.
    TaskCancel {
        #[arg(long)]
        agent: String,
        #[arg(long = "task-id")]
        task_id: i64,
    },
    /// List tasks, optionally filtered by status/label/assignee.
    TaskList {
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        label: Option<String>,
        #[arg(long)]
        assignee: Option<String>,
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
    /// Read new messages since your cursor; --ack-through advances the cursor.
    /// Default returns broadcasts + direct-to-you. `--direct` keeps only direct-to-you;
    /// `--broadcasts` keeps only general (no recipient). The two are mutually exclusive.
    Read {
        #[arg(long)]
        agent: String,
        #[arg(long)]
        topic: Option<String>,
        #[arg(long = "ack-through")]
        ack_through: Option<i64>,
        #[arg(long)]
        limit: Option<i64>,
        /// Show only direct-to-you messages.
        #[arg(long, conflicts_with = "broadcasts")]
        direct: bool,
        /// Show only broadcasts (no recipient).
        #[arg(long)]
        broadcasts: bool,
    },
    /// Inspect messages without touching any cursor.
    Peek {
        #[arg(long)]
        topic: Option<String>,
        #[arg(long)]
        since: Option<i64>,
        #[arg(long)]
        limit: Option<i64>,
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
    /// Health snapshot. --json for machine output; --watch to refresh continuously.
    Status {
        #[arg(long)]
        json: bool,
        #[arg(long)]
        watch: bool,
    },
    /// Reclaim all expired rows and checkpoint the WAL.
    Sweep,
    /// Print a one-screen cheat-sheet of all commands (for agents to re-orient).
    /// `help-agent` is kept as a back-compat alias.
    #[command(name = "help", alias = "help-agent")]
    Help,
}
