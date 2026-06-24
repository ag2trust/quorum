//! Command-line surface (clap). clap handles `--help`/usage errors itself, exiting 2 —
//! which matches our usage-error exit code.

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "quorum",
    version,
    about = "Local agent coordination (by agents, for agents)"
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
    /// Atomically claim a task (a specific --task-id, or the highest-priority open task).
    TaskClaim {
        #[arg(long)]
        agent: String,
        #[arg(long = "task-id")]
        task_id: Option<i64>,
    },
    /// Update a task you are assigned to.
    TaskUpdate {
        #[arg(long)]
        agent: String,
        #[arg(long = "task-id")]
        task_id: i64,
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        assignee: Option<String>,
        #[arg(long)]
        refs: Option<String>,
        #[arg(long = "body-stdin")]
        body_stdin: bool,
        #[arg(long = "body-file")]
        body_file: Option<PathBuf>,
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
}
