//! Command-line surface (clap). clap handles `--help`/usage errors itself, exiting 2 —
//! which matches our usage-error exit code.

use clap::{Parser, Subcommand};

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
}
