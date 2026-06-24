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
}
