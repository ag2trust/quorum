//! `quorum serve` — the agent-manager daemon.
//!
//! Builds a tokio runtime and runs an async tick loop that polls the mailbox,
//! spawns/drives agents, and shuts down cleanly on Ctrl-C. See spec §3.

use quorum_core::error::{QuorumError, Result};
use std::io::Write;
use std::path::Path;

fn log(msg: &str) {
    let _ = writeln!(std::io::stderr(), "quorum serve: {msg}");
}

pub fn run_serve(db_path: &Path, cap: usize) -> Result<()> {
    log(&format!("starting (cap={cap})"));

    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| QuorumError::Io(format!("failed to create tokio runtime: {e}")))?;

    rt.block_on(tick_loop(db_path, cap))
}

async fn tick_loop(db_path: &Path, cap: usize) -> Result<()> {
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .map_err(|e| QuorumError::Io(format!("failed to register SIGINT handler: {e}")))?;

    log(&format!("serving (cap={cap})"));

    loop {
        tokio::select! {
            _ = sigint.recv() => {
                log("shutting down (Ctrl-C)");
                return Ok(());
            }
            _ = tick(db_path) => {}
        }
    }
}

async fn tick(db_path: &Path) -> Result<()> {
    let path = db_path.to_owned();
    let count = tokio::task::spawn_blocking(move || -> Result<usize> {
        let conn = quorum_core::db::open(&path)?;
        let rows = quorum_core::mailbox::poll_unconsumed(&conn)?;
        Ok(rows.len())
    })
    .await
    .map_err(|e| QuorumError::Io(format!("spawn_blocking join: {e}")))?;

    let count = count?;
    if count > 0 {
        log(&format!("{count} unconsumed mailbox row(s)"));
    }

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    Ok(())
}
