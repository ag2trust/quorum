//! `quorum` — daemon-less CLI for local agent coordination.
//!
//! Each invocation opens the SQLite store, performs one atomic operation, prints JSON, and
//! exits with a stable code: 0 success · 1 clean "didn't get it"/not-holder · 2 usage/bad
//! input · 3 internal/DB/migration error.

mod cli;
mod input;
mod output;
mod paths;

use clap::Parser;
use quorum_core::claims::{ClaimOutcome, ClaimSelector};
use quorum_core::error::{QuorumError, Result};

fn run() -> Result<i32> {
    let cli = cli::Cli::parse();
    let source = command_source(&cli.command);
    let result = dispatch(cli.command);
    // Best-effort: log genuinely abnormal failures (exit 3) — never normal lost-race (1) or
    // usage errors (2).
    if let Err(ref e) = result {
        if e.exit_code() == 3 {
            best_effort_errlog(source, &e.to_string());
        }
    }
    result
}

fn command_source(cmd: &cli::Command) -> &'static str {
    match cmd {
        cli::Command::Init => "init",
        cli::Command::Roster => "roster",
        cli::Command::Claim { .. } => "claim",
        cli::Command::Release { .. } => "release",
        cli::Command::Renew { .. } => "renew",
        cli::Command::Claims { .. } => "claims",
        cli::Command::TaskCreate { .. } => "task-create",
        cli::Command::TaskClaim { .. } => "task-claim",
        cli::Command::TaskUpdate { .. } => "task-update",
        cli::Command::TaskList { .. } => "task-list",
        cli::Command::TaskGet { .. } => "task-get",
    }
}

/// Resolve an optional free-text body from `--body-stdin` / `--body-file` (at most one).
fn read_optional_body(stdin: bool, file: Option<std::path::PathBuf>) -> Result<Option<String>> {
    match (stdin, file) {
        (true, Some(_)) => Err(QuorumError::Usage(
            "use only one of --body-stdin / --body-file".into(),
        )),
        (true, None) => Ok(Some(input::read_text(input::TextSource::Stdin)?)),
        (false, Some(p)) => Ok(Some(input::read_text(input::TextSource::File(p))?)),
        (false, None) => Ok(None),
    }
}

fn best_effort_errlog(source: &str, detail: &str) {
    if let Ok(db) = paths::db_path() {
        if let Ok(conn) = quorum_core::db::open(&db) {
            quorum_core::errlog::log_error(&conn, quorum_core::clock::now(), source, detail);
        }
    }
}

/// Parse a duration like `45m`, `1h`, `30s`, `2d`, or bare seconds, into seconds.
fn parse_ttl(s: &str) -> Result<i64> {
    let s = s.trim();
    // The suffix arms only match 1-byte ASCII units, so `s[..len-1]` always lands on a char
    // boundary (a multi-byte suffix falls through to the `_` arm and is never sliced).
    let (num, mult) = match s.chars().last() {
        Some('s') => (&s[..s.len() - 1], 1),
        Some('m') => (&s[..s.len() - 1], 60),
        Some('h') => (&s[..s.len() - 1], 3600),
        Some('d') => (&s[..s.len() - 1], 86400),
        _ => (s, 1),
    };
    let v: i64 = num
        .trim()
        .parse()
        .map_err(|_| QuorumError::Usage(format!("invalid duration: {s}")))?;
    if v <= 0 {
        return Err(QuorumError::Usage(format!(
            "duration must be positive: {s}"
        )));
    }
    Ok(v * mult)
}

fn dispatch(cmd: cli::Command) -> Result<i32> {
    let now = quorum_core::clock::now();
    match cmd {
        cli::Command::Init => {
            paths::ensure_home()?;
            let db = paths::db_path()?;
            quorum_core::db::open(&db)?;
            output::emit(&serde_json::json!({ "ok": true, "db": db.to_string_lossy() }));
            Ok(0)
        }
        cli::Command::Roster => {
            let conn = quorum_core::db::open(&paths::db_path()?)?;
            let agents =
                quorum_core::agents::roster(&conn, now, quorum_core::agents::ONLINE_WINDOW_SECS)?;
            output::emit(&agents);
            Ok(0)
        }
        cli::Command::Claim { agent, target, ttl } => {
            let ttl = parse_ttl(&ttl)?;
            let mut conn = quorum_core::db::open(&paths::db_path()?)?;
            match quorum_core::claims::claim(&mut conn, &agent, &target, ttl, now)? {
                ClaimOutcome::Won(c) => {
                    output::emit(&serde_json::json!({
                        "ok": true, "claim_id": c.id, "target": c.target,
                        "holder": c.holder, "expires_at": c.expires_at,
                    }));
                    Ok(0)
                }
                ClaimOutcome::Lost { holder, expires_at } => {
                    output::emit(&serde_json::json!({
                        "ok": false, "holder": holder, "expires_at": expires_at,
                    }));
                    Ok(1)
                }
            }
        }
        cli::Command::Release {
            agent,
            target,
            claim_id,
        } => {
            let sel = match (target, claim_id) {
                (Some(t), None) => ClaimSelector::Target(t),
                (None, Some(id)) => ClaimSelector::Id(id),
                _ => {
                    return Err(QuorumError::Usage(
                        "exactly one of --target / --claim-id is required".into(),
                    ))
                }
            };
            let mut conn = quorum_core::db::open(&paths::db_path()?)?;
            let out = quorum_core::claims::release(&mut conn, &agent, &sel, now)?;
            output::emit(&out);
            Ok(0)
        }
        cli::Command::Renew {
            agent,
            claim_id,
            ttl,
        } => {
            let ttl = parse_ttl(&ttl)?;
            let mut conn = quorum_core::db::open(&paths::db_path()?)?;
            let c = quorum_core::claims::renew(&mut conn, &agent, claim_id, ttl, now)?;
            output::emit(&c);
            Ok(0)
        }
        cli::Command::Claims { target } => {
            let conn = quorum_core::db::open(&paths::db_path()?)?;
            let list = quorum_core::claims::list(&conn, target.as_deref(), now)?;
            output::emit(&list);
            Ok(0)
        }
        cli::Command::TaskCreate {
            created_by,
            title,
            priority,
            labels,
            refs,
            body_stdin,
            body_file,
        } => {
            let body = read_optional_body(body_stdin, body_file)?;
            let mut conn = quorum_core::db::open(&paths::db_path()?)?;
            let id = quorum_core::tasks::create(
                &mut conn,
                &created_by,
                &title,
                body.as_deref(),
                priority.unwrap_or(0),
                labels.as_deref(),
                refs.as_deref(),
                now,
            )?;
            output::emit(&serde_json::json!({ "id": id }));
            Ok(0)
        }
        cli::Command::TaskClaim { agent, task_id } => {
            let mut conn = quorum_core::db::open(&paths::db_path()?)?;
            match quorum_core::tasks::claim(&mut conn, &agent, task_id, now)? {
                Some(t) => {
                    output::emit(&t);
                    Ok(0)
                }
                None => {
                    output::emit(
                        &serde_json::json!({ "ok": false, "reason": "no claimable task" }),
                    );
                    Ok(1)
                }
            }
        }
        cli::Command::TaskUpdate {
            agent,
            task_id,
            status,
            assignee,
            refs,
            body_stdin,
            body_file,
        } => {
            let body = read_optional_body(body_stdin, body_file)?;
            let mut conn = quorum_core::db::open(&paths::db_path()?)?;
            let fields = quorum_core::tasks::TaskUpdate {
                status: status.as_deref(),
                body: body.as_deref(),
                refs: refs.as_deref(),
                assignee: assignee.as_deref(),
            };
            let t = quorum_core::tasks::update(&mut conn, &agent, task_id, &fields, now)?;
            output::emit(&t);
            Ok(0)
        }
        cli::Command::TaskList {
            status,
            label,
            assignee,
        } => {
            let conn = quorum_core::db::open(&paths::db_path()?)?;
            let list = quorum_core::tasks::list(
                &conn,
                status.as_deref(),
                label.as_deref(),
                assignee.as_deref(),
            )?;
            output::emit(&list);
            Ok(0)
        }
        cli::Command::TaskGet { task_id } => {
            let conn = quorum_core::db::open(&paths::db_path()?)?;
            match quorum_core::tasks::get(&conn, task_id)? {
                Some(t) => {
                    output::emit(&t);
                    Ok(0)
                }
                None => {
                    output::emit(&serde_json::json!({ "ok": false, "reason": "not found" }));
                    Ok(1)
                }
            }
        }
    }
}

fn main() {
    match run() {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            output::emit_err(&e);
            std::process::exit(e.exit_code());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_ttl;

    #[test]
    fn parse_ttl_units() {
        assert_eq!(parse_ttl("30s").unwrap(), 30);
        assert_eq!(parse_ttl("45m").unwrap(), 45 * 60);
        assert_eq!(parse_ttl("1h").unwrap(), 3600);
        assert_eq!(parse_ttl("2d").unwrap(), 2 * 86400);
        assert_eq!(parse_ttl("90").unwrap(), 90);
    }

    #[test]
    fn parse_ttl_rejects_bad() {
        assert!(parse_ttl("abc").is_err());
        assert!(parse_ttl("0").is_err());
        assert!(parse_ttl("-5").is_err());
    }
}
