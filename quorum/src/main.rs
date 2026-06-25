//! `quorum` — daemon-less CLI for local agent coordination.
//!
//! Each invocation opens the SQLite store, performs one atomic operation, prints JSON, and
//! exits with a stable code: 0 success · 1 clean "didn't get it"/not-holder · 2 usage/bad
//! input · 3 internal/DB/migration error.

mod cheatsheet;
mod cli;
mod config;
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
        cli::Command::TaskRelease { .. } => "task-release",
        cli::Command::TaskRenew { .. } => "task-renew",
        cli::Command::TaskCancel { .. } => "task-cancel",
        cli::Command::TaskList { .. } => "task-list",
        cli::Command::TaskGet { .. } => "task-get",
        cli::Command::Post { .. } => "post",
        cli::Command::Read { .. } => "read",
        cli::Command::Peek { .. } => "peek",
        cli::Command::Log { .. } => "log",
        cli::Command::Status { .. } => "status",
        cli::Command::Sweep => "sweep",
        cli::Command::Help => "help",
    }
}

/// Render a stats snapshot as a compact human-readable table.
fn print_status_table(s: &quorum_core::stats::Stats) {
    println!(
        "agents     : {} online / {} total",
        s.agents_online, s.agents_total
    );
    println!("messages   : {} live", s.messages_live);
    println!("claims     : {} active", s.claims_active);
    let tasks = if s.tasks.is_empty() {
        "none".to_string()
    } else {
        s.tasks
            .iter()
            .map(|t| format!("{}={}", t.status, t.count))
            .collect::<Vec<_>>()
            .join(" ")
    };
    println!("tasks      : {tasks}");
    println!("errors     : {} live", s.errors_live);
    for e in &s.last_errors {
        println!("  [{}] {}: {}", e.ts, e.source, e.detail);
    }
}

/// `status --watch`: re-render every ~1.5s. Opens a FRESH short-lived connection per tick and
/// closes it — never holds a transaction across ticks, which would pin the WAL (see CLAUDE.md).
fn watch_status(online_window: i64) -> Result<()> {
    loop {
        let now = quorum_core::clock::now();
        let conn = quorum_core::db::open(&paths::db_path()?)?;
        let s = quorum_core::stats::stats(&conn, now, online_window)?;
        drop(conn); // close before sleeping; do not hold across ticks
        print!("\x1b[2J\x1b[H"); // clear screen + home
        print_status_table(&s);
        std::io::Write::flush(&mut std::io::stdout()).ok();
        std::thread::sleep(std::time::Duration::from_millis(1500));
    }
}

/// Reject a negative numeric flag value (fail loud per the input-validation principle).
fn check_nonneg(flag: &str, v: Option<i64>) -> Result<()> {
    match v {
        Some(n) if n < 0 => Err(QuorumError::Usage(format!("{flag} must be >= 0"))),
        _ => Ok(()),
    }
}

/// Resolve an optional free-text body from `--body-stdin` / `--body-file` (at most one).
fn read_optional_body(stdin: bool, file: Option<std::path::PathBuf>) -> Result<Option<String>> {
    read_optional_text(stdin, file, "--body-stdin", "--body-file")
}

/// Resolve an optional free-text note from `--note-stdin` / `--note-file` (at most one).
fn read_optional_note(stdin: bool, file: Option<std::path::PathBuf>) -> Result<Option<String>> {
    read_optional_text(stdin, file, "--note-stdin", "--note-file")
}

fn read_optional_text(
    stdin: bool,
    file: Option<std::path::PathBuf>,
    stdin_flag: &str,
    file_flag: &str,
) -> Result<Option<String>> {
    match (stdin, file) {
        (true, Some(_)) => Err(QuorumError::Usage(format!(
            "use only one of {stdin_flag} / {file_flag}"
        ))),
        (true, None) => Ok(Some(input::read_text(input::TextSource::Stdin)?)),
        (false, Some(p)) => Ok(Some(input::read_text(input::TextSource::File(p))?)),
        (false, None) => Ok(None),
    }
}

/// Load config from the standard path. Called lazily, only by commands that read its fields,
/// so a malformed config never breaks recovery (`help`) or maintenance (`sweep`/`init`).
fn load_cfg() -> Result<config::Config> {
    config::load(&paths::config_path()?)
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
            // Write a default config if absent (don't clobber an existing one).
            let cfg_path = paths::config_path()?;
            if !cfg_path.exists() {
                std::fs::write(&cfg_path, config::DEFAULT_TOML)
                    .map_err(|e| QuorumError::Io(e.to_string()))?;
            }
            output::emit(&serde_json::json!({ "ok": true, "db": db.to_string_lossy() }));
            Ok(0)
        }
        cli::Command::Roster => {
            let cfg = load_cfg()?;
            let conn = quorum_core::db::open(&paths::db_path()?)?;
            let agents = quorum_core::agents::roster(&conn, now, cfg.online_window_secs)?;
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
            depends_on,
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
                depends_on.as_deref(),
                now,
            )?;
            output::emit(&serde_json::json!({ "id": id }));
            Ok(0)
        }
        cli::Command::TaskClaim {
            agent,
            task_id,
            match_label,
            ttl,
        } => {
            let ttl = match ttl {
                Some(s) => parse_ttl(&s)?,
                None => load_cfg()?.task_lease_ttl_secs,
            };
            let mut conn = quorum_core::db::open(&paths::db_path()?)?;
            let labels: Vec<&str> = match_label.iter().map(String::as_str).collect();
            match quorum_core::tasks::claim(&mut conn, &agent, task_id, &labels, ttl, now)? {
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
            refs,
            body_stdin,
            body_file,
            note_stdin,
            note_file,
        } => {
            let body = read_optional_body(body_stdin, body_file)?;
            let note = read_optional_note(note_stdin, note_file)?;
            let has_field_update = status.is_some() || refs.is_some() || body.is_some();
            if !has_field_update && note.is_none() {
                return Err(QuorumError::Usage(
                    "task-update needs at least one of --status/--refs/\
                     --body-stdin/--body-file/--note-stdin/--note-file"
                        .into(),
                ));
            }
            let mut conn = quorum_core::db::open(&paths::db_path()?)?;
            // Field updates first (assignee-gated under #14's lifecycle: only `--status done`
            // and free-text `--body-*`/`--refs` are accepted). If the caller isn't the holder
            // we abort before adding the note, so `--note-* + --status done` from a
            // non-assignee is a single coherent failure rather than a half-applied operation.
            let task = if has_field_update {
                let fields = quorum_core::tasks::TaskUpdate {
                    status: status.as_deref(),
                    body: body.as_deref(),
                    refs: refs.as_deref(),
                };
                quorum_core::tasks::update(&mut conn, &agent, task_id, &fields, now)?
            } else {
                match quorum_core::tasks::get(&conn, task_id)? {
                    Some(t) => t,
                    None => {
                        output::emit(&serde_json::json!({ "ok": false, "reason": "not found" }));
                        return Ok(1);
                    }
                }
            };
            // Note path: any agent may add a note; no assignee guard.
            if let Some(body) = note {
                if quorum_core::tasks::add_note(&mut conn, &agent, task_id, &body, now)?.is_none() {
                    output::emit(&serde_json::json!({ "ok": false, "reason": "not found" }));
                    return Ok(1);
                }
            }
            output::emit(&task);
            Ok(0)
        }
        cli::Command::TaskRelease { agent, task_id } => {
            let mut conn = quorum_core::db::open(&paths::db_path()?)?;
            let t = quorum_core::tasks::release(&mut conn, &agent, task_id, now)?;
            output::emit(&t);
            Ok(0)
        }
        cli::Command::TaskRenew {
            agent,
            task_id,
            ttl,
        } => {
            let ttl = match ttl {
                Some(s) => parse_ttl(&s)?,
                None => load_cfg()?.task_lease_ttl_secs,
            };
            let mut conn = quorum_core::db::open(&paths::db_path()?)?;
            let t = quorum_core::tasks::renew(&mut conn, &agent, task_id, ttl, now)?;
            output::emit(&t);
            Ok(0)
        }
        cli::Command::TaskCancel { agent, task_id } => {
            let mut conn = quorum_core::db::open(&paths::db_path()?)?;
            let t = quorum_core::tasks::cancel(&mut conn, &agent, task_id, now)?;
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
            match quorum_core::tasks::get_with_notes(&conn, task_id)? {
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
        cli::Command::Post {
            agent,
            kind,
            topic,
            to,
            ttl,
            refs,
            body_stdin,
            body_file,
        } => {
            let body = read_optional_body(body_stdin, body_file)?.ok_or_else(|| {
                QuorumError::Usage("post requires --body-stdin or --body-file".into())
            })?;
            let ttl = match ttl {
                Some(s) => parse_ttl(&s)?,
                None => load_cfg()?.message_ttl_secs,
            };
            let mut conn = quorum_core::db::open(&paths::db_path()?)?;
            let r = quorum_core::feed::post(
                &mut conn,
                &agent,
                &kind,
                topic.as_deref(),
                &body,
                refs.as_deref(),
                to.as_deref(),
                ttl,
                now,
            )?;
            output::emit(&r);
            Ok(0)
        }
        cli::Command::Read {
            agent,
            topic,
            ack_through,
            limit,
            direct,
            broadcasts,
        } => {
            check_nonneg("--limit", limit)?;
            // clap's `conflicts_with` already rejects --direct + --broadcasts at parse time;
            // this match is the in-code projection to the core filter enum.
            let filter = match (direct, broadcasts) {
                (true, false) => quorum_core::feed::ReadFilter::Direct,
                (false, true) => quorum_core::feed::ReadFilter::Broadcasts,
                _ => quorum_core::feed::ReadFilter::All,
            };
            let read_limit = load_cfg()?.read_limit;
            let mut conn = quorum_core::db::open(&paths::db_path()?)?;
            let msgs = quorum_core::feed::read(
                &mut conn,
                &agent,
                topic.as_deref(),
                ack_through,
                filter,
                limit.unwrap_or(read_limit),
                now,
            )?;
            output::emit(&msgs);
            Ok(0)
        }
        cli::Command::Peek {
            topic,
            since,
            limit,
        } => {
            check_nonneg("--limit", limit)?;
            check_nonneg("--since", since)?;
            let read_limit = load_cfg()?.read_limit;
            let conn = quorum_core::db::open(&paths::db_path()?)?;
            let msgs = quorum_core::feed::peek(
                &conn,
                topic.as_deref(),
                since,
                limit.unwrap_or(read_limit),
                now,
            )?;
            output::emit(&msgs);
            Ok(0)
        }
        cli::Command::Log { since, refs, limit } => {
            check_nonneg("--limit", limit)?;
            check_nonneg("--since", since)?;
            let read_limit = load_cfg()?.read_limit;
            let conn = quorum_core::db::open(&paths::db_path()?)?;
            let evs = quorum_core::events::list(
                &conn,
                since.unwrap_or(0),
                refs.as_deref(),
                limit.unwrap_or(read_limit),
                now,
            )?;
            output::emit(&evs);
            Ok(0)
        }
        cli::Command::Status { json, watch } => {
            let cfg = load_cfg()?;
            if watch {
                watch_status(cfg.online_window_secs)?;
                Ok(0) // unreachable in practice (loop until interrupted)
            } else {
                let conn = quorum_core::db::open(&paths::db_path()?)?;
                let s = quorum_core::stats::stats(&conn, now, cfg.online_window_secs)?;
                if json {
                    output::emit(&s);
                } else {
                    print_status_table(&s);
                }
                Ok(0)
            }
        }
        cli::Command::Sweep => {
            let conn = quorum_core::db::open(&paths::db_path()?)?;
            quorum_core::sweep::sweep_all(&conn, now)?;
            output::emit(&serde_json::json!({ "ok": true }));
            Ok(0)
        }
        cli::Command::Help => {
            print!("{}", cheatsheet::CHEATSHEET);
            Ok(0)
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
