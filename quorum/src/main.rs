//! `quorum` — daemon-less CLI for local agent coordination.
//!
//! Each invocation opens the SQLite store, performs one atomic operation, prints JSON, and
//! exits with a stable code: 0 success · 1 clean "didn't get it"/not-holder · 2 usage/bad
//! input · 3 internal/DB/migration error.

mod cheatsheet;
mod cli;
mod cockpit;
mod config;
mod input;
mod output;
mod paths;
mod serve;
mod serve_config;
mod verdict;

use clap::Parser;
use quorum_core::error::{QuorumError, Result};

const EMBEDDED_SKILL: &str = include_str!("../../.claude/skills/quorum/SKILL.md");

const DEFAULT_SERVE_TOML: &str = "\
# quorum serve config — uncomment and edit values as needed.
# CLI flags override these; missing keys use built-in defaults.
# See `quorum serve --help` for flag equivalents.

## Required — no defaults (serve will error without these or equivalent flags)
# repo = \"owner/name\"
# repo_dir = \"/path/to/checkout\"
# worktree_base = \"/path/to/worktrees\"

## Worker / model
# cap = 4
# model = \"sonnet\"
# effort = \"high\"
# names_file = \"/path/to/names.txt\"
# agent_bin = \"claude\"
# no_bare_agent = false
# allowed_tools = \"Bash,Read,Write,Edit,Grep,Glob\"
# base_branch = \"main\"

## Token / cost / wall-clock limits (unlimited when absent)
# max_turn_tokens = 200000
# max_task_tokens = 1000000
# max_turn_cost_usd = 5.0
# max_task_cost_usd = 50.0
# max_turn_wall_secs = 2700
# max_task_wall_secs = 14400
# idle_timeout_secs = 300

## Merge
# merge_token_file = \"/path/to/token\"
# merge_checks_timeout_secs = 900
# merge_checks_poll_secs = 30
# required_jobs = [\"ci\"]
# master_ci_gate = false
# master_ci_timeout_secs = 300

## Self-update drain
# self_update_drain = false
# drain_timeout_secs = 900
# self_repo = \"owner/name\"
# sha_poll_interval_secs = 60

## Diagnostics
# log_dir = \"/path/to/logs\"
# doctor_enabled = false

## R2 pre-merge review (adversarial second reviewer, sampled at R1 approval)
# r2_enabled = false
# r2_target_per_stratum = 5
# r2_steady_state_p = 0.10
";

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
        cli::Command::Reset { .. } => "reset",
        cli::Command::TaskCreate { .. } => "task-create",
        cli::Command::TaskClaim { .. } => "task-claim",
        cli::Command::TaskUpdate { .. } => "task-update",
        cli::Command::TaskList { .. } => "task-list",
        cli::Command::TaskGet { .. } => "task-get",
        cli::Command::Post { .. } => "post",
        cli::Command::Read { .. } => "read",
        cli::Command::Log { .. } => "log",
        cli::Command::Stop { .. } => "stop",
        cli::Command::Resume { .. } => "resume",
        cli::Command::Stops => "stops",
        cli::Command::Pin { .. } => "pin",
        cli::Command::Unpin { .. } => "unpin",
        cli::Command::Pins => "pins",
        cli::Command::Sync { .. } => "sync",
        cli::Command::Status { .. } => "status",
        cli::Command::Sweep => "sweep",
        cli::Command::Message { .. } => "message",
        cli::Command::React { .. } => "react",
        cli::Command::Submit { .. } => "submit",
        cli::Command::Serve { .. } => "serve",
        cli::Command::SessionRegister { .. } => "session-register",
        cli::Command::Activity { .. } => "activity",
        cli::Command::Tail { .. } => "tail",
        cli::Command::Perf { .. } => "perf",
        cli::Command::Classify { .. } => "classify",
        cli::Command::TaskClose { .. } => "task-close",
        cli::Command::Kill { .. } => "kill",
        cli::Command::ReviewInterpret { .. } => "review-interpret",
        cli::Command::Upgrade { .. } => "upgrade",
        cli::Command::Help => "help",
        cli::Command::InspectMessage { .. } => "inspect-message",
        cli::Command::InspectMessages { .. } => "inspect-messages",
        cli::Command::InspectEvents { .. } => "inspect-events",
        cli::Command::InspectErrors { .. } => "inspect-errors",
    }
}

/// `status --watch`: re-render every ~1.5s. Opens a FRESH short-lived connection per tick and
/// closes it — never holds a transaction across ticks, which would pin the WAL (see CLAUDE.md).
fn watch_status(online_window: i64) -> Result<()> {
    loop {
        let now = quorum_core::clock::now();
        let conn = quorum_core::db::open(&paths::db_path()?)?;
        let mut s = quorum_core::stats::stats(&conn, now, online_window)?;
        s.daemon = quorum_core::daemon_lock::liveness(&conn, now, DAEMON_STALE_SECS, pid_is_alive)?;
        drop(conn); // close before sleeping; do not hold across ticks
        print!("\x1b[H\x1b[J");
        cockpit::render(&s);
        std::io::Write::flush(&mut std::io::stdout()).ok();
        std::thread::sleep(std::time::Duration::from_millis(1500));
    }
}

const DAEMON_STALE_SECS: i64 = 30;

fn pid_is_alive(pid: i64) -> bool {
    let ret = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if ret == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
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

/// Resolve repo identity: explicit `--repo owner/name` override, or cwd/env fallback.
fn resolve_repo_override(repo: Option<&str>) -> Result<String> {
    match repo {
        Some(r) => {
            let slash_count = r.chars().filter(|&c| c == '/').count();
            if slash_count != 1 || r.starts_with('/') || r.ends_with('/') {
                return Err(QuorumError::Usage(format!(
                    "--repo must be owner/name (got {r:?})"
                )));
            }
            Ok(r.to_string())
        }
        None => paths::resolve_repo(),
    }
}

fn best_effort_errlog(source: &str, detail: &str) {
    if let Ok(db) = paths::db_path() {
        if let Ok(conn) = quorum_core::db::open(&db) {
            quorum_core::errlog::log_error(&conn, quorum_core::clock::now(), source, detail);
        }
    }
}

/// Largest accepted TTL: ~100 years in seconds. There are two TTL input paths — the `--ttl`
/// flag (bounded here in `parse_ttl`) and the `message_ttl_secs` / `task_lease_ttl_secs`
/// config defaults used when `--ttl` is omitted (bounded in `config::validate`). Clamping
/// BOTH to this ceiling guarantees `now + ttl` can never overflow i64 at any write site for
/// any realistic `now` — preserving the at-most-one-active-claim invariant (an overflowed
/// `expires_at` wraps into the past, silently letting a second agent re-win the target).
/// Unbounded TTLs aren't a real need: "non-expiring" control state lives in its own table,
/// not in a huge lease.
const MAX_TTL_SECS: i64 = 100 * 365 * 86_400;

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
    // `checked_mul` guards the unit multiply (wraps in release, panics in debug); the ceiling
    // then bounds the result so every downstream `now + ttl` is overflow-safe. Both map to a
    // clean usage error (exit 2) rather than a silent wrap or a panic.
    v.checked_mul(mult)
        .filter(|&secs| secs <= MAX_TTL_SECS)
        .ok_or_else(|| QuorumError::Usage(format!("duration too large (max {MAX_TTL_SECS}s): {s}")))
}

fn resolve_gh_repo(repo_dir: &str) -> Option<String> {
    let child = std::process::Command::new("gh")
        .args([
            "repo",
            "view",
            "--json",
            "nameWithOwner",
            "-q",
            ".nameWithOwner",
        ])
        .current_dir(repo_dir)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .ok()?;

    let nwo = wait_child_stdout(child, std::time::Duration::from_secs(15))?;
    let nwo = nwo.trim().to_string();
    if nwo.contains('/') {
        Some(nwo)
    } else {
        None
    }
}

/// Wait for a spawned child with a timeout. Returns stdout on success, None on
/// failure/timeout. On timeout the child is killed and a warning is emitted.
fn wait_child_stdout(
    mut child: std::process::Child,
    timeout: std::time::Duration,
) -> Option<String> {
    use std::io::Read as _;

    let deadline = std::time::Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if status.success() {
                    let mut buf = String::new();
                    if let Some(mut stdout) = child.stdout.take() {
                        let _ = stdout.read_to_string(&mut buf);
                    }
                    return Some(buf);
                }
                return None;
            }
            Ok(None) if std::time::Instant::now() >= deadline => {
                eprintln!(
                    "WARN: resolve_gh_repo: subprocess timed out after {timeout:?}, \
                     falling back to None"
                );
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            Ok(None) => std::thread::sleep(std::time::Duration::from_millis(100)),
            Err(_) => return None,
        }
    }
}

fn dispatch(cmd: cli::Command) -> Result<i32> {
    let now = quorum_core::clock::now();
    match cmd {
        cli::Command::Init => {
            paths::ensure_home()?;
            let db = paths::db_path()?;
            let (_conn, info) = quorum_core::db::open_init(&db)?;
            // Write a default config if absent (don't clobber an existing one).
            let cfg_path = paths::config_path()?;
            if !cfg_path.exists() {
                std::fs::write(&cfg_path, config::DEFAULT_TOML)
                    .map_err(|e| QuorumError::Io(e.to_string()))?;
            }
            let mut out = serde_json::json!({
                "ok": true,
                "db": db.to_string_lossy(),
                "schema_version": info.schema_version,
            });
            if info.migrated_from > 0 && info.migrated_from != info.schema_version {
                out["migrated_from"] = serde_json::json!(info.migrated_from);
            }

            // Repo skill: create .claude/skills/quorum/SKILL.md if missing.
            // Never overwrite — report stale if differs (use `quorum upgrade` to replace).
            if let Some(toplevel) = paths::git_toplevel() {
                let skill_path = toplevel.join(".claude/skills/quorum/SKILL.md");
                match std::fs::read_to_string(&skill_path) {
                    Ok(existing) if existing == EMBEDDED_SKILL => {}
                    Ok(_) => {
                        out["skill"] = serde_json::json!("stale");
                    }
                    Err(_) => {
                        if let Some(parent) = skill_path.parent() {
                            std::fs::create_dir_all(parent)
                                .map_err(|e| QuorumError::Io(e.to_string()))?;
                        }
                        std::fs::write(&skill_path, EMBEDDED_SKILL)
                            .map_err(|e| QuorumError::Io(e.to_string()))?;
                        out["skill"] = serde_json::json!({
                            "path": skill_path.to_string_lossy(),
                            "action": "created",
                        });
                    }
                }
            }

            // Serve config scaffold: create ~/.quorum/serve/<slug>.toml if absent.
            if let Some(repo_slug) = paths::try_resolve_repo() {
                let serve_path = serve_config::default_config_path(&repo_slug)?;
                if !serve_path.exists() {
                    if let Some(parent) = serve_path.parent() {
                        std::fs::create_dir_all(parent)
                            .map_err(|e| QuorumError::Io(e.to_string()))?;
                    }
                    std::fs::write(&serve_path, DEFAULT_SERVE_TOML)
                        .map_err(|e| QuorumError::Io(e.to_string()))?;
                    out["serve_config"] = serde_json::json!({
                        "path": serve_path.to_string_lossy(),
                        "action": "scaffolded",
                    });
                }
            }

            output::emit(&out);
            Ok(0)
        }
        cli::Command::Reset { yes } => {
            // Destructive: refuse without explicit confirmation (exit 2, no wipe).
            if !yes {
                return Err(QuorumError::Usage(
                    "reset wipes ALL state (agents, tasks, claims, messages). Re-run with --yes to confirm.".into(),
                ));
            }
            paths::ensure_home()?;
            let db = paths::db_path()?;
            // Remove the SQLite DB and its WAL/SHM sidecars (absent = fine), then recreate a
            // clean schema via open() (migrations run on the fresh file).
            let fname = db
                .file_name()
                .map(|f| f.to_string_lossy().into_owned())
                .unwrap_or_else(|| "quorum.db".into());
            for suffix in ["", "-wal", "-shm"] {
                let p = db.with_file_name(format!("{fname}{suffix}"));
                match std::fs::remove_file(&p) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(QuorumError::Io(e.to_string())),
                }
            }
            quorum_core::db::open(&db)?;
            output::emit(
                &serde_json::json!({ "ok": true, "reset": true, "db": db.to_string_lossy() }),
            );
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
            review_pr,
            body_file,
            repo,
        } => {
            let body = read_optional_body(body_stdin, body_file)?;
            let resolved_repo = resolve_repo_override(repo.as_deref())?;
            let mut conn = quorum_core::db::open(&paths::ensure_repo_dir(&resolved_repo)?)?;
            let id = quorum_core::tasks::create(
                &mut conn,
                &created_by,
                &title,
                body.as_deref(),
                priority.unwrap_or(0),
                labels.as_deref(),
                refs.as_deref(),
                depends_on.as_deref(),
                review_pr,
                now,
            )?;
            output::emit(&serde_json::json!({ "id": id, "repo": resolved_repo }));
            Ok(0)
        }
        cli::Command::TaskClaim {
            agent,
            task_id,
            match_label,
            ttl,
            repo,
        } => {
            let ttl = match ttl {
                Some(s) => parse_ttl(&s)?,
                None => load_cfg()?.task_lease_ttl_secs,
            };
            let resolved_repo = resolve_repo_override(repo.as_deref())?;
            let mut conn = quorum_core::db::open(&paths::ensure_repo_dir(&resolved_repo)?)?;
            let labels: Vec<&str> = match_label.iter().map(String::as_str).collect();
            match quorum_core::tasks::claim(&mut conn, &agent, task_id, &labels, ttl, now)? {
                Some(t) => {
                    // Issue #64: compact success response — omit `body` (caller knows it)
                    // and include `lease_expires_at` since the lease just landed.
                    let mut compact = quorum_core::tasks::TaskCompact::from(&t);
                    compact.lease_expires_at = Some(now + ttl);
                    // Issue #98: allocate (or reuse) a branch + worktree for this
                    // task — centralized anti-collision naming. Idempotent by
                    // construction (UNIQUE(task_id)), so a reopened-task re-claim
                    // returns the same branch as the original allocation.
                    let branch_hint =
                        quorum_core::branches::branch_hint_from_refs(t.refs.as_deref());
                    let alloc = quorum_core::branches::allocate_for_task(
                        &mut conn,
                        t.id,
                        ".worktrees",
                        &agent,
                        &t.title,
                        t.labels.as_deref(),
                        branch_hint.as_deref(),
                        now,
                    )?;
                    compact.suggested_branch = Some(alloc.branch);
                    compact.suggested_worktree = Some(alloc.worktree);
                    compact.branch_exists = Some(alloc.existed);
                    compact.repo = Some(resolved_repo.clone());
                    output::emit(&compact);
                    Ok(0)
                }
                None => {
                    output::emit(
                        &serde_json::json!({ "ok": false, "reason": "no claimable task", "repo": resolved_repo }),
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
            depends_on,
        } => {
            let body = read_optional_body(body_stdin, body_file)?;
            let note = read_optional_note(note_stdin, note_file)?;
            let has_field_update =
                status.is_some() || refs.is_some() || body.is_some() || depends_on.is_some();
            if !has_field_update && note.is_none() {
                return Err(QuorumError::Usage(
                    "task-update needs at least one of --status/--refs/\
                     --body-stdin/--body-file/--note-stdin/--note-file/--depends-on"
                        .into(),
                ));
            }
            let mut conn = quorum_core::db::open(&paths::db_path()?)?;
            let task = if has_field_update {
                let fields = quorum_core::tasks::TaskUpdate {
                    status: status.as_deref(),
                    body: body.as_deref(),
                    refs: refs.as_deref(),
                    verdict: None,
                    depends_on: depends_on.as_deref(),
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
            let note_id: Option<i64> = if let Some(body) = note {
                match quorum_core::tasks::add_note(&mut conn, &agent, task_id, &body, now)? {
                    Some(id) => Some(id),
                    None => {
                        output::emit(&serde_json::json!({ "ok": false, "reason": "not found" }));
                        return Ok(1);
                    }
                }
            } else {
                None
            };
            let mut compact = quorum_core::tasks::TaskCompact::from(&task);
            compact.note_id = note_id;
            output::emit(&compact);
            Ok(0)
        }
        cli::Command::TaskList {
            status,
            label,
            assignee,
            brief,
        } => {
            let conn = quorum_core::db::open(&paths::db_path()?)?;
            let list = quorum_core::tasks::list(
                &conn,
                status.as_deref(),
                label.as_deref(),
                assignee.as_deref(),
            )?;
            // --brief projects each task to a summary row (no body) for a cheap queue scan;
            // the full record stays available via `task-get`.
            if brief {
                let rows: Vec<quorum_core::tasks::TaskBrief> =
                    list.iter().map(Into::into).collect();
                output::emit(&rows);
            } else {
                output::emit(&list);
            }
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
            since,
        } => {
            check_nonneg("--limit", limit)?;
            let read_limit = load_cfg()?.read_limit;
            match agent {
                Some(ref ag) => {
                    // Agent-based read: cursor-aware, filter by recipient.
                    let filter = match (direct, broadcasts) {
                        (true, false) => quorum_core::feed::ReadFilter::Direct,
                        (false, true) => quorum_core::feed::ReadFilter::Broadcasts,
                        _ => quorum_core::feed::ReadFilter::All,
                    };
                    let mut conn = quorum_core::db::open(&paths::db_path()?)?;
                    let msgs = quorum_core::feed::read(
                        &mut conn,
                        ag,
                        topic.as_deref(),
                        ack_through,
                        filter,
                        limit.unwrap_or(read_limit),
                        now,
                    )?;
                    output::emit(&msgs);
                }
                None => {
                    // Agent-less read (replaces `peek`): no cursor, no recipient filter.
                    if ack_through.is_some() {
                        return Err(QuorumError::Usage("--ack-through requires --agent".into()));
                    }
                    if direct {
                        return Err(QuorumError::Usage("--direct requires --agent".into()));
                    }
                    check_nonneg("--since", since)?;
                    let conn = quorum_core::db::open(&paths::db_path()?)?;
                    let msgs = quorum_core::feed::peek(
                        &conn,
                        topic.as_deref(),
                        since,
                        limit.unwrap_or(read_limit),
                        now,
                    )?;
                    output::emit(&msgs);
                }
            }
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
        cli::Command::Status {
            json,
            watch,
            agents,
        } => {
            let cfg = load_cfg()?;
            if agents {
                let conn = quorum_core::db::open(&paths::db_path()?)?;
                let roster = quorum_core::agents::roster(&conn, now, cfg.online_window_secs)?;
                output::emit(&roster);
                return Ok(0);
            }
            if watch {
                watch_status(cfg.online_window_secs)?;
                Ok(0)
            } else {
                let conn = quorum_core::db::open(&paths::db_path()?)?;
                let mut s = quorum_core::stats::stats(&conn, now, cfg.online_window_secs)?;
                s.daemon = quorum_core::daemon_lock::liveness(
                    &conn,
                    now,
                    DAEMON_STALE_SECS,
                    pid_is_alive,
                )?;
                if json {
                    output::emit(&s);
                } else {
                    cockpit::render(&s);
                }
                Ok(0)
            }
        }
        cli::Command::Stop {
            agent,
            by,
            reason_stdin,
            reason_file,
        } => {
            // Reason must come via stdin/file per Invariant #10 (free text never as a flag).
            let reason = read_optional_body(reason_stdin, reason_file)?.ok_or_else(|| {
                QuorumError::Usage("--reason-stdin or --reason-file is required for `stop`".into())
            })?;
            let mut conn = quorum_core::db::open(&paths::db_path()?)?;
            let s = quorum_core::control::stop(&mut conn, agent.as_deref(), &reason, &by, now)?;
            output::emit(&s);
            Ok(0)
        }
        cli::Command::Resume { agent, by } => {
            let mut conn = quorum_core::db::open(&paths::db_path()?)?;
            match quorum_core::control::resume(&mut conn, agent.as_deref(), &by, now)? {
                Some(cleared) => {
                    output::emit(&serde_json::json!({
                        "ok": true,
                        "cleared": cleared,
                    }));
                    Ok(0)
                }
                None => {
                    output::emit(&serde_json::json!({
                        "ok": false,
                        "reason": "no active stop on that scope",
                    }));
                    Ok(1)
                }
            }
        }
        cli::Command::Stops => {
            let conn = quorum_core::db::open(&paths::db_path()?)?;
            let stops = quorum_core::control::list(&conn)?;
            output::emit(&stops);
            Ok(0)
        }
        cli::Command::Pin {
            agent,
            body_stdin,
            body_file,
        } => {
            // Body via stdin/file per Invariant #10 (free text never as a flag).
            let body = read_optional_body(body_stdin, body_file)?.ok_or_else(|| {
                QuorumError::Usage("--body-stdin or --body-file is required for `pin`".into())
            })?;
            let mut conn = quorum_core::db::open(&paths::db_path()?)?;
            let p = quorum_core::pinned::pin(&mut conn, &agent, &body, now)?;
            output::emit(&p);
            Ok(0)
        }
        cli::Command::Unpin { agent, id } => {
            let mut conn = quorum_core::db::open(&paths::db_path()?)?;
            match quorum_core::pinned::unpin(&mut conn, id, &agent, now)? {
                quorum_core::pinned::UnpinResult::Removed(p) => {
                    output::emit(&serde_json::json!({ "ok": true, "cleared": p }));
                    Ok(0)
                }
                quorum_core::pinned::UnpinResult::NotFound => {
                    output::emit(&serde_json::json!({
                        "ok": false,
                        "reason": "no pin at that id",
                    }));
                    Ok(1)
                }
                quorum_core::pinned::UnpinResult::NotCreator { author } => {
                    output::emit(&serde_json::json!({
                        "ok": false,
                        "reason": "creator-only: this pin belongs to another agent",
                        "author": author,
                    }));
                    Ok(1)
                }
            }
        }
        cli::Command::Pins => {
            let conn = quorum_core::db::open(&paths::db_path()?)?;
            let pins = quorum_core::pinned::list(&conn)?;
            output::emit(&pins);
            Ok(0)
        }
        cli::Command::Sync { agent, match_label } => {
            let cfg = load_cfg()?;
            let mut conn = quorum_core::db::open(&paths::db_path()?)?;
            let labels: Vec<&str> = match_label.iter().map(String::as_str).collect();
            let snap = quorum_core::sync::tick_with_budget(
                &mut conn,
                &agent,
                &labels,
                now,
                cfg.retire_after_active_secs,
                cfg.retire_after_tasks,
                cfg.retire_after_wall_secs,
            )?;
            output::emit(&snap);
            Ok(0)
        }
        cli::Command::Sweep => {
            let conn = quorum_core::db::open(&paths::db_path()?)?;
            quorum_core::sweep::sweep_all(&conn, now)?;
            output::emit(&serde_json::json!({ "ok": true }));
            Ok(0)
        }
        // EXPERIMENTAL (issue #101): session-register + activity. Both are stats-only;
        // failure is silent (exit 0) per the issue's "MUST NOT affect any existing
        // workflow" hard rule. A bad arg / DB-down hook firing must never error
        // the agent's tool call.
        cli::Command::SessionRegister { agent, session } => {
            // Best-effort: a missing DB or invalid input here returns exit 0 (stats only).
            if let Ok(conn) = quorum_core::db::open(&paths::db_path()?) {
                let _ = quorum_core::activity::register_session(&conn, &session, &agent, now);
            }
            output::emit(&serde_json::json!({ "ok": true }));
            Ok(0)
        }
        cli::Command::Activity { session, tool } => {
            if let Ok(conn) = quorum_core::db::open(&paths::db_path()?) {
                let _ = quorum_core::activity::record_activity(&conn, &session, &tool, now);
            }
            output::emit(&serde_json::json!({ "ok": true }));
            Ok(0)
        }
        cli::Command::Message {
            from,
            to,
            body_stdin,
            body_file,
        } => {
            let payload = read_optional_body(body_stdin, body_file)?;
            let payload = match payload {
                Some(p) => p,
                None => {
                    return Err(QuorumError::Usage(
                        "message requires --body-stdin or --body-file".into(),
                    ));
                }
            };
            let db = paths::db_path()?;
            let mut conn = quorum_core::db::open(&db)?;
            let row = quorum_core::mailbox::MailboxRow {
                agent: from,
                kind: quorum_core::mailbox::MailboxKind::Message,
                task_id: None,
                pr: None,
                verdict: None,
                feedback: None,
                note: None,
                to_agent: Some(to),
                payload: Some(payload),
            };
            let id = quorum_core::mailbox::append(&mut conn, &row)?;
            output::emit(&serde_json::json!({ "ok": true, "mailbox_id": id }));
            Ok(0)
        }
        cli::Command::React {
            agent,
            task_id,
            state,
        } => {
            match state.as_str() {
                "blocked" | "failed" | "needs-info" | "note" => {}
                _ => {
                    return Err(QuorumError::Usage(format!(
                        "--state must be 'blocked', 'failed', 'needs-info', or 'note', got '{state}'"
                    )));
                }
            }
            let db = paths::db_path()?;
            let mut conn = quorum_core::db::open(&db)?;
            let row = quorum_core::mailbox::MailboxRow {
                agent,
                kind: quorum_core::mailbox::MailboxKind::TaskUpdate,
                task_id: Some(task_id),
                pr: None,
                verdict: None,
                feedback: None,
                note: Some(state),
                to_agent: None,
                payload: None,
            };
            let id = quorum_core::mailbox::append(&mut conn, &row)?;
            output::emit(&serde_json::json!({ "ok": true, "mailbox_id": id }));
            Ok(0)
        }
        cli::Command::Submit {
            agent,
            pr,
            summary,
            verdict,
            feedback,
            blocking,
            run_id,
        } => {
            if let Some(ref v) = verdict {
                match v.as_str() {
                    "approved" | "changes" => {}
                    _ => {
                        return Err(QuorumError::Usage(format!(
                            "--verdict must be 'approved' or 'changes', got '{v}'"
                        )));
                    }
                }
            }
            verdict::validate(verdict.as_deref(), blocking, feedback.as_deref(), true)
                .map_err(QuorumError::Usage)?;
            let db = paths::db_path()?;
            let mut conn = quorum_core::db::open(&db)?;
            // Validate run capability if provided. Role is derived from the submit
            // shape — `--verdict` present → reviewer, absent → worker — so a
            // worker-role capability cannot sign a reviewer submit and vice-versa.
            if let Some(ref rid) = run_id {
                let expected_role = if verdict.is_some() {
                    "reviewer"
                } else {
                    "worker"
                };
                quorum_core::capabilities::validate(&conn, rid, &agent, expected_role, None)
                    .map_err(|e| QuorumError::Usage(format!("run-id validation: {e}")))?;
            }
            let kind = quorum_core::mailbox::MailboxKind::Done;
            let row = quorum_core::mailbox::MailboxRow {
                agent,
                kind,
                task_id: None,
                pr,
                verdict,
                feedback,
                note: summary,
                to_agent: None,
                payload: verdict::attestation_payload(blocking),
            };
            let id = quorum_core::mailbox::append(&mut conn, &row)?;
            output::emit(&serde_json::json!({ "ok": true, "mailbox_id": id }));
            Ok(0)
        }
        cli::Command::TaskClose {
            agent,
            task_id,
            reason_stdin,
            reason_file,
        } => {
            let reason = read_optional_body(reason_stdin, reason_file)?.ok_or_else(|| {
                QuorumError::Usage(
                    "--reason-stdin or --reason-file is required for `task-close`".into(),
                )
            })?;
            let mut conn = quorum_core::db::open(&paths::db_path()?)?;
            match quorum_core::tasks::close_manual(&mut conn, &agent, task_id, &reason, now)? {
                Some(task) => {
                    let compact = quorum_core::tasks::TaskCompact::from(&task);
                    output::emit(&compact);
                    Ok(0)
                }
                None => {
                    output::emit(&serde_json::json!({
                        "ok": false,
                        "reason": "task not found or already terminal",
                    }));
                    Ok(1)
                }
            }
        }
        cli::Command::Kill {
            agent,
            by,
            reason_stdin,
            reason_file,
        } => {
            let reason = read_optional_body(reason_stdin, reason_file)?;
            let db = paths::db_path()?;
            let mut conn = quorum_core::db::open(&db)?;
            let row = quorum_core::mailbox::MailboxRow {
                agent: by,
                kind: quorum_core::mailbox::MailboxKind::Kill,
                task_id: None,
                pr: None,
                verdict: None,
                feedback: None,
                note: reason,
                to_agent: Some(agent),
                payload: None,
            };
            let id = quorum_core::mailbox::append(&mut conn, &row)?;
            output::emit(&serde_json::json!({ "ok": true, "mailbox_id": id }));
            Ok(0)
        }
        cli::Command::Serve {
            config: config_flag,
            cap,
            repo_dir,
            worktree_base,
            names_file,
            agent_bin,
            model,
            effort,
            merge_token_file,
            merge_cmd,
            merge_checks_cmd,
            merge_mergeability_cmd,
            merge_checks_timeout_secs,
            merge_checks_poll_secs,
            no_bare_agent,
            max_turn_tokens,
            max_task_tokens,
            max_turn_cost_usd,
            max_task_cost_usd,
            max_turn_wall_secs,
            max_task_wall_secs,
            idle_timeout_secs,
            allowed_tools,
            log_dir,
            self_update_drain,
            drain_timeout_secs,
            self_repo,
            sha_poll_interval_secs,
            repo,
            base_branch,
            exit_when_gone,
            doctor_enabled,
        } => {
            use serve_config::*;

            // Step 1: determine repo identity (needed for default config path).
            // Flag > file-at-explicit-path > error. We need repo before we can
            // resolve the default config path, but repo might BE in the config file.
            // Break the cycle: if --config is explicit, load that; if --repo is
            // given, derive the default path from it; otherwise try loading from
            // repo_dir detection.
            let (file_cfg, config_path_used) = if let Some(ref p) = config_flag {
                let path = std::path::Path::new(p);
                let cfg = load(path, true)?;
                (cfg, Some(p.clone()))
            } else if let Some(ref r) = repo {
                let path = default_config_path(r)?;
                if path.exists() {
                    let cfg = load(&path, false)?;
                    (cfg, Some(path.to_string_lossy().into_owned()))
                } else {
                    (ServeFileConfig::default(), None)
                }
            } else {
                (ServeFileConfig::default(), None)
            };

            let r_repo = resolve_str(repo.as_deref(), file_cfg.repo.as_deref(), "");
            if r_repo.value.is_empty() {
                return Err(QuorumError::Usage(
                    "serve requires --repo (or repo in config file)".into(),
                ));
            }

            // If we didn't load a config (no --config, no --repo for default path),
            // try the default path using the now-resolved repo.
            let (file_cfg, config_path_used) =
                if config_path_used.is_none() && config_flag.is_none() {
                    let path = default_config_path(&r_repo.value)?;
                    if path.exists() {
                        let cfg = load(&path, false)?;
                        let p = path.to_string_lossy().into_owned();
                        (cfg, Some(p))
                    } else {
                        (file_cfg, None)
                    }
                } else {
                    (file_cfg, config_path_used)
                };

            let r_repo_dir = resolve_str(repo_dir.as_deref(), file_cfg.repo_dir.as_deref(), "");
            if r_repo_dir.value.is_empty() {
                return Err(QuorumError::Usage(
                    "serve requires --repo-dir (or repo_dir in config file)".into(),
                ));
            }
            let r_wt = resolve_str(
                worktree_base.as_deref(),
                file_cfg.worktree_base.as_deref(),
                "",
            );
            if r_wt.value.is_empty() {
                return Err(QuorumError::Usage(
                    "serve requires --worktree-base (or worktree_base in config file)".into(),
                ));
            }
            let r_cap = resolve_val(cap, file_cfg.cap, 4);
            let r_model = resolve_str(model.as_deref(), file_cfg.model.as_deref(), "sonnet");
            let r_effort = resolve_str(effort.as_deref(), file_cfg.effort.as_deref(), "high");
            let r_names = resolve_opt_str(names_file.as_deref(), file_cfg.names_file.as_deref());
            let r_agent_bin = resolve_opt_str(agent_bin.as_deref(), file_cfg.agent_bin.as_deref());
            let r_merge_token = resolve_opt_str(
                merge_token_file.as_deref(),
                file_cfg.merge_token_file.as_deref(),
            );
            let r_no_bare = resolve_bool(no_bare_agent, file_cfg.no_bare_agent, false);
            let r_max_turn_tokens = resolve_opt(max_turn_tokens, file_cfg.max_turn_tokens);
            let r_max_task_tokens = resolve_opt(max_task_tokens, file_cfg.max_task_tokens);
            let r_max_turn_cost = resolve_opt(max_turn_cost_usd, file_cfg.max_turn_cost_usd);
            let r_max_task_cost = resolve_opt(max_task_cost_usd, file_cfg.max_task_cost_usd);
            let r_max_turn_wall = resolve_opt(max_turn_wall_secs, file_cfg.max_turn_wall_secs);
            let r_max_task_wall = resolve_opt(max_task_wall_secs, file_cfg.max_task_wall_secs);
            let r_idle_timeout = resolve_opt(idle_timeout_secs, file_cfg.idle_timeout_secs);
            let r_allowed_tools =
                resolve_opt_str(allowed_tools.as_deref(), file_cfg.allowed_tools.as_deref());
            let r_log_dir = resolve_opt_str(log_dir.as_deref(), file_cfg.log_dir.as_deref());
            let r_self_update = resolve_bool(self_update_drain, file_cfg.self_update_drain, false);
            let r_drain_timeout = resolve_val(drain_timeout_secs, file_cfg.drain_timeout_secs, 900);
            let r_self_repo = resolve_opt_str(self_repo.as_deref(), file_cfg.self_repo.as_deref());
            let r_sha_poll =
                resolve_val(sha_poll_interval_secs, file_cfg.sha_poll_interval_secs, 60);
            let r_base_branch = resolve_str(
                base_branch.as_deref(),
                file_cfg.base_branch.as_deref(),
                "main",
            );
            let r_merge_checks_timeout = resolve_val(
                merge_checks_timeout_secs,
                file_cfg.merge_checks_timeout_secs,
                900,
            );
            let r_merge_checks_poll =
                resolve_val(merge_checks_poll_secs, file_cfg.merge_checks_poll_secs, 30);
            let r_required_jobs: Vec<String> = file_cfg.required_jobs.clone().unwrap_or_default();
            let r_master_ci_gate = resolve_bool(false, file_cfg.master_ci_gate, false);
            let r_master_ci_timeout = resolve_val(None, file_cfg.master_ci_timeout_secs, 300);
            let r_doctor_enabled = resolve_bool(doctor_enabled, file_cfg.doctor_enabled, false);
            let r_r2_enabled = file_cfg.r2_enabled.unwrap_or(false);
            let r_r2_target_per_stratum = file_cfg.r2_target_per_stratum.unwrap_or(5);
            let r_r2_steady_state_p = file_cfg.r2_steady_state_p.unwrap_or(0.10);

            let r_suggested_models = file_cfg.suggested_models.unwrap_or_default();
            for (k, v) in &r_suggested_models {
                let valid_key = matches!(k.as_str(), "1" | "2" | "3" | "4" | "5");
                let valid_val = v.split_once('/').is_some_and(|(tier, effort)| {
                    serve::tier_to_model_id_pub(tier).is_some()
                        && (effort == "medium" || effort == "high")
                });
                if !valid_key || !valid_val {
                    return Err(QuorumError::Usage(format!(
                        "bad suggested_models entry: {k} = \"{v}\" (expected key 1-5, value \"tier/effort\")"
                    )));
                }
            }

            // Print the resolved config banner.
            let banner_text = banner(&BannerData {
                config_path: config_path_used.as_deref(),
                repo: &r_repo,
                repo_dir: &r_repo_dir,
                worktree_base: &r_wt,
                base_branch: &r_base_branch,
                cap: &r_cap,
                model: &r_model,
                effort: &r_effort,
                log_dir: &r_log_dir,
                no_bare_agent: &r_no_bare,
                self_update_drain: &r_self_update,
                drain_timeout_secs: &r_drain_timeout,
                max_turn_wall_secs: &r_max_turn_wall,
                max_task_wall_secs: &r_max_task_wall,
                idle_timeout_secs: &r_idle_timeout,
                max_turn_tokens: &r_max_turn_tokens,
                max_task_tokens: &r_max_task_tokens,
                max_turn_cost_usd: &r_max_turn_cost,
                max_task_cost_usd: &r_max_task_cost,
                merge_checks_timeout_secs: &r_merge_checks_timeout,
                required_jobs: &r_required_jobs,
                master_ci_gate: &r_master_ci_gate,
                master_ci_timeout_secs: &r_master_ci_timeout,
                doctor_enabled: &r_doctor_enabled,
            });
            eprintln!(
                "quorum serve: {}",
                banner_text.replace('\n', "\nquorum serve: ")
            );

            let db = paths::ensure_repo_dir(&r_repo.value)?;

            let merge_executor: std::sync::Arc<dyn serve::merge::MergeExecutor> =
                if let Some(cmd) = merge_cmd {
                    std::sync::Arc::new(serve::merge::CommandMergeExecutor {
                        command: cmd,
                        checks_cmd: merge_checks_cmd,
                        mergeability_cmd: merge_mergeability_cmd,
                    })
                } else {
                    std::sync::Arc::new(serve::merge::GhMergeExecutor {
                        token_file: r_merge_token.value.map(std::path::PathBuf::from),
                        gh_repo: resolve_gh_repo(&r_repo_dir.value),
                    })
                };

            let resolved_log_dir = Some(
                r_log_dir
                    .value
                    .map(std::path::PathBuf::from)
                    .unwrap_or_else(|| {
                        paths::home_dir()
                            .unwrap_or_else(|_| std::path::PathBuf::from(".quorum"))
                            .join("logs")
                    }),
            );
            let resolved_self_repo = if r_self_update.value {
                r_self_repo
                    .value
                    .or_else(|| resolve_gh_repo(&r_repo_dir.value))
            } else {
                None
            };

            let config = serve::ServeConfig {
                db_path: db,
                cap: r_cap.value,
                repo_dir: std::path::PathBuf::from(r_repo_dir.value),
                worktree_base: std::path::PathBuf::from(r_wt.value),
                names_file: r_names.value.map(std::path::PathBuf::from),
                agent_bin: r_agent_bin.value,
                model: r_model.value,
                effort: r_effort.value,
                merge_executor,
                bare_agent: !r_no_bare.value,
                limits: serve::CostLimits {
                    max_turn_tokens: r_max_turn_tokens.value,
                    max_task_tokens: r_max_task_tokens.value,
                    max_turn_cost_usd: r_max_turn_cost.value,
                    max_task_cost_usd: r_max_task_cost.value,
                    max_turn_wall_secs: r_max_turn_wall.value,
                    max_task_wall_secs: r_max_task_wall.value,
                    idle_timeout_secs: r_idle_timeout.value,
                },
                log_dir: resolved_log_dir,
                self_update_drain: r_self_update.value,
                drain_timeout_secs: r_drain_timeout.value,
                self_repo: resolved_self_repo,
                sha_poll_interval_secs: r_sha_poll.value,
                merge_checks_timeout_secs: r_merge_checks_timeout.value,
                merge_checks_poll_secs: r_merge_checks_poll.value,
                repo: r_repo.value,
                base_branch: r_base_branch.value,
                exit_when_gone: exit_when_gone.map(std::path::PathBuf::from),
                required_jobs: r_required_jobs,
                master_ci_gate: r_master_ci_gate.value,
                master_ci_timeout_secs: r_master_ci_timeout.value,
                allowed_tools: r_allowed_tools.value.map(|s| s.to_string()),
                doctor_enabled: r_doctor_enabled.value,
                r2_enabled: r_r2_enabled,
                r2_target_per_stratum: r_r2_target_per_stratum,
                r2_steady_state_p: r_r2_steady_state_p,
                suggested_models: r_suggested_models,
            };
            Ok(serve::run_serve(config)?)
        }
        cli::Command::Tail { agent, follow, raw } => {
            let conn = quorum_core::db::open(&paths::db_path()?)?;
            let entries = quorum_core::journal::list_in_flight(&conn)?;
            let entry = entries.iter().find(|e| e.agent == agent);
            let log_dir = entry.and_then(|e| e.log_dir.as_deref()).ok_or_else(|| {
                QuorumError::Usage(format!(
                    "no active session log for agent '{agent}' (agent unknown or no log dir)"
                ))
            })?;
            let stream_path = std::path::Path::new(log_dir).join("stream.jsonl");
            if !stream_path.exists() {
                return Err(QuorumError::Usage(format!(
                    "stream.jsonl not found at {}",
                    stream_path.display()
                )));
            }
            drop(conn);

            use std::io::{BufRead, BufReader, Seek, SeekFrom};

            let print_from = |file: &mut std::fs::File| -> std::io::Result<()> {
                let reader = BufReader::new(file.try_clone()?);
                for line in reader.lines() {
                    let line = line?;
                    if line.trim().is_empty() {
                        continue;
                    }
                    if raw {
                        println!("{line}");
                    } else if let Some(event) = serve::stream::parse_line(&line) {
                        if let Some(rendered) = serve::render::render_event(&event) {
                            print!("{rendered}");
                        }
                    }
                }
                Ok(())
            };

            let mut file =
                std::fs::File::open(&stream_path).map_err(|e| QuorumError::Io(e.to_string()))?;
            print_from(&mut file).map_err(|e| QuorumError::Io(e.to_string()))?;

            if follow {
                loop {
                    std::thread::sleep(std::time::Duration::from_millis(500));
                    let pos = file
                        .stream_position()
                        .map_err(|e| QuorumError::Io(e.to_string()))?;
                    let meta = file
                        .metadata()
                        .map_err(|e| QuorumError::Io(e.to_string()))?;
                    if meta.len() > pos {
                        file.seek(SeekFrom::Start(pos))
                            .map_err(|e| QuorumError::Io(e.to_string()))?;
                        print_from(&mut file).map_err(|e| QuorumError::Io(e.to_string()))?;
                    }
                }
            }

            Ok(0)
        }
        cli::Command::Perf { by, json } => {
            let cut = match by.as_deref() {
                None => quorum_core::perf::PerfCut::Default,
                Some("complexity") => quorum_core::perf::PerfCut::Complexity,
                Some("reviewer") => quorum_core::perf::PerfCut::Reviewer,
                Some(other) => {
                    return Err(QuorumError::Usage(format!(
                        "unknown --by value '{other}'; valid: complexity, reviewer"
                    )));
                }
            };
            let repo = paths::resolve_repo()?;
            let cfg_path = serve_config::default_config_path(&repo)?;
            let file_cfg = serve_config::load(&cfg_path, false)?;
            let default_model = file_cfg.model.as_deref().unwrap_or("sonnet");
            let default_effort = file_cfg.effort.as_deref().unwrap_or("high");
            let conn = quorum_core::db::open(&paths::db_path()?)?;
            let report = quorum_core::perf::perf(&conn, cut, default_model, default_effort)?;
            if json {
                output::emit(&report);
            } else {
                quorum_core::perf::render_table(&report);
            }
            Ok(0)
        }
        cli::Command::Classify {
            backfill,
            agent_bin,
            no_bare_agent,
        } => {
            if !backfill {
                return Err(QuorumError::Usage(
                    "classify requires --backfill (daemon handles live classification)".into(),
                ));
            }
            let db = paths::db_path()?;
            let mut conn = quorum_core::db::open(&db)?;
            let tasks = quorum_core::classify::tasks_missing_cx_all(&conn)?;
            if tasks.is_empty() {
                output::emit(
                    &serde_json::json!({"classified": 0, "message": "all tasks already have cx_est"}),
                );
                return Ok(0);
            }

            let batch_size = 20;
            let mut total_stored = 0;

            for chunk in tasks.chunks(batch_size) {
                let turn = serve::classifier::classifier_turn(chunk, &[]);
                let spec = serve::classifier::classifier_spec(
                    &std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from(".")),
                    !no_bare_agent,
                );

                let rt = tokio::runtime::Runtime::new()
                    .map_err(|e| QuorumError::Io(format!("tokio runtime: {e}")))?;

                let stored = rt.block_on(async {
                    let mut proc = serve::agent::AgentProc::spawn(&spec, agent_bin.as_deref())
                        .map_err(|e| QuorumError::Io(format!("spawn classifier: {e}")))?;

                    proc.feed_turn(&turn)
                        .await
                        .map_err(|e| QuorumError::Io(format!("feed_turn: {e}")))?;

                    let mut response_text = String::new();
                    let timeout_dur = std::time::Duration::from_secs(120);
                    let deadline = tokio::time::Instant::now() + timeout_dur;

                    loop {
                        let remaining = deadline - tokio::time::Instant::now();
                        match tokio::time::timeout(remaining, proc.next_event()).await {
                            Ok(Some(serve::stream::Event::Result {
                                result, is_error, ..
                            })) => {
                                if is_error.unwrap_or(false) {
                                    eprintln!(
                                        "classifier error for batch starting at #{}",
                                        chunk[0].id
                                    );
                                    break;
                                }
                                let text = result
                                    .as_str()
                                    .map(|s| s.to_string())
                                    .unwrap_or_else(|| result.to_string());
                                if !text.is_empty() {
                                    response_text = text;
                                }
                                break;
                            }
                            Ok(Some(serve::stream::Event::Assistant { message })) => {
                                if let Some(content) =
                                    message.get("content").and_then(|c| c.as_str())
                                {
                                    response_text.push_str(content);
                                }
                            }
                            Ok(Some(_)) => {}
                            Ok(None) => break,
                            Err(_) => {
                                eprintln!(
                                    "classifier timeout for batch starting at #{}",
                                    chunk[0].id
                                );
                                break;
                            }
                        }
                    }

                    proc.kill_and_reap().await;

                    if response_text.is_empty() {
                        return Ok(0usize);
                    }

                    if let Some(results) = serve::classifier::parse_response(&response_text) {
                        let now = quorum_core::clock::now();
                        quorum_core::classify::store_classifications(
                            &mut conn,
                            &results,
                            quorum_core::classify::CLASSIFIER_VERSION,
                            now,
                        )
                    } else {
                        eprintln!(
                            "failed to parse classifier response for batch starting at #{}",
                            chunk[0].id
                        );
                        Ok(0usize)
                    }
                })?;

                total_stored += stored;
            }

            output::emit(&serde_json::json!({
                "classified": total_stored,
                "total_tasks": tasks.len(),
            }));
            Ok(0)
        }
        cli::Command::ReviewInterpret {
            pr,
            repo,
            task_id,
            agent_bin,
            no_bare_agent,
            json,
        } => {
            // Manual re-run / backfill surface for the automatic post-merge
            // collector (see quorum/src/serve/collector.rs). Normal merged PRs
            // are collected by the daemon automatically; this command exists to
            // (a) backfill review_findings on PRs that merged before the
            // collector existed, and (b) retry when a collector run recorded
            // status='failed'. Both paths write through the same idempotent
            // pipeline — retries overwrite prior rows keyed on pr_number.
            let db = paths::db_path()?;
            let repo_dir = paths::git_toplevel()
                .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| ".".into()));

            let request = serve::collector::CollectionRequest::new(
                pr,
                task_id,
                repo.clone(),
                db.clone(),
                repo_dir,
                agent_bin.clone(),
                !no_bare_agent,
            );

            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| QuorumError::Io(format!("tokio runtime: {e}")))?;
            let outcome = rt.block_on(serve::collector::run_collection(&request))?;

            let mut conn = quorum_core::db::open(&db)?;
            let findings = quorum_core::review_findings::list_for_pr(&conn, pr)?;
            // #127: a successful CLI run consumes any pending daemon retry job
            // for the same PR — no need for the tick loop to re-run. Idempotent:
            // no row → no-op. Only clears on a real success; a failed manual
            // run leaves the row in place so the daemon retries with backoff.
            if outcome.status == quorum_core::review_findings::RunStatus::Success {
                let _ = quorum_core::review_interpret_jobs::delete(&mut conn, pr);
            }

            if json {
                output::emit(&serde_json::json!({
                    "pr": pr,
                    "status": match outcome.status {
                        quorum_core::review_findings::RunStatus::Success => "success",
                        quorum_core::review_findings::RunStatus::Failed => "failed",
                    },
                    "findings_count": outcome.findings_count,
                    "collector_model": serve::classifier::CLASSIFIER_MODEL,
                    "collector_version": serve::collector::COLLECTOR_VERSION,
                    "findings": findings,
                }));
            } else {
                eprintln!(
                    "PR #{}: {} findings stored (collector {} / {})",
                    pr,
                    outcome.findings_count,
                    serve::classifier::CLASSIFIER_MODEL,
                    serve::collector::COLLECTOR_VERSION,
                );
                for f in &findings {
                    let pushback = if f.author_pushback {
                        let accepted = match f.pushback_accepted {
                            Some(true) => " (accepted)",
                            Some(false) => " (overridden)",
                            None => "",
                        };
                        format!(" [pushback{accepted}]")
                    } else {
                        String::new()
                    };
                    let addressed = f
                        .addressed_status
                        .as_deref()
                        .map(|s| format!(" <{s}>"))
                        .unwrap_or_default();
                    eprintln!(
                        "  [{}/{}] {}{}{} (from {})",
                        f.kind,
                        f.severity.as_deref().unwrap_or("?"),
                        f.text,
                        addressed,
                        pushback,
                        f.source_endpoint,
                    );
                }
            }

            Ok(0)
        }
        cli::Command::Upgrade { check } => {
            let toplevel = paths::git_toplevel()
                .ok_or_else(|| QuorumError::Usage("not inside a git repository".into()))?;
            let skill_path = toplevel.join(".claude/skills/quorum/SKILL.md");
            let existing: String = std::fs::read_to_string(&skill_path).unwrap_or_default();
            if existing == EMBEDDED_SKILL {
                output::emit(&serde_json::json!({"ok": true, "status": "current"}));
                return Ok(0);
            }
            // Print unified diff to stderr (human-readable).
            for line in diff_lines(&existing, EMBEDDED_SKILL) {
                eprintln!("{line}");
            }
            if check {
                output::emit(&serde_json::json!({"ok": false, "status": "stale"}));
                return Ok(1);
            }
            if let Some(parent) = skill_path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| QuorumError::Io(e.to_string()))?;
            }
            std::fs::write(&skill_path, EMBEDDED_SKILL)
                .map_err(|e| QuorumError::Io(e.to_string()))?;
            output::emit(&serde_json::json!({
                "ok": true,
                "status": "upgraded",
                "path": skill_path.to_string_lossy(),
            }));
            Ok(0)
        }
        cli::Command::Help => {
            print!("{}", cheatsheet::cheatsheet());
            Ok(0)
        }
        cli::Command::InspectMessage { seq, raw } => {
            let conn = quorum_core::db::open(&paths::db_path()?)?;
            match quorum_core::inspect::get_message(&conn, seq, now)? {
                Some(msg) => {
                    if raw {
                        output::emit(&msg);
                    } else {
                        output::emit(&serde_json::json!({
                            "message": msg,
                            "as_of": now,
                            "retention": quorum_core::inspect::MESSAGES_RETENTION,
                        }));
                    }
                    Ok(0)
                }
                None => {
                    output::emit(&serde_json::json!({
                        "ok": false,
                        "reason": "not found",
                        "as_of": now,
                        "retention": quorum_core::inspect::MESSAGES_RETENTION,
                    }));
                    Ok(1)
                }
            }
        }
        cli::Command::InspectMessages {
            author,
            recipient,
            kind,
            topic,
            refs_contains,
            state,
            since_seq,
            before_seq,
            since_ts,
            before_ts,
            limit,
            raw,
        } => {
            if let Some(ref s) = state {
                if s != "live" && s != "expired" {
                    return Err(QuorumError::Usage(format!(
                        "--state must be 'live' or 'expired' (got '{s}')"
                    )));
                }
            }
            check_nonneg("--limit", limit)?;
            let read_limit = load_cfg()?.read_limit;
            let effective_limit = limit.unwrap_or(read_limit);
            let conn = quorum_core::db::open(&paths::db_path()?)?;
            let f = quorum_core::inspect::MessagesFilter {
                author: author.as_deref(),
                recipient: recipient.as_deref(),
                kind: kind.as_deref(),
                topic: topic.as_deref(),
                refs_contains: refs_contains.as_deref(),
                state: state.as_deref(),
                since_seq,
                before_seq,
                since_ts,
                before_ts,
                limit: effective_limit,
            };
            let msgs = quorum_core::inspect::list_messages(&conn, &f, now)?;
            if raw {
                output::emit(&msgs);
            } else {
                let bounded = msgs.len() as i64 >= effective_limit;
                output::emit(&serde_json::json!({
                    "messages": msgs,
                    "count": msgs.len(),
                    "bounded": bounded,
                    "as_of": now,
                    "retention": quorum_core::inspect::MESSAGES_RETENTION,
                }));
            }
            Ok(0)
        }
        cli::Command::InspectEvents {
            kind,
            subject,
            state,
            since_seq,
            before_seq,
            since_ts,
            before_ts,
            limit,
            raw,
        } => {
            if let Some(ref s) = state {
                if s != "live" && s != "expired" {
                    return Err(QuorumError::Usage(format!(
                        "--state must be 'live' or 'expired' (got '{s}')"
                    )));
                }
            }
            check_nonneg("--limit", limit)?;
            let read_limit = load_cfg()?.read_limit;
            let effective_limit = limit.unwrap_or(read_limit);
            let conn = quorum_core::db::open(&paths::db_path()?)?;
            let f = quorum_core::inspect::EventsFilter {
                kind: kind.as_deref(),
                subject: subject.as_deref(),
                state: state.as_deref(),
                since_seq,
                before_seq,
                since_ts,
                before_ts,
                limit: effective_limit,
            };
            let evts = quorum_core::inspect::list_events(&conn, &f, now)?;
            if raw {
                output::emit(&evts);
            } else {
                let bounded = evts.len() as i64 >= effective_limit;
                output::emit(&serde_json::json!({
                    "events": evts,
                    "count": evts.len(),
                    "bounded": bounded,
                    "as_of": now,
                    "retention": quorum_core::inspect::EVENTS_RETENTION,
                }));
            }
            Ok(0)
        }
        cli::Command::InspectErrors {
            source,
            state,
            since_id,
            before_id,
            since_ts,
            before_ts,
            limit,
            raw,
        } => {
            if let Some(ref s) = state {
                if s != "live" && s != "expired" {
                    return Err(QuorumError::Usage(format!(
                        "--state must be 'live' or 'expired' (got '{s}')"
                    )));
                }
            }
            check_nonneg("--limit", limit)?;
            let read_limit = load_cfg()?.read_limit;
            let effective_limit = limit.unwrap_or(read_limit);
            let conn = quorum_core::db::open(&paths::db_path()?)?;
            let f = quorum_core::inspect::ErrorsFilter {
                source: source.as_deref(),
                state: state.as_deref(),
                since_id,
                before_id,
                since_ts,
                before_ts,
                limit: effective_limit,
            };
            let errs = quorum_core::inspect::list_errors(&conn, &f, now)?;
            if raw {
                output::emit(&errs);
            } else {
                let bounded = errs.len() as i64 >= effective_limit;
                output::emit(&serde_json::json!({
                    "errors": errs,
                    "count": errs.len(),
                    "bounded": bounded,
                    "as_of": now,
                    "retention": quorum_core::inspect::ERRORS_RETENTION,
                }));
            }
            Ok(0)
        }
    }
}

fn diff_lines(old: &str, new: &str) -> Vec<String> {
    let mut out = Vec::new();
    let old_lines: Vec<&str> = old.lines().collect();
    let new_lines: Vec<&str> = new.lines().collect();
    out.push("--- installed".to_string());
    out.push("+++ embedded".to_string());
    // Simple line-by-line diff (good enough for a single small file).
    let max = old_lines.len().max(new_lines.len());
    let mut i = 0;
    while i < max {
        let ol = old_lines.get(i).copied().unwrap_or("");
        let nl = new_lines.get(i).copied().unwrap_or("");
        if ol == nl {
            out.push(format!(" {ol}"));
        } else {
            if i < old_lines.len() {
                out.push(format!("-{ol}"));
            }
            if i < new_lines.len() {
                out.push(format!("+{nl}"));
            }
        }
        i += 1;
    }
    out
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
    use super::{parse_ttl, resolve_repo_override, wait_child_stdout};

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

    #[test]
    fn parse_ttl_rejects_overflow() {
        // Unit multiply must not wrap (release) or panic (debug): `200000000000000d`
        // overflows i64 when multiplied by 86_400.
        assert!(parse_ttl("200000000000000d").is_err());
        // A bare value larger than the ceiling but still in i64 range is rejected too,
        // so no downstream `now + ttl` can overflow and break the claim invariant.
        assert!(parse_ttl(&i64::MAX.to_string()).is_err());
        assert!(parse_ttl(&(super::MAX_TTL_SECS + 1).to_string()).is_err());
    }

    #[test]
    fn parse_ttl_accepts_ceiling() {
        // The ceiling itself is valid (boundary), and so is a generous real-world TTL.
        assert_eq!(
            parse_ttl(&super::MAX_TTL_SECS.to_string()).unwrap(),
            super::MAX_TTL_SECS
        );
        assert_eq!(parse_ttl("30d").unwrap(), 30 * 86_400);
    }

    #[test]
    fn wait_child_stdout_returns_output_on_fast_exit() {
        let child = std::process::Command::new("echo")
            .arg("hello")
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let result = wait_child_stdout(child, std::time::Duration::from_secs(5));
        assert_eq!(result.unwrap().trim(), "hello");
    }

    #[test]
    fn wait_child_stdout_returns_none_on_failure() {
        let child = std::process::Command::new("sh")
            .args(["-c", "exit 1"])
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let result = wait_child_stdout(child, std::time::Duration::from_secs(5));
        assert!(result.is_none());
    }

    #[test]
    fn wait_child_stdout_kills_on_timeout() {
        let child = std::process::Command::new("sleep")
            .arg("60")
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let start = std::time::Instant::now();
        let result = wait_child_stdout(child, std::time::Duration::from_millis(500));
        let elapsed = start.elapsed();
        assert!(result.is_none(), "timed-out child must return None");
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "must not wait for the full sleep; elapsed: {elapsed:?}"
        );
    }

    #[test]
    fn resolve_repo_override_accepts_valid_slug() {
        assert_eq!(
            resolve_repo_override(Some("ag2trust/quorum")).unwrap(),
            "ag2trust/quorum"
        );
    }

    #[test]
    fn resolve_repo_override_rejects_malformed() {
        for bad in &["noslash", "too/many/slashes", "/leading", "trailing/", ""] {
            assert!(
                resolve_repo_override(Some(bad)).is_err(),
                "should reject {bad:?}"
            );
        }
    }
}
