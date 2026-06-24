# Quorum Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement
> this plan task-by-task (inline execution, per owner preference). Steps use checkbox
> (`- [ ]`) syntax for tracking.

**Goal:** Build `quorum` — a daemon-less, CLI-first, SQLite-backed local coordination tool
for AI agents (messages, atomic claims, task queue), per `docs/2026-06-23-quorum-design.md`.

**Architecture:** A single `quorum` binary + one SQLite file. Each subcommand is a
short-lived process that opens the DB, runs migrations-on-open, performs one atomic
`BEGIN IMMEDIATE` transaction, prints JSON, and exits with a stable code. Logic lives in a
`quorum-core` library (unit-testable without a process); the `quorum` binary is a thin CLI
shell (clap → core → JSON + exit code). Atomicity is SQLite's cross-process file locking.

**Tech Stack:** Rust (2021), `rusqlite` (feature `bundled`), `clap` (derive), `serde` +
`serde_json`, `thiserror`, `toml`, `directories`; dev: `assert_cmd`, `tempfile`,
`predicates`.

## Global Constraints

- **SQLite via `rusqlite` `bundled` only** — never link system libsqlite3 (need ≥ 3.35 for
  `RETURNING`).
- **Mandatory PRAGMAs on every connection:** `journal_mode=WAL`, `synchronous=NORMAL`,
  `busy_timeout=5000`. Do **not** set `foreign_keys`.
- **Every mutation runs inside `BEGIN IMMEDIATE`** (rusqlite: `TransactionBehavior::Immediate`).
- **Exit codes (stable):** `0` success · `1` clean "didn't get it"/not-holder · `2`
  usage/bad-input · `3` internal/DB/migration/BUSY-after-timeout.
- **`active INTEGER NOT NULL DEFAULT 0`** on `claims`; partial index `UNIQUE(target) WHERE
  active=1`.
- **Every read filters `expires_at > now`** (messages, claims, tasks, roster, status).
- **Cursor advance is monotonic:** `last_seq = MAX(last_seq, ?)`.
- **Free text** via stdin/file/json only (never a flag); bound as SQLite parameter; reject
  invalid-UTF-8 and embedded NUL (exit 2); output is JSON.
- **Normal lost-race / not-holder (exit 1) must NOT write an `errors` row.**
- **`SCHEMA_VERSION` constant** gates migrations; refuse (exit 3) if `db user_version >
  SCHEMA_VERSION`.
- **TDD:** failing test first, watch it fail, minimal impl, watch it pass, commit.
- **Time:** `quorum_core::clock::now() -> i64` (unix seconds). Tests inject explicit
  `expires_at`/`now` values rather than sleeping.

## Core library API (the contract every phase shares)

```rust
// quorum-core/src/lib.rs  — module map
pub mod clock;      // now() -> i64
pub mod error;      // QuorumError, exit_code()
pub mod db;         // open(), SCHEMA_VERSION, migrate(), pragmas
pub mod sweep;      // sweep_on_write(), sweep_all()
pub mod errlog;     // log_error()
pub mod agents;     // touch() [implicit presence], roster() — no register/heartbeat in v1
pub mod claims;     // claim(), release(), renew(), list()
pub mod tasks;      // create(), claim(), update(), list(), get()
pub mod feed;       // post(), read(), peek()
```

```rust
// error.rs
#[derive(thiserror::Error, Debug)]
pub enum QuorumError {
    #[error("not the current holder")] NotHolder,             // exit 1
    #[error("usage: {0}")] Usage(String),                     // exit 2
    #[error("bad input: {0}")] BadInput(String),              // exit 2 (utf8/nul)
    #[error("database busy after timeout")] Busy,             // exit 3
    #[error("db schema {db} newer than binary {bin}")]
    SchemaTooNew { db: i64, bin: i64 },                       // exit 3
    #[error(transparent)] Db(#[from] rusqlite::Error),        // exit 3
    #[error("io: {0}")] Io(String),                           // exit 3
}
impl QuorumError { pub fn exit_code(&self) -> i32 { match self {
    QuorumError::NotHolder => 1,
    QuorumError::Usage(_) | QuorumError::BadInput(_) => 2,
    _ => 3,
}}}
pub type Result<T> = std::result::Result<T, QuorumError>;
```
> Note: a *clean lost race* is NOT a `QuorumError` — it is `Ok(ClaimOutcome::Lost{..})` /
> `Ok(None)`; the CLI maps those to exit 1 without logging. Only genuine errors flow through
> `QuorumError`.

---

## Workflow per phase (local PR + sub-agent review)

Each phase below is one local "PR." Execute it like this:

1. `git switch -c feat/NN-slug` from up-to-date `main` (this repo's base branch is `main`).
2. Implement the phase's tasks (TDD, frequent commits).
3. Run `rtk proxy cargo test` + `rtk proxy cargo clippy --all-targets -- -D warnings` +
   `cargo fmt --all --check`; paste real output as the phase's verification evidence.
4. **Dispatch a review sub-agent** with the branch diff (`git diff main...HEAD`) and the
   spec; it returns BLOCKER/SHOULD-FIX/NICE findings.
5. Address BLOCKER/SHOULD-FIX with fix commits; re-run checks.
6. `git switch main && git merge --no-ff feat/NN-slug -m "merge: NN-slug (reviewed)"`.
7. Next phase.

---

# Phase 0 — Scaffold (`feat/00-scaffold`)

**Deliverable:** a building workspace with the lib+bin skeleton, JSON-output and exit-code
plumbing, the text-input helper, and a trivial `quorum --version`.

### Task 0.1: Cargo workspace

**Files:** Create `Cargo.toml`, `quorum-core/Cargo.toml`, `quorum-core/src/lib.rs`,
`quorum/Cargo.toml`, `quorum/src/main.rs`.

- [ ] **Step 1: Workspace manifest** — `Cargo.toml`:
```toml
[workspace]
members = ["quorum-core", "quorum"]
resolver = "2"

[workspace.package]
version = "0.1.0"
edition = "2021"
```
- [ ] **Step 2: `quorum-core/Cargo.toml`:**
```toml
[package]
name = "quorum-core"
version.workspace = true
edition.workspace = true

[dependencies]
rusqlite = { version = "0.31", features = ["bundled"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "1"

[dev-dependencies]
tempfile = "3"
```
- [ ] **Step 3: `quorum/Cargo.toml`:**
```toml
[package]
name = "quorum"
version.workspace = true
edition.workspace = true

[[bin]]
name = "quorum"
path = "src/main.rs"

[dependencies]
quorum-core = { path = "../quorum-core" }
clap = { version = "4", features = ["derive"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
toml = "0.8"
directories = "5"

[dev-dependencies]
assert_cmd = "2"
predicates = "3"
tempfile = "3"
```
- [ ] **Step 4:** `quorum-core/src/lib.rs` → `pub mod clock;` plus empty module files created
  in later tasks; for now `pub fn placeholder() {}`. `quorum/src/main.rs` → `fn main(){}`.
- [ ] **Step 5: Verify build.** Run `cargo build`. Expected: compiles.
- [ ] **Step 6: Commit.** `git add -A && git commit -m "chore: cargo workspace scaffold"`.

### Task 0.2: clock + error modules (TDD)

**Files:** Create `quorum-core/src/clock.rs`, `quorum-core/src/error.rs`; modify `lib.rs`.
**Produces:** `clock::now() -> i64`; `error::{QuorumError, Result}`.

- [ ] **Step 1: Failing test** — `quorum-core/src/clock.rs`:
```rust
pub fn now() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}
#[cfg(test)]
mod tests {
    #[test] fn now_is_after_2020() { assert!(super::now() > 1_577_836_800); }
}
```
- [ ] **Step 2:** Add `error.rs` exactly as in the Core library API block above.
- [ ] **Step 3:** `lib.rs`: `pub mod clock; pub mod error;`.
- [ ] **Step 4: Run** `cargo test -p quorum-core`. Expected: PASS.
- [ ] **Step 5: Commit** `feat: clock + error types`.

### Task 0.3: CLI plumbing — JSON output, exit-code mapping, text input helper (TDD)

**Files:** Create `quorum/src/output.rs`, `quorum/src/input.rs`; modify `quorum/src/main.rs`.
**Produces:** `output::emit<T: Serialize>(&T)`; `output::emit_err(&QuorumError)` (stderr
JSON); `input::read_text(src: TextSource) -> Result<String>` validating UTF-8 + rejecting
NUL; `TextSource::{Stdin, File(PathBuf), JsonField(...)}`.

- [ ] **Step 1: Failing test** — `quorum/src/input.rs`:
```rust
use quorum_core::error::{QuorumError, Result};
use std::io::Read;
use std::path::PathBuf;

pub enum TextSource { Stdin, File(PathBuf) }

fn validate(bytes: Vec<u8>) -> Result<String> {
    if bytes.contains(&0) { return Err(QuorumError::BadInput("embedded NUL".into())); }
    String::from_utf8(bytes).map_err(|_| QuorumError::BadInput("invalid UTF-8".into()))
}

pub fn read_text(src: TextSource) -> Result<String> {
    let bytes = match src {
        TextSource::Stdin => {
            let mut b = Vec::new();
            std::io::stdin().read_to_end(&mut b).map_err(|e| QuorumError::Io(e.to_string()))?;
            b
        }
        TextSource::File(p) => std::fs::read(&p).map_err(|e| QuorumError::Io(e.to_string()))?,
    };
    validate(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn rejects_nul() { assert!(validate(vec![b'a', 0, b'b']).is_err()); }
    #[test] fn rejects_bad_utf8() { assert!(validate(vec![0xff, 0xfe]).is_err()); }
    #[test] fn accepts_unicode_and_newlines() {
        assert_eq!(validate("héllo\n`$x`\n".as_bytes().to_vec()).unwrap(), "héllo\n`$x`\n");
    }
}
```
- [ ] **Step 2:** `output.rs`:
```rust
use quorum_core::error::QuorumError;
use serde::Serialize;
pub fn emit<T: Serialize>(v: &T) { println!("{}", serde_json::to_string(v).unwrap()); }
pub fn emit_err(e: &QuorumError) {
    eprintln!("{}", serde_json::json!({"error": e.to_string()}));
}
```
- [ ] **Step 3:** `main.rs` skeleton mapping a `Result<i32>` to `std::process::exit`:
```rust
mod input; mod output;
use quorum_core::error::Result;
fn run() -> Result<i32> { Ok(0) } // replaced by clap dispatch in later phases
fn main() {
    match run() {
        Ok(code) => std::process::exit(code),
        Err(e) => { output::emit_err(&e); std::process::exit(e.exit_code()); }
    }
}
```
- [ ] **Step 4: Run** `cargo test`. Expected: PASS.
- [ ] **Step 5: Commit** `feat: CLI output/input plumbing + text validation`.

**Phase 0 verification:** `cargo build && cargo test && cargo clippy --all-targets -- -D warnings`.

---

# Phase 1 — Store foundation + `quorum init` (`feat/01-store`)

**Deliverable:** connection w/ PRAGMAs, schema, `user_version` migration-on-open (refusing
newer DBs), sweep + errlog helpers, and a working `quorum init`.

### Task 1.1: PRAGMAs + open (TDD)

**Files:** Create `quorum-core/src/db.rs`; modify `lib.rs`.
**Produces:** `db::SCHEMA_VERSION: i64`; `db::open(path: &Path) -> Result<Connection>`
(applies PRAGMAs, runs `migrate`); `db::apply_pragmas(&Connection)`.

- [ ] **Step 1: Failing test:**
```rust
#[test] fn pragmas_are_set() {
    let dir = tempfile::tempdir().unwrap();
    let c = open(&dir.path().join("q.db")).unwrap();
    let jm: String = c.query_row("PRAGMA journal_mode", [], |r| r.get(0)).unwrap();
    assert_eq!(jm.to_lowercase(), "wal");
    let bt: i64 = c.query_row("PRAGMA busy_timeout", [], |r| r.get(0)).unwrap();
    assert_eq!(bt, 5000);
}
```
- [ ] **Step 2: Implement** `apply_pragmas` (busy_timeout via `busy_timeout` PRAGMA;
  `journal_mode=WAL`; `synchronous=NORMAL`) and `open` (call `Connection::open`,
  `apply_pragmas`, then `migrate`). Leave `migrate` as a stub `Ok(())` returning until 1.2.
- [ ] **Step 3: Run** `cargo test -p quorum-core db::`. Expected: PASS.
- [ ] **Step 4: Commit** `feat: db open + mandatory PRAGMAs`.

### Task 1.2: Schema + migration-on-open (TDD)

**Files:** modify `db.rs`; create `quorum-core/src/schema.sql` (embedded via `include_str!`).
**Produces:** `db::migrate(&Connection) -> Result<()>`.

- [ ] **Step 1: Failing tests:**
```rust
#[test] fn migrate_creates_tables_idempotently() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("q.db");
    { let _ = open(&p).unwrap(); }
    let c = open(&p).unwrap(); // second open must not error
    let n: i64 = c.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='claims'",
        [], |r| r.get(0)).unwrap();
    assert_eq!(n, 1);
    let v: i64 = c.query_row("PRAGMA user_version", [], |r| r.get(0)).unwrap();
    assert_eq!(v, SCHEMA_VERSION);
}
#[test] fn refuses_newer_db() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("q.db");
    { let c = open(&p).unwrap();
      c.pragma_update(None, "user_version", SCHEMA_VERSION + 1).unwrap(); }
    match open(&p) { Err(crate::error::QuorumError::SchemaTooNew{..}) => {}, _ => panic!() }
}
```
- [ ] **Step 2:** `schema.sql` — all 6 tables + indexes exactly per spec (agents, messages,
  cursors, claims with `active INTEGER NOT NULL DEFAULT 0`, tasks, errors; partial unique
  index `CREATE UNIQUE INDEX IF NOT EXISTS claims_one_active ON claims(target) WHERE
  active=1`; `messages(topic,seq)`, `messages(expires_at)`).
- [ ] **Step 3:** `migrate`: read `user_version`; if `> SCHEMA_VERSION` → `SchemaTooNew`; if
  `< SCHEMA_VERSION` → `BEGIN IMMEDIATE`, `execute_batch(schema.sql)` (all `IF NOT EXISTS`),
  set `user_version=SCHEMA_VERSION`, commit. `SCHEMA_VERSION = 1`.
- [ ] **Step 4: Run** `cargo test -p quorum-core`. Expected: PASS (both tests).
- [ ] **Step 5: Commit** `feat: schema + user_version migration-on-open`.

### Task 1.3: sweep + errlog helpers (TDD)

**Files:** Create `quorum-core/src/sweep.rs`, `quorum-core/src/errlog.rs`; modify `lib.rs`.
**Produces:** `sweep::sweep_on_write(&Connection, now, limit)`; `sweep::sweep_all(&Connection,
now)`; `errlog::log_error(&Connection, now, source, detail)`.

- [ ] **Step 1: Failing test** (insert an already-expired message, sweep, assert gone):
```rust
#[test] fn sweep_removes_expired() {
    let dir = tempfile::tempdir().unwrap();
    let c = crate::db::open(&dir.path().join("q.db")).unwrap();
    c.execute("INSERT INTO messages(ts,author,topic,kind,body,expires_at)
               VALUES (1,'a','hub','info','x',10)", []).unwrap();
    sweep_on_write(&c, 100, 100).unwrap();
    let n: i64 = c.query_row("SELECT count(*) FROM messages", [], |r| r.get(0)).unwrap();
    assert_eq!(n, 0);
}
```
- [ ] **Step 2:** Implement bounded deletes (`DELETE FROM <t> WHERE expires_at < ?
  ORDER BY ... LIMIT ?` — for SQLite use `WHERE rowid IN (SELECT rowid FROM <t> WHERE
  expires_at < ? LIMIT ?)`) across messages, errors, expired claims (`active=0` set then
  delete the inactive expired), and done tasks (`status='done' AND updated_at < now-ttl`);
  `sweep_all` is unbounded + `wal_checkpoint(TRUNCATE)`. `log_error` inserts best-effort
  (ignore failure — never panic the caller).
- [ ] **Step 3: Run** `cargo test -p quorum-core sweep::`. Expected: PASS.
- [ ] **Step 4: Commit** `feat: sweep + error-log helpers`.

### Task 1.4: `quorum init` wired through CLI (TDD, integration)

**Files:** Create `quorum/src/paths.rs` (resolve `~/.quorum/`), `quorum/src/cli.rs` (clap
tree), `quorum/tests/cli_init.rs`; modify `main.rs`.
**Produces:** clap `Cli`/`Command` enum; `paths::db_path()`, `paths::config_path()`.

- [ ] **Step 1: Failing integration test** — `quorum/tests/cli_init.rs`:
```rust
use assert_cmd::Command;
#[test] fn init_creates_db() {
    let home = tempfile::tempdir().unwrap();
    Command::cargo_bin("quorum").unwrap()
        .env("QUORUM_HOME", home.path())
        .arg("init").assert().success();
    assert!(home.path().join("quorum.db").exists());
}
```
- [ ] **Step 2:** `paths.rs`: honor `QUORUM_HOME` env override (used by tests), else
  `directories` → `~/.quorum/`. `cli.rs`: clap derive with `Command::Init`. `main::run`
  dispatches `Init` → `db::open(paths::db_path()?)` then emit `{"ok":true}`.
- [ ] **Step 3: Run** `cargo test -p quorum`. Expected: PASS.
- [ ] **Step 4: Commit** `feat: quorum init`.

**Phase 1 verification + review** per the per-phase workflow. Spec tests covered: migration,
concurrent-init (add a 2-process test invoking `init` simultaneously asserting both succeed
and one DB), PRAGMAs, refuse-newer.

---

# Phase 2 — Implicit presence + `roster` (`feat/02-presence`)

**Deliverable:** the `agents::touch` helper (auto-create + bump `last_seen`, called by every
write-taking command in later phases) and a read-only `roster`. **No `register`/`heartbeat`
in v1** — presence is implicit and display-only.

### Task 2.1: `agents` core ops (TDD)
**Files:** Create `quorum-core/src/agents.rs`; modify `lib.rs`.
**Produces:** `agents::touch(tx: &Transaction, id, now) -> Result<()>` (called *inside* a
caller's existing `BEGIN IMMEDIATE` txn — not its own — so presence is part of the same
atomic write); `agents::roster(&Connection, now, online_window) -> Result<Vec<AgentView>>`
where `AgentView { id, last_seen, online: bool }`.

- [ ] **Step 1: Failing tests:** `touch("A", now)` then `roster` shows `A` `online=true` when
  `now - last_seen < window`; a stale `last_seen` shows `online=false`; a second `touch`
  updates `last_seen` (not `first_seen`).
- [ ] **Step 2:** Implement `touch` as `INSERT INTO agents(id,first_seen,last_seen)
  VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET last_seen=excluded.last_seen`. `roster` computes
  `online = (now - last_seen) < window` in the SELECT (no write).
- [ ] **Step 3: Run** tests. PASS.
- [ ] **Step 4: Commit** `feat: agents touch (implicit presence) + roster`.

> **Cross-phase rule:** every write-taking CLI command in Phases 3–5 calls `agents::touch`
> **and** `sweep::sweep_on_write` inside its `BEGIN IMMEDIATE` txn, before the domain write.
> Pure reads (`read` w/o ack, `peek`, `roster`, `claims`, `task-list`, `status`) do neither.

### Task 2.2: CLI wiring (TDD integration)
**Files:** modify `cli.rs`, `main.rs`; create `quorum/tests/cli_presence.rs`.
- [ ] **Step 1: Failing test:** there is no write command yet, so drive presence through a
  trivial seam — a hidden `quorum __touch --agent A` test-only subcommand (or call
  `agents::touch` via a `#[cfg(test)]` path), then `roster` JSON contains `A` `online:true`.
  Prefer: defer the integration assertion to Phase 3 (claim bumps presence) and here test
  `roster` returns `[]` on an empty DB + the core `touch`/`roster` unit tests from 2.1.
- [ ] **Step 2:** Add subcommand `Roster` only. Emit via `output::emit`.
- [ ] **Step 3: Run** tests. PASS.
- [ ] **Step 4: Commit** `feat: quorum roster`.

---

# Phase 3 — Claims + the N-process race canary (`feat/03-claims`)

**Deliverable:** atomic `claim`/`release`/`renew`/`claims`, the load-bearing invariant test,
reap-on-claim, holder-eviction, error-branch mapping.

### Task 3.1: `claims::claim` with reap (TDD)
**Files:** Create `quorum-core/src/claims.rs`; modify `lib.rs`.
**Produces:** `claims::claim(&Connection, agent, target, ttl, now) -> Result<ClaimOutcome>`
where `ClaimOutcome { Won(Claim), Lost { holder, expires_at } }`; `Claim { id, target,
holder, expires_at }`.

- [ ] **Step 1: Failing tests:** (a) first claim → `Won`; (b) second claim same target by
  another agent → `Lost{holder:first}`; (c) after the first's `expires_at` passes, a new
  claim `Won` (reap-on-claim).
- [ ] **Step 2: Implement** in one `BEGIN IMMEDIATE` txn:
```rust
tx.execute("UPDATE claims SET active=0 WHERE target=?1 AND active=1 AND expires_at < ?2",
           params![target, now])?;
let res = tx.execute(
    "INSERT INTO claims(target,holder,ts,expires_at,active) VALUES (?1,?2,?3,?4,1)",
    params![target, agent, now, now + ttl]);
match res {
    Ok(_) => { let id = tx.last_insert_rowid(); tx.commit()?; Ok(Won(Claim{..})) }
    Err(rusqlite::Error::SqliteFailure(e, _))
        if e.code == rusqlite::ErrorCode::ConstraintViolation => {
        // someone holds it — read holder, return Lost (NOT an error)
        let (holder, exp) = tx.query_row(
            "SELECT holder,expires_at FROM claims WHERE target=?1 AND active=1 AND expires_at>?2",
            params![target, now], |r| Ok((r.get::<_,String>(0)?, r.get::<_,i64>(1)?)))?;
        Ok(Lost{ holder, expires_at: exp })
    }
    Err(e) => Err(map_busy(e)),  // SQLITE_BUSY -> QuorumError::Busy (exit 3)
}
```
- [ ] **Step 3: Run** tests. PASS.
- [ ] **Step 4: Commit** `feat: atomic claim with reap-on-claim`.

### Task 3.2: release / renew / list (TDD)
**Produces:** `release(&Connection, agent, ClaimSel, now)` (idempotent on already-expired own
claim; `NotHolder` if active claim held by someone else); `renew(&Connection, agent,
claim_id, ttl, now) -> Result<Claim>` (`NotHolder` if not active holder); `list(&Connection,
target: Option<&str>, now)` filtered `active=1 AND expires_at>now`.
- [ ] **Step 1: Failing tests:** release-by-nonholder → `NotHolder`; renew extends
  `expires_at`; renew-by-nonholder → `NotHolder`; list hides expired.
- [ ] **Step 2:** Implement (guarded `UPDATE ... WHERE ... holder=? AND active=1` → check
  `rows_affected`; 0 rows → `NotHolder` unless the row exists but expired → idempotent ok).
- [ ] **Step 3:** Run tests. PASS. **Step 4:** Commit `feat: claim release/renew/list`.

### Task 3.3: CLI wiring + error-branch → exit codes (TDD integration)
**Files:** modify `cli.rs`,`main.rs`; create `quorum/tests/cli_claims.rs`.
- [ ] **Step 1: Failing tests:** `claim --target t --ttl 45m` exit 0 + `{ok:true}`; second
  exit 1 + `{ok:false,holder}`; **assert exit 1 wrote NO errors row** (query via a `peek`-style
  debug or a follow-up `status`).
- [ ] **Step 2:** Parse `--ttl` (`45m`,`1h`,`30s` → seconds). Map `ClaimOutcome::Lost` →
  print `{ok:false,..}` and `std::process::exit(1)` (no errlog). `Won` → exit 0. `Busy` →
  errlog + exit 3.
- [ ] **Step 3:** Run tests. PASS. **Step 4:** Commit `feat: quorum claim/release/renew/claims`.

### Task 3.4: The cross-process race canary (TDD integration)
**Files:** Create `quorum/tests/race.rs`.
- [ ] **Step 1: Test** spawns N (=20) concurrent `quorum claim --agent aK --target pr#1`
  child processes against one temp `QUORUM_HOME`, waits, then asserts: exactly one exited 0,
  and `quorum claims --target pr#1` shows exactly one active row.
```rust
// pseudo: Vec of std::process::Command spawned, collect statuses, count success==0
```
- [ ] **Step 2:** (no impl — this validates Phase 3.) Run `rtk proxy cargo test -p quorum --test race`. Expected: PASS, exactly one winner.
- [ ] **Step 3: Commit** `test: cross-process claim race canary`.

---

# Phase 4 — Tasks (`feat/04-tasks`)

**Deliverable:** `task-create`/`task-claim`/`task-update`/`task-list`/`task-get`.

### Task 4.1: core ops (TDD)
**Files:** Create `quorum-core/src/tasks.rs`.
**Produces:** `create(...) -> Result<i64>`; `claim(&Connection, agent, task_id:
Option<i64>, now) -> Result<Option<Task>>` (None = nothing claimable / already taken);
`update(&Connection, agent, id, fields, now) -> Result<Task>` (`NotHolder` if not assignee);
`list(...)`, `get(...)` (read-filtered).
- [ ] **Step 1: Failing tests:** create→claim returns task & sets assignee; second claim of
  same id → `None`; claim with no id picks highest `priority` open; update by non-assignee →
  `NotHolder`.
- [ ] **Step 2: Implement** `claim` via `UPDATE tasks SET status='claimed',assignee=?,
  updated_at=? WHERE id=(SELECT id FROM tasks WHERE status='open' [AND id=?] ORDER BY priority
  DESC, id ASC LIMIT 1) RETURNING *` inside `BEGIN IMMEDIATE`; `None` if zero rows.
- [ ] **Step 3:** Run tests. PASS. **Step 4:** Commit `feat: tasks core`.

### Task 4.2: CLI wiring (TDD integration) — body via `input::read_text`.
- [ ] Steps mirror 2.2/3.3: subcommands, JSON out, sweep-on-write, exit codes (claim-miss →
  exit 1). Test concurrent `task-claim` on one task → one winner. Commit
  `feat: quorum task-* commands`.

---

# Phase 5 — Feed + cursor (`feat/05-feed`)

**Deliverable:** `post`/`read`(pure + monotonic ack)/`peek`.

### Task 5.1: core ops (TDD)
**Files:** Create `quorum-core/src/feed.rs`.
**Produces:** `post(&Connection, author, kind, topic, body, refs, ttl, now) -> Result<PostResult{seq,expires_at}>`;
`read(&Connection, agent, topic, ack_through: Option<i64>, limit, now) -> Result<Vec<Message>>`;
`peek(&Connection, topic, since: Option<i64>, limit, now) -> Result<Vec<Message>>`.
- [ ] **Step 1: Failing tests:** post returns increasing `seq`; `read` without ack returns
  unacked (`seq>cursor`, `expires_at>now`) and does NOT advance cursor; `read` with
  `ack_through=N` advances cursor to `MAX(last_seq,N)` and a backward ack does not regress it;
  re-`read` after ack returns only newer.
- [ ] **Step 2: Implement.** `read` ack path inside `BEGIN IMMEDIATE`:
  `INSERT INTO cursors(agent_id,topic,last_seq) VALUES(?,?,?) ON CONFLICT(agent_id,topic)
  DO UPDATE SET last_seq=MAX(last_seq, excluded.last_seq)`. Pure read path = plain SELECT (no
  txn).
- [ ] **Step 3:** Run tests incl. monotonic + byte-exact body round-trip (quotes/newline/
  unicode). PASS. **Step 4:** Commit `feat: feed post/read/peek + monotonic cursor`.

### Task 5.2: CLI wiring (TDD integration)
- [ ] Subcommands `post`(body via stdin/file/json), `read`, `peek`. Test: pipe a heredoc body
  with `"quotes"`, `$x`, backticks, newline → `read` re-emits byte-exact; **NUL/invalid-UTF-8
  file → exit 2.** Commit `feat: quorum post/read/peek`.

---

# Phase 6 — Ops & polish (`feat/06-ops`)

**Deliverable:** `status [--watch]`, `sweep`, `help-agent`, config loading.

### Task 6.1: `status` snapshot (TDD)
**Produces:** `quorum-core` read-only `stats(&Connection, now) -> Stats` (agents
total/online, messages live, active claims, tasks by status, error count + last N).
- [ ] **Step 1: Failing test:** seed rows; `stats` counts match (expired excluded).
- [ ] **Step 2:** Implement read-filtered counts. CLI `status` renders a human table by
  default; `--json` emits the struct.
- [ ] **Step 3:** Commit `feat: quorum status`.

### Task 6.2: `status --watch` fresh-per-tick + WAL-health test (TDD)
- [ ] **Step 1: Test (WAL health):** open a fresh DB, do 500 short-lived writes via the
  binary, assert `q.db-wal` size is ~0 after. Then assert `--watch` loop opens/closes per
  tick (unit: the tick fn takes no long-lived conn — assert by structure/no held `Connection`
  across iterations; document the invariant in code).
- [ ] **Step 2:** Implement `--watch`: loop { open → stats → close → clear+print → sleep
  1–2s }. Never hold a connection across iterations.
- [ ] **Step 3:** Commit `feat: status --watch (fresh per-tick)`.

### Task 6.3: `sweep`, `help-agent`, config (TDD)
- [ ] `sweep` → `sweep_all` + checkpoint; test it truncates a grown WAL. `help-agent` →
  prints the command list + heredoc text-safety pattern + exit-code table (static string;
  test it contains `--body-stdin` and `exit`). Config: `config::load(path) -> Config`
  (missing → defaults; malformed → `QuorumError` exit 3); test both. Commit
  `feat: sweep + help-agent + config`.

---

## Final integration pass (`feat/07-finalize`)

- [ ] Write `README.md` (install, the 12 commands, the heredoc pattern, exit codes).
- [ ] Write the CLAUDE.md "Quick start" numbers against the real binary.
- [ ] Full `rtk proxy cargo test` + clippy + fmt; paste output.
- [ ] Author a `quorum.sh` CLAUDE.md snippet for the *parent* repo (how agents call the hub)
      — but do NOT modify the parent repo here; just stage the snippet under `docs/`.
- [ ] Review + `--no-ff` merge.

---

## Self-review (spec coverage)

- agents/messages/cursors/claims/tasks/errors tables → Phases 1–6 ✅
- partial unique index + `active NOT NULL DEFAULT 0` → 1.2 ✅
- PRAGMAs + `BEGIN IMMEDIATE` → 1.1 + every mutation ✅
- migration-on-open + refuse-newer → 1.2 ✅
- error-branch (CONSTRAINT vs BUSY, no errlog on exit 1) → 3.1/3.3 ✅
- exit-code contract → 0.3 + each CLI task ✅
- logical TTL read-filter (all reads) + sweep-on-write + bounded delete → 1.3 + each phase ✅
- lease-only reap-on-claim + holder-eviction → 3.1/3.2 ✅
- monotonic cursor + at-least-once → 5.1 ✅
- text safety (stdin/file/json, param binding, UTF-8/NUL reject) → 0.3 + 5.2 ✅
- status[/watch] (fresh per tick), sweep, help-agent, config → Phase 6 ✅
- N-process race canary + WAL-health + concurrent-init tests → 3.4 / 6.2 / 1.4 ✅
