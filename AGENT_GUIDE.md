# Quorum — Project Brief

**Quorum is a local coordination substrate for AI agents** — a single `quorum` binary + one
SQLite file per managed repo (`~/.quorum/repos/<owner>__<name>/quorum.db`). Agents post
messages, claim work atomically, and run a shared task queue by invoking
`quorum <subcommand>` as ordinary shell commands. It replaces an earlier
GitHub-Issue-based "hub" that was slow, never expired, and couldn't claim atomically.

- **Design spec (source of truth):** `docs/2026-06-23-quorum-design.md` — read it before any
  non-trivial work. This file is the *operating brief*; the spec is the *design of record*.
- **Status:** implemented and shipping. Run `cargo test` for current module/test counts.
  `cargo test` passes; release binary verified end-to-end.

## Purpose & principle (north star)

**By agents, for agents.** There is **no human in the loop to design around** — no web UI,
no human-readable formatting requirements, no manual pruning. The only lifecycle is TTL.
Every choice optimizes for four properties, **in this priority order**:

1. **Atomic** — concurrent ops never corrupt or double-grant. Race-safety is a property of
   the storage engine, not of agent discipline.
2. **Fail-safe** — failures are loud (distinct non-zero exit + error JSON), never silent
   corruption or silent wrong-holder.
3. **Simple** — smallest surface that solves the problem. YAGNI ruthlessly.
4. **Effective / fast** — cheap polling, instant claims, no token-expensive reads.

When a decision trades these off, the higher-priority property wins. The one concession to
humans is a read-only `quorum status [--watch]`.

## Architecture in one breath

**CLI commands** are short-lived processes: open DB → apply PRAGMAs → migrate-if-needed →
one atomic op (`BEGIN IMMEDIATE`) → print JSON → exit with a meaningful code. The per-repo
SQLite file is the only state. SQLite's cross-process file locking is the concurrency
authority. DB path resolution: `QUORUM_REPO` env var (set by daemon for workers) > cwd git
detection > loud error (exit 2). **`quorum serve`** is the long-lived daemon that drives the
task lifecycle state machine: spawn workers/reviewers, process verdicts, merge PRs. One
daemon per DB, enforced via `daemon_lock` table (pid + heartbeat). Crate layout:
`quorum-core` (lib: store, lifecycle, PRAGMAs, migrations — fully unit-testable) + `quorum`
(bin: clap, stdin/file I/O, JSON, exit codes, serve daemon).

## Load-bearing invariants (do NOT regress — each cost a review round to get right)

These are verified design decisions. Changing any of them needs the same scrutiny that
established it.

1. **Atomic claim = partial unique index, not application logic.**
   `UNIQUE(target) WHERE active = 1`, with `active INTEGER NOT NULL DEFAULT 0` (a NULL falls
   *out* of the partial index and silently disables protection). Claims/tasks are won inside
   a single `BEGIN IMMEDIATE` transaction. **Empirically proven:** the committed canary races 20 concurrent processes →
   exactly one winner. The N-process claim-race test is the canary — if it ever flakes, stop
   and find out why before anything else.
2. **Mandatory PRAGMAs, per connection:** `journal_mode=WAL`, `synchronous=NORMAL`,
   `busy_timeout=5000`. Without `busy_timeout`, a lost race surfaces as "database is locked"
   instead of a clean queue. Do **not** set `foreign_keys` (no FKs in v1 — it'd be a no-op).
3. **`rusqlite` with the `bundled` feature. Never link system libsqlite3** (need ≥ 3.35 for
   `RETURNING`; bundled also keeps "one file, one binary" true).
4. **Error-branch contract.** With `busy_timeout` set, the *normal* lost-race signal is
   `SQLITE_CONSTRAINT_UNIQUE` (or zero rows from a guarded `UPDATE … RETURNING`), **not**
   `SQLITE_BUSY`. Map lost-race → clean `{ok:false, holder}` **exit 1**, and **do not write
   an `errors` row** (it's normal operation). A post-timeout `SQLITE_BUSY` is a *distinct*
   abnormal condition → **exit 3** + log to `errors`. Never conflate them.
5. **Stable exit codes** (agents branch on these without parsing JSON): `0` success · `1`
   clean "didn't get it" / not-holder (expected) · `2` usage/arg/bad-input error · `3`
   internal / DB / migration error.
6. **TTL is logical-first.** `expires_at = now + ttl` at write; every *expiring* table
   (**messages, claims, events, errors**) filters `WHERE expires_at > now` so expiry is
   instant. **Agents and tasks are NOT TTL'd** — agents have no `expires_at` column and
   never expire; tasks have no `expires_at` column and persist indefinitely (only `done`
   tasks older than the sweep TTL are physically reclaimed by `quorum sweep`/sweep-on-write).
   Physical cleanup (bounded `DELETE … LIMIT 100` sweep-on-write, or `quorum sweep`) is
   housekeeping, not correctness.
7. **WAL self-truncates only with short-lived connections.** A long-lived reader holding an
   open transaction pins the WAL and it grows unbounded (verified: 8.5 MB and climbing).
   `status --watch` MUST open a fresh read per tick (connect→read→close), never hold a txn
   across polls. `quorum sweep` runs `wal_checkpoint(TRUNCATE)` as the escape hatch.
8. **Schema migration-on-open.** Read `PRAGMA user_version` on every command; apply
   forward-only idempotent migrations (`CREATE … IF NOT EXISTS`, additive `ALTER`) under the
   write lock; **refuse and fail loud (exit 3) if binary < db_version.** This is the defense
   against "correct in repo, wrong against the running file" drift (see Practices §3).
   One-shot commands exit 3 as above; the long-lived `serve` loop instead catches
   `SchemaTooNew` at tick and exits 75 (`EXIT_SELF_UPDATE`) so the supervisor rebuilds and
   relaunches on a current binary rather than fail-looping (see `classify_tick_error`).
9. **Cursor advance is monotonic:** `SET last_seq = MAX(last_seq, ?)`, never a bare set
   (concurrent/out-of-order acks must not move it backward). Delivery is at-least-once;
   consumers must be idempotent on `seq`.
10. **Text safety.** Free text enters via stdin/file/json (never a flag), is bound as a
    SQLite parameter (never concatenated into SQL), and is emitted as JSON. **Reject invalid
    UTF-8 and embedded NUL on input (exit 2)** — TEXT+JSON cannot carry arbitrary bytes; fail
    loud rather than mangle.
11. **Single daemon per DB.** `quorum serve` acquires an exclusive lease in the `daemon_lock`
    table on startup (pid + heartbeat refreshed every tick). A second daemon on the same DB
    exits 2 if the holder is live (heartbeat < 30s AND pid alive); takes over if stale/dead.
    Lease released on clean shutdown. This replaces the safety that `instance_id` alone
    provided — instance_id scopes journal rows, daemon_lock prevents concurrent instances.

## Quick start

```bash
cargo build --release            # produces target/release/quorum
cargo test                       # includes the N-process claim race canary
cargo clippy --all-targets -- -D warnings
cargo fmt --all
./preflight.sh                   # all four PR gates (branch base + fmt + clippy + test)
./dev-install.sh                 # build + install to ~/.local/bin + verify + install git hooks
./dev-install.sh --verify-only   # just check the installed binary is current
quorum init                      # create ~/.quorum/, DB, default config (idempotent)
quorum help                      # one-call cheat-sheet for agents (alias: help-agent)
scripts/serve-supervisor.sh [flags]  # supervised serve: auto-rebuild on exit 75
```

**Supervised launch (recommended).** Use `scripts/serve-supervisor.sh` instead of bare
`quorum serve` when running with `--self-update-drain`. The supervisor catches exit 75,
runs `git fetch origin main` + `./dev-install.sh` to rebuild, and relaunches. Build
failures relaunch the old binary with a loud alert; non-75 exits propagate (no loop).
A thrash guard caps restarts at 6/hour. This replaces the former manual "rebuild binary
on quorum merges" standing duty.

**After pulling new source, always run `./dev-install.sh`** — it builds, replaces the
installed binary at `~/.local/bin/quorum`, and verifies that required subcommands (`sync`,
`init`, `status`) exist and the DB schema is current. The 2026-06-26 cutover stalled
because a stale binary at `~/.local/bin` lacked `sync`; this script prevents that (#74).

For toolchain-free installation from GitHub Releases (no cargo required), use `install.sh`.

Verified end-to-end (release binary): `init` → `claim` → `task-create`/`task-claim` →
`post`/`read` → `status` all return clean JSON / the status table, exit 0. See `README.md`
for the captured session.

## Engineering practices (inherited from the parent project, trimmed to what fits Quorum)

1. **All changes through branches → PRs. Never commit to `master`.** Conventional-commit
   subjects (`feat:`, `fix:`, `docs:`, `test:`, `chore:`). End commit messages with a
   `Co-Authored-By:` line for the working session.
2. **Plans & specs are committed, not local-only (HARD RULE).** A design that lives only on
   disk doesn't exist — the next session can't read it. Update the spec *in place* when the
   design changes; `master` should always reflect what's actually being built. The spec is at
   `docs/2026-06-23-quorum-design.md`.
3. **Validate against the running system, not just the repo.** Quorum's hardest bugs are
   exactly the ones a passing `cargo test` can miss: WAL growth under a held reader, schema
   drift between a new binary and an old DB, cross-process lock behavior. Before claiming an
   atomicity/storage change works, exercise it against a real `.db` with **concurrent
   processes** — not a single-threaded test. "Compiles + unit-green" is necessary, not
   sufficient.
4. **Fix root causes; don't patch around bad designs.** If a workaround is growing, step back
   and remove the complexity. Prefer forward-only, idempotent migrations over backfills
   (local DB state is disposable).
5. **TDD where it earns its keep; verification before completion always.** Write the failing
   test first for the atomicity/TTL/migration invariants (they're easy to get subtly wrong,
   hard to debug later). **Evidence before assertions:** never claim "passing"/"fixed"
   without pasting the actual command output. If tests fail, say so with the output; if a
   step was skipped, say that. This extends to diagnoses: any mechanism claim ("X happens
   because Y") must cite a `file:line` or a DB row, and any proposed fix must cite the code
   path where the behavior is missing — if you can't point at where it should be and isn't,
   you haven't read the path. Separate **Verified** (with citations) from **Hypothesis**
   (unproven) in every diagnosis; the 2026-07-14 "zombie worker" misdiagnosis shipped correct
   DB facts and an invented mechanism in one confidence tone (see Gotchas).
6. **Grep before you code; copy working patterns.** Match the surrounding code's idiom,
   naming, and comment density rather than inventing a new style.
7. **No over-claims** — in docs, `--help`, or commit messages. Say what it does, not what it
   aspires to.
8. **Leave a learnings trail.** When a fix took >2 attempts, an owner correction changed
   direction, or a behavior contradicted expectation, capture the *fix/correct pattern* (not
   the debugging steps) — append it to this file's Gotchas or a `docs/learnings.md`, and
   include it in the PR. Aim to leave the project 1% better each session.

## Agent workflow on this repo

These rules exist because each caused a real multi-hour stall this week (2026-06-26).

### 1. Author PRs as `ag2trust-dev`

Use the default `gh` identity (ag2trust-dev) for commits and `gh pr create`. **Never author
as `brevitize`** and never pass a token override. A brevitize-authored PR deadlocks: brevitize
(PR author) can't self-approve, and ag2trust-dev's approval doesn't count (it's the commit
co-author) — only `--admin` clears it, which requires owner intervention.

### 2. Two-account merge model

| Account        | Role                                    |
|----------------|-----------------------------------------|
| `ag2trust-dev` | Commit, push, open PR                   |
| (different session) | Review (post `### Code review`)   |
| `brevitize`    | `gh pr review --approve` + `gh pr merge`|

Self-merge is blocked at the **session** level: the PR footer `🤖 <Name>` + `Co-Authored-By`
trailers identify the author session. A reviewer must be a different session than the author —
and different from the **deliverer**: whoever signaled the task's `done` (e.g. adopted the PR)
is disqualified from reviewing it, even if the git author is someone else (#206).

**Verdict contract (#206):** reviews classify findings BLOCKING/advisory and the verdict is
derived, not chosen — `--verdict approved` requires `--blocking 0`; any blocking finding
requires `--verdict changes --feedback`. The daemon reviewer prompt carries the full
contract inline and invokes the builtin `review` skill. The daemon demotes unattested
approvals to `changes`.

### 3. Work in your own git worktree

Never edit the shared `~/dev/quorum` checkout directly — a second agent's `git checkout -b`
hijacks the first agent's working tree and can lose WIP. Instead:

```bash
git worktree add -b <branch> ~/dev/quorum-wt/<branch> origin/main
# work in ~/dev/quorum-wt/<branch>
# when merged:
git worktree remove ~/dev/quorum-wt/<branch>
```

Keep `~/dev/quorum` on `main` clean as the shared fetch target.

**This includes reviews.** When reviewing a quorum PR via `review-and-merge`, do NOT
`git checkout` the PR branch in `~/dev/quorum`. Use `gh pr diff --repo ag2trust/quorum`,
`git show origin/<branch>:<path>`, or a throwaway worktree instead. The CTO rebuilds
from this tree; leaving it on a feature branch builds the wrong code (observed 2026-06-28).

**Always branch from `origin/main` — never from another feature branch** (#114 shipped
another PR's commits this way). `preflight.sh` gate 1 and the pre-push hook verify this
mechanically: more than one distinct co-author session in `origin/main..HEAD` fails the gate.

### 4. Branch protection: don't rebase just to be current

"Require up-to-date before merging" is **OFF** — an approved + CI-green PR merges even if a
few commits behind `main`. Don't rebase solely to catch up. "Dismiss stale approvals on new
commits" stays **ON** — a real push (fix commit, rebase) invalidates the existing approval and
requires re-review.

### 5. Preflight gate — green BEFORE `quorum submit` (HARD RULE)

`submit` transitions the task to `in-review` and spawns a reviewer, so submitting with a
red branch burns a reviewer session on mechanical findings (#112/#114 cost ~5 reviewer
sessions in one week). The gate is mechanical, not judgment:

```bash
rtk proxy ./preflight.sh    # branch-base check + fmt + clippy + test, fail-fast
```

Paste the full output — it must end `PREFLIGHT: PASS` — in the PR body under
`## Verification`. No green preflight → no `submit`. The pre-push hook (installed by
`./dev-install.sh`) re-runs the cheap subset (`--quick`: branch base + fmt) on every
push; never bypass it with `--no-verify`.

### 6. Test quality bar (authors write to it, reviewers enforce it)

For every test you add, ask: **would this test fail if the feature broke?** If not, it
isn't a test. Specifically:

- No assertions on log-line presence alone — assert state/behavior (DB row, exit code,
  JSON field), not that something was printed.
- Lifecycle / state-machine code needs a **negative-path test**: the transition that
  must NOT happen (e.g. SIGINT must NOT mark a task done), not only the happy path.
- Concurrency claims need multi-process tests, run in a loop (see Gotchas — a single
  green run hides flakiness).

### 7. Async / tokio pitfalls (server & daemon milestones)

Known bug classes from the daemon/server work — treat as checklist items, not judgment
calls:

- **Never race stateful work inside `select!`.** Every branch must be cancellation-safe:
  when another branch wins, the losing future is dropped mid-await and any state it
  half-mutated is corrupt. Move stateful work outside the race or make it atomic.
- **Signals set flags; they don't cancel or complete work.** A SIGINT/SIGTERM handler
  records "shutdown requested" — it must never transition task state itself (the
  SIGINT-marks-done bug class).
- **Before `done` on any shutdown-path change:** enumerate every shutdown path × every
  task state and state what happens in each cell (in the PR body or a test). The
  unenumerated cell is where the bug lives.

### 8. Interactive owner-facing sessions are coordinator-only by default

When running as an interactive session with the owner (human), default to **coordinator
mode**: analyze, inspect, recommend, or file Quorum tasks — but do NOT edit files, create
branches/worktrees, commit, push, open/close PRs, merge, or implement changes.

Exploratory phrasing — "can we", "could we", "what about", "how should we" — authorizes
analysis and read-only inspection only. When Quorum is available, enqueue implementation
and review work as Quorum tasks rather than acting directly.

Implementation in the current interactive session is authorized only when the owner gives
an explicit, unambiguous directive to implement (e.g. "do it", "make that change",
"implement it here").

Read-only operations (status checks, diagnosis, code inspection, `quorum status`) remain
always allowed.

**Origin:** PR #365 was implemented directly after an interactive session read "can we
make it" as an implementation directive.

## Provider-specific guidance

Everything above applies equally to Claude and Codex. The sections below apply only to the
named provider.

### Claude-only: RTK compresses Bash output

This machine runs **RTK (Rust Token Killer)** as a global Claude Code hook — every `Bash`
command is transparently rewritten and its output is **filtered/compressed** (you see a
lossy summary, not raw output). `Read`/`Grep`/`Glob` bypass it. When you need the true,
complete output — especially **`cargo test` / `cargo clippy` results you'll paste as
verification evidence** — run it through `rtk proxy <cmd>` to get the raw, unfiltered output.
A short or "all-green" test summary may be RTK hiding the failures.

### Codex-only

No additional Codex-only project instructions currently.

## Gotchas (Quorum-specific time-savers)

- The N-process claim-race test is the project's smoke alarm — keep it fast and in the
  default `cargo test` run.
- **The `claude` CLI boundary is invisible to fake_agent — test it for zero tokens.** Two
  live respawn-loops in one day (2026-07-10) both failed *before* any API call: (1) a
  non-UUID `--session-id` is rejected at arg parsing ("Invalid session ID. Must be a valid
  UUID." — the daemon only sees "process exited without response"), (2) `--bare` strips
  operator credentials, so on subscription-auth machines a bare agent's every turn returns
  "Not logged in". Fix pattern: session ids come only from `agent::new_session_id()`; every
  spawn path (worker, reviewer, classifier, backfill) must thread the `bare_agent` config —
  never hardcode `bare`. Prevention pattern: the real-CLI contract tests in `agent.rs`
  (spawn the installed binary with `CLAUDE_CONFIG_DIR` → empty tempdir + blanked cred env
  vars: it reaches arg-parse and auth but can never reach the API — any stream event back
  means args parsed; event-less exit is the crash-loop signature). Debug pattern: the
  classifier's real error text isn't in daemon logs — read the newest session jsonl under
  `~/.claude/projects/<repo-slug>/`.
- `read --ack-through` is a **write** (it advances the cursor), so it takes the write lock
  like everything else — it is not a "pure read." Plain `read`/`peek` without ack are reads.
- **Presence is implicit and display-only.** There is no `heartbeat` or `register` command in
  v1. Every write-taking command calls `agents::touch` (auto-create + bump `last_seen`) inside
  its txn; pure reads do not. `online` is derived (`now - last_seen < window`) and never drives
  eviction (claims are lease-only).
- A normal "lost the race" (exit 1) is **not** a failure — don't log it to `errors`, don't
  treat exit 1 as a crash in scripts/tests.
- After a long laptop sleep, leases and messages with past `expires_at` vanish at once
  (read-filter). Expected behavior, not a bug.
- Config: missing file → built-in defaults (don't fail); malformed → fail loud (exit 3).
- **WAL setup under concurrent first-creation needs care** (cost a flaky test to find): set
  `busy_timeout` *before* the `journal_mode=WAL` switch, AND retry the WAL switch on transient
  `SQLITE_BUSY`/`SQLITE_LOCKED` — the busy-timeout handler does **not** cover journal-mode
  changes, so N processes creating the DB at once can fail the switch even with the timeout
  set. WAL is persistent, so the race only exists on the very first switch (`db.rs::set_journal_wal`).
  Always stress concurrency tests in a loop (`for i in $(seq 1 12)`); a single green run hides flakiness.
- **Expiry boundary must be consistent everywhere: a claim/row is DEAD iff `expires_at <= now`,
  LIVE iff `expires_at > now`.** A reviewer caught reap using `< now` while the read-filter used
  `> now`: at exactly `now == expires_at` the corpse blocked the unique index but was invisible to
  the re-SELECT → `QueryReturnedNoRows` → errlog'd exit 3 for a routine claim. Keep reap (`<=`),
  read-filter (`>`), and release/renew holder-checks (`>`) all agreeing on this boundary. The race
  canary now also asserts `errors` count == 0.
- Match the **extended** SQLite code (`SQLITE_CONSTRAINT_UNIQUE`), not the primary
  `ConstraintViolation`, when detecting a lost claim — so a future CHECK/NOT NULL violation fails
  loud instead of being misread as a lost race.
- **`dontAsk` mode denies edits to `.claude/**` paths** even when Edit is in `--allowedTools`
  and the worktree contains `.claude/`. The claude CLI treats `.claude/` as a protected
  namespace requiring explicit human approval. Tasks that edit `.claude/skills/**` or
  `.claude/settings.*` will zombie in dontAsk mode — the worker asks "can you grant
  permission?" to an empty room. Mitigations: (1) the idle watchdog (idle_timeout_secs,
  default 300s) reaps such zombies automatically, (2) task descriptions involving `.claude/`
  edits should be routed to a human-attended session or run with an operator that pre-grants
  the paths.
- **Raw `task-update --status done` bypasses lifecycle — use `quorum submit` or `quorum
  task-close`.** The ag2trust task#81 / PR#3659 incident: an agent ran
  `task-update --status done` to terminal-close a working P0 task, bypassing review entirely.
  The `done` status is now lifecycle-only (enforced by the CLI — `task-update --status done`
  exits 2). Three verbs replace it: `quorum submit --pr N` (worker/reviewer hand-off into
  the state machine), `quorum task-close --reason-stdin` (manual/external terminal close with
  distinct `task_closed_manual` audit event), and the `done` state itself (set only by the
  system after approve + merge). See `quorum help` for the canonical surface.
- **An agent shown in WORKING after its task merged is usually the R2 auditor, not a
  zombie.** `maybe_spawn_r2` deliberately spawns the shadow auditor *after* merge; the
  worker itself is killed synchronously in the merge-success path (`cleanup_slot` →
  `kill_and_reap`). Check `agent_runs.sub_role` (`r2`) and `end_reason` (`r2-done`) before
  diagnosing a leak — quorum task #116 tracks labeling these in the cockpit.

## Design notes & known limitations (v1)

Intentional behaviors and known gaps — not bugs, but write them down so they're not
rediscovered:

- **Agent names are caller-owned, first-use-wins.** There is no `register` and no name
  generator: `--agent <id>` is any free-form string, auto-created on first write. Uniqueness is
  the PK only — two sessions that pick the same id are treated as **one** agent and silently
  merge. Distinct-name discipline lives in the *caller's* convention, not the tool. (v2
  consideration: optionally enforce uniqueness / hand out names.)
- **Presence = "participated recently", not "succeeded recently".** Any *write-taking* command
  bumps `last_seen` **before** its outcome — so a lost `task-claim` or a not-holder `release`
  (both exit 1) still mark the agent online, because they took the write lock and ran `touch`.
  A *pre-write* usage error (e.g. invalid `--kind`, exit 2) does NOT register the agent. So:
  write-taking-any-outcome → online; usage/bad-input-rejected-pre-write → no trace.
- **Test gaps:** no property/fuzz tests; the name-collision merge is untested; `status --watch`
  (infinite loop) is only structurally verified, not run; renew-vs-claim concurrency is covered
  deterministically but not as a multi-process stress (claims has the 20-process canary,
  task-claim a 12-process one).

## Where to read next

- **Design of record:** `docs/2026-06-23-quorum-design.md`
- Data model, full command surface, and the test matrix all live in the spec.
