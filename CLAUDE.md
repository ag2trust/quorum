# Quorum — Project Brief

**Quorum is a local coordination substrate for AI agents** — a single `quorum` binary + one
SQLite file (`~/.quorum/quorum.db`). Agents post messages, claim work atomically, and run a
shared task queue by invoking `quorum <subcommand>` as ordinary shell commands. It replaces
an earlier GitHub-Issue-based "hub" that was slow, never expired, and couldn't claim
atomically.

- **Design spec (source of truth):** `docs/2026-06-23-quorum-design.md` — read it before any
  non-trivial work. This file is the *operating brief*; the spec is the *design of record*.
- **Status:** pre-implementation (spec approved + twice-reviewed). No code yet.

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

A short-lived process per command: open DB → apply PRAGMAs → migrate-if-needed → one atomic
op (`BEGIN IMMEDIATE`) → print JSON → exit with a meaningful code. **No daemon, no server,
no network, no MCP.** The SQLite file is the only state. SQLite's cross-process file locking
is the concurrency authority. Crate layout: `quorum-core` (lib: store, logic, PRAGMAs,
migrations — fully unit-testable) + `quorum` (bin: clap, stdin/file I/O, JSON, exit codes).

## Load-bearing invariants (do NOT regress — each cost a review round to get right)

These are verified design decisions. Changing any of them needs the same scrutiny that
established it.

1. **Atomic claim = partial unique index, not application logic.**
   `UNIQUE(target) WHERE active = 1`, with `active INTEGER NOT NULL DEFAULT 0` (a NULL falls
   *out* of the partial index and silently disables protection). Claims/tasks are won inside
   a single `BEGIN IMMEDIATE` transaction. **Empirically proven:** 30 concurrent processes →
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
6. **TTL is logical-first.** `expires_at = now + ttl` at write; **every read filters
   `WHERE expires_at > now`** — messages *and* claims *and* `task-list`/`status`/`roster`.
   Expired data is invisible instantly; physical cleanup (bounded `DELETE … LIMIT 100`
   sweep-on-write, or `quorum sweep`) is housekeeping, not correctness.
7. **WAL self-truncates only with short-lived connections.** A long-lived reader holding an
   open transaction pins the WAL and it grows unbounded (verified: 8.5 MB and climbing).
   `status --watch` MUST open a fresh read per tick (connect→read→close), never hold a txn
   across polls. `quorum sweep` runs `wal_checkpoint(TRUNCATE)` as the escape hatch.
8. **Schema migration-on-open.** Read `PRAGMA user_version` on every command; apply
   forward-only idempotent migrations (`CREATE … IF NOT EXISTS`, additive `ALTER`) under the
   write lock; **refuse and fail loud (exit 3) if binary < db_version.** This is the defense
   against "correct in repo, wrong against the running file" drift (see Practices §3).
9. **Cursor advance is monotonic:** `SET last_seq = MAX(last_seq, ?)`, never a bare set
   (concurrent/out-of-order acks must not move it backward). Delivery is at-least-once;
   consumers must be idempotent on `seq`.
10. **Text safety.** Free text enters via stdin/file/json (never a flag), is bound as a
    SQLite parameter (never concatenated into SQL), and is emitted as JSON. **Reject invalid
    UTF-8 and embedded NUL on input (exit 2)** — TEXT+JSON cannot carry arbitrary bytes; fail
    loud rather than mangle.

## Quick start (fill in as the build lands)

```bash
cargo build --release            # produces target/release/quorum
cargo test                       # unit + integration; includes the N-process claim race
cargo clippy --all-targets -- -D warnings
cargo fmt --all
quorum init                      # create ~/.quorum/, DB, default config
quorum help --agent              # one-call cheat-sheet for agents
```

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
   step was skipped, say that.
6. **Grep before you code; copy working patterns.** Match the surrounding code's idiom,
   naming, and comment density rather than inventing a new style.
7. **No over-claims** — in docs, `--help`, or commit messages. Say what it does, not what it
   aspires to.
8. **Leave a learnings trail.** When a fix took >2 attempts, an owner correction changed
   direction, or a behavior contradicted expectation, capture the *fix/correct pattern* (not
   the debugging steps) — append it to this file's Gotchas or a `docs/learnings.md`, and
   include it in the PR. Aim to leave the project 1% better each session.

## Local-machine note: RTK compresses Bash output

This machine runs **RTK (Rust Token Killer)** as a global Claude Code hook — every `Bash`
command is transparently rewritten and its output is **filtered/compressed** (you see a
lossy summary, not raw output). `Read`/`Grep`/`Glob` bypass it. When you need the true,
complete output — especially **`cargo test` / `cargo clippy` results you'll paste as
verification evidence** — run it through `rtk proxy <cmd>` to get the raw, unfiltered output.
A short or "all-green" test summary may be RTK hiding the failures.

## Gotchas (Quorum-specific time-savers)

- The N-process claim-race test is the project's smoke alarm — keep it fast and in the
  default `cargo test` run.
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

## Where to read next

- **Design of record:** `docs/2026-06-23-quorum-design.md`
- Data model, full command surface, and the test matrix all live in the spec.
