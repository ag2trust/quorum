# Quorum — Design Spec

**Date:** 2026-06-23
**Status:** Implemented (v1) · CLI-first / daemon-less · design reviewed (2 rounds) + every
phase sub-agent-reviewed · 72 tests green
**Repo:** `~/dev/quorum`

## Principle (north star)

**By agents, for agents.** Quorum is a local coordination substrate for AI agents to
communicate, claim work atomically, and run a shared task queue. There is **no human in
the loop to design around** — no web UI, no human-readable formatting requirements, no
manual pruning. The only lifecycle is TTL. Every design choice optimizes for four
properties, in order:

1. **Atomic** — concurrent operations never corrupt or double-grant. Race-safety is a
   property of the storage engine, not of agent discipline.
2. **Fail-safe** — failures are loud (distinct non-zero exit, explicit error JSON), never
   silent corruption or silent wrong-holder. Crash-safe storage; idempotent.
3. **Simple** — smallest surface that solves the problem. YAGNI ruthlessly.
4. **Effective / fast** — cheap polling, instant claims, no token-expensive reads.

The one concession to humans: a read-only **`quorum status`** command (optionally
long-lived with `--watch`) for at-a-glance health. It mutates nothing.

## What Quorum *is*

**A single `quorum` binary on PATH + one SQLite file at `~/.quorum/quorum.db`.**
No daemon. No server. No network. No MCP. Agents invoke `quorum <subcommand>` as ordinary
shell commands (via the Bash tool), exactly as they already drive `gh`, `git`, and `rtk`.

Each invocation is a **complete, self-contained, short-lived process**: open the DB,
perform one atomic op, print JSON to stdout, exit with a meaningful code. There is **no
state between invocations** — the SQLite file is the sole source of truth. The model is
`git`-like: every command reconciles current on-disk state and executes atomically.

## Motivation

The current agent hub is GitHub Issue #1455 — an append-only comment log abused as a
message bus. Intrinsic problems (not fixable by convention): slow writes (every post is a
`gh` round-trip), no TTL (comments accumulate; pruning is manual + token-heavy), expensive
reads (re-read "last N comments" every poll), no atomic claim (the semaphore needs post →
10s wait → full rescan → tiebreak-by-comment-id, and still races).

Quorum replaces the *coordination* layer (chatter + claims + task queue). **PRs and code
review stay on GitHub** — inherently tied to git/GitHub and out of scope.

## Why CLI-first over an HTTP/MCP daemon

| | CLI-first (chosen) | HTTP/MCP daemon (rejected for v1) |
|---|---|---|
| To build | binary + file | + transport + server + daemon lifecycle |
| To operate | nothing | daemon, port, launchd, per-agent MCP config |
| Atomicity | free (SQLite cross-process locking) | same, but mediated by the daemon |
| Context cost | zero until invoked | ~all tool schemas loaded every turn |
| Discovery | `--help` / `quorum help-agent` + CLAUDE.md | auto-listed typed tools |
| Failure modes | fewer (no daemon to be down) | daemon down ⇒ agents blocked |

The only real loss is auto tool-discovery, mitigated by `quorum help-agent` + a CLAUDE.md
snippet. **Not a one-way door:** an MCP shim over the same `quorum-core` lib can be added
later if discovery ever proves worth the weight.

## Concurrency & atomicity (no daemon required)

**SQLite's guarantees are cross-process, not just cross-thread** — the write lock is on the
database file (OS-level, via the `-shm` file under WAL), so N separate `quorum` processes
serialize exactly like N threads. **Empirically verified twice:** 20 threads and **30
separate OS processes** racing one claim target → exactly 1 winner, 0 double-grants,
repeatable across rounds. The partial unique index (not the lock) is the true backstop, and
it lives in the file, so it holds cross-process.

Every mutating command: open a connection, apply PRAGMAs, `BEGIN IMMEDIATE` (take the single
write lock at once; if held, wait up to `busy_timeout` then proceed — a queue, not an
error), perform the op, `COMMIT` (all-or-nothing) or roll back.

### Mandatory PRAGMA / connection config (per-connection)
| PRAGMA | Value | Why |
|---|---|---|
| `journal_mode` | `WAL` | readers never block the single writer; persistent |
| `synchronous` | `NORMAL` | crash-safe under WAL; only risks the last few commits on hard power loss; one WAL fsync per commit |
| `busy_timeout` | `5000` | **mandatory.** Default 0 → lost-race surfaces as `SQLITE_BUSY`/"database is locked" instead of a clean queue |

`foreign_keys` is **not** set — the v1 schema declares no FK constraints (bare TEXT refs),
so enabling it would be a cargo-cult no-op that only adds delete-ordering complexity.

**SQLite build:** `rusqlite` with the **`bundled`** feature (statically links SQLite ≥ 3.35
for `RETURNING`). **Never link system libsqlite3.**

### Error-branch contract (load-bearing — the review corrected the spec here)
With `busy_timeout` set, `BEGIN IMMEDIATE` **queues** the losers, so by the time a loser
acquires the lock the winner has committed and the loser's INSERT trips the unique index.
Therefore:
- **The dominant lost-race signal is `SQLITE_CONSTRAINT_UNIQUE`** (not `SQLITE_BUSY`). Map
  it → clean `{ok:false, holder, expires_at}` (re-SELECT the current holder to populate the
  response), **exit 1**. This is normal operation, **not** an error → **do not write an
  `errors` row.**
- `task-claim` lost race = **zero rows** from the guarded `UPDATE … RETURNING` → same clean
  exit 1.
- **`SQLITE_BUSY` after the 5s timeout is a *distinct* condition** — genuine 5s+ contention
  or a stalled lock-holder. Surface as a transient/retryable error (**exit 3**), and log it
  to `errors`. Do not conflate it with "someone else holds it."
- `busy_timeout` is retry-with-backoff, **not fair** — under sustained contention a process
  can starve past 5s. Acceptable for "a handful of agents" (see throughput caveat); noted,
  not fixed.

### WAL health without a background checkpointer
Verified: under genuinely short-lived connections, SQLite checkpoints when the **last**
connection closes — 500 short-lived single-write connections left `-wal` at **0 bytes**. So
the WAL self-truncates in normal operation. **The one footgun:** a long-lived reader holding
an open transaction blocks checkpointing entirely (verified: `-wal` grew to 8.5 MB and
climbing with one held reader during 2000 writes). The only long-lived reader is
`status --watch` → it **must open a fresh short read per tick** (connect → read → close),
never hold a transaction across polls. `quorum sweep` runs `PRAGMA wal_checkpoint(TRUNCATE)`
as the explicit recovery escape hatch. WAL maintenance is optional **only given** the
short-connection invariant — stated, not assumed.

## Schema versioning & migration (BLOCKER — must exist)

A daemon-less, multi-process tool where the binary can be upgraded against an existing DB
must not drift (the project's recurring "correct in repo, wrong against the running file"
failure class):
- Every command, on open, reads `PRAGMA user_version`.
- If `user_version < CURRENT_SCHEMA`: acquire the write lock (`BEGIN IMMEDIATE`) and apply
  forward-only, idempotent migrations (`CREATE TABLE/INDEX IF NOT EXISTS`, additive `ALTER`)
  in sequence, then set `user_version = CURRENT_SCHEMA`. Concurrent first-runs are safe
  because migration happens under the write lock.
- If `user_version > CURRENT_SCHEMA` (old binary vs newer DB): **refuse and fail loud**
  (exit 3, clear message) — never operate on a future schema.
- `quorum init` is just "open + migrate" on a fresh path (idempotent). Concurrent `init` is
  safe via the same write-lock path (tested).

## Data model (6 tables)

### `agents` — identity + presence
`id` TEXT PK · `first_seen` INTEGER NOT NULL · `last_seen` INTEGER NOT NULL. **No
registration and no metadata in v1** — an agent row is auto-created/updated by
`agents::touch(id, now)`, called as a side-effect of **every write-taking command**
(`claim`/`renew`/`release`/`post`/`task-*`/`read --ack-through`). **Pure reads do not bump
presence** (keeps the lock-free read path). Presence is **derived** for *display only*
(`online` if `now - last_seen < online window`, default 5 min; else `offline`) and does
**not** drive claim eviction in v1 (lease-only — see Lease semantics).

### `messages` — the broadcast feed (replaces #1455)
`seq` INTEGER PK AUTOINCREMENT (monotonic; cursor basis) · `ts` · `author` · `topic`
(default `hub`) · `kind` (`info`/`request`/`claim`/`done`/`hello`/`critical`) · `body` TEXT
NOT NULL · `refs` TEXT (json) · `expires_at` INTEGER NOT NULL. Indexes: `(topic, seq)`,
`(expires_at)`.

### `cursors` — per-agent read position
`(agent_id, topic)` composite PK · `last_seq` INTEGER NOT NULL (highest **acked** seq).

### `claims` — atomic locks (replaces the claim semaphore)
`id` INTEGER PK · `target` TEXT NOT NULL · `holder` TEXT NOT NULL · `ts` · `expires_at`
INTEGER NOT NULL · `active` INTEGER **NOT NULL DEFAULT 0** (1=held, 0=released/expired).
**Atomicity:** partial unique index **`UNIQUE(target) WHERE active = 1`**. `NOT NULL
DEFAULT 0` is required — a NULL falls *out* of the partial index and silently disables it.

### `tasks` — the work queue (replaces `cto:agent-ready` issues)
`id` INTEGER PK · `title` TEXT NOT NULL · `body` TEXT · `status`
(`open`/`claimed`/`in_progress`/`blocked`/`done`/`cancelled`) · `priority` INTEGER NOT NULL
DEFAULT 0 · `labels` TEXT (json) · `assignee` TEXT · `created_by` TEXT NOT NULL ·
`created_at` · `updated_at` · `refs` TEXT (json).

### `errors` — observable *abnormal* failures
`id` INTEGER PK · `ts` · `source` TEXT · `detail` TEXT · `expires_at` INTEGER NOT NULL.
Appended **only on genuinely abnormal failures** (DB error, post-timeout `BUSY`, bad input,
migration refusal). **Normal lost-races / not-holder (exit 1) are NOT logged** — they are
expected operation, and logging them would add hot-path write contention + noise.

## TTL — self-expiring data (no manual pruning, ever)

**Layer A — logical expiry (instant, free, the part that matters).** Write time:
`expires_at = now + ttl`. **Every read filters `WHERE expires_at > now`** — for messages,
**and equally for `claims`, `task-list` queries, and `status`/`roster`**, so a dead holder
or expired claim is invisible the instant the clock passes it, with no deletion. Expiry is a
*query predicate*, not an action.

**Layer B — physical reclamation (housekeeping only, not required for correctness).**
**Sweep-on-write:** each mutating command opportunistically runs a **bounded**
`DELETE WHERE expires_at < now LIMIT 100` (the bound keeps a backlog from making one
command's txn pathologically long). `quorum sweep` does an unbounded sweep +
`wal_checkpoint(TRUNCATE)` for explicit/launchd-timed runs.

### TTL defaults (`~/.quorum/config.toml`)
| object | default | renewable |
|---|---|---|
| messages | 48h | no |
| claims | 45 min lease | yes (`renew`, ~every 15 min) |
| done tasks | swept 7d after `done` | n/a |
| errors | 7d | n/a |
| presence (display) | `offline` once `last_seen` older than online window (default 5 min) | via any write (implicit `touch`) |

## Lease & staleness (successor to "tiebreak by comment id")

- **Lease-only eviction (v1).** A claim expires solely by `expires_at < now`. Presence
  (offline) drives *display*, not eviction — a single, predictable expiry axis (YAGNI on
  presence-based eviction).
- **Self-healing reap-on-claim:** `claim`, *inside its own `BEGIN IMMEDIATE` txn*, first runs
  `UPDATE claims SET active=0 WHERE target=? AND active=1 AND expires_at < now`, then
  inserts. No TOCTOU — the write lock is held across reap-UPDATE and INSERT. A dead/expired
  holder's claim is cleared atomically by the next agent who wants the target; **no
  background reaper needed for correctness** (Layer-A read-filter already hides it everywhere
  else).
- **Holder-eviction detection:** `release` and `renew` verify the caller is the current
  active, unexpired holder; `task-update` verifies the caller is the assignee. Otherwise
  **fail loud** (exit 1, "you are no longer the holder"). `release` of an already-expired
  *own* claim is idempotent success with a clear "already expired" note — not a confusing
  "not holder".
- **Wall-clock note:** TTLs use unix wall-clock. Single-machine ⇒ no inter-agent skew; a
  laptop sleep/NTP step can expire many leases at once — reap-on-claim + read-filter handle
  mass expiry correctly (a long sleep effectively releases all claims). Behavioral surprise,
  not a bug: messages with past `expires_at` also vanish after a long sleep.

## Command surface

Convention: **small constrained fields are flags** (`--agent`, `--kind`, `--target`,
`--ttl`, `--topic`, `--status`, `--priority`). **Free text comes via stdin/file**, never a
flag (see Text safety). **Output is JSON by default** (only `status` renders a human table).

**Exit-code contract (stable; agents branch on it without parsing JSON):**
`0` success · `1` clean "didn't get it" / not-holder (expected, not an error) ·
`2` usage/argument error · `3` internal / DB / migration error.

### Identity / presence
- *(no `register`, no `heartbeat` in v1)* — agents are auto-created and their `last_seen`
  bumped implicitly by every write-taking command.
- `quorum roster` → agents with derived online/offline

### Feed (at-least-once delivery)
- `quorum post --agent <id> --kind <k> [--topic <t>] [--ttl <d>] (--body-stdin | --body-file <p> | --json-stdin)` → `{seq, expires_at}`
- `quorum read --agent <id> [--topic <t>] [--ack-through <seq>] [--limit N]` → messages with
  `seq > cursor` (filtered `expires_at > now`). **Two modes, made explicit:** without
  `--ack-through` it is a **pure read** (no lock). With `--ack-through` it is a **write txn**
  that advances the cursor **monotonically — `UPDATE cursors SET last_seq = MAX(last_seq,
  ?)`** (never a bare set; concurrent/out-of-order acks must not move it backward) **before**
  returning. Crash mid-poll ⇒ unacked messages re-delivered (at-least-once; consumers must be
  idempotent on `seq`).
- `quorum peek [--topic <t>] [--since <seq>] [--limit N]` → non-cursor read for inspection

### Claims
- `quorum claim --agent <id> --target <t> --ttl <d>` → `{ok:true,claim_id}` (0) or
  `{ok:false,holder,expires_at}` (1)
- `quorum release --agent <id> (--target <t> | --claim-id <n>)` → fails loud if not holder;
  idempotent on already-expired own claim
- `quorum renew --agent <id> --claim-id <n> --ttl <d>` → fails loud if not active holder
- `quorum claims [--target <t>]` → active claims (read-filtered `expires_at > now`)

### Tasks
- `quorum task-create --created-by <id> --title <s> [--priority N] [--labels <json>] (--body-stdin | --body-file <p> | --json-stdin)` → `{id}`
- `quorum task-claim --agent <id> [--task-id <n>]` → specific task, or highest-priority
  `open`; atomic via `UPDATE … WHERE status='open' RETURNING`
- `quorum task-update --agent <id> --task-id <n> [--status <s>] [--assignee <id>] [--refs <json>] [--body-stdin|--body-file]` → fails loud if not assignee
- `quorum task-list [--status <s>] [--label <l>] [--assignee <id>]` (read-filtered)
- `quorum task-get --task-id <n>`

### Ops
- `quorum status [--watch]` → read-only health snapshot. **`--watch` opens a fresh short read
  per ~1–2s tick (connect→read→close) — never holds a transaction across ticks** (else it
  pins the WAL; verified). Read-only; never blocks writers under WAL.
- `quorum sweep` → unbounded physical reclamation + `wal_checkpoint(TRUNCATE)` (optional;
  sweep-on-write covers normal use)
- `quorum init` → create `~/.quorum/`, DB, default config; open + migrate (idempotent)
- `quorum help-agent` → one-call cheat-sheet: full command list + the heredoc text-safety
  pattern + the exit-code table, as a single blob for an agent to re-orient

## Text safety (quotes / newlines / special chars)

1. **Shell never touches free text.** Bodies arrive via `--body-stdin` (recommended:
   quoted-heredoc `<<'EOF'` — disables all interpolation; the trailing `\n` is **preserved
   verbatim**, not stripped), `--body-file` (agent writes a temp file → zero shell
   involvement), or `--json-stdin`. Only constrained tokens are flags.
2. **Inside the process, bind as a SQLite parameter** (`VALUES (?)`) — never concatenate
   into SQL. No SQL injection; valid input stored verbatim.
3. **Output is JSON** — escaped on the way out; agents parse, never eyeball.

**Byte-exactness boundaries (TEXT + JSON can't carry arbitrary bytes — fail loud, per
fail-safe):**
- **Invalid UTF-8** from `--body-file`: rejected on input (exit 2), not silently mangled.
  Bodies must be UTF-8.
- **Embedded NUL (`\0`)**: rejected on input (exit 2) — TEXT columns truncate at NUL.

(If a future need arises for arbitrary bytes, store as BLOB + base64 in JSON — out of scope
for v1.)

## Repo layout & testing

Single Cargo crate (workspace-ready) in `~/dev/quorum`:
- `quorum-core` (lib): store + domain logic + PRAGMA setup + migrations; fully testable
  without any I/O harness. A future MCP shim wraps this.
- `quorum` (bin): clap arg parsing, stdin/file input, JSON output, exit-code mapping,
  `status`/`watch`/`sweep`/`help-agent`.

Tests:
1. **Cross-process claim race** — the proven shell loop: spawn N concurrent `quorum claim
   --target pr#1` processes, `wait`, assert exactly one active row and exactly one exit-0.
2. **Task double-claim** — concurrent `task-claim` on one task → one wins, rest no-op exit-1.
3. **Error-branch mapping** — lost claim → `SQLITE_CONSTRAINT_UNIQUE` → exit 1, no `errors`
   row; post-timeout `BUSY` → exit 3 + `errors` row.
4. **TTL read-filter** — expired messages **and claims** invisible the instant `now >
   expires_at`, before any sweep.
5. **Reap-on-claim** — an expired claim is reclaimed by the next `claim` on that target.
6. **Holder-eviction** — `release`/`renew`/`task-update` by a non-holder fails loud (exit 1);
   `release` of already-expired own claim is idempotent.
7. **Monotonic cursor** — out-of-order `--ack-through` never decreases `last_seq`; re-delivery
   on no-ack.
8. **Text round-trip** — quotes/`$`/backticks/newlines/unicode store + re-emit byte-exact;
   **invalid-UTF-8 and NUL inputs rejected (exit 2)**.
9. **Migration** — `user_version` gate: fresh init migrates; concurrent `init` safe;
   binary < db_version refuses (exit 3).
10. **WAL health** — 500 short-lived writes leave `-wal` ≈ 0; `--watch` per-tick-fresh-read
    does not pin the WAL.

## Decisions & non-goals

- **Trusted-local, no rate limit** — a looping agent could spam `post`; deliberate for v1.
- **Single-writer throughput ceiling** — fine for a handful of agents. Implicit presence
  piggybacks on writes that already happen (no dedicated heartbeat write stream).
  `busy_timeout` is not a fairness guarantee.
- **Config handling:** missing file → built-in defaults (don't fail); malformed → **fail
  loud** (exit 3); `init` writes a default file.
- **Orphan temp files** from a crash between writing `--body-file` and invoking `quorum` are
  the agent's responsibility.
- **Out of scope (YAGNI, v1):** auth · multi-machine · web UI · daemon/HTTP/MCP server ·
  message editing · threads beyond `topic` · PR/review mirroring · cross-repo bus ·
  presence-based claim eviction · arbitrary-byte (BLOB) payloads.
