# Quorum — Design Spec

**Date:** 2026-06-23 (lifecycle refactor 2026-07-06, v2 boundary 2026-07-16, v2 correction 2026-07-17, merge-wait contract 2026-07-20, no-CI contract 2026-07-23, coding-runner boundary 2026-07-24, explicit-cancellation contract 2026-07-26)
**Status:** Implemented (v1) · CLI + daemon · lifecycle state machine (`lifecycle.rs`)
· v2 boundary specified (§ Daemon-only execution; corrected — supersedes PR #375)
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

`status --json` exposes task `title`, `provider`, `model`, and `effort` as separate,
additive fields for live, queue, blocked, and pipeline rows. Live and pipeline values
come from persisted `agent_runs`; queue/blocked rows without an explicit task tier use
the explicit `pending` marker because a read-only status process does not invent the
daemon's configured provider default. Complexity is task metadata, never a model value.

### Product boundary: opinionated Git delivery

Quorum is an opinionated local **agentic Git/GitHub coding pipeline**. It provisions
supported coding CLI runners as managed workers and reviewers inside this lifecycle:

```text
accepted task
  → isolated worktree and branch
  → coding worker
  → pushed pull request
  → independent R1 review
  → adversarial R2 review
  → rework when required
  → required checks
  → daemon-controlled approval and merge
```

This boundary is intentional. Quorum is **not** a general-purpose agent orchestrator,
model gateway, arbitrary workflow engine, or agent-provider plugin host. The task,
worktree, PR, review, rework, CI, and merge lifecycle is the product. A coding CLI is
an execution mechanism inside that fixed lifecycle; it does not define or extend the
lifecycle.

Quorum supports a small closed set of built-in coding runners. Adding one is a
deliberate product change implemented and tested in this repository, not dynamic
provider registration. Runner-specific capabilities must never weaken lifecycle
authority: workers and reviewers signal outcomes, while the daemon alone owns task
transitions, formal approval, and merge.

The approved product contract lives in the separate `quorum-pml` definition. PML
states observable delivery outcomes using the terms **Managed Task**, **Coding Run**,
**Proposed Change**, **Delivery Contract**, and **Supported Coding Runner**. Provider
names, model IDs, CLI flags, stream protocols, and telemetry shapes are implementation
and verification evidence, not normative product language.

## What Quorum *is*

**A single `quorum` binary on PATH + one SQLite file per managed repo.**
DB path: `~/.quorum/repos/<owner>__<name>/quorum.db`. Repo identity is resolved from
`QUORUM_REPO` env var (set by the daemon for workers) > cwd git detection (parse the
`origin` remote URL) > loud error (exit 2). No daemon required for CLI commands. No
server. No network. No MCP. Agents invoke `quorum <subcommand>` as ordinary shell
commands (via the Bash tool), exactly as they already drive `gh`, `git`, and `rtk`.

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

## Data model

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
(`open`/`working`/`in-review`/`rework`/`merging`/`done`/`failed`/`cancelled`) · `priority`
INTEGER NOT NULL DEFAULT 0 · `labels` TEXT (json) · `assignee` TEXT · `created_by` TEXT NOT
NULL · `created_at` · `updated_at` · `refs` TEXT (json) · `author` TEXT · `reviewer` TEXT ·
`rework_round` INTEGER NOT NULL DEFAULT 0 · `review_only` INTEGER NOT NULL DEFAULT 0 ·
`depends_on` TEXT (json array of task IDs).

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
- `quorum task-create --created-by <id> --title <s> [--priority N] [--labels <json>] [--depends-on <json>] (--body-stdin | --body-file <p> | --json-stdin)` → `{id}` (status: `open`)
- `quorum task-create ... --review-pr <N>` → review-only task (status: `in-review`,
  `review_only=true`, `refs.pr=N`). Skips `open`/`working` entirely.
- ~~`quorum task-claim`~~ — **Removed (PR #161).** Daemon claims internally via
  `quorum_core::tasks::claim`. The atomic claim primitive, branch allocation,
  dependency gating, and reviewer attachment are all preserved as internal functions.
- `quorum task-update --agent <id> --task-id <n> [--status open|cancelled] [--verdict approve|changes] [--blocking N] [--refs <json>] [--body-stdin|--body-file]` → fails loud if not assignee. Only `open` (release/reopen) and `cancelled` are directly settable; `working`, `in-review`, `rework`, `merging`, `failed` go through lifecycle events. **(v2: `--status` restricted to `cancelled` only; `--verdict`/`--blocking` removed — verdicts go through run-scoped `submit`. See § Daemon-only execution.)**
- `quorum task-close --agent <id> --task-id <n> --reason-stdin|--reason-file` → explicit
  manual/external terminal close (merged by hand, fixed elsewhere, obsolete). From any
  state except `done`/`cancelled` — `failed` is included, because a task whose PR landed
  outside the managed lifecycle has no other route to `done` and its dependents stay
  parked until it gets there (`compute_ready` counts only `done`). Reason REQUIRED.
  Sets `done` but emits `task_closed_manual` event
  (never `task_done`) — the audit log distinction is the guardrail. Owner/manual use;
  agents finishing work must use `quorum submit` (`quorum done` is a deprecated alias).
- `quorum task-retry --task-id <n> --by <operator>` → operator retry for a task
  durably parked after an automatic bounded failure. General daemon parks restore
  their recorded lifecycle stage as specified in § Explicit cancellation and durable
  parking, except that a parked merge restores `in-review` so a fresh approval can
  safely drive the next merge attempt. Provider/auth/quota/protocol parks atomically
  require and clear their
  provider-block marker. A provider-parked
  `working` task returns to `open`; a true `rework` task remains unassigned in
  `rework` and is atomically reattached through a dedicated replacement-worker
  claim. Both paths carry the exact persisted failed turn. `in-review` is
  rejected; Codex R1/R2 belongs to the later reviewer-provider phase.
  Before teardown, Quorum stores the pending raw prompt, turn kind, exact
  model/effort, and provider thread ID (when issued) in task refs. Provisioning
  reuses the task branch and PR, resumes that thread when present (fresh
  `exec` only before a thread exists), and never substitutes the generic
  initial task prompt. Provider events do not consume retry metadata. It is
  removed atomically with the successful worker submit transition into
  `in-review`, so any crash while implementation remains active restarts from
  the same durable turn.
  It never changes PR identity, approvals, dependencies, rework count, or author.
  Unblocked and terminal tasks return the clean-negative exit 1.
- `quorum task-list [--status <s>] [--label <l>] [--assignee <id>]` (read-filtered)
- `quorum task-get --task-id <n>`

### Ops
- `quorum status [--watch]` → read-only health snapshot. Alerts and critical messages are
  displayed and affect health only for 12 hours; they remain available through the feed until
  their normal message TTL expires. **`--watch` opens a fresh short read
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

## Per-repo DB model

Each managed repository gets its own SQLite database. The DB path is computed from the
repo slug: `~/.quorum/repos/<owner>__<name>/quorum.db` (e.g. `ag2trust__quorum`).

**Resolution order** for the repo identity:
1. `QUORUM_REPO` env var — set by the daemon for spawned workers/reviewers.
2. cwd git detection — parse `origin` remote URL from the enclosing git checkout.
3. Neither → exit 2 with a clear error ("set QUORUM_REPO or run inside a git checkout").

**`quorum serve` requires `--repo`** (mandatory, no default). The daemon injects
`QUORUM_REPO=<repo>` into every worker/reviewer it spawns, so their CLI calls resolve
to the same per-repo DB without relying on cwd.

### Single-daemon-per-DB guard

On startup, `quorum serve` acquires an exclusive lease in the `daemon_lock` table
(one-row, stores pid + heartbeat timestamp). The heartbeat is refreshed on every tick.
A second daemon on the same DB:

- **Live holder** (heartbeat fresh within 30s AND pid alive via `kill(pid, 0)`) → exit 2
  with error naming the holder pid. Never a silent second daemon.
- **Stale holder** (heartbeat old OR pid dead) → take over the lease, log it.

On clean shutdown the lease is released (row deleted). A crash leaves a stale row that
the next daemon takes over.

### Cutover recipe (lifecycle refactor)

The lifecycle refactor (parts 1–3) changed the task status vocabulary and eliminated
review tasks (`kind:review`) in favor of a single-task state machine. There is no data
migration — the per-repo DB is disposable.

1. Stop the running daemon: `kill <pid>` or Ctrl-C.
2. Delete the per-repo DB: `rm ~/.quorum/repos/<owner>__<name>/quorum.db*`
3. Rebuild and install: `./dev-install.sh`
4. Relaunch (recommended — supervised, with self-update):
   ```sh
   scripts/serve-supervisor.sh \
     --repo <owner>/<name> \
     --cap 4 \
     --self-update-drain \
     --names-file <path-to-names> \
     --repo-dir <path-to-checkout> \
     --worktree-base <path-to-worktrees>
   ```
5. Verify: `quorum status` from a git checkout shows the new schema version and
   no stale tasks.

## Task lifecycle state machine

**Source of truth:** `quorum-core/src/lifecycle.rs` — a pure function
`transition(TaskView, Event) → (Status, Vec<Effect>)` with no I/O.

### Status graph

```
open → working → in-review → merging → done
                     ↕                   ↑
                   rework ───────────────┘
                     ↓
                   failed (rework cap exceeded)

Terminals: done, failed, cancelled (cancelled is reachable from any non-terminal
only through an explicit outside request)
```

| Status | Wire format | Terminal | Meaning |
|---|---|---|---|
| Open | `open` | no | Unclaimed, available for work |
| Working | `working` | no | Claimed by a worker agent |
| InReview | `in-review` | no | Worker signaled done (PR posted), awaiting reviewer |
| Rework | `rework` | no | Reviewer requested changes; worker must fix and re-push |
| Merging | `merging` | no | Approved; merge in progress |
| Done | `done` | yes | Successfully merged |
| Failed | `failed` | yes | Work stopped fail-safe: rework cap exceeded or daemon parked after a bounded/unresolvable failure |
| Cancelled | `cancelled` | yes | Explicitly cancelled by the task creator or current assignee |

### Events

| Event | Payload | Trigger |
|---|---|---|
| `Claimed { agent }` | agent name | `task-claim` / daemon auto-pick |
| `SignaledDone { pr }` | PR number | `submit --pr N` (first delivery) |
| `ReviewerAttached { agent }` | reviewer name | `task-claim` on an in-review task |
| `VerdictApprove` | — | `submit --verdict approved --blocking 0` |
| `VerdictChanges` | — | `submit --verdict changes --feedback "..."` |
| `ReworkPushed` | — | `submit --pr N` when `rework_round > 0` |
| `MergeSucceeded` | — | Daemon after successful `gh pr merge` |
| `MergeFailed { reason }` | description | Daemon after merge failure |
| `MergeConflict` | — | Daemon: PR has conflicts with base branch |
| `LeaseExpired` | — | Lease reaper |
| `AgentFailed { reason }` | description | Worker/reviewer process died |
| `Cancelled { by }` | who | Explicit `task-update --status cancelled` caller request only |

### Effects

| Effect | Meaning |
|---|---|
| `SetAuthor { agent }` | Record who wrote the code |
| `SetReviewer { agent }` | Record who is reviewing |
| `SpawnReviewer` | Daemon provisions a new reviewer process |
| `ResumeWorker` / `ResumeReviewer` | Daemon feeds a new turn to the sticky agent |
| `MergePr { pr }` | Daemon initiates merge flow |
| `IncrementReworkRound` | Bump `rework_round += 1` |
| `NotifyOwner { reason }` | Alert the task creator |
| `ReleaseLease` | Deactivate the claims row |
| `PostFindingsNote` | Post findings as a task note (review-only terminal) |

### Transition table

**From Open:**
- `Claimed { agent }` → Working · effects: SetAuthor
- `Cancelled { by }` → Cancelled · effects: ReleaseLease

**From Working:**
- `SignaledDone { pr }` → InReview · effects: SpawnReviewer
- `AgentFailed` / `LeaseExpired` → Open · effects: ReleaseLease (+NotifyOwner on failure)
- `Cancelled { by }` → Cancelled · effects: ReleaseLease

**From InReview:**
- `ReviewerAttached { agent }` → InReview (stays) · effects: SetReviewer · **guard: agent ≠ author**
- `VerdictApprove` → Merging · effects: MergePr
- `VerdictChanges` → Rework · effects: IncrementReworkRound, ResumeWorker
- `VerdictChanges` (review_only=true) → Failed · effects: PostFindingsNote, ReleaseLease
- `VerdictChanges` (rework_round ≥ REWORK_CAP) → Failed · effects: NotifyOwner, ReleaseLease
- `AgentFailed` → InReview (**sticky**) · effects: ReleaseLease, NotifyOwner, SpawnReviewer
- `LeaseExpired` → InReview (**sticky**) · effects: ReleaseLease, SpawnReviewer
- `Cancelled { by }` → Cancelled · effects: ReleaseLease

**From Rework:**
- `ReworkPushed` → InReview · effects: ResumeReviewer
- `AgentFailed` / `LeaseExpired` → Open · effects: ReleaseLease (+NotifyOwner on failure)
- `Cancelled { by }` → Cancelled · effects: ReleaseLease

**From Merging:**
- `MergeSucceeded` → Done · effects: ReleaseLease
- `MergeFailed { reason }` → InReview · effects: NotifyOwner, ResumeReviewer
- `MergeConflict` → Rework · effects: IncrementReworkRound, ResumeWorker
  (at rework cap → Failed · effects: NotifyOwner, ReleaseLease)
- `Cancelled { by }` → Cancelled · effects: ReleaseLease

**Terminals (Done, Failed, Cancelled):** reject all events.

### Guards and policies

- **Author/reviewer separation:** ReviewerAttached is rejected if the agent is the author.
  The daemon enforces #206: the deliverer (who signaled `submit`) cannot review.
- **Rework cap:** `REWORK_CAP = 3`. When `rework_round >= 3` and VerdictChanges fires,
  the task goes to Failed (not Rework).
- **Review-only entry:** `task-create --review-pr N` creates a task directly in `in-review`
  with `review_only=true`. VerdictChanges on a review-only task goes to Failed (no worker
  to rework).
- **Sticky InReview:** reviewer crash/expiry does NOT leave InReview — the task stays and a
  new reviewer is spawned. Prevents review tasks from reverting to Open.
- **Resume semantics:** rework feeds a new turn to the existing worker (ResumeWorker);
  reviewer re-review feeds a new turn to the existing reviewer (ResumeReviewer).
- **Verdict attestation (#206):** `--verdict approved` requires `--blocking 0`; any blocking
  finding requires `--verdict changes --feedback`. Unattested approvals are demoted to
  changes by the daemon.
- **Dependency gating:** tasks with `depends_on` are only claimable when all deps are `done`.
- **Concurrency cap:** `--cap N` limits the daemon to N concurrent tasks (≤ 2N agents:
  one worker + one reviewer per task).
- **No passive execution (v2).** External/interactive agents cannot claim, execute,
  review, or submit tasks. The v1 passive-agent path (a `submit` mailbox row from an
  agent not in the daemon's spawn roster) is removed in v2 (see § Daemon-only execution).
  External agents that need work reviewed file `task-create --review-pr N` and the daemon
  handles it through the normal lifecycle.

### Explicit cancellation and durable parking

`cancelled` is reserved for an explicit outside request through
`task-update --status cancelled`. Only the task creator or current assignee may make
that request, only from a nonterminal state. The operation is terminal, releases the
lease, emits `task_cancelled`, and the daemon tears down any active worker or reviewer.
No daemon, lifecycle-recovery, watchdog, merge, provisioning, dependency, or sweep path
may emit `Event::Cancelled` or write `status='cancelled'`.

Automatic conditions that cannot safely continue use the existing `failed` status as
a durable parked state. Parking atomically:

- writes the complete cause to a task note and `task_parked` event;
- stores `daemon_parked=true`, `daemon_parked_reason`, and the authoritative
  `daemon_resume_status` in task refs;
- releases the lease and clears the assignee so no process remains authoritative;
- preserves task ID, PR and branch refs, dependencies, approvals, author/reviewer
  provenance, and rework count;
- excludes the task from automatic worker/reviewer provisioning.

This applies to exhausted crash recovery, repeated instant worker death, merge-policy
blocks, reviewer repository mismatch, reviewer provision exhaustion, and terminal
not-done dependencies. The dependency cascade parks the dependent with resume status
`open`; readiness remains false until all dependencies are `done`.

`quorum task-retry --task-id N --by <operator>` is the sole resume operation for a
daemon-parked task. It atomically validates the marker, clears it, resets only the crash
recovery counter, and emits `task_retry`. `open`, `rework`, and `in-review` restore
directly. A `rework` retry also records `daemon_rework_retry_requested=true`; startup
recovery preserves it and the next daemon tick atomically claims and spawns a replacement
worker on the same task and branch. A parked `merging` task restores to `in-review`
because the original approval mailbox row and agents were consumed during teardown;
the orphan-review reconciler obtains fresh R1/R2 approval before the next merge attempt.
Retry does not change PR identity, approvals,
dependencies, author/reviewer provenance, or rework count. An unparked or terminal task
is a clean negative (exit 1). This explicit gate prevents hot respawn/provision loops:
daemon ticks cannot retry a parked task until the operator requests it.

### Review responsibility boundary (agents own PR collaboration)

For PR-backed tasks, the GitHub PR is the source of truth for the review conversation:
findings (BLOCKING and advisory), advisory suggestions, author responses/pushback,
reviewer resolution of prior findings, and evidence. Quorum coordinates lifecycle,
provisioning, the final formal APPROVE, and merge — it does **not** proxy the review
conversation. Concretely:

- **Reviewer agents** post every blocking and advisory finding to the PR — inline
  comments where a specific file/line applies, review summary comments for
  cross-cutting findings — and respond to author pushback on the PR itself.
  Encouraged GitHub operations: normal comments, inline comments, review summary
  comments, and reviewer-owned `gh pr review --request-changes` (the reviewer's own
  durable GitHub record when the verdict is `changes`).
- **Author/rework agents** address findings on the PR. If disagreeing with a finding,
  the author replies to it on the PR with concrete evidence rather than silently
  ignoring it. The final PR history must let a later collector determine, for each
  finding: fixed, accepted, overridden with evidence, or unaddressed.
- **Lifecycle signal only:** reviewers signal state with
  `quorum submit --verdict approved|changes --blocking N [--feedback ...]`. The
  submit payload is a lifecycle signal, not a second review ledger — the ledger is
  the PR. The `--feedback` string is preserved as rework-turn context for the warm
  worker but is not the authoritative record.
- **Daemon retains:** the final formal `gh pr review --approve` (posted from the merge
  account) and `gh pr merge`. Reviewer-owned APPROVE and merge remain forbidden.
- **Daemon no longer mirrors** a reviewer's `changes` verdict into a duplicate generic
  GitHub REQUEST_CHANGES review. That mirror was redundant with reviewer-owned
  REQUEST_CHANGES and buried the reviewer's actual findings under a generic body.

This preserves #206 verdict attestation, reviewer separation, the rework cap, sticky
reviewer, the stale-SHA gate, and R1/R2 lifecycle. It shifts only who writes to the PR:
agents, directly.

### R2 pre-merge review gate (#159, configurable sampling)

By default, every PR requires both R1 and R2 approval for the same head SHA
before merge. R2 sampling is opt-in: absent config resolves to
`r2_enabled = true`, `r2_target_per_stratum = 0`, and
`r2_steady_state_p = 1.0`, which preserves mandatory R2 exactly. Operators may
lower `r2_steady_state_p` (for example, to 0.30) and optionally set a per-stratum
coverage floor. A negative floor or probability outside `0.0..=1.0` is a usage
error; values are never clamped. `r2_enabled = false` disables sampling, not the
R2 safety gate, and therefore also leaves R2 mandatory.

When R1 approves, Quorum records a sampling decision in a daemon-owned table,
keyed by both PR number and head SHA; it is not task refs because task refs are
agent-writable metadata and cannot authorize a merge-gate bypass. Later rework
heads append rather than overwrite earlier decisions. The seed is derived only
from those stable values; the persisted decision prevents a restart, advancing
coverage count, or force-push back to a prior head from changing that head's
review requirement. Missing, unreadable, or task-mismatched persisted state
fails closed to mandatory R2. R1 and R2 both bind their approval to the head
captured when their review starts. A force-push during an R1-only sampled
review discards that stale verdict before it can write an approval or sampling
decision; reconciliation provisions a fresh R1 for the new head rather than
parking or merging an unreviewed diff. When sampling skips R2, the normal R1
approval path merges; when it requires R2, the following dual-review flow applies:

1. **Durable R1 approval** — the daemon records R1's approval in the `approvals`
   table keyed by `(pr_number, review_role='r1')` with `head_sha` and `blocking=0`.
2. **R1 teardown** — R1 is torn down (end reason `r2-superseded`). Task stays InReview.
3. **R2 spawn** — R2 is spawned with a `ReviewCounterpart` built from the worker
   slot if available, or resolved from the PR head ref via GitHub (allowing R2 to
   proceed even without a live worker). R2's prompt frames it as an adversarial
   second reviewer that attempts to falsify the merge-safety claim, reviews
   independently before comparing against R1, and requires evidence-bound findings.
4. **Verdict flow** — R2's verdict drives lifecycle:
   - Approved → record R2 durable approval `(pr, 'r2')`. Merge proceeds only when
     `dual_approved(pr)` returns a common head SHA (both R1 and R2 approved with
     matching non-empty head SHAs and zero blocking findings).
   - Changes → fire VerdictChanges → invalidate both R1 and R2 approvals →
     rework → author pushes → ReworkPushed resumes R2 (not R1).
5. **Stale-SHA gate** — head SHA is recorded at each R1 and R2 spawn and refreshed
   on re-review. Before durable approval or sampling, the daemon rejects a verdict
   whose launch SHA no longer matches the live PR; before merge it checks again to
   cover a push during merge processing.
6. **Rework routing** — after R2-requested rework, `ReworkPushed` yields
   `ResumeReviewer` (not `SpawnReviewer`). The `r2_origin` flag on the slot
   ensures rework routes back to R2.
7. **REQUEST_CHANGES verification** — when any reviewer verdict is `changes` with
   blocking findings, the daemon verifies a GitHub REQUEST_CHANGES review exists
   on the PR. If not present, it posts one via `gh pr review --request-changes`.
8. **Remediation workers** — when a `changes` verdict arrives and no worker exists
   (review-only tasks, adopted PRs, dead workers), the daemon spawns a managed
   remediation worker with the existing PR, blocking findings, and task context.
   The lifecycle's review-only early-fail path is removed; all tasks go through
   normal rework (rework cap still enforced).

No new lifecycle states were added. R2 uses the existing `InReview ⇄ Rework` transitions.

**Severity contract** — both R1 and R2 prompts enforce that concrete failure classes
(resource exhaustion, unbounded growth, network calls in DB txns, data loss, stuck paths)
are BLOCKING unless evidence disproves the failure.

### Daemon merge flow

After VerdictApprove (InReview → Merging):
1. Check stale SHA — if reviewer recorded a head SHA and it differs from current, fire
   MergeFailed → rework cycle (prevents stale approval from authorizing a changed diff).
2. Check mergeability — if conflicting, MergeConflict → rework cycle (worker rebases).
3. Wait for CI checks — outcome classified into Ready / Failed / TimedOut. See
   § Merge-wait vs. actionable-rework contract (#173) below for the full disposition.
4. Persist approval record (instance-independent, survives restart).
5. **Pre-merge mergeability recheck (#153):** recheck PR mergeability immediately before
   the merge attempt — the window from step 2 through the master-CI gate can span minutes.
   If conflicting, fire MergeConflict → rework cycle. If mergeable, proceed.
6. Execute `gh pr merge` — success → Done; policy-blocked → Failed with a durable
   `merging` resume marker (explicit retry restores `in-review` and re-drives approval);
   retryable failure → rework.
7. Self-update drain: if enabled, a successful merge triggers drain mode →
   exit 75 for the supervisor to rebuild and relaunch.
8. **Post-merge analytics collector** (#125) — fire-and-forget `tokio::spawn` runs
   after `MergeSucceeded`. Analytics-only; can never mutate lifecycle, verdict, or
   merge outcome. See below.

**Post-conflict review requirement (#153):** after a MergeConflict → rework → push cycle,
the task transitions ReworkPushed → InReview, requiring a fresh review of the new head.
The stale-SHA check (step 1) ensures a prior approval for a different head cannot
authorize the merge — no approval is reused across conflict resolution.

### Merge-wait vs. actionable-rework contract (#173)

**Origin:** ag2trust task #156 / PR #3734 — both reviews approved, but GitHub Actions
jobs remained QUEUED with zero steps. `ChecksOutcome::TimedOut` was converted to
`MergeFailed` + `VerdictChanges` + rework. The absent worker recovery stranded the task
because infrastructure-pending CI consumed rework budget without giving the worker anything
actionable to fix.

**Principle:** a merge-wait outcome is either *actionable* (a worker can fix something)
or *infrastructure-pending* (nothing is broken in the PR; the platform hasn't finished).
Only actionable outcomes consume rework budget, emit VerdictChanges/AgentFailed, or
allocate an agent. Infrastructure-pending outcomes stay in `merging` with metadata and
retry autonomously.

#### ChecksOutcome disposition table

| ChecksOutcome | Mergeability post-wait | Classification | Action |
|---|---|---|---|
| `Ready` | any | actionable (proceed) | Continue to step 4 (persist approval) and merge |
| `Failed { checks }` | any | actionable (code broken) | `MergeFailed` → InReview, then `VerdictChanges` → Rework (rework cap applies). Worker gets failing check names. |
| `TimedOut` | `Conflicting` | actionable (conflict) | `MergeConflict` → Rework directly (rework cap applies). Worker rebases. |
| `TimedOut` | `Mergeable` | **infrastructure-pending** | **Durable merge-wait** — no VerdictChanges, no AgentFailed, no rework budget consumed. See retry/backoff below. |
| `TimedOut` | `AlreadyMerged` | resolved externally | `PrFoundMerged` → Done |
| `TimedOut` | `Closed` | resolved externally | `PrFoundClosed` → Failed |

The drain-interrupted timeout is a special case: the daemon preserves state (mailbox row
unconsumed, task stays `merging`) and returns `Ok(())` so restart recovery re-enters the
merge flow. This is unchanged.

#### No-CI contract (#181)

When GitHub returns a valid PR payload whose `statusCheckRollup` is a structurally valid
empty JSON array (`[]`), the repository may have no configured CI checks — or checks may
not have registered yet after a recent push (the ag2trust #3583 transient-empty race).

**Disambiguation via consecutive polls:** `parse_checks_json` returns `Option<Vec<...>>`
for checks — `Some(vec![])` for a valid empty array, `None` for a missing field,
non-array value, or malformed JSON. `checks_query_from_parsed` maps `Some(vec![])` →
`NoChecksConfigured` (distinct from `AllPassed`) and `None` → `Pending`.
`GhMergeExecutor::wait_for_checks` requires **2+ consecutive `NoChecksConfigured` polls**
before returning `Ready`. This protects against the transient-empty-after-push race: the
first empty poll sleeps `poll_interval_secs`, then re-queries. If checks have registered
by the second poll, the counter resets and normal waiting continues. If still empty, the
repo genuinely has no CI and the daemon proceeds to merge.

**Required jobs override:** if `required_jobs` is configured, `validate_required_jobs`
still gates on each named job — absent jobs from an empty rollup produce `NotReady`,
not `AllSucceeded`. The no-CI shortcut only applies to the general checks gate.

#### No new lifecycle state

The task remains in `merging` throughout infrastructure-pending waits. No new status is
added. The daemon holds blocker metadata in memory (or in the journal row's `agent_state`
field for restart recovery):

- `merge_wait_reason`: string — last observed blocker (e.g. "CI checks pending: 0 of 3
  completed after 300s")
- `merge_wait_started_at`: unix timestamp — when the first wait began
- `merge_wait_retries`: count — how many retry cycles have been attempted
- `merge_wait_next_poll_at`: unix timestamp — next scheduled retry

`quorum status` exposes these for observability: a `merging` task with
`merge_wait_reason` set displays the blocker inline. No additional CLI surface.

#### Bounded retry with exponential backoff

When a `TimedOut` + `Mergeable` outcome enters merge-wait:

1. **Increment `merge_wait_retries`** and compute the next poll delay:
   `delay = min(merge_checks_timeout_secs * 2^retries, merge_wait_max_interval_secs)`.
   Default `merge_wait_max_interval_secs = 1800` (30 min). The first retry re-polls after
   the configured `merge_checks_timeout_secs` (same as the initial wait).
2. **Owner alert** — post a `NotifyOwner` event on the first wait entry and again at
   each power-of-two retry (1, 2, 4, 8, ...) to avoid alert fatigue. The notification
   includes the PR number, retry count, and elapsed wall-clock time.
3. **Hard ceiling** — `merge_wait_max_retries` (default 48, ~24h at 30-min cap). When
   exceeded, fire `MergeFailed { reason: "merge-wait retry limit exceeded" }` and
   `VerdictChanges` → rework (or Failed if rework cap exceeded). This is the only path
   where infrastructure-pending eventually consumes rework budget.
4. **Early resolution** — on each retry poll:
   a. Re-query `check_mergeability`. If `AlreadyMerged` → `PrFoundMerged` → Done.
      If `Closed` → `PrFoundClosed` → Failed. If `Conflicting` → `MergeConflict` →
      Rework (actionable).
   b. Re-query `head_sha`. If head has moved since the reviewed SHA, the approval is
      stale — fire `MergeFailed` → InReview (reviewer re-reviews the new head). This
      is actionable: someone pushed to the branch.
   c. Re-query `wait_for_checks`:
      - `Ready` → exit wait, continue to step 4 (persist approval) and merge.
      - `Failed { checks }` → actionable rework (as above).
      - `TimedOut` again → stay in merge-wait, loop to step 1.

#### What merge-wait must NOT do

These are negative invariants — violation of any one is a regression:

1. **Must NOT emit `VerdictChanges`** — infrastructure-pending is not a reviewer verdict.
2. **Must NOT emit `AgentFailed`** — no agent has failed; the CI platform hasn't responded.
3. **Must NOT increment `rework_round`** — no rework is happening.
4. **Must NOT allocate or spawn a worker/reviewer** — nothing for them to do.
5. **Must NOT consume the mailbox row** until the wait resolves (merged, reworked, or
   ceiling-exceeded). The unconsumed row is the durable record that a merge is pending.
6. **Must NOT delete the approval record** until the merge attempt completes (success or
   failure). Premature deletion causes restart recovery to re-work instead of re-merge.

#### Preserved state during merge-wait

The following must remain intact throughout the wait and across restarts:

| State | Location | Why |
|---|---|---|
| PR number | `tasks.pr` column | Identifies the merge target |
| Both durable approvals (R1 + R2) | `approvals` table | Restart recovery reconstructs "merge this PR" |
| Reviewed head SHA | `approvals.approved_head_sha` | Head-change detection (step 4b above) |
| Branch provenance | `tasks.branch`, journal row | Worker/remediation needs the branch |
| Dependency blocking | `tasks.depends_on` | Already-done deps stay done; blocked deps stay blocked |
| Task status = `merging` | `tasks.status` column | Restart recovery recognizes this as merge-pending |
| Mailbox row (unconsumed) | `mailbox` table | Restart re-enters merge flow from unconsumed approval |

#### Restart reconciliation

On daemon startup, before stateless recovery:

1. **#228 approval recovery** runs first: scans `approvals` table, validates each role's
   verdict against the current PR head SHA via `next_missing_review_role(conn, pr, sha)`.
   If all roles approved for the current SHA → merge. If any role is missing or stale →
   defer to generic recovery (the approval is preserved or dropped per disposition).
2. **Generic recovery** handles `merging` tasks: stays in `merging` only when
   `dual_approved()` confirms all required roles are approved for the same head SHA.
   Incomplete approval (e.g. R1 approved, R2 missing) resets the task to `in-review` via
   `AgentFailed`, so the tick loop provisions the first missing role (#191).
3. **Phase 5b** (orphan in-review tasks) checks for existing valid R1 approvals: if R1 is
   approved for the current PR SHA, it spawns R2 directly instead of re-running R1 (#191).

Head-SHA invalidation on restart: the approval record stores `approved_head_sha`. On
re-entry, `head_sha()` is queried and compared. If different, the approval is stale —
`MergeFailed` fires and the task enters rework with a fresh review requirement. This
prevents a restart from merging code the reviewer never saw.

#### Actionable rework with no live worker

When an actionable outcome (Failed checks, MergeConflict, retryable merge failure) fires
VerdictChanges and the resulting status is `rework`, but no live worker exists for the
task, the daemon spawns a **remediation worker** (`spawn_remediation_worker` in
`serve/mod.rs`). The remediation worker:

- Gets the existing PR branch (resolved from GitHub via `resolve_pr_target`, which returns
  the authoritative head ref, SHA, and fork status; falls back to daemon branch convention
  when GitHub is unavailable)
- Gets the blocking findings / merge error as its rework prompt
- Is bounded by the same recovery policy as other workers (idle timeout, cost cap)
- Counts toward the rework cap (`rework_round` was already incremented by lifecycle)

If remediation worker provisioning fails (branch not found, worktree failure), the daemon
fires `AgentFailed { reason: "no worker for rework" }` which transitions the task back
to InReview (sticky) and re-spawns a reviewer on the next tick — not a silent strand.

#### Code paths (current implementation references)

| Concept | Location |
|---|---|
| `ChecksOutcome` enum (Ready/Failed/TimedOut) | `quorum/src/serve/merge.rs:67-74` |
| `MergeFailureKind` enum (Retryable/PolicyPending/PolicyBlocked) | `quorum/src/serve/merge.rs:16-24` |
| Checks-wait timeout handling (current: fires rework) | `quorum/src/serve/mod.rs:2073` |
| Failed-checks rework path | `quorum/src/serve/mod.rs:1932` |
| PolicyPending retry loop | `quorum/src/serve/mod.rs:2638` |
| Lifecycle transition table | `quorum-core/src/lifecycle.rs:147` |
| Durable approval persist (#228) | `quorum/src/serve/mod.rs:2358` |
| Approval recovery on restart | `quorum/src/serve/mod.rs:959` |
| Remediation worker spawn | `quorum/src/serve/mod.rs:6567` |
| Drain-interrupted merge-wait | `quorum/src/serve/mod.rs:2061` |

#### Review target resolution (#189)

All reviewer and remediation provisioning resolves the PR's authoritative head ref, SHA,
and fork status from GitHub via `resolve_pr_target` (`PrTarget` struct in `serve/mod.rs`).
Worker branch conventions (`daemon/{agent}-t{task_id}`) are fetch hints only — when GitHub
is available, the resolved ref is used instead. After worktree provisioning, HEAD is
verified against the resolved SHA; a mismatch aborts the reviewer launch. Fork PRs are
fetched via `refs/pull/<pr>/head` (`WorktreeManager::fetch_pr_and_provision`). When GitHub
is unavailable, provisioning falls back to the worker branch convention without SHA
verification.

#### Acceptance tests

Each invariant below requires both a positive and a negative test. Tests marked
**(restart-spanning)** must exercise daemon stop/restart across the boundary.

**Infrastructure-pending (merge-wait):**

1. `pending_checks_stay_in_merging` — checks return `TimedOut`, PR is `Mergeable`.
   Assert: task stays `merging`, no `VerdictChanges` event, no `AgentFailed` event,
   `rework_round` unchanged, no worker/reviewer spawned. Mailbox row unconsumed.
   Approval record preserved.

2. `pending_checks_resolve_on_retry` — checks return `TimedOut` then `Ready` on
   retry. Assert: task transitions to `done` via normal merge path. Approval record
   deleted after merge.

3. `pending_checks_retry_ceiling_triggers_rework` — checks return `TimedOut`
   `merge_wait_max_retries + 1` times. Assert: `MergeFailed` + `VerdictChanges`
   fired, `rework_round` incremented, worker gets rework turn.

4. `pending_checks_head_moved_during_wait` — checks return `TimedOut`, retry
   detects head SHA change. Assert: `MergeFailed` fired (stale approval),
   task enters InReview for fresh review. No rework budget consumed for the wait
   itself.

5. `pending_checks_conflict_during_wait` — checks return `TimedOut`, retry
   detects `Conflicting`. Assert: `MergeConflict` → Rework. This IS actionable
   and DOES consume rework budget.

6. `pending_checks_pr_merged_during_wait` — checks return `TimedOut`, retry
   detects `AlreadyMerged`. Assert: `PrFoundMerged` → Done.

7. `pending_checks_pr_closed_during_wait` — checks return `TimedOut`, retry
   detects `Closed`. Assert: `PrFoundClosed` → Failed.

**Restart-spanning:**

8. `restart_resumes_merge_wait` **(restart-spanning)** — daemon stops while in
   merge-wait. Restart finds approval record + task in `merging`. Assert: merge
   flow re-entered, checks re-polled, eventually merges when checks pass.

9. `restart_detects_stale_head` **(restart-spanning)** — daemon stops while in
   merge-wait. Before restart, head SHA changes (external push). Assert: restart
   detects stale approval, fires `MergeFailed`, does NOT merge.

**Actionable rework (existing behavior, preserved):**

10. `failed_checks_trigger_rework` — checks return `Failed { checks }`. Assert:
    `MergeFailed` + `VerdictChanges` fired, worker gets rework turn with check names,
    `rework_round` incremented.

11. `conflict_during_checks_triggers_rework` — checks return `TimedOut`, mergeability
    is `Conflicting`. Assert: `MergeConflict` → Rework, worker rebases.

12. `retryable_merge_failure_triggers_rework` — merge attempt fails with
    `MergeFailureKind::Retryable`. Assert: `MergeFailed` + `VerdictChanges` → Rework.

13. `rework_no_worker_spawns_remediation` — actionable rework fires but no live worker.
    Assert: remediation worker spawned on existing PR branch. If spawn fails,
    `AgentFailed` fires (task stays InReview, not stranded).

**Negative paths (must NOT happen):**

14. `merge_wait_does_not_consume_rework_budget` — `TimedOut` + `Mergeable` repeated
    N times (N < ceiling). Assert: `rework_round` == 0 throughout.

15. `merge_wait_does_not_allocate_agent` — same scenario. Assert: no worker or
    reviewer spawn events in daemon log.

16. `merge_wait_does_not_delete_approval` — same scenario. Assert: approval record
    present in DB after each retry cycle.

17. `drain_interrupted_merge_wait_preserves_state` — drain signal during checks wait.
    Assert: mailbox row unconsumed, task stays `merging`, approval record intact.
    (Existing test `drain_timeout_honored_during_merge_checks` covers the timing;
    this test covers state preservation.)

**No-CI paths (#181):**

18. `parse_checks_empty_rollup_is_no_checks_configured` — `statusCheckRollup: []`
    with `mergeStateStatus: BLOCKED`. Assert: `ChecksQueryResult::NoChecksConfigured`
    (not `AllPassed` or `Pending`). Unit test.

19. `required_jobs_absent_from_empty_rollup` — `required_jobs` configured but
    `statusCheckRollup` is `[]`. Assert: `NotReady`, not `AllSucceeded`. Unit test.

20. `parse_checks_no_rollup_field_is_pending` — `statusCheckRollup` absent from JSON.
    Assert: `ChecksQueryResult::Pending`. Unit test.

21. `parse_checks_non_array_rollup_is_pending` — `statusCheckRollup` is a string/null.
    Assert: `ChecksQueryResult::Pending`. Unit test.

22. Consecutive-polls guard — `GhMergeExecutor::wait_for_checks` requires 2+
    consecutive `NoChecksConfigured` before returning `Ready`. Protects against
    transient-empty-after-push (#3583). Verified by unit test structure; E2E
    coverage deferred (requires gh-shim, see #181 PR review discussion).

### Post-merge review-analytics collector (#125)

Every successful merge kicks off a detached `serve::collector::run_collection` task
that classifies the finished PR into structured `review_findings`. The collector
runs **after** `MergeSucceeded` fires — the task is already `done`, the verdict is
already final — so its results are retrospective analytics only. Nothing it does
can undo the merge or change the task.

**Pipeline:**
1. **Deterministic input assembly** (Rust code, not the model): fetches PR
   metadata, submitted reviews (`pulls/{pr}/reviews`), inline review comments
   with reply threads (`pulls/{pr}/comments`), conversation comments
   (`issues/{pr}/comments`), commits, `gh pr checks` summary, and diff stat.
   Each payload is capped at 64 KB. Task context (author, reviewer, rework
   round, agent runs, verdicts) is joined in from the local DB.
   - **List endpoints paginate:** reviews, review comments, issue comments, and
     commits are fetched with `gh api --paginate --slurp` so multi-page
     collections return every record as a single JSON array — GitHub cannot
     silently truncate at page 1. Single-object endpoints (`pulls/{pr}`) skip
     both flags.
   - **Repo targeting via `GH_REPO` env:** `--repo owner/name` overrides
     (from `quorum review-interpret --repo` or the daemon's per-repo context)
     are threaded into `GH_REPO` on the spawned `gh` child. `gh api` does not
     accept the `-R` shorthand and would exit 1 with `unknown shorthand flag: R`
     if it were passed. `gh pr view` / `gh pr checks` accept `-R` and retain it
     (belt + braces alongside `GH_REPO`).
   - **Fetch failures are loud:** if any sub-fetch errors (gh missing, HTTP
     failure, unauthenticated), the collector records a `failed` run and does
     NOT call `replace_for_pr` — prior good analytics are preserved verbatim
     until a subsequent successful run replaces them.
2. **Bounded classifier turn** — a Haiku-class agent (`CLASSIFIER_MODEL` /
   `CLASSIFIER_EFFORT`) is spawned with an EMPTY tool allowlist (no Bash,
   Read, Write, Edit, gh — response-only). 3-minute wall-clock cap.
3. **Structured output** — response must be `{"findings":[...]}` with each
   finding carrying `kind` (blocking/suggestion), `author_pushback`,
   `pushback_accepted` (true/false/null), `addressed_status`
   (addressed/unaddressed/partial/unclear), and an `evidence` array of
   `{kind,id}` pointers to GitHub review/comment ids. Prose-only findings
   are rejected by contract.
4. **Idempotent write** — `replace_for_pr` deletes and re-inserts all findings
   for the PR; `record_run` UPSERTs a single row in `review_collection_runs`
   keyed on `pr_number`. Retrying overwrites; there is no history bloat.
5. **Loud failure surface** — any pipeline error (fetch, classifier timeout,
   classifier `is_error=true`, unparseable response, DB write) records a
   `review_collection_runs` row with `status='failed'` + error text and logs
   an `errors` row (`source='review-collector'`). The task lifecycle is
   NEVER touched on failure.

**Boundary invariants:**
- The classifier cannot post to GitHub (no gh in allowed tools; empty tool list).
- The classifier cannot mutate DB rows other than through the collector's own
  post-parse writes.
- Collection is scoped to `pr_number`; concurrent collections of different PRs
  don't interfere.
- `collector_model` and `collector_version` are stamped on every row so future
  analyses can filter by generation and re-interpretation replaces atomically.

**Retry surface:** `quorum review-interpret --pr N [--task-id N] [--repo owner/name]`
re-runs the same pipeline manually. It calls `serve::collector::run_collection`
directly — the manual CLI path and the automatic post-merge path share one
ingestion implementation. Used for retrying recorded failures
(`SELECT * FROM review_collection_runs WHERE status='failed'`).

**No historical backfill (#157):** the daemon does NOT scan terminal tasks at
startup to infer missing interpretation jobs. Jobs enter the durable queue only
via the `MergeSucceeded` enqueue path. Already-enqueued rows from prior merges
are drained normally by the tick loop; historical tasks without a queue row are
left untouched.

**Prospective-only performance boundary (#158):** `quorum perf` only reports on
tasks that reached terminal status after the analytics rollout. The boundary is a
unix timestamp stored durably in the `perf_watermark` table (single row, id=1),
seeded once during the v27 schema migration and never updated. Tasks with
`updated_at < watermark` are excluded from the default report. `quorum perf --all`
bypasses the watermark for historical analysis. The boundary survives daemon
restarts and binary upgrades (persisted in SQLite, read on every `perf` call).
Historical collector artifacts (collection runs, findings, errors) created by a
prior backfill are retained as audit data but do not affect the default report.

## Built-in coding runners: Claude and Codex

**Date:** 2026-07-24
**Status:** Approved design; implementation pending.

### Decision and boundary

Quorum supports exactly two built-in coding runners in this design:

- `claude` preserves the existing persistent Claude Code stream-json behavior.
- `codex` uses the stable non-interactive Codex CLI JSONL interface.

This is a closed Rust enum, not a public provider trait or plugin API:

```rust
enum AgentKind {
    Claude,
    Codex,
}
```

The runner boundary exists only to keep coding-CLI process details out of the Git/PR
lifecycle. Supporting another runner requires an explicit code, test, configuration,
and design change.

The daemon gives each managed run exactly one task and role, an isolated worktree and
branch, a run-scoped `QUORUM_RUN_ID`, model and effort selection, role instructions,
and the environment required to invoke `git`, `gh`, and `quorum`.

A runner may start or continue one coding turn, deliver a prompt, expose observable
assistant text and tool activity, report authoritative terminal success or failure,
return available usage, and be killed and reaped as a process group. A runner may not
select work, change lifecycle state directly, formally approve or merge, mark a task
done, or redefine review policy.

### Internal run contract

Use the smallest normalized model consumed by the daemon:

```rust
struct AgentSpec {
    kind: AgentKind,
    executable: PathBuf,
    model: String,
    effort: String,
    worktree: PathBuf,
    environment: Vec<(String, String)>,
    session_id: Option<String>,
}

enum AgentEvent {
    SessionStarted { id: String },
    AssistantText { text: String },
    Activity { kind: ActivityKind, summary: String },
    TurnCompleted { usage: Option<TokenUsage> },
    TurnFailed { message: String },
}
```

Do not mirror either CLI's complete schema. Preserve each raw JSON line in
`stream.jsonl`, parse only fields Quorum consumes, render a compact normalized
transcript, and ignore unknown events without advancing lifecycle state.

`journal.session_id` becomes an opaque **runner continuation ID** while retaining its
column name for schema compatibility:

- Claude receives a Quorum-generated UUID before spawn.
- Codex issues a thread ID in `thread.started`; Quorum persists it before relying on
  continuation.

Missing required continuation identity is an abnormal startup failure. Assistant
prose is never task completion.

### Claude behavior remains stable

The Claude runner preserves the production contract: one persistent child,
bidirectional stream-json, Quorum-generated session UUID, stdin-fed later turns,
`dontAsk`, Claude-only `allowedTools`, optional Claude-only `bare`, and terminal
`result` events with tokens and optional cumulative USD cost.

Existing Claude command construction, event semantics, defaults, prompts, real-CLI
contract tests, and recovery behavior must remain unchanged during extraction.
Claude-only capabilities stay isolated and receive no Codex emulation:

- `--allowedTools`;
- `--bare`;
- PostToolUse activity hooks;
- Claude-native Skill invocation;
- stream-provided cumulative USD cost.

### Reliable Codex surface

Use only `codex exec`. Do not use the experimental app server, experimental exec
server, TypeScript SDK, internal rollout files, or ephemeral mode.

First turn, conceptually:

```text
codex exec --json --ask-for-approval never --sandbox <mode>
  --cd <worktree> --model <model>
  -c model_reasoning_effort=<effort> -
```

Continuation, conceptually:

```text
codex exec resume <thread-id> --json --ask-for-approval never
  --sandbox <mode> --model <model>
  -c model_reasoning_effort=<effort> -
```

Exact flag placement is an executable contract pinned by tests against the installed
real CLI without spending model tokens.

Codex is turn-oriented: one process runs one turn and exits; a later process resumes
the returned thread ID. Quorum must not force Codex into Claude's persistent-stdin
model.

Consumed events:

| Codex event | Normalized meaning |
|---|---|
| `thread.started` | `SessionStarted` with opaque thread ID |
| completed `agent_message` item | `AssistantText` |
| command, file-change, or MCP item activity | `Activity` |
| `turn.completed` | terminal success plus available usage |
| `turn.failed` | terminal failure |
| top-level fatal `error` | terminal failure unless a later success is permitted by the pinned CLI contract |

An item-level error is observable activity, not independently authoritative failure:
Codex has emitted non-fatal item errors followed by `turn.completed` and exit 0.

Codex success requires both `turn.completed` and successful process exit. Failure
includes `turn.failed`, fatal top-level error, non-zero exit without authoritative
completion, EOF before a terminal event, missing thread identity, or idle/wall-clock
timeout.

Process termination is a runner fact, not an independent task-lifecycle event. Before
an exited managed process may produce `AgentFailed`, the daemon uses one immediate
transaction to classify the task phase, current owner, and matching mailbox outcome.
This contract covers initial workers, remediation workers, R1, and R2. Worker ownership
uses the same authority as submission: current task assignee or an active daemon-issued
task-scoped worker capability (including replacement/remediation workers whose preserved
`author` names the original branch author). Reviewer ownership requires the task to
remain `in-review` with that reviewer attached:

- a pending submission or verdict retains the slot until the mailbox row is consumed;
- a consumed submission/verdict after the phase advances is completed cleanup-only;
- transferred ownership or an already-advanced task makes a stale process exit
  cleanup-only;
- only a run that still owns its phase without a current submission/verdict produces
  `AgentFailed` (or, for a blocked Codex worker, durable provider-block recovery).

Consumed mailbox history is not sufficient while the same sticky run owns a later
round: an old initial submission cannot excuse a missing rework push, and an old R1/R2
verdict cannot excuse a missing verdict after re-review. Exit status and provider stderr
are retained as diagnostics, never lifecycle authority. The daemon classifies before
using failure language or alerts. Cleanup records `completed` for a recorded outcome,
`ownership_transferred` for a stale run, `crashed` only for a genuine owner-without-
outcome failure, and `provider_blocked` only after durable Codex retry state is stored.

This classification is intentionally independent of observation order. In particular,
`submit → in-review → exit`, `submit → exit → in-review`, and
`exit → late submission recovery` must converge on one lifecycle transition and one
reviewer spawn; the same applies to R1/R2 verdicts and remediation submissions. Cleanup
must not emit a second `AgentFailed`, duplicate `task_in_review`/`task_rework`, release
the new owner's lease, or classify exit status 0 as a crash merely because the
turn-oriented provider process ended.

### Capabilities and safety limits

Capabilities are fixed internal facts, not a negotiation framework:

| Capability | Claude | Codex |
|---|---:|---:|
| resumable continuation | yes | yes |
| JSON event stream | yes | yes |
| token usage | yes | yes |
| stream-provided USD cost | yes | no |
| CLI tool allowlist | yes | no |
| provider-native review skill | optional | not required |

Never fabricate missing telemetry. Token, wall-clock, task-wall, and idle limits
continue when their data is observable. Codex does not expose reliable ChatGPT
subscription USD cost per turn. If a Codex daemon is configured with a USD safety
limit, startup fails loudly rather than ignoring it or failing every completed turn.

Use the minimum Codex sandbox proven by the full lifecycle canary. Begin validation
with `danger-full-access` because runs use git, GitHub CLI, Quorum, repository hooks,
builds, and managed worktree paths. Use `workspace-write` plus explicit writable paths
only if worker, review, rework, and merge-signaling canaries pass without exceptions.
Approval policy is always `never`; no human exists inside a managed run.

### Prompt composition

Prompts are the common Git delivery contract plus a worker/R1/R2 role contract plus a
small runner note. Complete task and verdict contracts remain inline; provider skills
are supplemental methodology, never lifecycle dependencies.

Workers must work only on the assigned task, branch, and worktree; implement and
verify the outcome; push and open or update the PR; signal through `quorum submit`;
and never merge or mark the task done.

Reviewers must inspect the full diff and relevant surrounding behavior, follow
repository instructions, classify BLOCKING and advisory findings, put authoritative
findings on the PR, submit a matching verdict, and never formally approve, merge, or
review their own delivery.

Claude may invoke its built-in review skill. Codex follows `AGENTS.md` and available
Codex skills, but Quorum does not require a particular built-in skill. Shared prompts
say "repository instructions" instead of `CLAUDE.md`.

### Configuration and model routing

Runner selection is explicit and defaults to Claude for compatibility:

```toml
agent = "claude"
agent_bin = "claude"
model = "claude-opus-4-7"
effort = "high"
```

Codex:

```toml
agent = "codex"
agent_bin = "codex"
model = "gpt-5.6-terra"
effort = "high"

[codex]
sandbox = "danger-full-access"
ignore_user_config = false
```

Never infer runner kind from the executable filename. Existing top-level
`no_bare_agent` and `allowed_tools` remain backward-compatible Claude settings.
Runner-specific configuration is scoped under `[claude]` or `[codex]`.

**Per-run model selection (#194).** Each managed run resolves its provider from
the task's model selection, not from a daemon-global runner kind:

- A task with an explicit `tier:` label is validated at creation against one closed,
  shared vocabulary and resolves to its exact full model ID: `sonnet-5` →
  `claude-sonnet-5`, `opus-46` → `claude-opus-4-6`, `opus-47` →
  `claude-opus-4-7`, `opus-48` → `claude-opus-4-8`, `luna` →
  `gpt-5.6-luna`, `terra` → `gpt-5.6-terra`, and `sol` → `gpt-5.6-sol`.
  Unknown non-empty tiers (including legacy `o3` and `o4-mini`) fail with usage
  exit 2 instead of falling back. Empty `tier:` and `effort:` suffixes remain
  compatible no-ops for existing stored labels. Only `effort:medium` and
  `effort:high` are accepted; other non-empty effort labels fail with usage exit 2.
  `resolve_provider` maps the resulting model to `AgentKind::Claude` (any
  `claude-*` model) or `AgentKind::Codex` (known OpenAI models including `gpt-5*`).
- A task with no explicit model selection uses the daemon's configured
  `runner_kind` and `model`, preserving existing Claude-default behavior.
- The resolved provider, model, and effort are persisted in `agent_runs.provider`
  so continuation and recovery cannot switch providers mid-task.
- Reviewers continue to use the daemon's configured provider.

Claude's Sonnet/Opus order is not a cross-runner abstraction. Replace shared rank
inference with explicit per-role selections while preserving Claude defaults:

```toml
[models]
worker = "claude-opus-4-6"
reviewer = "claude-opus-4-7"
r2 = "claude-opus-4-8"
classifier = "claude-haiku-4-5-20251001"
doctor = "claude-sonnet-4-20250514"
```

Complexity overrides map directly to `model/effort`. R1 and R2 models are explicit;
there is no universal "next stronger model" across families.

### Delivery sequence

1. **Extract the runner boundary.** Move current Claude spawn/parsing behind it,
   normalize consumed events, preserve raw JSONL, and prove Claude behavior unchanged.
2. **Add Codex parsing and commands.** Fixture-test consumed events, negative terminal
   paths, unknown events, command shapes, and zero-token real-CLI argument validation.
3. **Enable Codex workers.** Prove initial work, submit, rework continuation, restart,
   watchdogs, auth/quota failure, and unsupported-USD-limit rejection.
4. **Enable Codex R1 and R2.** Prove changes/rework/re-review, stale-head rejection,
   self-review prevention, preflight evidence, CI wait, daemon approval, and merge.
5. **Simplify configuration.** Preserve old Claude configuration, add explicit
   per-role mappings, and install runner-appropriate Quorum guidance.

Classifier, doctor, review interpreter, and analytics collector are not initial Codex
parity requirements. They remain Claude-backed or disabled until the primary
worker → R1 → R2 → merge lifecycle is proven. Mixed-runner behavior is never inferred.

### Optional single-provider operation

After the mixed-runner lifecycle is proven, an operator may select one provider for every
managed coding role:

```toml
provider = "codex"

worker_model = "gpt-5.6-terra"
worker_effort = "medium"

review_model = "gpt-5.6-terra"
review_effort = "high"

classifier_model = "gpt-5.6-terra"
classifier_effort = "medium"
```

`provider` is optional. When absent, the legacy `agent` / `model` / `effort` configuration
and Claude-compatible defaults remain available. When present, it is a fail-safe operating
constraint: worker, R1, R2, live task classification, and post-merge review classification
must all resolve to that provider. An explicit task model or `tier:` label for another
provider is rejected rather than overriding the constraint, and spawn, retry, persistence,
or recovery must never fall back to another provider.

The role model and effort fields are independently configurable. R1 and R2 use the explicit
review selection instead of cross-provider strength inference. Every run persists the exact
provider, model, effort, role, and provider continuation identity before lifecycle
attachment; recovery reuses those durable values. Unknown models, provider/model mismatch,
missing continuation metadata, and unavailable configured runners fail loudly and enter the
existing bounded retry or parked-task path.

The initial operational profile uses Codex `gpt-5.6-terra` at medium effort for workers and
classifiers and high effort for R1/R2. Complexity recommendations are provider-aware
operational routing policy: Claude uses `sonnet-5`/`opus-46`/`opus-47`/`opus-48`, while Codex
uses `luna`/`terra`/`sol`, each at medium or high effort only. The active daemon provider
selects its own five-level ladder; `suggested_models` may explicitly override a level using
only the closed task-tier vocabulary and medium/high effort. Task `tier:`/`effort:` labels
still take precedence over worker defaults, and recommendations remain advisory. These
ladders do not claim cross-vendor benchmark equivalence or establish a cross-vendor strength
ordering.

### Verification gates

Before Codex is production-selectable:

- all Claude tests and real-CLI contracts pass unchanged;
- Codex fixtures cover every consumed event and negative terminal path;
- a disposable repository completes worker → PR → R1 → R2 → checks → merge;
- a changes verdict resumes the same task and Proposed Change;
- restart preserves or safely reconstructs the run;
- shutdown and signals never mark work done;
- auth, quota, protocol, and timeout failures are loud and do not respawn-loop;
- concurrent workers remain isolated;
- the same approved PML Delivery Contract is verified with Claude and Codex, with
  provider/model recorded in evidence instead of the definition.

### Out of scope

- arbitrary runner plugins;
- direct OpenAI API integration independent of Codex CLI;
- Anthropic-compatible gateways or ChatGPT credential proxies;
- emulating Claude `bare`, tool allowlists, or activity hooks for Codex;
- exact subscription-dollar accounting;
- Codex cloud tasks or experimental servers;
- cross-runner session migration;
- per-task or mixed-provider selection initially;
- general agent orchestration.

## Daemon-only execution and lean interface (v2 boundary)

**Date:** 2026-07-16
**Status:** Specified, not yet implemented.
**Supersedes:** PR #375 v2 boundary (merged 2026-07-16). This section is the
corrected design of record. Where PR #375 text conflicts with this section,
this section governs. Specific superseded clauses are cited inline with
**(PR #375 §X — superseded)** markers.

The v1 command surface grew organically: 29 subcommands, several unused by any
caller, several that only the daemon should invoke but are freely available to
any process. This section defines the v2 responsibility boundary — which
commands are public (any named agent), which are daemon-internal (Rust
functions, not CLI commands), and which are operator/admin — and specifies the
revised message delivery, pin, and troubleshooting models.

### Design goal

Quorum's command surface should be **impossible to misuse by a well-behaved
agent** — not merely "documented as off-limits." The daemon drives execution
(task selection, claiming, lease management, lifecycle transitions, verdict
processing) through internal Rust functions in `quorum-core`, not through CLI
subcommands that could be invoked by anyone. External agents interact through a
small, safe surface: create work, describe it, annotate it, read state, and
send messages. Names are attribution, not execution authority.

**No passive execution.** External callers never claim, execute, review, or
submit tasks. **(PR #375 § "Passive agent support (preserved)" — superseded;
PR #375 § `task-submit-external` — superseded.)** The `task-submit-external`
command and `MailboxKind::ExternalSubmit` variant specified in PR #375 are
removed from the target interface. The implicit passive-submit path (v1) is
also removed. If an external/interactive agent needs work reviewed, they file
a `task-create --review-pr N` and the daemon handles it through the normal
lifecycle.

### Capability model: per-run identity

**(PR #375 § "Capability model: run identity" — superseded.)**

PR #375 specified a single shared `QUORUM_DAEMON_TOKEN` — one per-daemon-
instance secret injected into every spawned agent. This lets any worker
impersonate any other worker or reviewer by invoking daemon-only CLI commands
with the shared token. The v2 model replaces this with **immutable per-run
identity**.

| Context | How identified | What it can do |
|---|---|---|
| **Daemon-managed run** | `QUORUM_RUN_ID` env var set by `quorum serve` at spawn time. Each run ID is a unique opaque token tied to exactly one `(run_id, task_id, role)` triple. The daemon records it in `agent_runs`. | `submit` and `react` for its own task only. The run ID is verified against `agent_runs` — a run can only signal on the task and role it was spawned for. All public commands are also available. |
| **External named caller** | Any invocation without `QUORUM_RUN_ID`. Identified by `--agent <name>`. | Public commands only (see table below). |
| **Operator / admin** | Human or privileged script. No special token — admin commands are inherently manual and loud. | Public + admin commands. |

The daemon generates a unique run ID (128-bit hex) for **each** spawned
worker/reviewer and injects `QUORUM_RUN_ID=<id>` into its environment. The
run ID is recorded in `agent_runs` with the associated `task_id` and `role`
(worker/r1/r2). CLI commands that require run identity (`submit`, `react`)
verify `QUORUM_RUN_ID` against `agent_runs` — a worker for task #5 cannot
submit on behalf of task #7.

**Why per-run, not per-daemon?** A shared daemon token is a process-tree
membership proof but not an authorization boundary — it proves "the daemon
spawned me" but grants the full daemon surface to every spawned agent. Per-run
identity is the minimal mechanism that ties capability to scope: one run, one
task, one role.

**Stale-run detection.** A run ID that does not appear in `agent_runs` (daemon
restarted, worker outlived its daemon) is rejected — exit 2, "unknown run."
No DB-stored daemon-wide secret to verify against.

### What managed agents can and cannot do

Managed agents (workers, reviewers) are spawned by the daemon. They receive
task context in their initial prompt — they do not discover or select tasks.
Their CLI surface is:

| Command | Purpose |
|---|---|
| `submit` | Signal task completion (`--pr N`) or emit review verdict (`--verdict approved\|changes`). Requires `QUORUM_RUN_ID`; verified against the run's task and role. |
| `react` | Signal non-terminal agent state (blocked/failed/needs-info). Requires `QUORUM_RUN_ID`. |
| `post` | Post a feed message (public command, available to all). |
| All public commands | See table below. |

**Not available to managed agents as CLI commands:**

- `sync` — **(PR #375 § "Rationale for moving sync to daemon-only" —
  superseded.)** `sync` is not moved to daemon-only; it is removed from the
  v2 target interface entirely. The daemon already constructs context directly
  (prompts, rework turns, messages) via internal Rust functions, never via
  `quorum sync`. Managed agents receive context through stdin turns, not by
  polling. External agents use `task-list`, `task-get`, `read`, `pins`,
  `status`.
- `task-claim` — **(PR #375 daemon-only `task-claim` — superseded.)**
  `task-claim` is removed from the v2 target interface, not token-gated. The
  daemon performs atomic task selection and claiming through internal
  `quorum-core` functions (`tasks::claim`). Managed agents never invoke claim
  commands.
- `claim`/`release`/`renew`/`claims` (generic claims) — **(PR #375 § daemon-
  only claims and rationale — superseded.)** PR #375 asserted "Generic claims
  are used by the daemon to coordinate lock targets (PR branches, merge
  slots)." Verified: the daemon (`quorum/src/serve/`) never calls
  `claims::claim`/`release`/`renew` — all daemon lock coordination uses
  `tasks::claim`. Generic claims have no verified production caller. They are
  removed from the v2 target interface. The `claims` module and its tests
  remain in `quorum-core` as a reusable primitive; the CLI commands are
  retired.

### Command families

Commands are grouped into three families. The "Retiring" column names the
current (v1) command being replaced or removed.

#### Public commands (any named agent)

These are safe for any caller — external interactive sessions, scripts, or
daemon-managed agents. They cannot mutate lifecycle state, hold leases, or
emit verdicts.

| Command | Purpose | Retiring |
|---|---|---|
| `task-create` | Create a task (status: `open`). Unchanged. | — |
| `task-list` | List tasks with filters. Unchanged. | — |
| `task-get` | Full task record including notes. Unchanged. | — |
| `task-update` | Edit task metadata: title, body, labels, priority, refs, notes. **Cannot set status** (status-setting paths are removed from this command; see admin `task-close`/`cancel`). | v1 `task-update --status` (status-setting removed) |
| `task-close` | Terminal close with required reason. Unchanged. | — |
| `post` | Post a feed message. Unchanged. | — |
| `read` | Read feed messages (with optional `--ack-through`). Unchanged. | — |
| `peek` | Non-cursor feed read. Unchanged. | — |
| `log` | Read event log. Unchanged. | — |
| `pin` | Post a standing notice. **Default TTL: 24h** (see Pins below). | v1 `pin` (non-expiring only) |
| `unpin` | Remove a standing notice. Unchanged. | — |
| `pins` | List standing notices. Unchanged. | — |
| `inspect` | Deep read-only troubleshooting (see Inspect below). | — (new) |
| `status` | Compact health snapshot. Unchanged. | — |
| `tail` | Stream agent session log. Unchanged. | — |
| `perf` | Performance report. Prospective-only by default (`--all` for historical). | v1 `perf` (included all terminal tasks) |
| `roster` | Agent presence (migrated from `status --agents`). | `status --agents` |
| `help` | Cheat-sheet. Unchanged. | — |
| `init` | Create DB + config. Unchanged. | — |
| `sweep` | Reclaim expired rows + WAL checkpoint. Unchanged. | — |
| `upgrade` | Update embedded artifacts. Unchanged. | — |

#### Run-scoped commands (require `QUORUM_RUN_ID`)

These are the managed agent's interface to the lifecycle. The run ID is
verified against `agent_runs` to ensure the caller can only act on its own
task in its own role.

| Command | Purpose | Retiring |
|---|---|---|
| `submit` | Signal task completion or emit review verdict. Verified against run's `(task_id, role)`. | v1 `submit` (was unscoped) |
| `react` | Signal non-terminal agent state (blocked/failed/needs-info). Verified against run's task. | v1 `react` (was unscoped) |

#### Admin commands (operator / privileged)

Emergency and lifecycle controls. No token required — these are inherently
manual, loud, and recoverable.

| Command | Purpose | Retiring |
|---|---|---|
| `kill` | Hard-terminate a daemon-managed agent. Emergency only. | — |
| `serve` | Launch the daemon. | — |
| `classify` | Manual task classification backfill. | — |
| `review-interpret` | Manual review-findings extraction. | — |
| `session-register` | Activity hook registration (experimental). | — |
| `activity` | Activity hook event (experimental). | — |

**(PR #375 § admin `stop`/`resume`/`stops` — superseded.)** See § Stop and
kill below.

#### Removed commands (v2)

| Command | Reason | Replacement |
|---|---|---|
| `sync` | Daemon constructs context internally; agents receive via stdin. | Daemon-internal functions. External callers use `task-list`/`task-get`/`read`/`pins`/`status`. |
| `task-claim` | Daemon claims internally; agents receive task context in prompts. | Daemon-internal `tasks::claim`. |
| `claim`/`release`/`renew`/`claims` | No verified daemon caller (see above). | Removed. `claims` module retained in `quorum-core` as reusable primitive. |
| `task-submit-external` | **(PR #375 — superseded.)** Passive execution removed entirely. | `task-create --review-pr N` for external review requests. |
| `task-update --status <s>` | Status-setting bypasses lifecycle. | `task-close` (public) for manual terminal close; `task-update --status cancelled` remains as the one exception. |
| `done` (alias) | Deprecated alias for `submit`. | `submit` (run-scoped). Alias removed, not hidden. |
| `stop` | **(PR #375 § cooperative stop/resume — superseded.)** Agent-directed stop polling removed. | Ordinary stop requests are messages (see § Stop and kill). |
| `resume` | Complement to `stop`; removed with it. | — |
| `stops` | List active stops; removed with `stop`. | — |
| `message` (daemon-routed) | Replaced by unified message delivery model. | Feed messages (`post`/`read`) + daemon stdin delivery. |

### Messages: daemon-delivered, non-interrupting

**(PR #375 § "Messages: durable, non-interrupting turns" — partially
superseded.)** The feed (`post`/`read`/`peek`) remains unchanged for external
agents. The managed delivery model is corrected:

**For daemon-managed agents,** messages are not delivered via `sync` polling
(sync is removed). The daemon delivers messages through the same stdin session
the agent is already running in, at safe boundaries:

1. **Targeting.** A message targets a current active run (by run ID or agent
   name) or all current runs of a given role (all workers, all R1s, all R2s,
   or all managed runs). The daemon resolves targeting at delivery time.
2. **Queuing.** Messages posted while an agent is in an active turn are queued
   by the daemon. They are not dropped, not delivered mid-turn.
3. **Delivery.** At the next safe boundary (agent finishes its current turn,
   or between turn-producing operations), the daemon delivers queued messages
   as a stdin turn via `AgentProc::feed_turn()`. This is the same mechanism
   already used for rework turns and Phase 4c message delivery.
4. **Per-recipient state.** Delivery is tracked per-run. A message delivered
   to run A does not mark it delivered for run B on the same task.
5. **Lifecycle-inert.** Message content never triggers a lifecycle transition.
   A message saying "please cancel task #42" is informational; only `task-
   update --status cancelled` or `task-close` actually cancels.

**Retained feed semantics.** External agents use `post`/`read`/`peek` as
before. Feed messages retain cursor-based at-least-once delivery, TTL expiry
(default 48h), and the monotonic `MAX(last_seq, ?)` cursor advance. The three
delivery states (undelivered/delivered/expired) are unchanged.

**Retention.** Feed messages retain their existing TTL model (default 48h,
configurable). Daemon message queue rows are swept with the normal `done`-task
reclamation cycle.

### Pins: standing prompt context with default expiry

**(PR #375 § "Pins: standing prompt context with TTL" — partially
superseded.)** The pin model is corrected:

- `quorum pin --agent <id> [--ttl <duration>] --body-stdin` — **default TTL
  is 24h** (not optional-non-expiring). If `--ttl` is omitted, `expires_at =
  now + 24h`. Explicit `--ttl` overrides the default. Explicit `--ttl 0` or
  `--no-expire` is available for permanent pins but is the exception, not the
  default.
- The `pinned` table gains a nullable `expires_at INTEGER` column. Pins with
  `expires_at IS NULL` are treated as permanent. Pins with `expires_at <= now`
  are filtered out of reads (same boundary as messages/claims).
- `sweep` reclaims expired pins (same pattern as messages).
- **Delivery to managed agents.** Pins do not depend on `sync` (which is
  removed). Active pins are injected into daemon-managed agent prompts at
  safe boundaries: included in the initial spawn prompt, and new/changed pins
  are delivered as stdin turns between agent turns (same mechanism as
  messages). External agents read pins via `quorum pins`.
- **Longer TTL must be explicit and bounded.** Any pin TTL longer than 24h
  requires explicit `--ttl <duration>`. There is no path to unbounded-by-
  default permanent pins.

**Schema migration:** additive — `ALTER TABLE pinned ADD COLUMN expires_at
INTEGER` (nullable, no default). Existing pins have `expires_at = NULL` and
are treated as permanent (grandfathered). New pins get `expires_at = now +
24h` by default. Forward-only, idempotent.

### Inspect: deep read-only troubleshooting

`quorum inspect` is a new public command that consolidates deep read-only
queries that today require multiple commands or direct DB access. It does not
replace `status` (which remains compact) or `tail` (which remains streaming).
Inspect is aware of retained artifacts (review findings, collection runs,
agent runs).

| Subcommand | Purpose | Current equivalent |
|---|---|---|
| `inspect task <id>` | Full task record + all notes + event history + agent runs + mailbox rows + review findings | `task-get` + `log --refs task#N` + manual DB queries |
| `inspect agent <name>` | Agent presence + all current/recent tasks + run history + message cursor positions | `roster` + `task-list --assignee` + manual DB queries |
| `inspect mailbox [--agent <name>]` | Unconsumed mailbox rows (optionally filtered by agent) | No public equivalent (daemon-internal `poll_unconsumed`) |
| `inspect db` | Schema version, row counts per table, WAL size, last sweep timestamp | Manual `PRAGMA` queries |

All `inspect` subcommands are read-only (no locks, no side effects, no
presence bump). Output is JSON. Exit codes follow the standard contract.

### Stop and kill

**(PR #375 § "Cooperative stop/resume is preserved, not replaced" —
superseded.)**

PR #375 preserved agent-directed cooperative stop/resume as separate admin
commands (`stop`/`resume`/`stops`) where agents poll `sync` for a stop
signal and cheap-poll for resume. This model is removed:

- **Ordinary stop requests are messages.** An operator or external agent
  wanting to halt a managed agent sends a message (via `post` with a
  `kind:stop` or similar convention). The daemon delivers it at the next safe
  boundary. The agent acts on it as content — there is no special `sync`-
  driven stop signal. This keeps the stop path inside the unified message
  delivery model rather than requiring a separate polling protocol.
- **`kill` is emergency termination.** Unchanged from v1: the CLI writes a
  `MailboxKind::Kill` row, and the daemon consumes it by SIGTERM then SIGKILL
  of the target agent process, slot release, and post-mortem ladder on any
  held task. Use for zombie workers, stuck processes, or emergency abort.
- **Self-update drain remains separate.** The `--self-update-drain` mechanism
  (signal-triggered drain, exit 75 for supervisor rebuild) is orthogonal to
  agent stop/kill and is unchanged.

Daemon scheduling pause/resume (pausing the daemon's spawn loop without
stopping individual agents) is **out of scope for v2** — drain covers the
"stop spawning new work" use case.

### Run identity and capability enforcement

**(PR #375 § "Run identity and capability enforcement" — superseded.)**

**Implementation path for per-run identity:**

1. `quorum serve` generates a unique 128-bit hex run ID for **each** spawned
   agent (worker or reviewer) and injects `QUORUM_RUN_ID=<id>` into that
   agent's environment.
2. The run ID is recorded in `agent_runs` with the `task_id` and `role`
   (worker/r1/r2) at spawn time. This is an existing table — no new table
   needed.
3. Run-scoped commands (`submit`, `react`) read `QUORUM_RUN_ID` from the
   environment. If absent → exit 2 ("this command requires a daemon-managed
   run"). If present, verify against `agent_runs`: the run must exist, be
   active, and the command must be valid for the run's task and role.
   Mismatched task/role → exit 2.
4. Public commands ignore `QUORUM_RUN_ID` entirely.
5. No daemon-wide shared token. No `daemon_lock.token` column. **(PR #375 §
   `daemon_lock` token column — superseded.)**

**Schema:** no new columns. `agent_runs` already stores `run_id`, `task_id`,
`role`, and `status`. The only change is that `run_id` values are injected
as env vars at spawn time and verified by run-scoped CLI commands.

### Compatibility and removal sequencing

**(PR #375 § "Compatibility and removal sequencing" — superseded.)**

The transition from v1 to v2 is **not** a flag day. Commands are removed
incrementally:

1. **Phase 1 (additive):** Add per-run `QUORUM_RUN_ID` generation and env
   injection to `serve`. Add `expires_at` column to `pinned` (default 24h).
   Add `inspect` command. Add `roster` as standalone. All v1 commands continue
   to work — no breakage.
2. **Phase 2 (soft removal):** `sync`,
   `stop`/`resume`/`stops`, and
   `message` emit a deprecation warning (stderr). `submit` and `react` accept
   both the old unscoped path and the new `QUORUM_RUN_ID` path.
   *Note:* `task-claim` and generic claims (`claim`/`release`/`renew`/`claims`)
   were hard-removed in PR #85 and PR #161 (skipped soft-removal).
3. **Phase 3 (hard removal):** Deprecated commands exit 2. `submit` and
   `react` require `QUORUM_RUN_ID`. `done` alias removed. `task-update
   --status` restricted to `cancelled` only.
4. **Phase 4 (cleanup):** Remove `status --agents` (replaced by `roster`).
   Remove the implicit passive-agent path from `submit`. Remove
   `task-submit-external` if it was ever added in a partial PR #375
   implementation.

Each phase is a separate PR. Phase 1 can ship immediately.

**DB migration is forward-only and idempotent** (one `ALTER TABLE pinned ADD
COLUMN expires_at INTEGER`, nullable). No data migration. The per-repo DB
remains disposable.

### Code paths being retired

| Path | File | What changes |
|---|---|---|
| `task-update --status open\|working\|in-review\|...` | `quorum/src/main.rs` (TaskUpdate handler) | Status field restricted to `cancelled` only; all other status transitions go through lifecycle events. |
| `done` alias on `submit` | `quorum/src/cli.rs:362` | `#[command(alias = "done")]` removed. |
| `status --agents` | `quorum/src/cli.rs:308`, `quorum/src/main.rs` | Flag removed; `roster` becomes standalone. |
| `sync` CLI command | `quorum/src/cli.rs`, `quorum/src/main.rs`, `quorum-core/src/sync.rs` | CLI entry point removed. `sync.rs` module retained in `quorum-core` for any internal daemon use. |
| `task-claim` CLI command | `quorum/src/cli.rs`, `quorum/src/main.rs` | **Hard-removed (PR #161).** `tasks::claim` retained as internal function. |
| Generic claims CLI (`claim`/`release`/`renew`/`claims`) | `quorum/src/cli.rs`, `quorum/src/main.rs` | **Hard-removed (PR #85).** `quorum-core/src/claims.rs` module retained. |
| `stop`/`resume`/`stops` CLI | `quorum/src/cli.rs`, `quorum/src/main.rs`, `quorum-core/src/control.rs` | CLI entry points removed. Stop requests become messages. |
| `message` CLI | `quorum/src/cli.rs`, `quorum/src/main.rs` | CLI entry point removed. Feed `post` + daemon stdin delivery replace it. |
| Passive-submit detection | `quorum/src/serve/mod.rs` (Phase 2) | Removed entirely (no passive execution). |
| `task-submit-external` | (if partially implemented from PR #375) | Removed. |

### Summary of new/changed schema

| Table | Change | Migration |
|---|---|---|
| `pinned` | Add `expires_at INTEGER` (nullable) | `ALTER TABLE pinned ADD COLUMN expires_at INTEGER` |

**(PR #375 § `daemon_lock` token column and `MailboxKind::ExternalSubmit` —
superseded.)** No `daemon_lock.token` column. No `ExternalSubmit` mailbox
kind. Per-run identity uses the existing `agent_runs` table without schema
changes.

### Invariants (new, in addition to the existing 11)

12. **Per-run capability gate.** `submit` and `react` require `QUORUM_RUN_ID`
    matching an active row in `agent_runs` for the correct `(task_id, role)`.
    Absent or mismatched → exit 2. The run ID is per-spawn, immutable, and
    dies with the agent process.
13. **Message content is lifecycle-inert.** No feed message body, kind, or
    ref field triggers a lifecycle transition. Lifecycle transitions happen
    only through the mailbox (consumed by the daemon) or direct CLI commands
    (`task-update --status cancelled`, `task-close`).
14. **Pin expiry defaults to 24h.** Pins without explicit `--ttl` expire
    after 24 hours. `pinned.expires_at <= now` means expired (same boundary
    as claims/messages). `pinned.expires_at IS NULL` means permanent
    (grandfathered or explicit `--no-expire`). Sweep reclaims expired pins.
15. **No passive execution.** External callers cannot claim, execute, review,
    or submit tasks. Task creation and annotation are the external interface;
    execution is daemon-only.

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
  presence-based claim eviction · arbitrary-byte (BLOB) payloads · **agent-name uniqueness
  enforcement** (v1 is caller-owned first-use-wins; same id silently merges — a v2 could
  reject re-use of an active name from a different session, or hand out names itself).
