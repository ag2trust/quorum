# Quorum — Design Spec

**Date:** 2026-06-23 (lifecycle refactor 2026-07-06, v2 boundary 2026-07-16)
**Status:** Implemented (v1) · CLI + daemon · lifecycle state machine (`lifecycle.rs`)
· v2 boundary specified (§ Daemon-only execution)
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
- `quorum task-claim --agent <id> [--task-id <n>]` → specific task, or highest-priority
  ready `open` task; atomic via `UPDATE … WHERE status='open' RETURNING`. Fires
  `Claimed { agent }` → `working`. Response includes `suggested_branch`,
  `suggested_worktree`, and `branch_exists` — centralized per-(task, project) branch
  allocation in `task_branches`, idempotent on `(task_id, repo)`, so rework re-claims
  return the SAME branch (issue #98). **Dependency gating:** `depends_on` tasks must all
  be `done` before a task is claimable.
- `quorum task-claim --agent <id> --task-id <n>` (on an `in-review` task) → fires
  `ReviewerAttached { agent }`, sets reviewer. **Guard:** agent must differ from author.
- `quorum task-update --agent <id> --task-id <n> [--status open|cancelled] [--verdict approve|changes] [--blocking N] [--refs <json>] [--body-stdin|--body-file]` → fails loud if not assignee. Only `open` (release/reopen) and `cancelled` are directly settable; `working`, `in-review`, `rework`, `merging`, `failed` go through lifecycle events. **(v2: `--status` restricted to `cancelled` only; `--verdict`/`--blocking` removed — verdicts go through daemon-only `submit`. See § Daemon-only execution.)**
- `quorum task-close --agent <id> --task-id <n> --reason-stdin|--reason-file` → explicit
  manual/external terminal close (merged by hand, fixed elsewhere, obsolete). From any
  non-terminal state; reason REQUIRED. Sets `done` but emits `task_closed_manual` event
  (never `task_done`) — the audit log distinction is the guardrail. Owner/manual use;
  agents finishing work must use `quorum done`.
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

Terminals: done, failed, cancelled (reachable from any non-terminal)
```

| Status | Wire format | Terminal | Meaning |
|---|---|---|---|
| Open | `open` | no | Unclaimed, available for work |
| Working | `working` | no | Claimed by a worker agent |
| InReview | `in-review` | no | Worker signaled done (PR posted), awaiting reviewer |
| Rework | `rework` | no | Reviewer requested changes; worker must fix and re-push |
| Merging | `merging` | no | Approved; merge in progress |
| Done | `done` | yes | Successfully merged |
| Failed | `failed` | yes | Rework cap exceeded, or review-only task got changes verdict |
| Cancelled | `cancelled` | yes | Explicitly cancelled |

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
| `LeaseExpired` | — | Lease reaper |
| `AgentFailed { reason }` | description | Worker/reviewer process died |
| `Cancelled { by }` | who | `task-update --status cancelled` or daemon policy |

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
- **Passive agent support:** `daemon_lock` (invariant 11) guarantees one daemon per DB.
  A `submit` mailbox row from an agent not in the daemon's spawn roster is treated as a
  passive/interactive agent submission: the daemon looks up the agent's working task by
  assignee, fires `SignaledDone { pr }` (or closes directly if no PR), and consumes the
  row. Phase 5b then spawns a reviewer on the next tick. Recovery skips working tasks
  assigned to passive agents (not found in the journal). This gives interactive Claude Code
  sessions the same review lifecycle as daemon-managed workers.

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

### R2 pre-merge review gate

When R1 (first reviewer) approves and the task is selected for R2 by the existing
stratified sampler (`review_audits::should_sample`), the daemon intercepts **before**
firing `VerdictApprove` and replaces R1 with an R2 reviewer:

1. **Sampling** — same stratum-based logic as before: under target → always sample;
   at/over target → sample with `r2_steady_state_p` probability. R2 is NOT mandatory
   for every PR.
2. **R1 teardown** — R1 is torn down (end reason `r2-superseded`). Task stays InReview.
3. **R2 spawn** — R2 is spawned as a normal pre-merge reviewer with the same escalation
   policy as R1 (one tier above worker model, capped at top tier, respecting config
   floor). R2's prompt frames it as an adversarial second reviewer that attempts
   to falsify the merge-safety claim, reviews independently before comparing
   against R1, and requires evidence-bound findings (concrete code paths with
   demonstrated failures).
4. **Verdict flow** — R2's verdict drives lifecycle:
   - Approved → fire VerdictApprove → proceed to merge (with stale-SHA check).
   - Changes → fire VerdictChanges → rework → author pushes → ReworkPushed resumes
     R2 (not R1). The `r2_origin` flag on the slot ensures rework routes back to R2.
5. **Stale-SHA gate** — head SHA is recorded at R2 spawn and refreshed on re-review.
   Before merge, the daemon compares the reviewed SHA to the current head. A mismatch
   fires MergeFailed so the PR goes through another review cycle.
6. **Rework routing** — after R2-requested rework, `ReworkPushed` yields
   `ResumeReviewer` (not `SpawnReviewer`). The daemon feeds this to R2, not R1.
7. **Audit recording** — on both approved and changes verdicts, the daemon records an
   R2 audit row via `review_audits::insert` for stratum coverage tracking.

No new lifecycle states were added. R2 uses the existing `InReview ⇄ Rework` transitions.

### Daemon merge flow

After VerdictApprove (InReview → Merging):
1. Check stale SHA — if reviewer recorded a head SHA and it differs from current, fire
   MergeFailed → rework cycle (prevents stale approval from authorizing a changed diff).
2. Check mergeability — if conflicting, MergeFailed → rework cycle.
3. Wait for CI checks — failed → rework; timed out → cancelled.
4. Persist approval record (instance-independent, survives restart).
5. Execute `gh pr merge` — success → Done; policy-blocked → Cancelled;
   retryable failure → rework.
6. Self-update drain: if enabled, a successful merge triggers drain mode →
   exit 75 for the supervisor to rebuild and relaunch.
7. **Post-merge analytics collector** (#125) — fire-and-forget `tokio::spawn` runs
   after `MergeSucceeded`. Analytics-only; can never mutate lifecycle, verdict, or
   merge outcome. See below.

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
directly — the manual backfill and the automatic post-merge path share one
ingestion implementation. Used for backfill on historic PRs and for retrying
recorded failures (`SELECT * FROM review_collection_runs WHERE status='failed'`).

## Daemon-only execution and lean interface (v2 boundary)

**Date:** 2026-07-16
**Status:** Specified, not yet implemented.

The v1 command surface grew organically: 29 subcommands, several unused by any
caller, several that only the daemon should invoke but are freely available to
any process. This section defines the v2 responsibility boundary — which
commands are public (any named agent), which are daemon-internal (only
daemon-managed runs), and which are operator/admin — and specifies the revised
message, pin, and troubleshooting models.

### Design goal

Quorum's command surface should be **impossible to misuse by a well-behaved
agent** — not merely "documented as off-limits." Daemon-only verbs (claim
execution, lease management, lifecycle transitions, verdict emission) are
enforced by capability, not convention. External agents interact through a
small, safe surface: create work, describe it, annotate it, read state, and
send messages.

### Capability model: run identity

Every CLI invocation already identifies itself via `--agent <name>`. The v2
boundary adds a second axis: **run context**.

| Context | How identified | What it can do |
|---|---|---|
| **Daemon-managed run** | `QUORUM_DAEMON_TOKEN` env var set by `quorum serve` at spawn time. Token is a per-daemon-instance random secret, not a credential — it proves "the daemon spawned me," not "I am authorized." | Full surface: sync, claim, submit, review, react, message (daemon-routed), plus all public commands. |
| **External named caller** | Any invocation without `QUORUM_DAEMON_TOKEN`. Identified by `--agent <name>`. | Public commands only (see table below). |
| **Operator / admin** | Human or privileged script. No special token — admin commands are inherently manual (stop/resume/kill affect the running system; misuse is loud and recoverable). | Public + admin commands. |

The daemon generates `QUORUM_DAEMON_TOKEN` (a 128-bit hex string) once at
startup and injects it into every spawned worker/reviewer environment. CLI
commands in the daemon-only family check for this token and exit 2 ("not a
daemon-managed run") if absent or mismatched. The token is **not** persisted —
it dies with the daemon process, so a stale worker from a previous daemon
instance cannot impersonate the current one.

**Why not a DB-stored credential?** The token is a process-tree membership
proof, not an access-control credential. Storing it in the DB would let any
process that can read the DB file impersonate a daemon worker — the opposite
of the goal. The env-var approach is the minimal mechanism that proves "this
process was spawned by this daemon instance."

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
| `task-update` | Edit task metadata: title, body, labels, priority, refs, notes. **Cannot set status** (status-setting paths are removed from this command; see daemon-only `submit` and admin `task-close`/`cancel`). | v1 `task-update --status` (status-setting removed) |
| `task-close` | Terminal close with required reason. Unchanged. | — |
| `post` | Post a feed message. Unchanged. | — |
| `read` | Read feed messages (with optional `--ack-through`). Unchanged. | — |
| `peek` | Non-cursor feed read. Unchanged. | — |
| `log` | Read event log. Unchanged. | — |
| `pin` | Post a standing notice. **Gains `--expires-at` / `--ttl`** (see Pins below). | v1 `pin` (non-expiring only) |
| `unpin` | Remove a standing notice. Unchanged. | — |
| `pins` | List standing notices. Unchanged. | — |
| `inspect` | Deep read-only troubleshooting (see Inspect below). | — (new) |
| `status` | Compact health snapshot. Unchanged. | — |
| `tail` | Stream agent session log. Unchanged. | — |
| `perf` | Performance report. Unchanged. | — |
| `roster` | Agent presence (migrated from `status --agents`). | `status --agents` |
| `help` | Cheat-sheet. Unchanged. | — |
| `init` | Create DB + config. Unchanged. | — |
| `sweep` | Reclaim expired rows + WAL checkpoint. Unchanged. | — |
| `upgrade` | Update embedded artifacts. Unchanged. | — |

#### Daemon-only commands (require `QUORUM_DAEMON_TOKEN`)

These drive the task lifecycle state machine. Only daemon-managed workers and
reviewers may invoke them. External callers get exit 2.

| Command | Purpose | Retiring |
|---|---|---|
| `sync` | Agent orientation tick. Returns current/next task, messages, pins, signals. Auto-acks cursor, auto-renews leases. | — (was public; now daemon-only) |
| `task-claim` | Claim a task (atomic). Fires `Claimed`/`ReviewerAttached`. | — (was public; now daemon-only) |
| `submit` | Signal task completion or emit review verdict. Writes mailbox row for daemon. | — (was public; now daemon-only) |
| `react` | Signal non-terminal agent state (blocked/failed/needs-info). | — (was public; now daemon-only) |
| `message` | Send a message to another daemon-managed agent. Daemon delivers as a turn. | — (was public; now daemon-only) |
| `claim` | Acquire a generic lock target. | — (was public; now daemon-only) |
| `release` | Release a generic lock. | — (was public; now daemon-only) |
| `renew` | Renew a generic lock lease. | — (was public; now daemon-only) |
| `claims` | List active generic locks. | — (was public; now daemon-only) |

**Rationale for moving `sync` to daemon-only:** `sync` is the heartbeat of a
managed run — it auto-renews leases, auto-acks cursors, and returns the
`next_task` pick. An external agent running `sync` would silently extend
leases it shouldn't hold, consume messages intended for managed runs, and
compete for task picks. External agents that need orientation use `task-list`,
`task-get`, `read`, `pins`, and `status` — the public read surface.

**Rationale for moving claims to daemon-only:** Generic claims (not task
claims) are used by the daemon to coordinate lock targets (PR branches, merge
slots). External callers have no legitimate use for them and could
accidentally block daemon operations.

#### Admin commands (operator / privileged)

Emergency and lifecycle controls. No token required — these are inherently
manual, loud, and recoverable.

| Command | Purpose | Retiring |
|---|---|---|
| `stop` | Halt agent(s). Non-expiring. | — |
| `resume` | Clear a stop. | — |
| `stops` | List active stops. | — |
| `kill` | Hard-terminate a daemon-managed agent via mailbox. | — |
| `serve` | Launch the daemon. | — |
| `classify` | Manual task classification backfill. | — |
| `review-interpret` | Manual review-findings extraction. | — |
| `session-register` | Activity hook registration (experimental). | — |
| `activity` | Activity hook event (experimental). | — |

#### Removed commands (v2)

| Command | Reason | Replacement |
|---|---|---|
| `task-update --status <s>` | Status-setting bypasses lifecycle. | `submit` (daemon-only) for completion/verdict; `task-close` (public) for manual terminal close; `task-update --status cancelled` remains as the one exception (cancel is always allowed from non-terminal). |
| `done` (alias) | Deprecated alias for `submit`. | `submit` (daemon-only). The alias is removed, not just hidden. |

**Passive agent support (preserved).** The v1 passive-agent path (a `submit`
mailbox row from an agent not in the daemon's spawn roster) is preserved but
narrowed: an external agent may invoke a new public command
`task-submit-external --agent <id> --task-id <n> --pr <N>` that writes a
`MailboxKind::ExternalSubmit` row. The daemon consumes it identically to
today's passive-agent path (looks up the agent's working task, fires
`SignaledDone`, spawns a reviewer). This replaces the implicit "external agent
calls `submit` without a token" path with an explicit, distinct entry point.

### Messages: durable, non-interrupting turns

The v1 message model has two channels: the **feed** (`post`/`read`) for
broadcast/direct messages, and the **mailbox** for daemon-consumed control
events. Both remain, with clarifications:

**Feed messages** (`post`/`read`/`peek`) are unchanged. Any named agent can
post. Messages are durable (persisted, cursor-based delivery, TTL-expiring),
non-interrupting (delivered at the next `sync` tick for managed agents, or
whenever an external agent calls `read`), and content-only — **ordinary
message content never changes lifecycle state.** A message saying "please
cancel task #42" is informational; the recipient must invoke `task-update
--status cancelled` (or `task-close`) to actually cancel.

**Daemon-routed messages** (`message` command, daemon-only) are a distinct
delivery path: the CLI writes a `MailboxKind::Message` row, and the daemon
delivers the payload as a stdin turn to the target agent when it is idle
(Phase 4c). These are also non-interrupting — "idle" means the agent has no
pending work, not that it is interrupted mid-task.

**Message audiences for daemon-managed runs.** The daemon controls which
messages a managed agent sees:

| Audience | Who sees it | How |
|---|---|---|
| Worker on task #N | Direct messages to that agent name | `sync` returns `direct` field |
| R1 reviewer on task #N | Direct messages to that agent name | `sync` returns `direct` field |
| R2 reviewer on task #N | Direct messages to that agent name | `sync` returns `direct` field |
| All managed agents | Broadcast messages (`recipient = NULL`) | `sync` returns `notifications.count`; `critical` broadcasts are inlined |
| Internal daemon helpers (classifier, doctor, collector) | **Excluded by default.** These are headless one-shot agents that do not poll `sync` and have no message cursor. | No delivery path; by design. |

**Delivery states.** A feed message has three states from the recipient's
perspective:

1. **Undelivered** — `seq > cursor` and `expires_at > now`. The message exists
   and the recipient hasn't acked past it.
2. **Delivered** — `seq <= cursor`. The recipient's cursor has advanced past
   this message (via `read --ack-through` or `sync` auto-ack).
3. **Expired** — `expires_at <= now`. Invisible to all reads regardless of
   cursor position.

There is no "read receipt" or "delivered-and-acked" distinction beyond the
cursor position — at-least-once delivery means the recipient may see a message
multiple times if their cursor doesn't advance.

**Retention.** Feed messages retain their existing TTL model (default 48h,
configurable). Mailbox rows are consumed (not deleted) by the daemon and swept
with the normal `done`-task reclamation cycle. No change to retention
semantics.

### Pins: standing prompt context with TTL

Pins evolve from non-expiring-only to **optionally expiring**:

- `quorum pin --agent <id> [--ttl <duration>] --body-stdin` — if `--ttl` is
  provided, sets `expires_at = now + ttl`. If omitted, the pin has no
  `expires_at` (non-expiring, same as v1). **Default TTL when `--ttl` is
  given but the value is omitted: 24h.**
- The `pinned` table gains a nullable `expires_at INTEGER` column. Pins with
  `expires_at IS NULL` never expire. Pins with `expires_at <= now` are
  filtered out of reads (same predicate as messages/claims).
- `sweep` reclaims expired pins (physical cleanup, same pattern as messages).
- **Visibility:** pins are surfaced in every `sync` response (daemon-managed
  agents) and via `quorum pins` (external agents). They are **safe-boundary
  context** — delivered at the next poll, never mid-turn.

**Schema migration:** additive — `ALTER TABLE pinned ADD COLUMN expires_at
INTEGER` (nullable, no default). Existing pins have `expires_at = NULL` and
remain non-expiring. Forward-only, idempotent.

### Inspect: deep read-only troubleshooting

`quorum inspect` is a new public command that consolidates deep read-only
queries that today require multiple commands or direct DB access. It does not
replace `status` (which remains compact) or `tail` (which remains streaming).

| Subcommand | Purpose | Current equivalent |
|---|---|---|
| `inspect task <id>` | Full task record + all notes + event history + agent runs + mailbox rows + review findings | `task-get` + `log --refs task#N` + manual DB queries |
| `inspect agent <name>` | Agent presence + all current/recent tasks + run history + message cursor positions | `roster` + `task-list --assignee` + manual DB queries |
| `inspect mailbox [--agent <name>]` | Unconsumed mailbox rows (optionally filtered by agent) | No public equivalent (daemon-internal `poll_unconsumed`) |
| `inspect claims [--target <t>]` | Active claims with full detail (holder, TTL, timestamps) | `claims` (moving to daemon-only; inspect provides the read-only view) |
| `inspect db` | Schema version, row counts per table, WAL size, last sweep timestamp | Manual `PRAGMA` queries |

All `inspect` subcommands are read-only (no locks, no side effects, no
presence bump). Output is JSON. Exit codes follow the standard contract.

### Kill: emergency termination

`quorum kill` remains the emergency termination path. Unchanged from v1: the
CLI writes a `MailboxKind::Kill` row, and the daemon consumes it by
SIGTERM→SIGKILL of the target agent process, slot release, and post-mortem
ladder on any held task.

**Cooperative stop/resume is preserved, not replaced.** `stop`/`resume` and
`kill` serve different purposes:

- `stop` is **cooperative** — the agent is told to halt but keeps polling. It
  can resume. Use for "pause everything" or "pause this agent."
- `kill` is **destructive** — the agent process is terminated. The daemon
  handles cleanup. Use for zombie workers, stuck processes, or emergency
  abort.

Daemon scheduling pause/resume (pausing the daemon's spawn loop without
stopping individual agents) is **out of scope for v2** — the drain mechanism
(`--self-update-drain`, signal-triggered drain) covers the "stop spawning new
work" use case. A future `quorum serve --pause` / `quorum serve --unpause`
could be added if drain proves insufficient.

### Run identity and capability enforcement

**Implementation path for daemon-token gating:**

1. `quorum serve` generates a 128-bit hex token at startup, stores it in
   memory (not DB), and injects `QUORUM_DAEMON_TOKEN=<token>` into every
   spawned agent's environment.
2. Each daemon-only command reads `QUORUM_DAEMON_TOKEN` from the environment.
   If absent → exit 2 ("this command requires a daemon-managed run"). If
   present but does not match the current daemon's token (checked via a new
   `daemon_lock` column `token TEXT`) → exit 2 ("token mismatch — stale
   run?").
3. The `daemon_lock` row gains a `token TEXT` column. The daemon writes its
   token on lock acquisition. Daemon-only commands verify the env token
   against `daemon_lock.token` — this catches the case where a worker outlives
   its daemon.
4. Public commands ignore `QUORUM_DAEMON_TOKEN` entirely — they work the same
   whether or not it's set.

**Schema migration:** `ALTER TABLE daemon_lock ADD COLUMN token TEXT`
(nullable — old daemons without the column still function; the token check is
skipped if `daemon_lock.token IS NULL`).

### Compatibility and removal sequencing

The transition from v1 to v2 is **not** a flag day. Commands are gated
incrementally:

1. **Phase 1 (additive):** Add `QUORUM_DAEMON_TOKEN` generation to `serve`,
   `token` column to `daemon_lock`, `expires_at` column to `pinned`, the
   `inspect` command, `task-submit-external`, and `roster` as a standalone
   command. All v1 commands continue to work — no breakage.
2. **Phase 2 (soft gate):** Daemon-only commands emit a **warning** (stderr,
   not exit code) when invoked without `QUORUM_DAEMON_TOKEN`. This gives any
   stray scripts time to adapt.
3. **Phase 3 (hard gate):** Daemon-only commands exit 2 without the token.
   The `done` alias is removed. `task-update --status` is restricted to
   `cancelled` only.
4. **Phase 4 (cleanup):** Remove `status --agents` (replaced by `roster`).
   Remove the implicit passive-agent path from `submit` (replaced by
   `task-submit-external`).

Each phase is a separate PR. Phase 1 can ship immediately. Phase 2 requires
daemon restart. Phase 3 requires all external callers to have migrated. Phase
4 is housekeeping.

**DB migration is forward-only and idempotent** (two `ALTER TABLE ... ADD
COLUMN` statements, both nullable). No data migration. The per-repo DB
remains disposable — a clean `rm + init` is always a valid recovery path.

### Code paths being retired

| Path | File | What changes |
|---|---|---|
| `task-update --status open\|working\|in-review\|...` | `quorum/src/main.rs` (TaskUpdate handler) | Status field restricted to `cancelled` only; all other status transitions go through lifecycle events. |
| `done` alias on `submit` | `quorum/src/cli.rs:362` | `#[command(alias = "done")]` removed. |
| `status --agents` | `quorum/src/cli.rs:308`, `quorum/src/main.rs` | Flag removed; `roster` becomes standalone (already implemented as `quorum roster`). |
| Implicit passive-submit via `submit` without token | `quorum/src/serve/mod.rs` (Phase 2, passive agent detection) | Replaced by explicit `task-submit-external` → `MailboxKind::ExternalSubmit`. |

### Summary of new/changed schema

| Table | Change | Migration |
|---|---|---|
| `daemon_lock` | Add `token TEXT` (nullable) | `ALTER TABLE daemon_lock ADD COLUMN token TEXT` |
| `pinned` | Add `expires_at INTEGER` (nullable) | `ALTER TABLE pinned ADD COLUMN expires_at INTEGER` |
| `mailbox` | Add `external_submit` to `kind` CHECK constraint (or use text matching as today) | No DDL — `MailboxKind::ExternalSubmit` is a new Rust enum variant mapped to `"external_submit"` string. |

### Invariants (new, in addition to the existing 11)

12. **Daemon-token capability gate.** Commands in the daemon-only family
    require `QUORUM_DAEMON_TOKEN` matching `daemon_lock.token`. Absent or
    mismatched → exit 2. The token is ephemeral (per-daemon-instance,
    in-memory, not persisted beyond `daemon_lock.token`).
13. **Message content is lifecycle-inert.** No feed message body, kind, or
    ref field triggers a lifecycle transition. Lifecycle transitions happen
    only through the mailbox (consumed by the daemon) or direct CLI commands
    (`task-update --status cancelled`, `task-close`).
14. **Pin expiry is optional.** `pinned.expires_at IS NULL` means
    non-expiring. `pinned.expires_at <= now` means expired (same boundary as
    claims/messages). Sweep reclaims expired pins.

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
