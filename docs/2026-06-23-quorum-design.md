# Quorum — Design Spec

**Date:** 2026-06-23 (lifecycle refactor 2026-07-06, v2 boundary 2026-07-16, v2 correction 2026-07-17, merge-wait contract 2026-07-20, no-CI contract 2026-07-23, coding-runner boundary 2026-07-24, explicit-cancellation contract 2026-07-26, source-cancellation escape hatch 2026-08-20)
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
  → daemon-published and verified pull request
  → required checks
  → independent R1 review
  → independent R2 review focused on any material gaps left by R1
  → rework when required
  → final required-checks revalidation
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
general network server. Agents invoke lifecycle-safe `quorum <subcommand>` operations as
ordinary shell commands. Managed PR collaboration additionally uses a credentialless
`quorum agent-mcp` stdio process injected by the daemon and scoped to the managed provider
process; persistent providers retain that MCP process across their stdin-fed turns. It writes
bounded, run-authorized GitHub operation requests through a narrow local daemon endpoint, and the
daemon alone writes SQLite and performs the remote operation. The MCP process never receives the
Quorum database, GitHub credentials, or lifecycle authority.

For contained runs, the same local endpoint carries the existing run-scoped `submit` and `react`
signals and the closed established public-command family, so those commands need neither a
database mount nor a broader daemon API. They retain their current semantics and are not exposed
as MCP tools. Admin commands remain unavailable inside the contained managed runner and use
direct short-lived SQLite operations only outside it.

Each ordinary CLI invocation is a **complete, self-contained, short-lived process**: direct mode
opens the DB, while contained public/run-scoped mode performs one bounded endpoint exchange; both
execute one operation, print JSON to stdout, and exit with a meaningful code. There is **no state
between CLI invocations** — the SQLite file is the sole durable source of truth. The model is
`git`-like: every command reconciles current persisted state and executes atomically.

## Motivation

The current agent hub is GitHub Issue #1455 — an append-only comment log abused as a
message bus. Intrinsic problems (not fixable by convention): slow writes (every post is a
`gh` round-trip), no TTL (comments accumulate; pruning is manual + token-heavy), expensive
reads (re-read "last N comments" every poll), no atomic claim (the semaphore needs post →
10s wait → full rescan → tiebreak-by-comment-id, and still races).

Quorum replaces the *coordination* layer (chatter + claims + task queue). **PRs and code
review stay authoritative on GitHub**; managed agents author that conversation through
Quorum-mediated collaboration operations.

## CLI-first coordination plus managed-agent MCP

General coordination stays CLI-first. Outside a contained managed runner, each short-lived
command opens SQLite, performs one atomic operation, emits JSON, and exits; inside one, the same
public CLI surface uses the closed local endpoint exchange above. Quorum does not add a general
HTTP daemon, remote MCP listener, OAuth surface, port, or second source of coordination state.

Managed GitHub collaboration is the narrow exception because direct Repository Service
credentials and shell-composed Markdown are the wrong boundary for an untrusted coding run. The
daemon injects one tools-only stdio MCP whose inventory is derived from the exact live run role.
The adapter lifetime follows the managed provider process: turn-oriented providers receive a
fresh adapter, while a persistent Claude child retains one adapter and authenticated local-endpoint
session across its stdin-fed turns. That persistent adapter keeps only bounded, opaque session
state for the current attempt/lifecycle/inventory descriptor and invalidates it through the
daemon-authorized phase handoff; the adapter is neither durable authority nor a second source of
coordination state. It forwards closed typed requests to a narrow local daemon endpoint, SQLite
holds every durable operation request and binding, and `quorum serve` executes remote GitHub work
outside transactions. Restricted internal model roles receive no MCP. The separate
outside/task-creator MCP requires its own authentication and remote-transport design and is not
implied by this agent interface.

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
`continue_pr` INTEGER NULL (authoritative existing-PR implementation entry; never inferred
from `refs.pr`) · `completion_provenance` TEXT NULL (`merged`/`manual`; NULL is
legacy or unknown and is never inferred from status or `refs.pr`) ·
`depends_on` TEXT (json array of task IDs) ·
`target_branch` TEXT NULL (authoritative PR base branch; NULL for legacy tasks.
`task-create` persists its validated `--base-branch` or the configured base at
creation; legacy rows resolve once before execution. Immutable once populated and
used for worktree provisioning, publication, review validation, remediation, and merge;
only genuinely targetless legacy rows fall back to the configured base).

### `errors` — observable *abnormal* failures
`id` INTEGER PK · `ts` · `source` TEXT · `detail` TEXT · `expires_at` INTEGER NOT NULL.
Appended **only on genuinely abnormal failures** (DB error, post-timeout `BUSY`, bad input,
migration refusal). **Normal lost-races / not-holder (exit 1) are NOT logged** — they are
expected operation, and logging them would add hot-path write contention + noise.

### `token_usage_runs` — durable per-invocation token telemetry

One row per managed worker/reviewer run or daemon-internal classifier/collector
invocation. The row preserves uncached input, cached input, cache-write input,
output, and reasoning tokens separately, plus purpose, provider, model, effort,
optional PR, and optional `agent_run_id`. `token_usage_run_tasks` maps one usage
row to one or more tasks because the classifier handles batches.

This is separate from `agent_runs`: classifier and collector invocations are not
managed agents, and cost history must remain queryable after task/agent-run sweep.
There are no foreign keys; numeric identities are historical attribution, not
delete authority. Usage history has a 30-day retention window, deliberately
longer than the 7-day completed-task window: sweep-on-write deletes at most 100
expired usage runs (and their task mappings) per mutation, while explicit sweep
deletes all expired usage. Cumulative snapshots for still-active managed runs
are retained until run closure so dormant recovery cannot lose required state.
Usage writes are instrumentation-only and best-effort.
A failed usage write is logged and ignored after the lifecycle/run-close write,
so it cannot fail a verdict, merge, collection result, or teardown.
Provider-error collector turns retain and record any usage reported by the
terminal event before the collection failure is persisted and returned.
Managed teardown likewise normalizes usage from unread terminal provider output
before snapshotting the durable row; terminal output remains lifecycle-inert.
Each managed terminal turn also upserts the cumulative split against its
`agent_run_id`, and dormant Codex/Grok recovery reloads that snapshot before
continuing the same run identity. Ordinary and decomposition classifiers reap
and record bounded terminal usage on normal completion, removal, and every
daemon shutdown path; decomposition classifier rows are attributed to their
source task. Open, write, and join failures on these best-effort paths are
logged without changing lifecycle outcomes.

Provider normalization keeps the source semantics explicit. Claude
`input_tokens`, `cache_read_input_tokens`, and
`cache_creation_input_tokens` remain separate. Codex `input_tokens` includes
cached input, so durable uncached input is `input_tokens -
cached_input_tokens` (saturating at zero); its cached, cache-write, output, and
reasoning-output values are retained independently.

Managed worker and reviewer token watchdog ceilings are a repository serve
policy. `token_limit_basis = "raw"` is the default and preserves the legacy
meaning of both `max_turn_tokens` and `max_task_tokens`: provider-reported
`input_tokens + output_tokens`. An explicit `token_limit_basis = "uncached"`
uses normalized `uncached_input_tokens + output_tokens`, with saturating
arithmetic; it never falls back to raw input when normalized telemetry is
required. The resolved basis is shown at daemon startup and included in token
ceiling breach diagnostics. Unknown bases and non-positive token ceilings are
usage errors.

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
| token usage history | swept 30d after recording | n/a |
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
- `quorum task-create ... --continue-pr <N>` → implementation task (status: `open`,
  `continue_pr=N`) rooted at the exact head of an open same-repository PR. It follows the
  normal worker/review/rework/merge lifecycle, dispatches directly regardless of classified
  size, and publishes back to that PR under lease. It is never a decomposition source because
  generated children cannot inherit the continuation publication authority. `--continue-pr`
  and `--review-pr` are mutually exclusive.
- ~~`quorum task-claim`~~ — **Removed (PR #161).** Daemon claims internally via
  `quorum_core::tasks::claim`. The atomic claim primitive, branch allocation,
  dependency gating, and reviewer attachment are all preserved as internal functions.
- `quorum task-update --agent <id> --task-id <n> [--status open|cancelled] [--verdict approve|changes] [--blocking N] [--refs <json>] [--body-stdin|--body-file]` → fails loud if not assignee. Creator/agent updates may not add, replace, or remove `refs.pr`; that association is daemon-owned. Only `open` (release/reopen) and `cancelled` are directly settable; `working`, `in-review`, `rework`, `merging`, `failed` go through lifecycle events. **(v2: `--status` restricted to `cancelled` only; `--verdict`/`--blocking` removed — verdicts go through run-scoped `submit`. See § Daemon-only execution.)**
- `quorum task-close --agent <id> --task-id <n> --reason-stdin|--reason-file` → explicit
  manual/external terminal close (merged by hand, fixed elsewhere, obsolete). From any
  state except `done`/`cancelled`, but never an active decomposition source, which must use
  graph cancellation — `failed` is included, because a task whose PR landed outside the
  managed lifecycle has no other route to `done` and its dependents stay parked until it
  gets there (`compute_ready` counts only `done`). Closing a generated child performs final
  graph/source reconciliation in the same transaction. Reason REQUIRED. Sets `done` but
  records `completion_provenance=manual` without removing `refs.pr`, and emits
  `task_closed_manual` event
  (never `task_done`) — the audit log distinction is the guardrail. Owner/manual use;
  agents finishing work must use `quorum submit` (`quorum done` is a deprecated alias).
- `quorum task-retry --task-id <n> --by <operator>` → operator retry for a task
  durably parked after an automatic bounded failure. General daemon parks restore
  their recorded lifecycle stage as specified in § Explicit cancellation and durable
  parking. A parked merge restores `merging` with one durable daemon-owned replay intent;
  the daemon revalidates the exact persisted PR target, base, head, required role approvals,
  sampled-R2 decision, and CI before making at most one approval/merge call. Invalid or
  incomplete authority returns to `in-review` for the first missing role. A missing or
  differently-task-bound sampling decision invalidates the R1 sampling anchor and decision
  together, preserving an exact R2 while a fresh R1 recreates the decision. An invalid extra
  R2 row for a durable sampled skip is removed under the existing one-shot attempt and the
  complete authority is reread once before any merge call.
  Provider/auth/quota/protocol parks atomically require and clear their provider-block marker.
  A provider-parked
  `working` task returns to `open`; a true `rework` task remains unassigned in
  `rework` and is atomically reattached through a dedicated replacement-worker
  claim. Both paths carry the exact persisted failed turn. `in-review` is
  rejected; Codex R1/R2 belongs to the later reviewer-provider phase.
  Before teardown, Quorum stores the pending raw prompt, turn kind, exact
  model/effort, and opaque provider continuation ID (when issued) in task refs.
  Provisioning reuses the task branch and PR, resumes that identity when present
  (a fresh turn only before an identity exists), and never substitutes the generic
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
  without any I/O harness. The credentialless agent MCP wraps its run-capability and durable
  GitHub-operation boundaries.
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

### Daemon limits and stall detection

Managed worker and reviewer processes use an idle-based stall guard. `max_idle_secs` is
the maximum time an active process may go without emitting an observable runner event;
each provider event resets the idle clock. The default is 900 seconds (15 minutes).
When the limit is exceeded, the daemon kills and reaps the process group, releases the
task authority, and handles the resulting failure through the normal lifecycle recovery
path. This detects a genuinely stalled process without treating a long, active turn as
stalled.

`max_task_wall_secs` remains an optional hard wall-clock cap for one live worker,
reviewer, or remediation slot. It spans all turns and continuations handled by that
slot and is independent of event activity: a slot that continues emitting events still
fails when this ceiling is exceeded. The in-memory clock resets when the slot is
replaced, when the lifecycle moves to another slot, or when the daemon restarts, so this
setting is not an end-to-end wall-clock cap for the full task lifecycle.

`max_turn_wall_secs` is deprecated and is no longer enforced as a per-turn wall-clock
limit. Existing configurations may retain it as a compatibility alias for the idle
setting, but new configurations must use `max_idle_secs`.

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
| `ChecksFailed { checks }` | failing check names | Daemon pre-review CI gate |
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
- `ChecksFailed` → Rework · effects: IncrementReworkRound, ResumeWorker
- `VerdictChanges` (review_only=true) → Rework · effects: IncrementReworkRound,
  ResumeWorker (at the rework cap → Failed · effects: NotifyOwner, ReleaseLease)
- `VerdictChanges` (rework_round ≥ the task's stamped rework cap) → Failed · effects: NotifyOwner, ReleaseLease
- `AgentFailed` → InReview (**sticky**) · effects: ReleaseLease, NotifyOwner, SpawnReviewer
- `LeaseExpired` → InReview (**sticky**) · effects: ReleaseLease, SpawnReviewer
- `Cancelled { by }` → Cancelled · effects: ReleaseLease

**From Rework:**
- `ReworkPushed` → InReview · effects: ResumeReviewer
- `AgentFailed` / `LeaseExpired` → Open · effects: ReleaseLease (+NotifyOwner on failure)
- `AgentFailed` / `LeaseExpired` (review_only=true) → Failed (parked, resume `rework`) ·
  effects: ReleaseLease, NotifyOwner. A lost remediation worker must not hand the
  unchanged PR head back to a fresh reviewer — that changes verdict would burn a rework
  round with zero remediation applied. The terminal park is never selected for
  automatic retry, including after daemon restart; only an explicit `task-retry`
  restores it to `rework`. The lapsed-lease sweep applies the same owner-gated park
  instead of reclaiming to `in-review`.
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
- **Author-side preflight cache:** A full `preflight.sh` invocation always runs the
  branch-base gate, because it fetches and checks the moving `origin/main` reference.
  Gates 2–4 may instead use a task-local green cache at
  `target/preflight-timing/last-green.json`. Its deterministic fingerprint covers HEAD,
  porcelain status, the binary diff from HEAD, and the paths, modes, and contents of
  untracked inputs (including ignored inputs outside generated `target/`). Only an exact
  `{ fingerprint, exit: 0 }` record written after a green, unchanged gates-2–4 run may
  skip those gates. Missing, malformed, or unreadable cache entries and every fingerprint
  error are cache misses. Fingerprinting never writes the index or uses the network; the
  daemon does not use this optimization and merge CI remains the full backstop.
- **Author-side per-binary test cache:** When the whole-tree cache misses, fmt, clippy,
  and compile/no-run still run in full. Before direct test-binary execution, the collector
  hashes each produced executable's contents with SHA-256 and looks up the per-worktree,
  uncommitted `target/preflight-timing/test-results.json` cache. It maps a digest to exact
  `{ "exit": 0, "target_name": "..." }` green records. Only completed direct executions
  with exit zero and complete cleanup are atomically published (temporary file plus
  `os.replace`); failed, timed-out, interrupted, cancelled, incomplete-cleanup, and
  unlaunched binaries are never cached. Missing, malformed, unreadable, or invalid cache
  data and executable-hash failures are cache misses, so the binary runs; a write failure
  leaves no durable result and the next run executes it again. A cache hit is evidence, not
  an execution: its `timing.json` binary entry has `cached: true`, `cached_pass`, zero
  duration, and no cleanup object. `test_execution` and the summary/final full-pass line
  report executed versus cached counts (and `not_run` for partial fail-fast output). The
  key is the produced file, not inputs or build paths, so a changed executable always runs
  even through sccache. This deliberately reduces repeat concurrency-canary coverage for
  unchanged binaries; delete `target/preflight-timing/test-results.json` to force a full
  binary-execution pass. The daemon does not use this optimization and merge CI remains the
  full backstop.
- **Author-side inert diffs:** After a cache miss, the ordinary single-base author path
  still runs `cargo fmt`, but skips clippy and tests only when the union of
  `BASE_REF...HEAD` changed paths and porcelain working-tree paths is non-empty and every
  path is `docs/**`, a root-level `*.md`, `LICENSE*`, `README*`, or `.github/**`.
  Any `*.rs` path or `Cargo.toml`/`Cargo.lock` basename is a build input and overrides
  those directory/root patterns, retaining the full suite.
  Continuation and integration bases are deliberately ineligible because they compare
  compound histories. Any unresolved base, Git error, malformed path listing, or path
  outside that allowlist runs the full suite. In particular, this is not a broad
  `**/*.md` or `.claude/**` rule: `.claude/skills/quorum/SKILL.md`, `schema.sql`, and the
  served web assets are `include_str!` build inputs. The daemon's CI remains the
  unconditional full-suite backstop.
- **Rework cap:** per-task, configured, and frozen at daemon adoption. The serve
  config knob `max_rework` (validated `>= 1`; unset falls back to the compiled
  default `REWORK_CAP = 7`) is stamped onto each task inside the *same* write
  transaction that accepts its classification, so the accepted refs and the
  immutable adoption-time cap land or roll back together. All supported
  classification writers (the daemon classifier and `quorum classify --backfill`)
  stamp; the immutable-once `WHERE rework_cap IS NULL` guard means a later config
  change only affects newly-adopted tasks. Legacy rows that predate the migration
  and never re-enter classification stay `NULL` and resolve to the compiled
  default. When `rework_round >= <the task's stamped cap>` and an actionable
  rework event (VerdictChanges, ChecksFailed, or MergeConflict) fires, the task
  goes to Failed (not Rework). Rounds are consumed only by review verdicts, CI
  failures, and merge conflicts on delivered work — never by infrastructure
  failures (provisioning, worker death, lease lapse), which park the task instead.
- **Review-only entry:** `task-create --review-pr N` creates a task directly in `in-review`
  with `review_only=true`. A blocking verdict enters normal bounded rework; it never falls
  through to generic implementation-worker provisioning, because remediation must retain
  the adopted PR target. Before reviewer provisioning, the daemon validates the live PR base
  against the task's immutable target branch, with configured-base fallback only for legacy
  targetless tasks. The daemon revalidates the live base before every reviewer feed and again
  before formal approval and merge, including restart recovery and policy retries. Review-only
  remediation integrates that same task target. Storage failures while resolving task authority
  remain abnormal daemon errors rather than ordinary target rejections or parked remediation.
- **Existing-PR implementation entry:** `task-create --continue-pr N` creates an `open`
  implementation intent and atomically rejects an already-owned PR. Before provisioning,
  the daemon resolves an open, same-repository, non-fork PR, rechecks exclusive nonterminal
  ownership, and persists its exact head branch and SHA. The daemon checks out and verifies
  that exact commit, then merges the freshly fetched configured base into the continuation
  branch before worker launch. The daemon explicitly overrides `merge.ff=only` for this operation;
  Git completes a clean integration (creating a merge commit when histories diverge), while a
  conflicting merge remains in progress for the worker to resolve and commit. This preserves the
  recorded PR head as an ancestor so later publication is
  fast-forward-only. The daemon supplies the configured base identity at the push boundary, and
  the publication gate attributes sessions only to commits reachable from the proposed tip but
  from neither the recorded PR head nor that freshly fetched base; inherited base commits do not
  become worker-owned. Before publication, the daemon revalidates the live PR as open and still
  targeting the configured base, then targets only its recorded branch under the recorded SHA
  lease. Ownership ambiguity, closure, base retargeting, branch replacement, or SHA movement fails
  closed; Quorum neither rebases onto the new
  head nor silently falls back to fresh implementation or a new PR. Existing same-task
  rework continues through the established PR association and does not create a new entry.
- **Entry authority:** task creators select exactly one of fresh implementation (neither
  flag), review-only (`--review-pr`), or existing-PR implementation (`--continue-pr`).
  They never choose a lifecycle status. Generic `refs.pr` is display/correlation metadata,
  not Proposed Change ownership or publication authority, and creator-supplied `refs.pr`
  is rejected. Only a successful daemon lifecycle transition may establish or update the
  authoritative PR association.
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
`open`; readiness remains false until all dependencies are `done`. The cascade also
distinguishes a merely-`failed` dependency (recoverable — the dep itself may still
retry to `done`) from a `cancelled` dependency (terminal-terminal — no path exists
back to `done` without a `depends_on` edit or closing the dependent). The park reason
names the specific failing dep — a `cancelled` dep is preferred over a `failed`
sibling because it drives the operator disposition — and the durable
`daemon_parked_unsatisfiable=true` bit records the distinction in refs. Every other
park path (`set_parked_refs`) clears any stale value so the marker is authoritative
for the current park only. `quorum status` includes `daemon_parked_unsatisfiable=1`
rows in the BLOCKED section with the cancelled dep in `deadlocked_on`, so the
operator sees the disposition queue without DB inspection.

Convergence when a dep transitions to `cancelled` runs in two coordinated
layers:

1. **Atomic at the cancellation:** `tasks::update` calls
   `converge_parked_dependents_of_cancelled` inside the cancel transaction.
   The transaction durably enqueues the cancelled task and examines one
   primary-key-ordered page of raw task rows. Matching non-classifier-policy
   daemon parks have their marker and reason upgraded before commit. The raw
   page bound, rather than a post-filter result limit, bounds examined history.
2. **Read-side inference in `stats::blocked_tasks`:** the BLOCKED section
   surfaces every `status='failed'` daemon-parked task whose `depends_on`
   currently contains any `cancelled` dep, regardless of the durable marker.
   This covers cancellation paths that do not route through `tasks::update`
   (decomposition-triggered cancels, direct test mutations, upgrade timing),
   and covers queued rows awaiting durable reconciliation and
   classifier-policy parks whose refs must not be overwritten
   (their durable `daemon_parked_reason` stays "classifier declined").

Bounded opportunistic write-sweeps advance one durable reconciliation cursor;
explicit `sweep_all` drains the queue. Thus dependents beyond the cancellation-
time page eventually receive durable refs through production paths. The v50
migration performs the same durable repair at upgrade time on
installed databases so pre-existing parks join the disposition queue
immediately. Both the migration and the runtime convergence skip
classifier-policy parks so the classifier cause is preserved; those rows
still surface via the read-side inference. Convergence emits
`task_parked_upgraded` and a task note; it does not duplicate the original
owner alert. Each periodic pass examines at most one indexed raw-ID page, so
mutation work is independent of retained failed-task history.

`quorum task-retry --task-id N --by <operator>` is the sole resume operation for a
daemon-parked task, a worker provider block, or the narrowly eligible exhausted
decomposition-planning state described below. For a daemon park it atomically validates
the marker, clears it (including the
unsatisfiable bit), resets only the crash recovery counter, and emits `task_retry`.
`open`, `rework`, and `in-review` restore directly. A `rework` retry also records
`daemon_rework_retry_requested=true`; startup recovery preserves it and the next
daemon tick atomically claims and spawns a replacement worker on the same task and
branch. A parked `merging` task restores to `merging` and records
`daemon_merge_retry=requested`. The daemon atomically advances one such intent to
`attempting` before network work, then revalidates the immutable task/PR association,
persisted target tuple, expected base, live head, exact task/role/SHA approvals, durable R2
sampling decision, and CI. Valid authority performs one merge replay without allocating a
reviewer; missing or stale authority is invalidated narrowly and returns to `in-review` for
the first missing role. Missing or task-mismatched sampling evidence cannot be replaced by
otherwise-valid R1/R2 rows: the daemon atomically removes that decision and R1, then normal
review recreates the decision from a fresh R1 while retaining an exact R2. If a durable skip
makes R2 optional but a stale extra R2 row exists, the daemon atomically removes only that row
and rereads the complete authority once under the same `attempting` marker; it never issues
more than the one merge call authorized by the owner retry. The exact approved head is carried
through the merge-executor boundary: the production executor rechecks it immediately before
formal approval and again afterward, then invokes GitHub merge with that SHA as the required
head. A force-push in the remaining call window is therefore rejected and returns to review
with stale approvals invalidated. Legacy tasks without an immutable `target_branch` cannot
directly replay approval; their approvals are invalidated and review is rebuilt against a
persisted base. A repeated policy/infrastructure failure parks again with approvals intact. A
worker-fixable replay result atomically consumes the `attempting` marker, invalidates approval
rows, records bounded remediation feedback, and transitions directly from `merging` to
`rework`; no intermediate `in-review` state can allocate a replacement reviewer or lose the
actionable turn. The ordinary live reviewed path also writes `attempting` immediately before
its first remote merge call, and must durably park a policy outcome before tearing down its
worker/reviewer. Final-role verdict persistence uses the head already validated against the
reviewer's launch SHA; it never refetches and binds the verdict to a later force-pushed head.
Startup never auto-replays a parked task; any interrupted `attempting` marker is parked for
another owner retry rather than issuing an uncertain duplicate call. A policy failure during
startup approval replay is likewise parked with exact-head approvals retained before generic
recovery, while a worker-fixable result invalidates those approvals, records
`daemon_rework_retry_requested=true`, and returns to rework. This marker survives the immediately
following generic recovery even when no worker journal or continuation exists, so the daemon
provisions the actionable remediation turn rather than resetting the task to `open`.
Retry does not change PR identity, approvals, dependencies, author/reviewer provenance, or
rework count. An
unparked or terminal task is a clean negative (exit 1). One additional clean negative
(exit 1) fires when the parked task's `depends_on` still contains any `cancelled`
task id: silently restoring the dependent would just have the sweep re-park it on
the next tick while leaving the operator with no disposition signal. The CLI names
the cancelled dep(s) in the JSON payload; the operator resolves it by editing
`depends_on` (existing task-update guarded path) or closing the dependent. No
automatic cancellation cascade is added. This explicit gate prevents hot
respawn/provision loops: daemon ticks cannot retry a parked task until the operator
requests it.

Daemon startup also reconciles bounded batches of legacy/corrupt terminal rows carrying
runnable remediation retry markers. `failed` parks retain their reason and explicit
owner-retry target but lose automatic retry/head-check authority; `done` and `cancelled`
rows lose all park/resume authority. The cleanup writes one audit note and is idempotent.
Before reconciliation, `quorum status` surfaces these rows as critical health alerts.

### Review responsibility boundary (agents author; Quorum mediates)

For PR-backed tasks, the GitHub PR is the source of truth for the review conversation:
findings (BLOCKING and advisory), advisory suggestions, author responses/pushback,
reviewer resolution of prior findings, and evidence. Quorum coordinates lifecycle,
provisioning, validated publication, the final formal APPROVE, and merge. Agents author the
conversation through the run-bound GitHub collaboration MCP; Quorum preserves their Markdown,
anchors contributions to the authorized PR/revision, and never derives lifecycle transitions
from comment text. Concretely:

- **Reviewer agents** complete their planned audit for the reviewed SHA before
  submitting a verdict: discovering one blocker does not end exploration. They
  post the complete discovered blocking and advisory set to one PR review
  summary (with inline comments where a specific file/line applies), and the
  `--blocking` count covers that complete blocker set. Before a verdict,
  reviewers derive a bounded, task-specific affected-path model from the
  embedded managed-task contract when provided and the mechanisms changed by
  the PR. They choose a useful representation — a short matrix, checklist,
  state/event map, or equivalent — to review applicable related lifecycle and
  compatibility paths together and determine whether the proposed remedy
  closes each relevant path. This does not prescribe a fixed format, require
  speculative findings, or demand exhaustive proof over unrelated code.
  Re-reviews verify prior fixes and re-audit the full current diff and relevant
  sibling paths, rather than only the most recent remediation commit. A new
  blocker in unchanged behavior must explain why it was not reasonably
  discoverable in the prior complete audit. Reviewers respond to author
  pushback on the PR itself. Each re-review publishes a cumulative disposition
  section that lists prior BLOCKING findings, the author's claimed remedy or
  response, and the current reviewer's independently determined disposition:
  fixed, reaffirmed, downgraded/follow-up, overridden/accepted, or unresolved.
  Findings first discovered in the current review are listed separately and
  retain the late-blocker explanation requirement above. Because Quorum does
  not currently have reliable structured extraction of PR review threads, the
  daemon requires this standardized section rather than synthesizing finding
  status. A daemon-provided ledger may later serve only as navigation context;
  the PR discussion remains authoritative, and neither a pushed commit nor an
  author's claim resolves a finding without the reviewer's current disposition.
  Both prior and new sections have fixed entry limits, and each field has a
  fixed Unicode-scalar limit. Explicit entry and field truncation directs the
  reader to the PR for omitted authoritative history; omitted current blockers
  still count in the verdict.
  Encouraged GitHub operations are the injected MCP's normal comments, pending COMMENT review,
  inline comments, review summary, thread replies, and reviewer-only thread resolution. Formal
  APPROVE and REQUEST_CHANGES reviews remain daemon-owned
  because managed reviewers use the same GitHub account as PR authors.
  Reviewers classify technical impact independently from merge disposition. A
  concrete finding is BLOCKING only when merging the exact change would leave
  the assigned primary outcome false, violate an applicable repository
  invariant, or introduce/materially worsen supported behavior, and its
  assumptions fit the established operating/threat model. Each blocking finding
  explains why the PR cannot merge, names the exact repository invariant it
  violates (or the precise assigned outcome left false or supported behavior
  materially worsened), and describes the broader affected path left unsafe
  rather than only its local symptom. Real pre-existing,
  adjacent, defense-in-depth, future, or stronger-threat-model concerns are
  FOLLOW-UP unless an explicit current contract makes them blocking. For
  documentation changes, reviewers require the smallest accurate statement of
  supported behavior rather than an exhaustive inventory of implementation
  exceptions; pre-existing edge behavior merely revealed by the change stays
  FOLLOW-UP when the primary outcome can remain accurate without cataloguing or
  fixing it. Follow-ups are recorded on the PR but never increase `--blocking`
  or prevent an otherwise valid approval.
- **Author/rework agents** address findings on the PR. If disagreeing with a finding,
  the author replies to it on the PR with concrete evidence rather than silently
  ignoring it. The final PR history must let a later collector determine, for each
  finding: fixed, accepted, overridden with evidence, or unaddressed. Replies use the
  referenced review thread rather than creating unrelated top-level comments.
- **Lifecycle signal only:** reviewers signal state with
  `quorum submit --verdict approved|changes --blocking N [--feedback ...]`. The
  submit payload is a lifecycle signal, not a second review ledger — the ledger is
  the PR. The `--feedback` string is preserved as rework-turn context for the warm
  worker but is not the authoritative record.
- **Daemon retains:** the final formal `gh pr review --approve` (posted from the merge
  account) and `gh pr merge`. Reviewer-owned APPROVE and merge remain forbidden.
- **Daemon writes** the formal REQUEST_CHANGES review from the merge account when a
  reviewer signals `changes`. This is lifecycle authority, not the findings ledger;
  the reviewer's inline and summary comments remain the source of truth.

This preserves #206 verdict attestation, reviewer separation, the rework cap, sticky
reviewer, the stale-SHA gate, and R1/R2 lifecycle. It shifts only who writes to the PR:
agents author the content and the daemon publishes it through run-scoped operations.

Initial daemon-created PRs render stable Outcome, Changes, Verification, Task, and optional
Notes sections. Task identity, accepted title, and accepted body come from daemon-owned task
state. Bounded worker delivery evidence fills the other sections; absent evidence is stated
explicitly. Existing-PR continuation and review-only entry do not overwrite an external body.

### R2 pre-merge review gate (#159, configurable sampling)

By default, every PR requires both R1 and R2 approval for the same head SHA
before merge. R2 sampling is opt-in: absent config resolves to
`r2_enabled = true`, `r2_target_per_stratum = 0`, and
`r2_steady_state_p = 1.0`, which preserves mandatory R2 exactly. Operators may
lower `r2_steady_state_p` (for example, to 0.30) and optionally set a per-stratum
coverage floor. A negative floor or probability outside `0.0..=1.0` is a usage
error; values are never clamped. `r2_enabled = false` disables sampling, not the
R2 safety gate, and therefore also leaves R2 mandatory.

R2 is skipped when R1 approves after `rework_round` has reached that task's
stamped rework cap (see the Rework cap bullet earlier in this document —
configured via `max_rework`, frozen per-task at daemon adoption, and falling
back to the compiled default only for legacy unstamped rows) and that PR head
has no prior decision. At that point no bounded rework round remains for an R2
changes verdict, so R1 approval is the final review gate. The skip is recorded
for that PR head through the same daemon-owned sampling-decision mechanism. A
prior decision requiring R2 remains authoritative if the branch later returns
to that head.

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
   proceed even without a live worker). Before provisioning, the daemon applies the
   pre-review CI gate to the current head SHA; a moved head must become green again
   before R2 consumes a slot. R2 reviews independently before comparing against R1,
   then checks for any material gaps R1 did not surface, if such gaps exist. Agreement
   with R1 and no additional findings are valid outcomes; findings remain evidence-bound.
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

**Severity and disposition contract** — both R1 and R2 prompts classify
technical impact separately from merge disposition using the reviewer
classification contract above: a finding is BLOCKING only when merging the
exact change would leave the assigned primary outcome false, violate an
applicable repository invariant, or introduce/materially worsen supported
behavior. Concrete resource exhaustion, unbounded growth, network calls in
DB transactions, data loss, corruption, security-boundary failures, and
stuck paths are presumptively major or critical impact, but their category
alone never decides merge disposition.

### Daemon-owned pre-review CI gate

Every reviewer provisioning attempt, initial or after rework, is gated by the daemon
against the current PR head SHA. Reviewer agents do not poll CI, run local
test/build/fmt/lint commands, inspect CI status, or police PR-body verification evidence,
formatting, transcripts, links, headings, evidence tokens, or checklists. They review the
implementation and its tests as code. The daemon alone owns CI enforcement for reviewer
provisioning and merge.

- `Ready` plus all configured `required_jobs` at `SUCCESS` permits provisioning.
- `Pending`, `TimedOut`, and pending required jobs keep the task `in-review`. The daemon
  polls in a background blocking task, consumes no reviewer identity or process slot, and
  makes no lifecycle transition.
- `Failed` or a completed required-job result other than `SUCCESS` fires
  `ChecksFailed` from the daemon and enters the existing rework/remediation path without
  spawning a reviewer.
- The gate is keyed by `(task, PR, head SHA)` and discarded after each provisioning
  attempt. R2 and sticky `ResumeReviewer` turns therefore recheck CI; a prior review
  cannot authorize any later review turn against a moved or newly failing head. A pending
  sticky resume is retained as lightweight daemon intent and retried without feeding the
  reviewer until the gate becomes ready.
- Immediately before acquiring or feeding a reviewer, the daemon re-resolves the
  authoritative PR head and requires it still equals the gated SHA. Reviewer worktree
  provisioning then verifies the fetched `HEAD` is that exact SHA. A mismatch discards
  the cached result and restarts gating without spawning a reviewer; the SHA recorded for
  stale-verdict detection is the same SHA that passed both checks.
- Before `ChecksFailed` commits `in-review → rework`, the daemon atomically persists the
  exact CI remediation PR, head SHA, failing checks, feedback, and bounded provision
  attempt count in task refs. Reaper and restart recovery preserve this rework intent,
  clear only stale runtime ownership, and retry remediation on the existing PR branch.
  Provisioning exhaustion parks loudly in `failed` with a `rework` resume marker; it never
  degrades to a generic `open` task or a fresh implementation prompt.
- Gate state is optimization-only memory. On shutdown it is dropped without mutating the
  task; restart finds the durable `in-review` task and polls again. Stateful lifecycle work
  is never raced inside `select!`.

The pre-merge checks wait remains as defense in depth for changes or check reruns that occur
after reviewer provisioning.

### Daemon merge flow

After VerdictApprove (InReview → Merging):
1. Check stale SHA — if reviewer recorded a head SHA and it differs from current, fire
   MergeFailed → rework cycle (prevents stale approval from authorizing a changed diff).
2. Check mergeability — if conflicting, MergeConflict → rework cycle. The daemon checks out
   the exact published PR head and merges the current base before worker launch; a conflicting
   merge remains in progress for the worker to resolve and commit. The worker never rebases.
3. Wait for CI checks — outcome classified into Ready / Failed / TimedOut. See
   § Merge-wait vs. actionable-rework contract (#173) below for the full disposition.
4. Persist approval record (instance-independent, survives restart).
5. **Pre-merge mergeability recheck (#153):** recheck PR mergeability immediately before
   the merge attempt — the window from step 2 through the master-CI gate can span minutes.
   If conflicting, fire MergeConflict → rework cycle. If mergeable, proceed.
6. Execute `gh pr merge` — success → Done; policy-blocked → Failed with a durable
   `merging` resume marker while retaining the exact-SHA durable approvals (explicit retry
   consumes one daemon-owned replay intent after live revalidation); retryable failure →
   invalidate approvals and enter rework.
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
| `TimedOut` | `Conflicting` | actionable (conflict) | `MergeConflict` → Rework directly (rework cap applies). Daemon prepares an ancestry-preserving base merge for the worker to resolve. |
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
6. **Must NOT delete the approval record** for a policy, credential, infrastructure, or
   pending failure. Delete it only when the merge succeeds or worker-fixable remediation
   invalidates the reviewed code boundary.

#### Preserved state during merge-wait

The following must remain intact throughout the wait and across restarts:

| State | Location | Why |
|---|---|---|
| PR number | `tasks.pr` column | Identifies the merge target |
| Required durable role approvals | `approvals` table | Restart recovery reconstructs exact-head review authority |
| Durable sampled-R2 decision | `r2_sampling_decisions` table | Proves whether R2 is required for this task/PR/head |
| Reviewed head SHA | `approvals.approved_head_sha` | Head-change detection (step 4b above) |
| Branch provenance | `tasks.branch`, journal row | Worker/remediation needs the branch |
| Dependency blocking | `tasks.depends_on` | Already-done deps stay done; blocked deps stay blocked |
| Task status = `merging` | `tasks.status` column | Restart recovery recognizes this as merge-pending |
| Mailbox row (if still unconsumed) | `mailbox` table | Recoverable delivery signal only; durable approvals remain authoritative without it |
| Explicit retry intent | `tasks.refs.daemon_merge_retry` | Bounds an owner retry to one daemon attempt |

#### Restart reconciliation

On daemon startup, before generic crash recovery:

1. **#228 approval recovery** runs first: scans `approvals` table, validates each role's
   verdict against the current PR head SHA via `next_missing_review_role(conn, pr, sha)`, and
   requires an exact task/PR/head row in `r2_sampling_decisions` before merge admission.
   If all roles approved for the current SHA → merge. If any role is missing or stale →
   defer to generic recovery (the approval is preserved or dropped per disposition).
   Before any network call the complete PR-wide row set must name one exact task and immutable
   author; mixed-task or mixed-author evidence is deferred fail-closed. Parked tasks and tasks
   carrying a merge-attempt marker are excluded: the former require owner authority, while the
   latter are handled by the bounded live retry reconciler or uncertain-attempt park. A policy
   failure from an unmarked startup replay is durably parked before recovery can run again;
   repeated starts therefore cannot create a merge loop. A worker-fixable replay result consumes
   its admitted boundary, invalidates approvals, and enters actionable rework with the durable
   remediation-retry marker that generic recovery preserves. Missing or differently-task-bound
   sampling evidence cannot be replaced by R1+R2 rows: startup atomically removes R1 plus the
   stale sampling row and returns a `merging` task to `in-review` for a fresh R1.
2. **Generic recovery** handles `merging` tasks: stays in `merging` only when
   `dual_approved()` confirms all required roles are approved for the same head SHA.
   Incomplete approval (e.g. R1 approved, R2 missing) resets the task to `in-review` via
   `AgentFailed`, so the tick loop provisions the first missing role (#191). The exception is a
   durable `daemon_merge_retry=requested`: generic recovery preserves `merging` even with
   incomplete evidence so the live reconciler can atomically consume the request, invalidate
   only stale evidence, and return to the first missing role without stranding the marker.
3. **Phase 5b** (orphan in-review tasks) checks for existing valid R1 approvals: if R1 is
   approved for the current PR SHA, it spawns R2 directly instead of re-running R1 (#191).
   Reviewer verdict persistence uses the task's durable author when no worker is live. When a
   repaired R1 returns and an exact task/author/head-bound R2 row was retained, that R2 is reused
   rather than provisioned again. Every persisted role remains bound to the launch-validated
   head even if the PR moves between that validation and the persistence write. Adoption of a
   stranded mailbox verdict likewise requires one unambiguous daemon-written `agent_runs`
   launch for the exact reviewer/task/PR and binds only to its immutable launch head; the current
   PR head is comparison input, never the SHA assigned to the approval.
4. **Explicit merge replay** atomically claims at most one `requested` marker per tick. A
   valid replay needs no live reviewer, mailbox row, or roster entry. Graceful shutdown lets
   an admitted call settle; startup parks an uncertain `attempting` marker without a second
   GitHub call and preserves its approvals for another explicit retry.

A delivered respawn-per-turn worker is not an orphan while its review is pending. Its
`awaiting-review` journal row has no PID and durably binds the agent/task, provider and exact
continuation, worktree, local and publication branches, and PR. Startup preserves that row and
worktree, verifies the live task claim plus task/run/capability/publication bindings, reserves the
same name, and reconstructs a dormant capacity slot without launching a provider turn. Missing or
mismatched identity is fatal recovery corruption; the row and name authority are not converted
into a fresh worker assignment. Startup accepts PID omission only for this explicit dormant shape.

Head-SHA invalidation on restart: the approval record stores `approved_head_sha`. On
re-entry, `head_sha()` is queried and compared. If different, the approval is stale —
`MergeFailed` fires and the task enters rework with a fresh review requirement. This
prevents a restart from merging code the reviewer never saw.

#### Actionable rework with no live worker

When an actionable outcome (Failed checks, MergeConflict, retryable merge failure, or a
blocking review verdict) enters `rework`, but no live worker exists for the task, the daemon
spawns a **remediation worker** (`spawn_remediation_worker` in `serve/mod.rs`). The
remediation worker:

- Gets the existing PR branch (resolved from GitHub via `resolve_pr_target`, which returns
  the authoritative head ref, SHA, and fork status; falls back to daemon branch convention
  when GitHub is unavailable)
- Gets the blocking findings / merge error as its rework prompt
- Is bounded by the same recovery policy as other workers (idle timeout, cost cap)
- Counts toward the worker cap and rework cap (`rework_round` was already incremented by
  lifecycle)

#### Workerless review-only Codex boundary

A review-only/adopted PR has no original managed worker by construction. For Codex only,
the daemon therefore permits a fresh `codex exec` remediation turn **only** when durable
`agent_runs` show that the task has no original managed worker and the task remains
`review_only`. This is not a fallback for an implementation task or a lost continuation:

- Before the fresh turn, the daemon resolves and persists the PR target, provisions the
  exact verified branch/SHA worktree, atomically claims the rework, and issues the
  run-scoped worker capability.
- On `thread.started`, the provider-issued thread ID is persisted in task refs before any
  later turn can depend on it. A later remediation is an exact `codex exec resume` of that
  ID with the persisted model, effort, prompt, PR target, and worker role.
- If an original managed worker exists, a Codex remediation requires its exact persisted
  continuation ID. Missing or malformed identity is a fail-closed provisioning failure;
  Quorum never silently starts a fresh thread for that implementation task.
- Once a workerless fresh remediation has been started, it has become a managed worker for
  this purpose. A crash or shutdown before a durable thread ID exists is also fail-closed,
  rather than authorizing a replacement fresh turn.

Deterministic remediation provisioning/configuration failure (including branch, worktree,
provider, or missing required continuation identity) releases the lease, persists the
blocking feedback, and parks the task in `failed` with its `rework` resume marker. It does
not emit `AgentFailed` back to `in-review`, so unchanged code cannot repeatedly consume
reviewer slots or rework rounds. An owner may explicitly `task-retry`; review-only retries
use the same persisted PR target and feedback through a dedicated reconciliation path, not
generic worker provisioning, and that path may fill only the remaining `config.cap` worker
slots in a tick.

During controlled shutdown/drain no fresh remediation is started. A running fresh
remediation is reaped through the normal worker cleanup path; its persisted thread ID
supports exact continuation after restart. If no durable ID was observed, recovery remains
fail-closed and leaves the parked task for explicit operator action.

#### Code paths (current implementation references)

| Concept | Location |
|---|---|
| `ChecksOutcome` enum (Ready/Failed/TimedOut) | `quorum/src/serve/merge.rs:67-74` |
| `MergeFailureKind` enum (StaleAuthority/Retryable/PolicyPending/PolicyBlocked) | `quorum/src/serve/merge.rs` |
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

**Pre-review CI gate:**

1. `pre_review_checks_pending_do_not_spawn_reviewer` — pending/not-yet-reported
   checks leave the task `in-review`, with no reviewer run or provision attempt.

2. `pre_review_checks_failed_enter_rework_without_reviewer` — failed checks fire
   the normal rework path with check names and no reviewer run.

3. `pre_review_checks_ready_spawn_reviewer` — green checks and successful configured
   required jobs permit R1 provisioning.

4. `pre_review_checks_restart_safe` **(restart-spanning)** — stop while the
   background check wait is pending, restart, and assert the durable `in-review`
   task is polled again before any reviewer spawns.

5. `r2_rechecks_current_head` — R1 approval followed by a moved or newly non-green
   head does not provision R2 until that current head is green.

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
    is `Conflicting`. Assert: `MergeConflict` → Rework, daemon-prepared base merge preserves
    the published PR head, and the worker resolves it without rebasing.

12. `retryable_merge_failure_triggers_rework` — merge attempt fails with
    `MergeFailureKind::Retryable`. Assert: one atomic `merging` → `rework` transition
    consumes the attempt marker and approvals while persisting actionable feedback; no
    intermediate `in-review` event is visible.

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

### Post-merge review interpretation and follow-up planning (#125)

Every successful merge kicks off a detached `serve::collector::run_collection` task
that classifies the finished PR into structured `review_findings` and an immutable
set of evidence-backed Follow-up Artifacts. The collector runs **after**
`MergeSucceeded` fires — the task is already `done` and the verdict is final — so
nothing it does can undo the merge or change the originating task or Task Graph.
Follow-up Artifacts may later produce separate future Managed Tasks through the
Planning Agent and daemon-owned materialization described in
`docs/2026-08-07-review-followup-planning-technical-spec.md`.

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
3. **Structured output** — response must be
   `{"findings":[...],"followup_artifacts":[...]}` with each
   finding carrying `kind` (blocking/suggestion), `author_pushback`,
   `pushback_accepted` (true/false/null), `addressed_status`
   (addressed/unaddressed/partial/unclear), and an `evidence` array of
   `{kind,id}` pointers to GitHub review/comment ids. Each artifact carries
   technical impact, scope relationship, concrete concern, non-blocking reason,
   affected behavior, desired outcome, verification expectations, and evidence.
   Prose-only or evidence-free findings/artifacts are rejected by contract.
4. **Atomic idempotent write** — one transaction replaces analytics, inserts the
   PR's immutable artifact batch only when absent, and UPSERTs the successful
   `review_collection_runs` row. Re-interpretation may refresh analytics but
   never rewrites, duplicates, or resurrects an existing artifact batch.
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
- Collection and follow-up planning never change the originating review verdict,
  merge, task, or graph state. Reviewers and Planning Agents never create tasks;
  only the daemon may apply a complete validated Follow-up Assessment.

**Follow-up assessment:** ordinary merged tasks become eligible after successful
interpretation. Eligibility requires daemon-owned `completion_provenance=merged`; a
manual close or legacy/unknown NULL provenance remains ineligible even when `done` retains
`refs.pr`. `MergeSucceeded`, externally observed `PrFoundMerged`, and authoritative
merge-recovery closure are the only merged-provenance writers; migration performs no
speculative backfill. Generated-child artifacts wait and are assessed together only
after the Task Graph completes, or after cancellation preserves a merged subset,
and every merged PR in the batch has successful interpretation. A fresh bounded
Planning Agent turn receives durable Planning Lineage, all active tasks, bounded
related completed work, current instructions, and a read-only repository view. It
must create, link, dismiss, or defer every artifact exactly once. There is no
semantic fingerprint table: grouping and comparison are planner judgments, and
ordinary task classification provides a second duplicate/readiness assessment.
The daemon applies all dispositions and task creations atomically; any failure
creates nothing. Detailed storage, retry, recovery, and evidence contracts are in
the focused technical specification.

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

## Built-in coding runners: Claude, Codex, and Grok Build

**Date:** 2026-07-24 (provider-neutral launch and recovery state 2026-08-05)
**Status:** Approved design; Claude and Codex managed lifecycle active; Grok Build
is enabled for managed workers only. Planner and reviewer roles remain gated.

### Decision and boundary

Quorum supports exactly three explicit built-in coding runners in this design:

- `claude` preserves the existing persistent Claude Code stream-json behavior.
- `codex` uses the stable non-interactive Codex CLI JSONL interface.
- `grok` uses the official Grok Build CLI's native headless `streaming-json`
  interface for managed workers only. Planner and reviewer roles remain gated.

This is a closed Rust enum, not a public provider trait or plugin API:

```rust
enum AgentKind {
    Claude,
    Codex,
    Grok,
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

Role orchestration starts a turn with the smallest provider-neutral request consumed by
the runner boundary:

```rust
enum LaunchMode {
    Normal,
    Restricted,
}

struct LaunchRequest<'a> {
    model: &'a str,
    effort: &'a str,
    worktree: &'a Path,
    prompt: &'a str,
    environment: &'a [(String, String)],
    mode: LaunchMode,
    continuation_id: Option<&'a str>,
}

enum AgentEvent {
    SessionStarted { id: String },
    AssistantText { text: String },
    Activity { kind: ActivityKind, summary: String },
    TurnCompleted { usage: Option<TokenUsage> },
    TurnFailed { message: String },
}
```

Before an authoritative managed-agent outcome, the runner boundary may also
attach one closed, provider-neutral failure disposition to proved startup or
early-exit evidence:

```rust
enum FailureDisposition {
    ProviderUnavailable, // authentication, account credit/quota, provider outage
    ProfileUnavailable,  // selected model/profile only
    RetryableSameRoute,  // transport/startup interruption
    NonFailover,         // execution/protocol boundary
    Unclassified,        // insufficient/internal evidence; fail safe
}
```

Provider adapters own any structured-code or bounded provider-specific message
classification. Unknown text remains `Unclassified`; conflicting evidence also
fails closed. Evidence is scoped to one managed turn even when Claude reuses a
persistent child: beginning a rework or re-review turn clears the prior turn's
success and failure observations. Once process exit is observed, the daemon
performs a bounded stdout drain and stderr-reader join before snapshotting the
disposition; failure to finalize either stream remains `Unclassified`. A terminal
provider success, managed completion/submission,
agent-reported failed/blocked/needs-info outcome, or review verdict prevents the
taxonomy from being applied. The disposition is evidence only at this stage: it
does not select a route, replace an assignment, consume an allocation, or change
lifecycle/recovery accounting.

The boundary resolves `model` through the closed `AgentKind` enum and explicitly
dispatches to one built-in adapter. It does not accept a caller-selected kind/model pair,
fall back between adapters, or expose a provider trait. Installed executable and existing
provider settings (`bare`/tool allowlist for Claude, sandbox for Codex, and the closed
Grok permission/sandbox profile) are adapter configuration, not part of turn identity.
`agent.rs`, `codex_agent.rs`, and `grok_agent.rs` alone translate the neutral request
into provider command specs, apply environment and execution mode, feed or embed the
initial prompt, and parse raw protocol lines. Restricted mode remains an explicit adapter
behavior: Claude uses `--setting-sources ""` to fully unload user/project settings, hooks,
and plugins (with the repo CLAUDE.md injected explicitly via
`--append-system-prompt-file` when present, and the planner's inline MCP server serving
normally), Codex uses its pinned read-only invocation without the normal sandbox bypass,
and Grok uses native `read-only` plus `dontAsk` with a reduced turn ceiling.

Do not mirror any CLI's complete schema. Preserve each raw JSON line in
`stream.jsonl`, parse only fields Quorum consumes, render a compact normalized
transcript, and ignore unknown events without advancing lifecycle state.

Task refs and dormant journal rows persist an opaque **runner continuation ID**. The journal's
`session_id` remains the daemon session identity; a dormant row uses the provider-tagged
`provider` and `continuation_id` fields so restart never infers one from the other:

- Claude receives a Quorum-generated UUID before spawn.
- Codex issues a thread ID in `thread.started`; Quorum persists it before relying on
  continuation.
- Grok issues a session ID in terminal `end`; Quorum emits and persists that identity
  immediately before terminal success and passes it back only through exact `--resume`.

Missing required continuation identity is an abnormal startup failure. Assistant
prose is never task completion.

A delivered turn-oriented worker remains a dormant logical slot while review is
in flight. If review requests rework, the daemon first atomically installs a new
task lease for that same agent, then revalidates the persisted continuation,
model/effort, worktree, PR, journal, prior run, and capability before launching
the next exact provider turn. Each resumed process receives a fresh agent-run row
and run capability; the completed turn's identities are retired. Codex resumes
only with `codex exec resume`. A missing or mismatched continuation never falls
back to a new thread, and launch failure stores the unchanged pending turn in the
existing durable provider-retry state before the slot is torn down.

Task refs persist runner recovery state as provider-tagged JSON objects. New writes use
`runner_continuation` (or the role-scoped `runner_reviewer_r1_continuation` /
`runner_reviewer_r2_continuation`), `runner_provider_block`, and `runner_retry`.
Continuation IDs are opaque. A retry records its provider, exact model and effort, raw
pending prompt, turn kind, optional continuation ID, and whether an operator requested
the retry. The block reason remains distinct from that pending-turn identity. A
provider/model mismatch, absent required continuation, or partial retry fails closed;
it never falls through to the generic initial prompt.

These refs are additive metadata, so no SQLite schema migration or backfill is required.
Readers continue accepting historical `codex_thread_id`, Codex reviewer thread keys,
and `codex_retry_*` / `codex_provider_*` records. Neutral records, when present, are
authoritative and must match the selected runner; historical assignments and evidence
are never rewritten merely to adopt the neutral representation.
Task creator APIs reject neutral `runner_*` and legacy `codex_*` provider-state refs;
creator and assignee metadata replacement also preserves existing refs in those namespaces.
Only the daemon-authoritative refs path may mutate or clear runner state. Managed dispatch
independently rejects any runner in an ineligible role recovered from durable state, so stale or
forged metadata cannot enable a planner, reviewer, classifier, or collector role.

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
  `AgentFailed` (or, for a blocked turn-oriented worker, durable provider-block recovery).

Consumed mailbox history is not sufficient while the same sticky run owns a later
round: an old initial submission cannot excuse a missing rework push, and an old R1/R2
verdict cannot excuse a missing verdict after re-review. Exit status and provider stderr
are retained as diagnostics, never lifecycle authority. The daemon classifies before
using failure language or alerts. Cleanup records `completed` for a recorded outcome,
`ownership_transferred` for a stale run, `crashed` only for a genuine owner-without-
outcome failure, and `provider_blocked` only after durable runner retry state is stored.

This classification is intentionally independent of observation order. In particular,
`submit → in-review → exit`, `submit → exit → in-review`, and
`exit → late submission recovery` must converge on one lifecycle transition and one
reviewer spawn. An initial worker's null-PR submission is a durable pending outcome
because the daemon, not the worker, resolves or creates its PR; the same convergence
applies to R1/R2 verdicts and remediation submissions. Cleanup
must not emit a second `AgentFailed`, duplicate `task_in_review`/`task_rework`, release
the new owner's lease, or classify exit status 0 as a crash merely because the
turn-oriented provider process ended.

### Official Grok Build transport

The Grok adapter uses only the official CLI's one-shot, read-only stdout protocol. It
does not use the ACP server, internal session files, executable-name inference, or an
emulation of Claude `allowedTools`/`bare`/hooks or Codex flags.

First turn, conceptually:

```text
grok -p <prompt> --output-format streaming-json --model grok-4.5
  --reasoning-effort <low|medium|high>
  --permission-mode bypassPermissions --sandbox <off|workspace>
  --max-turns <1..256> --verbatim
```

Continuation, conceptually:

```text
grok --resume <session-id> -p <prompt> --output-format streaming-json
  --model grok-4.5 --reasoning-effort <low|medium|high>
  --permission-mode bypassPermissions --sandbox <off|workspace>
  --max-turns <1..256> --verbatim
```

Exact argument order is pinned in fixtures and exercised against the installed binary.
Restricted launches are fresh turns using `read-only`, `dontAsk`, and at most eight
turns; restricted continuation is rejected. Normal configuration accepts only the
verified model, effort vocabulary, permission mode, sandbox profiles, and bounded turn
range. Unknown values and unverified safety combinations fail before spawn.

Consumed events:

| Grok `streaming-json` event | Normalized meaning |
|---|---|
| non-empty `text.data` | `AssistantText` |
| `tool_call` | compact `Activity` using `toolName`/`rawInput` |
| `end` with non-empty `sessionId` | continuation identity, then terminal success with complete usage/cost when available |
| `end` without a session ID | terminal failure |
| `error` | terminal failure with complete usage/cost when available |
| all other, unknown, or malformed lines | preserved raw and lifecycle-inert |

Every complete valid-UTF-8 stdout line at or below the one-MiB line bound is returned
byte-for-byte apart from the line terminator and written through the existing raw JSONL
path. An oversized line becomes an explicit truncation record. Invalid UTF-8 becomes an
explicit `provider.stdout_invalid_utf8` record that carries only byte counts/offsets and
is lifecycle-inert; bytes are never repaired into provider JSON. Stdout retained during
teardown, stderr lines, and bytes within an individual stderr line are separately bounded.
The process runs in its own process group. The adapter retains that group ID independently
of the leader's reap state, so teardown kills descendants holding inherited pipes before it
drains bounded output and reaps the child.

An `end` event is the protocol's success marker, but managed worker success additionally
requires exit status zero. `error`, non-zero exit,
EOF without `end`, missing session identity, timeout, and forced termination are failure
paths; the adapter never fabricates a terminal event from EOF or exit alone. Grok emits
the session identity late, so no continuation may be relied on before `end`.

#### Grok discovery record

**Verified facts (2026-08-05):**

- The installed official executable resolved to `~/.grok/bin/grok` and reported
  `grok 0.2.114 (0c785038798)`. Its help exposes `-p`/`--single`,
  `--output-format streaming-json`, `--resume`, `--model`,
  `--reasoning-effort`, `--permission-mode`, `--sandbox`, and `--max-turns`.
- The installed catalog exposed only `grok-4.5`; its supported effort choices were
  `low`, `medium`, and `high` (`high` default). The adapter therefore treats both
  vocabularies as closed rather than forwarding future strings optimistically.
- Native `streaming-json` is newline-delimited, type-tagged JSON. Official source and
  documentation define `thought`, `text`, `tool_call`, `tool_call_update`, `usage`,
  lifecycle/activity events, terminal `end`, and `error`. The successful `end` line is
  last and carries `sessionId` and `requestId`.
- Official usage fields distinguish uncached input, cache-read input, cache-creation
  input, and output tokens. The adapter reports total input as the sum of all three input
  buckets. `usage_is_incomplete` means the aggregate is not authoritative.
- `total_cost_usd` is emitted only when server cost is complete. Its absence means
  unknown, never zero. `cost_is_partial` or `usage_is_incomplete` suppresses normalized
  USD cost; Quorum does not derive prices or sum incomplete per-model rows.
- The CLI stores interactive/device authentication itself and supports `XAI_API_KEY`.
  Official precedence is model-specific configured key/environment key, then cached
  interactive/OAuth credentials, then the `XAI_API_KEY` fallback. Quorum inherits the
  CLI environment and credential state and neither stores nor prints credential values.
- Native permission modes include `default`, `acceptEdits`, `auto`, `dontAsk`,
  `bypassPermissions`, and `plan`. Native sandbox profiles include `off`, `workspace`,
  `devbox`, `read-only`, and `strict`; resumed sessions reject a changed sandbox. Quorum
  enables only the combinations above because the others have not passed lifecycle
  canaries.
- Isolated-home real-binary probes produced a structured `error` for missing/invalid
  authentication, rejected an invalid permission value during argument parsing, and
  entered the headless protocol with the pinned initial and resume placement without a
  successful model call. Some authentication failures did not promptly terminate after
  emitting the error, so bounded group kill/reap is part of the transport contract.

**Hypotheses and deliberately unverified behavior:**

- Grok has not passed attended real-CLI worker, remediation, R1, R2, restart, shutdown,
  mailbox, or cost-limit canaries. Unit/contract coverage enables worker routing only;
  production use remains gated on those canaries, and planner/reviewer roles remain disabled.
- Catalog additions, new reasoning efforts, `devbox`/`strict`/custom sandboxes, and
  permission modes other than the pinned profiles may become usable later, but are not
  accepted based on help text alone.
- Device login, cached interactive login, API-key success, and billable successful
  continuation were not executed in the zero-token transport probes. The adapter relies
  on the official CLI for those flows and makes no broader authentication or USD
  accounting claim.

### Capabilities and safety limits

Capabilities are fixed internal facts, not a negotiation framework:

| Capability | Claude | Codex | Grok Build |
|---|---:|---:|---:|
| resumable continuation | yes | yes | yes (workers only) |
| JSON event stream | yes | yes | yes |
| token usage | yes | yes | when complete |
| authoritative stream-provided USD cost | yes | no | no (not managed) |
| Quorum-managed CLI tool allowlist | yes | no | no |
| provider-native review skill | optional | not required | not enabled |

Never fabricate missing telemetry. Token, wall-clock, task-wall, and idle limits
continue when their data is observable. Codex does not expose reliable ChatGPT
subscription USD cost per turn. Grok workers use token and wall-clock bounds; no USD
pricing or cost accounting is enabled. If a Codex daemon is configured with a USD safety
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

Workers must work only on the assigned task, branch, and worktree; implement, verify,
and commit the outcome; signal through `quorum submit`; and never push, open or update
a PR, merge, or mark the task done. The daemon publishes the exact committed SHA with
an explicit refspec. For an existing same-repository PR it durably copies the
spawn-time PR head into the publication intent, resolves the live authoritative target,
and pushes only when the live head still equals that immutable baseline. A live head
already equal to the exact source is the idempotent post-push crash case; any third SHA
parks instead of being adopted as a new lease expectation. The push uses
`--force-with-lease=<head-ref>:<spawn-head>` and rejects any stale, fork, unavailable,
lease-rejected, or post-push SHA mismatch; only after verification may it transition
lifecycle. All publication-owned GitHub subprocesses have kill-and-reap timeouts and
fixed stdout/stderr byte limits so a hung or continuously verbose CLI cannot pin the
daemon tick, shutdown, or memory; exceeding either limit kills and reaps the child. For
an initial delivery it verifies
the new daemon branch under a zero/nonexistent lease, creates the PR, and verifies the PR
binds that exact branch/SHA. Publication intent and the `intent → pushed → pr_created →
verified` stages are durable task metadata. Startup recovery reuses an identical remote
branch and a single existing PR, then folds the exact mailbox row only after verification.
For an ordinary crash replay, the publisher takes the intent's immutable source SHA and
uses that object in the refspec; a later mutable worktree `HEAD` cannot change what is
published. Before persisting the intent, the daemon pins that SHA under a task-scoped
local Git ref, so parking may safely remove the failed run's worktree and run-local branch.
An explicit retry of a parked rework publication is a new delivery round, not an ordinary
crash replay. Its spawn accepts only the recorded remote baseline or a live head already
equal to the exact pinned source (the post-push verification-failure state); every third
SHA parks. In the already-published case, one SQLite transaction advances both the
persisted PR target and the publication intent's expected remote SHA to that exact source
before provisioning. The replacement worktree starts from the accepted live head and
integrates the current base. On completion, the daemon pins the replacement `HEAD` before
overwriting the intent's source, preserving the advanced PR/head authority; a crash before
intent persistence can only leave a harmless newer pin that reconciliation restores to
the still-durable source. Successful existing-PR worker lifecycle transitions guard that
the persisted PR target still equals the intent's prior lease baseline or already equals
the exact published source, then rotate that target to the exact source and retire the
intent in the same SQLite transaction as the lifecycle event, including the late-mailbox
fold. This makes the newly published source the next remediation round's lease baseline
while preserving idempotent post-push crash replay. The reachability pin is removed
afterward with an exact-SHA guard. Startup and bounded periodic reconciliation
walk minimal task-id/SHA projections in fixed cursor batches, restore missing or
mismatched intent pins, and exact-SHA-delete no-intent or terminal-task pins without
scanning the full task history or Git ref namespace in one pass. A retry from `pr_created`
repeats the same authoritative
branch/SHA/base validation before any push. Initial PR reconciliation
also requires the PR base to equal the task's immutable target branch, falling back to the
configured base only for a legacy targetless task. Rejected or ambiguous
publication parks the task; persisted PR target data is never authority for a publish
retry. This is protocol ownership plus a best-effort worktree `pushurl` lockout. A local
permissive run sharing the operator's OS identity still does not claim physical credential
isolation: an agent that can reach the operator's independent GitHub credential may bypass local
Git configuration. The isolated-runtime profile enforces the separate credential boundary by
giving the runner no GitHub token, authenticated CLI configuration, SSH agent, credential helper,
or credentialed Repository Service endpoint; only the daemon/broker performs remote writes.

Reviewers must inspect the full diff and relevant surrounding behavior, follow
repository instructions, classify BLOCKING and advisory findings, put authoritative
findings on the PR, submit a matching verdict, and never formally approve, merge, or
review their own delivery.

Claude may invoke its built-in review skill. Codex follows `AGENTS.md` and available
Codex skills, but Quorum does not require a particular built-in skill. Shared prompts
say "repository instructions" instead of `CLAUDE.md`.

### Configuration and model routing

Model routing is a repository-wide hard cutover. Every executable choice is defined once as
a stable model profile containing an exact provider model, supported runner, and effort.
Every required role then names a routing pool. Pool entries use positive integer percentages
that total exactly 100; fixed `agent`, `provider`, `model`, `effort`, `worker_model`,
`review_model`, `classifier_model`, and `collector_model` selection is not accepted.

```toml
[model_profiles.terra]
runner = "codex"
model = "gpt-5.6-terra"
effort = "high"

[model_profiles.opus]
runner = "claude"
model = "claude-opus-4-8"
effort = "high"

[routing.classifier]
terra = 80
opus = 20
[routing.planner]
opus = 100
[routing.arbiter]
opus = 100
[routing.collector]
terra = 100
[routing.worker.1]
terra = 100
[routing.worker.2]
terra = 100
[routing.worker.3]
terra = 80
opus = 20
[routing.worker.4]
opus = 100
[routing.worker.5]
opus = 100
[routing.reviewer.1]
terra = 100
[routing.reviewer.2]
terra = 100
[routing.reviewer.3]
terra = 80
opus = 20
[routing.reviewer.4]
opus = 100
[routing.reviewer.5]
opus = 100
```

Grok worker adapter configuration:

```toml
[grok]
sandbox = "off"                    # off | workspace (default: off)
permission_mode = "bypassPermissions"
max_turns = 64                      # 1..=256
```

`sandbox = "off"` is the production-capable default. Quorum provisions every managed
worker as a linked Git worktree, so the worker must be able to write the linked
worktree's `.git/index.lock` and the common-dir `FETCH_HEAD`, run repository hooks and
preflight, and invoke the scoped daemon completion endpoint. The attended task #53
canary (2026-08-19) attempted the previous `workspace` default against the linked-worktree
layout: the worker completed the requested README edit but its `git add`/`git commit`
could not create the linked-worktree `index.lock`, preflight's `git fetch` could not
write the common-dir `FETCH_HEAD`, and the generated `quorum_managed_workspace` profile
granted only the Grok state root. The default is therefore `off`. Operators may still
explicitly set `sandbox = "workspace"` to exercise the existing managed workspace
profile for transport experiments; the profile, its extension of the native `workspace`
sandbox, and the Grok state-root read/write grant are preserved for that opt-in path.
Broader Grok lifecycle validation (remediation, R1, R2, restart, shutdown, mailbox, and
cost-limit canaries) remains outstanding as recorded below.

`grok-4.5` may be selected only by a worker routing pool. Planner, arbiter, reviewer,
classifier, and collector Grok selections are rejected at startup. The `[routing.arbiter]`
pool selects the plan-review Arbiter and defaults to the planner pool when omitted, so an
existing configuration without it keeps working. The `[grok]`
section pins the adapter safety profile for those managed worker launches.

Never infer runner kind from the executable filename. Existing top-level
`no_bare_agent` and `allowed_tools` remain backward-compatible Claude settings.
Runner-specific configuration is scoped under `[claude]`, `[codex]`, or `[grok]`.

Each pool is materialized as a randomized 100-slot epoch containing exactly the configured
number of slots for each profile. The daemon persists the shuffled order and position before
use. Assignment creation and position advancement occur in one `BEGIN IMMEDIATE` transaction;
a restart cannot reroll the epoch or consume another slot. Pool membership or percentage
changes create a new policy generation and fresh epoch. Existing assignments retain their
profile snapshot. Allocation state is independent per repository, role, complexity pool, and
review stage: R1 and R2 use the same reviewer eligibility pool but separate bags and positions.

Startup fails before any claim when a profile is invalid, a required pool is absent or empty,
a profile is duplicated in a pool, a percentage is not a positive integer, a pool does not total
100, or legacy fixed-model routing is present. Never infer runner kind from an executable
filename. Runner-specific process options remain scoped under `[claude]`, `[codex]`, or
`[grok]`.

### Bounded task decomposition

A non-continuation, admission-ready implementation task is a decomposition source only when its
classified size is L or XL and `cx_est` is 4 or 5. After its dependencies are done, the daemon
serializes decomposition per repository: it stops new managed delivery, lets active delivery
finish, and plans against the resulting frozen base. S/M implementation tasks dispatch normally
regardless of complexity; non-continuation L tasks with `cx_est` 1–3 also dispatch directly to one
worker. Every `continue_pr` task dispatches directly because only the source task carries authority
to publish to the bound PR. Every review-only task dispatches directly to reviewer provisioning at
any classified size because it has no implementation work to decompose. A non-continuation XL task
with `cx_est` 1–3 violates the classification rubric and is parked with an explicit
reclassify-or-rescope reason.

Planning uses the profile selected from the planner routing pool. The planner receives a
read-only repository view and bounded source context but no network, database, coordination
command, or delivery authority. Planning is source-directed: repository inspection starts from
paths and symbols named by the source, follows observed calls by at most one hop, and has bounded
search/read guidance. The provider process has a 600-second wall-clock limit, a 128 KiB prompt
limit, a submitted-plan limit inside the endpoint's bounded frame, and a 16 MiB cumulative
streamed-stdout backstop; no provider spend ceiling is set. The response remains structurally
bounded to 8 KiB per text field and 32 list items. A separate planner spawn boundary enforces
those restrictions and accepts only one bounded, closed plan or blocker response; a provider whose
transport cannot be separated from model-generated network or filesystem access is refused
fail-closed. The view is an archive of the recorded frozen base SHA,
and source drift is rejected before launch. A valid concrete blocker parks the source immediately
with no second opinion.

The planner reports its plan by calling the daemon-owned `submit_plan` tool exactly once, over
the same authorized agent endpoint workers and reviewers submit through; its transcript is never
parsed. One identity covers the whole attempt: the daemon mints one run id and, in a single
transaction before the spawn, issues that run's `planner` capability and records the same id as
the graph's live planner session. A submission is accepted only when the capability, the graph's
planner session, and the submitting run all agree. The endpoint validates the plan at call time
and returns its own validation message, so the planner corrects the cited defect and resubmits
within the same turn; rejected calls are bounded per run, the first accepted submission stands,
and the capability is revoked when the attempt ends. On process exit the coordinator reads what
was durably accepted: a submission is the outcome however the process then ended, and a turn that
ends with none is a provider failure — printing a perfect plan as text delivers nothing.

Codex planning is supported only through its hardened planner-specific boundary: the CLI runs
read-only without coordination authority, and a bounded JSONL terminal response remains only a
candidate until stdout reaches EOF, final diagnostics are bounded and complete, and the process
exits successfully. Any contradictory or incomplete terminal evidence fails the turn. Grok
planning remains refused because Grok is enabled only for managed workers.

A plan contains 2–8 proposed implementation tasks and an acyclic prerequisite graph. Each child
names its concrete implementation delta, affected paths, and non-goals, and carries every
load-bearing source requirement forward faithfully in the child that owns it. Tasks follow
independently deliverable code or ownership seams; preserved
behavior and regression-only expectations remain criteria or non-goals rather than synthetic
implementation work. Before any task row is created, deterministic validation checks the closed
shape, references, cycles, prohibited synthetic integration work, and the structured deliverables
manifest (below); it no longer requires a byte-exact echo of source-marked literals. Plan
faithfulness — that every load-bearing source requirement and constraint is carried forward,
nothing is dropped or silently weakened, the children cover the source without overlap, and the
plan is coherent — is judged by the Arbiter plan-review gate (below), not by a deterministic
literal match. Only after the Arbiter approves is the complete proposal classified as one batch.
Every child must be
admission-ready, nonduplicate, and size S or M under the same execution-size rubric given to the
planner. Admission readiness means the scope is sufficiently clear for delivery; it is distinct
from runtime readiness, which still requires dependencies to be done.

**Arbiter plan-review gate.** Between deterministic validation and materialization the daemon
gates every structurally valid proposal on a single-shot, stateless **Arbiter** — a model
reviewer spawned fresh per proposal in the same frozen read-only repository view the planner used,
selected from the `[routing.arbiter]` pool (defaulting to the planner pool). The Arbiter judges the
proposal against the authoritative source on four mandates — faithfulness, coverage and
non-overlap, coherence, and decomposability — and emits exactly one closed verdict, parsed with the
planner's fail-closed discipline. The Arbiter only emits a verdict; the daemon alone transitions
lifecycle and materializes children (lifecycle authority stays with the daemon). Verdict mapping:
*approve* (or a *changes* verdict with no blocking finding) advances the graph from `validating`
to `preclassifying`, and the unchanged classification/materialization path then creates the
children; *changes* with at least one blocking finding records a normal `proposal` attempt whose
summary is the Arbiter's findings and returns the graph to `planning` so the planner re-proposes
against those findings; *reject_source* holds the graph and fails the source for a required
owner decision, materializing no children; and a malformed, absent, or provider/protocol-failed
verdict records a `provider` attempt (fail closed — never a silent approval). Every terminal
Arbiter outcome also writes exactly one additional `decomposition_attempts` row with
`kind='verdict'`: its reason code is `arbiter-approve`, `arbiter-changes`,
`arbiter-reject-source`, or `arbiter-provider`, and its bounded JSON summary contains the verdict,
findings (`severity` and `summary`), blocking count, duration, response bytes, assistant-event
and tool counts, plus provider, model, and effort. This observational verdict row neither changes
lifecycle nor consumes a budget; only `proposal` and `provider` attempts count toward their
existing limits. Its JSON stays valid while being reduced to the existing 2 KiB attempt-summary
cap. A *changes* verdict consumes the existing three-per-revision proposal budget; a provider
failure consumes the separate provider budget. The gate is keyed on the guarded `validating ->
preclassifying` transition, so a daemon restart that re-spawns and re-polls a fresh Arbiter cannot
double-materialize.

Each proposed child also carries a bounded structured deliverables manifest that distinguishes
requested writes from read-only contextual references. Deterministic validation rejects a write
using parent traversal or resolving outside the canonical managed repository, including an
in-repository symlink escape. External read-only references remain permitted because they grant no
write authority. Lexically external absolute writes cause no filesystem access. Required symlink
inspection runs off the serial daemon tick on at most one dedicated OS resolver thread with no
queued retry: a hard timeout fails closed, and the occupied resolver slot rejects later proposals
until the filesystem call actually returns. Resolver stalls therefore cannot consume Tokio's
shared blocking pool or delay database work. This admission check neither trusts planner
self-attestation nor redesigns the worker filesystem sandbox.

Semantic proposal rejections and provider/protocol failures have independent caps of three per
unchanged source revision. Semantic retries keep the repository freeze. Provider failure releases
the freeze during backoff, and retry drains again. Full prompts and transcripts are not persisted;
only bounded structured attempts, the accepted closed proposal needed to resume validation or
preclassification, and final reasons are durable. Restart inspection never consumes a semantic
proposal attempt.

Exhausting either three-attempt planning budget holds the graph and fails the source; it never
retries automatically. `task-retry` may explicitly resume that same aggregate only while a single
`BEGIN IMMEDIATE` transaction proves the source revision is unchanged and unassigned, the graph
is held without active/freeze authority, the hold code and exhausted counter agree, the source is
neither review-only nor a continuation, and no accepted plan, member/child, process, run, PR, or
delivery/merge evidence exists. The winner restores the source to `planning`, clears only the
retryable hold/session/base fields, resets the exhausted current-generation counter, and places the
graph in provider backoff so the coordinator must reacquire `one_planning_freeze` through the
ordinary guarded path. Semantic blockers, inconsistent holds, stale authority, materialization,
delivery evidence, and racing callers are clean negatives.

Coordinator snapshot selection always prioritizes the unique `freeze_active=1` aggregate before
older provider-backoff aggregates. An explicitly retried graph therefore cannot shadow, stall, or
consume output from a later graph that already owns the planning freeze; backoff graphs resume in
ID order only when no freeze owner remains. A live planner or classifier slot also pins its
in-memory graph identity: any mismatch with the durable selected graph is a loud internal failure,
never permission to consume that process result for a different aggregate.

Each graph permits at most two operator planning retries. Attempts remain append-only: their
ordinals increase monotonically across the unchanged source revision and each row records the
operator retry generation (historical rows are generation zero). Thus each proposal/provider
ledger is bounded at nine rows per graph. A successful retry emits one durable
`decomposition_planning_retry` event naming the operator and generation; cap exhaustion is an
actionable clean negative. Status exposes the hold code, whether it is currently retryable, and the
retry count/cap without exposing planner transcripts.

Reviewer provisioning reserves task authority in `BEGIN IMMEDIATE` before external resolution or
process creation. The same transaction checks task phase, classification, and planning freeze;
planning freeze acquisition excludes live reservations. The daemon releases the reservation only
after reviewer attachment or complete failed-provision cleanup.

One `BEGIN IMMEDIATE` transaction creates the entire graph: every generated task, classification,
edge, membership/provenance row, source `planning -> decomposed` transition, and active-graph
record. It then releases the freeze. Partial materialization is never visible. A partial unique
index permits one materialized active or blocked graph per repository, and source uniqueness
permits only one graph aggregate per source. Generated work is one level only and is immutable
after materialization.

Generated tasks retain ordinary independent implementation, review, rework, and protected merge.
Their atomic claim additionally requires an active decomposed source, done prerequisites, no
failed sibling or graph blocker, and fewer than two active implementation siblings. Eligible graph
children sort before unrelated new work, but reserve no idle capacity and never interrupt active
unrelated work. Active siblings may finish after another child fails; no later child may start.

A reviewer may submit a capability-bound, closed graph-blocker verdict only for a genuine safety
or authority boundary violation: a change that would grant authority, break restricted-role or
phase isolation, escape the managed repository, or expose secrets. A correct, safe change that
requires a bounded edit outside a generated child's `write` deliverables, including a
`read_only_reference` path, instead receives a BLOCKING `changes` verdict. That feedback names the
specific required edit and explicitly authorizes the rework worker to make the minimal, justified
edit in the named file or files, treating the assigned file list as advisory for that remediation.
After validating a graph-blocker against the current run, head, membership, and evidence, the
daemon atomically fails the affected child and blocks the graph without consuming ordinary rework.
The source remains decomposed and the blocked graph remains active until source cancellation;
recovery requires a replacement source, not automatic replanning after delivery has begun. The
only exception is the explicit, evidence-bound adoption of an already merged continuation
described below when the `boundary-violation` hold names the adopted child as its
`affected_task`; that operation completes the already-delivered graph, but never unblocks,
replans, or grants new execution authority.

A narrow incident-recovery primitive may adopt the exact merged delivery of a done managed
continuation task for the final failed member of an otherwise complete live graph (`state` active
or blocked, with `active=1`). The automatic path's immediate transaction requires the same repository and PR,
creator-selected `continue_pr`, explicit `source_task` provenance, live daemon publication and
merge events (`expires_at > now`), immutable managed-review authority, and one consistent PR
target/approved head SHA. It changes only the failed child and records recovery provenance while
preserving the PR; on a blocked graph it leaves the graph blocked and active. Missing or expired
evidence, replay, and losing concurrent callers are clean no-ops with no events; the winner emits
bounded child-completion events once.

For a coordinator/operator-selected incident pair, `quorum decomposition-adopt-recovery
--original-child-id <child> --recovery-task-id <continuation> --by <operator>` is the sole explicit
recovery-authority surface. The same `BEGIN IMMEDIATE` transaction rechecks active membership,
failed/final-child state, repository and PR identity, creator-selected continuation authority,
and exact target/head agreement. It permits absent `source_task` metadata because the caller has
named the exact pair, but rejects conflicting metadata. Instead of expiring feed events it requires
the durable daemon chain: the final assigned worker run is either `completed` before the persisted
final PR target or `merged` (which may end after that target resolves), an assigned approved
reviewer bound to that exact target head and sampling decision, and merged
completion provenance. Success writes the operator, source, child, recovery task, PR, and head to
the decomposition recovery ledger and child recovery projection before final-child completion. On
an active graph this is ordinary completion. On a blocked graph it is permitted only when the hold
is `boundary-violation` and its JSON `affected_task` exactly equals the named failed child; the
same transaction then clears that hold, completes the graph, and completes the source after
confirming every member is done. A block for any other child, malformed hold, or any other hold
code remains non-adoptable. It does not discover candidates, infer equivalence, or weaken the
automatic path's event rules. A rejected pair, replay, or concurrent loser is a clean negative
with no provenance or lifecycle event.

One legacy prepublication shape is also eligible through that exact operator surface. It exists
only for a generated final child that an older retry reopened after daemon-owned publication had
already failed, leaving the live graph blocked by the corresponding legacy non-JSON
`generated-child-failed` summary. The child must be unassigned `open`, have no completion or PR
target, and retain a task-scoped `daemon_publication` intent with no PR, remote SHA, or resolved
target and with a valid preserved local commit SHA. The publication branch must match the exact
daemon-authored grammar `daemon/<author-slug>-t<child-id>`, where `<author-slug>` is a non-empty
lowercase ASCII `[a-z0-9-]+` run and `<child-id>` is the exact child ID. The hold must name that
exact child and branch; all siblings must be done. The operator-named managed continuation must
postdate the stale child, satisfy the ordinary durable worker, approved-head, sampling, and
merged-provenance chain, and be merged to the decomposition source's
immutable target branch. This is not a general open-task adoption path: any structured/current
hold, assigned child, advanced publication state, target mismatch, stale PR row, malformed
task-scoped branch (including non-daemon prefixes, empty slugs, nested paths, whitespace,
uppercase letters, dots, and other invalid characters) or SHA is a clean negative. Classifier duplicate hints grant no lifecycle
authority and are neither required nor sufficient. Success
records the preserved publication branch and SHA in both recovery audit projections, removes the
stale publication intent, completes the child, clears only that exact legacy hold, and completes
the graph and source in the same transaction.

After graph consistency reconciliation and before generic stateless lifecycle recovery or
provisioning, the daemon automatically discovers eligible event-backed deliveries at startup and
on ordinary ticks. Discovery
consumes at most eight physical rows from the durable lifecycle-event sequence in ascending `seq` order,
then resolves explicit `source_task`, graph membership, PR targets, and live publication/merge
evidence using short reads only. It never scans terminal-task history or performs network I/O.
A dedicated monotonic cursor is acknowledged only after every guarded adoption call and durable
retry-marker write in the page. If the short read observes an unfinished sibling and the core
remains a clean negative, the daemon records one idempotent, TTL-bounded marker for the exact
child/recovery pair. A later sibling `task_done` event drains live markers oldest-first through a
second monotonic cursor, with at most eight candidate applications per pass. If the marker batch
fills that capacity, the sibling trigger remains unacknowledged while the pending cursor advances,
so later pairs receive a bounded subsequent pass. Partial graphs consume the current sibling
trigger without advancing pending markers and wait for the next sibling completion. Every settled
page advances, so a stalled graph cannot starve later deliveries. A crash before application or
after the core, marker, or pending-cursor commit replays safely. Startup discovery failure is
logged and fail-open; tick failure
uses the ordinary tick error policy without advancing the cursor. The guarded transaction remains
the sole adoption authority. The complete predicate is specified in the decomposition technical
specification.

The final child merge marks the source and graph done in the same transaction that marks that
child done. Source dependents remain blocked until then. Cancelling a source atomically makes
the graph non-runnable, cancels unfinished children, revokes authority, and records idempotent
cleanup intents. The daemon then stops processes and closes/removes only unmerged, revocable
artifacts. Lifecycle history, reviews, and merged delivery records remain. Direct child
cancellation is rejected. Source cancellation is the universal terminal escape hatch: it accepts
any non-terminal graph state — including the pre-children planning states (`freeze-requested`,
`draining`, `planning`, `validating`, `preclassifying`, `provider-backoff`, `held`) as well as
`active` and `blocked` — and atomically clears any pending planner freeze/backoff/hold, tearing
down whatever is in flight. It refuses only on authority mismatch (creator/assignee), stale
`--expected-revision`, or an already-terminal graph (`completed`/`cancelled`); internal graph state
is never a cancellation gate. This preserves the wider invariant that a non-terminal task is always
cancellable by an authorized caller. Cleanup intent execution is a durable lease state machine
(`pending -> running -> done|pending|exhausted`) with a three-attempt cap. Startup returns an
interrupted running lease to pending below the cap and exhausts it at the cap. Claims are atomic,
require a cancelled inactive graph and current graph membership, and preserve per-task action
order: process, proposed change, worktree, then branch. Malformed, unknown, or oversized persisted
intents exhaust loudly without external execution; completion and failure are guarded by the
claimed attempt so stale workers cannot settle a newer lease.

Task revisions use compare-and-swap edits. An accepted pre-materialization edit invalidates
pending classification/planning and restarts admission; stale/replayed edits do not count. The
fourth accepted edit is rejected. Materialized source/child scope and graph dependencies cannot be
edited. Daemon-owned lifecycle evidence remains writable.

Decomposition reconciliation runs before merged-continuation adoption, ordinary recovery, and
provisioning. A durable freeze
resumes first. Complete graphs resume without recreation. Incomplete or inconsistent graphs start
nothing; an unstarted graph may reset and replan within budget through the normal freeze/drain
path, while any delivery evidence requires cancellation and replacement except for the
evidence-complete exact-continuation case above. Its automatic daemon discovery is a bounded
reconciliation action on a consistent live graph. On a blocked graph it may only settle that exact
evidence-complete delivery; it leaves the graph blocked and source decomposed, grants no new work
authority, and does not complete the graph. The core transaction remains fail-closed and does not
broaden graph-blocker repair or replacement authority. Read-only status exposes bounded
membership, edges, progress, attempts, provenance, and blockers. The complete storage, protocol,
and recovery contract is in
`docs/2026-07-31-task-decomposition-technical-spec.md`.

**Classifier-owned per-run model selection.** Task creators describe the outcome,
constraints, and verification but have no routing authority. `task-create` rejects every
`complexity:*`, `tier:*`, and `effort:*` label with usage exit 2. Existing stored routing
labels are ignored.

- No managed worker or reviewer is dispatchable until the daemon classifier has persisted
  valid `cx_est` (1–5), `cx_size` (`S`, `M`, `L`, or `XL`), and `cx_ready` fields in task
  refs. A false readiness result carries a concrete `cx_not_ready_reason`; missing,
  partial, or malformed classification never falls back to worker or reviewer
  provisioning.
- The classifier is closed-book: it receives bounded task/dependency/recovery context but
  does not inspect source, Git, CI, or external systems. Readiness is permissive: ordinary
  repository discovery and bounded engineering choices are execution work, not a reason to
  reject the task.
- Direct dispatch and decomposition partition admission-ready implementation work. S/M tasks
  dispatch directly for every valid `cx_est`; non-continuation L tasks dispatch directly at
  `cx_est` 1–3 and decompose at 4–5; non-continuation XL tasks decompose at 4–5 and park at 1–3
  as a rubric mismatch. A `continue_pr` task always takes the direct route and never the
  decomposition route, regardless of classified size. A review-only task likewise always takes
  the direct reviewer route at any classified size. The daemon atomically parks an unready
  classification. Parking writes the standard refs, note, and event with no claim, run, or error
  row; an explicit retry requests reclassification of remaining work and a newly dispatchable
  result restores the saved lifecycle status.
- A new worker assignment selects from the complexity-specific worker routing pool. Task
  creators cannot lower, raise, or choose an individual profile.
- `resolve_provider` maps the selected model to `AgentKind::Claude` (any `claude-*`
  model), `AgentKind::Codex` (known OpenAI models including `gpt-5*`), or the exact
  `AgentKind::Grok` model `grok-4.5`. Managed worker resolution accepts Grok;
  planner, reviewer, classifier, and collector routing reject it.
- The resolved provider, model, and effort are persisted in `agent_runs.provider`
  so continuation and recovery cannot switch providers mid-task.
- A new R1 or R2 assignment selects from the complexity-specific reviewer pool. Review
  stages retain independent bags, and allocation never changes which stages are required.

There is no cross-runner strength ladder. Eligibility and percentages are explicit in each
pool, and a profile's effort is part of its durable executable snapshot.

### Delivery sequence

1. **Extract the runner boundary.** Move current Claude spawn/parsing behind it,
   normalize consumed events, preserve raw JSONL, and prove Claude behavior unchanged.
2. **Add Codex parsing and commands.** Fixture-test consumed events, negative terminal
   paths, unknown events, command shapes, and zero-token real-CLI argument validation.
3. **Enable Codex workers.** Prove initial work, submit, rework continuation, restart,
   watchdogs, auth/quota failure, and unsupported-USD-limit rejection.
4. **Enable Codex R1 and R2.** Prove changes/rework/re-review, stale-head rejection,
   self-review prevention, daemon-owned CI wait, approval, and merge.
5. **Cut over configuration.** Require profiles and complete routing pools, reject legacy
   fixed-model keys, and install runner-appropriate Quorum guidance.

Classifier, doctor, review interpreter, and analytics collector are not initial Codex
parity requirements. They remain Claude-backed or disabled until the primary
worker → R1 → R2 → merge lifecycle is proven. Mixed-runner behavior is never inferred.

### Assignment continuity and evidence

A role assignment is created once for a new responsibility. Restart, continuation, retry,
re-review, and rework reuse its persisted profile and consume no allocation slot. Removing a
profile from current policy cannot make its existing assignments unrecoverable. Classifier,
planner, collector, worker, R1, and R2 outcomes extend the existing canonical run, usage,
review, and outcome evidence with assignment/profile identity; routing does not create a
parallel statistics subsystem. Unknown models, unavailable runners, missing continuation
metadata, or a profile snapshot that cannot be executed fail loudly through the existing
bounded retry or parked-task path, never by silent substitution.

Each distinct configured route attempted for a responsibility may add one immutable evidence
row linked to, but never replacing, the original role assignment. The row snapshots the exact
profile and optionally records the closed pre-authoritative failure disposition. Replay is
idempotent by assignment and profile, and the eligible pool bounds the total distinct rows.
Exclusions are derived rather than separately granted: `ProviderUnavailable` excludes every
profile on that provider for the responsibility, while `ProfileUnavailable` excludes only that
profile. `RetryableSameRoute`, `NonFailover`, `Unclassified`, and an attempt with no classified
runner failure grant no alternate-route authority. Recording does not advance allocation,
`rework_round`, task recovery attempts, or lifecycle state. Alternate selection and launch are
not yet activated.

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
| **Daemon-managed run** | `QUORUM_RUN_ID` env var set by `quorum serve` at spawn time. Each run ID is a unique opaque token tied to exactly one `(run_id, task_id, role)` triple. The daemon records it in `agent_runs`. | `submit`, `react`, and progress-note append for its own task only. The run ID is verified against `agent_runs` — a run can only signal or append a note on the task and role it was spawned for. All public commands are also available. |
| **External named caller** | Any invocation without `QUORUM_RUN_ID`. Identified by `--agent <name>`. | Public commands only (see table below). |
| **Operator / admin** | Human or privileged script. No special token — admin commands are inherently manual and loud. | Public + admin commands. |

The daemon generates a unique run ID (128-bit hex) for **each** spawned
worker/reviewer and injects `QUORUM_RUN_ID=<id>` into its environment. The
run ID is recorded in `agent_runs` with the associated `task_id` and `role`
(worker/r1/r2). CLI commands that require run identity (`submit`, `react`,
and note-only `task-update`) verify `QUORUM_RUN_ID` against `agent_runs` — a
worker for task #5 cannot submit or append a note on behalf of task #7.

The daemon-owned local endpoint accepts only bounded, framed JSON operations:
`submit`, `react`, `append_note`, and `submit_plan`. `append_note` accepts one
non-empty, NUL-free note plus the prompt-compatible task and agent identity
flags. Of those, a `planner` capability is honored by `submit_plan` alone; it is
also honored by the inventory query, which answers phase `planner` with an empty
operation list so the MCP shell advertises `submit_plan` and nothing else. Every
other operation rejects that role, and a worker or reviewer capability cannot
submit a plan. The
live run capability remains authoritative, and the endpoint rejects a flag
that does not agree with its derived task or agent. It does not expose task
field mutation, refs, dependencies, status, SQL, or arbitrary task updates. A
revoked, ended, unknown, mismatched, or phase-ineligible run is rejected
without a write.

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
| `task-update --note-stdin\|--note-file` | Append one progress note through the scoped endpoint. The supplied task and agent flags are compatibility inputs, which must agree with the task and agent derived from the live run capability. |
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
  is orthogonal to agent stop/kill and is unchanged. See "Self-update (exit 75
  contract)" below for the staleness trigger and exit path.

Daemon scheduling pause/resume (pausing the daemon's spawn loop without
stopping individual agents) is **out of scope for v2** — drain covers the
"stop spawning new work" use case.

### Self-update (exit 75 contract)

`quorum serve` never patches itself in place. It exits with a reserved code,
`EXIT_SELF_UPDATE = 75`, and `scripts/serve-supervisor.sh` (or an equivalent
supervisor) treats that code as an upgrade signal: fetch
`origin/<self_update_branch>`, fast-forward merge, rebuild via
`./dev-install.sh`, and relaunch. Any other exit code propagates and stops the
supervisor loop — a crash is not an upgrade signal.

`base_branch` and `self_update_branch` have separate responsibilities.
`base_branch` selects the task/PR base used for worktree provisioning, PR
publication and validation, and merge targeting. `self_update_branch` selects
only the daemon build-staleness poll and the supervisor's fetch/rebuild source.
For example, `base_branch = "develop"` with `self_update_branch = "main"`
creates and merges task PRs against `develop` while rebuilding the daemon from
`main`.

For backward compatibility, when both the `--self-update-branch` CLI flag and
the `self_update_branch` config key are omitted, the self-update branch is the
fully resolved `base_branch`; a config that only sets `base_branch =
"develop"` therefore polls `develop`. An explicit CLI
`--self-update-branch` or config `self_update_branch` decouples the two.

The bundled supervisor resolves its rebuild branch from
`QUORUM_SELF_UPDATE_BRANCH`; it passes that branch to daemon staleness checks
as well as fetching it after exit 75. `QUORUM_BASE_BRANCH` remains a legacy
fallback only when `QUORUM_SELF_UPDATE_BRANCH` is unset, and `main` is used
when neither is set. A caller-supplied `--self-update-branch` takes precedence
over either environment variable.

`EXIT_SELF_UPDATE` has three supported triggers:

1. **Build staleness.** With `--self-update-drain`, the tick loop periodically
   (`sha_poll_interval_secs`, default 600 seconds) runs bounded
   `git ls-remote origin <self_update_branch>` outside DB work. When the
   remote self-update SHA does not match the running build SHA, it requests a
   self-update drain. An unavailable remote or an unidentifiable build SHA is
   logged and leaves the daemon serving.
2. **Schema too new.** A tick reporting `QuorumError::SchemaTooNew` means the
   on-disk DB is newer than the binary can read. The daemon cannot safely
   perform DB-backed cleanup, so it force-kills its in-flight
   worker/reviewer/planner/classifier processes and exits 75 for a rebuild
   and relaunch.
3. **Normal-tick merge.** When both `self_update_drain` and `self_repo` are
   configured, a successful merge performed by the normal tick merge executor
   requests the same self-update drain immediately after `MergeSucceeded`.

A self-update drain is bounded and shallow: ordinary worker/reviewer
provisioning respects the drain state, and the daemon drains its
worker/reviewer roster without waiting for their tasks to finish. The daemon
exits 75 when that roster becomes empty or `drain_timeout_secs` expires; at
timeout it force-kills the remaining worker/reviewer/planner/classifier slots.
Restart recovery then applies the normal durable lifecycle rules. This is
deliberately not a guarantee that every task reaches a terminal state before
handoff.

The supervisor handles exit 75 by fetching `origin/<self_update_branch>`,
fast-forwarding the checkout, running `./dev-install.sh` (with its bounded
build timeout), and relaunching. A failed fast-forward or build alerts and
relaunches the existing binary; its restart-thrash guard is bounded. Other
daemon exit codes propagate and stop the supervisor, so a crash is never
treated as an upgrade signal.

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

12. **Per-run capability gate.** `submit`, `react`, and note-only managed
    `task-update` require `QUORUM_RUN_ID` matching an active row in
    `agent_runs` for the correct `(task_id, role)`. Absent or mismatched →
    exit 2. The run ID is per-spawn, immutable, and dies with the agent
    process.
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
- **Local web dashboard:** `quorum web` is a separate, read-only loopback-only process,
  never part of `serve`. It exposes no mutation endpoints and does not authenticate or
  permit remote binding; remote delivery requires a separately designed secure transport.
  Each request opens, reads, and closes SQLite before responding. Dashboard task and run
  pages are bounded, stream reads are byte-capped, and `--log-dir` selects the daemon's
  configured log root.
- **Out of scope (YAGNI, v1):** general auth · multi-machine coordination · remote HTTP/MCP server ·
  message editing · threads beyond `topic` · PR/review mirroring · cross-repo bus ·
  presence-based claim eviction · arbitrary-byte (BLOB) payloads · **agent-name uniqueness
  enforcement** (v1 is caller-owned first-use-wins; same id silently merges — a v2 could
  reject re-use of an active name from a different session, or hand out names itself).
