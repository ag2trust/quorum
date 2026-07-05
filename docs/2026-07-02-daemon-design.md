# Quorum Agent-Manager Daemon — Design & Plan

**Date:** 2026-07-02
**Status:** M0–M7 implemented and merged

## Overview

The agent-manager daemon (`quorum serve`) is a long-running process that coordinates
AI agent workers and reviewers. It automates the full lifecycle: pick up a task →
spawn a worker → receive its PR → spawn a reviewer → process verdicts (approve → merge,
changes → rework) → teardown. The daemon is the only process that holds merge
credentials; agents never see them.

**Where this fits:** Quorum's CLI-first substrate (`quorum <subcommand>`) provides
atomic claims, messaging, and a task queue for agents. The daemon adds *orchestration*:
it drives agents through the task→PR→review→merge lifecycle, enforces cost ceilings,
and handles crash recovery. The CLI is the IPC layer — agents communicate with the
daemon via `quorum done`, `quorum task-update`, etc., which write to a shared SQLite
mailbox table that the daemon polls.

## Architecture

### Process model

```
quorum serve --cap N --repo-dir <path> --worktree-base <path> --names-file <path>
  │
  ├── tick loop (500ms)
  │     ├── Phase 1: poll mailbox (unconsumed rows)
  │     ├── Phase 2: process Done signals (verdicts, merge, rework)
  │     ├── Phase 3: drain reviewer events (stream-json stdout)
  │     ├── Phase 4: drain worker events (stream-json stdout)
  │     ├── Phase 5: spawn reviewer (if worker has PR, no reviewer yet)
  │     └── Phase 6: spawn worker (if slot empty, ready tasks exist)
  │
  ├── worker slot:   AgentProc (claude child, stream-json stdin/stdout)
  │     └── git worktree: {worktree-base}/{Name}-t{task_id}
  │
  └── reviewer slot: AgentProc (ephemeral, spawned per PR)
        └── git worktree: {worktree-base}/pr-{pr}-{Name}
```

The daemon runs a **single-threaded tokio runtime**. All SQLite access is offloaded
via `spawn_blocking` with short-lived connections (no WAL pinning). The tick loop
itself is never blocked on I/O.

### Stream-JSON process model

Each agent (worker or reviewer) is a `claude` child process launched in
`--output-format stream-json --input-format stream-json` mode. The daemon
communicates with it via:

- **stdin:** JSON turn objects (task prompt, rework feedback, review instructions).
  `AgentProc::feed_turn()` writes a JSON object + newline and flushes.
- **stdout:** A stream of newline-delimited JSON events, parsed by
  `stream::parse_line()` into typed `Event` variants:
  - `Assistant { message }` — Claude's text output (logged for observability)
  - `ToolUse { name, input }` — Claude invoking a tool
  - `Result { result, usage }` — Turn complete; carries `input_tokens`/`output_tokens`
  - `Other` — Catch-all for unrecognized event types

`drain_events()` reads events with a 5-second timeout per event. On `Event::Result`,
it records token usage to the journal and transitions the lifecycle phase.

### Agent process isolation

Each spawned agent:

- Runs in its own **process group** (`setpgid(0, 0)` in `pre_exec`). Teardown calls
  `killpg(SIGKILL)` to kill the agent and all its grandchildren.
- Gets its own **git worktree** — full filesystem isolation, no shared checkout.
- Runs with `--permission-mode dontAsk` — no human prompts.
- **Never receives merge credentials.** The daemon holds the GitHub token and executes
  merges itself via a `MergeExecutor` trait.

### SQLite as IPC bus

Agents communicate with the daemon exclusively through the SQLite database:

- **Mailbox table** (`mailbox`): Agents write rows via CLI commands
  (`quorum done --agent <name> --pr <N> --verdict approved`). The daemon polls
  `WHERE consumed_at IS NULL` every tick, processes each row, and marks it consumed.
  A partial index on `consumed_at IS NULL` keeps polling cheap as the table grows.

- **Journal table** (`journal`): The daemon upserts one row per in-flight agent on
  every lifecycle transition. Fields include `session_id` (for `--resume` on crash
  recovery), `worktree`, `branch`, `phase`, and `cost_tokens`. Entries are deleted
  on terminal transitions (teardown).

No sockets, no pipes beyond the stream-json stdin/stdout of each agent process.

## Lifecycle

### Worker lifecycle

```
[ready task in DB]
  → claim (atomic, open → claimed)
  → provision worktree (serialized via mutex)
  → journal: phase="working"
  → spawn AgentProc, feed task prompt as turn 1
  → drain events until Result
  → journal: phase="awaiting-review", record cost
  → agent signals `quorum done --pr <N>`
  → daemon sets pr=Some(N), keeps worker alive
  → [reviewer spawned — see below]
  → verdict: approved → merge → teardown, task="done"
  → verdict: changes → kill reviewer, feed rework turn to warm worker
       → journal: phase="working", rework_count++
       → [cycle repeats from drain]
```

On spawn failure at any step: task released back to "open", name released, worktree
removed. On SIGINT shutdown: reviewer torn down first, then worker with
task status="open" (re-claimable by next daemon run).

### Reviewer lifecycle

```
[worker has PR, no reviewer]
  → acquire reviewer name
  → fetch + provision reviewer worktree (from the PR head branch)
  → journal: phase="reviewing", role="reviewer"
  → spawn reviewer AgentProc, feed review prompt
  → drain events until Result
  → reviewer signals `quorum done --verdict approved|changes`
  → daemon processes verdict (see worker lifecycle)
  → teardown: kill process group, delete journal, remove worktree, release name
```

The reviewer is **ephemeral** — it has no task ownership and is torn down after
signaling a verdict.

### Warm rework

On review rejection (`verdict=changes`), the worker process is **not** killed.
Instead:

1. The reviewer is torn down.
2. `reviewer::build_rework_turn(agent_name, task_id, pr, feedback)` constructs a
   user-turn JSON object containing the review feedback and re-signal instructions
   (`quorum done --agent <name> --pr <N>`).
3. The turn is fed to the warm worker via `proc.feed_turn()`.
4. `rework_count` is incremented; journal resets to `phase="working"`.

This preserves the worker's accumulated context and avoids a cold restart.

### Daemon-owned merge

The daemon is the sole merge authority. On `verdict=approved`:

1. `merge_executor.merge(pr, repo_dir, ctx)` is called with reviewer lineage.
2. Production: `GhMergeExecutor` posts a formal GitHub approval review (carrying
   reviewer name + task id), then runs `gh pr merge <pr> --merge --delete-branch`
   with a `GH_TOKEN` read from a file (supports token rotation without restart).
3. Test: `CommandMergeExecutor` runs an arbitrary shell command with `{pr}`
   placeholder substitution.
4. On merge failure, the error is classified:
   - **Retryable** (merge conflict, branch behind base): reviewer killed, rework
     feedback sent to the warm worker ("Rebase on main, resolve any conflicts").
   - **PolicyBlocked** (base branch policy, auth failure, infra): both agents
     killed, task parked as `cancelled` with a `daemon:parked:merge-blocked` body
     preserving the reviewer verdict. No rework turn, no re-claim loop.

## Mailbox contract

The mailbox is a FIFO queue with exactly-once consumption semantics.

### Schema

| Column        | Type    | Purpose                                |
|---------------|---------|----------------------------------------|
| `id`          | INTEGER | Auto-increment PK                      |
| `agent`       | TEXT    | Who wrote the row                      |
| `kind`        | TEXT    | `done` · `task_update` · `message`     |
| `task_id`     | INTEGER | Relevant task (nullable)               |
| `pr`          | INTEGER | PR number (nullable)                   |
| `verdict`     | TEXT    | `approved` · `changes` (nullable)      |
| `feedback`    | TEXT    | Review feedback text (nullable)        |
| `note`        | TEXT    | Generic note (nullable)                |
| `to_agent`    | TEXT    | Directed message recipient (nullable)  |
| `payload`     | TEXT    | Arbitrary payload (nullable)           |
| `created_at`  | INTEGER | Unix timestamp                         |
| `consumed_at` | INTEGER | NULL until daemon processes the row    |

### Operations

- `append(conn, row)` — Agent writes a row under `BEGIN IMMEDIATE`.
- `poll_unconsumed(conn)` — Daemon reads all rows `WHERE consumed_at IS NULL`
  ordered by `id` (FIFO).
- `mark_consumed(conn, id)` — Daemon marks a row consumed after acting on it.

### Signal types (by `kind`)

| Kind          | Fields used                     | Daemon action                          |
|---------------|---------------------------------|----------------------------------------|
| `done`        | `pr`, `verdict`, `feedback`     | Process completion/verdict (see lifecycle) |
| `task_update` | `task_id`, `note`               | Update task metadata                   |
| `message`     | `to_agent`, `payload`           | Inter-agent messaging (M5)             |

## Journal (crash-recovery state)

One row per in-flight agent, upserted on every lifecycle transition.

| Column           | Type    | Purpose                                  |
|------------------|---------|------------------------------------------|
| `agent`          | TEXT    | PK — one row per agent                   |
| `role`           | TEXT    | `worker` · `reviewer`                    |
| `task_id`        | INTEGER | Task being worked (nullable for reviewer)|
| `session_id`     | TEXT    | Claude session UUID (for `--resume`)     |
| `worktree`       | TEXT    | Filesystem path to worktree              |
| `branch`         | TEXT    | Git branch name                          |
| `phase`          | TEXT    | `working` · `awaiting-review` · `reviewing` |
| `expected_signal`| TEXT    | What mailbox signal to expect next       |
| `cost_tokens`    | INTEGER | Accumulated input+output tokens          |
| `updated_at`     | INTEGER | Unix timestamp of last upsert            |

On crash recovery (M7), the daemon reads `list_in_flight()` and can resume agents
using `claude --resume <session_id>`.

## Concurrency model

- **Single-threaded tick loop.** No concurrent access to slot state. All
  concurrency is in I/O offloading via `spawn_blocking`.
- **SQLite serialization.** WAL mode allows concurrent readers while one writer
  holds the lock. `BEGIN IMMEDIATE` detects contention up-front. `busy_timeout=5000`
  provides a 5-second wait queue.
- **Worktree mutex.** A tokio `Mutex<()>` serializes all `git worktree add/remove`
  operations — git worktree commands are not safe to run concurrently against the
  same repo.
- **Process-group isolation.** Each agent runs in its own process group. Teardown
  kills the entire group, preventing orphaned grandchildren.
- **SIGINT is flag-based, not select-raced.** An `AtomicBool` is set by the SIGINT
  handler and checked between ticks. This avoids cancelling a tick mid-flight at an
  await point, which could leak a claimed task or orphan a process.

## Cost model

Token cost is tracked per-agent via the journal's `cost_tokens` field, accumulating
`input_tokens + output_tokens` from each `Event::Result::usage`. The `result` event
in stream-json also carries `total_cost_usd`, `num_turns`, and `duration_ms`.

The daemon enforces fail-closed per-turn and per-task ceilings via `quorum serve`
flags: `--max-turn-tokens`, `--max-task-tokens`, `--max-turn-cost-usd`,
`--max-task-cost-usd`, `--max-turn-wall-secs`, `--max-task-wall-secs`, and
`--max-rework-rounds`. Note that `total_cost_usd` on stream-json result events is
session-cumulative, so per-turn cost is computed as a delta (high-water mark), not
summed.

## Name pool

Agent names are loaded from a file (one per line). The pool requires `> 2 * cap`
names — at full capacity, each task occupies a worker name + a reviewer name, with
headroom for transitions. Names are acquired on spawn and released on teardown.

## Worktree management

- Workers: branch `daemon/{name}-t{task_id}`, path `{base}/{Name}-t{task_id}`
- Reviewers: branch `review/pr-{pr}-{name}`, path `{base}/pr-{pr}-{Name}`

Worker worktrees are provisioned from `origin/main`. Reviewer worktrees are
provisioned from the PR head branch (the worker's branch) so the reviewer has
the code under review checked out locally. The `WorktreeManager` serializes
these operations.

---

## Milestones

### M0: Foundations (merged)

Schema v12 — `mailbox` and `journal` tables. Tokio-based `quorum serve --cap N`
skeleton with 500ms tick loop and SIGINT shutdown. Tokio confined to the `quorum`
binary crate; `quorum-core` stays sync.

### M1: Single-agent vertical slice (merged)

Full single-worker lifecycle: stream-json parser, `AgentProc` with process-group
isolation, name pool, worktree manager, `quorum done` CLI, and the wired tick loop
(poll mailbox → spawn agent → drain events → teardown). Includes `fake_agent` for
deterministic CI testing.

### M2: Reviewer + verdict loop (merged)

Dual-slot architecture (worker + reviewer). Worker signals PR-ready → daemon spawns
ephemeral reviewer → reviewer issues `approved`/`changes` verdict → daemon merges
(on approve) or feeds rework turn to warm worker (on changes). Daemon-owned merge
via `MergeExecutor` trait; reviewer never touches merge credentials.

### M3: Concurrency + scheduler (merged)

Multi-worker support: run up to `cap` workers concurrently. Priority-based task
queue pull with tier and dependency awareness. Reviewer slots scale with workers
(one reviewer per waiting worker above the worker cap).

### M4: Cost + runaway controls (merged)

Per-turn and per-task token and wall-clock watchdogs. Maximum rework rounds. All
limits fail-closed — exceeding a ceiling kills the agent and releases the task,
never silently continues. Consumes `total_cost_usd`, `num_turns`, `duration_ms`
from the existing stream-json `result` events.

### M5: Messaging + agent state agency (merged)

Message push-as-turn: deliver queued messages to an agent at idle (between turns).
Agent state reactions: `blocked`, `failed`, `needs-info`, `note` — agents can
signal non-terminal states that the daemon tracks and surfaces via `quorum status`.

### M6: Logging + live status (merged)

Hierarchical log structure per agent session: `stream.jsonl` (raw events),
`transcript.md` (human-readable), `meta.json` (summary). Log rotation and GC by
age/size. Live `quorum status` integration showing in-flight agents, phases,
costs, and verdicts.

### M7: Crash recovery (merged)

Journal-driven resurrection on daemon restart: read `list_in_flight()`, reconnect
to agents via `claude --resume <session_id>`. Process-group cleanup with `killpg`.
Resume-turn templates for re-orienting a resumed agent. Name reconciliation
(reclaim names from journal). Worktree GC for orphaned worktrees.

**Schema v15** adds `pid` (process group ID), `pr` (PR number), and
`rework_count` to the journal table for full crash-recovery state.

**Implementation:** `recovery::recover()` is called on daemon startup before
the tick loop. Workers are resumed with `--resume`; reviewers are torn down
(ephemeral — Phase 5 respawns fresh ones). `AgentSpec::resume` controls
`--resume` vs `--session-id` flag selection. `Pool::reclaim()` reclaims
names from journal entries. `WorktreeManager::gc_orphaned()` removes
worktrees not referenced by active journal entries.
