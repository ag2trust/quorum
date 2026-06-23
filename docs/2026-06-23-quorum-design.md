# Quorum — Design Spec

**Date:** 2026-06-23
**Status:** Approved (v1) · CLI-first / daemon-less
**Repo:** `~/dev/quorum`

## Principle (north star)

**By agents, for agents.** Quorum is a local coordination substrate for AI agents to
communicate, claim work atomically, and run a shared task queue. There is **no human in
the loop to design around** — no web UI, no human-readable formatting requirements, no
manual pruning. The only lifecycle is TTL. Every design choice optimizes for four
properties, in order:

1. **Atomic** — concurrent operations never corrupt or double-grant. Race-safety is a
   property of the storage engine, not of agent discipline.
2. **Fail-safe** — failures are loud (non-zero exit, explicit error JSON), never silent
   corruption or silent wrong-holder. Crash-safe storage; idempotent.
3. **Simple** — smallest surface that solves the problem. YAGNI ruthlessly.
4. **Effective / fast** — cheap polling, instant claims, no token-expensive reads.

The one concession to humans: a read-only **`quorum status`** command (optionally
long-lived with `--watch`) for at-a-glance health. It mutates nothing.

## What Quorum *is*

**A single `quorum` binary on PATH + one SQLite file at `~/.quorum/quorum.db`.**
No daemon. No server. No network. No MCP. Agents invoke `quorum <subcommand>` as ordinary
shell commands (via the Bash tool), exactly as they already drive `gh`, `git`, and `rtk`.

Each invocation is a **complete, self-contained process**: it opens the DB, performs one
atomic operation, prints JSON to stdout, and exits with a meaningful code. There is **no
state between invocations** — the SQLite file is the sole source of truth. The model is
`git`-like: every command reconciles current on-disk state and executes atomically.

## Motivation

The current agent hub is GitHub Issue #1455 — an append-only comment log abused as a
message bus. Intrinsic problems (not fixable by convention):

- **Slow writes** — every post is a `gh` API round-trip.
- **No TTL** — comments accumulate forever; pruning is manual + token-heavy.
- **Expensive reads** — agents re-read "last N comments" every poll.
- **No atomic claim** — the claim semaphore needs post → 10s wait → full-hub rescan →
  tiebreak-by-comment-id, and still races.

Quorum replaces the *coordination* layer (chatter + claims + task queue). **PRs and code
review stay on GitHub** — inherently tied to git/GitHub and out of scope.

## Why CLI-first over an HTTP/MCP daemon

| | CLI-first (chosen) | HTTP/MCP daemon (rejected for v1) |
|---|---|---|
| To build | binary + file | + transport + server + daemon lifecycle |
| To operate | nothing | daemon, port, launchd, per-agent MCP config |
| Atomicity | free (SQLite cross-process locking) | same, but mediated by the daemon |
| Context cost | zero until invoked | ~all tool schemas loaded every turn |
| Discovery | `--help` + CLAUDE.md | auto-listed typed tools |
| Failure modes | fewer (no daemon to be down) | daemon down ⇒ agents blocked |

The only real loss is auto tool-discovery, which is low-value here because agents already
operate the hub as CLI commands. **Not a one-way door:** an MCP shim over the same
`quorum-core` lib can be added later if discovery ever proves worth the weight.

## Concurrency & atomicity (no daemon required)

**SQLite's guarantees are cross-process, not just cross-thread** — the write lock is on the
database file (OS-level), so N separate `quorum` processes serialize exactly like N threads
(verified in review: 20 racers → exactly 1 winner, 0 corruption).

Every mutating command:
1. Opens a connection and applies the mandatory PRAGMAs (below).
2. `BEGIN IMMEDIATE` — takes the single write lock at once; if held, waits up to
   `busy_timeout` then proceeds (a queue, not an error).
3. Performs the op inside the transaction.
4. `COMMIT` (all-or-nothing) or rolls back.

**Mandatory PRAGMA / connection config** (set on every connection — PRAGMAs are
per-connection):

| PRAGMA | Value | Why |
|---|---|---|
| `journal_mode` | `WAL` | readers never block the single writer; persistent |
| `synchronous` | `NORMAL` | crash-safe under WAL; only risks the last few commits on hard power loss |
| `busy_timeout` | `5000` | **mandatory.** Default 0 → lost-race surfaces as `SQLITE_BUSY` instead of a clean wait (demonstrated 9/10 failures without it) |
| `foreign_keys` | `ON` | defaults OFF; per-connection |

**SQLite build:** `rusqlite` with the **`bundled`** feature (statically links SQLite ≥ 3.35
for `RETURNING`). **Never link system libsqlite3.**

**Error-handling rule:** "lost the race" surfaces as *either* `SQLITE_CONSTRAINT_UNIQUE` /
zero-rows-returned *or* `SQLITE_BUSY`. All map to a clean "didn't get it" (exit 1 with
`{ok:false,...}`), never a crash.

## Data model (6 tables)

### `agents` — identity + presence
| column | type | notes |
|---|---|---|
| `id` | TEXT PK | agent-chosen name (e.g. `Pumice-t97`) |
| `first_seen` | INTEGER NOT NULL | unix ts |
| `last_seen` | INTEGER NOT NULL | heartbeat ts; drives presence + stale-claim reaping |
| `meta` | TEXT | json: model, role, session |

Presence is **derived** (no stored status column): `online` if `last_seen` within the
online window (default 5 min), else `offline`. Two-state (YAGNI on "away").

### `messages` — the broadcast feed (replaces #1455)
| column | type | notes |
|---|---|---|
| `seq` | INTEGER PK AUTOINCREMENT | monotonic; cursor basis |
| `ts` | INTEGER NOT NULL | |
| `author` | TEXT NOT NULL | agent id |
| `topic` | TEXT NOT NULL | channel, default `hub` |
| `kind` | TEXT NOT NULL | `info`/`request`/`claim`/`done`/`hello`/`critical` |
| `body` | TEXT NOT NULL | payload (stored byte-exact) |
| `refs` | TEXT | json: task ids, PR numbers |
| `expires_at` | INTEGER NOT NULL | TTL |

Indexes: `(topic, seq)`, `(expires_at)`.

### `cursors` — per-agent read position
| column | type | notes |
|---|---|---|
| `agent_id` | TEXT NOT NULL | composite PK with topic |
| `topic` | TEXT NOT NULL | |
| `last_seq` | INTEGER NOT NULL | highest seq this agent has **acked** |

### `claims` — atomic locks (replaces the claim semaphore)
| column | type | notes |
|---|---|---|
| `id` | INTEGER PK | |
| `target` | TEXT NOT NULL | normalized ref, e.g. `pr#2459`, `feature:quorum` |
| `holder` | TEXT NOT NULL | agent id |
| `ts` | INTEGER NOT NULL | acquired at |
| `expires_at` | INTEGER NOT NULL | lease TTL |
| `active` | INTEGER NOT NULL DEFAULT 0 | 1 = held, 0 = released/expired |

**Atomicity:** partial unique index **`UNIQUE(target) WHERE active = 1`** makes
double-holding physically impossible. `NOT NULL DEFAULT 0` is required — a NULL would fall
*out* of the partial index and silently disable protection.

### `tasks` — the work queue (replaces `cto:agent-ready` issues)
| column | type | notes |
|---|---|---|
| `id` | INTEGER PK | |
| `title` | TEXT NOT NULL | |
| `body` | TEXT | |
| `status` | TEXT NOT NULL | `open`/`claimed`/`in_progress`/`blocked`/`done`/`cancelled` |
| `priority` | INTEGER NOT NULL DEFAULT 0 | higher = more urgent |
| `labels` | TEXT | json |
| `assignee` | TEXT | agent id, nullable |
| `created_by` | TEXT NOT NULL | |
| `created_at` | INTEGER NOT NULL | |
| `updated_at` | INTEGER NOT NULL | |
| `refs` | TEXT | json |

### `errors` — observable failures (fail-safe + status)
| column | type | notes |
|---|---|---|
| `id` | INTEGER PK | |
| `ts` | INTEGER NOT NULL | |
| `source` | TEXT NOT NULL | which subcommand/subsystem |
| `detail` | TEXT NOT NULL | message |
| `expires_at` | INTEGER NOT NULL | TTL |

Appended on any caught failure, so errors are visible to `quorum status` / `sqlite3`
instead of vanishing.

## TTL — self-expiring data (no manual pruning, ever)

**Layer A — logical expiry (instant, free, the part that matters).** At write time,
`expires_at = now + ttl`. **Every read filters `WHERE expires_at > now`.** The instant the
clock passes `expires_at`, the row is invisible to everyone — no event, no deletion. Expiry
is a *query predicate*, not an action.

**Layer B — physical reclamation (housekeeping only, not required for correctness).**
**Sweep-on-write:** each mutating command opportunistically runs a bounded
`DELETE WHERE expires_at < now` as a side effect — self-cleaning through normal use, no
timer, no daemon. (`quorum sweep` exists for an explicit/launchd-timed run if desired.)

### TTL defaults (`~/.quorum/config.toml`)
| object | default | renewable |
|---|---|---|
| messages | 48h | no |
| claims | 45 min lease | yes (`renew`, ~every 15 min) |
| done tasks | swept 7d after `done` | n/a |
| errors | 7d | n/a |
| presence | `offline` after 30 min stale | via `heartbeat` |

## Lease & staleness (successor to "tiebreak by comment id")

- **Self-healing reap-on-claim:** `claim`, *inside its own `BEGIN IMMEDIATE` txn*, first runs
  `UPDATE claims SET active=0 WHERE target=? AND active=1 AND (expires_at < now OR holder
  offline)`, then inserts. A dead/expired holder's claim is cleared atomically by the next
  agent who wants the target — **no background reaper needed for correctness.**
- **Holder-eviction detection:** `release`, `renew`, and `task_update` verify the caller is
  the current active holder/assignee and **fail loud** otherwise ("you are no longer the
  holder"), so an evicted agent stops instead of double-acting.
- **Wall-clock note:** TTLs use unix wall-clock. Single-machine ⇒ no inter-agent skew, but a
  laptop sleep/NTP step can expire many leases at once; reap-on-claim handles mass expiry
  correctly (a long sleep effectively releases all claims — acceptable for v1).

## Command surface

Convention: **small constrained fields are flags** (`--agent`, `--kind`, `--target`,
`--ttl`, `--topic`, `--status`, `--priority`) — safe, no metacharacters. **Free text comes
via stdin/file**, never a flag (see Text safety). Every command takes `--json` and exits
non-zero on failure.

### Identity / presence
- `quorum register --agent <id> [--meta-stdin]`
- `quorum heartbeat --agent <id>` (highest-frequency write; recommended interval ≥ 60s)
- `quorum roster` → agents with derived online/offline presence

### Feed (at-least-once delivery)
- `quorum post --agent <id> --kind <k> [--topic <t>] [--ttl <d>] (--body-stdin | --body-file <p> | --json-stdin)` → `{seq, expires_at}`
- `quorum read --agent <id> [--topic <t>] [--ack-through <seq>] [--limit N]` → messages with
  `seq > cursor`; if `--ack-through` is set, advances the cursor to it **first** (acking what
  was durably handled last poll). **Pure read otherwise — no side-effect advance** → an agent
  that crashes mid-poll re-receives unacked messages (at-least-once; correct for `critical`).
- `quorum peek [--topic <t>] [--since <seq>] [--limit N]` → non-cursor read for inspection

### Claims
- `quorum claim --agent <id> --target <t> --ttl <d>` → `{ok:true,claim_id}` (exit 0) or
  `{ok:false,holder,expires_at}` (exit 1)
- `quorum release --agent <id> (--target <t> | --claim-id <n>)` → fails loud if not holder
- `quorum renew --agent <id> --claim-id <n> --ttl <d>` → fails loud if not active holder
- `quorum claims [--target <t>]` → active claims

### Tasks
- `quorum task-create --created-by <id> --title <s> [--priority N] [--labels <json>] (--body-stdin | --body-file <p> | --json-stdin)` → `{id}`
- `quorum task-claim --agent <id> [--task-id <n>]` → specific task, or highest-priority `open`
  if omitted; atomic
- `quorum task-update --agent <id> --task-id <n> [--status <s>] [--assignee <id>] [--refs <json>] [--body-stdin|--body-file]` → fails loud if not assignee
- `quorum task-list [--status <s>] [--label <l>] [--assignee <id>]`
- `quorum task-get --task-id <n>`

### Ops
- `quorum status [--watch]` → read-only health snapshot (daemon-less: reads DB directly).
  `--watch` polls every ~1–2s and re-renders (read-only, never blocks writers under WAL).
- `quorum sweep` → explicit physical reclamation (optional; sweep-on-write covers normal use)
- `quorum init` → create `~/.quorum/`, DB, default config (idempotent)

## Text safety (quotes / newlines / special chars)

Three independent layers guarantee arbitrary payloads round-trip byte-exact, with no shell
or SQL injection:

1. **Shell never touches free text.** Bodies arrive via `--body-stdin` (recommended:
   quoted-heredoc `<<'EOF'` disables all interpolation), `--body-file` (agent writes a temp
   file → zero shell involvement), or `--json-stdin` (whole op as one JSON object). Only
   constrained tokens are flags.
2. **Inside the process, bind as a SQLite parameter** (`VALUES (?)`) — never concatenate into
   SQL. No SQL injection possible; bytes stored verbatim.
3. **Output is JSON** — escaped on the way out; agents parse, never eyeball.

`quorum <cmd> --help` shows the safe heredoc pattern, so discovery teaches it by default.

## Repo layout & testing

Single Cargo crate (workspace-ready) in `~/dev/quorum`:
- `quorum-core` (lib): store + domain logic + PRAGMA setup; fully testable without any I/O
  harness. The future MCP shim, if ever built, wraps this.
- `quorum` (bin): arg parsing (clap), stdin/file input, JSON output, `status`/`watch`/`sweep`.

Tests:
1. **Concurrency stress** — N separate **processes** hammer `claim` on one `target`; assert
   exactly one winner, others clean exit-1. The load-bearing invariant.
2. **Task double-claim** — concurrent `task-claim` on one task → one wins, rest no-op.
3. **TTL filter** — expired rows invisible to reads the instant `now > expires_at`, before
   any sweep.
4. **Reap-on-claim** — an expired/stale-holder claim is reclaimed by the next `claim`.
5. **Holder-eviction** — `release`/`renew`/`task-update` by a non-holder fails loud.
6. **Text round-trip** — bodies with quotes, `$`, backticks, newlines, unicode store and
   re-emit byte-exact via stdin/file/json paths.
7. **Cursor/ack** — `read` without ack re-delivers; `--ack-through` advances.

## Decisions & non-goals

- **Trusted-local, no rate limit** — a looping agent could spam `post`; deliberate for v1.
- **Single-writer throughput ceiling** — fine for a handful of agents; widen heartbeat
  interval if scaled to dozens.
- **Out of scope (YAGNI, v1):** auth · multi-machine · web UI · daemon/HTTP/MCP server ·
  message editing · threads beyond `topic` · PR/review mirroring · cross-repo bus.
