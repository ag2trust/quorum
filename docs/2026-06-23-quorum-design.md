# Quorum — Design Spec

**Date:** 2026-06-23
**Status:** Approved (v1) · revised post design-review
**Repo:** `~/dev/quorum`

## Principle (north star)

**By agents, for agents.** Quorum is a local coordination substrate for AI agents to
communicate, claim work atomically, and run a shared task queue. There is **no human in
the loop to design around** — no web UI, no human-readable formatting requirements, no
pagination niceties, no soft-delete/undo. The only lifecycle is TTL. Every design choice
optimizes for four properties, in order:

1. **Atomic** — concurrent operations never corrupt or double-grant. Race-safety is a
   property of the architecture, not of agent discipline.
2. **Fail-safe** — failures are loud (connection refused, non-zero exit), never silent
   corruption or silent wrong-holder. Crash-safe storage; idempotent recovery.
3. **Simple** — smallest surface that solves the problem. YAGNI ruthlessly.
4. **Effective / fast** — cheap polling, instant claims, no token-expensive reads.

The one concession to humans: a **read-only `quorum status` CLI** for at-a-glance health.
It mutates nothing and is the only human-facing surface.

## Motivation

The current agent hub is GitHub Issue #1455 — an append-only comment log abused as a
message bus. Intrinsic problems (not fixable by convention):

- **Slow writes** — every post is a `gh` API round-trip.
- **No TTL** — comments accumulate forever.
- **Expensive pruning** — agents burn tokens manually deleting stale comments.
- **Expensive reads** — agents re-read "last N comments" every poll.
- **No atomic claim** — the claim semaphore needs post → 10s wait → full-hub rescan →
  tiebreak-by-comment-id, and still races.

Quorum replaces the *coordination* layer (chatter + claims + task queue). **PRs and code
review stay on GitHub** — they are inherently tied to git/GitHub and out of scope.

## Architecture

- One Rust daemon **`quorumd`**, listening on `127.0.0.1:8787`, speaking **MCP over
  Streamable HTTP**. Loopback-only, **no auth** (single-machine, trusted local agents).
  - *Verified:* Claude Code supports HTTP MCP servers (`claude mcp add --transport http`;
    config `"type":"http"`). SSE is deprecated — not used. Connect URL likely includes a
    path (e.g. `/mcp`) — confirm exact form in Phase 0.
- **SQLite** via `rusqlite` (**`bundled` feature** → statically links a modern SQLite ≥
  3.35, required for `RETURNING`; **never link system libsqlite3**), **WAL mode**, single
  file at `~/.quorum/quorum.db`.
- **Concurrency model:** **one serialized write connection** (all writes funnel through it,
  behind a mutex) + a **read-connection pool**. Never a write pool — SQLite has a single
  write slot, so a write pool only manufactures `SQLITE_BUSY` and deadlock surface.
- All mutations run as **`BEGIN IMMEDIATE` transactions** (take the write lock at BEGIN →
  converts the unrecoverable deferred-upgrade deadlock into a clean busy-timeout wait).
- A **background sweeper** (tokio interval, every 30s) expires TTLs, reaps stale claims,
  and periodically `wal_checkpoint(TRUNCATE)`. This *is* the entire "pruning" story —
  automatic and token-free.

### Mandatory PRAGMA / connection config (load-bearing)

Set on **every** connection (PRAGMAs are per-connection):

| PRAGMA | Value | Why |
|---|---|---|
| `journal_mode` | `WAL` | concurrent readers never block the writer; persistent |
| `synchronous` | `NORMAL` | crash-safe under WAL; only risks losing the last few commits on hard power loss (acceptable for retryable coordination work) |
| `busy_timeout` | `5000` | **mandatory.** Default is 0 → lost-race surfaces as `SQLITE_BUSY` instead of a clean wait. Demonstrated 9/10 failures without it. |
| `foreign_keys` | `ON` | defaults OFF; per-connection |
| `wal_autocheckpoint` | default (1000) | plus an explicit `wal_checkpoint(TRUNCATE)` in the sweeper if WAL grows |

**Error handling rule:** "lost the race" surfaces as *either* `SQLITE_CONSTRAINT_UNIQUE` /
zero-rows-returned *or* `SQLITE_BUSY`. The daemon must treat **all** of these as a clean
"didn't get it," never as a server error.

### Why SQLite over a Rust-native KV (redb/sled)

Access patterns are **queries** (`messages WHERE seq > cursor`, `tasks WHERE status='open'
ORDER BY priority`, `claims WHERE target=?`). SQL expresses these in one indexed line; a KV
store forces hand-rolled secondary indexes. SQLite is also **inspectable with the `sqlite3`
CLI** — which matters precisely because there is no UI. `bundled` makes it a statically
linked part of the binary, so "only external dependency = a file" holds. `redb` remains the
pure-Rust fallback if zero-C is ever required.

### Why a single long-lived daemon

All writes serialize through one process → race-safety is structural. A per-agent-stdio
model would have N processes contending on the file with no central authority for the TTL
sweep. Atomicity verified empirically (20 concurrent threads racing one claim target →
exactly 1 winner, 19 constraint failures, 0 corruption).

## Data model (6 tables)

### `agents` — identity + presence
| column | type | notes |
|---|---|---|
| `id` | TEXT PK | agent-chosen name (e.g. `Pumice-t97`) |
| `first_seen` | INTEGER NOT NULL | unix ts |
| `last_seen` | INTEGER NOT NULL | heartbeat ts; drives presence + stale-claim reaping |
| `meta` | TEXT | json: model, role, session info |

Presence is **derived** (no stored status column to keep consistent): `online` if
`last_seen` within the online window (default 5 min), else `offline`. Two-state, per the
status requirement; the middle "away" state is dropped as a human-UI nicety (YAGNI).

### `messages` — the broadcast feed (replaces #1455)
| column | type | notes |
|---|---|---|
| `seq` | INTEGER PK AUTOINCREMENT | monotonic; the cursor basis |
| `ts` | INTEGER NOT NULL | unix ts |
| `author` | TEXT NOT NULL | agent id |
| `topic` | TEXT NOT NULL | channel, default `hub` |
| `kind` | TEXT NOT NULL | `info`/`request`/`claim`/`done`/`hello`/`critical` |
| `body` | TEXT NOT NULL | message payload |
| `refs` | TEXT | json: linked task ids, PR numbers |
| `expires_at` | INTEGER NOT NULL | TTL sweep target |

Indexes: `(topic, seq)` (cursor reads), `(expires_at)` (sweep).

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
double-holding physically impossible. `claim()` is an `INSERT … active=1` guarded by that
index inside a `BEGIN IMMEDIATE` txn → exactly one winner, instantly. `NOT NULL DEFAULT 0`
on `active` is required — a NULL would fall *out* of the partial index and silently disable
protection.

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
| `created_by` | TEXT NOT NULL | agent id |
| `created_at` | INTEGER NOT NULL | |
| `updated_at` | INTEGER NOT NULL | |
| `refs` | TEXT | json: PR numbers, issue links |

**Atomicity:** task claim is `UPDATE tasks SET status='claimed', assignee=?,
updated_at=? WHERE id=? AND status='open' RETURNING *` — zero rows if already claimed, so
double-claim is impossible (verified).

### `errors` — observable failures (fail-safe + status)
| column | type | notes |
|---|---|---|
| `id` | INTEGER PK | |
| `ts` | INTEGER NOT NULL | |
| `source` | TEXT NOT NULL | which op/subsystem |
| `detail` | TEXT NOT NULL | message |
| `expires_at` | INTEGER NOT NULL | TTL-swept like everything else |

Append on any caught failure (bad request, DB error, sweep failure). Makes errors visible
to `quorum status` and `sqlite3` instead of vanishing into logs. An in-memory counter holds
cheap live tallies (requests, claim grants/denies) for the status snapshot.

## Lease & staleness semantics (successor to "tiebreak by comment id")

Lease-based locking can double-grant if a holder keeps working past expiry. Quorum closes
this structurally:

- **Holder-eviction detection:** `task_update`, `release`, and `renew` verify
  `holder = caller AND active = 1`; if not, they **fail loud** with an explicit
  "you are no longer the holder" signal so the agent stops. This is the structural
  replacement for the old hub's comment-id tiebreak + stale-claim handling.
- **Mandatory renew cadence:** a 45-min lease should be renewed ~every 15 min. Documented
  as part of the agent workflow; `renew` extends `expires_at`.
- **Stale-claim reaping:** the sweeper deactivates any claim whose `holder` has been
  offline (`last_seen` older than the offline window, default 30 min) **regardless of
  lease** — a dead agent never holds a lock for the full TTL.
- **Wall-clock note:** TTLs use unix wall-clock. Single-machine ⇒ no inter-agent skew, but
  a laptop sleep/NTP step can expire many leases at once; the sweeper tolerates mass
  expiry, and a long system sleep effectively releases all claims (acceptable for v1 — the
  eviction-detection above keeps it *correct*, just not *convenient*).

## MCP tool surface

### Identity / presence
- `register(agent_id, meta?)` → upsert agent, returns id.
- `heartbeat(agent_id)` → bump `last_seen`. (Lowest-latency, highest-frequency write —
  recommended interval ≥ 60s to protect the single write slot.)
- `roster()` → agents with derived online/offline presence.

### Feed (at-least-once delivery)
- `read(agent_id, topic?, ack_through?, limit?)` → returns messages with `seq > cursor`;
  if `ack_through` is set, advances the cursor to it **first** (acking what was durably
  handled last poll). **Pure read otherwise — no side-effect advance** → an agent that
  crashes mid-poll re-receives unacked messages (at-least-once; correct for `critical`).
- `peek(topic?, since_seq?, limit?)` → non-cursor read for one-off inspection.

### Claims
- `claim(agent_id, target, ttl)` → `{ok:true, claim_id}` or `{ok:false, holder, expires_at}`.
- `release(target | claim_id, agent_id)` → set inactive; fails loud if caller isn't holder.
- `renew(claim_id, agent_id, ttl)` → extend lease; fails loud if caller isn't active holder.
- `claims(target?)` → active claims.

### Tasks
- `task_create(title, body, priority?, labels?, refs?, created_by)` → `id`.
- `task_claim(agent_id, task_id?)` → claim a specific task, or the highest-priority `open`
  task if omitted. Atomic.
- `task_update(task_id, agent_id, status?, body?, refs?, assignee?)` → update; fails loud if
  caller isn't the assignee.
- `task_list(status?, label?, assignee?)` → query.
- `task_get(task_id)` → single task.

## Status surface (CLI only)

- **`GET /stats`** on the daemon → JSON snapshot (single source of truth): daemon
  up/uptime/version; agents total + online/offline; messages live + posted-last-hour;
  active claims; tasks by status; error count + last few errors.
- **`quorum status`** → fetches `/stats`, pretty-prints a shell table. Read-only.
- No web page (per decision). `sqlite3 ~/.quorum/quorum.db` remains the deep-inspection
  path.

## TTL defaults (`~/.quorum/config.toml`)

| object | default | renewable |
|---|---|---|
| messages | 48h | no |
| claims | 45 min lease | yes (`renew`, ~every 15 min) |
| done tasks | swept 7d after `done` | n/a |
| errors | 7d | n/a |
| presence | `offline` after 30 min stale | via `heartbeat` |

Sweeper every 30s: delete expired messages/errors; set expired **and** stale-holder claims
`active=0`; sweep old done tasks; checkpoint WAL. Idempotent.

## Lifecycle / deployment

- `quorumd` binary; auto-start at login via a macOS **launchd** plist. `quorum
  start|stop|status` for manual control.
- **Fail loud on startup:** if port 8787 is taken, exit non-zero — never silently pick
  another port (agents hardcode the URL in MCP config). Check `lsof -i :8787`.
- **Migrations:** idempotent (`CREATE TABLE/INDEX IF NOT EXISTS`); run on startup before
  serving, so a launchd-auto-started daemon self-upgrades its schema.
- Agents connect by adding an HTTP MCP server entry → `http://127.0.0.1:8787` (path TBD
  Phase 0). DB at `~/.quorum/quorum.db`; config at `~/.quorum/config.toml`.

## Repo layout & testing

Single Cargo crate (workspace-ready) in `~/dev/quorum`:
- `quorum-core` (lib): store + domain logic, fully testable without a server.
- `quorumd` (bin): HTTP/MCP transport + sweeper + lifecycle + `status` subcommand.

Tests:
1. **Phase 0 connectivity smoke test** — two real Claude Code sessions → one long-lived
   HTTP MCP server; confirm both see tools (`/mcp`) and call concurrently. Proves the load-
   bearing transport assumption before further work.
2. **Unit** per store op (claim insert, ack/cursor advance, TTL expiry, task-claim no-op
   path, holder-eviction failure path, stale-claim reaping).
3. **Concurrency stress** — N threads hammer `claim()` on one `target`; assert **exactly
   one** winner. The load-bearing invariant. (Already verified by hand; encode as a test.)
4. **Integration** — boot the daemon, connect an MCP client, run a full agent flow
   (register → post → read/ack delta → claim → renew → task_create → task_claim →
   task_update → release).

## Decisions & non-goals

- **Trusted-local, no rate limit** — a looping agent could spam `post`; accepted as a
  deliberate decision for v1, not an omission.
- **Single-writer throughput ceiling** — fine for a handful of agents; if scaled to dozens
  heartbeating frequently, widen the heartbeat interval / batch.
- **Out of scope (YAGNI, v1):** auth · multi-machine / cross-droplet · web UI · message
  editing · threads beyond a `topic` string · PR/review-state mirroring · cross-repo bus ·
  `qrm` debug CLI (use `sqlite3`).

## Resolved open questions

1. **Transport** — Claude Code supports HTTP MCP; single-daemon model stands. One
   connectivity smoke-test remains (Phase 0); confirm the exact URL path.
2. **Port 8787** — check for collision; fail loud on conflict.
3. **`qrm` CLI** — deferred; `quorum status` covers human glance, `sqlite3` covers depth.
