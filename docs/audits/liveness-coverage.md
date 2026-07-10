# Liveness Coverage Audit — `quorum serve` daemon

**Date:** 2026-07-09
**Task:** #57 (orphan-rescue-39)
**Trigger:** 30+ min agent stall; daemon believed agent mid-turn, stall counter showed 0, message delivery gated on idle that never came.

## Method

Enumerate every (task state × expected actor) pair in the lifecycle + daemon runtime.
For each: what unblocks it, what bounds the wait, what happens at the bound.
A cell is **BOUNDED** if the bound fires autonomously; **UNBOUNDED** if forward progress
depends on an external actor that may never act.

---

## Coverage Table

### Task lifecycle states

| # | State | Expected actor | Unblocked by | Bound | At bound | Status |
|---|-------|---------------|--------------|-------|----------|--------|
| L1 | **open** (ready) | daemon Phase 6 | `spawn_worker` claims it | tick interval (500ms) | claimed next tick | BOUNDED |
| L2 | **open** (not ready — deps unmet) | upstream task(s) | all `depends_on` tasks reach `done` | **NONE** (see G1) | — | **UNBOUNDED → G1** |
| L3 | **open** (poisoned) | human/operator | manual intervention or new task | intentional — safety mechanism | n/a (by design) | BOUNDED (by design) |
| L4 | **working** | worker agent | worker signals `done` or dies | (a) `max_turn_wall_secs` kills stall, (b) `max_task_wall_secs` caps total, (c) API 3.3h turn limit, (d) lease TTL → `reap_lapsed_tasks` → open | worker killed or task reclaimed | BOUNDED |
| L5 | **in-review** (reviewer attached) | reviewer agent | reviewer posts verdict | (a) `max_turn_wall_secs` on reviewer, (b) reviewer death → AgentFailed → respawn, (c) rework cap → failed | reviewer killed, new reviewer spawned | BOUNDED |
| L6 | **in-review** (no reviewer, worker alive w/ PR) | daemon Phase 5 | `spawn_reviewer` | provision tracker: 3 failures → parked/cancelled | task cancelled (provision exhausted) | BOUNDED |
| L7 | **in-review** (orphan — no worker, no reviewer) | daemon Phase 5b | orphan reviewer spawn | **provision-exhausted → skip (no cleanup)** (see G2) | — | **UNBOUNDED → G2** |
| L8 | **rework** | worker agent | worker signals `ReworkPushed` | same as L4 (working) — wall-clock + lease TTL | task reclaimed to open | BOUNDED |
| L9 | **merging** (waiting for checks) | CI system | `wait_for_checks` returns | `merge_checks_timeout_secs` | TimedOut → Cancelled | BOUNDED |
| L10 | **merging** (merge call) | GitHub API | `merge()` returns | synchronous blocking call with gh timeout | merge result (success/fail) → next state | BOUNDED |
| L11 | **done/failed/cancelled** | — | terminal | n/a | n/a | TERMINAL |

### Daemon-managed agent states

| # | State | Expected actor | Unblocked by | Bound | At bound | Status |
|---|-------|---------------|--------------|-------|----------|--------|
| A1 | worker **mid-turn** (`draining=true`) | worker process | `Result` event on stdout | `check_wall_clock_limits` every tick | worker killed, AgentFailed → open | BOUNDED |
| A2 | worker **idle** (awaiting review, `draining=false`) | daemon Phase 5 | reviewer spawned → verdict → next state | depends on reviewer lifecycle (L5/L6) | bounded by reviewer bounds | BOUNDED |
| A3 | reviewer **mid-turn** (`draining=true`) | reviewer process | `Result` event on stdout | `check_wall_clock_limits` every tick | reviewer killed, AgentFailed → respawn | BOUNDED |
| A4 | worker **idle** (awaiting message delivery) | daemon Phase 4c | message delivered when `draining=false` | bounded by how long `draining=true` lasts (A1) | message delivered next tick after idle | BOUNDED |

### Cross-cutting concerns

| # | Concern | Bound | At bound | Status |
|---|---------|-------|----------|--------|
| C1 | daemon shutdown mid-verdict | drain timeout → SIGTERM → force kill | agents killed, tasks recover on restart | BOUNDED |
| C2 | daemon crash / restart | recovery Phase 4 resets all non-terminal states | working/rework → open, merging → in-review, in-review → orphan spawn | BOUNDED |
| C3 | name-pool exhaustion | fallback generated names | logged, never blocks | BOUNDED |
| C4 | WAL growth from long reader | `quorum sweep` + `status --watch` uses short-lived connections | WAL truncated | BOUNDED |
| C5 | message expires before delivery | `expires_at` TTL filter | message invisible, sweep reclaims row | BOUNDED (message lost, not stuck) |

---

## Gaps Found

### G1: Cancelled dependency permanently blocks dependents (UNBOUNDED)

**Severity:** Medium — tasks can be orphaned indefinitely.

**Mechanism:** `compute_ready()` requires ALL `depends_on` tasks to have `status='done'`.
When a dependency is cancelled (or fails), the dependent task's `ready` flag never becomes
true. No cascade, no timeout, no reaping — the task sits in `open/not-ready` forever.

**Affected paths:**
- Task A depends on Task B. B is cancelled → A permanently blocked.
- Task A depends on Task B. B fails → A permanently blocked.

**Fix (this task):** In `sweep_on_write`, detect open tasks whose `depends_on` contains
only terminal-but-not-done tasks (failed/cancelled) and fire a `Cancelled` event with
reason `dep-cascade:<failed_dep_id>`. This runs on every mutation, so the cascade is bounded
by the next write to the DB.

**Test:** Unit test that creates tasks A→B, cancels B, runs sweep, asserts A is cancelled.

### G2: Orphan in-review with exhausted provision — no cleanup (UNBOUNDED)

**Severity:** Medium — task stuck in in-review with no escape.

**Mechanism:** Phase 5b (line 2339 in `serve/mod.rs`) checks
`reviewer_provision_tracker.is_exhausted()` for orphan in-review tasks. If exhausted,
it `continue`s — but unlike Phase 5 (line 2257), it does NOT park/cancel the task.
The task remains in `in-review` with no worker and no reviewer, and no mechanism ever
touches it again.

**Fix (this task):** When Phase 5b encounters an orphan with exhausted provision, fire
`Cancelled` with `daemon:parked:provision-exhausted` (same as Phase 5) and set the
task body.

**Test:** Integration test using fake-agent that creates an orphan in-review with
exhausted provision tracker and verifies the task is cancelled.

---

## Summary

| Cells | Bounded | Unbounded | Terminal |
|-------|---------|-----------|----------|
| 15 active | 13 | 2 (G1, G2) | 1 (L11) |

Both gaps have fixes in this task. No follow-up tasks needed.
