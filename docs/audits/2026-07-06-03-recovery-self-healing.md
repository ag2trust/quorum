# Audit 3/6: Recovery & Self-Healing (Crash Matrix)

**Auditor:** Anchor-d17 · **Date:** 2026-07-06 · **Scope:** `quorum/src/serve/recovery.rs`, `quorum-core/src/journal.rs`, `quorum-core/src/daemon_lock.rs`, session-resume paths in serve, schema-too-new force-kill path, supervisor handshake (exit 75).

**Method:** For every task status × every daemon lifecycle moment, trace what recovery does on restart. Each cell resolves to: resume the same agent session, re-arm the wait, or a LOUD terminal.

---

## Crash Matrix

### Axis 1: Task status at crash time (journal `phase` column)

| Phase | Role | Meaning |
|-------|------|---------|
| `working` | worker | Agent actively executing the task |
| `awaiting-review` (no PR) | worker | Worker done draining, no PR yet (edge: crash between drain_events setting `draining=false` and the mailbox `done` row arriving) |
| `awaiting-review` (with PR) | worker | Worker posted `done --pr N`, idle waiting for reviewer |
| `reviewing` | reviewer | Reviewer actively reviewing a PR |

### Axis 2: Daemon lifecycle moment

| Moment | Description |
|--------|-------------|
| **Mid-tick** | Crash during `tick()` execution (any phase 1–6) |
| **Mid-spawn** | Crash after journal upsert but before `AgentProc::spawn` returns / PID written |
| **Mid-merge** | Crash during `merge_executor.merge()` call (between approval and merge completion) |
| **During drain** | Crash after `drain_state.draining = true` but before all agents finish |
| **Force-kill (exit 75)** | `SchemaTooNew` detected — `kill_and_reap` all agents, exit 75 |
| **Signal shutdown** | 1st SIGINT/SIGTERM (drain) or 2nd (force) |

### Matrix Resolution

| Task Phase | Mid-tick | Mid-spawn | Mid-merge | During drain | Force-kill (75) | Signal shutdown |
|---|---|---|---|---|---|---|
| **working** | Resume: `--resume` + feed_turn with resume context | Resume: `--resume` (PID may be None → stale killpg is no-op, safe) | N/A (workers don't merge) | Resume: `--resume` (draining was pre-crash state, resume re-arms) | Journal survives, next daemon resumes | Teardown to `open` (1st sig) or force-kill (2nd sig), journal survives |
| **awaiting-review (with PR)** | PendingReview: no process spawned, Phase 5 provisions reviewer | PendingReview path (no process, reviewer provisioned on next tick) | N/A | PendingReview registered, pending_reviews drain teardown fires AgentFailed→open | Journal survives, next daemon creates PendingReview | Teardown pending_review to `open` |
| **awaiting-review (no PR)** | Resume: `--resume` as worker (phase=awaiting-review but no PR → falls through to spawn) | Same as mid-tick | N/A | Same as working | Journal survives, next daemon resumes | Same as working |
| **reviewing** | Teardown: reviewer journal deleted, Phase 5 respawns fresh | Teardown (reviewer is ephemeral) | Crash between approval-record and merge-execute: #228 approval recovery replays the merge | Teardown reviewer, reviewer is ephemeral | Force-killed, journal deleted on recovery | Teardown reviewer |

---

## Findings (priority order)

### Finding 1 — DUPLICATED-EXECUTION RISK: `release_and_cleanup` on missing worktree sets task to `open` without lifecycle event

**Priority:** 1 (duplicated-execution risk)
**File:** `quorum/src/serve/recovery.rs:143-158`
**Design-of-record deviation:** The task is set to `open` via a raw `tasks::update` (line 409 of `release_and_cleanup`) instead of firing `Event::AgentFailed` through `tasks::apply_event`. This bypasses the lifecycle state machine.

**Failure scenario:** Worker is in phase `working`, task status is `working`. Daemon crashes. On restart, worktree is missing (it was under `/tmp` and got cleaned). Recovery calls `release_and_cleanup` which does `tasks::update(status: "open")`. This is a raw status write, not a lifecycle event. If the task was in a status other than `working` (e.g., a stale journal entry from a prior rework where the task is actually in `in-review` or `rework`), the raw update silently forces it to `open` regardless of the lifecycle state machine's opinion. More critically: no `NotifyOwner` effect is emitted (the lifecycle table says `Working → AgentFailed` produces `ReleaseLease + NotifyOwner`), so the failure is silent.

Additionally, the `release_and_cleanup` function does NOT fire `AgentFailed`, which means if the task had already progressed to `in-review` (e.g., a stale journal entry) and the raw update forces it to `open`, a fresh worker will re-execute the task while the existing PR from the prior execution still exists — potential duplicate work.

**Proposed fix-task (3-6 lines):**
```
Replace `release_and_cleanup`'s raw `tasks::update(status: "open")` with 
`tasks::apply_event(AgentFailed { reason: "worktree missing on recovery" })`.
This routes through the lifecycle state machine, which emits the correct
effects (NotifyOwner, ReleaseLease) and rejects impossible transitions
(e.g., task already done/cancelled). Requires making `release_and_cleanup`
return a Result to surface lifecycle errors to the recovery loop.
```

---

### Finding 2 — SILENT-STALL RISK: PendingReview with a non-existent worktree stalls forever

**Priority:** 2 (silent-stall risk)
**File:** `quorum/src/serve/recovery.rs:208-243`
**Design-of-record deviation:** When an `awaiting-review` worker WITH a PR is recovered, it creates a `PendingReview` (line 221). But this path only runs if the worktree exists (line 143 check). If the worktree is missing AND the worker has a PR, execution falls through to `release_and_cleanup` which sets the task to `open` — discarding the existing PR and losing the work done.

However, there is a subtler stall: if the worktree exists but the PR recorded in the journal has been closed/merged externally (e.g., force-merged by an admin during downtime), the `PendingReview` is created and Phase 5 spawns a reviewer against a closed PR. The reviewer will eventually produce a verdict, but the merge will fail (PR already merged). Depending on the failure kind, this could loop through rework or stall.

This is not currently tested.

**Failure scenario:** Daemon crashes while task is in `awaiting-review` with PR #42. Admin merges PR #42 manually during downtime. Daemon restarts, creates `PendingReview(PR #42)`. Phase 5 spawns reviewer for closed/merged PR #42. Reviewer either fails to check out the code (branch deleted post-merge) or approves → merge fails (already merged) → MergeFailed → rework → worker tries to push to a deleted branch → AgentFailed → task goes to `open` → fresh worker re-executes → duplicate PR.

**Proposed fix-task:**
```
In recovery, before creating a PendingReview, verify the PR is still open
via `gh pr view --json state`. If merged, fire MergeSucceeded and close
the task. If closed (not merged), fire AgentFailed. Only create the
PendingReview if the PR is confirmed open.
```

---

### Finding 3 — DUPLICATED-EXECUTION RISK: Race window between `kill_stale_process_group` and `--resume` spawn

**Priority:** 1 (duplicated-execution risk)
**File:** `quorum/src/serve/recovery.rs:89-94` and `quorum/src/serve/recovery.rs:257`
**Design-of-record deviation:** Recovery sends `SIGKILL` to the old process group (line 27: `libc::killpg`), waits 100ms (line 94), then spawns a `--resume` agent. `SIGKILL` is delivered asynchronously by the kernel; 100ms is not a guaranteed reap time. On a loaded system, the old process group may not have fully exited when the new `--resume` process starts. Both the old (dying) agent and the new (resumed) agent could momentarily be writing to the same worktree and the same DB session, corrupting files or producing conflicting git operations.

**Failure scenario:** System under load. Old worker PID 1234 is in process group 1234. Recovery sends `killpg(1234, SIGKILL)`. 100ms passes. Old process is still in `D` state (uninterruptible I/O — e.g., NFS or slow disk). New `--resume` process PID 5678 starts in the same worktree. Both processes write to the same files for a brief window until the old process finally dies. Git operations conflict; worktree state is corrupted.

**Proposed fix-task:**
```
After killpg, call waitpid (or tokio equivalent) on the stored PID to
confirm the process has actually exited before spawning the resume. Use a
bounded timeout (e.g., 5s) and if the process hasn't died, log a loud
warning and skip resuming this entry (let it fall through to
release_and_cleanup). This eliminates the race window entirely.
```

---

### Finding 4 — CORRECTNESS: `release_and_cleanup` does not handle `task_id: None`

**Priority:** 3 (correctness vs design-of-record)
**File:** `quorum/src/serve/recovery.rs:388-420`
**Design-of-record deviation:** `release_and_cleanup` takes `task_id: Option<i64>`. If `task_id` is `None`, the function deletes the journal entry and cleans up, but never updates any task status. This means a journal entry with `task_id: None` (which `JournalEntry` allows — `task_id: Option<i64>`) would be silently cleaned up with no task state change.

**Failure scenario:** If a journal entry is created with `task_id: None` (possible if spawn occurs between name acquisition and task claim — see `spawn_worker` where journal is upserted at line 3235 with `task_id: Some(task.id)`, but a code bug or future change could introduce a None), recovery would delete the journal entry without releasing any task. The task would remain in its claimed state with no agent working on it — a silent stall until lease expiry.

This is a latent risk rather than an active bug: currently all journal upserts pass `Some(task_id)`. But the type signature permits it, and there is no defensive assertion or log.

**Proposed fix-task:**
```
Add a loud log line in `release_and_cleanup` when `task_id` is None:
  log("WARN: release_and_cleanup called with task_id=None for {agent}");
This makes the condition observable. Alternatively, change `JournalEntry::task_id`
to non-optional (`i64`) since it is always set in practice.
```

---

### Finding 5 — SILENT-STALL RISK: `daemon_lock::refresh` silently no-ops on wrong PID (no detection of lock theft)

**Priority:** 2 (silent-stall risk)
**File:** `quorum-core/src/daemon_lock.rs:69-75`
**Design-of-record deviation:** `refresh` uses `WHERE id = 1 AND pid = ?2`. If a competing daemon stole the lock (the heartbeat row now has a different PID), the `UPDATE` affects 0 rows and returns `Ok(())` silently. The original daemon continues ticking without knowing its lock was stolen. Two daemons now operate on the same DB — the very condition the lock exists to prevent.

**Failure scenario:** Daemon A (PID 100) holds the lock at heartbeat_at=1000. Daemon A's tick takes 35 seconds (heavy merge operation). At t=1030, daemon B starts, sees heartbeat_age=30 ≥ stale_secs=30 AND PID 100 is still alive (EPERM or same user) — wait, the condition is `heartbeat_age < stale_secs`, so age=30 is NOT < 30, meaning the `alive && heartbeat_age < stale_secs` check fails and takeover proceeds. Daemon B acquires the lock. Daemon A's tick finishes, calls `refresh(conn, 100, now)` — 0 rows affected, returns `Ok(())`. Daemon A continues ticking, unaware it lost the lock. Both daemons now process the same DB.

The `heartbeat_age < stale_secs` boundary is strict-less-than, so at exactly `stale_secs` the holder is considered stale. Combined with a heavy tick that takes exactly `stale_secs`, this is a real window.

**Proposed fix-task:**
```
Make `refresh` return the number of rows affected. If 0, the lock was
stolen — the daemon must exit immediately (or re-acquire). In tick_loop,
check the return value of `daemon_lock::refresh` and exit with a loud
error if the lock is no longer ours. Also consider using `>=` instead of
`<` in the stale check (i.e., `heartbeat_age <= stale_secs` for "live")
to close the boundary-equality race.
```

---

### Finding 6 — TEST GAP: No crash-recovery integration tests for `recovery::recover`

**Priority:** 4 (test gap)
**File:** `quorum/src/serve/recovery.rs:422-481`
**Evidence:** The only tests in `recovery.rs` are 3 unit tests for `build_resume_turn` (lines 446-480). There are zero tests exercising the actual `recover()` function, which contains the critical crash matrix logic.

**Untested crash matrix cells (explicit list):**

1. **Worker `working` phase, worktree exists** → resume via `--resume` + feed_turn. No test.
2. **Worker `working` phase, worktree missing** → `release_and_cleanup`. No test.
3. **Worker `awaiting-review` with PR, worktree exists** → PendingReview creation. No test.
4. **Worker `awaiting-review` with PR, worktree missing** → `release_and_cleanup`. No test.
5. **Worker `awaiting-review` without PR** → resume via `--resume`. No test.
6. **Reviewer `reviewing` phase** → teardown + journal delete. No test.
7. **Unknown role** → journal delete + name release. No test.
8. **Multiple entries, mixed roles** → interleaved processing. No test.
9. **`--resume` spawn failure** → `release_and_cleanup` fallback. No test.
10. **`feed_turn` failure** → kill_and_reap + `release_and_cleanup`. No test.
11. **Orphaned worktree GC** → `gc_orphaned` correctness. No test.
12. **Stale mailbox drain (F9)** during recovery. No test.
13. **Rapid double restart** (recovery running on top of partial recovery state). No test.

The `approvals::recover` path has good test coverage (5 tests in `approvals.rs`). The journal module has solid CRUD tests. But the recovery *orchestration* — the function that ties them together and handles the combinatorial crash matrix — has no integration tests at all.

**Proposed fix-task:**
```
Add integration tests for `recovery::recover` using a mock AgentProc
(or extract the DB + journal logic into a testable helper). Minimum coverage:
- worker/working with existing worktree → assert journal updated with new PID
- worker/working with missing worktree → assert task set to open, journal deleted
- worker/awaiting-review with PR → assert PendingReview created, no process spawned
- reviewer → assert journal deleted, name released
- spawn failure → assert release_and_cleanup called
Test the double-restart case by running recover twice on the same journal state.
```

---

### Finding 7 — TEST GAP: No tests for `daemon_lock` takeover race under concurrent acquisition

**Priority:** 4 (test gap)
**File:** `quorum-core/src/daemon_lock.rs:86-203`
**Evidence:** The daemon_lock tests cover single-threaded scenarios (acquire, re-acquire, live holder rejected, stale takeover, dead pid takeover, release, refresh). But there is no multi-process or multi-thread test verifying that two concurrent `try_acquire` calls resolve correctly under `BEGIN IMMEDIATE`.

This is the same class as the N-process claim-race canary for claims — the atomicity guarantee is load-bearing, but only unit-tested with sequential calls on a single connection. The `BEGIN IMMEDIATE` prevents concurrent writers, but the test doesn't prove it.

**Proposed fix-task:**
```
Add a multi-thread daemon_lock contention test (similar to the N-process
claim race canary): spawn N threads each calling try_acquire with different
PIDs. Assert exactly one gets Acquired and the rest get Held. Run in a loop
(for i in 1..12) to stress. This proves BEGIN IMMEDIATE serialization works
for daemon_lock, not just claims.
```

---

### Finding 8 — DEAD CODE: `_reviewers` parameter in `recovery::recover` is unused

**Priority:** 5 (dead code the compiler can't see)
**File:** `quorum/src/serve/recovery.rs:65`
**Evidence:** The `_reviewers: &mut [SlotState]` parameter is declared with a leading underscore. Recovery tears down all recovered reviewers (deletes their journal entry, removes worktree/branch, releases name) — it never pushes to the `reviewers` slice. The parameter exists in the signature but has never been used. The underscore prefix suppresses the compiler warning.

This is not a bug (reviewers ARE ephemeral and correctly torn down during recovery), but the parameter is dead weight in a function that already has 7 parameters marked `#[allow(clippy::too_many_arguments)]`.

**Proposed fix-task:**
```
Remove the `_reviewers` parameter from `recovery::recover` and update
the call site in `tick_loop` (line 670). Reduces the parameter count from
7 to 6.
```

---

### Finding 9 — CORRECTNESS: Force-kill path (exit 75) skips `pending_reviews` cleanup

**Priority:** 3 (correctness)
**File:** `quorum/src/serve/mod.rs:819-841`
**Design-of-record deviation:** The `ExitSelfUpdate` path force-kills workers and reviewers (lines 832-839), then returns `Ok(EXIT_SELF_UPDATE)`. But it does NOT process `pending_reviews`. A `PendingReview` has no live process (by definition — it's a journal-only position with no pid), so there's nothing to kill, but the name_pool release and journal/worktree cleanup are skipped.

On restart, `recovery::recover` will re-read the journal and process these entries — so the journal data is preserved. However, the worktree and branch from the prior PendingReview persist on disk until recovery GC cleans them. This is not a correctness bug per se (recovery handles it), but contrasts with the signal-shutdown path (lines 707-709) which explicitly teardown pending_reviews.

**Failure scenario:** SchemaTooNew detected. 2 workers killed, 1 PendingReview exists for task #5, PR #42. Exit 75 proceeds. Supervisor rebuilds. New daemon starts. `approvals::recover` runs first — if the PR was approved, it merges and cleans the journal. Then `recovery::recover` runs — PendingReview is re-created from journal. No stall, no duplicate work. But: if the rebuild is slow and the task lease expires during the rebuild, the task goes to `open`. When recovery creates the PendingReview, the task is `open` — but PendingReview expects it to be in-review. Phase 5 spawns a reviewer for an `open` task. Concurrent to this, Phase 6 may claim the same task for a fresh worker → duplicate execution.

**Proposed fix-task:**
```
In the ExitSelfUpdate path, add the same pending_reviews teardown as the
signal-shutdown path: iterate pending_reviews, release tasks to "open",
delete journal entries. This makes exit 75 and signal-shutdown symmetric.
Alternatively, verify that recovery handles the lease-expired-during-rebuild
case by checking task status before creating PendingReview.
```

---

## Out-of-scope handoffs

- **Audit 1/6 (atomicity/claims):** `tasks::update` in `release_and_cleanup` is not atomic with `journal::delete` — they run in the same `spawn_blocking` closure but as separate statements, not a single transaction. A crash between them could leave a dangling journal entry for a task that was already set to `open`.
- **Audit 2/6 (lifecycle state machine):** The `Merging` status rejects `AgentFailed` (lifecycle.rs:309) — but the daemon's force-kill path during merge fires `AgentFailed` after killing agents. If a task is in `Merging` status when force-killed, the AgentFailed event will be rejected. The task remains in `Merging` with no agent working on it — a stall until manual intervention.
- **Audit 5/6 (drain/shutdown):** The drain timeout path does NOT check for PendingReview entries that have paired reviewers still draining — it tears down ALL pending_reviews regardless, which may race with the reviewer verdict arriving in the same tick.

---

## Verification

```
$ cargo test
test result: ok. 306 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

No source code was modified.
