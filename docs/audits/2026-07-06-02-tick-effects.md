# Post-refactor audit 2/6: daemon tick & effect execution

**Date:** 2026-07-06
**Commit:** `4d23f320` (HEAD of `main`)
**Scope:** `quorum/src/serve/mod.rs` (tick loop, spawn paths, mailbox consumption,
merge path, drain/self-update), `quorum/src/serve/reviewer.rs`, merge executor
plumbing.
**Baseline:** `docs/audits/2026-07-06-00-baseline.md`

## Findings

### Finding 1 — `wait_for_checks` blocks tick loop, staling daemon lock heartbeat

**Priority:** 1 (duplicated-execution risk)

**File:** `quorum/src/serve/mod.rs:1172–1182`

**Evidence:** The approved-verdict merge path calls `wait_for_checks` via
`spawn_blocking` + `.await` (line 1177). This can block the tick function for up to
`merge_checks_timeout_secs` (default 900s). The daemon lock heartbeat refresh runs
at the top of the tick *loop* (line 847–856), not inside `tick()`, so it cannot fire
until `tick()` returns.

**Failure scenario:** Reviewer approves PR → daemon enters merge path →
`wait_for_checks` polls for 900s → heartbeat goes stale after
`DAEMON_LOCK_STALE_SECS` (30s, line 308) → a second daemon starting during this
window sees a stale heartbeat + dead pid (or EPERM) and takes over the lock →
**two daemons run concurrently**, violating load-bearing invariant #11
(single-daemon-per-DB). Both daemons claim tasks and spawn workers, causing
duplicated execution. Additionally, the drain timeout (line 779) cannot fire during
the blocked tick, so drain overruns by up to 900s.

**Proposed fix-task:**
```
feat(serve): heartbeat refresh as independent tokio task

Move the daemon lock heartbeat refresh (mod.rs:847-856) into a standalone
tokio::spawn interval task that ticks every 10s, independent of the main
tick loop. This prevents any long-running spawn_blocking inside tick()
from staling the heartbeat. The heartbeat task must hold a clone of
db_path and daemon_pid and run until shutdown.
```

---

### Finding 2 — `fire_event(SignaledDone)` return value discarded → reviewer spawn loop on cancelled task

**Priority:** 1 (duplicated-execution risk) / 2 (silent stall)

**File:** `quorum/src/serve/mod.rs:1886–1892`

**Evidence:** When a worker signals done with a PR (line 1876–1925), the daemon fires
`SignaledDone` or `ReworkPushed` via `fire_event` but discards the return value
(line 1886–1892, `.await` with no binding). If the lifecycle transition fails (e.g.,
the task was externally cancelled via `quorum task-update --status cancelled`), the
daemon still sets `workers[wi].pr = Some(pr)` (line 1877), updates the journal to
"awaiting-review" (line 1900), and leaves the worker idle.

**Failure scenario:** External CLI cancels a task → worker signals done →
`fire_event(SignaledDone)` returns `None` (rejected: "task is cancelled") → daemon
ignores failure, sets worker PR → Phase 5 spawns reviewer → reviewer approves →
`fire_event(VerdictApprove)` returns `None` (still cancelled) → daemon tears down
reviewer but does NOT clear worker PR (line 997–1005, contrast line 1846 in the
no-verdict case) → Phase 5 spawns another reviewer → **infinite reviewer spawn loop**.
Each cycle burns a full reviewer session until the daemon drains or the worker process
dies.

**Proposed fix-task:**
```
fix(serve): check fire_event result on worker Done

After fire_event for SignaledDone/ReworkPushed (mod.rs:1886), check the
return value. If None, the lifecycle rejected the transition — the task is
in an unexpected state. Clear the worker's PR, fire AgentFailed, and
clean up the slot (same path as the dead-worker handler). Also: in the
VerdictApprove-failure path (line 997-1005), clear the worker's PR
(`workers[wi].pr = None` if the worker still exists) to prevent the
reviewer spawn loop even if SignaledDone somehow succeeded but
VerdictApprove failed.
```

---

### Finding 3 — No PR state check before reviewer spawn; PrFoundMerged/PrFoundClosed events absent

**Priority:** 1 (duplicated-execution risk)

**File:** `quorum/src/serve/mod.rs:2287–2322` (Phase 5), `quorum/src/serve/mod.rs:1876–1892` (worker done handler)

**Evidence:** When a worker signals done with a PR number, the daemon trusts the PR is
open without checking GitHub state. Phase 5 then spawns a reviewer based solely on the
worker having a PR and no paired reviewer (line 2291–2293). Grep for
`PrFoundMerged`/`PrFoundClosed` returns zero matches — these events do not exist in
the `Event` enum (`quorum-core/src/lifecycle.rs:67–80`). The design spec
(`docs/2026-06-23-quorum-design.md:407–461`) does not define them either.

**Failure scenario:** Worker opens PR → PR is merged by another actor (manual merge,
different CI system, admin merge) → worker signals `done --pr N` → daemon fires
`SignaledDone` (succeeds, task → in-review) → Phase 5 spawns reviewer → reviewer
reviews an already-merged PR → reviewer approves → daemon runs merge flow →
`gh pr merge` fails (already merged) → `MergeResult.success = false` → daemon
handles as merge failure → triggers rework cycle against an already-merged PR →
eventual cancellation or rework-cap failure. A full reviewer session is wasted, and
the task ends in `failed` instead of `done`.

**Proposed fix-task:**
```
feat(serve): check PR state before reviewer spawn

In Phase 5, before spawning a reviewer for a worker's PR, call
MergeExecutor::check_mergeability or a new `pr_state` method to verify the
PR is still open. If merged, fire MergeSucceeded directly (the work is
done). If closed (not merged), fire AgentFailed with a descriptive reason.
This prevents wasting a reviewer session on a dead PR. Optionally, add
PrFoundMerged/PrFoundClosed as lifecycle Event variants for clean state
machine integration.
```

---

### Finding 4 — Lifecycle process-side effects (NotifyOwner, PostFindingsNote) silently dropped

**Priority:** 3 (correctness vs design-of-record)

**File:** `quorum/src/serve/mod.rs:3390–3431` (`fire_event` function)

**Evidence:** `fire_event` calls `tasks::apply_event`, which returns a
`TransitionResult` containing an `effects: Vec<Effect>` of process-side effects.
The daemon logs the effect names (line 3411) but never dispatches on the vector. The
daemon hard-codes its behavior based on task status and context. This works for most
effects (SpawnWorker/SpawnReviewer are covered by Phase 5/6; MergePr is hard-coded
in the approved path; ResumeWorker/ResumeReviewer are handled by rework logic). But
two effects are genuinely lost:

**`NotifyOwner`** — returned in 5 transitions:
- `Working + AgentFailed` → Open (`lifecycle.rs:178–186`)
- `InReview + VerdictChanges` (rework cap exceeded) → Failed (`lifecycle.rs:224–233`)
- `InReview + AgentFailed` → InReview (`lifecycle.rs:240–249`)
- `Rework + AgentFailed` → Open (`lifecycle.rs:267–275`)
- `Merging + MergeFailed` → InReview (`lifecycle.rs:290–298`)

**`PostFindingsNote`** — returned when:
- `InReview + VerdictChanges` (review_only=true) → Failed (`lifecycle.rs:218–222`)

The design spec documents these effects (lines 441, 449, 450, 455, 460) as part of
the state machine contract. The daemon silently drops them.

**Failure scenario:** A worker fails 3 rework cycles → lifecycle returns
`NotifyOwner { reason: "rework cap (3) exceeded" }` + `ReleaseLease` → daemon logs
the effect name but takes no notification action → the task's owner/creator has no
signal that their task failed due to rework exhaustion; they discover it only by
polling `quorum status`.

**Proposed fix-task:**
```
feat(serve): dispatch on NotifyOwner and PostFindingsNote effects

After fire_event returns, iterate over TransitionResult.effects and
execute process-side effects:
- NotifyOwner: post a message to the task's creator (via mailbox or a
  new quorum post --channel owner-alerts) with the reason string.
- PostFindingsNote: extract the reviewer's findings from the verdict
  feedback and post them as a note on the task.
As an intermediate step, at minimum log these at WARN level so they are
not silently lost. Update the design spec if NotifyOwner is intentionally
deferred to v2.
```

---

### Finding 5 — Reviewer provision exhaustion bypasses lifecycle state machine

**Priority:** 3 (correctness vs design-of-record)

**File:** `quorum/src/serve/mod.rs:2300–2341` (worker parking), `quorum/src/serve/mod.rs:2355–2393` (pending review parking)

**Evidence:** When reviewer provisioning fails `MAX_REVIEWER_PROVISION_STRIKES` (3)
times, the daemon parks the worker via `teardown_worker_with_body` (line 2328–2341)
with status `"cancelled"` and a `PARKED_BODY_PREFIX` body. This directly calls
`tasks::update` to set the status, bypassing `lifecycle::transition` entirely. The
lifecycle state machine would have produced a different transition with different
effects — the task is in `in_review` state, and Cancelled from in_review returns
`[ReleaseLease]` (lifecycle.rs:254–256). By bypassing the lifecycle, any future
effects added to the Cancelled transition (e.g., NotifyOwner) would be silently
skipped.

**Failure scenario:** No incorrect behavior today (the direct update achieves the same
end state). But if the lifecycle's Cancelled transition gains new effects (e.g.,
NotifyOwner to alert the task creator of parking), the bypass would silently skip
them. The parking path also doesn't emit a lifecycle event row, so the task's event
history shows a gap between "in_review" and "cancelled" with no transition event.

**Proposed fix-task:**
```
refactor(serve): park via lifecycle Cancelled event

Replace the direct teardown_worker_with_body("cancelled", ...) in the
parking path with fire_event(&Event::Cancelled { by: "daemon:
provision-exhausted" }) → let the lifecycle handle the transition and
produce effects, then call cleanup_slot to tear down the process. Set the
parking body via a subsequent tasks::update (body-only, no status change)
after the lifecycle transition.
```

---

### Finding 6 — No test: drain timeout violated by merge-in-progress tick

**Priority:** 4 (test gap)

**File:** `quorum/tests/cli_serve_drain.rs`

**Evidence:** `drain_timeout_force_kills_and_exits_75` (line 448) tests that drain
timeout force-kills agents, but does not cover the case where a merge is in progress
(wait_for_checks blocking) when the timeout fires. The test uses a fast-exiting
fake-agent, so no merge path is exercised during drain. No test verifies that the
effective drain time respects the configured timeout when a checks wait is active.

**Proposed test:**
```
test: drain_timeout_with_merge_in_progress

Set drain_timeout_secs=2, merge_checks_timeout_secs=30, and
merge_checks_poll_secs=5. Seed a task, let the worker complete and signal
done, then trigger drain (sha advance). Configure wait_for_checks to
block for 30s (return TimedOut). Verify the daemon exits within
drain_timeout_secs + one tick (2.5s), NOT after merge_checks_timeout_secs
(30s). This test will FAIL against the current code (expected — documents
the bug from Finding 1).
```

---

### Finding 7 — No test: fire_event failure on worker Done → reviewer spawn loop

**Priority:** 4 (test gap)

**File:** `quorum/tests/`

**Evidence:** No existing test covers the scenario where a task is externally
cancelled while its worker is active, causing `fire_event(SignaledDone)` to fail.
The existing `cli_serve_reviewer.rs` tests all operate on tasks in expected states.
The existing `cli_serve_mailbox.rs` tests cover multi-instance mailbox routing but
not lifecycle transition failures.

**Proposed test:**
```
test: externally_cancelled_task_does_not_spawn_reviewer_loop

Seed a task, let the worker start, then externally cancel the task via
`quorum task-update --status cancelled`. Have the worker signal done with
a PR. Verify: (a) fire_event returns None, (b) no reviewer is spawned,
(c) the worker slot is cleaned up, (d) the task remains cancelled.
```

---

### Finding 8 — No test: SignaledDone on already-merged PR

**Priority:** 4 (test gap)

**File:** `quorum/tests/`

**Evidence:** No test covers the scenario where a worker signals done with a PR
number that has already been merged on GitHub. The `cli_serve_reviewer.rs` tests all
use a mock merge executor that returns success, and none pre-merge the PR before the
worker signals done.

**Proposed test:**
```
test: done_on_already_merged_pr_closes_without_reviewer

Configure CommandMergeExecutor to report the PR as already merged (merge
returns success=false, failure_kind=PolicyBlocked with "already merged"
message). Seed a task, let the worker signal done with a PR. Verify:
either (a) the daemon detects the merged state and fires MergeSucceeded
directly, or (b) documents the current behavior (reviewer spawned, wasted
session, eventual failure).
```

---

### Finding 9 — No test: mailbox consume failure → idempotent replay

**Priority:** 4 (test gap)

**File:** `quorum/tests/`

**Evidence:** The `consume_mailbox_row` function (mod.rs:2429–2452) returns `false`
on failure, causing the tick to `break` and retry next tick. No test verifies that
replaying a mailbox row (re-processing after consume failure) is idempotent — i.e.,
that the second `fire_event` call (now against a task in the post-transition state)
fails harmlessly and doesn't corrupt state.

**Proposed test:**
```
test: mailbox_consume_failure_replay_is_idempotent

Inject a fault into mark_consumed (or use a read-only DB for the consume
step). Let the daemon process a Done mailbox row. Verify: on the next
tick, the row is re-polled, fire_event fails with InvalidTransition (task
already in in-review), and the daemon proceeds without state corruption.
The worker should still have its PR set and Phase 5 should spawn a
reviewer normally.
```

---

## Out-of-scope handoffs

- **Audit 1 (lifecycle):** lifecycle.rs returns `NotifyOwner` on `AgentFailed` from
  `InReview` (line 240–249), but the InReview + AgentFailed transition also returns
  `SpawnReviewer` — verify the reviewer-respawn is tested for the sticky-InReview
  invariant.
- **Audit 3 (recovery):** recovery.rs has 21% coverage. The `spawn_resume_worker_for_pending`
  function (mod.rs:3662–3773) is a recovery-adjacent code path that should be covered
  by recovery audit.
- **Audit 6 (merge/approvals):** `approvals::recover` (approvals.rs:41–122) runs before
  journal recovery and handles durable approval records across restarts — verify the
  ordering guarantee (approved → merged before journal → resumed) is tested.
- **Audit 5 (storage/CLI):** `tasks::close_after_merge` (tasks.rs:687–709) is used by
  approval recovery to finalize merged tasks — verify it handles edge cases (already-done
  task, concurrent closer).

## Verification

```
$ ./preflight.sh
=== preflight 1/4: branch base ===
branch base OK (1 session(s) ahead of origin/main)
=== preflight 2/4: cargo fmt --all -- --check ===
fmt OK
=== preflight 3/4: cargo clippy --all-targets -- -D warnings ===
clippy OK
=== preflight 4/4: cargo test --workspace ===
test result: ok. 306 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
PREFLIGHT: PASS (all 4 gates green)
```

No source code was modified.
