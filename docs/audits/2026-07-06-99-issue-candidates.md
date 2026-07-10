# Audit findings consolidation — issue candidate list

**Date:** 2026-07-06
**Source audits:** `2026-07-06-0{0..6}-*.md` (baseline + 6 lens audits)
**Consolidator:** Flange-d22

## Summary

| Severity | Count |
|----------|-------|
| Critical | 7 |
| High | 8 |
| Medium | 22 |
| Low | 3 |
| **Total** | **40** |

| Source audit | Findings contributed |
|---|---|
| 00-baseline | 3 (all merged into other candidates) |
| 01-lifecycle-claim-integrity | 6 |
| 02-tick-effects | 9 |
| 03-recovery-self-healing | 9 |
| 04-liveness | 5 (1 deduped into tick-effects) |
| 05-storage-cli | 6 (1 merged into lifecycle) |
| 06-ops-scripts-test-harness | 10 |
| watchdog live incidents (Gantry-m3) | 2 (C6, C7) |
| 03-recovery (promoted out-of-scope handoff) | 1 (M10) |

**Discernment pass (2026-07-06, Gantry-m3 + owner):** 39 of 40 marked `FILE: yes`, M9 marked `FILE: no` (bikeshed-tier), C4 scenario corrected, C6/C7/M10 added. Footer item 5 (sticky_until docs) deferred.

---

## Critical — production incident risk

### C1. Heartbeat blocked by wait_for_checks → dual daemon

`title:` bug: wait_for_checks blocks tick loop, staling daemon heartbeat
`severity:` critical
`labels:` `["kind:bug","severity:critical","audit:tick-effects"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** `wait_for_checks` in the merge path uses `spawn_blocking` + `.await`
(mod.rs:1172–1182), blocking the tick function for up to `merge_checks_timeout_secs`
(default 900s). The daemon lock heartbeat refresh runs at the top of the tick *loop*
(mod.rs:847–856), not inside `tick()`, so it cannot fire until `tick()` returns. After
`DAEMON_LOCK_STALE_SECS` (30s), a second daemon starting sees a stale heartbeat and
takes over the lock — **two daemons run concurrently**, violating load-bearing invariant
#11 (single-daemon-per-DB). Both daemons claim tasks and spawn workers, causing
duplicated execution. Additionally, the drain timeout cannot fire during the blocked tick.

**Evidence:** `quorum/src/serve/mod.rs:1172–1182` (wait_for_checks await),
`quorum/src/serve/mod.rs:847–856` (heartbeat in tick loop, not in tick fn),
`quorum/src/serve/mod.rs:308` (DAEMON_LOCK_STALE_SECS = 30).

**Fix:** Move daemon lock heartbeat refresh into a standalone `tokio::spawn` interval
task (every 10s), independent of the main tick loop.

Sourced from: audits/2026-07-06-02-tick-effects.md — Finding 1

---

### C2. apply_event has no caller authorization → duplicated execution

`title:` bug: apply_event accepts events from any agent, not just assignee
`severity:` critical
`labels:` `["kind:bug","severity:critical","audit:lifecycle"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** The `agent` parameter in `apply_event` (tasks.rs:463–494) is used for
`agents::touch` and event emission, but never compared against the task's `assignee`,
`author`, or `reviewer` before firing the lifecycle transition. A stale worker whose
lease expired can call `task-update --verdict approved` or the daemon can fire
`SignaledDone` from a stale agent, transitioning a task that was re-claimed by another
worker.

**Evidence:** `quorum-core/src/tasks.rs:463–494` (no assignee check),
`quorum-core/src/tasks.rs:631–639` (`update` backward-compat correctly gates on
`AND assignee=?6`, but `apply_event` does not).

**Failure scenario:** Worker A's lease expires → task returns to open → Worker B claims →
Worker A (stale) fires `SignaledDone` with old PR → task transitions to in-review with
A's stale PR, orphaning B's work.

**Fix:** Add caller-identity guard: for agent-initiated events (SignaledDone,
ReworkPushed, VerdictApprove, VerdictChanges) require `agent == task.assignee` or
`task.reviewer`. Daemon events (LeaseExpired, AgentFailed, etc.) accept any caller.
Return `NotHolder` on mismatch (exit 1).

Sourced from: audits/2026-07-06-01-lifecycle-claim-integrity.md — Finding 1

---

### C3. fire_event return value discarded → infinite reviewer spawn loop

`title:` bug: discarded fire_event result causes infinite reviewer spawn
`severity:` critical
`labels:` `["kind:bug","severity:critical","audit:tick-effects"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** When a worker signals done (mod.rs:1876–1925), the daemon fires
`SignaledDone` via `fire_event` but discards the return value. If the lifecycle rejects
the transition (e.g., task was externally cancelled), the daemon still sets
`workers[wi].pr = Some(pr)` and leaves the worker idle. Phase 5 spawns a reviewer →
reviewer approves → `VerdictApprove` rejected (still cancelled) → daemon tears down
reviewer but does NOT clear worker PR → Phase 5 spawns another reviewer → **infinite
loop**, each cycle burning a full reviewer session.

**Evidence:** `quorum/src/serve/mod.rs:1886–1892` (fire_event result discarded),
`quorum/src/serve/mod.rs:997–1005` (VerdictApprove failure path doesn't clear worker PR),
`quorum/src/serve/mod.rs:2287–2322` (Phase 5 spawns reviewer based on worker.pr).

**Fix:** Check `fire_event` return after SignaledDone/ReworkPushed. If None, clear
worker PR, fire AgentFailed, clean up slot. Also: in VerdictApprove-failure path, clear
worker PR to prevent loop even if SignaledDone somehow succeeded.

Sourced from: audits/2026-07-06-02-tick-effects.md — Finding 2

---

### C4. Recovery bypasses lifecycle state machine → duplicated execution

`title:` bug: release_and_cleanup uses raw status update, bypassing lifecycle
`severity:` critical
`labels:` `["kind:bug","severity:critical","audit:recovery"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** `release_and_cleanup` (recovery.rs:143–158) sets task status to `open` via
raw `tasks::update` instead of firing `Event::AgentFailed` through `tasks::apply_event`.
This bypasses the lifecycle state machine. If the task's actual status differs from what
the stale journal entry assumed (e.g., task progressed to `in-review`), the raw update
silently FAILS — `tasks::update`'s open-release matches only `WHERE status='working'`
(tasks.rs:594–601) and returns `NotHolder`; the error is swallowed (`let _ =`,
recovery.rs:409) and `journal::delete` still runs (recovery.rs:411). The task is left
permanently in-review with no journal row, no reviewer, and no lease — an invisible
orphan (live-confirmed on task #18, 2026-07-06; see C7). No `NotifyOwner` effect is
emitted, so the failure is silent.

*(Corrected 2026-07-06 during review of PR #251: the original audit scenario claimed
the task is forced to `open`; that path is guarded — the real outcome is the orphan.
The original duplicate-work scenario is blocked by the `WHERE status='working'` guard
at all current call sites; the related force-kill gap is tracked as M10.)*

**Evidence:** `quorum/src/serve/recovery.rs:143–158` (raw tasks::update),
`quorum/src/serve/recovery.rs:388–420` (release_and_cleanup implementation).

**Fix:** Replace `tasks::update(status: "open")` with
`tasks::apply_event(AgentFailed { reason: "worktree missing on recovery" })`. This
routes through the lifecycle, emits correct effects, and rejects impossible transitions.

Sourced from: audits/2026-07-06-03-recovery-self-healing.md — Finding 1

---

### C5. Race window between kill_stale_process_group and --resume spawn

`title:` bug: 100ms kill-to-resume delay is not a guaranteed reap time
`severity:` critical
`labels:` `["kind:bug","severity:critical","audit:recovery"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** Recovery sends `SIGKILL` to the old process group (recovery.rs:27:
`libc::killpg`), waits 100ms (line 94), then spawns a `--resume` agent. SIGKILL is
delivered asynchronously; 100ms is not a guaranteed reap time. On a loaded system (or
with uninterruptible I/O), the old process may still be alive when the new `--resume`
starts. Both agents write to the same worktree and DB session simultaneously.

**Evidence:** `quorum/src/serve/recovery.rs:89–94` (100ms sleep after killpg),
`quorum/src/serve/recovery.rs:257` (resume spawn).

**Fix:** After `killpg`, call `waitpid` on the stored PID with a bounded timeout (5s).
If the process hasn't died, log a loud warning and skip resuming (fall through to
`release_and_cleanup`).

Sourced from: audits/2026-07-06-03-recovery-self-healing.md — Finding 3

---

### C6. Rework re-signal never resumes reviewer — ResumeReviewer has no executor

`title:` bug: rework re-signal deadlocks — ResumeReviewer effect never executed
`severity:` critical
`labels:` `["kind:bug","severity:critical","audit:tick-effects","source:watchdog"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** After a rework round, the worker re-signals done and the daemon fires
`Event::ReworkPushed` (mod.rs:1881–1892) → the lifecycle returns
`(InReview, [ResumeReviewer])` — but `fire_event`'s return value is discarded at the
call site, and no code anywhere executes `Effect::ResumeReviewer` (repo-wide grep hits
only lifecycle.rs definitions and tasks.rs effect_name). The paired reviewer is never
fed a re-review turn, and because it is still paired, Phase 5's spawn scan
(mod.rs:2287–2299) refuses to provision a replacement. Worker and reviewer both idle
forever — a permanent deadlock with no timeout.

**Evidence:** `quorum/src/serve/mod.rs:1881–1892` (fire_event result discarded),
`quorum-core/src/lifecycle.rs:264–266` ((Rework, ReworkPushed) → ResumeReviewer),
`quorum/src/serve/mod.rs:2287–2299` (Phase 5 pairing guard).

**Live incident:** task #17 / PR #253, 2026-07-06 — deadlocked ~25 min after the rework
push; cleared only by manually killing the idle reviewer (Tiller-d16) so Phase 5 would
respawn (Ember-d21 then approved and the daemon merged).

**Fix:** Execute the effects returned by fire_event at the ReworkPushed site: on
`ResumeReviewer`, feed the paired reviewer a re-review turn (or tear it down and let
Phase 5 respawn). Longer-term: dispatch ALL lifecycle effects returned by fire_event
instead of logging them (same root cause family as C3 and H4).

Sourced from: live watchdog incident 2026-07-06 (Gantry-m3), PR #253 timeline — not
present in any audit report.

---

### C7. Drain/restart orphans in-review tasks — invisible forever, no self-heal

`title:` bug: in-review task orphaned across daemon restart; LeaseExpired never emitted
`severity:` critical
`labels:` `["kind:bug","severity:critical","audit:recovery","source:watchdog"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** Three defects chain to orphan any task that is in-review when the daemon
shuts down uncleanly: (1) shutdown teardown calls `tasks::update(status:"open")`, which
fails on in-review tasks (guarded `WHERE status='working'`); the `?` short-circuit skips
`journal::delete`, but worktree/branch removal proceeds outside the closure
(mod.rs:3542–3560); (2) on restart, the now-missing worktree routes the journal entry to
`release_and_cleanup`, which swallows the same failed update and deletes the journal row
(recovery.rs:406–411); (3) recovery consults only journal rows — never task status — so
an in-review task without a journal row is invisible, and `Event::LeaseExpired` is
emitted nowhere in the codebase, making the designed
`(InReview, LeaseExpired) → SpawnReviewer` self-heal (lifecycle.rs:250–253) unreachable
dead code.

**Evidence:** file:lines above; repo-wide grep confirms no LeaseExpired construction
outside lifecycle.rs and tests.

**Live incident:** task #18 / PR #251, 2026-07-06 — orphaned across the 16:02 relaunch
(in-review, reviewer NULL, claim inactive, no journal row, zero events 40+ min). Needed
a manual review + merge + direct DB reconcile; blocked the entire audit chain at the
#22 dependency gate.

**Fix:** (a) At recovery start, scan `in-review` tasks with no journal row and no live
reviewer → recreate a PendingReview from `refs.pr` (verifying the PR is still open, per
H3). (b) Make failed status writes in teardown/release loud instead of `let _`/`.ok()`.
(c) Either wire a LeaseExpired emitter into the sweep/reaper or remove the dead
transitions so the table stays honest.

Sourced from: live watchdog incident 2026-07-06 (Gantry-m3), PR #251 review record —
extends audit 03 Findings 1/2 (scenarios corrected in C4).

---

## High — correctness bug in main path

### H1. Reviewer not cleared after InReview failure → permanent stall

`title:` bug: expired reviewer blocks new reviewer claim via stale reviewer field
`severity:` high
`labels:` `["kind:bug","severity:high","audit:lifecycle"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** When `InReview + AgentFailed/LeaseExpired` fires, the lifecycle returns
`(InReview, [ReleaseLease, SpawnReviewer])`. The `ReleaseLease` handler in `apply_event`
deactivates the lease but clears `assignee`/`reviewer` only when `new_status.is_terminal()
|| new_status == Status::Open`. Since `new_status` is `InReview` (sticky), neither field
is cleared. The task-claim SQL requires `reviewer IS NULL` to attach a new reviewer.
Result: task is stuck in-review with no active reviewer and no way to attach one.

**Evidence:** `quorum-core/src/tasks.rs:516–523` (ReleaseLease handler),
`quorum-core/src/tasks.rs:384–387` (claim SQL `reviewer IS NULL` guard).

**Fix:** In `apply_event`'s ReleaseLease handler, clear `reviewer` and `assignee` when
`new_status == Status::InReview` (the sticky-review case).

Sourced from: audits/2026-07-06-01-lifecycle-claim-integrity.md — Finding 2

---

### H2. daemon_lock::refresh silently no-ops on lock theft

`title:` bug: daemon_lock refresh returns Ok on stolen lock, enabling dual daemon
`severity:` high
`labels:` `["kind:bug","severity:high","audit:recovery"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** `daemon_lock::refresh` uses `WHERE id = 1 AND pid = ?2`. If a competing
daemon stole the lock (heartbeat row now has a different PID), the UPDATE affects 0 rows
and returns `Ok(())` silently. The original daemon continues ticking without knowing its
lock was stolen. Two daemons now operate on the same DB.

**Evidence:** `quorum-core/src/daemon_lock.rs:69–75` (refresh WHERE pid=?2, no row-count
check). The stale check uses strict-less-than (`heartbeat_age < stale_secs`), so at
exactly `stale_secs` the holder is considered stale — combined with a heavy tick this is
a real window.

**Fix:** Make `refresh` return the row count. If 0, the lock was stolen — exit
immediately. Also consider `heartbeat_age <= stale_secs` for "live" to close the
boundary-equality race.

Sourced from: audits/2026-07-06-03-recovery-self-healing.md — Finding 5

---

### H3. No PR state check before reviewer spawn

`title:` bug: daemon spawns reviewer without verifying PR is still open
`severity:` high
`labels:` `["kind:bug","severity:high","audit:tick-effects","audit:recovery"]`
`needs_owner_call:` yes
`FILE:` yes

`body:`

**Problem.** When a worker signals done with a PR number, the daemon trusts the PR is
open. Phase 5 spawns a reviewer based solely on the worker having a PR and no paired
reviewer. If the PR was merged externally (manual merge, admin), the reviewer reviews an
already-merged PR → approves → merge fails (already merged) → rework cycle against a
merged PR → eventual failure. A full reviewer session is wasted.

The same issue exists in recovery: if a PR was merged during downtime, recovery creates
a `PendingReview` against a closed/merged PR, leading to the same wasted cycle.

**Evidence:** `quorum/src/serve/mod.rs:2287–2322` (Phase 5 — no PR state check),
`quorum/src/serve/mod.rs:1876–1892` (worker done handler trusts PR),
`quorum/src/serve/recovery.rs:208–243` (PendingReview created without PR check).
`PrFoundMerged`/`PrFoundClosed` events do not exist in the Event enum.

**Fix:** In Phase 5, before spawning a reviewer, check PR state via
`MergeExecutor::check_mergeability` or a new `pr_state` method. If merged, fire
`MergeSucceeded` directly. If closed (not merged), fire `AgentFailed`.
Owner call needed: should `PrFoundMerged`/`PrFoundClosed` be added as lifecycle Events?

**Owner decision (2026-07-06 discernment):** file approved. Implementer proposes
in the fix PR whether `PrFoundMerged`/`PrFoundClosed` become lifecycle Events.

Sourced from: audits/2026-07-06-02-tick-effects.md — Finding 3,
audits/2026-07-06-03-recovery-self-healing.md — Finding 2

---

### H4. NotifyOwner and PostFindingsNote effects silently dropped

`title:` bug: lifecycle NotifyOwner effect is declared but never executed
`severity:` high
`labels:` `["kind:bug","severity:high","audit:tick-effects","audit:liveness"]`
`needs_owner_call:` yes
`FILE:` yes

`body:`

**Problem.** The lifecycle emits `Effect::NotifyOwner { reason }` in 5 transitions
(AgentFailed from Working/InReview/Rework, VerdictChanges at rework cap, MergeFailed)
and `PostFindingsNote` when review_only VerdictChanges → Failed. But `apply_event`
(tasks.rs:503–523) has `_ => {}` for unhandled effects, and the daemon's `fire_event`
(mod.rs:3390–3431) logs the effect names but never dispatches them. Both effects are
silently dropped.

The design spec (docs/2026-06-23-quorum-design.md:429) defines `NotifyOwner` as "Alert
the task creator." This is currently a no-op, violating the spec. A task that hits the
rework cap and fails has no notification path — discoverable only by polling.

**Evidence:** `quorum-core/src/lifecycle.rs:96,178–186,224–233,240–249,267–275,290–298`
(NotifyOwner emitted), `quorum-core/src/tasks.rs:522` (`_ => {}`),
`quorum/src/serve/mod.rs:3390–3431` (fire_event logs but doesn't dispatch).

**Fix:** Implement NotifyOwner by posting a `kind:alert` message to the feed. Owner call
needed: is a feed message sufficient, or should this be a mailbox delivery / external
notification?

**Owner decision (2026-07-06 discernment):** file approved. Channel: post a
`kind:alert` message to the feed.

Sourced from: audits/2026-07-06-02-tick-effects.md — Finding 4,
audits/2026-07-06-04-liveness.md — Finding 2

---

### H5. compute_health false stall alarms on awaiting-review workers

`title:` bug: stall detection counts idle awaiting-review workers as stalled
`severity:` high
`labels:` `["kind:bug","severity:high","audit:liveness"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** `compute_health` and `stalled_count` (stats.rs:1039–1061, 346–353) filter
daemon agents by `role == "worker"` and check `last_activity_age_secs > 180`, but do not
check the worker's `phase` field. A worker in `"awaiting-review"` phase (correctly idle
while the reviewer works) triggers a false stall alarm after 180s.

**Evidence:** `quorum-core/src/stats.rs:1039–1046` (no phase check),
`quorum/src/serve/mod.rs:2607` (phase set to awaiting-review).

**Failure scenario:** Worker finishes in 60s, reviewer takes 300s. After 180s, dashboard
shows `HealthVerdict::Stalled`. Operator cancels the "stalled" task, wasting the
reviewer's work.

**Fix:** Filter `compute_health` and `stalled_count` to skip workers whose
`phase == "awaiting-review"`.

Sourced from: audits/2026-07-06-04-liveness.md — Finding 1

---

### H6. resolve_gh_repo blocks indefinitely on network

`title:` bug: resolve_gh_repo has no timeout, blocks serve startup forever
`severity:` high
`labels:` `["kind:bug","severity:high","audit:storage-cli"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** `resolve_gh_repo` (main.rs:168–190) spawns `gh repo view --json
nameWithOwner` as a synchronous subprocess with no timeout. Called during `quorum serve`
startup (main.rs:829, 847). If `gh` hangs (auth prompt, network partition, DNS stall),
the serve process blocks indefinitely before opening the DB — no heartbeat, no journal,
no daemon_lock. A supervisor watching for exit 75 will never fire.

**Evidence:** `quorum/src/main.rs:168–190` (resolve_gh_repo, no timeout).

**Fix:** Wrap in a child.wait_timeout() pattern or watchdog thread that kills the child
after 15 seconds. On timeout, fall back to `None` + warning to stderr.

Sourced from: audits/2026-07-06-05-storage-cli.md — Finding 1

---

### H7. Supervisor never fast-forwards after fetch → broken self-update

`title:` bug: serve-supervisor.sh fetches but never advances working tree
`severity:` high
`labels:` `["kind:bug","severity:high","audit:ops"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** Line 75 of `serve-supervisor.sh` runs `git fetch origin main`, which
updates `origin/main` but does NOT advance the working tree or local branch. Line 77
runs `./dev-install.sh`, which builds from the working tree — still the pre-fetch commit.
The new binary is identical to the old one. The daemon sees the same sha mismatch, drains
again, exits 75 again — the thrash guard stops after 6 wasted cycles. The self-update
mechanism is a complete no-op.

**Evidence:** `scripts/serve-supervisor.sh:75–77` (fetch without merge/checkout).

**Fix:** Add `git -C "$REPO_DIR" merge --ff-only origin/"$BASE_BRANCH"` between fetch
and `dev-install.sh`. If merge fails (dirty tree), alert and relaunch old binary.

Sourced from: audits/2026-07-06-06-ops-scripts-test-harness.md — Finding 1

---

### H8. Supervisor has no signal forwarding; Ctrl-C orphans daemon

`title:` bug: serve-supervisor.sh has no trap; signals orphan the child daemon
`severity:` high
`labels:` `["kind:bug","severity:high","audit:ops"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** `serve-supervisor.sh` has no `trap` for SIGINT/SIGTERM/SIGHUP. When run via
`nohup`, `screen`, or `systemd`, signals sent to the supervisor PID do not reach the
child `quorum serve` process. The daemon runs orphaned with no supervisor — self-update
and thrash guard are gone. The daemon_lock heartbeat stays live, so no other daemon can
start.

**Evidence:** `scripts/serve-supervisor.sh` (no `trap` statement anywhere in file).

**Fix:** Add trap that forwards signals to child PID and waits for exit:
```sh
trap 'kill -TERM "$child" 2>/dev/null; wait "$child"; exit' INT TERM
"$SERVE_BIN" serve "$@" &
child=$!
wait "$child"
```

Sourced from: audits/2026-07-06-06-ops-scripts-test-harness.md — Finding 2

---

## Medium — code hygiene / design decisions

### M1. task-update --status done bypasses lifecycle review

`title:` fix: gate or document task-update --status done bypassing review
`severity:` medium
`labels:` `["kind:bug","severity:medium","audit:lifecycle","audit:storage-cli"]`
`needs_owner_call:` yes
`FILE:` yes

`body:`

**Problem.** The `restricted` status list in `tasks::update` (tasks.rs:581) omits
`"done"`. The generic else-branch (tasks.rs:631–639) allows a worker to set status
directly to `done` from `working`, skipping in-review → merging → done entirely. The
spec says only `open` and `cancelled` are directly settable. The CLI docstring
(cli.rs:81) also misleadingly states that `done` "auto-spawns a review task" — this only
happens through the daemon's mailbox/lifecycle path (`quorum done`), not `task-update`.

**Evidence:** `quorum-core/src/tasks.rs:577–586` (restricted list, no "done"),
`quorum-core/src/tasks.rs:631–639` (else-branch allows done),
`quorum/src/cli.rs:81` (misleading docstring).

**Fix options:** (a) Add `"done"` to restricted list (breaking change if daemon uses this
path), or (b) update CLI docstring to clarify review only happens through `quorum done`.
Owner call: which option?

**Owner decision (2026-07-06 discernment):** file approved. Do option (b) —
docstring fix — now; option (a) restriction only after the daemon's internal
done-path (teardown_worker_with_body → tasks::update status=done) is migrated.

Sourced from: audits/2026-07-06-01-lifecycle-claim-integrity.md — Finding 3,
audits/2026-07-06-05-storage-cli.md — Finding 5

---

### M2. Reviewer provision exhaustion bypasses lifecycle state machine

`title:` fix: reviewer parking should use lifecycle Cancelled event, not raw update
`severity:` medium
`labels:` `["kind:bug","severity:medium","audit:tick-effects"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** When reviewer provisioning fails MAX_REVIEWER_PROVISION_STRIKES (3) times,
the daemon parks the worker via `teardown_worker_with_body` (mod.rs:2328–2341) with
status `"cancelled"`, directly calling `tasks::update`. This bypasses
`lifecycle::transition` entirely — no lifecycle event row is emitted, and any future
effects on the Cancelled transition (e.g., NotifyOwner) would be silently skipped.

**Evidence:** `quorum/src/serve/mod.rs:2300–2341` (worker parking via raw update),
`quorum/src/serve/mod.rs:2355–2393` (pending review parking).

**Fix:** Replace direct `teardown_worker_with_body("cancelled", ...)` with
`fire_event(&Event::Cancelled { by: "daemon:provision-exhausted" })`, then cleanup_slot.

Sourced from: audits/2026-07-06-02-tick-effects.md — Finding 5

---

### M3. Force-kill path (exit 75) skips pending_reviews cleanup

`title:` fix: ExitSelfUpdate path should teardown pending_reviews like signal shutdown
`severity:` medium
`labels:` `["kind:bug","severity:medium","audit:recovery"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** The `ExitSelfUpdate` path (mod.rs:819–841) force-kills workers and
reviewers but does NOT process `pending_reviews`. If the rebuild is slow and a task lease
expires during rebuild, the task goes to `open`. When recovery creates a PendingReview,
the task is `open` — but PendingReview expects `in-review`. Phase 5 spawns a reviewer
for an `open` task. Concurrent to this, Phase 6 may claim the same task → duplicate
execution.

**Evidence:** `quorum/src/serve/mod.rs:819–841` (exit 75 path, no pending_reviews),
`quorum/src/serve/mod.rs:707–709` (signal-shutdown path does teardown pending_reviews).

**Fix:** Add the same pending_reviews teardown as signal-shutdown, or verify recovery
checks task status before creating PendingReview.

Sourced from: audits/2026-07-06-03-recovery-self-healing.md — Finding 9

---

### M4. InReview + AgentFailed spec divergence (code better than spec)

`title:` docs: update spec — InReview+AgentFailed includes NotifyOwner effect
`severity:` medium
`labels:` `["kind:docs","severity:medium","audit:lifecycle"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** The code returns `[ReleaseLease, NotifyOwner{reason}, SpawnReviewer]` for
`(InReview, AgentFailed)`. The spec groups AgentFailed and LeaseExpired together with
effects `[ReleaseLease, SpawnReviewer]` — no NotifyOwner. The code is more informative
(agent crashed → worth alerting), but a developer reading the spec might remove
NotifyOwner in a refactor.

**Evidence:** `quorum-core/src/lifecycle.rs:240–249` (code has NotifyOwner),
`docs/2026-06-23-quorum-design.md` §Transition table (spec omits it).

**Fix:** Update spec: split AgentFailed and LeaseExpired into separate bullets under
"From InReview".

Sourced from: audits/2026-07-06-01-lifecycle-claim-integrity.md — Finding 4

---

### M5. release_and_cleanup doesn't handle task_id: None defensively

`title:` fix: add defensive logging when release_and_cleanup has task_id: None
`severity:` medium
`labels:` `["kind:bug","severity:medium","audit:recovery"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** `release_and_cleanup` takes `task_id: Option<i64>`. If `None`, the function
deletes the journal entry but never updates any task status. Currently all journal
upserts pass `Some(task_id)`, so this is a latent risk, not an active bug. But the type
signature permits it, and there is no defensive assertion or log.

**Evidence:** `quorum/src/serve/recovery.rs:388–420` (task_id: Option<i64>, no None
handling).

**Fix:** Add a WARN log when `task_id` is None, or change `JournalEntry::task_id` to
non-optional `i64`.

Sourced from: audits/2026-07-06-03-recovery-self-healing.md — Finding 4

---

### M6. Stale "closed" status references in code and schema comments

`title:` fix: replace stale "closed" status references with "done"
`severity:` medium
`labels:` `["kind:bug","severity:medium","audit:baseline","audit:storage-cli"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** Multiple locations reference `"closed"` as a task status, but it doesn't
exist — terminals are `done`, `failed`, `cancelled`. A developer trusting these
references may write code checking `status='closed'` (matching zero rows), silently
breaking logic.

**Locations:**
- `cockpit.rs:389,578` — display code references non-existent `"closed"` status
- `schema.sql:65` — comment says deps gate on `status='closed'`; actual SQL checks
  `status='done'` (tasks.rs:246, 369)

**Evidence:** baseline audit stale-vocabulary sweep + audits/2026-07-06-05-storage-cli.md
Finding 2.

**Fix:** Replace `'closed'` with `'done'` in schema.sql comment and cockpit.rs display
logic.

Sourced from: audits/2026-07-06-00-baseline.md — §4 Stale vocabulary,
audits/2026-07-06-05-storage-cli.md — Finding 2

---

### M7. Supervisor build has no timeout

`title:` fix: add timeout to dev-install.sh call in serve-supervisor.sh
`severity:` medium
`labels:` `["kind:bug","severity:medium","audit:ops"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** `serve-supervisor.sh:77` runs `./dev-install.sh` (which runs `cargo build
--release`) with no timeout. If the build hangs (crates.io download, proc-macro loop),
the supervisor blocks forever. No daemon is running, all agents are idle, and the thrash
guard cannot trip.

**Evidence:** `scripts/serve-supervisor.sh:77` (no timeout wrapper).

**Fix:** Wrap in `timeout 300 ./dev-install.sh`. On timeout, alert and relaunch old
binary.

Sourced from: audits/2026-07-06-06-ops-scripts-test-harness.md — Finding 3

---

### M8. Supervisor alert() only goes to stderr; invisible when daemonized

`title:` fix: supervisor alerts should be observable when daemonized
`severity:` medium
`labels:` `["kind:bug","severity:medium","audit:ops"]`
`needs_owner_call:` yes
`FILE:` yes

`body:`

**Problem.** `alert()` in `serve-supervisor.sh:38–40` is `printf ... >&2`. When run
daemonized (nohup, systemd), stderr may be `/dev/null`. Build failures, thrash guard
trips, and stale binary relaunches are invisible.

**Evidence:** `scripts/serve-supervisor.sh:38–40` (alert is stderr-only).

**Fix options:** Post a `quorum post --kind alert` message into the DB, write to a
well-known log file, or call an external notifier. Owner call: which notification
channel?

**Owner decision (2026-07-06 discernment):** file approved. Channel:
`quorum post --kind alert`.

Sourced from: audits/2026-07-06-06-ops-scripts-test-harness.md — Finding 4

---

### M9. Branch allocation retry exhaustion returns raw DB error

`title:` fix: branch collision exhaustion should return exit 2, not exit 3
`severity:` medium
`labels:` `["kind:bug","severity:medium","audit:storage-cli"]`
`needs_owner_call:` no
`FILE:` no

`body:`

**Problem.** When the 3-retry collision budget in `allocate_for_task` is exhausted, the
4th `SQLITE_CONSTRAINT_UNIQUE` falls through to `Err(e.into())`, mapping to
`QuorumError::Db` → exit 3. This is classified as internal/DB error rather than the more
accurate usage/input error (exit 2).

**Evidence:** `quorum-core/src/branches.rs:132–139` (retry exhaustion returns raw error).

**Fix:** Return `QuorumError::Usage` with a descriptive message about branch-name
collision.

Sourced from: audits/2026-07-06-05-storage-cli.md — Finding 6

---

### M10. Force-kill fires AgentFailed on Merging tasks — rejected, stuck in merging

`title:` bug: force-kill during merge leaves task stuck in merging state
`severity:` medium
`labels:` `["kind:bug","severity:medium","audit:lifecycle","audit:recovery","source:audit-03"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** `(Merging, AgentFailed)` is rejected by the lifecycle ("merging in
progress", lifecycle.rs:308), but the daemon's force-kill path fires `AgentFailed` after
killing agents. A task force-killed mid-merge stays in `merging` with no agent, no
lease, and no expected actor — a permanent stall requiring manual intervention.

**Evidence:** `quorum-core/src/lifecycle.rs:308` (reject), audit 03 out-of-scope
handoffs (raised, never promoted to a finding), consolidation footer item 4.

**Fix:** Decide the intended cell: fire `Cancelled` (or a new `MergeInterrupted`) from
the force-kill path, or have recovery re-arm the merge wait for `merging` tasks
(see the #228 approval-recovery precedent).

Sourced from: audits/2026-07-06-03-recovery-self-healing.md — out-of-scope handoffs;
consolidation footer item 4 (promoted 2026-07-06 discernment pass).

---

## Medium — test gaps

### T1. No crash-recovery integration tests for recovery::recover

`title:` test: add integration tests for recovery::recover crash matrix
`severity:` medium
`labels:` `["kind:test","severity:medium","audit:recovery","audit:baseline"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** `recovery.rs` has 21.47% line coverage — the only file under 50%. The only
tests are 3 unit tests for `build_resume_turn`. Zero tests exercise the `recover()`
function, which contains the critical crash matrix logic. 13 crash matrix cells are
explicitly untested (worker/working with/without worktree, awaiting-review with/without
PR, reviewer teardown, spawn failure, feed_turn failure, orphaned worktree GC, double
restart).

**Evidence:** `quorum/src/serve/recovery.rs:422–481` (only build_resume_turn tests),
baseline audit coverage report (21.47% for recovery.rs).

**Minimum coverage:** worker/working with existing worktree, worker/working with missing
worktree, worker/awaiting-review with PR, reviewer teardown, spawn failure fallback,
double-restart on same journal state.

Sourced from: audits/2026-07-06-03-recovery-self-healing.md — Finding 6,
audits/2026-07-06-00-baseline.md — §3 Coverage map

---

### T2. Missing lifecycle property/walk tests

`title:` test: add property tests and multi-step lifecycle walk tests
`severity:` medium
`labels:` `["kind:test","severity:medium","audit:lifecycle"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** The lifecycle test suite covers every cell of the transition table but lacks:
(a) property/fuzz tests asserting invariants across random event sequences (terminals
absorb, rework_round monotonic, never PR-bearing→Open except from Rework), (b) multi-step
walk: MergeFailed → re-review → done, (c) Rework → Open → re-claim with PR/branch
preservation, (d) close_after_merge from non-working states (in-review, merging), (e)
reviewer replacement after expiry (regression test for H1).

**Evidence:** `quorum-core/src/lifecycle.rs:327–944` (test module),
`quorum-core/src/tasks.rs:822–1624` (test module) — categories 5a-5e all absent.

**Fix:** Add proptest/quickcheck fuzz for lifecycle invariants + 4 integration tests for
the named walk/scenario gaps.

Sourced from: audits/2026-07-06-01-lifecycle-claim-integrity.md — Finding 5

---

### T3. No test: drain timeout violated by merge-in-progress

`title:` test: drain timeout with merge-in-progress tick
`severity:` medium
`labels:` `["kind:test","severity:medium","audit:tick-effects"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** `drain_timeout_force_kills_and_exits_75` tests drain timeout but does not
cover the case where `wait_for_checks` is blocking when the timeout fires. Expected: this
test will FAIL against current code, documenting the bug from C1.

**Evidence:** `quorum/tests/cli_serve_drain.rs:448` (no merge-in-progress scenario).

**Test spec:** Set drain_timeout_secs=2, merge_checks_timeout_secs=30. Seed task, worker
done, trigger drain. Verify daemon exits within drain_timeout_secs + one tick, NOT after
merge_checks_timeout_secs.

Sourced from: audits/2026-07-06-02-tick-effects.md — Finding 6

---

### T4. No test: fire_event failure → reviewer spawn loop

`title:` test: externally cancelled task does not trigger reviewer spawn loop
`severity:` medium
`labels:` `["kind:test","severity:medium","audit:tick-effects"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** No test covers the scenario where a task is externally cancelled while its
worker is active, causing `fire_event(SignaledDone)` to fail. This is the regression test
for C3.

**Evidence:** No existing test in `quorum/tests/` covers lifecycle rejection during
worker done handling.

**Test spec:** Seed task, start worker, externally cancel task, worker signals done with
PR. Verify: no reviewer spawned, worker slot cleaned up, task remains cancelled.

Sourced from: audits/2026-07-06-02-tick-effects.md — Finding 7

---

### T5. No test: SignaledDone on already-merged PR

`title:` test: worker done on already-merged PR should close without reviewer
`severity:` medium
`labels:` `["kind:test","severity:medium","audit:tick-effects"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** No test covers the scenario where a worker signals done with a PR that has
already been merged on GitHub. This is the regression test for H3.

**Evidence:** No test in `quorum/tests/cli_serve_reviewer.rs` pre-merges the PR before
the worker signals done.

**Test spec:** Configure mock merge executor to report PR as already merged. Seed task,
worker signals done. Verify: daemon detects merged state and fires MergeSucceeded, OR
document current (broken) behavior.

Sourced from: audits/2026-07-06-02-tick-effects.md — Finding 8

---

### T6. No test: mailbox consume failure → idempotent replay

`title:` test: mailbox consume failure replay is idempotent
`severity:` medium
`labels:` `["kind:test","severity:medium","audit:tick-effects"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** `consume_mailbox_row` (mod.rs:2429–2452) returns `false` on failure, causing
retry next tick. No test verifies that replaying a mailbox row is idempotent — that the
second `fire_event` call fails harmlessly.

**Evidence:** No test in `quorum/tests/` covers mailbox consume failure path.

**Test spec:** Inject fault into mark_consumed. Process Done mailbox row. On next tick,
verify: row re-polled, fire_event fails with InvalidTransition, daemon proceeds without
corruption.

Sourced from: audits/2026-07-06-02-tick-effects.md — Finding 9

---

### T7. No daemon_lock contention test

`title:` test: add multi-thread daemon_lock contention test
`severity:` medium
`labels:` `["kind:test","severity:medium","audit:recovery"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** daemon_lock tests cover single-threaded scenarios but have no multi-process
or multi-thread test verifying concurrent `try_acquire` calls resolve correctly under
`BEGIN IMMEDIATE`. This is the same class as the N-process claim-race canary.

**Evidence:** `quorum-core/src/daemon_lock.rs:86–203` (tests, all sequential).

**Test spec:** Spawn N threads calling try_acquire with different PIDs. Assert exactly one
gets Acquired, rest get Held. Run in a loop (1..12) to stress.

Sourced from: audits/2026-07-06-03-recovery-self-healing.md — Finding 7

---

### T8. Liveness/watchdog test coverage gaps

`title:` test: add stall-injection, wall-clock, and reviewer ceiling tests
`severity:` medium
`labels:` `["kind:test","severity:medium","audit:liveness"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** Three categories of liveness/watchdog tests are missing:

**(a) Stall injection (stats.rs):** No test injects a stall with phase-aware
DaemonAgentView inputs. Needed: awaiting-review worker not stalled, working worker
stalled after 180s, reviewer not counted in stall, mixed fleet isolation.

**(b) Wall-clock ceilings (cli_serve_watchdog.rs):** No integration test for
`max_turn_wall_secs` or `max_task_wall_secs` ceiling fire → worker killed → task
released. The `check_wall_clock_limits` function (mod.rs:2556–2569) is untested.

**(c) Reviewer ceiling fire (cli_serve_watchdog.rs):** All watchdog tests cover worker
kills only. Reviewer ceiling enforcement (mod.rs:1960–1993) is never tested. A bug here
would leave tasks stuck in in-review.

**Evidence:** `quorum-core/src/stats.rs:2006–2041` (no phase values in test views),
`quorum/tests/cli_serve_watchdog.rs` (no wall-clock or reviewer ceiling tests).

Sourced from: audits/2026-07-06-04-liveness.md — Findings 3, 4, 5

---

### T9. No v19→v20 migration test

`title:` test: add explicit v19→v20 migration test for lifecycle columns
`severity:` medium
`labels:` `["kind:test","severity:medium","audit:storage-cli"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** The v20 migration adds author, reviewer, rework_round, review_only to
`tasks`. No test covers v19→v20 explicitly. The v17→v18 test implicitly exercises later
migrations but does not assert the v20 columns exist or have correct defaults.

**Evidence:** `quorum-core/src/db.rs:312–333` (v20 migration), db.rs test module (no
v19→v20 test).

**Test spec:** Hand-craft v19 DB, insert seed task row, open via `open()`, assert
user_version == SCHEMA_VERSION, assert all four columns exist with correct defaults.

Sourced from: audits/2026-07-06-05-storage-cli.md — Finding 3

---

### T10. No concurrent branch allocation test

`title:` test: add concurrent branch allocation test
`severity:` medium
`labels:` `["kind:test","severity:medium","audit:storage-cli"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** No multi-process or multi-thread test exercises concurrent branch
allocations. Two concurrent CLI processes claiming different tasks with identical
slugified titles could race on the UNIQUE(branch) collision retry. The claim-race canary
covers task-claim but not the branch allocation that follows.

**Evidence:** `quorum-core/src/branches.rs:67–141` (allocate_for_task with retry).

**Test spec:** N threads calling allocate_for_task with different task_ids but identical
slugs. Assert each gets a distinct branch, no errors. Also test retry budget exhaustion
(4+ collisions) returns clean error.

Sourced from: audits/2026-07-06-05-storage-cli.md — Finding 4

---

### T11. fake_agent fidelity gaps

`title:` test: fake_agent should exercise quorum done and tool_use events
`severity:` medium
`labels:` `["kind:test","severity:medium","audit:ops"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** fake_agent emits `assistant` + `result` events on stdout but (a) never
writes a `MailboxKind::Done` row via `quorum done`, requiring tests to inject mailbox
rows separately, and (b) never emits `tool_use` events, so `tool_count` and `now_label`
are always 0/empty in tests. If the CLI contract between daemon and agent changes (e.g.,
`--pr` renamed), no integration test catches the seam break.

**Evidence:** `quorum/src/bin/fake_agent.rs` (no quorum done call, no tool_use events).

**Fix:** Add `--with-side-effects` mode that calls `quorum done` as subprocess, and
`--emit-tool-use` mode that emits 2-3 tool_use events. Update at least one serve test
per mode.

Sourced from: audits/2026-07-06-06-ops-scripts-test-harness.md — Findings 5, 6

---

### T12. Supervisor and serve test infrastructure gaps

`title:` test: supervisor shell tests in CI + signal coverage + fixed-sleep removal
`severity:` medium
`labels:` `["kind:test","severity:medium","audit:ops"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** Three test infrastructure gaps:

**(a) Shell tests not in CI:** `scripts/test-serve-supervisor.sh` has 7 tests but CI
never runs them. A supervisor change could break and merge undetected.

**(b) No signal handling test coverage:** Once signal forwarding is added (H8), the test
suite needs SIGTERM/SIGINT cases verifying both supervisor and child exit.

**(c) Fixed-duration sleeps in serve tests:** 30+ `thread::sleep` calls (1–3s) across
serve tests. On loaded CI runners, the daemon tick may not complete in time → flaky
failures. Estimated ~78s of pure wait time inflating CI wall-clock.

**Evidence:** `.github/workflows/ci.yml` (no shell test step),
`scripts/test-serve-supervisor.sh` (no signal tests),
multiple `cli_serve_*.rs` files (sleep calls throughout).

**Fix:** (a) Add `shell-tests` CI job. (b) Add signal test cases after H8 fix. (c)
Replace fixed sleeps with event-driven synchronization using `wait_for()` or polling
`quorum task-get --json`.

Sourced from: audits/2026-07-06-06-ops-scripts-test-harness.md — Findings 7, 9, 10

---

## Low — docs / dead code

### L1. CLAUDE.md status line is massively stale

`title:` docs: update CLAUDE.md status line — all numbers are wrong
`severity:` low
`labels:` `["kind:docs","severity:low","audit:ops"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** CLAUDE.md line 11 reads: "11 core modules, 6 bin modules, schema v5, 142
tests". Actual: 22 core modules, 1+11 bin/serve modules, schema v20, 306 tests. Every
number is wrong. An agent reading CLAUDE.md for orientation gets a picture of a project
nothing like the current codebase.

**Evidence:** `CLAUDE.md:11` vs `quorum-core/src/db.rs:12` (v20), `cargo test` (306).

**Fix:** Update line 11 with current counts, or replace with a "run `cargo test` to see
current counts" pointer to prevent rot.

Sourced from: audits/2026-07-06-06-ops-scripts-test-harness.md — Finding 8

---

### L2. Effect::SpawnWorker is dead code — never produced by any transition

`title:` chore: remove unused Effect::SpawnWorker variant
`severity:` low
`labels:` `["kind:chore","severity:low","audit:lifecycle"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** `Effect::SpawnWorker` is defined (lifecycle.rs:90) and mapped to
`"spawn_worker"` (tasks.rs:172), but no lifecycle transition produces it. The daemon
discovers open tasks and spawns workers in its tick loop, independent of lifecycle
effects. The variant inflates the Effect enum and could mislead readers.

**Evidence:** `quorum-core/src/lifecycle.rs:90` (variant defined),
`quorum-core/src/tasks.rs:172` (name mapping), exhaustive search confirms no transition
returns it.

**Fix:** Remove `Effect::SpawnWorker` and its name mapping.

Sourced from: audits/2026-07-06-01-lifecycle-claim-integrity.md — Finding 6

---

### L3. _reviewers parameter in recovery::recover is unused dead weight

`title:` chore: remove unused _reviewers parameter from recovery::recover
`severity:` low
`labels:` `["kind:chore","severity:low","audit:recovery"]`
`needs_owner_call:` no
`FILE:` yes

`body:`

**Problem.** The `_reviewers: &mut [SlotState]` parameter in `recovery::recover`
(recovery.rs:65) is declared with a leading underscore. Recovery tears down all recovered
reviewers by deleting journal entries — it never pushes to the reviewers slice. The
parameter is dead weight in a function with 7 parameters (already
`#[allow(clippy::too_many_arguments)]`).

**Evidence:** `quorum/src/serve/recovery.rs:65` (underscore-prefixed, never used).

**Fix:** Remove the parameter and update the call site in `tick_loop`.

Sourced from: audits/2026-07-06-03-recovery-self-healing.md — Finding 8

---

## Footer: unresolved cross-references

### Ambiguous deduplication notes

1. **H3 vs recovery PendingReview stall:** Audit 2 Finding 3 (no PR state check in Phase
   5) and Audit 3 Finding 2 (PendingReview with merged PR) describe the same class of
   bug — daemon trusts PR is open — but in different code paths (normal tick vs recovery).
   Merged into a single candidate (H3) since the fix is the same pattern (check PR state
   before proceeding), but the recovery path needs a separate code change in
   `recovery.rs:208–243`.

2. **NotifyOwner (H4) vs Audit 4 F2:** Audit 2 Finding 4 and Audit 4 Finding 2 describe
   identical issues (NotifyOwner never dispatched). Merged into H4 with cross-refs to
   both source reports.

3. **"closed" references (M6):** Baseline audit §4 and Audit 5 Finding 2 flag different
   locations (cockpit.rs vs schema.sql) with the same stale term. Merged since the fix is
   a single grep-and-replace.

4. **Audit 3 out-of-scope handoff — Merging + AgentFailed rejected:** Audit 3 noted that
   the force-kill path fires `AgentFailed` after killing agents, but `Merging` rejects
   `AgentFailed` (lifecycle.rs:309). This was not surfaced as a standalone finding by any
   audit. It may warrant a candidate if the force-kill path should fire `Cancelled`
   instead. Flagged for owner review.

5. **sticky_until undocumented:** Baseline audit noted `sticky_until` is live in code
   (sync.rs, schema.sql, db.rs) but absent from the design spec. No downstream audit
   filed this as a finding. Flagged for owner: should this be a docs candidate?

---

## Run log: issue filing (2026-07-06)

**Agent:** Hoist-d24 (task #23)
**Result:** No-op — 0 of 37 candidates marked `FILE: yes`. All entries remain `FILE: pending`. No GitHub issues filed.
