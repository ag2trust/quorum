# Post-refactor audit 1/6: lifecycle & claim integrity

**Date:** 2026-07-06
**Auditor:** Bellows-d11
**Commit:** `4d23f320` (HEAD of `main`)
**Scope:** `quorum-core/src/lifecycle.rs`, `quorum-core/src/tasks.rs`,
`quorum-core/src/claims.rs`, `quorum-core/src/branches.rs`

## Method

Cell-by-cell walk of every `(Status, Event)` pair in `lifecycle::transition` against
the design-of-record transition table (`docs/2026-06-23-quorum-design.md` §Task
lifecycle). Then integration audit of `tasks.rs` (which persists lifecycle effects and
owns the claim/lease SQL), `claims.rs` (atomic claim primitive), and `branches.rs`
(branch allocation, claim-adjacent).

## Transition table audit

All 88 cells (8 statuses × 11 events) verified against the spec. Every documented
valid transition produces the correct `(next_status, effects)`. Every undocumented
pair is explicitly rejected. One deviation noted (Finding 3).

## Findings (priority order per audit contract)

### Finding 1 — [P1: Duplicated-execution risk] `apply_event` has no caller authorization

**File:line:** `quorum-core/src/tasks.rs:463-494`

**Evidence:** The `agent` parameter is used for `agents::touch` (line 471) and event
emission (line 546), but never compared against the task's `assignee`, `author`, or
`reviewer` before firing the lifecycle transition (line 493).

The `update` backward-compat function (line 631-639) correctly gates on
`AND assignee=?6` in its SQL. The CLI `task-update --verdict` path (main.rs:357) calls
`apply_event` directly — no assignee check wraps it.

**Failure scenario:** Worker A claims task #7 (status=working, author=A, lease
expires T+3600). At T+3700 the daemon fires `LeaseExpired` → task returns to open.
Worker B claims task #7 (status=working, author=B). Worker A (stale, still running)
calls `quorum task-update --agent A --task-id 7 --verdict approved` or the daemon
calls `apply_event("A", 7, SignaledDone{pr:"old"})`. The lifecycle sees
status=working → accepts SignaledDone → task transitions to in-review with A's
stale PR. B's in-progress work is orphaned.

**Proposed fix-task:**
```
Add caller-identity guard to `apply_event` for agent-initiated events.
For SignaledDone / ReworkPushed: require agent == task.assignee (or author).
For VerdictApprove / VerdictChanges: require agent == task.reviewer (or assignee).
For daemon-initiated events (LeaseExpired, AgentFailed, MergeSucceeded,
MergeFailed, Cancelled): accept any caller (these are system events).
Return QuorumError::NotHolder on mismatch (exit 1, not an error).
```

---

### Finding 2 — [P2: Silent-stall risk] Reviewer not cleared after InReview failure/expiry → new reviewer cannot claim

**File:line:** `quorum-core/src/tasks.rs:516-523` (ReleaseLease handler in
`apply_event`), `quorum-core/src/tasks.rs:384-387` (claim SQL WHERE clause)

**Evidence:** When `InReview + AgentFailed` or `InReview + LeaseExpired` fires, the
lifecycle returns `(InReview, [ReleaseLease, SpawnReviewer])`. The `ReleaseLease`
handler in `apply_event` deactivates the lease (line 517) and clears `assignee` only
when `new_status.is_terminal() || new_status == Status::Open` (line 518). Since
`new_status` is `InReview` (sticky), neither `assignee` nor `reviewer` is cleared.

The task-claim SQL at line 384-387 requires `reviewer IS NULL` to attach a new
reviewer:
```sql
OR (status='in-review' AND reviewer IS NULL
    AND (author IS NULL OR author != ?1))
```

**Failure scenario:**
1. Task #5: status=in-review, author=W1, reviewer=R1, assignee=R1
2. R1's lease expires → daemon fires `LeaseExpired`
3. `apply_event` transitions: InReview → InReview, effects=[ReleaseLease,
   SpawnReviewer]
4. After the transaction: reviewer="R1" (not cleared), assignee="R1" (not cleared)
5. Daemon processes SpawnReviewer → spawns reviewer R2
6. R2 calls `quorum task-claim --agent R2 --task-id 5`
7. SQL WHERE: `status='in-review' AND reviewer IS NULL` → FALSE → returns None
8. Task is stuck: in-review, no active reviewer, no way to attach one via
   `task-claim`

The same applies to auto-pick (line 398: same `reviewer IS NULL` condition).

**Proposed fix-task:**
```
In `apply_event`'s ReleaseLease handler, clear `reviewer` and `assignee` when
`new_status == Status::InReview` (the sticky-review case). This allows
SpawnReviewer's freshly spawned reviewer to claim via the standard task-claim
path. The old reviewer name is preserved in the event log for audit trail.
Alternative: change the claim SQL to allow re-claiming in-review tasks when
the current reviewer's lease is expired (check claims table for active lease).
```

---

### Finding 3 — [P3: Correctness vs spec] `update` allows status 'done' from working, bypassing lifecycle review

**File:line:** `quorum-core/src/tasks.rs:577-586` (restricted status list),
`quorum-core/src/tasks.rs:631-639` (generic else branch)

**Evidence:** The restricted-status list at line 581 is
`["working", "in-review", "rework", "merging", "failed"]`. Status `"done"` is absent.
The generic else branch (line 631-639) executes:
```sql
UPDATE tasks SET status = COALESCE(?2, status) ...
WHERE id=?1 AND assignee=?6 AND status='working'
```
This allows a worker to set status directly to `done` from `working`, skipping
in-review → merging → done entirely. The spec (§Command surface) says: "Only `open`
(release/reopen) and `cancelled` are directly settable."

**Failure scenario:** Worker W calls `quorum task-update --agent W --task-id 8
--status done` on a working task. The task transitions working → done without review.
The rework cap, review-only guard, and reviewer-differs-from-author checks are all
bypassed. The function is labeled "backward compat for serve/" so the daemon may
rely on this path, but it's also reachable from the CLI.

**Proposed fix-task:**
```
Either add "done" to the restricted list (line 581) and route all done
transitions through apply_event's lifecycle, OR guard the else-branch so only
callers with a daemon-internal flag can bypass review. If the daemon needs a
direct working→done path for recovery (e.g., close_after_merge already covers
this), remove the status='done' path from `update` entirely.
```

---

### Finding 4 — [P3: Spec deviation] InReview + AgentFailed effects differ from spec

**File:line:** `quorum-core/src/lifecycle.rs:240-249`

**Evidence:** The code returns `[ReleaseLease, NotifyOwner{reason}, SpawnReviewer]`
for `(InReview, AgentFailed)`. The spec (§Transition table, "From InReview") groups
AgentFailed and LeaseExpired together with effects `[ReleaseLease, SpawnReviewer]` —
no NotifyOwner.

The code correctly distinguishes the two: AgentFailed includes NotifyOwner (agent
crashed — worth alerting); LeaseExpired does not (timeout — routine). This is better
behavior than the spec describes.

**Failure scenario:** No correctness bug — the code is more informative than the
spec promises. A developer reading the spec would not expect NotifyOwner in this cell
and might remove it in a refactor, breaking the owner-notification path for reviewer
crashes.

**Proposed fix-task:**
```
Update docs/2026-06-23-quorum-design.md §Transition table "From InReview":
split AgentFailed and LeaseExpired into separate bullets:
- AgentFailed → InReview (sticky) · effects: ReleaseLease, NotifyOwner, SpawnReviewer
- LeaseExpired → InReview (sticky) · effects: ReleaseLease, SpawnReviewer
```

---

### Finding 5 — [P4: Test gaps] Missing test categories

**File:line:** `quorum-core/src/lifecycle.rs:327-944` (test module),
`quorum-core/src/tasks.rs:822-1624` (test module)

**Evidence:** The existing test suite covers every cell of the transition table and
the major guards (reviewer≠author, rework_cap, review_only). The following categories
are absent:

**5a. No property-style / fuzz tests for lifecycle invariants.**
The lifecycle is a pure function with a small input space — ideal for property testing.
No test asserts invariants across random event sequences:
- "Never two active holders for the same task" (requires integrating with claims)
- "Terminals absorb all events" (tested per-terminal with sample events, but not
  fuzzed against arbitrary event sequences)
- "A task that has visited InReview cannot reach Open except through Rework"
  (verified manually in the table walk, but not mechanically tested)
- "rework_round is monotonically non-decreasing"

**5b. No multi-step walk for MergeFailed → re-review → done.**
`MergeFailed` returns the task to InReview with `ResumeReviewer`. No integration test
exercises the full path: merging → in-review (merge failed) → merging (re-approved) →
done.

**5c. No test for Rework → Open → re-claim with PR preservation.**
When Rework + AgentFailed/LeaseExpired transitions to Open, the PR ref stays in
`refs` and the branch allocation (branches.rs) returns the same branch. No test
verifies this end-to-end: create → claim → done → in-review → changes → rework →
agent-failed → open → re-claim (same branch? PR preserved? rework_round preserved?).

**5d. No test for `close_after_merge` from non-working states.**
`close_after_merge` (tasks.rs:687-709) accepts any non-terminal status via
`WHERE status NOT IN ('done','failed','cancelled')`. The test at line 1431 only
exercises it from working. No test from in-review, rework, or merging — these are
the states where a manual/external merge is most likely to be discovered by the
daemon.

**5e. No test for reviewer-stall scenario (Finding 2).**
No test verifies that a reviewer can (or cannot) claim a task after the previous
reviewer's failure/expiry. This would have caught Finding 2.

**Proposed fix-task:**
```
Add the following tests to quorum-core:
1. proptest/quickcheck: fuzz lifecycle::transition with random (Status, Event)
   sequences, asserting: terminals absorb, rework_round monotonic, never
   PR-bearing→Open except from Rework.
2. Integration test: merge_failed_re_review_done_walk — exercises the full
   MergeFailed recovery chain.
3. Integration test: rework_agent_failed_reclaim — verifies PR ref, branch
   allocation, and rework_round survive Rework→Open→re-claim.
4. Integration test: close_after_merge_from_in_review (and _from_merging).
5. Integration test: reviewer_replacement_after_expiry — the Finding 2
   regression test.
```

---

### Finding 6 — [P5: Dead code] `Effect::SpawnWorker` defined but never produced by any transition

**File:line:** `quorum-core/src/lifecycle.rs:90`, `quorum-core/src/tasks.rs:172`

**Evidence:** `Effect::SpawnWorker` is defined as a variant (lifecycle.rs:90) and
mapped to the string `"spawn_worker"` (tasks.rs:172). Exhaustive search of
lifecycle.rs confirms no transition produces it: `Open+Claimed` → `SetAuthor`,
`Working+LeaseExpired/AgentFailed` → `ReleaseLease` (+NotifyOwner), `Rework+` →
`ResumeWorker` (not SpawnWorker). The daemon discovers open tasks and spawns workers
in its tick loop, independent of lifecycle effects.

**Failure scenario:** No runtime impact — the variant is unreachable from the
lifecycle. It inflates the Effect enum and its name-mapping function, and could
mislead a reader into thinking the lifecycle signals worker spawning (it does not —
only reviewer spawning is lifecycle-driven).

**Proposed fix-task:**
```
Remove Effect::SpawnWorker from lifecycle.rs and its name mapping in tasks.rs.
If a future lifecycle transition needs it, re-add then. The daemon's open-task
scanning is the correct worker-spawn mechanism; the lifecycle shouldn't
duplicate it with a never-produced effect.
```

---

## Out-of-scope handoffs

- **Merging has no self-healing timeout (audit 2).** Tasks in Merging can only exit
  via daemon-driven events (MergeSucceeded, MergeFailed, Cancelled). If the daemon
  crashes during a merge and doesn't restart, the task is stuck. LeaseExpired and
  AgentFailed are both rejected from Merging. The daemon's tick loop (serve/mod.rs)
  must handle orphaned Merging tasks — verify in audit 2.

- **`"closed"` status reference in cockpit.rs (audit 4/5).** The baseline audit noted
  `cockpit.rs:389,578` references a `"closed"` status that doesn't exist in the state
  machine (terminals are done/failed/cancelled). Cosmetic, but could confuse status
  rendering.

- **`sticky_until` and `orig` columns live in schema but not in design spec (audit
  6).** These are legacy columns from the pre-lifecycle review-task model. The
  lifecycle refactor doesn't use them but they're still in the schema (schema.sql:68-75).

## Transition table verification matrix

For completeness, the full cell-by-cell matrix of `(status, event) → result`:

| Status \ Event | Claimed | SignaledDone | ReviewerAttached | VerdictApprove | VerdictChanges | ReworkPushed | MergeSucceeded | MergeFailed | LeaseExpired | AgentFailed | Cancelled |
|---|---|---|---|---|---|---|---|---|---|---|---|
| **Open** | Working ✓ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | Cancelled ✓ |
| **Working** | ✗ | InReview ✓ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | Open ✓ | Open ✓ | Cancelled ✓ |
| **InReview** | ✗ | ✗ | InReview ✓† | Merging ✓ | Rework/Failed ✓‡ | ✗ | ✗ | ✗ | InReview ✓ | InReview ✓ | Cancelled ✓ |
| **Rework** | ✗ | ✗ | ✗ | ✗ | ✗ | InReview ✓ | ✗ | ✗ | Open ✓ | Open ✓ | Cancelled ✓ |
| **Merging** | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | Done ✓ | InReview ✓ | ✗ | ✗ | Cancelled ✓ |
| **Done** | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |
| **Failed** | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |
| **Cancelled** | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ |

† Guard: agent ≠ author
‡ Rework if `rework_round < REWORK_CAP` and not `review_only`; Failed if `review_only` or `rework_round ≥ REWORK_CAP`

✓ = matches spec. ✗ = correctly rejected. All 88 cells verified.

## Claim integrity verification

- **Partial unique index** `UNIQUE(target) WHERE active=1` on claims table: verified
  in schema.sql:49, used by claims.rs:52-54 (extended code match). ✓
- **Boundary consistency** (`DEAD iff expires_at <= now`, `LIVE iff expires_at > now`):
  verified across all 6 filter sites in claims.rs and tasks.rs. ✓
- **Reap-on-claim** in claims.rs:79 (`expires_at <= ?2`) consistent with read-filter
  in claims.rs:109 (`expires_at > ?2`). ✓
- **Task-claim atomicity**: single `UPDATE ... WHERE status='open' RETURNING` under
  `BEGIN IMMEDIATE` — two concurrent processes: one gets the row, the other gets None.
  Verified by existing 12-process canary. ✓
- **Lease deactivation**: `deactivate_lease` (tasks.rs:41-46) targets live claims
  (`expires_at > now`); expired-but-active rows are reaped by the claim code's
  separate sweep (`expires_at <= now` at tasks.rs:439). Both paths covered. ✓

## Verification

`cargo test` — 666 passed, 0 failed, 0 ignored. No source changes made.
