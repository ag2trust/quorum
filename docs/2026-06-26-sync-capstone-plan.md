# `quorum sync` — implementation plan (closes #8)

**Status:** draft — awaiting owner LGTM before code starts
**Author:** Quartzite-7n3 (Claude Opus 4.7)
**Issue:** ag2trust/quorum#8 — locked decisions in the issue body

## What it is

One read-only CLI call that returns everything an agent needs to orient,
in strict priority order, as a single JSON payload. The agent's "compass."
Replaces the 3-4 calls a tick agents do today (`task-get` for current,
`task-list` for next, `read` for messages, `log` for events).

```
quorum sync --agent <id> [--match-label <label>]
```

Output (omit-empty — keys absent when their section is empty):

```jsonc
{
  "stop":         {reason, scope, since, by} | absent,  // 1. HALT — if set, agent stops
  "stop_cleared": true | absent,                        //    one-shot resume signal
  "critical":     [ … ] | absent,                       //    critical msgs to/at me (inlined)
  "current_task": { id, title, … } | absent,            // 2. the task I hold (summary, no body)
  "next_task":    { id, title, … } | absent,            // 3. highest-prio open task (XOR current_task)
  "direct":       [ { seq, author, body, … } ] | absent,// 4. unread direct msgs (full bodies)
  "notifications":{ count, critical: [ … ] } | absent,  //    unread broadcasts: count + critical bodies
  "log":          [ { seq, kind, subject, body, ts } ] | absent  // 5. events on my task/claims, bounded
}
```

## Locked design decisions (from issue + design session)

1. **Read-only.** `sync` never claims. `next_task` is *shown*. (No `--claim` opt.)
2. **State-adaptive XOR.** `current_task` present ⇒ `next_task` omitted (no second-task dangle); idle ⇒ `next_task` may appear.
3. **Omit-empty.** Quiet tick (mid-task, no msgs, no stop) ≈ near-empty payload. Token saver.
4. **No bodies in `sync`.** Tasks come as summaries; bodies are fetched once on `task-claim`/`task-get`.
5. **One side effect:** ack-on-next-tick cursor advance (preserves at-least-once — a message returned by `sync` N is acked at `sync` N+1).
6. **STOP first + absolute.** Global stop and per-agent stop both surface in `stop`; agent halts. Polling `sync` is cheap.
7. **`log` is scoped, not the firehose.** Filter to events whose `subject` matches `task#<my-current-id>` ∪ any `target` I currently hold a claim on. Bounded.

## Phasing

### Phase 1 — Core payload assembler (no stop)

**Why first:** #6 (stop/resume) is still in PR #20 (Lattice rebasing). Per CTO 04:15: "most of sync can be built now." Build everything that doesn't depend on `control` table; wire `stop`/`stop_cleared` in Phase 3 once #6 lands.

**Files:**
- `quorum-core/src/sync.rs` (new) — `Snapshot` struct + `gather(conn, agent, label_filter, now) -> Result<Snapshot>` (read-only).
- `quorum/src/cli.rs` — add `Sync { agent, match_label }` variant.
- `quorum/src/main.rs` — wire dispatch + JSON output (omit-empty via `#[serde(skip_serializing_if = "Option::is_none")]` on every Snapshot field, plus `Vec::is_empty` on lists).
- `quorum/src/cheatsheet.rs` — add `quorum sync` row + brief.

**Reuse:**
- `current_task`: `tasks::get_for_assignee(conn, agent)` (likely add this; or filter `tasks::list(status="claimed", assignee=Some(agent))`).
- `next_task`: extend `tasks::list` or add `tasks::pick_next(conn, label, now)` returning the highest-`priority` `open` + deps-ready + label-matched.
- `direct`/`critical`/`notifications`: extend `feed::read` (or wrap it) to bucket by `recipient = agent`, `kind = "critical"`, broadcast-count.
- `log`: scoped wrap around `events::list` with a subject-set filter.

**Auto-ack:** the one write. `feed::read` already supports `ack_through`; sync passes the highest `seq` it observed from the *previous* tick (stored as the cursor). Mechanism: on each `sync N`, advance cursor to `max(prev_cursor, max_seen_at_sync_N_minus_1)`; the *current* tick's returned messages remain in-window for one more tick (ack-on-next-tick).

  - Implementation sketch: `feed::read` is already cursor-aware. The order is:
    1. Compute the new cursor = the seq we last *showed* (stored in `cursors` table; this is exactly what `feed::read --ack-through` already does).
    2. The `direct`/`critical` payload assembled this tick is what we'll ack on next tick — we just need to *remember* the highest seq we showed. Do NOT advance cursor past unshown messages.
  - **Open question for owner:** does the "ack-on-next-tick" semantics need a new column on `cursors`, or can we re-derive from the prior tick's behavior? My read of feed.rs: pass `ack_through = <highest seq from prior tick>` synthesizes the right behavior with existing schema. Tentative: no schema change.

**Tests (Phase 1):**
- `sync_returns_current_task_when_assigned` (XOR check: no `next_task`).
- `sync_returns_next_task_when_idle` (deps-ready + match-label + highest-priority).
- `sync_omits_empty_sections` (quiet-tick payload is `{}` or near-empty).
- `sync_buckets_messages_correctly` (direct full / broadcast count / critical inlined).
- `sync_log_is_scoped_to_agent_targets` (no firehose; only events whose subject ∈ my-task-targets-set).
- `sync_auto_acks_on_next_tick` (at-least-once: message shown at tick N still shown at N+1, gone by N+2).
- `sync_is_idempotent_when_called_twice_with_no_state_change` (read-only — second call returns same payload; cursor advance doesn't change the read-set on a quiet tick).

### Phase 2 — Label filter + scoping

`--match-label` scopes `next_task` to the agent's capabilities. Re-uses #1's `task_claim --match-label` plumbing.

**Tests:**
- `sync_with_match_label_filters_next_task`
- `sync_match_label_does_not_affect_current_task` (you keep what you hold).

### Phase 3 — Wire `stop` + `stop_cleared` (after #6/#20 merges)

Tiny: re-read the `control` table inside `gather()`, populate `stop` if `is_stopped(global) OR is_stopped(agent)`, populate `stop_cleared: true` for one tick if the prior tick saw a stop and this one doesn't.

**Open question for owner:** `stop_cleared` is one-shot per agent. Do we track it via the cursors table (new column `last_stop_seen_at`) or derive from the event log (find the most recent `stop_cleared` event after my prior `sync` and emit `true`)? Tentative: event-log derivation — no schema change, idempotent.

**Tests (Phase 3):**
- `sync_stop_global` (HALT surfaces; `current_task`/`next_task`/etc still NOT shown — STOP is absolute).
- `sync_stop_targeted_at_me`
- `sync_stop_cleared_fires_once` (after resume, exactly one `stop_cleared: true`; next tick omits).

### Phase 4 — Cheatsheet, README table, polish

- `quorum/src/cheatsheet.rs` final wording.
- README command table row.
- Stress-canary still green.

## Files (estimate)

| Phase | Files touched | LoC est. |
|-------|---------------|---------:|
| 1 | sync.rs (new ~250), cli.rs (+10), main.rs (+30), feed.rs (small bucket helper), tasks.rs (small `pick_next`), tests (~200) | ~500 |
| 2 | sync.rs (+20), tests (~50) | ~70 |
| 3 | sync.rs (+40), tests (~80) | ~120 |
| 4 | cheatsheet.rs (+10), README (+5) | ~15 |

**Total: ~700 LoC.** Large for quorum's norm, justified by the issue: "this replaces the multi-call agent loop."

## Risks / things to call out

1. **Auto-ack semantics are subtle.** At-least-once is the contract; my read of feed.rs says ack-on-next-tick can be expressed with existing `feed::read --ack-through` plumbing. If owner reads it differently, schema change needed. Phase 1 will write a focused test (`sync_auto_acks_on_next_tick`) before committing.

2. **`log` scoping definition.** "Events on my task/claims" needs a precise filter. Tentative: `subject IN ('task#<my-current-id>') UNION (target FROM claims WHERE holder = me AND active = 1 AND expires_at > now)`. Bounded to `LIMIT 20` (matches `feed::DEFAULT_READ_LIMIT`-ish; tunable).

3. **Phase 3 needs #6 merged.** PR #20 (Lattice rebasing) is the unblock. Phase 1+2 can ship first.

4. **`reviewDecision: REVIEW_REQUIRED` blocker.** While `ag2trust-dev` is at `read` perm on quorum, `gh pr create` as ag2trust-dev fails. Implementation can proceed; PR-open waits on @brevitize's perm bump (already escalated 04:11).

## Owner / CTO check-in points

- [ ] **LGTM on this plan + phasing** before Phase 1 code starts.
- [ ] **Confirm auto-ack semantics** (ack-on-next-tick via existing `feed::read --ack-through`, no schema change) — answer determines Phase 1 test shape.
- [ ] After Phase 1+2 land + merge: greenlight Phase 3 (wire stop) once #20 merges.

— Quartzite-7n3
