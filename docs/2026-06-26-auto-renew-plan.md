# `auto-renew on agent touch` — implementation plan (closes #55)

**Status:** draft — small enough to ship in one PR; calling out plan first to surface the
deletion of `task-renew` for owner sign-off before code lands.
**Author:** Quartzite-7n3 (Claude Opus 4.7)
**Issue:** ag2trust/quorum#55

## What it is

Every command invoked with `--agent <id>` extends `expires_at` on **all active leases held
by that agent** to `now + default_ttl`, atomically with the existing `agents::touch`
presence bump. An agent keeps its work simply by *working through quorum* — no separate
heartbeat. Removes `task-renew` (pure duplicate); generic `renew` stays as an optional
manual override.

## Locked decisions (from the issue)

1. **Auto-renew is automatic on `touch`.** Every write site that calls
   `agents::touch(tx, agent, now)` triggers the lease-extend on `<agent>`'s active leases.
   No new code at the call sites.
2. **Race-safe.** Renew is one UPDATE inside the caller's existing `BEGIN IMMEDIATE`
   transaction — the lock is already held. The N-process race canary must stay green.
3. **Default TTL.** Reuse the existing `tasks::DEFAULT_LEASE_TTL_SECS` (3600s by default;
   `quorum/src/config.rs` already references this via the cli `Config`). For the core
   `agents::touch` to pick it up, we either (a) hard-code on the core const, or (b) pass
   the TTL in. **Lean: (a)** — `touch` is in core; the cli's config value mirrors the core
   const (Phase 1 of #32 pinned `Config::default()` to the core consts). One source of
   truth: `tasks::DEFAULT_LEASE_TTL_SECS`.
4. **Remove `task-renew`.** Pure duplicate per the issue.
5. **Generic `renew` stays.** Owner's lean per the issue. Optional manual extend.
6. **Pure-read commands don't renew.** `status`, `task-list`, `peek`, `roster`, `log`,
   `claims`, `stops`, `task-get` — none call `touch`, so they're already covered. Sanity:
   `sync::gather` reads only, doesn't touch. `sync::tick` writes a cursor advance — does it
   touch? **Currently no.** Decision: `sync::tick` SHOULD touch (it's the agent's main loop
   entry; without touch, the agent's compass is its only command and it wouldn't auto-renew).
   This means adding `agents::touch` inside `tick`'s cursor-advance txn.

## Implementation

**One PR, single phase** (small enough; the bulk is the `touch` extension + propagation).

### Files

| File | Change | LoC est. |
|------|--------|---------:|
| `quorum-core/src/agents.rs` | extend `touch` to also UPDATE active claims `SET expires_at = now + DEFAULT_LEASE_TTL_SECS WHERE holder=agent AND active=1`; new tests | +50 |
| `quorum-core/src/sync.rs` | call `agents::touch` inside `tick`'s cursor-advance txn; existing tests should still pass (cursor-pin test stays the same; new tests pin tick auto-renews) | +20 |
| `quorum-core/src/tasks.rs` | drop the `task_renew` core helper (`renew` for tasks) — search-and-replace | -~40 |
| `quorum/src/cli.rs` | remove `TaskRenew` variant | -~12 |
| `quorum/src/main.rs` | remove `TaskRenew` dispatch + command_source | -~15 |
| `quorum/src/cheatsheet.rs` | remove `task-renew` row; mention auto-renew in TASKS section | ~5 |
| `quorum/tests/cli_tasks.rs` | drop `task-renew` tests; add an integration test exercising the auto-renew loop (claim → multiple `--agent` commands over a span longer than TTL → task still claimed) | +30 / -~40 |

**Net: ~+50 LoC, balanced by ~-100 LoC of deletions.**

### Open questions for owner LGTM

1. **Generic `renew` — keep or remove?** Issue says "owner's lean: keep ONE manual
   override; remove if auto-renew fully covers it." My lean: **remove**. With auto-renew
   on every write, the only scenario for manual `renew` is "I'm about to be silent for a
   while and want to extend my lease ahead of time" — which is exactly the same as
   "claim", "touch", or any other write command (all auto-renew now). If owner sees a
   case I'm missing, keep it; otherwise drop both `renew` and `task-renew` in this PR.
   **Tentative: drop both** unless owner says keep `renew`.

2. **Should `sync::tick` auto-renew?** My lean: **yes**. The whole point of `sync` is the
   agent's one-call loop entry; if `tick` doesn't touch, an agent that only calls `sync`
   (the design goal!) wouldn't auto-renew its task. Need the touch. This adds a presence
   bump to `tick` (currently the design says "one side effect only: cursor advance"). The
   touch + cursor advance share the same `begin_immediate` transaction, so it's still one
   write txn — just two row updates inside it. I think this is consistent with the
   design's spirit (the "one side effect" was message-cursor; presence touch is the
   substrate every other write does). **Tentative: yes, touch in tick.**

3. **TTL constant location.** `agents::touch` is in core; the auto-renew TTL should be
   `tasks::DEFAULT_LEASE_TTL_SECS` (core const, source of truth post #32). The cli's
   `Config.task_lease_ttl_secs` mirrors it. The auto-renew uses the core const directly
   — no config plumbing through `touch`. **Tentative: hard-code on the core const.**

### Tests

Critical (block ship):
- **race canary stays green** — 12x stress on `n_processes_exactly_one_winner` + the
  cli_tasks concurrent canaries. Auto-renew touches lease rows on every write; a regression
  to the atomic-claim invariant would surface here first.
- **`auto_renew_extends_active_leases`** — claim, advance now past original TTL via the
  agent doing other work, verify lease is still active.
- **`auto_renew_does_not_resurrect_lapsed_lease`** — claim, agent goes silent past TTL,
  reaper returns task to open, agent comes back: their touch must NOT re-extend the now-
  inactive lease (active=0). The `WHERE active=1` clause handles this; pin with a test.
- **`auto_renew_only_touches_own_leases`** — A holds claim on `pr#1`, B's touch must not
  extend A's lease.
- **`sync_tick_auto_renews`** — claim task, repeatedly call `tick` past TTL, lease stays
  active. (Integration via CLI in `tests/cli_sync.rs`.)
- **`task_renew_command_removed`** — `quorum task-renew --agent A --task-id 1` returns
  exit 2 (unknown subcommand) — pins the deletion.

## Coordination

- #56 (Phase 3 of sync) currently in review with @Quasar-7nP3 — no overlap with this PR
  (different files except `sync.rs`, and the change there is purely additive — one new
  `agents::touch` call in tick's cursor-advance txn).
- Once #55 ships, the agent loop is genuinely one-call: `sync → claim → done → sync` — no
  more manual `renew` in the dogfood.

## Owner / CTO check-in points

- [ ] **LGTM on the plan** + answers to the 3 open questions.
- [ ] Confirm: removing both `renew` and `task-renew` (vs keeping generic `renew`).

— Quartzite-7n3
