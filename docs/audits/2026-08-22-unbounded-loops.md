# Unbounded loop audit — `quorum serve`

**Date:** 2026-08-22
**Trigger:** The daemon looped forever re-provisioning a reviewer for task #80 (PR #691) with
`tick error: usage: generated review task is not in the current active graph plan`. Root cause:
the graph had been `blocked` by a failed sibling (#81), the in-review selection filter only
checks `status == "in-review"`, and the authority-validation failure arm in
`provision_reviewer_reserved` returned `Err` without recording a provision strike. That bug is
fixed separately; this document records the rest of the class.

**Rule this audit enforces:** every retry / re-dispatch / re-provision / re-spawn path in the
daemon must be bounded by a **durable** count — even an arbitrary fixed one — after which the
task is parked or failed loudly.

**Method:** the `mod.rs` tick loop (`tick_loop`, `tick`) and `provision_reviewer_reserved` were
read end-to-end; parallel readers covered the provider-spawn layer, `quorum-core` attempt tables,
and `serve/` submodules. Every P0 was independently re-read at the cited lines. Line numbers are
as of `8a0e6bc` (main, 2026-08-22) and will drift.

Findings are **Verified** (code path read end-to-end) unless marked **Hypothesis**.

---

## 0. The amplifier

`tick()`'s only throttle is `tokio::time::sleep(500ms)` at `mod.rs:14334` — the *last*
statement of the function. `tick_loop` (`mod.rs:8812–9163`) has no sleep or yield of its own.
`classify_tick_error` (`mod.rs:6733-6750`) maps `Io | Db | Busy | Usage | BadInput | NotHolder`
to `Continue`, whose body is only `log(...)` (`mod.rs:9158`).

**Any `Err` out of `tick()` skips the sleep and re-enters immediately.** Every "returns `Err`
without a strike" finding below is therefore a full-CPU hot spin re-performing the preceding
action (git worktree add, `gh` call, provider spawn) as fast as the machine allows, while every
later phase of the tick is starved. `log()` writes to stderr without rotation.

Five calls abort the whole tick from the top level: `tick_decomposition` (`9238`, the first
statement), `reconcile_merged_continuations` (`9243`), `reconcile_merge_retries` (`13329`),
`reconcile_ci_remediations` (`13916`), `reconcile_remediation_retries` (`13925`).

**Remediation:** move the sleep to the top of the `tick_loop` body (or also sleep in the
`Continue` arm), add capped exponential backoff on consecutive tick errors, log only on
transitions.

---

## P0 — unbounded loop with process / LLM / worktree churn, or daemon wedge

### P0-1. `cleanup.rs:110-146` — tombstone retirement can brick the daemon at boot

`retire_settled_tombstones` selects `branch-delete` rows with `state='done'` (`:118`) and for
each calls `wt.retire_cleanup_tombstone` → `git ls-remote` + `git push --force-with-lease :<ref>`
(`worktree.rs:1564-1606`). The row is deleted only after success (`:140`). These rows never
pass through `claim_next`, so `attempts` is never incremented — no strike, no `errors` row, no
log before the `?` at `:132`.

Triggers: token expiry, remote SHA mismatch (`worktree.rs:1584`, permanent), lease rejection,
`deny_unknown_fields` drift on `BranchDeleteRef` (`cleanup.rs:57`, `:123`).

Blast radius: `Err` → `drain_tick` → `mod.rs:9052` `?` → `tick_loop` returns → exit 3 →
`serve-supervisor.sh:130-132` propagates the non-75 code and stops supervising. On restart,
`cleanup::startup` (`mod.rs:8602`) runs this first, before recovery and before the tick loop.
The daemon cannot boot and the row cannot be cleared except by a successful retirement.

**Fix:** per-row fault isolation (`continue` instead of `?` at `:124`/`:132`); durable
pre-attempt debit in a short `BEGIN IMMEDIATE`; move to `state='retire-exhausted'` at 3 and
exclude it from `:118`.

### P0-2. `provision_reviewer_reserved` — strike-less failure arms

Only two of ~12 exits record a strike: `mod.rs:17146-17161` (worktree family) and
`17519-17534` (run persistence). These do not:

| line | failure | already burned | strike |
|---|---|---|---|
| 17708-17732 | provider CLI spawn failed → `Ok(Failed)` | worktree, fetch, branch, capability, journal | no |
| 17428-17443 | pid-journal upsert failed **after spawn** → `Err` | LLM process spawned then killed | no |
| 17242-17254 | journal upsert failed | worktree | no |
| 17287-17318 | `prepare_reviewer_authority` (the #80 bug) | worktree | no |
| 17324-17330 / 17366-17372 | `load_review_cycle_context` / `load_task_review_contract` `??` | worktree + capability; **no cleanup call at all** — leaks reviewer name, worktree, branch, journal row, live `run_capabilities` row per tick | no |
| 16900 / 16910 / 16950 | PR-target resolve/validate | a live `gh` call per tick | no |

Neither backstop applies. `MAX_REVIEWER_PROVISION_STRIKES=3` reads
`reviewer_provision_attempts`, which never moves. `MAX_TOTAL_REVIEWER_RUNS=12` counts
`agent_runs` rows (`provision_attempts.rs:101-108`), but the reviewer `agent_runs` insert is at
`mod.rs:17469`, after spawn and pid-journal succeed — every arm above is upstream of it.

**Fix:** one `record_provision_strike(...)` helper called on every non-`Unavailable` exit —
ideally a guard struct that records unless success is explicitly settled, so a new arm cannot
omit it. Give `17324`/`17366` the `cleanup_failed_reviewer_provision` treatment.

### P0-3. `mod.rs:17554-17614` — `ReviewerAttached` arm clears strikes, then fails

`provision_attempts::clear` runs at `:17557-17563`, then `fire_event_result(ReviewerAttached)`
at `:17565`. If the transition is rejected, the arm kills the process and returns `Err` at
`:17612` with the counter freshly zeroed. A persistently rejected attachment re-provisions a
full reviewer (worktree + LLM process) every tick while erasing the evidence. Incidentally capped
at 12 by `MAX_TOTAL_REVIEWER_RUNS` (the run row exists by then), not 3.

### P0-4. `mod.rs:14311-14330` — doctor spawns a real Claude ~2×/s and orphans each one

Both failure arms — `spawn_doctor` failed (`:14327`) and `slot.proc.feed_turn` failed
(`:14320`) — log and drop the slot without inserting into `doctored_tasks` (contrast
`:14246`/`:14251`). The candidate selector (`:14267-14300`) returns the first matching stalled
task every tick, so it re-spawns `claude` (`doctor.rs:114`) every 500 ms forever. No counter,
durable or in-memory.

`AgentProc` has no `impl Drop` and children are spawned with `setpgid`, so the `feed_turn` arm
orphans a process group and leaves an unreaped child per iteration. Gated by `doctor_enabled`.

**Fix:** `doctored_tasks.insert(task_id)` in both arms + `kill_and_reap` on the feed arm;
better, a durable capped `refs.$.doctor_attempts` mirroring `record_ci_remediation_attempt`.

### P0-5. `mod.rs:7610` + `20213 / 20253 / 20293` — CI remediation counter bypassed

`reconcile_ci_remediations` records a strike only on `RemediationSpawnOutcome::ProvisionFailed`
(`:7613 → 7626-7635`). `spawn_remediation_worker(...).await?` at `:7610` propagates `Err` from
three arms — PR-target resolution (`20253`), claim revalidation DB failure (`20293`),
claim-guard failure (`20213`). Each tick: re-claim, live `gh` lookup, fail, release lease, abort
the rest of the tick, with `ci_remediation_attempts` still at 0.
`MAX_CI_REMEDIATION_PROVISION_STRIKES=3` is unreachable.

**Fix:** return `Ok(ProvisionFailed { cause })` from those three arms.

### P0-6. `mod.rs:7757` — owner-requested remediation retry has no counter

`reconcile_remediation_retries` selects on the boolean `PARKED_REWORK_RETRY_REF` and hits the
same three `Err` arms. The marker is cleared only by a successful `ReworkPushed`/`SignaledDone`
(`tasks.rs:2593-2607`), so a retry that keeps hitting an `Err` arm loops forever with a worktree
provision + `gh` call per tick.

### P0-7. `mod.rs:5911 / 6560 / 6655` — provider spawned, journal write fails, no attempt recorded

Arbiter (`spawn_arbiter`, `:5876`), planner (`:6523`), and decomposition classifier (`:6629`)
each have a `journal_decomposition_process` failure arm that does `kill_and_reap` /
`reap_classifier_with_usage` then `return Err` with no `record_decomposition_attempt` — while
the sibling arm in the same `match` (`:5921`, `:6568`, `:6667`) records correctly. The graph
stays in `validating`/`planning`/`preclassifying`, so the next tick re-materializes a frozen
planner view (repo archive, `:5861`/`:6508`) and re-spawns the provider. `tick_decomposition` is
the first statement of `tick()`, so there is no sleep between iterations. Same shape at
`mod.rs:6457` (`repository_head_sha(...)?` in the draining transition).

`mod.rs:6075 / 6176 / 6216`: `delete_decomposition_process(...)?` sits before the `match` that
writes the strike, so a journal-delete failure discards a real `ProviderFailed` verdict
unrecorded. **Fix:** `let _ = ...`.

### P0-8. `mod.rs:17148-17160` and `17521-17533` — the strike recorder fails open

Both strike sites are `db::open(&p).ok().and_then(|c| record_attempt(...).ok()).unwrap_or(0)`.
A failed open or write collapses to `0`, which never satisfies `strikes >= MAX`, and logs
`"provision strike 0/3"` — reading as success. A persistently failing counter write means
unbounded re-provisioning presented in the log as bounded. Fail-open on the safety counter.

**Fix:** on a failed `record_attempt`, log `FATAL` and treat as exhausted.

### P0-9. `agent.rs:605`, `codex_agent.rs:606`, `grok_agent.rs:1287` — `kill_and_reap` drains with no time bound

`while let Ok(Some(line)) = self.read_raw_line(None).await { terminal.push(...) }` after
`killpg` + `wait`. If a descendant escaped the process group and still holds the write end (the
race documented at `agent.rs:1344-1348`), this never returns; Claude/Codex also accumulate into
an unbounded `Vec`. `kill_and_reap` is awaited inline in the tick from every teardown path
(`mod.rs:15923`, `17497`, `17615`), so a hung drain stalls the daemon. The correct pattern is
`runner.rs:803-866`: `EXIT_EVIDENCE_LINES=256` + `EXIT_EVIDENCE_TIMEOUT=2s` + truncation marker.

### P0-10. `agent_endpoint.rs:347` — accept-error hot spin

`Err(_) => super::log("agent endpoint accept failed")` with no backoff and no counter. Under
`EMFILE`/`ENFILE`, `accept()` returns instantly, so the `select!` loop spins at 100% CPU. The
`connections.len() < MAX_CONNECTIONS` guard does not help — `connections` stays empty.

### P0-11. `mod.rs:4645-4648` + `planner.rs:377-384` — undecodable stored proposal wedges the daemon

`rehydrate_accepted_proposal` is a bare `serde_json::from_str::<Vec<ProposedTask>>`, and
`ProposedTask` carries `#[serde(deny_unknown_fields)]` with six required non-`default` fields
(`planner.rs:111-131`). `load_planning_snapshot` propagates the error, and `tick_decomposition`
is the first statement of `tick()` — a rolled-back daemon meeting a newer row, or any future
field added without `#[serde(default)]`, hot-spins the daemon with zero `decomposition_attempts`
and nothing parked. The compat-reset at `:6370-6395` only rescues proposals that deserialize and
fail validation. Same shape at `:6364-6369`, `:6580-6583`, `:6613-6616`.

---

## P1 — bounded only in memory, or aborts a whole tick

- **`PoisonTracker` is a bare `HashMap`** (`mod.rs:300-302`, built at `:8580`).
  `MAX_POISON_STRIKES=3` gates worker worktree provisioning (`:18145`), push lockout
  (`:18164`), and agent spawn (`:18511`). Below the cap it calls `release_task` (`:18527`) via
  `tasks::update`, not `apply_event`, so `tasks.recovery_attempts` is never debited. Every restart
  or self-update refills the budget. `provision_attempts.rs` fixed exactly this for reviewers;
  workers never got it.
- **Classifier retry has no cap** (`mod.rs:6763-6782`, `8589-8590`). `classifier_consec_errors`
  is function-local; backoff is `min(30·2^min(n-1,4), 300)` with no maximum. A permanently failing
  classifier spawns a provider every ≤300 s forever. `mod.rs:14063` takes exactly one task ordered
  `priority DESC, id`, so one unclassifiable head-of-queue task starves all classification.
- **`error_turn_count` (`MAX_ERROR_RETRIES=3`) is a `SlotState` field** (`mod.rs:3603`), reset at
  every construction site and at `recovery.rs:592`. Live-process exhaustion fires `AgentFailed`
  (durable), but the dormant-recovery path preserves tasks and never consumes
  `recovery_attempts` — a fresh 3-error budget every boot. The journal already persists
  `rework_count`/`cost_tokens`; add `error_turn_count`.
- **Whole-tick starvation from per-slot work:** `actionable_slot_breach(...).await?` inside
  `for r in reviewers` (`12429`) and `for w in workers` (`12541`);
  `resume_reviewer_after_ci(...).await?` (`12419`, entry retained in `pending_reviewer_resumes`
  → infinite); `recover_late_worker_done_with_publication(...).await?` (`12386`, after a push /
  `gh pr create` already ran); `poll_resume_reviewer_pre_review_checks(...)?` (`13431`/`13742`);
  `handle_pre_review_checks_failure(...)?` (`13600`/`13758`); `spawn_worker(...)?` (`13942`).
- **Undisposable slots retained forever with unbounded durable row growth.**
  `dispose_managed_process_exit` returns `None` when `cap_run_id` is absent (`mod.rs:18810-18820`)
  — permanent if capability issuance failed at spawn (`:18274` only logs). Callers `continue` and
  re-insert the slot (`:12444-12450`, `:12555-12560`, `:12784-12790`), so it occupies a worker
  slot forever and writes `persist_lifecycle_diagnostic` every tick — 4 rows including a
  `task_notes` row documented as having no TTL (`errlog.rs:76-88`) plus an owner alert. ~170k
  permanent rows/day at 2 Hz.
- **`worktree.rs:354-456` / `mod.rs:9076-9089` — one poisoned publication intent starves the
  sweep.** `reconcile_publication_sources` aborts the batch on the first sub-failure (`:433`,
  "names unavailable commit", permanent). Only the `Ok` arm advances
  `publication_ref_reconcile_cursor` (`mod.rs:9085`), so the same 64-row page re-runs every 60 s
  and lower-id pages are never reconciled again. Correctness risk.
- **`approvals.rs:433-440` — startup CI-not-ready returns `Ok(false)` with no durable state**
  after a serial 900 s `gh` poll. No `drop_approval`, marker, or park, so `replay_deferred`
  (`:102-111`) re-enters the poll on every boot, serially, blocking startup N × 900 s. The
  tick-loop counterpart (`mod.rs:8301-8309`) parks; the startup arm diverges.
- **`approvals.rs:40-259` — one `?` skips replay for every remaining PR.** Caller
  (`mod.rs:8676-8687`) swallows it; replay is startup-only. One `SQLITE_BUSY` on PR #1 means
  #2..#N are never replayed and `recovery::recover` demotes them to in-review for a full re-review.
- **`merge.rs` — 13 `gh` subprocesses with no timeout, output cap, or stdin redirect**
  (`:486, 520, 536, 578, 594, 614, 633, 927, 940, 962, 988, 1016, 1030`). An auth prompt blocks
  forever; `merge()` runs under `spawn_blocking` (`mod.rs:11319`), so a hang leaks a blocking-pool
  thread permanently. `collector.rs:1236` and `worktree.rs:88-188` show the bounded pattern.
- **`doctor.rs:124-153` — `drain_doctor_events` has no wall clock, line cap, or byte cap.**
  Contrast `classifier.rs:174-192`.
- **`cleanup.rs:177` — char/byte truncation mismatch.** `error.chars().take(1024)` (up to 4096
  bytes) vs `decomposition_cleanup.rs:130`'s 2048-byte limit. Non-ASCII git output → `Usage` →
  `??` at `:189` → exit 3, supervisor stops. Bounded via `requeue_interrupted`, at the cost of an
  outage per step.
- **`merged_continuation.rs:107-137`** — `adopt_recovery_delivery` / `persist_pending_candidate`
  / `advance_cursor` all `?` before the `through_seq` ack (`:135-138`), so the page replays every
  tick; on `Tick` it lands in `mod.rs:9243`'s `?`. Deterministic `Err` at
  `decomposition.rs:2377-2380`.
- **Decomposition attempts are recorded only after a terminal outcome, never before dispatch.**
  In-flight state lives in `coordinator.arbiter_slot` (`mod.rs:4445`), in-memory. Any crash or
  self-update restart (exit 75 is auto-restarted) re-spawns with `provider_failures` at 0.

---

## P2 — bounded, but not loud

- `cleanup.rs:169-189` — exhaustion is silent. `decomposition_cleanup::fail` returns
  `FailureOutcome::Exhausted`; `drain_batch` discards it. An exhausted `process` intent leaves a
  live unsupervised agent process never killed, with no operator-visible signal.
- `mod.rs:17167` / `17540` log `"REVIEWER PROVISION EXHAUSTED: parking task…"` but do not park;
  the park happens a tick later via `decide_provision → Exhausted` (`:13468`/`:13784`).
- Per-head strike reset re-arms the budget indefinitely (`provision_attempts.rs:35-38`,
  `mod.rs:17559`). Missing: a per-`(task, PR)` lifetime ceiling independent of SHA.
- `PRE_REVIEW_CHECKS_TIMEOUT_ALERT_AFTER=3` only alerts (`mod.rs:334`, `6914-6929`); counters are
  in-memory.
- `poison_task` parks without a feed alert (`mod.rs:18548-18561`).
- `merged_continuation.rs:265` — `filled_capacity` is `true` when `limit == 0`; guarded only by
  a `debug_assert!`.
- `merged_continuation.rs:5-7` — the "bounded durable retry marker" is an `events` row that
  expires after 24 h with no count, log, or park.
- `worktree.rs:1106-1152` (`gc_orphaned`) — retried every boot forever, no alert.
- `collector.rs:1277-1334` — unbounded response text from unbounded `next_raw_line()`;
  `collector.rs:904` discards its own `record_failure` result.
- `agent.rs:496-526` / `codex_agent.rs:534-564` — `read_raw_line(None)` has no byte ceiling.
  Grok has `STDOUT_LINE_BYTES = 1 MiB`.
- `RETRY_REF`'s `PendingTurn` carries no attempt count (`runner_state.rs:24-34`).
- `recovery.rs:659-687` — `remove_journaled_decomposition_views` filters `planner|classifier`;
  the arbiter journals under `role="arbiter"`, leaking one `quorum-planner-*` tree per crash.
- `mod.rs:2910-2913` claims a startup rebuild of missing collector jobs that does not exist; both
  merge sites fire the collector before enqueuing (`:10154`, `:11421`).
- `mod.rs:11010-11076` (master-CI gate) and `11349-11393` (policy retry) are bounded but block
  inside `tick()` for up to `master_ci_timeout_secs` / 3 × `merge_checks_timeout_secs`.
- `agent_mcp.rs:603-614` (agent sidecar) — `inventory()` failure → `continue` forever.

---

## Refuted / verified clean

- Reviewer recovery bypassing the exhaustion gate — refuted. `recover_interrupted=true` is passed
  only from Phase 5b (`mod.rs:13888`), downstream of `decide_provision → is_provision_exhausted`.
- `recovery.rs` Phase 4 is bounded by durable `tasks.recovery_attempts` /
  `MAX_RECOVERY_ATTEMPTS=3` (`tasks.rs:2385-2397`) with a loud park. Its fatal `Err(invalid(...))`
  arms are a deliberate global park.
- `merge.rs:891-918` and `1141-1150` check loops are deadline-bounded;
  `approvals::begin_approved_merge_attempt` is a correct one-shot fail-closed marker.
- `reviewer.rs`, `arbiter.rs`/`classifier.rs`/`planner.rs` transports, `session_log.rs`,
  `rereview_builder.rs`, `review_cycle_context.rs`, `review_ledger.rs`: no findings.

**Reference patterns to copy:** `review_interpret_jobs` (`mod.rs:14105-14226`) reserves the
durable attempt via `mark_error` before `spawn_detached`, caps at 5, excludes at-cap rows from
`list_ready`, and dead-letters exactly once. `decomposition_cleanup::claim_next`
(`decomposition_cleanup.rs:95-116`) increments `attempts` inside the leasing transaction, so a
crash mid-attempt still counts.

---

## Cross-cutting guards

1. **Generic durable attempt table + helper every failure arm must call.**
   `attempts(subject_kind, subject_id, action, generation, attempts, last_error, last_at)` with
   `record(...) -> i64` and `clear(...)`. Five bespoke tables exist today, each wired by hand at
   the arms someone remembered.
2. **Debit before the action, clear on success.** Every counter except `decomposition_cleanup`
   and `review_interpret_jobs` records after the failure in a separate transaction, so a crash or
   self-update restart between attempt and strike loses it. Pre-attempt reservation is crash-safe
   and removes the P0-8 fail-open class.
3. **Structural, not conventional.** No file under `serve/` except `mod.rs` records an attempt.
   Have expensive-action submodules take an `&dyn AttemptRecorder` so a call site cannot dispatch
   without consuming budget.
4. **Tick-level circuit breaker.** Key on `(task_id, phase, error-class)`; N identical outcomes in
   a row → park and skip. Catches every arm nobody instrumented.
5. **Never `?` per-task work out of `tick()`.** Capture per item, `continue`, aggregate. Kills the
   starvation class and the associated hot spin.
6. **Move the throttle** to the top of `tick_loop` so no error path skips it; capped backoff on
   consecutive tick errors.
7. **Global per-task LLM-spawn budget.** None exists; `MAX_TOTAL_REVIEWER_RUNS=12` covers
   reviewers only and is upstream-blind. Nothing caps workers, classifiers, doctors, planners,
   arbiters, or collectors per task.
8. **Loud, uniform exhaustion.** One `park_and_alert(task_id, action, cause)` for every cap.
9. **Bound the diagnostics.** Dedupe `persist_lifecycle_diagnostic` on `(task_id, event, error)`
   within a window.

## Sequencing

1. Fix PR for the #80 loop (authority strike, selection skip, no tick-abort, sleep move, fail-safe
   recorder).
2. Recover task state; restart daemon.
3. Guards 1, 2, 4, 5 together — they retire most P0/P1 structurally.
4. Standalone: P0-1 (boot wedge), P0-4 (doctor), P0-9 (reap timeout), P0-11 (proposal decode).
