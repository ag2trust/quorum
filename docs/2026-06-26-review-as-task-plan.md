# quorum#10 — `feat: review-as-task` plan

**Author:** Linoleum-8wK (claude-opus-4-7) · **Date:** 2026-06-26
**Issue:** [ag2trust/quorum#10](https://github.com/ag2trust/customer-api/issues/10) (priority:medium, model:4.7)
**Depends on (all merged):** #9 (statuses) · #1 (claim) · #2 (deps) · #3 (notes) · #6 (events) · #8 in-flight (sync — not a code dep)
**Source-of-truth design hooks:** [issue body](../) · `quorum-core/src/tasks.rs` (today's task surface) · CLAUDE.md (project invariants)

Status: **DRAFT — awaiting CTO/owner LGTM before any code commits.**

---

## 0. North-star recap

**Mandatory automatic review.** Every non-review task that reaches `done` auto-spawns a
high-priority review task. The reviewer's verdict drives the executor's task to terminal
`closed` (approve) or back to `open` + `rework` (changes). Mechanism, not policy:
"another agent checks the work" and "no self-merge" become impossible to skip because they
ride on top of `task-claim`/`task-update`.

Quorum stays generic — it doesn't know what the work-type-specific "final action" is
(merge a PR, approve a doc, ship a config). The reviewer performs it externally, then
marks the review task `done`; quorum only records that. PRs and code review stay on
GitHub; quorum coordinates the **handoff**.

---

## 1. Scope (verbatim from acceptance criteria)

- [ ] `task-update --status done` on a non-review task auto-creates a high-priority review
  task (refs the task, records `orig`)
- [ ] review tasks do not auto-spawn reviews (no recursion)
- [ ] `orig` agent cannot claim its own review task
- [ ] review `approve` → original task `closed`; review `changes` → original task `open` +
  `rework` + action items attached
- [ ] reopened task is claimable only by `orig` until `sticky_until`, then by anyone
- [ ] spawn / verdict / reopen emit events (#6); action items stored as notes (#3)
- [ ] race canary green; `help`/README updated

### Out of scope (v1, noted in issue):
- `revision:N` counter (re-review escalation) — future option, not v1
- force-reassign of a sticky-reopened task — not in v1; sticky window is fixed 30m
- review-of-review (recursion) — explicitly excluded; review tasks never auto-spawn

---

## 2. Storage design

### 2.1 Schema migration (v5 → v6)

Two additive columns on `tasks` (idempotent ALTER, matches existing v3→v4 / v4→v5 pattern
in `db.rs::migrate`):

```sql
ALTER TABLE tasks ADD COLUMN sticky_until INTEGER;   -- NULL = no sticky window
ALTER TABLE tasks ADD COLUMN orig         TEXT;      -- review tasks only: the original assignee
```

- `sticky_until INTEGER NULL` — unix-ts; while `> now`, only `assignee` may claim. Set on a
  `changes`-driven reopen, cleared by `task-release`/`task-cancel`/successful re-claim.
- `orig TEXT NULL` — the executor of the task being reviewed. Set on auto-spawn (review tasks
  only). Pre-existing rows: NULL — neither column existed in v5, so no historic data drift.

**Why columns, not refs JSON:**
- `sticky_until` is filtered in the `task-claim` SQL hot path (every claim). A JSON extract per
  row in the claim selector is slower and uglier than a bare `AND` on a column.
- `orig` likewise gates self-review on every claim of a `kind:review`-labeled task. Bare TEXT
  beats `json_extract(refs, '$.orig')`.
- Symmetry with the `depends_on` (#2) column choice — same reasoning: hot-path predicates live
  on columns, not in JSON.

**No FKs** per invariant (v1 schema has none).
**No index on `sticky_until`** — it's a per-row predicate on already-filtered
`status='open'` claims; the existing `tasks_status_priority` index narrows the scan first.

### 2.2 Label conventions (no schema change)

- Review tasks carry `"kind:review"` in their `labels` JSON.
- Reopened tasks gain `"rework"` (additive — preserves the original labels).
- Both compose with the existing `--match-label` AND filter — agents can pull/skip reviews
  by label without new flags.

### 2.3 Refs conventions

Review tasks store `refs = {"review_of": <T-id>, "pr": "<N>"|null}`. `review_of` lets the
verdict path find T cheaply (no scan). `pr` is a human breadcrumb; nothing branches on it.

---

## 3. New invariants (added to CLAUDE.md §"Load-bearing invariants")

These mirror the style of existing #9-era invariants — concise, "why" first.

11. **Auto-spawn is part of `done`'s transaction, not a follow-up call.** The review-task
    `INSERT` runs inside the same `BEGIN IMMEDIATE` txn as the `UPDATE … SET status='done'`.
    A crash between `done` and `spawn` would leave a task in `done` with no review forever
    (acceptance: "submitted = guaranteed reviewed eventually"). Atomic > Simple.
12. **Reviews never auto-spawn reviews.** The auto-spawn site checks the source task's labels
    for `"kind:review"` and skips. Mechanism, not "the reviewer just won't do it" — the spec
    is explicit ("no recursion").
13. **Self-review is rejected at the claim, not the update.** `task-claim` filters out review
    tasks whose `orig` equals the caller. A reject-at-update would let an `orig` claim and
    hold the lease (denying everyone else) until they noticed. Reject at the gate.
14. **Sticky reopen is an eligibility filter, not a priority bump.** The reopened task
    keeps its priority; only the `assignee`-eligibility narrows for `now < sticky_until`.
    A sticky-reopened high-priority task still beats a non-sticky low-priority one in the
    queue (acceptance: "Eligibility only — does not change priority").

---

## 4. Code surface

### 4.1 `quorum-core/src/tasks.rs` — extensions (no new module)

The review-as-task semantics live in `tasks.rs` because they are *transitions of a task*,
not a separate domain. Splitting to `review.rs` would tear the `done`-spawn-in-same-txn
invariant across modules without isolating anything meaningful.

Three new public items:

```rust
pub const REVIEW_LABEL: &str = "kind:review";
pub const REWORK_LABEL: &str = "rework";
pub const REVIEW_PRIORITY: i64 = 1000;          // beats normal high (9 in tests, ~100 in practice)
pub const STICKY_WINDOW_SECS: i64 = 1800;       // 30 min default per issue
```

Modified `pub fn update(...)` — three behavioral additions, no signature change:

1. **Auto-spawn on `done`** (immediately after the existing `task_done` event emission):
   - Skip if `task.labels` contains `"kind:review"` (invariant 12).
   - Insert a new task: title `"review: <T.title>"`, status `open`, priority `REVIEW_PRIORITY`,
     labels `["kind:review"]`, assignee NULL, created_by `<assignee-who-just-finished>`,
     refs `{"review_of": <T-id>}` (+ inherit `T.refs.pr` if present),
     `orig = <assignee>`, `depends_on` NULL (review unblocks immediately, not gated by T).
   - Emit a `review_spawned` event with `subject=task#<R-id>`, body `"for task#<T> by <orig>"`.

2. **New verdict path — only on review tasks moving to `done`:** extend `TaskUpdate` with
   `pub verdict: Option<&'a str>` (values: `"approve"` | `"changes"`; any other → exit 2).
   - Rejected as a usage error if the task is NOT `kind:review`.
   - Rejected if the status isn't `done` (verdict only valid on the review's submission).
   - On `approve` (still inside the same `BEGIN IMMEDIATE`):
     - `R.status = 'done'`, lease dropped (existing path).
     - `UPDATE tasks SET status='closed', updated_at=?now WHERE id=<T-from-refs.review_of>`.
     - Emit `task_closed` event on `task#<T>` ("approved by <reviewer>").
   - On `changes`:
     - `R.status = 'done'`, lease dropped (existing path).
     - `UPDATE tasks SET status='open', assignee=<orig-from-R>, sticky_until=?now+W,
        labels=<labels with "rework" appended>, updated_at=?now WHERE id=<T>`.
     - Emit `task_reopened` event on `task#<T>` ("changes requested by <reviewer>").

3. **No change to non-`done` updates** — refs/body still work, and the `"only done is settable"`
   guard for non-review tasks stays exactly as it is.

Modified `pub fn claim(...)` — two new AND clauses in the selector (both the explicit
`--task-id` and auto-pick paths):

```sql
-- Self-review block (invariant 13)
AND (labels IS NULL OR labels NOT LIKE '%"kind:review"%' OR orig != ?agent)
-- Sticky reopen (invariant 14)
AND (sticky_until IS NULL OR sticky_until <= ?now OR assignee = ?agent)
```

Both clauses are pure-narrowing — they compose cleanly with the existing `depends_on`-ready
clause and `match_label` filter. The explicit `--task-id` form also gets them; an `orig` trying
to claim its own review by id gets a clean exit-1 "didn't get it" (the dominant lost-race shape
in quorum) — not an error.

Modified `pub fn release(...)` — clear `sticky_until` to NULL when transitioning to `open`
(the holder released; sticky no longer makes sense — they're gone). Cancel similarly clears.

### 4.2 `quorum/src/cli.rs` — flag addition

`TaskUpdate` subcommand gets one new optional flag:

```rust
/// Reviewer's verdict on a review task being marked `done`. Required on review tasks,
/// forbidden on non-review. One of: approve, changes.
#[arg(long)]
verdict: Option<String>,
```

`main.rs` wires it through into `TaskUpdate.verdict`. Quorum validates the value (reject any
other string with a usage error, exit 2).

### 4.3 `quorum/src/cheatsheet.rs` + `README.md`

Add a short "Review workflow" section to the agent cheat-sheet and the README. Two-paragraph
addition; no behavioral docs duplication.

### 4.4 `quorum-core/src/db.rs` — v6 migration

```rust
pub const SCHEMA_VERSION: i64 = 6;
// inside migrate():
if current < 6 && !column_exists(conn, "tasks", "sticky_until")? {
    conn.execute("ALTER TABLE tasks ADD COLUMN sticky_until INTEGER", [])?;
}
if current < 6 && !column_exists(conn, "tasks", "orig")? {
    conn.execute("ALTER TABLE tasks ADD COLUMN orig TEXT", [])?;
}
```

Plus a `migrates_v5_to_v6_adds_review_columns` test mirroring the v4→v5 test pattern.

---

## 5. Tests (additions on top of the existing suite)

Unit tests in `quorum-core/src/tasks.rs` (~14):

- `done_auto_spawns_review_with_orig_and_refs`
- `done_on_review_task_does_not_recurse_spawn`
- `auto_spawned_review_carries_kind_review_label_and_high_priority`
- `auto_spawned_review_inherits_pr_ref_when_present`
- `orig_cannot_claim_own_review_via_auto_pick`
- `orig_cannot_claim_own_review_via_explicit_task_id`
- `non_orig_can_claim_review_freely`
- `approve_verdict_closes_original_task_atomically`
- `changes_verdict_reopens_original_with_rework_label_and_sticky`
- `verdict_required_on_review_done`
- `verdict_forbidden_on_non_review_done`
- `verdict_invalid_value_is_usage_error`
- `sticky_window_only_orig_may_claim_until_expiry`
- `sticky_window_clears_after_expiry_anyone_may_claim`
- `release_of_reopened_task_clears_sticky`

CLI tests in `quorum/tests/cli_tasks.rs` (~3 — surface coverage only):

- `task_update_with_verdict_approve_closes_chain`
- `task_update_with_verdict_changes_reopens_with_rework`
- `task_update_verdict_on_non_review_exits_2`

Migration test in `db.rs` (~1):

- `migrates_v5_to_v6_adds_review_columns_without_disturbing_existing_rows`

Race canary: the existing N-process claim canary in `quorum/tests/race.rs` covers the
self-review filter implicitly (it races a single `--target`; the new AND clauses don't
weaken atomicity). I'll add **one** explicit race test in the same file:

- `race_orig_vs_others_on_review_task` — 12 processes (1 = orig, 11 = non-orig) race
  `task-claim` on one auto-spawned review task → orig loses (exit 1, clean), exactly one of
  the other 11 wins, `errors` count == 0 (matches existing canary assertion).

**Total:** ~18 new test cases. The existing 142 must remain green.

---

## 6. Phases

Single PR, but two **commit boundaries** so the reviewer can read it in passes:

1. **Schema + claim filter + tests** — v6 migration; `orig`/`sticky_until` columns; the two
   new AND clauses in `claim`; the migration test; the self-review unit tests; the sticky-claim
   unit tests. **Atomicity-only change** — no auto-spawn yet, no verdict.
2. **Auto-spawn + verdict + sticky reopen + tests + docs** — the `update` extensions, the CLI
   flag, the cheatsheet/README text, the rest of the unit tests, the CLI tests, the race test.

Both commits land on `feat/review-as-task-10` before opening the PR. The split keeps the
"storage shape correct" change reviewable apart from the "behavior change" — invariant-#9 was
landed this way too (#14 split the lease from the executor surface).

---

## 7. Verification evidence (will be pasted in PR body)

```bash
# Run from the worktree root.
cargo fmt --all -- --check                                                     # exit 0
cargo clippy --all-targets -- -D warnings                                      # exit 0
rtk proxy cargo test                                                           # paste full summary; 142 + ~18 new green
rtk proxy bash quorum/tests/race.sh                                            # explicit 12×100 race canary, errors==0
quorum init                                                                    # idempotent migrate from v5 to v6 on existing ~/.quorum/quorum.db
echo '{"pr": 50}' | quorum task-create --created-by boss --title "land PR 50" --refs-stdin
quorum task-claim --agent A --task-id 1
quorum task-update --agent A --task-id 1 --status done
quorum task-list --label kind:review                                           # review task spawned, orig=A
quorum task-claim --agent A --task-id 2                                        # exit 1 (self-review blocked)
quorum task-claim --agent B --task-id 2                                        # exit 0
quorum task-update --agent B --task-id 2 --status done --verdict changes
quorum task-get --task-id 1                                                    # status=open, assignee=A, sticky_until > now, labels has "rework"
quorum task-claim --agent C --task-id 1                                        # exit 1 (sticky)
quorum task-claim --agent A --task-id 1                                        # exit 0 (orig in sticky window)
```

Verification follows CLAUDE.md "Pre-PR-open author sanity check" (collect-only/build must
clean before PR) and "verification before completion" (paste full pytest/cargo summaries —
piped through `rtk proxy` so the reviewer sees true output, not RTK-compressed).

---

## 8. Open questions for CTO (Lozenge-q7F)

Asking **before** Phase 1 so course-correction is cheap:

1. **`REVIEW_PRIORITY = 1000` as a hardcoded constant, or surfaced in `config.toml`?**
   My lean: hardcoded for v1. Reviews-precede-work is the mechanism; making it tunable
   invites "let me lower it to clear my queue" anti-patterns. (Config can land later if a
   real need surfaces — forward-only.)

2. **`STICKY_WINDOW_SECS = 1800` (30 min) hardcoded or config?** Same lean: hardcoded for v1.
   Issue says "Default W = 30m" without "configurable."

3. **`orig` as a new column vs. `refs.orig` JSON.** Plan picks column (hot-path). LGTM the
   schema choice or push back if you'd rather keep all "review metadata" in `refs`.

4. **Auto-spawn carries the executor's `created_by` for the review task** (so a roster trace
   shows who triggered it). LGTM, or should `created_by = "system"` to make the auto-emit
   visible?

5. **`changes` verdict reassigns to `orig` via direct UPDATE** (the only `update()` path that
   sets `assignee` after a release). The existing comment on `TaskUpdate` says "no `assignee`
   reassignment via task-update under the lease model." The reopen path is **distinct** — it's
   a system-driven reopen, not an agent reassignment, and the sticky window protects from
   reaper races. I'll add a code comment citing this. Flag if you'd rather do
   release-then-implicit-reassign-via-sticky-when-A-claims (more steps, same outcome).

I'll start Phase 1 (schema + claim filter) the moment any of {`LGTM`, "go", "ship",
specific objections addressed} lands on the hub. Defaulting to my leans on 1–5 unless you
say otherwise.

---

**Plan ends. Sign:** Linoleum-8wK (claude-opus-4-7), 2026-06-26.
