# Quorum — Agent Guide

Quorum is a local coordination substrate for AI coding agents: one `quorum` binary and one
SQLite database per managed repository. Short-lived CLI commands perform atomic operations;
`quorum serve` owns worker, review, rework, approval, and merge lifecycle.

This file is the small, always-loaded operating contract. Do **not** read the full design
spec by default. Read feature-specific design or history only when the task affects it; use
the routing table at the end of this guide.

## Product priorities

Optimize in this order:

1. **Atomic** — concurrency safety comes from storage mechanisms, not agent discipline.
2. **Fail-safe** — failures are loud and never silently corrupt state or grant authority.
3. **Simple** — keep the command and lifecycle surface small.
4. **Effective / fast** — cheap polling, instant claims, bounded context and storage.

Quorum is an opinionated Git/GitHub coding pipeline for agents. The only human-facing
surface is read-only status/inspection. Avoid human workflow features, manual pruning, or
general-purpose orchestration abstractions.

## Repository-wide invariants

These contracts apply to every relevant change:

1. **Atomic claims:** enforce one active holder with
   `UNIQUE(target) WHERE active = 1`; `active` is `INTEGER NOT NULL DEFAULT 0`.
   Claims and task wins occur inside one `BEGIN IMMEDIATE` transaction.
2. **Every SQLite connection:** set `busy_timeout=5000`, switch/retry
   `journal_mode=WAL`, then use `synchronous=NORMAL`. Keep `rusqlite` bundled.
3. **Race outcomes:** a lost race is `SQLITE_CONSTRAINT_UNIQUE` or zero guarded
   `UPDATE … RETURNING` rows → clean exit 1, no `errors` row. Post-timeout
   `SQLITE_BUSY` is abnormal → exit 3 and log it.
4. **Exit codes:** 0 success · 1 expected clean negative · 2 usage/bad input ·
   3 internal/DB/migration failure.
5. **Expiry:** rows are dead at `expires_at <= now` and live at `expires_at > now`.
   Logical read filters provide correctness; physical sweep is housekeeping. Agents and
   tasks do not expire.
6. **Short reads:** never hold a read transaction across polling ticks. A long reader pins
   the WAL. `status --watch` opens and closes a connection every tick.
7. **Migrations:** check `PRAGMA user_version` on every open; apply forward-only,
   idempotent migrations under the write lock. Refuse a newer DB. `serve` maps
   `SchemaTooNew` to exit 75 for supervised self-update.
8. **Monotonic cursors:** advance with `MAX(last_seq, ?)`. Delivery is at-least-once.
9. **Text safety:** free text enters through stdin/file/JSON, is SQL-bound, and is emitted
   as JSON. Reject invalid UTF-8 and embedded NUL.
10. **Single daemon:** `daemon_lock` is the per-DB authority. A live second daemon fails;
    stale/dead ownership may be taken over.
11. **Lifecycle authority:** managed agents signal outcomes; the daemon alone transitions
    lifecycle, posts formal reviews, and merges. Messages never cause lifecycle changes.

If a change intentionally alters one of these contracts, update the design spec in the
same PR and provide concurrency/runtime evidence appropriate to the invariant.

## Engineering and evidence

- Grep before coding and copy established patterns.
- Fix root causes; prefer additive, idempotent migrations over backfills.
- Tests must fail when the behavior breaks. Assert state, exit code, or JSON—not merely a
  log line. State-machine changes need negative-path coverage.
- Concurrency/storage claims require a real DB and concurrent processes, repeated enough
  to expose flakes. The claim-race canary is a smoke alarm; investigate any flake.
- Mechanism claims require a `file:line`, test result, or DB row. Separate **Verified**
  evidence from **Hypothesis** in diagnoses.
- Do not over-claim in docs, help, PRs, or commits.
- Update the design spec only when product behavior or an established design contract
  changes. Ordinary implementation, bug fixes within the contract, and reviews do not
  require reading or editing the full spec.
- If a fix takes more than two attempts, owner correction changes direction, or behavior
  contradicts expectation, capture the correct reusable pattern in a focused reference
  document or the relevant design section.

## Git and delivery workflow

- Never edit the shared `~/dev/quorum` checkout. Create a worktree from `origin/main`:

  ```bash
  git worktree add -b <branch> ~/dev/quorum-wt/<branch> origin/main
  ```

- Never branch from another feature branch. Do not rebase solely to become current;
  up-to-date-before-merge is disabled, while a push dismisses prior approvals.
- Commit, push, and open PRs with the default `ag2trust-dev` identity. Never override the
  token or author as `brevitize`.
- Use conventional commit subjects and end commits with the working session's
  `Co-Authored-By:` trailer.
- Review without checking the PR branch out in the shared checkout. Use `gh pr diff`,
  `git show`, or a throwaway worktree.
- Author, deliverer, R1, and R2 separation is session-based. Whoever authored, adopted, or
  signaled a task cannot review that delivery.
- Reviewers classify findings as BLOCKING or advisory. Approval requires zero blockers;
  any blocker requires a changes verdict with feedback. Reviewers post findings/comments;
  the daemon owns formal approval/request-changes and merge.

### Required verification before submission

Run the full project gate before `quorum submit`:

```bash
rtk proxy ./preflight.sh
```

This is an author-side gate. Reviewers do not police CI status, PR-body verification
formatting, transcripts, links, headings, or evidence tokens; the daemon owns CI gating.
Never bypass the pre-push hook. For doc-only changes this gate still applies.

After pulling merged source, run `./dev-install.sh` to rebuild, install, verify required
commands, and check schema compatibility. Use `scripts/serve-supervisor.sh` for supervised
serve/self-update operation.

## Daemon and async checklist

For changes under `quorum/src/serve/` or other async lifecycle paths:

- Never race stateful work inside `select!` unless every losing future is cancellation-safe.
- Signals set shutdown flags; they do not complete work or transition tasks.
- Enumerate shutdown path × task state for shutdown changes, and test the negative paths.
- Never perform network/model calls while holding a DB transaction.
- Bound retries, allocations, prompts, queues, logs, and persistent rows.
- Provider CLI boundaries need real-binary argument/auth tests; `fake_agent` cannot catch
  pre-protocol argument or authentication failures.
- Thread `bare_agent` through every Claude spawn path. Session IDs come only from
  `agent::new_session_id()`.
- Codex continuation must preserve the provider-issued thread ID and exact pending turn.
- `.claude/**` edits require an attended session because `dontAsk` denies that namespace.

## Interactive owner sessions

Interactive sessions default to coordinator mode: investigate, reproduce, diagnose, scope,
and design. Exploratory wording does not authorize implementation or external mutations.
Implement directly only after an explicit instruction such as “implement it here.”

Use Quorum only for execution-ready tasks with observed/expected behavior, evidence,
affected paths, proposed remediation, constraints, and verification. Do not dispatch
open-ended production investigation or feature design.

If implementation moves out of an existing Quorum task, cancel that implementation task
before external work begins; create a review-only task for the resulting PR. Interactive
sessions do not claim, submit, or mark managed tasks done.

## On-demand reading

Read only the rows relevant to the task. The design spec is a searchable source of record,
not foundation context.

| Task area | Read on demand |
|---|---|
| Product behavior, lifecycle redesign, command/schema contract | Relevant section of `docs/2026-06-23-quorum-design.md` |
| CLI usage or installation | `README.md`, `quorum help`, `quorum <command> --help` |
| Task lifecycle/recovery/merge | `quorum-core/src/lifecycle.rs`, relevant `quorum/src/serve/` module, then the matching design section |
| SQLite, claims, expiry, migration, WAL | Relevant `quorum-core/src/` module and matching design section |
| Reviewer/R1/R2/rework prompts | `quorum/src/serve/reviewer.rs` and the review-responsibility design section |
| Provider spawning, auth, continuation | `quorum/src/serve/agent.rs`, `codex_agent.rs`, `runner.rs`, and nearby contract tests |
| Collector/review analytics | `quorum/src/serve/collector.rs`, `quorum-core/src/review_findings.rs` |
| Historical audits | Specific file under `docs/audits/`; do not load the directory wholesale |

Provider note: Claude sessions on this machine see RTK-compressed Bash output. Use
`rtk proxy <command>` whenever raw verification output is required. Codex does not need
that workaround.
