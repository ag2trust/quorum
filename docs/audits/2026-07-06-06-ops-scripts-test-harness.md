# Post-refactor audit 6/6: ops scripts & test harness hygiene

**Date:** 2026-07-06
**Commit:** `110b3efe` (HEAD of `main`)
**Scope:** `scripts/serve-supervisor.sh`, `scripts/test-serve-supervisor.sh`,
`dev-install.sh`, `preflight.sh`, `.githooks/pre-push`, `.github/workflows/ci.yml`,
`.github/workflows/release.yml`, `quorum/src/bin/fake_agent.rs`,
docs/README/CLAUDE.md drift.

---

## Findings

### Finding 1 — Supervisor never fast-forwards after fetch (Priority 2: silent stall / broken self-update)

**File:** `scripts/serve-supervisor.sh:75-77`

**Evidence:** Line 75 runs `git -C "$REPO_DIR" fetch origin "$BASE_BRANCH"`, which
updates `origin/main` but does NOT advance the working tree or the local branch.
Line 77 runs `cd "$REPO_DIR" && ./dev-install.sh`, which runs `cargo build --release`
from the working tree — still pointing at the pre-fetch commit.

**Failure scenario:** The daemon detects a new sha on `origin/main`, drains all workers,
exits 75, the supervisor fetches, then rebuilds *the exact same source code*. The new
binary is identical to the old one. On relaunch the daemon sees the same sha mismatch
(local HEAD != origin/main), drains again, exits 75 again — the thrash guard eventually
stops the loop, but only after 6 wasted drain-rebuild cycles. Meanwhile, every in-flight
task was drained for nothing. The self-update mechanism is a no-op: no new code ever
gets built.

**Proposed fix-task:**
Add `git -C "$REPO_DIR" merge --ff-only origin/"$BASE_BRANCH"` between fetch and
`dev-install.sh`. If the merge fails (dirty tree or non-ff), alert and fall back to the
old binary — do not attempt a rebase or force-reset, since the supervisor's repo should
always be a clean checkout of `main`.

---

### Finding 2 — Supervisor has no signal forwarding; Ctrl-C orphans the daemon (Priority 2: silent stall)

**File:** `scripts/serve-supervisor.sh` (entire file — no `trap` statement)

**Evidence:** There is no `trap` for SIGINT, SIGTERM, or SIGHUP anywhere in the script.
The `while true` loop runs `"$SERVE_BIN" serve "$@"` as a foreground child. When the
user sends Ctrl-C (SIGINT) to the supervisor's process group, the shell default
behavior *may* deliver SIGINT to the child (depending on terminal job-control), but
this is not guaranteed when the supervisor is run via `nohup`, `screen`, or `systemd`
(where it has its own session). SIGTERM sent to the supervisor PID (e.g. `kill $PID`)
hits only the shell — the child `quorum serve` process is not signaled.

**Failure scenario:** An operator runs `kill <supervisor-pid>`. The supervisor shell
exits. The `quorum serve` child keeps running, now orphaned (reparented to init). The
daemon_lock heartbeat stays live, so no other daemon can start on this DB. The operator
must manually find and kill the orphaned serve process. If they don't, the daemon runs
unmanaged indefinitely — self-update is gone, thrash guard is gone.

**Proposed fix-task:**
Add a trap that forwards SIGTERM/SIGINT to the child PID:
```sh
trap 'kill -TERM "$child" 2>/dev/null; wait "$child"; exit' INT TERM
"$SERVE_BIN" serve "$@" &
child=$!
wait "$child"
code=$?
```
This pattern ensures the child is signaled on supervisor termination and the exit code
is captured correctly.

---

### Finding 3 — Supervisor build has no timeout; a hung build stalls self-update forever (Priority 2: silent stall)

**File:** `scripts/serve-supervisor.sh:77`

**Evidence:** `( cd "$REPO_DIR" && ./dev-install.sh )` runs `cargo build --release` with
no timeout. `dev-install.sh` itself has no timeout mechanism.

**Failure scenario:** `cargo build --release` hangs (e.g. waiting on a crates.io
download, an exhausted build cache, or a proc-macro that loops). The supervisor blocks
in the subshell forever. No daemon is running (the previous one exited 75). No alert is
emitted. All agents are idle with no dispatcher. Since the supervisor is blocked in a
child process, even the thrash guard cannot trip — it only runs after the build
returns.

**Proposed fix-task:**
Wrap the dev-install.sh call in a timeout: `timeout 300 ./dev-install.sh` (5 minutes,
adjustable via env). On timeout, alert and relaunch old binary. Requires GNU coreutils
`timeout` or a POSIX alternative (`perl -e 'alarm(300); exec @ARGV'`).

---

### Finding 4 — Supervisor alert() goes to stderr only; invisible when daemonized (Priority 2: silent stall)

**File:** `scripts/serve-supervisor.sh:38-40`

**Evidence:** `alert()` is `printf ... >&2`. When the supervisor runs daemonized (e.g.
`nohup scripts/serve-supervisor.sh &` or via launchd), stderr may be redirected to
`/dev/null` or a file nobody monitors.

**Failure scenario:** Build fails, old binary relaunches, alert is emitted to stderr.
Nobody reads it. The fleet runs on a stale binary indefinitely — the entire purpose of
the supervisor (keep code current) is silently defeated. The thrash guard tripping is
equally invisible.

**Proposed fix-task:**
In addition to stderr, write a structured `quorum post --kind alert --body-stdin`
message into the DB (the daemon will be relaunched after the alert and agents will see
it). Alternatively, write to a well-known log file and/or call an external notifier
(webhook). At minimum, document that operators must ensure stderr is captured.

---

### Finding 5 — fake_agent does not model `quorum done` mailbox signal (Priority 4: test gap)

**File:** `quorum/src/bin/fake_agent.rs` (entire file)

**Evidence:** The real agent lifecycle requires running `quorum done --agent <name>
--pr N` (workers) or `quorum done --agent <name> --pr N --verdict approved --blocking 0`
(reviewers) as a Bash tool call. This writes a `MailboxKind::Done` row to SQLite, which
the daemon polls in Phase 2 of the tick loop to detect task completion.

fake_agent emits `{type: "assistant"}` + `{type: "result"}` on stdout but never writes
any mailbox row. Tests that need the daemon to react to "worker finished" must inject
mailbox rows separately (e.g. `quorum_done()` helper in test code).

**Failure scenario (tests pass on fiction):** The agent prompt instructs workers to run
`quorum done --agent X --pr N`. If the prompt text or the CLI arg contract changes
(e.g. `--pr` is renamed to `--pull-request`), the daemon's Phase 2 parser and the
agent's prompted instructions diverge — but no integration test catches it because
fake_agent never exercises that path. The two sides (daemon parser + agent instructions)
are tested in isolation, and the seam between them is untested.

**Missing tests:**
- `test_fake_agent_signals_done_via_mailbox`: fake_agent runs `quorum done` as a
  subprocess; daemon detects it without test-harness injection.
- `test_reviewer_verdict_attestation_roundtrip`: fake_agent runs `quorum done --verdict
  approved --blocking 0` and the daemon's verdict gate accepts it end-to-end.

**Proposed fix-task:**
Create a `fake_agent_v2` binary (or extend fake_agent with a `--with-side-effects`
mode) that actually calls `quorum done` as a subprocess on its final turn. This
exercises the full mailbox round-trip: agent stdout events + quorum CLI → mailbox row →
daemon Phase 2 poll → lifecycle transition.

---

### Finding 6 — fake_agent emits no `tool_use` events (Priority 4: test gap)

**File:** `quorum/src/bin/fake_agent.rs` (entire file)

**Evidence:** The daemon's stream parser (`quorum/src/serve/stream.rs`) handles three
event types: `assistant`, `tool_use`, and `result`. Real agents emit hundreds of
`tool_use` events per turn (Bash, Read, Edit, etc.). The daemon tracks these for live
stats: `live_stats.tool_count`, `live_stats.now_label`, `events_per_min()`.

fake_agent never emits `tool_use` events. In all serve tests, `tool_count` is always 0
and `now_label` is always empty.

**Failure scenario:** A bug in `tool_use` event parsing (e.g. a renamed field, a missing
null-check on `name`) would go undetected in integration tests. The watchdog's
events-per-minute liveness check, if it ever uses tool events as a signal, would have
no test coverage for realistic event rates.

**Missing test:** `test_tool_use_events_counted`: fake_agent emits a few `tool_use`
events before its `result`; assert `live_stats.tool_count > 0` in the daemon.

**Proposed fix-task:**
Add a `--emit-tool-use` mode to fake_agent that emits 2-3 `{type: "tool_use",
name: "Bash", input: {...}}` events between the `assistant` and `result` events per
turn. Update at least one serve test to use this mode and assert the tool counter.

---

### Finding 7 — Supervisor shell tests not run in CI (Priority 4: test gap)

**File:** `.github/workflows/ci.yml`

**Evidence:** CI runs `cargo fmt`, `cargo clippy`, and `cargo test`. The file
`scripts/test-serve-supervisor.sh` exists with 7 shell-level tests for the supervisor,
but CI never runs it. The test script must be invoked manually.

**Failure scenario:** A change to `serve-supervisor.sh` (e.g. the proposed `git merge`
fix) breaks the supervisor loop logic. `cargo test` passes (it doesn't test shell
scripts). The PR merges. The breakage is only discovered when a live supervisor
misbehaves.

**Missing CI step:** Add a step after `cargo test` (or as a separate job) that runs
`scripts/test-serve-supervisor.sh`.

**Proposed fix-task:**
Add a `shell-tests` job to `.github/workflows/ci.yml`:
```yaml
  shell-tests:
    name: shell tests
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: scripts/test-serve-supervisor.sh
```

---

### Finding 8 — CLAUDE.md status line is massively stale (Priority 5: docs drift)

**File:** `CLAUDE.md:11`

**Evidence:** Line 11 reads:
> `Status: implemented and shipping. 11 core modules, 6 bin modules, schema v5, 142 tests`

Actual state (from this audit):
- Schema: v20 (`quorum-core/src/db.rs:12`)
- Tests: 306 (from `cargo test` output; 655 `#[test]` annotations)
- Core modules: 22 `.rs` files in `quorum-core/src/`
- Bin modules: 1 (`fake_agent.rs`), plus 11 serve modules

Every number in the status line is wrong. An agent reading CLAUDE.md for orientation
gets a picture of a v5/142-test project that is nothing like the v20/306-test codebase.

**Proposed fix-task:**
Update CLAUDE.md line 11 to reflect actual counts. Consider replacing hard-coded counts
with a range or a "run `cargo test` to see current counts" pointer, since they rot
within days.

---

### Finding 9 — Supervisor test does not cover signal handling (Priority 4: test gap)

**File:** `scripts/test-serve-supervisor.sh`

**Evidence:** The test script covers: non-75 exit propagation, exit 75 → rebuild →
relaunch, build failure → old binary, thrash guard, and multi-cycle recovery. But it
has no test for signal behavior — which is unsurprising since the supervisor currently
has no signal handling at all (Finding 2). Once signal forwarding is added, the test
suite needs cases for it.

**Missing tests:**
- `test_sigterm_to_supervisor_stops_child`: send SIGTERM to the supervisor, assert the
  child process is also terminated (not orphaned).
- `test_sigint_propagates_to_child`: send SIGINT, assert clean shutdown.

**Proposed fix-task:**
After implementing Finding 2's signal-forwarding fix, add test cases to
`test-serve-supervisor.sh` that spawn the supervisor in background, send signals, and
verify both processes exit.

---

### Finding 10 — Serve tests use fixed-duration sleeps; CI-flaky under load (Priority 4: test gap / flakiness)

**Files:** Multiple `quorum/tests/cli_serve_*.rs` files

**Evidence:** At least 30+ `thread::sleep(Duration::from_secs(N))` calls scattered
across serve tests, with N ranging from 1 to 3. These are wall-clock delays waiting for
the daemon to reach a particular state (e.g. "wait for reviewer to finish", "wait for
merge attempt"). Most tests use a `wait_for()` helper with a deadline for *detecting*
log lines, but the inter-step synchronization relies on raw sleeps.

Representative examples:
- `cli_serve_watchdog.rs:261`: `sleep(2)` — "wait for reviewer to finish"
- `cli_serve_mailbox.rs:272,414,465`: `sleep(1500ms)` — "wait for daemon tick"
- `cli_serve_concurrency.rs:362`: `sleep(3)` — "wait for tasks to be claimed"
- `cli_serve_reviewer.rs:625`: `sleep(3)` — "wait for rework to settle"
- `cli_serve_merge_checks.rs:268,354,447,562,667`: five `sleep(2)` — "wait for merge
  attempt"

**Failure scenario:** On a loaded CI runner (or a slower machine), the daemon tick takes
longer than the sleep. The test asserts state that hasn't been reached yet → flaky
failure. Conversely, the 2-3s sleeps add up: 13 serve test files × ~3 sleeps × ~2s =
~78s of pure wait time in the test suite, inflating CI wall-clock.

**Proposed fix-task:**
Replace fixed sleeps with event-driven synchronization: extend the `wait_for()` helper
to cover all inter-step waits, or poll `quorum task-get`/`quorum status --json` for the
expected state with a short poll interval and a generous deadline. This both eliminates
flakiness and reduces total test time.

---

## Out-of-scope handoffs

- **`cockpit.rs` references `"closed"` status** (baseline finding, audit 1/2 scope):
  `cockpit.rs:389,578` mention a `"closed"` state that doesn't exist in the lifecycle
  state machine.
- **`sticky_until` undocumented in design spec** (baseline finding, audit 1/2 scope):
  code uses it in `sync.rs`/`schema.sql`, design spec doesn't mention it.
- **`recovery.rs` at 21% coverage** (audit 3 scope): almost entirely untested at
  integration level.

## Verification

```
$ cargo test
test result: ok. 306 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

No source files were modified — audit is read-only. Every finding cites file:line
evidence and includes a concrete failure scenario.
