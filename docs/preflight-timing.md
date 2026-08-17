# Preflight timing artifacts and loaded-worktree comparisons

`./preflight.sh` invokes the timing collector only after its `branch_base` gate
passes. The collector's artifact is intentionally per-worktree: its
deterministic path is `target/preflight-timing/timing.json`, with the readable
companion at `target/preflight-timing/summary.txt`. A later collector run in
that worktree replaces both files. A `branch_base` failure starts no collector,
so it produces no new timing artifact and a prior artifact can remain at this
path; never treat that prior file as evidence for the failed run.

If the collector reaches compile/no-run, it also leaves these raw inputs in the
same directory:

- `cargo-test-no-run.jsonl` — Cargo's structured compiler-artifact stream.
- `cargo-test-no-run.stderr` — compiler diagnostics from the compile/no-run
  gate.
- `rustc-invocations.jsonl` — one wall-clocked Rust compiler invocation per
  JSON line.

The collector can instead write its artifact and any applicable raw files to a
chosen directory with `scripts/preflight/timing.sh --out DIR`. That direct
invocation does not add the `branch_base` gate; use full `./preflight.sh` when
the complete author-gate artifact is required.

## Artifact schema

`timing.json` is valid JSON with `version: 3` and these top-level fields:

| Field | Meaning |
| --- | --- |
| `timestamp_utc` | UTC completion timestamp in RFC 3339 form. |
| `top_n` | Requested list length (default `10`). |
| `test_jobs` | Maximum test executables scheduled concurrently (`--test-jobs`; default `2`, or `PREFLIGHT_TEST_JOBS`). |
| `test_threads` | `--test-threads` value used when executing each test binary. |
| `test_timeout_secs` | Per-test-executable deadline (default `120`). |
| `term_grace_secs` | Bounded wait after TERM and again after KILL while cleaning a test process group (default `2`). |
| `interrupted_signal` | Signal number that interrupted test execution, or JSON `null` when uninterrupted. |
| `first_failure` | The first failed gate or test binary, including its exit/outcome details and a target-specific Cargo `rerun_command` when compiler-artifact identity is sufficient. JSON `null` on success. |
| `gates` | Ordered objects with `name`, `duration_secs`, and `exit_code`. Full preflight prepends `branch_base`; the collector records `cargo_fmt`, `cargo_clippy`, `cargo_test_no_run`, and `test_execute` until a failure stops later gates. |
| `test_binaries` | One object per Cargo test executable discovered during the compile/no-run gate. Its identity fields are `package_id`, `manifest_path`, `target_name`, `target_kinds`, `executable`, and `fresh`; execution fields are described below. It is empty when compilation was never reached. |
| `top_n_slowest` | A bounded, descending copy of the slowest entries in `test_binaries`. |
| `rustc_wrapper` | Correlation accounting (`matched`, `log_entries`, and `log_path`) once compile/no-run starts; `{}` for an earlier fmt or clippy failure. |

`timing.json` and `summary.txt` are emitted for a collector run even when an
early collector gate fails. In that case, `cargo-test-no-run.jsonl`,
`cargo-test-no-run.stderr`, and `rustc-invocations.jsonl` do not exist, and
`rustc_wrapper` has no accounting fields. Those files and fields are therefore
conditional, not an artifact validity check.

After a binary is executed, its entry has `execute_secs`,
`execute_exit_code`, `execute_outcome` (`passed`, `failed`, `timed_out`, or
`interrupted`; fail-fast cancellation uses `owner_lost`),
`execute_timed_out`, and `execute_timeout_secs`. An in-flight binary cancelled
because another binary failed also has `execute_cancelled_by_fail_fast: true`.
Its `cleanup`
object records `attempted`, `term_sent`, `kill_sent`, `complete`, and `error`.
`complete: true` means the isolated test and its tracked descendant tree,
including descendants that created separate process groups, no longer exist
and the supervisor reaped the children it owns. `error` also retains a
transient process-discovery diagnostic when fallback or a later snapshot still
allowed cleanup to complete. A timeout uses exit code `124`.
The collector compiles once and then runs at most `test_jobs` discovered
executables concurrently. After the first observed nonzero exit or incomplete
cleanup, it schedules no more binaries and closes every other active
supervisor's owner pipe. Those supervisors perform bounded descendant cleanup
and are reaped before the collector publishes its partial artifact. The
completed result that caused cancellation remains the exact `first_failure`;
cancelled peers cannot replace it. Later discovered entries remain in
`test_binaries` without execution fields.
Each test is owned by a supervisor outside the collector's process group; if
the collector is abruptly killed, that supervisor observes owner loss and
performs the same bounded TERM/KILL cleanup before exiting. Because an
abruptly killed collector cannot finish writing JSON, that owner-loss cleanup
does not itself promise a new timing artifact.

### Signal-safe supervisor handle lifecycle

The collector's Python signal handlers raise immediately, so supervisor-handle
bookkeeping must remain correct at every bytecode boundary, not only around
blocking syscalls. Keep these rules together when changing scheduling or
cleanup:

- A handle remains in the collector's tracked active set until its supervisor
  result is decoded and the corresponding binary fields, including
  `first_failure`, are recorded.
- `finish()` blocks terminal signals while it reaps the supervisor, decodes and
  caches the result, and closes descriptors. It restores the prior mask only
  after the cached result is retryable and both descriptor fields are retired.
- Descriptor closure transfers ownership first: while terminal signals are
  blocked, set the field to `-1` before calling `close(2)`. This prevents both
  a leaked open descriptor marked closed and a closed descriptor retried as
  live after an interrupt.
- Fail-fast cancellation records `execute_cancelled_by_fail_fast` and closes
  the peer's owner pipe under one signal mask. An interrupt may be delivered
  afterward, but the interruption path must observe both facts and finish
  reaping the peer before artifact publication.

The shell regressions inject SIGTERM at completed-result collection,
fail-fast peer settlement, and immediately after the real owner-pipe close.
Retain those deterministic seams; ordinary process timing is too imprecise to
reliably exercise these boundaries.

### Nested collector fixture launch

The Rust supervisor fixtures copy `timing.sh` into a fresh temporary repository
and invoke it through `python3`, rather than asking the kernel to execute that
newly materialized script directly. Linux CI has returned `ETXTBSY` from the
direct spawn even after the fixture copy call completed. Explicit interpreter
launch still leaves the Python child as the signal target and process-group owner,
while avoiding an executable-inode race that is unrelated to the supervisor
behavior under test. Keep copied fixture paths per-instance and use this launch
pattern for new nested collector fixtures.

The default of two jobs and four threads per binary is based on one warm
full-suite comparison on the 10-core development host. Test-execution wall
time was 336.572 seconds at `1 x 4`, 241.591 seconds at `2 x 2`, and 174.005
seconds at `2 x 4`. The `4 x 1` profile was rejected: `cli_serve_merge_checks`
exceeded the existing 120-second binary deadline and made the run fail. These
measurements justify the conservative two-job default; they are not a portable
performance guarantee. Override either dimension when diagnosing machine- or
test-specific behavior.

For a binary, `compile_no_run_secs` is the sum of wall-clock intervals for the
matching test `rustc` invocations, and `compile_no_run_source` identifies the
result: `rustc_wrapper`, `cached_fresh`, or `unmatched`. `unmatched` has a
JSON `null` duration; `cached_fresh` has `0.0`. Rust compiler invocations can
overlap, so binary compile times are diagnostic attribution, not shares of the
compile gate wall-clock time. Use `gates[].duration_secs` for end-to-end gate
duration and do not add all binary times to estimate it.

## Reading the slow-binary list

`summary.txt` has a compact `slowest test binaries (top N of M)` table. It
shows `target_name`, `compile_no_run`, and `execute` for the same entries as
`top_n_slowest`. The ranking key is:

```
(compile_no_run_secs or 0) + (execute_secs or 0)
```

It is descending and limited to `top_n`; it is not separately ranked by
compile or execution time. Inspect those columns and the execution `outcome`
to decide which phase to investigate. A compile value of `n/a` means the wrapper could not match that
binary, not that its compile time was zero. The binary name in the text table
is truncated to 40 characters; use `timing.json` for the full executable path
and package identity.

## Optional loaded-worktree comparison

This is a repeatable diagnostic experiment, not an author gate. Every author
still runs the required full `rtk proxy ./preflight.sh` before `quorum submit`.
Do **not** add an idle or loaded comparison to that required gate, and do not
treat a slower loaded run as a gate failure.

Use three isolated worktrees at the same commit. Keep the machine otherwise
idle for the baseline, use the same `RUST_TEST_THREADS` value for every run,
and preserve each artifact outside a worktree before its next run overwrites
the deterministic path. For example, from a shell that can access the three
worktrees:

```sh
RESULTS=/tmp/quorum-preflight-compare-$(date +%Y%m%d-%H%M%S)
mkdir -p "$RESULTS"
WT0=/path/to/quorum-wt-0
WT1=/path/to/quorum-wt-1
WT2=/path/to/quorum-wt-2

run_preflight() {
  wt=$1 label=$2
  (
    preflight_status=not_run
    timing_copy_status=not_run
    summary_copy_status=not_run

    if cd "$wt"; then
      RUST_TEST_THREADS=4 rtk proxy ./preflight.sh >"$RESULTS/$label.log" 2>&1
      preflight_status=$?
      if [ "$preflight_status" -eq 0 ]; then
        cp target/preflight-timing/timing.json "$RESULTS/$label.timing.json"
        timing_copy_status=$?
        cp target/preflight-timing/summary.txt "$RESULTS/$label.summary.txt"
        summary_copy_status=$?
      fi
    fi

    # Preserve every outcome, including skipped copies after a failed preflight.
    printf 'preflight=%s\ntiming_copy=%s\nsummary_copy=%s\n' \
      "$preflight_status" "$timing_copy_status" "$summary_copy_status" \
      >"$RESULTS/$label.status"
    status_file_status=$?

    if [ "$preflight_status" = 0 ] \
      && [ "$timing_copy_status" = 0 ] \
      && [ "$summary_copy_status" = 0 ] \
      && [ "$status_file_status" = 0 ]; then
      exit 0
    fi
    exit 1
  )
}

wait_for_all() {
  round_failed=0
  for child_pid in "$@"; do
    wait "$child_pid" || round_failed=1
  done
  return "$round_failed"
}

# Idle baseline: wait for this one run before creating competing work.
if ! run_preflight "$WT0" idle; then
  printf 'idle round failed; see %s/idle.status and idle.log\n' "$RESULTS" >&2
  exit 1
fi

# Two concurrent worktrees: start both before either is awaited.
run_preflight "$WT0" two-a & p0=$!
run_preflight "$WT1" two-b & p1=$!
if ! wait_for_all "$p0" "$p1"; then
  printf 'two-worktree round failed; see %s/two-*.status and *.log\n' "$RESULTS" >&2
  exit 1
fi

# Three concurrent worktrees: start all three before any wait.
run_preflight "$WT0" three-a & p0=$!
run_preflight "$WT1" three-b & p1=$!
run_preflight "$WT2" three-c & p2=$!
if ! wait_for_all "$p0" "$p1" "$p2"; then
  printf 'three-worktree round failed; see %s/three-*.status and *.log\n' "$RESULTS" >&2
  exit 1
fi
```

Before each round, verify every worktree has the intended identical `HEAD` and
no source changes that would alter the workload. Record the host, Rust/Cargo
versions, `RUST_TEST_THREADS`, and whether the target directories were warm or
cold alongside `RESULTS`; keep that cache state the same across rounds. A
failed preflight is not comparable to a green run—fix the failure and repeat
the round. If `branch_base` failed, there is no new collector output and any
file at the deterministic path is stale. A failure after the collector began
can have a partial artifact, but it is diagnostic only and is excluded from
this comparison; the example preserves artifacts only from green runs.
Each `<label>.status` file records the preflight and both copy outcomes; all
three must be `0` for each label before comparing a round. The wait helper
waits for every started child even after a failure, then rejects the entire
round if any child or status-file write failed.

Compare the `gates` arrays for idle, both two-way artifacts, and all three
three-way artifacts. Compare `top_n_slowest` (or the matching summaries) to
find binaries whose compile attribution or execution time changes under load.
For a stable observation, repeat each round and compare a median or range,
rather than drawing a conclusion from one contended run. The concurrent
artifacts describe each worktree's observed wall time; they do not provide a
single system-wide total.
