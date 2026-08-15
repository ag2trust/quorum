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

`timing.json` is valid JSON with `version: 2` and these top-level fields:

| Field | Meaning |
| --- | --- |
| `timestamp_utc` | UTC completion timestamp in RFC 3339 form. |
| `top_n` | Requested list length (default `10`). |
| `test_threads` | `--test-threads` value used when executing each test binary. |
| `gates` | Ordered objects with `name`, `duration_secs`, and `exit_code`. Full preflight prepends `branch_base`; the collector records `cargo_fmt`, `cargo_clippy`, `cargo_test_no_run`, and `test_execute` until a failure stops later gates. |
| `test_binaries` | One object per Cargo test executable discovered during the compile/no-run gate. Its identity fields are `package_id`, `manifest_path`, `target_name`, `target_kinds`, `executable`, and `fresh`; after execution it also has `execute_secs` and `execute_exit_code`. It is empty when compilation was never reached. |
| `top_n_slowest` | A bounded, descending copy of the slowest entries in `test_binaries`. |
| `rustc_wrapper` | Correlation accounting (`matched`, `log_entries`, and `log_path`) once compile/no-run starts; `{}` for an earlier fmt or clippy failure. |

`timing.json` and `summary.txt` are emitted for a collector run even when an
early collector gate fails. In that case, `cargo-test-no-run.jsonl`,
`cargo-test-no-run.stderr`, and `rustc-invocations.jsonl` do not exist, and
`rustc_wrapper` has no accounting fields. Those files and fields are therefore
conditional, not an artifact validity check.

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
compile or execution time. Inspect the two columns to decide which phase to
investigate. A compile value of `n/a` means the wrapper could not match that
binary, not that its compile time was zero. The binary name in the text table
is truncated to 48 characters; use `timing.json` for the full executable path
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
    cd "$wt" || exit
    RUST_TEST_THREADS=4 rtk proxy ./preflight.sh >"$RESULTS/$label.log" 2>&1
    status=$?
    if [ "$status" -eq 0 ]; then
      cp target/preflight-timing/timing.json "$RESULTS/$label.timing.json"
      cp target/preflight-timing/summary.txt "$RESULTS/$label.summary.txt"
    fi
    exit "$status"
  )
}

# Idle baseline: wait for this one run before creating competing work.
run_preflight "$WT0" idle

# Two concurrent worktrees: start both before either is awaited.
run_preflight "$WT0" two-a & p0=$!
run_preflight "$WT1" two-b & p1=$!
wait "$p0"; wait "$p1"

# Three concurrent worktrees: start all three before any wait.
run_preflight "$WT0" three-a & p0=$!
run_preflight "$WT1" three-b & p1=$!
run_preflight "$WT2" three-c & p2=$!
wait "$p0"; wait "$p1"; wait "$p2"
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

Compare the `gates` arrays for idle, both two-way artifacts, and all three
three-way artifacts. Compare `top_n_slowest` (or the matching summaries) to
find binaries whose compile attribution or execution time changes under load.
For a stable observation, repeat each round and compare a median or range,
rather than drawing a conclusion from one contended run. The concurrent
artifacts describe each worktree's observed wall time; they do not provide a
single system-wide total.
