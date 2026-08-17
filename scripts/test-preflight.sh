#!/bin/sh
# Regression tests for preflight branch-base policy.

set -eu

ROOT=$(git rev-parse --show-toplevel)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

REMOTE="$TMP/origin.git"
REPO="$TMP/repo"
BIN="$TMP/bin"
git init --bare -q "$REMOTE"
git init -q "$REPO"
mkdir -p "$BIN"

cat >"$BIN/cargo" <<'EOF'
#!/bin/sh
if [ -n "${PREFLIGHT_CARGO_LOG:-}" ]; then
  printf '%s\n' "$*" >> "$PREFLIGHT_CARGO_LOG"
fi
# Modes: 0 = pass, 1 = fail every cargo invocation (fmt path),
# compile = pass fmt/clippy but fail `cargo test` compile/no-run with the same
# exit status Cargo returns for a real compile error.
[ "${PREFLIGHT_CARGO_FAIL:-0}" != 1 ] || {
  printf 'forced cargo diagnostic\n' >&2
  exit 1
}
[ "${PREFLIGHT_CARGO_FAIL:-0}" != compile ] || [ "$1" != test ] || {
  printf 'forced cargo test diagnostic\n' >&2
  exit 17
}
if [ "${PREFLIGHT_CARGO_TEST_BINARIES:-0}" = 1 ] && [ "$1" = test ]; then
  printf '{"reason":"compiler-artifact","package_id":"path+file:///fixture#fixture@0.1.0","manifest_path":"/fixture/Cargo.toml","target":{"name":"first_binary","kind":["test"]},"profile":{"test":true},"executable":"%s","fresh":false}\n' \
    "$PREFLIGHT_FIRST_TEST_BINARY"
  printf '{"reason":"compiler-artifact","package_id":"path+file:///fixture#fixture@0.1.0","manifest_path":"/fixture/Cargo.toml","target":{"name":"second_binary","kind":["test"]},"profile":{"test":true},"executable":"%s","fresh":false}\n' \
    "$PREFLIGHT_SECOND_TEST_BINARY"
  if [ -n "${PREFLIGHT_THIRD_TEST_BINARY:-}" ]; then
    printf '{"reason":"compiler-artifact","package_id":"path+file:///fixture#fixture@0.1.0","manifest_path":"/fixture/Cargo.toml","target":{"name":"third_binary","kind":["test"]},"profile":{"test":true},"executable":"%s","fresh":false}\n' \
      "$PREFLIGHT_THIRD_TEST_BINARY"
  fi
fi
exit 0
EOF
chmod +x "$BIN/cargo"

cd "$REPO"
git config user.name CI
git config user.email ci@example.invalid
git remote add origin "$REMOTE"

printf 'base\n' > state
git add state
git commit -qm 'base'
git branch -M main
git push -q origin main

git switch -qc develop
printf 'develop\n' >> state
git commit -qam 'develop work' -m 'Co-Authored-By: Develop-agent <develop@example.invalid>'
git push -q origin develop

git switch -q main
printf 'main\n' > main-only
git add main-only
git commit -qm 'main work' -m 'Co-Authored-By: Main-agent <main@example.invalid>'
git push -q origin main

# A daemon branch containing both current tips is a valid integration even
# though inherited commits carry different session trailers.
git switch -qc daemon/tester-t1
git merge -q --no-ff origin/develop -m 'integrate develop' \
  -m 'Co-Authored-By: Merge-agent <merge@example.invalid>'
cp "$ROOT/preflight.sh" ./preflight.sh
chmod +x ./preflight.sh
mkdir -p scripts/preflight
cp "$ROOT/scripts/preflight/timing.sh" scripts/preflight/timing.sh
chmod +x scripts/preflight/timing.sh
PATH="$BIN:$PATH" ./preflight.sh --quick >"$TMP/integration.out"
grep -q 'PREFLIGHT: PASS (quick' "$TMP/integration.out"

# Merely branching from develop is not integration: the branch omits current
# main and must remain subject to the normal branch-base rejection.
git switch -q --detach origin/develop
git switch -qc daemon/tester-t2
printf 'feature\n' > feature
git add feature
git commit -qm 'feature work' -m 'Co-Authored-By: Feature-agent <feature@example.invalid>'
cp "$ROOT/preflight.sh" ./preflight.sh
chmod +x ./preflight.sh
mkdir -p scripts/preflight
cp "$ROOT/scripts/preflight/timing.sh" scripts/preflight/timing.sh
chmod +x scripts/preflight/timing.sh
if PATH="$BIN:$PATH" ./preflight.sh --quick >"$TMP/feature.out" 2>&1; then
  echo 'expected develop-based feature branch to fail preflight' >&2
  exit 1
fi

# Install the real hook and prove its standard stdin tuple against a bare
# remote. The daemon publishes an exact SHA rather than a same-named local ref,
# so use that production-shaped refspec here too.
mkdir -p .githooks
cp "$ROOT/.githooks/pre-push" .githooks/pre-push
chmod +x .githooks/pre-push
git config core.hooksPath .githooks

# Git invokes pre-push with empty stdin when every selected ref is already up
# to date. The no-op must still run the ordinary quick gate and formatting.
git switch -q main
if PREFLIGHT_CARGO_FAIL=1 PATH="$BIN:$PATH" git push -q origin main \
  >"$TMP/noop-format.out" 2>&1; then
  echo 'expected formatting failure to reject an up-to-date push' >&2
  exit 1
fi
grep -q 'PREFLIGHT: FAIL (cargo fmt)' "$TMP/noop-format.out"
grep -q 'forced cargo diagnostic' "$TMP/noop-format.out"
PATH="$BIN:$PATH" git push -q origin main >"$TMP/noop.out" 2>&1
grep -q 'PREFLIGHT: PASS (quick' "$TMP/noop.out"

# The daemon may replay an exact durable source SHA after mutable HEAD moves.
# Prove both an initial publication and an existing-branch continuation use the
# proposed tuple SHA throughout instead of silently substituting HEAD.
git switch -q --detach origin/main
git switch -qc daemon/durable-local-t3
printf 'durable A\n' > durable
git add durable
git commit -qm 'durable initial publication' \
  -m 'Co-Authored-By: Durable-A <durable-a@example.invalid>'
DURABLE_A_SHA=$(git rev-parse HEAD)
printf 'local drift\n' >> durable
git commit -qam 'unpublished local drift' \
  -m 'Co-Authored-By: Drift-A <drift-a@example.invalid>'
PATH="$BIN:$PATH" git push -q origin \
  "$DURABLE_A_SHA:refs/heads/daemon/durable-t3"
REMOTE_SHA=$(git --git-dir="$REMOTE" rev-parse refs/heads/daemon/durable-t3)
[ "$REMOTE_SHA" = "$DURABLE_A_SHA" ]

git switch -q --detach "$DURABLE_A_SHA"
git switch -qc daemon/durable-replay-t4
printf 'durable B\n' >> durable
git commit -qam 'durable remediation' \
  -m 'Co-Authored-By: Durable-B <durable-b@example.invalid>'
DURABLE_B_SHA=$(git rev-parse HEAD)
printf 'more local drift\n' >> durable
git commit -qam 'second unpublished local drift' \
  -m 'Co-Authored-By: Drift-B <drift-b@example.invalid>'
PATH="$BIN:$PATH" git push -q origin \
  "$DURABLE_B_SHA:refs/heads/daemon/durable-t3"
REMOTE_SHA=$(git --git-dir="$REMOTE" rev-parse refs/heads/daemon/durable-t3)
[ "$REMOTE_SHA" = "$DURABLE_B_SHA" ]

git switch -q --detach origin/main
git switch -qc daemon/continuation-t3
printf 'worker A\n' > remediation
git add remediation
git commit -qm 'initial publication' \
  -m 'Co-Authored-By: Worker-A <worker-a@example.invalid>'
WORKER_A_SHA=$(git rev-parse HEAD)
PATH="$BIN:$PATH" git push -q origin \
  "$WORKER_A_SHA:refs/heads/daemon/continuation-t3"

printf 'worker B\n' >> remediation
git commit -qam 'sequential remediation' \
  -m 'Co-Authored-By: Worker-B <worker-b@example.invalid>'
WORKER_B_SHA=$(git rev-parse HEAD)
PATH="$BIN:$PATH" git push -q origin \
  "$WORKER_B_SHA:refs/heads/daemon/continuation-t3"
REMOTE_SHA=$(git --git-dir="$REMOTE" rev-parse refs/heads/daemon/continuation-t3)
[ "$REMOTE_SHA" = "$WORKER_B_SHA" ]

# A continuation prepared by the daemon merges the advanced base into the
# exact published PR head. Multiple sessions inherited from main must not be
# attributed to the continuation worker.
git switch -q main
printf 'base session A\n' > inherited-a
git add inherited-a
git commit -qm 'advance base from session A' \
  -m 'Co-Authored-By: Base-A <base-a@example.invalid>'
PATH="$BIN:$PATH" git push -q origin main
printf 'base session B\n' > inherited-b
git add inherited-b
git commit -qm 'advance base from session B' \
  -m 'Co-Authored-By: Base-B <base-b@example.invalid>'
PATH="$BIN:$PATH" git push -q origin main

git switch -q --detach "$WORKER_B_SHA"
git switch -qc daemon/inherited-base-continuation-t6
git merge -q --no-ff origin/main -m 'merge advanced main into continuation'
printf 'continuation worker\n' >> remediation
git commit -qam 'finish ancestry-preserving continuation' \
  -m 'Co-Authored-By: Continue-Worker <continue-worker@example.invalid>'
CONTINUATION_HEAD_SHA=$(git rev-parse HEAD)
git fetch -q origin \
  refs/heads/daemon/continuation-t3:refs/remotes/origin/daemon/continuation-t3
git branch --set-upstream-to=origin/daemon/continuation-t3
PATH="$BIN:$PATH" ./preflight.sh >"$TMP/full-merge-continuation.out"
grep -q 'configured-upstream continuation-owned commits' \
  "$TMP/full-merge-continuation.out"
grep -q 'PREFLIGHT: PASS (all 4 gates green)' \
  "$TMP/full-merge-continuation.out"
PATH="$BIN:$PATH" git push -q origin \
  "$CONTINUATION_HEAD_SHA:refs/heads/daemon/continuation-t3"
REMOTE_SHA=$(git --git-dir="$REMOTE" rev-parse refs/heads/daemon/continuation-t3)
[ "$REMOTE_SHA" = "$CONTINUATION_HEAD_SHA" ]

# The daemon supports repositories configured with a non-main base. Seed an
# existing PR head on develop, advance that base with two inherited sessions,
# and prove the daemon-supplied base identity reaches the real hook.
git switch -q --detach origin/develop
git switch -qc daemon/develop-pr-source-t8
printf 'develop PR\n' > develop-remediation
git add develop-remediation
git commit -qm 'develop PR head' \
  -m 'Co-Authored-By: Develop-Worker <develop-worker@example.invalid>'
DEVELOP_PR_HEAD=$(git rev-parse HEAD)
git --git-dir="$REMOTE" fetch -q "$REPO" \
  "$DEVELOP_PR_HEAD:refs/heads/daemon/develop-continuation-t8"

git switch -q develop
printf 'develop base A\n' > develop-inherited-a
git add develop-inherited-a
git commit -qm 'advance develop from session A' \
  -m 'Co-Authored-By: Develop-Base-A <develop-base-a@example.invalid>'
PATH="$BIN:$PATH" git push -q origin develop
printf 'develop base B\n' > develop-inherited-b
git add develop-inherited-b
git commit -qm 'advance develop from session B' \
  -m 'Co-Authored-By: Develop-Base-B <develop-base-b@example.invalid>'
PATH="$BIN:$PATH" git push -q origin develop

git switch -q --detach "$DEVELOP_PR_HEAD"
git switch -qc daemon/develop-continuation-worker-t9
git merge -q --no-ff origin/develop -m 'merge configured develop base'
printf 'develop continuation worker\n' >> develop-remediation
git commit -qam 'finish develop continuation' \
  -m 'Co-Authored-By: Develop-Continue <develop-continue@example.invalid>'
DEVELOP_CONTINUATION_SHA=$(git rev-parse HEAD)
if PATH="$BIN:$PATH" git push -q origin \
  "$DEVELOP_CONTINUATION_SHA:refs/heads/daemon/develop-continuation-t8" \
  >"$TMP/develop-default-base.out" 2>&1; then
  echo 'expected main-default continuation attribution to reject develop history' >&2
  exit 1
fi
grep -q 'sessions in branch-owned commits' "$TMP/develop-default-base.out"
REMOTE_SHA=$(git --git-dir="$REMOTE" rev-parse \
  refs/heads/daemon/develop-continuation-t8)
[ "$REMOTE_SHA" = "$DEVELOP_PR_HEAD" ]

QUORUM_CONTINUATION_BASE_BRANCH=develop PATH="$BIN:$PATH" git push -q origin \
  "$DEVELOP_CONTINUATION_SHA:refs/heads/daemon/develop-continuation-t8"
REMOTE_SHA=$(git --git-dir="$REMOTE" rev-parse \
  refs/heads/daemon/develop-continuation-t8)
[ "$REMOTE_SHA" = "$DEVELOP_CONTINUATION_SHA" ]

# Continuations only exempt already-published history. Multiple new worker
# sessions after the authoritative remote tip remain genuine stacking.
git switch -q --detach "$CONTINUATION_HEAD_SHA"
git switch -qc daemon/stacked-continuation-t7
printf 'continue A\n' >> remediation
git commit -qam 'first continuation session' \
  -m 'Co-Authored-By: Continue-A <continue-a@example.invalid>'
printf 'continue B\n' >> remediation
git commit -qam 'stacked continuation session' \
  -m 'Co-Authored-By: Continue-B <continue-b@example.invalid>'
STACKED_CONTINUATION_SHA=$(git rev-parse HEAD)
git branch --set-upstream-to=origin/daemon/continuation-t3
if PATH="$BIN:$PATH" ./preflight.sh \
  >"$TMP/full-stacked-continuation.out" 2>&1; then
  echo 'expected full gate to reject an upstream-tracking branch without a new base merge' >&2
  exit 1
fi
grep -q 'sessions in branch-owned commits' \
  "$TMP/full-stacked-continuation.out"
if PATH="$BIN:$PATH" git push -q origin \
  "$STACKED_CONTINUATION_SHA:refs/heads/daemon/continuation-t3" \
  >"$TMP/stacked-continuation.out" 2>&1; then
  echo 'expected stacked continuation sessions to fail pre-push' >&2
  exit 1
fi
grep -q 'sessions in branch-owned commits' "$TMP/stacked-continuation.out"
REMOTE_SHA=$(git --git-dir="$REMOTE" rev-parse refs/heads/daemon/continuation-t3)
[ "$REMOTE_SHA" = "$CONTINUATION_HEAD_SHA" ]

# Supported non-publication pushes keep their existing shapes. Formatting is
# still mandatory, but branch/session policy is irrelevant to tags and the
# daemon's cleanup tombstones.
git tag preflight-supported-tag "$STACKED_CONTINUATION_SHA"
PATH="$BIN:$PATH" git push -q origin refs/tags/preflight-supported-tag
REMOTE_SHA=$(git --git-dir="$REMOTE" rev-parse refs/tags/preflight-supported-tag)
[ "$REMOTE_SHA" = "$STACKED_CONTINUATION_SHA" ]

git tag preflight-format-must-run "$STACKED_CONTINUATION_SHA"
if PREFLIGHT_CARGO_FAIL=1 PATH="$BIN:$PATH" git push -q origin \
  refs/tags/preflight-format-must-run >"$TMP/tag-format.out" 2>&1; then
  echo 'expected formatting failure to reject a supported tag push' >&2
  exit 1
fi
if git --git-dir="$REMOTE" rev-parse --verify -q \
  refs/tags/preflight-format-must-run >/dev/null; then
  echo 'format-rejected tag unexpectedly reached the remote' >&2
  exit 1
fi

PATH="$BIN:$PATH" git push --atomic -q origin \
  "$CONTINUATION_HEAD_SHA:refs/heads/quorum-cleanup/preflight-t3" \
  ':refs/heads/daemon/continuation-t3'
REMOTE_SHA=$(git --git-dir="$REMOTE" rev-parse refs/heads/quorum-cleanup/preflight-t3)
[ "$REMOTE_SHA" = "$CONTINUATION_HEAD_SHA" ]
if git --git-dir="$REMOTE" rev-parse --verify -q \
  refs/heads/daemon/continuation-t3 >/dev/null; then
  echo 'cleanup transaction did not delete the publication branch' >&2
  exit 1
fi

PATH="$BIN:$PATH" git push -q origin ':refs/heads/quorum-cleanup/preflight-t3'
if git --git-dir="$REMOTE" rev-parse --verify -q \
  refs/heads/quorum-cleanup/preflight-t3 >/dev/null; then
  echo 'cleanup tombstone deletion did not reach the remote' >&2
  exit 1
fi

# If the publication branch is already absent, production records cleanup by
# creating only the tombstone ref. That single-ref sibling must also settle.
PATH="$BIN:$PATH" git push -q origin \
  "$CONTINUATION_HEAD_SHA:refs/heads/quorum-cleanup/preflight-absent-t3"
REMOTE_SHA=$(git --git-dir="$REMOTE" rev-parse \
  refs/heads/quorum-cleanup/preflight-absent-t3)
[ "$REMOTE_SHA" = "$CONTINUATION_HEAD_SHA" ]
PATH="$BIN:$PATH" git push -q origin \
  ':refs/heads/quorum-cleanup/preflight-absent-t3'
if git --git-dir="$REMOTE" rev-parse --verify -q \
  refs/heads/quorum-cleanup/preflight-absent-t3 >/dev/null; then
  echo 'single-ref cleanup tombstone did not retire' >&2
  exit 1
fi

# A genuinely stacked new branch still exposes both sessions because a zero
# remote SHA keeps the original origin/main..HEAD quick-gate range.
git switch -q --detach origin/main
git switch -qc daemon/stacked-t4
printf 'stack A\n' > stacked
git add stacked
git commit -qm 'stack base' \
  -m 'Co-Authored-By: Stack-A <stack-a@example.invalid>'
printf 'stack B\n' >> stacked
git commit -qam 'stacked feature' \
  -m 'Co-Authored-By: Stack-B <stack-b@example.invalid>'
STACKED_SHA=$(git rev-parse HEAD)
if PATH="$BIN:$PATH" git push -q origin \
  "$STACKED_SHA:refs/heads/daemon/stacked-t4" >"$TMP/stacked.out" 2>&1; then
  echo 'expected a newly stacked feature branch to fail pre-push' >&2
  exit 1
fi
grep -q 'sessions in branch-owned commits' "$TMP/stacked.out"

# The existing remote head must be an ancestor of the proposed update. Even a
# forced Git push cannot turn a stale/non-fast-forward tuple into a continuation.
git switch -q --detach origin/main
git switch -qc daemon/stale-t5
printf 'stale\n' > stale
git add stale
git commit -qm 'stale replacement' \
  -m 'Co-Authored-By: Stale-agent <stale@example.invalid>'
STALE_SHA=$(git rev-parse HEAD)
if PATH="$BIN:$PATH" git push --force -q origin \
  "$STALE_SHA:refs/heads/daemon/durable-t3" >"$TMP/stale.out" 2>&1; then
  echo 'expected stale/non-fast-forward update to fail pre-push' >&2
  exit 1
fi
grep -q 'stale or non-fast-forward' "$TMP/stale.out"

ZERO_SHA=0000000000000000000000000000000000000000

# Reject malformed, deletion, wrong-mapping, and multi-ref tuples before the
# quick gate can inspect an unrelated HEAD.
if printf 'malformed\n' \
  | PATH="$BIN:$PATH" .githooks/pre-push origin "$REMOTE" \
    >"$TMP/malformed.out" 2>&1; then
  echo 'expected malformed pre-push input to fail' >&2
  exit 1
fi
grep -q 'malformed local SHA' "$TMP/malformed.out"

if printf '%s %s %s %s\n' \
  '(delete)' "$ZERO_SHA" refs/heads/daemon/continuation-t3 "$WORKER_B_SHA" \
  | PATH="$BIN:$PATH" .githooks/pre-push origin "$REMOTE" \
    >"$TMP/delete.out" 2>&1; then
  echo 'expected deleted ref to fail pre-push' >&2
  exit 1
fi
grep -q 'deleted refs are not permitted' "$TMP/delete.out"

if printf '%s %s %s %s\n' \
  refs/heads/main "$STALE_SHA" refs/heads/daemon/wrong-t6 "$ZERO_SHA" \
  | PATH="$BIN:$PATH" .githooks/pre-push origin "$REMOTE" \
    >"$TMP/wrong-map.out" 2>&1; then
  echo 'expected wrong local ref mapping to fail pre-push' >&2
  exit 1
fi
grep -q 'local ref does not map to local SHA' "$TMP/wrong-map.out"

if {
    printf '%s %s %s %s\n' \
      "$STALE_SHA" "$STALE_SHA" refs/heads/daemon/one-t7 "$ZERO_SHA"
    printf '%s %s %s %s\n' \
      "$STALE_SHA" "$STALE_SHA" refs/heads/daemon/two-t8 "$ZERO_SHA"
  } | PATH="$BIN:$PATH" .githooks/pre-push origin "$REMOTE" \
    >"$TMP/multi-ref.out" 2>&1; then
  echo 'expected multiple ref mappings to fail pre-push' >&2
  exit 1
fi
grep -q 'multiple ref updates are not a cleanup transaction' "$TMP/multi-ref.out"

# Continuation ranges are hook-only quick-gate input and cannot weaken the
# mandatory full preflight invocation.
if PATH="$BIN:$PATH" ./preflight.sh \
  --continuation-from="$WORKER_A_SHA" >"$TMP/full-continuation.out" 2>&1; then
  echo 'expected full preflight to reject continuation range input' >&2
  exit 1
fi
grep -q 'require --quick' "$TMP/full-continuation.out"

# The full author gate must launch Cargo with this exact argument surface.
# In particular, the explicit quorum-core/test-support feature builds the
# private real-process helper used by the canaries; fake-agent tests cannot
# catch a Cargo feature or helper-launch failure before their protocol starts.
# Compare the complete ordered invocation log, rather than independently
# finding flags, so additions, removals, and ordering drift all fail.
cat >"$TMP/full-cargo.expected" <<'EOF'
fmt --all -- --check
clippy --all-targets --all-features --features quorum-core/test-support -- -D warnings
test --no-run --message-format=json --workspace --all-features --features quorum-core/test-support
EOF
PREFLIGHT_CARGO_LOG="$TMP/full-cargo.log" PATH="$BIN:$PATH" \
  ./preflight.sh >"$TMP/full.out"
cmp "$TMP/full-cargo.expected" "$TMP/full-cargo.log"
grep -q 'PREFLIGHT: PASS (all 4 gates green)' "$TMP/full.out"
grep -q '^  branch_base .*  ok$' target/preflight-timing/summary.txt
grep -q '"name": "branch_base"' target/preflight-timing/timing.json
grep -q 'slowest test binaries (top 0 of 0):' \
  target/preflight-timing/summary.txt
python3 - target/preflight-timing/timing.json <<'PY'
import json
import sys

data = json.load(open(sys.argv[1]))
assert data["test_jobs"] == 2
assert data["test_threads"] == 4
PY

if PATH="$BIN:$PATH" scripts/preflight/timing.sh --test-jobs 0 \
  >"$TMP/invalid-test-jobs.out" 2>&1; then
  echo 'expected zero test jobs to be rejected' >&2
  exit 1
fi
grep -q -- '--test-jobs must be positive' "$TMP/invalid-test-jobs.out"
if PREFLIGHT_TEST_JOBS=invalid PATH="$BIN:$PATH" \
  scripts/preflight/timing.sh >"$TMP/invalid-test-jobs-env.out" 2>&1; then
  echo 'expected invalid PREFLIGHT_TEST_JOBS to be rejected' >&2
  exit 1
fi
grep -q 'invalid int value' "$TMP/invalid-test-jobs-env.out"

# Test execution stops at the first failed binary boundary. The failed binary
# has already completed supervisor cleanup, the next binary never launches,
# and both structured outputs remain valid and actionable.
cat >"$TMP/first-test-binary" <<'EOF'
#!/bin/sh
printf 'first\n' >> "$PREFLIGHT_TEST_LAUNCH_LOG"
exit 23
EOF
cat >"$TMP/second-test-binary" <<'EOF'
#!/bin/sh
printf 'second\n' >> "$PREFLIGHT_TEST_LAUNCH_LOG"
exit 0
EOF
chmod +x "$TMP/first-test-binary" "$TMP/second-test-binary"
FAIL_FAST_OUT="$TMP/fail-fast-timing"
if PREFLIGHT_CARGO_TEST_BINARIES=1 \
  PREFLIGHT_FIRST_TEST_BINARY="$TMP/first-test-binary" \
  PREFLIGHT_SECOND_TEST_BINARY="$TMP/second-test-binary" \
  PREFLIGHT_TEST_LAUNCH_LOG="$TMP/test-launches.log" \
  PATH="$BIN:$PATH" scripts/preflight/timing.sh \
    --skip-fmt --skip-clippy --test-jobs 1 --out "$FAIL_FAST_OUT" \
    >"$TMP/fail-fast.out" 2>&1; then
  echo 'expected first test binary failure to reject timing run' >&2
  exit 1
fi
printf 'first\n' >"$TMP/test-launches.expected"
cmp "$TMP/test-launches.expected" "$TMP/test-launches.log"
python3 - "$FAIL_FAST_OUT/timing.json" <<'PY'
import json
import sys

data = json.load(open(sys.argv[1]))
assert data["version"] == 3
assert data["gates"][-1]["name"] == "test_execute"
assert data["gates"][-1]["exit_code"] == 23
assert data["first_failure"]["phase"] == "test_execute"
assert data["first_failure"]["target_name"] == "first_binary"
assert data["first_failure"]["exit_code"] == 23
assert data["first_failure"]["cleanup_complete"] is True
assert data["first_failure"]["rerun_command"] == (
    "cargo test --manifest-path /fixture/Cargo.toml --all-features "
    "--features quorum-core/test-support --test first_binary -- "
    "--test-threads 4 --nocapture"
)
assert data["test_binaries"][0]["execute_outcome"] == "failed"
assert "execute_outcome" not in data["test_binaries"][1]
PY
grep -q '^FIRST FAILURE:$' "$FAIL_FAST_OUT/summary.txt"
grep -q '^  test_binary: first_binary$' "$FAIL_FAST_OUT/summary.txt"
grep -Fq \
  'rerun: cargo test --manifest-path /fixture/Cargo.toml --all-features --features quorum-core/test-support --test first_binary -- --test-threads 4 --nocapture' \
  "$FAIL_FAST_OUT/summary.txt"
grep -q 'preflight timing: FIRST FAILURE: test binary' "$TMP/fail-fast.out"

# With two jobs, both initial binaries must run concurrently. The first waits
# for the second to start before failing; fail-fast then cancels and reaps the
# second supervisor tree, while the unscheduled third binary never launches.
cat >"$TMP/concurrent-first-test-binary" <<'EOF'
#!/bin/sh
printf 'first\n' >> "$PREFLIGHT_TEST_LAUNCH_LOG"
while [ ! -e "$PREFLIGHT_SECOND_READY" ]; do sleep 0.01; done
exit 23
EOF
cat >"$TMP/concurrent-second-test-binary" <<'EOF'
#!/bin/sh
printf 'second\n' >> "$PREFLIGHT_TEST_LAUNCH_LOG"
: > "$PREFLIGHT_SECOND_READY"
sleep 30
EOF
cat >"$TMP/concurrent-third-test-binary" <<'EOF'
#!/bin/sh
printf 'third\n' >> "$PREFLIGHT_TEST_LAUNCH_LOG"
exit 0
EOF
chmod +x "$TMP"/concurrent-*-test-binary
CONCURRENT_OUT="$TMP/concurrent-fail-fast-timing"
if PREFLIGHT_CARGO_TEST_BINARIES=1 \
  PREFLIGHT_FIRST_TEST_BINARY="$TMP/concurrent-first-test-binary" \
  PREFLIGHT_SECOND_TEST_BINARY="$TMP/concurrent-second-test-binary" \
  PREFLIGHT_THIRD_TEST_BINARY="$TMP/concurrent-third-test-binary" \
  PREFLIGHT_TEST_LAUNCH_LOG="$TMP/concurrent-test-launches.log" \
  PREFLIGHT_SECOND_READY="$TMP/concurrent-second-ready" \
  PATH="$BIN:$PATH" scripts/preflight/timing.sh \
    --skip-fmt --skip-clippy --test-jobs 2 --test-timeout-secs 5 \
    --term-grace-secs 0.1 --out "$CONCURRENT_OUT" \
    >"$TMP/concurrent-fail-fast.out" 2>&1; then
  echo 'expected concurrent first test binary failure to reject timing run' >&2
  exit 1
fi
sort "$TMP/concurrent-test-launches.log" >"$TMP/concurrent-launches.sorted"
printf 'first\nsecond\n' >"$TMP/concurrent-launches.expected"
cmp "$TMP/concurrent-launches.expected" "$TMP/concurrent-launches.sorted"
python3 - "$CONCURRENT_OUT/timing.json" <<'PY'
import json
import sys

data = json.load(open(sys.argv[1]))
assert data["test_jobs"] == 2
assert data["first_failure"]["target_name"] == "first_binary"
assert data["first_failure"]["exit_code"] == 23
first, second, third = data["test_binaries"]
assert first["execute_outcome"] == "failed"
assert second["execute_outcome"] == "owner_lost"
assert second["execute_cancelled_by_fail_fast"] is True
assert second["cleanup"]["complete"] is True
assert "execute_outcome" not in third
PY

# Cargo's compile/no-run diagnostics are captured by the structured collector,
# then replayed by preflight before it returns the same failure status as the
# former direct `cargo test` gate.
if PREFLIGHT_CARGO_FAIL=compile PATH="$BIN:$PATH" ./preflight.sh \
  >"$TMP/full-compile-failure.out" 2>&1; then
  echo 'expected compile/no-run failure to reject full preflight' >&2
  exit 1
fi
grep -q 'forced cargo test diagnostic' "$TMP/full-compile-failure.out"
grep -q 'PREFLIGHT: FAIL (cargo test)' "$TMP/full-compile-failure.out"

# CI uses the same collector so it compiles once with the explicit test-support
# feature, waits for cheap mechanical jobs, and preserves a four-thread total
# budget while exercising binary-level concurrency.
grep -Fqx '    needs: [fmt, clippy, shell-tests]' \
  "$ROOT/.github/workflows/ci.yml"
grep -Fqx '          --test-jobs 2' "$ROOT/.github/workflows/ci.yml"
grep -Fqx '          --test-threads 2' "$ROOT/.github/workflows/ci.yml"
grep -Fqx '          --out target/ci-test-timing' \
  "$ROOT/.github/workflows/ci.yml"

echo 'test-preflight: PASS'
