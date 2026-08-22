#!/bin/sh
# test-serve-supervisor.sh — shell tests for serve-supervisor.sh
#
# Creates a temp directory with stub binaries to test the supervisor's behavior
# without touching the real quorum binary or repo.
#
# Usage:
#   scripts/test-serve-supervisor.sh

set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
SUPERVISOR="$SCRIPT_DIR/serve-supervisor.sh"

PASS=0
FAIL=0
TESTS=0

pass() { PASS=$((PASS + 1)); TESTS=$((TESTS + 1)); printf '  PASS: %s\n' "$1"; }
fail() { FAIL=$((FAIL + 1)); TESTS=$((TESTS + 1)); printf '  FAIL: %s\n' "$1"; }

TMPDIR_TEST=$(mktemp -d)
cleanup() { rm -rf "$TMPDIR_TEST"; }
trap cleanup EXIT

make_stub_quorum() {
  cat > "$1" <<'STUB'
#!/bin/sh
# Reads exit codes from $QUORUM_TEST_SEQ, one per invocation
if [ ! -f "$QUORUM_TEST_SEQ" ]; then exit 1; fi
if [ -n "${QUORUM_TEST_ARGS:-}" ]; then
  printf '%s\n' "$*" >> "$QUORUM_TEST_ARGS"
fi
code=$(head -1 "$QUORUM_TEST_SEQ")
rest=$(tail -n +2 "$QUORUM_TEST_SEQ")
printf '%s\n' "$rest" > "$QUORUM_TEST_SEQ"
exit "$code"
STUB
  chmod +x "$1"
}

make_fake_repo() {
  git -C "$1" init -q 2>/dev/null
  git -C "$1" commit --allow-empty -m "init" -q 2>/dev/null
  git -C "$1" remote add origin "$1" 2>/dev/null || true
  git -C "$1" branch -M main 2>/dev/null || true
  git -C "$1" fetch origin main -q 2>/dev/null || true
}

# Run the supervisor with a stub quorum that exits with given codes in sequence.
# $1 = space-separated exit code sequence
# $2 = dev-install.sh exit code
# Remaining args: extra env vars as KEY=VALUE pairs
# Sets CAPTURED_EXIT and CAPTURED_OUTPUT.
capture_supervisor() {
  seq_codes="$1"
  dev_exit="$2"
  shift 2

  seq_file="$TMPDIR_TEST/seq_$$"
  for c in $seq_codes; do
    printf '%s\n' "$c"
  done > "$seq_file"

  repo_dir="$TMPDIR_TEST/repo_$$"
  mkdir -p "$repo_dir"
  make_fake_repo "$repo_dir"

  cat > "$repo_dir/dev-install.sh" <<DEVSTUB
#!/bin/sh
exit $dev_exit
DEVSTUB
  chmod +x "$repo_dir/dev-install.sh"

  stub_bin="$TMPDIR_TEST/stub-quorum"
  make_stub_quorum "$stub_bin"

  out_file="$TMPDIR_TEST/out_$$"
  args_file="$TMPDIR_TEST/args_$$"
  CAPTURED_EXIT=0

  QUORUM_REPO_DIR="$repo_dir" \
  QUORUM_SERVE_BIN="$stub_bin" \
  QUORUM_TEST_SEQ="$seq_file" \
  QUORUM_TEST_ARGS="$args_file" \
  "$@" \
  "$SUPERVISOR" > "$out_file" 2>&1 || CAPTURED_EXIT=$?

  CAPTURED_OUTPUT=$(cat "$out_file")

  rm -rf "$repo_dir" "$seq_file" "$out_file" "$args_file"
}

# === Tests ===

printf 'test-serve-supervisor: running tests\n\n'

# --- Test 0: contract alignment — SELF_UPDATE_EXIT_CODE matches daemon constant ---
printf 'test 0: contract alignment (SELF_UPDATE_EXIT_CODE = 75)\n'

script_code=$(grep '^SELF_UPDATE_EXIT_CODE=' "$SUPERVISOR" | head -1 | cut -d= -f2)
if [ "$script_code" = "75" ]; then pass "SELF_UPDATE_EXIT_CODE is 75"
else fail "SELF_UPDATE_EXIT_CODE is 75 (got $script_code)"; fi

# --- Test 1: non-75 exit propagates and stops ---
printf '\ntest 1: non-75 exit propagates\n'

capture_supervisor "0" "0"
if [ "$CAPTURED_EXIT" -eq 0 ]; then pass "exit 0 propagated"
else fail "exit 0 propagated (got $CAPTURED_EXIT)"; fi

capture_supervisor "1" "0"
if [ "$CAPTURED_EXIT" -eq 1 ]; then pass "exit 1 propagated"
else fail "exit 1 propagated (got $CAPTURED_EXIT)"; fi

capture_supervisor "2" "0"
if [ "$CAPTURED_EXIT" -eq 2 ]; then pass "exit 2 propagated"
else fail "exit 2 propagated (got $CAPTURED_EXIT)"; fi

capture_supervisor "3" "0"
if [ "$CAPTURED_EXIT" -eq 3 ]; then pass "exit 3 propagated"
else fail "exit 3 propagated (got $CAPTURED_EXIT)"; fi

# --- Test 2: exit 75 → rebuild → relaunch ---
printf '\ntest 2: exit 75 triggers rebuild then relaunch\n'

capture_supervisor "75 0" "0"
if [ "$CAPTURED_EXIT" -eq 0 ]; then pass "relaunched after exit 75 and exited cleanly"
else fail "relaunched after exit 75 (got exit $CAPTURED_EXIT)"; fi

if printf '%s' "$CAPTURED_OUTPUT" | grep -q "self-update drain handoff"; then
  pass "logged self-update drain handoff message"
else fail "logged self-update drain handoff message"; fi

if printf '%s' "$CAPTURED_OUTPUT" | grep -q "rebuild OK"; then
  pass "logged rebuild OK"
else fail "logged rebuild OK"; fi

# --- Test 3: exit 75 + build failure → alert + relaunch old binary ---
printf '\ntest 3: build failure relaunches old binary with alert\n'

capture_supervisor "75 0" "1"
if [ "$CAPTURED_EXIT" -eq 0 ]; then pass "relaunched old binary after build failure"
else fail "relaunched old binary after build failure (got exit $CAPTURED_EXIT)"; fi

if printf '%s' "$CAPTURED_OUTPUT" | grep -q "SUPERVISOR ALERT.*build FAILED"; then
  pass "alerted on build failure"
else fail "alerted on build failure"; fi

# --- Test 4: multiple exit-75 cycles ---
printf '\ntest 4: multiple exit-75 cycles work\n'

capture_supervisor "75 75 75 0" "0"
if [ "$CAPTURED_EXIT" -eq 0 ]; then pass "survived 3 successive exit-75 restarts"
else fail "survived 3 successive exit-75 restarts (got exit $CAPTURED_EXIT)"; fi

# --- Test 5: thrash guard trips at threshold ---
printf '\ntest 5: thrash guard (max=6)\n'

# 7 exit-75s — the 7th triggers the guard (6 restarts already recorded)
capture_supervisor "75 75 75 75 75 75 75 0" "0" \
  env QUORUM_THRASH_MAX=6 QUORUM_THRASH_WINDOW=3600
if [ "$CAPTURED_EXIT" -eq 75 ]; then pass "thrash guard stopped restarts at max"
else fail "thrash guard stopped restarts at max (got exit $CAPTURED_EXIT)"; fi

if printf '%s' "$CAPTURED_OUTPUT" | grep -q "thrash guard"; then
  pass "thrash guard alert logged"
else fail "thrash guard alert logged"; fi

# --- Test 6: thrash guard with lower threshold ---
printf '\ntest 6: thrash guard (max=2)\n'

capture_supervisor "75 75 75 0" "0" \
  env QUORUM_THRASH_MAX=2 QUORUM_THRASH_WINDOW=3600
if [ "$CAPTURED_EXIT" -eq 75 ]; then pass "thrash guard trips at max=2"
else fail "thrash guard trips at max=2 (got exit $CAPTURED_EXIT)"; fi

# --- Test 7: build failure then success on next cycle ---
printf '\ntest 7: build failure then success on next cycle\n'

seq_file="$TMPDIR_TEST/seq_t7"
printf '75\n75\n0\n' > "$seq_file"

repo_dir="$TMPDIR_TEST/repo_t7"
mkdir -p "$repo_dir"
make_fake_repo "$repo_dir"

counter_file="$TMPDIR_TEST/counter_t7"
rm -f "$counter_file"

cat > "$repo_dir/dev-install.sh" <<COUNTERSTUB
#!/bin/sh
if [ -f "$counter_file" ]; then
  count=\$(cat "$counter_file")
else
  count=0
fi
count=\$((count + 1))
printf '%s' "\$count" > "$counter_file"
if [ "\$count" -eq 1 ]; then exit 1; fi
exit 0
COUNTERSTUB
chmod +x "$repo_dir/dev-install.sh"

stub_bin="$TMPDIR_TEST/stub-quorum"
make_stub_quorum "$stub_bin"

out_file="$TMPDIR_TEST/out_t7"
CAPTURED_EXIT=0
QUORUM_REPO_DIR="$repo_dir" \
QUORUM_SERVE_BIN="$stub_bin" \
QUORUM_TEST_SEQ="$seq_file" \
"$SUPERVISOR" > "$out_file" 2>&1 || CAPTURED_EXIT=$?

CAPTURED_OUTPUT=$(cat "$out_file")

if [ "$CAPTURED_EXIT" -eq 0 ]; then pass "recovered after initial build failure"
else fail "recovered after initial build failure (got exit $CAPTURED_EXIT)"; fi

if printf '%s' "$CAPTURED_OUTPUT" | grep -q "build FAILED"; then
  pass "alerted on first build failure"
else fail "alerted on first build failure"; fi

if printf '%s' "$CAPTURED_OUTPUT" | grep -q "rebuild OK"; then
  pass "logged rebuild OK on second attempt"
else fail "logged rebuild OK on second attempt"; fi

rm -rf "$repo_dir" "$seq_file" "$out_file" "$counter_file"

# --- Test 8: SIGTERM forwarded to child; both exit ---
printf '\ntest 8: SIGTERM forwarded to child\n'

seq_file="$TMPDIR_TEST/seq_t8"
: > "$seq_file"

repo_dir="$TMPDIR_TEST/repo_t8"
mkdir -p "$repo_dir"
make_fake_repo "$repo_dir"
cat > "$repo_dir/dev-install.sh" <<'STUB'
#!/bin/sh
exit 0
STUB
chmod +x "$repo_dir/dev-install.sh"

long_stub="$TMPDIR_TEST/stub-quorum-long"
child_pid_file="$TMPDIR_TEST/child_pid_t8"
cat > "$long_stub" <<LONGSTUB
#!/bin/sh
printf '%s' "\$\$" > "$child_pid_file"
trap 'exit 0' TERM
while true; do sleep 1; done
LONGSTUB
chmod +x "$long_stub"

out_file="$TMPDIR_TEST/out_t8"
QUORUM_REPO_DIR="$repo_dir" \
QUORUM_SERVE_BIN="$long_stub" \
QUORUM_TEST_SEQ="$seq_file" \
"$SUPERVISOR" > "$out_file" 2>&1 &
supervisor_pid=$!

attempts=0
while [ ! -f "$child_pid_file" ] && [ "$attempts" -lt 20 ]; do
  sleep 0.1
  attempts=$((attempts + 1))
done

if [ -f "$child_pid_file" ]; then
  child_pid=$(cat "$child_pid_file")

  kill -TERM "$supervisor_pid" 2>/dev/null
  sup_exit=0
  wait "$supervisor_pid" 2>/dev/null || sup_exit=$?

  if [ "$sup_exit" -eq 143 ]; then pass "supervisor exited 143 on SIGTERM"
  else fail "supervisor exited 143 on SIGTERM (got $sup_exit)"; fi

  if ! kill -0 "$child_pid" 2>/dev/null; then pass "child process exited after SIGTERM"
  else
    fail "child process exited after SIGTERM (still running)"
    kill -9 "$child_pid" 2>/dev/null || true
  fi
else
  fail "supervisor exited 143 on SIGTERM (child never started)"
  fail "child process exited after SIGTERM (child never started)"
  kill "$supervisor_pid" 2>/dev/null || true
  wait "$supervisor_pid" 2>/dev/null || true
fi

rm -rf "$repo_dir" "$seq_file" "$out_file" "$child_pid_file" "$long_stub"

# --- Test 9: self-update branch controls fast-forward and daemon polling ---
printf '\ntest 9: self-update branch controls fast-forward and daemon polling\n'

repo_dir="$TMPDIR_TEST/repo_t10"
mkdir -p "$repo_dir"
git -C "$repo_dir" init -q
git -C "$repo_dir" commit --allow-empty -m "init" -q
git -C "$repo_dir" branch -M main

bare_dir="$TMPDIR_TEST/bare_t10"
git clone --bare -q "$repo_dir" "$bare_dir" 2>/dev/null
git -C "$repo_dir" remote add origin "$bare_dir" 2>/dev/null || true
git -C "$repo_dir" fetch origin main -q 2>/dev/null

git -C "$repo_dir" checkout -b self-update -q 2>/dev/null
git -C "$repo_dir" commit --allow-empty -m "advance" -q
git -C "$repo_dir" push origin self-update -q 2>/dev/null
git -C "$repo_dir" checkout main -q 2>/dev/null
pre_sha=$(git -C "$repo_dir" rev-parse HEAD)
expected_sha=$(git -C "$repo_dir" rev-parse origin/self-update)

seq_file="$TMPDIR_TEST/seq_t10"
printf '75\n0\n' > "$seq_file"

cat > "$repo_dir/dev-install.sh" <<'STUB'
#!/bin/sh
exit 0
STUB
chmod +x "$repo_dir/dev-install.sh"

stub_bin="$TMPDIR_TEST/stub-quorum"
make_stub_quorum "$stub_bin"

out_file="$TMPDIR_TEST/out_t10"
CAPTURED_EXIT=0
QUORUM_REPO_DIR="$repo_dir" \
QUORUM_SERVE_BIN="$stub_bin" \
QUORUM_TEST_SEQ="$seq_file" \
QUORUM_TEST_ARGS="$TMPDIR_TEST/args_t10" \
QUORUM_SELF_UPDATE_BRANCH=self-update \
QUORUM_BASE_BRANCH=ignored-legacy \
"$SUPERVISOR" > "$out_file" 2>&1 || CAPTURED_EXIT=$?

CAPTURED_OUTPUT=$(cat "$out_file")
post_sha=$(git -C "$repo_dir" rev-parse HEAD)

if [ "$pre_sha" != "$post_sha" ] && [ "$post_sha" = "$expected_sha" ]; then pass "self-update branch fast-forwarded"
else fail "self-update branch fast-forwarded (got $post_sha)"; fi

if grep -qx 'serve --self-update-branch self-update' "$TMPDIR_TEST/args_t10"; then
  pass "daemon staleness branch matches self-update branch"
else fail "daemon staleness branch matches self-update branch"; fi

if printf '%s' "$CAPTURED_OUTPUT" | grep -q "rebuild OK"; then
  pass "rebuild succeeded after fast-forward"
else fail "rebuild succeeded after fast-forward"; fi

rm -rf "$repo_dir" "$bare_dir" "$seq_file" "$out_file" "$TMPDIR_TEST/args_t10"

# --- Test 10: QUORUM_BASE_BRANCH remains the legacy self-update fallback ---
printf '\ntest 10: legacy base-branch override selects self-update branch\n'

repo_dir="$TMPDIR_TEST/repo_t10"
mkdir -p "$repo_dir"
git -C "$repo_dir" init -q
git -C "$repo_dir" commit --allow-empty -m "init" -q
git -C "$repo_dir" branch -M main

bare_dir="$TMPDIR_TEST/bare_t10"
git clone --bare -q "$repo_dir" "$bare_dir" 2>/dev/null
git -C "$repo_dir" remote add origin "$bare_dir" 2>/dev/null || true
git -C "$repo_dir" fetch origin main -q 2>/dev/null

git -C "$repo_dir" checkout -b legacy-base -q 2>/dev/null
git -C "$repo_dir" commit --allow-empty -m "legacy advance" -q
git -C "$repo_dir" push origin legacy-base -q 2>/dev/null
git -C "$repo_dir" checkout main -q 2>/dev/null
expected_sha=$(git -C "$repo_dir" rev-parse origin/legacy-base)

seq_file="$TMPDIR_TEST/seq_t10"
printf '75\n0\n' > "$seq_file"

cat > "$repo_dir/dev-install.sh" <<'STUB'
#!/bin/sh
exit 0
STUB
chmod +x "$repo_dir/dev-install.sh"

stub_bin="$TMPDIR_TEST/stub-quorum"
make_stub_quorum "$stub_bin"

out_file="$TMPDIR_TEST/out_t10"
CAPTURED_EXIT=0
QUORUM_REPO_DIR="$repo_dir" \
QUORUM_SERVE_BIN="$stub_bin" \
QUORUM_TEST_SEQ="$seq_file" \
QUORUM_TEST_ARGS="$TMPDIR_TEST/args_t10" \
QUORUM_BASE_BRANCH=legacy-base \
"$SUPERVISOR" > "$out_file" 2>&1 || CAPTURED_EXIT=$?

post_sha=$(git -C "$repo_dir" rev-parse HEAD)
if [ "$CAPTURED_EXIT" -eq 0 ] && [ "$post_sha" = "$expected_sha" ]; then pass "legacy base branch fast-forwarded"
else fail "legacy base branch fast-forwarded"; fi

if grep -qx 'serve --self-update-branch legacy-base' "$TMPDIR_TEST/args_t10"; then
  pass "legacy branch also configures daemon staleness"
else fail "legacy branch also configures daemon staleness"; fi

rm -rf "$repo_dir" "$bare_dir" "$seq_file" "$out_file" "$TMPDIR_TEST/args_t10"

# --- Test 11: fast-forward merge failure alerts and relaunches old binary ---
printf '\ntest 11: fast-forward merge failure alerts and continues\n'

repo_dir="$TMPDIR_TEST/repo_t10b"
mkdir -p "$repo_dir"
git -C "$repo_dir" init -q
git -C "$repo_dir" commit --allow-empty -m "init" -q
git -C "$repo_dir" branch -M main

bare_dir="$TMPDIR_TEST/bare_t10b"
git clone --bare -q "$repo_dir" "$bare_dir" 2>/dev/null
git -C "$repo_dir" remote add origin "$bare_dir" 2>/dev/null || true
git -C "$repo_dir" fetch origin main -q 2>/dev/null

# Create a divergent history that can't fast-forward
git -C "$repo_dir" commit --allow-empty -m "local diverge" -q
git -C "$repo_dir" checkout -b tmp-div -q 2>/dev/null
git -C "$repo_dir" reset --hard HEAD~1 -q
git -C "$repo_dir" commit --allow-empty -m "remote diverge" -q
git -C "$repo_dir" push origin tmp-div:main -f -q 2>/dev/null
git -C "$repo_dir" checkout main -q 2>/dev/null

seq_file="$TMPDIR_TEST/seq_t10b"
printf '75\n0\n' > "$seq_file"

cat > "$repo_dir/dev-install.sh" <<'STUB'
#!/bin/sh
exit 0
STUB
chmod +x "$repo_dir/dev-install.sh"

stub_bin="$TMPDIR_TEST/stub-quorum"
make_stub_quorum "$stub_bin"

out_file="$TMPDIR_TEST/out_t10b"
CAPTURED_EXIT=0
QUORUM_REPO_DIR="$repo_dir" \
QUORUM_SERVE_BIN="$stub_bin" \
QUORUM_TEST_SEQ="$seq_file" \
"$SUPERVISOR" > "$out_file" 2>&1 || CAPTURED_EXIT=$?

CAPTURED_OUTPUT=$(cat "$out_file")

if [ "$CAPTURED_EXIT" -eq 0 ]; then pass "continued after merge failure"
else fail "continued after merge failure (got exit $CAPTURED_EXIT)"; fi

if printf '%s' "$CAPTURED_OUTPUT" | grep -q "SUPERVISOR ALERT.*fast-forward merge failed"; then
  pass "alerted on merge failure"
else fail "alerted on merge failure"; fi

rm -rf "$repo_dir" "$bare_dir" "$seq_file" "$out_file"

# === Summary ===
printf '\n--- Results: %s tests, %s passed, %s failed ---\n' "$TESTS" "$PASS" "$FAIL"

if [ "$FAIL" -gt 0 ]; then
  exit 1
fi
exit 0
