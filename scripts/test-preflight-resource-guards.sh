#!/bin/sh
# Regression tests for local preflight resource guards.

set -eu

ROOT=$(git rev-parse --show-toplevel)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

REMOTE="$TMP/origin.git"
REPO="$TMP/repo"
BIN="$TMP/bin"
REAL_GIT=$(command -v git)
git init --bare -q "$REMOTE"
git init -q -b main "$REPO"
mkdir -p "$BIN" "$REPO/scripts/preflight" "$REPO/docker"

cat >"$BIN/cargo" <<'EOF'
#!/bin/sh
[ -z "${PREFLIGHT_CARGO_LOG:-}" ] || printf '%s\n' "$*" >>"$PREFLIGHT_CARGO_LOG"
exit 0
EOF
cat >"$BIN/git" <<EOF
#!/bin/sh
if [ "\${PREFLIGHT_FAIL_CONTAINER_DIFF:-0}" = 1 ]; then
  for argument in "\$@"; do
    [ "\$argument" != --name-only ] || exit 9
  done
fi
exec "$REAL_GIT" "\$@"
EOF
cat >"$BIN/docker" <<'EOF'
#!/bin/sh
printf 'docker %s\n' "$*" >>"${PREFLIGHT_DOCKER_LOG:?}"
exit 0
EOF
cat >"$BIN/sqlite3" <<'EOF'
#!/bin/sh
exit 0
EOF
chmod +x "$BIN/cargo" "$BIN/docker" "$BIN/git" "$BIN/sqlite3"

cp "$ROOT/preflight.sh" "$REPO/preflight.sh"
chmod +x "$REPO/preflight.sh"
cat >"$REPO/scripts/preflight/timing.sh" <<'EOF'
#!/bin/sh
set -eu

if [ -n "${PREFLIGHT_TIMING_STARTED:-}" ]; then
  : >"$PREFLIGHT_TIMING_STARTED"
fi
if [ -n "${PREFLIGHT_TIMING_RELEASE:-}" ]; then
  tries=0
  while [ ! -e "$PREFLIGHT_TIMING_RELEASE" ]; do
    tries=$((tries + 1))
    [ "$tries" -lt 200 ] || {
      printf 'timed out waiting for preflight test release\n' >&2
      exit 1
    }
    sleep 0.05
  done
fi

mkdir -p target/preflight-timing
cat >target/preflight-timing/timing.json <<'JSON'
{
  "gates": [],
  "test_binaries": []
}
JSON
cat >target/preflight-timing/summary.txt <<'SUMMARY'
preflight timing summary
gates:
SUMMARY
EOF
chmod +x "$REPO/scripts/preflight/timing.sh"

cat >"$REPO/Dockerfile" <<'EOF'
FROM scratch
EOF
cat >"$REPO/.dockerignore" <<'EOF'
target
EOF
cat >"$REPO/.gitignore" <<'EOF'
/target
/docker/generated.conf
EOF
cat >"$REPO/docker/entrypoint_test.sh" <<'EOF'
#!/bin/sh
printf 'entrypoint\n' >>"${PREFLIGHT_DOCKER_LOG:?}"
EOF
cat >"$REPO/docker/verify.sh" <<'EOF'
#!/bin/sh
printf 'verify %s\n' "$1" >>"${PREFLIGHT_DOCKER_LOG:?}"
EOF
chmod +x "$REPO/docker/entrypoint_test.sh" "$REPO/docker/verify.sh"

git -C "$REPO" config user.name CI
git -C "$REPO" config user.email ci@example.invalid
git -C "$REPO" remote add origin "$REMOTE"
git -C "$REPO" add .
git -C "$REPO" commit -qm 'resource guard fixture'
git -C "$REPO" push -q -u origin main
git -C "$REPO" switch -qc daemon/resource-ordinary-t1

# Ordinary product changes run the host entrypoint contract but defer the
# expensive cross-architecture image build to required CI.
mkdir -p "$REPO/quorum/src"
printf 'pub fn ordinary_change() {}\n' >"$REPO/quorum/src/change.rs"
: >"$TMP/docker.log"
(
  cd "$REPO"
  PREFLIGHT_DOCKER_LOG="$TMP/docker.log" PATH="$BIN:$PATH" ./preflight.sh
) >"$TMP/ordinary.out"
grep -q 'skipping linux/amd64 container build — no container-specific changes' \
  "$TMP/ordinary.out"
grep -qx 'entrypoint' "$TMP/docker.log"
! grep -q '^docker ' "$TMP/docker.log"
! grep -q '^verify ' "$TMP/docker.log"

# A container-specific change runs the exact local image build and verifier.
printf '# packaging change\n' >>"$REPO/Dockerfile"
: >"$TMP/docker.log"
(
  cd "$REPO"
  PREFLIGHT_DOCKER_LOG="$TMP/docker.log" PATH="$BIN:$PATH" ./preflight.sh
) >"$TMP/container-change.out"
grep -qx 'docker buildx version' "$TMP/docker.log"
grep -qx 'docker info' "$TMP/docker.log"
grep -q '^docker buildx build --load --platform linux/amd64 --tag quorum-preflight:' \
  "$TMP/docker.log"
grep -q '^verify quorum-preflight:' "$TMP/docker.log"
grep -q '^docker image rm quorum-preflight:' "$TMP/docker.log"
grep -q 'PREFLIGHT: PASS (all 6 gates green;' "$TMP/container-change.out"

# Both other documented container-specific surfaces make the same decision.
git -C "$REPO" checkout -q -- Dockerfile
printf '# ignore change\n' >>"$REPO/.dockerignore"
: >"$TMP/docker.log"
(
  cd "$REPO"
  PREFLIGHT_DOCKER_LOG="$TMP/docker.log" PATH="$BIN:$PATH" ./preflight.sh
) >"$TMP/dockerignore-change.out"
grep -q '^docker buildx build --load --platform linux/amd64 ' "$TMP/docker.log"

git -C "$REPO" checkout -q -- .dockerignore
printf '# entrypoint test change\n' >>"$REPO/docker/entrypoint_test.sh"
: >"$TMP/docker.log"
(
  cd "$REPO"
  PREFLIGHT_DOCKER_LOG="$TMP/docker.log" PATH="$BIN:$PATH" ./preflight.sh
) >"$TMP/docker-path-change.out"
grep -q '^docker buildx build --load --platform linux/amd64 ' "$TMP/docker.log"

# Classification ambiguity fails closed to the container gate.
git -C "$REPO" checkout -q -- docker/entrypoint_test.sh
printf 'ignored container input\n' >"$REPO/docker/generated.conf"
: >"$TMP/docker.log"
(
  cd "$REPO"
  PREFLIGHT_DOCKER_LOG="$TMP/docker.log" PATH="$BIN:$PATH" ./preflight.sh
) >"$TMP/ignored-container-input.out"
grep -q '^docker buildx build --load --platform linux/amd64 ' "$TMP/docker.log"

rm "$REPO/docker/generated.conf"
: >"$TMP/docker.log"
(
  cd "$REPO"
  PREFLIGHT_FAIL_CONTAINER_DIFF=1 PREFLIGHT_DOCKER_LOG="$TMP/docker.log" \
    PATH="$BIN:$PATH" ./preflight.sh
) >"$TMP/classification-failure.out" 2>&1
grep -q 'cannot classify container-specific changes; running container gate' \
  "$TMP/classification-failure.out"
grep -q '^docker buildx build --load --platform linux/amd64 ' "$TMP/docker.log"

# Explicit opt-in bypasses the docs-only fast path and runs the container gate.
git -C "$REPO" checkout -q -- Dockerfile
rm "$REPO/quorum/src/change.rs"
rmdir "$REPO/quorum/src" "$REPO/quorum"
mkdir -p "$REPO/docs"
printf 'documentation only\n' >"$REPO/docs/change.md"
: >"$TMP/docker.log"
(
  cd "$REPO"
  PREFLIGHT_DOCKER_LOG="$TMP/docker.log" PATH="$BIN:$PATH" \
    ./preflight.sh --docker
) >"$TMP/forced.out"
grep -q '^docker buildx build --load --platform linux/amd64 ' "$TMP/docker.log"
grep -q 'PREFLIGHT: PASS (all 6 gates green;' "$TMP/forced.out"

# The same opt-in also overrides the now-green whole-tree cache.
: >"$TMP/docker.log"
(
  cd "$REPO"
  PREFLIGHT_DOCKER_LOG="$TMP/docker.log" PATH="$BIN:$PATH" \
    ./preflight.sh --docker
) >"$TMP/forced-cached.out"
grep -q '^docker buildx build --load --platform linux/amd64 ' "$TMP/docker.log"

if (cd "$REPO" && ./preflight.sh --quick --docker) \
  >"$TMP/conflict.out" 2>&1; then
  printf 'expected --quick --docker to fail\n' >&2
  exit 1
fi
grep -q -- '--docker conflicts with --quick' "$TMP/conflict.out"

# Linked worktrees share one local full-gate lock. Hold the first timing gate,
# prove the second has not entered its expensive suite, then release both.
WORKTREE_A="$TMP/worktree-a"
WORKTREE_B="$TMP/worktree-b"
git -C "$REPO" worktree add -q -b daemon/resource-lock-a-t2 \
  "$WORKTREE_A" origin/main
git -C "$REPO" worktree add -q -b daemon/resource-lock-b-t3 \
  "$WORKTREE_B" origin/main
for worktree in "$WORKTREE_A" "$WORKTREE_B"; do
  mkdir -p "$worktree/quorum/src"
  printf 'pub fn lock_change() {}\n' >"$worktree/quorum/src/lock.rs"
  git -C "$worktree" add quorum/src/lock.rs
  git -C "$worktree" commit -qm 'exercise local preflight lock' \
    -m 'Co-Authored-By: Resource-guard <resource-guard@example.invalid>'
done

RELEASE="$TMP/release-first"
(
  cd "$WORKTREE_A"
  CI= GITHUB_ACTIONS= PREFLIGHT_TIMING_STARTED="$TMP/first-started" \
    PREFLIGHT_TIMING_RELEASE="$RELEASE" PREFLIGHT_DOCKER_LOG="$TMP/first-docker.log" \
    PATH="$BIN:$PATH" ./preflight.sh
) >"$TMP/first.out" 2>&1 &
FIRST_PID=$!

tries=0
while [ ! -e "$TMP/first-started" ]; do
  tries=$((tries + 1))
  [ "$tries" -lt 100 ] || {
    printf 'first preflight did not reach timing gate\n' >&2
    exit 1
  }
  sleep 0.05
done

# Quick hooks and CI jobs do not queue behind a local full author gate.
(
  cd "$WORKTREE_B"
  CI= GITHUB_ACTIONS= PREFLIGHT_CARGO_LOG="$TMP/quick-cargo.log" \
    PATH="$BIN:$PATH" ./preflight.sh --quick
) >"$TMP/quick.out" 2>&1 &
QUICK_PID=$!
tries=0
while kill -0 "$QUICK_PID" 2>/dev/null; do
  tries=$((tries + 1))
  [ "$tries" -lt 100 ] || {
    : >"$RELEASE"
    printf 'quick preflight waited behind the full-gate lock\n' >&2
    exit 1
  }
  sleep 0.05
done
wait "$QUICK_PID"
grep -q '^fmt --all -- --check$' "$TMP/quick-cargo.log"

(
  cd "$WORKTREE_B"
  CI=true GITHUB_ACTIONS=true PREFLIGHT_TIMING_STARTED="$TMP/ci-started" \
    PREFLIGHT_DOCKER_LOG="$TMP/ci-docker.log" PATH="$BIN:$PATH" ./preflight.sh
) >"$TMP/ci.out" 2>&1 &
CI_PID=$!
tries=0
while [ ! -e "$TMP/ci-started" ]; do
  tries=$((tries + 1))
  [ "$tries" -lt 100 ] || {
    : >"$RELEASE"
    printf 'CI preflight waited behind the local full-gate lock\n' >&2
    exit 1
  }
  sleep 0.05
done
wait "$CI_PID"
printf '// invalidate CI cache\n' >>"$WORKTREE_B/quorum/src/lock.rs"

(
  cd "$WORKTREE_B"
  CI= GITHUB_ACTIONS= PREFLIGHT_TIMING_STARTED="$TMP/second-started" \
    PREFLIGHT_DOCKER_LOG="$TMP/second-docker.log" PATH="$BIN:$PATH" \
    ./preflight.sh
) >"$TMP/second.out" 2>&1 &
SECOND_PID=$!

tries=0
while ! grep -q 'waiting for another local full preflight' "$TMP/second.out" 2>/dev/null; do
  tries=$((tries + 1))
  [ "$tries" -lt 100 ] || {
    printf 'second preflight did not wait for the shared lock\n' >&2
    exit 1
  }
  sleep 0.05
done
[ ! -e "$TMP/second-started" ] || {
  printf 'second expensive suite started before the first completed\n' >&2
  exit 1
}

: >"$RELEASE"
wait "$FIRST_PID"
wait "$SECOND_PID"
[ -e "$TMP/second-started" ]

# A signalled owner releases the advisory lock. Launch it in an isolated
# process group, terminate that group, and prove a fresh successor reaches its
# timing gate instead of waiting on stale state.
printf '// invalidate first cache\n' >>"$WORKTREE_A/quorum/src/lock.rs"
printf '// invalidate second cache\n' >>"$WORKTREE_B/quorum/src/lock.rs"
python3 - "$WORKTREE_A" "$BIN" "$TMP" <<'PY' &
import os
import subprocess
import sys

worktree, binary_dir, temporary = sys.argv[1:]
environment = os.environ.copy()
environment.update({
    "CI": "",
    "GITHUB_ACTIONS": "",
    "PATH": binary_dir + os.pathsep + environment["PATH"],
    "PREFLIGHT_TIMING_STARTED": temporary + "/signal-started",
    "PREFLIGHT_TIMING_RELEASE": temporary + "/never-release",
    "PREFLIGHT_DOCKER_LOG": temporary + "/signal-docker.log",
})
with open(temporary + "/signal.out", "w", encoding="utf-8") as output:
    process = subprocess.Popen(
        ["./preflight.sh"], cwd=worktree, env=environment,
        stdout=output, stderr=subprocess.STDOUT, start_new_session=True,
    )
    with open(temporary + "/signal-shell-pid", "w", encoding="utf-8") as target:
        target.write(str(process.pid))
    returncode = process.wait()
with open(temporary + "/signal-result", "w", encoding="utf-8") as result:
    result.write(str(returncode))
PY
SIGNAL_LAUNCHER_PID=$!

tries=0
while [ ! -e "$TMP/signal-started" ] || [ ! -e "$TMP/signal-shell-pid" ]; do
  tries=$((tries + 1))
  [ "$tries" -lt 100 ] || {
    printf 'signalled preflight did not acquire the lock\n' >&2
    exit 1
  }
  sleep 0.05
done
SIGNAL_SHELL_PID=$(cat "$TMP/signal-shell-pid")
SIGNAL_WRAPPER_PID=$(ps -axo pid=,ppid= \
  | awk -v parent="$SIGNAL_SHELL_PID" '$2 == parent { print $1; exit }')
[ -n "$SIGNAL_WRAPPER_PID" ]
kill -TERM "$SIGNAL_WRAPPER_PID"
wait "$SIGNAL_LAUNCHER_PID"
[ "$(cat "$TMP/signal-result")" -eq 143 ]

(
  cd "$WORKTREE_B"
  CI= GITHUB_ACTIONS= PREFLIGHT_TIMING_STARTED="$TMP/successor-started" \
    PREFLIGHT_DOCKER_LOG="$TMP/successor-docker.log" PATH="$BIN:$PATH" \
    ./preflight.sh
) >"$TMP/successor.out" 2>&1
[ -e "$TMP/successor-started" ]

echo 'test-preflight-resource-guards: PASS'
