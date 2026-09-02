#!/bin/sh
# Host-side contract tests for docker/entrypoint.sh. No Docker required.
set -u

HERE=$(cd "$(dirname "$0")" && pwd)
ENTRY=$HERE/entrypoint.sh
BASE_PATH=$PATH
REAL_GIT=$(command -v git)
FAILS=0

setup() {
  WORK=$(mktemp -d)
  mkdir -p "$WORK/bin" "$WORK/repo" "$WORK/home" "$WORK/wt" "$WORK/logs"
  "$REAL_GIT" -C "$WORK/repo" init -q
  cp "$HERE/serve-codex.toml" "$WORK/serve-codex.toml"

  cat >"$WORK/bin/git" <<EOF
#!/bin/sh
if [ "\${1:-}" = -C ]; then exec "$REAL_GIT" "\$@"; fi
if [ "\${1:-}" = ls-remote ]; then
  if [ "\${FAKE_HTTP_READY:-1}" = 1 ]; then
    printf "fatal: repository 'http://127.0.0.1:%s/' not found\\n" "\${FAKE_WEB_PORT:-8080}" >&2
  else
    printf "fatal: unable to access Web\\n" >&2
  fi
  exit 128
fi
exec "$REAL_GIT" "\$@"
EOF

  cat >"$WORK/bin/quorum" <<'EOF'
#!/bin/sh
command=${1:-}
shift || true
case "$command" in
  init)
    printf 'init %s\n' "$PWD" >>"${FAKE_CALLS:?}"
    sleep "${FAKE_INIT_SLEEP:-0}"
    exit "${FAKE_INIT_CODE:-0}"
    ;;
  serve)
    printf 'serve %s\n' "$*" >>"${FAKE_CALLS:?}"
    if [ "${FAKE_SERVE_IGNORE_TERM:-0}" = 1 ]; then
      trap '' TERM
      sh -c 'trap "" TERM; while :; do :; done' & descendant=$!
      printf '%s\n' "$descendant" >"${FAKE_DESCENDANT_PID:?}"
      while :; do :; done
    fi
    sleep "${FAKE_SERVE_SLEEP:-30}" & child=$!
    trap 'kill -TERM "$child" 2>/dev/null; wait "$child" 2>/dev/null; exit "${FAKE_SERVE_CODE:-0}"' TERM
    wait "$child" 2>/dev/null
    exit "${FAKE_SERVE_CODE:-0}"
    ;;
  web)
    printf 'web %s\n' "$*" >>"${FAKE_CALLS:?}"
    sleep "${FAKE_WEB_SLEEP:-30}" & child=$!
    trap 'kill -TERM "$child" 2>/dev/null; wait "$child" 2>/dev/null; exit "${FAKE_WEB_CODE:-0}"' TERM
    wait "$child" 2>/dev/null
    exit "${FAKE_WEB_CODE:-0}"
    ;;
  status)
    if [ "${FAKE_READY:-1}" = 1 ]; then
      printf '{"daemon":{"Alive":{"pid":42}}}\n'
    else
      printf '{"daemon":"None"}\n'
    fi
    ;;
esac
EOF
  chmod +x "$WORK/bin/git" "$WORK/bin/quorum"

  PATH="$WORK/bin:$BASE_PATH"
  QUORUM_REPO=acme/widget
  QUORUM_HOME="$WORK/home"
  QUORUM_REPO_DIR="$WORK/repo"
  QUORUM_WORKTREE_BASE="$WORK/wt"
  QUORUM_LOG_DIR="$WORK/logs"
  QUORUM_SERVE_TEMPLATE="$WORK/serve-codex.toml"
  QUORUM_READY_TRIES=3
  QUORUM_SHUTDOWN_TRIES=1
  FAKE_CALLS="$WORK/calls"
  FAKE_DESCENDANT_PID="$WORK/descendant.pid"
  export PATH QUORUM_REPO QUORUM_HOME QUORUM_REPO_DIR QUORUM_WORKTREE_BASE
  export QUORUM_LOG_DIR QUORUM_SERVE_TEMPLATE QUORUM_READY_TRIES
  export QUORUM_SHUTDOWN_TRIES FAKE_CALLS FAKE_DESCENDANT_PID
  unset FAKE_INIT_CODE FAKE_INIT_SLEEP FAKE_SERVE_CODE FAKE_SERVE_SLEEP
  unset FAKE_SERVE_IGNORE_TERM FAKE_WEB_CODE FAKE_WEB_SLEEP FAKE_READY
  unset FAKE_HTTP_READY
  unset QUORUM_SERVE_CONFIG QUORUM_SELF_UPDATE_DRAIN QUORUM_WEB_PORT
}

teardown() {
  if [ -f "$FAKE_DESCENDANT_PID" ]; then
    descendant=$(cat "$FAKE_DESCENDANT_PID")
    kill -KILL "$descendant" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}

check() {
  label=$1 expected=$2 actual=$3
  if [ "$expected" = "$actual" ]; then
    printf 'ok: %s\n' "$label"
  else
    printf 'FAIL: %s (expected %s, got %s)\n' "$label" "$expected" "$actual" >&2
    FAILS=$((FAILS + 1))
  fi
}

wait_for_call() {
  pattern=$1
  tries=0
  while ! grep -q "$pattern" "$FAKE_CALLS" 2>/dev/null; do
    tries=$((tries + 1))
    [ "$tries" -lt 30 ] || return 1
    sleep 0.1
  done
}

wait_bounded() {
  pid=$1
  tries=0
  while kill -0 "$pid" 2>/dev/null; do
    tries=$((tries + 1))
    [ "$tries" -lt 80 ] || return 1
    sleep 0.1
  done
}

# Missing identity and checkout validation fail before initialization.
setup
unset QUORUM_REPO
sh "$ENTRY" >/dev/null 2>&1
check 'missing identity' 1 "$?"
teardown

setup
rm -rf "$QUORUM_REPO_DIR/.git"
sh "$ENTRY" >/dev/null 2>&1
check 'missing checkout metadata' 1 "$?"
teardown

setup
rm -rf "$QUORUM_REPO_DIR/.git"
mkdir "$QUORUM_REPO_DIR/.git"
sh "$ENTRY" >/dev/null 2>&1
check 'invalid checkout metadata' 1 "$?"
teardown

# Fresh state installs Codex-only routing, init runs outside the checkout, and
# both children receive explicit persistent paths. Serve 0 is exact.
setup
FAKE_SERVE_SLEEP=1 FAKE_SERVE_CODE=0 FAKE_WEB_SLEEP=30 \
  sh "$ENTRY" >/dev/null 2>&1
check 'serve exit 0' 0 "$?"
config="$QUORUM_HOME/serve/acme__widget.toml"
check 'fresh config exists' yes "$(test -f "$config" && echo yes || echo no)"
check 'fresh config selects Codex' yes "$(grep -q 'runner = "codex"' "$config" && echo yes || echo no)"
check 'fresh config excludes Claude' no "$(grep -qi claude "$config" && echo yes || echo no)"
init_pwd=$(sed -n 's/^init //p' "$FAKE_CALLS")
check 'init outside managed checkout' "$QUORUM_HOME/init" "$init_pwd"
check 'serve and Web started' 'serve web ' "$(awk '$1 == "serve" || $1 == "web" { print $1 }' "$FAKE_CALLS" | sort | tr '\n' ' ')"
teardown

# Existing configuration is preserved byte-for-byte.
setup
mkdir -p "$QUORUM_HOME/serve"
printf 'operator-owned = true\n' >"$QUORUM_HOME/serve/acme__widget.toml"
before=$(cksum "$QUORUM_HOME/serve/acme__widget.toml")
FAKE_SERVE_SLEEP=1 FAKE_WEB_SLEEP=30 sh "$ENTRY" >/dev/null 2>&1
after=$(cksum "$QUORUM_HOME/serve/acme__widget.toml")
check 'existing config preserved' "$before" "$after"
teardown

# Init/config failures and readiness timeout are loud and start no unmanaged
# respawn loop. A serve-side config rejection is propagated exactly.
setup
FAKE_INIT_CODE=3 sh "$ENTRY" >/dev/null 2>&1
check 'init failure propagates' 3 "$?"
check 'init failure starts no children' 0 "$(grep -Ec '^(serve|web) ' "$FAKE_CALLS" 2>/dev/null || true)"
teardown

setup
QUORUM_SERVE_TEMPLATE="$WORK/missing-template.toml" sh "$ENTRY" >/dev/null 2>&1
check 'fresh config installation failure' 3 "$?"
init_calls=$(if [ -f "$FAKE_CALLS" ]; then grep -c '^init ' "$FAKE_CALLS" || true; else echo 0; fi)
check 'config installation failure runs no init' 0 "$init_calls"
teardown

setup
FAKE_SERVE_SLEEP=1 FAKE_SERVE_CODE=2 FAKE_READY=0 FAKE_WEB_SLEEP=30 \
  sh "$ENTRY" >/dev/null 2>&1
check 'serve config failure propagates' 2 "$?"
teardown

setup
QUORUM_READY_TRIES=1 FAKE_READY=1 FAKE_HTTP_READY=0 \
  FAKE_SERVE_SLEEP=30 FAKE_WEB_SLEEP=30 \
  sh "$ENTRY" >/dev/null 2>&1
check 'HTTP readiness timeout is nonzero' 1 "$?"
teardown

# A startup signal interrupts and reaps init without starting either service.
setup
FAKE_INIT_SLEEP=30 sh "$ENTRY" >/dev/null 2>&1 & entry_pid=$!
wait_for_call '^init '
kill -TERM "$entry_pid"
wait "$entry_pid" 2>/dev/null
check 'startup SIGTERM exit' 143 "$?"
check 'startup SIGTERM starts no children' 0 "$(grep -Ec '^(serve|web) ' "$FAKE_CALLS" 2>/dev/null || true)"
teardown

# A steady-state signal reaches both children and preserves serve's clean 0.
setup
FAKE_SERVE_SLEEP=30 FAKE_SERVE_CODE=0 FAKE_WEB_SLEEP=30 \
  sh "$ENTRY" >/dev/null 2>&1 & entry_pid=$!
wait_for_call '^serve '
wait_for_call '^web '
kill -TERM "$entry_pid"
wait "$entry_pid" 2>/dev/null
check 'steady-state SIGTERM propagates serve 0' 0 "$?"
teardown

# Web-first failure is documented exit 1. A TERM-ignoring serve and its
# TERM-ignoring descendant are KILLed and reaped within the configured bound.
setup
FAKE_SERVE_IGNORE_TERM=1 FAKE_WEB_SLEEP=1 FAKE_WEB_CODE=7 \
  sh "$ENTRY" >/dev/null 2>&1 & entry_pid=$!
if wait_bounded "$entry_pid"; then
  wait "$entry_pid" 2>/dev/null
  check 'Web-first failure' 1 "$?"
else
  check 'Web-first failure bounded' stopped running
  kill -KILL "$entry_pid" 2>/dev/null || true
fi
descendant=$(cat "$FAKE_DESCENDANT_PID")
check 'orphan descendant removed' no "$(kill -0 "$descendant" 2>/dev/null && echo yes || echo no)"
teardown

# Natural serve termination is authoritative for the container boundary.
for expected in 3 75; do
  setup
  FAKE_SERVE_SLEEP=1 FAKE_SERVE_CODE=$expected FAKE_WEB_SLEEP=30 \
    sh "$ENTRY" >/dev/null 2>&1
  check "serve exit $expected" "$expected" "$?"
  teardown
done

printf '%s\n' '---'
if [ "$FAILS" -eq 0 ]; then
  printf 'all entrypoint tests passed\n'
else
  printf '%s entrypoint tests failed\n' "$FAILS" >&2
  exit 1
fi
