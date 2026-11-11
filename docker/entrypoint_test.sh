#!/bin/sh
# Host-side contract tests for docker/entrypoint.sh. No Docker required.
set -u

HERE=$(cd "$(dirname "$0")" && pwd)
ENTRY=$HERE/entrypoint.sh
HEALTH=$HERE/health.sh
BASE_PATH=$PATH
REAL_GIT=$(command -v git)
FAILS=0

setup() {
  WORK=$(mktemp -d)
  mkdir -p "$WORK/bin" "$WORK/repo" "$WORK/home" "$WORK/wt" "$WORK/logs"
  "$REAL_GIT" -C "$WORK/repo" init -q
  cp "$HERE/serve-codex.toml" "$WORK/serve-codex.toml"

  if ! PATH=$BASE_PATH command -v timeout >/dev/null 2>&1; then
    # Host-only shim where GNU timeout is absent; the image ships coreutils.
    cat >"$WORK/bin/timeout" <<'EOF'
#!/bin/sh
[ "${1:-}" = -k ] && shift 2
shift
exec "$@"
EOF
    chmod +x "$WORK/bin/timeout"
  fi

  cat >"$WORK/bin/git" <<EOF
#!/bin/sh
if [ "\${1:-}" = -C ]; then exec "$REAL_GIT" "\$@"; fi
if [ "\${1:-}" = ls-remote ]; then
  if [ "\${FAKE_HTTP_READY:-1}" = 1 ]; then
    printf "fatal: repository 'http://127.0.0.1:%s/' not found\\n" "\${FAKE_WEB_PORT:-8080}" >&2
  else
    printf "fatal: unable to access '/secret/http-diagnostic'\\n" >&2
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
    if [ "${FAKE_INIT_CODE:-0}" = 0 ]; then
      db_dir="$QUORUM_HOME/repos/$(printf '%s' "$QUORUM_REPO" | sed 's#/#__#')"
      mkdir -p "$db_dir" && : >>"$db_dir/quorum.db"
    fi
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
    printf 'status\n' >>"${FAKE_CALLS:?}"
    case "${FAKE_READY:-1}" in
      1) daemon='{"Alive":{"pid":42,"heartbeat_age_secs":1}}' ;;
      stale) daemon='{"Stale":{"pid":42,"heartbeat_age_secs":99,"pid_dead":true}}' ;;
      *) daemon='"None"' ;;
    esac
    # Message bodies that spoof liveness and carry a private path must never
    # satisfy readiness or reach probe output.
    printf '{"agents_total":0,"recent_messages":[{"body":"Alive"},{"body":"/secret/status-path,\\"daemon\\":{\\"Alive\\":{"}],"daemon":%s,"resources":null}\n' "$daemon"
    exit "${FAKE_STATUS_CODE:-0}"
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
  QUORUM_HEALTH_PROBE="$HEALTH"
  QUORUM_RUN_DIR="$WORK/run"
  QUORUM_READY_TRIES=3
  QUORUM_SHUTDOWN_TRIES=1
  FAKE_CALLS="$WORK/calls"
  FAKE_DESCENDANT_PID="$WORK/descendant.pid"
  export PATH QUORUM_REPO QUORUM_HOME QUORUM_REPO_DIR QUORUM_WORKTREE_BASE
  export QUORUM_LOG_DIR QUORUM_SERVE_TEMPLATE QUORUM_READY_TRIES
  export QUORUM_SHUTDOWN_TRIES FAKE_CALLS FAKE_DESCENDANT_PID
  export QUORUM_HEALTH_PROBE QUORUM_RUN_DIR
  unset FAKE_INIT_CODE FAKE_INIT_SLEEP FAKE_SERVE_CODE FAKE_SERVE_SLEEP
  unset FAKE_SERVE_IGNORE_TERM FAKE_WEB_CODE FAKE_WEB_SLEEP FAKE_READY
  unset FAKE_HTTP_READY FAKE_STATUS_CODE
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

# Steady state: the supervisor records itself and both direct children, the
# out-of-band probe reports ready, and shutdown removes the record.
setup
FAKE_SERVE_SLEEP=30 FAKE_WEB_SLEEP=30 sh "$ENTRY" >/dev/null 2>&1 & entry_pid=$!
probe_out= probe_tries=0
while [ "$probe_tries" -lt 30 ]; do
  probe_out=$(sh "$HEALTH" readiness 2>&1) && break
  probe_tries=$((probe_tries + 1))
  sleep 0.1
done
check 'steady-state readiness probe' 'health: ready' "$probe_out"
check 'steady-state liveness probe' 'health: live' "$(sh "$HEALTH" liveness 2>&1)"
check 'record names supervisor' "supervisor=$entry_pid" \
  "$(grep '^supervisor=' "$QUORUM_RUN_DIR/supervisor.pids" 2>/dev/null)"
check 'record names repository' 'repo=acme/widget' \
  "$(grep '^repo=' "$QUORUM_RUN_DIR/supervisor.pids" 2>/dev/null)"
kill -TERM "$entry_pid"
wait "$entry_pid" 2>/dev/null
check 'shutdown removes runtime record' no \
  "$(test -e "$QUORUM_RUN_DIR/supervisor.pids" && echo yes || echo no)"
check 'probe after shutdown' 'health: not-ready check=runtime' \
  "$(sh "$HEALTH" readiness 2>&1)"
teardown

# A record left by an earlier start is cleared before init runs.
setup
mkdir -p "$QUORUM_RUN_DIR"
printf 'supervisor=%s\nserve=%s\nweb=%s\nrepo=acme/widget\n' $$ $$ $$ \
  >"$QUORUM_RUN_DIR/supervisor.pids"
# Observe while init is still running, before the EXIT cleanup could mask it.
FAKE_INIT_SLEEP=30 sh "$ENTRY" >/dev/null 2>&1 & entry_pid=$!
wait_for_call '^init '
check 'stale runtime record cleared before init' no \
  "$(test -e "$QUORUM_RUN_DIR/supervisor.pids" && echo yes || echo no)"
kill -TERM "$entry_pid"
wait "$entry_pid" 2>/dev/null
teardown

# Probe contract against a synthetic record of live processes. Each failure
# exits 1 and prints only its fixed check name.
probe_case() {
  probe_label=$1 probe_expected=$2
  shift 2
  probe_out=$(env "$@" sh "$HEALTH" readiness 2>&1)
  probe_code=$?
  check "$probe_label output" "$probe_expected" "$probe_out"
  if [ "$probe_expected" = 'health: ready' ]; then
    check "$probe_label exit" 0 "$probe_code"
  else
    check "$probe_label exit" 1 "$probe_code"
  fi
}

setup
sleep 60 & live_a=$!
sleep 60 & live_b=$!
sh -c 'exit 0' & dead=$!
wait "$dead"
mkdir -p "$QUORUM_RUN_DIR" "$QUORUM_HOME/repos/acme__widget"
: >"$QUORUM_HOME/repos/acme__widget/quorum.db"
record() {
  printf 'supervisor=%s\nserve=%s\nweb=%s\nrepo=%s\n' "$1" "$2" "$3" "$4" \
    >"$QUORUM_RUN_DIR/supervisor.pids"
}
record "$live_a" "$live_b" "$live_a" acme/widget

: >"$FAKE_CALLS"
probe_case 'ready' 'health: ready'
check 'one status read per probe' 1 "$(grep -c '^status$' "$FAKE_CALLS")"
check 'usage exit' 2 "$(sh "$HEALTH" bogus >/dev/null 2>&1; echo $?)"
check 'usage output' 'health: usage' "$(sh "$HEALTH" bogus 2>&1)"

probe_case 'missing daemon authority' 'health: not-ready check=authority' FAKE_READY=0
probe_case 'stale daemon authority' 'health: not-ready check=authority' FAKE_READY=stale
probe_case 'status exit 3' 'health: not-ready check=db' FAKE_STATUS_CODE=3
probe_case 'Web HTTP failure' 'health: not-ready check=http' FAKE_HTTP_READY=0
probe_case 'invalid identity' 'health: not-ready check=config' QUORUM_REPO=bad
probe_case 'missing runtime record' 'health: not-ready check=runtime' \
  QUORUM_RUN_DIR="$WORK/absent"

# A different repository is refused by the record, and when the record names
# it, the missing selected DB is refused before status could create it.
probe_case 'wrong repository' 'health: not-ready check=repo' QUORUM_REPO=acme/other
record "$live_a" "$live_b" "$live_a" acme/other
: >"$FAKE_CALLS"
probe_case 'wrong repository DB' 'health: not-ready check=db' QUORUM_REPO=acme/other
check 'wrong repository DB not created' no \
  "$(test -e "$QUORUM_HOME/repos/acme__other" && echo yes || echo no)"
check 'wrong repository runs no status' 0 "$(grep -c '^status$' "$FAKE_CALLS")"
record "$live_a" "$live_b" "$live_a" acme/widget

mv "$QUORUM_HOME/repos/acme__widget/quorum.db" "$WORK/db.off"
probe_case 'missing DB' 'health: not-ready check=db'
mv "$WORK/db.off" "$QUORUM_HOME/repos/acme__widget/quorum.db"

for required in "$QUORUM_WORKTREE_BASE" "$QUORUM_LOG_DIR" "$QUORUM_REPO_DIR"; do
  mv "$required" "$WORK/path.off"
  probe_case "missing path $(basename "$required")" 'health: not-ready check=paths'
  mv "$WORK/path.off" "$required"
done
if [ "$(id -u)" -ne 0 ]; then
  chmod 000 "$QUORUM_WORKTREE_BASE"
  probe_case 'inaccessible path' 'health: not-ready check=paths'
  chmod 755 "$QUORUM_WORKTREE_BASE"
fi

record "$live_a" "$dead" "$live_b" acme/widget
probe_case 'dead serve' 'health: not-ready check=serve'
check 'liveness dead serve' 'health: not-live check=serve' "$(sh "$HEALTH" liveness 2>&1)"
record "$live_a" "$live_b" "$dead" acme/widget
probe_case 'dead Web' 'health: not-ready check=web'
record "$dead" "$live_a" "$live_b" acme/widget
probe_case 'dead supervisor' 'health: not-ready check=supervisor'
printf 'supervisor=%s\nbogus=1\n' "$live_a" >"$QUORUM_RUN_DIR/supervisor.pids"
probe_case 'malformed runtime record' 'health: not-ready check=runtime'
kill "$live_a" "$live_b" 2>/dev/null
wait "$live_a" "$live_b" 2>/dev/null
teardown

printf '%s\n' '---'
if [ "$FAILS" -eq 0 ]; then
  printf 'all entrypoint tests passed\n'
else
  printf '%s entrypoint tests failed\n' "$FAILS" >&2
  exit 1
fi
