#!/bin/sh
# Bounded in-image Quorum probe, installed as /usr/local/bin/quorum-health.
#
#   quorum-health liveness   supervisor, `quorum serve`, and `quorum web` live
#   quorum-health readiness  liveness + /data paths + compatible repository DB
#                            + Alive daemon authority + loopback Web HTTP
#
# Output is exactly one fixed line: `health: live|ready` or
# `health: not-live|not-ready check=<name>`. Raw status/HTTP output is never
# printed. Exit 0 = pass, 1 = fail (Docker HEALTHCHECK unhealthy), 2 = usage.
# One attempt per invocation; retries belong to Docker's HEALTHCHECK.
set -u
# Child diagnostics may contain paths, messages, or errors; never emit them.
exec 2>/dev/null

STATUS_TIMEOUT_SECS=5   # one short-lived `quorum status --json` read
HTTP_TIMEOUT_SECS=2     # one loopback git smart-HTTP request
KILL_AFTER_SECS=1       # escalation after either timeout
HTTP_CAPTURE_BYTES=512  # captured HTTP client diagnostic
STATUS_FIELD_BYTES=64   # per-field prefix scanned from status JSON
RUNTIME_MAX_LINES=8     # runtime PID file lines read

MODE=${1:-readiness}
case "$MODE" in
  liveness) PASS=live FAIL=not-live ;;
  readiness) PASS=ready FAIL=not-ready ;;
  *)
    printf 'health: usage\n'
    exit 2
    ;;
esac
[ "$#" -le 1 ] || { printf 'health: usage\n'; exit 2; }

fail() {
  printf 'health: %s check=%s\n' "$FAIL" "$1"
  exit 1
}

# Same identity and path defaults as entrypoint.sh.
QUORUM_REPO=${QUORUM_REPO:-}
case "$QUORUM_REPO" in
  ?*/?*) ;;
  *) fail config ;;
esac
REPO_OWNER=${QUORUM_REPO%%/*}
REPO_NAME=${QUORUM_REPO#*/}
case "$REPO_NAME" in */*) fail config ;; esac
QUORUM_HOME=${QUORUM_HOME:-/data/quorum}
REPO_DIR=${QUORUM_REPO_DIR:-/data/repos/project}
WORKTREE_BASE=${QUORUM_WORKTREE_BASE:-/data/worktrees}
LOG_DIR=${QUORUM_LOG_DIR:-$QUORUM_HOME/logs}
WEB_PORT=${QUORUM_WEB_PORT:-8080}
RUNTIME_FILE=${QUORUM_RUN_DIR:-/run/quorum}/supervisor.pids
case "$WEB_PORT" in ''|*[!0-9]*) fail config ;; esac
export QUORUM_REPO QUORUM_HOME

# A PID is live when it exists and is not a zombie. /proc is authoritative
# when present; kill -0 is the portable fallback for host-side tests.
pid_live() {
  case "$1" in ''|*[!0-9]*) return 1 ;; esac
  if [ -d /proc/self ]; then
    stat_line=
    read -r stat_line <"/proc/$1/stat" 2>/dev/null || return 1
    stat_rest=${stat_line##*) }
    case "${stat_rest%% *}" in Z|X|x|'') return 1 ;; esac
    return 0
  fi
  kill -0 "$1" 2>/dev/null
}

# The supervisor rewrites this file after starting both children and removes
# it on exit; a missing or malformed file is never treated as live.
SUPERVISOR= SERVE= WEB= RECORDED_REPO=
[ -f "$RUNTIME_FILE" ] && [ -r "$RUNTIME_FILE" ] || fail runtime
runtime_lines=0
while [ "$runtime_lines" -lt "$RUNTIME_MAX_LINES" ] && IFS= read -r line; do
  runtime_lines=$((runtime_lines + 1))
  case "$line" in
    supervisor=*) SUPERVISOR=${line#supervisor=} ;;
    serve=*) SERVE=${line#serve=} ;;
    web=*) WEB=${line#web=} ;;
    repo=*) RECORDED_REPO=${line#repo=} ;;
    *) fail runtime ;;
  esac
done <"$RUNTIME_FILE"
pid_live "$SUPERVISOR" || fail supervisor
pid_live "$SERVE" || fail serve
pid_live "$WEB" || fail web
[ "$RECORDED_REPO" = "$QUORUM_REPO" ] || fail repo

if [ "$MODE" = liveness ]; then
  printf 'health: %s\n' "$PASS"
  exit 0
fi

# Required mounted state must exist and be usable by the runtime UID.
for required in "$QUORUM_HOME" "$REPO_DIR" "$WORKTREE_BASE" "$LOG_DIR"; do
  [ -d "$required" ] && [ -r "$required" ] && [ -w "$required" ] \
    && [ -x "$required" ] || fail paths
done
[ -e "$REPO_DIR/.git" ] || fail paths

# The selected repository DB must already exist: `quorum status` would
# otherwise create a fresh DB for a wrong identity. The supervisor runs
# `quorum init` with this binary before recording PIDs, so an existing DB is
# already current and this read applies no migration.
DB_FILE=$QUORUM_HOME/repos/${REPO_OWNER}__${REPO_NAME}/quorum.db
[ -f "$DB_FILE" ] && [ -r "$DB_FILE" ] && [ -w "$DB_FILE" ] || fail db

# One short-lived status read opens and closes the DB. Its JSON is streamed
# through a filter: commas become line breaks and each field is cut to a fixed
# prefix, so only two short counters ever reach this shell. serde_json escapes
# quotes inside strings, so `"daemon":{"Alive":{` only matches the top-level
# liveness field. The status exit code travels on fd 5.
status_result=$(
  {
    {
      timeout -k "$KILL_AFTER_SECS" "$STATUS_TIMEOUT_SECS" \
        quorum status --json 2>/dev/null </dev/null
      printf 'rc=%s\n' "$?" >&5
    } | tr ',' '\n' | cut -c "1-$STATUS_FIELD_BYTES" \
      | grep -c -e '^{\{0,1\}"daemon":{"Alive":{' \
      | sed 's/^/alive=/'
  } 5>&1
)
status_rc= alive_count=
for status_field in $status_result; do
  case "$status_field" in
    rc=*) status_rc=${status_field#rc=} ;;
    alive=*) alive_count=${status_field#alive=} ;;
  esac
done
[ "$status_rc" = 0 ] || fail db
[ "$alive_count" = 1 ] || fail authority

# Web must return a real HTTP response. git is a pinned runtime dependency
# and reports this exact error only after a real non-Git HTTP response.
web_response=$(
  timeout -k "$KILL_AFTER_SECS" "$HTTP_TIMEOUT_SECS" \
    git ls-remote "http://127.0.0.1:$WEB_PORT/" 2>&1 </dev/null \
    | head -c "$HTTP_CAPTURE_BYTES"
)
[ "$web_response" = "fatal: repository 'http://127.0.0.1:$WEB_PORT/' not found" ] \
  || fail http

printf 'health: %s\n' "$PASS"
exit 0
