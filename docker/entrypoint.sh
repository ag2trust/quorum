#!/bin/sh
# Supervise one `quorum serve` authority and one loopback-only Web process.
# Pinned tini is PID 1 and uses -g so an external signal reaches the complete
# process group. This script also bounds direct and recorded descendant cleanup.
set -u

fail() {
  printf 'entrypoint: %s\n' "$1" >&2
  exit "${2:-1}"
}

QUORUM_REPO=${QUORUM_REPO:-}
case "$QUORUM_REPO" in
  */*)
    REPO_OWNER=${QUORUM_REPO%%/*}
    REPO_NAME=${QUORUM_REPO#*/}
    case "$REPO_NAME" in */*) fail 'QUORUM_REPO must be exactly owner/name' ;; esac
    [ -n "$REPO_OWNER" ] && [ -n "$REPO_NAME" ] \
      || fail 'QUORUM_REPO must be exactly owner/name'
    ;;
  *) fail 'QUORUM_REPO (owner/name) is required' ;;
esac
export QUORUM_REPO

QUORUM_HOME=${QUORUM_HOME:-/data/quorum}
REPO_DIR=${QUORUM_REPO_DIR:-/data/repos/project}
WORKTREE_BASE=${QUORUM_WORKTREE_BASE:-/data/worktrees}
LOG_DIR=${QUORUM_LOG_DIR:-$QUORUM_HOME/logs}
WEB_PORT=${QUORUM_WEB_PORT:-8080}
READY_TRIES=${QUORUM_READY_TRIES:-30}
SHUTDOWN_TRIES=${QUORUM_SHUTDOWN_TRIES:-5}
SERVE_CONFIG=${QUORUM_SERVE_CONFIG:-$QUORUM_HOME/serve/${REPO_OWNER}__${REPO_NAME}.toml}
SERVE_TEMPLATE=${QUORUM_SERVE_TEMPLATE:-/usr/share/quorum/serve-codex.toml}
export QUORUM_HOME

positive_integer() {
  setting=$1
  setting_value=$2
  case "$setting_value" in ''|*[!0-9]*) fail "$setting must be a positive integer" ;; esac
  [ "$setting_value" -gt 0 ] || fail "$setting must be a positive integer"
}
positive_integer READY_TRIES "$READY_TRIES"
positive_integer SHUTDOWN_TRIES "$SHUTDOWN_TRIES"

[ -e "$REPO_DIR/.git" ] \
  || fail "$REPO_DIR is not a git checkout (.git is missing)"
git -C "$REPO_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
  || fail "$REPO_DIR is not a valid git checkout"

CONFIG_DIR=$(dirname "$SERVE_CONFIG")
mkdir -p "$CONFIG_DIR" "$QUORUM_HOME/init" "$WORKTREE_BASE" "$LOG_DIR" \
  || fail 'cannot prepare writable state below /data' 3

# Install public Codex-only routing only for a fresh repository identity. An
# operator-created configuration at the normal per-repo path is never changed.
if [ ! -e "$SERVE_CONFIG" ]; then
  config_candidate=$SERVE_CONFIG.candidate.$$
  if ! cp "$SERVE_TEMPLATE" "$config_candidate"; then
    rm -f "$config_candidate"
    fail "cannot prepare fresh serve config at $SERVE_CONFIG" 3
  fi
  if ln "$config_candidate" "$SERVE_CONFIG" 2>/dev/null; then
    :
  elif [ ! -e "$SERVE_CONFIG" ]; then
    rm -f "$config_candidate"
    fail "cannot install fresh serve config at $SERVE_CONFIG" 3
  fi
  rm -f "$config_candidate"
fi

INIT=
SERVE=
WEB=
INIT_TREE=
SERVE_TREE=
WEB_TREE=
SHUTTING_DOWN=0

alive() { [ -n "$1" ] && kill -0 "$1" 2>/dev/null; }

children_of() {
  parent=$1
  if [ -r "/proc/$parent/task/$parent/children" ]; then
    children=
    read -r children <"/proc/$parent/task/$parent/children" || true
    printf '%s\n' "$children"
  elif command -v pgrep >/dev/null 2>&1; then
    pgrep -P "$parent" 2>/dev/null || true
  fi
}

process_tree() {
  root=$1
  alive "$root" || return 0
  printf '%s\n' "$root"
  for descendant in $(children_of "$root"); do
    process_tree "$descendant"
  done
}

signal_pids() {
  signal=$1
  shift
  [ "$#" -eq 0 ] || kill "-$signal" "$@" 2>/dev/null || true
}

tree_alive() {
  for tree_pid in $1; do alive "$tree_pid" && return 0; done
  return 1
}

term() {
  SHUTTING_DOWN=1
  INIT_TREE=$(process_tree "$INIT")
  SERVE_TREE=$(process_tree "$SERVE")
  WEB_TREE=$(process_tree "$WEB")
  # tini -g independently forwards the same external signal to the process
  # group. These targeted sends also cover direct host-side contract tests.
  signal_pids TERM $INIT_TREE $SERVE_TREE $WEB_TREE
}
trap term TERM INT

# Stop a direct child plus descendants recorded when shutdown began. Descendant
# PIDs are retained even after orphaning, then TERM escalates to KILL on a
# bounded deadline. `wait` still returns the direct child's exact status.
stop_and_wait() {
  child_pid=$1
  known_tree=${2:-}
  current_tree=$(process_tree "$child_pid")
  shutdown_tree="$known_tree $current_tree"
  signal_pids TERM $shutdown_tree
  stop_i=0
  while tree_alive "$shutdown_tree" && [ "$stop_i" -lt "$SHUTDOWN_TRIES" ]; do
    stop_i=$((stop_i + 1))
    sleep 1
  done
  if tree_alive "$shutdown_tree"; then
    printf 'entrypoint: child tree rooted at %s ignored TERM; sending KILL\n' \
      "$child_pid" >&2
    signal_pids KILL $shutdown_tree
  fi
  wait "$child_pid"
}

# Init resolves identity exclusively from QUORUM_REPO and runs in a dedicated
# persistent directory outside the managed checkout. It remains idempotent and
# cannot create or refresh repository skill files in that checkout.
(
  cd "$QUORUM_HOME/init" || exit 3
  exec quorum init
) &
INIT=$!
if wait "$INIT"; then init_code=0; else init_code=$?; fi
if [ "$SHUTTING_DOWN" -eq 1 ]; then
  stop_and_wait "$INIT" "$INIT_TREE" >/dev/null 2>&1 || true
  exit 143
fi
INIT=
[ "$init_code" -eq 0 ] || exit "$init_code"

set -- --config "$SERVE_CONFIG" --repo "$QUORUM_REPO" \
  --repo-dir "$REPO_DIR" --worktree-base "$WORKTREE_BASE" --log-dir "$LOG_DIR"
if [ "${QUORUM_SELF_UPDATE_DRAIN:-0}" = 1 ]; then
  set -- "$@" --self-update-drain
fi
quorum serve "$@" &
SERVE=$!
if [ "$SHUTTING_DOWN" -eq 1 ]; then
  stop_and_wait "$SERVE" "$SERVE_TREE" >/dev/null 2>&1 || true
  exit 143
fi
quorum web --port "$WEB_PORT" --bind 127.0.0.1 --log-dir "$LOG_DIR" &
WEB=$!

serve_exit() {
  if stop_and_wait "$SERVE" "$SERVE_TREE"; then code=0; else code=$?; fi
  stop_and_wait "$WEB" "$WEB_TREE" >/dev/null 2>&1 || true
  exit "$code"
}

web_exit() {
  printf 'entrypoint: web exited unexpectedly; stopping serve\n' >&2
  stop_and_wait "$SERVE" "$SERVE_TREE" >/dev/null 2>&1 || true
  stop_and_wait "$WEB" "$WEB_TREE" >/dev/null 2>&1 || true
  exit 1
}

web_http_ready() {
  # git is already a pinned runtime dependency. Its smart-HTTP probe reports
  # this error only after receiving a real non-Git HTTP response from Web.
  web_response=$(timeout 2 git ls-remote "http://127.0.0.1:$WEB_PORT/" 2>&1 || true)
  case "$web_response" in
    "fatal: repository 'http://127.0.0.1:$WEB_PORT/' not found") return 0 ;;
    *) return 1 ;;
  esac
}

i=0
while [ "$i" -lt "$READY_TRIES" ]; do
  [ "$SHUTTING_DOWN" -eq 1 ] && serve_exit
  alive "$SERVE" || serve_exit
  alive "$WEB" || web_exit
  if quorum status --json 2>/dev/null | grep -q '"Alive"' \
    && web_http_ready; then
    break
  fi
  i=$((i + 1))
  sleep 1
done
if [ "$i" -ge "$READY_TRIES" ]; then
  printf 'entrypoint: daemon and Web not ready after %ss\n' "$READY_TRIES" >&2
  stop_and_wait "$SERVE" "$SERVE_TREE" >/dev/null 2>&1 || true
  stop_and_wait "$WEB" "$WEB_TREE" >/dev/null 2>&1 || true
  exit 1
fi

while alive "$SERVE" && alive "$WEB"; do sleep 1; done
if [ "$SHUTTING_DOWN" -eq 1 ]; then
  serve_exit
elif alive "$SERVE"; then
  web_exit
else
  serve_exit
fi
