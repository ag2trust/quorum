#!/bin/sh
# Host-side tests for docker/entrypoint.sh. No Docker required.
set -u
here="$(cd "$(dirname "$0")" && pwd)"
ENTRY="$here/entrypoint.sh"
fails=0

# Build a temp workspace with a fake `quorum` and a fake git checkout.
setup() {
  WORK="$(mktemp -d)"
  mkdir -p "$WORK/bin" "$WORK/repo/.git" "$WORK/wt" "$WORK/logs"
  cat >"$WORK/bin/quorum" <<'EOF'
#!/bin/sh
# Fake quorum. Behavior via FAKE_* env.
case "$1" in
  init)
    printf 'init\n' >>"${FAKE_CALLS:?}"
    exit "${FAKE_INIT_CODE:-0}" ;;
  serve)
    printf 'serve\n' >>"${FAKE_CALLS:?}"
    if [ "${FAKE_SERVE_IGNORE_TERM:-0}" = 1 ]; then trap '' TERM; while :; do :; done; fi
    sleep "${FAKE_SERVE_SLEEP:-5}" & child=$!
    trap 'kill "$child" 2>/dev/null' TERM
    wait "$child"
    exit "${FAKE_SERVE_CODE:-0}" ;;
  web)
    printf 'web\n' >>"${FAKE_CALLS:?}"
    if [ "${FAKE_WEB_IGNORE_TERM:-0}" = 1 ]; then trap '' TERM; while :; do :; done; fi
    sleep "${FAKE_WEB_SLEEP:-5}" & child=$!
    trap 'kill "$child" 2>/dev/null' TERM
    wait "$child"
    exit "${FAKE_WEB_CODE:-0}" ;;
  status)
    if [ "${FAKE_READY:-1}" = 1 ]; then echo '{"daemon":{"Alive":{"pid":1}}}';
    else echo '{"daemon":"None"}'; fi ;;
esac
EOF
  chmod +x "$WORK/bin/quorum"
  export PATH="$WORK/bin:$PATH"
  export QUORUM_REPO=acme/widget
  export QUORUM_REPO_DIR="$WORK/repo"
  export QUORUM_WORKTREE_BASE="$WORK/wt"
  export QUORUM_LOG_DIR="$WORK/logs"
  export QUORUM_READY_TRIES=5
  export FAKE_CALLS="$WORK/calls"
  unset QUORUM_SELF_UPDATE_DRAIN QUORUM_WEB_PORT 2>/dev/null || true
}
teardown() { rm -rf "$WORK"; unset FAKE_INIT_CODE FAKE_SERVE_CODE FAKE_SERVE_SLEEP FAKE_SERVE_IGNORE_TERM FAKE_WEB_CODE FAKE_WEB_SLEEP FAKE_WEB_IGNORE_TERM FAKE_READY FAKE_CALLS 2>/dev/null || true; }

check() { # label expected actual
  if [ "$3" = "$2" ]; then echo "ok: $1"; else echo "FAIL: $1 expected $2 got $3"; fails=$((fails+1)); fi
}

# Case A: missing QUORUM_REPO -> nonzero before starting anything.
setup; unset QUORUM_REPO
sh "$ENTRY" >/dev/null 2>&1; check "missing QUORUM_REPO fails" 1 "$?"; teardown

# Case B: missing repo checkout -> nonzero.
setup; export QUORUM_REPO_DIR="$WORK/nonexistent"
sh "$ENTRY" >/dev/null 2>&1; check "missing checkout fails" 1 "$?"; teardown

# Case C: first-run initialization precedes both children.
setup; FAKE_SERVE_SLEEP=1 FAKE_WEB_SLEEP=30 FAKE_READY=1 \
  sh "$ENTRY" >/dev/null 2>&1
check "first-run init succeeds" 0 "$?"
check "init runs before children" "init" "$(sed -n '1p' "$FAKE_CALLS")"
check "serve and web both start" "serve web " "$(sed -n '2,$p' "$FAKE_CALLS" | sort | tr '\n' ' ')"
teardown

# Case D: init failures propagate exactly and no child is started.
setup; FAKE_INIT_CODE=3 sh "$ENTRY" >/dev/null 2>&1
check "init exit 3 propagates" 3 "$?"
check "init failure starts no children" "init " "$(tr '\n' ' ' <"$FAKE_CALLS")"
teardown

# Case E: serve exits 3 during the gate (schema-too-new at startup) -> container exits 3.
setup; FAKE_SERVE_SLEEP=1 FAKE_SERVE_CODE=3 FAKE_READY=0 \
  sh "$ENTRY" >/dev/null 2>&1; check "serve exit 3 propagates" 3 "$?"; teardown

# Case F: serve exits 75 (self-update) after becoming ready -> container exits 75.
setup; FAKE_SERVE_SLEEP=2 FAKE_SERVE_CODE=75 FAKE_WEB_SLEEP=30 FAKE_READY=1 \
  sh "$ENTRY" >/dev/null 2>&1; check "serve exit 75 propagates" 75 "$?"; teardown

# Case G: web dies while serve stays up -> container exits 1 (synthesized,
# not web's own code -- FAKE_WEB_CODE=7 proves it's not a coincidental match).
setup; FAKE_WEB_SLEEP=1 FAKE_WEB_CODE=7 FAKE_SERVE_SLEEP=30 FAKE_READY=1 \
  sh "$ENTRY" >/dev/null 2>&1; check "web failure -> exit 1" 1 "$?"; teardown

# Case H: clean shutdown -> serve exits 0 -> container exits 0.
setup; FAKE_SERVE_SLEEP=2 FAKE_SERVE_CODE=0 FAKE_WEB_SLEEP=30 FAKE_READY=1 \
  sh "$ENTRY" >/dev/null 2>&1; check "clean serve exit 0" 0 "$?"; teardown

# Case I: external SIGTERM after ready -> term() trap kills both children and
# the container exits promptly and deterministically (not a hang).
setup
FAKE_SERVE_SLEEP=30 FAKE_WEB_SLEEP=30 FAKE_READY=1 sh "$ENTRY" >/dev/null 2>&1 &
ENTRY_PID=$!
sleep 1
kill -TERM "$ENTRY_PID" 2>/dev/null
tries=0
while kill -0 "$ENTRY_PID" 2>/dev/null; do
  tries=$((tries + 1))
  if [ "$tries" -gt 5 ]; then break; fi
  sleep 1
done
if kill -0 "$ENTRY_PID" 2>/dev/null; then
  echo "FAIL: SIGTERM shutdown hung (still running after ~5s)"; fails=$((fails + 1))
  kill -KILL "$ENTRY_PID" 2>/dev/null
  wait "$ENTRY_PID" 2>/dev/null
else
  wait "$ENTRY_PID" 2>/dev/null; code=$?
  check "SIGTERM shutdown exits deterministically" 0 "$code"
fi
teardown

# Case J: SIGTERM before readiness must still attribute shutdown to serve. Web
# exits with 7 when signalled, so an incorrect web-first classification is 1.
setup
FAKE_SERVE_SLEEP=30 FAKE_SERVE_CODE=0 FAKE_WEB_SLEEP=30 FAKE_WEB_CODE=7 FAKE_READY=0 \
  sh "$ENTRY" >/dev/null 2>&1 &
ENTRY_PID=$!
sleep 1
kill -TERM "$ENTRY_PID" 2>/dev/null
wait "$ENTRY_PID" 2>/dev/null; code=$?
check "pre-ready SIGTERM propagates serve exit" 0 "$code"
teardown

# Case K: serve's already-determined code survives a web sibling that ignores
# TERM. The bounded escalation must prevent the supervisor from hanging.
setup
FAKE_SERVE_SLEEP=1 FAKE_SERVE_CODE=75 FAKE_WEB_IGNORE_TERM=1 FAKE_READY=1 \
  sh "$ENTRY" >/dev/null 2>&1 &
ENTRY_PID=$!
tries=0
while kill -0 "$ENTRY_PID" 2>/dev/null && [ "$tries" -lt 8 ]; do
  tries=$((tries + 1)); sleep 1
done
if kill -0 "$ENTRY_PID" 2>/dev/null; then
  echo "FAIL: TERM-ignoring web made serve-exit teardown hang"; fails=$((fails + 1))
  kill -KILL "$ENTRY_PID" 2>/dev/null; wait "$ENTRY_PID" 2>/dev/null
else
  wait "$ENTRY_PID" 2>/dev/null; code=$?
  check "serve exit survives TERM-ignoring web" 75 "$code"
fi
teardown

# Case L: a crashed web still yields 1 when serve ignores TERM; escalation and
# reap bound the negative shutdown path.
setup
FAKE_SERVE_IGNORE_TERM=1 FAKE_WEB_SLEEP=1 FAKE_WEB_CODE=7 FAKE_READY=1 \
  sh "$ENTRY" >/dev/null 2>&1 &
ENTRY_PID=$!
tries=0
while kill -0 "$ENTRY_PID" 2>/dev/null && [ "$tries" -lt 8 ]; do
  tries=$((tries + 1)); sleep 1
done
if kill -0 "$ENTRY_PID" 2>/dev/null; then
  echo "FAIL: TERM-ignoring serve made web-failure teardown hang"; fails=$((fails + 1))
  kill -KILL "$ENTRY_PID" 2>/dev/null; wait "$ENTRY_PID" 2>/dev/null
else
  wait "$ENTRY_PID" 2>/dev/null; code=$?
  check "web failure survives TERM-ignoring serve" 1 "$code"
fi
teardown

echo "---"; [ "$fails" = 0 ] && echo "all passed" || { echo "$fails failed"; exit 1; }
