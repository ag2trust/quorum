#!/bin/sh
# Real linux/amd64 repeated abrupt-kill replacement and equal-PID contention
# over one host /data tree.
#
# (a) ABRUPT_ITERATIONS times (default 12): commit rows through the live
# container with supported CLI commands, SIGKILL it (no shutdown path, no lock
# release, no SQLite close), and start a replacement on the same image,
# QUORUM_REPO, and host tree once the fixed daemon_lock stale window has
# passed. Every committed row must survive; each crash image of the DB (plus
# WAL) must pass PRAGMA integrity_check; the replacement must take over the
# stale daemon_lock row left by the kill within a fixed bound without anything
# editing that row; and every recorded artifact, including the exact config
# bytes and checksum, must equal the baseline. The replacement serve PID is
# recorded each time and reuse of the stale holder's numeric PID is asserted.
#
# (b) With the final replacement live and authoritative, a second real
# container (default entrypoint) on the same tree must fail authority
# acquisition although its serve process has the same numeric PID as the
# holder's; the holder must remain the sole live authority. When no
# contender reaches the holder's PID, the holder is replaced by one more fully
# verified abrupt kill and takeover (bounded holder rounds).
#
# This proves on-disk state survival and lock authority only; it does not
# prove that an in-flight provider turn resumes.
set -eu

IMAGE=${1:-quorum:local}
ITERATIONS=${ABRUPT_ITERATIONS:-12}
case "$ITERATIONS" in
  '' | *[!0-9]*) ITERATIONS=0 ;;
esac
if [ "$ITERATIONS" -lt 2 ]; then
  printf 'replace-abrupt: ABRUPT_ITERATIONS must be an integer >= 2\n' >&2
  exit 2
fi
# DAEMON_LOCK_STALE_SECS is 30 (quorum/src/serve/mod.rs) and an age equal to
# it is still live, so a replacement started 32s after the kill must take over.
STALE_WAIT=32
# Fixed bound from kill to replacement readiness (stale wait + startup).
TAKEOVER_BOUND=75
SEED_AGENT=abrupt-seed

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
# shellcheck source=docker/replace-lib.sh
. "$SCRIPT_DIR/replace-lib.sh"
replace_init "$IMAGE" replace-abrupt

# Commit three task-note rows tagged with iteration $2 through container $1.
# Each CLI call returns only after its write transaction commits.
seed_rows() {
  docker exec "$1" sh -ec "
    for n in 1 2 3; do
      printf 'abrupt row %s.%s' $2 \$n | quorum task-update \
        --agent $SEED_AGENT --task-id 1 --note-stdin >/dev/null
    done
  " </dev/null || replace_fail "seeding iteration $2 rows in $1 failed"
}

# In-flight write load at kill time. Every connection Quorum opens is short,
# and the last one to close checkpoints and deletes the WAL, so a kill between
# writes leaves nothing to recover. Overlapping writers keep a WAL (and SHM)
# open, possibly mid-transaction, when SIGKILL lands. Load rows use a distinct
# body and are never asserted as committed; agent registration is unchanged
# because they reuse the seed agent.
start_write_load() {
  for loader in 1 2 3; do
    docker exec -d "$1" sh -c "
      while :; do
        printf 'abrupt load $loader' | quorum task-update --agent $SEED_AGENT \
          --task-id 1 --note-stdin >/dev/null 2>&1
      done
    " || replace_fail "cannot start write load in $1"
  done
  sleep 2
}

# Every row committed by iterations 0..$1, in commit order.
expected_rows() {
  i=0
  while [ "$i" -le "$1" ]; do
    printf 'abrupt row %s.1\nabrupt row %s.2\nabrupt row %s.3\n' "$i" "$i" "$i"
    i=$((i + 1))
  done
}

assert_rows_in_copy() {
  actual=$(sqlite3 "$1/db/quorum.db" \
    "SELECT body FROM task_notes WHERE task_id=1 AND agent='$SEED_AGENT'
       AND body LIKE 'abrupt row %' ORDER BY id") \
    || replace_fail "committed-row query failed on $1"
  [ "$actual" = "$(expected_rows "$2")" ] \
    || replace_fail "committed rows lost in $1: got [$actual]"
}

assert_rows_live() {
  actual=$(docker exec "$1" quorum task-get --task-id 1 \
    | grep -o 'abrupt row [0-9][0-9]*\.[0-9]' | LC_ALL=C sort) \
    || replace_fail "task-get in $1 failed"
  [ "$actual" = "$(expected_rows "$2" | LC_ALL=C sort)" ] \
    || replace_fail "replacement $1 does not show every committed row"
}

# Start-to-authority for container $1: serve observed from outside the PID
# namespace first, then daemon + Web readiness, exactly one live authority,
# and the effective per-repo config.
start_authority() {
  replace_run_default "$1"
  replace_wait_serve_started "$1" || replace_fail "$1 never started serve"
  replace_wait_ready "$1" || replace_fail "$1 never became ready"
  replace_assert_single_authority "$1"
  replace_assert_effective_config "$1"
}

WAL_KILLS=0

# Kill container $1 (serve PID $3), record snapshot $2 of the crash image, and
# verify it: recorded evidence, stale lock row left by the killed holder,
# integrity, and every row committed through iteration $4.
kill_and_record() {
  replace_kill_abrupt "$1"
  KILL_TS=$(date +%s)
  if [ -n "${BASE:-}" ]; then
    replace_snapshot "$2" "$BASE"
  else
    replace_snapshot "$2"
  fi
  replace_assert_recorded "$2"
  replace_assert_integrity "$2"
  replace_assert_eq "$3" \
    "$(sqlite3 "$2/db/quorum.db" 'SELECT pid FROM daemon_lock WHERE id=1')" \
    'stale daemon_lock holder pid left by SIGKILL'
  assert_rows_in_copy "$2" "$4"
  wal=$(sed -n 's/^quorum\.db-wal //p' "$2/db-files")
  shm=$(sed -n 's/^quorum\.db-shm //p' "$2/db-files")
  [ "${wal:-0}" -eq 0 ] || WAL_KILLS=$((WAL_KILLS + 1))
  printf 'replace-abrupt: kill %s: wal=%s shm=%s bytes; integrity ok; %s committed rows present\n' \
    "$4" "${wal:-absent}" "${shm:-absent}" "$((($4 + 1) * 3))"
}

replace_assert_platform
replace_prepare_tree
replace_seed_offline

# Baseline: fresh container, seeded rows, then the first abrupt kill.
replace_name c0
CURRENT=$REPLACE_NAME
start_authority "$CURRENT"
replace_seed_live "$CURRENT"
seed_rows "$CURRENT" 0
PID=$(replace_serve_pids "$CURRENT")
BASE=
BASE_DIR=$REPLACE_WORK/base
replace_status_snapshot "$CURRENT" "$BASE_DIR"
start_write_load "$CURRENT"
kill_and_record "$CURRENT" "$BASE_DIR" "$PID" 0
BASE=$BASE_DIR
CONFIG_SHA=$(sed -n 's/^config sha256 //p' "$BASE/manifest")
printf 'replace-abrupt: baseline recorded %s artifacts; config sha256 %s\n' \
  "$(wc -l <"$BASE/manifest" | tr -d ' ')" "$CONFIG_SHA"

PIDS=$PID
REUSES=0

# Replacement $1 after the previous abrupt kill: wait out the stale window,
# start it to authority within the takeover bound, and require every row
# committed through iteration $1 - 1.
replace_after_kill() {
  STALE_PID=$PID
  now=$(date +%s)
  remaining=$((KILL_TS + STALE_WAIT - now))
  [ "$remaining" -le 0 ] || sleep "$remaining"
  replace_name "r$1"
  CURRENT=$REPLACE_NAME
  start_authority "$CURRENT"
  elapsed=$(($(date +%s) - KILL_TS))
  [ "$elapsed" -le "$TAKEOVER_BOUND" ] \
    || replace_fail "stale takeover took ${elapsed}s (bound ${TAKEOVER_BOUND}s)"
  PID=$(replace_serve_pids "$CURRENT")
  PIDS="$PIDS $PID"
  [ "$PID" != "$STALE_PID" ] || REUSES=$((REUSES + 1))
  assert_rows_live "$CURRENT" $(($1 - 1))
  printf 'replace-abrupt: replacement %s: stale holder pid %s -> serve pid %s; authority %ss after kill\n' \
    "$1" "$STALE_PID" "$PID" "$elapsed"
}

# Commit iteration $1 rows through the live replacement, then kill it under
# write load and verify the crash image against the baseline.
kill_current() {
  seed_rows "$CURRENT" "$1"
  SNAP=$REPLACE_WORK/kill-$1
  replace_status_snapshot "$CURRENT" "$SNAP"
  start_write_load "$CURRENT"
  kill_and_record "$CURRENT" "$SNAP" "$PID" "$1"
  replace_assert_unchanged "$BASE" "$SNAP"
}

k=1
while :; do
  replace_after_kill "$k"
  [ "$k" -lt "$ITERATIONS" ] || break
  kill_current "$k"
  k=$((k + 1))
done

[ "$REUSES" -ge 1 ] \
  || replace_fail "no replacement reused the stale holder's numeric PID: $PIDS"
[ "$WAL_KILLS" -ge 1 ] \
  || replace_fail 'no abrupt kill left a non-empty WAL to recover'
printf 'replace-abrupt: %s abrupt replacements passed; serve pids %s; stale-holder PID reused %s times; %s kills left a non-empty WAL\n' \
  "$ITERATIONS" "$PIDS" "$REUSES" "$WAL_KILLS"

# (b) Equal-PID contender against the live final replacement. The probe on the
# contender's PATH records its own PID and execs the real binary in place, so
# the recorded PID is the contender's serve process. Contenders are never
# exec'd into, so nothing perturbs their PID sequence. Container start varies
# the serve PID by a small run-to-run offset on both sides, so equality is not
# assumed: bounded attempts run until one contender's serve has the holder's
# numeric PID, and every attempt must be rejected. A holder whose PID no
# contender reaches (CI observed holder 14 against 20 contenders at 13) is
# replaced by one more fully verified abrupt kill and takeover, bounded by
# HOLDER_TRIES.
HOLDER_TRIES=4
CONTENDER_TRIES=8
serve_start() {
  docker exec "$HOLDER" sh -c "cut -d' ' -f22 /proc/$HOLDER_PID/stat" \
    || replace_fail "cannot read holder serve start time"
}
PROBE=$REPLACE_WORK/pid-probe
cat >"$PROBE" <<'SH'
#!/bin/sh
[ "${1:-}" != serve ] || printf 'pid-probe: quorum serve pid %s\n' "$$" >&2
exec /usr/local/bin/quorum "$@"
SH
chmod 0555 "$PROBE"
EQUAL=0
round=1
while :; do
  HOLDER=$CURRENT
  HOLDER_PID=$PID
  HOLDER_START=$(serve_start)
  CONTENDER_PIDS=
  attempt=1
  while [ "$attempt" -le "$CONTENDER_TRIES" ]; do
    replace_name "contender$round-$attempt"
    CONTENDER=$REPLACE_NAME
    replace_run_default "$CONTENDER" \
      -e PATH=/opt/pid-probe:/opt/codex/bin:/usr/local/bin:/usr/bin:/bin \
      -v "$PROBE:/opt/pid-probe/quorum:ro"
    replace_wait_stopped "$CONTENDER" 60 || replace_fail "$CONTENDER did not exit"
    CONTENDER_LOG=$(docker logs "$CONTENDER" 2>&1)
    CONTENDER_PID=$(printf '%s\n' "$CONTENDER_LOG" \
      | sed -n 's/^pid-probe: quorum serve pid \([0-9][0-9]*\)$/\1/p')
    [ -n "$CONTENDER_PID" ] || replace_fail "$CONTENDER never started serve"
    replace_assert_eq 2 "$(docker inspect -f '{{.State.ExitCode}}' "$CONTENDER")" \
      "$CONTENDER authority rejection exit"
    printf '%s\n' "$CONTENDER_LOG" \
      | grep -qF "another daemon (pid $HOLDER_PID) is already serving this DB" \
      || replace_fail "$CONTENDER rejection did not name the live holder"
    docker rm "$CONTENDER" >/dev/null
    CONTENDER_PIDS="$CONTENDER_PIDS $CONTENDER_PID"
    if [ "$CONTENDER_PID" = "$HOLDER_PID" ]; then
      EQUAL=1
      break
    fi
    attempt=$((attempt + 1))
  done
  [ "$EQUAL" = 0 ] || break
  printf 'replace-abrupt: holder round %s: no contender serve had holder pid %s:%s; replacing the holder\n' \
    "$round" "$HOLDER_PID" "$CONTENDER_PIDS"
  [ "$round" -lt "$HOLDER_TRIES" ] || replace_fail \
    "no contender serve matched a holder pid in $HOLDER_TRIES holder rounds"
  kill_current "$k"
  k=$((k + 1))
  replace_after_kill "$k"
  round=$((round + 1))
done
# Past one 10s heartbeat: a stolen lock would make the holder's guarded
# refresh match zero rows and exit, so the same serve process (same start
# time) with a heartbeat refreshed after the contender exited is the authority.
sleep 12
replace_assert_single_authority "$HOLDER"
replace_assert_eq "$HOLDER_START" "$(serve_start)" 'holder serve process identity'
age=$(docker exec "$HOLDER" quorum status --json \
  | sed -n 's/.*"Alive":{[^}]*"heartbeat_age_secs":\([0-9][0-9]*\).*/\1/p')
[ -n "$age" ] && [ "$age" -le 11 ] \
  || replace_fail "holder heartbeat not refreshed after contention (age ${age:-none})"
printf 'replace-abrupt: contender serve pids%s all exited 2 against holder serve pid %s (equal on holder round %s attempt %s); holder remains sole authority\n' \
  "$CONTENDER_PIDS" "$HOLDER_PID" "$round" "$attempt"

# Clean stop of the holder releases its lock; the final artifacts still equal
# the baseline and every committed row survives.
FINAL=$REPLACE_WORK/final
replace_status_snapshot "$HOLDER" "$FINAL"
replace_stop_remove "$HOLDER"
replace_snapshot "$FINAL" "$BASE"
replace_assert_unchanged "$BASE" "$FINAL"
replace_assert_integrity "$FINAL"
assert_rows_in_copy "$FINAL" $((k - 1))
replace_assert_eq 0 \
  "$(sqlite3 "$FINAL/db/quorum.db" 'SELECT count(*) FROM daemon_lock')" \
  'daemon_lock rows after clean holder stop'
printf 'replace-abrupt: final artifacts, config (sha256 %s), and %s committed rows unchanged\n' \
  "$(sed -n 's/^config sha256 //p' "$FINAL/manifest")" "$((k * 3))"
printf 'replace-abrupt: abrupt replacement and equal-PID contention passed\n'
