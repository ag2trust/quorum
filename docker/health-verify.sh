#!/bin/sh
# Real linux/amd64 container verification for the in-image health probe
# (docker/health.sh, installed as /usr/local/bin/quorum-health) and the image
# HEALTHCHECK. Asserts exit codes, Docker health state, and DB row counts:
#   1. a healthy default container is `ready` and Docker-healthy in bounds;
#   2. every readiness prerequisite fails independently with its fixed check;
#   3. repeated probes leave no rows, transitions, readers, or large output;
#   4. probe output and Docker health logs carry no forbidden content.
set -eu

IMAGE=${1:-quorum:local}
PLATFORM=linux/amd64
WORK=$(mktemp -d)
SUFFIX=$$
PRIMARY=quorum-health-primary-$SUFFIX
OTHER=quorum-health-other-$SUFFIX
FAKE=quorum-health-fake-$SUFFIX
DATA_VOLUME=quorum-health-data-$SUFFIX
FAKE_VOLUME=quorum-health-fake-data-$SUFFIX
CONTAINERS="$PRIMARY $OTHER $FAKE"
VOLUMES="$DATA_VOLUME $FAKE_VOLUME"
DB_DIR=/data/quorum/repos/acme__widget

# Every wait below is bounded by one of these deadlines.
HEALTHY_DEADLINE_SECS=150   # image HEALTHCHECK: 30s interval, 120s start period
UNHEALTHY_DEADLINE_SECS=60  # fake container: 2s interval, 2 retries
STALE_DEADLINE_SECS=60      # serve heartbeat goes stale after 30s
RECOVER_DEADLINE_SECS=30
FREEZE_ATTEMPTS=3          # serve freezes tried for the authority negative
SETUP_DEADLINE_SECS=60
PROBE_EXEC_TIMEOUT_SECS=30  # outer bound on one probe (probe worst case ~9s)
REPEATED_PROBES=20
MAX_PROBE_BYTES=64          # longest fixed line is 34 bytes

# Forbidden content: only the fixed probe vocabulary may ever be emitted.
TITLE_SENTINEL=healthverify-task-title-$SUFFIX
BODY_SENTINEL=healthverify-prompt-body-$SUFFIX
ENV_SENTINEL=ghp_healthverify${SUFFIX}envvalue

mkdir -p "$WORK/probes"

cleanup() {
  docker rm -f $CONTAINERS >/dev/null 2>&1 || true
  docker volume rm -f $VOLUMES >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT INT TERM

fail() {
  printf 'health-verify: %s\n' "$1" >&2
  for container in $CONTAINERS; do
    if docker inspect "$container" >/dev/null 2>&1; then
      docker logs "$container" >&2 || true
      docker inspect -f '{{json .State.Health}}' "$container" >&2 || true
    fi
  done
  exit 1
}

# POSIX sh has no locals: each helper uses its own variable prefix.
assert_eq() {
  ae_expected=$1 ae_actual=$2 ae_label=$3
  [ "$ae_expected" = "$ae_actual" ] \
    || fail "$ae_label: expected $ae_expected, got $ae_actual"
}

elapsed_since() {
  printf '%s\n' "$(($(date +%s) - $1))"
}

prepare_volume() {
  volume=$1
  docker volume create "$volume" >/dev/null
  docker run --rm --platform "$PLATFORM" --user 0 -v "$volume:/data" \
    "$IMAGE" sh -ec '
    mkdir -p /data/repos/project /data/worktrees
    git -C /data/repos/project init -q
    git -C /data/repos/project -c user.email=verify@example.invalid \
      -c user.name=verify commit -q --allow-empty -m init
    chown -R 10001:10001 /data
  ' >/dev/null
}

http_responds() {
  container=$1
  response=$(docker exec "$container" timeout 3 git ls-remote \
    "http://127.0.0.1:8080/" 2>&1 || true)
  [ "$response" = "fatal: repository 'http://127.0.0.1:8080/' not found" ]
}

# One bounded probe; output is kept for the final content scan.
# Usage: probe_capture <mode> <container> [docker exec options...]
probe_capture() {
  mode=$1 container=$2
  shift 2
  PROBE_INDEX=$((${PROBE_INDEX:-0} + 1))
  PROBE_FILE=$WORK/probes/$PROBE_INDEX.out
  PROBE_RC=0
  docker exec "$@" "$container" timeout -k 1 "$PROBE_EXEC_TIMEOUT_SECS" \
    /usr/local/bin/quorum-health "$mode" >"$PROBE_FILE" 2>&1 || PROBE_RC=$?
  PROBE_OUT=$(cat "$PROBE_FILE")
  size=$(wc -c <"$PROBE_FILE" | tr -d ' ')
  [ "$size" -le "$MAX_PROBE_BYTES" ] \
    || fail "probe $PROBE_INDEX output is $size bytes (bound $MAX_PROBE_BYTES)"
}

# Usage: probe <expected-exit> <expected-line> <container> [exec options...]
probe() {
  want_rc=$1 want_out=$2 container=$3
  shift 3
  probe_capture readiness "$container" "$@"
  [ "$PROBE_RC" = "$want_rc" ] && [ "$PROBE_OUT" = "$want_out" ] \
    || fail "readiness probe: expected exit $want_rc '$want_out', got exit $PROBE_RC '$PROBE_OUT'"
}

expect_ready() {
  probe 0 'health: ready' "$1"
}

expect_not_ready() {
  check=$1 container=$2
  shift 2
  probe 1 "health: not-ready check=$check" "$container" "$@"
}

# Usage: wait_health <container> <healthy|unhealthy> <deadline-secs>
wait_health() {
  container=$1 want=$2 deadline=$3
  start=$(date +%s)
  while :; do
    state=$(docker inspect -f '{{.State.Health.Status}}' "$container" 2>/dev/null || echo missing)
    [ "$state" = "$want" ] && return 0
    running=$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || echo false)
    [ "$running" = true ] || return 1
    [ "$(elapsed_since "$start")" -lt "$deadline" ] || return 1
    sleep 2
  done
}

wait_fake_ready() {
  container=$1
  tries=0
  until docker exec "$container" test -s /run/quorum/supervisor.pids 2>/dev/null \
    && http_responds "$container"; do
    tries=$((tries + 1))
    [ "$tries" -lt "$SETUP_DEADLINE_SECS" ] || return 1
    sleep 1
  done
}

runtime_pid() {
  docker exec "$1" sed -n "s/^$2=//p" /run/quorum/supervisor.pids
}

signal_child() {
  docker exec "$1" sh -c 'kill -"$1" "$(sed -n "s/^$2=//p" /run/quorum/supervisor.pids)"' \
    sh "$2" "$3" || fail "cannot send SIG$2 to $3 in $1"
}

# Consistent DB copy: freeze the container (cgroup freezer) while copying the
# DB, WAL, and shm, then read counts on the host copy.
# Usage: snapshot <container> <db-dir> <name>
snapshot() {
  container=$1 db_dir=$2 name=$3
  rm -rf "$WORK/$name"
  mkdir -p "$WORK/$name"
  docker pause "$container" >/dev/null || fail "cannot pause $container"
  copy_rc=0
  docker cp "$container:$db_dir/." "$WORK/$name/db" >/dev/null 2>&1 || copy_rc=$?
  docker unpause "$container" >/dev/null || fail "cannot unpause $container"
  [ "$copy_rc" = 0 ] || fail "cannot copy $db_dir from $container"
  db=$WORK/$name/db/quorum.db
  assert_eq ok "$(sqlite3 "$db" 'PRAGMA quick_check;')" "$name snapshot integrity"
  sqlite3 "$db" "SELECT name FROM sqlite_master WHERE type = 'table'
    AND name NOT LIKE 'sqlite_%' ORDER BY name;" >"$WORK/$name/tables"
  [ -s "$WORK/$name/tables" ] || fail "$name snapshot has no tables"
  : >"$WORK/$name/counts"
  while IFS= read -r table; do
    printf '%s %s\n' "$table" \
      "$(sqlite3 "$db" "SELECT count(*) FROM \"$table\";")" >>"$WORK/$name/counts"
  done <"$WORK/$name/tables"
  sqlite3 "$db" 'SELECT * FROM tasks ORDER BY id;' >"$WORK/$name/tasks"
}

assert_snapshots_equal() {
  before=$1 after=$2 label=$3
  cmp -s "$WORK/$before/counts" "$WORK/$after/counts" || {
    diff "$WORK/$before/counts" "$WORK/$after/counts" >&2 || true
    fail "$label: repeated probes changed persistent row counts"
  }
  cmp -s "$WORK/$before/tasks" "$WORK/$after/tasks" \
    || fail "$label: repeated probes changed task lifecycle state"
}

# No probe process survives and only the recorded serve/Web children hold the
# repository DB open, so no probe reader lingers to pin the WAL.
assert_no_lingering_probe() {
  container=$1 label=$2
  processes=$(docker top "$container" -eo pid,args)
  if printf '%s\n' "$processes" \
    | grep -E 'quorum-health|quorum status|ls-remote|timeout ' >/dev/null; then
    printf '%s\n' "$processes" >&2
    fail "$label: probe process lingers after repeated probes"
  fi
  holders=$(docker exec "$container" sh -c '
    for proc in /proc/[0-9]*; do
      for fd in "$proc"/fd/*; do
        case "$(readlink "$fd" 2>/dev/null)" in
          *quorum.db*) printf "%s\n" "${proc#/proc/}"; break ;;
        esac
      done
    done')
  allowed=" $(runtime_pid "$container" serve) $(runtime_pid "$container" web) "
  for holder in $holders; do
    case "$allowed" in
      *" $holder "*) ;;
      *) fail "$label: unexpected process $holder holds the DB open" ;;
    esac
  done
}

repeated_probes() {
  rp_container=$1 rp_db_dir=$2 rp_rc=$3 rp_out=$4 rp_label=$5
  snapshot "$rp_container" "$rp_db_dir" "$rp_label-before"
  rp_total=0 rp_i=0
  while [ "$rp_i" -lt "$REPEATED_PROBES" ]; do
    probe "$rp_rc" "$rp_out" "$rp_container"
    rp_total=$((rp_total + $(wc -c <"$PROBE_FILE")))
    rp_i=$((rp_i + 1))
  done
  assert_eq "$((REPEATED_PROBES * (${#rp_out} + 1)))" "$rp_total" \
    "$rp_label: total output bytes for $REPEATED_PROBES probes"
  snapshot "$rp_container" "$rp_db_dir" "$rp_label-after"
  assert_snapshots_equal "$rp_label-before" "$rp_label-after" "$rp_label"
  assert_no_lingering_probe "$rp_container" "$rp_label"
}

# Every Output ends in a newline; drop the blank line docker inspect appends.
health_log() {
  docker inspect -f '{{range .State.Health.Log}}{{.Output}}{{end}}' "$1" \
    | sed '/^$/d'
}

# ---------------------------------------------------------------------------
# 1. Healthy default container: readiness passes, Docker reaches healthy.
prepare_volume "$DATA_VOLUME"
docker run -d --platform "$PLATFORM" --name "$PRIMARY" \
  -e QUORUM_REPO=acme/widget -e QUORUM_HEALTH_VERIFY_SENTINEL="$ENV_SENTINEL" \
  -v "$DATA_VOLUME:/data" "$IMAGE" >/dev/null
docker inspect -f '{{json .Config.Healthcheck.Test}}' "$PRIMARY" \
  | grep -F '"/usr/local/bin/quorum-health","readiness"' >/dev/null \
  || fail 'image HEALTHCHECK does not run quorum-health readiness'
start=$(date +%s)
wait_health "$PRIMARY" healthy "$HEALTHY_DEADLINE_SECS" \
  || fail "default container not Docker-healthy within ${HEALTHY_DEADLINE_SECS}s"
printf 'health-verify: Docker health healthy after %ss (bound %ss)\n' \
  "$(elapsed_since "$start")" "$HEALTHY_DEADLINE_SECS"
expect_ready "$PRIMARY"
probe_capture liveness "$PRIMARY"
assert_eq '0 health: live' "$PROBE_RC $PROBE_OUT" 'liveness probe'
docker exec "$PRIMARY" sh -c 'printf "%s" "$QUORUM_HEALTH_VERIFY_SENTINEL"' \
  | grep -F "$ENV_SENTINEL" >/dev/null \
  || fail 'environment sentinel not visible inside the container'
printf 'health-verify: healthy readiness/liveness passed\n'

# ---------------------------------------------------------------------------
# 3a. Repeated healthy probes: no rows, no transitions, no readers, bounded.
repeated_probes "$PRIMARY" "$DB_DIR" 0 'health: ready' healthy
printf 'health-verify: %s healthy probes left rows/tasks/readers unchanged\n' \
  "$REPEATED_PROBES"

# ---------------------------------------------------------------------------
# 2. Independent negatives. Each mutates one prerequisite of an otherwise
# ready container, asserts the fixed check name, then proves recovery.

# Wrong repository: the probe never accepts an identity the supervisor did not
# record.
expect_not_ready repo "$PRIMARY" -e QUORUM_REPO=acme/other
expect_ready "$PRIMARY"

# Web failure: the probe pointed at the wrong port.
expect_not_ready http "$PRIMARY" -e QUORUM_WEB_PORT=8081
expect_ready "$PRIMARY"

# Web failure: the recorded Web process is live but stopped (SIGSTOP).
signal_child "$PRIMARY" STOP web
expect_not_ready http "$PRIMARY"
signal_child "$PRIMARY" CONT web
expect_ready "$PRIMARY"
printf 'health-verify: wrong-repo and Web-failure negatives passed\n'

# Inaccessible required /data path: positive control first, then mode 000.
docker exec --user 0 "$PRIMARY" sh -ec '
  mkdir /data/scratch-worktrees
  chown 10001:10001 /data/scratch-worktrees
  chmod 0700 /data/scratch-worktrees' || fail 'cannot prepare scratch path'
probe 0 'health: ready' "$PRIMARY" -e QUORUM_WORKTREE_BASE=/data/scratch-worktrees
docker exec --user 0 "$PRIMARY" chmod 000 /data/scratch-worktrees
expect_not_ready paths "$PRIMARY" -e QUORUM_WORKTREE_BASE=/data/scratch-worktrees
docker exec --user 0 "$PRIMARY" rm -rf /data/scratch-worktrees
expect_ready "$PRIMARY"
printf 'health-verify: inaccessible-path negative passed\n'

# Unreadable and incompatible DB: scratch QUORUM_HOME copies of the live DB.
# The unmodified copy is a positive control (its heartbeat is still fresh),
# so each failure is caused by the one mutation applied to its own copy.
install_scratch_db() {
  scratch=$1 source=$2
  docker exec --user 0 "$PRIMARY" sh -ec '
    rm -rf "$1"
    mkdir -p "$1/logs" "$1/repos/acme__widget"' sh "$scratch" \
    || fail "cannot prepare $scratch"
  docker cp "$source" "$PRIMARY:$scratch/repos/acme__widget/quorum.db" >/dev/null \
    || fail "cannot install $scratch DB"
  docker exec --user 0 "$PRIMARY" chown -R 10001:10001 "$scratch" \
    || fail "cannot chown $scratch"
}
snapshot "$PRIMARY" "$DB_DIR" scratch-source
sqlite3 "$WORK/scratch-source/db/quorum.db" ".backup '$WORK/control.db'"
cp "$WORK/control.db" "$WORK/unreadable.db"
cp "$WORK/control.db" "$WORK/incompatible.db"
supported=$(sqlite3 "$WORK/control.db" 'PRAGMA user_version;')
sqlite3 "$WORK/incompatible.db" "PRAGMA user_version=$((supported + 1000));"
install_scratch_db /data/scratch-control "$WORK/control.db"
probe 0 'health: ready' "$PRIMARY" -e QUORUM_HOME=/data/scratch-control
install_scratch_db /data/scratch-unreadable "$WORK/unreadable.db"
docker exec --user 0 "$PRIMARY" \
  chmod 000 /data/scratch-unreadable/repos/acme__widget/quorum.db
expect_not_ready db "$PRIMARY" -e QUORUM_HOME=/data/scratch-unreadable
install_scratch_db /data/scratch-incompatible "$WORK/incompatible.db"
docker exec "$PRIMARY" sh -c \
  'f=/data/scratch-incompatible/repos/acme__widget/quorum.db; [ -r "$f" ] && [ -w "$f" ]' \
  || fail 'incompatible scratch DB must be readable so the schema check is reached'
expect_not_ready db "$PRIMARY" -e QUORUM_HOME=/data/scratch-incompatible
docker exec --user 0 "$PRIMARY" rm -rf /data/scratch-control \
  /data/scratch-unreadable /data/scratch-incompatible
expect_ready "$PRIMARY"
printf 'health-verify: unreadable-DB and incompatible-DB negatives passed\n'

# Wrong repository authority: a second supervisor-shaped container shares the
# primary's /data but selects acme/other. acme/widget holds a live authority
# on the same volume; acme/other has none, so readiness must fail on authority.
# Its serve child is a placeholder that never acquires the daemon lock.
FAKE_SUPERVISOR='
set -eu
mkdir -p "$QUORUM_HOME/init" /data/worktrees "$QUORUM_HOME/logs"
(cd "$QUORUM_HOME/init" && exec quorum init) >/dev/null
sleep 3600 &
serve=$!
quorum web --port 8080 --bind 127.0.0.1 --log-dir "$QUORUM_HOME/logs" &
web=$!
printf "supervisor=%s\nserve=%s\nweb=%s\nrepo=%s\n" \
  "$$" "$serve" "$web" "$QUORUM_REPO" >/run/quorum/supervisor.pids.tmp
mv /run/quorum/supervisor.pids.tmp /run/quorum/supervisor.pids
wait
'
docker run -d --platform "$PLATFORM" --name "$OTHER" \
  -e QUORUM_REPO=acme/other -v "$DATA_VOLUME:/data" \
  "$IMAGE" sh -c "$FAKE_SUPERVISOR" >/dev/null
wait_fake_ready "$OTHER" || fail 'wrong-repository container never started'
expect_ready "$PRIMARY"
expect_not_ready authority "$OTHER"
expect_not_ready repo "$OTHER" -e QUORUM_REPO=acme/widget
docker rm -f "$OTHER" >/dev/null
printf 'health-verify: wrong-repository authority negative passed\n'

# Missing daemon authority: freeze the real serve child. It stays a live,
# non-zombie PID, so only the heartbeat-based authority check can fail. A
# freeze that lands while serve holds the SQLite write lock instead makes the
# bounded status read fail (check=db, still not ready); that attempt is
# resumed and the freeze retried, so authority itself must be observed.
wait_primary_ready() {
  wr_start=$(date +%s)
  while :; do
    probe_capture readiness "$PRIMARY"
    [ "$PROBE_RC $PROBE_OUT" = '0 health: ready' ] && return 0
    [ "$(elapsed_since "$wr_start")" -lt "$RECOVER_DEADLINE_SECS" ] || return 1
    sleep 2
  done
}
freeze_attempt=0 authority_seen=0
while [ "$authority_seen" = 0 ]; do
  freeze_attempt=$((freeze_attempt + 1))
  [ "$freeze_attempt" -le "$FREEZE_ATTEMPTS" ] \
    || fail "stopped serve never reported check=authority in $FREEZE_ATTEMPTS freezes"
  signal_child "$PRIMARY" STOP serve
  start=$(date +%s)
  while :; do
    probe_capture readiness "$PRIMARY"
    case "$PROBE_RC $PROBE_OUT" in
      '1 health: not-ready check=authority')
        authority_seen=1
        printf 'health-verify: stopped serve reported check=authority after %ss (freeze %s)\n' \
          "$(elapsed_since "$start")" "$freeze_attempt"
        break
        ;;
      '1 health: not-ready check=db')
        printf 'health-verify: freeze %s held the DB write lock (check=db); retrying\n' \
          "$freeze_attempt"
        break
        ;;
      '0 health: ready') ;;
      *) fail "stopped-serve probe gave unexpected exit $PROBE_RC '$PROBE_OUT'" ;;
    esac
    [ "$(elapsed_since "$start")" -lt "$STALE_DEADLINE_SECS" ] \
      || fail "stopped serve never lost authority within ${STALE_DEADLINE_SECS}s"
    sleep 3
  done
  signal_child "$PRIMARY" CONT serve
  wait_primary_ready \
    || fail "resumed serve not ready within ${RECOVER_DEADLINE_SECS}s"
done
wait_health "$PRIMARY" healthy "$HEALTHY_DEADLINE_SECS" \
  || fail 'primary not Docker-healthy after negatives'
printf 'health-verify: stopped-serve authority negative passed\n'

# ---------------------------------------------------------------------------
# Absent daemon authority on independent state, with a real task whose title
# and body must never appear in probe output. No serve runs, so the task cannot
# transition. Docker must report the container unhealthy.
prepare_volume "$FAKE_VOLUME"
docker run -d --platform "$PLATFORM" --name "$FAKE" \
  -e QUORUM_REPO=acme/widget -e QUORUM_HEALTH_VERIFY_SENTINEL="$ENV_SENTINEL" \
  --health-interval=2s --health-retries=2 --health-start-period=1s \
  -v "$FAKE_VOLUME:/data" "$IMAGE" sh -c "$FAKE_SUPERVISOR" >/dev/null
wait_fake_ready "$FAKE" || fail 'authority-absent container never started'
printf '%s\n' "$BODY_SENTINEL" | docker exec -i "$FAKE" quorum task-create \
  --created-by verifier --title "$TITLE_SENTINEL" --body-stdin >/dev/null \
  || fail 'cannot create sentinel task'
docker exec "$FAKE" quorum status --json | grep -F '"daemon":"None"' >/dev/null \
  || fail 'authority-absent DB unexpectedly reports a daemon lock'
expect_not_ready authority "$FAKE"
repeated_probes "$FAKE" "$DB_DIR" 1 'health: not-ready check=authority' absent
grep -F "$TITLE_SENTINEL" "$WORK/absent-after/tasks" >/dev/null \
  || fail 'sentinel task missing from the probed DB'
wait_health "$FAKE" unhealthy "$UNHEALTHY_DEADLINE_SECS" \
  || fail "authority-absent container not Docker-unhealthy within ${UNHEALTHY_DEADLINE_SECS}s"
health_log "$FAKE" | grep -Fx 'health: not-ready check=authority' >/dev/null \
  || fail 'Docker health log lacks the fixed authority failure'
printf 'health-verify: absent-authority negative and Docker unhealthy passed\n'

# ---------------------------------------------------------------------------
# 4. Output scan over every probe invocation and every Docker health log.
health_log "$PRIMARY" >"$WORK/probes/primary-health.log"
health_log "$FAKE" >"$WORK/probes/fake-health.log"
docker inspect -f '{{json .State.Health.Log}}' "$PRIMARY" >"$WORK/probes/primary-health.json"
docker inspect -f '{{json .State.Health.Log}}' "$FAKE" >"$WORK/probes/fake-health.json"
grep -Fx 'health: ready' "$WORK/probes/primary-health.log" >/dev/null \
  || fail 'primary Docker health log has no ready entry'
for file in "$WORK"/probes/*; do
  case "$file" in
    *.json) ;;
    *)
      if grep -Evx 'health: (ready|live|not-ready check=[a-z]+)' "$file" >/dev/null; then
        fail "$(basename "$file") contains output outside the fixed vocabulary"
      fi
      ;;
  esac
  for needle in "$TITLE_SENTINEL" "$BODY_SENTINEL" "$ENV_SENTINEL" \
    acme widget /data /run /usr /tmp quorum.db .db 127.0.0.1 'diff --git' \
    QUORUM_ "$WORK"; do
    if grep -F -e "$needle" "$file" >/dev/null; then
      fail "$(basename "$file") leaked forbidden content: $needle"
    fi
  done
  if grep -E 'gh[pousr]_|github_pat_|sk-[A-Za-z0-9]|Bearer |PRIVATE KEY' "$file" >/dev/null; then
    fail "$(basename "$file") contains credential-like content"
  fi
done
printf 'health-verify: %s probe outputs and Docker health logs passed content scan\n' \
  "$PROBE_INDEX"

docker stop -t 20 "$PRIMARY" >/dev/null
assert_eq 0 "$(docker inspect -f '{{.State.ExitCode}}' "$PRIMARY")" 'primary clean stop'
printf 'health-verify: all real-container health cases passed\n'
