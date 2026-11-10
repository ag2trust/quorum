#!/bin/sh
# Shared helpers for real linux/amd64 container-replacement suites.
#
# Source after setting `set -eu`, then call `replace_init <image> <suite>`:
#
#   . "$SCRIPT_DIR/replace-lib.sh"
#   replace_init "$IMAGE" replace
#
# Persistence evidence is one throwaway host directory bind-mounted at /data
# (never image-layer files or a named volume). It is created under mktemp and
# removed, with every container this library named, on exit. Every wait is
# bounded. Seeding uses only supported `quorum` CLI commands and needs no
# GitHub or model credentials. Helpers never touch daemon_lock or edit DB rows.

REPLACE_PLATFORM=linux/amd64
REPLACE_REPO=${REPLACE_REPO:-acme/widget}
REPLACE_SLUG=$(printf '%s' "$REPLACE_REPO" | sed 's#/#__#')
REPLACE_SEED_AGENT=replace-seed
REPLACE_TASK_TITLE='replace persistence probe'
REPLACE_TOPIC=replace-proof
REPLACE_CONFIG=/data/quorum/serve/$REPLACE_SLUG.toml
REPLACE_DB=/data/quorum/repos/$REPLACE_SLUG/quorum.db

# Representative rows written by the seeding helpers. One query per line; the
# seeded task is id 1 because seeding starts from an empty database.
REPLACE_DB_QUERIES="SELECT * FROM tasks WHERE id=1
SELECT * FROM task_notes WHERE task_id=1 AND agent='$REPLACE_SEED_AGENT' ORDER BY id
SELECT * FROM pinned ORDER BY id
SELECT * FROM control ORDER BY scope
SELECT * FROM messages WHERE topic='$REPLACE_TOPIC' ORDER BY seq"

replace_init() {
  REPLACE_IMAGE=$1
  REPLACE_SUITE=$2
  REPLACE_WORK=$(mktemp -d)
  REPLACE_WORK=$(CDPATH= cd -- "$REPLACE_WORK" && pwd -P)
  REPLACE_TREE=$REPLACE_WORK/data
  REPLACE_CONTAINERS=
  trap replace_cleanup EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  mkdir "$REPLACE_TREE"
}

replace_cleanup() {
  [ -z "$REPLACE_CONTAINERS" ] \
    || docker rm -f $REPLACE_CONTAINERS >/dev/null 2>&1 || true
  # Runtime files are owned by 10001 on Linux hosts; empty the tree as root.
  if [ -d "$REPLACE_TREE" ]; then
    docker run --rm --platform "$REPLACE_PLATFORM" --user 0 \
      -v "$REPLACE_TREE:/data" "$REPLACE_IMAGE" \
      sh -c 'find /data -mindepth 1 -delete' >/dev/null 2>&1 || true
  fi
  rm -rf "$REPLACE_WORK"
}

replace_fail() {
  printf '%s: %s\n' "$REPLACE_SUITE" "$1" >&2
  for container in $REPLACE_CONTAINERS; do
    if docker inspect "$container" >/dev/null 2>&1; then
      printf '%s: logs of %s\n' "$REPLACE_SUITE" "$container" >&2
      docker logs "$container" >&2 || true
    fi
  done
  exit 1
}

replace_assert_eq() {
  [ "$1" = "$2" ] || replace_fail "$3: expected $1, got $2"
}

# Set REPLACE_NAME to a container name owned by this run and register it for
# cleanup. Call this directly (not through command substitution) so the
# registration remains in the current shell.
replace_name() {
  REPLACE_NAME=quorum-$REPLACE_SUITE-$1-$$
  REPLACE_CONTAINERS="$REPLACE_CONTAINERS $REPLACE_NAME"
}

# The image must be linux/amd64 and must execute as x86_64 under explicit
# --platform, including on an arm64 Docker host.
replace_assert_platform() {
  architecture=$(docker image inspect -f '{{.Os}}/{{.Architecture}}' "$REPLACE_IMAGE")
  replace_assert_eq "$REPLACE_PLATFORM" "$architecture" 'image platform'
  machine=$(docker run --rm --platform "$REPLACE_PLATFORM" "$REPLACE_IMAGE" uname -m)
  replace_assert_eq x86_64 "$machine" 'container machine'
}

# Run a one-shot root shell against the host tree; an optional second argument
# is a host directory mounted at /out.
replace_root_sh() {
  if [ -n "${2:-}" ]; then
    docker run --rm --platform "$REPLACE_PLATFORM" --user 0 \
      -e REPLACE_SLUG="$REPLACE_SLUG" -v "$REPLACE_TREE:/data" -v "$2:/out" \
      "$REPLACE_IMAGE" sh -ec "$1"
  else
    docker run --rm --platform "$REPLACE_PLATFORM" --user 0 \
      -e REPLACE_SLUG="$REPLACE_SLUG" -v "$REPLACE_TREE:/data" \
      "$REPLACE_IMAGE" sh -ec "$1"
  fi
}

# Run one-shot supported CLI commands as the image's runtime identity.
replace_cli_sh() {
  docker run --rm --platform "$REPLACE_PLATFORM" -i \
    -e QUORUM_REPO="$REPLACE_REPO" -v "$REPLACE_TREE:/data" \
    "$REPLACE_IMAGE" sh -ec "$1"
}

# Managed checkout with tracked content, plus unregistered operator content
# at the worktree root (worktree GC leaves unregistered directories alone).
replace_prepare_tree() {
  replace_root_sh '
    mkdir -p /data/repos/project/src /data/worktrees/operator-scratch \
      /data/quorum/logs
    cd /data/repos/project
    git init -q -b main
    printf "replacement fixture\n" >README.md
    printf "tracked payload\n" >src/payload.txt
    git add README.md src/payload.txt
    git -c user.email=verify@example.invalid -c user.name=verify \
      commit -q -m "replacement fixture"
    printf "operator scratch\n" >/data/worktrees/operator-scratch/notes.txt
    printf "operator log before replacement\n" \
      >/data/quorum/logs/operator.log
    chown -R 10001:10001 /data
  ' >/dev/null || replace_fail 'cannot prepare host /data tree'
}

# Offline seeding before any daemon runs: a task created and cancelled while
# no daemon can dispatch it, so its row is deterministic. It deliberately skips
# `quorum init`, which would write a default (non-Codex) serve config and
# pre-empt the entrypoint's fresh Codex template.
replace_seed_offline() {
  replace_cli_sh "
    cd /data
    printf 'Replacement persistence probe.\n' | quorum task-create \
      --created-by $REPLACE_SEED_AGENT --title '$REPLACE_TASK_TITLE' \
      --body-stdin >/dev/null
    quorum task-update --agent $REPLACE_SEED_AGENT --task-id 1 \
      --status cancelled >/dev/null
  " </dev/null || replace_fail 'offline CLI seeding failed'
}

# Live seeding through a running container: rows the daemon never mutates.
replace_seed_live() {
  docker exec -i "$1" sh -ec "
    printf 'persisted note\n' | quorum task-update --agent $REPLACE_SEED_AGENT \
      --task-id 1 --note-stdin >/dev/null
    printf 'standing notice\n' | quorum pin --agent $REPLACE_SEED_AGENT \
      --body-stdin >/dev/null
    printf 'replacement probe\n' | quorum post --agent $REPLACE_SEED_AGENT \
      --kind info --topic $REPLACE_TOPIC --ttl 48h --body-stdin >/dev/null
    printf 'probe stop\n' | quorum stop --agent replace-probe \
      --by $REPLACE_SEED_AGENT --reason-stdin >/dev/null
  " </dev/null || replace_fail "live CLI seeding in $1 failed"
}

# Supported CLI views of the seeded rows, read through a running container.
replace_cli_view() {
  docker exec "$1" sh -ec "
    quorum task-get --task-id 1
    quorum pins
    quorum stops
    quorum read --topic $REPLACE_TOPIC
  " || replace_fail "CLI view in $1 failed"
}

replace_run_default() {
  container=$1
  shift
  docker run -d --platform "$REPLACE_PLATFORM" --name "$container" \
    -e QUORUM_REPO="$REPLACE_REPO" -v "$REPLACE_TREE:/data" "$@" \
    "$REPLACE_IMAGE" >/dev/null || replace_fail "cannot start $container"
}

replace_http_get() {
  docker exec "$1" timeout 5 bash -c '
    exec 3<>/dev/tcp/127.0.0.1/8080
    printf "GET %s HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n" "$1" >&3
    cat <&3
  ' bash "$2"
}

replace_http_responds() {
  response=$(docker exec "$1" timeout 3 git ls-remote \
    "http://127.0.0.1:8080/" 2>&1 || true)
  [ "$response" = "fatal: repository 'http://127.0.0.1:8080/' not found" ]
}

# Readiness mirrors the entrypoint and docker/supervise.sh: live authority and
# a real Web HTTP response, bounded at 40 one-second tries.
replace_wait_ready() {
  tries=0
  while [ "$tries" -lt 40 ]; do
    if docker exec "$1" quorum status --json 2>/dev/null \
      | grep -q '"Alive"' && replace_http_responds "$1"; then
      return 0
    fi
    running=$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null || echo false)
    [ "$running" = true ] || return 1
    tries=$((tries + 1))
    sleep 1
  done
  return 1
}

replace_wait_stopped() {
  limit=${2:-20}
  tries=0
  while [ "$tries" -lt "$limit" ]; do
    running=$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null || echo missing)
    [ "$running" = false ] && return 0
    tries=$((tries + 1))
    sleep 1
  done
  return 1
}

# Bounded wait until a `quorum serve` process exists in $1. `docker top` runs
# outside the container, so the probe creates no process in its PID namespace;
# an in-container probe racing startup would shift the numeric PID serve gets.
replace_wait_serve_started() {
  tries=0
  while [ "$tries" -lt 40 ]; do
    docker top "$1" 2>/dev/null | grep -q 'quorum serve' && return 0
    running=$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null || echo false)
    [ "$running" = true ] || return 1
    tries=$((tries + 1))
    sleep 1
  done
  return 1
}

# Abrupt stop: SIGKILL to the container init makes the kernel kill every
# process in its PID namespace, so no shutdown path, lock release, or SQLite
# close/checkpoint runs. Requires exit 137, then removes the container.
replace_kill_abrupt() {
  docker kill -s KILL "$1" >/dev/null || replace_fail "cannot SIGKILL $1"
  replace_wait_stopped "$1" || replace_fail "$1 did not stop after SIGKILL"
  replace_assert_eq 137 "$(docker inspect -f '{{.State.ExitCode}}' "$1")" \
    "$1 SIGKILL exit"
  docker rm "$1" >/dev/null
}

# PRAGMA integrity_check over the DB (plus WAL) copy recorded in snapshot $1.
replace_assert_integrity() {
  integrity=$(sqlite3 "$1/db/quorum.db" 'PRAGMA integrity_check') \
    || replace_fail "integrity_check could not open $1/db/quorum.db"
  replace_assert_eq ok "$integrity" "PRAGMA integrity_check of $1"
}

# Clean SIGTERM stop with exit 0, then removal.
replace_stop_remove() {
  docker stop -t 20 "$1" >/dev/null
  replace_wait_stopped "$1" || replace_fail "clean SIGTERM did not stop $1"
  replace_assert_eq 0 "$(docker inspect -f '{{.State.ExitCode}}' "$1")" \
    "$1 clean stop exit"
  docker rm "$1" >/dev/null
}

# PIDs (container namespace) of every live `quorum serve` process in $1.
replace_serve_pids() {
  docker exec "$1" sh -c '
    for proc in /proc/[0-9]*; do
      cmd=$(tr "\000" " " <"$proc/cmdline" 2>/dev/null) || continue
      case "$cmd" in "quorum serve "*) printf "%s\n" "${proc#/proc/}" ;; esac
    done
  '
}

# Assert that the live serve process actually consumes the normal per-repo
# config whose bytes are part of the persistence snapshot.
replace_assert_effective_config() {
  pids=$(replace_serve_pids "$1")
  [ -n "$pids" ] || replace_fail "no serve process in $1"
  for pid in $pids; do
    command=$(docker exec "$1" sh -c \
      "tr '\\000' ' ' </proc/$pid/cmdline")
    case " $command " in
      *" --config $REPLACE_CONFIG "*) ;;
      *) replace_fail "serve process does not use $REPLACE_CONFIG: $command" ;;
    esac
  done
}

# Running containers (any origin) bind-mounting this run's host tree.
replace_tree_containers() {
  for id in $(docker ps -q); do
    docker inspect -f '{{.Name}}{{range .Mounts}} {{.Source}}{{end}}' "$id" \
      2>/dev/null || true
  done | awk -v tree="$REPLACE_TREE" '
    { for (i = 2; i <= NF; i++) if ($i == tree) { sub("^/", "", $1); print $1 } }'
}

# Exactly one live daemon authority on the tree: one running container mounts
# it, that container runs exactly one serve process, and `quorum status`
# reports that process as the live lock holder.
replace_assert_single_authority() {
  holders=$(replace_tree_containers)
  replace_assert_eq "$1" "$holders" 'running containers on host tree'
  pids=$(replace_serve_pids "$1")
  count=$(printf '%s\n' "$pids" | awk 'NF { n++ } END { print n + 0 }')
  replace_assert_eq 1 "$count" \
    "serve processes in $1"
  status=$(docker exec "$1" quorum status --json)
  alive=$(printf '%s\n' "$status" \
    | sed -n 's/.*"Alive":{"pid":\([0-9][0-9]*\).*/\1/p')
  replace_assert_eq "$pids" "$alive" 'status live daemon pid'
}

# Preserve the raw `quorum status --json` response and derive a stable view of
# its representative DB-backed fields. Live PID/heartbeat ages and host
# resource telemetry intentionally do not participate in replacement equality.
replace_status_snapshot() {
  container=$1
  out=$2
  mkdir -p "$out"
  docker exec "$container" quorum status --json >"$out/status.json" \
    || replace_fail "status JSON read failed in $container"
  python3 - "$out/status.json" >"$out/status.projection" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    status = json.load(source)

projection = {
    "agents_total": status["agents_total"],
    "messages_live": status["messages_live"],
    "claims_active": status["claims_active"],
    "tasks": status["tasks"],
    "errors_live": status["errors_live"],
}
json.dump(projection, sys.stdout, sort_keys=True, separators=(",", ":"))
sys.stdout.write("\n")
PY
}

replace_assert_status_unchanged() {
  cmp "$1/status.projection" "$2/status.projection" >&2 \
    || replace_fail 'representative quorum status JSON changed across replacement'
}

# Record every persistence artifact into directory $1 (manifest + raw config
# bytes + DB copy). Must run while no container serves the tree. With $2 (a
# previous snapshot), each recorded log is hashed over its original length:
# replacement startup may append, but it may not truncate or alter any byte
# that existed before replacement, and the set of log paths may not change.
replace_snapshot() {
  out=$1
  mkdir -p "$out"
  : >"$out/log-ref"
  [ -z "${2:-}" ] || grep '^log ' "$2/manifest" >"$out/log-ref" || true
  replace_root_sh '
    repo=/data/repos/project
    g() { git -c safe.directory="*" -C "$repo" "$@"; }
    {
      printf "repo head %s\n" "$(g rev-parse HEAD)"
      printf "repo branch %s\n" "$(g symbolic-ref HEAD)"
      g status --porcelain=v1 --untracked-files=all | sed "s/^/repo status /"
      (cd "$repo" && g ls-files -z | xargs -0 sha256sum --) | sed "s/^/repo file /"
      cfg=/data/quorum/serve/$REPLACE_SLUG.toml
      printf "config path %s\n" "$cfg"
      printf "config size %s\n" "$(wc -c <"$cfg")"
      printf "config sha256 %s\n" "$(sha256sum <"$cfg" | cut -d" " -f1)"
      printf "config stat %s\n" "$(stat -c "%u:%g %a" "$cfg")"
      cp "$cfg" /out/config.bytes
      (cd /data/worktrees && find . -mindepth 1 | LC_ALL=C sort) \
        | while IFS= read -r path; do
          file=/data/worktrees/$path
          if [ -f "$file" ]; then
            printf "worktree file %s %s\n" "$(sha256sum <"$file" | cut -d" " -f1)" "$path"
          else
            printf "worktree %s %s\n" "$(stat -c %F "$file" | tr " " _)" "$path"
          fi
        done
      find /data/quorum/logs -type f | LC_ALL=C sort | while IFS= read -r path; do
        printf "log-path %s\n" "$path"
      done
      if [ -s /out/log-ref ]; then
        while read -r _ size _ path; do
          if [ -f "$path" ]; then
            printf "log %s %s %s\n" "$size" \
              "$(head -c "$size" "$path" | sha256sum | cut -d" " -f1)" "$path"
          else
            printf "log %s missing %s\n" "$size" "$path"
          fi
        done </out/log-ref
      else
        find /data/quorum/logs -type f | LC_ALL=C sort | while IFS= read -r path; do
          printf "log %s %s %s\n" "$(wc -c <"$path")" \
            "$(sha256sum <"$path" | cut -d" " -f1)" "$path"
        done
      fi
    } >/out/manifest
    db=/data/quorum/repos/$REPLACE_SLUG/quorum.db
    mkdir /out/db
    # WAL/SHM presence and size at snapshot time (diagnostic, not compared).
    for file in "$db-wal" "$db-shm"; do
      [ ! -e "$file" ] || printf "%s %s\n" "${file##*/}" "$(wc -c <"$file")"
    done >/out/db-files
    for file in "$db" "$db-wal"; do
      [ ! -e "$file" ] || cp "$file" /out/db/
    done
    chmod -R a+rwX /out
  ' "$out" >/dev/null || replace_fail "snapshot into $out failed"
  printf '%s\n' "$REPLACE_DB_QUERIES" | while IFS= read -r query; do
    rows=$(sqlite3 -cmd '.mode quote' "$out/db/quorum.db" "$query") \
      || { printf 'db query failed\n'; continue; }
    printf '%s\n' "$rows" | grep . | sed "s|^|db $query => |"
  done >>"$out/manifest"
  [ ! -f "$out/status.projection" ] \
    || sed 's/^/status /' "$out/status.projection" >>"$out/manifest"
  rm -f "$out/log-ref"
}

# Fail unless snapshot $1 holds non-vacuous evidence for every artifact class.
replace_assert_recorded() {
  manifest=$1/manifest
  ! grep -q '^db query failed' "$manifest" || replace_fail 'DB query failed'
  for class in 'repo head ' 'repo file ' 'config sha256 ' 'worktree file ' \
    'log ' 'status ' 'db SELECT \* FROM tasks ' 'db SELECT \* FROM task_notes ' \
    'db SELECT \* FROM pinned ' 'db SELECT \* FROM control ' \
    'db SELECT \* FROM messages '; do
    if ! grep -q "^$class" "$manifest"; then
      sed 's/^/  /' "$manifest" >&2
      replace_fail "snapshot recorded no '$class' evidence"
    fi
  done
}

# Exit 0 iff snapshots $1 and $2 record identical artifacts; prints the diff.
replace_snapshots_equal() {
  diff -u "$1/manifest" "$2/manifest" >&2 \
    && cmp "$1/config.bytes" "$2/config.bytes" >&2 \
    && cmp "$1/status.projection" "$2/status.projection" >&2
}

replace_assert_unchanged() {
  replace_snapshots_equal "$1" "$2" \
    || replace_fail 'recorded artifacts changed across replacement'
}

# Negative self-check: altering any one recorded artifact class must make the
# comparator fail.
replace_comparator_selftest() {
  base=$1
  probe=$REPLACE_WORK/selftest
  for class in 'repo ' 'config ' 'worktree ' 'log ' 'db ' 'status '; do
    rm -rf "$probe"
    cp -R "$base" "$probe"
    awk -v class="$class" '
      !done && index($0, class) == 1 { $0 = $0 "x"; done = 1 } { print }
    ' "$base/manifest" >"$probe/manifest"
    if replace_snapshots_equal "$base" "$probe" 2>/dev/null; then
      replace_fail "comparator accepted altered '$class' evidence"
    fi
  done
  rm -rf "$probe"
  cp -R "$base" "$probe"
  printf '#\n' >>"$probe/config.bytes"
  if replace_snapshots_equal "$base" "$probe" 2>/dev/null; then
    replace_fail 'comparator accepted altered config bytes'
  fi
  rm -rf "$probe"
}

# Deliberately alter one recorded artifact between containers (comparator
# demonstration; the suite must then fail). Classes: repo config db log
# worktree.
replace_tamper() {
  case "$1" in
    repo) replace_root_sh '
        printf "tampered\n" >>/data/repos/project/src/payload.txt' ;;
    config) replace_root_sh '
        printf "# tampered\n" >>"/data/quorum/serve/$REPLACE_SLUG.toml"' ;;
    db) replace_cli_sh 'quorum unpin --agent replace-seed --id 1 >/dev/null' \
        </dev/null ;;
    log) replace_root_sh '
        for log in $(find /data/quorum/logs -type f); do : >"$log"; done' ;;
    worktree) replace_root_sh '
        printf "tampered\n" >>/data/worktrees/operator-scratch/notes.txt' ;;
    *) replace_fail "unknown tamper class: $1" ;;
  esac
  printf '%s: tampered %s artifact between containers\n' "$REPLACE_SUITE" "$1" >&2
}
