#!/bin/sh
# Real linux/amd64 container verification for the default tini service.
set -eu

IMAGE=${1:-quorum:local}
PLATFORM=linux/amd64
WORK=$(mktemp -d)
SUFFIX=$$
PRIMARY=quorum-primary-$SUFFIX
WEB_HOG=quorum-web-hog-$SUFFIX
LOCK_HOLDER=quorum-lock-holder-$SUFFIX
LOCK_CONTENDER=quorum-lock-contender-$SUFFIX
PROVIDER=quorum-provider-$SUFFIX
SCHEMA_HELPER=quorum-schema-helper-$SUFFIX
DATA_VOLUME=quorum-data-$SUFFIX
PROVIDER_VOLUME=quorum-provider-data-$SUFFIX
CONTAINERS="$PRIMARY $WEB_HOG $LOCK_HOLDER $LOCK_CONTENDER $PROVIDER $SCHEMA_HELPER"
VOLUMES="$DATA_VOLUME $PROVIDER_VOLUME"

cleanup() {
  docker rm -f $CONTAINERS >/dev/null 2>&1 || true
  docker volume rm -f $VOLUMES >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT INT TERM

fail() {
  printf 'supervise: %s\n' "$1" >&2
  for container in $CONTAINERS; do
    if docker inspect "$container" >/dev/null 2>&1; then
      docker logs "$container" >&2 || true
    fi
  done
  exit 1
}

assert_eq() {
  expected=$1 actual=$2 label=$3
  [ "$expected" = "$actual" ] \
    || fail "$label: expected $expected, got $actual"
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
  container=$1 port=${2:-8080}
  response=$(docker exec "$container" timeout 3 git ls-remote \
    "http://127.0.0.1:$port/" 2>&1 || true)
  [ "$response" = "fatal: repository 'http://127.0.0.1:$port/' not found" ]
}

wait_ready() {
  container=$1
  tries=0
  while [ "$tries" -lt 40 ]; do
    if docker exec "$container" quorum status --json 2>/dev/null \
      | grep -q '"Alive"' && http_responds "$container"; then
      return 0
    fi
    running=$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || echo false)
    [ "$running" = true ] || return 1
    tries=$((tries + 1))
    sleep 1
  done
  return 1
}

wait_stopped() {
  container=$1 limit=${2:-20}
  tries=0
  while [ "$tries" -lt "$limit" ]; do
    running=$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || echo missing)
    [ "$running" = false ] && return 0
    tries=$((tries + 1))
    sleep 1
  done
  return 1
}

run_default() {
  container=$1 volume=$2
  shift 2
  docker run -d --platform "$PLATFORM" --name "$container" \
    -e QUORUM_REPO=acme/widget \
    -v "$volume:/data" "$@" "$IMAGE"
}

prepare_volume "$DATA_VOLUME"

# One default run creates persistent public state and makes both children ready.
run_default "$PRIMARY" "$DATA_VOLUME" >/dev/null
wait_ready "$PRIMARY" || fail 'default container never reached daemon + HTTP readiness'
assert_eq 10001:10001 \
  "$(docker exec "$PRIMARY" sh -c 'printf "%s:%s" "$(id -u)" "$(id -g)"')" \
  'numeric runtime identity'
assert_eq 10001:10001 "$(docker inspect -f '{{.Config.User}}' "$PRIMARY")" \
  'configured numeric identity'
status=$(docker exec "$PRIMARY" quorum status --json)
printf '%s\n' "$status" | grep -q '"Alive"' \
  || fail 'status did not expose daemon authority'
http_responds "$PRIMARY" || fail 'loopback Web did not return a real HTTP response'
processes=$(docker top "$PRIMARY")
printf '%s\n' "$processes" | grep -q 'quorum serve' || fail 'serve child missing'
printf '%s\n' "$processes" | grep -q 'quorum web' || fail 'Web child missing'
docker exec "$PRIMARY" sh -ec '
  test -d /data/worktrees
  test -d /data/quorum/logs
  test -d /data/quorum/init
  test -f /data/quorum/serve/acme__widget.toml
  grep -q "runner = \"codex\"" /data/quorum/serve/acme__widget.toml
  ! grep -qi claude /data/quorum/serve/acme__widget.toml
  test ! -e /data/repos/project/.claude/skills/quorum/SKILL.md
' || fail 'persistent paths, Codex routing, or out-of-checkout init contract failed'

docker stop -t 20 "$PRIMARY" >/dev/null
wait_stopped "$PRIMARY" || fail 'clean SIGTERM did not stop the default container'
assert_eq 0 "$(docker inspect -f '{{.State.ExitCode}}' "$PRIMARY")" \
  'clean SIGTERM exit'
assert_eq 0 "$(docker inspect -f '{{.State.Pid}}' "$PRIMARY")" \
  'stopped container init PID'
if docker top "$PRIMARY" >/dev/null 2>&1; then
  fail 'stopped container still exposes child processes'
fi
docker rm "$PRIMARY" >/dev/null
printf 'supervise: default init/readiness/identity/clean-stop passed\n'

# Explicit command replacement bypasses the default supervisor command while
# retaining tini as PID 1.
docker run --rm --platform "$PLATFORM" "$IMAGE" quorum --help >/dev/null \
  || fail 'explicit command override failed'

# A real bind collision makes Web fail first; the service exits documented 1
# and does not internally respawn either child.
docker run -d --platform "$PLATFORM" --name "$WEB_HOG" \
  -e QUORUM_REPO=acme/widget \
  -v "$DATA_VOLUME:/data" "$IMAGE" quorum web --port 8080 --bind 127.0.0.1 \
  --log-dir /data/quorum/logs >/dev/null
tries=0
while ! http_responds "$WEB_HOG"; do
  tries=$((tries + 1))
  [ "$tries" -lt 15 ] || fail 'bind-collision Web holder never became ready'
  sleep 1
done
run_default "$PRIMARY" "$DATA_VOLUME" --network "container:$WEB_HOG" >/dev/null
wait_stopped "$PRIMARY" || fail 'Web bind collision did not terminate service'
assert_eq 1 "$(docker inspect -f '{{.State.ExitCode}}' "$PRIMARY")" \
  'Web-first bind collision exit'
docker rm "$PRIMARY" >/dev/null
docker stop -t 10 "$WEB_HOG" >/dev/null
docker rm "$WEB_HOG" >/dev/null
printf 'supervise: Web-first bind collision passed\n'

# Run quorum itself as PID 1 in two isolated PID namespaces. Both daemon
# processes therefore have numeric PID 1, while the instance-identity lock must
# still reject the second container sharing /data.
docker run -d --platform "$PLATFORM" --name "$LOCK_HOLDER" \
  --entrypoint /usr/local/bin/quorum \
  -e QUORUM_REPO=acme/widget -v "$DATA_VOLUME:/data" "$IMAGE" \
  serve --config /data/quorum/serve/acme__widget.toml --repo acme/widget \
  --repo-dir /data/repos/project --worktree-base /data/worktrees \
  --log-dir /data/quorum/logs >/dev/null
tries=0
while ! docker exec "$LOCK_HOLDER" quorum status --json 2>/dev/null \
  | grep -q '"Alive":{"pid":1'; do
  tries=$((tries + 1))
  [ "$tries" -lt 20 ] || fail 'PID-1 lock holder never acquired daemon authority'
  sleep 1
done
lock_code=0
docker run --platform "$PLATFORM" --name "$LOCK_CONTENDER" \
  --entrypoint /usr/local/bin/quorum \
  -e QUORUM_REPO=acme/widget -v "$DATA_VOLUME:/data" "$IMAGE" \
  serve --config /data/quorum/serve/acme__widget.toml --repo acme/widget \
  --repo-dir /data/repos/project --worktree-base /data/worktrees \
  --log-dir /data/quorum/logs >"$WORK/lock-contender.log" 2>&1 || lock_code=$?
assert_eq 2 "$lock_code" 'second same-PID container lock rejection'
grep -q 'another daemon (pid 1) is already serving this DB' "$WORK/lock-contender.log" \
  || fail 'lock rejection did not identify the live PID-1 holder'
docker rm "$LOCK_CONTENDER" >/dev/null
docker stop -t 20 "$LOCK_HOLDER" >/dev/null
assert_eq 0 "$(docker inspect -f '{{.State.ExitCode}}' "$LOCK_HOLDER")" \
  'PID-1 lock holder clean stop'
docker rm "$LOCK_HOLDER" >/dev/null
printf 'supervise: cross-container matching-PID daemon lock passed\n'

# Fresh-data managed dispatch must execute the Codex runner selected by the
# image's generated config. Claude is deliberately absent from the image.
prepare_volume "$PROVIDER_VOLUME"
docker run --rm --platform "$PLATFORM" --user 0 -v "$PROVIDER_VOLUME:/data" \
  -v "$(dirname "$0")/fake-codex.sh:/tmp/fake-codex.sh:ro" \
  "$IMAGE" sh -ec '
  mkdir -p /data/fake-bin
  cp /tmp/fake-codex.sh /data/fake-bin/codex
  chmod 0555 /data/fake-bin/codex
  chown -R 10001:10001 /data/fake-bin
' >/dev/null
run_default "$PROVIDER" "$PROVIDER_VOLUME" \
  -e PATH=/data/fake-bin:/opt/codex/bin:/usr/local/bin:/usr/bin:/bin >/dev/null
wait_ready "$PROVIDER" || fail 'fake-provider container never became ready'
printf 'prove fresh Codex routing\n' | docker exec -i "$PROVIDER" \
  quorum task-create --created-by verifier --title 'provider selection proof' \
  --body-stdin >/dev/null
tries=0
while ! docker exec "$PROVIDER" test -s /data/codex-invocations 2>/dev/null; do
  tries=$((tries + 1))
  [ "$tries" -lt 30 ] || fail 'first managed spawn never invoked Codex'
  sleep 1
done
invocation=$(docker exec "$PROVIDER" sed -n '1p' /data/codex-invocations)
printf '%s\n' "$invocation" | grep -q '^exec --json --model gpt-5.6-terra ' \
  || fail "first managed Codex invocation had unexpected arguments: $invocation"
if docker exec "$PROVIDER" sh -c 'command -v claude' >/dev/null 2>&1; then
  fail 'public image unexpectedly contains Claude'
fi
docker stop -t 20 "$PROVIDER" >/dev/null
assert_eq 0 "$(docker inspect -f '{{.State.ExitCode}}' "$PROVIDER")" \
  'fake-provider clean stop'
docker rm "$PROVIDER" >/dev/null
printf 'supervise: fresh-data first managed spawn selected bundled Codex routing\n'

# A newer persistent schema fails before children start and preserves exit 3.
docker run -d --platform "$PLATFORM" --name "$SCHEMA_HELPER" --user 0 \
  -v "$DATA_VOLUME:/data" \
  "$IMAGE" sh -c 'sleep 30' >/dev/null
docker cp "$SCHEMA_HELPER:/data/quorum/repos/acme__widget/quorum.db" \
  "$WORK/quorum.db"
sqlite3 "$WORK/quorum.db" 'PRAGMA user_version=99999;'
docker cp "$WORK/quorum.db" \
  "$SCHEMA_HELPER:/data/quorum/repos/acme__widget/quorum.db"
docker stop -t 1 "$SCHEMA_HELPER" >/dev/null
docker rm "$SCHEMA_HELPER" >/dev/null
run_default "$PRIMARY" "$DATA_VOLUME" >/dev/null
wait_stopped "$PRIMARY" || fail 'schema-too-new container did not stop'
assert_eq 3 "$(docker inspect -f '{{.State.ExitCode}}' "$PRIMARY")" \
  'schema-too-new exit'
docker rm "$PRIMARY" >/dev/null
printf 'supervise: schema-too-new exit 3 passed\n'

printf 'supervise: all real-container cases passed\n'
