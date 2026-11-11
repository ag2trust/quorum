#!/bin/sh
# Real-network verification: loopback Web is reachable only from inside the
# Quorum container's network namespace.
set -eu

IMAGE=${1:-quorum:local}
PLATFORM=linux/amd64
PORT=8080
WRONG_PORT=8081
CONNECT_SECS=3   # bound for every single HTTP attempt
READY_TRIES=15   # bound for Web startup polling (1s apart)
SUFFIX=$$
NETWORK=quorum-net-$SUFFIX
QUORUM=quorum-netns-$SUFFIX
PEER=quorum-peer-$SUFFIX
CONTAINERS="$QUORUM $PEER"

cleanup() {
  docker rm -f $CONTAINERS >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

fail() {
  printf 'network-verify: %s\n' "$1" >&2
  if docker inspect "$QUORUM" >/dev/null 2>&1; then
    docker logs "$QUORUM" >&2 || true
    docker exec "$QUORUM" cat /tmp/web.log >&2 2>/dev/null || true
  fi
  exit 1
}

# One bounded smart-HTTP request from a fresh helper container. The pinned
# in-image git reports `repository ... not found` only after it receives a
# real non-Git HTTP response (same client as quorum-health readiness).
#   probe <docker-run network args...> -- <url>
probe() {
  args=
  while [ "$1" != -- ]; do args="$args $1"; shift; done
  url=$2
  # shellcheck disable=SC2086
  docker run --rm --platform "$PLATFORM" $args --entrypoint /usr/bin/timeout \
    "$IMAGE" -k 1 "$CONNECT_SECS" git ls-remote "$url" 2>&1 || true
}

http_ok() {
  [ "$1" = "fatal: repository '$2' not found" ]
}

docker network create "$NETWORK" >/dev/null

# The Quorum container stays up independently of Web so Web can be stopped
# while the namespace remains joinable.
docker run -d --platform "$PLATFORM" --name "$QUORUM" --network "$NETWORK" \
  -e QUORUM_REPO=acme/widget "$IMAGE" sleep 600 >/dev/null
docker exec "$QUORUM" sh -c "
  mkdir -p /tmp/logs
  quorum web --port $PORT --bind 127.0.0.1 --log-dir /tmp/logs \
    >/tmp/web.log 2>&1 &
  echo \$! >/tmp/web.pid
"

# (5) No published or exposed ports on the running container or image.
assert_no_ports() {
  value=$(docker inspect -f "$1" "$2")
  case "$value" in
    ''|'map[]'|'<no value>') ;;
    *) fail "$3: expected none, got $value" ;;
  esac
}
assert_no_ports '{{.Config.ExposedPorts}}' "$IMAGE" 'image exposed ports'
assert_no_ports '{{.Config.ExposedPorts}}' "$QUORUM" 'container exposed ports'
assert_no_ports '{{.HostConfig.PortBindings}}' "$QUORUM" 'container port bindings'
assert_no_ports '{{.NetworkSettings.Ports}}' "$QUORUM" 'container published ports'
printf 'network-verify: no published or exposed ports\n'

# (1) A same-namespace helper reaches loopback Web with a real HTTP response.
url=http://127.0.0.1:$PORT/
tries=0
while :; do
  response=$(probe --network "container:$QUORUM" -- "$url")
  http_ok "$response" "$url" && break
  tries=$((tries + 1))
  [ "$tries" -lt "$READY_TRIES" ] \
    || fail "same-namespace helper never got an HTTP response: $response"
  sleep 1
done
printf 'network-verify: same-namespace helper reached loopback Web\n'

# (2) A peer on the same user-defined bridge cannot reach Web at the Quorum
# container's IP. The peer is a long-lived container so the failure is a
# bounded connect attempt, not a DNS or network setup error.
ip=$(docker inspect -f "{{(index .NetworkSettings.Networks \"$NETWORK\").IPAddress}}" "$QUORUM")
[ -n "$ip" ] || fail 'Quorum container has no bridge IP'
docker run -d --platform "$PLATFORM" --name "$PEER" --network "$NETWORK" \
  "$IMAGE" sleep 600 >/dev/null
peer_url=http://$ip:$PORT/
peer_code=0
peer_response=$(docker exec "$PEER" timeout -k 1 "$CONNECT_SECS" \
  git ls-remote "$peer_url" 2>&1) || peer_code=$?
[ "$peer_code" -ne 0 ] || fail 'bridge peer request unexpectedly succeeded'
http_ok "$peer_response" "$peer_url" \
  && fail 'bridge peer received an HTTP response from Web'
printf 'network-verify: bridge peer cannot reach Web at %s:%s\n' "$ip" "$PORT"

# (3a) The same-namespace helper fails on a wrong port.
wrong_url=http://127.0.0.1:$WRONG_PORT/
http_ok "$(probe --network "container:$QUORUM" -- "$wrong_url")" "$wrong_url" \
  && fail 'same-namespace helper got an HTTP response on the wrong port'
printf 'network-verify: same-namespace helper fails on wrong port\n'

# (3b) After Web stops, the same-namespace helper fails.
docker exec "$QUORUM" sh -c 'kill "$(cat /tmp/web.pid)"'
tries=0
while docker exec "$QUORUM" sh -c 'kill -0 "$(cat /tmp/web.pid)"' 2>/dev/null; do
  tries=$((tries + 1))
  [ "$tries" -lt 10 ] || fail 'Web did not stop after TERM'
  sleep 1
done
http_ok "$(probe --network "container:$QUORUM" -- "$url")" "$url" \
  && fail 'same-namespace helper got an HTTP response after Web stopped'
printf 'network-verify: same-namespace helper fails after Web stops\n'

# (4) A non-loopback bind is rejected (validate_loopback usage error, exit 2).
bind_code=0
bind_output=$(docker run --rm --platform "$PLATFORM" -e QUORUM_REPO=acme/widget \
  "$IMAGE" timeout -k 1 10 quorum web --bind 0.0.0.0 --port "$PORT" \
  --log-dir /tmp 2>&1) || bind_code=$?
[ "$bind_code" -ne 0 ] || fail 'quorum web --bind 0.0.0.0 unexpectedly succeeded'
[ "$bind_code" -ne 124 ] || fail 'quorum web --bind 0.0.0.0 started serving'
printf '%s\n' "$bind_output" | grep -q 'loopback-only' \
  || fail "0.0.0.0 bind failed for an unexpected reason (exit $bind_code): $bind_output"
printf 'network-verify: 0.0.0.0 bind rejected with exit %s\n' "$bind_code"

printf 'network-verify: all real-network cases passed\n'
