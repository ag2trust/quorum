#!/bin/sh
# Real-container integration + negative tests for the docker/entrypoint.sh supervisor.
# Exercises the actual `quorum:local` image (PID 1 = tini, default CMD = entrypoint.sh)
# against a throwaway git checkout mounted at /data. Strict POSIX (dash-compatible).
set -eu

image="${1:-quorum:local}"
work="$(mktemp -d)"
suffix="$$"
primary="qsup-$suffix"
webhog="qwebhog-$suffix"
trap 'docker rm -f "$primary" "$webhog" >/dev/null 2>&1 || true; rm -rf "$work"' EXIT

# Pre-provision a git checkout at the mounted /data/repos/project (entrypoint's default
# QUORUM_REPO_DIR). Shared across every case below.
git init -q "$work/repos/project"
( cd "$work/repos/project" && git -c user.email=t@t -c user.name=t commit -q --allow-empty -m init )
mkdir -p "$work/quorum" "$work/worktrees"

run() { # extra docker args...
  docker run -d --name "$primary" \
    -e QUORUM_REPO=acme/widget \
    -v "$work:/data" "$@" "$image"
}

# Prove that the listener returns the dashboard's known non-Git HTTP behavior.
# git reports this only after receiving an HTTP response without the expected
# Git smart-protocol content type; arbitrary client/network errors do not pass.
tcp_accepts() { # container port
  out="$(docker exec "$1" timeout 3 git ls-remote "http://127.0.0.1:$2/" 2>&1 || true)"
  expected="fatal: repository 'http://127.0.0.1:$2/' not found"
  [ "$out" = "$expected" ]
}

# Poll until the container has actually stopped, then return its exit code.
exit_code_after_stop() {
  n=0
  while [ "$n" -lt 10 ]; do
    running="$(docker inspect -f '{{.State.Running}}' "$primary" 2>/dev/null || echo unknown)"
    [ "$running" = "false" ] && break
    n=$((n + 1)); sleep 1
  done
  [ "$running" = "false" ] || { echo "supervise: $primary still running after ${n}s" >&2; docker logs "$primary" >&2 || true; exit 1; }
  docker inspect -f '{{.State.ExitCode}}' "$primary"
}

# =========================================================================
# Case 1 + 4: start/readiness, second-daemon lock rejection, clean stop.
# =========================================================================
run >/dev/null
ready=0
i=0; while [ "$i" -lt 30 ]; do
  if docker exec "$primary" quorum status --json 2>/dev/null | grep -q '"Alive"' \
     && tcp_accepts "$primary" 8080; then ready=1; break; fi
  i=$((i+1)); sleep 1
done
[ "$ready" = 1 ] || { echo "supervise: daemon/web never ready" >&2; docker logs "$primary" >&2; exit 1; }
echo "supervise: [1] first-run init + daemon/web TCP readiness passed"

# --- a second daemon inside the repository runtime must be rejected with exit 2 ---
# One active container owns each account/repository identity. Starting overlapping
# containers for the same identity is outside the supported topology; the runtime
# manager stops the prior container before replacement.
code=0
docker exec "$primary" quorum serve --repo acme/widget --repo-dir /data/repos/project \
  --worktree-base /data/worktrees --log-dir /data/quorum/logs \
  >"$work/second-daemon.out" 2>&1 || code=$?
[ "$code" = 2 ] || {
  echo "supervise: second daemon expected exit 2 got $code" >&2
  cat "$work/second-daemon.out" >&2; exit 1;
}
echo "supervise: [4] second daemon in one runtime rejected (exit 2) passed"

# --- clean stop: SIGTERM -> exit 0 ---
# Record that both relevant children existed inside this exact container.
before="$(docker top "$primary")"
printf '%s\n' "$before" | grep -q 'quorum serve'
printf '%s\n' "$before" | grep -q 'quorum web'
docker stop -t 30 "$primary" >/dev/null
code="$(exit_code_after_stop)"
[ "$code" = 0 ] || { echo "supervise: clean stop expected 0 got $code" >&2; docker logs "$primary" >&2; exit 1; }
# A stopped container must have no init PID or container-scoped processes.
[ "$(docker inspect -f '{{.State.Pid}}' "$primary")" = 0 ]
if docker top "$primary" >/dev/null 2>&1; then
  echo "supervise: stopped container still reports child processes" >&2; exit 1
fi
docker rm -f "$primary" >/dev/null
echo "supervise: [1] clean-stop (exit 0) passed"

# =========================================================================
# Case 2: web bind failure -> container exit 1.
#
# NOTE: the brief's suggested mechanism (QUORUM_WEB_PORT=1, a privileged port,
# expecting EACCES as non-root) does NOT reproduce here: this Docker host has
# net.ipv4.ip_unprivileged_port_start=0 (verified: `docker run --rm "$image"
# cat /proc/sys/net/ipv4/ip_unprivileged_port_start` prints 0), so UID 10001
# can bind port 1 like anyone else. Using EADDRINUSE instead, which is
# privilege-independent: a sidecar container running `quorum web` holds
# 127.0.0.1:8080 in its own netns, then qsup joins that netns
# (--network container:qwebhog) so its own web (default port 8080) collides.
# ---------------------------------------------------------------------------
docker run -d --name "$webhog" -e QUORUM_REPO=acme/widget -v "$work:/data" "$image" \
  quorum web --port 8080 --bind 127.0.0.1 --log-dir /data/quorum/logs >/dev/null
hogready=0
i=0; while [ "$i" -lt 10 ]; do
  if tcp_accepts "$webhog" 8080; then hogready=1; break; fi
  i=$((i+1)); sleep 1
done
[ "$hogready" = 1 ] || { echo "supervise: port-hog sidecar never accepted TCP" >&2; docker logs "$webhog" >&2; exit 1; }

run --network "container:$webhog" >/dev/null   # default QUORUM_WEB_PORT=8080 collides with the sidecar
code="$(exit_code_after_stop)"
[ "$code" = 1 ] || { echo "supervise: web-fail expected exit 1 got $code" >&2; docker logs "$primary" >&2; exit 1; }
docker rm -f "$primary" "$webhog" >/dev/null
echo "supervise: [2] web-fail (exit 1) passed"

# =========================================================================
# Case 3: schema-too-new at startup -> exit 3.
# =========================================================================
db="$work/quorum/repos/acme__widget/quorum.db"
sqlite3 "$db" 'PRAGMA user_version=99999;'
got="$(sqlite3 "$db" 'PRAGMA user_version;')"
[ "$got" = "99999" ] || { echo "supervise: failed to bump user_version, got $got" >&2; exit 1; }

run >/dev/null
code="$(exit_code_after_stop)"
[ "$code" = 3 ] || { echo "supervise: schema-too-new expected exit 3 got $code" >&2; docker logs "$primary" >&2; exit 1; }
docker rm -f "$primary" >/dev/null
echo "supervise: [3] schema-too-new (exit 3) passed"

echo "supervise: all cases passed"
