#!/bin/sh
set -eu

image="${1:-quorum:local}"

assert_eq() {
  expected="$1"
  actual="$2"
  label="$3"
  if [ "$actual" != "$expected" ]; then
    printf 'smoke: %s: expected %s, got %s\n' "$label" "$expected" "$actual" >&2
    exit 1
  fi
}

identity="$(docker run --rm --platform linux/amd64 "$image" sh -c 'printf "%s:%s" "$(id -u)" "$(id -g)"')"
assert_eq "10001:10001" "$identity" "runtime identity"

docker run --rm --platform linux/amd64 "$image" sh -ec '
  test "$HOME" = /home/quorum
  test "$QUORUM_HOME" = /data/quorum
  test "$(getent passwd 10001 | cut -d: -f1,6,7)" = "quorum:/home/quorum:/bin/sh"
  test "$(getent group 10001 | cut -d: -f1)" = quorum
  touch /data/quorum/.writable /data/repos/.writable /data/worktrees/.writable
  quorum --help >/dev/null
  QUORUM_REPO=smoke/project quorum init >/dev/null
  test -f /data/quorum/repos/smoke__project/quorum.db
  git --version | grep -F "2.39.5" >/dev/null
  gh --version | grep -F "gh version 2.23.0" >/dev/null
  codex --version | grep -F "codex-cli 0.146.0" >/dev/null
  tini --version 2>&1 | grep -F "tini version 0.19.0" >/dev/null
  test -f /usr/share/doc/codex/LICENSE
  test -f /usr/share/doc/codex/NOTICE
  test -f /usr/share/quorum/serve-codex.toml
  for path in /usr/local/bin/quorum /opt/codex/bin/codex /usr/bin/git /usr/bin/gh /etc/passwd; do
    test ! -w "$path"
    if sh -c "printf x >> \"\$1\"" sh "$path" 2>/dev/null; then
      printf "smoke: runtime user mutated root-owned path %s\n" "$path" >&2
      exit 1
    fi
  done
'

printf 'smoke: %s passed\n' "$image"
