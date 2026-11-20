#!/bin/sh
# Fail closed on image content that must not ship, or that must ship and is
# missing.
#
# Usage: docker/content-verify.sh IMAGE [CONTEXT_DIR]
#
#   1. The image is linux/amd64 (checked first; nothing else is trusted
#      otherwise).
#   2. OCI labels source, revision, version, licenses and description are
#      present and nonempty. QUORUM_EXPECT_VERSION / QUORUM_EXPECT_REVISION,
#      when set, must equal the version / revision labels.
#   3. Image config (env, labels, entrypoint, cmd) and `docker history`
#      contain no credential-like assignment or token value and no
#      hosting/tenancy material.
#   4. The filesystem carries the Quorum LICENSE and Codex LICENSE/NOTICE, and
#      no VCS metadata, Cargo build output, Rust source trees, local databases,
#      auth/credential files, SSH private keys, or PEM private keys.
#   5. The build context that .dockerignore admits from CONTEXT_DIR (default:
#      the repository root) holds no such inputs and stays under
#      CONTEXT_LIMIT_KIB.
#   6. The shipped agent-facing provider config
#      (/usr/share/quorum/serve-codex.toml) contains no credential-like
#      assignment, token-shaped value, or Quorum database path. Every managed
#      role reads only this file (or an operator-owned equivalent outside the
#      image); it must never carry GitHub or daemon-home authority.
#
# Diagnostics name the offending class and location, never a matched value.
# Exit codes: 0 verified · 1 forbidden/missing content · 2 usage.
set -eu

CONTEXT_LIMIT_KIB=32768

[ "$#" -ge 1 ] && [ "$#" -le 2 ] || {
  printf 'usage: %s IMAGE [CONTEXT_DIR]\n' "$0" >&2
  exit 2
}
image="$1"
context="${2:-$(dirname "$0")/..}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
failures=0

fail() {
  printf 'content-verify: %s\n' "$*" >&2
  failures=$((failures + 1))
}

# 1. Architecture.
architecture="$(docker image inspect -f '{{.Os}}/{{.Architecture}}' "$image")"
if [ "$architecture" != linux/amd64 ]; then
  printf 'content-verify: expected linux/amd64 image, got %s\n' "$architecture" >&2
  exit 1
fi

# 2. Required labels.
label() {
  docker image inspect -f "{{index .Config.Labels \"org.opencontainers.image.$1\"}}" "$image"
}
for name in source revision version licenses description; do
  value="$(label "$name")"
  if [ -z "$value" ] || [ "$value" = '<no value>' ]; then
    fail "missing OCI label org.opencontainers.image.$name"
  fi
done
if [ -n "${QUORUM_EXPECT_VERSION:-}" ] && [ "$(label version)" != "$QUORUM_EXPECT_VERSION" ]; then
  fail "version label is not the expected build-arg value"
fi
if [ -n "${QUORUM_EXPECT_REVISION:-}" ] && [ "$(label revision)" != "$QUORUM_EXPECT_REVISION" ]; then
  fail "revision label is not the expected build-arg value"
fi

# 3. Config and history. Build args consumed by RUN appear in history as
# `|N NAME=value`, so a secret passed with --build-arg is caught here.
docker image inspect -f '{{json .Config}}' "$image" >"$work/config"
docker history --no-trunc --format '{{.CreatedBy}}' "$image" >"$work/history"

# scan FILE CLASS GREP_ARGS...: report matching line numbers only.
scan() {
  file="$1" class="$2"
  shift 2
  lines="$(grep -n "$@" "$file" | cut -d: -f1 | tr '\n' ' ')" || true
  if [ -n "$lines" ]; then
    fail "$class in image $(basename "$file") (line ${lines% })"
  fi
}
for source in "$work/config" "$work/history"; do
  scan "$source" 'credential-like assignment' -i -E \
    '(^|[^A-Za-z0-9_])[A-Za-z0-9_]*(TOKEN|SECRET|PASSWORD|PASSWD|API_?KEY|ACCESS_?KEY|PRIVATE_?KEY|CREDENTIALS?)[A-Za-z0-9_]*=[^[:space:]",]'
  scan "$source" 'credential-like assignment' -E \
    '(^|[^A-Za-z0-9_])[A-Z0-9_]*AUTH[A-Z0-9_]*=[^[:space:]",]'
  scan "$source" 'token-shaped value' -E \
    '(gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|glpat-[A-Za-z0-9_-]{20,}|(^|[^A-Za-z0-9])sk-[A-Za-z0-9_-]{20,}|AKIA[0-9A-Z]{16}|xox[abposr]-[A-Za-z0-9-]{10,})'
  scan "$source" 'private key material' -E -- '-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----'
  scan "$source" 'hosting/tenancy material' -i -E '(tenant|tenancy|billing|customer)'
done

# 4. Filesystem, scanned as root so unreadable paths cannot hide content.
# /data is included: Docker seeds an anonymous volume from image content. The
# sentinel proves the scan ran to completion rather than dying early.
if ! docker run --rm --platform linux/amd64 --network none --user 0:0 \
  --entrypoint /bin/sh "$image" -c '
    for path in /usr/share/doc/quorum/LICENSE /usr/share/doc/codex/LICENSE \
      /usr/share/doc/codex/NOTICE; do
      [ -f "$path" ] && [ -s "$path" ] || printf "MISSING %s\n" "$path"
    done
    find / \( -path /proc -o -path /sys -o -path /dev \) -prune -o \( \
      -name .git -o -name .gitmodules -o \( -type d -name target \) \
      -o -name "*.rs" -o -name Cargo.toml -o -name Cargo.lock \
      -o -name "*.db" -o -name "*.db-wal" -o -name "*.db-shm" \
      -o -name "*.sqlite" -o -name "*.sqlite3" \
      -o -name auth.json -o -name .credentials.json -o -name .git-credentials \
      -o -name .netrc -o -name .env -o -path "*/gh/hosts.yml" \
      -o -name "id_rsa*" -o -name "id_dsa*" -o -name "id_ecdsa*" \
      -o -name "id_ed25519*" \
      \) -print | sed "s/^/FORBIDDEN /"
    find / \( -path /proc -o -path /sys -o -path /dev \) -prune -o -type f \
      -exec grep -l -I -E -e "-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----" {} + 2>/dev/null \
      | sed "s/^/KEY /"
    provider_config=/usr/share/quorum/serve-codex.toml
    if [ -f "$provider_config" ]; then
      if grep -q -i -E \
        "(^|[^A-Za-z0-9_])[A-Za-z0-9_]*(TOKEN|SECRET|PASSWORD|PASSWD|API_?KEY|ACCESS_?KEY|PRIVATE_?KEY|CREDENTIALS?)[A-Za-z0-9_]*[[:space:]]*=" \
        "$provider_config" 2>/dev/null \
        || grep -q -E \
          "(gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|glpat-[A-Za-z0-9_-]{20,}|(^|[^A-Za-z0-9])sk-[A-Za-z0-9_-]{20,}|AKIA[0-9A-Z]{16}|xox[abposr]-[A-Za-z0-9-]{10,})" \
          "$provider_config" 2>/dev/null; then
        printf "CONFIG-CRED %s\n" "$provider_config"
      fi
      if grep -q -i -E "(quorum\.db|\.sqlite3?|/data/quorum|QUORUM_HOME)" \
        "$provider_config" 2>/dev/null; then
        printf "CONFIG-DBPATH %s\n" "$provider_config"
      fi
    fi
    printf "SCAN-COMPLETE\n"
  ' >"$work/fs" 2>"$work/fs.err"; then
  fail "filesystem scan could not run: $(head -c 400 "$work/fs.err")"
elif ! grep -qx SCAN-COMPLETE "$work/fs"; then
  fail "filesystem scan did not complete"
else
  while IFS=' ' read -r kind path; do
    case "$kind" in
      MISSING) fail "missing required file $path" ;;
      FORBIDDEN) fail "forbidden path in image: $path" ;;
      KEY) fail "private key material in image file: $path" ;;
      CONFIG-CRED) fail "credential-like value in agent-facing provider config: $path" ;;
      CONFIG-DBPATH) fail "Quorum database path in agent-facing provider config: $path" ;;
    esac
  done <"$work/fs"
fi

# 5. Build context. Export exactly what .dockerignore admits, then inspect it.
if ! printf 'FROM scratch\nCOPY . /\n' | docker buildx build --quiet \
  --platform linux/amd64 -f - --output "type=local,dest=$work/context" \
  "$context" >/dev/null 2>"$work/context.err"; then
  fail "build context export failed: $(head -c 400 "$work/context.err")"
else
  (cd "$work/context" && find . \( -name .git -o -name target -o -name worktrees \
    -o -name .quorum -o -name "*.db" -o -name "*.db-wal" -o -name "*.db-shm" \
    -o -name "*.sqlite" -o -name "*.sqlite3" -o -name .env -o -name ".env.*" \
    -o -name auth.json -o -name .credentials.json -o -name .git-credentials \
    -o -name "id_rsa*" -o -name "id_dsa*" -o -name "id_ecdsa*" \
    -o -name "id_ed25519*" -o -name "*.pem" -o -name "*.key" \) -print) \
    >"$work/context.bad"
  while IFS= read -r path; do
    fail "forbidden build-context input: ${path#./}"
  done <"$work/context.bad"
  size_kib="$(du -sk "$work/context" | cut -f1)"
  if [ "$size_kib" -gt "$CONTEXT_LIMIT_KIB" ]; then
    fail "build context is ${size_kib} KiB, over the ${CONTEXT_LIMIT_KIB} KiB bound"
  fi
fi

if [ "$failures" -ne 0 ]; then
  printf 'content-verify: %s failure(s) for %s\n' "$failures" "$image" >&2
  exit 1
fi
printf 'content-verify: labels, licenses, forbidden data and build context (%s KiB) verified\n' \
  "$size_kib"
