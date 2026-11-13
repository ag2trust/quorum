#!/bin/sh
set -eu

image="${1:-quorum:local}"
log_dir="$(mktemp -d)"
trap 'rm -rf "$log_dir"' EXIT

architecture="$(docker image inspect -f '{{.Os}}/{{.Architecture}}' "$image")"
if [ "$architecture" != linux/amd64 ]; then
  printf 'verify: expected linux/amd64 image, got %s\n' "$architecture" >&2
  exit 1
fi
printf 'verify: image architecture linux/amd64\n'

"$(dirname "$0")/smoke.sh" "$image"
"$(dirname "$0")/supervise.sh" "$image"
"$(dirname "$0")/replace.sh" "$image"
# preflight.sh sets VERIFY_SKIP_REPLACE_SUITES to a skip reason only when the
# local Docker server is not amd64; left unset (the default, and always the
# case for required CI) both suites run. CI refuses the variable outright so
# a skip can never slip past the required container job.
if [ -z "${VERIFY_SKIP_REPLACE_SUITES:-}" ]; then
  "$(dirname "$0")/replace-abrupt.sh" "$image"
  "$(dirname "$0")/replace-negative.sh" "$image"
elif [ -n "${CI:-}" ] || [ -n "${GITHUB_ACTIONS:-}" ]; then
  printf 'verify: VERIFY_SKIP_REPLACE_SUITES is not allowed in CI\n' >&2
  exit 2
else
  printf 'verify: SKIPPED replace-abrupt.sh and replace-negative.sh (%s)\n' \
    "$VERIFY_SKIP_REPLACE_SUITES"
fi
"$(dirname "$0")/network-verify.sh" "$image"

if docker build \
  --platform linux/amd64 \
  --target codex-fetcher \
  --build-arg CODEX_SHA256=0000000000000000000000000000000000000000000000000000000000000000 \
  . >"$log_dir/checksum.log" 2>&1; then
  printf 'verify: invalid Codex checksum build unexpectedly passed\n' >&2
  exit 1
fi

if ! grep -F '/tmp/codex.tar.gz: FAILED' "$log_dir/checksum.log" >/dev/null; then
  printf 'verify: invalid checksum failed for an unexpected reason\n' >&2
  sed -n '1,160p' "$log_dir/checksum.log" >&2
  exit 1
fi

printf 'verify: invalid Codex checksum rejected before extraction\n'
