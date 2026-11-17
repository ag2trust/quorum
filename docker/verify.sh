#!/bin/sh
set -eu

image="${1:-quorum:local}"
log_dir="$(mktemp -d)"
# Derived negative-fixture images are built inside pipelines (subshells), so
# their tags are recorded in a file for cleanup.
cleanup() {
  if [ -s "$log_dir/derived-tags" ]; then
    xargs docker image rm <"$log_dir/derived-tags" >/dev/null 2>&1 || true
  fi
  rm -rf "$log_dir"
}
trap cleanup EXIT

architecture="$(docker image inspect -f '{{.Os}}/{{.Architecture}}' "$image")"
if [ "$architecture" != linux/amd64 ]; then
  printf 'verify: expected linux/amd64 image, got %s\n' "$architecture" >&2
  exit 1
fi
printf 'verify: image architecture linux/amd64\n'

"$(dirname "$0")/pin-check_test.sh" >/dev/null
"$(dirname "$0")/pin-check.sh"
"$(dirname "$0")/content-verify.sh" "$image"

# expect_content_failure LABEL NEEDLE IMAGE [CONTEXT]: content-verify must exit
# 1 and name the violation.
expect_content_failure() {
  if "$(dirname "$0")/content-verify.sh" "$3" ${4:+"$4"} \
    >"$log_dir/content.out" 2>"$log_dir/content.err"; then
    printf 'verify: content-verify accepted %s\n' "$1" >&2
    exit 1
  fi
  if ! grep -F -- "$2" "$log_dir/content.err" >/dev/null; then
    printf 'verify: content-verify rejected %s for an unexpected reason\n' "$1" >&2
    cat "$log_dir/content.err" >&2
    exit 1
  fi
  printf 'verify: content-verify rejects %s\n' "$1"
}

# negative NAME LABEL NEEDLE [PLATFORM] < DOCKERFILE: build a context-free
# derived image (Dockerfile on stdin) and require content-verify to reject it.
negative() {
  tag="quorum-content-negative:$1-$$"
  printf '%s\n' "$tag" >>"$log_dir/derived-tags"
  docker buildx build --quiet --load --platform "${4:-linux/amd64}" \
    --tag "$tag" - >/dev/null
  expect_content_failure "$2" "$3" "$tag"
}

printf 'FROM scratch\nLABEL org.opencontainers.image.source=x\n' \
  | negative arm64 'a non-amd64 image' 'expected linux/amd64 image, got linux/arm64' linux/arm64
printf 'FROM %s\nLABEL org.opencontainers.image.revision=""\n' "$image" \
  | negative unlabeled 'an empty revision label' 'missing OCI label org.opencontainers.image.revision'
printf 'FROM %s\nUSER 0\nRUN rm /usr/share/doc/quorum/LICENSE\n' "$image" \
  | negative no-license 'a stripped Quorum LICENSE' 'missing required file /usr/share/doc/quorum/LICENSE'
printf 'FROM %s\nUSER 0\nRUN rm /usr/share/doc/codex/LICENSE\n' "$image" \
  | negative no-codex-license 'a stripped Codex LICENSE' 'missing required file /usr/share/doc/codex/LICENSE'
printf 'FROM %s\nUSER 0\nRUN rm /usr/share/doc/codex/NOTICE\n' "$image" \
  | negative no-notice 'a stripped Codex NOTICE' 'missing required file /usr/share/doc/codex/NOTICE'
printf 'FROM %s\nENV GH_TOKEN=fixture-not-a-secret\n' "$image" \
  | negative env-token 'a credential env value' 'credential-like assignment in image config'
printf 'FROM %s\nARG SERVICE_API_KEY=fixture-not-a-secret\nRUN true\n' "$image" \
  | negative arg-secret 'a credential build arg in history' 'credential-like assignment in image history'
printf 'FROM %s\nLABEL org.example.tenant=fixture\n' "$image" \
  | negative tenancy 'hosting/tenancy material' 'hosting/tenancy material in image config'
printf 'FROM %s\nRUN mkdir -p /home/quorum/.codex && touch /home/quorum/.codex/auth.json\n' "$image" \
  | negative auth-json 'a planted auth.json' 'forbidden path in image: /home/quorum/.codex/auth.json'
printf 'FROM %s\nUSER 0\nRUN mkdir -p /opt/src/.git && touch /opt/src/main.rs\n' "$image" \
  | negative source-tree 'VCS metadata in the image' 'forbidden path in image: /opt/src/.git'
printf 'FROM %s\nUSER 0\nRUN mkdir -p /opt/src && touch /opt/src/main.rs\n' "$image" \
  | negative rust-source 'a Rust source tree' 'forbidden path in image: /opt/src/main.rs'
printf 'FROM %s\nRUN printf -- "-----BEGIN OPENSSH %%s-----\\n" "PRIVATE KEY" >/home/quorum/notes\n' "$image" \
  | negative ssh-key 'an SSH private key' 'private key material in image file: /home/quorum/notes'

# A context with no .dockerignore admits VCS metadata, local DBs and build
# output; an oversized one breaks the bound. The real .dockerignore filters the
# same inputs.
fixture="$log_dir/context"
mkdir -p "$fixture/.git" "$fixture/target/release" "$fixture/quorum/src"
: >"$fixture/.git/HEAD"
: >"$fixture/target/release/quorum"
: >"$fixture/quorum/quorum.db"
: >"$fixture/quorum/src/main.rs"
expect_content_failure 'a build context without .dockerignore' 'forbidden build-context input: .git' \
  "$image" "$fixture"
cp "$(dirname "$0")/../.dockerignore" "$fixture/.dockerignore"
"$(dirname "$0")/content-verify.sh" "$image" "$fixture" >/dev/null
printf 'verify: .dockerignore filters VCS, build output and local DBs from the context\n'
head -c 34000000 /dev/zero >"$fixture/quorum/src/oversized.rs"
expect_content_failure 'an oversized build context' 'KiB bound' "$image" "$fixture"

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
"$(dirname "$0")/health-verify.sh" "$image"
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
