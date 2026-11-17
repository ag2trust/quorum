#!/bin/sh
# Release verification for a published Quorum image, addressed only by digest.
#
#   docker/release-verify.sh \
#     --repository ghcr.io/ag2trust/quorum \
#     --digest sha256:<manifest digest> \
#     --image-id sha256:<tested config/image ID> \
#     --version <expected version label> \
#     --revision <expected 40-hex source commit> \
#     --source-repo ag2trust/quorum \
#     --signer-workflow ag2trust/quorum/.github/workflows/<workflow>.yml \
#     [--anonymous]
#
# Checks, in order; any failure stops verification:
#   1. the registry manifest for <repository>@<digest> is one image manifest
#      (never an index) whose config digest is --image-id;
#   2. `docker pull <repository>@<digest>` (never a tag) succeeds, and the
#      pulled image is that digest, linux/amd64, with the expected ID and OCI
#      source/revision/version/licenses/description labels;
#   3. provenance (SLSA) and SBOM (SPDX or CycloneDX) attestations verify with
#      the runner's external, current `gh attestation verify`, constrained by
#      --repo, --signer-workflow, --source-digest, and --predicate-type. The
#      image's bundled gh 2.23 has no `attestation` command and is not used;
#   4. a bounded service check: docker/smoke.sh under a hard timeout, then one
#      container started from the pulled digest must report
#      `quorum-health readiness` ready before a hard deadline. The container,
#      its volume, and all temp state are removed on success and failure.
#
# --anonymous points DOCKER_CONFIG at a fresh, empty temp directory and unsets
# registry credential variables before any registry access, so the manifest
# read and the pull by digest are credential-free. An authenticated pull is
# never accepted as public-availability evidence. gh keeps its GitHub API
# token (attestation lookup is an API call, not a registry login).
#
# Exit: 0 every check passed · 1 a check failed · 2 usage/bad input ·
# 3 required host tooling is missing or too old.
set -u

HERE=$(cd "$(dirname "$0")" && pwd)
PLATFORM=linux/amd64

# Static labels from the Dockerfile LABEL instruction; release-verify_test.sh
# fails if these drift from the Dockerfile.
EXPECTED_LICENSES='MIT AND Apache-2.0'
EXPECTED_DESCRIPTION='Self-hostable Quorum runtime'

PROVENANCE_PREDICATE=https://slsa.dev/provenance/v1
# Any one verified SBOM predicate satisfies the SBOM requirement.
SBOM_PREDICATES='https://spdx.dev/Document/v2.3 https://spdx.dev/Document/v2.2 https://cyclonedx.org/bom'

# Hard bounds (seconds). Every external command runs under `timeout`.
STEP_SECS=${RELEASE_VERIFY_STEP_SECS:-300}      # one registry/docker/gh call
SMOKE_SECS=${RELEASE_VERIFY_SMOKE_SECS:-300}    # whole docker/smoke.sh run
READY_SECS=${RELEASE_VERIFY_READY_SECS:-150}    # container start → ready
PROBE_SECS=30                                   # one quorum-health exec
KILL_AFTER_SECS=5

NL='
'

log() { printf 'release-verify: %s\n' "$1"; }
fail() { printf 'release-verify: FAIL %s\n' "$1" >&2; exit 1; }
usage() {
  printf 'release-verify: usage: %s\n' "$1" >&2
  printf 'release-verify: see the header of docker/release-verify.sh\n' >&2
  exit 2
}
tooling() { printf 'release-verify: tooling: %s\n' "$1" >&2; exit 3; }

# Usage: matches <value> <extended-regex>. Whole-value match; rejects newlines.
matches() {
  case "$1" in *"$NL"*) return 1 ;; esac
  printf '%s\n' "$1" | grep -Eqx -- "$2"
}

bounded() {
  b_secs=$1
  shift
  timeout -k "$KILL_AFTER_SECS" "$b_secs" "$@"
}

for b_name in STEP_SECS SMOKE_SECS READY_SECS; do
  eval "b_value=\$$b_name"
  matches "$b_value" '[1-9][0-9]{0,4}' || usage "RELEASE_VERIFY_$b_name must be a positive integer"
done

# ---------------------------------------------------------------------------
# Arguments
REPOSITORY= DIGEST= IMAGE_ID= VERSION= REVISION= SOURCE_REPO= SIGNER_WORKFLOW=
ANONYMOUS=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --anonymous) ANONYMOUS=1; shift; continue ;;
    --repository|--digest|--image-id|--version|--revision|--source-repo|--signer-workflow)
      [ "$#" -ge 2 ] || usage "$1 requires a value" ;;
    *) usage "unknown argument" ;;
  esac
  case "$1" in
    --repository) REPOSITORY=$2 ;;
    --digest) DIGEST=$2 ;;
    --image-id) IMAGE_ID=$2 ;;
    --version) VERSION=$2 ;;
    --revision) REVISION=$2 ;;
    --source-repo) SOURCE_REPO=$2 ;;
    --signer-workflow) SIGNER_WORKFLOW=$2 ;;
  esac
  shift 2
done

# A fully qualified repository: registry host (optional port), then a lowercase
# path. A tag (`:` in the path) or digest (`@`) suffix is rejected; the
# reference is always built as <repository>@<digest>.
matches "$REPOSITORY" '[a-z0-9]([a-z0-9.-]*[a-z0-9])?(:[0-9]+)?(/[a-z0-9]+([._-]+[a-z0-9]+)*)+' \
  || usage '--repository must be registry/path with no tag or digest'
case "${REPOSITORY%%/*}" in
  *.*|*:*|localhost) ;;
  *) usage '--repository must include a registry host' ;;
esac
matches "$DIGEST" 'sha256:[0-9a-f]{64}' || usage '--digest must be sha256:<64 hex>'
matches "$IMAGE_ID" 'sha256:[0-9a-f]{64}' || usage '--image-id must be sha256:<64 hex>'
matches "$VERSION" '[0-9A-Za-z][0-9A-Za-z._+-]{0,127}' || usage '--version is empty or invalid'
matches "$REVISION" '[0-9a-f]{40}' || usage '--revision must be a 40-hex commit'
matches "$SOURCE_REPO" '[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+' || usage '--source-repo must be owner/name'
matches "$SIGNER_WORKFLOW" '([A-Za-z0-9.-]+/)?[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+' \
  || usage '--signer-workflow must be [host/]owner/repo/path/to/workflow'

REF=$REPOSITORY@$DIGEST
EXPECTED_SOURCE=https://github.com/$SOURCE_REPO

# ---------------------------------------------------------------------------
# Tooling: the runner's gh must support every attestation constraint used.
for tool in docker gh jq timeout; do
  command -v "$tool" >/dev/null 2>&1 || tooling "$tool is not on PATH"
done
gh_help=$(gh attestation verify --help 2>&1) \
  || tooling 'gh has no `attestation verify` command; install a current gh'
for flag in --predicate-type --signer-workflow --source-digest --repo; do
  case "$gh_help" in
    *"$flag"*) ;;
    *) tooling "gh attestation verify lacks $flag; install a current gh" ;;
  esac
done

# ---------------------------------------------------------------------------
# Temp state and cleanup
WORK=$(mktemp -d) || tooling 'cannot create temp directory'
SUFFIX=$$
CONTAINER=quorum-release-verify-$SUFFIX
VOLUME=quorum-release-verify-data-$SUFFIX
SERVICE_STARTED=0

cleanup() {
  c_status=$?
  trap - EXIT
  if [ "$SERVICE_STARTED" = 1 ]; then
    bounded 60 docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    bounded 60 docker volume rm -f "$VOLUME" >/dev/null 2>&1 || true
  fi
  rm -rf "$WORK"
  exit "$c_status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [ "$ANONYMOUS" = 1 ]; then
  mkdir "$WORK/docker-config" || tooling 'cannot create clean DOCKER_CONFIG'
  DOCKER_CONFIG=$WORK/docker-config
  export DOCKER_CONFIG
  unset DOCKER_AUTH_CONFIG REGISTRY_AUTH_FILE
  log "anonymous mode: clean DOCKER_CONFIG, registry credentials unset"
fi

# Usage: registry_fail <what>. Anonymous failures name the required fix.
registry_fail() {
  if [ "$ANONYMOUS" = 1 ]; then
    fail "$1 without credentials failed for $REF; the GHCR package must be made public before release verification can pass"
  fi
  fail "$1 failed for $REF"
}

# ---------------------------------------------------------------------------
# 1. Registry manifest: one image manifest whose config is the tested ID.
bounded "$STEP_SECS" docker buildx imagetools inspect --raw "$REF" \
  >"$WORK/manifest.json" 2>"$WORK/manifest.err" || {
  sed -n '1,20p' "$WORK/manifest.err" >&2
  registry_fail 'manifest read'
}
manifest_fields=$(jq -r '[(.mediaType // ""), (has("manifests") | tostring),
  (.config.digest // "")] | @tsv' "$WORK/manifest.json" 2>/dev/null) \
  || fail "manifest for $REF is not valid JSON"
media_type=$(printf '%s\n' "$manifest_fields" | cut -f1)
has_manifests=$(printf '%s\n' "$manifest_fields" | cut -f2)
config_digest=$(printf '%s\n' "$manifest_fields" | cut -f3)
[ "$has_manifests" = false ] \
  || fail "$REF is an index/manifest list; expected one linux/amd64 image manifest"
case "$media_type" in
  application/vnd.oci.image.manifest.v1+json \
    | application/vnd.docker.distribution.manifest.v2+json) ;;
  *) fail "$REF has manifest media type '$media_type'; expected one image manifest" ;;
esac
[ "$config_digest" = "$IMAGE_ID" ] \
  || fail "manifest config digest $config_digest != tested image ID $IMAGE_ID"
log "manifest: single image, config $IMAGE_ID"

# ---------------------------------------------------------------------------
# 2. Pull by digest and assert the local image identity and labels.
bounded "$STEP_SECS" docker pull --platform "$PLATFORM" "$REF" \
  >"$WORK/pull.log" 2>&1 || {
  sed -n '1,20p' "$WORK/pull.log" >&2
  registry_fail 'pull by digest'
}
log "pulled $REF"

inspect() {
  bounded "$STEP_SECS" docker image inspect -f "$1" "$REF" 2>/dev/null
}

repo_digests=$(inspect '{{range .RepoDigests}}{{println .}}{{end}}') \
  || fail "cannot inspect pulled $REF"
printf '%s\n' "$repo_digests" | grep -Fqx -- "$REF" \
  || fail "pulled image does not carry repo digest $REF"

# The classic image store reports the config digest as the image ID; the
# containerd store reports the manifest digest. Both are content-bound to the
# manifest whose config digest step 1 already pinned to --image-id.
local_id=$(inspect '{{.Id}}') || fail "cannot inspect pulled $REF"
[ "$local_id" = "$IMAGE_ID" ] || [ "$local_id" = "$DIGEST" ] \
  || fail "pulled image ID $local_id != tested image ID $IMAGE_ID"

platform=$(inspect '{{.Os}}/{{.Architecture}}') || fail "cannot inspect pulled $REF"
[ "$platform" = "$PLATFORM" ] || fail "pulled image is $platform; expected $PLATFORM"

# Usage: check_label <key> <expected>
check_label() {
  cl_actual=$(inspect "{{index .Config.Labels \"org.opencontainers.image.$1\"}}") \
    || fail "cannot inspect pulled $REF"
  [ "$cl_actual" = "$2" ] \
    || fail "label org.opencontainers.image.$1 is '$cl_actual'; expected '$2'"
}
check_label source "$EXPECTED_SOURCE"
check_label revision "$REVISION"
check_label version "$VERSION"
check_label licenses "$EXPECTED_LICENSES"
check_label description "$EXPECTED_DESCRIPTION"
log "identity: $IMAGE_ID $PLATFORM, labels match $SOURCE_REPO@$REVISION $VERSION"

# ---------------------------------------------------------------------------
# 3. Attestations, verified by the runner's external gh.
# Usage: attest <predicate-type>
attest() {
  bounded "$STEP_SECS" gh attestation verify "oci://$REF" \
    --repo "$SOURCE_REPO" \
    --signer-workflow "$SIGNER_WORKFLOW" \
    --source-digest "$REVISION" \
    --predicate-type "$1" \
    --format json >"$WORK/attestation.json" 2>"$WORK/attestation.err"
}

attest "$PROVENANCE_PREDICATE" || {
  sed -n '1,20p' "$WORK/attestation.err" >&2
  fail "provenance attestation ($PROVENANCE_PREDICATE) missing or unverified for $REF"
}
log "provenance attestation verified ($PROVENANCE_PREDICATE)"

sbom_verified=
for predicate in $SBOM_PREDICATES; do
  if attest "$predicate"; then
    sbom_verified=$predicate
    break
  fi
done
[ -n "$sbom_verified" ] || {
  sed -n '1,20p' "$WORK/attestation.err" >&2
  fail "SBOM attestation (SPDX or CycloneDX) missing or unverified for $REF"
}
log "SBOM attestation verified ($sbom_verified)"

# ---------------------------------------------------------------------------
# 4. Bounded service check on the pulled digest.
bounded "$SMOKE_SECS" "$HERE/smoke.sh" "$REF" \
  || fail "smoke check failed or exceeded ${SMOKE_SECS}s"

SERVICE_STARTED=1
bounded "$STEP_SECS" docker volume create "$VOLUME" >/dev/null \
  || fail 'cannot create service-check volume'
bounded "$STEP_SECS" docker run --rm --pull never --platform "$PLATFORM" \
  --user 0 -v "$VOLUME:/data" "$REF" sh -ec '
  mkdir -p /data/repos/project /data/worktrees
  git -C /data/repos/project init -q
  git -C /data/repos/project -c user.email=verify@example.invalid \
    -c user.name=verify commit -q --allow-empty -m init
  chown -R 10001:10001 /data
' >/dev/null || fail 'cannot prepare service-check volume'
bounded "$STEP_SECS" docker run -d --pull never --platform "$PLATFORM" \
  --name "$CONTAINER" -e QUORUM_REPO=release-verify/project \
  -v "$VOLUME:/data" "$REF" >/dev/null || fail 'cannot start service-check container'

ready_start=$(date +%s)
while :; do
  running=$(bounded "$STEP_SECS" docker inspect -f '{{.State.Running}}' \
    "$CONTAINER" 2>/dev/null) || running=false
  if [ "$running" != true ]; then
    bounded "$STEP_SECS" docker logs "$CONTAINER" 2>&1 | sed -n '1,40p' >&2
    fail 'service-check container stopped before readiness'
  fi
  probe=$(bounded "$PROBE_SECS" docker exec "$CONTAINER" \
    timeout -k 1 "$PROBE_SECS" /usr/local/bin/quorum-health readiness 2>/dev/null) \
    && [ "$probe" = 'health: ready' ] && break
  if [ $(($(date +%s) - ready_start)) -ge "$READY_SECS" ]; then
    fail "service-check container not ready within ${READY_SECS}s (last: ${probe:-no output})"
  fi
  sleep 2
done
log "service check: quorum-health readiness ready after $(($(date +%s) - ready_start))s (bound ${READY_SECS}s)"

log "PASS $REF"
exit 0
