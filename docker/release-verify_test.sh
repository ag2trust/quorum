#!/bin/sh
# Host-side contract tests for docker/release-verify.sh. Stub `docker` and
# `gh` shims on PATH stand in for the registry, daemon, and attestation API;
# no Docker or network access is required. Requires jq and GNU timeout.
set -u

HERE=$(cd "$(dirname "$0")" && pwd)
SCRIPT=$HERE/release-verify.sh
FAILS=0

REPOSITORY=ghcr.io/ag2trust/quorum
DIGEST=sha256:1111111111111111111111111111111111111111111111111111111111111111
IMAGE_ID=sha256:2222222222222222222222222222222222222222222222222222222222222222
OTHER=sha256:3333333333333333333333333333333333333333333333333333333333333333
REVISION=0123456789abcdef0123456789abcdef01234567
VERSION=1.2.3
SIGNER=ag2trust/quorum/.github/workflows/container-promote.yml
REF=$REPOSITORY@$DIGEST
PROVENANCE=https://slsa.dev/provenance/v1
SPDX=https://spdx.dev/Document/v2.3
CYCLONEDX=https://cyclonedx.org/bom

T=$(mktemp -d)
trap 'rm -rf "$T"' EXIT
mkdir -p "$T/bin"

cat >"$T/bin/docker" <<'EOF'
#!/bin/sh
# Private-package simulation: registry access needs a config.json credential.
registry_denied() {
  [ "${STUB_PRIVATE:-0}" = 1 ] && [ ! -f "${DOCKER_CONFIG:-$HOME/.docker}/config.json" ]
}
printf '%s\n' "$*" >>"$STUB_LOG"
case "$1 ${2:-}" in
  "buildx imagetools")
    if registry_denied; then echo 'unauthorized' >&2; exit 1; fi
    cat "$STUB_MANIFEST"
    ;;
  "pull "*)
    printf 'pull-env config=%s entries=%s auth=%s\n' "${DOCKER_CONFIG:-unset}" \
      "$(ls -A "${DOCKER_CONFIG:-/nonexistent}" 2>/dev/null | wc -l | tr -d ' ')" \
      "${DOCKER_AUTH_CONFIG-unset}" >>"$STUB_LOG"
    if registry_denied; then echo 'denied' >&2; exit 1; fi
    exit "${STUB_PULL_RC:-0}"
    ;;
  "image inspect")
    case "$4" in
      *RepoDigests*) printf '%s\n' "$STUB_REPO_DIGEST" ;;
      '{{.Id}}') printf '%s\n' "$STUB_ID" ;;
      *Architecture*) printf '%s\n' "$STUB_PLATFORM" ;;
      *image.source*) printf '%s\n' "$STUB_SOURCE" ;;
      *image.revision*) printf '%s\n' "$STUB_REVISION" ;;
      *image.version*) printf '%s\n' "$STUB_VERSION" ;;
      *image.licenses*) printf '%s\n' "$STUB_LICENSES" ;;
      *image.description*) printf '%s\n' "$STUB_DESCRIPTION" ;;
      *) exit 1 ;;
    esac
    ;;
  "run "*)
    case "$*" in
      *' -d '*) echo container-id ;;
      *'id -u'*) printf '%s\n' "${STUB_IDENTITY:-10001:10001}" ;;
    esac
    ;;
  "inspect "*) printf '%s\n' "${STUB_RUNNING:-true}" ;;
  "exec "*)
    printf '%s\n' "${STUB_READY:-health: ready}"
    [ "${STUB_READY:-health: ready}" = 'health: ready' ]
    ;;
  "volume "*|"rm "*|"logs "*) ;;
  *) echo "docker stub: unexpected $*" >&2; exit 99 ;;
esac
EOF

cat >"$T/bin/gh" <<'EOF'
#!/bin/sh
if [ "${STUB_GH_OLD:-0}" = 1 ]; then echo 'unknown command "attestation"' >&2; exit 1; fi
[ "$1 $2" = "attestation verify" ] || { echo "gh stub: unexpected $*" >&2; exit 99; }
if [ "$3" = --help ]; then
  echo '--predicate-type --signer-workflow --source-digest -R, --repo'
  exit 0
fi
printf 'gh %s\n' "$*" >>"$STUB_LOG"
predicate=
while [ "$#" -gt 0 ]; do
  [ "$1" = --predicate-type ] && predicate=$2
  shift
done
case " $STUB_ATTESTED " in *" $predicate "*) echo '[{}]' ;; *) echo 'no attestations' >&2; exit 1 ;; esac
EOF
chmod +x "$T/bin/docker" "$T/bin/gh"

reset_stubs() {
  rm -rf "$T/tmp" "$T/log" "$T/manifest.json"
  mkdir -p "$T/tmp"
  : >"$T/log"
  STUB_LOG=$T/log
  STUB_MANIFEST=$T/manifest.json
  printf '{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"%s","size":1},"layers":[]}' \
    "$IMAGE_ID" >"$STUB_MANIFEST"
  STUB_REPO_DIGEST=$REF
  STUB_ID=$IMAGE_ID
  STUB_PLATFORM=linux/amd64
  STUB_SOURCE=https://github.com/ag2trust/quorum
  STUB_REVISION=$REVISION
  STUB_VERSION=$VERSION
  STUB_LICENSES='MIT AND Apache-2.0'
  STUB_DESCRIPTION='Self-hostable Quorum runtime'
  STUB_ATTESTED="$PROVENANCE $SPDX"
  STUB_PULL_RC=0
  STUB_RUNNING=true
  STUB_READY='health: ready'
  STUB_IDENTITY=10001:10001
  STUB_GH_OLD=0
  STUB_PRIVATE=0
  # Exported once here; tests then override values with plain assignments.
  export STUB_LOG STUB_MANIFEST STUB_REPO_DIGEST STUB_ID STUB_PLATFORM \
    STUB_SOURCE STUB_REVISION STUB_VERSION STUB_LICENSES STUB_DESCRIPTION \
    STUB_ATTESTED STUB_PULL_RC STUB_RUNNING STUB_READY STUB_IDENTITY \
    STUB_GH_OLD STUB_PRIVATE
  unset ARG_REPOSITORY ARG_DIGEST ARG_READY_SECS ARG_DOCKER_CONFIG ARG_AUTH
}

# Usage: run_verify [extra args...] — default args target the all-good stub.
# Per-call inputs come from ARG_* variables (reset by reset_stubs), never
# from `VAR=value function` prefixes, whose persistence POSIX leaves open.
run_verify() {
  RC=0
  OUT=$(
    if [ -n "${ARG_DOCKER_CONFIG:-}" ]; then DOCKER_CONFIG=$ARG_DOCKER_CONFIG; export DOCKER_CONFIG; fi
    if [ -n "${ARG_AUTH:-}" ]; then DOCKER_AUTH_CONFIG=$ARG_AUTH; export DOCKER_AUTH_CONFIG; fi
    TMPDIR=$T/tmp PATH=$T/bin:$PATH RELEASE_VERIFY_READY_SECS=${ARG_READY_SECS:-20} \
    "$SCRIPT" --repository "${ARG_REPOSITORY:-$REPOSITORY}" \
    --digest "${ARG_DIGEST:-$DIGEST}" --image-id "$IMAGE_ID" \
    --version "$VERSION" --revision "$REVISION" --source-repo ag2trust/quorum \
    --signer-workflow "$SIGNER" "$@" 2>&1
  ) || RC=$?
}

ok() { printf 'ok   %s\n' "$1"; }
bad() { printf 'FAIL %s\n%s\n' "$1" "$OUT" >&2; FAILS=$((FAILS + 1)); }

# Usage: expect <rc> <label> [grep-pattern-in-output]
expect() {
  if [ "$RC" != "$1" ]; then
    bad "$2: expected exit $1, got $RC"
  elif [ -n "${3:-}" ] && ! printf '%s\n' "$OUT" | grep -Fq -- "$3"; then
    bad "$2: output lacks '$3'"
  else
    ok "$2"
  fi
}

# Usage: log_has <label> <fixed-string>
log_has() {
  grep -Fq -- "$2" "$T/log" && ok "$1" || { OUT=$(cat "$T/log"); bad "$1: log lacks '$2'"; }
}
log_lacks() {
  grep -Fq -- "$2" "$T/log" && { OUT=$(cat "$T/log"); bad "$1: log has '$2'"; } || ok "$1"
}
temp_clean() {
  [ -z "$(ls -A "$T/tmp")" ] && ok "$1" || { OUT=$(ls -A "$T/tmp"); bad "$1: temp state left"; }
}
service_cleaned() {
  log_has "$1: container removed" "rm -f quorum-release-verify-"
  log_has "$1: volume removed" "volume rm -f quorum-release-verify-data-"
  temp_clean "$1: temp removed"
}

# --- all good ---------------------------------------------------------------
reset_stubs
run_verify
expect 0 'all-good stub path passes' "PASS $REF"
log_has 'pulls by exact digest' "pull --platform linux/amd64 $REF"
log_lacks 'never pulls a tag' "pull --platform linux/amd64 $REPOSITORY:"
log_has 'provenance verified with repo/signer/source constraints' \
  "gh attestation verify oci://$REF --repo ag2trust/quorum --signer-workflow $SIGNER --source-digest $REVISION --predicate-type $PROVENANCE"
log_has 'SBOM verified by predicate type' "--predicate-type $SPDX"
log_has 'service check starts pulled digest' "--name quorum-release-verify-"
service_cleaned 'success'
log_has 'authenticated mode keeps caller DOCKER_CONFIG' "pull-env config=${DOCKER_CONFIG:-unset} "

# --- usage negatives ----------------------------------------------------------
reset_stubs; ARG_REPOSITORY=$REPOSITORY:v1; run_verify
expect 2 'tag in --repository rejected'
reset_stubs; ARG_REPOSITORY=$REF; run_verify
expect 2 'digest suffix in --repository rejected'
reset_stubs; ARG_REPOSITORY=ag2trust/quorum; run_verify
expect 2 'unqualified repository rejected'
reset_stubs; ARG_DIGEST=latest; run_verify
expect 2 'tag as --digest rejected'
reset_stubs; ARG_DIGEST=sha256:abc; run_verify
expect 2 'short digest rejected'
log_lacks 'no registry access on usage errors' 'pull'
RC=0
OUT=$(PATH=$T/bin:$PATH "$SCRIPT" --repository "$REPOSITORY" --digest "$DIGEST" 2>&1) || RC=$?
expect 2 'missing required flags rejected'

# --- identity negatives -------------------------------------------------------
reset_stubs; STUB_REPO_DIGEST=$REPOSITORY@$OTHER; run_verify
expect 1 'digest mismatch fails' 'does not carry repo digest'
temp_clean 'digest mismatch: temp removed'

reset_stubs
printf '{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"digest":"%s"}}' \
  "$OTHER" >"$STUB_MANIFEST"
run_verify
expect 1 'manifest config digest mismatch fails' 'manifest config digest'

reset_stubs; STUB_ID=$OTHER; run_verify
expect 1 'pulled image ID mismatch fails' 'pulled image ID'

reset_stubs; STUB_ID=$DIGEST; run_verify
expect 0 'containerd-store ID (manifest digest) accepted'

for label in SOURCE REVISION VERSION LICENSES DESCRIPTION; do
  reset_stubs
  eval "STUB_$label=wrong-value"
  run_verify
  expect 1 "$label label mismatch fails" 'label org.opencontainers.image.'
done

reset_stubs
cat >"$STUB_MANIFEST" <<EOF
{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[
 {"digest":"$IMAGE_ID","platform":{"os":"linux","architecture":"amd64"}},
 {"digest":"$OTHER","platform":{"os":"linux","architecture":"arm64"}}]}
EOF
run_verify
expect 1 'multi-platform index fails' 'is an index'
log_lacks 'index rejected before pull' 'pull'

reset_stubs
printf '{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.list.v2+json","manifests":[]}' >"$STUB_MANIFEST"
run_verify
expect 1 'docker manifest list fails' 'is an index'

reset_stubs; STUB_PLATFORM=linux/arm64; run_verify
expect 1 'non-amd64 image fails' 'expected linux/amd64'

reset_stubs; STUB_PULL_RC=1; run_verify
expect 1 'pull failure fails' 'pull by digest failed'

# --- attestation negatives ----------------------------------------------------
reset_stubs; STUB_ATTESTED=$SPDX; run_verify
expect 1 'missing provenance attestation fails' 'provenance attestation'
log_lacks 'no service check after attestation failure' 'volume create'

reset_stubs; STUB_ATTESTED=$PROVENANCE; run_verify
expect 1 'missing SBOM attestation fails' 'SBOM attestation'

reset_stubs; STUB_ATTESTED="$PROVENANCE $CYCLONEDX"; run_verify
expect 0 'CycloneDX SBOM satisfies SBOM requirement'

reset_stubs; STUB_GH_OLD=1; run_verify
expect 3 'gh without attestation support is a tooling failure' 'current gh'

# --- bounded service check ----------------------------------------------------
reset_stubs; STUB_IDENTITY=0:0; run_verify
expect 1 'smoke failure fails' 'smoke check failed'

reset_stubs; STUB_READY='health: not-ready check=authority'
start=$(date +%s)
ARG_READY_SECS=3; run_verify
elapsed=$(($(date +%s) - start))
expect 1 'never-ready container fails at deadline' 'not ready within 3s'
[ "$elapsed" -lt 15 ] && ok "readiness deadline bounded (${elapsed}s)" \
  || bad "readiness deadline took ${elapsed}s"
service_cleaned 'not-ready'

reset_stubs; STUB_RUNNING=false; run_verify
expect 1 'stopped container fails' 'stopped before readiness'
service_cleaned 'stopped'

# --- anonymous mode -----------------------------------------------------------
mkdir -p "$T/creds"
printf '{"auths":{"ghcr.io":{"auth":"c3R1Yjpub3QtYS1zZWNyZXQ="}}}' >"$T/creds/config.json"

reset_stubs; ARG_DOCKER_CONFIG=$T/creds; ARG_AUTH='{"auths":{}}'
run_verify --anonymous
expect 0 'anonymous mode passes for a public package'
log_has 'anonymous pull uses an empty DOCKER_CONFIG' 'entries=0 auth=unset'
log_lacks 'anonymous pull never uses caller credentials' "config=$T/creds"
temp_clean 'anonymous: clean DOCKER_CONFIG removed'

reset_stubs; STUB_PRIVATE=1
ARG_DOCKER_CONFIG=$T/creds; run_verify
expect 0 'private package passes only with credentials'
ARG_AUTH='{"auths":{}}'; run_verify --anonymous
expect 1 'anonymous mode rejects a private package despite caller credentials' \
  'GHCR package must be made public'

reset_stubs; STUB_PULL_RC=1; run_verify --anonymous
expect 1 'anonymous pull failure fails' 'GHCR package must be made public'

# --- static labels track the Dockerfile ---------------------------------------
for value in 'org.opencontainers.image.licenses="MIT AND Apache-2.0"' \
  'org.opencontainers.image.description="Self-hostable Quorum runtime"' \
  'org.opencontainers.image.source="https://github.com/ag2trust/quorum"'; do
  if grep -Fq -- "$value" "$HERE/../Dockerfile"; then
    ok "Dockerfile label matches: $value"
  else
    OUT=; bad "Dockerfile label drifted from release-verify.sh: $value"
  fi
done

if [ "$FAILS" -ne 0 ]; then
  printf 'release-verify_test: %s failure(s)\n' "$FAILS" >&2
  exit 1
fi
printf 'release-verify_test: all passed\n'
