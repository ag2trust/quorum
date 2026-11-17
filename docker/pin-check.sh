#!/bin/sh
# Fail closed on mutable container build inputs.
#
# Usage: docker/pin-check.sh [--dockerfile PATH] [WORKFLOW_FILE...]
#
# Dockerfile mode (always; default PATH is the repository Dockerfile):
#   - a `# syntax=` frontend directive must be pinned by @sha256 digest;
#   - every FROM must be pinned by @sha256 digest unless it names `scratch`
#     or an earlier build stage; variable image references are refused;
#   - apt-get/apt install of a package in PINNED_APT_PACKAGES must carry
#     `=version`, and a `${VAR}` version needs a nonempty `ARG VAR=` default;
#   - CODEX_VERSION must default to an exact x.y.z release and CODEX_SHA256
#     to a 64-hex, non-placeholder digest.
# Workflow mode (each extra argument): every `uses:` must be a local `./`
# action, a `docker://` image pinned by @sha256, or `owner/repo[/path]@<sha>`
# with a full 40-hex commit SHA.
#
# Exit codes: 0 pinned · 1 mutable input found · 2 usage/unreadable input.
set -eu

PINNED_APT_PACKAGES="ca-certificates git gh tini"

usage() {
  printf 'usage: %s [--dockerfile PATH] [WORKFLOW_FILE...]\n' "$0" >&2
  exit 2
}

dockerfile="$(dirname "$0")/../Dockerfile"
if [ "${1:-}" = --dockerfile ]; then
  [ "$#" -ge 2 ] || usage
  dockerfile="$2"
  shift 2
fi
case "${1:-}" in
  -*) usage ;;
esac

for input in "$dockerfile" "$@"; do
  if [ ! -f "$input" ] || [ ! -r "$input" ]; then
    printf 'pin-check: cannot read %s\n' "$input" >&2
    exit 2
  fi
done

status=0

awk -v pinned="$PINNED_APT_PACKAGES" '
function fail(line, msg) {
  printf "pin-check: %s:%d: %s\n", FILENAME, line, msg > "/dev/stderr"
  bad = 1
}
function unquote(v) {
  gsub(/^["\047]+|["\047]+$/, "", v)
  return v
}
function is_hex(v, n) {
  return length(v) == n && v ~ /^[0-9a-f]+$/
}
function digest_pinned(ref,    at) {
  at = index(ref, "@sha256:")
  return at > 0 && is_hex(substr(ref, at + 8), 64)
}
function check_from(line, text,    n, t, i, image) {
  n = split(text, t, /[ \t]+/)
  i = 2
  while (i <= n && t[i] ~ /^--/) i++
  image = t[i]
  if (image == "") { fail(line, "FROM without an image"); return }
  # Check against stages declared on earlier lines before registering the
  # `AS name` on this line, so `FROM debian AS debian` cannot vouch for itself.
  if (image ~ /\$/) fail(line, "FROM uses a variable image reference: " image)
  else if (tolower(image) == "scratch" || (tolower(image) in stage && image !~ /[:@\/]/)) {}
  else if (!digest_pinned(image)) fail(line, "FROM is not pinned by @sha256 digest: " image)
  if (i + 2 <= n && toupper(t[i + 1]) == "AS") stage[tolower(t[i + 2])] = 1
}
function check_arg(text,    n, t, i, eq) {
  n = split(text, t, /[ \t]+/)
  for (i = 2; i <= n; i++) {
    eq = index(t[i], "=")
    if (eq > 0) argdef[substr(t[i], 1, eq - 1)] = unquote(substr(t[i], eq + 1))
    else if (!(t[i] in argdef)) argdef[t[i]] = ""
  }
}
function check_run(line, text,    n, t, i, j, installing, name, value, eq, var) {
  n = split(text, t, /[ \t]+/)
  installing = 0
  for (i = 1; i <= n; i++) {
    if (t[i] == "&&" || t[i] == "||" || t[i] == ";" || t[i] == "|" || t[i] == "\\") {
      installing = 0; continue
    }
    if (t[i] == "apt-get" || t[i] == "apt") {
      # Options may precede the subcommand (`apt-get -y install`); -o, -c
      # and -t take a separate argument.
      j = i + 1
      while (j <= n && t[j] ~ /^-/) {
        if (t[j] ~ /^-[oct]$/) j++
        j++
      }
      if (t[j] == "install") { installing = 1; i = j }
      continue
    }
    if (!installing || t[i] ~ /^-/) continue
    eq = index(t[i], "=")
    name = unquote(eq > 0 ? substr(t[i], 1, eq - 1) : t[i])
    sub(/;$/, "", name)
    if (!(name in mustpin)) continue
    if (eq == 0) { fail(line, "apt package " name " is not version-pinned"); continue }
    value = unquote(substr(t[i], eq + 1))
    sub(/;$/, "", value)
    if (value == "") { fail(line, "apt package " name " has an empty version pin"); continue }
    if (value ~ /^\$/) {
      var = value
      gsub(/[${}]/, "", var)
      if (argdef[var] == "") fail(line, "apt package " name " pin " value " has no nonempty ARG default")
    }
  }
}
function instruction(line, text,    kw) {
  sub(/^[ \t]+/, "", text)
  kw = toupper(text)
  sub(/[ \t].*/, "", kw)
  if (kw == "FROM") check_from(line, text)
  else if (kw == "ARG") check_arg(text)
  else if (kw == "RUN") check_run(line, text)
}
BEGIN {
  n = split(pinned, p, " ")
  for (i = 1; i <= n; i++) mustpin[p[i]] = 1
  directives = 1
}
{
  raw = $0
  if (directives && raw ~ /^#[ \t]*[a-zA-Z]+[ \t]*=/) {
    if (tolower(raw) ~ /^#[ \t]*syntax[ \t]*=/) {
      ref = raw
      sub(/^#[ \t]*[a-zA-Z]+[ \t]*=[ \t]*/, "", ref)
      sub(/[ \t]+$/, "", ref)
      if (!digest_pinned(ref)) fail(NR, "syntax frontend is not pinned by @sha256 digest: " ref)
    }
    next
  }
  directives = 0
  if (raw ~ /^[ \t]*#/ || (pending == "" && raw ~ /^[ \t]*$/)) next
  if (pending == "") start = NR
  if (raw ~ /\\[ \t]*$/) {
    sub(/\\[ \t]*$/, "", raw)
    pending = pending " " raw
    next
  }
  instruction(start, pending " " raw)
  pending = ""
}
END {
  if (pending != "") instruction(start, pending)
  if (!("CODEX_VERSION" in argdef) || argdef["CODEX_VERSION"] !~ /^[0-9]+\.[0-9]+\.[0-9]+$/)
    fail(NR, "CODEX_VERSION must default to an exact x.y.z release")
  sha = argdef["CODEX_SHA256"]
  if (!is_hex(sha, 64)) fail(NR, "CODEX_SHA256 must default to a 64-hex digest")
  else if (sha ~ /^0+$/) fail(NR, "CODEX_SHA256 is an all-zero placeholder")
  exit bad
}
' "$dockerfile" || status=1

if [ "$#" -gt 0 ]; then
  awk '
function fail(msg) {
  printf "pin-check: %s:%d: %s\n", FILENAME, FNR, msg > "/dev/stderr"
  bad = 1
}
/^[ \t]*(-[ \t]+)?["\047]?uses["\047]?[ \t]*:/ {
  ref = $0
  sub(/^[ \t]*(-[ \t]+)?["\047]?uses["\047]?[ \t]*:[ \t]*/, "", ref)
  sub(/[ \t]+#.*$/, "", ref)
  sub(/[ \t]+$/, "", ref)
  gsub(/^["\047]|["\047]$/, "", ref)
  if (ref ~ /^\.\//) next
  if (ref ~ /^docker:\/\//) {
    at = index(ref, "@sha256:")
    digest = at > 0 ? substr(ref, at + 8) : ""
    if (!(length(digest) == 64 && digest ~ /^[0-9a-f]+$/))
      fail("docker action is not pinned by @sha256 digest: " ref)
    next
  }
  at = index(ref, "@")
  sha = at > 0 ? substr(ref, at + 1) : ""
  if (at <= 1 || !(length(sha) == 40 && sha ~ /^[0-9a-f]+$/))
    fail("action is not pinned by a full 40-hex commit SHA: " ref)
}
END { exit bad }
' "$@" || status=1
fi

if [ "$status" -eq 0 ]; then
  printf 'pin-check: build inputs pinned\n'
fi
exit "$status"
