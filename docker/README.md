# Container image

The root `Dockerfile` builds the generic public `linux/amd64` Quorum service
image. Pinned `tini` is PID 1. The default command initializes durable state,
then supervises one `quorum serve` authority and one loopback-only `quorum web`
process without respawning either child.

## Build and verify

Build from the repository root so `.dockerignore` bounds the context:

```sh
docker buildx build --load --platform linux/amd64 --tag quorum:local .
./docker/verify.sh quorum:local
```

Verification checks the image architecture, pinned tools and Codex checksum,
host-side supervisor behavior, fresh Codex routing, and real-container service,
shutdown, bind-collision, and daemon-lock behavior. It requires Docker buildx
and `sqlite3` on the host.

The build pins the Debian base digest, Rust builder, direct `git`, `gh`, and
`tini` packages, and the Codex release and archive checksum. A wrong
`CODEX_SHA256` fails before extraction. The provider and base packages are
upgraded by rebuilding the image.

## Prepare and run

Use one writable volume at `/data`. Before the first run, place the managed Git
checkout at `/data/repos/project`; the service deliberately does not clone a
repository. Its `.git` entry may be a directory or linked-worktree file, but it
must identify a valid checkout. The mounted directory must be writable by
numeric UID/GID `10001:10001`.

```sh
docker run --rm \
  -e QUORUM_REPO=owner/name \
  -v /host/quorum-data:/data \
  quorum:local
```

`QUORUM_REPO` must be a nonempty `owner/name`. On every start, the entrypoint
prepares `/data/worktrees` and `/data/quorum/logs`, then runs idempotent
`quorum init` from `/data/quorum/init`, outside the managed checkout. Persistent
state is namespaced below `/data/quorum` through `QUORUM_HOME`.

For a fresh repository identity, the image atomically installs a Codex-only
routing file at `/data/quorum/serve/<owner>__<name>.toml`; every managed role
selects the bundled Codex CLI. An existing file at that path is never
overwritten. Set `QUORUM_SERVE_CONFIG` to use another persistent file.

Readiness requires all three facts at once: both supervised children are live,
`quorum status --json` reports live daemon authority, and Web returns a real
HTTP response on `127.0.0.1`. Web always binds loopback; publishing the port
does not make it remotely reachable. Remote access requires a separately
secured proxy sharing the container's network namespace.

The daemon identity lock rejects another container serving the same repository
database, including containers whose isolated PID namespaces give both daemon
processes the same numeric PID. Stop the active container before replacement.

## Lifecycle and overrides

An external SIGTERM reaches the full process group through `tini -g`. The
supervisor also records direct and descendant PIDs, sends TERM, waits a bounded
interval, escalates remaining processes to KILL, and reaps its direct children.
It never internally respawns them.

The container propagates `quorum serve`'s exact exit status, including 0, 3,
and 75. A Web-first failure stops serve and exits 1. Initialization failures
also propagate exactly. Set `QUORUM_SELF_UPDATE_DRAIN=1` to let serve request
external rebuild/relaunch with exit 75; it is disabled by default.

Docker command replacement is explicit and bypasses the default supervisor
command while retaining `tini`, for example:

```sh
docker run --rm quorum:local quorum --help
```

Runtime path and timing overrides are `QUORUM_REPO_DIR`,
`QUORUM_WORKTREE_BASE`, `QUORUM_LOG_DIR`, `QUORUM_WEB_PORT`,
`QUORUM_READY_TRIES`, and `QUORUM_SHUTDOWN_TRIES`. The image contains no
credentials, tenancy, billing, gateway, or provisioning behavior; inject
provider and GitHub credentials at runtime.

## Container replacement

Exactly one container may serve a given repository identity's database at a
time; replacing the container that holds authority is the normal way to
redeploy, update environment or mount settings, or recover from a crash.
`docker/replace.sh`, `docker/replace-abrupt.sh`, and `docker/replace-negative.sh`
(run through `docker/verify.sh IMAGE`, or individually with
`docker/<script>.sh IMAGE`) are the verified suites behind every guarantee
below.

### Clean replacement

Stop and remove the running container, then start a replacement from the
same image, the same `QUORUM_REPO`, and the same `/data` host mount:

```sh
docker stop quorum-prod && docker rm quorum-prod
docker run -d --name quorum-prod \
  -e QUORUM_REPO=owner/name \
  -v /host/quorum-data:/data \
  quorum:local
```

`docker/replace.sh` seeds rows through a live container, stops and removes
it, applies an operator edit to the serve config through a one-off root
container against the stopped tree, then starts a second container over
the same tree and asserts: the repository, a fixed set of representative
rows seeded by the test (a specific task, a task note, all `pinned` rows,
all `control` rows, and the messages for one topic) plus the aggregate
counts in the status projection, the exact config bytes (including the
operator edit), recorded log bytes, and the worktree root are all
unchanged — there is no full-table or full-database diff anywhere in the
suite; the replacement serves from the same image and
`QUORUM_REPO`/mount shape; exactly one live daemon authority exists after
the restart; and Web in the second container reads the replacement
daemon's own database. This proves on-disk state survival and authority
handover only — it does not prove that an in-flight provider turn resumes.

### Abrupt replacement and stale daemon-lock takeover

A container can also disappear without a clean shutdown (host crash, `kill
-9`, OOM). `docker/replace-abrupt.sh` repeatedly `SIGKILL`s a live container
under concurrent write load and starts a replacement on the same tree once
the daemon-lock staleness window has elapsed, asserting each time that:
every row committed before the kill survives; the crash-image database (and
its WAL, when the kill lands mid-transaction) passes `PRAGMA
integrity_check`; the replacement takes over the stale `daemon_lock` row
within a fixed bound (32s stale wait + startup, bounded at 75s total) without
anything else editing that row first; and every other recorded artifact,
including the exact config checksum, still matches the baseline. A
replacement may reuse the stale holder's exact numeric PID — this is
expected inside a container's isolated PID namespace and does not indicate a
second live authority.

With a final replacement live, the suite also starts a bounded sequence of
real contender containers on the same tree, each in its own isolated PID
namespace, until one contender's `quorum serve` process happens to land on
the exact same numeric PID as the live holder's (CONTENDER_TRIES per
holder; if none match, the holder itself is killed and replaced for another
round, up to HOLDER_TRIES, after which the suite fails outright rather than
passing without ever observing a collision). Every contender in the
sequence — including the one that lands on the holder's PID — fails
authority acquisition (exit 2, logging that another daemon with that PID
already holds the DB), and the original holder remains the sole live
authority throughout, confirmed by its unchanged process start time and a
refreshed heartbeat. A passing run therefore did observe genuine equal-PID
contention and is accurate evidence that it is rejected on something other
than the raw PID.

Operationally: after an abrupt container death, start the replacement with
the same image, `QUORUM_REPO`, and mount once the stale window has passed;
`docker/replace-abrupt.sh` only ever starts the replacement after waiting
out that window, so it takes over the DB's existing `daemon_lock` row
within the bound above rather than needing any manual lock cleanup. Whether
a replacement started before the window elapses is refused is not covered
by this suite — that path is exercised only by a Rust unit test in
`quorum-core/src/daemon_lock.rs`, outside this doc's scope.

### Serve-config preservation

The entrypoint installs a per-repo serve config at
`/data/quorum/serve/<owner>__<name>.toml` only when no file exists at that
path yet; an existing file — including one hand-edited by an operator after
the first start — is never regenerated, reset, or overwritten by any later
start or replacement. `docker/replace.sh` verifies this directly: it appends
an operator edit to the effective config before replacing the container.
While the replacement container is live, it asserts only that the serve
process's `--config` argument still points at the same config path. The
full config byte-for-byte, including its checksum, is compared once — after
the replacement container is stopped — against the config recorded before
replacement, and the operator edit is asserted present in that recorded
config.

### One container per repository

Starting a second container against the same `/data` tree and `QUORUM_REPO`
while the first is live fails authority acquisition — see "Exactly one
container may serve a given repository identity's database at a time"
above — even when both containers' isolated PID namespaces happen to
assign their `quorum serve` processes the same numeric PID
(`docker/replace-abrupt.sh`, case (b) above). Always stop the existing
container before starting a replacement over the same tree and identity.

A different `QUORUM_REPO` value is a separate identity with its own
on-disk namespace under the same `/data` mount: `docker/replace-negative.sh`
starts a container with a second `owner/name` — while the original
identity's container is not running — against the same host tree, and
asserts it serves from its own namespace
(`/data/quorum/repos/<owner>__<name>/quorum.db` and
`/data/quorum/serve/<owner>__<name>.toml`), while the original identity's
DB and config stay byte-identical throughout and after. This proves
on-disk namespace isolation between identities, not the safety of running
both identities' containers at the same time; the suite never starts them
concurrently, so this doc makes no claim about that case.

### Fail-closed behavior

`docker/replace-negative.sh` breaks one precondition at a time and asserts
the container exits non-zero for the documented reason, leaves no container
serving the tree, and leaves the host tree byte-for-byte unchanged — no
`daemon_lock` row is taken and no fresh state is created over existing data:

- An unwritable mount — a read-only `/data`, or a writable `/data` with a
  read-only `/data/quorum` — fails closed with `unable to open database
  file` (exit 3).
- A tree owned by a different numeric UID/GID (not a read-only mount, but
  root-owned, which trips git's dubious-ownership safety check rather than
  a POSIX read-permission denial — Docker Desktop host bind mounts do not
  enforce owner/mode bits) is refused as foreign-owned with `is not a valid
  git checkout` (exit 1).
- A missing `/data/repos/project/.git` fails closed with its own distinct
  message naming that precondition: `is not a git checkout (.git is
  missing)` (exit 1).
- A corrupted repository `HEAD` fails closed with the *same* `is not a
  valid git checkout` message as the foreign-ownership case above (exit 1):
  the entrypoint reaches both through the identical `git rev-parse
  --is-inside-work-tree` check, so that message alone does not let an
  operator tell a corrupted checkout apart from a foreign-owned one;
  distinguishing the two requires inspecting tree ownership or `HEAD`
  directly, not the container's logged message.
- A corrupt SQLite header fails closed with its own distinct message,
  `file is not a database` (exit 3).
- A database with `PRAGMA user_version` above what the image's `quorum`
  binary supports is refused with exit 3 before any lock is taken or any
  persistent PRAGMA is applied — both through the entrypoint's `quorum init`
  and through `quorum serve` invoked directly.

After each negative case, restoring the broken precondition reproduces the
accepted tree byte-for-byte; starting a real container again to confirm it
resumes as the sole authority with every recorded artifact, DB row, and the
exact config checksum intact happens once, for the original identity, at
the end of the suite.

### Rollback limitations

There is no supported downgrade path. A database already migrated to a
newer `PRAGMA user_version` than an older image's `quorum` binary supports
is refused outright (exit 3, logged as `db schema version N is newer than
this binary (M)`) rather than read or migrated backward; the refused
database is left byte-for-byte untouched. Rolling an image back after it has
run a newer binary against a `/data` tree therefore requires either keeping
the newer image, or discarding the state created under it so the older
image performs an ordinary fresh init — a direct corollary of that refusal
and of ordinary fresh-init behavior, not a separately tested guarantee —
since there is no in-place conversion back to an older schema.

### Scope of these guarantees

Every guarantee above, except the fresh-init-after-discard corollary in
"Rollback limitations" noted as such there, is backed by an assertion in
`replace.sh`, `replace-abrupt.sh`, or `replace-negative.sh`, not inferred.
These suites prove on-disk state survival, daemon-lock authority handover,
config preservation, and fail-closed behavior at the container and
filesystem level. They do not prove that an in-flight provider (agent) turn
in progress at the moment of replacement resumes — only that the durable
rows, worktree contents, and config bytes a resumed turn would depend on
survive intact.
