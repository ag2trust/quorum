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

`docker/replace.sh` seeds rows and an operator edit to the serve config
through a live container, stops and removes it, then starts a second
container over the same tree and asserts: the repository, every DB row, the
exact config bytes (including the operator edit), recorded log bytes, and
the worktree root are all unchanged; the replacement serves from the same
image and `QUORUM_REPO`/mount shape; exactly one live daemon authority
exists after the restart; and Web in the second container reads the
replacement daemon's own database. This proves on-disk state survival and
authority handover only — it does not prove that an in-flight provider turn
resumes.

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

With a final replacement live, the suite also starts a second real container
on the same tree whose `quorum serve` process is made to share the
holder's numeric PID; that contender still fails authority acquisition
(exit 2, logging that another daemon with that PID already holds the DB)
and the original holder remains the sole live authority, confirmed by its
unchanged process start time and a refreshed heartbeat. Equal-PID contention
is therefore rejected on something other than the raw PID.

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
an operator edit to the effective config before replacing the container and
asserts the edit, and the full config byte-for-byte including its checksum,
are still present after the replacement starts and after it is stopped.

### One container per repository

The daemon identity lock allows only one live authority per repository
database. Starting a second container against the same `/data` tree and
`QUORUM_REPO` while the first is live fails authority acquisition, even when
both containers' isolated PID namespaces happen to assign their `quorum
serve` processes the same numeric PID (`docker/replace-abrupt.sh`, case (b)
above). Always stop the existing container before starting a replacement
over the same tree and identity.

A different `QUORUM_REPO` value is a separate identity and is safe to run
concurrently over the same `/data` mount: `docker/replace-negative.sh`
starts a container with a second `owner/name` against the same host tree and
asserts it serves from its own namespace
(`/data/quorum/repos/<owner>__<name>/quorum.db` and
`/data/quorum/serve/<owner>__<name>.toml`), while the original identity's DB
and config stay byte-identical throughout and after.

### Fail-closed behavior

`docker/replace-negative.sh` breaks one precondition at a time and asserts
the container exits non-zero for the documented reason, leaves no container
serving the tree, and leaves the host tree byte-for-byte unchanged — no
`daemon_lock` row is taken and no fresh state is created over existing data:

- An unwritable mount — a read-only `/data`, or a writable `/data` with a
  read-only `/data/quorum` — fails closed with `unable to open database
  file` (exit 3).
- A tree owned by a different numeric UID/GID (not a read-only mount, but
  root-owned so UID/GID `10001:10001` cannot read it) is refused as
  foreign-owned with `is not a valid git checkout` (exit 1).
- A missing `/data/repos/project/.git`, a corrupted repository `HEAD`, or a
  corrupt SQLite header all fail closed (exit 1 or 3) with a message naming
  the broken precondition.
- A database with `PRAGMA user_version` above what the image's `quorum`
  binary supports is refused with exit 3 before any lock is taken or any
  persistent PRAGMA is applied — both through the entrypoint's `quorum init`
  and through `quorum serve` invoked directly.

After each negative case, restoring the broken precondition and starting
again reproduces the original accepted state exactly.

### Rollback limitations

There is no supported downgrade path. A database already migrated to a
newer `PRAGMA user_version` than an older image's `quorum` binary supports
is refused outright (exit 3, logged as `db schema version N is newer than
this binary (M)`) rather than read or migrated backward; the refused
database is left byte-for-byte untouched. Rolling an image back after it has
run a newer binary against a `/data` tree therefore requires either keeping
the newer image, or discarding the state created under it — there is no
in-place conversion back to an older schema.

### Scope of these guarantees

Every guarantee above is backed by an assertion in `replace.sh`,
`replace-abrupt.sh`, or `replace-negative.sh`, not inferred. These suites
prove on-disk state survival, daemon-lock authority handover, config
preservation, and fail-closed behavior at the container and filesystem
level. They do not prove that an in-flight provider (agent) turn in progress
at the moment of replacement resumes — only that the durable rows, worktree
contents, and config bytes a resumed turn would depend on survive intact.
