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
