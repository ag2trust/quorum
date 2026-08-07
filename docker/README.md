# Container image

The root `Dockerfile` builds the public, self-hostable `linux/amd64` Quorum runtime. Its
default command supervises `quorum serve` and the read-only `quorum web` dashboard for one
repository. `tini` runs as PID 1 and forwards signals to the supervisor.

## Build and verify

Build from the repository root so `.dockerignore` bounds the context:

```sh
docker build --platform linux/amd64 --tag quorum:local .
./docker/verify.sh quorum:local
```

Container verification requires a running Docker daemon and `sqlite3` on the host. The
mandatory full `preflight.sh` gate additionally requires the Docker buildx plugin.

The build pins the Debian base digest, Rust builder version, direct `git` and `gh` package
versions, and Codex release/checksum. A wrong `CODEX_SHA256` fails the build before the
archive is extracted. `verify.sh` exercises this negative path automatically; the
equivalent manual check is:

```sh
docker build \
  --build-arg CODEX_SHA256=0000000000000000000000000000000000000000000000000000000000000000 \
  --target codex-fetcher .
```

Rebuilding after a Debian package leaves its configured repositories may require updating
the base digest and package pins together. The root-owned provider binary is upgraded by
building and replacing the image. Quorum managed runs ignore Codex user configuration, so
this image does not claim to control provider update behavior through a user config file.

## Run the supervisor

Set `QUORUM_REPO` to the repository's `owner/name` identity and mount durable state at
`/data`. The volume must already contain a git checkout at `/data/repos/project`; the
supervisor does not clone the repository.

Run exactly one active container for each globally unique `owner/name` identity. Quorum
namespaces coordination state by that identity, so different repositories resolve different
state paths. Overlapping containers for the same identity are unsupported; during an upgrade,
stop the old container before starting its replacement on the preserved state.

```sh
docker run --rm \
  -e QUORUM_REPO=owner/name \
  -v /host/quorum-data:/data \
  quorum:local
```

On every start, the supervisor runs idempotent `quorum init` before either managed process.
This creates or migrates the repository database and ensures the persistent routing config
under `/data/quorum` exists; subsequent starts preserve that config.

The dashboard listens on loopback only (`127.0.0.1:8080`) and is therefore not published
outside the container, even if Docker maps the port. Set `QUORUM_WEB_PORT` to change its
port. A proxy in an ordinary container on the same bridge network cannot reach that
loopback address. Remote access requires a trusted sidecar sharing the Quorum container's
network namespace (for example, `--network container:<quorum-container>`) and proxying the
loopback listener. General ingress configuration is outside this image's current scope.

The container never restarts a failed child. If Web exits unexpectedly, the container
exits 1. Otherwise it propagates `quorum serve`'s exact code. Examples include 0 for a
clean drain, 1 for an expected clean negative, 2 for usage or bad input, 3 for an internal,
database, or migration failure, and 75 when supervised self-update is requested. An
external orchestrator owns image rebuild and container relaunch. Base-branch self-update
drain is disabled by default; set `QUORUM_SELF_UPDATE_DRAIN=1` to opt in.

## Runtime paths and settings

Run with one durable volume mounted at `/data`. Quorum's existing `QUORUM_HOME` override
places its database, configuration, and logs below `/data/quorum`. The supervisor defaults
to:

- repository checkout: `/data/repos/project`
- transient task worktrees: `/data/worktrees`
- Quorum state: `/data/quorum`

Override the checkout, worktree, log, Web port, or readiness budget with
`QUORUM_REPO_DIR`, `QUORUM_WORKTREE_BASE`, `QUORUM_LOG_DIR`, `QUORUM_WEB_PORT`, or
`QUORUM_READY_TRIES`, respectively. `QUORUM_REPO` is always required.

The process runs as numeric UID/GID `10001:10001`; the mounted volume must be writable by
that identity. The image contains no credentials. Inject provider and GitHub credentials
at runtime through the hosting control plane; do not bake them into an image or volume.

## Provider boundary

The image includes the pinned, checksummed Codex standalone package and its Apache license
and notice. It intentionally does not include Claude Code: its redistribution terms have
not been established for this public image. Self-hosters may create a derived image that
installs Claude Code under terms they have independently accepted. Quorum itself does not
need provider-specific container changes; `quorum serve --agent codex` selects the bundled
provider CLI.
