# Container image

The root `Dockerfile` builds the public, self-hostable `linux/amd64` Quorum runtime. It is
an image runtime contract, not yet a complete service: its diagnostic default command is
`quorum --help`. Daemon and Web process supervision will be added separately.

## Build and verify

Build from the repository root so `.dockerignore` bounds the context:

```sh
docker build --platform linux/amd64 --tag quorum:local .
./docker/verify.sh quorum:local
```

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

## Runtime paths

Run with one durable volume mounted at `/data`. Quorum's existing `QUORUM_HOME` override
places its database, configuration, and logs below `/data/quorum`. Use these explicit
paths when starting `quorum serve` in the later supervisor layer:

- repository checkout: `/data/repos/project`
- transient task worktrees: `/data/worktrees`
- Quorum state: `/data/quorum`

The process runs as numeric UID/GID `10001:10001`; the mounted volume must be writable by
that identity. The image contains no credentials. Inject provider and GitHub credentials
at runtime through the hosting control plane; do not bake them into an image or volume.

## Provider boundary

The image includes the pinned, checksummed Codex standalone package and its Apache license
and notice. It intentionally does not include Claude Code: its redistribution terms have
not been established for this public image. Self-hosters may create a derived image that
installs Claude Code under terms they have independently accepted. Quorum itself does not
need provider-specific container changes; configure a model profile with `runner = "codex"`
and route the desired roles to that profile to select the bundled provider CLI.
