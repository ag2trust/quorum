# Sandboxed Workspaces — Technical Specification

**Date:** 2026-11-12
**Status:** Proposed
**Implementation base:** `origin/develop` at `b090a1a`
**Implementation plan:** `docs/2026-11-12-sandboxed-workspace-implementation-plan.md`

## Problem

The daemon executes repository code today. Managed runs work in linked worktrees of the
operator's checkout (`repo_dir`), and the daemon then runs `git` inside those worktrees and in
the checkout they share a git directory with. Everything in a worktree is written by the
managed agent, including the per-worktree git directory, the shared common config the agent
can reach with an ordinary `git config`, the shared hooks directory, `.gitattributes`, and
every script in the tree. Concretely:

- Every daemon publication push runs `git -C <worktree> push`
  (`quorum/src/serve/worktree.rs:617-619`), so Git runs the pre-push hook resolved from the
  agent-written tree with the daemon's credentials. In this repository that hook runs
  `./preflight.sh` from the worktree (`.githooks/pre-push:37,151,161`), which compiles and runs
  agent-written code. The push URL itself is read from the worktree's config
  (`worktree.rs:604`).
- A linked worktree shares `config` and `hooks/` with `repo_dir`. An agent's plain
  `git config credential.helper …`, `core.sshCommand`, `core.hooksPath`, or
  `remote.origin.uploadpack` reaches every later daemon `git` call in `repo_dir`.
- Pre-teardown preservation runs `git add -A` inside the worktree with a scratch index
  (`quorum/src/serve/worktree/recover.rs:818-841`). A clean filter declared in agent-written
  config and selected by an agent-written `.gitattributes` runs as the daemon. No agent action
  is needed at that moment: startup GC and cleanup trigger it.
- Continuation and branch synchronization run `git merge` inside worktrees
  (`worktree.rs:1017,1193`), which runs merge hooks and configured merge drivers.

A sandbox cannot fix this. A linked worktree's `.git` file points into the operator's checkout,
so a sandbox that mounts only the workspace cannot use it, and mounting the common git
directory would hand the sandbox the daemon's hooks and config.

## Goals

- The daemon executes no repository code: no hooks, filters, drivers, configured commands,
  tests, preflight, or repository scripts, in any deployment profile.
- A writing role's workspace is a self-contained clone. A read-only role's workspace is an
  exported tree with no `.git`. No git directory the daemon executes against is ever inside,
  linked from, or shared with a workspace.
- The daemon never runs `git` with a workspace as its repository, git directory, or work tree,
  and never runs `gh` from inside any repository.
- Commits leave a run only as a git bundle file. The daemon imports it into a private
  repository it alone owns, with hooks, system and global configuration, and every transport
  except local files disabled, and verifies refs, ancestry, and size before anything is
  published.
- The daemon pushes only from its private repository, with hooks disabled, to one
  operator-configured URL.
- Files a run can write (bundles, session files, results) are read only through a bounded,
  no-follow, regular-file-only reader, so they cannot steer the daemon into its own secrets or
  fill its volume.
- Read-only roles are served diffs, history, file lists, and trees from the private repository
  as files, and have no repository to run `git` against.
- Local operators keep working, with one documented migration.

## Non-goals

- The sandbox launcher interface (typed run/stop/inspect). This spec names the seam it plugs
  into and the two things it must carry (mounts and the export step); the interface itself is
  separate work.
- Read-only role prompts. This spec defines the review materials the daemon produces; changing
  prompts to use them, and to stop instructing shell or `gh` for read-only roles, is separate
  work.
- How the daemon obtains GitHub credentials. A credential-supply contract is separate work; this
  spec states which git configuration the transport commands may still read until it exists.
- Running any verification locally. Verification belongs to pull-request CI and the developer.
- Repository-driven environment provisioning inside the sandbox.

## Terms

| Term | Meaning |
|---|---|
| **Private repository** | A bare repository the daemon creates and alone uses, at `private_repo`. Never mounted into, linked from, or shared with a run. |
| **Workspace** | A run's directory under `worktree_base`: a self-contained clone for a writing role, or an exported tree with no `.git` for a read-only role (planner, reviewer, review-only, continuation reviewer). Written by runs; untrusted in its entirety. |
| **Staging directory** | A daemon-only directory on the same filesystem as `worktree_base`, where a workspace is built before it is handed to a run. |
| **Session directory** | A per-task directory outside the workspace where the coding runner keeps its own conversation state so a later run of the same task can resume. Written by runs; untrusted; the daemon never parses it except through the untrusted-file reader. |
| **Run outbox** | A per-run directory, outside the workspace, where the run leaves its bundle. Written by the run; untrusted. |
| **Export step** | The fixed command that creates the bundle. It is part of the run and executes inside the run's isolation, never in the daemon. |
| **Import quarantine** | A per-import object directory inside the private repository's `objects/` filesystem. A bundle's pack is indexed there, checked, and either moved into the private repository or deleted. |
| **Launcher** | Whatever starts a run's processes. The built-in local launcher starts them on the operator's machine; an external launcher starts them in a sandbox. |
| **Operator remote URL** | `remote_url`: the only URL the daemon fetches from or pushes to. |

Deployment profiles follow `docs/2026-08-17-github-collaboration-mcp-technical-spec.md`:

- **Local permissive.** The local launcher. The agent shares the operator's machine and can
  reach anything the operator can. The daemon still executes no repository code and never
  shares a git directory with a run, but it does not claim a containment boundary.
- **Isolated runtime.** An external launcher. The run sees only its workspace, its session
  directory, and its outbox, reaches only the daemon's run-facing endpoints, and holds no
  repository credential.

## Current-state audit

Appendix A lists every place the daemon, or a process it spawns other than the agent itself,
runs `git`, `gh`, a shell, or reads run-written files, with `file:line` evidence from
`origin/develop` at `b090a1a`. Every production `git` invocation goes through
`quorum_core::git::git_command` (`quorum-core/src/git.rs:10`), which only adds
`-c core.fsmonitor=false`. `run_git` adds `GIT_TERMINAL_PROMPT=0` and `GIT_OPTIONAL_LOCKS=0`
and bounds time and output (`worktree.rs:207-315`). Nothing else is hardened.

| Class | Rows | Meaning |
|---|---|---|
| SAFE | 9 | Keep as is. Not repository code, or the agent process itself. |
| MOVE | 28 | Keep the operation; run it against the private repository with the hardened environment. |
| EMPTY-DIR | 11 | `gh` or repository-less `git`; run from a daemon-owned empty directory with an explicit repository or URL. |
| REMOVE | 19 | Delete; the responsibility moves into the run, into workspace seeding, or into durable state. |
| HARDEN | 3 | File reads of run-written state; route through the untrusted-file reader. |

Highest risk, in order:

1. **Publication push from the worktree** (`worktree.rs:598-627,1675,1769`). Runs the
   tree-resolved pre-push hook, which runs `preflight.sh` from the worktree, with daemon
   credentials. `QUORUM_CONTINUATION_BASE_BRANCH` is passed to that hook (`worktree.rs:1674`).
   The URL comes from agent-writable config (`worktree.rs:604`), so commits and credentials can
   be sent to an arbitrary host.
2. **Shared git directory.** All `repo_dir` commands (27 MOVE rows) read config and hooks that
   agents can write through their linked worktree. `disable_push` makes it worse: it enables
   `extensions.worktreeConfig` in the shared config (`worktree.rs:1455-1521`).
3. **Preservation `git add -A` in the worktree** (`recover.rs:818-841`). Clean filters run
   during startup GC and cleanup with no agent present.
4. **Daemon `git merge` in worktrees** (`worktree.rs:1017,1193`). Merge hooks and drivers.
5. **Provider executable resolved relative to the worktree** (`routed_provider_executable_in_path`, `quorum/src/serve/mod.rs:26790-26874`).
   A configured relative path, or a relative or empty `PATH` entry, resolves inside the
   agent-written tree, so a planted file is launched as the next route's provider.

No production path runs `preflight.sh`, tests, or another repository script directly. The only
route to them is the hooks above, which this design removes.

Outside the daemon, the operator's own tooling also executes `repo_dir` state:
`scripts/serve-supervisor.sh:154-164` runs `git fetch`, `git merge --ff-only`, and
`./dev-install.sh` in `repo_dir` on self-update. Hooks or config planted there through a linked
worktree stay planted after migration, so migration reports them (see "Migration").

## Mode decision

**The clone model becomes the only workspace model.** It ships behind
`workspace_mode = "linked-worktree" | "clone"` while the slices land. The default flips to
`clone` once the clone path covers every role, and the linked-worktree path is deleted one
release later.

Rationale:

- **It is right for local operators too.** The four highest-risk findings are just as
  exploitable on a laptop. An agent that writes a hook today gets it run by the daemon with the
  operator's GitHub credentials, whether or not a sandbox exists.
- **Two git lifecycles would double the riskiest code.** `worktree.rs` and `recover.rs` are
  about 3,600 production lines of serialized git state handling. Keeping both lifecycles
  permanently would mean every lifecycle fix, recovery path, and adversarial test exists twice,
  and the weaker path stays exploitable.
- **The profile is a launcher choice, not a git model.** Local permissive and isolated runtime
  differ only in where the run's processes, including the export step, execute. The daemon-side
  model is identical, so the guarantee "the daemon executes no repository code" has one
  implementation and one test suite.
- **The temporary switch keeps `develop` green.** Each slice adds clone-mode behavior beside
  the current behavior, with its own tests, and the default flips only when the clone path is
  complete.

What local operators see after the flip: worker workspaces are full clones instead of linked
worktrees, so they cost disk (see "Decisions"), read-only roles get an exported tree with no
`.git`, and an agent's workspace has no `origin` remote. Agents already publish only through the
daemon, so a missing remote changes nothing that works today. A daemon push no longer runs the
repository's pre-push hook, so a repository that relied on that hook as a gate for daemon pushes
must enforce the same checks in CI.

**Early hardening of the linked-worktree mode.** The worst paths should not wait for the flip.
Once this repository's pre-push checks run in CI, the shared `git_command` gains, in both modes,
an empty `core.hooksPath`, the protocol allowlist, `GIT_ATTR_SOURCE` for commands that do not
check out files, signature programs set to `false`, and a push URL taken from configuration (or
derived once at startup) instead of from the worktree. The planner view's `archive` already
runs from a daemon-owned git directory that borrows only `repo_dir`'s object store (audit row
M25, fixed ahead of this slice). Teardown also stops snapshotting worktrees with
`git add -A` and retires their directories instead. The container image moves to a current git
in the same change: its git 2.39 predates `GIT_ATTR_SOURCE` (2.41), so attribute isolation
would otherwise be inactive in the image. This closes highest-risk findings 1 and 3, and the
hook half of finding 2. Linked mode stays exploitable through filter, driver, and transport keys
an agent can write into the shared git directory, including `info/attributes`, which no
command-line override neutralizes. Only the clone model removes that.

**Git LFS.** Daemon pushes have never uploaded LFS objects themselves; the LFS pre-push hook
did. With hooks disabled, a push would publish pointer files without their objects. LFS is
refused for now. Both modes detect LFS use in the base or delivered tree and park the task with
the reason `lfs-unsupported` instead of pushing. Detection runs in the private repository or
`repo_dir`, reads no file content beyond what it needs, and looks for either signal:

- **`filter=lfs`.** Only `.gitattributes` blobs are read, found by path with `ls-tree -r`, and
  comment lines are skipped.
- **A pointer file.** `ls-tree -r -l` lists blob sizes; only blobs under 1 KiB are candidates.
  `cat-file --batch` reads each candidate, and it counts if its first line is
  `version https://git-lfs.github.com/spec/v1` or the legacy
  `version https://hawser.github.com/spec/v1`. A pointer file without a matching attribute
  still parks; a document that quotes the header line anywhere but its first line, or is
  1 KiB or larger, does not.

The base tree's result is cached by tree SHA. For a delivered tree only the paths changed against
the base (`diff-tree -r`) are examined. If a listing exceeds its output bound, detection fails
closed: the task parks with `lfs-unsupported` and the detail `detection bound exceeded`.

Supporting LFS would mean uploading LFS objects (`git lfs push`) from the private repository
with a daemon-chosen binary. That is future work.

## Architecture

```
            operator remote URL (fetch, push)        GitHub API (gh, empty cwd, --repo)
                     ▲   │                                     ▲
       hardened push │   │ hardened fetch + fsck               │
                     │   ▼                                     │
   ┌───────────────────────────────────────┐                  │
   │ private repository (bare, daemon-only)│──────────────────┘
   │  refs/remotes/origin/*                │
   │  refs/quorum/tasks/<id>/{base,head}   │◀── import + verify ── staged bundle copy
   │  objects/quorum-quarantine/<run>      │                            ▲
   │  refs/quorum-publication/*, recover,  │                 untrusted-file reader
   │  cleanup tombstones                   │                            │
   └──────────────┬────────────────────────┘                  run outbox/<run>/delivery.bundle
                  │ file:// fetch into staging, then rename            ▲
                  ▼                                                   │ export step (run-side)
          worktree_base/<workspace>  ── mounted into the run ─────────┘
```

The daemon writes the private repository and the staging directory. A run writes the
workspace, its session directory, and its outbox. Nothing written by a run is ever a git
directory the daemon executes against.

## Private repository

- **Location.** `private_repo`, default `<state dir>/repository.git`, next to `quorum.db`
  (`quorum/src/paths.rs:86-89`). It must not be inside `worktree_base`, `log_dir`, or `repo_dir`.
  Startup refuses overlap.
- **Creation.** `git init --bare --template=<empty template dir>` under the hardened
  environment, with `--object-format` set to the upstream's format (SHA-1 unless configured).
  The daemon then writes the config itself. Startup compares the config with an allowlist: the
  keys the daemon writes, plus the keys `git init` itself writes on some platforms
  (`core.repositoryformatversion`, `core.filemode`, `core.ignorecase`,
  `core.precomposeunicode`, `core.symlinks`, `extensions.objectformat`). Any other key is
  treated as corruption.

  ```
  [core] repositoryformatversion, bare = true, logAllRefUpdates = false
  [gc] auto = 0
  [receive] denyCurrentBranch = refuse
  [uploadpack] allowAnySHA1InWant = false, allowReachableSHA1InWant = false
  ```

  `hooks/`, `info/attributes`, `info/exclude`, `info/grafts`, `shallow`,
  `objects/info/alternates`, and `objects/info/http-alternates` must be absent. Startup verifies
  all of this before the first command.
- **Ref namespaces.**

  | Ref | Writer | Meaning |
  |---|---|---|
  | `refs/remotes/origin/<branch>` | hardened fetch | Upstream branches the daemon has fetched. |
  | `refs/quorum/pr/<n>` | hardened fetch | A fetched `refs/pull/<n>/head`. |
  | `refs/quorum/tasks/<id>/base` | seeding | The commit a task's first workspace was seeded from. |
  | `refs/quorum/tasks/<id>/head` | import | The last verified task commit. The only source of publication. |
  | `refs/quorum-publication/task-<id>` | publication | Existing pin; moves here unchanged. |
  | `refs/quorum-recover/...` | recovery | Existing recover refs; move here unchanged. |
  | `refs/quorum/cleanup/...` | cleanup | Existing tombstones; move here unchanged. |

  The private repository has no `refs/heads/*`. Task branches exist only as names on the
  remote and in the workspace.
- **Serialization.** One manager mutex serializes every command on the private repository, as
  `WorktreeManager.lock` does today. Commands keep `run_git`'s timeout, output bound, and
  kill-and-reap.
- **Maintenance.** Automatic GC is off. The daemon runs `git gc --prune=<age>` under the lock
  at startup and after every 50 imports, with the local timeout. A rejected import never puts
  objects into the private repository: its quarantine directory is deleted (see "Bundle
  verification rules").
- **Workspace state per task** (durable, additive columns): `seeded_base_sha` (the first seed),
  `seeded_upstream_sha` (the `origin/<base>` tip given to the current workspace),
  `imported_head_sha`, and a consecutive-rejection counter with the last rule code.

## Hardened git invocation

One constructor in `quorum-core` replaces `git_command` for every daemon-side `git` process. It
has two profiles. The local profile is for every command that reads or writes objects: import,
verification, diff, archive, merge-tree, refs, and seeding. The transport profile is for fetch,
push, and `ls-remote` against the operator remote URL.

**Process**

- The working directory is a daemon-owned empty directory, `<state dir>/empty`, mode `0500`,
  verified empty at startup.
- The repository is always named explicitly with `--git-dir=<path>`, plus `--work-tree=<path>`
  for staging checkouts only. `-C` is not used.
- Arguments taken from data (SHAs, ref names, URLs) follow `--end-of-options` where the
  subcommand accepts it. SHAs are validated as 40 or 64 lowercase hex characters. Ref names are
  validated with the existing Rust ref checks before they reach Git.
- Output is bounded and the process is killed and reaped as `run_git` does now.

**Environment**

| Variable | Local profile | Transport profile, `inherit` | Transport profile, `hermetic` |
|---|---|---|---|
| Inherited environment | cleared; then `PATH` from the daemon, `HOME=<state dir>/empty`, `LC_ALL=C`, `TZ=UTC` | cleared; then an allowlist copied from the daemon: `PATH`, `HOME`, `USER`, `LOGNAME`, `TMPDIR`, `LANG`, `LC_*`, `SSH_AUTH_SOCK`. Never `GH_TOKEN`, `GITHUB_TOKEN`, `QUORUM_*`, or any `GIT_*` | as the local profile |
| `GIT_CONFIG_NOSYSTEM` | `1` | unset | `1` |
| `GIT_CONFIG_GLOBAL` | `/dev/null` | unset | `/dev/null` |
| `GIT_ATTR_NOSYSTEM` | `1` | `1` | `1` |
| `GIT_ATTR_SOURCE` | empty tree `4b825dc…` (SHA-1) or its SHA-256 equivalent; the sanitized attribute tree of `<ours>` for `merge-tree` and `show --remerge-diff` only (see below) | same | same |
| `GIT_ALLOW_PROTOCOL` | `file` | the scheme of `remote_url` | the scheme of `remote_url` |
| `GIT_CEILING_DIRECTORIES` | `<state dir>` | same | same |
| `GIT_TERMINAL_PROMPT`, `GIT_OPTIONAL_LOCKS` | `0`, `0` | same | same |
| `GIT_NO_REPLACE_OBJECTS` | `1` | same | same |
| `GIT_EDITOR`, `GIT_SEQUENCE_EDITOR`, `GIT_PAGER` | `false`, `false`, `cat` | same | same |
| `GIT_ASKPASS`, `SSH_ASKPASS`, `GIT_SSH_COMMAND`, `GIT_SSH` | unset | unset | unset |

`inherit` keeps the operator's global and system git configuration files for transport commands
only, so a local operator's credential helper and SSH configuration keep working. That
configuration belongs to the operator, not to a run. Under the local permissive profile it is
still reachable by the agent, as everything on that machine is. Because it is not daemon-owned,
`inherit` also guards against URL rewriting. Before the first transport command and before every
push, the daemon runs:

- `git ls-remote --get-url --end-of-options <remote_url>` and requires the output to equal
  `remote_url`;
- `git config --get-regexp '^url\..*\.(push)?insteadof$'`, and refuses if any `pushInsteadOf`
  prefix matches `remote_url`.

A rewrite is refused, with exit 2 at startup and a park at push time, instead of being followed.

`hermetic` reads no configuration outside the private repository. It authenticates only through
the separate credential-supply contract and is accepted now for `file://` remotes and tests. A
deployment with an external launcher must use `hermetic`.

**Command-line configuration (both profiles)**

```
-c core.hooksPath=<state dir>/empty-hooks   # daemon-owned, empty, mode 0500
-c core.fsmonitor=false -c core.untrackedCache=false
-c core.alternateRefsCommand= -c core.alternateRefsPrefixes=
-c core.protectHFS=true -c core.protectNTFS=true
-c protocol.allow=never -c protocol.<scheme>.allow=always   # file locally; the remote_url scheme for transport
-c transfer.fsckObjects=true -c fetch.fsckObjects=true -c receive.fsckObjects=true
-c submodule.recurse=false -c fetch.recurseSubmodules=false -c push.recurseSubmodules=no
-c gc.auto=0 -c maintenance.auto=false -c fetch.writeCommitGraph=false
-c core.bigFileThreshold=16m -c core.deltaBaseCacheLimit=32m -c pack.threads=1
-c commit.gpgSign=false -c tag.gpgSign=false -c push.gpgSign=false
-c gpg.program=false -c gpg.ssh.program=false -c gpg.x509.program=false
-c diff.external= -c core.pager=cat
-c credential.helper=          # local profile and hermetic transport only
```

Command-line `-c` values take precedence over every configuration file, so in `inherit` mode an
operator `core.hooksPath` or `core.fsmonitor` is still overridden. Diff-producing commands also
pass `--no-ext-diff --no-textconv --no-color`.

Transport fetches may need an escape hatch for upstream history that strict fsck rejects (for
example, old commits with malformed author lines). The `git_fsck_ignore` key lists fsck message
IDs. Each becomes `-c fetch.fsck.<id>=ignore` on transport fetches only, never on bundle imports.

`GIT_ATTR_SOURCE` is not applied to the seeding checkout. There, honoring the tree's `eol` and
`working-tree-encoding` attributes is needed for a correct checkout, and it is safe: with no
filter or driver defined in any configuration the command reads, an attribute can only select
built-in behavior.

Merges are the other exception. `merge-tree` and `show --remerge-diff` run with
`GIT_ATTR_SOURCE=<attr tree>`, a sanitized attribute tree the daemon builds from `<ours>`, the
tree being merged into, which is the tree whose attributes a work-tree `git merge` would read.
The same tree must be used for both commands. Otherwise a merge that came out clean through
`merge=union` shows a spurious `remerge CONFLICT (content)` hunk in `remerge.patch`, so a
reviewer sees a conflict that never happened. For a remerge diff, `<ours>` is the merge's first
parent.

The attribute tree is built in Rust from the `.gitattributes` blobs of `<ours>`, read with
`ls-tree -r` and `cat-file`:

- Each kept line has its pattern and only these merge attributes: `merge=text`,
  `merge=binary`, `merge=union`, `-merge`, `!merge`, and `merge`. The built-in `binary` macro
  becomes `-merge`.
- Everything else is dropped: other attributes, including `conflict-marker-size`; `merge=`
  with any other value; macro definitions; and comments.
- The sanitized blobs are written with `hash-object -w` and assembled with `mktree` at the same
  paths, so directory-scoped `.gitattributes` keep their scope.
- The input is bounded to 1 MiB of `.gitattributes` content in total. Over the bound the daemon
  uses the empty tree, so every file gets the built-in text merge, and records an event.

The result honors the tree's built-in merge behaviors and nothing else. Custom merge drivers
remain impossible twice over:

- a `merge=<name>` attribute is not copied into the attribute tree;
- even if it were, it selects a driver only through `merge.<name>.driver` configuration, and
  none exists in any configuration the command reads. For an attribute naming an undefined
  driver, git falls back to its built-in text merge.

`conflict-marker-size` is dropped because git does not cap it. With git 2.53, one three-line
conflicting file with `conflict-marker-size=50000000` in the raw `<ours>` attributes took 1.1 s
and 160 MB resident in `merge-tree`, against 0.03 s and 10 MB with the empty tree. The cost grows
linearly with the value. `merge-tree` and `show --remerge-diff` also run under rule 16's
process group, timeout, and Linux rlimits (see "Bundle verification rules"), so any remaining
resource path is bounded; a merge killed that way takes the "any other exit" path below.

Every other setting in the local profile is unchanged for these two commands, and every other
command keeps the empty tree. With git 2.53 on a scratch bare repository:

- `merge=union` merged cleanly with `<ours>` as the attribute source and conflicted with the
  empty tree. With no attribute source set, git 2.53 also ignored the attributes of a bare
  repository's `HEAD`, so the source is always set explicitly.
- A hand-built sanitized tree holding only `u.txt merge=union` merged `u.txt` cleanly and gave
  the `conflict-marker-size=50000000` file default 7-character markers, in 0.04 s and 10 MB.
- For that union merge, `show --remerge-diff` printed a `remerge CONFLICT (content)` hunk with
  the empty tree as the attribute source, and nothing with `<ours>`.
- `merge=evil` with no driver configuration ran nothing and fell back to the text merge. When
  both sides edited the same line, git reported a conflict with standard markers; when they
  edited different lines, the merge was clean (exit 0). A `.gitconfig` in the tree defining
  `merge.evil.driver` was not read.
- As a control, the same attribute with `-c merge.evil.driver=…` did run that command. The
  daemon never passes such a key, and the private repository's config allowlist has none.

`git archive` is different: by default it ignores `GIT_ATTR_SOURCE` and applies the archived
tree's own attributes. With git 2.53, the empty tree as the attribute source, and no system or
global configuration, a plain `archive` did all of these:

- dropped an `export-ignore` file;
- expanded `$Format:%H$` in an `export-subst` file;
- converted an `eol=crlf` file to CRLF.

Worse, a commit with a hand-made `gpgsig` header and an `export-subst` file containing
`$Format:%G?$` made `archive` spawn `gpg` from `PATH` on attacker data. Every archive therefore
passes `--worktree-attributes`. In a bare repository with the empty tree as the attribute source,
that makes the export verbatim: the `export-ignore` file was present, `$Format…$` stayed literal,
line endings were kept, and no `gpg` ran. As defense in depth, the `-c` list also sets every
signature program to `false`.

**What this rules out.** The daemon cannot run hooks (`core.hooksPath` points at an empty
directory, and repository and global hooks paths are overridden). It cannot run a configured
filter, textconv, external diff, merge driver, fsmonitor, editor, pager, or askpass. None is
defined in the private repository's own config, global and system config are ignored by the
local profile, and repository attributes are read only by the seeding checkout and, after
sanitizing, by `merge-tree` and `show --remerge-diff`. There they can select only built-in
behavior. Everywhere else `GIT_ATTR_SOURCE` is the empty tree, and `archive` also passes
`--worktree-attributes` so the archived tree's own attributes are not applied. No signature
program can run. Transports other than local files and the remote's scheme are refused before
connection. With git 2.53, the local profile refused `ext::` and `https` URLs ("transport not allowed") and still
read a bundle file. Network fetches run with `fetch.fsckObjects=true`. Bundle imports do not
rely on fetch-side fsck, which only covers bundles from git 2.48 onward. They run
`index-pack --strict` explicitly (see "Bundle verification rules").

**URL validation.** `remote_url` must be `https://`, `ssh://`, scp-like `user@host:path`, or
`file://`. Transport-helper syntax (`<name>::`), a leading `-`, control characters, and embedded
credentials are rejected at startup with exit 2. The URL is never read from any repository
config at run time.

**Git version.** The hardened constructor checks `git --version` once at startup, in every mode
that uses it, and logs the result.

- **Floor: 2.43.**
  - `GIT_ATTR_SOURCE` arrived in 2.41, and older git ignores it silently.
  - `merge-tree --write-tree` with an attribute source was unreliable before 2.43.
  - Below 2.43, clone mode refuses to start (exit 2) with an error that names the version found
    and the floor.
  - Below 2.43, the linked-worktree mode's early hardening logs a warning naming the version
    found, says that attribute isolation is inactive, and continues; that mode's hardening is
    best effort anyway. The warning lasts until the linked-worktree mode is removed.
- **Not version-dependent:** bundle object checking uses `index-pack --strict` and does not
  depend on 2.48's fetch-side bundle fsck.
- **CI:** a job runs the hostile-git suites on the floor version as well as on a current
  release.
- **Security releases:** operators are expected to track upstream git security releases. The
  floor checks functionality, not patch currency; Quorum neither fails nor warns on a git that
  lacks the latest security fixes.
- **Container image:** the image always ships a current git release. Today it pins the
  distribution's 2.39 (`Dockerfile:32`), which is below the floor and predates
  `GIT_ATTR_SOURCE`. The image moves to a current git with the early linked-mode hardening
  (plan slice 2).

**Inherited `GIT_*` variables.** Both profiles start from a cleared environment, so variables in
the daemon's own environment cannot redirect a command. That covers `GIT_DIR`,
`GIT_WORK_TREE`, `GIT_INDEX_FILE`, `GIT_OBJECT_DIRECTORY`, `GIT_ALTERNATE_OBJECT_DIRECTORIES`,
`GIT_EXEC_PATH`, `GIT_TEMPLATE_DIR`, `GIT_CONFIG_PARAMETERS`, and
`GIT_CONFIG_COUNT`/`GIT_CONFIG_KEY_<n>`/`GIT_CONFIG_VALUE_<n>`. The import quarantine is the
only case where the daemon sets `GIT_OBJECT_DIRECTORY` and `GIT_ALTERNATE_OBJECT_DIRECTORIES`
itself.

**Source-scan test.** The existing test `production_git_commands_use_the_shared_constructor`
(`quorum-core/src/git.rs:48`) is extended so that production code outside the
private-repository module and the local launcher's export step must not construct a git
command, use `-C`, or set `current_dir` for a `git` or `gh` process.

## Workspace lifecycle

### 1. Allocate

A task allocation keeps its existing durable fields (branch name, workspace path under
`worktree_base`). It adds the workspace state listed under "Private repository". The path
binding lives only in the database; Git has no worktree registration to reconcile.

### 2. Seed

The daemon builds every workspace in the staging directory and hands it over with one
no-replace rename into `worktree_base`. That is `renameat2(RENAME_NOREPLACE)` on Linux and
`renamex_np(RENAME_EXCL)` on macOS; plain `rename(2)` would silently replace an empty directory
a run created at the target. Git therefore never runs in a directory that a run has ever been
able to write. Staging must be on the same filesystem as `worktree_base`; startup checks the
device IDs.

```
git init --template=<empty template dir> <staging>/<name>
git --git-dir=<staging>/<name>/.git fetch --no-tags --no-write-fetch-head \
    --end-of-options file://<private_repo> \
    <seed ref>:refs/heads/<branch> \
    refs/remotes/origin/<base>:refs/remotes/origin/<base>
# <seed ref> per the variant table below, e.g. refs/quorum/tasks/<id>/head
git --git-dir=… --work-tree=<staging>/<name> checkout --no-recurse-submodules <branch>
git --git-dir=… config user.name <operator identity> ; user.email <…>
```

- The fetch copies objects (`file://` uses the pack protocol), so no object file is hardlinked
  between the workspace and the private repository. A writable hardlink would let a run corrupt
  the daemon's objects. The seeding test asserts `st_nlink == 1` on workspace object files.
- The workspace has no remote, no hooks directory, and no config beyond the identity. Agents
  publish only through the daemon.
- The daemon does not chmod the seeded tree. The tree may contain symlinks from hostile
  content, such as a fork's PR head, and a permission walk could follow one into daemon files.
  Read-only is the launcher's mount mode for non-writing roles. Under local permissive there is
  no read-only enforcement, and none is claimed.

**Read-only roles get no `.git`.** The planner, reviewer, review-only, and continuation-reviewer
roles do not get a clone. Their workspace is an exported tree of the frozen commit, built in
staging and handed over with the same no-replace rename:

```
git --git-dir=<private_repo> archive --worktree-attributes --format=tar \
    --end-of-options <frozen sha>
# local profile: GIT_ATTR_SOURCE is the empty tree, and every signature program is `false`
```

`--worktree-attributes` is required. Without it `archive` applies the archived tree's own
`export-ignore`, `export-subst`, and `eol` attributes despite `GIT_ATTR_SOURCE`, and
`export-subst` can make it run `gpg` (see "Hardened git invocation"). With it, the role sees the
tree exactly as committed.

**Extraction.** The daemon extracts the stream itself in Rust instead of with the system `tar`.
Git can store paths that collide on a case-insensitive filesystem: a tree with an `A` symlink
and an `a/pwn` file passed `fsck --strict`. System `tar` is safe there only through its default
symlink handling, which differs between implementations. The extractor works relative to a
directory descriptor for `<staging>/<name>`:

- It creates every directory and file with `openat(…, O_NOFOLLOW | O_CREAT | O_EXCL)`, walking
  components with `O_NOFOLLOW | O_DIRECTORY`, so it never writes through a symlink.
- It creates symlinks with `symlinkat` and never resolves their targets. Absolute and `..`
  targets are kept as data, because they point nowhere the daemon follows.
- It refuses an entry with an absolute path, a `..` or `.git` component, or an entry type other
  than a file, directory, or symlink.
- It refuses an entry whose path meets an existing symlink or entry, as a case-folding
  collision does.

**Bounds.** The archive stream is bounded by `max_export_bytes` (`[workspace_limits]`, default
256 MiB, today's planner-view bound at `quorum/src/serve/mod.rs:6059`) and by the planner view's
30 s timeout.

- A stream over the bound parks the task with `export-too-large` without retry. A repository
  that large does not shrink on retry, so this is not a seeding failure.
- A refused entry parks the task with `export-unsafe-path`, naming the path, without retry.
- A timeout is an infrastructure failure, retried at most 3 times like an import, then parked
  with `export-too-large`.

The planner's frozen view uses the same export, bound, and extractor.

**Contents.** The workspace holds no `.git` directory or file, no refs, and no objects.
Everything else a read-only role needs from history (diff, log, file lists) comes as
daemon-written files in the run's inputs directory (see "Review and planning materials"). There
is nothing to export, so the launcher receives no export declaration and the export step does
not run for these roles.

**No discovery outside the workspace.** Git searches parent directories for a repository when
the current one has no `.git`. If `worktree_base` sat inside a repository, such as a dotfiles
repository in `$HOME` or `repo_dir` itself, an agent's `git` commands and the CLI's own
fallbacks would act on that repository. Those fallbacks are `paths::resolve_repo`
(`quorum/src/paths.rs:28-47`) and `git_toplevel` (`paths.rs:122`). Therefore:

- every run's environment sets `GIT_CEILING_DIRECTORIES` to the workspace's parent;
- a read-only run's environment also sets `GIT_DIR` to a path that does not exist and that the
  run cannot create (`<state dir>/empty/.none`, as for `gh`);
- every read-only run receives `QUORUM_REPO`;
- startup refuses, with exit 2, a `worktree_base` that has a `.git` entry in itself or in any
  ancestor. The check is a filesystem walk and runs no git.

Seed variants. *Start* is the commit the delivered tip must descend from. *Exclude* is the
export declaration's exclude set. Every exclude SHA is recorded durably when the workspace is
seeded.

| Run | Seeded `refs/heads/<branch>` | Extra refs | Start | Exclude |
|---|---|---|---|---|
| First worker run | the verified base tip; also written to `refs/quorum/tasks/<id>/base` | `origin/<base>` | base | base, upstream tip |
| Rework or retry (workspace reused) | nothing; the daemon does not touch it | nothing | imported head | imported head, `seeded_upstream_sha` |
| Rework or retry (workspace missing) | `refs/quorum/tasks/<id>/head` in a new workspace | `origin/<base>` | imported head | imported head, new upstream tip |
| Reviewer, review-only, continuation reviewer | no clone: the exported tree of the verified PR head SHA (`refs/quorum/pr/<n>`), plus review materials | none (no `.git`) | n/a (no export) | n/a |
| Planner | no clone: the exported tree of the frozen SHA of `refs/remotes/origin/<base>` | none (no `.git`) | n/a (no export) | n/a |
| Continuation worker, clean base merge | a daemon-made merge commit (see "Branch synchronization and continuation") | `origin/<base>` | the merge | the merge, upstream tip |
| Continuation worker or sync judgment, conflicting | `<ours>` (the PR head or sync target tip) | `<theirs>` as `refs/remotes/origin/<branch>` | `<ours>` **and** `<theirs>` | `<ours>`, `<theirs>`, upstream tip |
| Resume from a recover ref | the recover snapshot's parent commit; the snapshot tree is checked out in staging and the index is reset to the parent, reproducing uncommitted changes | `origin/<base>` | the parent | the parent, upstream tip |

A reused workspace (rework) is never touched by daemon git, so its `origin/<base>` is not
refreshed. Its exclude set names the upstream tip it was actually seeded with, which the
workspace holds, not the current tip, which it does not. When a run needs a different starting
point (a new continuation base, a recovery), the daemon seeds a new workspace and retires the
old directory (see "Cleanup").

Writing roles get a full, self-contained clone; there is no shallow seed. Read-only runs copy
no objects: each gets a fresh exported tree and its materials (see "Decisions").

### 3. Hand off to the run

The launcher seam receives, per run:

- the workspace path and its mode (read-write for writing roles, read-only otherwise);
- for read-only roles, the inputs directory holding the review materials;
- the session directory path (outside the workspace and per task);
- the run outbox path, `<state dir>/runs/<run_id>/outbox/`, which the daemon creates empty with
  mode `0700`;
- for writing roles, an export declaration: `{ branch, exclude: [sha…], file: "delivery.bundle" }`,
  with `exclude` taken from the seed-variant table.

Repository identity cannot come from the workspace, which has no remote:

- **Agent-MCP runs** already receive `QUORUM_REPO` as one of the run fields copied into the MCP
  child (`AGENT_MCP_ENV_VARS`, `quorum/src/serve/runner.rs:777-782`).
- **Restricted spawns** (planner, classifier) have it removed by `strip_coordination_env`
  (`quorum/src/serve/agent.rs:1219-1229`). If such a run invokes the CLI, `paths::resolve_repo`
  falls back to `git remote get-url origin` (`quorum/src/paths.rs:28-47`), which fails in a
  workspace, or, without the discovery ceiling, reads an enclosing repository. Clone mode keeps
  `QUORUM_REPO` for those spawns, and gives it to every read-only run, since it is not a
  credential.
- **Prompts that still name `gh` commands** need `GH_REPO=<owner/name>` in the run
  environment. That sets the repository, not a credential.

### 4. Export (run-side)

When a writing role's agent process exits, for any reason, the launcher runs the export step
inside the same isolation as the run. Read-only roles have no export step:

```
git -c core.hooksPath=/dev/null -c core.fsmonitor=false -c gc.auto=0 \
    -C <workspace> bundle create --version=3 <outbox>/delivery.bundle \
    refs/heads/<branch> --not <exclude…>
```

with `GIT_CONFIG_NOSYSTEM=1` and `GIT_CONFIG_GLOBAL=/dev/null`. Version 3 carries
`@object-format`, which a SHA-256 repository needs and which the import checks. The export writes
nothing else. A status manifest was considered and dropped: `git status` runs clean filters and
recurses into submodules, which would add run-side processes the launcher must then contain for
no lifecycle benefit.

The export step runs against the workspace by design. It is the run working on its own files.
Under the isolated runtime profile it executes in the sandbox, with no network and no
credentials. Under the local permissive profile the local launcher runs it:

- in a new process group, killed and reaped as a group after a fixed timeout (60 s), so a
  process it spawns cannot outlive it;
- with an environment built from an allowlist (`PATH`, `HOME`, `LANG`, `LC_*`, `TMPDIR`, plus
  the git variables above). No `GH_TOKEN`, `GITHUB_TOKEN`, `SSH_AUTH_SOCK`, `QUORUM_*`, or
  orchestrator token.

No such scrubbed environment exists today. `strip_operator_secrets`
(`quorum/src/serve/runner.rs:695`) removes only the orchestrator token, so the allowlist is new
work in the export slice. The export has no authority the agent did not already have. Its
hardening flags are defense in depth for the run, not a daemon guarantee.

A worker's lifecycle "done" signal is unchanged. Publication now waits until the run has exited
and the export step has finished. If a run stays alive after a terminal signal, the daemon ends
it after a bounded grace period.

### 5. Collect

The daemon opens `<outbox>/delivery.bundle` with the untrusted-file reader (see "Untrusted file
handling"). It streams the bundle, up to `max_bundle_bytes`, into a temporary file in
`<state dir>/imports/` created `O_EXCL` with mode `0600`, `fsync`s it, and renames it to
`<run_id>.bundle`. A crash therefore never leaves a partial copy under the final name. Every
later step uses only this private copy, so the run cannot change the bytes between
verification and import. A missing bundle maps to "no delivery" (see "Failure modes").

### 6. Import and verify

See "Bundle verification rules". On success the quarantined pack moves into the private
repository and `refs/quorum/tasks/<id>/head` moves to the verified tip with an expected-old
`update-ref`. The staged copy is removed on success and on rejection. It is kept only across an
infrastructure retry.

### 7. Publish

Publication keeps its durable intent, pin, lease, and verification stages
(`quorum/src/serve/mod.rs:2206-2384`). The changes:

- The source SHA is `refs/quorum/tasks/<id>/head`, never a workspace `HEAD`
  (replaces `head_sha`, `worktree.rs:1564`).
- The pin, the PR-head fetch, the fast-forward ancestry check, the push, and the `ls-remote`
  check run against the private repository and the operator remote URL:

  ```
  git --git-dir=<private_repo> push --no-verify --porcelain \
      --force-with-lease=refs/heads/<branch>:<expected|empty> \
      --end-of-options <remote_url> <sha>:refs/heads/<branch>
  ```

  The lease and fast-forward rules are unchanged (`worktree.rs:1589-1793`). The env var
  `QUORUM_CONTINUATION_BASE_BRANCH` and the `hook-stdout` part of push-rejection diagnostics
  (`worktree.rs:160-172`) are removed; the remote's own rejection text is still recorded.
- `gh pr create` and `gh pr view` run from the empty directory (see "Pull requests and `gh`").

### 8. Next run

A rework run reuses the workspace and exports with `exclude = [imported head,
seeded_upstream_sha]`. If the agent rewrote already-imported history, the new bundle does not
descend from the last head and import rejects it. That is the same fast-forward-only rule `push_to_pr_head` enforces today
(`worktree.rs:1644-1666`).

### 9. Cleanup

The daemon deletes workspaces, staging directories, outboxes, and session directories with
symlink-safe recursive removal, never with git. `std::fs::remove_dir_all` does not follow
symlinks on Unix (std since 1.58.1), and a wrapper refuses a root that is itself a symlink or
not a directory. A run may have removed write permission from its own directories. The wrapper
therefore walks the tree without following links, restoring the owner write bit on each real
directory through a descriptor opened with `O_NOFOLLOW | O_DIRECTORY`, and skips symlinks. A
superseded workspace (re-seeded, or found unreferenced at startup) is renamed
to `worktree_base/.retired/<name>-<unix time>` and deleted after
`superseded_workspace_retention_secs`. Uncommitted work is never destroyed immediately, and the
daemon never needs `git status` to find out whether there is any. A terminal task's workspace is
retired the same way.

## Bundle verification rules

The rules run in order. The first failure rejects the bundle with a stable rule code. A
rejected bundle never writes an object or a ref into the private repository: its pack only ever
exists in the import quarantine, which is deleted on rejection.

The design follows from two findings with git 2.53:

- **Foreign objects pass fetch-based checks.** A hand-built bundle with a valid prerequisite can
  reference objects the private repository already holds but the run was never given. One case
  is a tree entry naming another task's blob. Another is a merge whose second parent is another
  task's unpublished commit, known only by its SHA. Such a bundle passes `bundle verify`,
  `fetch` with `fetch.fsckObjects=true`, ancestry, and root checks, and the foreign blob then
  becomes readable through the imported ref. Rules 12 and 13 close this.
- **Rejected objects stay on disk.** Deleting the ref of a rejected fetch and running
  `prune --expire=now` left the rejected commit present: a fetch of more than
  `fetch.unpackLimit` objects keeps a pack, and `prune` removes only loose objects. Rule 9's
  quarantine closes this.

**A. File (untrusted-file reader)**

1. Opened without following any symlink in any path component under the outbox.
2. A regular file with `st_nlink == 1`.
3. At most `max_bundle_bytes` (default 256 MiB), checked by `fstat` and again while streaming.

Rule codes: `unsafe-file`, `oversized-bundle`.

**B. Header (Rust parser, before any git process reads the file)**

4. The first line is `# v2 git bundle` or `# v3 git bundle`. The header ends at the first empty
   line and is at most 64 KiB.
5. In v3, the only allowed capability is `@object-format=<the private repository's format>`. Any
   `@filter=` or unknown capability is rejected. v2 is accepted only for a SHA-1 repository.
6. **Exactly one ref line**, and its name is exactly `refs/heads/<task branch>`. A second ref,
   `HEAD`, any other branch, any tag, or a non-`refs/heads/` name is rejected. The experiment
   with `bundle create --all` produced `HEAD` and every branch, which this rule refuses.
7. **Between 1 and 64 prerequisites**, each well-formed. A bundle with no prerequisites carries
   full history and is rejected.

Rule codes: `malformed-header`, `unsupported-bundle`, `unexpected-refs`, `no-prerequisite`.

**C. Pack pre-scan (Rust, streaming, before any git process reads the pack)**

8. The pack after the header is parsed entry by entry. Each zlib stream is inflated with the
   output discarded, so memory stays bounded. The scan rejects any of the following:
   - an entry count above `max_objects` (default 100,000);
   - a declared object size, or a declared delta result size from the delta header, above
     `max_blob_bytes`;
   - a declared total above `max_inflated_bytes` (default 1 GiB);
   - an inflated stream longer than its declared size;
   - a malformed entry or trailer.

   The scan records the base OID of every `REF_DELTA` entry. CPU is bounded by the declared
   sizes, which are capped, so a tiny delta that declares a multi-GiB result is refused before
   git inflates it.

Rule codes: `malformed-pack`, `oversized-object`, `too-many-objects`.

**D. Quarantined indexing (local profile)**

9. The pack is indexed into a fresh quarantine directory,
   `<private_repo>/objects/quorum-quarantine/<run_id>/pack/`, with
   `GIT_OBJECT_DIRECTORY` set to the quarantine and `GIT_ALTERNATE_OBJECT_DIRECTORIES` set to
   the private repository's `objects/`:

   ```
   git --git-dir=<private_repo> index-pack --strict --fix-thin --stdin \
       --max-input-size=<max_bundle_bytes>
   ```

   `--strict` runs fsck on every received object. Bundle packs are thin: indexing a normal
   bundle pack without `--fix-thin` failed with `pack has 1 unresolved delta`. `--fix-thin`
   resolves the external bases from the private repository and appends them to the quarantined
   pack. Rule 12 checks those bases. The process runs under the resource limits in rule 16.

Rule code: `fsck-failed`.

Every later check runs with the same quarantine environment, so new objects resolve without
touching the private repository.

**E. Provenance**

10. Every prerequisite must be present and must be an ancestor of, or equal to, one of the
    export declaration's `exclude` SHAs. Presence comes from `cat-file -e` and ancestry from
    `merge-base --is-ancestor`. A missing prerequisite and a present-but-unseeded one give the
    same code, so a run cannot probe which SHAs the private repository holds.
11. The tip resolves from the quarantined pack.

Rule code: `bad-prerequisite`.

**F. Object membership**

12. Every external delta base recorded in rule 8 must be an object in the tree of one of the
    validated prerequisites (`ls-tree -r -t <prerequisite>`, plus the prerequisite commit and
    its root tree). These are the only bases `pack-objects --thin` uses. A `REF_DELTA` against
    any other object would let the pack reproduce content the run was never given.
13. Every object in `rev-list --objects --no-object-names <tip> --not <exclude…>` must have been
    received in the pack: it is in the quarantined pack's index (`show-index`), and it is not
    one of the external bases `--fix-thin` appended. This refuses both foreign-object shapes above. An
    object that resolves only through the private repository, such as another task's blob or
    commit, a recover snapshot, or a PR head, means the bundle relied on something it did not
    carry.

Rule codes: `foreign-delta-base`, `foreign-object`.

Received objects outside that range, which a well-formed export does not produce, are allowed
but count toward every cap.

**G. History**

14. The tip descends from every start commit in the seed-variant table: `base` for a first run,
    `head` for a rework, the merge for a clean continuation, and both `<ours>` and `<theirs>`
    for a conflicting continuation or sync judgment. So an agent cannot "resolve" a conflict by
    dropping `<theirs>`. The tip must also differ from each start commit, since equal means no
    new commits.
15. Beyond those:
    - the range introduces no new root commit (`rev-list --max-parents=0 <tip> --not
      <exclude…>` is empty);
    - `rev-list --count` is at most `max_commits` (default 2,000);
    - every blob, sized with `cat-file --batch-check`, is at most `max_blob_bytes` (default
      100 MiB, GitHub's hard file limit).

    These recheck the pre-scan's declared sizes against the actual objects.

Rule codes: `unrelated-history`, `merge-not-completed`, `no-new-commits`, `new-root`,
`too-many-commits`, `oversized-blob`.

**H. Resources**

16. Every git process in the import:
    - runs in its own process group, killed and reaped as a group;
    - runs under the local timeout and `core.bigFileThreshold=16m`,
      `core.deltaBaseCacheLimit=32m`, `pack.threads=1`;
    - on Linux, also runs under `RLIMIT_AS`, `RLIMIT_CPU`, and `RLIMIT_FSIZE` set in
      `pre_exec` (default 2 GiB, 120 s, `2 × max_bundle_bytes`).

    macOS does not enforce `RLIMIT_AS`, so local permissive deployments there rely on the
    pre-scan and the timeout. A git process killed by a signal, by an rlimit, or by the timeout
    while importing a staged bundle gives `resource`, a rejection charged to the run. A spawn
    failure or `ENOSPC` is an infrastructure failure (see "Failure modes").

Rule code: `resource`.

**Promotion.** The quarantined `.pack` is moved into `<private_repo>/objects/pack/`, then its
`.rev` if present, then its `.idx` last, so a reader never sees an index without its pack. The
quarantine is on the same filesystem, so each move is a rename. Then
`update-ref refs/quorum/tasks/<id>/head <tip> <expected-old>` runs, and the quarantine directory
is removed. Imports are serialized under the manager mutex. At startup, a quarantine directory
without a completed promotion is deleted and its staged copy replayed (see "Recovery").

## Untrusted file handling

A single module, `untrusted_fs`, is the only way the daemon reads a path a run can write.

- It opens relative to a daemon-held directory descriptor for the trusted root (outbox,
  session directory, provider invocation home), component by component, with
  `openat(…, O_NOFOLLOW | O_CLOEXEC | O_DIRECTORY)` for directories and
  `O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC` for the final file. On Linux it may use `openat2` with
  `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS` when available. `O_NONBLOCK`
  means a FIFO cannot hang the daemon. The result must then be a regular file.
- It requires `S_ISREG` and `st_nlink == 1` (a hardlink to a daemon secret is refused), and a
  size at most the caller's cap. It reads with that cap enforced and refuses rather than
  truncating unless the caller asked for a bounded prefix.
- Parsed content (provider project config, repository instruction files) is size-capped before
  parsing, must be UTF-8 with no NUL, and is treated as data.
- Provider adapters that read workspace files use the workspace itself as the trusted root.
  Today these are a provider's project-config files and a repository instruction file at the
  workspace root that is passed into a restricted run's system prompt. The adapter receives the
  bytes, or a daemon-written copy, and never a path that a symlink could redirect. Discovery
  walks never leave the workspace root, even when the run has deleted `.git`.
- Directories a run can write are deleted only with the symlink-safe removal above. The daemon
  never `canonicalize`s a run-writable path to decide where to write.

Under the isolated runtime profile a run cannot exceed its workspace, session, or outbox disk
quota; that is the launcher's job. The daemon bounds only what it copies into its own storage.

## Pull requests and `gh`

All `gh` processes come from one constructor (Appendix A, EMPTY-DIR rows):

- The working directory is `<state dir>/empty`.
- `--repo owner/name` is always passed, and `GH_REPO` is also set for `gh api`, whose
  `{owner}/{repo}` placeholders otherwise come from the working directory's remote
  (`quorum/src/serve/collector.rs:1415-1450`).
- `GIT_DIR=<state dir>/empty/.none` and `GIT_CEILING_DIRECTORIES=<state dir>` are set, so any
  `git` call `gh` makes fails instead of discovering a repository.
- `GH_PROMPT_DISABLED=1`, `GH_NO_UPDATE_NOTIFIER=1`, and `NO_COLOR=1` are set, and the token is
  handled as `merge.rs` does now.
- `gh pr create` always passes `--head`, `--base`, `--title`, and `--body-file -`, so it never
  inspects a local branch.
- `quorum serve` no longer resolves `owner/name` with `gh repo view` in `repo_dir`
  (`quorum/src/main.rs:417-438`). It uses `repo`, or parses `remote_url`.

A fake `gh` cannot show whether the real binary tolerates `GIT_DIR` pointing at a nonexistent
path for each subcommand Quorum uses. Following the repository guide's rule that CLI boundaries
need real-binary tests, an owner-run contract test exercises the real `gh` with that
environment against a scratch repository, for every subcommand Quorum uses: `pr create`,
`pr view`, `pr list`, `pr close`, `pr checks`, `pr comment`, `pr merge`, `pr review`,
`repo view`, and `api`.

`git check-ref-format` in `validate_target_branch` (`quorum-core/src/tasks.rs:1571`) runs from
the empty directory with the local profile.

## Review and planning materials

The daemon produces read-only inputs for non-writing roles from the private repository. They are
written into a per-run inputs directory beside the outbox, and the launcher mounts it read-only.
A read-only role's workspace has no `.git` (see "Seed"), so these files and the exported tree
are everything it gets from the repository's history.

| File | Command (local profile) | Bound |
|---|---|---|
| `diff.patch` | `diff --no-ext-diff --no-textconv --no-color -M <base>...<head>` | `max_review_diff_bytes` (default 4 MiB); on overflow, per-file patches in path order until the bound, with a `TRUNCATED` trailer listing omitted paths |
| `diffstat.txt` | `diff --stat=200 --no-color <base>...<head>` | 256 KiB |
| `files.json` | `diff --name-status -z` | 1 MiB |
| `log.json` | `log --format=%H%x00%P%x00%an%x00%aI%x00%s -z <base>..<head>` | `max_commits` entries |
| `remerge.patch` | `show --remerge-diff <merge>` for each daemon-made merge in range, with the merge's sanitized attribute tree as `GIT_ATTR_SOURCE` (see "Hardened git invocation") | 1 MiB |

The tree itself is the role's workspace, exported as described under "Seed", so the inputs
directory carries no copy of it. The daemon picks `<base>` and `<head>` per review kind, so every
range a review prompt names today has a file equivalent: for example, a branch-sync CI-fix
review's range starts at the recorded merge (`quorum/src/serve/reviewer.rs:477-492`), and a
branch-sync judgment review gets that merge's `remerge.patch`.

The internal API is
`PrivateRepo::review_materials(base, head, limits) -> ReviewMaterials { dir, base_sha, head_sha, truncated }`.
These materials and the exported tree are the input to the separate read-only role prompt work.
Today's read-only prompts name `git` commands (`quorum/src/serve/reviewer.rs:218,477-492`) that
cannot run in a workspace without `.git`. The default therefore does not flip to clone mode
until that prompt work has landed.

The planner's frozen view is the exported tree of `refs/remotes/origin/<base>` at the frozen SHA
in the private repository. That is today's view with two differences: the source is the private
repository, not `repo_dir`'s `HEAD` (`quorum/src/serve/mod.rs:6493,6752`), and the export is
verbatim. Before the M25 fix the archive applied `export-ignore`, `export-subst`, and `eol` from the
tree, so files could differ from what the planner saw then; since that fix the planner view is
already verbatim.

In this design the planner's inputs directory is created but empty: it has no PR range, and
today's planner view carries no history either. If the read-only prompt work gives the planner
history, it uses `review_materials` with a range ending at the frozen SHA.

Planner deliverable paths are classified the same way. Today
`classify_writable_deliverable_path_blocking` (`quorum-core/src/decomposition.rs:130-200`,
reached through `planner::validate_for_source` from `quorum/src/serve/mod.rs:9329` and `quorum/src/serve/agent_endpoint.rs:1201`)
canonicalizes each path against `repo_dir`'s working tree. That tree is stale in clone mode and
agent-writable in linked mode. In clone mode the check walks the frozen tree in the private
repository instead, using `ls-tree` per path component and refusing any component with mode
`120000` (symlink) or `160000` (gitlink).

## Branch synchronization and continuation

The daemon computes merges in the private repository and never runs `git merge` in a work tree.

```
GIT_ATTR_SOURCE=<attr tree of ours> \
git --git-dir=<private_repo> merge-tree --write-tree --name-only --no-messages <ours> <theirs>
```

- **Clean (exit 0).** The daemon writes the merge commit with `commit-tree <tree> -p <ours>
  -p <theirs>` and its existing daemon identity. For branch sync it pushes that commit through
  the hardened push. For continuation it seeds the worker's workspace at it. `--ff` semantics
  are kept: when `<ours>` is an ancestor of `<theirs>`, the result is `<theirs>`.
- **Conflict (exit 1).** The output lists the conflicted paths. These replace
  `sync_conflicted_files` (`worktree.rs:1348`). The daemon seeds a judgment or continuation
  workspace at `<ours>` with `<theirs>` present, and the run's instructions tell the agent to
  run the merge itself. The import requires the delivered tip to descend from both `<ours>` and
  `<theirs>` (rule 14, `merge-not-completed`). The agent's `git merge` runs in its own sandbox,
  where hooks and drivers are its own business. The existing contract that the daemon prepares
  `MERGE_HEAD` changes accordingly, and the prompt text at `quorum/src/serve/mod.rs:3127-3130`
  changes with it.
- **Any other exit.** A loud failure, as today.

`merge-tree` reads the sanitized attribute tree built from `<ours>` (see "Hardened git
invocation"), so a repository relying on `merge=union`, `merge=binary`, or `-merge` keeps that
behavior in daemon-side merges. A repository relying on a custom merge driver or on
`conflict-marker-size` does not: those attributes are dropped, git uses its text merge with
default markers, and any resulting conflict goes to the agent, who resolves it in the sandbox.

## Recovery, preservation, and startup reconciliation

- **Recover refs** move to the private repository unchanged in name and attribution. They are
  created only from commits the daemon already holds: an imported head, or a branch tip being
  deleted. The daemon no longer snapshots uncommitted workspace state. Retention of superseded
  workspaces replaces it.
- **Resume from a recover ref** seeds a new workspace (seed variant table) instead of
  `reset --keep` or `cherry-pick` in an existing tree (`recover.rs:549-727`).
- **Startup reconciliation** compares durable allocations with the directories under
  `worktree_base`. An allocated directory stays. An unallocated one is retired, not deleted. A
  top-level symlink is unlinked, never followed. There are no git worktree registrations to
  prune, unlock, or verify (removes `worktree.rs:1821-2211`).
- **Reviewer verdict recovery** uses the durable head SHA the reviewer workspace was exported
  from, recorded in the journal at provisioning, instead of `git rev-parse HEAD` in the reviewer's
  worktree (`quorum/src/serve/mod.rs:5528`).
- **Publication pins and cleanup tombstones** keep their CAS semantics in the private
  repository.
- **Staged bundle copies** found at startup are re-imported if the run's publication is still
  pending, and deleted otherwise. Leftover `objects/quorum-quarantine/<run_id>` directories are
  deleted first. Import is idempotent: promotion uses an expected-old `update-ref`, and a pack
  already promoted for the run is detected by its checksum.
- **Reconciliation scope.** Directory reconciliation lists only direct children of
  `worktree_base`. It skips `.retired/` and the staging directory, which have their own sweeps.
- **Legacy journals** (migrated tasks) have no seeded head SHA for verdict recovery. Recovery
  then takes the existing fail-safe path: it returns no recovered verdict and keeps the task
  for a fresh review (`quorum/src/serve/mod.rs:5543-5545`).

## Configuration surface

New `serve` keys, with the existing precedence (flag over file, unknown keys rejected):

```toml
# Workspace git model. "linked-worktree" is deprecated and is removed one release after
# "clone" becomes the default.
workspace_mode = "clone"
# Bare repository used only by the daemon. Default: <state dir>/repository.git.
# private_repo = "/home/user/.quorum/repos/owner__name/repository.git"
# The only URL Quorum fetches from and pushes to. If unset under the local launcher, it is read
# once at startup from <repo_dir>/.git/config (remote.origin.url) with `git config --file`
# (no includes, no discovery) and logged. Required with an external launcher.
# remote_url = "https://github.com/owner/name.git"
# Git configuration visible to fetch and push against remote_url: "inherit" (operator's global
# and system config) or "hermetic" (none; requires the credential-supply contract).
# git_transport_config = "inherit"

[workspace_limits]
max_bundle_bytes = 268435456         # 256 MiB
max_blob_bytes = 104857600           # 100 MiB
max_commits = 2000
max_review_diff_bytes = 4194304      # 4 MiB
max_export_bytes = 268435456         # 256 MiB, read-only workspace and planner view export
max_objects = 100000
max_inflated_bytes = 1073741824      # 1 GiB, declared total in the pack pre-scan
superseded_workspace_retention_secs = 604800

# fsck message IDs ignored on transport fetches from remote_url only (never on bundle imports).
# git_fsck_ignore = ["missingEmail"]
```

`worktree_base` keeps its name and now holds workspaces. `repo_dir` remains for the operator's
checkout: self-update, and one-time URL derivation and migration. In clone mode no managed run
touches it, and the daemon runs nothing in it except `git config --file` and the one-time
migration fetch.

## Failure modes

| Failure | Detection | Lifecycle mapping | Routing disposition |
|---|---|---|---|
| No bundle after a worker's done signal (no commits, export failed) | outbox empty after run exit | `AgentFailed { reason: "no-delivery: <exporter status>" }` through the existing worker failure and retry budget | `NonFailover` |
| Bundle rejected (rules 1–16, including `resource`) | verification rule | `AgentFailed { reason: "bundle-rejected:<rule>" }`; nothing pushed; quarantine and staged copy deleted; event records the rule and bounded detail | `NonFailover` |
| Same rejection rule on consecutive runs of a task, up to the existing rework or retry cap | durable counter (workspace state) | park with the rule as the park reason (`tasks::park`), `PublicationFailureKind::Other` | n/a |
| An `exclude` SHA from the declaration missing from the private repository | checked before rule 10, against the daemon's own record | park; a daemon-side inconsistency, not the run's fault | n/a |
| Import infrastructure failure (spawn failure, `ENOSPC`, lock) | process or IO result | staged copy kept; retried on the next tick, at most 3 times; then park | n/a |
| LFS in use (`filter=lfs` or a pointer file in the base or delivered tree), or the detection bound exceeded | `ls-tree` and `cat-file --batch` in the private repository (see "Git LFS") | park with `lfs-unsupported`; nothing pushed | n/a |
| `remote_url` rewritten by `url.*.insteadOf` or `pushInsteadOf` under `inherit` | `ls-remote --get-url` and the config check | exit 2 at startup; park at push time | n/a |
| Push rejected (lease, non-fast-forward, remote policy) | `push --porcelain` | unchanged: `push_rejection` park path (`worktree.rs:160`), without the local hook section | n/a |
| Seeding failure (private fetch, disk, rename) | process or IO result | existing provisioning-failure path: release the claim, retry with backoff | n/a |
| Read-only export over `max_export_bytes` | bounded stream | park with `export-too-large`; no retry | n/a |
| Read-only export entry refused by the extractor (absolute or `..` path, `.git` component, path through a symlink, case-folding collision, unsupported entry type) | Rust extractor | park with `export-unsafe-path`, naming the path; no retry | n/a |
| Read-only export timeout | 30 s timeout | infrastructure failure: retried at most 3 times, then park with `export-too-large` | n/a |
| Operator remote unreachable on fetch | transport result | existing fetch-failure handling (dependency defer, provision retry) | n/a |
| Invalid `remote_url`, private repository config drift, non-empty `empty-hooks`, old git, `worktree_base` inside a repository | startup check | refuse to start, exit 2 for configuration and exit 3 for state | n/a |

`NonFailover` follows the existing meaning: an execution-boundary failure that must not move the
run to another provider route (`quorum-core/src/routing_attempts.rs:20-31`).

## Launcher seam

The launcher interface is separate work. This design needs only two things to cross it, both data:

1. **Mounts.** The workspace path and mode, the session directory, the outbox, and the inputs
   directory, all created by the daemon.
2. **Export step.** For writing roles only, the export declaration from step 3. The launcher
   runs the fixed export command inside the run's isolation after the agent process ends,
   including on timeout or kill, and reports the exporter's exit status and up to 2 KiB of
   stderr. A read-only run has no declaration, and the launcher runs no export for it.

The local launcher is today's provider adapters (the `*_agent.rs` modules under
`quorum/src/serve/`, started through `runner::gated_command`) plus the export step. The adapters
keep `current_dir(workspace)`. They must stop reading workspace files except through
`untrusted_fs` (Appendix A, HARDEN rows). Neither a provider executable nor a provider home may
resolve relative to the workspace (REMOVE rows R18 and R19).

## Migration for local operators

Migration runs on the first start with `workspace_mode = "clone"`, under the existing startup
recovery, so no runs are alive.

**Preconditions (refuse to start, exit 2, listing what blocks):**

- **Quiescence.** No non-terminal task holds a legacy *writing* allocation, meaning a worker in
  `working` or `rework` with an allocated linked worktree. Its uncommitted work would otherwise
  leave the lifecycle while staying on disk. Operators let those tasks reach review or a
  terminal state under `linked-worktree` without admitting new work, then restart. Tasks in
  `open`, `in-review`, or `merging`, and terminal tasks, do not block.
- **URL agreement.** A `remote_url` derived from `<repo_dir>/.git/config` must parse, with
  `parse_repo_from_url`, to the configured `repo`. That file was writable through every linked
  worktree, so a mismatch is refused, not followed.

**Steps:**

1. Create and verify the private repository and validate `remote_url`.
2. **Build the expected-ref list from the database**, never from `repo_dir`'s advertisement:
   - each non-terminal task's branch;
   - each publication intent's pinned `local_sha`;
   - each recorded recover ref and cleanup tombstone, with its SHA.
3. **Import** with one hardened `file://` fetch per expected ref, using explicit refspecs, no
   globs, and fsck.
   - Where a publication intent exists, `refs/quorum/tasks/<id>/head` is set from its pinned
     `local_sha`, not from the branch tip, since the pin is what publication replays.
   - Otherwise it is set from the branch tip, which must descend from the allocation's recorded
     provenance SHA.
   - `refs/quorum/tasks/<id>/base` is set from that provenance SHA.
   - Recover refs, publication pins, and tombstones keep their names.
   - After the fetch, every expected ref must exist with its expected SHA where the database
     records one. Config in `repo_dir` such as `uploadpack.hideRefs` or `transfer.hideRefs` can
     hide refs from upload-pack, so a missing ref fails migration (exit 3) instead of being
     skipped silently.
   - The fetch spawns `git-upload-pack` against `repo_dir`. That is the one operation that still
     reads `repo_dir`'s possibly agent-written config. It runs once, only under the local
     permissive profile, where the legacy trust already applied. Git honors upload-pack's
     command hook (`uploadpack.packObjectsHook`) only from protected configuration, never from
     repository config.
4. **Report planted state in `repo_dir`.** Quorum never modifies `repo_dir`. Instead it emits one
   warning event listing what it finds:
   - hook files other than `*.sample` in `.git/hooks/`, and in any configured `core.hooksPath`;
   - config keys in `.git/config` and `.git/worktrees/*/config.worktree` outside a short
     allowlist (`core.*` defaults, `remote.origin.url`, `remote.origin.fetch`, `branch.*`,
     `user.*`);
   - `.git/info/attributes`.

   The operator's own git, and `scripts/serve-supervisor.sh:154-164` (`git merge` then
   `./dev-install.sh` in `repo_dir`), would otherwise keep running them.
5. Leave every legacy linked worktree on disk, untouched and unregistered from Quorum. Log one
   event per task with its path.
6. A branch synchronization with a kept conflict worktree restarts from its pinned tips through
   `merge-tree`.
7. **Write the durable one-row marker at completion**, in one transaction after every import is
   verified (an additive, forward-only migration). A crash before that point reruns the whole
   migration, which is idempotent: refs are set with expected-old values. Switching back to
   `linked-worktree` after the marker exists is refused with exit 2.

Operators who pushed through the daemon and relied on a repository pre-push hook must move those
checks to CI. For this repository, the hook's base-branch and formatting checks
(`.githooks/pre-push`) must be required CI jobs before the early linked-mode hardening lands.
`cargo fmt --all --check` already runs on every pull request (`.github/workflows/ci.yml:19-27`);
it must also be a required check in branch protection. The base-branch check has no CI job yet.
The `main` hotfix adding one is being prepared as a separate PR (see "Decisions").

## Security properties and residual risk

Holds in both profiles once the default flips:

- The daemon runs no hook, filter, driver, configured command, test, preflight, or repository
  script.
- The daemon never runs `git` with a workspace as its repository or work tree, and never runs
  `gh` inside a repository.
- Only verified commits on the task's branch, descending from what the daemon seeded, reach
  `refs/quorum/tasks/<id>/head`. Only that ref is published, and only to `remote_url`.
- A run-written file cannot make the daemon follow a symlink or hardlink, block on a FIFO, or
  copy more than the configured bounds.

Holds only under the isolated runtime profile: the run cannot read daemon credentials, the
database, the private repository, or other tasks' workspaces. Under local permissive the agent
shares the operator's machine, and Quorum does not claim otherwise.

Residual:

- Git itself parses hostile objects during indexing, fsck, diff, and archive. Mitigations are
  the Rust pre-scan, `index-pack --strict`, size caps, process-group timeouts, Linux rlimits,
  and a current git. A git parser vulnerability remains a daemon risk. On macOS, memory bounds
  rest on the pre-scan alone.
- The early linked-mode hardening leaves filter, driver, and transport keys in the shared git
  directory reachable until the clone model is the only model.
- Merges read a sanitized subset of repository attributes. A git bug in how `merge-tree` applies
  the built-in `union`, `binary`, or `text` merge would be reachable from repository content.
  Rule 16's timeout and rlimits bound its cost, not its correctness.
- A read-only workspace contains symlinks with arbitrary targets, created but never followed by
  the daemon. Under local permissive the agent can follow them, as it can reach anything on the
  machine.
- `inherit` transport configuration is operator-owned but, under local permissive, writable by
  agents like everything else on that machine.
- Hostile content does reach the remote. That is the point of a pull request. It is verified by
  CI and the developer, not by the daemon.

## Decisions (2026-11-12)

The owner settled these on 2026-11-12. They were open questions in the first draft.

1. **Git floor.** Clone mode refuses to start below git 2.43, with an error that names the
   version found. The linked-worktree mode only warns, until that mode is removed. Quorum does
   not gate on git security-release currency. The container image always ships a current git
   (see "Hardened git invocation").
2. **Clone cost.** Workers get a full, self-contained clone; there is no shallow seed. Read-only
   roles (planner, reviewer, review-only, continuation reviewer) get no `.git` at all: an
   exported tree of the frozen commit plus daemon-supplied review materials (diff, history, file
   lists) as files. They have no export step. Those materials are the input to the separate
   read-only role prompt work, which must land before the default flips (see "Seed" and "Review
   and planning materials").
3. **Merge attributes.** Daemon-side merges (`merge-tree`) honor the tree's `.gitattributes`
   for git's built-in merge behaviors, such as `union`, `binary`, and `text`. Custom merge
   drivers stay impossible because no driver configuration exists in any configuration the
   daemon's git reads. Following review, the daemon reads a sanitized attribute tree built from
   `<ours>` that keeps only those `merge` attributes, so `conflict-marker-size` cannot exhaust
   memory. `show --remerge-diff` uses the same tree. Every other command keeps the empty-tree
   attribute source, and `archive` also passes `--worktree-attributes` (see "Hardened git
   invocation").
4. **CI gate.** The `main` hotfix adding a CI job for the pre-push base-branch check is being
   prepared as a separate PR. Together with making `fmt` a required check, it is the first
   prerequisite. Slice 2, which removes the hook from daemon pushes, must not land before it.
5. **Git LFS.** Refused for now. LFS use, including a bare pointer file, is detected and the
   task parks with `lfs-unsupported`. Uploading LFS objects from the private repository is
   future work.

## Open questions

1. **Snapshotting uncommitted work.** Is retaining superseded workspaces for seven days enough,
   or should the export step also produce a snapshot bundle, imported only into
   `refs/quorum-recover/*` and never published?
2. **Default limits.** 256 MiB bundle, 100 MiB blob, 2,000 commits, 100,000 objects, 1 GiB
   declared inflated size, 4 MiB review diff, and on Linux a 2 GiB import address space. Are
   these acceptable?
3. **Migration quiescence.** Is it acceptable that operators must let writing tasks reach review
   before enabling clone mode, or should Quorum provide an explicit drain control for this?

## Appendix A — Audit

Evidence base: `origin/develop` at `b090a1a`. Line numbers are production code (before each
file's `#[cfg(test)] mod tests`). "repo_dir context" means the command runs with `-C repo_dir`
or `current_dir(repo_dir)`. Today that directory shares config and hooks with every agent's
linked worktree.

### SAFE (9)

| # | Site | What | Why safe |
|---|---|---|---|
| S1 | `quorum/src/serve/runner.rs:707` | `/bin/sh -c` launch-gate wrapper, then `exec` of the provider | Fixed Quorum script. |
| S2 | `quorum/src/serve/agent.rs:479,576,678`; `codex_agent.rs:406,464,522,581` (cwd `424,473,528,587,641`); `grok_agent.rs:1229,1249,1326` | Provider process spawn with `current_dir(worktree)` | This is the agent itself; it moves behind the launcher seam. |
| S3 | `quorum/src/paths.rs:47` | `git remote get-url origin` in the caller's cwd | CLI process of the caller; for a managed run, that is the agent's own process. |
| S4 | `quorum/src/paths.rs:122` | `git rev-parse --show-toplevel` | As S3. |
| S5 | `quorum/src/serve/merge.rs:1433,1462,1557` | `sh -c` of operator-supplied hidden test seams (`--merge-cmd` and similar) | Operator input, not repository code. |
| S6 | `quorum/src/serve/merge.rs:1501,1538` | `git rev-parse HEAD` in `repo_dir` inside the same test seam | Test-only executor; production uses `GhMergeExecutor`. |
| S7 | `quorum/src/serve/mod.rs:6528` | `tar -xf` of a daemon-produced `git archive` stream into a temp dir | Not repository code, but safety rests on the system `tar`'s default symlink handling: paths that collide on a case-insensitive filesystem (an `A` symlink and an `a/pwn` file) pass `fsck --strict`. Replaced by the Rust no-follow extractor ("Seed"). Removal is symlink-safe. Source moves (M25). |
| S8 | `quorum/src/serve/session_log.rs:597-910`; `quorum/src/web.rs:838-1096` | Session stream and transcript files | Written by Quorum from agent pipes; content is data. |
| S9 | `quorum/src/serve/cleanup.rs:523-532`; `recovery.rs:569-575` | `canonicalize` plus `symlink_metadata` before removal | Already symlink-checked. Superseded by symlink-safe removal. |

### MOVE to the private repository (28)

| # | Site | Operation | Note |
|---|---|---|---|
| M1 | `worktree.rs:413-450` (`421,437`) | `pin_publication_source`: `rev-parse`, `update-ref` | Called with the **worktree** as repo (`mod.rs:2240`); runs `reference-transaction` hooks from the worktree. |
| M2 | `worktree.rs:456-486` (`464`) | `retire_publication_source` | repo_dir context (`mod.rs:5351,16044`). |
| M3 | `worktree.rs:488-596` (`497,529,552,575`) | `reconcile_publication_sources` | repo_dir context (`mod.rs:1981`). |
| M4 | `worktree.rs:617-619,1674` | The publication `git -C <worktree> push` | **Highest risk.** Pre-push hook from the tree runs `preflight.sh` (`.githooks/pre-push:37,151,161`). |
| M5 | `worktree.rs:629-650` (`630`) | `branch_exists_unlocked` | Becomes a private ref lookup. |
| M6 | `worktree.rs:738` | `fetch origin <branch>` in `fetch_and_provision` | Uses `remote.origin.*` from shared config. |
| M7 | `worktree.rs:786-860` (`797,808,827`) | Dependency base fetch and ancestry | |
| M8 | `worktree.rs:862-920` (`875,895`) | `find_merged_pr_commit`: fetch, `log --grep` | |
| M9 | `worktree.rs:922-994` (`943,976`) | `verify_branch_contains_commits`, `resolve_ref_unlocked` | |
| M10 | `worktree.rs:1041-1091` (`1050,1076`) | `fetch_sync_tips` | |
| M11 | `worktree.rs:1093-1120` (`1100`) | `source_is_ancestor_of_target` | |
| M12 | `worktree.rs:1122-1142` (`1128`) | `fetch_branch_sync_target` | |
| M13 | `worktree.rs:1144-1179` (`1153`) | `verify_branch_sync_merge_ancestry` | |
| M14 | `worktree.rs:1389` | `fetch origin +refs/pull/<n>/head` | Fork content; gains fsck. |
| M15 | `worktree.rs:1589-1712` (`1600,1610,1626,1652,1675,1677,1688`) | `push_to_pr_head`: fetch, `rev-parse FETCH_HEAD`, ancestry, push, refetch, `ls-remote` | All `-C <worktree>`: the remote, URL, hooks, and `FETCH_HEAD` are agent-controlled. |
| M16 | `worktree.rs:1714-1793` (`1722,1743,1769,1778`) | `push_new_branch` | As M15. |
| M17 | `worktree.rs:2257-2285` (`2264,2271`) | `discover_branch_head` | |
| M18 | `worktree.rs:2287-2311` (`2293`) | `resolve_ref_sha` | |
| M19 | `worktree.rs:2313-2462` (`2329,2373,2396,2416,2435`) | Cleanup tombstone: `ls-remote`, `push --atomic`, `update-ref --stdin` | |
| M20 | `worktree.rs:2464-2569` (`2478,2507,2537,2554`) | `retire_cleanup_tombstone` | |
| M21 | `worktree.rs:2571-2589` (`2574`) | `delete_branch` (`branch -D`) | Becomes a task-ref deletion. |
| M22 | `recover.rs:315,865` | Recover snapshot `update-ref`, `commit-tree` | Store only; the tree source is removed (R13). |
| M23 | `recover.rs:470-547` (`520`) | `resume_branch_from_recover` | |
| M24 | `recover.rs:446,454,742,933,961,992,1051` | Recover-ref listing, trust check, uniqueness, ancestry, pruning, deletion | |
| M25 | `quorum/src/serve/mod.rs:6493,6516` | Planner view: `rev-parse HEAD` and `git archive` in `repo_dir` | Reads the operator checkout's `HEAD`. Becomes the frozen SHA in the private repository. **Current exposure on `develop`:** `archive` applies the tree's own attributes, so `export-subst` with `$Format:%G?$` on a commit with a forged `gpgsig` header makes the daemon run the signature program, either `gpg` from `PATH` or a `gpg.program` in `repo_dir`'s agent-writable config (both verified with git 2.53). Repository config can also name a `filter.<driver>.smudge` program that `archive` runs for any path the agent-writable `info/attributes` marks, and `--worktree-attributes` alone does not stop it (verified with git 2.53). **Fixed ahead of the early hardening:** `archive` runs with `--git-dir` set to a daemon-owned temporary directory and `GIT_OBJECT_DIRECTORY` set to `repo_dir`'s object store. It reads no system, global, or repository config, uses `core.attributesFile=/dev/null` and `--worktree-attributes` (so no attribute file and none of the tree's attributes apply), and sets the signature programs to `false`. |
| M26 | `quorum/src/serve/mod.rs:6752` | `repository_head_sha` (frozen-base capture from `repo_dir` `HEAD`) | As M25. |
| M27 | `quorum/src/serve/mod.rs:24585-24625` | Sync-branch cleanup: `ls-remote origin`, `push origin --delete` in `repo_dir` | |
| M28 | `quorum-core/src/decomposition.rs:130-200` (via `quorum/src/serve/planner.rs:286-301`, from `mod.rs:9329` and `agent_endpoint.rs:1201`) | Planner deliverable paths canonicalized against `repo_dir`'s working tree | Stale in clone mode and agent-writable in linked mode. Becomes an `ls-tree` walk of the frozen tree. |

### EMPTY-DIR (11)

| # | Site | Operation | Current cwd |
|---|---|---|---|
| E1 | `quorum/src/serve/mod.rs:1638-1646` | `run_publication_gh` (`gh pr create`, `gh pr list`) | `repo_dir` |
| E2 | `quorum/src/serve/mod.rs:1666-1685` (also `branch_sync.rs:946-961`) | `gh pr view` target resolution | `repo_dir` |
| E3 | `quorum/src/serve/mod.rs:5755` | Drift check `gh pr list --repo` | inherited daemon cwd |
| E4 | `quorum/src/serve/mod.rs:5703` | Self-update `git ls-remote origin` | `repo_dir`; becomes `ls-remote <url>` |
| E5 | `quorum/src/serve/merge.rs:554-568` | `GhMergeExecutor::build_gh_cmd` | `repo_dir` when `gh_repo` is unset |
| E6 | `quorum/src/serve/merge.rs:652-667` | `gh repo view` default branch | as E5 |
| E7 | `quorum/src/serve/collector.rs:1635-1660` | `gh api repos/{owner}/{repo}/…` | `repo_dir`; placeholders come from its remote when `GH_REPO` is unset |
| E8 | `quorum/src/serve/cleanup.rs:402-417` | `gh pr close` | `repo_dir` |
| E9 | `quorum/src/serve/branch_sync.rs:963-972` | `run_branch_sync_gh` | `repo_dir` |
| E10 | `quorum/src/main.rs:417-438` | `gh repo view` to resolve `owner/name` at serve startup | `repo_dir` |
| E11 | `quorum-core/src/tasks.rs:1571` | `git check-ref-format --branch` (daemon callers `mod.rs:1825,1885,24948`) | inherited cwd |

### REMOVE (19)

| # | Site | Operation | Replacement |
|---|---|---|---|
| R1 | `worktree.rs:598-627` (`604`) | Push URL from `git remote get-url origin` in the worktree | `remote_url` |
| R2 | `worktree.rs:652-680` (`657`) | `worktree list` branch collision | DB allocation |
| R3 | `worktree.rs:682-727` (`703,714`) | `git worktree add` (post-checkout hook; shared gitdir) | Staging seed and rename |
| R4 | `worktree.rs:762,767,1411,1416` | `branch -D` and `worktree add` in review provisioning | Exported tree of `refs/quorum/pr/<n>`, no `.git` |
| R5 | `worktree.rs:996-1039` (`1006,1017`) | Continuation: fetch and `merge` **inside the worktree** | `merge-tree` in the private repository |
| R6 | `worktree.rs:1181-1221` (`1193,1197`) | Branch-sync `merge` in the sync worktree | As R5 |
| R7 | `worktree.rs:1223-1328` (`1294,1313`) | Sync worktree create, reopen, adopt | Seed a judgment workspace |
| R8 | `worktree.rs:1330-1377` (`1331,1335,1356`) | `MERGE_HEAD` and conflict listing in the worktree | `merge-tree --name-only` output |
| R9 | `worktree.rs:1431-1538` (`1432,1456,1484`) | `set_config`, `enable_worktree_config` (mutates shared config), `disable_push` | The workspace has no remote |
| R10 | `worktree.rs:1540-1587` (`1545,1565`) | `verify_head_sha`, `head_sha` in the worktree | Imported head ref |
| R11 | `worktree.rs:1821-2125` (`1927,1957,2009,2039,2054,2082`) | Worktree GC, prune, unlock, remove, registration checks | Directory reconciliation and retirement |
| R12 | `worktree.rs:2127-2211` (`2140,2196`) | `remove_exact` | As R11 |
| R13 | `recover.rs:215-354,764-841` (`765,789,829`) | Preservation: `rev-parse`, `read-tree`, **`add -A`**, `write-tree` in the worktree | Workspace retention |
| R14 | `recover.rs:356-445,974-985` (`385,403,425`); `worker_recover.rs:297-310` | `release_preserving`, `registered_branch`, `branch_checked_out` | DB allocation |
| R15 | `recover.rs:549-605` (`581`) | `reset --keep` in the worktree | Seed a new workspace |
| R16 | `recover.rs:607-727` (`620,644,653,683,698,708`) | `cherry-pick --no-commit`, `reset --hard`, `clean`, `status` in the worktree | Seed with the snapshot tree in staging |
| R17 | `quorum/src/serve/mod.rs:5528` | Verdict recovery: `git rev-parse HEAD` with cwd = reviewer worktree from the journal | Durable seeded head SHA |
| R18 | `quorum/src/serve/mod.rs:26790-26874` (`routed_provider_executable_in_path`) | Fallback provider executable resolved relative to the worktree (relative path or relative `PATH` entry) | Absolute path or daemon `PATH` only |
| R19 | `quorum/src/serve/grok_agent.rs:910-922` | A relative provider-home path is joined to the worktree, so the provider loads a run-written home | Absolute path only |

### HARDEN (3)

| # | Site | Read | Problem |
|---|---|---|---|
| H1 | `quorum/src/serve/grok_agent.rs:558-612` | A provider's project-config files, read in the worktree **and each parent up to a `.git`** | `read_to_string` follows symlinks and is unbounded. If the run deletes `.git`, the walk (`:566`) climbs into the worktree's parent directories. |
| H2 | `quorum/src/serve/grok_agent.rs:820-880` | Provider auth state copied back from the agent-writable invocation home into the operator's home | `fs::metadata` follows symlinks; `fs::read` is unbounded. |
| H3 | `quorum/src/serve/agent.rs:517-519,633-635` | The repository instruction file at the worktree root, checked with `is_file()` (which follows symlinks) and passed by path into a restricted planner or classifier run's system prompt | A symlink pulls any operator-readable file into the prompt. The daemon must pass bytes read through `untrusted_fs`. |

### Prompt text that names git or `gh` for read-only roles

These are inputs to the read-only role prompt work and are not counted above:
`quorum/src/serve/reviewer.rs:218,477-492` and `quorum/src/serve/mod.rs:3127-3130`.
