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
- Each task's workspace is a self-contained clone. No git directory the daemon executes against
  is ever inside, linked from, or shared with a workspace.
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
- Read-only roles can be served diffs, history, and trees from the private repository without
  running `git` themselves.
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
| **Workspace** | One task's self-contained clone under `worktree_base`. Written by runs; untrusted in its entirety. |
| **Staging directory** | A daemon-only directory on the same filesystem as `worktree_base`, where a workspace is built before it is handed to a run. |
| **Run outbox** | A per-run directory, outside the workspace, where the run leaves its bundle and manifest. Written by the run; untrusted. |
| **Export step** | The fixed command that creates the bundle. It is part of the run and executes inside the run's isolation, never in the daemon. |
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
| MOVE | 27 | Keep the operation; run it against the private repository with the hardened environment. |
| EMPTY-DIR | 11 | `gh` or repository-less `git`; run from a daemon-owned empty directory with an explicit repository or URL. |
| REMOVE | 18 | Delete; the responsibility moves into the run, into workspace seeding, or into durable state. |
| HARDEN | 2 | File reads of run-written state; route through the untrusted-file reader. |

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

What local operators see after the flip: workspaces are full clones instead of linked
worktrees, so they cost disk (see open questions), and an agent's workspace has no `origin`
remote. Agents already publish only through the daemon, so a missing remote changes nothing
that works today. A daemon push no longer runs the repository's pre-push hook, so a repository
that relied on that hook as a gate for daemon pushes must enforce the same checks in CI.

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
   │  refs/quorum/import/<run>             │                            ▲
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
  The daemon then writes the config itself and treats any key it did not write as corruption:

  ```
  [core] repositoryformatversion, bare = true, logAllRefUpdates = false
  [gc] auto = 0
  [receive] denyCurrentBranch = refuse
  [uploadpack] allowAnySHA1InWant = false, allowReachableSHA1InWant = false
  ```

  `hooks/` is absent. `info/attributes`, `info/exclude`, and `objects/info/alternates` must be
  absent; startup verifies all of this before the first command.
- **Ref namespaces.**

  | Ref | Writer | Meaning |
  |---|---|---|
  | `refs/remotes/origin/<branch>` | hardened fetch | Upstream branches the daemon has fetched. |
  | `refs/quorum/pr/<n>` | hardened fetch | A fetched `refs/pull/<n>/head`. |
  | `refs/quorum/tasks/<id>/base` | seeding | The commit a task's first workspace was seeded from. |
  | `refs/quorum/tasks/<id>/head` | import | The last verified task commit. The only source of publication. |
  | `refs/quorum/import/<run>` | import | Quarantine during verification; never outlives the import. |
  | `refs/quorum-publication/task-<id>` | publication | Existing pin; moves here unchanged. |
  | `refs/quorum-recover/...` | recovery | Existing recover refs; move here unchanged. |
  | `refs/quorum/cleanup/...` | cleanup | Existing tombstones; move here unchanged. |

  The private repository has no `refs/heads/*`. Task branches exist only as names on the
  remote and in the workspace.
- **Serialization.** One manager mutex serializes every command on the private repository, as
  `WorktreeManager.lock` does today. Commands keep `run_git`'s timeout, output bound, and
  kill-and-reap.
- **Maintenance.** Automatic GC is off. The daemon runs `git gc --prune=<age>` under the lock
  at startup and after every 50 imports, with the local timeout. A rejected import prunes its
  own objects immediately (see "Import and verification").

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
| Inherited environment | cleared; then `PATH` from the daemon, `HOME=<state dir>/empty`, `LC_ALL=C`, `TZ=UTC` | inherited, minus every `GIT_*` variable | as the local profile |
| `GIT_CONFIG_NOSYSTEM` | `1` | unset | `1` |
| `GIT_CONFIG_GLOBAL` | `/dev/null` | unset | `/dev/null` |
| `GIT_ATTR_NOSYSTEM` | `1` | `1` | `1` |
| `GIT_ATTR_SOURCE` | empty tree `4b825dc…` (SHA-1) or its SHA-256 equivalent | same | same |
| `GIT_ALLOW_PROTOCOL` | `file` | the scheme of `remote_url` | the scheme of `remote_url` |
| `GIT_CEILING_DIRECTORIES` | `<state dir>` | same | same |
| `GIT_TERMINAL_PROMPT`, `GIT_OPTIONAL_LOCKS` | `0`, `0` | same | same |
| `GIT_NO_REPLACE_OBJECTS` | `1` | same | same |
| `GIT_EDITOR`, `GIT_SEQUENCE_EDITOR`, `GIT_PAGER` | `false`, `false`, `cat` | same | same |
| `GIT_ASKPASS`, `SSH_ASKPASS`, `GIT_SSH_COMMAND`, `GIT_SSH` | unset | inherited | unset |

`inherit` keeps the operator's global and system git configuration for transport commands
only, so a local operator's credential helper and SSH setup keep working. That configuration
belongs to the operator, not to a run. Under the local permissive profile it is still reachable
by the agent, as everything on that machine is. `hermetic` reads no configuration outside the
private repository. It authenticates only through the separate credential-supply contract and
is accepted now for `file://` remotes and tests. A deployment with an external launcher must
use `hermetic`.

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
-c commit.gpgSign=false -c tag.gpgSign=false -c push.gpgSign=false
-c diff.external= -c core.pager=cat
-c credential.helper=          # local profile and hermetic transport only
```

Command-line `-c` values take precedence over every configuration file, so in `inherit` mode an
operator `core.hooksPath` or `core.fsmonitor` is still overridden. Diff-producing commands also
pass `--no-ext-diff --no-textconv --no-color`.

**What this rules out.** The daemon cannot run hooks (`core.hooksPath` points at an empty
directory, and repository and global hooks paths are overridden). It cannot run a configured
filter, textconv, external diff, merge driver, fsmonitor, editor, pager, or askpass. None is
defined in the private repository's own config, global and system config are ignored by the
local profile, and repository attributes are not read (`GIT_ATTR_SOURCE` is the empty tree).
Transports other than local files and the remote's scheme are refused before connection. With
git 2.53, the local profile refused `ext::` and `https` URLs ("transport not allowed") and still
fetched from a bundle file. Malformed objects are refused by `fsck` on every fetch.

**URL validation.** `remote_url` must be `https://`, `ssh://`, scp-like `user@host:path`, or
`file://`. Transport-helper syntax (`<name>::`), a leading `-`, control characters, and embedded
credentials are rejected at startup with exit 2. The URL is never read from any repository
config at run time.

**Git version.** Startup in clone mode requires git 2.40 or later (`GIT_ATTR_SOURCE`; 2.38 for
`merge-tree --write-tree`) and logs the detected version. Operators are expected to track
upstream git security releases. The floor checks functionality, not patch currency.

**Source-scan test.** The existing test `production_git_commands_use_the_shared_constructor`
(`quorum-core/src/git.rs:48`) is extended so that production code outside the
private-repository module and the local launcher's export step must not construct a git
command, use `-C`, or set `current_dir` for a `git` or `gh` process.

## Workspace lifecycle

### 1. Allocate

A task allocation keeps its existing durable fields (branch name, workspace path under
`worktree_base`). It adds the seeded base SHA and the last imported head SHA. The path binding
lives only in the database; Git has no worktree registration to reconcile.

### 2. Seed

The daemon builds every workspace in the staging directory and hands it over with one
`rename(2)` into `worktree_base`. Git therefore never runs in a directory that a run has ever
been able to write. Staging must be on the same filesystem as `worktree_base`; startup checks
the device IDs.

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
- Before the handoff, the daemon chmods the tree for the role. The launcher owns the mount mode
  (read-only for non-writing roles).

Seed variants:

| Run | Seeded `refs/heads/<branch>` | Extra refs |
|---|---|---|
| First worker run | the verified base tip; also written to `refs/quorum/tasks/<id>/base` | `origin/<base>` |
| Rework or retry | `refs/quorum/tasks/<id>/head`; the existing workspace is reused when present | refreshed `origin/<base>` |
| Reviewer, review-only, continuation reviewer | `refs/quorum/pr/<n>` at the verified PR head SHA | `origin/<base>` |
| Continuation worker, clean base merge | a daemon-made merge commit (see "Branch synchronization and continuation") | `origin/<base>` |
| Continuation worker or sync judgment, conflicting | the PR head or sync target tip; the run's instructions say which commit to merge | the commit to merge, as `refs/remotes/origin/<branch>` |
| Resume from a recover ref | the recover snapshot's parent commit; the snapshot tree is checked out in staging and the index is reset to the parent, reproducing uncommitted changes | `origin/<base>` |

A reused workspace (rework) is never touched by daemon git. When a run needs a different
starting point (a new continuation base, a recovery), the daemon seeds a new workspace and
retires the old directory (see "Cleanup").

### 3. Hand off to the run

The launcher seam receives, per run:

- the workspace path and its mode (read-write for writing roles, read-only otherwise);
- the session directory path (outside the workspace and per task);
- the run outbox path, `<state dir>/runs/<run_id>/outbox/`, which the daemon creates empty with
  mode `0700`;
- for writing roles, an export declaration: `{ branch, exclude: [sha…], file: "delivery.bundle" }`.
  `exclude` lists every commit the daemon seeded: the task base or last head, plus the
  `origin/<base>` tip it seeded.

The run's environment already carries `QUORUM_REPO` (`quorum/src/serve/agent.rs:1223`), so
the CLI inside a workspace never needs a remote to identify the repository.

### 4. Export (run-side)

When the agent process exits, for any reason, the launcher runs the export step inside the same
isolation as the run:

```
git -c core.hooksPath=/dev/null -c core.fsmonitor=false -c gc.auto=0 \
    -C <workspace> bundle create --version=2 <outbox>/delivery.bundle \
    refs/heads/<branch> --not <exclude…>
```

with `GIT_CONFIG_NOSYSTEM=1` and `GIT_CONFIG_GLOBAL=/dev/null`. The step may also write
`<outbox>/manifest.json`, `{ "dirty": bool }`, from `git status --porcelain`. The manifest is
informational only and is size-capped at 4 KiB.

The export step runs against the workspace by design. It is the run working on its own files.
Under the isolated runtime profile it executes in the sandbox, with no network and no
credentials. Under the local permissive profile the local launcher runs it as a separate process
group with a scrubbed environment: no `GH_TOKEN`, no `SSH_AUTH_SOCK`, no credential helper, no
orchestrator token (`strip_operator_secrets`, `quorum/src/serve/runner.rs:695`). That gives it
no authority the agent did not already have. The hardening flags are defense in depth for the
run, not a daemon guarantee.

A worker's lifecycle "done" signal is unchanged. Publication now waits until the run has exited
and the export step has finished. If a run stays alive after a terminal signal, the daemon ends
it after a bounded grace period.

### 5. Collect

The daemon opens `<outbox>/delivery.bundle` with the untrusted-file reader (see "Untrusted file
handling") and streams it into `<state dir>/imports/<run_id>.bundle`, created `O_EXCL` with
mode `0600`, up to `max_bundle_bytes`. Every later step uses only this private copy, so the run
cannot change the bytes between verification and import. A missing bundle maps to "no
delivery" (see "Failure modes").

### 6. Import and verify

See "Bundle verification rules". On success, `refs/quorum/tasks/<id>/head` moves to the
verified tip in one `update-ref --stdin` transaction, which also deletes the quarantine ref, and
the staged copy is removed.

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

A rework run reuses the workspace and exports with `exclude = [head, origin/<base> tip]`. If the
agent rewrote already-imported history, the new bundle does not descend from the last head and
import rejects it. That is the same fast-forward-only rule `push_to_pr_head` enforces today
(`worktree.rs:1644-1666`).

### 9. Cleanup

The daemon deletes workspaces, staging directories, outboxes, and session directories with
symlink-safe recursive removal, never with git. `std::fs::remove_dir_all` does not follow
symlinks on Unix (std since 1.58.1), and a wrapper refuses a root that is itself a symlink or
not a directory. A superseded workspace (re-seeded, or found unreferenced at startup) is renamed
to `worktree_base/.retired/<name>-<unix time>` and deleted after
`superseded_workspace_retention_secs`. Uncommitted work is never destroyed immediately, and the
daemon never needs `git status` to find out whether there is any. A terminal task's workspace is
retired the same way.

## Bundle verification rules

Steps run in order. The first failure rejects the bundle with a stable rule code, deletes the
quarantine ref, and prunes.

**A. File (untrusted-file reader)**

1. Opened without following any symlink in any path component under the outbox.
2. A regular file with `st_nlink == 1`.
3. At most `max_bundle_bytes` (default 256 MiB), checked by `fstat` and again while streaming.

Rule codes: `unsafe-file`, `oversized-bundle`.

**B. Header (Rust parser, before any git process reads the file)**

4. The first line is `# v2 git bundle` or `# v3 git bundle`. The header ends at the first empty
   line and is at most 64 KiB.
5. In v3, the only allowed capability is `@object-format=<the private repository's format>`. Any
   `@filter=` or unknown capability is rejected. v2 implies SHA-1.
6. **Exactly one ref line**, and its name is exactly `refs/heads/<task branch>`. A second ref,
   `HEAD`, any other branch, any tag, or a non-`refs/heads/` name is rejected. The experiment
   with `bundle create --all` produced `HEAD` and every branch, which this rule refuses.
7. **At least one prerequisite.** Every prerequisite SHA is well-formed. A bundle with no
   prerequisites carries full history and is rejected.

Rule codes: `malformed-header`, `unsupported-bundle`, `unexpected-refs`, `no-prerequisite`.

**C. Objects (local profile, in the private repository)**

8. `git bundle verify <copy>` succeeds. Every prerequisite is present.
9. Every prerequisite is an ancestor of, or equal to, one of the export declaration's `exclude`
   SHAs (`merge-base --is-ancestor`). A prerequisite the daemon never seeded is rejected even if
   the object happens to exist.
10. `git fetch --no-tags --no-write-fetch-head --no-recurse-submodules --end-of-options <copy>
    refs/heads/<branch>:refs/quorum/import/<run>` succeeds with `fetch.fsckObjects=true`.

Rule codes: `missing-prerequisite`, `foreign-prerequisite`, `fsck-failed`.

**D. History**

11. The imported tip descends from the run's starting commit (`base` for a first run, `head` for
    a rework). For a clean continuation, the starting commit is the daemon-made merge.
12. The tip differs from the starting commit. Equal means no new commits.
13. The range introduces no new root commit: `rev-list --max-parents=0 <tip> --not <exclude…>`
    is empty. This refuses unrelated history smuggled in through a merge.
14. `rev-list --count <tip> --not <exclude…>` is at most `max_commits` (default 2,000).
15. Every blob in `rev-list --objects <tip> --not <exclude…>`, sized with
    `cat-file --batch-check`, is at most `max_blob_bytes` (default 100 MiB, GitHub's hard file
    limit). The total is at most `4 × max_bundle_bytes`. Both bounds are checked while
    streaming, so a highly compressible blob cannot exhaust daemon memory or a later workspace's
    disk.

Rule codes: `unrelated-history`, `no-new-commits`, `new-root`, `too-many-commits`,
`oversized-blob`.

On rejection, the daemon deletes `refs/quorum/import/<run>` and runs
`git prune --expire=now` under the manager lock, bounded by the local timeout, so rejected
objects do not accumulate. Imports are serialized, so pruning cannot race another import's
quarantine.

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
- Parsed content (manifests, provider config) is size-capped before parsing, must be UTF-8 with
  no NUL, and is treated as data.
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

`git check-ref-format` in `validate_target_branch` (`quorum-core/src/tasks.rs:1571`) runs from
the empty directory with the local profile.

## Review and planning materials

The daemon produces read-only inputs for non-writing roles from the private repository. They are
written into a per-run inputs directory beside the outbox, and the launcher mounts it read-only.

| File | Command (local profile) | Bound |
|---|---|---|
| `diff.patch` | `diff --no-ext-diff --no-textconv --no-color -M <base>...<head>` | `max_review_diff_bytes` (default 4 MiB); on overflow, per-file patches in path order until the bound, with a `TRUNCATED` trailer listing omitted paths |
| `diffstat.txt` | `diff --stat=200 --no-color <base>...<head>` | 256 KiB |
| `files.json` | `diff --name-status -z` | 1 MiB |
| `log.json` | `log --format=%H%x00%P%x00%an%x00%aI%x00%s -z <base>..<head>` | `max_commits` entries |
| `remerge.patch` | `show --remerge-diff <merge>` for each daemon-made merge in range | 1 MiB |
| `tree/` | `archive --format=tar <head>` extracted into the inputs directory, as the planner view does now (`quorum/src/serve/mod.rs:6476-6560`) | existing archive bound |

The internal API is
`PrivateRepo::review_materials(base, head, limits) -> ReviewMaterials { dir, base_sha, head_sha, truncated }`.
The read-only role prompt work consumes it. Until that lands, prompts are unchanged, and in clone
mode a read-only run's workspace (a clone at the PR head with `origin/<base>`) still supports the
commands the prompts name today (`quorum/src/serve/reviewer.rs:477-492`).

The planner's frozen view is built from `refs/remotes/origin/<base>` at the frozen SHA in the
private repository. The daemon no longer reads `repo_dir`'s `HEAD`
(`quorum/src/serve/mod.rs:6493,6752`).

## Branch synchronization and continuation

The daemon computes merges in the private repository and never runs `git merge` in a work tree.

```
git --git-dir=<private_repo> merge-tree --write-tree --name-only --no-messages <ours> <theirs>
```

- **Clean (exit 0).** The daemon writes the merge commit with `commit-tree <tree> -p <ours>
  -p <theirs>` and its existing daemon identity. For branch sync it pushes that commit through
  the hardened push. For continuation it seeds the worker's workspace at it. `--ff` semantics
  are kept: when `<ours>` is an ancestor of `<theirs>`, the result is `<theirs>`.
- **Conflict (exit 1).** The output lists the conflicted paths. These replace
  `sync_conflicted_files` (`worktree.rs:1348`). The daemon seeds a judgment or continuation
  workspace at `<ours>` with `<theirs>` present, and the run's instructions tell the agent to
  run the merge itself. The agent's `git merge` runs in its own sandbox, where hooks and drivers
  are its own business. The existing contract that the daemon prepares `MERGE_HEAD` changes
  accordingly, and the prompt text at `quorum/src/serve/mod.rs:3127-3130` changes with it.
- **Any other exit.** A loud failure, as today.

With `GIT_ATTR_SOURCE` set to the empty tree, `merge-tree` ignores repository `merge=`
attributes, so a repository relying on `merge=union` may see a conflict the daemon used to
resolve. The agent then resolves it in the sandbox. This is accepted.

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
- **Reviewer verdict recovery** uses the durable head SHA the reviewer workspace was seeded at,
  recorded in the journal at provisioning, instead of `git rev-parse HEAD` in the reviewer's
  worktree (`quorum/src/serve/mod.rs:5528`).
- **Publication pins and cleanup tombstones** keep their CAS semantics in the private
  repository.
- **Staged bundle copies** found at startup are re-imported if the run's publication is still
  pending, and deleted otherwise. Import is idempotent because the quarantine ref name is
  derived from the run ID.

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
superseded_workspace_retention_secs = 604800
```

`worktree_base` keeps its name and now holds workspaces. `repo_dir` remains for the operator's
checkout: self-update, and one-time URL derivation and migration. In clone mode no managed run
touches it, and the daemon runs nothing in it except `git config --file` and the one-time
migration fetch.

## Failure modes

| Failure | Detection | Lifecycle mapping | Routing disposition |
|---|---|---|---|
| No bundle after a worker's done signal (no commits, export failed) | outbox empty after run exit | `AgentFailed { reason: "no-delivery: <exporter status>" }` through the existing worker failure and retry budget | `NonFailover` |
| Bundle rejected (A to D) | verification rule | `AgentFailed { reason: "bundle-rejected:<rule>" }`; nothing pushed; event records the rule and bounded detail | `NonFailover` |
| Same rejection rule on consecutive runs of a task, up to the existing rework or retry cap | durable counter | park with the rule as the park reason (`tasks::park`), `PublicationFailureKind::Other` | n/a |
| Seeded base or head missing from the private repository | prerequisite check against the declaration | park; a daemon-side inconsistency, not the run's fault | n/a |
| Import infrastructure failure (spawn, timeout, disk full, lock) | process result | staged copy kept; retried on the next tick, at most 3 times; then park | n/a |
| Push rejected (lease, non-fast-forward, remote policy) | `push --porcelain` | unchanged: `push_rejection` park path (`worktree.rs:160`), without the local hook section | n/a |
| Seeding failure (private fetch, disk, rename) | process or IO result | existing provisioning-failure path: release the claim, retry with backoff | n/a |
| Operator remote unreachable on fetch | transport result | existing fetch-failure handling (dependency defer, provision retry) | n/a |
| Invalid `remote_url`, private repository config drift, non-empty `empty-hooks`, old git | startup check | refuse to start, exit 2 for configuration and exit 3 for state | n/a |
| Run-written manifest malformed or oversized | untrusted reader or parser | ignored and logged; never affects lifecycle | n/a |

`NonFailover` follows the existing meaning: an execution-boundary failure that must not move the
run to another provider route (`quorum-core/src/routing_attempts.rs:20-31`).

## Launcher seam

The launcher interface is separate work. This design needs only two things to cross it, both data:

1. **Mounts.** The workspace path and mode, the session directory, the outbox, and the inputs
   directory, all created by the daemon.
2. **Export step.** The export declaration from step 3. The launcher runs the fixed export
   command inside the run's isolation after the agent process ends, including on timeout or
   kill, and reports the exporter's exit status and up to 2 KiB of stderr.

The local launcher is today's provider adapters (`quorum/src/serve/agent.rs`,
`codex_agent.rs`, `grok_agent.rs`, via `runner::gated_command`) plus the export step. The
adapters keep `current_dir(workspace)`. They must stop reading workspace files except through
`untrusted_fs` (Appendix A, HARDEN rows), and the fallback route's executable must resolve
against the daemon's `PATH` or an absolute path, never the workspace (REMOVE row R18).

## Migration for local operators

On the first start with `workspace_mode = "clone"`, under the existing startup recovery (no runs
are alive):

1. Create and verify the private repository and derive or validate `remote_url`.
2. Import legacy state from `repo_dir`. This is one hardened `file://` fetch with fsck, of each
   non-terminal task's `refs/heads/<branch>` into `refs/quorum/tasks/<id>/head`, and of
   `refs/quorum-publication/*`, `refs/quorum-recover/*`, and `refs/quorum/cleanup/*` into the
   same names. It spawns `git-upload-pack` against `repo_dir`. That is the one operation that
   still reads `repo_dir`'s possibly agent-written config. It runs once, only under the local
   permissive profile, where the legacy trust already applied. Git honors upload-pack's command
   hook (`uploadpack.packObjectsHook`) only from protected configuration, never from repository
   config.
3. Leave every legacy linked worktree on disk, untouched and unregistered from Quorum. Log one
   event per task with its path, and state that uncommitted changes were not carried over and
   remain there for the operator. Quorum never deletes them.
4. A branch synchronization with a kept conflict worktree restarts from its pinned tips through
   `merge-tree`.
5. Record a durable one-row marker in the same transaction as the first clone-mode allocation
   (additive, forward-only migration), so step 2 runs exactly once. Switching back to
   `linked-worktree` after the marker exists is refused, with exit 2.

Operators who pushed through the daemon and relied on a repository pre-push hook must move those
checks to CI. For this repository, the hook's base-branch and formatting checks
(`.githooks/pre-push`) must be required CI jobs before the default flips.

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

- Git itself parses hostile objects during fetch, fsck, diff, and archive. Mitigations are fsck,
  size bounds, timeouts, and a current git. A git parser vulnerability remains a daemon risk.
- `inherit` transport configuration is operator-owned but, under local permissive, writable by
  agents like everything else on that machine.
- Hostile content does reach the remote. That is the point of a pull request. It is verified by
  CI and the developer, not by the daemon.

## Open questions

1. **Workspace disk cost.** Full `--no-local` clones per task. Should a shallow seed
   (`fetch --depth`) be offered? Bundle prerequisites still work, but agents lose history.
2. **Snapshotting uncommitted work.** Is retaining superseded workspaces for seven days enough,
   or should the export step also produce a snapshot bundle, imported only into
   `refs/quorum-recover/*` and never published?
3. **Default limits.** 256 MiB bundle, 100 MiB blob, 2,000 commits, 4 MiB review diff. Are these
   acceptable defaults?
4. **Minimum git version.** Should the floor fail on versions without the latest upstream
   security fixes, or only warn?
5. **This repository's pre-push gate.** Moving base-branch and `fmt` checks into required CI
   jobs is a `main` hotfix-class change. Who schedules it relative to the default flip?
6. **Conflict handling.** Is it acceptable that `merge=union` and similar attribute-driven merge
   behaviors stop applying to daemon-side merges?

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
| S7 | `quorum/src/serve/mod.rs:6528` | `tar -xf` of a daemon-produced `git archive` stream into a temp dir | A tree cannot hold both a symlink and a path through it. Removal is symlink-safe. Source moves (M25). |
| S8 | `quorum/src/serve/session_log.rs:597-910`; `quorum/src/web.rs:838-1096` | Session stream and transcript files | Written by Quorum from agent pipes; content is data. |
| S9 | `quorum/src/serve/cleanup.rs:523-532`; `recovery.rs:569-575` | `canonicalize` plus `symlink_metadata` before removal | Already symlink-checked. Superseded by symlink-safe removal. |

### MOVE to the private repository (27)

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
| M25 | `quorum/src/serve/mod.rs:6493,6516` | Planner view: `rev-parse HEAD` and `git archive` in `repo_dir` | Reads the operator checkout's `HEAD`. Becomes the frozen SHA in the private repository. |
| M26 | `quorum/src/serve/mod.rs:6752` | `repository_head_sha` (frozen-base capture from `repo_dir` `HEAD`) | As M25. |
| M27 | `quorum/src/serve/mod.rs:24585-24625` | Sync-branch cleanup: `ls-remote origin`, `push origin --delete` in `repo_dir` | |

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

### REMOVE (18)

| # | Site | Operation | Replacement |
|---|---|---|---|
| R1 | `worktree.rs:598-627` (`604`) | Push URL from `git remote get-url origin` in the worktree | `remote_url` |
| R2 | `worktree.rs:652-680` (`657`) | `worktree list` branch collision | DB allocation |
| R3 | `worktree.rs:682-727` (`703,714`) | `git worktree add` (post-checkout hook; shared gitdir) | Staging seed and rename |
| R4 | `worktree.rs:762,767,1411,1416` | `branch -D` and `worktree add` in review provisioning | Seed from `refs/quorum/pr/<n>` |
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

### HARDEN (2)

| # | Site | Read | Problem |
|---|---|---|---|
| H1 | `quorum/src/serve/grok_agent.rs:558-612` | Project MCP sources (`.grok/config.toml`, `.mcp.json`, `.cursor/mcp.json`) in the worktree **and each parent up to a `.git`** | `read_to_string` follows symlinks and is unbounded. |
| H2 | `quorum/src/serve/grok_agent.rs:820-880` | Provider auth state copied back from the agent-writable invocation home into the operator's home | `fs::metadata` follows symlinks; `fs::read` is unbounded. |

### Prompt text that names git or `gh` for read-only roles

These are inputs to the read-only role prompt work and are not counted above:
`quorum/src/serve/reviewer.rs:218,477-492` and `quorum/src/serve/mod.rs:3127-3130`.
