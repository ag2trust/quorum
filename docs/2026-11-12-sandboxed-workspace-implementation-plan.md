# Sandboxed Workspaces — Implementation Plan

**Date:** 2026-11-12
**Status:** Proposed
**Spec:** `docs/2026-11-12-sandboxed-workspace-technical-spec.md`
**Implementation base:** `origin/develop` at `b090a1a`

The plan is one prerequisite and eighteen slices. Each slice is one PR against `develop` with
its own tests, and leaves `develop` green.

- **Prerequisite and slices 1–6** harden today's behavior in both modes. That includes the worst
  linked-mode paths, so they do not wait for the flip.
- **Slices 7–15** build the clone model beside the linked-worktree model, which stays the
  default.
- **Slice 16** migrates.
- **Slice 17** flips the default.
- **Slice 18** deletes the legacy path one release later.

Until slice 17, `workspace_mode = "clone"` starts only with the hidden test-only flag
`--incomplete-clone-mode`, following the existing hidden-seam precedent (`--merge-cmd`).
Operators cannot enable a partial clone mode.

## Shared adversarial fixture

Slice 1 adds `quorum/tests/support/hostile_git.rs`. Later slices reuse it.

- **Tripwire.** Every planted command appends its name to a file under a per-test `TRIPWIRE`
  directory. Each hostile test ends by asserting either of these:
  - for daemon-side operations, the directory is empty;
  - for run-side operations, which are expected to fire, it lists what fired.
- **`plant_workspace(dir)`.**
  - Hooks: `post-checkout`, `pre-push`, `reference-transaction`, `post-merge`,
    `pre-merge-commit`, `pre-commit`, `post-rewrite`, `post-index-change`, `pre-auto-gc`.
  - `.git/config` keys:
    - `core.hooksPath`, `core.fsmonitor`, `core.sshCommand`, `core.pager`, `core.editor`,
      `core.askPass`, `core.alternateRefsCommand`
    - `filter.t.clean`, `filter.t.smudge`, `filter.t.process`
    - `diff.t.textconv`, `diff.t.command`, `diff.external`
    - `merge.t.driver`
    - `gpg.program`, `gpg.ssh.program`, `gpg.x509.program`
    - `credential.helper`
    - `remote.origin.url`, `remote.origin.pushurl`, `remote.origin.uploadpack`,
      `remote.origin.receivepack`
    - `uploadpack.hideRefs`
    - `include.path` pointing at a second planted file that sets `core.hooksPath`
    - `includeIf.gitdir:…`
  - Other planted state:
    - `.git/info/attributes`
    - a `.gitattributes` in the tree with `* filter=t diff=t merge=t`, plus `export-subst`,
      `export-ignore`, `eol=crlf`, and `conflict-marker-size=50000000` entries
    - a commit with a hand-made `gpgsig` header over a file containing `$Format:%G?$`
    - a `gpg` tripwire first on the test `PATH`
    - `.githooks/pre-push` with `core.hooksPath=.githooks`
    - an executable `preflight.sh` tripwire
    - a symlinked repository instruction file and provider project-config file pointing at a
      sentinel secret
    - an executable named like each provider binary at the workspace root
- **`plant_operator(home)`.** The same keys in a global `.gitconfig` under a test `HOME`, plus
  `url.<decoy>.insteadOf` and `url.<decoy>.pushInsteadOf` rewriting the remote URL.
- **`plant_repo_dir(dir)`.** The same keys in the operator checkout's config, `hooks/`,
  `info/attributes`, and each `worktrees/*/config.worktree`.
- **`plant_parent_env()`.** The daemon's own environment sets:
  - `GIT_DIR`, `GIT_WORK_TREE`, `GIT_INDEX_FILE`;
  - `GIT_OBJECT_DIRECTORY`, `GIT_ALTERNATE_OBJECT_DIRECTORIES`;
  - `GIT_EXEC_PATH` (a directory holding a tripwire `git-*`);
  - `GIT_TEMPLATE_DIR`;
  - `GIT_CONFIG_PARAMETERS`, and `GIT_CONFIG_COUNT`/`GIT_CONFIG_KEY_0`/`GIT_CONFIG_VALUE_0`
    (setting `core.hooksPath`);
  - `GH_TOKEN`.
- **`hostile_bundle` builders.** Hand-built bundles: header text plus a pack written with
  `pack-objects` from an attacker repository using `hash-object --literally` and
  `mktree --missing`. Cases:
  - foreign blob;
  - foreign merge parent;
  - foreign `REF_DELTA` base;
  - delta bomb, a small delta declaring a multi-GiB result;
  - zlib stream longer than its declared size;
  - extra refs, a tag, `HEAD`;
  - no prerequisite;
  - an unseeded prerequisite;
  - v3 `@filter`;
  - an object-format mismatch.

**CI.** A job runs every hostile suite on the git floor (2.43) as well as on the runner's
current git. The suites do not exist until slice 1 creates this fixture, so the job is added
right after slice 1 merges, as a separate `main` hotfix (workflow files are hotfix-class) synced
into `develop`.

## Prerequisite. CI gate for this repository (`main` hotfix, synced to `develop`)

`.githooks/pre-push` runs two checks. Both must be required CI checks before daemon pushes stop
running the hook:

- **Base-branch check.** No CI job runs it today. The `main` hotfix that adds one is being
  prepared as a separate PR (owner, 2026-11-12).
- **`cargo fmt --check`.** CI already runs `cargo fmt --all --check` on every pull request
  (`.github/workflows/ci.yml:19-27`). What remains is to confirm that `fmt` is a required check
  in branch protection for `main` and `develop`. That is a repository setting, not a PR.

Slice 2 removes the hook from daemon pushes and must not land before both are in place. Later
clone-mode push work (slice 10) comes after slice 2 anyway. Slices 1 and 3–9 do not change
daemon pushes and are not blocked, although the owner's order keeps this prerequisite first.

Verification: a PR branched from a feature branch fails the new CI job, a PR with `fmt` drift
fails `fmt`, and branch protection lists both as required.

## 1. Hardened git constructor

Add `quorum_core::git::{LocalGit, TransportGit}`, the spec's two profiles:

- cleared environment and allowlists, including the `inherit` allowlist;
- the `-c` list;
- `--git-dir` only, run from `<state dir>/empty`;
- `--end-of-options`;
- SHA and ref validators;
- startup checks for the empty directories and the git floor, applied wherever the constructors
  run;
- the `inherit` URL-rewrite check;
- `-c gpg.program=false -c gpg.ssh.program=false -c gpg.x509.program=false` in the `-c` list;
- `archive` always with `--worktree-attributes`;
- `AttrTree::sanitized(ours)`: the sanitized attribute tree from the spec, which keeps only
  built-in `merge` attributes and is bounded to 1 MiB of input. It is used as `GIT_ATTR_SOURCE`
  only by `merge-tree` (slice 14) and `show --remerge-diff` (`review_materials`, slice 13).
  Every other local-profile command uses the empty tree.

Additive; nothing calls it yet.

Tests:

- **Unit:** exact argv and env for both profiles. `inherit` passes only its allowlist, never
  `GH_TOKEN`, `GITHUB_TOKEN`, `QUORUM_*`, `GIT_*`, `GIT_SSH_COMMAND`, or `GIT_ASKPASS`.
  `hermetic` clears everything.
- **Planted environment:** `plant_operator` plus `plant_parent_env`. With the local profile, all
  of these succeed with the tripwire empty: `rev-parse`, `update-ref`, a `file://` fetch,
  `diff`, `archive`, `index-pack --strict`. In particular:
  - a `reference-transaction` hook in the global `core.hooksPath` does not fire;
  - no inherited `GIT_*` variable is honored;
  - the tripwire `git-*` in `GIT_EXEC_PATH` never runs;
  - `GH_TOKEN` is absent from the child's environment, as a recorded `env` dump shows.
- **Protocol:** the local profile rejects `ext::sh -c …`, `https://`, and `ssh://`. The transport
  profile for an `https` remote rejects `file://` and `ext::`.
- **URL rewriting:** `plant_operator`'s `insteadOf` makes `--get-url` differ, so startup fails
  with exit 2. `pushInsteadOf` is detected by the config check.
- **`--end-of-options`:** a ref argument `--upload-pack=…` is passed as data, not as an option.
- **Attribute source:** the exact env of a `merge-tree` call and of a `show --remerge-diff`
  call has `GIT_ATTR_SOURCE` set to the sanitized tree, and every other command's has the empty
  tree.
- **Sanitized attribute tree:** from a fixture with root and nested `.gitattributes`, the tree
  keeps `merge=union`, `merge=binary`, `merge=text`, `-merge`, `!merge`, and `merge`, and turns
  `binary` into `-merge`, each at its original path. It drops `conflict-marker-size`,
  `merge=evil`, `filter`, `diff`, `eol`, `export-subst`, macro definitions, and comments.
  Input over 1 MiB yields the empty tree and an event.
- **Archive is verbatim:** `archive` of the planted commit, with the `gpg` tripwire on `PATH`
  and `gpg.program` set in the private repository's config by the test, leaves the tripwire
  empty. The `export-ignore` file is present, `$Format:%H$` and `$Format:%G?$` stay literal, and
  the `eol=crlf` file keeps LF.
- **Version gate:** a fake `git` reporting `2.42` makes clone-mode startup fail with exit 2 and
  an error naming `2.42` and the floor `2.43`. In linked mode it logs a warning naming `2.42`
  and startup continues. A fake `git` reporting `2.43.0` passes in both modes.

## 2. Early linked-mode hardening

After the prerequisite, route the shared `git_command` through the hardened constructor's
command-line set in both modes:

- an empty `core.hooksPath`, the protocol allowlist, signature programs set to `false`, and
  `GIT_ATTR_SOURCE` for commands that do not check out files;
- the planner view's `archive` already isolated (audit row M25, fixed ahead of this slice:
  a daemon-owned git directory over `repo_dir`'s object store, no repository config or
  attributes, signature programs set to `false`);
- the container image moved to a current git (`Dockerfile:32` pins 2.39, which predates
  `GIT_ATTR_SOURCE`), with the image smoke test asserting that its git is at least the floor;
- a push URL from `remote_url` (or derived once at startup), replacing R1, with the rewrite
  check;
- removal of `QUORUM_CONTINUATION_BASE_BRANCH` and the `hook-stdout` diagnostic section.

Teardown and startup GC stop snapshotting worktrees (R13's `git add -A`) and retire worktree
directories instead, then `git worktree prune` in `repo_dir`. LFS detection parks with
`lfs-unsupported` before any push.

Tests:

- **Push from a hostile worktree (linked mode):**
  - `plant_workspace` in the worktree, plus `core.hooksPath=.githooks` and a `preflight.sh`
    tripwire in the tree, plus `plant_repo_dir`;
  - the daemon push succeeds to the configured URL;
  - the decoy URL in the worktree's `remote.origin.url` is untouched;
  - the tripwire has no `pre-push`, `reference-transaction`, or `preflight.sh` entry.
- **Startup GC:** GC of a worktree with planted `.gitattributes`, `filter.t.clean`, and
  `info/attributes` fires no filter. The directory is retired, not deleted.
- **Planner archive (linked mode):** with the planted `gpgsig` commit as `repo_dir`'s `HEAD`,
  `gpg.program` planted in `repo_dir`'s config, and the `gpg` tripwire on `PATH`, building the
  planner view leaves the tripwire empty, and `$Format:%G?$` stays literal in the view.
- **LFS:** each of these parks the publication with `lfs-unsupported`, and nothing is pushed:
  - a repository with `filter=lfs` in `.gitattributes`;
  - a delivered tree with an LFS pointer file and no `filter=lfs` attribute;
  - a pointer file with the legacy `hawser` header;
  - a tree whose listing exceeds the detection bound (fails closed).

  None of these parks: a file that has the pointer header line but not as its first line, a
  1 KiB or larger file that starts with it, or `filter=lfs` inside a `.gitattributes` comment.
  An unchanged base is not rescanned: a second publication on the same base runs no
  `cat-file` for base blobs.
- **Existing tests:** all linked-mode publication and cleanup tests stay green.

## 3. Untrusted-file reader and symlink-safe removal

Add `quorum/src/untrusted_fs.rs`:

- `open_regular_beneath(root_fd, rel, cap)`, component-wise `openat` with `O_NOFOLLOW`, or
  `openat2` on Linux;
- `copy_capped_to_private(…)`, which writes a temp file `O_EXCL` with mode `0600`, `fsync`s it,
  and renames it;
- `remove_tree(root)`, which refuses a symlink root, restores owner write bits on real
  directories through no-follow descriptors, skips symlinks, and wraps `remove_dir_all`.

Adopt it for HARDEN rows H1, H2, and H3. Bound H1's discovery walk to the workspace root.

Tests:

- **Reader:** each of these is refused:
  - final symlink, intermediate symlinked directory, `..` component;
  - FIFO, which must return promptly without blocking;
  - hardlink to an outside file (`st_nlink == 2`);
  - sparse file over the cap;
  - file growing past the cap during the copy;
  - directory where a file is expected.
- **Removal:**
  - a tree with a symlink to an outside sentinel leaves the sentinel intact;
  - a tree whose directories are `0500`, removed by the run's own `chmod`, is removed fully;
  - a symlink inside it pointing at a sentinel file keeps the sentinel's mode unchanged;
  - a symlink root is refused.
- **H1:** a provider project-config file symlinked to a sentinel is refused, as is an
  oversized one. With the workspace's `.git` deleted and a planted config in the parent
  directory, the walk never reads the parent.
- **H2:** an invocation-home auth file replaced by a symlink to a sentinel secret does not copy
  the sentinel into the durable home.
- **H3:** a symlinked repository instruction file pointing at a sentinel secret never reaches the
  restricted run's arguments or prompt. A regular one is passed as bytes.

## 4. `gh` and repository-less `git` from an empty directory

Add one `GhCommand` constructor. It runs from `<state dir>/empty` with mandatory `--repo`, and
sets `GH_REPO`, `GIT_DIR=<empty>/.none`, `GIT_CEILING_DIRECTORIES`, and the prompt and update
switches. Port EMPTY-DIR rows E1–E3 and E5–E10. Port E4 and E11 to the local or transport
profile. `quorum serve` stops calling `gh repo view` in `repo_dir`. This applies in both modes.

Tests:

- **Fake `gh`:** a fake `gh` on `PATH` records `cwd`, argv, and env. For every daemon `gh` call
  site, driven by existing integration tests:
  - `cwd` is the empty directory;
  - `--repo` or `GH_REPO` is present;
  - `GIT_DIR` points at the nonexistent path.
- **Decoy remote:** with `repo_dir` and the daemon's cwd naming a decoy repository through
  `remote.origin.url`, every call still targets the configured repository. `gh api` calls carry
  `GH_REPO`.
- **Real `gh` (owner-run contract test, following the guide's real-binary rule):** each
  subcommand Quorum uses runs against a scratch GitHub repository with the empty-directory
  environment and `GIT_DIR=…/.none`. The subcommands are `pr create`, `pr view`, `pr list`,
  `pr close`, `pr checks`, `pr comment`, `pr merge`, `pr review`, `repo view`, and `api`. Each
  must succeed, and none may report a git discovery error.
- **E11:** `validate_target_branch` gives the same accept and reject results as today, spawned
  from the empty directory.

## 5. Provider executable and provider-home resolution

REMOVE rows R18 and R19:

- bare names resolve only through absolute `PATH` entries;
- a configured executable path must be absolute;
- a provider-home path must be absolute.

Tests:

- `PATH=.:<bin>` and `PATH=::<bin>`, with a planted executable at the workspace root, resolve to
  `<bin>`.
- A relative `agent_bin` fails the fallback launch with `NonFailover`.
- A relative provider-home setting is refused.
- In all three cases the tripwire is empty.

## 6. Planner deliverable-path classification

Move M28. In clone mode `classify_writable_deliverable_path_blocking` walks the frozen tree in
the private repository with `ls-tree`, refusing symlink (`120000`) and gitlink (`160000`)
components. Linked mode keeps the working-tree check. This slice is gated on the private
repository from slice 7, so it lands after slice 7. It is listed here because it is a narrow
audit fix.

Tests:

- The frozen tree has `docs` as a symlink to `/etc`, so the deliverable `docs/new.md` is
  refused.
- A planted symlink in `repo_dir`'s working tree has no effect in clone mode.
- The existing classification tests stay green in linked mode.

## 7. Configuration and the private repository

Add these keys to the parser:

- `workspace_mode`, `private_repo`, `remote_url`, `git_transport_config`;
- `[workspace_limits]`, including `max_objects`, `max_inflated_bytes`, and `max_export_bytes`;
- `git_fsck_ignore`.

Add `PrivateRepo`, which owns its manager mutex, and its operations:

- create, with the config allowlist that accepts git-written platform keys;
- verify at startup (hooks, `info/attributes`, `info/grafts`, `shallow`, both alternates files,
  overlap, staging device);
- URL validation and derivation;
- `fetch_branch` and `fetch_pr_head` with fsck and `git_fsck_ignore`;
- `resolve` and `is_ancestor`.

Tests:

- **URL validation.** Accepted: `https`, `ssh`, scp-like, `file`. Rejected with exit 2:
  `ext::`, `fd::`, `-oProxyCommand=…`, `user:pass@`, CR/LF, empty.
- **Derivation:** reads only `<repo_dir>/.git/config` and ignores a planted `include.path` there.
  A `.git` file instead of a directory requires an explicit URL.
- **Tampered state:** startup refuses each tampered private-repository state with exit 3:
  - an unknown config key;
  - a hook file;
  - `info/attributes`, `info/grafts`, `shallow`, `objects/info/alternates`, or
    `objects/info/http-alternates`;
  - overlap, or staging on another device.

  Platform keys written by `git init` (`core.ignorecase`, `core.precomposeunicode`,
  `extensions.objectformat`) are accepted.
- **fsck:** `fetch_pr_head` of a source containing an fsck-invalid tree fails, and no ref is
  created. The same history fetched with its message ID in `git_fsck_ignore` succeeds.
- **Planted operator and checkout:** under `plant_repo_dir` and `plant_operator`, `fetch_branch`
  leaves the tripwire empty under both `inherit` and `hermetic`.

## 8. Pack pre-scan, quarantined indexing, and verified import

Add `bundle::parse_header` and `pack::prescan`. Both are pure Rust; `prescan` adds a `flate2`
dependency with the pure-Rust backend. Add
`PrivateRepo::import_bundle(staged_copy, Expectation { task_id, run_id, branch, starts, exclude, limits })`,
implementing spec rules 4–16:

- quarantine `index-pack --strict --fix-thin`;
- provenance with the single `bad-prerequisite` code;
- external-delta-base and object-membership checks;
- history rules, including descent from every start;
- resource limits and classification;
- ordered pack promotion and startup cleanup of leftover quarantines.

Tests. Each rejection asserts:

- the exact rule code;
- no `refs/quorum/tasks/*` ref moved;
- no quarantine directory remains;
- the private repository's object set is unchanged (`cat-file --batch-all-objects --batch-check`
  before and after);
- the staged copy is deleted.

Cases:

- **Foreign blob:** a commit whose tree names a blob present only under another task's head.
  Rule: `foreign-object`. The blob is not readable through any new ref.
- **Foreign merge parent:** a merge whose second parent is another task's unpublished commit.
  Rule: `foreign-object`. Ancestry from the start holds and no new root is introduced, so only
  membership catches it.
- **Foreign `REF_DELTA` base:** a delta against a blob outside every prerequisite tree. Rule:
  `foreign-delta-base`.
- **Legitimate thin bundle:** a bundle from the fixed export command whose pack deltas against
  the prerequisite tree imports successfully, with the appended base excluded from "received".
- **Rejected packed objects:** an import of more than 100 objects rejected late, on
  `too-many-commits` with a low limit, leaves none of its objects in the private repository.
  This is the case `prune` cannot clean.
- **Delta bomb:** a few-KiB delta declaring a 4 GiB result. Rule: `oversized-object`, from the
  pre-scan, before any git process starts. The test asserts git was not spawned. A zlib stream
  inflating past its declared size gives `malformed-pack`.
- **Resource:** on Linux, a pack that passes the pre-scan but makes `index-pack` exceed a
  lowered `RLIMIT_AS` gives `resource`, charged to the run. `ENOSPC`, simulated with a full
  tmpfs, is an infrastructure failure and keeps the staged copy.
- **Refs:** `bundle create --all`, another branch only, and the task branch plus a tag each give
  `unexpected-refs`.
- **Prerequisites:**
  - unrelated history with no prerequisite gives `no-prerequisite`;
  - a forged prerequisite that is present but unseeded, and one that is absent, both give
    `bad-prerequisite`, with identical error text;
  - more than 64 prerequisites gives `malformed-header`.
- **History:**
  - an unrelated root merged in gives `new-root`;
  - rewritten history on rework gives `unrelated-history`;
  - a conflicting-variant delivery that drops `<theirs>` gives `merge-not-completed`;
  - a tip equal to the start gives `no-new-commits`.
- **Size:**
  - a staged copy over `max_bundle_bytes` gives `oversized-bundle`;
  - a 200 MiB compressible blob gives `oversized-object`;
  - `max_commits + 1` commits gives `too-many-commits`.
- **Header:**
  - v1, `# v4`, an oversized header, a NUL in a ref, or bad hex gives `malformed-header`;
  - v3 `@filter` or an object-format mismatch gives `unsupported-bundle`;
  - v2 in a SHA-256 repository gives `unsupported-bundle`.
- **fsck:** a `.gitmodules` URL starting with `-`, or a `.git` tree entry. Rule: `fsck-failed`.
- **Hostile planting:** `plant_operator` plus `plant_parent_env` with a valid bundle. The import
  succeeds, and the tripwire shows no daemon-side firing.
- **Replay and crash:**
  - importing the same staged copy twice gives one promotion;
  - a crash injected after the `.pack` move and before the `.idx` move, then a restart, gives a
    consistent private repository and an idempotent re-import.

## 9. Workspace state, seeding, retirement, and cleanup

Add the durable workspace state (additive migration):

- `seeded_base_sha`, `seeded_upstream_sha`, `imported_head_sha`;
- the consecutive-rejection counter and last rule code.

Add `Workspace::seed(variant)` in staging, followed by a no-replace rename. Add
`Workspace::retire` and the retention sweep, using `remove_tree`. Reconciliation skips
`.retired/` and staging. No chmod of seeded trees.

Tests:

- **Seeded workspace:**
  - no remote, no `hooks/`, and only the identity in its config;
  - no `.git/objects` file with `st_nlink > 1`;
  - the exclude set recorded for each seed variant matches the spec table.
- **Hostile repository content:**
  - the base commit contains filter, diff, and merge attributes, symlinks to `$HOME` and to the
    DB, `.githooks/post-checkout`, and an executable `preflight.sh`;
  - seeding checks them out verbatim;
  - the tripwire is empty, even with `plant_operator` setting `core.hooksPath`,
    `post-index-change`, and global filter drivers;
  - the modes of the files the symlinks point at are unchanged.
- **Retirement:**
  - a run-chmodded read-only workspace containing symlinks to a sentinel and a FIFO is retired
    and swept without touching the sentinel, without blocking, and without running git;
  - `.retired/` and staging entries are never reconciled as workspaces.
- **Rename:** a run-created empty directory at the target name makes the no-replace rename fail
  loudly instead of replacing it.

## 10. Hardened publication from the private repository

Port M1–M4 and M15–M21 to `PrivateRepo`, for clone mode:

- pin, retire, and reconcile publication sources;
- `push_new_branch` and `push_to_pr_head`, with lease, fast-forward, and `ls-remote` checks;
- tombstones and task-ref deletion.

This is an API with direct tests. Lifecycle wiring is slice 12.

Tests:

- **Planted `pre-push`:**
  - the pushed tree contains `.githooks/pre-push` and a `preflight.sh` tripwire;
  - `plant_operator` sets a global `core.hooksPath`;
  - the push succeeds, the remote ref equals the imported head, and the tripwire is empty.
- **URL rewrites:**
  - the push goes to `remote_url` even when `repo_dir` and the workspace name decoy origins;
  - a `pushInsteadOf` planted after startup is caught by the before-push check, and nothing is
    pushed to the decoy.
- **Credentials:** under `hermetic`, a planted global `credential.helper` tripwire does not fire.
- **Remote rejection:** a `file://` remote's `pre-receive` rejects, which is the remote's own
  hook and is expected to run. The API returns the rejection text with no local hook section.
- **Lease and fast-forward:** the existing `push_to_pr_head` tests are ported with the private
  repository as the source.

## 11. Run outbox and export step (plumbing only)

The local launcher creates the outbox and runs the fixed export command (`--version=3`) after the
agent exits, including on timeout and kill. It runs in a new process group with a 60 s
group-kill timeout and an allowlisted environment. This environment is new; today only the
orchestrator token is stripped. `fake_agent` gains a commit-and-exit mode. Nothing consumes the
bundle yet.

Tests:

- The export of a `fake_agent` commit produces a v3 bundle with exactly the task ref and the
  declared prerequisites.
- **Export environment:** the export's environment (dumped by a test hook) contains no
  `GH_TOKEN`, `GITHUB_TOKEN`, `SSH_AUTH_SOCK`, `QUORUM_*`, or orchestrator token.
- **Leftover processes:** an export whose workspace config spawns a long-running child (through
  a run-side planted `core.fsmonitor`) leaves no surviving process after the group kill.
- The export of a reused rework workspace uses `seeded_upstream_sha` and succeeds, while the
  base has since moved upstream.

## 12. Worker lifecycle and failure mapping in clone mode

Wire seed → run → export → collect → import → publish for workers. Map `no-delivery`,
`bundle-rejected:<rule>` (`AgentFailed`, `NonFailover`), the park after repeated rejection
(using the slice 9 counter), infrastructure retries, `lfs-unsupported`, and push rejection.

Tests (real `quorum serve` with `--incomplete-clone-mode`, a `file://` remote, a fake `gh`, and
`fake_agent`):

- **Happy path:** PR created from the imported head; the task enters review.
- **Hostile workspace end to end:** `plant_workspace` before committing. The task publishes, and
  the tripwire shows no daemon-side entries.
- **Symlinked bundle:** `delivery.bundle` is a symlink to the daemon DB. Outcome: `unsafe-file`,
  the DB bytes never reach `imports/`, and nothing is pushed.
- **Symlinked outbox subpath:** the outbox is replaced by a symlink to another task's valid
  bundle. Outcome: `unsafe-file`.
- **FIFO bundle:** the daemon tick does not block, and the outcome is `unsafe-file`.
- **Oversized bundle:** the outcome is `oversized-bundle`, and `imports/` stays under the cap.
- **No commits, or another branch only:** `no-delivery`, charged to the retry budget.
- **Foreign-object bundle through the lifecycle:** `fake_agent` replaces the export with the
  slice 8 foreign-blob bundle. Outcome: `bundle-rejected:foreign-object`, and nothing is pushed.
- **Repeated rejection:** the same rule on consecutive runs parks at the cap, with the rule in
  the reason. This is the park test moved here from the publication slice.
- **Push rejection:** a remote `pre-receive` rejection parks through the existing
  push-rejection path.
- **LFS:** an LFS base parks with `lfs-unsupported`, and so does a delivery that adds a pointer
  file without an attribute.
- **Rework:** the second run reuses the workspace, and its push is a fast-forward.

## 13. Read-only roles and planning in clone mode

Read-only roles get no `.git` (owner decision, 2026-11-12). Implement:

- `Workspace::export_tree(sha)`: `archive --worktree-attributes` of the frozen commit from the
  private repository, bounded by `max_export_bytes` and the 30 s timeout. A Rust no-follow
  extractor replaces the system `tar` and works relative to a staging directory descriptor. The
  result is handed over with the no-replace rename. The planner view uses the same path;
- the `export-too-large` and `export-unsafe-path` parks, with no retry, and timeout as a bounded
  infrastructure retry;
- the discovery ceiling: `GIT_CEILING_DIRECTORIES` for every run;
  `GIT_DIR=<state dir>/empty/.none` and `QUORUM_REPO` for every read-only run; and the startup
  refusal of a `worktree_base` with a `.git` in itself or an ancestor;
- reviewer, review-only, and continuation-reviewer workspaces as the exported tree of the
  verified PR head (`refs/quorum/pr/<n>`), with the read-only mount mode and the inputs
  directory passed to the launcher, and no export declaration;
- `PrivateRepo::review_materials`, written into the inputs directory, with `<base>` and `<head>`
  chosen per review kind, and `remerge.patch` produced with the merge's sanitized attribute tree;
- an empty inputs directory for the planner;
- a durable seeded head SHA in the reviewer journal, replacing R17, with the fail-safe fallback
  for legacy journals;
- the planner frozen view as the exported tree of the frozen SHA, and frozen-base capture from
  the private repository (M25, M26);
- `QUORUM_REPO` kept for restricted spawns, and `GH_REPO` set in run environments.

These materials are the input to the separate read-only role prompt work. Until it lands,
today's read-only prompts, which name `git` commands, do not work in clone mode; that is
acceptable behind `--incomplete-clone-mode`, and slice 17 waits for it.

Tests:

- **No `.git`:** for each read-only role (planner, reviewer, review-only, continuation
  reviewer), the workspace contains no `.git` directory or file, no refs, and no objects. Its
  files equal `ls-tree -r` of the frozen commit, with symlinks kept as symlinks. A tree with
  `export-ignore`, `export-subst`, and `eol=crlf` attributes is exported verbatim.
- **Hostile export:** the planted `gpgsig` commit with an `export-subst` file containing
  `$Format:%G?$`, plus the `gpg` tripwire first on `PATH`, leaves the tripwire empty, and the
  file stays literal.
- **Extractor:**
  - a tree with an `A` symlink to a sentinel directory and an `a/pwn` file is refused with
    `export-unsafe-path` on a case-insensitive filesystem (macOS default, or a case-folding
    test volume), and the sentinel directory stays empty;
  - symlinks with absolute and `../..` targets are created as symlinks and never followed, and
    the files they point at are unchanged;
  - a hand-built tar stream with an absolute path, a `..` component, a `.git` component, a
    hardlink entry, and a device entry is refused, entry by entry.
- **Export bounds:** a frozen tree whose archive exceeds a lowered `max_export_bytes` parks the
  task with `export-too-large` after exactly one attempt, with no retry or backoff loop. A
  simulated timeout retries at most 3 times, then parks.
- **Enclosing repository:** `worktree_base` is placed under a scratch repository whose
  `remote.origin.url` names a decoy. Startup refuses with exit 2, naming the enclosing `.git`.
  With the startup check bypassed by a test seam, a read-only run's `git rev-parse
  --show-toplevel` and `quorum` CLI calls (`paths::resolve_repo`, `git_toplevel`) find no
  repository, and the CLI resolves the configured repository through `QUORUM_REPO`.
- **No export for read-only roles:** the launcher receives no export declaration, and the
  export command is never spawned, as the local launcher's test hook records. A read-only run
  that runs `git init` and commits in its workspace leaves no bundle in its outbox, and nothing
  is imported or pushed.
- **Reviewer verdict recovery:** a run that creates a `.git` in its read-only workspace, as a
  directory or as a symlink naming another commit, does not change the recovered SHA, and no
  git runs in it. A legacy journal row without a seeded SHA yields no recovered verdict and a
  fresh review.
- **Planner view:** the view follows the frozen SHA even when `repo_dir`'s `HEAD` moves, and a
  planted `repo_dir` config has no effect.
- **Restricted spawn:** a planner spawn in an exported-tree workspace with no `.git` resolves the
  repository through `QUORUM_REPO`.
- **Review materials:**
  - outputs match `git diff` and `git log` on a fixture;
  - a `diff=t` attribute plus a global textconv tripwire does not fire;
  - an oversized diff is truncated, with a `TRUNCATED` trailer listing the omitted paths;
  - `remerge.patch` is present for a daemon-made merge;
  - for a daemon merge that came out clean through `merge=union`, `remerge.patch` has no hunk
    for that file (with the empty tree it would show `remerge CONFLICT (content)`);
  - a branch-sync CI-fix review's materials cover exactly the range from the recorded merge to
    the gated head.

## 14. Continuation and branch synchronization via `merge-tree`

Replace R5–R8 in clone mode:

- `merge-tree --write-tree` in the private repository, with `GIT_ATTR_SOURCE` set to the
  sanitized attribute tree of `<ours>` (owner decision, 2026-11-12: built-in merge attributes
  are honored, custom drivers stay impossible; sanitizing follows review), under rule 16's
  process group, timeout, and Linux rlimits;
- `commit-tree` for clean merges;
- conflicted paths from `--name-only` output;
- conflict seeding with `<ours>` and `<theirs>` both as starts and excludes;
- adjusted prompt text at `quorum/src/serve/mod.rs:3127-3130`.

Tests:

- **Clean merge:** the seed is the expected two-parent merge commit, and branch sync pushes it.
- **Conflict:** the task body lists exactly the conflicted files. The agent's run-side merge
  exports and imports. A delivery that resolves by dropping `<theirs>` is rejected with
  `merge-not-completed`.
- **Hostile merge configuration:** `merge=t` plus a global driver tripwire does not fire.
- **`merge=union`:** a file marked `merge=union` in `<ours>`'s `.gitattributes`, edited at the
  same place on both sides, merges cleanly into the union of both edits.
- **Custom driver without configuration:** a file marked `merge=evil` in `<ours>`, with no
  `merge.evil.driver` anywhere and a `.gitconfig` in the tree defining one, executes nothing (the
  tripwire stays empty). Git falls back to its text merge: an overlapping edit is listed as a
  conflict, and a non-overlapping edit merges cleanly.
- **`conflict-marker-size`:** a three-line conflicting file with `conflict-marker-size=50000000`
  in `<ours>` produces default 7-character markers. The merge finishes within the timeout, and
  on Linux under `RLIMIT_AS`.
- **`--ff` semantics:** when the base is an ancestor of the head, no merge commit is made.

## 15. Recovery, reconciliation, and replay in clone mode

Port M19–M24 to the private repository. Implement:

- startup directory reconciliation;
- staged-bundle and quarantine replay, including the crash-between-collect-and-import case moved
  here from the worker slice;
- publication-pin reconciliation;
- resume-from-recover through seeding.

Tests:

- **Startup GC:** an unallocated hostile workspace is retired with no git run and the tripwire
  empty.
- **Top-level symlink:** a symlink under `worktree_base` is unlinked, and its target is intact.
- **Resume from recover:** the snapshot tree is reproduced as uncommitted changes in a new
  workspace.
- **Crash between collect and import:** restart replays the staged copy once.
- **Crash during promotion:** restart leaves a consistent private repository and re-imports.
- **Tombstones and pins:** the existing CAS tests are ported, plus a crash after the tombstone is
  written and before DB settlement.

## 16. Migration (not yet default)

Implement the spec's migration:

- the quiescence and URL-agreement preconditions;
- the expected-ref list from the database;
- explicit-refspec fetches;
- heads taken from publication pins where an intent exists;
- `base` from provenance;
- verification that every expected ref arrived;
- the planted-state report for `repo_dir`;
- the marker written at completion;
- refusal to switch back.

Tests:

- **Migration:** a DB plus a `repo_dir` with legacy worktrees, review-stage tasks, recover refs,
  publication pins, and tombstones migrate. Every expected ref is present with its expected SHA,
  and every legacy worktree directory is byte-identical afterwards.
- **Quiescence:** a task in `working` with a legacy worktree blocks startup with exit 2, naming
  the task.
- **Hidden refs:** `uploadpack.hideRefs` in `repo_dir` hiding a recover ref fails migration with
  exit 3, and no marker is written.
- **Publication pin:** a pending intent's head comes from the pin, not from a branch tip that
  moved after it.
- **URL mismatch:** a derived URL naming another `owner/name` exits 2.
- **Crash:** a crash before the marker reruns migration idempotently. After the marker, setting
  `linked-worktree` exits 2.
- **Planted state:** `plant_repo_dir` produces one warning event listing the hooks, keys, and
  `info/attributes` it found, and the tripwire is empty.

## 17. Default flip

Before this slice:

- the separate read-only role prompt work has landed, since read-only workspaces have no `.git`;
- the container image ships a current git release (done in slice 2).

Set the `workspace_mode` default to `clone`, and remove `--incomplete-clone-mode`. Update
`README.md`, `docs/serve-config.example.toml`, and the workspace and lifecycle sections of
`docs/2026-06-23-quorum-design.md`.

Tests:

- A local operator with no new keys starts in clone mode, with `remote_url` derived and logged,
  and completes the slice 12 happy path.
- The full clone suite passes on the floor git and on a current git.
- The container image's smoke test asserts that clone mode starts.

## 18. Remove the linked-worktree mode

One release after slice 17:

- delete the linked-worktree paths (REMOVE rows R1–R16) and `disable_push`;
- keep `workspace_mode = "linked-worktree"` as a recognized value that exits 2 with a migration
  message;
- tighten the source-scan test (`quorum-core/src/git.rs:48`).

Tests:

- The source-scan test fails on a deliberately added `git -C` in a fixture copy.
- The full clone suite passes.

## Verification per slice

Each slice runs `rtk proxy ./preflight.sh` before its PR, as the repository guide requires. The
`hostile_git` suites run in the normal test target and in the floor-git CI job, not behind a
feature flag, so every later change keeps the property.
