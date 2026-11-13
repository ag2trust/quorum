//! Hardened git constructor under a hostile operator configuration and a
//! hostile daemon environment (sandboxed-workspace plan, slice 1, "Planted
//! environment").
//!
//! This binary holds exactly one test because it plants variables in its own
//! process environment, which is shared by every thread of the binary.

#[path = "support/hostile_git.rs"]
mod hostile_git;

use hostile_git::*;
use quorum_core::git::{GitCommand, HardenedGit, ObjectFormat, WorkspaceMode, EMPTY_TREE_SHA1};
use std::collections::BTreeSet;
use std::time::Duration;

const LIMIT: usize = 16 * 1024 * 1024;
const TIMEOUT: Duration = Duration::from_secs(60);

fn ok(command: GitCommand) -> Vec<u8> {
    let output = command.output_bounded(None, LIMIT, TIMEOUT).unwrap();
    assert!(output.status.success(), "git failed: {}", stderr(&output));
    output.stdout
}

#[test]
fn local_profile_ignores_planted_operator_config_and_parent_environment() {
    let real = real_git();
    let root = tempfile::tempdir().unwrap();
    let root = root.path();
    let tripwire = Tripwire::new(root);
    let workspace = plant_workspace(&root.join("workspace"), &tripwire);
    let source_url = file_url(&workspace.dir);
    let home = root.join("home");
    // The operator's global config rewrites the very URL the fetch uses.
    plant_operator(
        &home,
        &tripwire,
        &source_url,
        &["insteadOf", "pushInsteadOf"],
    );
    let repo = private_repo(root);

    // A wrapper git that records each child's environment, then runs git.
    let dumps = root.join("env-dumps");
    std::fs::create_dir_all(&dumps).unwrap();
    let wrapper = root.join("wrapper/git");
    write_executable(
        &wrapper,
        &format!(
            "#!/bin/sh\n/usr/bin/env > '{}'/\"$$\"\nexec '{}' \"$@\"\n",
            dumps.display(),
            real.display()
        ),
    );

    let planted = plant_parent_env(root, &tripwire);
    let path = std::env::join_paths([
        workspace.gpg_dir.clone(),
        real.parent().unwrap().to_path_buf(),
        "/usr/bin".into(),
        "/bin".into(),
    ])
    .unwrap();
    std::env::set_var("PATH", &path);
    std::env::set_var("HOME", &home);
    for (key, value) in &planted {
        std::env::set_var(key, value);
    }

    let state = root.join("state");
    std::fs::create_dir_all(&state).unwrap();
    let hardened = HardenedGit::startup(&wrapper, &state, WorkspaceMode::Clone).unwrap();
    let git = hardened.local(&repo, ObjectFormat::Sha1);

    ok(git
        .command("fetch")
        .arg("--no-tags")
        .data(&source_url)
        .data("+refs/heads/main:refs/remotes/origin/main"));
    let head = ok(git
        .command("rev-parse")
        .arg("--verify")
        .data("refs/remotes/origin/main^{commit}"));
    assert_eq!(String::from_utf8_lossy(&head).trim(), workspace.head);
    ok(git
        .command("update-ref")
        .ref_name("refs/quorum/tasks/1/head")
        .unwrap()
        .sha(&workspace.head)
        .unwrap());
    let diff = ok(git
        .command("diff")
        .sha(EMPTY_TREE_SHA1)
        .unwrap()
        .sha(&workspace.head)
        .unwrap());
    assert!(String::from_utf8_lossy(&diff).contains("+line one"));
    let tar = ok(git
        .command("archive")
        .arg("--format=tar")
        .sha(&workspace.head)
        .unwrap());
    assert!(!tar.is_empty());
    let pack = git
        .command("pack-objects")
        .args(["--revs", "--stdout"])
        .output_bounded(
            Some(format!("{}\n", workspace.head).into_bytes()),
            LIMIT,
            TIMEOUT,
        )
        .unwrap();
    assert!(pack.status.success(), "pack-objects: {}", stderr(&pack));
    let pack_path = root.join("scratch/import.pack");
    std::fs::create_dir_all(pack_path.parent().unwrap()).unwrap();
    std::fs::write(&pack_path, &pack.stdout).unwrap();
    ok(git.command("index-pack").arg("--strict").arg(&pack_path));

    // The global reference-transaction hook, filters, textconv, gpg, the
    // GIT_EXEC_PATH tripwires, and the URL rewrite: nothing fired.
    tripwire.assert_empty();
    // The private repository was used, not the planted GIT_DIR.
    assert_eq!(
        git_in(root, &repo, &["rev-parse", "refs/quorum/tasks/1/head"]),
        workspace.head
    );

    let allowed: BTreeSet<&str> = [
        "PATH",
        "HOME",
        "LC_ALL",
        "TZ",
        "GIT_CONFIG_NOSYSTEM",
        "GIT_CONFIG_GLOBAL",
        "GIT_ATTR_NOSYSTEM",
        "GIT_ATTR_SOURCE",
        "GIT_ALLOW_PROTOCOL",
        "GIT_CEILING_DIRECTORIES",
        "GIT_TERMINAL_PROMPT",
        "GIT_OPTIONAL_LOCKS",
        "GIT_NO_REPLACE_OBJECTS",
        "GIT_EDITOR",
        "GIT_SEQUENCE_EDITOR",
        "GIT_PAGER",
        // Added by /bin/sh running the wrapper itself.
        "PWD",
        "OLDPWD",
        "SHLVL",
        "_",
        "__CF_USER_TEXT_ENCODING",
    ]
    .into_iter()
    .collect();
    let mut dumps_seen = 0;
    for dump in std::fs::read_dir(&dumps).unwrap() {
        let text = std::fs::read_to_string(dump.unwrap().path()).unwrap();
        dumps_seen += 1;
        assert!(!text.contains(SENTINEL_SECRET), "secret leaked: {text}");
        for line in text.lines() {
            let key = line.split('=').next().unwrap();
            assert!(allowed.contains(key), "child inherited {key}: {text}");
        }
        assert!(text.contains(&format!("HOME={}", state.join("empty").display())));
    }
    // startup's --version plus the seven commands above.
    assert_eq!(dumps_seen, 8);
}

/// Read the private repository with a clean fixture git, ignoring the planted
/// process environment.
fn git_in(root: &std::path::Path, repo: &std::path::Path, args: &[&str]) -> String {
    let mut full = vec!["--git-dir", repo.to_str().unwrap()];
    full.extend_from_slice(args);
    git(root, &full)
}
