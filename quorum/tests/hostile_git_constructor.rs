//! Hardened git constructor (sandboxed-workspace plan, slice 1): protocol
//! restriction, URL-rewrite refusal, `--end-of-options`, attribute sources, the
//! sanitized attribute tree, verbatim archives, and the git version gate.
//!
//! The planted-environment test lives in `hostile_git_planted_env.rs`: it
//! mutates the process environment and so needs a test binary of its own.

#[path = "support/hostile_git.rs"]
mod hostile_git;

use hostile_git::*;
use quorum_core::git::{
    AttrTree, HardenedGit, LocalGit, ObjectFormat, TransportConfig, WorkspaceMode,
    ATTR_TREE_DIR_LIMIT, ATTR_TREE_INPUT_LIMIT, EMPTY_TREE_SHA1,
};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::TempDir;

const LIMIT: usize = 16 * 1024 * 1024;
const TIMEOUT: Duration = Duration::from_secs(60);

struct Fixture {
    root: TempDir,
    tripwire: Tripwire,
    home: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let tripwire = Tripwire::new(root.path());
        let home = root.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        Fixture {
            root,
            tripwire,
            home,
        }
    }

    fn path(&self) -> &Path {
        self.root.path()
    }

    /// Startup with a daemon environment of `PATH` (with `extra_path` first)
    /// and the test `HOME`.
    fn startup(&self, extra_path: Option<&Path>) -> HardenedGit {
        let state = self.path().join("state");
        std::fs::create_dir_all(&state).unwrap();
        let mut paths: Vec<PathBuf> = extra_path.into_iter().map(Path::to_path_buf).collect();
        paths.push(real_git().parent().unwrap().to_path_buf());
        paths.push("/usr/bin".into());
        paths.push("/bin".into());
        let env: Vec<(OsString, OsString)> = vec![
            ("PATH".into(), std::env::join_paths(paths).unwrap()),
            ("HOME".into(), self.home.clone().into()),
            ("GH_TOKEN".into(), SENTINEL_SECRET.into()),
        ];
        HardenedGit::startup_with_env(real_git(), state, WorkspaceMode::Clone, env).unwrap()
    }

    fn local(&self, extra_path: Option<&Path>) -> (LocalGit, PathBuf) {
        let repo = private_repo(self.path());
        (
            self.startup(extra_path).local(&repo, ObjectFormat::Sha1),
            repo,
        )
    }
}

fn run(command: quorum_core::git::GitCommand) -> std::process::Output {
    command.output_bounded(None, LIMIT, TIMEOUT).unwrap()
}

fn ok(command: quorum_core::git::GitCommand) -> Vec<u8> {
    let output = run(command);
    assert!(output.status.success(), "git failed: {}", stderr(&output));
    output.stdout
}

fn fetch_main(git: &LocalGit, source: &Path) {
    ok(git
        .command("fetch")
        .arg("--no-tags")
        .data(file_url(source))
        .data("+refs/heads/main:refs/remotes/origin/main"));
}

fn env(command: &quorum_core::git::GitCommand, key: &str) -> String {
    command
        .env_value(key)
        .unwrap_or_else(|| panic!("{key} unset"))
        .to_str()
        .unwrap()
        .to_string()
}

#[test]
fn local_profile_refuses_non_file_protocols() {
    let fixture = Fixture::new();
    let (git, _) = fixture.local(None);
    let ext = format!(
        "ext::sh -c {}",
        fixture.tripwire.command("ext-transport").display()
    );
    for url in [
        ext.as_str(),
        "https://example.invalid/owner/name.git",
        "ssh://example.invalid/owner/name.git",
    ] {
        let output = run(git.command("fetch").data(url).data("refs/heads/main"));
        assert!(!output.status.success(), "{url} fetch succeeded");
        assert!(
            stderr(&output).contains("not allowed"),
            "{url}: {}",
            stderr(&output)
        );
    }
    fixture.tripwire.assert_empty();
}

#[test]
fn https_transport_profile_refuses_file_and_ext() {
    let fixture = Fixture::new();
    let source = fixture.path().join("source");
    std::fs::create_dir_all(&source).unwrap();
    git(&source, &["init", "-q", "-b", "main"]);
    git(&source, &["commit", "-q", "--allow-empty", "-m", "x"]);
    let repo = private_repo(fixture.path());
    let transport = fixture
        .startup(None)
        .transport(
            &repo,
            ObjectFormat::Sha1,
            "https://example.invalid/owner/name.git",
            TransportConfig::Inherit,
        )
        .unwrap();
    let ext = format!(
        "ext::sh -c {}",
        fixture.tripwire.command("ext-transport").display()
    );
    for url in [file_url(&source), ext] {
        let output = run(transport
            .command("fetch")
            .data(&url)
            .data("refs/heads/main"));
        assert!(!output.status.success(), "{url} fetch succeeded");
        assert!(
            stderr(&output).contains("not allowed"),
            "{url}: {}",
            stderr(&output)
        );
    }
    fixture.tripwire.assert_empty();
}

#[test]
fn transport_refuses_rewritten_remote_urls() {
    let remote = "https://example.invalid/owner/name.git";
    let construct = |rewrites: &[&str]| {
        let fixture = Fixture::new();
        plant_operator(&fixture.home, &fixture.tripwire, remote, rewrites);
        let repo = private_repo(fixture.path());
        let result = fixture.startup(None).transport(
            &repo,
            ObjectFormat::Sha1,
            remote,
            TransportConfig::Inherit,
        );
        fixture.tripwire.assert_empty();
        result
    };

    let error = construct(&["insteadOf"]).unwrap_err();
    assert_eq!(error.exit_code(), 2, "{error}");
    assert!(error.to_string().contains("insteadOf"), "{error}");

    let error = construct(&["pushInsteadOf"]).unwrap_err();
    assert_eq!(error.exit_code(), 2, "{error}");
    assert!(error.to_string().contains("pushinsteadof"), "{error}");

    let transport = construct(&[]).unwrap();
    assert_eq!(transport.remote_url(), remote);
    assert_eq!(transport.scheme(), "https");

    // A remote section named after the URL redirects a push by URL through
    // its pushurl/receivepack, which `--get-url` does not show.
    let fixture = Fixture::new();
    plant_operator(&fixture.home, &fixture.tripwire, remote, &[]);
    let receivepack = fixture.tripwire.command("named-remote-receivepack");
    let mut config = std::fs::read_to_string(fixture.home.join(".gitconfig")).unwrap();
    config.push_str(&format!(
        "[remote \"{remote}\"]\n\tpushurl = ext::{rp}\n\treceivepack = {rp}\n",
        rp = receivepack.display()
    ));
    std::fs::write(fixture.home.join(".gitconfig"), config).unwrap();
    let repo = private_repo(fixture.path());
    let error = fixture
        .startup(None)
        .transport(&repo, ObjectFormat::Sha1, remote, TransportConfig::Inherit)
        .unwrap_err();
    assert_eq!(error.exit_code(), 2, "{error}");
    assert!(error.to_string().contains("configured remote"), "{error}");
    fixture.tripwire.assert_empty();
}

#[test]
fn data_after_end_of_options_is_never_an_option() {
    let fixture = Fixture::new();
    let source = fixture.path().join("source");
    std::fs::create_dir_all(&source).unwrap();
    git(&source, &["init", "-q", "-b", "main"]);
    git(&source, &["commit", "-q", "--allow-empty", "-m", "x"]);
    let (git, _) = fixture.local(None);
    let upload_pack = fixture.tripwire.command("upload-pack-option");
    let output = run(git
        .command("ls-remote")
        .data(format!("--upload-pack={}", upload_pack.display()))
        .data(file_url(&source)));
    assert!(!output.status.success(), "option-shaped data was obeyed");
    fixture.tripwire.assert_empty();

    // Control: the same remote listed normally works.
    let listed = ok(git.command("ls-remote").data(file_url(&source)));
    assert!(String::from_utf8_lossy(&listed).contains("refs/heads/main"));
}

/// A repository with a union-merged file, root and nested attributes, and a
/// conflicting edit on both sides.
fn merge_fixture(fixture: &Fixture) -> (PathBuf, String, String, String) {
    let work = fixture.path().join("work");
    std::fs::create_dir_all(work.join("sub")).unwrap();
    git(&work, &["init", "-q", "-b", "main"]);
    std::fs::write(
        work.join(".gitattributes"),
        "# root attributes\n\
         [attr]evil merge=evil\n\
         u.txt merge=union conflict-marker-size=50000000\n\
         b.dat merge=binary filter=t\n\
         t.txt merge=text diff=t\n\
         n.txt -merge eol=crlf\n\
         x.txt !merge export-subst\n\
         m.txt merge export-ignore\n\
         bin.dat binary\n\
         e.txt merge=evil evil\n",
    )
    .unwrap();
    std::fs::write(
        work.join("sub/.gitattributes"),
        "*.txt merge=union\n*.cfg filter=t diff=t\n",
    )
    .unwrap();
    std::fs::write(work.join("u.txt"), "base\n").unwrap();
    git(&work, &["add", "-A"]);
    git(&work, &["commit", "-q", "-m", "base"]);
    let base = git(&work, &["rev-parse", "HEAD"]);
    std::fs::write(work.join("u.txt"), "ours\n").unwrap();
    git(&work, &["commit", "-q", "-am", "ours"]);
    let ours = git(&work, &["rev-parse", "HEAD"]);
    git(&work, &["checkout", "-q", "-b", "theirs", &base]);
    std::fs::write(work.join("u.txt"), "theirs\n").unwrap();
    git(&work, &["commit", "-q", "-am", "theirs"]);
    let theirs = git(&work, &["rev-parse", "HEAD"]);
    (work.join(".git"), base, ours, theirs)
}

#[test]
fn sanitized_attribute_tree_keeps_only_builtin_merge_attributes() {
    let fixture = Fixture::new();
    let (git_dir, _, ours, theirs) = merge_fixture(&fixture);
    let git = fixture.startup(None).local(&git_dir, ObjectFormat::Sha1);
    let attrs = AttrTree::sanitized(&git, &ours).unwrap();
    assert!(attrs.bound_exceeded().is_none());
    assert_ne!(attrs.sha(), EMPTY_TREE_SHA1);

    let work = git_dir.parent().unwrap();
    let listing = git_cmd_lines(work, &["ls-tree", "-r", "--name-only", attrs.sha()]);
    assert_eq!(listing, [".gitattributes", "sub/.gitattributes"]);
    assert_eq!(
        crate::git(
            work,
            &[
                "cat-file",
                "blob",
                &format!("{}:.gitattributes", attrs.sha())
            ]
        ),
        "u.txt merge=union\n\
         b.dat merge=binary\n\
         t.txt merge=text\n\
         n.txt -merge\n\
         x.txt !merge\n\
         m.txt merge\n\
         bin.dat -merge"
    );
    assert_eq!(
        crate::git(
            work,
            &[
                "cat-file",
                "blob",
                &format!("{}:sub/.gitattributes", attrs.sha())
            ]
        ),
        "*.txt merge=union"
    );

    // The sanitized tree keeps union merging; the empty tree does not.
    let merge = |attr_tree: &AttrTree| {
        run(git
            .merge_tree(attr_tree)
            .arg("--write-tree")
            .sha(&ours)
            .unwrap()
            .sha(&theirs)
            .unwrap())
    };
    let union = merge(&attrs);
    assert!(union.status.success(), "union merge: {}", stderr(&union));
    let plain = merge(&AttrTree::empty(ObjectFormat::Sha1));
    assert_eq!(
        plain.status.code(),
        Some(1),
        "empty-tree merge must conflict"
    );
    fixture.tripwire.assert_empty();
}

fn git_cmd_lines(dir: &Path, args: &[&str]) -> Vec<String> {
    crate::git(dir, args).lines().map(String::from).collect()
}

#[test]
fn attribute_tree_directory_count_is_bounded() {
    let fixture = Fixture::new();
    let work = fixture.path().join("deep");
    // Each file adds five directories; together they exceed the bound while
    // staying under the file and input bounds.
    let files = ATTR_TREE_DIR_LIMIT / 5 + 1;
    for index in 0..files {
        let dir = work.join(format!("d{index}/a/b/c/e"));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(".gitattributes"), "*.txt merge=union\n").unwrap();
    }
    git(&work, &["init", "-q", "-b", "main"]);
    git(&work, &["add", "-A"]);
    git(&work, &["commit", "-q", "-m", "deep"]);
    let head = git(&work, &["rev-parse", "HEAD"]);
    let local = fixture
        .startup(None)
        .local(work.join(".git"), ObjectFormat::Sha1);
    let attrs = AttrTree::sanitized(&local, &head).unwrap();
    assert_eq!(attrs.sha(), EMPTY_TREE_SHA1);
    let event = attrs.bound_exceeded().expect("bound event");
    assert!(event.contains("directory bound"), "{event}");
}

#[test]
fn oversized_attribute_input_yields_the_empty_tree_and_an_event() {
    let fixture = Fixture::new();
    let work = fixture.path().join("big");
    std::fs::create_dir_all(&work).unwrap();
    git(&work, &["init", "-q", "-b", "main"]);
    let mut big = "u.txt merge=union\n".to_string();
    big.push_str(&"#".repeat(ATTR_TREE_INPUT_LIMIT as usize));
    std::fs::write(work.join(".gitattributes"), big).unwrap();
    git(&work, &["add", "-A"]);
    git(&work, &["commit", "-q", "-m", "big"]);
    let head = git(&work, &["rev-parse", "HEAD"]);
    let local = fixture
        .startup(None)
        .local(work.join(".git"), ObjectFormat::Sha1);
    let attrs = AttrTree::sanitized(&local, &head).unwrap();
    assert_eq!(attrs.sha(), EMPTY_TREE_SHA1);
    let event = attrs.bound_exceeded().expect("bound event");
    assert!(event.contains("bound"), "{event}");
}

#[test]
fn only_merge_tree_and_remerge_diff_read_the_sanitized_attribute_tree() {
    let fixture = Fixture::new();
    let (git_dir, _, ours, _) = merge_fixture(&fixture);
    let git = fixture.startup(None).local(&git_dir, ObjectFormat::Sha1);
    let attrs = AttrTree::sanitized(&git, &ours).unwrap();
    assert_ne!(attrs.sha(), EMPTY_TREE_SHA1);

    assert_eq!(env(&git.merge_tree(&attrs), "GIT_ATTR_SOURCE"), attrs.sha());
    let remerge = git.remerge_diff(&attrs);
    assert_eq!(env(&remerge, "GIT_ATTR_SOURCE"), attrs.sha());
    assert!(remerge.argv().iter().any(|arg| arg == "--remerge-diff"));
    for sub in [
        "merge-tree",
        "show",
        "diff",
        "archive",
        "rev-parse",
        "update-ref",
        "fetch",
        "index-pack",
        "cat-file",
        "ls-tree",
    ] {
        assert_eq!(
            env(&git.command(sub), "GIT_ATTR_SOURCE"),
            EMPTY_TREE_SHA1,
            "{sub}"
        );
    }
}

#[test]
fn archive_is_verbatim_and_runs_no_signature_program() {
    let fixture = Fixture::new();
    let workspace = plant_workspace(&fixture.path().join("workspace"), &fixture.tripwire);
    let (git, repo) = fixture.local(Some(&workspace.gpg_dir));
    fetch_main(&git, &workspace.dir);
    // The test, not the daemon, puts a signature program into the private
    // repository's configuration.
    let gpg = workspace.gpg_dir.join("gpg");
    crate::git(
        fixture.path(),
        &[
            "config",
            "--file",
            repo.join("config").to_str().unwrap(),
            "gpg.program",
            gpg.to_str().unwrap(),
        ],
    );
    let tar = ok(git
        .command("archive")
        .arg("--format=tar")
        .sha(&workspace.head)
        .unwrap());
    let contains = |needle: &[u8]| tar.windows(needle.len()).any(|window| window == needle);
    assert!(contains(b"ignored.txt"), "export-ignore file dropped");
    assert!(contains(b"export-ignore file\n"));
    assert!(
        contains(b"commit $Format:%H$ sig $Format:%G?$\n"),
        "export-subst expanded"
    );
    assert!(contains(b"line one\nline two\n"), "eol=crlf applied");
    assert!(!contains(b"line one\r\n"), "eol=crlf applied");
    fixture.tripwire.assert_empty();
}

fn fake_git(dir: &Path, version: &str) -> PathBuf {
    let path = dir.join(format!("fake-git-{version}"));
    write_executable(
        &path,
        &format!("#!/bin/sh\nprintf 'git version {version}\\n'\n"),
    );
    path
}

#[test]
fn git_version_floor_gates_clone_mode_and_warns_in_linked_mode() {
    let fixture = Fixture::new();
    let state = fixture.path().join("state");
    std::fs::create_dir_all(&state).unwrap();
    let old = fake_git(fixture.path(), "2.42");
    let floor = fake_git(fixture.path(), "2.43.0");
    let startup = |bin: &Path, mode| {
        HardenedGit::startup_with_env(bin, &state, mode, Vec::<(OsString, OsString)>::new())
    };

    let error = startup(&old, WorkspaceMode::Clone).unwrap_err();
    assert_eq!(error.exit_code(), 2);
    let message = error.to_string();
    assert!(
        message.contains("2.42") && message.contains("2.43"),
        "{message}"
    );

    let linked = startup(&old, WorkspaceMode::LinkedWorktree).unwrap();
    let warning = linked.floor_warning().expect("below-floor warning");
    assert!(
        warning.contains("2.42") && warning.contains("attribute isolation is inactive"),
        "{warning}"
    );

    for mode in [WorkspaceMode::Clone, WorkspaceMode::LinkedWorktree] {
        let git = startup(&floor, mode).unwrap();
        assert!(git.floor_warning().is_none());
        assert_eq!(git.version().to_string(), "2.43.0");
    }
}

#[test]
fn startup_refuses_a_planted_empty_directory() {
    let fixture = Fixture::new();
    let state = fixture.path().join("state");
    std::fs::create_dir_all(state.join("empty-hooks")).unwrap();
    std::fs::write(state.join("empty-hooks/pre-push"), "#!/bin/sh\n").unwrap();
    let error = HardenedGit::startup_with_env(
        real_git(),
        &state,
        WorkspaceMode::Clone,
        Vec::<(OsString, OsString)>::new(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("must be empty"), "{error}");
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(
        state.join("empty-hooks"),
        std::fs::Permissions::from_mode(0o700),
    )
    .unwrap();
}
