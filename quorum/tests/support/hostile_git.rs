//! Shared adversarial git fixture (sandboxed-workspace plan, "Shared
//! adversarial fixture").
//!
//! Every planted command is a tripwire script that appends to a file named
//! after it under the per-test `TRIPWIRE` directory. Daemon-side tests end with
//! [`Tripwire::assert_empty`]; run-side tests assert [`Tripwire::fired`].
//!
//! Fixture git commands run through [`fixture_git`] with a cleared environment
//! and no global or system configuration, so they are deterministic and
//! unaffected by [`plant_parent_env`]. Plant configuration only after a
//! repository's commits exist: planted hooks and filters would otherwise fire
//! during setup.
//!
//! The hand-built `hostile_bundle` builders arrive with bundle verification
//! (plan slice 8), their first user.

#![allow(dead_code)]

use std::ffi::OsString;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::OnceLock;

/// Hooks planted in every hooks directory.
pub const HOOKS: &[&str] = &[
    "post-checkout",
    "pre-push",
    "reference-transaction",
    "post-merge",
    "pre-merge-commit",
    "pre-commit",
    "post-rewrite",
    "post-index-change",
    "pre-auto-gc",
];

/// Executables named like each provider binary, planted at the workspace root.
pub const PROVIDER_BINARIES: &[&str] = &["claude", "codex", "grok", "gemini"];

pub const SENTINEL_SECRET: &str = "QUORUM-HOSTILE-SENTINEL-SECRET";

/// The real git binary, resolved from `PATH` the first time it is asked for.
/// Resolve it before planting `PATH`.
pub fn real_git() -> PathBuf {
    static GIT: OnceLock<PathBuf> = OnceLock::new();
    GIT.get_or_init(|| {
        let path = std::env::var_os("PATH").expect("PATH");
        std::env::split_paths(&path)
            .map(|dir| dir.join("git"))
            .find(|candidate| candidate.is_file())
            .expect("git on PATH")
    })
    .clone()
}

/// A git command for fixture setup: cleared environment, no global/system
/// configuration, a fixed identity, and no hooks.
pub fn fixture_git(dir: &Path) -> Command {
    let mut command = Command::new(real_git());
    command
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", dir)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_AUTHOR_NAME", "Fixture")
        .env("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
        .env("GIT_COMMITTER_NAME", "Fixture")
        .env("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
        .env("GIT_AUTHOR_DATE", "1700000000 +0000")
        .env("GIT_COMMITTER_DATE", "1700000000 +0000")
        .current_dir(dir)
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
        ]);
    command
}

/// Run a fixture git command and return trimmed stdout; panics on failure.
pub fn git(dir: &Path, args: &[&str]) -> String {
    let output = fixture_git(dir).args(args).output().expect("spawn git");
    assert!(
        output.status.success(),
        "fixture git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

/// Run a fixture git command with `stdin`; panics on failure.
pub fn git_stdin(dir: &Path, args: &[&str], stdin: &[u8]) -> String {
    use std::io::Write;
    let mut child = fixture_git(dir)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn git");
    child.stdin.take().unwrap().write_all(stdin).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "fixture git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

pub fn write_executable(path: &Path, body: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, body).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
}

/// Per-test tripwire directory plus the planted commands that write to it.
pub struct Tripwire {
    pub dir: PathBuf,
    bin: PathBuf,
}

impl Tripwire {
    pub fn new(root: &Path) -> Self {
        let dir = root.join("TRIPWIRE");
        let bin = root.join("tripwire-bin");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::create_dir_all(&bin).unwrap();
        Tripwire { dir, bin }
    }

    fn body(&self, name: &str) -> String {
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}/{name}'\nexit 0\n",
            self.dir.display()
        )
    }

    /// Plant a tripwire command at `path` that records `name`.
    pub fn install(&self, path: &Path, name: &str) {
        write_executable(path, &self.body(name));
    }

    /// A tripwire command in the shared tripwire bin directory.
    pub fn command(&self, name: &str) -> PathBuf {
        let path = self.bin.join(name);
        self.install(&path, name);
        path
    }

    /// Names of every planted command that fired.
    pub fn fired(&self) -> Vec<String> {
        let mut fired: Vec<String> = std::fs::read_dir(&self.dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        fired.sort();
        fired
    }

    pub fn assert_empty(&self) {
        let fired = self.fired();
        assert!(fired.is_empty(), "planted commands fired: {fired:?}");
    }
}

/// A hooks directory holding a tripwire for every hook in [`HOOKS`].
pub fn plant_hooks(dir: &Path, tripwire: &Tripwire, prefix: &str) {
    for hook in HOOKS {
        tripwire.install(&dir.join(hook), &format!("{prefix}-hook-{hook}"));
    }
}

/// The hostile configuration keys, as config-file text. `hooks_path` is the
/// value of `core.hooksPath`; `prefix` names this plant's tripwires.
pub fn hostile_config(tripwire: &Tripwire, root: &Path, hooks_path: &str, prefix: &str) -> String {
    let cmd = |name: &str| {
        tripwire
            .command(&format!("{prefix}-{name}"))
            .display()
            .to_string()
    };
    let include_hooks = root.join(format!("{prefix}-include-hooks"));
    plant_hooks(&include_hooks, tripwire, &format!("{prefix}-include"));
    let include = root.join(format!("{prefix}-include.gitconfig"));
    std::fs::write(
        &include,
        format!("[core]\n\thooksPath = \"{}\"\n", include_hooks.display()),
    )
    .unwrap();
    let include_if = root.join(format!("{prefix}-includeif.gitconfig"));
    std::fs::write(
        &include_if,
        format!(
            "[core]\n\thooksPath = \"{}\"\n\tfsmonitor = \"{}\"\n",
            include_hooks.display(),
            cmd("includeif-fsmonitor")
        ),
    )
    .unwrap();
    format!(
        "[core]\n\
         \thooksPath = \"{hooks_path}\"\n\
         \tfsmonitor = \"{fsmonitor}\"\n\
         \tsshCommand = \"{ssh}\"\n\
         \tpager = \"{pager}\"\n\
         \teditor = \"{editor}\"\n\
         \taskPass = \"{askpass}\"\n\
         \talternateRefsCommand = \"{alternate}\"\n\
         [filter \"t\"]\n\
         \tclean = \"{clean}\"\n\
         \tsmudge = \"{smudge}\"\n\
         \tprocess = \"{process}\"\n\
         [diff \"t\"]\n\
         \ttextconv = \"{textconv}\"\n\
         \tcommand = \"{diffcmd}\"\n\
         [diff]\n\
         \texternal = \"{external}\"\n\
         [merge \"t\"]\n\
         \tdriver = \"{driver}\"\n\
         [gpg]\n\
         \tprogram = \"{gpg}\"\n\
         [gpg \"ssh\"]\n\
         \tprogram = \"{gpgssh}\"\n\
         [gpg \"x509\"]\n\
         \tprogram = \"{gpgx509}\"\n\
         [credential]\n\
         \thelper = \"{credential}\"\n\
         [remote \"origin\"]\n\
         \turl = \"ext::{remote}\"\n\
         \tpushurl = \"ext::{pushremote}\"\n\
         \tuploadpack = \"{uploadpack}\"\n\
         \treceivepack = \"{receivepack}\"\n\
         [uploadpack]\n\
         \thideRefs = refs/quorum\n\
         [include]\n\
         \tpath = \"{include}\"\n\
         [includeIf \"gitdir:/\"]\n\
         \tpath = \"{include_if}\"\n",
        fsmonitor = cmd("core-fsmonitor"),
        ssh = cmd("core-sshCommand"),
        pager = cmd("core-pager"),
        editor = cmd("core-editor"),
        askpass = cmd("core-askPass"),
        alternate = cmd("core-alternateRefsCommand"),
        clean = cmd("filter-clean"),
        smudge = cmd("filter-smudge"),
        process = cmd("filter-process"),
        textconv = cmd("diff-textconv"),
        diffcmd = cmd("diff-command"),
        external = cmd("diff-external"),
        driver = cmd("merge-driver"),
        gpg = cmd("gpg-program"),
        gpgssh = cmd("gpg-ssh-program"),
        gpgx509 = cmd("gpg-x509-program"),
        credential = cmd("credential-helper"),
        remote = cmd("remote-url"),
        pushremote = cmd("remote-pushurl"),
        uploadpack = cmd("remote-uploadpack"),
        receivepack = cmd("remote-receivepack"),
        include = include.display(),
        include_if = include_if.display(),
    )
}

fn append(path: &Path, text: &str) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    file.write_all(text.as_bytes()).unwrap();
}

pub const HOSTILE_ATTRIBUTES: &str = "* filter=t diff=t merge=t\n";

/// A planted workspace (see [`plant_workspace`]).
pub struct PlantedWorkspace {
    pub dir: PathBuf,
    /// The ordinary commit.
    pub base: String,
    /// `main`: a child of `base` with a hand-made `gpgsig` header.
    pub head: String,
    /// Directory holding the `gpg` tripwire; put it first on the test `PATH`.
    pub gpg_dir: PathBuf,
}

/// A non-bare repository at `dir` whose tree, history, hooks, and
/// configuration are hostile.
pub fn plant_workspace(dir: &Path, tripwire: &Tripwire) -> PlantedWorkspace {
    let root = dir.parent().unwrap();
    std::fs::create_dir_all(dir).unwrap();
    git(dir, &["init", "-q", "-b", "main"]);

    std::fs::write(
        dir.join(".gitattributes"),
        format!(
            "{HOSTILE_ATTRIBUTES}subst.txt export-subst\n\
             ignored.txt export-ignore\n\
             crlf.txt eol=crlf\n\
             conflict.txt conflict-marker-size=50000000\n"
        ),
    )
    .unwrap();
    std::fs::write(
        dir.join("subst.txt"),
        "commit $Format:%H$ sig $Format:%G?$\n",
    )
    .unwrap();
    std::fs::write(dir.join("ignored.txt"), "export-ignore file\n").unwrap();
    std::fs::write(dir.join("crlf.txt"), "line one\nline two\n").unwrap();
    std::fs::write(dir.join("conflict.txt"), "a\nb\nc\n").unwrap();
    tripwire.install(
        &dir.join(".githooks/pre-push"),
        "workspace-githooks-pre-push",
    );
    tripwire.install(&dir.join("preflight.sh"), "workspace-preflight");
    for provider in PROVIDER_BINARIES {
        tripwire.install(
            &dir.join(provider),
            &format!("workspace-provider-{provider}"),
        );
    }
    let secret = root.join("sentinel-secret");
    std::fs::write(&secret, SENTINEL_SECRET).unwrap();
    for link in ["CLAUDE.md", "AGENTS.md", ".mcp.json"] {
        std::os::unix::fs::symlink(&secret, dir.join(link)).unwrap();
    }
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-q", "-m", "hostile base"]);
    let base = git(dir, &["rev-parse", "HEAD"]);
    let tree = git(dir, &["rev-parse", "HEAD^{tree}"]);
    let signed = format!(
        "tree {tree}\n\
         parent {base}\n\
         author Fixture <fixture@example.invalid> 1700000000 +0000\n\
         committer Fixture <fixture@example.invalid> 1700000000 +0000\n\
         gpgsig -----BEGIN PGP SIGNATURE-----\n \n iQEzBAABCAAdFiEEhostile\n -----END PGP SIGNATURE-----\n\
         \n\
         hostile signed commit\n"
    );
    let head = git_stdin(
        dir,
        &["hash-object", "-t", "commit", "-w", "--stdin"],
        signed.as_bytes(),
    );
    git(dir, &["update-ref", "refs/heads/main", &head]);

    let git_dir = dir.join(".git");
    plant_hooks(&git_dir.join("hooks"), tripwire, "workspace");
    append(
        &git_dir.join("config"),
        &hostile_config(tripwire, root, ".githooks", "workspace"),
    );
    std::fs::create_dir_all(git_dir.join("info")).unwrap();
    std::fs::write(git_dir.join("info/attributes"), HOSTILE_ATTRIBUTES).unwrap();

    let gpg_dir = root.join("gpg-bin");
    for program in ["gpg", "gpgsm", "ssh-keygen"] {
        tripwire.install(&gpg_dir.join(program), &format!("path-{program}"));
    }
    PlantedWorkspace {
        dir: dir.to_path_buf(),
        base,
        head,
        gpg_dir,
    }
}

/// A global `.gitconfig` under the test `HOME` with every hostile key, plus
/// `url.<decoy>.insteadOf` and/or `url.<decoy>.pushInsteadOf` rewriting
/// `remote_url` (`rewrites` names which: `"insteadOf"`, `"pushInsteadOf"`).
pub fn plant_operator(home: &Path, tripwire: &Tripwire, remote_url: &str, rewrites: &[&str]) {
    std::fs::create_dir_all(home).unwrap();
    let hooks = home.join("operator-hooks");
    plant_hooks(&hooks, tripwire, "operator");
    let mut config = hostile_config(tripwire, home, &hooks.display().to_string(), "operator");
    let decoy = format!("ext::{}", tripwire.command("operator-decoy-url").display());
    for rewrite in rewrites {
        config.push_str(&format!("[url \"{decoy}\"]\n\t{rewrite} = {remote_url}\n"));
    }
    std::fs::write(home.join(".gitconfig"), config).unwrap();
}

/// The same keys in an operator checkout's git directory: `config`, `hooks/`,
/// `info/attributes`, and each `worktrees/*/config.worktree`.
pub fn plant_repo_dir(git_dir: &Path, tripwire: &Tripwire) {
    let root = git_dir.parent().unwrap();
    plant_hooks(&git_dir.join("hooks"), tripwire, "repo");
    let hooks = git_dir.join("hooks").display().to_string();
    append(
        &git_dir.join("config"),
        &hostile_config(tripwire, root, &hooks, "repo"),
    );
    std::fs::create_dir_all(git_dir.join("info")).unwrap();
    std::fs::write(git_dir.join("info/attributes"), HOSTILE_ATTRIBUTES).unwrap();
    if let Ok(worktrees) = std::fs::read_dir(git_dir.join("worktrees")) {
        for worktree in worktrees {
            let worktree = worktree.unwrap().path();
            append(
                &worktree.join("config.worktree"),
                &hostile_config(tripwire, root, &hooks, "repo-worktree"),
            );
        }
    }
}

/// The daemon-environment variables a hostile parent could set. Returns the
/// list; the caller applies it to its own process (`std::env::set_var`) only
/// in a test binary with a single test.
pub fn plant_parent_env(root: &Path, tripwire: &Tripwire) -> Vec<(String, OsString)> {
    let attacker = root.join("attacker.git");
    git(root, &["init", "-q", "--bare", attacker.to_str().unwrap()]);
    plant_repo_dir(&attacker, tripwire);
    let exec_path = root.join("attacker-exec-path");
    for name in [
        "git-upload-pack",
        "git-receive-pack",
        "git-pack-objects",
        "git-index-pack",
        "git-remote-file",
        "git-remote-https",
        "git-remote-ext",
    ] {
        tripwire.install(&exec_path.join(name), &format!("exec-path-{name}"));
    }
    let templates = root.join("attacker-templates");
    plant_hooks(&templates.join("hooks"), tripwire, "template");
    let hooks = root.join("attacker-env-hooks");
    plant_hooks(&hooks, tripwire, "env");
    let hooks = hooks.display().to_string();
    let objects = root.join("attacker-objects");
    std::fs::create_dir_all(&objects).unwrap();
    vec![
        ("GIT_DIR".into(), attacker.clone().into()),
        (
            "GIT_WORK_TREE".into(),
            root.join("attacker-worktree").into(),
        ),
        ("GIT_INDEX_FILE".into(), root.join("attacker-index").into()),
        ("GIT_OBJECT_DIRECTORY".into(), objects.clone().into()),
        ("GIT_ALTERNATE_OBJECT_DIRECTORIES".into(), objects.into()),
        ("GIT_EXEC_PATH".into(), exec_path.into()),
        ("GIT_TEMPLATE_DIR".into(), templates.into()),
        (
            "GIT_CONFIG_PARAMETERS".into(),
            format!("'core.hooksPath'='{hooks}'").into(),
        ),
        ("GIT_CONFIG_COUNT".into(), "1".into()),
        ("GIT_CONFIG_KEY_0".into(), "core.hooksPath".into()),
        ("GIT_CONFIG_VALUE_0".into(), hooks.into()),
        ("GH_TOKEN".into(), SENTINEL_SECRET.into()),
    ]
}

/// A clean bare repository standing in for the daemon's private repository.
pub fn private_repo(root: &Path) -> PathBuf {
    let repo = root.join("repository.git");
    git(root, &["init", "-q", "--bare", repo.to_str().unwrap()]);
    repo
}

pub fn file_url(path: &Path) -> String {
    format!("file://{}", path.display())
}

pub fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}
