use std::{
    env, fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use serde_json::Value;
use tempfile::TempDir;

const COMPILE_DIAGNOSTIC: &str = "fixture compile failure: unresolved import fixture_missing";
const TEST_DIAGNOSTIC: &str = "fixture test failure: assertion failed";

#[derive(Clone, Copy)]
enum Scenario {
    Success,
    TestFailure,
    CompileFailure,
    Interrupted,
}

impl Scenario {
    fn env_value(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::TestFailure => "test-failure",
            Self::CompileFailure => "compile-failure",
            Self::Interrupted => "interrupted",
        }
    }
}

struct Fixture {
    _temp: TempDir,
    repo: PathBuf,
    bin: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("create fixture directory");
        let root = temp.path();
        let remote = root.join("origin.git");
        let repo = root.join("repo");
        let bin = root.join("bin");
        fs::create_dir(&bin).expect("create fake binary directory");

        git(root, ["init", "--bare", "-q", remote.to_str().unwrap()]);
        git(root, ["init", "-q", repo.to_str().unwrap()]);
        git(&repo, ["config", "user.name", "Fixture"]);
        git(&repo, ["config", "user.email", "fixture@example.invalid"]);
        git(&repo, ["remote", "add", "origin", remote.to_str().unwrap()]);
        fs::write(repo.join("state"), "base\n").expect("write fixture state");
        git(&repo, ["add", "state"]);
        git(&repo, ["commit", "-qm", "fixture base"]);
        git(&repo, ["branch", "-M", "main"]);
        git(&repo, ["push", "-q", "origin", "main"]);

        let source_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");
        copy_executable(
            &source_root.join("preflight.sh"),
            &repo.join("preflight.sh"),
        );
        let timing_dir = repo.join("scripts/preflight");
        fs::create_dir_all(&timing_dir).expect("create timing script directory");
        copy_executable(
            &source_root.join("scripts/preflight/timing.sh"),
            &timing_dir.join("timing.sh"),
        );

        write_executable(&bin.join("cargo"), CARGO_SHIM);
        write_executable(&repo.join("fixture-test-failure"), TEST_FAILURE_SHIM);
        write_executable(
            &repo.join("fixture-test-interrupted"),
            INTERRUPTED_TEST_SHIM,
        );

        Self {
            _temp: temp,
            repo,
            bin,
        }
    }

    fn preflight(&self, scenario: Scenario) -> Output {
        let path = format!(
            "{}:{}",
            self.bin.display(),
            env::var("PATH").expect("PATH is set")
        );
        Command::new("./preflight.sh")
            .current_dir(&self.repo)
            .env("PATH", path)
            .env("PREFLIGHT_TIMING_SCENARIO", scenario.env_value())
            .env(
                "PREFLIGHT_TEST_EXECUTABLE",
                match scenario {
                    Scenario::TestFailure => self.repo.join("fixture-test-failure"),
                    Scenario::Interrupted => self.repo.join("fixture-test-interrupted"),
                    _ => self.repo.join("fixture-test-unused"),
                },
            )
            .output()
            .expect("run preflight")
    }

    fn timing(&self) -> Value {
        let path = self.repo.join("target/preflight-timing/timing.json");
        let text = fs::read_to_string(path).expect("timing artifact is written");
        serde_json::from_str(&text).expect("timing artifact is valid JSON")
    }
}

fn git<const N: usize>(cwd: &Path, args: [&str; N]) {
    let output = Command::new("git")
        .current_dir(cwd)
        .args(args)
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn write_executable(path: &Path, contents: &str) {
    fs::write(path, contents).expect("write fixture executable");
    let mut permissions = fs::metadata(path)
        .expect("stat fixture executable")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("chmod fixture executable");
}

fn copy_executable(source: &Path, destination: &Path) {
    fs::copy(source, destination).expect("copy preflight fixture source");
    let mut permissions = fs::metadata(destination)
        .expect("stat copied preflight source")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(destination, permissions).expect("chmod copied preflight source");
}

fn assert_gate(timing: &Value, name: &str, expected_exit_code: i64) {
    let gate = timing["gates"]
        .as_array()
        .expect("timing gates is an array")
        .iter()
        .find(|gate| gate["name"] == name)
        .unwrap_or_else(|| panic!("timing artifact is missing {name}"));
    assert_eq!(gate["exit_code"].as_i64(), Some(expected_exit_code));
    assert!(
        gate["duration_secs"].as_f64().is_some(),
        "{name} has a numeric duration"
    );
}

fn assert_branch_base(timing: &Value) {
    assert_eq!(timing["version"].as_i64(), Some(2));
    assert_gate(timing, "branch_base", 0);
}

#[test]
fn success_writes_a_parseable_complete_timing_artifact() {
    let fixture = Fixture::new();
    let output = fixture.preflight(Scenario::Success);

    assert!(
        output.status.success(),
        "preflight failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let timing = fixture.timing();
    assert_branch_base(&timing);
    for gate in [
        "cargo_fmt",
        "cargo_clippy",
        "cargo_test_no_run",
        "test_execute",
    ] {
        assert_gate(&timing, gate, 0);
    }
}

#[test]
fn test_failure_writes_a_parseable_timing_artifact() {
    let fixture = Fixture::new();
    let output = fixture.preflight(Scenario::TestFailure);

    assert_eq!(output.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(TEST_DIAGNOSTIC),
        "test diagnostic was not preserved"
    );
    let timing = fixture.timing();
    assert_branch_base(&timing);
    assert_gate(&timing, "test_execute", 42);
}

#[test]
fn compilation_failure_preserves_diagnostics_and_timing() {
    let fixture = Fixture::new();
    let output = fixture.preflight(Scenario::CompileFailure);

    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(COMPILE_DIAGNOSTIC),
        "compiler diagnostic was not replayed"
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("PREFLIGHT: FAIL (cargo test)"),
        "preflight did not preserve the original failed-gate result"
    );
    let timing = fixture.timing();
    assert_branch_base(&timing);
    assert_gate(&timing, "cargo_test_no_run", 17);
}

#[test]
fn interrupted_execution_writes_a_parseable_timing_artifact() {
    let fixture = Fixture::new();
    let output = fixture.preflight(Scenario::Interrupted);

    assert_eq!(output.status.code(), Some(1));
    let timing = fixture.timing();
    assert_branch_base(&timing);
    let gate = timing["gates"]
        .as_array()
        .expect("timing gates is an array")
        .iter()
        .find(|gate| gate["name"] == "test_execute")
        .expect("timing artifact is missing test_execute");
    assert_ne!(gate["exit_code"].as_i64(), Some(0));
    assert!(gate["duration_secs"].as_f64().is_some());
}

const CARGO_SHIM: &str = r##"#!/bin/sh
case "$1" in
  fmt|clippy) exit 0 ;;
  test)
    if [ "${PREFLIGHT_TIMING_SCENARIO:-success}" = compile-failure ]; then
      printf '%s\n' 'fixture compile failure: unresolved import fixture_missing' >&2
      exit 17
    fi
    if [ "${PREFLIGHT_TIMING_SCENARIO:-success}" != success ]; then
      printf '{"reason":"compiler-artifact","package_id":"fixture","manifest_path":"/fixture/Cargo.toml","target":{"name":"fixture_test","kind":["test"]},"profile":{"test":true},"executable":"%s","fresh":false}\n' "$PREFLIGHT_TEST_EXECUTABLE"
    fi
    exit 0
    ;;
esac
printf 'unexpected cargo invocation: %s\n' "$*" >&2
exit 99
"##;

const TEST_FAILURE_SHIM: &str = r##"#!/bin/sh
printf '%s\n' 'fixture test failure: assertion failed' >&2
exit 42
"##;

const INTERRUPTED_TEST_SHIM: &str = r##"#!/bin/sh
kill -INT $$
"##;
