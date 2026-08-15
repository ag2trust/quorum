use std::{
    env, fs,
    os::unix::{
        fs::PermissionsExt,
        process::{CommandExt, ExitStatusExt},
    },
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
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
    Timeout,
    DiscoverySpawnFailure,
    Interrupted,
    AbruptOwnerDeath,
}

impl Scenario {
    fn env_value(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::TestFailure => "test-failure",
            Self::CompileFailure => "compile-failure",
            Self::Timeout => "timeout",
            Self::DiscoverySpawnFailure => "discovery-spawn-failure",
            Self::Interrupted => "interrupted",
            Self::AbruptOwnerDeath => "abrupt-owner-death",
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
        write_executable(&repo.join("fixture-test-blocking"), BLOCKING_TEST_SHIM);

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
        let mut command = Command::new("./preflight.sh");
        command
            .current_dir(&self.repo)
            .env("PATH", path)
            .env("PREFLIGHT_TIMING_SCENARIO", scenario.env_value())
            .env(
                "PREFLIGHT_TEST_EXECUTABLE",
                match scenario {
                    Scenario::TestFailure => self.repo.join("fixture-test-failure"),
                    Scenario::Timeout
                    | Scenario::DiscoverySpawnFailure
                    | Scenario::Interrupted
                    | Scenario::AbruptOwnerDeath => self.repo.join("fixture-test-blocking"),
                    _ => self.repo.join("fixture-test-unused"),
                },
            )
            .env("PREFLIGHT_TERM_GRACE_SECS", "0.2")
            .env("PREFLIGHT_FIXTURE_PID_FILE", self.pid_file());
        if matches!(scenario, Scenario::Timeout) {
            command.env("PREFLIGHT_TEST_TIMEOUT_SECS", "5");
        }
        if matches!(scenario, Scenario::DiscoverySpawnFailure) {
            command
                .env("PREFLIGHT_TEST_TIMEOUT_SECS", "2")
                .env("TIMING_TEST_PROCESS_TABLE_SPAWN_FAILURES", "1");
        }
        command.output().expect("run preflight")
    }

    fn timing_collector(&self, scenario: Scenario) -> Command {
        let path = format!(
            "{}:{}",
            self.bin.display(),
            env::var("PATH").expect("PATH is set")
        );
        let mut command = Command::new("scripts/preflight/timing.sh");
        command
            .current_dir(&self.repo)
            .args([
                "--skip-fmt",
                "--skip-clippy",
                "--test-timeout-secs",
                "30",
                "--term-grace-secs",
                "0.2",
            ])
            .env("PATH", path)
            .env("PREFLIGHT_TIMING_SCENARIO", scenario.env_value())
            .env(
                "PREFLIGHT_TEST_EXECUTABLE",
                self.repo.join("fixture-test-blocking"),
            )
            .env("PREFLIGHT_FIXTURE_PID_FILE", self.pid_file())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    }

    fn interrupted_timing(&self) -> Child {
        self.timing_collector(Scenario::Interrupted)
            .spawn()
            .expect("spawn interruptible timing collector")
    }

    fn abruptly_owned_timing(&self) -> Child {
        self.timing_collector(Scenario::AbruptOwnerDeath)
            .process_group(0)
            .spawn()
            .expect("spawn abruptly killable timing collector")
    }

    fn pid_file(&self) -> PathBuf {
        self.repo.join("fixture-pids")
    }

    fn wait_for_fixture_pids(&self) -> Vec<i32> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(text) = fs::read_to_string(self.pid_file()) {
                let pids = text
                    .split_whitespace()
                    .map(|pid| pid.parse().expect("fixture PID is numeric"))
                    .collect::<Vec<_>>();
                assert_eq!(pids.len(), 2, "fixture records test and grandchild");
                return pids;
            }
            assert!(Instant::now() < deadline, "fixture did not publish PIDs");
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn assert_fixture_uses_separate_process_groups(&self) {
        let text = fs::read_to_string(self.pid_file().with_extension("groups"))
            .expect("fixture process groups are written");
        let groups = text
            .split_whitespace()
            .map(|pgid| pgid.parse::<i32>().expect("fixture PGID is numeric"))
            .collect::<Vec<_>>();
        assert_eq!(groups.len(), 2, "fixture records two process groups");
        assert_ne!(
            groups[0], groups[1],
            "fixture grandchild must escape the test process group"
        );
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
    assert_eq!(timing["version"].as_i64(), Some(3));
    assert_gate(timing, "branch_base", 0);
}

fn process_exists(pid: i32) -> bool {
    unsafe { libc::kill(pid, 0) == 0 }
}

fn assert_processes_gone(pids: &[i32]) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while pids.iter().copied().any(process_exists) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(20));
    }
    let survivors = pids
        .iter()
        .copied()
        .filter(|pid| process_exists(*pid))
        .collect::<Vec<_>>();
    assert!(
        survivors.is_empty(),
        "fixture processes survived: {survivors:?}"
    );
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
        "test diagnostic was not preserved; stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
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
fn timeout_reaps_descendants_in_separate_process_groups() {
    let fixture = Fixture::new();
    let output = fixture.preflight(Scenario::Timeout);

    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("fixture_test"));
    assert!(stderr.contains("timed out after"));
    assert!(stderr.contains("--nocapture to diagnose"));
    let timing = fixture.timing();
    assert_branch_base(&timing);
    assert_gate(&timing, "test_execute", 124);
    let binary = &timing["test_binaries"][0];
    assert_eq!(binary["execute_outcome"], "timed_out");
    assert_eq!(binary["execute_timed_out"], true);
    assert_eq!(binary["cleanup"]["term_sent"], true);
    assert_eq!(binary["cleanup"]["kill_sent"], true);
    assert_eq!(
        binary["cleanup"]["complete"], true,
        "cleanup={}",
        binary["cleanup"]
    );
    fixture.assert_fixture_uses_separate_process_groups();
    assert_processes_gone(&fixture.wait_for_fixture_pids());
}

#[test]
fn process_discovery_spawn_failure_is_bounded_and_reaps_descendants() {
    let fixture = Fixture::new();
    let started = Instant::now();
    let output = fixture.preflight(Scenario::DiscoverySpawnFailure);

    assert_eq!(output.status.code(), Some(1));
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(30),
        "process discovery failure did not terminate promptly: {elapsed:?}"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("process-tree discovery could not start")
            && stderr.contains("injected process-table spawn failure"),
        "missing bounded discovery diagnostic: {stderr}"
    );
    let timing = fixture.timing();
    assert_branch_base(&timing);
    assert_gate(&timing, "test_execute", 124);
    let binary = &timing["test_binaries"][0];
    assert_eq!(binary["execute_outcome"], "timed_out");
    assert_eq!(binary["cleanup"]["term_sent"], true);
    assert_eq!(binary["cleanup"]["kill_sent"], true);
    assert!(
        binary["execute_secs"].as_f64().unwrap() < 5.0,
        "test supervisor exceeded its bounded cleanup window"
    );
    assert_eq!(binary["cleanup"]["complete"], true);
    assert!(binary["cleanup"]["error"]
        .as_str()
        .expect("cleanup error is recorded")
        .contains("process-tree discovery could not start"));
    fixture.assert_fixture_uses_separate_process_groups();
    assert_processes_gone(&fixture.wait_for_fixture_pids());
}

#[test]
fn interruption_reaps_the_complete_descendant_tree() {
    let fixture = Fixture::new();
    let mut collector = fixture.interrupted_timing();
    let pid_deadline = Instant::now() + Duration::from_secs(10);
    while !fixture.pid_file().exists() {
        if let Some(status) = collector.try_wait().expect("poll timing collector startup") {
            let output = collector
                .wait_with_output()
                .expect("collect failed startup");
            panic!(
                "timing collector exited before fixture startup ({status}); stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        assert!(
            Instant::now() < pid_deadline,
            "fixture did not publish PIDs"
        );
        thread::sleep(Duration::from_millis(10));
    }
    let fixture_pids = fixture.wait_for_fixture_pids();
    let started = Instant::now();
    assert_eq!(
        unsafe { libc::kill(collector.id() as i32, libc::SIGTERM) },
        0,
        "signal timing collector"
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    while collector
        .try_wait()
        .expect("poll timing collector")
        .is_none()
    {
        if Instant::now() >= deadline {
            collector.kill().expect("kill stuck timing collector");
            panic!("interrupted timing collector did not exit");
        }
        thread::sleep(Duration::from_millis(20));
    }
    let output = collector.wait_with_output().expect("collect timing output");

    assert_eq!(output.status.code(), Some(128 + libc::SIGTERM));
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("interrupted while running test binary 'fixture_test'"));
    let timing = fixture.timing();
    assert_eq!(timing["version"].as_i64(), Some(3));
    assert_eq!(
        timing["interrupted_signal"].as_i64(),
        Some(libc::SIGTERM as i64)
    );
    assert_gate(&timing, "test_execute", (128 + libc::SIGTERM) as i64);
    let binary = &timing["test_binaries"][0];
    assert_eq!(binary["execute_outcome"], "interrupted");
    assert_eq!(
        binary["cleanup"]["complete"], true,
        "cleanup={}",
        binary["cleanup"]
    );
    assert!(binary["execute_secs"].as_f64().unwrap() < 5.0);
    assert_processes_gone(&fixture_pids);
}

#[test]
fn abrupt_owner_group_death_reaps_separate_descendant_groups() {
    let fixture = Fixture::new();
    let collector = fixture.abruptly_owned_timing();
    let fixture_pids = fixture.wait_for_fixture_pids();
    fixture.assert_fixture_uses_separate_process_groups();

    assert_eq!(
        unsafe { libc::killpg(collector.id() as i32, libc::SIGKILL) },
        0,
        "kill timing collector process group"
    );
    let output = collector
        .wait_with_output()
        .expect("collect abruptly killed timing output");

    assert_eq!(output.status.signal(), Some(libc::SIGKILL));
    assert_processes_gone(&fixture_pids);
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

const BLOCKING_TEST_SHIM: &str = r##"#!/usr/bin/env python3
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

pid_file = Path(os.environ["PREFLIGHT_FIXTURE_PID_FILE"])
ready_file = pid_file.with_suffix(".ready")
groups_file = pid_file.with_suffix(".groups")
signal.signal(signal.SIGTERM, lambda _signum, _frame: None)
child_code = """import os, signal, sys, time
from pathlib import Path
os.setpgid(0, 0)
signal.signal(signal.SIGTERM, lambda _signum, _frame: None)
Path(sys.argv[1]).write_text(str(os.getpid()))
while True: time.sleep(1)
"""
child = subprocess.Popen([sys.executable, "-c", child_code, str(ready_file)])
while not ready_file.exists():
    time.sleep(0.01)
groups_file.write_text(f"{os.getpgrp()} {os.getpgid(child.pid)}\n")
pid_file.write_text(f"{os.getpid()} {child.pid}\n")
while True:
    time.sleep(1)
"##;
