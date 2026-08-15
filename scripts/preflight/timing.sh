#!/usr/bin/env python3
"""Structured per-gate and per-test-binary timing collector.

Runs the preflight test gates (fmt, clippy, test compile, test execute) and,
for each test executable, records compile/no-run and execution durations by
combining ``cargo --message-format=json`` (for the *identity* of each test
binary) with a ``RUSTC_WRAPPER`` shim (for the *exact per-rustc-invocation
compile interval*, preserving Cargo's normal parallel build).

Cargo's stable ``compiler-artifact`` JSON message identifies a test binary and
its executable path but carries neither a start time nor a duration; naive
inter-artifact-arrival gaps misattribute concurrent and shared work. This
script therefore points ``RUSTC_WRAPPER`` at itself (dispatching via the
``TIMING_RUSTC_WRAPPER_ACTIVE`` env var) so it fork/execs rustc while wall-
clocking each invocation, then correlates each entry to a compiler-artifact
message by ``(--crate-name, --test)``. Execution time is measured by invoking
each test executable directly and wall-clocking its run.

Outputs (under ``target/preflight-timing/`` by default):
  timing.json                  — machine-readable artifact
  summary.txt                  — human-readable summary + bounded top-N
  cargo-test-no-run.jsonl      — raw cargo JSON stream
  cargo-test-no-run.stderr     — cargo stderr
  rustc-invocations.jsonl      — per-rustc-invocation timing log

Usage:
    scripts/preflight/timing.sh [--top-n N] [--out DIR] [--test-threads N]
                                [--skip-fmt] [--skip-clippy] [--self-test]

Defaults: --top-n 10, --out target/preflight-timing,
          --test-threads $RUST_TEST_THREADS or 4.

Exit codes mirror preflight.sh: 0 pass, 1 gate failure, 2 usage.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

CARGO_FEATURES = ["--all-features", "--features", "quorum-core/test-support"]

WRAPPER_ACTIVE_ENV = "TIMING_RUSTC_WRAPPER_ACTIVE"
WRAPPER_LOG_ENV = "TIMING_RUSTC_LOG"


def now() -> float:
    return time.monotonic()


# ---------------------------------------------------------------------------
# RUSTC_WRAPPER mode
# ---------------------------------------------------------------------------


def _parse_rustc_argv(argv: list[str]) -> tuple[str | None, bool, list[str]]:
    """Extract ``--crate-name``, whether ``--test`` is present, and
    ``--crate-type`` values from a rustc argv."""
    crate_name: str | None = None
    is_test = False
    crate_types: list[str] = []
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--crate-name" and i + 1 < len(argv):
            crate_name = argv[i + 1]
            i += 2
            continue
        if a == "--test":
            is_test = True
        elif a == "--crate-type" and i + 1 < len(argv):
            crate_types.append(argv[i + 1])
            i += 2
            continue
        elif a.startswith("--crate-type="):
            crate_types.append(a.split("=", 1)[1])
        i += 1
    return crate_name, is_test, crate_types


def _rustc_wrapper() -> int:
    """Fork/exec rustc, wall-clock the invocation, append a JSON line to the
    log named by ``TIMING_RUSTC_LOG``. Single ``f.write()`` on an <PIPE_BUF
    payload is atomic on POSIX, so concurrent wrappers may safely share the
    log file."""
    argv = sys.argv[1:]
    if not argv:
        return 2
    crate_name, is_test, crate_types = _parse_rustc_argv(argv)

    start = time.monotonic()
    try:
        pid = os.fork()
    except OSError:
        rc = subprocess.run(argv).returncode
        end = time.monotonic()
    else:
        if pid == 0:
            try:
                os.execvp(argv[0], argv)
            except OSError:
                os._exit(127)
        _, status = os.waitpid(pid, 0)
        end = time.monotonic()
        rc = os.WEXITSTATUS(status) if os.WIFEXITED(status) else 1

    log = os.environ.get(WRAPPER_LOG_ENV)
    if log and crate_name:
        # ``CARGO_MANIFEST_DIR`` uniquely identifies the source package for
        # each rustc invocation cargo drives, disambiguating targets that
        # collapse to the same rustc crate name across workspace members.
        manifest_dir = os.environ.get("CARGO_MANIFEST_DIR", "")
        pkg_name = os.environ.get("CARGO_PKG_NAME", "")
        pkg_version = os.environ.get("CARGO_PKG_VERSION", "")
        entry = {
            "crate_name": crate_name,
            "is_test": is_test,
            "crate_types": crate_types,
            "duration_secs": round(end - start, 6),
            "end_monotonic": round(end, 6),
            "exit_code": rc,
            "manifest_dir": manifest_dir,
            "pkg_name": pkg_name,
            "pkg_version": pkg_version,
        }
        try:
            with open(log, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except OSError:
            pass
    return rc


# ---------------------------------------------------------------------------
# Correlation
# ---------------------------------------------------------------------------


def _normalize_crate_name(name: str) -> str:
    """rustc emits the Rust identifier form of a crate name (hyphens replaced
    with underscores) via ``--crate-name``. Cargo's structured ``target.name``
    keeps the original hyphens for non-library targets. Normalize both sides
    here so ``fake-agent`` (cargo target) matches ``fake_agent`` (rustc key).
    """
    return name.replace("-", "_")


def correlate_compile_times(
    rustc_log: Path, binaries: list[dict]
) -> tuple[int, int]:
    """Attach exact per-invocation compile times from the RUSTC_WRAPPER log
    onto ``binaries`` in place. The compound key is
    ``(manifest_dir, is_test, normalized_crate_name)``:

    - ``manifest_dir`` (from cargo's ``CARGO_MANIFEST_DIR`` env in the wrapper
      and ``dirname(manifest_path)`` in the compiler-artifact message)
      distinguishes workspace members whose target names would otherwise
      collide.
    - ``is_test`` guards against matching a non-test build of the same crate.
    - ``normalized_crate_name`` reconciles cargo's original hyphenated
      ``target.name`` with rustc's underscored ``--crate-name``.

    Returns ``(matched_count, log_entry_count)``.
    """
    Key = tuple  # (manifest_dir: str, is_test: bool, name: str)
    by_key: dict[Key, float] = {}
    # Fallback key when a wrapper record lacks manifest_dir (older log lines
    # or non-cargo invocations); used only if no manifest-qualified match.
    by_name_only: dict[tuple[bool, str], float] = {}
    entries = 0
    if rustc_log.exists():
        with rustc_log.open() as f:
            for line in f:
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    e = json.loads(stripped)
                except json.JSONDecodeError:
                    continue
                entries += 1
                if not e.get("is_test"):
                    continue
                name = e.get("crate_name")
                if not name:
                    continue
                dur = float(e.get("duration_secs") or 0.0)
                norm = _normalize_crate_name(name)
                by_name_only[(True, norm)] = (
                    by_name_only.get((True, norm), 0.0) + dur
                )
                manifest_dir = e.get("manifest_dir") or ""
                if manifest_dir:
                    key = (manifest_dir, True, norm)
                    by_key[key] = by_key.get(key, 0.0) + dur
    matched = 0
    for b in binaries:
        target_name = b.get("target_name") or ""
        norm_name = _normalize_crate_name(target_name)
        manifest_path = b.get("manifest_path") or ""
        manifest_dir = (
            str(Path(manifest_path).parent) if manifest_path else ""
        )
        dur: float | None = None
        source: str | None = None
        if manifest_dir:
            k = (manifest_dir, True, norm_name)
            if k in by_key:
                dur = by_key[k]
                source = "rustc_wrapper"
        if dur is None:
            k2 = (True, norm_name)
            if k2 in by_name_only:
                dur = by_name_only[k2]
                source = "rustc_wrapper"
        if dur is not None:
            b["compile_no_run_secs"] = round(dur, 3)
            b["compile_no_run_source"] = source
            matched += 1
        elif b.get("fresh"):
            # Cargo skipped rustc for this artifact — no compile cost this run.
            b["compile_no_run_secs"] = 0.0
            b["compile_no_run_source"] = "cached_fresh"
        else:
            b["compile_no_run_secs"] = None
            b["compile_no_run_source"] = "unmatched"
    return matched, entries


# ---------------------------------------------------------------------------
# Cargo drivers
# ---------------------------------------------------------------------------


def run_gate(argv: list[str]) -> tuple[float, int]:
    t0 = now()
    proc = subprocess.run(argv)
    return now() - t0, proc.returncode


def compile_tests(
    compile_log: Path,
    stderr_log: Path,
    rustc_log: Path,
    wrapper_path: str,
) -> tuple[float, int, list[dict], int, int]:
    """Run ``cargo test --no-run --message-format=json`` with the RUSTC_WRAPPER
    active. Enumerate test binaries from ``compiler-artifact`` messages, then
    correlate exact compile intervals from the wrapper log.
    """
    rustc_log.write_text("")
    env = os.environ.copy()
    env["RUSTC_WRAPPER"] = wrapper_path
    env[WRAPPER_ACTIVE_ENV] = "1"
    env[WRAPPER_LOG_ENV] = str(rustc_log)

    argv = [
        "cargo", "test", "--no-run", "--message-format=json",
        "--workspace", *CARGO_FEATURES,
    ]
    binaries: list[dict] = []
    t0 = now()
    with compile_log.open("w") as clog, stderr_log.open("w") as elog:
        proc = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=elog,
            text=True,
            env=env,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            clog.write(line)
            stripped = line.strip()
            if not stripped:
                continue
            try:
                msg = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            if msg.get("reason") != "compiler-artifact":
                continue
            profile = msg.get("profile") or {}
            target = msg.get("target") or {}
            executable = msg.get("executable")
            if not profile.get("test") or not executable:
                continue
            binaries.append({
                "package_id": msg.get("package_id"),
                "manifest_path": msg.get("manifest_path"),
                "target_name": target.get("name"),
                "target_kinds": list(target.get("kind") or []),
                "executable": executable,
                "fresh": bool(msg.get("fresh")),
            })
        proc.wait()
    matched, log_entries = correlate_compile_times(rustc_log, binaries)
    return now() - t0, proc.returncode, binaries, matched, log_entries


def run_test_binary(exe: str, threads: int) -> tuple[float, int]:
    argv = [exe, "--test-threads", str(threads)]
    t0 = now()
    proc = subprocess.run(argv)
    return now() - t0, proc.returncode


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def slowest(binaries: list[dict], top_n: int) -> list[dict]:
    def key(b: dict) -> float:
        return (
            (b.get("execute_secs") or 0.0)
            + (b.get("compile_no_run_secs") or 0.0)
        )

    return sorted(binaries, key=key, reverse=True)[:top_n]


def emit_artifact(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    json.loads(path.read_text())


def emit_summary(path: Path, data: dict, top_n: int) -> None:
    lines: list[str] = []
    lines.append("=== preflight timing summary ===")
    lines.append(f"timestamp_utc: {data['timestamp_utc']}")
    wrapper = data.get("rustc_wrapper") or {}
    if wrapper:
        lines.append(
            "rustc_wrapper: matched "
            f"{wrapper.get('matched', 0)} of "
            f"{len(data.get('test_binaries') or [])} binaries "
            f"from {wrapper.get('log_entries', 0)} rustc invocations"
        )
    lines.append("")
    lines.append("gates:")
    for g in data["gates"]:
        rc = g["exit_code"]
        tag = "ok" if rc == 0 else f"FAIL(exit={rc})"
        lines.append(f"  {g['name']:<24} {g['duration_secs']:>10.2f}s  {tag}")
    lines.append("")
    binaries = data.get("test_binaries") or []
    top = slowest(binaries, top_n)
    lines.append(
        f"slowest test binaries (top {len(top)} of {len(binaries)}):"
    )
    lines.append(
        f"  {'binary':<48} {'compile_no_run':>16} {'execute':>12}"
    )
    for b in top:
        name = b.get("target_name") or Path(b["executable"]).name
        c = b.get("compile_no_run_secs")
        e = b.get("execute_secs") or 0.0
        c_str = f"{c:>14.2f}s" if c is not None else f"{'n/a':>15}"
        lines.append(f"  {name[:48]:<48} {c_str} {e:>10.2f}s")
    lines.append("")
    path.write_text("\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# Main collect
# ---------------------------------------------------------------------------


def collect(args: argparse.Namespace, wrapper_path: str) -> int:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    artifact = out / "timing.json"
    summary = out / "summary.txt"
    compile_log = out / "cargo-test-no-run.jsonl"
    stderr_log = out / "cargo-test-no-run.stderr"
    rustc_log = out / "rustc-invocations.jsonl"

    gates: list[dict] = []
    binaries: list[dict] = []
    wrapper_stats: dict = {}
    status = 0

    def add_gate(name: str, duration: float, rc: int) -> None:
        nonlocal status
        gates.append({
            "name": name,
            "duration_secs": round(duration, 3),
            "exit_code": rc,
        })
        if rc != 0 and status == 0:
            status = 1

    if not args.skip_fmt:
        print("=== timing 1/4: cargo fmt --all -- --check ===", flush=True)
        dur, rc = run_gate(["cargo", "fmt", "--all", "--", "--check"])
        add_gate("cargo_fmt", dur, rc)

    if not args.skip_clippy and status == 0:
        print(
            "=== timing 2/4: cargo clippy --all-targets --all-features "
            "--features quorum-core/test-support -- -D warnings ===",
            flush=True,
        )
        dur, rc = run_gate([
            "cargo", "clippy", "--all-targets", *CARGO_FEATURES,
            "--", "-D", "warnings",
        ])
        add_gate("cargo_clippy", dur, rc)

    if status == 0:
        print(
            "=== timing 3/4: cargo test --no-run --message-format=json "
            "--workspace (RUSTC_WRAPPER active) ===",
            flush=True,
        )
        dur, rc, binaries, matched, log_entries = compile_tests(
            compile_log, stderr_log, rustc_log, wrapper_path
        )
        add_gate("cargo_test_no_run", dur, rc)
        wrapper_stats = {
            "matched": matched,
            "log_entries": log_entries,
            "log_path": str(rustc_log),
        }

    if status == 0:
        print(
            f"=== timing 4/4: run {len(binaries)} test binaries "
            f"(--test-threads {args.test_threads}) ===",
            flush=True,
        )
        t0 = now()
        exec_rc = 0
        for b in binaries:
            edur, erc = run_test_binary(b["executable"], args.test_threads)
            b["execute_secs"] = round(edur, 3)
            b["execute_exit_code"] = erc
            if erc != 0 and exec_rc == 0:
                exec_rc = erc
        add_gate("test_execute", now() - t0, exec_rc)

    data = {
        "version": 2,
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "top_n": args.top_n,
        "test_threads": args.test_threads,
        "gates": gates,
        "test_binaries": binaries,
        "top_n_slowest": slowest(binaries, args.top_n),
        "rustc_wrapper": wrapper_stats,
    }
    emit_artifact(artifact, data)
    emit_summary(summary, data, args.top_n)

    tag = "PASS" if status == 0 else "FAIL"
    print(f"\nPREFLIGHT TIMING: {tag} — {artifact} / {summary}")
    return status


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------


def self_test() -> int:
    """Fixture checks — no cargo required. Cover:
      1. Artifact is valid JSON and top-N in the summary is bounded.
      2. Rustc-argv parser extracts crate_name/--test/--crate-type correctly.
      3. Correlation attaches exact per-binary durations from a synthetic
         wrapper log and leaves unmatched binaries as null.
    """
    # ---- (2) rustc-argv parser ----
    name, is_test, kinds = _parse_rustc_argv([
        "/usr/bin/rustc", "--crate-name", "quorum_core", "--edition=2021",
        "src/lib.rs", "--crate-type", "lib", "--test",
        "--crate-type=proc-macro",
    ])
    assert name == "quorum_core", name
    assert is_test is True
    assert kinds == ["lib", "proc-macro"], kinds

    name, is_test, _ = _parse_rustc_argv([
        "/usr/bin/rustc", "--crate-name=serde",
        "--crate-name", "serde_json",
    ])
    # Only the space-separated form is used; the ``=`` form isn't emitted by
    # cargo for --crate-name, but if both appear the last space-separated one
    # wins deterministically.
    assert name == "serde_json", name
    assert is_test is False

    # ---- (3) correlation fixture ----
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        log = out / "rustc.jsonl"
        # Reviewer regression: hyphenated cargo target (`fake-agent`) must
        # match the rustc key (`fake_agent`).  Also cover: two workspace
        # members that expose the same target name → manifest_dir
        # disambiguates.  Also cover: legacy wrapper record without
        # manifest_dir → falls back to name-only key.
        log.write_text("\n".join([
            json.dumps({
                "crate_name": "quorum_core", "is_test": True,
                "duration_secs": 10.0,
                "manifest_dir": "/ws/quorum-core",
            }),
            json.dumps({
                "crate_name": "quorum", "is_test": True,
                "duration_secs": 24.0,
                "manifest_dir": "/ws/quorum",
            }),
            # Non-test build of same crate — must be ignored.
            json.dumps({
                "crate_name": "quorum_core", "is_test": False,
                "duration_secs": 5.0,
                "manifest_dir": "/ws/quorum-core",
            }),
            json.dumps({
                "crate_name": "cli_serve_config", "is_test": True,
                "duration_secs": 27.0,
                "manifest_dir": "/ws/quorum",
            }),
            # Duplicate test invocation — must sum.
            json.dumps({
                "crate_name": "cli_serve_config", "is_test": True,
                "duration_secs": 0.5,
                "manifest_dir": "/ws/quorum",
            }),
            # Reviewer's hyphenated case: rustc key `fake_agent` from a
            # target Cargo advertises as `fake-agent`.
            json.dumps({
                "crate_name": "fake_agent", "is_test": True,
                "duration_secs": 3.25,
                "manifest_dir": "/ws/quorum",
            }),
            # Same target name in a different package — manifest_dir wins.
            json.dumps({
                "crate_name": "shared_name", "is_test": True,
                "duration_secs": 7.0,
                "manifest_dir": "/ws/pkg_a",
            }),
            json.dumps({
                "crate_name": "shared_name", "is_test": True,
                "duration_secs": 11.0,
                "manifest_dir": "/ws/pkg_b",
            }),
            # Legacy record — no manifest_dir; name-only fallback.
            json.dumps({
                "crate_name": "legacy_bin", "is_test": True,
                "duration_secs": 4.5,
            }),
            "not-json",
            "",
        ]) + "\n")
        binaries = [
            {"target_name": "quorum_core", "executable": "/x",
             "target_kinds": ["lib"], "fresh": False,
             "manifest_path": "/ws/quorum-core/Cargo.toml"},
            {"target_name": "quorum", "executable": "/y",
             "target_kinds": ["bin"], "fresh": False,
             "manifest_path": "/ws/quorum/Cargo.toml"},
            {"target_name": "cli_serve_config", "executable": "/z",
             "target_kinds": ["test"], "fresh": False,
             "manifest_path": "/ws/quorum/Cargo.toml"},
            {"target_name": "unmatched_bin", "executable": "/w",
             "target_kinds": ["test"], "fresh": False,
             "manifest_path": "/ws/quorum/Cargo.toml"},
            {"target_name": "cached_bin", "executable": "/v",
             "target_kinds": ["test"], "fresh": True,
             "manifest_path": "/ws/quorum/Cargo.toml"},
            # Hyphenated cargo target name — must match `fake_agent`.
            {"target_name": "fake-agent", "executable": "/fa",
             "target_kinds": ["bin"], "fresh": False,
             "manifest_path": "/ws/quorum/Cargo.toml"},
            # Same target name, different packages.
            {"target_name": "shared_name", "executable": "/sa",
             "target_kinds": ["test"], "fresh": False,
             "manifest_path": "/ws/pkg_a/Cargo.toml"},
            {"target_name": "shared_name", "executable": "/sb",
             "target_kinds": ["test"], "fresh": False,
             "manifest_path": "/ws/pkg_b/Cargo.toml"},
            # Legacy fallback — no manifest_path on either side works too;
            # here we set one on the binary but wrapper record has none.
            {"target_name": "legacy_bin", "executable": "/lb",
             "target_kinds": ["test"], "fresh": False,
             "manifest_path": "/ws/legacy/Cargo.toml"},
        ]
        matched, entries = correlate_compile_times(log, binaries)
        assert matched == 7, matched
        assert entries == 9, entries
        assert binaries[0]["compile_no_run_secs"] == 10.0
        assert binaries[0]["compile_no_run_source"] == "rustc_wrapper"
        assert binaries[1]["compile_no_run_secs"] == 24.0
        assert binaries[2]["compile_no_run_secs"] == 27.5, \
            binaries[2]["compile_no_run_secs"]
        assert binaries[3]["compile_no_run_secs"] is None
        assert binaries[3]["compile_no_run_source"] == "unmatched"
        assert binaries[4]["compile_no_run_secs"] == 0.0
        assert binaries[4]["compile_no_run_source"] == "cached_fresh"
        # Hyphenated target matched via normalization.
        assert binaries[5]["compile_no_run_secs"] == 3.25, \
            binaries[5]["compile_no_run_secs"]
        assert binaries[5]["compile_no_run_source"] == "rustc_wrapper"
        # Same-name-different-package disambiguation.
        assert binaries[6]["compile_no_run_secs"] == 7.0
        assert binaries[7]["compile_no_run_secs"] == 11.0
        # Legacy fallback (wrapper record without manifest_dir).
        assert binaries[8]["compile_no_run_secs"] == 4.5
        assert binaries[8]["compile_no_run_source"] == "rustc_wrapper"

        # Missing log file → all unmatched, no crash.
        binaries2 = [
            {"target_name": "x", "executable": "/x", "target_kinds": []}
        ]
        matched2, entries2 = correlate_compile_times(
            out / "no-such-log.jsonl", binaries2
        )
        assert matched2 == 0 and entries2 == 0
        assert binaries2[0]["compile_no_run_secs"] is None

    # ---- (1) artifact + bounded top-N ----
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        binaries = [
            {
                "package_id": "p1",
                "target_name": f"bin_{i:02d}",
                "target_kinds": ["lib"],
                "executable": f"/tmp/bin_{i:02d}",
                "compile_no_run_secs": float(i),
                "compile_no_run_source": "rustc_wrapper",
                "execute_secs": float(20 - i),
                "execute_exit_code": 0,
            }
            for i in range(20)
        ]
        data = {
            "version": 2,
            "timestamp_utc": "2026-08-14T00:00:00Z",
            "top_n": 5,
            "test_threads": 4,
            "gates": [
                {"name": "cargo_fmt", "duration_secs": 1.2, "exit_code": 0},
                {"name": "cargo_clippy", "duration_secs": 45.6,
                 "exit_code": 0},
            ],
            "test_binaries": binaries,
            "top_n_slowest": slowest(binaries, 5),
            "rustc_wrapper": {"matched": 20, "log_entries": 200,
                              "log_path": str(out / "rustc.jsonl")},
        }
        artifact = out / "timing.json"
        summary = out / "summary.txt"
        emit_artifact(artifact, data)

        parsed = json.loads(artifact.read_text())
        assert parsed["version"] == 2
        assert len(parsed["test_binaries"]) == 20
        assert len(parsed["top_n_slowest"]) == 5

        emit_summary(summary, data, top_n=5)
        rows = [
            ln for ln in summary.read_text().splitlines()
            if ln.startswith("  bin_")
        ]
        assert len(rows) == 5, f"expected 5 rows, got {len(rows)}"

        emit_summary(summary, data, top_n=1000)
        rows = [
            ln for ln in summary.read_text().splitlines()
            if ln.startswith("  bin_")
        ]
        assert len(rows) == 20, f"expected 20 rows, got {len(rows)}"

        emit_summary(summary, {**data, "test_binaries": []}, top_n=10)
        rows = [
            ln for ln in summary.read_text().splitlines()
            if ln.startswith("  bin_")
        ]
        assert rows == []

    print("self-test OK")
    return 0


# ---------------------------------------------------------------------------
# Entry
# ---------------------------------------------------------------------------


def main() -> int:
    if os.environ.get(WRAPPER_ACTIVE_ENV) == "1":
        return _rustc_wrapper()

    p = argparse.ArgumentParser(
        description=(
            "Structured per-gate and per-test-binary timing collector."
        ),
    )
    p.add_argument("--top-n", type=int, default=10)
    p.add_argument("--out", default="target/preflight-timing")
    p.add_argument(
        "--test-threads",
        type=int,
        default=int(os.environ.get("RUST_TEST_THREADS", "4")),
    )
    p.add_argument("--skip-fmt", action="store_true")
    p.add_argument("--skip-clippy", action="store_true")
    p.add_argument(
        "--self-test", action="store_true", dest="self_test_mode"
    )
    args = p.parse_args()

    if args.top_n <= 0:
        p.error("--top-n must be positive")
    if args.test_threads <= 0:
        p.error("--test-threads must be positive")

    if args.self_test_mode:
        return self_test()

    wrapper_path = os.path.abspath(__file__)
    return collect(args, wrapper_path)


if __name__ == "__main__":
    sys.exit(main())
