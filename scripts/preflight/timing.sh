#!/usr/bin/env python3
"""Structured per-gate and per-test-binary timing collector.

Runs the preflight test gates (fmt, clippy, test compile, test execute) and,
for each test executable, records compile/no-run and execution durations by
reading ``cargo --message-format=json`` rather than parsing human prose.

Writes both a machine-readable JSON artifact (``timing.json``) and a
human-readable text summary (``summary.txt``) to a deterministic local path
under ``target/preflight-timing/`` suitable for CI artifact upload. The
summary includes a bounded top-N list of the slowest test binaries from the
single run.

Per-binary compile/no-run attribution is derived from the wall-clock gap
between successive ``compiler-artifact`` messages in the stream. Cargo compiles
units in parallel, so this gap approximates emission cadence rather than
exclusive CPU time for a single unit; the identity of each test binary,
however, comes verbatim from the structured message. Per-binary execution time
is measured by invoking each test executable directly (bypassing cargo) and
wall-clocking its run.

The extra work versus a plain preflight is: (a) parsing the JSON stream
already emitted by ``--message-format=json`` (cheap, O(#artifact messages));
and (b) invoking each test binary in its own process, which cargo already
does by default, so overhead is limited to shell/fork setup per binary.

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


def now() -> float:
    return time.monotonic()


def run_gate(argv: list[str]) -> tuple[float, int]:
    t0 = now()
    proc = subprocess.run(argv)
    return now() - t0, proc.returncode


def compile_tests(
    compile_log: Path, stderr_log: Path
) -> tuple[float, int, list[dict]]:
    """Run ``cargo test --no-run --message-format=json`` and derive per-binary
    compile/no-run durations from the emission gap between successive
    ``compiler-artifact`` messages. Also mirrors the raw stream to
    ``compile_log`` with a monotonic timestamp per line so the run is
    reproducible offline.
    """
    argv = [
        "cargo", "test", "--no-run", "--message-format=json",
        "--workspace", *CARGO_FEATURES,
    ]
    binaries: list[dict] = []
    t0 = now()
    with compile_log.open("w") as clog, stderr_log.open("w") as elog:
        proc = subprocess.Popen(
            argv, stdout=subprocess.PIPE, stderr=elog, text=True
        )
        assert proc.stdout is not None
        prev_ts = t0
        for line in proc.stdout:
            ts = now()
            clog.write(f"{ts:.6f} {line}")
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
                "target_name": target.get("name"),
                "target_kinds": list(target.get("kind") or []),
                "executable": executable,
                "compile_no_run_secs": round(ts - prev_ts, 3),
                "artifact_emitted_at_secs": round(ts - t0, 3),
            })
            prev_ts = ts
        proc.wait()
    return now() - t0, proc.returncode, binaries


def run_test_binary(exe: str, threads: int) -> tuple[float, int]:
    argv = [exe, "--test-threads", str(threads)]
    t0 = now()
    proc = subprocess.run(argv)
    return now() - t0, proc.returncode


def slowest(binaries: list[dict], top_n: int) -> list[dict]:
    return sorted(
        binaries,
        key=lambda b: (
            (b.get("execute_secs") or 0.0)
            + (b.get("compile_no_run_secs") or 0.0)
        ),
        reverse=True,
    )[:top_n]


def emit_artifact(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    # Round-trip parse guards against silent corruption from future edits.
    json.loads(path.read_text())


def emit_summary(path: Path, data: dict, top_n: int) -> None:
    lines: list[str] = []
    lines.append("=== preflight timing summary ===")
    lines.append(f"timestamp_utc: {data['timestamp_utc']}")
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
        c = b.get("compile_no_run_secs") or 0.0
        e = b.get("execute_secs") or 0.0
        lines.append(
            f"  {name[:48]:<48} {c:>14.2f}s {e:>10.2f}s"
        )
    lines.append("")
    path.write_text("\n".join(lines) + "\n")


def collect(args: argparse.Namespace) -> int:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    artifact = out / "timing.json"
    summary = out / "summary.txt"
    compile_log = out / "cargo-test-no-run.jsonl"
    stderr_log = out / "cargo-test-no-run.stderr"

    gates: list[dict] = []
    binaries: list[dict] = []
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
            "=== timing 2/4: cargo clippy --all-targets "
            "--all-features --features quorum-core/test-support "
            "-- -D warnings ===",
            flush=True,
        )
        dur, rc = run_gate([
            "cargo", "clippy", "--all-targets", *CARGO_FEATURES,
            "--", "-D", "warnings",
        ])
        add_gate("cargo_clippy", dur, rc)

    if status == 0:
        print(
            "=== timing 3/4: cargo test --no-run "
            "--message-format=json --workspace ===",
            flush=True,
        )
        dur, rc, binaries = compile_tests(compile_log, stderr_log)
        add_gate("cargo_test_no_run", dur, rc)

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
        "version": 1,
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "top_n": args.top_n,
        "test_threads": args.test_threads,
        "gates": gates,
        "test_binaries": binaries,
        "top_n_slowest": slowest(binaries, args.top_n),
    }
    emit_artifact(artifact, data)
    emit_summary(summary, data, args.top_n)

    tag = "PASS" if status == 0 else "FAIL"
    print(f"\nPREFLIGHT TIMING: {tag} — {artifact} / {summary}")
    return status


def self_test() -> int:
    """Synthetic-fixture check — no cargo required. Confirms the artifact is
    valid JSON and the top-N list in the summary is bounded by N (or by the
    binary count, whichever is smaller)."""
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        binaries = [
            {
                "package_id": "p1",
                "target_name": f"bin_{i:02d}",
                "target_kinds": ["lib"],
                "executable": f"/tmp/bin_{i:02d}",
                "compile_no_run_secs": float(i),
                "artifact_emitted_at_secs": float(i),
                "execute_secs": float(20 - i),
                "execute_exit_code": 0,
            }
            for i in range(20)
        ]
        data = {
            "version": 1,
            "timestamp_utc": "2026-08-14T00:00:00Z",
            "top_n": 5,
            "test_threads": 4,
            "gates": [
                {"name": "cargo_fmt", "duration_secs": 1.2, "exit_code": 0},
                {
                    "name": "cargo_clippy",
                    "duration_secs": 45.6,
                    "exit_code": 0,
                },
            ],
            "test_binaries": binaries,
            "top_n_slowest": slowest(binaries, 5),
        }
        artifact = out / "timing.json"
        summary = out / "summary.txt"
        emit_artifact(artifact, data)

        parsed = json.loads(artifact.read_text())
        assert parsed["version"] == 1, parsed
        assert len(parsed["test_binaries"]) == 20
        assert len(parsed["top_n_slowest"]) == 5

        emit_summary(summary, data, top_n=5)
        rows = [
            ln for ln in summary.read_text().splitlines()
            if ln.startswith("  bin_")
        ]
        assert len(rows) == 5, f"expected 5 rows, got {len(rows)}: {rows}"

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
        assert rows == [], f"expected 0 rows for empty binaries, got {rows}"

        print("self-test OK")
    return 0


def main() -> int:
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
    return collect(args)


if __name__ == "__main__":
    sys.exit(main())
