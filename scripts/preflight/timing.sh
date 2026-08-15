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
                                [--test-timeout-secs N]
                                [--term-grace-secs N]
                                [--skip-fmt] [--skip-clippy] [--self-test]

Defaults: --top-n 10, --out target/preflight-timing,
          --test-threads $RUST_TEST_THREADS or 4,
          --test-timeout-secs $PREFLIGHT_TEST_TIMEOUT_SECS or 120,
          --term-grace-secs $PREFLIGHT_TERM_GRACE_SECS or 2.

Exit codes mirror preflight.sh: 0 pass, 1 gate failure, 2 usage; an interrupted
test-execution phase returns 128 plus the received signal number.
"""

from __future__ import annotations

import argparse
import json
import os
import select
import shlex
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

CARGO_FEATURES = ["--all-features", "--features", "quorum-core/test-support"]

WRAPPER_ACTIVE_ENV = "TIMING_RUSTC_WRAPPER_ACTIVE"
WRAPPER_LOG_ENV = "TIMING_RUSTC_LOG"
SUPERVISOR_ACTIVE_ENV = "TIMING_TEST_SUPERVISOR_ACTIVE"
SUPERVISOR_OWNER_FD_ENV = "TIMING_TEST_SUPERVISOR_OWNER_FD"
SUPERVISOR_RESULT_FD_ENV = "TIMING_TEST_SUPERVISOR_RESULT_FD"
SUPERVISOR_ARGV_ENV = "TIMING_TEST_SUPERVISOR_ARGV"
SUPERVISOR_TIMEOUT_ENV = "TIMING_TEST_SUPERVISOR_TIMEOUT"
SUPERVISOR_GRACE_ENV = "TIMING_TEST_SUPERVISOR_GRACE"
SUPERVISOR_DISPLAY_ENV = "TIMING_TEST_SUPERVISOR_DISPLAY"
PROCESS_TABLE_FAULT_ENV = "TIMING_TEST_PROCESS_TABLE_SPAWN_FAILURES"

DEFAULT_TEST_TIMEOUT_SECS = 120.0
DEFAULT_TERM_GRACE_SECS = 2.0
TIMEOUT_EXIT_CODE = 124


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


class CollectorInterrupted(BaseException):
    def __init__(self, signum: int):
        super().__init__(signum)
        self.signum = signum
        self.result: dict | None = None


def _process_group_exists(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _reap_children() -> None:
    """Reap direct children, including Linux subreaper adoptees."""
    while True:
        try:
            pid, _ = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return
        if pid == 0:
            return


def _enable_child_subreaper() -> None:
    """Keep orphaned test descendants reparented here on Linux.

    Other POSIX systems reap orphans through their normal init process. Linux
    lets the test supervisor become the nearest subreaper so killed
    grandchildren do not accumulate as zombies beneath a container's PID 1.
    """
    if not sys.platform.startswith("linux"):
        return
    try:
        import ctypes

        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
            err = ctypes.get_errno()
            raise OSError(err, os.strerror(err))
    except (ImportError, OSError) as exc:
        print(
            f"preflight timing: could not become child subreaper: {exc}",
            file=sys.stderr,
            flush=True,
        )


def _linux_process_table() -> dict[int, tuple[int, int]] | None:
    """Read PID -> (PPID, PGID) without spawning under Linux."""
    proc_root = Path("/proc")
    if not (proc_root / "self/stat").exists():
        return None
    rows: dict[int, tuple[int, int]] = {}
    try:
        entries = list(proc_root.iterdir())
    except OSError:
        return None
    for entry in entries:
        if not entry.name.isdigit():
            continue
        try:
            stat = (entry / "stat").read_text(errors="replace")
            # The command name in field 2 may contain spaces or parentheses.
            # Fields following its final ')' start with state, PPID, and PGID.
            fields = stat[stat.rfind(")") + 2:].split()
            rows[int(entry.name)] = (int(fields[1]), int(fields[2]))
        except (IndexError, OSError, ValueError):
            # Processes may disappear while /proc is being traversed.
            continue
    return rows


def _process_table_failure(
    error: str,
) -> tuple[dict[int, tuple[int, int]], str]:
    rows = _linux_process_table()
    if rows is not None:
        return rows, f"process-tree discovery degraded: {error}; used /proc fallback"
    return {}, error


def _process_table() -> tuple[dict[int, tuple[int, int]], str | None]:
    """Return PID -> (PPID, PGID) from a bounded POSIX process snapshot."""
    try:
        injected_failures = int(os.environ.get(PROCESS_TABLE_FAULT_ENV, "0"))
    except ValueError:
        injected_failures = 0
    try:
        if injected_failures > 0:
            os.environ[PROCESS_TABLE_FAULT_ENV] = str(injected_failures - 1)
            raise OSError("injected process-table spawn failure")
        ps_proc = subprocess.Popen(
            ["ps", "-axo", "pid=,ppid=,pgid="],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except OSError as exc:
        return _process_table_failure(
            f"process-tree discovery could not start: {exc}"
        )
    try:
        stdout, stderr = ps_proc.communicate(timeout=0.5)
    except subprocess.TimeoutExpired:
        ps_proc.kill()
        ps_proc.wait()
        return _process_table_failure("process-tree discovery timed out")
    except OSError as exc:
        try:
            ps_proc.kill()
        except OSError:
            pass
        try:
            ps_proc.wait(timeout=0.5)
        except (OSError, subprocess.TimeoutExpired):
            pass
        return _process_table_failure(
            f"process-tree discovery failed while reading: {exc}"
        )
    if ps_proc.returncode != 0:
        return _process_table_failure(
            f"process-tree discovery failed: {stderr.strip()}"
        )

    rows: dict[int, tuple[int, int]] = {}
    try:
        for line in stdout.splitlines():
            pid, ppid, pgid = (int(value) for value in line.split())
            if pid != ps_proc.pid:
                rows[pid] = (ppid, pgid)
    except ValueError as exc:
        return {}, f"invalid process-tree snapshot: {exc}"
    return rows, None


def _discover_descendants(
    root_pid: int, known: set[int]
) -> tuple[dict[int, tuple[int, int]], str | None]:
    rows, error = _process_table()

    # Keep every PID ever observed: a descendant can be reparented after its
    # parent is killed, or can move into a new process group. Linux subreaper
    # adoptees appear as direct supervisor children. The original group is
    # included even if its leader has already exited.
    known.add(root_pid)
    for pid, (ppid, pgid) in rows.items():
        if ppid == os.getpid() or pgid == root_pid:
            known.add(pid)
    changed = True
    while changed:
        changed = False
        for pid, (ppid, _pgid) in rows.items():
            if pid not in known and ppid in known:
                known.add(pid)
                changed = True
    return {pid: rows[pid] for pid in known if pid in rows}, error


def _cleanup_process_tree(
    proc: subprocess.Popen, grace_secs: float
) -> dict:
    """Bounded TERM/KILL cleanup for every descendant of ``proc``."""
    root_pgid = proc.pid
    supervisor_pgid = os.getpgrp()
    known = {proc.pid}
    discovery_error: str | None = None
    discovery_errors: list[str] = []
    signal_errors: list[str] = []
    cleanup = {
        "attempted": False,
        "term_sent": False,
        "kill_sent": False,
        "complete": False,
        "error": None,
    }

    def snapshot() -> dict[int, tuple[int, int]]:
        nonlocal discovery_error
        proc.poll()
        _reap_children()
        live, error = _discover_descendants(proc.pid, known)
        if error is not None and error not in discovery_errors:
            discovery_errors.append(error)
        # A Linux /proc fallback is a complete snapshot, while a later clean
        # ps snapshot proves that a transient startup failure has recovered.
        discovery_error = (
            error
            if error is not None
            and not error.startswith("process-tree discovery degraded:")
            else None
        )
        return live

    def signal_targets(
        live: dict[int, tuple[int, int]],
        sig: signal.Signals,
        key: str,
    ) -> None:
        groups = {
            pgid for _pid, (_ppid, pgid) in live.items()
            if pgid > 0 and pgid != supervisor_pgid
        }
        # Keep the original group as a fallback if discovery raced its leader
        # or failed. Each known PID is also signaled in case it changes groups
        # between the snapshot and killpg.
        groups.add(root_pgid)
        sent = False
        for pgid in groups:
            try:
                os.killpg(pgid, sig)
                sent = True
            except ProcessLookupError:
                pass
            except OSError as exc:
                signal_errors.append(f"killpg({pgid}, {sig.name}): {exc}")
        for pid in live:
            try:
                os.kill(pid, sig)
                sent = True
            except ProcessLookupError:
                pass
            except OSError as exc:
                signal_errors.append(f"kill({pid}, {sig.name}): {exc}")
        cleanup["attempted"] = cleanup["attempted"] or sent
        cleanup[key] = cleanup[key] or sent

    def send(sig: signal.Signals, key: str) -> None:
        signal_targets(snapshot(), sig, key)

    def wait_until_gone(
        deadline: float, sig: signal.Signals, key: str
    ) -> bool:
        while True:
            live = snapshot()
            if (
                not live
                and not _process_group_exists(root_pgid)
                and discovery_error is None
            ):
                return True
            if now() >= deadline:
                return False
            # Close process-creation and process-group-change races by applying
            # the current cleanup stage to every newly discovered survivor.
            if live:
                signal_targets(live, sig, key)
            time.sleep(min(0.02, max(0.0, deadline - now())))

    live = snapshot()
    if live or _process_group_exists(root_pgid):
        send(signal.SIGTERM, "term_sent")
        gone = wait_until_gone(
            now() + grace_secs, signal.SIGTERM, "term_sent"
        )
        if not gone:
            send(signal.SIGKILL, "kill_sent")
            gone = wait_until_gone(
                now() + grace_secs, signal.SIGKILL, "kill_sent"
            )
    else:
        gone = True

    if proc.poll() is None:
        try:
            proc.wait(timeout=max(grace_secs, 0.01))
        except subprocess.TimeoutExpired:
            gone = False
            cleanup["error"] = cleanup["error"] or (
                f"group leader {proc.pid} did not exit after SIGKILL"
            )
    _reap_children()
    final_live = snapshot()
    cleanup["complete"] = (
        gone
        and not final_live
        and not _process_group_exists(root_pgid)
        and discovery_error is None
    )
    if not cleanup["complete"]:
        survivors = sorted(final_live)
        if discovery_error is not None:
            cleanup["error"] = discovery_error
        elif survivors:
            cleanup["error"] = (
                f"descendant processes still exist after SIGKILL: {survivors}"
            )
        elif signal_errors:
            cleanup["error"] = signal_errors[-1]
        elif cleanup["error"] is not None:
            pass
        else:
            cleanup["error"] = (
                f"process group {root_pgid} still exists after SIGKILL"
            )
    elif discovery_errors:
        cleanup["error"] = discovery_errors[-1]
    return cleanup


def _run_result(
    started: float,
    rc: int,
    outcome: str,
    timeout_secs: float,
    cleanup: dict,
) -> dict:
    return {
        "duration_secs": now() - started,
        "exit_code": rc,
        "outcome": outcome,
        "timed_out": outcome == "timed_out",
        "timeout_secs": timeout_secs,
        "cleanup": cleanup,
    }


def _cleanup_diagnostic(cleanup: dict) -> str:
    actions = []
    if cleanup["term_sent"]:
        actions.append("SIGTERM sent")
    if cleanup["kill_sent"]:
        actions.append("SIGKILL sent")
    actions.append(
        "descendant tree reaped" if cleanup["complete"]
        else f"cleanup incomplete: {cleanup['error']}"
    )
    if cleanup["complete"] and cleanup["error"] is not None:
        actions.append(f"discovery diagnostic: {cleanup['error']}")
    return ", ".join(actions)


class SupervisorInterrupted(BaseException):
    def __init__(self, signum: int):
        super().__init__(signum)
        self.signum = signum


def _write_supervisor_result(fd: int, result: dict) -> None:
    payload = (json.dumps(result, sort_keys=True) + "\n").encode()
    try:
        while payload:
            written = os.write(fd, payload)
            payload = payload[written:]
    except (BrokenPipeError, OSError):
        # Abrupt owner death normally removes the result reader. Cleanup is
        # still complete, so the supervisor can exit without reporting back.
        pass


def _test_supervisor() -> int:
    """Own one isolated test group, even if the timing collector is killed."""
    owner_fd = int(os.environ[SUPERVISOR_OWNER_FD_ENV])
    result_fd = int(os.environ[SUPERVISOR_RESULT_FD_ENV])
    argv = json.loads(os.environ[SUPERVISOR_ARGV_ENV])
    timeout_secs = float(os.environ[SUPERVISOR_TIMEOUT_ENV])
    grace_secs = float(os.environ[SUPERVISOR_GRACE_ENV])
    display_name = os.environ[SUPERVISOR_DISPLAY_ENV]
    started = now()
    proc: subprocess.Popen | None = None
    result: dict | None = None

    def interrupted(signum: int, _frame: object) -> None:
        raise SupervisorInterrupted(signum)

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, interrupted)

    try:
        _enable_child_subreaper()
        old_mask = signal.pthread_sigmask(
            signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM}
        )

        def restore_child_signal_mask() -> None:
            signal.pthread_sigmask(signal.SIG_SETMASK, old_mask)

        test_env = os.environ.copy()
        for key in (
            SUPERVISOR_ACTIVE_ENV,
            SUPERVISOR_OWNER_FD_ENV,
            SUPERVISOR_RESULT_FD_ENV,
            SUPERVISOR_ARGV_ENV,
            SUPERVISOR_TIMEOUT_ENV,
            SUPERVISOR_GRACE_ENV,
            SUPERVISOR_DISPLAY_ENV,
        ):
            test_env.pop(key, None)
        try:
            proc = subprocess.Popen(
                argv,
                env=test_env,
                start_new_session=True,
                preexec_fn=restore_child_signal_mask,
            )
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, old_mask)

        deadline = started + timeout_secs
        outcome = "failed"
        rc = 1
        while True:
            polled = proc.poll()
            if polled is not None:
                rc = polled
                outcome = "passed" if rc == 0 else "failed"
                break
            remaining = deadline - now()
            if remaining <= 0:
                rc = TIMEOUT_EXIT_CODE
                outcome = "timed_out"
                break
            readable, _, _ = select.select(
                [owner_fd], [], [], min(remaining, 0.05)
            )
            if readable and os.read(owner_fd, 1) == b"":
                rc = 1
                outcome = "owner_lost"
                break

        cleanup = _cleanup_process_tree(proc, grace_secs)
        result = _run_result(started, rc, outcome, timeout_secs, cleanup)
        if outcome == "timed_out":
            print(
                f"preflight timing: test binary {display_name!r} timed out "
                f"after {result['duration_secs']:.2f}s "
                f"(deadline {timeout_secs:.2f}s); "
                f"{_cleanup_diagnostic(cleanup)}. Re-run "
                f"{shlex.join([*argv, '--nocapture'])} to diagnose.",
                file=sys.stderr,
                flush=True,
            )
        elif outcome != "owner_lost" and not cleanup["complete"]:
            print(
                f"preflight timing: test binary {display_name!r} exited "
                f"but {_cleanup_diagnostic(cleanup)}",
                file=sys.stderr,
                flush=True,
            )
    except SupervisorInterrupted as exc:
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, signal.SIG_IGN)
        cleanup = (
            _cleanup_process_tree(proc, grace_secs)
            if proc is not None
            else {
                "attempted": False,
                "term_sent": False,
                "kill_sent": False,
                "complete": True,
                "error": None,
            }
        )
        result = _run_result(
            started, 128 + exc.signum, "interrupted", timeout_secs, cleanup
        )
    except BaseException as exc:
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, signal.SIG_IGN)
        cleanup = (
            _cleanup_process_tree(proc, grace_secs)
            if proc is not None
            else {
                "attempted": False,
                "term_sent": False,
                "kill_sent": False,
                "complete": True,
                "error": None,
            }
        )
        cleanup["error"] = cleanup["error"] or f"supervisor failure: {exc}"
        result = _run_result(started, 1, "failed", timeout_secs, cleanup)
    finally:
        if result is not None:
            _write_supervisor_result(result_fd, result)
        os.close(owner_fd)
        os.close(result_fd)
    return 0


def _read_supervisor_result(fd: int) -> dict:
    payload = bytearray()
    while True:
        chunk = os.read(fd, 4096)
        if not chunk:
            break
        payload.extend(chunk)
    if not payload:
        raise RuntimeError("test supervisor exited without a result")
    return json.loads(payload)


def run_test_binary(
    exe: str,
    threads: int,
    timeout_secs: float,
    term_grace_secs: float,
    display_name: str,
) -> dict:
    argv = [exe, "--test-threads", str(threads)]
    t0 = now()
    proc: subprocess.Popen | None = None
    owner_read, owner_write = os.pipe()
    result_read, result_write = os.pipe()
    previous_handlers: dict[signal.Signals, object] = {}

    def interrupted(signum: int, _frame: object) -> None:
        raise CollectorInterrupted(signum)

    for sig in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[sig] = signal.signal(sig, interrupted)

    try:
        env = os.environ.copy()
        env.update({
            SUPERVISOR_ACTIVE_ENV: "1",
            SUPERVISOR_OWNER_FD_ENV: str(owner_read),
            SUPERVISOR_RESULT_FD_ENV: str(result_write),
            SUPERVISOR_ARGV_ENV: json.dumps(argv),
            SUPERVISOR_TIMEOUT_ENV: str(timeout_secs),
            SUPERVISOR_GRACE_ENV: str(term_grace_secs),
            SUPERVISOR_DISPLAY_ENV: display_name,
        })

        # The supervisor leaves the daemon-owned worker group before it starts
        # the test. If the collector is SIGKILLed, EOF on owner_read tells the
        # independently live supervisor to clean and reap the test group.
        old_mask = signal.pthread_sigmask(
            signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM}
        )

        def restore_child_signal_mask() -> None:
            signal.pthread_sigmask(signal.SIG_SETMASK, old_mask)

        try:
            proc = subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve())],
                env=env,
                pass_fds=(owner_read, result_write),
                start_new_session=True,
                preexec_fn=restore_child_signal_mask,
            )
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, old_mask)
        os.close(owner_read)
        owner_read = -1
        os.close(result_write)
        result_write = -1
        proc.wait()
        return _read_supervisor_result(result_read)
    except CollectorInterrupted as exc:
        for sig in previous_handlers:
            signal.signal(sig, signal.SIG_IGN)
        if owner_read >= 0:
            os.close(owner_read)
            owner_read = -1
        if result_write >= 0:
            os.close(result_write)
            result_write = -1
        os.close(owner_write)
        owner_write = -1
        if proc is not None:
            proc.wait(timeout=(2 * term_grace_secs) + 2)
            result = _read_supervisor_result(result_read)
        else:
            result = _run_result(
                t0,
                128 + exc.signum,
                "interrupted",
                timeout_secs,
                {
                    "attempted": False,
                    "term_sent": False,
                    "kill_sent": False,
                    "complete": True,
                    "error": None,
                },
            )
        result["exit_code"] = 128 + exc.signum
        result["outcome"] = "interrupted"
        result["timed_out"] = False
        exc.result = result
        print(
            f"preflight timing: interrupted while running test binary "
            f"{display_name!r} after {exc.result['duration_secs']:.2f}s; "
            f"{_cleanup_diagnostic(result['cleanup'])}",
            file=sys.stderr,
            flush=True,
        )
        raise
    except BaseException:
        for sig in previous_handlers:
            signal.signal(sig, signal.SIG_IGN)
        if owner_write >= 0:
            os.close(owner_write)
            owner_write = -1
        if proc is not None and proc.poll() is None:
            try:
                proc.wait(timeout=(2 * term_grace_secs) + 2)
            except subprocess.TimeoutExpired:
                pass
        raise
    finally:
        for fd in (owner_read, owner_write, result_read, result_write):
            if fd >= 0:
                os.close(fd)
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)


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
    lines.append(
        f"test_timeout_secs: {data.get('test_timeout_secs', 'n/a')}"
    )
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
        f"  {'binary':<40} {'compile_no_run':>16} {'execute':>12}  outcome"
    )
    for b in top:
        name = b.get("target_name") or Path(b["executable"]).name
        c = b.get("compile_no_run_secs")
        e = b.get("execute_secs") or 0.0
        c_str = f"{c:>14.2f}s" if c is not None else f"{'n/a':>15}"
        outcome = b.get("execute_outcome", "unknown")
        lines.append(
            f"  {name[:40]:<40} {c_str} {e:>10.2f}s  {outcome}"
        )
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
    interrupted_signal: int | None = None

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
            f"(--test-threads {args.test_threads}, "
            f"{args.test_timeout_secs:g}s deadline each) ===",
            flush=True,
        )
        t0 = now()
        exec_rc = 0
        for b in binaries:
            name = b.get("target_name") or Path(b["executable"]).name
            try:
                result = run_test_binary(
                    b["executable"],
                    args.test_threads,
                    args.test_timeout_secs,
                    args.term_grace_secs,
                    name,
                )
            except CollectorInterrupted as exc:
                assert exc.result is not None
                result = exc.result
                interrupted_signal = exc.signum
            b["execute_secs"] = round(result["duration_secs"], 3)
            b["execute_exit_code"] = result["exit_code"]
            b["execute_outcome"] = result["outcome"]
            b["execute_timed_out"] = result["timed_out"]
            b["execute_timeout_secs"] = result["timeout_secs"]
            b["cleanup"] = result["cleanup"]
            if (
                result["exit_code"] != 0
                or not result["cleanup"]["complete"]
            ) and exec_rc == 0:
                exec_rc = result["exit_code"] or 1
            if interrupted_signal is not None:
                break
        add_gate("test_execute", now() - t0, exec_rc)
        if interrupted_signal is not None:
            status = 128 + interrupted_signal

    data = {
        "version": 3,
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "top_n": args.top_n,
        "test_threads": args.test_threads,
        "test_timeout_secs": args.test_timeout_secs,
        "term_grace_secs": args.term_grace_secs,
        "interrupted_signal": interrupted_signal,
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
            "version": 3,
            "timestamp_utc": "2026-08-14T00:00:00Z",
            "top_n": 5,
            "test_threads": 4,
            "test_timeout_secs": 120.0,
            "term_grace_secs": 2.0,
            "interrupted_signal": None,
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
        assert parsed["version"] == 3
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
    if os.environ.get(SUPERVISOR_ACTIVE_ENV) == "1":
        return _test_supervisor()

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
    p.add_argument(
        "--test-timeout-secs",
        type=float,
        default=float(os.environ.get(
            "PREFLIGHT_TEST_TIMEOUT_SECS", DEFAULT_TEST_TIMEOUT_SECS
        )),
        help="deadline for each test executable (default: 120 seconds)",
    )
    p.add_argument(
        "--term-grace-secs",
        type=float,
        default=float(os.environ.get(
            "PREFLIGHT_TERM_GRACE_SECS", DEFAULT_TERM_GRACE_SECS
        )),
        help="grace after TERM and KILL while reaping a test process group",
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
    if args.test_timeout_secs <= 0:
        p.error("--test-timeout-secs must be positive")
    if args.term_grace_secs <= 0:
        p.error("--term-grace-secs must be positive")

    if args.self_test_mode:
        return self_test()

    wrapper_path = os.path.abspath(__file__)
    return collect(args, wrapper_path)


if __name__ == "__main__":
    sys.exit(main())
