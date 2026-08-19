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
``TIMING_RUSTC_WRAPPER_ACTIVE`` env var) so it wall-clocks each invocation.
When Cargo already has a wrapper such as ``sccache``, the timing wrapper chains
it as the inner command instead of replacing it. The collector then correlates
each entry to a compiler-artifact message by ``(--crate-name, --test)``.
Execution time is measured by invoking each test executable directly and
wall-clocking its run.

Outputs (under ``target/preflight-timing/`` by default):
  timing.json                  — machine-readable artifact
  summary.txt                  — human-readable summary + bounded top-N
  cargo-test-no-run.jsonl      — raw cargo JSON stream
  cargo-test-no-run.stderr     — cargo stderr
  rustc-invocations.jsonl      — per-rustc-invocation timing log

Usage:
    scripts/preflight/timing.sh [--top-n N] [--out DIR] [--test-jobs N]
                                [--test-threads N]
                                [--test-timeout-secs N]
                                [--term-grace-secs N]
                                [--skip-fmt] [--skip-clippy] [--self-test]

Defaults: --top-n 10, --out target/preflight-timing,
          --test-jobs $PREFLIGHT_TEST_JOBS or 2,
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
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

CARGO_FEATURES = ["--all-features", "--features", "quorum-core/test-support"]

WRAPPER_ACTIVE_ENV = "TIMING_RUSTC_WRAPPER_ACTIVE"
WRAPPER_LOG_ENV = "TIMING_RUSTC_LOG"
WRAPPER_CHAIN_ENV = "TIMING_RUSTC_INNER_WRAPPER"
SUPERVISOR_ACTIVE_ENV = "TIMING_TEST_SUPERVISOR_ACTIVE"
SUPERVISOR_OWNER_FD_ENV = "TIMING_TEST_SUPERVISOR_OWNER_FD"
SUPERVISOR_RESULT_FD_ENV = "TIMING_TEST_SUPERVISOR_RESULT_FD"
SUPERVISOR_ARGV_ENV = "TIMING_TEST_SUPERVISOR_ARGV"
SUPERVISOR_TIMEOUT_ENV = "TIMING_TEST_SUPERVISOR_TIMEOUT"
SUPERVISOR_GRACE_ENV = "TIMING_TEST_SUPERVISOR_GRACE"
SUPERVISOR_DISPLAY_ENV = "TIMING_TEST_SUPERVISOR_DISPLAY"
PROCESS_TABLE_FAULT_ENV = "TIMING_TEST_PROCESS_TABLE_SPAWN_FAILURES"
DARWIN_PARTIAL_PID_FILE_ENV = "TIMING_TEST_DARWIN_PARTIAL_PID_FILE"
DARWIN_PARTIAL_CHILD_LIST_PID_FILE_ENV = (
    "TIMING_TEST_DARWIN_PARTIAL_CHILD_LIST_PID_FILE"
)
DARWIN_REUSED_PID_FILE_ENV = "TIMING_TEST_DARWIN_REUSED_PID_FILE"
DARWIN_REUSED_ROOT_PID_FILE_ENV = "TIMING_TEST_DARWIN_REUSED_ROOT_PID_FILE"

DEFAULT_TEST_TIMEOUT_SECS = 120.0
DEFAULT_TERM_GRACE_SECS = 2.0
TIMEOUT_EXIT_CODE = 124

ProcessStart = tuple[int, int]
RetainedProcesses = dict[int, ProcessStart | None]


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


def _same_executable(left: str, right: str) -> bool:
    """Best-effort recursion guard for two executable path spellings."""
    left_path = shutil.which(left) or left
    right_path = shutil.which(right) or right
    return os.path.realpath(left_path) == os.path.realpath(right_path)


def _install_timing_wrapper(env: dict[str, str], wrapper_path: str) -> str | None:
    """Install the timing wrapper without discarding Cargo's cache wrapper.

    ``RUSTC_WRAPPER`` takes precedence over ``CARGO_BUILD_RUSTC_WRAPPER``.
    Preserve that ordering, including the documented empty-string reset, and
    pass the inherited wrapper to timing-wrapper mode as its inner command.
    """
    if "RUSTC_WRAPPER" in env:
        inherited = env["RUSTC_WRAPPER"]
    else:
        inherited = env.get("CARGO_BUILD_RUSTC_WRAPPER", "")

    env.pop(WRAPPER_CHAIN_ENV, None)
    chained = inherited or None
    if chained and not _same_executable(chained, wrapper_path):
        env[WRAPPER_CHAIN_ENV] = chained
    else:
        chained = None
    env["RUSTC_WRAPPER"] = wrapper_path
    return chained


def _rustc_wrapper() -> int:
    """Fork/exec the inherited wrapper (or rustc), and time the invocation.

    Single ``f.write()`` on an <PIPE_BUF payload is atomic on POSIX, so
    concurrent wrappers may safely share the log file.
    """
    rustc_argv = sys.argv[1:]
    if not rustc_argv:
        return 2
    crate_name, is_test, crate_types = _parse_rustc_argv(rustc_argv)
    inherited = os.environ.get(WRAPPER_CHAIN_ENV)
    argv = [inherited, *rustc_argv] if inherited else rustc_argv

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
) -> tuple[float, int, list[dict], int, int, str | None]:
    """Run ``cargo test --no-run --message-format=json`` with the RUSTC_WRAPPER
    active. Enumerate test binaries from ``compiler-artifact`` messages, then
    correlate exact compile intervals from the wrapper log.
    """
    rustc_log.write_text("")
    env = os.environ.copy()
    chained_wrapper = _install_timing_wrapper(env, wrapper_path)
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
    return (
        now() - t0,
        proc.returncode,
        binaries,
        matched,
        log_entries,
        chained_wrapper,
    )


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


def _reap_adoptees(leader_pid: int) -> None:
    """Reap exited subreaper adoptees while the test binary still runs.

    A zombie remains a member of its process group, so an unreaped adoptee
    keeps ``killpg(pgid, 0)`` succeeding and a test polling for its own
    descendant group's disappearance never observes it. Peek with ``WNOWAIT``
    and skip the group leader so its exit status is never stolen from
    ``Popen``; ``poll()`` maps a stolen status to a fabricated success.
    """
    if not sys.platform.startswith("linux"):
        # Adoptees exist only under the Linux subreaper; elsewhere the sole
        # supervisor child is the leader, which Popen owns.
        return
    while True:
        try:
            info = os.waitid(
                os.P_ALL, 0, os.WEXITED | os.WNOHANG | os.WNOWAIT
            )
        except (ChildProcessError, OSError):
            return
        if info is None or info.si_pid == leader_pid:
            return
        try:
            os.waitpid(info.si_pid, os.WNOHANG)
        except (ChildProcessError, OSError):
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


def _darwin_process_table(
    root_pid: int,
    known: RetainedProcesses,
) -> tuple[dict[int, tuple[int, int]] | None, str | None]:
    """Read the retained process tree without spawning under macOS."""
    if sys.platform != "darwin":
        return None, None
    try:
        import ctypes

        class ProcBsdInfo(ctypes.Structure):
            _fields_ = [
                ("pbi_flags", ctypes.c_uint32),
                ("pbi_status", ctypes.c_uint32),
                ("pbi_xstatus", ctypes.c_uint32),
                ("pbi_pid", ctypes.c_uint32),
                ("pbi_ppid", ctypes.c_uint32),
                ("pbi_uid", ctypes.c_uint32),
                ("pbi_gid", ctypes.c_uint32),
                ("pbi_ruid", ctypes.c_uint32),
                ("pbi_rgid", ctypes.c_uint32),
                ("pbi_svuid", ctypes.c_uint32),
                ("pbi_svgid", ctypes.c_uint32),
                ("rfu_1", ctypes.c_uint32),
                ("pbi_comm", ctypes.c_char * 16),
                ("pbi_name", ctypes.c_char * 32),
                ("pbi_nfiles", ctypes.c_uint32),
                ("pbi_pgid", ctypes.c_uint32),
                ("pbi_pjobc", ctypes.c_uint32),
                ("e_tdev", ctypes.c_uint32),
                ("e_tpgid", ctypes.c_uint32),
                ("pbi_nice", ctypes.c_int32),
                ("pbi_start_tvsec", ctypes.c_uint64),
                ("pbi_start_tvusec", ctypes.c_uint64),
            ]

        libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        libproc.proc_listchildpids.argtypes = [
            ctypes.c_int,
            ctypes.c_void_p,
            ctypes.c_int,
        ]
        libproc.proc_listchildpids.restype = ctypes.c_int
        libproc.proc_pidinfo.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_uint64,
            ctypes.c_void_p,
            ctypes.c_int,
        ]
        libproc.proc_pidinfo.restype = ctypes.c_int

        partial_pid: int | None = None
        partial_pid_file = os.environ.get(DARWIN_PARTIAL_PID_FILE_ENV)
        if partial_pid_file:
            try:
                partial_pid = int(Path(partial_pid_file).read_text().strip())
            except (OSError, ValueError):
                pass
        partial_child_list_pid: int | None = None
        partial_child_list_pid_file = os.environ.get(
            DARWIN_PARTIAL_CHILD_LIST_PID_FILE_ENV
        )
        if partial_child_list_pid_file:
            try:
                partial_child_list_pid = int(
                    Path(partial_child_list_pid_file).read_text().strip()
                )
            except (OSError, ValueError):
                pass

        rows: dict[int, tuple[int, int]] = {}
        listed_children: dict[int, set[int]] = {}
        pending = [(pid, None) for pid in known if pid != root_pid]
        if root_pid in known:
            pending.append((root_pid, None))
        visited: set[int] = set()
        while pending:
            pid, discovered_parent = pending.pop()
            if pid <= 0 or pid in visited:
                continue
            visited.add(pid)
            info = ProcBsdInfo()
            copied = libproc.proc_pidinfo(
                pid,
                3,  # PROC_PIDTBSDINFO
                0,
                ctypes.byref(info),
                ctypes.sizeof(info),
            )
            if pid == partial_pid:
                copied = 0
            if copied == ctypes.sizeof(info):
                start = (
                    int(info.pbi_start_tvsec),
                    int(info.pbi_start_tvusec),
                )
                parent_pid = int(info.pbi_ppid)
                retained_start = known.get(pid)
                if retained_start is not None and retained_start != start:
                    # This PID now identifies another process. Do not traverse
                    # it or trust its current process group during cleanup,
                    # unless it was independently rediscovered as a current
                    # child of the retained tree.
                    known.pop(pid, None)
                    if discovered_parent is None or parent_pid != discovered_parent:
                        continue
                elif (
                    pid not in known
                    and discovered_parent is not None
                    and parent_pid != discovered_parent
                ):
                    # The listed child exited and its PID was reused before
                    # its detail snapshot. The replacement is not ours.
                    continue
                known[pid] = start
                rows[pid] = (parent_pid, int(info.pbi_pgid))
            else:
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    # The process exited between the retained PID and detail
                    # snapshots.
                    known.pop(pid, None)
                    continue
                except PermissionError:
                    pass
                return None, f"libproc snapshot incomplete for live PID {pid}"

            count = libproc.proc_listchildpids(pid, None, 0)
            if count < 0:
                return None, f"libproc could not size children of live PID {pid}"
            if count == 0:
                listed_children[pid] = set()
                continue
            capacity = count + 16
            children = (ctypes.c_int * capacity)()
            count = libproc.proc_listchildpids(
                pid, children, ctypes.sizeof(children)
            )
            if pid == partial_child_list_pid:
                count = 0
            if count < 0:
                return None, f"libproc returned partial children for live PID {pid}"
            if count == 0:
                listed_children[pid] = set()
                continue
            if count >= capacity:
                return None, f"libproc children of live PID {pid} exceeded its buffer"
            children_found = {
                child_pid
                for child_pid in children[:min(count, capacity)]
                if child_pid > 0
            }
            listed_children[pid] = children_found
            pending.extend((child_pid, pid) for child_pid in children_found)

        # A zero or shortened child-list result is only distinguishable from
        # a real leaf when it contradicts a retained live child's BSD info.
        # Treat that contradiction as partial instead of certifying the tree.
        for child_pid, (parent_pid, _pgid) in rows.items():
            if (
                child_pid in known
                and parent_pid in listed_children
                and child_pid not in listed_children[parent_pid]
            ):
                return (
                    None,
                    f"libproc child list incomplete for live PID {parent_pid}",
                )
        return rows, None
    except (AttributeError, ImportError, OSError, ValueError):
        return None, "libproc process snapshot failed"


def _process_table_failure(
    error: str,
    root_pid: int,
    known: RetainedProcesses,
) -> tuple[dict[int, tuple[int, int]], str]:
    rows = _linux_process_table()
    if rows is not None:
        return rows, f"process-tree discovery degraded: {error}; used /proc fallback"
    rows, darwin_error = _darwin_process_table(root_pid, known)
    if rows is not None:
        return rows, f"process-tree discovery degraded: {error}; used libproc fallback"
    if darwin_error is not None:
        error = f"{error}; {darwin_error}"
    return {}, error


def _process_table(
    root_pid: int,
    known: RetainedProcesses,
) -> tuple[dict[int, tuple[int, int]], str | None]:
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
            f"process-tree discovery could not start: {exc}", root_pid, known
        )
    try:
        stdout, stderr = ps_proc.communicate(timeout=0.5)
    except subprocess.TimeoutExpired:
        ps_proc.kill()
        ps_proc.wait()
        return _process_table_failure(
            "process-tree discovery timed out", root_pid, known
        )
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
            f"process-tree discovery failed while reading: {exc}",
            root_pid,
            known,
        )
    if ps_proc.returncode != 0:
        return _process_table_failure(
            f"process-tree discovery failed: {stderr.strip()}",
            root_pid,
            known,
        )

    rows: dict[int, tuple[int, int]] = {}
    try:
        for line in stdout.splitlines():
            pid, ppid, pgid = (int(value) for value in line.split())
            if pid != ps_proc.pid:
                rows[pid] = (ppid, pgid)
    except ValueError as exc:
        return {}, f"invalid process-tree snapshot: {exc}"
    if sys.platform == "darwin":
        # A ps row carries no stable process identity. Make libproc's start
        # timestamp authoritative before retained PIDs can reach signaling.
        darwin_rows, darwin_error = _darwin_process_table(root_pid, known)
        if darwin_rows is None:
            return {}, darwin_error or "libproc process snapshot failed"
        return darwin_rows, None
    return rows, None


def _extend_known_descendants(
    root_pid: int,
    known: RetainedProcesses,
    rows: dict[int, tuple[int, int]],
) -> None:
    # Keep the stable identity of every live PID observed: a descendant can be
    # reparented after its parent is killed, or can move into a new process
    # group. Linux subreaper adoptees appear as direct supervisor children.
    # Darwin's libproc traversal adds the root with its start time; do not
    # recreate a root entry after an identity mismatch discarded it.
    if sys.platform != "darwin":
        known.setdefault(root_pid, None)
    for pid, (ppid, pgid) in rows.items():
        if ppid == os.getpid() or pgid == root_pid:
            known.setdefault(pid, None)
    changed = True
    while changed:
        changed = False
        for pid, (ppid, _pgid) in rows.items():
            if pid not in known and ppid in known:
                known[pid] = None
                changed = True


def _discover_descendants(
    root_pid: int, known: RetainedProcesses
) -> tuple[dict[int, tuple[int, int]], str | None]:
    rows, error = _process_table(root_pid, known)
    _extend_known_descendants(root_pid, known, rows)
    return {pid: rows[pid] for pid in known if pid in rows}, error


def _cleanup_process_tree(
    proc: subprocess.Popen,
    grace_secs: float,
    previously_known: RetainedProcesses | None = None,
) -> dict:
    """Bounded TERM/KILL cleanup for every descendant of ``proc``."""
    root_pgid = proc.pid
    supervisor_pgid = os.getpgrp()
    known = dict(previously_known or {})
    root_running = proc.poll() is None
    if (
        sys.platform == "darwin"
        and not root_running
        and proc.pid in known
        and known[proc.pid] is None
    ):
        # Once waitpid has reaped an unidentified leader, its PID can identify
        # an unrelated process. It is no longer safe to discover or signal by
        # that bare number.
        known.pop(proc.pid)
    if previously_known is None or root_running:
        known.setdefault(proc.pid, None)
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
        if (
            sys.platform == "darwin"
            and proc.returncode is not None
            and proc.pid in known
            and known[proc.pid] is None
        ):
            known.pop(proc.pid)
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
        # or failed. On Darwin, require a retained process to remain in the
        # isolated test session before trusting a group from a degraded
        # snapshot; a reused PID cannot join that existing session.
        def in_root_session(pid: int) -> bool:
            try:
                return os.getsid(pid) == root_pgid
            except (OSError, ProcessLookupError):
                return False

        if (
            sys.platform != "darwin"
            or any(pgid == root_pgid for _ppid, pgid in live.values())
            or any(in_root_session(pid) for pid in known)
        ):
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
        target_pids = set(live)
        if discovery_error is not None:
            # A partial snapshot cannot prove whether a retained descendant is
            # still live. Signal its recorded PID so discovery failure cannot
            # orphan an escaped process group when the root group exits.
            if sys.platform == "darwin":
                target_pids.update(pid for pid in known if in_root_session(pid))
            else:
                target_pids.update(known)
        for pid in target_pids:
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
        incomplete = [
            error
            for error in discovery_errors
            if not error.startswith("process-tree discovery degraded:")
        ]
        cleanup["error"] = "; ".join(
            (incomplete or discovery_errors)[-4:]
        )
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
    known_descendants: RetainedProcesses = {}

    def interrupted(signum: int, _frame: object) -> None:
        raise SupervisorInterrupted(signum)

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, interrupted)

    try:
        _enable_child_subreaper()
        old_mask = signal.pthread_sigmask(
            signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM}
        )
        child_mask = set(old_mask) - {signal.SIGINT, signal.SIGTERM}

        def restore_child_signal_mask() -> None:
            # A supervisor can inherit the scheduler's temporary mask. Its
            # test child should retain the ordinary unblocked signal state.
            signal.pthread_sigmask(signal.SIG_SETMASK, child_mask)

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
        known_descendants[proc.pid] = None
        if sys.platform == "darwin":
            # Capture the leader's stable start identity before poll()/waitpid
            # can reap it and make the PID reusable. A short-lived child is
            # still owned here even if it has already become a zombie.
            _darwin_process_table(proc.pid, known_descendants)
            reused_pid_file = os.environ.get(DARWIN_REUSED_PID_FILE_ENV)
            if reused_pid_file:
                try:
                    reused_pid = int(Path(reused_pid_file).read_text().strip())
                    if reused_pid > 0:
                        # Test-only stale identity: this models a retained PID
                        # whose original process exited and whose number now
                        # belongs to an unrelated live process.
                        known_descendants[reused_pid] = (0, 0)
                except (OSError, ValueError):
                    pass
        next_darwin_snapshot = started
        outcome = "failed"
        rc = 1
        while True:
            polled = proc.poll()
            if polled is not None:
                rc = polled
                outcome = "passed" if rc == 0 else "failed"
                break
            _reap_adoptees(proc.pid)
            remaining = deadline - now()
            if remaining <= 0:
                rc = TIMEOUT_EXIT_CODE
                outcome = "timed_out"
                break
            if sys.platform == "darwin" and now() >= next_darwin_snapshot:
                rows, _error = _darwin_process_table(
                    proc.pid, known_descendants
                )
                if rows is not None:
                    _extend_known_descendants(
                        proc.pid, known_descendants, rows
                    )
                next_darwin_snapshot = now() + 0.25
            readable, _, _ = select.select(
                [owner_fd], [], [], min(remaining, 0.05)
            )
            if readable and os.read(owner_fd, 1) == b"":
                rc = 1
                outcome = "owner_lost"
                break

        if sys.platform == "darwin":
            reused_root_pid_file = os.environ.get(
                DARWIN_REUSED_ROOT_PID_FILE_ENV
            )
            if reused_root_pid_file:
                try:
                    reused_root_pid = int(
                        Path(reused_root_pid_file).read_text().strip()
                    )
                    if reused_root_pid > 0 and proc.returncode is not None:
                        # Test-only post-reap reuse: cleanup must reject this
                        # unidentified replacement instead of signaling it.
                        known_descendants.pop(proc.pid, None)
                        proc.pid = reused_root_pid
                        known_descendants[proc.pid] = None
                except (OSError, ValueError):
                    pass

        cleanup = _cleanup_process_tree(
            proc, grace_secs, known_descendants
        )
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
            _cleanup_process_tree(proc, grace_secs, known_descendants)
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
            _cleanup_process_tree(proc, grace_secs, known_descendants)
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


class TestBinaryHandle:
    """Collector-owned connection to one independently safe supervisor."""

    def __init__(
        self,
        proc: subprocess.Popen,
        owner_write: int,
        result_read: int,
        display_name: str,
    ):
        self.proc = proc
        self.owner_write = owner_write
        self.result_read = result_read
        self.display_name = display_name
        self._result: dict | None = None

    def poll(self) -> int | None:
        return self.proc.poll()

    def cancel(self) -> None:
        """Report owner loss so the supervisor cleans its whole test tree."""
        self._close_descriptors(("owner_write",))

    def interrupt(self, signum: int) -> None:
        """Ask the supervisor to report interruption after bounded cleanup."""
        if self.proc.poll() is None:
            try:
                os.kill(self.proc.pid, signum)
            except ProcessLookupError:
                pass

    def finish(self) -> dict:
        if self._result is not None:
            return self._result

        # A terminal signal must not strand this handle between process reap
        # and result decoding. Cache the complete result and close both pipes
        # before restoring the collector's signal mask. If a pending signal
        # then raises CollectorInterrupted, the scheduler still tracks this
        # handle and a second finish() returns the cached result.
        old_mask = signal.pthread_sigmask(
            signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM}
        )
        try:
            self.proc.wait()
            self._result = _read_supervisor_result(self.result_read)
            return self._result
        finally:
            try:
                self.close()
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, old_mask)

    def close(self) -> None:
        self._close_descriptors(("owner_write", "result_read"))

    def _close_descriptors(self, attrs: tuple[str, ...]) -> None:
        """Transfer descriptor ownership before an interruption can raise.

        Blocking terminal signals across the field update and close syscall
        prevents either unsafe half-state: an open descriptor marked closed,
        or a closed descriptor still marked live and closed again on retry.
        """
        old_mask = signal.pthread_sigmask(
            signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM}
        )
        close_error: OSError | None = None
        try:
            for attr in attrs:
                fd = getattr(self, attr)
                if fd < 0:
                    continue
                setattr(self, attr, -1)
                try:
                    os.close(fd)
                except OSError as exc:
                    if close_error is None:
                        close_error = exc
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, old_mask)
        if close_error is not None:
            raise close_error


def start_test_binary(
    exe: str,
    threads: int,
    timeout_secs: float,
    term_grace_secs: float,
    display_name: str,
) -> TestBinaryHandle:
    argv = [exe, "--test-threads", str(threads)]
    proc: subprocess.Popen | None = None
    owner_read, owner_write = os.pipe()
    result_read, result_write = os.pipe()

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
        supervisor_mask = set(old_mask) - {
            signal.SIGINT, signal.SIGTERM
        }

        def restore_child_signal_mask() -> None:
            signal.pthread_sigmask(signal.SIG_SETMASK, supervisor_mask)

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
        return TestBinaryHandle(
            proc, owner_write, result_read, display_name
        )
    except BaseException:
        if owner_write >= 0:
            os.close(owner_write)
            owner_write = -1
        if proc is not None and proc.poll() is None:
            try:
                proc.wait(timeout=(2 * term_grace_secs) + 2)
            except subprocess.TimeoutExpired:
                pass
        if result_read >= 0:
            os.close(result_read)
            result_read = -1
        raise
    finally:
        for fd in (owner_read, result_write):
            if fd >= 0:
                os.close(fd)


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


def cargo_rerun_command(binary: dict, test_threads: int) -> str | None:
    """Return a copy/pasteable Cargo command for one discovered executable.

    Cargo's compiler-artifact message gives us the package manifest and target
    kind directly. Prefer those stable fields over parsing the opaque
    ``package_id`` string.
    """
    manifest_path = binary.get("manifest_path")
    target_name = binary.get("target_name")
    target_kinds = binary.get("target_kinds") or []
    if not manifest_path or not target_name:
        return None

    selector: list[str]
    if "test" in target_kinds:
        selector = ["--test", target_name]
    elif "bin" in target_kinds:
        selector = ["--bin", target_name]
    elif "example" in target_kinds:
        selector = ["--example", target_name]
    elif "bench" in target_kinds:
        selector = ["--bench", target_name]
    elif "lib" in target_kinds or "proc-macro" in target_kinds:
        selector = ["--lib"]
    else:
        return None

    argv = [
        "cargo", "test", "--manifest-path", manifest_path,
        *CARGO_FEATURES, *selector,
        "--", "--test-threads", str(test_threads), "--nocapture",
    ]
    return shlex.join(argv)


def test_binary_failure(
    binary: dict, result: dict, test_threads: int
) -> dict:
    return {
        "phase": "test_execute",
        "target_name": (
            binary.get("target_name") or Path(binary["executable"]).name
        ),
        "package_id": binary.get("package_id"),
        "manifest_path": binary.get("manifest_path"),
        "executable": binary["executable"],
        "exit_code": result["exit_code"] or 1,
        "outcome": result["outcome"],
        "cleanup_complete": result["cleanup"]["complete"],
        "rerun_command": cargo_rerun_command(binary, test_threads),
    }


def record_test_result(
    binary: dict, result: dict, *, cancelled_by_fail_fast: bool = False
) -> None:
    binary["execute_secs"] = round(result["duration_secs"], 3)
    binary["execute_exit_code"] = result["exit_code"]
    binary["execute_outcome"] = result["outcome"]
    binary["execute_timed_out"] = result["timed_out"]
    binary["execute_timeout_secs"] = result["timeout_secs"]
    binary["cleanup"] = result["cleanup"]
    if cancelled_by_fail_fast:
        binary["execute_cancelled_by_fail_fast"] = True


def execute_test_binaries(
    binaries: list[dict],
    jobs: int,
    threads: int,
    timeout_secs: float,
    term_grace_secs: float,
) -> tuple[int, int | None, dict | None]:
    """Run compiled executables with bounded concurrency and safe fail-fast.

    The collector only schedules supervisors. Each supervisor remains the
    exclusive owner of its test process tree and its timeout/cleanup policy.
    """
    active: list[tuple[dict, TestBinaryHandle]] = []
    next_index = 0
    exec_rc = 0
    interrupted_signal: int | None = None
    first_failure: dict | None = None
    previous_handlers: dict[signal.Signals, object] = {}

    def interrupted(signum: int, _frame: object) -> None:
        raise CollectorInterrupted(signum)

    def start(binary: dict) -> None:
        # Do not let a pending signal land between Popen returning and the
        # handle becoming visible to the cleanup path.
        old_mask = signal.pthread_sigmask(
            signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM}
        )
        try:
            name = binary.get("target_name") or Path(
                binary["executable"]
            ).name
            handle = start_test_binary(
                binary["executable"], threads, timeout_secs,
                term_grace_secs, name,
            )
            active.append((binary, handle))
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, old_mask)

    for sig in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[sig] = signal.signal(sig, interrupted)

    try:
        while next_index < len(binaries) or active:
            while next_index < len(binaries) and len(active) < jobs:
                start(binaries[next_index])
                next_index += 1

            completed = [
                entry for entry in active if entry[1].poll() is not None
            ]
            if not completed:
                time.sleep(0.02)
                continue

            for binary, handle in completed:
                result = handle.finish()
                record_test_result(binary, result)
                failed = (
                    result["exit_code"] != 0
                    or not result["cleanup"]["complete"]
                )
                if not failed or first_failure is not None:
                    active.remove((binary, handle))
                    continue

                exec_rc = result["exit_code"] or 1
                first_failure = test_binary_failure(
                    binary, result, threads
                )
                name = first_failure["target_name"]
                print(
                    "preflight timing: FIRST FAILURE: test binary "
                    f"{name!r} ({result['outcome']}, exit {exec_rc})",
                    file=sys.stderr,
                    flush=True,
                )
                rerun = first_failure.get("rerun_command")
                if rerun:
                    print(
                        f"preflight timing: rerun: {rerun}",
                        file=sys.stderr,
                        flush=True,
                    )

                # The causal result is now recorded. Remove only this settled
                # handle; every peer remains tracked until its cancellation
                # result has been collected below.
                active.remove((binary, handle))

                # Stop scheduling at the first observed failure. Closing the
                # owner pipe makes each still-running supervisor perform the
                # same bounded descendant cleanup as abrupt owner loss.
                cancellation: list[
                    tuple[dict, TestBinaryHandle, bool]
                ] = []
                for other_binary, other_handle in active:
                    was_running = other_handle.poll() is None
                    if was_running:
                        # Treat the artifact marker and owner-pipe close as one
                        # signal-safe state transition. A pending terminal
                        # signal may raise as soon as the mask is restored,
                        # but the interruption path will then see both facts.
                        old_mask = signal.pthread_sigmask(
                            signal.SIG_BLOCK,
                            {signal.SIGINT, signal.SIGTERM},
                        )
                        try:
                            other_binary[
                                "execute_cancelled_by_fail_fast"
                            ] = True
                            other_handle.cancel()
                        finally:
                            signal.pthread_sigmask(
                                signal.SIG_SETMASK, old_mask
                            )
                    cancellation.append(
                        (other_binary, other_handle, was_running)
                    )
                for other_binary, other_handle, was_running in cancellation:
                    other_result = other_handle.finish()
                    record_test_result(
                        other_binary,
                        other_result,
                        cancelled_by_fail_fast=was_running,
                    )
                    active.remove((other_binary, other_handle))
                return exec_rc, None, first_failure
    except CollectorInterrupted as exc:
        interrupted_signal = exc.signum
        # Ignore a repeated terminal signal while every active supervisor
        # performs its own bounded cleanup and reports a final result.
        for sig in previous_handlers:
            signal.signal(sig, signal.SIG_IGN)
        for _binary, handle in active:
            handle.interrupt(exc.signum)
        for binary, handle in active:
            result = handle.finish()
            record_test_result(binary, result)
            failed = (
                result["exit_code"] != 0
                or not result["cleanup"]["complete"]
            )
            if first_failure is None and failed:
                first_failure = test_binary_failure(binary, result, threads)
            print(
                "preflight timing: interrupted while running test binary "
                f"{handle.display_name!r} after "
                f"{result['duration_secs']:.2f}s; "
                f"{_cleanup_diagnostic(result['cleanup'])}",
                file=sys.stderr,
                flush=True,
            )
        active.clear()
        exec_rc = 128 + exc.signum
        if first_failure is None:
            first_failure = {
                "phase": "test_execute",
                "exit_code": exec_rc,
                "outcome": "interrupted",
                "rerun_command": None,
            }
    finally:
        # This also closes owner pipes if an unexpected collector exception
        # escapes, retaining owner-loss cleanup rather than orphaning tests.
        for _binary, handle in active:
            handle.cancel()
        for _binary, handle in active:
            try:
                handle.finish()
            except BaseException:
                handle.close()
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)

    return exec_rc, interrupted_signal, first_failure


def emit_artifact(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    json.loads(path.read_text())


def emit_summary(path: Path, data: dict, top_n: int) -> None:
    lines: list[str] = []
    lines.append("=== preflight timing summary ===")
    lines.append(f"timestamp_utc: {data['timestamp_utc']}")
    lines.append(f"test_jobs: {data.get('test_jobs', 2)}")
    lines.append(f"test_threads: {data.get('test_threads', 'n/a')}")
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
        if wrapper.get("chained_wrapper"):
            lines.append(
                "rustc_wrapper_chain: timing -> "
                f"{wrapper['chained_wrapper']} -> rustc"
            )
    first_failure = data.get("first_failure")
    if first_failure:
        lines.append("")
        lines.append("FIRST FAILURE:")
        lines.append(f"  phase: {first_failure['phase']}")
        if first_failure.get("target_name"):
            lines.append(f"  test_binary: {first_failure['target_name']}")
        lines.append(f"  exit_code: {first_failure['exit_code']}")
        if first_failure.get("outcome"):
            lines.append(f"  outcome: {first_failure['outcome']}")
        if first_failure.get("rerun_command"):
            lines.append(f"  rerun: {first_failure['rerun_command']}")
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
    first_failure: dict | None = None

    def add_gate(name: str, duration: float, rc: int) -> None:
        nonlocal first_failure, status
        gates.append({
            "name": name,
            "duration_secs": round(duration, 3),
            "exit_code": rc,
        })
        if rc != 0 and status == 0:
            status = 1
            if first_failure is None:
                first_failure = {
                    "phase": name,
                    "exit_code": rc,
                    "outcome": "failed",
                    "rerun_command": None,
                }

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
        (
            dur,
            rc,
            binaries,
            matched,
            log_entries,
            chained_wrapper,
        ) = compile_tests(
            compile_log, stderr_log, rustc_log, wrapper_path
        )
        add_gate("cargo_test_no_run", dur, rc)
        wrapper_stats = {
            "matched": matched,
            "log_entries": log_entries,
            "log_path": str(rustc_log),
            "chained_wrapper": chained_wrapper,
        }

    if status == 0:
        print(
            f"=== timing 4/4: run {len(binaries)} test binaries "
            f"(--test-jobs {args.test_jobs}, "
            f"--test-threads {args.test_threads}, "
            f"{args.test_timeout_secs:g}s deadline each) ===",
            flush=True,
        )
        t0 = now()
        exec_rc, interrupted_signal, test_failure = execute_test_binaries(
            binaries,
            args.test_jobs,
            args.test_threads,
            args.test_timeout_secs,
            args.term_grace_secs,
        )
        if test_failure is not None:
            first_failure = test_failure
        add_gate("test_execute", now() - t0, exec_rc)
        if interrupted_signal is not None:
            status = 128 + interrupted_signal

    data = {
        "version": 3,
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "top_n": args.top_n,
        "test_jobs": args.test_jobs,
        "test_threads": args.test_threads,
        "test_timeout_secs": args.test_timeout_secs,
        "term_grace_secs": args.term_grace_secs,
        "interrupted_signal": interrupted_signal,
        "first_failure": first_failure,
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
      4. The timing wrapper preserves and invokes an inherited cache wrapper.
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

    # ---- (4) inherited wrapper composition ----
    wrapper_path = os.path.abspath(__file__)
    env = {"RUSTC_WRAPPER": "sccache"}
    assert _install_timing_wrapper(env, wrapper_path) == "sccache"
    assert env["RUSTC_WRAPPER"] == wrapper_path
    assert env[WRAPPER_CHAIN_ENV] == "sccache"

    # An explicitly empty RUSTC_WRAPPER disables the config-derived wrapper;
    # do not resurrect CARGO_BUILD_RUSTC_WRAPPER behind Cargo's back.
    env = {
        "RUSTC_WRAPPER": "",
        "CARGO_BUILD_RUSTC_WRAPPER": "config-cache",
    }
    assert _install_timing_wrapper(env, wrapper_path) is None
    assert WRAPPER_CHAIN_ENV not in env

    # When only Cargo's config environment override is present, preserve it.
    env = {"CARGO_BUILD_RUSTC_WRAPPER": "config-cache"}
    assert _install_timing_wrapper(env, wrapper_path) == "config-cache"
    assert env[WRAPPER_CHAIN_ENV] == "config-cache"

    # Never chain the collector into itself.
    env = {"RUSTC_WRAPPER": wrapper_path}
    assert _install_timing_wrapper(env, wrapper_path) is None
    assert WRAPPER_CHAIN_ENV not in env

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        compiler = out / "fake-rustc"
        cache = out / "fake-cache"
        capture = out / "cache-argv"
        log = out / "rustc.jsonl"
        compiler.write_text("#!/bin/sh\nexit 0\n")
        cache.write_text(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$TIMING_CHAIN_CAPTURE\"\n"
            "exec \"$@\"\n"
        )
        compiler.chmod(0o755)
        cache.chmod(0o755)
        child_env = os.environ.copy()
        child_env[WRAPPER_ACTIVE_ENV] = "1"
        child_env[WRAPPER_LOG_ENV] = str(log)
        child_env[WRAPPER_CHAIN_ENV] = str(cache)
        child_env["TIMING_CHAIN_CAPTURE"] = str(capture)
        subprocess.run(
            [
                wrapper_path,
                str(compiler),
                "--crate-name",
                "cache_probe",
                "--test",
            ],
            env=child_env,
            check=True,
        )
        captured = capture.read_text().splitlines()
        assert captured == [
            str(compiler),
            "--crate-name",
            "cache_probe",
            "--test",
        ], captured
        record = json.loads(log.read_text())
        assert record["crate_name"] == "cache_probe", record
        assert record["is_test"] is True, record
        assert record["exit_code"] == 0, record

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
            "test_jobs": 1,
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
        "--test-jobs",
        type=int,
        default=os.environ.get("PREFLIGHT_TEST_JOBS", "2"),
        help="maximum test executables run concurrently (default: 2)",
    )
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
    if args.test_jobs <= 0:
        p.error("--test-jobs must be positive")
    if args.test_threads <= 0:
        p.error("--test-threads must be positive")
    if args.test_timeout_secs <= 0:
        p.error("--test-timeout-secs must be positive")
    if args.term_grace_secs <= 0:
        p.error("--term-grace-secs must be positive")

    if args.self_test_mode:
        return self_test()

    wrapper_path = os.path.abspath(__file__)
    # Tests deliberately exercise failing CLI paths, and exit-3 diagnostics
    # may open (and therefore migrate) a Quorum database. Never let a test
    # process inherit an agent's production QUORUM_HOME. The override belongs
    # here, at the collector boundary, so Cargo, test supervisors, and every
    # test executable inherit the same isolated home even when an individual
    # test forgets to provide one.
    with tempfile.TemporaryDirectory(prefix="quorum-preflight-") as quorum_home:
        os.environ["QUORUM_HOME"] = quorum_home
        os.environ["QUORUM_REPO"] = "quorum/preflight"
        return collect(args, wrapper_path)


if __name__ == "__main__":
    sys.exit(main())
