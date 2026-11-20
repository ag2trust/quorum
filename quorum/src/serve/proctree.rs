//! Per-agent process-tree marker, discovery, immediate tree kill, and a bounded reap wait.
//!
//! # Mechanism
//!
//! Every managed agent spawn gets a fresh, random per-spawn tree token (a v4 UUID in
//! 32-character lowercase hex). [`apply`] exports it to the child as the environment
//! marker `QUORUM_PROC_TREE=<tokens>`. Every descendant inherits its parent's environment
//! across `fork`/`exec`, `setsid`, `setpgid`, and reparenting to init, so the marker
//! survives exactly the escapes that defeat the existing per-agent `killpg(SIGKILL)`: a
//! grandchild that starts its own session or process group still carries the token.
//!
//! The marker value is a `:`-separated list. When the spawning process (or the command)
//! already carries a marker, valid inherited tokens are kept and the new token is
//! appended (at most [`MAX_MARKER_TOKENS`], dropping the outermost first). A nested Quorum
//! daemon run by an agent therefore marks its own agents with both tokens, and the outer
//! agent's tree kill still reaches them. A process belongs to a tree when one list element
//! equals the handle's token exactly; prefixes and substrings never match.
//!
//! Discovery ([`members`]) is a bounded scan of same-uid processes and needs no extra
//! privileges:
//!
//! * **Linux:** `/proc/<pid>` owned by the daemon's effective uid, `/proc/<pid>/environ`
//!   for the marker, and `/proc/<pid>/stat` for state (zombies are skipped) and `ppid`.
//! * **macOS:** `proc_listallpids` plus `proc_pidinfo(PROC_PIDTBSDINFO)` for uid, state,
//!   and `ppid`, and `sysctl(KERN_PROCARGS2)` for the environment block.
//!
//! Both sources expose the environment the process was `exec`ed with, so a process that
//! later calls `unsetenv` keeps its marker. `ppid` ancestry is a secondary, reporting-only
//! signal: an unmarked live process whose parent chain reaches a marked member in the same
//! snapshot is reported by [`members`] (so [`reap_tree`] waits for or reports it), but it
//! is never signalled individually. Only the spawn-time process group and marked
//! processes are ever signalled.
//!
//! # Kill and reap
//!
//! [`kill_tree_now`] is synchronous, never sleeps, and never waits, so `Drop` impls may
//! call it. It sends `SIGKILL` to the spawn-time process group (only when the caller
//! passes `include_pgid`, preserving the callers' existing pgid-reuse guard) and to every
//! discovered member, re-verifying the marker on each pid immediately before signalling.
//! On Linux the re-verification is pinned to the process identity with a pidfd
//! (`pidfd_open`, read `environ`, `pidfd_send_signal`), so a pid reused between the scan
//! and the signal is never hit; kernels without pidfds fall back to verify-then-`kill`.
//! macOS has no pidfd, so its verify-then-`kill` leaves a microsecond reuse window that
//! would require the pid space to wrap inside it.
//!
//! It never signals the daemon itself (`getpid()`), the daemon's own process group
//! (`getpgrp()`), pid or pgid `<= 1`, or any process that does not carry this handle's
//! exact token, which excludes the processes of every other agent.
//!
//! [`reap_tree`] repeats the marker kill (never the process-group kill: the caller owns
//! that decision) and polls with tokio sleeps until no member remains or the deadline
//! passes. [`TREE_REAP_TIMEOUT`] is the single bound on that post-`SIGKILL` wait; a timeout
//! is logged with the token, pgid, and survivor count and returned, never raised as an
//! error. Reaping the leader itself stays with the caller's `Child` handle; zombies are
//! not members.
//!
//! There is deliberately no `SIGTERM` stage and no per-reap grace delay. The daemon-level
//! grace already exists (first `SIGINT`/`SIGTERM` enters drain, `drain_timeout_secs`,
//! then force-kill) and per-agent kill has always been an immediate `killpg(SIGKILL)`.
//!
//! # Rejected alternatives
//!
//! * **Per-agent cgroup or systemd scope:** exact and escape-proof, but Linux-only and it
//!   requires user-systemd delegation (or root) on the daemon host.
//! * **Daemon-wide `PR_SET_CHILD_SUBREAPER`:** Linux-only, and every orphan reparents to
//!   the daemon without saying which agent it came from, so per-agent attribution is lost.
//!
//! # Limitations
//!
//! * A descendant that scrubs its entire environment (`env -i`, `execve` with an empty
//!   `envp`) **and** leaves the process group is not found once its marked ancestors are
//!   gone. While it stays in the process group the caller's `killpg` still reaches it.
//! * A descendant whose environment the same uid cannot read (for example a set-uid or
//!   non-dumpable process) is neither found nor signalled. That is the fail-safe
//!   direction: an unverifiable process is never killed.
//! * Platforms other than Linux and macOS discover nothing; only the process-group kill
//!   applies there.

// Provider wiring (Claude, Codex, Grok) lands in sibling tasks; until then only tests use
// this module.
#![allow(dead_code)]

use std::collections::HashSet;
use std::ffi::{OsStr, OsString};
use std::time::Duration;

/// Environment variable that carries the `:`-separated tree-token list.
pub const TREE_MARKER_ENV: &str = "QUORUM_PROC_TREE";

/// The only bound on the post-`SIGKILL` wait in [`reap_tree`].
pub const TREE_REAP_TIMEOUT: Duration = Duration::from_secs(5);

/// Poll interval between reap scans. This is the scan cadence, not a grace delay: every
/// scan is preceded by an immediate `SIGKILL`.
const REAP_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Nesting bound on the marker list (outermost tokens are dropped first).
const MAX_MARKER_TOKENS: usize = 16;

/// Hex length of a tree token (`Uuid::simple`).
const TOKEN_LEN: usize = 32;

/// Upper bound on the bytes read from one process's environment block.
const ENV_READ_LIMIT: usize = 4 << 20;

/// One spawned agent process tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeHandle {
    pgid: i32,
    leader_pid: i32,
    token: String,
}

impl TreeHandle {
    /// Record the spawned leader. Callers keep their `setpgid(0, 0)` `pre_exec`, so the
    /// spawn-time process group equals the leader pid.
    pub fn bind_leader(&mut self, leader_pid: u32) {
        let pid = i32::try_from(leader_pid).unwrap_or(0);
        self.leader_pid = pid;
        self.pgid = pid;
    }

    /// Spawn-time process group, or 0 before [`TreeHandle::bind_leader`].
    pub fn pgid(&self) -> i32 {
        self.pgid
    }

    /// Leader pid, or 0 before [`TreeHandle::bind_leader`].
    pub fn leader_pid(&self) -> i32 {
        self.leader_pid
    }

    /// This spawn's tree token.
    pub fn token(&self) -> &str {
        &self.token
    }
}

/// Result of [`reap_tree`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TreeReapOutcome {
    /// No live member remains.
    Gone,
    /// The wait bound expired; these members were still alive at the last scan.
    TimedOut { survivors: Vec<i32> },
}

/// Mark `cmd` with a fresh tree token and return its handle.
///
/// Call this after any `env_clear`/`env_remove` stripping, because those would drop the
/// marker. Bind the leader with [`TreeHandle::bind_leader`] after spawning.
pub fn apply(cmd: &mut tokio::process::Command) -> TreeHandle {
    let token = uuid::Uuid::new_v4().simple().to_string();
    let explicit = cmd
        .as_std()
        .get_envs()
        .find(|(key, _)| *key == OsStr::new(TREE_MARKER_ENV))
        .map(|(_, value)| value.map(OsStr::to_os_string));
    let inherited: Option<OsString> = match explicit {
        Some(value) => value,
        None => std::env::var_os(TREE_MARKER_ENV),
    };
    cmd.env(TREE_MARKER_ENV, marker_value(inherited.as_deref(), &token));
    TreeHandle {
        pgid: 0,
        leader_pid: 0,
        token,
    }
}

/// Live processes in this tree: same-uid processes carrying the exact token, plus
/// unmarked descendants whose parent chain reaches one of them in the same snapshot.
/// Never includes the calling process, pid `<= 1`, or zombies.
pub fn members(handle: &TreeHandle) -> Vec<i32> {
    if !is_token(&handle.token) {
        return Vec::new();
    }
    let self_pid = unsafe { libc::getpid() };
    let mut snapshot = platform::snapshot(handle.token.as_bytes());
    snapshot.retain(|entry| entry.pid > 1 && entry.pid != self_pid);
    closure(&snapshot)
}

/// Immediately `SIGKILL` the tree: the spawn-time process group when `include_pgid` (and
/// allowed by the daemon guards), then every discovered member whose marker re-verifies.
/// Synchronous: one bounded process scan, no sleep, no wait.
pub fn kill_tree_now(handle: &TreeHandle, include_pgid: bool) {
    if include_pgid && pgid_signal_allowed(handle.pgid) {
        unsafe {
            libc::killpg(handle.pgid, libc::SIGKILL);
        }
    }
    for pid in members(handle) {
        signal_if_marked(pid, &handle.token);
    }
}

/// Repeat the marker kill and poll until no member remains or the deadline passes. The
/// wait is bounded by both `deadline` and [`TREE_REAP_TIMEOUT`]. The process-group kill is
/// never repeated here; call [`kill_tree_now`] with the caller's `include_pgid` first.
pub async fn reap_tree(handle: &TreeHandle, deadline: tokio::time::Instant) -> TreeReapOutcome {
    let deadline = deadline.min(tokio::time::Instant::now() + TREE_REAP_TIMEOUT);
    loop {
        let survivors = members(handle);
        if survivors.is_empty() {
            return TreeReapOutcome::Gone;
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            eprintln!(
                "proctree: WARNING — tree {} (pgid {}, leader {}) still has {} live member(s) \
                 at the post-SIGKILL reap bound; giving up",
                handle.token,
                handle.pgid,
                handle.leader_pid,
                survivors.len()
            );
            return TreeReapOutcome::TimedOut { survivors };
        }
        for &pid in &survivors {
            signal_if_marked(pid, &handle.token);
        }
        tokio::time::sleep(REAP_POLL_INTERVAL.min(deadline - now)).await;
    }
}

/// `SIGKILL` `pid` only if it passes the daemon guards and still carries `token`,
/// verified immediately before the signal. Returns whether a signal was delivered.
fn signal_if_marked(pid: i32, token: &str) -> bool {
    if !pid_signal_allowed(pid) || !is_token(token) {
        return false;
    }
    platform::signal_if_marked(pid, token.as_bytes())
}

fn pid_signal_allowed(pid: i32) -> bool {
    pid > 1 && pid != unsafe { libc::getpid() }
}

fn pgid_signal_allowed(pgid: i32) -> bool {
    pgid > 1 && pgid != unsafe { libc::getpgrp() } && pgid != unsafe { libc::getpid() }
}

fn is_token(value: &str) -> bool {
    value.len() == TOKEN_LEN
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Build the marker value: valid inherited tokens (bounded, outermost dropped first)
/// followed by `token`.
fn marker_value(inherited: Option<&OsStr>, token: &str) -> String {
    let mut tokens: Vec<&str> = inherited
        .and_then(OsStr::to_str)
        .map(|value| {
            value
                .split(':')
                .filter(|t| is_token(t) && *t != token)
                .collect()
        })
        .unwrap_or_default();
    let keep = MAX_MARKER_TOKENS - 1;
    if tokens.len() > keep {
        tokens.drain(..tokens.len() - keep);
    }
    tokens.push(token);
    tokens.join(":")
}

/// Whether one `KEY=value` environment entry is a marker containing exactly `token`.
fn entry_carries(entry: &[u8], token: &[u8]) -> bool {
    entry
        .strip_prefix(TREE_MARKER_ENV.as_bytes())
        .and_then(|rest| rest.strip_prefix(b"="))
        .is_some_and(|value| value.split(|b| *b == b':').any(|t| t == token))
}

fn env_carries<'a>(entries: impl IntoIterator<Item = &'a [u8]>, token: &[u8]) -> bool {
    entries.into_iter().any(|entry| entry_carries(entry, token))
}

/// The environment strings of a macOS `KERN_PROCARGS2` buffer: `argc`, exec path, NUL
/// padding, `argc` argument strings, then environment strings up to an empty string.
#[cfg(any(target_os = "macos", test))]
fn procargs2_env(data: &[u8]) -> Option<impl Iterator<Item = &[u8]>> {
    let argc = i32::from_ne_bytes(data.get(..4)?.try_into().ok()?);
    let argc = usize::try_from(argc).ok()?;
    let mut rest = &data[4..];
    let path_end = rest.iter().position(|b| *b == 0)?;
    rest = &rest[path_end..];
    let first = rest.iter().position(|b| *b != 0).unwrap_or(rest.len());
    rest = &rest[first..];
    for _ in 0..argc {
        let end = rest.iter().position(|b| *b == 0)?;
        rest = &rest[end + 1..];
    }
    Some(
        rest.split(|b| *b == 0)
            .take_while(|entry| !entry.is_empty()),
    )
}

/// One live, non-zombie, same-uid process in a scan.
#[derive(Clone, Copy, Debug)]
struct ProcEntry {
    pid: i32,
    ppid: i32,
    marked: bool,
}

/// Marked pids plus unmarked descendants reachable through `ppid` in this snapshot.
fn closure(snapshot: &[ProcEntry]) -> Vec<i32> {
    let mut set: HashSet<i32> = snapshot
        .iter()
        .filter(|entry| entry.marked)
        .map(|entry| entry.pid)
        .collect();
    if set.is_empty() {
        return Vec::new();
    }
    loop {
        let before = set.len();
        for entry in snapshot {
            if set.contains(&entry.ppid) {
                set.insert(entry.pid);
            }
        }
        if set.len() == before {
            break;
        }
    }
    let mut pids: Vec<i32> = set.into_iter().collect();
    pids.sort_unstable();
    pids
}

#[cfg(target_os = "linux")]
mod platform {
    use super::{env_carries, ProcEntry, ENV_READ_LIMIT};
    use std::io::Read;
    use std::os::fd::{FromRawFd, OwnedFd};
    use std::os::unix::fs::MetadataExt;

    pub(super) fn snapshot(token: &[u8]) -> Vec<ProcEntry> {
        let euid = unsafe { libc::geteuid() };
        let Ok(dir) = std::fs::read_dir("/proc") else {
            return Vec::new();
        };
        let mut buf = Vec::new();
        let mut out = Vec::new();
        for entry in dir.flatten() {
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<i32>().ok())
            else {
                continue;
            };
            if pid <= 0 || !entry.metadata().is_ok_and(|meta| meta.uid() == euid) {
                continue;
            }
            let Some((state, ppid)) = read_stat(pid) else {
                continue;
            };
            if state == 'Z' || state == 'X' || state == 'x' {
                continue;
            }
            let marked = environ_carries(pid, token, &mut buf);
            out.push(ProcEntry { pid, ppid, marked });
        }
        out
    }

    fn read_stat(pid: i32) -> Option<(char, i32)> {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        super::parse_linux_stat(&stat)
    }

    fn environ_carries(pid: i32, token: &[u8], buf: &mut Vec<u8>) -> bool {
        buf.clear();
        let Ok(file) = std::fs::File::open(format!("/proc/{pid}/environ")) else {
            return false;
        };
        if file.take(ENV_READ_LIMIT as u64).read_to_end(buf).is_err() {
            return false;
        }
        env_carries(buf.split(|b| *b == 0), token)
    }

    pub(super) fn signal_if_marked(pid: i32, token: &[u8]) -> bool {
        let mut buf = Vec::new();
        let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        if raw < 0 {
            if std::io::Error::last_os_error().raw_os_error() != Some(libc::ENOSYS) {
                return false;
            }
            return environ_carries(pid, token, &mut buf)
                && unsafe { libc::kill(pid, libc::SIGKILL) } == 0;
        }
        let Ok(raw) = libc::c_int::try_from(raw) else {
            return false;
        };
        // SAFETY: `pidfd_open` returned a fresh descriptor that only this guard owns.
        let pidfd = unsafe { OwnedFd::from_raw_fd(raw) };
        // The pidfd pins the process identity: if `pid` was reused after `pidfd_open`,
        // the signal below targets the dead original and fails with ESRCH.
        environ_carries(pid, token, &mut buf)
            && unsafe {
                libc::syscall(
                    libc::SYS_pidfd_send_signal,
                    std::os::fd::AsRawFd::as_raw_fd(&pidfd),
                    libc::SIGKILL,
                    std::ptr::null::<libc::siginfo_t>(),
                    0,
                )
            } == 0
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use super::{env_carries, procargs2_env, ProcEntry, ENV_READ_LIMIT};

    /// Upper bound on pids listed in one scan.
    const MAX_PIDS: usize = 1 << 18;

    pub(super) fn snapshot(token: &[u8]) -> Vec<ProcEntry> {
        let euid = unsafe { libc::geteuid() };
        let mut buf = vec![0u8; argmax()];
        let mut out = Vec::new();
        for pid in list_pids() {
            if pid <= 0 {
                continue;
            }
            let Some(info) = bsd_info(pid) else {
                continue;
            };
            if info.pbi_uid != euid || info.pbi_status == libc::SZOMB {
                continue;
            }
            let marked = procargs_carries(pid, token, &mut buf);
            out.push(ProcEntry {
                pid,
                ppid: i32::try_from(info.pbi_ppid).unwrap_or(0),
                marked,
            });
        }
        out
    }

    fn list_pids() -> Vec<i32> {
        let estimate = unsafe { libc::proc_listallpids(std::ptr::null_mut(), 0) };
        let Ok(estimate) = usize::try_from(estimate) else {
            return Vec::new();
        };
        let cap = estimate.saturating_add(256).min(MAX_PIDS);
        let mut pids: Vec<libc::pid_t> = vec![0; cap];
        let bytes = libc::c_int::try_from(cap * std::mem::size_of::<libc::pid_t>())
            .unwrap_or(libc::c_int::MAX);
        let got = unsafe { libc::proc_listallpids(pids.as_mut_ptr().cast(), bytes) };
        let Ok(got) = usize::try_from(got) else {
            return Vec::new();
        };
        pids.truncate(got.min(cap));
        pids
    }

    fn bsd_info(pid: i32) -> Option<libc::proc_bsdinfo> {
        // SAFETY: `proc_bsdinfo` is a plain C struct; all-zero is a valid value.
        let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
        let size = libc::c_int::try_from(std::mem::size_of::<libc::proc_bsdinfo>()).ok()?;
        let written = unsafe {
            libc::proc_pidinfo(
                pid,
                libc::PROC_PIDTBSDINFO,
                0,
                (&mut info as *mut libc::proc_bsdinfo).cast(),
                size,
            )
        };
        (written == size).then_some(info)
    }

    fn argmax() -> usize {
        let mut mib = [libc::CTL_KERN, libc::KERN_ARGMAX];
        let mut value: libc::c_int = 0;
        let mut len = std::mem::size_of::<libc::c_int>();
        let rc = unsafe {
            libc::sysctl(
                mib.as_mut_ptr(),
                2,
                (&mut value as *mut libc::c_int).cast(),
                &mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        match usize::try_from(value) {
            Ok(value) if rc == 0 && value > 0 => value.min(ENV_READ_LIMIT),
            _ => 1 << 20,
        }
    }

    fn procargs_carries(pid: i32, token: &[u8], buf: &mut [u8]) -> bool {
        let mut mib = [libc::CTL_KERN, libc::KERN_PROCARGS2, pid];
        let mut len = buf.len();
        let rc = unsafe {
            libc::sysctl(
                mib.as_mut_ptr(),
                3,
                buf.as_mut_ptr().cast(),
                &mut len,
                std::ptr::null_mut(),
                0,
            )
        };
        if rc != 0 {
            return false;
        }
        procargs2_env(&buf[..len.min(buf.len())]).is_some_and(|env| env_carries(env, token))
    }

    pub(super) fn signal_if_marked(pid: i32, token: &[u8]) -> bool {
        let mut buf = vec![0u8; argmax()];
        procargs_carries(pid, token, &mut buf) && unsafe { libc::kill(pid, libc::SIGKILL) } == 0
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
mod platform {
    use super::ProcEntry;

    pub(super) fn snapshot(_token: &[u8]) -> Vec<ProcEntry> {
        Vec::new()
    }

    pub(super) fn signal_if_marked(_pid: i32, _token: &[u8]) -> bool {
        false
    }
}

/// Parse `(state, ppid)` from a Linux `/proc/<pid>/stat` line. The command name is in
/// parentheses and may itself contain spaces or `)`, so fields start after the last `)`.
#[cfg(any(target_os = "linux", test))]
fn parse_linux_stat(stat: &str) -> Option<(char, i32)> {
    let after = &stat[stat.rfind(')')? + 1..];
    let mut fields = after.split_ascii_whitespace();
    let state = fields.next()?.chars().next()?;
    let ppid = fields.next()?.parse().ok()?;
    Some((state, ppid))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::CommandExt as _;
    use std::path::Path;
    use tokio::time::Instant;

    const WAIT: Duration = Duration::from_secs(10);

    fn alive(pid: i32) -> bool {
        unsafe { libc::kill(pid, 0) == 0 }
    }

    fn handle_for_tests() -> TreeHandle {
        apply(&mut tokio::process::Command::new("true"))
    }

    /// Spawn `sh -c <script> sh <pidfile>` in its own process group, optionally marked.
    fn spawn(script: &str, pidfile: &Path, mark: Option<&str>) -> tokio::process::Child {
        let mut cmd = tokio::process::Command::new("sh");
        cmd.arg("-c")
            .arg(script)
            .arg("sh")
            .arg(pidfile)
            .env_remove(TREE_MARKER_ENV)
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true);
        if let Some(value) = mark {
            cmd.env(TREE_MARKER_ENV, value);
        }
        unsafe {
            cmd.as_std_mut().pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        cmd.spawn().expect("spawn sh")
    }

    /// Leader script that starts a grandchild in a new session (so it leaves the leader's
    /// process group), records the grandchild's pid in `$1`, then execs into a sleep.
    const SETSID_TREE: &str = r#"
inner='echo $$ > "$1.tmp" && mv "$1.tmp" "$1"; exec sleep 300'
if command -v setsid >/dev/null 2>&1; then
  setsid sh -c "$inner" sh "$1" &
else
  perl -MPOSIX -e 'POSIX::setsid() or die; exec @ARGV' sh -c "$inner" sh "$1" &
fi
exec sleep 300
"#;

    /// Spawn a marked tree via [`apply`] and return its leader, handle, and grandchild pid.
    async fn spawn_marked_tree(dir: &Path, name: &str) -> (tokio::process::Child, TreeHandle, i32) {
        let pidfile = dir.join(name);
        let mut cmd = tokio::process::Command::new("sh");
        cmd.arg("-c")
            .arg(SETSID_TREE)
            .arg("sh")
            .arg(&pidfile)
            .env_remove(TREE_MARKER_ENV)
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true);
        unsafe {
            cmd.as_std_mut().pre_exec(|| {
                if libc::setpgid(0, 0) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut handle = apply(&mut cmd);
        let child = cmd.spawn().expect("spawn marked tree");
        handle.bind_leader(child.id().expect("leader pid"));
        let grandchild = wait_for_pidfile(&pidfile).await;
        (child, handle, grandchild)
    }

    async fn wait_for_pidfile(path: &Path) -> i32 {
        let start = Instant::now();
        loop {
            if let Ok(text) = std::fs::read_to_string(path) {
                if let Ok(pid) = text.trim().parse() {
                    return pid;
                }
            }
            assert!(
                start.elapsed() < WAIT,
                "pidfile {} never appeared",
                path.display()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Wait until `pid` is fully gone (ESRCH), allowing its reaper to collect the zombie.
    async fn assert_esrch(pid: i32) {
        let start = Instant::now();
        loop {
            let rc = unsafe { libc::kill(pid, 0) };
            if rc == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
                return;
            }
            assert!(start.elapsed() < WAIT, "pid {pid} never reported ESRCH");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_until_member(handle: &TreeHandle, pid: i32) {
        let start = Instant::now();
        while !members(handle).contains(&pid) {
            assert!(start.elapsed() < WAIT, "pid {pid} never became a member");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn setsid_grandchild_is_found_killed_and_reaped_while_other_tree_survives() {
        let dir = tempfile::tempdir().unwrap();
        let (mut leader_a, tree_a, grandchild_a) = spawn_marked_tree(dir.path(), "a").await;
        let (mut leader_b, tree_b, grandchild_b) = spawn_marked_tree(dir.path(), "b").await;
        assert_ne!(tree_a.token(), tree_b.token());

        // The grandchild escaped the leader's process group and session.
        assert_ne!(unsafe { libc::getpgid(grandchild_a) }, tree_a.pgid());
        assert_eq!(unsafe { libc::getsid(grandchild_a) }, grandchild_a);

        wait_until_member(&tree_a, tree_a.leader_pid()).await;
        wait_until_member(&tree_a, grandchild_a).await;
        wait_until_member(&tree_b, grandchild_b).await;
        let found = members(&tree_a);
        assert!(found.contains(&grandchild_a), "{found:?}");
        assert!(found.contains(&tree_a.leader_pid()), "{found:?}");
        assert!(!found.contains(&grandchild_b) && !found.contains(&tree_b.leader_pid()));
        assert!(!found.contains(&unsafe { libc::getpid() }));

        kill_tree_now(&tree_a, true);
        let outcome = reap_tree(&tree_a, Instant::now() + TREE_REAP_TIMEOUT).await;
        assert_eq!(outcome, TreeReapOutcome::Gone);
        let status = leader_a.wait().await.unwrap();
        assert_eq!(
            std::os::unix::process::ExitStatusExt::signal(&status),
            Some(libc::SIGKILL)
        );
        assert_esrch(grandchild_a).await;
        assert_esrch(tree_a.leader_pid()).await;

        // The other agent's tree and the test process itself are untouched.
        assert!(alive(tree_b.leader_pid()) && alive(grandchild_b));
        let others = members(&tree_b);
        assert!(others.contains(&grandchild_b) && others.contains(&tree_b.leader_pid()));
        assert!(alive(unsafe { libc::getpid() }));
        assert!(leader_b.try_wait().unwrap().is_none());

        kill_tree_now(&tree_b, true);
        assert_eq!(
            reap_tree(&tree_b, Instant::now() + TREE_REAP_TIMEOUT).await,
            TreeReapOutcome::Gone
        );
        leader_b.wait().await.unwrap();
        assert_esrch(grandchild_b).await;
    }

    #[tokio::test]
    async fn unmarked_and_near_miss_processes_are_never_signalled() {
        let dir = tempfile::tempdir().unwrap();
        let handle = handle_for_tests();
        let token = handle.token().to_string();
        let script = r#"echo $$ > "$1"; exec sleep 300"#;
        // Unmarked (stands in for a reused pid), superset, prefix, and foreign-list values.
        let marks = [
            None,
            Some(format!("{token}0")),
            Some(token[..TOKEN_LEN - 1].to_string()),
            Some(format!("x{token}:{}", "f".repeat(TOKEN_LEN))),
        ];
        let mut children = Vec::new();
        for (i, mark) in marks.iter().enumerate() {
            let pidfile = dir.path().join(format!("p{i}"));
            let child = spawn(script, &pidfile, mark.as_deref());
            let pid = wait_for_pidfile(&pidfile).await;
            children.push((child, pid));
        }

        assert!(members(&handle).is_empty());
        kill_tree_now(&handle, false);
        for (_, pid) in &children {
            assert!(!signal_if_marked(*pid, &token), "pid {pid} was signalled");
        }
        assert_eq!(
            reap_tree(&handle, Instant::now() + TREE_REAP_TIMEOUT).await,
            TreeReapOutcome::Gone
        );
        for (child, pid) in &mut children {
            assert!(alive(*pid), "pid {pid} died");
            assert!(child.try_wait().unwrap().is_none(), "pid {pid} exited");
        }
        for (mut child, _) in children {
            child.kill().await.unwrap();
        }
    }

    #[tokio::test]
    async fn nested_marker_keeps_outer_token_and_both_trees_find_the_process() {
        let dir = tempfile::tempdir().unwrap();
        let outer = handle_for_tests();
        let pidfile = dir.path().join("nested");
        let mut cmd = tokio::process::Command::new("sh");
        cmd.arg("-c")
            .arg(r#"echo $$ > "$1"; exec sleep 300"#)
            .arg("sh")
            .arg(&pidfile)
            .env(TREE_MARKER_ENV, outer.token())
            .kill_on_drop(true);
        let mut inner = apply(&mut cmd);
        let mut child = cmd.spawn().unwrap();
        inner.bind_leader(child.id().unwrap());
        let pid = wait_for_pidfile(&pidfile).await;
        assert_eq!(pid, inner.leader_pid());

        wait_until_member(&inner, pid).await;
        assert!(members(&outer).contains(&pid));

        kill_tree_now(&outer, false);
        assert_eq!(
            reap_tree(&outer, Instant::now() + TREE_REAP_TIMEOUT).await,
            TreeReapOutcome::Gone
        );
        child.wait().await.unwrap();
        assert!(members(&inner).is_empty());
    }

    #[tokio::test]
    async fn reap_times_out_at_caller_deadline_and_reports_survivors() {
        // An unkillable member cannot be produced without privileges, so drive the
        // timeout with a live marked process and an already-expired deadline.
        let dir = tempfile::tempdir().unwrap();
        let pidfile = dir.path().join("t");
        let mut cmd = tokio::process::Command::new("sh");
        cmd.arg("-c")
            .arg(r#"echo $$ > "$1"; exec sleep 300"#)
            .arg("sh")
            .arg(&pidfile)
            .env_remove(TREE_MARKER_ENV)
            .kill_on_drop(true);
        let mut handle = apply(&mut cmd);
        let mut child = cmd.spawn().unwrap();
        handle.bind_leader(child.id().unwrap());
        let pid = wait_for_pidfile(&pidfile).await;
        wait_until_member(&handle, pid).await;

        let started = std::time::Instant::now();
        let outcome = reap_tree(&handle, Instant::now()).await;
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(
            outcome,
            TreeReapOutcome::TimedOut {
                survivors: vec![pid]
            }
        );
        child.kill().await.unwrap();
    }

    #[test]
    fn daemon_guards_refuse_self_group_and_init() {
        let own_pgrp = unsafe { libc::getpgrp() };
        let own_pid = unsafe { libc::getpid() };
        for pgid in [-1, 0, 1, own_pgrp, own_pid] {
            assert!(!pgid_signal_allowed(pgid), "pgid {pgid}");
        }
        for pid in [-1, 0, 1, own_pid] {
            assert!(!pid_signal_allowed(pid), "pid {pid}");
        }
        let unbound = handle_for_tests();
        assert_eq!((unbound.pgid(), unbound.leader_pid()), (0, 0));
        // Unbound: the pgid guard refuses killpg(0), which would hit the caller's group.
        kill_tree_now(&unbound, true);
        assert!(alive(own_pid));
    }

    #[test]
    fn marker_matching_is_exact_per_list_element() {
        let token = "0123456789abcdef0123456789abcdef";
        let other = "fedcba9876543210fedcba9876543210";
        let t = token.as_bytes();
        assert!(entry_carries(
            format!("QUORUM_PROC_TREE={token}").as_bytes(),
            t
        ));
        assert!(entry_carries(
            format!("QUORUM_PROC_TREE={other}:{token}").as_bytes(),
            t
        ));
        assert!(!entry_carries(
            format!("QUORUM_PROC_TREE={token}0").as_bytes(),
            t
        ));
        assert!(!entry_carries(
            format!("QUORUM_PROC_TREEX={token}").as_bytes(),
            t
        ));
        assert!(!entry_carries(
            format!("XQUORUM_PROC_TREE={token}").as_bytes(),
            t
        ));
        assert!(!entry_carries(
            format!("QUORUM_PROC_TREE={other}").as_bytes(),
            t
        ));
        assert!(!entry_carries(b"QUORUM_PROC_TREE=", t));
    }

    #[test]
    fn marker_value_appends_valid_tokens_with_a_nesting_bound() {
        let token = "0123456789abcdef0123456789abcdef";
        assert_eq!(marker_value(None, token), token);
        let outer = "f".repeat(TOKEN_LEN);
        let inherited = format!("junk:{outer}:{token}:ABCDEF");
        assert_eq!(
            marker_value(Some(OsStr::new(&inherited)), token),
            format!("{outer}:{token}")
        );
        let many: Vec<String> = (0..40).map(|i| format!("{i:032x}")).collect();
        let value = marker_value(Some(OsStr::new(&many.join(":"))), token);
        let parts: Vec<&str> = value.split(':').collect();
        assert_eq!(parts.len(), MAX_MARKER_TOKENS);
        assert_eq!(parts.last(), Some(&token));
        assert_eq!(parts[0], many[40 - (MAX_MARKER_TOKENS - 1)]);
        assert!(is_token(handle_for_tests().token()));
    }

    #[test]
    fn closure_adds_unmarked_descendants_only_through_marked_ancestors() {
        let e = |pid, ppid, marked| ProcEntry { pid, ppid, marked };
        let snapshot = [
            e(10, 1, true),
            e(11, 10, false),
            e(12, 11, false),
            e(20, 1, false),
            e(21, 20, false),
            e(30, 12, true),
        ];
        assert_eq!(closure(&snapshot), vec![10, 11, 12, 30]);
        assert!(closure(&[e(20, 1, false)]).is_empty());
    }

    #[test]
    fn procargs2_env_skips_exec_path_padding_and_argv() {
        let mut data = 2i32.to_ne_bytes().to_vec();
        data.extend_from_slice(
            b"/bin/sh\0\0\0\0sh\0QUORUM_PROC_TREE=argv\0A=1\0QUORUM_PROC_TREE=x\0\0junk\0",
        );
        let env: Vec<&[u8]> = procargs2_env(&data).unwrap().collect();
        assert_eq!(env, vec![&b"A=1"[..], &b"QUORUM_PROC_TREE=x"[..]]);
        assert!(procargs2_env(&[1, 0]).is_none());
        let mut truncated = 3i32.to_ne_bytes().to_vec();
        truncated.extend_from_slice(b"/bin/sh\0a\0");
        assert!(procargs2_env(&truncated).is_none());
    }

    #[test]
    fn linux_stat_parsing_handles_parenthesised_names() {
        assert_eq!(parse_linux_stat("42 (sh) S 7 42 42 0"), Some(('S', 7)));
        assert_eq!(parse_linux_stat("42 (a) b) Z 9 42"), Some(('Z', 9)));
        assert_eq!(parse_linux_stat("42 (sh"), None);
    }
}
