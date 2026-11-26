//! Per-task, per-slot session store for the external runner.
//!
//! Layout (spec `docs/2026-11-20-external-cli-runner-technical-spec.md`,
//! "Sessions" > "Layout"): `<session_base>/<task id>/<slot>/session`.
//!
//! Daemon rules ("Sessions" > "Daemon rules") enforced here:
//! - `<task id>/` and `<slot>/` are created with mode `0700`. Every component
//!   must be a real directory: it is checked with `lstat`/`fstatat(AT_SYMLINK_NOFOLLOW)`
//!   and opened with `O_NOFOLLOW | O_DIRECTORY`, and the opened descriptor must
//!   be the inode that was checked.
//! - The session file is never opened or read. The only operations on it are
//!   `fstatat(AT_SYMLINK_NOFOLLOW)` and `unlinkat`, relative to the slot
//!   directory's descriptor. A directory at that name is removed with the
//!   symlink-safe recursive removal below.
//! - Whole task trees are removed with the same symlink-safe removal when the
//!   task is `done` or `cancelled` (including `task-close`, which writes
//!   `done`), or does not exist. Failed and parked tasks keep theirs.
//!
//! Symlink-safe removal (clone-mode spec, "9. Cleanup") walks the tree through
//! descriptors without following links, restores the owner `rwx` bits on each
//! real directory before entering it (a run may have removed its own write
//! permission), and unlinks symlinks as entries, never their targets.
//!
//! Errors carry only an operation label, a component label, and the OS error;
//! never file contents or run-supplied text.

// No run uses the slot API until the external runner adapter (slice 3) and
// resume (slice 6) land; only the terminal/startup sweep is wired today.
#![cfg_attr(not(test), allow(dead_code))]

use std::ffi::{CStr, CString};
use std::fmt;
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

/// File name of a slot's session file inside its slot directory.
pub const SESSION_FILE: &str = "session";
const DIR_MODE: libc::mode_t = 0o700;
/// Directory nesting below a removal root that removal will descend. Each
/// level holds one descriptor; deeper trees fail removal loudly.
const MAX_REMOVE_DEPTH: usize = 128;

/// The role position a session belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Slot {
    Worker,
    ReviewerR1,
    ReviewerR2,
}

impl Slot {
    pub const ALL: [Slot; 3] = [Slot::Worker, Slot::ReviewerR1, Slot::ReviewerR2];

    pub fn as_str(self) -> &'static str {
        match self {
            Slot::Worker => "worker",
            Slot::ReviewerR1 => "reviewer-r1",
            Slot::ReviewerR2 => "reviewer-r2",
        }
    }
}

impl fmt::Display for Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The documented default `session_base`: `<state dir>/sessions`, where the
/// state directory is the directory holding `quorum.db`.
pub fn default_session_base(db_path: &Path) -> PathBuf {
    db_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("sessions")
}

/// A session-store failure. Callers on a launch path map it to `NonFailover`.
#[derive(Debug)]
pub struct SessionStoreError {
    op: &'static str,
    component: String,
    source: io::Error,
}

impl SessionStoreError {
    fn new(op: &'static str, component: impl Into<String>, source: io::Error) -> Self {
        Self {
            op,
            component: component.into(),
            source,
        }
    }
}

impl fmt::Display for SessionStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "session store: {} {}: {}",
            self.op, self.component, self.source
        )
    }
}

impl std::error::Error for SessionStoreError {}

type Result<T> = std::result::Result<T, SessionStoreError>;

/// What `fstatat(AT_SYMLINK_NOFOLLOW)` reports at the session file name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionFile {
    Absent,
    Regular {
        len: u64,
    },
    /// A symlink, directory, FIFO, socket, or device. Never resumable.
    NotRegular,
}

impl SessionFile {
    /// Resume condition 3: a regular file of non-zero size.
    pub fn is_resumable(self) -> bool {
        matches!(self, SessionFile::Regular { len } if len > 0)
    }
}

/// An opened, validated slot directory. All session file operations go
/// through its descriptor.
#[derive(Debug)]
pub struct SlotDir {
    dir: OwnedFd,
    path: PathBuf,
}

impl SlotDir {
    /// `<session_base>/<task id>/<slot>`; exported to slot runs as
    /// `QUORUM_SESSION_DIR`.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// `<slot dir>/session`; the `{session_file}` placeholder value.
    pub fn session_file_path(&self) -> PathBuf {
        self.path.join(SESSION_FILE)
    }

    pub fn session_file(&self) -> Result<SessionFile> {
        let name = session_file_name();
        let st = fstatat_nofollow(self.dir.as_raw_fd(), &name)
            .map_err(|e| SessionStoreError::new("stat", "session file", e))?;
        Ok(match st {
            None => SessionFile::Absent,
            Some(st) if file_type(&st) == libc::S_IFREG => SessionFile::Regular {
                len: u64::try_from(st.st_size).unwrap_or(0),
            },
            Some(_) => SessionFile::NotRegular,
        })
    }

    /// Fresh-start removal of the session file. A directory at that name is
    /// removed recursively without following links. Absence is success.
    pub fn remove_session_file(&self) -> Result<()> {
        // A run may have removed write permission from its slot directory
        // since this descriptor was opened.
        fchmod(self.dir.as_raw_fd(), DIR_MODE)
            .map_err(|e| SessionStoreError::new("chmod", "slot directory", e))?;
        let name = session_file_name();
        remove_entry_at(self.dir.as_raw_fd(), &name, 0)
            .map_err(|e| SessionStoreError::new("remove", "session file", e))
    }
}

/// Create (if missing) and open `<session_base>/<task id>/<slot>/`.
///
/// `session_base` itself is created with mode `0700` if missing. The task and
/// slot directories are created with, and reset to, mode `0700`. A symlink or
/// non-directory at any component is an error.
pub fn open_slot(session_base: &Path, task_id: i64, slot: Slot) -> Result<SlotDir> {
    let task_name = task_dir_name(task_id)?;
    let base = open_base(session_base, true)?
        .expect("open_base with create returns a directory or an error");
    let task_dir = ensure_owned_child_dir(&base, &task_name)
        .map_err(|e| SessionStoreError::new("open", format!("task {task_id} directory"), e))?;
    let slot_name = CString::new(slot.as_str()).expect("slot names contain no NUL");
    let slot_dir = ensure_owned_child_dir(&task_dir, &slot_name).map_err(|e| {
        SessionStoreError::new("open", format!("task {task_id} slot {slot} directory"), e)
    })?;
    Ok(SlotDir {
        dir: slot_dir,
        path: session_base.join(task_id.to_string()).join(slot.as_str()),
    })
}

/// Remove `<session_base>/<task id>/` and everything below it without
/// following links. Returns whether an entry existed. A missing
/// `session_base` is not an error.
pub fn remove_task_tree(session_base: &Path, task_id: i64) -> Result<bool> {
    let name = task_dir_name(task_id)?;
    let Some(base) = open_base(session_base, false)? else {
        return Ok(false);
    };
    let existed = fstatat_nofollow(base.as_raw_fd(), &name)
        .map_err(|e| SessionStoreError::new("stat", format!("task {task_id} tree"), e))?
        .is_some();
    remove_entry_at(base.as_raw_fd(), &name, 0)
        .map_err(|e| SessionStoreError::new("remove", format!("task {task_id} tree"), e))?;
    Ok(existed)
}

/// Task ids with an entry directly under `session_base`, ascending. Entries
/// whose name is not a canonical positive task id are not Quorum's and are
/// left alone. A missing `session_base` lists nothing.
pub fn list_task_trees(session_base: &Path) -> Result<Vec<i64>> {
    let Some(base) = open_base(session_base, false)? else {
        return Ok(Vec::new());
    };
    let names = list_names(&base).map_err(|e| SessionStoreError::new("list", "session_base", e))?;
    let mut ids: Vec<i64> = names
        .iter()
        .filter_map(|name| parse_task_dir_name(name.to_bytes()))
        .collect();
    ids.sort_unstable();
    Ok(ids)
}

/// Result of one sweep pass.
#[derive(Debug, Default)]
pub struct SweepOutcome {
    pub removed: Vec<i64>,
    pub failed: Vec<(i64, SessionStoreError)>,
}

/// Remove the trees of tasks that are `done` or `cancelled`, or that do not
/// exist, under `session_base` only, at most `limit` per pass. Task status is
/// read with short autocommit statements before any filesystem removal, so no
/// read transaction is held across it. `failed` and parked tasks keep their
/// trees.
pub fn sweep_finished(
    conn: &rusqlite::Connection,
    session_base: &Path,
    limit: usize,
) -> quorum_core::error::Result<SweepOutcome> {
    let ids = list_task_trees(session_base)
        .map_err(|e| quorum_core::error::QuorumError::Io(e.to_string()))?;
    let mut finished = Vec::new();
    {
        let mut stmt = conn.prepare("SELECT status FROM tasks WHERE id = ?1")?;
        for id in ids {
            if finished.len() >= limit {
                break;
            }
            let status: Option<String> = rusqlite::OptionalExtension::optional(
                stmt.query_row([id], |row| row.get::<_, String>(0)),
            )?;
            if matches!(status.as_deref(), None | Some("done") | Some("cancelled")) {
                finished.push(id);
            }
        }
    }
    let mut outcome = SweepOutcome::default();
    for id in finished {
        match remove_task_tree(session_base, id) {
            Ok(_) => outcome.removed.push(id),
            Err(e) => outcome.failed.push((id, e)),
        }
    }
    Ok(outcome)
}

// ── Internals ──────────────────────────────────────────────────────────────

fn session_file_name() -> CString {
    CString::new(SESSION_FILE).expect("constant contains no NUL")
}

fn task_dir_name(task_id: i64) -> Result<CString> {
    if task_id <= 0 {
        return Err(SessionStoreError::new(
            "validate",
            "task id",
            io::Error::new(io::ErrorKind::InvalidInput, "task id must be positive"),
        ));
    }
    Ok(CString::new(task_id.to_string()).expect("decimal digits contain no NUL"))
}

fn parse_task_dir_name(name: &[u8]) -> Option<i64> {
    if name.is_empty() || name[0] == b'0' || !name.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(name).ok()?.parse::<i64>().ok()
}

fn not_real_directory() -> io::Error {
    io::Error::other("not a real directory")
}

fn last_error() -> io::Error {
    io::Error::last_os_error()
}

fn file_type(st: &libc::stat) -> libc::mode_t {
    st.st_mode & libc::S_IFMT
}

/// Open `session_base` with `O_NOFOLLOW | O_DIRECTORY` after an `lstat` that
/// shows a real directory, and require the opened inode to be the checked
/// one. `None` only when absent and `create` is false.
fn open_base(base: &Path, create: bool) -> Result<Option<OwnedFd>> {
    let mut created = false;
    if create {
        match std::fs::DirBuilder::new().mode(0o700).create(base) {
            Ok(()) => created = true,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(SessionStoreError::new("create", "session_base", e)),
        }
    }
    let c_base = CString::new(base.as_os_str().as_bytes()).map_err(|_| {
        SessionStoreError::new(
            "validate",
            "session_base",
            io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"),
        )
    })?;
    let checked = match lstat(&c_base) {
        Ok(st) => st,
        Err(e) if !create && e.raw_os_error() == Some(libc::ENOENT) => return Ok(None),
        Err(e) => return Err(SessionStoreError::new("stat", "session_base", e)),
    };
    if file_type(&checked) != libc::S_IFDIR {
        return Err(SessionStoreError::new(
            "open",
            "session_base",
            not_real_directory(),
        ));
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(base)
        .map_err(|e| SessionStoreError::new("open", "session_base", e))?;
    let fd = OwnedFd::from(file);
    let st =
        fstat(fd.as_raw_fd()).map_err(|e| SessionStoreError::new("stat", "session_base", e))?;
    if st.st_dev != checked.st_dev || st.st_ino != checked.st_ino {
        return Err(SessionStoreError::new(
            "open",
            "session_base",
            not_real_directory(),
        ));
    }
    if created {
        // Defeat a umask that left more than owner bits; mkdir never adds bits.
        fchmod(fd.as_raw_fd(), DIR_MODE)
            .map_err(|e| SessionStoreError::new("chmod", "session_base", e))?;
    }
    Ok(Some(fd))
}

/// `mkdirat(0700)` if missing, open as a checked real directory, and reset
/// its mode to `0700`.
fn ensure_owned_child_dir(parent: &OwnedFd, name: &CStr) -> io::Result<OwnedFd> {
    if unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), DIR_MODE) } != 0 {
        let e = last_error();
        if e.raw_os_error() != Some(libc::EEXIST) {
            return Err(e);
        }
    }
    let dir = open_child_dir(parent.as_raw_fd(), name)?
        .ok_or_else(|| io::Error::from_raw_os_error(libc::ENOENT))?;
    fchmod(dir.as_raw_fd(), DIR_MODE)?;
    Ok(dir)
}

/// Open `name` under `parent` only if `fstatat(AT_SYMLINK_NOFOLLOW)` shows a
/// real directory, restoring owner `rwx` first if a run removed it, then
/// `openat(O_NOFOLLOW | O_DIRECTORY)` and require the same inode. `None` when
/// absent.
fn open_child_dir(parent: RawFd, name: &CStr) -> io::Result<Option<OwnedFd>> {
    let Some(st) = fstatat_nofollow(parent, name)? else {
        return Ok(None);
    };
    if file_type(&st) != libc::S_IFDIR {
        return Err(not_real_directory());
    }
    if st.st_mode & DIR_MODE != DIR_MODE {
        // AT_SYMLINK_NOFOLLOW: a directory swapped for a symlink after the
        // stat above is refused rather than having its target changed.
        let mode = (st.st_mode & 0o7777) | DIR_MODE;
        if unsafe { libc::fchmodat(parent, name.as_ptr(), mode, libc::AT_SYMLINK_NOFOLLOW) } != 0 {
            return Err(last_error());
        }
    }
    let fd = unsafe {
        libc::openat(
            parent,
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        let e = last_error();
        return Err(match e.raw_os_error() {
            Some(libc::ELOOP) | Some(libc::ENOTDIR) => not_real_directory(),
            _ => e,
        });
    }
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    let opened = fstat(fd.as_raw_fd())?;
    if opened.st_dev != st.st_dev || opened.st_ino != st.st_ino {
        return Err(not_real_directory());
    }
    Ok(Some(fd))
}

/// Remove `name` under `parent`: a non-directory (including a symlink) is
/// unlinked; a real directory is emptied recursively and then removed.
/// Absence is success.
fn remove_entry_at(parent: RawFd, name: &CStr, depth: usize) -> io::Result<()> {
    let Some(st) = fstatat_nofollow(parent, name)? else {
        return Ok(());
    };
    if file_type(&st) != libc::S_IFDIR {
        return unlink_at(parent, name, 0);
    }
    if depth >= MAX_REMOVE_DEPTH {
        return Err(io::Error::other("session tree exceeds removal depth"));
    }
    let Some(dir) = open_child_dir(parent, name)? else {
        return Ok(());
    };
    for child in list_names(&dir)? {
        remove_entry_at(dir.as_raw_fd(), &child, depth + 1)?;
    }
    drop(dir);
    // ENOTEMPTY here also covers any entry readdir failed to report.
    unlink_at(parent, name, libc::AT_REMOVEDIR)
}

fn unlink_at(parent: RawFd, name: &CStr, flags: libc::c_int) -> io::Result<()> {
    if unsafe { libc::unlinkat(parent, name.as_ptr(), flags) } != 0 {
        let e = last_error();
        if e.raw_os_error() != Some(libc::ENOENT) {
            return Err(e);
        }
    }
    Ok(())
}

/// Entry names of an open directory, excluding `.` and `..`.
fn list_names(dir: &OwnedFd) -> io::Result<Vec<CString>> {
    let dup = unsafe { libc::fcntl(dir.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
    if dup < 0 {
        return Err(last_error());
    }
    let dirp = unsafe { libc::fdopendir(dup) };
    if dirp.is_null() {
        let e = last_error();
        unsafe { libc::close(dup) };
        return Err(e);
    }
    unsafe { libc::rewinddir(dirp) };
    let mut names = Vec::new();
    loop {
        // A NULL return ends the listing. A read error that hides entries
        // surfaces as the caller's ENOTEMPTY on directory removal.
        let entry = unsafe { libc::readdir(dirp) };
        if entry.is_null() {
            break;
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        let bytes = name.to_bytes();
        if bytes == b"." || bytes == b".." {
            continue;
        }
        names.push(name.to_owned());
    }
    unsafe { libc::closedir(dirp) };
    Ok(names)
}

fn fstatat_nofollow(dir: RawFd, name: &CStr) -> io::Result<Option<libc::stat>> {
    let mut st = MaybeUninit::<libc::stat>::uninit();
    if unsafe {
        libc::fstatat(
            dir,
            name.as_ptr(),
            st.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        let e = last_error();
        if e.raw_os_error() == Some(libc::ENOENT) {
            return Ok(None);
        }
        return Err(e);
    }
    Ok(Some(unsafe { st.assume_init() }))
}

fn lstat(path: &CStr) -> io::Result<libc::stat> {
    let mut st = MaybeUninit::<libc::stat>::uninit();
    if unsafe { libc::lstat(path.as_ptr(), st.as_mut_ptr()) } != 0 {
        return Err(last_error());
    }
    Ok(unsafe { st.assume_init() })
}

fn fstat(fd: RawFd) -> io::Result<libc::stat> {
    let mut st = MaybeUninit::<libc::stat>::uninit();
    if unsafe { libc::fstat(fd, st.as_mut_ptr()) } != 0 {
        return Err(last_error());
    }
    Ok(unsafe { st.assume_init() })
}

fn fchmod(fd: RawFd, mode: libc::mode_t) -> io::Result<()> {
    if unsafe { libc::fchmod(fd, mode) } != 0 {
        return Err(last_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{symlink, PermissionsExt};

    fn mode_of(path: &Path) -> u32 {
        std::fs::symlink_metadata(path)
            .unwrap()
            .permissions()
            .mode()
            & 0o7777
    }

    fn chmod(path: &Path, mode: u32) {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }

    fn assert_not_real_dir(err: SessionStoreError) {
        assert!(
            err.to_string().contains("not a real directory"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn layout_and_modes_for_every_slot() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path().join("sessions");
        for slot in Slot::ALL {
            let dir = open_slot(&base, 42, slot).unwrap();
            let expected = base.join("42").join(slot.as_str());
            assert_eq!(dir.path(), expected);
            assert_eq!(dir.session_file_path(), expected.join("session"));
            assert!(std::fs::symlink_metadata(&expected).unwrap().is_dir());
            assert_eq!(mode_of(&expected), 0o700);
            assert_eq!(dir.session_file().unwrap(), SessionFile::Absent);
        }
        assert_eq!(mode_of(&base), 0o700);
        assert_eq!(mode_of(&base.join("42")), 0o700);
        let mut names: Vec<_> = std::fs::read_dir(base.join("42"))
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        assert_eq!(names, ["reviewer-r1", "reviewer-r2", "worker"]);
        // Reopening is idempotent.
        open_slot(&base, 42, Slot::Worker).unwrap();
        assert_eq!(
            default_session_base(Path::new("/state/quorum.db")),
            PathBuf::from("/state/sessions")
        );
    }

    #[test]
    fn reopening_resets_modes_a_run_removed() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path().join("sessions");
        let slot = open_slot(&base, 7, Slot::Worker)
            .unwrap()
            .path()
            .to_path_buf();
        chmod(&slot, 0o500);
        chmod(&base.join("7"), 0o000);
        let dir = open_slot(&base, 7, Slot::Worker).unwrap();
        assert_eq!(mode_of(&base.join("7")), 0o700);
        assert_eq!(mode_of(&slot), 0o700);
        // The run removes its own write permission after the daemon opened
        // the slot; fresh-start removal still succeeds.
        std::fs::write(slot.join("session"), b"x").unwrap();
        chmod(&slot, 0o500);
        dir.remove_session_file().unwrap();
        assert_eq!(dir.session_file().unwrap(), SessionFile::Absent);
    }

    #[test]
    fn rejects_task_id_that_is_not_positive() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path().join("sessions");
        assert!(open_slot(&base, 0, Slot::Worker).is_err());
        assert!(open_slot(&base, -3, Slot::Worker).is_err());
        assert!(remove_task_tree(&base, 0).is_err());
    }

    #[test]
    fn symlink_or_non_directory_at_session_base_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("elsewhere");
        std::fs::create_dir(&target).unwrap();
        let base = root.path().join("sessions");
        symlink(&target, &base).unwrap();
        assert_not_real_dir(open_slot(&base, 1, Slot::Worker).unwrap_err());
        assert_not_real_dir(remove_task_tree(&base, 1).unwrap_err());
        assert_not_real_dir(list_task_trees(&base).unwrap_err());
        assert_eq!(std::fs::read_dir(&target).unwrap().count(), 0);

        let file_base = root.path().join("file-base");
        std::fs::write(&file_base, b"").unwrap();
        assert_not_real_dir(open_slot(&file_base, 1, Slot::Worker).unwrap_err());
    }

    #[test]
    fn symlink_or_non_directory_at_task_component_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path().join("sessions");
        std::fs::create_dir(&base).unwrap();
        let target = root.path().join("elsewhere");
        std::fs::create_dir(&target).unwrap();
        symlink(&target, base.join("5")).unwrap();
        assert_not_real_dir(open_slot(&base, 5, Slot::Worker).unwrap_err());
        assert_eq!(std::fs::read_dir(&target).unwrap().count(), 0);

        std::fs::write(base.join("6"), b"").unwrap();
        assert_not_real_dir(open_slot(&base, 6, Slot::Worker).unwrap_err());
    }

    #[test]
    fn symlink_or_non_directory_at_slot_component_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path().join("sessions");
        let target = root.path().join("elsewhere");
        std::fs::create_dir(&target).unwrap();
        chmod(&target, 0o755);
        open_slot(&base, 9, Slot::ReviewerR1).unwrap();
        symlink(&target, base.join("9").join("worker")).unwrap();
        assert_not_real_dir(open_slot(&base, 9, Slot::Worker).unwrap_err());
        assert_eq!(std::fs::read_dir(&target).unwrap().count(), 0);
        assert_eq!(
            mode_of(&target),
            0o755,
            "symlink target mode must be untouched"
        );

        std::fs::write(base.join("9").join("reviewer-r2"), b"").unwrap();
        assert_not_real_dir(open_slot(&base, 9, Slot::ReviewerR2).unwrap_err());
    }

    #[test]
    fn session_file_states_are_reported_without_following_links() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path().join("sessions");
        let dir = open_slot(&base, 3, Slot::Worker).unwrap();
        let file = dir.session_file_path();

        std::fs::write(&file, b"").unwrap();
        assert_eq!(dir.session_file().unwrap(), SessionFile::Regular { len: 0 });
        assert!(!dir.session_file().unwrap().is_resumable());

        std::fs::write(&file, b"conversation").unwrap();
        assert_eq!(
            dir.session_file().unwrap(),
            SessionFile::Regular { len: 12 }
        );
        assert!(dir.session_file().unwrap().is_resumable());
        std::fs::remove_file(&file).unwrap();

        let outside = root.path().join("outside");
        std::fs::write(&outside, b"daemon secret").unwrap();
        symlink(&outside, &file).unwrap();
        assert_eq!(dir.session_file().unwrap(), SessionFile::NotRegular);
        assert!(!dir.session_file().unwrap().is_resumable());
        std::fs::remove_file(&file).unwrap();

        std::fs::create_dir(&file).unwrap();
        assert_eq!(dir.session_file().unwrap(), SessionFile::NotRegular);
        std::fs::remove_dir(&file).unwrap();

        let c_file = CString::new(file.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(c_file.as_ptr(), 0o600) }, 0);
        assert_eq!(dir.session_file().unwrap(), SessionFile::NotRegular);
    }

    #[test]
    fn fresh_start_removal_handles_files_links_and_directories() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path().join("sessions");
        let dir = open_slot(&base, 4, Slot::Worker).unwrap();
        let file = dir.session_file_path();

        // Absent is success.
        dir.remove_session_file().unwrap();

        std::fs::write(&file, b"state").unwrap();
        dir.remove_session_file().unwrap();
        assert_eq!(dir.session_file().unwrap(), SessionFile::Absent);

        // A symlink is removed as a link; its target survives.
        let outside_file = root.path().join("outside-file");
        std::fs::write(&outside_file, b"keep").unwrap();
        symlink(&outside_file, &file).unwrap();
        dir.remove_session_file().unwrap();
        assert_eq!(dir.session_file().unwrap(), SessionFile::Absent);
        assert_eq!(std::fs::read(&outside_file).unwrap(), b"keep");

        // A directory is removed recursively without following links, even
        // where a run removed permissions inside it.
        let outside_dir = root.path().join("outside-dir");
        std::fs::create_dir(&outside_dir).unwrap();
        std::fs::write(outside_dir.join("precious"), b"keep").unwrap();
        std::fs::create_dir_all(file.join("a/b")).unwrap();
        std::fs::write(file.join("a/b/leaf"), b"x").unwrap();
        symlink(&outside_dir, file.join("a/link-dir")).unwrap();
        symlink(&outside_file, file.join("link-file")).unwrap();
        chmod(&file.join("a/b"), 0o000);
        chmod(&file.join("a"), 0o500);
        dir.remove_session_file().unwrap();
        assert_eq!(dir.session_file().unwrap(), SessionFile::Absent);
        assert_eq!(
            std::fs::read(outside_dir.join("precious")).unwrap(),
            b"keep"
        );
        assert_eq!(std::fs::read(&outside_file).unwrap(), b"keep");
    }

    #[test]
    fn removal_failure_is_an_error() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path().join("sessions");
        let dir = open_slot(&base, 8, Slot::Worker).unwrap();
        let mut deep = dir.session_file_path();
        for _ in 0..=MAX_REMOVE_DEPTH {
            deep.push("d");
        }
        std::fs::create_dir_all(&deep).unwrap();
        let err = dir.remove_session_file().unwrap_err();
        assert!(err.to_string().contains("removal depth"), "{err}");
        assert_eq!(dir.session_file().unwrap(), SessionFile::NotRegular);

        let err = remove_task_tree(&base, 8).unwrap_err();
        assert!(err.to_string().contains("task 8 tree"), "{err}");
        assert!(base.join("8").exists());
    }

    #[test]
    fn task_tree_removal_and_listing() {
        let root = tempfile::tempdir().unwrap();
        let base = root.path().join("sessions");
        // A missing session_base lists and removes nothing.
        assert!(list_task_trees(&base).unwrap().is_empty());
        assert!(!remove_task_tree(&base, 1).unwrap());
        assert!(!base.exists(), "removal must not create session_base");

        for slot in Slot::ALL {
            let dir = open_slot(&base, 11, slot).unwrap();
            std::fs::write(dir.session_file_path(), b"s").unwrap();
            chmod(dir.path(), 0o500);
        }
        open_slot(&base, 2, Slot::Worker).unwrap();
        let outside = root.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("precious"), b"keep").unwrap();
        symlink(&outside, base.join("12")).unwrap();
        for foreign in ["007", "0", "abc", "-1", "12a"] {
            std::fs::create_dir(base.join(foreign)).unwrap();
        }
        assert_eq!(list_task_trees(&base).unwrap(), vec![2, 11, 12]);

        assert!(remove_task_tree(&base, 11).unwrap());
        assert!(!base.join("11").exists());
        assert!(!remove_task_tree(&base, 11).unwrap());

        // A symlinked task entry is unlinked; its target survives.
        assert!(remove_task_tree(&base, 12).unwrap());
        assert!(std::fs::symlink_metadata(base.join("12")).is_err());
        assert_eq!(std::fs::read(outside.join("precious")).unwrap(), b"keep");

        assert_eq!(list_task_trees(&base).unwrap(), vec![2]);
        assert!(base.join("007").is_dir(), "foreign entries are left alone");
    }

    #[test]
    fn sweep_removes_done_cancelled_and_unknown_task_trees_only() {
        use quorum_core::tasks;
        let root = tempfile::tempdir().unwrap();
        let db_path = root.path().join("quorum.db");
        let mut conn = quorum_core::db::open(&db_path).unwrap();
        let mut id_for = |status: &str| {
            let id = tasks::create(
                &mut conn, "boss", status, None, 50, None, None, None, None, 1,
            )
            .unwrap();
            conn.execute(
                "UPDATE tasks SET status=?2 WHERE id=?1",
                rusqlite::params![id, status],
            )
            .unwrap();
            id
        };
        let open = id_for("open");
        let working = id_for("working");
        let rework = id_for("rework");
        let failed = id_for("failed");
        let done = id_for("done");
        let cancelled = id_for("cancelled");
        let unknown = 9_999;
        let base = default_session_base(&db_path);
        for id in [open, working, rework, failed, done, cancelled, unknown] {
            open_slot(&base, id, Slot::Worker).unwrap();
        }

        let outcome = sweep_finished(&conn, &base, usize::MAX).unwrap();
        assert!(outcome.failed.is_empty());
        assert_eq!(outcome.removed, vec![done, cancelled, unknown]);
        assert_eq!(
            list_task_trees(&base).unwrap(),
            vec![open, working, rework, failed]
        );

        // The limit bounds a pass; the next pass continues.
        for id in [done, cancelled, unknown] {
            open_slot(&base, id, Slot::Worker).unwrap();
        }
        let first = sweep_finished(&conn, &base, 2).unwrap();
        assert_eq!(first.removed, vec![done, cancelled]);
        let second = sweep_finished(&conn, &base, 2).unwrap();
        assert_eq!(second.removed, vec![unknown]);

        // A missing session_base is a no-op.
        let missing = root.path().join("absent");
        let none = sweep_finished(&conn, &missing, usize::MAX).unwrap();
        assert!(none.removed.is_empty() && none.failed.is_empty());
        assert!(!missing.exists());
    }

    #[test]
    fn sweep_reports_per_tree_failures_and_continues() {
        let root = tempfile::tempdir().unwrap();
        let db_path = root.path().join("quorum.db");
        let conn = quorum_core::db::open(&db_path).unwrap();
        let base = default_session_base(&db_path);
        let stuck = open_slot(&base, 500, Slot::Worker).unwrap();
        let mut deep = stuck.session_file_path();
        for _ in 0..=MAX_REMOVE_DEPTH {
            deep.push("d");
        }
        std::fs::create_dir_all(&deep).unwrap();
        open_slot(&base, 501, Slot::Worker).unwrap();
        let outcome = sweep_finished(&conn, &base, usize::MAX).unwrap();
        assert_eq!(outcome.removed, vec![501]);
        assert_eq!(outcome.failed.len(), 1);
        assert_eq!(outcome.failed[0].0, 500);
    }
}
