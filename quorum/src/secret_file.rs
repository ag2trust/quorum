//! Read a one-line secret, such as an API key, from a file.
//!
//! A secret in a file stays out of the process environment, which child
//! processes inherit and container tooling displays. Symbolic links are
//! followed, as secret volumes use them; every check is made on the
//! descriptor that is read, so the file checked is the file read.

use std::path::Path;

/// Largest secret file accepted, in bytes, including a trailing newline.
pub(crate) const MAX_SECRET_FILE_BYTES: u64 = 4096;

/// Read the secret held in `path`.
///
/// The path, after any symbolic links, must name a regular file of at most
/// [`MAX_SECRET_FILE_BYTES`] that neither its group nor others may write; its
/// owner and the directory that holds it are trusted. Group and other read
/// access are allowed: a secret mounted read-only into a container is often
/// owned by another user and readable through its group or by everyone in the
/// container. Who may read the file is the deployer's decision, made by the
/// mount.
///
/// One trailing newline (`\n` or `\r\n`) is removed. What remains must be one
/// or more printable ASCII characters with no spaces. An error describes the
/// file, never its contents.
pub(crate) fn read(path: &Path) -> Result<String, String> {
    use std::os::unix::fs::MetadataExt as _;
    let (file, metadata) = open_and_stat(path)?;
    if metadata.mode() & 0o022 != 0 {
        return Err("must not be writable by group or others".into());
    }
    parse(file, &metadata)
}

/// Read the secret held in `path`, as [`read`], but trust no one besides
/// `owner_uid` to reach it at all.
///
/// Unlike `read`, group and other access of any kind — not just write — is
/// refused, and the file must be owned by `owner_uid`. Use this for a secret
/// that is itself an access credential rather than one deployed by a trusted
/// mount: nothing short of the owner may even read it.
pub(crate) fn read_owner_only(path: &Path, owner_uid: u32) -> Result<String, String> {
    use std::os::unix::fs::MetadataExt as _;
    let (file, metadata) = open_and_stat(path)?;
    if metadata.uid() != owner_uid {
        return Err("must be owned by the user reading it".into());
    }
    if metadata.mode() & 0o077 != 0 {
        return Err("must not be accessible by group or others".into());
    }
    parse(file, &metadata)
}

/// Open `path`, following symbolic links, and stat the descriptor that was
/// opened so every later check is made on the file that is read.
fn open_and_stat(path: &Path) -> Result<(std::fs::File, std::fs::Metadata), String> {
    use std::os::unix::fs::OpenOptionsExt as _;
    let file = std::fs::OpenOptions::new()
        .read(true)
        // Never block on a FIFO with no writer and never adopt a terminal:
        // what was opened is checked next.
        .custom_flags(libc::O_NONBLOCK | libc::O_NOCTTY)
        .open(path)
        .map_err(|error| format!("cannot be opened: {error}"))?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("cannot be read: {error}"))?;
    if !metadata.is_file() {
        return Err("must be a regular file".into());
    }
    Ok((file, metadata))
}

/// Read and validate the secret from an already opened and stat-checked
/// file. Permission and ownership checks happen before this is called; this
/// is only the size bound, trailing-newline trim, and content shape.
fn parse(file: std::fs::File, metadata: &std::fs::Metadata) -> Result<String, String> {
    use std::io::Read as _;
    let too_large = || format!("must not be larger than {MAX_SECRET_FILE_BYTES} bytes");
    if metadata.len() > MAX_SECRET_FILE_BYTES {
        return Err(too_large());
    }
    let mut contents = Vec::new();
    // Bounded again in case the file grew after it was checked.
    file.take(MAX_SECRET_FILE_BYTES + 1)
        .read_to_end(&mut contents)
        .map_err(|error| format!("cannot be read: {error}"))?;
    if contents.len() as u64 > MAX_SECRET_FILE_BYTES {
        return Err(too_large());
    }
    let mut secret = contents.as_slice();
    if let Some(line) = secret.strip_suffix(b"\n") {
        secret = line.strip_suffix(b"\r").unwrap_or(line);
    }
    if secret.is_empty() {
        return Err("is empty".into());
    }
    if !secret.iter().all(u8::is_ascii_graphic) {
        return Err(
            "must hold one line of printable ASCII with no spaces or control characters".into(),
        );
    }
    // Printable ASCII is valid UTF-8.
    Ok(String::from_utf8_lossy(secret).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;

    const SECRET: &str = "file-secret-9c2e";

    fn file_with(contents: &[u8], mode: u32) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret");
        std::fs::write(&path, contents).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        (dir, path)
    }

    fn refusal(path: &Path) -> String {
        let reason = read(path).expect_err("the file should be refused");
        assert!(!reason.contains(SECRET), "{reason}");
        reason
    }

    #[test]
    fn reads_one_line_and_removes_only_its_trailing_newline() {
        for contents in [
            SECRET.to_string(),
            format!("{SECRET}\n"),
            format!("{SECRET}\r\n"),
        ] {
            let (_dir, path) = file_with(contents.as_bytes(), 0o600);
            assert_eq!(read(&path).unwrap(), SECRET, "{contents:?}");
        }
        for contents in [
            format!("{SECRET}\n\n"),
            format!("{SECRET}\r"),
            format!("{SECRET} \n"),
            format!(" {SECRET}"),
            format!("{SECRET}\nsecond-line"),
        ] {
            let (_dir, path) = file_with(contents.as_bytes(), 0o600);
            refusal(&path);
        }
    }

    #[test]
    fn a_secret_readable_by_its_group_or_others_is_accepted() {
        // A read-only mounted secret: owned by someone, readable through the
        // group or by everyone, writable by nobody else.
        for mode in [0o400, 0o440, 0o444, 0o640, 0o644] {
            let (_dir, path) = file_with(SECRET.as_bytes(), mode);
            assert_eq!(read(&path).unwrap(), SECRET, "mode {mode:o}");
        }
    }

    #[test]
    fn a_secret_others_may_write_is_refused() {
        for mode in [0o620, 0o602, 0o660, 0o666] {
            let (_dir, path) = file_with(SECRET.as_bytes(), mode);
            let reason = refusal(&path);
            assert!(reason.contains("writable"), "mode {mode:o}: {reason}");
        }
    }

    #[test]
    fn only_a_regular_file_is_read() {
        let (dir, _target) = file_with(SECRET.as_bytes(), 0o600);

        assert!(refusal(dir.path()).contains("regular file"));

        // Opening a FIFO with no writer must not block.
        let fifo = dir.path().join("fifo");
        let c_path = std::ffi::CString::new(fifo.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
        assert!(refusal(&fifo).contains("regular file"));

        assert!(refusal(&dir.path().join("missing")).contains("cannot be opened"));
    }

    #[test]
    fn a_symbolic_link_is_followed_and_its_target_checked() {
        use std::os::unix::fs::symlink;
        // A projected secret volume: `key -> ..data/key`, `..data -> ..<ts>/`.
        let (dir, _target) = file_with(b"", 0o600);
        let version = dir.path().join("..2026_11_12");
        std::fs::create_dir(&version).unwrap();
        std::fs::write(version.join("key"), SECRET).unwrap();
        std::fs::set_permissions(version.join("key"), std::fs::Permissions::from_mode(0o444))
            .unwrap();
        symlink("..2026_11_12", dir.path().join("..data")).unwrap();
        symlink("..data/key", dir.path().join("key")).unwrap();
        assert_eq!(read(&dir.path().join("key")).unwrap(), SECRET);

        // The target is held to every rule a file named directly is.
        let to_dir = dir.path().join("to-dir");
        symlink(&version, &to_dir).unwrap();
        assert!(refusal(&to_dir).contains("regular file"));

        let fifo = dir.path().join("fifo");
        let c_path = std::ffi::CString::new(fifo.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
        let to_fifo = dir.path().join("to-fifo");
        symlink(&fifo, &to_fifo).unwrap();
        assert!(refusal(&to_fifo).contains("regular file"));

        let writable = dir.path().join("writable");
        std::fs::write(&writable, SECRET).unwrap();
        std::fs::set_permissions(&writable, std::fs::Permissions::from_mode(0o620)).unwrap();
        let to_writable = dir.path().join("to-writable");
        symlink(&writable, &to_writable).unwrap();
        assert!(refusal(&to_writable).contains("writable"));

        let dangling = dir.path().join("dangling");
        symlink(dir.path().join("missing"), &dangling).unwrap();
        assert!(refusal(&dangling).contains("cannot be opened"));
    }

    #[test]
    fn size_and_contents_are_bounded() {
        let at_limit = "k".repeat(MAX_SECRET_FILE_BYTES as usize - 1) + "\n";
        let (_dir, path) = file_with(at_limit.as_bytes(), 0o600);
        assert_eq!(
            read(&path).unwrap().len(),
            MAX_SECRET_FILE_BYTES as usize - 1
        );

        let over = format!("{SECRET}{}", "k".repeat(MAX_SECRET_FILE_BYTES as usize));
        let (_dir, path) = file_with(over.as_bytes(), 0o600);
        assert!(refusal(&path).contains("larger than"));

        for contents in [&b""[..], b"\n", b"\r\n"] {
            let (_dir, path) = file_with(contents, 0o600);
            assert!(refusal(&path).contains("empty"), "{contents:?}");
        }
        // A carriage return alone is not a newline.
        let (_dir, path) = file_with(b"\r", 0o600);
        assert!(refusal(&path).contains("printable"));

        for contents in [
            format!("{SECRET} tail"),
            format!("{SECRET}\ttail"),
            format!("{SECRET}\x1b[0m"),
            format!("{SECRET}\0"),
            format!("{SECRET}\u{e9}"),
        ] {
            let (_dir, path) = file_with(contents.as_bytes(), 0o600);
            assert!(refusal(&path).contains("printable"), "{contents:?}");
        }
    }

    #[test]
    fn owner_only_accepts_the_owner_and_refuses_everyone_else() {
        let euid = unsafe { libc::geteuid() };

        let (_dir, path) = file_with(SECRET.as_bytes(), 0o600);
        assert_eq!(read_owner_only(&path, euid).unwrap(), SECRET);

        let reason = read_owner_only(&path, euid + 1).expect_err("owner mismatch is refused");
        assert!(!reason.contains(SECRET), "{reason}");
        assert!(reason.contains("owned by"), "{reason}");
    }

    #[test]
    fn owner_only_refuses_any_group_or_other_access_not_just_write() {
        let euid = unsafe { libc::geteuid() };
        // `read` accepts these for a trusted, read-only mounted secret;
        // `read_owner_only` must not, since nothing but the owner may reach
        // this secret at all.
        for mode in [0o640, 0o644, 0o604, 0o606, 0o620, 0o666] {
            let (_dir, path) = file_with(SECRET.as_bytes(), mode);
            let reason = read_owner_only(&path, euid).expect_err("group/other access is refused");
            assert!(!reason.contains(SECRET), "{reason}");
            assert!(
                reason.contains("group or others"),
                "mode {mode:o}: {reason}"
            );
        }
    }
}
