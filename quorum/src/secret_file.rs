//! Read a one-line secret, such as an API key, from a file.
//!
//! A secret in a file stays out of the process environment, which child
//! processes inherit and container tooling displays. The file is checked on
//! the descriptor that is read, so the file checked is the file read.

use std::path::Path;

/// Largest secret file accepted, in bytes, including a trailing newline.
pub(crate) const MAX_SECRET_FILE_BYTES: u64 = 4096;

/// Read the secret held in `path`.
///
/// The path's last component must not be a symbolic link, and it must name a
/// regular file of at most [`MAX_SECRET_FILE_BYTES`] that neither its group
/// nor others may write, so no other user can substitute the secret. Group
/// and other read access are allowed: a secret mounted read-only into a
/// container is often owned by another user and readable through its group
/// or by everyone in the container. Who may read the file is the deployer's
/// decision, made by the mount.
///
/// One trailing newline (`\n` or `\r\n`) is removed. What remains must be one
/// or more printable ASCII characters with no spaces. An error describes the
/// file, never its contents.
pub(crate) fn read(path: &Path) -> Result<String, String> {
    use std::io::Read as _;
    use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
    let file = std::fs::OpenOptions::new()
        .read(true)
        // Refuse a symbolic link, never block on a FIFO with no writer, and
        // never adopt a terminal: what was opened is checked next.
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_NOCTTY)
        .open(path)
        .map_err(|error| match error.raw_os_error() {
            Some(libc::ELOOP) => "must not be a symbolic link".to_string(),
            _ => format!("cannot be opened: {error}"),
        })?;
    let metadata = file
        .metadata()
        .map_err(|error| format!("cannot be read: {error}"))?;
    if !metadata.is_file() {
        return Err("must be a regular file".into());
    }
    if metadata.mode() & 0o022 != 0 {
        return Err("must not be writable by group or others".into());
    }
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
        let (dir, target) = file_with(SECRET.as_bytes(), 0o600);

        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert!(refusal(&link).contains("symbolic link"));

        assert!(refusal(dir.path()).contains("regular file"));

        // Opening a FIFO with no writer must not block.
        let fifo = dir.path().join("fifo");
        let c_path = std::ffi::CString::new(fifo.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
        assert!(refusal(&fifo).contains("regular file"));

        assert!(refusal(&dir.path().join("missing")).contains("cannot be opened"));
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
}
