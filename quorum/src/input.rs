//! Reading free-text payloads safely.
//!
//! Free text never travels as a shell argument — it arrives via stdin or a file. Once read,
//! it is validated: embedded NUL and invalid UTF-8 are rejected (TEXT + JSON cannot carry
//! arbitrary bytes), so we fail loud rather than silently mangle. Valid text is then bound
//! as a SQLite parameter by the caller (never concatenated into SQL).

use quorum_core::error::{QuorumError, Result};
use std::io::Read;
use std::path::PathBuf;

/// Where a free-text payload comes from.
pub enum TextSource {
    Stdin,
    File(PathBuf),
}

fn validate(bytes: Vec<u8>) -> Result<String> {
    if bytes.contains(&0) {
        return Err(QuorumError::BadInput("embedded NUL byte".into()));
    }
    String::from_utf8(bytes).map_err(|_| QuorumError::BadInput("invalid UTF-8".into()))
}

/// Read and validate a free-text payload from the given source.
pub fn read_text(src: TextSource) -> Result<String> {
    let bytes = match src {
        TextSource::Stdin => {
            let mut b = Vec::new();
            std::io::stdin()
                .read_to_end(&mut b)
                .map_err(|e| QuorumError::Io(e.to_string()))?;
            b
        }
        TextSource::File(p) => std::fs::read(&p).map_err(|e| QuorumError::Io(e.to_string()))?,
    };
    validate(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_nul() {
        assert!(validate(vec![b'a', 0, b'b']).is_err());
    }

    #[test]
    fn rejects_bad_utf8() {
        assert!(validate(vec![0xff, 0xfe]).is_err());
    }

    #[test]
    fn accepts_unicode_quotes_newlines() {
        let s = "héllo \"world\"\n`$x` 'mixed'\n";
        assert_eq!(validate(s.as_bytes().to_vec()).unwrap(), s);
    }

    #[test]
    fn read_text_file_arm_roundtrips_and_rejects_nul() {
        use std::io::Write;
        // valid file → byte-exact round-trip
        let mut ok = tempfile::NamedTempFile::new().unwrap();
        ok.write_all("h\u{e9}llo\n`$x`\n".as_bytes()).unwrap();
        let got = read_text(TextSource::File(ok.path().to_path_buf())).unwrap();
        assert_eq!(got, "héllo\n`$x`\n");

        // file containing NUL → BadInput
        let mut bad = tempfile::NamedTempFile::new().unwrap();
        bad.write_all(&[b'a', 0, b'b']).unwrap();
        assert!(read_text(TextSource::File(bad.path().to_path_buf())).is_err());
    }
}
