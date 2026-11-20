//! Pure inline-anchor validation against the reviewer launch SHA diff.
//!
//! `add_comment_to_pending_review` must verify that a reviewer-supplied anchor
//! (`path`, `line`, `side`, optional `startLine`/`startSide`, `subjectType`)
//! exists on the immutable reviewer launch SHA before admission. This module
//! parses a bounded `git diff` unified diff produced for that SHA into
//! per-path hunk ranges and validates anchors against it, returning a closed
//! typed error. An invalid anchor is never downgraded to a general comment:
//! the only success value is a fully validated inline anchor.
//!
//! Commentable lines follow GitHub's diff model: on the RIGHT side every line
//! of a hunk's new range (added and context lines) is commentable; on the LEFT
//! side every line of its old range (removed and context lines). Because hunk
//! line numbers are contiguous per side, each hunk is stored as two half-open
//! ranges, which bounds memory by hunk count rather than line count.
//!
//! Nothing here performs I/O; the caller supplies the diff bytes and the SHA
//! the diff was computed for.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ops::Range;

/// Maximum accepted diff size in bytes.
pub const MAX_DIFF_BYTES: usize = 16 * 1024 * 1024;
/// Maximum number of files in one diff (GitHub's own PR diff file limit).
pub const MAX_DIFF_FILES: usize = 3_000;
/// Maximum number of hunks across the whole diff.
pub const MAX_DIFF_HUNKS: usize = 100_000;
/// Maximum number of physical diff lines across the whole diff.
pub const MAX_DIFF_LINES: usize = 1_000_000;
/// Maximum accepted path length in bytes.
pub const MAX_ANCHOR_PATH_BYTES: usize = 4_096;

/// Diff side of an anchor, using GitHub's vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Left,
    Right,
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Side::Left => "LEFT",
            Side::Right => "RIGHT",
        }
    }

    fn parse(raw: &str) -> Option<Side> {
        match raw {
            "LEFT" => Some(Side::Left),
            "RIGHT" => Some(Side::Right),
            _ => None,
        }
    }

    fn other(self) -> Side {
        match self {
            Side::Left => Side::Right,
            Side::Right => Side::Left,
        }
    }
}

/// Failure to accept diff input. Any failure rejects the whole diff; there is
/// no partially parsed result.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DiffParseError {
    #[error("diff exceeds {MAX_DIFF_BYTES} bytes")]
    TooLarge,
    #[error("diff contains an embedded NUL byte")]
    EmbeddedNul,
    #[error("diff is not valid UTF-8")]
    InvalidUtf8,
    #[error("diff head SHA is not a lowercase 40- or 64-character hex object id")]
    InvalidSha,
    #[error("diff exceeds {MAX_DIFF_FILES} files")]
    TooManyFiles,
    #[error("diff exceeds {MAX_DIFF_HUNKS} hunks")]
    TooManyHunks,
    #[error("diff exceeds {MAX_DIFF_LINES} lines")]
    TooManyLines,
    #[error("malformed diff at line {line}: {reason}")]
    Malformed { line: usize, reason: &'static str },
}

impl DiffParseError {
    pub fn code(&self) -> &'static str {
        match self {
            DiffParseError::TooLarge => "diff_too_large",
            DiffParseError::EmbeddedNul => "diff_embedded_nul",
            DiffParseError::InvalidUtf8 => "diff_invalid_utf8",
            DiffParseError::InvalidSha => "diff_invalid_sha",
            DiffParseError::TooManyFiles => "diff_too_many_files",
            DiffParseError::TooManyHunks => "diff_too_many_hunks",
            DiffParseError::TooManyLines => "diff_too_many_lines",
            DiffParseError::Malformed { .. } => "diff_malformed",
        }
    }
}

/// Closed set of anchor rejections. Each class is distinct so callers can
/// report it precisely; none of them is a fallback to a general comment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AnchorError {
    #[error("diff head SHA does not equal the reviewer launch SHA")]
    StaleSha,
    #[error("subjectType is not supported for inline anchors")]
    UnsupportedSubjectType,
    #[error("path is not part of the launch SHA diff")]
    PathNotInDiff,
    #[error("line is missing or not a positive line number")]
    MissingLine,
    #[error("side/startSide must be LEFT or RIGHT")]
    InvalidSide,
    #[error("line is not inside any diff hunk")]
    LineOutsideDiff,
    #[error("line is only commentable on the other diff side")]
    SideMismatch,
    #[error("range is inverted, crosses hunks, or mixes sides")]
    InvalidRange,
}

impl AnchorError {
    pub fn code(self) -> &'static str {
        match self {
            AnchorError::StaleSha => "stale_sha",
            AnchorError::UnsupportedSubjectType => "unsupported_subject_type",
            AnchorError::PathNotInDiff => "path_not_in_diff",
            AnchorError::MissingLine => "missing_line",
            AnchorError::InvalidSide => "invalid_side",
            AnchorError::LineOutsideDiff => "line_outside_diff",
            AnchorError::SideMismatch => "side_mismatch",
            AnchorError::InvalidRange => "invalid_range",
        }
    }
}

/// Reviewer-supplied anchor, as received from the tool call. Raw strings are
/// kept so unknown `side`/`subjectType` values reach the typed validator
/// instead of failing opaquely at deserialization.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InlineAnchor {
    pub path: String,
    #[serde(default)]
    pub line: Option<u64>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub start_line: Option<u64>,
    #[serde(default)]
    pub start_side: Option<String>,
    #[serde(default)]
    pub subject_type: Option<String>,
}

/// A fully validated inline anchor, ready to be published unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidatedAnchor {
    pub path: String,
    pub line: u64,
    pub side: Side,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_line: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_side: Option<Side>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Hunk {
    left: Range<u64>,
    right: Range<u64>,
}

impl Hunk {
    fn range(&self, side: Side) -> &Range<u64> {
        match side {
            Side::Left => &self.left,
            Side::Right => &self.right,
        }
    }
}

/// Parsed diff for one head SHA.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchDiff {
    head_sha: String,
    files: BTreeMap<String, Vec<Hunk>>,
}

fn valid_sha(sha: &str) -> bool {
    (sha.len() == 40 || sha.len() == 64)
        && sha
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl LaunchDiff {
    /// Parse a `git diff` unified diff (default `a/`/`b/` prefixes) computed
    /// for `head_sha`. Input is bounded in bytes, files, hunks, and lines, and
    /// must be valid UTF-8 without NUL bytes.
    pub fn parse(head_sha: &str, diff: &[u8]) -> Result<LaunchDiff, DiffParseError> {
        if diff.len() > MAX_DIFF_BYTES {
            return Err(DiffParseError::TooLarge);
        }
        if diff.contains(&0) {
            return Err(DiffParseError::EmbeddedNul);
        }
        let text = std::str::from_utf8(diff).map_err(|_| DiffParseError::InvalidUtf8)?;
        if !valid_sha(head_sha) {
            return Err(DiffParseError::InvalidSha);
        }
        Parser::default().run(text).map(|files| LaunchDiff {
            head_sha: head_sha.to_string(),
            files,
        })
    }

    pub fn head_sha(&self) -> &str {
        &self.head_sha
    }

    /// Validate `anchor` against this diff for the reviewer `launch_sha`.
    pub fn validate(
        &self,
        launch_sha: &str,
        anchor: &InlineAnchor,
    ) -> Result<ValidatedAnchor, AnchorError> {
        if self.head_sha != launch_sha {
            return Err(AnchorError::StaleSha);
        }
        match anchor.subject_type.as_deref() {
            // The published add_comment_to_pending_review schema uses "LINE";
            // lowercase "line" is accepted for compatibility. "FILE" and any
            // other value are rejected, never downgraded.
            None | Some("LINE") | Some("line") => {}
            Some(_) => return Err(AnchorError::UnsupportedSubjectType),
        }
        let hunks = self
            .files
            .get(anchor.path.as_str())
            .ok_or(AnchorError::PathNotInDiff)?;
        let line = match anchor.line {
            Some(line) if line >= 1 => line,
            _ => return Err(AnchorError::MissingLine),
        };
        let side = Side::parse(anchor.side.as_deref().ok_or(AnchorError::InvalidSide)?)
            .ok_or(AnchorError::InvalidSide)?;
        let end_hunk = match find_hunk(hunks, side, line) {
            Some(index) => index,
            None if find_hunk(hunks, side.other(), line).is_some() => {
                return Err(AnchorError::SideMismatch)
            }
            None => return Err(AnchorError::LineOutsideDiff),
        };

        let (start_line, start_side) = match (anchor.start_line, anchor.start_side.as_deref()) {
            (None, None) => (None, None),
            (None, Some(_)) => return Err(AnchorError::InvalidRange),
            (Some(start), raw_side) => {
                let start_side = match raw_side {
                    None => side,
                    Some(raw) => Side::parse(raw).ok_or(AnchorError::InvalidSide)?,
                };
                if start_side != side || start == 0 || start >= line {
                    return Err(AnchorError::InvalidRange);
                }
                match find_hunk(hunks, side, start) {
                    Some(index) if index == end_hunk => {}
                    Some(_) => return Err(AnchorError::InvalidRange),
                    None => return Err(AnchorError::LineOutsideDiff),
                }
                (Some(start), Some(start_side))
            }
        };

        Ok(ValidatedAnchor {
            path: anchor.path.clone(),
            line,
            side,
            start_line,
            start_side,
        })
    }
}

fn find_hunk(hunks: &[Hunk], side: Side, line: u64) -> Option<usize> {
    hunks
        .iter()
        .position(|hunk| hunk.range(side).contains(&line))
}

#[derive(Default)]
struct PendingFile {
    old_path: Option<Option<String>>,
    path: Option<String>,
    hunks: Vec<Hunk>,
    left_remaining: u64,
    right_remaining: u64,
}

#[derive(Default)]
struct Parser {
    files: BTreeMap<String, Vec<Hunk>>,
    current: Option<PendingFile>,
    file_count: usize,
    hunk_count: usize,
}

fn malformed(line: usize, reason: &'static str) -> DiffParseError {
    DiffParseError::Malformed { line, reason }
}

impl Parser {
    fn run(mut self, text: &str) -> Result<BTreeMap<String, Vec<Hunk>>, DiffParseError> {
        for (index, raw) in text.split_terminator('\n').enumerate() {
            let lineno = index + 1;
            if lineno > MAX_DIFF_LINES {
                return Err(DiffParseError::TooManyLines);
            }
            self.line(lineno, raw)?;
        }
        self.finish_file(text.split_terminator('\n').count() + 1)?;
        Ok(self.files)
    }

    fn line(&mut self, lineno: usize, raw: &str) -> Result<(), DiffParseError> {
        let in_hunk = self
            .current
            .as_ref()
            .is_some_and(|f| f.left_remaining > 0 || f.right_remaining > 0);
        if in_hunk {
            return self.hunk_body(lineno, raw);
        }
        if raw.starts_with("diff --git ") {
            self.finish_file(lineno)?;
            self.file_count += 1;
            if self.file_count > MAX_DIFF_FILES {
                return Err(DiffParseError::TooManyFiles);
            }
            self.current = Some(PendingFile::default());
            return Ok(());
        }
        let Some(file) = self.current.as_mut() else {
            return Err(malformed(lineno, "content before first diff --git header"));
        };
        if file.path.is_none() {
            // Extended header section: only ---/+++ carry the anchor path.
            if let Some(rest) = raw.strip_prefix("--- ") {
                if file.old_path.is_some() {
                    return Err(malformed(lineno, "duplicate --- header"));
                }
                file.old_path =
                    Some(header_path(rest, "a/").ok_or(malformed(lineno, "bad --- path"))?);
            } else if let Some(rest) = raw.strip_prefix("+++ ") {
                let Some(old) = file.old_path.take() else {
                    return Err(malformed(lineno, "+++ without ---"));
                };
                let new = header_path(rest, "b/").ok_or(malformed(lineno, "bad +++ path"))?;
                let path = new
                    .or(old)
                    .ok_or(malformed(lineno, "both sides are /dev/null"))?;
                if self.files.contains_key(&path) {
                    return Err(malformed(lineno, "duplicate file path"));
                }
                file.path = Some(path);
            } else if raw.starts_with("@@") {
                return Err(malformed(lineno, "hunk before ---/+++ headers"));
            }
            return Ok(());
        }
        if raw.starts_with("\\") {
            return Ok(());
        }
        if raw.starts_with("@@ ") {
            self.hunk_count += 1;
            if self.hunk_count > MAX_DIFF_HUNKS {
                return Err(DiffParseError::TooManyHunks);
            }
            let (left, right) =
                parse_hunk_header(raw).ok_or(malformed(lineno, "bad hunk header"))?;
            if let Some(prev) = file.hunks.last() {
                if left.start < prev.left.end || right.start < prev.right.end {
                    return Err(malformed(lineno, "hunks out of order or overlapping"));
                }
            }
            file.left_remaining = left.end - left.start;
            file.right_remaining = right.end - right.start;
            if file.left_remaining == 0 && file.right_remaining == 0 {
                return Err(malformed(lineno, "empty hunk"));
            }
            file.hunks.push(Hunk { left, right });
            return Ok(());
        }
        Err(malformed(lineno, "unexpected line between hunks"))
    }

    fn hunk_body(&mut self, lineno: usize, raw: &str) -> Result<(), DiffParseError> {
        let file = self.current.as_mut().expect("in_hunk implies current file");
        let (left, right) = match raw.as_bytes().first() {
            Some(b' ') => (1, 1),
            Some(b'-') => (1, 0),
            Some(b'+') => (0, 1),
            Some(b'\\') => (0, 0),
            _ => return Err(malformed(lineno, "invalid hunk line prefix")),
        };
        if left > file.left_remaining || right > file.right_remaining {
            return Err(malformed(lineno, "hunk line exceeds header counts"));
        }
        file.left_remaining -= left;
        file.right_remaining -= right;
        Ok(())
    }

    fn finish_file(&mut self, lineno: usize) -> Result<(), DiffParseError> {
        let Some(file) = self.current.take() else {
            return Ok(());
        };
        if file.left_remaining > 0 || file.right_remaining > 0 {
            return Err(malformed(lineno, "truncated hunk"));
        }
        if file.old_path.is_some() {
            return Err(malformed(lineno, "--- without +++"));
        }
        if let Some(path) = file.path {
            self.files.insert(path, file.hunks);
        }
        Ok(())
    }
}

/// Parse a `---`/`+++` header path. Returns `Some(None)` for `/dev/null`.
fn header_path(rest: &str, prefix: &str) -> Option<Option<String>> {
    // git appends a TAB after names containing a space.
    let rest = rest.strip_suffix('\t').unwrap_or(rest);
    if rest == "/dev/null" {
        return Some(None);
    }
    let name = if rest.starts_with('"') {
        unquote_c(rest)?
    } else {
        rest.to_string()
    };
    let path = name.strip_prefix(prefix)?;
    if path.is_empty() || path.len() > MAX_ANCHOR_PATH_BYTES {
        return None;
    }
    Some(Some(path.to_string()))
}

/// Undo git's C-style path quoting (`core.quotePath`).
fn unquote_c(quoted: &str) -> Option<String> {
    let inner = quoted.strip_prefix('"')?.strip_suffix('"')?;
    let bytes = inner.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'"' {
            return None;
        }
        if b != b'\\' {
            out.push(b);
            i += 1;
            continue;
        }
        let esc = *bytes.get(i + 1)?;
        i += 2;
        let decoded = match esc {
            b'\\' => b'\\',
            b'"' => b'"',
            b'a' => 0x07,
            b'b' => 0x08,
            b'f' => 0x0c,
            b'n' => b'\n',
            b'r' => b'\r',
            b't' => b'\t',
            b'v' => 0x0b,
            b'0'..=b'3' => {
                let digits = bytes.get(i - 1..i + 2)?;
                if !digits.iter().all(|d| (b'0'..=b'7').contains(d)) {
                    return None;
                }
                i += 1 + 1;
                digits
                    .iter()
                    .fold(0u8, |acc, d| acc.wrapping_mul(8) + (d - b'0'))
            }
            _ => return None,
        };
        if decoded == 0 {
            return None;
        }
        out.push(decoded);
    }
    String::from_utf8(out).ok()
}

/// Parse `@@ -a[,b] +c[,d] @@[ section]` into half-open LEFT/RIGHT ranges.
fn parse_hunk_header(raw: &str) -> Option<(Range<u64>, Range<u64>)> {
    let rest = raw.strip_prefix("@@ -")?;
    let (old, rest) = rest.split_once(" +")?;
    let (new, tail) = rest.split_once(" @@")?;
    if !(tail.is_empty() || tail.starts_with(' ')) {
        return None;
    }
    Some((parse_range(old)?, parse_range(new)?))
}

fn parse_range(spec: &str) -> Option<Range<u64>> {
    let (start, count) = match spec.split_once(',') {
        Some((s, c)) => (parse_num(s)?, parse_num(c)?),
        None => (parse_num(spec)?, 1),
    };
    if count == 0 {
        // Empty side: git reports the line before the hunk (possibly 0).
        return Some(start..start);
    }
    if start == 0 {
        return None;
    }
    let end = start.checked_add(count)?;
    if end > u64::from(u32::MAX) {
        return None;
    }
    Some(start..end)
}

fn parse_num(s: &str) -> Option<u64> {
    if s.is_empty() || s.len() > 10 || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHA: &str = "0123456789abcdef0123456789abcdef01234567";
    const OTHER_SHA: &str = "fedcba9876543210fedcba9876543210fedcba98";

    // src/lib.rs: hunk 1 LEFT 1..=4 RIGHT 1..=5; hunk 2 LEFT 20..=22 RIGHT 21..=22.
    const DIFF: &str = "\
diff --git a/src/lib.rs b/src/lib.rs
index 1111111..2222222 100644
--- a/src/lib.rs
+++ b/src/lib.rs
@@ -1,4 +1,5 @@ fn main() {
 one
-two
+TWO
+two-and-a-half
 three
 four
@@ -20,3 +21,2 @@
 twenty
-twenty-one
 twenty-two
\\ No newline at end of file
diff --git a/new.txt b/new.txt
new file mode 100644
index 0000000..3333333
--- /dev/null
+++ b/new.txt
@@ -0,0 +1,2 @@
+hello
+world
diff --git a/gone.txt b/gone.txt
deleted file mode 100644
index 4444444..0000000
--- a/gone.txt
+++ /dev/null
@@ -1,2 +0,0 @@
-bye
-now
diff --git a/img.png b/img.png
index 5555555..6666666 100644
Binary files a/img.png and b/img.png differ
diff --git \"a/sp ace\\303\\251.txt\" \"b/sp ace\\303\\251.txt\"
--- \"a/sp ace\\303\\251.txt\"
+++ \"b/sp ace\\303\\251.txt\"
@@ -1 +1 @@
-x
+y
";

    fn diff() -> LaunchDiff {
        LaunchDiff::parse(SHA, DIFF.as_bytes()).expect("fixture parses")
    }

    fn anchor(path: &str, line: u64, side: &str) -> InlineAnchor {
        InlineAnchor {
            path: path.into(),
            line: Some(line),
            side: Some(side.into()),
            ..Default::default()
        }
    }

    fn range(
        path: &str,
        start: u64,
        start_side: Option<&str>,
        line: u64,
        side: &str,
    ) -> InlineAnchor {
        InlineAnchor {
            start_line: Some(start),
            start_side: start_side.map(str::to_string),
            ..anchor(path, line, side)
        }
    }

    fn check(a: &InlineAnchor) -> Result<ValidatedAnchor, AnchorError> {
        diff().validate(SHA, a)
    }

    #[test]
    fn accepts_right_added_and_context_lines() {
        for line in [1, 2, 3, 4, 5, 21, 22] {
            let ok = check(&anchor("src/lib.rs", line, "RIGHT")).expect("right line");
            assert_eq!((ok.line, ok.side, ok.start_line), (line, Side::Right, None));
        }
        assert!(check(&anchor("new.txt", 2, "RIGHT")).is_ok());
    }

    #[test]
    fn accepts_left_removed_and_context_lines() {
        for line in [1, 2, 3, 4, 20, 21, 22] {
            let ok = check(&anchor("src/lib.rs", line, "LEFT")).expect("left line");
            assert_eq!(ok.side, Side::Left);
        }
        assert!(check(&anchor("gone.txt", 1, "LEFT")).is_ok());
    }

    #[test]
    fn accepts_explicit_line_subject_type_and_quoted_path() {
        let a = InlineAnchor {
            subject_type: Some("LINE".into()),
            ..anchor("sp aceé.txt", 1, "RIGHT")
        };
        assert_eq!(check(&a).unwrap().path, "sp aceé.txt");
        let lower = InlineAnchor {
            subject_type: Some("line".into()),
            ..anchor("src/lib.rs", 2, "RIGHT")
        };
        assert!(check(&lower).is_ok());
        let multi = InlineAnchor {
            subject_type: Some("LINE".into()),
            ..range("src/lib.rs", 2, Some("RIGHT"), 5, "RIGHT")
        };
        assert!(check(&multi).is_ok());
    }

    #[test]
    fn accepts_multiline_range_within_one_hunk() {
        let ok = check(&range("src/lib.rs", 2, Some("RIGHT"), 5, "RIGHT")).unwrap();
        assert_eq!((ok.start_line, ok.start_side), (Some(2), Some(Side::Right)));
        // startSide defaults to side.
        let ok = check(&range("src/lib.rs", 20, None, 22, "LEFT")).unwrap();
        assert_eq!((ok.start_line, ok.start_side), (Some(20), Some(Side::Left)));
        let json = serde_json::to_value(&ok).unwrap();
        assert_eq!(json["side"], "LEFT");
        assert_eq!(json["startLine"], 20);
    }

    #[test]
    fn rejects_stale_sha() {
        let d = diff();
        assert_eq!(
            d.validate(OTHER_SHA, &anchor("src/lib.rs", 2, "RIGHT")),
            Err(AnchorError::StaleSha)
        );
        // Stale SHA wins even over an otherwise invalid anchor.
        assert_eq!(
            d.validate(OTHER_SHA, &anchor("nope.rs", 2, "RIGHT")),
            Err(AnchorError::StaleSha)
        );
        assert_eq!(AnchorError::StaleSha.code(), "stale_sha");
    }

    #[test]
    fn rejects_path_not_in_diff() {
        assert_eq!(
            check(&anchor("src/other.rs", 1, "RIGHT")),
            Err(AnchorError::PathNotInDiff)
        );
        // Binary file without hunks is not an inline-commentable path.
        assert_eq!(
            check(&anchor("img.png", 1, "RIGHT")),
            Err(AnchorError::PathNotInDiff)
        );
        // Prefix-bearing path is not normalized.
        assert_eq!(
            check(&anchor("b/src/lib.rs", 1, "RIGHT")),
            Err(AnchorError::PathNotInDiff)
        );
    }

    #[test]
    fn rejects_line_outside_diff() {
        for line in [6, 19, 23, 100] {
            assert_eq!(
                check(&anchor("src/lib.rs", line, "RIGHT")),
                Err(AnchorError::LineOutsideDiff),
                "line {line}"
            );
        }
        assert_eq!(
            check(&anchor("gone.txt", 3, "LEFT")),
            Err(AnchorError::LineOutsideDiff)
        );
    }

    #[test]
    fn rejects_side_mismatch() {
        // RIGHT 5 exists, LEFT 5 does not.
        assert_eq!(
            check(&anchor("src/lib.rs", 5, "LEFT")),
            Err(AnchorError::SideMismatch)
        );
        // LEFT 20 exists, RIGHT 20 does not.
        assert_eq!(
            check(&anchor("src/lib.rs", 20, "RIGHT")),
            Err(AnchorError::SideMismatch)
        );
        assert_eq!(
            check(&anchor("new.txt", 1, "LEFT")),
            Err(AnchorError::SideMismatch)
        );
        assert_eq!(
            check(&anchor("gone.txt", 1, "RIGHT")),
            Err(AnchorError::SideMismatch)
        );
        assert_eq!(
            check(&anchor("src/lib.rs", 2, "right")),
            Err(AnchorError::InvalidSide)
        );
        let mut no_side = anchor("src/lib.rs", 2, "RIGHT");
        no_side.side = None;
        assert_eq!(check(&no_side), Err(AnchorError::InvalidSide));
    }

    #[test]
    fn rejects_invalid_ranges() {
        let cases = [
            range("src/lib.rs", 4, None, 2, "RIGHT"),         // inverted
            range("src/lib.rs", 3, None, 3, "RIGHT"),         // empty
            range("src/lib.rs", 2, None, 21, "RIGHT"),        // cross-hunk
            range("src/lib.rs", 1, Some("LEFT"), 3, "RIGHT"), // mixed sides
            range("src/lib.rs", 0, None, 3, "RIGHT"),         // zero start
        ];
        for a in &cases {
            assert_eq!(check(a), Err(AnchorError::InvalidRange), "{a:?}");
        }
        let mut side_only = anchor("src/lib.rs", 3, "RIGHT");
        side_only.start_side = Some("RIGHT".into());
        assert_eq!(check(&side_only), Err(AnchorError::InvalidRange));
        // Start outside any hunk is reported as such.
        assert_eq!(
            check(&range("src/lib.rs", 10, None, 21, "RIGHT")),
            Err(AnchorError::LineOutsideDiff)
        );
        assert_eq!(
            check(&range("src/lib.rs", 1, Some("up"), 3, "RIGHT")),
            Err(AnchorError::InvalidSide)
        );
    }

    #[test]
    fn rejects_unsupported_subject_type_and_missing_line() {
        for st in ["FILE", "file", "Line", "hunk"] {
            let a = InlineAnchor {
                subject_type: Some(st.into()),
                ..anchor("src/lib.rs", 2, "RIGHT")
            };
            assert_eq!(check(&a), Err(AnchorError::UnsupportedSubjectType), "{st}");
        }
        let mut no_line = anchor("src/lib.rs", 2, "RIGHT");
        no_line.line = None;
        assert_eq!(check(&no_line), Err(AnchorError::MissingLine));
        assert_eq!(
            check(&anchor("src/lib.rs", 0, "RIGHT")),
            Err(AnchorError::MissingLine)
        );
    }

    #[test]
    fn deserializes_camel_case_and_rejects_unknown_fields() {
        let a: InlineAnchor = serde_json::from_str(
            r#"{"path":"src/lib.rs","line":3,"side":"RIGHT","startLine":2,"startSide":"RIGHT","subjectType":"LINE"}"#,
        )
        .unwrap();
        assert!(check(&a).is_ok());
        assert!(serde_json::from_str::<InlineAnchor>(r#"{"path":"x","position":3}"#).is_err());
    }

    #[test]
    fn rejects_unsafe_or_unbounded_input() {
        assert_eq!(
            LaunchDiff::parse(SHA, b"diff --git a b\0"),
            Err(DiffParseError::EmbeddedNul)
        );
        assert_eq!(
            LaunchDiff::parse(SHA, b"\xff\xfe"),
            Err(DiffParseError::InvalidUtf8)
        );
        assert_eq!(
            LaunchDiff::parse(SHA, &vec![b' '; MAX_DIFF_BYTES + 1]),
            Err(DiffParseError::TooLarge)
        );
        assert_eq!(
            LaunchDiff::parse("ABC", DIFF.as_bytes()),
            Err(DiffParseError::InvalidSha)
        );

        let mut many_files = String::new();
        for i in 0..=MAX_DIFF_FILES {
            many_files.push_str(&format!("diff --git a/f{i} b/f{i}\n"));
        }
        assert_eq!(
            LaunchDiff::parse(SHA, many_files.as_bytes()),
            Err(DiffParseError::TooManyFiles)
        );

        let mut many_hunks = String::from("diff --git a/f b/f\n--- a/f\n+++ b/f\n");
        for i in 0..=MAX_DIFF_HUNKS as u64 {
            many_hunks.push_str(&format!("@@ -{n} +{n} @@\n-a\n+b\n", n = i + 1));
        }
        assert_eq!(
            LaunchDiff::parse(SHA, many_hunks.as_bytes()),
            Err(DiffParseError::TooManyHunks)
        );

        let mut many_lines =
            String::from("diff --git a/f b/f\n--- a/f\n+++ b/f\n@@ -1,1000000 +1,1000000 @@\n");
        for _ in 0..MAX_DIFF_LINES {
            many_lines.push_str(" \n");
        }
        assert_eq!(
            LaunchDiff::parse(SHA, many_lines.as_bytes()),
            Err(DiffParseError::TooManyLines)
        );
    }

    #[test]
    fn rejects_malformed_diffs() {
        let bad = [
            "garbage\n",
            "diff --git a/f b/f\n@@ -1 +1 @@\n-a\n+b\n",
            "diff --git a/f b/f\n--- a/f\n+++ b/f\n@@ -1,2 +1,2 @@\n-a\n+b\n",
            "diff --git a/f b/f\n--- a/f\n+++ b/f\n@@ -1 +1 @@\n-a\n-b\n+c\n",
            "diff --git a/f b/f\n--- a/f\n+++ b/f\n@@ -1 +1 @@\n-a\n+b\nstray\n",
            "diff --git a/f b/f\n--- a/f\n+++ b/f\n@@ -1 +1 @@\nxa\n",
            "diff --git a/f b/f\n--- a/f\n+++ b/f\n@@ -5 +5 @@\n-a\n+b\n@@ -1 +1 @@\n-a\n+b\n",
            "diff --git a/f b/f\n--- a/f\n+++ b/f\n@@ -1 +1 @@\n-a\n+b\ndiff --git a/f b/f\n--- a/f\n+++ b/f\n@@ -9 +9 @@\n-a\n+b\n",
            "diff --git a/f b/f\n--- /dev/null\n+++ /dev/null\n",
            "diff --git a/f b/f\n--- f\n+++ f\n",
            "diff --git a/f b/f\n--- a/f\n+++ b/f\n@@ -1 +1 @ bad\n",
            "diff --git a/f b/f\n--- a/f\n+++ b/f\n@@ -0,0 +0,0 @@\n",
            "diff --git a/f b/f\n--- a/f\n",
            "diff --git a/f b/f\n--- \"a/q\\x\"\n+++ b/f\n",
        ];
        for input in bad {
            assert!(
                matches!(
                    LaunchDiff::parse(SHA, input.as_bytes()),
                    Err(DiffParseError::Malformed { .. })
                ),
                "should reject: {input:?}"
            );
        }
        // An empty diff parses but admits no anchor.
        let empty = LaunchDiff::parse(SHA, b"").unwrap();
        assert_eq!(
            empty.validate(SHA, &anchor("f", 1, "RIGHT")),
            Err(AnchorError::PathNotInDiff)
        );
    }
}
