//! Bounded, streaming failure digest of a GitHub Actions job log.
//!
//! This module is pure: it performs no network or GitHub access. It reads any
//! [`BufRead`] job log line by line through a per-line byte cap (never an
//! unbounded `read_line`), strips the GitHub timestamp prefix and ANSI escape
//! sequences, and keeps only bounded collections of failure markers:
//!
//! * up to [`FIRST_FAILURE_LINES_MAX`] `preflight timing: FIRST FAILURE:`
//!   lines, kept separately so no volume of other markers can displace them;
//! * up to [`TIMEOUT_LINES_MAX`] `timed out after` lines;
//! * up to [`FAILED_NAMES_MAX`] `test <path> ... FAILED` names;
//! * up to [`PANICS_MAX`] `panicked at` lines, each paired with the following
//!   non-blank (assertion) line;
//! * a ring buffer of the last [`TAIL_LINES`] non-blank lines before the last
//!   `##[error]`, rendered only when none of the markers above were found.
//!
//! [`render_digest`] produces text of at most [`DIGEST_MAX_BYTES`] bytes, cut
//! on a line boundary with an explicit truncation marker when cut. The output
//! always carries the run id and job id.

use std::collections::VecDeque;
use std::io::{self, BufRead};

/// Maximum number of distinct `... FAILED` test names kept.
pub const FAILED_NAMES_MAX: usize = 20;
/// Number of non-blank lines kept before `##[error]` for the tail fallback.
pub const TAIL_LINES: usize = 30;
/// Hard cap on the rendered digest, in bytes.
pub const DIGEST_MAX_BYTES: usize = 4096;
/// Maximum number of `preflight timing: FIRST FAILURE:` lines kept.
pub const FIRST_FAILURE_LINES_MAX: usize = 4;
/// Maximum number of `timed out after` lines kept.
pub const TIMEOUT_LINES_MAX: usize = 8;
/// Maximum number of panic (+ assertion) pairs kept.
pub const PANICS_MAX: usize = 5;
/// Maximum raw bytes buffered for one log line; the remainder is discarded.
pub const LINE_READ_MAX_BYTES: usize = 16 * 1024;
/// Maximum bytes of one cleaned line kept in the digest.
pub const LINE_MAX_BYTES: usize = 512;
/// Maximum bytes of the job name rendered in the header.
pub const JOB_NAME_MAX_BYTES: usize = 256;

const LINE_TRUNCATED_SUFFIX: &str = " [line truncated]";
const FIRST_FAILURE_MARKER: &str = "preflight timing: FIRST FAILURE:";
const TIMEOUT_MARKER: &str = "timed out after";
const PANIC_MARKER: &str = "panicked at";
const ERROR_MARKER: &str = "##[error]";

/// One `panicked at` line and the non-blank line that followed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PanicDigest {
    pub line: String,
    pub assertion: Option<String>,
}

/// Where the fallback tail came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TailSource {
    /// The lines preceding the last `##[error]` line.
    BeforeError,
    /// No `##[error]` line was seen; the last lines of the log.
    EndOfLog,
}

/// Bounded failure summary of one job log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CiJobDigest {
    pub first_failure_lines: Vec<String>,
    pub first_failure_truncated: bool,
    pub timeout_lines: Vec<String>,
    pub timeout_lines_truncated: bool,
    pub failed_names: Vec<String>,
    pub failed_names_truncated: bool,
    pub panics: Vec<PanicDigest>,
    pub panics_truncated: bool,
    /// At most [`TAIL_LINES`] non-blank lines; rendered only as a fallback.
    pub tail: Vec<String>,
    pub tail_source: TailSource,
    /// The last `##[error]` line, when one was seen.
    pub error_line: Option<String>,
}

impl CiJobDigest {
    /// True when any first-failure, timeout, FAILED, or panic marker was found.
    pub fn has_markers(&self) -> bool {
        !self.first_failure_lines.is_empty()
            || !self.timeout_lines.is_empty()
            || !self.failed_names.is_empty()
            || !self.panics.is_empty()
    }
}

/// Stream `reader` and build a bounded digest. Memory use is bounded by the
/// constants above regardless of the log size or line length.
pub fn digest_job_log<R: BufRead>(mut reader: R) -> io::Result<CiJobDigest> {
    let mut raw = Vec::with_capacity(1024);
    let mut builder = Builder::default();
    while let Some(cut) = read_capped_line(&mut reader, &mut raw, LINE_READ_MAX_BYTES)? {
        builder.push(clean_line(&raw, cut));
    }
    Ok(builder.finish())
}

/// Render `digest` as text of at most [`DIGEST_MAX_BYTES`] bytes.
pub fn render_digest(digest: &CiJobDigest, job_name: &str, run_id: u64, job_id: u64) -> String {
    fit_lines(
        &render_lines(digest, job_name, run_id, job_id),
        run_id,
        job_id,
    )
}

#[derive(Default)]
struct Builder {
    first_failure_lines: Vec<String>,
    first_failure_truncated: bool,
    timeout_lines: Vec<String>,
    timeout_lines_truncated: bool,
    failed_names: Vec<String>,
    failed_names_truncated: bool,
    panics: Vec<PanicDigest>,
    panics_truncated: bool,
    pending_panic: Option<usize>,
    ring: VecDeque<String>,
    error_tail: Option<Vec<String>>,
    error_line: Option<String>,
}

impl Builder {
    fn push(&mut self, line: String) {
        let t = line.trim();
        if t.is_empty() {
            return;
        }
        let is_panic = t.contains(PANIC_MARKER);
        if let Some(idx) = self.pending_panic.take() {
            if !is_panic {
                self.panics[idx].assertion = Some(t.to_string());
            }
        }
        if t.contains(FIRST_FAILURE_MARKER) {
            push_capped(
                &mut self.first_failure_lines,
                &mut self.first_failure_truncated,
                FIRST_FAILURE_LINES_MAX,
                t,
            );
        } else if t.contains(TIMEOUT_MARKER) {
            push_capped(
                &mut self.timeout_lines,
                &mut self.timeout_lines_truncated,
                TIMEOUT_LINES_MAX,
                t,
            );
        }
        if let Some(name) = failed_name(t) {
            if !self.failed_names.iter().any(|n| n == name) {
                if self.failed_names.len() < FAILED_NAMES_MAX {
                    self.failed_names.push(name.to_string());
                } else {
                    self.failed_names_truncated = true;
                }
            }
        }
        if is_panic {
            if self.panics.len() < PANICS_MAX {
                self.panics.push(PanicDigest {
                    line: t.to_string(),
                    assertion: None,
                });
                self.pending_panic = Some(self.panics.len() - 1);
            } else {
                self.panics_truncated = true;
            }
        }
        if t.contains(ERROR_MARKER) {
            self.error_tail = Some(self.ring.iter().cloned().collect());
            self.error_line = Some(t.to_string());
        }
        if self.ring.len() == TAIL_LINES {
            self.ring.pop_front();
        }
        self.ring.push_back(line);
    }

    fn finish(self) -> CiJobDigest {
        let (tail, tail_source) = match self.error_tail {
            Some(tail) => (tail, TailSource::BeforeError),
            None => (self.ring.into_iter().collect(), TailSource::EndOfLog),
        };
        CiJobDigest {
            first_failure_lines: self.first_failure_lines,
            first_failure_truncated: self.first_failure_truncated,
            timeout_lines: self.timeout_lines,
            timeout_lines_truncated: self.timeout_lines_truncated,
            failed_names: self.failed_names,
            failed_names_truncated: self.failed_names_truncated,
            panics: self.panics,
            panics_truncated: self.panics_truncated,
            tail,
            tail_source,
            error_line: self.error_line,
        }
    }
}

fn push_capped(lines: &mut Vec<String>, truncated: &mut bool, max: usize, t: &str) {
    if lines.len() < max {
        lines.push(t.to_string());
    } else {
        *truncated = true;
    }
}

fn section_header(title: &str, shown: usize, truncated: bool) -> String {
    if truncated {
        format!("{title} (first {shown} shown; more omitted):")
    } else {
        format!("{title} ({shown}):")
    }
}

/// `test <name> ... FAILED` → `<name>`.
fn failed_name(t: &str) -> Option<&str> {
    let name = t.strip_prefix("test ")?.strip_suffix(" ... FAILED")?.trim();
    (!name.is_empty()).then_some(name)
}

/// Read one `\n`-terminated line into `out`, buffering at most `cap` bytes and
/// discarding the rest. Returns `None` at EOF, otherwise whether bytes were cut.
fn read_capped_line<R: BufRead>(
    reader: &mut R,
    out: &mut Vec<u8>,
    cap: usize,
) -> io::Result<Option<bool>> {
    out.clear();
    let mut any = false;
    let mut cut = false;
    loop {
        let avail = match reader.fill_buf() {
            Ok(buf) => buf,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        if avail.is_empty() {
            return Ok(any.then_some(cut));
        }
        any = true;
        let newline = avail.iter().position(|&b| b == b'\n');
        let chunk_len = newline.unwrap_or(avail.len());
        let take = chunk_len.min(cap.saturating_sub(out.len()));
        out.extend_from_slice(&avail[..take]);
        cut |= take < chunk_len;
        let consumed = if newline.is_some() {
            chunk_len + 1
        } else {
            chunk_len
        };
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(Some(cut));
        }
    }
}

/// Decode (lossily), strip timestamp + ANSI/control characters, trim the end,
/// and cap the result at [`LINE_MAX_BYTES`].
fn clean_line(raw: &[u8], read_cut: bool) -> String {
    let lossy = String::from_utf8_lossy(raw);
    let line = lossy.strip_prefix('\u{feff}').unwrap_or(&lossy);
    let mut out = strip_ansi(strip_timestamp(line));
    out.truncate(out.trim_end().len());
    cap_line(out, LINE_MAX_BYTES, read_cut)
}

fn cap_line(mut s: String, max: usize, already_cut: bool) -> String {
    if s.len() <= max && !already_cut {
        return s;
    }
    let mut end = s.len().min(max);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s.truncate(end);
    s.push_str(LINE_TRUNCATED_SUFFIX);
    s
}

/// Strip a leading GitHub log timestamp such as `2026-11-26T06:00:00.1234567Z `.
fn strip_timestamp(s: &str) -> &str {
    let b = s.as_bytes();
    let digits = |from: usize, to: usize| b[from..to].iter().all(u8::is_ascii_digit);
    if b.len() < 20
        || !digits(0, 4)
        || b[4] != b'-'
        || !digits(5, 7)
        || b[7] != b'-'
        || !digits(8, 10)
        || b[10] != b'T'
        || !digits(11, 13)
        || b[13] != b':'
        || !digits(14, 16)
        || b[16] != b':'
        || !digits(17, 19)
    {
        return s;
    }
    let mut i = 19;
    if b[i] == b'.' {
        i += 1;
        let start = i;
        while b.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        if i == start {
            return s;
        }
    }
    if b.get(i) != Some(&b'Z') {
        return s;
    }
    i += 1;
    if b.get(i) == Some(&b' ') {
        i += 1;
    }
    &s[i..]
}

/// Remove ANSI escape sequences (CSI, OSC, two-byte escapes) and every other
/// control character except tab.
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\x1b' => match chars.next() {
                Some('[') => skip_csi(&mut chars),
                Some(']') => {
                    // OSC: terminated by BEL or ESC '\'.
                    while let Some(c) = chars.next() {
                        if c == '\x07' {
                            break;
                        }
                        if c == '\x1b' {
                            if chars.peek() == Some(&'\\') {
                                chars.next();
                            }
                            break;
                        }
                    }
                }
                _ => {}
            },
            '\u{9b}' => skip_csi(&mut chars),
            '\t' => out.push(c),
            c if c.is_control() => {}
            c => out.push(c),
        }
    }
    out
}

fn skip_csi(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    for c in chars.by_ref() {
        if ('\x40'..='\x7e').contains(&c) {
            break;
        }
    }
}

fn render_lines(digest: &CiJobDigest, job_name: &str, run_id: u64, job_id: u64) -> Vec<String> {
    let job_name = cap_line(
        strip_ansi(job_name).trim().to_string(),
        JOB_NAME_MAX_BYTES,
        false,
    );
    let mut lines = vec![
        "CI job failure digest".to_string(),
        format!("job: {job_name}"),
        format!("run id: {run_id}"),
        format!("job id: {job_id}"),
    ];
    if digest.has_markers() {
        if !digest.first_failure_lines.is_empty() {
            lines.push(section_header(
                "first failure",
                digest.first_failure_lines.len(),
                digest.first_failure_truncated,
            ));
            lines.extend(digest.first_failure_lines.iter().map(|l| format!("  {l}")));
        }
        if !digest.timeout_lines.is_empty() {
            lines.push(section_header(
                "timeouts",
                digest.timeout_lines.len(),
                digest.timeout_lines_truncated,
            ));
            lines.extend(digest.timeout_lines.iter().map(|l| format!("  {l}")));
        }
        if !digest.failed_names.is_empty() {
            lines.push(section_header(
                "failed tests",
                digest.failed_names.len(),
                digest.failed_names_truncated,
            ));
            lines.extend(digest.failed_names.iter().map(|n| format!("  {n}")));
        }
        if !digest.panics.is_empty() {
            lines.push(if digest.panics_truncated {
                format!(
                    "panics (first {} shown; more omitted):",
                    digest.panics.len()
                )
            } else {
                "panics:".to_string()
            });
            for panic in &digest.panics {
                lines.push(format!("  {}", panic.line));
                if let Some(assertion) = &panic.assertion {
                    lines.push(format!("    {assertion}"));
                }
            }
        }
        return lines;
    }
    match digest.tail_source {
        TailSource::BeforeError => lines.push(format!(
            "no failure markers found; last {} non-blank lines before ##[error]:",
            digest.tail.len()
        )),
        TailSource::EndOfLog if digest.tail.is_empty() => {
            lines.push("no failure markers found; log is empty".to_string());
        }
        TailSource::EndOfLog => lines.push(format!(
            "no failure markers or ##[error] found; last {} non-blank lines of log:",
            digest.tail.len()
        )),
    }
    lines.extend(digest.tail.iter().map(|l| format!("  {l}")));
    if let Some(error_line) = &digest.error_line {
        lines.push("error marker:".to_string());
        lines.push(format!("  {error_line}"));
    }
    lines
}

fn truncation_marker(omitted: usize, run_id: u64, job_id: u64) -> String {
    format!(
        "[digest truncated at {DIGEST_MAX_BYTES} bytes: {omitted} more lines omitted; \
         see run {run_id} job {job_id}]"
    )
}

/// Join `lines` with `\n`; when over [`DIGEST_MAX_BYTES`], keep the longest
/// prefix of whole lines that fits together with the truncation marker.
fn fit_lines(lines: &[String], run_id: u64, job_id: u64) -> String {
    let mut prefix = Vec::with_capacity(lines.len() + 1);
    prefix.push(0usize);
    for line in lines {
        prefix.push(prefix[prefix.len() - 1] + line.len() + 1);
    }
    let join = |n: usize| {
        let mut out = String::with_capacity(prefix[n]);
        for line in &lines[..n] {
            out.push_str(line);
            out.push('\n');
        }
        out
    };
    if prefix[lines.len()] <= DIGEST_MAX_BYTES {
        return join(lines.len());
    }
    let mut keep = lines.len();
    loop {
        keep -= 1;
        let marker = truncation_marker(lines.len() - keep, run_id, job_id);
        if prefix[keep] + marker.len() < DIGEST_MAX_BYTES || keep == 0 {
            let mut out = join(keep);
            out.push_str(&marker);
            out.push('\n');
            return out;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufReader, Cursor, Read};

    fn digest(log: &str) -> CiJobDigest {
        digest_job_log(Cursor::new(log.as_bytes())).unwrap()
    }

    fn assert_clean(out: &str) {
        assert!(!out.contains('\x1b'), "ANSI escape left in {out:?}");
        assert!(!out.contains("2026-11-20T"), "timestamp left in {out:?}");
        assert!(out.len() <= DIGEST_MAX_BYTES, "{} bytes", out.len());
    }

    const PR_1016_LOG: &str = concat!(
        "2026-11-20T10:00:00.0000000Z ##[group]Run rtk proxy ./preflight.sh\n",
        "2026-11-20T10:00:01.1234567Z running 412 tests\n",
        "2026-11-20T10:00:02.1234567Z test serve::a::ok_one ... ok\n",
        "2026-11-20T10:00:02.1234567Z test serve::merge::one ... FAILED\n",
        "2026-11-20T10:00:02.1234567Z test serve::merge::two ... \x1b[31mFAILED\x1b[0m\n",
        "2026-11-20T10:00:03.1234567Z test serve::rework::three ... FAILED\n",
        "2026-11-20T10:00:03.1234567Z test serve::rework::four ... FAILED\n",
        "2026-11-20T10:00:04.1234567Z test serve::review::five ... FAILED\n",
        "2026-11-20T10:00:04.1234567Z test serve::review::six ... FAILED\n",
        "2026-11-20T10:00:05.1234567Z test serve::runner::seven ... FAILED\r\n",
        "2026-11-20T10:00:05.1234567Z test serve::merge::one ... FAILED\n",
        "2026-11-20T10:00:06.1234567Z \n",
        "2026-11-20T10:10:00.0000000Z \x1b[1;33mpreflight timing: test binary 'quorum' timed out after 600s\x1b[0m\n",
        "2026-11-20T10:10:00.0000000Z preflight timing: FIRST FAILURE: test binary 'quorum' (timed_out, exit 124)\n",
        "2026-11-20T10:10:01.0000000Z ##[error]Process completed with exit code 124.\n",
    );

    #[test]
    fn pr_1016_timeout_with_failed_names() {
        let d = digest(PR_1016_LOG);
        let out = render_digest(&d, "preflight", 1016, 4242);
        assert_eq!(
            out,
            concat!(
                "CI job failure digest\n",
                "job: preflight\n",
                "run id: 1016\n",
                "job id: 4242\n",
                "first failure (1):\n",
                "  preflight timing: FIRST FAILURE: test binary 'quorum' (timed_out, exit 124)\n",
                "timeouts (1):\n",
                "  preflight timing: test binary 'quorum' timed out after 600s\n",
                "failed tests (7):\n",
                "  serve::merge::one\n",
                "  serve::merge::two\n",
                "  serve::rework::three\n",
                "  serve::rework::four\n",
                "  serve::review::five\n",
                "  serve::review::six\n",
                "  serve::runner::seven\n",
            )
        );
        assert_eq!(out.len(), 405);
        assert_clean(&out);
    }

    #[test]
    fn first_failure_survives_many_earlier_timeout_lines() {
        // timing.sh streams test output first and prints FIRST FAILURE only
        // after the binary exits; `wait_until` panics say "timed out after".
        let mut log = String::new();
        for i in 0..TIMEOUT_LINES_MAX + 4 {
            log.push_str(&format!(
                "2026-11-20T10:00:00.0Z thread 't{i}' panicked at quorum/tests/x.rs:1:1:\n\
                 2026-11-20T10:00:00.0Z timed out after 30s waiting for w{i}\n"
            ));
        }
        log.push_str(
            "2026-11-20T10:10:00.0Z preflight timing: FIRST FAILURE: test binary 'quorum' (timed_out, exit 124)\n",
        );
        let d = digest(&log);
        assert_eq!(
            d.first_failure_lines,
            vec![
                "preflight timing: FIRST FAILURE: test binary 'quorum' (timed_out, exit 124)"
                    .to_string()
            ]
        );
        assert!(!d.first_failure_truncated);
        assert_eq!(d.timeout_lines.len(), TIMEOUT_LINES_MAX);
        assert!(d.timeout_lines_truncated);
        let out = render_digest(&d, "preflight", 5, 6);
        let mut expected = String::from(
            "CI job failure digest\njob: preflight\nrun id: 5\njob id: 6\n\
             first failure (1):\n  \
             preflight timing: FIRST FAILURE: test binary 'quorum' (timed_out, exit 124)\n\
             timeouts (first 8 shown; more omitted):\n",
        );
        for i in 0..TIMEOUT_LINES_MAX {
            expected.push_str(&format!("  timed out after 30s waiting for w{i}\n"));
        }
        expected.push_str("panics (first 5 shown; more omitted):\n");
        for i in 0..PANICS_MAX {
            expected.push_str(&format!(
                "  thread 't{i}' panicked at quorum/tests/x.rs:1:1:\n    \
                 timed out after 30s waiting for w{i}\n"
            ));
        }
        assert_eq!(out, expected);
        assert_eq!(out.len(), 968);
        assert_clean(&out);

        let mut many = String::new();
        for i in 0..FIRST_FAILURE_LINES_MAX + 1 {
            many.push_str(&format!("preflight timing: FIRST FAILURE: step {i}\n"));
        }
        let d = digest(&many);
        assert_eq!(d.first_failure_lines.len(), FIRST_FAILURE_LINES_MAX);
        assert!(d.first_failure_truncated);
        assert!(
            render_digest(&d, "p", 1, 2).contains("first failure (first 4 shown; more omitted):\n")
        );
    }

    #[test]
    fn libtest_panic_pairs_with_assertion() {
        let log = concat!(
            "2026-11-20T10:00:00.1Z test lifecycle::tests::merge_guard ... FAILED\n",
            "2026-11-20T10:00:00.1Z \n",
            "2026-11-20T10:00:00.1Z failures:\n",
            "2026-11-20T10:00:00.1Z \n",
            "2026-11-20T10:00:00.1Z ---- lifecycle::tests::merge_guard stdout ----\n",
            "2026-11-20T10:00:00.1Z thread 'lifecycle::tests::merge_guard' panicked at quorum-core/src/lifecycle.rs:10:5:\n",
            "2026-11-20T10:00:00.1Z \n",
            "2026-11-20T10:00:00.1Z assertion `left == right` failed\n",
            "2026-11-20T10:00:00.1Z   left: 1\n",
            "2026-11-20T10:00:00.1Z  right: 2\n",
            "2026-11-20T10:00:00.1Z ##[error]Process completed with exit code 101.\n",
        );
        let out = render_digest(&digest(log), "test", 7, 8);
        assert_eq!(
            out,
            concat!(
                "CI job failure digest\n",
                "job: test\n",
                "run id: 7\n",
                "job id: 8\n",
                "failed tests (1):\n",
                "  lifecycle::tests::merge_guard\n",
                "panics:\n",
                "  thread 'lifecycle::tests::merge_guard' panicked at quorum-core/src/lifecycle.rs:10:5:\n",
                "    assertion `left == right` failed\n",
            )
        );
        assert_eq!(out.len(), 235);
        assert_clean(&out);
    }

    #[test]
    fn clippy_error_uses_tail_before_error() {
        let log = concat!(
            "2026-11-20T10:00:00.0000000Z ##[group]Run cargo clippy -- -D warnings\n",
            "2026-11-20T10:00:01.0000000Z \x1b[0m\x1b[1m\x1b[38;5;9merror\x1b[0m: unused variable: `x`\n",
            "2026-11-20T10:00:01.0000000Z  --> quorum-core/src/foo.rs:3:9\n",
            "2026-11-20T10:00:01.0000000Z   |\n",
            "2026-11-20T10:00:01.0000000Z \n",
            "2026-11-20T10:00:01.0000000Z error: could not compile `quorum-core` due to 1 previous error\n",
            "2026-11-20T10:00:02.0000000Z ##[error]Process completed with exit code 101.\n",
            "2026-11-20T10:00:03.0000000Z Post job cleanup.\n",
        );
        let d = digest(log);
        assert!(!d.has_markers());
        assert_eq!(d.tail_source, TailSource::BeforeError);
        let out = render_digest(&d, "clippy", 11, 12);
        assert_eq!(
            out,
            concat!(
                "CI job failure digest\n",
                "job: clippy\n",
                "run id: 11\n",
                "job id: 12\n",
                "no failure markers found; last 5 non-blank lines before ##[error]:\n",
                "  ##[group]Run cargo clippy -- -D warnings\n",
                "  error: unused variable: `x`\n",
                "   --> quorum-core/src/foo.rs:3:9\n",
                "    |\n",
                "  error: could not compile `quorum-core` due to 1 previous error\n",
                "error marker:\n",
                "  ##[error]Process completed with exit code 101.\n",
            )
        );
        assert_eq!(out.len(), 364);
        assert_clean(&out);
    }

    fn numbered_log(n: usize, terminal: &str) -> String {
        let mut log = String::new();
        for i in 1..=n {
            log.push_str(&format!("2026-11-20T10:00:00.0000000Z step line {i}\n"));
            log.push_str("2026-11-20T10:00:00.0000000Z    \n");
        }
        log.push_str(terminal);
        log
    }

    fn expected_tail(header: &str, from: usize, to: usize, footer: &[&str]) -> String {
        let mut out = String::from("CI job failure digest\njob: build\nrun id: 1\njob id: 2\n");
        out.push_str(header);
        out.push('\n');
        for i in from..=to {
            out.push_str(&format!("  step line {i}\n"));
        }
        for line in footer {
            out.push_str(line);
            out.push('\n');
        }
        out
    }

    #[test]
    fn cancelled_job_tail_is_last_thirty_non_blank_lines() {
        let log = numbered_log(
            45,
            "2026-11-20T10:00:00.0000000Z ##[error]The operation was canceled.\n",
        );
        let d = digest(&log);
        assert_eq!(d.tail.len(), TAIL_LINES);
        let out = render_digest(&d, "build", 1, 2);
        assert_eq!(
            out,
            expected_tail(
                "no failure markers found; last 30 non-blank lines before ##[error]:",
                16,
                45,
                &["error marker:", "  ##[error]The operation was canceled."],
            )
        );
        assert_eq!(out.len(), 624);
        assert_clean(&out);
    }

    #[test]
    fn no_marker_log_without_error_uses_end_of_log_tail() {
        let d = digest(&numbered_log(3, ""));
        assert_eq!(d.tail_source, TailSource::EndOfLog);
        let out = render_digest(&d, "build", 1, 2);
        assert_eq!(
            out,
            expected_tail(
                "no failure markers or ##[error] found; last 3 non-blank lines of log:",
                1,
                3,
                &[],
            )
        );
        assert_clean(&out);

        let empty = render_digest(&digest(""), "build", 1, 2);
        assert_eq!(
            empty,
            "CI job failure digest\njob: build\nrun id: 1\njob id: 2\n\
             no failure markers found; log is empty\n"
        );
    }

    #[test]
    fn failed_names_are_capped() {
        let mut log = String::new();
        for i in 0..25 {
            log.push_str(&format!("test t{i:02} ... FAILED\n"));
        }
        let d = digest(&log);
        assert_eq!(d.failed_names.len(), FAILED_NAMES_MAX);
        assert!(d.failed_names_truncated);
        let out = render_digest(&d, "build", 1, 2);
        let mut expected = String::from(
            "CI job failure digest\njob: build\nrun id: 1\njob id: 2\n\
             failed tests (first 20 shown; more omitted):\n",
        );
        for i in 0..20 {
            expected.push_str(&format!("  t{i:02}\n"));
        }
        assert_eq!(out, expected);
        assert!(!out.contains("t20"));
    }

    #[test]
    fn oversized_digest_is_cut_on_line_boundary_with_marker() {
        let mut log = String::new();
        for i in 0..FAILED_NAMES_MAX {
            log.push_str(&format!("test {}{i:02} ... FAILED\n", "n".repeat(400)));
        }
        let d = digest(&log);
        let full = render_lines(&d, "build", 1, 2);
        let out = render_digest(&d, "build", 1, 2);
        assert!(out.len() <= DIGEST_MAX_BYTES);
        // 4 header + 1 section header + 9 names (9 * 405 = 3645 bytes) fit
        // with the marker; the remaining 11 names are omitted.
        let kept: String = full[..14].iter().map(|l| format!("{l}\n")).collect();
        let marker = "[digest truncated at 4096 bytes: 11 more lines omitted; see run 1 job 2]\n";
        assert_eq!(out, format!("{kept}{marker}"));
        assert_eq!(out.len(), 3790);
        assert_clean(&out);
    }

    #[test]
    fn huge_single_line_is_streamed_with_bounded_buffers() {
        // 8 MiB without a newline, then markers; generated lazily, never
        // materialized, so the parser must stream it.
        let huge = io::repeat(b'x').take(8 << 20);
        let rest = Cursor::new(
            b"\n2026-11-20T10:00:00.0Z \x1b[31mtest big::after ... FAILED\x1b[0m\n".to_vec(),
        );
        let reader = BufReader::new(huge.chain(rest));
        let d = digest_job_log(reader).unwrap();
        assert_eq!(d.failed_names, vec!["big::after".to_string()]);
        assert_eq!(d.tail.len(), 2);
        assert_eq!(
            d.tail[0],
            format!("{}{LINE_TRUNCATED_SUFFIX}", "x".repeat(LINE_MAX_BYTES))
        );
        let out = render_digest(&d, "build", 3, 4);
        assert_eq!(
            out,
            "CI job failure digest\njob: build\nrun id: 3\njob id: 4\n\
             failed tests (1):\n  big::after\n"
        );

        // A huge no-marker log: the tail fallback stays within the cap.
        let mut long_lines = String::new();
        for _ in 0..200 {
            long_lines.push_str(&"y".repeat(2000));
            long_lines.push('\n');
        }
        long_lines.push_str("##[error]Process completed with exit code 1.\n");
        let d = digest(&long_lines);
        assert_eq!(d.tail.len(), TAIL_LINES);
        let out = render_digest(&d, "build", 3, 4);
        assert!(out.len() <= DIGEST_MAX_BYTES);
        assert!(out.ends_with(
            "[digest truncated at 4096 bytes: 25 more lines omitted; see run 3 job 4]\n"
        ));
        assert_eq!(out.len(), 3918);
    }

    #[test]
    fn read_capped_line_discards_overflow_and_reports_cut() {
        let mut r = Cursor::new(b"abcdef\nxy\nlast".to_vec());
        let mut buf = Vec::new();
        assert_eq!(read_capped_line(&mut r, &mut buf, 4).unwrap(), Some(true));
        assert_eq!(buf, b"abcd");
        assert_eq!(read_capped_line(&mut r, &mut buf, 4).unwrap(), Some(false));
        assert_eq!(buf, b"xy");
        assert_eq!(read_capped_line(&mut r, &mut buf, 4).unwrap(), Some(false));
        assert_eq!(buf, b"last");
        assert_eq!(read_capped_line(&mut r, &mut buf, 4).unwrap(), None);
    }

    #[test]
    fn strips_timestamps_and_escape_sequences() {
        assert_eq!(
            strip_timestamp("2026-11-26T06:00:00.1234567Z hello"),
            "hello"
        );
        assert_eq!(strip_timestamp("2026-11-26T06:00:00Z hello"), "hello");
        assert_eq!(
            strip_timestamp("2026-11-26T06:00:00.Z hello"),
            "2026-11-26T06:00:00.Z hello"
        );
        assert_eq!(strip_timestamp("not a timestamp"), "not a timestamp");
        assert_eq!(strip_timestamp("2026"), "2026");
        assert_eq!(
            strip_ansi(
                "\x1b[1;31mred\x1b[0m \x1b]8;;https://x\x07link\x1b]8;;\x1b\\ \u{9b}2Kz\r\x00\tend"
            ),
            "red link z\tend"
        );
        assert_eq!(clean_line(b"\xff ok", false), "\u{fffd} ok");
        assert_eq!(
            clean_line("\u{feff}2026-11-26T06:00:00Z bom".as_bytes(), false),
            "bom"
        );
        let multibyte = "é".repeat(LINE_MAX_BYTES);
        let capped = clean_line(multibyte.as_bytes(), false);
        assert!(capped.ends_with(LINE_TRUNCATED_SUFFIX));
        assert_eq!(capped.len(), LINE_MAX_BYTES + LINE_TRUNCATED_SUFFIX.len());
    }

    #[test]
    fn panic_followed_by_panic_has_no_assertion_and_panics_are_capped() {
        let mut log = String::from("thread 'a' panicked at a.rs:1:1:\n");
        for i in 0..PANICS_MAX + 2 {
            log.push_str(&format!("thread 't{i}' panicked at x.rs:1:1:\nmsg {i}\n"));
        }
        let d = digest(&log);
        assert_eq!(d.panics.len(), PANICS_MAX);
        assert!(d.panics_truncated);
        assert_eq!(d.panics[0].assertion, None);
        assert_eq!(d.panics[1].assertion.as_deref(), Some("msg 0"));
    }
}
