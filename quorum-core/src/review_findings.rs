//! Structured review findings parsed from PR comment threads (#93).
//!
//! A cheap Haiku pass interprets author↔reviewer interaction in PR comments,
//! extracting blocking findings, non-blocking suggestions, and author rebuttals.
//! Rows are write-once per interpretation run; re-running for the same PR
//! replaces all prior findings (delete + re-insert).

use crate::clock;
use crate::db::begin_immediate;
use crate::error::Result;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewFinding {
    #[serde(skip_deserializing)]
    pub id: i64,
    #[serde(default)]
    pub pr_number: i64,
    #[serde(default)]
    pub task_id: Option<i64>,
    pub reviewer: String,
    pub kind: String,
    #[serde(default)]
    pub author_pushback: bool,
    #[serde(default)]
    pub pushback_accepted: Option<bool>,
    #[serde(default)]
    pub severity: Option<String>,
    pub text: String,
    #[serde(default = "default_source")]
    pub source_endpoint: String,
}

fn default_source() -> String {
    "pulls".to_string()
}

/// Haiku response schema: array of findings.
#[derive(Debug, Deserialize)]
pub struct InterpreterResponse {
    pub findings: Vec<ReviewFinding>,
}

/// Replace all findings for a PR (idempotent re-run).
pub fn replace_for_pr(
    conn: &mut Connection,
    pr_number: i64,
    findings: &[ReviewFinding],
) -> Result<()> {
    let now = clock::now();
    let tx = begin_immediate(conn)?;
    tx.execute(
        "DELETE FROM review_findings WHERE pr_number = ?1",
        params![pr_number],
    )?;
    for f in findings {
        tx.execute(
            "INSERT INTO review_findings
                (pr_number, task_id, reviewer, kind, author_pushback, pushback_accepted,
                 severity, text, source_endpoint, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                pr_number,
                f.task_id,
                f.reviewer,
                f.kind,
                f.author_pushback as i64,
                f.pushback_accepted.map(|b| b as i64),
                f.severity,
                f.text,
                f.source_endpoint,
                now,
            ],
        )?;
    }
    tx.commit()?;
    Ok(())
}

/// List all findings for a PR.
pub fn list_for_pr(conn: &Connection, pr_number: i64) -> Result<Vec<ReviewFinding>> {
    let mut stmt = conn.prepare(
        "SELECT id, pr_number, task_id, reviewer, kind, author_pushback, pushback_accepted,
                severity, text, source_endpoint
         FROM review_findings WHERE pr_number = ?1 ORDER BY id",
    )?;
    let rows = stmt.query_map(params![pr_number], |r| {
        Ok(ReviewFinding {
            id: r.get(0)?,
            pr_number: r.get(1)?,
            task_id: r.get(2)?,
            reviewer: r.get(3)?,
            kind: r.get(4)?,
            author_pushback: r.get::<_, i64>(5)? != 0,
            pushback_accepted: r.get::<_, Option<i64>>(6)?.map(|v| v != 0),
            severity: r.get(7)?,
            text: r.get(8)?,
            source_endpoint: r.get(9)?,
        })
    })?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

/// Build the Haiku interpreter prompt from raw comment JSON.
pub fn build_prompt(
    pulls_comments_json: &str,
    issues_comments_json: &str,
    pr_number: i64,
) -> String {
    format!(
        r#"You are a review-comment interpreter. You do NOT judge the code — you parse the author↔reviewer interaction in PR #{pr_number} comments.

## Input

### Review/inline comments (pulls/{pr_number}/comments)
{pulls_comments_json}

### Conversation comments (issues/{pr_number}/comments)
{issues_comments_json}

## Task

Extract every distinct review finding from the comments. For each finding, classify:

1. **kind**: `"blocking"` if the reviewer required a change (language like "must", "needs to", "please fix", marked as a blocker), or `"suggestion"` if it was a non-blocking nice-to-have ("consider", "nit", "might want to", "optional", "could").
2. **reviewer**: the GitHub username of the commenter who raised the finding.
3. **author_pushback**: `true` if the PR author pushed back on this finding ("not actually broken because", "disagree", "intentional", declined to change).
4. **pushback_accepted**: `true` if the reviewer accepted the pushback (dropped the objection, said "fair point", approved despite it). `false` if the reviewer overrode/insisted. `null` if there was no pushback or the resolution is unclear.
5. **severity**: one of `"critical"`, `"major"`, `"minor"`, `"nit"`, or `null` if unclear.
6. **text**: a one-sentence summary of the finding.
7. **source_endpoint**: `"pulls"` if the finding came from the review/inline comments, `"issues"` if from the conversation comments.

## Output

Respond with ONLY a JSON object:
```json
{{"findings": [
  {{"reviewer": "...", "kind": "blocking", "author_pushback": false, "pushback_accepted": null, "severity": "major", "text": "...", "source_endpoint": "pulls"}},
  ...
]}}
```

If there are no review findings (e.g. only bot comments or merge notifications), return `{{"findings": []}}`.
Do NOT invent findings — only extract what the comments actually contain."#
    )
}

/// Parse the Haiku response into structured findings.
pub fn parse_response(
    text: &str,
    pr_number: i64,
    task_id: Option<i64>,
) -> Option<Vec<ReviewFinding>> {
    let json_text = extract_json(text)?;
    let mut resp: InterpreterResponse = serde_json::from_str(json_text).ok()?;
    for f in &mut resp.findings {
        f.pr_number = pr_number;
        f.task_id = task_id;
    }
    Some(resp.findings)
}

fn extract_json(text: &str) -> Option<&str> {
    let trimmed = text.trim();
    if trimmed.starts_with('{') {
        return Some(trimmed);
    }
    if let Some(start) = trimmed.find("```json") {
        let after = &trimmed[start + 7..];
        if let Some(end) = after.find("```") {
            return Some(after[..end].trim());
        }
    }
    if let Some(start) = trimmed.find("```") {
        let after = &trimmed[start + 3..];
        if let Some(end) = after.find("```") {
            let inner = after[..end].trim();
            if inner.starts_with('{') {
                return Some(inner);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;

    fn test_conn() -> (Connection, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let conn = db::open(&dir.path().join("q.db")).unwrap();
        (conn, dir)
    }

    #[test]
    fn replace_and_list_roundtrip() {
        let (mut conn, _dir) = test_conn();
        let findings = vec![
            ReviewFinding {
                id: 0,
                pr_number: 100,
                task_id: Some(42),
                reviewer: "alice".into(),
                kind: "blocking".into(),
                author_pushback: false,
                pushback_accepted: None,
                severity: Some("major".into()),
                text: "Missing null check".into(),
                source_endpoint: "pulls".into(),
            },
            ReviewFinding {
                id: 0,
                pr_number: 100,
                task_id: Some(42),
                reviewer: "alice".into(),
                kind: "suggestion".into(),
                author_pushback: true,
                pushback_accepted: Some(true),
                severity: Some("nit".into()),
                text: "Consider renaming variable".into(),
                source_endpoint: "issues".into(),
            },
        ];
        replace_for_pr(&mut conn, 100, &findings).unwrap();
        let got = list_for_pr(&conn, 100).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].kind, "blocking");
        assert!(!got[0].author_pushback);
        assert_eq!(got[0].pushback_accepted, None);
        assert_eq!(got[1].kind, "suggestion");
        assert!(got[1].author_pushback);
        assert_eq!(got[1].pushback_accepted, Some(true));
        assert_eq!(got[1].source_endpoint, "issues");
    }

    #[test]
    fn replace_is_idempotent() {
        let (mut conn, _dir) = test_conn();
        let f1 = vec![ReviewFinding {
            id: 0,
            pr_number: 100,
            task_id: None,
            reviewer: "bob".into(),
            kind: "blocking".into(),
            author_pushback: false,
            pushback_accepted: None,
            severity: None,
            text: "old finding".into(),
            source_endpoint: "pulls".into(),
        }];
        replace_for_pr(&mut conn, 100, &f1).unwrap();
        assert_eq!(list_for_pr(&conn, 100).unwrap().len(), 1);

        let f2 = vec![ReviewFinding {
            id: 0,
            pr_number: 100,
            task_id: None,
            reviewer: "bob".into(),
            kind: "suggestion".into(),
            author_pushback: false,
            pushback_accepted: None,
            severity: Some("nit".into()),
            text: "new finding".into(),
            source_endpoint: "pulls".into(),
        }];
        replace_for_pr(&mut conn, 100, &f2).unwrap();
        let got = list_for_pr(&conn, 100).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].text, "new finding");
    }

    #[test]
    fn parse_response_canned_thread() {
        let response = r#"{"findings": [
            {"reviewer": "rev-1", "kind": "blocking", "author_pushback": false, "pushback_accepted": null, "severity": "major", "text": "Missing error handling in parse_config", "source_endpoint": "pulls"},
            {"reviewer": "rev-1", "kind": "suggestion", "author_pushback": false, "pushback_accepted": null, "severity": "nit", "text": "Consider using a constant for the timeout value", "source_endpoint": "pulls"},
            {"reviewer": "rev-1", "kind": "blocking", "author_pushback": true, "pushback_accepted": true, "severity": "minor", "text": "Should validate input length", "source_endpoint": "issues"},
            {"reviewer": "rev-1", "kind": "blocking", "author_pushback": true, "pushback_accepted": false, "severity": "critical", "text": "Race condition in concurrent access", "source_endpoint": "issues"}
        ]}"#;
        let findings = parse_response(response, 200, Some(50)).unwrap();
        assert_eq!(findings.len(), 4);

        // Blocking finding
        assert_eq!(findings[0].kind, "blocking");
        assert!(!findings[0].author_pushback);
        assert_eq!(findings[0].pr_number, 200);
        assert_eq!(findings[0].task_id, Some(50));

        // Non-blocking suggestion
        assert_eq!(findings[1].kind, "suggestion");
        assert_eq!(findings[1].severity, Some("nit".into()));

        // Author rebuttal accepted by reviewer
        assert!(findings[2].author_pushback);
        assert_eq!(findings[2].pushback_accepted, Some(true));
        assert_eq!(findings[2].source_endpoint, "issues");

        // Author rebuttal overridden by reviewer
        assert!(findings[3].author_pushback);
        assert_eq!(findings[3].pushback_accepted, Some(false));
    }

    #[test]
    fn parse_response_with_fences() {
        let response = "Here are the findings:\n```json\n{\"findings\": [{\"reviewer\": \"a\", \"kind\": \"suggestion\", \"text\": \"nit\", \"source_endpoint\": \"pulls\"}]}\n```";
        let findings = parse_response(response, 1, None).unwrap();
        assert_eq!(findings.len(), 1);
    }

    #[test]
    fn parse_response_empty() {
        let response = r#"{"findings": []}"#;
        let findings = parse_response(response, 1, None).unwrap();
        assert!(findings.is_empty());
    }

    #[test]
    fn parse_response_invalid() {
        assert!(parse_response("not json", 1, None).is_none());
    }

    #[test]
    fn both_endpoints_captured() {
        let (mut conn, _dir) = test_conn();
        let findings = vec![
            ReviewFinding {
                id: 0,
                pr_number: 300,
                task_id: Some(60),
                reviewer: "rev".into(),
                kind: "blocking".into(),
                author_pushback: false,
                pushback_accepted: None,
                severity: Some("major".into()),
                text: "From inline review".into(),
                source_endpoint: "pulls".into(),
            },
            ReviewFinding {
                id: 0,
                pr_number: 300,
                task_id: Some(60),
                reviewer: "rev".into(),
                kind: "suggestion".into(),
                author_pushback: false,
                pushback_accepted: None,
                severity: Some("minor".into()),
                text: "From conversation".into(),
                source_endpoint: "issues".into(),
            },
        ];
        replace_for_pr(&mut conn, 300, &findings).unwrap();
        let got = list_for_pr(&conn, 300).unwrap();
        assert_eq!(got.len(), 2);
        let pulls_count = got.iter().filter(|f| f.source_endpoint == "pulls").count();
        let issues_count = got.iter().filter(|f| f.source_endpoint == "issues").count();
        assert_eq!(pulls_count, 1);
        assert_eq!(issues_count, 1);
    }
}
