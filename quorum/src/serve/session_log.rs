//! Per-agent session log: hierarchical files (stream.jsonl, transcript.md, meta.json).
//!
//! Each agent session gets its own directory under `{log_dir}/{agent}-{start_ts}/`.
//! `stream.jsonl` captures raw events, `transcript.md` formats assistant output for
//! human reading, and `meta.json` summarizes the session on finalize.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use super::render;
use super::stream::Event;

pub struct SessionLog {
    dir: PathBuf,
    stream_file: File,
    transcript_file: File,
    meta: SessionMeta,
}

#[derive(serde::Serialize)]
struct SessionMeta {
    agent: String,
    role: String,
    task_id: Option<i64>,
    session_id: String,
    branch: String,
    start_time: i64,
    end_time: Option<i64>,
    cost_tokens: i64,
    cost_usd: f64,
    final_phase: String,
    verdict: Option<String>,
    rework_count: u32,
}

impl SessionLog {
    pub fn create(
        log_dir: &Path,
        agent: &str,
        role: &str,
        task_id: Option<i64>,
        session_id: &str,
        branch: &str,
        start_time: i64,
    ) -> io::Result<Self> {
        let dir = log_dir.join(format!("{agent}-{start_time}"));
        fs::create_dir_all(&dir)?;

        let stream_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("stream.jsonl"))?;

        let mut transcript_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("transcript.md"))?;

        writeln!(transcript_file, "# Session: {agent} ({role})")?;
        if let Some(tid) = task_id {
            writeln!(transcript_file, "Task #{tid} · branch `{branch}`\n")?;
        } else {
            writeln!(transcript_file, "Branch `{branch}`\n")?;
        }

        let meta = SessionMeta {
            agent: agent.to_string(),
            role: role.to_string(),
            task_id,
            session_id: session_id.to_string(),
            branch: branch.to_string(),
            start_time,
            end_time: None,
            cost_tokens: 0,
            cost_usd: 0.0,
            final_phase: "working".to_string(),
            verdict: None,
            rework_count: 0,
        };

        Ok(SessionLog {
            dir,
            stream_file,
            transcript_file,
            meta,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn log_event(&mut self, event: &Event) {
        if let Ok(json) = serde_json::to_string(event) {
            let _ = writeln!(self.stream_file, "{json}");
        }

        if let Some(rendered) = render::render_event(event) {
            let _ = writeln!(self.transcript_file, "{rendered}");
        }

        let _ = self.transcript_file.flush();
        let _ = self.stream_file.flush();
    }

    pub fn set_phase(&mut self, phase: &str) {
        self.meta.final_phase = phase.to_string();
    }

    pub fn update_cost(&mut self, tokens: i64, cost_usd: f64) {
        self.meta.cost_tokens = tokens;
        self.meta.cost_usd = cost_usd;
    }

    pub fn log_rework(&mut self, round: u32) {
        self.meta.rework_count = round;
        let _ = writeln!(self.transcript_file, "\n# Rework round {round}\n");
    }

    pub fn finalize(&mut self, verdict: Option<&str>) {
        self.meta.end_time = Some(super::now_unix());
        self.meta.verdict = verdict.map(|s| s.to_string());

        let meta_path = self.dir.join("meta.json");
        if let Ok(json) = serde_json::to_string_pretty(&self.meta) {
            let _ = fs::write(meta_path, json);
        }
    }
}

/// Delete session log directories older than `max_age_secs`.
pub fn sweep_logs(log_dir: &Path, max_age_secs: u64) -> io::Result<u64> {
    let mut removed = 0u64;
    let entries = match fs::read_dir(log_dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let meta_path = path.join("meta.json");
        let age = if meta_path.exists() {
            age_from_meta(&meta_path).unwrap_or_else(|| age_from_mtime(&path))
        } else {
            age_from_mtime(&path)
        };
        if age > max_age_secs {
            let _ = fs::remove_dir_all(&path);
            removed += 1;
        }
    }
    Ok(removed)
}

fn age_from_meta(path: &Path) -> Option<u64> {
    let data = fs::read_to_string(path).ok()?;
    let v: serde_json::Value = serde_json::from_str(&data).ok()?;
    let end_time = v.get("end_time")?.as_i64()?;
    let now = super::now_unix();
    Some((now - end_time).max(0) as u64)
}

fn age_from_mtime(path: &Path) -> u64 {
    path.metadata()
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.elapsed().ok())
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn session_log_creates_files_and_finalizes() {
        let dir = TempDir::new().unwrap();
        let log_dir = dir.path();

        let mut log = SessionLog::create(
            log_dir,
            "TestAgent",
            "worker",
            Some(42),
            "sess-1",
            "feat/test",
            1000,
        )
        .unwrap();

        assert!(log.dir().join("stream.jsonl").exists());
        assert!(log.dir().join("transcript.md").exists());

        let event = Event::Assistant {
            message: serde_json::json!({"content": "Hello world"}),
        };
        log.log_event(&event);

        let tool_event = Event::ToolUse {
            name: "Bash".into(),
            input: serde_json::json!({"command": "ls"}),
        };
        log.log_event(&tool_event);

        log.update_cost(500, 0.05);
        log.set_phase("awaiting-review");
        log.finalize(Some("approved"));

        assert!(log.dir().join("meta.json").exists());

        let meta: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(log.dir().join("meta.json")).unwrap())
                .unwrap();
        assert_eq!(meta["agent"], "TestAgent");
        assert_eq!(meta["role"], "worker");
        assert_eq!(meta["task_id"], 42);
        assert_eq!(meta["cost_tokens"], 500);
        assert_eq!(meta["final_phase"], "awaiting-review");
        assert_eq!(meta["verdict"], "approved");

        let stream = fs::read_to_string(log.dir().join("stream.jsonl")).unwrap();
        let lines: Vec<&str> = stream.lines().collect();
        assert_eq!(lines.len(), 2);

        let transcript = fs::read_to_string(log.dir().join("transcript.md")).unwrap();
        assert!(transcript.contains("Hello world"));
        assert!(transcript.contains("> Bash:"));
    }

    #[test]
    fn sweep_logs_removes_old_directories() {
        let dir = TempDir::new().unwrap();
        let log_dir = dir.path();

        // Create a "session" dir with a meta.json that has an old end_time.
        let old_dir = log_dir.join("OldAgent-100");
        fs::create_dir_all(&old_dir).unwrap();
        let old_meta = serde_json::json!({
            "agent": "OldAgent",
            "end_time": 100,
        });
        fs::write(old_dir.join("meta.json"), old_meta.to_string()).unwrap();

        // Create a "session" dir with a recent end_time.
        let new_dir = log_dir.join("NewAgent-9999999999");
        fs::create_dir_all(&new_dir).unwrap();
        let new_meta = serde_json::json!({
            "agent": "NewAgent",
            "end_time": 9999999999i64,
        });
        fs::write(new_dir.join("meta.json"), new_meta.to_string()).unwrap();

        let removed = sweep_logs(log_dir, 86400).unwrap();
        assert!(removed >= 1);
        assert!(!old_dir.exists(), "old session dir should be removed");
        assert!(new_dir.exists(), "new session dir should be kept");
    }

    #[test]
    fn sweep_logs_handles_missing_dir() {
        let dir = TempDir::new().unwrap();
        let nonexistent = dir.path().join("nope");
        let removed = sweep_logs(&nonexistent, 86400).unwrap();
        assert_eq!(removed, 0);
    }

    #[test]
    fn result_event_appears_in_stream_and_transcript() {
        let dir = TempDir::new().unwrap();
        let mut log =
            SessionLog::create(dir.path(), "Agent", "worker", Some(1), "s", "b", 1000).unwrap();

        let result_event = Event::Result {
            result: serde_json::json!({}),
            usage: Some(super::super::stream::Usage {
                input_tokens: 200,
                output_tokens: 100,
            }),
            total_cost_usd: Some(0.0123),
            num_turns: Some(1),
            duration_ms: Some(5000),
            is_error: Some(false),
        };
        log.log_event(&result_event);

        let stream = fs::read_to_string(log.dir().join("stream.jsonl")).unwrap();
        assert_eq!(stream.lines().count(), 1);
        assert!(stream.contains("\"result\""));

        let transcript = fs::read_to_string(log.dir().join("transcript.md")).unwrap();
        assert!(transcript.contains("300 tokens"));
        assert!(transcript.contains("$0.0123"));
    }

    #[test]
    fn assistant_array_content_renders_to_transcript() {
        let dir = TempDir::new().unwrap();
        let mut log =
            SessionLog::create(dir.path(), "Agent", "worker", Some(1), "s", "b", 1000).unwrap();

        let event = Event::Assistant {
            message: serde_json::json!({
                "content": [
                    {"type": "text", "text": "I'll check the code."},
                    {"type": "tool_use", "name": "Read", "input": {"file_path": "/src/main.rs"}},
                ]
            }),
        };
        log.log_event(&event);

        let transcript = fs::read_to_string(log.dir().join("transcript.md")).unwrap();
        assert!(
            transcript.contains("I'll check the code."),
            "array text blocks must render: {transcript}"
        );
        assert!(
            transcript.contains("> Read: main.rs"),
            "array tool_use blocks must render: {transcript}"
        );
    }

    #[test]
    fn log_rework_writes_header() {
        let dir = TempDir::new().unwrap();
        let mut log =
            SessionLog::create(dir.path(), "Agent", "worker", Some(1), "s", "b", 1000).unwrap();
        log.log_rework(2);
        let transcript = fs::read_to_string(log.dir().join("transcript.md")).unwrap();
        assert!(transcript.contains("Rework round 2"));
    }

    #[test]
    fn log_event_flushes_immediately() {
        let dir = TempDir::new().unwrap();
        let mut log =
            SessionLog::create(dir.path(), "Agent", "worker", Some(1), "s", "b", 1000).unwrap();

        let event = Event::ToolUse {
            name: "Bash".into(),
            input: serde_json::json!({"command": "cargo test"}),
        };
        log.log_event(&event);

        // Read from a separate file handle — verifies data is flushed to OS
        let transcript = fs::read_to_string(log.dir().join("transcript.md")).unwrap();
        assert!(
            transcript.contains("> Bash:"),
            "transcript must be visible to other readers immediately after log_event"
        );
        let stream = fs::read_to_string(log.dir().join("stream.jsonl")).unwrap();
        assert!(
            stream.contains("tool_use"),
            "stream.jsonl must be visible immediately after log_event"
        );
    }
}
