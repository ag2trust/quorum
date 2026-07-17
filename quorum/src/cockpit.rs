use quorum_core::drift::{TwinPr, UnbackedPr};
use quorum_core::stats::{
    AlertMessage, BlockedTask, DaemonAgentView, DaemonLiveness, DedupedError, HealthVerdict,
    PipelineTask, QueueTask, Stats,
};
use std::io::Write;

const DEAD_THRESHOLD: i64 = 180;
const STALL_THRESHOLD: i64 = 60;

pub(crate) struct Style {
    color: bool,
}

impl Style {
    fn detect() -> Self {
        let no_color = std::env::var("NO_COLOR").is_ok();
        let is_tty = unsafe { libc::isatty(libc::STDOUT_FILENO) != 0 };
        let term = std::env::var("TERM").unwrap_or_default();
        Style {
            color: !no_color && is_tty && term != "dumb",
        }
    }

    #[cfg(test)]
    fn plain() -> Self {
        Style { color: false }
    }

    fn ansi(&self, code: &str, text: &str) -> String {
        if self.color {
            format!("\x1b[{code}m{text}\x1b[0m")
        } else {
            text.to_string()
        }
    }

    fn bold(&self, text: &str) -> String {
        self.ansi("1", text)
    }

    fn dim(&self, text: &str) -> String {
        self.ansi("2", text)
    }

    fn green(&self, text: &str) -> String {
        self.ansi("32", text)
    }

    fn yellow(&self, text: &str) -> String {
        self.ansi("33", text)
    }

    fn red(&self, text: &str) -> String {
        self.ansi("31", text)
    }

    fn freshness_dot(&self, age_secs: Option<i64>) -> String {
        match age_secs {
            Some(a) if a < STALL_THRESHOLD => {
                if self.color {
                    self.green("●")
                } else {
                    "*".to_string()
                }
            }
            Some(a) if a <= DEAD_THRESHOLD => {
                if self.color {
                    self.yellow("●")
                } else {
                    "~".to_string()
                }
            }
            _ => {
                if self.color {
                    self.red("●")
                } else {
                    "!".to_string()
                }
            }
        }
    }

    fn section_rule(&self, title: &str, width: usize) -> String {
        let label = format!(" {title} ");
        let rule_len = width.saturating_sub(label.len());
        if self.color {
            format!("{}{}", self.bold(&label), self.dim(&"─".repeat(rule_len)))
        } else {
            format!("{label}{}", "-".repeat(rule_len))
        }
    }
}

fn fmt_age(secs: i64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else if secs < 86400 {
        format!("{}h", secs / 3600)
    } else {
        format!("{}d", secs / 86400)
    }
}

fn fmt_tokens(tok: i64) -> String {
    if tok >= 1000 {
        format!("{}k", tok / 1000)
    } else {
        format!("{tok}")
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max.saturating_sub(1)).collect();
        format!("{truncated}…")
    }
}

pub fn render(s: &Stats) {
    let sty = Style::detect();
    render_with_style(s, &sty, &mut std::io::stdout());
}

pub fn render_with_style(s: &Stats, sty: &Style, w: &mut dyn Write) {
    let width = 78;

    render_header(s, sty, w, width);
    render_working(s, sty, w, width);
    render_queue(&s.queue_tasks, sty, w, width);
    render_blocked(&s.blocked, sty, w, width);
    render_pipeline(&s.pipeline, &s.daemon_agents, sty, w, width);
    render_unbacked_prs(&s.unbacked_prs, &s.twin_prs, sty, w, width);
    render_alerts(&s.alerts, sty, w, width);
    render_errors(&s.recent_errors, s.older_errors_silenced, sty, w, width);
}

fn render_header(s: &Stats, sty: &Style, w: &mut dyn Write, width: usize) {
    let verdict_str = match s.health {
        HealthVerdict::OnTrack => {
            if sty.color {
                sty.green("✓ on track")
            } else {
                "[ok] on track".to_string()
            }
        }
        HealthVerdict::Attention => {
            if sty.color {
                sty.yellow("⚠ attention")
            } else {
                "[!] attention".to_string()
            }
        }
        HealthVerdict::Stalled => {
            if sty.color {
                sty.red("✗ stalled")
            } else {
                "[X] stalled".to_string()
            }
        }
    };
    let _ = writeln!(w);
    let ver = crate::cli::short_version();
    let _ = write!(w, " {}", sty.bold("quorum"));
    let _ = write!(w, " {}", sty.dim(ver));
    let _ = writeln!(w, " {} {}", sty.bold("·"), verdict_str);
    let rule = if sty.color {
        sty.dim(&"─".repeat(width))
    } else {
        "-".repeat(width)
    };
    let _ = writeln!(w, " {rule}");

    let daemon_str = match &s.daemon {
        DaemonLiveness::None => "daemon: none".to_string(),
        DaemonLiveness::Alive {
            pid,
            heartbeat_age_secs,
        } => {
            format!(
                "daemon: pid {} alive, heartbeat {}s ago",
                pid, heartbeat_age_secs
            )
        }
        DaemonLiveness::Stale {
            pid,
            heartbeat_age_secs,
            pid_dead,
        } => {
            let age = fmt_age(*heartbeat_age_secs);
            let reason = if *pid_dead {
                format!(", pid {} dead", pid)
            } else {
                String::new()
            };
            format!("daemon: STALE — heartbeat {} ago{}", age, reason)
        }
    };
    let daemon_line = match &s.daemon {
        DaemonLiveness::None => sty.dim(&daemon_str),
        DaemonLiveness::Alive { .. } => {
            if sty.color {
                sty.green(&daemon_str)
            } else {
                daemon_str
            }
        }
        DaemonLiveness::Stale { .. } => {
            if sty.color {
                sty.red(&daemon_str)
            } else {
                daemon_str
            }
        }
    };
    let _ = writeln!(w, " {daemon_line}");

    let worker_task_ids: std::collections::HashSet<i64> = s
        .daemon_agents
        .iter()
        .filter(|d| d.role == "worker")
        .filter_map(|d| d.task_id)
        .collect();
    let orphan_reviewers = s
        .daemon_agents
        .iter()
        .filter(|d| d.role == "reviewer")
        .filter(|d| d.task_id.is_none_or(|tid| !worker_task_ids.contains(&tid)))
        .count();
    let working_count = worker_task_ids.len() + orphan_reviewers;
    let queued = s.queue_tasks.len();
    let blocked = s.blocked.len();
    let merged_hr = s.throughput.closed_last_hour;
    let stalled = s.stalled_count;
    let cost_str = if s.session_cost > 0.005 {
        format!("${:.2}", s.session_cost)
    } else {
        "—".to_string()
    };
    let _ = writeln!(
        w,
        " {} working   {} queued   {} blocked      {} merged/hr   {} stalled   session {}",
        working_count, queued, blocked, merged_hr, stalled, cost_str,
    );
}

fn render_working(s: &Stats, sty: &Style, w: &mut dyn Write, width: usize) {
    let _ = writeln!(w);
    let _ = writeln!(w, "{}", sty.section_rule("WORKING", width));

    let workers: Vec<&DaemonAgentView> = s
        .daemon_agents
        .iter()
        .filter(|d| d.role == "worker")
        .collect();

    let reviewers: Vec<&DaemonAgentView> = s
        .daemon_agents
        .iter()
        .filter(|d| d.role == "reviewer")
        .collect();

    let worker_task_ids: std::collections::HashSet<i64> =
        workers.iter().filter_map(|d| d.task_id).collect();

    let orphan_reviewers: Vec<&&DaemonAgentView> = reviewers
        .iter()
        .filter(|d| d.task_id.is_none_or(|tid| !worker_task_ids.contains(&tid)))
        .collect();

    if workers.is_empty() && orphan_reviewers.is_empty() {
        let _ = writeln!(w, "  {}", sty.dim("(idle — no agents working)"));
        return;
    }

    let _ = writeln!(
        w,
        "  {:<12}  {:<8} {:<4}  {:>4}  {:<18}  {:>3}  {:>5}  {:>3}  {:>4}  NOW",
        "AGENT", "MODEL", "EFF", "TASK", "WHAT", "UP", "TOK", "T", "EV/m"
    );

    for d in &workers {
        render_agent_row(d, sty, w);

        if let Some(tid) = d.task_id {
            for rev in &reviewers {
                if rev.task_id == Some(tid) {
                    render_reviewer_subrow(rev, sty, w);
                }
            }
        }
    }

    for rev in &orphan_reviewers {
        render_agent_row(rev, sty, w);
    }
}

fn render_agent_row(d: &DaemonAgentView, sty: &Style, w: &mut dyn Write) {
    let dot = sty.freshness_dot(d.last_activity_age_secs);
    let (model, effort) = split_tier_eff(d.tier_eff.as_deref());
    let task_str = d
        .task_id
        .map(|id| format!("#{id}"))
        .unwrap_or_else(|| "—".to_string());
    let title = d
        .task_title
        .as_deref()
        .map(|t| truncate(t, 18))
        .unwrap_or_else(|| "—".to_string());
    let up = d
        .uptime_secs
        .map(fmt_age)
        .unwrap_or_else(|| "—".to_string());
    let tok = fmt_tokens(d.cost_tokens);
    let tools = d.tool_count.to_string();
    let evm = d
        .events_per_min
        .map(|v| format!("{:.0}", v))
        .unwrap_or_else(|| "—".to_string());
    let now = d.now_label.as_deref().unwrap_or("—");
    let now_display = truncate(now, 24);
    let rework_suffix = if d.rework_count > 0 {
        format!(" ↻{}", d.rework_count)
    } else {
        String::new()
    };
    let agent_label = if d.sub_role.as_deref() == Some("r2") {
        format!("{}(r2)", d.agent)
    } else {
        d.agent.clone()
    };

    let _ = writeln!(
        w,
        "{} {:<12}  {:<8} {:<4}  {:>4}  {:<18}  {:>3}  {:>5}  {:>3}  {:>4}  {}{}",
        dot,
        agent_label,
        model,
        effort,
        task_str,
        title,
        up,
        tok,
        tools,
        evm,
        now_display,
        rework_suffix,
    );
}

fn split_tier_eff(tier_eff: Option<&str>) -> (&str, &str) {
    let value = tier_eff.unwrap_or("—");
    value.split_once('·').unwrap_or((value, "—"))
}

fn render_reviewer_subrow(rev: &DaemonAgentView, sty: &Style, w: &mut dyn Write) {
    let rev_up = rev.uptime_secs.map(fmt_age).unwrap_or_else(|| {
        rev.last_activity_age_secs
            .map(fmt_age)
            .unwrap_or_else(|| "—".to_string())
    });
    let rev_tok = fmt_tokens(rev.cost_tokens);
    let role_label = if rev.sub_role.as_deref() == Some("r2") {
        "r2 audit"
    } else {
        "reviewer"
    };
    let sub = if sty.color {
        format!(
            "    {} {}  {} · {} · {} tok",
            sty.dim("└"),
            role_label,
            rev.agent,
            rev_up,
            rev_tok,
        )
    } else {
        format!(
            "    +- {}  {} · {} · {} tok",
            role_label, rev.agent, rev_up, rev_tok,
        )
    };
    let _ = writeln!(w, "{sub}");
}

fn render_queue(queue: &[QueueTask], sty: &Style, w: &mut dyn Write, width: usize) {
    let _ = writeln!(w);
    let _ = writeln!(w, "{}", sty.section_rule("QUEUE", width));
    if queue.is_empty() {
        let _ = writeln!(w, "  {}", sty.dim("(empty)"));
        return;
    }
    let _ = writeln!(
        w,
        "  {:<6} {:<24} {:<8} {:<4} {:>3}  PR",
        "TASK", "WHAT", "MODEL", "EFF", "PRI"
    );
    for q in queue {
        let (model, effort) = split_tier_eff(Some(&q.tier_eff));
        let pr_str =
            q.pr.map(|p| format!("#{p}"))
                .unwrap_or_else(|| "—".to_string());
        let _ = writeln!(
            w,
            "  #{:<5} {:<24} {:<8} {:<4} {:>3}  {}",
            q.id,
            truncate(&q.title, 24),
            model,
            effort,
            q.priority,
            pr_str,
        );
    }
}

fn render_blocked(blocked: &[BlockedTask], sty: &Style, w: &mut dyn Write, width: usize) {
    let _ = writeln!(w);
    let _ = writeln!(w, "{}", sty.section_rule("BLOCKED", width));
    if blocked.is_empty() {
        let _ = writeln!(w, "  {}", sty.dim("(none)"));
        return;
    }
    let _ = writeln!(
        w,
        "  {:<6} {:<24} {:<8} {:<4} BLOCKER",
        "TASK", "WHAT", "MODEL", "EFF"
    );
    for b in blocked {
        let (model, effort) = split_tier_eff(Some(&b.tier_eff));
        let dep = b
            .waiting_on
            .first()
            .map(|d| format!("#{d}"))
            .unwrap_or_else(|| "?".to_string());
        let is_deadlocked = !b.deadlocked_on.is_empty();
        let block_icon = if is_deadlocked {
            if sty.color {
                "💀"
            } else {
                "[DEADLOCK]"
            }
        } else {
            if sty.color {
                "⛔"
            } else {
                "[blocked]"
            }
        };
        let suffix = if is_deadlocked {
            let dead_ids: Vec<String> = b.deadlocked_on.iter().map(|d| format!("#{d}")).collect();
            format!(" (CANCELLED — will never unblock: {})", dead_ids.join(", "))
        } else {
            String::new()
        };
        let _ = writeln!(
            w,
            "  #{:<5} {:<24} {:<8} {:<4} {} waits on {}{}",
            b.id,
            truncate(&b.title, 24),
            model,
            effort,
            block_icon,
            dep,
            suffix,
        );
    }
}

fn render_pipeline(
    pipeline: &[PipelineTask],
    daemon_agents: &[DaemonAgentView],
    sty: &Style,
    w: &mut dyn Write,
    width: usize,
) {
    let _ = writeln!(w);
    let rule_suffix = if sty.color {
        sty.dim("  task → PR → state")
    } else {
        "  task -> PR -> state".to_string()
    };
    let _ = writeln!(
        w,
        "{}{}",
        sty.section_rule("PIPELINE", width - 20),
        rule_suffix,
    );
    if pipeline.is_empty() {
        let _ = writeln!(w, "  {}", sty.dim("(no tasks)"));
        return;
    }
    for p in pipeline {
        let in_review = daemon_agents
            .iter()
            .any(|d| d.role == "reviewer" && d.task_id == Some(p.id));

        let (icon, label) = pipeline_state(p, in_review, sty);
        let pr_str =
            p.pr.map(|pr| format!("#{pr}"))
                .unwrap_or_else(|| "—".to_string());
        let _ = writeln!(
            w,
            "  #{:<4} {:<24} {} {:<14} {}",
            p.id,
            truncate(&p.title, 24),
            icon,
            label,
            pr_str,
        );
    }
}

fn pipeline_state(p: &PipelineTask, in_review: bool, sty: &Style) -> (String, String) {
    if p.status == "done" {
        let icon = if sty.color {
            sty.green("✅")
        } else {
            "[merged]".to_string()
        };
        return (icon, "merged".to_string());
    }
    if in_review {
        let icon = if sty.color {
            "🔍".to_string()
        } else {
            "[review]".to_string()
        };
        return (icon, "in review".to_string());
    }
    let icon = if sty.color {
        "◐".to_string()
    } else {
        format!("[{}]", p.status)
    };
    (icon, p.status.clone())
}

fn render_unbacked_prs(
    unbacked: &[UnbackedPr],
    twins: &[TwinPr],
    sty: &Style,
    w: &mut dyn Write,
    width: usize,
) {
    if unbacked.is_empty() && twins.is_empty() {
        return;
    }
    let _ = writeln!(w);
    let _ = writeln!(w, "{}", sty.section_rule("UNBACKED PRS", width));
    for u in unbacked {
        let warn = if sty.color {
            sty.yellow("⚠")
        } else {
            "[!]".to_string()
        };
        let _ = writeln!(
            w,
            "  {} #{:<5} {:<30} {}",
            warn,
            u.number,
            truncate(&u.title, 30),
            sty.dim(&u.branch),
        );
    }
    for t in twins {
        let warn = if sty.color {
            sty.yellow("⚠")
        } else {
            "[!]".to_string()
        };
        let prs: Vec<String> = t.pr_numbers.iter().map(|n| format!("#{n}")).collect();
        let _ = writeln!(
            w,
            "  {} task #{:<4} twin PRs: {}",
            warn,
            t.task_id,
            prs.join(", "),
        );
    }
}

fn render_alerts(alerts: &[AlertMessage], sty: &Style, w: &mut dyn Write, width: usize) {
    if alerts.is_empty() {
        return;
    }
    let _ = writeln!(w);
    let _ = writeln!(w, "{}", sty.section_rule("ALERTS", width));
    for a in alerts {
        let age = fmt_age(a.age_secs);
        let icon = if a.kind == "critical" {
            if sty.color {
                sty.red("!!")
            } else {
                "!!".to_string()
            }
        } else {
            if sty.color {
                sty.yellow("!")
            } else {
                "!".to_string()
            }
        };
        let refs_str = a
            .refs
            .as_deref()
            .map(|r| format!("  {}", sty.dim(r)))
            .unwrap_or_default();
        let _ = writeln!(
            w,
            "  {} [{:>4} ago] {}{}",
            icon,
            age,
            truncate(&a.body, 55),
            refs_str,
        );
    }
}

fn render_errors(
    errors: &[DedupedError],
    older_silenced: i64,
    sty: &Style,
    w: &mut dyn Write,
    width: usize,
) {
    let _ = writeln!(w);
    let _ = writeln!(w, "{}", sty.section_rule("ERRORS", width));
    if errors.is_empty() && older_silenced == 0 {
        let _ = writeln!(w, "  {}", sty.dim("last 1h: none"));
        return;
    }
    if errors.is_empty() {
        let _ = writeln!(
            w,
            "  {}",
            sty.dim(&format!(
                "last 1h: none · {} older silenced",
                older_silenced
            ))
        );
        return;
    }
    for e in errors {
        let age = fmt_age(e.latest_age_secs);
        let count_str = if e.count > 1 {
            format!(" ×{}", e.count)
        } else {
            String::new()
        };
        let detail = truncate(&e.detail, 50);
        let _ = writeln!(
            w,
            "  [{:>4} ago] {}: {}{}",
            age, e.source, detail, count_str,
        );
    }
    if older_silenced > 0 {
        let _ = writeln!(
            w,
            "  {}",
            sty.dim(&format!("{older_silenced} older silenced"))
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quorum_core::stats::Stats;

    fn default_stats() -> Stats {
        Stats::default()
    }

    #[test]
    fn empty_state_renders_without_crash() {
        let s = default_stats();
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("on track"));
        assert!(output.contains("WORKING"));
        assert!(output.contains("idle"));
        assert!(output.contains("QUEUE"));
        assert!(output.contains("BLOCKED"));
        assert!(output.contains("PIPELINE"));
        assert!(output.contains("ERRORS"));
    }

    #[test]
    fn model_and_effort_are_visible_across_active_queue_and_blocked_work() {
        let mut s = default_stats();
        s.daemon_agents.push(DaemonAgentView {
            agent: "Worker-a1".into(),
            role: "worker".into(),
            sub_role: None,
            task_id: Some(1),
            phase: "working".into(),
            cost_tokens: 0,
            agent_state: None,
            cost_usd: 0.0,
            log_dir: None,
            last_activity_age_secs: Some(1),
            task_title: Some("active task".into()),
            tier_eff: Some("opus48·hi".into()),
            pr: None,
            rework_count: 0,
            tool_count: 0,
            now_label: None,
            events_per_min: None,
            uptime_secs: Some(1),
        });
        s.queue_tasks.push(QueueTask {
            id: 2,
            title: "queued task".into(),
            tier_eff: "opus47·md".into(),
            priority: 10,
            pr: None,
        });
        s.blocked.push(BlockedTask {
            id: 3,
            title: "blocked task".into(),
            tier_eff: "opus46·hi".into(),
            waiting_on: vec![1],
            deadlocked_on: vec![],
        });

        let mut buf = Vec::new();
        render_with_style(&s, &Style::plain(), &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("MODEL"), "missing MODEL header: {output}");
        for (title, model, effort) in [
            ("active task", "opus48", "hi"),
            ("queued task", "opus47", "md"),
            ("blocked task", "opus46", "hi"),
        ] {
            assert!(
                output.lines().any(|line| line.contains(title)
                    && line.contains(model)
                    && line.contains(effort)),
                "{title} model/effort missing: {output}"
            );
        }
    }

    #[test]
    fn no_color_emits_no_ansi_codes() {
        let s = default_stats();
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(
            !output.contains('\x1b'),
            "NO_COLOR output must not contain ANSI escape codes, got: {output}"
        );
    }

    #[test]
    fn color_output_contains_ansi() {
        let s = default_stats();
        let sty = Style { color: true };
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(
            output.contains('\x1b'),
            "Color output must contain ANSI escape codes"
        );
    }

    #[test]
    fn health_verdict_stalled_when_dead_agent() {
        let mut s = default_stats();
        s.daemon_agents.push(DaemonAgentView {
            agent: "W1".into(),
            role: "worker".into(),
            sub_role: None,
            task_id: Some(1),
            phase: "working".into(),
            cost_tokens: 100,
            agent_state: None,
            cost_usd: 0.01,
            log_dir: None,
            last_activity_age_secs: Some(200),
            task_title: Some("test".into()),
            tier_eff: Some("opus46·hi".into()),
            pr: None,
            rework_count: 0,
            tool_count: 0,
            now_label: None,
            events_per_min: None,
            uptime_secs: None,
        });
        s.health = HealthVerdict::Stalled;
        s.stalled_count = 1;
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("stalled"));
    }

    #[test]
    fn freshness_dot_plain_mode() {
        let sty = Style::plain();
        assert_eq!(sty.freshness_dot(Some(5)), "*");
        assert_eq!(sty.freshness_dot(Some(60)), "~");
        assert_eq!(sty.freshness_dot(Some(200)), "!");
        assert_eq!(sty.freshness_dot(None), "!");
    }

    #[test]
    fn error_dedup_renders_count() {
        let errors = vec![DedupedError {
            detail: "names pool exhausted".into(),
            source: "serve".into(),
            count: 4,
            latest_age_secs: 120,
        }];
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_errors(&errors, 3, &sty, &mut buf, 78);
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("×4"), "should show dedup count: {output}");
        assert!(
            output.contains("3 older silenced"),
            "should show silenced count: {output}"
        );
    }

    #[test]
    fn truncate_long_strings() {
        assert_eq!(truncate("hello", 10), "hello");
        assert_eq!(truncate("hello world!", 5), "hell…");
    }

    #[test]
    fn pipeline_state_icons_plain() {
        let sty = Style::plain();
        let done = PipelineTask {
            id: 1,
            title: "t".into(),
            status: "done".into(),
            pr: Some(42),
            blocked: false,
        };
        let (icon, label) = pipeline_state(&done, false, &sty);
        assert_eq!(icon, "[merged]");
        assert_eq!(label, "merged");

        let in_review = PipelineTask {
            id: 4,
            title: "t".into(),
            status: "working".into(),
            pr: Some(99),
            blocked: false,
        };
        let (icon, _) = pipeline_state(&in_review, true, &sty);
        assert_eq!(icon, "[review]");
    }

    #[test]
    fn render_working_shows_reviewer_subrow() {
        let mut s = default_stats();
        s.daemon_agents.push(DaemonAgentView {
            agent: "W1".into(),
            role: "worker".into(),
            sub_role: None,
            task_id: Some(10),
            phase: "review".into(),
            cost_tokens: 40000,
            agent_state: None,
            cost_usd: 0.71,
            log_dir: None,
            last_activity_age_secs: Some(3),
            task_title: Some("parks reopenable".into()),
            tier_eff: Some("opus46·md".into()),
            pr: Some(193),
            rework_count: 0,
            tool_count: 27,
            now_label: Some("Bash: cargo test".into()),
            events_per_min: Some(14.0),
            uptime_secs: Some(240),
        });
        s.daemon_agents.push(DaemonAgentView {
            agent: "R1".into(),
            role: "reviewer".into(),
            sub_role: None,
            task_id: Some(10),
            phase: "reviewing".into(),
            cost_tokens: 12000,
            agent_state: None,
            cost_usd: 0.15,
            log_dir: None,
            last_activity_age_secs: Some(120),
            task_title: None,
            tier_eff: None,
            pr: None,
            rework_count: 0,
            tool_count: 5,
            now_label: None,
            events_per_min: Some(2.0),
            uptime_secs: Some(120),
        });
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("W1"), "worker should appear: {output}");
        assert!(
            output.contains("reviewer"),
            "reviewer subrow should appear: {output}"
        );
        assert!(
            output.contains("R1"),
            "reviewer name should appear: {output}"
        );

        // Header "EFF" column must align with data "EFF" column
        let header_line = output
            .lines()
            .find(|l| l.contains("EFF") && l.contains("TASK"))
            .unwrap();
        let data_line = output.lines().find(|l| l.contains("W1")).unwrap();
        let header_eff_col = header_line.find("EFF").unwrap();
        let data_eff_col = data_line.find("md").unwrap(); // "md" = eff value from "opus46·md"
        assert_eq!(
            header_eff_col, data_eff_col,
            "EFF header col ({header_eff_col}) must match data col ({data_eff_col})\nheader: {header_line}\n  data: {data_line}"
        );
    }

    #[test]
    fn render_unbacked_prs_section() {
        let mut s = default_stats();
        s.unbacked_prs.push(UnbackedPr {
            number: 267,
            title: "stale superseded PR".into(),
            branch: "daemon/feat-old".into(),
        });
        s.twin_prs.push(TwinPr {
            task_id: 42,
            pr_numbers: vec![259, 269],
        });
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(
            output.contains("UNBACKED PRS"),
            "section header should appear: {output}"
        );
        assert!(
            output.contains("#267"),
            "unbacked PR number should appear: {output}"
        );
        assert!(
            output.contains("stale superseded PR"),
            "unbacked PR title should appear: {output}"
        );
        assert!(
            output.contains("daemon/feat-old"),
            "unbacked PR branch should appear: {output}"
        );
        assert!(
            output.contains("task #42"),
            "twin task id should appear: {output}"
        );
        assert!(
            output.contains("#259, #269"),
            "twin PR numbers should appear: {output}"
        );
    }

    #[test]
    fn orphan_reviewer_renders_as_working() {
        let mut s = default_stats();
        s.daemon_agents.push(DaemonAgentView {
            agent: "R-solo".into(),
            role: "reviewer".into(),
            sub_role: None,
            task_id: Some(50),
            phase: "reviewing".into(),
            cost_tokens: 8000,
            agent_state: None,
            cost_usd: 0.10,
            log_dir: None,
            last_activity_age_secs: Some(5),
            task_title: Some("review PR #3610".into()),
            tier_eff: Some("opus46·hi".into()),
            pr: None,
            rework_count: 0,
            tool_count: 12,
            now_label: Some("Read: src/main.rs".into()),
            events_per_min: Some(6.0),
            uptime_secs: Some(180),
        });
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(
            !output.contains("no agents working"),
            "orphan reviewer must not show idle: {output}"
        );
        assert!(
            output.contains("R-solo"),
            "orphan reviewer agent name must appear: {output}"
        );
        assert!(
            output.contains("#50"),
            "orphan reviewer task id must appear: {output}"
        );
        assert!(
            output.contains("1 working"),
            "header must count orphan reviewer: {output}"
        );
    }

    #[test]
    fn r2_reviewer_shows_marker() {
        let mut s = default_stats();
        // Active R2 pre-merge reviewer (sampled at R1 approval).
        s.daemon_agents.push(DaemonAgentView {
            agent: "Keel-8z3a".into(),
            role: "reviewer".into(),
            sub_role: Some("r2".into()),
            task_id: Some(85),
            phase: "reviewing".into(),
            cost_tokens: 5000,
            agent_state: None,
            cost_usd: 0.06,
            log_dir: None,
            last_activity_age_secs: Some(10),
            task_title: Some("merged task".into()),
            tier_eff: Some("opus46·hi".into()),
            pr: Some(3667),
            rework_count: 0,
            tool_count: 3,
            now_label: None,
            events_per_min: Some(4.0),
            uptime_secs: Some(60),
        });
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(
            output.contains("(r2)"),
            "R2 reviewer must show (r2) marker: {output}"
        );
        assert!(
            output.contains("Keel-8z3a"),
            "R2 reviewer name must appear: {output}"
        );
    }

    #[test]
    fn r2_reviewer_subrow_shows_r2_audit_label() {
        let mut s = default_stats();
        s.daemon_agents.push(DaemonAgentView {
            agent: "W1".into(),
            role: "worker".into(),
            sub_role: None,
            task_id: Some(10),
            phase: "working".into(),
            cost_tokens: 1000,
            agent_state: None,
            cost_usd: 0.01,
            log_dir: None,
            last_activity_age_secs: Some(5),
            task_title: Some("task".into()),
            tier_eff: None,
            pr: None,
            rework_count: 0,
            tool_count: 0,
            now_label: None,
            events_per_min: None,
            uptime_secs: None,
        });
        s.daemon_agents.push(DaemonAgentView {
            agent: "R2-aud".into(),
            role: "reviewer".into(),
            sub_role: Some("r2".into()),
            task_id: Some(10),
            phase: "reviewing".into(),
            cost_tokens: 2000,
            agent_state: None,
            cost_usd: 0.02,
            log_dir: None,
            last_activity_age_secs: Some(3),
            task_title: None,
            tier_eff: None,
            pr: None,
            rework_count: 0,
            tool_count: 1,
            now_label: None,
            events_per_min: None,
            uptime_secs: Some(30),
        });
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(
            output.contains("r2 audit"),
            "R2 reviewer subrow must show 'r2 audit' label: {output}"
        );
    }

    #[test]
    fn unbacked_prs_section_hidden_when_empty() {
        let s = default_stats();
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(
            !output.contains("UNBACKED"),
            "section should be hidden when no unbacked PRs: {output}"
        );
    }

    #[test]
    fn alerts_section_renders_when_present() {
        use quorum_core::stats::AlertMessage;
        let mut s = default_stats();
        s.alerts.push(AlertMessage {
            body: "task #42: rework cap exceeded".into(),
            refs: Some("task:42".into()),
            age_secs: 120,
            kind: "alert".into(),
        });
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(
            output.contains("ALERTS"),
            "ALERTS section should appear: {output}"
        );
        assert!(
            output.contains("rework cap exceeded"),
            "alert body should appear: {output}"
        );
        assert!(
            output.contains("task:42"),
            "alert refs should appear: {output}"
        );
    }

    #[test]
    fn alerts_section_hidden_when_empty() {
        let s = default_stats();
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(
            !output.contains("ALERTS"),
            "ALERTS section should be hidden when no alerts: {output}"
        );
    }

    #[test]
    fn critical_alert_shows_double_bang() {
        use quorum_core::stats::AlertMessage;
        let mut s = default_stats();
        s.alerts.push(AlertMessage {
            body: "task #99: provision failure".into(),
            refs: None,
            age_secs: 30,
            kind: "critical".into(),
        });
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(
            output.contains("!!"),
            "critical alert should show !!: {output}"
        );
    }

    #[test]
    fn daemon_none_renders() {
        let s = default_stats();
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(
            output.contains("daemon: none"),
            "expected 'daemon: none': {output}"
        );
    }

    #[test]
    fn daemon_alive_renders() {
        let mut s = default_stats();
        s.daemon = DaemonLiveness::Alive {
            pid: 12345,
            heartbeat_age_secs: 4,
        };
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(
            output.contains("daemon: pid 12345 alive, heartbeat 4s ago"),
            "expected alive line: {output}"
        );
    }

    #[test]
    fn daemon_stale_renders() {
        let mut s = default_stats();
        s.daemon = DaemonLiveness::Stale {
            pid: 99999,
            heartbeat_age_secs: 720,
            pid_dead: true,
        };
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(
            output.contains("daemon: STALE")
                && output.contains("12m ago")
                && output.contains("pid 99999 dead"),
            "expected stale line: {output}"
        );
    }
}
