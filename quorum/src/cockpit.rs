use quorum_core::drift::{TwinPr, UnbackedPr};
use quorum_core::stats::{
    AlertMessage, BlockedTask, DaemonAgentView, DaemonLiveness, DecompositionStatusView,
    DedupedError, HealthVerdict, MergeBlockerView, PipelineTask, QueueTask, ReviewingTask, Stats,
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
    render_with_style_at_width(s, sty, w, 78);
}

fn render_with_style_at_width(s: &Stats, sty: &Style, w: &mut dyn Write, width: usize) {
    render_header(s, sty, w, width);
    render_working(s, sty, w, width);
    render_queue(&s.queue_tasks, sty, w, width);
    render_blocked(&s.blocked, sty, w, width);
    render_reviewing(&s.reviewing, sty, w, width);
    render_pipeline(&s.pipeline, sty, w, width);
    render_decomposition(s.decomposition.as_ref(), sty, w, width);
    render_merge_wait(&s.merge_blockers, sty, w, width);
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

    let working_count = s
        .daemon_agents
        .iter()
        .filter(|d| d.role == "worker")
        .filter_map(|d| d.task_id)
        .collect::<std::collections::HashSet<_>>()
        .len();
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

    if workers.is_empty() {
        let _ = writeln!(w, "  {}", sty.dim("(idle — no agents working)"));
        return;
    }

    let _ = writeln!(
        w,
        "  {:<12}  {:<8} {:<18} {:<8} {:>5}  {:>3}  {:>5}  {:>3}  {:>4}  NOW",
        "AGENT", "PROVIDER", "MODEL", "EFF", "TASK", "UP", "TOK", "T", "EV/m"
    );

    for d in &workers {
        render_agent_row(d, sty, w);
    }
}

fn render_agent_row(d: &DaemonAgentView, sty: &Style, w: &mut dyn Write) {
    let dot = sty.freshness_dot(d.last_activity_age_secs);
    let provider = d.provider.as_deref().unwrap_or("pending");
    let model = d.model.as_deref().unwrap_or("pending");
    let effort = d.effort.as_deref().unwrap_or("pending");
    let task_str = d
        .task_id
        .map(|id| format!("#{id}"))
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
    let error_suffix = format_live_error(d, sty);
    let agent_label = if d.sub_role.as_deref() == Some("r2") {
        format!("{}(r2)", d.agent)
    } else {
        d.agent.clone()
    };

    let _ = writeln!(
        w,
        "{} {:<12}  {:<8} {:<18} {:<8} {:>5}  {:>3}  {:>5}  {:>3}  {:>4}  {}{}{}",
        dot,
        agent_label,
        provider,
        model,
        effort,
        task_str,
        up,
        tok,
        tools,
        evm,
        now_display,
        rework_suffix,
        error_suffix,
    );
    if let Some(title) = d.task_title.as_deref() {
        let _ = writeln!(w, "      {}", truncate(title, 72));
    }
}

fn format_live_error(d: &DaemonAgentView, sty: &Style) -> String {
    if d.live_error_count == 0 {
        return String::new();
    }
    let label = format!(" ERR {}/{MAX_ERROR_RETRIES}", d.live_error_count);
    let text_part = d
        .live_error_text
        .as_deref()
        .map(|t| format!(" · {}", truncate(t, 40)))
        .unwrap_or_default();
    let raw = format!("{label}{text_part}");
    if sty.color {
        format!(" {}", sty.red(&raw))
    } else {
        format!(" [{raw}]")
    }
}

const MAX_ERROR_RETRIES: u32 = 3;

fn render_queue(queue: &[QueueTask], sty: &Style, w: &mut dyn Write, width: usize) {
    let _ = writeln!(w);
    let _ = writeln!(w, "{}", sty.section_rule("QUEUE", width));
    if queue.is_empty() {
        let _ = writeln!(w, "  {}", sty.dim("(empty)"));
        return;
    }
    let _ = writeln!(
        w,
        "  {:<6} {:<8} {:<18} {:<8} {:>3}  PR",
        "TASK", "PROVIDER", "MODEL", "EFF", "PRI"
    );
    for q in queue {
        let pr_str =
            q.pr.map(|p| format!("#{p}"))
                .unwrap_or_else(|| "—".to_string());
        let _ = writeln!(
            w,
            "  #{:<5} {:<8} {:<18} {:<8} {:>3}  {}",
            q.id,
            q.provider.as_deref().unwrap_or("pending"),
            q.model.as_deref().unwrap_or("pending"),
            q.effort.as_deref().unwrap_or("pending"),
            q.priority,
            pr_str,
        );
        let _ = writeln!(w, "      {}", truncate(&q.title, width.saturating_sub(6)));
    }
}

fn render_reviewing(reviewing: &[ReviewingTask], sty: &Style, w: &mut dyn Write, width: usize) {
    if reviewing.is_empty() {
        return;
    }
    let _ = writeln!(w);
    let _ = writeln!(w, "{}", sty.section_rule("REVIEWING", width));
    let _ = writeln!(
        w,
        "  {:<6} {:<36} {:<7} {:<16} STATE",
        "TASK", "TITLE", "PR", "REVIEWER"
    );
    for task in reviewing {
        let pr = task
            .pr
            .map(|pr| format!("#{pr}"))
            .unwrap_or_else(|| "—".to_string());
        let reviewer = task.reviewer.as_deref().unwrap_or("—");
        let _ = writeln!(
            w,
            "  #{:<5} {:<36} {:<7} {:<16} {}",
            task.id,
            truncate(&task.title, 36),
            pr,
            truncate(reviewer, 16),
            task.state,
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
        "  {:<6} {:<8} {:<18} {:<8} BLOCKER",
        "TASK", "PROVIDER", "MODEL", "EFF"
    );
    for b in blocked {
        let dep = if b.waiting_on.is_empty() {
            "?".to_string()
        } else {
            b.waiting_on
                .iter()
                .map(|d| format!("#{d}"))
                .collect::<Vec<_>>()
                .join(", ")
        };
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
            "  #{:<5} {:<8} {:<18} {:<8} {} waits on {}{}",
            b.id,
            b.provider.as_deref().unwrap_or("pending"),
            b.model.as_deref().unwrap_or("pending"),
            b.effort.as_deref().unwrap_or("pending"),
            block_icon,
            dep,
            suffix,
        );
        let _ = writeln!(w, "      {}", truncate(&b.title, width.saturating_sub(6)));
    }
}

fn render_pipeline(pipeline: &[PipelineTask], sty: &Style, w: &mut dyn Write, width: usize) {
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
    let _ = writeln!(
        w,
        "  {:<6} {:<8} {:<18} {:<8} PR  STATE",
        "TASK", "PROVIDER", "MODEL", "EFF"
    );
    for p in pipeline {
        let (icon, label) = pipeline_state(p, sty);
        let pr_str =
            p.pr.map(|pr| format!("#{pr}"))
                .unwrap_or_else(|| "—".to_string());
        let _ = writeln!(
            w,
            "  #{:<5} {:<8} {:<18} {:<8} {:<3} {} {}",
            p.id,
            p.provider.as_deref().unwrap_or("pending"),
            p.model.as_deref().unwrap_or("pending"),
            p.effort.as_deref().unwrap_or("pending"),
            pr_str,
            icon,
            label,
        );
        let _ = writeln!(w, "      {}", truncate(&p.title, width.saturating_sub(6)));
    }
}

fn render_decomposition(
    graph: Option<&DecompositionStatusView>,
    sty: &Style,
    w: &mut dyn Write,
    width: usize,
) {
    let Some(graph) = graph else {
        return;
    };
    let _ = writeln!(w);
    let _ = writeln!(w, "{}", sty.section_rule("DECOMPOSITION", width));
    let planner = match (&graph.planner_provider, &graph.planner_model) {
        (Some(provider), Some(model)) => format!("{provider}/{model}"),
        (Some(provider), None) => provider.clone(),
        _ => "pending".to_string(),
    };
    let revision = graph
        .accepted_plan_revision
        .map(|revision| revision.to_string())
        .unwrap_or_else(|| "—".to_string());
    let _ = writeln!(
        w,
        "  source #{:<5} state={:<14} children={}/{} planner={} rev={}",
        graph.source_task_id,
        graph.graph_state,
        graph.completed_children,
        graph.total_children,
        planner,
        revision,
    );
    let _ = writeln!(
        w,
        "      {}",
        truncate(&graph.source_title, width.saturating_sub(6))
    );
    let _ = writeln!(
        w,
        "      attempts proposal={}/3 provider={}/3",
        graph.proposal_attempts, graph.provider_failures
    );
    for member in &graph.members {
        let prerequisites = if member.prerequisites.is_empty() {
            "—".to_string()
        } else {
            member
                .prerequisites
                .iter()
                .map(|id| format!("#{id}"))
                .collect::<Vec<_>>()
                .join(",")
        };
        let _ = writeln!(
            w,
            "      #{:<5} {:<12} {:<12} deps {}",
            member.task_id, member.local_key, member.status, prerequisites
        );
    }
    for reason in &graph.reasons {
        let _ = writeln!(
            w,
            "      blocker: {}",
            truncate(reason, width.saturating_sub(15))
        );
    }
}

fn pipeline_state(p: &PipelineTask, sty: &Style) -> (String, String) {
    if p.status == "done" {
        let icon = if sty.color {
            sty.green("✅")
        } else {
            "[merged]".to_string()
        };
        return (icon, "merged".to_string());
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

fn render_merge_wait(blockers: &[MergeBlockerView], sty: &Style, w: &mut dyn Write, width: usize) {
    if blockers.is_empty() {
        return;
    }
    let _ = writeln!(w);
    let _ = writeln!(w, "{}", sty.section_rule("MERGE WAIT", width));
    let _ = writeln!(
        w,
        "  {:<6} {:<24} {:3} {:<10} {:>4} {:>4}  PR",
        "TASK", "WHAT", "", "BLOCKER", "WAIT", "RTR"
    );
    for b in blockers {
        let wait = fmt_age(b.waiting_secs);
        let pr_str =
            b.pr.map(|p| format!("#{p}"))
                .unwrap_or_else(|| "—".to_string());
        let icon = match b.blocker_kind.as_str() {
            "conflict" => {
                if sty.color {
                    sty.yellow("⚠")
                } else {
                    "[!]".to_string()
                }
            }
            _ => {
                if sty.color {
                    "⏳".to_string()
                } else {
                    "[~]".to_string()
                }
            }
        };
        let _ = writeln!(
            w,
            "  #{:<5} {:<24} {:3} {:<10} {:>4} {:>4}  {}",
            b.task_id,
            truncate(&b.title, 24),
            icon,
            b.blocker_kind,
            wait,
            b.retry_count,
            pr_str,
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
    fn decomposition_renders_bounded_graph_details() {
        let mut s = default_stats();
        s.decomposition = Some(DecompositionStatusView {
            graph_id: 1,
            source_task_id: 40,
            source_title: "deliver the requested outcome".into(),
            source_status: "decomposed".into(),
            graph_state: "blocked".into(),
            proposal_attempts: 2,
            provider_failures: 1,
            planner_provider: Some("codex".into()),
            planner_model: Some("gpt-5.6-sol".into()),
            accepted_plan_revision: Some(3),
            completed_children: 1,
            total_children: 2,
            child_statuses: vec![],
            failed_children: vec![42],
            reasons: vec!["child failed review".into()],
            members: vec![quorum_core::stats::DecompositionMemberView {
                task_id: 42,
                local_key: "api".into(),
                title: "build API".into(),
                status: "failed".into(),
                prerequisites: vec![41],
            }],
        });
        let mut buf = Vec::new();
        render_with_style(&s, &Style::plain(), &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("DECOMPOSITION"));
        assert!(output.contains("source #40"));
        assert!(output.contains("children=1/2"));
        assert!(output.contains("proposal=2/3 provider=1/3"));
        assert!(output.contains("#42") && output.contains("deps #41"));
        assert!(output.contains("blocker: child failed review"));
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
            tier_eff: Some("c2".into()),
            provider: Some("codex".into()),
            model: Some("gpt-5.6-terra".into()),
            effort: Some("medium".into()),
            pr: None,
            rework_count: 0,
            tool_count: 0,
            now_label: None,
            events_per_min: None,
            uptime_secs: Some(1),
            live_error_count: 0,
            live_error_text: None,
        });
        s.queue_tasks.push(QueueTask {
            id: 2,
            title: "queued task".into(),
            provider: Some("claude".into()),
            model: Some("claude-opus-4-7".into()),
            effort: Some("medium".into()),
            tier_eff: "opus47·md".into(),
            priority: 10,
            pr: None,
        });
        s.blocked.push(BlockedTask {
            id: 3,
            title: "blocked task".into(),
            provider: Some("claude".into()),
            model: Some("claude-opus-4-6".into()),
            effort: Some("high".into()),
            tier_eff: "opus46·hi".into(),
            waiting_on: vec![1],
            deadlocked_on: vec![],
        });

        let mut buf = Vec::new();
        render_with_style(&s, &Style::plain(), &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("MODEL"), "missing MODEL header: {output}");
        for (title, provider, model, effort) in [
            ("active task", "codex", "gpt-5.6-terra", "medium"),
            ("queued task", "claude", "claude-opus-4-7", "medium"),
            ("blocked task", "claude", "claude-opus-4-6", "high"),
        ] {
            assert!(
                output.contains(title)
                    && output.contains(provider)
                    && output.contains(model)
                    && output.contains(effort),
                "{title} model/effort missing: {output}"
            );
        }
        assert!(
            !output.contains("WHAT"),
            "legacy WHAT header remains: {output}"
        );
        assert!(
            output
                .lines()
                .all(|line| !line.contains("gpt-5.6-terra") || !line.contains("c2")),
            "complexity leaked into model: {output}"
        );
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
            provider: None,
            model: None,
            effort: None,
            pr: None,
            rework_count: 0,
            tool_count: 0,
            now_label: None,
            events_per_min: None,
            uptime_secs: None,
            live_error_count: 0,
            live_error_text: None,
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
            provider: None,
            model: None,
            effort: None,
            status: "done".into(),
            pr: Some(42),
            blocked: false,
        };
        let (icon, label) = pipeline_state(&done, &sty);
        assert_eq!(icon, "[merged]");
        assert_eq!(label, "merged");

        let pending = PipelineTask {
            id: 4,
            title: "t".into(),
            provider: None,
            model: None,
            effort: None,
            status: "working".into(),
            pr: Some(99),
            blocked: false,
        };
        let (icon, _) = pipeline_state(&pending, &sty);
        assert_eq!(icon, "[working]");
    }

    #[test]
    fn render_reviewing_shows_live_reviewer_outside_working() {
        let mut s = default_stats();
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
            provider: None,
            model: None,
            effort: None,
            pr: None,
            rework_count: 0,
            tool_count: 5,
            now_label: None,
            events_per_min: Some(2.0),
            uptime_secs: Some(120),
            live_error_count: 0,
            live_error_text: None,
        });
        s.reviewing.push(ReviewingTask {
            id: 10,
            title: "parks reopenable".into(),
            pr: Some(193),
            reviewer: Some("R1".into()),
            state: "reviewing".into(),
        });
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(
            output.contains("REVIEWING"),
            "section should appear: {output}"
        );
        assert!(
            output.contains("R1"),
            "reviewer name should appear in REVIEWING: {output}"
        );
        assert_eq!(
            output.matches("R1").count(),
            1,
            "reviewer must not also appear in WORKING: {output}"
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
    fn reviewer_does_not_count_as_working() {
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
            provider: None,
            model: None,
            effort: None,
            pr: None,
            rework_count: 0,
            tool_count: 12,
            now_label: Some("Read: src/main.rs".into()),
            events_per_min: Some(6.0),
            uptime_secs: Some(180),
            live_error_count: 0,
            live_error_text: None,
        });
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(
            output.contains("no agents working"),
            "WORKING should be idle: {output}"
        );
        assert!(
            output.contains("0 working"),
            "header must not count reviewer as working: {output}"
        );
    }

    #[test]
    fn reviewing_section_is_suppressed_when_empty() {
        let mut s = default_stats();
        s.queue_tasks.push(QueueTask {
            id: 85,
            title: "queued task".into(),
            provider: None,
            model: None,
            effort: None,
            tier_eff: "pending".into(),
            priority: 0,
            pr: None,
        });
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(
            !output.contains("REVIEWING"),
            "section should be hidden: {output}"
        );
    }

    #[test]
    fn reviewing_section_renders_merging_task() {
        let mut s = default_stats();
        s.reviewing.push(ReviewingTask {
            id: 10,
            title: "merge approved PR".into(),
            pr: Some(400),
            reviewer: None,
            state: "merging".into(),
        });
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(
            output.contains("merging"),
            "merging state must render: {output}"
        );
    }

    // ── #177: MERGE WAIT section ────────────────────────────────────

    #[test]
    fn merge_wait_section_renders_when_present() {
        let mut s = default_stats();
        s.merge_blockers.push(MergeBlockerView {
            task_id: 42,
            title: "review PR #367".into(),
            pr: Some(367),
            blocker_kind: "conflict".into(),
            status: "in-review".into(),
            waiting_secs: 1800,
            retry_count: 1,
        });
        s.merge_blockers.push(MergeBlockerView {
            task_id: 50,
            title: "merge approved PR".into(),
            pr: Some(400),
            blocker_kind: "ci_pending".into(),
            status: "merging".into(),
            waiting_secs: 120,
            retry_count: 0,
        });
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(
            output.contains("MERGE WAIT"),
            "section header must appear: {output}"
        );
        assert!(
            output.contains("#42") && output.contains("conflict"),
            "conflict blocker visible: {output}"
        );
        assert!(
            output.contains("#50") && output.contains("ci_pending"),
            "ci_pending blocker visible: {output}"
        );
        assert!(output.contains("#367"), "PR number visible: {output}");
        assert!(output.contains("#400"), "PR number visible: {output}");
    }

    #[test]
    fn merge_wait_section_hidden_when_empty() {
        let s = default_stats();
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(
            !output.contains("MERGE WAIT"),
            "section hidden when no merge blockers: {output}"
        );
    }

    #[test]
    fn merge_wait_conflict_shows_warning_icon_plain() {
        let mut s = default_stats();
        s.merge_blockers.push(MergeBlockerView {
            task_id: 10,
            title: "conflict task".into(),
            pr: Some(100),
            blocker_kind: "conflict".into(),
            status: "in-review".into(),
            waiting_secs: 600,
            retry_count: 2,
        });
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        let line = output.lines().find(|l| l.contains("#10")).unwrap();
        assert!(
            line.contains("[!]"),
            "conflict should show warning icon in plain mode: {line}"
        );
    }

    #[test]
    fn merge_wait_columns_align() {
        let mut s = default_stats();
        s.merge_blockers.push(MergeBlockerView {
            task_id: 7,
            title: "short".into(),
            pr: Some(99),
            blocker_kind: "conflict".into(),
            status: "in-review".into(),
            waiting_secs: 60,
            retry_count: 3,
        });
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        let header = output.lines().find(|l| l.contains("BLOCKER")).unwrap();
        let data = output.lines().find(|l| l.contains("#7")).unwrap();
        let hdr_blocker = header.find("BLOCKER").unwrap();
        let data_blocker = data.find("conflict").unwrap();
        assert_eq!(
            hdr_blocker, data_blocker,
            "BLOCKER header must align with data column.\nheader: {header}\ndata:   {data}"
        );
        let hdr_wait = header.find("WAIT").unwrap();
        let hdr_rtr = header.find("RTR").unwrap();
        let hdr_pr = header.find("PR").unwrap();
        let data_pr = data.find("#99").unwrap();
        assert!(
            data_pr >= hdr_pr,
            "PR data must align with or follow PR header.\nheader: {header}\ndata:   {data}"
        );
        assert!(
            hdr_rtr > hdr_wait,
            "RTR column must be after WAIT column in header"
        );
    }

    #[test]
    fn blocked_shows_all_dependency_ids() {
        let mut s = default_stats();
        s.blocked.push(BlockedTask {
            id: 10,
            title: "multi-dep task".into(),
            tier_eff: "opus46·hi".into(),
            provider: None,
            model: None,
            effort: None,
            waiting_on: vec![4, 6, 9],
            deadlocked_on: vec![],
        });
        let mut buf = Vec::new();
        render_with_style(&s, &Style::plain(), &mut buf);
        let output = String::from_utf8(buf).unwrap();
        let line = output.lines().find(|l| l.contains("#10")).unwrap();
        assert!(
            line.contains("#4") && line.contains("#6") && line.contains("#9"),
            "all dep ids must appear: {line}"
        );
        assert!(
            line.contains("#4, #6, #9"),
            "dep ids must be comma-separated: {line}"
        );
    }

    #[test]
    fn blocked_mixed_live_and_cancelled_deps() {
        let mut s = default_stats();
        s.blocked.push(BlockedTask {
            id: 20,
            title: "mixed-dep task".into(),
            tier_eff: "opus46·md".into(),
            provider: None,
            model: None,
            effort: None,
            waiting_on: vec![3, 5, 7],
            deadlocked_on: vec![5],
        });
        let mut buf = Vec::new();
        render_with_style(&s, &Style::plain(), &mut buf);
        let output = String::from_utf8(buf).unwrap();
        let line = output.lines().find(|l| l.contains("#20")).unwrap();
        assert!(
            line.contains("#3") && line.contains("#5") && line.contains("#7"),
            "all blocker ids must appear in waits-on: {line}"
        );
        assert!(
            line.contains("CANCELLED") && line.contains("#5"),
            "cancelled dep detail must appear: {line}"
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

    // ── #182: live provider error rendering ─────────────────────────

    fn make_worker_with_error(error_count: u32, error_text: Option<&str>) -> DaemonAgentView {
        DaemonAgentView {
            agent: "W-err".into(),
            role: "worker".into(),
            sub_role: None,
            task_id: Some(42),
            phase: "working".into(),
            cost_tokens: 500,
            agent_state: None,
            cost_usd: 0.01,
            log_dir: None,
            last_activity_age_secs: Some(2),
            task_title: Some("erroring task".into()),
            tier_eff: Some("opus46·hi".into()),
            provider: None,
            model: None,
            effort: None,
            pr: None,
            rework_count: 0,
            tool_count: 3,
            now_label: None,
            events_per_min: Some(5.0),
            uptime_secs: Some(60),
            live_error_count: error_count,
            live_error_text: error_text.map(|s| s.to_string()),
        }
    }

    #[test]
    fn worker_error_visible_in_plain() {
        let mut s = default_stats();
        s.daemon_agents
            .push(make_worker_with_error(1, Some("session limit")));
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        let line = output.lines().find(|l| l.contains("W-err")).unwrap();
        assert!(line.contains("ERR 1/3"), "error count must appear: {line}");
        assert!(
            line.contains("session limit"),
            "error text must appear: {line}"
        );
    }

    #[test]
    fn worker_no_error_no_marker() {
        let mut s = default_stats();
        s.daemon_agents.push(make_worker_with_error(0, None));
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        let line = output.lines().find(|l| l.contains("W-err")).unwrap();
        assert!(
            !line.contains("ERR"),
            "no error marker when error_count=0: {line}"
        );
    }

    #[test]
    fn reviewer_is_rendered_in_reviewing_not_working() {
        let mut s = default_stats();
        s.daemon_agents.push(DaemonAgentView {
            agent: "R-err".into(),
            role: "reviewer".into(),
            sub_role: None,
            task_id: Some(10),
            phase: "reviewing".into(),
            cost_tokens: 200,
            agent_state: None,
            cost_usd: 0.005,
            log_dir: None,
            last_activity_age_secs: Some(3),
            task_title: None,
            tier_eff: None,
            provider: None,
            model: None,
            effort: None,
            pr: None,
            rework_count: 0,
            tool_count: 1,
            now_label: None,
            events_per_min: None,
            uptime_secs: Some(30),
            live_error_count: 2,
            live_error_text: Some("rate limited".into()),
        });
        s.reviewing.push(ReviewingTask {
            id: 10,
            title: "task".into(),
            pr: None,
            reviewer: Some("R-err".into()),
            state: "reviewing".into(),
        });
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        let rev_line = output.lines().find(|l| l.contains("R-err")).unwrap();
        assert!(
            rev_line.contains("reviewing"),
            "reviewer must appear in REVIEWING: {rev_line}"
        );
        assert!(
            output.contains("no agents working"),
            "reviewer must not keep WORKING active: {output}"
        );
    }

    #[test]
    fn live_error_triggers_attention_health() {
        let mut s = default_stats();
        s.daemon_agents
            .push(make_worker_with_error(1, Some("provider error")));
        s.health = HealthVerdict::Attention;
        let sty = Style::plain();
        let mut buf = Vec::new();
        render_with_style(&s, &sty, &mut buf);
        let output = String::from_utf8(buf).unwrap();
        assert!(
            output.contains("attention"),
            "health must show attention with live error: {output}"
        );
    }
}
