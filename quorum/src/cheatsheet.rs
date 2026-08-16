//! Short workflow orientation. Exact flags belong to each command's clap help.

pub fn cheatsheet() -> &'static str {
    r#"quorum — local, daemon-managed coding agents

COORDINATOR
  Create one of three task kinds:

  quorum task-create --created-by <id> --title <outcome> --body-stdin
      New implementation from the configured base branch.

  quorum task-create --created-by <id> --title <outcome> --continue-pr <N> --body-stdin
      Continue implementation from the exact current head of PR #N.

  quorum task-create --created-by <id> --title "Review PR #N" --review-pr <N> --body-stdin
      Review an existing implementation. No worker is available for requested changes.

  --continue-pr and --review-pr are mutually exclusive. Generic refs are metadata, not PR
  authority. The daemon chooses complexity, model, and effort.

  Send only execution-ready tasks. Interactive callers create, inspect, or cancel work;
  they do not claim tasks, impersonate managed agents, submit work, or set tasks to done.

MANAGED AGENTS
  A managed worker or reviewer follows its spawn prompt. It receives its assignment and
  run identity from the daemon; it does not poll or claim work. The prompt supplies the
  exact submit command. Use `quorum react` when blocked, failed, or needing input.

OPERATOR
  quorum serve --help             Start the manager and inspect provider/config flags.
  quorum status [--json]          Inspect daemon and queue health.
  quorum web                      Open the loopback-only dashboard.
  quorum task-list --brief        List the queue without task bodies.
  quorum task-get --task-id <N>   Read one task and its notes.
  quorum log --refs task#N        Read lifecycle events for a task.
  quorum tail <agent>             Read a managed session log.
  quorum kill --help              Terminate a stuck managed agent.
  quorum decomposition-adopt-recovery --help
                                  Adopt one exact proven continuation delivery.

MESSAGES
  `post`/`read` are agent-authored feed messages. `log` is the system lifecycle event
  stream. Put free text in stdin or files, not shell arguments.

DISCOVERY
  quorum <command> --help         Exact flags and command behavior.
  quorum --help                   Public command list.

EXIT CODES
  0 success · 1 expected negative · 2 bad input · 3 internal/database failure
"#
}

#[cfg(test)]
mod tests {
    use super::cheatsheet;

    #[test]
    fn help_teaches_current_entry_modes_and_roles() {
        let help = cheatsheet();
        assert!(help.contains("--continue-pr <N>"));
        assert!(help.contains("--review-pr <N>"));
        assert!(help.contains("MANAGED AGENTS"));
        assert!(help.contains("OPERATOR"));
    }

    #[test]
    fn help_does_not_teach_legacy_agent_workflows() {
        let help = cheatsheet();
        assert!(!help.contains("task-claim"));
        assert!(!help.contains("task-release"));
        assert!(!help.contains("quorum sync"));
        assert!(!help.contains("issue #"));
    }
}
