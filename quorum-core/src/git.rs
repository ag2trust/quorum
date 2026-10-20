//! Shared Git process construction.

use std::ffi::OsStr;
use std::process::Command;

/// Construct a Git command with filesystem monitoring disabled.
///
/// A repository-local fsmonitor daemon can be hung or misconfigured. Disabling
/// it for Quorum-owned Git probes keeps daemon and CLI operations bounded.
pub fn git_command(bin: impl AsRef<OsStr>) -> Command {
    let mut command = Command::new(bin);
    command.args(["-c", "core.fsmonitor=false"]);
    command
}

#[cfg(test)]
mod tests {
    use super::git_command;
    use std::path::{Path, PathBuf};

    fn production_sources(dir: &Path, sources: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|name| name == "tests") {
                    continue;
                }
                production_sources(&path, sources);
            } else if path.extension().is_some_and(|extension| extension == "rs")
                && path.file_name().is_none_or(|name| name != "tests.rs")
            {
                sources.push(path);
            }
        }
    }

    #[test]
    fn disables_fsmonitor_before_git_subcommands() {
        let command = git_command("git");
        assert_eq!(
            command.get_args().collect::<Vec<_>>(),
            ["-c", "core.fsmonitor=false"]
        );
    }

    #[test]
    fn production_git_commands_use_the_shared_constructor() {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let mut sources = Vec::new();
        production_sources(&workspace.join("quorum-core/src"), &mut sources);
        production_sources(&workspace.join("quorum/src"), &mut sources);

        for source_path in sources {
            let source = std::fs::read_to_string(&source_path).unwrap();
            // Unit-test modules intentionally construct their own fixture Git
            // commands. Production source precedes the module in this project.
            let production = source.split("#[cfg(test)]\nmod tests {").next().unwrap();
            for bypass in [
                "Command::new(\"git\")",
                "Command::new(git_bin)",
                "Command::new(&git_bin)",
            ] {
                assert!(
                    !production.contains(bypass),
                    "production Git command bypasses git_command in {}: {bypass}",
                    source_path.display()
                );
            }
        }
    }
}
