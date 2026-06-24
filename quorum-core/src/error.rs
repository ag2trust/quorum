//! Error type with a stable mapping to process exit codes.
//!
//! A *clean lost race* (claim already held, task already taken) is NOT an error — it is a
//! successful `Ok(...)` outcome that the CLI maps to exit 1 without logging. Only genuine
//! failures flow through [`QuorumError`].

#[derive(thiserror::Error, Debug)]
pub enum QuorumError {
    /// Caller is not the current active holder/assignee. Exit 1 (expected, not logged).
    #[error("not the current holder")]
    NotHolder,
    /// Bad usage / arguments. Exit 2.
    #[error("usage: {0}")]
    Usage(String),
    /// Bad input payload (invalid UTF-8, embedded NUL). Exit 2.
    #[error("bad input: {0}")]
    BadInput(String),
    /// Genuine `SQLITE_BUSY` after the busy_timeout elapsed. Exit 3.
    #[error("database busy after timeout")]
    Busy,
    /// On-disk schema is newer than this binary understands. Exit 3.
    #[error("db schema version {db} is newer than this binary ({bin})")]
    SchemaTooNew { db: i64, bin: i64 },
    /// Underlying SQLite error. Exit 3.
    #[error(transparent)]
    Db(#[from] rusqlite::Error),
    /// I/O failure (stdin/file/fs). Exit 3.
    #[error("io: {0}")]
    Io(String),
}

impl QuorumError {
    /// Stable process exit code. Agents branch on this without parsing JSON.
    pub fn exit_code(&self) -> i32 {
        match self {
            QuorumError::NotHolder => 1,
            QuorumError::Usage(_) | QuorumError::BadInput(_) => 2,
            _ => 3,
        }
    }
}

pub type Result<T> = std::result::Result<T, QuorumError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes_are_stable() {
        assert_eq!(QuorumError::NotHolder.exit_code(), 1);
        assert_eq!(QuorumError::Usage("x".into()).exit_code(), 2);
        assert_eq!(QuorumError::BadInput("x".into()).exit_code(), 2);
        assert_eq!(QuorumError::Busy.exit_code(), 3);
        assert_eq!(QuorumError::SchemaTooNew { db: 2, bin: 1 }.exit_code(), 3);
        assert_eq!(QuorumError::Io("x".into()).exit_code(), 3);
    }
}
