//! Durable, atomic model-profile allocation for managed role responsibilities.

use crate::db::{begin_immediate, map_sql_err};
use crate::error::{QuorumError, Result};
use rusqlite::{params, Connection, OptionalExtension, ToSql, Transaction};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

const BAG_SLOTS: usize = 100;

/// Marker replaced by [`guarded_evidence_insert`] with the assignment-match
/// predicate. The SQL before this marker must be an `INSERT ... SELECT` without
/// a `WHERE` clause; additional `AND` predicates and clauses such as
/// `ON CONFLICT` may follow the marker.
pub const EVIDENCE_ASSIGNMENT_GUARD: &str = "/* quorum-role-assignment-guard */";

const EVIDENCE_ASSIGNMENT_ID_PARAM: &str = ":quorum_assignment_id";
const EVIDENCE_TASK_ID_PARAM: &str = ":quorum_assignment_task_id";
const EVIDENCE_RESPONSIBILITY_PARAM: &str = ":quorum_assignment_responsibility";
const EVIDENCE_ROLE_PARAM: &str = ":quorum_assignment_role";
const EVIDENCE_PROVIDER_PARAM: &str = ":quorum_assignment_provider";
const EVIDENCE_RUNNER_PARAM: &str = ":quorum_assignment_runner";
const EVIDENCE_MODEL_PARAM: &str = ":quorum_assignment_model";
const EVIDENCE_EFFORT_PARAM: &str = ":quorum_assignment_effort";

const EVIDENCE_ASSIGNMENT_PREDICATE: &str = "WHERE (
    :quorum_assignment_id IS NULL
    OR EXISTS(
        SELECT 1 FROM role_assignments AS assignment
        WHERE assignment.id=:quorum_assignment_id
          AND assignment.task_id IS :quorum_assignment_task_id
          AND assignment.responsibility_key=:quorum_assignment_responsibility
          AND assignment.role=:quorum_assignment_role
          AND assignment.provider=:quorum_assignment_provider
          AND assignment.runner=:quorum_assignment_runner
          AND assignment.model=:quorum_assignment_model
          AND assignment.effort=:quorum_assignment_effort
    )
)";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelProfile {
    pub id: String,
    pub provider: String,
    pub runner: String,
    pub model: String,
    pub effort: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeightedProfile {
    pub profile: ModelProfile,
    /// Integer percentage. Every pool must total exactly 100.
    pub percent: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedPool {
    pub pool_key: String,
    pub policy_generation: String,
    pub profiles: Vec<WeightedProfile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentRequest {
    pub responsibility_key: String,
    pub task_id: Option<i64>,
    pub pr_number: Option<i64>,
    pub role: String,
    pub review_stage: Option<String>,
    pub complexity: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleAssignment {
    pub id: i64,
    pub responsibility_key: String,
    pub task_id: Option<i64>,
    pub pr_number: Option<i64>,
    pub role: String,
    pub review_stage: Option<String>,
    pub complexity: Option<String>,
    pub profile_id: String,
    pub provider: String,
    pub runner: String,
    pub model: String,
    pub effort: String,
    pub pool_key: String,
    pub policy_generation: String,
    pub created_at: i64,
}

/// Minimum semantic identity required when canonical evidence links to a role
/// assignment.
///
/// `task_id` (compared NULL-safely) and `responsibility_key` jointly identify
/// the assigned responsibility. `role`, `provider`, `runner`, `model`, and
/// `effort` identify the executable profile. Evidence-specific fields such as a
/// PR, review stage, or plan revision belong in the canonical responsibility
/// key; evidence writers may enforce additional path-specific constraints.
/// Historical evidence with a NULL `role_assignment_id` remains valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvidenceAssignmentContext<'a> {
    pub role_assignment_id: Option<i64>,
    pub task_id: Option<i64>,
    pub responsibility_key: &'a str,
    pub role: &'a str,
    pub provider: &'a str,
    pub runner: &'a str,
    pub model: &'a str,
    pub effort: &'a str,
}

impl ValidatedPool {
    pub fn validate(&self) -> Result<()> {
        validate_text("pool key", &self.pool_key)?;
        validate_text("policy generation", &self.policy_generation)?;
        if self.profiles.is_empty() {
            return usage("routing pool is empty");
        }
        let mut ids = HashSet::new();
        let mut total = 0usize;
        for weighted in &self.profiles {
            if weighted.percent == 0 {
                return usage("routing percentages must be positive");
            }
            total += usize::from(weighted.percent);
            let profile = &weighted.profile;
            for (label, value) in [
                ("profile id", profile.id.as_str()),
                ("provider", profile.provider.as_str()),
                ("runner", profile.runner.as_str()),
                ("model", profile.model.as_str()),
                ("effort", profile.effort.as_str()),
            ] {
                validate_text(label, value)?;
            }
            if !ids.insert(profile.id.as_str()) {
                return usage("routing pool contains a duplicate profile");
            }
        }
        if total != BAG_SLOTS {
            return usage("routing percentages must total exactly 100");
        }
        Ok(())
    }
}

impl AssignmentRequest {
    fn validate(&self) -> Result<()> {
        validate_text("responsibility key", &self.responsibility_key)?;
        if !matches!(
            self.role.as_str(),
            "classifier" | "planner" | "arbiter" | "worker" | "reviewer" | "collector"
        ) {
            return usage("invalid managed role");
        }
        match (self.role.as_str(), self.review_stage.as_deref()) {
            ("reviewer", Some("r1" | "r2")) => {}
            ("reviewer", _) => return usage("reviewer assignment requires r1 or r2 stage"),
            (_, None) => {}
            (_, Some(_)) => return usage("only reviewer assignments may have a review stage"),
        }
        if self.task_id.is_some_and(|id| id <= 0) || self.pr_number.is_some_and(|id| id <= 0) {
            return usage("assignment task and PR ids must be positive");
        }
        if let Some(complexity) = &self.complexity {
            validate_text("complexity", complexity)?;
        }
        Ok(())
    }
}

/// Assign a responsibility using entropy obtained from SQLite. The persisted bag, rather
/// than this seed or a PRNG implementation, is the restart authority.
pub fn assign_or_get(
    conn: &mut Connection,
    request: &AssignmentRequest,
    pool: &ValidatedPool,
    now: i64,
) -> Result<RoleAssignment> {
    let seed: i64 = conn.query_row("SELECT random()", [], |row| row.get(0))?;
    assign_or_get_with_seed(conn, request, pool, seed as u64, now)
}

/// Seed-injectable form used by deterministic tests.
pub fn assign_or_get_with_seed(
    conn: &mut Connection,
    request: &AssignmentRequest,
    pool: &ValidatedPool,
    seed: u64,
    now: i64,
) -> Result<RoleAssignment> {
    request.validate()?;
    pool.validate()?;
    let tx = begin_immediate(conn)?;
    let assignment = assign_or_get_tx(&tx, request, pool, seed, now)?;
    tx.commit().map_err(map_sql_err)?;
    Ok(assignment)
}

/// Create or reuse an assignment inside a caller-owned write transaction. This is the
/// primitive lifecycle mutators use when consuming an allocation turn must be atomic with
/// winning some other authority (for example, the repository planning freeze).
pub fn assign_or_get_tx(
    tx: &Transaction<'_>,
    request: &AssignmentRequest,
    pool: &ValidatedPool,
    seed: u64,
    now: i64,
) -> Result<RoleAssignment> {
    request.validate()?;
    pool.validate()?;
    validate_scope(request, pool)?;
    let candidate_bag = shuffled_bag(pool, seed);

    if let Some(existing) = get_by_responsibility(tx, &request.responsibility_key)? {
        ensure_same_responsibility(&existing, request)?;
        return Ok(existing);
    }

    tx.execute(
        "INSERT INTO routing_cursors(pool_key,policy_generation,epoch,bag_json,next_slot,updated_at)
         VALUES (?1,?2,0,?3,0,?4) ON CONFLICT(pool_key,policy_generation) DO NOTHING",
        params![
            pool.pool_key,
            pool.policy_generation,
            serde_json::to_string(&candidate_bag).expect("profile ids serialize"),
            now
        ],
    )?;

    let (epoch, bag_json, next_slot): (i64, String, i64) = tx.query_row(
        "SELECT epoch,bag_json,next_slot FROM routing_cursors
         WHERE pool_key=?1 AND policy_generation=?2",
        params![pool.pool_key, pool.policy_generation],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let (epoch, bag, slot) = if next_slot == BAG_SLOTS as i64 {
        let json = serde_json::to_string(&candidate_bag).expect("profile ids serialize");
        let changed = tx.execute(
            "UPDATE routing_cursors SET epoch=?3,bag_json=?4,next_slot=0,updated_at=?5
             WHERE pool_key=?1 AND policy_generation=?2 AND epoch=?6 AND next_slot=100",
            params![
                pool.pool_key,
                pool.policy_generation,
                epoch + 1,
                json,
                now,
                epoch
            ],
        )?;
        if changed != 1 {
            return Err(QuorumError::Io(
                "routing cursor failed to roll over atomically".into(),
            ));
        }
        (epoch + 1, candidate_bag, 0usize)
    } else {
        let bag: Vec<String> = serde_json::from_str(&bag_json)
            .map_err(|_| QuorumError::Io("stored routing bag is invalid".into()))?;
        if bag.len() != BAG_SLOTS || !(0..BAG_SLOTS as i64).contains(&next_slot) {
            return Err(QuorumError::Io("stored routing cursor is invalid".into()));
        }
        (epoch, bag, next_slot as usize)
    };
    let profiles: HashMap<&str, &ModelProfile> = pool
        .profiles
        .iter()
        .map(|entry| (entry.profile.id.as_str(), &entry.profile))
        .collect();
    let mut actual_counts: HashMap<&str, usize> = HashMap::new();
    for profile_id in &bag {
        if !profiles.contains_key(profile_id.as_str()) {
            return Err(QuorumError::Io(
                "stored routing bag references an unknown profile".into(),
            ));
        }
        *actual_counts.entry(profile_id.as_str()).or_default() += 1;
    }
    if pool.profiles.iter().any(|weighted| {
        actual_counts
            .get(weighted.profile.id.as_str())
            .copied()
            .unwrap_or_default()
            != usize::from(weighted.percent)
    }) {
        return Err(QuorumError::Io(
            "stored routing bag does not match configured percentages".into(),
        ));
    }
    let profile = profiles.get(bag[slot].as_str()).copied().ok_or_else(|| {
        QuorumError::Io("stored routing bag references an unknown profile".into())
    })?;

    tx.execute(
        "INSERT INTO role_assignments(
             responsibility_key,task_id,pr_number,role,review_stage,complexity,
             profile_id,provider,runner,model,effort,pool_key,policy_generation,created_at)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
        params![
            request.responsibility_key,
            request.task_id,
            request.pr_number,
            request.role,
            request.review_stage,
            request.complexity,
            profile.id,
            profile.provider,
            profile.runner,
            profile.model,
            profile.effort,
            pool.pool_key,
            pool.policy_generation,
            now
        ],
    )?;
    let assignment_id = tx.last_insert_rowid();
    let changed = tx.execute(
        "UPDATE routing_cursors SET next_slot=?4,updated_at=?5
         WHERE pool_key=?1 AND policy_generation=?2 AND epoch=?3 AND next_slot=?6",
        params![
            pool.pool_key,
            pool.policy_generation,
            epoch,
            slot as i64 + 1,
            now,
            slot as i64
        ],
    )?;
    if changed != 1 {
        return Err(QuorumError::Io(
            "routing cursor failed to advance atomically".into(),
        ));
    }
    let assignment = get_by_id(tx, assignment_id)?
        .ok_or_else(|| QuorumError::Io("created role assignment is missing".into()))?;
    Ok(assignment)
}

pub fn get(conn: &Connection, id: i64) -> Result<Option<RoleAssignment>> {
    get_by_id(conn, id)
}

/// Execute a one-row canonical evidence `INSERT ... SELECT`, guarded by the
/// minimum semantic assignment match in [`EvidenceAssignmentContext`].
///
/// `insert_select` must contain [`EVIDENCE_ASSIGNMENT_GUARD`] exactly once and
/// use `:quorum_assignment_id` for the evidence row's nullable foreign key.
/// Other values are supplied as named `parameters`; names beginning with
/// `:quorum_assignment_` are reserved for this primitive. The guard and insert
/// are one SQLite statement, so an existing but mismatched assignment can never
/// become visible on the evidence row. A mismatch returns an internal error so
/// propagation with `?` also rolls back any caller-owned lifecycle transaction.
pub fn guarded_evidence_insert(
    conn: &Connection,
    evidence: &str,
    context: &EvidenceAssignmentContext<'_>,
    insert_select: &str,
    parameters: &[(&str, &dyn ToSql)],
) -> Result<()> {
    if insert_select.matches(EVIDENCE_ASSIGNMENT_GUARD).count() != 1 {
        return Err(QuorumError::Io(
            "guarded evidence insert must contain exactly one assignment guard".into(),
        ));
    }
    if parameters
        .iter()
        .any(|(name, _)| name.starts_with(":quorum_assignment_"))
    {
        return Err(QuorumError::Io(
            "guarded evidence insert used a reserved assignment parameter".into(),
        ));
    }

    let sql = insert_select.replace(EVIDENCE_ASSIGNMENT_GUARD, EVIDENCE_ASSIGNMENT_PREDICATE);
    let assignment_parameters: [(&str, &dyn ToSql); 8] = [
        (EVIDENCE_ASSIGNMENT_ID_PARAM, &context.role_assignment_id),
        (EVIDENCE_TASK_ID_PARAM, &context.task_id),
        (EVIDENCE_RESPONSIBILITY_PARAM, &context.responsibility_key),
        (EVIDENCE_ROLE_PARAM, &context.role),
        (EVIDENCE_PROVIDER_PARAM, &context.provider),
        (EVIDENCE_RUNNER_PARAM, &context.runner),
        (EVIDENCE_MODEL_PARAM, &context.model),
        (EVIDENCE_EFFORT_PARAM, &context.effort),
    ];
    let mut all_parameters = Vec::with_capacity(parameters.len() + assignment_parameters.len());
    all_parameters.extend_from_slice(parameters);
    all_parameters.extend_from_slice(&assignment_parameters);

    let mut statement = conn.prepare(&sql)?;
    let mut names = HashSet::with_capacity(all_parameters.len());
    for (name, _) in &all_parameters {
        if !names.insert(*name) || statement.parameter_index(name)?.is_none() {
            return Err(QuorumError::Io(
                "guarded evidence insert has duplicate or unused named parameters".into(),
            ));
        }
    }
    if statement.parameter_count() != names.len() {
        return Err(QuorumError::Io(
            "guarded evidence insert has unbound named parameters".into(),
        ));
    }

    let inserted = statement.execute(&all_parameters[..])?;
    if inserted != 1 {
        return Err(evidence_mismatch(evidence));
    }
    Ok(())
}

pub(crate) fn evidence_mismatch(evidence: &str) -> QuorumError {
    QuorumError::Io(format!(
        "role assignment does not match {evidence} evidence"
    ))
}

fn shuffled_bag(pool: &ValidatedPool, seed: u64) -> Vec<String> {
    let mut bag = Vec::with_capacity(BAG_SLOTS);
    for entry in &pool.profiles {
        bag.extend(std::iter::repeat_n(
            entry.profile.id.clone(),
            usize::from(entry.percent),
        ));
    }
    let mut rng = StableRng(seed);
    for index in (1..bag.len()).rev() {
        let swap = (rng.next() % (index as u64 + 1)) as usize;
        bag.swap(index, swap);
    }
    bag
}

struct StableRng(u64);

impl StableRng {
    fn next(&mut self) -> u64 {
        // SplitMix64: small, dependency-free, and sufficient for randomized bag ordering.
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D049BB133111EB);
        value ^ (value >> 31)
    }
}

fn get_by_responsibility(conn: &Transaction<'_>, key: &str) -> Result<Option<RoleAssignment>> {
    query_assignment(conn, "responsibility_key=?1", rusqlite::params![key])
}

fn get_by_id(conn: &Connection, id: i64) -> Result<Option<RoleAssignment>> {
    query_assignment(conn, "id=?1", rusqlite::params![id])
}

fn query_assignment<P: rusqlite::Params>(
    conn: &Connection,
    predicate: &str,
    params: P,
) -> Result<Option<RoleAssignment>> {
    let sql = format!(
        "SELECT id,responsibility_key,task_id,pr_number,role,review_stage,complexity,
                profile_id,provider,runner,model,effort,pool_key,policy_generation,created_at
         FROM role_assignments WHERE {predicate}"
    );
    conn.query_row(&sql, params, |row| {
        Ok(RoleAssignment {
            id: row.get(0)?,
            responsibility_key: row.get(1)?,
            task_id: row.get(2)?,
            pr_number: row.get(3)?,
            role: row.get(4)?,
            review_stage: row.get(5)?,
            complexity: row.get(6)?,
            profile_id: row.get(7)?,
            provider: row.get(8)?,
            runner: row.get(9)?,
            model: row.get(10)?,
            effort: row.get(11)?,
            pool_key: row.get(12)?,
            policy_generation: row.get(13)?,
            created_at: row.get(14)?,
        })
    })
    .optional()
    .map_err(Into::into)
}

fn ensure_same_responsibility(
    existing: &RoleAssignment,
    request: &AssignmentRequest,
) -> Result<()> {
    if existing.task_id != request.task_id
        || existing.pr_number != request.pr_number
        || existing.role != request.role
        || existing.review_stage != request.review_stage
        || existing.complexity != request.complexity
    {
        return Err(QuorumError::Io(
            "responsibility key is already bound to different assignment semantics".into(),
        ));
    }
    Ok(())
}

fn validate_scope(request: &AssignmentRequest, pool: &ValidatedPool) -> Result<()> {
    let expected_pool = match request.role.as_str() {
        "worker" => {
            let complexity = request.complexity.as_deref().ok_or_else(|| {
                QuorumError::Usage("worker assignment requires complexity".into())
            })?;
            format!("worker.{complexity}")
        }
        "reviewer" => {
            let complexity = request.complexity.as_deref().ok_or_else(|| {
                QuorumError::Usage("reviewer assignment requires complexity".into())
            })?;
            let stage = request
                .review_stage
                .as_deref()
                .expect("request validation pins stage");
            format!("reviewer.{complexity}.{stage}")
        }
        "classifier" | "planner" | "arbiter" | "collector" => {
            if request.complexity.is_some() {
                return usage("fixed-role assignment must not have complexity");
            }
            request.role.clone()
        }
        _ => unreachable!("request validation pins role"),
    };
    if pool.pool_key != expected_pool {
        return usage("assignment role, stage, or complexity does not match routing pool");
    }
    Ok(())
}

fn validate_text(label: &str, value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 1024 || value.contains('\0') {
        return usage(&format!("invalid {label}"));
    }
    Ok(())
}

fn usage<T>(message: &str) -> Result<T> {
    Err(QuorumError::Usage(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool(generation: &str) -> ValidatedPool {
        ValidatedPool {
            pool_key: "worker.M".into(),
            policy_generation: generation.into(),
            profiles: vec![
                WeightedProfile {
                    profile: ModelProfile {
                        id: "a".into(),
                        provider: "codex".into(),
                        runner: "codex".into(),
                        model: "a-model".into(),
                        effort: "high".into(),
                    },
                    percent: 80,
                },
                WeightedProfile {
                    profile: ModelProfile {
                        id: "b".into(),
                        provider: "claude".into(),
                        runner: "claude".into(),
                        model: "b-model".into(),
                        effort: "medium".into(),
                    },
                    percent: 20,
                },
            ],
        }
    }

    fn request(index: usize) -> AssignmentRequest {
        AssignmentRequest {
            responsibility_key: format!("worker:task:{index}"),
            task_id: Some(index as i64 + 1),
            pr_number: None,
            role: "worker".into(),
            review_stage: None,
            complexity: Some("M".into()),
        }
    }

    const TEST_EVIDENCE_INSERT: &str = "INSERT INTO canonical_evidence(
            role_assignment_id,payload)
        SELECT :quorum_assignment_id,:payload
        /* quorum-role-assignment-guard */";

    const TEST_EVIDENCE_INSERT_WITH_AUTHORITY_GUARD: &str = "INSERT INTO canonical_evidence(
            role_assignment_id,payload)
        SELECT :quorum_assignment_id,:payload
        /* quorum-role-assignment-guard */
        AND EXISTS(SELECT 1 FROM lifecycle_authority WHERE allowed=1)";

    fn evidence_fixture() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let conn = crate::db::open(&dir.path().join("evidence.db")).unwrap();
        conn.execute_batch(
            "CREATE TABLE lifecycle_state(value TEXT NOT NULL);
             INSERT INTO lifecycle_state(value) VALUES ('before');
             CREATE TABLE lifecycle_authority(allowed INTEGER NOT NULL);
             INSERT INTO lifecycle_authority(allowed) VALUES (0);
             CREATE TABLE canonical_evidence(
                 id INTEGER PRIMARY KEY,
                 role_assignment_id INTEGER REFERENCES role_assignments(id),
                 payload TEXT NOT NULL
             );
             INSERT INTO role_assignments(
                 id,responsibility_key,task_id,role,complexity,profile_id,
                 provider,runner,model,effort,pool_key,policy_generation,created_at)
             VALUES (7,'worker:task:7',7,'worker','M','profile',
                     'codex','codex','gpt-5.6-sol','high','worker.M','g1',1);",
        )
        .unwrap();
        (dir, conn)
    }

    fn evidence_context(role_assignment_id: Option<i64>) -> EvidenceAssignmentContext<'static> {
        EvidenceAssignmentContext {
            role_assignment_id,
            task_id: Some(7),
            responsibility_key: "worker:task:7",
            role: "worker",
            provider: "codex",
            runner: "codex",
            model: "gpt-5.6-sol",
            effort: "high",
        }
    }

    #[test]
    fn guarded_evidence_insert_accepts_exact_match_and_historical_null_link() {
        let (_dir, conn) = evidence_fixture();
        let exact = evidence_context(Some(7));
        let exact_payload = "exact";
        guarded_evidence_insert(
            &conn,
            "test",
            &exact,
            TEST_EVIDENCE_INSERT,
            &[(":payload", &exact_payload)],
        )
        .unwrap();

        let mut historical = evidence_context(None);
        historical.task_id = None;
        historical.responsibility_key = "historical:responsibility";
        historical.role = "collector";
        historical.provider = "claude";
        historical.runner = "claude";
        historical.model = "historical-model";
        historical.effort = "medium";
        let historical_payload = "historical";
        guarded_evidence_insert(
            &conn,
            "test",
            &historical,
            TEST_EVIDENCE_INSERT,
            &[(":payload", &historical_payload)],
        )
        .unwrap();

        let rows: Vec<(Option<i64>, String)> = conn
            .prepare("SELECT role_assignment_id,payload FROM canonical_evidence ORDER BY id")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![(Some(7), "exact".into()), (None, "historical".into())]
        );
    }

    #[test]
    fn historical_null_link_cannot_bypass_an_additional_authority_guard() {
        let (_dir, mut conn) = evidence_fixture();
        let historical = evidence_context(None);
        let payload = "must not persist";

        let result = (|| -> Result<()> {
            let tx = begin_immediate(&mut conn)?;
            tx.execute("UPDATE lifecycle_state SET value='partial'", [])?;
            guarded_evidence_insert(
                &tx,
                "test",
                &historical,
                TEST_EVIDENCE_INSERT_WITH_AUTHORITY_GUARD,
                &[(":payload", &payload)],
            )?;
            tx.commit().map_err(map_sql_err)?;
            Ok(())
        })();

        assert!(result.is_err());
        assert_eq!(
            conn.query_row("SELECT count(*) FROM canonical_evidence", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            conn.query_row("SELECT value FROM lifecycle_state", [], |row| row
                .get::<_, String>(0))
                .unwrap(),
            "before"
        );
    }

    #[test]
    fn guarded_evidence_insert_rejects_every_semantic_mismatch_atomically() {
        #[derive(Debug, Clone, Copy)]
        enum Mismatch {
            Task,
            Responsibility,
            Role,
            Provider,
            Runner,
            Model,
            Effort,
        }

        for mismatch in [
            Mismatch::Task,
            Mismatch::Responsibility,
            Mismatch::Role,
            Mismatch::Provider,
            Mismatch::Runner,
            Mismatch::Model,
            Mismatch::Effort,
        ] {
            let (_dir, mut conn) = evidence_fixture();
            let mut context = evidence_context(Some(7));
            match mismatch {
                Mismatch::Task => context.task_id = Some(8),
                Mismatch::Responsibility => context.responsibility_key = "worker:task:8",
                Mismatch::Role => context.role = "reviewer",
                Mismatch::Provider => context.provider = "claude",
                Mismatch::Runner => context.runner = "claude",
                Mismatch::Model => context.model = "other-model",
                Mismatch::Effort => context.effort = "medium",
            }
            let payload = "must not persist";
            let result = (|| -> Result<()> {
                let tx = begin_immediate(&mut conn)?;
                tx.execute("UPDATE lifecycle_state SET value='partial'", [])?;
                guarded_evidence_insert(
                    &tx,
                    "test",
                    &context,
                    TEST_EVIDENCE_INSERT,
                    &[(":payload", &payload)],
                )?;
                tx.commit().map_err(map_sql_err)?;
                Ok(())
            })();

            assert!(result.is_err(), "{mismatch:?} unexpectedly matched");
            assert_eq!(
                conn.query_row("SELECT count(*) FROM canonical_evidence", [], |row| row
                    .get::<_, i64>(0))
                    .unwrap(),
                0,
                "{mismatch:?} inserted evidence"
            );
            assert_eq!(
                conn.query_row("SELECT value FROM lifecycle_state", [], |row| row
                    .get::<_, String>(0))
                    .unwrap(),
                "before",
                "{mismatch:?} left a partial lifecycle mutation"
            );
        }
    }

    #[test]
    fn exact_distribution_is_persisted_and_restart_safe() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");
        let mut conn = crate::db::open(&path).unwrap();
        let mut counts = HashMap::new();
        for index in 0..100 {
            let assignment =
                assign_or_get_with_seed(&mut conn, &request(index), &pool("g1"), index as u64, 10)
                    .unwrap();
            *counts.entry(assignment.profile_id).or_insert(0) += 1;
        }
        assert_eq!(counts.get("a"), Some(&80));
        assert_eq!(counts.get("b"), Some(&20));
        drop(conn);
        let reopened = crate::db::open(&path).unwrap();
        let cursor: (i64, i64) = reopened
            .query_row("SELECT epoch,next_slot FROM routing_cursors", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!(cursor, (0, 100));
    }

    #[test]
    fn reuse_does_not_advance_and_semantic_collision_fails_closed() {
        let (_dir, mut conn) = {
            let d = tempfile::tempdir().unwrap();
            let c = crate::db::open(&d.path().join("q.db")).unwrap();
            (d, c)
        };
        let first = assign_or_get_with_seed(&mut conn, &request(0), &pool("g1"), 1, 10).unwrap();
        let again = assign_or_get_with_seed(&mut conn, &request(0), &pool("g1"), 999, 20).unwrap();
        assert_eq!(first, again);
        assert_eq!(
            conn.query_row("SELECT next_slot FROM routing_cursors", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        let mut collision = request(0);
        collision.role = "planner".into();
        collision.complexity = None;
        assert!(assign_or_get_with_seed(&mut conn, &collision, &pool("g1"), 2, 20).is_err());
    }

    #[test]
    fn generations_have_independent_cursors_and_preserve_old_assignment() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = crate::db::open(&dir.path().join("q.db")).unwrap();
        let old = assign_or_get_with_seed(&mut conn, &request(0), &pool("g1"), 1, 1).unwrap();
        let new = assign_or_get_with_seed(&mut conn, &request(1), &pool("g2"), 2, 2).unwrap();
        assert_eq!(
            conn.query_row("SELECT count(*) FROM routing_cursors", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(get(&conn, old.id).unwrap().unwrap(), old);
        assert_eq!(new.policy_generation, "g2");
    }

    #[test]
    fn corrupt_persisted_bag_rolls_back_assignment_and_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = crate::db::open(&dir.path().join("q.db")).unwrap();
        conn.execute(
            "INSERT INTO routing_cursors(pool_key,policy_generation,epoch,bag_json,next_slot,updated_at)
             VALUES ('worker.M','g1',0,json_array('missing'),0,1)",
            [],
        )
        .unwrap();
        assert!(assign_or_get_with_seed(&mut conn, &request(0), &pool("g1"), 1, 2).is_err());
        assert_eq!(
            conn.query_row("SELECT count(*) FROM role_assignments", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            conn.query_row("SELECT next_slot FROM routing_cursors", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn persisted_bag_with_wrong_profile_counts_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = crate::db::open(&dir.path().join("q.db")).unwrap();
        let corrupt_bag = serde_json::to_string(&vec!["a"; BAG_SLOTS]).unwrap();
        conn.execute(
            "INSERT INTO routing_cursors(pool_key,policy_generation,epoch,bag_json,next_slot,updated_at)
             VALUES ('worker.M','g1',0,?1,0,1)",
            [corrupt_bag],
        )
        .unwrap();

        let error = assign_or_get_with_seed(&mut conn, &request(0), &pool("g1"), 1, 2).unwrap_err();
        assert!(error.to_string().contains("configured percentages"));
        assert_eq!(
            conn.query_row("SELECT count(*) FROM role_assignments", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            conn.query_row("SELECT next_slot FROM routing_cursors", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn role_complexity_stage_must_match_pool_scope() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = crate::db::open(&dir.path().join("q.db")).unwrap();
        let mut wrong = pool("g1");
        wrong.pool_key = "worker.S".into();
        assert!(assign_or_get_with_seed(&mut conn, &request(0), &wrong, 1, 1).is_err());
        let mut fixed = request(0);
        fixed.role = "planner".into();
        fixed.complexity = Some("M".into());
        fixed.responsibility_key = "planner:task:1:revision:1".into();
        let mut planner_pool = pool("g1");
        planner_pool.pool_key = "planner".into();
        assert!(assign_or_get_with_seed(&mut conn, &fixed, &planner_pool, 1, 1).is_err());
        assert_eq!(
            conn.query_row("SELECT count(*) FROM role_assignments", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn concurrent_distinct_responsibilities_serialize_without_lost_turns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");
        crate::db::open(&path).unwrap();
        let handles = (0..32)
            .map(|index| {
                let path = path.clone();
                std::thread::spawn(move || {
                    let mut conn = crate::db::open(&path).unwrap();
                    assign_or_get_with_seed(
                        &mut conn,
                        &request(index),
                        &pool("g1"),
                        index as u64,
                        10,
                    )
                    .unwrap();
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().unwrap();
        }
        let conn = crate::db::open(&path).unwrap();
        assert_eq!(
            conn.query_row("SELECT count(*) FROM role_assignments", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            32
        );
        assert_eq!(
            conn.query_row("SELECT next_slot FROM routing_cursors", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            32
        );
    }

    #[test]
    fn concurrent_same_responsibility_consumes_exactly_one_turn() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");
        crate::db::open(&path).unwrap();
        let handles = (0..24)
            .map(|seed| {
                let path = path.clone();
                std::thread::spawn(move || {
                    let mut conn = crate::db::open(&path).unwrap();
                    assign_or_get_with_seed(&mut conn, &request(0), &pool("g1"), seed, 10)
                        .unwrap()
                        .id
                })
            })
            .collect::<Vec<_>>();
        let ids = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<HashSet<_>>();
        assert_eq!(ids.len(), 1);
        let conn = crate::db::open(&path).unwrap();
        assert_eq!(
            conn.query_row("SELECT count(*) FROM role_assignments", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            conn.query_row("SELECT next_slot FROM routing_cursors", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }
}
