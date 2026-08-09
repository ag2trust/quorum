mod support;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use support::protocol::{
    Barrier, MaterializeAssessmentInput, Operation, EXIT_NEGATIVE, EXIT_SUCCESS,
};

const ROUNDS: usize = 8;
const RACERS: usize = 4;
const TIMEOUT: Duration = Duration::from_secs(20);

fn seed(path: &Path) {
    let conn = quorum_core::db::open(path).unwrap();
    conn.pragma_update(None, "foreign_keys", true).unwrap();
    conn.execute_batch(
        "INSERT INTO tasks(id,title,status,created_by,created_at,updated_at,refs)
         VALUES (1,'source','done','owner',1,1,'{\"pr\":100}'),
                (2,'other','done','owner',1,1,'{\"pr\":200}');
         INSERT INTO review_followup_batches(
             pr_number,task_id,source_task_id,collector_version,
             artifact_count,state,created_at,updated_at)
         VALUES (100,1,1,'followups-v1',1,'collected',1,1),
                (200,2,2,'followups-v1',1,'collected',1,1);
         INSERT INTO review_collection_runs(
             pr_number,task_id,status,error,collector_model,collector_version,
             findings_count,followup_count,attempted_at,completed_at)
         VALUES (100,1,'success',NULL,'collector','followups-v1',0,1,1,1),
                (200,2,'success',NULL,'collector','followups-v1',0,1,1,1);
         INSERT INTO review_followup_artifacts(
             id,pr_number,ordinal,technical_impact,scope_relationship,concern,
             non_blocking_reason,affected_behavior,desired_outcome,
             verification_expectations,evidence_ids,created_at,updated_at)
         VALUES
             (11,100,0,'major','out_of_scope','one','reason','behavior','outcome',
              '[\"verify\"]','[{\"kind\":\"review\",\"id\":1}]',1,1),
             (12,200,0,'minor','design_debt','two','reason','behavior','outcome',
              '[\"verify\"]','[{\"kind\":\"review\",\"id\":2}]',1,1);",
    )
    .unwrap();
}

fn barrier(ready_path: PathBuf, go_path: &Path) -> Barrier {
    Barrier {
        ready_path,
        go_path: go_path.to_path_buf(),
        timeout_ms: TIMEOUT.as_millis() as u64,
    }
}

fn release_when_ready(ready_paths: &[PathBuf], go_path: &Path) {
    let deadline = Instant::now() + TIMEOUT;
    while !ready_paths.iter().all(|path| path.is_file()) {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for assessment race helpers"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    std::fs::write(go_path, b"go").unwrap();
}

#[test]
fn repeated_process_race_materializes_exactly_one_assessment_and_membership() {
    for round in 0..ROUNDS {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join(format!("assessment-{round}.db"));
        seed(&db_path);
        let go_path = dir.path().join("go");
        let ready_paths = (0..RACERS)
            .map(|index| dir.path().join(format!("ready-{index}")))
            .collect::<Vec<_>>();
        let helpers = ready_paths
            .iter()
            .map(|ready_path| {
                support::spawn(
                    Operation::MaterializeAssessment,
                    &MaterializeAssessmentInput {
                        db_path: db_path.clone(),
                        scope_kind: "task".into(),
                        scope_id: 1,
                        source_task_id: 1,
                        artifact_ids: vec![11],
                        now: 10,
                        barrier: barrier(ready_path.clone(), &go_path),
                    },
                )
                .unwrap()
            })
            .collect::<Vec<_>>();

        release_when_ready(&ready_paths, &go_path);
        let outputs = helpers
            .into_iter()
            .map(|helper| helper.wait(TIMEOUT).unwrap())
            .collect::<Vec<_>>();
        assert!(
            outputs.iter().all(|output| output.stderr.is_empty()),
            "round {round}: {outputs:?}"
        );
        assert_eq!(
            outputs
                .iter()
                .filter(|output| output.status.code() == Some(EXIT_SUCCESS)
                    && output.json()["won"] == true)
                .count(),
            1,
            "round {round}: {outputs:?}"
        );
        assert_eq!(
            outputs
                .iter()
                .filter(|output| output.status.code() == Some(EXIT_NEGATIVE)
                    && output.json()["won"] == false)
                .count(),
            RACERS - 1,
            "round {round}: {outputs:?}"
        );

        let conn = quorum_core::db::open(&db_path).unwrap();
        let materialized: (i64, i64, i64, i64) = conn
            .query_row(
                "SELECT
                     (SELECT count(*) FROM review_followup_assessments
                      WHERE scope_kind='task' AND scope_id=1),
                     (SELECT count(*) FROM review_followup_assessment_artifacts
                      WHERE artifact_id=11),
                     (SELECT count(*) FROM review_followup_assessment_artifacts m
                      JOIN review_followup_assessments a ON a.id=m.assessment_id
                      WHERE a.scope_kind='task' AND a.scope_id=1 AND m.artifact_id=11),
                     (SELECT count(*) FROM errors)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(materialized, (1, 1, 1, 0), "round {round}");
        assert!(conn
            .execute(
                "INSERT INTO review_followup_assessment_artifacts(assessment_id,artifact_id)
                 SELECT id,12 FROM review_followup_assessments WHERE scope_id=1",
                [],
            )
            .is_err());
        assert!(conn
            .execute(
                "DELETE FROM review_followup_assessment_artifacts WHERE artifact_id=11",
                [],
            )
            .is_err());
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM review_followup_assessment_artifacts",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
            1,
            "round {round}"
        );
    }
}
