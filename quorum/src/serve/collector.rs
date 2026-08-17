//! Automatic post-merge review-analytics collector (#125).
//!
//! Called from the daemon after `MergeSucceeded` fires. Deterministic Rust code
//! gathers all inputs (raw `gh api` fetches + DB reads); a bounded Haiku-class
//! classifier turns the assembled record into structured findings; results land
//! in `review_findings` + `review_collection_runs`.
//!
//! Boundary rules (see #125 task brief):
//! - The collector never mutates the task, the merge outcome, the reviewer
//!   verdict, or the rework lifecycle. All writes are analytics-only.
//! - Classifier failures are recorded (loud row in `review_collection_runs` +
//!   `errors` row) and returned; they never unwind a merge.
//! - The classifier is spawned WITHOUT any allowlist tools — it receives the
//!   assembled prompt and must respond with a JSON envelope only. It has no
//!   Bash/Read/Write/Edit and cannot post to GitHub.
//! - Idempotent: re-running for the same PR replaces prior findings and the run
//!   record via `pr_number`-keyed UPSERT.

use super::classifier::{CLASSIFIER_EFFORT, CLASSIFIER_MODEL};
use super::runner::{AdapterConfig, AgentEvent, AgentKind, LaunchMode, LaunchRequest, RunnerProc};
use quorum_core::clock;
use quorum_core::error::{QuorumError, Result};
use quorum_core::review_findings::{
    self, AgentRunSummary, CollectionRun, CollectorInputs, ReviewFinding, RunStatus, TaskContext,
    VerdictSummary,
};
use quorum_core::review_followups::{
    EvidenceKind, FollowupEvidenceIds, ReviewFollowupArtifact, ReviewFollowupEvidenceId,
    ScopeRelationship, TechnicalImpact, VerificationExpectations, MAX_FOLLOWUP_ARTIFACTS,
    MAX_FOLLOWUP_EVIDENCE_IDS, MAX_FOLLOWUP_TEXT_BYTES,
};
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;
use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::timeout;

/// Version stamp on every finding / run row this collector generation writes.
/// Bump when the prompt/schema meaningfully changes so future readers can filter
/// analytics by generation.
pub const COLLECTOR_VERSION: &str = "v3";

/// Hard wall-clock cap on the classifier turn. Post-merge, so failing here
/// leaves the merged task alone — the observable surface is `review_collection_runs`.
const CLASSIFIER_TIMEOUT: Duration = Duration::from_secs(180);

/// Cap each raw GitHub payload we splice into the prompt. Bounded input protects
/// budget on huge PRs (a 3k-comment thread should not become a 200k-token prompt).
const MAX_PAYLOAD_BYTES: usize = 64 * 1024;

/// Wall-clock cap per `gh api` sub-call. If a fetch hangs the classifier never
/// runs — better than letting the daemon-scoped task stall indefinitely.
const GH_FETCH_TIMEOUT: Duration = Duration::from_secs(30);

const MAX_REVIEW_FINDINGS: usize = 128;

/// The largest distinct evidence universe a maximum-size response can cite:
/// every finding and every artifact may use its complete bounded evidence list.
/// Production extraction stops at this record count while streaming the raw
/// paginated JSON, before retaining another entry or allocating a full DOM.
const MAX_FETCHED_EVIDENCE_RECORDS: usize =
    (MAX_REVIEW_FINDINGS + MAX_FOLLOWUP_ARTIFACTS) * MAX_FOLLOWUP_EVIDENCE_IDS;

/// Bound the raw evidence material this PR's added parser will inspect. The
/// underlying `gh` capture is older transport debt, but payloads above this
/// aggregate limit fail before Serde performs any additional parsing work.
const MAX_FETCHED_EVIDENCE_JSON_BYTES: usize = 4 * 1024 * 1024;
const MAX_FETCHED_EVIDENCE_ARRAY_DEPTH: usize = 2;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCollectorResponse {
    findings: Vec<RawFinding>,
    followup_artifacts: Vec<RawFollowupArtifact>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFinding {
    reviewer: String,
    kind: String,
    author_pushback: bool,
    pushback_accepted: Option<bool>,
    severity: Option<String>,
    text: String,
    source_endpoint: String,
    addressed_status: String,
    evidence: Vec<RawEvidenceId>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawEvidenceId {
    kind: EvidenceKind,
    id: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFollowupArtifact {
    source_finding_index: usize,
    technical_impact: TechnicalImpact,
    scope_relationship: ScopeRelationship,
    concern: RawConcreteConcern,
    non_blocking_reason: String,
    affected_behavior: String,
    desired_outcome: RawObservableOutcome,
    verification_expectations: Vec<String>,
    evidence: Vec<RawEvidenceId>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConcreteConcern {
    failure_mode: String,
    trigger_or_assumption: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawObservableOutcome {
    observable_behavior: String,
    observation_condition: String,
}

#[derive(Debug)]
struct ValidatedArtifactText {
    concern: String,
    desired_outcome: String,
}

#[derive(Debug)]
struct ValidatedCollectorResponse {
    findings: Vec<ReviewFinding>,
    followup_artifacts: Vec<ReviewFollowupArtifact>,
}

fn parse_and_validate_response(
    text: &str,
    inputs: &CollectorInputs,
    task_id: Option<i64>,
    collector_model: &str,
    collector_version: &str,
) -> Result<ValidatedCollectorResponse> {
    let json_text = extract_collector_json(text)
        .ok_or_else(|| QuorumError::Usage("collector response is not a JSON object".into()))?;
    let raw: RawCollectorResponse = serde_json::from_str(json_text)
        .map_err(|error| QuorumError::Usage(format!("invalid collector response: {error}")))?;

    if raw.findings.len() > MAX_REVIEW_FINDINGS {
        return Err(QuorumError::Usage(
            "collector response exceeds 128 findings".into(),
        ));
    }
    if raw.followup_artifacts.len() > MAX_FOLLOWUP_ARTIFACTS {
        return Err(QuorumError::Usage(
            "collector response exceeds 32 follow-up artifacts".into(),
        ));
    }

    let available_evidence = fetched_evidence(inputs)?;
    for finding in &raw.findings {
        validate_finding(finding, &available_evidence)?;
    }

    // Keep the existing review_findings parser as the sole normalization and
    // provenance-stamping path. The strict envelope above adds collector-only
    // validation without changing compatibility for other callers of that API.
    let findings = review_findings::parse_response_with_provenance(
        json_text,
        inputs.pr_number,
        task_id,
        Some(collector_model),
        Some(collector_version),
    )
    .ok_or_else(|| QuorumError::Usage("collector findings could not be normalized".into()))?;

    let mut followup_artifacts = Vec::with_capacity(raw.followup_artifacts.len());
    for (ordinal, artifact) in raw.followup_artifacts.into_iter().enumerate() {
        validate_artifact_source(&artifact, &raw.findings)?;
        let artifact_text = validate_artifact_text(&artifact)?;
        let verification_expectations =
            VerificationExpectations::new(artifact.verification_expectations)?;
        let evidence_ids = validated_evidence(artifact.evidence, &available_evidence)?;
        followup_artifacts.push(ReviewFollowupArtifact::new(
            None,
            inputs.pr_number,
            ordinal as i64,
            artifact.technical_impact,
            artifact.scope_relationship,
            artifact_text.concern,
            artifact.non_blocking_reason,
            artifact.affected_behavior,
            artifact_text.desired_outcome,
            verification_expectations,
            evidence_ids,
            None,
            0,
            0,
        )?);
    }

    Ok(ValidatedCollectorResponse {
        findings,
        followup_artifacts,
    })
}

fn extract_collector_json(text: &str) -> Option<&str> {
    let trimmed = text.trim();
    if trimmed.starts_with('{') {
        return Some(trimmed);
    }
    if let Some(start) = trimmed.find("```json") {
        let after = &trimmed[start + 7..];
        return after.find("```").map(|end| after[..end].trim());
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

fn validate_finding(
    finding: &RawFinding,
    available_evidence: &HashSet<(EvidenceKind, i64)>,
) -> Result<()> {
    validate_bounded_text("finding reviewer", &finding.reviewer)?;
    validate_bounded_text("finding text", &finding.text)?;
    if !matches!(finding.kind.as_str(), "blocking" | "suggestion") {
        return Err(QuorumError::Usage(format!(
            "invalid collector finding kind: {}",
            finding.kind
        )));
    }
    if !matches!(finding.source_endpoint.as_str(), "pulls" | "issues") {
        return Err(QuorumError::Usage(format!(
            "invalid collector finding source endpoint: {}",
            finding.source_endpoint
        )));
    }
    if !matches!(
        finding.addressed_status.as_str(),
        "addressed" | "unaddressed" | "partial" | "unclear" | "withdrawn"
    ) {
        return Err(QuorumError::Usage(format!(
            "invalid collector finding addressed status: {}",
            finding.addressed_status
        )));
    }
    if finding
        .severity
        .as_deref()
        .is_some_and(|severity| !matches!(severity, "critical" | "major" | "minor" | "nit"))
    {
        return Err(QuorumError::Usage(
            "invalid collector finding severity".into(),
        ));
    }
    if !finding.author_pushback && finding.pushback_accepted.is_some() {
        return Err(QuorumError::Usage(
            "collector finding accepts pushback that was not reported".into(),
        ));
    }
    validated_evidence(finding.evidence.clone(), available_evidence)?;
    Ok(())
}

fn validate_artifact_source(artifact: &RawFollowupArtifact, findings: &[RawFinding]) -> Result<()> {
    let source = findings.get(artifact.source_finding_index).ok_or_else(|| {
        QuorumError::Usage(format!(
            "follow-up artifact source finding index {} is out of bounds",
            artifact.source_finding_index
        ))
    })?;
    if source.kind != "suggestion" {
        return Err(QuorumError::Usage(
            "follow-up artifact source finding is not a suggestion".into(),
        ));
    }
    if matches!(source.addressed_status.as_str(), "addressed" | "withdrawn")
        || source.pushback_accepted == Some(true)
    {
        return Err(QuorumError::Usage(
            "follow-up artifact source was fixed, withdrawn, or accepted as invalid".into(),
        ));
    }

    let artifact_evidence = artifact
        .evidence
        .iter()
        .map(|evidence| (evidence.kind, evidence.id))
        .collect::<HashSet<_>>();
    if !source
        .evidence
        .iter()
        .any(|evidence| artifact_evidence.contains(&(evidence.kind, evidence.id)))
    {
        return Err(QuorumError::Usage(
            "follow-up artifact does not share evidence with its source finding".into(),
        ));
    }
    Ok(())
}

fn validate_artifact_text(artifact: &RawFollowupArtifact) -> Result<ValidatedArtifactText> {
    for (field, value) in [
        ("non-blocking reason", artifact.non_blocking_reason.as_str()),
        ("affected behavior", artifact.affected_behavior.as_str()),
    ] {
        validate_bounded_text(field, value)?;
    }
    for expectation in &artifact.verification_expectations {
        validate_bounded_text("verification expectation", expectation)?;
    }
    let concern = join_structured_text(
        "concern",
        "failure mode",
        &artifact.concern.failure_mode,
        "trigger or assumption",
        &artifact.concern.trigger_or_assumption,
    )?;
    let desired_outcome = join_structured_text(
        "desired outcome",
        "observable behavior",
        &artifact.desired_outcome.observable_behavior,
        "observation condition",
        &artifact.desired_outcome.observation_condition,
    )?;
    Ok(ValidatedArtifactText {
        concern,
        desired_outcome,
    })
}

fn validate_bounded_text(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() || value.contains('\0') || value.len() > MAX_FOLLOWUP_TEXT_BYTES {
        return Err(QuorumError::Usage(format!(
            "invalid bounded collector {field}"
        )));
    }
    Ok(())
}

fn join_structured_text(
    field: &str,
    first_label: &str,
    first: &str,
    second_label: &str,
    second: &str,
) -> Result<String> {
    validate_bounded_text(first_label, first)?;
    validate_bounded_text(second_label, second)?;
    let combined = format!("{} when {}", first.trim(), second.trim());
    validate_bounded_text(field, &combined)?;
    Ok(combined)
}

fn validated_evidence(
    evidence: Vec<RawEvidenceId>,
    available_evidence: &HashSet<(EvidenceKind, i64)>,
) -> Result<FollowupEvidenceIds> {
    let evidence = evidence
        .into_iter()
        .map(|evidence| {
            if !available_evidence.contains(&(evidence.kind, evidence.id)) {
                return Err(QuorumError::Usage(format!(
                    "collector evidence {}#{} is absent from fetched input",
                    evidence.kind, evidence.id
                )));
            }
            ReviewFollowupEvidenceId::new(evidence.kind, evidence.id)
        })
        .collect::<Result<Vec<_>>>()?;
    FollowupEvidenceIds::new(evidence)
}

fn fetched_evidence(inputs: &CollectorInputs) -> Result<HashSet<(EvidenceKind, i64)>> {
    if let Some(index) = &inputs.fetched_evidence {
        if index.len() > MAX_FETCHED_EVIDENCE_RECORDS {
            return Err(QuorumError::Usage(
                "fetched evidence index exceeds bounded record count".into(),
            ));
        }
        return index
            .iter()
            .map(|evidence| {
                let kind = evidence.kind.parse::<EvidenceKind>()?;
                ReviewFollowupEvidenceId::new(kind, evidence.id)?;
                Ok((kind, evidence.id))
            })
            .collect();
    }
    fetched_evidence_from_json(
        &inputs.reviews_json,
        &inputs.review_comments_json,
        &inputs.issue_comments_json,
    )
}

fn fetched_evidence_from_json(
    reviews_json: &str,
    review_comments_json: &str,
    issue_comments_json: &str,
) -> Result<HashSet<(EvidenceKind, i64)>> {
    let input_bytes = reviews_json
        .len()
        .checked_add(review_comments_json.len())
        .and_then(|bytes| bytes.checked_add(issue_comments_json.len()))
        .ok_or_else(|| QuorumError::Usage("fetched evidence byte count overflow".into()))?;
    if input_bytes > MAX_FETCHED_EVIDENCE_JSON_BYTES {
        return Err(QuorumError::Usage(
            "fetched evidence JSON exceeds bounded input size".into(),
        ));
    }
    let mut index = FetchedEvidenceIndex::default();
    add_fetched_ids(&mut index, EvidenceKind::Review, reviews_json)?;
    add_fetched_ids(
        &mut index,
        EvidenceKind::ReviewComment,
        review_comments_json,
    )?;
    add_fetched_ids(&mut index, EvidenceKind::IssueComment, issue_comments_json)?;
    Ok(index.evidence)
}

#[derive(Default)]
struct FetchedEvidenceIndex {
    evidence: HashSet<(EvidenceKind, i64)>,
    records: usize,
}

fn add_fetched_ids(index: &mut FetchedEvidenceIndex, kind: EvidenceKind, json: &str) -> Result<()> {
    let mut deserializer = serde_json::Deserializer::from_str(json);
    FetchedEvidenceSeed {
        index,
        kind,
        array_depth: 0,
    }
    .deserialize(&mut deserializer)
    .and_then(|()| deserializer.end())
    .map_err(|error| QuorumError::Usage(format!("malformed fetched {kind} evidence JSON: {error}")))
}

struct FetchedEvidenceSeed<'a> {
    index: &'a mut FetchedEvidenceIndex,
    kind: EvidenceKind,
    array_depth: usize,
}

impl<'de> DeserializeSeed<'de> for FetchedEvidenceSeed<'_> {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<(), D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_any(FetchedEvidenceVisitor {
            index: self.index,
            kind: self.kind,
            array_depth: self.array_depth,
        })
    }
}

struct FetchedEvidenceVisitor<'a> {
    index: &'a mut FetchedEvidenceIndex,
    kind: EvidenceKind,
    array_depth: usize,
}

impl<'de> Visitor<'de> for FetchedEvidenceVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a GitHub evidence record or paginated array")
    }

    fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<(), A::Error>
    where
        A: SeqAccess<'de>,
    {
        if self.array_depth >= MAX_FETCHED_EVIDENCE_ARRAY_DEPTH {
            return Err(de::Error::custom(
                "fetched evidence exceeds bounded array depth",
            ));
        }
        let index = self.index;
        while sequence
            .next_element_seed(FetchedEvidenceSeed {
                index: &mut *index,
                kind: self.kind,
                array_depth: self.array_depth + 1,
            })?
            .is_some()
        {}
        Ok(())
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<(), A::Error>
    where
        A: MapAccess<'de>,
    {
        self.index.records = self
            .index
            .records
            .checked_add(1)
            .ok_or_else(|| de::Error::custom("fetched evidence record count overflow"))?;
        if self.index.records > MAX_FETCHED_EVIDENCE_RECORDS {
            return Err(de::Error::custom(
                "fetched evidence exceeds bounded record count",
            ));
        }

        let mut id = None;
        while let Some(field) = map.next_key::<FetchedEvidenceField>()? {
            match field {
                FetchedEvidenceField::Id => id = map.next_value::<Option<i64>>()?,
                FetchedEvidenceField::Other => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        if let Some(id) = id.filter(|id| *id > 0) {
            self.index.evidence.insert((self.kind, id));
        }
        Ok(())
    }

    fn visit_bool<E>(self, _value: bool) -> std::result::Result<(), E> {
        Ok(())
    }

    fn visit_i64<E>(self, _value: i64) -> std::result::Result<(), E> {
        Ok(())
    }

    fn visit_u64<E>(self, _value: u64) -> std::result::Result<(), E> {
        Ok(())
    }

    fn visit_f64<E>(self, _value: f64) -> std::result::Result<(), E> {
        Ok(())
    }

    fn visit_str<E>(self, _value: &str) -> std::result::Result<(), E> {
        Ok(())
    }

    fn visit_none<E>(self) -> std::result::Result<(), E> {
        Ok(())
    }

    fn visit_unit<E>(self) -> std::result::Result<(), E> {
        Ok(())
    }
}

enum FetchedEvidenceField {
    Id,
    Other,
}

impl<'de> Deserialize<'de> for FetchedEvidenceField {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_identifier(FetchedEvidenceFieldVisitor)
    }
}

struct FetchedEvidenceFieldVisitor;

impl Visitor<'_> for FetchedEvidenceFieldVisitor {
    type Value = FetchedEvidenceField;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a GitHub evidence record field")
    }

    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E> {
        Ok(if value == "id" {
            FetchedEvidenceField::Id
        } else {
            FetchedEvidenceField::Other
        })
    }
}

/// Everything the collector needs to run for one PR. Passed into
/// [`run_collection_with_inputs`] so tests can bypass `gh api` entirely.
#[derive(Debug, Clone)]
pub struct CollectionRequest {
    pub pr_number: i64,
    pub task_id: Option<i64>,
    pub repo_slug: Option<String>,
    pub db_path: PathBuf,
    pub repo_dir: PathBuf,
    pub agent_bin: Option<String>,
    pub bare_agent: bool,
    pub collector_provider: String,
    pub collector_runner: String,
    pub collector_model: String,
    pub collector_effort: String,
    pub role_assignment_id: Option<i64>,
    pub codex_sandbox: String,
    /// Extra env vars threaded into the spawned classifier process. Empty in
    /// production. Used by tests to script the fake-agent binary per-call so
    /// concurrent tests do not race on a process-global env var.
    #[allow(dead_code)]
    pub env_vars: Vec<(String, String)>,
}

impl CollectionRequest {
    /// Convenience constructor for callers that don't need custom env vars.
    pub fn new(
        pr_number: i64,
        task_id: Option<i64>,
        repo_slug: Option<String>,
        db_path: PathBuf,
        repo_dir: PathBuf,
        agent_bin: Option<String>,
        bare_agent: bool,
    ) -> Self {
        let default_provider = AgentKind::for_model(CLASSIFIER_MODEL)
            .map(|kind| kind.to_string())
            .unwrap_or_default();
        Self {
            pr_number,
            task_id,
            repo_slug,
            db_path,
            repo_dir,
            agent_bin,
            bare_agent,
            collector_provider: default_provider.clone(),
            collector_runner: default_provider,
            collector_model: CLASSIFIER_MODEL.to_string(),
            collector_effort: CLASSIFIER_EFFORT.to_string(),
            role_assignment_id: None,
            codex_sandbox: "danger-full-access".to_string(),
            env_vars: vec![],
        }
    }

    pub fn with_collector(
        mut self,
        provider: impl Into<String>,
        runner: impl Into<String>,
        model: impl Into<String>,
        effort: impl Into<String>,
        codex_sandbox: impl Into<String>,
    ) -> Self {
        self.collector_provider = provider.into();
        self.collector_runner = runner.into();
        self.collector_model = model.into();
        self.collector_effort = effort.into();
        self.codex_sandbox = codex_sandbox.into();
        self
    }
}

/// Result summary returned to the caller (the daemon logs it; the CLI prints it).
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CollectionOutcome {
    pub pr_number: i64,
    pub task_id: Option<i64>,
    pub status: RunStatus,
    pub findings_count: i64,
    pub error: Option<String>,
}

struct ClassifierTurnOutcome {
    response: Result<String>,
    usage: super::runner::TokenUsage,
}

/// Full pipeline: fetch inputs, spawn classifier, parse + store. On any failure
/// the run record is stamped `failed` with the error text — never returns without
/// having written the run row.
pub async fn run_collection(request: &CollectionRequest) -> Result<CollectionOutcome> {
    let attempted_at = clock::now();

    // 1) Deterministic fetch.
    let inputs_result = fetch_inputs(request).await;
    let inputs = match inputs_result {
        Ok(i) => i,
        Err(e) => {
            let err_text = format!("collector fetch failed: {e}");
            record_failure(request, &err_text, attempted_at).await;
            return Err(QuorumError::Io(err_text));
        }
    };
    run_collection_with_inputs(request, inputs, attempted_at).await
}

/// Same as [`run_collection`] but skips the deterministic fetch — the caller
/// supplies a fully-built [`CollectorInputs`]. Tests bypass `gh` here; the
/// production caller is [`run_collection`], which fetches then delegates.
pub async fn run_collection_with_inputs(
    request: &CollectionRequest,
    inputs: CollectorInputs,
    attempted_at: i64,
) -> Result<CollectionOutcome> {
    // 2) Spawn classifier + await bounded turn.
    let turn = match spawn_and_run_classifier(request, &inputs).await {
        Ok(turn) => turn,
        Err(e) => {
            let err_text = format!("collector classifier failed: {e}");
            record_failure(request, &err_text, attempted_at).await;
            return Err(QuorumError::Io(err_text));
        }
    };
    record_token_usage(request, turn.usage).await;
    let response_text = match turn.response {
        Ok(response) => response,
        Err(e) => {
            let err_text = format!("collector classifier failed: {e}");
            record_failure(request, &err_text, attempted_at).await;
            return Err(QuorumError::Io(err_text));
        }
    };

    // 3) Parse response.
    let validated = match parse_and_validate_response(
        &response_text,
        &inputs,
        request.task_id,
        &request.collector_model,
        COLLECTOR_VERSION,
    ) {
        Ok(response) => response,
        Err(_) => {
            let err_text = format!(
                "collector response parse failed for PR #{} — response len {}",
                request.pr_number,
                response_text.len()
            );
            record_failure(request, &err_text, attempted_at).await;
            return Err(QuorumError::Io(err_text));
        }
    };
    let findings = validated.findings;
    // Persistence of the already validated immutable artifact batch belongs to
    // the later atomic-success slice. Keep this parser-only change from
    // altering existing success writes.
    let _followup_artifacts = validated.followup_artifacts;

    // 4) Persist findings + success run row.
    let pr = request.pr_number;
    let task_id = request.task_id;
    let db_path = request.db_path.clone();
    let collector_provider = request.collector_provider.clone();
    let collector_runner = request.collector_runner.clone();
    let collector_model = request.collector_model.clone();
    let collector_effort = request.collector_effort.clone();
    let role_assignment_id = request.role_assignment_id;
    let count = findings.len() as i64;
    let write_result = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&db_path)?;
        let now = clock::now();
        review_findings::replace_for_pr_and_record_run(
            &mut conn,
            pr,
            &findings,
            &CollectionRun {
                pr_number: pr,
                task_id,
                status: RunStatus::Success,
                error: None,
                collector_model,
                collector_provider: Some(collector_provider),
                collector_runner: Some(collector_runner),
                collector_effort: Some(collector_effort),
                collector_version: COLLECTOR_VERSION.to_string(),
                findings_count: count,
                attempted_at,
                completed_at: Some(now),
                role_assignment_id,
            },
        )?;
        Ok(())
    })
    .await;

    match write_result {
        Ok(Ok(())) => Ok(CollectionOutcome {
            pr_number: pr,
            task_id,
            status: RunStatus::Success,
            findings_count: count,
            error: None,
        }),
        Ok(Err(e)) => {
            let err_text = format!("collector db write failed: {e}");
            record_failure(request, &err_text, attempted_at).await;
            Err(QuorumError::Io(err_text))
        }
        Err(e) => {
            let err_text = format!("collector db-write join failed: {e}");
            record_failure(request, &err_text, attempted_at).await;
            Err(QuorumError::Io(err_text))
        }
    }
}

/// Record a failed run + log to `errors`. Best-effort: a follow-up failure here
/// must not shadow the original error the caller is about to report.
async fn record_failure(request: &CollectionRequest, error: &str, attempted_at: i64) {
    let db_path = request.db_path.clone();
    let pr = request.pr_number;
    let task_id = request.task_id;
    let error_text = error.to_string();
    let collector_provider = request.collector_provider.clone();
    let collector_runner = request.collector_runner.clone();
    let collector_model = request.collector_model.clone();
    let collector_effort = request.collector_effort.clone();
    let role_assignment_id = request.role_assignment_id;
    let _ = tokio::task::spawn_blocking(move || -> Result<()> {
        let conn = quorum_core::db::open(&db_path)?;
        let now = clock::now();
        let record_result = review_findings::record_run(
            &conn,
            &CollectionRun {
                pr_number: pr,
                task_id,
                status: RunStatus::Failed,
                error: Some(error_text.clone()),
                collector_model,
                collector_provider: Some(collector_provider),
                collector_runner: Some(collector_runner),
                collector_effort: Some(collector_effort),
                collector_version: COLLECTOR_VERSION.to_string(),
                findings_count: 0,
                attempted_at,
                completed_at: Some(now),
                role_assignment_id,
            },
        );
        // The guarded evidence write may reject a stale or mismatched managed
        // assignment. Preserve that failure while still reporting it through
        // the existing canonical error telemetry.
        quorum_core::errlog::log_error(&conn, now, "review-collector", &error_text);
        record_result
    })
    .await;
}

/// Token telemetry is best-effort and intentionally independent from collector
/// findings and lifecycle records.
async fn record_token_usage(request: &CollectionRequest, usage: super::runner::TokenUsage) {
    let db_path = request.db_path.clone();
    let task_ids: Vec<i64> = request.task_id.into_iter().collect();
    let pr_number = request.pr_number;
    let provider = request.collector_provider.clone();
    let model = request.collector_model.clone();
    let effort = request.collector_effort.clone();
    let usage = quorum_core::token_usage::TokenUsage {
        uncached_input_tokens: usage.uncached_input_tokens as i64,
        cached_input_tokens: usage.cached_input_tokens as i64,
        cache_write_input_tokens: usage.cache_write_input_tokens as i64,
        output_tokens: usage.output_tokens as i64,
        reasoning_tokens: usage.reasoning_tokens as i64,
    };
    match tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = quorum_core::db::open(&db_path)?;
        quorum_core::token_usage::record(
            &mut conn,
            None,
            "collector",
            &task_ids,
            Some(pr_number),
            &provider,
            &model,
            &effort,
            usage,
            clock::now(),
        )?;
        Ok(())
    })
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            eprintln!("quorum collector: token usage write failed (ignored): {error}")
        }
        Err(error) => eprintln!("quorum collector: token usage join failed (ignored): {error}"),
    }
}

/// Deterministically fetch every input the classifier will read. Failures at
/// any single sub-fetch propagate — better to record one loud failed run and
/// preserve any prior good analytics than to hand the model a partial view and
/// overwrite prior findings with a degraded record. Bounded via
/// [`MAX_PAYLOAD_BYTES`] so a pathological PR does not blow up the classifier's
/// context budget.
///
/// Pagination: list endpoints (reviews, comments, commits) are fetched via
/// `gh api --paginate --slurp` so every page is retained as a single JSON
/// array. Repo targeting: `--repo owner/name` is threaded via `GH_REPO` env
/// var — `gh api` does not accept `-R`.
pub async fn fetch_inputs(request: &CollectionRequest) -> Result<CollectorInputs> {
    let pr = request.pr_number;
    let repo = request.repo_slug.clone();
    let repo_dir = request.repo_dir.clone();

    let pr_metadata_json = gh_api(
        &repo,
        &repo_dir,
        &format!("repos/{{owner}}/{{repo}}/pulls/{pr}"),
        false,
    )
    .await?;
    let reviews_json = gh_api(
        &repo,
        &repo_dir,
        &format!("repos/{{owner}}/{{repo}}/pulls/{pr}/reviews"),
        true,
    )
    .await?;
    let review_comments_json = gh_api(
        &repo,
        &repo_dir,
        &format!("repos/{{owner}}/{{repo}}/pulls/{pr}/comments"),
        true,
    )
    .await?;
    let issue_comments_json = gh_api(
        &repo,
        &repo_dir,
        &format!("repos/{{owner}}/{{repo}}/issues/{pr}/comments"),
        true,
    )
    .await?;
    let commits_json = gh_api(
        &repo,
        &repo_dir,
        &format!("repos/{{owner}}/{{repo}}/pulls/{pr}/commits"),
        true,
    )
    .await?;
    let diff_stat = gh_diff_stat(&repo, &repo_dir, pr).await?;
    let checks_summary = gh_checks_summary(&repo, &repo_dir, pr).await?;
    let mut fetched_evidence =
        fetched_evidence_from_json(&reviews_json, &review_comments_json, &issue_comments_json)?
            .into_iter()
            .map(|(kind, id)| review_findings::EvidenceId {
                kind: kind.to_string(),
                id,
            })
            .collect::<Vec<_>>();
    fetched_evidence.sort_by(|left, right| {
        left.kind
            .cmp(&right.kind)
            .then_with(|| left.id.cmp(&right.id))
    });

    // DB context (task metadata + agent runs + verdicts). All best-effort;
    // absence produces empty context rather than a hard failure.
    let task_context = build_task_context(&request.db_path, request.task_id).await;

    Ok(CollectorInputs {
        pr_number: pr,
        pr_metadata_json: truncate(&pr_metadata_json),
        reviews_json: truncate(&reviews_json),
        review_comments_json: truncate(&review_comments_json),
        issue_comments_json: truncate(&issue_comments_json),
        fetched_evidence: Some(fetched_evidence),
        commits_json: truncate(&commits_json),
        checks_summary: truncate(&checks_summary),
        diff_stat: truncate(&diff_stat),
        task_context,
    })
}

fn truncate(s: &str) -> String {
    if s.len() <= MAX_PAYLOAD_BYTES {
        s.to_string()
    } else {
        let mut cut = MAX_PAYLOAD_BYTES;
        // Avoid slicing mid-UTF-8 code point.
        while cut > 0 && !s.is_char_boundary(cut) {
            cut -= 1;
        }
        format!("{}\n... [truncated {} bytes]", &s[..cut], s.len() - cut)
    }
}

async fn build_task_context(db_path: &Path, task_id: Option<i64>) -> TaskContext {
    let Some(tid) = task_id else {
        return TaskContext::default();
    };
    let db_path = db_path.to_path_buf();
    let joined = tokio::task::spawn_blocking(move || -> Option<TaskContext> {
        let conn = quorum_core::db::open(&db_path).ok()?;
        let task = quorum_core::tasks::get(&conn, tid).ok().flatten()?;
        let runs = quorum_core::agent_runs::runs_for_task(&conn, tid).unwrap_or_default();
        let agent_runs = runs
            .into_iter()
            .map(|r| AgentRunSummary {
                agent: r.agent,
                role: r.role,
                sub_role: r.sub_role,
                model: r.model,
                effort: r.effort,
                end_reason: r.end_reason,
            })
            .collect();
        // Approvals table is small (live approvals only) but historic ones are
        // deleted on merge, so at most one row will typically still exist when
        // this runs (during the merge-succeeded firing itself). Fall through
        // gracefully if empty.
        let mut verdicts = Vec::new();
        if let Ok(all) = quorum_core::approvals::list(&conn) {
            for a in all.into_iter().filter(|a| a.task_id == tid) {
                verdicts.push(VerdictSummary {
                    reviewer: a.reviewer,
                    verdict: a.verdict,
                    blocking_count: a.blocking_count,
                    head_sha: a.approved_head_sha,
                    created_at: 0,
                });
            }
        }
        Some(TaskContext {
            task_id: Some(tid),
            author: task.author,
            reviewer: task.reviewer,
            rework_round: task.rework_round,
            review_only: task.review_only,
            agent_runs,
            verdicts,
        })
    })
    .await
    .ok()
    .flatten();
    joined.unwrap_or(TaskContext {
        task_id: Some(tid),
        ..TaskContext::default()
    })
}

/// Build the `gh api` argv. Repo is NOT threaded via `-R` (unsupported by
/// `gh api`); the caller sets `GH_REPO` in the child env via [`gh_env`]. When
/// `paginate` is true, adds `--paginate --slurp` so multi-page collections
/// return a single JSON array with every page's records preserved.
pub(crate) fn build_gh_api_args(endpoint: &str, paginate: bool) -> Vec<String> {
    let mut args: Vec<String> = vec!["api".into()];
    if paginate {
        args.push("--paginate".into());
        args.push("--slurp".into());
    }
    args.push(endpoint.to_string());
    args
}

/// Env vars to set on any spawned `gh` process so `--repo owner/name` overrides
/// take effect without relying on the `-R` shorthand (which `gh api` rejects).
pub(crate) fn gh_env(repo: &Option<String>) -> Vec<(String, String)> {
    match repo {
        Some(r) if !r.is_empty() => vec![("GH_REPO".to_string(), r.clone())],
        _ => Vec::new(),
    }
}

async fn gh_api(
    repo: &Option<String>,
    cwd: &Path,
    endpoint: &str,
    paginate: bool,
) -> Result<String> {
    let args = build_gh_api_args(endpoint, paginate);
    run_gh(&args, cwd, &gh_env(repo)).await
}

async fn gh_diff_stat(repo: &Option<String>, cwd: &Path, pr: i64) -> Result<String> {
    // `gh pr view` accepts `-R` and it is the canonical way to override repo for
    // the higher-level `gh pr *` surface. Keep it explicit here (belt + braces:
    // GH_REPO is also set) so a shim that greps the argv sees the target repo.
    let mut args: Vec<String> = vec![
        "pr".into(),
        "view".into(),
        pr.to_string(),
        "--json".into(),
        "changedFiles,additions,deletions,title".into(),
    ];
    if let Some(r) = repo {
        args.push("-R".into());
        args.push(r.clone());
    }
    run_gh(&args, cwd, &gh_env(repo)).await
}

async fn gh_checks_summary(repo: &Option<String>, cwd: &Path, pr: i64) -> Result<String> {
    let mut args: Vec<String> = vec!["pr".into(), "checks".into(), pr.to_string()];
    if let Some(r) = repo {
        args.push("-R".into());
        args.push(r.clone());
    }
    match run_gh_raw(&args, cwd, &gh_env(repo)).await {
        Ok(stdout) => Ok(stdout),
        Err(GhError::NoChecks) => Ok("no checks reported".to_string()),
        Err(GhError::Failed(e)) => Err(e),
    }
}

/// Distinguishes "no checks reported" (valid empty state for repos without CI)
/// from genuine gh failures (auth, network, rate-limit, malformed response).
enum GhError {
    /// `gh pr checks` exited nonzero with stderr containing "no checks reported".
    NoChecks,
    /// Any other failure — propagate as a retryable error.
    Failed(QuorumError),
}

async fn run_gh(args: &[String], cwd: &Path, env: &[(String, String)]) -> Result<String> {
    match run_gh_raw(args, cwd, env).await {
        Ok(s) => Ok(s),
        Err(GhError::NoChecks) => Err(QuorumError::Io(
            "gh: no checks reported (unexpected in run_gh)".into(),
        )),
        Err(GhError::Failed(e)) => Err(e),
    }
}

async fn run_gh_raw(
    args: &[String],
    cwd: &Path,
    env: &[(String, String)],
) -> std::result::Result<String, GhError> {
    let cwd = cwd.to_path_buf();
    let args = args.to_vec();
    let env = env.to_vec();
    let handle = tokio::task::spawn_blocking(move || -> std::result::Result<String, GhError> {
        let mut cmd = std::process::Command::new("gh");
        cmd.args(&args).current_dir(&cwd);
        for (k, v) in &env {
            cmd.env(k, v);
        }
        let out = cmd
            .output()
            .map_err(|e| GhError::Failed(QuorumError::Io(format!("gh: {e}"))))?;
        if !out.status.success() {
            let stderr = String::from_utf8_lossy(&out.stderr);
            if stderr.contains("no checks reported") {
                return Err(GhError::NoChecks);
            }
            return Err(GhError::Failed(QuorumError::Io(format!(
                "gh {args:?} failed: {stderr}"
            ))));
        }
        String::from_utf8(out.stdout)
            .map_err(|e| GhError::Failed(QuorumError::Io(format!("invalid utf8: {e}"))))
    });
    match timeout(GH_FETCH_TIMEOUT, handle).await {
        Ok(Ok(res)) => res,
        Ok(Err(join_err)) => Err(GhError::Failed(QuorumError::Io(format!(
            "gh join failed: {join_err}"
        )))),
        Err(_) => Err(GhError::Failed(QuorumError::Io("gh timed out".into()))),
    }
}

/// Spawn the Haiku classifier, feed the prompt, wait for a bounded Result.
/// The spawn passes an EMPTY allowed_tools list — the classifier can only
/// respond in text, cannot Bash / Read / Write / Edit, and cannot reach GitHub.
async fn spawn_and_run_classifier(
    request: &CollectionRequest,
    inputs: &CollectorInputs,
) -> Result<ClassifierTurnOutcome> {
    let prompt = review_findings::build_collector_prompt(inputs);
    let mut proc = RunnerProc::launch(
        &LaunchRequest {
            model: &request.collector_model,
            effort: &request.collector_effort,
            worktree: &request.repo_dir,
            prompt: &prompt,
            environment: &request.env_vars,
            mode: LaunchMode::Normal,
            continuation_id: None,
        },
        &AdapterConfig {
            executable: request.agent_bin.as_deref(),
            claude_bare: request.bare_agent,
            claude_allowed_tools: "",
            codex_sandbox: &request.codex_sandbox,
            grok: Default::default(),
        },
    )
    .await
    .map_err(|e| QuorumError::Io(format!("spawn classifier: {e}")))?;

    let deadline = tokio::time::Instant::now() + CLASSIFIER_TIMEOUT;
    let mut response_text = String::new();
    let mut usage_total = super::runner::TokenUsage::default();
    let outcome: Result<String> = loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break Err(QuorumError::Io(format!(
                "classifier timeout for PR #{}",
                request.pr_number
            )));
        }
        match timeout(remaining, proc.next_raw_line()).await {
            Ok(Some(raw)) => {
                let line = proc.normalize_line(&raw);
                if let Some(text) = line.terminal_text.as_ref().filter(|text| !text.is_empty()) {
                    response_text = text.clone();
                }
                let mut terminal = None;
                for event in line.events {
                    match event {
                        AgentEvent::AssistantText { text } => response_text.push_str(&text),
                        AgentEvent::TurnCompleted { usage, .. } => {
                            if let Some(usage) = usage {
                                usage_total.saturating_add_assign(usage);
                            }
                            terminal = Some(Ok(response_text.clone()))
                        }
                        AgentEvent::TurnFailed { message, usage, .. } => {
                            if let Some(usage) = usage {
                                usage_total.saturating_add_assign(usage);
                            }
                            let message = line.terminal_text.as_deref().unwrap_or(&message);
                            terminal = Some(Err(QuorumError::Io(format!(
                                "classifier returned an error: {message}"
                            ))))
                        }
                        _ => {}
                    }
                }
                if let Some(result) = terminal {
                    break result;
                }
            }
            Ok(None) => {
                break if response_text.is_empty() {
                    Err(QuorumError::Io(
                        "classifier stream closed with no response".into(),
                    ))
                } else {
                    Ok(response_text.clone())
                };
            }
            Err(_) => {
                break Err(QuorumError::Io(format!(
                    "classifier timeout for PR #{}",
                    request.pr_number
                )));
            }
        }
    };

    // Reaping can drain a terminal event raced by the timeout. It remains
    // lifecycle-inert, but its usage is still durable telemetry.
    let kind = proc.kind();
    let terminal = proc.kill_and_reap().await;
    Ok(finalize_classifier_turn(
        kind,
        terminal,
        outcome,
        usage_total,
    ))
}

fn finalize_classifier_turn(
    kind: AgentKind,
    terminal: Vec<super::runner::CapturedOutput>,
    response: Result<String>,
    mut usage: super::runner::TokenUsage,
) -> ClassifierTurnOutcome {
    for captured in terminal {
        let super::runner::CapturedOutput::Stdout(raw_line) = captured else {
            continue;
        };
        for event in super::runner::normalize_line(kind, &raw_line) {
            match event {
                AgentEvent::TurnCompleted {
                    usage: Some(value), ..
                }
                | AgentEvent::TurnFailed {
                    usage: Some(value), ..
                } => usage.saturating_add_assign(value),
                _ => {}
            }
        }
    }
    ClassifierTurnOutcome { response, usage }
}

/// Spawn a detached collection task from the daemon merge branch. Never awaits.
/// The daemon tick returns immediately; the collection runs in the background
/// and records its own success/failure to `review_collection_runs`. This is the
/// architectural boundary — the merged task's lifecycle is already complete
/// before we get here, and nothing this function does can undo that.
pub fn spawn_detached(request: CollectionRequest) {
    tokio::spawn(async move {
        match run_collection(&request).await {
            Ok(outcome) => {
                eprintln!(
                    "quorum serve: post-merge collector for PR #{} completed \
                     ({} findings)",
                    outcome.pr_number, outcome.findings_count
                );
            }
            Err(e) => {
                eprintln!(
                    "quorum serve: post-merge collector for PR #{} FAILED: {} \
                     (recorded in review_collection_runs; retry via `quorum review-interpret`)",
                    request.pr_number, e
                );
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configured_collector_identity_is_carried_by_collection_request() {
        let request = CollectionRequest::new(
            1,
            None,
            None,
            PathBuf::from("/tmp/q.db"),
            PathBuf::from("/tmp"),
            None,
            false,
        )
        .with_collector(
            "codex",
            "codex",
            "gpt-5.6-terra",
            "high",
            "danger-full-access",
        );

        assert_eq!(request.collector_provider, "codex");
        assert_eq!(request.collector_runner, "codex");
        assert_eq!(request.collector_model, "gpt-5.6-terra");
        assert_eq!(request.collector_effort, "high");
    }
    use quorum_core::db;

    fn tmp_conn() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.db");
        let _ = db::open(&path).unwrap();
        (dir, path)
    }

    #[test]
    fn truncate_bounded_input_marks_cut() {
        let s = "a".repeat(MAX_PAYLOAD_BYTES + 100);
        let out = truncate(&s);
        assert!(out.contains("truncated"));
        assert!(out.len() < s.len() + 40);
    }

    #[test]
    fn truncate_short_input_passes_through() {
        let s = "hello";
        assert_eq!(truncate(s), "hello");
    }

    #[test]
    fn truncate_respects_utf8_boundary() {
        // Emoji is 4 bytes; construct a string just past MAX_PAYLOAD_BYTES
        // with a multibyte char straddling the boundary. Slicing mid-codepoint
        // would panic — the guard steps back until it lands on a boundary.
        let mut s = "a".repeat(MAX_PAYLOAD_BYTES - 2);
        s.push('😀'); // 4-byte codepoint straddles the cap
        s.push_str(&"b".repeat(100));
        let out = truncate(&s); // must not panic
        assert!(out.contains("truncated"));
    }

    #[test]
    fn timeout_boundary_reap_retains_terminal_usage() {
        let outcome = finalize_classifier_turn(
            AgentKind::Claude,
            vec![super::super::runner::CapturedOutput::Stdout(
                r#"{"type":"result","result":"late","is_error":false,"usage":{"input_tokens":100,"cache_read_input_tokens":80,"cache_creation_input_tokens":10,"output_tokens":5}}"#
                    .into(),
            )],
            Err(QuorumError::Io("classifier timeout for PR #464".into())),
            super::super::runner::TokenUsage::default(),
        );

        assert!(matches!(
            outcome.response,
            Err(QuorumError::Io(ref message)) if message.contains("timeout")
        ));
        assert_eq!(
            outcome.usage,
            super::super::runner::TokenUsage {
                input_tokens: 100,
                uncached_input_tokens: 20,
                cached_input_tokens: 80,
                cache_write_input_tokens: 10,
                output_tokens: 5,
                reasoning_tokens: 0,
            },
            "terminal usage raced by the timeout must survive final reap"
        );
    }

    #[tokio::test]
    async fn record_failure_writes_run_and_error_rows() {
        let (_dir, db_path) = tmp_conn();
        let request = CollectionRequest::new(
            42,
            Some(7),
            None,
            db_path.clone(),
            std::env::current_dir().unwrap(),
            None,
            true,
        )
        .with_collector(
            "codex",
            "codex",
            "gpt-5.6-terra",
            "medium",
            "danger-full-access",
        );
        record_failure(&request, "boom", 1000).await;

        let conn = db::open(&db_path).unwrap();
        let run = review_findings::get_run(&conn, 42).unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Failed);
        assert_eq!(run.error.as_deref(), Some("boom"));
        assert_eq!(run.collector_model, "gpt-5.6-terra");
        assert_eq!(run.collector_version, COLLECTOR_VERSION);

        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM errors WHERE source='review-collector'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
    }

    #[tokio::test]
    async fn record_failure_logs_error_when_guarded_run_rejects_assignment() {
        let (_dir, db_path) = tmp_conn();
        let mut request = CollectionRequest::new(
            45,
            Some(7),
            None,
            db_path.clone(),
            std::env::current_dir().unwrap(),
            None,
            true,
        )
        .with_collector(
            "codex",
            "codex",
            "gpt-5.6-terra",
            "medium",
            "danger-full-access",
        );
        request.role_assignment_id = Some(77);

        let conn = db::open(&db_path).unwrap();
        conn.execute(
            "INSERT INTO role_assignments(
                 id,responsibility_key,task_id,pr_number,role,profile_id,provider,runner,model,effort,
                 pool_key,policy_generation,created_at)
             VALUES (77,'collector:pr:45',7,45,'collector','collector-profile','codex',
                     'claude','gpt-5.6-terra','medium','collector','g1',1)",
            [],
        )
        .unwrap();
        drop(conn);

        record_failure(&request, "assignment mismatch", 1000).await;

        let conn = db::open(&db_path).unwrap();
        assert!(review_findings::get_run(&conn, 45).unwrap().is_none());
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM errors
                 WHERE source='review-collector' AND detail='assignment mismatch'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
    }

    #[tokio::test]
    async fn record_failure_is_idempotent_on_retry() {
        let (_dir, db_path) = tmp_conn();
        let request = CollectionRequest::new(
            43,
            None,
            None,
            db_path.clone(),
            std::env::current_dir().unwrap(),
            None,
            true,
        );
        record_failure(&request, "attempt-1", 1000).await;
        record_failure(&request, "attempt-2", 2000).await;

        let conn = db::open(&db_path).unwrap();
        // Only one row for the PR (UPSERT keyed on pr_number).
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM review_collection_runs WHERE pr_number=?1",
                rusqlite::params![43i64],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
        let run = review_findings::get_run(&conn, 43).unwrap().unwrap();
        assert_eq!(run.error.as_deref(), Some("attempt-2"));
        assert_eq!(run.attempted_at, 2000);
    }

    #[tokio::test]
    async fn unknown_classifier_model_fails_loudly_without_runner_fallback() {
        let dir = setup_git_dir();
        let db_path = dir.path().join("q.db");
        let _ = db::open(&db_path).unwrap();
        let request = CollectionRequest::new(
            44,
            None,
            None,
            db_path.clone(),
            dir.path().to_path_buf(),
            Some("/bin/false".into()),
            true,
        )
        .with_collector(
            "unknown",
            "unknown",
            "unknown-provider-model",
            "medium",
            "danger-full-access",
        );

        let error = run_collection_with_inputs(&request, synthetic_inputs(44), 1000)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("unknown model"));

        let conn = db::open(&db_path).unwrap();
        let run = review_findings::get_run(&conn, 44).unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Failed);
        assert_eq!(run.collector_model, "unknown-provider-model");
    }

    #[tokio::test]
    async fn build_task_context_absent_task_returns_default() {
        let (_dir, db_path) = tmp_conn();
        let ctx = build_task_context(&db_path, None).await;
        assert!(ctx.task_id.is_none());
        assert!(ctx.agent_runs.is_empty());
    }

    // ------------------------------------------------------------------
    // Live end-to-end tests driving the built `fake-agent` binary.
    //
    // These cover the classifier → parse → persist pipeline via
    // `run_collection_with_inputs`, so they don't need `gh` or a real repo.
    // The fetch layer is covered separately by the arg-builder unit tests
    // plus the PATH-shim gh integration test — keeping the pipeline halves
    // decoupled so a broken shim never masks a classifier regression.
    // ------------------------------------------------------------------

    fn fake_agent_path() -> std::path::PathBuf {
        assert_cmd::cargo::cargo_bin("fake-agent")
    }

    fn setup_git_dir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path().to_string_lossy();
        let _ = std::process::Command::new("git")
            .args(["-C", &d, "init", "-b", "main"])
            .output();
        dir
    }

    fn synthetic_inputs(pr: i64) -> CollectorInputs {
        CollectorInputs {
            pr_number: pr,
            pr_metadata_json: "{}".into(),
            reviews_json: "[]".into(),
            review_comments_json: r#"[[{"id":101}]]"#.into(),
            issue_comments_json: r#"[{"id":202}]"#.into(),
            fetched_evidence: None,
            commits_json: "[]".into(),
            checks_summary: "unknown".into(),
            diff_stat: "unknown".into(),
            task_context: TaskContext::default(),
        }
    }

    fn parser_inputs(review_comment_ids: &[i64], issue_comment_ids: &[i64]) -> CollectorInputs {
        let records = |ids: &[i64]| {
            serde_json::to_string(
                &ids.iter()
                    .map(|id| serde_json::json!({"id": id}))
                    .collect::<Vec<_>>(),
            )
            .unwrap()
        };
        CollectorInputs {
            pr_number: 42,
            pr_metadata_json: "{}".into(),
            reviews_json: "[]".into(),
            review_comments_json: records(review_comment_ids),
            issue_comments_json: records(issue_comment_ids),
            fetched_evidence: None,
            commits_json: "[]".into(),
            checks_summary: "all passed".into(),
            diff_stat: "1 file".into(),
            task_context: TaskContext::default(),
        }
    }

    fn finding(kind: &str, id: i64) -> serde_json::Value {
        serde_json::json!({
            "reviewer": "reviewer",
            "kind": kind,
            "author_pushback": false,
            "pushback_accepted": null,
            "severity": "minor",
            "text": "Timeout handling can lose the pending operation",
            "source_endpoint": "pulls",
            "addressed_status": "unaddressed",
            "evidence": [{"kind": "review_comment", "id": id}]
        })
    }

    fn artifact(id: i64) -> serde_json::Value {
        serde_json::json!({
            "source_finding_index": 0,
            "technical_impact": "major",
            "scope_relationship": "out_of_scope",
            "concern": {
                "failure_mode": "The worker loses the pending operation",
                "trigger_or_assumption": "A provider request times out during shutdown"
            },
            "non_blocking_reason": "The merged change does not alter timeout handling",
            "affected_behavior": "Worker shutdown after a provider timeout",
            "desired_outcome": {
                "observable_behavior": "The worker preserves the pending operation for retry",
                "observation_condition": "A provider request times out during shutdown"
            },
            "verification_expectations": ["A timeout recovery test preserves the operation"],
            "evidence": [{"kind": "review_comment", "id": id}]
        })
    }

    fn response(findings: Vec<serde_json::Value>, artifacts: Vec<serde_json::Value>) -> String {
        serde_json::json!({
            "findings": findings,
            "followup_artifacts": artifacts,
        })
        .to_string()
    }

    fn parse_fixture(
        response: &str,
        inputs: &CollectorInputs,
    ) -> Result<ValidatedCollectorResponse> {
        parse_and_validate_response(response, inputs, Some(7), "collector-model", "v-test")
    }

    #[test]
    fn collector_protocol_accepts_valid_zero_artifact_interpretation() {
        let parsed = parse_fixture(
            r#"{"findings":[],"followup_artifacts":[]}"#,
            &parser_inputs(&[], &[]),
        )
        .unwrap();
        assert!(parsed.findings.is_empty());
        assert!(parsed.followup_artifacts.is_empty());
    }

    #[test]
    fn collector_protocol_accepts_single_artifact_and_stamps_relationships() {
        let parsed = parse_fixture(
            &response(vec![finding("suggestion", 10)], vec![artifact(10)]),
            &parser_inputs(&[10], &[]),
        )
        .unwrap();
        assert_eq!(parsed.findings.len(), 1);
        assert_eq!(parsed.findings[0].pr_number, 42);
        assert_eq!(parsed.findings[0].task_id, Some(7));
        assert_eq!(parsed.followup_artifacts.len(), 1);
        assert_eq!(parsed.followup_artifacts[0].pr_number(), 42);
        assert_eq!(parsed.followup_artifacts[0].ordinal(), 0);
        assert_eq!(
            parsed.followup_artifacts[0].technical_impact(),
            TechnicalImpact::Major
        );
    }

    #[test]
    fn collector_protocol_accepts_every_artifact_enum_value() {
        for impact in ["critical", "major", "minor", "nit"] {
            let mut value = artifact(10);
            value["technical_impact"] = serde_json::Value::String(impact.into());
            assert!(parse_fixture(
                &response(vec![finding("suggestion", 10)], vec![value]),
                &parser_inputs(&[10], &[]),
            )
            .is_ok());
        }
        for relationship in [
            "pre_existing",
            "out_of_scope",
            "threat_model_expansion",
            "defense_in_depth",
            "future_requirement",
            "design_debt",
        ] {
            let mut value = artifact(10);
            value["scope_relationship"] = serde_json::Value::String(relationship.into());
            assert!(parse_fixture(
                &response(vec![finding("suggestion", 10)], vec![value]),
                &parser_inputs(&[10], &[]),
            )
            .is_ok());
        }
    }

    #[test]
    fn collector_protocol_rejects_unknown_enum_values() {
        for (field, value) in [
            ("technical_impact", "severe"),
            ("scope_relationship", "adjacent"),
        ] {
            let mut candidate = artifact(10);
            candidate[field] = serde_json::Value::String(value.into());
            assert!(parse_fixture(
                &response(vec![finding("suggestion", 10)], vec![candidate]),
                &parser_inputs(&[10], &[]),
            )
            .is_err());
        }

        let mut candidate = artifact(10);
        candidate["evidence"][0]["kind"] = serde_json::json!("commit");
        assert!(parse_fixture(
            &response(vec![finding("suggestion", 10)], vec![candidate]),
            &parser_inputs(&[10], &[]),
        )
        .is_err());
    }

    #[test]
    fn collector_protocol_accepts_every_evidence_kind() {
        for kind in ["review", "review_comment", "issue_comment"] {
            let mut inputs = parser_inputs(&[10], &[10]);
            inputs.reviews_json = r#"[{"id":10}]"#.into();
            let mut source = finding("suggestion", 10);
            source["evidence"][0]["kind"] = serde_json::json!(kind);
            let mut candidate = artifact(10);
            candidate["evidence"][0]["kind"] = serde_json::json!(kind);
            assert!(parse_fixture(&response(vec![source], vec![candidate]), &inputs).is_ok());
        }
    }

    #[test]
    fn collector_protocol_rejects_unknown_finding_enum_values() {
        let inputs = parser_inputs(&[10], &[]);
        for (field, value) in [
            ("kind", "advisory"),
            ("severity", "moderate"),
            ("source_endpoint", "commits"),
            ("addressed_status", "fixed"),
        ] {
            let mut source = finding("suggestion", 10);
            source[field] = serde_json::json!(value);
            assert!(parse_fixture(&response(vec![source], vec![]), &inputs).is_err());
        }
    }

    #[test]
    fn collector_protocol_rejects_unknown_fields_at_every_level() {
        let inputs = parser_inputs(&[10], &[]);

        let mut envelope: serde_json::Value = serde_json::from_str(&response(
            vec![finding("suggestion", 10)],
            vec![artifact(10)],
        ))
        .unwrap();
        envelope["extra"] = serde_json::json!(true);
        assert!(parse_fixture(&envelope.to_string(), &inputs).is_err());

        let mut unknown_finding = finding("suggestion", 10);
        unknown_finding["extra"] = serde_json::json!(true);
        assert!(parse_fixture(
            &response(vec![unknown_finding], vec![artifact(10)]),
            &inputs
        )
        .is_err());

        let mut unknown_artifact = artifact(10);
        unknown_artifact["extra"] = serde_json::json!(true);
        assert!(parse_fixture(
            &response(vec![finding("suggestion", 10)], vec![unknown_artifact]),
            &inputs,
        )
        .is_err());

        let mut unknown_concern = artifact(10);
        unknown_concern["concern"]["extra"] = serde_json::json!(true);
        assert!(parse_fixture(
            &response(vec![finding("suggestion", 10)], vec![unknown_concern]),
            &inputs,
        )
        .is_err());

        let mut unknown_outcome = artifact(10);
        unknown_outcome["desired_outcome"]["extra"] = serde_json::json!(true);
        assert!(parse_fixture(
            &response(vec![finding("suggestion", 10)], vec![unknown_outcome]),
            &inputs,
        )
        .is_err());

        let mut unknown_evidence = artifact(10);
        unknown_evidence["evidence"][0]["extra"] = serde_json::json!(true);
        assert!(parse_fixture(
            &response(vec![finding("suggestion", 10)], vec![unknown_evidence]),
            &inputs,
        )
        .is_err());
    }

    #[test]
    fn collector_protocol_accepts_maximum_counts_and_bounds() {
        let ids = (1..=MAX_REVIEW_FINDINGS as i64).collect::<Vec<_>>();
        let findings = ids
            .iter()
            .map(|id| finding("suggestion", *id))
            .collect::<Vec<_>>();
        let mut artifacts = (0..MAX_FOLLOWUP_ARTIFACTS)
            .map(|_| artifact(1))
            .collect::<Vec<_>>();
        let trigger = "concurrent shutdown overlaps request handling";
        artifacts[0]["concern"]["failure_mode"] = serde_json::Value::String(
            "x".repeat(MAX_FOLLOWUP_TEXT_BYTES - " when ".len() - trigger.len()),
        );
        artifacts[0]["concern"]["trigger_or_assumption"] =
            serde_json::Value::String(trigger.into());
        for candidate in &mut artifacts {
            candidate["verification_expectations"] = serde_json::json!([
                "expectation one",
                "expectation two",
                "expectation three",
                "expectation four",
                "expectation five",
                "expectation six",
                "expectation seven",
                "expectation eight"
            ]);
        }
        let parsed =
            parse_fixture(&response(findings, artifacts), &parser_inputs(&ids, &[])).unwrap();
        assert_eq!(parsed.findings.len(), MAX_REVIEW_FINDINGS);
        assert_eq!(parsed.followup_artifacts.len(), MAX_FOLLOWUP_ARTIFACTS);
        assert_eq!(
            parsed.followup_artifacts[0]
                .verification_expectations()
                .as_slice()
                .len(),
            8
        );
    }

    #[test]
    fn collector_protocol_accepts_maximum_evidence_count() {
        let ids = (1..=128i64).collect::<Vec<_>>();
        let evidence = ids
            .iter()
            .map(|id| serde_json::json!({"kind": "review_comment", "id": id}))
            .collect::<Vec<_>>();
        let mut source = finding("suggestion", 1);
        source["evidence"] = serde_json::Value::Array(evidence.clone());
        let mut candidate = artifact(1);
        candidate["evidence"] = serde_json::Value::Array(evidence);
        let parsed = parse_fixture(
            &response(vec![source], vec![candidate]),
            &parser_inputs(&ids, &[]),
        )
        .unwrap();
        assert_eq!(
            parsed.followup_artifacts[0].evidence_ids().as_slice().len(),
            128
        );
    }

    #[test]
    fn collector_protocol_rejects_evidence_absent_from_fetched_input() {
        assert!(parse_fixture(
            &response(vec![finding("suggestion", 10)], vec![artifact(10)]),
            &parser_inputs(&[11], &[]),
        )
        .is_err());
    }

    #[test]
    fn collector_protocol_uses_pre_truncation_evidence_index() {
        let mut inputs = parser_inputs(&[], &[]);
        inputs.review_comments_json = "[{\"id\":10}\n... [truncated 200 bytes]".into();
        inputs.fetched_evidence = Some(vec![review_findings::EvidenceId {
            kind: "review_comment".into(),
            id: 10,
        }]);
        assert!(parse_fixture(
            &response(vec![finding("suggestion", 10)], vec![artifact(10)]),
            &inputs,
        )
        .is_ok());
    }

    #[test]
    fn fetched_evidence_streams_paginated_records_without_nested_payload_ids() {
        let large_body = "x".repeat(MAX_PAYLOAD_BYTES * 2);
        let reviews = format!(
            r#"[[{{"id":10,"body":{},"nested":{{"id":999}}}}],[{{"id":11}}]]"#,
            serde_json::to_string(&large_body).unwrap()
        );
        let evidence = fetched_evidence_from_json(&reviews, "[]", "[]").unwrap();
        assert_eq!(evidence.len(), 2);
        assert!(evidence.contains(&(EvidenceKind::Review, 10)));
        assert!(evidence.contains(&(EvidenceKind::Review, 11)));
        assert!(!evidence.contains(&(EvidenceKind::Review, 999)));
    }

    #[test]
    fn fetched_evidence_rejects_record_overrun_at_streaming_boundary() {
        let records = std::iter::repeat_n(r#"{"id":1}"#, MAX_FETCHED_EVIDENCE_RECORDS + 1)
            .collect::<Vec<_>>()
            .join(",");
        let reviews = format!("[{records}]");
        let error = fetched_evidence_from_json(&reviews, "[]", "[]").unwrap_err();
        assert!(error
            .to_string()
            .contains("fetched evidence exceeds bounded record count"));
    }

    #[test]
    fn fetched_evidence_rejects_oversized_input_before_json_parsing() {
        let oversized_malformed = "[".repeat(MAX_FETCHED_EVIDENCE_JSON_BYTES + 1);
        let error = fetched_evidence_from_json(&oversized_malformed, "[]", "[]").unwrap_err();
        assert!(error
            .to_string()
            .contains("fetched evidence JSON exceeds bounded input size"));
    }

    #[test]
    fn fetched_evidence_rejects_unexpected_array_nesting() {
        let error = fetched_evidence_from_json("[[[{\"id\":1}]]]", "[]", "[]").unwrap_err();
        assert!(error
            .to_string()
            .contains("fetched evidence exceeds bounded array depth"));
    }

    #[test]
    fn collector_protocol_rejects_artifact_without_suggestion_source() {
        assert!(parse_fixture(
            &response(vec![finding("blocking", 10)], vec![artifact(10)]),
            &parser_inputs(&[10], &[]),
        )
        .is_err());
    }

    #[test]
    fn collector_protocol_disambiguates_mixed_validity_findings_with_shared_evidence() {
        let inputs = parser_inputs(&[10], &[]);
        let mut invalid = finding("suggestion", 10);
        invalid["addressed_status"] = serde_json::json!("addressed");
        let valid = finding("suggestion", 10);

        let mut valid_artifact = artifact(10);
        valid_artifact["source_finding_index"] = serde_json::json!(1);
        assert!(parse_fixture(
            &response(vec![invalid.clone(), valid.clone()], vec![valid_artifact]),
            &inputs,
        )
        .is_ok());

        let invalid_artifact = artifact(10);
        assert!(parse_fixture(
            &response(vec![invalid, valid], vec![invalid_artifact]),
            &inputs,
        )
        .is_err());
    }

    #[test]
    fn collector_protocol_rejects_invalid_source_finding_references() {
        let inputs = parser_inputs(&[10], &[]);
        for invalid_index in [
            serde_json::json!(-1),
            serde_json::json!(1),
            serde_json::json!("0"),
        ] {
            let mut candidate = artifact(10);
            candidate["source_finding_index"] = invalid_index;
            assert!(parse_fixture(
                &response(vec![finding("suggestion", 10)], vec![candidate]),
                &inputs,
            )
            .is_err());
        }

        let mut missing = artifact(10);
        missing
            .as_object_mut()
            .unwrap()
            .remove("source_finding_index");
        assert!(parse_fixture(
            &response(vec![finding("suggestion", 10)], vec![missing]),
            &inputs,
        )
        .is_err());
    }

    #[test]
    fn collector_protocol_requires_evidence_shared_with_selected_source() {
        let inputs = parser_inputs(&[10, 11], &[]);
        let evidence_match = finding("suggestion", 10);
        let selected_without_match = finding("suggestion", 11);
        let mut candidate = artifact(10);
        candidate["source_finding_index"] = serde_json::json!(1);

        assert!(parse_fixture(
            &response(
                vec![evidence_match, selected_without_match],
                vec![candidate]
            ),
            &inputs,
        )
        .is_err());
    }

    #[test]
    fn collector_protocol_rejects_fixed_withdrawn_or_invalid_sources() {
        let inputs = parser_inputs(&[10], &[]);
        let mut fixed = finding("suggestion", 10);
        fixed["addressed_status"] = serde_json::json!("addressed");
        assert!(parse_fixture(&response(vec![fixed], vec![artifact(10)]), &inputs).is_err());

        // A reviewer can independently retract a suggestion without any
        // author pushback. That final state must be representable and rejected.
        let mut withdrawn = finding("suggestion", 10);
        withdrawn["addressed_status"] = serde_json::json!("withdrawn");
        assert!(parse_fixture(&response(vec![withdrawn], vec![artifact(10)]), &inputs).is_err());

        let mut accepted_as_invalid = finding("suggestion", 10);
        accepted_as_invalid["author_pushback"] = serde_json::json!(true);
        accepted_as_invalid["pushback_accepted"] = serde_json::json!(true);
        assert!(parse_fixture(
            &response(vec![accepted_as_invalid], vec![artifact(10)]),
            &inputs,
        )
        .is_err());
    }

    #[test]
    fn collector_protocol_preserves_withdrawn_finding_without_artifact() {
        let inputs = parser_inputs(&[10], &[]);
        let mut withdrawn = finding("suggestion", 10);
        withdrawn["addressed_status"] = serde_json::json!("withdrawn");

        let parsed = parse_fixture(&response(vec![withdrawn], vec![]), &inputs).unwrap();
        assert_eq!(
            parsed.findings[0].addressed_status.as_deref(),
            Some("withdrawn")
        );
        assert!(parsed.followup_artifacts.is_empty());
    }

    #[test]
    fn collector_protocol_rejects_unstructured_vague_concern_or_desired_outcome() {
        let inputs = parser_inputs(&[10], &[]);
        for field in ["concern", "desired_outcome"] {
            for vague_text in [
                "Make code better",
                "Improve reliability",
                "Better reliability.",
                "Improve queue reliability",
                "Queue reliability should improve",
                "The queue should be more reliable",
            ] {
                let mut candidate = artifact(10);
                candidate[field] = serde_json::json!(vague_text);
                assert!(
                    parse_fixture(
                        &response(vec![finding("suggestion", 10)], vec![candidate]),
                        &inputs,
                    )
                    .is_err(),
                    "{field} accepted vague text: {vague_text}"
                );
            }
        }

        let mut incomplete_concern = artifact(10);
        incomplete_concern["concern"] =
            serde_json::json!({"failure_mode": "Queue reliability should improve"});
        assert!(parse_fixture(
            &response(vec![finding("suggestion", 10)], vec![incomplete_concern]),
            &inputs,
        )
        .is_err());

        let mut incomplete_outcome = artifact(10);
        incomplete_outcome["desired_outcome"] =
            serde_json::json!({"observable_behavior": "The queue should be more reliable"});
        assert!(parse_fixture(
            &response(vec![finding("suggestion", 10)], vec![incomplete_outcome]),
            &inputs,
        )
        .is_err());
    }

    #[test]
    fn collector_protocol_accepts_concise_concrete_concern_and_outcome() {
        let inputs = parser_inputs(&[10], &[]);
        let mut candidate = artifact(10);
        candidate["concern"]["failure_mode"] = serde_json::json!("Requests deadlock");
        candidate["concern"]["trigger_or_assumption"] =
            serde_json::json!("Concurrent shutdown overlaps request handling");
        candidate["desired_outcome"]["observable_behavior"] =
            serde_json::json!("Requests complete");
        candidate["desired_outcome"]["observation_condition"] =
            serde_json::json!("The retry resumes after shutdown");
        let parsed = parse_fixture(
            &response(vec![finding("suggestion", 10)], vec![candidate]),
            &inputs,
        )
        .unwrap();
        assert_eq!(
            parsed.followup_artifacts[0].concern(),
            "Requests deadlock when Concurrent shutdown overlaps request handling"
        );
        assert_eq!(
            parsed.followup_artifacts[0].desired_outcome(),
            "Requests complete when The retry resumes after shutdown"
        );
    }

    #[test]
    fn collector_protocol_rejects_duplicate_artifact_evidence() {
        let mut candidate = artifact(10);
        candidate["evidence"] = serde_json::json!([
            {"kind": "review_comment", "id": 10},
            {"kind": "review_comment", "id": 10}
        ]);
        assert!(parse_fixture(
            &response(vec![finding("suggestion", 10)], vec![candidate]),
            &parser_inputs(&[10], &[]),
        )
        .is_err());
    }

    #[test]
    fn collector_protocol_rejects_evidence_count_overrun() {
        let ids = (1..=129i64).collect::<Vec<_>>();
        let evidence = ids
            .iter()
            .map(|id| serde_json::json!({"kind": "review_comment", "id": id}))
            .collect::<Vec<_>>();
        let mut candidate = artifact(1);
        candidate["evidence"] = serde_json::Value::Array(evidence);
        assert!(parse_fixture(
            &response(vec![finding("suggestion", 1)], vec![candidate]),
            &parser_inputs(&ids, &[]),
        )
        .is_err());
    }

    #[test]
    fn collector_protocol_rejects_malformed_json() {
        assert!(parse_fixture(
            r#"{"findings":[],"followup_artifacts":[}"#,
            &parser_inputs(&[], &[]),
        )
        .is_err());
    }

    #[test]
    fn collector_protocol_rejects_count_overruns() {
        let too_many_findings = (0..=MAX_REVIEW_FINDINGS)
            .map(|_| finding("suggestion", 10))
            .collect();
        assert!(parse_fixture(
            &response(too_many_findings, vec![]),
            &parser_inputs(&[10], &[]),
        )
        .is_err());

        let too_many_artifacts = (0..=MAX_FOLLOWUP_ARTIFACTS).map(|_| artifact(10)).collect();
        assert!(parse_fixture(
            &response(vec![finding("suggestion", 10)], too_many_artifacts),
            &parser_inputs(&[10], &[]),
        )
        .is_err());
    }

    #[test]
    fn collector_protocol_rejects_each_overlong_artifact_string() {
        let inputs = parser_inputs(&[10], &[]);
        for field in ["non_blocking_reason", "affected_behavior"] {
            let mut candidate = artifact(10);
            candidate[field] = serde_json::Value::String("x".repeat(MAX_FOLLOWUP_TEXT_BYTES + 1));
            assert!(parse_fixture(
                &response(vec![finding("suggestion", 10)], vec![candidate]),
                &inputs,
            )
            .is_err());
        }
        for (field, component) in [
            ("concern", "failure_mode"),
            ("concern", "trigger_or_assumption"),
            ("desired_outcome", "observable_behavior"),
            ("desired_outcome", "observation_condition"),
        ] {
            let mut candidate = artifact(10);
            candidate[field][component] =
                serde_json::Value::String("x".repeat(MAX_FOLLOWUP_TEXT_BYTES + 1));
            assert!(parse_fixture(
                &response(vec![finding("suggestion", 10)], vec![candidate]),
                &inputs,
            )
            .is_err());
        }
    }

    #[test]
    fn collector_protocol_rejects_aggregate_artifact_overrun() {
        let mut candidate = artifact(10);
        for field in ["non_blocking_reason", "affected_behavior"] {
            candidate[field] = serde_json::Value::String("x".repeat(8_000));
        }
        for (field, component) in [
            ("concern", "failure_mode"),
            ("concern", "trigger_or_assumption"),
            ("desired_outcome", "observable_behavior"),
            ("desired_outcome", "observation_condition"),
        ] {
            candidate[field][component] = serde_json::Value::String("x".repeat(3_900));
        }
        candidate["verification_expectations"] = serde_json::Value::Array(
            (0..8)
                .map(|_| serde_json::Value::String("x".repeat(5_000)))
                .collect(),
        );
        assert!(parse_fixture(
            &response(vec![finding("suggestion", 10)], vec![candidate]),
            &parser_inputs(&[10], &[]),
        )
        .is_err());
    }

    #[test]
    fn collector_protocol_rejects_verification_list_bounds() {
        let inputs = parser_inputs(&[10], &[]);
        for expectations in [
            Vec::<serde_json::Value>::new(),
            (0..9).map(|_| serde_json::json!("test")).collect(),
        ] {
            let mut candidate = artifact(10);
            candidate["verification_expectations"] = serde_json::Value::Array(expectations);
            assert!(parse_fixture(
                &response(vec![finding("suggestion", 10)], vec![candidate]),
                &inputs,
            )
            .is_err());
        }

        let mut candidate = artifact(10);
        candidate["verification_expectations"] =
            serde_json::json!(["x".repeat(MAX_FOLLOWUP_TEXT_BYTES + 1)]);
        assert!(parse_fixture(
            &response(vec![finding("suggestion", 10)], vec![candidate]),
            &inputs,
        )
        .is_err());
    }

    fn live_request(
        dir: &std::path::Path,
        db: &std::path::Path,
        pr: i64,
        task_id: Option<i64>,
        force_fail: bool,
    ) -> CollectionRequest {
        let mut env_vars = Vec::new();
        if force_fail {
            env_vars.push(("FAKE_AGENT_COLLECTOR_FAIL".to_string(), "1".to_string()));
        }
        CollectionRequest {
            pr_number: pr,
            task_id,
            repo_slug: None,
            db_path: db.to_path_buf(),
            repo_dir: dir.to_path_buf(),
            agent_bin: Some(fake_agent_path().to_string_lossy().to_string()),
            bare_agent: true,
            collector_provider: AgentKind::for_model(CLASSIFIER_MODEL).unwrap().to_string(),
            collector_runner: AgentKind::for_model(CLASSIFIER_MODEL).unwrap().to_string(),
            collector_model: CLASSIFIER_MODEL.to_string(),
            collector_effort: CLASSIFIER_EFFORT.to_string(),
            role_assignment_id: None,
            codex_sandbox: "danger-full-access".to_string(),
            env_vars,
        }
    }

    async fn run_live(request: &CollectionRequest) -> Result<CollectionOutcome> {
        run_collection_with_inputs(request, synthetic_inputs(request.pr_number), 1000).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_positive_run_stores_findings_and_run_row() {
        let dir = setup_git_dir();
        let db = dir.path().join("q.db");
        let conn = db::open(&db).unwrap();
        conn.execute(
            "INSERT INTO role_assignments(
                 id,responsibility_key,task_id,pr_number,role,profile_id,
                 provider,runner,model,effort,pool_key,policy_generation,created_at)
             VALUES (77,'collector:pr:42',7,42,'collector','profile',
                     'claude','claude',?1,?2,'collector','g1',1)",
            rusqlite::params![CLASSIFIER_MODEL, CLASSIFIER_EFFORT],
        )
        .unwrap();
        drop(conn);
        let mut request = live_request(dir.path(), &db, 42, Some(7), false);
        request.role_assignment_id = Some(77);

        let outcome = run_live(&request)
            .await
            .expect("fake-agent should produce a valid collector response");
        assert_eq!(outcome.findings_count, 2);
        assert_eq!(outcome.status, RunStatus::Success);

        let conn = db::open(&db).unwrap();
        let run = review_findings::get_run(&conn, 42).unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Success);
        assert_eq!(run.findings_count, 2);
        assert!(run.error.is_none());
        assert_eq!(run.collector_version, COLLECTOR_VERSION);
        assert_eq!(run.role_assignment_id, Some(77));
        assert!(run.completed_at.is_some());

        let findings = review_findings::list_for_pr(&conn, 42).unwrap();
        assert_eq!(findings.len(), 2);

        // First finding: blocking + addressed, evidence -> review_comment#101.
        assert_eq!(findings[0].kind, "blocking");
        assert_eq!(findings[0].addressed_status.as_deref(), Some("addressed"));
        assert_eq!(findings[0].evidence[0].kind, "review_comment");
        assert_eq!(findings[0].evidence[0].id, 101);
        assert_eq!(findings[0].task_id, Some(7));
        assert!(findings[0].collector_model.is_some());
        assert_eq!(
            findings[0].collector_version.as_deref(),
            Some(COLLECTOR_VERSION)
        );

        // Second: suggestion, pushback accepted, evidence -> issue_comment#202.
        assert_eq!(findings[1].kind, "suggestion");
        assert!(findings[1].author_pushback);
        assert_eq!(findings[1].pushback_accepted, Some(true));
        assert_eq!(findings[1].evidence[0].kind, "issue_comment");
        assert_eq!(findings[1].evidence[0].id, 202);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_negative_classifier_failure_records_failed_run_no_findings() {
        let dir = setup_git_dir();
        let db = dir.path().join("q.db");
        let _ = db::open(&db).unwrap();
        // Insert a "merged/done" task marker to prove the collector never
        // touches the lifecycle. We just record the pre-state and re-assert
        // it after the failure.
        {
            let mut conn = db::open(&db).unwrap();
            let tid = quorum_core::tasks::create(
                &mut conn,
                "system",
                "test-task",
                None,
                0,
                None,
                None,
                None,
                None,
                100,
            )
            .unwrap();
            assert!(tid > 0);
            // NOTE: we don't run the full lifecycle here — we just need proof
            // that the task row exists unchanged after the collector fails.
        }

        let request = live_request(dir.path(), &db, 88, Some(1), true);
        let result = run_live(&request).await;

        assert!(
            result.is_err(),
            "collector must surface classifier failure to caller"
        );

        let conn = db::open(&db).unwrap();

        // Failed run row exists and is loudly retryable.
        let run = review_findings::get_run(&conn, 88).unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Failed);
        assert_eq!(run.findings_count, 0);
        assert!(run.error.is_some());

        // No findings written on failure — the analytics table stays clean.
        let findings = review_findings::list_for_pr(&conn, 88).unwrap();
        assert!(findings.is_empty());

        // errors table row logged.
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM errors WHERE source='review-collector'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(n >= 1);

        // Task row still exists unchanged — the collector NEVER touches tasks.
        let task_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM tasks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(task_count, 1, "collector must not delete/modify tasks");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_idempotent_retry_no_duplicates() {
        let dir = setup_git_dir();
        let db = dir.path().join("q.db");
        let _ = db::open(&db).unwrap();
        let request = live_request(dir.path(), &db, 200, Some(9), false);

        run_live(&request).await.unwrap();
        run_live(&request).await.unwrap();

        let conn = db::open(&db).unwrap();
        let run_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM review_collection_runs WHERE pr_number=?1",
                rusqlite::params![200i64],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(run_count, 1);

        let findings_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM review_findings WHERE pr_number=?1",
                rusqlite::params![200i64],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(findings_count, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_failure_then_retry_success_replaces_state() {
        let dir = setup_git_dir();
        let db = dir.path().join("q.db");
        let _ = db::open(&db).unwrap();

        // First attempt: force failure.
        let fail_request = live_request(dir.path(), &db, 301, Some(11), true);
        let _ = run_live(&fail_request).await;

        {
            let conn = db::open(&db).unwrap();
            let run = review_findings::get_run(&conn, 301).unwrap().unwrap();
            assert_eq!(run.status, RunStatus::Failed);
        }

        // Second attempt: success (no fail env). UPSERT flips the row.
        let ok_request = live_request(dir.path(), &db, 301, Some(11), false);
        run_live(&ok_request).await.expect("retry succeeds");

        let conn = db::open(&db).unwrap();
        let run = review_findings::get_run(&conn, 301).unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Success);
        assert_eq!(run.findings_count, 2);
        assert!(run.error.is_none());
    }

    // ------------------------------------------------------------------
    // Repo-targeting + pagination unit tests (#126). `gh api` does not
    // accept `-R`; the collector routes explicit repo overrides via
    // `GH_REPO` env and adds `--paginate --slurp` for list endpoints so
    // GitHub cannot silently truncate at page 1.
    // ------------------------------------------------------------------

    #[test]
    fn build_gh_api_args_never_uses_dash_r_flag() {
        // Regression for the installed-gh failure mode: `gh api -R owner/name`
        // → "unknown shorthand flag: R". If this constant slipped back into the
        // argv, every collector run would fail before the classifier boots.
        let args = build_gh_api_args("repos/{owner}/{repo}/pulls/42", false);
        assert!(
            !args.iter().any(|a| a == "-R" || a == "--repo"),
            "gh api argv must not carry -R/--repo (unsupported); saw {args:?}"
        );
        assert_eq!(args[0], "api");
        assert!(args.iter().any(|a| a == "repos/{owner}/{repo}/pulls/42"));
    }

    #[test]
    fn build_gh_api_args_paginates_list_endpoints() {
        let args = build_gh_api_args("repos/{owner}/{repo}/pulls/42/comments", true);
        assert!(
            args.iter().any(|a| a == "--paginate"),
            "list endpoints must paginate; saw {args:?}"
        );
        assert!(
            args.iter().any(|a| a == "--slurp"),
            "--slurp keeps every page in one JSON array; saw {args:?}"
        );
    }

    #[test]
    fn build_gh_api_args_single_object_skips_pagination() {
        // pulls/{pr} (metadata) is not a list — paginating a single-object
        // endpoint would still work but the --slurp shape would confuse the
        // downstream prompt that expects a plain object.
        let args = build_gh_api_args("repos/{owner}/{repo}/pulls/42", false);
        assert!(!args.iter().any(|a| a == "--paginate"));
        assert!(!args.iter().any(|a| a == "--slurp"));
    }

    #[test]
    fn gh_env_sets_gh_repo_when_repo_provided() {
        let env = gh_env(&Some("owner/name".into()));
        assert_eq!(env, vec![("GH_REPO".to_string(), "owner/name".to_string())]);
    }

    #[test]
    fn gh_env_empty_when_no_repo_override() {
        assert!(gh_env(&None).is_empty());
        assert!(gh_env(&Some(String::new())).is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fetch_failure_preserves_prior_good_findings() {
        // A subsequent collector run that fails at fetch (loud error) must NOT
        // wipe the prior successful findings — analytics stay authoritative
        // until a replacement good run lands.
        let dir = setup_git_dir();
        let db = dir.path().join("q.db");
        let _ = db::open(&db).unwrap();

        // Seed a good result for PR #77 via the classifier pipeline.
        let good = live_request(dir.path(), &db, 77, Some(3), false);
        run_live(&good).await.unwrap();

        {
            let conn = db::open(&db).unwrap();
            let findings = review_findings::list_for_pr(&conn, 77).unwrap();
            assert_eq!(findings.len(), 2, "seed findings should land");
        }

        // Now retry via the real fetch path against a bogus repo slug. Either
        // outcome is a loud fetch failure (gh missing → "gh: ..."; gh present
        // → 404). Both must record a failed run and preserve prior findings.
        // We point `agent_bin` at a nonexistent path so that even if fetch
        // somehow succeeded, the classifier spawn would still fail — either
        // way the test asserts the boundary: prior good rows survive.
        let retry = CollectionRequest {
            pr_number: 77,
            task_id: Some(3),
            repo_slug: Some(
                "quorum-collector-nonexistent-owner-t126/quorum-collector-nonexistent-repo-t126"
                    .into(),
            ),
            db_path: db.clone(),
            repo_dir: dir.path().to_path_buf(),
            agent_bin: Some("/nonexistent/quorum-fake-agent-t126".into()),
            bare_agent: true,
            collector_provider: AgentKind::for_model(CLASSIFIER_MODEL).unwrap().to_string(),
            collector_runner: AgentKind::for_model(CLASSIFIER_MODEL).unwrap().to_string(),
            collector_model: CLASSIFIER_MODEL.to_string(),
            collector_effort: CLASSIFIER_EFFORT.to_string(),
            role_assignment_id: None,
            codex_sandbox: "danger-full-access".to_string(),
            env_vars: vec![],
        };
        let result = run_collection(&retry).await;

        assert!(result.is_err(), "bogus target must produce loud failure");

        // Prior good findings survive — the replace_for_pr on the good path
        // is scoped to the success branch, so a failed retry never runs it.
        let conn = db::open(&db).unwrap();
        let findings = review_findings::list_for_pr(&conn, 77).unwrap();
        assert_eq!(
            findings.len(),
            2,
            "fetch/spawn failure must not clobber prior good analytics"
        );
        let run = review_findings::get_run(&conn, 77).unwrap().unwrap();
        assert_eq!(
            run.status,
            RunStatus::Failed,
            "retry attempt records a loud failed run"
        );
    }

    // ------------------------------------------------------------------
    // Executable CLI + shim-`gh` integration test (#126). Puts a Rust-built
    // `gh` shim on PATH and drives `quorum review-interpret --repo owner/name`
    // end-to-end. Proves: (a) `--repo` doesn't inject `-R` into `gh api`
    // (which would fail), (b) `--paginate --slurp` is applied to list
    // endpoints and the classifier sees late-page records verbatim.
    // ------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cli_review_interpret_with_repo_override_and_paginated_gh_shim() {
        // Build the shim gh binary from a small script — writes captured argv
        // + GH_REPO to a log, echoes multi-page slurped JSON for list endpoints.
        let shim_dir = tempfile::tempdir().unwrap();
        let log_path = shim_dir.path().join("gh-invocations.log");
        let shim_path = shim_dir.path().join("gh");
        let shim_script = format!(
            r#"#!/usr/bin/env bash
set -euo pipefail
echo "GH_REPO=${{GH_REPO:-}} ARGS=$*" >> "{log}"
# Fail loudly if the CLI ever passes -R to `gh api`.
if [[ "$1" == "api" ]]; then
  for a in "$@"; do
    if [[ "$a" == "-R" ]]; then
      echo "unknown shorthand flag: R" >&2
      exit 1
    fi
  done
fi
case "$*" in
  *"pulls/1/comments"*)
    # Two "pages" slurped into a single outer array — proves late-page
    # records survive. IDs 101 (page 1) and 102 (page 2 late record).
    echo '[[{{"id":101,"body":"first-page"}}],[{{"id":102,"body":"late-page"}}]]'
    ;;
  *"issues/1/comments"*)
    echo '[{{"id":202,"body":"suggestion"}}]'
    ;;
  *"pulls/1/reviews"*|*"pulls/1/commits"*)
    echo '[]'
    ;;
  *"pulls/1"*)
    echo '{{}}'
    ;;
  *"pr view"*|*"pr checks"*)
    echo 'ok'
    ;;
  *)
    echo '{{}}'
    ;;
esac
"#,
            log = log_path.display()
        );
        std::fs::write(&shim_path, shim_script).unwrap();
        let out = std::process::Command::new("chmod")
            .arg("+x")
            .arg(&shim_path)
            .output()
            .unwrap();
        assert!(out.status.success(), "chmod +x failed");

        // Isolated QUORUM_HOME so parallel tests don't collide on ~/.quorum.
        let home = tempfile::tempdir().unwrap();

        let orig_path = std::env::var_os("PATH").unwrap_or_default();
        let new_path = format!(
            "{}:{}",
            shim_dir.path().display(),
            orig_path.to_string_lossy()
        );

        let outcome = tokio::task::spawn_blocking(move || {
            assert_cmd::Command::cargo_bin("quorum")
                .unwrap()
                .env("PATH", &new_path)
                .env("QUORUM_HOME", home.path())
                .env("QUORUM_REPO", "shim-owner/shim-repo")
                .args([
                    "review-interpret",
                    "--pr",
                    "1",
                    "--repo",
                    "override-owner/override-repo",
                    "--agent-bin",
                    fake_agent_path().to_str().unwrap(),
                    "--json",
                ])
                .output()
                .unwrap()
        })
        .await
        .unwrap();

        assert!(
            outcome.status.success(),
            "quorum review-interpret exit {}: stdout={} stderr={}",
            outcome.status,
            String::from_utf8_lossy(&outcome.stdout),
            String::from_utf8_lossy(&outcome.stderr),
        );

        // Shim log proves:
        //  1. `--repo` was threaded via GH_REPO env, not `-R` on `gh api`.
        //  2. list endpoints used `--paginate --slurp`.
        let log = std::fs::read_to_string(&log_path).unwrap();
        assert!(
            log.contains("GH_REPO=override-owner/override-repo"),
            "log:\n{log}"
        );
        assert!(log.contains("api --paginate --slurp"), "log:\n{log}");
        // And never `-R` on any `gh api` invocation.
        for line in log.lines() {
            if line.contains("ARGS=api ") {
                assert!(!line.contains(" -R "), "gh api must not carry -R: {line}");
            }
        }
    }

    // ------------------------------------------------------------------
    // Fake-gh contract tests: no-checks vs genuine failure (#168).
    //
    // Repos without CI (e.g. BoostMyAgents) make `gh pr checks` exit 1
    // with "no checks reported" on stderr. This must normalize to a valid
    // collector input, NOT trigger retry/dead-letter. Genuine failures
    // (auth, network, rate-limit) must still propagate as errors.
    // ------------------------------------------------------------------

    /// Build a fake-gh shim script. `checks_behavior` is spliced into the
    /// `pr checks` case — it controls exit code and output.
    fn write_gh_shim(dir: &Path, checks_behavior: &str) -> PathBuf {
        let shim_path = dir.join("gh");
        let script = format!(
            r#"#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  *"pr checks"*)
    {checks}
    ;;
  *"pr view"*)
    echo '{{"changedFiles":1,"additions":5,"deletions":2,"title":"test"}}'
    ;;
  *"pulls/"*"/comments"*)
    echo '[[{{"id":101}}]]'
    ;;
  *"issues/"*"/comments"*)
    echo '[{{"id":202}}]'
    ;;
  *"pulls/"*"/reviews"*|*"pulls/"*"/commits"*)
    echo '[]'
    ;;
  *"pulls/"*)
    echo '{{}}'
    ;;
  *)
    echo '{{}}'
    ;;
esac
"#,
            checks = checks_behavior
        );
        std::fs::write(&shim_path, script).unwrap();
        std::process::Command::new("chmod")
            .arg("+x")
            .arg(&shim_path)
            .output()
            .unwrap();
        shim_path
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gh_checks_no_checks_reported_normalizes_to_valid_input() {
        // Repos without CI: `gh pr checks` exits 1, stderr = "no checks reported\n"
        let shim_dir = tempfile::tempdir().unwrap();
        write_gh_shim(shim_dir.path(), r#"echo "no checks reported" >&2; exit 1"#);

        let home = tempfile::tempdir().unwrap();
        let orig_path = std::env::var_os("PATH").unwrap_or_default();
        let new_path = format!(
            "{}:{}",
            shim_dir.path().display(),
            orig_path.to_string_lossy()
        );

        let outcome = tokio::task::spawn_blocking(move || {
            assert_cmd::Command::cargo_bin("quorum")
                .unwrap()
                .env("PATH", &new_path)
                .env("QUORUM_HOME", home.path())
                .env("QUORUM_REPO", "no-ci-owner/no-ci-repo")
                .args([
                    "review-interpret",
                    "--pr",
                    "1",
                    "--agent-bin",
                    fake_agent_path().to_str().unwrap(),
                    "--json",
                ])
                .output()
                .unwrap()
        })
        .await
        .unwrap();

        assert!(
            outcome.status.success(),
            "no-checks must not fail the collector; exit {}: stderr={}",
            outcome.status,
            String::from_utf8_lossy(&outcome.stderr),
        );

        // Verify the classifier ran and produced findings (proves the pipeline
        // continued past the checks fetch rather than erroring out).
        let stdout = String::from_utf8_lossy(&outcome.stdout);
        assert!(
            stdout.contains("\"status\":\"success\"") || stdout.contains("findings"),
            "collector should succeed with no-checks input; stdout={stdout}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gh_checks_genuine_failure_still_errors() {
        // Genuine failure: `gh pr checks` exits 1 with auth/network error
        let shim_dir = tempfile::tempdir().unwrap();
        write_gh_shim(
            shim_dir.path(),
            r#"echo "HTTP 401: Bad credentials" >&2; exit 1"#,
        );

        let home = tempfile::tempdir().unwrap();
        let orig_path = std::env::var_os("PATH").unwrap_or_default();
        let new_path = format!(
            "{}:{}",
            shim_dir.path().display(),
            orig_path.to_string_lossy()
        );

        let outcome = tokio::task::spawn_blocking(move || {
            assert_cmd::Command::cargo_bin("quorum")
                .unwrap()
                .env("PATH", &new_path)
                .env("QUORUM_HOME", home.path())
                .env("QUORUM_REPO", "no-ci-owner/no-ci-repo")
                .args([
                    "review-interpret",
                    "--pr",
                    "1",
                    "--agent-bin",
                    fake_agent_path().to_str().unwrap(),
                    "--json",
                ])
                .output()
                .unwrap()
        })
        .await
        .unwrap();

        // Genuine auth failure must propagate — exit nonzero or JSON with failed status
        let stdout = String::from_utf8_lossy(&outcome.stdout);
        let stderr = String::from_utf8_lossy(&outcome.stderr);
        let failed = !outcome.status.success()
            || stdout.contains("\"status\":\"failed\"")
            || stderr.contains("Bad credentials");
        assert!(
            failed,
            "genuine gh failure must propagate as error; exit={} stdout={stdout} stderr={stderr}",
            outcome.status,
        );
    }
}
