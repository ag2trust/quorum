//! Inference forwarder: model access for a sandboxed run without the
//! upstream credential.
//!
//! A loopback HTTP listener that mirrors the Messages API. A managed run
//! points its model client at it and presents its run capability where the
//! API key would go. Each request is authorized by the daemon endpoint,
//! checked against the request policy, and re-sent upstream with the
//! operator's credential, which is read from this process's environment and
//! never sent to the caller or written to a log.
//!
//! Like `agent-mcp --listen`, this process has no database path: run
//! authority comes only from the daemon endpoint, on every request.

mod policy;
mod usage;

use axum::body::{Body, Bytes};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode, Uri};
use axum::response::Response;
use hyper::body::{Body as HttpBody, Frame, Incoming};
use policy::{printable, Policy};
use quorum_core::error::{QuorumError, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::future::Future;
use std::hash::BuildHasher;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpStream, UnixStream};
use tokio_rustls::rustls;
use usage::{Counts, Ledger, Meter, Refusal, Reservation, Slot};

const PROTOCOL_VERSION: u8 = 1;
const MAX_FRAME_BYTES: usize = 64 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CAPABILITY_BYTES: usize = 128;
const MAX_ENV_BYTES: usize = 4096;

const MESSAGES_PATH: &str = "/v1/messages";
const DEFAULT_UPSTREAM: &str = "https://api.anthropic.com";
const DEFAULT_API_VERSION: &str = "2023-06-01";

/// Bound on one request body. Requests are text only (the policy admits no
/// images or documents), and a million-token conversation is about 4 MiB of
/// JSON, so this leaves a twofold margin. A body is read only after its run
/// is authorized and holds one of its in-flight slots. Worst case, every
/// connection buffers one: the parsed body costs about three times its size
/// and its re-serialized copy once more, roughly 32 MiB a connection and
/// 512 MiB in all, with any one run limited to its in-flight slots' share.
const MAX_REQUEST_BODY_BYTES: usize = 8 * 1024 * 1024;
/// Bound on an upstream response that is buffered rather than streamed.
const MAX_BUFFERED_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
/// Read-buffer bound for one request head. A head is refused once this much
/// of it is buffered without completing, so a head slightly over can pass.
const MAX_HEAD_BYTES: usize = 64 * 1024;
/// Open connections served at once; further ones wait in the listen backlog.
/// HTTP/1.1 carries one request per connection at a time, so this also
/// bounds concurrent upstream requests and buffered bodies.
const MAX_HTTP_CONNECTIONS: usize = 16;
/// Daemon endpoint exchanges in flight at once, kept well under the
/// endpoint's own connection slots.
const MAX_ENDPOINT_EXCHANGES: usize = 8;
/// Further endpoint exchanges kept for re-checking open streams. With the
/// request-path exchanges this stays under the endpoint's connection slots.
const MAX_RECHECK_EXCHANGES: usize = 4;
/// A connection must deliver a complete request head within this bound.
const HTTP_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// An authorized request must deliver its whole body within this bound.
const BODY_READ_TIMEOUT: Duration = Duration::from_secs(30);
/// Bound on reaching the upstream, including its TLS handshake.
const UPSTREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Default bound on the upstream's silence: before its response head, and
/// between chunks of a streamed response. A large non-streamed reply can
/// take minutes to begin.
const DEFAULT_UPSTREAM_TIMEOUT_SECS: u64 = 600;
/// Default bound on a caller that stops reading its response.
const DEFAULT_CLIENT_STALL_TIMEOUT_SECS: u64 = 60;
/// Default interval at which a stream's run is checked again with the
/// daemon; a stream outlives its run by at most this long.
const DEFAULT_AUTH_RECHECK_SECS: u64 = 60;
/// Default number of requests one run may have in flight.
const DEFAULT_RUN_MAX_IN_FLIGHT: usize = 2;
/// A connection this old takes no further requests; the one in flight, if
/// any, may finish.
const CONNECTION_LIFETIME: Duration = Duration::from_secs(60 * 60);
/// A connection is dropped outright at this age: its lifetime plus the
/// longest a single request can legitimately still take.
const CONNECTION_HARD_LIFETIME: Duration = Duration::from_secs(2 * 60 * 60 + 15 * 60);
/// Bound on one streamed response from first byte to last.
const MAX_STREAM_DURATION: Duration = Duration::from_secs(60 * 60);
/// After SIGINT/SIGTERM, open connections get this long to finish.
const HTTP_SHUTDOWN_DRAIN: Duration = Duration::from_secs(5);
/// Runs whose usage is tracked at once.
const MAX_TRACKED_RUNS: usize = 4096;
/// Idle runs re-checked with the daemon, off the request path, when the
/// ledger fills.
const LEDGER_SWEEP_BATCH: usize = 64;

/// Where requests are re-sent.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Upstream {
    tls: bool,
    host: String,
    port: u16,
    /// Path prefix without a trailing slash; empty for a bare origin.
    prefix: String,
}

impl Upstream {
    /// Parse an operator-supplied base URL. Plain HTTP is accepted only for
    /// a loopback host, so the credential never crosses a network in clear.
    fn parse(url: &str) -> std::result::Result<Self, String> {
        let uri: Uri = url.parse().map_err(|_| "is not a valid URL".to_string())?;
        let tls = match uri.scheme_str() {
            Some("https") => true,
            Some("http") => false,
            _ => return Err("must start with https:// or http://".into()),
        };
        let authority = uri.authority().ok_or("has no host")?;
        if authority.as_str().contains('@') {
            return Err("must not contain credentials".into());
        }
        if uri.query().is_some() {
            return Err("must not contain a query".into());
        }
        let host = authority
            .host()
            .trim_start_matches('[')
            .trim_end_matches(']');
        if host.is_empty() {
            return Err("has no host".into());
        }
        if !tls && !is_loopback_host(host) {
            return Err("may use http:// only for a loopback host".into());
        }
        Ok(Self {
            tls,
            host: host.to_string(),
            port: authority.port_u16().unwrap_or(if tls { 443 } else { 80 }),
            prefix: uri.path().trim_end_matches('/').to_string(),
        })
    }

    fn host_header(&self) -> String {
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        match (self.tls, self.port) {
            (true, 443) | (false, 80) => host,
            (_, port) => format!("{host}:{port}"),
        }
    }
}

fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

struct Forwarder {
    endpoint: PathBuf,
    repository: String,
    upstream: Upstream,
    /// The upstream credential, marked sensitive so it is never rendered.
    credential: HeaderValue,
    policy: Policy,
    upstream_timeout: Duration,
    client_stall_timeout: Duration,
    auth_recheck: Duration,
    /// Per-process key for run tags, so a tag cannot be matched against a
    /// guessed run id outside this process.
    tag_key: std::collections::hash_map::RandomState,
    /// Whether a ledger sweep is already running.
    sweeping: AtomicBool,
    ledger: Arc<Ledger>,
    endpoint_slots: tokio::sync::Semaphore,
    /// Endpoint exchanges reserved for re-checking open streams, so a burst
    /// of new requests cannot starve them.
    recheck_slots: tokio::sync::Semaphore,
    tls: Option<tokio_rustls::TlsConnector>,
}

/// The live run a request was authorized for.
#[derive(Debug, Clone)]
struct Run {
    task_id: i64,
    role: String,
    /// Keyed tag for log lines, stable within one forwarder process. The
    /// run id is the bearer credential, so it is never logged itself.
    tag: String,
}

#[derive(Debug, PartialEq, Eq)]
enum AuthFailure {
    Rejected,
    Unavailable,
}

#[derive(Debug, Deserialize)]
struct EndpointResponse {
    version: u8,
    ok: bool,
    #[serde(default)]
    result: Option<EndpointInventory>,
    #[serde(default)]
    error: Option<EndpointError>,
}

#[derive(Debug, Deserialize)]
struct EndpointInventory {
    #[serde(rename = "type")]
    kind: String,
    repository: String,
    task_id: i64,
    role: String,
}

#[derive(Debug, Deserialize)]
struct EndpointError {
    code: String,
}

impl Forwarder {
    /// Ask the daemon endpoint whether `capability` names a live run of this
    /// repository. Nothing is cached: a revoked or finished run is refused on
    /// its next request.
    async fn authorize(&self, capability: &str) -> std::result::Result<Run, AuthFailure> {
        self.authorize_with(capability, &self.endpoint_slots).await
    }

    async fn authorize_with(
        &self,
        capability: &str,
        slots: &tokio::sync::Semaphore,
    ) -> std::result::Result<Run, AuthFailure> {
        let body = serde_json::to_vec(&json!({
            "version": PROTOCOL_VERSION,
            "capability": capability,
            "operation": {"type": "inventory"},
        }))
        .map_err(|_| AuthFailure::Unavailable)?;
        let exchange = async {
            let _slot = slots.acquire().await;
            let mut stream = UnixStream::connect(&self.endpoint).await?;
            stream.write_all(&(body.len() as u32).to_be_bytes()).await?;
            stream.write_all(&body).await?;
            let mut prefix = [0_u8; 4];
            stream.read_exact(&mut prefix).await?;
            let length = u32::from_be_bytes(prefix) as usize;
            if length == 0 || length > MAX_FRAME_BYTES {
                return Err(std::io::Error::other("invalid response frame"));
            }
            let mut response = vec![0; length];
            stream.read_exact(&mut response).await?;
            Ok::<_, std::io::Error>(response)
        };
        let response = tokio::time::timeout(IO_TIMEOUT, exchange)
            .await
            .map_err(|_| AuthFailure::Unavailable)?
            .map_err(|_| AuthFailure::Unavailable)?;
        let response: EndpointResponse =
            serde_json::from_slice(&response).map_err(|_| AuthFailure::Unavailable)?;
        if response.version != PROTOCOL_VERSION {
            return Err(AuthFailure::Unavailable);
        }
        match (response.ok, response.result, response.error) {
            (true, Some(inventory), None)
                if inventory.kind == "inventory"
                    && inventory.repository == self.repository
                    && inventory.task_id > 0
                    && matches!(inventory.role.as_str(), "worker" | "reviewer" | "planner") =>
            {
                Ok(Run {
                    task_id: inventory.task_id,
                    role: inventory.role,
                    tag: format!("{:016x}", self.tag_key.hash_one(capability)),
                })
            }
            (false, None, Some(error)) if error.code == "unauthorized" => {
                Err(AuthFailure::Rejected)
            }
            _ => Err(AuthFailure::Unavailable),
        }
    }

    /// Re-check idle tracked runs with the daemon and stop tracking those it
    /// no longer accepts. Runs on its own task, one at a time, so no request
    /// waits on it; a run that is still live keeps its count.
    fn sweep_in_background(self: &Arc<Self>) {
        if self.sweeping.swap(true, Ordering::AcqRel) {
            return;
        }
        let forwarder = Arc::clone(self);
        tokio::spawn(async move {
            for tracked in forwarder.ledger.least_recent_idle(LEDGER_SWEEP_BATCH) {
                if matches!(
                    forwarder.authorize(&tracked).await,
                    Err(AuthFailure::Rejected)
                ) {
                    forwarder.ledger.forget(&tracked);
                }
            }
            forwarder.sweeping.store(false, Ordering::Release);
        });
    }
}

/// An error in the mirrored API's own shape, so its clients report it well.
/// `x-should-retry: false` stops those clients from retrying a refusal that
/// cannot succeed.
fn error_response(status: StatusCode, kind: &str, message: &str, retry: bool) -> Response {
    let body = json!({"type": "error", "error": {"type": kind, "message": message}});
    let mut response = Response::new(Body::from(body.to_string()));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    if !retry {
        headers.insert("x-should-retry", HeaderValue::from_static("false"));
    }
    response
}

fn refusal(status: StatusCode, kind: &str, message: &str) -> Response {
    error_response(status, kind, message, false)
}

/// Mark a response sent without reading the request body: the unread body
/// makes the connection unusable for another request, so it is closed
/// instead of waiting on a peer that may never send the rest.
fn closing(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CONNECTION, HeaderValue::from_static("close"));
    response
}

/// The run capability a request presents: `x-api-key`, or
/// `Authorization: Bearer`. Exactly one well-formed value per header, and
/// the same value if both are sent. The shape rules are the daemon
/// endpoint's own plus no whitespace.
fn run_capability(headers: &HeaderMap) -> Option<&str> {
    fn single<'a>(headers: &'a HeaderMap, name: &HeaderName) -> Option<Option<&'a str>> {
        let mut values = headers.get_all(name).iter();
        let Some(value) = values.next() else {
            return Some(None);
        };
        if values.next().is_some() {
            return None;
        }
        value.to_str().ok().map(Some)
    }
    let api_key = single(headers, &HeaderName::from_static("x-api-key"))?;
    let bearer = match single(headers, &header::AUTHORIZATION)? {
        Some(value) => {
            let (scheme, capability) = value.split_once(' ')?;
            if !scheme.eq_ignore_ascii_case("Bearer") {
                return None;
            }
            Some(capability)
        }
        None => None,
    };
    let capability = match (api_key, bearer) {
        (Some(key), Some(bearer)) if key == bearer => key,
        (Some(key), None) => key,
        (None, Some(bearer)) => bearer,
        _ => return None,
    };
    let well_formed = !capability.is_empty()
        && capability.len() <= MAX_CAPABILITY_BYTES
        && !capability
            .chars()
            .any(|character| character == '\0' || character.is_whitespace());
    well_formed.then_some(capability)
}

/// DNS-rebinding guard: the request must name a loopback host.
fn names_loopback_host(headers: &HeaderMap) -> bool {
    let mut values = headers.get_all(header::HOST).iter();
    let (Some(value), None) = (values.next(), values.next()) else {
        return false;
    };
    let Ok(authority) = value
        .to_str()
        .map_err(|_| ())
        .and_then(|host| host.parse::<axum::http::uri::Authority>().map_err(|_| ()))
    else {
        return false;
    };
    is_loopback_host(
        authority
            .host()
            .trim_start_matches('[')
            .trim_end_matches(']'),
    )
}

#[derive(Debug, PartialEq, Eq)]
enum ReadFailure {
    TooLarge,
    Failed,
}

/// Collect a body, refusing it as soon as it exceeds `limit`.
async fn read_limited<B>(mut body: B, limit: usize) -> std::result::Result<Bytes, ReadFailure>
where
    B: HttpBody<Data = Bytes> + Unpin,
{
    let mut collected: Vec<u8> = Vec::new();
    loop {
        let frame = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
        match frame {
            None => return Ok(collected.into()),
            Some(Err(_)) => return Err(ReadFailure::Failed),
            Some(Ok(frame)) => {
                if let Some(data) = frame.data_ref() {
                    if collected.len() + data.len() > limit {
                        return Err(ReadFailure::TooLarge);
                    }
                    collected.extend_from_slice(data);
                }
            }
        }
    }
}

/// One metadata-only log line per authorized request. Never a prompt, a
/// completion, a header value, or either credential.
struct RequestLog {
    run: Run,
    model: Option<String>,
}

impl RequestLog {
    fn emit(&self, status: u16, counts: Counts, run_total: Option<u64>) {
        let model = self.model.as_deref().map_or("-".into(), printable);
        let total = run_total.map_or("-".into(), |total| total.to_string());
        eprintln!(
            "quorum inference-forwarder: run={} task={} role={} model={} status={} \
             input_tokens={} output_tokens={} run_total_tokens={}",
            self.run.tag,
            self.run.task_id,
            self.run.role,
            model,
            status,
            counts.input_total(),
            counts.output_total(),
            total,
        );
    }

    fn refuse(&self, status: StatusCode, kind: &str, message: &str) -> Response {
        self.emit(status.as_u16(), Counts::default(), None);
        refusal(status, kind, message)
    }
}

async fn handle(forwarder: Arc<Forwarder>, request: Request<Body>) -> Response {
    let (parts, body) = request.into_parts();
    if !names_loopback_host(&parts.headers) {
        return closing(refusal(
            StatusCode::FORBIDDEN,
            "permission_error",
            "host not allowed",
        ));
    }
    // Any query is ignored: nothing from it is forwarded.
    if parts.method != Method::POST || parts.uri.path() != MESSAGES_PATH {
        return closing(refusal(
            StatusCode::NOT_FOUND,
            "not_found_error",
            "not found",
        ));
    }
    let Some(capability) = run_capability(&parts.headers) else {
        return closing(refusal(
            StatusCode::UNAUTHORIZED,
            "authentication_error",
            "run authority rejected",
        ));
    };
    let run = match forwarder.authorize(capability).await {
        Ok(run) => run,
        Err(AuthFailure::Rejected) => {
            return closing(refusal(
                StatusCode::UNAUTHORIZED,
                "authentication_error",
                "run authority rejected",
            ))
        }
        Err(AuthFailure::Unavailable) => {
            return closing(error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "api_error",
                "run authority could not be checked",
                true,
            ))
        }
    };
    let mut log = RequestLog { run, model: None };
    // Taken before the body is read and held until the response is
    // delivered or abandoned, so one run cannot occupy the listener.
    let slot = match forwarder.ledger.enter(capability) {
        Ok(slot) => slot,
        Err(Refusal::Busy) => {
            return closing(log.refuse(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limit_error",
                "this run already has its maximum number of requests in flight",
            ))
        }
        Err(Refusal::CapReached) => return closing(log.refuse(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limit_error",
            "this run has used its token allowance; no further model requests are available to it",
        )),
        Err(Refusal::Full) => {
            forwarder.sweep_in_background();
            log.emit(503, Counts::default(), None);
            return closing(error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "overloaded_error",
                "forwarder is tracking its maximum number of runs",
                true,
            ));
        }
    };

    let declared_length = parts
        .headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    let too_large = |log: &RequestLog| {
        closing(log.refuse(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request_too_large",
            "request body is too large",
        ))
    };
    if declared_length.is_some_and(|length| length > MAX_REQUEST_BODY_BYTES as u64) {
        return too_large(&log);
    }
    let bytes = match tokio::time::timeout(
        BODY_READ_TIMEOUT,
        read_limited(body, MAX_REQUEST_BODY_BYTES),
    )
    .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(ReadFailure::TooLarge)) => return too_large(&log),
        Ok(Err(ReadFailure::Failed)) => {
            return log.refuse(
                StatusCode::BAD_REQUEST,
                "invalid_request_error",
                "request body could not be read",
            )
        }
        Err(_) => {
            return closing(log.refuse(
                StatusCode::REQUEST_TIMEOUT,
                "invalid_request_error",
                "request body was not received in time",
            ))
        }
    };
    let invalid = |log: &RequestLog, message: &str| {
        log.refuse(StatusCode::BAD_REQUEST, "invalid_request_error", message)
    };
    let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
        return invalid(&log, "request body must be valid JSON");
    };
    drop(bytes);
    let admitted = match forwarder.policy.admit(&value) {
        Ok(admitted) => admitted,
        Err(message) => return invalid(&log, &message),
    };
    log.model = Some(admitted.model);
    let mut beta_values = Vec::new();
    for value in parts.headers.get_all("anthropic-beta") {
        match value.to_str() {
            Ok(value) => beta_values.push(value),
            Err(_) => return invalid(&log, "anthropic-beta is malformed"),
        }
    }
    let betas = match forwarder.policy.admit_betas(beta_values.into_iter()) {
        Ok(betas) => betas,
        Err(message) => return invalid(&log, &message),
    };
    let api_version = match parts.headers.get("anthropic-version") {
        None => HeaderValue::from_static(DEFAULT_API_VERSION),
        Some(value)
            if !value.is_empty()
                && value.len() <= 32
                && value
                    .as_bytes()
                    .iter()
                    .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'-') =>
        {
            value.clone()
        }
        Some(_) => return invalid(&log, "anthropic-version is malformed"),
    };

    // The forwarded body is the one that was checked: re-serialized from the
    // parsed value, so no second reading of the caller's bytes (duplicate
    // keys, for one) can reach the upstream.
    let Ok(outbound) = serde_json::to_vec(&value) else {
        return invalid(&log, "request body must be valid JSON");
    };
    drop(value);

    // Hold the most this request could cost before anything is sent. When
    // the upstream may compact the conversation it reads the input a second
    // time to summarize it, so the input part of the hold is doubled.
    let input_bound = usage::estimated_input_tokens(outbound.len());
    let input_bound = if admitted.compaction {
        input_bound.saturating_mul(2)
    } else {
        input_bound
    };
    let Some(reservation) = forwarder
        .ledger
        .reserve(capability, input_bound.saturating_add(admitted.max_tokens))
    else {
        return log.refuse(
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limit_error",
            "this run's remaining token allowance cannot cover this request's \
             max_tokens plus its estimated input",
        );
    };
    let charge = Charge {
        reservation,
        max_tokens: admitted.max_tokens,
        compaction: admitted.compaction,
        _slot: slot,
    };
    let response = match forwarder
        .send(outbound, api_version, betas.as_deref())
        .await
    {
        Ok(response) => response,
        Err(failure) => {
            let (status, message, sent) = match failure {
                UpstreamFailure::Unreachable => (
                    StatusCode::BAD_GATEWAY,
                    "upstream could not be reached",
                    false,
                ),
                UpstreamFailure::ConnectTimedOut => (
                    StatusCode::GATEWAY_TIMEOUT,
                    "upstream could not be reached in time",
                    false,
                ),
                UpstreamFailure::Lost => (
                    StatusCode::BAD_GATEWAY,
                    "upstream closed without answering",
                    true,
                ),
                UpstreamFailure::TimedOut => (
                    StatusCode::GATEWAY_TIMEOUT,
                    "upstream did not answer in time",
                    true,
                ),
            };
            // A request that was sent may still be served and billed, so its
            // hold is charged; one that never left costs nothing.
            let run_total = if sent {
                charge.reservation.keep()
            } else {
                charge.reservation.release()
            };
            log.emit(status.as_u16(), Counts::default(), Some(run_total));
            return error_response(status, "api_error", message, true);
        }
    };
    forwarder.relay(response, charge, capability, log).await
}

/// What one admitted request holds: tokens of its run's allowance, and one
/// of the run's in-flight slots.
struct Charge {
    reservation: Reservation,
    max_tokens: u64,
    compaction: bool,
    _slot: Slot,
}

impl Charge {
    /// Close the hold against what the upstream reported and return the
    /// run's settled total. A response without closing usage is charged the
    /// whole hold. So is any response to a request that allowed compaction
    /// unless it itemized its steps: a compaction step is billed outside the
    /// top-level counts, and without the itemization there is no telling
    /// whether one ran.
    fn close(self, meter: &Meter) -> u64 {
        let accounted = !self.compaction || meter.itemized();
        if meter.final_seen() && accounted {
            self.reservation.settle(meter.billed())
        } else {
            self.reservation.keep()
        }
    }
}

#[derive(Debug)]
enum UpstreamFailure {
    /// No connection was made; nothing was sent.
    Unreachable,
    /// Connecting took too long; nothing was sent.
    ConnectTimedOut,
    /// The request was sent and the connection ended without a response.
    Lost,
    /// The request was sent and no response head arrived in time.
    TimedOut,
}

impl Forwarder {
    /// Send one checked request upstream on its own connection and return
    /// the response head. Only the headers built here are sent: the caller's
    /// credential and every other inbound header stay behind.
    async fn send(
        &self,
        body: Vec<u8>,
        api_version: HeaderValue,
        betas: Option<&str>,
    ) -> std::result::Result<hyper::Response<Incoming>, UpstreamFailure> {
        let mut request = hyper::Request::builder()
            .method(Method::POST)
            .uri(format!("{}{MESSAGES_PATH}", self.upstream.prefix))
            .header(header::HOST, self.upstream.host_header())
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT_ENCODING, "identity")
            .header(header::USER_AGENT, "quorum-inference-forwarder")
            .header("anthropic-version", api_version)
            .header("x-api-key", self.credential.clone());
        if let Some(betas) = betas {
            request = request.header("anthropic-beta", betas);
        }
        let request = request
            .body(Body::from(body))
            .map_err(|_| UpstreamFailure::Unreachable)?;

        let connect = async {
            let stream = TcpStream::connect((self.upstream.host.as_str(), self.upstream.port))
                .await
                .map_err(|_| UpstreamFailure::Unreachable)?;
            let _ = stream.set_nodelay(true);
            match &self.tls {
                None => Ok(Connection::Plain(stream)),
                Some(connector) => {
                    let name = rustls::pki_types::ServerName::try_from(self.upstream.host.clone())
                        .map_err(|_| UpstreamFailure::Unreachable)?;
                    connector
                        .connect(name, stream)
                        .await
                        .map(|stream| Connection::Tls(Box::new(stream)))
                        .map_err(|_| UpstreamFailure::Unreachable)
                }
            }
        };
        let connection = tokio::time::timeout(UPSTREAM_CONNECT_TIMEOUT, connect)
            .await
            .map_err(|_| UpstreamFailure::ConnectTimedOut)??;
        let exchange = async {
            match connection {
                Connection::Plain(stream) => exchange(stream, request).await,
                Connection::Tls(stream) => exchange(*stream, request).await,
            }
        };
        tokio::time::timeout(self.upstream_timeout, exchange)
            .await
            .map_err(|_| UpstreamFailure::TimedOut)?
    }

    /// Turn the upstream's response into the caller's, reconciling the
    /// request's hold with the usage the upstream reports.
    async fn relay(
        self: &Arc<Self>,
        response: hyper::Response<Incoming>,
        charge: Charge,
        capability: &str,
        log: RequestLog,
    ) -> Response {
        let (parts, body) = response.into_parts();
        let status = parts.status;
        let content_type = parts.headers.get(header::CONTENT_TYPE).cloned();
        let is_event_stream = content_type
            .as_ref()
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/event-stream"));

        let mut relayed = Response::new(Body::empty());
        *relayed.status_mut() = status;
        // Only headers a client needs; nothing identifying the upstream account.
        for name in ["content-type", "request-id", "retry-after"] {
            if let Some(value) = parts.headers.get(name) {
                relayed
                    .headers_mut()
                    .insert(HeaderName::from_static(name), value.clone());
            }
        }

        if status.is_success() && is_event_stream {
            relayed
                .headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
            *relayed.body_mut() = Body::new(MeteredStream {
                inner: Some(body),
                idle: Box::pin(tokio::time::sleep(self.upstream_timeout)),
                deadline: Box::pin(tokio::time::sleep(MAX_STREAM_DURATION)),
                idle_timeout: self.upstream_timeout,
                recheck: Box::pin(tokio::time::sleep(self.auth_recheck)),
                checking: None,
                unverified: Box::pin(tokio::time::sleep(self.auth_recheck * 2)),
                forwarder: Arc::clone(self),
                capability: capability.to_string(),
                meter: Meter::default(),
                charge: Some(charge),
                log,
            });
            return relayed;
        }

        let read = tokio::time::timeout(
            self.upstream_timeout,
            read_limited(body, MAX_BUFFERED_RESPONSE_BYTES),
        )
        .await;
        // An error status means the upstream did no billable work. A
        // successful response that cannot be read to its usage is charged
        // the whole hold.
        let unread = |charge: Charge| {
            if status.is_success() {
                charge.reservation.keep()
            } else {
                charge.reservation.release()
            }
        };
        let bytes = match read {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(_)) => {
                log.emit(502, Counts::default(), Some(unread(charge)));
                return error_response(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "upstream response could not be read",
                    true,
                );
            }
            Err(_) => {
                log.emit(504, Counts::default(), Some(unread(charge)));
                return error_response(
                    StatusCode::GATEWAY_TIMEOUT,
                    "api_error",
                    "upstream did not answer in time",
                    true,
                );
            }
        };
        // A response the upstream gave about the operator's credential is
        // the operator's problem and is not relayed; neither is any body
        // that repeats the credential.
        if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN)
            || usage::contains(&bytes, self.credential.as_bytes())
        {
            log.emit(502, Counts::default(), Some(unread(charge)));
            return refusal(
                StatusCode::BAD_GATEWAY,
                "api_error",
                "upstream refused the forwarder's own request",
            );
        }
        let mut meter = Meter::default();
        let run_total = if status.is_success() {
            if let Some(usage) = serde_json::from_slice::<Value>(&bytes)
                .ok()
                .as_ref()
                .and_then(|value| value.get("usage"))
            {
                meter.observe_final(usage);
            }
            charge.close(&meter)
        } else {
            charge.reservation.release()
        };
        log.emit(status.as_u16(), meter.counts(), Some(run_total));
        *relayed.body_mut() = Body::from(bytes);
        relayed
    }
}

enum Connection {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

async fn exchange<I>(
    io: I,
    request: hyper::Request<Body>,
) -> std::result::Result<hyper::Response<Incoming>, UpstreamFailure>
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(io))
            .await
            .map_err(|_| UpstreamFailure::Unreachable)?;
    // The connection ends when the response body is finished or dropped.
    tokio::spawn(async move {
        let _ = connection.await;
    });
    sender
        .send_request(request)
        .await
        .map_err(|_| UpstreamFailure::Lost)
}

/// A streamed upstream response passed through chunk by chunk as it arrives,
/// with its usage events counted on the way past. A stream the upstream
/// stops feeding is ended with one error event instead of hanging.
struct MeteredStream {
    inner: Option<Incoming>,
    idle: Pin<Box<tokio::time::Sleep>>,
    deadline: Pin<Box<tokio::time::Sleep>>,
    idle_timeout: Duration,
    /// When the stream's run is next checked with the daemon.
    recheck: Pin<Box<tokio::time::Sleep>>,
    /// A check in progress.
    checking: Option<Pin<Box<dyn Future<Output = Option<AuthFailure>> + Send>>>,
    /// Fires when the run has gone too long without a successful check.
    unverified: Pin<Box<tokio::time::Sleep>>,
    forwarder: Arc<Forwarder>,
    capability: String,
    meter: Meter,
    /// Held until the stream is dropped, however it ends.
    charge: Option<Charge>,
    log: RequestLog,
}

impl MeteredStream {
    fn fail(&mut self, message: &str) -> Frame<Bytes> {
        self.inner = None;
        let event = json!({"type": "error", "error": {"type": "api_error", "message": message}});
        Frame::data(Bytes::from(format!("event: error\ndata: {event}\n\n")))
    }
}

impl HttpBody for MeteredStream {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        if this.inner.is_none() {
            return Poll::Ready(None);
        }
        // A stream must not outlive its run: ask the daemon again at each
        // interval and end the stream once the run is rejected. This fails
        // closed: a stream whose run has not been confirmed live for two
        // intervals is ended too, whether the daemon is unreachable or the
        // checks are not completing.
        if this.checking.is_none() && this.recheck.as_mut().poll(cx).is_ready() {
            let forwarder = Arc::clone(&this.forwarder);
            let capability = this.capability.clone();
            this.checking = Some(Box::pin(async move {
                forwarder
                    .authorize_with(&capability, &forwarder.recheck_slots)
                    .await
                    .err()
            }));
        }
        if let Some(check) = this.checking.as_mut() {
            if let Poll::Ready(outcome) = check.as_mut().poll(cx) {
                this.checking = None;
                let now = tokio::time::Instant::now();
                let interval = this.forwarder.auth_recheck;
                match outcome {
                    Some(AuthFailure::Rejected) => {
                        return Poll::Ready(Some(Ok(this.fail("run is no longer live"))));
                    }
                    Some(AuthFailure::Unavailable) => {}
                    None => this.unverified.as_mut().reset(now + interval * 2),
                }
                this.recheck.as_mut().reset(now + interval);
                let _ = this.recheck.as_mut().poll(cx);
            }
        }
        if this.unverified.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Some(Ok(this.fail("run could not be confirmed live"))));
        }
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(None);
        };
        match Pin::new(inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                let Ok(data) = frame.into_data() else {
                    // Trailers carry nothing a client of this API uses.
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                };
                if this.meter.feed_events(&data) && this.meter.input_known() {
                    // The real input replaces its estimate; output is still
                    // held at its ceiling until the closing usage arrives.
                    if let Some(charge) = this.charge.as_mut() {
                        let counts = this.meter.counts();
                        let known = counts
                            .input_total()
                            .saturating_add(counts.output_total().max(charge.max_tokens));
                        // With compaction the reported input may leave out
                        // a step, so the hold is only ever raised.
                        charge.reservation.hold(if charge.compaction {
                            known.max(charge.reservation.held())
                        } else {
                            known
                        });
                    }
                }
                this.idle
                    .as_mut()
                    .reset(tokio::time::Instant::now() + this.idle_timeout);
                Poll::Ready(Some(Ok(Frame::data(data))))
            }
            Poll::Ready(Some(Err(_))) => Poll::Ready(Some(Ok(this.fail("upstream stream failed")))),
            Poll::Ready(None) => {
                this.inner = None;
                Poll::Ready(None)
            }
            Poll::Pending => {
                if this.idle.as_mut().poll(cx).is_ready()
                    || this.deadline.as_mut().poll(cx).is_ready()
                {
                    return Poll::Ready(Some(Ok(this.fail("upstream stream timed out"))));
                }
                Poll::Pending
            }
        }
    }
}

impl Drop for MeteredStream {
    fn drop(&mut self) {
        // Runs whether the stream completed, failed, or the caller hung up.
        // Only a stream that delivered its closing usage is charged what it
        // used; any other ending is charged the hold.
        let run_total = self.charge.take().map(|charge| charge.close(&self.meter));
        self.log.emit(200, self.meter.counts(), run_total);
    }
}

fn required_env(name: &str, max_bytes: usize) -> Result<String> {
    let value = std::env::var(name)
        .map_err(|_| QuorumError::Usage(format!("inference-forwarder requires {name}")))?;
    if value.is_empty() || value.len() > max_bytes || value.contains('\0') {
        return Err(invalid_env(name));
    }
    Ok(value)
}

fn optional_env(name: &str, max_bytes: usize) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) if value.is_empty() => Ok(None),
        Ok(value) if value.len() <= max_bytes && !value.contains('\0') => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        _ => Err(invalid_env(name)),
    }
}

/// Names the variable only; a rejected value is never echoed.
fn invalid_env(name: &str) -> QuorumError {
    QuorumError::Usage(format!("inference-forwarder received invalid {name}"))
}

/// A comma-separated list of identifiers, or `None` when unset or empty.
fn list_env(name: &str) -> Result<Option<Vec<String>>> {
    let Some(value) = optional_env(name, MAX_ENV_BYTES)? else {
        return Ok(None);
    };
    let entries: Vec<String> = value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(str::to_owned)
        .collect();
    if entries
        .iter()
        .any(|entry| entry.len() > 128 || !entry.chars().all(|c| c.is_ascii_graphic()))
    {
        return Err(invalid_env(name));
    }
    Ok((!entries.is_empty()).then_some(entries))
}

/// A duration in whole seconds, from 1 to 3600, or `default` when unset.
fn seconds_env(name: &str, default: u64) -> Result<u64> {
    match optional_env(name, 32)? {
        None => Ok(default),
        Some(value) => match value.parse::<u64>() {
            Ok(seconds) if (1..=3600).contains(&seconds) => Ok(seconds),
            _ => Err(invalid_env(name)),
        },
    }
}

/// Server certificates are verified against the bundled public web roots
/// only; an upstream behind a private certificate authority is not reachable.
fn tls_connector() -> Result<tokio_rustls::TlsConnector> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|_| QuorumError::Io("failed to configure TLS for the upstream".into()))?
    .with_root_certificates(roots)
    .with_no_client_auth();
    Ok(tokio_rustls::TlsConnector::from(Arc::new(config)))
}

impl Forwarder {
    fn from_env() -> Result<Self> {
        let endpoint = required_env("QUORUM_AGENT_ENDPOINT", MAX_ENV_BYTES)?;
        let repository = required_env("QUORUM_REPO", 255)?;
        let credential_name = "QUORUM_INFERENCE_UPSTREAM_API_KEY";
        let mut credential = HeaderValue::from_str(&required_env(credential_name, MAX_ENV_BYTES)?)
            .map_err(|_| invalid_env(credential_name))?;
        credential.set_sensitive(true);
        let url_name = "QUORUM_INFERENCE_UPSTREAM_URL";
        let url = optional_env(url_name, MAX_ENV_BYTES)?;
        let upstream = Upstream::parse(url.as_deref().unwrap_or(DEFAULT_UPSTREAM))
            .map_err(|reason| QuorumError::Usage(format!("{url_name} {reason}")))?;
        let cap_name = "QUORUM_INFERENCE_RUN_TOKEN_CAP";
        let cap = match optional_env(cap_name, 32)? {
            None => None,
            Some(value) => match value.parse::<u64>() {
                Ok(cap) if cap > 0 => Some(cap),
                _ => return Err(invalid_env(cap_name)),
            },
        };
        let timeout = seconds_env(
            "QUORUM_INFERENCE_UPSTREAM_TIMEOUT_SECS",
            DEFAULT_UPSTREAM_TIMEOUT_SECS,
        )?;
        let client_stall_timeout = seconds_env(
            "QUORUM_INFERENCE_CLIENT_STALL_TIMEOUT_SECS",
            DEFAULT_CLIENT_STALL_TIMEOUT_SECS,
        )?;
        let auth_recheck = seconds_env(
            "QUORUM_INFERENCE_AUTH_RECHECK_SECS",
            DEFAULT_AUTH_RECHECK_SECS,
        )?;
        let in_flight_name = "QUORUM_INFERENCE_RUN_MAX_IN_FLIGHT";
        let max_in_flight = match optional_env(in_flight_name, 32)? {
            None => DEFAULT_RUN_MAX_IN_FLIGHT,
            Some(value) => match value.parse::<usize>() {
                Ok(limit) if (1..=MAX_HTTP_CONNECTIONS).contains(&limit) => limit,
                _ => return Err(invalid_env(in_flight_name)),
            },
        };
        let allowed_betas = list_env("QUORUM_INFERENCE_ALLOWED_BETAS")?.unwrap_or_default();
        let compaction_name = "QUORUM_INFERENCE_ALLOW_COMPACTION";
        let allow_compaction = match optional_env(compaction_name, 8)?.as_deref() {
            None | Some("0") => false,
            Some("1") => true,
            Some(_) => return Err(invalid_env(compaction_name)),
        };
        // Compaction is unusable without its beta, so enabling one without
        // allowing the other is a configuration mistake, not a silent no-op.
        if allow_compaction
            && !allowed_betas
                .iter()
                .any(|beta| beta.starts_with(policy::COMPACTION_BETA_PREFIX))
        {
            return Err(QuorumError::Usage(format!(
                "{compaction_name} requires a compaction beta in QUORUM_INFERENCE_ALLOWED_BETAS"
            )));
        }
        let tls = upstream.tls.then(tls_connector).transpose()?;
        Ok(Self {
            endpoint: PathBuf::from(endpoint),
            repository,
            upstream,
            credential,
            policy: Policy {
                allowed_models: list_env("QUORUM_INFERENCE_ALLOWED_MODELS")?,
                allowed_betas,
                allow_compaction,
            },
            upstream_timeout: Duration::from_secs(timeout),
            client_stall_timeout: Duration::from_secs(client_stall_timeout),
            auth_recheck: Duration::from_secs(auth_recheck),
            tag_key: std::collections::hash_map::RandomState::new(),
            sweeping: AtomicBool::new(false),
            ledger: Arc::new(Ledger::new(cap, max_in_flight, MAX_TRACKED_RUNS)),
            endpoint_slots: tokio::sync::Semaphore::new(MAX_ENDPOINT_EXCHANGES),
            recheck_slots: tokio::sync::Semaphore::new(MAX_RECHECK_EXCHANGES),
            tls,
        })
    }
}

/// Serve the forwarder on a loopback address until SIGINT or SIGTERM.
pub fn run(addr: SocketAddr) -> Result<()> {
    if !addr.ip().is_loopback() {
        return Err(QuorumError::Usage(
            "inference-forwarder --listen is loopback-only; it serves plain HTTP authorized by run capability"
                .into(),
        ));
    }
    let forwarder = Arc::new(Forwarder::from_env()?);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|_| QuorumError::Io("failed to start inference forwarder runtime".into()))?;
    runtime.block_on(async move {
        let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
            .map_err(|e| QuorumError::Io(format!("failed to register SIGINT handler: {e}")))?;
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .map_err(|e| QuorumError::Io(format!("failed to register SIGTERM handler: {e}")))?;
        let bind_error =
            |e| QuorumError::Io(format!("failed to bind inference forwarder listener: {e}"));
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(bind_error)?;
        let local = listener.local_addr().map_err(bind_error)?;
        let client_stall_timeout = forwarder.client_stall_timeout;
        let app = axum::Router::new().fallback(move |request: Request<Body>| {
            let forwarder = Arc::clone(&forwarder);
            async move { handle(forwarder, request).await }
        });
        // The one line a launcher needs to learn a port-0 bind.
        eprintln!("quorum inference-forwarder listening on http://{local}");

        let slots = Arc::new(tokio::sync::Semaphore::new(MAX_HTTP_CONNECTIONS));
        let (shutdown, _) = tokio::sync::watch::channel(false);
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                // Signals first, then reaping, so finished connections never
                // accumulate behind a steady stream of new ones.
                biased;
                _ = sigint.recv() => break,
                _ = sigterm.recv() => break,
                Some(_) = connections.join_next(), if !connections.is_empty() => {}
                accepted = accept_with_slot(&listener, &slots) => {
                    if let Some((stream, slot)) = accepted {
                        connections.spawn(serve_connection(
                            stream,
                            app.clone(),
                            shutdown.subscribe(),
                            client_stall_timeout,
                            slot,
                        ));
                    }
                }
            }
        }
        // Signals only start shutdown. Stop accepting, let open connections
        // finish for a bounded time, then drop what remains. A request cut
        // off here has already been counted for the usage it reported.
        drop(listener);
        let _ = shutdown.send(true);
        let _ = tokio::time::timeout(HTTP_SHUTDOWN_DRAIN, async {
            while connections.join_next().await.is_some() {}
        })
        .await;
        connections.shutdown().await;
        Ok(())
    })
}

/// Accept one connection, but only once a connection slot is free, so the
/// excess waits in the kernel's bounded listen backlog rather than in memory.
/// Cancel-safe: a dropped call releases its slot and leaves the backlog alone.
async fn accept_with_slot(
    listener: &tokio::net::TcpListener,
    slots: &Arc<tokio::sync::Semaphore>,
) -> Option<(TcpStream, tokio::sync::OwnedSemaphorePermit)> {
    // The semaphore is never closed, so acquisition cannot fail.
    let slot = Arc::clone(slots).acquire_owned().await.ok()?;
    match listener.accept().await {
        Ok((stream, _)) => Some((stream, slot)),
        Err(_) => {
            // Usually transient. Pause so a persistent failure cannot spin.
            tokio::time::sleep(Duration::from_millis(50)).await;
            None
        }
    }
}

/// A client connection that records how long a write to it has been pending,
/// so a peer that stops reading can be detected from outside the connection
/// task's own polling.
struct WatchedIo {
    inner: TcpStream,
    blocked_since: Arc<Mutex<Option<tokio::time::Instant>>>,
}

impl WatchedIo {
    fn note<T>(&self, poll: &Poll<T>) {
        let mut blocked = self
            .blocked_since
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match poll {
            Poll::Pending => {
                blocked.get_or_insert_with(tokio::time::Instant::now);
            }
            Poll::Ready(_) => *blocked = None,
        }
    }
}

impl AsyncRead for WatchedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for WatchedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let poll = Pin::new(&mut self.inner).poll_write(cx, buf);
        self.note(&poll);
        poll
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let poll = Pin::new(&mut self.inner).poll_write_vectored(cx, bufs);
        self.note(&poll);
        poll
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Serve one HTTP/1.1 connection until it closes, its request head times
/// out, its peer stops reading, it reaches its lifetime, or shutdown is
/// signalled. `_slot` is held for the connection's life.
///
/// The stall and lifetime bounds are timers of this task, not of the
/// response body, so they fire even while nothing polls the response: a peer
/// that stops reading leaves the body unpolled and a write pending.
async fn serve_connection(
    stream: TcpStream,
    app: axum::Router,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    client_stall_timeout: Duration,
    _slot: tokio::sync::OwnedSemaphorePermit,
) {
    let _ = stream.set_nodelay(true);
    let blocked_since = Arc::new(Mutex::new(None));
    let io = WatchedIo {
        inner: stream,
        blocked_since: Arc::clone(&blocked_since),
    };
    let connection = hyper::server::conn::http1::Builder::new()
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(HTTP_HEADER_READ_TIMEOUT)
        .max_buf_size(MAX_HEAD_BYTES)
        .serve_connection(
            hyper_util::rt::TokioIo::new(io),
            hyper_util::service::TowerToHyperService::new(app),
        );
    let stalled = async {
        let tick = client_stall_timeout.min(Duration::from_millis(250));
        loop {
            tokio::time::sleep(tick).await;
            let blocked = *blocked_since
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if blocked.is_some_and(|since| since.elapsed() >= client_stall_timeout) {
                return;
            }
        }
    };
    let lifetime = tokio::time::sleep(CONNECTION_LIFETIME);
    let hard_lifetime = tokio::time::sleep(CONNECTION_HARD_LIFETIME);
    tokio::pin!(connection, stalled, lifetime, hard_lifetime);
    let mut draining = false;
    loop {
        tokio::select! {
            _ = connection.as_mut() => break,
            // Dropping the connection drops its request, which releases the
            // run's in-flight slot and charges what the request still held.
            _ = &mut stalled => break,
            _ = &mut hard_lifetime => break,
            _ = &mut lifetime, if !draining => {
                draining = true;
                connection.as_mut().graceful_shutdown();
            }
            _ = shutdown.changed(), if !draining => {
                draining = true;
                connection.as_mut().graceful_shutdown();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.append(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        map
    }

    #[test]
    fn run_capability_comes_from_exactly_one_well_formed_credential() {
        for (pairs, expected) in [
            (vec![("x-api-key", "run-1")], Some("run-1")),
            (vec![("authorization", "Bearer run-1")], Some("run-1")),
            (vec![("authorization", "bearer run-1")], Some("run-1")),
            (
                vec![("x-api-key", "run-1"), ("authorization", "Bearer run-1")],
                Some("run-1"),
            ),
            (
                vec![("x-api-key", "run-1"), ("authorization", "Bearer run-2")],
                None,
            ),
            (vec![("x-api-key", "run-1"), ("x-api-key", "run-1")], None),
            (vec![("authorization", "Basic run-1")], None),
            (vec![("authorization", "Bearer")], None),
            (vec![("authorization", "Bearer  run-1")], None),
            (vec![("x-api-key", "run 1")], None),
            (vec![("x-api-key", "")], None),
            (vec![], None),
        ] {
            assert_eq!(run_capability(&headers(&pairs)), expected, "{pairs:?}");
        }
        let long = "r".repeat(MAX_CAPABILITY_BYTES + 1);
        assert_eq!(run_capability(&headers(&[("x-api-key", &long)])), None);
    }

    #[test]
    fn only_loopback_hosts_are_served() {
        for (host, allowed) in [
            ("127.0.0.1:8080", true),
            ("localhost", true),
            ("LOCALHOST:1", true),
            ("[::1]:8080", true),
            ("evil.example", false),
            ("192.0.2.1:8080", false),
            ("localhost.evil.example", false),
            ("", false),
        ] {
            assert_eq!(
                names_loopback_host(&headers(&[("host", host)])),
                allowed,
                "{host}"
            );
        }
        assert!(!names_loopback_host(&headers(&[])));
        assert!(!names_loopback_host(&headers(&[
            ("host", "localhost"),
            ("host", "localhost")
        ])));
    }

    #[test]
    fn upstream_urls_are_validated() {
        assert_eq!(
            Upstream::parse(DEFAULT_UPSTREAM).unwrap(),
            Upstream {
                tls: true,
                host: "api.anthropic.com".into(),
                port: 443,
                prefix: String::new(),
            }
        );
        let gateway = Upstream::parse("https://gateway.example:8443/models/").unwrap();
        assert_eq!(gateway.prefix, "/models");
        assert_eq!(gateway.host_header(), "gateway.example:8443");
        let local = Upstream::parse("http://127.0.0.1:9000").unwrap();
        assert!(!local.tls);
        assert_eq!(local.host_header(), "127.0.0.1:9000");
        assert_eq!(
            Upstream::parse("http://[::1]:9000").unwrap().host_header(),
            "[::1]:9000"
        );
        for refused in [
            "http://gateway.example",
            "http://192.0.2.1:80",
            "ftp://127.0.0.1",
            "https://user:secret@gateway.example",
            "https://gateway.example/?key=1",
            "gateway.example",
            "",
        ] {
            Upstream::parse(refused).unwrap_err();
        }
    }

    #[tokio::test]
    async fn bodies_are_refused_once_over_their_limit() {
        assert_eq!(
            read_limited(Body::from(vec![b'x'; 64]), 64)
                .await
                .unwrap()
                .len(),
            64
        );
        assert_eq!(
            read_limited(Body::from(vec![b'x'; 65]), 64).await,
            Err(ReadFailure::TooLarge)
        );
    }
}
