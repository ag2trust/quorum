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
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpStream, UnixStream};
use tokio_rustls::rustls;
use usage::{Admission, Counts, Ledger, Meter};

const PROTOCOL_VERSION: u8 = 1;
const MAX_FRAME_BYTES: usize = 64 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CAPABILITY_BYTES: usize = 128;
const MAX_ENV_BYTES: usize = 4096;

const MESSAGES_PATH: &str = "/v1/messages";
const COUNT_TOKENS_PATH: &str = "/v1/messages/count_tokens";
const DEFAULT_UPSTREAM: &str = "https://api.anthropic.com";
const DEFAULT_API_VERSION: &str = "2023-06-01";

/// Bound on one request body, matching the largest request the mirrored API
/// accepts. A body is read only after its run capability is authorized.
const MAX_REQUEST_BODY_BYTES: usize = 32 * 1024 * 1024;
/// Bound on an upstream response that is buffered rather than streamed.
const MAX_BUFFERED_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
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
/// A connection must deliver a complete request head within this bound.
const HTTP_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// An authorized request must deliver its whole body within this bound.
const BODY_READ_TIMEOUT: Duration = Duration::from_secs(30);
/// Bound on reaching the upstream, including its TLS handshake.
const UPSTREAM_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Default bound on the upstream's silence: before its response head, and
/// between chunks of a streamed response.
const DEFAULT_UPSTREAM_TIMEOUT_SECS: u64 = 300;
/// Bound on one streamed response from first byte to last.
const MAX_STREAM_DURATION: Duration = Duration::from_secs(60 * 60);
/// After SIGINT/SIGTERM, open connections get this long to finish.
const HTTP_SHUTDOWN_DRAIN: Duration = Duration::from_secs(5);
/// Runs whose usage is tracked at once.
const MAX_TRACKED_RUNS: usize = 4096;
/// Least-recently-seen runs re-checked with the daemon when the ledger fills.
const LEDGER_SWEEP_BATCH: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    Messages,
    CountTokens,
}

impl Route {
    fn of(method: &Method, path: &str) -> Option<Self> {
        if method != Method::POST {
            return None;
        }
        match path {
            MESSAGES_PATH => Some(Self::Messages),
            COUNT_TOKENS_PATH => Some(Self::CountTokens),
            _ => None,
        }
    }

    const fn path(self) -> &'static str {
        match self {
            Self::Messages => MESSAGES_PATH,
            Self::CountTokens => COUNT_TOKENS_PATH,
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Messages => "messages",
            Self::CountTokens => "count_tokens",
        }
    }
}

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
    ledger: Ledger,
    endpoint_slots: tokio::sync::Semaphore,
    tls: Option<tokio_rustls::TlsConnector>,
}

/// The live run a request was authorized for.
#[derive(Debug, Clone)]
struct Run {
    task_id: i64,
    role: String,
    /// Stable, non-reversible tag for log lines. The run id is the bearer
    /// credential, so it is never logged itself.
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

fn run_tag(capability: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    capability.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

impl Forwarder {
    /// Ask the daemon endpoint whether `capability` names a live run of this
    /// repository. Nothing is cached: a revoked or finished run is refused on
    /// its next request.
    async fn authorize(&self, capability: &str) -> std::result::Result<Run, AuthFailure> {
        let body = serde_json::to_vec(&json!({
            "version": PROTOCOL_VERSION,
            "capability": capability,
            "operation": {"type": "inventory"},
        }))
        .map_err(|_| AuthFailure::Unavailable)?;
        let exchange = async {
            let _slot = self.endpoint_slots.acquire().await;
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
                    tag: run_tag(capability),
                })
            }
            (false, None, Some(error)) if error.code == "unauthorized" => {
                Err(AuthFailure::Rejected)
            }
            _ => Err(AuthFailure::Unavailable),
        }
    }

    /// Admit a run against its token cap. When the ledger is full, runs the
    /// daemon no longer accepts are dropped from it and admission is retried
    /// once; a run that is still live keeps its count.
    async fn admit(&self, capability: &str) -> Admission {
        match self.ledger.admit(capability) {
            Admission::Full => {}
            admission => return admission,
        }
        for tracked in self.ledger.least_recent(LEDGER_SWEEP_BATCH) {
            if matches!(self.authorize(&tracked).await, Err(AuthFailure::Rejected)) {
                self.ledger.forget(&tracked);
            }
        }
        self.ledger.admit(capability)
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
    route: Route,
    model: Option<String>,
}

impl RequestLog {
    fn emit(&self, status: u16, counts: Counts, run_total: Option<u64>) {
        let model = self.model.as_deref().map_or("-".into(), printable);
        let total = run_total.map_or("-".into(), |total| total.to_string());
        eprintln!(
            "quorum inference-forwarder: run={} task={} role={} route={} model={} status={} \
             input_tokens={} output_tokens={} run_total_tokens={}",
            self.run.tag,
            self.run.task_id,
            self.run.role,
            self.route.name(),
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
    let Some(route) = Route::of(&parts.method, parts.uri.path()) else {
        return closing(refusal(
            StatusCode::NOT_FOUND,
            "not_found_error",
            "not found",
        ));
    };
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
    let mut log = RequestLog {
        run,
        route,
        model: None,
    };
    if route == Route::Messages {
        match forwarder.admit(capability).await {
            Admission::Admitted => {}
            Admission::CapReached => {
                return closing(log.refuse(
                    StatusCode::TOO_MANY_REQUESTS,
                    "rate_limit_error",
                    "this run has used its token allowance; no further model requests are available to it",
                ))
            }
            Admission::Full => {
                log.emit(503, Counts::default(), None);
                return closing(error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "overloaded_error",
                    "forwarder is tracking its maximum number of runs",
                    true,
                ));
            }
        }
    }

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
    let response = match forwarder
        .send(route, outbound, api_version, betas.as_deref())
        .await
    {
        Ok(response) => response,
        Err(failure) => {
            let (status, message) = match failure {
                UpstreamFailure::Unreachable => {
                    (StatusCode::BAD_GATEWAY, "upstream could not be reached")
                }
                UpstreamFailure::TimedOut => (
                    StatusCode::GATEWAY_TIMEOUT,
                    "upstream did not answer in time",
                ),
            };
            log.emit(status.as_u16(), Counts::default(), None);
            return error_response(status, "api_error", message, true);
        }
    };
    forwarder.relay(response, capability, log).await
}

#[derive(Debug)]
enum UpstreamFailure {
    Unreachable,
    TimedOut,
}

impl Forwarder {
    /// Send one checked request upstream on its own connection and return
    /// the response head. Only the headers built here are sent: the caller's
    /// credential and every other inbound header stay behind.
    async fn send(
        &self,
        route: Route,
        body: Vec<u8>,
        api_version: HeaderValue,
        betas: Option<&str>,
    ) -> std::result::Result<hyper::Response<Incoming>, UpstreamFailure> {
        let mut request = hyper::Request::builder()
            .method(Method::POST)
            .uri(format!("{}{}", self.upstream.prefix, route.path()))
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
            .map_err(|_| UpstreamFailure::TimedOut)??;
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

    /// Turn the upstream's response into the caller's, counting its usage
    /// against the run.
    async fn relay(
        self: Arc<Self>,
        response: hyper::Response<Incoming>,
        capability: &str,
        log: RequestLog,
    ) -> Response {
        let (parts, body) = response.into_parts();
        let status = parts.status;
        let counted = log.route == Route::Messages && status.is_success();
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

        if counted && is_event_stream {
            relayed
                .headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
            *relayed.body_mut() = Body::new(MeteredStream {
                inner: Some(body),
                idle: Box::pin(tokio::time::sleep(self.upstream_timeout)),
                deadline: Box::pin(tokio::time::sleep(MAX_STREAM_DURATION)),
                idle_timeout: self.upstream_timeout,
                meter: Meter::default(),
                run_total: None,
                forwarder: Arc::clone(&self),
                capability: capability.to_string(),
                log,
            });
            return relayed;
        }

        let read = tokio::time::timeout(
            self.upstream_timeout,
            read_limited(body, MAX_BUFFERED_RESPONSE_BYTES),
        )
        .await;
        let bytes = match read {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(_)) => {
                log.emit(502, Counts::default(), None);
                return error_response(
                    StatusCode::BAD_GATEWAY,
                    "api_error",
                    "upstream response could not be read",
                    true,
                );
            }
            Err(_) => {
                log.emit(504, Counts::default(), None);
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
            log.emit(502, Counts::default(), None);
            return refusal(
                StatusCode::BAD_GATEWAY,
                "api_error",
                "upstream refused the forwarder's own request",
            );
        }
        let mut meter = Meter::default();
        let mut run_total = None;
        if counted {
            let reported = serde_json::from_slice::<Value>(&bytes)
                .ok()
                .and_then(|value| value.get("usage").map(|usage| meter.observe(usage)));
            run_total = Some(self.ledger.charge(capability, reported.unwrap_or(0)));
        }
        log.emit(status.as_u16(), meter.counts(), run_total);
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
        .map_err(|_| UpstreamFailure::Unreachable)
}

/// A streamed upstream response passed through chunk by chunk as it arrives,
/// with its usage events counted on the way past. A stream the upstream
/// stops feeding is ended with one error event instead of hanging.
struct MeteredStream {
    inner: Option<Incoming>,
    idle: Pin<Box<tokio::time::Sleep>>,
    deadline: Pin<Box<tokio::time::Sleep>>,
    idle_timeout: Duration,
    meter: Meter,
    run_total: Option<u64>,
    forwarder: Arc<Forwarder>,
    capability: String,
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
                let reported = this.meter.feed_events(&data);
                if reported > 0 {
                    this.run_total = Some(this.forwarder.ledger.charge(&this.capability, reported));
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
        self.log.emit(200, self.meter.counts(), self.run_total);
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
        let timeout_name = "QUORUM_INFERENCE_UPSTREAM_TIMEOUT_SECS";
        let timeout = match optional_env(timeout_name, 32)? {
            None => DEFAULT_UPSTREAM_TIMEOUT_SECS,
            Some(value) => match value.parse::<u64>() {
                Ok(seconds) if (1..=3600).contains(&seconds) => seconds,
                _ => return Err(invalid_env(timeout_name)),
            },
        };
        let tls = upstream.tls.then(tls_connector).transpose()?;
        Ok(Self {
            endpoint: PathBuf::from(endpoint),
            repository,
            upstream,
            credential,
            policy: Policy {
                allowed_models: list_env("QUORUM_INFERENCE_ALLOWED_MODELS")?,
                allowed_betas: list_env("QUORUM_INFERENCE_ALLOWED_BETAS")?.unwrap_or_default(),
            },
            upstream_timeout: Duration::from_secs(timeout),
            ledger: Ledger::new(cap, MAX_TRACKED_RUNS),
            endpoint_slots: tokio::sync::Semaphore::new(MAX_ENDPOINT_EXCHANGES),
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

/// Serve one HTTP/1.1 connection until it closes, its request head times
/// out, or shutdown is signalled. `_slot` is held for the connection's life.
async fn serve_connection(
    stream: TcpStream,
    app: axum::Router,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    _slot: tokio::sync::OwnedSemaphorePermit,
) {
    let _ = stream.set_nodelay(true);
    let connection = hyper::server::conn::http1::Builder::new()
        .timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(HTTP_HEADER_READ_TIMEOUT)
        .max_buf_size(MAX_HEAD_BYTES)
        .serve_connection(
            hyper_util::rt::TokioIo::new(stream),
            hyper_util::service::TowerToHyperService::new(app),
        );
    tokio::pin!(connection);
    tokio::select! {
        _ = connection.as_mut() => {}
        _ = shutdown.changed() => {
            connection.as_mut().graceful_shutdown();
            let _ = connection.await;
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
