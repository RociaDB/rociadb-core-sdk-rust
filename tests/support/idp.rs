//! A mock OAuth2 identity provider, served over loopback by `hyper`.
//!
//! It answers `POST /token` with the client-credentials response
//! [`fetch_token`](rociadb_sdk::auth::fetch_token) expects — `access_token`,
//! `expires_in`, `token_type` — and gives the tests three things a canned
//! HTTP stub could not:
//!
//! - **a scripted sequence**: [`MockIdp::fail_next`] makes the next *n*
//!   requests fail with an HTTP status, after which the provider goes back to
//!   issuing tokens, which is what the token-refresh backoff needs;
//! - **a different token every time**: each successful request hands out
//!   `token-1`, `token-2`, … so a test can tell *which* token the interceptor
//!   attached to a call, and therefore whether a refresh actually happened;
//! - **a request count and the form bodies received**, so "the client
//!   refreshed exactly once" is an assertion rather than an inference.
//!
//! It binds `127.0.0.1:0`, so several tests run in parallel without agreeing
//! on a port, and it stops when the value is dropped.

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
// `tokio::time::Instant`, not `std::time::Instant`: `wait_for_requests` sleeps on
// the tokio clock, so its deadline has to be measured on the same one. Under
// `#[tokio::test(start_paused = true)]` the tokio clock is virtual and advances
// instantly through a `sleep` while the std clock does not move at all, which
// left the "bounded poll" unbounded in virtual time and made the `Duration` it
// reports meaningless.
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::Instant;

/// Token lifetime the provider advertises unless a test asks for another one.
/// Long enough that the background refresh task never fires during a test
/// that did not ask it to: the SDK refreshes after two thirds of the
/// lifetime, so 600 seconds means "not during this test".
const DEFAULT_EXPIRES_IN_SECS: u64 = 600;

#[derive(Debug)]
struct IdpState {
    /// Every request the provider received on any path, failures included.
    requests: u64,
    /// Tokens handed out, in order. `issued.len() + 1` names the next one.
    issued: Vec<String>,
    /// HTTP statuses to answer with instead of a token, one per request,
    /// oldest first.
    scripted_failures: VecDeque<u16>,
    /// `expires_in` the provider advertises.
    expires_in: u64,
    /// Form bodies received, so a test can check the grant type and client id
    /// actually sent.
    bodies: Vec<String>,
}

impl Default for IdpState {
    fn default() -> Self {
        Self {
            requests: 0,
            issued: Vec::new(),
            scripted_failures: VecDeque::new(),
            expires_in: DEFAULT_EXPIRES_IN_SECS,
            bodies: Vec::new(),
        }
    }
}

/// A running mock identity provider. Dropping it stops the listener.
#[derive(Debug)]
pub struct MockIdp {
    address: SocketAddr,
    state: Arc<Mutex<IdpState>>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl MockIdp {
    /// Start a provider that issues `token-1`, `token-2`, … each valid for
    /// 600 seconds.
    pub async fn start() -> Self {
        Self::start_with_expires_in(DEFAULT_EXPIRES_IN_SECS).await
    }

    /// Start a provider whose tokens advertise `expires_in` seconds.
    ///
    /// A short lifetime is how a test drives the background refresh task: the
    /// SDK derives its cadence from this value, so a two-second token is
    /// refreshed about a second after it was issued.
    pub async fn start_with_expires_in(expires_in: u64) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the mock identity provider must bind an ephemeral loopback port");
        let address = listener
            .local_addr()
            .expect("a bound listener must report its address");
        let state = Arc::new(Mutex::new(IdpState {
            expires_in,
            ..IdpState::default()
        }));
        let (shutdown, mut shutdown_rx) = oneshot::channel::<()>();

        let accepting = Arc::clone(&state);
        tokio::spawn(async move {
            loop {
                let stream = tokio::select! {
                    _ = &mut shutdown_rx => break,
                    accepted = listener.accept() => match accepted {
                        Ok((stream, _peer)) => stream,
                        Err(_) => break,
                    },
                };
                let state = Arc::clone(&accepting);
                tokio::spawn(async move {
                    let service = service_fn(move |request| handle(Arc::clone(&state), request));
                    // A connection that ends badly (the client hanging up
                    // mid-response, say) is not a test failure: the
                    // assertions are on what the provider recorded.
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });

        Self {
            address,
            state,
            shutdown: Some(shutdown),
        }
    }

    /// The URL to hand to
    /// [`auth_client_credentials`](rociadb_sdk::RociaDbBuilder::auth_client_credentials).
    pub fn token_url(&self) -> String {
        format!("http://{}/token", self.address)
    }

    /// How many requests the provider has received, failures included.
    pub fn requests(&self) -> u64 {
        self.lock().requests
    }

    /// Every token handed out so far, oldest first.
    pub fn issued_tokens(&self) -> Vec<String> {
        self.lock().issued.clone()
    }

    /// The most recently issued token, if any.
    pub fn latest_token(&self) -> Option<String> {
        self.lock().issued.last().cloned()
    }

    /// The `authorization` header value the SDK builds from the most recent
    /// token, for comparison against what the fake gRPC server recorded.
    pub fn latest_bearer(&self) -> Option<String> {
        self.latest_token().map(|token| format!("Bearer {token}"))
    }

    /// Every form body received, oldest first.
    pub fn bodies(&self) -> Vec<String> {
        self.lock().bodies.clone()
    }

    /// Answer the next `times` requests with HTTP `status` instead of a
    /// token. Requests beyond that are served normally.
    pub fn fail_next(&self, times: usize, status: u16) {
        let mut state = self.lock();
        for _ in 0..times {
            state.scripted_failures.push_back(status);
        }
    }

    /// Wait until the provider has received at least `count` requests, or
    /// `timeout` elapses. Returns how long the wait took, or `None` on
    /// timeout — a bounded poll rather than a fixed sleep, so a test that
    /// observes a background refresh stays both fast and honest about how
    /// long it actually waited.
    pub async fn wait_for_requests(&self, count: u64, timeout: Duration) -> Option<Duration> {
        let started = Instant::now();
        while started.elapsed() < timeout {
            if self.requests() >= count {
                return Some(started.elapsed());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        (self.requests() >= count).then(|| started.elapsed())
    }

    fn lock(&self) -> MutexGuard<'_, IdpState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Drop for MockIdp {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

/// Serve one request. Nothing is awaited while the state lock is held: the
/// body is read first, and the response is built from values copied out.
async fn handle(
    state: Arc<Mutex<IdpState>>,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = request.uri().path().to_string();
    let body = request
        .into_body()
        .collect()
        .await
        .map(|collected| collected.to_bytes())
        .unwrap_or_default();
    let body = String::from_utf8_lossy(&body).into_owned();

    let mut state = state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    state.requests += 1;
    state.bodies.push(body);

    if path != "/token" {
        return Ok(respond(
            StatusCode::NOT_FOUND,
            "text/plain",
            format!("no such endpoint: {path}"),
        ));
    }
    if let Some(status) = state.scripted_failures.pop_front() {
        return Ok(respond(
            StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            "text/plain",
            "scripted identity provider failure",
        ));
    }

    let token = format!("token-{}", state.issued.len() + 1);
    state.issued.push(token.clone());
    let expires_in = state.expires_in;
    Ok(respond(
        StatusCode::OK,
        "application/json",
        format!(r#"{{"access_token":"{token}","expires_in":{expires_in},"token_type":"Bearer"}}"#),
    ))
}

fn respond(
    status: StatusCode,
    content_type: &'static str,
    body: impl Into<Bytes>,
) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", content_type)
        .body(Full::new(body.into()))
        .expect("a response built from constants must be well-formed")
}
