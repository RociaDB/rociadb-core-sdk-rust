//! Auth helpers for bearer tokens and API keys.

use crate::Result;
use crate::error::AuthResultExt;
use serde::Deserialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{Mutex, Notify, oneshot};
use tokio::task::JoinHandle;
use tokio::time;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::{Request, Status, service::Interceptor};
use tracing::warn;

/// Floor applied to the derived refresh interval, in case the IdP ever
/// advertises a very short (or zero) token lifetime.
const MIN_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// Response payload for OAuth2 client credentials token.
#[non_exhaustive]
#[derive(Deserialize)]
pub struct TokenResponse {
    /// The bearer token itself, attached verbatim to the `authorization`
    /// header of every outgoing RPC. A live credential: never log it (this
    /// type's `Debug` impl redacts it for that reason).
    pub access_token: String,
    /// Lifetime the IdP advertises for `access_token`, in seconds from the
    /// moment it was issued. The background refresh task derives its cadence
    /// from this value.
    pub expires_in: u64,
    /// Token type the IdP reports, `"Bearer"` for the client-credentials
    /// grant this crate uses.
    pub token_type: String,
}

// Manual `Debug` impl instead of `#[derive(Debug)]`: a derived impl would
// print `access_token` — a live bearer credential — in clear text, and
// unlike `TokenManager` (which never exposes a `TokenResponse` at all),
// this type is returned directly to caller code by the standalone
// `fetch_token` helper below, so a routine debug-print or log of a
// fetched token would leak a working credential. Mirrors
// `BuilderAuthConfig`'s redacting `Debug` impl in `lib.rs`, which takes
// the same care for `client_secret`.
impl std::fmt::Debug for TokenResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenResponse")
            .field("access_token", &"[redacted]")
            .field("expires_in", &self.expires_in)
            .field("token_type", &self.token_type)
            .finish()
    }
}

/// Fetch a token using client credentials.
///
/// `client_secret` is posted as a form field and never appears in this
/// function's own `Debug` output, log lines, or error messages (see
/// [`TokenResponse`]'s manual `Debug` impl for the same care taken on the
/// response side) — but it is passed and held as a plain `String`, so
/// nothing here scrubs the secret from memory once it is no longer
/// needed, and a core dump, an attached debugger, or plaintext left
/// behind in swapped-out memory could still recover it for as long as the
/// process is alive. Closing that gap would need a `zeroize`- or
/// `secrecy`-backed secret type, which this crate does not currently
/// depend on.
///
/// `token_url` is not required to be `https://`, matching this crate's
/// treatment of the upstream gRPC host (also usable as plain `http://` for
/// local development): a non-`https` `token_url` is logged as a `warn!`
/// rather than rejected, since `client_id`/`client_secret` and the token
/// returned are sent in cleartext over such a connection.
pub async fn fetch_token(
    http: &reqwest::Client,
    token_url: &str,
    client_id: &str,
    client_secret: &str,
) -> Result<TokenResponse> {
    if !token_url.to_ascii_lowercase().starts_with("https://") {
        // Mirrors the `warn!` `RociaDbBuilder::build` emits for
        // `disable_auth()`: the crate deliberately allows plaintext here
        // too, so this stays a warning rather than a hard rejection, but
        // the caller should still be told that `client_id`/`client_secret`
        // and the bearer token this returns are about to cross the wire
        // unencrypted.
        warn!(
            token_url = %token_url,
            "fetching an OAuth2 token over a non-https token_url; client credentials and the \
             access token will be sent in cleartext"
        );
    }

    let res = http
        .post(token_url)
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
        ])
        .send()
        .await
        .auth_context("token request failed")?
        .error_for_status()
        .auth_context("token endpoint returned error")?;

    res.json::<TokenResponse>()
        .await
        .auth_context("failed to parse token response")
}

/// Token manager with cached authorization header.
#[derive(Clone)]
pub struct TokenManager {
    inner: Arc<TokenManagerInner>,
}

struct TokenManagerInner {
    http: reqwest::Client,
    token_url: String,
    client_id: String,
    /// Never printed by this crate's own `Debug` output, log lines, or
    /// error messages (see [`TokenResponse`]'s manual `Debug` impl, and
    /// `BuilderAuthConfig`'s in `lib.rs`, for the same care taken
    /// elsewhere). That only rules out *this crate* leaking it through its
    /// own instrumentation, though: it is still a plain `String` for as
    /// long as this `TokenManagerInner` is alive, so a core dump, an
    /// attached debugger, or plaintext left behind in swapped-out memory
    /// could still recover it. Scrubbing the backing bytes on drop would
    /// need a `zeroize`- or `secrecy`-backed secret type, which is a
    /// dependency call for this crate's maintainer to make on a published
    /// 1.0 crate rather than something to add unilaterally here.
    client_secret: String,
    header_value: Arc<RwLock<MetadataValue<Ascii>>>,
    /// `expires_in` (seconds) from the most recently fetched token, as
    /// reported by the IdP. Drives [`TokenManager::refresh_interval`].
    expires_in: AtomicU64,
    /// Coalesces concurrent [`TokenManager::refresh_now`] calls into a
    /// single in-flight fetch. A caller records the current
    /// `refresh_generation` *before* acquiring this lock, so that once it
    /// gets in, it can tell whether another caller already refreshed while
    /// it was waiting (see `refresh_generation` below) and, if so, skip its
    /// own redundant fetch instead of hitting the IdP again.
    refresh_lock: Mutex<()>,
    /// Bumped by [`TokenManager::refresh_now`] immediately after installing
    /// a newly fetched token into `header_value`, while still holding
    /// `refresh_lock`. Because every write to `header_value` happens while
    /// holding that lock, fetches are fully serialized: there is no window
    /// in which a slower concurrent fetch could land after (and overwrite)
    /// a faster one. `Relaxed` is enough — this only decides whether a
    /// waiting caller can skip redundant work, not memory visibility of the
    /// token itself, which the `header_value` lock already guarantees.
    refresh_generation: AtomicU64,
    /// Wakes the background task spawned by [`TokenManager::spawn_refresh`]
    /// as soon as possible, without the caller waiting for the network
    /// round trip. See [`TokenManager::request_refresh`]. A `notify_one()`
    /// call with no task currently waiting stores a permit that the next
    /// `notified().await` consumes immediately, so a request issued between
    /// two loop iterations of the background task is never lost.
    refresh_notify: Notify,
}

impl TokenManager {
    /// Create a new token manager and fetch the first token.
    pub async fn new(
        http: reqwest::Client,
        token_url: String,
        client_id: String,
        client_secret: String,
    ) -> Result<Self> {
        let token = fetch_token(&http, &token_url, &client_id, &client_secret).await?;
        let expires_in = AtomicU64::new(token.expires_in);
        let header_value = Arc::new(RwLock::new(build_header(&token)?));

        Ok(Self {
            inner: Arc::new(TokenManagerInner {
                http,
                token_url,
                client_id,
                client_secret,
                header_value,
                expires_in,
                refresh_lock: Mutex::new(()),
                refresh_generation: AtomicU64::new(0),
                refresh_notify: Notify::new(),
            }),
        })
    }

    /// Derive a safe background-refresh interval from the token lifetime
    /// (`expires_in`, in seconds) most recently reported by the IdP,
    /// leaving margin so the token never actually expires between two
    /// refreshes: `max(expires_in * 2 / 3, 5s)`, clamped so it never
    /// exceeds the token's own remaining lifetime. With the IdP's fixed
    /// 600-second lifetime this yields a 400-second interval, i.e. a
    /// refresh with roughly a third of the token's lifetime still left.
    ///
    /// The 5-second floor exists so a very short (or zero) `expires_in`
    /// still gets a workable cadence instead of one so tight it would
    /// busy-loop refreshing. But that floor implicitly assumes the token
    /// lives at least 5 seconds — for an unusual third-party IdP issuing
    /// shorter-lived tokens than [`TokenManager`]'s primary use case (the
    /// crate's own `rocia-idp`, which issues fixed 600-second tokens) ever
    /// does, an unconditional floor would schedule the next refresh
    /// *after* the token has already expired, guaranteeing a window where
    /// every RPC is made with a stale credential. The clamp below caps the
    /// result at `expires_in - 1` (never below 1 second, so the
    /// `tokio::time::interval` built from it is always valid) so the next
    /// refresh always lands before the current token actually expires,
    /// even if that means refreshing far more often than the 5-second
    /// floor alone would suggest.
    pub fn refresh_interval(&self) -> Duration {
        let expires_in = self.inner.expires_in.load(Ordering::Relaxed);
        let with_margin = expires_in.saturating_mul(2) / 3;
        let floored = Duration::from_secs(with_margin).max(MIN_REFRESH_INTERVAL);
        let ceiling = Duration::from_secs(expires_in.saturating_sub(1).max(1));
        floored.min(ceiling)
    }

    /// Create an interceptor that injects the bearer token.
    pub fn interceptor(&self) -> BearerInterceptor {
        BearerInterceptor::new(Arc::clone(&self.inner.header_value))
    }

    /// Force a token refresh immediately.
    ///
    /// Concurrent callers are coalesced into a single in-flight fetch.
    /// Because `RociaDbClient` is designed to be shared via `Arc` across
    /// many tasks, a token expiring can make every task's next RPC fail at
    /// once, each then calling this to recover; with no coalescing that
    /// would fire one POST per failing task straight at the IdP (risking a
    /// 429) and, since completions can land out of order, a slower fetch
    /// could overwrite a token a faster concurrent one had already cached.
    /// Instead, a caller records the current refresh generation, then
    /// either wins the race to fetch or — if another caller already
    /// refreshed by the time it gets the lock — returns `Ok(())` without
    /// making a redundant request of its own. Every write to the cached
    /// header happens while holding that same lock, so fetches are fully
    /// serialized and a slower one can never land after (and overwrite) a
    /// faster one. A caller that arrives after an in-flight fetch has
    /// already completed still performs its own fetch, as an explicit
    /// refresh request should.
    pub async fn refresh_now(&self) -> Result<()> {
        let observed_generation = self.inner.refresh_generation.load(Ordering::Relaxed);
        let _refresh_permit = self.inner.refresh_lock.lock().await;
        if self.inner.refresh_generation.load(Ordering::Relaxed) != observed_generation {
            // Another caller already refreshed while we were waiting for
            // the lock: the cached header is already newer than anything a
            // redundant fetch here could produce.
            return Ok(());
        }

        let token = fetch_token(
            &self.inner.http,
            &self.inner.token_url,
            &self.inner.client_id,
            &self.inner.client_secret,
        )
        .await?;
        let header = build_header(&token)?;
        self.inner
            .expires_in
            .store(token.expires_in, Ordering::Relaxed);
        {
            let mut guard = self
                .inner
                .header_value
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *guard = header;
        }
        // Bumped only after the new header is fully installed, so a caller
        // that observes this can safely skip its own fetch.
        self.inner
            .refresh_generation
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Request a token refresh without waiting for it.
    ///
    /// Unlike [`TokenManager::refresh_now`], this is **synchronous** and
    /// returns immediately: it only wakes the background task started by
    /// [`TokenManager::spawn_refresh`] (via a shared [`tokio::sync::Notify`])
    /// so it refreshes at the next opportunity, without the caller paying
    /// for the network round trip. If no background task is running (for
    /// example, [`TokenManager::spawn_refresh`] was never called), this is
    /// a harmless no-op — the notification is simply never consumed.
    pub fn request_refresh(&self) {
        self.inner.refresh_notify.notify_one();
    }

    /// Spawn a background refresh task. Returns a [`TokenRefreshGuard`]
    /// that stops the task on drop.
    ///
    /// `interval` only seeds the ticker's initial cadence: after every
    /// successful refresh (on the timer or via
    /// [`TokenManager::request_refresh`]), the ticker is reset to a freshly
    /// computed [`TokenManager::refresh_interval`]. `expires_in` is
    /// re-stored on every refresh, so a third-party IdP whose token
    /// lifetime changes over time (unlike the crate's own `rocia-idp`,
    /// which issues a fixed 600-second lifetime) still gets a cadence that
    /// tracks its most recently observed value, rather than one baked in
    /// once at spawn time and never revisited.
    pub fn spawn_refresh(&self, interval: Duration) -> TokenRefreshGuard {
        let manager = self.clone();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut ticker = time::interval(interval);
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        match manager.refresh_now().await {
                            Ok(()) => ticker.reset_after(manager.refresh_interval()),
                            Err(err) => warn!(error = %err, "token refresh failed"),
                        }
                    }
                    // Woken by `TokenManager::request_refresh` (and thus
                    // `RociaDbClient::invalidate_auth_token`) so a caller can
                    // signal "do not trust the cached token" without paying
                    // for the refresh round trip itself — this background
                    // task absorbs that latency instead.
                    _ = manager.inner.refresh_notify.notified() => {
                        match manager.refresh_now().await {
                            Ok(()) => ticker.reset_after(manager.refresh_interval()),
                            Err(err) => warn!(error = %err, "requested token refresh failed"),
                        }
                    }
                    _ = &mut shutdown_rx => {
                        break;
                    }
                }
            }
        });

        TokenRefreshGuard {
            shutdown: Some(shutdown_tx),
            task,
        }
    }
}

fn build_header(token: &TokenResponse) -> Result<MetadataValue<Ascii>> {
    let bearer = format!("{} {}", token.token_type, token.access_token);
    bearer
        .parse::<MetadataValue<Ascii>>()
        .auth_context("invalid access token metadata value")
}

/// Drop guard for the refresh task.
///
/// Dropping this immediately stops the background refresh: bind it to a
/// variable that lives as long as the client needs auth to keep working
/// (`RociaDbBuilder::build` does this for you). `#[must_use]` catches the
/// common mistake of calling `spawn_refresh(..)` and discarding the result,
/// which would stop the refresh task right away.
#[must_use = "dropping the guard immediately stops the background token refresh task"]
pub struct TokenRefreshGuard {
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl Drop for TokenRefreshGuard {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.abort();
    }
}

/// Interceptor that injects the bearer token when enabled.
#[derive(Clone)]
pub struct BearerInterceptor {
    header_value: Option<Arc<RwLock<MetadataValue<Ascii>>>>,
}

impl BearerInterceptor {
    fn new(header_value: Arc<RwLock<MetadataValue<Ascii>>>) -> Self {
        Self {
            header_value: Some(header_value),
        }
    }

    /// Create an interceptor that does nothing.
    pub(crate) fn disabled() -> Self {
        Self { header_value: None }
    }
}

impl Interceptor for BearerInterceptor {
    fn call(&mut self, mut req: Request<()>) -> std::result::Result<Request<()>, Status> {
        if let Some(header_value) = self.header_value.as_ref() {
            let header_value = header_value
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();
            req.metadata_mut().insert("authorization", header_value);
        }
        Ok(req)
    }
}

#[cfg(test)]
mod tests {
    use super::{BearerInterceptor, TokenManager, TokenManagerInner, TokenResponse, build_header};
    use std::sync::Arc;
    use std::sync::RwLock;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use tokio::sync::{Mutex, Notify};
    use tonic::Request;
    use tonic::metadata::{Ascii, MetadataValue};
    use tonic::service::Interceptor;

    /// Builds a `TokenManager` directly from `TokenManagerInner` (rather
    /// than via `TokenManager::new`, which performs a real HTTP round trip)
    /// so these tests stay fully offline. `token_url` is deliberately not a
    /// well-formed URL: `reqwest` fails to parse it and any `.send()` call
    /// resolves immediately with an error, without ever opening a socket —
    /// so a test that calls `refresh_now` here stays deterministic and
    /// network-free too.
    fn offline_token_manager(header_value: MetadataValue<Ascii>, expires_in: u64) -> TokenManager {
        TokenManager {
            inner: Arc::new(TokenManagerInner {
                http: reqwest::Client::new(),
                token_url: "this is not a url".to_string(),
                client_id: "unused-client-id".to_string(),
                client_secret: "unused-client-secret".to_string(),
                header_value: Arc::new(RwLock::new(header_value)),
                expires_in: AtomicU64::new(expires_in),
                refresh_lock: Mutex::new(()),
                refresh_generation: AtomicU64::new(0),
                refresh_notify: Notify::new(),
            }),
        }
    }

    fn sample_header(access_token: &str) -> MetadataValue<Ascii> {
        build_header(&TokenResponse {
            access_token: access_token.to_string(),
            expires_in: 600,
            token_type: "Bearer".to_string(),
        })
        .expect("a well-formed token response must build a valid header")
    }

    #[tokio::test]
    async fn request_refresh_stores_a_wake_permit_consumed_by_the_next_notified_await() {
        let manager = offline_token_manager(sample_header("token"), 600);

        // `request_refresh` is a plain, non-async function — calling it
        // with no `.await` is itself part of what this test locks in:
        // unlike `refresh_now`, it must never make the caller wait for a
        // network round trip.
        manager.request_refresh();

        // A bounded wait: if `request_refresh` regressed into a no-op,
        // `notified()` would never resolve on its own and this test would
        // hang instead of failing outright — the timeout turns that into a
        // clean, fast failure.
        tokio::time::timeout(
            Duration::from_millis(200),
            manager.inner.refresh_notify.notified(),
        )
        .await
        .expect(
            "request_refresh must store a wake permit that the next notified().await consumes \
             immediately, without needing a concurrently waiting task",
        );
    }

    #[tokio::test]
    async fn refresh_now_never_replaces_a_still_valid_cached_token_when_the_refresh_fails() {
        let original_header = sample_header("still-valid-token");
        let manager = offline_token_manager(original_header.clone(), 600);

        let result = manager.refresh_now().await;
        assert!(
            result.is_err(),
            "a malformed token_url must make refresh_now fail"
        );

        let current = manager
            .inner
            .header_value
            .read()
            .expect("header lock must not be poisoned");
        assert_eq!(
            *current, original_header,
            "a failed refresh must never replace a still-cached, still-valid header value"
        );
    }

    #[tokio::test]
    async fn refresh_now_coalesces_with_a_refresh_already_in_flight() {
        let manager = offline_token_manager(sample_header("token"), 600);

        // Hold the refresh lock ourselves to stand in for another
        // `refresh_now` call that is already in flight, then spawn a
        // second call. It must block on the same lock, exactly like a
        // genuinely concurrent caller would.
        let held = manager.inner.refresh_lock.lock().await;
        let waiting = manager.clone();
        let handle = tokio::spawn(async move { waiting.refresh_now().await });

        // `#[tokio::test]` runs on a current-thread runtime, so the spawned
        // task only makes progress when this task yields. One `yield_now`
        // is enough to let it read `refresh_generation` and then block on
        // `refresh_lock`, both of which happen before its first `.await`
        // that can actually suspend on contention.
        tokio::task::yield_now().await;

        // Simulate the in-flight refresh completing successfully: bump the
        // generation exactly as a real `refresh_now` would, immediately
        // before releasing the lock.
        manager
            .inner
            .refresh_generation
            .fetch_add(1, Ordering::Relaxed);
        drop(held);

        let result = handle
            .await
            .expect("the spawned refresh_now call must not panic");
        assert!(
            result.is_ok(),
            "a caller that observes the generation advance while waiting for the lock must \
             return Ok(()) without attempting its own fetch — one was attempted here, since \
             the offline harness's malformed token_url always fails: {result:?}"
        );
    }

    #[test]
    fn refresh_interval_applies_the_documented_margin_floor_and_ceiling() {
        // (expires_in reported by the IdP, expected refresh_interval)
        let cases = [
            // The documented steady-state case: the IdP's fixed 600-second
            // token yields a 400-second cadence, a third of its lifetime
            // still unused. Neither the floor nor the ceiling engages.
            (600, 400),
            // A representative shorter lifetime where the 2/3 margin still
            // clears the 5-second floor on its own.
            (90, 60),
            // A short lifetime where 2/3 of expires_in would fall under
            // MIN_REFRESH_INTERVAL: the floor engages, but the token still
            // outlives the floored interval, so the floor's 5 seconds
            // stands unclamped.
            (6, 5),
            // A lifetime shorter than the floor itself: without the
            // ceiling clamp, the floor alone would schedule the next
            // refresh at 5s, a full 2s after this 3s token has already
            // expired. The clamp must cap the result at expires_in - 1.
            (3, 2),
        ];

        for (expires_in, expected_secs) in cases {
            let manager = offline_token_manager(sample_header("token"), expires_in);
            assert_eq!(
                manager.refresh_interval(),
                Duration::from_secs(expected_secs),
                "refresh_interval() for expires_in = {expires_in}s"
            );
        }
    }

    #[test]
    fn call_attaches_the_cached_bearer_header_under_authorization_when_enabled() {
        let header_value = sample_header("test-access-token");
        let manager = offline_token_manager(header_value.clone(), 600);
        let mut interceptor = manager.interceptor();

        let request = interceptor
            .call(Request::new(()))
            .expect("an enabled interceptor with a valid cached header must never fail");

        assert_eq!(
            request.metadata().get("authorization"),
            Some(&header_value),
            "call() must attach the cached bearer header under the \"authorization\" key"
        );
    }

    #[test]
    fn call_leaves_metadata_untouched_when_disabled() {
        let mut interceptor = BearerInterceptor::disabled();

        let request = interceptor
            .call(Request::new(()))
            .expect("a disabled interceptor must never fail");

        assert!(
            request.metadata().is_empty(),
            "disabled() must leave outgoing request metadata untouched"
        );
    }
}
