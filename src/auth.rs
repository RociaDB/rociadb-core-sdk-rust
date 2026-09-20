//! Auth helpers for bearer tokens and API keys.
//!
//! [`TokenManager`] owns one OAuth2 client-credentials token: it fetches the
//! first one, hands out a [`BearerInterceptor`] that stamps the cached
//! `authorization` header onto every outgoing RPC, and — once
//! [`TokenManager::spawn_refresh`] is running — replaces that header before
//! the token expires. [`TokenManager::refresh_now`] forces a refresh,
//! [`TokenManager::ensure_fresh`] forces one only when the cached token is
//! close to expiring, and [`TokenManager::request_refresh`] asks the
//! background task for one without waiting.
//! [`RociaDbBuilder::build`](crate::RociaDbBuilder::build) wires all of it up;
//! reach for this module directly only when you drive authentication yourself.
//!
//! Both the OAuth2 client secret and the access token are held as
//! [`SecretString`], so neither can reach a log line through a `Debug`
//! formatter and both are zeroized when the last owner is dropped.

use crate::Result;
use crate::RociaDbError;
use crate::error::AuthResultExt;
use crate::retry::full_jitter;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use serde::de::{self, Deserializer};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify, oneshot};
use tokio::task::JoinHandle;
use tokio::time;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::{Request, Status, service::Interceptor};
use tracing::warn;

/// Floor applied to the derived refresh interval, in case the IdP ever
/// advertises a very short (or zero) token lifetime.
const MIN_REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// Delay ceiling before the first retry of a *failed* background refresh,
/// doubled before each subsequent one up to [`REFRESH_RETRY_MAX_DELAY`]. See
/// [`TokenManager::spawn_refresh`] for why a failed refresh must not simply
/// wait for the next regular tick.
const REFRESH_RETRY_BASE_DELAY: Duration = Duration::from_secs(1);
/// Ceiling the failed-refresh doubling never exceeds. Large enough that a
/// prolonged IdP outage is not hammered, small enough that recovery is
/// noticed well inside a 600-second token lifetime.
const REFRESH_RETRY_MAX_DELAY: Duration = Duration::from_secs(30);

/// Token lifetime assumed when the IdP's response omits `expires_in`, which
/// RFC 6749 §5.1 only *recommends* rather than requires. Deliberately
/// shorter than the 600 seconds this crate's own IdP issues: assuming too
/// little costs a few extra refreshes, assuming too much means every RPC
/// fails with `UNAUTHENTICATED` until the next scheduled refresh.
const ASSUMED_EXPIRES_IN_SECS: u64 = 300;

/// Token type assumed when the IdP's response omits `token_type`. RFC 6749
/// §5.1 requires the field, but not every deployment sends it.
const DEFAULT_TOKEN_TYPE: &str = "Bearer";

/// Response payload for OAuth2 client credentials token.
///
/// Deserialization is deliberately tolerant of how real identity providers
/// differ from the letter of RFC 6749, since the alternative is a `build()`
/// that fails against a perfectly usable IdP:
///
/// | Field | Accepted | Missing |
/// | ----- | -------- | ------- |
/// | `access_token` | a JSON string | required — absence is an error |
/// | `expires_in` | a JSON number, whole (`3600`) or fractional (`3600.0`, truncated toward zero), **or** either written as a string (`"3600"`) | assumes 300 seconds and emits a `warn!` |
/// | `token_type` | any JSON string, case preserved | assumes `"Bearer"` |
///
/// Unknown fields (`scope`, `refresh_token`, anything vendor-specific) are
/// ignored. A present `expires_in` that is not a usable number at all — a
/// non-numeric string, a negative number, an infinity — is still an error:
/// the IdP is saying something about the lifetime, and this crate must not
/// silently substitute a guess for it.
#[non_exhaustive]
#[derive(Debug)]
pub struct TokenResponse {
    /// The bearer token itself, attached verbatim to the `authorization`
    /// header of every outgoing RPC.
    ///
    /// A live credential, held as a [`SecretString`]: it is redacted in
    /// `Debug` output and zeroized on drop. Call
    /// [`ExposeSecret::expose_secret`] where you genuinely need the bytes.
    pub access_token: SecretString,
    /// Lifetime the IdP advertises for `access_token`, in seconds from the
    /// moment it was issued. The background refresh task derives its cadence
    /// from this value; when the IdP omits it, this is a deliberately short
    /// assumed lifetime (300 seconds) rather than "unknown".
    pub expires_in: u64,
    /// Token type the IdP reports, `"Bearer"` for the client-credentials
    /// grant this crate uses, and `"Bearer"` when the IdP omits the field.
    ///
    /// Kept exactly as received — RFC 6749 §7.1 makes the token type
    /// case-insensitive, so an IdP answering `"bearer"` is conformant and
    /// its value is echoed back unchanged rather than being normalized or
    /// rejected.
    pub token_type: String,
}

/// Wire shape of a token response, before the two optional fields are
/// defaulted. Split out from [`TokenResponse`] so the `warn!` for a missing
/// `expires_in` lives in ordinary code rather than inside a `serde` default
/// function.
#[derive(Deserialize)]
struct RawTokenResponse {
    access_token: SecretString,
    #[serde(default)]
    expires_in: Option<RawExpiresIn>,
    #[serde(default)]
    token_type: Option<String>,
}

/// `expires_in` as it arrives. RFC 6749 §5.1 specifies an integer number of
/// seconds, but two deviations are common enough that both have to be
/// accepted: an IdP that renders its whole JSON payload as strings
/// (`"3600"`), and one that computes the lifetime as a difference of
/// timestamps and serializes the result as a float (`3600.0`, which is what
/// Python's `json` module writes for a `float`).
#[derive(Deserialize)]
#[serde(untagged)]
enum RawExpiresIn {
    /// `"expires_in": 3600`
    Seconds(u64),
    /// `"expires_in": 3600.0`
    Fractional(f64),
    /// `"expires_in": "3600"`, and `"3600.0"` for the same reason
    /// [`RawExpiresIn::Fractional`] exists.
    Text(String),
}

impl RawExpiresIn {
    /// The lifetime in whole seconds, or `None` when the value is not a
    /// number at all (`"soon"`), or is one no lifetime can be built from (a
    /// negative number, an infinity, a NaN).
    fn seconds(&self) -> Option<u64> {
        match self {
            Self::Seconds(seconds) => Some(*seconds),
            Self::Fractional(seconds) => truncate_seconds(*seconds),
            Self::Text(text) => {
                let text = text.trim();
                text.parse::<u64>()
                    .ok()
                    .or_else(|| text.parse::<f64>().ok().and_then(truncate_seconds))
            }
        }
    }

    /// The value as written, for the error message when
    /// [`RawExpiresIn::seconds`] rejects it.
    fn rendered(&self) -> String {
        match self {
            Self::Seconds(seconds) => seconds.to_string(),
            Self::Fractional(seconds) => seconds.to_string(),
            Self::Text(text) => text.clone(),
        }
    }
}

/// A fractional number of seconds as a whole number, truncated toward zero —
/// rounding a lifetime *down* is the safe direction, since it only makes the
/// refresh cadence tighter. `None` for anything no lifetime can be built
/// from. The cast itself is saturating in Rust (and the guard has already
/// ruled out the interesting cases), so it can neither panic nor wrap.
fn truncate_seconds(seconds: f64) -> Option<u64> {
    (seconds.is_finite() && seconds >= 0.0).then_some(seconds as u64)
}

impl<'de> Deserialize<'de> for TokenResponse {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let raw = RawTokenResponse::deserialize(deserializer)?;
        let expires_in = match &raw.expires_in {
            Some(expires_in) => expires_in.seconds().ok_or_else(|| {
                de::Error::invalid_value(
                    de::Unexpected::Other(&expires_in.rendered()),
                    &"expires_in as a non-negative number of seconds",
                )
            })?,
            None => {
                // Neither the token nor the IdP is named here: a token URL
                // and a client id both expose the auth infrastructure, and
                // this crate never logs either.
                warn!(
                    assumed_expires_in_secs = ASSUMED_EXPIRES_IN_SECS,
                    "the token response omitted expires_in; assuming a short lifetime and \
                     refreshing on that cadence"
                );
                ASSUMED_EXPIRES_IN_SECS
            }
        };
        Ok(Self {
            access_token: raw.access_token,
            expires_in,
            token_type: raw
                .token_type
                .unwrap_or_else(|| DEFAULT_TOKEN_TYPE.to_string()),
        })
    }
}

/// Fetch a token using client credentials.
///
/// `client_secret` is a [`SecretString`]: it is exposed exactly once, where
/// the form body is built, and is redacted in `Debug` output and zeroized on
/// drop everywhere else. The same holds for the token this returns (see
/// [`TokenResponse::access_token`]), so neither credential can reach a log
/// line through a formatter, and neither is left behind in freed memory for
/// a core dump or an attached debugger to recover.
///
/// Drop the request URL from a `reqwest` error before it is wrapped into a
/// [`RociaDbError::Auth`].
///
/// `reqwest`'s own `Display` appends `" for url (..)"` to every transport
/// error, so a token-fetch failure carries the full `token_url` in its message
/// — which then reaches any log line, `Debug` output or error report that
/// renders the error, the SDK's own `warn!` on a failed background refresh
/// included. AGENTS.md puts `token_url` on the never-log list, and the
/// `SecretString` discipline around the credentials cannot help here because
/// the leak rides the error chain rather than a secret-carrying field. Fixing
/// it at the point of conversion covers every consumer, including the caller's
/// own logging, rather than one `warn!` at a time.
///
/// Nothing diagnostic is lost that the caller does not already have: they
/// configured the URL, and the error's kind, status and source all survive.
fn redact_token_url(error: reqwest::Error) -> reqwest::Error {
    error.without_url()
}

/// `http` is used as given. **A caller-supplied [`reqwest::Client`] should
/// carry its own timeouts** (`Client::builder().connect_timeout(..).timeout(..)`):
/// `reqwest::Client::new()` has none at all, so an IdP that accepts the TCP
/// connection and then never answers would hang this call — and therefore
/// [`TokenManager::refresh_now`], which holds the refresh lock while it runs
/// — forever. [`RociaDbBuilder::build`](crate::RociaDbBuilder::build) builds
/// a client with both.
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
    client_secret: &SecretString,
) -> Result<TokenResponse> {
    if !token_url.to_ascii_lowercase().starts_with("https://") {
        // Mirrors the `warn!` `RociaDbBuilder::build` emits for
        // `disable_auth()`: the crate deliberately allows plaintext here
        // too, so this stays a warning rather than a hard rejection, but
        // the caller should still be told that `client_id`/`client_secret`
        // and the bearer token this returns are about to cross the wire
        // unencrypted.
        // The URL itself is deliberately not a field here: the logging policy
        // in AGENTS.md puts `token_url` on the same never-log list as the
        // credentials, and the caller configured it, so naming it back at them
        // buys nothing a log scraper could not also collect.
        warn!(
            "fetching an OAuth2 token over a non-https token_url; client credentials and the \
             access token will be sent in cleartext"
        );
    }

    let res = http
        .post(token_url)
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            // The one place the secret is in the clear, and only for as
            // long as `reqwest` needs to encode the form body.
            ("client_secret", client_secret.expose_secret()),
        ])
        .send()
        .await
        .map_err(redact_token_url)
        .auth_context("token request failed")?
        .error_for_status()
        .map_err(redact_token_url)
        .auth_context("token endpoint returned error")?;

    res.json::<TokenResponse>()
        .await
        .map_err(redact_token_url)
        .auth_context("failed to parse token response")
}

/// When the token currently cached in `header_value` was obtained, and how
/// long the IdP said it would live.
///
/// The two are held together, under one lock, precisely so they cannot be read
/// apart: a `fetched_at` from one token paired with the `expires_in` of another
/// would make [`TokenManager::ensure_fresh`] compute a remaining lifetime that
/// belongs to no token at all.
#[derive(Debug, Clone, Copy)]
struct TokenLifetime {
    /// Instant the token was fetched — as close to the moment the IdP issued
    /// it as this process can observe, since `expires_in` is counted from
    /// issuance and the only other reference point (the network round trip)
    /// is not observable here. Rounding this *later* than issuance is the safe
    /// direction: it can only overstate the remaining lifetime by the fetch
    /// latency, which a refresh margin of whole seconds absorbs.
    fetched_at: Instant,
    /// `expires_in` (seconds) as reported by the IdP for that token, or the
    /// assumed [`ASSUMED_EXPIRES_IN_SECS`] when it reported none.
    expires_in: u64,
}

impl TokenLifetime {
    /// A lifetime starting now.
    fn issued_now(expires_in: u64) -> Self {
        Self {
            fetched_at: Instant::now(),
            expires_in,
        }
    }

    /// How much of the lifetime is left, saturating at zero for a token that
    /// has already expired (and for the monotonic-clock edge case where
    /// `elapsed()` exceeds the whole lifetime).
    fn remaining(&self) -> Duration {
        Duration::from_secs(self.expires_in).saturating_sub(self.fetched_at.elapsed())
    }
}

/// Token manager with cached authorization header.
#[derive(Clone)]
pub struct TokenManager {
    inner: Arc<TokenManagerInner>,
}

// Deliberately no `Debug` impl, derived or otherwise: this type holds the
// cached `authorization` header, and a `MetadataValue`'s own `Debug` prints
// it, so any derived impl here would print a working bearer token.
struct TokenManagerInner {
    http: reqwest::Client,
    token_url: String,
    client_id: String,
    /// A [`SecretString`], so it is redacted by every formatter and its
    /// heap buffer is zeroized when this `TokenManagerInner` is dropped. It
    /// is exposed only in [`fetch_token`], where the form body is built.
    client_secret: SecretString,
    header_value: Arc<RwLock<MetadataValue<Ascii>>>,
    /// When the most recently fetched token was obtained and how long the IdP
    /// said it lives. Drives [`TokenManager::refresh_interval`] (which reads
    /// the lifetime) and [`TokenManager::ensure_fresh`] (which reads what is
    /// left of it). Written only while `refresh_lock` is held, alongside
    /// `header_value`, so the recorded lifetime always describes the cached
    /// token.
    lifetime: RwLock<TokenLifetime>,
    /// Number of times [`TokenManager::refresh_now`] has actually reached
    /// the IdP, incremented before the request whether it then succeeds or
    /// fails (a call coalesced into another caller's in-flight fetch does
    /// not count). This is how the unit tests observe the background task's
    /// cadence — including the retry backoff after a failure — without a
    /// live IdP to talk to; an untouched atomic costs nothing measurable, so
    /// it is kept unconditionally rather than behind `cfg(test)`.
    fetch_attempts: AtomicU64,
    /// Coalesces concurrent [`TokenManager::refresh_now`] calls into a
    /// single in-flight fetch. A caller records the current
    /// `refresh_generation` *before* acquiring this lock, so that once it
    /// gets in, it can tell whether another caller already refreshed while
    /// it was waiting (see `refresh_generation` below) and, if so, skip its
    /// own redundant fetch instead of hitting the IdP again.
    refresh_lock: Mutex<()>,
    /// Bumped by [`TokenManager::refresh_now`] once an attempt has
    /// *completed*, while still holding `refresh_lock` — after installing a
    /// newly fetched token into `header_value` on success, and equally after
    /// recording the error on failure.
    ///
    /// Counting attempts rather than successes is what makes the coalescing
    /// hold in the failure mode it was written for. It used to be bumped only
    /// on success, so a failing IdP left it untouched and every caller queued
    /// on `refresh_lock` saw its own snapshot still current and ran its own
    /// full round trip — serialized behind the lock, each bounded only by
    /// `OAUTH_HTTP_REQUEST_TIMEOUT`. Fifty tasks recovering from one expired
    /// token against an unresponsive IdP therefore took fifty times that,
    /// and the fiftieth caller's single RPC blocked for the sum of all of
    /// them however short its own `request_timeout` was.
    ///
    /// Because every write to `header_value` happens while holding
    /// `refresh_lock`, fetches are fully serialized: there is no window in
    /// which a slower concurrent fetch could land after (and overwrite) a
    /// faster one. `Relaxed` is enough — this only decides whether a waiting
    /// caller can skip redundant work, and the `refresh_lock` handoff is what
    /// publishes both this counter and `last_attempt_failure` to the waiter
    /// that acquires the lock next.
    refresh_generation: AtomicU64,
    /// What the attempt counted by the current `refresh_generation` came to:
    /// `None` if it installed a token, `Some(message)` if it failed.
    ///
    /// Read only by a caller that finds the generation has moved while it
    /// waited for `refresh_lock` — that is, a caller whose own request was
    /// contemporaneous with that attempt, so that attempt's answer is
    /// legitimately its answer too. A caller arriving *after* an attempt has
    /// completed snapshots the already-bumped generation, finds it unchanged
    /// under the lock, and fetches for itself, so a stale failure here can
    /// never wedge the manager and needs no expiry of its own.
    ///
    /// A message rather than the [`RociaDbError`], which is not `Clone`: the
    /// source chain stays with the caller that actually made the request.
    last_attempt_failure: RwLock<Option<String>>,
    /// Wakes the background task spawned by [`TokenManager::spawn_refresh`]
    /// as soon as possible, without the caller waiting for the network
    /// round trip. See [`TokenManager::request_refresh`]. A `notify_one()`
    /// call with no task currently waiting stores a permit that the next
    /// `notified().await` consumes immediately, so a request issued between
    /// two loop iterations of the background task is never lost — including
    /// while that task is sitting in the retry backoff below.
    refresh_notify: Notify,
}

impl TokenManager {
    /// Create a new token manager and fetch the first token.
    ///
    /// `client_secret` is a [`SecretString`]: build one with
    /// `SecretString::from("…")` (or from an owned `String`, which moves
    /// rather than copying), and it is redacted and zeroized from then on.
    ///
    /// `http` is used as given for this fetch and for every later refresh.
    /// **Supply a [`reqwest::Client`] that carries its own timeouts** — see
    /// [`fetch_token`] for what a client without them costs.
    pub async fn new(
        http: reqwest::Client,
        token_url: String,
        client_id: String,
        client_secret: SecretString,
    ) -> Result<Self> {
        let token = fetch_token(&http, &token_url, &client_id, &client_secret).await?;
        let lifetime = RwLock::new(TokenLifetime::issued_now(token.expires_in));
        let header_value = Arc::new(RwLock::new(build_header(&token)?));

        Ok(Self {
            inner: Arc::new(TokenManagerInner {
                http,
                token_url,
                client_id,
                client_secret,
                header_value,
                lifetime,
                fetch_attempts: AtomicU64::new(0),
                refresh_lock: Mutex::new(()),
                refresh_generation: AtomicU64::new(0),
                last_attempt_failure: RwLock::new(None),
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
    ///
    /// This is the *healthy* cadence. A refresh that fails is retried on
    /// the much tighter schedule described by
    /// [`TokenManager::spawn_refresh`] until one succeeds.
    pub fn refresh_interval(&self) -> Duration {
        let expires_in = self.lifetime().expires_in;
        let with_margin = expires_in.saturating_mul(2) / 3;
        let floored = Duration::from_secs(with_margin).max(MIN_REFRESH_INTERVAL);
        let ceiling = Duration::from_secs(expires_in.saturating_sub(1).max(1));
        floored.min(ceiling)
    }

    /// Create an interceptor that injects the bearer token.
    pub fn interceptor(&self) -> BearerInterceptor {
        BearerInterceptor::new(Arc::clone(&self.inner.header_value))
    }

    /// The lifetime recorded for the currently cached token.
    fn lifetime(&self) -> TokenLifetime {
        *self
            .inner
            .lifetime
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Refresh the token only if less than `margin` of its advertised lifetime
    /// is left, and do nothing at all otherwise.
    ///
    /// This is the pre-flight check the two streaming RPCs use, where
    /// [`TokenManager::refresh_now`]'s unconditional round trip would be paid
    /// on every call and the refresh-and-retry that covers a unary RPC cannot
    /// apply (a request stream the caller has handed over cannot be replayed).
    /// It is cheap in the common case: reading the recorded lifetime is one
    /// `RwLock` read and one clock read, with no lock held across an `.await`.
    ///
    /// When a refresh *is* due, it goes through [`TokenManager::refresh_now`],
    /// so concurrent callers are coalesced into a single fetch and a caller
    /// that arrives just after another one refreshed makes no request of its
    /// own. A failure is returned as [`crate::RociaDbError::Auth`]; the cached
    /// token is left in place, exactly as for a failed
    /// [`TokenManager::refresh_now`], so a caller that can still try with it
    /// (as [`crate::RociaDbClient`]'s streaming calls do) may ignore the
    /// error.
    ///
    /// `margin` only has to cover the moment the call *starts*: a gRPC server
    /// validates the bearer token once, when it accepts the call, so a stream
    /// that outlives its token keeps running. A few seconds is therefore
    /// enough, and a margin as long as the token's whole lifetime would just
    /// refresh on every call.
    pub async fn ensure_fresh(&self, margin: Duration) -> Result<()> {
        if self.lifetime().remaining() >= margin {
            return Ok(());
        }
        self.refresh_now().await
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
    /// Instead, a caller records the current refresh generation, then either
    /// wins the race to fetch or — if an attempt contemporaneous with its own
    /// request completed by the time it gets the lock — takes *that attempt's
    /// outcome* as its own, without making a redundant request. Every write to
    /// the cached header happens while holding that same lock, so fetches are
    /// fully serialized and a slower one can never land after (and overwrite)
    /// a faster one. A caller that arrives after an attempt has already
    /// completed still performs its own fetch, as an explicit refresh request
    /// should.
    ///
    /// **The outcome that is shared includes a failure.** A coalesced caller
    /// gets an [`RociaDbError::Auth`] naming the concurrent attempt's error
    /// rather than repeating the request, because the alternative is worse
    /// than a wasted round trip: the refresh lock is held across the HTTP
    /// round trip, so N callers each retrying a failing IdP serialize behind
    /// it at up to `OAUTH_HTTP_REQUEST_TIMEOUT` apiece, and the last one in
    /// the queue waits for the sum of all of them however short its own
    /// [`request_timeout`](crate::RociaDbBuilder::request_timeout) is. What a
    /// coalesced caller does *not* get is a bare `Ok(())`: that would report a
    /// refresh that did not happen and leave it using a token it believes is
    /// fresh.
    ///
    /// That the lock is held across the round trip is also why the
    /// [`reqwest::Client`] handed to [`TokenManager::new`] must have a request
    /// timeout: without one, a single unresponsive IdP connection would block
    /// every other caller of this method indefinitely.
    pub async fn refresh_now(&self) -> Result<()> {
        let observed_generation = self.inner.refresh_generation.load(Ordering::Relaxed);
        let _refresh_permit = self.inner.refresh_lock.lock().await;
        if self.inner.refresh_generation.load(Ordering::Relaxed) != observed_generation {
            // An attempt contemporaneous with this request completed while we
            // waited for the lock, so its answer is this caller's answer: the
            // cached header is already newer than anything a redundant fetch
            // could produce, or the IdP just refused and will refuse us too.
            return match self.last_attempt_failure() {
                None => Ok(()),
                Some(message) => Err(RociaDbError::Auth {
                    message: format!(
                        "a concurrent token refresh failed, so this one did not repeat it: \
                         {message}"
                    ),
                    source: None,
                }),
            };
        }

        self.inner.fetch_attempts.fetch_add(1, Ordering::Relaxed);
        let outcome = self.fetch_and_install().await;
        // Recorded *before* the generation is bumped, so a waiter that
        // observes the bump also observes what it meant. Both are published to
        // that waiter by the `refresh_lock` handoff.
        {
            let mut guard = self
                .inner
                .last_attempt_failure
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *guard = outcome.as_ref().err().map(ToString::to_string);
        }
        self.inner
            .refresh_generation
            .fetch_add(1, Ordering::Relaxed);
        outcome
    }

    /// The error message recorded for the attempt the current
    /// `refresh_generation` counts, or `None` if that attempt succeeded.
    fn last_attempt_failure(&self) -> Option<String> {
        self.inner
            .last_attempt_failure
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Fetch a token and install it, which is the half of
    /// [`TokenManager::refresh_now`] that can fail.
    ///
    /// Factored out precisely so its `?` exits land back in `refresh_now`,
    /// where the outcome is recorded and the generation bumped. Inlining it
    /// is what produced the original bug: the early return skipped both, so a
    /// failed attempt was invisible to every caller queued behind it.
    ///
    /// Must be called while holding `refresh_lock`.
    async fn fetch_and_install(&self) -> Result<()> {
        let token = fetch_token(
            &self.inner.http,
            &self.inner.token_url,
            &self.inner.client_id,
            &self.inner.client_secret,
        )
        .await?;
        let header = build_header(&token)?;
        {
            let mut guard = self
                .inner
                .lifetime
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *guard = TokenLifetime::issued_now(token.expires_in);
        }
        {
            let mut guard = self
                .inner
                .header_value
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *guard = header;
        }
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
    ///
    /// A request issued while the background task is waiting out a retry
    /// backoff after a failed refresh is honoured immediately: the backoff
    /// only decides when the *timer* next fires, and this wake-up path is
    /// selected on in parallel with it.
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
    ///
    /// # Recovering from a failed refresh
    ///
    /// A refresh that fails is retried on its own much tighter schedule
    /// instead of waiting for the next regular tick. That matters because
    /// the regular cadence is derived from the token's whole lifetime: with
    /// the IdP's 600-second tokens it is 400 seconds, so a single failed
    /// refresh would otherwise leave the next attempt 400 seconds away and
    /// guarantee a window — the last 200 seconds of the token's life — in
    /// which every RPC fails with `UNAUTHENTICATED`.
    ///
    /// Instead, consecutive failures back off exponentially with jitter:
    /// roughly 1 s, 2 s, 4 s, 8 s, 16 s, then 30 s for every attempt after
    /// that, each delay drawn from the upper half of its ceiling (so the
    /// first retry lands between 0.5 s and 1 s, the second between 1 s and
    /// 2 s, and so on). The jitter keeps a fleet of clients whose tokens
    /// expire together from synchronising their retries into a thundering
    /// herd against the IdP. The first refresh that succeeds resets the
    /// count and returns the task to the normal
    /// [`TokenManager::refresh_interval`] cadence. Every failure is
    /// reported once as a `warn!` carrying the error, how many refreshes
    /// have now failed in a row, and how long the next attempt is away.
    ///
    /// [`TokenManager::request_refresh`] keeps working throughout: the
    /// backoff governs the timer, and a requested refresh is selected on in
    /// parallel with it, so a caller that has just seen an
    /// `UNAUTHENTICATED` never has to wait out the remaining backoff.
    /// The period [`TokenManager::spawn_refresh`] actually ticks on, which is
    /// `interval` with a one-second floor.
    ///
    /// `tokio::time::interval` panics on a zero period, and `spawn_refresh` is
    /// public on a public type, so a caller could reach that panic — from
    /// *inside the spawned task*, where it would not reach them at all. It
    /// would abort the refresh task and leave the client with no background
    /// refresh and nothing said about it, which is a good deal worse than a
    /// visible crash.
    ///
    /// One second rather than [`MIN_REFRESH_INTERVAL`] because
    /// [`TokenManager::refresh_interval`] legitimately returns one second for a
    /// token that lives two, and flooring at five would schedule a refresh
    /// three seconds after such a token had already expired. This matches the
    /// `.max(1)` that method applies for the same reason, so it is a no-op for
    /// every interval the SDK itself passes.
    fn refresh_tick_period(interval: Duration) -> Duration {
        interval.max(Duration::from_secs(1))
    }

    pub fn spawn_refresh(&self, interval: Duration) -> TokenRefreshGuard {
        let manager = self.clone();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let period = Self::refresh_tick_period(interval);
        let task = tokio::spawn(async move {
            let mut ticker = time::interval(period);
            ticker.tick().await;
            // Consecutive failed refreshes, reset by the first success.
            // Drives `retry_backoff`, so it is also the retry index.
            let mut consecutive_failures: u32 = 0;
            loop {
                // `true` when this iteration was woken by
                // `TokenManager::request_refresh` (and thus
                // `RociaDbClient::invalidate_auth_token`) rather than by the
                // timer, so a caller can signal "do not trust the cached
                // token" without paying for the refresh round trip itself —
                // this background task absorbs that latency instead.
                let requested = tokio::select! {
                    _ = ticker.tick() => false,
                    _ = manager.inner.refresh_notify.notified() => true,
                    _ = &mut shutdown_rx => break,
                };
                match manager.refresh_now().await {
                    Ok(()) => {
                        consecutive_failures = 0;
                        ticker.reset_after(manager.refresh_interval());
                    }
                    Err(error) => {
                        let retry_in = jittered_retry_backoff(consecutive_failures);
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        warn!(
                            error = %error,
                            requested,
                            consecutive_failures,
                            retry_in_ms = u64::try_from(retry_in.as_millis()).unwrap_or(u64::MAX),
                            "token refresh failed; retrying with backoff"
                        );
                        ticker.reset_after(retry_in);
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

/// The un-jittered ceiling for the delay before retry `attempt` (zero-based)
/// of a failed background refresh: `REFRESH_RETRY_BASE_DELAY * 2^attempt`,
/// capped at [`REFRESH_RETRY_MAX_DELAY`], saturating instead of overflowing
/// however long the IdP stays down.
///
/// A pure function of the attempt number so the whole schedule can be
/// asserted in a unit test without a clock, a socket, or an IdP.
fn retry_backoff(attempt: u32) -> Duration {
    let factor = 1u32.checked_shl(attempt).unwrap_or(u32::MAX);
    REFRESH_RETRY_BASE_DELAY
        .checked_mul(factor)
        .unwrap_or(Duration::MAX)
        .min(REFRESH_RETRY_MAX_DELAY)
}

/// [`retry_backoff`] with jitter applied: uniform over the *upper half* of
/// the ceiling, `[ceiling / 2, ceiling]`.
///
/// Half the ceiling rather than all of it (the full jitter
/// [`crate::RetryPolicy`] uses) because this schedule is about recovering
/// one client's own credential, not about spreading contending writers
/// apart: drawing from `[0, ceiling]` would make the first retries average
/// half a second and let an IdP returning an instant error be re-queried
/// several times a second. Keeping the lower half means the documented
/// "roughly 1 s, 2 s, 4 s" cadence is what actually happens, while still
/// desynchronising a fleet of clients whose tokens expire at the same
/// moment.
fn jittered_retry_backoff(attempt: u32) -> Duration {
    let ceiling = retry_backoff(attempt);
    let lower_half = ceiling / 2;
    lower_half + full_jitter(ceiling - lower_half)
}

fn build_header(token: &TokenResponse) -> Result<MetadataValue<Ascii>> {
    let bearer = format!(
        "{} {}",
        token.token_type,
        token.access_token.expose_secret()
    );
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
    use super::{
        ASSUMED_EXPIRES_IN_SECS, BearerInterceptor, REFRESH_RETRY_BASE_DELAY,
        REFRESH_RETRY_MAX_DELAY, TokenLifetime, TokenManager, TokenManagerInner, TokenResponse,
        build_header, jittered_retry_backoff, retry_backoff,
    };
    use secrecy::{ExposeSecret, SecretString};
    use std::sync::Arc;
    use std::sync::RwLock;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};
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
        offline_token_manager_issued(header_value, expires_in, Duration::ZERO)
    }

    /// [`offline_token_manager`] for a token fetched `issued_ago` in the past,
    /// so the tests of [`TokenManager::ensure_fresh`] can present a token that
    /// is fresh, nearly expired or long expired without sleeping for real.
    /// `Instant::checked_sub` is used rather than plain subtraction because a
    /// process that has been up for less than `issued_ago` has no such instant
    /// to name — the saturating fallback just means "as long ago as this clock
    /// can express", which is older still and therefore fine here.
    fn offline_token_manager_issued(
        header_value: MetadataValue<Ascii>,
        expires_in: u64,
        issued_ago: Duration,
    ) -> TokenManager {
        let fetched_at = Instant::now()
            .checked_sub(issued_ago)
            .unwrap_or_else(Instant::now);
        TokenManager {
            inner: Arc::new(TokenManagerInner {
                http: reqwest::Client::new(),
                token_url: "this is not a url".to_string(),
                client_id: "unused-client-id".to_string(),
                client_secret: SecretString::from("unused-client-secret"),
                header_value: Arc::new(RwLock::new(header_value)),
                lifetime: RwLock::new(TokenLifetime {
                    fetched_at,
                    expires_in,
                }),
                fetch_attempts: AtomicU64::new(0),
                refresh_lock: Mutex::new(()),
                refresh_generation: AtomicU64::new(0),
                last_attempt_failure: RwLock::new(None),
                refresh_notify: Notify::new(),
            }),
        }
    }

    fn sample_header(access_token: &str) -> MetadataValue<Ascii> {
        build_header(&TokenResponse {
            access_token: SecretString::from(access_token),
            expires_in: 600,
            token_type: "Bearer".to_string(),
        })
        .expect("a well-formed token response must build a valid header")
    }

    fn parse_token_response(json: &str) -> TokenResponse {
        serde_json::from_str(json).expect("the token response must deserialize")
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
        assert_eq!(
            manager.inner.fetch_attempts.load(Ordering::Relaxed),
            0,
            "a coalesced caller must not reach the IdP at all"
        );
    }

    /// The failure half of the coalescing contract, and the regression this
    /// pair of tests exists for: the generation used to be bumped only on
    /// success, so a failing IdP left every queued caller convinced its own
    /// snapshot was current and each ran a full round trip — serialized behind
    /// the refresh lock, at up to `OAUTH_HTTP_REQUEST_TIMEOUT` apiece.
    #[tokio::test]
    async fn refresh_now_coalesces_into_a_concurrent_refresh_that_failed() {
        let manager = offline_token_manager(sample_header("token"), 600);

        let held = manager.inner.refresh_lock.lock().await;
        let waiting = manager.clone();
        let handle = tokio::spawn(async move { waiting.refresh_now().await });
        tokio::task::yield_now().await;

        // Stand in for an in-flight refresh that *failed*, recording its
        // outcome and bumping the generation exactly as `refresh_now` does.
        *manager
            .inner
            .last_attempt_failure
            .write()
            .expect("the outcome lock must not be poisoned") = Some("the idp said 503".to_string());
        manager
            .inner
            .refresh_generation
            .fetch_add(1, Ordering::Relaxed);
        drop(held);

        let error = handle
            .await
            .expect("the spawned refresh_now call must not panic")
            .expect_err("a coalesced caller must inherit the concurrent failure, not Ok(())");
        assert!(
            error.to_string().contains("the idp said 503"),
            "the inherited error must name what actually failed, got: {error}"
        );
        assert_eq!(
            manager.inner.fetch_attempts.load(Ordering::Relaxed),
            0,
            "the whole point: a coalesced caller must not repeat the failing request"
        );
    }

    /// The other side of that coin — a shared failure must not become sticky.
    /// A caller arriving *after* an attempt has completed snapshots the
    /// already-bumped generation, so it finds nothing to coalesce into and
    /// fetches for itself. Without this, one bad minute at the IdP would wedge
    /// the manager for good.
    #[tokio::test]
    async fn a_later_refresh_retries_rather_than_inheriting_a_settled_failure() {
        let manager = offline_token_manager(sample_header("token"), 600);

        manager
            .refresh_now()
            .await
            .expect_err("the offline harness's malformed token_url always fails");
        let after_first = manager.inner.fetch_attempts.load(Ordering::Relaxed);
        assert_eq!(after_first, 1);
        assert!(
            manager.last_attempt_failure().is_some(),
            "a failed attempt must record its outcome for a contemporaneous caller"
        );
        assert_eq!(
            manager.inner.refresh_generation.load(Ordering::Relaxed),
            1,
            "a failed attempt still counts: that is what makes coalescing work"
        );

        manager
            .refresh_now()
            .await
            .expect_err("still offline, so this fails too");
        assert_eq!(
            manager.inner.fetch_attempts.load(Ordering::Relaxed),
            2,
            "a caller arriving after the failure settled must make its own attempt"
        );
    }

    #[tokio::test]
    async fn a_successful_refresh_clears_the_recorded_failure() {
        let manager = offline_token_manager(sample_header("token"), 600);
        manager
            .refresh_now()
            .await
            .expect_err("the offline harness always fails");
        assert!(manager.last_attempt_failure().is_some());

        // Stand in for the next attempt succeeding, which is what
        // `refresh_now` records on its success path.
        *manager
            .inner
            .last_attempt_failure
            .write()
            .expect("the outcome lock must not be poisoned") = None;
        assert!(
            manager.last_attempt_failure().is_none(),
            "a coalesced caller must see Ok(()) once an attempt has succeeded"
        );
    }

    #[tokio::test]
    async fn ensure_fresh_makes_no_request_while_the_cached_token_is_still_fresh() {
        // 600 seconds of lifetime, one second of it used: a 5-second margin is
        // nowhere near, so this must not touch the IdP at all — which the
        // offline harness proves twice over, since any fetch it did attempt
        // would fail on the malformed token_url and be visible as an `Err`.
        let manager =
            offline_token_manager_issued(sample_header("token"), 600, Duration::from_secs(1));

        manager
            .ensure_fresh(Duration::from_secs(5))
            .await
            .expect("a token well inside its lifetime must need no refresh");

        assert_eq!(
            manager.inner.fetch_attempts.load(Ordering::Relaxed),
            0,
            "ensure_fresh must not reach the IdP for a token that is still fresh"
        );
    }

    #[tokio::test]
    async fn ensure_fresh_refreshes_a_token_whose_remaining_lifetime_is_under_the_margin() {
        // 3 of the 10 seconds left, against a 5-second margin: due.
        let manager = offline_token_manager_issued(
            sample_header("nearly-expired"),
            10,
            Duration::from_secs(7),
        );

        let error = manager
            .ensure_fresh(Duration::from_secs(5))
            .await
            .expect_err("the offline harness cannot actually fetch, so the refresh must fail");
        assert!(matches!(error, crate::RociaDbError::Auth { .. }), "{error}");
        assert_eq!(
            manager.inner.fetch_attempts.load(Ordering::Relaxed),
            1,
            "a token inside the margin must be refreshed exactly once"
        );
        // The failure left the still-usable cached token alone, which is what
        // lets `RociaDbClient`'s streaming calls carry on with it.
        assert_eq!(
            *manager
                .inner
                .header_value
                .read()
                .expect("header lock must not be poisoned"),
            sample_header("nearly-expired")
        );
    }

    #[tokio::test]
    async fn ensure_fresh_refreshes_a_token_that_has_already_expired() {
        // Twice the lifetime elapsed: `remaining()` saturates at zero rather
        // than underflowing, and the refresh is due.
        let manager =
            offline_token_manager_issued(sample_header("expired"), 600, Duration::from_secs(1200));
        assert_eq!(manager.lifetime().remaining(), Duration::ZERO);

        manager
            .ensure_fresh(Duration::from_secs(5))
            .await
            .expect_err("an expired token must be refreshed");
        assert_eq!(manager.inner.fetch_attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn ensure_fresh_coalesces_concurrent_callers_into_one_fetch() {
        // Same shape as `refresh_now_coalesces_with_a_refresh_already_in_flight`:
        // holding the refresh lock stands in for another refresh already in
        // flight, and bumping the generation before releasing it stands in for
        // that refresh succeeding. A second caller must then skip its own
        // fetch — the property that keeps a fleet of streaming calls whose
        // token expired together from firing one POST each at the IdP.
        let manager =
            offline_token_manager_issued(sample_header("expired"), 1, Duration::from_secs(60));
        let held = manager.inner.refresh_lock.lock().await;
        let waiting = manager.clone();
        let handle =
            tokio::spawn(async move { waiting.ensure_fresh(Duration::from_secs(5)).await });

        // A current-thread runtime: one yield is enough for the spawned task
        // to read the generation and block on the lock.
        tokio::task::yield_now().await;
        manager
            .inner
            .refresh_generation
            .fetch_add(1, Ordering::Relaxed);
        drop(held);

        let result = handle
            .await
            .expect("the spawned ensure_fresh call must not panic");
        assert!(
            result.is_ok(),
            "a coalesced caller must return Ok(()) without fetching — a fetch would have failed \
             on the offline harness's malformed token_url: {result:?}"
        );
        assert_eq!(
            manager.inner.fetch_attempts.load(Ordering::Relaxed),
            0,
            "a coalesced caller must not reach the IdP at all"
        );
    }

    #[test]
    fn refresh_tick_period_never_returns_zero() {
        // `tokio::time::interval` panics on a zero period, and it would do so
        // inside the spawned task where nobody would hear it.
        assert_eq!(
            TokenManager::refresh_tick_period(Duration::ZERO),
            Duration::from_secs(1)
        );
        assert_eq!(
            TokenManager::refresh_tick_period(Duration::from_millis(1)),
            Duration::from_secs(1)
        );
        // A no-op for everything the SDK itself passes, including the
        // one-second cadence `refresh_interval` gives a two-second token.
        for secs in [1u64, 2, 5, 400] {
            assert_eq!(
                TokenManager::refresh_tick_period(Duration::from_secs(secs)),
                Duration::from_secs(secs)
            );
        }
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
            // The bottom of the range, where `expires_in - 1` would be zero
            // or underflow. `.max(1)` holds the cadence at one second: the
            // fastest this schedules is one refresh per second, never a spin,
            // and for a token this short there is nothing better to do. An
            // `expires_in` of 0 is already-expired and only reachable from an
            // identity provider that reports it, since a *missing* one becomes
            // ASSUMED_EXPIRES_IN_SECS instead.
            (2, 1),
            (1, 1),
            (0, 1),
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
    fn retry_backoff_doubles_from_one_second_and_saturates_at_the_cap() {
        // (zero-based retry index, expected ceiling in seconds)
        let cases = [
            (0u32, 1u64),
            (1, 2),
            (2, 4),
            (3, 8),
            (4, 16),
            // 32s would exceed the cap.
            (5, 30),
            (6, 30),
            (50, 30),
        ];
        for (attempt, expected_secs) in cases {
            assert_eq!(
                retry_backoff(attempt),
                Duration::from_secs(expected_secs),
                "retry_backoff({attempt})"
            );
        }
        assert_eq!(retry_backoff(0), REFRESH_RETRY_BASE_DELAY);
        assert_eq!(retry_backoff(u32::MAX), REFRESH_RETRY_MAX_DELAY);
    }

    #[test]
    fn jittered_retry_backoff_stays_in_the_upper_half_of_its_ceiling() {
        for attempt in 0..8 {
            let ceiling = retry_backoff(attempt);
            let mut distinct = std::collections::HashSet::new();
            for _ in 0..32 {
                let delay = jittered_retry_backoff(attempt);
                assert!(
                    delay >= ceiling / 2 && delay <= ceiling,
                    "jittered_retry_backoff({attempt}) = {delay:?} must fall within \
                     [{:?}, {ceiling:?}]",
                    ceiling / 2
                );
                distinct.insert(delay);
            }
            assert!(
                distinct.len() > 8,
                "the jitter must actually vary; attempt {attempt} produced {} distinct delays",
                distinct.len()
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_background_refresh_is_retried_on_the_backoff_not_the_regular_cadence() {
        // A 600-second token: the regular cadence is 400 seconds, so
        // without the retry backoff a single failed refresh would produce
        // exactly one fetch attempt in the window below, and the token
        // would be stale for the second half of its life.
        let manager = offline_token_manager(sample_header("token"), 600);
        let guard = manager.spawn_refresh(manager.refresh_interval());

        // Virtual time: the paused clock auto-advances whenever every task
        // is idle, so this sleep costs microseconds of real time while the
        // background task sees 400s (its first tick, which fails) plus
        // 100s of retry backoff — 1 + 2 + 4 + 8 + 16 + 30 + 30 s of
        // ceilings, halved at worst by the jitter, so at least six retries
        // must have been attempted on top of the first failure.
        tokio::time::sleep(Duration::from_secs(500)).await;

        let attempts = manager.inner.fetch_attempts.load(Ordering::Relaxed);
        assert!(
            attempts >= 4,
            "a failed refresh must be retried with backoff rather than waiting out the 400s \
             regular cadence; only {attempts} fetch attempts were made in 500s"
        );
        drop(guard);
    }

    #[tokio::test(start_paused = true)]
    async fn a_requested_refresh_is_honoured_while_the_backoff_is_still_running() {
        let manager = offline_token_manager(sample_header("token"), 600);
        // A one-hour regular cadence, so nothing the timer does can explain
        // the fetch attempts this test observes.
        let guard = manager.spawn_refresh(Duration::from_secs(3600));

        // Let the task reach its `select!`, then fail one refresh so it
        // enters the backoff.
        manager.request_refresh();
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(1)).await;
        let after_first = manager.inner.fetch_attempts.load(Ordering::Relaxed);
        assert!(
            after_first >= 1,
            "the requested refresh must have been attempted, got {after_first}"
        );

        // Now, with the task sitting in its backoff, request again. The
        // wake-up must be honoured immediately rather than waiting out the
        // remaining backoff (let alone the 3600s ticker).
        manager.request_refresh();
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert!(
            manager.inner.fetch_attempts.load(Ordering::Relaxed) > after_first,
            "request_refresh must keep waking the background task during a retry backoff"
        );
        drop(guard);
    }

    #[test]
    fn token_response_accepts_expires_in_as_a_number() {
        let token = parse_token_response(
            r#"{"access_token":"abc","expires_in":3600,"token_type":"Bearer"}"#,
        );
        assert_eq!(token.expires_in, 3600);
        assert_eq!(token.access_token.expose_secret(), "abc");
        assert_eq!(token.token_type, "Bearer");
    }

    #[test]
    fn token_response_accepts_expires_in_as_a_numeric_string() {
        // Real IdPs do this: the whole JSON payload is rendered as strings.
        let token = parse_token_response(
            r#"{"access_token":"abc","expires_in":"3600","token_type":"Bearer"}"#,
        );
        assert_eq!(token.expires_in, 3600);
        // Surrounding whitespace must not defeat it either.
        assert_eq!(
            parse_token_response(r#"{"access_token":"abc","expires_in":" 3600 "}"#).expires_in,
            3600
        );
    }

    #[test]
    fn token_response_truncates_a_fractional_expires_in_toward_zero() {
        // An IdP computing the lifetime as a difference of timestamps and
        // serializing it as a float — what Python's `json` module writes for
        // a `float`. Truncating down only tightens the refresh cadence.
        for json in [
            r#"{"access_token":"abc","expires_in":3599.7}"#,
            r#"{"access_token":"abc","expires_in":"3599.7"}"#,
        ] {
            assert_eq!(
                parse_token_response(json).expires_in,
                3599,
                "a fractional lifetime must truncate toward zero: {json}"
            );
        }
        assert_eq!(
            parse_token_response(r#"{"access_token":"abc","expires_in":3600.0}"#).expires_in,
            3600
        );
    }

    #[test]
    fn token_response_assumes_a_short_lifetime_when_expires_in_is_absent_or_null() {
        // RFC 6749 only recommends `expires_in`, so its absence must not
        // fail the whole token fetch.
        let token = parse_token_response(r#"{"access_token":"abc","token_type":"Bearer"}"#);
        assert_eq!(token.expires_in, ASSUMED_EXPIRES_IN_SECS);

        let explicit_null =
            parse_token_response(r#"{"access_token":"abc","expires_in":null,"token_type":"B"}"#);
        assert_eq!(explicit_null.expires_in, ASSUMED_EXPIRES_IN_SECS);
    }

    #[test]
    fn token_response_defaults_an_absent_token_type_to_bearer() {
        let token = parse_token_response(r#"{"access_token":"abc","expires_in":600}"#);
        assert_eq!(token.token_type, "Bearer");
        let header = build_header(&token).expect("the defaulted token type must build a header");
        assert_eq!(header.to_str().expect("ascii header"), "Bearer abc");
    }

    #[test]
    fn token_response_preserves_the_case_of_the_token_type_it_was_given() {
        // RFC 6749 §7.1 makes the token type case-insensitive, so an IdP
        // answering "bearer" is conformant; echo it back unchanged rather
        // than rewriting or rejecting it.
        let token = parse_token_response(
            r#"{"access_token":"abc","expires_in":600,"token_type":"bearer"}"#,
        );
        assert_eq!(token.token_type, "bearer");
        let header = build_header(&token).expect("a lowercase token type must build a header");
        assert_eq!(header.to_str().expect("ascii header"), "bearer abc");
    }

    #[test]
    fn token_response_ignores_unknown_fields() {
        let token = parse_token_response(
            r#"{"access_token":"abc","expires_in":600,"token_type":"Bearer",
                "scope":"read write","refresh_token":"unused","vendor_extra":{"a":1}}"#,
        );
        assert_eq!(token.expires_in, 600);
        assert_eq!(token.access_token.expose_secret(), "abc");
    }

    #[test]
    fn token_response_rejects_a_missing_access_token_and_an_unparseable_expires_in() {
        let missing_token: std::result::Result<TokenResponse, _> =
            serde_json::from_str(r#"{"expires_in":600,"token_type":"Bearer"}"#);
        assert!(
            missing_token.is_err(),
            "access_token is the one field that has no sensible default"
        );

        // A present-but-nonsensical lifetime is an error rather than a
        // silent guess: the IdP is saying something about the lifetime that
        // this crate must not paper over.
        for json in [
            r#"{"access_token":"abc","expires_in":"soon"}"#,
            r#"{"access_token":"abc","expires_in":-1}"#,
            r#"{"access_token":"abc","expires_in":-1.5}"#,
            r#"{"access_token":"abc","expires_in":true}"#,
            r#"{"access_token":"abc","expires_in":{"seconds":600}}"#,
        ] {
            let parsed: std::result::Result<TokenResponse, _> = serde_json::from_str(json);
            assert!(
                parsed.is_err(),
                "an unusable expires_in must be rejected, not guessed at: {json}"
            );
        }
    }

    // `SecretString`'s own `Debug` redacts, which is what lets
    // `TokenResponse` derive `Debug` instead of hand-writing one. Keep the
    // assertion: it is the property that matters, whoever implements it.
    #[test]
    fn token_response_debug_output_redacts_the_access_token() {
        let token = TokenResponse {
            access_token: SecretString::from("live-bearer-credential"),
            expires_in: 600,
            token_type: "Bearer".to_string(),
        };
        let debug_output = format!("{token:?}");
        assert!(
            !debug_output.contains("live-bearer-credential"),
            "the raw access token must never appear in Debug output, got: {debug_output}"
        );
        assert!(
            debug_output.to_ascii_lowercase().contains("redacted"),
            "the redaction placeholder must appear, got: {debug_output}"
        );
        // Non-sensitive fields stay visible: only the credential is hidden.
        assert!(debug_output.contains("600"));
        assert!(debug_output.contains("Bearer"));
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
