//! Client-side retry for the gRPC codes the server expects callers to
//! replay.
//!
//! [`RetryPolicy`] describes *how long* to wait between attempts;
//! [`RociaDbClient::retry`] runs a closure under that policy. Only two
//! status codes are ever retried — `ABORTED` always, and `UNAVAILABLE` when
//! [`RetryPolicy::with_retry_unavailable`] turns it on — because every other
//! failure this SDK can return is either final
//! (`INVALID_ARGUMENT`, `NOT_FOUND`, `ALREADY_EXISTS`, `PERMISSION_DENIED`)
//! or already handled elsewhere (`UNAUTHENTICATED` is refreshed and retried
//! inside the client itself, see [`RociaDbClient::refresh_auth_token`]).

use crate::{Result, RociaDbClient, RociaDbError};
use std::future::Future;
use std::time::Duration;
use tracing::debug;

/// Attempts [`RetryPolicy::default`] makes in total, the first one included:
/// three attempts means at most two retries.
const DEFAULT_MAX_ATTEMPTS: u32 = 3;
/// Delay ceiling [`RetryPolicy::default`] uses before the first retry. The
/// conflicts this retries are resolved by the storage layer in milliseconds,
/// not seconds, so the first retry is deliberately quick.
const DEFAULT_BASE_DELAY: Duration = Duration::from_millis(100);
/// Ceiling [`RetryPolicy::default`]'s doubling never exceeds.
const DEFAULT_MAX_DELAY: Duration = Duration::from_secs(2);

/// How many random bits [`jitter_numerator`] draws from a v4 UUID, and hence
/// the denominator (`2^56`) the jitter fraction is expressed over.
const JITTER_BITS: u32 = 56;

/// How [`RociaDbClient::retry`] spaces its attempts.
///
/// `#[non_exhaustive]`: read the fields freely, but build a policy from
/// [`RetryPolicy::new`] (or [`RetryPolicy::default`]) plus the `with_*`
/// setters, so a field added in a later release does not break the build.
///
/// # Schedule
///
/// Attempt *n* (zero-based, so retry *n* is attempt *n + 1*) waits a random
/// duration drawn uniformly from `[0, min(base_delay * 2^n, max_delay)]` —
/// exponential backoff with *full jitter*. Full jitter rather than a fixed
/// delay because the callers that see `ABORTED` are usually contending with
/// each other: if every one of them backed off by exactly the same amount,
/// they would collide again on the next attempt. Drawing from the whole
/// interval spreads them out instead.
///
/// With the defaults (`base_delay` 100 ms, `max_delay` 2 s) the ceilings are
/// 100 ms, 200 ms, 400 ms, …, 2 s, 2 s, and the actual waits are uniform
/// below each.
///
/// # What is retried
///
/// `ABORTED` always: it is the one code the server requires every caller to
/// replay (see [`RociaDbError::is_aborted`], which also explains why it can
/// arrive on a read). `UNAVAILABLE` only when
/// [`with_retry_unavailable(true)`](RetryPolicy::with_retry_unavailable) is
/// set. Nothing else, ever — retrying an `INVALID_ARGUMENT` or a
/// `NOT_FOUND` would only turn one clear failure into several slow ones.
///
/// # Example
///
/// ```rust,no_run
/// use rociadb_sdk::{RetryPolicy, RociaDbBuilder, WriteOptions};
/// use serde_json::json;
/// use std::time::Duration;
///
/// # #[tokio::main]
/// # async fn main() -> rociadb_sdk::Result<()> {
/// let client = RociaDbBuilder::new().disable_auth().build().await?;
/// let policy = RetryPolicy::new()
///     .with_max_attempts(5)
///     .with_base_delay(Duration::from_millis(50));
///
/// // One idempotency key, minted once, reused by every attempt.
/// let options = WriteOptions::new().with_request_id("import-batch-7:node-1");
/// client
///     .retry(&policy, || {
///         let options = options.clone();
///         async { client.put_node("tenant-1", "catalog", "node-1", &json!({}), options).await }
///     })
///     .await?;
/// # Ok(())
/// # }
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts, the first one included. `1` disables retrying; `0` is
    /// treated as `1` rather than as "never run the closure at all".
    /// Defaults to 3.
    pub max_attempts: u32,
    /// Delay ceiling before the first retry, doubled before each subsequent
    /// one. Defaults to 100 ms. `Duration::ZERO` retries immediately, with
    /// no sleep at all.
    pub base_delay: Duration,
    /// Ceiling the doubling never exceeds. Defaults to 2 s. A value below
    /// `base_delay` wins: the ceiling is applied last, so every delay is
    /// drawn from `[0, max_delay]`.
    pub max_delay: Duration,
    /// Whether `UNAVAILABLE` is retried in addition to `ABORTED`. Defaults
    /// to `false`; see
    /// [`with_retry_unavailable`](Self::with_retry_unavailable) for when to
    /// turn it on.
    pub retry_unavailable: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            base_delay: DEFAULT_BASE_DELAY,
            max_delay: DEFAULT_MAX_DELAY,
            retry_unavailable: false,
        }
    }
}

impl RetryPolicy {
    /// A policy with every field at its default: three attempts, a 100 ms
    /// base delay, a 2 s ceiling, and `UNAVAILABLE` not retried. Same as
    /// [`RetryPolicy::default`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the total number of attempts, the first one included. `1`
    /// disables retrying; `0` is clamped to `1`.
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Set the delay ceiling before the first retry (doubled before each
    /// subsequent one).
    pub fn with_base_delay(mut self, base_delay: Duration) -> Self {
        self.base_delay = base_delay;
        self
    }

    /// Set the ceiling the doubling never exceeds.
    pub fn with_max_delay(mut self, max_delay: Duration) -> Self {
        self.max_delay = max_delay;
        self
    }

    /// Also retry `UNAVAILABLE`, not just `ABORTED`.
    ///
    /// `UNAVAILABLE` means the SDK could not reach a working server at all —
    /// tonic reports a failed dial this way, and so does a server that is
    /// restarting or draining. Retrying it is safe for a read, and safe for
    /// a write **that carries a caller-supplied `request_id`**, since the
    /// server's deduplication then recognizes a replay. Left at its `false`
    /// default, because for a write with an SDK-generated key a retry is a
    /// second, distinct write as far as the server is concerned, and the
    /// original may well have been applied before the connection broke.
    pub fn with_retry_unavailable(mut self, retry_unavailable: bool) -> Self {
        self.retry_unavailable = retry_unavailable;
        self
    }

    /// `max_attempts`, with `0` read as `1`: a policy always runs the
    /// closure at least once.
    fn attempts(&self) -> u32 {
        self.max_attempts.max(1)
    }

    /// The un-jittered ceiling for the delay before retry `retry`
    /// (zero-based): `min(base_delay * 2^retry, max_delay)`, saturating
    /// instead of overflowing for a large `retry` or `base_delay`.
    fn delay_ceiling(&self, retry: u32) -> Duration {
        let factor = 1u32.checked_shl(retry).unwrap_or(u32::MAX);
        let scaled = self.base_delay.checked_mul(factor).unwrap_or(Duration::MAX);
        scaled.min(self.max_delay)
    }

    /// The delay actually slept before retry `retry`: uniform over
    /// `[0, delay_ceiling(retry)]`.
    fn delay(&self, retry: u32) -> Duration {
        full_jitter(self.delay_ceiling(retry))
    }

    /// Whether this policy replays a call that failed with `error`.
    fn should_retry(&self, error: &RociaDbError) -> bool {
        error.is_aborted()
            || (self.retry_unavailable && error.code() == Some(tonic::Code::Unavailable))
    }
}

/// 56 random bits, as a numerator over `2^56`.
///
/// Taken from a v4 UUID rather than from a `rand` dependency this crate
/// would otherwise not need: `uuid` is already here (every generated
/// `request_id` is a v4 UUID) and `Uuid::new_v4` fills all 128 bits from the
/// operating system's CSPRNG, forcing only the version nibble (byte 6) and
/// the two variant bits (byte 8). Bytes 9 to 15 are therefore untouched
/// random bytes, which is exactly the seven this reads — no version or
/// variant bit is included, so the value is uniform over `[0, 2^56)`.
fn jitter_numerator() -> u64 {
    let uuid = uuid::Uuid::new_v4();
    let bytes = uuid.as_bytes();
    let mut numerator = [0u8; 8];
    numerator[1..8].copy_from_slice(&bytes[9..16]);
    u64::from_be_bytes(numerator)
}

/// Draw a duration uniformly from `[0, cap]` ("full jitter").
///
/// The scaling is a 128-bit multiply-and-shift rather than a modulo, so
/// every nanosecond in the interval is equally likely instead of the lowest
/// few being slightly favoured. Saturating throughout: a `cap` beyond
/// `u64::MAX` nanoseconds (about 584 years) is treated as that maximum
/// rather than overflowing.
pub(crate) fn full_jitter(cap: Duration) -> Duration {
    if cap.is_zero() {
        return Duration::ZERO;
    }
    let nanos = u64::try_from(cap.as_nanos()).unwrap_or(u64::MAX);
    // `(nanos + 1) * numerator >> 56` with `numerator < 2^56` maps the
    // fraction onto the inclusive range `[0, nanos]`. The product is at most
    // `2^64 * 2^56 = 2^120`, so it cannot overflow `u128`.
    let scaled = ((u128::from(nanos) + 1) * u128::from(jitter_numerator())) >> JITTER_BITS;
    Duration::from_nanos(u64::try_from(scaled).unwrap_or(u64::MAX).min(nanos))
}

impl RociaDbClient {
    /// Run `op` under `policy`, replaying it while it fails with a status
    /// the policy retries.
    ///
    /// The closure is called once per attempt and must build the whole call
    /// each time (that is why it is `FnMut() -> Fut` rather than a single
    /// future): a future that has already failed cannot be polled again.
    /// The first error the policy does *not* retry is returned immediately,
    /// and so is the last error once `max_attempts` is exhausted — this
    /// never returns a synthetic "retries exhausted" error of its own, so
    /// [`code`](RociaDbError::code) and [`reason`](RociaDbError::reason)
    /// still describe what the server actually said.
    ///
    /// # Reuse one `request_id` across every attempt
    ///
    /// **`ABORTED` never proves nothing was written** (see
    /// [`RociaDbError::is_aborted`]). A retried write must therefore carry
    /// the *same* idempotency key as the attempt that failed, so the server
    /// recognizes the replay instead of applying the write twice: build the
    /// key once, outside the closure, and pass it in with
    /// [`WriteOptions::with_request_id`](crate::WriteOptions::with_request_id)
    /// (or the equivalent setter on
    /// [`DocumentWriteOptions`](crate::DocumentWriteOptions),
    /// [`FileUploadOptions`](crate::FileUploadOptions),
    /// [`NodeInput`](crate::NodeInput), [`EdgeInput`](crate::EdgeInput)).
    /// A closure that leaves `request_id` unset gets a *fresh*
    /// SDK-generated key on every attempt, which is exactly the case the
    /// server cannot deduplicate.
    ///
    /// Reads need no such care, and neither does the client: `&self` is only
    /// taken so this reads as a method on the client the closure uses.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use rociadb_sdk::{RetryPolicy, RociaDbBuilder};
    /// use serde_json::Value;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> rociadb_sdk::Result<()> {
    /// let client = RociaDbBuilder::new().disable_auth().build().await?;
    /// let document: Value = client
    ///     .retry(&RetryPolicy::new(), || {
    ///         client.get_document("tenant-1", "products", "sku-123")
    ///     })
    ///     .await?;
    /// # let _ = document;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn retry<T, F, Fut>(&self, policy: &RetryPolicy, mut op: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let attempts = policy.attempts();
        let mut retry = 0u32;
        loop {
            let error = match op().await {
                Ok(value) => return Ok(value),
                Err(error) => error,
            };
            // `retry` is the number of retries already made, so the attempt
            // that just failed was number `retry + 1`.
            if retry + 1 >= attempts || !policy.should_retry(&error) {
                return Err(error);
            }
            let delay = policy.delay(retry);
            debug!(
                attempt = retry + 1,
                max_attempts = attempts,
                delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                code = ?error.code(),
                "retrying a retryable upstream status"
            );
            tokio::time::sleep(delay).await;
            retry += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_BASE_DELAY, DEFAULT_MAX_ATTEMPTS, DEFAULT_MAX_DELAY, RetryPolicy, full_jitter,
    };
    use crate::test_support::lazy_test_client;
    use crate::{Result, RociaDbError};
    use std::cell::Cell;
    use std::time::Duration;
    use tonic::Status;

    fn aborted() -> RociaDbError {
        RociaDbError::Status {
            operation: "failed to put node",
            status: Status::aborted("write conflict, retry"),
        }
    }

    fn unavailable() -> RociaDbError {
        RociaDbError::Status {
            operation: "failed to put node",
            status: Status::unavailable("server draining"),
        }
    }

    fn not_found() -> RociaDbError {
        RociaDbError::Status {
            operation: "failed to get node",
            status: Status::not_found("node absent"),
        }
    }

    #[test]
    fn new_matches_default_and_documents_its_values() {
        assert_eq!(RetryPolicy::new(), RetryPolicy::default());
        let policy = RetryPolicy::new();
        assert_eq!(policy.max_attempts, DEFAULT_MAX_ATTEMPTS);
        assert_eq!(policy.base_delay, DEFAULT_BASE_DELAY);
        assert_eq!(policy.max_delay, DEFAULT_MAX_DELAY);
        assert!(
            !policy.retry_unavailable,
            "UNAVAILABLE must be opt-in, not retried by default"
        );
    }

    #[test]
    fn setters_are_chainable_and_readable() {
        let policy = RetryPolicy::new()
            .with_max_attempts(7)
            .with_base_delay(Duration::from_millis(25))
            .with_max_delay(Duration::from_secs(10))
            .with_retry_unavailable(true);
        assert_eq!(policy.max_attempts, 7);
        assert_eq!(policy.base_delay, Duration::from_millis(25));
        assert_eq!(policy.max_delay, Duration::from_secs(10));
        assert!(policy.retry_unavailable);
    }

    #[test]
    fn delay_ceiling_doubles_then_saturates_at_max_delay() {
        let policy = RetryPolicy::new()
            .with_base_delay(Duration::from_millis(100))
            .with_max_delay(Duration::from_secs(2));
        // (zero-based retry index, expected ceiling in milliseconds)
        let cases = [
            (0u32, 100u64),
            (1, 200),
            (2, 400),
            (3, 800),
            (4, 1600),
            (5, 2000),
            (6, 2000),
        ];
        for (retry, expected) in cases {
            assert_eq!(
                policy.delay_ceiling(retry),
                Duration::from_millis(expected),
                "ceiling for retry {retry}"
            );
        }
    }

    #[test]
    fn delay_ceiling_never_overflows_for_an_absurd_retry_count_or_base_delay() {
        // `base_delay * 2^retry` overflows a `Duration` long before `retry`
        // reaches these values; the schedule must saturate at `max_delay`
        // rather than panic in a debug build.
        let policy = RetryPolicy::new().with_max_delay(Duration::from_secs(5));
        for retry in [30, 31, 32, 64, u32::MAX] {
            assert_eq!(policy.delay_ceiling(retry), Duration::from_secs(5));
        }

        let huge = RetryPolicy::new()
            .with_base_delay(Duration::MAX)
            .with_max_delay(Duration::MAX);
        assert_eq!(huge.delay_ceiling(3), Duration::MAX);
    }

    #[test]
    fn delay_ceiling_applies_max_delay_even_below_the_base_delay() {
        let policy = RetryPolicy::new()
            .with_base_delay(Duration::from_secs(30))
            .with_max_delay(Duration::from_secs(1));
        assert_eq!(policy.delay_ceiling(0), Duration::from_secs(1));
    }

    #[test]
    fn a_zero_base_delay_schedules_no_wait_at_all() {
        let policy = RetryPolicy::new().with_base_delay(Duration::ZERO);
        for retry in 0..5 {
            assert_eq!(policy.delay_ceiling(retry), Duration::ZERO);
        }
        assert_eq!(full_jitter(Duration::ZERO), Duration::ZERO);
    }

    #[test]
    fn full_jitter_stays_within_its_cap_and_actually_varies() {
        let cap = Duration::from_millis(200);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..64 {
            let drawn = full_jitter(cap);
            assert!(
                drawn <= cap,
                "full jitter must never exceed its cap: {drawn:?} > {cap:?}"
            );
            seen.insert(drawn);
        }
        // 64 draws from a 200 ms interval at nanosecond resolution: a jitter
        // source that collapsed to a constant (a mis-sliced UUID, say) would
        // be caught here, while a real one practically never repeats.
        assert!(
            seen.len() > 32,
            "full jitter must spread the delay out, got only {} distinct values",
            seen.len()
        );
        // `Duration::MAX` must saturate rather than overflow the scaling.
        assert!(full_jitter(Duration::MAX) <= Duration::MAX);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_returns_the_first_success_without_sleeping_again() {
        let client = lazy_test_client();
        let calls = Cell::new(0u32);
        let value = client
            .retry(&RetryPolicy::new(), || {
                calls.set(calls.get() + 1);
                async { Ok::<u32, RociaDbError>(7) }
            })
            .await
            .expect("a closure that succeeds first time must return Ok");
        assert_eq!(value, 7);
        assert_eq!(calls.get(), 1, "success must not be retried");
    }

    #[tokio::test(start_paused = true)]
    async fn retry_replays_aborted_until_the_closure_succeeds() {
        let client = lazy_test_client();
        let calls = Cell::new(0u32);
        let value = client
            .retry(&RetryPolicy::new().with_max_attempts(5), || {
                calls.set(calls.get() + 1);
                let attempt = calls.get();
                async move {
                    if attempt < 3 {
                        Err(aborted())
                    } else {
                        Ok(attempt)
                    }
                }
            })
            .await
            .expect("two ABORTED failures followed by a success must return Ok");
        assert_eq!(value, 3);
        assert_eq!(calls.get(), 3, "exactly three attempts were needed");
    }

    #[tokio::test(start_paused = true)]
    async fn retry_gives_up_after_max_attempts_and_returns_the_last_error() {
        let client = lazy_test_client();
        let calls = Cell::new(0u32);
        let error = client
            .retry(&RetryPolicy::new().with_max_attempts(4), || {
                calls.set(calls.get() + 1);
                async { Err::<(), _>(aborted()) }
            })
            .await
            .expect_err("a closure that always fails must surface its error");
        assert_eq!(calls.get(), 4, "max_attempts counts the first attempt");
        assert!(
            error.is_aborted(),
            "the server's own status must be returned, not a synthetic one"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retry_never_replays_a_code_outside_the_policy() {
        let client = lazy_test_client();
        let calls = Cell::new(0u32);
        let error = client
            .retry(&RetryPolicy::new(), || {
                calls.set(calls.get() + 1);
                async { Err::<(), _>(not_found()) }
            })
            .await
            .expect_err("NOT_FOUND must be returned as-is");
        assert_eq!(calls.get(), 1, "NOT_FOUND must never be retried");
        assert!(error.is_not_found());

        // A non-status error (no gRPC code at all) is just as final.
        let calls = Cell::new(0u32);
        let error = client
            .retry(&RetryPolicy::new(), || {
                calls.set(calls.get() + 1);
                async { Err::<(), _>(RociaDbError::validation("page limit must be positive")) }
            })
            .await
            .expect_err("a Validation error must be returned as-is");
        assert_eq!(calls.get(), 1);
        assert!(matches!(error, RociaDbError::Validation(_)));
    }

    #[tokio::test(start_paused = true)]
    async fn unavailable_is_retried_only_when_the_policy_opts_in() {
        let client = lazy_test_client();

        let calls = Cell::new(0u32);
        client
            .retry(&RetryPolicy::new(), || {
                calls.set(calls.get() + 1);
                async { Err::<(), _>(unavailable()) }
            })
            .await
            .expect_err("UNAVAILABLE must fail immediately by default");
        assert_eq!(calls.get(), 1, "UNAVAILABLE is not retried by default");

        let calls = Cell::new(0u32);
        client
            .retry(
                &RetryPolicy::new()
                    .with_max_attempts(3)
                    .with_retry_unavailable(true),
                || {
                    calls.set(calls.get() + 1);
                    async { Err::<(), _>(unavailable()) }
                },
            )
            .await
            .expect_err("every attempt fails here");
        assert_eq!(
            calls.get(),
            3,
            "with_retry_unavailable(true) must replay UNAVAILABLE"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn max_attempts_of_zero_or_one_runs_the_closure_exactly_once() {
        let client = lazy_test_client();
        for max_attempts in [0, 1] {
            let calls = Cell::new(0u32);
            client
                .retry(&RetryPolicy::new().with_max_attempts(max_attempts), || {
                    calls.set(calls.get() + 1);
                    async { Err::<(), _>(aborted()) }
                })
                .await
                .expect_err("the single attempt fails");
            assert_eq!(
                calls.get(),
                1,
                "max_attempts = {max_attempts} must still run the closure once"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retry_actually_sleeps_between_attempts() {
        // Under a paused clock, `tokio::time::sleep` only completes because
        // the runtime auto-advances time when every task is idle — so the
        // elapsed virtual time is a direct measure of the backoff the retry
        // loop slept. Four attempts sleep three times, each drawn from
        // [0, 1s], so the total is in (0, 3s]; the lower bound is what would
        // catch a loop that dropped the sleep entirely.
        let client = lazy_test_client();
        let started = tokio::time::Instant::now();
        let policy = RetryPolicy::new()
            .with_max_attempts(4)
            .with_base_delay(Duration::from_secs(1))
            .with_max_delay(Duration::from_secs(1));
        let result: Result<()> = client
            .retry(&policy, || async { Err::<(), _>(aborted()) })
            .await;
        assert!(result.is_err());
        let elapsed = started.elapsed();
        assert!(
            elapsed > Duration::ZERO && elapsed <= Duration::from_secs(3),
            "three jittered sleeps under a 1s ceiling must total (0s, 3s], got {elapsed:?}"
        );
    }
}
