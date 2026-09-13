//! Public error type returned by every fallible `RociaDbClient` method.
//!
//! Every public method returns [`Result<T>`], an alias for
//! `std::result::Result<T, RociaDbError>`. Callers that need to branch on
//! the failure kind can `match` on [`RociaDbError`] directly instead of
//! reaching for `downcast_ref` on a boxed `dyn Error`.

/// Result alias used throughout the public API.
pub type Result<T> = std::result::Result<T, RociaDbError>;

/// Error returned by the SDK.
///
/// The [`RociaDbError::Status`] variant is the one produced by every failed
/// gRPC call: it carries the raw [`tonic::Status`], so nothing is lost
/// compared to calling the generated client directly. Beyond the standard
/// gRPC `code`, the upstream server always attaches a `reason` trailing
/// metadata value — see [`RociaDbError::reason`], which also documents the
/// one code whose `reason` does not simply name it. In particular the
/// server treats `UNAUTHENTICATED` as a signal to refresh the auth token
/// and retry (see [`RociaDbError::is_unauthenticated`] and
/// [`crate::RociaDbClient::refresh_auth_token`]), whereas
/// `PERMISSION_DENIED` is final — the token is valid but lacks the required
/// scope, and retrying after a refresh will not help (see
/// [`RociaDbError::is_permission_denied`]). Two more codes carry meaning
/// beyond `code()`/`reason()` alone: `ALREADY_EXISTS`, the one non-auth
/// code this SDK's own docs treat as an expected outcome to branch on (see
/// [`RociaDbError::is_already_exists`]), and `ABORTED`, the one code every
/// caller is expected to retry automatically — on any call, reads included
/// (see [`RociaDbError::is_aborted`]).
/// New variants may be added in a minor release: match on this enum with a
/// wildcard arm to stay forward-compatible.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum RociaDbError {
    /// A gRPC call to the upstream server returned a non-OK status.
    #[error("{operation}: {status}")]
    Status {
        /// Short description of the failed operation (for example
        /// `"failed to upsert document"`).
        operation: &'static str,
        /// The status the server returned, complete with its code, message
        /// and trailing metadata.
        #[source]
        status: tonic::Status,
    },

    /// Failed to connect to, or configure, the upstream endpoint: invalid
    /// host, TLS setup, connection refused, or missing builder
    /// configuration (host, token URL, client id/secret).
    ///
    /// [`Display`](std::fmt::Display) folds in the underlying cause
    /// whenever one is present, so a bare `.to_string()` (or a `%err`
    /// tracing field built from it, as the background token-refresh task
    /// does) already distinguishes a DNS failure from a TLS mismatch from a
    /// refused connection, instead of rendering the same message for all
    /// three. The cause is absent only for the handful of internal call
    /// sites that build this variant from a validation failure with no I/O
    /// involved; call [`std::error::Error::source`] directly when you need
    /// to match on the cause's concrete type rather than read it.
    #[error("{message}{}", source_suffix(.source))]
    Connection {
        /// Description of what could not be connected to or configured.
        message: String,
        /// The underlying cause, when the failure came from I/O or TLS
        /// rather than from a pure configuration check.
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Failed to obtain or refresh the upstream auth token.
    ///
    /// As with [`RociaDbError::Connection`], [`Display`](std::fmt::Display)
    /// folds in the underlying cause when one is present, rather than
    /// collapsing every I/O error, poisoned-lock failure, or nested
    /// [`RociaDbError`] into the same fixed string.
    #[error("{message}{}", source_suffix(.source))]
    Auth {
        /// Description of which step of token acquisition or refresh failed.
        message: String,
        /// The underlying cause, when there is one.
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Failed to encode a value as JSON before sending it upstream.
    #[error("failed to encode {context}: {source}")]
    Encode {
        /// Name of the value that could not be encoded (for example
        /// `"node json"`).
        context: &'static str,
        /// The serialization error `serde_json` reported.
        #[source]
        source: serde_json::Error,
    },

    /// Failed to decode a JSON payload received from upstream.
    #[error("failed to decode {context}: {source}")]
    Decode {
        /// Name of the payload that could not be decoded (for example
        /// `"document json"`).
        context: &'static str,
        /// The deserialization error `serde_json` reported. For a page of
        /// documents its message leads with `"item <index>: "`, naming the
        /// zero-based position of the offending item within the page.
        #[source]
        source: serde_json::Error,
    },

    /// A client-side validation rule was violated before any network call
    /// was made (a zero page limit, a file size out of bounds, a chunk
    /// stream whose total byte count does not match the declared
    /// `size_bytes`, etc).
    #[error("{0}")]
    Validation(String),
}

/// Renders `": {source}"` when a cause is present, or an empty string when
/// it is not, so [`RociaDbError::Connection`] and [`RociaDbError::Auth`]
/// can interpolate an optional `#[source]` into their `Display` output
/// without printing a stray `: ` for the internal call sites that build
/// either variant with no cause attached.
fn source_suffix(source: &Option<Box<dyn std::error::Error + Send + Sync>>) -> String {
    match source {
        Some(source) => format!(": {source}"),
        None => String::new(),
    }
}

impl RociaDbError {
    /// The gRPC status code, present only for [`RociaDbError::Status`].
    pub fn code(&self) -> Option<tonic::Code> {
        match self {
            Self::Status { status, .. } => Some(status.code()),
            _ => None,
        }
    }

    /// The server's `reason` trailing metadata, present only for
    /// [`RociaDbError::Status`]. Six of its seven values are exactly the
    /// snake_case name of the gRPC code returned alongside them —
    /// `invalid_argument`, `not_found`, `already_exists`,
    /// `permission_denied`, `unauthenticated`, `internal` — so branching on
    /// `reason()` partitions errors no differently than branching on
    /// [`Self::code`] (or the `is_*` predicates) already does; prefer those
    /// for control flow and reach for `reason()` only when you need a
    /// stable string to log or forward rather than to match on.
    ///
    /// The exception is [`tonic::Code::Aborted`]: its `reason` is
    /// `"conflict"`, not the `"aborted"` the pattern above would predict.
    /// The naming does not carry a different meaning — see
    /// [`Self::is_aborted`] for what the code itself signals — but it is
    /// the one place `reason()` and `code()` genuinely diverge rather than
    /// mirroring each other under two names.
    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Status { status, .. } => status
                .metadata()
                .get("reason")
                .and_then(|v| v.to_str().ok()),
            _ => None,
        }
    }

    /// The raw gRPC status, present only for [`RociaDbError::Status`].
    pub fn status(&self) -> Option<&tonic::Status> {
        match self {
            Self::Status { status, .. } => Some(status),
            _ => None,
        }
    }

    /// True when the server rejected the call as unauthenticated. The
    /// server treats this as a renewal signal: call
    /// [`crate::RociaDbClient::refresh_auth_token`] and retry.
    pub fn is_unauthenticated(&self) -> bool {
        self.code() == Some(tonic::Code::Unauthenticated)
    }

    /// True when the server rejected the call for lacking permission.
    /// Unlike [`Self::is_unauthenticated`], this is final: the token is
    /// valid but lacks the required scope, and refreshing it will not help.
    pub fn is_permission_denied(&self) -> bool {
        self.code() == Some(tonic::Code::PermissionDenied)
    }

    /// True when the server rejected the call because it would violate a
    /// uniqueness constraint. The one call in this SDK that can produce
    /// `ALREADY_EXISTS` is [`crate::RociaDbClient::add_edge`] (and
    /// [`crate::RociaDbClient::add_edges`]): a `(from, label, to)` triplet
    /// names at most one edge, since the adjacency index is keyed by the
    /// triplet rather than by `edge_id`, so adding a second edge over a
    /// triplet another edge already holds fails this way. This is expected
    /// to happen in normal use, not only under misuse: retry with a fresh
    /// `edge_id`, or a different `label`/`to`, rather than the same
    /// arguments, which will fail identically every time. Reusing the
    /// *same* `edge_id` on its own triplet is not a conflict — it replaces
    /// that edge's properties instead, which is what makes replaying an
    /// already-successful `add_edge` harmless.
    pub fn is_already_exists(&self) -> bool {
        self.code() == Some(tonic::Code::AlreadyExists)
    }

    /// True when the server reports a transient storage conflict that the
    /// caller is expected to retry. Unlike [`Self::is_unauthenticated`] and
    /// [`Self::is_permission_denied`], which callers may reasonably leave
    /// unhandled, `ABORTED` is the one code every caller of this API must
    /// handle: it is safe, and expected, to resend the exact same call
    /// (with the same `request_id`, if one was supplied) rather than
    /// surface the error.
    ///
    /// It can arrive on any call, reads included, not only on writes: a
    /// `tikv` write conflict, a lock held by a concurrent transaction, or a
    /// region error can surface it on [`crate::RociaDbClient::get_document`],
    /// [`crate::RociaDbClient::list_documents`],
    /// [`crate::RociaDbClient::query_documents`], or
    /// [`crate::RociaDbClient::search_documents`] just as it can on a
    /// write. On [`crate::RociaDbClient::put_document`] and
    /// [`crate::RociaDbClient::delete_document`] it surfaces only after the
    /// server's own five internal retries are exhausted; every other write
    /// — [`crate::RociaDbClient::put_node`],
    /// [`crate::RociaDbClient::add_edge`],
    /// [`crate::RociaDbClient::delete_edge`],
    /// [`crate::RociaDbClient::upload_file`],
    /// [`crate::RociaDbClient::delete_file`] — has no such cushion and can
    /// return it on the very first conflict.
    ///
    /// The same code also covers a concurrent duplicate of a call carrying
    /// a `request_id` that is still in flight: the server cannot yet say
    /// whether the original succeeded and refuses to guess. Retry, and
    /// that retry finds the original either finished — returning `Ok`
    /// without re-executing — or, if the caller's connection dropped or
    /// the server died mid-call, still reserved for up to the server's
    /// request lease (300 seconds by default); retries during that window
    /// keep returning `ABORTED`, and the first one after the lease expires
    /// replays the call and succeeds.
    ///
    /// Either way, **`ABORTED` never proves nothing was written.** On
    /// `put_document`/`delete_document`, the document, its index entries,
    /// and the idempotency marker share one transaction, but the marker's
    /// own commit — which can itself collide — happens after that
    /// transaction lands, so an `ABORTED` surfaced by that step arrives on
    /// a document already committed. None of this changes what to do:
    /// retry the same call, with backoff rather than a tight loop, since
    /// server-side contention absorbs conflicts in latency rather than
    /// making them disappear.
    pub fn is_aborted(&self) -> bool {
        self.code() == Some(tonic::Code::Aborted)
    }
}

impl RociaDbError {
    pub(crate) fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }

    pub(crate) fn connection(message: impl Into<String>) -> Self {
        Self::Connection {
            message: message.into(),
            source: None,
        }
    }
}

/// Extension trait mapping a failed gRPC call into [`RociaDbError::Status`],
/// mirroring anyhow's `.context(...)` ergonomics for the one error source
/// that must stay fully typed.
pub(crate) trait StatusResultExt<T> {
    fn status_context(self, operation: &'static str) -> Result<T>;
}

impl<T> StatusResultExt<T> for std::result::Result<T, tonic::Status> {
    fn status_context(self, operation: &'static str) -> Result<T> {
        self.map_err(|status| RociaDbError::Status { operation, status })
    }
}

/// Extension trait wrapping any connection/config failure into
/// [`RociaDbError::Connection`]. Also accepts another [`RociaDbError`] as
/// the source, so a higher-level step (for example "failed to initialize
/// token manager") can nest a lower-level one without losing it.
pub(crate) trait ConnectionResultExt<T> {
    fn connection_context(self, message: &str) -> Result<T>;
}

impl<T, E> ConnectionResultExt<T> for std::result::Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn connection_context(self, message: &str) -> Result<T> {
        self.map_err(|source| RociaDbError::Connection {
            message: message.to_string(),
            source: Some(Box::new(source)),
        })
    }
}

/// Extension trait wrapping any auth-token failure into
/// [`RociaDbError::Auth`]. Also accepts another [`RociaDbError`] as the
/// source (see [`ConnectionResultExt`] for why).
pub(crate) trait AuthResultExt<T> {
    fn auth_context(self, message: &str) -> Result<T>;
}

impl<T, E> AuthResultExt<T> for std::result::Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn auth_context(self, message: &str) -> Result<T> {
        self.map_err(|source| RociaDbError::Auth {
            message: message.to_string(),
            source: Some(Box::new(source)),
        })
    }
}

/// Extension trait mapping `serde_json` (de)serialization failures into
/// [`RociaDbError::Encode`] / [`RociaDbError::Decode`].
pub(crate) trait JsonResultExt<T> {
    fn encode_context(self, context: &'static str) -> Result<T>;
    fn decode_context(self, context: &'static str) -> Result<T>;
}

impl<T> JsonResultExt<T> for std::result::Result<T, serde_json::Error> {
    fn encode_context(self, context: &'static str) -> Result<T> {
        self.map_err(|source| RociaDbError::Encode { context, source })
    }

    fn decode_context(self, context: &'static str) -> Result<T> {
        self.map_err(|source| RociaDbError::Decode { context, source })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use tonic::metadata::MetadataValue;
    use tonic::{Code, Status};

    fn status_with_reason(code: Code, message: &str, reason: &str) -> Status {
        let mut status = Status::new(code, message);
        status.metadata_mut().insert(
            "reason",
            reason.parse::<MetadataValue<_>>().expect("ascii reason"),
        );
        status
    }

    #[test]
    fn status_error_exposes_code_and_server_reason() {
        // The server always attaches a `reason` trailing metadata value
        // alongside the gRPC code; both must survive the trip into
        // `RociaDbError::Status` unchanged.
        let status = status_with_reason(Code::NotFound, "document not found", "not_found");
        let error = RociaDbError::Status {
            operation: "failed to get document",
            status,
        };

        assert_eq!(error.code(), Some(Code::NotFound));
        assert_eq!(error.reason(), Some("not_found"));
        assert_eq!(
            error.status().map(tonic::Status::code),
            Some(Code::NotFound)
        );

        // The message must stay informative: both the high-level operation
        // and the server-provided detail should be readable in the Display
        // output, not just the bare variant name.
        let message = error.to_string();
        assert!(
            message.contains("failed to get document"),
            "message should name the failed operation, got: {message}"
        );
        assert!(
            message.contains("document not found"),
            "message should carry the server's detail, got: {message}"
        );
    }

    #[test]
    fn status_error_without_reason_metadata_reports_none() {
        let status = Status::new(Code::Internal, "boom");
        let error = RociaDbError::Status {
            operation: "failed to do something",
            status,
        };
        assert_eq!(error.code(), Some(Code::Internal));
        assert_eq!(
            error.reason(),
            None,
            "no reason metadata was attached, so reason() must not invent one"
        );
    }

    #[test]
    fn is_unauthenticated_true_only_for_unauthenticated_status() {
        let unauthenticated = RociaDbError::Status {
            operation: "failed to list documents",
            status: Status::unauthenticated("token expired"),
        };
        assert!(unauthenticated.is_unauthenticated());
        assert!(
            !unauthenticated.is_permission_denied(),
            "unauthenticated must not also read as permission_denied"
        );
    }

    #[test]
    fn is_permission_denied_true_only_for_permission_denied_status() {
        let forbidden = RociaDbError::Status {
            operation: "failed to delete document",
            status: Status::permission_denied("missing scope"),
        };
        assert!(forbidden.is_permission_denied());
        assert!(
            !forbidden.is_unauthenticated(),
            "permission_denied must not also read as unauthenticated"
        );
    }

    #[test]
    fn is_already_exists_true_only_for_already_exists_status() {
        let conflict = RociaDbError::Status {
            operation: "failed to add edge",
            status: Status::already_exists("triplet already taken by another edge"),
        };
        assert!(conflict.is_already_exists());
        assert!(
            !conflict.is_aborted(),
            "already_exists must not also read as aborted"
        );
    }

    #[test]
    fn is_aborted_true_only_for_aborted_status() {
        let aborted = RociaDbError::Status {
            operation: "failed to get document",
            status: Status::aborted("write conflict, retry"),
        };
        assert!(aborted.is_aborted());
        assert!(
            !aborted.is_already_exists(),
            "aborted must not also read as already_exists"
        );
    }

    #[test]
    fn is_unauthenticated_and_is_permission_denied_are_false_for_other_status_codes() {
        let not_found = RociaDbError::Status {
            operation: "failed to get node",
            status: Status::not_found("node absent"),
        };
        assert!(!not_found.is_unauthenticated());
        assert!(!not_found.is_permission_denied());
        assert!(!not_found.is_already_exists());
        assert!(!not_found.is_aborted());
    }

    #[test]
    fn non_status_variants_carry_no_grpc_code_reason_or_status() {
        // `code`/`reason`/`status`/the four `is_*` predicates only make
        // sense for a failed gRPC call; every other variant must report
        // "absent" rather than panicking or fabricating a value.
        let validation = RociaDbError::validation("page limit must be greater than zero");
        assert_eq!(validation.code(), None);
        assert_eq!(validation.reason(), None);
        assert!(validation.status().is_none());
        assert!(!validation.is_unauthenticated());
        assert!(!validation.is_permission_denied());
        assert!(!validation.is_already_exists());
        assert!(!validation.is_aborted());
    }

    #[test]
    fn validation_constructor_produces_the_validation_variant_with_an_informative_message() {
        let error = RociaDbError::validation("checksum must be exactly 32 bytes (sha256)");
        assert!(matches!(error, RociaDbError::Validation(_)));
        assert_eq!(
            error.to_string(),
            "checksum must be exactly 32 bytes (sha256)"
        );
    }

    #[test]
    fn connection_constructor_produces_its_own_variant() {
        let connection = RociaDbError::connection("invalid host URL");
        assert!(matches!(connection, RociaDbError::Connection { .. }));
        assert_eq!(connection.to_string(), "invalid host URL");
        assert_eq!(connection.code(), None);
    }

    #[test]
    fn status_context_maps_a_failed_rpc_into_the_status_variant() {
        let outcome: std::result::Result<(), Status> =
            Err(Status::not_found("document does not exist"));
        let error = outcome
            .status_context("failed to get document")
            .expect_err("a gRPC error must map to Err");
        assert!(
            matches!(error, RociaDbError::Status { operation, .. } if operation == "failed to get document")
        );
        assert_eq!(error.code(), Some(Code::NotFound));
    }

    #[test]
    fn status_context_passes_success_through_unchanged() {
        let outcome: std::result::Result<u8, Status> = Ok(42);
        let value = outcome
            .status_context("irrelevant")
            .expect("Ok must stay Ok");
        assert_eq!(value, 42);
    }

    #[test]
    fn json_result_ext_maps_encode_and_decode_failures_into_their_own_variants() {
        let bad_json: std::result::Result<Value, serde_json::Error> =
            serde_json::from_str("{ not valid json");

        let decode_error = bad_json
            .decode_context("document json")
            .expect_err("invalid JSON must fail to decode");
        assert!(
            matches!(decode_error, RociaDbError::Decode { context, .. } if context == "document json")
        );
        assert!(
            decode_error.to_string().contains("document json"),
            "message should name what failed to decode, got: {decode_error}"
        );

        // serde_json has no built-in value that fails to serialize (it maps
        // NaN/Infinity to `null` rather than erroring), so a minimal
        // `Serialize` impl that always errors is the deterministic way to
        // exercise the Encode path too, without any network call.
        struct AlwaysFailsToSerialize;
        impl serde::Serialize for AlwaysFailsToSerialize {
            fn serialize<S: serde::Serializer>(
                &self,
                _serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("simulated encode failure"))
            }
        }
        let unserializable: std::result::Result<String, serde_json::Error> =
            serde_json::to_string(&AlwaysFailsToSerialize);
        let encode_error = unserializable
            .encode_context("document json")
            .expect_err("a Serialize impl that always errors must fail to encode");
        assert!(
            matches!(encode_error, RociaDbError::Encode { context, .. } if context == "document json")
        );
        assert!(
            encode_error.to_string().contains("document json"),
            "message should name what failed to encode, got: {encode_error}"
        );
    }

    #[test]
    fn connection_result_ext_wraps_the_source_and_can_nest_a_rociadb_error() {
        let io_error: std::result::Result<(), std::io::Error> = Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "connection refused",
        ));
        let wrapped = io_error
            .connection_context("failed to connect to upstream")
            .expect_err("io error must map to Connection");
        assert!(matches!(wrapped, RociaDbError::Connection { .. }));
        // Display must carry the underlying cause, not just the fixed
        // label: a bare `.to_string()` (or a `%err` tracing field) is the
        // only way most call sites ever see this error, and "connection
        // refused" is what tells it apart from a DNS failure or a TLS
        // mismatch that would otherwise render identically.
        let message = wrapped.to_string();
        assert!(
            message.contains("failed to connect to upstream"),
            "message should name the failed step, got: {message}"
        );
        assert!(
            message.contains("connection refused"),
            "message should carry the underlying cause, got: {message}"
        );
        assert!(
            std::error::Error::source(&wrapped).is_some(),
            "the underlying io::Error must be preserved as the source"
        );

        // A higher-level step can nest an already-typed RociaDbError
        // without losing it, mirroring anyhow's context chaining.
        let inner: std::result::Result<(), RociaDbError> =
            Err(RociaDbError::validation("host must not be empty"));
        let nested = inner
            .connection_context("failed to initialize client")
            .expect_err("nested RociaDbError must map to Connection");
        assert!(matches!(nested, RociaDbError::Connection { .. }));
        let message = nested.to_string();
        assert!(
            message.contains("failed to initialize client"),
            "message should name the failed step, got: {message}"
        );
        assert!(
            message.contains("host must not be empty"),
            "message should carry the nested cause, got: {message}"
        );
        let source = std::error::Error::source(&nested).expect("source must be preserved");
        assert_eq!(source.to_string(), "host must not be empty");
    }

    #[test]
    fn auth_result_ext_wraps_the_source_into_the_auth_variant() {
        let poisoned: std::result::Result<(), std::io::Error> =
            Err(std::io::Error::other("lock poisoned"));
        let error = poisoned
            .auth_context("failed to read cached token")
            .expect_err("a poisoned lock must map to Auth");
        assert!(matches!(error, RociaDbError::Auth { .. }));
        let message = error.to_string();
        assert!(
            message.contains("failed to read cached token"),
            "message should name the failed step, got: {message}"
        );
        assert!(
            message.contains("lock poisoned"),
            "message should carry the underlying cause, got: {message}"
        );
        assert!(
            std::error::Error::source(&error).is_some(),
            "the underlying error must be preserved as the source"
        );
    }
}
