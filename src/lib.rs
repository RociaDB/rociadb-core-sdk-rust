//! Rocia DB SDK client for gRPC upstream services.
//!
//! The client speaks to a RociaDB server over gRPC and covers four service
//! areas: documents, graph nodes and edges, file transfer, and tenants.
//! Every call goes through a single [`RociaDbClient`], built by
//! [`RociaDbBuilder`], which owns the connection and the auth token refresh.
//!
//! # Quick example
//!
//! ```rust,no_run
//! use rociadb_sdk::{DocumentWriteOptions, NodeBinding, RociaDbBuilder};
//! use serde_json::json;
//!
//! # #[tokio::main]
//! # async fn main() -> rociadb_sdk::Result<()> {
//! let client = RociaDbBuilder::new()
//!     .host("http://127.0.0.1:50051")
//!     .auth_client_credentials(
//!         "https://example.com/token",
//!         "client-id",
//!         "client-secret",
//!     )
//!     .build()
//!     .await?;
//!
//! client
//!     .put_document(
//!         "tenant-1",
//!         "products",
//!         "sku-123",
//!         &json!({"sku": "sku-123"}),
//!         DocumentWriteOptions::new()
//!             .with_node_binding(NodeBinding::new("product", "catalog")),
//!     )
//!     .await?;
//!
//! let product: serde_json::Value = client
//!     .get_document("tenant-1", "products", "sku-123")
//!     .await?;
//! # let _ = product;
//! # Ok(())
//! # }
//! ```
//!
//! # One method per operation
//!
//! There is exactly one method per server operation. Everything optional
//! travels in an options or input struct passed as the last argument —
//! never in a `_with_request_id` / `_with_node_binding` / `_as` sibling
//! method:
//!
//! - [`WriteOptions`] for a write whose only tunable is its idempotency
//!   key, [`DocumentWriteOptions`] for a document write (which can also
//!   bind a graph node), and [`FileUploadOptions`] /
//!   [`FileStreamUploadOptions`] for the two ergonomic upload paths. Each
//!   is built with `new()` plus chainable `with_*` setters, and
//!   [`WriteOptions`] documents — in one place — the idempotency key every
//!   write generates when you do not supply one.
//! - [`NodeInput`] and [`EdgeInput`] carry one node or edge to write, by
//!   name rather than as a run of same-typed positional arguments.
//! - Reads that decode a payload are generic over the target type:
//!   `get_document::<Product>(..)`, `get_node::<Value>(..)`,
//!   `get_edge::<Weight>(..)`.
//! - Pagination stays positional (`limit: Option<u32>`,
//!   `cursor: Option<&str>`) and returns [`Page<T>`], or [`DocumentPage<T>`]
//!   for the three document reads that also report a total count.
//!
//! # Authentication
//!
//! [`RociaDbBuilder`] enables OAuth2 client-credentials auth by default,
//! reading `AUTH_TOKEN_URL`, `AUTH_CLIENT_ID` and `AUTH_CLIENT_SECRET` from
//! the environment unless
//! [`auth_client_credentials`](RociaDbBuilder::auth_client_credentials)
//! supplies them. [`build`](RociaDbBuilder::build) fetches the first token
//! and the client keeps it fresh from then on; the credentials and the token
//! are held as [`SecretString`], so they are redacted by every formatter and
//! zeroized when the client is dropped.
//!
//! Three things happen automatically, and none of them needs calling code:
//!
//! - **Scheduled refresh.** A background task refreshes the token after
//!   about two thirds of its lifetime (400 seconds for the 600-second tokens
//!   the IdP issues), and stops when the last clone of the client is
//!   dropped. The cadence follows the lifetime the IdP reports, so it tracks
//!   an IdP that changes it.
//! - **Fast recovery from a failed refresh.** A refresh that fails is
//!   retried after roughly 1 s, then 2 s, 4 s, 8 s, 16 s and 30 s (jittered,
//!   capped at 30 s) until one succeeds, rather than waiting out the whole
//!   regular interval — which would leave a long window in which every RPC
//!   fails with `UNAUTHENTICATED`. See
//!   [`TokenManager::spawn_refresh`](auth::TokenManager::spawn_refresh).
//! - **Refresh-and-retry on `UNAUTHENTICATED`.** A unary RPC that comes back
//!   `UNAUTHENTICATED` triggers one coalesced token refresh and is then
//!   re-issued exactly once. A caller only sees that status when the second
//!   attempt fails too, or when the refresh itself did. The same applies to
//!   the two calls that open a stream but have not yet handed anything over:
//!   [`upload_file`](RociaDbClient::upload_file), whose buffer it owns and can
//!   re-send under the same idempotency key, and the opening call of
//!   [`download_file_stream`](RociaDbClient::download_file_stream) (so also
//!   [`download_file`](RociaDbClient::download_file) and
//!   [`download_file_verified`](RociaDbClient::download_file_verified)).
//! - **A pre-flight refresh before every streaming RPC.** An upload or
//!   download whose cached token has less than a few seconds of life left
//!   refreshes it before opening the call, so a stream never starts on a token
//!   the server is about to reject. This is all
//!   [`upload_file_chunked`](RociaDbClient::upload_file_chunked) and
//!   [`upload_file_stream`](RociaDbClient::upload_file_stream) get: a request
//!   stream the caller has handed over cannot be replayed. A pre-flight
//!   refresh that fails is a `warn!`, not an error — the cached token may
//!   still work, and the call proceeds with it.
//!
//! [`RociaDbClient::refresh_auth_token`] and
//! [`RociaDbClient::invalidate_auth_token`] remain for driving a refresh out
//! of band, and [`disable_auth`](RociaDbBuilder::disable_auth) turns the
//! whole mechanism off for a controlled local deployment.
//!
//! # Timeouts, transport and retries
//!
//! - [`connect_timeout`](RociaDbBuilder::connect_timeout) bounds the dial (10
//!   seconds by default), and
//!   [`request_timeout`](RociaDbBuilder::request_timeout) puts a deadline on
//!   every unary RPC (opt-in, no default).
//! - [`tls_config`](RociaDbBuilder::tls_config) replaces the default
//!   native-roots TLS setup, for a private CA or mTLS, and
//!   [`http2_keep_alive`](RociaDbBuilder::http2_keep_alive) turns on HTTP/2
//!   keep-alive pings for a connection held open across an idle NAT or load
//!   balancer.
//! - [`build_with_channel`](RociaDbBuilder::build_with_channel) takes a
//!   [`Channel`] you built yourself — a custom connector, a Unix socket, a
//!   load-balanced list — and still does all of the auth work above.
//! - [`RetryPolicy`] and [`RociaDbClient::retry`] replay a call while it
//!   fails with `ABORTED`, the one status the server expects callers to
//!   retry (see [`RociaDbError::is_aborted`]) — and with `UNAVAILABLE` too
//!   when [`with_retry_unavailable`](RetryPolicy::with_retry_unavailable)
//!   asks for it.
//!
//! # Building
//!
//! No system dependency is required: `cargo build` on a bare Rust toolchain
//! is enough. The build script compiles the `.proto` bundled with this crate
//! using [`protox`](https://docs.rs/protox), a pure-Rust protobuf compiler,
//! so there is no `protoc` binary to install and no `PROTOC` environment
//! variable to set — on a developer machine, in CI, or on docs.rs. The
//! Google well-known types the API imports come from `protox` itself.
//!
//! # Guides
//!
//! The deep dives live in the repository, next to the code, and every
//! example in them is compiled as a doctest. rustdoc cannot render them as
//! pages of this documentation, so they are listed here by path — read them
//! on
//! [GitHub](https://github.com/RociaDB/rociadb-core-sdk-rust/tree/main/docs)
//! or in a checkout:
//!
//! - `docs/authentication.md` — token lifetime, the background refresh and
//!   its backoff, refresh-and-retry on `UNAUTHENTICATED`, the `auth` module.
//! - `docs/errors-and-retries.md` — every [`RociaDbError`] variant, the
//!   predicates, `ABORTED`, and [`RetryPolicy`].
//! - `docs/documents.md` — writes, reads, listings, queries, and what
//!   `total_count` costs.
//! - `docs/graph.md` — nodes, edges, the `(from, label, to)` uniqueness
//!   rule, batches, and neighbor traversal.
//! - `docs/files.md` — the upload wire contract, the three upload tiers, and
//!   verified downloads.
//! - `docs/pagination.md` — limits, cursors, and the one correct stop
//!   condition.
//! - `docs/tenancy.md` — what `tenant_id` is and is not, the token scopes,
//!   and the tenant registry.
//! - `docs/transport.md` — where TLS terminates, the timeouts, and bringing
//!   your own [`Channel`].
//! - `docs/typescript-parity.md` — the names and shapes that do not
//!   translate mechanically to the TypeScript SDK.
//!
//! # Example project
//!
//! [`example-rust-project`](https://github.com/RociaDB/example-rust-project)
//! wires this SDK into a runnable program, end to end.
//!
//! # Where things live
//!
//! Document, graph, file and tenant calls are all inherent methods on
//! [`RociaDbClient`], and every public type is re-exported at the crate
//! root, so exactly one module is left to name:
//!
//! - [`auth`] — token acquisition and refresh, and the interceptors that
//!   attach credentials to outgoing calls. Useful when you drive
//!   authentication yourself rather than through the builder.
//!
//! # Stability
//!
//! The public API follows semantic versioning from 2.0.0 onward, with one
//! documented exception: the internal `pb` module holds code generated from
//! the `.proto` files by prost and tonic, and is not covered by that
//! promise. A routine prost or tonic upgrade can reshape those generated
//! types without
//! this SDK's own API changing. Five of them — [`CollectionInfo`],
//! [`StatResponse`], [`Neighbor`], [`UploadRequest`] and
//! [`DownloadResponse`] — appear in public signatures and are re-exported at
//! the crate root for that reason; depend on the re-exports: the `pb` module
//! itself is private.
//!
//! The same caveat covers the five types re-exported straight from another
//! crate so that configuring this one needs no extra direct dependency:
//! [`Streaming`], [`Channel`] and [`ClientTlsConfig`] from `tonic`,
//! [`SecretString`] (with [`ExposeSecret`]) from `secrecy`, and [`Bytes`]
//! from `bytes`. A major upgrade of any of those crates can reshape them
//! without this SDK's own API changing.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

/// Compiles every Rust example in `README.md` as a doctest, so a code block
/// that stops matching the API fails `cargo test` instead of misleading a
/// reader.
///
/// The file is attached as documentation on a private, `cfg(doctest)`-only
/// item: rustdoc collects the code blocks when running doctests and the item
/// does not exist in any other build, so nothing about the public API
/// changes. Consequence to keep in mind when editing the file: rustdoc treats
/// an **untagged** fence as Rust, so every non-Rust block must carry its
/// language (` ```toml `, ` ```bash `, ` ```text `), and every Rust block that
/// would talk to a server is ` ```rust,no_run `.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

/// The guides under `docs/`, compiled as doctests on the same terms as
/// [`ReadmeDoctests`]. One item per file so a failure names the guide it came
/// from.
#[cfg(doctest)]
mod guide_doctests {
    #[doc = include_str!("../docs/authentication.md")]
    struct Authentication;
    #[doc = include_str!("../docs/errors-and-retries.md")]
    struct ErrorsAndRetries;
    #[doc = include_str!("../docs/documents.md")]
    struct Documents;
    #[doc = include_str!("../docs/graph.md")]
    struct Graph;
    #[doc = include_str!("../docs/files.md")]
    struct Files;
    #[doc = include_str!("../docs/pagination.md")]
    struct Pagination;
    #[doc = include_str!("../docs/tenancy.md")]
    struct Tenancy;
    #[doc = include_str!("../docs/transport.md")]
    struct Transport;
    #[doc = include_str!("../docs/typescript-parity.md")]
    struct TypeScriptParity;
}

pub mod auth;
mod document;
mod error;
mod file;
mod graph;
pub(crate) mod pb;
mod retry;
mod tenant;

/// Re-exported so callers do not need `bytes` as a direct dependency just to
/// name the item type of the chunk stream
/// [`RociaDbClient::upload_file_chunked`] takes
/// (`Stream<Item = std::io::Result<Bytes>>`). A `tokio_util::io::ReaderStream`
/// already yields exactly that, so a caller wrapping a file or socket never has
/// to build a [`Bytes`] value by hand. The crate documentation's stability
/// caveat applies: a major `bytes` upgrade can reshape this type without the
/// SDK's own API changing.
pub use bytes::Bytes;
pub use document::{
    DocumentPage, DocumentQueryFilter, DocumentQueryOperator, DocumentQuerySort,
    DocumentQuerySortDirection, DocumentWriteOptions, NodeBinding,
};
pub use error::{Result, RociaDbError};
pub use file::{FileStreamUploadOptions, FileUploadOptions};
pub use graph::{Edge, EdgeInput, NeighborNode, NodeInput};
/// Generated protobuf types that appear directly in a public method signature,
/// re-exported here so callers can name them without depending on the crate's
/// private `pb` module. The stability caveat in the crate documentation applies
/// to them: a prost or tonic upgrade can reshape these types without the SDK's
/// own API changing.
pub use pb::upstream::v1::{
    CollectionInfo, DownloadResponse, Neighbor, StatResponse, UploadRequest,
};
pub use retry::RetryPolicy;
/// Re-exported so callers can hold the OAuth2 client secret, and read the
/// tokens this crate hands back, in a type that redacts itself in `Debug`
/// output and zeroizes its buffer on drop — without taking `secrecy` as a
/// direct dependency. [`ExposeSecret::expose_secret`] is the only way to read
/// the bytes back. The crate documentation's stability caveat applies: a
/// major `secrecy` upgrade can reshape these without the SDK's own API
/// changing.
pub use secrecy::{ExposeSecret, SecretString};
/// Re-exported so callers do not need `tonic` as a direct dependency just to
/// name the return type of [`RociaDbClient::download_file_stream`]. The same
/// stability caveat applies: a major tonic upgrade can reshape this type
/// without the SDK's own API changing.
pub use tonic::codec::Streaming;
/// Re-exported so callers do not need `tonic` as a direct dependency just to
/// configure the builder: [`ClientTlsConfig`] for
/// [`RociaDbBuilder::tls_config`] and [`Channel`] for
/// [`RociaDbBuilder::build_with_channel`]. The same stability caveat applies:
/// a major tonic upgrade can reshape these types without the SDK's own API
/// changing.
pub use tonic::transport::{Channel, ClientTlsConfig};

use crate::auth::{BearerInterceptor, TokenManager, TokenRefreshGuard};
use crate::error::{AuthResultExt, ConfigResultExt, ConnectionResultExt, StatusResultExt};
use crate::pb::upstream::v1::PageRequest;
use crate::pb::upstream::v1::document_service_client::DocumentServiceClient;
use crate::pb::upstream::v1::file_service_client::FileServiceClient;
use crate::pb::upstream::v1::graph_service_client::GraphServiceClient;
use crate::pb::upstream::v1::tenant_service_client::TenantServiceClient;
use std::env;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tonic::codegen::InterceptedService;
use tonic::transport::Endpoint;
use tracing::{debug, warn};

/// Max concurrent in-flight requests for batch operations.
const CONCURRENT_REQUESTS: usize = 10;
/// Page size used when the caller does not provide one.
const DEFAULT_PAGE_SIZE: u32 = 20;
const AUTH_TOKEN_URL_ENV: &str = "AUTH_TOKEN_URL";
const AUTH_CLIENT_ID_ENV: &str = "AUTH_CLIENT_ID";
const AUTH_CLIENT_SECRET_ENV: &str = "AUTH_CLIENT_SECRET";
/// Connect timeout applied in [`RociaDbBuilder::build`] when
/// [`RociaDbBuilder::connect_timeout`] was never called, so a host that
/// never answers cannot hang `build()` forever.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Overall deadline applied to every request the OAuth2 HTTP client makes —
/// the token fetch in [`RociaDbBuilder::build`] and every later refresh.
///
/// `reqwest::Client::new()` has no timeout of any kind, so an IdP that
/// accepts the TCP connection and then never answers would hang `build()`,
/// and every [`RociaDbClient::refresh_auth_token`] call after it, forever —
/// the latter while holding the refresh lock, which would block every other
/// task waiting to refresh too. Generous compared with a token endpoint's
/// real latency (tens of milliseconds), because exceeding it fails the
/// client's authentication outright: it exists to break a hang, not to
/// enforce a service level.
const OAUTH_HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// How much of the cached token's lifetime must be left for a streaming RPC to
/// start without refreshing it first. Below this,
/// [`RociaDbClient::refresh_token_before_stream`] refreshes (coalesced with any
/// refresh already in flight) before the call opens.
///
/// A few seconds is genuinely enough, and deliberately much shorter than the
/// token's whole lifetime: **a gRPC server validates the bearer token once,
/// when it accepts the call**, so a transfer that runs for an hour on a
/// 600-second token is not a problem — only a transfer that *starts* with a
/// token already past its expiry is. The margin therefore has to cover just the
/// gap between reading the cached token and the server checking it: the refresh
/// round trip that this triggers, the connection setup, and any scheduling
/// delay in between. Five seconds covers all of it with room to spare, while
/// staying short enough that the check is a no-op on essentially every call —
/// the background refresh task already replaces the token after two thirds of
/// its lifetime, so this only ever fires when that task has been failing.
const STREAMING_TOKEN_REFRESH_MARGIN: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
enum BuilderAuthConfig {
    Enabled {
        token_url: Option<String>,
        client_id: Option<String>,
        /// A [`SecretString`], so `#[derive(Debug)]` on this enum prints a
        /// redaction rather than the secret, and the buffer is zeroized
        /// when the builder (and the `TokenManagerInner` it is cloned into
        /// at `build()`) is dropped — closing the window in which a core
        /// dump, an attached debugger, or swapped-out memory could recover
        /// it.
        client_secret: Option<SecretString>,
    },
    Disabled,
}

/// Builder for [`RociaDbClient`].
///
/// Every setter takes `self` and returns `Self`, so a whole configuration
/// can be written as one chain from a temporary
/// (`RociaDbBuilder::new().host("..").disable_auth()`), or kept in a
/// variable and extended a step at a time. [`RociaDbBuilder::build`] takes
/// `&self`, so one builder can produce several clients.
#[derive(Debug)]
pub struct RociaDbBuilder {
    host: Option<String>,
    auth: BuilderAuthConfig,
    connect_timeout: Option<Duration>,
    request_timeout: Option<Duration>,
    /// `None` means "the default native-roots configuration"; see
    /// [`RociaDbBuilder::tls_config`].
    tls_config: Option<ClientTlsConfig>,
    /// `(interval, timeout)` from [`RociaDbBuilder::http2_keep_alive`], or
    /// `None` for tonic's default of no keep-alive pings at all.
    http2_keep_alive: Option<(Duration, Duration)>,
}

/// gRPC client for document, graph, file, and tenant services.
///
/// `Clone` is cheap: clones share the same underlying channel, token
/// manager, and background token-refresh task (the refresh task keeps
/// running until every clone has been dropped). Every method takes `&self`
/// (not `&mut self`): each call clones the cheap, `Arc`-backed inner
/// service client before issuing its RPC, the same way the batch helpers
/// ([`RociaDbClient::put_nodes`], [`RociaDbClient::add_edges`]) always
/// have. A shared `RociaDbClient` behind an `Arc` therefore needs no
/// `Mutex` to be usable concurrently.
///
/// [`Debug`](std::fmt::Debug) reports the host the client was built for and
/// whether auth is enabled — never a token, a client id, or a secret.
#[derive(Clone)]
pub struct RociaDbClient {
    upstream_document: DocumentServiceClient<InterceptedService<Channel, BearerInterceptor>>,
    upstream_graph: GraphServiceClient<InterceptedService<Channel, BearerInterceptor>>,
    upstream_file: FileServiceClient<InterceptedService<Channel, BearerInterceptor>>,
    upstream_tenant: TenantServiceClient<InterceptedService<Channel, BearerInterceptor>>,
    /// Host this client was built for, kept only so `Debug` can name it.
    /// An `Arc<str>` rather than a `String` so cloning the client stays
    /// allocation-free, as its documentation promises.
    host: Arc<str>,
    /// Deadline applied to every unary RPC, from
    /// [`RociaDbBuilder::request_timeout`]. `None` (the default) means no
    /// client-side deadline at all.
    request_timeout: Option<Duration>,
    /// `None` when auth is disabled. Used to service
    /// [`RociaDbClient::refresh_auth_token`].
    token_manager: Option<TokenManager>,
    /// Keeps the background token-refresh task alive for as long as this
    /// client (or any of its clones) exists. Never read directly, hence the
    /// leading underscore; it exists purely for its `Drop`.
    _token_refresh_guard: Option<Arc<TokenRefreshGuard>>,
}

// Manual `Debug` impl instead of `#[derive(Debug)]`: a derived impl would
// print the four generated service clients (channel internals, and a
// `BearerInterceptor` holding the cached bearer token) and the
// `TokenManager` behind them. Host plus "is auth on" is the whole of what a
// caller can act on, and neither is a credential.
impl std::fmt::Debug for RociaDbClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RociaDbClient")
            .field("host", &self.host)
            .field("auth_enabled", &self.token_manager.is_some())
            .finish_non_exhaustive()
    }
}

/// One page of listed items with the cursor for the next page.
///
/// Returned by every paginated read except the three document reads that
/// also report a total count, which return [`DocumentPage<T>`] instead.
/// `next_cursor` is `None` once the server has no further page. The cursor
/// is opaque and must be passed back unchanged.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page<T> {
    /// The items on this page, in the order the server returned them.
    pub items: Vec<T>,
    /// Cursor to pass back to fetch the page after this one, or `None` when
    /// this is the last page.
    pub next_cursor: Option<String>,
}

/// Per-call options for a write whose only tunable is its idempotency key:
/// [`RociaDbClient::delete_document`], [`RociaDbClient::put_node`],
/// [`RociaDbClient::delete_edge`] and [`RociaDbClient::delete_file`].
///
/// A document write takes [`DocumentWriteOptions`] (a graph node binding on
/// top of the key) and the two ergonomic uploads take
/// [`FileUploadOptions`] / [`FileStreamUploadOptions`], but every one of
/// them defaults its `request_id` the same way — stated once, below.
///
/// # Idempotency key defaults
///
/// The server deduplicates a write on `(tenant, operation, target,
/// request_id)`, so a replay carrying the same `request_id` is recognized
/// as the same write instead of being applied twice. Supply the key
/// yourself — and reuse the same value on every retry — whenever a replay
/// after a timeout must not write twice; markers expire after the server's
/// `gc.request_ttl_secs` (24 hours by default).
///
/// Left unset, the SDK mints a fresh `"<operation>:<uuid>"` key on every
/// call. That makes each call distinct, so it protects against a *network*
/// replay of one call, not against the caller issuing the same logical
/// write twice:
///
/// | Call | Generated `request_id` |
/// | ---- | ---------------------- |
/// | [`put_document`](RociaDbClient::put_document) | `put_document:{collection}:<uuid>` |
/// | [`delete_document`](RociaDbClient::delete_document) | `delete_document:{collection}:<uuid>` |
/// | [`put_node`](RociaDbClient::put_node), [`put_nodes`](RociaDbClient::put_nodes) | `put_node:<uuid>` |
/// | [`add_edge`](RociaDbClient::add_edge), [`add_edges`](RociaDbClient::add_edges) | `add_edge:<uuid>` |
/// | [`delete_edge`](RociaDbClient::delete_edge) | `delete_edge:<uuid>` |
/// | [`upload_file`](RociaDbClient::upload_file), [`upload_file_chunked`](RociaDbClient::upload_file_chunked) | `upload_file:<uuid>` |
/// | [`delete_file`](RociaDbClient::delete_file) | `delete_file:<uuid>` |
///
/// Two cases need a note beyond the table.
/// [`put_document`](RociaDbClient::put_document) with a [`NodeBinding`]
/// issues two writes and deliberately reuses the one `request_id` for both:
/// the dedup scope includes the operation, so the `PutDoc` and `PutNode`
/// markers cannot collide, and replaying the whole call stays idempotent.
/// [`upload_file_stream`](RociaDbClient::upload_file_stream) generates
/// nothing at all — it forwards whatever the caller put on the first
/// message of the stream.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteOptions {
    /// Idempotency key for the write. `None` lets the SDK generate one; see
    /// the [idempotency key defaults](Self#idempotency-key-defaults).
    pub request_id: Option<String>,
}

impl WriteOptions {
    /// Options with every field at its default: no caller-supplied
    /// idempotency key.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the idempotency key for this write; see the [idempotency key
    /// defaults](Self#idempotency-key-defaults) for what happens without
    /// one.
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }
}

/// Build a `PageRequest` applying the SDK default page size.
///
/// The server rejects `limit == 0` with `INVALID_ARGUMENT`; this is
/// rejected here too so the caller gets an immediate, clear error instead
/// of a round trip to the server. The server's own page-size ceiling
/// (`limits.max_page_size`, 200 by default) is intentionally not
/// duplicated here — it is configurable server-side, so any positive limit
/// is forwarded unchanged and the server has the final say.
///
/// `PageRequest::limit` is an `optional` protobuf field, so leaving it
/// unset is distinguishable on the wire from an explicit `0` and makes the
/// server apply its own default (50). The SDK does not use that: it always
/// sends an explicit limit, [`DEFAULT_PAGE_SIZE`] when the caller gave
/// none, so the page size a caller gets never depends on the server's
/// configuration.
pub(crate) fn page_request(
    limit: Option<u32>,
    cursor: Option<&str>,
) -> Result<Option<PageRequest>> {
    if limit == Some(0) {
        return Err(RociaDbError::validation(
            "page limit must be greater than zero",
        ));
    }
    Ok(Some(PageRequest {
        limit: Some(limit.unwrap_or(DEFAULT_PAGE_SIZE)),
        cursor: cursor.unwrap_or_default().to_string(),
    }))
}

/// Map the protobuf empty-string cursor to `None`.
pub(crate) fn non_empty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

/// Reject a `host` URL that carries anything beyond a hostname and port,
/// before any connection attempt: a mistyped host with a leftover path (for
/// example `http://127.0.0.1:50051/v1` pasted from somewhere else), a query
/// string (`http://127.0.0.1:50051?debug=1`), or a fragment
/// (`http://127.0.0.1:50051#note`) would otherwise be silently accepted by
/// tonic, which only reads the authority when dialing and drops everything
/// else on the floor with no error — including a query string a caller
/// might expect to reach the server.
///
/// `http::Uri::path()` already returns `"/"` for a URI with no explicit
/// path component (verified against `http` 1.x), so the path check rejects
/// strictly more than "path is exactly absent". The fragment check runs
/// against the raw string before any parsing happens: `http::Uri` itself
/// scans off and silently discards a fragment while building the `Uri`
/// value (see its `PathAndQuery` parser), so by the time a `Uri` exists
/// there is no `fragment()` accessor left to consult here — the fragment
/// has already vanished.
fn validate_host_path(host: &str) -> Result<()> {
    if host.contains('#') {
        return Err(RociaDbError::config(format!(
            "RociaDB host must contain only a hostname and port, got a fragment in {host:?}"
        )));
    }
    let uri: http::Uri = host.parse().config_context("invalid upstream host")?;
    let path = uri.path();
    if !path.is_empty() && path != "/" {
        return Err(RociaDbError::config(format!(
            "RociaDB host must contain only a hostname and port, got path {path:?}"
        )));
    }
    if let Some(query) = uri.query() {
        return Err(RociaDbError::config(format!(
            "RociaDB host must contain only a hostname and port, got query {query:?}"
        )));
    }
    Ok(())
}

/// Resolve the connect timeout [`RociaDbBuilder::build`] applies: `explicit`
/// when [`RociaDbBuilder::connect_timeout`] was called, or
/// [`DEFAULT_CONNECT_TIMEOUT`] otherwise — rejecting a zero timeout either
/// way. Extracted as a pure, network-free function (mirrors
/// [`validate_host_path`]) so both the default value and the zero-timeout
/// rejection are unit-testable without ever dialing an upstream.
fn resolve_connect_timeout(explicit: Option<Duration>) -> Result<Duration> {
    let connect_timeout = explicit.unwrap_or(DEFAULT_CONNECT_TIMEOUT);
    if connect_timeout.is_zero() {
        return Err(RociaDbError::config(
            "connect timeout must be greater than zero",
        ));
    }
    Ok(connect_timeout)
}

/// Resolve the per-RPC deadline [`RociaDbBuilder::build`] applies: whatever
/// [`RociaDbBuilder::request_timeout`] was given, or `None` for "no
/// client-side deadline". There is deliberately no default — unlike the
/// connect timeout, a request deadline depends on what the caller's own
/// calls do (a `query_documents` over a large collection is not a
/// `get_document`), so guessing one would break slow-but-healthy calls.
///
/// A zero timeout is rejected, exactly as for the connect timeout: it would
/// mean "every RPC fails instantly", which is never what a caller who
/// reached for this method wanted.
fn resolve_request_timeout(explicit: Option<Duration>) -> Result<Option<Duration>> {
    if explicit.is_some_and(|timeout| timeout.is_zero()) {
        return Err(RociaDbError::config(
            "request timeout must be greater than zero",
        ));
    }
    Ok(explicit)
}

/// Build the [`RociaDbError::Status`] a per-RPC deadline produces, carrying a
/// real [`tonic::Status`] so [`RociaDbError::code`] reports
/// [`tonic::Code::DeadlineExceeded`] like any other status.
fn deadline_exceeded(operation: &'static str, timeout: Duration) -> RociaDbError {
    RociaDbError::Status {
        operation,
        status: tonic::Status::deadline_exceeded(format!(
            "the client-side request timeout of {}ms expired",
            u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX)
        )),
    }
}

/// Whether `status` is `tonic`'s own report that the `grpc-timeout` header
/// expired, rather than anything the server said.
///
/// tonic's client channel enforces that header locally, through a timeout
/// layer of its own (`GrpcTimeout`, in `tonic::transport::service`) — but it
/// maps the expiry to `CANCELLED`, not `DEADLINE_EXCEEDED`, and it covers the
/// call only up to the response headers. A caller who set
/// [`RociaDbBuilder::request_timeout`] should see one code for one cause
/// whichever layer notices first, so this recognizes tonic's outcome and
/// [`RociaDbClient::attempt`] reports it as the same [`deadline_exceeded`] as
/// its own `tokio::time::timeout`.
///
/// The test is a walk of the status's source chain looking for
/// [`tonic::TimeoutExpired`] rather than a match on the status message:
/// `tonic::Status::try_from_error` keeps the error it was built from as the
/// status's `source`, so the marker type is there to be found (nested inside
/// a `tonic::transport::Error`) and nothing depends on the wording tonic
/// chose. A status the server sent carries no source at all, so a genuine
/// server-sent `CANCELLED` can never match.
fn is_local_deadline_expired(status: &tonic::Status) -> bool {
    let mut source = std::error::Error::source(status);
    while let Some(error) = source {
        if error.is::<tonic::TimeoutExpired>() {
            return true;
        }
        source = error.source();
    }
    false
}

impl Default for RociaDbBuilder {
    fn default() -> Self {
        Self {
            host: Some("http://127.0.0.1:50051".to_string()),
            auth: BuilderAuthConfig::Enabled {
                token_url: None,
                client_id: None,
                client_secret: None,
            },
            connect_timeout: None,
            request_timeout: None,
            tls_config: None,
            http2_keep_alive: None,
        }
    }
}

impl RociaDbBuilder {
    /// Create a builder with default settings: host
    /// `http://127.0.0.1:50051`, auth enabled and read from the
    /// `AUTH_TOKEN_URL`, `AUTH_CLIENT_ID` and `AUTH_CLIENT_SECRET`
    /// environment variables unless
    /// [`auth_client_credentials`](Self::auth_client_credentials) supplies
    /// them.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the upstream host (for example `http://127.0.0.1:50051`).
    ///
    /// Only a scheme, host and port are accepted; a path, query string or
    /// fragment is rejected by [`build`](Self::build) rather than silently
    /// dropped when dialing.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    /// Configure OAuth2 client credentials for upstream auth, overriding
    /// the `AUTH_TOKEN_URL`, `AUTH_CLIENT_ID` and `AUTH_CLIENT_SECRET`
    /// environment variables [`build`](Self::build) would otherwise read.
    ///
    /// `client_secret` is wrapped in a [`SecretString`] the moment it
    /// arrives, so from here on it is redacted in `Debug` output and
    /// zeroized when the builder and the clients built from it are dropped.
    /// The parameter stays `impl Into<String>` so the ordinary cases
    /// (`&str`, `String`, `std::env::var(..)?`) keep working; pass an owned
    /// `String` where you can, since it moves straight into the secret
    /// instead of being copied out of a buffer nothing will scrub. A caller
    /// already holding a `SecretString` hands over
    /// `secret.expose_secret().to_string()` — one deliberate, visible
    /// exposure, which is the point of the type.
    pub fn auth_client_credentials(
        mut self,
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> Self {
        self.auth = BuilderAuthConfig::Enabled {
            token_url: Some(token_url.into()),
            client_id: Some(client_id.into()),
            client_secret: Some(SecretString::from(client_secret.into())),
        };
        self
    }

    /// Disable auth headers on outgoing requests.
    ///
    /// Intended for a controlled local or test deployment only:
    /// [`build`](Self::build) emits a `warn!` when it takes effect.
    pub fn disable_auth(mut self) -> Self {
        self.auth = BuilderAuthConfig::Disabled;
        self
    }

    /// Set the deadline used while connecting to the upstream host.
    ///
    /// The value is stored as-is here (no validation), the same way
    /// [`host`](Self::host) and
    /// [`auth_client_credentials`](Self::auth_client_credentials) never
    /// validate before [`build`](Self::build) — validation (rejecting a
    /// zero timeout) happens there instead. When this is never called,
    /// `build()` applies a 10-second default unconditionally: without any
    /// timeout at all, `.connect().await` could hang forever against a host
    /// with slow DNS/TCP, which is a robustness gap rather than a mere
    /// convenience.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Set a deadline for every unary RPC the client issues.
    ///
    /// Opt-in with no default: a request deadline depends on what the
    /// caller's own calls do, so the SDK never invents one. The value is
    /// stored as-is (a zero duration is rejected by
    /// [`build`](Self::build), like the connect timeout, rather than here).
    ///
    /// # How it is enforced
    ///
    /// Two ways at once, per attempt:
    ///
    /// - the `grpc-timeout` header is set on the request, so the **server**
    ///   learns the deadline and can abandon the work instead of finishing a
    ///   call nobody is waiting for. tonic's client channel also enforces
    ///   that header locally, but only up to the response headers;
    /// - the whole call future is wrapped in a `tokio::time::timeout`, which
    ///   additionally covers decoding the response message and its trailers.
    ///
    /// Either way the call fails with [`RociaDbError::Status`] whose
    /// [`code`](RociaDbError::code) is
    /// [`DeadlineExceeded`](tonic::Code::DeadlineExceeded).
    ///
    /// # What it does not cover
    ///
    /// The deadline is **per attempt**, not per call: when auth is enabled
    /// and the first attempt comes back `UNAUTHENTICATED`, the refreshed
    /// retry gets a fresh deadline of its own, so a single call can take up
    /// to twice this long (see [`RociaDbClient::refresh_auth_token`]). The
    /// same is true of each attempt made by
    /// [`RociaDbClient::retry`].
    ///
    /// The file transfers are deliberately **not** covered — neither the two
    /// streaming RPCs themselves ([`upload_file_stream`],
    /// [`download_file_stream`]) nor the [`upload_file`],
    /// [`upload_file_chunked`] and [`download_file`] helpers built on them.
    /// How long a stream the caller is feeding or draining may take is a
    /// property of that stream's own data rate, not of the SDK, and a
    /// deadline meant for a single unary round trip would abort a perfectly
    /// healthy multi-gigabyte transfer. Bound those with a
    /// `tokio::time::timeout` of your own around the call.
    ///
    /// [`upload_file`]: RociaDbClient::upload_file
    /// [`upload_file_chunked`]: RociaDbClient::upload_file_chunked
    /// [`upload_file_stream`]: RociaDbClient::upload_file_stream
    /// [`download_file`]: RociaDbClient::download_file
    /// [`download_file_stream`]: RociaDbClient::download_file_stream
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// Replace the TLS configuration used when dialing the upstream host.
    ///
    /// Without this, [`build`](Self::build) uses
    /// `ClientTlsConfig::new().with_native_roots()` — the operating system's
    /// trust store, which is also what the OAuth2 HTTP client trusts. Supply
    /// your own to add a private CA
    /// ([`ca_certificate`](ClientTlsConfig::ca_certificate)), to present a
    /// client certificate for mTLS
    /// ([`identity`](ClientTlsConfig::identity)), or to override the name
    /// the server's certificate is verified against
    /// ([`domain_name`](ClientTlsConfig::domain_name)).
    ///
    /// **tonic only applies TLS to an `https://` host.** A configuration
    /// passed here is stored on the endpoint either way, but it has no
    /// effect on a `http://` host: the connector decides whether to wrap the
    /// socket from the URI scheme alone, so a `http://` host silently stays
    /// plaintext. If TLS matters, the host must say `https://`.
    ///
    /// Has no effect on [`build_with_channel`](Self::build_with_channel),
    /// which does no dialing — configure TLS on the channel you build there.
    pub fn tls_config(mut self, tls_config: ClientTlsConfig) -> Self {
        self.tls_config = Some(tls_config);
        self
    }

    /// Send HTTP/2 keep-alive pings on the connection, every `interval`,
    /// closing it when a ping goes `timeout` unanswered.
    ///
    /// Off by default, which is tonic's own default. Turn it on for a client
    /// that holds a connection open through something that silently drops
    /// idle flows — a NAT, a stateful firewall, a cloud load balancer: with
    /// no keep-alive the SDK only discovers the dead connection when the
    /// next RPC fails on it. `keep_alive_while_idle(true)` is implied, so the
    /// pings continue while no RPC is in flight, which is exactly when the
    /// flow would otherwise be reaped.
    ///
    /// Pick an `interval` comfortably shorter than the idle timeout you are
    /// working around (30 s against a 60 s NAT, say) and a `timeout` of a few
    /// seconds. An `interval` far shorter than that wastes a round trip per
    /// tick per connection, and some servers reject pings they consider too
    /// frequent with an HTTP/2 `ENHANCE_YOUR_CALM`.
    ///
    /// Applies to [`build`](Self::build) only, like every other endpoint
    /// setting: [`build_with_channel`](Self::build_with_channel) uses the
    /// channel as given.
    pub fn http2_keep_alive(mut self, interval: Duration, timeout: Duration) -> Self {
        self.http2_keep_alive = Some((interval, timeout));
        self
    }

    /// Build a client connected to the upstream.
    ///
    /// Takes `&self`, so the same builder can be reused to produce several
    /// clients.
    ///
    /// When auth is enabled, this fetches the first token and starts a
    /// background task that refreshes it before it expires (the IdP's
    /// tokens are short-lived — 600 seconds today) for as long as the
    /// returned `RociaDbClient` or any of its clones is kept alive; a
    /// refresh that fails is retried on a short backoff until one succeeds.
    /// Every unary RPC also refreshes and retries once on its own when the
    /// server answers `UNAUTHENTICATED`, and every streaming RPC refreshes
    /// before it opens when its token is nearly expired, so
    /// [`RociaDbClient::refresh_auth_token`] is only needed to drive a
    /// refresh out of band.
    ///
    /// # Errors
    ///
    /// [`RociaDbError::Config`] for anything wrong with the configuration —
    /// a missing or malformed host, a missing `AUTH_*` value, a zero
    /// timeout, a TLS configuration the endpoint rejects — all detected
    /// before any socket is opened. [`RociaDbError::Connection`] when the
    /// dial itself fails, and [`RociaDbError::Auth`] when the IdP does.
    pub async fn build(&self) -> Result<RociaDbClient> {
        let host = self
            .host
            .as_ref()
            .ok_or_else(|| RociaDbError::config("missing upstream host"))?;
        debug!(
            host = %host,
            auth_enabled = !matches!(self.auth, BuilderAuthConfig::Disabled),
            "building rocia db client"
        );
        validate_host_path(host)?;
        let connect_timeout = resolve_connect_timeout(self.connect_timeout)?;
        let tls_config = self
            .tls_config
            .clone()
            .unwrap_or_else(|| ClientTlsConfig::new().with_native_roots());
        let mut endpoint = Endpoint::from_shared(host.clone())
            .config_context("invalid upstream host")?
            .tls_config(tls_config)
            .config_context("failed to configure TLS")?
            .connect_timeout(connect_timeout);
        if let Some((interval, timeout)) = self.http2_keep_alive {
            endpoint = endpoint
                .http2_keep_alive_interval(interval)
                .keep_alive_timeout(timeout)
                // Without this, tonic only pings while an RPC is in flight —
                // which is never the case for the idle connection this
                // setting exists to keep alive.
                .keep_alive_while_idle(true);
        }
        let channel = endpoint
            .connect()
            .await
            .connection_context("failed to connect to upstream")?;
        self.build_on_channel(channel, host.clone(), connect_timeout)
            .await
    }

    /// Build a client on a [`Channel`] the caller already has, skipping both
    /// host validation and dialing.
    ///
    /// Everything auth-related still happens exactly as in
    /// [`build`](Self::build): the first token is fetched, the background
    /// refresh starts, and the bearer interceptor is installed on all four
    /// service clients. What is skipped is only what belongs to the channel:
    /// the host URL checks, the TLS configuration and the HTTP/2 keep-alive
    /// settings, because the channel handed in has already decided all of
    /// it.
    ///
    /// Use this for a transport `Endpoint` cannot express on its own: a
    /// custom connector (`Endpoint::connect_with_connector`), a Unix domain
    /// socket, a load-balanced `Channel::balance_list`, or an in-process
    /// server in a test. A lazy channel
    /// (`Endpoint::connect_lazy`) works too, and moves the first connection
    /// attempt to the first RPC.
    ///
    /// Both timeouts are still read and still validated.
    /// [`request_timeout`](Self::request_timeout) applies to every unary RPC
    /// however the channel was built, and
    /// [`connect_timeout`](Self::connect_timeout) is what the OAuth2 HTTP
    /// client uses to reach the IdP — the one connection this method does
    /// open itself.
    ///
    /// [`Debug`](std::fmt::Debug) on the returned client reports the host
    /// *configured on the builder*, which here is only a label — the channel
    /// decides where the requests actually go.
    pub async fn build_with_channel(&self, channel: Channel) -> Result<RociaDbClient> {
        let host = self
            .host
            .clone()
            .unwrap_or_else(|| "<caller-supplied channel>".to_string());
        debug!(
            host = %host,
            auth_enabled = !matches!(self.auth, BuilderAuthConfig::Disabled),
            "building rocia db client on a caller-supplied channel"
        );
        let connect_timeout = resolve_connect_timeout(self.connect_timeout)?;
        self.build_on_channel(channel, host, connect_timeout).await
    }

    /// The half of building a client that has nothing to do with the
    /// channel: resolve the request timeout, set up authentication, and wrap
    /// the channel in the four generated service clients.
    ///
    /// Shared by [`build`](Self::build) and
    /// [`build_with_channel`](Self::build_with_channel) so the token-manager
    /// setup — which is where the subtle parts are (environment fallbacks, a
    /// timeout-carrying HTTP client, the background refresh guard the client
    /// must keep alive) — exists exactly once.
    ///
    /// `connect_timeout` is passed in rather than re-resolved: both callers
    /// have already validated it, `build` because it also dials with it.
    async fn build_on_channel(
        &self,
        channel: Channel,
        host: String,
        connect_timeout: Duration,
    ) -> Result<RociaDbClient> {
        let request_timeout = resolve_request_timeout(self.request_timeout)?;
        let (interceptor, token_manager, token_refresh_guard) = match &self.auth {
            BuilderAuthConfig::Disabled => {
                warn!(host = %host, "building rocia db client with auth disabled");
                (BearerInterceptor::disabled(), None, None)
            }
            BuilderAuthConfig::Enabled {
                token_url,
                client_id,
                client_secret,
            } => {
                let token_url = token_url
                    .clone()
                    .or_else(|| env::var(AUTH_TOKEN_URL_ENV).ok())
                    .ok_or_else(|| {
                        RociaDbError::config("missing auth token url (set AUTH_TOKEN_URL)")
                    })?;
                let client_id = client_id
                    .clone()
                    .or_else(|| env::var(AUTH_CLIENT_ID_ENV).ok())
                    .ok_or_else(|| {
                        RociaDbError::config("missing auth client id (set AUTH_CLIENT_ID)")
                    })?;
                let client_secret = client_secret
                    .clone()
                    .or_else(|| {
                        env::var(AUTH_CLIENT_SECRET_ENV)
                            .ok()
                            .map(SecretString::from)
                    })
                    .ok_or_else(|| {
                        RociaDbError::config("missing auth client secret (set AUTH_CLIENT_SECRET)")
                    })?;

                // Both timeouts matter: without them a token endpoint that
                // accepts the connection and never answers would hang this
                // `build()` — and every later refresh, each holding the
                // refresh lock — indefinitely.
                let http = reqwest::Client::builder()
                    .connect_timeout(connect_timeout)
                    .timeout(OAUTH_HTTP_REQUEST_TIMEOUT)
                    .build()
                    .config_context("failed to build the OAuth2 HTTP client")?;

                // `token_url`/`client_id` are deliberately not logged here:
                // they expose the auth infrastructure (IdP endpoint, OAuth2
                // client identity) in any log pipeline configured at debug
                // level.
                debug!(host = %host, "initializing upstream token manager");
                let token_manager = TokenManager::new(http, token_url, client_id, client_secret)
                    .await
                    .auth_context("failed to initialize token manager")?;
                let interceptor = token_manager.interceptor();
                // Without a background refresh, the IdP token would simply
                // expire after its `expires_in` (600s here). Start it now
                // and keep the guard alive inside the client for as long as
                // it (or any clone of it) exists.
                let refresh_interval = token_manager.refresh_interval();
                debug!(
                    host = %host,
                    refresh_interval_secs = refresh_interval.as_secs(),
                    "starting background token refresh"
                );
                let guard = token_manager.spawn_refresh(refresh_interval);
                (interceptor, Some(token_manager), Some(Arc::new(guard)))
            }
        };
        let upstream_document =
            DocumentServiceClient::with_interceptor(channel.clone(), interceptor.clone());
        let upstream_graph =
            GraphServiceClient::with_interceptor(channel.clone(), interceptor.clone());
        let upstream_file =
            FileServiceClient::with_interceptor(channel.clone(), interceptor.clone());
        let upstream_tenant = TenantServiceClient::with_interceptor(channel, interceptor);
        Ok(RociaDbClient {
            upstream_document,
            upstream_graph,
            upstream_file,
            upstream_tenant,
            host: Arc::from(host.as_str()),
            request_timeout,
            token_manager,
            _token_refresh_guard: token_refresh_guard,
        })
    }
}

impl RociaDbClient {
    /// Force an immediate refresh of the upstream auth token.
    ///
    /// **Every unary RPC already does this for you**, once, whenever the
    /// server answers `UNAUTHENTICATED`: the token is refreshed (coalesced
    /// with any concurrent refresh) and the call is re-issued a single time,
    /// so a caller normally sees that status only when the retry failed too.
    /// [`RociaDbClient::upload_file`] and the opening call of
    /// [`RociaDbClient::download_file_stream`] do the same, and every
    /// streaming call refreshes up front when its token is nearly expired.
    /// A background task also refreshes the token before it expires, and
    /// retries on a short backoff when a refresh fails.
    ///
    /// What is left for this method is the out-of-band cases: a token you
    /// know has been revoked, a credential rotation you want to pick up
    /// immediately, an [`upload_file_chunked`](RociaDbClient::upload_file_chunked)
    /// or [`upload_file_stream`](RociaDbClient::upload_file_stream) that failed
    /// with `UNAUTHENTICATED` (a request stream the caller has handed over
    /// cannot be replayed for them, so those two are the only calls left
    /// without an automatic retry), or code that wants to pay the refresh cost
    /// up front rather than on the next call.
    ///
    /// `UNAUTHENTICATED` is the renewal signal, as opposed to
    /// `PERMISSION_DENIED`, which means the token is valid but lacks the
    /// required scope — refreshing it will not help. A no-op returning
    /// `Ok(())` when the client was built with
    /// [`RociaDbBuilder::disable_auth`].
    pub async fn refresh_auth_token(&self) -> Result<()> {
        match &self.token_manager {
            Some(manager) => manager.refresh_now().await,
            None => Ok(()),
        }
    }

    /// Signal that the cached upstream auth token should no longer be
    /// trusted, without waiting for a fresh one.
    ///
    /// This is the lazy counterpart to
    /// [`RociaDbClient::refresh_auth_token`]: it is **synchronous** and
    /// returns immediately — it only wakes the background refresh task
    /// (started by [`RociaDbBuilder::build`]) so it refreshes at the next
    /// opportunity, instead of making the caller pay for the network round
    /// trip. Prefer this over [`RociaDbClient::refresh_auth_token`] when
    /// you just want to mark the token stale (for example, from a
    /// fire-and-forget error handler) rather than block until a new one is
    /// in hand before retrying. A no-op when the client was built with
    /// [`RociaDbBuilder::disable_auth`].
    pub fn invalidate_auth_token(&self) {
        if let Some(manager) = &self.token_manager {
            manager.request_refresh();
        }
    }

    /// Issue one unary RPC: wrap `message` in a [`tonic::Request`], apply
    /// the per-RPC deadline, hand it to `call`, refresh-and-retry once on
    /// `UNAUTHENTICATED`, and map a non-OK [`tonic::Status`] into
    /// [`RociaDbError::Status`] tagged with `operation`.
    ///
    /// Every unary call in the crate goes through here — including the ones
    /// the batch helpers ([`RociaDbClient::put_nodes`],
    /// [`RociaDbClient::add_edges`]) and the neighbor-node fan-out issue
    /// one per item.
    ///
    /// That single choke point is the point: both behaviours below are
    /// properties of "any unary RPC", and belong here rather than repeated
    /// at twenty call sites. The deadline is the half that is genuinely
    /// unary-only; the refresh-and-retry half is shared with the opening call
    /// of the `Download` stream through
    /// [`RociaDbClient::attempt_with_replay`], and with
    /// [`RociaDbClient::upload_file`]'s replay of its own request stream
    /// through [`RociaDbClient::refresh_for_replay`].
    ///
    /// # Deadline
    ///
    /// When [`RociaDbBuilder::request_timeout`] was set, each attempt both
    /// carries a `grpc-timeout` header (so the server can abandon the work)
    /// and is wrapped in a `tokio::time::timeout` (which also covers
    /// decoding the response body, unlike tonic's own header-phase
    /// enforcement). Both paths surface the same
    /// `DEADLINE_EXCEEDED`-carrying error.
    ///
    /// # Refresh-and-retry on `UNAUTHENTICATED`
    ///
    /// With auth enabled, a first attempt answered `UNAUTHENTICATED` — the
    /// status the server uses to mean "renew your token" — triggers
    /// [`TokenManager::refresh_now`](auth::TokenManager::refresh_now), which
    /// coalesces with any refresh already in flight, and the call is
    /// re-issued exactly once. Never more than once: a second
    /// `UNAUTHENTICATED` against a token minted moments earlier is a
    /// credential or scope problem that looping cannot fix. If the refresh
    /// itself fails, the *original* `UNAUTHENTICATED` is returned (it
    /// describes what the caller actually asked for) and the refresh failure
    /// is reported as a `warn!`.
    pub(crate) async fn unary<Req, Resp, F, Fut>(
        &self,
        operation: &'static str,
        message: Req,
        call: F,
    ) -> Result<Resp>
    where
        Req: Clone,
        F: Fn(tonic::Request<Req>) -> Fut,
        Fut: Future<Output = std::result::Result<tonic::Response<Resp>, tonic::Status>>,
    {
        self.attempt_with_replay(operation, message, self.request_timeout, call)
            .await
    }

    /// Open one server-streaming RPC — in this crate, `Download` — with the
    /// same refresh-and-retry as [`RociaDbClient::unary`] and **no** per-RPC
    /// deadline.
    ///
    /// Only the *opening* call is covered, which is exactly the call this
    /// returns: tonic resolves a server-streaming request once the response
    /// headers arrive, and a server that rejects the call outright answers
    /// with a trailers-only response (the `grpc-status` in the headers), which
    /// tonic turns into an `Err` from this very future — before a single
    /// message exists. An `UNAUTHENTICATED` rejection is therefore replayable
    /// on identical terms to a unary call: nothing has been handed to the
    /// caller and nothing has been consumed. A status that arrives *later*,
    /// in the stream's trailers, reaches the caller through
    /// [`Streaming::message`](tonic::codec::Streaming::message) and is none of
    /// this function's business.
    ///
    /// # Why no deadline
    ///
    /// [`RociaDbBuilder::request_timeout`] is deliberately not applied, and
    /// that is a decision about the `grpc-timeout` header rather than about
    /// the local `tokio::time::timeout`. The header would announce a deadline
    /// for the whole RPC, and how long a download takes is a property of the
    /// file's size and the link, not of a round trip.
    ///
    /// Two concrete consequences, the first verified against tonic 0.14:
    /// tonic's own server enforces the header through the same
    /// `GrpcTimeout` layer as its client, and that layer only races the future
    /// that produces the response *headers* — so against a tonic server the
    /// header would not truncate a slow body, but it *would* kill a download
    /// whose first response header takes longer than the deadline (a cold
    /// file, a slow storage seek), reporting `CANCELLED` on a transfer that
    /// was merely slow to start. And the gRPC specification makes
    /// `grpc-timeout` a deadline for the entire call, so a server that
    /// implements it that way — most do — would cut the stream mid-body once
    /// it expired, turning a healthy multi-gigabyte transfer into a truncated
    /// one. Callers who want a bound on a transfer wrap the whole thing in a
    /// `tokio::time::timeout` of their own, which is what the public docs say.
    pub(crate) async fn server_streaming<Req, Resp, F, Fut>(
        &self,
        operation: &'static str,
        message: Req,
        call: F,
    ) -> Result<Resp>
    where
        Req: Clone,
        F: Fn(tonic::Request<Req>) -> Fut,
        Fut: Future<Output = std::result::Result<tonic::Response<Resp>, tonic::Status>>,
    {
        self.attempt_with_replay(operation, message, None, call)
            .await
    }

    /// The replay core shared by [`RociaDbClient::unary`] and
    /// [`RociaDbClient::server_streaming`]: one attempt under `timeout`, and
    /// on `UNAUTHENTICATED` one token refresh followed by exactly one more
    /// attempt. `timeout` is a parameter rather than a read of
    /// [`RociaDbClient::request_timeout`] precisely so the two callers can
    /// differ on the deadline while sharing every rule about the replay.
    ///
    /// This is why `Req: Clone`: the first attempt consumes `message`, so a
    /// replay needs a copy made beforehand. The clone happens only when auth
    /// is enabled — with `disable_auth()` no refresh exists, nothing can be
    /// replayed, and every RPC in the crate would otherwise pay for a copy
    /// that is never read.
    ///
    /// `call` takes the whole `tonic::Request` (not just the message) so this
    /// function and [`RociaDbClient::attempt`] stay the only places that touch
    /// per-call metadata and extensions. It is `Fn`, not `FnOnce`, for the
    /// same replay reason.
    async fn attempt_with_replay<Req, Resp, F, Fut>(
        &self,
        operation: &'static str,
        message: Req,
        timeout: Option<Duration>,
        call: F,
    ) -> Result<Resp>
    where
        Req: Clone,
        F: Fn(tonic::Request<Req>) -> Fut,
        Fut: Future<Output = std::result::Result<tonic::Response<Resp>, tonic::Status>>,
    {
        let replay = self.token_manager.as_ref().map(|_| Clone::clone(&message));
        let error = match self.attempt(operation, message, timeout, &call).await {
            Ok(response) => return Ok(response),
            Err(error) => error,
        };
        // A token manager is present exactly when auth is enabled, which is
        // exactly when `replay` was cloned above — so this never falls through
        // in practice, and returning the original error is the right answer if
        // it ever did.
        let Some(replay) = replay else {
            return Err(error);
        };
        if !self.refresh_for_replay(operation, &error).await {
            return Err(error);
        }
        self.attempt(operation, replay, timeout, &call).await
    }

    /// Whether `error` is an `UNAUTHENTICATED` the caller should replay once,
    /// refreshing the token here if so.
    ///
    /// The one place the crate's "refresh once, then replay once" rule lives.
    /// [`RociaDbClient::attempt_with_replay`] uses it for every unary RPC and
    /// for the `Download` stream's opening call, and
    /// [`RociaDbClient::upload_file`] uses it directly, because its request is
    /// a *stream* and so has to be rebuilt rather than cloned — but must
    /// follow exactly the same rules.
    ///
    /// `false` for anything other than `UNAUTHENTICATED`, `false` when auth is
    /// disabled (there is no token to refresh and nothing a replay could
    /// change), and `false` when the refresh itself failed — which is also the
    /// one case that emits a `warn!`, since the original `UNAUTHENTICATED`
    /// that the caller then returns says nothing about why no new token could
    /// be had. Never loops: one call, one refresh, and the decision is the
    /// caller's to act on once.
    pub(crate) async fn refresh_for_replay(
        &self,
        operation: &'static str,
        error: &RociaDbError,
    ) -> bool {
        if !error.is_unauthenticated() {
            return false;
        }
        let Some(manager) = &self.token_manager else {
            return false;
        };
        debug!(
            operation,
            "upstream rejected the call as unauthenticated; refreshing the token and retrying once"
        );
        if let Err(refresh_error) = manager.refresh_now().await {
            warn!(
                operation,
                error = %refresh_error,
                "refreshing the auth token after an UNAUTHENTICATED response failed; returning \
                 the original error"
            );
            return false;
        }
        true
    }

    /// Refresh the auth token before a streaming RPC when little of its
    /// lifetime is left, and carry on regardless of whether that worked.
    ///
    /// The pre-flight half of what the streaming RPCs get in place of the
    /// refresh-and-retry a unary RPC enjoys. A no-op when auth is disabled,
    /// and — thanks to
    /// [`TokenManager::ensure_fresh`](auth::TokenManager::ensure_fresh) — a
    /// clock read and nothing more whenever more than
    /// [`STREAMING_TOKEN_REFRESH_MARGIN`] of the cached token's lifetime
    /// remains, which is the normal case.
    ///
    /// A failed refresh is a `warn!` and not an error: the cached token is
    /// still there and may well still be valid (the background refresh task
    /// may simply have hit a blip, or the margin may be nowhere near the real
    /// expiry), so failing the caller's upload or download over it would turn
    /// a working call into a broken one. If the token really is finished, the
    /// server says `UNAUTHENTICATED` and that reaches the caller.
    pub(crate) async fn refresh_token_before_stream(&self, operation: &'static str) {
        let Some(manager) = &self.token_manager else {
            return;
        };
        if let Err(error) = manager.ensure_fresh(STREAMING_TOKEN_REFRESH_MARGIN).await {
            warn!(
                operation,
                error = %error,
                "refreshing the auth token before a streaming RPC failed; continuing with the \
                 cached token"
            );
        }
    }

    /// One attempt at an RPC, with `timeout` applied as the per-attempt
    /// deadline (`None` for no client-side deadline at all).
    ///
    /// Split out of [`RociaDbClient::attempt_with_replay`] because the
    /// refresh-and-retry path needs to run it twice, and each attempt must get
    /// its own deadline: a retry that inherited the first attempt's remaining
    /// budget would routinely be born already expired.
    async fn attempt<Req, Resp, F, Fut>(
        &self,
        operation: &'static str,
        message: Req,
        timeout: Option<Duration>,
        call: &F,
    ) -> Result<Resp>
    where
        F: Fn(tonic::Request<Req>) -> Fut,
        Fut: Future<Output = std::result::Result<tonic::Response<Resp>, tonic::Status>>,
    {
        let mut request = tonic::Request::new(message);
        let Some(timeout) = timeout else {
            return Ok(call(request).await.status_context(operation)?.into_inner());
        };
        // Tells the server the deadline (it can then stop work nobody is
        // waiting for) and arms tonic's own client-side `grpc-timeout`
        // layer, which covers the call up to the response headers.
        request.set_timeout(timeout);
        // The outer timeout additionally covers decoding the response
        // message and reading its trailers, which happen after tonic's layer
        // has already resolved.
        match tokio::time::timeout(timeout, call(request)).await {
            Err(_elapsed) => Err(deadline_exceeded(operation, timeout)),
            Ok(Err(status)) if is_local_deadline_expired(&status) => {
                Err(deadline_exceeded(operation, timeout))
            }
            Ok(outcome) => Ok(outcome.status_context(operation)?.into_inner()),
        }
    }
}

/// Helpers shared by the unit tests of several modules.
#[cfg(test)]
pub(crate) mod test_support {
    use super::{
        BearerInterceptor, DocumentServiceClient, FileServiceClient, GraphServiceClient,
        RociaDbClient, TenantServiceClient,
    };
    use std::sync::Arc;
    use tonic::transport::Endpoint;

    /// A `RociaDbClient` wired to a channel that never actually dials
    /// (`Endpoint::connect_lazy` performs no I/O — it only builds a
    /// connector that would try to connect on the *first real RPC*). Used
    /// to test the client-side gating that must reject a request before
    /// ever reaching the network — if such a test regressed and the
    /// gating ran too late, it would hang or fail against the unreachable
    /// `127.0.0.1:1` host instead of returning promptly.
    pub(crate) fn lazy_test_client() -> RociaDbClient {
        lazy_test_client_with_request_timeout(None)
    }

    /// [`lazy_test_client`] with an explicit per-RPC deadline, for the tests
    /// that exercise the deadline path in `RociaDbClient::attempt`.
    pub(crate) fn lazy_test_client_with_request_timeout(
        request_timeout: Option<std::time::Duration>,
    ) -> RociaDbClient {
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let interceptor = BearerInterceptor::disabled();
        RociaDbClient {
            upstream_document: DocumentServiceClient::with_interceptor(
                channel.clone(),
                interceptor.clone(),
            ),
            upstream_graph: GraphServiceClient::with_interceptor(
                channel.clone(),
                interceptor.clone(),
            ),
            upstream_file: FileServiceClient::with_interceptor(
                channel.clone(),
                interceptor.clone(),
            ),
            upstream_tenant: TenantServiceClient::with_interceptor(channel, interceptor),
            host: Arc::from("http://127.0.0.1:1"),
            request_timeout,
            token_manager: None,
            _token_refresh_guard: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_CONNECT_TIMEOUT, Endpoint, OAUTH_HTTP_REQUEST_TIMEOUT, RociaDbBuilder,
        RociaDbClient, WriteOptions, deadline_exceeded, is_local_deadline_expired, page_request,
        resolve_connect_timeout, resolve_request_timeout, validate_host_path,
    };
    use crate::RociaDbError;
    use crate::test_support::{lazy_test_client, lazy_test_client_with_request_timeout};
    use std::time::Duration;

    #[test]
    fn client_is_send_sync_so_an_arc_needs_no_mutex() {
        // `RociaDbClient` methods take `&self`, not `&mut self` (each call
        // clones the cheap, Arc-backed inner service client before issuing
        // its RPC). This is only sound to share across tasks if the type is
        // both `Send` and `Sync`: a plain compile-time trait assertion, not
        // a runtime check, but it locks in the intent so a future field
        // that breaks it fails the build instead of shipping silently.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RociaDbClient>();
        assert_send_sync::<std::sync::Arc<RociaDbClient>>();
    }

    // `page_request`'s None-defaults case and its zero-limit rejection are
    // exercised elsewhere (`graph::tests`), but neither covers the common
    // case of a caller-supplied limit and cursor actually reaching the
    // `PageRequest` unchanged — the one behavior every paginated RPC in this
    // crate depends on.
    #[test]
    fn page_request_passes_through_an_explicit_limit_and_cursor_unchanged() {
        let page = page_request(Some(75), Some("cursor-x"))
            .expect("a positive limit with a cursor must be accepted")
            .expect("a page request must always be produced");
        assert_eq!(page.limit, Some(75));
        assert_eq!(page.cursor, "cursor-x");
    }

    #[test]
    fn write_options_default_to_no_request_id() {
        assert_eq!(WriteOptions::new(), WriteOptions::default());
        assert!(WriteOptions::new().request_id.is_none());
    }

    #[test]
    fn write_options_with_request_id_is_chainable_and_readable() {
        let options = WriteOptions::new().with_request_id("retry-1");
        assert_eq!(options.request_id.as_deref(), Some("retry-1"));
    }

    #[test]
    fn host_path_validation_accepts_a_host_with_no_path_component() {
        validate_host_path("http://127.0.0.1:50051").expect("an absent path must be accepted");
    }

    #[test]
    fn host_path_validation_accepts_a_bare_root_path() {
        validate_host_path("http://127.0.0.1:50051/").expect("a bare \"/\" must be accepted");
    }

    #[test]
    fn host_path_validation_rejects_a_host_carrying_a_leftover_path() {
        let error = validate_host_path("http://127.0.0.1:50051/v1")
            .expect_err("a host with a non-root path must be rejected");
        assert!(matches!(error, RociaDbError::Config { .. }));
        assert!(
            error.to_string().contains("/v1"),
            "the error should name the offending path, got: {error}"
        );
    }

    #[test]
    fn host_path_validation_rejects_a_host_carrying_a_leftover_query_string() {
        // A query string parses with `path() == "/"`, so without a
        // dedicated check it would sail through the path-only validation
        // and tonic would silently drop it when dialing.
        let error = validate_host_path("http://127.0.0.1:50051?debug=1")
            .expect_err("a host with a query string must be rejected");
        assert!(matches!(error, RociaDbError::Config { .. }));
        assert!(
            error.to_string().contains("debug=1"),
            "the error should name the offending query, got: {error}"
        );
    }

    #[test]
    fn host_path_validation_rejects_a_host_carrying_a_leftover_fragment() {
        let error = validate_host_path("http://127.0.0.1:50051#note")
            .expect_err("a host with a fragment must be rejected");
        assert!(matches!(error, RociaDbError::Config { .. }));
        assert!(
            error.to_string().contains('#'),
            "the error should mention the fragment, got: {error}"
        );
    }

    #[test]
    fn default_connect_timeout_matches_the_typescript_sdk_default() {
        assert_eq!(DEFAULT_CONNECT_TIMEOUT, Duration::from_secs(10));
    }

    #[test]
    fn the_oauth_http_client_timeout_is_documented_and_finite() {
        // The point of the constant is that it exists at all: a
        // `reqwest::Client` with no timeout turns an unresponsive IdP into a
        // permanent hang inside `build()` and every later refresh.
        assert_eq!(OAUTH_HTTP_REQUEST_TIMEOUT, Duration::from_secs(30));
    }

    #[test]
    fn resolve_connect_timeout_falls_back_to_the_default_when_unset() {
        let timeout =
            resolve_connect_timeout(None).expect("the default timeout must always be accepted");
        assert_eq!(timeout, DEFAULT_CONNECT_TIMEOUT);
    }

    #[test]
    fn resolve_connect_timeout_accepts_a_caller_supplied_positive_value() {
        let timeout = resolve_connect_timeout(Some(Duration::from_secs(3)))
            .expect("a positive explicit timeout must be accepted");
        assert_eq!(timeout, Duration::from_secs(3));
    }

    #[test]
    fn resolve_connect_timeout_rejects_zero() {
        let error = resolve_connect_timeout(Some(Duration::ZERO))
            .expect_err("a zero connect timeout must be rejected");
        assert!(matches!(error, RociaDbError::Config { .. }));
        assert!(error.to_string().contains("greater than zero"));
    }

    #[test]
    fn resolve_request_timeout_has_no_default_and_rejects_zero() {
        // Unlike the connect timeout, an absent request timeout stays absent:
        // the SDK must not invent a deadline for calls whose duration it
        // cannot predict.
        assert_eq!(
            resolve_request_timeout(None).expect("an absent request timeout must be accepted"),
            None
        );
        assert_eq!(
            resolve_request_timeout(Some(Duration::from_secs(4)))
                .expect("a positive explicit timeout must be accepted"),
            Some(Duration::from_secs(4))
        );
        let error = resolve_request_timeout(Some(Duration::ZERO))
            .expect_err("a zero request timeout must be rejected");
        assert!(matches!(error, RociaDbError::Config { .. }));
        assert!(error.to_string().contains("greater than zero"));
    }

    #[test]
    fn a_deadline_surfaces_as_a_status_error_reporting_deadline_exceeded() {
        let error = deadline_exceeded("failed to get document", Duration::from_millis(1500));
        assert_eq!(error.code(), Some(tonic::Code::DeadlineExceeded));
        let message = error.to_string();
        assert!(
            message.contains("failed to get document"),
            "the operation must still be named, got: {message}"
        );
        assert!(
            message.contains("1500ms"),
            "the expired deadline should be readable, got: {message}"
        );
    }

    #[test]
    fn tonics_own_grpc_timeout_expiry_is_recognised_and_nothing_else_is() {
        // This is the status tonic's client-side `grpc-timeout` layer
        // produces: `CANCELLED`, not `DEADLINE_EXCEEDED`, which is exactly
        // why `attempt` normalizes it. The assertion on the code documents
        // tonic's mapping, so a version that changes it shows up here.
        let from_tonic = tonic::Status::from_error(Box::new(tonic::TimeoutExpired(())));
        assert_eq!(from_tonic.code(), tonic::Code::Cancelled);
        assert!(
            is_local_deadline_expired(&from_tonic),
            "a status built from TimeoutExpired must be recognised through its source chain"
        );

        // A status the server sent carries no source at all, so a genuine
        // server-sent CANCELLED — even one worded like tonic's — must pass
        // through untouched.
        assert!(!is_local_deadline_expired(&tonic::Status::cancelled(
            "client cancelled the call"
        )));
        assert!(!is_local_deadline_expired(&tonic::Status::cancelled(
            "Timeout expired"
        )));
        assert!(!is_local_deadline_expired(&tonic::Status::aborted(
            "write conflict, retry"
        )));
    }

    #[tokio::test]
    async fn a_request_timeout_fires_against_a_host_that_accepts_and_never_answers() {
        // A plain TCP listener that accepts the connection and then says
        // nothing: the HTTP/2 handshake never completes, so tonic's own
        // `grpc-timeout` layer — which only arms once the connection is
        // ready, inside `Connection::call` — never gets a chance to fire.
        // Only the SDK's own `tokio::time::timeout` can end this call, which
        // is precisely why it wraps the whole future rather than trusting
        // the header alone.
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("binding an ephemeral port");
        let address = listener.local_addr().expect("the bound address");
        let accepting = std::thread::spawn(move || {
            // Hold the accepted socket open (dropping it would send a FIN
            // and let the client fail early for the wrong reason) for longer
            // than the deadline under test, then let the thread end.
            let held = listener.incoming().next().and_then(std::result::Result::ok);
            std::thread::sleep(Duration::from_secs(2));
            drop(held);
        });

        let channel = Endpoint::from_shared(format!("http://{address}"))
            .expect("a loopback address must parse as an endpoint")
            .connect_lazy();
        let client = RociaDbBuilder::new()
            .disable_auth()
            .request_timeout(Duration::from_millis(150))
            .build_with_channel(channel)
            .await
            .expect("a lazy channel with auth disabled must build");

        let started = std::time::Instant::now();
        let error = client
            .list_tenants(Some(1), None)
            .await
            .expect_err("a silent server must not let the call hang");
        let elapsed = started.elapsed();
        assert_eq!(
            error.code(),
            Some(tonic::Code::DeadlineExceeded),
            "an expired request timeout must surface as DEADLINE_EXCEEDED, got: {error}"
        );
        assert!(
            elapsed < Duration::from_secs(1),
            "the deadline must fire promptly, took {elapsed:?}"
        );
        accepting
            .join()
            .expect("the listener thread must not panic");
    }

    #[tokio::test]
    async fn a_request_timeout_bounds_a_unary_call_against_an_unreachable_host() {
        // The channel is lazy and 127.0.0.1:1 refuses (or drops) the
        // connection, so this exercises `attempt`'s deadline path end to
        // end: whichever of the two mechanisms notices first, the error must
        // report a gRPC code and come back promptly rather than hanging.
        let client = lazy_test_client_with_request_timeout(Some(Duration::from_millis(50)));
        let error = client
            .list_tenants(Some(1), None)
            .await
            .expect_err("an unreachable host must fail");
        assert!(
            error.code().is_some(),
            "a failed unary call must carry a gRPC status, got: {error}"
        );
    }

    #[test]
    fn builder_setters_are_chainable_from_a_temporary_and_from_a_binding() {
        // The whole point of the owned (`self -> Self`) setter style: a
        // chain started on a temporary stays usable, and a half-configured
        // builder can be stored in a variable and extended later. Under the
        // previous `&mut self -> &mut Self` shape the second form did not
        // compile at all (it borrowed a dropped temporary), so this is a
        // compile-time assertion first and a value check second.
        let chained = RociaDbBuilder::new()
            .host("http://127.0.0.1:50051")
            .connect_timeout(Duration::from_secs(7))
            .request_timeout(Duration::from_secs(9))
            .tls_config(crate::ClientTlsConfig::new().with_native_roots())
            .http2_keep_alive(Duration::from_secs(30), Duration::from_secs(5))
            .disable_auth();
        assert_eq!(chained.connect_timeout, Some(Duration::from_secs(7)));
        assert_eq!(chained.request_timeout, Some(Duration::from_secs(9)));
        assert!(chained.tls_config.is_some());
        assert_eq!(
            chained.http2_keep_alive,
            Some((Duration::from_secs(30), Duration::from_secs(5)))
        );

        let partial = RociaDbBuilder::new().host("http://example.invalid:50051");
        let finished = partial.disable_auth();
        assert_eq!(
            finished.host.as_deref(),
            Some("http://example.invalid:50051")
        );
    }

    #[test]
    fn builder_defaults_leave_every_new_transport_setting_unset() {
        // Each of these must stay opt-in: a default request deadline would
        // break slow-but-healthy calls, a default keep-alive would add
        // traffic nobody asked for, and `None` for the TLS config is what
        // selects the documented native-roots default.
        let builder = RociaDbBuilder::new();
        assert_eq!(builder.request_timeout, None);
        assert_eq!(builder.http2_keep_alive, None);
        assert!(builder.tls_config.is_none());
    }

    #[test]
    fn builder_connect_timeout_setter_stores_the_value_unvalidated() {
        // Mirrors `RociaDbBuilder::host` / `auth_client_credentials`: the
        // setter never validates, only `build()` does (via
        // `resolve_connect_timeout`, tested above) — so even a nonsensical
        // zero duration must be stored as-is here.
        let builder = RociaDbBuilder::new().connect_timeout(Duration::ZERO);
        assert_eq!(builder.connect_timeout, Some(Duration::ZERO));

        let builder = RociaDbBuilder::new().connect_timeout(Duration::from_secs(42));
        assert_eq!(builder.connect_timeout, Some(Duration::from_secs(42)));
    }

    #[tokio::test]
    async fn build_rejects_a_zero_connect_timeout_before_any_network_call() {
        // `validate_host_path` and the connect-timeout check both run
        // before `Endpoint::connect()`, so this must return promptly with
        // `Config` instead of hanging or failing against the (deliberately
        // unreachable) host.
        let error = RociaDbBuilder::new()
            .host("http://127.0.0.1:1")
            .connect_timeout(Duration::ZERO)
            .build()
            .await
            .expect_err("a zero connect timeout must fail build()");
        assert!(matches!(error, RociaDbError::Config { .. }));
    }

    #[tokio::test]
    async fn build_rejects_a_host_with_a_leftover_path_before_any_network_call() {
        let error = RociaDbBuilder::new()
            .host("http://127.0.0.1:1/v1")
            .build()
            .await
            .expect_err("a host carrying a path must fail build()");
        assert!(matches!(error, RociaDbError::Config { .. }));
    }

    #[tokio::test]
    async fn build_with_channel_skips_host_validation_and_dialing() {
        // A host that `build()` rejects outright, on a lazy channel that
        // never connects: `build_with_channel` must still produce a client,
        // because neither the host string nor the dial is its business.
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let client = RociaDbBuilder::new()
            .host("http://127.0.0.1:1/a/path?and=query")
            .disable_auth()
            .build_with_channel(channel)
            .await
            .expect("a caller-supplied channel must bypass host validation and dialing");
        // The host is carried through purely as a `Debug` label.
        let debug_output = format!("{client:?}");
        assert!(debug_output.contains("http://127.0.0.1:1/a/path?and=query"));
        assert!(debug_output.contains("auth_enabled: false"));
    }

    #[tokio::test]
    async fn build_with_channel_still_validates_the_timeouts_it_uses() {
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let error = RociaDbBuilder::new()
            .disable_auth()
            .request_timeout(Duration::ZERO)
            .build_with_channel(channel)
            .await
            .expect_err("a zero request timeout must fail build_with_channel()");
        assert!(matches!(error, RociaDbError::Config { .. }));

        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let error = RociaDbBuilder::new()
            .disable_auth()
            .connect_timeout(Duration::ZERO)
            .build_with_channel(channel)
            .await
            .expect_err("a zero connect timeout must fail build_with_channel() too");
        assert!(matches!(error, RociaDbError::Config { .. }));
    }

    #[tokio::test]
    async fn build_with_channel_carries_the_request_timeout_onto_the_client() {
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let client = RociaDbBuilder::new()
            .disable_auth()
            .request_timeout(Duration::from_millis(250))
            .build_with_channel(channel)
            .await
            .expect("a lazy channel with auth disabled must build");
        assert_eq!(client.request_timeout, Some(Duration::from_millis(250)));
    }

    // `SecretString`'s own `Debug` redacts, which is what lets
    // `BuilderAuthConfig` derive `Debug` instead of hand-writing one. Keep
    // the assertion: it is the property that matters, whoever implements it.
    #[test]
    fn builder_debug_output_redacts_the_client_secret() {
        let builder = RociaDbBuilder::new().auth_client_credentials(
            "https://idp.example.com/token",
            "client-123",
            "super-secret-value",
        );
        let debug_output = format!("{builder:?}");
        assert!(
            !debug_output.contains("super-secret-value"),
            "the raw client_secret must never appear in Debug output, got: {debug_output}"
        );
        assert!(
            debug_output.to_ascii_lowercase().contains("redacted"),
            "the redaction placeholder must appear, got: {debug_output}"
        );
        // Non-sensitive fields must stay visible: only the secret is
        // redacted, not the whole auth config (still useful for
        // diagnostics).
        assert!(debug_output.contains("https://idp.example.com/token"));
        assert!(debug_output.contains("client-123"));
    }

    #[tokio::test]
    async fn client_debug_names_the_host_and_whether_auth_is_enabled() {
        // `lazy_test_client()` needs a tokio runtime just to build its
        // (never-dialed) channel, hence `#[tokio::test]`.
        let client = lazy_test_client();
        let debug_output = format!("{client:?}");
        assert!(
            debug_output.contains("http://127.0.0.1:1"),
            "Debug must name the host the client was built for, got: {debug_output}"
        );
        assert!(
            debug_output.contains("auth_enabled: false"),
            "Debug must report whether auth is enabled, got: {debug_output}"
        );
        // Nothing credential-shaped may leak: the interceptor holds a live
        // bearer token and the token manager its client secret, so neither
        // the service clients nor the token manager may be printed.
        assert!(
            !debug_output.contains("BearerInterceptor"),
            "Debug must not print the interceptor holding the bearer token, got: {debug_output}"
        );
        assert!(
            !debug_output.contains("TokenManager"),
            "Debug must not print the token manager, got: {debug_output}"
        );
    }

    #[tokio::test]
    async fn invalidate_auth_token_is_a_harmless_no_op_when_auth_is_disabled() {
        // `lazy_test_client()` itself needs a tokio runtime just to build
        // its (never-dialed) channel — but `invalidate_auth_token` is
        // called here with no `.await`, which is the point: it is
        // synchronous by design and must never need to wait on a network
        // round trip, unlike `refresh_auth_token`.
        let client = lazy_test_client();
        client.invalidate_auth_token();
    }
}
