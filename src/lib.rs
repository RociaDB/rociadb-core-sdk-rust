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
//! # Building
//!
//! No system dependency is required: `cargo build` on a bare Rust toolchain
//! is enough. The build script compiles the `.proto` bundled with this crate
//! using [`protox`](https://docs.rs/protox), a pure-Rust protobuf compiler,
//! so there is no `protoc` binary to install and no `PROTOC` environment
//! variable to set — on a developer machine, in CI, or on docs.rs. The
//! Google well-known types the API imports come from `protox` itself.
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
//! The public API follows semantic versioning, with one documented
//! exception: the internal `pb` module holds code generated from the
//! `.proto` files by prost and tonic, and is not covered by that promise. A
//! routine prost or tonic upgrade can reshape those generated types without
//! this SDK's own API changing. Five of them — [`CollectionInfo`],
//! [`StatResponse`], [`Neighbor`], [`UploadRequest`] and
//! [`DownloadResponse`] — appear in public signatures and are re-exported at
//! the crate root for that reason; depend on the re-exports: the `pb` module
//! itself is private.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod auth;
mod document;
mod error;
mod file;
mod graph;
pub(crate) mod pb;
mod tenant;

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
/// Re-exported so callers do not need `tonic` as a direct dependency just to
/// name the return type of [`RociaDbClient::download_file_stream`]. The same
/// stability caveat applies: a major tonic upgrade can reshape this type
/// without the SDK's own API changing.
pub use tonic::codec::Streaming;

use crate::auth::{BearerInterceptor, TokenManager, TokenRefreshGuard};
use crate::error::{AuthResultExt, ConnectionResultExt, StatusResultExt};
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
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
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

#[derive(Clone)]
enum BuilderAuthConfig {
    Enabled {
        token_url: Option<String>,
        client_id: Option<String>,
        /// Never printed by this crate's own instrumentation (see
        /// `BuilderAuthConfig`'s manual `Debug` impl below), but it is
        /// still held as a plain `String` for as long as this builder (or
        /// the `TokenManagerInner` it is handed to at `build()`) is alive,
        /// so a core dump, an attached debugger, or plaintext left behind
        /// in swapped-out memory could still recover it. Closing that gap
        /// would need a `zeroize`- or `secrecy`-backed secret type, which
        /// is a dependency call for this crate's maintainer to make on a
        /// published crate rather than something to add unilaterally here.
        client_secret: Option<String>,
    },
    Disabled,
}

// Manual `Debug` impl instead of `#[derive(Debug)]`: a derived impl would
// print `client_secret` in clear text, so any `format!("{:?}", ..)` or
// debug-level log of a `RociaDbBuilder` would leak the OAuth2 secret.
impl std::fmt::Debug for BuilderAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Enabled {
                token_url,
                client_id,
                client_secret: _,
            } => f
                .debug_struct("Enabled")
                .field("token_url", token_url)
                .field("client_id", client_id)
                .field("client_secret", &"[redacted]")
                .finish(),
            Self::Disabled => f.write_str("Disabled"),
        }
    }
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
        return Err(RociaDbError::connection(format!(
            "RociaDB host must contain only a hostname and port, got a fragment in {host:?}"
        )));
    }
    let uri: http::Uri = host.parse().connection_context("invalid upstream host")?;
    let path = uri.path();
    if !path.is_empty() && path != "/" {
        return Err(RociaDbError::connection(format!(
            "RociaDB host must contain only a hostname and port, got path {path:?}"
        )));
    }
    if let Some(query) = uri.query() {
        return Err(RociaDbError::connection(format!(
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
        return Err(RociaDbError::validation(
            "connect timeout must be greater than zero",
        ));
    }
    Ok(connect_timeout)
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
    pub fn auth_client_credentials(
        mut self,
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> Self {
        self.auth = BuilderAuthConfig::Enabled {
            token_url: Some(token_url.into()),
            client_id: Some(client_id.into()),
            client_secret: Some(client_secret.into()),
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

    /// Build a client connected to the upstream.
    ///
    /// Takes `&self`, so the same builder can be reused to produce several
    /// clients.
    ///
    /// When auth is enabled, this fetches the first token and starts a
    /// background task that refreshes it before it expires (the IdP's
    /// tokens are short-lived — 600 seconds today) for as long as the
    /// returned `RociaDbClient` or any of its clones is kept alive. Call
    /// [`RociaDbClient::refresh_auth_token`] after an `UNAUTHENTICATED`
    /// error to force an out-of-band refresh.
    pub async fn build(&self) -> Result<RociaDbClient> {
        let host = self
            .host
            .as_ref()
            .ok_or_else(|| RociaDbError::connection("missing upstream host"))?;
        debug!(
            host = %host,
            auth_enabled = !matches!(self.auth, BuilderAuthConfig::Disabled),
            "building rocia db client"
        );
        validate_host_path(host)?;
        let connect_timeout = resolve_connect_timeout(self.connect_timeout)?;
        let endpoint = Endpoint::from_shared(host.clone())
            .connection_context("invalid upstream host")?
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .connection_context("failed to configure TLS")?
            .connect_timeout(connect_timeout);
        let channel = endpoint
            .connect()
            .await
            .connection_context("failed to connect to upstream")?;
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
                        RociaDbError::connection("missing auth token url (set AUTH_TOKEN_URL)")
                    })?;
                let client_id = client_id
                    .clone()
                    .or_else(|| env::var(AUTH_CLIENT_ID_ENV).ok())
                    .ok_or_else(|| {
                        RociaDbError::connection("missing auth client id (set AUTH_CLIENT_ID)")
                    })?;
                let client_secret = client_secret
                    .clone()
                    .or_else(|| env::var(AUTH_CLIENT_SECRET_ENV).ok())
                    .ok_or_else(|| {
                        RociaDbError::connection(
                            "missing auth client secret (set AUTH_CLIENT_SECRET)",
                        )
                    })?;

                // `token_url`/`client_id` are deliberately not logged here:
                // they expose the auth infrastructure (IdP endpoint, OAuth2
                // client identity) in any log pipeline configured at debug
                // level.
                debug!(host = %host, "initializing upstream token manager");
                let token_manager =
                    TokenManager::new(reqwest::Client::new(), token_url, client_id, client_secret)
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
            token_manager,
            _token_refresh_guard: token_refresh_guard,
        })
    }
}

impl RociaDbClient {
    /// Force an immediate refresh of the upstream auth token.
    ///
    /// Call this after an RPC fails with `UNAUTHENTICATED` — the server
    /// treats that status as the signal to renew the token, as opposed to
    /// `PERMISSION_DENIED`, which means the token is valid but lacks the
    /// required scope and retrying after a refresh will not help. A no-op
    /// returning `Ok(())` when the client was built with
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

    /// Issue one unary RPC: wrap `message` in a [`tonic::Request`], hand it
    /// to `call`, and map a non-OK [`tonic::Status`] into
    /// [`RociaDbError::Status`] tagged with `operation`.
    ///
    /// Every unary call in the crate goes through here — including the ones
    /// the batch helpers ([`RociaDbClient::put_nodes`],
    /// [`RociaDbClient::add_edges`]) and the neighbor-node fan-out issue
    /// one per item. Only the two streaming RPCs (`Upload`, `Download`) call
    /// the generated client directly, because neither a per-call deadline
    /// nor a transparent replay applies to a stream the caller is feeding
    /// or draining.
    ///
    /// That single choke point is the point: a per-call deadline and an
    /// automatic refresh-and-retry on `UNAUTHENTICATED` are both properties
    /// of "any unary RPC", and both belong here rather than repeated at
    /// twenty call sites. `Req: Clone` is required for the same reason —
    /// replaying a call means building its request a second time — even
    /// though today's single attempt consumes `message` without cloning it.
    ///
    /// `call` takes the whole `tonic::Request` (not just the message) so
    /// this function stays the only place that touches per-call metadata
    /// and extensions.
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
        let response = call(tonic::Request::new(message))
            .await
            .status_context(operation)?;
        Ok(response.into_inner())
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
            token_manager: None,
            _token_refresh_guard: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_CONNECT_TIMEOUT, RociaDbBuilder, RociaDbClient, WriteOptions, page_request,
        resolve_connect_timeout, validate_host_path,
    };
    use crate::RociaDbError;
    use crate::test_support::lazy_test_client;
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
        assert!(matches!(error, RociaDbError::Connection { .. }));
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
        assert!(matches!(error, RociaDbError::Connection { .. }));
        assert!(
            error.to_string().contains("debug=1"),
            "the error should name the offending query, got: {error}"
        );
    }

    #[test]
    fn host_path_validation_rejects_a_host_carrying_a_leftover_fragment() {
        let error = validate_host_path("http://127.0.0.1:50051#note")
            .expect_err("a host with a fragment must be rejected");
        assert!(matches!(error, RociaDbError::Connection { .. }));
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
        assert!(matches!(error, RociaDbError::Validation(_)));
        assert!(error.to_string().contains("greater than zero"));
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
            .disable_auth();
        assert_eq!(chained.connect_timeout, Some(Duration::from_secs(7)));

        let partial = RociaDbBuilder::new().host("http://example.invalid:50051");
        let finished = partial.disable_auth();
        assert_eq!(
            finished.host.as_deref(),
            Some("http://example.invalid:50051")
        );
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
        // `Validation` instead of hanging or failing against the
        // (deliberately unreachable) host.
        let error = RociaDbBuilder::new()
            .host("http://127.0.0.1:1")
            .connect_timeout(Duration::ZERO)
            .build()
            .await
            .expect_err("a zero connect timeout must fail build()");
        assert!(matches!(error, RociaDbError::Validation(_)));
    }

    #[tokio::test]
    async fn build_rejects_a_host_with_a_leftover_path_before_any_network_call() {
        let error = RociaDbBuilder::new()
            .host("http://127.0.0.1:1/v1")
            .build()
            .await
            .expect_err("a host carrying a path must fail build()");
        assert!(matches!(error, RociaDbError::Connection { .. }));
    }

    // `BuilderAuthConfig`'s manual `Debug` impl must redact `client_secret`
    // — a derived `Debug` would print it in clear text.
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
            debug_output.contains("[redacted]"),
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
