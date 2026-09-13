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
//! use rociadb_sdk::RociaDbBuilder;
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
//!     .create_document(
//!         "tenant-1",
//!         "products",
//!         "sku-123",
//!         json!({"sku": "sku-123"}),
//!         Some("product".to_string()),
//!         Some("products".to_string()),
//!     )
//!     .await?;
//! # Ok(())
//! # }
//! ```
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
//! [`RociaDbClient`], so the module list is short:
//!
//! - [`auth`] — token acquisition and refresh, and the interceptors that
//!   attach credentials to outgoing calls. Useful when you drive
//!   authentication yourself rather than through the builder.
//! - [`mod@file`] — the option types for uploads ([`FileUploadOptions`],
//!   [`FileStreamUploadOptions`]) and the chunking rules the wire contract
//!   imposes.
//! - [`graph`] — the edge, page and node types graph reads return
//!   ([`Edge`], [`NeighborPage`], [`NeighborNode`]).
//!
//! # Stability
//!
//! The public API follows semantic versioning from 1.0.0 onward, with one
//! documented exception: the internal `pb` module holds code generated from
//! the `.proto` files by prost and tonic, and is not covered by that promise.
//! A routine prost or
//! tonic upgrade can reshape those generated types without this SDK's own API
//! changing. Five of them — [`CollectionInfo`], [`StatResponse`],
//! [`Neighbor`], [`UploadRequest`] and [`DownloadResponse`] — appear in public
//! signatures and are re-exported at the crate root for that reason; depend on
//! the re-exports: the `pb` module itself is private.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod auth;
mod document;
mod error;
pub mod file;
pub mod graph;
pub(crate) mod pb;
mod tenant;

pub use error::{Result, RociaDbError};
pub use file::{FileStreamUploadOptions, FileUploadOptions};
pub use graph::{Edge, NeighborNode, NeighborPage};
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

use crate::error::{AuthResultExt, ConnectionResultExt, JsonResultExt, StatusResultExt};
use crate::pb::upstream::v1::document_service_client::DocumentServiceClient;
use crate::pb::upstream::v1::file_service_client::FileServiceClient;
use crate::pb::upstream::v1::graph_service_client::GraphServiceClient;
use crate::pb::upstream::v1::tenant_service_client::TenantServiceClient;
use crate::pb::upstream::v1::{
    AddEdgeRequest, FindByFieldRequest, GetDocRequest, GetNodeRequest, ListCollectionsRequest,
    ListDocRequest, PageRequest, PageResponse, PutDocRequest, PutNodeRequest, QueryDocRequest,
    QueryFilter, QueryOperator, QuerySort, SortDirection,
};
use auth::{BearerInterceptor, TokenManager, TokenRefreshGuard};
use futures::{TryStreamExt, stream};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tonic::codegen::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

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
        /// published 1.0 crate rather than something to add unilaterally
        /// here.
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

/// Builder for RociaDbClient.
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
#[derive(Clone)]
pub struct RociaDbClient {
    upstream_document: DocumentServiceClient<InterceptedService<Channel, BearerInterceptor>>,
    upstream_graph: GraphServiceClient<InterceptedService<Channel, BearerInterceptor>>,
    upstream_file: FileServiceClient<InterceptedService<Channel, BearerInterceptor>>,
    upstream_tenant: TenantServiceClient<InterceptedService<Channel, BearerInterceptor>>,
    /// `None` when auth is disabled. Used to service
    /// [`RociaDbClient::refresh_auth_token`].
    token_manager: Option<TokenManager>,
    /// Keeps the background token-refresh task alive for as long as this
    /// client (or any of its clones) exists. Never read directly, hence the
    /// leading underscore; it exists purely for its `Drop`.
    _token_refresh_guard: Option<Arc<TokenRefreshGuard>>,
}

/// One page of listed items with the cursor for the next page.
///
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

/// One page of document results, together with the total number of
/// documents matching the request (before pagination). `items` and
/// `next_cursor` follow the same contract as [`Page<T>`].
///
/// The cost of `total_count` is **not** the same across the three methods
/// that produce it, because the server computes it differently for each:
/// - [`RociaDbClient::list_documents`] (`ListDoc`): free — the server keeps
///   a running per-collection counter updated on every write, so reading it
///   costs nothing beyond the listing itself.
/// - [`RociaDbClient::search_documents`] (`FindByField`): a count over the
///   matching field-index entries.
/// - [`RociaDbClient::query_documents`] (`QueryDoc`): expensive — the server
///   only knows the total once it has filtered the *complete* candidate set
///   for the query, so the cost scales with the number of candidates on
///   every single call. Do not call this in a loop expecting a cheap
///   number; fetch it once and cache it if the same query is issued
///   repeatedly.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentPage<T> {
    /// The decoded documents on this page, in the order the server returned
    /// them.
    pub items: Vec<T>,
    /// Cursor to pass back to fetch the page after this one, or `None` when
    /// this is the last page.
    pub next_cursor: Option<String>,
    /// Total number of documents matching the request, before pagination.
    /// It describes the same instant as `items` — both are taken from a
    /// single state of the store — but nothing is promised from one call to
    /// the next. See the type-level documentation for what it costs.
    pub total_count: u64,
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

/// Decode one page of raw per-item JSON payloads into `T`, and map the
/// page's cursor with [`non_empty`]. Shared by
/// [`RociaDbClient::list_documents`], [`RociaDbClient::search_documents`]
/// and [`RociaDbClient::query_documents`] — the three RPCs whose response
/// shape is "a list of JSON blobs plus an optional `PageResponse`" — so
/// the extraction rule is defined, and unit-tested, in exactly one place
/// against a synthetic response instead of being copy-pasted three times
/// and only ever exercised through a live gRPC call.
///
/// Decoding still stops at the first bad item: `collect` short-circuits, so
/// a caller never receives a partial page alongside an error. What changes
/// is the diagnostic — the returned `serde_json::Error`'s message is
/// rewritten to lead with `"item <index>: "`, naming the zero-based
/// position of the offending item within the page, because a page can hold
/// dozens of items and "one of them failed to parse" leaves the caller
/// nothing to act on.
fn decode_document_page<T>(
    json: Vec<Vec<u8>>,
    page: Option<PageResponse>,
) -> std::result::Result<(Vec<T>, Option<String>), serde_json::Error>
where
    T: DeserializeOwned,
{
    let items = json
        .into_iter()
        .enumerate()
        .map(|(index, data)| {
            serde_json::from_slice::<T>(&data).map_err(|error| {
                <serde_json::Error as serde::de::Error>::custom(format!("item {index}: {error}"))
            })
        })
        .collect::<std::result::Result<Vec<T>, serde_json::Error>>()?;
    Ok((items, page.and_then(|page| non_empty(page.next_cursor))))
}

/// Validate that `node_label` and `node_graph` are either both set or both
/// absent, before any network call. Pulled out of
/// [`RociaDbClient::create_document`] as a pure function so the rule is
/// unit-testable without a live client.
fn validate_node_binding(node_label: &Option<String>, node_graph: &Option<String>) -> Result<()> {
    if node_label.is_some() != node_graph.is_some() {
        return Err(RociaDbError::validation(format!(
            "node_label and node_graph must be provided together (got node_label={:?}, node_graph={:?})",
            node_label, node_graph
        )));
    }
    Ok(())
}

/// A document's graph node binding, the named-field alternative to the
/// `node_label`/`node_graph` parameter pair on
/// [`RociaDbClient::create_document`] and
/// [`RociaDbClient::create_document_with_request_id`].
///
/// Those two parameters are adjacent, identically-typed (`Option<String>`)
/// positional arguments in a six- and seven-argument call respectively, and
/// the internal validation this SDK runs on them only checks that both are
/// present or both are absent — never that they weren't swapped. A
/// transposed call therefore compiles, passes that check, and silently
/// writes the graph node under the wrong `node_id`/`graph` pairing, with no
/// error anywhere. Passing a single `Option<NodeBinding>` to
/// [`RociaDbClient::create_document_with_node_binding`] or
/// [`RociaDbClient::create_document_with_node_binding_and_request_id`]
/// instead closes that gap two ways: `label` and `graph` are no longer two
/// of several same-typed arguments sitting next to each other in a long
/// call, and "label without graph" or "graph without label" stops being
/// representable at all — there is exactly one `Option` to be `Some` or
/// `None`, not two that must be kept in sync by hand.
///
/// [`NodeBinding::new`] still takes `label` then `graph` positionally, the
/// same shape [`NodeInput::new`] and [`EdgeInput::new`] already use for
/// their own constructors, so this does not make a `label`/`graph`
/// transposition impossible — it narrows where one can happen down to this
/// one dedicated two-argument call, out from among the other four
/// arguments `create_document` takes alongside it.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeBinding {
    /// Label prefixed to the document id to form the graph node id, as
    /// `"{label}:{document_id}"`.
    pub label: String,
    /// Name of the graph the node is written to.
    pub graph: String,
}

impl NodeBinding {
    /// Bind to `label` within `graph`. See [`NodeBinding`] for why passing
    /// this instead of the equivalent `node_label`/`node_graph` pair guards
    /// against a transposed call.
    pub fn new(label: impl Into<String>, graph: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            graph: graph.into(),
        }
    }
}

/// Decompose a [`NodeBinding`] option into the `(node_label, node_graph)`
/// pair [`RociaDbClient::create_document_with_request_id`] expects. Pulled
/// out as a pure function, the same way [`validate_node_binding`] is pulled
/// out of [`RociaDbClient::create_document`], so this mapping is
/// unit-testable without a live client: `label` and `graph` are the same
/// two `String`s [`validate_node_binding`] guards, just carried as one
/// value instead of two, and a future edit that swapped which field feeds
/// which side of the pair would still compile clean.
fn node_binding_to_pair(node_binding: Option<NodeBinding>) -> (Option<String>, Option<String>) {
    match node_binding {
        Some(NodeBinding { label, graph }) => (Some(label), Some(graph)),
        None => (None, None),
    }
}

/// Supported document query operators exposed by the SDK.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentQueryOperator {
    /// The field equals the single value given.
    Eq,
    /// The field equals any one of the values given.
    In,
    /// The field contains the single value given as a case-insensitive
    /// substring. A term shorter than three characters is not indexable, and
    /// the server refuses a query in which no filter is indexable with
    /// `INVALID_ARGUMENT` rather than serving it from a full scan — pair a
    /// short term with an [`Eq`](Self::Eq) or [`In`](Self::In) filter on
    /// another field.
    Contains,
}

impl DocumentQueryOperator {
    fn as_proto(self) -> i32 {
        match self {
            Self::Eq => QueryOperator::Eq as i32,
            Self::In => QueryOperator::In as i32,
            Self::Contains => QueryOperator::Contains as i32,
        }
    }
}

/// Supported document sort directions exposed by the SDK.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentQuerySortDirection {
    /// Ascending order.
    Asc,
    /// Descending order.
    Desc,
}

impl DocumentQuerySortDirection {
    fn as_proto(self) -> i32 {
        match self {
            Self::Asc => SortDirection::Asc as i32,
            Self::Desc => SortDirection::Desc as i32,
        }
    }
}

/// Filter definition for `QueryDoc`. Every filter of a query is combined
/// with logical AND.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DocumentQueryFilter {
    /// Name of the document field to compare.
    pub field: String,
    /// How `field` is compared against `values`.
    pub operator: DocumentQueryOperator,
    /// Values to compare against, each serialized to JSON before being sent.
    /// How many the server expects depends on `operator`:
    /// [`Eq`](DocumentQueryOperator::Eq) and
    /// [`Contains`](DocumentQueryOperator::Contains) take one,
    /// [`In`](DocumentQueryOperator::In) takes the set to match.
    pub values: Vec<Value>,
}

impl DocumentQueryFilter {
    /// Build a filter on `field`, comparing it against `values` with
    /// `operator`. How many values an operator expects is the server's
    /// contract, not the SDK's: `Eq` takes one, `In` takes the set to match.
    pub fn new(
        field: impl Into<String>,
        operator: DocumentQueryOperator,
        values: Vec<Value>,
    ) -> Self {
        Self {
            field: field.into(),
            operator,
            values,
        }
    }
}

/// Sort definition for `QueryDoc`. A query's sort list is applied in the
/// order given, and results are always tie-broken by document id, so the
/// ordering is total and stable across pages.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DocumentQuerySort {
    /// Name of the document field to sort on.
    pub field: String,
    /// Direction to sort `field` in.
    pub direction: DocumentQuerySortDirection,
}

impl DocumentQuerySort {
    /// Sort on `field` in `direction`.
    pub fn new(field: impl Into<String>, direction: DocumentQuerySortDirection) -> Self {
        Self {
            field: field.into(),
            direction,
        }
    }
}

/// One node to upsert, used by [`RociaDbClient::put_nodes`].
///
/// `node_id` is the **complete** node id (for example `"product:sku-1"`),
/// not a `(label, id)` pair for the SDK to reassemble: `label:id` remains a
/// usage convention, not something the server enforces or the SDK
/// recomposes.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct NodeInput {
    /// Complete id of the node to upsert, for example `"product:sku-1"`.
    pub node_id: String,
    /// Properties to store on the node. The server requires a JSON object
    /// here, never a scalar or an array.
    pub value: Value,
    /// Idempotency key for this item's `PutNode` call. When `None`, one is
    /// generated automatically (`put_node:<uuid>` — the same prefix
    /// [`RociaDbClient::put_node`] uses for a single-item write, so a
    /// `PutNode` call always carries the same default prefix regardless of
    /// which path produced it). Provide it explicitly — and reuse the same
    /// value on a retry — so a batch replayed after a timeout resumes
    /// safely: the server deduplicates on `(tenant, operation, target,
    /// request_id)`, so a repeated `request_id` is recognized as the same
    /// write rather than a new one.
    pub request_id: Option<String>,
}

impl NodeInput {
    /// Upsert `value` at `node_id`, letting the SDK generate the
    /// idempotency key. Chain [`NodeInput::with_request_id`] to supply your
    /// own — which is what makes a retried batch safe to replay.
    pub fn new(node_id: impl Into<String>, value: Value) -> Self {
        Self {
            node_id: node_id.into(),
            value,
            request_id: None,
        }
    }

    /// Set the idempotency key for this item; see [`NodeInput::request_id`].
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }
}

/// One edge to upsert, used by [`RociaDbClient::add_edges`] and
/// [`RociaDbClient::add_edge_with_input`].
///
/// `edge_id` is raw and must not be prefixed with `label`.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeInput {
    /// Id of the edge to upsert. Raw: do not prefix it with `label`.
    pub edge_id: String,
    /// Id of the node the edge starts from. It must already exist, or the
    /// write fails with `NOT_FOUND`.
    pub from: String,
    /// Id of the node the edge points to. It must already exist, or the
    /// write fails with `NOT_FOUND`.
    pub to: String,
    /// Type of relation the edge carries. A `(from, label, to)` triplet names
    /// at most one edge: a second edge over a triplet another edge already
    /// holds fails with `ALREADY_EXISTS`, while reusing the same `edge_id`
    /// over its own triplet replaces the edge's properties.
    pub label: String,
    /// Properties to store on the edge.
    pub value: Value,
    /// Idempotency key for this item's `AddEdge` call. When `None`, one is
    /// generated automatically (a bare UUID, with no prefix). See
    /// [`NodeInput::request_id`] for why reusing it on a retry matters.
    pub request_id: Option<String>,
}

impl EdgeInput {
    /// Upsert an edge `label` carrying `value`, running `from` -> `to`,
    /// letting the SDK generate the idempotency key. Chain
    /// [`EdgeInput::with_request_id`] to supply your own. `edge_id` is raw:
    /// do not prefix it with `label`.
    pub fn new(
        edge_id: impl Into<String>,
        from: impl Into<String>,
        to: impl Into<String>,
        label: impl Into<String>,
        value: Value,
    ) -> Self {
        Self {
            edge_id: edge_id.into(),
            from: from.into(),
            to: to.into(),
            label: label.into(),
            value,
            request_id: None,
        }
    }

    /// Set the idempotency key for this item; see [`EdgeInput::request_id`].
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }
}

/// Build the ordered `PutNodeRequest` batch for [`RociaDbClient::put_nodes`].
/// Pulled out as a pure, network-free function — the same way
/// [`crate::file::chunk_upload_requests`] is for uploads — so the batch's
/// wire shape is unit-testable without a live client: item order is
/// preserved (`nodes` is consumed via `into_iter` in the order given),
/// duplicate `node_id`s are not merged (each `NodeInput` becomes exactly
/// one `PutNodeRequest`), and `request_id` is passed through unchanged or
/// defaulted to `put_node:<uuid>` when absent — the same default prefix
/// [`RociaDbClient::put_node`] uses for a single-item write, so every
/// `PutNode` call defaults consistently regardless of whether it went
/// through the batch or single-item path.
fn build_put_node_requests(
    tenant_id: &str,
    graph_name: &str,
    nodes: Vec<NodeInput>,
) -> Result<Vec<PutNodeRequest>> {
    nodes
        .into_iter()
        .map(|node| {
            let json = serde_json::to_vec(&node.value).encode_context("node json")?;
            Ok(PutNodeRequest {
                tenant_id: tenant_id.to_string(),
                graph: graph_name.to_string(),
                node_id: node.node_id,
                json,
                request_id: node
                    .request_id
                    .unwrap_or_else(|| format!("put_node:{}", Uuid::new_v4())),
            })
        })
        .collect()
}

/// Build the ordered `AddEdgeRequest` batch for [`RociaDbClient::add_edges`].
/// Same rationale and guarantees as [`build_put_node_requests`]: order
/// preserved, duplicate `edge_id`s not merged, `request_id` passed through
/// unchanged or defaulted to a bare UUID (no prefix) when absent.
fn build_add_edge_requests(
    tenant_id: &str,
    graph_name: &str,
    edges: Vec<EdgeInput>,
) -> Result<Vec<AddEdgeRequest>> {
    edges
        .into_iter()
        .map(|edge| {
            let json = serde_json::to_vec(&edge.value).encode_context("edge json")?;
            debug!(
                tenant_id = tenant_id,
                graph = graph_name,
                edge_id = edge.edge_id,
                from = edge.from,
                to = edge.to,
                label = edge.label,
                "prepared graph edge upsert"
            );
            Ok(AddEdgeRequest {
                tenant_id: tenant_id.to_string(),
                graph: graph_name.to_string(),
                edge_id: edge.edge_id,
                from: edge.from,
                to: edge.to,
                label: edge.label,
                json,
                request_id: edge
                    .request_id
                    .unwrap_or_else(|| Uuid::new_v4().to_string()),
            })
        })
        .collect()
}

/// Split an ordered batch into groups sharing the same `key`, preserving
/// each group's internal order (the order `items` were given in). Used by
/// [`RociaDbClient::put_nodes`] and [`RociaDbClient::add_edges`] to let
/// items with different keys (`node_id`/`edge_id`) fan out fully
/// concurrently while items sharing a key are dispatched strictly one after
/// another — see those methods' doc comments for why that matters for a
/// batch carrying a duplicate id. Which group runs before which is
/// unconstrained (a `HashMap` is used, so that order is arbitrary): only
/// the order *within* a group is a guarantee, because only that order is
/// one the caller can actually observe (items with different ids are
/// applied to unrelated server-side state, so nothing distinguishes one
/// interleaving of them from another).
fn group_preserving_order<T>(items: Vec<T>, key: impl Fn(&T) -> &str) -> Vec<Vec<T>> {
    let mut groups: HashMap<String, Vec<T>> = HashMap::new();
    for item in items {
        groups.entry(key(&item).to_string()).or_default().push(item);
    }
    groups.into_values().collect()
}

/// Default idempotency key for the `PutDoc` write issued by
/// [`RociaDbClient::create_document`] when the caller does not use
/// [`RociaDbClient::create_document_with_request_id`] directly. Pulled out
/// as a pure, network-free function — the same reason
/// [`build_put_node_requests`] and [`build_add_edge_requests`] exist — so
/// the exact default prefix (`put_document:{collection}:<uuid>`, matching
/// [`RociaDbClient::put_document`]'s own default) is unit-testable without
/// a live client or a network call.
fn default_document_request_id(collection_name: &str) -> String {
    format!("put_document:{}:{}", collection_name, Uuid::new_v4())
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
    /// Create a builder with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the upstream host (ex: http://127.0.0.1:50051).
    pub fn host(&mut self, host: impl Into<String>) -> &mut Self {
        self.host = Some(host.into());
        self
    }

    /// Configure OAuth2 client credentials for upstream auth.
    pub fn auth_client_credentials(
        &mut self,
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> &mut Self {
        self.auth = BuilderAuthConfig::Enabled {
            token_url: Some(token_url.into()),
            client_id: Some(client_id.into()),
            client_secret: Some(client_secret.into()),
        };
        self
    }

    /// Disable auth headers on outgoing requests.
    pub fn disable_auth(&mut self) -> &mut Self {
        self.auth = BuilderAuthConfig::Disabled;
        self
    }

    /// Set the deadline used while connecting to the upstream host.
    ///
    /// The value is stored as-is here (no validation), the same way
    /// [`RociaDbBuilder::host`] and
    /// [`RociaDbBuilder::auth_client_credentials`] never validate before
    /// [`RociaDbBuilder::build`] — validation (rejecting a zero timeout)
    /// happens there instead. When this is never called, `build()` applies
    /// a 10-second default unconditionally: without any timeout at all,
    /// `.connect().await` could hang forever against a host with slow
    /// DNS/TCP, which is a robustness gap rather than a mere convenience.
    pub fn connect_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Build a client connected to the upstream.
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
        info!(host = %host, "building rocia db client");
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
                info!(
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
        info!(host = %host, "rocia db client ready");
        Ok(RociaDbClient {
            upstream_document,
            upstream_graph,
            upstream_file,
            upstream_tenant,
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
}

impl RociaDbClient {
    /// Create or update a document, and optionally a graph node reference.
    ///
    /// `node_label` and `node_graph` must be provided together: if only one
    /// of them is set, this returns an error before any network call.
    ///
    /// `node_label` and `node_graph` are adjacent, same-typed parameters, so
    /// a call that transposes them compiles and passes that check while
    /// writing the graph node under the wrong `node_id`/`graph` pairing —
    /// see [`NodeBinding`] for the full risk. When that matters more than
    /// the extra type, prefer
    /// [`RociaDbClient::create_document_with_node_binding`], which takes the
    /// pair as one [`NodeBinding`] value instead.
    ///
    /// This call is **not atomic**: the document is written first, and the
    /// graph node binding (when requested) is written second. If the node
    /// write fails, the document is left in place without its node
    /// binding — callers that need both or neither must handle that
    /// themselves (for example by retrying the node write, or by treating
    /// a document without its expected node as needing repair).
    pub async fn create_document(
        &self,
        tenant_id: &str,
        collection_name: &str,
        document_id: &str,
        value: Value,
        node_label: Option<String>,
        node_graph: Option<String>,
    ) -> Result<()> {
        let request_id = default_document_request_id(collection_name);
        self.create_document_with_request_id(
            tenant_id,
            collection_name,
            document_id,
            &value,
            node_label,
            node_graph,
            request_id,
        )
        .await
    }

    /// Same as [`RociaDbClient::create_document`], with a caller-provided
    /// idempotency key for the document write (the `PutDoc` call only — the
    /// graph node binding, when requested, keeps generating its own key,
    /// exactly as it already does in [`RociaDbClient::create_document`]).
    /// Reuse the same `request_id` on a retry so the server recognizes a
    /// repeated write instead of applying it twice.
    ///
    /// Unlike [`RociaDbClient::create_document`], `value` is generic over
    /// any `Serialize` type — consistent with
    /// [`RociaDbClient::put_document_with_request_id`],
    /// [`RociaDbClient::put_node_with_request_id`], and
    /// [`RociaDbClient::add_edge_with_request_id`] — rather than requiring
    /// the caller to pre-serialize into `serde_json::Value` first.
    ///
    /// `node_label` and `node_graph` carry the same transposition risk here
    /// as on [`RociaDbClient::create_document`] — see [`NodeBinding`].
    /// Prefer
    /// [`RociaDbClient::create_document_with_node_binding_and_request_id`]
    /// when that risk matters more than the extra type.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_document_with_request_id<T: Serialize + ?Sized>(
        &self,
        tenant_id: &str,
        collection_name: &str,
        document_id: &str,
        value: &T,
        node_label: Option<String>,
        node_graph: Option<String>,
        request_id: impl Into<String>,
    ) -> Result<()> {
        validate_node_binding(&node_label, &node_graph)?;
        debug!(
            tenant_id = tenant_id,
            collection = collection_name,
            document_id = document_id,
            has_node_binding = node_label.is_some() && node_graph.is_some(),
            "upserting document"
        );
        let json = serde_json::to_vec(value)
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    document_id = document_id,
                    error = %error,
                    "failed to encode document json"
                );
            })
            .encode_context("document json")?;
        let doc = PutDocRequest {
            tenant_id: tenant_id.to_string(),
            collection: collection_name.to_string(),
            id: document_id.to_string(),
            json,
            request_id: request_id.into(),
        };
        let mut upstream_document = self.upstream_document.clone();
        upstream_document
            .put_doc(doc)
            .await
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    document_id = document_id,
                    error = %error,
                    "failed to upsert document"
                );
            })
            .status_context("failed to upsert document")?;
        info!(
            tenant_id = tenant_id,
            collection = collection_name,
            document_id = document_id,
            "document upserted"
        );
        if let (Some(label), Some(graph)) = (node_label, node_graph) {
            debug!(
                tenant_id = tenant_id,
                collection = collection_name,
                document_id = document_id,
                graph = %graph,
                label = %label,
                "upserting graph node binding for document"
            );
            let json = serde_json::to_vec(&json!({
                "collection": collection_name,
                "id": document_id,
            }))
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    document_id = document_id,
                    graph = %graph,
                    label = %label,
                    error = %error,
                    "failed to encode node json"
                );
            })
            .encode_context("node json")?;
            let req = PutNodeRequest {
                tenant_id: tenant_id.to_string(),
                graph: graph.clone(),
                node_id: format!("{}:{}", label, document_id),
                json,
                request_id: format!("put_node:{}", Uuid::new_v4()),
            };
            let mut upstream_graph = self.upstream_graph.clone();
            upstream_graph
                .put_node(req)
                .await
                .inspect_err(|error| {
                    error!(
                        tenant_id = tenant_id,
                        collection = collection_name,
                        document_id = document_id,
                        graph = %graph,
                        label = %label,
                        error = %error,
                        "failed to upsert graph node binding"
                    );
                })
                .status_context("failed to upsert graph node binding")?;
            info!(
                tenant_id = tenant_id,
                collection = collection_name,
                document_id = document_id,
                graph = %graph,
                label = %label,
                "graph node binding upserted"
            );
        }
        Ok(())
    }

    /// Same as [`RociaDbClient::create_document`], but takes the node
    /// binding as a single [`NodeBinding`] instead of the adjacent
    /// `node_label`/`node_graph` parameters — see [`NodeBinding`] for why
    /// that removes the risk of a transposed call silently writing the
    /// graph node under the wrong pairing.
    pub async fn create_document_with_node_binding(
        &self,
        tenant_id: &str,
        collection_name: &str,
        document_id: &str,
        value: Value,
        node_binding: Option<NodeBinding>,
    ) -> Result<()> {
        let request_id = default_document_request_id(collection_name);
        self.create_document_with_node_binding_and_request_id(
            tenant_id,
            collection_name,
            document_id,
            &value,
            node_binding,
            request_id,
        )
        .await
    }

    /// Same as [`RociaDbClient::create_document_with_request_id`], but takes
    /// the node binding as a single [`NodeBinding`] instead of the adjacent
    /// `node_label`/`node_graph` parameters — see [`NodeBinding`] for why
    /// that removes the risk of a transposed call silently writing the
    /// graph node under the wrong pairing.
    pub async fn create_document_with_node_binding_and_request_id<T: Serialize + ?Sized>(
        &self,
        tenant_id: &str,
        collection_name: &str,
        document_id: &str,
        value: &T,
        node_binding: Option<NodeBinding>,
        request_id: impl Into<String>,
    ) -> Result<()> {
        let (node_label, node_graph) = node_binding_to_pair(node_binding);
        self.create_document_with_request_id(
            tenant_id,
            collection_name,
            document_id,
            value,
            node_label,
            node_graph,
            request_id,
        )
        .await
    }

    /// Find documents whose `search_field` equals `value` (`FindByField`).
    ///
    /// `total_count` on the returned [`DocumentPage`] is a count over the
    /// matching field-index entries — see [`DocumentPage`] for how this
    /// compares to [`RociaDbClient::list_documents`] and
    /// [`RociaDbClient::query_documents`].
    pub async fn search_documents<T>(
        &self,
        tenant_id: &str,
        collection_name: &str,
        search_field: &str,
        value: &(impl Serialize + ?Sized),
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<DocumentPage<T>>
    where
        T: DeserializeOwned,
    {
        debug!(
            tenant_id = tenant_id,
            collection = collection_name,
            search_field = search_field,
            limit = limit.unwrap_or(DEFAULT_PAGE_SIZE),
            cursor = cursor.unwrap_or(""),
            "searching documents by field"
        );
        let page = page_request(limit, cursor)?;

        let value_json = serde_json::to_vec(value)
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    search_field = search_field,
                    error = %error,
                    "failed to encode search value"
                );
            })
            .encode_context("search value")?;

        let mut upstream_document = self.upstream_document.clone();
        let result = upstream_document
            .find_by_field(FindByFieldRequest {
                tenant_id: tenant_id.to_string(),
                collection: collection_name.to_string(),
                field: search_field.to_string(),
                value_json,
                page,
            })
            .await
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    search_field = search_field,
                    error = %error,
                    "failed to search documents"
                );
            })
            .status_context("failed to search documents")?
            .into_inner();

        let (resp, next_cursor) = decode_document_page::<T>(result.json, result.page)
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    search_field = search_field,
                    error = %error,
                    "failed to decode search results"
                );
            })
            .decode_context("search results")?;

        debug!(
            tenant_id = tenant_id,
            collection = collection_name,
            search_field = search_field,
            result_count = resp.len(),
            total_count = result.total_count,
            next_cursor = next_cursor.as_deref().unwrap_or(""),
            "document search completed"
        );

        Ok(DocumentPage {
            items: resp,
            next_cursor,
            total_count: result.total_count,
        })
    }

    /// Return one paginated page of every document in `collection_name`
    /// (`ListDoc`).
    ///
    /// `total_count` on the returned [`DocumentPage`] is **free**: the
    /// server keeps a running per-collection counter updated on every
    /// write, so reading it costs nothing beyond the listing itself — see
    /// [`DocumentPage`] for how this compares to
    /// [`RociaDbClient::search_documents`] and
    /// [`RociaDbClient::query_documents`].
    pub async fn list_documents<T>(
        &self,
        tenant_id: &str,
        collection_name: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<DocumentPage<T>>
    where
        T: DeserializeOwned,
    {
        debug!(
            tenant_id = tenant_id,
            collection = collection_name,
            limit = limit.unwrap_or(DEFAULT_PAGE_SIZE),
            cursor = cursor.unwrap_or(""),
            "listing documents"
        );
        let page = page_request(limit, cursor)?;
        let mut upstream_document = self.upstream_document.clone();
        let result = upstream_document
            .list_doc(ListDocRequest {
                tenant_id: tenant_id.to_string(),
                collection: collection_name.to_string(),
                page,
            })
            .await
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    error = %error,
                    "failed to list documents"
                );
            })
            .status_context("failed to list documents")?
            .into_inner();

        let (resp, next_cursor) = decode_document_page::<T>(result.json, result.page)
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    error = %error,
                    "failed to decode listed documents"
                );
            })
            .decode_context("listed documents")?;

        debug!(
            tenant_id = tenant_id,
            collection = collection_name,
            result_count = resp.len(),
            total_count = result.total_count,
            next_cursor = next_cursor.as_deref().unwrap_or(""),
            "document listing completed"
        );

        Ok(DocumentPage {
            items: resp,
            next_cursor,
            total_count: result.total_count,
        })
    }

    /// List the document collections holding at least one document. Each
    /// `CollectionInfo` carries its document count.
    pub async fn list_collections(
        &self,
        tenant_id: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<Page<CollectionInfo>> {
        debug!(
            tenant_id = tenant_id,
            limit = limit.unwrap_or(DEFAULT_PAGE_SIZE),
            cursor = cursor.unwrap_or(""),
            "listing collections"
        );
        let mut upstream_document = self.upstream_document.clone();
        let result = upstream_document
            .list_collections(ListCollectionsRequest {
                tenant_id: tenant_id.to_string(),
                page: page_request(limit, cursor)?,
            })
            .await
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    error = %error,
                    "failed to list collections"
                );
            })
            .status_context("failed to list collections")?
            .into_inner();

        Ok(Page {
            items: result.collections,
            next_cursor: result.page.and_then(|page| non_empty(page.next_cursor)),
        })
    }

    /// Execute a paginated multi-filter document query.
    ///
    /// The underlying server applies filters with logical AND and uses the
    /// provided sort list in order. The returned `next_cursor` is an opaque
    /// server cursor that should be fed back unchanged.
    ///
    /// `total_count` on the returned [`DocumentPage`] is **expensive**: the
    /// server only knows it after filtering the complete candidate set for
    /// the query, so the cost scales with the number of candidates on every
    /// call — never call this in a loop just to get a count; see
    /// [`DocumentPage`] for the full comparison with
    /// [`RociaDbClient::list_documents`] and
    /// [`RociaDbClient::search_documents`].
    pub async fn query_documents<T>(
        &self,
        tenant_id: &str,
        collection_name: &str,
        filters: &[DocumentQueryFilter],
        sort: &[DocumentQuerySort],
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<DocumentPage<T>>
    where
        T: DeserializeOwned,
    {
        debug!(
            tenant_id = tenant_id,
            collection = collection_name,
            filter_count = filters.len(),
            sort_count = sort.len(),
            limit = limit.unwrap_or(DEFAULT_PAGE_SIZE),
            cursor = cursor.unwrap_or(""),
            "querying documents"
        );

        let page = page_request(limit, cursor)?;

        let proto_filters = filters
            .iter()
            .map(|filter| -> Result<QueryFilter> {
                Ok(QueryFilter {
                    field: filter.field.clone(),
                    operator: filter.operator.as_proto(),
                    values_json: filter
                        .values
                        .iter()
                        .map(serde_json::to_vec)
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .inspect_err(|error| {
                            error!(
                                tenant_id = tenant_id,
                                collection = collection_name,
                                field = %filter.field,
                                error = %error,
                                "failed to encode query filter value"
                            );
                        })
                        .encode_context("query filter value")?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let proto_sort = sort
            .iter()
            .map(|sort| QuerySort {
                field: sort.field.clone(),
                direction: sort.direction.as_proto(),
            })
            .collect::<Vec<_>>();

        let mut upstream_document = self.upstream_document.clone();
        let result = upstream_document
            .query_doc(QueryDocRequest {
                tenant_id: tenant_id.to_string(),
                collection: collection_name.to_string(),
                filters: proto_filters,
                sort: proto_sort,
                page,
            })
            .await
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    error = %error,
                    "failed to query documents"
                );
            })
            .status_context("failed to query documents")?
            .into_inner();

        let (resp, next_cursor) = decode_document_page::<T>(result.json, result.page)
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    error = %error,
                    "failed to decode queried documents"
                );
            })
            .decode_context("queried documents")?;

        debug!(
            tenant_id = tenant_id,
            collection = collection_name,
            result_count = resp.len(),
            total_count = result.total_count,
            next_cursor = next_cursor.as_deref().unwrap_or(""),
            "document query completed"
        );

        Ok(DocumentPage {
            items: resp,
            next_cursor,
            total_count: result.total_count,
        })
    }

    /// Fetch a single document by id and decode its JSON payload into `T`
    /// (`GetDoc`).
    ///
    /// Unlike [`search_documents`](Self::search_documents),
    /// [`list_documents`](Self::list_documents) and
    /// [`query_documents`](Self::query_documents), this returns the value
    /// directly rather than a [`DocumentPage`]: there is nothing to paginate
    /// when fetching by id.
    pub async fn get_document<T>(
        &self,
        tenant_id: &str,
        collection_name: &str,
        document_id: &str,
    ) -> Result<T>
    where
        T: DeserializeOwned,
    {
        debug!(
            tenant_id = tenant_id,
            collection = collection_name,
            document_id = document_id,
            "loading document"
        );
        let mut upstream_document = self.upstream_document.clone();
        let result = upstream_document
            .get_doc(GetDocRequest {
                tenant_id: tenant_id.to_string(),
                collection: collection_name.to_string(),
                id: document_id.to_string(),
            })
            .await
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    document_id = document_id,
                    error = %error,
                    "failed to load document"
                );
            })
            .status_context("failed to load document")?
            .into_inner();

        let resp = serde_json::from_slice::<T>(&result.json)
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    collection = collection_name,
                    document_id = document_id,
                    error = %error,
                    "failed to decode document"
                );
            })
            .decode_context("document")?;
        debug!(
            tenant_id = tenant_id,
            collection = collection_name,
            document_id = document_id,
            "document loaded"
        );

        Ok(resp)
    }

    /// Upsert a batch of nodes in a graph with bounded concurrency (at most
    /// 10 `PutNode` calls in flight at once). `nodes` is consumed in the
    /// order the caller provides — duplicate `node_id`s are **not** merged,
    /// both are sent. Items with different `node_id`s run fully
    /// concurrently against each other (up to the 10-in-flight bound), but
    /// items sharing a `node_id` are dispatched strictly one after another,
    /// in the order given: the next `PutNode` call for that id is not sent
    /// until the previous one's response comes back. That makes
    /// last-item-wins genuinely true for a duplicate id — the caller's last
    /// write really is the last one the server applies — rather than a race
    /// between two in-flight calls whose completion order the network, not
    /// the caller, decides.
    ///
    /// **This batch is not atomic and stops at the first error**: on
    /// failure, in-flight requests are cancelled and the error does not say
    /// which items had already succeeded. To resume after a failure, replay
    /// the same `nodes` sequence with the same [`NodeInput::request_id`]
    /// values you used the first time — the server deduplicates on
    /// `(tenant, operation, target, request_id)`, so already-applied writes
    /// are recognized and skipped rather than reapplied, and only the
    /// writes that never landed actually happen.
    pub async fn put_nodes(
        &self,
        tenant_id: &str,
        graph_name: &str,
        nodes: impl IntoIterator<Item = NodeInput>,
    ) -> Result<()> {
        let nodes: Vec<NodeInput> = nodes.into_iter().collect();
        debug!(
            tenant_id = tenant_id,
            graph = graph_name,
            node_count = nodes.len(),
            "upserting graph nodes batch"
        );
        let requests = build_put_node_requests(tenant_id, graph_name, nodes)?;
        let groups = group_preserving_order(requests, |request| request.node_id.as_str());
        stream::iter(groups.into_iter().map(Ok::<_, RociaDbError>))
            .try_for_each_concurrent(CONCURRENT_REQUESTS, |group| {
                let mut upstream = self.upstream_graph.clone();
                async move {
                    for node in group {
                        let node_id = node.node_id.clone();
                        upstream
                            .put_node(node)
                            .await
                            .status_context("failed to upsert node")
                            .map_err(|error| {
                                error!(
                                    graph = graph_name,
                                    node_id = node_id,
                                    error = %error,
                                    "failed to upsert graph node"
                                );
                                error
                            })?;
                    }
                    Ok(())
                }
            })
            .await?;
        info!(
            tenant_id = tenant_id,
            graph = graph_name,
            "graph nodes batch upserted"
        );
        Ok(())
    }

    /// Fetch a node and decode its JSON payload. `node_id` uses the
    /// `label:id` format.
    pub async fn get_node(
        &self,
        tenant_id: &str,
        graph_name: &str,
        node_id: &str,
    ) -> Result<Value> {
        debug!(
            tenant_id = tenant_id,
            graph = graph_name,
            node_id = node_id,
            "loading graph node"
        );
        let mut upstream_graph = self.upstream_graph.clone();
        let resp = upstream_graph
            .get_node(GetNodeRequest {
                tenant_id: tenant_id.to_string(),
                graph: graph_name.to_string(),
                node_id: node_id.to_string(),
            })
            .await
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    graph = graph_name,
                    node_id = node_id,
                    error = %error,
                    "failed to load graph node"
                );
            })
            .status_context("failed to load graph node")?
            .into_inner();
        let value = serde_json::from_slice(&resp.json)
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    graph = graph_name,
                    node_id = node_id,
                    error = %error,
                    "failed to decode node json"
                );
            })
            .decode_context("node json")?;
        debug!(
            tenant_id = tenant_id,
            graph = graph_name,
            node_id = node_id,
            "graph node loaded"
        );
        Ok(value)
    }

    /// Fetch an edge by id, with its properties as a `serde_json::Value`.
    ///
    /// The convenience counterpart of
    /// [`RociaDbClient::get_edge_as`], which decodes into a type of your
    /// choosing; see it for what the read guarantees and what `NOT_FOUND`
    /// means here.
    pub async fn get_edge(
        &self,
        tenant_id: &str,
        graph_name: &str,
        edge_id: &str,
    ) -> Result<Edge<Value>> {
        debug!(
            tenant_id = tenant_id,
            graph = graph_name,
            edge_id = edge_id,
            "loading graph edge"
        );
        let edge = self
            .get_edge_as(tenant_id, graph_name, edge_id)
            .await
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    graph = graph_name,
                    edge_id = edge_id,
                    error = %error,
                    "failed to load graph edge"
                );
            })?;
        debug!(
            tenant_id = tenant_id,
            graph = graph_name,
            edge_id = edge_id,
            "graph edge loaded"
        );
        Ok(edge)
    }

    /// Upsert a batch of edges with bounded concurrency (at most 10
    /// `AddEdge` calls in flight at once). `edges` is consumed in the order
    /// the caller provides — duplicate `edge_id`s are **not** merged, both
    /// are sent. Items with different `edge_id`s run fully concurrently
    /// against each other (up to the 10-in-flight bound), but items sharing
    /// an `edge_id` are dispatched strictly one after another, in the order
    /// given: the next `AddEdge` call for that id is not sent until the
    /// previous one's response comes back. That makes last-item-wins
    /// genuinely true for a duplicate id — the caller's last write really is
    /// the last one the server applies — rather than a race between two
    /// in-flight calls whose completion order the network, not the caller,
    /// decides.
    ///
    /// The server returns `NOT_FOUND` for any edge whose `from` or `to`
    /// node does not already exist in `graph_name`: create both endpoint
    /// nodes before adding an edge between them. It returns
    /// `ALREADY_EXISTS` for any edge that would be a *second* one over a
    /// `(from, label, to)` triplet another edge already holds — see
    /// [`RociaDbClient::add_edge`].
    ///
    /// **This batch is not atomic and stops at the first error**: on
    /// failure, in-flight requests are cancelled and the error does not say
    /// which items had already succeeded. To resume after a failure, replay
    /// the same `edges` sequence with the same [`EdgeInput::request_id`]
    /// values you used the first time — the server deduplicates on
    /// `(tenant, operation, target, request_id)`, so already-applied writes
    /// are recognized and skipped rather than reapplied, and only the
    /// writes that never landed actually happen.
    pub async fn add_edges(
        &self,
        tenant_id: &str,
        graph_name: &str,
        edges: impl IntoIterator<Item = EdgeInput>,
    ) -> Result<()> {
        let edges: Vec<EdgeInput> = edges.into_iter().collect();
        debug!(
            tenant_id = tenant_id,
            graph = graph_name,
            edge_count = edges.len(),
            "upserting graph edges batch"
        );
        let requests = build_add_edge_requests(tenant_id, graph_name, edges)?;
        let groups = group_preserving_order(requests, |request| request.edge_id.as_str());
        stream::iter(groups.into_iter().map(Ok::<_, RociaDbError>))
            .try_for_each_concurrent(CONCURRENT_REQUESTS, |group| {
                let mut upstream = self.upstream_graph.clone();
                async move {
                    for edge in group {
                        let edge_id = edge.edge_id.clone();
                        let from = edge.from.clone();
                        let to = edge.to.clone();
                        let label = edge.label.clone();
                        upstream
                            .add_edge(edge)
                            .await
                            .status_context("failed to add edge")
                            .map_err(|error| {
                                error!(
                                    graph = graph_name,
                                    edge_id = edge_id,
                                    from = from,
                                    to = to,
                                    label = label,
                                    error = %error,
                                    "failed to upsert graph edge"
                                );
                                error
                            })?;
                    }
                    Ok(())
                }
            })
            .await?;
        info!(
            tenant_id = tenant_id,
            graph = graph_name,
            "graph edges batch upserted"
        );

        Ok(())
    }

    /// Delete an edge by id.
    ///
    /// **Idempotent**: deleting an `edge_id` that does not exist succeeds
    /// rather than returning `NOT_FOUND`. See
    /// [`RociaDbClient::delete_edge_with_request_id`] for what that costs a
    /// caller who wanted to be told.
    pub async fn delete_edge(
        &self,
        tenant_id: &str,
        graph_name: &str,
        edge_id: &str,
    ) -> Result<()> {
        debug!(
            tenant_id = tenant_id,
            graph = graph_name,
            edge_id = edge_id,
            "deleting graph edge"
        );
        self.delete_edge_with_request_id(
            tenant_id,
            graph_name,
            edge_id,
            format!("delete_edge:{}", Uuid::new_v4()),
        )
        .await?;
        info!(
            tenant_id = tenant_id,
            graph = graph_name,
            edge_id = edge_id,
            "graph edge deleted"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BearerInterceptor, DEFAULT_CONNECT_TIMEOUT, DocumentPage, DocumentQueryFilter,
        DocumentQueryOperator, DocumentQuerySort, DocumentQuerySortDirection,
        DocumentServiceClient, EdgeInput, FileServiceClient, GraphServiceClient, NodeBinding,
        NodeInput, PageResponse, QueryOperator, RociaDbBuilder, RociaDbClient, SortDirection,
        TenantServiceClient, build_add_edge_requests, build_put_node_requests,
        decode_document_page, default_document_request_id, group_preserving_order,
        node_binding_to_pair, page_request, resolve_connect_timeout, validate_host_path,
        validate_node_binding,
    };
    use crate::{FileStreamUploadOptions, RociaDbError};
    use futures::stream;
    use std::time::Duration;
    use tonic::transport::Endpoint;

    /// A `RociaDbClient` wired to a channel that never actually dials
    /// (`Endpoint::connect_lazy` performs no I/O — it only builds a
    /// connector that would try to connect on the *first real RPC*). Used
    /// to test the client-side gating that must reject a request before
    /// ever reaching the network — if such a test regressed and the
    /// gating ran too late, it would hang or fail against the unreachable
    /// `127.0.0.1:1` host instead of returning promptly.
    fn lazy_test_client() -> RociaDbClient {
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
            token_manager: None,
            _token_refresh_guard: None,
        }
    }

    #[test]
    fn node_binding_accepts_both_absent() {
        validate_node_binding(&None, &None).expect("both absent must be accepted");
    }

    #[test]
    fn node_binding_accepts_both_present() {
        validate_node_binding(&Some("product".to_string()), &Some("products".to_string()))
            .expect("both present must be accepted");
    }

    #[test]
    fn node_binding_rejects_label_without_graph() {
        let error = validate_node_binding(&Some("product".to_string()), &None)
            .expect_err("node_label without node_graph must be rejected");
        assert!(matches!(error, RociaDbError::Validation(_)));
        assert!(error.to_string().contains("must be provided together"));
        assert!(error.to_string().contains("node_label=Some(\"product\")"));
    }

    #[test]
    fn node_binding_rejects_graph_without_label() {
        let error = validate_node_binding(&None, &Some("products".to_string()))
            .expect_err("node_graph without node_label must be rejected");
        assert!(matches!(error, RociaDbError::Validation(_)));
        assert!(error.to_string().contains("must be provided together"));
        assert!(error.to_string().contains("node_graph=Some(\"products\")"));
    }

    #[test]
    fn node_binding_to_pair_keeps_label_and_graph_apart() {
        // Asserting each side of the pair individually (rather than just
        // that the call compiles) is the point: `label` and `graph` are
        // both `String`, so a future edit that swapped which field feeds
        // which side of the returned tuple would still type-check and pass
        // a test that only checked shape, not value.
        let (label, graph) = node_binding_to_pair(Some(NodeBinding::new("product", "catalog")));
        assert_eq!(label.as_deref(), Some("product"));
        assert_eq!(graph.as_deref(), Some("catalog"));
    }

    #[test]
    fn node_binding_to_pair_maps_absent_to_both_absent() {
        assert_eq!(node_binding_to_pair(None), (None, None));
    }

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
    fn document_query_operator_as_proto_maps_every_variant_to_the_generated_constant() {
        // A swapped match arm here would silently send the wrong filter
        // semantics to the server with no client-side error, so each
        // variant is checked against the exact generated constant rather
        // than just checking that `as_proto` returns *something*.
        assert_eq!(
            DocumentQueryOperator::Eq.as_proto(),
            QueryOperator::Eq as i32
        );
        assert_eq!(
            DocumentQueryOperator::In.as_proto(),
            QueryOperator::In as i32
        );
        assert_eq!(
            DocumentQueryOperator::Contains.as_proto(),
            QueryOperator::Contains as i32
        );
    }

    #[test]
    fn document_query_sort_direction_as_proto_maps_every_variant_to_the_generated_constant() {
        assert_eq!(
            DocumentQuerySortDirection::Asc.as_proto(),
            SortDirection::Asc as i32
        );
        assert_eq!(
            DocumentQuerySortDirection::Desc.as_proto(),
            SortDirection::Desc as i32
        );
    }

    #[test]
    fn document_query_filter_and_sort_support_equality_comparison() {
        let filter_a = DocumentQueryFilter::new(
            "sku",
            DocumentQueryOperator::Eq,
            vec![serde_json::json!("sku-1")],
        );
        let filter_b = DocumentQueryFilter::new(
            "sku",
            DocumentQueryOperator::Eq,
            vec![serde_json::json!("sku-1")],
        );
        let filter_c = DocumentQueryFilter::new(
            "sku",
            DocumentQueryOperator::Eq,
            vec![serde_json::json!("sku-2")],
        );
        assert_eq!(filter_a, filter_b);
        assert_ne!(filter_a, filter_c);

        let sort_a = DocumentQuerySort::new("sku", DocumentQuerySortDirection::Asc);
        let sort_b = DocumentQuerySort::new("sku", DocumentQuerySortDirection::Asc);
        let sort_c = DocumentQuerySort::new("sku", DocumentQuerySortDirection::Desc);
        assert_eq!(sort_a, sort_b);
        assert_ne!(sort_a, sort_c);
    }

    // `build_put_node_requests` / `build_add_edge_requests` are the pure,
    // network-free cores of `RociaDbClient::put_nodes` /
    // `RociaDbClient::add_edges` (see their doc comments). These tests lock
    // in the three properties an ordered `Vec<NodeInput>` / `Vec<EdgeInput>`
    // batch input must have: caller order is preserved, duplicate keys are
    // not merged, and each item gets its own idempotency key.

    #[test]
    fn put_node_requests_preserve_caller_order() {
        // A `HashMap`-keyed batch input could not guarantee this —
        // iteration order over a hash map is unspecified, so it could
        // silently reorder `PutNode` calls relative to what the caller
        // wrote.
        let nodes = vec![
            NodeInput {
                node_id: "product:3".to_string(),
                value: serde_json::json!({"n": 3}),
                request_id: None,
            },
            NodeInput {
                node_id: "product:1".to_string(),
                value: serde_json::json!({"n": 1}),
                request_id: None,
            },
            NodeInput {
                node_id: "product:2".to_string(),
                value: serde_json::json!({"n": 2}),
                request_id: None,
            },
        ];
        let requests =
            build_put_node_requests("tenant", "catalog", nodes).expect("build must succeed");
        let ids: Vec<&str> = requests.iter().map(|r| r.node_id.as_str()).collect();
        assert_eq!(ids, vec!["product:3", "product:1", "product:2"]);
    }

    #[test]
    fn put_node_requests_do_not_merge_duplicate_node_ids() {
        let nodes = vec![
            NodeInput {
                node_id: "product:1".to_string(),
                value: serde_json::json!({"n": 1}),
                request_id: None,
            },
            NodeInput {
                node_id: "product:1".to_string(),
                value: serde_json::json!({"n": 2}),
                request_id: None,
            },
        ];
        let requests =
            build_put_node_requests("tenant", "catalog", nodes).expect("build must succeed");
        assert_eq!(
            requests.len(),
            2,
            "a HashMap keyed by node_id would have collapsed this to one request"
        );
        assert_eq!(requests[0].node_id, "product:1");
        assert_eq!(requests[1].node_id, "product:1");
        assert_ne!(
            requests[0].json, requests[1].json,
            "each duplicate keeps its own payload"
        );
    }

    #[test]
    fn put_node_requests_use_node_id_verbatim_with_no_label_recomposition() {
        let nodes = vec![NodeInput {
            node_id: "product:sku-1".to_string(),
            value: serde_json::json!({}),
            request_id: None,
        }];
        let requests =
            build_put_node_requests("tenant", "catalog", nodes).expect("build must succeed");
        assert_eq!(requests[0].node_id, "product:sku-1");
    }

    #[test]
    fn put_node_requests_pass_through_caller_supplied_request_id() {
        let nodes = vec![NodeInput {
            node_id: "product:1".to_string(),
            value: serde_json::json!({}),
            request_id: Some("caller-chosen-id".to_string()),
        }];
        let requests =
            build_put_node_requests("tenant", "catalog", nodes).expect("build must succeed");
        assert_eq!(requests[0].request_id, "caller-chosen-id");
    }

    #[test]
    fn put_node_requests_default_request_id_matches_the_single_item_put_node_prefix() {
        // `put_nodes` (batch) and `put_node` (single-item) both issue
        // `PutNode` calls, so an absent id must default to the exact same
        // prefix on both paths: `put_node:<uuid>`.
        let nodes = vec![
            NodeInput {
                node_id: "product:1".to_string(),
                value: serde_json::json!({}),
                request_id: None,
            },
            NodeInput {
                node_id: "product:2".to_string(),
                value: serde_json::json!({}),
                request_id: None,
            },
        ];
        let requests =
            build_put_node_requests("tenant", "catalog", nodes).expect("build must succeed");
        for request in &requests {
            let uuid_part = request
                .request_id
                .strip_prefix("put_node:")
                .expect("default request_id must use the put_node: prefix");
            uuid::Uuid::parse_str(uuid_part).expect("suffix after the prefix must be a uuid");
        }
        assert_ne!(
            requests[0].request_id, requests[1].request_id,
            "each item without an explicit request_id must get its own generated id"
        );
    }

    #[test]
    fn add_edge_requests_preserve_caller_order() {
        let edges = vec![
            EdgeInput {
                edge_id: "e3".to_string(),
                from: "a".to_string(),
                to: "b".to_string(),
                label: "knows".to_string(),
                value: serde_json::json!({}),
                request_id: None,
            },
            EdgeInput {
                edge_id: "e1".to_string(),
                from: "b".to_string(),
                to: "c".to_string(),
                label: "knows".to_string(),
                value: serde_json::json!({}),
                request_id: None,
            },
            EdgeInput {
                edge_id: "e2".to_string(),
                from: "c".to_string(),
                to: "d".to_string(),
                label: "knows".to_string(),
                value: serde_json::json!({}),
                request_id: None,
            },
        ];
        let requests =
            build_add_edge_requests("tenant", "catalog", edges).expect("build must succeed");
        let ids: Vec<&str> = requests.iter().map(|r| r.edge_id.as_str()).collect();
        assert_eq!(ids, vec!["e3", "e1", "e2"]);
    }

    #[test]
    fn add_edge_requests_do_not_merge_duplicate_edge_ids() {
        let edges = vec![
            EdgeInput {
                edge_id: "e1".to_string(),
                from: "a".to_string(),
                to: "b".to_string(),
                label: "knows".to_string(),
                value: serde_json::json!({"v": 1}),
                request_id: None,
            },
            EdgeInput {
                edge_id: "e1".to_string(),
                from: "a".to_string(),
                to: "b".to_string(),
                label: "knows".to_string(),
                value: serde_json::json!({"v": 2}),
                request_id: None,
            },
        ];
        let requests =
            build_add_edge_requests("tenant", "catalog", edges).expect("build must succeed");
        assert_eq!(
            requests.len(),
            2,
            "a HashMap keyed by edge_id would have collapsed this to one request"
        );
        assert_ne!(
            requests[0].json, requests[1].json,
            "each duplicate keeps its own payload"
        );
    }

    #[test]
    fn add_edge_requests_pass_through_caller_supplied_request_id() {
        let edges = vec![EdgeInput {
            edge_id: "e1".to_string(),
            from: "a".to_string(),
            to: "b".to_string(),
            label: "knows".to_string(),
            value: serde_json::json!({}),
            request_id: Some("caller-chosen-id".to_string()),
        }];
        let requests =
            build_add_edge_requests("tenant", "catalog", edges).expect("build must succeed");
        assert_eq!(requests[0].request_id, "caller-chosen-id");
    }

    #[test]
    fn add_edge_requests_default_request_id_stays_a_bare_uuid() {
        // Unlike nodes, edges default to no prefix at all (a bare
        // `Uuid::new_v4().to_string()`).
        let edges = vec![
            EdgeInput {
                edge_id: "e1".to_string(),
                from: "a".to_string(),
                to: "b".to_string(),
                label: "knows".to_string(),
                value: serde_json::json!({}),
                request_id: None,
            },
            EdgeInput {
                edge_id: "e2".to_string(),
                from: "b".to_string(),
                to: "c".to_string(),
                label: "knows".to_string(),
                value: serde_json::json!({}),
                request_id: None,
            },
        ];
        let requests =
            build_add_edge_requests("tenant", "catalog", edges).expect("build must succeed");
        for request in &requests {
            uuid::Uuid::parse_str(&request.request_id)
                .expect("default request_id must be a bare uuid with no prefix");
        }
        assert_ne!(
            requests[0].request_id, requests[1].request_id,
            "each item without an explicit request_id must get its own generated id"
        );
    }

    #[test]
    fn group_preserving_order_keeps_each_groups_relative_order() {
        // `put_nodes`/`add_edges` rely on this: different keys may come back
        // in any order (backed by a `HashMap`), but the items sharing one
        // key must stay in the exact order they were given in, since that
        // order becomes the order the batch dispatch applies them in.
        let items = vec![("a", 1), ("b", 1), ("a", 2), ("a", 3), ("b", 2)];
        let groups = group_preserving_order(items, |item| item.0);

        let values_for = |key: &str| -> Vec<i32> {
            groups
                .iter()
                .find(|group| group.first().is_some_and(|item| item.0 == key))
                .map(|group| group.iter().map(|item| item.1).collect())
                .unwrap_or_default()
        };
        assert_eq!(values_for("a"), vec![1, 2, 3]);
        assert_eq!(values_for("b"), vec![1, 2]);
        assert_eq!(
            groups.iter().map(Vec::len).sum::<usize>(),
            5,
            "every item must land in exactly one group"
        );
    }

    // `decode_document_page` is the shared, network-free core of
    // `list_documents`, `search_documents` and `query_documents`'s response
    // handling (see its doc comment). Feeding it a synthetic response shape
    // directly exercises the real extraction logic those three RPCs run,
    // rather than only re-checking Rust's own field-access semantics on a
    // hand-built `DocumentPage`.

    #[test]
    fn decode_document_page_decodes_items_and_extracts_the_cursor() {
        let json = vec![
            serde_json::to_vec(&serde_json::json!({"n": 1})).expect("encode must succeed"),
            serde_json::to_vec(&serde_json::json!({"n": 2})).expect("encode must succeed"),
        ];
        let page = Some(PageResponse {
            next_cursor: "cursor-2".to_string(),
        });
        let (items, next_cursor) = decode_document_page::<serde_json::Value>(json, page)
            .expect("well-formed items must decode");
        assert_eq!(
            items,
            vec![serde_json::json!({"n": 1}), serde_json::json!({"n": 2})]
        );
        assert_eq!(next_cursor.as_deref(), Some("cursor-2"));
    }

    #[test]
    fn decode_document_page_maps_the_servers_empty_cursor_convention_to_none() {
        let page = Some(PageResponse {
            next_cursor: String::new(),
        });
        let (items, next_cursor) = decode_document_page::<serde_json::Value>(vec![], page)
            .expect("an empty page must still decode");
        assert!(items.is_empty());
        assert!(
            next_cursor.is_none(),
            "an empty next_cursor means \"no further page\" and must map to None — \
             the same rule list_documents/search_documents/query_documents depend on \
             to know when to stop paginating"
        );
    }

    #[test]
    fn decode_document_page_with_no_page_message_has_no_cursor() {
        let (items, next_cursor) = decode_document_page::<serde_json::Value>(vec![], None)
            .expect("a missing page message must still decode");
        assert!(items.is_empty());
        assert!(next_cursor.is_none());
    }

    #[test]
    fn decode_document_page_names_the_failing_items_position() {
        // `collect` still short-circuits on the first bad item — this only
        // checks that the resulting error names *which* item broke, instead
        // of just saying that something in the page failed to parse.
        let json = vec![
            serde_json::to_vec(&serde_json::json!({"n": 1})).expect("encode must succeed"),
            b"{ not valid json".to_vec(),
            serde_json::to_vec(&serde_json::json!({"n": 3})).expect("encode must succeed"),
        ];
        let error = decode_document_page::<serde_json::Value>(json, None)
            .expect_err("a malformed item must fail to decode");
        assert!(
            error.to_string().contains("item 1"),
            "the error must name the zero-based index of the item that failed, got: {error}"
        );
    }

    #[test]
    fn document_page_exposes_items_next_cursor_and_total_count() {
        let page = DocumentPage {
            items: vec!["a", "b"],
            next_cursor: Some("cursor-2".to_string()),
            total_count: 42,
        };
        assert_eq!(page.items, vec!["a", "b"]);
        assert_eq!(page.next_cursor.as_deref(), Some("cursor-2"));
        assert_eq!(page.total_count, 42);
    }

    #[test]
    fn document_page_has_no_next_cursor_on_the_last_page() {
        let page: DocumentPage<i32> = DocumentPage {
            items: vec![1, 2, 3],
            next_cursor: None,
            total_count: 3,
        };
        assert!(page.next_cursor.is_none());
        assert_eq!(page.items, vec![1, 2, 3]);
        assert_eq!(page.total_count, 3);
    }

    #[test]
    fn document_page_derives_clone_and_equality() {
        let page = DocumentPage {
            items: vec![1],
            next_cursor: None,
            total_count: 1,
        };
        assert_eq!(page.clone(), page);
        let different = DocumentPage {
            items: vec![1],
            next_cursor: None,
            total_count: 2,
        };
        assert_ne!(page, different);
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
    fn builder_connect_timeout_setter_stores_the_value_unvalidated() {
        // Mirrors `RociaDbBuilder::host` / `auth_client_credentials`: the
        // setter never validates, only `build()` does (via
        // `resolve_connect_timeout`, tested above) — so even a nonsensical
        // zero duration must be stored as-is here.
        let mut builder = RociaDbBuilder::new();
        builder.connect_timeout(Duration::ZERO);
        assert_eq!(builder.connect_timeout, Some(Duration::ZERO));

        let mut builder = RociaDbBuilder::new();
        builder.connect_timeout(Duration::from_secs(42));
        assert_eq!(builder.connect_timeout, Some(Duration::from_secs(42)));
    }

    #[tokio::test]
    async fn build_rejects_a_zero_connect_timeout_before_any_network_call() {
        // `validate_host_path` and the connect-timeout check both run
        // before `Endpoint::connect()`, so this must return promptly with
        // `Validation` instead of hanging or failing against the
        // (deliberately unreachable) host.
        let mut builder = RociaDbBuilder::new();
        builder
            .host("http://127.0.0.1:1")
            .connect_timeout(Duration::ZERO);
        // `RociaDbClient` intentionally does not derive `Debug` (it would
        // expose channel/interceptor internals), so `expect_err` cannot be
        // used here — match instead.
        let error = match builder.build().await {
            Ok(_) => panic!("a zero connect timeout must fail build()"),
            Err(error) => error,
        };
        assert!(matches!(error, RociaDbError::Validation(_)));
    }

    #[tokio::test]
    async fn build_rejects_a_host_with_a_leftover_path_before_any_network_call() {
        let mut builder = RociaDbBuilder::new();
        builder.host("http://127.0.0.1:1/v1");
        let error = match builder.build().await {
            Ok(_) => panic!("a host carrying a path must fail build()"),
            Err(error) => error,
        };
        assert!(matches!(error, RociaDbError::Connection { .. }));
    }

    // `BuilderAuthConfig`'s manual `Debug` impl must redact `client_secret`
    // — a derived `Debug` would print it in clear text.
    #[test]
    fn builder_debug_output_redacts_the_client_secret() {
        let mut builder = RociaDbBuilder::new();
        builder.auth_client_credentials(
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

    #[test]
    fn default_document_request_id_uses_the_put_document_prefix_with_a_fresh_uuid_each_time() {
        let first = default_document_request_id("catalog");
        let second = default_document_request_id("catalog");
        let uuid_part = first
            .strip_prefix("put_document:catalog:")
            .expect("default request_id must use the put_document:{collection}: prefix");
        uuid::Uuid::parse_str(uuid_part).expect("suffix after the prefix must be a uuid");
        assert_ne!(
            first, second,
            "each call without an explicit request_id must get its own generated id"
        );
    }

    // `upload_file_chunked`'s pre-flight validation (file size, checksum
    // length) must run — and fail — before the method ever touches the
    // network, so these tests run against a client wired to an unreachable
    // host and must still return promptly.

    #[tokio::test]
    async fn upload_file_chunked_rejects_an_oversized_file_before_any_network_call() {
        let client = lazy_test_client();
        let oversized = 5u64 * 1024 * 1024 * 1024 + 1; // 5 GiB + 1 byte
        let result = client
            .upload_file_chunked(
                "tenant",
                "bucket",
                "file",
                oversized,
                vec![0u8; 32],
                stream::empty::<Vec<u8>>(),
                FileStreamUploadOptions::default(),
            )
            .await;
        let error = result.expect_err("a file over the 5 GiB limit must be rejected");
        assert!(matches!(error, RociaDbError::Validation(_)));
        assert!(error.to_string().contains("5 GiB"));
    }

    #[tokio::test]
    async fn upload_file_chunked_rejects_a_wrong_length_checksum_before_any_network_call() {
        let client = lazy_test_client();
        let result = client
            .upload_file_chunked(
                "tenant",
                "bucket",
                "file",
                0,
                vec![0u8; 10], // must be exactly 32 bytes (sha256)
                stream::empty::<Vec<u8>>(),
                FileStreamUploadOptions::default(),
            )
            .await;
        let error = result.expect_err("a checksum that is not 32 bytes must be rejected");
        assert!(matches!(error, RociaDbError::Validation(_)));
        assert!(error.to_string().contains("32 bytes"));
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
