//! Shared harness for the integration tests.
//!
//! [`FakeServer`] implements all four services of
//! `proto/upstream/v1/upstream.proto` in memory and serves them over a
//! loopback socket, so a test exercises the real client path end to end —
//! [`RociaDbBuilder`] to [`Channel`] to the bearer interceptor to the
//! generated client to `RociaDbClient::unary` to JSON decoding — with no
//! external service, no `protoc`, and no network beyond `127.0.0.1`.
//!
//! The server stubs come from the second codegen pass in `build.rs`, which
//! writes server-only code into `$OUT_DIR/test_server/`. The library's own
//! generated code stays client-only, because the `tonic` it depends on has no
//! server feature; the `tonic` in `[dev-dependencies]` does.
//!
//! # Fidelity, and its limits
//!
//! The store is a set of `BTreeMap`s behind one `Mutex`, and it reproduces
//! the parts of the server's contract the SDK actually depends on: cursor
//! pagination over a sorted key order, `total_count` on the three document
//! listings, the `(from, label, to)` edge uniqueness rule, idempotent
//! deletes, the 1 MiB upload chunk cap, a 32-byte checksum length check that
//! never looks at the bytes, and the `reason` trailing metadata the real
//! server attaches to every error. It is not a RocksDB or a TiKV: no
//! transactions, no indexes, no garbage collection, and nothing here proves
//! anything about the real server's behaviour — only about the client's.
//!
//! # Test knobs
//!
//! - [`FakeServer::fail_next`] scripts the next *n* calls of one RPC to fail
//!   with a status code (with the right `reason`), which is how the retry,
//!   refresh-and-retry and error-mapping paths are driven.
//! - [`FakeServer::delay`] makes one RPC sleep before answering, for the
//!   deadline tests.
//! - Every call is recorded with the `authorization` metadata it carried and
//!   its full request as JSON ([`FakeServer::calls_for`]), so "the node
//!   binding wrote this node id with that payload and reused the document's
//!   request id" is a direct assertion on what the server received.

#![allow(dead_code)]

pub mod idp;

pub use idp::MockIdp;

/// gRPC server stubs for `rocia.v1`, from the second codegen pass in
/// `build.rs`. `OUT_DIR` is set for every target of a package with a build
/// script, integration tests included.
pub mod pb {
    #![allow(missing_docs, clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/test_server/rocia.v1.rs"));
}

use futures::StreamExt;
use pb::document_service_server::{DocumentService, DocumentServiceServer};
use pb::file_service_server::{FileService, FileServiceServer};
use pb::graph_service_server::{GraphService, GraphServiceServer};
use pb::tenant_service_server::{TenantService, TenantServiceServer};
use rociadb_sdk::{Channel, RociaDbBuilder, RociaDbClient};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tonic::metadata::MetadataMap;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Endpoint, Server};
use tonic::{Code, Request, Response, Status, Streaming};

/// Page size the server applies when a request leaves `limit` unset. Mirrors
/// the real server's default; the SDK always sends an explicit limit, so
/// nothing in the suite depends on it.
const DEFAULT_SERVER_PAGE_SIZE: u32 = 50;

/// Largest `chunk` the upload RPC accepts, exactly as the real server.
const MAX_UPLOAD_CHUNK: usize = 1024 * 1024;

/// Chunk size the download RPC slices a stored file into.
///
/// Deliberately **not** 1 MiB and not a power of two: the server promises
/// nothing about download chunk size (see
/// [`DownloadResponse::chunk`](rociadb_sdk::DownloadResponse)), and a client
/// that quietly assumed the upload chunk size came back unchanged would pass
/// against a server that echoed 1 MiB and fail in production.
const DOWNLOAD_CHUNK: usize = 64 * 1024 + 7;

/// Length of the SHA-256 digest the upload RPC requires — and, like the real
/// server, the only thing it checks about it.
const CHECKSUM_LEN: usize = 32;

/// One recorded call, as the server saw it.
#[derive(Debug, Clone)]
pub struct RecordedCall {
    /// Protobuf method name, for example `"PutDoc"`.
    pub rpc: &'static str,
    /// The `authorization` metadata the call carried, or `None` when it
    /// carried none — which is what `disable_auth()` must produce.
    pub authorization: Option<String>,
    /// The request's `request_id` field, when it has one.
    pub request_id: Option<String>,
    /// The whole request as JSON. Available because both codegen passes
    /// derive `serde::Serialize` on every message, so no per-RPC matcher has
    /// to be written by hand. `bytes` fields arrive as arrays of numbers —
    /// see [`RecordedCall::json_field`]. The `Upload` RPC records its first
    /// message with `chunk` blanked, so a multi-megabyte upload does not turn
    /// into a multi-million-element JSON array.
    pub request: Value,
}

impl RecordedCall {
    /// One field of the recorded request, or `Value::Null` when absent.
    pub fn field(&self, name: &str) -> &Value {
        self.request.get(name).unwrap_or(&Value::Null)
    }

    /// One string field of the recorded request. Panics when the field is
    /// missing or not a string, which in a test is the right outcome.
    pub fn str_field(&self, name: &str) -> &str {
        self.field(name)
            .as_str()
            .unwrap_or_else(|| panic!("{} has no string field {name:?}", self.rpc))
    }

    /// A protobuf `bytes` field decoded back into the JSON payload it
    /// carries — the shape every `json` field on the wire has.
    pub fn json_field(&self, name: &str) -> Value {
        serde_json::from_slice(&self.bytes_field(name))
            .unwrap_or_else(|error| panic!("{}.{name} is not valid JSON: {error}", self.rpc))
    }

    /// A protobuf `bytes` field, back as bytes.
    pub fn bytes_field(&self, name: &str) -> Vec<u8> {
        self.field(name)
            .as_array()
            .unwrap_or_else(|| panic!("{} has no bytes field {name:?}", self.rpc))
            .iter()
            .map(|byte| {
                u8::try_from(byte.as_u64().unwrap_or(u64::MAX))
                    .unwrap_or_else(|_| panic!("{}.{name} is not a byte array", self.rpc))
            })
            .collect()
    }
}

/// One stored edge.
#[derive(Debug, Clone)]
pub struct StoredEdge {
    pub from: String,
    pub to: String,
    pub label: String,
    pub json: Vec<u8>,
}

/// One stored file.
#[derive(Debug, Clone)]
pub struct StoredFile {
    /// The bytes served on download.
    pub bytes: Vec<u8>,
    /// The size `Stat` reports. Normally `bytes.len()`, but
    /// [`FakeServer::drop_stored_byte`] moves the two apart on purpose.
    pub size_bytes: u64,
    pub content_type: String,
    /// The checksum `Stat` reports — whatever the uploader declared, never
    /// verified against `bytes`, exactly like the real server.
    pub checksum: Vec<u8>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Default)]
struct State {
    /// `(tenant, collection) -> id -> json`.
    documents: BTreeMap<(String, String), BTreeMap<String, Vec<u8>>>,
    /// `(tenant, graph) -> node_id -> json`.
    nodes: BTreeMap<(String, String), BTreeMap<String, Vec<u8>>>,
    /// `(tenant, graph) -> edge_id -> edge`.
    edges: BTreeMap<(String, String), BTreeMap<String, StoredEdge>>,
    /// `(tenant, bucket) -> file_id -> file`.
    files: BTreeMap<(String, String), BTreeMap<String, StoredFile>>,
    /// Registered by every write, like the real server's tenant registry.
    tenants: BTreeSet<String>,
    /// Scripted failures, oldest first, per RPC.
    failures: HashMap<String, VecDeque<Code>>,
    /// Per-RPC artificial latency.
    delays: HashMap<String, Duration>,
    /// Every call the server saw, in order.
    calls: Vec<RecordedCall>,
    /// Monotonic clock for `created_at` / `updated_at`, so timestamps are
    /// deterministic instead of wall-clock.
    writes: u64,
}

/// The four service implementations, sharing one store.
#[derive(Clone, Default)]
struct FakeService {
    state: Arc<Mutex<State>>,
}

/// A running in-process gRPC server. Dropping it stops the listener.
pub struct FakeServer {
    address: SocketAddr,
    state: Arc<Mutex<State>>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl FakeServer {
    /// Bind an ephemeral loopback port and serve all four services on it.
    pub async fn start() -> Self {
        let incoming =
            TcpIncoming::bind("127.0.0.1:0".parse().expect("a literal loopback address"))
                .expect("the fake server must bind an ephemeral loopback port");
        let address = incoming
            .local_addr()
            .expect("a bound listener must report its address");
        let service = FakeService::default();
        let state = Arc::clone(&service.state);
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let mut builder = Server::builder();
        let router = builder
            .add_service(DocumentServiceServer::new(service.clone()))
            .add_service(GraphServiceServer::new(service.clone()))
            .add_service(FileServiceServer::new(service.clone()))
            .add_service(TenantServiceServer::new(service));
        let task = tokio::spawn(async move {
            let _ = router
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        Self {
            address,
            state,
            shutdown: Some(shutdown),
            task,
        }
    }

    /// The address the server is listening on.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// The server's address as a `host` for
    /// [`RociaDbBuilder::host`](rociadb_sdk::RociaDbBuilder::host).
    pub fn host(&self) -> String {
        format!("http://{}", self.address)
    }

    /// A channel pointed at the server, for
    /// [`build_with_channel`](rociadb_sdk::RociaDbBuilder::build_with_channel).
    ///
    /// Lazy: no connection is opened until the first RPC, which keeps every
    /// helper here free of a dial that could fail on its own.
    pub fn channel(&self) -> Channel {
        Endpoint::from_shared(self.host())
            .expect("a loopback host must parse as an endpoint")
            .connect_lazy()
    }

    /// A builder pointed at this server with auth disabled.
    pub fn builder(&self) -> RociaDbBuilder {
        RociaDbBuilder::new().host(self.host()).disable_auth()
    }

    /// A builder pointed at this server, authenticating against `idp`.
    pub fn authenticated_builder(&self, idp: &MockIdp) -> RociaDbBuilder {
        RociaDbBuilder::new()
            .host(self.host())
            .auth_client_credentials(
                idp.token_url(),
                "integration-test-client",
                "integration-test-secret",
            )
    }

    /// A client on a caller-supplied channel, with auth disabled — the
    /// default for tests that are not about authentication.
    pub async fn client(&self) -> RociaDbClient {
        self.builder()
            .build_with_channel(self.channel())
            .await
            .expect("building a client on a loopback channel must succeed")
    }

    /// A client that authenticates against `idp`, on a caller-supplied
    /// channel.
    pub async fn authenticated_client(&self, idp: &MockIdp) -> RociaDbClient {
        self.authenticated_builder(idp)
            .build_with_channel(self.channel())
            .await
            .expect("building an authenticated client must succeed")
    }

    /// Every call the server has seen, oldest first.
    pub fn calls(&self) -> Vec<RecordedCall> {
        self.lock().calls.clone()
    }

    /// Every call the server has seen for one RPC, oldest first.
    pub fn calls_for(&self, rpc: &str) -> Vec<RecordedCall> {
        self.lock()
            .calls
            .iter()
            .filter(|call| call.rpc == rpc)
            .cloned()
            .collect()
    }

    /// The single call the server saw for one RPC. Panics unless there was
    /// exactly one, which is usually the assertion worth making.
    pub fn only_call(&self, rpc: &str) -> RecordedCall {
        let mut calls = self.calls_for(rpc);
        assert_eq!(calls.len(), 1, "expected exactly one {rpc} call");
        calls.remove(0)
    }

    /// How many calls the server saw for one RPC.
    pub fn call_count(&self, rpc: &str) -> usize {
        self.lock().calls.iter().filter(|c| c.rpc == rpc).count()
    }

    /// The `authorization` metadata of every call, oldest first.
    pub fn authorizations(&self) -> Vec<Option<String>> {
        self.lock()
            .calls
            .iter()
            .map(|call| call.authorization.clone())
            .collect()
    }

    /// Forget every recorded call, so a later assertion counts only what
    /// happened after this point.
    pub fn clear_calls(&self) {
        self.lock().calls.clear();
    }

    /// Fail the next `times` calls of `rpc` with `code`, carrying the
    /// `reason` trailing metadata the real server would attach.
    pub fn fail_next(&self, rpc: &str, times: usize, code: Code) {
        let mut state = self.lock();
        let queue = state.failures.entry(rpc.to_string()).or_default();
        for _ in 0..times {
            queue.push_back(code);
        }
    }

    /// Sleep `delay` before answering any call of `rpc`.
    pub fn delay(&self, rpc: &str, delay: Duration) {
        self.lock().delays.insert(rpc.to_string(), delay);
    }

    /// Read one stored file back, as the server holds it.
    pub fn stored_file(&self, tenant: &str, bucket: &str, file_id: &str) -> Option<StoredFile> {
        self.lock()
            .files
            .get(&(tenant.to_string(), bucket.to_string()))
            .and_then(|bucket| bucket.get(file_id))
            .cloned()
    }

    /// Flip one bit of a stored file's checksum, leaving the bytes and the
    /// size alone: the file then downloads whole and fails verification.
    pub fn corrupt_stored_checksum(&self, tenant: &str, bucket: &str, file_id: &str) {
        self.mutate_file(tenant, bucket, file_id, |file| {
            let first = file
                .checksum
                .first_mut()
                .expect("a stored file must carry a checksum");
            *first ^= 0b1000_0000;
        });
    }

    /// Flip one bit of a stored file's bytes, leaving the recorded size and
    /// checksum alone: the file downloads at the right length and hashes to
    /// something else.
    pub fn corrupt_stored_bytes(&self, tenant: &str, bucket: &str, file_id: &str) {
        self.mutate_file(tenant, bucket, file_id, |file| {
            let first = file
                .bytes
                .first_mut()
                .expect("a stored file must carry bytes to corrupt");
            *first ^= 0b0000_0001;
        });
    }

    /// Drop the last byte of a stored file while leaving the size `Stat`
    /// reports untouched: the download is one byte short of its metadata.
    pub fn drop_stored_byte(&self, tenant: &str, bucket: &str, file_id: &str) {
        self.mutate_file(tenant, bucket, file_id, |file| {
            file.bytes.pop().expect("a stored file must carry bytes");
        });
    }

    fn mutate_file(
        &self,
        tenant: &str,
        bucket: &str,
        file_id: &str,
        mutate: impl FnOnce(&mut StoredFile),
    ) {
        let mut state = self.lock();
        let file = state
            .files
            .get_mut(&(tenant.to_string(), bucket.to_string()))
            .and_then(|bucket| bucket.get_mut(file_id))
            .unwrap_or_else(|| panic!("no stored file {tenant}/{bucket}/{file_id}"));
        mutate(file);
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Drop for FakeServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.abort();
    }
}

// ---------------------------------------------------------------------------
// Statuses
// ---------------------------------------------------------------------------

/// The `reason` trailing metadata the real server attaches to a status.
///
/// Six of the seven values are the snake_case name of the code;
/// [`Code::Aborted`] is the exception and reads `conflict`. Codes the server
/// never produces itself (`UNAVAILABLE` is the transport's, not the
/// application's) carry no reason, so a test scripting one sees exactly what
/// a real client would.
pub fn reason_for(code: Code) -> Option<&'static str> {
    match code {
        Code::InvalidArgument => Some("invalid_argument"),
        Code::NotFound => Some("not_found"),
        Code::AlreadyExists => Some("already_exists"),
        Code::PermissionDenied => Some("permission_denied"),
        Code::Unauthenticated => Some("unauthenticated"),
        Code::Aborted => Some("conflict"),
        Code::Internal => Some("internal"),
        _ => None,
    }
}

/// Build a status the way the server does: a code, a message, and the
/// matching `reason` trailing metadata.
pub fn status(code: Code, message: impl Into<String>) -> Status {
    let mut status = Status::new(code, message);
    if let Some(reason) = reason_for(code) {
        status.metadata_mut().insert(
            "reason",
            reason.parse().expect("a reason is always valid ASCII"),
        );
    }
    status
}

fn not_found(message: impl Into<String>) -> Status {
    status(Code::NotFound, message)
}

fn already_exists(message: impl Into<String>) -> Status {
    status(Code::AlreadyExists, message)
}

fn invalid_argument(message: impl Into<String>) -> Status {
    status(Code::InvalidArgument, message)
}

// ---------------------------------------------------------------------------
// Shared plumbing
// ---------------------------------------------------------------------------

impl FakeService {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Record a unary call, then apply its scripted delay and failure.
    async fn enter_unary<T: Serialize>(
        &self,
        rpc: &'static str,
        request: &Request<T>,
    ) -> Result<(), Status> {
        let payload = serde_json::to_value(request.get_ref())
            .expect("every generated message derives Serialize");
        self.enter(rpc, request.metadata(), payload).await
    }

    /// Record one call and apply its knobs. Nothing is awaited while the
    /// store is locked.
    async fn enter(
        &self,
        rpc: &'static str,
        metadata: &MetadataMap,
        request: Value,
    ) -> Result<(), Status> {
        let (delay, failure) = {
            let mut state = self.lock();
            let authorization = metadata
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            let request_id = request
                .get("request_id")
                .and_then(Value::as_str)
                .map(str::to_string);
            state.calls.push(RecordedCall {
                rpc,
                authorization,
                request_id,
                request,
            });
            let failure = state
                .failures
                .get_mut(rpc)
                .and_then(std::collections::VecDeque::pop_front);
            let delay = state.delays.get(rpc).copied();
            (delay, failure)
        };
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
        match failure {
            Some(code) => Err(status(code, format!("scripted {rpc} failure"))),
            None => Ok(()),
        }
    }
}

impl State {
    /// Register a tenant, the way every write on the real server does.
    fn register(&mut self, tenant_id: &str) {
        self.tenants.insert(tenant_id.to_string());
    }

    /// A monotonically increasing pseudo-timestamp, so `created_at` and
    /// `updated_at` are deterministic across runs.
    fn tick(&mut self) -> String {
        self.writes += 1;
        format!("1970-01-01T00:00:{:02}Z", self.writes % 60)
    }

    /// Neighbors of `node_id` over `label`, keyed by edge id — which is also
    /// the pagination order, since the edges are held in a `BTreeMap`.
    fn neighbors(
        &self,
        tenant_id: &str,
        graph: &str,
        node_id: &str,
        label: &str,
        outgoing: bool,
    ) -> Vec<(String, pb::Neighbor)> {
        self.edges
            .get(&(tenant_id.to_string(), graph.to_string()))
            .map(|edges| {
                edges
                    .iter()
                    .filter(|(_, edge)| {
                        edge.label == label
                            && if outgoing {
                                edge.from == node_id
                            } else {
                                edge.to == node_id
                            }
                    })
                    .map(|(edge_id, edge)| {
                        (
                            edge_id.clone(),
                            pb::Neighbor {
                                node_id: if outgoing {
                                    edge.to.clone()
                                } else {
                                    edge.from.clone()
                                },
                                edge_id: edge_id.clone(),
                            },
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// Apply a `PageRequest` to entries already in the server's sort order.
///
/// The cursor is the key the previous page ended on, so the next page starts
/// strictly after it; an unknown cursor is `INVALID_ARGUMENT`, as on the real
/// server. `next_cursor` is empty on the last page — the empty-string
/// convention the SDK maps to `None`.
fn paginate<T>(
    entries: Vec<(String, T)>,
    page: Option<pb::PageRequest>,
) -> Result<(Vec<T>, String), Status> {
    let (limit, cursor) = match page {
        Some(page) => (
            page.limit.unwrap_or(DEFAULT_SERVER_PAGE_SIZE),
            page.cursor.clone(),
        ),
        None => (DEFAULT_SERVER_PAGE_SIZE, String::new()),
    };
    if limit == 0 {
        return Err(invalid_argument("page limit must be greater than zero"));
    }
    let start = if cursor.is_empty() {
        0
    } else {
        match entries.iter().position(|(key, _)| *key == cursor) {
            Some(index) => index + 1,
            None => return Err(invalid_argument(format!("unknown page cursor {cursor:?}"))),
        }
    };
    let end = (start + limit as usize).min(entries.len());
    let next_cursor = if end < entries.len() {
        entries[end - 1].0.clone()
    } else {
        String::new()
    };
    let items = entries
        .into_iter()
        .skip(start)
        .take(end.saturating_sub(start))
        .map(|(_, item)| item)
        .collect();
    Ok((items, next_cursor))
}

fn page_response(next_cursor: String) -> Option<pb::PageResponse> {
    Some(pb::PageResponse { next_cursor })
}

/// Decode a stored document so a filter can look at its fields.
fn as_object(json: &[u8]) -> Value {
    serde_json::from_slice(json).unwrap_or(Value::Null)
}

/// Order two JSON values for `QueryDoc`'s sort list: numbers numerically,
/// strings lexicographically, everything else by its rendered form.
fn compare_json(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => left
            .as_f64()
            .unwrap_or(f64::NAN)
            .partial_cmp(&right.as_f64().unwrap_or(f64::NAN))
            .unwrap_or(Ordering::Equal),
        (Value::String(left), Value::String(right)) => left.cmp(right),
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        _ => left.to_string().cmp(&right.to_string()),
    }
}

/// Evaluate one `QueryDoc` filter against a decoded document.
///
/// `Eq` compares the top-level field to the single value; `In` tests
/// membership in the value set; `Contains` is a case-insensitive substring
/// test on a string field, and exact membership when the field is an array —
/// enough for the SDK's tests, and deliberately less than the real server's
/// indexed implementation.
fn matches_filter(document: &Value, filter: &pb::QueryFilter) -> Result<bool, Status> {
    let field = document.get(&filter.field).cloned().unwrap_or(Value::Null);
    let values: Vec<Value> = filter
        .values_json
        .iter()
        .map(|value| serde_json::from_slice(value).unwrap_or(Value::Null))
        .collect();
    let operator = pb::QueryOperator::try_from(filter.operator)
        .map_err(|_| invalid_argument(format!("unknown query operator {}", filter.operator)))?;
    Ok(match operator {
        pb::QueryOperator::Eq => values.first() == Some(&field),
        pb::QueryOperator::In => values.contains(&field),
        pb::QueryOperator::Contains => match (&field, values.first()) {
            (Value::String(haystack), Some(Value::String(needle))) => haystack
                .to_lowercase()
                .contains(needle.to_lowercase().as_str()),
            (Value::Array(items), Some(needle)) => items.contains(needle),
            _ => false,
        },
        pb::QueryOperator::Unspecified => {
            return Err(invalid_argument("query operator must be specified"));
        }
    })
}

// ---------------------------------------------------------------------------
// DocumentService
// ---------------------------------------------------------------------------

#[tonic::async_trait]
impl DocumentService for FakeService {
    async fn put_doc(&self, request: Request<pb::PutDocRequest>) -> Result<Response<()>, Status> {
        self.enter_unary("PutDoc", &request).await?;
        let request = request.into_inner();
        let mut state = self.lock();
        state.register(&request.tenant_id);
        state
            .documents
            .entry((request.tenant_id, request.collection))
            .or_default()
            .insert(request.id, request.json);
        Ok(Response::new(()))
    }

    async fn get_doc(
        &self,
        request: Request<pb::GetDocRequest>,
    ) -> Result<Response<pb::GetDocResponse>, Status> {
        self.enter_unary("GetDoc", &request).await?;
        let request = request.into_inner();
        let state = self.lock();
        let json = state
            .documents
            .get(&(request.tenant_id, request.collection))
            .and_then(|collection| collection.get(&request.id))
            .cloned()
            .ok_or_else(|| not_found(format!("document {} not found", request.id)))?;
        Ok(Response::new(pb::GetDocResponse { json }))
    }

    async fn delete_doc(
        &self,
        request: Request<pb::DeleteDocRequest>,
    ) -> Result<Response<()>, Status> {
        self.enter_unary("DeleteDoc", &request).await?;
        let request = request.into_inner();
        let mut state = self.lock();
        state.register(&request.tenant_id);
        // Idempotent: deleting something absent is not an error.
        if let Some(collection) = state
            .documents
            .get_mut(&(request.tenant_id, request.collection))
        {
            collection.remove(&request.id);
        }
        Ok(Response::new(()))
    }

    async fn find_by_field(
        &self,
        request: Request<pb::FindByFieldRequest>,
    ) -> Result<Response<pb::FindByFieldResponse>, Status> {
        self.enter_unary("FindByField", &request).await?;
        let request = request.into_inner();
        let wanted: Value = serde_json::from_slice(&request.value_json)
            .map_err(|error| invalid_argument(format!("value_json is not JSON: {error}")))?;
        let state = self.lock();
        let matching: Vec<(String, Vec<u8>)> = state
            .documents
            .get(&(request.tenant_id, request.collection))
            .map(|collection| {
                collection
                    .iter()
                    .filter(|(_, json)| as_object(json).get(&request.field) == Some(&wanted))
                    .map(|(id, json)| (id.clone(), json.clone()))
                    .collect()
            })
            .unwrap_or_default();
        let total_count = matching.len() as u64;
        let (json, next_cursor) = paginate(matching, request.page)?;
        Ok(Response::new(pb::FindByFieldResponse {
            json,
            page: page_response(next_cursor),
            total_count,
        }))
    }

    async fn list_doc(
        &self,
        request: Request<pb::ListDocRequest>,
    ) -> Result<Response<pb::ListDocResponse>, Status> {
        self.enter_unary("ListDoc", &request).await?;
        let request = request.into_inner();
        let state = self.lock();
        let documents: Vec<(String, Vec<u8>)> = state
            .documents
            .get(&(request.tenant_id, request.collection))
            .map(|collection| {
                collection
                    .iter()
                    .map(|(id, json)| (id.clone(), json.clone()))
                    .collect()
            })
            .unwrap_or_default();
        let total_count = documents.len() as u64;
        let (json, next_cursor) = paginate(documents, request.page)?;
        Ok(Response::new(pb::ListDocResponse {
            json,
            page: page_response(next_cursor),
            total_count,
        }))
    }

    async fn query_doc(
        &self,
        request: Request<pb::QueryDocRequest>,
    ) -> Result<Response<pb::QueryDocResponse>, Status> {
        self.enter_unary("QueryDoc", &request).await?;
        let request = request.into_inner();
        let state = self.lock();
        let mut matching: Vec<(String, Vec<u8>)> = Vec::new();
        if let Some(collection) = state
            .documents
            .get(&(request.tenant_id.clone(), request.collection.clone()))
        {
            for (id, json) in collection {
                let document = as_object(json);
                let mut keep = true;
                for filter in &request.filters {
                    if !matches_filter(&document, filter)? {
                        keep = false;
                        break;
                    }
                }
                if keep {
                    matching.push((id.clone(), json.clone()));
                }
            }
        }
        // The sort list applies in order and is tie-broken by document id,
        // which `matching` is already in — so a stable sort is all it takes.
        for sort in request.sort.iter().rev() {
            let descending = sort.direction == pb::SortDirection::Desc as i32;
            matching.sort_by(|(_, left), (_, right)| {
                let ordering = compare_json(
                    &as_object(left)
                        .get(&sort.field)
                        .cloned()
                        .unwrap_or(Value::Null),
                    &as_object(right)
                        .get(&sort.field)
                        .cloned()
                        .unwrap_or(Value::Null),
                );
                if descending {
                    ordering.reverse()
                } else {
                    ordering
                }
            });
        }
        let total_count = matching.len() as u64;
        let (json, next_cursor) = paginate(matching, request.page)?;
        Ok(Response::new(pb::QueryDocResponse {
            json,
            page: page_response(next_cursor),
            total_count,
        }))
    }

    async fn list_collections(
        &self,
        request: Request<pb::ListCollectionsRequest>,
    ) -> Result<Response<pb::ListCollectionsResponse>, Status> {
        self.enter_unary("ListCollections", &request).await?;
        let request = request.into_inner();
        let state = self.lock();
        let collections: Vec<(String, pb::CollectionInfo)> = state
            .documents
            .iter()
            .filter(|((tenant, _), documents)| {
                *tenant == request.tenant_id && !documents.is_empty()
            })
            .map(|((_, collection), documents)| {
                (
                    collection.clone(),
                    pb::CollectionInfo {
                        collection: collection.clone(),
                        count: documents.len() as u64,
                    },
                )
            })
            .collect();
        let (collections, next_cursor) = paginate(collections, request.page)?;
        Ok(Response::new(pb::ListCollectionsResponse {
            collections,
            page: page_response(next_cursor),
        }))
    }
}

// ---------------------------------------------------------------------------
// GraphService
// ---------------------------------------------------------------------------

#[tonic::async_trait]
impl GraphService for FakeService {
    async fn put_node(&self, request: Request<pb::PutNodeRequest>) -> Result<Response<()>, Status> {
        self.enter_unary("PutNode", &request).await?;
        let request = request.into_inner();
        let mut state = self.lock();
        state.register(&request.tenant_id);
        state
            .nodes
            .entry((request.tenant_id, request.graph))
            .or_default()
            .insert(request.node_id, request.json);
        Ok(Response::new(()))
    }

    async fn get_node(
        &self,
        request: Request<pb::GetNodeRequest>,
    ) -> Result<Response<pb::GetNodeResponse>, Status> {
        self.enter_unary("GetNode", &request).await?;
        let request = request.into_inner();
        let state = self.lock();
        let json = state
            .nodes
            .get(&(request.tenant_id, request.graph))
            .and_then(|graph| graph.get(&request.node_id))
            .cloned()
            .ok_or_else(|| not_found(format!("node {} not found", request.node_id)))?;
        Ok(Response::new(pb::GetNodeResponse { json }))
    }

    async fn add_edge(&self, request: Request<pb::AddEdgeRequest>) -> Result<Response<()>, Status> {
        self.enter_unary("AddEdge", &request).await?;
        let request = request.into_inner();
        let mut state = self.lock();
        let key = (request.tenant_id.clone(), request.graph.clone());

        // Both endpoints must already exist as nodes.
        let nodes = state.nodes.get(&key);
        for endpoint in [&request.from, &request.to] {
            if !nodes.is_some_and(|nodes| nodes.contains_key(endpoint)) {
                return Err(not_found(format!("node {endpoint} not found")));
            }
        }
        // A (from, label, to) triplet names at most one edge: a *second*
        // edge over a taken triplet is a conflict, while the edge that
        // already holds it may be rewritten.
        if let Some(edges) = state.edges.get(&key) {
            let taken = edges.iter().any(|(edge_id, edge)| {
                *edge_id != request.edge_id
                    && edge.from == request.from
                    && edge.to == request.to
                    && edge.label == request.label
            });
            if taken {
                return Err(already_exists(format!(
                    "an edge already connects {} -{}-> {}",
                    request.from, request.label, request.to
                )));
            }
        }

        state.register(&request.tenant_id);
        state.edges.entry(key).or_default().insert(
            request.edge_id,
            StoredEdge {
                from: request.from,
                to: request.to,
                label: request.label,
                json: request.json,
            },
        );
        Ok(Response::new(()))
    }

    async fn get_edge(
        &self,
        request: Request<pb::GetEdgeRequest>,
    ) -> Result<Response<pb::GetEdgeResponse>, Status> {
        self.enter_unary("GetEdge", &request).await?;
        let request = request.into_inner();
        let state = self.lock();
        let edge = state
            .edges
            .get(&(request.tenant_id, request.graph))
            .and_then(|graph| graph.get(&request.edge_id))
            .cloned()
            .ok_or_else(|| not_found(format!("edge {} not found", request.edge_id)))?;
        Ok(Response::new(pb::GetEdgeResponse {
            from: edge.from,
            to: edge.to,
            label: edge.label,
            json: edge.json,
        }))
    }

    async fn delete_edge(
        &self,
        request: Request<pb::DeleteEdgeRequest>,
    ) -> Result<Response<()>, Status> {
        self.enter_unary("DeleteEdge", &request).await?;
        let request = request.into_inner();
        let mut state = self.lock();
        state.register(&request.tenant_id);
        // Idempotent, like every delete on this API.
        if let Some(graph) = state.edges.get_mut(&(request.tenant_id, request.graph)) {
            graph.remove(&request.edge_id);
        }
        Ok(Response::new(()))
    }

    async fn neighbors_out(
        &self,
        request: Request<pb::NeighborsOutRequest>,
    ) -> Result<Response<pb::NeighborsOutResponse>, Status> {
        self.enter_unary("NeighborsOut", &request).await?;
        let request = request.into_inner();
        let state = self.lock();
        let neighbors = state.neighbors(
            &request.tenant_id,
            &request.graph,
            &request.from,
            &request.label,
            true,
        );
        let (neighbors, next_cursor) = paginate(neighbors, request.page)?;
        Ok(Response::new(pb::NeighborsOutResponse {
            neighbors,
            page: page_response(next_cursor),
        }))
    }

    async fn neighbors_in(
        &self,
        request: Request<pb::NeighborsInRequest>,
    ) -> Result<Response<pb::NeighborsInResponse>, Status> {
        self.enter_unary("NeighborsIn", &request).await?;
        let request = request.into_inner();
        let state = self.lock();
        let neighbors = state.neighbors(
            &request.tenant_id,
            &request.graph,
            &request.to,
            &request.label,
            false,
        );
        let (neighbors, next_cursor) = paginate(neighbors, request.page)?;
        Ok(Response::new(pb::NeighborsInResponse {
            neighbors,
            page: page_response(next_cursor),
        }))
    }

    async fn list_graphs(
        &self,
        request: Request<pb::ListGraphsRequest>,
    ) -> Result<Response<pb::ListGraphsResponse>, Status> {
        self.enter_unary("ListGraphs", &request).await?;
        let request = request.into_inner();
        let state = self.lock();
        let graphs: Vec<(String, String)> = state
            .nodes
            .iter()
            .filter(|((tenant, _), nodes)| *tenant == request.tenant_id && !nodes.is_empty())
            .map(|((_, graph), _)| (graph.clone(), graph.clone()))
            .collect();
        let (graphs, next_cursor) = paginate(graphs, request.page)?;
        Ok(Response::new(pb::ListGraphsResponse {
            graphs,
            page: page_response(next_cursor),
        }))
    }

    async fn list_nodes(
        &self,
        request: Request<pb::ListNodesRequest>,
    ) -> Result<Response<pb::ListNodesResponse>, Status> {
        self.enter_unary("ListNodes", &request).await?;
        let request = request.into_inner();
        let state = self.lock();
        // An unknown graph is an empty page, never NOT_FOUND: a graph has no
        // existence of its own on the real server either.
        let node_ids: Vec<(String, String)> = state
            .nodes
            .get(&(request.tenant_id, request.graph))
            .map(|nodes| nodes.keys().map(|id| (id.clone(), id.clone())).collect())
            .unwrap_or_default();
        let (node_ids, next_cursor) = paginate(node_ids, request.page)?;
        Ok(Response::new(pb::ListNodesResponse {
            node_ids,
            page: page_response(next_cursor),
        }))
    }
}

// ---------------------------------------------------------------------------
// FileService
// ---------------------------------------------------------------------------

#[tonic::async_trait]
impl FileService for FakeService {
    async fn upload(
        &self,
        request: Request<Streaming<pb::UploadRequest>>,
    ) -> Result<Response<()>, Status> {
        let metadata = request.metadata().clone();
        let mut stream = request.into_inner();
        let mut head: Option<pb::UploadRequest> = None;
        let mut bytes: Vec<u8> = Vec::new();
        let mut chunk_sizes: Vec<usize> = Vec::new();

        while let Some(message) = stream.message().await? {
            if message.chunk.len() > MAX_UPLOAD_CHUNK {
                return Err(invalid_argument(format!(
                    "chunk of {} bytes exceeds the {MAX_UPLOAD_CHUNK}-byte limit",
                    message.chunk.len()
                )));
            }
            chunk_sizes.push(message.chunk.len());
            bytes.extend_from_slice(&message.chunk);
            if head.is_none() {
                // Recorded without its chunk: a multi-megabyte payload would
                // otherwise be serialized into a JSON array of that many
                // numbers just to be thrown away.
                head = Some(pb::UploadRequest {
                    chunk: Vec::new(),
                    ..message
                });
            }
        }

        let head =
            head.ok_or_else(|| invalid_argument("upload stream carried no messages at all"))?;
        let mut recorded =
            serde_json::to_value(&head).expect("every generated message derives Serialize");
        recorded["chunk_sizes"] = json!(chunk_sizes);
        self.enter("Upload", &metadata, recorded).await?;

        // The server checks the checksum's *length* and never the digest —
        // the asymmetry `download_file_verified` exists to work around.
        if head.checksum.len() != CHECKSUM_LEN {
            return Err(invalid_argument(format!(
                "checksum must be exactly {CHECKSUM_LEN} bytes, got {}",
                head.checksum.len()
            )));
        }
        if bytes.len() as u64 != head.size_bytes {
            return Err(invalid_argument(format!(
                "received {} bytes but size_bytes declared {}",
                bytes.len(),
                head.size_bytes
            )));
        }

        let mut state = self.lock();
        let now = state.tick();
        state.register(&head.tenant_id);
        let files = state
            .files
            .entry((head.tenant_id.clone(), head.bucket.clone()))
            .or_default();
        let created_at = files
            .get(&head.file_id)
            .map(|existing| existing.created_at.clone())
            .unwrap_or_else(|| now.clone());
        files.insert(
            head.file_id.clone(),
            StoredFile {
                size_bytes: bytes.len() as u64,
                bytes,
                content_type: head.content_type.clone(),
                checksum: head.checksum.clone(),
                created_at,
                updated_at: now,
            },
        );
        Ok(Response::new(()))
    }

    type DownloadStream = futures::stream::BoxStream<'static, Result<pb::DownloadResponse, Status>>;

    async fn download(
        &self,
        request: Request<pb::DownloadRequest>,
    ) -> Result<Response<Self::DownloadStream>, Status> {
        self.enter_unary("Download", &request).await?;
        let request = request.into_inner();
        let file = {
            let state = self.lock();
            state
                .files
                .get(&(request.tenant_id, request.bucket))
                .and_then(|bucket| bucket.get(&request.file_id))
                .cloned()
                .ok_or_else(|| not_found(format!("file {} not found", request.file_id)))?
        };
        let chunks: Vec<Result<pb::DownloadResponse, Status>> = file
            .bytes
            .chunks(DOWNLOAD_CHUNK)
            .map(|chunk| {
                Ok(pb::DownloadResponse {
                    chunk: chunk.to_vec(),
                })
            })
            .collect();
        Ok(Response::new(futures::stream::iter(chunks).boxed()))
    }

    async fn stat(
        &self,
        request: Request<pb::StatRequest>,
    ) -> Result<Response<pb::StatResponse>, Status> {
        self.enter_unary("Stat", &request).await?;
        let request = request.into_inner();
        let state = self.lock();
        let file = state
            .files
            .get(&(request.tenant_id, request.bucket))
            .and_then(|bucket| bucket.get(&request.file_id))
            .ok_or_else(|| not_found(format!("file {} not found", request.file_id)))?;
        Ok(Response::new(pb::StatResponse {
            size_bytes: file.size_bytes,
            content_type: file.content_type.clone(),
            checksum: file.checksum.clone(),
            created_at: file.created_at.clone(),
            updated_at: file.updated_at.clone(),
        }))
    }

    async fn delete(&self, request: Request<pb::DeleteRequest>) -> Result<Response<()>, Status> {
        self.enter_unary("Delete", &request).await?;
        let request = request.into_inner();
        let mut state = self.lock();
        state.register(&request.tenant_id);
        // Idempotent.
        if let Some(bucket) = state.files.get_mut(&(request.tenant_id, request.bucket)) {
            bucket.remove(&request.file_id);
        }
        Ok(Response::new(()))
    }

    async fn list_buckets(
        &self,
        request: Request<pb::ListBucketsRequest>,
    ) -> Result<Response<pb::ListBucketsResponse>, Status> {
        self.enter_unary("ListBuckets", &request).await?;
        let request = request.into_inner();
        let state = self.lock();
        let buckets: Vec<(String, String)> = state
            .files
            .iter()
            .filter(|((tenant, _), files)| *tenant == request.tenant_id && !files.is_empty())
            .map(|((_, bucket), _)| (bucket.clone(), bucket.clone()))
            .collect();
        let (buckets, next_cursor) = paginate(buckets, request.page)?;
        Ok(Response::new(pb::ListBucketsResponse {
            buckets,
            page: page_response(next_cursor),
        }))
    }

    async fn list_files(
        &self,
        request: Request<pb::ListFilesRequest>,
    ) -> Result<Response<pb::ListFilesResponse>, Status> {
        self.enter_unary("ListFiles", &request).await?;
        let request = request.into_inner();
        let state = self.lock();
        let file_ids: Vec<(String, String)> = state
            .files
            .get(&(request.tenant_id, request.bucket))
            .map(|files| files.keys().map(|id| (id.clone(), id.clone())).collect())
            .unwrap_or_default();
        let (file_ids, next_cursor) = paginate(file_ids, request.page)?;
        Ok(Response::new(pb::ListFilesResponse {
            file_ids,
            page: page_response(next_cursor),
        }))
    }
}

// ---------------------------------------------------------------------------
// TenantService
// ---------------------------------------------------------------------------

#[tonic::async_trait]
impl TenantService for FakeService {
    async fn list_tenants(
        &self,
        request: Request<pb::ListTenantsRequest>,
    ) -> Result<Response<pb::ListTenantsResponse>, Status> {
        self.enter_unary("ListTenants", &request).await?;
        let request = request.into_inner();
        let state = self.lock();
        let tenants: Vec<(String, String)> = state
            .tenants
            .iter()
            .map(|tenant| (tenant.clone(), tenant.clone()))
            .collect();
        let (tenant_ids, next_cursor) = paginate(tenants, request.page)?;
        Ok(Response::new(pb::ListTenantsResponse {
            tenant_ids,
            page: page_response(next_cursor),
        }))
    }
}

// ---------------------------------------------------------------------------
// Assertions shared by several test files
// ---------------------------------------------------------------------------

/// The SHA-256 digest of `bytes`, for comparing against what an upload sent
/// or a stat reported.
pub fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// A deterministic pseudo-random byte buffer of `len` bytes.
///
/// Not `vec![0; len]`: a run of identical bytes would hide a chunking bug
/// that reordered or duplicated whole chunks, which is exactly what the
/// upload and download paths could get wrong.
pub fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}
