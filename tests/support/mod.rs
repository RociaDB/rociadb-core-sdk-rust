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
//! never looks at the bytes, `created_at` / `updated_at` as RFC 3339 instants
//! that a replacement upload moves forward, the `reason` trailing metadata
//! the real server attaches to every error, and `request_id` deduplication
//! across all seven write RPCs. It is not a RocksDB or a TiKV: no
//! transactions, no indexes, no garbage collection, and nothing here proves
//! anything about the real server's behaviour — only about the client's.
//!
//! Deduplication is keyed on `(request_id, operation, target)`, exactly as the
//! proto and `UploadRequest.request_id`'s generated documentation state it —
//! keying on the id alone would make this fake absorb writes a real server
//! applies, which is the more dangerous direction to be wrong in: a test would
//! pass while the SDK shipped a bug. The marker is written *after* the store
//! write, so a call the server rejected burns no key and a corrected retry under
//! it is applied.
//!
//! **Only the sequential replay is modelled, and the rest is a divergence rather
//! than a silence in the contract.** `docs/errors-and-retries.md` describes more
//! than this implements, and describes it precisely:
//!
//! - A genuinely concurrent duplicate — a second call sharing a `request_id`
//!   with one still in flight — gets **`ABORTED`, not `Ok`**, because the
//!   original has not finished and the server cannot claim success on its
//!   behalf. Here it is **absorbed and answered `Ok`**: every write handler runs
//!   `absorbs_replay` → write → `mark_applied` under one `MutexGuard` with no
//!   `.await` inside it, so the marker is published before the guard is released
//!   and the second caller never sees a gap to race through. Measured, not
//!   assumed: two concurrent `upload_file` calls sharing a key both return `Ok`,
//!   both reach the server, and the store clock ticks once — the second payload
//!   is discarded.
//!
//!   This is the divergence to be most careful of, because it runs the dangerous
//!   way round: the harness reports success for a write that never happened,
//!   where the real server refuses to. A test asserting that the SDK copes with
//!   a concurrent duplicate would pass here and prove nothing about a real
//!   deployment — the `ABORTED` such a caller must actually handle, and which
//!   `RociaDbClient::retry` exists to replay, is never produced.
//! - An in-flight call that is interrupted leaves a **reservation** held for
//!   `gc.request_lease_secs` (300 seconds by default). Every replay before that
//!   lease expires gets `ABORTED`, and the first one after it re-executes the
//!   write. There are no reservations here at all: an interrupted call leaves no
//!   trace and the next replay is simply applied.
//! - Markers never expire here, where the real server drops them after
//!   `gc.request_ttl_secs` (24 hours by default). Nothing in this suite runs long
//!   enough for that one to show.
//!
//! The clock is where it is deliberately *less* real than it could be: a fixed
//! base plus one second per write, so a timestamp does not depend on when the
//! suite runs. The format is a guess, and knowingly so — nothing in this
//! repository states the real server's — which is why the SDK parses those two
//! fields lazily and why [`FakeServer::emit_non_rfc3339_timestamps`] exists.
//!
//! # Test knobs
//!
//! - [`FakeServer::fail_next`] scripts the next *n* calls of one RPC to fail
//!   with a status code (with the right `reason`), which is how the retry,
//!   refresh-and-retry and error-mapping paths are driven.
//! - [`FakeServer::delay`] makes one RPC sleep before answering, for the
//!   deadline tests.
//! - [`FakeServer::accept_only_tokens`] makes every RPC reject a bearer token
//!   outside a named set with `UNAUTHENTICATED`, which is how a test proves
//!   *which* token a call carried — including one that no refresh-and-retry
//!   could rescue, as for the streaming uploads.
//! - [`FakeServer::emit_non_rfc3339_timestamps`] formats file timestamps the way
//!   a server that does not speak RFC 3339 would, for the test that a caller
//!   keeps the raw string when
//!   [`FileTimestamp::system_time`](rociadb_sdk::FileTimestamp::system_time)
//!   cannot read it.
//! - [`FakeServer::fail_download_after_chunks`] fails a `Download` part-way
//!   through, and [`FakeServer::fail_upload_before_reading`] rejects an
//!   `Upload` without reading its request stream. Both exist because
//!   [`FakeServer::fail_next`] can only reject a call *before* it starts: for
//!   the two streaming RPCs that meant a transfer could be refused up front or
//!   drained in full, but never torn mid-flight or answered early. Those were
//!   exactly the interleavings two real client bugs lived in, so the gap is
//!   worth keeping closed.
//! - [`FakeServer::corrupt_stored_bytes`], [`FakeServer::corrupt_stored_checksum`]
//!   and [`FakeServer::drop_stored_byte`] move a stored file and its metadata
//!   apart, which is what the verified downloads are checked against.
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

/// Largest message the fake server will decode, well above tonic's own 4 MiB
/// default so that the harness never becomes the ceiling a test trips over —
/// see where the services are registered for why that matters.
const HARNESS_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// Length of the SHA-256 digest the upload RPC requires — and, like the real
/// server, the only thing it checks about it.
const CHECKSUM_LEN: usize = 32;

/// The instant this server's clock starts at: 2024-01-01T00:00:00Z, as seconds
/// since the Unix epoch.
///
/// A fixed base plus one second per write (see [`State::tick`]) is what makes
/// `created_at` and `updated_at` deterministic across runs while still ordering
/// two writes to the same file. The real server's clock is a real clock; the
/// only thing the suite may rely on is the *shape* of what it sends.
const TIMESTAMP_BASE_UNIX_SECONDS: i64 = 1_704_067_200;

/// Seconds in a day, for the two timestamp formatters.
const SECONDS_PER_DAY: i64 = 86_400;

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

/// One applied write's idempotency marker, keyed exactly as the wire contract
/// describes: the `request_id`, the operation, **and** the target.
///
/// All three, because the scope includes the target — the proto and
/// `UploadRequest.request_id`'s generated documentation both say reusing one key
/// across two documents, edges or files performs both writes. Keying on the id
/// alone would make the harness absorb writes a real server applies, which is
/// the more dangerous direction for a fake to be wrong in: a test would pass
/// while the SDK shipped a bug.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RequestMarker {
    request_id: String,
    operation: &'static str,
    target: (String, String, String),
}

/// The marker for one write, or `None` when the call carried no `request_id`.
///
/// An empty key is not a key, and the reason is the *absence* of field presence
/// rather than its presence: `request_id` is a plain `string` on all seven write
/// messages — the only `optional` field in the whole schema is
/// `PageRequest.limit`, made so deliberately to regain presence — so `""` and
/// "unset" are indistinguishable on the wire and nothing could tell them apart
/// here either. A caller building its own `UploadRequest` for
/// `upload_file_stream` therefore opts out of deduplication simply by leaving the
/// field alone.
fn request_marker(
    request_id: &str,
    operation: &'static str,
    target: (&str, &str, &str),
) -> Option<RequestMarker> {
    if request_id.is_empty() {
        return None;
    }
    Some(RequestMarker {
        request_id: request_id.to_string(),
        operation,
        target: (
            target.0.to_string(),
            target.1.to_string(),
            target.2.to_string(),
        ),
    })
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
    /// Idempotency markers for the writes that have actually landed, so a
    /// sequential replay of one is absorbed instead of applied twice.
    ///
    /// Written *after* the store write, never before, because the contract puts
    /// registration after it too: a call rejected before storage is reached — a
    /// scripted failure, an `AddEdge` to a missing node — leaves no marker, so a
    /// corrected retry with the same key is applied rather than absorbed.
    applied_requests: BTreeSet<RequestMarker>,
    /// Scripted failures, oldest first, per RPC.
    failures: HashMap<String, VecDeque<Code>>,
    /// Bearer tokens the server accepts, or `None` for "any, including none at
    /// all" — the default, which is what every test that is not about
    /// authentication wants. See [`FakeServer::accept_only_tokens`].
    accepted_tokens: Option<BTreeSet<String>>,
    /// Per-RPC artificial latency.
    delays: HashMap<String, Duration>,
    /// Every call the server saw, in order.
    calls: Vec<RecordedCall>,
    /// Monotonic clock for `created_at` / `updated_at`, so timestamps are
    /// deterministic instead of wall-clock: one second per write from
    /// [`TIMESTAMP_BASE_UNIX_SECONDS`].
    writes: u64,
    /// Set by [`FakeServer::fail_download_after_chunks`]: how many chunks a
    /// `Download` hands over before the stream fails, and with what code.
    ///
    /// Scripted here rather than through `failures` because that map only
    /// rejects a call *before* it starts, which for a server-streaming RPC is
    /// the opening call. Nothing could express a transfer that begins
    /// successfully and then dies — and that is precisely the shape a caller
    /// of `download_file_verified_to` has to survive, since it has already
    /// written those chunks to the caller's writer by then.
    download_chunk_failure: Option<(usize, Code)>,
    /// Set by [`FakeServer::fail_upload_before_reading`]: reject `Upload`
    /// without consuming its request stream at all.
    ///
    /// The ordinary `failures` path cannot do this either: the handler drains
    /// the whole stream before it consults it, so every scripted upload failure
    /// arrives only after the client has finished sending. A real server is
    /// free to answer on the first message, which is what makes a client's
    /// send-side error handling reachable.
    upload_reject_before_reading: Option<Code>,
    /// Set by [`FakeServer::emit_non_rfc3339_timestamps`], after which
    /// [`State::tick`] formats every timestamp without its offset. Named for
    /// the departure rather than for the norm so that the default — a server
    /// that does speak RFC 3339 — is the `bool`'s own `false`.
    non_rfc3339_timestamps: bool,
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
        // The server decodes up to `HARNESS_MAX_MESSAGE_BYTES` rather than
        // tonic's own 4 MiB default, so the harness is never the thing that caps
        // a test. A test about the *client's* decode ceiling has to be able to
        // put a message larger than that ceiling into the store first, and with
        // the default here it could not: the server would refuse the write
        // before the client's own limit ever came into play.
        let router = builder
            .add_service(
                DocumentServiceServer::new(service.clone())
                    .max_decoding_message_size(HARNESS_MAX_MESSAGE_BYTES),
            )
            .add_service(
                GraphServiceServer::new(service.clone())
                    .max_decoding_message_size(HARNESS_MAX_MESSAGE_BYTES),
            )
            .add_service(
                FileServiceServer::new(service.clone())
                    .max_decoding_message_size(HARNESS_MAX_MESSAGE_BYTES),
            )
            .add_service(
                TenantServiceServer::new(service)
                    .max_decoding_message_size(HARNESS_MAX_MESSAGE_BYTES),
            );
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

    /// From now on, answer any call whose bearer token is not one of `tokens`
    /// with `UNAUTHENTICATED` (and the `reason: "unauthenticated"` the real
    /// server attaches), on every RPC including the two streaming ones.
    ///
    /// `tokens` are the raw token values the mock identity provider issues
    /// (`"token-2"`), not whole header values: the `"Bearer "` prefix the SDK
    /// builds is added here.
    ///
    /// This is how a test pins down *which* token a call carried without
    /// reading the recorded header afterwards — and, more to the point, the
    /// only way to prove that a call which gets **no** automatic retry (a
    /// streaming upload) nonetheless went out with a token the server would
    /// accept. A call carrying no `authorization` at all is rejected too, so a
    /// `disable_auth()` client must not be pointed at a server configured this
    /// way.
    pub fn accept_only_tokens<I, T>(&self, tokens: I)
    where
        I: IntoIterator<Item = T>,
        T: AsRef<str>,
    {
        let accepted = tokens
            .into_iter()
            .map(|token| format!("Bearer {}", token.as_ref()))
            .collect();
        self.lock().accepted_tokens = Some(accepted);
    }

    /// From now on, format `created_at` and `updated_at` the way a server that
    /// does not speak RFC 3339 would: `YYYY-MM-DD hh:mm:ss`, with the time zone
    /// left to the reader's imagination.
    ///
    /// Not a straw man — dropping the offset is what a server rendering a
    /// database timestamp with its default `to_string` does, and it is the one
    /// format most likely to be mistaken for RFC 3339 while not being it. The
    /// point of the knob is that nothing in `proto/` says which format the real
    /// server uses, so the SDK must keep the string readable whatever it is:
    /// with this set, a caller's
    /// [`FileTimestamp::system_time`](rociadb_sdk::FileTimestamp::system_time)
    /// fails while
    /// [`FileTimestamp::as_str`](rociadb_sdk::FileTimestamp::as_str) still
    /// hands back exactly what was sent.
    ///
    /// Affects writes made after this call; the clock itself is unchanged, so
    /// the timestamps stay deterministic and ordered either way.
    pub fn emit_non_rfc3339_timestamps(&self) {
        self.lock().non_rfc3339_timestamps = true;
    }

    /// Make **the next** `Download` hand over `chunks` chunks and then fail the
    /// stream with `code`.
    ///
    /// One-shot, like [`FakeServer::fail_upload_before_reading`] and unlike
    /// [`FakeServer::fail_next`], which takes a count: the script is consumed by
    /// the first `Download` that reaches it, so a test wanting two torn
    /// transfers has to arm it twice.
    ///
    /// The one failure shape `fail_next` cannot script: it rejects a call
    /// before it starts, which for a server-streaming RPC means the opening
    /// call, so a transfer that begins successfully and then dies had no way to
    /// be tested. That is the case the verifying downloads have to survive —
    /// `download_file_verified_to` has already handed those chunks to the
    /// caller's writer by the time the stream breaks.
    ///
    /// `chunks` of 0 fails before any chunk, which is still distinct from a
    /// rejected opening call: the stream opened.
    pub fn fail_download_after_chunks(&self, chunks: usize, code: Code) {
        self.lock().download_chunk_failure = Some((chunks, code));
    }

    /// Reject the next `Upload` with `code` without reading its request stream.
    ///
    /// Every other scripted upload failure arrives only after the handler has
    /// drained the whole stream, so a client's send side never sees a server
    /// that answered early — which a real one is free to do from the first
    /// message. This is what makes that path reachable.
    pub fn fail_upload_before_reading(&self, code: Code) {
        self.lock().upload_reject_before_reading = Some(code);
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
            let rejected_token = state.accepted_tokens.as_ref().is_some_and(|accepted| {
                !authorization
                    .as_deref()
                    .is_some_and(|header| accepted.contains(header))
            });
            state.calls.push(RecordedCall {
                rpc,
                authorization,
                request_id,
                request,
            });
            if rejected_token {
                // Recorded (so the test can see which token was offered) but
                // rejected before anything else applies: an unacceptable
                // credential must not consume a scripted failure meant for the
                // call that gets through, nor wait out its delay.
                return Err(status(
                    Code::Unauthenticated,
                    format!("{rpc} rejected: the bearer token is not accepted"),
                ));
            }
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
    /// `true` when this exact `(request_id, operation, target)` has already been
    /// applied, so the server absorbs the call and answers `Ok` without
    /// touching the store.
    fn absorbs_replay(&self, marker: &Option<RequestMarker>) -> bool {
        marker
            .as_ref()
            .is_some_and(|marker| self.applied_requests.contains(marker))
    }

    /// Record that a write landed, so a later replay of it is absorbed.
    fn mark_applied(&mut self, marker: Option<RequestMarker>) {
        if let Some(marker) = marker {
            self.applied_requests.insert(marker);
        }
    }

    /// Register a tenant, the way every write on the real server does.
    fn register(&mut self, tenant_id: &str) {
        self.tenants.insert(tenant_id.to_string());
    }

    /// The next timestamp this server stamps a write with: one second later
    /// than the last, counting from [`TIMESTAMP_BASE_UNIX_SECONDS`].
    ///
    /// Deterministic rather than wall-clock — the suite must not depend on when
    /// it runs — and monotonic, so a replacement upload's `updated_at` is
    /// genuinely later than the `created_at` it leaves alone. Formatted as
    /// RFC 3339 in UTC, which is what a caller's
    /// [`FileTimestamp::system_time`](rociadb_sdk::FileTimestamp::system_time)
    /// can read, unless [`FakeServer::emit_non_rfc3339_timestamps`] has asked
    /// for the offset to be dropped.
    fn tick(&mut self) -> String {
        self.writes += 1;
        let writes =
            i64::try_from(self.writes).expect("no test performs 2^63 writes against one server");
        let unix_seconds = TIMESTAMP_BASE_UNIX_SECONDS + writes;
        if self.non_rfc3339_timestamps {
            format_local_date_time(unix_seconds)
        } else {
            format_rfc3339(unix_seconds)
        }
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
        let marker = request_marker(
            &request.request_id,
            "PutDoc",
            (&request.tenant_id, &request.collection, &request.id),
        );
        let mut state = self.lock();
        if state.absorbs_replay(&marker) {
            return Ok(Response::new(()));
        }
        state.register(&request.tenant_id);
        state
            .documents
            .entry((request.tenant_id, request.collection))
            .or_default()
            .insert(request.id, request.json);
        state.mark_applied(marker);
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
        let marker = request_marker(
            &request.request_id,
            "DeleteDoc",
            (&request.tenant_id, &request.collection, &request.id),
        );
        let mut state = self.lock();
        if state.absorbs_replay(&marker) {
            return Ok(Response::new(()));
        }
        state.register(&request.tenant_id);
        // Idempotent: deleting something absent is not an error.
        if let Some(collection) = state
            .documents
            .get_mut(&(request.tenant_id, request.collection))
        {
            collection.remove(&request.id);
        }
        state.mark_applied(marker);
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
        let marker = request_marker(
            &request.request_id,
            "PutNode",
            (&request.tenant_id, &request.graph, &request.node_id),
        );
        let mut state = self.lock();
        if state.absorbs_replay(&marker) {
            return Ok(Response::new(()));
        }
        state.register(&request.tenant_id);
        state
            .nodes
            .entry((request.tenant_id, request.graph))
            .or_default()
            .insert(request.node_id, request.json);
        state.mark_applied(marker);
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
        let marker = request_marker(
            &request.request_id,
            "AddEdge",
            (&request.tenant_id, &request.graph, &request.edge_id),
        );
        let mut state = self.lock();
        if state.absorbs_replay(&marker) {
            return Ok(Response::new(()));
        }
        let key = (request.tenant_id.clone(), request.graph.clone());

        // Both endpoints must already exist as nodes. A rejection here leaves no
        // marker, so a retry that first creates the node is applied.
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
        state.mark_applied(marker);
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
        let marker = request_marker(
            &request.request_id,
            "DeleteEdge",
            (&request.tenant_id, &request.graph, &request.edge_id),
        );
        let mut state = self.lock();
        if state.absorbs_replay(&marker) {
            return Ok(Response::new(()));
        }
        state.register(&request.tenant_id);
        // Idempotent, like every delete on this API.
        if let Some(graph) = state.edges.get_mut(&(request.tenant_id, request.graph)) {
            graph.remove(&request.edge_id);
        }
        state.mark_applied(marker);
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
        // Before `into_inner`, and before a single message is read: a real
        // server may answer off the first message rather than at the end, and
        // the client's send side behaves differently when it does. Dropping
        // `request` here closes the receive side, which is exactly what the
        // client then observes.
        if let Some(code) = self.lock().upload_reject_before_reading.take() {
            return Err(Status::new(
                code,
                "upload rejected before reading the stream",
            ));
        }
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

        let marker = request_marker(
            &head.request_id,
            "Upload",
            (&head.tenant_id, &head.bucket, &head.file_id),
        );

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

        // Absorbed *after* the checksum and size checks, and before `tick()`.
        //
        // After, so that a replay whose metadata disagrees with its bytes is
        // rejected rather than waved through: `upload_file` rebuilds and re-sends
        // its whole request to replay an `UNAUTHENTICATED`, so a bug that
        // corrupted the rebuilt metadata would be invisible if this answered `Ok`
        // on the key alone. The contract says a matching replay is absorbed; it
        // does not say validation is skipped, and catching client bugs is what
        // this fake is for.
        //
        // Before `tick()`, because an absorbed replay performs no write and must
        // not advance the store's clock.
        let mut state = self.lock();
        if state.absorbs_replay(&marker) {
            return Ok(Response::new(()));
        }
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
        state.mark_applied(marker);
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
        let mut chunks: Vec<Result<pb::DownloadResponse, Status>> = file
            .bytes
            .chunks(DOWNLOAD_CHUNK)
            .map(|chunk| {
                Ok(pb::DownloadResponse {
                    chunk: chunk.to_vec(),
                })
            })
            .collect();
        // A stream that opened successfully and then dies part-way through.
        // Truncating first is what makes the count mean "chunks the caller
        // actually received": `chunks` beyond the file's own length simply
        // means the whole file arrives and then the stream errors.
        if let Some((deliver, code)) = self.lock().download_chunk_failure.take() {
            chunks.truncate(deliver);
            chunks.push(Err(Status::new(
                code,
                format!("download failed after {deliver} chunks"),
            )));
        }
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
        let marker = request_marker(
            &request.request_id,
            "Delete",
            (&request.tenant_id, &request.bucket, &request.file_id),
        );
        let mut state = self.lock();
        if state.absorbs_replay(&marker) {
            return Ok(Response::new(()));
        }
        state.register(&request.tenant_id);
        // Idempotent.
        if let Some(bucket) = state.files.get_mut(&(request.tenant_id, request.bucket)) {
            bucket.remove(&request.file_id);
        }
        state.mark_applied(marker);
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

/// Format `unix_seconds` as an RFC 3339 timestamp in UTC
/// (`YYYY-MM-DDThh:mm:ssZ`), the shape the real server's `created_at` and
/// `updated_at` are assumed to have.
///
/// Written here rather than reached for from a date-time crate: the fake server
/// must produce a timestamp the SDK's own parser has never seen, and building it
/// from the same crate the parser avoided would only prove the two agree.
/// Second resolution is enough — the SDK's parser treats the fraction as
/// optional, and the suite asserts on the instant, not on the digits.
fn format_rfc3339(unix_seconds: i64) -> String {
    let (year, month, day, hour, minute, second) = civil_from_unix(unix_seconds);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// The same instant as [`format_rfc3339`], without the `Z` that makes it an
/// instant: what [`FakeServer::emit_non_rfc3339_timestamps`] switches to.
fn format_local_date_time(unix_seconds: i64) -> String {
    let (year, month, day, hour, minute, second) = civil_from_unix(unix_seconds);
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}")
}

/// Split `unix_seconds` into `(year, month, day, hour, minute, second)` in UTC.
///
/// The date half is Howard Hinnant's `civil_from_days`, the inverse of the
/// `days_from_civil` the SDK's parser runs — deliberately the other direction,
/// so a bug shared by both would have to be a bug in the algorithm rather than
/// in one transcription of it. Exact for any instant this harness produces.
fn civil_from_unix(unix_seconds: i64) -> (i64, i64, i64, i64, i64, i64) {
    let days = unix_seconds.div_euclid(SECONDS_PER_DAY);
    let seconds_of_day = unix_seconds.rem_euclid(SECONDS_PER_DAY);

    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097; // [0, 146096]
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365; // [0, 399]
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100); // [0, 365]
    let shifted_month = (5 * day_of_year + 2) / 153; // [0, 11], with March as 0
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1; // [1, 31]
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    }; // [1, 12]
    let year = if month <= 2 { year + 1 } else { year };

    (
        year,
        month,
        day,
        seconds_of_day / 3_600,
        (seconds_of_day / 60) % 60,
        seconds_of_day % 60,
    )
}

/// The instant this server stamps its `nth` write with, `nth` counting from 1 —
/// what a [`FileTimestamp`](rociadb_sdk::FileTimestamp) from that write has to
/// parse to.
///
/// Built from a `Duration` here and formatted as text over there, so an
/// assertion against it closes the loop the suite actually cares about: the
/// server formats an instant, the wire carries a string, and the SDK's parser
/// recovers the instant the server meant.
pub fn nth_write_time(nth: u64) -> std::time::SystemTime {
    let base = u64::try_from(TIMESTAMP_BASE_UNIX_SECONDS).expect("the base instant is after 1970");
    std::time::UNIX_EPOCH + Duration::from_secs(base + nth)
}

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
