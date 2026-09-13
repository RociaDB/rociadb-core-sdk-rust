# Changelog

All notable changes to this crate are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - Unreleased

### Breaking

- **Minimum supported Rust version is 1.88**, stated in `Cargo.toml` as
  `rust-version` and checked in CI against the committed `Cargo.lock`. The
  floor comes from the dependency graph (tonic, tonic-prost and
  tonic-prost-build 0.14.6 all declare 1.88), not from `edition = "2024"`.
- `tokio` is now depended on with only the features the library itself needs
  (`rt`, `sync`, `time`, `macros`). `rt-multi-thread` moved to
  `[dev-dependencies]`. A consumer that relied on this crate enabling
  `rt-multi-thread` for them — through Cargo's feature unification — must now
  enable it on their own `tokio` dependency.
- **The public API is restructured around one method per operation.** Optional
  per-call parameters now travel in an options or input struct passed last,
  replacing the `_with_request_id` / `_with_node_binding` / `_as` sibling
  methods. Nothing is deprecated: the 1.0 names are gone. Pagination keeps its
  positional `limit: Option<u32>, cursor: Option<&str>` convention, and
  `tenant_id: &str` stays the first argument of every tenant-scoped call.

  | 1.0 | 2.0 |
  | --- | --- |
  | `create_document(t, c, id, value, node_label, node_graph)` | `put_document(t, c, id, &value, DocumentWriteOptions::new().with_node_binding(NodeBinding::new(label, graph)))` |
  | `create_document_with_request_id(t, c, id, &value, node_label, node_graph, rid)` | `put_document(t, c, id, &value, DocumentWriteOptions::new().with_request_id(rid).with_node_binding(..))` |
  | `create_document_with_node_binding(t, c, id, value, binding)` | `put_document(t, c, id, &value, DocumentWriteOptions::new().with_node_binding(binding))` |
  | `create_document_with_node_binding_and_request_id(..)` | `put_document(t, c, id, &value, DocumentWriteOptions::new().with_request_id(rid).with_node_binding(binding))` |
  | `put_document(t, c, id, &value)` | `put_document(t, c, id, &value, DocumentWriteOptions::new())` |
  | `put_document_with_request_id(t, c, id, &value, rid)` | `put_document(t, c, id, &value, DocumentWriteOptions::new().with_request_id(rid))` |
  | `delete_document(t, c, id)` | `delete_document(t, c, id, WriteOptions::new())` |
  | `delete_document_with_request_id(t, c, id, rid)` | `delete_document(t, c, id, WriteOptions::new().with_request_id(rid))` |
  | `search_documents::<T>` with `value: &(impl Serialize + ?Sized)` | `search_documents::<T, V>` with `value: &V, V: Serialize + ?Sized` (a turbofish is now `::<Doc, _>`) |
  | `get_node(t, g, id) -> Value` | `get_node::<Value>(t, g, id)` |
  | `get_node_as::<T>(t, g, id)` | `get_node::<T>(t, g, id)` |
  | `get_edge(t, g, id) -> Edge<Value>` | `get_edge::<Value>(t, g, id)` |
  | `get_edge_as::<T>(t, g, id)` | `get_edge::<T>(t, g, id)` |
  | `put_node(t, g, id, &value)` | `put_node(t, g, id, &value, WriteOptions::new())` |
  | `put_node_with_request_id(t, g, id, &value, rid)` | `put_node(t, g, id, &value, WriteOptions::new().with_request_id(rid))` |
  | `add_edge(t, g, edge_id, from, to, label, &value)` | `add_edge(t, g, EdgeInput::new(edge_id, from, to, label, value))` |
  | `add_edge_with_request_id(t, g, edge_id, from, to, label, &value, rid)` | `add_edge(t, g, EdgeInput::new(..).with_request_id(rid))` |
  | `add_edge_with_input(t, g, edge)` | `add_edge(t, g, edge)` |
  | `delete_edge(t, g, id)` | `delete_edge(t, g, id, WriteOptions::new())` |
  | `delete_edge_with_request_id(t, g, id, rid)` | `delete_edge(t, g, id, WriteOptions::new().with_request_id(rid))` |
  | `neighbors_out(..) -> NeighborPage`, `.neighbors` | `neighbors_out(..) -> Page<Neighbor>`, `.items` |
  | `neighbors_in(..) -> NeighborPage`, `.neighbors` | `neighbors_in(..) -> Page<Neighbor>`, `.items` |
  | `upload_file(t, b, f, bytes: impl AsRef<[u8]>, opts)` | `upload_file(t, b, f, bytes: impl Into<Vec<u8>>, opts)` |
  | `upload_file_owned(t, b, f, bytes: Vec<u8>, opts)` | `upload_file(t, b, f, bytes, opts)` (a `Vec<u8>` still moves without a copy) |
  | `upload_file_chunked(t, b, f, size_bytes, checksum, chunks, opts)` | `upload_file_chunked(t, b, f, chunks, FileStreamUploadOptions::new(size_bytes, checksum))` |
  | `delete_file(t, b, f)` | `delete_file(t, b, f, WriteOptions::new())` |
  | `delete_file_with_request_id(t, b, f, rid)` | `delete_file(t, b, f, WriteOptions::new().with_request_id(rid))` |
  | `FileUploadOptions { checksum: Option<Vec<u8>> }` | `FileUploadOptions { checksum: Option<[u8; 32]> }`, or `.with_checksum([u8; 32])` |
  | `FileStreamUploadOptions::default()` | `FileStreamUploadOptions::new(size_bytes, checksum)` — no `Default`, since neither of those two fields has a sensible one |
  | `NeighborPage` | `Page<Neighbor>` |
  | `rociadb_sdk::file::*`, `rociadb_sdk::graph::*` | `rociadb_sdk::*` (`file` and `graph` are private; `auth` is the only public module) |
  | `builder.host(..)` and the other setters: `&mut self -> &mut Self` | `self -> Self`: `RociaDbBuilder::new().host(..).disable_auth().build().await?`, and `let b = RociaDbBuilder::new().host(..);` now compiles |

- **A checksum is `[u8; 32]`, not `Vec<u8>`.** `FileUploadOptions::checksum` is
  `Option<[u8; 32]>` and `FileStreamUploadOptions::checksum` is `[u8; 32]`, so
  the length is checked by the compiler and the runtime
  "checksum must be exactly 32 bytes" `Validation` error is gone. The SHA-256
  digest is still computed for you when `FileUploadOptions::checksum` is
  `None`. Callers holding a `Vec<u8>` convert with
  `<[u8; 32]>::try_from(v.as_slice())`.
- **`add_edge` now defaults its `request_id` to `add_edge:<uuid>`** instead of
  a bare UUID with no prefix — on the single-item and batch paths alike. It was
  the only write whose generated key carried no operation prefix. A caller that
  parsed those keys, or replayed one recorded from 1.0, sees a different shape;
  the server's dedup scope already includes the operation, so nothing about
  deduplication changes.
- **Logging levels changed.** The SDK no longer emits `info!` on routine
  success or `error!` on routine failure: a library hands the error back and
  lets the caller decide how to log it. Every RPC now emits exactly one
  `debug!` line with its identifying fields (tenant, collection/graph/bucket,
  ids, counts) and no payloads. The `warn!` lines are `disable_auth()`, a
  non-`https` token URL, a failed background token refresh, a token response
  with no `expires_in`, and a token refresh that failed after an
  `UNAUTHENTICATED` response. A deployment that relied on the `info!`/`error!`
  lines must lower its filter for this crate's target to `debug`.
- **Secrets are `secrecy::SecretString`, not `String`.** The OAuth2 client
  secret and the bearer token are redacted by every formatter and zeroized
  when dropped, which changes three signatures in `auth`:

  | 1.0 | 2.0 |
  | --- | --- |
  | `TokenManager::new(http, token_url: String, client_id: String, client_secret: String)` | `TokenManager::new(http, token_url: String, client_id: String, client_secret: SecretString)` |
  | `fetch_token(http, token_url: &str, client_id: &str, client_secret: &str)` | `fetch_token(http, token_url: &str, client_id: &str, client_secret: &SecretString)` |
  | `TokenResponse { access_token: String, .. }` | `TokenResponse { access_token: SecretString, .. }` (read it with `ExposeSecret::expose_secret`) |

  `RociaDbBuilder::auth_client_credentials` keeps `impl Into<String>` and
  wraps the secret on arrival, so builder code is unchanged. `SecretString`
  and `ExposeSecret` are re-exported at the crate root.
- **`TokenResponse` deserialization is lenient, and `expires_in` /
  `token_type` are no longer required.** `expires_in` accepts a whole or
  fractional JSON number, or either written as a string (`"3600"`), and when
  absent assumes 300 seconds with a `warn!` instead of failing the whole
  token fetch; `token_type` defaults to `"Bearer"` and its case is preserved
  (RFC 6749 §7.1 makes it case-insensitive). A present `expires_in` that is
  not a usable number at all is still an error. An IdP whose response was
  previously rejected now works.
- **New `RociaDbError::Config` variant**, and the errors that were mis-filed
  under `Connection` and `Validation` moved into it. `Connection` now means a
  genuine dial or transport failure only, and `Validation` a client-side data
  rule on one call's arguments only.

  | Failure | 1.0 variant | 2.0 variant |
  | ------- | ----------- | ----------- |
  | missing host, host with a path / query / fragment, unparseable host URL | `Connection` | `Config` |
  | missing `AUTH_TOKEN_URL` / `AUTH_CLIENT_ID` / `AUTH_CLIENT_SECRET` | `Connection` | `Config` |
  | a TLS configuration the endpoint rejects | `Connection` | `Config` |
  | zero connect timeout (and the new zero request timeout) | `Validation` | `Config` |
  | failed dial, DNS failure, refused connection | `Connection` | `Connection` |
  | zero page limit, oversized file, chunk/size mismatch | `Validation` | `Validation` |

  The internal `RociaDbError::connection(message)` constructor (no source)
  is gone; `Config` has `RociaDbError::config` in its place. Code matching
  on `Connection { .. }` for a configuration mistake must match
  `Config { .. }` instead — the enum is `#[non_exhaustive]`, so a wildcard
  arm keeps compiling either way.

### Added

- English documentation on the generated types re-exported at the crate root
  (`CollectionInfo`, `StatResponse`, `Neighbor`, `UploadRequest`,
  `DownloadResponse`) and on each of their fields, describing the wire
  semantics. It is attached by the build script, so it is regenerated with the
  code rather than drifting from it.
- `#![forbid(unsafe_code)]` and `#![warn(missing_docs)]` on the crate, and doc
  comments on every public item the latter reported — the fields of `Page`,
  `DocumentPage`, `NodeBinding`, `DocumentQueryFilter`, `DocumentQuerySort`,
  `NodeInput`, `EdgeInput`, `Edge`, `NeighborNode`,
  `FileUploadOptions`, `FileStreamUploadOptions`, `TokenResponse` and the
  `RociaDbError` variants, plus the variants of `DocumentQueryOperator` and
  `DocumentQuerySortDirection`.
- `WriteOptions` (`request_id`) and `DocumentWriteOptions` (`request_id`,
  `node_binding`): the per-call options every write now takes. Both are
  `#[non_exhaustive]`, `Debug + Clone + Default + PartialEq + Eq`, with `new()`
  and chainable `with_*` setters. `WriteOptions`' documentation is the single
  place where the generated idempotency key of *every* write is stated, and
  every write links to it.
- `FileUploadOptions::new`, `with_content_type`, `with_checksum`,
  `with_request_id`, and `FileStreamUploadOptions::new(size_bytes, checksum)`,
  `with_content_type`, `with_request_id`. Both structs are now
  `#[non_exhaustive]` and derive `PartialEq + Eq`.
- `impl Debug for RociaDbClient`, reporting the host the client was built for
  and whether auth is enabled — never a token, a client id, or a secret.
- `.github/workflows/ci.yml`: formatting, Clippy, tests, a documentation build
  with `RUSTDOCFLAGS="-D warnings"`, an MSRV job on Rust 1.88, and a
  `cargo deny` job.
- `deny.toml`: RustSec advisories, a permissive-only licence allow-list,
  duplicate and wildcard bans as warnings, and crates.io as the only allowed
  source.
- `RociaDbBuilder::request_timeout(Duration)`: a deadline for every unary
  RPC. Opt-in, with no default. Each attempt both carries a `grpc-timeout`
  header (so the server can abandon the work) and is wrapped in a
  `tokio::time::timeout` (which also covers decoding the response body,
  unlike tonic's own header-phase enforcement); either way the call fails
  with a `Status` error whose `code()` is `DeadlineExceeded`. A zero value is
  rejected by `build()`. The streaming upload and download paths are
  deliberately not covered.
- `RociaDbBuilder::tls_config(ClientTlsConfig)`, replacing the default
  `ClientTlsConfig::new().with_native_roots()` — for a private CA, mTLS, or
  an overridden verification domain. tonic only applies TLS to an `https://`
  host.
- `RociaDbBuilder::http2_keep_alive(interval, timeout)`, mapping to tonic's
  `http2_keep_alive_interval` / `keep_alive_timeout` /
  `keep_alive_while_idle(true)` — for a connection held open through
  something that reaps idle flows.
- `RociaDbBuilder::build_with_channel(Channel)`: build a client on a channel
  the caller already has (custom connector, Unix socket, balanced list,
  in-process test server), skipping host validation and dialing while keeping
  the whole auth setup and the request deadline.
- `RetryPolicy` (`#[non_exhaustive]`, `Debug + Clone + PartialEq + Eq +
  Default`, with `new()`, `with_max_attempts`, `with_base_delay`,
  `with_max_delay`, `with_retry_unavailable`) and
  `RociaDbClient::retry(&policy, op)`, which replays a closure while it fails
  with `ABORTED` (and `UNAVAILABLE` when enabled), waiting an exponentially
  increasing, fully jittered delay between attempts and returning the last
  error once the attempts are exhausted. The jitter is derived from a v4
  UUID's random bytes, so no `rand` dependency was added.
- `RociaDbError::is_not_found()` and `RociaDbError::is_invalid_argument()`,
  alongside the existing `is_unauthenticated` / `is_permission_denied` /
  `is_already_exists` / `is_aborted`.
- `RociaDbClient::download_file_verified(tenant_id, bucket, file_id)`: the
  verifying counterpart of `download_file`. It calls `stat_file` first,
  streams the download while hashing it with SHA-256, and checks the byte
  count against `size_bytes` and the digest against the stored `checksum`
  before returning the buffer. The buffer is pre-allocated from the reported
  size, capped at 64 MiB so a server reporting an absurd `size_bytes` cannot
  make the client reserve gigabytes up front. **What it proves**: the bytes
  received are the bytes the uploader *declared* — the server never verified
  the uploader's checksum against the payload, so this catches storage
  corruption, truncation and partial overwrites, and not an uploader whose
  declared digest never matched its own bytes. `download_file` and
  `download_file_stream` link to it where they describe that asymmetry.
- `RociaDbError::ChecksumMismatch { expected: Vec<u8>, actual: Vec<u8> }` and
  `RociaDbError::SizeMismatch { expected: u64, actual: u64 }`, raised only by
  `download_file_verified`. Two flat variants rather than one nested
  `Integrity` value, so a caller branches with one match arm per failure and
  reads the numbers straight off it. The size is checked first (a truncated
  transfer fails both, and the byte count is the more actionable report);
  `Display` renders the two digests as lowercase hex, and both carry raw
  bytes so nothing has to be decoded to compare them.
- `RociaDbClient::neighbor_nodes_out<T>` and
  `RociaDbClient::neighbor_nodes_in<T>`:
  `(tenant_id, graph, node_id, label, limit, cursor) -> Result<Page<NeighborNode<T>>>`.
  One `neighbors_*` page, then the node payload of each neighbor *on that
  page* fetched concurrently (at most `CONCURRENT_REQUESTS` = 10 in flight,
  order preserved) and decoded into `T`. The work is bounded by `limit`
  rather than by the node's degree, and the cursor is the one
  `neighbors_out` / `neighbors_in` issued, with the same scoping rules.
  `get_outgoing_neighbor_nodes` / `get_incoming_neighbor_nodes` keep their
  all-pages behaviour, now documented as unbounded with a pointer here, and
  both shapes share one fan-out helper.
- An integration test suite (`tests/documents.rs`, `tests/graph.rs`,
  `tests/files.rs`, `tests/auth.rs`, `tests/resilience.rs`, on a shared
  `tests/support/`) that runs the real client path — builder, channel, bearer
  interceptor, generated client, the unary helper, JSON decoding — against an
  in-process tonic server implementing all four services and a mock OAuth2
  identity provider served by hyper. Both bind `127.0.0.1:0`, so the suite is
  loopback-only, parallel-safe and needs nothing installed; the server
  reproduces the parts of the contract the SDK depends on (cursor pagination,
  `total_count`, the `(from, label, to)` edge uniqueness rule, idempotent
  deletes, the 1 MiB upload chunk cap, a checksum length check that never
  looks at the bytes, and the `reason` trailing metadata on every error) and
  offers scripted per-RPC failures, per-RPC delays and a recorder of every
  request and its `authorization` header.
- Crate-root re-exports so configuring the SDK needs no extra direct
  dependency: `Channel` and `ClientTlsConfig` (from `tonic`), `SecretString`
  and `ExposeSecret` (from `secrecy`). The same stability caveat as
  `Streaming` applies.
- `secrecy` 0.10 as a dependency (MSRV 1.60, dual Apache-2.0/MIT, one
  transitive crate — `zeroize`, already in the graph via rustls).
- This changelog.

### Changed

- **`protoc` is no longer required to build.** The build script compiles the
  `.proto` with [`protox`](https://docs.rs/protox), a pure-Rust protobuf
  compiler, so `cargo build` works on a bare toolchain — no system package, no
  `PROTOC` environment variable, on a developer machine, in CI, or on docs.rs.
- `google.protobuf.Empty` now maps to `()` instead of `pbjson_types::Empty`.
  This is invisible in the SDK's own API (no public signature named it), but it
  is what allows the dependency below to go away.
- The vendored well-known-type `.proto` files under `proto/google/` were
  deleted; `protox` supplies them. `proto/upstream/v1/upstream.proto` is
  unchanged and still mirrors the server repository byte for byte.
- `Cargo.toml`'s `exclude` list now also drops `.github`, `deny.toml` and
  `mise.toml` from the published package. `CHANGELOG.md` is deliberately
  included.
- **Module layout.** Each service's RPCs and types live with each other:
  documents in `document.rs`, graph in `graph.rs`, files in `file.rs`, tenants
  in `tenant.rs`. `lib.rs` keeps only the crate documentation, the builder, the
  client (with its auth methods), `Page`, `WriteOptions` and the private
  helpers the modules share. Everything public is re-exported at the crate
  root, so the namespace is flat and `auth` is the only public module — the
  paths `rociadb_sdk::file::..` and `rociadb_sdk::graph::..` no longer exist.
- Every unary RPC now goes through one private helper on the client, which
  wraps the request, applies the per-call deadline, refreshes and retries once
  on `UNAUTHENTICATED`, and maps a failed `tonic::Status` — so none of that is
  repeated at twenty call sites. The two streaming RPCs (`Upload`, `Download`)
  still call the generated client directly and get none of it.
- The neighbor page size used while walking every page in
  `get_outgoing_neighbor_nodes` / `get_incoming_neighbor_nodes` is a documented
  named constant instead of a literal `50` repeated at two call sites. The
  value is unchanged.
- **`build.rs` runs a second codegen pass**, writing server-only stubs
  (`build_server(true).build_client(false)`, the same `type_attribute` and
  documentation settings) into `$OUT_DIR/test_server/` for the integration
  tests to `include!`. The library's own generated code is unchanged and
  stays client-only — its `tonic` has no server feature, and the one in
  `[dev-dependencies]` does. The `.proto` is still compiled once: the
  `FileDescriptorSet` is cloned between the two passes. A consumer pays a few
  tens of milliseconds inside a build script that measures around 150 ms in
  total, and compiles none of the second pass's output.
- New development dependencies, all of them crates the graph already carried
  (only features and five small crates — `axum`, `axum-core`, `matchit`,
  `mime`, `httpdate` — are added to `Cargo.lock`, and nothing changes for a
  consumer building the library): `tonic` with `server` + `router`, `hyper`
  with `http1` + `server`, `hyper-util` with `tokio`, `http-body-util`, and
  `net` on `tokio`.

### Removed

- Every 1.0 method the table above maps away, with no `#[deprecated]` shim:
  `create_document`, `create_document_with_request_id`,
  `create_document_with_node_binding`,
  `create_document_with_node_binding_and_request_id`,
  `put_document_with_request_id`, `delete_document_with_request_id`,
  `get_node_as`, `get_edge_as`, `put_node_with_request_id`,
  `add_edge_with_request_id`, `add_edge_with_input`,
  `delete_edge_with_request_id`, `upload_file_owned` and
  `delete_file_with_request_id`.
- The `NeighborPage` type, replaced by `Page<Neighbor>`.
- The `pbjson-types` dependency, which existed only to name
  `google.protobuf.Empty`. It pulled `pbjson`, `chrono`, `num-traits` and
  `autocfg` into every consumer's build; `cargo tree -e normal` goes from 166
  entries to 162.
- The `mise.toml` `protoc` tool entry, and every `PROTOC=` instruction in the
  crate documentation, `AGENTS.md` and `README.md`.

### Fixed

- Updated `h2` to 0.4.19 in `Cargo.lock` for RUSTSEC-2026-0258 (unbounded empty
  DATA frames, reachable through both `tonic` and `reqwest`).
- **A failed background token refresh no longer waits out the whole regular
  interval.** It used to wait for the next tick — 400 seconds for the IdP's
  600-second tokens — so a single failed refresh guaranteed a window in which
  every RPC failed with `UNAUTHENTICATED`. Consecutive failures now back off
  exponentially with jitter (roughly 1 s, 2 s, 4 s, 8 s, 16 s, then 30 s,
  each drawn from the upper half of its ceiling) until one succeeds, then the
  task returns to the normal cadence. `request_refresh()` keeps waking the
  task during a backoff, and each failure is reported once as a `warn!`
  carrying the error, the consecutive-failure count and the next delay.
- **The OAuth2 HTTP client has timeouts.** It was built with
  `reqwest::Client::new()`, which has none, so an IdP that accepted the TCP
  connection and never answered could hang `build()` — and every later
  `refresh_auth_token()`, each while holding the refresh lock — forever. It
  now carries the builder's connect timeout and a 30-second request timeout.
- **A unary RPC answered `UNAUTHENTICATED` now refreshes the token and
  retries once, automatically**, when auth is enabled. The refresh is
  coalesced with any already in flight; a refresh that itself fails is
  reported as a `warn!` and the original `UNAUTHENTICATED` is returned; the
  call is never retried more than once. Streaming uploads and downloads are
  not covered — call `refresh_auth_token()` yourself there.

## [1.0.0] - 2026-09-01

First stable release. See the
[repository history](https://github.com/RociaDB/rociadb-core-sdk-rust/commits/main)
for what it contained.
