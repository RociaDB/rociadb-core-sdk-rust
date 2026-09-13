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
  ids, counts) and no payloads. The three remaining `warn!` lines are
  unchanged: `disable_auth()`, a non-`https` token URL, and a failed background
  token refresh. A deployment that relied on the `info!`/`error!` lines must
  lower its filter for this crate's target to `debug`.

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
  wraps the request and maps a failed `tonic::Status`. That single choke point
  is where a per-call deadline and an automatic refresh-and-retry will be added
  without touching twenty call sites. The two streaming RPCs (`Upload`,
  `Download`) still call the generated client directly.
- The neighbor page size used while walking every page in
  `get_outgoing_neighbor_nodes` / `get_incoming_neighbor_nodes` is a documented
  named constant instead of a literal `50` repeated at two call sites. The
  value is unchanged.

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

## [1.0.0] - 2026-09-01

First stable release. See the
[repository history](https://github.com/RociaDB/rociadb-core-sdk-rust/commits/main)
for what it contained.
