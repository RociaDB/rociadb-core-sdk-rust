# rociadb-sdk

Rust client SDK for the Rocia DB gRPC upstream services: documents, graph,
files, and tenants.

The Node.js/TypeScript SDK lives in its own sibling repository
([`rociadb-core-sdk-ts`](https://github.com/RociaDB/rociadb-core-sdk-ts)),
not in this checkout — there is no `typescript/` directory here.

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Authentication](#authentication)
- [Transport and TLS](#transport-and-tls)
- [Documents](#documents)
- [Graph](#graph)
- [File Operations](#file-operations)
- [Tenants](#tenants)
- [Pagination](#pagination)
- [Document Query Rules](#document-query-rules)
- [Tenancy and Authorization Scopes](#tenancy-and-authorization-scopes)
- [Error Handling](#error-handling)
- [API Conventions](#api-conventions)
- [Parity with the TypeScript SDK](#parity-with-the-typescript-sdk)
- [Development](#development)

## Overview

This crate is a thin, typed client wrapper around the generated gRPC clients
for Rocia DB's four upstream services — documents, graph, files, and
tenants (23 RPCs in total). It handles connection setup, OAuth2
client-credentials authentication with automatic token refresh, JSON
encoding/decoding of payloads, pagination bookkeeping, and the file-upload
wire contract, so callers work with plain Rust types (`serde_json::Value` or
any `Serialize`/`DeserializeOwned` type) instead of hand-building protobuf
messages.

## Installation

```toml
[dependencies]
rociadb-sdk = "1.0"
```

Building the crate runs a build script that compiles the bundled
`.proto` files, so **`protoc` must be available** on any machine that
compiles it — including CI. Install it from your package manager
(`apt install protobuf-compiler`, `brew install protobuf`) or from the
[protobuf releases](https://github.com/protocolbuffers/protobuf/releases),
and make sure it is on `PATH` or pointed at by the `PROTOC` environment
variable. No other system dependency is required: the Google well-known
types the API uses are vendored under `proto/google/protobuf/`.

This is a standalone crate, not a workspace member: there is no
`crates/rociadb-sdk` path inside it. To work against an unreleased change,
depend on a checkout or on git instead of the published version:

```toml
[dependencies]
# From a sibling checkout:
rociadb-sdk = { path = "../rocia-db-sdk-rust" }
# Or pinned to a tag from git:
# rociadb-sdk = { git = "https://github.com/RociaDB/rociadb-core-sdk-rust", tag = "v1.0.0" }
```

`Cargo.toml` declares `rust-version = "1.88"` — the minimum toolchain this
crate is built and tested against. The floor is set by the dependency graph,
not by `edition = "2024"`; the comment above that field records which
dependencies pin it.

For a runnable project that wires the SDK up end to end, see
[`example-rust-project`](https://github.com/RociaDB/example-rust-project).

## Quick Start

```rust
use rociadb_sdk::RociaDbBuilder;
use serde_json::json;

# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
let client = RociaDbBuilder::new()
    .host("http://127.0.0.1:50051")
    .auth_client_credentials(
        "https://example.com/token",
        "client-id",
        "client-secret",
    )
    .build()
    .await?;

client
    .create_document(
        "tenant-1",
        "products",
        "sku-123",
        json!({"sku": "sku-123", "label": "Widget"}),
        Some("product".to_string()),
        Some("products".to_string()),
    )
    .await?;

let node = client.get_node("tenant-1", "products", "product:sku-123").await?;
println!("node = {node}");
# Ok(())
# }
```

`.host(...)` above is a plaintext (`http://`) local address for
illustration. See [Transport and TLS](#transport-and-tls) for what to use
against a real deployment.

`RociaDbClient` is `Clone`, and every clone shares one underlying channel,
one token manager, and one background token-refresh task by design: each
method clones the cheap, `Arc`-backed inner service client before issuing
its RPC, so a client shared behind an `Arc` needs no `Mutex` to be used
concurrently. There is no `close()` method — drop the last live clone to
release the connection and stop the background refresh task; see
[Parity with the TypeScript SDK](#parity-with-the-typescript-sdk) for why
that is the intended equivalent, not a missing feature.

## Authentication

### Token lifetime and refresh

The identity provider (`rocia-idp`) issues bearer tokens that live for
exactly **600 seconds**, hardcoded server-side — there is no negotiating a
longer-lived token. `RociaDbBuilder::build` fetches the first token and
starts a background task that refreshes it before it expires:
`max(expires_in * 2 / 3, 5s)`, i.e. roughly every 400 seconds for the current
600-second lifetime, leaving about a third of the lifetime as margin. The
task keeps running for as long as the returned `RociaDbClient`, or any of
its clones, stays alive; dropping the last clone stops it.

A client that never calls `RociaDbBuilder::build` (or that discards the
refresh guard some other way) simply dies after 10 minutes with
`UNAUTHENTICATED` on every call. The builder handles this for you; only
build your own `TokenManager` (see [Auth Helpers](#auth-helpers)) if you
need auth outside the `RociaDbClient` lifecycle.

### `UNAUTHENTICATED` vs `PERMISSION_DENIED`

The two failure modes need different handling, and confusing them wastes
retries:

- `UNAUTHENTICATED` — the token is missing, expired, malformed, or issued by
  a different issuer. This **is** a signal to renew: call
  `RociaDbClient::refresh_auth_token()` and retry.
- `PERMISSION_DENIED` — the token is valid but lacks the required scope.
  Retrying after a refresh will not help, because a fresh token carries the
  same scope. This happens in exactly two cases: a read-only client calling
  one of the 7 write RPCs, or an admin-scoped token (from `rocia-idp`'s
  account-management API) calling *any* of the 23 RPCs, reads included — see
  [Tenancy and Authorization Scopes](#tenancy-and-authorization-scopes).

```rust
match client.get_document::<serde_json::Value>("tenant-1", "products", "sku-123").await {
    Ok(doc) => { /* ... */ }
    Err(err) if err.is_unauthenticated() => {
        client.refresh_auth_token().await?;
        // retry
    }
    Err(err) if err.is_permission_denied() => {
        // do not retry: wrong scope or wrong client_id
    }
    Err(err) => return Err(err),
}
```

`is_unauthenticated()` and `is_permission_denied()` are shorthands on
`RociaDbError` for the two gRPC codes that matter most here — see
[Error Handling](#error-handling) for the full typed-error surface,
including `.code()`, `.reason()`, and `.status()` for anything more
specific than these two predicates.

### Two ways to recover from `UNAUTHENTICATED`

`refresh_auth_token()` (above) is **eager**: it awaits the round trip to
the identity provider and only returns once a fresh token is confirmed and
in hand, or propagates the fetch error otherwise — the right choice right
before retrying the call that just failed. `invalidate_auth_token()` is its
**lazy** counterpart: it is synchronous, returns immediately, and only wakes
the background refresh task so it fetches a fresh token at its next
opportunity — nobody pays for the network round trip inline. Reach for it
when you just want to mark the cached token stale (a fire-and-forget error
handler, for example) without blocking the current call on a fresh token
being in hand first:

```rust
// Somewhere that observed an UNAUTHENTICATED but is not the caller that
// needs to retry immediately (a background health check, for example):
// signal staleness and move on — no `.await`, no network call here.
client.invalidate_auth_token();
```

Both are no-ops when the client was built with `RociaDbBuilder::disable_auth`.
Neither ever discards a still-valid cached token just because a background
refresh attempt failed: the interceptor keeps injecting the last known-good
token until a replacement is confirmed.

### Default Auth Behavior

Authentication is enabled by default in `RociaDbBuilder`. If you do not call
`auth_client_credentials`, the builder reads these environment variables at
`build()` time:

- `AUTH_TOKEN_URL`
- `AUTH_CLIENT_ID`
- `AUTH_CLIENT_SECRET`

For local/testing environments you can disable auth explicitly:

```rust
let client = RociaDbBuilder::new()
    .host("http://127.0.0.1:50051")
    .disable_auth()
    .build()
    .await?;
```

### Auth Helpers

The `auth` module provides token and interceptor helpers directly, for
callers who need auth outside of `RociaDbClient` (for example, to reuse the
same token against a different service).

```rust
use rociadb_sdk::auth::TokenManager;

let http = reqwest::Client::new();
let token_manager = TokenManager::new(
    http,
    "https://example.com/token".to_string(),
    "client-id".to_string(),
    "client-secret".to_string(),
).await?;
let _refresh_guard = token_manager.spawn_refresh(token_manager.refresh_interval());
```

`spawn_refresh` returns a `#[must_use]` guard: dropping it immediately
stops the background refresh, which is why the example above binds it to a
variable instead of discarding it.

## Transport and TLS

`rocia-db` does not implement TLS itself and never will: the server
always listens in plaintext. In production, TLS terminates at a reverse
proxy placed in front of it (the proxy holds the certificates and handles
renewal); the SDK then connects with `https://` to the proxy, typically on
port 443, and the proxy forwards plaintext gRPC (HTTP/2) to the backend on
its own port (5xxxx conventionally). Connecting `http://` directly to the
bare server, as in the examples above, is only appropriate for local
development or an already-encrypted internal network segment.

The proxy must be configured for HTTP/2 end to end (no TLS-to-HTTP/1.1
downgrade toward the backend), or every gRPC call fails immediately, usually
with `UNAVAILABLE`.

### Host validation and connect timeout

`.host(...)` must be a bare `scheme://host:port` — no path component
beyond an absent one or a lone `/`. `RociaDbBuilder::build` rejects anything
else (`RociaDbError::Connection`) before attempting a connection, so a
mistyped host with a leftover path (`http://127.0.0.1:50051/v1`, pasted from
somewhere else) fails loudly instead of tonic silently ignoring the path
component and dialing the host anyway.

`RociaDbBuilder::connect_timeout` sets the deadline applied while
connecting. It defaults to **10 seconds** if never called — `build()`
always applies some connect timeout, so a slow or unreachable DNS/TCP
target fails after a bounded wait instead of hanging `.await` forever.

```rust
use rociadb_sdk::RociaDbBuilder;
use std::time::Duration;

let client = RociaDbBuilder::new()
    .host("https://rociadb.internal:50051")
    .connect_timeout(Duration::from_secs(3))
    .auth_client_credentials("https://example.com/token", "client-id", "client-secret")
    .build()
    .await?;
```

A zero-duration timeout is rejected with `RociaDbError::Validation` at
`build()` time, before any connection attempt.

## Documents

Every document write takes a `tenant_id`, a `collection`, a `document_id`,
and a JSON-serializable value. `put_document` writes a document with no
graph binding; `create_document` (shown in [Quick Start](#quick-start))
additionally upserts a graph node pointing back at it when both
`node_label` and `node_graph` are provided.

```rust
use serde_json::json;

client
    .put_document("tenant-1", "products", "sku-123", &json!({"sku": "sku-123", "label": "Widget"}))
    .await?;

let doc: serde_json::Value = client.get_document("tenant-1", "products", "sku-123").await?;

client.delete_document("tenant-1", "products", "sku-123").await?;
```

`delete_document` is **idempotent**: deleting an id that does not exist
succeeds rather than returning `NOT_FOUND`. So are `delete_edge` and
`delete_file` — no deletion in this API reports a missing target.

### Reading, listing, and querying

- `get_document::<T>` deserializes directly into the requested type.
- `list_documents::<T>` returns every document in a collection, paginated.
- `search_documents::<T>` finds documents whose `search_field` equals a
  scalar value (`FindByField`).
- `query_documents::<T>` runs a multi-filter, sortable query (`QueryDoc`).

All three listing methods return
[`DocumentPage<T>`](#page-documentpage-neighborpage-and-edge) — `items`,
`next_cursor`, and `total_count` — following the shared rules in
[Pagination](#pagination). See [Document Query Rules](#document-query-rules)
for the server-side validation each one enforces, and note that
`total_count`'s cost differs sharply between the three: free on
`list_documents`, an index count on `search_documents`, and proportional to
the full candidate set on `query_documents` — never call `query_documents`
in a loop just to get a count.

```rust
use rociadb_sdk::{DocumentQueryFilter, DocumentQueryOperator, DocumentQuerySort, DocumentQuerySortDirection};
use serde_json::json;

// list_documents: every document in a collection.
let page = client
    .list_documents::<serde_json::Value>("tenant-1", "products", Some(50), None)
    .await?;

// search_documents: exact match on one field.
let matches = client
    .search_documents::<serde_json::Value>("tenant-1", "products", "sku", &json!("sku-123"), None, None)
    .await?;

// query_documents: multiple filters, combined with AND, plus sorting.
let filters = [DocumentQueryFilter::new(
    "status",
    DocumentQueryOperator::Eq,
    vec![json!("active")],
)];
let sort = [DocumentQuerySort::new("label", DocumentQuerySortDirection::Asc)];
let results = client
    .query_documents::<serde_json::Value>("tenant-1", "products", &filters, &sort, Some(50), None)
    .await?;
```

## Graph

### Single node and edge operations

```rust
use serde_json::json;

client.put_node("tenant-1", "products", "product:sku-1", &json!({"sku": "sku-1"})).await?;
let node: serde_json::Value = client.get_node_as("tenant-1", "products", "product:sku-1").await?;

client
    .add_edge("tenant-1", "products", "1", "product:sku-1", "group:grp-1", "belongs_to", &json!({"weight": 1}))
    .await?;
let edge: rociadb_sdk::Edge<serde_json::Value> =
    client.get_edge("tenant-1", "products", "1").await?;
client.delete_edge("tenant-1", "products", "1").await?;
```

`get_node` (used in [Quick Start](#quick-start)) is a convenience method
returning `serde_json::Value`; `get_node_as::<T>` deserializes into any
`DeserializeOwned` type. `add_edge` fails with `NOT_FOUND` if either `from`
or `to` does not already exist as a node — create both endpoint nodes
first, and see [Document Query Rules](#document-query-rules) for the
`ALREADY_EXISTS` a duplicate `(from, label, to)` triplet produces. Like
`delete_document` and `delete_file`, `delete_edge` is **idempotent**:
deleting an edge that is not there succeeds and touches nothing.

### Reading an edge back

`get_edge` and `get_edge_as::<T>` are the only reads that return an edge's
**properties**: `neighbors_out`/`neighbors_in` name the neighbor and the
edge id only, so the JSON carried on a relation — a weight, a status, a
date — has no other way back.

Both return an [`Edge<T>`](#page-documentpage-neighborpage-and-edge) whose
five fields are exactly the five `add_edge` takes, in the same orientation,
so an edge read back feeds straight into a write:

```rust
#[derive(serde::Serialize, serde::Deserialize)]
struct Belongs { weight: u32 }

let edge: rociadb_sdk::Edge<Belongs> =
    client.get_edge_as("tenant-1", "products", "1").await?;
println!("{} -[{}]-> {} ({})", edge.from, edge.label, edge.to, edge.value.weight);

client
    .add_edge(
        "tenant-1", "products", &edge.edge_id,
        &edge.from, &edge.to, &edge.label,
        &Belongs { weight: edge.value.weight + 1 },
    )
    .await?;
```

An unknown `edge_id` returns `NOT_FOUND`, as `get_node_as` does for a
missing node. An edge id is scoped to one `(tenant_id, graph)` pair: the
same id under another graph, or for another tenant, is a different edge and
therefore absent. Properties come back in the shape they were written in,
not as the bytes originally sent — the server normalizes JSON on write
(object keys sorted, whitespace stripped). Unlike `get_node`, this read is
not served from a server-side cache; every call reaches storage.

### Batch operations

The client uses a bounded concurrency of 10 for batch upserts. `put_nodes`
and `add_edges` each take an ordered sequence of `NodeInput`/`EdgeInput`
structs — anything `IntoIterator<Item = NodeInput>` /
`IntoIterator<Item = EdgeInput>`, a `Vec` in the common case — not a
`HashMap`: items are dispatched in the order given, and two items sharing
the same id are never silently merged into one, both are sent.

```rust
use rociadb_sdk::{EdgeInput, NodeInput};
use serde_json::json;

let nodes = vec![
    NodeInput::new("product:sku-1", json!({"sku": "sku-1"})),
    // Supply your own idempotency key when a retry must not write twice:
    NodeInput::new("product:sku-2", json!({"sku": "sku-2"}))
        .with_request_id("put_node:import-42"),
];
client.put_nodes("tenant-1", "products", nodes).await?;

let edges = vec![EdgeInput::new(
    "1",
    "product:sku-1",
    "group:grp-1",
    "belongs_to",
    json!({"weight": 1}),
)];
client.add_edges("tenant-1", "products", edges).await?;
```

`node_id` is the **complete** node id (`"product:sku-1"`) — the SDK
never recomposes it from a `(label, id)` pair, so build the full id
yourself. The edge id is raw and must not be prefixed with the label.
`request_id: None` lets the SDK generate a fresh idempotency key for that
item; set it explicitly — and reuse the same value on a retry — whenever a
batch might need to be replayed (see below).

**Neither batch is atomic: each stops at the first error.** In-flight
requests are cancelled, and the error does not say which items had already
succeeded. The correct way to resume is to replay the same items with the
same `request_id` values used on the first attempt — the server
deduplicates on `(tenant, operation, request_id)`, so already-applied
writes are recognized and skipped rather than reapplied, and only the
writes that never landed actually happen.

### Neighbors

`neighbors_out`/`neighbors_in` return one raw, single-page
[`NeighborPage`](#page-documentpage-neighborpage-and-edge) of graph edges —
`Neighbor { node_id, edge_id }` — following the pagination rules in
[Pagination](#pagination):

```rust
let page = client
    .neighbors_out("tenant-1", "products", "product:sku-1", "belongs_to", Some(50), None)
    .await?;
for neighbor in &page.neighbors {
    println!("edge {} -> node {}", neighbor.edge_id, neighbor.node_id);
}
```

`get_outgoing_neighbor_nodes`/`get_incoming_neighbor_nodes` paginate to
completion internally and additionally fetch each neighbor's node payload,
returning hydrated `NeighborNode<T>` values instead of raw edges:

```rust
let neighbors = client
    .get_outgoing_neighbor_nodes::<serde_json::Value>(
        "tenant-1",
        "products",
        "product:sku-1",
        "belongs_to",
    )
    .await?;
```

They stop only when `next_cursor` comes back absent — never merely because
one page was empty or shorter than requested, and never on a short or empty
page mid-listing (a stale index entry surviving a deleted node, for
example, can legitimately produce one with more data still to come) — with
one extra safety net: they also stop if the server ever repeats the same
cursor twice in a row, guarding against an infinite loop on a misbehaving
server.

### Listing graphs and nodes

```rust
let graphs = client.list_graphs("tenant-1", None, None).await?;
let nodes = client.list_nodes("tenant-1", "products", Some(100), None).await?;
```

## File Operations

`upload_file` and `download_file` are ergonomic in-memory helpers; the
underlying gRPC contract they implement is worth understanding even if you
never touch `upload_file_chunked` or `upload_file_stream` directly.

### The upload wire contract

- **Chunk size is the client's choice, capped at 1 MiB — it is not a fixed
  requirement.** The server stores each chunk verbatim at its position in
  the stream and, on download, reads chunks back until it has collected
  `size_bytes` bytes in total, without assuming any particular chunk size.
  A single message's `chunk` larger than 1 MiB is rejected outright with
  `INVALID_ARGUMENT` (`"chunk exceeds 1 MiB"`); anything at or under that
  cap is fine, sliced however the client likes. `upload_file` and
  `upload_file_chunked` (below) both always emit exactly-1-MiB chunks (the
  last one may be shorter) — not because the server requires it, but
  because 1 MiB is the largest message the server allows, so it is also the
  fewest possible messages for a given file; this is also why
  `FileUploadOptions` has no `chunk_size` knob. It remains the only chunk
  size that is safe against a server older than `1.0.0-rc.16`.
- The **first** message of the upload stream must carry the metadata:
  `tenant_id`, `bucket`, `file_id`, `size_bytes` (the exact total byte
  count), `content_type`, `checksum`, and `request_id`. Every later message
  is only read for its `chunk` field.
- `checksum` must be exactly **32 raw bytes** — a SHA-256 digest. The server
  rejects any other length, including empty, with `INVALID_ARGUMENT`
  (`"checksum must be 32 bytes (sha256)"`); note that the server does *not*
  verify the checksum actually matches the uploaded bytes, only that it is
  32 bytes long. `upload_file` computes this SHA-256 digest of the buffer
  automatically when `FileUploadOptions.checksum` is `None`; if you supply
  your own, it must be exactly 32 bytes or the call fails client-side,
  before any network call.
- The sum of every `chunk`'s bytes across the stream must equal `size_bytes`
  exactly, or the server rejects the upload with `INVALID_ARGUMENT`
  (`"size_bytes does not match uploaded data"`) at the end of the stream —
  this is what makes `size_bytes` a value the SDK (and the server, on
  download) can trust, rather than just a caller-supplied claim.
- Re-uploading an existing `file_id` **replaces it, with no error for the
  duplicate** — there is no separate delete-then-upload dance required. The
  content served by `download_file`/`stat_file` afterward is always the
  newest upload.
- Files over the server's `limits.max_file_bytes` (**5 GiB by default**) are
  rejected. `upload_file` checks this client-side and returns a clear error
  before sending anything.
- An empty file is a valid, common case: it needs exactly one message
  (metadata only, empty `chunk`) and no data messages. `upload_file` handles
  this for you.
- The file only becomes visible (in `list_files`, `stat_file`,
  `download_file`) once the whole stream has been received and validated.
  An interrupted stream leaves orphaned chunks that a background GC
  eventually reclaims; the partial file never appears anywhere.

```rust
use rociadb_sdk::file::FileUploadOptions;

// options.checksum: None -> upload_file computes SHA-256 of the buffer for
// you and chunks it into exactly-1-MiB messages. You do not need to touch
// checksum or chunking yourself for the common in-memory case.
client
    .upload_file(
        "tenant-1",
        "assets",
        "manual.txt",
        b"hello RociaDB",
        FileUploadOptions {
            content_type: "text/plain".into(),
            ..Default::default()
        },
    )
    .await?;

let metadata = client.stat_file("tenant-1", "assets", "manual.txt").await?;
let bytes = client.download_file("tenant-1", "assets", "manual.txt").await?;
client.delete_file("tenant-1", "assets", "manual.txt").await?;
```

### Streaming an upload without buffering the whole file

`upload_file_chunked` is the middle tier between `upload_file` (buffers
the whole file, computes the checksum for you) and `upload_file_stream`
(a raw pass-through with zero validation — see below). Give it a
`Stream<Item = Vec<u8>>` of arbitrarily-sized pieces — however the source
naturally produces them, a `64 KiB` `AsyncRead` wrapper, messages from
another stream, anything — and it re-buffers internally, always emitting
exactly-1-MiB gRPC messages to the server, the same chunking `upload_file`
produces from an in-memory buffer. `size_bytes` and `checksum` (the 32-byte
SHA-256 digest of the complete file) must both be supplied up front,
because file metadata travels on the very first gRPC message, before this
method has read a single byte from `chunks` — hash the source ahead of time
(a first pass over the file, for example) if you only have raw bytes to
start from. If `chunks` ends up producing more or fewer total bytes than
`size_bytes` declared, this fails client-side with
`RociaDbError::Validation` instead of sending a stream the server would
reject anyway at the end.

```rust
use rociadb_sdk::file::FileStreamUploadOptions;
use futures::stream;
use sha2::{Digest, Sha256};

let payload = std::fs::read("large-report.csv").expect("read source file");
let checksum = Sha256::digest(&payload).to_vec();
let size_bytes = payload.len() as u64;

// Any chunking the source naturally produces — 64 KiB here purely as an
// example. upload_file_chunked re-slices to the server's 1 MiB messages
// internally regardless of what you hand it.
let chunks: Vec<Vec<u8>> = payload.chunks(64 * 1024).map(|c| c.to_vec()).collect();

client
    .upload_file_chunked(
        "tenant-1",
        "reports",
        "large-report.csv",
        size_bytes,
        checksum,
        stream::iter(chunks),
        FileStreamUploadOptions {
            content_type: "text/csv".into(),
            ..Default::default()
        },
    )
    .await?;
```

**Naming trap when porting code between SDKs:** despite doing the
re-chunking and validation, this method is not called `upload_file_stream`
— that name was already taken in this SDK by the raw, zero-validation
escape hatch below. See
[Parity with the TypeScript SDK](#parity-with-the-typescript-sdk) for the
full naming table — the TypeScript SDK's equivalent of *this* method is
named differently again.

For large downloads, prefer `download_file_stream` to avoid buffering
the complete file in memory. Use `upload_file_stream` only when you are
ready to build every protobuf message yourself and match the wire contract
above exactly; it is a low-level escape hatch that does **not** rechunk,
cap a chunk's size, or compute a checksum for you. Getting the chunk *size*
wrong here fails fast with `INVALID_ARGUMENT` rather than silently
corrupting a later download — but a wrong `size_bytes` total, or a
`checksum` that does not actually match the bytes (the server only checks
its length, never its content), can still slip through as an upload that
looks successful while carrying bad data. Prefer `upload_file_chunked`
above unless you specifically need to hand-build the message stream.

### Listing buckets and files

```rust
let buckets = client.list_buckets("tenant-1", None, None).await?;
let files = client.list_files("tenant-1", "assets", Some(100), None).await?;
```

## Tenants

```rust
let mut cursor: Option<String> = None;
loop {
    let page = client.list_tenants(Some(100), cursor.as_deref()).await?;
    for tenant_id in &page.items {
        println!("tenant = {tenant_id}");
    }
    match page.next_cursor {
        Some(next) => cursor = Some(next),
        None => break,
    }
}
```

`list_tenants` is the only method not scoped to a tenant: it enumerates the
whole deployment from a dedicated service, so expect `PERMISSION_DENIED`
when the credentials are limited to a single tenant.

## Pagination

Every listing RPC (`ListDoc`, `ListCollections`, `ListGraphs`,
`ListNodes`, `NeighborsOut`/`NeighborsIn`, `ListBuckets`, `ListFiles`,
`ListTenants`, and `FindByField`/`QueryDoc`) shares the same rules:

- `limit == 0` is rejected client-side, immediately, with a clear error —
  no round trip to the server (`page limit must be greater than zero`).
- The server enforces its own ceiling, `limits.max_page_size`, **200 by
  default but configurable per deployment**. The SDK does *not* duplicate
  this ceiling client-side: any `limit >= 1` is forwarded unchanged, and the
  server has the final say (rejecting anything above its configured ceiling
  with `INVALID_ARGUMENT`).
- When `limit` is `None`, the SDK sends **20** — note this is the SDK's own
  default, not the server's own default of 50. `PageRequest.limit` is an
  `optional` protobuf field, so leaving it off the wire is distinguishable
  from an explicit `0` and is what makes the server apply its own default;
  the SDK does not use that, and always sends an explicit `PageRequest`
  with an explicit limit. The page size you get therefore never depends on
  the server's configuration.
- **`next_cursor` empty (`None` in this SDK's `Page`/`NeighborPage` types) is
  the only end-of-list signal.** A page can legitimately be short, or even
  completely empty, in the middle of a listing (for example, an index entry
  surviving a document or node that was since deleted) while still carrying
  a fresh `next_cursor` — do not stop just because a page had few or no
  items; only stop when `next_cursor` comes back absent.
- One caveat in the other direction: when the total count is an exact
  multiple of `limit`, the last full page still carries a cursor, and the
  next call returns an empty page with no cursor. This is expected, not a
  bug — the server has no way to know it just handed out the last item.
- Cursors are opaque: never construct or parse one, only pass back what the
  server gave you. Do not persist a cursor across sessions; it is only
  meant to live for the duration of one pagination pass.

`list_collections`'s `count` on each `CollectionInfo` is a maintained
counter, free to read regardless of collection size — the natural starting
point for a dashboard, since it gives both structure and volume in one call:

```rust
let collections = client.list_collections("tenant-1", Some(50), None).await?;
for info in &collections.items {
    println!("{} holds {} documents", info.collection, info.count);
}
```

## Document Query Rules

A few server-side validations are easy to trip over and are not
re-checked client-side (except where noted), so they surface as
`INVALID_ARGUMENT` or `NOT_FOUND` from the RPC itself:

| RPC / method | Rule |
|---|---|
| `search_documents` (`FindByField`) | `value` must serialize to a JSON **scalar** (a string, number, bool, or null) — `"active"`, `42`, `true`, `null`. An object or array is rejected with `INVALID_ARGUMENT`. |
| `put_node`/`put_nodes` (`PutNode`) | `value` must serialize to a JSON **object**, never a scalar or an array. |
| `add_edge`/`add_edges` (`AddEdge`) | Fails with `NOT_FOUND` if either `from` or `to` does not already exist as a node in the graph. Create both endpoint nodes first. |
| `add_edge`/`add_edges` (`AddEdge`, uniqueness) | A `(from, label, to)` triplet names **at most one edge**: the adjacency index is keyed by the triplet and does not carry the edge id. A *second* edge over a triplet another edge already holds fails with `ALREADY_EXISTS`. Reusing the **same** `edge_id` over its own triplet is allowed and replaces its properties — which is what makes replaying a successful `add_edge` harmless. |
| `delete_edge`/`delete_edge_with_request_id` (`DeleteEdge`) | **Idempotent**, like `delete_document`/`delete_file`: deleting an edge that is not there succeeds and touches nothing. No deletion in this API reports a missing target, so a wrong `edge_id` is not reported either — read the edge first (`get_edge`, `neighbors_out`, `neighbors_in`) when you need to know whether it existed. |
| `get_edge`/`get_edge_as` (`GetEdge`) | Returns `NOT_FOUND` for an unknown `edge_id`. An edge id is scoped to one `(tenant_id, graph)` pair: the same id under another graph, or for another tenant, is a different edge and therefore absent. |
| `query_documents` (`QueryDoc`, `Contains` operator) | Case-insensitive substring match, but a `Contains` term shorter than **3 characters is not indexable**. A query where *no* filter is indexable is refused with `INVALID_ARGUMENT` rather than served by a full scan — pair a short term with an `Eq` or `In` filter on another field. |
| `list_documents` (`ListDoc`) vs `query_documents` (`QueryDoc`) | The `u64` total count returned alongside the results is **free** on `list_documents` (a maintained counter) but **costly** on `query_documents` (computed after full filtering). Prefer `list_documents` when there is nothing to filter, and never call `query_documents` in a loop just to get a count. |
| `create_document`/`put_document`, `put_node`, `add_edge` (`json` payload) | The encoded JSON payload must not exceed the server's `limits.max_doc_bytes`, **2 MiB by default**. Larger payloads are rejected with `INVALID_ARGUMENT`. |

Filters passed to `query_documents` combine with logical AND; results are
always tie-broken by document id, so ordering is total and stable across
pages.

`create_document`'s document-then-node write is **not atomic**: if the node
write fails after the document write succeeds, the document is left without
its graph binding — callers that need both or neither must handle that
themselves (for example by retrying the node write, or by treating a
document without its expected node as needing repair).

## Tenancy and Authorization Scopes

**`tenant_id` is a business-level partition, not a security boundary.**
It is not derived from any caller identity — any authenticated client can
address any tenant. Enforcing which caller may touch which tenant is the
calling application's responsibility, not the server's.

Two token scopes matter for this SDK:

- **read-only** — gets `PERMISSION_DENIED` on the 7 write RPCs:
  `create_document`/`put_document`, `delete_document`, `put_node`/`put_nodes`,
  `add_edge`/`add_edges`, `delete_edge`, `upload_file`/`upload_file_stream`,
  and `delete_file`. Every read method (documents, graph, files, tenants)
  works normally, which is enough to build a full read-only exploration
  console.
- **admin** — the scope used to create and rotate `rocia-idp` service
  accounts through its own account-management API — is refused on **all 23
  RPCs**, reads included. It has no business talking to the data plane: an
  admin-scoped token calling any method on `RociaDbClient` gets
  `PERMISSION_DENIED`. If a *read* call unexpectedly returns
  `PERMISSION_DENIED`, this is almost always the cause: double-check which
  `client_id` produced the token, since the data account and the admin
  account are two distinct credentials.

## Error Handling

Public methods return `rociadb_sdk::Result<T>`, an alias for
`std::result::Result<T, RociaDbError>`. `RociaDbError` is a typed enum,
not a boxed `dyn Error`, so callers can `match` on the failure kind
directly instead of downcasting:

- `Status { operation, status }` — a gRPC call to the upstream server
  returned a non-OK status; carries the raw `tonic::Status`, so nothing is
  lost compared to calling the generated client directly. `.code()`
  returns the `tonic::Code`; `.reason()` returns the server's `reason`
  trailing-metadata value (`invalid_argument`, `not_found`,
  `already_exists`, `permission_denied`, `unauthenticated`, `internal`) —
  finer-grained than the code alone; `.status()` returns the raw
  `tonic::Status` for anything not covered by the two accessors above.
  `.is_unauthenticated()` and `.is_permission_denied()` are shorthands for
  the two codes that matter most for retry logic (see
  [`UNAUTHENTICATED` vs `PERMISSION_DENIED`](#unauthenticated-vs-permission_denied)).
- `Connection { .. }` — failed to connect to, or configure, the upstream
  endpoint (invalid host, TLS setup, connection refused, missing builder
  configuration).
- `Auth { .. }` — failed to obtain or refresh the upstream auth token.
- `Encode { context, .. }` / `Decode { context, .. }` — failed to encode a
  value as JSON before sending it upstream, or failed to decode a JSON
  payload received from upstream; wraps the underlying `serde_json::Error`.
- `Validation(String)` — a client-side rule was violated before any
  network call (a zero page limit, a checksum of the wrong length, an
  incomplete `node_label`/`node_graph` pair, a file size out of bounds,
  etc).

Every variant implements `std::error::Error`, so `RociaDbError` composes
normally with `?` inside a function returning `anyhow::Result<...>` (or any
other error type with a blanket `From<E: std::error::Error>`).

## API Conventions

Document reads deserialize directly to the requested type. For graph reads, use
`get_node_as::<T>`, `get_edge_as::<T>`, `get_outgoing_neighbor_nodes::<T>`, or
`get_incoming_neighbor_nodes::<T>` for the same behavior. `get_node` and
`get_edge` remain convenience methods returning `serde_json::Value` (the
latter inside an `Edge<serde_json::Value>`).

### Idempotency keys

Mutating helpers generate a unique `request_id` by default. Use the corresponding
`*_with_request_id` method when retries must reuse a stable idempotency key. Batch
operations generate one key per item, via `NodeInput::request_id` /
`EdgeInput::request_id` — `None` to auto-generate, `Some(..)` to control it
yourself. The auto-generated prefix identifies which call produced it —
`put_document:{collection}:<uuid>` for a document write, `put_node:<uuid>`
for a node write, a bare UUID (no prefix) for an edge write — which is
useful when inspecting or logging `request_id` values, though the exact
prefix is not part of the public contract and should not be parsed.

`create_document_with_request_id` is that sibling for `create_document`:
`request_id` applies **only** to the document write (the `PutDoc` call) —
the graph node binding, when `node_label`/`node_graph` are both `Some`,
keeps generating its own key exactly as `create_document` already does.
Unlike `create_document` (which takes an already-serialized
`serde_json::Value`, to avoid a breaking signature change), this sibling is
generic over any `Serialize` type, consistent with
`put_document_with_request_id`/`put_node_with_request_id`/
`add_edge_with_request_id`. Deletions have siblings too —
`delete_document_with_request_id`, `delete_edge_with_request_id` and
`delete_file_with_request_id`:

```rust
use serde_json::json;

client
    .create_document_with_request_id(
        "tenant-1",
        "products",
        "sku-123",
        &json!({"sku": "sku-123", "label": "Widget"}),
        Some("product".to_string()),
        Some("products".to_string()),
        "reindex-job-42:sku-123",
    )
    .await?;
```

Idempotency is scoped to `(tenant, operation, request_id)`: the same
`request_id` reused across two *different* operations (a `put_document`
then a `delete_document`, say) does not cancel or dedupe against each
other. Idempotency markers expire after the server's `gc.request_ttl_secs`,
**24 hours by default** — a replay after that window re-executes.

### `Page`, `DocumentPage`, `NeighborPage`, and `Edge`

Every listing method returns a named struct, never a bare tuple:
`Page<T>` (`items`, `next_cursor`) when there is no total to
report, and `DocumentPage<T>` (`items`, `next_cursor`, `total_count`)
for the three document-query methods that also report how many results
matched overall — `search_documents`, `list_documents`, and
`query_documents`. The one exception is naming, not shape: `neighbors_out`/
`neighbors_in` return `NeighborPage`, the same `next_cursor`-terminated page
but with the field named `neighbors` instead of `items`, since it carries
raw `Neighbor` records rather than a generic `T` — see
[Neighbors](#neighbors).

Single-item graph reads are named too: `get_edge`/`get_edge_as::<T>` return
`Edge<T>` (`edge_id`, `from`, `to`, `label`, `value`), the five fields
`add_edge` takes, so an edge read back round-trips into a write — see
[Reading an edge back](#reading-an-edge-back). `edge_id` is echoed from the
argument the read was made with; the server does not repeat it in the
response.

### The `pb` module

The generated protobuf/gRPC types live in the `pb` module, but that
module is **not** part of the SDK's semver contract — a routine prost or
tonic upgrade can reshape it without the SDK's own API changing at all.
The handful of generated types that do appear in a public method signature
are re-exported individually at the crate root instead: `CollectionInfo`,
`StatResponse`, `Neighbor`, `UploadRequest`, `DownloadResponse`. Depend on
those re-exports, not on paths reaching into `pb` directly.

## Parity with the TypeScript SDK

This SDK and the TypeScript SDK
([`rociadb-core-sdk-ts`](https://github.com/RociaDB/rociadb-core-sdk-ts))
cover the same 23 RPCs against the same server, and are maintained to the
same standard: **every capability available in one is available in the
other.**

> **Temporarily out of parity.** `GetEdge` is new in the upstream
> `.proto`. It is wrapped here as `get_edge`/`get_edge_as`; the TypeScript
> SDK does not expose it yet. This is a gap to close in
> [`rociadb-core-sdk-ts`](https://github.com/RociaDB/rociadb-core-sdk-ts),
> not a deliberate difference — remove this note once it ships there.

Neither imitates the other's syntax — this crate stays
snake_case/`Result`-idiomatic Rust, the TypeScript package stays
camelCase/exception-idiomatic TypeScript — and the two languages'
structural differences (ownership vs. garbage collection, exhaustive enum
matching vs. discriminated unions, RAII vs. explicit `close()`) legitimately
shape each SDK differently where a mechanical, character-for-character
translation would fight the language. Parity is about what you can *do*,
not about matching method names or shapes one-to-one, and most names do
translate mechanically (`put_nodes` ↔ `putNodes`,
`get_outgoing_neighbor_nodes` ↔ `getOutgoingNeighborNodes`, and so on). The
handful of places where a name or shape does **not** translate
mechanically — where translating a call by ear lands you on the wrong
method — are the table below.

The two are packaged and versioned independently: this crate is
`rociadb-sdk` on crates.io, the TypeScript package is `rocia-db-sdk` on
npm, and their version numbers advance separately. Neither the differing
package name nor a gap in major version is a signal of a capability
difference — the table below is the source of truth for parity.

| Capability | Rust ([`rociadb-core-sdk-rust`](https://github.com/RociaDB/rociadb-core-sdk-rust)) | TypeScript ([`rociadb-core-sdk-ts`](https://github.com/RociaDB/rociadb-core-sdk-ts)) | Note |
|---|---|---|---|
| Assisted streaming upload — re-chunks to the 1 MiB wire contract, validates the total, caller supplies the checksum | `upload_file_chunked` | `uploadFileStream` | Names do **not** correspond — see below. |
| Raw streaming upload — zero validation, caller builds every protobuf message | `upload_file_stream` | `uploadFileRaw` | Names do **not** correspond — the mirror image of the row above. |
| Idempotency key scoped to a `create_document` call's document write only (the graph node binding keeps its own auto-generated key) | `create_document_with_request_id` — a sibling method, `request_id: impl Into<String>` | `createDocument(..., { requestId })` — an options-object field | Same capability, different shape: a sibling method vs. an options field, the established pattern on each side. |
| Releasing the connection and the background token-refresh task | Drop the last live `RociaDbClient` clone | `client.close()` | No Rust method by design — see below. |
| Lazy token invalidation, at the level of the background refresh task itself (not the `RociaDbClient`-level wrapper, which *does* translate mechanically: `invalidate_auth_token` ↔ `invalidateToken`) | `TokenManager::request_refresh` | `TokenManager.invalidate()` | Different verb chosen independently on each side for the same "mark it stale, wake the background task, do not block" idea. |
| Standalone OAuth2 token fetch, usable outside of `TokenManager` | `auth::fetch_token` | `fetchOAuthToken` (exported from `auth.ts`, re-exported at the package root) | TypeScript needed a name that does not collide with the `fetch` Web API it wraps; Rust has no such collision. |
| Discriminating why an `Err` happened | `RociaDbError` — a `match`-able enum: `Status { .. }` / `Connection { .. }` / `Auth { .. }` / `Encode { .. }` / `Decode { .. }` / `Validation(String)` | `RociaDbError.kind: RociaDbErrorKind`, one class with a `"status" \| "connection" \| "auth" \| "encode" \| "decode" \| "validation"` field | Different shape, not just a different name — see below. |
| Escape hatch to the raw generated protobuf/gRPC types, to build a custom client against the same `.proto` | **none** — the generated module is private (`pub(crate) mod pb`). The generated types that reach a public signature are re-exported at the crate root instead: `CollectionInfo`, `StatResponse`, `Neighbor`, `UploadRequest`, `DownloadResponse`, and `Streaming` | the `rocia-db-sdk/proto` subpath export | **A real capability gap, not a naming difference.** TypeScript lets a caller reach every generated type; Rust deliberately does not, because `1.0.0` puts the crate's public surface under semantic versioning and generated code is reshaped by any prost or tonic upgrade. Reopen this if a Rust consumer needs it — it would be an addition, not a removal. |

**The error-kind trap, spelled out:** both sides recognize the exact
same six causes, in the same order, but represent the choice differently.
Rust's `RociaDbError` is a real sum type, marked `#[non_exhaustive]` so a
later release can recognize a new cause without breaking existing callers:
a match must carry a wildcard arm, and the compiler flags a missing one
among the causes it does name. TypeScript keeps a single
`RociaDbError` class (so an existing `instanceof RociaDbError` check never
breaks) and puts the same six-way choice in a `kind` field instead —
narrowing on `error.kind` gets you the same exhaustiveness check from
`tsc`, just via a discriminated union instead of a variant match. Neither
representation is "the same code translated"; each is the idiomatic way to
express one closed set of causes in its own language.

**The upload naming trap, spelled out:** `upload_file_chunked` (Rust)
and `uploadFileStream` (TypeScript) are the *same* capability — the middle
tier that re-chunks and validates for you (see
[Streaming an upload without buffering the whole file](#streaming-an-upload-without-buffering-the-whole-file)).
`upload_file_stream` (Rust) and `uploadFileRaw` (TypeScript) are also the
*same* capability — the raw, zero-validation escape hatch. `upload_file_stream`
and `uploadFileStream` are **not** each other's counterpart, despite the
near-identical name: the Rust one is the raw escape hatch, the TypeScript
one is the validated middle tier. Porting upload code between the two SDKs
by matching names alone silently swaps which tier you land on.

**Why there is no Rust `close()`:** `RociaDbClient` is `Clone`, and
every clone shares one underlying channel and one background refresh task
by design (see [Authentication](#authentication)) — a `close(&self)` taking
`&self` would tear the channel down out from under every other live clone,
silently breaking that documented guarantee. The idiomatic Rust equivalent
already exists and gives the identical guarantee: drop the last clone.
`tonic::transport::Channel` is itself cheap to clone and shares one real
connection underneath, so this is not a weaker substitute — it is the same
guarantee, spelled the Rust way (RAII instead of an explicit call).

One capability is intentionally kept on one side without a mirror on the
other: having both `RociaDbBuilder::build()` and a direct
`RociaDbClient.connect()` entry point (TypeScript only — the builder there
is a thin wrapper with no capability of its own, so duplicating a second
entry point in Rust would add an API to maintain for zero new
capability).

## Development

```bash
PROTOC=/usr/bin/protoc cargo build
PROTOC=/usr/bin/protoc cargo fmt --all -- --check
PROTOC=/usr/bin/protoc cargo clippy --all-targets --all-features -- -D warnings
PROTOC=/usr/bin/protoc cargo test
```

`PROTOC` only needs to point at a real `protoc` binary; `mise install`
(see `mise.toml`) installs a pinned one for you at
`~/.local/share/mise/installs/protoc/35.0/bin/protoc`, in which case the
`PROTOC=` prefix above is not needed.

Add focused unit tests in `#[cfg(test)] mod tests` next to the code they
cover, and public API scenarios under `tests/`; keep tests deterministic
and independent of a live RociaDB or OAuth2 service. Run formatting,
Clippy, and tests before submitting changes — see
[`AGENTS.md`](https://github.com/RociaDB/rociadb-core-sdk-rust/blob/main/AGENTS.md)
for the full
contribution guidelines, including commit and pull-request conventions and
the canonical location for `.proto` changes.

Licensed under Apache-2.0.
