# rociadb-sdk

Rust client SDK for RociaDB's gRPC upstream services: documents, graph,
files and tenants — 23 RPCs behind one typed, async client.

The crate is a thin, typed wrapper around the generated gRPC clients. It
handles connection setup, OAuth2 client-credentials authentication with
automatic token refresh, JSON encoding and decoding of payloads, pagination
bookkeeping, and the file-upload wire contract, so callers work with plain
Rust types (`serde_json::Value`, or any `Serialize` / `DeserializeOwned`
type of their own) instead of hand-building protobuf messages.

The Node.js/TypeScript SDK lives in its own sibling repository
([`rociadb-core-sdk-ts`](https://github.com/RociaDB/rociadb-core-sdk-ts)),
not in this checkout — there is no `typescript/` directory here.

## Guides

The deep dives live in [`docs/`](docs/):
[authentication](docs/authentication.md) ·
[errors and retries](docs/errors-and-retries.md) ·
[documents](docs/documents.md) ·
[graph](docs/graph.md) ·
[files](docs/files.md) ·
[pagination](docs/pagination.md) ·
[tenancy and authorization](docs/tenancy.md) ·
[transport and TLS](docs/transport.md) ·
[parity with the TypeScript SDK](docs/typescript-parity.md).

## Installation

```toml
[dependencies]
rociadb-sdk = "2"
```

**No system dependency is required.** The build script compiles the bundled
`.proto` with [`protox`](https://docs.rs/protox), a pure-Rust protobuf
compiler, so there is no `protoc` binary to install and no `PROTOC`
environment variable to set — on a developer machine, in CI, or on docs.rs.
The Google well-known types the API imports come from `protox` itself.

`Cargo.toml` declares `rust-version = "1.88"`, the minimum toolchain this
crate is built and tested against. The floor comes from the dependency
graph, not from `edition = "2024"`; the comment above that field records
which dependencies pin it.

This is a standalone crate, not a workspace member. To work against an
unreleased change, depend on a checkout or on git:

```toml
[dependencies]
rociadb-sdk = { path = "../rociadb-core-sdk-rust" }
# or: rociadb-sdk = { git = "https://github.com/RociaDB/rociadb-core-sdk-rust", tag = "v2.0.0" }
```

For a runnable project that wires the SDK up end to end, see
[`example-rust-project`](https://github.com/RociaDB/example-rust-project).

## Quick start

```rust,no_run
use rociadb_sdk::{
    DocumentWriteOptions, EdgeInput, FileUploadOptions, NodeBinding, RociaDbBuilder,
    WriteOptions,
};
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

// Write a document, and bind a graph node to it in the same call.
client
    .put_document(
        "tenant-1",
        "products",
        "sku-123",
        &json!({"sku": "sku-123", "label": "Widget"}),
        DocumentWriteOptions::new()
            .with_node_binding(NodeBinding::new("product", "catalog")),
    )
    .await?;

// Read it back into any DeserializeOwned type.
let product: serde_json::Value =
    client.get_document("tenant-1", "products", "sku-123").await?;
println!("product = {product}");

// Write a second node and relate the two.
client
    .put_node(
        "tenant-1",
        "catalog",
        "group:grp-1",
        &json!({"name": "Widgets"}),
        WriteOptions::new(),
    )
    .await?;
client
    .add_edge(
        "tenant-1",
        "catalog",
        EdgeInput::new(
            "edge-1",
            "product:sku-123",
            "group:grp-1",
            "belongs_to",
            json!({"weight": 1}),
        ),
    )
    .await?;

// Upload a file: the SHA-256 digest and the 1 MiB chunking are handled here.
client
    .upload_file(
        "tenant-1",
        "assets",
        "manual.txt",
        b"hello RociaDB".to_vec(),
        FileUploadOptions::new().with_content_type("text/plain"),
    )
    .await?;
# Ok(())
# }
```

`.host(..)` above is a plaintext local address for illustration; see
[transport and TLS](docs/transport.md) for what to use against a real
deployment.

`RociaDbClient` is `Clone`, and every clone shares one channel, one token
manager and one background refresh task. Each method takes `&self` and
clones the cheap, `Arc`-backed inner service client before issuing its RPC,
so a client shared behind an `Arc` needs no `Mutex`. There is no `close()`:
drop the last live clone to release the connection and stop the refresh
task.

## Conventions

### One method per operation

There is exactly one method per server operation. Everything optional
travels in an options or input struct passed as the last argument — never in
a `_with_request_id` / `_with_node_binding` / `_as` sibling method. Every
such struct is `#[non_exhaustive]`, built with a `new` constructor plus
chainable `with_*` setters, and exposes its fields for reading:

- `WriteOptions` for a write whose only tunable is its idempotency key:
  `delete_document`, `put_node`, `delete_edge`, `delete_file`.
- `DocumentWriteOptions` for `put_document`, which can also bind a graph
  node (`with_node_binding(NodeBinding::new(label, graph))`).
- `FileUploadOptions` and `FileStreamUploadOptions` for the two ergonomic
  upload paths.
- `NodeInput` and `EdgeInput` carry one node or edge to write, by name
  rather than as a run of same-typed positional arguments.

Reads that decode a payload are generic over the target type:
`get_document::<Product>(..)`, `get_node::<Value>(..)`,
`get_edge::<Weight>(..)`. Pagination stays positional
(`limit: Option<u32>`, `cursor: Option<&str>`), and `tenant_id: &str` is the
first argument of every tenant-scoped call.

### Idempotency keys

The server deduplicates a write on `(tenant, operation, target,
request_id)`. Left unset, the SDK mints a fresh `"<operation>:<uuid>"` key
per call, which protects against a network replay of one call but not
against the caller issuing the same logical write twice. Supply your own —
and reuse it on every retry — whenever a replay after a timeout must not
write twice:

```rust,no_run
# use rociadb_sdk::{DocumentWriteOptions, RociaDbBuilder, WriteOptions};
# use serde_json::json;
# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
client
    .put_document(
        "tenant-1",
        "products",
        "sku-123",
        &json!({"sku": "sku-123"}),
        DocumentWriteOptions::new().with_request_id("reindex-job-42:sku-123"),
    )
    .await?;

client
    .delete_document(
        "tenant-1",
        "products",
        "sku-124",
        WriteOptions::new().with_request_id("cleanup-7:sku-124"),
    )
    .await?;
# Ok(())
# }
```

`WriteOptions`' own documentation is the single place that lists the
generated key of every write. Markers expire after the server's
`gc.request_ttl_secs`, 24 hours by default.

### Pagination

Every paginated read returns `Page<T>` (`items`, `next_cursor`), except the
three document reads that also report a total, which return
`DocumentPage<T>` (`items`, `next_cursor`, `total_count`). **An absent
`next_cursor` is the only end-of-list signal** — a page can legitimately be
short or empty mid-listing:

```rust,no_run
# use rociadb_sdk::RociaDbBuilder;
# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
let mut cursor: Option<String> = None;
loop {
    let page = client
        .list_documents::<serde_json::Value>("tenant-1", "products", Some(100), cursor.as_deref())
        .await?;
    for document in &page.items {
        println!("{document}");
    }
    match page.next_cursor {
        Some(next) => cursor = Some(next),
        None => break,
    }
}
# Ok(())
# }
```

See [pagination](docs/pagination.md) for limits, cursor scoping and the
cost of `total_count`.

### Typed errors

Every public method returns `rociadb_sdk::Result<T>`, an alias for
`std::result::Result<T, RociaDbError>`. `RociaDbError` is a `match`-able
`#[non_exhaustive]` enum — `Status`, `Config`, `Connection`, `Auth`,
`Encode`, `Decode`, `Validation`, `ChecksumMismatch`, `SizeMismatch` — with
`code()`, `reason()` and `status()` accessors plus predicates for the codes
worth branching on:

```rust,no_run
# use rociadb_sdk::RociaDbBuilder;
# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
match client
    .get_document::<serde_json::Value>("tenant-1", "products", "sku-123")
    .await
{
    Ok(document) => println!("{document}"),
    // "absent" is usually a value in the caller's domain, not a failure.
    Err(error) if error.is_not_found() => println!("no such product"),
    // Valid token, missing scope: a refresh will not help.
    Err(error) if error.is_permission_denied() => return Err(error),
    Err(error) => return Err(error),
}
# Ok(())
# }
```

The full set is `is_unauthenticated`, `is_permission_denied`,
`is_not_found`, `is_invalid_argument`, `is_already_exists` and
`is_aborted`. See [errors and retries](docs/errors-and-retries.md).

### Retrying `ABORTED`

`ABORTED` is the one status the server expects every caller to replay — on
reads as well as writes. `RetryPolicy` and `RociaDbClient::retry` implement
that loop with exponential backoff and full jitter. Build the idempotency
key **once, outside the closure**, so every attempt carries the same one:

```rust,no_run
# use rociadb_sdk::{RetryPolicy, RociaDbBuilder, WriteOptions};
# use serde_json::json;
# use std::time::Duration;
# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
let policy = RetryPolicy::new()
    .with_max_attempts(5)
    .with_base_delay(Duration::from_millis(50));
let options = WriteOptions::new().with_request_id("import-batch-7:node-1");

client
    .retry(&policy, || {
        let options = options.clone();
        async {
            client
                .put_node("tenant-1", "catalog", "node-1", &json!({"ok": true}), options)
                .await
        }
    })
    .await?;
# Ok(())
# }
```

### Authentication happens on its own

`RociaDbBuilder` enables OAuth2 client-credentials auth by default, reading
`AUTH_TOKEN_URL`, `AUTH_CLIENT_ID` and `AUTH_CLIENT_SECRET` from the
environment unless `auth_client_credentials` supplies them. `build()`
fetches the first token; from then on a background task refreshes it before
it expires, and **every unary RPC answered `UNAUTHENTICATED` refreshes the
token and retries itself exactly once**. Seeing that status therefore means
the refreshed credential was rejected too. A streaming call refreshes up front
if its token is nearly expired, and `upload_file` and the call that opens a
download retry once as well; only `upload_file_chunked` and
`upload_file_stream` are left to retry by hand, since the SDK cannot re-drain a
stream you handed over. Credentials and tokens are held
as `SecretString`, so they are redacted by every formatter and zeroized on
drop. `disable_auth()` turns the whole mechanism off for a controlled local
deployment. See [authentication](docs/authentication.md).

### Timeouts

`connect_timeout` bounds the dial (10 seconds by default) and
`request_timeout` puts a deadline on every unary RPC (opt-in, no default).
A deadline that expires fails with a `Status` error whose `code()` is
`DeadlineExceeded`. The streaming upload and download paths are deliberately
not covered — wrap those in a `tokio::time::timeout` of your own:

```rust,no_run
use rociadb_sdk::RociaDbBuilder;
use std::time::Duration;

# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
let client = RociaDbBuilder::new()
    .host("https://rociadb.internal:443")
    .connect_timeout(Duration::from_secs(3))
    .request_timeout(Duration::from_secs(10))
    .build()
    .await?;
# let _ = client;
# Ok(())
# }
```

## Transport and TLS

RociaDB does not implement TLS itself and never will: the server always
listens in plaintext. In production TLS terminates at a reverse proxy in
front of it, the SDK connects to that proxy with `https://` (typically port
443), and the proxy forwards plaintext gRPC to the backend. The proxy must
be HTTP/2 end to end — a TLS-to-HTTP/1.1 downgrade toward the backend makes
every call fail, usually with `UNAVAILABLE`.

`tls_config` replaces the default native-roots setup (a private CA, mTLS, or
an overridden verification domain), `http2_keep_alive` turns on HTTP/2
keep-alive pings, and `build_with_channel` takes a `Channel` you built
yourself. Full details in [transport and TLS](docs/transport.md).

## Development

```bash
cargo build
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
cargo deny check
```

Nothing beyond a Rust toolchain is needed; `mise install` (see `mise.toml`)
installs the pinned one. `cargo test` runs the unit tests, the integration
suite against an in-process gRPC server and mock identity provider under
`tests/`, and the doctests — including every example in this README and in
`docs/`, which are compiled through `include_str!` from `src/lib.rs`.

See [`AGENTS.md`](AGENTS.md) for the full contribution guidelines, including
commit and pull-request conventions and the canonical location for `.proto`
changes, and [`CHANGELOG.md`](CHANGELOG.md) when upgrading.

Licensed under Apache-2.0.
