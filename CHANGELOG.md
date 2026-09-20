# Changelog

All notable changes to this crate are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - Unreleased

A breaking release. The public API is restructured around one method per
operation; authentication, transport and resilience gain the controls a
production deployment needs; the build no longer requires `protoc`; and the
documentation is rewritten and compiled as doctests. Nothing is deprecated —
the 1.0 names are gone, and the table below maps every one of them.

### Breaking

- **The public API is restructured around one method per operation.**
  Optional per-call parameters now travel in an options or input struct
  passed last, replacing the `_with_request_id` / `_with_node_binding` /
  `_as` sibling methods. Pagination keeps its positional
  `limit: Option<u32>, cursor: Option<&str>` convention, and
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
  | `upload_file_chunked` with `chunks: Stream<Item = Vec<u8>>` | `chunks: Stream<Item = std::io::Result<bytes::Bytes>>` — pass a `tokio_util::io::ReaderStream` straight in, or wrap each piece as `Ok(Bytes::from(piece))` |
  | `stat_file(t, b, f) -> StatResponse` | `stat_file(t, b, f) -> FileMetadata` — the three data fields keep their names and types; `created_at` / `updated_at` are now `FileTimestamp`, whose `as_str()` is the string 1.0 handed over |
  | `delete_file(t, b, f)` | `delete_file(t, b, f, WriteOptions::new())` |
  | `delete_file_with_request_id(t, b, f, rid)` | `delete_file(t, b, f, WriteOptions::new().with_request_id(rid))` |
  | `FileUploadOptions { checksum: Option<Vec<u8>>, .. }` | `FileUploadOptions::new().with_checksum([u8; 32])` |
  | `FileStreamUploadOptions::default()` | `FileStreamUploadOptions::new(size_bytes, checksum)` — no `Default`, since neither of those two fields has a sensible one |
  | `NeighborPage` | `Page<Neighbor>` |
  | `rociadb_sdk::file::*`, `rociadb_sdk::graph::*` | `rociadb_sdk::*` (`file` and `graph` are private; `auth` is the only public module) |
  | `builder.host(..)` and the other setters: `&mut self -> &mut Self` | `self -> Self`: `RociaDbBuilder::new().host(..).disable_auth().build().await?`, and `let b = RociaDbBuilder::new().host(..);` now compiles |

- **`upload_file_chunked` takes a fallible chunk stream of `Bytes`.** Its
  `chunks` parameter is now
  `S: Stream<Item = std::io::Result<bytes::Bytes>> + Send + 'static` instead of
  `Stream<Item = Vec<u8>>`, and `bytes = "1"` is a new **regular dependency**
  because that item type is part of the public signature (`Bytes` is
  re-exported at the crate root, with the same stability caveat as `Streaming`
  and `Channel`, so no caller needs `bytes` as a direct dependency). Two things
  this buys, both of which the old signature made impossible:

  - a `tokio_util::io::ReaderStream` over a `tokio::fs::File`, a socket or a
    decompressor yields exactly this item, so it is passed in with **no**
    adaptation — no `map`, no `to_vec`, no collecting;
  - a read that fails partway through has somewhere to say so. It now fails the
    upload with the new `RociaDbError::Io`; previously a failing source could
    only end the stream early, and the upload was reported as a `Validation`
    size mismatch that blamed the caller's `size_bytes` for a disk that could
    not be read.

  Existing callers holding `Vec<u8>` pieces wrap each one:
  `stream::iter(pieces.into_iter().map(|p| Ok(Bytes::from(p))))`. The memory
  bound is unchanged — never more than one outgoing chunk buffered — and a
  `Bytes` item is still copied into that buffer, as any other bytes would be;
  the type is for what it makes easy at the call site, not to make the upload
  zero-copy. `bytes` is not a new crate in the dependency graph: `tonic`,
  `prost`, `h2` and `reqwest` all already pull it in.
- **`FileUploadOptions` and `FileStreamUploadOptions` are now
  `#[non_exhaustive]`.** In 1.0 both were plain structs assembled with a
  literal and `..Default::default()`; that no longer compiles from outside
  the crate. Build them with `new()` plus the chainable `with_*` setters
  instead. Their fields stay `pub` for reading, and both now derive
  `PartialEq + Eq`. Every other option, input and page type was already
  `#[non_exhaustive]` in 1.0.
- **A checksum is `[u8; 32]`, not `Vec<u8>`.** `FileUploadOptions::checksum`
  is `Option<[u8; 32]>` and `FileStreamUploadOptions::checksum` is
  `[u8; 32]`, so the length is checked by the compiler and the runtime
  "checksum must be exactly 32 bytes" `Validation` error is gone. The
  SHA-256 digest is still computed for you when `FileUploadOptions::checksum`
  is `None`. Callers holding a `Vec<u8>` convert with
  `<[u8; 32]>::try_from(v.as_slice())`.
- **`stat_file` returns the SDK's own `FileMetadata`, and `StatResponse` is no
  longer re-exported.** The three data fields are unchanged in name and type
  (`size_bytes: u64`, `content_type: String`, `checksum: Vec<u8>`), so only code
  that *named* the response type, or that read the timestamps as strings, has to
  change:

  | 1.0 | 2.0 |
  | --- | --- |
  | `let stat: StatResponse = client.stat_file(..).await?;` | `let metadata: FileMetadata = client.stat_file(..).await?;` |
  | `stat.created_at` (a `String`) | `metadata.created_at.as_str()`, or `metadata.created_at` wherever a `Display` will do |
  | parse `stat.updated_at` yourself | `metadata.updated_at.system_time()?` — see the Added section |

  The reason for the change is the timestamps. Both are `string` on the wire and
  **nothing in the `.proto` states their format**, so the SDK cannot offer a
  parsed type without betting every `stat_file` call on a guess about the server.
  An SDK-owned struct lets them be a type that keeps the raw string and parses
  only when asked, which costs a caller one accessor — not the call — when the
  server formats them some other way. The generated types re-exported at the
  crate root are down to four (`CollectionInfo`, `Neighbor`, `UploadRequest`,
  `DownloadResponse`), and the English documentation the build script used to
  attach to `StatResponse` and its fields now lives on `FileMetadata`.
- **`add_edge` now defaults its `request_id` to `add_edge:<uuid>`** instead
  of a bare UUID with no prefix — on the single-item and batch paths alike.
  It was the only write whose generated key carried no operation prefix. A
  caller that parsed those keys, or replayed one recorded from 1.0, sees a
  different shape; the server's dedup scope already includes the operation,
  so nothing about deduplication changes.
- **Logging levels changed.** The SDK no longer emits `info!` on routine
  success or `error!` on routine failure: a library hands the error back and
  lets the caller decide how to log it. Every RPC now emits exactly one
  `debug!` line with its identifying fields (tenant, collection/graph/bucket,
  ids, counts) and no payloads. The six remaining `warn!` lines are
  `disable_auth()`, a non-`https` token URL, a failed background token
  refresh, a token response with no `expires_in`, a token refresh that
  failed after an `UNAUTHENTICATED` response, and a failed pre-flight token
  refresh before a streaming RPC. A deployment that relied on the
  `info!`/`error!` lines must lower its filter for this crate's target to
  `debug`.
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
- `tokio` is now depended on with only the features the library itself needs
  (`io-util`, `rt`, `sync`, `time`, `macros`); 1.0 asked for
  `rt-multi-thread`, which moved to `[dev-dependencies]`. A consumer that
  relied on this crate enabling `rt-multi-thread` for them — through Cargo's
  feature unification — must now enable it on their own `tokio` dependency.

### Added

- **`RociaDbBuilder::max_decoding_message_size`**, which lifts tonic's 4 MiB
  ceiling on a single decoded message. That ceiling was previously unreachable
  from the public API — `build_with_channel` does not help, because the limit
  lives on the generated clients rather than on the `Channel` — so one document
  over 4 MiB, or a page of twenty averaging 210 KB, failed to decode with no
  recourse, and the wire contract explicitly promises nothing about download
  chunk sizes. Applies to all four services or to none. Leave it unset and
  tonic's default stands.
- **`Code` and `Status` are now re-exported at the crate root.** Both were
  already public API — `RociaDbError::Status` carries a `Status` in a public
  field and `RociaDbError::code` returns a `Code` — so branching on a gRPC code,
  the most common thing a caller does with an error, required taking `tonic` as
  a direct dependency. The same stability caveat as the other re-exports applies.
- `WriteOptions` (`request_id`) and `DocumentWriteOptions` (`request_id`,
  `node_binding`): the per-call options every write now takes. Both are
  `#[non_exhaustive]`, `Debug + Clone + Default + PartialEq + Eq`, with
  `new()` and chainable `with_*` setters. `WriteOptions`' documentation is
  the single place where the generated idempotency key of *every* write is
  stated, and every write links to it.
- `FileUploadOptions::new`, `with_content_type`, `with_checksum`,
  `with_request_id`, and `FileStreamUploadOptions::new(size_bytes, checksum)`,
  `with_content_type`, `with_request_id`.
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
- `RociaDbBuilder::max_file_bytes(u64)`: the largest file `upload_file` and
  `upload_file_chunked` will send. **Defaults to 5 GiB**, unchanged from 1.0
  where the same number was a hard-coded constant — so no existing upload
  starts or stops being rejected. It only **mirrors** the server's configurable
  `limits.max_file_bytes`, which the client cannot read and which always has
  the final say: setting it below the server's limit turns an oversized upload
  into an immediate local `RociaDbError::Validation` instead of a transfer that
  fails after minutes, and setting it above merely moves the failure
  server-side. A zero value is rejected by `build()` and `build_with_channel()`
  with `RociaDbError::Config`, like a zero timeout, before any socket is
  opened. The `Validation` message now names the configured ceiling instead of
  saying "5 GiB"; `upload_file_stream` still validates nothing at all.
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
- `RociaDbClient::download_file_verified_to(tenant_id, bucket, file_id, writer)`
  where `writer: &mut W, W: tokio::io::AsyncWrite + Unpin + ?Sized`: the same
  verification with **no buffer at all**. It stats first, hashes and writes each
  chunk as it arrives, bails the moment the byte count overshoots `size_bytes`
  (without writing that chunk), checks the size then the digest, flushes, and
  returns the number of bytes written — so a 5 GiB file costs one chunk of
  memory rather than 5 GiB. `?Sized` means a `&mut dyn AsyncWrite + Unpin` works
  as well as a `tokio::fs::File`, a `BufWriter`, a socket or a `Vec<u8>`.
  `download_file_verified` is now this method pointed at a `Vec<u8>` (which is
  itself an `AsyncWrite`), so the hashing and both checks exist once; its capped
  pre-allocation is unchanged.

  Two things to know. **A failed verification has already written bytes**: a
  digest is only known at the last byte, so `SizeMismatch` / `ChecksumMismatch`
  arrive with most or all of the file already in your writer, and discarding it
  is yours to do — download to a temporary name and rename on `Ok`. And a
  writer that refuses a chunk, or fails the final flush, surfaces as
  `RociaDbError::Io` with `context: "writing the downloaded file"`. What a match
  proves is exactly what `download_file_verified` proves, and no more.
- `FileMetadata`, the type `stat_file` now returns: `#[non_exhaustive]`,
  `Debug + Clone + PartialEq + Eq`, with `size_bytes`, `content_type`,
  `checksum`, `created_at` and `updated_at` all `pub` for reading. No
  constructor and no setters — it is a value the server produced, not a request
  a caller builds.
- `FileTimestamp`, the type of `FileMetadata::created_at` and
  `FileMetadata::updated_at`: `#[non_exhaustive]`,
  `Debug + Clone + PartialEq + Eq`, wrapping the raw string the server sent.
  - `as_str()` and `Display` hand that string back verbatim, whatever it is.
  - `system_time()` parses it into a `std::time::SystemTime`, and `unix_nanos()`
    into an `i128` of nanoseconds since the epoch. Both parse on every call
    (nothing is cached) and both run one parser, so they accept and reject
    exactly the same strings. `chrono::DateTime<Utc>` and `time::OffsetDateTime`
    each implement `From<SystemTime>`, so either is one hop away; this crate
    depends on neither, and the parser is hand-written with no new dependency.
  - The format is **RFC 3339** — a four-digit date, a `T` (either case) or a
    single space, a 24-hour time, optional fractional seconds of any length (the
    first nine kept, the rest truncated rather than rounded), and a `Z`, `z` or
    `±hh:mm` offset — with every field range-checked against the proleptic
    Gregorian calendar. Anything else is rejected, **a missing offset included**.
    RFC 3339 is what the SDK assumes because it is the conventional wire form
    for an instant carried as a string, not because the schema says so: a server
    writing another format still exposes it through `as_str()`, and only these
    two methods stop working.
  - A failure is `RociaDbError::Decode` with `context` `"file timestamp"`,
    quoting the offending value (bounded in length) and saying what is wrong
    with it. It is raised by the accessor rather than by `stat_file`, so an
    unreadable timestamp never fails a call or hides the rest of the response.
  - Two decisions worth knowing: a **leap second** (`:60`, which RFC 3339
    allows) is rejected, because Unix time has no separate instant to map one to
    and moving it by a second silently would be worse; and an instant **before
    1970** is supported, as `UNIX_EPOCH - Duration` from `system_time()` and as a
    negative number from `unix_nanos()`.
- `RociaDbError::Io { context: &'static str, source: std::io::Error }`, for an
  I/O handle the caller supplied, raised by two calls that its `context` tells
  apart. `"the upload chunk stream"` is `upload_file_chunked` when its `chunks`
  stream yields an `Err` — a read that failed on the caller's own file or
  socket. Nothing further is pulled from the stream, the upload is abandoned (so
  the server, which publishes a file only once it has the whole stream, stores
  nothing), and this error is reported **ahead of** whatever status the server
  returned for the stream that then ended early — the same error-slot precedence
  the size-mismatch check already had. `"writing the downloaded file"` is
  `download_file_verified_to` when the writer it is filling refuses a chunk or
  fails to flush. `Display` folds in the `io::Error`, which stays reachable as
  the `source` for a caller that needs its `kind()`.
- `RociaDbError::ChecksumMismatch { expected: Vec<u8>, actual: Vec<u8> }` and
  `RociaDbError::SizeMismatch { expected: u64, actual: u64 }`, raised only by
  the two verified downloads. Two flat variants rather than one nested
  `Integrity` value, so a caller branches with one match arm per failure and
  reads the numbers straight off it. The size is checked first (a truncated
  transfer fails both, and the byte count is the more actionable report);
  `Display` renders the two digests as lowercase hex, and both carry raw
  bytes so nothing has to be decoded to compare them.
- **Token handling for the two streaming RPCs**, which previously got none at
  all.

  - `TokenManager::ensure_fresh(margin: Duration) -> Result<()>`: refresh the
    cached token only when less than `margin` of its advertised lifetime is
    left, and return immediately otherwise. The manager now records *when* each
    token was fetched alongside the `expires_in` the IdP reported, so what is
    left of a lifetime is a value it can compute; a refresh that is due goes
    through `refresh_now`, so concurrent callers are still coalesced into one
    fetch.
  - **Every** streaming call — all three uploads and every download — runs that
    check before opening, with a five-second margin. Five seconds is enough
    because a gRPC server validates the bearer token once, when it accepts the
    call: a transfer that outlives its token keeps running, so the margin only
    has to cover the gap between reading the cached token and the server
    checking it. A pre-flight refresh that fails is a `warn!` and the call
    proceeds with the cached token, which may well still be valid.
  - `upload_file` now **retries once** on `UNAUTHENTICATED`, exactly as a unary
    RPC does: it holds its buffer in an `Arc`, so the request stream is rebuilt
    without copying the file, and the replay carries the **same** `request_id`,
    which is what makes it safe — the server deduplicates on it, so a replay
    that lands on an upload it had already committed is absorbed rather than
    written twice.
  - The call that **opens** a download does too, so `download_file_stream`,
    `download_file`, `download_file_verified` and `download_file_verified_to`
    all recover from one `UNAUTHENTICATED`. A server that rejects a server-streaming call answers
    before any message exists, so that rejection resolves the opening call
    itself and there is nothing consumed to replay around. No `grpc-timeout`
    header is sent (`request_timeout` still does not apply to a transfer).
  - `upload_file_chunked` and `upload_file_stream` get the pre-flight refresh
    only, and their documentation now says why: the request stream belongs to
    the caller, and h2 begins writing body frames as soon as the call opens, so
    "nothing has been consumed yet" is not a state the SDK can establish.
    Recover with `refresh_auth_token()` and a freshly built stream.
- `RociaDbClient::neighbor_nodes_out<T>` and
  `RociaDbClient::neighbor_nodes_in<T>`:
  `(tenant_id, graph, node_id, label, limit, cursor) -> Result<Page<NeighborNode<T>>>`.
  One `neighbors_*` page, then the node payload of each neighbor *on that
  page* fetched concurrently (at most 10 in flight, order preserved) and
  decoded into `T`. The work is bounded by `limit` rather than by the node's
  degree, and the cursor is the one `neighbors_out` / `neighbors_in` issued,
  with the same scoping rules. `get_outgoing_neighbor_nodes` /
  `get_incoming_neighbor_nodes` keep their all-pages behaviour, now
  documented as unbounded with a pointer here, and both shapes share one
  fan-out helper.
- `impl Debug for RociaDbClient`, reporting the host the client was built for
  and whether auth is enabled — never a token, a client id, or a secret.
- Crate-root re-exports so configuring the SDK needs no extra direct
  dependency: `Channel` and `ClientTlsConfig` (from `tonic`), `SecretString`
  and `ExposeSecret` (from `secrecy`). The same stability caveat as
  `Streaming` applies.
- `#![forbid(unsafe_code)]` and `#![warn(missing_docs)]` on the crate, and doc
  comments on every public item the latter reported — the fields of `Page`,
  `DocumentPage`, `NodeBinding`, `DocumentQueryFilter`, `DocumentQuerySort`,
  `NodeInput`, `EdgeInput`, `Edge`, `NeighborNode`, `FileUploadOptions`,
  `FileStreamUploadOptions`, `TokenResponse` and the `RociaDbError` variants,
  plus the variants of `DocumentQueryOperator` and
  `DocumentQuerySortDirection`.
- English documentation on the generated types re-exported at the crate root
  (`CollectionInfo`, `Neighbor`, `UploadRequest`, `DownloadResponse`) and on each
  of their fields, describing the wire semantics. It is attached by the build
  script, so it is regenerated with the code rather than drifting from it. The
  file metadata's own documentation moved to `FileMetadata` with the type.
- A `docs/` directory of guides — authentication, errors and retries,
  documents, graph, files, pagination, tenancy and authorization, transport
  and TLS, and parity with the TypeScript SDK — carrying the reference
  material the 1.0 README held in one 1 159-line file. `docs/` ships in the
  published package, and the crate documentation lists the guides by path.
- Every code example in `README.md` and in `docs/` is compiled by
  `cargo test`, through `#[cfg(doctest)] #[doc = include_str!(..)]` items in
  `src/lib.rs`. An example that stops matching the API now fails the build
  instead of misleading a reader.
- An integration test suite (`tests/documents.rs`, `tests/graph.rs`,
  `tests/files.rs`, `tests/auth.rs`, `tests/resilience.rs`,
  `tests/shared_client.rs`, on a shared `tests/support/`) that runs the real
  client path — builder, channel, bearer interceptor, generated client, the
  unary helper, JSON decoding — against an in-process tonic server
  implementing all four services and a mock OAuth2 identity provider served
  by hyper. Both bind `127.0.0.1:0`, so the suite is loopback-only,
  parallel-safe and needs nothing installed; the server reproduces the parts
  of the contract the SDK depends on (cursor pagination, `total_count`, the
  `(from, label, to)` edge uniqueness rule, idempotent deletes, the 1 MiB
  upload chunk cap, a checksum length check that never looks at the bytes,
  and the `reason` trailing metadata on every error) and offers scripted
  per-RPC failures, per-RPC delays and a recorder of every request and its
  `authorization` header.
- `.github/workflows/ci.yml`: formatting, Clippy, tests, a documentation
  build with `RUSTDOCFLAGS="-D warnings"`, an MSRV job on Rust 1.88, and a
  `cargo deny` job.
- `deny.toml`: RustSec advisories, a permissive-only licence allow-list,
  duplicate and wildcard bans as warnings, and crates.io as the only allowed
  source.
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
- **Module layout.** Each service's RPCs and types live with each other:
  documents in `document.rs`, graph in `graph.rs`, files in `file.rs`, tenants
  in `tenant.rs`, retries in `retry.rs`. `lib.rs` keeps only the crate
  documentation, the builder, the client (with its auth methods), `Page`,
  `WriteOptions` and the private helpers the modules share. Everything public
  is re-exported at the crate root, so the namespace is flat and `auth` is the
  only public module — the paths `rociadb_sdk::file::..` and
  `rociadb_sdk::graph::..` no longer exist.
- Every unary RPC now goes through one private helper on the client, which
  wraps the request, applies the per-call deadline, refreshes and retries once
  on `UNAUTHENTICATED`, and maps a failed `tonic::Status` — so none of that is
  repeated at twenty call sites. The refresh-and-retry half of it is shared, and
  the deadline is a parameter: the same core serves the call that opens a
  download (with no deadline) and, through the one helper that decides whether
  to replay, `upload_file`'s re-send of its own request stream. The `Upload` RPC
  is the only place left that calls a generated client directly, because its
  request is a stream rather than a cloneable message.
- **The library's `tokio` dependency enables `io-util`.** It is what
  `download_file_verified_to` drives the caller's writer with: the `AsyncWrite`
  trait in its signature needs no feature, but the `AsyncWriteExt::write_all`
  and `AsyncWriteExt::flush` it calls live behind that one. Nothing is added to
  the dependency graph or to `Cargo.lock` — tokio declares `io-util =
  ["bytes"]`, and `bytes` is already a direct dependency of this crate.
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
- `README.md` is rewritten for the 2.0 API and cut from 1 159 lines to a
  quick start plus the conventions that apply everywhere; the reference
  material moved to `docs/`.
- The minimum supported Rust version is **unchanged at 1.88** — 1.0.0 already
  declared it. The `rust-version` field now carries a comment recording which
  dependencies pin the floor (tonic, tonic-prost and tonic-prost-build all
  declare 1.88; the `icu_*` crates reached through reqwest declare 1.86), and
  CI checks it against the committed `Cargo.lock`.
- `Cargo.toml`'s `exclude` list now also drops `.github`, `deny.toml` and
  `mise.toml` from the published package. `CHANGELOG.md` and `docs/` are
  deliberately included.
- New development dependencies, all of them crates the graph already carried
  (only features and five small crates — `axum`, `axum-core`, `matchit`,
  `mime`, `httpdate` — are added to `Cargo.lock`, and nothing changes for a
  consumer building the library): `tonic` with `server` + `router`, `hyper`
  with `http1` + `server`, `hyper-util` with `tokio`, `http-body-util`,
  `tokio-util` with `io` (for the `ReaderStream` the upload tests hand to
  `upload_file_chunked`), and `net` + `test-util` + `rt-multi-thread` + `fs` on
  `tokio`.

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
  `google.protobuf.Empty`, and the `pbjson`, `chrono` and `num-traits` crates
  it pulled into every consumer's build with it. Across the whole of 2.0,
  `cargo tree -e normal` goes from 166 crates to 163: those four out,
  `secrecy` in.
- The `mise.toml` `protoc` tool entry, and every `PROTOC=` instruction in the
  crate documentation, `AGENTS.md` and `README.md`.
- The crate-wide `#![allow(clippy::doc_lazy_continuation)]`; the
  documentation it silenced was reflowed instead.

### Fixed

- **`upload_file_chunked` now agrees with the server about what happened.**
  Three ways it did not, all of them in how the rechunker measured the declared
  `size_bytes` against the bytes it actually had:

  - *An over-declared upload could publish a truncated file.* The overshoot was
    measured against the single chunk about to go out rather than against every
    declared byte in hand, so when `size_bytes` was an exact multiple of 1 MiB
    the chunk that *completed* the total was legal on its own, went out, and gave
    the server a whole stream to commit — the excess was noticed one iteration
    later. The caller got a `Validation` error for a file published under their
    own `file_id`, truncated. The check now covers buffered and pending bytes,
    and the chunk completing the total waits until the source confirms it has
    nothing more, so nothing is published for an upload the client refuses.
  - *A source failing after its last byte failed the upload.* Every declared byte
    had already gone out, so the server committed a complete, valid file and the
    call still returned `RociaDbError::Io` — which a caller reading as "nothing
    was written" would delete or re-queue. That failure is now logged at `warn!`
    and the server's verdict returned.
  - *And that forgiveness was inconsistent.* Its condition was bytes already
    *sent*, so it held only at an exact 1 MiB multiple: at any other size the
    tail was still buffered and the identical failure came back as `Io`. It is
    now measured against bytes *read*, which is what
    `docs/errors-and-retries.md` always described.

  **One deliberate exception:** a `size_bytes` of zero is never forgiven. It
  satisfies "every declared byte is in hand" before the source is read at all, so
  forgiving there would publish an empty file — and publishing replaces whatever
  that `file_id` held, in one atomic swap. A caller whose size computation
  wrongly returned zero, and whose source then failed, would have destroyed the
  stored file and been told `Ok`. A zero-byte upload whose source simply *ends*
  is unaffected and still succeeds.

  If you were relying on the idempotency key to recover from the first of these:
  on a server that deduplicates by `request_id` as the wire contract describes, a
  corrected retry reusing the key of the call that published the truncated file
  would have been absorbed rather than replacing it. The test harness models that
  contract, so the SDK's behaviour under a deduplicating server is covered;
  whether a given deployment matches it is that deployment's claim, not something
  this crate can verify.
- **Concurrent token refreshes now coalesce when the refresh fails, not only
  when it succeeds.** The generation counter advanced only after a token was
  installed, so a failing identity provider left every caller queued on the
  refresh lock convinced its own snapshot was current — and the lock is held
  across the HTTP round trip, so N callers waited behind one another at up to 30
  seconds each. Fifty tasks recovering from one expired token meant the fiftieth
  blocked for the sum of the other forty-nine, however short its
  `request_timeout`. A coalesced caller now inherits the concurrent attempt's
  outcome, including its error; one arriving after an attempt has settled still
  makes its own, so a failure cannot wedge the client.
- **`token_url` and `client_id` no longer reach any log line, error message or
  `Debug` output.** Three paths leaked them, none through a secret-carrying
  field, so `SecretString` could not help: the non-https warning logged the URL
  outright; `reqwest`'s `Display` appends `" for url (..)"`, carrying the whole
  endpoint into the error a *caller* sees and into the SDK's own warning on a
  failed background refresh; and `RociaDbBuilder` derived `Debug`, printing both
  values in cleartext. The builder's `Debug` now reports whether each credential
  field is set, and nothing more.
- **`upload_file` no longer blocks a runtime worker while hashing.** SHA-256 over
  a multi-gigabyte buffer ran inline with no await point in it, starving every
  other task on that worker. Buffers over 1 MiB are now hashed on the blocking
  pool; a caller-supplied checksum and smaller buffers are unchanged.
- **`TokenManager::spawn_refresh` no longer panics on a zero interval.**
  `tokio::time::interval` rejects a zero period, and it did so inside the spawned
  task, where the panic reached nobody: it aborted the refresh task and left the
  client with no background refresh and no error. The period is now floored at
  one second.
- `RociaDbError::Io`'s message no longer calls every failure a read. The
  download case rendered "failed to read writing the downloaded file", which is
  both nonsense and the wrong direction — it writes to the caller's writer. It
  now reads "writing the downloaded file failed: ...".
- Documentation corrections where the text described a safer contract than the
  code delivers: `Validation` is not always raised before a network call (a
  chunk-stream size mismatch cannot be), `Config` is not always raised before a
  connection is attempted (`build` dials before reading the `AUTH_*` variables
  or validating `request_timeout`), `is_unauthenticated` does not mean streaming
  calls were never retried (`upload_file` and the call that opens a download
  are), `reason()` is `None` for the SDK's own client-side `DEADLINE_EXCEEDED`,
  the `tls_config` guide's example left the client with no trust anchors at all,
  and the crate's stability caveat omitted the `serde` and `serde_json` items in
  its public API.
- `RociaDbError::Connection` no longer implies it can arrive from a reset
  connection mid-call. It comes from the initial dial and from nowhere else:
  once a client exists its channel is established, so a connection later
  refused, reset or lost arrives as a `Status` carrying `UNAVAILABLE` — which is
  also the code `RetryPolicy` retries on, so code matching on `Connection` to
  decide whether to retry a transport failure was matching on a variant it would
  never see. Now enforced by a test.
- `download_file` documents that nothing bounds what it allocates. It never
  stats the file, there is no ceiling to configure, and `max_file_bytes` gates
  uploads only, so calling it on a file whose size you do not control hands that
  endpoint your process's memory. The three bounded alternatives, and what
  bounds each, are now named alongside it. No behaviour change.
- `list_tenants` no longer suggests it may be access-controlled. It is not:
  any authenticated data-plane token can call it, exactly as `docs/tenancy.md`
  already said. Living on its own service is what would let a policy be added
  later, not evidence that one exists — so the listing is not privileged
  information.
- Updated `h2` to 0.4.19 in `Cargo.lock` for RUSTSEC-2026-0258 (unbounded empty
  DATA frames, reachable through both `tonic` and `reqwest`).
- Updated `rustls` to 0.23.45 (and `rustls-webpki` to 0.103.15) in `Cargo.lock`
  for GHSA-2mjx-qc3c-rqvc, reachable through both `tonic` and `reqwest`. A patch
  bump within the same major version, so nothing in this crate's API or its MSRV
  changes.
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
  call is never retried more than once.
- **A streaming upload or download no longer starts on a token that is about to
  be rejected**, and two of them recover from a rejection on their own. Both
  streaming RPCs used to get nothing: no pre-flight refresh, no retry, so a
  transfer begun seconds before a token expired — or begun while the background
  refresh task was failing — failed with `UNAUTHENTICATED` and left the caller
  to notice. Every streaming call now refreshes first when little of the
  token's lifetime is left, `upload_file` replays its own buffer once under the
  same `request_id`, and the call that opens a download replays once too. See
  the Added section for which call gets what, and why
  `upload_file_chunked`/`upload_file_stream` cannot be replayed for you.
- **A failed read from an `upload_file_chunked` source is no longer reported as
  a size mismatch.** With the old `Stream<Item = Vec<u8>>` item type a source
  that hit an I/O error could only end early, and the upload failed with a
  `Validation` error naming byte counts — pointing at the caller's `size_bytes`
  rather than at the disk that could not be read. The item type is now
  `std::io::Result<Bytes>` and the error surfaces as `RociaDbError::Io` carrying
  the original `std::io::Error`.

## [1.0.0] - 2026-09-01

First stable release. See the
[repository history](https://github.com/RociaDB/rociadb-core-sdk-rust/commits/main)
for what it contained.
