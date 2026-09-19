//! File upload/download helpers.
//!
//! The option types this module defines are re-exported at the crate root
//! ([`crate::FileUploadOptions`], [`crate::FileStreamUploadOptions`]), and
//! the RPCs are inherent methods on [`RociaDbClient`].
//! [`RociaDbClient::upload_file_stream`] documents the server's upload wire
//! contract in full — including the 1 MiB per-message cap every upload path
//! here respects.
use crate::error::StatusResultExt;
use crate::pb::upstream::v1::{
    DeleteRequest, DownloadRequest, DownloadResponse, ListBucketsRequest, ListFilesRequest,
    StatRequest, StatResponse, UploadRequest,
};
use crate::{
    Bytes, DEFAULT_PAGE_SIZE, Page, Result, RociaDbClient, RociaDbError, WriteOptions, non_empty,
    page_request,
};
use futures::{Stream, StreamExt, stream};
use sha2::{Digest, Sha256};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tonic::codec::Streaming;
use tracing::debug;
use uuid::Uuid;

/// Size of every upload message the SDK emits, except the last one. Not
/// configurable: see [`RociaDbClient::upload_file_stream`].
const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024; // 1 MiB.

/// `operation` tag on every error the three upload paths produce, and the label
/// the pre-flight refresh and the replay log under. One constant because all
/// three go through [`RociaDbClient::upload_raw`], which must name the same
/// operation as the helpers built on it.
const UPLOAD_OPERATION: &str = "failed to upload file";

/// `operation` tag for the call that opens a download stream. Not shared with
/// the error [`RociaDbClient::download_file`] reports for a failure *during* the
/// stream (`"file download stream failed"`), which is a different event: the
/// call had already been accepted.
const DOWNLOAD_OPERATION: &str = "failed to start file download";

/// Server-side max file size (`limits.max_file_bytes`, 5 GiB default).
const MAX_FILE_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// Ceiling on the buffer [`RociaDbClient::download_file_verified`]
/// pre-allocates from the `size_bytes` the server reported.
///
/// The size is the server's word, not a measurement, so allocating it blindly
/// would let a compromised or simply buggy server make the client reserve
/// gigabytes before a single byte of the file has arrived. 64 MiB is large
/// enough that every realistic file is allocated exactly once, and small
/// enough to be an unremarkable allocation if the number is nonsense; a file
/// genuinely larger than this just grows its buffer while streaming, the same
/// way [`RociaDbClient::download_file`] always does.
pub(crate) const MAX_PREALLOCATED_DOWNLOAD_BYTES: u64 = 64 * 1024 * 1024;

/// Default MIME type recorded for a file whose uploader did not name one.
const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

/// Options applied to [`RociaDbClient::upload_file`], the in-memory
/// byte-buffer upload.
///
/// There is intentionally no `chunk_size` knob: see
/// [`RociaDbClient::upload_file_stream`] for why 1 MiB is the only size
/// worth using.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileUploadOptions {
    /// MIME type recorded for the file. Defaults to
    /// `"application/octet-stream"`; the server records it as given and never
    /// inspects the bytes to confirm it.
    pub content_type: String,
    /// SHA-256 digest of the uploaded bytes. When `None`,
    /// [`RociaDbClient::upload_file`] computes it from the buffer
    /// automatically — which is almost always what you want; set it
    /// explicitly only when the digest is already known from elsewhere (a
    /// manifest, a previous pass over the same bytes).
    ///
    /// The server checks the length and nothing else: a digest that does
    /// not match the bytes sent produces an upload that looks successful
    /// while recording a checksum the stored file does not satisfy. The
    /// `[u8; 32]` type takes the length half of that off the table at
    /// compile time.
    pub checksum: Option<[u8; 32]>,
    /// Idempotency key for the upload. When `None`, one is generated
    /// automatically (`upload_file:<uuid>` — see the [idempotency key
    /// defaults](WriteOptions#idempotency-key-defaults)). Provide it
    /// explicitly — and reuse the same value on a retry — so an upload
    /// replayed after a timeout is absorbed rather than performed twice.
    pub request_id: Option<String>,
}

impl Default for FileUploadOptions {
    fn default() -> Self {
        Self {
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            checksum: None,
            request_id: None,
        }
    }
}

impl FileUploadOptions {
    /// Options with every field at its default: `"application/octet-stream"`,
    /// a checksum computed from the bytes being uploaded, and a generated
    /// idempotency key.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `content_type` as the file's MIME type.
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }

    /// Send `checksum` instead of computing the SHA-256 digest of the
    /// uploaded bytes; see [`FileUploadOptions::checksum`].
    pub fn with_checksum(mut self, checksum: [u8; 32]) -> Self {
        self.checksum = Some(checksum);
        self
    }

    /// Set the idempotency key for this upload; see the [idempotency key
    /// defaults](WriteOptions#idempotency-key-defaults).
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }
}

/// Options applied to [`RociaDbClient::upload_file_chunked`], the streaming
/// upload.
///
/// Unlike [`FileUploadOptions`], `size_bytes` and `checksum` are required
/// rather than optional, which is why this type has no `Default` and its
/// constructor takes both: the file's metadata travels on the first gRPC
/// message, before a single byte has been read from the caller's stream, so
/// neither value can be derived on the fly the way
/// [`RociaDbClient::upload_file`] derives them from a complete in-memory
/// buffer.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStreamUploadOptions {
    /// Exact total number of bytes the caller's stream will produce. The
    /// upload fails with [`RociaDbError::Validation`] if the stream ends up
    /// shorter or longer, and the server checks the same thing at the end of
    /// the stream.
    pub size_bytes: u64,
    /// SHA-256 digest of the complete file. Hash the source ahead of time (a
    /// first pass over the file, for example): a streaming upload cannot
    /// compute this while sending, since the digest has to be on the first
    /// message. As with [`FileUploadOptions::checksum`], the server checks
    /// the length only.
    pub checksum: [u8; 32],
    /// MIME type recorded for the file. Defaults to
    /// `"application/octet-stream"`; the server records it as given and never
    /// inspects the bytes to confirm it.
    pub content_type: String,
    /// Idempotency key for the upload. When `None`, one is generated
    /// automatically (`upload_file:<uuid>` — see the [idempotency key
    /// defaults](WriteOptions#idempotency-key-defaults)). Provide it
    /// explicitly — and reuse the same value on a retry — so an upload
    /// replayed after a timeout is absorbed rather than performed twice.
    pub request_id: Option<String>,
}

impl FileStreamUploadOptions {
    /// Options for a file of exactly `size_bytes` bytes whose SHA-256 digest
    /// is `checksum`, with `"application/octet-stream"` as the MIME type and
    /// a generated idempotency key.
    pub fn new(size_bytes: u64, checksum: [u8; 32]) -> Self {
        Self {
            size_bytes,
            checksum,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            request_id: None,
        }
    }

    /// Record `content_type` as the file's MIME type.
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }

    /// Set the idempotency key for this upload; see the [idempotency key
    /// defaults](WriteOptions#idempotency-key-defaults).
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }
}

impl RociaDbClient {
    /// Upload a caller-built stream of protobuf [`UploadRequest`] messages.
    ///
    /// This is a low-level escape hatch for genuine streaming uploads (data
    /// that never fits in memory). The SDK does **not** rechunk or compute
    /// a checksum here — the caller is fully responsible for the wire
    /// contract the server enforces:
    /// - the **first** message must carry `tenant_id`, `bucket`, `file_id`,
    ///   `size_bytes` (the exact total byte count) and `checksum` set to
    ///   the SHA-256 digest of the whole file, as exactly 32 raw bytes;
    /// - every message's `chunk` must not exceed 1 MiB (1_048_576 bytes) —
    ///   below that cap, the server accepts any size, sliced however the
    ///   caller likes;
    /// - `content_type` and `checksum` on messages after the first are
    ///   ignored by the server and can be left empty;
    /// - `request_id`, if any, is read from the first message too; nothing
    ///   is generated for you here, unlike every other write on this client.
    ///
    /// A `chunk` over 1 MiB, a checksum of the wrong length, or a
    /// mismatched `size_bytes` all fail the upload outright with
    /// `INVALID_ARGUMENT` rather than corrupting anything silently. The one
    /// thing the server never verifies is whether `checksum` actually
    /// matches the bytes sent — only that it is 32 bytes long — so a wrong
    /// checksum can still produce an upload that looks successful while
    /// carrying bad data.
    ///
    /// **Why the SDK's own uploads always emit exactly 1 MiB chunks**: that
    /// is the largest message the server allows, so it is also the fewest
    /// possible messages for a given file, and it remains the only chunk
    /// size that is safe against a server older than `1.0.0-rc.16`. Neither
    /// [`RociaDbClient::upload_file`] nor
    /// [`RociaDbClient::upload_file_chunked`] exposes a knob for it.
    ///
    /// For the common case — uploading an in-memory byte buffer — use
    /// [`RociaDbClient::upload_file`] instead, which builds a correct
    /// stream for you; for a large source you cannot buffer but can still
    /// checksum ahead of time, prefer
    /// [`RociaDbClient::upload_file_chunked`], which re-chunks and
    /// validates for you.
    ///
    /// [`RociaDbBuilder::request_timeout`](crate::RociaDbBuilder::request_timeout)
    /// does **not** apply here, nor to any of the upload helpers built on
    /// this method: how long a stream takes is a property of the caller's
    /// own data rate, not of a single round trip. Wrap the call in a
    /// `tokio::time::timeout` of your own if it needs a deadline.
    ///
    /// # Authentication
    ///
    /// With auth enabled, the token is refreshed before the call opens if
    /// little of its lifetime is left, and that is **all** this method gets:
    /// an `UNAUTHENTICATED` answer is returned to you rather than retried.
    /// The reason is `requests`, which this method does not own — it is a
    /// stream the caller handed over, and h2 starts writing body frames onto
    /// the wire as soon as the call opens, so "nothing has been consumed yet"
    /// is not a state the SDK can establish, let alone rely on. Re-sending
    /// would mean draining a stream that has already been partly drained.
    /// Recover by calling
    /// [`refresh_auth_token`](RociaDbClient::refresh_auth_token) and
    /// re-issuing the call with a freshly built stream — and reuse the first
    /// message's `request_id` when you do, so the server recognizes the
    /// replay. [`RociaDbClient::upload_file`] has no such problem (it owns
    /// its buffer) and does retry once for you.
    pub async fn upload_file_stream<S>(&self, requests: S) -> Result<()>
    where
        S: Stream<Item = UploadRequest> + Send + 'static,
    {
        debug!("uploading a caller-built file stream");
        self.upload_raw(requests)
            .await
            .status_context(UPLOAD_OPERATION)?;
        Ok(())
    }

    /// Issue the client-streaming `Upload` RPC and hand back the raw
    /// [`tonic::Status`] on failure.
    ///
    /// The one place the generated file client's `upload` is called, and
    /// therefore the one place that runs the pre-flight token refresh every
    /// upload path gets (see
    /// [`RociaDbClient::refresh_token_before_stream`]).
    /// Unlike a unary RPC — which goes through [`RociaDbClient::unary`] — a
    /// streaming upload gets no per-call deadline, and no replay is possible
    /// from *here*: the stream has been moved in and can only be consumed
    /// once. [`RociaDbClient::upload_file`], which builds its stream from a
    /// buffer it owns, replays by calling this a second time with a freshly
    /// built stream.
    ///
    /// The status is left unmapped because
    /// [`RociaDbClient::upload_file_chunked`] must decide between it and its
    /// own client-side error before either is returned, and
    /// [`RociaDbClient::upload_file`] must inspect it before deciding to
    /// replay.
    async fn upload_raw<S>(&self, requests: S) -> std::result::Result<(), tonic::Status>
    where
        S: Stream<Item = UploadRequest> + Send + 'static,
    {
        self.refresh_token_before_stream(UPLOAD_OPERATION).await;
        let mut upstream_file = self.upstream_file.clone();
        upstream_file
            .upload(requests)
            .await
            .map(tonic::Response::into_inner)
    }

    /// Upload an in-memory byte buffer, split into gRPC messages of the
    /// server's largest allowed chunk size.
    ///
    /// The buffer is always split into 1 MiB (1_048_576-byte) chunks, the
    /// last one possibly shorter; not configurable, see
    /// [`RociaDbClient::upload_file_stream`] for why. When
    /// [`FileUploadOptions::checksum`] is `None`, the SHA-256 digest of
    /// `bytes` is computed and sent automatically. Files over 5 GiB
    /// (`limits.max_file_bytes`, the server default) are rejected
    /// client-side with a clear error instead of failing partway through the
    /// upload.
    ///
    /// `bytes` is taken as `impl Into<Vec<u8>>`, so both ownership styles
    /// are one call: a `Vec<u8>` you already hold — a file read off disk, a
    /// buffer assembled in memory — is **moved** straight into the chunking
    /// step with no copy, while a borrowed `&[u8]` (or `&[u8; N]`, or
    /// `&str`) is **copied once** into the owned buffer that the chunking
    /// step and the underlying `'static` upload stream require. That copy is
    /// unavoidable for a borrowed buffer, and worth avoiding for a large
    /// owned one: it doubles peak memory for the whole upload, since Rust's
    /// drop scopes keep the original alive until the upload finishes.
    ///
    /// See the [idempotency key
    /// defaults](WriteOptions#idempotency-key-defaults) for the key used
    /// when [`FileUploadOptions::request_id`] is unset.
    ///
    /// # Authentication
    ///
    /// This is the one upload path that gets the full treatment a unary RPC
    /// gets: the token is refreshed before the call when little of its
    /// lifetime is left, and an `UNAUTHENTICATED` answer triggers one coalesced
    /// refresh and one re-send — never more than one. It can do that because
    /// the buffer belongs to it (it is held in an `Arc`, so the second attempt
    /// re-slices the same bytes rather than copying the file), and because the
    /// re-send carries the
    /// **same** `request_id`: the server deduplicates on it, so a replay that
    /// lands on an upload the server had already committed is absorbed instead
    /// of writing the file twice. [`RociaDbClient::upload_file_chunked`] and
    /// [`RociaDbClient::upload_file_stream`] cannot do this — their source is
    /// a stream, not a buffer — and get the pre-flight refresh only.
    pub async fn upload_file(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
        bytes: impl Into<Vec<u8>>,
        options: FileUploadOptions,
    ) -> Result<()> {
        // An `Arc` rather than a plain `Vec<u8>`: the request stream has to be
        // rebuildable for the replay below, and `Arc::clone` shares the buffer
        // instead of copying a file's worth of bytes for a second attempt that
        // usually never happens.
        let bytes = Arc::new(bytes.into());
        let size_bytes = u64::try_from(bytes.len())
            .map_err(|_| RociaDbError::validation("file is too large"))?;
        validate_file_size(size_bytes)?;

        debug!(
            tenant_id = tenant_id,
            bucket = bucket,
            file_id = file_id,
            size_bytes = size_bytes,
            "uploading file"
        );
        let checksum = resolve_checksum(options.checksum, &bytes);
        let request_id = options
            .request_id
            .unwrap_or_else(default_upload_file_request_id);

        // One attempt's worth of request stream, built fresh each time it is
        // called. Every attempt sends the same `request_id`, which is what
        // makes the replay safe: the server treats the second attempt as the
        // same write as the first.
        let requests = || {
            stream::iter(chunk_upload_requests(
                tenant_id.to_string(),
                bucket.to_string(),
                file_id.to_string(),
                Arc::clone(&bytes),
                options.content_type.clone(),
                checksum,
                request_id.clone(),
            ))
        };

        let error = match self.upload_raw(requests()).await {
            Ok(()) => return Ok(()),
            Err(status) => RociaDbError::Status {
                operation: UPLOAD_OPERATION,
                status,
            },
        };
        // Exactly the rules `RociaDbClient::unary` follows, from the same
        // helper: replay only on `UNAUTHENTICATED`, only with auth enabled,
        // only once, and return the original error (with a `warn!`) if the
        // refresh itself failed.
        if !self.refresh_for_replay(UPLOAD_OPERATION, &error).await {
            return Err(error);
        }
        self.upload_raw(requests())
            .await
            .status_context(UPLOAD_OPERATION)?;
        Ok(())
    }

    /// Upload a stream of arbitrarily-sized byte chunks without buffering
    /// the complete file in memory.
    ///
    /// This is the middle tier between [`RociaDbClient::upload_file`]
    /// (buffers the whole file, computes the checksum for you) and
    /// [`RociaDbClient::upload_file_stream`] (a raw pass-through with zero
    /// validation, and the caller must already match the server's exact
    /// wire contract). `chunks` may be split however the source naturally
    /// produces data — a 64 KiB `AsyncRead` wrapper, protobuf messages
    /// from another stream, anything — this method re-buffers internally
    /// and always emits exactly-1-MiB gRPC messages to the server (the last
    /// one may be shorter), the same chunking [`RociaDbClient::upload_file`]
    /// produces from an in-memory buffer. It never holds more than one
    /// outgoing chunk's worth of bytes at a time, however `chunks` happens
    /// to be sliced.
    ///
    /// # The item type
    ///
    /// `std::io::Result<Bytes>` is chosen so the obvious source needs no
    /// adaptation at all: `tokio_util::io::ReaderStream` wraps any
    /// `tokio::io::AsyncRead` — a `tokio::fs::File`, a socket, a decompressor
    /// — and yields exactly this item, so it is passed straight in.
    ///
    /// ```rust,no_run
    /// # use rociadb_sdk::{FileStreamUploadOptions, RociaDbBuilder};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = RociaDbBuilder::new().disable_auth().build().await?;
    /// # let (size_bytes, checksum) = (0u64, [0u8; 32]);
    /// let file = tokio::fs::File::open("large-report.csv").await?;
    /// client
    ///     .upload_file_chunked(
    ///         "tenant-1",
    ///         "reports",
    ///         "large-report.csv",
    ///         tokio_util::io::ReaderStream::new(file),
    ///         FileStreamUploadOptions::new(size_bytes, checksum),
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// The `Result` is the load-bearing half: a read that fails partway
    /// through is an `Err` item, and it **fails the upload** with
    /// [`RociaDbError::Io`] carrying the [`std::io::Error`]. An item type of
    /// plain bytes would leave a failing source no way to say so — it could
    /// only end early, and the upload would be reported as a size mismatch,
    /// blaming the caller's `size_bytes` for a disk that could not be read.
    /// Nothing is pulled from `chunks` after an `Err`, and the [`Bytes`] items
    /// are copied into the outgoing chunk buffer like any other bytes: the
    /// type is there for what it makes easy at the call site, not to make the
    /// upload zero-copy.
    ///
    /// # Validation and errors
    ///
    /// [`FileStreamUploadOptions::size_bytes`] must be the exact total the
    /// caller intends to send and [`FileStreamUploadOptions::checksum`] the
    /// SHA-256 digest of the complete file; see that type for why neither
    /// can be computed here. If `chunks` ends up producing more or fewer
    /// total bytes than `size_bytes` declared, this fails with
    /// [`RociaDbError::Validation`] instead of silently sending a
    /// corrupt-on-download file: the server itself also checks this at the
    /// end of the stream, but catching it here gives a clearer, immediate
    /// error naming the actual byte counts involved.
    ///
    /// Both client-side failures — the [`RociaDbError::Io`] from a failing
    /// source and the [`RociaDbError::Validation`] from a byte count that
    /// disagrees — take precedence over whatever status the server returned
    /// for the stream that then ended early, because they say what actually
    /// went wrong.
    ///
    /// # Authentication
    ///
    /// With auth enabled the token is refreshed before the call opens if
    /// little of its lifetime is left, and that is all: an `UNAUTHENTICATED`
    /// answer is returned rather than retried, because `chunks` is the
    /// caller's and h2 starts writing body frames as soon as the call opens —
    /// so "nothing has been consumed yet" is not a state the SDK can
    /// establish, and a replay would have to re-drain a stream that is already
    /// partly drained. Recover by calling
    /// [`refresh_auth_token`](RociaDbClient::refresh_auth_token) and
    /// re-issuing with a fresh `chunks`, reusing the same
    /// [`FileStreamUploadOptions::request_id`] so the server recognizes the
    /// replay. [`RociaDbClient::upload_file`] owns its buffer and does retry
    /// once on its own.
    ///
    /// **Naming note**: despite matching the server's chunking contract,
    /// this is not called `upload_file_stream` — that name belongs to the
    /// raw, zero-validation escape hatch above it.
    pub async fn upload_file_chunked<S>(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
        chunks: S,
        options: FileStreamUploadOptions,
    ) -> Result<()>
    where
        S: Stream<Item = std::io::Result<Bytes>> + Send + 'static,
    {
        validate_file_size(options.size_bytes)?;
        debug!(
            tenant_id = tenant_id,
            bucket = bucket,
            file_id = file_id,
            size_bytes = options.size_bytes,
            "uploading file from a chunk stream"
        );
        let request_id = options
            .request_id
            .unwrap_or_else(default_upload_file_request_id);

        // Set by `rechunk_upload_requests` when the source failed with an I/O
        // error or produced a total byte count that does not match
        // `size_bytes`, since the outgoing `Stream<Item = UploadRequest>`
        // itself has no channel to carry an error — it can only end early.
        // Checked below regardless of whether the RPC itself succeeded or
        // failed, so this client-side error takes precedence over whatever the
        // server made of a stream that ended up short or truncated.
        let error_slot: Arc<Mutex<Option<RociaDbError>>> = Arc::new(Mutex::new(None));
        let requests = rechunk_upload_requests(
            tenant_id.to_string(),
            bucket.to_string(),
            file_id.to_string(),
            options.size_bytes,
            options.content_type,
            options.checksum,
            request_id,
            chunks,
            Arc::clone(&error_slot),
        );

        let upload_result = self.upload_raw(requests).await;
        if let Some(error) = error_slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            return Err(error);
        }
        upload_result.status_context(UPLOAD_OPERATION)?;
        Ok(())
    }

    /// Start a server-streaming download without buffering the complete file.
    ///
    /// This performs no integrity verification of its own — it is a thin
    /// wrapper that opens the raw gRPC download stream and hands it back
    /// as-is. That is a real asymmetry with the upload path: every upload
    /// method on this client sends a SHA-256 checksum with the file (and
    /// [`RociaDbClient::upload_file`] computes it for you), and
    /// [`StatResponse::checksum`] exposes the checksum recorded for a stored
    /// file — but nothing on the download side ever computes or checks a
    /// checksum against the chunks the server sends back, and the server
    /// does not send one on download for this method to check. The only
    /// protection you get is whatever the transport itself already provides
    /// (TLS and HTTP/2 framing catch corruption or truncation in transit),
    /// which says nothing about whether the bytes stored on the server still
    /// match what was originally uploaded.
    ///
    /// [`RociaDbClient::download_file_verified`] closes that gap for a file
    /// that fits in memory: it stats first, hashes while it streams, and
    /// fails rather than returning bytes that disagree with the metadata.
    /// Streaming callers who cannot buffer the file do the same thing
    /// themselves — hash each chunk as it arrives and compare the digest
    /// against [`RociaDbClient::stat_file`]'s `checksum` at the end.
    ///
    /// [`RociaDbBuilder::request_timeout`](crate::RociaDbBuilder::request_timeout)
    /// does **not** apply here, nor to [`RociaDbClient::download_file`]: how
    /// long a transfer takes is a property of the file's size and the link,
    /// not of a single round trip. Wrap the call in a `tokio::time::timeout`
    /// of your own if it needs a deadline.
    ///
    /// # Authentication
    ///
    /// The call that *opens* the stream is covered like a unary RPC: the token
    /// is refreshed first if little of its lifetime is left, and an
    /// `UNAUTHENTICATED` rejection triggers one coalesced refresh and one
    /// re-issue. That works here — unlike for a streaming upload — because a
    /// server that rejects the call does so before any message exists, so this
    /// future resolves with the status and there is nothing consumed to replay
    /// around. Nothing covers the stream *after* it opens: the server checked
    /// the bearer token once, when it accepted the call, so a long transfer is
    /// not interrupted by its token expiring, but a status that arrives in the
    /// stream's trailers reaches you through
    /// [`Streaming::message`](tonic::codec::Streaming::message) unretried.
    pub async fn download_file_stream(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
    ) -> Result<Streaming<DownloadResponse>> {
        debug!(
            tenant_id = tenant_id,
            bucket = bucket,
            file_id = file_id,
            "downloading file"
        );
        self.refresh_token_before_stream(DOWNLOAD_OPERATION).await;
        let request = DownloadRequest {
            tenant_id: tenant_id.to_string(),
            bucket: bucket.to_string(),
            file_id: file_id.to_string(),
        };
        // `server_streaming` rather than `unary`: the same refresh-and-retry,
        // deliberately without the per-RPC deadline. See its documentation for
        // what a `grpc-timeout` header would do to a server-streaming call.
        self.server_streaming(DOWNLOAD_OPERATION, request, |request| {
            let mut upstream = self.upstream_file.clone();
            async move { upstream.download(request).await }
        })
        .await
    }

    /// Download a complete file into memory.
    ///
    /// Collects every chunk from [`RociaDbClient::download_file_stream`]
    /// into one buffer via `extend_from_slice` and nothing else — see that
    /// method's docs for the full asymmetry with the upload path: no
    /// checksum is computed or checked here either, so a file that was
    /// corrupted or truncated in storage is still returned successfully,
    /// with its bad bytes intact and no error raised. Use
    /// [`RociaDbClient::download_file_verified`] when the bytes have to be
    /// checked against the metadata the server recorded for them.
    pub async fn download_file(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
    ) -> Result<Vec<u8>> {
        let mut stream = self
            .download_file_stream(tenant_id, bucket, file_id)
            .await?;
        let mut bytes = Vec::new();
        while let Some(response) = stream
            .message()
            .await
            .status_context("file download stream failed")?
        {
            bytes.extend_from_slice(&response.chunk);
        }
        Ok(bytes)
    }

    /// Download a complete file into memory and check it against the
    /// metadata the server recorded for it.
    ///
    /// The verifying counterpart of [`RociaDbClient::download_file`]. It
    /// calls [`RociaDbClient::stat_file`] first, streams the download while
    /// feeding every chunk to a SHA-256 hasher, and only then hands the
    /// buffer back — after checking both the byte count against
    /// [`StatResponse::size_bytes`] and the digest against
    /// [`StatResponse::checksum`]. A file whose stored bytes have been
    /// corrupted or truncated therefore fails here instead of being returned
    /// as if nothing were wrong.
    ///
    /// # What this proves, and what it does not
    ///
    /// **The server never verified the uploader's checksum.** It checks that
    /// the value is 32 bytes long and stores it; it never hashes the bytes it
    /// received to confirm the two agree (see
    /// [`StatResponse::checksum`] and
    /// [`RociaDbClient::upload_file_stream`]). So a match here proves the
    /// bytes you just received are the bytes the uploader *declared* — it
    /// catches storage corruption, a truncated transfer, and a partially
    /// overwritten file, and it does not catch an uploader that sent a
    /// checksum which never matched its own payload. End-to-end integrity
    /// against a source you do not control needs a digest carried out of
    /// band, not this.
    ///
    /// Nothing is checked atomically with the download either: `stat_file`
    /// and the download are two calls, so a file replaced between them is
    /// read as a mismatch rather than as the new version. Replacement is
    /// atomic server-side (see [`UploadRequest::file_id`](crate::UploadRequest)),
    /// so the mismatch is the worst case — never a mixed file.
    ///
    /// # Errors
    ///
    /// [`RociaDbError::SizeMismatch`] when the byte count disagrees with
    /// `size_bytes` (checked first: a truncated stream fails both checks, and
    /// the byte count is the more actionable report), then
    /// [`RociaDbError::ChecksumMismatch`] when the digest disagrees with the
    /// stored checksum — including when that checksum is not a 32-byte
    /// SHA-256 digest at all, since the comparison is over raw bytes.
    /// Otherwise whatever [`RociaDbClient::stat_file`] and
    /// [`RociaDbClient::download_file_stream`] return: `NOT_FOUND` for an
    /// unknown `file_id`, and so on.
    ///
    /// # Memory
    ///
    /// The whole file is buffered, like [`RociaDbClient::download_file`]. The
    /// buffer is pre-allocated from the size `stat_file` reported, capped at
    /// 64 MiB, so a server reporting an absurd `size_bytes` cannot make the
    /// client reserve gigabytes before a single byte has arrived; a genuinely
    /// larger file simply grows the buffer as it streams, exactly as
    /// `download_file` does.
    ///
    /// [`RociaDbBuilder::request_timeout`](crate::RociaDbBuilder::request_timeout)
    /// covers the `stat_file` call (a unary RPC) but not the download stream,
    /// as everywhere else on this client.
    pub async fn download_file_verified(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
    ) -> Result<Vec<u8>> {
        let stat = self.stat_file(tenant_id, bucket, file_id).await?;
        debug!(
            tenant_id = tenant_id,
            bucket = bucket,
            file_id = file_id,
            size_bytes = stat.size_bytes,
            "downloading file with verification"
        );

        // Never trust `size_bytes` with an allocation: it is a number the
        // server chose, and `Vec::with_capacity` would reserve it up front.
        let capacity = usize::try_from(stat.size_bytes.min(MAX_PREALLOCATED_DOWNLOAD_BYTES))
            .unwrap_or(usize::MAX);
        let mut bytes = Vec::with_capacity(capacity);
        let mut hasher = Sha256::new();

        let mut stream = self
            .download_file_stream(tenant_id, bucket, file_id)
            .await?;
        while let Some(response) = stream
            .message()
            .await
            .status_context("file download stream failed")?
        {
            // Hashed incrementally as the chunks arrive: a second pass over
            // the assembled buffer would read the whole file twice for no
            // gain.
            hasher.update(&response.chunk);
            bytes.extend_from_slice(&response.chunk);
            // Stop as soon as the stream overshoots the size the metadata
            // announced, rather than draining an arbitrarily long stream
            // into memory only to reject it at the end: the outcome is the
            // same mismatch, but the buffer never grows past `size_bytes`
            // plus one chunk.
            if bytes.len() as u64 > stat.size_bytes {
                return Err(RociaDbError::SizeMismatch {
                    expected: stat.size_bytes,
                    actual: bytes.len() as u64,
                });
            }
        }

        let actual_size = bytes.len() as u64;
        if actual_size != stat.size_bytes {
            return Err(RociaDbError::SizeMismatch {
                expected: stat.size_bytes,
                actual: actual_size,
            });
        }
        let digest: [u8; 32] = hasher.finalize().into();
        if digest.as_slice() != stat.checksum.as_slice() {
            return Err(RociaDbError::ChecksumMismatch {
                expected: stat.checksum,
                actual: digest.to_vec(),
            });
        }
        Ok(bytes)
    }

    /// Return metadata for one stored file.
    pub async fn stat_file(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
    ) -> Result<StatResponse> {
        debug!(
            tenant_id = tenant_id,
            bucket = bucket,
            file_id = file_id,
            "reading file metadata"
        );
        let request = StatRequest {
            tenant_id: tenant_id.to_string(),
            bucket: bucket.to_string(),
            file_id: file_id.to_string(),
        };
        self.unary("failed to stat file", request, |request| {
            let mut upstream = self.upstream_file.clone();
            async move { upstream.stat(request).await }
        })
        .await
    }

    /// Return one paginated page of bucket names holding at least one file.
    pub async fn list_buckets(
        &self,
        tenant_id: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<Page<String>> {
        debug!(
            tenant_id = tenant_id,
            limit = limit.unwrap_or(DEFAULT_PAGE_SIZE),
            cursor = cursor.unwrap_or(""),
            "listing buckets"
        );
        let request = ListBucketsRequest {
            tenant_id: tenant_id.to_string(),
            page: page_request(limit, cursor)?,
        };
        let response = self
            .unary("failed to list buckets", request, |request| {
                let mut upstream = self.upstream_file.clone();
                async move { upstream.list_buckets(request).await }
            })
            .await?;
        Ok(Page {
            items: response.buckets,
            next_cursor: response.page.and_then(|page| non_empty(page.next_cursor)),
        })
    }

    /// Return one paginated page of file ids stored in one bucket.
    pub async fn list_files(
        &self,
        tenant_id: &str,
        bucket: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<Page<String>> {
        debug!(
            tenant_id = tenant_id,
            bucket = bucket,
            limit = limit.unwrap_or(DEFAULT_PAGE_SIZE),
            cursor = cursor.unwrap_or(""),
            "listing files"
        );
        let request = ListFilesRequest {
            tenant_id: tenant_id.to_string(),
            bucket: bucket.to_string(),
            page: page_request(limit, cursor)?,
        };
        let response = self
            .unary("failed to list files", request, |request| {
                let mut upstream = self.upstream_file.clone();
                async move { upstream.list_files(request).await }
            })
            .await?;
        Ok(Page {
            items: response.file_ids,
            next_cursor: response.page.and_then(|page| non_empty(page.next_cursor)),
        })
    }

    /// Delete one stored file.
    ///
    /// **Idempotent**, like [`RociaDbClient::delete_document`] and
    /// [`RociaDbClient::delete_edge`]: deleting a `file_id` that does not
    /// exist succeeds and touches nothing. Call
    /// [`RociaDbClient::stat_file`] first when you need to know whether the
    /// file was there.
    ///
    /// See the [idempotency key
    /// defaults](WriteOptions#idempotency-key-defaults) for the key used
    /// when [`WriteOptions::request_id`] is unset.
    pub async fn delete_file(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
        options: WriteOptions,
    ) -> Result<()> {
        debug!(
            tenant_id = tenant_id,
            bucket = bucket,
            file_id = file_id,
            "deleting file"
        );
        let request = DeleteRequest {
            tenant_id: tenant_id.to_string(),
            bucket: bucket.to_string(),
            file_id: file_id.to_string(),
            request_id: options
                .request_id
                .unwrap_or_else(|| format!("delete_file:{}", Uuid::new_v4())),
        };
        self.unary("failed to delete file", request, |request| {
            let mut upstream = self.upstream_file.clone();
            async move { upstream.delete(request).await }
        })
        .await?;
        Ok(())
    }
}

/// Default idempotency key for an `Upload` call, shared by
/// [`RociaDbClient::upload_file`] and
/// [`RociaDbClient::upload_file_chunked`] so the prefix never depends on
/// which of the two produced the upload.
fn default_upload_file_request_id() -> String {
    format!("upload_file:{}", Uuid::new_v4())
}

/// Resolve the checksum to send: the caller's when they supplied one, or the
/// SHA-256 digest of `bytes` computed here. Pure and network-free — and
/// infallible, since `[u8; 32]` makes the length the compiler's business
/// rather than a runtime check.
fn resolve_checksum(checksum: Option<[u8; 32]>, bytes: &[u8]) -> [u8; 32] {
    checksum.unwrap_or_else(|| Sha256::digest(bytes).into())
}

/// Validate that `size_bytes` does not exceed [`MAX_FILE_BYTES`] (5 GiB),
/// before any network call. Shared by [`RociaDbClient::upload_file`] and
/// [`RociaDbClient::upload_file_chunked`] so both reject an oversized file
/// with the same client-side error instead of letting the upload run and
/// fail server-side partway through.
fn validate_file_size(size_bytes: u64) -> Result<()> {
    if size_bytes > MAX_FILE_BYTES {
        return Err(RociaDbError::validation(format!(
            "file is {size_bytes} bytes, which exceeds the server's {MAX_FILE_BYTES}-byte \
             (5 GiB) limit"
        )));
    }
    Ok(())
}

/// Lazily build the per-chunk `UploadRequest` sequence for `bytes`.
///
/// Only the first request carries the file metadata (`tenant_id`,
/// `bucket`, `file_id`, `size_bytes`, `content_type`, `checksum`,
/// `request_id`): the server only reads those fields off the first message
/// of the stream (see [`RociaDbClient::upload_file_stream`]), so building
/// them for every chunk would just be wasted clones. Requests are produced
/// on demand as the returned iterator is polled by the outgoing stream,
/// never collected into a `Vec` up front.
///
/// `checksum` is `[u8; 32]` up to here and becomes a `Vec<u8>` only at the
/// wire boundary, where the protobuf field demands one.
///
/// `bytes` is an `Arc` so [`RociaDbClient::upload_file`] can build this
/// sequence twice — once per attempt of its refresh-and-replay — without
/// copying the file. Each chunk is still copied into its own `Vec<u8>`, because
/// that is what the protobuf field is.
fn chunk_upload_requests(
    tenant_id: String,
    bucket: String,
    file_id: String,
    bytes: Arc<Vec<u8>>,
    content_type: String,
    checksum: [u8; 32],
    request_id: String,
) -> impl Iterator<Item = UploadRequest> {
    let size_bytes = bytes.len() as u64;
    // A zero-byte file still needs one message to carry the metadata, even
    // though it has no chunk to store.
    let chunk_count = if bytes.is_empty() {
        1
    } else {
        size_bytes.div_ceil(DEFAULT_CHUNK_SIZE as u64)
    };

    let mut tenant_id = Some(tenant_id);
    let mut bucket = Some(bucket);
    let mut file_id = Some(file_id);
    let mut content_type = Some(content_type);
    let mut checksum = Some(checksum);
    let mut request_id = Some(request_id);

    (0..chunk_count).map(move |index| {
        let start = index as usize * DEFAULT_CHUNK_SIZE;
        let end = (start + DEFAULT_CHUNK_SIZE).min(bytes.len());
        UploadRequest {
            tenant_id: tenant_id.take().unwrap_or_default(),
            bucket: bucket.take().unwrap_or_default(),
            file_id: file_id.take().unwrap_or_default(),
            size_bytes: if index == 0 { size_bytes } else { 0 },
            content_type: content_type.take().unwrap_or_default(),
            checksum: checksum
                .take()
                .map(|checksum| checksum.to_vec())
                .unwrap_or_default(),
            chunk: bytes[start..end].to_vec(),
            request_id: request_id.take().unwrap_or_default(),
        }
    })
}

/// File metadata attached only to the first `UploadRequest` produced by
/// [`rechunk_upload_requests`]; every later message leaves these fields at
/// their protobuf default (see [`chunk_upload_requests`] for why).
struct UploadMetadata {
    tenant_id: String,
    bucket: String,
    file_id: String,
    content_type: String,
    checksum: [u8; 32],
    request_id: String,
}

/// Mutable state driving [`rechunk_upload_requests`]'s `stream::unfold`,
/// boxed and type-erased over the caller's source stream so the state
/// itself stays a plain, non-generic type.
struct RechunkState {
    source: Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send>>,
    /// Bytes accumulated toward the next outgoing chunk. Never allowed to
    /// grow past [`DEFAULT_CHUNK_SIZE`]: every place that adds to it copies
    /// in at most the space remaining before that cap (see
    /// [`RechunkState::ingest`] and [`RechunkState::drain_pending`]), so a
    /// source that yields one huge item — a whole file handed over as a
    /// single [`Bytes`], say — still only ever grows this buffer one
    /// bounded slice at a time, never in a single copy that jumps straight
    /// to the item's full size.
    buffer: Vec<u8>,
    /// The unread tail of a source item that didn't fully fit into
    /// `buffer` when [`RechunkState::ingest`] received it, together with
    /// `pending_offset` marking how much of it has been copied into
    /// `buffer` so far. Drained into `buffer` in further bounded slices by
    /// [`RechunkState::drain_pending`] as room frees up, instead of ever
    /// being copied in all at once. Holding it as the [`Bytes`] the source
    /// yielded keeps this a refcount bump rather than a copy of the tail.
    pending: Bytes,
    /// How many bytes at the front of `pending` have already been copied
    /// into `buffer`. `pending` is reset to an empty [`Bytes`] once this
    /// reaches `pending.len()`, so a fully drained oversized item does not
    /// linger in memory (and, for a [`Bytes`] slice of a larger allocation,
    /// does not keep that allocation alive) waiting to be reused.
    pending_offset: usize,
    size_bytes: u64,
    total_written: u64,
    wrote_any: bool,
    source_exhausted: bool,
    metadata: Option<UploadMetadata>,
    error_slot: Arc<Mutex<Option<RociaDbError>>>,
}

impl RechunkState {
    /// Hand `error` to [`RociaDbClient::upload_file_chunked`] through
    /// `error_slot`, the only channel out of a stream whose item type is a
    /// bare `UploadRequest`. Written once and then read once, after the RPC
    /// settles; the outgoing stream ends immediately afterwards, so nothing
    /// overwrites it.
    fn record_error(&self, error: RociaDbError) {
        let mut guard = self
            .error_slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = Some(error);
    }

    /// Record the "would exceed / falls short of `size_bytes`" validation
    /// error into `error_slot`, so [`RociaDbClient::upload_file_chunked`]
    /// can surface it after the stream this state drives has ended.
    fn record_size_error(&self, message: String) {
        self.record_error(RociaDbError::validation(message));
    }

    /// Record a failed read from the caller's own source, which is neither the
    /// caller's arithmetic (a [`RociaDbError::Validation`]) nor anything the
    /// server said.
    fn record_io_error(&self, error: std::io::Error) {
        self.record_error(RociaDbError::Io {
            context: "the upload chunk stream",
            source: error,
        });
    }

    /// Turn `chunk` into the next `UploadRequest`, attaching the file
    /// metadata only if this is the first request ever produced (mirrors
    /// [`chunk_upload_requests`]'s `index == 0` special case).
    fn next_request(&mut self, chunk: Vec<u8>) -> UploadRequest {
        match self.metadata.take() {
            Some(metadata) => UploadRequest {
                tenant_id: metadata.tenant_id,
                bucket: metadata.bucket,
                file_id: metadata.file_id,
                size_bytes: self.size_bytes,
                content_type: metadata.content_type,
                checksum: metadata.checksum.to_vec(),
                chunk,
                request_id: metadata.request_id,
            },
            None => UploadRequest {
                chunk,
                ..Default::default()
            },
        }
    }

    /// Copy a freshly received source item into `buffer`, never in one
    /// piece larger than the space currently left before
    /// [`DEFAULT_CHUNK_SIZE`]. When `piece` is bigger than that space, only
    /// its head is copied in now; the tail becomes `pending` (with
    /// `pending_offset` marking that the head has already been accounted
    /// for) to be drained in further bounded slices by
    /// [`RechunkState::drain_pending`] on later polls, once `buffer` has
    /// been emptied out by an emitted chunk.
    ///
    /// This is the fix for the failure mode this module's docs warn about:
    /// without it, a single `Vec::extend` call with an oversized `piece`
    /// (for example a caller who already holds the whole file as one
    /// in-memory [`Bytes`] and yields it as a single stream item) would
    /// grow `buffer` straight past one output chunk's worth, buffering
    /// memory proportional to the whole file despite this function's docs
    /// promising otherwise.
    fn ingest(&mut self, piece: Bytes) {
        let space_left = DEFAULT_CHUNK_SIZE - self.buffer.len();
        if piece.len() <= space_left {
            self.buffer.extend_from_slice(&piece);
        } else {
            self.buffer.extend_from_slice(&piece[..space_left]);
            self.pending = piece;
            self.pending_offset = space_left;
        }
    }

    /// `true` while `pending` still holds bytes that have not yet been
    /// copied into `buffer`.
    fn has_pending(&self) -> bool {
        self.pending_offset < self.pending.len()
    }

    /// Copy as much of the unread tail of `pending` into `buffer` as fits
    /// in the space left before [`DEFAULT_CHUNK_SIZE`], advancing
    /// `pending_offset`, and free `pending` entirely once it has all been
    /// copied over. Called ahead of pulling the next item from `source`,
    /// so an oversized item already in `pending` finishes draining — in
    /// chunk-sized slices, interleaved with emitting the chunks `buffer`
    /// fills up to — before any more memory is pulled in from upstream.
    fn drain_pending(&mut self) {
        if !self.has_pending() {
            return;
        }
        let space_left = DEFAULT_CHUNK_SIZE - self.buffer.len();
        let available = self.pending.len() - self.pending_offset;
        let take = space_left.min(available);
        let end = self.pending_offset + take;
        self.buffer
            .extend_from_slice(&self.pending[self.pending_offset..end]);
        self.pending_offset = end;
        if !self.has_pending() {
            self.pending = Bytes::new();
            self.pending_offset = 0;
        }
    }
}

/// Re-chunk an arbitrarily-sized byte stream into `UploadRequest` messages
/// of exactly [`DEFAULT_CHUNK_SIZE`] (1 MiB) each — the last one possibly
/// shorter — the core of [`RociaDbClient::upload_file_chunked`]. Never
/// buffers more than one outgoing chunk's worth of bytes at a time, unlike
/// [`chunk_upload_requests`], which already holds the complete file in
/// memory by the time it runs. That bound holds regardless of how `chunks`
/// happens to be split: a single source item larger than one chunk — even
/// one as large as the whole file — is still copied into the outgoing
/// buffer through [`RechunkState::ingest`] and [`RechunkState::drain_pending`]
/// a bounded slice at a time rather than in one `extend` call, so it can
/// never grow the buffer past [`DEFAULT_CHUNK_SIZE`].
///
/// Validates as it goes: a chunk that would push the running total past
/// `size_bytes` is rejected *before* being turned into a request (so it is
/// never sent), and running short of `size_bytes` once `chunks` is
/// exhausted is detected right after the last real chunk. An `Err` item from
/// `chunks` ends everything at once — nothing further is pulled from the
/// source, no further request is emitted, and the [`std::io::Error`] is
/// reported as [`RociaDbError::Io`]. Because the returned
/// `Stream<Item = UploadRequest>` has no channel of its own to carry an error
/// — a tonic client-streaming call only accepts a stream that produces
/// requests, never `Result`s — any such failure is recorded into `error_slot`
/// instead, and the stream simply ends early (or, for a short source, ends
/// normally after reporting the mismatch). The caller (see
/// [`RociaDbClient::upload_file_chunked`]) checks `error_slot` once the RPC
/// settles.
///
/// An empty source (`size_bytes` 0, no bytes at all) still produces exactly
/// one empty request, because the server only learns the file's metadata
/// from a message, and an upload that writes nothing would never deliver
/// it — the same rule [`chunk_upload_requests`] applies for a zero-byte
/// in-memory buffer.
#[allow(clippy::too_many_arguments)]
fn rechunk_upload_requests<S>(
    tenant_id: String,
    bucket: String,
    file_id: String,
    size_bytes: u64,
    content_type: String,
    checksum: [u8; 32],
    request_id: String,
    chunks: S,
    error_slot: Arc<Mutex<Option<RociaDbError>>>,
) -> impl Stream<Item = UploadRequest>
where
    S: Stream<Item = std::io::Result<Bytes>> + Send + 'static,
{
    let state = RechunkState {
        source: Box::pin(chunks),
        buffer: Vec::new(),
        pending: Bytes::new(),
        pending_offset: 0,
        size_bytes,
        total_written: 0,
        wrote_any: false,
        source_exhausted: false,
        metadata: Some(UploadMetadata {
            tenant_id,
            bucket,
            file_id,
            content_type,
            checksum,
            request_id,
        }),
        error_slot,
    };

    stream::unfold(state, |mut state| async move {
        loop {
            if state.buffer.len() >= DEFAULT_CHUNK_SIZE {
                let piece_len = DEFAULT_CHUNK_SIZE as u64;
                if state.total_written + piece_len > state.size_bytes {
                    state.record_size_error(format!(
                        "upload_file_chunked received more data than size_bytes \
                         ({} bytes) declared",
                        state.size_bytes
                    ));
                    return None;
                }
                let piece: Vec<u8> = state.buffer.drain(..DEFAULT_CHUNK_SIZE).collect();
                state.total_written += piece_len;
                state.wrote_any = true;
                let request = state.next_request(piece);
                return Some((request, state));
            }

            // Drain any tail left over from an oversized source item before
            // pulling more data in, so it empties out in bounded slices —
            // interleaved with the chunk emissions above — rather than
            // sitting fully copied in `buffer` or growing it past
            // `DEFAULT_CHUNK_SIZE` on some later `ingest` call.
            if state.has_pending() {
                state.drain_pending();
                continue;
            }

            if !state.source_exhausted {
                match state.source.next().await {
                    Some(Ok(piece)) => {
                        state.ingest(piece);
                        continue;
                    }
                    Some(Err(error)) => {
                        // The caller's source failed. Stop pulling from it —
                        // returning `None` here drops the state, and with it
                        // the source — and end the outgoing stream, whatever
                        // is still buffered: sending the bytes read so far
                        // would only produce a truncated upload the server
                        // would then reject for a reason that has nothing to
                        // do with what went wrong. The recorded error wins
                        // over that rejection in `upload_file_chunked`.
                        state.record_io_error(error);
                        return None;
                    }
                    None => {
                        state.source_exhausted = true;
                        continue;
                    }
                }
            }

            // Source exhausted, less than one full chunk buffered: flush
            // the remainder (possibly empty, for a zero-byte file).
            if !state.buffer.is_empty() || !state.wrote_any {
                let piece_len = state.buffer.len() as u64;
                if state.total_written + piece_len > state.size_bytes {
                    state.record_size_error(format!(
                        "upload_file_chunked received more data than size_bytes \
                         ({} bytes) declared",
                        state.size_bytes
                    ));
                    return None;
                }
                state.total_written += piece_len;
                state.wrote_any = true;
                let piece = std::mem::take(&mut state.buffer);
                let request = state.next_request(piece);
                return Some((request, state));
            }

            if state.total_written != state.size_bytes {
                state.record_size_error(format!(
                    "upload_file_chunked sent {} bytes but size_bytes declared {}",
                    state.total_written, state.size_bytes
                ));
            }
            return None;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_CHUNK_SIZE, FileStreamUploadOptions, FileUploadOptions, MAX_FILE_BYTES,
        MAX_PREALLOCATED_DOWNLOAD_BYTES, RechunkState, chunk_upload_requests,
        default_upload_file_request_id, rechunk_upload_requests, resolve_checksum,
        validate_file_size,
    };
    use crate::pb::upstream::v1::UploadRequest;
    use crate::test_support::lazy_test_client;
    use crate::{Bytes, RociaDbError};
    use futures::executor::block_on;
    use futures::{StreamExt, stream};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    /// Length of a SHA-256 digest. The production code no longer needs this
    /// as a constant — `[u8; 32]` carries it — but the tests still assert
    /// against the number itself.
    const CHECKSUM_LEN: usize = 32;

    /// One source item, as `upload_file_chunked`'s stream yields them.
    fn chunk(bytes: Vec<u8>) -> std::io::Result<Bytes> {
        Ok(Bytes::from(bytes))
    }

    /// A failing source item, standing in for a read that died partway
    /// through the caller's file or socket.
    fn read_failure() -> std::io::Result<Bytes> {
        Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "the source stopped reading",
        ))
    }

    #[test]
    fn upload_options_have_safe_defaults() {
        let options = FileUploadOptions::new();
        assert_eq!(options, FileUploadOptions::default());
        assert_eq!(options.content_type, "application/octet-stream");
        assert!(options.checksum.is_none());
        assert!(options.request_id.is_none());
    }

    #[test]
    fn upload_options_setters_are_chainable_and_readable() {
        let options = FileUploadOptions::new()
            .with_content_type("text/csv")
            .with_checksum([7u8; CHECKSUM_LEN])
            .with_request_id("retry-1");
        assert_eq!(options.content_type, "text/csv");
        assert_eq!(options.checksum, Some([7u8; CHECKSUM_LEN]));
        assert_eq!(options.request_id.as_deref(), Some("retry-1"));
    }

    #[test]
    fn file_stream_upload_options_require_size_and_checksum_and_default_the_rest() {
        let options = FileStreamUploadOptions::new(1234, [3u8; CHECKSUM_LEN]);
        assert_eq!(options.size_bytes, 1234);
        assert_eq!(options.checksum, [3u8; CHECKSUM_LEN]);
        assert_eq!(options.content_type, "application/octet-stream");
        assert!(options.request_id.is_none());

        let options = options
            .with_content_type("application/pdf")
            .with_request_id("retry-2");
        assert_eq!(options.content_type, "application/pdf");
        assert_eq!(options.request_id.as_deref(), Some("retry-2"));
        // The required fields survive the chained setters.
        assert_eq!(options.size_bytes, 1234);
        assert_eq!(options.checksum, [3u8; CHECKSUM_LEN]);
    }

    #[test]
    fn default_upload_request_id_uses_the_upload_file_prefix_with_a_fresh_uuid_each_time() {
        let first = default_upload_file_request_id();
        let second = default_upload_file_request_id();
        let uuid_part = first
            .strip_prefix("upload_file:")
            .expect("default request_id must use the upload_file: prefix");
        uuid::Uuid::parse_str(uuid_part).expect("suffix after the prefix must be a uuid");
        assert_ne!(first, second, "each call must mint a fresh idempotency key");
    }

    #[test]
    fn upload_requests_chunk_at_exactly_one_mebibyte() {
        let bytes = vec![7u8; DEFAULT_CHUNK_SIZE + 10];
        let requests: Vec<_> = chunk_upload_requests(
            "tenant".into(),
            "bucket".into(),
            "file".into(),
            Arc::new(bytes.clone()),
            "text/plain".into(),
            [0u8; CHECKSUM_LEN],
            "stable-request".into(),
        )
        .collect();

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].chunk.len(), DEFAULT_CHUNK_SIZE);
        assert_eq!(requests[1].chunk.len(), 10);
        assert_eq!(requests[0].size_bytes, bytes.len() as u64);
        // Only the first message carries metadata; the server ignores the
        // rest of the fields on later messages.
        assert_eq!(requests[1].size_bytes, 0);
        assert_eq!(requests[0].tenant_id, "tenant");
        assert!(requests[1].tenant_id.is_empty());
        assert_eq!(requests[0].checksum.len(), CHECKSUM_LEN);
        assert!(requests[1].checksum.is_empty());
        assert!(
            requests
                .iter()
                .all(|request| request.request_id == "stable-request"
                    || request.request_id.is_empty())
        );
        assert_eq!(requests[0].request_id, "stable-request");
    }

    #[test]
    fn empty_upload_still_emits_one_request() {
        let requests: Vec<_> = chunk_upload_requests(
            "tenant".into(),
            "bucket".into(),
            "file".into(),
            Arc::new(Vec::new()),
            FileUploadOptions::default().content_type,
            [0u8; CHECKSUM_LEN],
            "req".into(),
        )
        .collect();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].size_bytes, 0);
        assert!(requests[0].chunk.is_empty());
    }

    /// Asserts that chunking `total_bytes` matches the wire contract
    /// described in [`crate::RociaDbClient::upload_file_stream`]: every
    /// chunk but the last is exactly 1 MiB, the last is non-empty and no
    /// larger than 1 MiB, and the sum of chunk bytes equals `size_bytes`.
    fn assert_chunking_matches_server_contract(total_bytes: usize) {
        let bytes = vec![9u8; total_bytes];
        let requests: Vec<_> = chunk_upload_requests(
            "tenant".into(),
            "bucket".into(),
            "file".into(),
            Arc::new(bytes.clone()),
            "application/octet-stream".into(),
            [0u8; CHECKSUM_LEN],
            "req".into(),
        )
        .collect();

        assert!(!requests.is_empty(), "at least one message is required");
        assert_eq!(requests[0].size_bytes, total_bytes as u64);

        let bytes_sent: usize = requests.iter().map(|request| request.chunk.len()).sum();
        assert_eq!(
            bytes_sent, total_bytes,
            "sum of chunk bytes must equal size_bytes exactly"
        );

        if total_bytes == 0 {
            assert_eq!(requests.len(), 1);
            assert!(requests[0].chunk.is_empty());
            return;
        }

        let (last, all_but_last) = requests.split_last().expect("at least one request");
        for request in all_but_last {
            assert_eq!(
                request.chunk.len(),
                DEFAULT_CHUNK_SIZE,
                "every chunk but the last must be exactly 1 MiB"
            );
        }
        assert!(!last.chunk.is_empty(), "the last chunk must not be empty");
        assert!(
            last.chunk.len() <= DEFAULT_CHUNK_SIZE,
            "the last chunk must not exceed 1 MiB"
        );
    }

    #[test]
    fn chunking_zero_bytes() {
        assert_chunking_matches_server_contract(0);
    }

    #[test]
    fn chunking_one_byte() {
        assert_chunking_matches_server_contract(1);
    }

    #[test]
    fn chunking_exactly_one_mebibyte() {
        assert_chunking_matches_server_contract(DEFAULT_CHUNK_SIZE);
    }

    #[test]
    fn chunking_one_mebibyte_plus_one_byte() {
        assert_chunking_matches_server_contract(DEFAULT_CHUNK_SIZE + 1);
    }

    #[test]
    fn chunking_about_two_and_a_half_mebibytes() {
        assert_chunking_matches_server_contract(DEFAULT_CHUNK_SIZE * 2 + DEFAULT_CHUNK_SIZE / 2);
    }

    #[test]
    fn resolve_checksum_computes_sha256_by_default() {
        // Known-answer test for SHA-256("hello world"), independent of the
        // crate's own `Sha256::digest` call, so a wiring mistake (wrong
        // input bytes, wrong algorithm) would be caught even if it still
        // happened to produce 32 bytes.
        let checksum = resolve_checksum(None, b"hello world");
        assert_eq!(
            checksum.to_vec(),
            decode_hex("b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9")
        );
    }

    #[test]
    fn resolve_checksum_is_deterministic_and_content_dependent() {
        let first = resolve_checksum(None, b"payload-a");
        let second = resolve_checksum(None, b"payload-a");
        let different = resolve_checksum(None, b"payload-b");
        assert_eq!(first, second, "same bytes must yield the same checksum");
        assert_ne!(
            first, different,
            "different bytes must yield a different checksum"
        );
    }

    #[test]
    fn resolve_checksum_accepts_a_caller_supplied_digest_verbatim() {
        let supplied = [7u8; CHECKSUM_LEN];
        let checksum = resolve_checksum(Some(supplied), b"irrelevant");
        assert_eq!(
            checksum, supplied,
            "a caller-supplied digest must be sent as-is, never recomputed"
        );
    }

    fn decode_hex(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex pair"))
            .collect()
    }

    #[test]
    fn the_verified_download_preallocation_is_capped_well_below_the_file_size_limit() {
        // The point of the cap: `download_file_verified` reserves
        // `min(size_bytes, cap)` up front, so the biggest allocation a
        // server can provoke with a made-up `size_bytes` is the cap — not
        // the 5 GiB a file is actually allowed to reach, and not the 2^64
        // an unchecked `u64` would allow.
        assert_eq!(MAX_PREALLOCATED_DOWNLOAD_BYTES, 64 * 1024 * 1024);
        const {
            assert!(MAX_PREALLOCATED_DOWNLOAD_BYTES < MAX_FILE_BYTES);
        }
        for reported in [0, 1, 4096, MAX_PREALLOCATED_DOWNLOAD_BYTES, u64::MAX] {
            let capacity = usize::try_from(reported.min(MAX_PREALLOCATED_DOWNLOAD_BYTES))
                .unwrap_or(usize::MAX);
            assert!(
                capacity as u64 <= MAX_PREALLOCATED_DOWNLOAD_BYTES,
                "a reported size of {reported} must never reserve more than the cap"
            );
        }
    }

    #[test]
    fn validate_file_size_accepts_exactly_the_5_gib_limit() {
        validate_file_size(MAX_FILE_BYTES).expect("exactly the limit must be accepted");
    }

    #[test]
    fn validate_file_size_rejects_one_byte_over_the_5_gib_limit() {
        let error = validate_file_size(MAX_FILE_BYTES + 1)
            .expect_err("one byte over the limit must be rejected");
        assert!(matches!(error, RociaDbError::Validation(_)));
        assert!(error.to_string().contains("5 GiB"));
    }

    // `upload_file_chunked`'s pre-flight size validation must run — and
    // fail — before the method ever touches the network, so this runs
    // against a client wired to an unreachable host and must still return
    // promptly. (The checksum length is no longer validated at runtime:
    // `[u8; 32]` makes a wrong length unrepresentable.)
    #[tokio::test]
    async fn upload_file_chunked_rejects_an_oversized_file_before_any_network_call() {
        let client = lazy_test_client();
        let oversized = MAX_FILE_BYTES + 1;
        let error = client
            .upload_file_chunked(
                "tenant",
                "bucket",
                "file",
                stream::empty::<std::io::Result<Bytes>>(),
                FileStreamUploadOptions::new(oversized, [0u8; CHECKSUM_LEN]),
            )
            .await
            .expect_err("a file over the 5 GiB limit must be rejected");
        assert!(matches!(error, RociaDbError::Validation(_)));
        assert!(error.to_string().contains("5 GiB"));
    }

    /// Drives [`rechunk_upload_requests`] to completion against an
    /// in-memory source and returns the produced requests, whatever error
    /// ended up in `error_slot`, and how many items were actually pulled from
    /// the source — the last of which is what proves nothing is pulled after
    /// an `Err`. No network, no tokio runtime needed: `stream::iter` resolves
    /// synchronously, so `futures::executor::block_on` alone is enough to
    /// drive the `stream::unfold` chain to its end.
    fn collect_rechunked_source(
        size_bytes: u64,
        source_items: Vec<std::io::Result<Bytes>>,
    ) -> (Vec<UploadRequest>, Option<RociaDbError>, usize) {
        let error_slot: Arc<Mutex<Option<RociaDbError>>> = Arc::new(Mutex::new(None));
        let pulled = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&pulled);
        let source = stream::iter(source_items).inspect(move |_| {
            counter.fetch_add(1, Ordering::Relaxed);
        });
        let requests: Vec<UploadRequest> = block_on(
            rechunk_upload_requests(
                "tenant".into(),
                "bucket".into(),
                "file".into(),
                size_bytes,
                "application/octet-stream".into(),
                [0u8; CHECKSUM_LEN],
                "req".into(),
                source,
                Arc::clone(&error_slot),
            )
            .collect::<Vec<_>>(),
        );
        let error = error_slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        (requests, error, pulled.load(Ordering::Relaxed))
    }

    /// [`collect_rechunked_source`] for a source that never fails, which is
    /// every test but the three about a failing one.
    fn collect_rechunked(
        size_bytes: u64,
        source_pieces: Vec<Vec<u8>>,
    ) -> (Vec<UploadRequest>, Option<RociaDbError>) {
        let (requests, error, _pulled) =
            collect_rechunked_source(size_bytes, source_pieces.into_iter().map(chunk).collect());
        (requests, error)
    }

    #[test]
    fn rechunk_exact_multiple_of_one_mebibyte_has_no_trailing_empty_chunk() {
        let total = DEFAULT_CHUNK_SIZE * 2;
        let (requests, error) = collect_rechunked(total as u64, vec![vec![5u8; total]]);
        assert!(error.is_none(), "unexpected validation error: {error:?}");
        assert_eq!(
            requests.len(),
            2,
            "an exact multiple of the chunk size must not emit a trailing empty request"
        );
        assert_eq!(requests[0].chunk.len(), DEFAULT_CHUNK_SIZE);
        assert_eq!(requests[1].chunk.len(), DEFAULT_CHUNK_SIZE);
        // Only the first message carries metadata, exactly like
        // `chunk_upload_requests`.
        assert_eq!(requests[0].tenant_id, "tenant");
        assert!(requests[1].tenant_id.is_empty());
        assert_eq!(requests[0].size_bytes, total as u64);
        assert_eq!(requests[1].size_bytes, 0);
        assert_eq!(requests[0].checksum.len(), CHECKSUM_LEN);
        assert!(requests[1].checksum.is_empty());
    }

    #[test]
    fn rechunk_non_multiple_ends_with_a_short_last_chunk() {
        let total = DEFAULT_CHUNK_SIZE + 100;
        let (requests, error) = collect_rechunked(total as u64, vec![vec![9u8; total]]);
        assert!(error.is_none(), "unexpected validation error: {error:?}");
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].chunk.len(), DEFAULT_CHUNK_SIZE);
        assert_eq!(requests[1].chunk.len(), 100);
    }

    #[test]
    fn rechunk_reassembles_many_small_source_pieces_byte_for_byte() {
        // Feed the re-chunker a source split into many small (64 KiB)
        // pieces — nothing like the 1 MiB output chunk size — with a
        // distinctive byte pattern so any misordering or off-by-one
        // slicing bug would be caught, not just the total byte count.
        let piece_len = 64 * 1024;
        let piece_count = 40; // ~2.5 MiB total: spans multiple 1 MiB output chunks
        let mut expected = Vec::new();
        let mut pieces = Vec::new();
        for i in 0..piece_count {
            let piece: Vec<u8> = (0..piece_len).map(|b| ((i * 7 + b) % 256) as u8).collect();
            expected.extend_from_slice(&piece);
            pieces.push(piece);
        }
        let total = expected.len() as u64;
        let (requests, error) = collect_rechunked(total, pieces);
        assert!(error.is_none(), "unexpected validation error: {error:?}");

        let reassembled: Vec<u8> = requests.iter().flat_map(|r| r.chunk.clone()).collect();
        assert_eq!(
            reassembled, expected,
            "reassembled bytes must exactly match the source, regardless of how it was chunked \
             on input"
        );

        let (last, all_but_last) = requests.split_last().expect("at least one request");
        for request in all_but_last {
            assert_eq!(
                request.chunk.len(),
                DEFAULT_CHUNK_SIZE,
                "every chunk but the last must be exactly 1 MiB"
            );
        }
        assert!(!last.chunk.is_empty());
        assert!(last.chunk.len() <= DEFAULT_CHUNK_SIZE);
    }

    #[test]
    fn rechunk_zero_byte_file_still_emits_one_metadata_carrying_request() {
        let (requests, error) = collect_rechunked(0, vec![]);
        assert!(error.is_none(), "unexpected validation error: {error:?}");
        assert_eq!(requests.len(), 1);
        assert!(requests[0].chunk.is_empty());
        assert_eq!(requests[0].size_bytes, 0);
        assert_eq!(
            requests[0].tenant_id, "tenant",
            "the sole request of an empty upload must still carry file metadata, otherwise the \
             server never learns about the file"
        );
    }

    #[test]
    fn rechunk_rejects_more_data_than_declared_size_bytes_before_sending_the_offending_chunk() {
        let declared = DEFAULT_CHUNK_SIZE as u64; // caller declares only 1 MiB
        // the source produces 2 MiB in a single piece
        let (requests, error) =
            collect_rechunked(declared, vec![vec![1u8; DEFAULT_CHUNK_SIZE * 2]]);
        let error = error.expect("an overflow must be recorded as a validation error");
        assert!(matches!(error, RociaDbError::Validation(_)));
        assert!(error.to_string().contains("more data than size_bytes"));
        let sent: usize = requests.iter().map(|r| r.chunk.len()).sum();
        assert!(
            sent <= declared as usize,
            "the chunk that would push the total past size_bytes must never be sent, got \
             {sent} bytes sent for a {declared}-byte declared size"
        );
    }

    #[test]
    fn rechunk_reports_a_shortfall_once_the_source_is_exhausted() {
        let declared = (DEFAULT_CHUNK_SIZE * 2) as u64; // caller declares 2 MiB
        // the source only ever produces 1 MiB
        let (requests, error) = collect_rechunked(declared, vec![vec![3u8; DEFAULT_CHUNK_SIZE]]);
        let error = error.expect("a shortfall must be recorded as a validation error");
        assert!(matches!(error, RociaDbError::Validation(_)));
        let message = error.to_string();
        assert!(message.contains("sent"));
        assert!(message.contains("but size_bytes declared"));
        let sent: usize = requests.iter().map(|r| r.chunk.len()).sum();
        assert_eq!(sent, DEFAULT_CHUNK_SIZE);
    }

    #[test]
    fn rechunk_honors_caller_supplied_request_id_and_content_type_on_the_first_request_only() {
        let error_slot: Arc<Mutex<Option<RociaDbError>>> = Arc::new(Mutex::new(None));
        let requests: Vec<UploadRequest> = block_on(
            rechunk_upload_requests(
                "tenant".into(),
                "bucket".into(),
                "file".into(),
                10,
                "text/csv".into(),
                [0u8; CHECKSUM_LEN],
                "caller-request-id".into(),
                stream::iter(vec![chunk(vec![1u8; 10])]),
                Arc::clone(&error_slot),
            )
            .collect::<Vec<_>>(),
        );
        assert!(
            error_slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_none()
        );
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].content_type, "text/csv");
        assert_eq!(requests[0].request_id, "caller-request-id");
    }

    /// Asserts that `error` is the [`RociaDbError::Io`] the three failing-source
    /// tests expect, carrying the source's own `io::Error` rather than a
    /// message about byte counts.
    fn assert_is_the_source_read_failure(error: Option<RociaDbError>) {
        let error = error.expect("a failing source must be recorded as an error");
        let RociaDbError::Io { context, source } = &error else {
            panic!("a failing source must produce RociaDbError::Io, got: {error}");
        };
        assert_eq!(*context, "the upload chunk stream");
        assert_eq!(source.kind(), std::io::ErrorKind::UnexpectedEof);
        let message = error.to_string();
        assert!(
            message.contains("the source stopped reading"),
            "Display must fold in the source's own message, got: {message}"
        );
        assert!(
            !message.contains("size_bytes"),
            "a read failure must not be reported as a size mismatch, got: {message}"
        );
    }

    #[test]
    fn rechunk_surfaces_a_source_failure_on_the_very_first_item() {
        // Zero items read before the failure: the upload must fail with the
        // I/O error and send nothing at all.
        let (requests, error, pulled) =
            collect_rechunked_source(4096, vec![read_failure(), chunk(vec![1u8; 4096])]);
        assert_is_the_source_read_failure(error);
        assert!(
            requests.is_empty(),
            "nothing may be sent when the source fails before producing a byte, got {} requests",
            requests.len()
        );
        assert_eq!(
            pulled, 1,
            "the item after the failure must never be pulled from the source"
        );
    }

    #[test]
    fn rechunk_surfaces_a_source_failure_after_one_item() {
        // One short item, then a failure: the buffered bytes are dropped
        // rather than sent as a truncated file, and the error is the read's.
        let (requests, error, pulled) = collect_rechunked_source(
            (DEFAULT_CHUNK_SIZE * 2) as u64,
            vec![chunk(vec![7u8; 1024]), read_failure(), chunk(vec![9u8; 16])],
        );
        assert_is_the_source_read_failure(error);
        assert!(
            requests.is_empty(),
            "a partial first chunk must not be emitted once the source has failed"
        );
        assert_eq!(pulled, 2, "nothing after the failing item may be pulled");
    }

    #[test]
    fn rechunk_surfaces_a_source_failure_after_several_items_and_emitted_chunks() {
        // Enough bytes to have emitted two full 1 MiB messages before the
        // failure: the messages already sent stand (they are on the wire), the
        // upload still fails, and the error still names the read rather than
        // the truncated total.
        let (requests, error, pulled) = collect_rechunked_source(
            (DEFAULT_CHUNK_SIZE * 4) as u64,
            vec![
                chunk(vec![1u8; DEFAULT_CHUNK_SIZE]),
                chunk(vec![2u8; DEFAULT_CHUNK_SIZE]),
                chunk(vec![3u8; 512]),
                read_failure(),
                chunk(vec![4u8; DEFAULT_CHUNK_SIZE]),
                chunk(vec![5u8; DEFAULT_CHUNK_SIZE]),
            ],
        );
        assert_is_the_source_read_failure(error);
        assert_eq!(
            requests.len(),
            2,
            "only the chunks that were complete before the failure may have been sent"
        );
        assert!(
            requests
                .iter()
                .all(|request| request.chunk.len() == DEFAULT_CHUNK_SIZE)
        );
        assert_eq!(
            pulled, 4,
            "the source must be polled exactly up to and including the failing item"
        );
    }

    // The other half of the failing-source contract — that the recorded
    // `RociaDbError::Io` wins over the status the *server* returns for the
    // stream that then ended early — needs a server to return a status at all,
    // so it lives in `tests/files.rs` against the in-process one. Against an
    // unreachable host there is nothing to assert: tonic fails the call at the
    // connection stage, before the request stream is ever polled, so the source
    // never gets the chance to fail.

    #[test]
    fn ingest_never_grows_the_buffer_past_one_chunk_for_a_single_oversized_item() {
        // Regression test for the unbounded-buffer bug: a caller who
        // already held the whole file as one in-memory buffer and
        // yielded it as a single stream item used to have that whole item
        // copied into `buffer` by one `Vec::extend` call, defeating the
        // "never buffers more than one outgoing chunk" bound
        // `rechunk_upload_requests` promises.
        // `RechunkState::ingest`/`RechunkState::drain_pending`
        // are exercised directly here (rather than through the async
        // `rechunk_upload_requests` pipeline) so `buffer.len()` can be
        // asserted at every intermediate step, not just inferred from the
        // sizes of the `UploadRequest`s that eventually come out the other
        // end.
        let error_slot: Arc<Mutex<Option<RociaDbError>>> = Arc::new(Mutex::new(None));
        let mut state = RechunkState {
            source: Box::pin(stream::empty::<std::io::Result<Bytes>>()),
            buffer: Vec::new(),
            pending: Bytes::new(),
            pending_offset: 0,
            size_bytes: 0,
            total_written: 0,
            wrote_any: false,
            source_exhausted: false,
            metadata: None,
            error_slot,
        };

        let oversized = Bytes::from(vec![42u8; DEFAULT_CHUNK_SIZE * 5 + 7]);
        state.ingest(oversized.clone());
        assert!(
            state.buffer.len() <= DEFAULT_CHUNK_SIZE,
            "a single ingest() call must never grow the buffer past one output chunk, got {} \
             bytes for a {}-byte item",
            state.buffer.len(),
            oversized.len()
        );

        // Drain exactly like `rechunk_upload_requests`'s `stream::unfold`
        // loop does: emit the buffer once it reaches a full chunk,
        // otherwise pull more of the oversized item's tail out of
        // `pending` — checking the bound holds at every step, not just
        // right after the first `ingest()` call.
        let mut reassembled = Vec::new();
        loop {
            if state.buffer.len() >= DEFAULT_CHUNK_SIZE {
                reassembled.extend(state.buffer.drain(..DEFAULT_CHUNK_SIZE));
            } else if state.has_pending() {
                state.drain_pending();
            } else {
                break;
            }
            assert!(
                state.buffer.len() <= DEFAULT_CHUNK_SIZE,
                "buffer must stay bounded by one output chunk at every step while draining an \
                 oversized item, got {} bytes",
                state.buffer.len()
            );
        }
        reassembled.append(&mut state.buffer);

        assert_eq!(
            reassembled, oversized,
            "draining an oversized item through ingest()/drain_pending() must reproduce it \
             byte-for-byte, with no bytes lost, duplicated, or reordered"
        );
    }

    #[test]
    fn rechunk_reassembles_a_single_oversized_source_item_via_the_full_pipeline() {
        // Companion to the `ingest`/`drain_pending` test above: the same
        // scenario (one source item several times larger than
        // `DEFAULT_CHUNK_SIZE`) driven through the full async
        // `rechunk_upload_requests` pipeline, confirming the fix holds
        // end-to-end and not only at the state-machine level.
        let total = DEFAULT_CHUNK_SIZE * 3 + 12345;
        let oversized: Vec<u8> = (0..total).map(|b| (b % 251) as u8).collect();
        let (requests, error) = collect_rechunked(total as u64, vec![oversized.clone()]);
        assert!(error.is_none(), "unexpected validation error: {error:?}");

        let reassembled: Vec<u8> = requests.iter().flat_map(|r| r.chunk.clone()).collect();
        assert_eq!(
            reassembled, oversized,
            "a single oversized stream item must still reassemble byte-for-byte"
        );

        let (last, all_but_last) = requests.split_last().expect("at least one request");
        for request in all_but_last {
            assert_eq!(
                request.chunk.len(),
                DEFAULT_CHUNK_SIZE,
                "every chunk but the last must still be exactly one full chunk"
            );
        }
        assert!(!last.chunk.is_empty());
        assert!(last.chunk.len() <= DEFAULT_CHUNK_SIZE);
    }
}
