//! File upload/download helpers.
//!
//! The upstream server accepts upload chunks up to 1 MiB (1_048_576 bytes)
//! per message — anything larger is rejected outright with
//! `INVALID_ARGUMENT` — and on download reads chunks back until it has
//! collected `size_bytes` bytes in total, without assuming any particular
//! chunk size.
//!
//! [`RociaDbClient::upload_file`], [`RociaDbClient::upload_file_owned`] and
//! [`RociaDbClient::upload_file_chunked`] all emit exactly-1-MiB chunks (the
//! last one may be shorter): that is the largest message the server allows,
//! so it is also the fewest possible messages for a given file, and it
//! remains the only chunk size that is safe against a server older than
//! `1.0.0-rc.16`.
//!
//! Only reach for [`RociaDbClient::upload_file_stream`] directly if you
//! understand and reproduce the wire contract yourself; see its own docs
//! for what it does and does not validate.
use crate::error::StatusResultExt;
use crate::pb::upstream::v1::{
    DeleteRequest, DownloadRequest, DownloadResponse, ListBucketsRequest, ListFilesRequest,
    StatRequest, StatResponse, UploadRequest,
};
use crate::{Page, Result, RociaDbClient, RociaDbError, non_empty, page_request};
use futures::{Stream, StreamExt, stream};
use sha2::{Digest, Sha256};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tonic::codec::Streaming;
use uuid::Uuid;

/// Size of every upload message the SDK emits, except the last one. Not
/// configurable: see the module docs.
const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024; // 1 MiB.

/// Length in bytes of a SHA-256 digest, required by the server's checksum
/// validation.
const CHECKSUM_LEN: usize = 32;

/// Server-side max file size (`limits.max_file_bytes`, 5 GiB default).
const MAX_FILE_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// Options applied to an ergonomic byte-buffer upload.
///
/// There is intentionally no `chunk_size` knob: see the module docs for why
/// 1 MiB is the only size worth using.
#[derive(Debug, Clone)]
pub struct FileUploadOptions {
    pub content_type: String,
    /// SHA-256 digest of the uploaded bytes, as exactly 32 raw bytes. When
    /// `None`, [`RociaDbClient::upload_file`] computes it from the buffer
    /// automatically. When `Some`, it must be exactly 32 bytes or the
    /// upload fails before any network call — the server rejects any other
    /// length with `INVALID_ARGUMENT`.
    pub checksum: Option<Vec<u8>>,
    pub request_id: Option<String>,
}

impl Default for FileUploadOptions {
    fn default() -> Self {
        Self {
            content_type: "application/octet-stream".to_string(),
            checksum: None,
            request_id: None,
        }
    }
}

/// Options applied to [`RociaDbClient::upload_file_chunked`].
///
/// Unlike [`FileUploadOptions`], there is no `checksum` field here: for a
/// streaming upload the checksum cannot be computed automatically (the
/// whole point is to never hold the complete file in memory), so
/// [`RociaDbClient::upload_file_chunked`] takes it as its own required
/// `checksum` parameter instead of folding it into this struct — keeping it
/// there would wrongly suggest it is optional, the way it genuinely is on
/// [`FileUploadOptions::checksum`].
#[derive(Debug, Clone)]
pub struct FileStreamUploadOptions {
    pub content_type: String,
    pub request_id: Option<String>,
}

impl Default for FileStreamUploadOptions {
    fn default() -> Self {
        Self {
            content_type: "application/octet-stream".to_string(),
            request_id: None,
        }
    }
}

impl RociaDbClient {
    /// Upload a caller-built stream of protobuf `UploadRequest` messages.
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
    ///   caller likes (see the module docs for why this SDK's own
    ///   `upload_file`/`upload_file_chunked` still always choose exactly
    ///   1 MiB chunks even though the server no longer requires it);
    /// - `content_type` and `checksum` on messages after the first are
    ///   ignored by the server and can be left empty.
    ///
    /// A `chunk` over 1 MiB, a checksum of the wrong length, or a
    /// mismatched `size_bytes` all fail the upload outright with
    /// `INVALID_ARGUMENT` rather than corrupting anything silently. The one
    /// thing the server never verifies is whether `checksum` actually
    /// matches the bytes sent — only that it is 32 bytes long — so a wrong
    /// checksum can still produce an upload that looks successful while
    /// carrying bad data.
    ///
    /// For the common case — uploading an in-memory byte buffer — use
    /// [`RociaDbClient::upload_file`] instead, which builds a correct
    /// stream for you; for a large source you cannot buffer but can still
    /// checksum ahead of time, prefer
    /// [`RociaDbClient::upload_file_chunked`], which re-chunks and
    /// validates for you.
    pub async fn upload_file_stream<S>(&self, requests: S) -> Result<()>
    where
        S: Stream<Item = UploadRequest> + Send + 'static,
    {
        let mut upstream_file = self.upstream_file.clone();
        upstream_file
            .upload(requests)
            .await
            .status_context("failed to upload file")?;
        Ok(())
    }

    /// Upload an in-memory byte buffer, split into gRPC messages of the
    /// server's largest allowed chunk size.
    ///
    /// The buffer is always split into 1 MiB (1_048_576-byte) chunks (the
    /// last chunk may be shorter); not configurable, see the module docs
    /// for why. When `options.checksum` is `None`, the SHA-256 digest of
    /// `bytes` is computed and sent automatically; when it is `Some`, it
    /// must be exactly 32 bytes or this returns an error before any
    /// network call. Files over 5 GiB (`limits.max_file_bytes`, the server
    /// default) are rejected client-side with a clear error instead of
    /// failing partway through the upload.
    ///
    /// `bytes` is only ever read here, never consumed, so this necessarily
    /// copies it once to build the owned buffer the chunking step and the
    /// underlying `'static` upload stream require. If you already own a
    /// `Vec<u8>` — the common case for a file freshly read off disk or
    /// assembled in memory — call [`RociaDbClient::upload_file_owned`]
    /// instead: it accepts that `Vec<u8>` directly and moves it straight
    /// into the chunking step with no copy, which matters as file size
    /// approaches the 5 GiB ceiling above.
    pub async fn upload_file(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
        bytes: impl AsRef<[u8]>,
        options: FileUploadOptions,
    ) -> Result<()> {
        self.upload_file_owned(tenant_id, bucket, file_id, bytes.as_ref().to_vec(), options)
            .await
    }

    /// Upload an in-memory byte buffer you already own, split into gRPC
    /// messages of the server's largest allowed chunk size.
    ///
    /// Identical to [`RociaDbClient::upload_file`] in every respect —
    /// chunking, automatic checksum computation, the 5 GiB size limit —
    /// except that `bytes` is taken by value instead of by reference: a
    /// `Vec<u8>` passed here is moved straight into the chunking step with
    /// no intermediate copy, whereas `upload_file` must always copy its
    /// borrowed buffer to build the owned `Vec<u8>` the chunking step and
    /// the underlying `'static` upload stream require. For a caller who
    /// already holds the file as an owned `Vec<u8>` — read off disk,
    /// assembled in memory, or received from another API — that copy is
    /// pure waste: it doubles peak memory for the whole upload, and Rust's
    /// drop-scope rules keep the original buffer alive until the upload
    /// finishes, so the doubled memory persists for the entire duration,
    /// not just briefly. That waste becomes significant as file size
    /// approaches the 5 GiB ceiling documented above. Prefer
    /// [`RociaDbClient::upload_file`] only when you have a borrowed
    /// `&[u8]` (or another type implementing `AsRef<[u8]>`) and no owned
    /// buffer to give up — there, the copy is unavoidable regardless of
    /// which method you call.
    pub async fn upload_file_owned(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
        bytes: Vec<u8>,
        options: FileUploadOptions,
    ) -> Result<()> {
        let size_bytes = u64::try_from(bytes.len())
            .map_err(|_| RociaDbError::validation("file is too large"))?;
        validate_file_size(size_bytes)?;

        let checksum = resolve_checksum(options.checksum, &bytes)?;
        let request_id = options
            .request_id
            .unwrap_or_else(|| format!("upload_file:{}", Uuid::new_v4()));

        let requests = chunk_upload_requests(
            tenant_id.to_string(),
            bucket.to_string(),
            file_id.to_string(),
            bytes,
            options.content_type,
            checksum,
            request_id,
        );
        self.upload_file_stream(stream::iter(requests)).await
    }

    /// Upload a stream of arbitrarily-sized byte chunks without buffering
    /// the complete file in memory.
    ///
    /// This is the middle tier between [`RociaDbClient::upload_file`]
    /// (buffers the whole file, computes the checksum for you) and
    /// [`RociaDbClient::upload_file_stream`] (a raw pass-through with zero
    /// validation, and the caller must already match the server's exact
    /// wire contract). `chunks` may be split however the source naturally
    /// produces data — a `64 KiB` `AsyncRead` wrapper, protobuf messages
    /// from another stream, anything — this method re-buffers internally
    /// and always emits exactly-1-MiB gRPC messages to the server (the last
    /// one may be shorter), the same chunking [`RociaDbClient::upload_file`]
    /// produces from an in-memory buffer.
    ///
    /// `size_bytes` must be the exact total the caller intends to send, and
    /// `checksum` must already be the 32-byte SHA-256 digest of the
    /// complete file: unlike [`RociaDbClient::upload_file`], neither can be
    /// computed for you here, because file metadata travels on the first
    /// gRPC message, before this method has read a single byte from
    /// `chunks`. Hash the source ahead of time (a first pass over the file,
    /// for example) if you only have raw bytes. `checksum` is validated to
    /// be exactly 32 bytes before any network call.
    ///
    /// If `chunks` ends up producing more or fewer total bytes than
    /// `size_bytes` declared, this fails with
    /// [`RociaDbError::Validation`] instead of silently sending a
    /// corrupt-on-download file: the server itself also checks this at the
    /// end of the stream, but catching it here gives a clearer, immediate
    /// error naming the actual byte counts involved.
    ///
    /// **Naming note**: despite matching the server's chunking contract,
    /// this is not called `upload_file_stream` — that name was already
    /// taken by the raw, zero-validation escape hatch above it.
    #[allow(clippy::too_many_arguments)]
    pub async fn upload_file_chunked<S>(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
        size_bytes: u64,
        checksum: Vec<u8>,
        chunks: S,
        options: FileStreamUploadOptions,
    ) -> Result<()>
    where
        S: Stream<Item = Vec<u8>> + Send + 'static,
    {
        validate_file_size(size_bytes)?;
        require_checksum_len(&checksum)?;
        let request_id = options
            .request_id
            .unwrap_or_else(|| format!("upload_file:{}", Uuid::new_v4()));

        // Set by `rechunk_upload_requests` when the source produced a total
        // byte count that does not match `size_bytes`, since the outgoing
        // `Stream<Item = UploadRequest>` itself has no channel to carry an
        // error — it can only end early. Checked below regardless of
        // whether the RPC itself succeeded or failed, so this client-side
        // validation error takes precedence over whatever the server made
        // of a stream that ended up short or truncated.
        let error_slot: Arc<Mutex<Option<RociaDbError>>> = Arc::new(Mutex::new(None));
        let requests = rechunk_upload_requests(
            tenant_id.to_string(),
            bucket.to_string(),
            file_id.to_string(),
            size_bytes,
            options.content_type,
            checksum,
            request_id,
            chunks,
            Arc::clone(&error_slot),
        );

        let mut upstream_file = self.upstream_file.clone();
        let upload_result = upstream_file.upload(requests).await;
        if let Some(error) = error_slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            return Err(error);
        }
        upload_result.status_context("failed to upload file")?;
        Ok(())
    }

    /// Start a server-streaming download without buffering the complete file.
    ///
    /// This performs no integrity verification of its own — it is a thin
    /// wrapper that opens the raw gRPC download stream and hands it back
    /// as-is. That is a real asymmetry with the upload path: every upload
    /// method on this client validates a SHA-256 checksum before (or, for
    /// [`RociaDbClient::upload_file`]/[`RociaDbClient::upload_file_owned`],
    /// as part of) sending any bytes, and [`StatResponse::checksum`]
    /// exposes the checksum recorded for a stored file — but nothing on
    /// the download side ever computes or checks a checksum against the
    /// chunks the server sends back, and the server does not send one on
    /// download for this method to check. The only protection you get is
    /// whatever the transport itself already provides (TLS and HTTP/2
    /// framing catch corruption or truncation in transit), which says
    /// nothing about whether the bytes stored on the server still match
    /// what was originally uploaded. If you need that end-to-end
    /// guarantee, call [`RociaDbClient::stat_file`] yourself and compare
    /// its `checksum` against a SHA-256 digest you compute over the
    /// downloaded bytes — this crate does not do that comparison for you.
    pub async fn download_file_stream(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
    ) -> Result<Streaming<DownloadResponse>> {
        let mut upstream_file = self.upstream_file.clone();
        Ok(upstream_file
            .download(DownloadRequest {
                tenant_id: tenant_id.to_string(),
                bucket: bucket.to_string(),
                file_id: file_id.to_string(),
            })
            .await
            .status_context("failed to start file download")?
            .into_inner())
    }

    /// Download a complete file into memory.
    ///
    /// Collects every chunk from [`RociaDbClient::download_file_stream`]
    /// into one buffer via `extend_from_slice` and nothing else — see that
    /// method's docs for the full asymmetry with the upload path: no
    /// checksum is computed or checked here either, so a file that was
    /// corrupted or truncated in storage is still returned successfully,
    /// with its bad bytes intact and no error raised. Verify integrity
    /// yourself with [`RociaDbClient::stat_file`] if that matters for your
    /// use case.
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

    /// Return metadata for one stored file.
    pub async fn stat_file(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
    ) -> Result<StatResponse> {
        let mut upstream_file = self.upstream_file.clone();
        Ok(upstream_file
            .stat(StatRequest {
                tenant_id: tenant_id.to_string(),
                bucket: bucket.to_string(),
                file_id: file_id.to_string(),
            })
            .await
            .status_context("failed to stat file")?
            .into_inner())
    }

    /// Return one paginated page of bucket names holding at least one file.
    pub async fn list_buckets(
        &self,
        tenant_id: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<Page<String>> {
        let mut upstream_file = self.upstream_file.clone();
        let response = upstream_file
            .list_buckets(ListBucketsRequest {
                tenant_id: tenant_id.to_string(),
                page: page_request(limit, cursor)?,
            })
            .await
            .status_context("failed to list buckets")?
            .into_inner();
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
        let mut upstream_file = self.upstream_file.clone();
        let response = upstream_file
            .list_files(ListFilesRequest {
                tenant_id: tenant_id.to_string(),
                bucket: bucket.to_string(),
                page: page_request(limit, cursor)?,
            })
            .await
            .status_context("failed to list files")?
            .into_inner();
        Ok(Page {
            items: response.file_ids,
            next_cursor: response.page.and_then(|page| non_empty(page.next_cursor)),
        })
    }

    /// Delete one stored file using an automatically generated idempotency key.
    pub async fn delete_file(&self, tenant_id: &str, bucket: &str, file_id: &str) -> Result<()> {
        self.delete_file_with_request_id(
            tenant_id,
            bucket,
            file_id,
            format!("delete_file:{}", Uuid::new_v4()),
        )
        .await
    }

    /// Delete one stored file with a caller-provided idempotency key.
    pub async fn delete_file_with_request_id(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
        request_id: impl Into<String>,
    ) -> Result<()> {
        let mut upstream_file = self.upstream_file.clone();
        upstream_file
            .delete(DeleteRequest {
                tenant_id: tenant_id.to_string(),
                bucket: bucket.to_string(),
                file_id: file_id.to_string(),
                request_id: request_id.into(),
            })
            .await
            .status_context("failed to delete file")?;
        Ok(())
    }
}

/// Resolve the checksum to send: computes the SHA-256 digest of `bytes`
/// automatically when `checksum` is `None`; when it is `Some`, validates it
/// is exactly [`CHECKSUM_LEN`] bytes before returning it. Pure and
/// network-free, so [`RociaDbClient::upload_file`] can fail fast on a bad
/// checksum before any RPC — the server rejects any other length with
/// `INVALID_ARGUMENT`.
fn resolve_checksum(checksum: Option<Vec<u8>>, bytes: &[u8]) -> Result<Vec<u8>> {
    match checksum {
        Some(checksum) => {
            require_checksum_len(&checksum)?;
            Ok(checksum)
        }
        None => Ok(Sha256::digest(bytes).to_vec()),
    }
}

/// Validate that `checksum` is exactly [`CHECKSUM_LEN`] bytes, before any
/// network call. Shared by [`resolve_checksum`] (used by
/// [`RociaDbClient::upload_file`], where the checksum is optional and
/// computed automatically when absent) and
/// [`RociaDbClient::upload_file_chunked`] (where it is a required
/// parameter, since a streaming upload cannot compute it on the fly — see
/// that method's docs).
fn require_checksum_len(checksum: &[u8]) -> Result<()> {
    if checksum.len() != CHECKSUM_LEN {
        return Err(RociaDbError::validation(format!(
            "checksum must be exactly {CHECKSUM_LEN} bytes (sha256), got {} bytes",
            checksum.len()
        )));
    }
    Ok(())
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
/// of the stream (see module docs), so building them for every chunk would
/// just be wasted clones. Requests are produced on demand as the returned
/// iterator is polled by the outgoing stream, never collected into a `Vec`
/// up front.
fn chunk_upload_requests(
    tenant_id: String,
    bucket: String,
    file_id: String,
    bytes: Vec<u8>,
    content_type: String,
    checksum: Vec<u8>,
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
            checksum: checksum.take().unwrap_or_default(),
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
    checksum: Vec<u8>,
    request_id: String,
}

/// Mutable state driving [`rechunk_upload_requests`]'s `stream::unfold`,
/// boxed and type-erased over the caller's source stream so the state
/// itself stays a plain, non-generic type.
struct RechunkState {
    source: Pin<Box<dyn Stream<Item = Vec<u8>> + Send>>,
    /// Bytes accumulated toward the next outgoing chunk. Never allowed to
    /// grow past [`DEFAULT_CHUNK_SIZE`]: every place that adds to it copies
    /// in at most the space remaining before that cap (see
    /// [`RechunkState::ingest`] and [`RechunkState::drain_pending`]), so a
    /// source that yields one huge item — a whole file handed over as a
    /// single `Vec<u8>`, say — still only ever grows this buffer one
    /// bounded slice at a time, never in a single copy that jumps straight
    /// to the item's full size.
    buffer: Vec<u8>,
    /// The unread tail of a source item that didn't fully fit into
    /// `buffer` when [`RechunkState::ingest`] received it, together with
    /// `pending_offset` marking how much of it has been copied into
    /// `buffer` so far. Drained into `buffer` in further bounded slices by
    /// [`RechunkState::drain_pending`] as room frees up, instead of ever
    /// being copied in all at once.
    pending: Vec<u8>,
    /// How many bytes at the front of `pending` have already been copied
    /// into `buffer`. `pending` is reset to an empty, zero-capacity `Vec`
    /// once this reaches `pending.len()`, so a fully drained oversized item
    /// does not linger in memory waiting to be reused.
    pending_offset: usize,
    size_bytes: u64,
    total_written: u64,
    wrote_any: bool,
    source_exhausted: bool,
    metadata: Option<UploadMetadata>,
    error_slot: Arc<Mutex<Option<RociaDbError>>>,
}

impl RechunkState {
    /// Record the "would exceed / falls short of `size_bytes`" validation
    /// error into `error_slot`, so [`RociaDbClient::upload_file_chunked`]
    /// can surface it after the stream this state drives has ended.
    fn record_size_error(&self, message: String) {
        let mut guard = self
            .error_slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = Some(RociaDbError::validation(message));
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
                checksum: metadata.checksum,
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
    /// in-memory `Vec<u8>` and yields it as a single stream item) would
    /// grow `buffer` straight past one output chunk's worth, buffering
    /// memory proportional to the whole file despite this function's docs
    /// promising otherwise.
    fn ingest(&mut self, piece: Vec<u8>) {
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
            self.pending = Vec::new();
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
/// exhausted is detected right after the last real chunk. Because the
/// returned `Stream<Item = UploadRequest>` has no channel of its own to
/// carry an error — a tonic client-streaming call only accepts a stream
/// that produces requests, never `Result`s — any such failure is recorded
/// into `error_slot` instead, and the stream simply ends early (or, for a
/// short source, ends normally after reporting the mismatch). The caller
/// (see [`RociaDbClient::upload_file_chunked`]) checks `error_slot` once
/// the RPC settles.
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
    checksum: Vec<u8>,
    request_id: String,
    chunks: S,
    error_slot: Arc<Mutex<Option<RociaDbError>>>,
) -> impl Stream<Item = UploadRequest>
where
    S: Stream<Item = Vec<u8>> + Send + 'static,
{
    let state = RechunkState {
        source: Box::pin(chunks),
        buffer: Vec::new(),
        pending: Vec::new(),
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
                    Some(piece) => {
                        state.ingest(piece);
                        continue;
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
        CHECKSUM_LEN, DEFAULT_CHUNK_SIZE, FileStreamUploadOptions, FileUploadOptions,
        MAX_FILE_BYTES, RechunkState, chunk_upload_requests, rechunk_upload_requests,
        require_checksum_len, resolve_checksum, validate_file_size,
    };
    use crate::RociaDbError;
    use crate::pb::upstream::v1::UploadRequest;
    use futures::executor::block_on;
    use futures::{StreamExt, stream};
    use std::sync::{Arc, Mutex};

    #[test]
    fn upload_options_have_safe_defaults() {
        let options = FileUploadOptions::default();
        assert_eq!(options.content_type, "application/octet-stream");
        assert!(options.checksum.is_none());
        assert!(options.request_id.is_none());
    }

    #[test]
    fn upload_requests_chunk_at_exactly_one_mebibyte() {
        let bytes = vec![7u8; DEFAULT_CHUNK_SIZE + 10];
        let requests: Vec<_> = chunk_upload_requests(
            "tenant".into(),
            "bucket".into(),
            "file".into(),
            bytes.clone(),
            "text/plain".into(),
            vec![0u8; CHECKSUM_LEN],
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
            Vec::new(),
            FileUploadOptions::default().content_type,
            vec![0u8; CHECKSUM_LEN],
            "req".into(),
        )
        .collect();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].size_bytes, 0);
        assert!(requests[0].chunk.is_empty());
    }

    /// Asserts that chunking `total_bytes` matches the wire contract
    /// described in the module docs: every chunk but the last is exactly
    /// 1 MiB, the last is non-empty and no larger than 1 MiB, and the sum
    /// of chunk bytes equals `size_bytes`.
    fn assert_chunking_matches_server_contract(total_bytes: usize) {
        let bytes = vec![9u8; total_bytes];
        let requests: Vec<_> = chunk_upload_requests(
            "tenant".into(),
            "bucket".into(),
            "file".into(),
            bytes.clone(),
            "application/octet-stream".into(),
            vec![0u8; CHECKSUM_LEN],
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
        let checksum =
            resolve_checksum(None, b"hello world").expect("default checksum must succeed");
        assert_eq!(checksum.len(), CHECKSUM_LEN);
        assert_eq!(
            checksum,
            decode_hex("b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9")
        );
    }

    #[test]
    fn resolve_checksum_is_deterministic_and_content_dependent() {
        let first = resolve_checksum(None, b"payload-a").expect("checksum must succeed");
        let second = resolve_checksum(None, b"payload-a").expect("checksum must succeed");
        let different = resolve_checksum(None, b"payload-b").expect("checksum must succeed");
        assert_eq!(first, second, "same bytes must yield the same checksum");
        assert_ne!(
            first, different,
            "different bytes must yield a different checksum"
        );
    }

    #[test]
    fn resolve_checksum_accepts_caller_supplied_32_bytes() {
        let supplied = vec![7u8; CHECKSUM_LEN];
        let checksum = resolve_checksum(Some(supplied.clone()), b"irrelevant")
            .expect("a 32-byte checksum must be accepted as-is");
        assert_eq!(checksum, supplied);
    }

    #[test]
    fn resolve_checksum_rejects_wrong_length_before_any_network_call() {
        let error = resolve_checksum(Some(vec![1u8; 10]), b"irrelevant")
            .expect_err("a 10-byte checksum must be rejected");
        assert!(matches!(error, RociaDbError::Validation(_)));
        let message = error.to_string();
        assert!(message.contains("32 bytes"));
        assert!(
            message.contains("got 10 bytes"),
            "message should report the actual (wrong) length, got: {message}"
        );
    }

    fn decode_hex(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex pair"))
            .collect()
    }

    #[test]
    fn file_stream_upload_options_have_safe_defaults() {
        let options = FileStreamUploadOptions::default();
        assert_eq!(options.content_type, "application/octet-stream");
        assert!(options.request_id.is_none());
    }

    #[test]
    fn require_checksum_len_accepts_exactly_32_bytes() {
        require_checksum_len(&[0u8; CHECKSUM_LEN]).expect("32 bytes must be accepted");
    }

    #[test]
    fn require_checksum_len_rejects_wrong_length_before_any_network_call() {
        let error = require_checksum_len(&[1u8; 10]).expect_err("10 bytes must be rejected");
        assert!(matches!(error, RociaDbError::Validation(_)));
        let message = error.to_string();
        assert!(message.contains("32 bytes"));
        assert!(message.contains("got 10 bytes"));
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

    /// Drives [`rechunk_upload_requests`] to completion against an
    /// in-memory source and returns both the produced requests and
    /// whatever validation error, if any, ended up in `error_slot`. No
    /// network, no tokio runtime needed: `stream::iter` resolves
    /// synchronously, so `futures::executor::block_on` alone is enough to
    /// drive the `stream::unfold` chain to its end.
    fn collect_rechunked(
        size_bytes: u64,
        source_pieces: Vec<Vec<u8>>,
    ) -> (Vec<UploadRequest>, Option<RociaDbError>) {
        let error_slot: Arc<Mutex<Option<RociaDbError>>> = Arc::new(Mutex::new(None));
        let requests: Vec<UploadRequest> = block_on(
            rechunk_upload_requests(
                "tenant".into(),
                "bucket".into(),
                "file".into(),
                size_bytes,
                "application/octet-stream".into(),
                vec![0u8; CHECKSUM_LEN],
                "req".into(),
                stream::iter(source_pieces),
                Arc::clone(&error_slot),
            )
            .collect::<Vec<_>>(),
        );
        let error = error_slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
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
                vec![0u8; CHECKSUM_LEN],
                "caller-request-id".into(),
                stream::iter(vec![vec![1u8; 10]]),
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

    #[test]
    fn ingest_never_grows_the_buffer_past_one_chunk_for_a_single_oversized_item() {
        // Regression test for finding #3: before the fix, a caller who
        // already held the whole file as one in-memory `Vec<u8>` and
        // yielded it as a single stream item would have that whole item
        // copied into `buffer` by one `Vec::extend` call, defeating the
        // "never buffers more than one outgoing chunk" bound this module's
        // docs promise. `RechunkState::ingest`/`RechunkState::drain_pending`
        // are exercised directly here (rather than through the async
        // `rechunk_upload_requests` pipeline) so `buffer.len()` can be
        // asserted at every intermediate step, not just inferred from the
        // sizes of the `UploadRequest`s that eventually come out the other
        // end.
        let error_slot: Arc<Mutex<Option<RociaDbError>>> = Arc::new(Mutex::new(None));
        let mut state = RechunkState {
            source: Box::pin(stream::empty::<Vec<u8>>()),
            buffer: Vec::new(),
            pending: Vec::new(),
            pending_offset: 0,
            size_bytes: 0,
            total_written: 0,
            wrote_any: false,
            source_exhausted: false,
            metadata: None,
            error_slot,
        };

        let oversized = vec![42u8; DEFAULT_CHUNK_SIZE * 5 + 7];
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
