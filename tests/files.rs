//! File RPCs against the in-process server: the chunked upload contract, the
//! raw `upload_file_stream` escape hatch and the two server-side rules it can
//! break, a byte-exact download of a file larger than one chunk, the
//! verifying download and both of its failure modes, idempotent deletes, and
//! the two listings.

mod support;

use rociadb_sdk::{
    Bytes, FileStreamUploadOptions, FileUploadOptions, RociaDbError, UploadRequest, WriteOptions,
};
use support::{FakeServer, payload, sha256};

const TENANT: &str = "tenant-1";
const BUCKET: &str = "assets";
/// The chunk size the SDK uploads with, and the one thing the server caps.
const ONE_MIB: usize = 1024 * 1024;

/// One item of an `upload_file_chunked` source stream, for the tests that build
/// their chunks in memory rather than reading them off a file.
fn chunk(bytes: Vec<u8>) -> std::io::Result<Bytes> {
    Ok(Bytes::from(bytes))
}

#[tokio::test]
async fn a_multi_chunk_upload_stats_and_downloads_byte_for_byte() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    // Larger than one chunk and not a multiple of it, so the last message is
    // short — the shape most likely to be mis-sliced.
    let bytes = payload(ONE_MIB * 2 + 4321);

    client
        .upload_file(
            TENANT,
            BUCKET,
            "manual.bin",
            bytes.clone(),
            FileUploadOptions::new().with_content_type("application/pdf"),
        )
        .await
        .expect("the upload must succeed");

    // The wire: exactly three messages, 1 MiB each but the last, metadata on
    // the first one only.
    let upload = server.only_call("Upload");
    let chunk_sizes: Vec<usize> =
        serde_json::from_value(upload.field("chunk_sizes").clone()).expect("recorded chunk sizes");
    assert_eq!(chunk_sizes, vec![ONE_MIB, ONE_MIB, 4321]);
    assert_eq!(upload.str_field("content_type"), "application/pdf");

    let stat = client
        .stat_file(TENANT, BUCKET, "manual.bin")
        .await
        .expect("stat must succeed");
    assert_eq!(stat.size_bytes, bytes.len() as u64);
    assert_eq!(stat.content_type, "application/pdf");
    assert_eq!(
        stat.checksum,
        sha256(&bytes).to_vec(),
        "an upload with no explicit checksum must send the SHA-256 of the bytes"
    );

    let downloaded = client
        .download_file(TENANT, BUCKET, "manual.bin")
        .await
        .expect("the download must succeed");
    assert_eq!(
        downloaded, bytes,
        "the download must reassemble byte for byte, whatever chunk size the server chose"
    );
}

#[tokio::test]
async fn an_empty_file_still_uploads_stats_and_downloads() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    client
        .upload_file(
            TENANT,
            BUCKET,
            "empty.bin",
            Vec::new(),
            FileUploadOptions::new(),
        )
        .await
        .expect("a zero-byte upload must succeed");

    let stat = client
        .stat_file(TENANT, BUCKET, "empty.bin")
        .await
        .expect("stat must succeed");
    assert_eq!(stat.size_bytes, 0);
    assert_eq!(stat.checksum, sha256(&[]).to_vec());
    assert!(
        client
            .download_file(TENANT, BUCKET, "empty.bin")
            .await
            .expect("the download must succeed")
            .is_empty()
    );
}

#[tokio::test]
async fn an_explicit_checksum_is_sent_verbatim_and_content_type_defaults() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    let declared = [7u8; 32];
    client
        .upload_file(
            TENANT,
            BUCKET,
            "declared.bin",
            b"some bytes".as_slice(),
            FileUploadOptions::new().with_checksum(declared),
        )
        .await
        .expect("the upload must succeed");

    let stat = client
        .stat_file(TENANT, BUCKET, "declared.bin")
        .await
        .expect("stat must succeed");
    assert_eq!(
        stat.checksum,
        declared.to_vec(),
        "a caller-supplied digest must never be recomputed"
    );
    assert_eq!(stat.content_type, "application/octet-stream");
}

#[tokio::test]
async fn upload_file_chunked_rechunks_odd_sized_source_pieces() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    // Deliberately uneven pieces that line up with nothing: 300 KiB + 1,
    // 7 bytes, 900 KiB, and a remainder.
    let sizes = [300 * 1024 + 1, 7, 900 * 1024, 123_456];
    let total: usize = sizes.iter().sum();
    let bytes = payload(total);
    let mut offset = 0;
    let mut pieces = Vec::new();
    for size in sizes {
        pieces.push(chunk(bytes[offset..offset + size].to_vec()));
        offset += size;
    }

    client
        .upload_file_chunked(
            TENANT,
            BUCKET,
            "streamed.bin",
            futures::stream::iter(pieces),
            FileStreamUploadOptions::new(total as u64, sha256(&bytes))
                .with_content_type("text/csv")
                .with_request_id("upload-key-1"),
        )
        .await
        .expect("the chunked upload must succeed");

    let upload = server.only_call("Upload");
    assert_eq!(upload.request_id.as_deref(), Some("upload-key-1"));
    let chunk_sizes: Vec<usize> =
        serde_json::from_value(upload.field("chunk_sizes").clone()).expect("recorded chunk sizes");
    let (last, leading) = chunk_sizes.split_last().expect("at least one message");
    assert!(
        leading.iter().all(|size| *size == ONE_MIB),
        "every message but the last must be exactly 1 MiB, got {chunk_sizes:?}"
    );
    assert!(*last <= ONE_MIB && *last > 0);
    assert_eq!(chunk_sizes.iter().sum::<usize>(), total);

    let downloaded = client
        .download_file(TENANT, BUCKET, "streamed.bin")
        .await
        .expect("the download must succeed");
    assert_eq!(downloaded, bytes);
}

#[tokio::test]
async fn upload_file_stream_passes_a_caller_built_stream_through_unchanged() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    let bytes = payload(3000);

    // The raw escape hatch: the caller owns the whole wire contract, so this
    // builds the two messages by hand — metadata on the first, a bare chunk on
    // the second — and the SDK forwards them without re-chunking or hashing.
    client
        .upload_file_stream(futures::stream::iter(vec![
            UploadRequest {
                tenant_id: TENANT.to_string(),
                bucket: BUCKET.to_string(),
                file_id: "hand-built.bin".to_string(),
                size_bytes: bytes.len() as u64,
                content_type: "application/x-custom".to_string(),
                checksum: sha256(&bytes).to_vec(),
                chunk: bytes[..1000].to_vec(),
                request_id: "hand-built-key".to_string(),
            },
            UploadRequest {
                chunk: bytes[1000..].to_vec(),
                ..UploadRequest::default()
            },
        ]))
        .await
        .expect("a correctly built stream must be accepted");

    let upload = server.only_call("Upload");
    assert_eq!(upload.request_id.as_deref(), Some("hand-built-key"));
    let chunk_sizes: Vec<usize> =
        serde_json::from_value(upload.field("chunk_sizes").clone()).expect("recorded chunk sizes");
    assert_eq!(
        chunk_sizes,
        vec![1000, 2000],
        "nothing may re-chunk what the caller sliced itself"
    );
    assert_eq!(
        client
            .download_file(TENANT, BUCKET, "hand-built.bin")
            .await
            .expect("the download must succeed"),
        bytes
    );
}

#[tokio::test]
async fn a_chunk_over_the_one_mebibyte_cap_is_rejected_by_the_server() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    // Only the raw escape hatch can produce this: `upload_file` and
    // `upload_file_chunked` both slice at exactly 1 MiB.
    let oversized = payload(ONE_MIB + 1);

    let error = client
        .upload_file_stream(futures::stream::iter(vec![UploadRequest {
            tenant_id: TENANT.to_string(),
            bucket: BUCKET.to_string(),
            file_id: "oversized.bin".to_string(),
            size_bytes: oversized.len() as u64,
            content_type: String::new(),
            checksum: sha256(&oversized).to_vec(),
            chunk: oversized,
            request_id: String::new(),
        }]))
        .await
        .expect_err("a chunk over the cap must be refused");
    assert!(error.is_invalid_argument(), "got: {error}");
    assert_eq!(error.reason(), Some("invalid_argument"));
}

#[tokio::test]
async fn a_checksum_of_the_wrong_length_is_rejected_by_the_server() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    // `FileUploadOptions::checksum` is `[u8; 32]`, so the SDK cannot send a
    // wrong length at all — the escape hatch can, and the server is the one
    // that says no. (It checks the length and nothing else, which is why
    // `download_file_verified` exists.)
    let error = client
        .upload_file_stream(futures::stream::iter(vec![UploadRequest {
            tenant_id: TENANT.to_string(),
            bucket: BUCKET.to_string(),
            file_id: "short-checksum.bin".to_string(),
            size_bytes: 3,
            content_type: String::new(),
            checksum: vec![1, 2, 3],
            chunk: b"abc".to_vec(),
            request_id: String::new(),
        }]))
        .await
        .expect_err("a checksum that is not 32 bytes must be refused");
    assert!(error.is_invalid_argument(), "got: {error}");
}

#[tokio::test]
async fn a_chunk_stream_shorter_than_its_declared_size_is_a_validation_error() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    let bytes = payload(2048);

    let error = client
        .upload_file_chunked(
            TENANT,
            BUCKET,
            "short.bin",
            futures::stream::iter(vec![chunk(bytes.clone())]),
            // Declares one byte more than the stream will ever produce.
            FileStreamUploadOptions::new(bytes.len() as u64 + 1, sha256(&bytes)),
        )
        .await
        .expect_err("a short stream must be rejected");
    assert!(
        matches!(error, RociaDbError::Validation(_)),
        "the client's own size check must win over whatever the server made of it, got: {error}"
    );
    assert!(error.to_string().contains("size_bytes"));

    // And the same for a stream that produces more than it declared.
    let error = client
        .upload_file_chunked(
            TENANT,
            BUCKET,
            "long.bin",
            futures::stream::iter(vec![chunk(bytes.clone())]),
            FileStreamUploadOptions::new(16, sha256(&bytes)),
        )
        .await
        .expect_err("an overlong stream must be rejected");
    assert!(matches!(error, RociaDbError::Validation(_)), "got: {error}");
}

#[tokio::test]
async fn upload_file_chunked_takes_a_reader_stream_over_a_real_file_unadapted() {
    // The reason the item type is `std::io::Result<Bytes>`: a
    // `tokio_util::io::ReaderStream` over a `tokio::fs::File` is passed in with
    // no adapter, no `map`, no collecting. Nothing here converts anything.
    let server = FakeServer::start().await;
    let client = server.client().await;
    // Over one outgoing chunk and not a multiple of it, so the re-chunker has
    // to reassemble the reader's own 4 KiB-ish slices into 1 MiB messages and
    // end with a short one.
    let bytes = payload(ONE_MIB + 7919);
    let path = std::env::temp_dir().join(format!(
        "rociadb-sdk-reader-stream-{}.bin",
        std::process::id()
    ));
    std::fs::write(&path, &bytes).expect("writing the temporary source file must succeed");

    let file = tokio::fs::File::open(&path)
        .await
        .expect("opening the temporary source file must succeed");
    let result = client
        .upload_file_chunked(
            TENANT,
            BUCKET,
            "from-a-file.bin",
            tokio_util::io::ReaderStream::new(file),
            FileStreamUploadOptions::new(bytes.len() as u64, sha256(&bytes)),
        )
        .await;
    // Remove the temporary file before asserting, so a failure does not leave
    // it behind.
    let _ = std::fs::remove_file(&path);
    result.expect("a ReaderStream over a real file must upload as-is");

    let chunk_sizes: Vec<usize> =
        serde_json::from_value(server.only_call("Upload").field("chunk_sizes").clone())
            .expect("recorded chunk sizes");
    assert_eq!(
        chunk_sizes,
        vec![ONE_MIB, 7919],
        "the reader's own slicing must be re-chunked to the wire contract, got {chunk_sizes:?}"
    );
    assert_eq!(
        client
            .download_file(TENANT, BUCKET, "from-a-file.bin")
            .await
            .expect("the download must succeed"),
        bytes,
        "the file must come back byte for byte"
    );
}

#[tokio::test]
async fn a_failing_chunk_source_surfaces_as_an_io_error_not_a_size_mismatch() {
    // The other half of the fallible item type: a read that dies partway
    // through must reach the caller as its own `io::Error`, and must win over
    // whatever the server made of the stream that then ended early — here an
    // `INVALID_ARGUMENT` for a 4096-byte upload that delivered 64 bytes.
    let server = FakeServer::start().await;
    let client = server.client().await;

    let error = client
        .upload_file_chunked(
            TENANT,
            BUCKET,
            "unreadable.bin",
            futures::stream::iter(vec![
                chunk(payload(64)),
                Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "the source disk gave up",
                )),
                chunk(payload(4032)),
            ]),
            FileStreamUploadOptions::new(4096, sha256(&payload(4096))),
        )
        .await
        .expect_err("a failing source must fail the upload");

    match &error {
        RociaDbError::Io { context, source } => {
            assert_eq!(*context, "the upload chunk stream");
            assert_eq!(source.kind(), std::io::ErrorKind::UnexpectedEof);
        }
        other => panic!("expected an Io error, got: {other}"),
    }
    assert!(
        error.to_string().contains("the source disk gave up"),
        "the source's own message must be readable, got: {error}"
    );
    assert!(
        error.code().is_none(),
        "the server's status for the truncated stream must not be what surfaces, got: {error}"
    );
    assert!(
        server
            .stored_file(TENANT, BUCKET, "unreadable.bin")
            .is_none(),
        "an upload abandoned mid-stream must store nothing"
    );
}

#[tokio::test]
async fn download_file_verified_returns_the_bytes_when_everything_agrees() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    let bytes = payload(ONE_MIB + 17);
    client
        .upload_file(
            TENANT,
            BUCKET,
            "verified.bin",
            bytes.clone(),
            FileUploadOptions::new(),
        )
        .await
        .expect("the upload must succeed");

    let downloaded = client
        .download_file_verified(TENANT, BUCKET, "verified.bin")
        .await
        .expect("a file whose stored checksum matches its bytes must verify");
    assert_eq!(downloaded, bytes);
    // One stat plus one download: the verification costs exactly the extra
    // unary call, nothing more.
    assert_eq!(server.call_count("Stat"), 1);
    assert_eq!(server.call_count("Download"), 1);
}

#[tokio::test]
async fn download_file_verified_rejects_bytes_that_do_not_match_the_stored_checksum() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    let bytes = payload(4096);
    client
        .upload_file(
            TENANT,
            BUCKET,
            "tampered.bin",
            bytes.clone(),
            FileUploadOptions::new(),
        )
        .await
        .expect("the upload must succeed");
    // Same length, different bytes: only the digest can catch this.
    server.corrupt_stored_bytes(TENANT, BUCKET, "tampered.bin");

    let error = client
        .download_file_verified(TENANT, BUCKET, "tampered.bin")
        .await
        .expect_err("corrupted bytes must not be returned as if they were fine");
    match error {
        RociaDbError::ChecksumMismatch { expected, actual } => {
            assert_eq!(expected, sha256(&bytes).to_vec());
            assert_ne!(actual, expected);
            assert_eq!(actual.len(), 32);
        }
        other => panic!("expected a checksum mismatch, got: {other}"),
    }

    // `download_file` still hands the bad bytes back without complaint —
    // which is exactly the asymmetry the verifying variant exists to close.
    let unverified = client
        .download_file(TENANT, BUCKET, "tampered.bin")
        .await
        .expect("the unverified download must still succeed");
    assert_ne!(unverified, bytes);
}

#[tokio::test]
async fn download_file_verified_rejects_a_stored_checksum_that_never_matched() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    let bytes = payload(1024);
    client
        .upload_file(
            TENANT,
            BUCKET,
            "mislabelled.bin",
            bytes.clone(),
            FileUploadOptions::new(),
        )
        .await
        .expect("the upload must succeed");
    // The other half of the same problem: the bytes are intact and the
    // *checksum* is wrong. The server never verified it, so only the client
    // can notice.
    server.corrupt_stored_checksum(TENANT, BUCKET, "mislabelled.bin");

    let error = client
        .download_file_verified(TENANT, BUCKET, "mislabelled.bin")
        .await
        .expect_err("a checksum the bytes do not satisfy must fail verification");
    assert!(
        matches!(error, RociaDbError::ChecksumMismatch { .. }),
        "got: {error}"
    );
    let message = error.to_string();
    assert!(
        message.contains(&hex(&sha256(&bytes))),
        "the digest actually computed must be readable in the message, got: {message}"
    );
}

#[tokio::test]
async fn download_file_verified_rejects_a_download_shorter_than_its_metadata() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    let bytes = payload(4096);
    client
        .upload_file(
            TENANT,
            BUCKET,
            "truncated.bin",
            bytes.clone(),
            FileUploadOptions::new(),
        )
        .await
        .expect("the upload must succeed");
    server.drop_stored_byte(TENANT, BUCKET, "truncated.bin");

    let error = client
        .download_file_verified(TENANT, BUCKET, "truncated.bin")
        .await
        .expect_err("a truncated file must not be returned");
    match error {
        RociaDbError::SizeMismatch { expected, actual } => {
            assert_eq!(expected, bytes.len() as u64);
            assert_eq!(actual, bytes.len() as u64 - 1);
        }
        other => panic!("expected a size mismatch — it is checked first — got: {other}"),
    }
}

#[tokio::test]
async fn download_file_verified_propagates_a_missing_file_as_not_found() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    let error = client
        .download_file_verified(TENANT, BUCKET, "absent.bin")
        .await
        .expect_err("an unknown file must fail");
    assert!(error.is_not_found(), "got: {error}");
    // It fails on the stat, before any download is attempted.
    assert_eq!(server.call_count("Stat"), 1);
    assert_eq!(server.call_count("Download"), 0);
}

#[tokio::test]
async fn delete_file_is_idempotent() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    client
        .upload_file(
            TENANT,
            BUCKET,
            "doomed.bin",
            b"bytes".as_slice(),
            FileUploadOptions::new(),
        )
        .await
        .expect("the upload must succeed");

    for attempt in 0..3 {
        client
            .delete_file(TENANT, BUCKET, "doomed.bin", WriteOptions::new())
            .await
            .unwrap_or_else(|error| panic!("delete {attempt} must succeed, got: {error}"));
    }
    client
        .delete_file(TENANT, BUCKET, "never.bin", WriteOptions::new())
        .await
        .expect("deleting a file that never existed must succeed");

    assert!(
        client
            .stat_file(TENANT, BUCKET, "doomed.bin")
            .await
            .expect_err("the deleted file must be gone")
            .is_not_found()
    );
}

#[tokio::test]
async fn list_buckets_and_list_files_page_over_what_was_uploaded() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    for (bucket, file_id) in [
        (BUCKET, "a.bin"),
        (BUCKET, "b.bin"),
        (BUCKET, "c.bin"),
        ("archive", "old.bin"),
    ] {
        client
            .upload_file(
                TENANT,
                bucket,
                file_id,
                b"x".as_slice(),
                FileUploadOptions::new(),
            )
            .await
            .expect("the upload must succeed");
    }

    let buckets = client
        .list_buckets(TENANT, None, None)
        .await
        .expect("listing buckets must succeed");
    assert_eq!(
        buckets.items,
        vec!["archive".to_string(), "assets".to_string()]
    );
    assert!(buckets.next_cursor.is_none());

    let first = client
        .list_files(TENANT, BUCKET, Some(2), None)
        .await
        .expect("listing files must succeed");
    assert_eq!(first.items, vec!["a.bin".to_string(), "b.bin".to_string()]);
    let second = client
        .list_files(TENANT, BUCKET, Some(2), first.next_cursor.as_deref())
        .await
        .expect("the second page must succeed");
    assert_eq!(second.items, vec!["c.bin".to_string()]);
    assert!(second.next_cursor.is_none());

    // An unknown bucket is an empty page, not an error.
    let unknown = client
        .list_files(TENANT, "no-such-bucket", None, None)
        .await
        .expect("an unknown bucket must still succeed");
    assert!(unknown.items.is_empty());
}

/// Lowercase hex, for comparing against the message
/// [`RociaDbError::ChecksumMismatch`] renders.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
