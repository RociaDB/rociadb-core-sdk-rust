# Files

Files live under a `(tenant_id, bucket)` pair and are addressed by
`file_id`. Uploads and downloads are the only streaming RPCs in the API;
everything else about files — `stat_file`, `list_buckets`, `list_files`,
`delete_file` — is an ordinary unary call.

## The upload wire contract

Worth understanding even if you never touch `upload_file_chunked` or
`upload_file_stream` directly, because it is what the ergonomic helpers
implement for you.

- **Chunk size is the client's choice, capped at 1 MiB — not a fixed
  requirement.** The server stores each chunk verbatim at its position in
  the stream and, on download, reads chunks back until it has collected
  `size_bytes` in total, assuming no particular chunk size. A single
  message's `chunk` larger than 1 MiB (1 048 576 bytes) is rejected with
  `INVALID_ARGUMENT` (`"chunk exceeds 1 MiB"`); anything at or under the cap
  is fine, sliced however the client likes.
- **The SDK always emits exactly-1-MiB chunks** (the last one possibly
  shorter) — not because the server requires it, but because 1 MiB is the
  largest message allowed and therefore the fewest messages for a given
  file. It also remains the only chunk size that is safe against a server
  older than `1.0.0-rc.16`. This is why neither `FileUploadOptions` nor
  `FileStreamUploadOptions` has a `chunk_size` knob.
- **The first message carries the metadata**: `tenant_id`, `bucket`,
  `file_id`, `size_bytes` (the exact total byte count), `content_type`,
  `checksum` and `request_id`. Every later message is read only for its
  `chunk` field.
- **`checksum` must be exactly 32 raw bytes**, a SHA-256 digest. The server
  rejects any other length, empty included, with `INVALID_ARGUMENT`
  (`"checksum must be 32 bytes (sha256)"`). The SDK types it as `[u8; 32]`,
  so the length is the compiler's business rather than a runtime check.
  **The server never verifies that the digest matches the bytes** — only
  that it is 32 bytes long.
- **The sum of every `chunk` across the stream must equal `size_bytes`
  exactly**, or the server rejects the upload at the end of the stream with
  `INVALID_ARGUMENT` (`"size_bytes does not match uploaded data"`). That is
  what makes `size_bytes` a value the server can trust on download rather
  than a caller-supplied claim.
- **Re-uploading an existing `file_id` replaces it, with no error for the
  duplicate** — no delete-then-upload dance. The replacement is atomic: the
  upload writes into its own generation and never touches the published
  version's chunks, so a download that started before the commit serves the
  old version in full, one that starts after serves the new version in full,
  and an in-flight download keeps serving the version it began with. There
  is never a mixed file.
- **Files over the server's `limits.max_file_bytes` (5 GiB by default) are
  rejected.** `upload_file` and `upload_file_chunked` check this client-side
  and return `RociaDbError::Validation` before sending anything. The ceiling
  they check against is `RociaDbBuilder::max_file_bytes` — see
  [the client-side ceiling](#the-client-side-size-ceiling).
- **An empty file is valid and common**: exactly one message (metadata only,
  empty `chunk`) and no data messages. `upload_file` handles it.
- **A file becomes visible only once the whole stream has been received and
  validated.** Until then it is absent from `list_files`, `stat_file` and
  downloads. An interrupted stream leaves orphaned chunks that a background
  GC reclaims; the partial file never appears anywhere.

`RociaDbBuilder::request_timeout` does **not** cover any of this: how long a
transfer takes is a property of the file's size and the link, not of a
single round trip. Wrap an upload or download in a `tokio::time::timeout` of
your own when it needs a deadline.

## Tokens on a streaming call

A gRPC server validates the bearer token **once, when it accepts the call**, so
a transfer that runs for an hour on a ten-minute token is not a problem. What is
a problem is a stream that *starts* on a token about to be rejected, and the SDK
handles that in two ways:

- **Every** streaming call — all three uploads, and every download — refreshes
  the token before opening if less than five seconds of its advertised lifetime
  is left. A refresh that fails here is logged as a `warn!` and the call goes
  ahead with the cached token, which may well still be valid; if it is not, the
  server says `UNAUTHENTICATED` and that reaches you.
- `upload_file` and the call that *opens* a download (so `download_file_stream`,
  `download_file`, `download_file_verified` and `download_file_verified_to`)
  additionally get the same
  refresh-and-retry a unary RPC gets: an `UNAUTHENTICATED` answer triggers one
  coalesced refresh and one re-issue. `upload_file` can do this because it owns
  its buffer and re-sends under the same `request_id`, so a replay that lands on
  an upload the server had already committed is deduplicated rather than written
  twice. A download can do it because a rejected server-streaming call fails
  before any message exists.

`upload_file_chunked` and `upload_file_stream` get the pre-flight refresh and
nothing more: their source is a stream **you** handed over, and h2 starts writing
body frames as soon as the call opens, so "nothing has been consumed yet" is not
a state the SDK can establish — a replay would have to re-drain a stream that is
already partly drained. Recover by hand where it matters: `refresh_auth_token()`,
then re-issue with a freshly built stream and the same `request_id`. See
[authentication](authentication.md).

## The three upload tiers

| Method | Input | Does for you | Use when |
| ------ | ----- | ------------ | -------- |
| `upload_file` | `impl Into<Vec<u8>>` | chunking, the SHA-256 digest, **and** one automatic retry after `UNAUTHENTICATED` | the file fits in memory |
| `upload_file_chunked` | `Stream<Item = std::io::Result<Bytes>>` | re-chunking, validates the total against `size_bytes`, and surfaces a failed read as `RociaDbError::Io` | the file does not fit in memory but can be hashed ahead of time |
| `upload_file_stream` | `Stream<Item = UploadRequest>` | nothing at all | you need to build every protobuf message yourself |

### `upload_file` — an in-memory buffer

```rust,no_run
use rociadb_sdk::{FileUploadOptions, RociaDbBuilder, WriteOptions};

# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
client
    .upload_file(
        "tenant-1",
        "assets",
        "manual.txt",
        b"hello RociaDB".to_vec(),
        FileUploadOptions::new()
            .with_content_type("text/plain")
            .with_request_id("import-42:manual.txt"),
    )
    .await?;

let metadata = client.stat_file("tenant-1", "assets", "manual.txt").await?;
println!("{} bytes, {}", metadata.size_bytes, metadata.content_type);

client
    .delete_file("tenant-1", "assets", "manual.txt", WriteOptions::new())
    .await?;
# Ok(())
# }
```

With `FileUploadOptions::checksum` left `None` — the default — the SHA-256
digest of the buffer is computed and sent for you, which is almost always
what you want. Set it explicitly only when the digest is already known from
elsewhere (a manifest, an earlier pass over the same bytes).

`bytes` is `impl Into<Vec<u8>>`, so both ownership styles are one call: a
`Vec<u8>` you already hold is **moved** straight into the chunking step with
no copy, while a borrowed `&[u8]` (or `&[u8; N]`, or `&str`) is **copied
once** into the owned buffer the `'static` upload stream requires. That copy
is unavoidable for a borrowed buffer and worth avoiding for a large owned
one: it doubles peak memory for the whole upload, since the original stays
alive until the upload finishes.

`content_type` defaults to `"application/octet-stream"`. The server records
it as given and never inspects the bytes to confirm it.

### `upload_file_chunked` — a stream you cannot buffer

The item type is `std::io::Result<Bytes>`, which is exactly what
`tokio_util::io::ReaderStream` yields for any `tokio::io::AsyncRead` — a file, a
socket, a decompressor. So the common case needs no adaptation at all: hash the
file in one pass, then hand the reader straight over.

```rust,no_run
use futures::StreamExt;
use rociadb_sdk::{FileStreamUploadOptions, RociaDbBuilder};
use sha2::{Digest, Sha256};
use tokio_util::io::ReaderStream;

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
// First pass: measure and hash, without buffering the file either. Neither
// value can be computed while sending, since both travel on the first gRPC
// message.
let mut hasher = Sha256::new();
let mut size_bytes = 0u64;
let mut hashing = ReaderStream::new(tokio::fs::File::open("large-report.csv").await?);
while let Some(chunk) = hashing.next().await {
    let chunk = chunk?;
    size_bytes += chunk.len() as u64;
    hasher.update(&chunk);
}
let checksum: [u8; 32] = hasher.finalize().into();

// Second pass: stream it. `ReaderStream` reads in its own ~4 KiB slices and
// `upload_file_chunked` re-slices them to the server's 1 MiB messages.
let file = tokio::fs::File::open("large-report.csv").await?;

client
    .upload_file_chunked(
        "tenant-1",
        "reports",
        "large-report.csv",
        ReaderStream::new(file),
        FileStreamUploadOptions::new(size_bytes, checksum).with_content_type("text/csv"),
    )
    .await?;
# Ok(())
# }
```

Any other stream of the same item type works too, so a source that is not an
`AsyncRead` — chunks arriving from elsewhere, a generator — wraps each piece in
`Ok(Bytes::from(..))`:

```rust,no_run
use futures::stream;
use rociadb_sdk::{Bytes, FileStreamUploadOptions, RociaDbBuilder};

# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
# let (size_bytes, checksum) = (6u64, [0u8; 32]);
let chunks = stream::iter(vec![
    Ok(Bytes::from_static(b"abc")),
    Ok(Bytes::from_static(b"def")),
]);
client
    .upload_file_chunked(
        "tenant-1",
        "reports",
        "assembled.csv",
        chunks,
        FileStreamUploadOptions::new(size_bytes, checksum),
    )
    .await?;
# Ok(())
# }
```

`size_bytes` and `checksum` are constructor arguments rather than optional
fields — which is why `FileStreamUploadOptions` has no `Default` — because
the metadata travels on the very first gRPC message, before a single byte
has been read from the caller's stream. Neither can be derived on the fly
the way `upload_file` derives them from a complete buffer: hash the source
ahead of time.

The `Result` in the item type is the load-bearing half. A read that fails partway
through yields an `Err`, and that **fails the upload** with `RociaDbError::Io`
carrying the `std::io::Error` — nothing further is pulled from the stream. With a
plain-bytes item type a failing source could only end early, and the upload would
be reported as a size mismatch, blaming your `size_bytes` for a disk that could
not be read. The `Bytes` items are copied into the outgoing chunk buffer like any
other bytes: the type is there for what it makes easy at the call site, not to
make the upload zero-copy.

If the stream ends up producing more or fewer total bytes than `size_bytes`
declared, the call fails with `RociaDbError::Validation` naming the actual
byte counts, rather than sending a stream the server would reject anyway at
the end. Both client-side failures — the `Io` and the `Validation` — are
reported ahead of whatever status the server returned for the stream that then
ended early, because they say what actually went wrong. It never holds more than
one outgoing chunk's worth of bytes at a time, however the input happens to be
sliced.

**Naming trap when porting code between SDKs:** despite doing the
re-chunking and the validation, this method is not called
`upload_file_stream` — that name belongs to the raw escape hatch below. See
[parity with the TypeScript SDK](typescript-parity.md).

### `upload_file_stream` — the raw escape hatch

Zero validation: no re-chunking, no chunk-size cap, no checksum, and no
generated `request_id` — the first message's is forwarded as-is. The caller
must match the wire contract above exactly.

```rust,no_run
use futures::stream;
use rociadb_sdk::{RociaDbBuilder, UploadRequest};

# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
# let checksum: [u8; 32] = [0; 32];
// One metadata-first message carrying the whole (tiny) payload.
let requests = vec![UploadRequest {
    tenant_id: "tenant-1".to_string(),
    bucket: "assets".to_string(),
    file_id: "manual.txt".to_string(),
    size_bytes: 13,
    content_type: "text/plain".to_string(),
    checksum: checksum.to_vec(),
    chunk: b"hello RociaDB".to_vec(),
    request_id: "import-42:manual.txt".to_string(),
}];
client.upload_file_stream(stream::iter(requests)).await?;
# Ok(())
# }
```

Getting the chunk *size* wrong here fails fast with `INVALID_ARGUMENT`
rather than corrupting a later download — but a wrong `size_bytes` total, or
a `checksum` that does not match the bytes, can still produce an upload that
looks successful while carrying bad data. Prefer `upload_file_chunked`
unless you specifically need to hand-build the message stream.

## The client-side size ceiling

`RociaDbBuilder::max_file_bytes(bytes)` sets the largest file `upload_file` and
`upload_file_chunked` will send. It defaults to **5 GiB**, the default of the
server setting it mirrors (`limits.max_file_bytes`), and a zero value is
rejected at `build()` time with `RociaDbError::Config` like a zero timeout.

```rust,no_run
use rociadb_sdk::RociaDbBuilder;

# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
// This deployment never stores anything over 256 MiB, so an oversized upload
// should fail here rather than after minutes on the wire.
let client = RociaDbBuilder::new()
    .disable_auth()
    .max_file_bytes(256 * 1024 * 1024)
    .build()
    .await?;
# let _ = client;
# Ok(())
# }
```

**It only mirrors the server's limit — the server always has the final say**,
and the client cannot read its configuration:

- set it **below** the server's limit and an oversized upload fails
  immediately, locally and for free, with `RociaDbError::Validation` naming
  both byte counts;
- set it **above** and nothing is unlocked: the failure simply moves
  server-side, arriving as an `INVALID_ARGUMENT` `RociaDbError::Status` once
  enough of the stream has been sent for the server to say so;
- leave it alone and you get the server's *default*, which is right until
  somebody changes it on the server.

`upload_file_stream` is unaffected: it validates nothing at all by design.

## Downloads

```rust,no_run
use rociadb_sdk::RociaDbBuilder;

# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
// Buffered, unverified.
let bytes = client.download_file("tenant-1", "assets", "manual.txt").await?;

// Buffered, and checked against what `stat_file` reports.
let verified = client
    .download_file_verified("tenant-1", "assets", "manual.txt")
    .await?;

println!("{} / {} bytes", bytes.len(), verified.len());
# Ok(())
# }
```

Verified but **not** buffered: `download_file_verified_to` applies the same
checks while writing into any `tokio::io::AsyncWrite` you hand it — a file, a
socket, a `BufWriter`, a `Vec<u8>` — and returns the number of bytes written,
so a 5 GiB file costs one chunk of memory instead of 5 GiB.

```rust,no_run
use rociadb_sdk::RociaDbBuilder;

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
// Download to a temporary name, and publish it only once it has verified.
let mut file = tokio::fs::File::create("report.csv.part").await?;
match client
    .download_file_verified_to("tenant-1", "reports", "report.csv", &mut file)
    .await
{
    Ok(bytes) => {
        tokio::fs::rename("report.csv.part", "report.csv").await?;
        println!("{bytes} verified bytes");
    }
    Err(error) => {
        // Verification can only fail at the end, so the partial file exists
        // and is yours to remove.
        tokio::fs::remove_file("report.csv.part").await?;
        return Err(error.into());
    }
}
# Ok(())
# }
```

Raw and unverified, when what you need is the chunks themselves rather than a
destination to put them in. The stream hands back a raw `tonic::Status` on
failure rather than a `RociaDbError`, so an example that mixes the two uses a
boxed error:

```rust,no_run
use rociadb_sdk::RociaDbBuilder;

# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
let mut stream = client
    .download_file_stream("tenant-1", "assets", "manual.txt")
    .await?;
let mut total = 0usize;
while let Some(response) = stream.message().await? {
    total += response.chunk.len();
}
println!("{total} bytes");
# Ok(())
# }
```

`download_file_stream` returns a `Streaming<DownloadResponse>` and performs
no integrity verification of its own; `download_file` collects that stream
into one buffer and performs none either. That is a real asymmetry with the
upload path: every upload method sends a SHA-256 checksum with the file, and
`StatResponse::checksum` exposes the one recorded for a stored file, but
nothing on the download side checks anything. The only protection is
whatever the transport already provides — TLS and HTTP/2 framing catch
corruption in transit, which says nothing about whether the bytes stored on
the server still match what was uploaded.

### The verified downloads, and what they actually prove

Both call `stat_file` first, stream the download while feeding every chunk to a
SHA-256 hasher, and check the byte count against `StatResponse::size_bytes` and
the digest against `StatResponse::checksum` before reporting success. A file
whose stored bytes have been corrupted or truncated fails here instead of being
returned as if nothing were wrong. They differ only in where the bytes go:

| Method | Destination | Returns | Memory |
| ------ | ----------- | ------- | ------ |
| `download_file_verified` | a buffer it owns | `Vec<u8>` | the whole file |
| `download_file_verified_to` | a `&mut W` you own, `W: AsyncWrite + Unpin + ?Sized` | `u64`, the bytes written | one chunk |

**But the server never verified the uploader's checksum.** It checks that
the value is 32 bytes long and stores it; it never hashes what it received
to confirm the two agree. So a match proves the bytes you just received are
the bytes the uploader *declared*: it catches storage corruption, a
truncated transfer and a partially overwritten file, and it does **not**
catch an uploader that sent a checksum which never matched its own payload.
End-to-end integrity against a source you do not control needs a digest
carried out of band, not this.

Nothing is checked atomically with the download either: `stat_file` and the
download are two calls, so a file replaced between them reads as a mismatch
rather than as the new version. Replacement is atomic server-side, so the
mismatch is the worst case — never a mixed file.

Failures are `RociaDbError::SizeMismatch` (checked first: a truncated stream
fails both checks, and the byte count is the more actionable report) then
`RociaDbError::ChecksumMismatch` — including when the stored checksum is not
a 32-byte SHA-256 digest at all, since the comparison is over raw bytes.
`download_file_verified_to` adds one more: `RociaDbError::Io` with `context`
`"writing the downloaded file"`, when your writer refuses a chunk or the final
flush fails. Otherwise whatever `stat_file` and the download return, `NOT_FOUND`
for an unknown `file_id` included.

`download_file_verified` buffers the whole file, like `download_file`. Its
buffer is pre-allocated from the reported size but **capped at 64 MiB**, so a
server reporting an absurd `size_bytes` cannot make the client reserve
gigabytes before a byte has arrived; a genuinely larger file simply grows its
buffer as it streams. Either way the stream is abandoned as soon as it
overshoots `size_bytes` — that chunk is never written — rather than being
drained to the end only to be rejected.

#### A failed verification has already written bytes

This is the one thing `download_file_verified_to` asks of you in exchange for
the memory it saves. **Verification can only fail at the end**: a file's digest
is not known until its last byte has arrived, and a byte count cannot be
checked earlier either. So when it returns `SizeMismatch` or
`ChecksumMismatch`, your writer has already received most or all of the file,
and **discarding it is your job** — delete the temporary file, roll back the
transaction, truncate the buffer. Writing straight to the destination path
makes a failure overwrite good data with bad; download to a temporary name and
rename on `Ok`, as in the example above. `download_file_verified` has no such
hazard: it owns the buffer it throws away, which is reason enough to prefer it
whenever the file does fit in memory.

A caller who needs the chunks themselves — rather than a writer to put them in
— does the same thing by hand over `download_file_stream`: hash each chunk as
it arrives and compare the digest against `stat_file`'s `checksum` at the end.

## Metadata, listing and deletion

`stat_file` returns a `StatResponse` with `size_bytes`, `content_type`,
`checksum`, `created_at` and `updated_at`.

```rust,no_run
# use rociadb_sdk::{RociaDbBuilder, WriteOptions};
# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
let buckets = client.list_buckets("tenant-1", None, None).await?;
let files = client.list_files("tenant-1", "assets", Some(100), None).await?;
println!("{} buckets, {} files", buckets.items.len(), files.items.len());

client
    .delete_file("tenant-1", "assets", "manual.txt", WriteOptions::new())
    .await?;
# Ok(())
# }
```

`list_buckets` lists bucket names holding at least one file. `delete_file`
is **idempotent**, like `delete_document` and `delete_edge`: deleting a
`file_id` that does not exist succeeds and touches nothing, so call
`stat_file` first when you need to know whether the file was there. An
upload still in flight is never deleted.
