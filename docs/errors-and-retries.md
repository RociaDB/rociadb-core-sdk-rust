# Errors and retries

Every public method returns `rociadb_sdk::Result<T>`, an alias for
`std::result::Result<T, RociaDbError>`. `RociaDbError` is a typed
`#[non_exhaustive]` enum, not a boxed `dyn Error`, so a caller branches on
the failure kind with a `match` instead of downcasting — with a wildcard arm
to stay forward-compatible. Every variant implements `std::error::Error`, so
`RociaDbError` also composes normally with `?` inside a function returning
`anyhow::Result<..>`.

## The variants

| Variant | Raised when |
| ------- | ----------- |
| `Status { operation, status }` | a gRPC call returned a non-OK status; carries the raw `tonic::Status`, so nothing is lost compared with calling the generated client directly |
| `Config { message, source }` | the client is misconfigured, detected by `RociaDbBuilder` **before** any connection is attempted |
| `Connection { message, source }` | a genuine dial or transport failure: DNS, a refused or reset connection, a connect timeout, a TLS handshake |
| `Auth { message, source }` | obtaining or refreshing the upstream token failed |
| `Encode { context, source }` | a value could not be serialized to JSON before being sent |
| `Decode { context, source }` | a JSON payload received from upstream could not be decoded; for a page of documents the message leads with `"item <index>: "`, naming the offending position |
| `Io { context, source }` | an I/O handle **you** handed over failed, carrying the `std::io::Error`: an `Err` item from `upload_file_chunked`'s chunk stream, or a writer that refused the bytes of a `download_file_verified_to` |
| `Validation(String)` | a client-side rule about the *data* of one call was violated before any network call |
| `ChecksumMismatch { expected, actual }` | a verified download's SHA-256 digest disagrees with the stored checksum |
| `SizeMismatch { expected, actual }` | a verified download's byte count disagrees with the stored `size_bytes` |

`Config`, `Connection` and `Auth` fold their optional underlying cause into
`Display`, so a bare `.to_string()` already distinguishes a DNS failure from
a TLS mismatch from a refused connection. `Io` does the same with its
`std::io::Error`, which stays reachable as the `source` when you need to match on
its `kind()`.

### `Config` vs `Connection` vs `Validation`

Three variants carry no gRPC status because nothing ever reached the server,
and the boundary between them is the one worth knowing. A `Config` error is
fixed by changing how the client is built, a `Connection` error by fixing
the network or the server, a `Validation` error by changing the arguments of
one call.

| Failure | Variant |
| ------- | ------- |
| missing host; a host URL carrying a path, query string or fragment; an unparseable host URL | `Config` |
| missing `AUTH_TOKEN_URL` / `AUTH_CLIENT_ID` / `AUTH_CLIENT_SECRET` | `Config` |
| a `ClientTlsConfig` the endpoint rejects | `Config` |
| a zero `connect_timeout`, `request_timeout` or `max_file_bytes` | `Config` |
| a failed dial, a DNS failure, a refused connection | `Connection` |
| a file over `max_file_bytes`, a zero page limit, a chunk stream whose length disagrees with the declared size | `Validation` |

### `Io`

A fourth variant carries no status, for a third reason: the failure is in an
I/O handle **you** handed over. Two calls can raise it, and the `context` field
says which:

- `"the upload chunk stream"` — `upload_file_chunked` takes a
  `Stream<Item = std::io::Result<Bytes>>`, and an `Err` item (a read that died
  on your file or socket) ends the upload, ahead of whatever status the server
  returned for the stream that then stopped early. Nothing is pulled from the
  stream after the error, and nothing is stored: the server only publishes a
  file once it has received and validated the whole stream.
- `"writing the downloaded file"` — `download_file_verified_to` writes into a
  `tokio::io::AsyncWrite` of yours, and a chunk it refuses (or a failing final
  flush) abandons the download there. Whatever was already written is yours to
  clean up.

See [files](files.md).

### `ChecksumMismatch` and `SizeMismatch`

These two are different again: the call *succeeded* and the data it returned
is wrong. They are raised only by the two verified downloads —
`download_file_verified` and `download_file_verified_to`, the only calls in
this SDK that check what they downloaded — and never by the server. The size
is checked first, because a truncated transfer fails both checks and the byte
count is the more actionable report. Both digests are carried raw rather than
hex-encoded, so a caller can compare or store them without decoding;
`Display` renders them as lowercase hex. See [files](files.md) for exactly
what a match does and does not prove — and, for the writer variant, for why a
mismatch leaves bytes behind for you to discard.

## Reading a status

For `Status`, three accessors return `Some`; for every other variant they
return `None`.

- `code()` — the `tonic::Code`.
- `status()` — the raw `tonic::Status`, for anything the other two do not
  cover.
- `reason()` — the `reason` trailing metadata the server attaches to every
  error.

Six of the seven `reason` values are exactly the snake_case name of the code
returned alongside them — `invalid_argument`, `not_found`, `already_exists`,
`permission_denied`, `unauthenticated`, `internal` — so branching on
`reason()` partitions errors no differently than branching on `code()`.
Prefer the code (or a predicate) for control flow and use `reason()` when
you need a stable string to log or forward. The one exception is `ABORTED`,
whose `reason` is `"conflict"` rather than the `"aborted"` the pattern would
predict; the naming carries no extra meaning.

`DEADLINE_EXCEEDED` is the one code the SDK itself can produce without the
server saying anything: it is what a
[`request_timeout`](transport.md#request-timeout) expiring looks like.

## Predicates

```rust,no_run
# use rociadb_sdk::RociaDbBuilder;
# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
let outcome = client
    .get_document::<serde_json::Value>("tenant-1", "products", "sku-123")
    .await;

let document = match outcome {
    Ok(document) => Some(document),
    // Absent is usually a value in the caller's domain, not a failure.
    Err(error) if error.is_not_found() => None,
    // A bug in the caller: the arguments have to change, not the timing.
    Err(error) if error.is_invalid_argument() => return Err(error),
    Err(error) => return Err(error),
};
# let _ = document;
# Ok(())
# }
```

| Predicate | Code | What it means here |
| --------- | ---- | ------------------ |
| `is_unauthenticated()` | `UNAUTHENTICATED` | on every call but `upload_file_chunked` / `upload_file_stream`, a *refreshed* credential was rejected too — see [authentication](authentication.md) |
| `is_permission_denied()` | `PERMISSION_DENIED` | valid token, missing scope: final, a refresh will not help |
| `is_not_found()` | `NOT_FOUND` | every single-item read can return it, and so can `add_edge` when an endpoint node is missing. Listings return an empty page instead, and the deletes are idempotent, so neither ever reports it |
| `is_invalid_argument()` | `INVALID_ARGUMENT` | a rule only the server knows; retrying changes nothing |
| `is_already_exists()` | `ALREADY_EXISTS` | in this SDK, only `add_edge` / `add_edges`, from the `(from, label, to)` uniqueness rule |
| `is_aborted()` | `ABORTED` | a transient storage conflict the caller is expected to replay — below |

## `ABORTED`: the code every caller must replay

`ABORTED` is the one status in this API a caller must retry automatically,
not merely may. It reports a transient storage conflict, never a terminal
refusal: the call was safe and the identical request would very likely
succeed moments later.

**It is not limited to writes.** On the `tikv` storage backend a write
conflict is only one of three causes — a lock held by a concurrent
transaction and a region error are the other two — and both of those can
surface on *any* call, reads included: `get_document`, `list_documents`,
`query_documents`, `search_documents`, `list_collections` and
`download_file` can all return it. A retry policy that wraps only mutating
calls will be caught out by one on a plain read.

`put_document` and `delete_document` absorb transient conflicts with up to
**5 internal server-side retries** before surfacing `ABORTED`. Every other
write — `put_node` / `put_nodes`, `add_edge` / `add_edges`, `delete_edge`,
`upload_file`, `delete_file` — and every read has no such cushion and can
return it on the very first conflict. What the caller has to do is identical
either way; the internal retries only raise how much contention it takes to
see one.

**"Transient" does not mean "nothing was written."** A document write, its
index entries, its collection counter and its idempotency marker commit
inside one transaction, so a conflict on that transaction really does leave
nothing behind. But confirming the idempotency marker is a separate step
that happens *after* that commit — so an `ABORTED` raised by that step
arrives on a document that is already durably written. Harmless to retry:
the replay, with the same `request_id`, is recognized as a duplicate of an
already-applied write and short-circuits to `Ok` instead of writing again.
It does mean `ABORTED` must never be read as proof that a write did not
happen.

A genuinely concurrent duplicate — a second call sharing a `request_id` with
one still in flight — also gets `ABORTED` rather than `Ok`, for the same
reason: the original has not finished, so the server cannot claim success on
its behalf. Replaying afterwards with that same `request_id` finds the
now-finished write and returns `Ok` without executing it again. The same
holds if the in-flight call was interrupted — a dropped connection, an
expired deadline, or the server dying between the write and the marker
confirmation: the pending reservation holds for `gc.request_lease_secs`
(**300 seconds by default**), every replay before that lease expires gets
`ABORTED` too, and the first one after it re-executes the write. Nothing is
corrupted or lost in either case: a reservation never returns `Ok`, so none
of these `ABORTED`s masks a write that silently failed to happen.

Space retries out under sustained contention rather than looping tightly —
several minutes of `ABORTED` on a call whose deadline already expired
client-side is expected there, not a sign of an outage.

## `RetryPolicy` and `RociaDbClient::retry`

`RetryPolicy` describes *how long* to wait between attempts;
`RociaDbClient::retry(&policy, op)` runs a closure under it.

```rust,no_run
# use rociadb_sdk::{EdgeInput, RetryPolicy, RociaDbBuilder};
# use serde_json::json;
# use std::time::Duration;
# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
let policy = RetryPolicy::new()
    .with_max_attempts(5)
    .with_base_delay(Duration::from_millis(50))
    .with_max_delay(Duration::from_secs(1));

// The key is minted once, outside the closure, so every attempt carries the
// same one and the server can recognize a replay.
let edge = EdgeInput::new(
    "edge-1",
    "product:sku-1",
    "group:grp-1",
    "belongs_to",
    json!({"weight": 1}),
)
.with_request_id("import-batch-7:edge-1");

client
    .retry(&policy, || {
        let edge = edge.clone();
        async { client.add_edge("tenant-1", "catalog", edge).await }
    })
    .await?;
# Ok(())
# }
```

**Schedule.** Attempt *n* (zero-based, so retry *n* is attempt *n + 1*)
waits a random duration drawn uniformly from
`[0, min(base_delay * 2^n, max_delay)]` — exponential backoff with *full
jitter*. Full jitter rather than a fixed delay because the callers that see
`ABORTED` are usually contending with each other: identical backoffs would
collide again on the next attempt. With the defaults the ceilings are 100 ms,
200 ms, 400 ms, …, 2 s, 2 s, and the actual waits are uniform below each.
The jitter is drawn from a v4 UUID's random bytes, so no `rand` dependency
was added.

| Field | Setter | Default |
| ----- | ------ | ------- |
| `max_attempts` | `with_max_attempts` | 3 (so at most two retries; `1` disables retrying, `0` is read as `1`) |
| `base_delay` | `with_base_delay` | 100 ms (`Duration::ZERO` retries with no sleep at all) |
| `max_delay` | `with_max_delay` | 2 s (applied last, so a value below `base_delay` wins) |
| `retry_unavailable` | `with_retry_unavailable` | `false` |

**What is retried.** `ABORTED` always. `UNAVAILABLE` only when
`with_retry_unavailable(true)` is set: it means the SDK could not reach a
working server at all — tonic reports a failed dial that way, and so does a
server that is restarting or draining. Retrying it is safe for a read, and
safe for a write **that carries a caller-supplied `request_id`**; it is off
by default because for a write with an SDK-generated key a retry is a
second, distinct write as far as the server is concerned, and the original
may well have been applied before the connection broke. Nothing else is ever
retried — replaying an `INVALID_ARGUMENT` or a `NOT_FOUND` would only turn
one clear failure into several slow ones, and `UNAUTHENTICATED` is already
refreshed and retried inside the client.

**Reuse one `request_id` across every attempt.** Because `ABORTED` never
proves nothing was written, a retried write must carry the *same* key as the
attempt that failed. Build it once, outside the closure, and pass it in with
`WriteOptions::with_request_id` (or the equivalent setter on
`DocumentWriteOptions`, `FileUploadOptions`, `FileStreamUploadOptions`,
`NodeInput`, `EdgeInput`). A closure that leaves `request_id` unset gets a
*fresh* SDK-generated key on every attempt, which is exactly the case the
server cannot deduplicate. Reads need no such care.

`retry` never returns a synthetic "retries exhausted" error of its own: the
first error the policy does not retry is returned immediately, and so is the
last error once `max_attempts` is exhausted — so `code()` and `reason()`
still describe what the server actually said.
