# Authentication

`rociadb-sdk` authenticates with the OAuth2 **client-credentials** grant.
`RociaDbBuilder` has it enabled by default: `build()` fetches the first
token, installs an interceptor that stamps an `authorization` header on
every outgoing RPC, and starts a background task that keeps the token fresh
for as long as the client — or any of its clones — is alive.

Four things then happen on their own, and none of them needs calling code:
scheduled refresh, fast recovery from a failed refresh, refresh-and-retry on
`UNAUTHENTICATED`, and a pre-flight refresh before a streaming call.

## Configuring credentials

Supply the credentials on the builder, or leave them to the environment:

```rust,no_run
use rociadb_sdk::RociaDbBuilder;

# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
let client = RociaDbBuilder::new()
    .host("https://rociadb.internal:443")
    .auth_client_credentials(
        "https://example.com/token",
        "client-id",
        "client-secret",
    )
    .build()
    .await?;
# let _ = client;
# Ok(())
# }
```

Without `auth_client_credentials`, `build()` reads three environment
variables — and fails with `RociaDbError::Config` naming the missing one if
any is absent:

| Variable | Meaning |
| -------- | ------- |
| `AUTH_TOKEN_URL` | OAuth2 token endpoint |
| `AUTH_CLIENT_ID` | client identifier |
| `AUTH_CLIENT_SECRET` | client secret |

For a controlled local or test deployment, turn the whole mechanism off.
`build()` emits a `warn!` when it does:

```rust,no_run
# use rociadb_sdk::RociaDbBuilder;
# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
let client = RociaDbBuilder::new()
    .host("http://127.0.0.1:50051")
    .disable_auth()
    .build()
    .await?;
# let _ = client;
# Ok(())
# }
```

A `token_url` that is not `https://` is allowed — local development needs it
— but logged as a `warn!`, because the client id, the client secret and the
token that comes back all cross the wire in cleartext.

## Secrets are `SecretString`

The client secret and the bearer token are held as
`secrecy::SecretString`, re-exported at the crate root together with
`ExposeSecret`. They are redacted by every formatter, including the derived
`Debug` of the builder and of `TokenResponse`, and their heap buffers are
zeroized when the last owner is dropped. `RociaDbClient`'s own `Debug`
reports the host and whether auth is enabled — never a token, a client id or
a secret.

`auth_client_credentials` still takes `impl Into<String>` and wraps the
secret on arrival, so ordinary builder code is unchanged. Pass an owned
`String` where you can: it moves straight into the secret rather than being
copied out of a buffer nothing will scrub. A caller already holding a
`SecretString` hands over `secret.expose_secret().to_string()` — one
deliberate, visible exposure, which is the point of the type.

## Token lifetime and scheduled refresh

The identity provider (`rocia-idp`) issues bearer tokens that live for
exactly **600 seconds**, hardcoded server-side — there is no negotiating a
longer-lived token.

The background task refreshes at `max(expires_in * 2 / 3, 5s)`, clamped so
it never exceeds the token's own remaining lifetime: roughly every 400
seconds for the current 600-second tokens, leaving about a third of the
lifetime as margin. `expires_in` is re-read on every refresh, so the cadence
tracks an IdP that changes it rather than being baked in once at startup.
The task stops when the last clone of the client is dropped.

## Recovering from a failed refresh

A refresh that fails is **not** left to the next regular tick — that would
be 400 seconds away and would guarantee a window, the last 200 seconds of
the token's life, in which every RPC fails with `UNAUTHENTICATED`.

Consecutive failures back off exponentially with jitter instead: roughly
1 s, 2 s, 4 s, 8 s, 16 s, then 30 s for every attempt after that, each delay
drawn from the **upper half** of its ceiling (so the first retry lands
between 0.5 s and 1 s, the second between 1 s and 2 s, and so on). The
jitter keeps a fleet of clients whose tokens expire together from
synchronising into a thundering herd against the IdP. The first refresh that
succeeds resets the count and returns the task to the normal cadence.

Every failure is reported once as a `warn!` carrying the error, how many
refreshes have now failed in a row, and how long the next attempt is away.
A still-valid cached token is never discarded because a refresh attempt
failed: the interceptor keeps injecting the last known-good token until a
replacement is confirmed.

## Refresh-and-retry on `UNAUTHENTICATED`

Every **unary** RPC goes through one private helper on the client. When auth
is enabled and the first attempt comes back `UNAUTHENTICATED` — the status
the server uses to mean "renew your token" — that helper triggers one
coalesced token refresh and re-issues the call exactly once.

- Never more than once. A second `UNAUTHENTICATED` against a token minted
  moments earlier is a credential or scope problem that looping cannot fix.
- If the refresh itself fails, the **original** `UNAUTHENTICATED` is
  returned (it describes what the caller actually asked for) and the refresh
  failure is reported as a `warn!`.
- The deadline from `request_timeout` is per attempt, so a call that is
  retried this way can take up to twice that long.

So a caller who sees `RociaDbError::is_unauthenticated()` on a call that is
covered here is seeing a refreshed credential being rejected too — the client id
or secret is wrong, revoked, or not entitled to this deployment. The fix is in
the configuration, not in another retry.

## What the streaming RPCs get

The two streaming RPCs — `Upload` and `Download` — cannot all be treated the
same way, because what can be replayed differs. Both halves below are automatic;
neither needs calling code.

| Call | Pre-flight refresh | Replay after `UNAUTHENTICATED` |
| ---- | ------------------ | ------------------------------ |
| `upload_file` | yes | **yes** — same buffer, same `request_id` |
| `upload_file_chunked` | yes | no |
| `upload_file_stream` | yes | no |
| `download_file_stream` / `download_file` / `download_file_verified` / `download_file_verified_to` | yes | **yes** — on the call that opens the stream |

**The pre-flight refresh.** Before opening any of these calls, the SDK refreshes
the token if less than **five seconds** of its advertised lifetime is left, and
otherwise does nothing at all (one clock read). Five seconds is enough because a
gRPC server validates the bearer token **once, when it accepts the call** — a
transfer that outlives its token keeps running, so the margin only has to cover
the gap between reading the cached token and the server checking it. A pre-flight
refresh that fails is a `warn!` rather than an error: the cached token may well
still be valid, so the call goes ahead with it, and if it really is finished the
server's `UNAUTHENTICATED` reaches you as usual. Concurrent callers are coalesced
into one fetch, like every other refresh. `TokenManager::ensure_fresh(margin)` is
the same check, public, for code driving authentication itself.

**The replay.** `upload_file` owns its buffer, so it can rebuild the request
stream and re-send it once — with the **same** `request_id`, which is what makes
that safe: the server deduplicates on it, so a replay that lands on an upload it
had already committed is absorbed rather than written twice. A download is
replayable for a different reason: a server that rejects a server-streaming call
does so before any message exists, so tonic resolves the opening call with the
status and there is nothing consumed to replay around. Neither carries a
`grpc-timeout` header (`request_timeout` does not apply to a transfer), and
neither covers a status that arrives later, in the stream's trailers.

**Why not the other two.** `upload_file_chunked` and `upload_file_stream` are
handed a stream that belongs to the caller, and h2 begins writing body frames as
soon as the call opens — so "nothing has been consumed yet" is not a state the
SDK can establish, let alone rely on, and a replay would mean re-draining a
stream that is already partly drained. Recover by hand: call
`refresh_auth_token()` and re-issue with a freshly built stream, reusing the same
`request_id` so the server recognizes the replay.

## `refresh_auth_token` vs `invalidate_auth_token`

Both remain for driving a refresh out of band, and both are no-ops when the
client was built with `disable_auth()`.

`refresh_auth_token()` is **eager**: it awaits the round trip to the
identity provider and returns only once a fresh token is in hand, or
propagates the fetch error. Concurrent callers are coalesced into a single
in-flight fetch, so a fleet of tasks all recovering at once produces one
POST rather than one per task — **including when that POST fails.** A
coalesced caller is handed the concurrent attempt's error rather than
repeating the request, because the refresh lock is held across the round trip:
N callers each retrying a dead identity provider would serialize behind it at
up to 30 seconds apiece, and the last in the queue would wait for the sum of
all of them however short its own `request_timeout` is. What a coalesced
caller never gets is a bare `Ok(())` for a refresh that did not happen. A
caller arriving *after* an attempt has settled makes its own, so one bad
minute at the identity provider does not wedge the client. Reach for it right
before retrying a call
that just failed, for a token you know has been revoked, for a credential
rotation to pick up immediately, or after an `UNAUTHENTICATED` on an
`upload_file_chunked` / `upload_file_stream` — the two calls nothing retries for
you.

`invalidate_auth_token()` is its **lazy** counterpart: synchronous, returns
immediately, and only wakes the background refresh task so it fetches at its
next opportunity — nobody pays for the network round trip inline. It is
honoured even while the task is waiting out a retry backoff.

```rust,no_run
# use rociadb_sdk::{FileStreamUploadOptions, RociaDbBuilder};
# use tokio_util::io::ReaderStream;
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
# let client = RociaDbBuilder::new().build().await?;
# let (size_bytes, checksum) = (0u64, [0u8; 32]);
// A chunked upload is not retried for you — the SDK cannot re-drain a stream
// you handed over — so refresh and re-issue it yourself, with a *fresh* stream
// and the same `request_id` so the server recognizes the replay.
let options = FileStreamUploadOptions::new(size_bytes, checksum)
    .with_request_id("import-42:large-report.csv");
let source = || async {
    Ok::<_, std::io::Error>(ReaderStream::new(
        tokio::fs::File::open("large-report.csv").await?,
    ))
};
let upload = client.upload_file_chunked(
    "tenant-1",
    "reports",
    "large-report.csv",
    source().await?,
    options.clone(),
);
match upload.await {
    Ok(()) => println!("uploaded"),
    Err(error) if error.is_unauthenticated() => {
        client.refresh_auth_token().await?;
        client
            .upload_file_chunked(
                "tenant-1",
                "reports",
                "large-report.csv",
                source().await?,
                options,
            )
            .await?;
        println!("uploaded on the second attempt");
    }
    Err(error) => return Err(error.into()),
}

// Elsewhere — a background health check, say — that observed staleness but
// is not the caller who has to retry: signal it and move on. No `.await`,
// no network call here.
client.invalidate_auth_token();
# Ok(())
# }
```

## `UNAUTHENTICATED` vs `PERMISSION_DENIED`

The two need different handling, and confusing them wastes retries:

- **`UNAUTHENTICATED`** — the token is missing, expired, malformed, or
  issued by a different issuer. This is the renewal signal, and every call but
  `upload_file_chunked` and `upload_file_stream` already acts on it.
- **`PERMISSION_DENIED`** — the token is valid but lacks the required scope.
  Retrying after a refresh will not help, because a fresh token carries the
  same scope. It happens in exactly two cases: a read-only client calling
  one of the 7 write RPCs, or an admin-scoped token (from `rocia-idp`'s
  account-management API) calling *any* of the 23 RPCs, reads included. See
  [tenancy and authorization](tenancy.md).

## Driving authentication yourself

`auth` is the crate's one public module, for callers who need a token
outside the `RociaDbClient` lifecycle — to reuse the same credential against
a different service, for example.

```rust,no_run
use rociadb_sdk::SecretString;
use rociadb_sdk::auth::TokenManager;
use std::time::Duration;

# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
// Supply a client with timeouts: `reqwest::Client::new()` has none at all,
// so an IdP that accepts the connection and never answers would hang the
// fetch — and every later refresh, each holding the refresh lock — forever.
let http = reqwest::Client::builder()
    .connect_timeout(Duration::from_secs(10))
    .timeout(Duration::from_secs(30))
    .build()
    .expect("build the OAuth2 HTTP client");

let manager = TokenManager::new(
    http,
    "https://example.com/token".to_string(),
    "client-id".to_string(),
    SecretString::from("client-secret".to_string()),
)
.await?;

// `spawn_refresh` returns a `#[must_use]` guard: dropping it stops the
// background refresh immediately, so bind it for as long as auth must work.
let _refresh_guard = manager.spawn_refresh(manager.refresh_interval());

// The interceptor that stamps the cached header onto an outgoing request.
let _interceptor = manager.interceptor();

// Mark the cached token stale without blocking; the background task picks
// it up. `TokenManager::refresh_now` is the awaiting counterpart.
manager.request_refresh();

// Refresh only if little of the cached token's lifetime is left — the check the
// SDK itself runs before a streaming call. A no-op (one clock read) otherwise.
manager.ensure_fresh(Duration::from_secs(5)).await?;
# Ok(())
# }
```

`auth::fetch_token` is the standalone one-shot fetch underneath all of it,
and `auth::TokenResponse` is what it returns: `access_token`
(a `SecretString`), `expires_in` and `token_type`.

## What the SDK accepts from an identity provider

Deserialization of the token response is deliberately tolerant of how real
providers differ from the letter of RFC 6749, since the alternative is a
`build()` that fails against a perfectly usable IdP:

| Field | Accepted | Missing |
| ----- | -------- | ------- |
| `access_token` | a JSON string | required — absence is an error |
| `expires_in` | a JSON number, whole (`3600`) or fractional (`3600.0`, truncated toward zero), **or** either written as a string (`"3600"`) | assumes 300 seconds and emits a `warn!` |
| `token_type` | any JSON string, case preserved (RFC 6749 §7.1 makes it case-insensitive) | assumes `"Bearer"` |

Unknown fields — `scope`, `refresh_token`, anything vendor-specific — are
ignored. A *present* `expires_in` that is not a usable number at all (a
non-numeric string, a negative number, an infinity) is still an error: the
IdP is saying something about the lifetime, and the SDK must not silently
substitute a guess for it.
