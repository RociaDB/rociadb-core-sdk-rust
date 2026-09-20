# Transport and TLS

## Where TLS terminates

**RociaDB does not implement TLS itself and never will: the server always
listens in plaintext.** In production, TLS terminates at a reverse proxy
placed in front of it — the proxy holds the certificates and handles
renewal. The SDK then connects to the proxy with `https://`, typically on
port 443, and the proxy forwards plaintext gRPC to the backend on its own
port (conventionally 5xxxx).

The proxy must be configured for **HTTP/2 end to end**: a TLS-to-HTTP/1.1
downgrade toward the backend makes every gRPC call fail immediately, usually
with `UNAVAILABLE`.

Connecting `http://` directly to the bare server — as the examples
throughout these guides do — is appropriate only for local development or an
already-encrypted internal network segment.

**tonic decides whether to wrap the socket from the URI scheme alone.** A
TLS configuration is applied to an `https://` host and silently ignored on a
`http://` one. If TLS matters, the host must say `https://`.

## Host validation

`.host(..)` must be a bare `scheme://host:port`. `build()` rejects a path
(beyond an absent one or a lone `/`), a query string or a fragment with
`RociaDbError::Config`, **before any connection attempt** — so a mistyped
host with a leftover path (`http://127.0.0.1:50051/v1`, pasted from
somewhere else) fails loudly instead of tonic dropping the extra components
on the floor and dialing the host anyway.

The default host is `http://127.0.0.1:50051`.

## Connect timeout

`connect_timeout` sets the deadline applied while dialing. It defaults to
**10 seconds** when never called: `build()` always applies one, so a slow or
unreachable DNS/TCP target fails after a bounded wait instead of hanging
`.await` forever. A zero duration is rejected with `RociaDbError::Config` at
`build()` time.

The same value is what the OAuth2 HTTP client uses to reach the identity
provider; that client additionally carries a fixed 30-second overall request
timeout, so an IdP that accepts the connection and never answers cannot hang
`build()` or a later refresh.

## Request timeout

`request_timeout` puts a deadline on every **unary** RPC. It is opt-in with
no default, because a sensible value depends on what the caller's own calls
do — a `query_documents` over a large collection is not a `get_document` —
so the SDK never invents one. A zero duration is rejected at `build()` time.

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

It is enforced two ways at once, per attempt:

- the `grpc-timeout` header is set on the request, so the **server** learns
  the deadline and can abandon work nobody is waiting for. tonic's client
  channel also enforces that header locally, but only up to the response
  headers;
- the whole call future is wrapped in a `tokio::time::timeout`, which
  additionally covers decoding the response message and its trailers.

Either way the call fails with `RociaDbError::Status` whose `code()` is
`DeadlineExceeded` — one code for one cause, whichever layer notices first.

**It is per attempt, not per call.** When auth is enabled and the first
attempt comes back `UNAUTHENTICATED`, the refreshed retry gets a fresh
deadline of its own, so a single call can take up to twice this long. The
same is true of each attempt made by `RociaDbClient::retry`.

**File transfers are deliberately not covered** — neither the two streaming
RPCs (`upload_file_stream`, `download_file_stream`) nor the `upload_file`,
`upload_file_chunked`, `download_file`, `download_file_verified` and
`download_file_verified_to` helpers built on them (the `stat_file` call inside
the last two is a unary RPC and is covered). How long a stream takes is a
property of its own data rate, not of the SDK, and a deadline meant for a
single round trip would abort a perfectly healthy multi-gigabyte transfer.
Bound those with a `tokio::time::timeout` of your own.

That holds even for the *opening* call of a download, which otherwise goes
through the same path as a unary RPC: the exclusion is about the `grpc-timeout`
header, which announces a deadline for the whole RPC. tonic's own server enforces
that header only up to the response headers, so against a tonic server it would
not truncate a slow body — but it *would* fail a download whose first header
takes longer than the deadline, reporting `CANCELLED` for a transfer that was
merely slow to start. And the gRPC specification makes the header a deadline for
the entire call, which most implementations honour by cutting the stream once it
expires.

## Maximum file size

`max_file_bytes` is the one builder setting that has nothing to do with the
transport: it caps the size of a file `upload_file` and `upload_file_chunked`
will send, defaults to **5 GiB** (the default of the server's own
`limits.max_file_bytes`, which it only mirrors — the server still has the final
say), and rejects a zero value at `build()` time with `RociaDbError::Config`
like both timeouts. It applies identically to a client built with
`build_with_channel`, since no dialing is involved. See
[files](files.md#the-client-side-size-ceiling).

## Custom TLS

`tls_config` **replaces** the default
`ClientTlsConfig::new().with_native_roots()` — the operating system's trust
store, which is also what the OAuth2 HTTP client trusts. Replaces, not
extends: a fresh `ClientTlsConfig` carries an *empty* trust store, and nothing
re-adds the OS roots on your behalf. A config built with only `domain_name`,
or only `ca_certificate`, therefore trusts nothing else — every certificate
signed by a public CA fails to verify, and the channel cannot connect at all.

Call `with_native_roots()` yourself unless trusting *only* your own CA is
exactly what you want:

```rust,no_run
use rociadb_sdk::{ClientTlsConfig, RociaDbBuilder};

# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
let client = RociaDbBuilder::new()
    .host("https://rociadb.internal:443")
    .tls_config(
        ClientTlsConfig::new()
            .with_native_roots()
            .domain_name("rociadb.internal"),
    )
    .build()
    .await?;
# let _ = client;
# Ok(())
# }
```

Add a private CA with `ca_certificate`, present a client certificate for mTLS
with `identity`, and override the name the server's certificate is verified
against with `domain_name`.

**A private CA has to be installed in two places.** `tls_config` governs the
gRPC channel only; the OAuth2 token client keeps reading the OS trust store
(`reqwest` is built with `rustls-tls-native-roots` for precisely that reason).
Narrowing the channel to a private CA while the token endpoint presents a
publicly-signed certificate — or the reverse — leaves you with a channel that
connects and a token fetch that fails TLS verification, which `build()` then
surfaces as a confusing auth error. Install the CA at the OS level as well and
keep `with_native_roots()` on the channel, so the two agree.

`ClientTlsConfig` and `Channel` are re-exported at the crate root so that
configuring the SDK needs no extra direct dependency. Building a
`Certificate` or an `Identity` to hand to `ca_certificate` / `identity` does
need `tonic` itself as a direct dependency — those types are not re-exported,
because they appear in no signature of this crate.

A configuration the endpoint rejects surfaces as `RociaDbError::Config`, not
`Connection`: nothing has been dialed yet.

## HTTP/2 keep-alive

`http2_keep_alive(interval, timeout)` sends keep-alive pings on the
connection every `interval` and closes it when a ping goes `timeout`
unanswered. It is off by default, which is tonic's own default.

Turn it on for a client that holds a connection open through something that
silently drops idle flows — a NAT, a stateful firewall, a cloud load
balancer: with no keep-alive the SDK only discovers the dead connection when
the next RPC fails on it. `keep_alive_while_idle(true)` is implied, so the
pings continue while no RPC is in flight, which is exactly when the flow
would otherwise be reaped.

Pick an `interval` comfortably shorter than the idle timeout you are working
around (30 s against a 60 s NAT, say) and a `timeout` of a few seconds. A
much shorter `interval` wastes a round trip per tick per connection, and
some servers reject pings they consider too frequent with an HTTP/2
`ENHANCE_YOUR_CALM`.

## Bringing your own channel

`build_with_channel` takes a `Channel` you already have and skips both host
validation and dialing — for a transport `Endpoint` cannot express on its
own: a custom connector, a Unix domain socket, a load-balanced
`Channel::balance_list`, or an in-process server in a test. A lazy channel
works too, and moves the first connection attempt to the first RPC.

```rust,no_run
use rociadb_sdk::{Channel, RociaDbBuilder};

# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
let channel = Channel::from_static("http://127.0.0.1:50051").connect_lazy();
let client = RociaDbBuilder::new()
    .disable_auth()
    .build_with_channel(channel)
    .await?;
# let _ = client;
# Ok(())
# }
```

Everything auth-related still happens exactly as in `build()`: the first
token is fetched, the background refresh starts, and the bearer interceptor
is installed on all four service clients. What is skipped is only what
belongs to the channel — the host URL checks, `tls_config` and
`http2_keep_alive`, because the channel handed in has already decided all of
it. Both timeouts are still read and validated: `request_timeout` applies to
every unary RPC however the channel was built, and `connect_timeout` is what
the OAuth2 HTTP client uses to reach the IdP, the one connection this method
does open itself.

`Debug` on the resulting client reports the host *configured on the
builder*, which here is only a label — the channel decides where the requests
actually go.

## Reusing a builder, and reusing a client

`build()` and `build_with_channel()` take `&self`, so one builder can
produce several clients. Every setter takes `self` and returns `Self`, so a
configuration can be written as one chain from a temporary or kept in a
variable and extended a step at a time.

`RociaDbClient` is `Clone` and every clone shares one channel, one token
manager and one background refresh task. Cloning is allocation-free, every
method takes `&self`, and a client behind an `Arc` needs no `Mutex`. There
is no `close()`: drop the last live clone to release the connection and stop
the refresh task.
