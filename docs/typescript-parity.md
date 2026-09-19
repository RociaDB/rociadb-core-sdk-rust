# Parity with the TypeScript SDK

This SDK and the TypeScript SDK
([`rociadb-core-sdk-ts`](https://github.com/RociaDB/rociadb-core-sdk-ts))
cover the same 23 RPCs against the same server, and are maintained to the
same standard: **every capability available in one is available in the
other.**

> **Parity unverified for `GetEdge`.** It is new in the upstream `.proto`
> and is wrapped here as `get_edge`. Whether
> [`rociadb-core-sdk-ts`](https://github.com/RociaDB/rociadb-core-sdk-ts)
> already carries it has **not** been checked from this repository — treat
> the parity claim above as unconfirmed for this one RPC. Per `AGENTS.md`
> the `.proto` change has to be mirrored there as well; confirm both, then
> remove this note.

> **Parity unverified for what 2.0 added.** `download_file_verified`,
> `neighbor_nodes_out` / `neighbor_nodes_in`, the `Config`,
> `ChecksumMismatch` and `SizeMismatch` error variants, `RetryPolicy` /
> `RociaDbClient::retry`, `request_timeout`, `tls_config`,
> `http2_keep_alive` and `build_with_channel` are all new on the Rust side
> in 2.0. Whether the TypeScript package has equivalents has **not** been
> checked from this repository. Confirm each against
> `rociadb-core-sdk-ts`, then fold the ones that match into the table below
> and open an issue there for the ones that do not.

Neither SDK imitates the other's syntax — this crate stays
snake_case/`Result`-idiomatic Rust, the TypeScript package stays
camelCase/exception-idiomatic TypeScript — and the two languages'
structural differences (ownership vs. garbage collection, exhaustive enum
matching vs. discriminated unions, RAII vs. explicit `close()`) legitimately
shape each SDK differently where a mechanical, character-for-character
translation would fight the language. Parity is about what you can *do*, not
about matching method names or shapes one-to-one, and most names do
translate mechanically (`put_nodes` ↔ `putNodes`,
`get_outgoing_neighbor_nodes` ↔ `getOutgoingNeighborNodes`, and so on).

The handful of places where a name or shape does **not** translate
mechanically — where translating a call by ear lands you on the wrong method
— are the table below.

The two are packaged and versioned independently: this crate is
`rociadb-sdk` on crates.io, the TypeScript package is `rocia-db-sdk` on npm,
and their version numbers advance separately. Neither the differing package
name nor a gap in major version is a signal of a capability difference.

| Capability | Rust ([`rociadb-core-sdk-rust`](https://github.com/RociaDB/rociadb-core-sdk-rust)) | TypeScript ([`rociadb-core-sdk-ts`](https://github.com/RociaDB/rociadb-core-sdk-ts)) | Note |
|---|---|---|---|
| Assisted streaming upload — re-chunks to the 1 MiB wire contract, validates the total, caller supplies the checksum | `upload_file_chunked` | `uploadFileStream` | Names do **not** correspond — see below. The Rust source stream is fallible (`Stream<Item = std::io::Result<Bytes>>`, so a `tokio_util::io::ReaderStream` needs no adaptation and a failed read surfaces as `RociaDbError::Io`); the TypeScript one takes an `AsyncIterable<Uint8Array>`, where a throwing iterator plays the same role. |
| Raw streaming upload — zero validation, caller builds every protobuf message | `upload_file_stream` | `uploadFileRaw` | Names do **not** correspond — the mirror image of the row above. |
| Idempotency key on a document write that can also bind a graph node | `put_document(t, c, id, &value, DocumentWriteOptions::new().with_request_id(rid).with_node_binding(..))` | `createDocument(..., { requestId })` | Same capability, different shape: an options struct passed as the last argument vs. an options object — the established pattern on each side. In Rust 2.0 the one key covers **both** writes (the server's dedup scope includes the operation, so the `PutDoc` and `PutNode` markers cannot collide), which makes replaying the whole call idempotent. |
| Releasing the connection and the background token-refresh task | Drop the last live `RociaDbClient` clone | `client.close()` | No Rust method by design — see below. |
| Lazy token invalidation at the level of the background refresh task itself (not the `RociaDbClient`-level wrapper, which *does* translate mechanically: `invalidate_auth_token` ↔ `invalidateToken`) | `TokenManager::request_refresh` | `TokenManager.invalidate()` | Different verb chosen independently on each side for the same "mark it stale, wake the background task, do not block" idea. |
| Standalone OAuth2 token fetch, usable outside of `TokenManager` | `auth::fetch_token` | `fetchOAuthToken` (exported from `auth.ts`, re-exported at the package root) | TypeScript needed a name that does not collide with the `fetch` Web API it wraps; Rust has no such collision. |
| Discriminating why an `Err` happened | `RociaDbError` — a `match`-able `#[non_exhaustive]` enum: `Status { .. }` / `Config { .. }` / `Connection { .. }` / `Auth { .. }` / `Encode { .. }` / `Decode { .. }` / `Io { .. }` / `Validation(String)` / `ChecksumMismatch { .. }` / `SizeMismatch { .. }` | `RociaDbError.kind: RociaDbErrorKind`, one class with a `"status" \| "connection" \| "auth" \| "encode" \| "decode" \| "validation"` field | Different shape, not just a different name — see below. The Rust enum also carries four causes the TypeScript union above does not name, all added in 2.0. |
| Escape hatch to the raw generated protobuf/gRPC types, to build a custom client against the same `.proto` | **none** — the generated module is private (`pub(crate) mod pb`). The generated types that reach a public signature are re-exported at the crate root instead: `CollectionInfo`, `StatResponse`, `Neighbor`, `UploadRequest`, `DownloadResponse`, and `Streaming` | the `rocia-db-sdk/proto` subpath export | **A real capability gap, not a naming difference.** TypeScript lets a caller reach every generated type; Rust deliberately does not, because the crate's public surface is under semantic versioning from 2.0.0 onward and generated code is reshaped by any prost or tonic upgrade. Reopen this if a Rust consumer needs it — it would be an addition, not a removal. |

## The error-kind trap, spelled out

Both sides represent "why did this fail" as one closed set of causes, but
differently. Rust's `RociaDbError` is a real sum type, marked
`#[non_exhaustive]` so a later release can recognize a new cause without
breaking existing callers: a match must carry a wildcard arm, and the
compiler flags a missing one among the causes it does name. TypeScript keeps
a single `RociaDbError` class — so an existing `instanceof RociaDbError`
check never breaks — and puts the choice in a `kind` field instead;
narrowing on `error.kind` gets the same exhaustiveness check from `tsc`, via
a discriminated union instead of a variant match. Neither representation is
"the same code translated"; each is the idiomatic way to express one closed
set of causes in its own language.

The sets themselves no longer line up exactly. Rust 2.0 split configuration
mistakes out of `Connection` into their own `Config` variant, added
`ChecksumMismatch` / `SizeMismatch` for `download_file_verified`, and added `Io`
for a chunk stream whose source failed to read. A
TypeScript caller porting a `match` will find four arms with no `kind` to
narrow on — see the unverified-parity note above, and
[errors and retries](errors-and-retries.md) for what each one means.

## The upload naming trap, spelled out

`upload_file_chunked` (Rust) and `uploadFileStream` (TypeScript) are the
*same* capability — the middle tier that re-chunks and validates for you
(see [files](files.md#the-three-upload-tiers)). `upload_file_stream` (Rust)
and `uploadFileRaw` (TypeScript) are also the *same* capability — the raw,
zero-validation escape hatch.

`upload_file_stream` and `uploadFileStream` are **not** each other's
counterpart, despite the near-identical name: the Rust one is the raw escape
hatch, the TypeScript one is the validated middle tier. Porting upload code
between the two SDKs by matching names alone silently swaps which tier you
land on.

## Why there is no Rust `close()`

`RociaDbClient` is `Clone`, and every clone shares one underlying channel
and one background refresh task by design (see
[authentication](authentication.md)) — a `close(&self)` would tear the
channel down out from under every other live clone, silently breaking that
documented guarantee. The idiomatic Rust equivalent already exists and gives
the identical guarantee: drop the last clone. `tonic::transport::Channel` is
itself cheap to clone and shares one real connection underneath, so this is
not a weaker substitute — it is the same guarantee, spelled the Rust way
(RAII instead of an explicit call).

## One capability deliberately kept on one side

Having both a builder and a direct `RociaDbClient.connect()` entry point is
TypeScript-only: the builder there is a thin wrapper with no capability of
its own, so duplicating a second entry point in Rust would add an API to
maintain for zero new capability. Rust's `RociaDbBuilder::build` and
`build_with_channel` are the only two ways in.
