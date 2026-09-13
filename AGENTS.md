# Repository Guidelines

## Project Structure & Module Organization

This repository contains only the Rust SDK for RociaDB's gRPC services.

- `src/` — the crate. `lib.rs` holds the crate documentation, `RociaDbBuilder`,
  `RociaDbClient` and the private helpers every module shares; each service
  owns a module (`document.rs`, `graph.rs`, `file.rs`, `tenant.rs`), with
  `auth.rs` for tokens, `error.rs` for `RociaDbError`, `retry.rs` for
  `RetryPolicy`, and `pb.rs` including the generated code. `auth` is the only
  public module: everything else public is re-exported at the crate root.
- `proto/` — the canonical schemas. `build.rs` drives code generation from
  them.
- `tests/` — integration tests plus the shared harness in `tests/support/`.
- `docs/` — the reference guides the README links to. They ship in the
  published package.
- `target/` — generated; keep it out of source changes.

The Node.js/TypeScript SDK has moved to its own sibling repository
([`rociadb-core-sdk-ts`](https://github.com/RociaDB/rociadb-core-sdk-ts)) and is
no longer part of this checkout — there is no `typescript/` directory here, and
no `npm` commands apply to this repo.

## Build, Test, and Development Commands

- `mise install` installs the pinned Rust toolchain.
- `cargo build` compiles the SDK and regenerates gRPC bindings when proto
  inputs change.
- `cargo test` runs unit, integration, and documentation tests.
- `cargo fmt --all -- --check` verifies standard Rust formatting; run
  `cargo fmt --all` to apply it.
- `cargo clippy --all-targets --all-features -- -D warnings` treats lint
  warnings as failures.
- `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` checks and builds API
  documentation locally. A broken intra-doc link fails the build.
- `cargo deny check` runs the advisory, licence, ban, and source policy in
  `deny.toml`.

Run all six before submitting changes; `.github/workflows/ci.yml` runs the same
set plus an MSRV check (`cargo check --lib --all-features --locked` on Rust
1.88, which must match `rust-version` in `Cargo.toml`).

**No system dependency is needed to build.** `build.rs` compiles
`proto/upstream/v1/upstream.proto` with `protox`, a pure-Rust protobuf
compiler, so there is no `protoc` binary to install and no `PROTOC`
environment variable to set — on a developer machine, in CI, or on docs.rs.
`protox` also supplies the Google well-known types, which is why none are
vendored under `proto/`. `build.rs` runs a second codegen pass into
`$OUT_DIR/test_server/` for the integration tests' in-process server; the
library's own generated code stays client-only.

## Coding Style & Naming Conventions

The crate sets `#![forbid(unsafe_code)]` and `#![warn(missing_docs)]`. Use
rustfmt defaults and idiomatic Rust naming. Write all code, comments and
documentation in English only — do not introduce French text or `EN:`/`FR:`
prefixes. Do not edit generated output.

### API conventions

- **Exactly one method per server operation.** Optional per-call parameters
  travel in an options or input struct passed as the last argument — never in a
  `_with_request_id` / `_with_node_binding` / `_as` sibling method. Pagination
  stays positional (`limit: Option<u32>`, `cursor: Option<&str>`), and
  `tenant_id: &str` is the first argument of every tenant-scoped call.
- **Every option and input struct is `#[non_exhaustive]`**, derives
  `Debug + Clone` (plus `PartialEq`/`Eq`/`Default` where the fields allow),
  exposes `pub` fields for reading, and is built with a `new()` constructor
  plus chainable `with_*` setters taking `self`. A new field is therefore
  additive.
- **Every unary RPC goes through `RociaDbClient::unary`.** That one private
  helper wraps the request, applies the per-RPC deadline, refreshes the token
  and retries once on `UNAUTHENTICATED`, and maps a failed `tonic::Status`
  into `RociaDbError::Status`. Do not call a generated client directly from a
  new method; the only exceptions are the two streaming RPCs (`Upload`,
  `Download`), where neither a per-call deadline nor a transparent replay
  applies.
- Public methods return `rociadb_sdk::Result<T>` (an alias for
  `std::result::Result<T, RociaDbError>`, defined in `src/error.rs`) rather
  than `anyhow::Result`. Extend `RociaDbError` — or add a variant, the enum is
  `#[non_exhaustive]` — for a new fallible case instead of reaching for
  `anyhow`, and keep the boundary between its variants: `Config` for a
  configuration mistake, `Connection` for a dial or transport failure,
  `Validation` for a client-side rule on one call's arguments.
- Avoid panics and unchecked casts in public paths.

### Logging policy

This is a library. `tracing` at `debug!` for "what is about to happen" — one
line per RPC with its identifying fields — and **never** `info!` or `error!`
for routine success or failure: the caller receives the error and decides how
to log it. `warn!` is reserved for the five conditions that already use it:
`disable_auth()`, a non-`https` token URL, a failed background token refresh, a
token response with no `expires_in`, and a token refresh that failed after an
`UNAUTHENTICATED` response.

**Never log a secret.** No tokens, no `client_secret`, no `client_id`, no
`token_url`, no document, node, edge or file payloads. Secrets are held as
`secrecy::SecretString` so a formatter cannot leak them by accident; keep it
that way, and keep `RociaDbClient`'s manual `Debug` impl limited to the host
and whether auth is enabled.

## Documentation

- **Every public item needs a doc comment** — struct fields and enum variants
  included; `#![warn(missing_docs)]` enforces it and CI builds the docs with
  `-D warnings`.
- Document the server's behaviour where a caller can trip over it: idempotent
  deletes, the `(from, label, to)` edge uniqueness rule, what `total_count`
  costs, what a verified download does and does not prove. State the
  limitation rather than implying a guarantee the server does not make.
- `README.md` stays short: what the crate is, installation, a quick start, the
  conventions that apply everywhere, and links into `docs/`. Reference material
  belongs in a `docs/` guide.
- **Every code example is compiled.** `src/lib.rs` carries `#[cfg(doctest)]`
  items that `include_str!` the README and each `docs/*.md` file, so rustdoc
  collects their fenced blocks as doctests. Two consequences when editing those
  files: rustdoc treats an **untagged** fence as Rust, so every non-Rust block
  must carry its language (` ```toml `, ` ```bash `, ` ```text `); and a Rust
  example that would talk to a server is ` ```rust,no_run ` with hidden `# `
  lines supplying the async main and a built client. Adding a new `docs/*.md`
  file means adding it to the `guide_doctests` module in `src/lib.rs`.
- Examples must use only the public API.

## Testing Guidelines

- **Unit tests** go in `#[cfg(test)] mod tests` next to the code they cover.
  Keep them deterministic and independent of a live RociaDB or OAuth2 service:
  pure helpers are factored out precisely so their rules can be asserted
  without a socket, and time-dependent tests use
  `#[tokio::test(start_paused = true)]` rather than sleeping.
- **Integration tests** live in `tests/` and run against the in-process server
  and mock identity provider in `tests/support/`. Both bind `127.0.0.1:0`, so
  the suite is loopback-only, parallel-safe and needs nothing installed. The
  harness offers scripted per-RPC failures, per-RPC delays, and a recorder of
  every request with the `authorization` header it carried — use those instead
  of adding a new mocking dependency. `tests/support/mod.rs` documents the
  fidelity of the fake store and its limits: it proves things about the
  client, never about the real server.
- **Doctests** cover the README and `docs/`, as above.
- Cover success paths, invalid configuration, authentication behaviour,
  pagination, and serialization edge cases relevant to each change.

## Changelog

Every user-visible change gets an entry in `CHANGELOG.md`, under the unreleased
version's `Breaking` / `Added` / `Changed` / `Removed` / `Fixed` heading. Say
what changed and what a caller has to do about it; a breaking rename or
signature change belongs in the migration table, with its 1.x spelling on the
left and its replacement on the right. Verify that every API name an entry
mentions actually exists before committing it.

## Commit & Pull Request Guidelines

Use concise, imperative commit subjects (for example, `Add query sort support`)
and keep commits narrowly scoped, matching the style of existing history. Pull
requests should explain the behavior change, motivation, validation commands,
and any compatibility impact. Link relevant issues; include request/response
examples for API changes and note regenerated protobuf effects. Screenshots are
only useful for documentation rendering changes.

## Releasing

1. Bump `version` in `Cargo.toml` (and run `cargo check` so `Cargo.lock`
   follows).
2. Replace `Unreleased` with the release date in that version's `CHANGELOG.md`
   heading.
3. Run the six commands above; they must all pass.
4. Check the package contents with `cargo package --list`: `docs/`,
   `CHANGELOG.md`, `README.md`, `build.rs` and `proto/` must be present, and
   `.github`, `AGENTS.md`, `deny.toml` and `mise.toml` absent.
5. Merge to `main`, tag the merge commit `vX.Y.Z`, and push the tag.
6. `cargo publish`.

## Protobuf

Protobuf changes must originate in the canonical `proto/` here and be mirrored
**byte for byte** into the sibling
[`rociadb-core-sdk-ts`](https://github.com/RociaDB/rociadb-core-sdk-ts)
repository's own copy. That repository uses TypeScript strict mode, two-space
indentation, `camelCase` values, and `PascalCase` types, but none of its files
live in this checkout. A new or changed RPC also needs its parity row checked
in `docs/typescript-parity.md`.

## Security & Configuration

Authentication reads `AUTH_TOKEN_URL`, `AUTH_CLIENT_ID`, and
`AUTH_CLIENT_SECRET`. Never commit real credentials, tokens, or environment
files. Use `disable_auth()` only for controlled local or test environments.
`tenant_id` is a business-level partition, not a security boundary — see
`docs/tenancy.md`.
