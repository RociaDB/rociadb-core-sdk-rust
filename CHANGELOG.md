# Changelog

All notable changes to this crate are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - Unreleased

### Breaking

- **Minimum supported Rust version is 1.88**, stated in `Cargo.toml` as
  `rust-version` and checked in CI against the committed `Cargo.lock`. The
  floor comes from the dependency graph (tonic, tonic-prost and
  tonic-prost-build 0.14.6 all declare 1.88), not from `edition = "2024"`.
- `tokio` is now depended on with only the features the library itself needs
  (`rt`, `sync`, `time`, `macros`). `rt-multi-thread` moved to
  `[dev-dependencies]`. A consumer that relied on this crate enabling
  `rt-multi-thread` for them — through Cargo's feature unification — must now
  enable it on their own `tokio` dependency.

### Added

- English documentation on the generated types re-exported at the crate root
  (`CollectionInfo`, `StatResponse`, `Neighbor`, `UploadRequest`,
  `DownloadResponse`) and on each of their fields, describing the wire
  semantics. It is attached by the build script, so it is regenerated with the
  code rather than drifting from it.
- `#![forbid(unsafe_code)]` and `#![warn(missing_docs)]` on the crate, and doc
  comments on every public item the latter reported — the fields of `Page`,
  `DocumentPage`, `NodeBinding`, `DocumentQueryFilter`, `DocumentQuerySort`,
  `NodeInput`, `EdgeInput`, `Edge`, `NeighborNode`, `NeighborPage`,
  `FileUploadOptions`, `FileStreamUploadOptions`, `TokenResponse` and the
  `RociaDbError` variants, plus the variants of `DocumentQueryOperator` and
  `DocumentQuerySortDirection`.
- `.github/workflows/ci.yml`: formatting, Clippy, tests, a documentation build
  with `RUSTDOCFLAGS="-D warnings"`, an MSRV job on Rust 1.88, and a
  `cargo deny` job.
- `deny.toml`: RustSec advisories, a permissive-only licence allow-list,
  duplicate and wildcard bans as warnings, and crates.io as the only allowed
  source.
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
- `Cargo.toml`'s `exclude` list now also drops `.github`, `deny.toml` and
  `mise.toml` from the published package. `CHANGELOG.md` is deliberately
  included.

### Removed

- The `pbjson-types` dependency, which existed only to name
  `google.protobuf.Empty`. It pulled `pbjson`, `chrono`, `num-traits` and
  `autocfg` into every consumer's build; `cargo tree -e normal` goes from 166
  entries to 162.
- The `mise.toml` `protoc` tool entry, and every `PROTOC=` instruction in the
  crate documentation, `AGENTS.md` and `README.md`.

### Fixed

- Updated `h2` to 0.4.19 in `Cargo.lock` for RUSTSEC-2026-0258 (unbounded empty
  DATA frames, reachable through both `tonic` and `reqwest`).

## [1.0.0] - 2026-09-01

First stable release. See the
[repository history](https://github.com/RociaDB/rociadb-core-sdk-rust/commits/main)
for what it contained.
