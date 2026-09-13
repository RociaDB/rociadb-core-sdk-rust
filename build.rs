//! Build script: generates the gRPC client code from `proto/`, plus the
//! server stubs the integration tests run their in-process server on.
//!
//! The `.proto` is compiled by `protox`, a pure-Rust protobuf compiler, so
//! building this crate needs no `protoc` binary and no system include
//! directory — `cargo build` works on a bare toolchain, including on docs.rs
//! and in CI. `protox` also carries its own copy of the Google well-known
//! types (`google/protobuf/empty.proto` is the only one `upstream.proto`
//! imports), which is why none of them are vendored here.

use std::path::PathBuf;

/// Subdirectory of `OUT_DIR` the second codegen pass writes to. The
/// integration tests pick the result up with
/// `include!(concat!(env!("OUT_DIR"), "/test_server/rocia.v1.rs"))` — `OUT_DIR`
/// is set for every target of a package that has a build script, integration
/// tests included.
const TEST_SERVER_OUT_DIR: &str = "test_server";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `include_source_info(false)` strips `SourceCodeInfo` from the descriptor
    // set, and with it every comment. That is deliberate: the canonical
    // `.proto` is mirrored byte for byte from the server repository, comments
    // included, and those comments are written in French. Carried through,
    // they would become rustdoc on the generated types — and five of those
    // types are re-exported at the crate root, so the text would ship on
    // docs.rs. The English documentation attached below is the single source
    // of truth for callers; the `.proto` stays the source of truth for the
    // wire. Dropping the source info here also removes the need for the two
    // separate `disable_comments` settings prost and tonic each want.
    //
    // `include_imports(true)` makes the descriptor set self-contained, so
    // prost-build can resolve every type reference from the set alone. Today
    // the only import is `google/protobuf/empty.proto`, for which that is
    // belt-and-braces: prost knows the well-known types without a descriptor
    // and maps `.google.protobuf.Empty` to `()`, generating no Rust code for
    // the file. It stops being redundant the moment `upstream.proto` imports
    // anything prost does have to generate or resolve.
    let file_descriptor_set = protox::Compiler::new(["proto"])?
        .include_source_info(false)
        .include_imports(true)
        .open_files(["proto/upstream/v1/upstream.proto"])?
        .file_descriptor_set();

    // Pass 1 — the library's own code, at the root of `OUT_DIR`, included by
    // `src/pb.rs`. Client only: nothing in `src/` uses a server API, and
    // `tonic` itself is a regular dependency without its server feature stack,
    // so a generated `*Server` type would not even compile for a consumer.
    documented(tonic_prost_build::configure().build_server(false))
        .compile_fds(file_descriptor_set.clone())?;

    // Pass 2 — the mirror image, in `$OUT_DIR/test_server/`: server stubs and
    // no client. `tests/support/mod.rs` includes it to implement all four
    // services in-process, so the integration tests exercise the real client
    // path (builder -> channel -> interceptor -> generated client -> `unary`
    // -> decoding) against a real gRPC server over a loopback socket, with no
    // external service and no `protoc`.
    //
    // Why a second pass rather than one `build_server(true)` pass shared by
    // both: the generated server code names `tonic::transport::Server`'s
    // supporting types, which only exist when `tonic` is built with its
    // `server` feature. That feature is enabled by the `tonic` entry in
    // `[dev-dependencies]` and therefore only when tests are built — so
    // server code emitted into the library's own module would break every
    // consumer's `cargo build`. Keeping the two passes apart also keeps the
    // library's public surface exactly what it was: `src/pb.rs` includes only
    // the client file, and the message types are generated twice into two
    // separate modules rather than shared.
    //
    // What a consumer pays for a pass whose output they never compile: one
    // more prost/tonic codegen run over a single 380-line `.proto`, and ~100 KB
    // written under `target/`. The whole build script — the `protox` compile
    // and both passes — measures around 150 ms, so this is a few tens of
    // milliseconds once per `cargo build` of this crate, against a `tonic` and
    // `prost` build that takes seconds. The `.proto` itself is compiled once
    // either way: the `FileDescriptorSet` is cloned, not recompiled. Nothing
    // here reaches the network, and nothing is written outside `OUT_DIR`.
    // Nothing is *compiled* for a consumer either — the generated file is only
    // ever included by `tests/support/mod.rs`.
    let test_server_out_dir = PathBuf::from(std::env::var("OUT_DIR")?).join(TEST_SERVER_OUT_DIR);
    // prost-build writes into this directory but never creates it.
    std::fs::create_dir_all(&test_server_out_dir)?;
    documented(
        tonic_prost_build::configure()
            .build_client(false)
            .build_server(true)
            .out_dir(&test_server_out_dir),
    )
    .compile_fds(file_descriptor_set)?;

    // Emitting any rerun-if-changed line switches Cargo off its default
    // "watch the whole package" heuristic, so every path the build depends on
    // has to be listed explicitly from here on. There are only two: the single
    // `.proto` (its lone import is supplied by `protox` itself, not from the
    // tree) and this script.
    println!("cargo:rerun-if-changed=proto/upstream/v1/upstream.proto");
    println!("cargo:rerun-if-changed=build.rs");
    Ok(())
}

/// Apply the settings both codegen passes share: the serde derives every
/// message carries, and the English documentation in [`DOCS`] / [`FIELD_DOCS`].
///
/// Shared so the two passes cannot drift apart — the messages the test server
/// decodes must be the same types, with the same derives, as the ones the
/// client encodes, and `serde::Serialize` in particular is what lets
/// `tests/support` record a received request as JSON without hand-writing a
/// matcher per RPC.
fn documented(builder: tonic_prost_build::Builder) -> tonic_prost_build::Builder {
    let mut builder =
        builder.type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");
    for (path, doc) in DOCS {
        builder = builder.type_attribute(path, doc_attribute(doc));
    }
    for (path, doc) in FIELD_DOCS {
        builder = builder.field_attribute(path, doc_attribute(doc));
    }
    builder
}

/// Wrap one documentation line into the `#[doc = ".."]` attribute
/// prost-build injects verbatim above the item it is registered for.
fn doc_attribute(doc: &str) -> String {
    format!("#[doc = {doc:?}]")
}

/// Documentation for the generated messages that appear in a public method
/// signature and are therefore re-exported at the crate root. The rest of the
/// generated code is internal to the crate and left undocumented (`pb` carries
/// `#[allow(missing_docs)]`).
const DOCS: &[(&str, &str)] = &[
    (
        ".rocia.v1.CollectionInfo",
        "One document collection, as reported by \
         [`RociaDbClient::list_collections`](crate::RociaDbClient::list_collections).",
    ),
    (
        ".rocia.v1.Neighbor",
        "One graph neighbor: the node reached and the edge that reaches it. Returned by \
         [`RociaDbClient::neighbors_out`](crate::RociaDbClient::neighbors_out) and \
         [`RociaDbClient::neighbors_in`](crate::RociaDbClient::neighbors_in).",
    ),
    (
        ".rocia.v1.StatResponse",
        "Metadata recorded for one stored file, as returned by \
         [`RociaDbClient::stat_file`](crate::RociaDbClient::stat_file). Describes the published \
         version of the file: an upload still in flight is not visible here.",
    ),
    (
        ".rocia.v1.UploadRequest",
        "One message of a file-upload stream. The first message of a stream carries the file \
         metadata (`size_bytes`, `content_type`, `checksum`) and may carry a first `chunk`; every \
         later message carries only a `chunk`. Build these by hand only for \
         [`RociaDbClient::upload_file_stream`](crate::RociaDbClient::upload_file_stream), whose \
         documentation states the wire contract in full — the other upload methods build the \
         stream for you.",
    ),
    (
        ".rocia.v1.DownloadResponse",
        "One message of a file-download stream, as yielded by \
         [`RociaDbClient::download_file_stream`](crate::RociaDbClient::download_file_stream). \
         Concatenating every `chunk` in the order received reproduces the file.",
    ),
];

/// Documentation for the fields of the messages in [`DOCS`]. Registered per
/// field because prost-build applies a field attribute to exactly the path
/// given, and because each field carries its own wire semantics.
const FIELD_DOCS: &[(&str, &str)] = &[
    (
        ".rocia.v1.CollectionInfo.collection",
        "Name of the collection.",
    ),
    (
        ".rocia.v1.CollectionInfo.count",
        "Number of documents the collection holds. Read from a counter the server maintains on \
         every write, so it costs nothing to report; it describes the same instant as the page it \
         arrives with, and nothing is promised from one call to the next.",
    ),
    (
        ".rocia.v1.Neighbor.node_id",
        "Id of the neighboring node: the edge's `to` for an outgoing read, its `from` for an \
         incoming one.",
    ),
    (
        ".rocia.v1.Neighbor.edge_id",
        "Id of the edge connecting the queried node to `node_id`. Pass it to \
         [`RociaDbClient::get_edge`](crate::RociaDbClient::get_edge) to read the edge's own \
         properties.",
    ),
    (
        ".rocia.v1.StatResponse.size_bytes",
        "Total size of the stored file in bytes.",
    ),
    (
        ".rocia.v1.StatResponse.content_type",
        "MIME type recorded at upload time, exactly as the uploader declared it. The server does \
         not inspect the bytes to confirm it.",
    ),
    (
        ".rocia.v1.StatResponse.checksum",
        "SHA-256 digest recorded at upload time, as 32 raw bytes. The server stores what the \
         uploader declared without verifying it against the bytes received, so this confirms what \
         was claimed for the file, not what the file contains.",
    ),
    (
        ".rocia.v1.StatResponse.created_at",
        "Timestamp of the first upload of this `file_id`, as a string formatted by the server.",
    ),
    (
        ".rocia.v1.StatResponse.updated_at",
        "Timestamp of the most recent upload of this `file_id`, as a string formatted by the \
         server. Equal to `created_at` until the file is replaced.",
    ),
    (
        ".rocia.v1.UploadRequest.tenant_id",
        "Tenant the file belongs to. Read from the first message of the stream.",
    ),
    (
        ".rocia.v1.UploadRequest.bucket",
        "Bucket the file is stored in. Read from the first message of the stream.",
    ),
    (
        ".rocia.v1.UploadRequest.file_id",
        "Id of the file to write. Read from the first message of the stream. Replacing an existing \
         `file_id` is atomic: the upload writes its own generation and the published version is \
         swapped in one step, so a download either serves the old version whole or the new one \
         whole, and an interrupted upload never leaves a mixed one behind.",
    ),
    (
        ".rocia.v1.UploadRequest.size_bytes",
        "Exact total byte count of the file, on the first message of the stream. The server fails \
         the upload with `INVALID_ARGUMENT` if the chunks do not add up to it.",
    ),
    (
        ".rocia.v1.UploadRequest.content_type",
        "MIME type to record for the file, on the first message of the stream. Ignored on later \
         messages.",
    ),
    (
        ".rocia.v1.UploadRequest.checksum",
        "SHA-256 digest of the complete file, as exactly 32 raw bytes, on the first message of the \
         stream. The server rejects any other length with `INVALID_ARGUMENT` but never checks the \
         digest against the bytes received. Ignored on later messages.",
    ),
    (
        ".rocia.v1.UploadRequest.chunk",
        "Next slice of file bytes. At most 1 MiB (1_048_576 bytes) per message; a larger chunk is \
         rejected with `INVALID_ARGUMENT`. Any smaller size is accepted, and the slicing need not \
         be uniform.",
    ),
    (
        ".rocia.v1.UploadRequest.request_id",
        "Optional idempotency key, on the first message of the stream. A sequential replay reusing \
         the same key, operation and target is absorbed and answered Ok. The key's scope includes \
         the target, so reusing one across two files performs both uploads. Markers expire after \
         the server's `gc.request_ttl_secs` (24 hours by default).",
    ),
    (
        ".rocia.v1.DownloadResponse.chunk",
        "Next slice of file bytes. The server chooses the slicing and makes no promise about chunk \
         size; read until the stream ends.",
    ),
];
