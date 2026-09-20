//! Compile-time proof of two properties of the 2.0 API.
//!
//! First, that a `RociaDbClient` shared behind an `Arc` is usable without a
//! `Mutex`: every method takes `&self`, so none of the calls below would fail
//! to type-check through an `Arc`, and callers never need to serialise
//! requests. Second, that the option and input structs, the generic reads and
//! the owned builder all compose the way their documentation claims — a
//! signature change that broke any of it would fail this build.
//!
//! Nothing here runs a request — the functions are never called. They only
//! have to compile.
//!
//! One real test exists so this target does not report "0 passed" in CI output,
//! which reads like a broken target to anyone who has not opened the file.

use futures::stream;
use rociadb_sdk::{
    Bytes, Channel, ClientTlsConfig, Code, DocumentPage, DocumentQueryFilter,
    DocumentQueryOperator, DocumentQuerySort, DocumentQuerySortDirection, DocumentWriteOptions,
    Edge, EdgeInput, ExposeSecret, FileMetadata, FileStreamUploadOptions, FileTimestamp,
    FileUploadOptions, Neighbor, NeighborNode, NodeBinding, NodeInput, Page, Result, RetryPolicy,
    RociaDbBuilder, RociaDbClient, RociaDbError, SecretString, Status, UploadRequest, WriteOptions,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWrite;

/// Everything else in this file is checked by the compiler and never run. This
/// exists only so the target reports a pass rather than "0 passed": if it ran,
/// the build already proved every signature below still composes.
#[test]
fn the_public_api_still_composes() {}

/// A caller's own document type, to pin down that the generic reads decode
/// into something other than `serde_json::Value`.
#[derive(Debug, Serialize, Deserialize)]
struct Product {
    sku: String,
}

#[allow(dead_code)]
async fn reads_through_an_arc(client: Arc<RociaDbClient>) -> Result<()> {
    let _: Value = client.get_document("tenant", "products", "sku-1").await?;
    let _: DocumentPage<Value> = client
        .list_documents("tenant", "products", None, None)
        .await?;
    let _: DocumentPage<Value> = client
        .search_documents("tenant", "products", "sku", "sku-1", Some(10), None)
        .await?;
    let _ = client.list_collections("tenant", Some(20), None).await?;
    let _ = client.list_graphs("tenant", None, None).await?;
    let _ = client.list_nodes("tenant", "catalog", None, None).await?;
    let _: FileMetadata = client.stat_file("tenant", "assets", "manual.txt").await?;
    let _ = client.list_buckets("tenant", None, None).await?;
    let _ = client.list_files("tenant", "assets", None, None).await?;
    let _: Vec<u8> = client
        .download_file("tenant", "assets", "manual.txt")
        .await?;
    let _ = client.list_tenants(None, None).await?;
    Ok(())
}

// The generic reads decode into whatever the caller names, `serde_json::Value`
// included, and there is exactly one method per read: no `_as` sibling.
#[allow(dead_code)]
async fn generic_reads_pick_their_own_types(client: Arc<RociaDbClient>) -> Result<()> {
    let _: Value = client
        .get_node("tenant", "catalog", "product:sku-1")
        .await?;
    let _: Edge<Value> = client.get_edge("tenant", "catalog", "edge-1").await?;
    let _: Edge<u32> = client.get_edge("tenant", "catalog", "edge-1").await?;
    let _ = client
        .get_node::<Value>("tenant", "catalog", "product:sku-1")
        .await?;
    let _: Vec<NeighborNode<Value>> = client
        .get_outgoing_neighbor_nodes("tenant", "catalog", "product:sku-1", "belongs_to")
        .await?;
    let _: Vec<NeighborNode<Value>> = client
        .get_incoming_neighbor_nodes("tenant", "catalog", "group:featured", "belongs_to")
        .await?;
    Ok(())
}

// The paged neighbor-node reads return the same `Page<T>` every other
// paginated read does, carry the same positional `limit`/`cursor`, and decode
// into whatever the caller names — including a struct of their own.
#[allow(dead_code)]
async fn paged_neighbor_nodes_compose_with_the_shared_page_type(
    client: Arc<RociaDbClient>,
) -> Result<()> {
    let outgoing: Page<NeighborNode<Value>> = client
        .neighbor_nodes_out(
            "tenant",
            "catalog",
            "product:sku-1",
            "belongs_to",
            Some(25),
            None,
        )
        .await?;
    let _: Vec<NeighborNode<Value>> = outgoing.items;

    let mut cursor = outgoing.next_cursor;
    while let Some(next) = cursor {
        let page: Page<NeighborNode<Product>> = client
            .neighbor_nodes_in(
                "tenant",
                "catalog",
                "group:featured",
                "belongs_to",
                None,
                Some(&next),
            )
            .await?;
        let _: Vec<String> = page
            .items
            .iter()
            .map(|neighbor| neighbor.value.sku.clone())
            .collect();
        cursor = page.next_cursor;
    }
    Ok(())
}

// `download_file_verified` returns the same owned buffer `download_file` does,
// and its two integrity failures are ordinary `RociaDbError` variants a caller
// can match on by name.
#[allow(dead_code)]
async fn a_verified_download_returns_bytes_or_names_the_mismatch(
    client: Arc<RociaDbClient>,
) -> Result<Vec<u8>> {
    match client
        .download_file_verified("tenant", "assets", "manual.pdf")
        .await
    {
        Ok(bytes) => Ok(bytes),
        Err(RociaDbError::ChecksumMismatch { expected, actual }) => {
            let _: (Vec<u8>, Vec<u8>) = (expected, actual);
            Ok(Vec::new())
        }
        Err(RociaDbError::SizeMismatch { expected, actual }) => {
            let _: (u64, u64) = (expected, actual);
            Ok(Vec::new())
        }
        Err(other) => Err(other),
    }
}

// `download_file_verified_to` runs the same checks without buffering, and its
// `W: AsyncWrite + Unpin + ?Sized` bound has to accept every shape a caller
// actually holds: a concrete `tokio::fs::File`, an in-memory `Vec<u8>`, and —
// this is what `?Sized` is for — a `&mut dyn AsyncWrite` whose destination is
// chosen at run time. It hands back a byte count, never the file.
#[allow(dead_code)]
async fn a_verified_download_streams_into_any_writer(
    client: Arc<RociaDbClient>,
    mut file: tokio::fs::File,
) -> Result<()> {
    let _to_disk: u64 = client
        .download_file_verified_to("tenant", "assets", "manual.pdf", &mut file)
        .await?;

    let mut buffer: Vec<u8> = Vec::new();
    let _to_memory: u64 = client
        .download_file_verified_to("tenant", "assets", "manual.pdf", &mut buffer)
        .await?;

    let erased: &mut (dyn AsyncWrite + Unpin) = &mut buffer;
    let _to_a_trait_object: u64 = client
        .download_file_verified_to("tenant", "assets", "manual.pdf", erased)
        .await?;

    // And its integrity failures are the same two variants the buffered
    // variant reports, plus the `Io` of a writer that refused the bytes.
    let mut sink = tokio::io::sink();
    match client
        .download_file_verified_to("tenant", "assets", "manual.pdf", &mut sink)
        .await
    {
        Ok(written) => {
            let _: u64 = written;
        }
        Err(RociaDbError::SizeMismatch { expected, actual }) => {
            let _: (u64, u64) = (expected, actual);
        }
        Err(RociaDbError::ChecksumMismatch { expected, actual }) => {
            let _: (Vec<u8>, Vec<u8>) = (expected, actual);
        }
        Err(RociaDbError::Io { context, source }) => {
            let _: (&'static str, std::io::Error) = (context, source);
        }
        Err(other) => return Err(other),
    }
    Ok(())
}

// `stat_file` hands back the SDK's own `FileMetadata`, whose two timestamps are
// readable as text without a parse and convertible into a `SystemTime` with one.
// Nothing here needs `chrono` or `time`: `SystemTime` is the hand-off point, and
// both of those implement `From<SystemTime>`.
#[allow(dead_code)]
async fn file_metadata_reads_as_text_or_as_an_instant(client: Arc<RociaDbClient>) -> Result<()> {
    let metadata: FileMetadata = client.stat_file("tenant", "assets", "manual.pdf").await?;
    let _: u64 = metadata.size_bytes;
    let _: &str = metadata.content_type.as_str();
    let _: &[u8] = &metadata.checksum;

    let created: &FileTimestamp = &metadata.created_at;
    let _: &str = created.as_str();
    let _: String = created.to_string();
    let _: String = format!("{}", metadata.updated_at);

    // Both accessors are ordinary `rociadb_sdk::Result`s, and a server that
    // formatted its timestamps some other way is a `Decode` to match on rather
    // than a call that failed.
    match metadata.created_at.system_time() {
        Ok(instant) => {
            let _: std::time::SystemTime = instant;
        }
        Err(RociaDbError::Decode { context, source }) => {
            let _: (&'static str, serde_json::Error) = (context, source);
        }
        Err(other) => return Err(other),
    }
    let _: i128 = metadata.updated_at.unix_nanos()?;
    let _: bool = metadata.created_at == metadata.updated_at;
    let _: FileMetadata = metadata.clone();
    Ok(())
}

// Neighbor pagination returns the shared `Page<T>`, not a bespoke page type.
#[allow(dead_code)]
async fn neighbors_come_back_as_the_shared_page_type(client: Arc<RociaDbClient>) -> Result<()> {
    let outgoing: Page<Neighbor> = client
        .neighbors_out(
            "tenant",
            "catalog",
            "product:sku-1",
            "belongs_to",
            None,
            None,
        )
        .await?;
    let _: Vec<Neighbor> = outgoing.items;
    let _: Option<String> = outgoing.next_cursor;

    let incoming: Page<Neighbor> = client
        .neighbors_in(
            "tenant",
            "catalog",
            "group:featured",
            "belongs_to",
            Some(50),
            Some("cursor"),
        )
        .await?;
    let _: Vec<Neighbor> = incoming.items;
    Ok(())
}

// An `Edge` read back carries exactly what an `EdgeInput` needs, so it must be
// usable as the source of a write without restating any of them.
#[allow(dead_code)]
async fn an_edge_round_trips_into_a_write(client: Arc<RociaDbClient>) -> Result<()> {
    let edge: Edge<Value> = client.get_edge("tenant", "catalog", "edge-1").await?;
    client
        .add_edge(
            "tenant",
            "catalog",
            EdgeInput::new(edge.edge_id, edge.from, edge.to, edge.label, edge.value),
        )
        .await
}

#[allow(dead_code)]
async fn writes_through_an_arc(client: Arc<RociaDbClient>) -> Result<()> {
    client
        .put_document(
            "tenant",
            "products",
            "sku-1",
            &json!({"a": 1}),
            DocumentWriteOptions::new(),
        )
        .await?;
    client
        .put_node(
            "tenant",
            "catalog",
            "product:sku-1",
            &json!({"a": 1}),
            WriteOptions::new(),
        )
        .await?;
    client
        .add_edge(
            "tenant",
            "catalog",
            EdgeInput::new(
                "membership-1",
                "product:sku-1",
                "group:featured",
                "belongs_to",
                json!({"weight": 1}),
            ),
        )
        .await?;
    client
        .delete_document("tenant", "products", "sku-1", WriteOptions::new())
        .await?;
    client
        .delete_edge("tenant", "catalog", "membership-1", WriteOptions::new())
        .await?;
    client
        .delete_file("tenant", "assets", "manual.txt", WriteOptions::new())
        .await?;
    Ok(())
}

// Every write takes its options struct, built with `new()` plus chainable
// `with_*` setters — the replacement for what were formerly, in 1.0, the
// `_with_request_id` and `_with_node_binding` sibling methods.
#[allow(dead_code)]
async fn option_structs_carry_what_the_extra_methods_used_to(
    client: Arc<RociaDbClient>,
) -> Result<()> {
    client
        .put_document(
            "tenant",
            "products",
            "sku-1",
            &json!({"a": 1}),
            DocumentWriteOptions::new()
                .with_request_id("stable-document-key")
                .with_node_binding(NodeBinding::new("product", "catalog")),
        )
        .await?;
    // A non-generic `&str` payload must work too: `T: Serialize + ?Sized`.
    client
        .put_document(
            "tenant",
            "notes",
            "note-1",
            "a bare string document",
            DocumentWriteOptions::default(),
        )
        .await?;
    client
        .put_node(
            "tenant",
            "catalog",
            "product:sku-1",
            &json!({"a": 1}),
            WriteOptions::new().with_request_id("stable-node-key"),
        )
        .await?;
    client
        .delete_document(
            "tenant",
            "products",
            "sku-1",
            WriteOptions::default().with_request_id("stable-delete-key"),
        )
        .await?;
    Ok(())
}

// `upload_file` takes both ownership styles through one `impl Into<Vec<u8>>`
// parameter: an owned buffer moves, a borrowed one is copied once.
#[allow(dead_code)]
async fn uploads_accept_owned_and_borrowed_buffers(client: Arc<RociaDbClient>) -> Result<()> {
    let owned: Vec<u8> = vec![1, 2, 3];
    client
        .upload_file(
            "tenant",
            "assets",
            "owned.bin",
            owned,
            FileUploadOptions::new(),
        )
        .await?;
    client
        .upload_file(
            "tenant",
            "assets",
            "borrowed.bin",
            b"borrowed bytes".as_slice(),
            FileUploadOptions::new()
                .with_content_type("text/plain")
                .with_checksum([0u8; 32])
                .with_request_id("stable-upload-key"),
        )
        .await?;
    client
        .upload_file_chunked(
            "tenant",
            "assets",
            "streamed.bin",
            stream::iter(vec![
                Ok(Bytes::from_static(&[1u8; 8])),
                Ok(Bytes::from_static(&[2u8; 2])),
            ]),
            FileStreamUploadOptions::new(10, [0u8; 32])
                .with_content_type("application/octet-stream")
                .with_request_id("stable-chunked-key"),
        )
        .await?;
    client
        .upload_file_stream(stream::iter(vec![UploadRequest::default()]))
        .await?;
    Ok(())
}

// `upload_file_chunked`'s item type is `std::io::Result<Bytes>` precisely so a
// `tokio_util::io::ReaderStream` — the obvious way to turn a file, a socket or
// a decompressor into a chunk stream — is accepted with no adapter at all. If
// that ever stopped being true, this function would stop compiling, which is
// the whole point of the file.
#[allow(dead_code)]
async fn a_reader_stream_is_a_chunk_stream(
    client: Arc<RociaDbClient>,
    file: tokio::fs::File,
) -> Result<()> {
    client
        .upload_file_chunked(
            "tenant",
            "assets",
            "from-a-file.bin",
            tokio_util::io::ReaderStream::new(file),
            FileStreamUploadOptions::new(10, [0u8; 32]),
        )
        .await
}

// The shared client must survive being sent across tasks, which is what a
// caller actually does with an `Arc`.
#[allow(dead_code)]
fn spawns_concurrent_readers(client: Arc<RociaDbClient>) {
    for _ in 0..4 {
        let client = Arc::clone(&client);
        tokio::spawn(async move {
            let filters = [DocumentQueryFilter::new(
                "active",
                DocumentQueryOperator::Eq,
                vec![json!(true)],
            )];
            let sort = [DocumentQuerySort::new(
                "created_at",
                DocumentQuerySortDirection::Desc,
            )];
            let _: Result<DocumentPage<Value>> = client
                .query_documents("tenant", "products", &filters, &sort, Some(50), None)
                .await;
        });
    }
}

// The batch helpers take `impl IntoIterator` on `&self`. That combination is
// the one most likely to stop compiling through an `Arc`, so pin it here
// alongside the single-item calls.
#[allow(dead_code)]
async fn batches_through_an_arc(client: Arc<RociaDbClient>) -> Result<()> {
    client
        .put_nodes(
            "tenant",
            "catalog",
            vec![
                NodeInput::new("product:sku-1", json!({"sku": "sku-1"}))
                    .with_request_id("stable-node-key"),
            ],
        )
        .await?;
    client
        .add_edges(
            "tenant",
            "catalog",
            vec![
                EdgeInput::new(
                    "membership-1",
                    "product:sku-1",
                    "group:featured",
                    "belongs_to",
                    json!({"weight": 1}),
                )
                .with_request_id("stable-edge-key"),
            ],
        )
        .await?;
    Ok(())
}

// The owned builder must chain from a temporary all the way into `build()`,
// and must also survive being parked in a variable half-configured — the two
// shapes `&mut self -> &mut Self` setters could not both support.
#[allow(dead_code)]
async fn the_builder_chains_both_ways() -> Result<()> {
    let from_a_temporary = RociaDbBuilder::new()
        .host("http://127.0.0.1:50051")
        .disable_auth()
        .build()
        .await?;
    let _ = format!("{from_a_temporary:?}");

    let partial = RociaDbBuilder::new().host("http://127.0.0.1:50051");
    let configured = partial
        .connect_timeout(Duration::from_secs(5))
        .auth_client_credentials("https://example.com/token", "client-id", "client-secret");
    // `build(&self)` borrows, so one builder can produce several clients.
    let _first = configured.build().await?;
    let _second = configured.build().await?;
    Ok(())
}

// The transport hooks added in 2.0 chain like every other setter, and the two
// tonic types they need are re-exported here, so a caller configuring TLS or a
// custom channel does not need `tonic` as a direct dependency.
#[allow(dead_code)]
async fn the_builder_exposes_the_transport_hooks() -> Result<()> {
    let _tls_and_deadlines = RociaDbBuilder::new()
        .host("https://rociadb.example.com:443")
        .request_timeout(Duration::from_secs(10))
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .http2_keep_alive(Duration::from_secs(30), Duration::from_secs(5))
        // Not a transport setting: a client-side ceiling on the size of a file
        // the two ergonomic uploads will send, mirroring the server's own.
        .max_file_bytes(512 * 1024 * 1024)
        // Lifts tonic's own 4 MiB ceiling on one decoded message. Unlike the
        // endpoint settings above, this one also applies through
        // `build_with_channel`, because it lives on the generated clients rather
        // than on the `Channel`.
        .max_decoding_message_size(16 * 1024 * 1024)
        .disable_auth()
        .build()
        .await?;

    // A channel the caller built: `build_with_channel` skips dialing and host
    // validation but keeps the auth wiring and the request deadline.
    let channel: Channel = Channel::from_static("http://127.0.0.1:50051").connect_lazy();
    let _on_a_channel = RociaDbBuilder::new()
        .request_timeout(Duration::from_secs(10))
        .max_decoding_message_size(16 * 1024 * 1024)
        .disable_auth()
        .build_with_channel(channel)
        .await?;
    Ok(())
}

// `Code` and `Status` are re-exported because `RociaDbError` exposes both — a
// `Status` in a public field and a `Code` from `code()` — so branching on a gRPC
// code, which is the most common thing a caller does with an error, must not
// require `tonic` as a direct dependency.
#[allow(dead_code)]
fn an_error_can_be_matched_on_its_grpc_code_without_naming_tonic(error: RociaDbError) -> bool {
    // Every accessor the crate root has to make reachable for this to work.
    let code: Option<Code> = error.code();
    let status: Option<&Status> = error.status();
    let reason: Option<&str> = error.reason();

    if let Some(status) = status {
        let _: Code = status.code();
        let _: &str = status.message();
    }
    let _ = reason;

    matches!(
        code,
        Some(Code::Unavailable | Code::DeadlineExceeded | Code::Aborted)
    )
}

// The same through a `match` on the enum itself, since `Status` is a public
// field of one of its variants.
#[allow(dead_code)]
fn an_error_can_be_destructured_to_its_status(error: &RociaDbError) -> Option<Code> {
    match error {
        RociaDbError::Status { status, .. } => Some(status.code()),
        _ => None,
    }
}

// `SecretString` is re-exported so a caller can keep the client secret in a
// redacting, zeroizing type of its own and still hand it to the builder.
#[allow(dead_code)]
async fn a_caller_can_hold_the_client_secret_as_a_secret_string() -> Result<()> {
    let secret = SecretString::from(std::env::var("APP_CLIENT_SECRET").unwrap_or_default());
    let _client = RociaDbBuilder::new()
        .auth_client_credentials(
            "https://idp.example.com/token",
            "client-id",
            secret.expose_secret().to_string(),
        )
        .build()
        .await?;
    Ok(())
}

// `retry` takes a closure rebuilding the call on every attempt, works through
// an `Arc`, and composes with the options structs — including reusing one
// `request_id` across attempts, which is what makes a replayed write safe.
#[allow(dead_code)]
async fn retries_through_an_arc(client: Arc<RociaDbClient>) -> Result<()> {
    let policy = RetryPolicy::new()
        .with_max_attempts(5)
        .with_base_delay(Duration::from_millis(50))
        .with_max_delay(Duration::from_secs(1))
        .with_retry_unavailable(true);

    let _: Value = client
        .retry(&policy, || {
            client.get_document("tenant", "products", "sku-1")
        })
        .await?;

    let options = WriteOptions::new().with_request_id("import-7:sku-1");
    client
        .retry(&policy, || {
            let options = options.clone();
            async {
                client
                    .put_node(
                        "tenant",
                        "catalog",
                        "product:sku-1",
                        &json!({"sku": "sku-1"}),
                        options,
                    )
                    .await
            }
        })
        .await?;

    let document_options = DocumentWriteOptions::new()
        .with_request_id("import-7:doc:sku-1")
        .with_node_binding(NodeBinding::new("product", "catalog"));
    client
        .retry(&RetryPolicy::default(), || {
            let document_options = document_options.clone();
            async {
                client
                    .put_document(
                        "tenant",
                        "products",
                        "sku-1",
                        &json!({"sku": "sku-1"}),
                        document_options,
                    )
                    .await
            }
        })
        .await
}
