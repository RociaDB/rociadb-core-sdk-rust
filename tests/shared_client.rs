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

use futures::stream;
use rociadb_sdk::{
    Channel, ClientTlsConfig, DocumentPage, DocumentQueryFilter, DocumentQueryOperator,
    DocumentQuerySort, DocumentQuerySortDirection, DocumentWriteOptions, Edge, EdgeInput,
    ExposeSecret, FileStreamUploadOptions, FileUploadOptions, Neighbor, NeighborNode, NodeBinding,
    NodeInput, Page, Result, RetryPolicy, RociaDbBuilder, RociaDbClient, SecretString,
    StatResponse, UploadRequest, WriteOptions,
};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;

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
    let _: StatResponse = client.stat_file("tenant", "assets", "manual.txt").await?;
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
// `with_*` setters — the replacement for the 1.0 `_with_request_id` and
// `_with_node_binding` siblings.
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
            stream::iter(vec![vec![1u8; 8], vec![2u8; 2]]),
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
        .disable_auth()
        .build()
        .await?;

    // A channel the caller built: `build_with_channel` skips dialing and host
    // validation but keeps the auth wiring and the request deadline.
    let channel: Channel = Channel::from_static("http://127.0.0.1:50051").connect_lazy();
    let _on_a_channel = RociaDbBuilder::new()
        .request_timeout(Duration::from_secs(10))
        .disable_auth()
        .build_with_channel(channel)
        .await?;
    Ok(())
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
