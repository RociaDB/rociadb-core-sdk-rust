//! Compile-time proof that a `RociaDbClient` shared behind an `Arc` is usable
//! without a `Mutex`: every method takes `&self`, so none of the calls below
//! would fail to type-check through an `Arc`, and callers never need to
//! serialise requests.
//!
//! Nothing here runs a request — the functions are never called. They only
//! have to compile.

use rociadb_sdk::{
    DocumentQueryFilter, DocumentQueryOperator, Edge, EdgeInput, NodeInput, Result, RociaDbClient,
};
use std::sync::Arc;

#[allow(dead_code)]
async fn reads_through_an_arc(client: Arc<RociaDbClient>) -> Result<()> {
    let _: serde_json::Value = client.get_document("tenant", "products", "sku-1").await?;
    let _ = client.list_collections("tenant", Some(20), None).await?;
    let _ = client.list_graphs("tenant", None, None).await?;
    let _ = client.stat_file("tenant", "assets", "manual.txt").await?;
    let _ = client.list_tenants(None, None).await?;
    let _: Edge<serde_json::Value> = client.get_edge("tenant", "catalog", "edge-1").await?;
    let _: Edge<serde_json::Value> = client.get_edge_as("tenant", "catalog", "edge-1").await?;
    Ok(())
}

// An `Edge` read back carries exactly the five fields `add_edge` takes, so it
// must be usable as the source of a write without restating any of them.
#[allow(dead_code)]
async fn an_edge_round_trips_into_a_write(client: Arc<RociaDbClient>) -> Result<()> {
    let edge: Edge<serde_json::Value> = client.get_edge_as("tenant", "catalog", "edge-1").await?;
    client
        .add_edge(
            "tenant",
            "catalog",
            &edge.edge_id,
            &edge.from,
            &edge.to,
            &edge.label,
            &edge.value,
        )
        .await
}

#[allow(dead_code)]
async fn writes_through_an_arc(client: Arc<RociaDbClient>) -> Result<()> {
    client
        .put_document("tenant", "products", "sku-1", &serde_json::json!({"a": 1}))
        .await?;
    client
        .put_node(
            "tenant",
            "catalog",
            "product:sku-1",
            &serde_json::json!({"a": 1}),
        )
        .await?;
    client
        .delete_document("tenant", "products", "sku-1")
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
                vec![serde_json::json!(true)],
            )];
            let _: Result<rociadb_sdk::DocumentPage<serde_json::Value>> = client
                .query_documents("tenant", "products", &filters, &[], Some(50), None)
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
                NodeInput::new("product:sku-1", serde_json::json!({"sku": "sku-1"}))
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
                    serde_json::json!({"weight": 1}),
                )
                .with_request_id("stable-edge-key"),
            ],
        )
        .await?;
    Ok(())
}
