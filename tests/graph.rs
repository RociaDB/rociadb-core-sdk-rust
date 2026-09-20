//! Graph RPCs against the in-process server: node and edge round trips, the
//! `(from, label, to)` uniqueness rule, the endpoint-existence rule,
//! idempotent edge deletes, neighbor pagination in both directions, the two
//! batch helpers, and both shapes of neighbor-node read — the paged
//! `neighbor_nodes_*` and the unbounded `get_*_neighbor_nodes`.

mod support;

use rociadb_sdk::{EdgeInput, NeighborNode, NodeInput, WriteOptions};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use support::FakeServer;

const TENANT: &str = "tenant-1";
const GRAPH: &str = "catalog";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct NodeBody {
    name: String,
    rank: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct EdgeBody {
    weight: u32,
}

/// One hub node with `count` outgoing `member` edges to `leaf:NN` nodes.
/// Edge ids are zero-padded so the server's key order — which is the
/// pagination order — matches the order they were created in.
async fn seed_star(client: &rociadb_sdk::RociaDbClient, count: usize) {
    client
        .put_node(
            TENANT,
            GRAPH,
            "hub:1",
            &NodeBody {
                name: "hub".to_string(),
                rank: 0,
            },
            WriteOptions::new(),
        )
        .await
        .expect("the hub node must be written");

    let nodes: Vec<NodeInput> = (0..count)
        .map(|index| {
            NodeInput::new(
                format!("leaf:{index:03}"),
                json!({"name": format!("leaf-{index:03}"), "rank": index}),
            )
        })
        .collect();
    client
        .put_nodes(TENANT, GRAPH, nodes)
        .await
        .expect("the leaf nodes must be written");

    let edges: Vec<EdgeInput> = (0..count)
        .map(|index| {
            EdgeInput::new(
                format!("edge-{index:03}"),
                "hub:1",
                format!("leaf:{index:03}"),
                "member",
                json!({"weight": index}),
            )
        })
        .collect();
    client
        .add_edges(TENANT, GRAPH, edges)
        .await
        .expect("the edges must be written");
}

#[tokio::test]
async fn put_node_then_get_node_round_trips_a_typed_payload() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    let node = NodeBody {
        name: "widget".to_string(),
        rank: 7,
    };

    client
        .put_node(TENANT, GRAPH, "product:sku-1", &node, WriteOptions::new())
        .await
        .expect("the node write must succeed");

    let read: NodeBody = client
        .get_node(TENANT, GRAPH, "product:sku-1")
        .await
        .expect("the node must be readable");
    assert_eq!(read, node);

    // The same node decodes into a `Value` just as well.
    let raw: Value = client
        .get_node(TENANT, GRAPH, "product:sku-1")
        .await
        .expect("the node must decode into a Value too");
    assert_eq!(raw, json!({"name": "widget", "rank": 7}));

    let error = client
        .get_node::<Value>(TENANT, GRAPH, "product:absent")
        .await
        .expect_err("an unknown node must fail");
    assert!(error.is_not_found());
    assert_eq!(error.reason(), Some("not_found"));
}

#[tokio::test]
async fn add_edge_then_get_edge_round_trips_all_five_fields_in_the_same_direction() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    for node_id in ["user:1", "user:2"] {
        client
            .put_node(TENANT, GRAPH, node_id, &json!({}), WriteOptions::new())
            .await
            .expect("the endpoint nodes must exist first");
    }

    client
        .add_edge(
            TENANT,
            GRAPH,
            EdgeInput::new(
                "edge-1",
                "user:1",
                "user:2",
                "follows",
                json!({"weight": 3}),
            ),
        )
        .await
        .expect("the edge write must succeed");

    let edge = client
        .get_edge::<EdgeBody>(TENANT, GRAPH, "edge-1")
        .await
        .expect("the edge must be readable");
    // Each field individually: `from` and `to` are same-typed, so a
    // transposition anywhere between the input struct and the response
    // mapping would still compile.
    assert_eq!(edge.edge_id, "edge-1");
    assert_eq!(edge.from, "user:1");
    assert_eq!(edge.to, "user:2");
    assert_eq!(edge.label, "follows");
    assert_eq!(edge.value, EdgeBody { weight: 3 });

    let error = client
        .get_edge::<Value>(TENANT, GRAPH, "edge-absent")
        .await
        .expect_err("an unknown edge must fail");
    assert!(error.is_not_found());
}

#[tokio::test]
async fn a_second_edge_over_a_taken_triplet_is_already_exists() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    for node_id in ["user:1", "user:2"] {
        client
            .put_node(TENANT, GRAPH, node_id, &json!({}), WriteOptions::new())
            .await
            .expect("the endpoint nodes must exist first");
    }
    client
        .add_edge(
            TENANT,
            GRAPH,
            EdgeInput::new("edge-1", "user:1", "user:2", "follows", json!({})),
        )
        .await
        .expect("the first edge must succeed");

    let error = client
        .add_edge(
            TENANT,
            GRAPH,
            EdgeInput::new("edge-2", "user:1", "user:2", "follows", json!({})),
        )
        .await
        .expect_err("a second edge over the same triplet must be refused");
    assert!(error.is_already_exists(), "got: {error}");
    assert_eq!(error.reason(), Some("already_exists"));

    // Reusing the *same* edge id over its own triplet replaces the edge's
    // properties instead of conflicting — which is what makes replaying a
    // successful `add_edge` harmless.
    client
        .add_edge(
            TENANT,
            GRAPH,
            EdgeInput::new(
                "edge-1",
                "user:1",
                "user:2",
                "follows",
                json!({"weight": 9}),
            ),
        )
        .await
        .expect("rewriting an edge over its own triplet must succeed");
    let edge = client
        .get_edge::<EdgeBody>(TENANT, GRAPH, "edge-1")
        .await
        .expect("the rewritten edge must be readable");
    assert_eq!(edge.value, EdgeBody { weight: 9 });
}

#[tokio::test]
async fn an_edge_to_a_missing_endpoint_node_is_not_found() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    client
        .put_node(TENANT, GRAPH, "user:1", &json!({}), WriteOptions::new())
        .await
        .expect("one endpoint exists");

    let error = client
        .add_edge(
            TENANT,
            GRAPH,
            EdgeInput::new("edge-1", "user:1", "user:missing", "follows", json!({})),
        )
        .await
        .expect_err("an edge to a node that does not exist must fail");
    assert!(error.is_not_found(), "got: {error}");
    assert_eq!(error.reason(), Some("not_found"));
}

#[tokio::test]
async fn delete_edge_is_idempotent() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    for node_id in ["user:1", "user:2"] {
        client
            .put_node(TENANT, GRAPH, node_id, &json!({}), WriteOptions::new())
            .await
            .expect("the endpoint nodes must exist first");
    }
    client
        .add_edge(
            TENANT,
            GRAPH,
            EdgeInput::new("edge-1", "user:1", "user:2", "follows", json!({})),
        )
        .await
        .expect("the edge write must succeed");

    for attempt in 0..3 {
        client
            .delete_edge(TENANT, GRAPH, "edge-1", WriteOptions::new())
            .await
            .unwrap_or_else(|error| panic!("delete {attempt} must succeed, got: {error}"));
    }
    client
        .delete_edge(TENANT, GRAPH, "edge-never", WriteOptions::new())
        .await
        .expect("deleting an edge that never existed must succeed");

    assert!(
        client
            .get_edge::<Value>(TENANT, GRAPH, "edge-1")
            .await
            .expect_err("the deleted edge must be gone")
            .is_not_found()
    );
}

#[tokio::test]
async fn neighbors_out_and_in_page_in_both_directions() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    seed_star(&client, 5).await;

    let first = client
        .neighbors_out(TENANT, GRAPH, "hub:1", "member", Some(2), None)
        .await
        .expect("the first page must succeed");
    assert_eq!(first.items.len(), 2);
    assert_eq!(first.items[0].node_id, "leaf:000");
    assert_eq!(first.items[0].edge_id, "edge-000");

    let second = client
        .neighbors_out(
            TENANT,
            GRAPH,
            "hub:1",
            "member",
            Some(2),
            first.next_cursor.as_deref(),
        )
        .await
        .expect("the second page must succeed");
    let third = client
        .neighbors_out(
            TENANT,
            GRAPH,
            "hub:1",
            "member",
            Some(2),
            second.next_cursor.as_deref(),
        )
        .await
        .expect("the third page must succeed");
    assert_eq!(third.items.len(), 1);
    assert!(third.next_cursor.is_none());

    let walked: Vec<String> = first
        .items
        .iter()
        .chain(&second.items)
        .chain(&third.items)
        .map(|neighbor| neighbor.node_id.clone())
        .collect();
    assert_eq!(
        walked,
        vec!["leaf:000", "leaf:001", "leaf:002", "leaf:003", "leaf:004"]
    );

    // The incoming direction walks a different index: each leaf has exactly
    // one incoming `member` edge, from the hub.
    let incoming = client
        .neighbors_in(TENANT, GRAPH, "leaf:003", "member", None, None)
        .await
        .expect("the incoming read must succeed");
    assert_eq!(incoming.items.len(), 1);
    assert_eq!(incoming.items[0].node_id, "hub:1");
    assert_eq!(incoming.items[0].edge_id, "edge-003");
    assert!(incoming.next_cursor.is_none());

    // A label nothing matches is an empty page, not an error.
    let empty = client
        .neighbors_out(TENANT, GRAPH, "hub:1", "no-such-label", None, None)
        .await
        .expect("an unmatched label must still succeed");
    assert!(empty.items.is_empty());
    assert!(empty.next_cursor.is_none());
}

#[tokio::test]
async fn put_nodes_sends_every_item_in_order_and_keeps_duplicate_ids_apart() {
    let server = FakeServer::start().await;
    let client = server.client().await;

    client
        .put_nodes(
            TENANT,
            GRAPH,
            vec![
                NodeInput::new("product:1", json!({"v": 1})).with_request_id("batch-1:a"),
                NodeInput::new("product:2", json!({"v": 2})).with_request_id("batch-1:b"),
                // A duplicate id: both are sent, in order, and the last one
                // is the one that survives.
                NodeInput::new("product:1", json!({"v": 3})).with_request_id("batch-1:c"),
            ],
        )
        .await
        .expect("the batch must succeed");

    assert_eq!(
        server.call_count("PutNode"),
        3,
        "no item may be merged away"
    );
    let for_product_1: Vec<Value> = server
        .calls_for("PutNode")
        .into_iter()
        .filter(|call| call.str_field("node_id") == "product:1")
        .map(|call| call.json_field("json"))
        .collect();
    assert_eq!(
        for_product_1,
        vec![json!({"v": 1}), json!({"v": 3})],
        "items sharing a node id must be dispatched in the caller's order"
    );

    let request_ids: Vec<String> = server
        .calls_for("PutNode")
        .into_iter()
        .filter_map(|call| call.request_id)
        .collect();
    for key in ["batch-1:a", "batch-1:b", "batch-1:c"] {
        assert!(
            request_ids.iter().any(|id| id == key),
            "every caller-supplied idempotency key must reach the wire, {key} did not: \
             {request_ids:?}"
        );
    }

    let stored: Value = client
        .get_node(TENANT, GRAPH, "product:1")
        .await
        .expect("the node must be readable");
    assert_eq!(
        stored,
        json!({"v": 3}),
        "last write wins for a duplicate id"
    );
}

#[tokio::test]
async fn add_edges_sends_every_item_and_keeps_duplicate_ids_in_order() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    client
        .put_nodes(
            TENANT,
            GRAPH,
            vec![
                NodeInput::new("a", json!({})),
                NodeInput::new("b", json!({})),
                NodeInput::new("c", json!({})),
            ],
        )
        .await
        .expect("the endpoint nodes must exist first");

    client
        .add_edges(
            TENANT,
            GRAPH,
            vec![
                EdgeInput::new("e1", "a", "b", "knows", json!({"v": 1}))
                    .with_request_id("batch-2:a"),
                EdgeInput::new("e2", "b", "c", "knows", json!({"v": 2}))
                    .with_request_id("batch-2:b"),
                // Same edge id, same triplet: a rewrite, not a conflict.
                EdgeInput::new("e1", "a", "b", "knows", json!({"v": 3}))
                    .with_request_id("batch-2:c"),
            ],
        )
        .await
        .expect("the batch must succeed");

    assert_eq!(server.call_count("AddEdge"), 3);
    let for_e1: Vec<Value> = server
        .calls_for("AddEdge")
        .into_iter()
        .filter(|call| call.str_field("edge_id") == "e1")
        .map(|call| call.json_field("json"))
        .collect();
    assert_eq!(for_e1, vec![json!({"v": 1}), json!({"v": 3})]);

    let edge = client
        .get_edge::<Value>(TENANT, GRAPH, "e1")
        .await
        .expect("the edge must be readable");
    assert_eq!(edge.value, json!({"v": 3}));
}

#[tokio::test]
async fn add_edges_stops_at_the_first_failure() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    client
        .put_nodes(TENANT, GRAPH, vec![NodeInput::new("a", json!({}))])
        .await
        .expect("one endpoint exists");

    let error = client
        .add_edges(
            TENANT,
            GRAPH,
            vec![EdgeInput::new("e1", "a", "missing", "knows", json!({}))],
        )
        .await
        .expect_err("an edge to a missing node must fail the batch");
    assert!(error.is_not_found(), "got: {error}");
}

#[tokio::test]
async fn neighbor_nodes_out_returns_one_page_with_its_payloads_decoded() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    seed_star(&client, 5).await;
    server.clear_calls();

    let page = client
        .neighbor_nodes_out::<NodeBody>(TENANT, GRAPH, "hub:1", "member", Some(2), None)
        .await
        .expect("the paged neighbor-node read must succeed");
    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].node_id, "leaf:000");
    assert_eq!(page.items[0].edge_id, "edge-000");
    assert_eq!(page.items[0].value.name, "leaf-000");
    assert_eq!(page.items[1].value.rank, 1);

    // Bounded by `limit`, not by the node's degree: one neighbors page plus
    // one GetNode per neighbor *on that page*.
    assert_eq!(server.call_count("NeighborsOut"), 1);
    assert_eq!(server.call_count("GetNode"), 2);

    let second = client
        .neighbor_nodes_out::<NodeBody>(
            TENANT,
            GRAPH,
            "hub:1",
            "member",
            Some(2),
            page.next_cursor.as_deref(),
        )
        .await
        .expect("the second page must succeed");
    let third = client
        .neighbor_nodes_out::<NodeBody>(
            TENANT,
            GRAPH,
            "hub:1",
            "member",
            Some(2),
            second.next_cursor.as_deref(),
        )
        .await
        .expect("the third page must succeed");
    assert!(third.next_cursor.is_none());

    let names: Vec<String> = page
        .items
        .iter()
        .chain(&second.items)
        .chain(&third.items)
        .map(|neighbor| neighbor.value.name.clone())
        .collect();
    assert_eq!(
        names,
        vec!["leaf-000", "leaf-001", "leaf-002", "leaf-003", "leaf-004"],
        "the fan-out must preserve the order the neighbors came back in"
    );
}

#[tokio::test]
async fn neighbor_nodes_in_returns_one_page_from_the_incoming_index() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    seed_star(&client, 3).await;

    let page = client
        .neighbor_nodes_in::<NodeBody>(TENANT, GRAPH, "leaf:001", "member", Some(10), None)
        .await
        .expect("the incoming paged read must succeed");
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].node_id, "hub:1");
    assert_eq!(page.items[0].edge_id, "edge-001");
    assert_eq!(page.items[0].value.name, "hub");
    assert!(page.next_cursor.is_none());
}

#[tokio::test]
async fn neighbor_nodes_out_fails_the_whole_page_when_one_node_read_fails() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    seed_star(&client, 3).await;
    // One of the three `GetNode` calls answers NOT_FOUND — what a neighbor
    // whose node disappeared between the two calls would look like. (The API
    // has no node delete, so it is scripted rather than provoked.) The page is
    // decoded as a unit, so the read fails instead of silently returning a
    // short page with the survivors.
    server.fail_next("GetNode", 1, tonic::Code::NotFound);

    let error = client
        .neighbor_nodes_out::<NodeBody>(TENANT, GRAPH, "hub:1", "member", Some(3), None)
        .await
        .expect_err("a neighbor node that cannot be read must fail the page");
    assert!(error.is_not_found(), "got: {error}");
    assert!(
        error.to_string().contains("failed to get neighbor node"),
        "the fan-out must name its own operation, got: {error}"
    );
}

#[tokio::test]
async fn get_outgoing_neighbor_nodes_walks_every_page() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    // More than the 50-neighbor page size the all-pages read uses, so this
    // genuinely spans several pages.
    seed_star(&client, 55).await;
    server.clear_calls();

    let neighbors: Vec<NeighborNode<NodeBody>> = client
        .get_outgoing_neighbor_nodes(TENANT, GRAPH, "hub:1", "member")
        .await
        .expect("the all-pages read must succeed");

    assert_eq!(neighbors.len(), 55);
    assert_eq!(neighbors[0].node_id, "leaf:000");
    assert_eq!(neighbors[54].node_id, "leaf:054");
    assert_eq!(neighbors[54].value.rank, 54);
    assert_eq!(
        server.call_count("NeighborsOut"),
        2,
        "55 neighbors at 50 per page is exactly two neighbor pages"
    );
    assert_eq!(server.call_count("GetNode"), 55);

    let incoming: Vec<NeighborNode<NodeBody>> = client
        .get_incoming_neighbor_nodes(TENANT, GRAPH, "leaf:010", "member")
        .await
        .expect("the incoming all-pages read must succeed");
    assert_eq!(incoming.len(), 1);
    assert_eq!(incoming[0].value.name, "hub");
}

#[tokio::test]
async fn list_graphs_and_list_nodes_page_over_what_was_written() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    seed_star(&client, 3).await;
    client
        .put_node(TENANT, "other", "n:1", &json!({}), WriteOptions::new())
        .await
        .expect("a node in a second graph must be written");

    let graphs = client
        .list_graphs(TENANT, None, None)
        .await
        .expect("listing graphs must succeed");
    assert_eq!(
        graphs.items,
        vec!["catalog".to_string(), "other".to_string()]
    );

    let nodes = client
        .list_nodes(TENANT, GRAPH, Some(2), None)
        .await
        .expect("listing nodes must succeed");
    assert_eq!(nodes.items.len(), 2);
    assert!(nodes.next_cursor.is_some());

    // An unknown graph is an empty page, never NOT_FOUND.
    let unknown = client
        .list_nodes(TENANT, "no-such-graph", None, None)
        .await
        .expect("an unknown graph must still succeed");
    assert!(unknown.items.is_empty());
    assert!(unknown.next_cursor.is_none());
}

#[tokio::test]
async fn list_tenants_reports_the_tenants_writes_registered() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    client
        .put_node(TENANT, GRAPH, "n:1", &json!({}), WriteOptions::new())
        .await
        .expect("the write must register the tenant");
    client
        .put_node("tenant-2", GRAPH, "n:1", &json!({}), WriteOptions::new())
        .await
        .expect("the write must register the second tenant");

    let tenants = client
        .list_tenants(None, None)
        .await
        .expect("listing tenants must succeed");
    assert_eq!(
        tenants.items,
        vec!["tenant-1".to_string(), "tenant-2".to_string()]
    );
}
