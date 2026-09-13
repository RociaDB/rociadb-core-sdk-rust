//! Graph node and edge operations: single-item reads and writes, batch
//! upserts with bounded concurrency, and paginated neighbor traversal.
//!
//! The types this module defines are re-exported at the crate root
//! ([`crate::NodeInput`], [`crate::EdgeInput`], [`crate::Edge`],
//! [`crate::NeighborNode`]), and the RPCs are inherent methods on
//! [`RociaDbClient`].
use crate::error::JsonResultExt;
use crate::pb::upstream::v1::{
    AddEdgeRequest, DeleteEdgeRequest, GetEdgeRequest, GetEdgeResponse, GetNodeRequest,
    ListGraphsRequest, ListNodesRequest, NeighborsInRequest, NeighborsOutRequest, PutNodeRequest,
};
use crate::{
    CONCURRENT_REQUESTS, DEFAULT_PAGE_SIZE, Neighbor, Page, Result, RociaDbClient, RociaDbError,
    WriteOptions, non_empty, page_request,
};
use futures::{StreamExt, TryStreamExt, stream};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use tracing::debug;
use uuid::Uuid;

/// Page size [`RociaDbClient::get_outgoing_neighbor_nodes`] and
/// [`RociaDbClient::get_incoming_neighbor_nodes`] request while walking
/// every page of neighbors.
///
/// Not caller-tunable: those two methods load the complete neighbor list, so
/// the page size only trades the number of round trips against the size of
/// each response and never changes the result. 50 sits between the SDK's own
/// [`crate::DEFAULT_PAGE_SIZE`] (20, tuned for a single interactive page) and
/// the server's default `limits.max_page_size` (200), which a deployment may
/// lower.
const NEIGHBOR_PAGE_SIZE: u32 = 50;

/// A graph neighbor together with its decoded node payload.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct NeighborNode<T> {
    /// Id of the edge that reaches the node.
    pub edge_id: String,
    /// Id of the neighboring node.
    pub node_id: String,
    /// The node's own properties, decoded into `T`.
    pub value: T,
}

/// One graph edge, endpoints and label included, with its properties
/// decoded into `T`.
///
/// Returned by [`RociaDbClient::get_edge`]. The five fields are exactly what
/// an [`EdgeInput`] carries, in the same orientation, so an edge read back
/// can be fed straight into [`RociaDbClient::add_edge`] without unwrapping
/// anything: `value` carries the edge's own properties, never the envelope
/// the server stores them in.
///
/// `edge_id` is echoed from the argument the read was made with — the
/// server does not repeat it in the response — while `from`, `to`, `label`
/// and `value` come from the server.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct Edge<T> {
    /// Id of the edge, echoed from the argument the read was made with.
    pub edge_id: String,
    /// Id of the node the edge starts from.
    pub from: String,
    /// Id of the node the edge points to.
    pub to: String,
    /// Type of relation the edge carries.
    pub label: String,
    /// The edge's own properties, decoded into `T`. Their JSON form is
    /// normalized on write (object keys sorted, whitespace removed), so what
    /// comes back is the JSON that was stored, not the original bytes.
    pub value: T,
}

/// One node to upsert, used by [`RociaDbClient::put_nodes`].
///
/// `node_id` is the **complete** node id (for example `"product:sku-1"`),
/// not a `(label, id)` pair for the SDK to reassemble: `label:id` remains a
/// usage convention, not something the server enforces or the SDK
/// recomposes.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct NodeInput {
    /// Complete id of the node to upsert, for example `"product:sku-1"`.
    pub node_id: String,
    /// Properties to store on the node. The server requires a JSON object
    /// here, never a scalar or an array.
    pub value: Value,
    /// Idempotency key for this item's `PutNode` call. When `None`, one is
    /// generated automatically (`put_node:<uuid>`, the same default
    /// [`RociaDbClient::put_node`] applies to a single-item write — see the
    /// [idempotency key defaults](WriteOptions#idempotency-key-defaults)).
    /// Provide it explicitly — and reuse the same value on a retry — so a
    /// batch replayed after a timeout resumes safely: the server
    /// deduplicates on `(tenant, operation, target, request_id)`, so a
    /// repeated `request_id` is recognized as the same write rather than a
    /// new one.
    pub request_id: Option<String>,
}

impl NodeInput {
    /// Upsert `value` at `node_id`, letting the SDK generate the
    /// idempotency key. Chain [`NodeInput::with_request_id`] to supply your
    /// own — which is what makes a retried batch safe to replay.
    pub fn new(node_id: impl Into<String>, value: Value) -> Self {
        Self {
            node_id: node_id.into(),
            value,
            request_id: None,
        }
    }

    /// Set the idempotency key for this item; see [`NodeInput::request_id`].
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }
}

/// One edge to upsert, taken by [`RociaDbClient::add_edge`] and
/// [`RociaDbClient::add_edges`].
///
/// `edge_id` is raw and must not be prefixed with `label`. `from` and `to`
/// are named rather than positional for a reason: they are same-typed, and a
/// transposed pair persists the edge in the reverse direction with no error
/// whenever both endpoint nodes exist.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeInput {
    /// Id of the edge to upsert. Raw: do not prefix it with `label`.
    pub edge_id: String,
    /// Id of the node the edge starts from. It must already exist, or the
    /// write fails with `NOT_FOUND`.
    pub from: String,
    /// Id of the node the edge points to. It must already exist, or the
    /// write fails with `NOT_FOUND`.
    pub to: String,
    /// Type of relation the edge carries. A `(from, label, to)` triplet names
    /// at most one edge: a second edge over a triplet another edge already
    /// holds fails with `ALREADY_EXISTS`, while reusing the same `edge_id`
    /// over its own triplet replaces the edge's properties.
    pub label: String,
    /// Properties to store on the edge.
    pub value: Value,
    /// Idempotency key for this item's `AddEdge` call. When `None`, one is
    /// generated automatically (`add_edge:<uuid>` — see the [idempotency key
    /// defaults](WriteOptions#idempotency-key-defaults)). See
    /// [`NodeInput::request_id`] for why reusing it on a retry matters.
    pub request_id: Option<String>,
}

impl EdgeInput {
    /// Upsert an edge `label` carrying `value`, running `from` -> `to`,
    /// letting the SDK generate the idempotency key. Chain
    /// [`EdgeInput::with_request_id`] to supply your own. `edge_id` is raw:
    /// do not prefix it with `label`.
    pub fn new(
        edge_id: impl Into<String>,
        from: impl Into<String>,
        to: impl Into<String>,
        label: impl Into<String>,
        value: Value,
    ) -> Self {
        Self {
            edge_id: edge_id.into(),
            from: from.into(),
            to: to.into(),
            label: label.into(),
            value,
            request_id: None,
        }
    }

    /// Set the idempotency key for this item; see [`EdgeInput::request_id`].
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }
}

#[derive(Clone, Copy)]
enum NeighborDirection {
    Outgoing,
    Incoming,
}

/// Default idempotency key for a `PutNode` call, used by both
/// [`RociaDbClient::put_node`] (when [`WriteOptions::request_id`] is unset)
/// and [`RociaDbClient::put_nodes`] (when a [`NodeInput::request_id`] is),
/// so the prefix never depends on which path produced the call.
fn default_put_node_request_id() -> String {
    format!("put_node:{}", Uuid::new_v4())
}

/// Default idempotency key for an `AddEdge` call, shared by
/// [`RociaDbClient::add_edge`] and [`RociaDbClient::add_edges`] for the same
/// reason [`default_put_node_request_id`] is shared.
fn default_add_edge_request_id() -> String {
    format!("add_edge:{}", Uuid::new_v4())
}

/// Assemble an [`Edge<T>`] from a `GetEdgeResponse`'s endpoint and label
/// fields, the `edge_id` the read was made with, and the properties already
/// decoded from its `json` payload.
///
/// Split out of [`RociaDbClient::get_edge`] purely so this mapping can be
/// unit-tested on its own, without a live connection: `from`, `to`, `label`
/// and `edge_id` are four same-typed `String`-shaped values copied straight
/// into four same-typed fields, and a future edit that swapped two of those
/// assignments would still type-check and compile clean while silently
/// reversing the direction of every edge read back through this call.
fn edge_from_response<T>(edge_id: &str, response: GetEdgeResponse, value: T) -> Edge<T> {
    Edge {
        edge_id: edge_id.to_string(),
        from: response.from,
        to: response.to,
        label: response.label,
        value,
    }
}

/// Build the `PutNodeRequest` for one [`NodeInput`], encoding its `value` as
/// JSON and defaulting an absent `request_id`. Pulled out as a pure,
/// network-free function — the same reason
/// [`crate::document::build_put_doc_request`] exists for a document write —
/// so the request's field mapping is unit-testable without a live client.
fn build_put_node_request(tenant_id: &str, graph: &str, node: NodeInput) -> Result<PutNodeRequest> {
    let json = serde_json::to_vec(&node.value).encode_context("node json")?;
    Ok(PutNodeRequest {
        tenant_id: tenant_id.to_string(),
        graph: graph.to_string(),
        node_id: node.node_id,
        json,
        request_id: node.request_id.unwrap_or_else(default_put_node_request_id),
    })
}

/// Build the ordered `PutNodeRequest` batch for [`RociaDbClient::put_nodes`],
/// one [`build_put_node_request`] per item. Item order is preserved (`nodes`
/// is consumed in the order given) and duplicate `node_id`s are not merged:
/// each [`NodeInput`] becomes exactly one `PutNodeRequest`.
fn build_put_node_requests(
    tenant_id: &str,
    graph: &str,
    nodes: Vec<NodeInput>,
) -> Result<Vec<PutNodeRequest>> {
    nodes
        .into_iter()
        .map(|node| build_put_node_request(tenant_id, graph, node))
        .collect()
}

/// Build the `AddEdgeRequest` for one [`EdgeInput`], encoding its `value` as
/// JSON and defaulting an absent `request_id`. Same rationale as
/// [`build_put_node_request`]: `from` and `to` are two same-typed `String`
/// fields carried from `edge` into the request, and this makes that mapping
/// unit-testable without a live connection.
fn build_add_edge_request(tenant_id: &str, graph: &str, edge: EdgeInput) -> Result<AddEdgeRequest> {
    let json = serde_json::to_vec(&edge.value).encode_context("edge json")?;
    Ok(AddEdgeRequest {
        tenant_id: tenant_id.to_string(),
        graph: graph.to_string(),
        edge_id: edge.edge_id,
        from: edge.from,
        to: edge.to,
        label: edge.label,
        json,
        request_id: edge.request_id.unwrap_or_else(default_add_edge_request_id),
    })
}

/// Build the ordered `AddEdgeRequest` batch for [`RociaDbClient::add_edges`].
/// Same guarantees as [`build_put_node_requests`]: order preserved,
/// duplicate `edge_id`s not merged.
fn build_add_edge_requests(
    tenant_id: &str,
    graph: &str,
    edges: Vec<EdgeInput>,
) -> Result<Vec<AddEdgeRequest>> {
    edges
        .into_iter()
        .map(|edge| build_add_edge_request(tenant_id, graph, edge))
        .collect()
}

/// Split an ordered batch into groups sharing the same `key`, preserving
/// each group's internal order (the order `items` were given in). Used by
/// [`RociaDbClient::put_nodes`] and [`RociaDbClient::add_edges`] to let
/// items with different keys (`node_id`/`edge_id`) fan out fully
/// concurrently while items sharing a key are dispatched strictly one after
/// another — see those methods' doc comments for why that matters for a
/// batch carrying a duplicate id. Which group runs before which is
/// unconstrained (a `HashMap` is used, so that order is arbitrary): only
/// the order *within* a group is a guarantee, because only that order is
/// one the caller can actually observe (items with different ids are
/// applied to unrelated server-side state, so nothing distinguishes one
/// interleaving of them from another).
fn group_preserving_order<T>(items: Vec<T>, key: impl Fn(&T) -> &str) -> Vec<Vec<T>> {
    let mut groups: HashMap<String, Vec<T>> = HashMap::new();
    for item in items {
        groups.entry(key(&item).to_string()).or_default().push(item);
    }
    groups.into_values().collect()
}

/// Decide whether neighbor pagination should keep going, given every cursor
/// already used earlier in this pagination pass (`seen_cursors`, which the
/// caller is expected to have inserted the just-used cursor into before
/// calling this) and the `next_cursor` the last page came back with.
/// Continues on any fresh cursor — including when the page that carried it
/// was empty or shorter than the requested limit, since the server can
/// legitimately hand back a short or empty page mid-listing (a stale index
/// entry pointing at a deleted node, for example) followed by more data.
/// Stops only when `next_cursor` is absent, or when it repeats *any* cursor
/// already used in this pass, not just the one used immediately before it.
///
/// That broader check is what lets this catch a multi-step cycle
/// (`A -> B -> A -> B -> ...`) and not just an immediate repeat (`A -> A`):
/// a two-cursor loop would otherwise pass the narrower "does it match the
/// previous cursor" test forever, since the previous cursor is never the
/// one it repeats. `seen_cursors` grows by at most one entry per page
/// fetched, so its size is bounded by however many *distinct* cursors a
/// listing legitimately produces — the same order of magnitude as the
/// neighbors already being accumulated in `get_neighbor_nodes`'s `Vec`, and
/// nowhere near enough to be worth trading away for a cheaper but lossier
/// fixed-size window. A cursor that never repeats never stops the
/// pagination, no matter how many pages that takes, so — unlike a hard
/// page-count cap — this can't mistake a long but genuine listing for a
/// runaway loop and truncate it early.
fn next_pagination_cursor(
    seen_cursors: &HashSet<String>,
    next_cursor: Option<String>,
) -> Option<String> {
    match next_cursor {
        Some(next_cursor) if !seen_cursors.contains(&next_cursor) => Some(next_cursor),
        _ => None,
    }
}

impl RociaDbClient {
    /// Fetch one node and decode its JSON payload into `T`.
    ///
    /// `node_id` is the complete node id, `label:id` by convention (for
    /// example `"product:sku-1"`). Pass `serde_json::Value` as `T` —
    /// `get_node::<Value>(..)` — when the shape is not known up front. An
    /// unknown `node_id` is `NOT_FOUND`.
    pub async fn get_node<T: DeserializeOwned>(
        &self,
        tenant_id: &str,
        graph: &str,
        node_id: &str,
    ) -> Result<T> {
        debug!(
            tenant_id = tenant_id,
            graph = graph,
            node_id = node_id,
            "loading graph node"
        );
        let request = GetNodeRequest {
            tenant_id: tenant_id.to_string(),
            graph: graph.to_string(),
            node_id: node_id.to_string(),
        };
        let response = self
            .unary("failed to load node", request, |request| {
                let mut upstream = self.upstream_graph.clone();
                async move { upstream.get_node(request).await }
            })
            .await?;
        serde_json::from_slice(&response.json).decode_context("node json")
    }

    /// Fetch one edge by id and decode its properties into `T`.
    ///
    /// This is the only read that returns an edge's properties:
    /// [`RociaDbClient::neighbors_out`] and
    /// [`RociaDbClient::neighbors_in`] name the neighbor and the edge id
    /// only, so the JSON carried on a relation — a weight, a status, a date
    /// — has no other way back.
    ///
    /// The server returns `NOT_FOUND` for an unknown `edge_id`, just as
    /// [`RociaDbClient::get_node`] does for a missing node. An edge id is
    /// scoped to one `(tenant_id, graph)` pair: the same id under another
    /// graph, or for another tenant, is a different edge and therefore
    /// absent.
    ///
    /// The properties come back in the shape they were written in, not as
    /// the bytes originally sent: the server normalizes JSON on write
    /// (object keys sorted, whitespace stripped).
    ///
    /// Unlike [`RociaDbClient::get_node`], this read is not served from a
    /// server-side cache — every call reaches storage.
    pub async fn get_edge<T: DeserializeOwned>(
        &self,
        tenant_id: &str,
        graph: &str,
        edge_id: &str,
    ) -> Result<Edge<T>> {
        debug!(
            tenant_id = tenant_id,
            graph = graph,
            edge_id = edge_id,
            "loading graph edge"
        );
        let request = GetEdgeRequest {
            tenant_id: tenant_id.to_string(),
            graph: graph.to_string(),
            edge_id: edge_id.to_string(),
        };
        let response = self
            .unary("failed to load edge", request, |request| {
                let mut upstream = self.upstream_graph.clone();
                async move { upstream.get_edge(request).await }
            })
            .await?;
        let value = serde_json::from_slice::<T>(&response.json).decode_context("edge json")?;
        Ok(edge_from_response(edge_id, response, value))
    }

    /// Create or replace one node, using its complete node id (for example
    /// `"product:sku-1"`).
    ///
    /// Like a document write, this replaces the node's properties outright
    /// rather than merging them. See the [idempotency key
    /// defaults](WriteOptions#idempotency-key-defaults) for the key used
    /// when [`WriteOptions::request_id`] is unset.
    pub async fn put_node<T: Serialize + ?Sized>(
        &self,
        tenant_id: &str,
        graph: &str,
        node_id: &str,
        value: &T,
        options: WriteOptions,
    ) -> Result<()> {
        debug!(
            tenant_id = tenant_id,
            graph = graph,
            node_id = node_id,
            "upserting graph node"
        );
        let request = PutNodeRequest {
            tenant_id: tenant_id.to_string(),
            graph: graph.to_string(),
            node_id: node_id.to_string(),
            json: serde_json::to_vec(value).encode_context("node json")?,
            request_id: options
                .request_id
                .unwrap_or_else(default_put_node_request_id),
        };
        self.unary("failed to upsert node", request, |request| {
            let mut upstream = self.upstream_graph.clone();
            async move { upstream.put_node(request).await }
        })
        .await?;
        Ok(())
    }

    /// Upsert a batch of nodes in a graph with bounded concurrency (at most
    /// 10 `PutNode` calls in flight at once). `nodes` is consumed in the
    /// order the caller provides — duplicate `node_id`s are **not** merged,
    /// both are sent. Items with different `node_id`s run fully
    /// concurrently against each other (up to the 10-in-flight bound), but
    /// items sharing a `node_id` are dispatched strictly one after another,
    /// in the order given: the next `PutNode` call for that id is not sent
    /// until the previous one's response comes back. That makes
    /// last-item-wins genuinely true for a duplicate id — the caller's last
    /// write really is the last one the server applies — rather than a race
    /// between two in-flight calls whose completion order the network, not
    /// the caller, decides.
    ///
    /// **This batch is not atomic and stops at the first error**: on
    /// failure, in-flight requests are cancelled and the error does not say
    /// which items had already succeeded. To resume after a failure, replay
    /// the same `nodes` sequence with the same [`NodeInput::request_id`]
    /// values you used the first time — the server deduplicates on
    /// `(tenant, operation, target, request_id)`, so already-applied writes
    /// are recognized and skipped rather than reapplied, and only the
    /// writes that never landed actually happen.
    pub async fn put_nodes(
        &self,
        tenant_id: &str,
        graph: &str,
        nodes: impl IntoIterator<Item = NodeInput>,
    ) -> Result<()> {
        let requests = build_put_node_requests(tenant_id, graph, nodes.into_iter().collect())?;
        debug!(
            tenant_id = tenant_id,
            graph = graph,
            node_count = requests.len(),
            "upserting graph nodes batch"
        );
        let groups = group_preserving_order(requests, |request| request.node_id.as_str());
        stream::iter(groups.into_iter().map(Ok::<_, RociaDbError>))
            .try_for_each_concurrent(CONCURRENT_REQUESTS, |group| async move {
                for node in group {
                    self.unary("failed to upsert node", node, |request| {
                        let mut upstream = self.upstream_graph.clone();
                        async move { upstream.put_node(request).await }
                    })
                    .await?;
                }
                Ok(())
            })
            .await
    }

    /// Create one edge, or replace the one `edge.edge_id` already names.
    ///
    /// The server returns `NOT_FOUND` if `edge.from` or `edge.to` does not
    /// already exist as a node in `graph`: create both endpoint nodes before
    /// adding an edge between them.
    ///
    /// **A `(from, label, to)` triplet names at most one edge.** The
    /// adjacency index is keyed by that triplet and does not carry the edge
    /// id, so adding a *second* edge over a triplet another edge already
    /// holds fails with `ALREADY_EXISTS`. Reusing the **same** `edge_id`
    /// over its own triplet stays allowed and replaces its properties —
    /// which is what makes replaying a successful `add_edge` harmless. A
    /// known `edge_id` may also change endpoints or label: its former
    /// adjacency is retracted in the same transaction.
    ///
    /// See the [idempotency key
    /// defaults](WriteOptions#idempotency-key-defaults) for the key used
    /// when [`EdgeInput::request_id`] is unset.
    pub async fn add_edge(&self, tenant_id: &str, graph: &str, edge: EdgeInput) -> Result<()> {
        let request = build_add_edge_request(tenant_id, graph, edge)?;
        debug!(
            tenant_id = tenant_id,
            graph = graph,
            edge_id = %request.edge_id,
            from = %request.from,
            to = %request.to,
            label = %request.label,
            "upserting graph edge"
        );
        self.unary("failed to add edge", request, |request| {
            let mut upstream = self.upstream_graph.clone();
            async move { upstream.add_edge(request).await }
        })
        .await?;
        Ok(())
    }

    /// Upsert a batch of edges with bounded concurrency (at most 10
    /// `AddEdge` calls in flight at once). `edges` is consumed in the order
    /// the caller provides — duplicate `edge_id`s are **not** merged, both
    /// are sent. Items with different `edge_id`s run fully concurrently
    /// against each other (up to the 10-in-flight bound), but items sharing
    /// an `edge_id` are dispatched strictly one after another, in the order
    /// given: the next `AddEdge` call for that id is not sent until the
    /// previous one's response comes back. That makes last-item-wins
    /// genuinely true for a duplicate id — the caller's last write really is
    /// the last one the server applies — rather than a race between two
    /// in-flight calls whose completion order the network, not the caller,
    /// decides.
    ///
    /// The server returns `NOT_FOUND` for any edge whose `from` or `to`
    /// node does not already exist in `graph`: create both endpoint
    /// nodes before adding an edge between them. It returns
    /// `ALREADY_EXISTS` for any edge that would be a *second* one over a
    /// `(from, label, to)` triplet another edge already holds — see
    /// [`RociaDbClient::add_edge`].
    ///
    /// **This batch is not atomic and stops at the first error**: on
    /// failure, in-flight requests are cancelled and the error does not say
    /// which items had already succeeded. To resume after a failure, replay
    /// the same `edges` sequence with the same [`EdgeInput::request_id`]
    /// values you used the first time — the server deduplicates on
    /// `(tenant, operation, target, request_id)`, so already-applied writes
    /// are recognized and skipped rather than reapplied, and only the
    /// writes that never landed actually happen.
    pub async fn add_edges(
        &self,
        tenant_id: &str,
        graph: &str,
        edges: impl IntoIterator<Item = EdgeInput>,
    ) -> Result<()> {
        let requests = build_add_edge_requests(tenant_id, graph, edges.into_iter().collect())?;
        debug!(
            tenant_id = tenant_id,
            graph = graph,
            edge_count = requests.len(),
            "upserting graph edges batch"
        );
        let groups = group_preserving_order(requests, |request| request.edge_id.as_str());
        stream::iter(groups.into_iter().map(Ok::<_, RociaDbError>))
            .try_for_each_concurrent(CONCURRENT_REQUESTS, |group| async move {
                for edge in group {
                    self.unary("failed to add edge", edge, |request| {
                        let mut upstream = self.upstream_graph.clone();
                        async move { upstream.add_edge(request).await }
                    })
                    .await?;
                }
                Ok(())
            })
            .await
    }

    /// Delete one edge by id.
    ///
    /// **Idempotent**: deleting an `edge_id` that does not exist succeeds
    /// rather than returning `NOT_FOUND`, exactly like
    /// [`RociaDbClient::delete_document`] and
    /// [`RociaDbClient::delete_file`]. The cost of that is real — a caller
    /// that got the `edge_id` wrong is not told so. Read the edge first,
    /// with [`RociaDbClient::get_edge`], [`RociaDbClient::neighbors_out`] or
    /// [`RociaDbClient::neighbors_in`], when you need to know whether it
    /// existed.
    ///
    /// See the [idempotency key
    /// defaults](WriteOptions#idempotency-key-defaults) for the key used
    /// when [`WriteOptions::request_id`] is unset.
    pub async fn delete_edge(
        &self,
        tenant_id: &str,
        graph: &str,
        edge_id: &str,
        options: WriteOptions,
    ) -> Result<()> {
        debug!(
            tenant_id = tenant_id,
            graph = graph,
            edge_id = edge_id,
            "deleting graph edge"
        );
        let request = DeleteEdgeRequest {
            tenant_id: tenant_id.to_string(),
            graph: graph.to_string(),
            edge_id: edge_id.to_string(),
            request_id: options
                .request_id
                .unwrap_or_else(|| format!("delete_edge:{}", Uuid::new_v4())),
        };
        self.unary("failed to delete edge", request, |request| {
            let mut upstream = self.upstream_graph.clone();
            async move { upstream.delete_edge(request).await }
        })
        .await?;
        Ok(())
    }

    /// Return one paginated page of outgoing neighbors.
    ///
    /// The returned cursor is scoped to the exact `(tenant_id, graph, from,
    /// label, direction)` it was issued for, direction included:
    /// [`RociaDbClient::neighbors_in`] walks a distinct index and never
    /// accepts a cursor from here. Replaying it after changing `from`,
    /// `label`, or swapping in `neighbors_in` is rejected with
    /// `INVALID_ARGUMENT` rather than silently served against the new
    /// combination — restart the traversal with `cursor = None` instead.
    pub async fn neighbors_out(
        &self,
        tenant_id: &str,
        graph: &str,
        from: &str,
        label: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<Page<Neighbor>> {
        debug!(
            tenant_id = tenant_id,
            graph = graph,
            from = from,
            label = label,
            limit = limit.unwrap_or(DEFAULT_PAGE_SIZE),
            cursor = cursor.unwrap_or(""),
            "listing outgoing neighbors"
        );
        let request = NeighborsOutRequest {
            tenant_id: tenant_id.to_string(),
            graph: graph.to_string(),
            from: from.to_string(),
            label: label.to_string(),
            page: page_request(limit, cursor)?,
        };
        let response = self
            .unary("failed to get outgoing neighbors", request, |request| {
                let mut upstream = self.upstream_graph.clone();
                async move { upstream.neighbors_out(request).await }
            })
            .await?;
        Ok(Page {
            items: response.neighbors,
            next_cursor: response.page.and_then(|page| non_empty(page.next_cursor)),
        })
    }

    /// Return one paginated page of incoming neighbors.
    ///
    /// The returned cursor is scoped to the exact `(tenant_id, graph, to,
    /// label, direction)` it was issued for, direction included:
    /// [`RociaDbClient::neighbors_out`] walks a distinct index and never
    /// accepts a cursor from here. Replaying it after changing `to`,
    /// `label`, or swapping in `neighbors_out` is rejected with
    /// `INVALID_ARGUMENT` rather than silently served against the new
    /// combination — restart the traversal with `cursor = None` instead.
    pub async fn neighbors_in(
        &self,
        tenant_id: &str,
        graph: &str,
        to: &str,
        label: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<Page<Neighbor>> {
        debug!(
            tenant_id = tenant_id,
            graph = graph,
            to = to,
            label = label,
            limit = limit.unwrap_or(DEFAULT_PAGE_SIZE),
            cursor = cursor.unwrap_or(""),
            "listing incoming neighbors"
        );
        let request = NeighborsInRequest {
            tenant_id: tenant_id.to_string(),
            graph: graph.to_string(),
            to: to.to_string(),
            label: label.to_string(),
            page: page_request(limit, cursor)?,
        };
        let response = self
            .unary("failed to get incoming neighbors", request, |request| {
                let mut upstream = self.upstream_graph.clone();
                async move { upstream.neighbors_in(request).await }
            })
            .await?;
        Ok(Page {
            items: response.neighbors,
            next_cursor: response.page.and_then(|page| non_empty(page.next_cursor)),
        })
    }

    /// Return one paginated page of graph names holding at least one node.
    pub async fn list_graphs(
        &self,
        tenant_id: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<Page<String>> {
        debug!(
            tenant_id = tenant_id,
            limit = limit.unwrap_or(DEFAULT_PAGE_SIZE),
            cursor = cursor.unwrap_or(""),
            "listing graphs"
        );
        let request = ListGraphsRequest {
            tenant_id: tenant_id.to_string(),
            page: page_request(limit, cursor)?,
        };
        let response = self
            .unary("failed to list graphs", request, |request| {
                let mut upstream = self.upstream_graph.clone();
                async move { upstream.list_graphs(request).await }
            })
            .await?;
        Ok(Page {
            items: response.graphs,
            next_cursor: response.page.and_then(|page| non_empty(page.next_cursor)),
        })
    }

    /// Return one paginated page of node ids stored in one graph.
    ///
    /// An unknown `graph` name, or one that belongs to another tenant,
    /// returns `Ok` with an empty [`Page<T>`] and no cursor — never an error.
    /// A graph has no independent existence in the server's data model: it
    /// comes into being the moment its first node is written by
    /// [`RociaDbClient::put_node`], and there is no separate record of it
    /// for a lookup to have failed against. That makes "this graph does not
    /// exist" and "this graph exists but is empty" the same observable
    /// state from here. A caller that needs to tell the two apart cannot do
    /// so from this call alone: check the name against
    /// [`RociaDbClient::list_graphs`], or write a node to it, instead.
    pub async fn list_nodes(
        &self,
        tenant_id: &str,
        graph: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<Page<String>> {
        debug!(
            tenant_id = tenant_id,
            graph = graph,
            limit = limit.unwrap_or(DEFAULT_PAGE_SIZE),
            cursor = cursor.unwrap_or(""),
            "listing graph nodes"
        );
        let request = ListNodesRequest {
            tenant_id: tenant_id.to_string(),
            graph: graph.to_string(),
            page: page_request(limit, cursor)?,
        };
        let response = self
            .unary("failed to list nodes", request, |request| {
                let mut upstream = self.upstream_graph.clone();
                async move { upstream.list_nodes(request).await }
            })
            .await?;
        Ok(Page {
            items: response.node_ids,
            next_cursor: response.page.and_then(|page| non_empty(page.next_cursor)),
        })
    }

    /// Load **every** outgoing neighbor of `node_id` over `label` and decode
    /// each neighbor's node payload into `T`.
    ///
    /// Unbounded by design: this walks every page of
    /// [`RociaDbClient::neighbors_out`] (50 neighbors per page, not
    /// caller-tunable) and then issues one `GetNode` per neighbor, at most
    /// 10 in flight at a time, collecting the lot in memory. Cost and
    /// memory therefore scale with the node's degree, and nothing here caps
    /// either — reach for [`RociaDbClient::neighbors_out`] directly, one
    /// page at a time, for a node whose degree you do not control.
    pub async fn get_outgoing_neighbor_nodes<T: DeserializeOwned>(
        &self,
        tenant_id: &str,
        graph: &str,
        node_id: &str,
        label: &str,
    ) -> Result<Vec<NeighborNode<T>>> {
        self.get_neighbor_nodes(
            tenant_id,
            graph,
            node_id,
            label,
            NeighborDirection::Outgoing,
        )
        .await
    }

    /// Load **every** incoming neighbor of `node_id` over `label` and decode
    /// each neighbor's node payload into `T`.
    ///
    /// The incoming counterpart of
    /// [`RociaDbClient::get_outgoing_neighbor_nodes`], with the same
    /// unbounded cost: see it for what that means for a high-degree node.
    pub async fn get_incoming_neighbor_nodes<T: DeserializeOwned>(
        &self,
        tenant_id: &str,
        graph: &str,
        node_id: &str,
        label: &str,
    ) -> Result<Vec<NeighborNode<T>>> {
        self.get_neighbor_nodes(
            tenant_id,
            graph,
            node_id,
            label,
            NeighborDirection::Incoming,
        )
        .await
    }

    // Paginates via `next_pagination_cursor`: see its doc for why an empty
    // or short page never stops the loop on its own, and why `seen_cursors`
    // — rather than just the one cursor used most recently — is what lets a
    // multi-step cycle be caught.
    async fn get_neighbor_nodes<T: DeserializeOwned>(
        &self,
        tenant_id: &str,
        graph: &str,
        node_id: &str,
        label: &str,
        direction: NeighborDirection,
    ) -> Result<Vec<NeighborNode<T>>> {
        let mut cursor: Option<String> = None;
        let mut seen_cursors = HashSet::new();
        let mut neighbors = Vec::new();
        loop {
            if let Some(current) = cursor.as_deref() {
                seen_cursors.insert(current.to_string());
            }
            let page = match direction {
                NeighborDirection::Outgoing => {
                    self.neighbors_out(
                        tenant_id,
                        graph,
                        node_id,
                        label,
                        Some(NEIGHBOR_PAGE_SIZE),
                        cursor.as_deref(),
                    )
                    .await?
                }
                NeighborDirection::Incoming => {
                    self.neighbors_in(
                        tenant_id,
                        graph,
                        node_id,
                        label,
                        Some(NEIGHBOR_PAGE_SIZE),
                        cursor.as_deref(),
                    )
                    .await?
                }
            };
            neighbors.extend(page.items);
            match next_pagination_cursor(&seen_cursors, page.next_cursor) {
                Some(next_cursor) => cursor = Some(next_cursor),
                None => break,
            }
        }

        debug!(
            tenant_id = tenant_id,
            graph = graph,
            node_id = node_id,
            label = label,
            neighbor_count = neighbors.len(),
            "loading neighbor nodes"
        );
        let tenant_id = tenant_id.to_string();
        let graph = graph.to_string();
        stream::iter(neighbors)
            .map(|neighbor| {
                let request = GetNodeRequest {
                    tenant_id: tenant_id.clone(),
                    graph: graph.clone(),
                    node_id: neighbor.node_id.clone(),
                };
                async move {
                    let response = self
                        .unary("failed to get neighbor node", request, |request| {
                            let mut upstream = self.upstream_graph.clone();
                            async move { upstream.get_node(request).await }
                        })
                        .await?;
                    let value = serde_json::from_slice(&response.json)
                        .decode_context("neighbor node json")?;
                    Ok(NeighborNode {
                        edge_id: neighbor.edge_id,
                        node_id: neighbor.node_id,
                        value,
                    })
                }
            })
            .buffered(CONCURRENT_REQUESTS)
            .try_collect()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::{
        EdgeInput, NodeInput, build_add_edge_request, build_add_edge_requests,
        build_put_node_requests, edge_from_response, group_preserving_order,
        next_pagination_cursor,
    };
    use crate::pb::upstream::v1::GetEdgeResponse;
    use crate::{RociaDbError, non_empty, page_request};
    use serde_json::json;
    use std::collections::HashSet;

    fn seen(cursors: &[&str]) -> HashSet<String> {
        cursors.iter().map(|cursor| cursor.to_string()).collect()
    }

    #[test]
    fn pagination_uses_defaults_and_hides_empty_cursor() {
        let page = page_request(None, None)
            .expect("page request should not fail")
            .expect("page should be present");
        assert_eq!(page.limit, Some(20));
        assert!(page.cursor.is_empty());
        assert_eq!(non_empty(String::new()), None);
        assert_eq!(non_empty("next".into()).as_deref(), Some("next"));
    }

    #[test]
    fn zero_limit_is_rejected() {
        let error = page_request(Some(0), None).expect_err("limit 0 should be rejected");
        assert!(matches!(error, RociaDbError::Validation(_)));
        assert!(error.to_string().contains("greater than zero"));
    }

    #[test]
    fn pagination_stops_when_next_cursor_is_absent() {
        assert_eq!(next_pagination_cursor(&seen(&[]), None), None);
        assert_eq!(next_pagination_cursor(&seen(&["cursor-1"]), None), None);
    }

    #[test]
    fn pagination_continues_on_empty_page_with_a_fresh_cursor() {
        assert_eq!(
            next_pagination_cursor(&seen(&[]), Some("cursor-1".to_string())),
            Some("cursor-1".to_string())
        );
        assert_eq!(
            next_pagination_cursor(&seen(&["cursor-1"]), Some("cursor-2".to_string())),
            Some("cursor-2".to_string())
        );
    }

    #[test]
    fn pagination_stops_on_an_immediately_repeated_cursor() {
        assert_eq!(
            next_pagination_cursor(&seen(&["cursor-1"]), Some("cursor-1".to_string())),
            None
        );
    }

    #[test]
    fn pagination_stops_on_a_cycle_longer_than_one_step() {
        // A -> B -> A: by the time "cursor-b" is the one just used,
        // "cursor-a" is already in `seen_cursors` from an earlier step, not
        // the one immediately before it. A guard that only compared against
        // the single previous cursor would miss this and loop forever; this
        // is exactly the multi-step cycle the fix is for.
        assert_eq!(
            next_pagination_cursor(
                &seen(&["cursor-a", "cursor-b"]),
                Some("cursor-a".to_string())
            ),
            None
        );
    }

    #[test]
    fn pagination_continues_past_many_distinct_cursors() {
        // A long but genuine listing must never be mistaken for a cycle:
        // as long as every cursor is new, pagination keeps going no matter
        // how many pages have already been seen.
        let already_seen = seen(&["cursor-1", "cursor-2", "cursor-3"]);
        assert_eq!(
            next_pagination_cursor(&already_seen, Some("cursor-4".to_string())),
            Some("cursor-4".to_string())
        );
    }

    #[test]
    fn edge_from_response_does_not_swap_from_and_to() {
        let response = GetEdgeResponse {
            from: "user:1".to_string(),
            to: "user:2".to_string(),
            label: "follows".to_string(),
            json: Vec::new(),
        };

        let edge = edge_from_response("edge-1", response, 42u32);

        // Asserting each field individually (rather than just that the call
        // compiles) is the point: `from` and `to` are both `String`, so a
        // future edit that swapped their assignments in
        // `edge_from_response` would still type-check and pass a test that
        // only checked shape, not value.
        assert_eq!(edge.edge_id, "edge-1");
        assert_eq!(edge.from, "user:1");
        assert_eq!(edge.to, "user:2");
        assert_eq!(edge.label, "follows");
        assert_eq!(edge.value, 42);
    }

    #[test]
    fn build_add_edge_request_does_not_swap_from_and_to() {
        let edge = EdgeInput::new(
            "edge-1",
            "user:1",
            "user:2",
            "follows",
            json!({"since": 2024}),
        );

        let request = build_add_edge_request("tenant-a", "social", edge)
            .expect("building the request should not fail");

        // Same rationale as `edge_from_response_does_not_swap_from_and_to`:
        // `from` and `to` are both `String` fields carried from `EdgeInput`
        // into `AddEdgeRequest`, so a future edit that swapped their
        // assignments in `build_add_edge_request` would still type-check
        // and pass a test that only checked shape, not value.
        assert_eq!(request.tenant_id, "tenant-a");
        assert_eq!(request.graph, "social");
        assert_eq!(request.edge_id, "edge-1");
        assert_eq!(request.from, "user:1");
        assert_eq!(request.to, "user:2");
        assert_eq!(request.label, "follows");
        assert_eq!(
            request.json,
            serde_json::to_vec(&json!({"since": 2024}))
                .expect("encoding a plain JSON object should not fail")
        );
    }

    #[test]
    fn build_add_edge_request_keeps_an_explicit_request_id() {
        let edge = EdgeInput::new("edge-1", "user:1", "user:2", "follows", json!(null))
            .with_request_id("retry-1");

        let request = build_add_edge_request("tenant-a", "social", edge)
            .expect("building the request should not fail");

        assert_eq!(request.request_id, "retry-1");
    }

    // `build_put_node_requests` / `build_add_edge_requests` are the pure,
    // network-free cores of `RociaDbClient::put_nodes` /
    // `RociaDbClient::add_edges` (see their doc comments). These tests lock
    // in the three properties an ordered `Vec<NodeInput>` / `Vec<EdgeInput>`
    // batch input must have: caller order is preserved, duplicate keys are
    // not merged, and each item gets its own idempotency key.

    #[test]
    fn put_node_requests_preserve_caller_order() {
        // A `HashMap`-keyed batch input could not guarantee this —
        // iteration order over a hash map is unspecified, so it could
        // silently reorder `PutNode` calls relative to what the caller
        // wrote.
        let nodes = vec![
            NodeInput::new("product:3", json!({"n": 3})),
            NodeInput::new("product:1", json!({"n": 1})),
            NodeInput::new("product:2", json!({"n": 2})),
        ];
        let requests =
            build_put_node_requests("tenant", "catalog", nodes).expect("build must succeed");
        let ids: Vec<&str> = requests.iter().map(|r| r.node_id.as_str()).collect();
        assert_eq!(ids, vec!["product:3", "product:1", "product:2"]);
    }

    #[test]
    fn put_node_requests_do_not_merge_duplicate_node_ids() {
        let nodes = vec![
            NodeInput::new("product:1", json!({"n": 1})),
            NodeInput::new("product:1", json!({"n": 2})),
        ];
        let requests =
            build_put_node_requests("tenant", "catalog", nodes).expect("build must succeed");
        assert_eq!(
            requests.len(),
            2,
            "a HashMap keyed by node_id would have collapsed this to one request"
        );
        assert_eq!(requests[0].node_id, "product:1");
        assert_eq!(requests[1].node_id, "product:1");
        assert_ne!(
            requests[0].json, requests[1].json,
            "each duplicate keeps its own payload"
        );
    }

    #[test]
    fn put_node_requests_use_node_id_verbatim_with_no_label_recomposition() {
        let nodes = vec![NodeInput::new("product:sku-1", json!({}))];
        let requests =
            build_put_node_requests("tenant", "catalog", nodes).expect("build must succeed");
        assert_eq!(requests[0].node_id, "product:sku-1");
    }

    #[test]
    fn put_node_requests_pass_through_caller_supplied_request_id() {
        let nodes =
            vec![NodeInput::new("product:1", json!({})).with_request_id("caller-chosen-id")];
        let requests =
            build_put_node_requests("tenant", "catalog", nodes).expect("build must succeed");
        assert_eq!(requests[0].request_id, "caller-chosen-id");
    }

    #[test]
    fn put_node_requests_default_request_id_matches_the_single_item_put_node_prefix() {
        // `put_nodes` (batch) and `put_node` (single-item) both issue
        // `PutNode` calls, so an absent id must default to the exact same
        // prefix on both paths: `put_node:<uuid>`.
        let nodes = vec![
            NodeInput::new("product:1", json!({})),
            NodeInput::new("product:2", json!({})),
        ];
        let requests =
            build_put_node_requests("tenant", "catalog", nodes).expect("build must succeed");
        for request in &requests {
            let uuid_part = request
                .request_id
                .strip_prefix("put_node:")
                .expect("default request_id must use the put_node: prefix");
            uuid::Uuid::parse_str(uuid_part).expect("suffix after the prefix must be a uuid");
        }
        assert_ne!(
            requests[0].request_id, requests[1].request_id,
            "each item without an explicit request_id must get its own generated id"
        );
    }

    #[test]
    fn add_edge_requests_preserve_caller_order() {
        let edges = vec![
            EdgeInput::new("e3", "a", "b", "knows", json!({})),
            EdgeInput::new("e1", "b", "c", "knows", json!({})),
            EdgeInput::new("e2", "c", "d", "knows", json!({})),
        ];
        let requests =
            build_add_edge_requests("tenant", "catalog", edges).expect("build must succeed");
        let ids: Vec<&str> = requests.iter().map(|r| r.edge_id.as_str()).collect();
        assert_eq!(ids, vec!["e3", "e1", "e2"]);
    }

    #[test]
    fn add_edge_requests_do_not_merge_duplicate_edge_ids() {
        let edges = vec![
            EdgeInput::new("e1", "a", "b", "knows", json!({"v": 1})),
            EdgeInput::new("e1", "a", "b", "knows", json!({"v": 2})),
        ];
        let requests =
            build_add_edge_requests("tenant", "catalog", edges).expect("build must succeed");
        assert_eq!(
            requests.len(),
            2,
            "a HashMap keyed by edge_id would have collapsed this to one request"
        );
        assert_ne!(
            requests[0].json, requests[1].json,
            "each duplicate keeps its own payload"
        );
    }

    #[test]
    fn add_edge_requests_default_request_id_uses_the_add_edge_prefix() {
        // 1.0 defaulted an `AddEdge` call's `request_id` to a bare UUID with
        // no prefix, alone among the writes; 2.0 normalises it to
        // `add_edge:<uuid>`, on the batch path and the single-item one
        // alike.
        let edges = vec![
            EdgeInput::new("e1", "a", "b", "knows", json!({})),
            EdgeInput::new("e2", "b", "c", "knows", json!({})),
        ];
        let requests =
            build_add_edge_requests("tenant", "catalog", edges).expect("build must succeed");
        for request in &requests {
            let uuid_part = request
                .request_id
                .strip_prefix("add_edge:")
                .expect("default request_id must use the add_edge: prefix");
            uuid::Uuid::parse_str(uuid_part).expect("suffix after the prefix must be a uuid");
        }
        assert_ne!(
            requests[0].request_id, requests[1].request_id,
            "each item without an explicit request_id must get its own generated id"
        );

        // The single-item path must agree with the batch one.
        let single = build_add_edge_request(
            "tenant",
            "catalog",
            EdgeInput::new("e3", "c", "d", "knows", json!({})),
        )
        .expect("build must succeed");
        assert!(single.request_id.starts_with("add_edge:"));
    }

    #[test]
    fn group_preserving_order_keeps_each_groups_relative_order() {
        // `put_nodes`/`add_edges` rely on this: different keys may come back
        // in any order (backed by a `HashMap`), but the items sharing one
        // key must stay in the exact order they were given in, since that
        // order becomes the order the batch dispatch applies them in.
        let items = vec![("a", 1), ("b", 1), ("a", 2), ("a", 3), ("b", 2)];
        let groups = group_preserving_order(items, |item| item.0);

        let values_for = |key: &str| -> Vec<i32> {
            groups
                .iter()
                .find(|group| group.first().is_some_and(|item| item.0 == key))
                .map(|group| group.iter().map(|item| item.1).collect())
                .unwrap_or_default()
        };
        assert_eq!(values_for("a"), vec![1, 2, 3]);
        assert_eq!(values_for("b"), vec![1, 2]);
        assert_eq!(
            groups.iter().map(Vec::len).sum::<usize>(),
            5,
            "every item must land in exactly one group"
        );
    }
}
