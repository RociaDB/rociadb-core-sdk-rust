//! Graph node and edge operations: single-item reads and writes, batch
//! upserts with bounded concurrency, and paginated neighbor traversal.
use crate::error::{JsonResultExt, StatusResultExt};
use crate::pb::upstream::v1::{
    AddEdgeRequest, DeleteEdgeRequest, GetEdgeRequest, GetEdgeResponse, GetNodeRequest,
    ListGraphsRequest, ListNodesRequest, Neighbor, NeighborsInRequest, NeighborsOutRequest,
    PutNodeRequest,
};
use crate::{CONCURRENT_REQUESTS, EdgeInput, Page, Result, RociaDbClient, non_empty, page_request};
use futures::{StreamExt, TryStreamExt, stream};
use serde::{Serialize, de::DeserializeOwned};
use std::collections::HashSet;
use tracing::error;
use uuid::Uuid;

/// One page of graph neighbors returned by the upstream service.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NeighborPage {
    pub neighbors: Vec<Neighbor>,
    pub next_cursor: Option<String>,
}

/// A graph neighbor together with its decoded node payload.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct NeighborNode<T> {
    pub edge_id: String,
    pub node_id: String,
    pub value: T,
}

/// One graph edge, endpoints and label included, with its properties
/// decoded into `T`.
///
/// Returned by [`RociaDbClient::get_edge_as`] and
/// [`RociaDbClient::get_edge`]. The five fields are exactly the five an
/// [`RociaDbClient::add_edge`] call takes, in the same orientation, so an
/// edge read back can be fed straight into a write without unwrapping
/// anything: `value` carries the edge's own properties, never the envelope
/// the server stores them in.
///
/// `edge_id` is echoed from the argument the read was made with — the
/// server does not repeat it in the response — while `from`, `to`, `label`
/// and `value` come from the server.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct Edge<T> {
    pub edge_id: String,
    pub from: String,
    pub to: String,
    pub label: String,
    pub value: T,
}

#[derive(Clone, Copy)]
enum NeighborDirection {
    Outgoing,
    Incoming,
}

/// Assemble an [`Edge<T>`] from a `GetEdgeResponse`'s endpoint and label
/// fields, the `edge_id` the read was made with, and the properties already
/// decoded from its `json` payload.
///
/// Split out of [`RociaDbClient::get_edge_as`] purely so this mapping can be
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

/// Build the `AddEdgeRequest` for [`RociaDbClient::add_edge_with_input`],
/// generating the default `request_id` (a bare UUID, no prefix — the same
/// default [`RociaDbClient::add_edge`] uses) when `edge.request_id` is
/// `None`.
///
/// Deliberately not routed through [`crate::build_add_edge_requests`] (the
/// batch builder [`RociaDbClient::add_edges`] uses): that would mean
/// wrapping `edge` in a one-element `Vec` and then unwrapping the single
/// result back out, for no benefit over building the request directly.
/// Pulled out as its own pure function for the same reason
/// [`edge_from_response`] is: `from` and `to` are two same-typed `String`
/// fields carried from `edge` into the request, and this makes that
/// mapping unit-testable without a live connection.
fn build_add_edge_request(
    tenant_id: &str,
    graph_name: &str,
    edge: EdgeInput,
) -> Result<AddEdgeRequest> {
    let json = serde_json::to_vec(&edge.value).encode_context("edge json")?;
    Ok(AddEdgeRequest {
        tenant_id: tenant_id.to_string(),
        graph: graph_name.to_string(),
        edge_id: edge.edge_id,
        from: edge.from,
        to: edge.to,
        label: edge.label,
        json,
        request_id: edge
            .request_id
            .unwrap_or_else(|| Uuid::new_v4().to_string()),
    })
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
    /// Fetch one node and decode its JSON payload into the requested type.
    pub async fn get_node_as<T: DeserializeOwned>(
        &self,
        tenant_id: &str,
        graph: &str,
        node_id: &str,
    ) -> Result<T> {
        let mut upstream_graph = self.upstream_graph.clone();
        let response = upstream_graph
            .get_node(GetNodeRequest {
                tenant_id: tenant_id.to_string(),
                graph: graph.to_string(),
                node_id: node_id.to_string(),
            })
            .await
            .status_context("failed to get node")?
            .into_inner();
        serde_json::from_slice(&response.json).decode_context("node json")
    }

    /// Fetch one edge by id and decode its properties into the requested type.
    ///
    /// This is the only read that returns an edge's properties:
    /// [`RociaDbClient::neighbors_out`] and
    /// [`RociaDbClient::neighbors_in`] name the neighbor and the edge id
    /// only, so the JSON carried on a relation — a weight, a status, a date
    /// — has no other way back.
    ///
    /// The server returns `NOT_FOUND` for an unknown `edge_id`, just as
    /// [`RociaDbClient::get_node_as`] does for a missing node. An edge id is
    /// scoped to one `(tenant_id, graph)` pair: the same id under another
    /// graph, or for another tenant, is a different edge and therefore
    /// absent.
    ///
    /// The properties come back in the shape they were written in, not as
    /// the bytes originally sent: the server normalizes JSON on write
    /// (object keys sorted, whitespace stripped).
    ///
    /// Unlike [`RociaDbClient::get_node_as`], this read is not served from a
    /// server-side cache — every call reaches storage.
    pub async fn get_edge_as<T: DeserializeOwned>(
        &self,
        tenant_id: &str,
        graph: &str,
        edge_id: &str,
    ) -> Result<Edge<T>> {
        let mut upstream_graph = self.upstream_graph.clone();
        let response = upstream_graph
            .get_edge(GetEdgeRequest {
                tenant_id: tenant_id.to_string(),
                graph: graph.to_string(),
                edge_id: edge_id.to_string(),
            })
            .await
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    graph = graph,
                    edge_id = edge_id,
                    error = %error,
                    "failed to get edge"
                );
            })
            .status_context("failed to get edge")?
            .into_inner();
        let value = serde_json::from_slice::<T>(&response.json)
            .inspect_err(|error| {
                error!(
                    tenant_id = tenant_id,
                    graph = graph,
                    edge_id = edge_id,
                    error = %error,
                    "failed to decode edge json"
                );
            })
            .decode_context("edge json")?;
        Ok(edge_from_response(edge_id, response, value))
    }

    /// Create or replace one node using its complete node id (for example `product:42`).
    pub async fn put_node<T: Serialize + ?Sized>(
        &self,
        tenant_id: &str,
        graph: &str,
        node_id: &str,
        value: &T,
    ) -> Result<()> {
        self.put_node_with_request_id(
            tenant_id,
            graph,
            node_id,
            value,
            format!("put_node:{}", Uuid::new_v4()),
        )
        .await
    }

    /// Create or replace one node with a caller-provided idempotency key.
    pub async fn put_node_with_request_id<T: Serialize + ?Sized>(
        &self,
        tenant_id: &str,
        graph: &str,
        node_id: &str,
        value: &T,
        request_id: impl Into<String>,
    ) -> Result<()> {
        let json = serde_json::to_vec(value).encode_context("node json")?;
        let mut upstream_graph = self.upstream_graph.clone();
        upstream_graph
            .put_node(PutNodeRequest {
                tenant_id: tenant_id.to_string(),
                graph: graph.to_string(),
                node_id: node_id.to_string(),
                json,
                request_id: request_id.into(),
            })
            .await
            .status_context("failed to put node")?;
        Ok(())
    }

    /// Create one edge, or replace the one `edge_id` already names.
    ///
    /// The server returns `NOT_FOUND` if `from` or `to` does not already
    /// exist as a node in `graph`: create both endpoint nodes before
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
    /// `from` and `to` are two of six consecutive same-typed `&str`
    /// arguments here, so a call that transposes them compiles cleanly and
    /// — whenever both endpoint nodes already exist, the normal case —
    /// silently persists the edge in the reversed direction with no error.
    /// When that risk matters more than the extra type, prefer
    /// [`RociaDbClient::add_edge_with_input`], which takes an [`EdgeInput`]
    /// with named `from`/`to` fields instead.
    #[allow(clippy::too_many_arguments)]
    pub async fn add_edge<T: Serialize + ?Sized>(
        &self,
        tenant_id: &str,
        graph: &str,
        edge_id: &str,
        from: &str,
        to: &str,
        label: &str,
        value: &T,
    ) -> Result<()> {
        self.add_edge_with_request_id(
            tenant_id,
            graph,
            edge_id,
            from,
            to,
            label,
            value,
            Uuid::new_v4().to_string(),
        )
        .await
    }

    /// Create or replace one edge with a caller-provided idempotency key.
    ///
    /// The server returns `NOT_FOUND` if `from` or `to` does not already
    /// exist as a node in `graph`: create both endpoint nodes before
    /// adding an edge between them. See [`RociaDbClient::add_edge`] for the
    /// `(from, label, to)` uniqueness rule and the `ALREADY_EXISTS` it
    /// produces, and for the `from`/`to` transposition risk that
    /// [`RociaDbClient::add_edge_with_input`] avoids.
    #[allow(clippy::too_many_arguments)]
    pub async fn add_edge_with_request_id<T: Serialize + ?Sized>(
        &self,
        tenant_id: &str,
        graph: &str,
        edge_id: &str,
        from: &str,
        to: &str,
        label: &str,
        value: &T,
        request_id: impl Into<String>,
    ) -> Result<()> {
        let json = serde_json::to_vec(value).encode_context("edge json")?;
        let mut upstream_graph = self.upstream_graph.clone();
        upstream_graph
            .add_edge(AddEdgeRequest {
                tenant_id: tenant_id.to_string(),
                graph: graph.to_string(),
                edge_id: edge_id.to_string(),
                from: from.to_string(),
                to: to.to_string(),
                label: label.to_string(),
                json,
                request_id: request_id.into(),
            })
            .await
            .status_context("failed to add edge")?;
        Ok(())
    }

    /// Create or replace one edge, taking it as a single [`EdgeInput`]
    /// instead of `edge_id`, `from`, `to`, and `label` as four adjacent
    /// positional `&str` arguments — see [`RociaDbClient::add_edge`] for why
    /// that positional shape is easy to get wrong, and [`EdgeInput`] for the
    /// named-field type [`RociaDbClient::add_edges`] already uses for the
    /// same reason.
    ///
    /// `edge.request_id` is used as-is when set; when `None`, one is
    /// generated the same way [`RociaDbClient::add_edge`] generates its own
    /// (a bare UUID, no prefix), so this single call covers what
    /// [`RociaDbClient::add_edge`] and
    /// [`RociaDbClient::add_edge_with_request_id`] need two separate methods
    /// for. See [`RociaDbClient::add_edge`] for the `NOT_FOUND` and
    /// `ALREADY_EXISTS` conditions and the `(from, label, to)` uniqueness
    /// rule — they apply here unchanged, `edge.value` taking the place of
    /// `value` and `edge.label` the place of `label`.
    ///
    /// Building a fresh `EdgeInput` still takes `from` and `to` positionally
    /// via [`EdgeInput::new`], so this does not make a `from`/`to`
    /// transposition impossible when the `EdgeInput` is constructed right
    /// before this call — it removes the risk from *this* call, not from
    /// building the value passed to it. It earns its keep most clearly when
    /// an `EdgeInput` already exists, built once and reused — assembled for
    /// [`RociaDbClient::add_edges`] and also written individually, for
    /// example — since then there is no positional call left to transpose.
    pub async fn add_edge_with_input(
        &self,
        tenant_id: &str,
        graph: &str,
        edge: EdgeInput,
    ) -> Result<()> {
        let request = build_add_edge_request(tenant_id, graph, edge)?;
        let mut upstream_graph = self.upstream_graph.clone();
        upstream_graph
            .add_edge(request)
            .await
            .status_context("failed to add edge")?;
        Ok(())
    }

    /// Delete one edge with a caller-provided idempotency key.
    ///
    /// Deleting an edge that is not there is **not** an error: the call
    /// succeeds and touches nothing, exactly like
    /// [`RociaDbClient::delete_document`] and
    /// [`RociaDbClient::delete_file`]. The cost of that is real — a caller
    /// that got the `edge_id` wrong is no longer told so. Read the edge
    /// first, with [`RociaDbClient::get_edge_as`],
    /// [`RociaDbClient::neighbors_out`] or
    /// [`RociaDbClient::neighbors_in`], when you need to know whether it
    /// existed.
    pub async fn delete_edge_with_request_id(
        &self,
        tenant_id: &str,
        graph: &str,
        edge_id: &str,
        request_id: impl Into<String>,
    ) -> Result<()> {
        let mut upstream_graph = self.upstream_graph.clone();
        upstream_graph
            .delete_edge(DeleteEdgeRequest {
                tenant_id: tenant_id.to_string(),
                graph: graph.to_string(),
                edge_id: edge_id.to_string(),
                request_id: request_id.into(),
            })
            .await
            .status_context("failed to delete edge")?;
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
    ) -> Result<NeighborPage> {
        let mut upstream_graph = self.upstream_graph.clone();
        let response = upstream_graph
            .neighbors_out(NeighborsOutRequest {
                tenant_id: tenant_id.to_string(),
                graph: graph.to_string(),
                from: from.to_string(),
                label: label.to_string(),
                page: page_request(limit, cursor)?,
            })
            .await
            .status_context("failed to get outgoing neighbors")?
            .into_inner();
        Ok(NeighborPage {
            neighbors: response.neighbors,
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
    ) -> Result<NeighborPage> {
        let mut upstream_graph = self.upstream_graph.clone();
        let response = upstream_graph
            .neighbors_in(NeighborsInRequest {
                tenant_id: tenant_id.to_string(),
                graph: graph.to_string(),
                to: to.to_string(),
                label: label.to_string(),
                page: page_request(limit, cursor)?,
            })
            .await
            .status_context("failed to get incoming neighbors")?
            .into_inner();
        Ok(NeighborPage {
            neighbors: response.neighbors,
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
        let mut upstream_graph = self.upstream_graph.clone();
        let response = upstream_graph
            .list_graphs(ListGraphsRequest {
                tenant_id: tenant_id.to_string(),
                page: page_request(limit, cursor)?,
            })
            .await
            .status_context("failed to list graphs")?
            .into_inner();
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
        let mut upstream_graph = self.upstream_graph.clone();
        let response = upstream_graph
            .list_nodes(ListNodesRequest {
                tenant_id: tenant_id.to_string(),
                graph: graph.to_string(),
                page: page_request(limit, cursor)?,
            })
            .await
            .status_context("failed to list nodes")?
            .into_inner();
        Ok(Page {
            items: response.node_ids,
            next_cursor: response.page.and_then(|page| non_empty(page.next_cursor)),
        })
    }

    /// Load all outgoing neighbors and decode each node payload.
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

    /// Load all incoming neighbors and decode each node payload.
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
                        Some(50),
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
                        Some(50),
                        cursor.as_deref(),
                    )
                    .await?
                }
            };
            neighbors.extend(page.neighbors);
            match next_pagination_cursor(&seen_cursors, page.next_cursor) {
                Some(next_cursor) => cursor = Some(next_cursor),
                None => break,
            }
        }

        let tenant_id = tenant_id.to_string();
        let graph = graph.to_string();
        stream::iter(neighbors)
            .map(|neighbor| {
                let tenant_id = tenant_id.clone();
                let graph = graph.clone();
                let mut upstream = self.upstream_graph.clone();
                async move {
                    let response = upstream
                        .get_node(GetNodeRequest {
                            tenant_id,
                            graph,
                            node_id: neighbor.node_id.clone(),
                        })
                        .await
                        .status_context("failed to get neighbor node")?
                        .into_inner();
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
    use super::{build_add_edge_request, edge_from_response, next_pagination_cursor};
    use crate::pb::upstream::v1::GetEdgeResponse;
    use crate::{EdgeInput, RociaDbError, non_empty, page_request};
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

    #[test]
    fn build_add_edge_request_generates_a_request_id_when_absent() {
        let edge = EdgeInput::new("edge-1", "user:1", "user:2", "follows", json!(null));

        let request = build_add_edge_request("tenant-a", "social", edge)
            .expect("building the request should not fail");

        assert!(!request.request_id.is_empty());
        // The default is a bare UUID with no prefix, unlike
        // `put_node`/`PutNode`'s `put_node:<uuid>` default — see
        // `EdgeInput::request_id`.
        assert!(!request.request_id.contains(':'));
    }
}
