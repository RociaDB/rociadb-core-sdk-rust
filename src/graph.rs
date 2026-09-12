//! Graph node and edge operations: single-item reads and writes, batch
//! upserts with bounded concurrency, and paginated neighbor traversal.
use crate::error::{JsonResultExt, StatusResultExt};
use crate::pb::upstream::v1::{
    AddEdgeRequest, DeleteEdgeRequest, GetEdgeRequest, GetNodeRequest, ListGraphsRequest,
    ListNodesRequest, Neighbor, NeighborsInRequest, NeighborsOutRequest, PutNodeRequest,
};
use crate::{CONCURRENT_REQUESTS, Page, Result, RociaDbClient, non_empty, page_request};
use futures::{StreamExt, TryStreamExt, stream};
use serde::{Serialize, de::DeserializeOwned};
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

/// Decide whether neighbor pagination should keep going, given the cursor
/// just used (`current_cursor`) and the `next_cursor` the last page came
/// back with. Continues on any fresh cursor — including when the page that
/// carried it was empty or shorter than the requested limit, since the
/// server can legitimately hand back a short or empty page mid-listing (a
/// stale index entry pointing at a deleted node, for example) followed by
/// more data. Stops only when `next_cursor` is absent, or when the server
/// repeats the cursor we just used (a guard against an infinite loop on a
/// misbehaving server).
fn next_pagination_cursor(
    current_cursor: Option<&str>,
    next_cursor: Option<String>,
) -> Option<String> {
    match next_cursor {
        Some(next_cursor) if current_cursor != Some(next_cursor.as_str()) => Some(next_cursor),
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
            .status_context("failed to get edge")?
            .into_inner();
        Ok(Edge {
            edge_id: edge_id.to_string(),
            from: response.from,
            to: response.to,
            label: response.label,
            value: serde_json::from_slice(&response.json).decode_context("edge json")?,
        })
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
    /// produces.
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
    // or short page never stops the loop on its own.
    async fn get_neighbor_nodes<T: DeserializeOwned>(
        &self,
        tenant_id: &str,
        graph: &str,
        node_id: &str,
        label: &str,
        direction: NeighborDirection,
    ) -> Result<Vec<NeighborNode<T>>> {
        let mut cursor = None;
        let mut neighbors = Vec::new();
        loop {
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
            match next_pagination_cursor(cursor.as_deref(), page.next_cursor) {
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
    use super::next_pagination_cursor;
    use crate::{RociaDbError, non_empty, page_request};

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
        assert_eq!(next_pagination_cursor(None, None), None);
        assert_eq!(next_pagination_cursor(Some("cursor-1"), None), None);
    }

    #[test]
    fn pagination_continues_on_empty_page_with_a_fresh_cursor() {
        assert_eq!(
            next_pagination_cursor(None, Some("cursor-1".to_string())),
            Some("cursor-1".to_string())
        );
        assert_eq!(
            next_pagination_cursor(Some("cursor-1"), Some("cursor-2".to_string())),
            Some("cursor-2".to_string())
        );
    }

    #[test]
    fn pagination_stops_on_a_repeated_cursor() {
        assert_eq!(
            next_pagination_cursor(Some("cursor-1"), Some("cursor-1".to_string())),
            None
        );
    }
}
