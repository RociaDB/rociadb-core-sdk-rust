# Graph

Nodes and edges live under a `(tenant_id, graph)` pair. A graph has no
independent existence in the server's data model: it comes into being the
moment its first node is written, and disappears when its last one is
removed.

## Nodes

```rust,no_run
use rociadb_sdk::{NodeInput, RociaDbBuilder, WriteOptions};
use serde_json::{Value, json};

# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
client
    .put_node(
        "tenant-1",
        "catalog",
        "product:sku-1",
        &json!({"sku": "sku-1"}),
        WriteOptions::new(),
    )
    .await?;

// Decodes into any DeserializeOwned type; pass `Value` when the shape is
// not known up front.
let node: Value = client.get_node("tenant-1", "catalog", "product:sku-1").await?;
println!("{node}");

// A batch, with bounded concurrency and one idempotency key per item.
let nodes = vec![
    NodeInput::new("product:sku-2", json!({"sku": "sku-2"})),
    NodeInput::new("product:sku-3", json!({"sku": "sku-3"}))
        .with_request_id("import-42:sku-3"),
];
client.put_nodes("tenant-1", "catalog", nodes).await?;
# Ok(())
# }
```

`node_id` is the **complete** node id (`"product:sku-1"`), not a
`(label, id)` pair for the SDK to reassemble: `label:id` is a usage
convention the server does not enforce and the SDK does not recompose. An
unknown `node_id` on `get_node` is `NOT_FOUND`.

Like a document write, `put_node` replaces the node's properties outright
rather than merging them. The server requires the payload to be a JSON
**object** — never a scalar or an array.

## Edges

```rust,no_run
use rociadb_sdk::{Edge, EdgeInput, RociaDbBuilder, WriteOptions};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Serialize, Deserialize)]
struct Belongs {
    weight: u32,
}

# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
client
    .add_edge(
        "tenant-1",
        "catalog",
        EdgeInput::new(
            "edge-1",
            "product:sku-1",
            "group:grp-1",
            "belongs_to",
            json!({"weight": 1}),
        ),
    )
    .await?;

// The five fields of `Edge<T>` are exactly what `EdgeInput` carries, in the
// same orientation, so an edge read back feeds straight into a write.
let edge: Edge<Belongs> = client.get_edge("tenant-1", "catalog", "edge-1").await?;
println!("{} -[{}]-> {} ({})", edge.from, edge.label, edge.to, edge.value.weight);

client
    .add_edge(
        "tenant-1",
        "catalog",
        EdgeInput::new(
            edge.edge_id,
            edge.from,
            edge.to,
            edge.label,
            json!({"weight": edge.value.weight + 1}),
        ),
    )
    .await?;

client
    .delete_edge("tenant-1", "catalog", "edge-1", WriteOptions::new())
    .await?;
# Ok(())
# }
```

`EdgeInput` names `from` and `to` rather than taking them positionally for a
reason: they are same-typed, and a transposed pair persists the edge in the
reverse direction with no error whenever both endpoint nodes exist. The
`edge_id` is raw — do not prefix it with `label`.

`get_edge` is the only read that returns an edge's **properties**:
`neighbors_out` / `neighbors_in` name the neighbor and the edge id only, so
the JSON carried on a relation — a weight, a status, a date — has no other
way back. The properties come back in the shape they were written in, not as
the bytes originally sent: the server normalizes JSON on write (object keys
sorted, whitespace stripped). `edge_id` on the returned `Edge<T>` is echoed
from the argument the read was made with; the server does not repeat it in
the response. Unlike `get_node`, this read is not served from a server-side
cache — every call reaches storage.

An unknown `edge_id` is `NOT_FOUND`. An edge id is scoped to one
`(tenant_id, graph)` pair: the same id under another graph, or for another
tenant, is a different edge and therefore absent.

`delete_edge` is **idempotent**: deleting an edge that is not there succeeds
and touches nothing, so a wrong `edge_id` is not reported either. Read the
edge first — with `get_edge`, `neighbors_out` or `neighbors_in` — when you
need to know whether it existed.

### Endpoints must exist

`add_edge` and `add_edges` fail with `NOT_FOUND` if `from` or `to` does not
already exist as a node in the graph. Create both endpoint nodes first.

### A `(from, label, to)` triplet names at most one edge

The adjacency index is keyed by the triplet and does **not** carry the edge
id. So:

- adding a *second* edge over a triplet another edge already holds fails
  with `ALREADY_EXISTS` — the one non-auth code this SDK treats as an
  expected outcome to branch on (`RociaDbError::is_already_exists`). Retry
  with a fresh `edge_id`, or a different `label` / `to`; the same arguments
  will fail identically every time.
- reusing the **same** `edge_id` over its own triplet is allowed and
  replaces that edge's properties — which is what makes replaying a
  successful `add_edge` harmless.
- a known `edge_id` may also change endpoints or label: its former adjacency
  is retracted in the same transaction.

## Batches

`put_nodes` and `add_edges` take anything
`IntoIterator<Item = NodeInput>` / `IntoIterator<Item = EdgeInput>` — a
`Vec` in the common case — and issue at most **10 calls in flight at once**.

Items are consumed in the order given, and duplicate ids are **not** merged:
both are sent. Items with different ids run fully concurrently against each
other, but items sharing an id are dispatched strictly one after another —
the next call for that id is not sent until the previous one's response
comes back. That makes last-item-wins genuinely true for a duplicate id,
rather than a race between two in-flight calls whose completion order the
network decides.

**Neither batch is atomic, and each stops at the first error.** In-flight
requests are cancelled and the error does not say which items had already
succeeded. To resume, replay the same sequence with the same `request_id`
values used the first time: the server deduplicates on
`(tenant, operation, target, request_id)`, so already-applied writes are
recognized and skipped rather than reapplied, and only the writes that never
landed actually happen. The target — the node id or the edge id — is part of
that key, so two different items in one batch are never confused with each
other even if they somehow shared a `request_id`.

An item with `request_id: None` gets a fresh SDK-generated key, which is
exactly what a replay cannot deduplicate. Set it explicitly on any batch
that might have to be replayed.

## Neighbors

`neighbors_out` / `neighbors_in` return one page of raw
`Page<Neighbor>` — each `Neighbor` carrying `node_id` and `edge_id` and
nothing else:

```rust,no_run
# use rociadb_sdk::RociaDbBuilder;
# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
let page = client
    .neighbors_out("tenant-1", "catalog", "product:sku-1", "belongs_to", Some(50), None)
    .await?;
for neighbor in &page.items {
    println!("edge {} -> node {}", neighbor.edge_id, neighbor.node_id);
}
let _next = page.next_cursor;
# Ok(())
# }
```

A cursor returned here is scoped to the exact
`(tenant_id, graph, node, label, direction)` it was issued for — direction
meaning `neighbors_out` versus `neighbors_in`, which walk two distinct
indexes and never accept each other's cursor. Passing it back after changing
the node, the label or the direction is rejected with `INVALID_ARGUMENT`
rather than silently served against the new combination. Switching any one
of those mid-traversal means restarting from the first page, with
`cursor = None`.

### Neighbors with their node payloads

Two shapes, and the paged one is the one to reach for by default.

**`neighbor_nodes_out<T>` / `neighbor_nodes_in<T>`** take the same
`(tenant_id, graph, node_id, label, limit, cursor)` arguments and return
`Page<NeighborNode<T>>`: exactly one `neighbors_*` call for the page asked
for, then one `GetNode` per neighbor *on that page* (at most 10 in flight,
order preserved) decoded into `T`. The work is bounded by `limit` rather
than by the node's degree, and `next_cursor` drives the next page with the
same scoping rules as above.

```rust,no_run
use rociadb_sdk::RociaDbBuilder;
use serde_json::Value;

# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
let mut cursor: Option<String> = None;
loop {
    let page = client
        .neighbor_nodes_out::<Value>(
            "tenant-1",
            "catalog",
            "product:sku-1",
            "belongs_to",
            Some(50),
            cursor.as_deref(),
        )
        .await?;
    for neighbor in &page.items {
        println!("{} via {} = {}", neighbor.node_id, neighbor.edge_id, neighbor.value);
    }
    match page.next_cursor {
        Some(next) => cursor = Some(next),
        None => break,
    }
}
# Ok(())
# }
```

**`get_outgoing_neighbor_nodes<T>` / `get_incoming_neighbor_nodes<T>`** are
the all-pages variants: `(tenant_id, graph, node_id, label)` and a
`Vec<NeighborNode<T>>`. They are **unbounded by design** — they walk every
page of neighbors (50 per page, not caller-tunable) and issue one `GetNode`
per neighbor, collecting the lot in memory, so cost and memory scale with
the node's degree and nothing caps either. Use them only when "all of them"
is genuinely what you mean and the degree is known to be small.

Pagination inside them stops only when `next_cursor` comes back absent —
never merely because a page was short or empty, which the server can
legitimately produce mid-listing (a stale index entry pointing at a deleted
node, for example) with more data still to come. One extra safety net: they
also stop if a cursor repeats **any** cursor already used in the same pass,
which catches a multi-step cycle and not just an immediate repeat.

Either shape fails the whole page with `NOT_FOUND` if a neighbor's node is
deleted between the two calls, since the page is decoded as a unit.

## Listing graphs and nodes

```rust,no_run
# use rociadb_sdk::RociaDbBuilder;
# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
let graphs = client.list_graphs("tenant-1", None, None).await?;
let nodes = client.list_nodes("tenant-1", "catalog", Some(100), None).await?;
println!("{} graphs, {} nodes on this page", graphs.items.len(), nodes.items.len());
# Ok(())
# }
```

`list_graphs` lists graph names holding at least one node.

`list_nodes` against an unknown graph — or one belonging to another tenant —
returns `Ok` with an empty `Page` and no cursor, never an error. Because a
graph has no separate record, "this graph does not exist" and "this graph
exists but is empty" are the same observable state from here. A caller that
needs to tell them apart checks the name against `list_graphs`, or writes a
node to it.
