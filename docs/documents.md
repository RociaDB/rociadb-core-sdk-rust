# Documents

Seven methods, all inherent on `RociaDbClient`: `put_document`,
`get_document`, `delete_document`, `list_documents`, `search_documents`,
`query_documents` and `list_collections`. Every one of them takes
`tenant_id` first and `collection` second.

## Writing and reading one document

```rust,no_run
use rociadb_sdk::{DocumentWriteOptions, RociaDbBuilder, WriteOptions};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Product {
    sku: String,
    label: String,
}

# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
let product = Product { sku: "sku-123".into(), label: "Widget".into() };

client
    .put_document("tenant-1", "products", "sku-123", &product, DocumentWriteOptions::new())
    .await?;

// Decodes straight into the requested type; `serde_json::Value` works too.
let stored: Product = client.get_document("tenant-1", "products", "sku-123").await?;
println!("{} / {}", stored.sku, stored.label);

client
    .delete_document("tenant-1", "products", "sku-123", WriteOptions::new())
    .await?;
# Ok(())
# }
```

`put_document` is a **complete replacement, not a partial merge**: `value`
becomes the document's entire body. Writing an object that leaves out fields
the current version has deletes them, the same way writing a shorter file
over a longer one truncates what used to follow. The server has no merge or
patch operation to fall back on — read the existing document with
`get_document`, apply the change to the decoded value, and write the
complete result back.

`delete_document` is **idempotent**: deleting an id that does not exist
succeeds rather than returning `NOT_FOUND`, and so do `delete_edge` and
`delete_file`. No deletion in this API reports a missing target, so a caller
who got `collection` or `document_id` wrong is not told so. Read the
document first when you need to know whether it was there.

An unknown `document_id` on `get_document` is `NOT_FOUND` — see
[errors and retries](errors-and-retries.md).

## Binding a graph node to a document

`DocumentWriteOptions::with_node_binding` makes the same call also upsert
the graph node `"{label}:{document_id}"` in the binding's graph, carrying
`{"collection": .., "id": ..}` as its properties, so the document can be
reached by graph traversal:

```rust,no_run
# use rociadb_sdk::{DocumentWriteOptions, NodeBinding, RociaDbBuilder};
# use serde_json::json;
# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
client
    .put_document(
        "tenant-1",
        "products",
        "sku-123",
        &json!({"sku": "sku-123", "label": "Widget"}),
        DocumentWriteOptions::new()
            .with_node_binding(NodeBinding::new("product", "catalog"))
            .with_request_id("reindex-job-42:sku-123"),
    )
    .await?;
// The node written above is "product:sku-123" in the "catalog" graph.
# Ok(())
# }
```

`NodeBinding` carries `label` and `graph` as one value because the two are
same-typed and therefore transposable: "one without the other" is not
representable, and the only place a transposition can happen is the single
`NodeBinding::new(label, graph)` call.

**The second write is not atomic with the first.** The document is written
first and the node second, so a failed node write leaves the document in
place without its binding. Both writes deliberately share one `request_id` —
the server's dedup scope includes the operation, so the `PutDoc` and
`PutNode` markers cannot collide — which makes replaying the whole call
safe. Callers that need both or neither either retry, or treat a document
without its expected node as needing repair.

## Listing, searching and querying

Three reads return a `DocumentPage<T>` (`items`, `next_cursor`,
`total_count`) and follow the shared rules in [pagination](pagination.md).

```rust,no_run
use rociadb_sdk::{
    DocumentQueryFilter, DocumentQueryOperator, DocumentQuerySort,
    DocumentQuerySortDirection, RociaDbBuilder,
};
use serde_json::{Value, json};

# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
// Every document in a collection.
let listed = client
    .list_documents::<Value>("tenant-1", "products", Some(50), None)
    .await?;
println!("{} of {}", listed.items.len(), listed.total_count);

// Exact match on one indexed field. `T` is the decoded document type and
// `V` the type of the value searched for, so the turbofish is `::<T, _>`.
let matched = client
    .search_documents::<Value, _>("tenant-1", "products", "sku", &json!("sku-123"), None, None)
    .await?;
# let _ = matched;

// Several filters, combined with AND, plus sorting.
let filters = [
    DocumentQueryFilter::new("status", DocumentQueryOperator::Eq, vec![json!("active")]),
    DocumentQueryFilter::new(
        "tier",
        DocumentQueryOperator::In,
        vec![json!("gold"), json!("silver")],
    ),
];
let sort = [DocumentQuerySort::new("label", DocumentQuerySortDirection::Asc)];
let queried = client
    .query_documents::<Value>("tenant-1", "products", &filters, &sort, Some(50), None)
    .await?;
# let _ = queried;
# Ok(())
# }
```

`DocumentQueryOperator` is `Eq`, `In` or `Contains`;
`DocumentQuerySortDirection` is `Asc` or `Desc`. How many values an operator
expects is the server's contract, not the SDK's: `Eq` and `Contains` take
one, `In` takes the set to match. Filters combine with logical **AND**, and
results are always tie-broken by document id, so the ordering is total and
stable across pages.

### What `total_count` costs

Not the same on the three methods, because the server computes it
differently for each:

| Method | Cost |
| ------ | ---- |
| `list_documents` (`ListDoc`) | **free** — a running per-collection counter, updated on every write |
| `search_documents` (`FindByField`) | a count over the matching field-index entries |
| `query_documents` (`QueryDoc`) | **expensive** — the server only knows the total once it has filtered the *complete* candidate set, on every single call |

Never call `query_documents` in a loop just to get a number; fetch it once
and cache it if the same query is issued repeatedly. Prefer `list_documents`
when there is nothing to filter.

## Listing collections

`list_collections` returns a `Page<CollectionInfo>`, and each entry's
`count` is that maintained counter — free to read regardless of collection
size, which makes this the natural starting point for a dashboard:

```rust,no_run
# use rociadb_sdk::RociaDbBuilder;
# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
let collections = client.list_collections("tenant-1", Some(50), None).await?;
for info in &collections.items {
    println!("{} holds {} documents", info.collection, info.count);
}
# Ok(())
# }
```

Only collections holding at least one document are listed.

## Server-side rules

These are enforced by the server and, except where noted, not re-checked
client-side, so they surface as `INVALID_ARGUMENT` or `NOT_FOUND` from the
RPC itself.

| Rule | Detail |
| ---- | ------ |
| Identifier length (all RPCs) | Every field that becomes a storage-key segment — `tenant_id`, `collection`, `id`, `field`, `graph`, `node_id`, `edge_id`, `from`, `to`, `label`, `bucket`, `file_id`, `request_id` — is rejected beyond **256 UTF-8 bytes** with `INVALID_ARGUMENT`, naming the offending field and the limit. Length counts bytes of the value as sent, not characters, so a multi-byte identifier is capped sooner than its character count suggests. The check runs at the boundary, before any key is built, so an oversized identifier never surfaces as `INTERNAL` from storage. |
| JSON payload size | The encoded payload of `put_document`, `put_node` and `add_edge` must not exceed the server's `limits.max_doc_bytes`, **2 MiB by default**. Larger payloads are `INVALID_ARGUMENT`. |
| `search_documents` value | Must serialize to a JSON **scalar** — a string, number, bool or null. An object or array is `INVALID_ARGUMENT`. |
| `query_documents` with `Contains` | Case-insensitive substring match, but a term shorter than **3 characters is not indexable**. A query in which *no* filter is indexable is refused with `INVALID_ARGUMENT` rather than served from a full scan — pair a short term with an `Eq` or `In` filter on another field. |
| Page limits | `limit == 0` is rejected client-side with `RociaDbError::Validation`; the server's own ceiling (`limits.max_page_size`, 200 by default) is its to enforce. See [pagination](pagination.md). |

The graph-side rules — node payloads having to be JSON objects, edge
endpoints having to exist, and the `(from, label, to)` uniqueness rule —
are in [graph](graph.md).
