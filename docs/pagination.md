# Pagination

Every paginated read takes `limit: Option<u32>` and `cursor: Option<&str>`
as its last two positional arguments, and returns a named struct rather than
a bare tuple.

| Returns | Fields | Methods |
| ------- | ------ | ------- |
| `DocumentPage<T>` | `items`, `next_cursor`, `total_count` | `list_documents`, `search_documents`, `query_documents` |
| `Page<T>` | `items`, `next_cursor` | `list_collections`, `list_graphs`, `list_nodes`, `neighbors_out`, `neighbors_in`, `neighbor_nodes_out`, `neighbor_nodes_in`, `list_buckets`, `list_files`, `list_tenants` |

Both are `#[non_exhaustive]`: read the fields freely, but do not construct
one with a struct literal.

## The loop

```rust,no_run
use rociadb_sdk::RociaDbBuilder;

# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
let mut cursor: Option<String> = None;
let mut seen = 0usize;
loop {
    let page = client.list_files("tenant-1", "assets", Some(100), cursor.as_deref()).await?;
    seen += page.items.len();
    for file_id in &page.items {
        println!("{file_id}");
    }
    // The ONLY correct stop condition. Not `items.is_empty()`, not
    // `items.len() < limit`.
    match page.next_cursor {
        Some(next) => cursor = Some(next),
        None => break,
    }
}
println!("{seen} files");
# Ok(())
# }
```

## Limits

- **`limit == 0` is rejected client-side, immediately**, with
  `RociaDbError::Validation` (`page limit must be greater than zero`) and no
  round trip to the server.
- **The server enforces its own ceiling**, `limits.max_page_size`, **200 by
  default but configurable per deployment**. The SDK deliberately does not
  duplicate that ceiling: any `limit >= 1` is forwarded unchanged and the
  server has the final say, rejecting anything above its configured ceiling
  with `INVALID_ARGUMENT`.
- **`limit = None` sends 20.** That is the SDK's own default, not the
  server's, which is 50. `PageRequest.limit` is an `optional` protobuf
  field, so leaving it off the wire is distinguishable from an explicit `0`
  and is what would make the server apply its own default — the SDK never
  does that, and always sends an explicit limit, so the page size a caller
  gets never depends on the server's configuration.

## `next_cursor` is the only end-of-list signal

**A page can legitimately be short, or even completely empty, in the middle
of a listing** — an index entry surviving a document or node that was since
deleted, for example — while still carrying a fresh `next_cursor`. Do not
stop because a page had few or no items; stop only when `next_cursor` comes
back absent.

One caveat in the other direction: when the total count is an exact multiple
of `limit`, the last full page still carries a cursor and the next call
returns an empty page with no cursor. That is expected, not a bug — the
server has no way to know it just handed out the last item.

Cursors are **opaque**: never construct or parse one, only pass back what
the server gave you. Do not persist a cursor across sessions; it is meant to
live for the duration of one pagination pass. The SDK maps the protobuf
empty-string cursor to `None`, so there is never an empty-string cursor to
handle.

### Cursors are scoped

A cursor belongs to the exact call that issued it. The neighbor listings
make this sharpest: a cursor from `neighbors_out` is scoped to
`(tenant_id, graph, node, label, direction)` and `neighbors_in` walks a
distinct index that never accepts it. Passing a cursor back after changing
any of those is rejected with `INVALID_ARGUMENT` rather than silently served
against the new combination — restart with `cursor = None` instead. The same
holds for `neighbor_nodes_out` / `neighbor_nodes_in`, which issue and
consume exactly those cursors. See [graph](graph.md).

## `total_count`

On `DocumentPage<T>`, `total_count` is the number of documents matching the
request **before** pagination. It describes the same instant as `items` —
both come from a single state of the store — but nothing is promised from
one call to the next.

What it costs differs sharply between the three methods that report it:
free on `list_documents`, an index count on `search_documents`, and
proportional to the full candidate set on `query_documents`. The details are
in [documents](documents.md#what-total_count-costs).

For collections, `list_collections` reports a per-collection `count` on each
`CollectionInfo` that is a maintained counter, free to read regardless of
collection size.

## Loading every page at once

The SDK does this itself in exactly two places:
`get_outgoing_neighbor_nodes` and `get_incoming_neighbor_nodes`, which are
documented as unbounded for that reason. Everywhere else the loop is the
caller's, so the caller decides how much to hold in memory. See
[graph](graph.md#neighbors-with-their-node-payloads) for the bounded
alternative.
