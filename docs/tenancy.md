# Tenancy and authorization

## `tenant_id` is a business partition, not a security boundary

**`tenant_id` is not derived from any caller identity.** Any authenticated
client can address any tenant. Enforcing which caller may touch which tenant
is the calling application's responsibility, not the server's — the
`tenant_id` argument that leads every scoped call partitions data, it does
not protect it.

## The two token scopes

- **read-only** — gets `PERMISSION_DENIED` on the 7 write RPCs:
  `put_document`, `delete_document`, `put_node` / `put_nodes`,
  `add_edge` / `add_edges`, `delete_edge`, the uploads
  (`upload_file` / `upload_file_chunked` / `upload_file_stream`) and
  `delete_file`. Every read method — documents, graph, files, tenants —
  works normally, which is enough to build a full read-only exploration
  console.
- **admin** — the scope used to create and rotate `rocia-idp` service
  accounts through its own account-management API — is refused on **all 23
  RPCs**, reads included. It has no business talking to the data plane.

So if a *read* unexpectedly returns `PERMISSION_DENIED`, this is almost
always the cause: double-check which `client_id` produced the token, since
the data account and the admin account are two distinct credentials. And
because a fresh token carries the same scope, `PERMISSION_DENIED` is never
worth retrying — see
[authentication](authentication.md#unauthenticated-vs-permission_denied).

## Listing tenants

`list_tenants` is the only method not scoped to a tenant: it enumerates the
whole deployment from a dedicated service, and returns a `Page<String>` of
tenant ids under the usual [pagination](pagination.md) rules.

```rust,no_run
use rociadb_sdk::RociaDbBuilder;

# #[tokio::main]
# async fn main() -> rociadb_sdk::Result<()> {
# let client = RociaDbBuilder::new().disable_auth().build().await?;
let mut cursor: Option<String> = None;
loop {
    let page = client.list_tenants(Some(100), cursor.as_deref()).await?;
    for tenant_id in &page.items {
        println!("tenant = {tenant_id}");
    }
    match page.next_cursor {
        Some(next) => cursor = Some(next),
        None => break,
    }
}
# Ok(())
# }
```

Any authenticated data-plane token can call it — read-only and read-write
credentials both work, since no tenant-scoped credential exists that would
need excluding. A `PERMISSION_DENIED` here is not a narrower credential
scope: it is an admin-scoped token presented against the data plane, the
same cause as everywhere else.

### The registry is a side effect of writes

**A tenant id appears here only after one of the 7 write calls has actually
committed data under it**, never before. A tenant that has only ever been
read from does not show up: there is nothing to list for it. Registration
also follows the write rather than preceding it, so a call rejected before
storage is reached — a missing required field, an oversized identifier, any
other `INVALID_ARGUMENT` — registers nothing.

### Deletion lags behind emptying a tenant

Once a tenant's last document, node, edge and file are gone, it does **not**
drop out of this listing immediately. A background garbage-collection pass
has to run first, and it sweeps the registry for tenants with nothing left
behind them only on an interval — `gc.interval_secs`, one hour by default
server-side, or sooner if an operator triggers `POST /admin/gc`. A caller
that deletes everything for a tenant and immediately calls `list_tenants`
should expect to still see it listed until the next pass completes; that is
not a sign the deletion failed.

An upload still in flight does not count as data left behind either: it
stays invisible to `stat_file`, `list_files` and downloads until it commits,
at which point the tenant it belongs to (re)appears here through that same
commit.
