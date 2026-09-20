//! Tenant registry listing. One RPC, an inherent method on
//! [`RociaDbClient`].
use crate::pb::upstream::v1::ListTenantsRequest;
use crate::{DEFAULT_PAGE_SIZE, Page, Result, RociaDbClient, non_empty, page_request};
use tracing::debug;

impl RociaDbClient {
    /// Return one paginated page of tenant ids known to the deployment.
    ///
    /// This RPC is not scoped to a tenant: it enumerates the whole deployment.
    ///
    /// **It is not access-controlled today.** Any authenticated data-plane
    /// token can call it — read-only and read-write alike — because no
    /// tenant-scoped credential exists that would need excluding. Having it on
    /// its own service is what would let a policy be applied to it later
    /// without touching the three data services; it is not evidence that one
    /// exists. A `PERMISSION_DENIED` here means an admin-scoped token was
    /// presented against the data plane, the same cause as anywhere else, not
    /// a narrower scope for this call. Do not treat the list as privileged
    /// information, and see `docs/tenancy.md` for why `tenant_id` is a
    /// business partition rather than a security boundary.
    ///
    /// **The registry this lists is a side effect of writes, not a set of
    /// tenants the server tracks directly.** A tenant id appears here only
    /// after one of the seven write calls — [`RociaDbClient::put_document`],
    /// [`RociaDbClient::delete_document`], [`RociaDbClient::put_node`],
    /// [`RociaDbClient::add_edge`], [`RociaDbClient::delete_edge`],
    /// [`RociaDbClient::upload_file`] and [`RociaDbClient::delete_file`] —
    /// has actually committed data under it, never before. A tenant that has
    /// only ever been read from does not show up: there is nothing to list
    /// for it. Registration also follows the write rather than preceding
    /// it, so a call rejected before storage is reached (a missing required
    /// field, an oversized identifier, or any other `INVALID_ARGUMENT`)
    /// registers nothing.
    ///
    /// **Deletion lags behind emptying a tenant.** Once a tenant's last
    /// document, node, edge and file are gone, it does not drop out of this
    /// listing immediately — a background garbage-collection pass has to run
    /// first, and it only sweeps the registry for tenants with nothing left
    /// behind them on an interval (`gc.interval_secs`, one hour by default
    /// server-side, or an operator triggering `POST /admin/gc` sooner). A
    /// caller that deletes everything for a tenant and immediately calls
    /// this method should expect to still see that tenant listed until the
    /// next pass completes — that is not a sign the deletion failed. An
    /// upload still in flight does not count as data left behind either: it
    /// stays invisible to [`RociaDbClient::stat_file`],
    /// [`RociaDbClient::list_files`] and downloads until it commits, at
    /// which point the tenant it belongs to (re)appears here through that
    /// same commit.
    pub async fn list_tenants(
        &self,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<Page<String>> {
        debug!(
            limit = limit.unwrap_or(DEFAULT_PAGE_SIZE),
            cursor = cursor.unwrap_or(""),
            "listing tenants"
        );
        let request = ListTenantsRequest {
            page: page_request(limit, cursor)?,
        };
        let response = self
            .unary("failed to list tenants", request, |request| {
                let mut upstream = self.upstream_tenant.clone();
                async move { upstream.list_tenants(request).await }
            })
            .await?;
        Ok(Page {
            items: response.tenant_ids,
            next_cursor: response.page.and_then(|page| non_empty(page.next_cursor)),
        })
    }
}
