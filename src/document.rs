//! Document operations: single-document reads and writes, paginated
//! listings, field search, multi-filter queries, and collection listing.
//!
//! The types this module defines are re-exported at the crate root
//! ([`crate::DocumentWriteOptions`], [`crate::DocumentPage`],
//! [`crate::NodeBinding`], [`crate::DocumentQueryFilter`],
//! [`crate::DocumentQuerySort`], [`crate::DocumentQueryOperator`],
//! [`crate::DocumentQuerySortDirection`]), and the RPCs are inherent methods
//! on [`RociaDbClient`].
use crate::error::JsonResultExt;
use crate::pb::upstream::v1::{
    DeleteDocRequest, FindByFieldRequest, GetDocRequest, ListCollectionsRequest, ListDocRequest,
    PageResponse, PutDocRequest, PutNodeRequest, QueryDocRequest, QueryFilter, QueryOperator,
    QuerySort, SortDirection,
};
use crate::{
    CollectionInfo, DEFAULT_PAGE_SIZE, Page, Result, RociaDbClient, WriteOptions, non_empty,
    page_request,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tracing::debug;
use uuid::Uuid;

/// One page of document results, together with the total number of
/// documents matching the request (before pagination). `items` and
/// `next_cursor` follow the same contract as [`Page<T>`].
///
/// The cost of `total_count` is **not** the same across the three methods
/// that produce it, because the server computes it differently for each:
/// - [`RociaDbClient::list_documents`] (`ListDoc`): free — the server keeps
///   a running per-collection counter updated on every write, so reading it
///   costs nothing beyond the listing itself.
/// - [`RociaDbClient::search_documents`] (`FindByField`): a count over the
///   matching field-index entries.
/// - [`RociaDbClient::query_documents`] (`QueryDoc`): expensive — the server
///   only knows the total once it has filtered the *complete* candidate set
///   for the query, so the cost scales with the number of candidates on
///   every single call. Do not call this in a loop expecting a cheap
///   number; fetch it once and cache it if the same query is issued
///   repeatedly.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentPage<T> {
    /// The decoded documents on this page, in the order the server returned
    /// them.
    pub items: Vec<T>,
    /// Cursor to pass back to fetch the page after this one, or `None` when
    /// this is the last page.
    pub next_cursor: Option<String>,
    /// Total number of documents matching the request, before pagination.
    /// It describes the same instant as `items` — both are taken from a
    /// single state of the store — but nothing is promised from one call to
    /// the next. See the type-level documentation for what it costs.
    pub total_count: u64,
}

/// A document's graph node binding: the `label` the node id is built from
/// and the `graph` it is written to.
///
/// Set it on [`DocumentWriteOptions::with_node_binding`] to have
/// [`RociaDbClient::put_document`] also upsert the graph node
/// `"{label}:{document_id}"`, carrying `{"collection": .., "id": ..}` as its
/// properties, so the document can be reached by graph traversal.
///
/// The two fields are same-typed and therefore transposable, which is why
/// they travel as one value: `label` without `graph` (or the reverse) is not
/// representable, and the only place a transposition can happen is the
/// single two-argument [`NodeBinding::new`] call — not spread across the
/// arguments of the document write itself.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeBinding {
    /// Label prefixed to the document id to form the graph node id, as
    /// `"{label}:{document_id}"`.
    pub label: String,
    /// Name of the graph the node is written to.
    pub graph: String,
}

impl NodeBinding {
    /// Bind to `label` within `graph`.
    pub fn new(label: impl Into<String>, graph: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            graph: graph.into(),
        }
    }
}

/// Per-call options for [`RociaDbClient::put_document`]: an idempotency key,
/// and optionally a graph node to bind the document to.
///
/// See the [idempotency key
/// defaults](WriteOptions#idempotency-key-defaults) for the key generated
/// when `request_id` is left unset, and for why the node binding's own write
/// deliberately reuses it.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DocumentWriteOptions {
    /// Idempotency key for the document write. `None` lets the SDK generate
    /// `put_document:{collection}:<uuid>`.
    pub request_id: Option<String>,
    /// Graph node to upsert alongside the document. `None` writes the
    /// document only.
    pub node_binding: Option<NodeBinding>,
}

impl DocumentWriteOptions {
    /// Options with every field at its default: no caller-supplied
    /// idempotency key and no graph node binding.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the idempotency key for this write; see the [idempotency key
    /// defaults](WriteOptions#idempotency-key-defaults).
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    /// Also upsert a graph node for this document; see [`NodeBinding`] and
    /// [`RociaDbClient::put_document`] (which documents that the two writes
    /// are not atomic).
    pub fn with_node_binding(mut self, node_binding: NodeBinding) -> Self {
        self.node_binding = Some(node_binding);
        self
    }
}

/// Supported document query operators exposed by the SDK.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentQueryOperator {
    /// The field equals the single value given.
    Eq,
    /// The field equals any one of the values given.
    In,
    /// The field contains the single value given as a case-insensitive
    /// substring. A term shorter than three characters is not indexable, and
    /// the server refuses a query in which no filter is indexable with
    /// `INVALID_ARGUMENT` rather than serving it from a full scan — pair a
    /// short term with an [`Eq`](Self::Eq) or [`In`](Self::In) filter on
    /// another field.
    Contains,
}

impl DocumentQueryOperator {
    fn as_proto(self) -> i32 {
        match self {
            Self::Eq => QueryOperator::Eq as i32,
            Self::In => QueryOperator::In as i32,
            Self::Contains => QueryOperator::Contains as i32,
        }
    }
}

/// Supported document sort directions exposed by the SDK.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentQuerySortDirection {
    /// Ascending order.
    Asc,
    /// Descending order.
    Desc,
}

impl DocumentQuerySortDirection {
    fn as_proto(self) -> i32 {
        match self {
            Self::Asc => SortDirection::Asc as i32,
            Self::Desc => SortDirection::Desc as i32,
        }
    }
}

/// Filter definition for `QueryDoc`. Every filter of a query is combined
/// with logical AND.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DocumentQueryFilter {
    /// Name of the document field to compare.
    pub field: String,
    /// How `field` is compared against `values`.
    pub operator: DocumentQueryOperator,
    /// Values to compare against, each serialized to JSON before being sent.
    /// How many the server expects depends on `operator`:
    /// [`Eq`](DocumentQueryOperator::Eq) and
    /// [`Contains`](DocumentQueryOperator::Contains) take one,
    /// [`In`](DocumentQueryOperator::In) takes the set to match.
    pub values: Vec<Value>,
}

impl DocumentQueryFilter {
    /// Build a filter on `field`, comparing it against `values` with
    /// `operator`. How many values an operator expects is the server's
    /// contract, not the SDK's: `Eq` takes one, `In` takes the set to match.
    pub fn new(
        field: impl Into<String>,
        operator: DocumentQueryOperator,
        values: Vec<Value>,
    ) -> Self {
        Self {
            field: field.into(),
            operator,
            values,
        }
    }
}

/// Sort definition for `QueryDoc`. A query's sort list is applied in the
/// order given, and results are always tie-broken by document id, so the
/// ordering is total and stable across pages.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DocumentQuerySort {
    /// Name of the document field to sort on.
    pub field: String,
    /// Direction to sort `field` in.
    pub direction: DocumentQuerySortDirection,
}

impl DocumentQuerySort {
    /// Sort on `field` in `direction`.
    pub fn new(field: impl Into<String>, direction: DocumentQuerySortDirection) -> Self {
        Self {
            field: field.into(),
            direction,
        }
    }
}

/// Default idempotency key for the `PutDoc` write issued by
/// [`RociaDbClient::put_document`] when
/// [`DocumentWriteOptions::request_id`] is unset. Pulled out as a pure,
/// network-free function — the same reason [`build_put_doc_request`] is — so
/// the exact default prefix (`put_document:{collection}:<uuid>`) is
/// unit-testable without a live client or a network call.
fn default_document_request_id(collection: &str) -> String {
    format!("put_document:{collection}:{}", Uuid::new_v4())
}

/// Default idempotency key for the `DeleteDoc` call issued by
/// [`RociaDbClient::delete_document`] when [`WriteOptions::request_id`] is
/// unset. Same rationale as [`default_document_request_id`].
fn default_delete_document_request_id(collection: &str) -> String {
    format!("delete_document:{collection}:{}", Uuid::new_v4())
}

/// Build the `PutDocRequest` for one document write, encoding `value` as
/// JSON. Pulled out as a pure, network-free function — the same reason
/// [`crate::graph::build_put_node_request`] exists for a node write — so the
/// request's field mapping and the JSON-encode failure path are both
/// unit-testable without a live client: `collection` and `document_id` are
/// both `&str` going into two same-typed `String` fields, so a future edit
/// that swapped their assignments would still type-check and compile clean
/// while silently sending every write to the wrong collection.
fn build_put_doc_request<T: Serialize + ?Sized>(
    tenant_id: &str,
    collection: &str,
    document_id: &str,
    value: &T,
    request_id: String,
) -> Result<PutDocRequest> {
    let json = serde_json::to_vec(value).encode_context("document json")?;
    Ok(PutDocRequest {
        tenant_id: tenant_id.to_string(),
        collection: collection.to_string(),
        id: document_id.to_string(),
        json,
        request_id,
    })
}

/// Build the `DeleteDocRequest` for one document delete. Same rationale as
/// [`build_put_doc_request`]: pulling the field mapping out into its own
/// pure function makes it unit-testable on its own, which is what catches a
/// future edit that mixed up `collection` and `document_id` — both
/// same-typed `String` fields here too — before it ever reaches a live
/// server.
fn build_delete_doc_request(
    tenant_id: &str,
    collection: &str,
    document_id: &str,
    request_id: String,
) -> DeleteDocRequest {
    DeleteDocRequest {
        tenant_id: tenant_id.to_string(),
        collection: collection.to_string(),
        id: document_id.to_string(),
        request_id,
    }
}

/// Build the `PutNodeRequest` for the graph node a
/// [`NodeBinding`] asks [`RociaDbClient::put_document`] to write alongside
/// the document: node id `"{label}:{document_id}"` in `binding.graph`,
/// carrying `{"collection": .., "id": ..}` as its properties.
///
/// Pure and network-free for the same reason [`build_put_doc_request`] is:
/// the node id is assembled from two `&str`s and the payload names the same
/// two values again, so this is where a transposition or a renamed JSON key
/// can be caught by a test instead of by a traversal that silently finds
/// nothing.
fn build_node_binding_request(
    tenant_id: &str,
    collection: &str,
    document_id: &str,
    binding: &NodeBinding,
    request_id: String,
) -> Result<PutNodeRequest> {
    let json = serde_json::to_vec(&json!({
        "collection": collection,
        "id": document_id,
    }))
    .encode_context("node json")?;
    Ok(PutNodeRequest {
        tenant_id: tenant_id.to_string(),
        graph: binding.graph.clone(),
        node_id: format!("{}:{}", binding.label, document_id),
        json,
        request_id,
    })
}

/// Decode one page of raw per-item JSON payloads into `T`, and map the
/// page's cursor with [`non_empty`]. Shared by
/// [`RociaDbClient::list_documents`], [`RociaDbClient::search_documents`]
/// and [`RociaDbClient::query_documents`] — the three RPCs whose response
/// shape is "a list of JSON blobs plus an optional `PageResponse`" — so
/// the extraction rule is defined, and unit-tested, in exactly one place
/// against a synthetic response instead of being copy-pasted three times
/// and only ever exercised through a live gRPC call.
///
/// Decoding still stops at the first bad item: `collect` short-circuits, so
/// a caller never receives a partial page alongside an error. What changes
/// is the diagnostic — the returned `serde_json::Error`'s message is
/// rewritten to lead with `"item <index>: "`, naming the zero-based
/// position of the offending item within the page, because a page can hold
/// dozens of items and "one of them failed to parse" leaves the caller
/// nothing to act on.
fn decode_document_page<T>(
    json: Vec<Vec<u8>>,
    page: Option<PageResponse>,
) -> std::result::Result<(Vec<T>, Option<String>), serde_json::Error>
where
    T: DeserializeOwned,
{
    let items = json
        .into_iter()
        .enumerate()
        .map(|(index, data)| {
            serde_json::from_slice::<T>(&data).map_err(|error| {
                <serde_json::Error as serde::de::Error>::custom(format!("item {index}: {error}"))
            })
        })
        .collect::<std::result::Result<Vec<T>, serde_json::Error>>()?;
    Ok((items, page.and_then(|page| non_empty(page.next_cursor))))
}

impl RociaDbClient {
    /// Create or replace one document, and optionally bind a graph node to
    /// it.
    ///
    /// This is a **complete replacement, not a partial merge**: `value`
    /// becomes the document's entire body. Writing an object that leaves
    /// out fields the current version has does not preserve them — it
    /// deletes them, the same way writing a shorter file over a longer one
    /// truncates what used to follow. The server has no merge or patch
    /// operation to fall back on: if you only meant to update part of a
    /// document, read the existing one first with
    /// [`RociaDbClient::get_document`], apply your change to the decoded
    /// value, and write the complete result back here.
    ///
    /// With [`DocumentWriteOptions::with_node_binding`], this also upserts
    /// the graph node `"{label}:{document_id}"` in the binding's graph,
    /// carrying `{"collection": .., "id": ..}` as its properties. That
    /// second write **is not atomic** with the first: the document is
    /// written first and the node second, so if the node write fails the
    /// document is left in place without its binding. Callers that need
    /// both or neither must handle that themselves — by retrying (the two
    /// writes share one idempotency key, so a replay of the whole call is
    /// safe), or by treating a document without its expected node as
    /// needing repair.
    ///
    /// See the [idempotency key
    /// defaults](WriteOptions#idempotency-key-defaults) for the key used
    /// when [`DocumentWriteOptions::request_id`] is unset.
    pub async fn put_document<T: Serialize + ?Sized>(
        &self,
        tenant_id: &str,
        collection: &str,
        document_id: &str,
        value: &T,
        options: DocumentWriteOptions,
    ) -> Result<()> {
        let request_id = options
            .request_id
            .unwrap_or_else(|| default_document_request_id(collection));
        // The node request is built before the document is written so that
        // a failure to encode either payload happens before anything is
        // sent, and so the node write can take its clone of `request_id`
        // before the document request consumes it. Reusing the one key
        // across both writes is deliberate: the server's dedup scope
        // includes the operation, so the `PutDoc` and `PutNode` markers
        // cannot collide, and replaying the whole call stays idempotent.
        let node_request = options
            .node_binding
            .as_ref()
            .map(|binding| {
                build_node_binding_request(
                    tenant_id,
                    collection,
                    document_id,
                    binding,
                    request_id.clone(),
                )
            })
            .transpose()?;
        let request = build_put_doc_request(tenant_id, collection, document_id, value, request_id)?;

        debug!(
            tenant_id = tenant_id,
            collection = collection,
            document_id = document_id,
            has_node_binding = node_request.is_some(),
            "upserting document"
        );
        self.unary("failed to upsert document", request, |request| {
            let mut upstream = self.upstream_document.clone();
            async move { upstream.put_doc(request).await }
        })
        .await?;

        if let Some(node_request) = node_request {
            debug!(
                tenant_id = tenant_id,
                collection = collection,
                document_id = document_id,
                graph = %node_request.graph,
                node_id = %node_request.node_id,
                "upserting graph node binding for document"
            );
            self.unary(
                "failed to upsert graph node binding",
                node_request,
                |request| {
                    let mut upstream = self.upstream_graph.clone();
                    async move { upstream.put_node(request).await }
                },
            )
            .await?;
        }
        Ok(())
    }

    /// Fetch a single document by id and decode its JSON payload into `T`
    /// (`GetDoc`).
    ///
    /// Unlike [`search_documents`](Self::search_documents),
    /// [`list_documents`](Self::list_documents) and
    /// [`query_documents`](Self::query_documents), this returns the value
    /// directly rather than a [`DocumentPage`]: there is nothing to paginate
    /// when fetching by id. An unknown `document_id` is `NOT_FOUND`.
    pub async fn get_document<T>(
        &self,
        tenant_id: &str,
        collection: &str,
        document_id: &str,
    ) -> Result<T>
    where
        T: DeserializeOwned,
    {
        debug!(
            tenant_id = tenant_id,
            collection = collection,
            document_id = document_id,
            "loading document"
        );
        let request = GetDocRequest {
            tenant_id: tenant_id.to_string(),
            collection: collection.to_string(),
            id: document_id.to_string(),
        };
        let response = self
            .unary("failed to load document", request, |request| {
                let mut upstream = self.upstream_document.clone();
                async move { upstream.get_doc(request).await }
            })
            .await?;
        serde_json::from_slice::<T>(&response.json).decode_context("document")
    }

    /// Delete one document.
    ///
    /// **Idempotent**: deleting a `document_id` that does not exist
    /// succeeds rather than returning `NOT_FOUND`, and deleting the same
    /// document a second time succeeds too — exactly like
    /// [`RociaDbClient::delete_edge`] and [`RociaDbClient::delete_file`].
    /// The cost of that is real: a caller that got `collection` or
    /// `document_id` wrong is not told so. Read the document first, with
    /// [`RociaDbClient::get_document`], when you need to know whether it
    /// was there.
    ///
    /// See the [idempotency key
    /// defaults](WriteOptions#idempotency-key-defaults) for the key used
    /// when [`WriteOptions::request_id`] is unset.
    pub async fn delete_document(
        &self,
        tenant_id: &str,
        collection: &str,
        document_id: &str,
        options: WriteOptions,
    ) -> Result<()> {
        debug!(
            tenant_id = tenant_id,
            collection = collection,
            document_id = document_id,
            "deleting document"
        );
        let request_id = options
            .request_id
            .unwrap_or_else(|| default_delete_document_request_id(collection));
        let request = build_delete_doc_request(tenant_id, collection, document_id, request_id);
        self.unary("failed to delete document", request, |request| {
            let mut upstream = self.upstream_document.clone();
            async move { upstream.delete_doc(request).await }
        })
        .await?;
        Ok(())
    }

    /// Return one paginated page of every document in `collection`
    /// (`ListDoc`).
    ///
    /// `total_count` on the returned [`DocumentPage`] is **free**: the
    /// server keeps a running per-collection counter updated on every
    /// write, so reading it costs nothing beyond the listing itself — see
    /// [`DocumentPage`] for how this compares to
    /// [`RociaDbClient::search_documents`] and
    /// [`RociaDbClient::query_documents`].
    pub async fn list_documents<T>(
        &self,
        tenant_id: &str,
        collection: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<DocumentPage<T>>
    where
        T: DeserializeOwned,
    {
        debug!(
            tenant_id = tenant_id,
            collection = collection,
            limit = limit.unwrap_or(DEFAULT_PAGE_SIZE),
            cursor = cursor.unwrap_or(""),
            "listing documents"
        );
        let request = ListDocRequest {
            tenant_id: tenant_id.to_string(),
            collection: collection.to_string(),
            page: page_request(limit, cursor)?,
        };
        let response = self
            .unary("failed to list documents", request, |request| {
                let mut upstream = self.upstream_document.clone();
                async move { upstream.list_doc(request).await }
            })
            .await?;
        let (items, next_cursor) = decode_document_page::<T>(response.json, response.page)
            .decode_context("listed documents")?;
        Ok(DocumentPage {
            items,
            next_cursor,
            total_count: response.total_count,
        })
    }

    /// Find documents whose `search_field` equals `value` (`FindByField`).
    ///
    /// `T` is the type each document is decoded into and `V` the type of the
    /// value searched for — any `Serialize` value, `&str` included, which is
    /// serialized to JSON and compared against the field index.
    ///
    /// `total_count` on the returned [`DocumentPage`] is a count over the
    /// matching field-index entries — see [`DocumentPage`] for how this
    /// compares to [`RociaDbClient::list_documents`] and
    /// [`RociaDbClient::query_documents`].
    pub async fn search_documents<T, V>(
        &self,
        tenant_id: &str,
        collection: &str,
        search_field: &str,
        value: &V,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<DocumentPage<T>>
    where
        T: DeserializeOwned,
        V: Serialize + ?Sized,
    {
        debug!(
            tenant_id = tenant_id,
            collection = collection,
            search_field = search_field,
            limit = limit.unwrap_or(DEFAULT_PAGE_SIZE),
            cursor = cursor.unwrap_or(""),
            "searching documents by field"
        );
        let request = FindByFieldRequest {
            tenant_id: tenant_id.to_string(),
            collection: collection.to_string(),
            field: search_field.to_string(),
            value_json: serde_json::to_vec(value).encode_context("search value")?,
            page: page_request(limit, cursor)?,
        };
        let response = self
            .unary("failed to search documents", request, |request| {
                let mut upstream = self.upstream_document.clone();
                async move { upstream.find_by_field(request).await }
            })
            .await?;
        let (items, next_cursor) = decode_document_page::<T>(response.json, response.page)
            .decode_context("search results")?;
        Ok(DocumentPage {
            items,
            next_cursor,
            total_count: response.total_count,
        })
    }

    /// Execute a paginated multi-filter document query.
    ///
    /// The underlying server applies filters with logical AND and uses the
    /// provided sort list in order. The returned `next_cursor` is an opaque
    /// server cursor that should be fed back unchanged.
    ///
    /// `total_count` on the returned [`DocumentPage`] is **expensive**: the
    /// server only knows it after filtering the complete candidate set for
    /// the query, so the cost scales with the number of candidates on every
    /// call — never call this in a loop just to get a count; see
    /// [`DocumentPage`] for the full comparison with
    /// [`RociaDbClient::list_documents`] and
    /// [`RociaDbClient::search_documents`].
    pub async fn query_documents<T>(
        &self,
        tenant_id: &str,
        collection: &str,
        filters: &[DocumentQueryFilter],
        sort: &[DocumentQuerySort],
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<DocumentPage<T>>
    where
        T: DeserializeOwned,
    {
        debug!(
            tenant_id = tenant_id,
            collection = collection,
            filter_count = filters.len(),
            sort_count = sort.len(),
            limit = limit.unwrap_or(DEFAULT_PAGE_SIZE),
            cursor = cursor.unwrap_or(""),
            "querying documents"
        );
        let request = QueryDocRequest {
            tenant_id: tenant_id.to_string(),
            collection: collection.to_string(),
            filters: build_query_filters(filters)?,
            sort: sort
                .iter()
                .map(|sort| QuerySort {
                    field: sort.field.clone(),
                    direction: sort.direction.as_proto(),
                })
                .collect(),
            page: page_request(limit, cursor)?,
        };
        let response = self
            .unary("failed to query documents", request, |request| {
                let mut upstream = self.upstream_document.clone();
                async move { upstream.query_doc(request).await }
            })
            .await?;
        let (items, next_cursor) = decode_document_page::<T>(response.json, response.page)
            .decode_context("queried documents")?;
        Ok(DocumentPage {
            items,
            next_cursor,
            total_count: response.total_count,
        })
    }

    /// List the document collections holding at least one document. Each
    /// [`CollectionInfo`] carries its document count.
    pub async fn list_collections(
        &self,
        tenant_id: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<Page<CollectionInfo>> {
        debug!(
            tenant_id = tenant_id,
            limit = limit.unwrap_or(DEFAULT_PAGE_SIZE),
            cursor = cursor.unwrap_or(""),
            "listing collections"
        );
        let request = ListCollectionsRequest {
            tenant_id: tenant_id.to_string(),
            page: page_request(limit, cursor)?,
        };
        let response = self
            .unary("failed to list collections", request, |request| {
                let mut upstream = self.upstream_document.clone();
                async move { upstream.list_collections(request).await }
            })
            .await?;
        Ok(Page {
            items: response.collections,
            next_cursor: response.page.and_then(|page| non_empty(page.next_cursor)),
        })
    }
}

/// Encode every [`DocumentQueryFilter`]'s values as JSON and map the SDK's
/// operator enum onto its protobuf constant. Pulled out of
/// [`RociaDbClient::query_documents`] so the encode failure path is a plain
/// `?` there rather than a nested `collect` over two fallible levels.
fn build_query_filters(filters: &[DocumentQueryFilter]) -> Result<Vec<QueryFilter>> {
    filters
        .iter()
        .map(|filter| {
            Ok(QueryFilter {
                field: filter.field.clone(),
                operator: filter.operator.as_proto(),
                values_json: filter
                    .values
                    .iter()
                    .map(serde_json::to_vec)
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .encode_context("query filter value")?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        DocumentPage, DocumentQueryFilter, DocumentQueryOperator, DocumentQuerySort,
        DocumentQuerySortDirection, DocumentWriteOptions, NodeBinding, build_delete_doc_request,
        build_node_binding_request, build_put_doc_request, build_query_filters,
        decode_document_page, default_delete_document_request_id, default_document_request_id,
    };
    use crate::RociaDbError;
    use crate::pb::upstream::v1::{PageResponse, QueryOperator, SortDirection};
    use serde_json::json;

    #[test]
    fn document_write_options_default_to_no_request_id_and_no_binding() {
        let options = DocumentWriteOptions::new();
        assert_eq!(options, DocumentWriteOptions::default());
        assert!(options.request_id.is_none());
        assert!(options.node_binding.is_none());
    }

    #[test]
    fn document_write_options_setters_are_chainable_and_readable() {
        let options = DocumentWriteOptions::new()
            .with_request_id("retry-1")
            .with_node_binding(NodeBinding::new("product", "catalog"));
        assert_eq!(options.request_id.as_deref(), Some("retry-1"));
        let binding = options
            .node_binding
            .as_ref()
            .expect("the binding must be stored");
        // Asserting each side individually is the point: `label` and `graph`
        // are both `String`, so a future edit that swapped them in
        // `NodeBinding::new` would still type-check.
        assert_eq!(binding.label, "product");
        assert_eq!(binding.graph, "catalog");
    }

    #[test]
    fn default_document_request_id_uses_the_put_document_prefix_with_a_fresh_uuid_each_time() {
        let first = default_document_request_id("catalog");
        let second = default_document_request_id("catalog");
        let uuid_part = first
            .strip_prefix("put_document:catalog:")
            .expect("default request_id must use the put_document:{collection}: prefix");
        uuid::Uuid::parse_str(uuid_part).expect("suffix after the prefix must be a uuid");
        assert_ne!(
            first, second,
            "each call without an explicit request_id must get its own generated id"
        );
    }

    #[test]
    fn default_delete_document_request_id_uses_the_prefix_with_a_fresh_uuid_each_time() {
        let first = default_delete_document_request_id("products");
        let second = default_delete_document_request_id("products");
        let uuid_part = first
            .strip_prefix("delete_document:products:")
            .expect("default request_id must use the delete_document:{collection}: prefix");
        uuid::Uuid::parse_str(uuid_part).expect("suffix after the prefix must be a uuid");
        assert_ne!(first, second, "each call must mint a fresh idempotency key");
    }

    #[test]
    fn build_put_doc_request_maps_fields_without_mixing_up_collection_and_id() {
        let request = build_put_doc_request(
            "tenant-1",
            "products",
            "sku-1",
            &json!({"name": "widget"}),
            "req-1".to_string(),
        )
        .expect("a valid value must encode");

        // Asserting each field individually — not just that the call
        // compiles — is the point: `collection` and `document_id` are both
        // `&str` feeding two same-typed `String` fields, so a future edit
        // that swapped their assignments in `build_put_doc_request` would
        // still type-check and pass a test that only checked shape.
        assert_eq!(request.tenant_id, "tenant-1");
        assert_eq!(request.collection, "products");
        assert_eq!(request.id, "sku-1");
        assert_eq!(request.request_id, "req-1");
        assert_eq!(
            request.json,
            serde_json::to_vec(&json!({"name": "widget"})).expect("encode")
        );
    }

    #[test]
    fn build_put_doc_request_surfaces_an_encode_error_for_an_unserializable_value() {
        // serde_json has no built-in value that fails to serialize (NaN and
        // Infinity both map to `null` rather than erroring), so a minimal
        // `Serialize` impl that always errors is the deterministic way to
        // exercise this path without a network call — mirroring the same
        // pattern used in `error.rs`'s own encode-failure test.
        struct AlwaysFailsToSerialize;
        impl serde::Serialize for AlwaysFailsToSerialize {
            fn serialize<S: serde::Serializer>(
                &self,
                _serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("simulated encode failure"))
            }
        }

        let error = build_put_doc_request(
            "tenant-1",
            "products",
            "sku-1",
            &AlwaysFailsToSerialize,
            "req-1".to_string(),
        )
        .expect_err("an always-failing Serialize impl must fail to encode");
        assert!(
            matches!(error, RociaDbError::Encode { context, .. } if context == "document json"),
            "got: {error:?}"
        );
    }

    #[test]
    fn build_delete_doc_request_maps_fields_without_mixing_up_collection_and_id() {
        let request =
            build_delete_doc_request("tenant-1", "products", "sku-1", "req-2".to_string());
        assert_eq!(request.tenant_id, "tenant-1");
        assert_eq!(request.collection, "products");
        assert_eq!(request.id, "sku-1");
        assert_eq!(request.request_id, "req-2");
    }

    #[test]
    fn node_binding_request_prefixes_the_document_id_with_the_label() {
        let request = build_node_binding_request(
            "tenant-1",
            "products",
            "sku-1",
            &NodeBinding::new("product", "catalog"),
            "req-3".to_string(),
        )
        .expect("the binding payload must encode");

        assert_eq!(request.tenant_id, "tenant-1");
        assert_eq!(
            request.graph, "catalog",
            "the node must be written to the binding's graph, not to the collection"
        );
        assert_eq!(
            request.node_id, "product:sku-1",
            "the node id is \"{{label}}:{{document_id}}\""
        );
        assert_eq!(
            request.json,
            serde_json::to_vec(&json!({"collection": "products", "id": "sku-1"})).expect("encode"),
            "the node carries the collection and document id, and nothing else"
        );
        assert_eq!(
            request.request_id, "req-3",
            "the node write reuses the document write's idempotency key"
        );
    }

    #[test]
    fn document_query_operator_as_proto_maps_every_variant_to_the_generated_constant() {
        // A swapped match arm here would silently send the wrong filter
        // semantics to the server with no client-side error, so each
        // variant is checked against the exact generated constant rather
        // than just checking that `as_proto` returns *something*.
        assert_eq!(
            DocumentQueryOperator::Eq.as_proto(),
            QueryOperator::Eq as i32
        );
        assert_eq!(
            DocumentQueryOperator::In.as_proto(),
            QueryOperator::In as i32
        );
        assert_eq!(
            DocumentQueryOperator::Contains.as_proto(),
            QueryOperator::Contains as i32
        );
    }

    #[test]
    fn document_query_sort_direction_as_proto_maps_every_variant_to_the_generated_constant() {
        assert_eq!(
            DocumentQuerySortDirection::Asc.as_proto(),
            SortDirection::Asc as i32
        );
        assert_eq!(
            DocumentQuerySortDirection::Desc.as_proto(),
            SortDirection::Desc as i32
        );
    }

    #[test]
    fn document_query_filter_and_sort_support_equality_comparison() {
        let filter_a = DocumentQueryFilter::new(
            "sku",
            DocumentQueryOperator::Eq,
            vec![serde_json::json!("sku-1")],
        );
        let filter_b = DocumentQueryFilter::new(
            "sku",
            DocumentQueryOperator::Eq,
            vec![serde_json::json!("sku-1")],
        );
        let filter_c = DocumentQueryFilter::new(
            "sku",
            DocumentQueryOperator::Eq,
            vec![serde_json::json!("sku-2")],
        );
        assert_eq!(filter_a, filter_b);
        assert_ne!(filter_a, filter_c);

        let sort_a = DocumentQuerySort::new("sku", DocumentQuerySortDirection::Asc);
        let sort_b = DocumentQuerySort::new("sku", DocumentQuerySortDirection::Asc);
        let sort_c = DocumentQuerySort::new("sku", DocumentQuerySortDirection::Desc);
        assert_eq!(sort_a, sort_b);
        assert_ne!(sort_a, sort_c);
    }

    #[test]
    fn build_query_filters_encodes_every_value_and_keeps_filter_order() {
        let filters = [
            DocumentQueryFilter::new("sku", DocumentQueryOperator::Eq, vec![json!("sku-1")]),
            DocumentQueryFilter::new(
                "tag",
                DocumentQueryOperator::In,
                vec![json!("new"), json!("sale")],
            ),
        ];
        let encoded = build_query_filters(&filters).expect("plain JSON values must encode");
        assert_eq!(encoded.len(), 2);
        assert_eq!(encoded[0].field, "sku");
        assert_eq!(encoded[0].operator, QueryOperator::Eq as i32);
        assert_eq!(
            encoded[0].values_json,
            vec![serde_json::to_vec(&json!("sku-1")).expect("encode")]
        );
        assert_eq!(encoded[1].field, "tag");
        assert_eq!(encoded[1].operator, QueryOperator::In as i32);
        assert_eq!(encoded[1].values_json.len(), 2);
    }

    // `decode_document_page` is the shared, network-free core of
    // `list_documents`, `search_documents` and `query_documents`'s response
    // handling (see its doc comment). Feeding it a synthetic response shape
    // directly exercises the real extraction logic those three RPCs run,
    // rather than only re-checking Rust's own field-access semantics on a
    // hand-built `DocumentPage`.

    #[test]
    fn decode_document_page_decodes_items_and_extracts_the_cursor() {
        let json = vec![
            serde_json::to_vec(&serde_json::json!({"n": 1})).expect("encode must succeed"),
            serde_json::to_vec(&serde_json::json!({"n": 2})).expect("encode must succeed"),
        ];
        let page = Some(PageResponse {
            next_cursor: "cursor-2".to_string(),
        });
        let (items, next_cursor) = decode_document_page::<serde_json::Value>(json, page)
            .expect("well-formed items must decode");
        assert_eq!(
            items,
            vec![serde_json::json!({"n": 1}), serde_json::json!({"n": 2})]
        );
        assert_eq!(next_cursor.as_deref(), Some("cursor-2"));
    }

    #[test]
    fn decode_document_page_maps_the_servers_empty_cursor_convention_to_none() {
        let page = Some(PageResponse {
            next_cursor: String::new(),
        });
        let (items, next_cursor) = decode_document_page::<serde_json::Value>(vec![], page)
            .expect("an empty page must still decode");
        assert!(items.is_empty());
        assert!(
            next_cursor.is_none(),
            "an empty next_cursor means \"no further page\" and must map to None — \
             the same rule list_documents/search_documents/query_documents depend on \
             to know when to stop paginating"
        );
    }

    #[test]
    fn decode_document_page_with_no_page_message_has_no_cursor() {
        let (items, next_cursor) = decode_document_page::<serde_json::Value>(vec![], None)
            .expect("a missing page message must still decode");
        assert!(items.is_empty());
        assert!(next_cursor.is_none());
    }

    #[test]
    fn decode_document_page_names_the_failing_items_position() {
        // `collect` still short-circuits on the first bad item — this only
        // checks that the resulting error names *which* item broke, instead
        // of just saying that something in the page failed to parse.
        let json = vec![
            serde_json::to_vec(&serde_json::json!({"n": 1})).expect("encode must succeed"),
            b"{ not valid json".to_vec(),
            serde_json::to_vec(&serde_json::json!({"n": 3})).expect("encode must succeed"),
        ];
        let error = decode_document_page::<serde_json::Value>(json, None)
            .expect_err("a malformed item must fail to decode");
        assert!(
            error.to_string().contains("item 1"),
            "the error must name the zero-based index of the item that failed, got: {error}"
        );
    }

    #[test]
    fn document_page_exposes_items_next_cursor_and_total_count() {
        let page = DocumentPage {
            items: vec!["a", "b"],
            next_cursor: Some("cursor-2".to_string()),
            total_count: 42,
        };
        assert_eq!(page.items, vec!["a", "b"]);
        assert_eq!(page.next_cursor.as_deref(), Some("cursor-2"));
        assert_eq!(page.total_count, 42);
    }

    #[test]
    fn document_page_has_no_next_cursor_on_the_last_page() {
        let page: DocumentPage<i32> = DocumentPage {
            items: vec![1, 2, 3],
            next_cursor: None,
            total_count: 3,
        };
        assert!(page.next_cursor.is_none());
        assert_eq!(page.items, vec![1, 2, 3]);
        assert_eq!(page.total_count, 3);
    }

    #[test]
    fn document_page_derives_clone_and_equality() {
        let page = DocumentPage {
            items: vec![1],
            next_cursor: None,
            total_count: 1,
        };
        assert_eq!(page.clone(), page);
        let different = DocumentPage {
            items: vec![1],
            next_cursor: None,
            total_count: 2,
        };
        assert_ne!(page, different);
    }
}
