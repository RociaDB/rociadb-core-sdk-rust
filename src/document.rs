use crate::Result;
use crate::RociaDbClient;
use crate::error::{JsonResultExt, StatusResultExt};
use crate::pb::upstream::v1::{DeleteDocRequest, PutDocRequest};
use serde::Serialize;
use uuid::Uuid;

/// Default idempotency-key prefix for a `DeleteDoc` call issued through
/// [`RociaDbClient::delete_document`], when the caller does not use
/// [`RociaDbClient::delete_document_with_request_id`] directly. The `PutDoc`
/// counterpart of this is [`crate::default_document_request_id`], which
/// already exists for [`crate::RociaDbClient::create_document`] and is
/// reused below by [`RociaDbClient::put_document`] rather than duplicated —
/// this one is pulled out the same way, as a pure, network-free function,
/// so the exact default prefix (`delete_document:{collection}:<uuid>`) is
/// unit-testable without a live client or a network call.
fn default_delete_document_request_id(collection: &str) -> String {
    format!("delete_document:{collection}:{}", Uuid::new_v4())
}

/// Build the `PutDocRequest` for one document write, encoding `value` as
/// JSON. Pulled out as a pure, network-free function — the same reason
/// [`crate::build_put_node_requests`] exists for a node batch — so the
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

impl RociaDbClient {
    /// Create or replace one document without creating a graph binding.
    ///
    /// See [`RociaDbClient::put_document_with_request_id`] for what
    /// "replace" means here for a document that already exists at this id.
    pub async fn put_document<T: Serialize + ?Sized>(
        &self,
        tenant_id: &str,
        collection: &str,
        document_id: &str,
        value: &T,
    ) -> Result<()> {
        self.put_document_with_request_id(
            tenant_id,
            collection,
            document_id,
            value,
            crate::default_document_request_id(collection),
        )
        .await
    }

    /// Create or replace one document with a caller-provided idempotency key.
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
    pub async fn put_document_with_request_id<T: Serialize + ?Sized>(
        &self,
        tenant_id: &str,
        collection: &str,
        document_id: &str,
        value: &T,
        request_id: impl Into<String>,
    ) -> Result<()> {
        let request =
            build_put_doc_request(tenant_id, collection, document_id, value, request_id.into())?;
        let mut upstream_document = self.upstream_document.clone();
        upstream_document
            .put_doc(request)
            .await
            .status_context("failed to put document")?;
        Ok(())
    }

    /// Delete one document using an automatically generated idempotency key.
    ///
    /// **Idempotent**: deleting a `document_id` that does not exist
    /// succeeds rather than returning `NOT_FOUND`, and deleting the same
    /// document a second time succeeds too. See
    /// [`RociaDbClient::delete_document_with_request_id`] for what that
    /// costs a caller who wanted to be told whether anything was actually
    /// there.
    pub async fn delete_document(
        &self,
        tenant_id: &str,
        collection: &str,
        document_id: &str,
    ) -> Result<()> {
        self.delete_document_with_request_id(
            tenant_id,
            collection,
            document_id,
            default_delete_document_request_id(collection),
        )
        .await
    }

    /// Delete one document with a caller-provided idempotency key.
    ///
    /// Deleting a document that is not there is **not** an error: the call
    /// succeeds and touches nothing, exactly like
    /// [`RociaDbClient::delete_edge`] and [`RociaDbClient::delete_file`].
    /// The cost of that is real — a caller that got `collection` or
    /// `document_id` wrong is no longer told so. Read the document first,
    /// with [`RociaDbClient::get_document`], when you need to know whether
    /// it existed.
    pub async fn delete_document_with_request_id(
        &self,
        tenant_id: &str,
        collection: &str,
        document_id: &str,
        request_id: impl Into<String>,
    ) -> Result<()> {
        let request =
            build_delete_doc_request(tenant_id, collection, document_id, request_id.into());
        let mut upstream_document = self.upstream_document.clone();
        upstream_document
            .delete_doc(request)
            .await
            .status_context("failed to delete document")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_delete_doc_request, build_put_doc_request, default_delete_document_request_id,
    };
    use crate::RociaDbError;
    use serde_json::json;

    #[test]
    fn default_delete_document_request_id_uses_the_prefix_with_a_fresh_uuid_each_time() {
        let first = default_delete_document_request_id("products");
        let second = default_delete_document_request_id("products");
        assert!(
            first.starts_with("delete_document:products:"),
            "got: {first}"
        );
        assert!(
            second.starts_with("delete_document:products:"),
            "got: {second}"
        );
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
}
