//! Document RPCs against the in-process server: round trips, the graph node
//! binding, idempotent deletes, the three paginated reads, and the two things
//! only the wire can prove — that a caller's `request_id` reaches the server
//! and that a missing document comes back as `NOT_FOUND` with the server's
//! `reason`.

mod support;

use rociadb_sdk::{
    DocumentQueryFilter, DocumentQueryOperator, DocumentQuerySort, DocumentQuerySortDirection,
    DocumentWriteOptions, NodeBinding, WriteOptions,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use support::FakeServer;
use tonic::Code;

const TENANT: &str = "tenant-1";

/// A document whose encoded response clears tonic's 4 MiB receive default with
/// room to spare, for the two tests of `max_decoding_message_size`.
const OVERSIZED_DOCUMENT_BYTES: usize = 5 * 1024 * 1024;

/// Left unset, tonic caps a decoded message at 4 MiB — a ceiling this SDK does
/// not choose and, until `max_decoding_message_size` existed, could not lift.
/// One document over that size was simply unreadable, with no recourse.
///
/// The harness raises the *server's* limit, so the only ceiling in play here is
/// the client's own.
#[tokio::test]
async fn a_document_over_four_mebibytes_is_unreadable_at_the_default_decode_limit() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    let document = json!({ "blob": "x".repeat(OVERSIZED_DOCUMENT_BYTES) });
    client
        .put_document(
            TENANT,
            "blobs",
            "big",
            &document,
            DocumentWriteOptions::new(),
        )
        .await
        .expect("writing it is fine: the send side has no such ceiling");

    let error = client
        .get_document::<Value>(TENANT, "blobs", "big")
        .await
        .expect_err("the response exceeds the default 4 MiB decode ceiling");

    assert_eq!(
        error.code(),
        Some(Code::OutOfRange),
        "tonic reports an over-long message as OUT_OF_RANGE, got: {error}"
    );
}

/// The other half, and the one that proves the setter reaches the generated
/// clients rather than merely being stored on the builder: the same read, on a
/// client whose ceiling was raised, returns the document whole.
///
/// Built with `build_with_channel`, which is also the claim in the setter's own
/// documentation — the limit lives on the generated clients, not on the
/// `Channel`, so supplying a channel neither sets it nor overrides it.
#[tokio::test]
async fn raising_the_decode_limit_makes_the_same_document_readable() {
    let server = FakeServer::start().await;
    let client = server
        .builder()
        .max_decoding_message_size(16 * 1024 * 1024)
        .build_with_channel(server.channel())
        .await
        .expect("building a client with a raised decode ceiling must succeed");
    let document = json!({ "blob": "x".repeat(OVERSIZED_DOCUMENT_BYTES) });
    client
        .put_document(
            TENANT,
            "blobs",
            "big",
            &document,
            DocumentWriteOptions::new(),
        )
        .await
        .expect("the write must succeed");

    let read: Value = client
        .get_document(TENANT, "blobs", "big")
        .await
        .expect("a raised ceiling must let the oversized document through");
    assert_eq!(read, document, "the document must round-trip byte for byte");
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Product {
    sku: String,
    price: u32,
    active: bool,
}

/// Write `count` products into `products`, ids `sku-000` … in order.
async fn seed_products(client: &rociadb_sdk::RociaDbClient, count: usize) {
    for index in 0..count {
        let product = Product {
            sku: format!("sku-{index:03}"),
            price: (index as u32 + 1) * 10,
            active: index % 2 == 0,
        };
        client
            .put_document(
                TENANT,
                "products",
                &product.sku.clone(),
                &product,
                DocumentWriteOptions::new(),
            )
            .await
            .expect("seeding a document must succeed");
    }
}

#[tokio::test]
async fn put_then_get_round_trips_a_typed_struct() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    let product = Product {
        sku: "sku-1".to_string(),
        price: 4200,
        active: true,
    };

    client
        .put_document(
            TENANT,
            "products",
            "sku-1",
            &product,
            DocumentWriteOptions::new(),
        )
        .await
        .expect("a document write must succeed");

    let read: Product = client
        .get_document(TENANT, "products", "sku-1")
        .await
        .expect("the document just written must be readable");
    assert_eq!(read, product);
}

#[tokio::test]
async fn put_then_get_round_trips_a_serde_json_value() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    let document = json!({"sku": "sku-2", "tags": ["new", "sale"], "nested": {"a": 1}});

    client
        .put_document(
            TENANT,
            "products",
            "sku-2",
            &document,
            DocumentWriteOptions::new(),
        )
        .await
        .expect("a document write must succeed");

    let read: Value = client
        .get_document(TENANT, "products", "sku-2")
        .await
        .expect("the document just written must be readable");
    assert_eq!(read, document);
}

#[tokio::test]
async fn a_node_binding_writes_the_label_prefixed_node_and_reuses_the_request_id() {
    let server = FakeServer::start().await;
    let client = server.client().await;

    client
        .put_document(
            TENANT,
            "products",
            "sku-3",
            &json!({"sku": "sku-3"}),
            DocumentWriteOptions::new()
                .with_request_id("import-7:sku-3")
                .with_node_binding(NodeBinding::new("product", "catalog")),
        )
        .await
        .expect("a document write with a node binding must succeed");

    // What the server received is the only place this is observable: the node
    // id is "{label}:{document_id}", the payload names the collection and the
    // document id, the node goes to the binding's graph (not the collection),
    // and the one idempotency key covers both writes.
    let document = server.only_call("PutDoc");
    let node = server.only_call("PutNode");
    assert_eq!(node.str_field("node_id"), "product:sku-3");
    assert_eq!(node.str_field("graph"), "catalog");
    assert_eq!(
        node.json_field("json"),
        json!({"collection": "products", "id": "sku-3"})
    );
    assert_eq!(document.request_id.as_deref(), Some("import-7:sku-3"));
    assert_eq!(
        node.request_id, document.request_id,
        "the node write must reuse the document write's idempotency key"
    );

    // And the node really is in the graph afterwards.
    let stored: Value = client
        .get_node(TENANT, "catalog", "product:sku-3")
        .await
        .expect("the bound node must exist");
    assert_eq!(stored, json!({"collection": "products", "id": "sku-3"}));
}

#[tokio::test]
async fn put_document_without_a_binding_writes_no_node_at_all() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    client
        .put_document(
            TENANT,
            "products",
            "sku-4",
            &json!({"sku": "sku-4"}),
            DocumentWriteOptions::new(),
        )
        .await
        .expect("a plain document write must succeed");
    assert_eq!(server.call_count("PutNode"), 0);
}

#[tokio::test]
async fn delete_document_is_idempotent() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    client
        .put_document(
            TENANT,
            "products",
            "sku-5",
            &json!({"sku": "sku-5"}),
            DocumentWriteOptions::new(),
        )
        .await
        .expect("the write must succeed");

    for attempt in 0..3 {
        client
            .delete_document(TENANT, "products", "sku-5", WriteOptions::new())
            .await
            .unwrap_or_else(|error| panic!("delete {attempt} must succeed, got: {error}"));
    }
    // Deleting something that was never there is equally fine.
    client
        .delete_document(TENANT, "products", "never-existed", WriteOptions::new())
        .await
        .expect("deleting an absent document must succeed");

    let error = client
        .get_document::<Value>(TENANT, "products", "sku-5")
        .await
        .expect_err("the deleted document must be gone");
    assert!(error.is_not_found());
}

#[tokio::test]
async fn get_document_on_a_missing_id_is_not_found_and_carries_the_servers_reason() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    let error = client
        .get_document::<Value>(TENANT, "products", "absent")
        .await
        .expect_err("an unknown document id must fail");
    assert!(error.is_not_found(), "got: {error}");
    assert_eq!(error.code(), Some(tonic::Code::NotFound));
    assert_eq!(
        error.reason(),
        Some("not_found"),
        "the server's `reason` trailing metadata must survive into RociaDbError"
    );
    assert!(
        error.to_string().contains("failed to load document"),
        "the operation must still be named, got: {error}"
    );
}

#[tokio::test]
async fn a_caller_supplied_request_id_reaches_the_wire_on_every_write() {
    let server = FakeServer::start().await;
    let client = server.client().await;

    client
        .put_document(
            TENANT,
            "products",
            "sku-6",
            &json!({}),
            DocumentWriteOptions::new().with_request_id("write-key"),
        )
        .await
        .expect("the write must succeed");
    client
        .delete_document(
            TENANT,
            "products",
            "sku-6",
            WriteOptions::new().with_request_id("delete-key"),
        )
        .await
        .expect("the delete must succeed");

    assert_eq!(
        server.only_call("PutDoc").request_id.as_deref(),
        Some("write-key")
    );
    assert_eq!(
        server.only_call("DeleteDoc").request_id.as_deref(),
        Some("delete-key")
    );
}

#[tokio::test]
async fn a_generated_request_id_uses_the_documented_prefix_on_the_wire() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    client
        .put_document(
            TENANT,
            "products",
            "sku-7",
            &json!({}),
            DocumentWriteOptions::new(),
        )
        .await
        .expect("the write must succeed");

    let request_id = server
        .only_call("PutDoc")
        .request_id
        .expect("a write always carries an idempotency key");
    let uuid = request_id
        .strip_prefix("put_document:products:")
        .expect("the generated key must carry the documented prefix");
    uuid::Uuid::parse_str(uuid).expect("the suffix must be a uuid");
}

#[tokio::test]
async fn list_documents_pages_with_a_cursor_and_reports_the_total_count() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    seed_products(&client, 5).await;

    let first = client
        .list_documents::<Product>(TENANT, "products", Some(2), None)
        .await
        .expect("listing must succeed");
    assert_eq!(first.items.len(), 2);
    assert_eq!(first.total_count, 5, "the total ignores pagination");
    let cursor = first
        .next_cursor
        .clone()
        .expect("a page with more behind it must carry a cursor");

    let second = client
        .list_documents::<Product>(TENANT, "products", Some(2), Some(&cursor))
        .await
        .expect("the second page must succeed");
    assert_eq!(second.items.len(), 2);
    assert_eq!(second.total_count, 5);

    let third = client
        .list_documents::<Product>(
            TENANT,
            "products",
            Some(2),
            Some(
                second
                    .next_cursor
                    .as_deref()
                    .expect("a third page must be announced"),
            ),
        )
        .await
        .expect("the third page must succeed");
    assert_eq!(third.items.len(), 1);
    assert!(
        third.next_cursor.is_none(),
        "the last page must map the server's empty cursor to None"
    );

    // Every document, once, in id order.
    let skus: Vec<String> = first
        .items
        .iter()
        .chain(&second.items)
        .chain(&third.items)
        .map(|product| product.sku.clone())
        .collect();
    assert_eq!(
        skus,
        vec!["sku-000", "sku-001", "sku-002", "sku-003", "sku-004"]
    );
}

#[tokio::test]
async fn search_documents_finds_a_top_level_field_and_pages() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    seed_products(&client, 6).await;

    // Three of the six seeded products are active.
    let first = client
        .search_documents::<Product, _>(TENANT, "products", "active", &true, Some(2), None)
        .await
        .expect("the search must succeed");
    assert_eq!(first.total_count, 3);
    assert_eq!(first.items.len(), 2);
    assert!(first.items.iter().all(|product| product.active));

    let second = client
        .search_documents::<Product, _>(
            TENANT,
            "products",
            "active",
            &true,
            Some(2),
            first.next_cursor.as_deref(),
        )
        .await
        .expect("the second page must succeed");
    assert_eq!(second.items.len(), 1);
    assert!(second.next_cursor.is_none());

    // A value nothing matches is an empty page, not an error.
    let none = client
        .search_documents::<Product, _>(TENANT, "products", "sku", &"missing", None, None)
        .await
        .expect("a search matching nothing must succeed");
    assert!(none.items.is_empty());
    assert_eq!(none.total_count, 0);
}

#[tokio::test]
async fn query_documents_filters_sorts_and_pages() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    seed_products(&client, 6).await;

    let filters = [DocumentQueryFilter::new(
        "active",
        DocumentQueryOperator::Eq,
        vec![json!(true)],
    )];
    let sort = [DocumentQuerySort::new(
        "price",
        DocumentQuerySortDirection::Desc,
    )];

    let first = client
        .query_documents::<Product>(TENANT, "products", &filters, &sort, Some(2), None)
        .await
        .expect("the query must succeed");
    assert_eq!(first.total_count, 3, "the total counts every match");
    assert_eq!(first.items.len(), 2);
    assert!(
        first.items[0].price > first.items[1].price,
        "the descending sort must reach the server, got {:?}",
        first.items
    );

    let second = client
        .query_documents::<Product>(
            TENANT,
            "products",
            &filters,
            &sort,
            Some(2),
            first.next_cursor.as_deref(),
        )
        .await
        .expect("the second page must succeed");
    assert_eq!(second.items.len(), 1);
    assert!(second.next_cursor.is_none());

    // An `In` filter over the same collection, to prove the operator mapping
    // survives the wire rather than only the `Eq` case.
    let in_filter = [DocumentQueryFilter::new(
        "sku",
        DocumentQueryOperator::In,
        vec![json!("sku-001"), json!("sku-003")],
    )];
    let matched = client
        .query_documents::<Product>(TENANT, "products", &in_filter, &[], None, None)
        .await
        .expect("the IN query must succeed");
    let skus: Vec<&str> = matched
        .items
        .iter()
        .map(|product| product.sku.as_str())
        .collect();
    assert_eq!(skus, vec!["sku-001", "sku-003"]);
}

#[tokio::test]
async fn query_documents_supports_a_contains_filter() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    for (id, name) in [("a", "Blue Widget"), ("b", "Red Sprocket")] {
        client
            .put_document(
                TENANT,
                "parts",
                id,
                &json!({"name": name}),
                DocumentWriteOptions::new(),
            )
            .await
            .expect("seeding must succeed");
    }

    let filters = [DocumentQueryFilter::new(
        "name",
        DocumentQueryOperator::Contains,
        vec![json!("widget")],
    )];
    let page = client
        .query_documents::<Value>(TENANT, "parts", &filters, &[], None, None)
        .await
        .expect("the query must succeed");
    assert_eq!(page.items, vec![json!({"name": "Blue Widget"})]);
}

#[tokio::test]
async fn list_collections_reports_each_collections_document_count() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    seed_products(&client, 3).await;
    client
        .put_document(
            TENANT,
            "orders",
            "order-1",
            &json!({}),
            DocumentWriteOptions::new(),
        )
        .await
        .expect("the write must succeed");

    let page = client
        .list_collections(TENANT, None, None)
        .await
        .expect("listing collections must succeed");
    let counts: Vec<(String, u64)> = page
        .items
        .iter()
        .map(|info| (info.collection.clone(), info.count))
        .collect();
    assert_eq!(
        counts,
        vec![("orders".to_string(), 1), ("products".to_string(), 3)]
    );
    assert!(page.next_cursor.is_none());
}

#[tokio::test]
async fn a_zero_page_limit_is_rejected_before_the_request_is_sent() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    let error = client
        .list_documents::<Value>(TENANT, "products", Some(0), None)
        .await
        .expect_err("a zero limit must be rejected");
    assert!(matches!(error, rociadb_sdk::RociaDbError::Validation(_)));
    assert_eq!(
        server.call_count("ListDoc"),
        0,
        "the client-side check must run before anything reaches the server"
    );
}
