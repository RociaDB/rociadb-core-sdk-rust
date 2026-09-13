//! Deadlines and retries against the in-process server: a per-RPC timeout
//! that fires against a slow handler (and the streaming download it
//! deliberately does not cover), the `ABORTED` replay loop, the `UNAVAILABLE`
//! opt-in, and the two replay mechanisms — the caller's `retry` and the
//! client's own refresh-and-retry — composed on one call.

mod support;

use rociadb_sdk::{RetryPolicy, RociaDbError, WriteOptions};
use serde_json::json;
use std::time::{Duration, Instant};
use support::{FakeServer, MockIdp};
use tonic::Code;

const TENANT: &str = "tenant-1";
const GRAPH: &str = "catalog";

/// A retry schedule with the backoff wound right down: the point of these
/// tests is which calls are made, not how long the SDK waits between them
/// (the schedule itself is a unit test in `src/retry.rs`).
fn fast_policy(max_attempts: u32) -> RetryPolicy {
    RetryPolicy::new()
        .with_max_attempts(max_attempts)
        .with_base_delay(Duration::from_millis(1))
        .with_max_delay(Duration::from_millis(5))
}

#[tokio::test]
async fn a_request_timeout_fires_against_a_handler_that_sleeps_past_the_deadline() {
    let server = FakeServer::start().await;
    // Ten times the deadline: the call must not wait for this.
    server.delay("ListTenants", Duration::from_millis(1500));
    let client = server
        .builder()
        .request_timeout(Duration::from_millis(150))
        .build_with_channel(server.channel())
        .await
        .expect("the client must build");

    let started = Instant::now();
    let error = client
        .list_tenants(None, None)
        .await
        .expect_err("a handler slower than the deadline must not be waited out");
    let elapsed = started.elapsed();

    assert_eq!(
        error.code(),
        Some(Code::DeadlineExceeded),
        "an expired request timeout must surface as DEADLINE_EXCEEDED, got: {error}"
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "the deadline must fire promptly, took {elapsed:?}"
    );
    assert!(
        error.to_string().contains("failed to list tenants"),
        "the operation must still be named, got: {error}"
    );
}

#[tokio::test]
async fn a_request_timeout_leaves_a_call_that_finishes_in_time_alone() {
    let server = FakeServer::start().await;
    server.delay("ListTenants", Duration::from_millis(20));
    let client = server
        .builder()
        .request_timeout(Duration::from_secs(5))
        .build_with_channel(server.channel())
        .await
        .expect("the client must build");

    client
        .list_tenants(None, None)
        .await
        .expect("a handler well inside the deadline must succeed");
}

#[tokio::test]
async fn a_request_timeout_does_not_apply_to_a_streaming_download() {
    let server = FakeServer::start().await;
    let client = server
        .builder()
        .request_timeout(Duration::from_millis(100))
        .build_with_channel(server.channel())
        .await
        .expect("the client must build");
    client
        .upload_file(
            TENANT,
            "assets",
            "slow.bin",
            b"payload".as_slice(),
            rociadb_sdk::FileUploadOptions::new(),
        )
        .await
        .expect("the upload must succeed");
    // Well past the deadline a unary call would be held to.
    server.delay("Download", Duration::from_millis(400));

    let bytes = client
        .download_file(TENANT, "assets", "slow.bin")
        .await
        .expect("the documented exemption: a transfer is not a unary round trip");
    assert_eq!(bytes, b"payload");
}

#[tokio::test]
async fn retry_replays_an_aborted_write_with_the_same_request_id_until_it_lands() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    server.fail_next("PutNode", 2, Code::Aborted);

    // One idempotency key, minted once, reused by every attempt — which is
    // what lets the server recognise the replay rather than apply the write
    // twice.
    let options = WriteOptions::new().with_request_id("import-7:node-1");
    client
        .retry(&fast_policy(3), || {
            let options = options.clone();
            async {
                client
                    .put_node(TENANT, GRAPH, "n:1", &json!({"v": 1}), options)
                    .await
            }
        })
        .await
        .expect("two ABORTED responses followed by success must return Ok");

    let calls = server.calls_for("PutNode");
    assert_eq!(calls.len(), 3, "three attempts, the first one included");
    for (index, call) in calls.iter().enumerate() {
        assert_eq!(
            call.request_id.as_deref(),
            Some("import-7:node-1"),
            "attempt {index} must carry the caller's idempotency key"
        );
    }
}

#[tokio::test]
async fn retry_returns_the_last_aborted_error_once_the_attempts_are_exhausted() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    server.fail_next("ListTenants", 5, Code::Aborted);

    let error = client
        .retry(&fast_policy(3), || client.list_tenants(None, None))
        .await
        .expect_err("a server that keeps aborting must surface its own error");
    assert!(error.is_aborted(), "got: {error}");
    assert_eq!(
        error.reason(),
        Some("conflict"),
        "ABORTED is the one code whose `reason` is not its own name"
    );
    assert_eq!(
        server.call_count("ListTenants"),
        3,
        "max_attempts counts the first attempt"
    );
}

#[tokio::test]
async fn retry_never_replays_a_status_outside_the_policy() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    server.fail_next("GetDoc", 3, Code::NotFound);

    let error = client
        .retry(&fast_policy(3), || {
            client.get_document::<serde_json::Value>(TENANT, "products", "sku-1")
        })
        .await
        .expect_err("NOT_FOUND is final");
    assert!(error.is_not_found(), "got: {error}");
    assert_eq!(
        server.call_count("GetDoc"),
        1,
        "a final status must be returned on the first attempt"
    );
}

#[tokio::test]
async fn unavailable_is_replayed_only_when_the_policy_opts_in() {
    let server = FakeServer::start().await;
    let client = server.client().await;

    server.fail_next("ListTenants", 2, Code::Unavailable);
    let error = client
        .retry(&fast_policy(3), || client.list_tenants(None, None))
        .await
        .expect_err("UNAVAILABLE is not retried by default");
    assert_eq!(error.code(), Some(Code::Unavailable), "got: {error}");
    assert_eq!(server.call_count("ListTenants"), 1);

    // Same scripted failures, the opt-in turned on: the third attempt lands.
    let server = FakeServer::start().await;
    let client = server.client().await;
    server.fail_next("ListTenants", 2, Code::Unavailable);
    client
        .retry(&fast_policy(3).with_retry_unavailable(true), || {
            client.list_tenants(None, None)
        })
        .await
        .expect("with_retry_unavailable(true) must replay UNAVAILABLE");
    assert_eq!(server.call_count("ListTenants"), 3);
}

#[tokio::test]
async fn retry_composes_with_the_automatic_refresh_and_retry_on_unauthenticated() {
    // Two independent mechanisms on one call: the SDK's own refresh-and-retry
    // (once, inside every unary RPC) and the caller's `retry` loop. The
    // scripted sequence forces both to act — ABORTED for the outer loop, then
    // UNAUTHENTICATED for the inner one — and the caller must see neither.
    let idp = MockIdp::start().await;
    let server = FakeServer::start().await;
    let client = server.authenticated_client(&idp).await;
    server.fail_next("ListTenants", 1, Code::Aborted);
    server.fail_next("ListTenants", 1, Code::Unauthenticated);

    client
        .retry(&fast_policy(3), || client.list_tenants(None, None))
        .await
        .expect("the two mechanisms together must land the call");

    assert_eq!(
        server.call_count("ListTenants"),
        3,
        "attempt 1 aborts, attempt 2 is unauthenticated and is replayed in place"
    );
    assert_eq!(idp.requests(), 2, "exactly one token refresh");
    let authorizations: Vec<Option<String>> = server.authorizations();
    assert_eq!(
        authorizations,
        vec![
            Some("Bearer token-1".to_string()),
            Some("Bearer token-1".to_string()),
            Some("Bearer token-2".to_string()),
        ],
        "only the attempt after the refresh carries the new token"
    );
}

#[tokio::test]
async fn an_aborted_batch_item_surfaces_from_the_batch_helper() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    server.fail_next("PutNode", 1, Code::Aborted);

    let error = client
        .put_nodes(
            TENANT,
            GRAPH,
            vec![
                rociadb_sdk::NodeInput::new("n:1", json!({})),
                rociadb_sdk::NodeInput::new("n:2", json!({})),
            ],
        )
        .await
        .expect_err("a batch stops at the first failing item");
    assert!(error.is_aborted(), "got: {error}");

    // Replaying the batch with the same idempotency keys is the documented
    // recovery, and it succeeds once the scripted failure is used up.
    client
        .put_nodes(
            TENANT,
            GRAPH,
            vec![
                rociadb_sdk::NodeInput::new("n:1", json!({})).with_request_id("batch:n1"),
                rociadb_sdk::NodeInput::new("n:2", json!({})).with_request_id("batch:n2"),
            ],
        )
        .await
        .expect("the replay must succeed");
}

#[tokio::test]
async fn a_validation_error_is_returned_without_any_retry_or_round_trip() {
    let server = FakeServer::start().await;
    let client = server.client().await;

    let error = client
        .retry(&fast_policy(3), || client.list_tenants(Some(0), None))
        .await
        .expect_err("a zero page limit is a client-side rule");
    assert!(matches!(error, RociaDbError::Validation(_)), "got: {error}");
    assert_eq!(
        server.call_count("ListTenants"),
        0,
        "nothing may reach the server, on any attempt"
    );
}
