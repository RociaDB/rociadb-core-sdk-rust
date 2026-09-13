//! Authentication, end to end against the in-process server and the mock
//! identity provider: the header the interceptor attaches, the automatic
//! refresh-and-retry on `UNAUTHENTICATED`, the out-of-band refresh methods,
//! the background refresh backoff, and the two ways `build()` can fail.

mod support;

use rociadb_sdk::{RociaDbBuilder, RociaDbError, WriteOptions};
use serde_json::{Value, json};
use std::time::Duration;
use support::{FakeServer, MockIdp};
use tonic::Code;

const TENANT: &str = "tenant-1";

#[tokio::test]
async fn the_interceptor_attaches_the_idp_token_to_every_call() {
    let idp = MockIdp::start().await;
    let server = FakeServer::start().await;
    let client = server.authenticated_client(&idp).await;

    assert_eq!(idp.requests(), 1, "build() fetches the first token");
    client
        .put_node(TENANT, "catalog", "n:1", &json!({}), WriteOptions::new())
        .await
        .expect("the write must succeed");
    client
        .list_tenants(None, None)
        .await
        .expect("the read must succeed");

    let expected = idp
        .latest_bearer()
        .expect("the provider must have issued a token");
    assert_eq!(expected, "Bearer token-1");
    assert_eq!(
        server.authorizations(),
        vec![Some(expected.clone()), Some(expected)],
        "every call must carry the cached bearer header"
    );
    assert_eq!(
        idp.requests(),
        1,
        "a token that is still valid must not be re-fetched per call"
    );

    // And the form body really is a client-credentials grant.
    let body = idp.bodies().first().cloned().unwrap_or_default();
    assert!(
        body.contains("grant_type=client_credentials"),
        "got: {body}"
    );
    assert!(
        body.contains("client_id=integration-test-client"),
        "got: {body}"
    );
}

#[tokio::test]
async fn disable_auth_sends_no_authorization_header_at_all() {
    let server = FakeServer::start().await;
    let client = server.client().await;
    client
        .list_tenants(None, None)
        .await
        .expect("the read must succeed");
    assert_eq!(server.authorizations(), vec![None]);
}

#[tokio::test]
async fn one_unauthenticated_response_refreshes_the_token_and_replays_the_call() {
    let idp = MockIdp::start().await;
    let server = FakeServer::start().await;
    let client = server.authenticated_client(&idp).await;
    client
        .put_document(
            TENANT,
            "products",
            "sku-1",
            &json!({"sku": "sku-1"}),
            rociadb_sdk::DocumentWriteOptions::new(),
        )
        .await
        .expect("seeding must succeed");
    server.clear_calls();
    server.fail_next("GetDoc", 1, Code::Unauthenticated);

    // The caller sees none of this: the SDK refreshes and replays once.
    let document: Value = client
        .get_document(TENANT, "products", "sku-1")
        .await
        .expect("the replayed call must succeed transparently");
    assert_eq!(document, json!({"sku": "sku-1"}));

    assert_eq!(idp.requests(), 2, "exactly one refresh");
    let calls = server.calls_for("GetDoc");
    assert_eq!(calls.len(), 2, "the call is issued twice, no more");
    assert_eq!(calls[0].authorization.as_deref(), Some("Bearer token-1"));
    assert_eq!(
        calls[1].authorization.as_deref(),
        Some("Bearer token-2"),
        "the replay must carry the *new* token, not the rejected one"
    );
}

#[tokio::test]
async fn a_second_unauthenticated_response_reaches_the_caller_after_one_refresh() {
    let idp = MockIdp::start().await;
    let server = FakeServer::start().await;
    let client = server.authenticated_client(&idp).await;
    server.fail_next("ListTenants", 2, Code::Unauthenticated);

    let error = client
        .list_tenants(None, None)
        .await
        .expect_err("a credential the server keeps rejecting must reach the caller");
    assert!(error.is_unauthenticated(), "got: {error}");
    assert_eq!(error.reason(), Some("unauthenticated"));
    assert_eq!(
        idp.requests(),
        2,
        "the token is refreshed exactly once, never in a loop"
    );
    assert_eq!(
        server.call_count("ListTenants"),
        2,
        "the call is replayed exactly once"
    );
}

#[tokio::test]
async fn a_failed_refresh_returns_the_original_unauthenticated_error() {
    let idp = MockIdp::start().await;
    let server = FakeServer::start().await;
    let client = server.authenticated_client(&idp).await;
    server.fail_next("ListTenants", 1, Code::Unauthenticated);
    // The refresh the UNAUTHENTICATED triggers is the one that fails.
    idp.fail_next(1, 500);

    let error = client
        .list_tenants(None, None)
        .await
        .expect_err("with no usable token, the original error must come back");
    assert!(
        error.is_unauthenticated(),
        "the original UNAUTHENTICATED describes what the caller asked for, not the refresh \
         failure, got: {error}"
    );
    assert_eq!(idp.requests(), 2, "the refresh was attempted once");
    assert_eq!(
        server.call_count("ListTenants"),
        1,
        "a call must not be replayed when there is no new token to replay it with"
    );
}

#[tokio::test]
async fn refresh_auth_token_fetches_a_new_token_and_later_calls_carry_it() {
    let idp = MockIdp::start().await;
    let server = FakeServer::start().await;
    let client = server.authenticated_client(&idp).await;
    client
        .list_tenants(None, None)
        .await
        .expect("the first read must succeed");

    client
        .refresh_auth_token()
        .await
        .expect("an out-of-band refresh must succeed");
    assert_eq!(idp.requests(), 2);

    client
        .list_tenants(None, None)
        .await
        .expect("the second read must succeed");
    assert_eq!(
        server.authorizations(),
        vec![
            Some("Bearer token-1".to_string()),
            Some("Bearer token-2".to_string())
        ]
    );
}

#[tokio::test]
async fn refresh_auth_token_surfaces_an_idp_failure_without_dropping_the_cached_token() {
    let idp = MockIdp::start().await;
    let server = FakeServer::start().await;
    let client = server.authenticated_client(&idp).await;
    idp.fail_next(1, 503);

    let error = client
        .refresh_auth_token()
        .await
        .expect_err("a 503 from the provider must surface");
    assert!(matches!(error, RociaDbError::Auth { .. }), "got: {error}");

    // The still-valid cached token keeps working: a failed refresh must never
    // replace it with nothing.
    client
        .list_tenants(None, None)
        .await
        .expect("the cached token must still be usable");
    assert_eq!(
        server.authorizations(),
        vec![Some("Bearer token-1".to_string())]
    );
}

#[tokio::test]
async fn invalidate_auth_token_wakes_the_background_refresh_without_blocking() {
    let idp = MockIdp::start().await;
    let server = FakeServer::start().await;
    let client = server.authenticated_client(&idp).await;
    assert_eq!(idp.requests(), 1);

    // Synchronous by design: no `.await`, so the caller never pays for the
    // round trip. The background task does.
    client.invalidate_auth_token();

    let waited = idp
        .wait_for_requests(2, Duration::from_secs(5))
        .await
        .expect("the background task must honour the request promptly");
    assert!(
        waited < Duration::from_secs(5),
        "the refresh took {waited:?}, which is not \"at the next opportunity\""
    );

    client
        .list_tenants(None, None)
        .await
        .expect("the read must succeed");
    assert_eq!(
        server.authorizations(),
        vec![Some("Bearer token-2".to_string())],
        "calls after the requested refresh must carry the new token"
    );
}

#[tokio::test]
async fn a_failed_background_refresh_is_retried_on_the_documented_backoff() {
    // A two-second token: the SDK's cadence is `max(expires_in * 2 / 3, 5s)`
    // clamped to `expires_in - 1`, so the background task refreshes about a
    // second after each token is issued. The first of those refreshes fails,
    // and the retry must land within the documented ~1 s backoff rather than
    // waiting out another whole cadence.
    let idp = MockIdp::start_with_expires_in(2).await;
    let server = FakeServer::start().await;
    let client = server.authenticated_client(&idp).await;
    assert_eq!(idp.requests(), 1, "build() issued token-1");
    idp.fail_next(1, 500);

    // Request 2 is the scheduled refresh that fails; request 3 is the backoff
    // retry that succeeds and issues token-2.
    let waited = idp
        .wait_for_requests(3, Duration::from_secs(8))
        .await
        .expect("a failed refresh must be retried on the backoff, not the regular cadence");
    assert!(
        waited < Duration::from_secs(6),
        "the first retry is documented at roughly one second after the failure; this took \
         {waited:?}"
    );
    assert!(
        idp.issued_tokens().len() >= 2,
        "the retry must actually have issued a new token, got {:?}",
        idp.issued_tokens()
    );

    client
        .list_tenants(None, None)
        .await
        .expect("the read must succeed");
    let header = server
        .authorizations()
        .first()
        .cloned()
        .flatten()
        .expect("the call must carry a bearer header");
    assert_ne!(
        header, "Bearer token-1",
        "the token in use must have changed after the background refresh"
    );
    assert!(header.starts_with("Bearer token-"), "got: {header}");
}

#[tokio::test]
async fn build_reports_an_auth_error_when_the_identity_provider_rejects_the_credentials() {
    let idp = MockIdp::start().await;
    let server = FakeServer::start().await;
    idp.fail_next(1, 500);

    let error = server
        .authenticated_builder(&idp)
        .build()
        .await
        .expect_err("a provider answering 500 must fail build()");
    assert!(matches!(error, RociaDbError::Auth { .. }), "got: {error}");
    assert!(
        error.to_string().contains("token"),
        "the message must name the step that failed, got: {error}"
    );
}

#[tokio::test]
async fn build_dials_the_host_and_validates_it_first() {
    let idp = MockIdp::start().await;
    let server = FakeServer::start().await;

    // The one test that goes through `build()` rather than
    // `build_with_channel`: host validation, endpoint construction and a real
    // dial are all exercised here.
    let client = server
        .authenticated_builder(&idp)
        .connect_timeout(Duration::from_secs(5))
        .build()
        .await
        .expect("a reachable loopback host must build");
    client
        .list_tenants(None, None)
        .await
        .expect("a client built by build() must be usable");
    assert_eq!(
        server.authorizations(),
        vec![Some("Bearer token-1".to_string())]
    );

    // A host carrying a path is rejected before any socket is opened.
    let error = RociaDbBuilder::new()
        .host(format!("{}/v1", server.host()))
        .disable_auth()
        .build()
        .await
        .expect_err("a host with a path must be rejected");
    assert!(matches!(error, RociaDbError::Config { .. }), "got: {error}");
    assert!(error.to_string().contains("/v1"));
}

#[tokio::test]
async fn build_reports_a_config_error_when_no_credentials_are_configured_anywhere() {
    // `build()` falls back to AUTH_TOKEN_URL / AUTH_CLIENT_ID /
    // AUTH_CLIENT_SECRET, so "no credentials" only means anything when the
    // ambient environment supplies none either — true in CI, and true on a
    // developer machine that has not exported them for something else.
    // Setting or clearing environment variables from a test would race every
    // other test in this binary (and is `unsafe` in edition 2024), so this
    // reads them instead of imposing them.
    let inherited = ["AUTH_TOKEN_URL", "AUTH_CLIENT_ID", "AUTH_CLIENT_SECRET"]
        .iter()
        .any(|name| std::env::var_os(name).is_some());
    if inherited {
        eprintln!("skipped: the environment supplies AUTH_* credentials");
        return;
    }

    let server = FakeServer::start().await;
    let error = RociaDbBuilder::new()
        .host(server.host())
        .build()
        .await
        .expect_err("auth is on by default and nothing configures it");
    assert!(matches!(error, RociaDbError::Config { .. }), "got: {error}");
    assert!(
        error.to_string().contains("AUTH_TOKEN_URL"),
        "the message must name the variable to set, got: {error}"
    );
    assert_eq!(
        server.calls().len(),
        0,
        "a configuration error must be raised before anything is sent"
    );
}

#[tokio::test]
async fn client_debug_names_the_host_and_never_the_token_or_the_secret() {
    let idp = MockIdp::start().await;
    let server = FakeServer::start().await;
    // A secret that could not plausibly appear by accident.
    const SECRET: &str = "unmistakable-client-secret-9f3a7c";
    let builder = RociaDbBuilder::new()
        .host(server.host())
        .auth_client_credentials(idp.token_url(), "integration-test-client", SECRET);
    let client = builder
        .build_with_channel(server.channel())
        .await
        .expect("the client must build");

    let rendered = format!("{client:?}");
    assert!(
        rendered.contains(&server.host()),
        "Debug must name the host, got: {rendered}"
    );
    assert!(
        rendered.contains("auth_enabled: true"),
        "Debug must report whether auth is on, got: {rendered}"
    );
    assert!(
        !rendered.contains(SECRET),
        "Debug must never contain the client secret, got: {rendered}"
    );
    let token = idp
        .latest_token()
        .expect("the provider must have issued a token");
    assert!(
        !rendered.contains(&token),
        "Debug must never contain the bearer token, got: {rendered}"
    );
    // And the same for the builder that produced it.
    let rendered = format!("{builder:?}");
    assert!(!rendered.contains(SECRET), "got: {rendered}");
}
