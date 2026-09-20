//! Authentication, end to end against the in-process server and the mock
//! identity provider: the header the interceptor attaches, the automatic
//! refresh-and-retry on `UNAUTHENTICATED` (on unary calls, on `upload_file`
//! and on the call that opens a download), the pre-flight refresh every
//! streaming RPC gets, the out-of-band refresh methods, the background refresh
//! backoff, and the two ways `build()` can fail.

mod support;

use rociadb_sdk::{
    Bytes, FileStreamUploadOptions, FileUploadOptions, RociaDbBuilder, RociaDbError, UploadRequest,
    WriteOptions,
};
use serde_json::{Value, json};
use std::time::Duration;
use support::{FakeServer, MockIdp, payload, sha256};
use tonic::Code;

const TENANT: &str = "tenant-1";
const BUCKET: &str = "assets";

/// `expires_in` for the tests of the pre-flight refresh.
///
/// Four seconds is chosen so that the very first streaming call is already
/// inside the SDK's five-second refresh margin — the pre-flight refresh fires
/// with no real-time waiting at all — while the *background* refresh task,
/// whose cadence is `min(max(expires_in * 2 / 3, 5s), expires_in - 1)` and so
/// three seconds here, has not yet had a chance to fire. That keeps "exactly
/// one refresh, and it happened before the call" an exact assertion: with the
/// 1-2 second lifetime the same scenario could also be built from, the
/// background task would tick once a second and every request count would be a
/// race. The "already expired" and coalescing cases are covered without a
/// clock at all by the unit tests of `TokenManager::ensure_fresh`.
const INSIDE_THE_REFRESH_MARGIN_SECS: u64 = 4;

/// Poll the server with cheap reads until the bearer header it records is no
/// longer `previous`, and return the header that replaced it.
///
/// A refreshed token becomes observable in two steps: the provider answers
/// (which is what [`MockIdp::wait_for_requests`] sees) and, a few milliseconds
/// later, the client parses the body and installs the new header. A test that
/// called the server between those two steps would still carry the old token
/// — so this polls the only thing the assertion is really about, the header
/// actually attached to a call, and gives up loudly after `timeout`.
async fn wait_for_token_in_use(
    client: &rociadb_sdk::RociaDbClient,
    server: &FakeServer,
    previous: &str,
    timeout: Duration,
) -> String {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        client
            .list_tenants(None, None)
            .await
            .expect("the read must succeed");
        let header = server
            .authorizations()
            .last()
            .cloned()
            .flatten()
            .expect("the call must carry a bearer header");
        if header != previous {
            return header;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the token in use was still {previous} after {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

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

/// `reqwest` appends `" for url (..)"` to every transport and status error it
/// produces, so a failed token fetch used to carry the whole `token_url` in the
/// message the *caller* sees — not merely in one of the SDK's own `warn!`
/// lines. AGENTS.md puts `token_url` on the never-log list, and an error the
/// caller is expected to log is the widest leak of the lot, so the URL is
/// stripped where the `reqwest` error is wrapped.
#[tokio::test]
async fn a_failed_explicit_refresh_does_not_carry_the_token_url_in_its_error() {
    let idp = MockIdp::start().await;
    let server = FakeServer::start().await;
    let client = server.authenticated_client(&idp).await;
    idp.fail_next(1, 500);

    let error = client
        .refresh_auth_token()
        .await
        .expect_err("a 500 from the token endpoint must fail an explicit refresh");

    let token_url = idp.token_url();
    let host = token_url
        .trim_start_matches("http://")
        .trim_end_matches("/token")
        .to_string();
    // Walk the whole chain: `RociaDbError::Auth` interpolates its source, and a
    // caller reporting an error commonly walks `source()` as well.
    let mut rendered = vec![format!("{error}"), format!("{error:?}")];
    let mut source: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(&error);
    while let Some(current) = source {
        rendered.push(format!("{current}"));
        source = current.source();
    }
    for text in &rendered {
        assert!(
            !text.contains(&token_url) && !text.contains(&host),
            "the token endpoint must not appear in a failed refresh's error, got: {text}"
        );
    }
    // The error still has to say what went wrong, or stripping the URL would
    // have cost the caller their diagnosis.
    assert!(matches!(error, RociaDbError::Auth { .. }), "got: {error:?}");
    assert!(
        rendered[0].to_ascii_lowercase().contains("token"),
        "the message must still name what failed, got: {}",
        rendered[0]
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
async fn one_unauthenticated_response_replays_upload_file_with_the_same_request_id() {
    let idp = MockIdp::start().await;
    let server = FakeServer::start().await;
    let client = server.authenticated_client(&idp).await;
    let bytes = payload(3000);
    server.fail_next("Upload", 1, Code::Unauthenticated);

    // `upload_file` owns its buffer, so it can rebuild the request stream and
    // re-send: the caller sees none of this.
    client
        .upload_file(
            TENANT,
            BUCKET,
            "replayed.bin",
            bytes.clone(),
            FileUploadOptions::new().with_request_id("upload-key-replay"),
        )
        .await
        .expect("the replayed upload must succeed transparently");

    assert_eq!(idp.requests(), 2, "exactly one refresh");
    let uploads = server.calls_for("Upload");
    assert_eq!(uploads.len(), 2, "the upload is issued twice, no more");
    assert_eq!(uploads[0].authorization.as_deref(), Some("Bearer token-1"));
    assert_eq!(
        uploads[1].authorization.as_deref(),
        Some("Bearer token-2"),
        "the replay must carry the *new* token, not the rejected one"
    );
    assert!(
        uploads
            .iter()
            .all(|call| call.request_id.as_deref() == Some("upload-key-replay")),
        "both attempts must carry the same idempotency key, which is what makes the replay safe: \
         {uploads:?}"
    );

    // Stored once, with the right bytes: the replay did not append to or
    // duplicate what the first attempt sent.
    let stored = server
        .stored_file(TENANT, BUCKET, "replayed.bin")
        .expect("the file must be stored");
    assert_eq!(stored.bytes, bytes);
    assert_eq!(stored.size_bytes, bytes.len() as u64);
    assert_eq!(
        client
            .list_files(TENANT, BUCKET, None, None)
            .await
            .expect("listing must succeed")
            .items,
        vec!["replayed.bin".to_string()],
        "the bucket must hold exactly one file"
    );
}

#[tokio::test]
async fn a_second_unauthenticated_response_fails_upload_file_after_one_refresh() {
    let idp = MockIdp::start().await;
    let server = FakeServer::start().await;
    let client = server.authenticated_client(&idp).await;
    server.fail_next("Upload", 2, Code::Unauthenticated);

    let error = client
        .upload_file(
            TENANT,
            BUCKET,
            "rejected.bin",
            payload(16),
            FileUploadOptions::new(),
        )
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
        server.call_count("Upload"),
        2,
        "the upload is replayed exactly once"
    );
    assert!(server.stored_file(TENANT, BUCKET, "rejected.bin").is_none());
}

#[tokio::test]
async fn one_unauthenticated_response_replays_the_call_that_opens_a_download() {
    let idp = MockIdp::start().await;
    let server = FakeServer::start().await;
    let client = server.authenticated_client(&idp).await;
    let bytes = payload(70_000); // more than one download chunk
    client
        .upload_file(
            TENANT,
            BUCKET,
            "downloaded.bin",
            bytes.clone(),
            FileUploadOptions::new(),
        )
        .await
        .expect("seeding must succeed");
    server.clear_calls();

    // A server that rejects a server-streaming call does it before any message
    // exists, so the rejection resolves the opening call itself — which is
    // exactly what makes it replayable.
    server.fail_next("Download", 1, Code::Unauthenticated);
    assert_eq!(
        client
            .download_file(TENANT, BUCKET, "downloaded.bin")
            .await
            .expect("the replayed download must succeed transparently"),
        bytes
    );

    // And the same for the verifying variant, whose `stat_file` is an ordinary
    // unary call and is left alone here.
    server.fail_next("Download", 1, Code::Unauthenticated);
    assert_eq!(
        client
            .download_file_verified(TENANT, BUCKET, "downloaded.bin")
            .await
            .expect("the replayed verified download must succeed too"),
        bytes
    );

    assert_eq!(idp.requests(), 3, "one refresh per rejected download");
    let downloads = server.calls_for("Download");
    assert_eq!(
        downloads.len(),
        4,
        "two downloads, each issued exactly twice"
    );
    let carried: Vec<Option<&str>> = downloads
        .iter()
        .map(|call| call.authorization.as_deref())
        .collect();
    assert_eq!(
        carried,
        vec![
            Some("Bearer token-1"),
            Some("Bearer token-2"),
            Some("Bearer token-2"),
            Some("Bearer token-3"),
        ],
        "each replay must carry the token minted for it"
    );
}

#[tokio::test]
async fn a_second_unauthenticated_response_fails_a_download_after_one_refresh() {
    let idp = MockIdp::start().await;
    let server = FakeServer::start().await;
    let client = server.authenticated_client(&idp).await;
    client
        .upload_file(
            TENANT,
            BUCKET,
            "unreachable.bin",
            payload(32),
            FileUploadOptions::new(),
        )
        .await
        .expect("seeding must succeed");
    server.clear_calls();
    server.fail_next("Download", 2, Code::Unauthenticated);

    let error = client
        .download_file(TENANT, BUCKET, "unreachable.bin")
        .await
        .expect_err("a credential the server keeps rejecting must reach the caller");
    assert!(error.is_unauthenticated(), "got: {error}");
    assert_eq!(error.reason(), Some("unauthenticated"));
    assert_eq!(idp.requests(), 2, "exactly one refresh");
    assert_eq!(
        server.call_count("Download"),
        2,
        "the download is replayed exactly once"
    );
}

#[tokio::test]
async fn a_nearly_expired_token_is_refreshed_before_upload_file_chunked_opens_the_call() {
    // `upload_file_chunked` gets no replay — its source is the caller's stream
    // — so a server that accepts only the *new* token proves the refresh
    // happened before the call, not after a rejection.
    let idp = MockIdp::start_with_expires_in(INSIDE_THE_REFRESH_MARGIN_SECS).await;
    let server = FakeServer::start().await;
    let client = server.authenticated_client(&idp).await;
    assert_eq!(idp.requests(), 1, "build() issued token-1");
    server.accept_only_tokens(["token-2"]);
    let bytes = payload(2048);

    client
        .upload_file_chunked(
            TENANT,
            BUCKET,
            "preflight-chunked.bin",
            futures::stream::iter(vec![Ok(Bytes::from(bytes.clone()))]),
            FileStreamUploadOptions::new(bytes.len() as u64, sha256(&bytes)),
        )
        .await
        .expect("the pre-flight refresh must let the upload through on the first attempt");

    assert_eq!(idp.requests(), 2, "exactly one pre-flight refresh");
    let upload = server.only_call("Upload");
    assert_eq!(
        upload.authorization.as_deref(),
        Some("Bearer token-2"),
        "the one and only attempt must already carry the refreshed token"
    );
    assert_eq!(
        server
            .stored_file(TENANT, BUCKET, "preflight-chunked.bin")
            .expect("the file must be stored")
            .bytes,
        bytes
    );
}

#[tokio::test]
async fn a_nearly_expired_token_is_refreshed_before_upload_file_stream_opens_the_call() {
    let idp = MockIdp::start_with_expires_in(INSIDE_THE_REFRESH_MARGIN_SECS).await;
    let server = FakeServer::start().await;
    let client = server.authenticated_client(&idp).await;
    server.accept_only_tokens(["token-2"]);
    let bytes = payload(512);

    client
        .upload_file_stream(futures::stream::iter(vec![UploadRequest {
            tenant_id: TENANT.to_string(),
            bucket: BUCKET.to_string(),
            file_id: "preflight-raw.bin".to_string(),
            size_bytes: bytes.len() as u64,
            content_type: "application/octet-stream".to_string(),
            checksum: sha256(&bytes).to_vec(),
            chunk: bytes.clone(),
            request_id: "preflight-raw-key".to_string(),
        }]))
        .await
        .expect("the pre-flight refresh must let the raw upload through too");

    assert_eq!(idp.requests(), 2, "exactly one pre-flight refresh");
    assert_eq!(
        server.only_call("Upload").authorization.as_deref(),
        Some("Bearer token-2")
    );
}

#[tokio::test]
async fn a_nearly_expired_token_is_refreshed_before_a_download_opens() {
    let idp = MockIdp::start_with_expires_in(INSIDE_THE_REFRESH_MARGIN_SECS).await;
    let server = FakeServer::start().await;
    let client = server.authenticated_client(&idp).await;
    let bytes = payload(1024);
    // Seeded against a server that accepts anything: the seeding upload has a
    // pre-flight refresh of its own (its token is inside the margin too), and
    // pinning a token here would only test that one twice over.
    client
        .upload_file(
            TENANT,
            BUCKET,
            "preflight-download.bin",
            bytes.clone(),
            FileUploadOptions::new(),
        )
        .await
        .expect("seeding must succeed");
    // The seeding upload's own pre-flight refresh already minted token-2, so
    // the download's pre-flight is the one that mints token-3.
    let issued_before = idp.requests();
    assert_eq!(idp.issued_tokens(), vec!["token-1", "token-2"]);
    server.accept_only_tokens(["token-3"]);
    server.clear_calls();

    assert_eq!(
        client
            .download_file(TENANT, BUCKET, "preflight-download.bin")
            .await
            .expect("the pre-flight refresh must let the download open on the first attempt"),
        bytes
    );

    assert_eq!(
        idp.requests(),
        issued_before + 1,
        "exactly one pre-flight refresh for the download"
    );
    let download = server.only_call("Download");
    assert_eq!(
        download.authorization.as_deref(),
        Some("Bearer token-3"),
        "the refresh must have happened before the call, so no replay was needed"
    );
}

#[tokio::test]
async fn a_failed_pre_flight_refresh_lets_the_call_proceed_with_the_cached_token() {
    let idp = MockIdp::start_with_expires_in(INSIDE_THE_REFRESH_MARGIN_SECS).await;
    let server = FakeServer::start().await;
    let client = server.authenticated_client(&idp).await;
    // The cached token is still the one the server accepts, and the pre-flight
    // refresh that is about to be attempted fails.
    server.accept_only_tokens(["token-1"]);
    idp.fail_next(1, 500);
    let bytes = payload(256);

    // A failed pre-flight refresh is a `warn!`, not an error: the cached token
    // may well still work — as it does here — so the upload must go ahead with
    // it rather than failing for a refresh nobody asked for.
    client
        .upload_file_chunked(
            TENANT,
            BUCKET,
            "stale-but-valid.bin",
            futures::stream::iter(vec![Ok(Bytes::from(bytes.clone()))]),
            FileStreamUploadOptions::new(bytes.len() as u64, sha256(&bytes)),
        )
        .await
        .expect("a failed pre-flight refresh must not fail the upload");

    assert_eq!(idp.requests(), 2, "the refresh was attempted once");
    assert_eq!(
        idp.issued_tokens(),
        vec!["token-1".to_string()],
        "the failed refresh issued nothing"
    );
    assert_eq!(
        server.only_call("Upload").authorization.as_deref(),
        Some("Bearer token-1"),
        "the call must go out with the token that was already cached"
    );
    assert_eq!(
        server
            .stored_file(TENANT, BUCKET, "stale-but-valid.bin")
            .expect("the file must be stored")
            .bytes,
        bytes
    );
}

#[tokio::test]
async fn a_token_with_plenty_of_life_left_triggers_no_pre_flight_refresh() {
    // The complement of the four tests above, and the case that matters for
    // cost: with the provider's ordinary 600-second token, a streaming call
    // must not touch the identity provider at all.
    let idp = MockIdp::start().await;
    let server = FakeServer::start().await;
    let client = server.authenticated_client(&idp).await;
    assert_eq!(idp.requests(), 1);
    let bytes = payload(64);

    client
        .upload_file_chunked(
            TENANT,
            BUCKET,
            "fresh.bin",
            futures::stream::iter(vec![Ok(Bytes::from(bytes.clone()))]),
            FileStreamUploadOptions::new(bytes.len() as u64, sha256(&bytes)),
        )
        .await
        .expect("the upload must succeed");
    client
        .download_file(TENANT, BUCKET, "fresh.bin")
        .await
        .expect("the download must succeed");

    assert_eq!(
        idp.requests(),
        1,
        "a token nowhere near expiry must not be re-fetched before a streaming call"
    );
    assert_eq!(
        server.authorizations(),
        vec![
            Some("Bearer token-1".to_string()),
            Some("Bearer token-1".to_string())
        ]
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

    // The provider has answered, but the client installs the new header a
    // few milliseconds after that: poll the header in use rather than assume
    // the very next call already carries it.
    let header =
        wait_for_token_in_use(&client, &server, "Bearer token-1", Duration::from_secs(5)).await;
    assert_eq!(
        header, "Bearer token-2",
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

    // Request 3 being counted means the provider has answered, not that the
    // client has installed the token it carried — that happens a few
    // milliseconds later. Poll the header in use instead of assuming the very
    // next call already has the new one (with a two-second lifetime the token
    // keeps rotating, so only "no longer token-1" is asserted).
    let header =
        wait_for_token_in_use(&client, &server, "Bearer token-1", Duration::from_secs(5)).await;
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
