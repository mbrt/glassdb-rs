//! Behavioral tests for the S3 backend, run against the pure-Rust in-process
//! fake S3 server in [`crate::fake_server`] (the analog of the Go tests'
//! `gofakes3` + `httptest.Server`).

use std::io;
use std::time::Duration;

use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::error::{ConnectorError, ErrorMetadata, SdkError};
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::primitives::SdkBody;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::http::StatusCode;
use glassdb_backend::{Backend, BackendError, ListCursor, ListLimit, Version};
use hyper::Method;

use crate::fake_server::FakeS3;
use crate::{
    Builder, ConditionalPutAction, ConditionalPutEvent, ConditionalPutState, DEFAULT_MAX_ATTEMPTS,
    MAX_CONFLICT_RETRIES, ProviderFact, ProviderFailure, S3Backend, annotate, annotate_list,
    annotate_read, conflict_backoff,
};

// ---------------------------------------------------------------------------
// Backend construction
// ---------------------------------------------------------------------------

fn backend(fake: &FakeS3) -> S3Backend {
    fake.backend("test")
}

fn builder(fake: &FakeS3) -> Builder {
    S3Backend::builder(fake.client(), "test")
}

/// A standard retryer that retries the same errors as the default (incl. 503
/// SlowDown) but with negligible backoff, keeping the tests quick.
fn fast_retry() -> RetryConfig {
    RetryConfig::standard()
        .with_max_attempts(5)
        .with_initial_backoff(std::time::Duration::from_millis(1))
        .with_max_backoff(std::time::Duration::from_millis(1))
}

fn raw_response(status: u16) -> HttpResponse {
    HttpResponse::new(StatusCode::try_from(status).unwrap(), SdkBody::empty())
}

fn service_failure(code: Option<&str>, status: u16) -> SdkError<PutObjectError> {
    let mut metadata = ErrorMetadata::builder();
    if let Some(code) = code {
        metadata = metadata.code(code);
    }
    SdkError::service_error(
        PutObjectError::generic(metadata.build()),
        raw_response(status),
    )
}

fn test_io_error() -> io::Error {
    io::Error::other("test failure")
}

fn timeout_failure() -> SdkError<PutObjectError> {
    SdkError::timeout_error(test_io_error())
}

fn dispatch_failure() -> SdkError<PutObjectError> {
    SdkError::dispatch_failure(ConnectorError::io(Box::new(test_io_error())))
}

fn response_failure() -> SdkError<PutObjectError> {
    SdkError::response_error(test_io_error(), raw_response(400))
}

fn construction_failure() -> SdkError<PutObjectError> {
    SdkError::construction_failure(test_io_error())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicError {
    Precondition,
    NotFound,
    Unavailable,
    InvalidCursor,
    Other,
}

fn public_error(error: &BackendError) -> PublicError {
    match error {
        BackendError::Precondition => PublicError::Precondition,
        BackendError::NotFound => PublicError::NotFound,
        BackendError::Unavailable(_) => PublicError::Unavailable,
        BackendError::InvalidCursor => PublicError::InvalidCursor,
        BackendError::Other { .. } => PublicError::Other,
    }
}

#[test]
fn http_status_provider_facts_preserve_public_errors() {
    for (status, fact, definitive, read) in [
        (
            304,
            ProviderFact::Precondition,
            PublicError::Precondition,
            PublicError::Precondition,
        ),
        (
            400,
            ProviderFact::Other,
            PublicError::Other,
            PublicError::Other,
        ),
        (
            404,
            ProviderFact::NotFound,
            PublicError::NotFound,
            PublicError::NotFound,
        ),
        (
            409,
            ProviderFact::Conflict,
            PublicError::Precondition,
            PublicError::Precondition,
        ),
        (
            412,
            ProviderFact::Precondition,
            PublicError::Precondition,
            PublicError::Precondition,
        ),
        (
            429,
            ProviderFact::Throttle,
            PublicError::Other,
            PublicError::Unavailable,
        ),
        (
            500,
            ProviderFact::Ambiguous,
            PublicError::Other,
            PublicError::Unavailable,
        ),
        (
            502,
            ProviderFact::Ambiguous,
            PublicError::Other,
            PublicError::Unavailable,
        ),
        (
            503,
            ProviderFact::Throttle,
            PublicError::Other,
            PublicError::Unavailable,
        ),
        (
            504,
            ProviderFact::Ambiguous,
            PublicError::Other,
            PublicError::Unavailable,
        ),
    ] {
        let failure = ProviderFailure::from(service_failure(None, status));
        assert_eq!(failure.fact, fact, "status {status}");
        assert_eq!(
            public_error(&annotate("Test", "path", failure)),
            definitive,
            "definitive status {status}"
        );
        assert_eq!(
            public_error(&annotate_read(
                "Test",
                "path",
                service_failure(None, status)
            )),
            read,
            "read status {status}"
        );
    }
}

#[test]
fn service_code_provider_facts_preserve_public_errors() {
    for (code, fact, definitive, read) in [
        (
            "PreconditionFailed",
            ProviderFact::Precondition,
            PublicError::Precondition,
            PublicError::Precondition,
        ),
        (
            "ConditionalRequestConflict",
            ProviderFact::Conflict,
            PublicError::Precondition,
            PublicError::Precondition,
        ),
        (
            "SlowDown",
            ProviderFact::Throttle,
            PublicError::Other,
            PublicError::Unavailable,
        ),
        (
            "ThrottlingException",
            ProviderFact::Throttle,
            PublicError::Other,
            PublicError::Unavailable,
        ),
        (
            "NoSuchKey",
            ProviderFact::NotFound,
            PublicError::NotFound,
            PublicError::NotFound,
        ),
        (
            "NotFound",
            ProviderFact::NotFound,
            PublicError::NotFound,
            PublicError::NotFound,
        ),
        (
            "NoSuchBucket",
            ProviderFact::NotFound,
            PublicError::NotFound,
            PublicError::NotFound,
        ),
        (
            "AccessDenied",
            ProviderFact::Other,
            PublicError::Other,
            PublicError::Other,
        ),
    ] {
        let failure = ProviderFailure::from(service_failure(Some(code), 400));
        assert_eq!(failure.fact, fact, "code {code}");
        assert_eq!(
            public_error(&annotate("Test", "path", failure)),
            definitive,
            "definitive code {code}"
        );
        assert_eq!(
            public_error(&annotate_read(
                "Test",
                "path",
                service_failure(Some(code), 400)
            )),
            read,
            "read code {code}"
        );
    }
}

#[test]
fn transport_provider_facts_preserve_public_errors() {
    type FailureFactory = fn() -> SdkError<PutObjectError>;
    for (name, failure, fact, definitive, read) in [
        (
            "timeout",
            timeout_failure as FailureFactory,
            ProviderFact::Ambiguous,
            PublicError::Other,
            PublicError::Unavailable,
        ),
        (
            "dispatch",
            dispatch_failure as FailureFactory,
            ProviderFact::Ambiguous,
            PublicError::Other,
            PublicError::Unavailable,
        ),
        (
            "response",
            response_failure as FailureFactory,
            ProviderFact::Ambiguous,
            PublicError::Other,
            PublicError::Unavailable,
        ),
        (
            "construction",
            construction_failure as FailureFactory,
            ProviderFact::Other,
            PublicError::Other,
            PublicError::Other,
        ),
    ] {
        let normalized = ProviderFailure::from(failure());
        assert_eq!(normalized.fact, fact, "transport {name}");
        assert_eq!(
            public_error(&annotate("Test", "path", normalized)),
            definitive,
            "definitive transport {name}"
        );
        assert_eq!(
            public_error(&annotate_read("Test", "path", failure())),
            read,
            "read transport {name}"
        );
    }
}

#[test]
fn list_cursor_errors_use_normalized_metadata() {
    for code in [
        "InvalidArgument",
        "InvalidToken",
        "InvalidContinuationToken",
    ] {
        assert_eq!(
            public_error(&annotate_list(
                "prefix/",
                true,
                service_failure(Some(code), 403)
            )),
            PublicError::InvalidCursor,
            "code {code}"
        );
        assert_eq!(
            public_error(&annotate_list(
                "prefix/",
                false,
                service_failure(Some(code), 403)
            )),
            PublicError::Other,
            "code {code} without cursor"
        );
    }
    assert_eq!(
        public_error(&annotate_list("prefix/", true, service_failure(None, 400))),
        PublicError::InvalidCursor,
        "status 400"
    );
}

#[derive(Clone, Copy, Debug)]
enum PutEvent {
    Applied,
    AppliedWithoutVersion,
    Failed(ProviderFact),
}

impl PutEvent {
    fn into_event(self) -> ConditionalPutEvent<PutObjectError> {
        match self {
            PutEvent::Applied => ConditionalPutEvent::Applied(Version::new("\"version\"")),
            PutEvent::AppliedWithoutVersion => ConditionalPutEvent::AppliedWithoutVersion,
            PutEvent::Failed(fact) => {
                let (code, status) = match fact {
                    ProviderFact::Precondition => (Some("PreconditionFailed"), 412),
                    ProviderFact::Conflict => (Some("ConditionalRequestConflict"), 409),
                    ProviderFact::Throttle => (Some("SlowDown"), 503),
                    ProviderFact::Ambiguous => (None, 500),
                    ProviderFact::NotFound => (Some("NoSuchKey"), 404),
                    ProviderFact::Other => (Some("AccessDenied"), 403),
                };
                ConditionalPutEvent::Failed(Box::new(ProviderFailure::from(service_failure(
                    code, status,
                ))))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PutAction {
    Return,
    Retry(Duration),
    Precondition,
    InDoubt,
    Terminal(ProviderFact),
}

fn put_action(action: ConditionalPutAction<PutObjectError>) -> PutAction {
    match action {
        ConditionalPutAction::Return(_) => PutAction::Return,
        ConditionalPutAction::Retry(after) => PutAction::Retry(after),
        ConditionalPutAction::Precondition => PutAction::Precondition,
        ConditionalPutAction::InDoubt => PutAction::InDoubt,
        ConditionalPutAction::Terminal(failure) => PutAction::Terminal(failure.fact),
    }
}

#[test]
fn conditional_put_transition_table() {
    for (name, events, actions, may_have_applied) in [
        (
            "success",
            &[PutEvent::Applied][..],
            &[PutAction::Return][..],
            false,
        ),
        (
            "success without version",
            &[PutEvent::AppliedWithoutVersion],
            &[PutAction::InDoubt],
            false,
        ),
        (
            "clean precondition",
            &[PutEvent::Failed(ProviderFact::Precondition)],
            &[PutAction::Precondition],
            false,
        ),
        (
            "ambiguity followed by precondition",
            &[
                PutEvent::Failed(ProviderFact::Ambiguous),
                PutEvent::Failed(ProviderFact::Precondition),
            ],
            &[PutAction::Retry(conflict_backoff(0)), PutAction::InDoubt],
            true,
        ),
        (
            "throttle followed by precondition",
            &[
                PutEvent::Failed(ProviderFact::Throttle),
                PutEvent::Failed(ProviderFact::Precondition),
            ],
            &[
                PutAction::Retry(conflict_backoff(0)),
                PutAction::Precondition,
            ],
            false,
        ),
        (
            "terminal failure",
            &[PutEvent::Failed(ProviderFact::Other)],
            &[PutAction::Terminal(ProviderFact::Other)],
            false,
        ),
        (
            "terminal failure after ambiguity",
            &[
                PutEvent::Failed(ProviderFact::Ambiguous),
                PutEvent::Failed(ProviderFact::Other),
            ],
            &[PutAction::Retry(conflict_backoff(0)), PutAction::InDoubt],
            true,
        ),
    ] {
        let mut state = ConditionalPutState::default();
        let actual: Vec<_> = events
            .iter()
            .map(|event| put_action(state.transition(event.into_event())))
            .collect();

        assert_eq!(actual, actions, "{name}");
        assert_eq!(state.attempts, events.len() as u32, "{name}");
        assert_eq!(state.may_have_applied, may_have_applied, "{name}");
    }
}

#[test]
fn conditional_put_retry_exhaustion_table() {
    for (name, fact, retries, exhausted, may_have_applied) in [
        (
            "conflict",
            ProviderFact::Conflict,
            MAX_CONFLICT_RETRIES,
            PutAction::Precondition,
            false,
        ),
        (
            "throttle",
            ProviderFact::Throttle,
            DEFAULT_MAX_ATTEMPTS,
            PutAction::Terminal(ProviderFact::Throttle),
            false,
        ),
        (
            "ambiguity",
            ProviderFact::Ambiguous,
            DEFAULT_MAX_ATTEMPTS,
            PutAction::InDoubt,
            true,
        ),
    ] {
        let mut state = ConditionalPutState::default();
        for retry in 0..retries {
            assert_eq!(
                put_action(state.transition(PutEvent::Failed(fact).into_event())),
                PutAction::Retry(conflict_backoff(retry)),
                "{name} retry {retry}"
            );
        }
        assert_eq!(
            put_action(state.transition(PutEvent::Failed(fact).into_event())),
            exhausted,
            "{name} exhausted"
        );
        assert_eq!(state.attempts, retries + 1, "{name}");
        assert_eq!(state.may_have_applied, may_have_applied, "{name}");
    }
}

#[test]
fn conditional_put_retry_budget_is_shared_across_provider_facts() {
    let mut state = ConditionalPutState::default();
    for retry in 0..DEFAULT_MAX_ATTEMPTS {
        assert_eq!(
            put_action(state.transition(PutEvent::Failed(ProviderFact::Throttle).into_event())),
            PutAction::Retry(conflict_backoff(retry))
        );
    }

    assert_eq!(
        put_action(state.transition(PutEvent::Failed(ProviderFact::Ambiguous).into_event())),
        PutAction::Terminal(ProviderFact::Ambiguous)
    );
    assert!(!state.may_have_applied);
}

// ---------------------------------------------------------------------------
// Tests (ported from backend/s3/s3_test.go)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_returns_value_and_version() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    for (name, value) in [
        ("non-empty", b"hello world".to_vec()),
        ("empty", Vec::new()),
        ("binary", vec![0x00, 0x01, 0x02, 0xff]),
    ] {
        let version = b.write_if_not_exists(name, value.clone()).await.unwrap();
        assert!(!version.is_unset());

        let r = b.read(name).await.unwrap();
        assert_eq!(r.contents, value, "case {name}");
        assert_eq!(r.version, version, "case {name}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identical_content_keeps_version() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    // With ADR-023 the body itself drives the ETag (no nonce), so re-uploading
    // identical bytes yields the same version, exactly as real S3 behaves.
    let v1 = b.write_if_not_exists("k", b"same".to_vec()).await.unwrap();
    let v2 = b.write_if("k", b"same".to_vec(), &v1).await.unwrap();
    assert_eq!(v1, v2);

    // Distinct content yields a distinct version.
    let v3 = b.write_if("k", b"other".to_vec(), &v2).await.unwrap();
    assert_ne!(v1, v3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_if_not_exists() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    b.write_if_not_exists("k", b"a".to_vec()).await.unwrap();
    let err = b.write_if_not_exists("k", b"b".to_vec()).await.unwrap_err();
    assert!(matches!(err, BackendError::Precondition));
    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"a");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_if_cas() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let v0 = b.write_if_not_exists("k", b"a".to_vec()).await.unwrap();

    let err = b
        .write_if("k", b"b".to_vec(), &Version::new("\"stale\""))
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::Precondition));

    let v1 = b.write_if("k", b"b".to_vec(), &v0).await.unwrap();
    assert_ne!(v0, v1);
    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"b");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_if_null_version_fails_precondition() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let v0 = b.write_if_not_exists("k", b"a".to_vec()).await.unwrap();

    // A null expected version has an empty token; it must fail rather than
    // overwrite unconditionally.
    let err = b
        .write_if("k", b"b".to_vec(), &Version::default())
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::Precondition));

    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"a");
    assert_eq!(r.version, v0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_if_modified() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let v0 = b.write_if_not_exists("k", b"x".to_vec()).await.unwrap();

    // The cached version still matches: revalidation reports Precondition (the
    // 304 Not Modified path) instead of transferring the body.
    let err = b.read_if_modified("k", &v0).await.unwrap_err();
    assert!(matches!(err, BackendError::Precondition));

    // A stale (different) version: the current object is returned in full.
    let r = b
        .read_if_modified("k", &Version::new("\"other\""))
        .await
        .unwrap();
    assert_eq!(r.contents, b"x");
    assert_eq!(r.version, v0);

    // After a content change the cached version no longer matches, so the new
    // value is returned.
    let v1 = b.write_if("k", b"y".to_vec(), &v0).await.unwrap();
    let r = b.read_if_modified("k", &v0).await.unwrap();
    assert_eq!(r.contents, b"y");
    assert_eq!(r.version, v1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_if_modified_unset_version_reads() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let v0 = b.write_if_not_exists("k", b"x".to_vec()).await.unwrap();

    // An unset expected version has nothing to revalidate against, so it behaves
    // like a plain read.
    let r = b.read_if_modified("k", &Version::default()).await.unwrap();
    assert_eq!(r.contents, b"x");
    assert_eq!(r.version, v0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_if_matching_etag() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let version = b.write_if_not_exists("k", b"x".to_vec()).await.unwrap();
    b.delete_if("k", &version).await.unwrap();
    let err = b.read("k").await.unwrap_err();
    assert!(matches!(err, BackendError::NotFound));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_if_missing_is_not_found() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);

    let error = b
        .delete_if("missing", &Version::new("\"old\""))
        .await
        .unwrap_err();

    assert!(matches!(error, BackendError::NotFound));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_delete_if_preserves_current_object() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let old = b.write_if_not_exists("k", b"old".to_vec()).await.unwrap();
    let current = b.write_if("k", b"current".to_vec(), &old).await.unwrap();

    let err = b.delete_if("k", &old).await.unwrap_err();
    assert!(matches!(err, BackendError::Precondition));
    let read = b.read("k").await.unwrap();
    assert_eq!(read.contents, b"current");
    assert_eq!(read.version, current);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_not_found() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let err = b.read("missing").await.unwrap_err();
    assert!(matches!(err, BackendError::NotFound));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_is_recursive_and_paginated() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    for name in ["d/a/1", "d/a/2", "d/a/b/1", "d/c/1", "d/root"] {
        b.write_if_not_exists(name, name.as_bytes().to_vec())
            .await
            .unwrap();
    }
    let limit = ListLimit::new(2).unwrap();
    let first = b.list("d/", None, limit).await.unwrap();
    assert_eq!(first.objects, vec!["d/a/1", "d/a/2"]);
    let second = b.list("d/", first.next.as_ref(), limit).await.unwrap();
    assert_eq!(second.objects, vec!["d/a/b/1", "d/c/1"]);
    let third = b.list("d/", second.next.as_ref(), limit).await.unwrap();
    assert_eq!(third.objects, vec!["d/root"]);
    assert!(third.next.is_none());

    let err = b
        .list("d/", Some(&ListCursor::new("invalid")), limit)
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::InvalidCursor));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_write_retries_through_slow_down() {
    let fake = FakeS3::start().await;
    let b = builder(&fake).retry_config(fast_retry()).build();
    fake.set_slowdown(2, Some(Method::PUT));

    b.write_if_not_exists("k", b"v".to_vec()).await.unwrap();
    assert_eq!(fake.slowdown_remaining(), 0);

    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"v");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_retries_through_slow_down() {
    let fake = FakeS3::start().await;
    let b = builder(&fake).retry_config(fast_retry()).build();

    // The write is a PUT, so it is not throttled here.
    b.write_if_not_exists("k", b"v".to_vec()).await.unwrap();

    fake.set_slowdown(2, Some(Method::GET));
    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"v");
    assert_eq!(fake.slowdown_remaining(), 0);
}

// Transient read unavailability (ADR-009): a read is idempotent, so a transient
// failure the SDK retryer does not ride over (here a `503 SlowDown` with retries
// disabled) must surface as retryable `Unavailable`, letting the engine recover
// it in place — not as the generic `Other` the pre-fix code produced for a 503
// on a read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_transient_failure_surfaces_unavailable() {
    let fake = FakeS3::start().await;
    let b = builder(&fake).disable_retries().build();

    // Seed via PUT (not throttled), then throttle the next GET.
    b.write_if_not_exists("k", b"v".to_vec()).await.unwrap();
    fake.set_slowdown(1, Some(Method::GET));

    let err = b.read("k").await.unwrap_err();
    assert!(
        matches!(err, BackendError::Unavailable(_)),
        "a 503 on an idempotent read must be Unavailable, got {err:?}"
    );

    // The object is intact; once the throttle clears the read succeeds.
    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"v");
}

// In-doubt contract (ADR-009): a conditional write whose ack is lost must NOT be
// reported as a confident `Precondition`. Object storage has no at-most-once
// request id, so when the SDK (or any layer) re-sends a conditional PUT whose
// first attempt landed, the retry observes a precondition failure for its own
// write that is indistinguishable from a real conflict. The S3 backend therefore
// owns the conditional-write retry loop and surfaces such an outcome as
// `Unavailable`; the engine then fails the transaction in-doubt rather than
// retrying it into a double-apply. These tests would see `Precondition` against
// the pre-fix code (which let the SDK retryer mask the lost ack).

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_if_not_exists_lost_ack_is_in_doubt() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);

    // The create lands, but its ack is lost; the re-send sees the object exists
    // and gets 412.
    fake.set_lost_ack(1);
    let err = b.write_if_not_exists("k", b"v".to_vec()).await.unwrap_err();
    assert!(
        matches!(err, BackendError::Unavailable(_)),
        "expected Unavailable (in-doubt), got {err:?}"
    );

    // The first attempt really did persist the object.
    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"v");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_if_lost_ack_is_in_doubt() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let v0 = b.write_if_not_exists("k", b"a".to_vec()).await.unwrap();

    // The CAS write lands (changing the ETag), but its ack is lost; the re-send's
    // If-Match no longer matches and gets 412.
    fake.set_lost_ack(1);
    let err = b.write_if("k", b"b".to_vec(), &v0).await.unwrap_err();
    assert!(
        matches!(err, BackendError::Unavailable(_)),
        "expected Unavailable (in-doubt), got {err:?}"
    );

    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"b");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_if_lost_ack_is_in_doubt() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let version = b.write_if_not_exists("k", b"v".to_vec()).await.unwrap();

    fake.set_lost_ack(1);
    let err = b.delete_if("k", &version).await.unwrap_err();
    assert!(matches!(err, BackendError::Unavailable(_)));
    assert!(matches!(b.read("k").await, Err(BackendError::NotFound)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clean_conflict_still_precondition() {
    // Guard against over-eagerly tainting: a genuine conflict with no lost ack
    // must still be a retryable `Precondition`, not in-doubt.
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    b.write_if_not_exists("k", b"a".to_vec()).await.unwrap();
    let err = b.write_if_not_exists("k", b"b".to_vec()).await.unwrap_err();
    assert!(matches!(err, BackendError::Precondition), "got {err:?}");
}
