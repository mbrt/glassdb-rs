//! Behavioral tests for the S3 backend, run against the pure-Rust in-process
//! fake S3 server in [`crate::fake_server`] (the analog of the Go tests'
//! `gofakes3` + `httptest.Server`).

use aws_sdk_s3::config::retry::RetryConfig;
use glassdb_backend::{Backend, BackendError, ListCursor, ListLimit, Version};
use hyper::Method;

use crate::fake_server::FakeS3;
use crate::{Builder, S3Backend};

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
        let version = b.write(name, value.clone()).await.unwrap();
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
    let v1 = b.write("k", b"same".to_vec()).await.unwrap();
    let v2 = b.write("k", b"same".to_vec()).await.unwrap();
    assert_eq!(v1, v2);

    // Distinct content yields a distinct version.
    let v3 = b.write("k", b"other".to_vec()).await.unwrap();
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
    let v0 = b.write("k", b"a".to_vec()).await.unwrap();

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
    let v0 = b.write("k", b"a".to_vec()).await.unwrap();

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
    let v0 = b.write("k", b"x".to_vec()).await.unwrap();

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
    let v1 = b.write("k", b"y".to_vec()).await.unwrap();
    let r = b.read_if_modified("k", &v0).await.unwrap();
    assert_eq!(r.contents, b"y");
    assert_eq!(r.version, v1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_if_modified_unset_version_reads() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let v0 = b.write("k", b"x".to_vec()).await.unwrap();

    // An unset expected version has nothing to revalidate against, so it behaves
    // like a plain read.
    let r = b.read_if_modified("k", &Version::default()).await.unwrap();
    assert_eq!(r.contents, b"x");
    assert_eq!(r.version, v0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_removes_object() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    b.write("k", b"x".to_vec()).await.unwrap();
    b.delete("k").await.unwrap();
    let err = b.read("k").await.unwrap_err();
    assert!(matches!(err, BackendError::NotFound));
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
        b.write(name, name.as_bytes().to_vec()).await.unwrap();
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
async fn write_retries_through_slow_down() {
    let fake = FakeS3::start().await;
    let b = builder(&fake).retry_config(fast_retry()).build();
    fake.set_slowdown(2, Some(Method::PUT));

    b.write("k", b"v".to_vec()).await.unwrap();
    assert_eq!(fake.slowdown_remaining(), 0);

    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"v");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_retries_through_slow_down() {
    let fake = FakeS3::start().await;
    let b = builder(&fake).retry_config(fast_retry()).build();

    // The write is a PUT, so it is not throttled here.
    b.write("k", b"v".to_vec()).await.unwrap();

    fake.set_slowdown(2, Some(Method::GET));
    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"v");
    assert_eq!(fake.slowdown_remaining(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nop_retryer_surfaces_slow_down() {
    let fake = FakeS3::start().await;
    let b = builder(&fake).disable_retries().build();
    fake.set_slowdown(1, Some(Method::PUT));

    let err = b.write("k", b"v".to_vec()).await.unwrap_err();
    // An unclassified S3 failure is rendered through the structured request
    // error, so the request coordinates surface as typed fields under `{:?}`
    // (op/path/code/status), and the SDK error is kept as the underlying cause.
    let dbg = format!("{err:?}");
    assert!(dbg.contains(r#"op: "Write""#), "got: {dbg}");
    assert!(dbg.contains(r#"code: Some("SlowDown")"#), "got: {dbg}");
    assert!(dbg.contains("status: Some(503)"), "got: {dbg}");

    use std::error::Error as _;
    assert!(
        err.source().is_some(),
        "SDK error should be kept as the cause"
    );
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
    b.write("k", b"v".to_vec()).await.unwrap();
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
    let v0 = b.write("k", b"a".to_vec()).await.unwrap();

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
async fn clean_conflict_still_precondition() {
    // Guard against over-eagerly tainting: a genuine conflict with no lost ack
    // must still be a retryable `Precondition`, not in-doubt.
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    b.write_if_not_exists("k", b"a".to_vec()).await.unwrap();
    let err = b.write_if_not_exists("k", b"b".to_vec()).await.unwrap_err();
    assert!(matches!(err, BackendError::Precondition), "got {err:?}");
}
