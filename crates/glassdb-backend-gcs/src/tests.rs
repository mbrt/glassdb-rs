//! Behavioral tests for the GCS backend, run against the pure-Rust in-process
//! fake in test support.

mod support;

use glassdb_backend::{Backend, BackendError, Version};

use self::support::FakeGcs;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_write_roundtrip() {
    let fake = FakeGcs::start().await;
    let b = fake.backend();
    for (name, value) in [
        ("non-empty", b"hello world".to_vec()),
        ("empty", Vec::new()),
        ("binary", vec![0x00, 0x01, 0x02, 0xff]),
    ] {
        let version = b.write_if_not_exists(name, value.clone()).await.unwrap();
        assert!(!version.is_unset());

        let r = b.read(name).await.unwrap();
        assert_eq!(r.contents, value, "case {name}");
        assert_eq!(r.version, version);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_produces_fresh_version_each_time() {
    let fake = FakeGcs::start().await;
    let b = fake.backend();
    let v1 = b.write_if_not_exists("k", b"same".to_vec()).await.unwrap();
    let v2 = b.write_if("k", b"same".to_vec(), &v1).await.unwrap();
    assert_ne!(v1, v2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_if_not_exists() {
    let fake = FakeGcs::start().await;
    let b = fake.backend();
    b.write_if_not_exists("k", b"a".to_vec()).await.unwrap();
    let err = b.write_if_not_exists("k", b"b".to_vec()).await.unwrap_err();
    assert!(matches!(err, BackendError::Precondition));
    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"a");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_if_cas() {
    let fake = FakeGcs::start().await;
    let b = fake.backend();
    let v0 = b.write_if_not_exists("k", b"a".to_vec()).await.unwrap();

    let err = b
        .write_if("k", b"b".to_vec(), &Version::new("999"))
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
    let fake = FakeGcs::start().await;
    let b = fake.backend();
    let v0 = b.write_if_not_exists("k", b"a".to_vec()).await.unwrap();

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
    let fake = FakeGcs::start().await;
    let b = fake.backend();
    let v0 = b.write_if_not_exists("k", b"x".to_vec()).await.unwrap();

    // Unchanged generation => precondition (not modified).
    let err = b.read_if_modified("k", &v0).await.unwrap_err();
    assert!(matches!(err, BackendError::Precondition));

    // A stale version returns the current content and the fresh version.
    let r = b.read_if_modified("k", &Version::new("1")).await.unwrap();
    assert_eq!(r.contents, b"x");
    assert_eq!(r.version, v0);

    // After a content write the generation changes, so the old token no longer
    // matches and the body is returned.
    let v1 = b.write_if("k", b"y".to_vec(), &v0).await.unwrap();
    assert_ne!(v0, v1);
    let r = b.read_if_modified("k", &v0).await.unwrap();
    assert_eq!(r.contents, b"y");
    assert_eq!(r.version, v1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_if_matching_generation() {
    let fake = FakeGcs::start().await;
    let b = fake.backend();
    let version = b.write_if_not_exists("k", b"x".to_vec()).await.unwrap();
    b.delete_if("k", &version).await.unwrap();
    let err = b.read("k").await.unwrap_err();
    assert!(matches!(err, BackendError::NotFound));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_delete_if_preserves_current_object() {
    let fake = FakeGcs::start().await;
    let b = fake.backend();
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
    let fake = FakeGcs::start().await;
    let b = fake.backend();
    let err = b.read("missing").await.unwrap_err();
    assert!(matches!(err, BackendError::NotFound));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_is_recursive_and_paginated() {
    let fake = FakeGcs::start().await;
    let b = fake.backend();
    glassdb_backend::implementation::assert_list_conformance(&b).await;
}

// In-doubt contract (ADR-009): a conditional write whose outcome is uncertain
// must NOT be reported as a confident error the engine would retry into a
// double-apply. GCS applies conditional writes atomically and this backend does
// not retry them, so a clean precondition is a genuine conflict; but a `5xx`
// (or a transport error) leaves the write in doubt — it may have landed before
// the failure — and must surface as `Unavailable`. These tests would see
// `Other` against the pre-fix code, which mapped any non-precondition status to
// a generic error.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_if_not_exists_lost_ack_is_in_doubt() {
    let fake = FakeGcs::start().await;
    let b = fake.backend();

    // The create lands, but the server answers 500, hiding that it landed.
    fake.set_lost_ack(1);
    let err = b.write_if_not_exists("k", b"v".to_vec()).await.unwrap_err();
    assert!(
        matches!(err, BackendError::Unavailable(_)),
        "expected Unavailable (in-doubt), got {err:?}"
    );

    // The write really did persist.
    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"v");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_if_lost_ack_is_in_doubt() {
    let fake = FakeGcs::start().await;
    let b = fake.backend();
    let v0 = b.write_if_not_exists("k", b"a".to_vec()).await.unwrap();

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
    let fake = FakeGcs::start().await;
    let b = fake.backend();
    let version = b.write_if_not_exists("k", b"v".to_vec()).await.unwrap();

    fake.set_lost_ack(1);
    let err = b.delete_if("k", &version).await.unwrap_err();
    assert!(matches!(err, BackendError::Unavailable(_)));
    assert!(matches!(b.read("k").await, Err(BackendError::NotFound)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clean_conflict_still_precondition() {
    // A genuine conflict with no lost ack must stay a retryable `Precondition`.
    let fake = FakeGcs::start().await;
    let b = fake.backend();
    b.write_if_not_exists("k", b"a".to_vec()).await.unwrap();
    let err = b.write_if_not_exists("k", b"b".to_vec()).await.unwrap_err();
    assert!(matches!(err, BackendError::Precondition), "got {err:?}");
}

// Transient read unavailability: a read is idempotent, so a `5xx` (or transport
// error) on an idempotent request is always safe to retry (ADR-009). The backend
// classifies it as `Unavailable` rather than a generic `Other`, so the engine
// can recover the outage in place.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_server_error_surfaces_unavailable() {
    let fake = FakeGcs::start().await;
    let b = fake.backend();
    b.write_if_not_exists("k", b"v".to_vec()).await.unwrap();

    // The object stays durable, but the next metadata GET answers 500.
    fake.set_read_fault(1);
    let err = b.read("k").await.unwrap_err();
    assert!(
        matches!(err, BackendError::Unavailable(_)),
        "a 5xx on an idempotent read must be Unavailable, got {err:?}"
    );

    // Once the transient fault clears, the read succeeds against the durable
    // object — the failure never destroyed any data.
    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"v");
}

#[test]
fn read_5xx_is_unavailable() {
    use crate::check_status;

    // A `5xx` on an idempotent request maps to retryable `Unavailable`...
    for s in [
        reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        reqwest::StatusCode::BAD_GATEWAY,
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
    ] {
        let err = check_status(s, "Read", "k").unwrap_err();
        assert!(
            matches!(err, BackendError::Unavailable(_)),
            "status {s} should be Unavailable, got {err:?}"
        );
    }
    // ...but a non-5xx unclassified status stays a generic `Other`.
    let err = check_status(reqwest::StatusCode::FORBIDDEN, "Read", "k").unwrap_err();
    assert!(matches!(err, BackendError::Other { .. }), "got {err:?}");
}

#[test]
fn unclassified_status_produces_structured_error() {
    use crate::{check_conditional_status, check_status};
    use std::error::Error as _;

    // A non-success status that maps to no dedicated classification renders
    // through the structured `GcsStatusError`: op/path/status surface as typed
    // fields under `{:?}` rather than only inside a formatted message, and the
    // typed error is kept as the cause.
    let err = check_status(reqwest::StatusCode::FORBIDDEN, "Read", "k").unwrap_err();
    assert!(matches!(err, BackendError::Other { .. }));
    let dbg = format!("{err:?}");
    assert!(dbg.contains(r#"op: "Read""#), "got: {dbg}");
    assert!(dbg.contains(r#"path: "k""#), "got: {dbg}");
    assert!(dbg.contains("status: 403"), "got: {dbg}");
    assert!(err.source().is_some(), "structured error kept as the cause");

    // A conditional request keeps the same structured mapping for a non-5xx,
    // non-precondition status...
    let err = check_conditional_status(reqwest::StatusCode::FORBIDDEN, "Write", "k");
    assert!(matches!(err, BackendError::Other { .. }));

    // ...while a 5xx stays an in-doubt `Unavailable` (ADR-009).
    let err = check_conditional_status(reqwest::StatusCode::INTERNAL_SERVER_ERROR, "Write", "k");
    assert!(matches!(err, BackendError::Unavailable(_)));
}
