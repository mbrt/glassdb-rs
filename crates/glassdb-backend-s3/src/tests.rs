//! Behavioral tests for the S3 backend, run against the pure-Rust in-process
//! fake S3 server in [`crate::fake_server`] (the analog of the Go tests'
//! `gofakes3` + `httptest.Server`).

use aws_sdk_s3::config::retry::RetryConfig;
use glassdb_backend::{
    Backend, BackendError, LAST_WRITER_TAG, Tags, Version, WriterId, encode_writer_tag,
};
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
async fn read_strips_nonce() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    for (name, value) in [
        ("non-empty", b"hello world".to_vec()),
        ("empty", Vec::new()),
        ("binary", vec![0x00, 0x01, 0x02, 0xff]),
    ] {
        let mut tags = Tags::new();
        tags.insert("key".to_string(), "val".to_string());
        let meta = b.write(name, value.clone(), tags).await.unwrap();
        assert!(!meta.version.is_unset());

        let r = b.read(name).await.unwrap();
        assert_eq!(r.contents, value, "case {name}");
        assert_eq!(r.tags.get("key").map(String::as_str), Some("val"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_produces_fresh_version_each_time() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    // Re-uploading identical bytes must still change the version because the
    // nonce forces a fresh ETag.
    let m1 = b.write("k", b"same".to_vec(), Tags::new()).await.unwrap();
    let m2 = b.write("k", b"same".to_vec(), Tags::new()).await.unwrap();
    assert_ne!(m1.version, m2.version);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_tags_if_merges_and_cas() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);

    let writer = WriterId::new(b"tx-1".to_vec());
    let mut tags = Tags::new();
    tags.insert(LAST_WRITER_TAG.to_string(), encode_writer_tag(&writer));
    tags.insert("lock-type".to_string(), "-".to_string());
    let m0 = b.write("k", b"value".to_vec(), tags).await.unwrap();

    let mut new_tags = Tags::new();
    new_tags.insert("lock-type".to_string(), "w".to_string());
    new_tags.insert("locked-by".to_string(), "tx2".to_string());
    let m1 = b.set_tags_if("k", &m0.version, new_tags).await.unwrap();
    assert_ne!(m0.version, m1.version);
    assert_eq!(
        m1.tags.get(LAST_WRITER_TAG).map(String::as_str),
        Some(encode_writer_tag(&writer).as_str())
    );
    assert_eq!(m1.tags.get("lock-type").map(String::as_str), Some("w"));
    assert_eq!(m1.tags.get("locked-by").map(String::as_str), Some("tx2"));

    // The underlying value is untouched by a tag update.
    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"value");

    // The now-stale version fails the precondition.
    let mut t = Tags::new();
    t.insert("lock-type".to_string(), "r".to_string());
    let err = b.set_tags_if("k", &m0.version, t).await.unwrap_err();
    assert!(matches!(err, BackendError::Precondition));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_tags_if_not_found() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let mut t = Tags::new();
    t.insert("lock-type".to_string(), "r".to_string());
    let err = b
        .set_tags_if("missing", &Version::new("\"x\""), t)
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::NotFound));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_if_not_exists() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    b.write_if_not_exists("k", b"a".to_vec(), Tags::new())
        .await
        .unwrap();
    let err = b
        .write_if_not_exists("k", b"b".to_vec(), Tags::new())
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::Precondition));
    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"a");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_if_cas() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let m0 = b.write("k", b"a".to_vec(), Tags::new()).await.unwrap();

    let err = b
        .write_if("k", b"b".to_vec(), &Version::new("\"stale\""), Tags::new())
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::Precondition));

    let m1 = b
        .write_if("k", b"b".to_vec(), &m0.version, Tags::new())
        .await
        .unwrap();
    assert_ne!(m0.version, m1.version);
    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"b");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_if_null_version_fails_precondition() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let m0 = b.write("k", b"a".to_vec(), Tags::new()).await.unwrap();

    // A null expected version has an empty token; it must fail rather than
    // overwrite unconditionally.
    let err = b
        .write_if("k", b"b".to_vec(), &Version::default(), Tags::new())
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::Precondition));

    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"a");
    assert_eq!(r.version, m0.version);

    let mut t = Tags::new();
    t.insert("lock-type".to_string(), "r".to_string());
    let err = b
        .set_tags_if("k", &Version::default(), t)
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::Precondition));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_if_modified() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let writer = WriterId::new(b"w1".to_vec());
    let mut tags = Tags::new();
    tags.insert(LAST_WRITER_TAG.to_string(), encode_writer_tag(&writer));
    b.write("k", b"x".to_vec(), tags).await.unwrap();

    let err = b.read_if_modified("k", &writer).await.unwrap_err();
    assert!(matches!(err, BackendError::Precondition));

    let r = b
        .read_if_modified("k", &WriterId::new(b"other".to_vec()))
        .await
        .unwrap();
    assert_eq!(r.contents, b"x");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_if() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let m0 = b.write("k", b"x".to_vec(), Tags::new()).await.unwrap();

    let err = b
        .delete_if("k", &Version::new("\"wrong\""))
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::Precondition));
    b.read("k").await.unwrap();

    b.delete_if("k", &m0.version).await.unwrap();
    let err = b.read("k").await.unwrap_err();
    assert!(matches!(err, BackendError::NotFound));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_and_metadata_not_found() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    let err = b.read("missing").await.unwrap_err();
    assert!(matches!(err, BackendError::NotFound));
    let err = b.get_metadata("missing").await.unwrap_err();
    assert!(matches!(err, BackendError::NotFound));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_with_subdirs() {
    let fake = FakeS3::start().await;
    let b = backend(&fake);
    for name in ["d/a/1", "d/a/2", "d/a/b/1", "d/c/1", "d/root"] {
        b.write(name, name.as_bytes().to_vec(), Tags::new())
            .await
            .unwrap();
    }
    let got = b.list("d").await.unwrap();
    assert_eq!(got, vec!["d/a/", "d/c/", "d/root"]);
    let got = b.list("d/a").await.unwrap();
    assert_eq!(got, vec!["d/a/1", "d/a/2", "d/a/b/"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_retries_through_slow_down() {
    let fake = FakeS3::start().await;
    let b = builder(&fake).retry_config(fast_retry()).build();
    fake.set_slowdown(2, Some(Method::PUT));

    b.write("k", b"v".to_vec(), Tags::new()).await.unwrap();
    assert_eq!(fake.slowdown_remaining(), 0);

    let r = b.read("k").await.unwrap();
    assert_eq!(r.contents, b"v");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_retries_through_slow_down() {
    let fake = FakeS3::start().await;
    let b = builder(&fake).retry_config(fast_retry()).build();

    // The write is a PUT, so it is not throttled here.
    b.write("k", b"v".to_vec(), Tags::new()).await.unwrap();

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

    let err = b.write("k", b"v".to_vec(), Tags::new()).await.unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains("SlowDown"), "got: {msg}");
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
    let err = b
        .write_if_not_exists("k", b"v".to_vec(), Tags::new())
        .await
        .unwrap_err();
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
    let m0 = b.write("k", b"a".to_vec(), Tags::new()).await.unwrap();

    // The CAS write lands (changing the ETag), but its ack is lost; the re-send's
    // If-Match no longer matches and gets 412.
    fake.set_lost_ack(1);
    let err = b
        .write_if("k", b"b".to_vec(), &m0.version, Tags::new())
        .await
        .unwrap_err();
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
    b.write_if_not_exists("k", b"a".to_vec(), Tags::new())
        .await
        .unwrap();
    let err = b
        .write_if_not_exists("k", b"b".to_vec(), Tags::new())
        .await
        .unwrap_err();
    assert!(matches!(err, BackendError::Precondition), "got {err:?}");
}
