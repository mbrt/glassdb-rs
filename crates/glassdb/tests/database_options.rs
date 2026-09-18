//! Shared database settings and local client options.

use std::sync::Arc;
use std::time::Duration;

use glassdb::backend::{BackendError, ListLimit};
use glassdb::memory::MemoryBackend;
use glassdb::middleware::{BackendOp, HookBackend};
use glassdb::{Backend, Database, Error, InlinePolicy, ProtocolTiming, SplitPolicy};
use glassdb_storage::transaction::TxCommitStatus;

fn small_policy() -> SplitPolicy {
    SplitPolicy::builder()
        .node_max_bytes(512)
        .split_headroom_bytes(128)
        .node_soft_max_bytes(256)
        .build()
        .unwrap()
}

#[tokio::test(start_paused = true)]
async fn reopening_cannot_lower_the_limits_of_existing_data() {
    let backend = Arc::new(MemoryBackend::new());
    let creator = Database::open("limits", backend.clone()).await.unwrap();
    let key = vec![b'k'; 256];
    creator
        .root_collection()
        .write(&key, &[b'v'; 900])
        .await
        .unwrap();
    creator.shutdown().await;

    let reopened = Database::builder("limits", backend)
        .split_policy(small_policy())
        .open()
        .await
        .unwrap();
    let root = reopened.root_collection();
    tokio::time::timeout(Duration::from_secs(5), root.write(b"new", b"value"))
        .await
        .expect("stored limits must permit adding a key beside an existing inline value")
        .unwrap();
    assert_eq!(root.read(&key).await.unwrap(), Some(vec![b'v'; 900]));
    root.write(&key, b"replacement").await.unwrap();
    assert_eq!(
        root.read(&key).await.unwrap(),
        Some(b"replacement".to_vec())
    );
    reopened.shutdown().await;
}

#[tokio::test]
async fn reopening_with_defaults_retains_custom_key_and_directory_limits() {
    let backend = Arc::new(MemoryBackend::new());
    let creator = Database::builder("limits", backend.clone())
        .split_policy(small_policy())
        .open()
        .await
        .unwrap();
    creator.shutdown().await;

    let reopened = Database::open("limits", backend).await.unwrap();
    let root = reopened.root_collection();
    assert!(matches!(
        root.write(&[b'k'; 256], b"value").await,
        Err(Error::InvalidInput(_))
    ));
    root.create_collection([b'a'; 200]).await.unwrap();
    assert!(matches!(
        root.create_collection([b'b'; 200]).await,
        Err(Error::InvalidInput(_))
    ));
    root.write(b"short", b"value").await.unwrap();
    assert_eq!(root.read(b"short").await.unwrap(), Some(b"value".to_vec()));
    reopened.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn reopening_uses_the_stored_commit_recovery_timeout() {
    let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
    let creator = Database::builder("timing", backend.clone())
        .protocol_timing(ProtocolTiming::simulation())
        .open()
        .await
        .unwrap();
    creator.shutdown().await;

    let reopened = Database::builder("timing", backend.clone())
        .inline_policy(InlinePolicy::none())
        .open()
        .await
        .unwrap();
    backend.set_after(|op, _| {
        let committed_log = matches!(op,
            BackendOp::WriteIf { path, value, .. }
                | BackendOp::WriteIfNotExists { path, value }
                if path.contains("/_t/")
                    && glassdb_storage::txobject::status(value)
                        .is_ok_and(|status| status == TxCommitStatus::Ok));
        let status_read = matches!(op,
            BackendOp::Read { path } | BackendOp::ReadIfModified { path, .. }
                if path.contains("/_t/"));
        Box::pin(async move {
            if committed_log || status_read {
                Err(BackendError::Unavailable(
                    "lost commit acknowledgement".into(),
                ))
            } else {
                Ok(())
            }
        })
    });
    let root = reopened.root_collection();
    let result = tokio::time::timeout(Duration::from_secs(2), root.write(b"key", b"value"))
        .await
        .expect("recovery must use the stored 250 ms timeout, not the default 15 seconds");
    assert!(matches!(result, Err(Error::InDoubt(_))), "{result:?}");
    backend.clear_after();
    assert_eq!(root.read(b"key").await.unwrap(), Some(b"value".to_vec()));
    reopened.shutdown().await;
}

#[tokio::test]
async fn invalid_creation_timing_does_not_initialize_storage() {
    for timing in [
        ProtocolTiming::new(Duration::from_nanos(1), Duration::ZERO),
        ProtocolTiming::new(Duration::MAX, Duration::ZERO),
        ProtocolTiming::new(Duration::from_secs(1), Duration::MAX),
    ] {
        let backend = Arc::new(MemoryBackend::new());
        let result = Database::builder("invalid", backend.clone())
            .protocol_timing(timing)
            .open()
            .await;
        assert!(matches!(result, Err(Error::InvalidInput(_))));
        let objects = backend
            .list("invalid/", None, ListLimit::new(100).unwrap())
            .await
            .unwrap();
        assert!(objects.objects.is_empty());
    }
}
