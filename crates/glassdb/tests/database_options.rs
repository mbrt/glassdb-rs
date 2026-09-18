//! Shared database settings and local client options.

use std::sync::Arc;
use std::time::Duration;

use glassdb::memory::MemoryBackend;
use glassdb::{Database, Error, SplitPolicy};

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
