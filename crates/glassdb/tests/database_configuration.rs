//! Database configuration is checked before backend work starts.

use std::sync::Arc;

use glassdb::backend::middleware::RecordingBackend;
use glassdb::{Database, Error, memory::MemoryBackend};

#[tokio::test]
async fn invalid_database_names_are_rejected_before_io() {
    for name in [
        "a".repeat(256),
        String::new(),
        "db/name".into(),
        "dé".into(),
    ] {
        let backend = Arc::new(RecordingBackend::new(Arc::new(MemoryBackend::new())));
        let log = backend.log();
        assert!(matches!(
            Database::builder(name, backend).open().await,
            Err(Error::InvalidInput(_))
        ));
        assert!(log.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn database_names_at_the_format_boundaries_remain_usable() {
    for name in ["a".to_owned(), "Db9".repeat(85)] {
        let db = Database::open(&name, MemoryBackend::new()).await.unwrap();
        let root = db.root_collection();
        root.write(b"key", b"value").await.unwrap();
        assert_eq!(
            root.read(b"key").await.unwrap().as_deref(),
            Some(&b"value"[..])
        );
        db.shutdown().await;
    }
}
