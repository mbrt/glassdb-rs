//! Regression tests for definite backend failures.
//!
//! A backend can fail a request with an error that it does not classify, for
//! example an expired credential. The request has no effect, so the caller gets
//! [`Error::Backend`], not [`Error::Internal`], which means a GlassDB bug.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use glassdb::backend::memory::MemoryBackend;
use glassdb::backend::middleware::{BackendOp, HookBackend, HookFuture};
use glassdb::backend::{Backend, BackendError};
use glassdb::{CollectionPath, Database, Error};

/// Seeds collection `c` with key `k` through a plain database, then opens a
/// second database, with a cold cache, over a backend that rejects each
/// operation that `rejects` selects. Returns that database and its collection.
async fn open_with_rejections(
    rejects: fn(&BackendOp<'_>) -> bool,
) -> (Database, glassdb::Collection) {
    let memory: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    Database::open("example", memory.clone())
        .await
        .unwrap()
        .root_collection()
        .create_collection_if_absent(b"c")
        .await
        .unwrap()
        .write(b"k", b"v")
        .await
        .unwrap();

    let backend = HookBackend::new(memory);
    let armed = Arc::new(AtomicBool::new(false));
    backend.set_before({
        let armed = armed.clone();
        move |op| {
            let reject = armed.load(Ordering::SeqCst) && rejects(op);
            let future: HookFuture = Box::pin(async move {
                if reject {
                    Err(BackendError::other("expired credential"))
                } else {
                    Ok(())
                }
            });
            future
        }
    });
    let db = Database::open("example", backend).await.unwrap();
    let coll = db
        .open_collection(&CollectionPath::new(b"c").unwrap())
        .await
        .unwrap();
    armed.store(true, Ordering::SeqCst);
    (db, coll)
}

#[tokio::test]
async fn a_rejected_read_is_a_backend_error() {
    let (db, coll) = open_with_rejections(|op| {
        matches!(
            op,
            BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
        )
    })
    .await;

    let coll = &coll;
    let error = db
        .tx(|tx| async move { tx.read(coll, b"k").await })
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Backend { .. }),
        "expected a backend error, got {error:?}"
    );
}

#[tokio::test]
async fn a_rejected_commit_is_a_backend_error() {
    let (_db, coll) = open_with_rejections(|op| {
        matches!(
            op,
            BackendOp::WriteIf { .. } | BackendOp::WriteIfNotExists { .. }
        )
    })
    .await;

    let error = coll.write(b"k", b"w").await.unwrap_err();
    assert!(
        matches!(error, Error::Backend { .. }),
        "expected a backend error, got {error:?}"
    );
}

// A rejected create of the metadata during an open is a backend error too.
#[tokio::test]
async fn a_rejected_open_is_a_backend_error() {
    let backend = HookBackend::new(Arc::new(MemoryBackend::new()) as Arc<dyn Backend>);
    backend.set_before(|op| {
        let reject =
            matches!(op, BackendOp::WriteIfNotExists { path, .. } if *path == "example/glassdb");
        let future: HookFuture = Box::pin(async move {
            if reject {
                Err(BackendError::other("expired credential"))
            } else {
                Ok(())
            }
        });
        future
    });

    let error = match Database::open("example", backend).await {
        Ok(_) => panic!("the open succeeded although its metadata create was rejected"),
        Err(error) => error,
    };
    assert!(
        matches!(error, Error::Backend { .. }),
        "expected a backend error, got {error:?}"
    );
}
