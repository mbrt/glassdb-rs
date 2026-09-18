//! Public-operation admission, capacity release, and shutdown behavior.

use std::future::{Future, pending};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::FutureExt;
use glassdb::{Database, Error, memory::MemoryBackend};

fn expect_full<T>(operation: impl Future<Output = Result<T, Error>>, limit: usize) {
    assert!(
        matches!(operation.now_or_never(), Some(Err(Error::LimitExceeded {
        resource: "active database operations", limit: actual,
    })) if actual == limit)
    );
}

#[tokio::test(start_paused = true)]
async fn capacity_is_shared_by_database_clones_and_public_operations() {
    let db = Database::builder("limits", MemoryBackend::new())
        .max_active_operations(1)
        .open()
        .await
        .unwrap();
    let mut held = Box::pin(db.tx(|_| pending::<Result<(), Error>>()));
    assert!(futures::poll!(held.as_mut()).is_pending());

    let clone = db.clone();
    let c = clone.root_collection();
    let calls = AtomicUsize::new(0);
    let before = db.stats().backend;
    expect_full(
        clone.tx(|_| {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        }),
        1,
    );
    expect_full(c.read(b"k"), 1);
    expect_full(c.read_stale(b"k", Duration::ZERO), 1);
    expect_full(c.write(b"k", b"v"), 1);
    expect_full(c.delete(b"k"), 1);
    expect_full(c.iter_keys(), 1);
    expect_full(c.create_collection(b"child"), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(db.stats().backend, before);

    drop(held);
    c.write(b"k", b"v").await.unwrap();
    assert_eq!(c.read(b"k").await.unwrap().as_deref(), Some(&b"v"[..]));
}

#[tokio::test(start_paused = true)]
async fn default_capacity_is_finite_and_cancellation_releases_it() {
    let db = Database::open("limits", MemoryBackend::new())
        .await
        .unwrap();
    let mut held = Vec::new();
    for _ in 0..256 {
        let mut operation = Box::pin(db.tx(|_| pending::<Result<(), Error>>()));
        assert!(futures::poll!(operation.as_mut()).is_pending());
        held.push(operation);
    }
    expect_full(db.tx(|_| async { Ok(()) }), 256);
    held.pop();
    db.tx(|_| async { Ok(()) }).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn separate_opens_have_independent_capacity() {
    let backend = Arc::new(MemoryBackend::new());
    let first = Database::builder("limits", backend.clone())
        .max_active_operations(1)
        .open()
        .await
        .unwrap();
    let second = Database::builder("limits", backend)
        .max_active_operations(1)
        .open()
        .await
        .unwrap();
    let mut held = Box::pin(first.tx(|_| pending::<Result<(), Error>>()));
    assert!(futures::poll!(held.as_mut()).is_pending());
    second.root_collection().write(b"k", b"v").await.unwrap();
    expect_full(first.root_collection().read(b"k"), 1);
}

#[tokio::test(start_paused = true)]
async fn zero_capacity_rejects_operations_but_allows_shutdown() {
    let db = Database::builder("limits", MemoryBackend::new())
        .max_active_operations(0)
        .open()
        .await
        .unwrap();
    expect_full(db.tx(|_| async { Ok(()) }), 0);
    expect_full(db.root_collection().read_stale(b"k", Duration::ZERO), 0);
    db.shutdown().await;
    assert!(matches!(
        db.root_collection().read(b"k").await,
        Err(Error::ShuttingDown)
    ));
}

#[tokio::test(start_paused = true)]
async fn error_outcomes_release_capacity() {
    let db = Database::builder("limits", MemoryBackend::new())
        .max_active_operations(1)
        .open()
        .await
        .unwrap();
    let result = db.tx(|tx| async move { tx.abort() }).await;
    assert!(matches!(result, Err(Error::Aborted)));
    db.root_collection().write(b"k", b"v").await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn shutdown_rejects_admission_while_existing_operations_drain() {
    let db = Database::builder("limits", MemoryBackend::new())
        .max_active_operations(1)
        .open()
        .await
        .unwrap();
    let mut held = Box::pin(db.tx(|_| pending::<Result<(), Error>>()));
    assert!(futures::poll!(held.as_mut()).is_pending());
    let mut shutdown = Box::pin(db.shutdown());
    assert!(futures::poll!(shutdown.as_mut()).is_pending());
    assert!(matches!(
        db.tx(|_| async { Ok(()) }).await,
        Err(Error::ShuttingDown)
    ));
    drop(held);
    shutdown.await;
}
