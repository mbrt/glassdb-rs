//! Temporary F07-A attempt-lifecycle characterization.
//!
//! F07-C removes the private probe and exact accounting cases, retaining only
//! lightweight behavior-level regressions.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use glassdb_backend::memory::MemoryBackend;
use tokio::sync::oneshot;

use super::*;

const BODY_ERROR: &str = "transaction condition failed";

async fn database_with_collection() -> (Database, Collection) {
    let db = Database::open("attempts", MemoryBackend::new())
        .await
        .unwrap();
    let collection = db
        .root_collection()
        .create_collection_if_absent(b"items")
        .await
        .unwrap();
    db.inner.attempt_lifecycle.reset();
    (db, collection)
}

fn assert_lifecycle(db: &Database, begun: usize, ended: usize, abandoned: usize) {
    let snapshot = db.inner.attempt_lifecycle.snapshot();
    assert_eq!(snapshot.begun, begun, "snapshot: {snapshot:?}");
    assert_eq!(snapshot.ended, ended, "snapshot: {snapshot:?}");
    assert_eq!(snapshot.abandoned, abandoned, "snapshot: {snapshot:?}");
    assert!(snapshot.active.is_empty(), "snapshot: {snapshot:?}");
}

async fn shutdown_finishes(db: &Database) {
    tokio::time::timeout(Duration::from_secs(5), db.shutdown())
        .await
        .expect("shutdown left transaction cleanup pending");
}

async fn cancel_at_phase(phase: AttemptAwait, force_wound: bool) {
    let (db, collection) = database_with_collection().await;
    if force_wound {
        db.inner.attempt_lifecycle.force_next_commit_wound();
    }
    let (arrived, release) = db.inner.attempt_lifecycle.pause_next(phase);

    let task = tokio::spawn({
        let db = db.clone();
        let collection = collection.clone();
        async move {
            let collection_ref = &collection;
            db.tx(|tx| async move {
                if phase == AttemptAwait::ReadValidation {
                    tx.read(collection_ref, b"key").await?;
                    return Err::<(), Error>(Error::internal(BODY_ERROR));
                }
                tx.write(collection_ref, b"key", b"value")
            })
            .await
        }
    });

    arrived
        .await
        .expect("transaction did not reach pause point");
    let active = db.inner.attempt_lifecycle.snapshot();
    assert_eq!(active.begun, 1, "snapshot: {active:?}");
    assert_eq!(active.ended, 0, "snapshot: {active:?}");
    assert_eq!(active.abandoned, 0, "snapshot: {active:?}");
    assert_eq!(active.active.len(), 1, "snapshot: {active:?}");

    task.abort();
    let join_error = task.await.unwrap_err();
    assert!(join_error.is_cancelled());
    drop(release);

    assert_lifecycle(&db, 1, 0, 1);
    shutdown_finishes(&db).await;
}

#[tokio::test(start_paused = true)]
async fn cancellation_while_body_is_pending_starts_no_attempt() {
    let (db, _collection) = database_with_collection().await;
    let (entered_tx, entered_rx) = oneshot::channel();
    let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));

    let task = tokio::spawn({
        let db = db.clone();
        let entered_tx = entered_tx.clone();
        async move {
            db.tx(move |_tx| {
                let entered_tx = entered_tx.clone();
                async move {
                    if let Some(entered_tx) = entered_tx
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .take()
                    {
                        let _ = entered_tx.send(());
                    }
                    std::future::pending::<Result<(), Error>>().await
                }
            })
            .await
        }
    });

    entered_rx.await.unwrap();
    task.abort();
    let join_error = task.await.unwrap_err();
    assert!(join_error.is_cancelled());

    assert_lifecycle(&db, 0, 0, 0);
    shutdown_finishes(&db).await;
}

#[tokio::test(start_paused = true)]
async fn cancellation_while_read_validation_is_pending_abandons_the_attempt() {
    cancel_at_phase(AttemptAwait::ReadValidation, false).await;
}

#[tokio::test(start_paused = true)]
async fn cancellation_while_commit_is_pending_abandons_the_attempt() {
    cancel_at_phase(AttemptAwait::Commit, false).await;
}

#[tokio::test(start_paused = true)]
async fn cancellation_while_wound_restart_is_pending_abandons_the_old_attempt() {
    cancel_at_phase(AttemptAwait::WoundRestart, true).await;
}

#[tokio::test(start_paused = true)]
async fn wound_restart_ends_both_attempts() {
    let (db, collection) = database_with_collection().await;
    db.inner.attempt_lifecycle.force_next_commit_wound();
    let calls = Arc::new(AtomicUsize::new(0));

    db.tx({
        let calls = calls.clone();
        move |tx| {
            calls.fetch_add(1, Ordering::SeqCst);
            let collection = collection.clone();
            async move { tx.write(&collection, b"key", b"value") }
        }
    })
    .await
    .unwrap();

    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_lifecycle(&db, 2, 2, 0);
    shutdown_finishes(&db).await;
}

#[tokio::test(start_paused = true)]
async fn body_error_is_returned_only_after_its_reads_validate() {
    let (db, collection) = database_with_collection().await;
    collection.write(b"key", b"before").await.unwrap();
    db.inner.attempt_lifecycle.reset();

    let calls = Arc::new(AtomicUsize::new(0));
    let (read_tx, read_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let read_tx = Arc::new(Mutex::new(Some(read_tx)));
    let release_rx = Arc::new(Mutex::new(Some(release_rx)));
    let task = tokio::spawn({
        let db = db.clone();
        let collection = collection.clone();
        let calls = calls.clone();
        async move {
            db.tx(move |tx| {
                calls.fetch_add(1, Ordering::SeqCst);
                let collection = collection.clone();
                let read_tx = read_tx
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                let release_rx = release_rx
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                async move {
                    tx.read(&collection, b"key").await?;
                    if let Some(read_tx) = read_tx {
                        let _ = read_tx.send(());
                    }
                    if let Some(release_rx) = release_rx {
                        let _ = release_rx.await;
                    }
                    Err::<(), Error>(Error::internal(BODY_ERROR))
                }
            })
            .await
        }
    });

    read_rx.await.unwrap();
    collection.write(b"key", b"after").await.unwrap();
    db.inner.attempt_lifecycle.reset();
    release_tx.send(()).unwrap();

    let error = task.await.unwrap().unwrap_err();
    assert_eq!(error.to_string(), BODY_ERROR);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_lifecycle(&db, 1, 1, 0);
    shutdown_finishes(&db).await;
}
