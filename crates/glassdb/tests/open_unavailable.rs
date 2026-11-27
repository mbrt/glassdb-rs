//! Regression tests for storage outages during `Database::open`.
//!
//! An open reads the database metadata first and accepts whatever metadata is
//! stored, so a retry of the open is safe even when one of its writes landed.
//! So the open retries a short outage in place, and a persistent outage of any
//! step is [`Error::Unavailable`], never [`Error::InDoubt`] or
//! [`Error::Internal`].

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use glassdb::backend::memory::MemoryBackend;
use glassdb::backend::middleware::{BackendOp, HookBackend, HookFuture};
use glassdb::backend::{Backend, BackendError};
use glassdb::{Database, Error};

const METADATA: &str = "example/glassdb";

/// Decides the result of one backend operation before it is forwarded. `reads`
/// is the number of earlier metadata reads.
type Fault = fn(&BackendOp<'_>, usize) -> Option<BackendError>;

/// Opens the database over `inner`, with `fault` applied to each operation,
/// and returns the error of the open.
async fn open_error(inner: Arc<dyn Backend>, fault: Fault) -> Error {
    let backend = HookBackend::new(inner);
    let reads = Arc::new(AtomicUsize::new(0));
    backend.set_before(move |op| {
        let earlier = match op {
            BackendOp::Read { path } if *path == METADATA => reads.fetch_add(1, Ordering::SeqCst),
            _ => reads.load(Ordering::SeqCst),
        };
        let result = fault(op, earlier).map_or(Ok(()), Err);
        let future: HookFuture = Box::pin(async move { result });
        future
    });
    match Database::open("example", backend).await {
        Ok(_) => panic!("the open succeeded during an outage"),
        Err(error) => error,
    }
}

fn outage() -> Option<BackendError> {
    Some(BackendError::Unavailable("outage".into()))
}

#[tokio::test(start_paused = true)]
async fn an_outage_of_any_step_of_a_new_open_is_unavailable() {
    let cases: [(&str, Fault); 4] = [
        ("metadata read", |op, _| {
            matches!(op, BackendOp::Read { path } if *path == METADATA)
                .then(outage)
                .flatten()
        }),
        ("permanent collection", |op, _| {
            matches!(op, BackendOp::WriteIfNotExists { path, .. } if *path != METADATA)
                .then(outage)
                .flatten()
        }),
        ("metadata create", |op, _| {
            matches!(op, BackendOp::WriteIfNotExists { path, .. } if *path == METADATA)
                .then(outage)
                .flatten()
        }),
        ("permanent collection check", |op, _| {
            matches!(op, BackendOp::Read { path } if *path != METADATA)
                .then(outage)
                .flatten()
        }),
    ];
    for (step, fault) in cases {
        let error = open_error(Arc::new(MemoryBackend::new()), fault).await;
        assert!(
            matches!(error, Error::Unavailable(_)),
            "{step}: expected unavailable, got {error:?}"
        );
    }
}

// A concurrent creator stored the metadata between the first read and the
// create of this open. The create then fails its precondition, and the open
// reads the metadata again. An outage of that read is unavailable too.
#[tokio::test(start_paused = true)]
async fn an_outage_of_the_read_after_a_concurrent_create_is_unavailable() {
    let inner: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    Database::open("example", inner.clone())
        .await
        .unwrap()
        .shutdown()
        .await;

    let error = open_error(inner, |op, earlier| match op {
        // The first read misses the metadata of the concurrent creator.
        BackendOp::Read { path } if *path == METADATA && earlier == 0 => {
            Some(BackendError::NotFound)
        }
        BackendOp::Read { path } if *path == METADATA => outage(),
        _ => None,
    })
    .await;
    assert!(
        matches!(error, Error::Unavailable(_)),
        "expected unavailable, got {error:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_short_outage_of_any_step_is_retried_in_place() {
    type Step = fn(&BackendOp<'_>) -> bool;
    let cases: [(&str, Step); 4] = [
        (
            "metadata read",
            |op| matches!(op, BackendOp::Read { path } if *path == METADATA),
        ),
        (
            "permanent collection",
            |op| matches!(op, BackendOp::WriteIfNotExists { path, .. } if *path != METADATA),
        ),
        (
            "metadata create",
            |op| matches!(op, BackendOp::WriteIfNotExists { path, .. } if *path == METADATA),
        ),
        (
            "permanent collection check",
            |op| matches!(op, BackendOp::Read { path } if *path != METADATA),
        ),
    ];
    for (step, fails) in cases {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()) as Arc<dyn Backend>);
        let failed = Arc::new(AtomicBool::new(false));
        backend.set_before({
            let failed = failed.clone();
            move |op| {
                let result = if fails(op) && !failed.swap(true, Ordering::SeqCst) {
                    Err(BackendError::Unavailable("short outage".into()))
                } else {
                    Ok(())
                };
                let future: HookFuture = Box::pin(async move { result });
                future
            }
        });

        let opened = Database::open("example", backend).await;
        assert!(
            failed.load(Ordering::SeqCst),
            "{step}: the step did not run"
        );
        match opened {
            Ok(db) => db.shutdown().await,
            Err(error) => panic!("{step}: the open failed: {error:?}"),
        }
    }
}
