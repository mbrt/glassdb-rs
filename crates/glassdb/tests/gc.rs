use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use glassdb::backend::{BackendError, memory::MemoryBackend};
use glassdb::middleware::{BackendOp, HookBackend};
use glassdb::{Backend, Database};
use glassdb_data::{DbPrefix, TxId};
use glassdb_storage::transaction::{TxCommitStatus, TxRecord, TxRecordStore};
use glassdb_storage::{CachedStore, Requirement, StorageError, Timeline};

fn tx_record_store(backend: Arc<dyn Backend>) -> TxRecordStore {
    TxRecordStore::new(
        CachedStore::new(backend, 1 << 20, Timeline::new(), None),
        DbPrefix::try_from("gc").unwrap(),
    )
}

async fn old_objects(tx_records: &TxRecordStore, count: usize) -> Vec<TxId> {
    let mut ids = Vec::new();
    for i in 0..count {
        let id = TxId::with_priority(
            i as u64,
            &(i as u64).wrapping_mul(0x9e3779b97f4a7c15).to_be_bytes(),
        );
        let mut record = TxRecord::new(id, TxCommitStatus::Aborted);
        record.timestamp = Some(UNIX_EPOCH);
        tx_records.set(&record).await.unwrap();
        ids.push(id);
    }
    ids
}

async fn step(seconds: u64) {
    tokio::time::advance(Duration::from_secs(seconds)).await;
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn idle_instances_back_off_without_writes() {
    let backend = Arc::new(MemoryBackend::new());
    let db = Database::open("gc", backend.clone()).await.unwrap();
    step(0).await;
    let before = db.stats();
    for _ in 0..600 {
        step(1).await;
    }
    let activity = db.stats() - before;
    assert!(activity.gc.lists > 1);
    assert!(
        activity.gc.lists < 40,
        "idle GC made {} LISTs",
        activity.gc.lists
    );
    assert_eq!(activity.gc.progress, 0);
    assert_eq!(activity.backend.obj_writes, 0);
    assert_eq!(db.diagnostics().gc.prefix_count, 1);
    db.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn separate_instances_find_orphans_without_local_hints() {
    let backend = Arc::new(MemoryBackend::new());
    let tx_records = tx_record_store(backend.clone());
    let ids = old_objects(&tx_records, 40).await;
    let first = Database::open("gc", backend.clone()).await.unwrap();
    let second = Database::open("gc", backend.clone()).await.unwrap();
    step(0).await;
    for _ in 0..10 {
        step(4).await;
    }
    for id in ids {
        assert!(matches!(
            tx_record_store(backend.clone())
                .get_at(&id, Requirement::ANY)
                .await,
            Err(StorageError::NotFound)
        ));
    }
    assert!(first.stats().gc.lists > 0);
    assert!(second.stats().gc.lists > 0);
    first.shutdown().await;
    second.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn writers_complete_while_gc_is_saturated_and_stalled() {
    for configured_limit in [None, Some(1), Some(3), Some(12)] {
        let limit = configured_limit.unwrap_or(8);
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let tx_records = tx_record_store(backend.clone());
        old_objects(&tx_records, 100).await;
        let mut builder = Database::builder("gc", backend.clone());
        if let Some(limit) = configured_limit {
            builder = builder.gc_parallelism(NonZeroUsize::new(limit).unwrap());
        }
        let db = builder.open().await.unwrap();
        backend.set_before(|operation| {
            let stall =
                matches!(operation, BackendOp::DeleteIf { path, .. } if path.contains("/_t/"));
            Box::pin(async move {
                if stall {
                    tokio::time::sleep(Duration::from_secs(70)).await;
                }
                Ok(())
            })
        });
        step(0).await;
        for _ in 0..8 {
            step(4).await;
        }
        let gc = db.diagnostics().gc;
        assert!(gc.in_flight > 0 && gc.in_flight <= limit as u64, "{gc:?}");
        assert!(gc.ready > 0, "{gc:?}");
        let collection = db.root_collection();
        tokio::time::timeout(
            Duration::from_secs(1),
            db.tx(|tx| {
                let collection = collection.clone();
                async move { tx.write(&collection, b"writer", b"still runs") }
            }),
        )
        .await
        .unwrap()
        .unwrap();
        // A healthy, long check must not restart when its total duration exceeds a lease.
        step(65).await;
        let stats = db.stats();
        assert_eq!(stats.gc.failures, 0);
        assert!(stats.gc.progress > 0);
        assert_eq!(db.diagnostics().gc.in_flight, limit as u64);
        backend.clear_before();
        db.shutdown().await;
    }
}

#[tokio::test(start_paused = true)]
async fn list_failures_do_not_look_like_an_idle_database() {
    let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
    let db = Database::open("gc", backend.clone()).await.unwrap();
    backend.set_before(|operation| {
        let fail = matches!(operation, BackendOp::List { path, .. } if *path == "gc/_t/");
        Box::pin(async move {
            if fail {
                Err(BackendError::Unavailable("offline".into()))
            } else {
                Ok(())
            }
        })
    });
    step(0).await;
    for _ in 0..12 {
        step(5).await;
    }
    assert!(db.stats().gc.failures > 0);
    let tx_records = tx_record_store(backend.clone());
    let ids = old_objects(&tx_records, 1).await;
    backend.clear_before();
    for _ in 0..24 {
        step(5).await;
    }
    assert!(matches!(
        tx_record_store(backend.clone())
            .get_at(&ids[0], Requirement::ANY)
            .await,
        Err(StorageError::NotFound)
    ));
    db.shutdown().await;
}
