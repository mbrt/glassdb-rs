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
    // The default parallelism is above the 100 stalled records, so GC runs at
    // most all of them at once. Checks that stall before their writes do not
    // raise the concurrency above the read-only limit of 8.
    for configured_limit in [None, Some(1), Some(3), Some(12)] {
        let limit = configured_limit.unwrap_or(256).min(8);
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let tx_records = tx_record_store(backend.clone());
        old_objects(&tx_records, 100).await;
        let mut builder = Database::builder("gc", backend.clone());
        if let Some(configured) = configured_limit {
            builder = builder.gc_parallelism(NonZeroUsize::new(configured).unwrap());
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

fn record_path(id: &TxId) -> String {
    glassdb_data::ObjectPath::Transaction {
        db_prefix: DbPrefix::try_from("gc").unwrap(),
        id: *id,
    }
    .to_string()
}

async fn record_exists(backend: Arc<dyn Backend>, id: &TxId) -> bool {
    !matches!(
        tx_record_store(backend).get_at(id, Requirement::ANY).await,
        Err(StorageError::NotFound)
    )
}

#[tokio::test(start_paused = true)]
async fn full_gc_checks_every_page_and_reports_records_it_keeps() {
    let backend = Arc::new(MemoryBackend::new());
    let db = Database::open("gc", backend.clone()).await.unwrap();
    let tx_records = tx_record_store(backend.clone());
    // More than one LIST page.
    let aborted = old_objects(&tx_records, 1_001).await;
    let pending = TxId::with_priority(1, b"pending");
    let mut record = TxRecord::new(pending, TxCommitStatus::Pending);
    record.timestamp = Some(UNIX_EPOCH);
    tx_records.set(&record).await.unwrap();
    let young = TxId::with_priority(2, b"young");
    let mut record = TxRecord::new(young, TxCommitStatus::Aborted);
    record.timestamp = Some(glassdb_concurr::rt::system_now());
    tx_records.set(&record).await.unwrap();

    let report = db
        .collect_garbage(NonZeroUsize::new(16).unwrap())
        .await
        .unwrap();

    assert_eq!(report.deleted, 1_001);
    assert_eq!(report.retained, 1);
    assert_eq!(report.deferred, 1);
    assert_eq!(
        (report.missing, report.reclaimed_effects, report.failed),
        (0, 0, 0)
    );
    let retry_after = report.retry_after.unwrap();
    assert!(retry_after > Duration::ZERO && retry_after <= Duration::from_secs(45));
    for id in aborted {
        assert!(!record_exists(backend.clone(), &id).await);
    }
    assert!(record_exists(backend.clone(), &pending).await);
    assert!(record_exists(backend.clone(), &young).await);
    db.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn full_gc_counts_failed_checks_and_continues() {
    let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
    let db = Database::open("gc", backend.clone()).await.unwrap();
    let ids = old_objects(&tx_record_store(backend.clone()), 3).await;
    let failing = record_path(&ids[0]);
    backend.set_before(move |operation| {
        let fail = matches!(operation, BackendOp::Read { path } if *path == failing);
        Box::pin(async move {
            if fail {
                Err(BackendError::Unavailable("offline".into()))
            } else {
                Ok(())
            }
        })
    });

    let report = db.collect_garbage(NonZeroUsize::MIN).await.unwrap();

    assert_eq!((report.deleted, report.failed), (2, 1));
    assert!(record_exists(backend.clone(), &ids[0]).await);
    backend.clear_before();
    db.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_full_gc_completes_the_checks_of_a_dropped_one() {
    let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
    let db = Database::open("gc", backend.clone()).await.unwrap();
    let ids = old_objects(&tx_record_store(backend.clone()), 1).await;
    backend.set_before(|operation| {
        let stall = matches!(operation, BackendOp::DeleteIf { path, .. } if path.contains("/_t/"));
        Box::pin(async move {
            if stall {
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
            Ok(())
        })
    });

    let dropped = tokio::time::timeout(
        Duration::from_secs(1),
        db.collect_garbage(NonZeroUsize::MIN),
    )
    .await;
    assert!(dropped.is_err(), "the delete stalls past the timeout");
    // The dropped check stopped before its delete. Background GC starts its
    // first scan after the pending timeout, so it cannot delete the record yet.
    step(6).await;
    assert!(record_exists(backend.clone(), &ids[0]).await);
    backend.clear_before();

    let report = db.collect_garbage(NonZeroUsize::MIN).await.unwrap();

    assert_eq!(report.deleted, 1);
    assert!(!record_exists(backend.clone(), &ids[0]).await);
    db.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn full_gc_is_refused_after_shutdown() {
    let db = Database::open("gc", Arc::new(MemoryBackend::new()))
        .await
        .unwrap();
    db.shutdown().await;

    let result = db.collect_garbage(NonZeroUsize::MIN).await;

    assert!(
        matches!(result, Err(glassdb::Error::ShuttingDown)),
        "{result:?}"
    );
}

// Background GC writes at most at its base rate, 100 writes in each second,
// when its instance runs no transactions (ADR-078).
#[tokio::test(start_paused = true)]
async fn background_gc_without_transactions_keeps_to_the_base_rate() {
    let backend = Arc::new(MemoryBackend::new());
    let tx_records = tx_record_store(backend.clone());
    old_objects(&tx_records, 2_000).await;
    let db = Database::builder("gc", backend.clone())
        .gc_parallelism(NonZeroUsize::new(512).unwrap())
        .open()
        .await
        .unwrap();
    // A paused clock advances only when every task waits, so GC is limited by
    // its budget and not by the test.
    tokio::time::sleep(Duration::from_secs(20)).await;
    let before = db.stats().gc.progress;
    tokio::time::sleep(Duration::from_secs(10)).await;
    let deleted = db.stats().gc.progress - before;

    // Each deleted aborted record is one write. The allowance holds one second
    // of writes, so ten seconds allow up to eleven seconds of the base rate.
    assert!(
        (900..=1_100).contains(&deleted),
        "deleted {deleted} records in 10 s"
    );
    db.shutdown().await;
}

// The writes of a full GC do not spend the budget of background GC, which
// keeps its base rate afterwards (ADR-078).
#[tokio::test(start_paused = true)]
async fn a_full_gc_does_not_spend_the_background_budget() {
    let backend = Arc::new(MemoryBackend::new());
    let tx_records = tx_record_store(backend.clone());
    old_objects(&tx_records, 3_000).await;
    let db = Database::builder("gc", backend.clone())
        .gc_parallelism(NonZeroUsize::new(512).unwrap())
        .open()
        .await
        .unwrap();
    let report = db
        .collect_garbage(NonZeroUsize::new(64).unwrap())
        .await
        .unwrap();
    assert_eq!(report.deleted, 3_000, "{report:?}");
    // New records, which only background GC deletes. They are more than its
    // base rate can delete in the measured time.
    for i in 0..5_000u64 {
        let id = TxId::with_priority(i, &(i + 1_000_000).to_be_bytes());
        let mut record = TxRecord::new(id, TxCommitStatus::Aborted);
        record.timestamp = Some(UNIX_EPOCH);
        tx_records.set(&record).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(20)).await;
    let before = db.stats().gc.progress;
    tokio::time::sleep(Duration::from_secs(10)).await;
    let deleted = db.stats().gc.progress - before;

    assert!(
        (900..=1_100).contains(&deleted),
        "deleted {deleted} records in 10 s"
    );
    db.shutdown().await;
}
