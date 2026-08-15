//! Shutdown and cancellation integration behavior.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use glassdb::backend::BackendError;
use glassdb::backend::memory::MemoryBackend;
use glassdb::backend::middleware::{BackendOp, HookBackend};
use glassdb::{Backend, Database, Error, InlinePolicy, ProtocolTiming};
use tokio::sync::oneshot;

#[path = "integration_support/mod.rs"]
pub mod integration_support;

use integration_support::{
    LoglessCommitControl, PauseControl, incremented_value, init_db, mem, read_int,
    read_int_from_tx, rmw, write_int,
};

// Committed read-write transactions return before their write-back runs (it is
// spawned in the background), but a graceful shutdown drains the live tasks, so
// afterwards no transaction still holds locks.
#[tokio::test(start_paused = true)]
async fn shutdown_after_many_commits_drains_background_write_back() {
    let db = init_db(mem()).await;
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();

    for _ in 0..256 {
        let coll_ref = &coll;
        db.tx(|tx| async move {
            tx.write(coll_ref, b"k1", b"v1")?;
            Ok(())
        })
        .await
        .unwrap();
    }

    db.shutdown().await;

    let diag = db.diagnostics();
    assert!(
        diag.transactions.is_empty(),
        "shutdown should drain the background write-back and release locks: {diag:?}",
    );
}

#[tokio::test(start_paused = true)]
async fn clean_write_back_loss_retries_after_the_quiet_period() {
    let memory: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let backend = HookBackend::new(memory);
    let db = Database::builder("example", backend.clone())
        .retry_initial_interval(Duration::from_millis(1))
        .retry_max_interval(Duration::from_millis(1))
        .protocol_timing(ProtocolTiming::new(
            Duration::from_millis(250),
            Duration::from_millis(500),
            Duration::from_secs(10),
            Duration::from_secs(30),
        ))
        .open()
        .await
        .unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();

    let leaf_cas = Arc::new(AtomicUsize::new(0));
    backend.set_before({
        let leaf_cas = leaf_cas.clone();
        move |operation| {
            let result = match operation {
                BackendOp::WriteIf { path, .. } if path.ends_with("/_r") => {
                    let attempt = leaf_cas.fetch_add(1, Ordering::SeqCst);
                    if attempt == 1 {
                        Err(BackendError::Precondition)
                    } else {
                        Ok(())
                    }
                }
                _ => Ok(()),
            };
            Box::pin(async move { result })
        }
    });

    let value = vec![7; 4096];
    coll.write(b"key", &value).await.unwrap();
    for _ in 0..100 {
        if leaf_cas.load(Ordering::SeqCst) >= 2 && db.diagnostics().transactions.is_empty() {
            break;
        }
        tokio::time::advance(Duration::from_millis(10)).await;
    }
    assert_eq!(
        leaf_cas.load(Ordering::SeqCst),
        2,
        "the lock CAS and cleanly losing write-back CAS should have run"
    );
    assert!(
        db.diagnostics().transactions.is_empty(),
        "the initial write-back pass did not transfer ownership"
    );

    let fresh = Database::open("example", backend.clone()).await.unwrap();
    let fresh_coll = fresh
        .root_collection()
        .open_collection(b"demo-coll")
        .await
        .unwrap();
    assert_eq!(fresh_coll.read(b"key").await.unwrap().unwrap(), value);

    tokio::time::advance(Duration::from_secs(9)).await;
    tokio::task::yield_now().await;
    assert_eq!(leaf_cas.load(Ordering::SeqCst), 2);

    tokio::time::advance(Duration::from_secs(2)).await;
    for _ in 0..100 {
        if leaf_cas.load(Ordering::SeqCst) >= 3 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        leaf_cas.load(Ordering::SeqCst),
        3,
        "the delayed ordinary write-back should converge at the quiet deadline"
    );

    fresh.shutdown().await;
    db.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_forces_and_resumes_a_delayed_write_back() {
    type LeafGate = (oneshot::Sender<()>, oneshot::Receiver<()>);

    let memory: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let backend = HookBackend::new(memory);
    let db = Database::builder("example", backend.clone())
        .retry_initial_interval(Duration::from_millis(1))
        .retry_max_interval(Duration::from_millis(1))
        .protocol_timing(ProtocolTiming::new(
            Duration::from_millis(250),
            Duration::from_millis(500),
            Duration::from_secs(3600),
            Duration::from_secs(7200),
        ))
        .open()
        .await
        .unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();

    let leaf_cas = Arc::new(AtomicUsize::new(0));
    let (arrived_tx, arrived) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let gate = Arc::new(Mutex::new(Some((arrived_tx, released) as LeafGate)));
    backend.set_before({
        let leaf_cas = leaf_cas.clone();
        let gate = gate.clone();
        move |operation| {
            let (result, parked) = match operation {
                BackendOp::WriteIf { path, .. } if path.ends_with("/_r") => {
                    match leaf_cas.fetch_add(1, Ordering::SeqCst) {
                        1 => (Err(BackendError::Precondition), None),
                        2 => (Ok(()), gate.lock().unwrap().take()),
                        _ => (Ok(()), None),
                    }
                }
                _ => (Ok(()), None),
            };
            Box::pin(async move {
                if let Some((arrived, released)) = parked {
                    let _ = arrived.send(());
                    let _ = released.await;
                }
                result
            })
        }
    });

    let value = vec![9; 4096];
    coll.write(b"key", &value).await.unwrap();
    for _ in 0..100 {
        if leaf_cas.load(Ordering::SeqCst) >= 2 && db.diagnostics().transactions.is_empty() {
            break;
        }
        tokio::time::advance(Duration::from_millis(10)).await;
    }
    assert_eq!(leaf_cas.load(Ordering::SeqCst), 2);
    assert!(db.diagnostics().transactions.is_empty());

    let first_shutdown = tokio::spawn({
        let db = db.clone();
        async move { db.shutdown().await }
    });
    arrived.await.unwrap();
    assert!(!first_shutdown.is_finished());
    first_shutdown.abort();
    assert!(first_shutdown.await.unwrap_err().is_cancelled());

    let resumed_shutdown = tokio::spawn({
        let db = db.clone();
        async move { db.shutdown().await }
    });
    tokio::task::yield_now().await;
    assert!(!resumed_shutdown.is_finished());
    release.send(()).unwrap();
    resumed_shutdown.await.unwrap();

    let fresh = Database::open("example", backend).await.unwrap();
    let fresh_coll = fresh
        .root_collection()
        .open_collection(b"demo-coll")
        .await
        .unwrap();
    assert_eq!(fresh_coll.read(b"key").await.unwrap().unwrap(), value);
    fresh.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn queued_write_back_reroutes_after_a_root_split() {
    let memory: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let backend = HookBackend::new(memory);
    let db = Database::builder("example", backend.clone())
        .retry_initial_interval(Duration::from_millis(1))
        .retry_max_interval(Duration::from_millis(1))
        .inline_policy(InlinePolicy {
            max_value_bytes: 8,
            max_leaf_bytes: 8,
        })
        .protocol_timing(ProtocolTiming::new(
            Duration::from_millis(250),
            Duration::from_millis(500),
            Duration::from_secs(3600),
            Duration::from_secs(7200),
        ))
        .open()
        .await
        .unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();
    coll.write(b"a", &write_int(0)).await.unwrap();
    coll.write(b"b", &write_int(0)).await.unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;

    let leaf_cas = Arc::new(AtomicUsize::new(0));
    backend.set_before({
        let leaf_cas = leaf_cas.clone();
        move |operation| {
            let result = match operation {
                BackendOp::WriteIf { path, .. } if path.ends_with("/_r") => {
                    let attempt = leaf_cas.fetch_add(1, Ordering::SeqCst);
                    if attempt == 1 {
                        Err(BackendError::Precondition)
                    } else {
                        Ok(())
                    }
                }
                _ => Ok(()),
            };
            Box::pin(async move { result })
        }
    });

    let value = vec![5; 4096];
    coll.write(b"queued", &value).await.unwrap();
    for _ in 0..100 {
        if leaf_cas.load(Ordering::SeqCst) >= 2 && db.diagnostics().transactions.is_empty() {
            break;
        }
        tokio::time::advance(Duration::from_millis(10)).await;
    }
    assert_eq!(leaf_cas.load(Ordering::SeqCst), 2);
    assert!(db.diagnostics().transactions.is_empty());

    rmw(&db, &coll, b"a", 1).await.unwrap();
    rmw(&db, &coll, b"b", 1).await.unwrap();
    let before_split = db.stats().splitter.inline_pressure.completed;
    let mut completed = before_split;
    for _ in 0..5 {
        tokio::time::advance(Duration::from_secs(1)).await;
        completed = db.stats().splitter.inline_pressure.completed;
        if completed > before_split {
            break;
        }
    }
    assert!(
        completed > before_split,
        "inline-pressure split did not land"
    );

    db.shutdown().await;
    let fresh = Database::open("example", backend).await.unwrap();
    let fresh_coll = fresh
        .root_collection()
        .open_collection(b"demo-coll")
        .await
        .unwrap();
    assert_eq!(fresh_coll.read(b"queued").await.unwrap().unwrap(), value);
    fresh.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_rejects_every_public_async_entry_point() {
    let db = init_db(mem()).await;
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();

    db.shutdown().await;

    assert!(matches!(coll.read(b"key").await, Err(Error::ShuttingDown)));
    assert!(matches!(
        coll.read_stale(b"key", std::time::Duration::ZERO).await,
        Err(Error::ShuttingDown)
    ));
    assert!(matches!(
        coll.create_collection(b"child").await,
        Err(Error::ShuttingDown)
    ));
    assert!(matches!(
        coll.iter_collections().await,
        Err(Error::ShuttingDown)
    ));
    assert!(matches!(
        db.tx(|_| async { Ok::<(), Error>(()) }).await,
        Err(Error::ShuttingDown)
    ));
}
/// Dropping a `Database::tx` future mid-flight (e.g. via `tokio::time::timeout`)
/// must not corrupt anything and must not leave the database unusable. The
/// next transaction observes the committed state (or the absence of one) and
/// completes promptly.
#[tokio::test(start_paused = true)]
async fn cancelled_tx_future_does_not_block_followups() {
    use std::time::Duration;

    let db = init_db(mem()).await;
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"c")
        .await
        .unwrap();
    coll.write(b"k", &write_int(1)).await.unwrap();

    let coll_ref = &coll;
    // The closure stages a write and then blocks forever; the outer timeout
    // drops the entire `Database::tx` future. Because engine attempts begin only
    // after a closure returns, cancelling here discards the staged state without
    // requiring engine cleanup.
    let r = tokio::time::timeout(Duration::from_millis(50), async {
        db.tx(|tx| async move {
            let _ = read_int_from_tx(&tx, coll_ref, b"k").await?;
            tx.write(coll_ref, b"k", &write_int(99))?;
            std::future::pending::<()>().await;
            Ok(())
        })
        .await
    })
    .await;
    assert!(r.is_err(), "expected timeout, got {r:?}");

    // The cancelled tx never committed, so the original value still wins.
    let val = coll.read(b"k").await.unwrap().unwrap();
    assert_eq!(read_int(&val), 1);

    // A normal RMW still runs and commits without contention.
    rmw(&db, &coll, b"k", 1).await.unwrap();
    let val = coll.read(b"k").await.unwrap().unwrap();
    assert_eq!(read_int(&val), 2);
}

/// A dropped attempt on the logless one-CAS path (ADR-051) must not write an
/// aborted transaction object. That id never took a logged identity: it is
/// invisible to peers, holds no lock, and — once its CAS is dispatched — may in
/// fact have committed, so an abort marker would be both pointless and a lie.
#[tokio::test(start_paused = true)]
async fn cancelled_logless_commit_writes_no_aborted_object() {
    use std::time::Duration;

    let control = LoglessCommitControl::wrap(mem());
    let db = Database::open("example", control.backend()).await.unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"c")
        .await
        .unwrap();
    coll.write(b"k", &write_int(1)).await.unwrap();
    // Let the seed's background write-back finish so the gate traps the commit
    // under test rather than a lingering leaf CAS.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let (arrived, release) = control.arm();

    // A lone small overwrite of an existing key: eligible for the logless path,
    // whose commit is the parked leaf CAS.
    let stalled = tokio::spawn({
        let db = db.clone();
        let coll = coll.clone();
        async move {
            let coll_ref = &coll;
            db.tx(|tx| async move { tx.write(coll_ref, b"k", &write_int(42)) })
                .await
        }
    });
    arrived.await.unwrap();
    stalled.abort();
    let _ = stalled.await;

    // Let the parked CAS finish and any scheduled cleanup run.
    let _ = release.send(());
    tokio::time::sleep(Duration::from_secs(1)).await;
    db.shutdown().await;

    assert_eq!(
        control.aborted_writes(),
        0,
        "a cancelled logless attempt must not invent an aborted transaction"
    );
}

/// When a `Database::tx` future is dropped after a lock CAS lands but before
/// terminal commit dispatch, its internal guard pins the identity as wounded.
/// Peers can then release its locks without waiting for the lock lease.
#[tokio::test(start_paused = true)]
async fn cancelled_tx_during_commit_unblocks_peer_promptly() {
    use std::time::Duration;

    let (backend, pause) = PauseControl::wrap(mem());
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"c")
        .await
        .unwrap();
    coll.write(b"k1", &write_int(1)).await.unwrap();
    coll.write(b"k2", &write_int(2)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    let (lock_landed, release_lock) = pause.arm_leaf_write_gate();

    // Spawn a tx that writes two distinct keys, so it goes through the
    // standard locked commit path (the single-RW fast path requires 1
    // read + 1 write on the same key and would skip the tx-log write).
    let stalled = tokio::spawn({
        let db = db.clone();
        let coll = coll.clone();
        async move {
            let coll_ref = &coll;
            db.tx(|tx| async move {
                tx.write(coll_ref, b"k1", &write_int(42))?;
                tx.write(coll_ref, b"k2", &write_int(43))
            })
            .await
        }
    });

    // The lock is externally visible, but the owner has not observed the CAS
    // completion and cannot have dispatched its terminal commit.
    lock_landed.await.unwrap();

    // Drop the future. `TransactionAbortGuard::drop` fires here, calling
    // `Algo::async_abort` which spawns a background task that writes the
    // pinned Wounded marker to the tx log via the (now-disarmed) backend.
    stalled.abort();
    let _ = stalled.await;
    drop(release_lock);

    // A peer transaction on the same keys must complete quickly. Without
    // the wound marker it would spin on the locks until the 15-second
    // lease expires; with it, the locker treats `Wounded` as aborted and
    // overrides.
    let coll_ref = &coll;
    let r = tokio::time::timeout(
        Duration::from_secs(5),
        db.tx(|tx| async move {
            let n1 = read_int_from_tx(&tx, coll_ref, b"k1").await?;
            let n2 = read_int_from_tx(&tx, coll_ref, b"k2").await?;
            tx.write(coll_ref, b"k1", &incremented_value(b"k1", n1, 10)?)?;
            tx.write(coll_ref, b"k2", &incremented_value(b"k2", n2, 10)?)
        }),
    )
    .await;
    let r = r.expect("peer tx timed out: TransactionAbortGuard didn't release the lock promptly");
    r.unwrap();

    // The cancelled tx never committed (its values 42/43 are gone); the
    // peer's reads observed the original values and incremented from there.
    let v1 = coll.read(b"k1").await.unwrap().unwrap();
    assert_eq!(read_int(&v1), 11);
    let v2 = coll.read(b"k2").await.unwrap().unwrap();
    assert_eq!(read_int(&v2), 12);
}

/// The locked path installs its lock before writing its committed transaction
/// object, so a future dropped in that window leaves a lock behind whose object
/// never landed. The path takes its logged identity *before* those writes, so the
/// cancellation guard can finalize the id: a peer then resolves the abandoned
/// holder immediately instead of waiting out the unknown-transaction grace
/// period.
#[tokio::test(start_paused = true)]
async fn cancelled_single_rw_commit_unblocks_peer_promptly() {
    use std::time::Duration;

    // Over the inline budget, so the commit takes the locked path rather than the
    // logless one-CAS path (ADR-051), which takes no identity at all.
    fn padded(tag: u8) -> Vec<u8> {
        vec![tag; 2048]
    }

    let (backend, pause) = PauseControl::wrap(mem());
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"c")
        .await
        .unwrap();
    coll.write(b"k", &padded(1)).await.unwrap();
    // Drain the seed's background write-back (the paused clock only advances
    // once every task is idle) so the leaf CAS awaited below is the one this
    // test is about.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Park the lock CAS after it lands. Dropping the future here leaves exactly
    // the holder-without-an-object state, before terminal commit dispatch.
    let (installed, release_lock) = pause.arm_leaf_write_gate();
    let stalled = tokio::spawn({
        let db = db.clone();
        let coll = coll.clone();
        async move {
            let coll_ref = &coll;
            db.tx(|tx| async move {
                tx.read(coll_ref, b"k").await?;
                tx.write(coll_ref, b"k", &padded(42))
            })
            .await
        }
    });
    installed.await.unwrap();
    stalled.abort();
    let _ = stalled.await;
    drop(release_lock);

    let coll_ref = &coll;
    let peer = tokio::time::timeout(
        Duration::from_secs(5),
        db.tx(|tx| async move {
            tx.read(coll_ref, b"k").await?;
            tx.write(coll_ref, b"k", &padded(7))
        }),
    )
    .await
    .expect("peer tx timed out: the cancelled commit left an unresolvable holder");
    peer.unwrap();

    let value = coll.read(b"k").await.unwrap().unwrap();
    assert_eq!(value[0], 7, "the cancelled attempt never committed");
}

/// Clean shutdown waits for the async wound scheduled when a transaction is
/// cancelled after publishing a holder but before terminal commit dispatch.
#[tokio::test(start_paused = true)]
async fn shutdown_waits_for_cancelled_tx_async_abort() {
    use std::time::Duration;

    let (backend, pause) = PauseControl::wrap(mem());
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"c")
        .await
        .unwrap();
    coll.write(b"k1", &write_int(1)).await.unwrap();
    coll.write(b"k2", &write_int(2)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    let (lock_landed, release_lock) = pause.arm_leaf_write_gate();
    let (wound_arrived, release_wound) = pause.arm_wound_write_gate();

    let stalled = tokio::spawn({
        let db = db.clone();
        let coll = coll.clone();
        async move {
            let coll_ref = &coll;
            db.tx(|tx| async move {
                tx.write(coll_ref, b"k1", &write_int(42))?;
                tx.write(coll_ref, b"k2", &write_int(43))
            })
            .await
        }
    });

    lock_landed.await.unwrap();
    stalled.abort();
    let _ = stalled.await;
    drop(release_lock);

    let shutdown = tokio::spawn({
        let db = db.clone();
        async move {
            db.shutdown().await;
        }
    });

    tokio::time::timeout(Duration::from_secs(1), wound_arrived)
        .await
        .expect("async wound did not start during shutdown")
        .unwrap();

    for _ in 0..10 {
        tokio::task::yield_now().await;
        assert!(
            !shutdown.is_finished(),
            "shutdown returned before async wound completed"
        );
    }

    release_wound.send(()).unwrap();
    shutdown.await.unwrap();
}
