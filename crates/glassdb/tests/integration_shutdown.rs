//! Shutdown and cancellation integration behavior.

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::FutureExt;
use glassdb::{Database, Error, InlinePolicy};

pub mod integration_support;

use integration_support::{
    LoglessCommitControl, PauseControl, PreparedCollectionRecoveryControl,
    RetirementFailureControl, incremented_value, init_db, mem, read_int, read_int_from_tx, rmw,
    write_int,
};

const STALE_BODY_PANIC: &str = "panic after observing a stale snapshot";
const RETAINED_LOCK_PANIC: &str = "panicking locked replay";
const PREPARED_COLLECTION_PANIC: &str = "panic with a prepared collection";

fn panic_after_stale_read() -> ! {
    std::panic::panic_any(STALE_BODY_PANIC)
}

fn panic_during_locked_replay() -> ! {
    std::panic::panic_any(RETAINED_LOCK_PANIC)
}

fn panic_with_prepared_collection() -> ! {
    std::panic::panic_any(PREPARED_COLLECTION_PANIC)
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
    // after a transaction body returns, cancelling here discards the staged state without
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

/// A panic interrupts the transaction instead of returning a body outcome. Even
/// if a read becomes stale, it escapes without validation or replay and all
/// body-local changes remain unpublished.
#[tokio::test]
async fn first_execution_stale_panic_discards_staged_data_and_catalog_changes() {
    let backend = mem();
    let setup = Database::open("example", backend.clone()).await.unwrap();
    let setup_coll = setup
        .root_collection()
        .create_collection_if_absent(b"c")
        .await
        .unwrap();
    setup_coll.write(b"guard", b"old").await.unwrap();
    setup.shutdown().await;

    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db.open_collection("c").await.unwrap();
    let invalidator = Database::open("example", backend).await.unwrap();
    let invalidator_coll = invalidator.open_collection("c").await.unwrap();
    let executions = AtomicUsize::new(0);

    let outcome = AssertUnwindSafe(db.tx(|tx| {
        executions.fetch_add(1, Ordering::SeqCst);
        let coll = coll.clone();
        let invalidator = invalidator.clone();
        let invalidator_coll = invalidator_coll.clone();
        async move {
            tx.read(&coll, b"guard").await?;
            tx.write(&coll, b"staged", b"not-visible")?;
            let temporary = tx
                .create_collection(&tx.root_collection(), b"temporary")
                .await?;
            tx.write(&temporary, b"staged", b"not-visible")?;
            invalidator_coll.write(b"guard", b"new").await?;
            invalidator.shutdown().await;
            panic_after_stale_read();
            #[allow(unreachable_code)]
            Ok::<(), Error>(())
        }
    }))
    .catch_unwind()
    .await;

    let payload = outcome.expect_err("the transaction body should propagate its panic");
    assert_eq!(
        payload.downcast_ref::<&'static str>(),
        Some(&STALE_BODY_PANIC)
    );
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert_eq!(coll.read(b"staged").await.unwrap(), None);
    assert!(!db.collection_exists("temporary").await.unwrap());
    db.shutdown().await;
}

/// A stale locked attempt replays its body while retaining its identity and
/// locks. If that replay panics before returning a future, ownership of those
/// resources is handed to recovery before the unwind reaches the caller.
#[tokio::test(start_paused = true)]
async fn panicking_locked_replay_hands_off_its_retained_resources() {
    use std::time::Duration;

    let backend = mem();
    let setup = Database::builder("example", backend.clone())
        .inline_policy(InlinePolicy::none())
        .open()
        .await
        .unwrap();
    let setup_coll = setup
        .root_collection()
        .create_collection_if_absent(b"c")
        .await
        .unwrap();
    setup_coll.write(b"k", &write_int(1)).await.unwrap();
    setup.shutdown().await;

    let db = Database::builder("example", backend.clone())
        .inline_policy(InlinePolicy::none())
        .open()
        .await
        .unwrap();
    let coll = db.open_collection("c").await.unwrap();
    let invalidator = Database::builder("example", backend)
        .inline_policy(InlinePolicy::none())
        .open()
        .await
        .unwrap();
    let invalidator_coll = invalidator.open_collection("c").await.unwrap();

    let executions = AtomicUsize::new(0);
    let outcome = AssertUnwindSafe(db.tx(|tx| {
        let execution = executions.fetch_add(1, Ordering::SeqCst);
        if execution == 1 {
            panic_during_locked_replay();
        }
        let coll = coll.clone();
        let invalidator = invalidator.clone();
        let invalidator_coll = invalidator_coll.clone();
        async move {
            let observed = read_int_from_tx(&tx, &coll, b"k").await?;
            if execution == 0 {
                invalidator_coll.write(b"k", &write_int(100)).await?;
                invalidator.shutdown().await;
            }
            tx.write(&coll, b"k", &incremented_value(b"k", observed, 1)?)
        }
    }))
    .catch_unwind()
    .await;

    let payload = outcome.expect_err("the replay should propagate its panic");
    assert_eq!(
        payload.downcast_ref::<&'static str>(),
        Some(&RETAINED_LOCK_PANIC)
    );
    assert_eq!(
        executions.load(Ordering::SeqCst),
        2,
        "the panicking body is not validated or replayed"
    );
    let peer = tokio::time::timeout(Duration::from_secs(5), coll.write(b"k", &write_int(7)))
        .await
        .expect("peer waited for the interrupted transaction's full lease");
    peer.unwrap();
    assert_eq!(
        read_int(&coll.read(b"k").await.unwrap().unwrap()),
        7,
        "the interrupted execution's staged value stays invisible"
    );

    db.shutdown().await;
}

/// Collection preparation happens before locked validation. A panic on the
/// retained replay must leave the logical name absent while durable recovery
/// later reclaims the unreachable physical incarnation.
#[tokio::test(start_paused = true)]
async fn panicking_locked_replay_records_its_prepared_collection_for_recovery() {
    let recovery = PreparedCollectionRecoveryControl::wrap(mem());
    let setup = Database::open("example", recovery.backend()).await.unwrap();
    let setup_coll = setup
        .root_collection()
        .create_collection_if_absent(b"c")
        .await
        .unwrap();
    setup_coll.write(b"guard", b"old").await.unwrap();
    setup.shutdown().await;

    let db = Database::open("example", recovery.backend()).await.unwrap();
    let coll = db.open_collection("c").await.unwrap();
    let invalidator = Database::open("example", recovery.backend()).await.unwrap();
    let invalidator_coll = invalidator.open_collection("c").await.unwrap();
    let (prepared, retired) = recovery.arm();
    let executions = AtomicUsize::new(0);

    let outcome = AssertUnwindSafe(db.tx(|tx| {
        let execution = executions.fetch_add(1, Ordering::SeqCst);
        if execution == 1 {
            panic_with_prepared_collection();
        }
        let coll = coll.clone();
        let invalidator = invalidator.clone();
        let invalidator_coll = invalidator_coll.clone();
        async move {
            tx.read(&coll, b"guard").await?;
            let temporary = tx
                .create_collection(&tx.root_collection(), b"temporary")
                .await?;
            tx.write(&temporary, b"staged", b"not-visible")?;
            invalidator_coll.write(b"guard", b"new").await?;
            invalidator.shutdown().await;
            Ok::<(), Error>(())
        }
    }))
    .catch_unwind()
    .await;

    let payload = outcome.expect_err("the retained replay should propagate its panic");
    assert_eq!(
        payload.downcast_ref::<&'static str>(),
        Some(&PREPARED_COLLECTION_PANIC)
    );
    assert_eq!(executions.load(Ordering::SeqCst), 2);
    prepared
        .await
        .expect("collection preparation was not observed");
    let prepared_collections = retired
        .await
        .expect("interrupted transaction retirement was not observed");
    assert_eq!(prepared_collections, 1);
    assert!(!db.collection_exists("temporary").await.unwrap());

    db.shutdown().await;
}

/// If ordinary finalization fails, the body outcome still wins and the armed
/// guard transfers the held identity to managed retirement.
#[tokio::test(start_paused = true)]
async fn failed_finalization_keeps_the_retirement_guard_armed() {
    use std::time::Duration;

    let retirement = RetirementFailureControl::wrap(mem());
    let setup = Database::open("example", retirement.backend())
        .await
        .unwrap();
    let setup_coll = setup
        .root_collection()
        .create_collection_if_absent(b"c")
        .await
        .unwrap();
    setup_coll.write(b"guard", b"old").await.unwrap();
    setup.shutdown().await;

    let db = Database::open("example", retirement.backend())
        .await
        .unwrap();
    let coll = db.open_collection("c").await.unwrap();
    let invalidator = Database::open("example", retirement.backend())
        .await
        .unwrap();
    let invalidator_coll = invalidator.open_collection("c").await.unwrap();
    let executions = AtomicUsize::new(0);
    let (failed, recovered) = retirement.observe();

    let result = db
        .tx(|tx| {
            let execution = executions.fetch_add(1, Ordering::SeqCst);
            let coll = coll.clone();
            let invalidator = invalidator.clone();
            let invalidator_coll = invalidator_coll.clone();
            let retirement = retirement.clone();
            async move {
                tx.read(&coll, b"guard").await?;
                if execution == 0 {
                    invalidator_coll.write(b"guard", b"new").await?;
                    invalidator.shutdown().await;
                } else {
                    retirement.arm();
                }
                Err::<(), _>(Error::InvalidInput("body outcome".into()))
            }
        })
        .await;

    assert!(matches!(
        result,
        Err(Error::InvalidInput(message)) if message == "body outcome"
    ));
    assert_eq!(executions.load(Ordering::SeqCst), 2);
    failed
        .await
        .expect("synchronous finalization did not reach the injected failure");
    tokio::time::timeout(Duration::from_secs(1), recovered)
        .await
        .expect("managed retirement did not retry the failed finalization")
        .expect("retirement recovery acknowledgement was dropped");

    tokio::time::timeout(Duration::from_secs(5), coll.write(b"guard", b"peer"))
        .await
        .expect("peer waited for the unfinished transaction's full lease")
        .unwrap();
    db.shutdown().await;
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
    let db = Database::builder("example", backend.clone())
        .inline_policy(InlinePolicy::none())
        .open()
        .await
        .unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"c")
        .await
        .unwrap();
    coll.write(b"k1", &write_int(1)).await.unwrap();
    coll.write(b"k2", &write_int(2)).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    let (lock_landed, release_lock) = pause.arm_leaf_write_gate();

    // Inline publication is disabled so the transaction reaches the standard
    // locked commit path and its transaction-log write.
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

    // Drop the future. The engine transaction's retirement guard starts a
    // background task that writes the pinned Wounded marker to the tx log via
    // the now-disarmed backend.
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
    let r = r.expect("peer tx timed out: retirement didn't release the lock promptly");
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
/// cancellation guard can finalize the identity. A peer then resolves the
/// cancelled holder immediately instead of waiting out the unknown-transaction
/// grace period.
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
async fn shutdown_waits_for_cancelled_tx_retirement() {
    use std::time::Duration;

    let (backend, pause) = PauseControl::wrap(mem());
    let db = Database::builder("example", backend.clone())
        .inline_policy(InlinePolicy::none())
        .open()
        .await
        .unwrap();
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
