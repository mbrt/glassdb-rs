use std::sync::Arc;
use std::time::Duration;

use glassdb::backend::{BackendError, ListLimit, memory::MemoryBackend};
use glassdb::middleware::{BackendOp, HookBackend};
use glassdb::{Backend, Database, GcLimits, InlinePolicy};
use glassdb_concurr::{exec, rt};
use glassdb_storage::transaction::TxCommitStatus;
use glassdb_trans::ProtocolTiming;

#[test]
fn reopening_retains_the_stored_gc_safety_horizon() {
    exec::block_on(async {
        let backend = Arc::new(MemoryBackend::new());
        let creator = Database::builder("timing", backend.clone())
            .protocol_timing(ProtocolTiming::simulation())
            .inline_policy(InlinePolicy::none())
            .open()
            .await
            .unwrap();
        creator
            .root_collection()
            .write(b"key", b"old")
            .await
            .unwrap();
        creator.shutdown().await;
        let listed = backend
            .list("timing/_t/", None, ListLimit::new(100).unwrap())
            .await
            .unwrap();
        assert_eq!(listed.objects.len(), 1);
        let old_path = &listed.objects[0];

        let reopened = Database::builder("timing", backend.clone())
            .protocol_timing(ProtocolTiming::new(
                Duration::from_millis(50),
                Duration::ZERO,
            ))
            .inline_policy(InlinePolicy::none())
            .open()
            .await
            .unwrap();
        reopened
            .root_collection()
            .write(b"key", b"new")
            .await
            .unwrap();
        rt::sleep(Duration::from_millis(700)).await;
        assert!(backend.read(old_path).await.is_ok());
        rt::sleep(Duration::from_millis(100)).await;
        assert!(matches!(
            backend.read(old_path).await,
            Err(BackendError::NotFound)
        ));
        assert_eq!(
            reopened.root_collection().read(b"key").await.unwrap(),
            Some(b"new".to_vec())
        );
        reopened.shutdown().await;
    });
}

#[test]
fn gc_preserves_wounded_preparations_and_the_replayed_collection() {
    use glassdb_data::{CollectionAddress, ObjectPath};
    use glassdb_storage::InlinePolicy;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    exec::block_on(async {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let db = Database::builder("renewal", backend.clone())
            .protocol_timing(ProtocolTiming::simulation())
            .inline_policy(InlinePolicy::none())
            .open()
            .await
            .unwrap();
        let root_path = ObjectPath::TreeRoot {
            collection: CollectionAddress::root("renewal"),
        }
        .to_string();
        let remaining = Arc::new(AtomicUsize::new(3 * 50));
        let prepared_roots = Arc::new(Mutex::new(Vec::new()));
        backend.set_before({
            let remaining = remaining.clone();
            let prepared_roots = prepared_roots.clone();
            move |op| {
                if let BackendOp::WriteIfNotExists { path, .. } = op
                    && path.ends_with("/_r")
                    && *path != root_path
                {
                    prepared_roots.lock().unwrap().push(path.to_string());
                }
                let fail = matches!(op, BackendOp::WriteIf { path, .. } if *path == root_path)
                    && remaining
                        .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                        .is_ok();
                Box::pin(async move {
                    if fail {
                        Err(BackendError::Precondition)
                    } else {
                        Ok(())
                    }
                })
            }
        });
        let child = db
            .tx(|tx| async move {
                let root = tx.root_collection();
                let child = tx.create_collection(&root, b"child").await?;
                tx.write(&root, b"key", b"value")?;
                Ok(child)
            })
            .await
            .unwrap();
        backend.clear_before();
        let roots = prepared_roots.lock().unwrap().clone();
        assert_eq!(remaining.load(Ordering::SeqCst), 0);
        assert_eq!(roots.len(), 2);
        assert_ne!(roots[0], roots[1]);
        db.tx(|tx| {
            let child = child.clone();
            async move { tx.write(&child, b"key", b"survives GC") }
        })
        .await
        .unwrap();
        rt::sleep(Duration::from_secs(2)).await;
        // The interrupted acquisition cannot prove retirement. GC must leave
        // its prepared root with the pinned wound, even after repeated scans.
        let gc = db.stats().gc;
        assert!(gc.lists > 0, "{gc:?}");
        assert_eq!(gc.progress, 0, "{gc:?}");
        backend.read(&roots[0]).await.unwrap();
        assert_eq!(
            child.read(b"key").await.unwrap(),
            Some(b"survives GC".to_vec())
        );
        db.shutdown().await;
    });
}

#[test]
fn a_hint_survives_the_safety_horizon_without_successful_gc_scans() {
    exec::block_on(async {
        // The write-back of a commit waits for the batching window (ADR-087),
        // so the checks wait for it after each commit.
        let window = Duration::from_millis(1);
        let after_write_back = window + Duration::from_millis(1);
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let db = Database::builder("gc", backend.clone())
            .protocol_timing(ProtocolTiming::simulation())
            .batching_window(window)
            .open()
            .await
            .unwrap();
        let collection = db.root_collection();
        let first = vec![1; 4096];
        db.tx(|tx| {
            let collection = collection.clone();
            let value = first.clone();
            async move { tx.write(&collection, b"key", &value) }
        })
        .await
        .unwrap();
        rt::sleep(after_write_back).await;
        let listed = backend
            .list("gc/_t/", None, ListLimit::new(1000).unwrap())
            .await
            .unwrap();
        assert_eq!(listed.objects.len(), 1);
        let old_path = listed.objects[0].clone();
        backend.set_before(|op| {
            let fail = matches!(op, BackendOp::List { path, .. } if *path == "gc/_t/");
            Box::pin(async move {
                if fail {
                    Err(BackendError::Unavailable("LIST disabled".into()))
                } else {
                    Ok(())
                }
            })
        });
        let second = vec![2; 4096];
        db.tx(|tx| {
            let collection = collection.clone();
            let value = second.clone();
            async move { tx.write(&collection, b"key", &value) }
        })
        .await
        .unwrap();
        rt::sleep(after_write_back).await;
        let gc = db.diagnostics().gc;
        assert!(gc.deferred > 0, "{gc:?}");
        assert!(backend.read(&old_path).await.is_ok());
        rt::sleep(Duration::from_millis(700)).await;
        assert!(backend.read(&old_path).await.is_ok());
        rt::sleep(Duration::from_millis(100)).await;
        assert!(matches!(
            backend.read(&old_path).await,
            Err(BackendError::NotFound)
        ));
        db.shutdown().await;
    });
}

#[test]
fn gc_hint_limits_preserve_writes_and_scan_reclamation() {
    exec::block_on(async {
        let keys = [b"a", b"b", b"c", b"d", b"e"];
        for (pending, retained) in [(0, 5), (5, 0), (2, 5), (5, 2), (2, 1)] {
            let backend = Arc::new(MemoryBackend::new());
            let writer = Database::builder("gc", backend.clone())
                .inline_policy(InlinePolicy::none())
                .open()
                .await
                .unwrap();
            for key in keys {
                writer.root_collection().write(key, b"old").await.unwrap();
            }
            writer.shutdown().await;
            let listed = backend
                .list("gc/_t/", None, ListLimit::new(1000).unwrap())
                .await
                .unwrap();
            assert_eq!(listed.objects.len(), keys.len());

            let db = Database::builder("gc", backend.clone())
                .gc_limits(GcLimits {
                    max_pending_hints: pending,
                    max_hint_candidates: retained,
                    ..GcLimits::default()
                })
                .open()
                .await
                .unwrap();
            let collection = db.root_collection();
            db.tx(|tx| {
                let collection = collection.clone();
                async move {
                    for key in keys {
                        tx.write(&collection, key, b"new")?;
                    }
                    Ok(())
                }
            })
            .await
            .unwrap();
            rt::sleep(Duration::from_millis(1)).await;

            let admitted = pending.min(retained);
            let backlog = db.diagnostics().gc;
            assert_eq!(backlog.deferred, admitted as u64);
            assert_eq!(backlog.ready, 0);
            assert_eq!(backlog.in_flight, 0);
            let stats = db.stats();
            assert_eq!(stats.gc.dropped_candidates, (keys.len() - admitted) as u64);
            assert_eq!(stats.gc.lists, 0);

            rt::sleep(Duration::from_secs(120)).await;
            assert!(db.stats().gc.lists > 0);
            for path in listed.objects {
                assert!(matches!(
                    backend.read(&path).await,
                    Err(BackendError::NotFound)
                ));
            }
            for key in keys {
                assert_eq!(
                    collection.read(key).await.unwrap().as_deref(),
                    Some(b"new".as_slice())
                );
            }
            db.shutdown().await;
        }
    });
}

// GC releases the locks of a committed holder only after no key lock names it.
// So it does not end the membership lock of a holder whose write-back has not
// landed, and a scan still omits the key that the holder deleted (ADR-077).
#[test]
fn gc_keeps_the_membership_lock_of_a_committed_delete_before_its_write_back() {
    use std::sync::atomic::{AtomicBool, Ordering};

    exec::block_on(async {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let open = || {
            Database::builder("held", backend.clone())
                .protocol_timing(ProtocolTiming::simulation())
                .open()
        };
        let deleter = open().await.unwrap();
        let collection = deleter.root_collection();
        collection.write(b"deleted", b"v").await.unwrap();

        // Holds the first leaf write after the transaction record turns
        // committed, which is the write-back of the delete.
        let committed = Arc::new(AtomicBool::new(false));
        let held = Arc::new(AtomicBool::new(false));
        backend.set_before({
            let committed = committed.clone();
            let held = held.clone();
            move |op| {
                let hold = match op {
                    BackendOp::WriteIf { path, value, .. }
                    | BackendOp::WriteIfNotExists { path, value }
                        if path.contains("/_t/") =>
                    {
                        if glassdb_storage::txrecord::status(value)
                            .is_ok_and(|status| status == TxCommitStatus::Committed)
                        {
                            committed.store(true, Ordering::SeqCst);
                        }
                        false
                    }
                    BackendOp::WriteIf { path, .. } if path.ends_with("/_r") => committed
                        .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok(),
                    _ => false,
                };
                if hold {
                    held.store(true, Ordering::SeqCst);
                }
                Box::pin(async move {
                    if hold {
                        std::future::pending::<()>().await;
                    }
                    Ok(())
                })
            }
        });
        rt::spawn({
            let deleter = deleter.clone();
            async move {
                // The scan keeps the delete from a direct commit, so the delete
                // locks the key and the membership.
                let _ = deleter
                    .tx(|tx| async move {
                        let root = tx.root_collection();
                        tx.scan_keys(&root, glassdb::KeyScan::all()).await?;
                        tx.delete(&root, b"deleted")
                    })
                    .await;
            }
        });
        while !held.load(Ordering::SeqCst) {
            rt::sleep(Duration::from_millis(1)).await;
        }

        let collector = open().await.unwrap();
        rt::sleep(Duration::from_secs(5)).await;
        let gc = collector.stats().gc;
        assert!(gc.lists > 0, "{gc:?}");

        let scanner = open().await.unwrap();
        let scan = scanner
            .root_collection()
            .scan_keys(glassdb::KeyScan::all())
            .await
            .unwrap();
        assert!(scan.keys().is_empty(), "{:?}", scan.keys());
    });
}

// Regression: a stale view reads a leaf state that names the committed holder
// T after GC on the same instance deleted the record of T. A writer with a
// cached leaf from before the write-back wounded T at the empty path. The read
// must resolve the key again and return its current value, not report that the
// final status and the record disagree.
#[test]
fn a_stale_view_reads_through_a_leaked_wound_of_a_reclaimed_commit() {
    use std::num::NonZeroUsize;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::Notify;

    fn is_record_delete(operation: &BackendOp<'_>) -> bool {
        matches!(operation, BackendOp::DeleteIf { path, .. } if path.contains("/_t/"))
    }

    fn unavailable(fail: bool) -> Result<(), BackendError> {
        if fail {
            Err(BackendError::Unavailable("injected".into()))
        } else {
            Ok(())
        }
    }

    exec::block_on(async {
        let memory = Arc::new(MemoryBackend::new());
        let owner_backend = HookBackend::new(memory.clone());
        let writer_backend = HookBackend::new(memory.clone());
        // Values stay out of the leaves, so a read needs the record of the
        // writer (ADR-051).
        let open = |backend: Arc<dyn Backend>| {
            Database::builder("leak", backend)
                .protocol_timing(ProtocolTiming::simulation())
                .inline_policy(InlinePolicy::none())
                .open()
        };
        let owner = open(owner_backend.clone()).await.unwrap();
        let writer = open(writer_backend.clone()).await.unwrap();
        let reader = open(memory.clone()).await.unwrap();
        owner.root_collection().write(b"key", b"old").await.unwrap();

        // Only the GC of the reader deletes records, so the reader loses the
        // body of T. The GC scan of the writer does not reach T: its check
        // reads the current leaf, which would refresh the cache of the writer.
        writer_backend.set_before(|operation| {
            let fail = is_record_delete(operation)
                || matches!(operation, BackendOp::List { path, .. } if path.contains("/_t/"));
            Box::pin(async move { unavailable(fail) })
        });

        // The writer starts before T, so it outranks T.
        let started = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let first = Arc::new(AtomicBool::new(true));
        let leak = rt::spawn({
            let writer = writer.clone();
            let (started, resume, first) = (started.clone(), resume.clone(), first.clone());
            async move {
                writer
                    .tx(|tx| {
                        let (started, resume, first) =
                            (started.clone(), resume.clone(), first.clone());
                        async move {
                            if first.swap(false, Ordering::SeqCst) {
                                started.notify_one();
                                resume.notified().await;
                            }
                            let c = tx.root_collection();
                            // Two blind writes take the locked path, which
                            // resolves the holders of the cached leaf.
                            tx.write(&c, b"key", b"writer")?;
                            tx.write(&c, b"other", b"writer")
                        }
                    })
                    .await
                    .unwrap();
            }
        });
        started.notified().await;
        rt::sleep(Duration::from_millis(1)).await;

        // The owner holds the commit of T while T holds the lock on the key.
        let held = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let holding = Arc::new(AtomicBool::new(false));
        let record_path = Arc::new(Mutex::new(String::new()));
        owner_backend.set_before({
            let (held, release, holding, record_path) = (
                held.clone(),
                release.clone(),
                holding.clone(),
                record_path.clone(),
            );
            move |operation| {
                let fail = is_record_delete(operation);
                let hold = match operation {
                    BackendOp::WriteIfNotExists { path, value }
                        if path.contains("/_t/")
                            && glassdb_storage::txrecord::status(value)
                                .is_ok_and(|status| status == TxCommitStatus::Committed)
                            && !holding.swap(true, Ordering::SeqCst) =>
                    {
                        *record_path.lock().unwrap() = path.to_string();
                        true
                    }
                    _ => false,
                };
                let (held, release) = (held.clone(), release.clone());
                Box::pin(async move {
                    if hold {
                        held.notify_one();
                        release.notified().await;
                    }
                    unavailable(fail)
                })
            }
        });
        let commit = rt::spawn({
            let owner = owner.clone();
            async move { owner.root_collection().write(b"key", b"new").await.unwrap() }
        });
        held.notified().await;
        let path = record_path.lock().unwrap().clone();

        // The writer and the reader cache the leaf with T as the holder, and
        // the stale view keeps that state.
        for db in [&writer, &reader] {
            assert_eq!(
                db.root_collection().read(b"key").await.unwrap(),
                Some(b"old".to_vec())
            );
        }
        let view = reader.root_collection().stale(Duration::from_secs(3600));
        assert_eq!(view.read(b"key").await.unwrap(), Some(b"old".to_vec()));

        release.notify_one();
        commit.await.unwrap();
        // The reader resolves T from its cached leaf and learns that T
        // committed.
        assert_eq!(
            reader.root_collection().read(b"key").await.unwrap(),
            Some(b"new".to_vec())
        );
        // A later writer replaces T, so GC can delete the record of T.
        owner
            .root_collection()
            .write(b"key", b"newer")
            .await
            .unwrap();
        loop {
            if matches!(memory.read(&path).await, Err(BackendError::NotFound)) {
                break;
            }
            let report = reader.collect_garbage(NonZeroUsize::MIN).await.unwrap();
            let retry_after = report
                .retry_after
                .expect("GC did not delete the record of T");
            rt::sleep(retry_after.max(Duration::from_millis(1))).await;
        }

        // The writer resolves T from its cached leaf and wounds it.
        resume.notify_one();
        leak.await.unwrap();
        let stored = memory.read(&path).await.unwrap();
        assert_eq!(
            glassdb_storage::txrecord::status(&stored.contents).unwrap(),
            TxCommitStatus::BlindWounded
        );

        assert_eq!(view.read(b"key").await.unwrap(), Some(b"writer".to_vec()));
        owner.shutdown().await;
        writer.shutdown().await;
        reader.shutdown().await;
    });
}

// Regression: T creates a key, and a stale view scans while T is committed but
// not yet written back. GC deletes the record of T, and a writer with a cached
// leaf from before the write-back fences T at the empty path. A later scan of
// the view cuts at a point after the commit point of T, so it must still list
// the key: the blind wound must not make the scan drop T (ADR-104).
#[test]
fn a_stale_scan_keeps_a_committed_creation_after_a_blind_wound() {
    use std::num::NonZeroUsize;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::Notify;

    fn is_record_delete(operation: &BackendOp<'_>) -> bool {
        matches!(operation, BackendOp::DeleteIf { path, .. } if path.contains("/_t/"))
    }

    fn unavailable(fail: bool) -> Result<(), BackendError> {
        if fail {
            Err(BackendError::Unavailable("injected".into()))
        } else {
            Ok(())
        }
    }

    fn keys(page: glassdb::KeyPage) -> Vec<Vec<u8>> {
        page.keys().to_vec()
    }

    exec::block_on(async {
        let memory = Arc::new(MemoryBackend::new());
        let owner_backend = HookBackend::new(memory.clone());
        let writer_backend = HookBackend::new(memory.clone());
        // Values stay out of the leaves, so a read needs the record of the
        // writer (ADR-051).
        let open = |backend: Arc<dyn Backend>| {
            Database::builder("leak", backend)
                .protocol_timing(ProtocolTiming::simulation())
                .inline_policy(InlinePolicy::none())
                .open()
        };
        let owner = open(owner_backend.clone()).await.unwrap();
        let writer = open(writer_backend.clone()).await.unwrap();
        let reader = open(memory.clone()).await.unwrap();
        owner.root_collection().write(b"base", b"b").await.unwrap();

        // Only the GC of the reader deletes records. The GC scan of the
        // writer does not reach T: its check reads the current leaf, which
        // would refresh the cache of the writer.
        writer_backend.set_before(|operation| {
            let fail = is_record_delete(operation)
                || matches!(operation, BackendOp::List { path, .. } if path.contains("/_t/"));
            Box::pin(async move { unavailable(fail) })
        });

        // The writer starts before T, so it outranks T. It writes two keys
        // that exist, so it takes the locked path and changes no membership.
        let started = Arc::new(Notify::new());
        let resume = Arc::new(Notify::new());
        let first = Arc::new(AtomicBool::new(true));
        let fence = rt::spawn({
            let writer = writer.clone();
            let (started, resume, first) = (started.clone(), resume.clone(), first.clone());
            async move {
                writer
                    .tx(|tx| {
                        let (started, resume, first) =
                            (started.clone(), resume.clone(), first.clone());
                        async move {
                            if first.swap(false, Ordering::SeqCst) {
                                started.notify_one();
                                resume.notified().await;
                            }
                            let c = tx.root_collection();
                            tx.write(&c, b"created", b"writer")?;
                            tx.write(&c, b"base", b"writer")
                        }
                    })
                    .await
                    .unwrap();
            }
        });
        started.notified().await;
        rt::sleep(Duration::from_millis(1)).await;

        // The owner holds the commit record of T, and then its write-back.
        let committing = Arc::new(Notify::new());
        let resume_commit = Arc::new(Notify::new());
        let writing_back = Arc::new(Notify::new());
        let resume_write_back = Arc::new(Notify::new());
        let committed = Arc::new(AtomicBool::new(false));
        let held_write_back = Arc::new(AtomicBool::new(false));
        let record_path = Arc::new(Mutex::new(String::new()));
        owner_backend.set_before({
            let (committing, resume_commit, writing_back, resume_write_back) = (
                committing.clone(),
                resume_commit.clone(),
                writing_back.clone(),
                resume_write_back.clone(),
            );
            let (committed, held_write_back, record_path) = (
                committed.clone(),
                held_write_back.clone(),
                record_path.clone(),
            );
            move |operation| {
                let fail = is_record_delete(operation);
                let hold_commit = match operation {
                    BackendOp::WriteIfNotExists { path, value }
                        if path.contains("/_t/")
                            && glassdb_storage::txrecord::status(value)
                                .is_ok_and(|status| status == TxCommitStatus::Committed)
                            && !committed.swap(true, Ordering::SeqCst) =>
                    {
                        *record_path.lock().unwrap() = path.to_string();
                        true
                    }
                    _ => false,
                };
                let hold_write_back = matches!(
                    operation,
                    BackendOp::WriteIf { path, .. } if path.ends_with("/_r")
                ) && committed.load(Ordering::SeqCst)
                    && !held_write_back.swap(true, Ordering::SeqCst);
                let (committing, resume_commit, writing_back, resume_write_back) = (
                    committing.clone(),
                    resume_commit.clone(),
                    writing_back.clone(),
                    resume_write_back.clone(),
                );
                Box::pin(async move {
                    if hold_commit {
                        committing.notify_one();
                        resume_commit.notified().await;
                    }
                    if hold_write_back {
                        writing_back.notify_one();
                        resume_write_back.notified().await;
                    }
                    unavailable(fail)
                })
            }
        });
        let commit = rt::spawn({
            let owner = owner.clone();
            async move {
                // The scan keeps the create from a direct commit, so T takes
                // the create lock and the membership lock.
                owner
                    .tx(|tx| async move {
                        let root = tx.root_collection();
                        tx.scan_keys(&root, glassdb::KeyScan::all()).await?;
                        tx.write(&root, b"created", b"v")
                    })
                    .await
                    .unwrap()
            }
        });

        // T is pending. The writer caches the leaf with T as a holder.
        committing.notified().await;
        let path = record_path.lock().unwrap().clone();
        assert_eq!(
            writer.root_collection().read(b"created").await.unwrap(),
            None
        );
        resume_commit.notify_one();

        // T committed, and its write-back is held. The reader caches the leaf
        // with T as a holder, twice, so that the state has an interval, and
        // two views scan it.
        writing_back.notified().await;
        let all = || glassdb::KeyScan::all();
        let expected = vec![b"base".to_vec(), b"created".to_vec()];
        for _ in 0..2 {
            let page = reader.root_collection().scan_keys(all()).await.unwrap();
            assert_eq!(keys(page), expected);
            rt::sleep(Duration::from_millis(10)).await;
        }
        let first_view = reader.root_collection().stale(Duration::from_secs(3600));
        let second_view = reader.root_collection().stale(Duration::from_secs(3600));
        for view in [&first_view, &second_view] {
            assert_eq!(keys(view.scan_keys(all()).await.unwrap()), expected);
        }
        resume_write_back.notify_one();
        commit.await.unwrap();

        // A later writer replaces T, so GC on the reader deletes its record.
        owner
            .root_collection()
            .write(b"created", b"v2")
            .await
            .unwrap();
        loop {
            if matches!(memory.read(&path).await, Err(BackendError::NotFound)) {
                break;
            }
            let report = reader.collect_garbage(NonZeroUsize::MIN).await.unwrap();
            let retry_after = report
                .retry_after
                .expect("GC did not delete the record of T");
            rt::sleep(retry_after.max(Duration::from_millis(1))).await;
        }

        // The writer resolves T from its cached leaf and wounds it at the empty path.
        resume.notify_one();
        fence.await.unwrap();
        let stored = memory.read(&path).await.unwrap();
        assert_eq!(
            glassdb_storage::txrecord::status(&stored.contents).unwrap(),
            TxCommitStatus::BlindWounded
        );

        // The first scan finds the record of T reclaimed. The second one finds
        // the blind wound of T, and must not drop T at the cut point.
        for view in [&second_view, &first_view] {
            assert_eq!(keys(view.scan_keys(all()).await.unwrap()), expected);
        }
        owner.shutdown().await;
        writer.shutdown().await;
        reader.shutdown().await;
    });
}
