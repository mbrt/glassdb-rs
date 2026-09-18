use std::sync::Arc;
use std::time::Duration;

use glassdb::backend::{BackendError, ListLimit, memory::MemoryBackend};
use glassdb::middleware::{BackendOp, HookBackend};
use glassdb::{Backend, Database, GcLimits, InlinePolicy};
use glassdb_concurr::{exec, rt};
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
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let db = Database::builder("gc", backend.clone())
            .protocol_timing(ProtocolTiming::simulation())
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
        rt::sleep(Duration::from_millis(1)).await;
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
        rt::sleep(Duration::from_millis(1)).await;
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
