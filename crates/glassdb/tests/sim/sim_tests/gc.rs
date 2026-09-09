use std::sync::Arc;
use std::time::Duration;

use glassdb::backend::{BackendError, ListLimit, memory::MemoryBackend};
use glassdb::middleware::{BackendOp, HookBackend};
use glassdb::{Backend, Database};
use glassdb_concurr::{exec, rt};
use glassdb_trans::ProtocolTiming;

#[test]
fn old_identity_gc_preserves_the_replayed_collection() {
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
        // Timed-out acquisition pins its marker even after GC removes its roots.
        let gc = db.stats().gc;
        assert!(gc.progress > 0, "{gc:?}");
        assert!(matches!(
            backend.read(&roots[0]).await,
            Err(BackendError::NotFound)
        ));
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
