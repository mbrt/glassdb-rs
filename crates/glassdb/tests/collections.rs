use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use glassdb::backend::memory::MemoryBackend;
use glassdb::backend::middleware::RecordingBackend;
use glassdb::backend::{Backend, ListLimit};
use glassdb::{
    Collection, CollectionPath, Database, Error, MAX_COLLECTION_NAME_BYTES, SplitPolicy,
};

#[tokio::test]
async fn root_collection_is_permanent_and_key_bearing() {
    let backend = Arc::new(MemoryBackend::new());
    let db = Database::open("example", backend.clone()).await.unwrap();
    let root = db.root_collection();

    assert_eq!(root.name(), None);
    root.write(b"root-key", b"value").await.unwrap();

    let other = Database::open("example", backend).await.unwrap();
    assert_eq!(
        other
            .root_collection()
            .read(b"root-key")
            .await
            .unwrap()
            .unwrap(),
        b"value"
    );
}

#[tokio::test]
async fn paths_resolve_to_bound_handles_and_require_existing_ancestors() {
    let db = Database::open("example", MemoryBackend::new())
        .await
        .unwrap();
    let parent = db
        .root_collection()
        .create_collection(b"parent")
        .await
        .unwrap();
    let child = parent.create_collection(b"child").await.unwrap();
    child.write(b"k", b"v").await.unwrap();

    let path = CollectionPath::new(b"parent")
        .unwrap()
        .child(b"child")
        .unwrap();
    assert!(db.collection_exists(&path).await.unwrap());
    let opened = db.open_collection(&path).await.unwrap();
    assert_eq!(opened.name(), Some(b"child".as_slice()));
    assert_eq!(opened.read(b"k").await.unwrap().unwrap(), b"v");

    let missing_ancestor = CollectionPath::new(b"missing")
        .unwrap()
        .child(b"child")
        .unwrap();
    assert!(matches!(
        db.create_collection(&missing_ancestor).await,
        Err(Error::NotFound)
    ));
    assert!(!db.collection_exists(&missing_ancestor).await.unwrap());
}

#[tokio::test]
async fn path_existence_matches_opening_backend_operation_count() {
    let backend = Arc::new(MemoryBackend::new());
    let setup = Database::open("example", backend.clone()).await.unwrap();
    let parent = setup.create_collection("parent").await.unwrap();
    parent.create_collection("child").await.unwrap();
    setup.shutdown().await;
    let path = CollectionPath::new(b"parent")
        .unwrap()
        .child(b"child")
        .unwrap();

    let exists_recorder = Arc::new(RecordingBackend::new(backend.clone()));
    let exists_log = exists_recorder.log();
    let exists_db = Database::open("example", exists_recorder).await.unwrap();
    exists_log.lock().unwrap().clear();
    assert!(exists_db.collection_exists(&path).await.unwrap());
    exists_db.shutdown().await;
    let exists_operations = exists_log.lock().unwrap().len();

    let open_recorder = Arc::new(RecordingBackend::new(backend));
    let open_log = open_recorder.log();
    let open_db = Database::open("example", open_recorder).await.unwrap();
    open_log.lock().unwrap().clear();
    open_db.open_collection(&path).await.unwrap();
    open_db.shutdown().await;
    let open_operations = open_log.lock().unwrap().len();

    assert!(
        exists_operations > 0,
        "the cold path lookup must reach storage"
    );
    assert_eq!(exists_operations, open_operations);
}

#[tokio::test]
async fn strict_and_idempotent_create_have_distinct_race_contracts() {
    let backend = Arc::new(MemoryBackend::new());
    let db1 = Database::open("example", backend.clone()).await.unwrap();
    let db2 = Database::open("example", backend.clone()).await.unwrap();
    let path = CollectionPath::new(b"contended").unwrap();

    let (left, right) = tokio::join!(db1.create_collection(&path), db2.create_collection(&path));
    assert!(
        matches!(
            (&left, &right),
            (Ok(_), Err(Error::AlreadyExists)) | (Err(Error::AlreadyExists), Ok(_))
        ),
        "exactly one strict creator must win"
    );

    let left = db1.create_collection_if_absent(&path).await.unwrap();
    let right = db2.create_collection_if_absent(&path).await.unwrap();
    left.write(b"k", b"v").await.unwrap();
    assert_eq!(right.read(b"k").await.unwrap().unwrap(), b"v");

    db1.shutdown().await;
    db2.shutdown().await;

    let objects = backend
        .list("example/_c/", None, ListLimit::new(100).unwrap())
        .await
        .unwrap()
        .objects;
    assert_eq!(
        objects.iter().filter(|path| path.ends_with("/_i")).count(),
        2,
        "the clean race loser must reclaim its unpublished record"
    );
    assert_eq!(
        objects.iter().filter(|path| path.ends_with("/_r")).count(),
        2,
        "the clean race loser must reclaim its unpublished tree root"
    );
}

#[tokio::test]
#[allow(deprecated)]
async fn child_listing_returns_sorted_incarnation_bound_handles() {
    let db = Database::open("example", MemoryBackend::new())
        .await
        .unwrap();
    let parent = db
        .root_collection()
        .create_collection(b"parent")
        .await
        .unwrap();
    parent
        .create_collection(b"\xff")
        .await
        .unwrap()
        .write(b"k", b"last")
        .await
        .unwrap();
    parent
        .create_collection(b"a")
        .await
        .unwrap()
        .write(b"k", b"first")
        .await
        .unwrap();

    let legacy_names = parent
        .collections()
        .await
        .unwrap()
        .map(|entry| entry.map(|entry| entry.name))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut plain = parent.iter_collections().await.unwrap();
    assert_eq!(plain.len(), 2);
    let first = plain.next().unwrap();
    assert_eq!(plain.len(), 1);
    drop(parent);
    let entries = std::iter::once(first).chain(plain).collect::<Vec<_>>();
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.name.clone())
            .collect::<Vec<_>>(),
        legacy_names
    );
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.name.as_slice())
            .collect::<Vec<_>>(),
        vec![b"a".as_slice(), b"\xff".as_slice()]
    );
    assert_eq!(
        entries[0].collection.read(b"k").await.unwrap().unwrap(),
        b"first"
    );
    assert_eq!(
        entries[1].collection.read(b"k").await.unwrap().unwrap(),
        b"last"
    );
}

#[tokio::test]
#[allow(deprecated)]
async fn child_listing_retries_after_the_directory_changes() {
    let backend = Arc::new(MemoryBackend::new());
    let db = Database::open("example", backend.clone()).await.unwrap();
    let peer = Database::open("example", backend).await.unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));

    let names = db
        .tx({
            let attempts = attempts.clone();
            move |tx| {
                let peer = peer.clone();
                let attempts = attempts.clone();
                async move {
                    let root = tx.root_collection();
                    let legacy = tx
                        .collections(&root)
                        .await?
                        .map(|entry| entry.map(|entry| entry.name))
                        .collect::<Result<Vec<_>, _>>()?;
                    let names = tx
                        .iter_collections(&root)
                        .await?
                        .map(|entry| entry.name)
                        .collect::<Vec<_>>();
                    glassdb::ensure_tx!(
                        names == legacy,
                        Error::internal(format!(
                            "plain collection iterator differed from legacy: {names:?} vs {legacy:?}"
                        ))
                    );
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        peer.create_collection("appeared").await?;
                    }
                    tx.write(&root, b"marker", b"committed")?;
                    Ok(names)
                }
            }
        })
        .await
        .unwrap();

    assert_eq!(names, vec![b"appeared".to_vec()]);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn bound_handle_data_access_does_not_revalidate_its_logical_path() {
    let recorder = Arc::new(RecordingBackend::new(Arc::new(MemoryBackend::new())));
    let log = recorder.log();

    let creator = Database::open("example", recorder.clone()).await.unwrap();
    let child = creator
        .root_collection()
        .create_collection(b"child")
        .await
        .unwrap();
    child.write(b"k", b"v").await.unwrap();
    creator.shutdown().await;

    let reader = Database::open("example", recorder).await.unwrap();
    let child = reader
        .open_collection(&CollectionPath::new(b"child").unwrap())
        .await
        .unwrap();
    log.lock().unwrap().clear();

    assert_eq!(child.read(b"k").await.unwrap().unwrap(), b"v");
    let operations = log.lock().unwrap();
    assert!(
        !operations.is_empty(),
        "the cold bound read must reach storage"
    );
    assert!(
        operations
            .iter()
            .all(|operation| !operation.path.ends_with("/_i")),
        "bound data access must not read collection records"
    );
}

#[tokio::test]
async fn collection_names_are_validated_before_io() {
    assert!(matches!(
        CollectionPath::new([] as [u8; 0]),
        Err(Error::InvalidInput(_))
    ));
    assert!(CollectionPath::new([0u8; MAX_COLLECTION_NAME_BYTES]).is_ok());
    assert!(matches!(
        CollectionPath::new([0u8; MAX_COLLECTION_NAME_BYTES + 1]),
        Err(Error::InvalidInput(_))
    ));

    let db = Database::open("example", MemoryBackend::new())
        .await
        .unwrap();
    assert!(matches!(
        db.root_collection().create_collection(b"").await,
        Err(Error::InvalidInput(_))
    ));
}

#[tokio::test]
async fn collection_directories_respect_the_record_size_limit() {
    let db = Database::builder("example", MemoryBackend::new())
        .split_policy(SplitPolicy {
            node_max_bytes: 256,
            split_headroom_bytes: 64,
            ..SplitPolicy::default()
        })
        .open()
        .await
        .unwrap();
    let name = [b'x'; MAX_COLLECTION_NAME_BYTES];

    assert!(matches!(
        db.root_collection().create_collection(name).await,
        Err(Error::InvalidInput(_))
    ));
    assert!(!db.root_collection().collection_exists(name).await.unwrap());
}

#[tokio::test]
async fn string_names_are_converted_to_collection_paths() {
    let db = Database::open("example", MemoryBackend::new())
        .await
        .unwrap();

    let parent = db.create_collection("parent").await.unwrap();
    assert!(db.collection_exists("parent").await.unwrap());
    assert_eq!(
        db.open_collection(String::from("parent"))
            .await
            .unwrap()
            .name(),
        Some(b"parent".as_slice())
    );
    db.create_collection_if_absent("parent").await.unwrap();

    let child = parent.create_collection("child").await.unwrap();
    assert!(parent.collection_exists("child").await.unwrap());
    assert_eq!(
        parent
            .open_collection(String::from("child"))
            .await
            .unwrap()
            .name(),
        child.name()
    );
    parent.create_collection_if_absent("child").await.unwrap();

    assert!(matches!(
        db.open_collection("").await,
        Err(Error::InvalidInput(_))
    ));
}

#[tokio::test]
async fn initialized_database_never_recreates_a_missing_permanent_record() {
    let backend = Arc::new(MemoryBackend::new());
    let db = Database::open("example", backend.clone()).await.unwrap();
    db.shutdown().await;

    let record_path = "example/_c/0000000000000000000000/_i";
    let record = backend.read(record_path).await.unwrap();
    backend
        .delete_if(record_path, &record.version)
        .await
        .unwrap();

    let reopened = Database::open("example", backend.clone()).await;
    assert!(
        matches!(reopened, Err(Error::Internal { .. })),
        "a missing permanent record must be reported as corruption"
    );
    assert!(matches!(
        backend.read(record_path).await,
        Err(glassdb::backend::BackendError::NotFound)
    ));
}

#[tokio::test]
async fn initialized_database_never_recreates_a_missing_permanent_tree_root() {
    let backend = Arc::new(MemoryBackend::new());
    let db = Database::open("example", backend.clone()).await.unwrap();
    db.shutdown().await;

    let root_path = "example/_c/0000000000000000000000/_r";
    let root = backend.read(root_path).await.unwrap();
    backend.delete_if(root_path, &root.version).await.unwrap();

    let reopened = Database::open("example", backend.clone()).await;
    assert!(
        matches!(reopened, Err(Error::Internal { .. })),
        "a missing permanent tree root must be reported as corruption"
    );
    assert!(matches!(
        backend.read(root_path).await,
        Err(glassdb::backend::BackendError::NotFound)
    ));
}

#[tokio::test]
async fn missing_bound_tree_root_is_not_empty_or_recreated_by_data_operations() {
    let backend = Arc::new(MemoryBackend::new());
    let creator = Database::open("example", backend.clone()).await.unwrap();
    creator
        .root_collection()
        .create_collection(b"child")
        .await
        .unwrap();
    creator.shutdown().await;

    let permanent_root = "example/_c/0000000000000000000000/_r";
    let child_root = backend
        .list("example/_c/", None, ListLimit::new(100).unwrap())
        .await
        .unwrap()
        .objects
        .into_iter()
        .find(|path| path.ends_with("/_r") && path != permanent_root)
        .unwrap();
    let observed = backend.read(&child_root).await.unwrap();
    backend
        .delete_if(&child_root, &observed.version)
        .await
        .unwrap();

    let reader = Database::open("example", backend.clone()).await.unwrap();
    let child = reader
        .open_collection(&CollectionPath::new(b"child").unwrap())
        .await
        .unwrap();

    assert!(matches!(
        child.read(b"k").await,
        Err(Error::StaleCollection)
    ));
    let write = child.write(b"k", b"v").await;
    assert!(
        matches!(write, Err(Error::StaleCollection)),
        "unexpected write result: {write:?}"
    );
    assert!(matches!(
        child.iter_keys().await,
        Err(Error::StaleCollection)
    ));
    assert!(matches!(
        backend.read(&child_root).await,
        Err(glassdb::backend::BackendError::NotFound)
    ));
}

#[tokio::test]
async fn collection_changes_compose_with_data_and_nested_changes() {
    let db = Database::open("example", MemoryBackend::new())
        .await
        .unwrap();

    let (users, active) = db
        .tx(|tx| async move {
            let root = tx.root_collection();
            let (users, created) = tx.create_collection_if_absent(&root, b"users").await?;
            glassdb::ensure_tx!(
                created,
                Error::internal("new users collection was reported as existing")
            );
            let (same_users, created) = tx.create_collection_if_absent(&root, b"users").await?;
            glassdb::ensure_tx!(
                created,
                Error::internal("transaction did not retain its staged collection incarnation")
            );
            tx.write(&same_users, b"second-handle", b"ready")?;
            let active = tx.create_collection(&users, b"active").await?;
            tx.write(&users, b"seed", b"ready")?;
            tx.write(&active, b"alice", b"1")?;

            let listed = tx.iter_collections(&users).await?.collect::<Vec<_>>();
            glassdb::ensure_tx!(
                listed.len() == 1,
                Error::internal(format!(
                    "staged users collection listed {} children instead of one",
                    listed.len()
                ))
            );
            let listed_name = listed
                .first()
                .map(|entry| entry.name.as_slice())
                .ok_or_else(|| Error::internal("staged active collection was not listed"))?;
            glassdb::ensure_tx!(
                listed_name == b"active",
                Error::internal(format!(
                    "staged users collection listed unexpected child {listed_name:?}"
                ))
            );
            Ok((users, active))
        })
        .await
        .unwrap();

    assert_eq!(users.read(b"seed").await.unwrap().unwrap(), b"ready");
    assert_eq!(
        users.read(b"second-handle").await.unwrap().unwrap(),
        b"ready"
    );
    assert_eq!(active.read(b"alice").await.unwrap().unwrap(), b"1");
    let (_, created) = db
        .tx(|tx| async move {
            let root = tx.root_collection();
            tx.create_collection_if_absent(&root, b"users").await
        })
        .await
        .unwrap();
    assert!(!created);
}

#[tokio::test]
async fn failed_transaction_retries_invalidated_reads_without_publishing_changes() {
    let backend = Arc::new(MemoryBackend::new());
    let db = Database::open("example", backend.clone()).await.unwrap();
    let peer = Database::open("example", backend).await.unwrap();
    let root = db.root_collection();
    let peer_root = peer.root_collection();
    root.write(b"guard", b"old").await.unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));

    let result = db
        .tx({
            let root = root.clone();
            let peer_root = peer_root.clone();
            let attempts = attempts.clone();
            move |tx| {
                let root = root.clone();
                let peer_root = peer_root.clone();
                let attempts = attempts.clone();
                async move {
                    tx.read(&root, b"guard").await?.ok_or(Error::NotFound)?;
                    let collection = tx.create_collection(&root, b"temporary").await?;
                    tx.write(&collection, b"k", b"v")?;
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        peer_root.write(b"guard", b"new").await?;
                    }
                    Err::<(), _>(Error::InvalidInput("stop".into()))
                }
            }
        })
        .await;

    assert!(matches!(result, Err(Error::InvalidInput(_))));
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert!(!db.collection_exists("temporary").await.unwrap());
}

#[tokio::test]
async fn collection_creation_reuses_its_reserved_incarnation_across_retry() {
    let backend = Arc::new(MemoryBackend::new());
    let db = Database::open("example", backend.clone()).await.unwrap();
    let peer = Database::open("example", backend).await.unwrap();
    let root = db.root_collection();
    let peer_root = peer.root_collection();
    root.write(b"guard", b"old").await.unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempted_handles = Arc::new(Mutex::new(Vec::<Collection>::new()));

    db.tx({
        let root = root.clone();
        let peer_root = peer_root.clone();
        let attempts = attempts.clone();
        let attempted_handles = attempted_handles.clone();
        move |tx| {
            let root = root.clone();
            let peer_root = peer_root.clone();
            let attempts = attempts.clone();
            let attempted_handles = attempted_handles.clone();
            async move {
                tx.read(&root, b"guard").await?.ok_or(Error::NotFound)?;
                let collection = tx.create_collection(&root, b"created-on-retry").await?;
                tx.write(&collection, b"k", b"v")?;
                {
                    let mut handles = attempted_handles
                        .lock()
                        .map_err(|_| Error::internal("attempted handle capture was poisoned"))?;
                    handles.push(collection);
                }
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    peer_root.write(b"guard", b"new").await?;
                }
                Ok(())
            }
        }
    })
    .await
    .unwrap();

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    let attempted_handles = attempted_handles.lock().unwrap().clone();
    assert_eq!(attempted_handles.len(), 2);
    for collection in attempted_handles {
        assert_eq!(collection.read(b"k").await.unwrap().unwrap(), b"v");
    }
}

#[tokio::test]
async fn create_then_drop_without_data_collapses_to_noop() {
    let db = Database::open("example", MemoryBackend::new())
        .await
        .unwrap();

    db.tx(|tx| async move {
        let root = tx.root_collection();
        let temporary = tx.create_collection(&root, b"temporary").await?;
        tx.drop_collection(&temporary).await?;
        let still_exists = tx.collection_exists(&root, b"temporary").await?;
        glassdb::ensure_tx!(
            !still_exists,
            Error::internal("dropped staged collection remained visible")
        );
        let write = tx.write(&temporary, b"k", b"v");
        glassdb::ensure_tx!(
            matches!(write, Err(Error::InvalidInput(_))),
            Error::internal(format!(
                "write through a dropped staged collection returned {write:?}"
            ))
        );
        Ok(())
    })
    .await
    .unwrap();

    assert!(!db.collection_exists("temporary").await.unwrap());
}

#[tokio::test]
async fn not_empty_is_revalidated_before_it_is_returned() {
    let db = Database::open("example", MemoryBackend::new())
        .await
        .unwrap();
    let parent = db.create_collection("parent").await.unwrap();
    let child = parent.create_collection("child").await.unwrap();
    let first_attempt = Arc::new(AtomicBool::new(true));

    db.tx({
        let parent = parent.clone();
        let child = child.clone();
        let first_attempt = first_attempt.clone();
        move |tx| {
            let parent = parent.clone();
            let child = child.clone();
            let first_attempt = first_attempt.clone();
            async move {
                match tx.drop_collection(&parent).await {
                    Ok(()) => Ok(()),
                    Err(Error::NotEmpty) => {
                        if first_attempt.swap(false, Ordering::SeqCst) {
                            child.drop_collection().await?;
                        }
                        Err(Error::NotEmpty)
                    }
                    Err(error) => Err(error),
                }
            }
        }
    })
    .await
    .unwrap();

    assert!(
        !first_attempt.load(Ordering::SeqCst),
        "the first attempt must observe the child"
    );
    assert!(matches!(
        parent.read(b"k").await,
        Err(Error::StaleCollection)
    ));
}

#[tokio::test]
async fn drop_is_non_recursive_and_fences_obsolete_handles() {
    let db = Database::open("example", MemoryBackend::new())
        .await
        .unwrap();
    let parent = db.create_collection("parent").await.unwrap();
    let child = parent.create_collection("child").await.unwrap();
    child.write(b"k", b"old").await.unwrap();

    assert!(matches!(
        parent.drop_collection().await,
        Err(Error::NotEmpty)
    ));
    child.drop_collection().await.unwrap();
    let replacement = parent.create_collection("child").await.unwrap();
    replacement.write(b"k", b"new").await.unwrap();

    assert!(matches!(
        child.read(b"k").await,
        Err(Error::StaleCollection)
    ));
    assert_eq!(replacement.read(b"k").await.unwrap().unwrap(), b"new");
}

#[tokio::test]
async fn children_can_be_dropped_before_their_parent_in_one_transaction() {
    let db = Database::open("example", MemoryBackend::new())
        .await
        .unwrap();
    let parent = db.create_collection("parent").await.unwrap();
    let child = parent.create_collection("child").await.unwrap();
    child.write(b"k", b"old").await.unwrap();

    db.tx({
        let parent = parent.clone();
        let child = child.clone();
        move |tx| {
            let parent = parent.clone();
            let child = child.clone();
            async move {
                tx.drop_collection(&child).await?;
                tx.drop_collection(&parent).await?;
                tx.write(&tx.root_collection(), b"drop-marker", b"committed")?;
                Ok(())
            }
        }
    })
    .await
    .unwrap();

    assert!(!db.collection_exists("parent").await.unwrap());
    assert_eq!(
        db.root_collection()
            .read(b"drop-marker")
            .await
            .unwrap()
            .unwrap(),
        b"committed"
    );
    assert!(matches!(
        child.read(b"k").await,
        Err(Error::StaleCollection)
    ));
}

#[tokio::test]
async fn a_cached_handle_in_another_client_observes_the_drop_fence() {
    let backend = Arc::new(MemoryBackend::new());
    let first = Database::open("example", backend.clone()).await.unwrap();
    let second = Database::open("example", backend).await.unwrap();
    let old = first.create_collection("child").await.unwrap();
    old.write(b"k", b"old").await.unwrap();
    assert_eq!(old.read(b"k").await.unwrap().unwrap(), b"old");

    second
        .open_collection("child")
        .await
        .unwrap()
        .drop_collection()
        .await
        .unwrap();

    let stale = old.read(b"k").await;
    assert!(
        matches!(stale, Err(Error::StaleCollection)),
        "unexpected stale-handle result: {stale:?}"
    );
    assert!(matches!(
        old.collection_exists(b"nested").await,
        Err(Error::StaleCollection)
    ));
    assert!(matches!(old.iter_keys().await, Err(Error::StaleCollection)));
}

#[tokio::test]
async fn drop_rejects_invalid_targets_and_staged_data_writes() {
    let backend = Arc::new(MemoryBackend::new());
    let db = Database::open("example", backend.clone()).await.unwrap();
    let other = Database::open("other", backend).await.unwrap();
    let collection = db.create_collection("collection").await.unwrap();

    assert!(matches!(
        db.root_collection().drop_collection().await,
        Err(Error::InvalidInput(_))
    ));
    assert!(matches!(
        other
            .tx(|tx| {
                let collection = collection.clone();
                async move { tx.drop_collection(&collection).await }
            })
            .await,
        Err(Error::InvalidInput(_))
    ));
    assert!(matches!(
        db.tx(|tx| {
            let collection = collection.clone();
            async move {
                tx.write(&collection, b"k", b"v")?;
                tx.drop_collection(&collection).await
            }
        })
        .await,
        Err(Error::InvalidInput(_))
    ));
}
