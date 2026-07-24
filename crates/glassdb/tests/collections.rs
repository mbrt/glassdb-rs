use std::sync::Arc;

use glassdb::backend::memory::MemoryBackend;
use glassdb::backend::middleware::RecordingBackend;
use glassdb::backend::{Backend, ListLimit};
use glassdb::{CollectionPath, Database, Error, MAX_COLLECTION_NAME_BYTES};

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
        "the clean race loser must reclaim its unpublished root"
    );
}

#[tokio::test]
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

    let entries = parent
        .collections()
        .await
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
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
            .all(|operation| operation.path != "example/_c/0000000000000000000000/_i"),
        "bound data access must route by collection ID without rereading its parent directory"
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
async fn initialized_database_never_recreates_a_missing_permanent_root() {
    let backend = Arc::new(MemoryBackend::new());
    let db = Database::open("example", backend.clone()).await.unwrap();
    db.shutdown().await;

    let root_path = "example/_c/0000000000000000000000/_i";
    let root = backend.read(root_path).await.unwrap();
    backend.delete_if(root_path, &root.version).await.unwrap();

    let reopened = Database::open("example", backend.clone()).await;
    assert!(
        matches!(reopened, Err(Error::Internal { .. })),
        "a missing permanent root must be reported as corruption"
    );
    assert!(matches!(
        backend.read(root_path).await,
        Err(glassdb::backend::BackendError::NotFound)
    ));
}

#[tokio::test]
async fn missing_bound_root_is_not_empty_or_recreated_by_data_operations() {
    let backend = Arc::new(MemoryBackend::new());
    let creator = Database::open("example", backend.clone()).await.unwrap();
    creator
        .root_collection()
        .create_collection(b"child")
        .await
        .unwrap();
    creator.shutdown().await;

    let permanent_root = "example/_c/0000000000000000000000/_i";
    let child_root = backend
        .list("example/_c/", None, ListLimit::new(100).unwrap())
        .await
        .unwrap()
        .objects
        .into_iter()
        .find(|path| path.ends_with("/_i") && path != permanent_root)
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

    assert!(matches!(child.read(b"k").await, Err(Error::NotFound)));
    assert!(matches!(
        child.write(b"k", b"v").await,
        Err(Error::NotFound)
    ));
    assert!(matches!(child.keys().await, Err(Error::NotFound)));
    assert!(matches!(
        backend.read(&child_root).await,
        Err(glassdb::backend::BackendError::NotFound)
    ));
}
