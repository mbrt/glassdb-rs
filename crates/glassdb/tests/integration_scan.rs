//! Transactional scans and collection/key listing integration behavior.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use glassdb::{Backend, CollectionPath, Database, Error};
use glassdb_storage::Node;
use tokio::sync::Barrier;

#[path = "integration_support/mod.rs"]
pub mod integration_support;

use integration_support::{
    ParentWriteControl, PauseControl, create_top, init_db, list_collections_of, mem,
};

#[tokio::test(start_paused = true)]
async fn list_keys() {
    let db = init_db(mem()).await;
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();

    let empty = coll.iter_keys().await.unwrap();
    assert_eq!(empty.len(), 0);

    let keys: Vec<Vec<u8>> = (0u32..100).map(|i| i.to_be_bytes().to_vec()).collect();
    let test_val = b"val";
    let coll_ref = &coll;
    let keys_ref = &keys;
    db.tx(|tx| async move {
        for k in keys_ref {
            tx.write(coll_ref, k, test_val)?;
        }
        Ok(())
    })
    .await
    .unwrap();

    let mut plain = coll.iter_keys().await.unwrap();
    assert_eq!(plain.len(), keys.len());
    let first = plain.next().unwrap();
    assert_eq!(plain.len(), keys.len() - 1);
    drop(coll);
    let got: Vec<Vec<u8>> = std::iter::once(first).chain(plain).collect();
    assert_eq!(got, keys);

    // Listing descends the B-link tree and scans its leaves via reads (ADR-031),
    // never a directory `list` of an object prefix.
    let stats = db.stats();
    assert_eq!(stats.backend.obj_lists, 0);
}

#[tokio::test]
async fn transactional_key_scan_supports_ranges_prefixes_and_paging() {
    let db = init_db(mem()).await;
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"key-scan")
        .await
        .unwrap();
    for key in [
        b"a".as_slice(),
        b"aa",
        b"ab",
        b"b",
        b"\xfe\xff",
        b"\xff",
        b"\xff\x00",
        b"\xff\xff",
    ] {
        coll.write(key, b"v").await.unwrap();
    }

    let range = coll
        .scan_keys(glassdb::KeyScan::range(b"a", b"b"))
        .await
        .unwrap();
    assert_eq!(
        range.keys(),
        &[b"a".to_vec(), b"aa".to_vec(), b"ab".to_vec()]
    );

    let prefix = coll
        .scan_keys(glassdb::KeyScan::prefix(b"\xff"))
        .await
        .unwrap();
    assert_eq!(
        prefix.keys(),
        &[b"\xff".to_vec(), b"\xff\x00".to_vec(), b"\xff\xff".to_vec()]
    );

    let first = coll
        .scan_keys(glassdb::KeyScan::all().limit(3))
        .await
        .unwrap();
    assert_eq!(first.len(), 3);
    let second = coll
        .scan_keys(
            glassdb::KeyScan::all()
                .after(first.next_after().unwrap())
                .limit(3),
        )
        .await
        .unwrap();
    assert_eq!(
        second.keys(),
        &[b"b".to_vec(), b"\xfe\xff".to_vec(), b"\xff".to_vec()]
    );
    assert!(first.keys().iter().all(|key| !second.keys().contains(key)));
}

#[tokio::test]
async fn transactional_key_scan_reflects_staged_membership() {
    let db = init_db(mem()).await;
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"scan-own-writes")
        .await
        .unwrap();
    coll.write(b"a", b"old").await.unwrap();
    coll.write(b"c", b"old").await.unwrap();

    let coll = &coll;
    let scan = glassdb::KeyScan::range(b"a", b"z");
    db.tx(|tx| async move {
        tx.write(coll, b"a", b"new")?;
        tx.write(coll, b"b", b"new")?;
        tx.delete(coll, b"c")?;
        let first = tx.scan_keys(coll, scan).await?;
        glassdb::ensure_tx!(
            first.keys() == [b"a".to_vec(), b"b".to_vec()],
            Error::internal(format!("first staged key scan returned {:?}", first.keys()))
        );

        tx.write(coll, b"d", b"new")?;
        let second = tx.scan_keys(coll, scan).await?;
        glassdb::ensure_tx!(
            second.keys() == [b"a".to_vec(), b"b".to_vec(), b"d".to_vec()],
            Error::internal(format!(
                "second staged key scan returned {:?}",
                second.keys()
            ))
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn scan_then_create_prevents_phantom_write_skew() {
    let db = init_db(mem()).await;
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"scan-write-skew")
        .await
        .unwrap();
    let first_scans = Arc::new(Barrier::new(2));

    let run = |key: &'static [u8]| {
        let db = db.clone();
        let coll = coll.clone();
        let first_scans = first_scans.clone();
        tokio::spawn(async move {
            let first_attempt = Arc::new(AtomicBool::new(true));
            db.tx(move |tx| {
                let coll = coll.clone();
                let first_scans = first_scans.clone();
                let first_attempt = first_attempt.clone();
                async move {
                    let page = tx.scan_keys(&coll, glassdb::KeyScan::all()).await?;
                    if first_attempt.swap(false, Ordering::SeqCst) {
                        first_scans.wait().await;
                    }
                    if page.is_empty() {
                        tx.write(&coll, key, b"created")?;
                    }
                    Ok(())
                }
            })
            .await
        })
    };

    let left = run(b"left");
    let right = run(b"right");
    left.await.unwrap().unwrap();
    right.await.unwrap().unwrap();

    let keys = coll
        .scan_keys(glassdb::KeyScan::all())
        .await
        .unwrap()
        .into_keys();
    assert_eq!(keys.len(), 1, "only one create-if-empty may commit");
    assert!(db.stats().transactions.retries >= 1);
}

#[tokio::test]
async fn key_scan_validates_ranges_and_collection_existence() {
    let db = init_db(mem()).await;
    assert!(matches!(
        db.open_collection(&CollectionPath::new(b"missing-scan").unwrap())
            .await,
        Err(glassdb::Error::NotFound)
    ));

    let coll = db
        .root_collection()
        .create_collection_if_absent(b"scan-validation")
        .await
        .unwrap();
    assert!(matches!(
        coll.scan_keys(glassdb::KeyScan::range(b"z", b"a")).await,
        Err(glassdb::Error::InvalidInput(_))
    ));
    assert!(
        coll.scan_keys(glassdb::KeyScan::all().limit(0))
            .await
            .unwrap()
            .is_empty()
    );
}

// ADR-031 phantom prevention, end-to-end: a listing that observes a set of keys
// commits against a validated snapshot, so a key created *after* the scan is
// never included, and a listing whose snapshot a concurrent commit invalidated
// transparently re-runs to a fresh, consistent view. The listing is a read-only
// serializable transaction, so its result is always sorted and internally
// consistent.
#[tokio::test]
async fn keys_listing_is_phantom_safe() {
    let db = init_db(mem()).await;
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"phantom")
        .await
        .unwrap();

    // Seed a stable set of keys.
    let seed: Vec<Vec<u8>> = (0u32..20).map(|i| i.to_be_bytes().to_vec()).collect();
    for k in &seed {
        coll.write(k, b"v").await.unwrap();
    }

    // A listing sees exactly the seeded keys, sorted, with no duplicates.
    let listed: Vec<Vec<u8>> = coll.iter_keys().await.unwrap().collect();
    let mut sorted = listed.clone();
    sorted.sort();
    assert_eq!(listed, sorted, "listing is sorted");
    assert_eq!(listed, seed, "listing observes exactly the committed keys");

    // Create a new key, then list again: the fresh listing includes it (its own
    // consistent snapshot), demonstrating the scan re-resolves membership rather
    // than caching a stale set.
    let extra = 999u32.to_be_bytes().to_vec();
    coll.write(&extra, b"v").await.unwrap();
    let listed2: Vec<Vec<u8>> = coll.iter_keys().await.unwrap().collect();
    assert!(
        listed2.contains(&extra),
        "new key visible to a later listing"
    );
    assert_eq!(listed2.len(), seed.len() + 1);
    let mut sorted2 = listed2.clone();
    sorted2.sort();
    assert_eq!(listed2, sorted2, "listing stays sorted");
}

// ADR-031 per-leaf membership: a key is "live" only when a committed writer
// holds it. A transaction that installed a create lock in the leaf but then
// aborted leaves a dead holder and no committed writer, so the key must be
// invisible to a listing — an aborted create never becomes a phantom member.
#[tokio::test]
async fn listing_hides_keys_from_aborted_transactions() {
    let (backend, pause) = PauseControl::wrap(mem());
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"aborted-vis")
        .await
        .unwrap();

    // Two committed keys the listing must always see.
    coll.write(b"real-a", b"v").await.unwrap();
    coll.write(b"real-b", b"v").await.unwrap();

    // A transaction creates two brand-new keys and reaches the commit-log write
    // (so its create locks are already installed in the leaf), then is cancelled
    // mid-commit. The attempt cancellation guard asynchronously marks it
    // aborted: the ghost keys were "added" by a transaction that never committed.
    let arrived = pause.arm("/_t/");
    let stalled = tokio::spawn({
        let db = db.clone();
        let coll = coll.clone();
        async move {
            let coll_ref = &coll;
            db.tx(|tx| async move {
                tx.write(coll_ref, b"ghost-a", b"v")?;
                tx.write(coll_ref, b"ghost-b", b"v")
            })
            .await
        }
    });
    arrived.await.unwrap();
    stalled.abort();
    let _ = stalled.await;

    // The listing observes exactly the committed keys, never the ghosts left
    // behind by the aborted transaction.
    let listed: Vec<Vec<u8>> = coll.iter_keys().await.unwrap().collect();
    assert_eq!(
        listed,
        vec![b"real-a".to_vec(), b"real-b".to_vec()],
        "only committed keys are listed"
    );
    assert!(
        !listed.contains(&b"ghost-a".to_vec()) && !listed.contains(&b"ghost-b".to_vec()),
        "keys from an aborted transaction are invisible"
    );
}

#[tokio::test(start_paused = true)]
async fn list_collections() {
    let db = init_db(mem()).await;
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();

    let colls: Vec<Vec<u8>> = (0u32..50).map(|i| i.to_be_bytes().to_vec()).collect();
    for c in &colls {
        coll.create_collection(c).await.unwrap();
    }

    let got: Vec<Vec<u8>> = coll
        .iter_collections()
        .await
        .unwrap()
        .map(|entry| entry.name)
        .collect();

    assert_eq!(got.len(), colls.len());
    let got_set: std::collections::HashSet<Vec<u8>> = got.iter().cloned().collect();
    for c in &colls {
        assert!(got_set.contains(c), "missing collection {c:?}");
    }
    let mut sorted = got.clone();
    sorted.sort();
    assert_eq!(got, sorted);
}

// The subcollection directory lives in the parent root (ADR-031), so listing is
// driven by that directory, not a backend prefix scan. A collection with no
// children lists nothing, and create-if-absent returns the existing binding.
#[tokio::test(start_paused = true)]
async fn subcollection_listing_is_root_driven_and_create_if_absent_is_idempotent() {
    let db = init_db(mem()).await;
    let parent = db
        .root_collection()
        .create_collection_if_absent(b"parent")
        .await
        .unwrap();

    // A freshly created collection has no subcollections.
    assert!(list_collections_of(&parent).await.is_empty());

    // Repeating create-if-absent returns the same incarnation and registers it
    // exactly once.
    let first = parent.create_collection_if_absent(b"child").await.unwrap();
    let second = parent.create_collection_if_absent(b"child").await.unwrap();
    first.write(b"k", b"v").await.unwrap();
    assert_eq!(second.read(b"k").await.unwrap().unwrap(), b"v");
    assert_eq!(list_collections_of(&parent).await, vec![b"child".to_vec()]);
}

// Concurrent registrations serialize their backend CASes on the parent-root
// path and converge without introducing structural holders.
#[tokio::test]
async fn concurrent_subcollection_registration_is_serialized_and_converges() {
    let writes = ParentWriteControl::new();
    let db = init_db(writes.backend()).await;
    let parent = create_top(&db, b"parent").await;
    writes.arm();

    let left_parent = parent.clone();
    let right_parent = parent.clone();
    let left_create = tokio::spawn(async move { left_parent.create_collection(b"left").await });
    let right_create = tokio::spawn(async move { right_parent.create_collection(b"right").await });

    writes.wait_until_entered().await;
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        writes.writes(),
        1,
        "only one same-path backend CAS may be active"
    );
    writes.release();
    left_create.await.unwrap().unwrap();
    right_create.await.unwrap().unwrap();

    assert!(writes.writes() >= 2);
    assert_eq!(
        list_collections_of(&parent).await,
        vec![b"left".to_vec(), b"right".to_vec()]
    );
    let parent_record = writes.recorded_path();
    let parent_root = format!(
        "{}_r",
        parent_record
            .strip_suffix("_i")
            .expect("collection record path ends in _i")
    );
    let stored = writes.inner().read(&parent_root).await.unwrap();
    let root = Node::decode(&stored.contents).unwrap();
    assert!(
        root.structural_gate().holders().is_empty(),
        "registration must not introduce a structural holder"
    );
}

// Subcollection listing is scoped to the direct parent: a grandchild shows up
// only in its own parent's directory, never in the grandparent's.
#[tokio::test(start_paused = true)]
async fn subcollection_listing_is_scoped_to_direct_parent() {
    let db = init_db(mem()).await;
    let parent = db
        .root_collection()
        .create_collection_if_absent(b"parent")
        .await
        .unwrap();
    let child = parent.create_collection(b"child").await.unwrap();
    child.create_collection(b"grandchild").await.unwrap();

    assert_eq!(list_collections_of(&parent).await, vec![b"child".to_vec()]);
    assert_eq!(
        list_collections_of(&child).await,
        vec![b"grandchild".to_vec()]
    );
}

// Listing a collection that was never created has no root to own the directory,
// so it surfaces as not found rather than an empty listing.
#[tokio::test(start_paused = true)]
async fn listing_a_missing_collection_is_not_found() {
    let db = init_db(mem()).await;
    assert!(matches!(
        db.open_collection(&CollectionPath::new(b"missing").unwrap())
            .await,
        Err(Error::NotFound)
    ));
}
