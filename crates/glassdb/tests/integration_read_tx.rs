//! Stale read-only transactions (ADR-097), the cut points of read-only
//! `Database::tx` calls (ADR-099), and the cut point that a stale view keeps
//! (ADR-103).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use glassdb::backend::middleware::{BackendOp, HookBackend, HookFuture};
use glassdb::{
    Backend, Collection, CollectionPath, Database, Error, KeyScan, ReadTransaction,
    StaleDatabaseView, Stats, Transaction,
};

pub mod integration_support;

use integration_support::{create_top, mem, open_top};

const LEAVES: usize = 8;
const STALENESS: Duration = Duration::from_secs(10);

/// Creates one collection for each key, so that each key has its own leaf,
/// and writes `v` to key `k` of each.
async fn collections_with_one_key(db: &Database) -> Vec<Collection> {
    let mut collections = Vec::new();
    for i in 0..LEAVES {
        let collection = create_top(db, format!("c{i}").as_bytes()).await;
        collection.write(b"k", b"v").await.unwrap();
        collections.push(collection);
    }
    collections
}

async fn tx_read_all(db: &Database, collections: &[Collection]) -> Vec<Vec<u8>> {
    let collections = collections.to_vec();
    db.tx(move |tx: Transaction| {
        let collections = collections.clone();
        async move {
            let mut values = Vec::new();
            for collection in &collections {
                values.push(tx.read(collection, b"k").await?.unwrap_or_default());
            }
            Ok(values)
        }
    })
    .await
    .unwrap()
}

/// Reads key `k` of each collection through a new view of `db`.
async fn read_tx_all(
    db: &Database,
    staleness: Duration,
    collections: &[Collection],
) -> Vec<Vec<u8>> {
    view_read_all(&db.stale(staleness), collections).await
}

/// Reads key `k` of each collection in one call through `view`.
async fn view_read_all(view: &StaleDatabaseView, collections: &[Collection]) -> Vec<Vec<u8>> {
    let collections = collections.to_vec();
    view.read_tx(move |tx: ReadTransaction| {
        let collections = collections.clone();
        async move {
            let mut values = Vec::new();
            for collection in &collections {
                values.push(tx.read(collection, b"k").await?.unwrap_or_default());
            }
            Ok(values)
        }
    })
    .await
    .unwrap()
}

/// Returns the backend operations of `db` since `before`.
fn backend_ops(db: &Database, before: &Stats) -> (u64, u64, u64) {
    let delta = db.stats() - *before;
    (
        delta.backend.reads,
        delta.backend.writes,
        delta.backend.lists,
    )
}

#[tokio::test(start_paused = true)]
async fn warm_read_tx_over_many_leaves_needs_no_backend_operation() {
    let db = Database::open("example", mem()).await.unwrap();
    let collections = collections_with_one_key(&db).await;

    let before = db.stats();
    tx_read_all(&db, &collections).await;
    assert_eq!(
        backend_ops(&db, &before),
        (LEAVES as u64, 0, 0),
        "db.tx validates each leaf"
    );

    let before = db.stats();
    let values = read_tx_all(&db, STALENESS, &collections).await;
    assert_eq!(values, vec![b"v".to_vec(); LEAVES]);
    assert_eq!(backend_ops(&db, &before), (0, 0, 0));
}

#[tokio::test(start_paused = true)]
async fn read_tx_checks_states_older_than_the_staleness_bound() {
    let db = Database::open("example", mem()).await.unwrap();
    let collections = collections_with_one_key(&db).await;
    tx_read_all(&db, &collections).await;

    tokio::time::sleep(STALENESS * 2).await;
    let before = db.stats();
    read_tx_all(&db, STALENESS, &collections).await;
    assert_eq!(backend_ops(&db, &before), (LEAVES as u64, 0, 0));
}

/// Reads `keys` of `c` in one call through `view`.
async fn view_read_keys(view: &StaleDatabaseView, c: &Collection, keys: &[&[u8]]) -> Vec<Vec<u8>> {
    let c = c.clone();
    let keys: Vec<Vec<u8>> = keys.iter().map(|key| key.to_vec()).collect();
    view.read_tx(move |tx: ReadTransaction| {
        let (c, keys) = (c.clone(), keys.clone());
        async move {
            let mut values = Vec::new();
            for key in &keys {
                values.push(tx.read(&c, key).await?.unwrap_or_default());
            }
            Ok(values)
        }
    })
    .await
    .unwrap()
}

/// Writes `old` to key `k` of a new collection, and confirms its state, so
/// that the state has an interval.
async fn confirmed_collection(db: &Database) -> Collection {
    let c = create_top(db, b"c").await;
    c.write(b"k", b"old").await.unwrap();
    tx_read_all(db, std::slice::from_ref(&c)).await;
    c
}

#[tokio::test(start_paused = true)]
async fn a_view_keeps_its_cut_point_after_a_write_of_its_own_instance() {
    let db = Database::open("example", mem()).await.unwrap();
    let c = confirmed_collection(&db).await;
    let view = db.stale(Duration::MAX);
    assert_eq!(
        view_read_all(&view, std::slice::from_ref(&c)).await,
        [b"old"]
    );

    c.write(b"k", b"new").await.unwrap();
    let before = db.stats();
    assert_eq!(
        view_read_all(&view, std::slice::from_ref(&c)).await,
        [b"old"]
    );
    assert_eq!(backend_ops(&db, &before), (0, 0, 0));
    // A new view reads the cached state.
    assert_eq!(read_tx_all(&db, Duration::MAX, &[c]).await, [b"new"]);
}

#[tokio::test(start_paused = true)]
async fn a_view_keeps_its_cut_point_after_its_instance_sees_a_foreign_write() {
    let backend = mem();
    let reader = Database::open("example", backend.clone()).await.unwrap();
    let (x, z) = cache_then_write_both(&reader, backend).await;
    let both = [x, z];
    let view = reader.stale(Duration::MAX);
    assert_eq!(view_read_all(&view, &both).await, [b"old", b"old"]);

    assert_eq!(tx_read_all(&reader, &both).await, [b"new", b"new"]);
    let before = reader.stats();
    assert_eq!(view_read_all(&view, &both).await, [b"old", b"old"]);
    assert_eq!(backend_ops(&reader, &before), (0, 0, 0));
}

#[tokio::test(start_paused = true)]
async fn a_view_moves_its_cut_point_when_the_point_is_older_than_the_bound() {
    let db = Database::open("example", mem()).await.unwrap();
    let c = confirmed_collection(&db).await;
    let view = db.stale(STALENESS);
    assert_eq!(
        view_read_all(&view, std::slice::from_ref(&c)).await,
        [b"old"]
    );

    c.write(b"k", b"new").await.unwrap();
    assert_eq!(
        view_read_all(&view, std::slice::from_ref(&c)).await,
        [b"old"]
    );
    tokio::time::sleep(STALENESS * 2).await;
    assert_eq!(view_read_all(&view, &[c]).await, [b"new"]);
}

/// A write of another key of a leaf gives the leaf a new state, but the read
/// key keeps its writer, so the view keeps its cut point.
#[tokio::test(start_paused = true)]
async fn a_view_keeps_its_cut_point_when_only_another_key_of_the_leaf_changes() {
    let backend = mem();
    let reader = Database::open("example", backend.clone()).await.unwrap();
    let pair = create_top(&reader, b"pair").await;
    pair.write(b"a", b"a").await.unwrap();
    pair.write(b"b", b"b").await.unwrap();
    let other = create_top(&reader, b"other").await;
    other.write(b"k", b"v").await.unwrap();
    // Only the state of `other` has a confirmation after its start, so the
    // floor of the view is after the last confirmation of `pair`.
    tx_read_all(&reader, std::slice::from_ref(&other)).await;
    let view = reader.stale(Duration::MAX);
    assert_eq!(
        view_read_all(&view, std::slice::from_ref(&other)).await,
        [b"v"]
    );

    let writer = Database::open("example", backend).await.unwrap();
    open_top(&writer, b"pair")
        .await
        .write(b"b", b"new")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(pair.read(b"b").await.unwrap().unwrap(), b"new");

    let before = reader.stats();
    assert_eq!(view_read_keys(&view, &pair, &[b"a"]).await, [b"a"]);
    assert_eq!(view_read_all(&view, &[other]).await, [b"v"]);
    assert_eq!(backend_ops(&reader, &before), (0, 0, 0));
    writer.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn warm_read_tx_scan_needs_no_backend_operation() {
    let db = Database::open("example", mem()).await.unwrap();
    let c = create_top(&db, b"c").await;
    for key in [b"a", b"b", b"c"] {
        c.write(key, b"v").await.unwrap();
    }
    let scan = |db: Database, c: Collection| async move {
        db.stale(STALENESS)
            .read_tx(move |tx: ReadTransaction| {
                let c = c.clone();
                async move { Ok(tx.scan_keys(&c, KeyScan::all()).await?.keys().len()) }
            })
            .await
            .unwrap()
    };
    // The first scan confirms the leaf, which gives its state an interval.
    scan(db.clone(), c.clone()).await;
    scan(db.clone(), c.clone()).await;

    let before = db.stats();
    assert_eq!(scan(db.clone(), c.clone()).await, 3);
    assert_eq!(backend_ops(&db, &before), (0, 0, 0));
}

#[tokio::test(start_paused = true)]
async fn warm_read_tx_collection_listing_needs_no_backend_operation() {
    let db = Database::open("example", mem()).await.unwrap();
    let parent = create_top(&db, b"parent").await;
    for name in [b"x", b"y"] {
        parent.create_collection(name).await.unwrap();
    }
    let list = |db: Database, parent: Collection| async move {
        db.stale(STALENESS)
            .read_tx(move |tx: ReadTransaction| {
                let parent = parent.clone();
                async move {
                    let exists = tx.collection_exists(&parent, b"x").await?;
                    Ok((exists, tx.iter_collections(&parent).await?.count()))
                }
            })
            .await
            .unwrap()
    };
    list(db.clone(), parent.clone()).await;
    list(db.clone(), parent.clone()).await;

    let before = db.stats();
    assert_eq!(list(db.clone(), parent.clone()).await, (true, 2));
    assert_eq!(backend_ops(&db, &before), (0, 0, 0));
}

/// Calls each read helper of a collection and of a database. `$c` and `$db`
/// are the strong handles or their stale views, which have the same helpers.
macro_rules! call_read_helpers {
    ($c:expr, $db:expr) => {{
        let top = CollectionPath::new(b"c").unwrap();
        (
            $c.read(b"k").await.unwrap(),
            $c.read(b"missing").await.unwrap(),
            $c.scan_keys(KeyScan::all()).await.unwrap().into_keys(),
            $c.iter_collections()
                .await
                .unwrap()
                .map(|entry| entry.name)
                .collect::<Vec<_>>(),
            $c.open_collection(b"x")
                .await
                .unwrap()
                .name()
                .map(<[u8]>::to_vec),
            $c.collection_exists(b"x").await.unwrap(),
            $c.collection_exists(b"missing").await.unwrap(),
            $db.open_collection(&top)
                .await
                .unwrap()
                .name()
                .map(<[u8]>::to_vec),
            $db.collection_exists(&top.child(b"x").unwrap())
                .await
                .unwrap(),
            $db.collection_exists("missing").await.unwrap(),
        )
    }};
}

/// Each helper of a stale view gives the result of its strong helper, and with
/// a warm cache it needs no backend operation.
#[tokio::test(start_paused = true)]
async fn warm_stale_view_helpers_need_no_backend_operation() {
    let db = Database::open("example", mem()).await.unwrap();
    let c = create_top(&db, b"c").await;
    c.write(b"k", b"v").await.unwrap();
    c.create_collection(b"x").await.unwrap();

    let strong = call_read_helpers!(c, db);
    call_read_helpers!(c.stale(STALENESS), db.stale(STALENESS));

    let before = db.stats();
    let stale = call_read_helpers!(c.stale(STALENESS), db.stale(STALENESS));
    assert_eq!(stale, strong);
    assert_eq!(backend_ops(&db, &before), (0, 0, 0));
}

/// Caches the states of `x` and `z` in `reader` with a common interval, then
/// writes `new` to both from another instance.
async fn cache_then_write_both(
    reader: &Database,
    backend: std::sync::Arc<dyn glassdb::Backend>,
) -> (Collection, Collection) {
    let x = create_top(reader, b"x").await;
    let z = create_top(reader, b"z").await;
    x.write(b"k", b"old").await.unwrap();
    z.write(b"k", b"old").await.unwrap();
    tx_read_all(reader, &[x.clone(), z.clone()]).await;

    let writer = Database::open("example", backend).await.unwrap();
    let (wx, wz) = (open_top(&writer, b"x").await, open_top(&writer, b"z").await);
    writer
        .tx(move |tx| {
            let (x, z) = (wx.clone(), wz.clone());
            async move {
                tx.write(&x, b"k", b"new")?;
                tx.write(&z, b"k", b"new")
            }
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    (x, z)
}

#[tokio::test(start_paused = true)]
async fn calls_through_one_view_see_every_transaction_whose_effect_an_earlier_call_saw() {
    let backend = mem();
    let reader = Database::open("example", backend.clone()).await.unwrap();
    let (x, z) = cache_then_write_both(&reader, backend).await;
    // A strong read caches the new state of `x`, but does not order a view.
    assert_eq!(
        tx_read_all(&reader, std::slice::from_ref(&x)).await,
        [b"new"]
    );

    let view = reader.stale(Duration::MAX);
    assert_eq!(view_read_all(&view, &[x]).await, [b"new"]);
    // Any age is accepted, but the floor of the view is after the commit
    // that `x` showed, and a clone shares the floor.
    assert_eq!(view_read_all(&view.clone(), &[z]).await, [b"new"]);
}

#[tokio::test(start_paused = true)]
async fn calls_through_two_views_are_not_ordered() {
    let backend = mem();
    let reader = Database::open("example", backend.clone()).await.unwrap();
    let (x, z) = cache_then_write_both(&reader, backend).await;

    // A zero bound needs a check, so this read finds the new state of `x`.
    assert_eq!(read_tx_all(&reader, Duration::ZERO, &[x]).await, [b"new"]);
    // A new view has its own floor, so the cached state of `z` serves it.
    assert_eq!(read_tx_all(&reader, Duration::MAX, &[z]).await, [b"old"]);
}

#[tokio::test(start_paused = true)]
async fn read_tx_after_tx_can_miss_a_foreign_write_that_tx_saw() {
    let backend = mem();
    let reader = Database::open("example", backend.clone()).await.unwrap();
    let (x, z) = cache_then_write_both(&reader, backend).await;

    assert_eq!(tx_read_all(&reader, &[x]).await, [b"new"]);
    // A strong transaction raises no floor.
    assert_eq!(
        read_tx_all(&reader, STALENESS, std::slice::from_ref(&z)).await,
        [b"old"]
    );
    // A zero bound follows every commit that completed before the call.
    assert_eq!(read_tx_all(&reader, Duration::ZERO, &[z]).await, [b"new"]);
}

#[tokio::test(start_paused = true)]
async fn read_tx_returns_an_error_outcome_after_validation() {
    let db = Database::open("example", mem()).await.unwrap();
    let c = create_top(&db, b"c").await;
    let err = db
        .stale(STALENESS)
        .read_tx(move |tx: ReadTransaction| {
            let c = c.clone();
            async move {
                tx.read(&c, b"k").await?;
                tx.abort()
            }
        })
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Aborted), "{err:?}");
}

/// The record of a collection that the body opens is part of the cut, also
/// when a key access covers the binding (ADR-092 coverage needs a lock).
#[tokio::test(start_paused = true)]
async fn read_tx_checks_the_record_of_a_collection_that_it_opens() {
    let db = Database::open("example", mem()).await.unwrap();
    let c = create_top(&db, b"c").await;
    c.write(b"k", b"v").await.unwrap();
    let open_and_read = |db: Database| async move {
        db.stale(STALENESS)
            .read_tx(|tx: ReadTransaction| async move {
                let c = tx.open_collection(&tx.root_collection(), b"c").await?;
                tx.read(&c, b"k").await
            })
            .await
            .unwrap()
    };
    open_and_read(db.clone()).await;
    open_and_read(db.clone()).await;

    tokio::time::sleep(STALENESS * 2).await;
    // A read through the handle confirms the leaf, but not the record.
    tx_read_all(&db, std::slice::from_ref(&c)).await;
    let before = db.stats();
    assert_eq!(open_and_read(db.clone()).await.unwrap(), b"v");
    assert_eq!(backend_ops(&db, &before), (1, 0, 0));
}

/// After a fallback to the validation of `Database::tx`, the floor of the
/// view rises to that validation barrier, so the states that it checked can
/// form the next cut of the view with no backend operation.
#[tokio::test(start_paused = true)]
async fn read_tx_after_a_fallback_reuses_the_checked_states() {
    let inner = mem();
    let (hooked, control) = integration_support::PauseControl::wrap(inner.clone());
    let reader = Database::open("example", inner).await.unwrap();
    let collections = collections_with_one_key(&reader).await;
    tx_read_all(&reader, &collections).await;

    // A locked commit of another instance parks after its lock CAS, so the
    // key of `c0` has a pending write holder.
    let writer = Database::builder("example", hooked)
        .inline_policy(glassdb::InlinePolicy::none())
        .open()
        .await
        .unwrap();
    let wc0 = open_top(&writer, b"c0").await;
    let (arrived, release) = control.arm_leaf_write_gate();
    let pending = tokio::spawn(async move { wc0.write(b"k", b"w").await });
    arrived.await.unwrap();

    // The read of `c0` depends on the holder, so it validates at a barrier.
    let view = reader.stale(STALENESS);
    let values = view_read_all(&view, &collections).await;
    assert_eq!(values[0], b"v");
    let before = reader.stats();
    view_read_all(&view, &collections[1..]).await;
    assert_eq!(backend_ops(&reader, &before), (0, 0, 0));

    release.send(()).unwrap();
    pending.await.unwrap().unwrap();
}

mod persistent {
    use super::*;
    use glassdb::PersistentCacheConfig;

    struct TempDir(std::path::PathBuf);

    impl TempDir {
        fn new(name: &str) -> Self {
            let path =
                std::env::temp_dir().join(format!("glassdb-read-tx-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Longer than the default open and shutdown timeouts, so that a slow
    /// disk does not disable the persistent cache. A `fsync` can wait tens of
    /// seconds for writeback, for example just after a build.
    const CACHE_TIMEOUT: Duration = Duration::from_secs(300);

    async fn open(
        backend: std::sync::Arc<dyn glassdb::Backend>,
        dir: &TempDir,
        l1: usize,
    ) -> Database {
        let db = Database::builder("example", backend)
            .cache_size(l1)
            .persistent_cache(PersistentCacheConfig {
                directory: dir.0.clone(),
                capacity_bytes: 256 << 20,
                open_timeout: CACHE_TIMEOUT,
                shutdown_timeout: CACHE_TIMEOUT,
            })
            .open()
            .await
            .unwrap();
        assert_cache_enabled(&db, "open");
        db
    }

    async fn shutdown(db: &Database) {
        db.shutdown().await;
        assert_cache_enabled(db, "shutdown");
    }

    /// Fails when the persistent cache disabled itself. These tests count
    /// backend reads, so without this check a disabled cache makes them fail
    /// with more reads than expected, and the cause is not clear.
    fn assert_cache_enabled(db: &Database, step: &str) {
        assert_eq!(
            db.stats().cache.l2_errors,
            0,
            "the persistent cache disabled itself before the end of {step}, \
             probably because a disk operation took longer than its timeout"
        );
    }

    async fn open_collections(db: &Database) -> Vec<Collection> {
        let mut collections = Vec::new();
        for i in 0..LEAVES {
            collections.push(open_top(db, format!("c{i}").as_bytes()).await);
        }
        collections
    }

    async fn measure(db: &Database, staleness: Duration) -> (u64, u64) {
        let collections = open_collections(db).await;
        let before = db.stats();
        read_tx_all(db, staleness, &collections).await;
        let first = backend_ops(db, &before).0;
        let before = db.stats();
        read_tx_all(db, staleness, &collections).await;
        let second = backend_ops(db, &before).0;
        assert_cache_enabled(db, "the reads");
        (first, second)
    }

    /// Opens a fresh instance on `backend` with the persistent cache in
    /// `dir`, after a first open stored every leaf body there. Leaves that only
    /// a CAS installed reach the persistent cache through a later fetch.
    async fn seeded(name: &str) -> (TempDir, std::sync::Arc<dyn glassdb::Backend>) {
        let dir = TempDir::new(name);
        let backend = mem();
        let db = open(backend.clone(), &dir, 512 << 20).await;
        collections_with_one_key(&db).await;
        shutdown(&db).await;
        drop(db);
        let db = open(backend.clone(), &dir, 512 << 20).await;
        measure(&db, Duration::MAX).await;
        shutdown(&db).await;
        (dir, backend)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn after_a_reopen_any_age_needs_no_check_and_a_finite_bound_checks_once() {
        let (dir, backend) = seeded("reopen").await;
        let db = open(backend.clone(), &dir, 512 << 20).await;
        assert_eq!(measure(&db, Duration::MAX).await, (0, 0));
        shutdown(&db).await;
        drop(db);

        let db = open(backend, &dir, 512 << 20).await;
        assert_eq!(measure(&db, STALENESS).await, (LEAVES as u64, 0));
        shutdown(&db).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn states_that_return_from_the_persistent_cache_keep_their_interval() {
        let (dir, backend) = seeded("evict").await;
        // An L1 too small to keep a leaf, so every read comes from L2.
        let db = open(backend, &dir, 1).await;
        assert_eq!(measure(&db, STALENESS).await, (LEAVES as u64, 0));
        shutdown(&db).await;
    }
}

/// A stale read is a stale read-only transaction with one read (ADR-098)
/// through a view of its collection. That view and a view of the database
/// have their own floors, so they are not ordered, in either direction.
#[tokio::test(start_paused = true)]
async fn a_stale_read_and_a_read_tx_of_two_views_are_not_ordered() {
    for stale_read_first in [false, true] {
        let backend = mem();
        let reader = Database::open("example", backend.clone()).await.unwrap();
        let (x, z) = cache_then_write_both(&reader, backend).await;

        let (first, second) = if stale_read_first {
            let first = x.stale(Duration::ZERO).read(b"k").await.unwrap().unwrap();
            let second = read_tx_all(&reader, Duration::MAX, &[z]).await.remove(0);
            (first, second)
        } else {
            let first = read_tx_all(&reader, Duration::ZERO, &[x]).await.remove(0);
            let second = z.stale(Duration::MAX).read(b"k").await.unwrap().unwrap();
            (first, second)
        };
        assert_eq!((first, second), (b"new".to_vec(), b"old".to_vec()));
    }
}

/// The staleness bound is measured from the start of the call, so the fetch
/// of an uncached state in the body already reaches a zero bound.
#[tokio::test(start_paused = true)]
async fn a_zero_bound_read_of_an_uncached_key_fetches_it_once() {
    let backend = mem();
    let writer = Database::open("example", backend.clone()).await.unwrap();
    create_top(&writer, b"c")
        .await
        .write(b"k", b"v")
        .await
        .unwrap();
    let reader = Database::open("example", backend).await.unwrap();
    let c = open_top(&reader, b"c").await;

    let before = reader.stats();
    let value = c.stale(Duration::ZERO).read(b"k").await.unwrap();
    assert_eq!(value.as_deref(), Some(b"v".as_slice()));
    assert_eq!(backend_ops(&reader, &before), (1, 0, 0));
}

/// A read-only `Database::tx` measures its zero staleness bound from the start
/// of the call too, so the fetch of an uncached state needs no check.
#[tokio::test(start_paused = true)]
async fn a_read_only_tx_of_an_uncached_key_fetches_it_once() {
    let backend = mem();
    let writer = Database::open("example", backend.clone()).await.unwrap();
    create_top(&writer, b"c")
        .await
        .write(b"k", b"v")
        .await
        .unwrap();
    let reader = Database::open("example", backend).await.unwrap();
    let c = open_top(&reader, b"c").await;

    let before = reader.stats();
    let values = tx_read_all(&reader, std::slice::from_ref(&c)).await;
    assert_eq!(values, [b"v"]);
    assert_eq!(backend_ops(&reader, &before), (1, 0, 0));
}

/// A read-only execution whose cut finds a changed state replays. A replay
/// that writes commits through the commit path, which does not use the
/// staleness cutoff of the call.
#[tokio::test(start_paused = true)]
async fn a_replay_that_writes_after_a_read_only_execution_commits() {
    let backend = mem();
    let reader = Database::open("example", backend.clone()).await.unwrap();
    let c = create_top(&reader, b"c").await;
    c.write(b"k", b"old").await.unwrap();
    let writer = Database::open("example", backend).await.unwrap();
    let w = open_top(&writer, b"c").await;

    let before = reader.stats();
    let first = AtomicBool::new(true);
    let copied = reader
        .tx(|tx| {
            let (c, w, first) = (c.clone(), w.clone(), &first);
            async move {
                let value = tx.read(&c, b"k").await?.unwrap_or_default();
                if first.swap(false, Ordering::SeqCst) {
                    // The cached state of `k` precedes the call, and another
                    // instance replaces it before the certification.
                    w.write(b"k", b"new").await?;
                    return Ok(value);
                }
                tx.write(&c, b"copy", &value)?;
                Ok(value)
            }
        })
        .await
        .unwrap();

    assert_eq!(copied, b"new");
    assert_eq!(c.read(b"copy").await.unwrap().as_deref(), Some(&b"new"[..]));
    assert_eq!((reader.stats() - before).transactions.replays, 1);
}

/// ADR-047 lets a stale read return data from before a drop of its collection
/// while its bound allows. A cut point before the drop keeps that rule, and a
/// check that observes the drop fails.
/// Returns a backend over `inner` that holds every operation after the first
/// transaction record that it commits, and the flag that reports that commit.
fn hold_after_commit(inner: Arc<dyn Backend>) -> (Arc<dyn Backend>, Arc<AtomicBool>) {
    let committed = Arc::new(AtomicBool::new(false));
    let backend = HookBackend::new(inner);
    backend.set_before({
        let committed = committed.clone();
        move |_| {
            let hold = committed.load(Ordering::SeqCst);
            let future: HookFuture = Box::pin(async move {
                if hold {
                    std::future::pending::<()>().await;
                }
                Ok(())
            });
            future
        }
    });
    backend.set_after({
        let committed = committed.clone();
        move |op, outcome| {
            if outcome.is_success()
                && matches!(op, BackendOp::WriteIf { path, .. } if path.contains("/_t/"))
            {
                committed.store(true, Ordering::SeqCst);
            }
            let future: HookFuture = Box::pin(async { Ok(()) });
            future
        }
    });
    (backend, committed)
}

/// A body opens a collection through a cached binding while another instance
/// drops it. The drop has committed, but it has not updated the records yet,
/// so a lookup in the collection finds the committed drop. No cut point
/// explains that absence, so the body replays and sees the drop instead of
/// returning `StaleCollection`.
#[tokio::test(start_paused = true)]
async fn read_tx_replays_when_its_body_finds_a_committed_drop() {
    let backend = mem();
    let setup = Database::open("example", backend.clone()).await.unwrap();
    create_top(&setup, b"c")
        .await
        .write(b"k", b"v")
        .await
        .unwrap();
    let reader = Database::open("example", backend.clone()).await.unwrap();
    // The second run gives the cached binding and leaf an interval.
    for _ in 0..2 {
        reader
            .stale(Duration::MAX)
            .read_tx(|tx: ReadTransaction| async move {
                let c = tx.open_collection(&tx.root_collection(), b"c").await?;
                tx.read(&c, b"k").await
            })
            .await
            .unwrap();
    }
    let (held, committed) = hold_after_commit(backend);
    let writer = Database::open("example", held).await.unwrap();
    let c = open_top(&writer, b"c").await;
    let drop = tokio::spawn(async move { c.drop_collection().await });
    while !committed.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }

    let child = reader
        .stale(Duration::MAX)
        .read_tx(|tx: ReadTransaction| async move {
            let c = match tx.open_collection(&tx.root_collection(), b"c").await {
                Ok(c) => c,
                Err(Error::NotFound) => return Ok(None),
                Err(error) => return Err(error),
            };
            tx.read(&c, b"k").await?;
            Ok(Some(tx.collection_exists(&c, b"child").await?))
        })
        .await;
    assert!(matches!(child, Ok(None)), "{child:?}");
    drop.abort();
}

#[tokio::test(start_paused = true)]
async fn stale_read_returns_pre_drop_data_only_within_its_bound() {
    let backend = mem();
    let reader = Database::open("example", backend.clone()).await.unwrap();
    let c = create_top(&reader, b"c").await;
    c.write(b"k", b"v").await.unwrap();
    // The second read gives the cached states an interval.
    for _ in 0..2 {
        c.stale(Duration::MAX).read(b"k").await.unwrap();
    }
    let writer = Database::open("example", backend).await.unwrap();
    open_top(&writer, b"c")
        .await
        .drop_collection()
        .await
        .unwrap();

    let before = reader.stats();
    let value = c.stale(STALENESS).read(b"k").await.unwrap();
    assert_eq!(value.as_deref(), Some(b"v".as_slice()));
    assert_eq!(backend_ops(&reader, &before), (0, 0, 0));
    let err = c.stale(Duration::ZERO).read(b"k").await.unwrap_err();
    assert!(matches!(err, Error::StaleCollection), "{err:?}");
}
