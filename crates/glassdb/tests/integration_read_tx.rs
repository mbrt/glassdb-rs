//! Stale read-only transactions (ADR-097).

use std::time::Duration;

use glassdb::{Collection, Database, Error, KeyScan, ReadTransaction, Stats, Transaction};

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

async fn read_tx_all(
    db: &Database,
    staleness: Duration,
    collections: &[Collection],
) -> Vec<Vec<u8>> {
    let collections = collections.to_vec();
    db.read_tx(staleness, move |tx: ReadTransaction| {
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

#[tokio::test(start_paused = true)]
async fn read_tx_sees_the_writes_of_its_own_instance() {
    let db = Database::open("example", mem()).await.unwrap();
    let collections = collections_with_one_key(&db).await;
    tx_read_all(&db, &collections).await;

    collections[3].write(b"k", b"w").await.unwrap();
    let values = read_tx_all(&db, Duration::MAX, &collections).await;
    assert_eq!(values[3], b"w");
}

#[tokio::test(start_paused = true)]
async fn warm_read_tx_scan_needs_no_backend_operation() {
    let db = Database::open("example", mem()).await.unwrap();
    let c = create_top(&db, b"c").await;
    for key in [b"a", b"b", b"c"] {
        c.write(key, b"v").await.unwrap();
    }
    let scan = |db: Database, c: Collection| async move {
        db.read_tx(STALENESS, move |tx: ReadTransaction| {
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
        db.read_tx(STALENESS, move |tx: ReadTransaction| {
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
async fn read_tx_never_sees_an_earlier_state_than_a_previous_read_tx() {
    let backend = mem();
    let reader = Database::open("example", backend.clone()).await.unwrap();
    let (x, z) = cache_then_write_both(&reader, backend).await;

    // A zero bound needs a check, so this read finds the new state of `x`.
    assert_eq!(read_tx_all(&reader, Duration::ZERO, &[x]).await, [b"new"]);
    // Any age is accepted, but the floor is after the commit that `x` showed.
    assert_eq!(read_tx_all(&reader, Duration::MAX, &[z]).await, [b"new"]);
}

#[tokio::test(start_paused = true)]
async fn read_tx_after_tx_can_miss_a_foreign_write_that_tx_saw() {
    let backend = mem();
    let reader = Database::open("example", backend.clone()).await.unwrap();
    let (x, z) = cache_then_write_both(&reader, backend).await;

    assert_eq!(tx_read_all(&reader, &[x]).await, [b"new"]);
    // Only stale read-only transactions raise the floor.
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
        .read_tx(STALENESS, move |tx: ReadTransaction| {
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
        db.read_tx(STALENESS, |tx: ReadTransaction| async move {
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

/// When a state changed, the fallback validates at the barrier of the cut, so
/// the states that the cut already checked are not checked again.
#[tokio::test(start_paused = true)]
async fn a_changed_state_costs_read_tx_no_more_than_tx() {
    let mut costs = Vec::new();
    for stale in [false, true] {
        let backend = mem();
        let reader = Database::open("example", backend.clone()).await.unwrap();
        let collections = collections_with_one_key(&reader).await;
        tx_read_all(&reader, &collections).await;
        let writer = Database::open("example", backend).await.unwrap();
        open_top(&writer, b"c0")
            .await
            .write(b"k", b"w")
            .await
            .unwrap();

        let before = reader.stats();
        let values = if stale {
            read_tx_all(&reader, Duration::ZERO, &collections).await
        } else {
            tx_read_all(&reader, &collections).await
        };
        assert_eq!(values[0], b"w");
        costs.push(backend_ops(&reader, &before).0);
    }
    assert!(
        costs[1] <= costs[0],
        "read_tx {} > tx {}",
        costs[1],
        costs[0]
    );
}

/// After a fallback to the validation of `Database::tx`, the floor rises to
/// that validation barrier, so the states that it checked can form the next
/// cut with no backend operation.
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
    let values = read_tx_all(&reader, Duration::ZERO, &collections).await;
    assert_eq!(values[0], b"v");
    let before = reader.stats();
    read_tx_all(&reader, STALENESS, &collections[1..]).await;
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

    async fn open(
        backend: std::sync::Arc<dyn glassdb::Backend>,
        dir: &TempDir,
        l1: usize,
    ) -> Database {
        Database::builder("example", backend)
            .cache_size(l1)
            .persistent_cache(PersistentCacheConfig {
                directory: dir.0.clone(),
                capacity_bytes: 256 << 20,
                open_timeout: PersistentCacheConfig::DEFAULT_OPEN_TIMEOUT,
                shutdown_timeout: PersistentCacheConfig::DEFAULT_SHUTDOWN_TIMEOUT,
            })
            .open()
            .await
            .unwrap()
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
        (first, backend_ops(db, &before).0)
    }

    /// Opens a fresh instance on `backend` with the persistent cache in
    /// `dir`, after a first open stored every leaf body there. Leaves that only
    /// a CAS installed reach the persistent cache through a later fetch.
    async fn seeded(name: &str) -> (TempDir, std::sync::Arc<dyn glassdb::Backend>) {
        let dir = TempDir::new(name);
        let backend = mem();
        let db = open(backend.clone(), &dir, 512 << 20).await;
        collections_with_one_key(&db).await;
        db.shutdown().await;
        drop(db);
        let db = open(backend.clone(), &dir, 512 << 20).await;
        measure(&db, Duration::MAX).await;
        db.shutdown().await;
        (dir, backend)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn after_a_reopen_any_age_needs_no_check_and_a_finite_bound_checks_once() {
        let (dir, backend) = seeded("reopen").await;
        let db = open(backend.clone(), &dir, 512 << 20).await;
        assert_eq!(measure(&db, Duration::MAX).await, (0, 0));
        db.shutdown().await;
        drop(db);

        let db = open(backend, &dir, 512 << 20).await;
        assert_eq!(measure(&db, STALENESS).await, (LEAVES as u64, 0));
        db.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn states_that_return_from_the_persistent_cache_keep_their_interval() {
        let (dir, backend) = seeded("evict").await;
        // An L1 too small to keep a leaf, so every read comes from L2.
        let db = open(backend, &dir, 1).await;
        assert_eq!(measure(&db, STALENESS).await, (LEAVES as u64, 0));
        db.shutdown().await;
    }
}

/// `Collection::read_stale` is a stale read-only transaction with one read
/// (ADR-098), so the monotonic floor orders it with `read_tx` in both
/// directions.
#[tokio::test(start_paused = true)]
async fn read_stale_and_read_tx_share_the_monotonic_floor() {
    for read_stale_first in [false, true] {
        let backend = mem();
        let reader = Database::open("example", backend.clone()).await.unwrap();
        let (x, z) = cache_then_write_both(&reader, backend).await;

        let (first, second) = if read_stale_first {
            let first = x.read_stale(b"k", Duration::ZERO).await.unwrap().unwrap();
            let second = read_tx_all(&reader, Duration::MAX, &[z]).await.remove(0);
            (first, second)
        } else {
            let first = read_tx_all(&reader, Duration::ZERO, &[x]).await.remove(0);
            let second = z.read_stale(b"k", Duration::MAX).await.unwrap().unwrap();
            (first, second)
        };
        assert_eq!((first, second), (b"new".to_vec(), b"new".to_vec()));
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
    let value = c.read_stale(b"k", Duration::ZERO).await.unwrap();
    assert_eq!(value.as_deref(), Some(b"v".as_slice()));
    assert_eq!(backend_ops(&reader, &before), (1, 0, 0));
}

/// ADR-047 lets a stale read return data from before a drop of its collection
/// while its bound allows. A cut point before the drop keeps that rule, and a
/// check that observes the drop fails.
#[tokio::test(start_paused = true)]
async fn read_stale_returns_pre_drop_data_only_within_its_bound() {
    let backend = mem();
    let reader = Database::open("example", backend.clone()).await.unwrap();
    let c = create_top(&reader, b"c").await;
    c.write(b"k", b"v").await.unwrap();
    // The second read gives the cached states an interval.
    for _ in 0..2 {
        c.read_stale(b"k", Duration::MAX).await.unwrap();
    }
    let writer = Database::open("example", backend).await.unwrap();
    open_top(&writer, b"c")
        .await
        .drop_collection()
        .await
        .unwrap();

    let before = reader.stats();
    let value = c.read_stale(b"k", STALENESS).await.unwrap();
    assert_eq!(value.as_deref(), Some(b"v".as_slice()));
    assert_eq!(backend_ops(&reader, &before), (0, 0, 0));
    let err = c.read_stale(b"k", Duration::ZERO).await.unwrap_err();
    assert!(matches!(err, Error::StaleCollection), "{err:?}");
}
