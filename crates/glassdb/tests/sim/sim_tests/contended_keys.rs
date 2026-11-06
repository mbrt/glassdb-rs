//! Leaves that separate the keys that several database instances write
//! (ADR-085, ADR-088).
//!
//! The tests run under the deterministic executor. Its virtual clock gives
//! the wall time of transaction identities and the monotonic time of the
//! database instances, and its seed gives their random bytes. So each seed
//! gives the same splits and merges at each run.
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use glassdb::backend::memory::MemoryBackend;
use glassdb::exec::{TapeScheduler, block_on_with};
use glassdb::middleware::HookBackend;
use glassdb::{
    Backend, Collection, CollectionPath, Database, KeyScan, TopologyPolicy, TopologyStats,
};
use glassdb_concurr::rt;

use crate::sim_support::tape;

const SEEDS: [u64; 4] = [1, 2, 3, 4];

const BACKEND_DELAY: Duration = Duration::from_millis(5);

const COLLECTION: &[u8] = b"contended";

// Each operation of this backend takes time, so that the database instances
// send leaf CASes at the same time.
fn slow_mem() -> Arc<dyn Backend> {
    let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
    backend.set_before(|_| {
        Box::pin(async {
            rt::sleep(BACKEND_DELAY).await;
            Ok(())
        })
    });
    backend
}

fn value(round: i64) -> Vec<u8> {
    round.to_le_bytes().to_vec()
}

// Runs `test` once for each seed.
fn for_each_seed<F, Fut>(test: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = ()> + 'static,
{
    for seed in SEEDS {
        block_on_with(TapeScheduler::new(tape(seed)), seed, test());
    }
}

// Four database instances with `policy` over one collection of 8 keys, which
// the first one wrote. The leaf of the 8 keys is below the soft cap.
async fn four_instances(policy: TopologyPolicy) -> (Vec<Database>, Vec<Collection>) {
    let backend = slow_mem();
    let mut dbs = Vec::new();
    for _ in 0..4 {
        let db = Database::builder("example", backend.clone())
            .topology_policy(policy)
            .open()
            .await
            .unwrap();
        dbs.push(db);
    }
    let path = CollectionPath::new(COLLECTION).unwrap();
    let coll = dbs[0].create_collection_if_absent(&path).await.unwrap();
    for key in 0..8 {
        coll.write(&[key], &value(0)).await.unwrap();
    }
    let mut colls = vec![coll];
    for db in &dbs[1..] {
        colls.push(db.open_collection(&path).await.unwrap());
    }
    (dbs, colls)
}

// Writes each key of `keys` `rounds` times, each key from its own task.
async fn write_concurrently(coll: &Collection, keys: impl IntoIterator<Item = u8>, rounds: i64) {
    let writers: Vec<_> = keys
        .into_iter()
        .map(|key| {
            let coll = coll.clone();
            rt::spawn(async move {
                for round in 0..rounds {
                    coll.write(&[key], &value(round)).await.unwrap();
                }
            })
        })
        .collect();
    for writer in writers {
        writer.await.unwrap();
    }
}

// Each database instance writes its own key of the keys 0 to 3, so the
// database instances lose leaf CASes to each other.
async fn write_one_key_each(colls: &[Collection]) {
    join_all(
        colls
            .iter()
            .zip(0u8..)
            .map(|(coll, key)| write_concurrently(coll, [key], 1000)),
    )
    .await;
}

fn sum(dbs: &[Database]) -> TopologyStats {
    let mut stats = TopologyStats::default();
    for db in dbs {
        stats += db.stats().topology;
    }
    stats
}

async fn shutdown(dbs: Vec<Database>) {
    for db in dbs {
        db.shutdown().await;
    }
}

// The leaf splits to separate the keys of the database instances, and no
// merge puts them back together while the database instances keep writing.
// Regression: the first database instance also wrote the keys 4 to 7, so it
// saw a local writer in each leaf, and merged them.
#[test]
fn the_keys_of_database_instances_split_apart() {
    for_each_seed(|| async {
        let (dbs, colls) = four_instances(TopologyPolicy::default()).await;

        write_one_key_each(&colls).await;

        let stats = sum(&dbs);
        assert!(stats.splits.contention.completed >= 1, "{stats:?}");
        assert_eq!(stats.merges.completed, 0, "{stats:?}");
        assert!(stats.merges.kept_apart >= 1, "{stats:?}");
        let keys = colls[0].scan_keys(KeyScan::all()).await.unwrap();
        assert_eq!(keys.keys().len(), 8);
        shutdown(dbs).await;
    });
}

// Only the leaf sizes decide the size causes, so the leaf below the soft cap
// does not split, even when the database instances lose leaf CASes to each
// other.
#[test]
fn the_size_causes_do_not_split_the_keys_of_database_instances_apart() {
    for_each_seed(|| async {
        let (dbs, colls) = four_instances(TopologyPolicy::SizeCauses).await;

        write_one_key_each(&colls).await;

        let stats = sum(&dbs);
        assert_eq!(
            (stats.splits.completed, stats.splits.contention.completed),
            (0, 0),
            "{stats:?}"
        );
        shutdown(dbs).await;
    });
}

// The rounds of one database instance share a leaf CAS, so its writers lose
// no leaf CASes to each other, and its keys stay in one leaf.
#[test]
fn the_keys_that_one_database_instance_writes_stay_together() {
    for_each_seed(|| async {
        let (dbs, colls) = four_instances(TopologyPolicy::default()).await;

        write_concurrently(&colls[0], 0..8, 1000).await;

        let stats = sum(&dbs);
        assert_eq!(
            (stats.splits.completed, stats.splits.contention.completed),
            (0, 0),
            "{stats:?}"
        );
        shutdown(dbs).await;
    });
}

// After the database instances stop, their writers stop being recent. Then the
// writes of one database instance to all keys merge the leaves again.
#[test]
fn the_leaves_merge_again_when_their_writers_stop() {
    for_each_seed(|| async {
        let (dbs, colls) = four_instances(TopologyPolicy::default()).await;
        write_one_key_each(&colls).await;
        let split = sum(&dbs);
        assert!(split.splits.contention.completed >= 1, "{split:?}");

        rt::sleep(Duration::from_secs(61)).await;
        for _ in 0..3 {
            write_concurrently(&colls[0], 0..8, 1).await;
            rt::sleep(Duration::from_secs(1)).await;
        }

        let merged = sum(&dbs);
        assert!(merged.merges.completed >= 1, "{merged:?}");
        shutdown(dbs).await;
    });
}
