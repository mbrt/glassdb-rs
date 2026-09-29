//! Leaf splits and merges that the avoidable time policy decides (ADR-074).

use std::sync::Arc;
use std::time::Duration;

use glassdb::middleware::HookBackend;
use glassdb::{
    Backend, Collection, Database, Error, KeyScan, NodeSizePolicy, RestructurerStats,
    TopologyPolicy,
};

pub mod integration_support;

use integration_support::{create_top, mem, open_top, write_int};

const BACKEND_DELAY: Duration = Duration::from_millis(5);

// Under paused time, the memory backend takes no time, and so would every
// avoidable time. Each operation of this backend takes time.
fn slow_mem() -> Arc<dyn Backend> {
    let backend = HookBackend::new(mem());
    backend.set_before(|_| {
        Box::pin(async {
            tokio::time::sleep(BACKEND_DELAY).await;
            Ok(())
        })
    });
    backend
}

fn leaves_of_at_most(entries: usize) -> NodeSizePolicy {
    NodeSizePolicy::builder()
        .leaf_max_entries(entries)
        .leaf_min_entries(0)
        .index_min_children(0)
        .build()
        .unwrap()
}

async fn open(backend: &Arc<dyn Backend>, sizes: NodeSizePolicy) -> Database {
    Database::builder("example", backend.clone())
        .node_size_policy(sizes)
        .topology_policy(TopologyPolicy::AvoidableTime)
        .open()
        .await
        .unwrap()
}

async fn write_keys(coll: &Collection, keys: u8) {
    for key in 0..keys {
        coll.write(&[key], &write_int(0)).await.unwrap();
    }
}

async fn scan_all(coll: &Collection) -> Vec<Vec<u8>> {
    coll.scan_keys(KeyScan::all())
        .await
        .unwrap()
        .keys()
        .to_vec()
}

// Opens a collection of `keys` keys over leaves of at most 4 entries, and
// returns the splits that the soft caps caused.
async fn split_collection(backend: &Arc<dyn Backend>, keys: u8) -> u64 {
    let db = open(backend, leaves_of_at_most(4)).await;
    let coll = create_top(&db, b"split").await;
    write_keys(&coll, keys).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let splits = db.stats().restructurer.splits;
    db.shutdown().await;
    splits
}

// The avoidable time policy decides only the changes that the node sizes do
// not force.
#[tokio::test(start_paused = true)]
async fn leaves_over_a_soft_cap_still_split() {
    assert!(split_collection(&slow_mem(), 16).await >= 3);
}

// A scan that continues into the next leaf reads one more leaf, which one
// merge of the two leaves removes.
#[tokio::test(start_paused = true)]
async fn scans_that_cross_leaves_merge_them() {
    let backend = slow_mem();
    split_collection(&backend, 16).await;
    let db = open(&backend, leaves_of_at_most(64)).await;
    let coll = open_top(&db, b"split").await;

    let until = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < until {
        scan_all(&coll).await;
    }

    assert!(db.stats().restructurer.merges >= 1);
    assert_eq!(scan_all(&coll).await.len(), 16);
    db.shutdown().await;
}

// Regression: a body that returned an error replayed each time that a write
// changed its reads in two adjacent leaves before the validation of the
// reads. The time of these replays did not count for a merge of the leaves.
#[tokio::test(start_paused = true)]
async fn replays_of_failed_bodies_over_two_leaves_merge_them() {
    let backend = slow_mem();
    assert_eq!(split_collection(&backend, 5).await, 1);
    let db = open(&backend, leaves_of_at_most(64)).await;
    let coll = open_top(&db, b"split").await;
    let writer = {
        let coll = coll.clone();
        tokio::spawn(async move {
            let mut round = 0;
            loop {
                coll.write(&[0], &write_int(round)).await.unwrap();
                coll.write(&[4], &write_int(round)).await.unwrap();
                round += 1;
            }
        })
    };

    let c = &coll;
    let until = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < until {
        let result = db
            .tx(|tx| async move {
                tx.read(c, &[0]).await?;
                tx.read(c, &[4]).await?;
                Err::<(), _>(Error::NotFound)
            })
            .await;
        assert!(matches!(result, Err(Error::NotFound)), "{result:?}");
    }
    writer.abort();

    assert!(db.stats().restructurer.merges >= 1);
    db.shutdown().await;
}

// Regression: under the avoidable time policy, a leaf whose keys were all
// deleted stayed, because no transaction used it again and so it got no
// merge-side time. With a queue-like load, the leaves grew without a limit.
#[tokio::test(start_paused = true)]
async fn leaves_whose_keys_are_all_deleted_merge() {
    let backend = slow_mem();
    let splits = split_collection(&backend, 16).await;
    let db = open(&backend, leaves_of_at_most(64)).await;
    let coll = open_top(&db, b"split").await;

    for key in 0..15 {
        coll.delete(&[key]).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Each split added one leaf, and only the last leaf has a live entry.
    assert_eq!(db.stats().restructurer.merges, splits);
    assert_eq!(scan_all(&coll).await, vec![vec![15]]);
    db.shutdown().await;
}

type Owner = fn(u8) -> usize;

const HALVES: Owner = |key| usize::from(key / 4);
const INTERLEAVED: Owner = |key| usize::from(key % 2);

// Opens two database instances with the avoidable time policy over one leaf
// of 8 keys, writes each key 200 times from the instance that `owner` gives,
// and returns the sum of the restructurer stats of both instances.
async fn contend(owner: Owner) -> RestructurerStats {
    let backend = slow_mem();
    let first = open(&backend, NodeSizePolicy::default()).await;
    let second = open(&backend, NodeSizePolicy::default()).await;
    let coll = create_top(&first, b"contended").await;
    write_keys(&coll, 8).await;
    let colls = [coll, open_top(&second, b"contended").await];

    let writers = (0u8..8).map(|key| {
        let coll = colls[owner(key)].clone();
        tokio::spawn(async move {
            for round in 0..200 {
                coll.write(&[key], &write_int(round)).await.unwrap();
            }
        })
    });
    for writer in writers.collect::<Vec<_>>() {
        writer.await.unwrap();
    }

    let mut stats = first.stats().restructurer;
    stats += second.stats().restructurer;
    first.shutdown().await;
    second.shutdown().await;
    stats
}

// Two database instances that write different halves of one leaf lose CASes
// to each other, which one split between the halves can remove. The soft caps
// do not split the leaf. The writers of one instance share rounds, so they
// alone cause no avoidable time.
#[tokio::test(start_paused = true)]
async fn a_leaf_splits_between_the_instances_that_contend_on_it() {
    let stats = contend(HALVES).await;

    assert!(stats.splits >= 1, "{stats:?}");
}

// A split at the median leaves writers of both instances in each half, so the
// CASes that they lose are not avoidable time.
#[tokio::test(start_paused = true)]
async fn a_leaf_that_instances_write_interleaved_keys_of_does_not_split() {
    let stats = contend(INTERLEAVED).await;

    assert_eq!(stats.splits, 0, "{stats:?}");
}
