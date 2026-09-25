//! Leaf splits and merges that avoidable time decides (ADR-074).

use std::sync::Arc;
use std::time::Duration;

use glassdb::middleware::HookBackend;
use glassdb::{
    Backend, Collection, Database, KeyScan, LeafChanges, NodeSizePolicy, RestructurerStats,
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
        .leaf_min_entries(2)
        .index_min_children(0)
        .build()
        .unwrap()
}

async fn open(backend: &Arc<dyn Backend>, sizes: NodeSizePolicy, rule: LeafChanges) -> Database {
    Database::builder("example", backend.clone())
        .node_size_policy(sizes)
        .leaf_changes(rule)
        .open()
        .await
        .unwrap()
}

async fn write_keys(coll: &Collection, keys: u8) {
    for key in 0..keys {
        coll.write(&[key], &write_int(0)).await.unwrap();
    }
}

async fn scan_crossings(db: &Database, coll: &Collection) -> u64 {
    let before = db.stats();
    coll.scan_keys(KeyScan::all()).await.unwrap();
    (db.stats() - before).transactions.scan_leaf_crossings
}

// Writes 16 keys over leaves of at most 4 entries, and waits for the splits
// of the soft caps.
async fn split_collection(backend: &Arc<dyn Backend>) {
    let db = open(backend, leaves_of_at_most(4), LeafChanges::Size).await;
    let coll = create_top(&db, b"split").await;
    write_keys(&coll, 16).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(scan_crossings(&db, &coll).await >= 3);
    db.shutdown().await;
}

// The leaves are below the underfull threshold of the size rule, but no
// transaction crosses them, so a merge would remove no avoidable time.
#[tokio::test(start_paused = true)]
async fn leaves_that_no_transaction_crosses_do_not_merge() {
    let backend = slow_mem();
    let db = open(&backend, leaves_of_at_most(4), LeafChanges::default()).await;
    let coll = create_top(&db, b"sparse").await;
    write_keys(&coll, 16).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let splits = db.stats().restructurer.splits;
    assert!(splits >= 3, "splits: {splits}");

    for key in (0..16).filter(|key| key % 4 != 0) {
        coll.delete(&[key]).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(5)).await;

    assert_eq!(db.stats().restructurer.merges, 0);
    db.shutdown().await;
}

// Scans of 8 concurrent readers, for about 3 seconds.
async fn scan_concurrently(coll: &Collection) {
    let scanners = (0..8).map(|_| {
        let coll = coll.clone();
        tokio::spawn(async move {
            for _ in 0..50 {
                coll.scan_keys(KeyScan::all()).await.unwrap();
            }
        })
    });
    for scanner in scanners.collect::<Vec<_>>() {
        scanner.await.unwrap();
    }
}

// Concurrent scans that cross leaves pay for merges, until the merged leaves
// reach half of the soft caps. One sequential scanner does not pay for a
// merge in one window.
#[tokio::test(start_paused = true)]
async fn scans_that_cross_leaves_merge_them_within_the_size_vetoes() {
    let backend = slow_mem();
    split_collection(&backend).await;

    let db = open(&backend, leaves_of_at_most(4), LeafChanges::default()).await;
    let coll = open_top(&db, b"split").await;
    scan_concurrently(&coll).await;
    assert_eq!(db.stats().restructurer.merges, 0);
    db.shutdown().await;

    let db = open(&backend, leaves_of_at_most(64), LeafChanges::default()).await;
    let coll = open_top(&db, b"split").await;
    let crossings = scan_crossings(&db, &coll).await;
    scan_concurrently(&coll).await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    assert!(db.stats().restructurer.merges >= 1);
    assert!(scan_crossings(&db, &coll).await < crossings);
    db.shutdown().await;
}

/// Which of two database instances writes each of 8 keys.
type Owner = fn(u8) -> usize;

const HALVES: Owner = |key| usize::from(key / 4);
const INTERLEAVED: Owner = |key| usize::from(key % 2);

/// The outcome of [`contend`].
struct Contention {
    /// The sum of the restructurer stats of both instances.
    stats: RestructurerStats,
    /// The leaf boundaries between the keys 3 and 4, which separate the
    /// halves of [`HALVES`].
    middle_crossings: u64,
}

// Opens two database instances with `rule` over one leaf of 8 keys, and
// writes each key 200 times from the instance that `owner` gives.
async fn contend(rule: LeafChanges, owner: Owner) -> Contention {
    let backend = slow_mem();
    let first = open(&backend, NodeSizePolicy::default(), rule).await;
    let second = open(&backend, NodeSizePolicy::default(), rule).await;
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
    Contention {
        stats,
        middle_crossings: middle_crossings(&backend).await,
    }
}

// A new instance reads the current leaves, and not the ones in the caches of
// the writers.
async fn middle_crossings(backend: &Arc<dyn Backend>) -> u64 {
    let reader = open(backend, NodeSizePolicy::default(), LeafChanges::Size).await;
    let coll = open_top(&reader, b"contended").await;
    let before = reader.stats();
    // A scan crosses into the next leaf when its end is the first key of that
    // leaf, so the end is just after key 4.
    coll.scan_keys(KeyScan::range(&[3], &[4, 0])).await.unwrap();
    let crossings = (reader.stats() - before).transactions.scan_leaf_crossings;
    reader.shutdown().await;
    crossings
}

// Two database instances that write different halves of one leaf lose CASes
// to each other, which one split between the halves can remove. The soft caps
// do not split the leaf. The writers of one instance share rounds, so they
// alone cause no avoidable time.
#[tokio::test(start_paused = true)]
async fn a_leaf_that_instances_contend_on_splits_between_them() {
    let Contention {
        stats,
        middle_crossings,
    } = contend(LeafChanges::default(), HALVES).await;

    assert!(stats.avoidable.split.lost_cas > Duration::ZERO);
    assert!(stats.splits >= 1, "{stats:?}");
    assert_eq!(middle_crossings, 1);
}

// A split at the median leaves writers of both instances in each half, so the
// CASes that they lose are not avoidable time.
#[tokio::test(start_paused = true)]
async fn a_leaf_that_instances_write_interleaved_keys_of_does_not_split() {
    let Contention { stats, .. } = contend(LeafChanges::default(), INTERLEAVED).await;

    assert_eq!(stats.avoidable.split.lost_cas, Duration::ZERO);
    assert_eq!(stats.splits, 0, "{stats:?}");
}

// The same contention does not split a leaf when the split threshold is too
// high for the time that it causes.
#[tokio::test(start_paused = true)]
async fn the_split_threshold_scales_the_time_that_pays_for_a_split() {
    let rule = LeafChanges::AvoidableTime {
        split_threshold: 1000.0,
        merge_threshold: 1.0,
    };
    let Contention { stats, .. } = contend(rule, HALVES).await;

    assert!(stats.avoidable.split.lost_cas > Duration::ZERO);
    assert_eq!(stats.splits, 0);
}
