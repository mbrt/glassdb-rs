//! Leaf splits and merges that a topology policy decides (ADR-074).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use glassdb::middleware::HookBackend;
use glassdb::{
    AvoidableTimePolicy, Backend, Collection, Database, FixedTopology, KeyScan, NodeSizePolicy,
    RestructurerStats, TopologyChange, TopologyPolicy, TopologyWindow,
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

async fn open(
    backend: &Arc<dyn Backend>,
    sizes: NodeSizePolicy,
    policy: impl TopologyPolicy,
) -> Database {
    Database::builder("example", backend.clone())
        .node_size_policy(sizes)
        .topology_policy(policy)
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

/// Records each window, and applies one decision rule to it.
struct Recording<F> {
    windows: Arc<Mutex<Vec<TopologyWindow>>>,
    decide: F,
}

impl<F> Recording<F>
where
    F: Fn(&TopologyWindow) -> Vec<TopologyChange> + Send + Sync + 'static,
{
    fn new(decide: F) -> (Self, Arc<Mutex<Vec<TopologyWindow>>>) {
        let windows = Arc::new(Mutex::new(Vec::new()));
        let policy = Self {
            windows: windows.clone(),
            decide,
        };
        (policy, windows)
    }
}

impl<F> TopologyPolicy for Recording<F>
where
    F: Fn(&TopologyWindow) -> Vec<TopologyChange> + Send + Sync + 'static,
{
    fn decide(&self, window: &TopologyWindow) -> Vec<TopologyChange> {
        self.windows.lock().unwrap().push(window.clone());
        (self.decide)(window)
    }
}

fn merge_every_pair(window: &TopologyWindow) -> Vec<TopologyChange> {
    window
        .pairs
        .iter()
        .map(|pair| TopologyChange::Merge(pair.left.clone()))
        .collect()
}

// Opens a collection of 16 keys over leaves of at most 4 entries, after the
// background splits of the soft caps.
async fn split_collection(backend: &Arc<dyn Backend>) {
    let db = Database::builder("example", backend.clone())
        .node_size_policy(leaves_of_at_most(4))
        .open()
        .await
        .unwrap();
    let coll = create_top(&db, b"split").await;
    write_keys(&coll, 16).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(scan_crossings(&db, &coll).await >= 3);
    db.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_policy_replaces_the_soft_caps_of_leaves() {
    let backend = slow_mem();
    let db = open(&backend, leaves_of_at_most(4), FixedTopology).await;
    let coll = create_top(&db, b"fixed").await;

    write_keys(&coll, 16).await;
    tokio::time::sleep(Duration::from_secs(5)).await;

    assert_eq!(db.stats().restructurer.splits, 0);
    assert_eq!(scan_crossings(&db, &coll).await, 0);
    db.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn the_splits_of_a_policy_land_and_the_next_windows_show_the_changed_leaves() {
    let backend = slow_mem();
    let (policy, windows) = Recording::new(|window: &TopologyWindow| {
        window
            .leaves
            .iter()
            .filter(|(_, leaf)| leaf.size.is_some_and(|size| size.entries > 4))
            .map(|(id, _)| TopologyChange::Split(id.clone()))
            .collect()
    });
    let db = open(&backend, NodeSizePolicy::default(), policy).await;
    let coll = create_top(&db, b"split").await;

    write_keys(&coll, 16).await;
    tokio::time::sleep(Duration::from_secs(5)).await;

    assert!(db.stats().restructurer.splits >= 3);
    assert!(scan_crossings(&db, &coll).await >= 3);
    db.shutdown().await;
    let windows = windows.lock().unwrap();
    assert!(
        windows
            .iter()
            .any(|window| window.leaves.values().any(|leaf| leaf.split_recently)),
    );
}

// A demand merge does not need an underfull leaf. The merged leaf must still
// stay at or below half of the soft caps.
#[tokio::test(start_paused = true)]
async fn merges_of_a_policy_skip_the_underfull_threshold_but_not_the_size_vetoes() {
    let backend = slow_mem();
    split_collection(&backend).await;

    let (policy, _) = Recording::new(merge_every_pair);
    let db = open(&backend, leaves_of_at_most(4), policy).await;
    let coll = open_top(&db, b"split").await;
    for _ in 0..5 {
        scan_crossings(&db, &coll).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert_eq!(db.stats().restructurer.merges, 0);
    db.shutdown().await;

    let (policy, _) = Recording::new(merge_every_pair);
    let db = open(&backend, leaves_of_at_most(64), policy).await;
    let coll = open_top(&db, b"split").await;
    let crossings = scan_crossings(&db, &coll).await;
    for _ in 0..10 {
        scan_crossings(&db, &coll).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert!(db.stats().restructurer.merges >= 1);
    assert!(scan_crossings(&db, &coll).await < crossings);
    db.shutdown().await;
}

// A transaction with keys in all the leaves can use a direct commit only after
// the merges of all the pairs, so each pair shows a part of its time.
#[tokio::test(start_paused = true)]
async fn each_pair_of_a_chain_of_leaves_shows_the_time_of_a_transaction_over_the_chain() {
    let backend = slow_mem();
    split_collection(&backend).await;
    let (policy, windows) = Recording::new(|_: &TopologyWindow| Vec::new());
    let db = open(&backend, leaves_of_at_most(4), policy).await;
    let coll = open_top(&db, b"split").await;
    let pairs = scan_crossings(&db, &coll).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    windows.lock().unwrap().clear();

    let coll_ref = &coll;
    db.tx(|tx| async move {
        for key in 0u8..16 {
            tx.write(coll_ref, &[key], &write_int(1))?;
        }
        Ok(())
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    db.shutdown().await;
    let windows = windows.lock().unwrap();
    let misses: Vec<_> = windows
        .iter()
        .flat_map(|window| &window.pairs)
        .filter(|pair| pair.avoidable.adjacent_miss > Duration::ZERO)
        .collect();
    assert_eq!(misses.len(), usize::try_from(pairs).unwrap());
    assert!(
        misses
            .iter()
            .all(|pair| pair.avoidable.adjacent_miss == misses[0].avoidable.adjacent_miss)
    );
}

#[tokio::test(start_paused = true)]
async fn the_root_leaf_does_not_merge() {
    let backend = slow_mem();
    let (policy, _) = Recording::new(|window: &TopologyWindow| {
        window
            .leaves
            .keys()
            .map(|id| TopologyChange::Merge(id.clone()))
            .collect()
    });
    let db = open(&backend, NodeSizePolicy::default(), policy).await;
    let coll = create_top(&db, b"root").await;

    write_keys(&coll, 4).await;
    tokio::time::sleep(Duration::from_secs(3)).await;

    assert_eq!(db.stats().restructurer.merges, 0);
    assert_eq!(
        coll.scan_keys(KeyScan::all()).await.unwrap().keys().len(),
        4
    );
    db.shutdown().await;
}

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

// Opens two database instances with the avoidable time policy over one leaf
// of 8 keys, and writes each key 200 times from the instance that `owner`
// gives.
async fn contend(owner: Owner) -> Contention {
    let backend = slow_mem();
    let policy = AvoidableTimePolicy::new;
    let first = open(&backend, NodeSizePolicy::default(), policy()).await;
    let second = open(&backend, NodeSizePolicy::default(), policy()).await;
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
    let reader = open(backend, NodeSizePolicy::default(), FixedTopology).await;
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
async fn the_avoidable_time_policy_splits_a_leaf_between_the_instances_that_contend_on_it() {
    let Contention {
        stats,
        middle_crossings,
    } = contend(HALVES).await;

    assert!(stats.avoidable.split.lost_cas > Duration::ZERO);
    assert!(stats.splits >= 1, "{stats:?}");
    assert_eq!(middle_crossings, 1);
}

// A split at the median leaves writers of both instances in each half, so the
// CASes that they lose are not avoidable time.
#[tokio::test(start_paused = true)]
async fn a_leaf_that_instances_write_interleaved_keys_of_does_not_split() {
    let Contention { stats, .. } = contend(INTERLEAVED).await;

    assert_eq!(stats.avoidable.split.lost_cas, Duration::ZERO);
    assert_eq!(stats.splits, 0, "{stats:?}");
}
