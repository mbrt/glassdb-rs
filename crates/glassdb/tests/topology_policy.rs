//! Leaf splits and merges that the size causes decide.

use std::time::Duration;

use glassdb::{Database, KeyScan, NodeSizePolicy, TopologyPolicy};

pub mod integration_support;

use integration_support::{create_top, mem, write_int};

// Splits 5 keys into two leaves, and deletes all keys but the first and the
// last, so that each leaf keeps exactly one live entry. The size causes merge
// leaves below the underfull threshold of the node sizes, even when they still
// have live entries.
#[tokio::test(start_paused = true)]
async fn the_size_causes_merge_leaves_below_the_underfull_threshold() {
    let sizes = NodeSizePolicy::builder()
        .leaf_max_entries(4)
        .leaf_min_entries(2)
        .index_min_children(0)
        .build()
        .unwrap();
    let db = Database::builder("example", mem())
        .node_size_policy(sizes)
        .topology_policy(TopologyPolicy::SizeCauses)
        .open()
        .await
        .unwrap();
    let coll = create_top(&db, b"sizes").await;
    for key in 0..5u8 {
        coll.write(&[key], &write_int(0)).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(db.stats().restructurer.splits, 1);

    for key in 1..4u8 {
        coll.delete(&[key]).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(10)).await;

    assert_eq!(db.stats().restructurer.merges, 1);
    let keys = coll.scan_keys(KeyScan::all()).await.unwrap();
    assert_eq!(keys.keys(), [vec![0], vec![4]]);
    db.shutdown().await;
}
