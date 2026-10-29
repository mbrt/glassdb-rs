//! Leaves that keep the keys that transactions commit together (ADR-084).

use std::sync::Arc;
use std::time::Duration;

use glassdb::{Backend, Collection, Database, KeyScan, NodeSizePolicy};

pub mod integration_support;

use integration_support::{create_top, mem, read_int, write_int};

// Leaves of at most 4 entries that never merge for being underfull, so that
// only write spans merge them.
fn leaves_of_at_most_four() -> NodeSizePolicy {
    NodeSizePolicy::builder()
        .leaf_max_entries(4)
        .leaf_min_entries(0)
        .index_min_children(0)
        .build()
        .unwrap()
}

async fn open(backend: &Arc<dyn Backend>) -> Database {
    Database::builder("example", backend.clone())
        .node_size_policy(leaves_of_at_most_four())
        .open()
        .await
        .unwrap()
}

// Increments the keys `keys` in one transaction.
async fn increment(db: &Database, coll: &Collection, keys: &[u8]) {
    db.tx(|tx| async move {
        for key in keys {
            let value = tx.read(coll, &[*key]).await?.map_or(0, |v| read_int(&v));
            tx.write(coll, &[*key], &write_int(value + 1))?;
        }
        Ok(())
    })
    .await
    .unwrap();
}

// The soft cap leaves the 16 keys in leaves of 4 keys, so a leaf boundary
// divides each pair of keys that the later transactions write together. The
// leaves of the pairs merge.
#[tokio::test(start_paused = true)]
async fn leaves_merge_for_the_keys_that_transactions_write_together() {
    let backend = mem();
    let db = open(&backend).await;
    let coll = create_top(&db, b"spans").await;
    for key in 0..16 {
        coll.write(&[key], &write_int(0)).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    let splits = db.stats().restructurer.splits;
    assert!(splits >= 3, "{splits}");

    let pairs = async |rounds: usize| {
        for _ in 0..rounds {
            for pair in [[3, 4], [7, 8], [11, 12]] {
                increment(&db, &coll, &pair).await;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    };
    pairs(20).await;
    let merged = db.stats().restructurer;
    assert!(merged.merges >= 1);

    // The merged leaves keep the pairs, so they neither split nor merge again.
    pairs(20).await;
    let later = db.stats().restructurer;
    assert_eq!((later.splits, later.merges), (merged.splits, merged.merges));
    let keys = coll.scan_keys(KeyScan::all()).await.unwrap();
    assert_eq!(keys.keys().len(), 16);
    db.shutdown().await;
}
