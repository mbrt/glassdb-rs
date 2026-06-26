//! End-to-end tests for the minimal v2 engine (ADR-016 … ADR-021) on the
//! in-memory backend. These exercise the object-storage-native layout directly:
//! shards as the lock table + MVCC index, the collection root for membership,
//! and unified transaction objects, with strict-serializable wound-wait.

use std::sync::Arc;
use std::time::Duration;

use glassdb_backend::memory::MemoryBackend;
use glassdb_backend::{Backend, Tags};
use glassdb_concurr::Clock;
use glassdb_data::shard::shard_index;
use glassdb_data::{TxId, paths};
use glassdb_storage::{
    CollectionRoot, LockType, Shard, ShardEntry, StorageError, TxCommitStatus, TxLog, txobject,
};
use glassdb_trans::TransError;
use glassdb_trans::v2::Collection;

fn backend() -> Arc<dyn Backend> {
    Arc::new(MemoryBackend::new())
}

async fn new_collection() -> Collection {
    Collection::create(backend(), "db/c").await.unwrap()
}

fn u64_bytes(n: u64) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}

fn parse_u64(v: Option<Vec<u8>>) -> u64 {
    v.map(|b| u64::from_le_bytes(b.try_into().unwrap()))
        .unwrap_or(0)
}

#[tokio::test]
async fn put_get_roundtrip() {
    let c = new_collection().await;
    assert_eq!(c.get(b"k").await.unwrap(), None);
    c.put(b"k", b"v".to_vec()).await.unwrap();
    assert_eq!(c.get(b"k").await.unwrap(), Some(b"v".to_vec()));
}

#[tokio::test]
async fn overwrite_existing_key() {
    let c = new_collection().await;
    c.put(b"k", b"v1".to_vec()).await.unwrap();
    c.put(b"k", b"v2".to_vec()).await.unwrap();
    assert_eq!(c.get(b"k").await.unwrap(), Some(b"v2".to_vec()));
}

#[tokio::test]
async fn delete_makes_key_absent() {
    let c = new_collection().await;
    c.put(b"k", b"v".to_vec()).await.unwrap();
    c.delete(b"k").await.unwrap();
    assert_eq!(c.get(b"k").await.unwrap(), None);
    // Deleting an absent key is a no-op.
    c.delete(b"missing").await.unwrap();
}

#[tokio::test]
async fn open_missing_collection_errors() {
    let err = Collection::open(backend(), "db/none").await.unwrap_err();
    // A missing collection surfaces as the root object being not found.
    assert!(matches!(err, TransError::Storage(StorageError::NotFound)));
}

#[tokio::test]
async fn multi_key_transaction_is_atomic() {
    let c = new_collection().await;
    c.transact(|tx| async move {
        tx.put(b"a", b"1".to_vec());
        tx.put(b"b", b"2".to_vec());
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(c.get(b"a").await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(c.get(b"b").await.unwrap(), Some(b"2".to_vec()));
}

#[tokio::test]
async fn read_your_writes_within_transaction() {
    let c = new_collection().await;
    let seen = c
        .transact(|tx| async move {
            tx.put(b"k", b"staged".to_vec());
            tx.get(b"k").await
        })
        .await
        .unwrap();
    assert_eq!(seen, Some(b"staged".to_vec()));
}

#[tokio::test]
async fn list_reflects_committed_keys() {
    let c = new_collection().await;
    assert!(c.list().await.unwrap().is_empty());
    c.put(b"a", b"1".to_vec()).await.unwrap();
    c.put(b"b", b"2".to_vec()).await.unwrap();
    c.put(b"c", b"3".to_vec()).await.unwrap();
    let mut keys = c.list().await.unwrap();
    keys.sort();
    assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);

    c.delete(b"b").await.unwrap();
    let mut keys = c.list().await.unwrap();
    keys.sort();
    assert_eq!(keys, vec![b"a".to_vec(), b"c".to_vec()]);
}

#[tokio::test]
async fn many_keys_roundtrip_across_shards() {
    let c = new_collection().await;
    for i in 0..50u64 {
        c.put(format!("key-{i}").as_bytes(), u64_bytes(i))
            .await
            .unwrap();
    }
    for i in 0..50u64 {
        assert_eq!(
            parse_u64(c.get(format!("key-{i}").as_bytes()).await.unwrap()),
            i
        );
    }
    assert_eq!(c.list().await.unwrap().len(), 50);
}

// Two distinct keys that hash to the *same* shard must both commit when written
// concurrently: shard-CAS contention is merge-on-retry, not a lock conflict
// (ADR-020).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_distinct_keys_same_shard_both_commit() {
    let c = new_collection().await;
    let base = b"k0";
    let target = shard_index(base);
    // Find a second key in the same shard.
    let mut other = None;
    for i in 1..100_000u64 {
        let cand = format!("k{i}");
        if shard_index(cand.as_bytes()) == target {
            other = Some(cand);
            break;
        }
    }
    let other = other.expect("a colliding key exists");
    assert_eq!(shard_index(other.as_bytes()), target);

    let c1 = c.clone();
    let c2 = c.clone();
    let other2 = other.clone();
    let h1 = tokio::spawn(async move { c1.put(b"k0", b"v0".to_vec()).await });
    let h2 = tokio::spawn(async move { c2.put(other2.as_bytes(), b"v1".to_vec()).await });
    h1.await.unwrap().unwrap();
    h2.await.unwrap().unwrap();

    assert_eq!(c.get(b"k0").await.unwrap(), Some(b"v0".to_vec()));
    assert_eq!(c.get(other.as_bytes()).await.unwrap(), Some(b"v1".to_vec()));
}

// The serializability stress test: many concurrent read-modify-write increments
// of one counter must total exactly the number of increments (no lost updates).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_counter_increments_are_serializable() {
    let c = new_collection().await;
    c.put(b"counter", u64_bytes(0)).await.unwrap();

    const N: u64 = 16;
    let mut handles = Vec::new();
    for _ in 0..N {
        let c = c.clone();
        handles.push(tokio::spawn(async move {
            c.transact(|tx| async move {
                let cur = parse_u64(tx.get(b"counter").await?);
                tx.put(b"counter", u64_bytes(cur + 1));
                Ok(())
            })
            .await
        }));
    }
    for h in handles {
        h.await.unwrap().unwrap();
    }
    assert_eq!(parse_u64(c.get(b"counter").await.unwrap()), N);
}

// Concurrent blind writers to the same key all commit (wound-wait guarantees
// progress); the final value is one of the writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_same_key_make_progress() {
    let c = new_collection().await;
    c.put(b"k", u64_bytes(0)).await.unwrap();

    const N: u64 = 8;
    let mut handles = Vec::new();
    for i in 1..=N {
        let c = c.clone();
        handles.push(tokio::spawn(async move { c.put(b"k", u64_bytes(i)).await }));
    }
    for h in handles {
        h.await.unwrap().unwrap();
    }
    let final_val = parse_u64(c.get(b"k").await.unwrap());
    assert!(
        (1..=N).contains(&final_val),
        "unexpected final value {final_val}"
    );
}

// A transaction spanning two keys in different shards observes a consistent
// snapshot: a concurrent transfer between them never lets the reader see money
// created or destroyed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_shard_transfer_preserves_invariant() {
    let c = new_collection().await;
    c.put(b"acct-a", u64_bytes(100)).await.unwrap();
    c.put(b"acct-b", u64_bytes(0)).await.unwrap();

    const ROUNDS: u64 = 20;
    let transferer = {
        let c = c.clone();
        tokio::spawn(async move {
            for _ in 0..ROUNDS {
                c.transact(|tx| async move {
                    let a = parse_u64(tx.get(b"acct-a").await?);
                    let b = parse_u64(tx.get(b"acct-b").await?);
                    if a > 0 {
                        tx.put(b"acct-a", u64_bytes(a - 1));
                        tx.put(b"acct-b", u64_bytes(b + 1));
                    }
                    Ok(())
                })
                .await
                .unwrap();
            }
        })
    };

    // Concurrent auditor: the total must always be exactly 100.
    let auditor = {
        let c = c.clone();
        tokio::spawn(async move {
            for _ in 0..ROUNDS {
                let total = c
                    .transact(|tx| async move {
                        let a = parse_u64(tx.get(b"acct-a").await?);
                        let b = parse_u64(tx.get(b"acct-b").await?);
                        Ok(a + b)
                    })
                    .await
                    .unwrap();
                assert_eq!(total, 100, "invariant violated: total={total}");
            }
        })
    };

    transferer.await.unwrap();
    auditor.await.unwrap();
    assert_eq!(parse_u64(c.get(b"acct-a").await.unwrap()), 80);
    assert_eq!(parse_u64(c.get(b"acct-b").await.unwrap()), 20);
}

// Many transactions each writing the *same two cross-shard keys* must all
// commit (liveness under the parallel-locking + serial-fallback machinery), and
// the result must be atomic across shards: both keys reflect the same winning
// transaction. This exercises the multi-shard lock path that the serial fallback
// (ADR-020) protects from livelock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_cross_shard_writers_stay_atomic() {
    let c = new_collection().await;
    let k1 = b"x".to_vec();
    let s1 = shard_index(&k1);
    // Find a second key that lands in a different shard.
    let mut k2 = None;
    for i in 0..100_000u64 {
        let cand = format!("y{i}");
        if shard_index(cand.as_bytes()) != s1 {
            k2 = Some(cand.into_bytes());
            break;
        }
    }
    let k2 = k2.expect("a cross-shard key exists");
    assert_ne!(shard_index(&k1), shard_index(&k2));

    const N: u64 = 12;
    let mut handles = Vec::new();
    for i in 1..=N {
        let c = c.clone();
        let k1 = k1.clone();
        let k2 = k2.clone();
        handles.push(tokio::spawn(async move {
            c.transact(move |tx| {
                let k1 = k1.clone();
                let k2 = k2.clone();
                async move {
                    tx.put(&k1, u64_bytes(i));
                    tx.put(&k2, u64_bytes(i));
                    Ok(())
                }
            })
            .await
        }));
    }
    // Every transaction must commit — no livelock/hang.
    for h in handles {
        h.await.unwrap().unwrap();
    }
    // Cross-shard atomicity: both keys carry the same winning transaction's value.
    let v1 = parse_u64(c.get(&k1).await.unwrap());
    let v2 = parse_u64(c.get(&k2).await.unwrap());
    assert_eq!(v1, v2, "cross-shard writes not atomic: {v1} != {v2}");
    assert!((1..=N).contains(&v1), "unexpected final value {v1}");
}

// Crash recovery (ADR-021): a transaction that installed locks (a shard entry +
// the collection-root membership lock) and then *crashed* before committing
// leaves a pending transaction object whose lease eventually expires. A younger
// writer — which loses the wound-wait priority race against the older crashed
// holder — must still reclaim the locks once the lease expires, rather than wait
// forever. Time is anchored to tokio's (paused) clock so expiry is deterministic.
#[tokio::test(start_paused = true)]
async fn expired_lease_is_reclaimed_after_crash() {
    let backend = backend();
    let clock = Clock::anchored();
    let coll = Collection::create_with_clock(backend.clone(), "db/c", clock.clone())
        .await
        .unwrap();
    let prefix = "db/c";

    // A crashed transaction: an *older* priority and a lease as of "now".
    let key = b"k".to_vec();
    let lease = clock.now();
    let dead = TxId::new_at(lease);

    // Its pending transaction object (the lease anchor).
    let dead_obj = TxLog {
        id: dead.clone(),
        timestamp: Some(lease),
        status: TxCommitStatus::Pending,
        writes: Vec::new(),
        locks: Vec::new(),
    };
    backend
        .write(
            &paths::from_transaction(prefix, &dead),
            txobject::encode(&dead_obj).unwrap(),
            Tags::new(),
        )
        .await
        .unwrap();

    // Its create-lock on the key's shard entry.
    let entry = ShardEntry {
        key: key.clone(),
        lock_type: LockType::Create,
        locked_by: vec![dead.clone()],
        current_writer: None,
        deleted: false,
    };
    backend
        .write(
            &paths::from_shard(prefix, shard_index(&key)),
            Shard::from_entries([entry]).encode(),
            Tags::new(),
        )
        .await
        .unwrap();

    // Its membership write-lock on the collection root.
    let root_path = paths::collection_info(prefix);
    let r = backend.read(&root_path).await.unwrap();
    let mut root = CollectionRoot::decode(&r.contents).unwrap();
    root.set_membership_lock(LockType::Write, [dead.clone()]);
    backend
        .write(&root_path, root.encode(), Tags::new())
        .await
        .unwrap();

    // While the lease is live, the pending writer's value is not effective, so
    // the key reads as absent — but the lock is held, so it cannot be reclaimed
    // by priority alone (the writer below is younger).
    assert_eq!(coll.get(&key).await.unwrap(), None);

    // Advance past the lease term (PENDING_TX_TIMEOUT + MAX_CLOCK_SKEW).
    tokio::time::advance(Duration::from_secs(60)).await;

    // The younger writer reclaims the expired holder's shard + root locks and
    // commits.
    coll.put(&key, b"v".to_vec()).await.unwrap();
    assert_eq!(coll.get(&key).await.unwrap(), Some(b"v".to_vec()));
}
