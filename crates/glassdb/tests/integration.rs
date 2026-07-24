//! Integration tests ported from the Go `glassdb_test.go` (memory-backend
//! subset). Time-sensitive paths use `tokio::time::pause` for determinism.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use glassdb::backend::memory::MemoryBackend;
use glassdb::backend::middleware::{BackendOp, HookBackend, HookFuture};
use glassdb::{
    Backend, Collection, CollectionPath, Database, Error, ProtocolTiming, SplitPolicy, Transaction,
};
use glassdb_storage::{CollectionRoot, TxCommitStatus};
use tokio::sync::{Barrier, Notify, oneshot};

async fn init_db(b: Arc<dyn Backend>) -> Database {
    Database::open("example", b).await.unwrap()
}

async fn create_top(db: &Database, name: &[u8]) -> Collection {
    db.create_collection_if_absent(&CollectionPath::new(name).unwrap())
        .await
        .unwrap()
}

async fn open_top(db: &Database, name: &[u8]) -> Collection {
    db.open_collection(&CollectionPath::new(name).unwrap())
        .await
        .unwrap()
}

fn mem() -> Arc<dyn Backend> {
    Arc::new(MemoryBackend::new())
}

fn write_int(n: i64) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}

fn read_int(b: &[u8]) -> i64 {
    let mut arr = [0u8; 8];
    arr.copy_from_slice(b);
    i64::from_le_bytes(arr)
}

async fn read_int_from_tx(tx: &Transaction, c: &Collection, k: &[u8]) -> Result<i64, Error> {
    match tx.read(c, k).await {
        Ok(Some(v)) => Ok(read_int(&v)),
        // Treat a missing value as zero (i.e. initialize it).
        Ok(None) => Ok(0),
        Err(e) => Err(e),
    }
}

async fn rmw(db: &Database, coll: &Collection, key: &[u8], iters: usize) -> Result<(), Error> {
    for _ in 0..iters {
        db.tx(|tx| async move {
            let num = read_int_from_tx(&tx, coll, key).await?;
            tx.write(coll, key, &write_int(num + 1))
        })
        .await?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn rw() {
    let db = init_db(mem()).await;
    let key = b"key1";
    let val = b"value1";

    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();

    coll.write(key, val).await.unwrap();
    let buf = coll.read(key).await.unwrap().unwrap();
    assert_eq!(buf, val);

    let stats = db.stats();
    assert_eq!(stats.tx_n, 2);
    assert_eq!(stats.tx_writes, 1);
    assert_eq!(stats.tx_retries, 0);
}

#[tokio::test]
async fn individually_oversized_key_is_invalid_input() {
    let policy = SplitPolicy {
        node_max_bytes: 256,
        split_headroom_bytes: 64,
        ..SplitPolicy::default()
    };
    let db = Database::builder("example", mem())
        .split_policy(policy)
        .open()
        .await
        .unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo")
        .await
        .unwrap();
    let before = db.stats();

    let err = coll.write(&[b'k'; 128], b"value").await.unwrap_err();
    assert!(matches!(err, Error::InvalidInput(_)), "got {err:?}");
    let delta = db.stats() - before;
    assert_eq!(
        delta.lock_calls, 0,
        "invalid keys are rejected before locking"
    );
}

// The distributed locker's counters are surfaced through `Database::stats()`
// (the same reset-on-read accumulation pattern as the backend object counters),
// not only through the internal diagnostics snapshot. A committed write
// transaction takes the locked commit path (a read-only commit does not), so it
// must bump `lock_calls` while a pure read leaves the counter unchanged.
#[tokio::test(start_paused = true)]
async fn stats_report_locker_activity() {
    let db = init_db(mem()).await;
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();

    let before = db.stats();
    coll.write(b"key1", b"value1").await.unwrap();
    let after_write = db.stats();
    assert!(
        after_write.lock_calls > before.lock_calls,
        "a committed write must report locker calls: {} -> {}",
        before.lock_calls,
        after_write.lock_calls
    );
    assert!(
        after_write.coord_submissions > before.coord_submissions,
        "a committed write must submit coordinator work: {} -> {}",
        before.coord_submissions,
        after_write.coord_submissions
    );
    assert!(
        after_write.coord_rounds > before.coord_rounds,
        "a committed write must start coordinator rounds: {} -> {}",
        before.coord_rounds,
        after_write.coord_rounds
    );
    assert!(
        after_write.coord_submissions >= after_write.coord_rounds,
        "one round cannot serve more work than was submitted"
    );

    // A read-only transaction commits via the lock-free fast path, so the
    // counter is unchanged across it.
    let _ = coll.read(b"key1").await.unwrap();
    let after_read = db.stats();
    assert_eq!(
        after_read.lock_calls, after_write.lock_calls,
        "a read-only commit takes no locks"
    );

    db.shutdown().await;
    let drained = db.stats();
    assert!(drained.coord_submissions >= after_write.coord_submissions);
    assert_eq!(db.stats(), drained, "stats snapshots remain cumulative");
}

#[tokio::test(start_paused = true)]
async fn stats_report_transactional_decoded_cache_hits() {
    let backend = mem();
    let writer_db = init_db(backend.clone()).await;
    let reader_db = init_db(backend).await;
    let key = b"key";
    let value = b"value";

    let writer = create_top(&writer_db, b"cache-stats").await;
    let reader = open_top(&reader_db, b"cache-stats").await;
    writer.write(key, value).await.unwrap();

    let before_cold = reader_db.stats();
    assert_eq!(reader.read(key).await.unwrap().unwrap(), value);
    let cold = reader_db.stats() - before_cold;
    assert_eq!(cold.tx_reads, 1);
    assert_eq!(cold.tx_cache_hits, 0);

    let before_warm = reader_db.stats();
    let reader_ref = &reader;
    reader_db
        .tx(|tx| async move {
            assert_eq!(tx.read(reader_ref, key).await?.unwrap(), value);
            assert_eq!(tx.read(reader_ref, key).await?.unwrap(), value);
            Ok(())
        })
        .await
        .unwrap();
    let warm = reader_db.stats() - before_warm;
    assert_eq!(warm.tx_reads, 1);
    assert_eq!(warm.tx_cache_hits, 1);

    let before_stale = reader_db.stats();
    assert_eq!(
        reader
            .read_stale(key, std::time::Duration::MAX)
            .await
            .unwrap()
            .unwrap(),
        value
    );
    let stale = reader_db.stats() - before_stale;
    assert_eq!(stale.tx_reads, 0);
    assert_eq!(stale.tx_cache_hits, 0);

    reader.delete(key).await.unwrap();
    let before_deleted = reader_db.stats();
    assert!(reader.read(key).await.unwrap().is_none());
    let deleted = reader_db.stats() - before_deleted;
    assert_eq!(deleted.tx_reads, 1);
    assert_eq!(deleted.tx_cache_hits, 1);
}

#[tokio::test(start_paused = true)]
async fn delete() {
    let db = init_db(mem()).await;
    let key = b"key1";
    let val = b"value1";

    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();

    coll.write(key, val).await.unwrap();
    coll.delete(key).await.unwrap();

    assert!(coll.read(key).await.unwrap().is_none());
    assert!(
        coll.read_stale(key, std::time::Duration::MAX)
            .await
            .unwrap()
            .is_none()
    );

    let stats = db.stats();
    assert_eq!(stats.tx_n, 3);
    assert_eq!(stats.tx_writes, 2);
    assert!(stats.tx_retries <= 1);
}

/// Regression: reading a found key and deleting that same key in one
/// transaction must commit. Such a transaction is shaped like a single
/// read-write, but the logless fast path cannot perform a delete (it would
/// issue a conditional delete while holding no lock, which the storage locker
/// rejects). Deletes must therefore route through the locked commit path. This
/// failed with an internal error before deletes were excluded from the fast
/// path.
#[tokio::test(start_paused = true)]
async fn read_then_delete_single_tx() {
    let db = init_db(mem()).await;
    let key = b"key1";
    let val = b"value1";

    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();
    coll.write(key, val).await.unwrap();

    // Read the existing value, then delete the same key, in one transaction.
    let coll = &coll;
    let prev = db
        .tx(|tx| async move {
            let v = tx.read(coll, key).await?.ok_or(Error::NotFound)?;
            tx.delete(coll, key)?;
            Ok(v)
        })
        .await
        .expect("a single read-then-delete transaction must commit");
    assert_eq!(prev, val);

    assert!(coll.read(key).await.unwrap().is_none());
}

#[tokio::test(start_paused = true)]
async fn read_from_another() {
    let b = mem();
    let db1 = init_db(b.clone()).await;
    let db2 = init_db(b).await;

    let coll = b"rw-another";
    let key = b"key1";
    let val = b"value1";

    let db1coll = create_top(&db1, coll).await;
    db1coll.write(key, val).await.unwrap();

    let buf = open_top(&db2, coll).await.read(key).await.unwrap().unwrap();
    assert_eq!(buf, val);
}

#[tokio::test(start_paused = true)]
async fn read_deleted_from_another() {
    let b = mem();
    let db1 = init_db(b.clone()).await;
    let db2 = init_db(b).await;

    let coll = b"rw-delete-another";
    let key1 = b"key1";
    let key2 = b"key2";
    let val = b"value1";
    let newval = b"value1-modified";

    let db1coll = db1
        .root_collection()
        .create_collection_if_absent(coll)
        .await
        .unwrap();
    let db1coll = &db1coll;
    db1.tx(|tx| async move {
        tx.write(db1coll, key1, val)?;
        tx.write(db1coll, key2, val)
    })
    .await
    .unwrap();

    let db2coll = &open_top(&db2, coll).await;
    db2.tx(|tx| async move {
        tx.write(db2coll, key1, newval)?;
        tx.delete(db2coll, key2)
    })
    .await
    .unwrap();

    let (key1_read, key2_found) = db1
        .tx(|tx| async move {
            let k1 = tx.read(db1coll, key1).await?.ok_or(Error::NotFound)?;
            let found = tx.read(db1coll, key2).await?.is_some();
            Ok((k1, found))
        })
        .await
        .unwrap();

    assert_eq!(key1_read, newval);
    assert!(!key2_found);
}

#[tokio::test(start_paused = true)]
async fn rmw_single() {
    let db = init_db(mem()).await;
    let key = b"key";

    let coll = create_top(&db, b"rmw-c").await;
    rmw(&db, &coll, key, 30).await.unwrap();

    let stats = db.stats();
    assert_eq!(stats.tx_n, 30);
    assert_eq!(stats.tx_reads, 30);
    assert_eq!(stats.tx_writes, 30);
    assert_eq!(stats.tx_retries, 0);

    let val = coll.read(key).await.unwrap().unwrap();
    assert_eq!(read_int(&val), 30);
}

async fn multiple_rmw(
    db: &Database,
    coll: &Collection,
    key1: &[u8],
    key2: &[u8],
    iters: usize,
) -> Result<(), Error> {
    for _ in 0..iters {
        db.tx(|tx| async move {
            let n1 = read_int_from_tx(&tx, coll, key1).await?;
            tx.write(coll, key1, &write_int(n1 + 1))?;
            let n2 = read_int_from_tx(&tx, coll, key2).await?;
            tx.write(coll, key2, &write_int(n2 + 1))
        })
        .await?;
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn concurrent_rmw() {
    let b = mem();
    let db1 = init_db(b.clone()).await;
    let db2 = init_db(b).await;
    let coll_name = b"rmw-c";
    let key = b"key";

    let coll1 = create_top(&db1, coll_name).await;
    let coll2 = open_top(&db2, coll_name).await;
    let (r1, r2) = tokio::join!(rmw(&db1, &coll1, key, 30), rmw(&db2, &coll2, key, 30),);
    r1.unwrap();
    r2.unwrap();

    let val = coll2.read(key).await.unwrap().unwrap();
    assert_eq!(read_int(&val), 60);
}

#[tokio::test(start_paused = true)]
async fn multiple_rmw_single() {
    let db = init_db(mem()).await;
    let key1 = b"key1";
    let key2 = b"key2";

    let coll = create_top(&db, b"multiple-rmw-c").await;
    multiple_rmw(&db, &coll, key1, key2, 30).await.unwrap();

    let val = coll.read(key1).await.unwrap().unwrap();
    assert_eq!(read_int(&val), 30);

    let stats = db.stats();
    assert_eq!(stats.tx_n, 31);
    assert_eq!(stats.tx_retries, 0);
}

#[tokio::test(start_paused = true)]
async fn concurrent_multiple_rmw() {
    let b = mem();
    let db1 = init_db(b.clone()).await;
    let db2 = init_db(b).await;
    let coll_name = b"rmw-c";
    let key1 = b"key1";
    let key2 = b"key2";

    let coll1 = create_top(&db1, coll_name).await;
    let coll2 = open_top(&db2, coll_name).await;
    let (r1, r2) = tokio::join!(
        multiple_rmw(&db1, &coll1, key1, key2, 30),
        multiple_rmw(&db2, &coll2, key1, key2, 30),
    );
    r1.unwrap();
    r2.unwrap();

    let val = coll2.read(key1).await.unwrap().unwrap();
    assert_eq!(read_int(&val), 60);
    let val = coll2.read(key2).await.unwrap().unwrap();
    assert_eq!(read_int(&val), 60);
}

// Reads many keys concurrently within a single transaction (the parallelism
// `read_multi` used to provide), now via `join_all` over `Transaction::read`.
#[tokio::test(start_paused = true)]
async fn concurrent_reads() {
    use futures::future::join_all;

    let db = init_db(mem()).await;
    let coll = &create_top(&db, b"demo-coll").await;

    let keys: Vec<Vec<u8>> = (0..15).map(|i| format!("key{i}").into_bytes()).collect();
    let keys = &keys;

    // Initialize the values.
    db.tx(|tx| async move {
        for k in keys {
            tx.write(coll, k, &write_int(0))?;
        }
        Ok(())
    })
    .await
    .unwrap();

    // Read all (in parallel) and increment.
    for _ in 0..30 {
        db.tx(|tx| async move {
            let vals = join_all(keys.iter().map(|k| tx.read(coll, k))).await;
            for (k, r) in keys.iter().zip(vals) {
                let cur = read_int(&r?.ok_or(Error::NotFound)?);
                tx.write(coll, k, &write_int(cur + 1))?;
            }
            Ok(())
        })
        .await
        .unwrap();
    }

    let stats = db.stats();
    assert_eq!(stats.tx_n, 31);
    assert_eq!(stats.tx_retries, 0);

    for k in keys {
        let b = coll.read(k).await.unwrap().unwrap();
        assert_eq!(read_int(&b), 30);
    }
}

#[tokio::test(start_paused = true)]
async fn read_stale() {
    use std::time::Duration;

    let db = init_db(mem()).await;
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();
    let key = b"key";

    let staleness = Duration::from_millis(300);
    let sleep_time = Duration::from_millis(100);
    let max_behind = (staleness.as_millis() / sleep_time.as_millis()) as i64 + 1;

    let coll = &coll;
    for i in 0..30i64 {
        // Increment the value. The read avoids making this a blind write.
        db.tx(|tx| async move {
            let _ = read_int_from_tx(&tx, coll, key).await?;
            tx.write(coll, key, &write_int(i))
        })
        .await
        .unwrap();

        let val = coll.read_stale(key, staleness).await.unwrap().unwrap();
        let read_num = read_int(&val);
        assert!(read_num <= i, "weak read {read_num} should be <= {i}");
        if i >= max_behind {
            assert!(read_num >= i - max_behind);
        }

        tokio::time::sleep(sleep_time).await;
    }

    let stats = db.stats();
    assert_eq!(stats.tx_n, 30);
    assert_eq!(stats.tx_retries, 0);
}

// `Database::diagnostics` smoke test: on a fresh Database the snapshot is empty, and after
// running a transaction that acquires locks the snapshot exposes the
// post-commit state in a structured form that callers can render via the
// `Display` impl. (Locks linger briefly while the background cleanup task
// releases them; deeper unit tests in `glassdb-trans` assert the
// per-tx-held-locks shape directly.)
#[tokio::test(start_paused = true)]
async fn diagnostics_returns_typed_snapshot() {
    let db = init_db(mem()).await;

    // A fresh Database has no coordination state.
    let idle = db.diagnostics();
    assert!(idle.coordinator_dedup.is_empty(), "fresh dedup: {idle:?}");
    assert!(idle.transactions.is_empty(), "fresh tx locks: {idle:?}");

    // After running a transaction, the snapshot is still callable and renders
    // through the Display impl; the schema (typed fields) is the contract we
    // care about here.
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();
    let coll_ref = &coll;
    db.tx(|tx| async move {
        tx.write(coll_ref, b"k1", b"v1")?;
        Ok(())
    })
    .await
    .unwrap();

    let diag = db.diagnostics();
    let rendered = format!("{diag}");
    assert!(
        rendered.starts_with("Diagnostics:"),
        "unexpected dump: {rendered}",
    );
}

// A committed read-write transaction returns before its write-back runs (it is
// spawned in the background), but a graceful shutdown drains that spawned task,
// so afterwards no transaction still holds locks — the write-back published its
// pointers and released them.
#[tokio::test(start_paused = true)]
async fn shutdown_drains_background_write_back() {
    let db = init_db(mem()).await;
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();

    let coll_ref = &coll;
    db.tx(|tx| async move {
        tx.write(coll_ref, b"k1", b"v1")?;
        Ok(())
    })
    .await
    .unwrap();

    db.shutdown().await;

    let diag = db.diagnostics();
    assert!(
        diag.transactions.is_empty(),
        "shutdown should drain the background write-back and release locks: {diag:?}",
    );
}

#[tokio::test(start_paused = true)]
async fn shutdown_rejects_every_public_async_entry_point() {
    let db = init_db(mem()).await;
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();

    db.shutdown().await;

    assert!(matches!(coll.read(b"key").await, Err(Error::ShuttingDown)));
    assert!(matches!(
        coll.read_stale(b"key", std::time::Duration::ZERO).await,
        Err(Error::ShuttingDown)
    ));
    assert!(matches!(
        coll.create_collection(b"child").await,
        Err(Error::ShuttingDown)
    ));
    assert!(matches!(coll.collections().await, Err(Error::ShuttingDown)));
    assert!(matches!(
        db.tx(|_| async { Ok::<(), Error>(()) }).await,
        Err(Error::ShuttingDown)
    ));
}

#[tokio::test(start_paused = true)]
async fn list_keys() {
    let db = init_db(mem()).await;
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();

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

    let got: Vec<Vec<u8>> = coll
        .keys()
        .await
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(got.len(), keys.len());
    let got_set: std::collections::HashSet<Vec<u8>> = got.iter().cloned().collect();
    for k in &keys {
        assert!(got_set.contains(k), "missing key {k:?}");
    }
    // The listing must be sorted.
    let mut sorted = got.clone();
    sorted.sort();
    assert_eq!(got, sorted);

    // Listing descends the B-link tree and scans its leaves via reads (ADR-031),
    // never a directory `list` of an object prefix.
    let stats = db.stats();
    assert_eq!(stats.obj_lists, 0);
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
        assert_eq!(first.keys(), &[b"a".to_vec(), b"b".to_vec()]);

        tx.write(coll, b"d", b"new")?;
        let second = tx.scan_keys(coll, scan).await?;
        assert_eq!(
            second.keys(),
            &[b"a".to_vec(), b"b".to_vec(), b"d".to_vec()]
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
    assert!(db.stats().tx_retries >= 1);
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
    let listed: Vec<Vec<u8>> = coll
        .keys()
        .await
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let mut sorted = listed.clone();
    sorted.sort();
    assert_eq!(listed, sorted, "listing is sorted");
    assert_eq!(listed, seed, "listing observes exactly the committed keys");

    // Create a new key, then list again: the fresh listing includes it (its own
    // consistent snapshot), demonstrating the scan re-resolves membership rather
    // than caching a stale set.
    let extra = 999u32.to_be_bytes().to_vec();
    coll.write(&extra, b"v").await.unwrap();
    let listed2: Vec<Vec<u8>> = coll
        .keys()
        .await
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
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
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let (backend, pause) = PauseControl::wrap(mem);
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
    // mid-commit. The `TxAbortGuard` asynchronously marks it aborted: the ghost
    // keys were "added" by a transaction that never committed.
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
    let listed: Vec<Vec<u8>> = coll
        .keys()
        .await
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
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
        .collections()
        .await
        .unwrap()
        .map(|entry| entry.map(|entry| entry.name))
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(got.len(), colls.len());
    let got_set: std::collections::HashSet<Vec<u8>> = got.iter().cloned().collect();
    for c in &colls {
        assert!(got_set.contains(c), "missing collection {c:?}");
    }
    let mut sorted = got.clone();
    sorted.sort();
    assert_eq!(got, sorted);
}

async fn list_collections_of(coll: &Collection) -> Vec<Vec<u8>> {
    coll.collections()
        .await
        .unwrap()
        .map(|entry| entry.map(|entry| entry.name))
        .collect::<Result<_, _>>()
        .unwrap()
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
    let mem = Arc::new(MemoryBackend::new());
    let backend = HookBackend::new(mem.clone());
    let db = init_db(backend.clone() as Arc<dyn Backend>).await;
    let parent = create_top(&db, b"parent").await;

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let parent_writes = Arc::new(AtomicUsize::new(0));
    let parent_root = Arc::new(Mutex::new(None::<String>));
    backend.set_before({
        let parent_root = parent_root.clone();
        let entered = entered.clone();
        let release = release.clone();
        let parent_writes = parent_writes.clone();
        move |op| {
            let parent_cas = matches!(op, BackendOp::WriteIf { path, .. } if path.ends_with("/_i"));
            if let BackendOp::WriteIf { path, .. } = op
                && parent_cas
            {
                parent_root
                    .lock()
                    .unwrap()
                    .get_or_insert_with(|| (*path).to_owned());
            }
            let block = parent_cas && parent_writes.fetch_add(1, Ordering::SeqCst) == 0;
            let entered = entered.clone();
            let release = release.clone();
            let future: HookFuture = Box::pin(async move {
                if block {
                    entered.notify_one();
                    release.notified().await;
                }
                Ok(())
            });
            future
        }
    });

    let left_parent = parent.clone();
    let right_parent = parent.clone();
    let left_create = tokio::spawn(async move { left_parent.create_collection(b"left").await });
    let right_create = tokio::spawn(async move { right_parent.create_collection(b"right").await });

    entered.notified().await;
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        parent_writes.load(Ordering::SeqCst),
        1,
        "only one same-path backend CAS may be active"
    );
    release.notify_one();
    left_create.await.unwrap().unwrap();
    right_create.await.unwrap().unwrap();

    assert!(parent_writes.load(Ordering::SeqCst) >= 2);
    assert_eq!(
        list_collections_of(&parent).await,
        vec![b"left".to_vec(), b"right".to_vec()]
    );
    let parent_root = parent_root
        .lock()
        .unwrap()
        .clone()
        .expect("parent CAS path was recorded");
    let stored = mem.read(&parent_root).await.unwrap();
    let root = CollectionRoot::decode(&stored.contents).unwrap();
    assert!(
        root.node().structural_gate().holders().is_empty(),
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

#[tokio::test(start_paused = true)]
async fn builder_custom_options() {
    use std::time::Duration;

    let db = Database::builder("example", mem())
        .cache_size(8 * 1024 * 1024)
        .retry_initial_interval(Duration::from_millis(10))
        .retry_max_interval(Duration::from_millis(100))
        .protocol_timing(ProtocolTiming::new(
            Duration::from_secs(1),
            Duration::from_secs(2),
        ))
        .open()
        .await
        .unwrap();

    let coll = db
        .root_collection()
        .create_collection_if_absent(b"demo-coll")
        .await
        .unwrap();
    coll.write(b"key1", b"value1").await.unwrap();
    let buf = coll.read(b"key1").await.unwrap().unwrap();
    assert_eq!(buf, b"value1");
}

/// Dropping a `Database::tx` future mid-flight (e.g. via `tokio::time::timeout`)
/// must not corrupt anything and must not leave the database unusable. The
/// next transaction observes the committed state (or the absence of one) and
/// completes promptly, exercising the `TxAbortGuard` cleanup hook end-to-end.
#[tokio::test(start_paused = true)]
async fn cancelled_tx_future_does_not_block_followups() {
    use std::time::Duration;

    let db = init_db(mem()).await;
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"c")
        .await
        .unwrap();
    coll.write(b"k", &write_int(1)).await.unwrap();

    let coll_ref = &coll;
    // The closure stages a write and then blocks forever; the outer timeout
    // drops the entire `Database::tx` future. With the `Tx`-drop safety net in
    // place this releases all attached state synchronously and schedules an
    // async abort of whatever engine-side tx may have been registered.
    let r = tokio::time::timeout(Duration::from_millis(50), async {
        db.tx(|tx| async move {
            let _ = read_int_from_tx(&tx, coll_ref, b"k").await?;
            tx.write(coll_ref, b"k", &write_int(99))?;
            std::future::pending::<()>().await;
            Ok(())
        })
        .await
    })
    .await;
    assert!(r.is_err(), "expected timeout, got {r:?}");

    // The cancelled tx never committed, so the original value still wins.
    let val = coll.read(b"k").await.unwrap().unwrap();
    assert_eq!(read_int(&val), 1);

    // A normal RMW still runs and commits without contention.
    rmw(&db, &coll, b"k", 1).await.unwrap();
    let val = coll.read(b"k").await.unwrap().unwrap();
    assert_eq!(read_int(&val), 2);
}

/// Controls hooks that pause writes at known points in the commit pipeline.
struct PauseControl {
    trap: Mutex<Option<Trap>>,
    abort_write_gate: Mutex<Option<AbortWriteGate>>,
}

struct Trap {
    path_contains: &'static str,
    arrived: oneshot::Sender<()>,
}

struct AbortWriteGate {
    arrived: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

impl PauseControl {
    fn wrap(inner: Arc<dyn Backend>) -> (Arc<HookBackend>, Arc<Self>) {
        let control = Arc::new(Self {
            trap: Mutex::new(None),
            abort_write_gate: Mutex::new(None),
        });
        let backend = HookBackend::new(inner);
        backend.set_before({
            let control = control.clone();
            move |op| {
                let (abort_gate, path) = match op {
                    BackendOp::WriteIfNotExists { path, value } => (
                        control.take_abort_write_gate(path, value),
                        Some((*path).to_owned()),
                    ),
                    _ => (None, None),
                };
                let control = control.clone();
                let future: HookFuture = Box::pin(async move {
                    if let Some(gate) = abort_gate {
                        let _ = gate.arrived.send(());
                        let _ = gate.release.await;
                    }
                    if let Some(arrived) = path.as_deref().and_then(|path| control.take_match(path))
                    {
                        let _ = arrived.send(());
                        std::future::pending::<()>().await;
                        unreachable!("pause should outlive any future that hits it");
                    }
                    Ok(())
                });
                future
            }
        });
        (backend, control)
    }

    /// Arms the (one-shot) trap. Returns a receiver that is fired when the
    /// next matching `write_if_not_exists` enters the wrapper and parks.
    fn arm(&self, path_contains: &'static str) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        *self.trap.lock().unwrap() = Some(Trap {
            path_contains,
            arrived: tx,
        });
        rx
    }

    fn arm_abort_write_gate(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (arrived_tx, arrived_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        *self.abort_write_gate.lock().unwrap() = Some(AbortWriteGate {
            arrived: arrived_tx,
            release: release_rx,
        });
        (arrived_rx, release_tx)
    }

    fn take_match(&self, path: &str) -> Option<oneshot::Sender<()>> {
        let mut t = self.trap.lock().unwrap();
        if let Some(trap) = t.as_ref()
            && path.contains(trap.path_contains)
        {
            return t.take().map(|trap| trap.arrived);
        }
        None
    }

    fn take_abort_write_gate(&self, path: &str, value: &[u8]) -> Option<AbortWriteGate> {
        // With the tagless backend (ADR-023) the commit status is in the object
        // body, so decode it to recognize an aborted transaction object.
        if !path.contains("/_t/") || !is_aborted_tx_log(value) {
            return None;
        }
        self.abort_write_gate.lock().unwrap().take()
    }
}

/// Reports whether `body` is a transaction object marked aborted.
fn is_aborted_tx_log(body: &[u8]) -> bool {
    glassdb_storage::txobject::status(body)
        .map(|status| status == TxCommitStatus::Aborted)
        .unwrap_or(false)
}

/// When a `Database::tx` future is dropped *during* its commit (after
/// `algo.begin` registered the engine-side transaction, before `algo.end`
/// ran), the `TxAbortGuard` must schedule an async abort so peer
/// transactions see the abort marker promptly instead of waiting for the
/// 15-second lock lease. We exercise the exact mid-commit cancel path by
/// trapping the first `write_if_not_exists` on a transaction-log path (the
/// commit-log write, which only happens once locks have been acquired) and
/// dropping the future from there.
#[tokio::test(start_paused = true)]
async fn cancelled_tx_during_commit_unblocks_peer_promptly() {
    use std::time::Duration;

    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let (backend, pause) = PauseControl::wrap(mem);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"c")
        .await
        .unwrap();
    coll.write(b"k1", &write_int(1)).await.unwrap();
    coll.write(b"k2", &write_int(2)).await.unwrap();

    // Trap the next commit-log write (`_t/<ss>/<txid>` path). Setup ops above
    // don't match and pass straight through.
    let arrived = pause.arm("/_t/");

    // Spawn a tx that writes two distinct keys, so it goes through the
    // standard locked commit path (the single-RW fast path requires 1
    // read + 1 write on the same key and would skip the tx-log write).
    let stalled = tokio::spawn({
        let db = db.clone();
        let coll = coll.clone();
        async move {
            let coll_ref = &coll;
            db.tx(|tx| async move {
                tx.write(coll_ref, b"k1", &write_int(42))?;
                tx.write(coll_ref, b"k2", &write_int(43))
            })
            .await
        }
    });

    // Wait until the spawned tx has reached the commit-log trap. From here
    // the engine-side tx is registered (`algo.begin` ran), the locks have
    // been acquired, but the commit log hasn't been written yet. This is
    // exactly the window the `TxAbortGuard` exists for.
    arrived.await.unwrap();

    // Drop the future. `TxAbortGuard::drop` fires here, calling
    // `Algo::async_abort` which spawns a background task that writes the
    // Aborted marker to the tx log via the (now-disarmed) backend.
    stalled.abort();
    let _ = stalled.await;

    // A peer transaction on the same keys must complete quickly. Without
    // the abort marker it would spin on the locks until the 15-second
    // lease expires; with it, the locker sees `Aborted` and overrides.
    let coll_ref = &coll;
    let r = tokio::time::timeout(
        Duration::from_secs(5),
        db.tx(|tx| async move {
            let n1 = read_int_from_tx(&tx, coll_ref, b"k1").await?;
            let n2 = read_int_from_tx(&tx, coll_ref, b"k2").await?;
            tx.write(coll_ref, b"k1", &write_int(n1 + 10))?;
            tx.write(coll_ref, b"k2", &write_int(n2 + 10))
        }),
    )
    .await;
    let r = r.expect("peer tx timed out: TransactionAbortGuard didn't release the lock promptly");
    r.unwrap();

    // The cancelled tx never committed (its values 42/43 are gone); the
    // peer's reads observed the original values and incremented from there.
    let v1 = coll.read(b"k1").await.unwrap().unwrap();
    assert_eq!(read_int(&v1), 11);
    let v2 = coll.read(b"k2").await.unwrap().unwrap();
    assert_eq!(read_int(&v2), 12);
}

/// Clean shutdown must wait for the async abort scheduled when a transaction
/// future is dropped between `algo.begin` and `algo.end`. This test parks that
/// abort-log write and verifies `Database::shutdown` remains pending until the write
/// is released.
#[tokio::test(start_paused = true)]
async fn shutdown_waits_for_cancelled_tx_async_abort() {
    use std::time::Duration;

    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let (backend, pause) = PauseControl::wrap(mem);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db
        .root_collection()
        .create_collection_if_absent(b"c")
        .await
        .unwrap();
    coll.write(b"k1", &write_int(1)).await.unwrap();
    coll.write(b"k2", &write_int(2)).await.unwrap();

    let commit_arrived = pause.arm("/_t/");
    let (abort_arrived, release_abort) = pause.arm_abort_write_gate();

    let stalled = tokio::spawn({
        let db = db.clone();
        let coll = coll.clone();
        async move {
            let coll_ref = &coll;
            db.tx(|tx| async move {
                tx.write(coll_ref, b"k1", &write_int(42))?;
                tx.write(coll_ref, b"k2", &write_int(43))
            })
            .await
        }
    });

    commit_arrived.await.unwrap();
    stalled.abort();
    let _ = stalled.await;

    let shutdown = tokio::spawn({
        let db = db.clone();
        async move {
            db.shutdown().await;
        }
    });

    tokio::time::timeout(Duration::from_secs(1), abort_arrived)
        .await
        .expect("async abort did not start during shutdown")
        .unwrap();

    for _ in 0..10 {
        tokio::task::yield_now().await;
        assert!(
            !shutdown.is_finished(),
            "shutdown returned before async abort completed"
        );
    }

    release_abort.send(()).unwrap();
    shutdown.await.unwrap();
}
