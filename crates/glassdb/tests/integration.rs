//! Integration tests ported from the Go `glassdb_test.go` (memory-backend
//! subset). Time-sensitive paths use `tokio::time::pause` for determinism.

use std::sync::Arc;

use glassdb::backend::memory::MemoryBackend;
use glassdb::{Backend, Collection, DB, Error, Tx};

async fn init_db(b: Arc<dyn Backend>) -> DB {
    DB::open("example", b).await.unwrap()
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

async fn read_int_from_tx(tx: &Tx, c: &Collection, k: &[u8]) -> Result<i64, Error> {
    match tx.read(c, k).await {
        Ok(v) => Ok(read_int(&v)),
        // Treat a missing value as zero (i.e. initialize it).
        Err(e) if e.is_not_found() => Ok(0),
        Err(e) => Err(e),
    }
}

async fn rmw(db: &DB, coll: &Collection, key: &[u8], iters: usize) -> Result<(), Error> {
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

    let coll = db.collection(b"demo-coll");
    coll.create().await.unwrap();

    coll.write(key, val).await.unwrap();
    let buf = coll.read_strong(key).await.unwrap();
    assert_eq!(buf, val);

    let stats = db.stats();
    assert_eq!(stats.tx_n, 2);
    assert_eq!(stats.tx_writes, 1);
    assert_eq!(stats.tx_retries, 0);
}

#[tokio::test(start_paused = true)]
async fn delete() {
    let db = init_db(mem()).await;
    let key = b"key1";
    let val = b"value1";

    let coll = db.collection(b"demo-coll");
    coll.create().await.unwrap();

    coll.write(key, val).await.unwrap();
    coll.delete(key).await.unwrap();

    let err = coll.read_strong(key).await.unwrap_err();
    assert!(err.is_not_found(), "expected not-found, got {err:?}");

    let stats = db.stats();
    assert_eq!(stats.tx_n, 3);
    assert_eq!(stats.tx_writes, 2);
    assert!(stats.tx_retries <= 1);
}

#[tokio::test(start_paused = true)]
async fn read_from_another() {
    let b = mem();
    let db1 = init_db(b.clone()).await;
    let db2 = init_db(b).await;

    let coll = b"rw-another";
    let key = b"key1";
    let val = b"value1";

    db1.collection(coll).create().await.unwrap();
    db1.collection(coll).write(key, val).await.unwrap();

    let buf = db2.collection(coll).read_strong(key).await.unwrap();
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

    let db1coll = db1.collection(coll);
    db1coll.create().await.unwrap();
    let db1coll = &db1coll;
    db1.tx(|tx| async move {
        tx.write(db1coll, key1, val)?;
        tx.write(db1coll, key2, val)
    })
    .await
    .unwrap();

    let db2coll = &db2.collection(coll);
    db2.tx(|tx| async move {
        tx.write(db2coll, key1, newval)?;
        tx.delete(db2coll, key2)
    })
    .await
    .unwrap();

    let (key1_read, key2_found) = db1
        .tx(|tx| async move {
            let k1 = tx.read(db1coll, key1).await?;
            let found = match tx.read(db1coll, key2).await {
                Ok(_) => true,
                Err(e) if e.is_not_found() => false,
                Err(e) => return Err(e),
            };
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
    let coll = db.collection(b"rmw-c");
    let key = b"key";

    coll.create().await.unwrap();
    rmw(&db, &coll, key, 30).await.unwrap();

    let stats = db.stats();
    assert_eq!(stats.tx_n, 30);
    assert_eq!(stats.tx_reads, 30);
    assert_eq!(stats.tx_writes, 30);
    assert_eq!(stats.tx_retries, 0);

    let val = coll.read_strong(key).await.unwrap();
    assert_eq!(read_int(&val), 30);
}

async fn multiple_rmw(
    db: &DB,
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

    db1.collection(coll_name).create().await.unwrap();

    let coll1 = db1.collection(coll_name);
    let coll2 = db2.collection(coll_name);
    let (r1, r2) = tokio::join!(rmw(&db1, &coll1, key, 30), rmw(&db2, &coll2, key, 30),);
    r1.unwrap();
    r2.unwrap();

    let val = db2.collection(coll_name).read_strong(key).await.unwrap();
    assert_eq!(read_int(&val), 60);
}

#[tokio::test(start_paused = true)]
async fn multiple_rmw_single() {
    let db = init_db(mem()).await;
    let coll = db.collection(b"multiple-rmw-c");
    let key1 = b"key1";
    let key2 = b"key2";

    coll.create().await.unwrap();
    multiple_rmw(&db, &coll, key1, key2, 30).await.unwrap();

    let val = coll.read_strong(key1).await.unwrap();
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

    db1.collection(coll_name).create().await.unwrap();

    let coll1 = db1.collection(coll_name);
    let coll2 = db2.collection(coll_name);
    let (r1, r2) = tokio::join!(
        multiple_rmw(&db1, &coll1, key1, key2, 30),
        multiple_rmw(&db2, &coll2, key1, key2, 30),
    );
    r1.unwrap();
    r2.unwrap();

    let val = db2.collection(coll_name).read_strong(key1).await.unwrap();
    assert_eq!(read_int(&val), 60);
    let val = db2.collection(coll_name).read_strong(key2).await.unwrap();
    assert_eq!(read_int(&val), 60);
}

// Reads many keys concurrently within a single transaction (the parallelism
// `read_multi` used to provide), now via `join_all` over `Tx::read`.
#[tokio::test(start_paused = true)]
async fn concurrent_reads() {
    use futures::future::join_all;

    let db = init_db(mem()).await;
    let coll = &db.collection(b"demo-coll");
    coll.create().await.unwrap();

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
                let cur = read_int(&r?);
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
        let b = coll.read_strong(k).await.unwrap();
        assert_eq!(read_int(&b), 30);
    }
}

#[tokio::test(start_paused = true)]
async fn read_weak() {
    use std::time::Duration;

    let db = init_db(mem()).await;
    let coll = db.collection(b"demo-coll");
    coll.create().await.unwrap();
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

        let val = coll.read_weak(key, staleness).await.unwrap();
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

// `DB::diagnostics` smoke test: on a fresh DB the snapshot is empty, and after
// running a transaction that acquires locks the snapshot exposes the
// post-commit state in a structured form that callers can render via the
// `Display` impl. (Locks linger briefly while the background cleanup task
// releases them; deeper unit tests in `glassdb-trans` assert the
// per-tx-held-locks shape directly.)
#[tokio::test(start_paused = true)]
async fn diagnostics_returns_typed_snapshot() {
    let db = init_db(mem()).await;

    // A fresh DB has no coordination state.
    let idle = db.diagnostics();
    assert!(idle.locker_dedup.is_empty(), "fresh dedup: {idle:?}");
    assert!(idle.transactions.is_empty(), "fresh tx locks: {idle:?}");

    // After running a transaction, the snapshot is still callable and renders
    // through the Display impl; the schema (typed fields) is the contract we
    // care about here.
    let coll = db.collection(b"demo-coll");
    coll.create().await.unwrap();
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

#[tokio::test(start_paused = true)]
async fn list_keys() {
    let db = init_db(mem()).await;
    let coll = db.collection(b"demo-coll");
    coll.create().await.unwrap();

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

    let mut iter = coll.keys().await.unwrap();
    let mut got: Vec<Vec<u8>> = Vec::new();
    for k in iter.by_ref() {
        got.push(k);
    }
    assert!(iter.err().is_none(), "iter error: {:?}", iter.err());

    assert_eq!(got.len(), keys.len());
    let got_set: std::collections::HashSet<Vec<u8>> = got.iter().cloned().collect();
    for k in &keys {
        assert!(got_set.contains(k), "missing key {k:?}");
    }
    // The listing must be sorted.
    let mut sorted = got.clone();
    sorted.sort();
    assert_eq!(got, sorted);

    let stats = db.stats();
    assert_eq!(stats.obj_lists, 1);
}

#[tokio::test(start_paused = true)]
async fn list_collections() {
    let db = init_db(mem()).await;
    let coll = db.collection(b"demo-coll");
    coll.create().await.unwrap();

    let colls: Vec<Vec<u8>> = (0u32..50).map(|i| i.to_be_bytes().to_vec()).collect();
    for c in &colls {
        coll.collection(c).create().await.unwrap();
    }

    let mut iter = coll.collections().await.unwrap();
    let mut got: Vec<Vec<u8>> = Vec::new();
    for c in iter.by_ref() {
        got.push(c);
    }
    assert!(iter.err().is_none(), "iter error: {:?}", iter.err());

    assert_eq!(got.len(), colls.len());
    let got_set: std::collections::HashSet<Vec<u8>> = got.iter().cloned().collect();
    for c in &colls {
        assert!(got_set.contains(c), "missing collection {c:?}");
    }
    let mut sorted = got.clone();
    sorted.sort();
    assert_eq!(got, sorted);
}

#[tokio::test(start_paused = true)]
async fn builder_custom_options() {
    use std::time::Duration;

    let db = DB::builder("example", mem())
        .cache_size(8 * 1024 * 1024)
        .retry_initial_interval(Duration::from_millis(10))
        .retry_max_interval(Duration::from_millis(100))
        .open()
        .await
        .unwrap();

    let coll = db.collection(b"demo-coll");
    coll.create().await.unwrap();
    coll.write(b"key1", b"value1").await.unwrap();
    let buf = coll.read_strong(b"key1").await.unwrap();
    assert_eq!(buf, b"value1");
}
