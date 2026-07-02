//! Integration tests ported from the Go `glassdb_test.go` (memory-backend
//! subset). Time-sensitive paths use `tokio::time::pause` for determinism.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use glassdb::backend::memory::MemoryBackend;
use glassdb::backend::{BackendError, ReadReply, Version};
use glassdb::{Backend, Collection, Database, Error, Transaction};
use glassdb_storage::TxCommitStatus;
use tokio::sync::oneshot;

async fn init_db(b: Arc<dyn Backend>) -> Database {
    Database::open("example", b).await.unwrap()
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
        Ok(v) => Ok(read_int(&v)),
        // Treat a missing value as zero (i.e. initialize it).
        Err(Error::NotFound) => Ok(0),
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

    let coll = db.collection(b"demo-coll");
    coll.create().await.unwrap();

    coll.write(key, val).await.unwrap();
    let buf = coll.read(key).await.unwrap();
    assert_eq!(buf, val);

    let stats = db.stats();
    assert_eq!(stats.tx_n, 2);
    assert_eq!(stats.tx_writes, 1);
    assert_eq!(stats.tx_retries, 0);
}

// The distributed locker's counters are surfaced through `Database::stats()`
// (the same reset-on-read accumulation pattern as the backend object counters),
// not only through the internal diagnostics snapshot. A committed write
// transaction takes the locked commit path (a read-only commit does not), so it
// must bump `lock_calls` while a pure read leaves the counter unchanged.
#[tokio::test(start_paused = true)]
async fn stats_report_locker_activity() {
    let db = init_db(mem()).await;
    let coll = db.collection(b"demo-coll");
    coll.create().await.unwrap();

    let before = db.stats();
    coll.write(b"key1", b"value1").await.unwrap();
    let after_write = db.stats();
    assert!(
        after_write.lock_calls > before.lock_calls,
        "a committed write must report locker calls: {} -> {}",
        before.lock_calls,
        after_write.lock_calls
    );

    // A read-only transaction commits via the lock-free fast path, so the
    // counter is unchanged across it.
    let _ = coll.read(b"key1").await.unwrap();
    let after_read = db.stats();
    assert_eq!(
        after_read.lock_calls, after_write.lock_calls,
        "a read-only commit takes no locks"
    );
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

    let err = coll.read(key).await.unwrap_err();
    assert!(
        matches!(err, Error::NotFound),
        "expected not-found, got {err:?}"
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

    let coll = db.collection(b"demo-coll");
    coll.create().await.unwrap();
    coll.write(key, val).await.unwrap();

    // Read the existing value, then delete the same key, in one transaction.
    let coll = &coll;
    let prev = db
        .tx(|tx| async move {
            let v = tx.read(coll, key).await?;
            tx.delete(coll, key)?;
            Ok(v)
        })
        .await
        .expect("a single read-then-delete transaction must commit");
    assert_eq!(prev, val);

    let err = coll.read(key).await.unwrap_err();
    assert!(
        matches!(err, Error::NotFound),
        "expected not-found after delete, got {err:?}"
    );
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

    let buf = db2.collection(coll).read(key).await.unwrap();
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
                Err(Error::NotFound) => false,
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

    let val = coll.read(key).await.unwrap();
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

    db1.collection(coll_name).create().await.unwrap();

    let coll1 = db1.collection(coll_name);
    let coll2 = db2.collection(coll_name);
    let (r1, r2) = tokio::join!(rmw(&db1, &coll1, key, 30), rmw(&db2, &coll2, key, 30),);
    r1.unwrap();
    r2.unwrap();

    let val = db2.collection(coll_name).read(key).await.unwrap();
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

    let val = coll.read(key1).await.unwrap();
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

    let val = db2.collection(coll_name).read(key1).await.unwrap();
    assert_eq!(read_int(&val), 60);
    let val = db2.collection(coll_name).read(key2).await.unwrap();
    assert_eq!(read_int(&val), 60);
}

// Reads many keys concurrently within a single transaction (the parallelism
// `read_multi` used to provide), now via `join_all` over `Transaction::read`.
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
        let b = coll.read(k).await.unwrap();
        assert_eq!(read_int(&b), 30);
    }
}

#[tokio::test(start_paused = true)]
async fn read_stale() {
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

        let val = coll.read_stale(key, staleness).await.unwrap();
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

// A committed read-write transaction returns before its write-back runs (it is
// spawned in the background), but a graceful shutdown drains that spawned task,
// so afterwards no transaction still holds locks — the write-back published its
// pointers and released them.
#[tokio::test(start_paused = true)]
async fn shutdown_drains_background_write_back() {
    let db = init_db(mem()).await;
    let coll = db.collection(b"demo-coll");
    coll.create().await.unwrap();

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

    let got: Vec<Vec<u8>> = coll
        .collections()
        .await
        .unwrap()
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

#[tokio::test(start_paused = true)]
async fn builder_custom_options() {
    use std::time::Duration;

    let db = Database::builder("example", mem())
        .cache_size(8 * 1024 * 1024)
        .retry_initial_interval(Duration::from_millis(10))
        .retry_max_interval(Duration::from_millis(100))
        .open()
        .await
        .unwrap();

    let coll = db.collection(b"demo-coll");
    coll.create().await.unwrap();
    coll.write(b"key1", b"value1").await.unwrap();
    let buf = coll.read(b"key1").await.unwrap();
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
    let coll = db.collection(b"c");
    coll.create().await.unwrap();
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
    let val = coll.read(b"k").await.unwrap();
    assert_eq!(read_int(&val), 1);

    // A normal RMW still runs and commits without contention.
    rmw(&db, &coll, b"k", 1).await.unwrap();
    let val = coll.read(b"k").await.unwrap();
    assert_eq!(read_int(&val), 2);
}

/// A [`Backend`] decorator that lets a test pause at a single, known point in
/// the commit pipeline. It arms a one-shot trap matched by path substring;
/// the first `write_if_not_exists` whose path matches the substring (1) signals
/// the test via a `oneshot::Sender`, then (2) parks forever on
/// `pending().await` so the surrounding future stays alive until the test
/// drops it.
struct PausingBackend {
    inner: Arc<dyn Backend>,
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

impl PausingBackend {
    fn new(inner: Arc<dyn Backend>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            trap: Mutex::new(None),
            abort_write_gate: Mutex::new(None),
        })
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
    glassdb_storage::txobject::decode(&glassdb_data::TxId::default(), body)
        .map(|l| l.status == TxCommitStatus::Aborted)
        .unwrap_or(false)
}

#[async_trait]
impl Backend for PausingBackend {
    async fn read_if_modified(
        &self,
        path: &str,
        expected: &Version,
    ) -> Result<ReadReply, BackendError> {
        self.inner.read_if_modified(path, expected).await
    }

    async fn read(&self, path: &str) -> Result<ReadReply, BackendError> {
        self.inner.read(path).await
    }

    async fn write(&self, path: &str, value: Vec<u8>) -> Result<Version, BackendError> {
        self.inner.write(path, value).await
    }

    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BackendError> {
        self.inner.write_if(path, value, expected).await
    }

    async fn write_if_not_exists(
        &self,
        path: &str,
        value: Vec<u8>,
    ) -> Result<Version, BackendError> {
        if let Some(gate) = self.take_abort_write_gate(path, &value) {
            let _ = gate.arrived.send(());
            let _ = gate.release.await;
        }
        if let Some(arrived) = self.take_match(path) {
            let _ = arrived.send(());
            std::future::pending::<()>().await;
            unreachable!("PausingBackend pause should outlive any future that hits it");
        }
        self.inner.write_if_not_exists(path, value).await
    }

    async fn delete(&self, path: &str) -> Result<(), BackendError> {
        self.inner.delete(path).await
    }

    async fn list(&self, dir_path: &str) -> Result<Vec<String>, BackendError> {
        self.inner.list(dir_path).await
    }
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
    let backend = PausingBackend::new(mem);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db.collection(b"c");
    coll.create().await.unwrap();
    coll.write(b"k1", &write_int(1)).await.unwrap();
    coll.write(b"k2", &write_int(2)).await.unwrap();

    // Trap the next commit-log write (`_t/<txid>` path). Setup ops above
    // don't match and pass straight through.
    let arrived = backend.arm("/_t/");

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
    let v1 = coll.read(b"k1").await.unwrap();
    assert_eq!(read_int(&v1), 11);
    let v2 = coll.read(b"k2").await.unwrap();
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
    let backend = PausingBackend::new(mem);
    let db = Database::open("example", backend.clone()).await.unwrap();
    let coll = db.collection(b"c");
    coll.create().await.unwrap();
    coll.write(b"k1", &write_int(1)).await.unwrap();
    coll.write(b"k2", &write_int(2)).await.unwrap();

    let commit_arrived = backend.arm("/_t/");
    let (abort_arrived, release_abort) = backend.arm_abort_write_gate();

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
