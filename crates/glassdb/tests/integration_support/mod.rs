use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use glassdb::backend::BackendError;
use glassdb::backend::memory::MemoryBackend;
use glassdb::backend::middleware::{BackendOp, HookBackend, HookFuture};
use glassdb::{Backend, Collection, CollectionPath, Database, Error, Transaction};
use glassdb_data::ObjectPath;
use glassdb_storage::transaction::TxCommitStatus;
use tokio::sync::{Notify, oneshot};

pub async fn init_db(backend: Arc<dyn Backend>) -> Database {
    Database::open("example", backend).await.unwrap()
}

pub async fn create_top(db: &Database, name: &[u8]) -> Collection {
    db.create_collection_if_absent(&CollectionPath::new(name).unwrap())
        .await
        .unwrap()
}

pub async fn open_top(db: &Database, name: &[u8]) -> Collection {
    db.open_collection(&CollectionPath::new(name).unwrap())
        .await
        .unwrap()
}

pub fn mem() -> Arc<dyn Backend> {
    Arc::new(MemoryBackend::new())
}

pub fn write_int(n: i64) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}

pub fn try_read_int(bytes: &[u8]) -> Option<i64> {
    Some(i64::from_le_bytes(bytes.try_into().ok()?))
}

pub fn read_int(bytes: &[u8]) -> i64 {
    try_read_int(bytes).expect("integer value has the wrong width")
}

pub async fn read_int_from_tx(
    tx: &Transaction,
    collection: &Collection,
    key: &[u8],
) -> Result<i64, Error> {
    match tx.read(collection, key).await {
        Ok(Some(value)) => try_read_int(&value).ok_or_else(|| {
            Error::internal(format!("key {key:?} has invalid integer value {value:?}"))
        }),
        // Treat a missing value as zero (i.e. initialize it).
        Ok(None) => Ok(0),
        Err(error) => Err(error),
    }
}

pub fn incremented_value(key: &[u8], current: i64, amount: i64) -> Result<Vec<u8>, Error> {
    current
        .checked_add(amount)
        .map(write_int)
        .ok_or_else(|| Error::internal(format!("integer overflow for key {key:?}")))
}

pub async fn rmw(
    db: &Database,
    collection: &Collection,
    key: &[u8],
    iterations: usize,
) -> Result<(), Error> {
    for _ in 0..iterations {
        db.tx(|tx| async move {
            let value = read_int_from_tx(&tx, collection, key).await?;
            tx.write(collection, key, &incremented_value(key, value, 1)?)
        })
        .await?;
    }
    Ok(())
}

pub async fn multiple_rmw(
    db: &Database,
    collection: &Collection,
    first_key: &[u8],
    second_key: &[u8],
    iterations: usize,
) -> Result<(), Error> {
    for _ in 0..iterations {
        db.tx(|tx| async move {
            let first = read_int_from_tx(&tx, collection, first_key).await?;
            tx.write(
                collection,
                first_key,
                &incremented_value(first_key, first, 1)?,
            )?;
            let second = read_int_from_tx(&tx, collection, second_key).await?;
            tx.write(
                collection,
                second_key,
                &incremented_value(second_key, second, 1)?,
            )
        })
        .await?;
    }
    Ok(())
}

pub async fn list_collections_of(collection: &Collection) -> Vec<Vec<u8>> {
    collection
        .iter_collections()
        .await
        .unwrap()
        .map(|entry| entry.name)
        .collect()
}

/// Controls hooks that pause writes at known points in the commit pipeline and
/// report when a leaf write has landed.
pub struct PauseControl {
    trap: Mutex<Option<Trap>>,
    wound_write_gate: Mutex<Option<WoundWriteGate>>,
    leaf_write_gate: Mutex<Option<LeafWriteGate>>,
}

struct Trap {
    path_contains: &'static str,
    arrived: oneshot::Sender<()>,
}

struct WoundWriteGate {
    arrived: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

struct LeafWriteGate {
    arrived: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

impl PauseControl {
    pub fn wrap(inner: Arc<dyn Backend>) -> (Arc<HookBackend>, Arc<Self>) {
        let control = Arc::new(Self {
            trap: Mutex::new(None),
            wound_write_gate: Mutex::new(None),
            leaf_write_gate: Mutex::new(None),
        });
        let backend = HookBackend::new(inner);
        backend.set_after({
            let control = control.clone();
            move |op, outcome| {
                let gate = match op {
                    BackendOp::WriteIf { path, .. }
                        if outcome.is_success() && is_leaf_path(path) =>
                    {
                        control.leaf_write_gate.lock().unwrap().take()
                    }
                    _ => None,
                };
                let future: HookFuture = Box::pin(async move {
                    if let Some(gate) = gate {
                        let _ = gate.arrived.send(());
                        let _ = gate.release.await;
                    }
                    Ok(())
                });
                future
            }
        });
        backend.set_before({
            let control = control.clone();
            move |op| {
                let (wound_gate, path) = match op {
                    BackendOp::WriteIfNotExists { path, value } => (
                        control.take_wound_write_gate(path, value),
                        Some((*path).to_owned()),
                    ),
                    _ => (None, None),
                };
                let control = control.clone();
                let future: HookFuture = Box::pin(async move {
                    if let Some(gate) = wound_gate {
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

    /// Arms a one-shot trap on the next matching `write_if_not_exists`.
    pub fn arm(&self, path_contains: &'static str) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        *self.trap.lock().unwrap() = Some(Trap {
            path_contains,
            arrived: tx,
        });
        rx
    }

    /// Parks the next successful coordination-leaf CAS after it lands.
    pub fn arm_leaf_write_gate(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (arrived_tx, arrived_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        *self.leaf_write_gate.lock().unwrap() = Some(LeafWriteGate {
            arrived: arrived_tx,
            release: release_rx,
        });
        (arrived_rx, release_tx)
    }

    pub fn arm_wound_write_gate(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (arrived_tx, arrived_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        *self.wound_write_gate.lock().unwrap() = Some(WoundWriteGate {
            arrived: arrived_tx,
            release: release_rx,
        });
        (arrived_rx, release_tx)
    }

    fn take_match(&self, path: &str) -> Option<oneshot::Sender<()>> {
        let mut trap = self.trap.lock().unwrap();
        if let Some(armed) = trap.as_ref()
            && path.contains(armed.path_contains)
        {
            return trap.take().map(|armed| armed.arrived);
        }
        None
    }

    fn take_wound_write_gate(&self, path: &str, value: &[u8]) -> Option<WoundWriteGate> {
        // With the tagless backend (ADR-023) the commit status is in the object
        // body, so decode it to recognize the pinned wound written for a
        // cancelled owner whose in-flight mutation did not acknowledge return.
        if !path.contains("/_t/") || !is_wounded_tx_log(value) {
            return None;
        }
        self.wound_write_gate.lock().unwrap().take()
    }
}

/// Coordinates the first parent-record CAS and records its path.
pub struct ParentWriteControl {
    backend: Arc<HookBackend>,
    inner: Arc<MemoryBackend>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    writes: Arc<AtomicUsize>,
    record: Arc<Mutex<Option<String>>>,
}

impl ParentWriteControl {
    pub fn new() -> Self {
        let inner = Arc::new(MemoryBackend::new());
        let backend = HookBackend::new(inner.clone());
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let writes = Arc::new(AtomicUsize::new(0));
        let record = Arc::new(Mutex::new(None::<String>));

        Self {
            backend,
            inner,
            entered,
            release,
            writes,
            record,
        }
    }

    pub fn arm(&self) {
        self.backend.set_before({
            let entered = self.entered.clone();
            let release = self.release.clone();
            let writes = self.writes.clone();
            let record = self.record.clone();
            move |op| {
                let parent_cas =
                    matches!(op, BackendOp::WriteIf { path, .. } if path.ends_with("/_i"));
                if let BackendOp::WriteIf { path, .. } = op
                    && parent_cas
                {
                    record
                        .lock()
                        .unwrap()
                        .get_or_insert_with(|| (*path).to_owned());
                }
                let block = parent_cas && writes.fetch_add(1, Ordering::SeqCst) == 0;
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
    }

    pub fn backend(&self) -> Arc<dyn Backend> {
        self.backend.clone()
    }

    pub fn inner(&self) -> &Arc<MemoryBackend> {
        &self.inner
    }

    pub async fn wait_until_entered(&self) {
        self.entered.notified().await;
    }

    pub fn release(&self) {
        self.release.notify_one();
    }

    pub fn writes(&self) -> usize {
        self.writes.load(Ordering::SeqCst)
    }

    pub fn recorded_path(&self) -> String {
        self.record
            .lock()
            .unwrap()
            .clone()
            .expect("parent CAS path was recorded")
    }
}

impl Default for ParentWriteControl {
    fn default() -> Self {
        Self::new()
    }
}

type LeafCasGate = (oneshot::Sender<()>, oneshot::Receiver<()>);

/// Parks one logless leaf CAS and counts abort-side transaction-log writes.
pub struct LoglessCommitControl {
    backend: Arc<HookBackend>,
    aborted_writes: Arc<AtomicUsize>,
    gate: Arc<Mutex<Option<LeafCasGate>>>,
}

impl LoglessCommitControl {
    pub fn wrap(inner: Arc<dyn Backend>) -> Self {
        let backend = HookBackend::new(inner);
        let aborted_writes = Arc::new(AtomicUsize::new(0));
        let gate: Arc<Mutex<Option<LeafCasGate>>> = Arc::new(Mutex::new(None));
        backend.set_before({
            let aborted_writes = aborted_writes.clone();
            let gate = gate.clone();
            move |op| {
                let mut parked = None;
                match op {
                    BackendOp::WriteIf { path, .. } if path.ends_with("/_r") => {
                        parked = gate.lock().unwrap().take();
                    }
                    BackendOp::WriteIf { path, value, .. }
                    | BackendOp::WriteIfNotExists { path, value }
                        if path.contains("/_t/") && is_abort_side_tx_log(value) =>
                    {
                        aborted_writes.fetch_add(1, Ordering::SeqCst);
                    }
                    _ => {}
                }
                let future: HookFuture = Box::pin(async move {
                    if let Some((arrived, released)) = parked {
                        let _ = arrived.send(());
                        let _ = released.await;
                    }
                    Ok(())
                });
                future
            }
        });

        Self {
            backend,
            aborted_writes,
            gate,
        }
    }

    pub fn backend(&self) -> Arc<HookBackend> {
        self.backend.clone()
    }

    pub fn arm(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (arrived_tx, arrived) = oneshot::channel();
        let (release, released) = oneshot::channel();
        *self.gate.lock().unwrap() = Some((arrived_tx, released));
        (arrived, release)
    }

    pub fn aborted_writes(&self) -> usize {
        self.aborted_writes.load(Ordering::SeqCst)
    }
}

/// Acknowledges preparation and durable recovery registration for one newly
/// created collection without imposing scheduler timing on the test.
pub struct PreparedCollectionRecoveryControl {
    backend: Arc<HookBackend>,
    armed: AtomicBool,
    prepared: Mutex<Option<oneshot::Sender<()>>>,
    retired: Mutex<Option<oneshot::Sender<usize>>>,
}

impl PreparedCollectionRecoveryControl {
    pub fn wrap(inner: Arc<dyn Backend>) -> Arc<Self> {
        let backend = HookBackend::new(inner);
        let control = Arc::new(Self {
            backend: backend.clone(),
            armed: AtomicBool::new(false),
            prepared: Mutex::new(None),
            retired: Mutex::new(None),
        });
        backend.set_after({
            let control = control.clone();
            move |operation, outcome| {
                let mut prepared = None;
                let mut retired = None;
                if control.armed.load(Ordering::SeqCst) && outcome.is_success() {
                    match operation {
                        BackendOp::WriteIfNotExists { path, .. }
                            if matches!(
                                ObjectPath::try_from(*path),
                                Ok(ObjectPath::CollectionRecord { .. })
                            ) =>
                        {
                            prepared = control.prepared.lock().unwrap().take();
                        }
                        BackendOp::WriteIf { path, value, .. }
                        | BackendOp::WriteIfNotExists { path, value }
                            if path.contains("/_t/")
                                && glassdb_storage::txobject::status(value)
                                    .is_ok_and(|status| status == TxCommitStatus::Aborted) =>
                        {
                            if let Ok(ObjectPath::Transaction { db_root, id }) =
                                ObjectPath::try_from(*path)
                                && let Ok(log) =
                                    glassdb_storage::txobject::decode(db_root.as_str(), &id, value)
                            {
                                control.armed.store(false, Ordering::SeqCst);
                                retired = control
                                    .retired
                                    .lock()
                                    .unwrap()
                                    .take()
                                    .map(|retired| (retired, log.prepared_collections.len()));
                            }
                        }
                        _ => {}
                    }
                }
                let future: HookFuture = Box::pin(async move {
                    if let Some(prepared) = prepared {
                        let _ = prepared.send(());
                    }
                    if let Some((retired, prepared_collections)) = retired {
                        let _ = retired.send(prepared_collections);
                    }
                    Ok(())
                });
                future
            }
        });
        control
    }

    pub fn backend(&self) -> Arc<HookBackend> {
        self.backend.clone()
    }

    pub fn arm(&self) -> (oneshot::Receiver<()>, oneshot::Receiver<usize>) {
        let (prepared, prepared_rx) = oneshot::channel();
        let (retired, retired_rx) = oneshot::channel();
        *self.prepared.lock().unwrap() = Some(prepared);
        *self.retired.lock().unwrap() = Some(retired);
        self.armed.store(true, Ordering::SeqCst);
        (prepared_rx, retired_rx)
    }
}

/// Fails one owner-side `Aborted` write and acknowledges the managed retry that
/// follows the failed synchronous transaction finalization.
pub struct RetirementFailureControl {
    backend: Arc<HookBackend>,
    armed: AtomicBool,
    failure_observed: AtomicBool,
    failed: Mutex<Option<oneshot::Sender<()>>>,
    recovered: Mutex<Option<oneshot::Sender<()>>>,
}

impl RetirementFailureControl {
    pub fn wrap(inner: Arc<dyn Backend>) -> Arc<Self> {
        let backend = HookBackend::new(inner);
        let control = Arc::new(Self {
            backend: backend.clone(),
            armed: AtomicBool::new(false),
            failure_observed: AtomicBool::new(false),
            failed: Mutex::new(None),
            recovered: Mutex::new(None),
        });
        backend.set_before({
            let control = control.clone();
            move |operation| {
                let fail = match operation {
                    BackendOp::WriteIf { value, .. }
                    | BackendOp::WriteIfNotExists { value, .. } => {
                        is_aborted_tx_log(value) && control.armed.swap(false, Ordering::SeqCst)
                    }
                    _ => false,
                };
                let failed = fail
                    .then(|| {
                        control.failure_observed.store(true, Ordering::SeqCst);
                        control.failed.lock().unwrap().take()
                    })
                    .flatten();
                let future: HookFuture = Box::pin(async move {
                    if let Some(failed) = failed {
                        let _ = failed.send(());
                    }
                    if fail {
                        return Err(BackendError::other(
                            "injected owner-retirement write failure",
                        ));
                    }
                    Ok(())
                });
                future
            }
        });
        backend.set_after({
            let control = control.clone();
            move |operation, outcome| {
                let recovered = (outcome.is_success()
                    && control.failure_observed.load(Ordering::SeqCst)
                    && match operation {
                        BackendOp::WriteIf { value, .. }
                        | BackendOp::WriteIfNotExists { value, .. } => is_aborted_tx_log(value),
                        _ => false,
                    })
                .then(|| control.recovered.lock().unwrap().take())
                .flatten();
                let future: HookFuture = Box::pin(async move {
                    if let Some(recovered) = recovered {
                        let _ = recovered.send(());
                    }
                    Ok(())
                });
                future
            }
        });
        control
    }

    pub fn backend(&self) -> Arc<HookBackend> {
        self.backend.clone()
    }

    pub fn observe(&self) -> (oneshot::Receiver<()>, oneshot::Receiver<()>) {
        let (failed, failed_rx) = oneshot::channel();
        let (recovered, recovered_rx) = oneshot::channel();
        self.failure_observed.store(false, Ordering::SeqCst);
        *self.failed.lock().unwrap() = Some(failed);
        *self.recovered.lock().unwrap() = Some(recovered);
        (failed_rx, recovered_rx)
    }

    pub fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
}

/// Reports whether `path` addresses a coordination leaf: a small collection's
/// root (`_r`) or a standalone node (`_n`).
fn is_leaf_path(path: &str) -> bool {
    path.ends_with("/_r") || path.contains("/_n/")
}

/// Reports whether `body` is an abort-side terminal transaction object.
fn is_abort_side_tx_log(body: &[u8]) -> bool {
    glassdb_storage::txobject::status(body)
        .map(|status| matches!(status, TxCommitStatus::Aborted | TxCommitStatus::Wounded))
        .unwrap_or(false)
}

fn is_aborted_tx_log(body: &[u8]) -> bool {
    glassdb_storage::txobject::status(body).is_ok_and(|status| status == TxCommitStatus::Aborted)
}

/// Reports whether `body` is a pinned transaction wound.
fn is_wounded_tx_log(body: &[u8]) -> bool {
    glassdb_storage::txobject::status(body)
        .map(|status| status == TxCommitStatus::Wounded)
        .unwrap_or(false)
}
