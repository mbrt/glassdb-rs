//! Active transaction state. Ported from the Go `tx.go`: staged writes and
//! tracked reads (to provide repeatable reads and avoid phantom reads), plus
//! access collection for the commit algorithm.
//!
//! [`Transaction`] is a cheap, `Send` handle over shared, interior-mutable state
//! (`Arc<Mutex<TransactionInner>>`). It is passed *by value* into the transaction
//! closure so the resulting future is `Send` and can be `tokio::spawn`-ed; the
//! framework keeps its own handle (see [`Transaction::handle`]) to read the collected
//! accesses after the closure returns and to reset between retries. All methods
//! take `&self` and only hold the lock briefly — never across an `.await` — so
//! several reads can run concurrently within a single transaction.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use glassdb_data::{CollectionAddress, CollectionId, KeyRef, TxId};
use glassdb_storage::{LeafObservation, StorageError};
use glassdb_trans::{
    CollectionCatalog, CollectionChange, CollectionData, CollectionOp, Data, DirectoryRead,
    DirectoryReadKind, ReadAccess, Reader, Resolver, ScanAccess, ScanMutation, WriteAccess,
};

use crate::collection::{Collection, CollectionPath, validate_collection_name};
use crate::db::DbInner;
use crate::error::Error;
use crate::iter::{CollectionEntry, CollectionsIter};
use crate::scan::{KeyPage, KeyScan};

/// An active database transaction. Reads and writes are buffered and only
/// applied atomically when the surrounding [`crate::Database::tx`] commits.
///
/// Awaiting [`Transaction::read`] (and the enclosing [`crate::Database::tx`] future) is
/// durability-safe to cancel by being dropped (`tokio::time::timeout`,
/// `select!`, or `JoinHandle::abort`). When the future is dropped mid-flight
/// the surrounding `Database::tx` arranges (via an internal RAII guard) for the
/// engine-side transaction to be asynchronously aborted, so locks are
/// released promptly instead of waiting for lease expiry.
pub struct Transaction {
    db: Arc<DbInner>,
    reader: Reader,
    resolver: Resolver,
    catalog: CollectionCatalog,
    inner: Arc<Mutex<TransactionInner>>,
}

#[derive(Default)]
struct TransactionInner {
    staged: HashMap<KeyRef, StagedValue>,
    reads: HashMap<KeyRef, ReadState>,
    scans: Vec<ScanAccess>,
    directories: HashMap<CollectionAddress, DirectoryState>,
    directory_reads: Vec<DirectoryRead>,
    collection_changes: BTreeMap<(CollectionAddress, Vec<u8>), CollectionChange>,
    created: HashSet<CollectionAddress>,
    dropped: HashSet<CollectionAddress>,
    dropped_bindings: HashSet<(CollectionAddress, Vec<u8>)>,
    reservations: HashMap<(CollectionAddress, Vec<u8>), CollectionId>,
    aborted: bool,
}

struct DirectoryState {
    base: BTreeMap<Vec<u8>, CollectionId>,
    current: BTreeMap<Vec<u8>, CollectionId>,
}

pub(crate) struct TransactionMetrics {
    pub(crate) cache_hits: u64,
}

impl TransactionInner {
    fn record_read(&mut self, key: KeyRef, mut state: ReadState) {
        // Concurrent reads of one path can both miss the transaction-local
        // state. Preserve a hit observed by either result while still counting
        // the path once, consistently with `tx_reads`.
        if self.reads.get(&key).is_some_and(ReadState::cache_hit) {
            state.set_cache_hit();
        }
        self.reads.insert(key, state);
    }
}

impl Transaction {
    /// Reads the value for `key` within the transaction, returning `None` when
    /// the key is absent. Repeatable: a value read once is returned consistently,
    /// and a key not found stays not found (avoiding phantom reads).
    ///
    /// Takes `&self`, so multiple reads can be polled concurrently (e.g. with
    /// `futures::future::join_all`) to fetch keys in parallel.
    pub async fn read(&self, c: &Collection, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.validate_handle(c)?;
        let key = KeyRef::new(c.address().clone(), key);
        // Brief lock to consult the per-transaction cache. The guard is dropped
        // before the backend read below so it is never held across `.await`.
        {
            let inner = self.inner.lock().unwrap();
            if let Some(staged) = inner.staged.get(&key) {
                return Ok(staged.read());
            }
            if let Some(ReadState::NotFound { .. }) = inner.reads.get(&key) {
                // Be consistent with values not found the first time.
                return Ok(None);
            }
            if inner.dropped.contains(c.address()) {
                return Err(Error::StaleCollection);
            }
            if inner.created.contains(c.address()) {
                return Ok(None);
            }
        }

        match self.reader.read(&key, std::time::Duration::MAX).await {
            Ok(outcome) => match outcome.value {
                None => {
                    let mut inner = self.inner.lock().unwrap();
                    inner.record_read(
                        key,
                        ReadState::NotFound {
                            last_writer: outcome.last_writer,
                            cache_hit: outcome.cache_hit,
                            leaf: outcome.leaf,
                        },
                    );
                    Ok(None)
                }
                Some(rv) => {
                    let mut inner = self.inner.lock().unwrap();
                    inner
                        .staged
                        .insert(key.clone(), StagedValue::Read(rv.value.clone()));
                    inner.record_read(
                        key,
                        ReadState::Found {
                            last_writer: rv.version.writer,
                            cache_hit: outcome.cache_hit,
                            leaf: outcome.leaf,
                        },
                    );
                    Ok(Some(rv.value.to_vec()))
                }
            },
            // A read is side-effect-free; `from_read` centralizes the mapping
            // (notably a sustained outage becomes the retry-safe
            // `Error::Unavailable` rather than `InDoubt`).
            Err(e) => Err(self.map_collection_read_error(c, e)),
        }
    }

    /// Materializes one sorted page of keys within this transaction.
    ///
    /// The scan participates in serializable validation and reflects writes and
    /// deletes staged before this call. Values remain separate tracked reads.
    pub async fn scan_keys(&self, c: &Collection, scan: KeyScan<'_>) -> Result<KeyPage, Error> {
        self.validate_handle(c)?;
        let range = scan.normalize()?;
        let limit = range.limit;
        let (mut overlay, created) = {
            let inner = self.inner.lock().unwrap();
            if inner.dropped.contains(c.address()) {
                return Err(Error::StaleCollection);
            }
            let overlay = inner
                .staged
                .iter()
                .filter_map(|(key, value)| {
                    if key.collection() != c.address() {
                        return None;
                    }
                    let present = match value {
                        StagedValue::Read(_) => return None,
                        StagedValue::Put(_) => true,
                        StagedValue::Delete => false,
                    };
                    Some(ScanMutation {
                        key: key.key().to_vec(),
                        present,
                    })
                })
                .collect::<Vec<_>>();
            (overlay, inner.created.contains(c.address()))
        };
        overlay.sort_by(|a, b| a.key.cmp(&b.key));

        if created {
            let keys = overlay
                .iter()
                .filter(|mutation| mutation.present && range.contains(&mutation.key))
                .map(|mutation| mutation.key.clone())
                .take(limit.unwrap_or(usize::MAX))
                .collect();
            return Ok(KeyPage::new(keys, limit));
        }

        let result = self
            .resolver
            .scan_keys(c.address(), &range, &overlay, None, None)
            .await
            .map_err(|error| self.map_collection_read_error(c, error))?;
        let keys = result.keys;
        self.inner.lock().unwrap().scans.push(ScanAccess {
            collection: c.address().clone(),
            range,
            overlay,
            keys: keys.clone(),
            frontier: result.frontier,
            covered: result.covered,
        });
        Ok(KeyPage::new(keys, limit))
    }

    /// Stages a write of `value` to `key`.
    pub fn write(&self, c: &Collection, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.validate_handle(c)?;
        if self.inner.lock().unwrap().dropped.contains(c.address()) {
            return Err(Error::InvalidInput(
                "cannot write a collection after dropping it".into(),
            ));
        }
        let key = KeyRef::new(c.address().clone(), key);
        self.inner
            .lock()
            .unwrap()
            .staged
            .insert(key, StagedValue::Put(Arc::from(value)));
        Ok(())
    }

    /// Marks `key` for deletion within the transaction.
    pub fn delete(&self, c: &Collection, key: &[u8]) -> Result<(), Error> {
        self.validate_handle(c)?;
        if self.inner.lock().unwrap().dropped.contains(c.address()) {
            return Err(Error::InvalidInput(
                "cannot write a collection after dropping it".into(),
            ));
        }
        let key = KeyRef::new(c.address().clone(), key);
        self.inner
            .lock()
            .unwrap()
            .staged
            .insert(key, StagedValue::Delete);
        Ok(())
    }

    /// Returns this transaction's handle to the permanent database root.
    pub fn root_collection(&self) -> Collection {
        Collection::new_root(self.db.clone())
    }

    /// Strictly creates and binds a direct child collection.
    pub async fn create_collection(
        &self,
        parent: &Collection,
        name: impl AsRef<[u8]>,
    ) -> Result<Collection, Error> {
        let (collection, _) = self.create_child(parent, name.as_ref(), true).await?;
        Ok(collection)
    }

    /// Opens or creates a direct child and reports whether this transaction
    /// created it.
    pub async fn create_collection_if_absent(
        &self,
        parent: &Collection,
        name: impl AsRef<[u8]>,
    ) -> Result<(Collection, bool), Error> {
        self.create_child(parent, name.as_ref(), false).await
    }

    /// Opens the direct child currently bound to `name`.
    pub async fn open_collection(
        &self,
        parent: &Collection,
        name: impl AsRef<[u8]>,
    ) -> Result<Collection, Error> {
        let name = name.as_ref();
        validate_collection_name(name)?;
        self.validate_handle(parent)?;
        self.ensure_directory(parent.address()).await?;
        let id = {
            let mut inner = self.inner.lock().unwrap();
            if inner.dropped.contains(parent.address()) {
                return Err(Error::StaleCollection);
            }
            let state = inner
                .directories
                .get(parent.address())
                .expect("directory was loaded above");
            let base = state.base.get(name).copied();
            let current = state.current.get(name).copied();
            inner.directory_reads.push(DirectoryRead {
                parent: parent.address().clone(),
                kind: DirectoryReadKind::Entry {
                    name: name.to_vec(),
                    collection: base,
                },
            });
            current.ok_or(Error::NotFound)?
        };
        Ok(Collection::new_child(
            CollectionAddress::new(self.db.name.as_str(), id),
            parent.address().clone(),
            name,
            self.db.clone(),
        ))
    }

    /// Reports whether a direct child is currently bound to `name`.
    pub async fn collection_exists(
        &self,
        parent: &Collection,
        name: impl AsRef<[u8]>,
    ) -> Result<bool, Error> {
        let name = name.as_ref();
        validate_collection_name(name)?;
        self.validate_handle(parent)?;
        self.ensure_directory(parent.address()).await?;
        let mut inner = self.inner.lock().unwrap();
        if inner.dropped.contains(parent.address()) {
            return Err(Error::StaleCollection);
        }
        let state = inner
            .directories
            .get(parent.address())
            .expect("directory was loaded above");
        let base = state.base.get(name).copied();
        let exists = state.current.contains_key(name);
        inner.directory_reads.push(DirectoryRead {
            parent: parent.address().clone(),
            kind: DirectoryReadKind::Entry {
                name: name.to_vec(),
                collection: base,
            },
        });
        Ok(exists)
    }

    /// Resolves an unresolved collection path from the permanent root.
    pub async fn open_collection_path<P>(&self, path: P) -> Result<Collection, Error>
    where
        P: TryInto<CollectionPath>,
        P::Error: Into<Error>,
    {
        let path = path.try_into().map_err(Into::into)?;
        let mut parent = self.root_collection();
        for name in path.segments() {
            parent = self.open_collection(&parent, name).await?;
        }
        Ok(parent)
    }

    /// Reports whether every component of an unresolved path is bound.
    pub async fn collection_path_exists<P>(&self, path: P) -> Result<bool, Error>
    where
        P: TryInto<CollectionPath>,
        P::Error: Into<Error>,
    {
        let path = path.try_into().map_err(Into::into)?;
        let mut parent = self.root_collection();
        for name in path.segments() {
            if !self.collection_exists(&parent, name).await? {
                return Ok(false);
            }
            parent = self.open_collection(&parent, name).await?;
        }
        Ok(true)
    }

    /// Returns the direct child bindings in raw-name order.
    pub async fn collections(&self, parent: &Collection) -> Result<CollectionsIter, Error> {
        self.validate_handle(parent)?;
        self.ensure_directory(parent.address()).await?;
        let current = {
            let mut inner = self.inner.lock().unwrap();
            if inner.dropped.contains(parent.address()) {
                return Err(Error::StaleCollection);
            }
            let state = inner
                .directories
                .get(parent.address())
                .expect("directory was loaded above");
            let base = state
                .base
                .iter()
                .map(|(name, id)| (name.clone(), *id))
                .collect::<Vec<_>>();
            let current = state
                .current
                .iter()
                .map(|(name, id)| (name.clone(), *id))
                .collect::<Vec<_>>();
            inner.directory_reads.push(DirectoryRead {
                parent: parent.address().clone(),
                kind: DirectoryReadKind::Listing {
                    children: base.clone(),
                },
            });
            current
        };
        let entries = current
            .into_iter()
            .map(|(name, id)| {
                CollectionEntry::new(
                    name.clone(),
                    Collection::new_child(
                        CollectionAddress::new(self.db.name.as_str(), id),
                        parent.address().clone(),
                        &name,
                        self.db.clone(),
                    ),
                )
            })
            .collect();
        Ok(CollectionsIter::new(entries))
    }

    /// Non-recursively drops the exact collection incarnation bound by `collection`.
    pub async fn drop_collection(&self, collection: &Collection) -> Result<(), Error> {
        self.validate_handle(collection)?;
        if collection.address().id().is_root() {
            return Err(Error::InvalidInput(
                "the permanent root collection cannot be dropped".into(),
            ));
        }
        let parent = collection
            .parent_address()
            .ok_or_else(|| Error::InvalidInput("collection has no direct parent".into()))?
            .clone();
        let name = collection
            .name()
            .ok_or_else(|| Error::InvalidInput("collection has no direct name".into()))?
            .to_vec();
        self.ensure_directory(&parent).await?;
        self.ensure_directory(collection.address()).await?;

        let mut inner = self.inner.lock().unwrap();
        if inner
            .dropped_bindings
            .contains(&(parent.clone(), name.clone()))
        {
            return Err(Error::StaleCollection);
        }
        let parent_state = inner
            .directories
            .get(&parent)
            .expect("parent directory was loaded above");
        let expected = parent_state.base.get(name.as_slice()).copied();
        let current = parent_state.current.get(name.as_slice()).copied();
        inner.directory_reads.push(DirectoryRead {
            parent: parent.clone(),
            kind: DirectoryReadKind::Entry {
                name: name.clone(),
                collection: expected,
            },
        });
        if current != Some(collection.address().id()) {
            return Err(Error::StaleCollection);
        }
        let target = inner
            .directories
            .get(collection.address())
            .expect("target directory was loaded above");
        let target_base = target
            .base
            .iter()
            .map(|(name, id)| (name.clone(), *id))
            .collect::<Vec<_>>();
        let target_not_empty = !target.current.is_empty();
        inner.directory_reads.push(DirectoryRead {
            parent: collection.address().clone(),
            kind: DirectoryReadKind::Listing {
                children: target_base,
            },
        });
        if target_not_empty {
            return Err(Error::NotEmpty);
        }
        if inner.staged.iter().any(|(key, value)| {
            key.collection() == collection.address()
                && matches!(value, StagedValue::Put(_) | StagedValue::Delete)
        }) {
            return Err(Error::InvalidInput(
                "cannot drop a collection after staging data writes to it".into(),
            ));
        }
        inner
            .directories
            .get_mut(&parent)
            .expect("parent directory was loaded above")
            .current
            .remove(name.as_slice());
        let binding = (parent.clone(), name.clone());
        if inner.created.remove(collection.address()) {
            inner.collection_changes.remove(&binding);
            inner
                .directory_reads
                .retain(|read| &read.parent != collection.address());
        } else {
            inner.collection_changes.insert(
                binding.clone(),
                CollectionChange {
                    parent,
                    name,
                    collection: collection.address().clone(),
                    expected,
                    op: CollectionOp::Drop,
                },
            );
        }
        inner.dropped.insert(collection.address().clone());
        inner.dropped_bindings.insert(binding);
        Ok(())
    }

    /// Explicitly aborts the transaction. Returns [`Error::Aborted`].
    pub fn abort(&self) -> Result<(), Error> {
        self.inner.lock().unwrap().aborted = true;
        Err(Error::Aborted)
    }

    pub(crate) fn new(db: Arc<DbInner>) -> Self {
        let resolver = Resolver::new(db.shards.clone(), db.tmon.clone());
        Transaction {
            reader: Reader::new(resolver.clone(), db.timeline.clone(), db.retry),
            resolver,
            catalog: CollectionCatalog::new(db.shards.clone(), db.tmon.clone(), db.retry),
            db,
            inner: Arc::new(Mutex::new(TransactionInner::default())),
        }
    }

    /// Returns another handle to the same transaction state. The framework
    /// passes a handle to the user closure (which consumes it) while keeping one
    /// to inspect the staged accesses and reset between retries.
    pub(crate) fn handle(&self) -> Transaction {
        Transaction {
            db: self.db.clone(),
            reader: self.reader.clone(),
            resolver: self.resolver.clone(),
            catalog: self.catalog.clone(),
            inner: self.inner.clone(),
        }
    }

    pub(crate) fn aborted(&self) -> bool {
        self.inner.lock().unwrap().aborted
    }

    pub(crate) fn reset(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.staged.clear();
        inner.reads.clear();
        inner.scans.clear();
        inner.directories.clear();
        inner.directory_reads.clear();
        inner.collection_changes.clear();
        inner.created.clear();
        inner.dropped.clear();
        inner.dropped_bindings.clear();
    }

    pub(crate) fn collect_accesses(&self) -> (Data, CollectionData) {
        let inner = self.inner.lock().unwrap();
        let mut writes = Vec::new();
        for (k, v) in &inner.staged {
            match v {
                StagedValue::Read(_) => {}
                StagedValue::Put(val) => writes.push(WriteAccess::put(k.clone(), val.clone())),
                StagedValue::Delete => writes.push(WriteAccess::delete(k.clone())),
            }
        }
        let mut reads = Vec::new();
        for (k, v) in &inner.reads {
            let (last_writer, leaf) = match v {
                ReadState::Found {
                    last_writer, leaf, ..
                } => (Some(last_writer.clone()), leaf.clone()),
                ReadState::NotFound {
                    last_writer, leaf, ..
                } => (last_writer.clone(), leaf.clone()),
            };
            reads.push(ReadAccess {
                key: k.clone(),
                last_writer,
                leaf,
            });
        }
        // Emit accesses in a stable path order so the commit path (transaction
        // log contents, lock acquisition order, validation order) is
        // independent of `HashMap`'s randomized iteration, and of the order in
        // which concurrent reads happened to insert their entries. This makes a
        // simulation replay byte-for-byte identical and is harmless in production.
        writes.sort_by(|a, b| a.key.cmp(&b.key));
        reads.sort_by(|a, b| a.key.cmp(&b.key));
        // Scans are recorded in listing order, which is already deterministic
        // (leaves scanned left-to-right), so they need no re-sorting.
        let scans = inner.scans.clone();
        (
            Data {
                reads,
                writes,
                scans,
            },
            CollectionData {
                reads: inner.directory_reads.clone(),
                changes: inner.collection_changes.values().cloned().collect(),
            },
        )
    }

    pub(crate) fn metrics(&self) -> TransactionMetrics {
        let inner = self.inner.lock().unwrap();
        TransactionMetrics {
            cache_hits: inner.reads.values().filter(|r| r.cache_hit()).count() as u64,
        }
    }

    async fn create_child(
        &self,
        parent: &Collection,
        name: &[u8],
        strict: bool,
    ) -> Result<(Collection, bool), Error> {
        validate_collection_name(name)?;
        self.validate_handle(parent)?;
        self.ensure_directory(parent.address()).await?;
        let mut inner = self.inner.lock().unwrap();
        if inner.dropped.contains(parent.address()) {
            return Err(Error::StaleCollection);
        }
        let binding = (parent.address().clone(), name.to_vec());
        let state = inner
            .directories
            .get(parent.address())
            .expect("directory was loaded above");
        let base = state.base.get(name).copied();
        let existing = state.current.get(name).copied();
        inner.directory_reads.push(DirectoryRead {
            parent: parent.address().clone(),
            kind: DirectoryReadKind::Entry {
                name: name.to_vec(),
                collection: base,
            },
        });
        if let Some(id) = existing {
            if strict {
                return Err(Error::AlreadyExists);
            }
            let address = CollectionAddress::new(self.db.name.as_str(), id);
            let created = inner.created.contains(&address);
            return Ok((
                Collection::new_child(address, parent.address().clone(), name, self.db.clone()),
                created,
            ));
        }
        if inner.dropped_bindings.contains(&binding) {
            return Err(Error::InvalidInput(
                "cannot recreate a collection binding after dropping it in one transaction".into(),
            ));
        }
        let id = match inner.reservations.get(&binding).copied() {
            Some(id) => id,
            None => {
                let id = CollectionId::new_random();
                inner.reservations.insert(binding.clone(), id);
                id
            }
        };
        let address = CollectionAddress::new(self.db.name.as_str(), id);
        inner
            .directories
            .get_mut(parent.address())
            .expect("directory was loaded above")
            .current
            .insert(name.to_vec(), id);
        inner.collection_changes.insert(
            binding,
            CollectionChange {
                parent: parent.address().clone(),
                name: name.to_vec(),
                collection: address.clone(),
                expected: base,
                op: CollectionOp::Create,
            },
        );
        inner.created.insert(address.clone());
        Ok((
            Collection::new_child(address, parent.address().clone(), name, self.db.clone()),
            true,
        ))
    }

    async fn ensure_directory(&self, parent: &CollectionAddress) -> Result<(), Error> {
        {
            let mut inner = self.inner.lock().unwrap();
            if inner.directories.contains_key(parent) {
                return Ok(());
            }
            if inner.dropped.contains(parent) {
                return Err(Error::StaleCollection);
            }
            if inner.created.contains(parent) {
                inner.directories.insert(
                    parent.clone(),
                    DirectoryState {
                        base: BTreeMap::new(),
                        current: BTreeMap::new(),
                    },
                );
                return Ok(());
            }
        }

        let snapshot = self.catalog.snapshot(parent).await.map_err(|error| {
            let mapped = Error::from(error);
            if matches!(mapped, Error::NotFound) && !parent.id().is_root() {
                Error::StaleCollection
            } else {
                mapped
            }
        })?;
        let children = snapshot.children.into_iter().collect::<BTreeMap<_, _>>();
        let mut inner = self.inner.lock().unwrap();
        inner
            .directories
            .entry(parent.clone())
            .or_insert_with(|| DirectoryState {
                base: children.clone(),
                current: children,
            });
        Ok(())
    }

    fn validate_handle(&self, collection: &Collection) -> Result<(), Error> {
        if collection.database_id() != self.db.database_id
            || collection.address().db_root() != self.db.name
        {
            return Err(Error::InvalidInput(
                "collection handle belongs to a different database".into(),
            ));
        }
        Ok(())
    }

    fn map_collection_read_error(&self, collection: &Collection, error: StorageError) -> Error {
        if matches!(error, StorageError::NotFound) && !collection.address().id().is_root() {
            Error::StaleCollection
        } else {
            Error::from_read(error)
        }
    }
}

enum StagedValue {
    Read(Arc<[u8]>),
    Put(Arc<[u8]>),
    Delete,
}

impl StagedValue {
    fn read(&self) -> Option<Vec<u8>> {
        match self {
            StagedValue::Read(value) | StagedValue::Put(value) => Some(value.to_vec()),
            StagedValue::Delete => None,
        }
    }
}

enum ReadState {
    Found {
        last_writer: TxId,
        cache_hit: bool,
        leaf: LeafObservation,
    },
    NotFound {
        last_writer: Option<TxId>,
        cache_hit: bool,
        leaf: LeafObservation,
    },
}

impl ReadState {
    fn cache_hit(&self) -> bool {
        match self {
            ReadState::Found { cache_hit, .. } | ReadState::NotFound { cache_hit, .. } => {
                *cache_hit
            }
        }
    }

    fn set_cache_hit(&mut self) {
        match self {
            ReadState::Found { cache_hit, .. } | ReadState::NotFound { cache_hit, .. } => {
                *cache_hit = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use crate::{Backend, CollectionPath, Database, KeyScan, memory::MemoryBackend};

    // ADR-031 phantom prevention, the in-flight case: when a key is created
    // *while* a listing transaction is running — after it scanned the leaf but
    // before it validated — the create rewrites the covered leaf, bumping its
    // version. The listing's commit validation detects the changed snapshot and
    // re-runs the transaction; the retry re-scans the fresh leaf and therefore
    // includes the racing key. A create is never silently dropped from a listing
    // it raced.
    #[tokio::test]
    async fn listing_retries_to_include_a_key_added_during_the_scan() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let db = Database::open("example", backend).await.unwrap();
        let path = CollectionPath::new(b"phantom-retry").unwrap();
        let coll = db.create_collection(&path).await.unwrap();

        let seed: Vec<Vec<u8>> = (0u32..5).map(|i| i.to_be_bytes().to_vec()).collect();
        for k in &seed {
            coll.write(k, b"v").await.unwrap();
        }

        let extra = 999u32.to_be_bytes().to_vec();
        let first_attempt = AtomicBool::new(true);

        // The listing runs in a read-only transaction. On its first attempt a
        // concurrent transaction commits a new key *after* the scan recorded the
        // leaf version, modeling a create that lands mid-listing. That
        // invalidates the recorded snapshot, forcing the listing to retry.
        let listed = db
            .tx(|tx| {
                let coll = coll.clone();
                let extra = extra.clone();
                let first_attempt = &first_attempt;
                async move {
                    let keys = tx.scan_keys(&coll, KeyScan::all()).await?.into_keys();
                    if first_attempt.swap(false, Ordering::SeqCst) {
                        coll.write(&extra, b"v").await?;
                    }
                    Ok(keys)
                }
            })
            .await
            .unwrap();

        assert!(
            listed.contains(&extra),
            "the key created during the listing is included after the retry"
        );
        let mut expected: Vec<Vec<u8>> = seed;
        expected.push(extra);
        expected.sort();
        assert_eq!(
            listed, expected,
            "the listing observes the full, sorted committed set"
        );
        assert!(
            db.stats().tx_retries >= 1,
            "the listing must have retried after its snapshot was invalidated"
        );
    }
}
