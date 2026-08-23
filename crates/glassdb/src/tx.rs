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

mod catalog;
mod data;

use std::sync::{Arc, Mutex};

use glassdb_data::{CollectionAddress, KeyRef};
use glassdb_trans::{AccessSet, CollectionData};

use self::catalog::{CatalogOverlay, CreateMode};
use self::data::{DataOverlay, OverlayRead};
use crate::collection::{Collection, CollectionPath, validate_collection_name};
use crate::db::DbInner;
use crate::error::Error;
use crate::iter::{CollectionEntry, CollectionIter};
use crate::scan::{KeyPage, KeyScan};

/// An active database transaction. Reads and writes are buffered and only
/// applied atomically when the surrounding [`crate::Database::tx`] commits.
///
/// Awaiting [`Transaction::read`] (and the enclosing [`crate::Database::tx`] future) is
/// durability-safe to cancel by being dropped (`tokio::time::timeout`,
/// `select!`, or `JoinHandle::abort`). When the future is dropped mid-flight
/// the surrounding `Database::tx` uses an internal RAII guard to hand any
/// engine-side attempt to managed retirement. Panics use the same handoff.
/// Durable helpers and garbage collection may reclaim physical resources
/// asynchronously.
pub struct Transaction {
    db: Arc<DbInner>,
    inner: Arc<Mutex<TransactionInner>>,
}

#[derive(Default)]
struct TransactionInner {
    data: DataOverlay,
    catalog: CatalogOverlay,
    aborted: bool,
}

pub(crate) struct TransactionMetrics {
    pub(crate) cache_hits: u64,
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
            match inner.data.read(&key) {
                OverlayRead::Known(value) => return Ok(value),
                OverlayRead::Unknown => {}
            }
            if inner.catalog.is_dropped(c.address()) {
                return Err(Error::StaleCollection);
            }
            if inner.catalog.is_created(c.address()) {
                return Ok(None);
            }
        }

        match self.db.engine.read(&key, std::time::Duration::MAX).await {
            Ok(outcome) => {
                let (value, cache_hit, evidence) = outcome.into_parts();
                match value {
                    None => {
                        let mut inner = self.inner.lock().unwrap();
                        inner.data.record_not_found(key, cache_hit, evidence);
                        Ok(None)
                    }
                    Some(rv) => {
                        let mut inner = self.inner.lock().unwrap();
                        inner
                            .data
                            .record_found(key, rv.value.clone(), cache_hit, evidence);
                        Ok(Some(rv.value.to_vec()))
                    }
                }
            }
            // A read is side-effect-free; `from_read` centralizes the mapping
            // (notably a sustained outage becomes the retry-safe
            // `Error::Unavailable` rather than `InDoubt`).
            Err(e) => Err(Error::from_read(e)),
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
        let (overlay, created) = {
            let inner = self.inner.lock().unwrap();
            if inner.catalog.is_dropped(c.address()) {
                return Err(Error::StaleCollection);
            }
            let overlay = inner.data.scan_mutations(c.address());
            (overlay, inner.catalog.is_created(c.address()))
        };

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
            .db
            .engine
            .scan(c.address(), &range, &overlay)
            .await
            .map_err(Error::from_read)?;
        let keys = result.keys().to_vec();
        let access = result.into_access(c.address().clone(), range, overlay);
        self.inner.lock().unwrap().data.record_scan(access);
        Ok(KeyPage::new(keys, limit))
    }

    /// Stages a write of `value` to `key`.
    pub fn write(&self, c: &Collection, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.validate_handle(c)?;
        if self.inner.lock().unwrap().catalog.is_dropped(c.address()) {
            return Err(Error::InvalidInput(
                "cannot write a collection after dropping it".into(),
            ));
        }
        let key = KeyRef::new(c.address().clone(), key);
        self.inner.lock().unwrap().data.write(key, Arc::from(value));
        Ok(())
    }

    /// Marks `key` for deletion within the transaction.
    pub fn delete(&self, c: &Collection, key: &[u8]) -> Result<(), Error> {
        self.validate_handle(c)?;
        if self.inner.lock().unwrap().catalog.is_dropped(c.address()) {
            return Err(Error::InvalidInput(
                "cannot write a collection after dropping it".into(),
            ));
        }
        let key = KeyRef::new(c.address().clone(), key);
        self.inner.lock().unwrap().data.delete(key);
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
        let (collection, _) = self
            .create_child(parent, name.as_ref(), CreateMode::Strict)
            .await?;
        Ok(collection)
    }

    /// Opens or creates a direct child and reports whether this transaction
    /// created it.
    pub async fn create_collection_if_absent(
        &self,
        parent: &Collection,
        name: impl AsRef<[u8]>,
    ) -> Result<(Collection, bool), Error> {
        self.create_child(parent, name.as_ref(), CreateMode::IfAbsent)
            .await
    }

    /// Opens the direct child currently bound to `name`.
    pub async fn open_collection(
        &self,
        parent: &Collection,
        name: impl AsRef<[u8]>,
    ) -> Result<Collection, Error> {
        self.resolve_child(parent, name.as_ref())
            .await?
            .ok_or(Error::NotFound)
    }

    /// Reports whether a direct child is currently bound to `name`.
    pub async fn collection_exists(
        &self,
        parent: &Collection,
        name: impl AsRef<[u8]>,
    ) -> Result<bool, Error> {
        Ok(self.resolve_child(parent, name.as_ref()).await?.is_some())
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
            let Some(child) = self.resolve_child(&parent, name).await? else {
                return Ok(false);
            };
            parent = child;
        }
        Ok(true)
    }

    /// Returns an owned iterator over direct child bindings in raw-name order.
    ///
    /// The directory observation and materialization complete before the
    /// iterator is returned; the enclosing transaction validates that
    /// observation when its attempt completes. Each yielded handle remains
    /// bound to the listed incarnation.
    pub async fn iter_collections(&self, parent: &Collection) -> Result<CollectionIter, Error> {
        self.validate_handle(parent)?;
        self.ensure_directory(parent.address()).await?;
        let current = {
            let mut inner = self.inner.lock().unwrap();
            inner.catalog.children(parent.address())?
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
        Ok(CollectionIter::new(entries))
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
        let has_data_writes = inner.data.has_writes_for(collection.address());
        inner
            .catalog
            .drop_collection(parent, name, collection.address(), has_data_writes)
    }

    /// Explicitly aborts the transaction. Returns [`Error::Aborted`].
    pub fn abort(&self) -> Result<(), Error> {
        self.inner.lock().unwrap().aborted = true;
        Err(Error::Aborted)
    }

    pub(crate) fn new(db: Arc<DbInner>) -> Self {
        Transaction {
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
            inner: self.inner.clone(),
        }
    }

    pub(crate) fn aborted(&self) -> bool {
        self.inner.lock().unwrap().aborted
    }

    pub(crate) fn reset(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.data.reset();
        inner.catalog.reset();
    }

    pub(crate) fn collect_accesses(&self) -> (AccessSet, CollectionData) {
        let inner = self.inner.lock().unwrap();
        (inner.data.accesses(), inner.catalog.accesses())
    }

    pub(crate) fn metrics(&self) -> TransactionMetrics {
        let inner = self.inner.lock().unwrap();
        TransactionMetrics {
            cache_hits: inner.data.cache_hits(),
        }
    }

    async fn create_child(
        &self,
        parent: &Collection,
        name: &[u8],
        mode: CreateMode,
    ) -> Result<(Collection, bool), Error> {
        validate_collection_name(name)?;
        self.validate_handle(parent)?;
        self.ensure_directory(parent.address()).await?;
        let mut inner = self.inner.lock().unwrap();
        let (address, created) = inner.catalog.create_child(parent.address(), name, mode)?;
        Ok((
            Collection::new_child(address, parent.address().clone(), name, self.db.clone()),
            created,
        ))
    }

    async fn resolve_child(
        &self,
        parent: &Collection,
        name: &[u8],
    ) -> Result<Option<Collection>, Error> {
        validate_collection_name(name)?;
        self.validate_handle(parent)?;
        self.ensure_directory(parent.address()).await?;
        let id = self
            .inner
            .lock()
            .unwrap()
            .catalog
            .child(parent.address(), name)?;
        Ok(id.map(|id| {
            Collection::new_child(
                CollectionAddress::new(self.db.name.as_str(), id),
                parent.address().clone(),
                name,
                self.db.clone(),
            )
        }))
    }

    async fn ensure_directory(&self, parent: &CollectionAddress) -> Result<(), Error> {
        {
            let mut inner = self.inner.lock().unwrap();
            if !inner.catalog.prepare_directory(parent)? {
                return Ok(());
            }
        }

        let snapshot = self.db.engine.collection_snapshot(parent).await?;
        let mut inner = self.inner.lock().unwrap();
        inner.catalog.install_snapshot(parent.clone(), snapshot);
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
            db.stats().transactions.retries >= 1,
            "the listing must have retried after its snapshot was invalidated"
        );
    }
}
