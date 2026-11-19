//! Collection handles, unresolved collection paths, and standalone collection
//! management.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use glassdb_data::{CollectionAddress, CollectionName, DatabaseId};

use crate::db::DbInner;
use crate::error::Error;
use crate::iter::{CollectionIter, KeyIter};
use crate::scan::{KeyPage, KeyScan};
use crate::tx::CreateMode;

/// An unresolved sequence of logical collection names.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CollectionPath {
    segments: Arc<[CollectionName]>,
}

impl CollectionPath {
    /// Creates a path containing one top-level collection name.
    pub fn new(name: impl AsRef<[u8]>) -> Result<Self, Error> {
        Ok(Self {
            segments: vec![collection_name(name.as_ref())?].into(),
        })
    }

    /// Returns a path extended by one direct child name.
    pub fn child(&self, name: impl AsRef<[u8]>) -> Result<Self, Error> {
        let mut segments = self.segments.to_vec();
        segments.push(collection_name(name.as_ref())?);
        Ok(Self {
            segments: segments.into(),
        })
    }

    /// Returns the path's raw names from outermost to innermost.
    pub fn segments(&self) -> impl ExactSizeIterator<Item = &[u8]> + DoubleEndedIterator {
        self.segments.iter().map(CollectionName::as_bytes)
    }

    /// Returns the path's validated names from outermost to innermost.
    pub(crate) fn names(&self) -> &[CollectionName] {
        &self.segments
    }
}

impl From<&CollectionPath> for CollectionPath {
    fn from(path: &CollectionPath) -> Self {
        path.clone()
    }
}

impl TryFrom<&str> for CollectionPath {
    type Error = Error;

    fn try_from(name: &str) -> Result<Self, Self::Error> {
        Self::new(name.as_bytes())
    }
}

impl TryFrom<String> for CollectionPath {
    type Error = Error;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        Self::new(name.as_bytes())
    }
}

impl TryFrom<&String> for CollectionPath {
    type Error = Error;

    fn try_from(name: &String) -> Result<Self, Self::Error> {
        Self::new(name.as_bytes())
    }
}

/// A named group of key-value pairs bound to one collection ID.
#[derive(Clone)]
pub struct Collection {
    address: CollectionAddress,
    parent: Option<CollectionAddress>,
    name: Option<CollectionName>,
    db: Arc<DbInner>,
}

impl Collection {
    /// Reads the value for `key` with strong (serializable) consistency,
    /// returning `None` when the key is absent.
    pub async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.reads(None).read(key).await
    }

    /// Writes `value` for `key` within a transaction.
    pub async fn write(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.db
            .tx(|tx| async move { tx.write(self, key, value) })
            .await
    }

    /// Removes `key` within a transaction.
    pub async fn delete(&self, key: &[u8]) -> Result<(), Error> {
        self.db.tx(|tx| async move { tx.delete(self, key) }).await
    }

    /// Atomically reads `key`, applies `f`, and writes the result back.
    ///
    /// The update function may run more than once when its read is invalidated. If it
    /// panics, the payload propagates without read validation or replay and no
    /// staged write from that execution is published.
    pub async fn update<F>(&self, key: &[u8], f: F) -> Result<Vec<u8>, Error>
    where
        F: FnMut(Vec<u8>) -> Result<Vec<u8>, Error> + Send,
    {
        // The transaction body is replayed on conflict, so it must be `FnMut`. An
        // `async move` block would move `f` into the future (making the closure
        // `FnOnce`), so share it through an `Arc<Mutex<_>>` cloned per body execution.
        // The update function is synchronous, so the guard is never held across an
        // `.await`.
        let f = Arc::new(Mutex::new(f));
        self.db
            .tx(move |tx| {
                let f = f.clone();
                async move {
                    let old = tx.read(self, key).await?.ok_or(Error::NotFound)?;
                    let newb = (f.lock().unwrap())(old)?;
                    tx.write(self, key, &newb)?;
                    Ok(newb)
                }
            })
            .await
    }

    /// Opens the direct child currently bound to `name`.
    pub async fn open_collection(&self, name: impl AsRef<[u8]>) -> Result<Collection, Error> {
        self.reads(None).open_collection(name).await
    }

    /// Reports whether a direct child is currently bound to `name`.
    pub async fn collection_exists(&self, name: impl AsRef<[u8]>) -> Result<bool, Error> {
        self.reads(None).collection_exists(name).await
    }

    /// Strictly creates and binds a new direct child.
    pub async fn create_collection(&self, name: impl AsRef<[u8]>) -> Result<Collection, Error> {
        self.create_child(name.as_ref(), CreateMode::Strict).await
    }

    /// Returns the direct child bound to `name`, creating it when absent.
    pub async fn create_collection_if_absent(
        &self,
        name: impl AsRef<[u8]>,
    ) -> Result<Collection, Error> {
        self.create_child(name.as_ref(), CreateMode::IfAbsent).await
    }

    /// Returns an owned iterator over the collection's materialized keys.
    ///
    /// The listing runs inside a read-only serializable transaction. All I/O
    /// and validation complete before the iterator is returned, so iteration
    /// itself cannot fail and yields sorted raw keys.
    pub async fn iter_keys(&self) -> Result<KeyIter, Error> {
        self.reads(None).iter_keys().await
    }

    /// Materializes one serializable, sorted page of collection keys.
    pub async fn scan_keys(&self, scan: KeyScan<'_>) -> Result<KeyPage, Error> {
        self.reads(None).scan_keys(scan).await
    }

    /// Returns an owned iterator over direct child bindings in raw-name order.
    ///
    /// All I/O and serializable validation complete before the iterator is
    /// returned. Each yielded handle remains bound to the listed collection ID.
    pub async fn iter_collections(&self) -> Result<CollectionIter, Error> {
        self.reads(None).iter_collections().await
    }

    /// Non-recursively drops this exact collection.
    pub async fn drop_collection(&self) -> Result<(), Error> {
        self.db
            .tx(|tx| async move { tx.drop_collection(self).await })
            .await
    }

    /// Returns this handle's direct logical name, or `None` for the root collection.
    pub fn name(&self) -> Option<&[u8]> {
        self.name.as_ref().map(CollectionName::as_bytes)
    }

    /// Returns a view whose read helpers each run one stale read-only
    /// transaction with the bound `max_staleness`.
    ///
    /// Each helper is the same as a
    /// [`StaleDatabaseView::read_tx`](crate::StaleDatabaseView::read_tx) with
    /// the same bound, whose body does the work of the strong helper of this
    /// collection, with the same guarantees.
    pub fn stale(&self, max_staleness: Duration) -> StaleCollectionView {
        StaleCollectionView {
            collection: self.clone(),
            max_staleness,
        }
    }

    pub(crate) fn new_root(db: Arc<DbInner>) -> Self {
        Self {
            address: CollectionAddress::root(db.name.as_str()),
            parent: None,
            name: None,
            db,
        }
    }

    pub(crate) fn new_child(
        address: CollectionAddress,
        parent: CollectionAddress,
        name: CollectionName,
        db: Arc<DbInner>,
    ) -> Self {
        Self {
            address,
            parent: Some(parent),
            name: Some(name),
            db,
        }
    }

    pub(crate) fn address(&self) -> &CollectionAddress {
        &self.address
    }

    /// Returns the parent and name of the binding that this handle was
    /// resolved through, or `None` for the root collection.
    pub(crate) fn binding(&self) -> Option<(&CollectionAddress, &CollectionName)> {
        self.parent.as_ref().zip(self.name.as_ref())
    }

    pub(crate) fn database_id(&self) -> DatabaseId {
        self.db.database_id
    }

    fn reads(&self, max_staleness: Option<Duration>) -> CollectionReads<'_> {
        CollectionReads {
            collection: self,
            max_staleness,
        }
    }

    async fn create_child(&self, name: &[u8], mode: CreateMode) -> Result<Collection, Error> {
        let name = &collection_name(name)?;
        self.db
            .tx(|tx| async move { Ok(tx.create_child(self, name, mode).await?.0) })
            .await
    }
}

/// The read helpers of one collection, each run as a stale read-only
/// transaction. [`Collection::stale`] returns it.
///
/// A view is cheap to clone.
#[derive(Clone)]
pub struct StaleCollectionView {
    collection: Collection,
    max_staleness: Duration,
}

impl StaleCollectionView {
    /// Reads the value for `key`, returning `None` when the key is absent.
    pub async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.reads().read(key).await
    }

    /// Opens the direct child bound to `name`.
    pub async fn open_collection(&self, name: impl AsRef<[u8]>) -> Result<Collection, Error> {
        self.reads().open_collection(name).await
    }

    /// Reports whether a direct child is bound to `name`.
    pub async fn collection_exists(&self, name: impl AsRef<[u8]>) -> Result<bool, Error> {
        self.reads().collection_exists(name).await
    }

    /// Returns an owned iterator over the materialized keys of the collection.
    ///
    /// All I/O completes in one transaction before the iterator is returned.
    pub async fn iter_keys(&self) -> Result<KeyIter, Error> {
        self.reads().iter_keys().await
    }

    /// Materializes one sorted page of collection keys.
    pub async fn scan_keys(&self, scan: KeyScan<'_>) -> Result<KeyPage, Error> {
        self.reads().scan_keys(scan).await
    }

    /// Returns an owned iterator over direct child bindings in raw-name order.
    ///
    /// All I/O completes in one transaction before the iterator is returned.
    pub async fn iter_collections(&self) -> Result<CollectionIter, Error> {
        self.reads().iter_collections().await
    }

    fn reads(&self) -> CollectionReads<'_> {
        CollectionReads {
            collection: &self.collection,
            max_staleness: Some(self.max_staleness),
        }
    }
}

/// The read helpers of one collection, as strong transactions or as stale
/// read-only transactions.
struct CollectionReads<'a> {
    collection: &'a Collection,
    /// `None` runs each helper as a strong transaction, so that the strong
    /// helpers of [`Collection`] share these bodies.
    max_staleness: Option<Duration>,
}

impl CollectionReads<'_> {
    async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let c = self.collection;
        c.db.tx_with(
            self.max_staleness,
            |tx| async move { tx.read(c, key).await },
        )
        .await
    }

    async fn open_collection(&self, name: impl AsRef<[u8]>) -> Result<Collection, Error> {
        let c = self.collection;
        let name = &collection_name(name.as_ref())?;
        c.db.tx_with(self.max_staleness, |tx| async move {
            tx.open_child(c, name).await
        })
        .await
    }

    async fn collection_exists(&self, name: impl AsRef<[u8]>) -> Result<bool, Error> {
        let c = self.collection;
        let name = &collection_name(name.as_ref())?;
        c.db.tx_with(self.max_staleness, |tx| async move {
            Ok(tx.resolve_child(c, name).await?.is_some())
        })
        .await
    }

    async fn iter_keys(&self) -> Result<KeyIter, Error> {
        Ok(KeyIter::new(
            self.scan_keys(KeyScan::all()).await?.into_keys(),
        ))
    }

    async fn scan_keys(&self, scan: KeyScan<'_>) -> Result<KeyPage, Error> {
        let c = self.collection;
        c.db.tx_with(self.max_staleness, |tx| async move {
            tx.scan_keys(c, scan).await
        })
        .await
    }

    async fn iter_collections(&self) -> Result<CollectionIter, Error> {
        let c = self.collection;
        c.db.tx_with(self.max_staleness, |tx| async move {
            tx.iter_collections(c).await
        })
        .await
    }
}

/// Converts a caller-supplied name into a collection name, and reports an
/// invalid name as invalid input.
pub(crate) fn collection_name(name: &[u8]) -> Result<CollectionName, Error> {
    CollectionName::new(name).map_err(|error| Error::InvalidInput(error.to_string()))
}
