//! Collection handles, unresolved collection paths, and standalone collection
//! management.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use glassdb_data::{CollectionAddress, DatabaseId, KeyRef, MAX_COLLECTION_NAME_BYTES};

use crate::db::DbInner;
use crate::error::Error;
use crate::iter::{CollectionIter, KeyIter};
use crate::scan::{KeyPage, KeyScan};

/// An unresolved sequence of logical collection names.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CollectionPath {
    segments: Arc<[Vec<u8>]>,
}

impl CollectionPath {
    /// Creates a path containing one top-level collection name.
    pub fn new(name: impl AsRef<[u8]>) -> Result<Self, Error> {
        validate_collection_name(name.as_ref())?;
        Ok(Self {
            segments: vec![name.as_ref().to_vec()].into(),
        })
    }

    /// Returns a path extended by one direct child name.
    pub fn child(&self, name: impl AsRef<[u8]>) -> Result<Self, Error> {
        validate_collection_name(name.as_ref())?;
        let mut segments = self.segments.to_vec();
        segments.push(name.as_ref().to_vec());
        Ok(Self {
            segments: segments.into(),
        })
    }

    /// Returns the path's raw names from outermost to innermost.
    pub fn segments(&self) -> impl ExactSizeIterator<Item = &[u8]> + DoubleEndedIterator {
        self.segments.iter().map(Vec::as_slice)
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

/// A named group of key-value pairs bound to one collection incarnation.
#[derive(Clone)]
pub struct Collection {
    address: CollectionAddress,
    parent: Option<CollectionAddress>,
    name: Option<Arc<[u8]>>,
    db: Arc<DbInner>,
}

impl Collection {
    /// Reads the value for `key` with strong (serializable) consistency,
    /// returning `None` when the key is absent.
    pub async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.db
            .tx(|tx| async move { tx.read(self, key).await })
            .await
    }

    /// Reads the value for `key` allowing stale results up to `max_staleness`,
    /// returning `None` when the key is absent.
    pub async fn read_stale(
        &self,
        key: &[u8],
        max_staleness: Duration,
    ) -> Result<Option<Vec<u8>>, Error> {
        let _guard = self.db.admit_operation()?;
        let key = KeyRef::new(self.address.clone(), key);
        match self.db.engine.read(&key, max_staleness).await {
            Ok(outcome) => Ok(outcome.value.map(|rv| rv.value.to_vec())),
            Err(e) => Err(Error::from_read(e)),
        }
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
    /// The callback may run more than once when its read is invalidated. If it
    /// panics, the payload propagates without read validation or replay and no
    /// staged write from that execution is published.
    pub async fn update<F>(&self, key: &[u8], f: F) -> Result<Vec<u8>, Error>
    where
        F: FnMut(Vec<u8>) -> Result<Vec<u8>, Error> + Send,
    {
        // The transaction body is rerun on conflict, so it must be `FnMut`. An
        // `async move` block would move `f` into the future (making the closure
        // `FnOnce`), so share it through an `Arc<Mutex<_>>` cloned per attempt.
        // The user callback is synchronous, so the guard is never held across an
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
        let name = name.as_ref();
        validate_collection_name(name)?;
        self.db
            .tx(|tx| async move { tx.open_collection(self, name).await })
            .await
    }

    /// Reports whether a direct child is currently bound to `name`.
    pub async fn collection_exists(&self, name: impl AsRef<[u8]>) -> Result<bool, Error> {
        let name = name.as_ref();
        validate_collection_name(name)?;
        self.db
            .tx(|tx| async move { tx.collection_exists(self, name).await })
            .await
    }

    /// Strictly creates and binds a new direct child.
    pub async fn create_collection(&self, name: impl AsRef<[u8]>) -> Result<Collection, Error> {
        let name = name.as_ref();
        validate_collection_name(name)?;
        self.db
            .tx(|tx| async move { tx.create_collection(self, name).await })
            .await
    }

    /// Returns the direct child bound to `name`, creating it when absent.
    pub async fn create_collection_if_absent(
        &self,
        name: impl AsRef<[u8]>,
    ) -> Result<Collection, Error> {
        let name = name.as_ref();
        validate_collection_name(name)?;
        self.db
            .tx(|tx| async move { Ok(tx.create_collection_if_absent(self, name).await?.0) })
            .await
    }

    /// Returns an owned iterator over the collection's materialized keys.
    ///
    /// The listing runs inside a read-only serializable transaction. All I/O
    /// and validation complete before the iterator is returned, so iteration
    /// itself cannot fail and yields sorted raw keys.
    pub async fn iter_keys(&self) -> Result<KeyIter, Error> {
        Ok(KeyIter::new(
            self.scan_keys(KeyScan::all()).await?.into_keys(),
        ))
    }

    /// Materializes one serializable, sorted page of collection keys.
    pub async fn scan_keys(&self, scan: KeyScan<'_>) -> Result<KeyPage, Error> {
        self.db
            .tx(|tx| async move { tx.scan_keys(self, scan).await })
            .await
    }

    /// Returns an owned iterator over direct child bindings in raw-name order.
    ///
    /// All I/O and serializable validation complete before the iterator is
    /// returned. Each yielded handle remains bound to the listed incarnation.
    pub async fn iter_collections(&self) -> Result<CollectionIter, Error> {
        self.db
            .tx(|tx| async move { tx.iter_collections(self).await })
            .await
    }

    /// Non-recursively drops this exact collection incarnation.
    pub async fn drop_collection(&self) -> Result<(), Error> {
        self.db
            .tx(|tx| async move { tx.drop_collection(self).await })
            .await
    }

    /// Returns this handle's direct logical name, or `None` for the database root.
    pub fn name(&self) -> Option<&[u8]> {
        self.name.as_deref()
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
        name: &[u8],
        db: Arc<DbInner>,
    ) -> Self {
        Self {
            address,
            parent: Some(parent),
            name: Some(Arc::from(name)),
            db,
        }
    }

    pub(crate) fn address(&self) -> &CollectionAddress {
        &self.address
    }

    pub(crate) fn parent_address(&self) -> Option<&CollectionAddress> {
        self.parent.as_ref()
    }

    pub(crate) fn database_id(&self) -> DatabaseId {
        self.db.database_id
    }
}

pub(crate) fn validate_collection_name(name: &[u8]) -> Result<(), Error> {
    if name.is_empty() || name.len() > MAX_COLLECTION_NAME_BYTES {
        return Err(Error::InvalidInput(format!(
            "collection name must contain 1..={MAX_COLLECTION_NAME_BYTES} bytes"
        )));
    }
    Ok(())
}
