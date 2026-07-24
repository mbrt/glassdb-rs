//! Collection handles, unresolved collection paths, and standalone collection
//! management.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use glassdb_data::{CollectionAddress, KeyRef, MAX_COLLECTION_NAME_BYTES};
use glassdb_storage::Requirement;
use glassdb_trans::{Reader, Resolver};

use crate::db::{CreateMode, DbInner};
use crate::error::Error;
use crate::iter::{CollectionEntry, CollectionsIter, KeysIter};
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
    // TODO(ADR-047): Extend the handle with its direct parent address when
    // lifecycle operations need exact parent/name binding checks.
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
        let r = Reader::new(
            Resolver::new(self.db.shards.clone(), self.db.tmon.clone()),
            self.db.timeline.clone(),
            self.db.retry,
        );
        match r.read(&key, max_staleness).await {
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
        self.db.open_child(self, name).await
    }

    /// Reports whether a direct child is currently bound to `name`.
    pub async fn collection_exists(&self, name: impl AsRef<[u8]>) -> Result<bool, Error> {
        let name = name.as_ref();
        validate_collection_name(name)?;
        self.db.child_exists(self, name).await
    }

    /// Strictly creates and binds a new direct child.
    pub async fn create_collection(&self, name: impl AsRef<[u8]>) -> Result<Collection, Error> {
        let name = name.as_ref();
        validate_collection_name(name)?;
        self.db.create_child(self, name, CreateMode::Strict).await
    }

    /// Returns the direct child bound to `name`, creating it when absent.
    pub async fn create_collection_if_absent(
        &self,
        name: impl AsRef<[u8]>,
    ) -> Result<Collection, Error> {
        let name = name.as_ref();
        validate_collection_name(name)?;
        self.db.create_child(self, name, CreateMode::IfAbsent).await
    }

    /// Returns an iterator over the keys in the collection.
    ///
    /// The listing scans the keys in order. The scan runs inside a read-only
    /// serializable transaction and returns the keys in order.
    pub async fn keys(&self) -> Result<KeysIter, Error> {
        Ok(KeysIter::new(
            self.scan_keys(KeyScan::all()).await?.into_keys(),
        ))
    }

    /// Materializes one serializable, sorted page of collection keys.
    pub async fn scan_keys(&self, scan: KeyScan<'_>) -> Result<KeyPage, Error> {
        self.db
            .tx(|tx| async move { tx.scan_keys(self, scan).await })
            .await
    }

    /// Returns the direct child bindings in raw-name order.
    ///
    /// The returned handles remain bound to the listed incarnations even if a
    /// later lifecycle operation changes the logical names.
    pub async fn collections(&self) -> Result<CollectionsIter, Error> {
        let _guard = self.db.admit_operation()?;
        let requirement = Requirement::AtLeast(self.db.timeline.now());
        let (root, _) = self
            .db
            .shards
            .load_root(&self.address.physical_prefix(), requirement)
            .await
            .map_err(Error::from_read)?;
        let entries = root
            .children()
            .map(|(name, id)| {
                CollectionEntry::new(
                    name.to_vec(),
                    Collection::new_child(
                        CollectionAddress::new(self.db.name.as_str(), id),
                        name,
                        self.db.clone(),
                    ),
                )
            })
            .collect();
        Ok(CollectionsIter::new(entries))
    }

    /// Returns this handle's direct logical name, or `None` for the database root.
    pub fn name(&self) -> Option<&[u8]> {
        self.name.as_deref()
    }

    pub(crate) fn new_root(db: Arc<DbInner>) -> Self {
        Self {
            address: CollectionAddress::root(db.name.as_str()),
            name: None,
            db,
        }
    }

    pub(crate) fn new_child(address: CollectionAddress, name: &[u8], db: Arc<DbInner>) -> Self {
        Self {
            address,
            name: Some(Arc::from(name)),
            db,
        }
    }

    pub(crate) fn address(&self) -> &CollectionAddress {
        &self.address
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
