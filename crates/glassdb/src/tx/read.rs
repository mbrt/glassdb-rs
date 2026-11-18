//! The handle of a stale read-only transaction (ADR-097).

use crate::collection::{Collection, CollectionPath};
use crate::error::Error;
use crate::iter::CollectionIter;
use crate::scan::{KeyPage, KeyScan};
use crate::tx::Transaction;

/// A stale read-only transaction, which [`crate::Database::read_tx`] runs.
///
/// It has the read operations of [`Transaction`] and no write operation. The
/// body sees the state of the database at one cut point. Reads are tracked
/// and repeatable as in a [`Transaction`].
pub struct ReadTransaction(Transaction);

impl ReadTransaction {
    pub(crate) fn new(tx: Transaction) -> Self {
        Self(tx)
    }

    /// Reads the value for `key`, or returns `None` when the key is absent.
    ///
    /// Takes `&self`, so several reads can run concurrently.
    pub async fn read(&self, c: &Collection, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.0.read(c, key).await
    }

    /// Materializes one sorted page of keys.
    pub async fn scan_keys(&self, c: &Collection, scan: KeyScan<'_>) -> Result<KeyPage, Error> {
        self.0.scan_keys(c, scan).await
    }

    /// Returns the handle to the root collection.
    pub fn root_collection(&self) -> Collection {
        self.0.root_collection()
    }

    /// Opens the direct child bound to `name`.
    pub async fn open_collection(
        &self,
        parent: &Collection,
        name: impl AsRef<[u8]>,
    ) -> Result<Collection, Error> {
        self.0.open_collection(parent, name).await
    }

    /// Reports whether a direct child is bound to `name`.
    pub async fn collection_exists(
        &self,
        parent: &Collection,
        name: impl AsRef<[u8]>,
    ) -> Result<bool, Error> {
        self.0.collection_exists(parent, name).await
    }

    /// Resolves a collection path from the permanent root.
    pub async fn open_collection_path<P>(&self, path: P) -> Result<Collection, Error>
    where
        P: TryInto<CollectionPath>,
        P::Error: Into<Error>,
    {
        self.0.open_collection_path(path).await
    }

    /// Reports whether every component of a collection path is bound.
    pub async fn collection_path_exists<P>(&self, path: P) -> Result<bool, Error>
    where
        P: TryInto<CollectionPath>,
        P::Error: Into<Error>,
    {
        self.0.collection_path_exists(path).await
    }

    /// Returns an owned iterator over the direct child bindings in raw-name
    /// order.
    pub async fn iter_collections(&self, parent: &Collection) -> Result<CollectionIter, Error> {
        self.0.iter_collections(parent).await
    }

    /// Returns the error for an explicit abort.
    pub fn abort(&self) -> Result<(), Error> {
        self.0.abort()
    }
}
