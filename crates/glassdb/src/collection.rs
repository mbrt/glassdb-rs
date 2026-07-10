//! A named group of key-value pairs. Ported from the Go `collection.go`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use glassdb_backend::Backend;
use glassdb_data::paths;
use glassdb_storage::CollectionRoot;
use glassdb_trans::Reader;

use crate::db::DbInner;
use crate::error::Error;
use crate::iter::{CollectionsIter, KeysIter};

/// A named group of key-value pairs within a database.
#[derive(Clone)]
pub struct Collection {
    prefix: String,
    db: Arc<DbInner>,
}

impl Collection {
    /// Reads the value for `key` with strong (serializable) consistency.
    pub async fn read(&self, key: &[u8]) -> Result<Vec<u8>, Error> {
        let res: Option<Vec<u8>> = self
            .db
            .tx(|tx| async move {
                match tx.read(self, key).await {
                    Ok(v) => Ok(Some(v)),
                    // We must still validate the transaction even when not found.
                    Err(Error::NotFound) => Ok(None),
                    Err(e) => Err(e),
                }
            })
            .await?;
        res.ok_or(Error::NotFound)
    }

    /// Reads the value for `key` allowing stale results up to `max_staleness`.
    pub async fn read_stale(&self, key: &[u8], max_staleness: Duration) -> Result<Vec<u8>, Error> {
        let p = paths::from_key(&self.prefix, key);
        let r = Reader::new(
            self.db.values.clone(),
            self.db.shards.clone(),
            self.db.tmon.clone(),
            self.db.retry,
        );
        match r.read(&p, max_staleness).await {
            Ok(rv) => Ok(rv.value.to_vec()),
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
                    let old = tx.read(self, key).await?;
                    let newb = (f.lock().unwrap())(old)?;
                    tx.write(self, key, &newb)?;
                    Ok(newb)
                }
            })
            .await
    }

    /// Returns a sub-collection with the given name.
    pub fn collection(&self, name: &[u8]) -> Collection {
        let p = paths::from_collection(&self.prefix, name);
        self.db.open_collection(p)
    }

    /// Ensures the collection exists, creating it if necessary.
    pub async fn create(&self) -> Result<(), Error> {
        let root = CollectionRoot::new();
        self.db
            .shards
            .create_root(&self.prefix, &root)
            .await
            .map(|_| ())
            .map_err(Error::from)
    }

    /// Returns an iterator over the keys in the collection.
    ///
    /// The listing scans the keys in order. The scan runs inside a read-only
    /// serializable transaction and returns the keys in order.
    pub async fn keys(&self) -> Result<KeysIter, Error> {
        let this = self.clone();
        let keys = self
            .db
            .tx(move |tx| {
                let this = this.clone();
                async move { tx.keys(&this).await }
            })
            .await?;
        Ok(KeysIter::new(keys))
    }

    /// Returns an iterator over the sub-collections in this collection.
    pub async fn collections(&self) -> Result<CollectionsIter, Error> {
        let cprefix = paths::collections_prefix(&self.prefix);
        let items = self
            .db
            .backend
            .list(&cprefix)
            .await
            .map_err(|e| Error::from_read(e.into()))?;
        Ok(CollectionsIter::new(items))
    }

    pub(crate) fn new(prefix: String, db: Arc<DbInner>) -> Self {
        Collection { prefix, db }
    }

    pub(crate) fn prefix(&self) -> &str {
        &self.prefix
    }
}
