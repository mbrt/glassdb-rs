//! Iterators over collection keys and sub-collections. Ported from the Go
//! `iter.go`. The backend returns the full listing up front, so these iterate
//! an in-memory snapshot.

use glassdb_data::paths;

use crate::error::Error;

/// Iterates over the keys in a collection.
pub struct KeysIter {
    items: std::vec::IntoIter<String>,
    prefix: String,
    err: Option<Error>,
}

impl KeysIter {
    pub(crate) fn new(items: Vec<String>) -> Self {
        KeysIter {
            items: items.into_iter(),
            prefix: String::new(),
            err: None,
        }
    }

    /// Returns the first error encountered during iteration, if any.
    pub fn err(&self) -> Option<&Error> {
        self.err.as_ref()
    }
}

impl Iterator for KeysIter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Vec<u8>> {
        let backend_path = self.items.next()?;
        if self.prefix.is_empty() {
            match paths::parse(&backend_path) {
                Ok(r) if r.typ == paths::Type::Key => self.prefix = format!("{}/", r.prefix),
                Ok(r) => {
                    self.err = Some(Error::Other(format!(
                        "got path type {:?}, expected Key",
                        r.typ
                    )));
                    return None;
                }
                Err(e) => {
                    self.err = Some(Error::Other(format!("parsing path {backend_path:?}: {e}")));
                    return None;
                }
            }
        }
        let trimmed = backend_path
            .strip_prefix(&self.prefix)
            .unwrap_or(&backend_path);
        match paths::to_key(trimmed) {
            Ok(k) => Some(k),
            Err(e) => {
                self.err = Some(Error::Other(e.to_string()));
                None
            }
        }
    }
}

/// Iterates over the sub-collections within a collection.
pub struct CollectionsIter {
    items: std::vec::IntoIter<String>,
    prefix: String,
    err: Option<Error>,
}

impl CollectionsIter {
    pub(crate) fn new(items: Vec<String>) -> Self {
        CollectionsIter {
            items: items.into_iter(),
            prefix: String::new(),
            err: None,
        }
    }

    /// Returns the first error encountered during iteration, if any.
    pub fn err(&self) -> Option<&Error> {
        self.err.as_ref()
    }
}

impl Iterator for CollectionsIter {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Vec<u8>> {
        let backend_path = self.items.next()?;
        // Listing directories can produce a trailing slash; remove it.
        let backend_path = backend_path.trim_end_matches('/').to_string();
        if self.prefix.is_empty() {
            match paths::parse(&backend_path) {
                Ok(r) if r.typ == paths::Type::Collection => self.prefix = format!("{}/", r.prefix),
                Ok(r) => {
                    self.err = Some(Error::Other(format!(
                        "got path type {:?}, expected Collection",
                        r.typ
                    )));
                    return None;
                }
                Err(e) => {
                    self.err = Some(Error::Other(format!("parsing path {backend_path:?}: {e}")));
                    return None;
                }
            }
        }
        let trimmed = backend_path
            .strip_prefix(&self.prefix)
            .unwrap_or(&backend_path);
        match paths::to_collection(trimmed) {
            Ok(c) => Some(c),
            Err(e) => {
                self.err = Some(Error::Other(e.to_string()));
                None
            }
        }
    }
}
