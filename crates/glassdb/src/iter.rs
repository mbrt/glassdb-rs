//! Iterators over collection keys and sub-collections. Ported from the Go
//! `iter.go`. The backend returns the full listing up front, so these iterate
//! an in-memory snapshot.

use glassdb_data::paths;

use crate::error::Error;

/// Iterates over the keys in a collection.
pub struct KeysIter {
    items: std::vec::IntoIter<String>,
    prefix: String,
    done: bool,
}

impl KeysIter {
    pub(crate) fn new(items: Vec<String>) -> Self {
        KeysIter {
            items: items.into_iter(),
            prefix: String::new(),
            done: false,
        }
    }
}

impl Iterator for KeysIter {
    type Item = Result<Vec<u8>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let backend_path = self.items.next()?;
        if self.prefix.is_empty() {
            match paths::parse(&backend_path) {
                Ok(r) if r.typ == paths::Type::Key => self.prefix = format!("{}/", r.prefix),
                Ok(r) => {
                    self.done = true;
                    return Some(Err(Error::internal(format!(
                        "got path type {:?}, expected Key",
                        r.typ
                    ))));
                }
                Err(e) => {
                    self.done = true;
                    return Some(Err(Error::with_source(
                        format!("parsing path {backend_path:?}"),
                        e,
                    )));
                }
            }
        }
        let trimmed = backend_path
            .strip_prefix(&self.prefix)
            .unwrap_or(&backend_path);
        match paths::to_key(trimmed) {
            Ok(k) => Some(Ok(k)),
            Err(e) => {
                self.done = true;
                Some(Err(Error::with_source(
                    format!("decoding key from path {trimmed:?}"),
                    e,
                )))
            }
        }
    }
}

/// Iterates over the sub-collections within a collection.
pub struct CollectionsIter {
    items: std::vec::IntoIter<String>,
    prefix: String,
    done: bool,
}

impl CollectionsIter {
    pub(crate) fn new(items: Vec<String>) -> Self {
        CollectionsIter {
            items: items.into_iter(),
            prefix: String::new(),
            done: false,
        }
    }
}

impl Iterator for CollectionsIter {
    type Item = Result<Vec<u8>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let backend_path = self.items.next()?;
        // Listing directories can produce a trailing slash; remove it.
        let backend_path = backend_path.trim_end_matches('/').to_string();
        if self.prefix.is_empty() {
            match paths::parse(&backend_path) {
                Ok(r) if r.typ == paths::Type::Collection => self.prefix = format!("{}/", r.prefix),
                Ok(r) => {
                    self.done = true;
                    return Some(Err(Error::internal(format!(
                        "got path type {:?}, expected Collection",
                        r.typ
                    ))));
                }
                Err(e) => {
                    self.done = true;
                    return Some(Err(Error::with_source(
                        format!("parsing path {backend_path:?}"),
                        e,
                    )));
                }
            }
        }
        let trimmed = backend_path
            .strip_prefix(&self.prefix)
            .unwrap_or(&backend_path);
        match paths::to_collection(trimmed) {
            Ok(c) => Some(Ok(c)),
            Err(e) => {
                self.done = true;
                Some(Err(Error::with_source(
                    format!("decoding collection from path {trimmed:?}"),
                    e,
                )))
            }
        }
    }
}
