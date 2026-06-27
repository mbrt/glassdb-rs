//! In-memory backend for testing and development (ADR-016, ADR-023).
//!
//! Content-CAS only: the opaque version token is the object generation, bumped
//! on every content write. This matches the (now generation-only) GCS token,
//! so the in-memory backend keeps modelling production conditional-write
//! semantics.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::{Backend, BackendError, ReadReply, Version};

#[derive(Clone, Default)]
struct Object {
    data: Vec<u8>,
    generation: i64,
}

impl Object {
    fn version(&self) -> Version {
        Version::new(self.generation.to_string())
    }
}

struct State {
    objects: HashMap<String, Object>,
    next_gen: i64,
}

/// An in-memory implementation of [`Backend`].
pub struct MemoryBackend {
    state: Mutex<State>,
}

impl MemoryBackend {
    /// Creates a new, empty in-memory backend.
    pub fn new() -> Self {
        MemoryBackend {
            state: Mutex::new(State {
                objects: HashMap::new(),
                next_gen: 1,
            }),
        }
    }
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl State {
    fn next_generation(&mut self) -> i64 {
        let res = self.next_gen;
        self.next_gen += 1;
        res
    }

    fn update_data(&mut self, obj: &mut Object, d: Vec<u8>) {
        obj.data = d;
        obj.generation = self.next_generation();
    }
}

#[async_trait]
impl Backend for MemoryBackend {
    async fn read(&self, path: &str) -> Result<ReadReply, BackendError> {
        let state = self.state.lock().unwrap();
        let obj = state.objects.get(path).ok_or(BackendError::NotFound)?;
        Ok(ReadReply {
            contents: obj.data.clone(),
            version: obj.version(),
        })
    }

    async fn read_if_modified(
        &self,
        path: &str,
        expected: &Version,
    ) -> Result<ReadReply, BackendError> {
        let state = self.state.lock().unwrap();
        let obj = state.objects.get(path).ok_or(BackendError::NotFound)?;
        if &obj.version() == expected {
            return Err(BackendError::Precondition);
        }
        Ok(ReadReply {
            contents: obj.data.clone(),
            version: obj.version(),
        })
    }

    async fn write(&self, path: &str, value: Vec<u8>) -> Result<Version, BackendError> {
        let mut state = self.state.lock().unwrap();
        let mut obj = state.objects.get(path).cloned().unwrap_or_default();
        state.update_data(&mut obj, value);
        let version = obj.version();
        state.objects.insert(path.to_string(), obj);
        Ok(version)
    }

    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BackendError> {
        let mut state = self.state.lock().unwrap();
        let mut obj = state
            .objects
            .get(path)
            .ok_or(BackendError::NotFound)?
            .clone();
        if &obj.version() != expected {
            return Err(BackendError::Precondition);
        }
        state.update_data(&mut obj, value);
        let version = obj.version();
        state.objects.insert(path.to_string(), obj);
        Ok(version)
    }

    async fn write_if_not_exists(
        &self,
        path: &str,
        value: Vec<u8>,
    ) -> Result<Version, BackendError> {
        let mut state = self.state.lock().unwrap();
        if state.objects.contains_key(path) {
            return Err(BackendError::Precondition);
        }
        let mut obj = Object::default();
        state.update_data(&mut obj, value);
        let version = obj.version();
        state.objects.insert(path.to_string(), obj);
        Ok(version)
    }

    async fn delete(&self, path: &str) -> Result<(), BackendError> {
        let mut state = self.state.lock().unwrap();
        if state.objects.remove(path).is_none() {
            return Err(BackendError::NotFound);
        }
        Ok(())
    }

    async fn list(&self, dir_path: &str) -> Result<Vec<String>, BackendError> {
        let dir = if dir_path.ends_with('/') {
            dir_path.to_string()
        } else {
            format!("{dir_path}/")
        };
        let state = self.state.lock().unwrap();
        let mut set = std::collections::BTreeSet::new();
        for k in state.objects.keys() {
            if !k.starts_with(&dir) {
                continue;
            }
            let rest = &k[dir.len()..];
            match rest.find('/') {
                Some(idx) => {
                    set.insert(k[..dir.len() + idx].to_string());
                }
                None => {
                    set.insert(k.clone());
                }
            }
        }
        Ok(set.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn write_read_delete() {
        let b = MemoryBackend::new();
        assert!(matches!(b.read("a").await, Err(BackendError::NotFound)));
        let v = b.write("a", b"hello".to_vec()).await.unwrap();
        assert_eq!(&*v.token, "1");
        let r = b.read("a").await.unwrap();
        assert_eq!(r.contents, b"hello");
        b.delete("a").await.unwrap();
        assert!(matches!(b.delete("a").await, Err(BackendError::NotFound)));
    }

    #[tokio::test]
    async fn write_if_not_exists_and_conditions() {
        let b = MemoryBackend::new();
        let v = b.write_if_not_exists("a", b"v".to_vec()).await.unwrap();
        assert!(matches!(
            b.write_if_not_exists("a", b"v2".to_vec()).await,
            Err(BackendError::Precondition)
        ));
        // WriteIf with wrong version fails.
        assert!(matches!(
            b.write_if("a", b"v2".to_vec(), &Version::new("9")).await,
            Err(BackendError::Precondition)
        ));
        let v2 = b.write_if("a", b"v2".to_vec(), &v).await.unwrap();
        assert_ne!(v, v2);
    }

    #[tokio::test]
    async fn read_if_modified_tracks_version() {
        let b = MemoryBackend::new();
        let v = b.write("a", b"v".to_vec()).await.unwrap();

        // Same version => precondition (not modified).
        assert!(matches!(
            b.read_if_modified("a", &v).await,
            Err(BackendError::Precondition)
        ));
        // A stale version => returns the current content.
        let r = b.read_if_modified("a", &Version::new("0")).await.unwrap();
        assert_eq!(r.contents, b"v");

        // After a content write the version changes, so the old token no longer
        // matches and the body is returned.
        let v2 = b.write("a", b"v2".to_vec()).await.unwrap();
        assert_ne!(v, v2);
        let r = b.read_if_modified("a", &v).await.unwrap();
        assert_eq!(r.contents, b"v2");
        assert_eq!(r.version, v2);
    }

    #[tokio::test]
    async fn list_lexicographic_with_subdirs() {
        let b = MemoryBackend::new();
        for p in ["d/b", "d/a", "d/sub/x", "d/sub/y", "other/z"] {
            b.write(p, b"v".to_vec()).await.unwrap();
        }
        let got = b.list("d").await.unwrap();
        assert_eq!(
            got,
            vec!["d/a".to_string(), "d/b".to_string(), "d/sub".to_string()]
        );
    }
}
