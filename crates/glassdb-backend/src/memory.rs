//! In-memory backend for testing and development (ADR-016, ADR-023, ADR-042).
//!
//! Content-CAS only: the opaque version token is the object generation, bumped
//! on every content write. This matches the (now generation-only) GCS token,
//! so the in-memory backend keeps modelling production conditional-mutation
//! semantics.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::{Backend, BackendError, ListCursor, ListLimit, ListPage, ReadReply, Version};

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

    async fn delete_if(&self, path: &str, expected: &Version) -> Result<(), BackendError> {
        let mut state = self.state.lock().unwrap();
        let object = state.objects.get(path).ok_or(BackendError::NotFound)?;
        if &object.version() != expected {
            return Err(BackendError::Precondition);
        }
        state.objects.remove(path);
        Ok(())
    }

    async fn list(
        &self,
        prefix: &str,
        cursor: Option<&ListCursor>,
        limit: ListLimit,
    ) -> Result<ListPage, BackendError> {
        validate_list_prefix(prefix)?;
        let after = cursor
            .map(|cursor| decode_list_cursor(prefix, cursor))
            .transpose()?;
        let state = self.state.lock().unwrap();
        let mut matches: Vec<&str> = state
            .objects
            .keys()
            .map(String::as_str)
            .filter(|key| key.starts_with(prefix))
            .filter(|key| after.is_none_or(|after| *key > after))
            .collect();
        matches.sort_unstable();

        let has_more = matches.len() > limit.get();
        let objects: Vec<String> = matches
            .into_iter()
            .take(limit.get())
            .map(str::to_string)
            .collect();
        let next = if has_more {
            objects.last().map(|last| encode_list_cursor(prefix, last))
        } else {
            None
        };
        Ok(ListPage { objects, next })
    }
}

fn validate_list_prefix(prefix: &str) -> Result<(), BackendError> {
    if prefix.is_empty() || prefix.ends_with('/') {
        Ok(())
    } else {
        Err(BackendError::other(format!(
            "list prefix must be empty or end in '/': {prefix:?}"
        )))
    }
}

fn encode_list_cursor(prefix: &str, last: &str) -> ListCursor {
    ListCursor::new(format!("{}:{prefix}{last}", prefix.len()))
}

fn decode_list_cursor<'a>(prefix: &str, cursor: &'a ListCursor) -> Result<&'a str, BackendError> {
    let (prefix_len, body) = cursor
        .as_str()
        .split_once(':')
        .ok_or(BackendError::InvalidCursor)?;
    let prefix_len = prefix_len
        .parse::<usize>()
        .map_err(|_| BackendError::InvalidCursor)?;
    let stored_prefix = body.get(..prefix_len).ok_or(BackendError::InvalidCursor)?;
    let last = body.get(prefix_len..).ok_or(BackendError::InvalidCursor)?;
    if stored_prefix != prefix || !last.starts_with(prefix) {
        return Err(BackendError::InvalidCursor);
    }
    Ok(last)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_read_delete_if() {
        let b = MemoryBackend::new();
        assert!(matches!(b.read("a").await, Err(BackendError::NotFound)));
        let v = b.write_if_not_exists("a", b"hello".to_vec()).await.unwrap();
        assert_eq!(&*v.token, "1");
        let r = b.read("a").await.unwrap();
        assert_eq!(r.contents, b"hello");
        b.delete_if("a", &v).await.unwrap();
        assert!(matches!(
            b.delete_if("a", &v).await,
            Err(BackendError::NotFound)
        ));
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
        let v = b.write_if_not_exists("a", b"v".to_vec()).await.unwrap();

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
        let v2 = b.write_if("a", b"v2".to_vec(), &v).await.unwrap();
        assert_ne!(v, v2);
        let r = b.read_if_modified("a", &v).await.unwrap();
        assert_eq!(r.contents, b"v2");
        assert_eq!(r.version, v2);
    }

    #[tokio::test]
    async fn list_is_recursive_and_paginated() {
        let b = MemoryBackend::new();
        for p in ["d/b", "d/a", "d/sub/x", "d/sub/y", "other/z"] {
            b.write_if_not_exists(p, b"v".to_vec()).await.unwrap();
        }
        let limit = ListLimit::new(2).unwrap();
        let first = b.list("d/", None, limit).await.unwrap();
        assert_eq!(first.objects, vec!["d/a", "d/b"]);
        let second = b.list("d/", first.next.as_ref(), limit).await.unwrap();
        assert_eq!(second.objects, vec!["d/sub/x", "d/sub/y"]);
        assert!(second.next.is_none());

        let err = b
            .list("other/", first.next.as_ref(), limit)
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::InvalidCursor));
    }

    #[tokio::test]
    async fn stale_delete_cannot_remove_recreated_state() {
        let b = MemoryBackend::new();
        let old = b.write_if_not_exists("a", b"old".to_vec()).await.unwrap();
        b.delete_if("a", &old).await.unwrap();
        let current = b
            .write_if_not_exists("a", b"current".to_vec())
            .await
            .unwrap();

        assert!(matches!(
            b.delete_if("a", &old).await,
            Err(BackendError::Precondition)
        ));
        let read = b.read("a").await.unwrap();
        assert_eq!(read.contents, b"current");
        assert_eq!(read.version, current);
    }
}
