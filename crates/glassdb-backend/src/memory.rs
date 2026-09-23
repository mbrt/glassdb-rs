//! In-memory backend for testing and development (ADR-016, ADR-023, ADR-042).
//!
//! Content-CAS only: the opaque revision token is the object generation, bumped
//! on every content write. This matches the (now generation-only) GCS token,
//! so the in-memory backend keeps modelling production conditional-mutation
//! semantics.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::implementation::{bind_list_cursor, list_provider_token};
use crate::{Backend, BackendError, ListCursor, ListLimit, ListPage, ReadReply, Revision};

#[derive(Clone, Default)]
struct Object {
    data: Vec<u8>,
    generation: i64,
}

impl Object {
    fn revision(&self) -> Revision {
        Revision::new(self.generation.to_string())
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
            revision: obj.revision(),
        })
    }

    async fn read_if_modified(
        &self,
        path: &str,
        expected: &Revision,
    ) -> Result<ReadReply, BackendError> {
        let state = self.state.lock().unwrap();
        let obj = state.objects.get(path).ok_or(BackendError::NotFound)?;
        if &obj.revision() == expected {
            return Err(BackendError::Precondition);
        }
        Ok(ReadReply {
            contents: obj.data.clone(),
            revision: obj.revision(),
        })
    }

    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Revision,
    ) -> Result<Revision, BackendError> {
        let mut state = self.state.lock().unwrap();
        let mut obj = state
            .objects
            .get(path)
            .ok_or(BackendError::NotFound)?
            .clone();
        if &obj.revision() != expected {
            return Err(BackendError::Precondition);
        }
        state.update_data(&mut obj, value);
        let revision = obj.revision();
        state.objects.insert(path.to_string(), obj);
        Ok(revision)
    }

    async fn write_if_not_exists(
        &self,
        path: &str,
        value: Vec<u8>,
    ) -> Result<Revision, BackendError> {
        let mut state = self.state.lock().unwrap();
        if state.objects.contains_key(path) {
            return Err(BackendError::Precondition);
        }
        let mut obj = Object::default();
        state.update_data(&mut obj, value);
        let revision = obj.revision();
        state.objects.insert(path.to_string(), obj);
        Ok(revision)
    }

    async fn delete_if(&self, path: &str, expected: &Revision) -> Result<(), BackendError> {
        let mut state = self.state.lock().unwrap();
        let object = state.objects.get(path).ok_or(BackendError::NotFound)?;
        if &object.revision() != expected {
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
        let after = list_provider_token(prefix, cursor)?
            .map(|cursor| decode_memory_list_cursor(prefix, cursor))
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
            objects
                .last()
                .map(|last| encode_memory_list_cursor(prefix, last))
                .transpose()?
        } else {
            None
        };
        Ok(ListPage { objects, next })
    }
}

fn encode_memory_list_cursor(prefix: &str, last: &str) -> Result<ListCursor, BackendError> {
    bind_list_cursor(prefix, &format!("m:{last}"))
}

fn decode_memory_list_cursor<'a>(
    prefix: &str,
    provider_token: &'a str,
) -> Result<&'a str, BackendError> {
    let last = provider_token
        .strip_prefix("m:")
        .ok_or(BackendError::InvalidCursor)?;
    if !last.starts_with(prefix) {
        return Err(BackendError::InvalidCursor);
    }
    Ok(last)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn backend_conformance() {
        crate::implementation::assert_backend_conformance(&MemoryBackend::new()).await;
    }

    #[tokio::test]
    async fn list_rejects_invalid_provider_cursor() {
        let b = MemoryBackend::new();
        let cursor = bind_list_cursor("prefix/", "invalid").unwrap();
        let result = b.list("prefix/", Some(&cursor), ListLimit::MIN).await;
        assert!(
            matches!(result, Err(BackendError::InvalidCursor)),
            "got {result:?}"
        );
    }
}
