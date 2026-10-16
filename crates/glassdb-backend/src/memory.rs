//! In-memory backend for testing and development (ADR-016, ADR-023, ADR-042).
//!
//! Content-CAS only: the opaque revision token is a hash of the object
//! contents, like the S3 ETag. Identical contents at one path therefore get the
//! same revision again, so tests see the revision ABA that the protocol must
//! tolerate (ADR-043).

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::implementation::{bind_list_cursor, list_provider_token};
use crate::{Backend, BackendError, ListCursor, ListLimit, ListPage, ReadReply, Revision};

#[derive(Clone)]
struct Object {
    data: Vec<u8>,
    hash: u64,
}

impl Object {
    fn new(data: Vec<u8>) -> Self {
        let hash = content_hash(&data);
        Object { data, hash }
    }

    fn revision(&self) -> Revision {
        Revision::new(format!("{:016x}", self.hash))
    }
}

/// Returns a hash that depends only on `data`.
///
/// The hash must not change between runs or Rust versions, because simulation
/// replays compare backend operations exactly. So this uses FNV-1a and not a
/// seeded or `std` hasher. A collision would look like a revision ABA, which
/// the protocol must tolerate anyway.
fn content_hash(data: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    data.iter().fold(OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(PRIME)
    })
}

#[derive(Clone)]
struct State {
    objects: HashMap<String, Object>,
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
            }),
        }
    }

    /// Returns an independent backend that starts with the current objects and
    /// revisions of this one.
    pub fn fork(&self) -> Self {
        MemoryBackend {
            state: Mutex::new(self.state.lock().unwrap().clone()),
        }
    }
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
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
        let obj = state.objects.get(path).ok_or(BackendError::NotFound)?;
        if &obj.revision() != expected {
            return Err(BackendError::Precondition);
        }
        let obj = Object::new(value);
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
        let obj = Object::new(value);
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
    async fn fork_keeps_revisions_and_diverges() {
        let original = MemoryBackend::new();
        let kept = original
            .write_if_not_exists("a", b"1".to_vec())
            .await
            .unwrap();
        let fork = original.fork();

        // Conditional writes that the original allowed are allowed on the fork.
        let read = fork.read("a").await.unwrap();
        assert_eq!(read.contents, b"1");
        assert_eq!(read.revision, kept);
        let forked = fork.write_if("a", b"2".to_vec(), &kept).await.unwrap();
        fork.write_if_not_exists("b", b"3".to_vec()).await.unwrap();

        // Neither backend sees the changes of the other.
        assert_eq!(original.read("a").await.unwrap().contents, b"1");
        assert!(matches!(
            original.read("b").await,
            Err(BackendError::NotFound)
        ));
        original.write_if("a", b"4".to_vec(), &kept).await.unwrap();
        assert_eq!(fork.read("a").await.unwrap().revision, forked);
    }

    // Like an S3 ETag, the revision depends only on the contents. Writing back
    // earlier contents restores the earlier revision.
    #[tokio::test]
    async fn identical_contents_get_the_same_revision_again() {
        let b = MemoryBackend::new();
        let first = b.write_if_not_exists("a", b"1".to_vec()).await.unwrap();
        let second = b.write_if("a", b"2".to_vec(), &first).await.unwrap();
        assert_ne!(first, second);
        let restored = b.write_if("a", b"1".to_vec(), &second).await.unwrap();
        assert_eq!(restored, first);

        b.delete_if("a", &restored).await.unwrap();
        let recreated = b.write_if_not_exists("a", b"1".to_vec()).await.unwrap();
        assert_eq!(recreated, first);
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
