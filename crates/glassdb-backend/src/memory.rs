//! In-memory backend for testing and development. Ported from the Go
//! `backend/memory` package. Version tokens use the `gen/metagen` format
//! matching the GCS backend.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use glassdb_concurr::Ctx;

use crate::{
    Backend, BackendError, LAST_WRITER_TAG, Metadata, ReadReply, Tags, Version, WriterId,
    encode_writer_tag,
};

#[derive(Clone, Default)]
struct Object {
    data: Vec<u8>,
    tags: Tags,
    generation: i64,
    metagen: i64,
}

impl Object {
    fn version(&self) -> Version {
        Version::new(format!("{}/{}", self.generation, self.metagen))
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

fn check_ctx(ctx: &Ctx) -> Result<(), BackendError> {
    ctx.err().map_err(|_| BackendError::Cancelled)
}

impl State {
    fn next_generation(&mut self) -> i64 {
        let res = self.next_gen;
        self.next_gen += 1;
        res
    }

    fn update_tags(obj: &mut Object, t: &Tags) {
        if t.is_empty() {
            return;
        }
        for (k, v) in t {
            obj.tags.insert(k.clone(), v.clone());
        }
        obj.metagen += 1;
    }

    fn update_data(&mut self, obj: &mut Object, d: Vec<u8>) {
        obj.data = d;
        obj.generation = self.next_generation();
        obj.metagen = 1;
    }
}

#[async_trait]
impl Backend for MemoryBackend {
    async fn read_if_modified(
        &self,
        ctx: &Ctx,
        path: &str,
        expected_writer: &WriterId,
    ) -> Result<ReadReply, BackendError> {
        check_ctx(ctx)?;
        let state = self.state.lock().unwrap();
        let obj = state.objects.get(path).ok_or(BackendError::NotFound)?;
        let current = obj
            .tags
            .get(LAST_WRITER_TAG)
            .map(String::as_str)
            .unwrap_or("");
        if current == encode_writer_tag(expected_writer) {
            return Err(BackendError::Precondition);
        }
        Ok(ReadReply {
            contents: obj.data.clone(),
            version: obj.version(),
            tags: obj.tags.clone(),
        })
    }

    async fn read(&self, ctx: &Ctx, path: &str) -> Result<ReadReply, BackendError> {
        check_ctx(ctx)?;
        let state = self.state.lock().unwrap();
        let obj = state.objects.get(path).ok_or(BackendError::NotFound)?;
        Ok(ReadReply {
            contents: obj.data.clone(),
            version: obj.version(),
            tags: obj.tags.clone(),
        })
    }

    async fn get_metadata(&self, ctx: &Ctx, path: &str) -> Result<Metadata, BackendError> {
        check_ctx(ctx)?;
        let state = self.state.lock().unwrap();
        let obj = state.objects.get(path).ok_or(BackendError::NotFound)?;
        Ok(Metadata {
            tags: obj.tags.clone(),
            version: obj.version(),
        })
    }

    async fn set_tags_if(
        &self,
        ctx: &Ctx,
        path: &str,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        check_ctx(ctx)?;
        let mut state = self.state.lock().unwrap();
        let mut obj = state
            .objects
            .get(path)
            .ok_or(BackendError::NotFound)?
            .clone();
        if &obj.version() != expected {
            return Err(BackendError::Precondition);
        }
        State::update_tags(&mut obj, &tags);
        let meta = Metadata {
            tags: obj.tags.clone(),
            version: obj.version(),
        };
        state.objects.insert(path.to_string(), obj);
        Ok(meta)
    }

    async fn write(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        check_ctx(ctx)?;
        let mut state = self.state.lock().unwrap();
        let mut obj = state.objects.get(path).cloned().unwrap_or_default();
        State::update_tags(&mut obj, &tags);
        state.update_data(&mut obj, value);
        let meta = Metadata {
            tags: obj.tags.clone(),
            version: obj.version(),
        };
        state.objects.insert(path.to_string(), obj);
        Ok(meta)
    }

    async fn write_if(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        check_ctx(ctx)?;
        let mut state = self.state.lock().unwrap();
        let mut obj = state
            .objects
            .get(path)
            .ok_or(BackendError::NotFound)?
            .clone();
        if &obj.version() != expected {
            return Err(BackendError::Precondition);
        }
        State::update_tags(&mut obj, &tags);
        state.update_data(&mut obj, value);
        let meta = Metadata {
            tags: obj.tags.clone(),
            version: obj.version(),
        };
        state.objects.insert(path.to_string(), obj);
        Ok(meta)
    }

    async fn write_if_not_exists(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        check_ctx(ctx)?;
        let mut state = self.state.lock().unwrap();
        if state.objects.contains_key(path) {
            return Err(BackendError::Precondition);
        }
        let mut obj = Object::default();
        State::update_tags(&mut obj, &tags);
        state.update_data(&mut obj, value);
        let meta = Metadata {
            tags: obj.tags.clone(),
            version: obj.version(),
        };
        state.objects.insert(path.to_string(), obj);
        Ok(meta)
    }

    async fn delete(&self, ctx: &Ctx, path: &str) -> Result<(), BackendError> {
        check_ctx(ctx)?;
        let mut state = self.state.lock().unwrap();
        if state.objects.remove(path).is_none() {
            return Err(BackendError::NotFound);
        }
        Ok(())
    }

    async fn delete_if(
        &self,
        ctx: &Ctx,
        path: &str,
        expected: &Version,
    ) -> Result<(), BackendError> {
        check_ctx(ctx)?;
        let mut state = self.state.lock().unwrap();
        let obj = state.objects.get(path).ok_or(BackendError::NotFound)?;
        if &obj.version() != expected {
            return Err(BackendError::Precondition);
        }
        state.objects.remove(path);
        Ok(())
    }

    async fn list(&self, ctx: &Ctx, dir_path: &str) -> Result<Vec<String>, BackendError> {
        check_ctx(ctx)?;
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

    fn ctx() -> Ctx {
        Ctx::background()
    }

    #[tokio::test]
    async fn write_read_delete() {
        let b = MemoryBackend::new();
        assert!(matches!(
            b.read(&ctx(), "a").await,
            Err(BackendError::NotFound)
        ));
        let m = b
            .write(&ctx(), "a", b"hello".to_vec(), Tags::new())
            .await
            .unwrap();
        assert_eq!(m.version.token, "1/1");
        let r = b.read(&ctx(), "a").await.unwrap();
        assert_eq!(r.contents, b"hello");
        b.delete(&ctx(), "a").await.unwrap();
        assert!(matches!(
            b.delete(&ctx(), "a").await,
            Err(BackendError::NotFound)
        ));
    }

    #[tokio::test]
    async fn write_if_not_exists_and_conditions() {
        let b = MemoryBackend::new();
        let m = b
            .write_if_not_exists(&ctx(), "a", b"v".to_vec(), Tags::new())
            .await
            .unwrap();
        assert!(matches!(
            b.write_if_not_exists(&ctx(), "a", b"v2".to_vec(), Tags::new())
                .await,
            Err(BackendError::Precondition)
        ));
        // WriteIf with wrong version fails.
        assert!(matches!(
            b.write_if(
                &ctx(),
                "a",
                b"v2".to_vec(),
                &Version::new("9/9"),
                Tags::new()
            )
            .await,
            Err(BackendError::Precondition)
        ));
        let m2 = b
            .write_if(&ctx(), "a", b"v2".to_vec(), &m.version, Tags::new())
            .await
            .unwrap();
        assert_ne!(m.version, m2.version);
    }

    #[tokio::test]
    async fn set_tags_bumps_metagen_and_read_if_modified() {
        let b = MemoryBackend::new();
        let mut tags = Tags::new();
        let writer = WriterId::new(vec![1, 2, 3]);
        tags.insert(LAST_WRITER_TAG.to_string(), encode_writer_tag(&writer));
        let m = b.write(&ctx(), "a", b"v".to_vec(), tags).await.unwrap();
        assert_eq!(m.version.token, "1/1");

        // ReadIfModified with the same writer => precondition (unchanged).
        assert!(matches!(
            b.read_if_modified(&ctx(), "a", &writer).await,
            Err(BackendError::Precondition)
        ));
        // ReadIfModified with a different writer => returns content.
        let other = WriterId::new(vec![9]);
        assert!(b.read_if_modified(&ctx(), "a", &other).await.is_ok());

        // SetTagsIf bumps metagen only.
        let mut t2 = Tags::new();
        t2.insert("k".to_string(), "v".to_string());
        let m2 = b.set_tags_if(&ctx(), "a", &m.version, t2).await.unwrap();
        assert_eq!(m2.version.token, "1/2");
    }

    #[tokio::test]
    async fn list_lexicographic_with_subdirs() {
        let b = MemoryBackend::new();
        for p in ["d/b", "d/a", "d/sub/x", "d/sub/y", "other/z"] {
            b.write(&ctx(), p, b"v".to_vec(), Tags::new())
                .await
                .unwrap();
        }
        let got = b.list(&ctx(), "d").await.unwrap();
        assert_eq!(
            got,
            vec!["d/a".to_string(), "d/b".to_string(), "d/sub".to_string()]
        );
    }
}
