//! In-memory object state and transitions shared by fake S3 connections.

use std::collections::{BTreeSet, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Mutex;

use super::faults::FaultState;
use super::latency::LatencyModel;

#[derive(Clone, Default)]
pub(super) struct StoredObject {
    body: Vec<u8>,
    meta: HashMap<String, String>,
    etag: String,
}

impl StoredObject {
    pub(super) fn body(&self) -> &[u8] {
        &self.body
    }

    pub(super) fn meta(&self) -> &HashMap<String, String> {
        &self.meta
    }

    pub(super) fn etag(&self) -> &str {
        &self.etag
    }
}

pub(super) enum ReadObject<T> {
    Missing(T),
    NotModified(T),
    Found(T),
}

pub(super) enum HeadObject<T> {
    Missing(T),
    Found(T),
}

pub(super) enum PutObject<T> {
    AlreadyExists(T),
    EtagMismatch(T),
    Applied(String),
}

pub(super) struct PutRequest<'a> {
    pub(super) key: &'a str,
    pub(super) body: Vec<u8>,
    pub(super) meta: HashMap<String, String>,
    pub(super) if_match: Option<&'a str>,
    pub(super) if_none_match: Option<&'a str>,
}

pub(super) enum DeleteObject<T> {
    Missing(T),
    EtagMismatch(T),
    Deleted,
}

pub(super) struct ListedObjects {
    pub(super) contents: Vec<String>,
    pub(super) common: BTreeSet<String>,
    pub(super) truncated: bool,
}

#[derive(Default)]
pub(super) struct ObjectStore {
    objects: Mutex<HashMap<String, StoredObject>>,
}

impl ObjectStore {
    pub(super) fn get<T>(
        &self,
        key: &str,
        if_none_match: impl FnOnce() -> Option<String>,
        missing: impl FnOnce() -> T,
        not_modified: impl FnOnce(&str) -> T,
        found: impl FnOnce(&StoredObject) -> T,
    ) -> ReadObject<T> {
        let objects = self.objects.lock().unwrap();
        let Some(object) = objects.get(key) else {
            return ReadObject::Missing(missing());
        };
        if let Some(expected) = if_none_match()
            && expected == object.etag
        {
            return ReadObject::NotModified(not_modified(&object.etag));
        }
        ReadObject::Found(found(object))
    }

    pub(super) fn head<T>(
        &self,
        key: &str,
        missing: impl FnOnce() -> T,
        found: impl FnOnce(&StoredObject) -> T,
    ) -> HeadObject<T> {
        let objects = self.objects.lock().unwrap();
        match objects.get(key) {
            Some(object) => HeadObject::Found(found(object)),
            None => HeadObject::Missing(missing()),
        }
    }

    pub(super) fn put<T>(
        &self,
        request: PutRequest<'_>,
        already_exists: impl FnOnce() -> T,
        etag_mismatch: impl FnOnce() -> T,
    ) -> PutObject<T> {
        let mut objects = self.objects.lock().unwrap();
        let existing = objects.get(request.key);
        if request.if_none_match == Some("*") && existing.is_some() {
            return PutObject::AlreadyExists(already_exists());
        }
        if let Some(expected) = request.if_match {
            match existing {
                Some(object) if object.etag == expected => {}
                _ => return PutObject::EtagMismatch(etag_mismatch()),
            }
        }

        // Like real S3, the ETag of a (non-multipart) object is derived from its
        // content: identical bytes yield an identical ETag, and any content
        // change yields a new one. This is what makes ADR-023's nonce removal
        // safe — the body itself drives the CAS token.
        let etag = content_etag(&request.body);
        objects.insert(
            request.key.to_string(),
            StoredObject {
                body: request.body,
                meta: request.meta,
                etag: etag.clone(),
            },
        );
        PutObject::Applied(etag)
    }

    pub(super) fn delete<T>(
        &self,
        key: &str,
        if_match: Option<&str>,
        missing: impl FnOnce() -> T,
        etag_mismatch: impl FnOnce() -> T,
    ) -> DeleteObject<T> {
        let mut objects = self.objects.lock().unwrap();
        if let Some(expected) = if_match {
            let Some(object) = objects.get(key) else {
                return DeleteObject::Missing(missing());
            };
            if object.etag != expected {
                return DeleteObject::EtagMismatch(etag_mismatch());
            }
        }
        objects.remove(key);
        DeleteObject::Deleted
    }

    pub(super) fn list<T>(
        &self,
        prefix: &str,
        delimiter: &str,
        max_keys: usize,
        after: Option<&str>,
        render: impl FnOnce(ListedObjects) -> T,
    ) -> T {
        let objects = self.objects.lock().unwrap();
        let mut contents = Vec::new();
        let mut common = BTreeSet::new();
        for key in objects.keys() {
            let Some(rest) = key.strip_prefix(prefix) else {
                continue;
            };
            if !delimiter.is_empty()
                && let Some(index) = rest.find(delimiter)
            {
                common.insert(format!("{prefix}{}", &rest[..=index]));
                continue;
            }
            contents.push(key.clone());
        }
        contents.sort();
        if let Some(after) = after {
            contents.retain(|key| key.as_str() > after);
        }
        let truncated = contents.len() > max_keys;
        contents.truncate(max_keys);

        render(ListedObjects {
            contents,
            common,
            truncated,
        })
    }
}

pub(super) struct FakeState {
    pub(super) objects: ObjectStore,
    pub(super) faults: FaultState,
    pub(super) latency: Option<LatencyModel>,
}

impl FakeState {
    pub(super) fn new(latency: Option<LatencyModel>) -> Self {
        Self {
            objects: ObjectStore::default(),
            faults: FaultState::default(),
            latency,
        }
    }
}

/// A content-derived ETag, mirroring real S3 where a non-multipart object's
/// ETag is a hash of its bytes. A fixed-seed hasher keeps it deterministic
/// within the process (the exact value is opaque to the backend).
fn content_etag(body: &[u8]) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    body.hash(&mut hasher);
    format!("\"{:016x}\"", hasher.finish())
}
