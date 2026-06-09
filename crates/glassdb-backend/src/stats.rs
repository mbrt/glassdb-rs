//! A [`Backend`] decorator that counts operations. Ported from the Go
//! `statsBackend` in `stats.go`.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use async_trait::async_trait;

use crate::{Backend, BackendError, Metadata, ReadReply, Tags, Version, WriterId};

/// Snapshot of backend operation counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackendStats {
    pub meta_reads: i64,
    pub meta_writes: i64,
    pub obj_reads: i64,
    pub obj_writes: i64,
    pub obj_lists: i64,
}

/// Wraps a backend and counts the operations performed on it.
pub struct StatsBackend {
    inner: Arc<dyn Backend>,
    meta_reads: AtomicI64,
    meta_writes: AtomicI64,
    obj_reads: AtomicI64,
    obj_writes: AtomicI64,
    obj_lists: AtomicI64,
}

impl StatsBackend {
    /// Wraps `inner` to count its operations.
    pub fn new(inner: Arc<dyn Backend>) -> Self {
        StatsBackend {
            inner,
            meta_reads: AtomicI64::new(0),
            meta_writes: AtomicI64::new(0),
            obj_reads: AtomicI64::new(0),
            obj_writes: AtomicI64::new(0),
            obj_lists: AtomicI64::new(0),
        }
    }

    /// Returns the current counters and resets them to zero.
    pub fn stats_and_reset(&self) -> BackendStats {
        BackendStats {
            meta_reads: self.meta_reads.swap(0, Ordering::Relaxed),
            meta_writes: self.meta_writes.swap(0, Ordering::Relaxed),
            obj_reads: self.obj_reads.swap(0, Ordering::Relaxed),
            obj_writes: self.obj_writes.swap(0, Ordering::Relaxed),
            obj_lists: self.obj_lists.swap(0, Ordering::Relaxed),
        }
    }
}

#[async_trait]
impl Backend for StatsBackend {
    async fn read_if_modified(
        &self,
        path: &str,
        expected_writer: &WriterId,
    ) -> Result<ReadReply, BackendError> {
        self.obj_reads.fetch_add(1, Ordering::Relaxed);
        self.inner.read_if_modified(path, expected_writer).await
    }

    async fn read(&self, path: &str) -> Result<ReadReply, BackendError> {
        self.obj_reads.fetch_add(1, Ordering::Relaxed);
        self.inner.read(path).await
    }

    async fn get_metadata(&self, path: &str) -> Result<Metadata, BackendError> {
        self.meta_reads.fetch_add(1, Ordering::Relaxed);
        self.inner.get_metadata(path).await
    }

    async fn set_tags_if(
        &self,
        path: &str,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.meta_writes.fetch_add(1, Ordering::Relaxed);
        self.inner.set_tags_if(path, expected, tags).await
    }

    async fn write(
        &self,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.obj_writes.fetch_add(1, Ordering::Relaxed);
        self.inner.write(path, value, tags).await
    }

    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.obj_writes.fetch_add(1, Ordering::Relaxed);
        self.inner.write_if(path, value, expected, tags).await
    }

    async fn write_if_not_exists(
        &self,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.obj_writes.fetch_add(1, Ordering::Relaxed);
        self.inner.write_if_not_exists(path, value, tags).await
    }

    async fn delete(&self, path: &str) -> Result<(), BackendError> {
        self.obj_writes.fetch_add(1, Ordering::Relaxed);
        self.inner.delete(path).await
    }

    async fn delete_if(&self, path: &str, expected: &Version) -> Result<(), BackendError> {
        self.obj_writes.fetch_add(1, Ordering::Relaxed);
        self.inner.delete_if(path, expected).await
    }

    async fn list(&self, dir_path: &str) -> Result<Vec<String>, BackendError> {
        self.obj_lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list(dir_path).await
    }
}
