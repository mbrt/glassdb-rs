//! A [`Backend`] decorator that counts operations. Ported from the Go
//! `statsBackend` in `stats.go`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;

use crate::{Backend, BackendError, ReadReply, Version};

/// Snapshot of backend operation counters.
///
/// The content-CAS-only trait (ADR-023) has no metadata-only operations, so the
/// counters track object reads, writes, and lists only.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackendStats {
    pub obj_reads: u64,
    pub obj_writes: u64,
    pub obj_lists: u64,
}

/// Wraps a backend and counts the operations performed on it.
pub struct StatsBackend {
    inner: Arc<dyn Backend>,
    obj_reads: AtomicU64,
    obj_writes: AtomicU64,
    obj_lists: AtomicU64,
}

impl StatsBackend {
    /// Wraps `inner` to count its operations.
    pub fn new(inner: Arc<dyn Backend>) -> Self {
        StatsBackend {
            inner,
            obj_reads: AtomicU64::new(0),
            obj_writes: AtomicU64::new(0),
            obj_lists: AtomicU64::new(0),
        }
    }

    /// Returns the current counters and resets them to zero.
    pub fn stats_and_reset(&self) -> BackendStats {
        BackendStats {
            obj_reads: self.obj_reads.swap(0, Ordering::Relaxed),
            obj_writes: self.obj_writes.swap(0, Ordering::Relaxed),
            obj_lists: self.obj_lists.swap(0, Ordering::Relaxed),
        }
    }
}

#[async_trait]
impl Backend for StatsBackend {
    async fn read(&self, path: &str) -> Result<ReadReply, BackendError> {
        self.obj_reads.fetch_add(1, Ordering::Relaxed);
        self.inner.read(path).await
    }

    async fn read_if_modified(
        &self,
        path: &str,
        expected: &Version,
    ) -> Result<ReadReply, BackendError> {
        self.obj_reads.fetch_add(1, Ordering::Relaxed);
        self.inner.read_if_modified(path, expected).await
    }

    async fn write(&self, path: &str, value: Vec<u8>) -> Result<Version, BackendError> {
        self.obj_writes.fetch_add(1, Ordering::Relaxed);
        self.inner.write(path, value).await
    }

    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BackendError> {
        self.obj_writes.fetch_add(1, Ordering::Relaxed);
        self.inner.write_if(path, value, expected).await
    }

    async fn write_if_not_exists(
        &self,
        path: &str,
        value: Vec<u8>,
    ) -> Result<Version, BackendError> {
        self.obj_writes.fetch_add(1, Ordering::Relaxed);
        self.inner.write_if_not_exists(path, value).await
    }

    async fn delete(&self, path: &str) -> Result<(), BackendError> {
        self.obj_writes.fetch_add(1, Ordering::Relaxed);
        self.inner.delete(path).await
    }

    async fn list(&self, dir_path: &str) -> Result<Vec<String>, BackendError> {
        self.obj_lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list(dir_path).await
    }
}
