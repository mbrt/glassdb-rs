//! A [`Backend`] decorator that counts operations. Ported from the Go
//! `statsBackend` in `stats.go`.

use std::ops::{AddAssign, Sub};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;

use crate::{Backend, BackendError, ListCursor, ListLimit, ListPage, ReadReply, Revision};

/// Snapshot of backend operation counters.
///
/// The conditional-only trait (ADR-042) has no metadata-only operations, so
/// the counters track object reads, mutations, and lists only. Deletes remain
/// part of `writes`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackendStats {
    /// Object reads, conditional or not.
    pub reads: u64,
    /// Object mutations, including conditional deletes.
    pub writes: u64,
    /// Object list requests.
    pub lists: u64,
}

impl AddAssign for BackendStats {
    fn add_assign(&mut self, rhs: Self) {
        // Destructuring fails to compile when a field is added and not summed.
        let Self {
            reads,
            writes,
            lists,
        } = rhs;
        self.reads += reads;
        self.writes += writes;
        self.lists += lists;
    }
}

impl Sub for BackendStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        Self {
            reads: self.reads.saturating_sub(rhs.reads),
            writes: self.writes.saturating_sub(rhs.writes),
            lists: self.lists.saturating_sub(rhs.lists),
        }
    }
}

/// Reads the cumulative count of writes that one [`StatsBackend`] sent.
///
/// The count only grows: [`StatsBackend::stats_and_reset`] does not reset it,
/// so several readers can each measure their own intervals.
#[derive(Debug, Clone)]
pub struct WriteMeter(Arc<AtomicU64>);

impl WriteMeter {
    /// Returns the number of writes sent so far.
    pub fn writes(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Wraps a backend and counts the operations performed on it.
pub struct StatsBackend {
    inner: Arc<dyn Backend>,
    reads: AtomicU64,
    writes: AtomicU64,
    lists: AtomicU64,
    total_writes: Arc<AtomicU64>,
}

impl StatsBackend {
    /// Wraps `inner` to count its operations.
    pub fn new(inner: Arc<dyn Backend>) -> Self {
        StatsBackend {
            inner,
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            lists: AtomicU64::new(0),
            total_writes: Arc::default(),
        }
    }

    /// Returns a handle that reads the cumulative write count.
    pub fn write_meter(&self) -> WriteMeter {
        WriteMeter(self.total_writes.clone())
    }

    fn count_write(&self) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.total_writes.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the current counters and resets them to zero.
    pub fn stats_and_reset(&self) -> BackendStats {
        BackendStats {
            reads: self.reads.swap(0, Ordering::Relaxed),
            writes: self.writes.swap(0, Ordering::Relaxed),
            lists: self.lists.swap(0, Ordering::Relaxed),
        }
    }
}

#[async_trait]
impl Backend for StatsBackend {
    async fn read(&self, path: &str) -> Result<ReadReply, BackendError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.read(path).await
    }

    async fn read_if_modified(
        &self,
        path: &str,
        expected: &Revision,
    ) -> Result<ReadReply, BackendError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.read_if_modified(path, expected).await
    }

    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Revision,
    ) -> Result<Revision, BackendError> {
        self.count_write();
        self.inner.write_if(path, value, expected).await
    }

    async fn write_if_not_exists(
        &self,
        path: &str,
        value: Vec<u8>,
    ) -> Result<Revision, BackendError> {
        self.count_write();
        self.inner.write_if_not_exists(path, value).await
    }

    async fn delete_if(&self, path: &str, expected: &Revision) -> Result<(), BackendError> {
        self.count_write();
        self.inner.delete_if(path, expected).await
    }

    async fn list(
        &self,
        prefix: &str,
        cursor: Option<&ListCursor>,
        limit: ListLimit,
    ) -> Result<ListPage, BackendError> {
        self.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix, cursor, limit).await
    }
}
