//! A [`Backend`] decorator that injects deterministic delays before every
//! operation, driven by a byte sequence. Ported from the Go
//! `middleware.ScheduledBackend` / `Scheduler`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use glassdb_concurr::Ctx;

use crate::{Backend, BackendError, Metadata, ReadReply, Tags, Version, WriterId};

/// Produces deterministic delays from a byte sequence. Each call to the
/// scheduler consumes one byte and yields `byte * tick`; once the sequence is
/// exhausted it yields a zero delay. Safe for concurrent use.
pub struct Scheduler {
    state: Mutex<State>,
    tick: Duration,
}

struct State {
    data: Vec<u8>,
    pos: usize,
}

impl Scheduler {
    /// Creates a scheduler consuming `data`, one byte per operation, scaling
    /// each by `tick`.
    pub fn new(data: Vec<u8>, tick: Duration) -> Self {
        Scheduler {
            state: Mutex::new(State { data, pos: 0 }),
            tick,
        }
    }

    fn next(&self) -> u8 {
        let mut s = self.state.lock().unwrap();
        if s.pos < s.data.len() {
            let d = s.data[s.pos];
            s.pos += 1;
            d
        } else {
            0
        }
    }
}

/// A [`Backend`] decorator that waits a scheduler-determined delay before each
/// operation. Useful inside paused-time tests for deterministic, instant
/// operation ordering.
pub struct ScheduledBackend {
    inner: Arc<dyn Backend>,
    sched: Arc<Scheduler>,
}

impl ScheduledBackend {
    /// Wraps `inner`, delaying each operation by the next scheduler value.
    pub fn new(inner: Arc<dyn Backend>, sched: Arc<Scheduler>) -> Self {
        ScheduledBackend { inner, sched }
    }

    async fn wait(&self, ctx: &Ctx) {
        let d = self.sched.next();
        if d == 0 {
            return;
        }
        let dur = self.sched.tick * u32::from(d);
        tokio::select! {
            _ = tokio::time::sleep(dur) => {}
            _ = ctx.cancelled() => {}
        }
    }
}

#[async_trait]
impl Backend for ScheduledBackend {
    async fn read_if_modified(
        &self,
        ctx: &Ctx,
        path: &str,
        expected_writer: &WriterId,
    ) -> Result<ReadReply, BackendError> {
        self.wait(ctx).await;
        self.inner
            .read_if_modified(ctx, path, expected_writer)
            .await
    }

    async fn read(&self, ctx: &Ctx, path: &str) -> Result<ReadReply, BackendError> {
        self.wait(ctx).await;
        self.inner.read(ctx, path).await
    }

    async fn get_metadata(&self, ctx: &Ctx, path: &str) -> Result<Metadata, BackendError> {
        self.wait(ctx).await;
        self.inner.get_metadata(ctx, path).await
    }

    async fn set_tags_if(
        &self,
        ctx: &Ctx,
        path: &str,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.wait(ctx).await;
        self.inner.set_tags_if(ctx, path, expected, tags).await
    }

    async fn write(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.wait(ctx).await;
        self.inner.write(ctx, path, value, tags).await
    }

    async fn write_if(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.wait(ctx).await;
        self.inner.write_if(ctx, path, value, expected, tags).await
    }

    async fn write_if_not_exists(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.wait(ctx).await;
        self.inner.write_if_not_exists(ctx, path, value, tags).await
    }

    async fn delete(&self, ctx: &Ctx, path: &str) -> Result<(), BackendError> {
        self.wait(ctx).await;
        self.inner.delete(ctx, path).await
    }

    async fn delete_if(
        &self,
        ctx: &Ctx,
        path: &str,
        expected: &Version,
    ) -> Result<(), BackendError> {
        self.wait(ctx).await;
        self.inner.delete_if(ctx, path, expected).await
    }

    async fn list(&self, ctx: &Ctx, dir_path: &str) -> Result<Vec<String>, BackendError> {
        self.wait(ctx).await;
        self.inner.list(ctx, dir_path).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_consumes_bytes_then_zero() {
        let s = Scheduler::new(vec![3, 0, 7], Duration::from_millis(1));
        assert_eq!(s.next(), 3);
        assert_eq!(s.next(), 0);
        assert_eq!(s.next(), 7);
        // Exhausted: always zero.
        assert_eq!(s.next(), 0);
        assert_eq!(s.next(), 0);
    }
}
