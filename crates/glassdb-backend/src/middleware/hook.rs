//! A [`Backend`] decorator for deterministic, operation-specific test hooks.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::{Backend, BackendError, ListCursor, ListLimit, ListPage, ReadReply, Version};

/// A backend operation presented to a hook before and after it is forwarded.
#[derive(Debug, Clone, Copy)]
pub enum BackendOp<'a> {
    /// A full object read.
    Read { path: &'a str },
    /// A version-conditional object read.
    ReadIfModified {
        path: &'a str,
        expected: &'a Version,
    },
    /// An unconditional object write.
    Write { path: &'a str, value: &'a [u8] },
    /// A compare-and-swap object write.
    WriteIf {
        path: &'a str,
        value: &'a [u8],
        expected: &'a Version,
    },
    /// A create-if-absent object write.
    WriteIfNotExists { path: &'a str, value: &'a [u8] },
    /// An unconditional object deletion.
    Delete { path: &'a str },
    /// One page of a prefix listing.
    List {
        path: &'a str,
        cursor: Option<&'a ListCursor>,
        limit: ListLimit,
    },
}

impl BackendOp<'_> {
    /// Returns the object path targeted by the operation.
    pub fn path(&self) -> &str {
        match self {
            BackendOp::Read { path }
            | BackendOp::ReadIfModified { path, .. }
            | BackendOp::Write { path, .. }
            | BackendOp::WriteIf { path, .. }
            | BackendOp::WriteIfNotExists { path, .. }
            | BackendOp::Delete { path }
            | BackendOp::List { path, .. } => path,
        }
    }
}

/// The inner backend's outcome presented to an after hook.
#[derive(Debug, Clone, Copy)]
pub enum HookOutcome<'a> {
    /// The operation succeeded.
    Success,
    /// The operation returned an error.
    Error(&'a BackendError),
}

impl HookOutcome<'_> {
    /// Reports whether the inner operation succeeded.
    pub fn is_success(self) -> bool {
        matches!(self, HookOutcome::Success)
    }
}

/// The future returned by a backend hook.
///
/// Before hooks return an error to skip the inner operation. After hooks return
/// an error to replace the inner outcome. The future is static, so hooks inspect
/// borrowed operation data before capturing owned state for asynchronous work.
pub type HookFuture = Pin<Box<dyn Future<Output = Result<(), BackendError>> + Send + 'static>>;

type BeforeHook = Arc<dyn for<'a> Fn(&BackendOp<'a>) -> HookFuture + Send + Sync>;
type AfterHook =
    Arc<dyn for<'a, 'b> Fn(&BackendOp<'a>, HookOutcome<'b>) -> HookFuture + Send + Sync>;

/// A [`Backend`] decorator that can intercept every operation before and after
/// it reaches an inner backend.
pub struct HookBackend {
    inner: Arc<dyn Backend>,
    before: Mutex<Option<BeforeHook>>,
    after: Mutex<Option<AfterHook>>,
}

impl HookBackend {
    /// Wraps `inner` with an initially unhooked backend.
    pub fn new(inner: Arc<dyn Backend>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            before: Mutex::new(None),
            after: Mutex::new(None),
        })
    }

    /// Replaces the hook run before each backend operation.
    pub fn set_before<F>(&self, before: F)
    where
        F: for<'a> Fn(&BackendOp<'a>) -> HookFuture + Send + Sync + 'static,
    {
        *self.before.lock().unwrap() = Some(Arc::new(before));
    }

    /// Removes the before hook.
    pub fn clear_before(&self) {
        *self.before.lock().unwrap() = None;
    }

    /// Replaces the hook run after each forwarded backend operation.
    pub fn set_after<F>(&self, after: F)
    where
        F: for<'a, 'b> Fn(&BackendOp<'a>, HookOutcome<'b>) -> HookFuture + Send + Sync + 'static,
    {
        *self.after.lock().unwrap() = Some(Arc::new(after));
    }

    /// Removes the after hook.
    pub fn clear_after(&self) {
        *self.after.lock().unwrap() = None;
    }

    async fn hooked<T, Fut>(
        &self,
        op: BackendOp<'_>,
        forward: impl FnOnce() -> Fut,
    ) -> Result<T, BackendError>
    where
        Fut: Future<Output = Result<T, BackendError>>,
    {
        let before = self.before.lock().unwrap().clone();
        if let Some(before) = before {
            before(&op).await?;
        }
        let result = forward().await;
        let outcome = match &result {
            Ok(_) => HookOutcome::Success,
            Err(error) => HookOutcome::Error(error),
        };
        let after = self.after.lock().unwrap().clone();
        if let Some(after) = after {
            after(&op, outcome).await?;
        }
        result
    }
}

#[async_trait]
impl Backend for HookBackend {
    async fn read(&self, path: &str) -> Result<ReadReply, BackendError> {
        self.hooked(BackendOp::Read { path }, || self.inner.read(path))
            .await
    }

    async fn read_if_modified(
        &self,
        path: &str,
        expected: &Version,
    ) -> Result<ReadReply, BackendError> {
        self.hooked(BackendOp::ReadIfModified { path, expected }, || {
            self.inner.read_if_modified(path, expected)
        })
        .await
    }

    async fn write(&self, path: &str, value: Vec<u8>) -> Result<Version, BackendError> {
        self.hooked(
            BackendOp::Write {
                path,
                value: &value,
            },
            || self.inner.write(path, value.clone()),
        )
        .await
    }

    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BackendError> {
        self.hooked(
            BackendOp::WriteIf {
                path,
                value: &value,
                expected,
            },
            || self.inner.write_if(path, value.clone(), expected),
        )
        .await
    }

    async fn write_if_not_exists(
        &self,
        path: &str,
        value: Vec<u8>,
    ) -> Result<Version, BackendError> {
        self.hooked(
            BackendOp::WriteIfNotExists {
                path,
                value: &value,
            },
            || self.inner.write_if_not_exists(path, value.clone()),
        )
        .await
    }

    async fn delete(&self, path: &str) -> Result<(), BackendError> {
        self.hooked(BackendOp::Delete { path }, || self.inner.delete(path))
            .await
    }

    async fn list(
        &self,
        prefix: &str,
        cursor: Option<&ListCursor>,
        limit: ListLimit,
    ) -> Result<ListPage, BackendError> {
        self.hooked(
            BackendOp::List {
                path: prefix,
                cursor,
                limit,
            },
            || self.inner.list(prefix, cursor, limit),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;
    use crate::memory::MemoryBackend;

    fn ready(result: Result<(), BackendError>) -> HookFuture {
        Box::pin(async move { result })
    }

    #[tokio::test]
    async fn before_hook_observes_every_operation() {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let seen = Arc::new(AtomicUsize::new(0));
        backend.set_before({
            let seen = seen.clone();
            move |op| {
                let bit = match op {
                    BackendOp::Read { .. } => 1 << 0,
                    BackendOp::ReadIfModified { .. } => 1 << 1,
                    BackendOp::Write { .. } => 1 << 2,
                    BackendOp::WriteIf { .. } => 1 << 3,
                    BackendOp::WriteIfNotExists { .. } => 1 << 4,
                    BackendOp::Delete { .. } => 1 << 5,
                    BackendOp::List { .. } => 1 << 6,
                };
                seen.fetch_or(bit, Ordering::SeqCst);
                ready(Ok(()))
            }
        });

        let version = backend.write("p", b"one".to_vec()).await.unwrap();
        backend.read("p").await.unwrap();
        assert!(matches!(
            backend.read_if_modified("p", &version).await,
            Err(BackendError::Precondition)
        ));
        let version = backend
            .write_if("p", b"two".to_vec(), &version)
            .await
            .unwrap();
        backend
            .write_if_not_exists("q", b"three".to_vec())
            .await
            .unwrap();
        backend.delete("p").await.unwrap();
        backend
            .list("", None, ListLimit::new(1).unwrap())
            .await
            .unwrap();
        assert!(!version.is_unset());
        assert_eq!(seen.load(Ordering::SeqCst), (1 << 7) - 1);
    }

    #[tokio::test]
    async fn before_error_prevents_the_operation_from_landing() {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        backend.set_before(|op| {
            ready(if matches!(op, BackendOp::Write { .. }) {
                Err(BackendError::Precondition)
            } else {
                Ok(())
            })
        });
        assert!(matches!(
            backend.write("p", b"value".to_vec()).await,
            Err(BackendError::Precondition)
        ));
        assert!(matches!(
            backend.read("p").await,
            Err(BackendError::NotFound)
        ));
    }

    #[tokio::test]
    async fn async_before_hook_gates_then_forwards() {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        backend.write("p", b"value".to_vec()).await.unwrap();
        let entered = Arc::new(AtomicBool::new(false));
        let released = Arc::new(AtomicBool::new(false));
        backend.set_before({
            let entered = entered.clone();
            let released = released.clone();
            move |op| {
                if !matches!(op, BackendOp::Read { .. }) {
                    return ready(Ok(()));
                }
                entered.store(true, Ordering::SeqCst);
                let released = released.clone();
                Box::pin(async move {
                    while !released.load(Ordering::SeqCst) {
                        tokio::task::yield_now().await;
                    }
                    Ok(())
                })
            }
        });
        let read = tokio::spawn({
            let backend = backend.clone();
            async move { backend.read("p").await }
        });
        while !entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        assert!(!read.is_finished());
        released.store(true, Ordering::SeqCst);
        assert_eq!(read.await.unwrap().unwrap().contents, b"value");
    }

    #[tokio::test]
    async fn after_error_overrides_a_landed_success() {
        let inner: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let backend = HookBackend::new(inner.clone());
        backend.set_after(|op, outcome| {
            ready(
                if matches!(op, BackendOp::WriteIfNotExists { .. }) && outcome.is_success() {
                    Err(BackendError::Unavailable("lost ack".into()))
                } else {
                    Ok(())
                },
            )
        });
        assert!(matches!(
            backend.write_if_not_exists("p", b"value".to_vec()).await,
            Err(BackendError::Unavailable(_))
        ));
        assert_eq!(inner.read("p").await.unwrap().contents, b"value");
        assert!(matches!(
            backend.write_if_not_exists("p", b"other".to_vec()).await,
            Err(BackendError::Precondition)
        ));
    }

    #[tokio::test]
    async fn hooks_can_reenter_without_holding_configuration_locks() {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        backend.write("seed", b"value".to_vec()).await.unwrap();
        let weak = Arc::downgrade(&backend);
        backend.set_after(move |op, _| {
            if !matches!(op, BackendOp::Write { path, .. } if *path == "outer") {
                return ready(Ok(()));
            }
            let backend = weak.upgrade().unwrap();
            Box::pin(async move {
                backend.read("seed").await?;
                Ok(())
            })
        });
        backend.write("outer", b"value".to_vec()).await.unwrap();
    }
}
