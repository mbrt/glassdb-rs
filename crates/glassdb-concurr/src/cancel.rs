//! A hierarchical cancellation token built on `tokio::sync::Notify`.
//!
//! This replaces `tokio_util::sync::CancellationToken`. `tokio_util` brings in
//! machinery tied to tokio's own runtime, whereas `Notify` is part of
//! `tokio::sync`, which is runtime-agnostic and runs unchanged on the in-repo
//! deterministic executor (`--cfg sim`, ADR-011), so a token built on it works
//! identically in production and under simulation.
//!
//! It is deliberately *not* built on `tokio::sync::watch`: `watch` shards its
//! internal notifier and picks a shard with `tokio`'s thread-local RNG, which
//! the simulation executor does not control. That RNG persists across runs on
//! the same thread, so a watch-based token would make scheduling non-reproducible
//! (see ADR-008). `Notify` draws no randomness, keeping every `cancelled()` await
//! deterministic.
//!
//! Semantics mirror the subset of `CancellationToken` the codebase uses: a
//! token is cancelled when it, or any of its ancestors, is cancelled.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

/// A clonable cancellation signal with parent/child propagation.
#[derive(Clone)]
pub struct CancelToken {
    inner: Arc<Inner>,
}

struct Inner {
    cancelled: AtomicBool,
    notify: Notify,
    parent: Option<CancelToken>,
}

impl Inner {
    fn root(parent: Option<CancelToken>) -> Arc<Self> {
        Arc::new(Inner {
            cancelled: AtomicBool::new(false),
            notify: Notify::new(),
            parent,
        })
    }
}

impl CancelToken {
    /// Creates a fresh, uncancelled root token.
    pub fn new() -> Self {
        CancelToken {
            inner: Inner::root(None),
        }
    }

    /// Creates a child token. The child is cancelled when it is cancelled
    /// directly or when any ancestor is cancelled.
    pub fn child_token(&self) -> CancelToken {
        CancelToken {
            inner: Inner::root(Some(self.clone())),
        }
    }

    /// Cancels this token (and, transitively, anything that observes it as an
    /// ancestor). Idempotent.
    pub fn cancel(&self) {
        // Wake waiters only on the first transition to cancelled.
        if !self.inner.cancelled.swap(true, Ordering::SeqCst) {
            self.inner.notify.notify_waiters();
        }
    }

    /// Reports whether this token or any ancestor has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        if self.inner.cancelled.load(Ordering::SeqCst) {
            return true;
        }
        match &self.inner.parent {
            Some(p) => p.is_cancelled(),
            None => false,
        }
    }

    /// Resolves once this token or any ancestor is cancelled. Returns
    /// immediately if already cancelled.
    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        // Race a notification from this token and every ancestor; the first to
        // flip wins. `notify_waiters` only wakes waiters registered before the
        // send, so each branch *enables* its waiter and only then re-checks the
        // flag, closing the gap against a concurrent `cancel`.
        let mut tokens: Vec<&CancelToken> = Vec::new();
        let mut cur = Some(self);
        while let Some(t) = cur {
            tokens.push(t);
            cur = t.inner.parent.as_ref();
        }
        let futs = tokens.into_iter().map(|t| {
            Box::pin(async move {
                let notified = t.inner.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if t.inner.cancelled.load(Ordering::SeqCst) {
                    return;
                }
                notified.await;
            })
        });
        let _ = futures::future::select_all(futs).await;
    }
}

impl Default for CancelToken {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancel_sets_flag_and_wakes() {
        let t = CancelToken::new();
        assert!(!t.is_cancelled());
        let t2 = t.clone();
        t2.cancel();
        assert!(t.is_cancelled());
        // Already cancelled: returns immediately.
        t.cancelled().await;
    }

    #[tokio::test]
    async fn child_observes_parent_cancel() {
        let parent = CancelToken::new();
        let child = parent.child_token();
        assert!(!child.is_cancelled());
        parent.cancel();
        assert!(child.is_cancelled());
        child.cancelled().await;
    }

    #[tokio::test]
    async fn child_cancel_does_not_affect_parent() {
        let parent = CancelToken::new();
        let child = parent.child_token();
        child.cancel();
        assert!(child.is_cancelled());
        assert!(!parent.is_cancelled());
    }

    #[tokio::test]
    async fn cancelled_awaits_until_signal() {
        let t = CancelToken::new();
        let t2 = t.clone();
        let waiter = tokio::spawn(async move { t2.cancelled().await });
        tokio::task::yield_now().await;
        t.cancel();
        waiter.await.unwrap();
    }
}
