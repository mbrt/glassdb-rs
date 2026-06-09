//! A minimal one-shot cancellation primitive.
//!
//! [`AbortSignal`] is an `AtomicBool` plus a [`tokio::sync::Notify`]: one
//! `cancel()` flips the flag and wakes every waiter on [`AbortSignal::cancelled`].
//! Used as a wakeup channel for places that need to drop a future from
//! outside — e.g. the sim `rt::JoinHandle::abort`, the dedup machinery
//! tearing down running rounds on `Dedup::close`, and the simulation
//! harness's crash nemesis. The convention everywhere is the same:
//! `select!` the abort signal against the work; the losing arm is dropped.

use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

/// One-shot cancellation flag with an async wakeup. Cheap to allocate
/// (one `AtomicBool` + one `Notify`) and has no parent/child machinery —
/// callers `select!` it against the future they want to drop.
pub struct AbortSignal {
    cancelled: AtomicBool,
    notify: Notify,
}

impl AbortSignal {
    /// Creates a fresh, un-cancelled signal.
    pub fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    /// Flips the flag and wakes every current [`AbortSignal::cancelled`] waiter.
    /// Subsequent calls are no-ops.
    pub fn cancel(&self) {
        if !self.cancelled.swap(true, Ordering::SeqCst) {
            self.notify.notify_waiters();
        }
    }

    /// Whether [`AbortSignal::cancel`] has been observed.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// Resolves the moment the signal is (or becomes) cancelled. Safe to
    /// poll from multiple tasks: every waiter is woken by `notify_waiters`.
    pub async fn cancelled(&self) {
        if self.cancelled.load(Ordering::SeqCst) {
            return;
        }
        // Register the waiter *before* the second flag check, so a racing
        // `cancel()` either flips the flag (we observe it below) or fires
        // `notify_waiters()` after we registered (we wake).
        let notified = self.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.cancelled.load(Ordering::SeqCst) {
            return;
        }
        notified.await;
    }
}

impl Default for AbortSignal {
    fn default() -> Self {
        Self::new()
    }
}
