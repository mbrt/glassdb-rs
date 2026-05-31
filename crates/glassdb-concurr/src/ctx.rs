//! A lightweight cancellation/value context analogous to Go's
//! `context.Context`. It carries an optional [`CancellationToken`] and an
//! optional transaction-id override (used by the transaction engine for
//! deterministic testing, mirroring Go's `CtxWithTxID`).

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

/// A cancellation context that also propagates a few values.
#[derive(Clone, Default)]
pub struct Ctx {
    token: Option<CancellationToken>,
    tx_id: Option<Arc<Vec<u8>>>,
}

/// Error returned when a context has been cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cancelled;

impl std::fmt::Display for Cancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "context canceled")
    }
}

impl std::error::Error for Cancelled {}

impl Ctx {
    /// A non-cancellable background context.
    pub fn background() -> Self {
        Self::default()
    }

    /// Wraps an existing cancellation token.
    pub fn from_token(token: CancellationToken) -> Self {
        Self {
            token: Some(token),
            tx_id: None,
        }
    }

    /// Creates a cancellable context, returning the context and its token.
    pub fn with_cancel() -> (Self, CancellationToken) {
        let t = CancellationToken::new();
        (Self::from_token(t.clone()), t)
    }

    /// Creates a child context whose token is cancelled when the parent's is
    /// (or when the returned token is cancelled). Values are preserved.
    pub fn child_cancel(&self) -> (Self, CancellationToken) {
        let t = match &self.token {
            Some(p) => p.child_token(),
            None => CancellationToken::new(),
        };
        (
            Self {
                token: Some(t.clone()),
                tx_id: self.tx_id.clone(),
            },
            t,
        )
    }

    /// Replaces the cancellation source with `token`, preserving values.
    /// Mirrors Go's `ContextWithNewCancel`.
    pub fn with_new_cancel(&self, token: CancellationToken) -> Self {
        Self {
            token: Some(token),
            tx_id: self.tx_id.clone(),
        }
    }

    /// Reports whether the context has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.token.as_ref().is_some_and(|t| t.is_cancelled())
    }

    /// Returns `Err(Cancelled)` if the context has been cancelled.
    pub fn err(&self) -> Result<(), Cancelled> {
        if self.is_cancelled() {
            Err(Cancelled)
        } else {
            Ok(())
        }
    }

    /// Resolves when the context is cancelled. Pends forever if the context is
    /// not cancellable.
    pub async fn cancelled(&self) {
        match &self.token {
            Some(t) => t.cancelled().await,
            None => std::future::pending::<()>().await,
        }
    }

    /// Returns a copy carrying the given transaction-id override.
    pub fn with_tx_id(&self, id: Vec<u8>) -> Self {
        Self {
            token: self.token.clone(),
            tx_id: Some(Arc::new(id)),
        }
    }

    /// Returns the transaction-id override, if any.
    pub fn tx_id(&self) -> Option<&[u8]> {
        self.tx_id.as_deref().map(|v| v.as_slice())
    }
}
