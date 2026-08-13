//! Client task ownership for the deterministic simulation harness.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use glassdb_backend::Backend;
use glassdb_concurr::rt;
use tokio_util::sync::CancellationToken;

use crate::sim::SimMedia;
use crate::{Database, Error};

use super::{FaultConfig, RunMedia, SimWorkload, client_error_is_admissible};

/// Owns client crash/restart tasks and their cancellation signals for one run.
pub(super) struct ClientRunner {
    handles: Vec<rt::JoinHandle<()>>,
    signals: Vec<CancellationToken>,
}

impl ClientRunner {
    /// Spawns each client in request-stream order.
    pub(super) fn spawn<W: SimWorkload>(
        client_ops: Vec<Vec<W::Op>>,
        client_backends: Vec<Arc<dyn Backend>>,
        run_media: Option<&RunMedia>,
        state: &Arc<W::State>,
        faults: FaultConfig,
    ) -> Self {
        let nclients = client_ops.len();
        let mut handles = Vec::with_capacity(nclients);
        let mut signals = Vec::with_capacity(nclients);
        for (client, (ops, backend)) in client_ops.into_iter().zip(client_backends).enumerate() {
            let signal = CancellationToken::new();
            signals.push(signal.clone());
            let state = state.clone();
            let media = run_media.map(|run_media| run_media.clients[client].clone());
            handles.push(rt::spawn(
                ClientTask::<W> {
                    ops,
                    backend,
                    signal,
                    state,
                    media,
                    faults,
                }
                .run(),
            ));
        }
        Self { handles, signals }
    }

    /// Returns the client signals targeted by the crash nemesis.
    pub(super) fn signals(&self) -> &[CancellationToken] {
        &self.signals
    }

    /// Collects client results in spawn order and propagates task panics.
    pub(super) async fn join(&mut self) {
        for handle in std::mem::take(&mut self.handles) {
            handle.await.expect("client task failed");
        }
    }
}

struct ClientTask<W: SimWorkload> {
    ops: Vec<W::Op>,
    backend: Arc<dyn Backend>,
    signal: CancellationToken,
    state: Arc<W::State>,
    media: Option<SimMedia>,
    faults: FaultConfig,
}

impl<W: SimWorkload> ClientTask<W> {
    async fn run(self) {
        let Self {
            ops,
            backend,
            signal,
            state,
            media,
            faults,
        } = self;
        let consumed = Arc::new(AtomicUsize::new(0));
        let crashed = {
            let db = match W::open_db(&backend, media.clone()).await {
                Ok(db) => db,
                Err(error) => {
                    Self::assert_admissible_error(faults, "opening client database", error);
                    return;
                }
            };
            let crashed = tokio::select! {
                biased;
                _ = signal.cancelled() => true,
                _ = Self::run_operations(
                    &db,
                    &ops,
                    &state,
                    &consumed,
                    faults,
                ) => false,
            };
            if !crashed {
                db.shutdown().await;
            }
            crashed
        };
        if crashed && let Some(media) = &media {
            // A crashed process cannot synchronize its cache before reopening.
            media.crash();
        }

        // The in-doubt operation is never replayed, while an uncancellable
        // restart finishes the remaining request stream in its original order.
        let consumed = consumed.load(Ordering::SeqCst);
        if crashed && consumed < ops.len() {
            match W::open_db(&backend, media).await {
                Ok(db) => {
                    let restart_consumed = AtomicUsize::new(0);
                    Self::run_operations(&db, &ops[consumed..], &state, &restart_consumed, faults)
                        .await;
                    db.shutdown().await;
                }
                Err(error) => {
                    Self::assert_admissible_error(
                        faults,
                        "reopening crashed client database",
                        error,
                    );
                }
            }
        }
    }

    /// Runs a client's request stream in order, publishing consumption before
    /// each request starts so cancellation cannot cause it to be replayed.
    async fn run_operations(
        db: &Database,
        ops: &[W::Op],
        state: &W::State,
        consumed: &AtomicUsize,
        faults: FaultConfig,
    ) {
        for (operation, op) in ops.iter().enumerate() {
            // Publish consumption before starting the operation because the
            // cancellation branch drops this future without a return value.
            consumed.store(operation + 1, Ordering::SeqCst);
            if let Err(error) = W::run_op(db, op, state).await {
                Self::assert_admissible_error(faults, "running client operation", error);
                return;
            }
        }
        consumed.store(ops.len(), Ordering::SeqCst);
    }

    fn assert_admissible_error(faults: FaultConfig, context: &str, error: Error) {
        if client_error_is_admissible(faults, &error) {
            return;
        }
        panic!("{context} returned unexpected error: {error} ({error:?})");
    }
}
