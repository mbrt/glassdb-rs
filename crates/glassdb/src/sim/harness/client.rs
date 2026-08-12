//! Client task ownership for the deterministic simulation harness.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use glassdb_backend::Backend;
use glassdb_concurr::rt;
use tokio_util::sync::CancellationToken;

use crate::sim::SimMedia;
use crate::sim::trace::{
    ClientOperationTrace, TraceClientPhase, TraceClientRun, TraceOperationPhase, TraceSink,
    TraceSpawnRole,
};
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
        trace: &TraceSink,
    ) -> Self {
        let nclients = client_ops.len();
        let mut handles = Vec::with_capacity(nclients);
        let mut signals = Vec::with_capacity(nclients);
        for (client, (ops, backend)) in client_ops.into_iter().zip(client_backends).enumerate() {
            let signal = CancellationToken::new();
            signals.push(signal.clone());
            let state = state.clone();
            let media = run_media.map(|run_media| run_media.clients[client].clone());
            trace.spawn(TraceSpawnRole::Client(client as u32), true);
            let task_trace = trace.clone();
            handles.push(rt::spawn(
                ClientTask::<W> {
                    client,
                    ops,
                    backend,
                    signal,
                    state,
                    media,
                    faults,
                    trace: task_trace,
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
    client: usize,
    ops: Vec<W::Op>,
    backend: Arc<dyn Backend>,
    signal: CancellationToken,
    state: Arc<W::State>,
    media: Option<SimMedia>,
    faults: FaultConfig,
    trace: TraceSink,
}

impl<W: SimWorkload> ClientTask<W> {
    async fn run(self) {
        let Self {
            client,
            ops,
            backend,
            signal,
            state,
            media,
            faults,
            trace,
        } = self;
        trace.client(client, TraceClientPhase::Started, 0);
        let consumed = Arc::new(AtomicUsize::new(0));
        let crashed = {
            let db = match W::open_db(&backend, media.clone()).await {
                Ok(db) => db,
                Err(error) => {
                    Self::assert_admissible_error(faults, "opening client database", error);
                    trace.client(client, TraceClientPhase::OpenFailed, 0);
                    trace.client(client, TraceClientPhase::Finished, 0);
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
                    ClientOperationTrace::new(
                        &trace,
                        client,
                        0,
                        TraceClientRun::Initial,
                    ),
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
        if crashed {
            trace.client(client, TraceClientPhase::Crashed, consumed);
        }
        let mut final_consumed = consumed;
        if crashed && consumed < ops.len() {
            match W::open_db(&backend, media).await {
                Ok(db) => {
                    trace.client(client, TraceClientPhase::Restarted, consumed);
                    let restart_consumed = AtomicUsize::new(0);
                    let restarted = Self::run_operations(
                        &db,
                        &ops[consumed..],
                        &state,
                        &restart_consumed,
                        faults,
                        ClientOperationTrace::new(
                            &trace,
                            client,
                            consumed,
                            TraceClientRun::Restart,
                        ),
                    )
                    .await;
                    final_consumed = consumed + restarted;
                    db.shutdown().await;
                }
                Err(error) => {
                    Self::assert_admissible_error(
                        faults,
                        "reopening crashed client database",
                        error,
                    );
                    trace.client(client, TraceClientPhase::RestartOpenFailed, consumed);
                }
            }
        }
        trace.client(client, TraceClientPhase::Finished, final_consumed);
    }

    /// Runs a client's request stream in order and returns how many requests
    /// were consumed, including a final request whose outcome is in doubt.
    async fn run_operations(
        db: &Database,
        ops: &[W::Op],
        state: &W::State,
        consumed: &AtomicUsize,
        faults: FaultConfig,
        trace: ClientOperationTrace<'_>,
    ) -> usize {
        for (operation, op) in ops.iter().enumerate() {
            // Publish consumption before starting the operation because the
            // cancellation branch drops this future without a return value.
            consumed.store(operation + 1, Ordering::SeqCst);
            trace.record(operation, TraceOperationPhase::Started);
            match W::run_op(db, op, state).await {
                Ok(()) => trace.record(operation, TraceOperationPhase::Succeeded),
                Err(error) => {
                    Self::assert_admissible_error(faults, "running client operation", error);
                    trace.record(operation, TraceOperationPhase::AdmissibleError);
                    return operation + 1;
                }
            }
        }
        consumed.store(ops.len(), Ordering::SeqCst);
        ops.len()
    }

    fn assert_admissible_error(faults: FaultConfig, context: &str, error: Error) {
        if client_error_is_admissible(faults, &error) {
            return;
        }
        panic!("{context} returned unexpected error: {error} ({error:?})");
    }
}
