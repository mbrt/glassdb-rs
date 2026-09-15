//! Client streams and shared database-instance lifetimes for simulation.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use glassdb_backend::Backend;
use glassdb_concurr::rt;
use tokio_util::sync::CancellationToken;

use crate::sim::SimMedia;
use crate::{Database, Error};

use super::super::CLIENTS_PER_INSTANCE;
use super::{FaultConfig, RunMedia, SimWorkload, client_error_is_admissible};

// A stalled request must not prevent checks of completed and in-doubt work.
// This bounds foreground execution only; final consistency verification still
// runs through a fresh database instance.
const FOREGROUND_LIMIT: Duration = Duration::from_secs(10);

#[derive(PartialEq, Eq)]
enum InstanceExit {
    Finished,
    Crashed,
    TimedOut,
}

/// Owns instance tasks and their crash signals for one run.
pub(super) struct ClientRunner {
    handles: Vec<rt::JoinHandle<bool>>,
    signals: Vec<CancellationToken>,
}

impl ClientRunner {
    /// Runs adjacent pairs of client streams through shared database handles.
    pub(super) fn spawn<W: SimWorkload>(
        client_ops: Vec<Vec<W::Op>>,
        instance_backends: Vec<Arc<dyn Backend>>,
        run_media: Option<&RunMedia>,
        state: &Arc<W::State>,
        faults: FaultConfig,
    ) -> Self {
        assert_eq!(
            instance_backends.len(),
            client_ops.len().div_ceil(CLIENTS_PER_INSTANCE)
        );
        let mut clients = client_ops.into_iter();
        let mut handles = Vec::with_capacity(instance_backends.len());
        let mut signals = Vec::with_capacity(instance_backends.len());
        for (instance, backend) in instance_backends.into_iter().enumerate() {
            let signal = CancellationToken::new();
            signals.push(signal.clone());
            let clients = clients
                .by_ref()
                .take(CLIENTS_PER_INSTANCE)
                .map(|ops| {
                    Arc::new(ClientStream {
                        ops,
                        consumed: AtomicUsize::new(0),
                        stopped: AtomicBool::new(false),
                    })
                })
                .collect();
            handles.push(rt::spawn(
                InstanceTask::<W> {
                    clients,
                    backend,
                    signal,
                    state: state.clone(),
                    media: run_media.map(|media| media.instances[instance].clone()),
                    faults,
                }
                .run(),
            ));
        }
        Self { handles, signals }
    }

    /// Returns the instance signals targeted by the crash nemesis.
    pub(super) fn signals(&self) -> &[CancellationToken] {
        &self.signals
    }

    /// Collects instance results and reports whether a foreground limit was reached.
    pub(super) async fn join(&mut self) -> bool {
        let mut timed_out = false;
        for handle in std::mem::take(&mut self.handles) {
            timed_out |= handle.await.expect("client task failed");
        }
        timed_out
    }
}

struct ClientStream<O> {
    ops: Vec<O>,
    consumed: AtomicUsize,
    stopped: AtomicBool,
}

impl<O> ClientStream<O> {
    fn has_remaining(&self) -> bool {
        !self.stopped.load(Ordering::SeqCst)
            && self.consumed.load(Ordering::SeqCst) < self.ops.len()
    }
}

struct InstanceTask<W: SimWorkload> {
    clients: Vec<Arc<ClientStream<W::Op>>>,
    backend: Arc<dyn Backend>,
    signal: CancellationToken,
    state: Arc<W::State>,
    media: Option<SimMedia>,
    faults: FaultConfig,
}

impl<W: SimWorkload> InstanceTask<W> {
    async fn run(self) -> bool {
        let exit = self.run_until_stopped(self.signal.clone()).await;
        if exit == InstanceExit::Finished {
            return false;
        }
        if let Some(media) = &self.media {
            // All client handles and the owning database must be dropped before
            // the shared cache medium can crash and reopen.
            media.crash();
        }
        if exit == InstanceExit::TimedOut {
            return true;
        }
        if self.clients.iter().any(|client| client.has_remaining()) {
            // Each interrupted operation stays in doubt. A new signal prevents
            // another nemesis crash during restart; the foreground limit remains.
            return self.run_until_stopped(CancellationToken::new()).await
                == InstanceExit::TimedOut;
        }
        false
    }

    /// Runs one database instance until its streams finish or it crashes.
    async fn run_until_stopped(&self, signal: CancellationToken) -> InstanceExit {
        let db = match W::open_db(&self.backend, self.media.clone()).await {
            Ok(db) => db,
            Err(error) => {
                Self::assert_admissible_error(self.faults, "opening client database", error);
                return InstanceExit::Finished;
            }
        };
        let mut handles = Vec::with_capacity(self.clients.len());
        for client in &self.clients {
            if !client.has_remaining() {
                continue;
            }
            let client = client.clone();
            let db = db.clone();
            let state = self.state.clone();
            let signal = signal.clone();
            let faults = self.faults;
            handles.push(rt::spawn(async move {
                tokio::select! {
                    biased;
                    _ = signal.cancelled() => {}
                    _ = Self::run_operations(&db, &client, &state, faults) => {}
                }
            }));
        }
        // Dropping a task handle detaches it. Join every client before dropping
        // the database so a crashed instance cannot retain live shared handles.
        let join = async move {
            for handle in handles {
                handle.await.expect("client task failed");
            }
        };
        tokio::pin!(join);
        tokio::select! {
            biased;
            _ = &mut join => {},
            _ = rt::sleep(FOREGROUND_LIMIT) => {
                signal.cancel();
                join.await;
                return InstanceExit::TimedOut;
            }
        }
        if signal.is_cancelled() {
            InstanceExit::Crashed
        } else {
            db.shutdown().await;
            InstanceExit::Finished
        }
    }

    /// Runs a client's remaining request stream in order.
    async fn run_operations(
        db: &Database,
        client: &ClientStream<W::Op>,
        state: &W::State,
        faults: FaultConfig,
    ) {
        let consumed = client.consumed.load(Ordering::SeqCst);
        for (operation, op) in client.ops.iter().enumerate().skip(consumed) {
            // Publish consumption before invocation so cancellation cannot replay
            // an operation that may already have changed storage.
            client.consumed.store(operation + 1, Ordering::SeqCst);
            if let Err(error) = W::run_op(db, op, state).await {
                Self::assert_admissible_error(faults, "running client operation", error);
                if !W::CONTINUE_AFTER_ADMISSIBLE_ERROR {
                    client.stopped.store(true, Ordering::SeqCst);
                    return;
                }
            }
        }
        client.stopped.store(true, Ordering::SeqCst);
    }

    fn assert_admissible_error(faults: FaultConfig, context: &str, error: Error) {
        if client_error_is_admissible(faults, &error) {
            return;
        }
        panic!("{context} returned unexpected error: {error} ({error:?})");
    }
}

#[cfg(test)]
mod sim_tests {
    use std::sync::Mutex;

    use glassdb_backend::{
        BackendError,
        memory::MemoryBackend,
        middleware::{BackendOp, HookBackend},
    };
    use glassdb_concurr::exec;
    use tokio::sync::Notify;

    use super::super::RunPlan;
    use super::*;
    use crate::sim::key_name;
    use crate::sim::{
        HistoryCollectionOp as C, HistoryInstruction as I, HistoryTransaction, HistoryWorkload,
    };

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Step {
        Wait(usize),
        Write(usize),
        Fail(usize),
    }

    #[derive(Clone, Default)]
    struct RestartWorkload {
        clients: Vec<Vec<Step>>,
        expected_retired: usize,
        expected_writes: Vec<usize>,
    }

    struct RestartState {
        calls: Mutex<Vec<Step>>,
        entered: AtomicUsize,
        retired: AtomicUsize,
        written: AtomicUsize,
        failed: AtomicBool,
        expected_retired: usize,
        changed: Notify,
        release: Notify,
    }

    struct PendingCall<'a> {
        state: &'a RestartState,
        client: usize,
    }

    impl Drop for PendingCall<'_> {
        fn drop(&mut self) {
            self.state
                .retired
                .fetch_or(1 << self.client, Ordering::SeqCst);
            self.state.changed.notify_one();
        }
    }

    impl SimWorkload for RestartWorkload {
        type Op = Step;
        type State = RestartState;

        fn clients(&self) -> &[Vec<Self::Op>] {
            &self.clients
        }

        fn new_state(&self) -> Self::State {
            RestartState {
                calls: Mutex::new(Vec::new()),
                entered: AtomicUsize::new(0),
                retired: AtomicUsize::new(0),
                written: AtomicUsize::new(0),
                failed: AtomicBool::new(false),
                expected_retired: self.expected_retired,
                changed: Notify::new(),
                release: Notify::new(),
            }
        }

        async fn seed(&self, _db: &Database) {}

        async fn run_op(db: &Database, step: &Step, state: &Self::State) -> Result<(), Error> {
            state.calls.lock().unwrap().push(step.clone());
            match *step {
                Step::Wait(client) => {
                    let _pending = PendingCall { state, client };
                    db.root_collection().read(&key_name(client)).await?;
                    state.entered.fetch_or(1 << client, Ordering::SeqCst);
                    state.changed.notify_one();
                    state.release.notified().await;
                }
                Step::Write(client) => {
                    if client < 2 {
                        assert_eq!(
                            state.retired.load(Ordering::SeqCst) & 3,
                            state.expected_retired
                        );
                    }
                    let key = key_name(client);
                    let key = &key;
                    db.tx(|tx| async move {
                        let root = tx.root_collection();
                        tx.write(&root, key, b"value")
                    })
                    .await?;
                    state.written.fetch_or(1 << client, Ordering::SeqCst);
                    state.changed.notify_one();
                }
                Step::Fail(_) => {
                    state.failed.store(true, Ordering::SeqCst);
                    state.changed.notify_one();
                    return Err(Error::Unavailable("client operation failed".into()));
                }
            }
            Ok(())
        }

        async fn verify(&self, db: &Database, _state: &Self::State, _failures_enabled: bool) {
            for client in 0..4 {
                let expected = self
                    .expected_writes
                    .contains(&client)
                    .then(|| b"value".to_vec());
                assert_eq!(
                    db.root_collection().read(&key_name(client)).await.unwrap(),
                    expected
                );
            }
        }
    }

    #[test]
    fn an_instance_crash_retires_both_clients_before_restart() {
        for media_tape in [None, Some(Vec::new())] {
            exec::block_on(async move {
                let workload = RestartWorkload {
                    clients: (0..4)
                        .map(|client| vec![Step::Wait(client), Step::Write(client)])
                        .collect(),
                    expected_retired: 3,
                    expected_writes: (0..4).collect(),
                };
                let mut context =
                    RunPlan::new(workload, FaultConfig::none(), 1, Vec::new(), media_tape)
                        .setup()
                        .await;
                let state = context.state.clone();
                let mut clients = context.start_clients();
                assert_eq!(clients.signals().len(), 2);
                while state.entered.load(Ordering::SeqCst) != 15 {
                    state.changed.notified().await;
                }
                clients.signals()[0].cancel();
                while state.written.load(Ordering::SeqCst) != 3 {
                    state.changed.notified().await;
                }
                assert_eq!(
                    state.retired.load(Ordering::SeqCst),
                    3,
                    "the independent instance must remain active"
                );
                state.release.notify_waiters();
                clients.join().await;
                let mut calls = state.calls.lock().unwrap().clone();
                for client in 0..4 {
                    for expected in [Step::Wait(client), Step::Write(client)] {
                        let index = calls
                            .iter()
                            .position(|call| *call == expected)
                            .expect("missing client operation");
                        calls.remove(index);
                    }
                }
                assert!(calls.is_empty(), "an operation was replayed: {calls:?}");
                context.teardown(false).await;
            });
        }
    }

    #[test]
    fn foreground_limit_preserves_completed_work_for_verification() {
        for media_tape in [None, Some(Vec::new())] {
            exec::block_on(async move {
                let workload = RestartWorkload {
                    clients: (0..4)
                        .map(|client| {
                            vec![Step::Write(client), Step::Wait(client), Step::Write(client)]
                        })
                        .collect(),
                    expected_retired: 0,
                    expected_writes: (0..4).collect(),
                };
                let mut context =
                    RunPlan::new(workload, FaultConfig::none(), 1, Vec::new(), media_tape)
                        .setup()
                        .await;
                let state = context.state.clone();
                let mut clients = context.start_clients();
                let timed_out = clients.join().await;
                assert!(timed_out);
                assert_eq!(state.entered.load(Ordering::SeqCst), 15);
                assert_eq!(state.retired.load(Ordering::SeqCst), 15);
                assert_eq!(state.calls.lock().unwrap().len(), 8);
                context.teardown(timed_out).await;
            });
        }
    }

    #[test]
    fn restart_does_not_resume_a_client_stopped_by_an_error() {
        exec::block_on(async {
            let workload = RestartWorkload {
                clients: vec![
                    vec![Step::Fail(0), Step::Write(0)],
                    vec![Step::Wait(1), Step::Write(1)],
                    Vec::new(),
                    Vec::new(),
                ],
                expected_retired: 2,
                expected_writes: vec![1],
            };
            let mut context = RunPlan::new(workload, FaultConfig::failures(0), 1, Vec::new(), None)
                .setup()
                .await;
            let state = context.state.clone();
            let mut clients = context.start_clients();
            while !state.failed.load(Ordering::SeqCst) || state.entered.load(Ordering::SeqCst) != 2
            {
                state.changed.notified().await;
            }
            clients.signals()[0].cancel();
            clients.join().await;
            let calls = state.calls.lock().unwrap().clone();
            assert_eq!(
                calls.iter().filter(|call| **call == Step::Fail(0)).count(),
                1
            );
            assert_eq!(
                calls.iter().filter(|call| **call == Step::Wait(1)).count(),
                1
            );
            assert_eq!(
                calls.iter().filter(|call| **call == Step::Write(1)).count(),
                1
            );
            assert!(!calls.contains(&Step::Write(0)));
            context.teardown(false).await;
        });
    }

    #[derive(Debug, Clone, Copy, Default)]
    enum PublicFailure {
        #[default]
        Read,
        MutationReply(usize),
    }

    #[derive(Clone)]
    struct RecoveryWorkload {
        history: HistoryWorkload,
        backend: Arc<HookBackend>,
        failure: PublicFailure,
    }

    impl Default for RecoveryWorkload {
        fn default() -> Self {
            Self {
                history: HistoryWorkload::default(),
                backend: HookBackend::new(Arc::new(MemoryBackend::new())),
                failure: PublicFailure::default(),
            }
        }
    }

    struct RecoveryState {
        history: <HistoryWorkload as SimWorkload>::State,
        backend: Arc<HookBackend>,
        failure: PublicFailure,
        outcomes: Mutex<Vec<(u64, Result<(), Error>)>>,
        failed_operation_returned: CancellationToken,
    }

    impl RecoveryState {
        fn inject_failure(&self) -> CancellationToken {
            let cancelled = CancellationToken::new();
            let deadline = cancelled.clone();
            let backend = self.backend.clone();
            // Some failed mutations are retried without returning to the
            // caller. Bound the outage so those cases can also finish.
            if matches!(self.failure, PublicFailure::MutationReply(_)) {
                rt::spawn(async move {
                    tokio::select! {
                        _ = deadline.cancelled() => {},
                        _ = rt::sleep(Duration::from_secs(1)) => {
                            backend.clear_before();
                            backend.clear_after();
                        }
                    }
                });
            }
            match self.failure {
                PublicFailure::Read => self.backend.set_before(|op| {
                    let fail = matches!(
                        op,
                        BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                    );
                    Box::pin(async move {
                        if fail {
                            Err(BackendError::Unavailable("read outage".into()))
                        } else {
                            Ok(())
                        }
                    })
                }),
                PublicFailure::MutationReply(allowed) => {
                    let failed = Arc::new(AtomicBool::new(false));
                    let mutations = AtomicUsize::new(0);
                    self.backend.set_before({
                        let failed = failed.clone();
                        move |_| {
                            let fail = failed.load(Ordering::SeqCst);
                            Box::pin(async move {
                                if fail {
                                    Err(BackendError::Unavailable("outage after lost reply".into()))
                                } else {
                                    Ok(())
                                }
                            })
                        }
                    });
                    self.backend.set_after(move |op, _| {
                        let mutation = matches!(
                            op,
                            BackendOp::WriteIf { .. }
                                | BackendOp::WriteIfNotExists { .. }
                                | BackendOp::DeleteIf { .. }
                        );
                        let fail = mutation && mutations.fetch_add(1, Ordering::SeqCst) >= allowed;
                        if fail {
                            failed.store(true, Ordering::SeqCst);
                        }
                        Box::pin(async move {
                            if fail {
                                Err(BackendError::Unavailable("lost mutation reply".into()))
                            } else {
                                Ok(())
                            }
                        })
                    });
                }
            }
            cancelled
        }
    }

    impl SimWorkload for RecoveryWorkload {
        type Op = HistoryTransaction;
        type State = RecoveryState;

        const CONTINUE_AFTER_ADMISSIBLE_ERROR: bool =
            HistoryWorkload::CONTINUE_AFTER_ADMISSIBLE_ERROR;

        fn clients(&self) -> &[Vec<Self::Op>] {
            &self.history.clients
        }

        fn new_state(&self) -> Self::State {
            RecoveryState {
                history: self.history.new_state(),
                backend: self.backend.clone(),
                failure: self.failure,
                outcomes: Mutex::new(Vec::new()),
                failed_operation_returned: CancellationToken::new(),
            }
        }

        async fn open_db(
            backend: &Arc<dyn Backend>,
            media: Option<SimMedia>,
        ) -> Result<Database, Error> {
            HistoryWorkload::open_db(backend, media).await
        }

        async fn seed(&self, db: &Database) {
            self.history.seed(db).await;
        }

        async fn run_op(db: &Database, op: &Self::Op, state: &Self::State) -> Result<(), Error> {
            if op.op_id == 3 {
                state.failed_operation_returned.cancelled().await;
            }
            let healing = (op.op_id == 1).then(|| state.inject_failure());
            let result = if op.op_id == 1 && matches!(state.failure, PublicFailure::Read) {
                // A public point read exposes Unavailable without an uncertain
                // mutation. It makes no change to the history's modeled state.
                db.root_collection()
                    .read_stale(b"unavailable-probe", Duration::ZERO)
                    .await
                    .map(|_| ())
            } else {
                HistoryWorkload::run_op(db, op, &state.history).await
            };
            if op.op_id == 0 {
                // Let the preceding write-back finish before selecting a
                // mutation reply from the next collection operation.
                rt::sleep(Duration::from_millis(1)).await;
            }
            if let Some(healing) = healing {
                healing.cancel();
                state.backend.clear_before();
                state.backend.clear_after();
                state.failed_operation_returned.cancel();
            }
            state
                .outcomes
                .lock()
                .unwrap()
                .push((op.op_id, result.clone()));
            result
        }

        async fn verify(&self, db: &Database, state: &Self::State, allow_in_doubt: bool) {
            self.history
                .verify(db, &state.history, allow_in_doubt)
                .await;
            let collection = db
                .open_collection(&crate::CollectionPath::new(b"history-shared-0").unwrap())
                .await
                .unwrap();
            assert_eq!(collection.read(b"value").await.unwrap(), Some(vec![3]));
            let outcomes = state.outcomes.lock().unwrap();
            assert_eq!(
                outcomes.len(),
                4,
                "subsequent operations did not run: {outcomes:?}"
            );
            for (id, result) in outcomes.iter() {
                match (id, state.failure) {
                    (1, PublicFailure::Read) => {
                        assert!(matches!(result, Err(Error::Unavailable(_))), "{result:?}")
                    }
                    (1, PublicFailure::MutationReply(_)) => {
                        assert!(
                            matches!(
                                result,
                                Ok(()) | Err(Error::InDoubt(_)) | Err(Error::Unavailable(_))
                            ),
                            "{result:?}"
                        )
                    }
                    _ => assert!(result.is_ok(), "{id}: {result:?}"),
                }
            }
        }
    }

    #[test]
    fn history_continues_on_public_errors_with_shared_collection_names() {
        for peer in [1, 2] {
            for cached in [false, true] {
                let mut in_doubt = false;
                for failure in std::iter::once(PublicFailure::Read)
                    .chain((0..8).map(PublicFailure::MutationReply))
                {
                    in_doubt |= exec::block_on(async move {
                        let media_tape = cached.then(Vec::new);
                        let mut workload = RecoveryWorkload {
                            failure,
                            ..Default::default()
                        };
                        workload.history.clients[0] = [C::Write(1), C::Drop, C::Write(3)]
                            .into_iter()
                            .enumerate()
                            .map(|(id, operation)| HistoryTransaction {
                                op_id: id as u64,
                                client_id: 0,
                                instructions: vec![
                                    I::Collection { slot: 0, operation },
                                    I::WriteLiteral {
                                        key: 0,
                                        value: id as u8 + 1,
                                    },
                                ],
                            })
                            .collect();
                        workload.history.clients[peer] = vec![HistoryTransaction {
                            op_id: 3,
                            client_id: peer,
                            instructions: vec![I::Collection {
                                slot: 0,
                                operation: C::Read,
                            }],
                        }];
                        let state = Arc::new(workload.new_state());
                        let media = media_tape.map(|tape| RunMedia::new(tape, 0, 2));
                        // Use the ordinary public backend interface for injection.
                        // No transaction object, fence, cache, or monitor state
                        // participates in error classification or verification.
                        let seed_backend: Arc<dyn Backend> = workload.backend.clone();
                        let seed = RecoveryWorkload::open_db(&seed_backend, None)
                            .await
                            .unwrap();
                        workload.seed(&seed).await;
                        seed.shutdown().await;
                        let mut clients = ClientRunner::spawn::<RecoveryWorkload>(
                            workload.history.clients.clone(),
                            vec![seed_backend.clone(), seed_backend.clone()],
                            media.as_ref(),
                            &state,
                            FaultConfig::failures(0),
                        );
                        assert!(
                            !clients.join().await,
                            "foreground limit reached: peer={peer}, failure={failure:?}, cached={}, public outcomes={:?}",
                            media.is_some(),
                            state.outcomes.lock().unwrap()
                        );
                        let verify = RecoveryWorkload::open_db(&seed_backend, None)
                            .await
                            .unwrap();
                        workload.verify(&verify, &state, true).await;
                        verify.shutdown().await;
                        let outcomes = state.outcomes.lock().unwrap();
                        outcomes.iter().any(|(id, result)| {
                            *id == 1 && matches!(result, Err(Error::InDoubt(_)))
                        })
                    });
                }
                assert!(
                    in_doubt,
                    "no uncertain public outcome: peer={peer}, cached={cached}"
                );
            }
        }
    }
}
