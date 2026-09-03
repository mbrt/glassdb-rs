//! Exact strict-serializability checker for bounded transaction histories.
//!
//! This module models public point and concurrent-group reads, writes, deletes,
//! and normalized key membership scans. It does not inspect transaction logs,
//! cached objects, or other implementation state, so the oracle cannot
//! accidentally reproduce the protocol it checks.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};

use arbitrary::{Arbitrary, Unstructured};
use glassdb_backend::Backend;
use glassdb_concurr::rt;

use crate::{Collection, CollectionPath, Database, Error, InlinePolicy, KeyScan, Transaction};

use super::harness::{SimWorkload, open_det_db};
use super::{SimMedia, key_name, tiny_split_policy};

const HISTORY_COLLECTION: &[u8] = b"history";
const HISTORY_KEY_COUNT: usize = 3;
const HISTORY_REGISTER_COUNT: usize = 2;
const MAX_HISTORY_CLIENTS: usize = 3;
const MAX_HISTORY_TXS_PER_CLIENT: usize = 4;
const CHECK_BRANCH_BUDGET: usize = 1_000_000;
const APPLICATION_ERROR_MARKER: &str = "history-user-error";

/// The checker-owned abstract database state.
type AbstractState = BTreeMap<u8, u8>;

/// One resolved action from a transaction-body execution.
#[derive(Debug, Clone, PartialEq, Eq)]
enum HistoryAction {
    /// A point read and its returned value (`None` means absent).
    Read { key: u8, value: Option<u8> },
    /// Concurrent point-read observations from one transaction snapshot.
    ///
    /// A map deliberately represents the group without task completion order.
    ReadGroup {
        /// Observed value (`None` means absent) for every concurrently read key.
        observations: BTreeMap<u8, Option<u8>>,
    },
    /// A transaction-local write after expression evaluation.
    Write { key: u8, value: u8 },
    /// A transaction-local deletion.
    Delete { key: u8 },
    /// A materialized membership scan over normalized half-open bounds.
    Scan {
        /// Inclusive lower bound, unless `start_exclusive` is set.
        start: u8,
        /// Whether `start` itself is excluded.
        start_exclusive: bool,
        /// Exclusive upper bound.
        end: u8,
        /// Maximum number of returned keys.
        limit: u8,
        /// Sorted present keys returned by the transaction.
        keys: Vec<u8>,
    },
}

/// State of one recorded transaction-body execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BodyState {
    /// The body has not returned a body outcome.
    Incomplete,
    /// The body returned a commit outcome.
    CommitOutcome,
    /// The body returned a modeled application-error outcome.
    ApplicationErrorOutcome,
    /// The body returned an explicit-abort outcome.
    ExplicitAbort,
    /// The body returned an engine-error outcome.
    EngineErrorOutcome,
}

/// One complete execution of a public transaction body.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BodyTrace {
    /// Retry number within the public transaction, starting at zero.
    body_number: usize,
    /// Point reads, unordered read groups, scans, and resolved local mutations
    /// in program order.
    ordered_actions: Vec<HistoryAction>,
    /// The final mutation for every key touched by the local overlay.
    final_mutations: BTreeMap<u8, Option<u8>>,
    /// State of the body execution before validation or commit.
    state: BodyState,
}

/// Public result classification used by the sequential specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HistoryOutcome {
    /// The transaction returned a snapshot-transparent outcome and must appear
    /// exactly once.
    SnapshotTransparent,
    /// A definitive failure or explicit abort with no database effect.
    DefiniteNoEffect,
    /// The caller was told that a commit may or may not have happened.
    InDoubt,
    /// The public transaction was interrupted without a notification.
    Interrupted,
}

/// A public transaction invocation and every body execution caused by retries.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PublicOp {
    /// Stable workload operation identifier.
    op_id: u64,
    /// Client that invoked the operation.
    client_id: usize,
    /// Checker-owned event point at invocation.
    invocation_point: u64,
    /// Body executions in retry order.
    body_executions: Vec<BodyTrace>,
    /// Checker-owned event point at public notification, absent for interruption.
    notification_point: Option<u64>,
    /// Public outcome.
    outcome: HistoryOutcome,
}

/// Statistics and one witness serialization for an accepted history.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CheckStats {
    /// Candidate placements examined by the exact search.
    explored_branches: usize,
    /// Operation IDs in one accepted serialization; omitted optional operations
    /// are absent.
    serialization: Vec<u64>,
}

/// A malformed or non-serializable history.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CheckError {
    detail: String,
}

impl CheckError {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

impl fmt::Display for CheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.detail)
    }
}

impl std::error::Error for CheckError {}

#[derive(Clone)]
struct LogicalOp<'a> {
    public: &'a PublicOp,
    trace: &'a BodyTrace,
    mutates: bool,
    optional: bool,
    predecessors: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SearchKey {
    state: AbstractState,
    mandatory: u64,
    optional: u64,
}

struct Search<'a> {
    ops: Vec<LogicalOp<'a>>,
    final_state: &'a AbstractState,
    seen: BTreeSet<SearchKey>,
    explored: usize,
    witness: Vec<u64>,
}

fn selected_trace(op: &PublicOp) -> Result<Option<(&BodyTrace, bool, bool)>, CheckError> {
    let last = op.body_executions.last();
    match op.outcome {
        HistoryOutcome::SnapshotTransparent => match last {
            Some(trace) if trace.state == BodyState::CommitOutcome => {
                Ok(Some((trace, true, false)))
            }
            Some(trace) if trace.state == BodyState::ApplicationErrorOutcome => {
                Ok(Some((trace, false, false)))
            }
            _ => Err(CheckError::new(format!(
                "snapshot-transparent op {} has no matching body outcome",
                op.op_id
            ))),
        },
        HistoryOutcome::InDoubt => last
            .filter(|trace| trace.state == BodyState::CommitOutcome)
            .map(|trace| Some((trace, true, true)))
            .ok_or_else(|| {
                CheckError::new(format!(
                    "in-doubt op {} has no commit-eligible body trace",
                    op.op_id
                ))
            }),
        // A dropped future that has no body outcome cannot
        // have entered the commit protocol and therefore has no optional effect.
        HistoryOutcome::Interrupted => Ok(last
            .filter(|trace| trace.state == BodyState::CommitOutcome)
            .map(|trace| (trace, true, true))),
        HistoryOutcome::DefiniteNoEffect => Ok(None),
    }
}

fn validate_trace(op_id: u64, trace: &BodyTrace) -> Result<(), CheckError> {
    let mut derived = BTreeMap::new();
    for action in &trace.ordered_actions {
        match action {
            HistoryAction::Read { .. }
            | HistoryAction::ReadGroup { .. }
            | HistoryAction::Scan { .. } => {}
            HistoryAction::Write { key, value } => {
                derived.insert(*key, Some(*value));
            }
            HistoryAction::Delete { key } => {
                derived.insert(*key, None);
            }
        }
    }
    if derived != trace.final_mutations {
        return Err(CheckError::new(format!(
            "op {op_id} body {} final mutations {:?} do not match actions {derived:?}",
            trace.body_number, trace.final_mutations
        )));
    }
    Ok(())
}

fn scanned_keys(
    state: &AbstractState,
    overlay: &BTreeMap<u8, Option<u8>>,
    start: u8,
    start_exclusive: bool,
    end: u8,
    limit: u8,
) -> Vec<u8> {
    let mut visible = state
        .keys()
        .copied()
        .map(|key| (key, true))
        .collect::<BTreeMap<_, _>>();
    for (key, value) in overlay {
        visible.insert(*key, value.is_some());
    }
    visible
        .into_iter()
        .filter_map(|(key, present)| present.then_some(key))
        .filter(|key| {
            (if start_exclusive {
                *key > start
            } else {
                *key >= start
            }) && *key < end
        })
        .take(limit as usize)
        .collect()
}

fn apply_trace(state: &AbstractState, op: &LogicalOp<'_>) -> Result<AbstractState, String> {
    let mut overlay = BTreeMap::<u8, Option<u8>>::new();
    for action in &op.trace.ordered_actions {
        match action {
            HistoryAction::Read { key, value } => {
                let actual = overlay
                    .get(key)
                    .copied()
                    .unwrap_or_else(|| state.get(key).copied());
                if actual != *value {
                    return Err(format!(
                        "op {} read k{key} as {value:?}, candidate state gives {actual:?}",
                        op.public.op_id
                    ));
                }
            }
            HistoryAction::ReadGroup { observations } => {
                for (key, value) in observations {
                    let actual = overlay
                        .get(key)
                        .copied()
                        .unwrap_or_else(|| state.get(key).copied());
                    if actual != *value {
                        return Err(format!(
                            "op {} read group observed k{key} as {value:?}, candidate state gives \
                             {actual:?}",
                            op.public.op_id
                        ));
                    }
                }
            }
            HistoryAction::Write { key, value } => {
                overlay.insert(*key, Some(*value));
            }
            HistoryAction::Delete { key } => {
                overlay.insert(*key, None);
            }
            HistoryAction::Scan {
                start,
                start_exclusive,
                end,
                limit,
                keys,
            } => {
                let actual = scanned_keys(state, &overlay, *start, *start_exclusive, *end, *limit);
                if actual != *keys {
                    return Err(format!(
                        "op {} scanned start={start}, exclusive={start_exclusive}, end={end}, \
                         limit={limit} as {keys:?}; candidate state gives {actual:?}",
                        op.public.op_id
                    ));
                }
            }
        }
    }

    let mut next = state.clone();
    if op.mutates {
        for (key, value) in &op.trace.final_mutations {
            match value {
                Some(value) => {
                    next.insert(*key, *value);
                }
                None => {
                    next.remove(key);
                }
            }
        }
    }
    Ok(next)
}

impl Search<'_> {
    fn run(
        &mut self,
        state: AbstractState,
        mandatory: u64,
        optional: u64,
        prefix: &mut Vec<u64>,
    ) -> Result<bool, CheckError> {
        // Every remaining optional operation may be omitted. This is the exact
        // completion rule for in-doubt and interrupted public transactions.
        if mandatory == 0 && state == *self.final_state {
            self.witness = prefix.clone();
            return Ok(true);
        }

        let key = SearchKey {
            state: state.clone(),
            mandatory,
            optional,
        };
        if !self.seen.insert(key) {
            return Ok(false);
        }

        let remaining = mandatory | optional;
        for index in 0..self.ops.len() {
            let bit = 1u64 << index;
            if remaining & bit == 0 || self.ops[index].predecessors & remaining != 0 {
                continue;
            }
            self.explored += 1;
            if self.explored > CHECK_BRANCH_BUDGET {
                return Err(CheckError::new(format!(
                    "history search exceeded its {CHECK_BRANCH_BUDGET}-branch budget"
                )));
            }
            let Ok(next) = apply_trace(&state, &self.ops[index]) else {
                continue;
            };
            prefix.push(self.ops[index].public.op_id);
            if self.run(next, mandatory & !bit, optional & !bit, prefix)? {
                return Ok(true);
            }
            prefix.pop();
        }
        Ok(false)
    }
}

/// Checks whether a finite public history has a strict-serializable completion.
///
/// Snapshot-transparent transactions are mandatory. In-doubt and commit-eligible
/// interrupted transactions may appear zero or one time. The
/// search is exact within its explicit branch budget and respects real-time
/// order between definitively completed operations and later invocations.
fn check_history(
    initial_state: &AbstractState,
    history: &[PublicOp],
    final_state: &AbstractState,
) -> Result<CheckStats, CheckError> {
    let mut ids = BTreeSet::new();
    let mut points = BTreeSet::new();
    for op in history {
        if !ids.insert(op.op_id) {
            return Err(CheckError::new(format!(
                "duplicate operation id {}",
                op.op_id
            )));
        }
        if !points.insert(op.invocation_point) {
            return Err(CheckError::new(format!(
                "duplicate history event point {}",
                op.invocation_point
            )));
        }
        if let Some(notification) = op.notification_point {
            if notification <= op.invocation_point {
                return Err(CheckError::new(format!(
                    "op {} notification does not follow invocation",
                    op.op_id
                )));
            }
            if !points.insert(notification) {
                return Err(CheckError::new(format!(
                    "duplicate history event point {notification}"
                )));
            }
        }
        match op.outcome {
            HistoryOutcome::Interrupted if op.notification_point.is_some() => {
                return Err(CheckError::new(format!(
                    "interrupted op {} has a notification",
                    op.op_id
                )));
            }
            HistoryOutcome::Interrupted => {}
            _ if op.notification_point.is_none() => {
                return Err(CheckError::new(format!(
                    "completed op {} has no notification",
                    op.op_id
                )));
            }
            _ => {}
        }
        for (body_number, trace) in op.body_executions.iter().enumerate() {
            if trace.body_number != body_number {
                return Err(CheckError::new(format!(
                    "op {} body trace {} is numbered {}",
                    op.op_id, body_number, trace.body_number
                )));
            }
            if trace.state == BodyState::Incomplete
                && (body_number + 1 != op.body_executions.len()
                    || op.outcome != HistoryOutcome::Interrupted)
            {
                return Err(CheckError::new(format!(
                    "op {} has an incomplete body outside final interruption",
                    op.op_id
                )));
            }
            validate_trace(op.op_id, trace)?;
        }
    }

    let mut ops = Vec::new();
    for public in history {
        if let Some((trace, mutates, optional)) = selected_trace(public)? {
            ops.push(LogicalOp {
                public,
                trace,
                mutates,
                optional,
                predecessors: 0,
            });
        }
    }
    ops.sort_by_key(|op| op.public.op_id);
    if ops.len() > 63 {
        return Err(CheckError::new(format!(
            "history has {} logical operations; checker limit is 63",
            ops.len()
        )));
    }

    for current in 0..ops.len() {
        for predecessor in 0..ops.len() {
            if current == predecessor || ops[predecessor].optional {
                continue;
            }
            let Some(response) = ops[predecessor].public.notification_point else {
                continue;
            };
            if response < ops[current].public.invocation_point {
                ops[current].predecessors |= 1u64 << predecessor;
            }
        }
    }

    let mut mandatory = 0;
    let mut optional = 0;
    for (index, op) in ops.iter().enumerate() {
        if op.optional {
            optional |= 1u64 << index;
        } else {
            mandatory |= 1u64 << index;
        }
    }

    let mut search = Search {
        ops,
        final_state,
        seen: BTreeSet::new(),
        explored: 0,
        witness: Vec::new(),
    };
    let mut prefix = Vec::new();
    if search.run(initial_state.clone(), mandatory, optional, &mut prefix)? {
        return Ok(CheckStats {
            explored_branches: search.explored,
            serialization: search.witness,
        });
    }

    let edges: Vec<(u64, u64)> = search
        .ops
        .iter()
        .flat_map(|op| {
            search
                .ops
                .iter()
                .enumerate()
                .filter(move |(index, _)| op.predecessors & (1u64 << index) != 0)
                .map(move |(_, predecessor)| (predecessor.public.op_id, op.public.op_id))
        })
        .collect();
    Err(CheckError::new(format!(
        "history has no legal serialization after {} candidate placements\n\
         initial state: {initial_state:?}\nfinal state: {final_state:?}\n\
         real-time edges: {edges:?}\nhistory: {history:#?}",
        search.explored
    )))
}

/// One instruction in a generated public transaction program.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryInstruction {
    /// Read a point key into a local register.
    Read { key: u8, register: u8 },
    /// Read distinct keys concurrently into distinct local registers.
    ///
    /// Generated groups contain at most the workload's two-register bound.
    /// Their results are recorded as one unordered snapshot observation.
    ReadGroup {
        /// `(key, register)` pairs. Pair order does not represent read order.
        reads: Vec<(u8, u8)>,
    },
    /// Write a literal byte.
    WriteLiteral { key: u8, value: u8 },
    /// Copy a register's present byte into a key.
    WriteRegister { key: u8, register: u8 },
    /// Increment a register's present byte and write it to a key.
    WriteIncremented { key: u8, register: u8 },
    /// Delete a key.
    Delete { key: u8 },
    /// Materialize one bounded page from `[start, end)`, optionally advancing
    /// the lower bound past `after` before applying `limit`.
    Scan {
        start: u8,
        end: u8,
        after: Option<u8>,
        limit: u8,
    },
    /// Return a modeled application error unless a register equals the expected value.
    RequireEqual { register: u8, expected: Option<u8> },
    /// Explicitly abort the transaction.
    Abort,
    /// Add a deterministic scheduling point without changing semantics.
    Yield,
}

/// A bounded point/group-read/write/scan transaction run by one simulated client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryTransaction {
    /// Stable ID assigned while decoding the workload.
    pub op_id: u64,
    /// Owning client.
    pub client_id: usize,
    /// Instructions interpreted in order on every internal retry.
    pub instructions: Vec<HistoryInstruction>,
}

/// Concurrent transaction programs checked by exact serialization.
#[derive(Debug, Clone)]
pub struct HistoryWorkload {
    /// One transaction sequence per client.
    pub clients: Vec<Vec<HistoryTransaction>>,
}

impl Default for HistoryWorkload {
    fn default() -> Self {
        Self {
            clients: vec![Vec::new(), Vec::new()],
        }
    }
}

fn arbitrary_key(u: &mut Unstructured<'_>) -> arbitrary::Result<u8> {
    Ok(u.arbitrary::<u8>()? % HISTORY_KEY_COUNT as u8)
}

fn arbitrary_register(u: &mut Unstructured<'_>) -> arbitrary::Result<u8> {
    Ok(u.arbitrary::<u8>()? % HISTORY_REGISTER_COUNT as u8)
}

fn arbitrary_program(u: &mut Unstructured<'_>) -> arbitrary::Result<Vec<HistoryInstruction>> {
    let key = arbitrary_key(u)?;
    let register = arbitrary_register(u)?;
    let mut instructions = match u.arbitrary::<u8>()? % 8 {
        0 => vec![
            HistoryInstruction::Read { key, register },
            HistoryInstruction::WriteIncremented { key, register },
        ],
        1 => {
            let other = (key + 1 + u.arbitrary::<u8>()? % (HISTORY_KEY_COUNT as u8 - 1))
                % HISTORY_KEY_COUNT as u8;
            vec![
                HistoryInstruction::ReadGroup {
                    reads: vec![
                        (key, register),
                        (other, (register + 1) % HISTORY_REGISTER_COUNT as u8),
                    ],
                },
                HistoryInstruction::WriteIncremented { key, register },
                HistoryInstruction::WriteIncremented {
                    key: other,
                    register: (register + 1) % HISTORY_REGISTER_COUNT as u8,
                },
            ]
        }
        2 => vec![
            HistoryInstruction::WriteLiteral {
                key,
                value: u.arbitrary()?,
            },
            HistoryInstruction::Read { key, register },
        ],
        3 => vec![
            HistoryInstruction::Read { key, register },
            HistoryInstruction::WriteRegister {
                key: arbitrary_key(u)?,
                register,
            },
        ],
        4 => vec![HistoryInstruction::Delete { key }],
        5 => {
            let other = arbitrary_key(u)?;
            let start = key.min(other);
            let end = key.max(other) + 1;
            let after = if u.arbitrary::<u8>()? % 3 == 0 {
                None
            } else {
                Some(u.arbitrary::<u8>()? % (HISTORY_KEY_COUNT as u8 + 1))
            };
            vec![
                HistoryInstruction::Scan {
                    start,
                    end,
                    after,
                    limit: u.arbitrary::<u8>()? % (HISTORY_KEY_COUNT as u8 + 2),
                },
                // Pair the membership observation with a mutation so two
                // concurrent scan programs can form a phantom-sensitive cycle.
                HistoryInstruction::Delete { key },
            ]
        }
        6 => vec![
            HistoryInstruction::Read { key, register },
            HistoryInstruction::RequireEqual {
                register,
                expected: Some(u.arbitrary()?),
            },
        ],
        _ => vec![
            HistoryInstruction::WriteLiteral {
                key,
                value: u.arbitrary()?,
            },
            HistoryInstruction::Abort,
        ],
    };
    if u.arbitrary::<u8>()? % 3 == 0 {
        let at = u.arbitrary::<u8>()? as usize % (instructions.len() + 1);
        instructions.insert(at, HistoryInstruction::Yield);
    }
    Ok(instructions)
}

impl<'a> Arbitrary<'a> for HistoryWorkload {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let clients = 2 + u.arbitrary::<u8>()? as usize % (MAX_HISTORY_CLIENTS - 1);
        let mut next_id = 0u64;
        let mut programs = Vec::with_capacity(clients);
        for client_id in 0..clients {
            let count = u.arbitrary::<u8>()? as usize % (MAX_HISTORY_TXS_PER_CLIENT + 1);
            let mut client = Vec::with_capacity(count);
            for _ in 0..count {
                client.push(HistoryTransaction {
                    op_id: next_id,
                    client_id,
                    instructions: arbitrary_program(u)?,
                });
                next_id += 1;
            }
            programs.push(client);
        }
        Ok(Self { clients: programs })
    }
}

struct PendingOp {
    op_id: u64,
    client_id: usize,
    invocation_point: u64,
    body_executions: Vec<BodyTrace>,
    notification_point: Option<u64>,
    outcome: Option<HistoryOutcome>,
}

struct RecorderInner {
    next_point: u64,
    operations: BTreeMap<u64, PendingOp>,
}

/// Per-run public-history recorder used only by [`HistoryWorkload`].
pub struct HistoryRecorder {
    inner: Mutex<RecorderInner>,
    final_op_id: u64,
}

impl HistoryRecorder {
    fn new(workload: &HistoryWorkload) -> Self {
        let mut ids = BTreeSet::new();
        let mut maximum = None;
        for (client_id, programs) in workload.clients.iter().enumerate() {
            for program in programs {
                assert_eq!(
                    program.client_id, client_id,
                    "history program {} is assigned to the wrong client",
                    program.op_id
                );
                assert!(
                    ids.insert(program.op_id),
                    "duplicate history program id {}",
                    program.op_id
                );
                maximum =
                    Some(maximum.map_or(program.op_id, |value: u64| value.max(program.op_id)));
            }
        }
        Self {
            inner: Mutex::new(RecorderInner {
                next_point: 0,
                operations: BTreeMap::new(),
            }),
            final_op_id: maximum.map_or(0, |value| value + 1),
        }
    }

    fn begin(&self, program: &HistoryTransaction) {
        let mut inner = self.inner.lock().unwrap();
        let point = inner.next_point;
        inner.next_point += 1;
        let previous = inner.operations.insert(
            program.op_id,
            PendingOp {
                op_id: program.op_id,
                client_id: program.client_id,
                invocation_point: point,
                body_executions: Vec::new(),
                notification_point: None,
                outcome: None,
            },
        );
        assert!(previous.is_none(), "history operation invoked twice");
    }

    fn begin_body(&self, op_id: u64) -> usize {
        let mut inner = self.inner.lock().unwrap();
        let operation = inner
            .operations
            .get_mut(&op_id)
            .expect("body started before invocation");
        let body_number = operation.body_executions.len();
        operation.body_executions.push(BodyTrace {
            body_number,
            ordered_actions: Vec::new(),
            final_mutations: BTreeMap::new(),
            state: BodyState::Incomplete,
        });
        body_number
    }

    fn complete_body(&self, op_id: u64, trace: BodyTrace) {
        let mut inner = self.inner.lock().unwrap();
        let operation = inner
            .operations
            .get_mut(&op_id)
            .expect("body completed before invocation");
        let body = operation
            .body_executions
            .get_mut(trace.body_number)
            .expect("body completed without being started");
        assert_eq!(body.state, BodyState::Incomplete, "body completed twice");
        *body = trace;
    }

    fn notify(&self, op_id: u64, outcome: HistoryOutcome) {
        let mut inner = self.inner.lock().unwrap();
        let point = inner.next_point;
        inner.next_point += 1;
        let operation = inner
            .operations
            .get_mut(&op_id)
            .expect("notification recorded before invocation");
        assert!(
            operation.outcome.is_none(),
            "history operation notified twice"
        );
        operation.notification_point = Some(point);
        operation.outcome = Some(outcome);
    }

    fn add_final_read(&self, invocation: u64, notification: u64, final_state: &AbstractState) {
        let actions = (0..HISTORY_KEY_COUNT as u8)
            .map(|key| HistoryAction::Read {
                key,
                value: final_state.get(&key).copied(),
            })
            .collect();
        let mut inner = self.inner.lock().unwrap();
        let previous = inner.operations.insert(
            self.final_op_id,
            PendingOp {
                op_id: self.final_op_id,
                client_id: usize::MAX,
                invocation_point: invocation,
                body_executions: vec![BodyTrace {
                    body_number: 0,
                    ordered_actions: actions,
                    final_mutations: BTreeMap::new(),
                    state: BodyState::CommitOutcome,
                }],
                notification_point: Some(notification),
                outcome: Some(HistoryOutcome::SnapshotTransparent),
            },
        );
        assert!(previous.is_none(), "final read operation id collided");
    }

    fn event_point(&self) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        let point = inner.next_point;
        inner.next_point += 1;
        point
    }

    fn history(&self) -> Vec<PublicOp> {
        self.inner
            .lock()
            .unwrap()
            .operations
            .values()
            .map(|operation| PublicOp {
                op_id: operation.op_id,
                client_id: operation.client_id,
                invocation_point: operation.invocation_point,
                body_executions: operation.body_executions.clone(),
                notification_point: operation.notification_point,
                outcome: operation.outcome.unwrap_or(HistoryOutcome::Interrupted),
            })
            .collect()
    }
}

fn application_error(detail: impl fmt::Display) -> Error {
    Error::internal(format!("{APPLICATION_ERROR_MARKER}: {detail}"))
}

fn is_application_error(error: &Error) -> bool {
    matches!(error, Error::Internal { msg, .. } if msg.starts_with(APPLICATION_ERROR_MARKER))
}

fn byte_value(value: Option<Vec<u8>>, key: u8) -> Result<Option<u8>, Error> {
    match value {
        Some(value) if value.len() == 1 => Ok(Some(value[0])),
        Some(value) => Err(Error::internal(format!(
            "history key k{key} contains non-byte value {value:?}"
        ))),
        None => Ok(None),
    }
}

fn history_key(key: &[u8]) -> Result<u8, Error> {
    (0..HISTORY_KEY_COUNT as u8)
        .find(|candidate| key == key_name(*candidate as usize))
        .ok_or_else(|| Error::internal(format!("history scan returned unknown key {key:?}")))
}

async fn interpret_body(
    tx: Transaction,
    collection: &Collection,
    program: &HistoryTransaction,
    recorder: &HistoryRecorder,
) -> Result<(), Error> {
    // Install an incomplete attempt before the first await. If the enclosing
    // public future is dropped during this body, an earlier retry attempt that
    // reached commit cannot be mistaken for the interrupted attempt's effect.
    let body_number = recorder.begin_body(program.op_id);
    let mut registers = [None::<Option<u8>>; HISTORY_REGISTER_COUNT];
    let mut actions = Vec::new();
    let mut mutations = BTreeMap::new();
    let mut result = Ok(());

    for instruction in &program.instructions {
        result = match instruction {
            HistoryInstruction::Read { key, register } => {
                match tx.read(collection, &key_name(*key as usize)).await {
                    Ok(value) => match byte_value(value, *key) {
                        Ok(value) => {
                            registers[*register as usize] = Some(value);
                            actions.push(HistoryAction::Read { key: *key, value });
                            Ok(())
                        }
                        Err(error) => Err(error),
                    },
                    Err(error) => Err(error),
                }
            }
            HistoryInstruction::ReadGroup { reads } => {
                let keys = reads.iter().map(|(key, _)| *key).collect::<BTreeSet<_>>();
                let target_registers = reads
                    .iter()
                    .map(|(_, register)| *register)
                    .collect::<BTreeSet<_>>();
                if !(2..=HISTORY_REGISTER_COUNT).contains(&reads.len())
                    || keys.len() != reads.len()
                    || target_registers.len() != reads.len()
                    || keys.iter().any(|key| *key >= HISTORY_KEY_COUNT as u8)
                    || target_registers
                        .iter()
                        .any(|register| *register >= HISTORY_REGISTER_COUNT as u8)
                {
                    Err(Error::internal(format!(
                        "invalid bounded history read group {reads:?}"
                    )))
                } else {
                    let read_results =
                        futures::future::try_join_all(reads.iter().map(|(key, register)| {
                            let key = *key;
                            let register = *register;
                            let tx = &tx;
                            async move {
                                let name = key_name(key as usize);
                                let value = byte_value(tx.read(collection, &name).await?, key)?;
                                Ok::<_, Error>((key, register, value))
                            }
                        }))
                        .await;
                    match read_results {
                        Ok(read_results) => {
                            let mut observations = BTreeMap::new();
                            for (key, register, value) in read_results {
                                registers[register as usize] = Some(value);
                                observations.insert(key, value);
                            }
                            actions.push(HistoryAction::ReadGroup { observations });
                            Ok(())
                        }
                        Err(error) => Err(error),
                    }
                }
            }
            HistoryInstruction::WriteLiteral { key, value } => tx
                .write(collection, &key_name(*key as usize), &[*value])
                .map(|()| {
                    actions.push(HistoryAction::Write {
                        key: *key,
                        value: *value,
                    });
                    mutations.insert(*key, Some(*value));
                }),
            HistoryInstruction::WriteRegister { key, register } => {
                match registers[*register as usize] {
                    Some(Some(value)) => tx
                        .write(collection, &key_name(*key as usize), &[value])
                        .map(|()| {
                            actions.push(HistoryAction::Write { key: *key, value });
                            mutations.insert(*key, Some(value));
                        }),
                    _ => Err(application_error(format!(
                        "register r{register} has no present value"
                    ))),
                }
            }
            HistoryInstruction::WriteIncremented { key, register } => {
                match registers[*register as usize]
                    .and_then(|value| value)
                    .and_then(|value| value.checked_add(1))
                {
                    Some(value) => tx
                        .write(collection, &key_name(*key as usize), &[value])
                        .map(|()| {
                            actions.push(HistoryAction::Write { key: *key, value });
                            mutations.insert(*key, Some(value));
                        }),
                    None => Err(application_error(format!(
                        "register r{register} cannot be incremented"
                    ))),
                }
            }
            HistoryInstruction::Delete { key } => {
                tx.delete(collection, &key_name(*key as usize)).map(|()| {
                    actions.push(HistoryAction::Delete { key: *key });
                    mutations.insert(*key, None);
                })
            }
            HistoryInstruction::Scan {
                start,
                end,
                after,
                limit,
            } => {
                let start_key = key_name(*start as usize);
                let end_key = key_name(*end as usize);
                let after_key = after.map(|key| key_name(key as usize));
                let mut scan = KeyScan::range(&start_key, &end_key).limit(*limit as usize);
                if let Some(after_key) = &after_key {
                    scan = scan.after(after_key);
                }
                match tx.scan_keys(collection, scan).await {
                    Ok(page) => page
                        .into_keys()
                        .into_iter()
                        .map(|key| history_key(&key))
                        .collect::<Result<Vec<_>, _>>()
                        .map(|keys| {
                            let (start, start_exclusive) = match after {
                                Some(after) if after >= start => (*after, true),
                                _ => (*start, false),
                            };
                            actions.push(HistoryAction::Scan {
                                start,
                                start_exclusive,
                                end: *end,
                                limit: *limit,
                                keys,
                            });
                        }),
                    Err(error) => Err(error),
                }
            }
            HistoryInstruction::RequireEqual { register, expected } => {
                if registers[*register as usize] == Some(*expected) {
                    Ok(())
                } else {
                    Err(application_error(format!(
                        "r{register} is {:?}, expected {expected:?}",
                        registers[*register as usize]
                    )))
                }
            }
            HistoryInstruction::Abort => tx.abort(),
            HistoryInstruction::Yield => {
                rt::yield_now().await;
                Ok(())
            }
        };
        if result.is_err() {
            break;
        }
    }

    let body_state = match &result {
        Ok(()) => BodyState::CommitOutcome,
        Err(Error::Aborted) => BodyState::ExplicitAbort,
        Err(error) if is_application_error(error) => BodyState::ApplicationErrorOutcome,
        Err(_) => BodyState::EngineErrorOutcome,
    };
    recorder.complete_body(
        program.op_id,
        BodyTrace {
            body_number,
            ordered_actions: actions,
            final_mutations: mutations,
            state: body_state,
        },
    );
    result
}

async fn run_program(
    db: &Database,
    program: &HistoryTransaction,
    recorder: &HistoryRecorder,
) -> Result<(), Error> {
    let collection = db
        .open_collection(&CollectionPath::new(HISTORY_COLLECTION)?)
        .await?;
    recorder.begin(program);
    let result = db
        .tx(|tx| interpret_body(tx, &collection, program, recorder))
        .await;

    match result {
        Ok(()) => {
            recorder.notify(program.op_id, HistoryOutcome::SnapshotTransparent);
            Ok(())
        }
        Err(error) if is_application_error(&error) => {
            recorder.notify(program.op_id, HistoryOutcome::SnapshotTransparent);
            Ok(())
        }
        Err(Error::Aborted) => {
            recorder.notify(program.op_id, HistoryOutcome::DefiniteNoEffect);
            Ok(())
        }
        Err(error @ Error::InDoubt(_)) => {
            recorder.notify(program.op_id, HistoryOutcome::InDoubt);
            Err(error)
        }
        Err(error) => {
            recorder.notify(program.op_id, HistoryOutcome::DefiniteNoEffect);
            Err(error)
        }
    }
}

fn initial_state() -> AbstractState {
    (0..HISTORY_KEY_COUNT as u8).map(|key| (key, 0)).collect()
}

impl SimWorkload for HistoryWorkload {
    type Op = HistoryTransaction;
    type State = HistoryRecorder;

    fn clients(&self) -> &[Vec<Self::Op>] {
        &self.clients
    }

    fn new_state(&self) -> Self::State {
        HistoryRecorder::new(self)
    }

    fn open_db(
        backend: &Arc<dyn Backend>,
        media: Option<SimMedia>,
    ) -> impl Future<Output = Result<Database, Error>> + Send {
        // Three preseeded keys exceed the two-entry leaf cap. The same public
        // histories therefore cross a B-link split without exposing topology
        // to the oracle.
        open_det_db(backend, tiny_split_policy(), InlinePolicy::default(), media)
    }

    async fn seed(&self, db: &Database) {
        let collection = db
            .root_collection()
            .create_collection_if_absent(HISTORY_COLLECTION)
            .await
            .expect("create history collection");
        let collection = &collection;
        db.tx(|tx| async move {
            for key in 0..HISTORY_KEY_COUNT {
                tx.write(collection, &key_name(key), &[0])?;
            }
            Ok(())
        })
        .await
        .expect("seed history keys");
    }

    async fn run_op(db: &Database, op: &Self::Op, state: &Self::State) -> Result<(), Error> {
        run_program(db, op, state).await
    }

    async fn verify(&self, db: &Database, state: &Self::State, _failures_enabled: bool) {
        let collection = db
            .open_collection(&CollectionPath::new(HISTORY_COLLECTION).unwrap())
            .await
            .expect("open history collection for final read");
        let invocation = state.event_point();
        let collection = &collection;
        let final_state = db
            .tx(|tx| async move {
                let mut values = AbstractState::new();
                for key in 0..HISTORY_KEY_COUNT as u8 {
                    let value =
                        byte_value(tx.read(collection, &key_name(key as usize)).await?, key)?;
                    if let Some(value) = value {
                        values.insert(key, value);
                    }
                }
                Ok(values)
            })
            .await
            .expect("read final history state");
        let notification = state.event_point();
        state.add_final_read(invocation, notification, &final_state);
        let history = state.history();
        if let Err(error) = check_history(&initial_state(), &history, &final_state) {
            panic!("public transaction history is not strict serializable: {error}");
        }
    }

    fn spawn_observer(
        &self,
        _backbone: &Arc<dyn glassdb_backend::Backend>,
        _state: &Arc<Self::State>,
        _media: Option<SimMedia>,
    ) -> Option<rt::JoinHandle<()>> {
        // Point and concurrent-group reads are recorded inside explicit
        // transaction programs. The long-lived snapshot observer is
        // intentionally outside this pilot.
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(entries: &[(u8, u8)]) -> AbstractState {
        entries.iter().copied().collect()
    }

    fn trace(actions: Vec<HistoryAction>, state: BodyState) -> BodyTrace {
        let mut final_mutations = BTreeMap::new();
        for action in &actions {
            match action {
                HistoryAction::Read { .. }
                | HistoryAction::ReadGroup { .. }
                | HistoryAction::Scan { .. } => {}
                HistoryAction::Write { key, value } => {
                    final_mutations.insert(*key, Some(*value));
                }
                HistoryAction::Delete { key } => {
                    final_mutations.insert(*key, None);
                }
            }
        }
        BodyTrace {
            body_number: 0,
            ordered_actions: actions,
            final_mutations,
            state,
        }
    }

    fn read_group(entries: &[(u8, Option<u8>)]) -> HistoryAction {
        HistoryAction::ReadGroup {
            observations: entries.iter().copied().collect(),
        }
    }

    fn op(
        id: u64,
        invocation: u64,
        notification: Option<u64>,
        outcome: HistoryOutcome,
        trace: BodyTrace,
    ) -> PublicOp {
        PublicOp {
            op_id: id,
            client_id: id as usize,
            invocation_point: invocation,
            body_executions: vec![trace],
            notification_point: notification,
            outcome,
        }
    }

    fn increment(id: u64, invocation: u64, notification: u64, from: u8) -> PublicOp {
        op(
            id,
            invocation,
            Some(notification),
            HistoryOutcome::SnapshotTransparent,
            trace(
                vec![
                    HistoryAction::Read {
                        key: 0,
                        value: Some(from),
                    },
                    HistoryAction::Write {
                        key: 0,
                        value: from + 1,
                    },
                ],
                BodyState::CommitOutcome,
            ),
        )
    }

    fn brute_force_accepts(
        initial: &AbstractState,
        history: &[PublicOp],
        final_state: &AbstractState,
    ) -> bool {
        fn visit(
            state: &AbstractState,
            ops: &[LogicalOp<'_>],
            remaining: &mut Vec<usize>,
            final_state: &AbstractState,
        ) -> bool {
            if remaining.iter().all(|index| ops[*index].optional) && state == final_state {
                return true;
            }
            for position in 0..remaining.len() {
                let index = remaining[position];
                let candidate = &ops[index];
                let blocked = remaining.iter().any(|other| {
                    if *other == index || ops[*other].optional {
                        return false;
                    }
                    ops[*other]
                        .public
                        .notification_point
                        .is_some_and(|response| response < candidate.public.invocation_point)
                });
                if blocked {
                    continue;
                }
                let Ok(next) = apply_trace(state, candidate) else {
                    continue;
                };
                remaining.remove(position);
                if visit(&next, ops, remaining, final_state) {
                    return true;
                }
                remaining.insert(position, index);
            }
            false
        }

        let ops = history
            .iter()
            .filter_map(|public| {
                selected_trace(public)
                    .expect("generated cross-check history is well formed")
                    .map(|(trace, mutates, optional)| LogicalOp {
                        public,
                        trace,
                        mutates,
                        optional,
                        predecessors: 0,
                    })
            })
            .collect::<Vec<_>>();
        visit(initial, &ops, &mut (0..ops.len()).collect(), final_state)
    }

    #[test]
    fn memoized_checker_matches_small_brute_force_enumerator() {
        let initial = state(&[(0, 0)]);
        for first_read in [0, 1] {
            for first_write in [0, 1] {
                for second_read in [0, 1] {
                    for second_write in [0, 1] {
                        for first_optional in [false, true] {
                            for second_optional in [false, true] {
                                let history = vec![
                                    op(
                                        0,
                                        0,
                                        Some(3),
                                        if first_optional {
                                            HistoryOutcome::InDoubt
                                        } else {
                                            HistoryOutcome::SnapshotTransparent
                                        },
                                        trace(
                                            vec![
                                                HistoryAction::Read {
                                                    key: 0,
                                                    value: Some(first_read),
                                                },
                                                HistoryAction::Write {
                                                    key: 0,
                                                    value: first_write,
                                                },
                                            ],
                                            BodyState::CommitOutcome,
                                        ),
                                    ),
                                    op(
                                        1,
                                        1,
                                        Some(2),
                                        if second_optional {
                                            HistoryOutcome::InDoubt
                                        } else {
                                            HistoryOutcome::SnapshotTransparent
                                        },
                                        trace(
                                            vec![
                                                HistoryAction::Read {
                                                    key: 0,
                                                    value: Some(second_read),
                                                },
                                                HistoryAction::Write {
                                                    key: 0,
                                                    value: second_write,
                                                },
                                            ],
                                            BodyState::CommitOutcome,
                                        ),
                                    ),
                                ];
                                for final_value in [0, 1] {
                                    let final_state = state(&[(0, final_value)]);
                                    assert_eq!(
                                        check_history(&initial, &history, &final_state).is_ok(),
                                        brute_force_accepts(&initial, &history, &final_state),
                                        "history={history:#?}, final_state={final_state:?}"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn accepts_independent_transactions_in_every_observed_permutation() {
        let initial = state(&[(0, 0), (1, 0), (2, 0)]);
        let final_state = state(&[(0, 1), (1, 1), (2, 1)]);
        for order in [
            [0u8, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ] {
            let history = order
                .into_iter()
                .enumerate()
                .map(|(position, key)| {
                    op(
                        key as u64,
                        (position * 2) as u64,
                        Some((position * 2 + 1) as u64),
                        HistoryOutcome::SnapshotTransparent,
                        trace(
                            vec![HistoryAction::Write { key, value: 1 }],
                            BodyState::CommitOutcome,
                        ),
                    )
                })
                .collect::<Vec<_>>();
            check_history(&initial, &history, &final_state).unwrap();
        }
    }

    #[test]
    fn rejects_lost_update_and_accepts_serial_increment_control() {
        let initial = state(&[(0, 0)]);
        let lost = vec![increment(0, 0, 3, 0), increment(1, 1, 2, 0)];
        assert!(check_history(&initial, &lost, &state(&[(0, 1)])).is_err());

        let serial = vec![increment(0, 0, 3, 0), increment(1, 1, 2, 1)];
        check_history(&initial, &serial, &state(&[(0, 2)])).unwrap();
    }

    #[test]
    fn rejects_fractured_commit_and_accepts_atomic_control() {
        let initial = state(&[(0, 0), (1, 0)]);
        let history = vec![op(
            0,
            0,
            Some(1),
            HistoryOutcome::SnapshotTransparent,
            trace(
                vec![
                    HistoryAction::Write { key: 0, value: 1 },
                    HistoryAction::Write { key: 1, value: 1 },
                ],
                BodyState::CommitOutcome,
            ),
        )];
        assert!(check_history(&initial, &history, &state(&[(0, 1), (1, 0)])).is_err());
        check_history(&initial, &history, &state(&[(0, 1), (1, 1)])).unwrap();
    }

    #[test]
    fn rejects_stale_read_and_accepts_concurrent_control() {
        let initial = state(&[(0, 0)]);
        let write = op(
            0,
            0,
            Some(1),
            HistoryOutcome::SnapshotTransparent,
            trace(
                vec![HistoryAction::Write { key: 0, value: 1 }],
                BodyState::CommitOutcome,
            ),
        );
        let stale_read = op(
            1,
            2,
            Some(3),
            HistoryOutcome::SnapshotTransparent,
            trace(
                vec![HistoryAction::Read {
                    key: 0,
                    value: Some(0),
                }],
                BodyState::CommitOutcome,
            ),
        );
        assert!(check_history(&initial, &[write.clone(), stale_read], &state(&[(0, 1)])).is_err());

        let concurrent_read = op(
            1,
            0,
            Some(3),
            HistoryOutcome::SnapshotTransparent,
            trace(
                vec![HistoryAction::Read {
                    key: 0,
                    value: Some(0),
                }],
                BodyState::CommitOutcome,
            ),
        );
        let concurrent_write = PublicOp {
            invocation_point: 1,
            notification_point: Some(2),
            ..write
        };
        check_history(
            &initial,
            &[concurrent_write, concurrent_read],
            &state(&[(0, 1)]),
        )
        .unwrap();
    }

    #[test]
    fn read_group_accepts_one_snapshot_and_rejects_a_torn_observation() {
        let initial = state(&[(0, 0), (1, 0)]);
        let final_state = state(&[(0, 1), (1, 1)]);
        let writer = op(
            0,
            0,
            Some(3),
            HistoryOutcome::SnapshotTransparent,
            trace(
                vec![
                    HistoryAction::Write { key: 0, value: 1 },
                    HistoryAction::Write { key: 1, value: 1 },
                ],
                BodyState::CommitOutcome,
            ),
        );

        let before = op(
            1,
            1,
            Some(2),
            HistoryOutcome::SnapshotTransparent,
            trace(
                vec![read_group(&[(1, Some(0)), (0, Some(0))])],
                BodyState::CommitOutcome,
            ),
        );
        check_history(&initial, &[writer.clone(), before], &final_state).unwrap();

        let after = op(
            1,
            1,
            Some(2),
            HistoryOutcome::SnapshotTransparent,
            trace(
                vec![read_group(&[(0, Some(1)), (1, Some(1))])],
                BodyState::CommitOutcome,
            ),
        );
        check_history(&initial, &[writer.clone(), after], &final_state).unwrap();

        let torn = op(
            1,
            1,
            Some(2),
            HistoryOutcome::SnapshotTransparent,
            trace(
                vec![read_group(&[(0, Some(0)), (1, Some(1))])],
                BodyState::CommitOutcome,
            ),
        );
        assert!(check_history(&initial, &[writer, torn], &final_state).is_err());

        assert_eq!(
            read_group(&[(1, Some(0)), (0, Some(1))]),
            read_group(&[(0, Some(1)), (1, Some(0))]),
            "read-group representation must not encode task completion order"
        );
    }

    #[test]
    fn rejects_a_range_phantom_and_accepts_legal_scan_controls() {
        let initial = state(&[(0, 0)]);
        let insert = op(
            0,
            0,
            Some(3),
            HistoryOutcome::SnapshotTransparent,
            trace(
                vec![HistoryAction::Write { key: 1, value: 9 }],
                BodyState::CommitOutcome,
            ),
        );
        let scan_without_insert = op(
            1,
            1,
            Some(2),
            HistoryOutcome::SnapshotTransparent,
            trace(
                vec![HistoryAction::Scan {
                    start: 0,
                    start_exclusive: false,
                    end: 3,
                    limit: 3,
                    keys: vec![0],
                }],
                BodyState::CommitOutcome,
            ),
        );
        let final_state = state(&[(0, 0), (1, 9)]);

        // Overlap permits the scan to linearize before the insert.
        check_history(
            &initial,
            &[insert.clone(), scan_without_insert.clone()],
            &final_state,
        )
        .unwrap();

        let prior_insert = PublicOp {
            notification_point: Some(1),
            ..insert
        };
        let phantom = PublicOp {
            invocation_point: 2,
            notification_point: Some(3),
            ..scan_without_insert
        };
        assert!(check_history(&initial, &[prior_insert.clone(), phantom], &final_state,).is_err());

        let complete_scan = op(
            1,
            2,
            Some(3),
            HistoryOutcome::SnapshotTransparent,
            trace(
                vec![HistoryAction::Scan {
                    start: 0,
                    start_exclusive: false,
                    end: 3,
                    limit: 3,
                    keys: vec![0, 1],
                }],
                BodyState::CommitOutcome,
            ),
        );
        check_history(&initial, &[prior_insert, complete_scan], &final_state).unwrap();
    }

    #[test]
    fn scan_observes_the_local_membership_overlay_and_normalized_bounds() {
        let initial = state(&[(0, 0), (1, 1)]);
        let history = vec![op(
            0,
            0,
            Some(1),
            HistoryOutcome::SnapshotTransparent,
            trace(
                vec![
                    HistoryAction::Delete { key: 0 },
                    HistoryAction::Write { key: 2, value: 2 },
                    HistoryAction::Scan {
                        start: 0,
                        start_exclusive: true,
                        end: 3,
                        limit: 1,
                        keys: vec![1],
                    },
                ],
                BodyState::CommitOutcome,
            ),
        )];
        check_history(&initial, &history, &state(&[(1, 1), (2, 2)])).unwrap();
    }

    #[test]
    fn fuzz_program_decoder_reaches_bounded_scans() {
        // This is the workload prefix after the eight-byte RNG seed in the
        // committed history corpus. Keeping a scan here prevents the initial
        // corpus from degenerating into point-only coverage.
        let mut input = Unstructured::new(b"bBcbeeccaBcbha");
        let workload = HistoryWorkload::arbitrary(&mut input).unwrap();
        assert!(matches!(
            workload.clients[0][0].instructions.as_slice(),
            [
                HistoryInstruction::Scan {
                    start: 0,
                    end: 3,
                    after: None,
                    limit: 4,
                },
                HistoryInstruction::Delete { key: 0 },
            ]
        ));
    }

    #[test]
    fn generated_read_groups_are_bounded_and_feed_distinct_registers() {
        let mut input = Unstructured::new(&[0, 0, 1, 0, 1]);
        let program = arbitrary_program(&mut input).unwrap();
        assert!(matches!(
            program.as_slice(),
            [
                HistoryInstruction::ReadGroup { reads },
                HistoryInstruction::WriteIncremented {
                    key: 0,
                    register: 0,
                },
                HistoryInstruction::WriteIncremented {
                    key: 1,
                    register: 1,
                },
            ] if reads == &[(0, 0), (1, 1)]
        ));
    }

    #[test]
    fn rejects_real_time_inversion_and_accepts_overlapping_control() {
        let initial = state(&[(0, 0)]);
        let first = op(
            0,
            0,
            Some(1),
            HistoryOutcome::SnapshotTransparent,
            trace(
                vec![HistoryAction::Write { key: 0, value: 1 }],
                BodyState::CommitOutcome,
            ),
        );
        let second = op(
            1,
            2,
            Some(3),
            HistoryOutcome::SnapshotTransparent,
            trace(
                vec![HistoryAction::Write { key: 0, value: 2 }],
                BodyState::CommitOutcome,
            ),
        );
        assert!(
            check_history(
                &initial,
                &[first.clone(), second.clone()],
                &state(&[(0, 1)])
            )
            .is_err()
        );
        let overlapping = PublicOp {
            invocation_point: 0,
            notification_point: Some(3),
            ..second
        };
        let first = PublicOp {
            invocation_point: 1,
            notification_point: Some(2),
            ..first
        };
        check_history(&initial, &[first, overlapping], &state(&[(0, 1)])).unwrap();
    }

    #[test]
    fn optional_effects_apply_zero_or_once_and_reject_double_apply() {
        let initial = state(&[(0, 0)]);
        for outcome in [HistoryOutcome::InDoubt, HistoryOutcome::Interrupted] {
            let notification = (outcome == HistoryOutcome::InDoubt).then_some(1);
            let optional = op(
                0,
                0,
                notification,
                outcome,
                trace(
                    vec![
                        HistoryAction::Read {
                            key: 0,
                            value: Some(0),
                        },
                        HistoryAction::Write { key: 0, value: 1 },
                    ],
                    BodyState::CommitOutcome,
                ),
            );
            check_history(&initial, std::slice::from_ref(&optional), &initial).unwrap();
            check_history(&initial, std::slice::from_ref(&optional), &state(&[(0, 1)])).unwrap();
            assert!(check_history(&initial, &[optional], &state(&[(0, 2)])).is_err());
        }
    }

    #[test]
    fn interrupted_incomplete_retry_cannot_reuse_an_earlier_commit_outcome() {
        let initial = state(&[(0, 0)]);
        let mut interrupted = increment(0, 0, 1, 0);
        interrupted.notification_point = None;
        interrupted.outcome = HistoryOutcome::Interrupted;
        interrupted.body_executions.push(BodyTrace {
            body_number: 1,
            ordered_actions: Vec::new(),
            final_mutations: BTreeMap::new(),
            state: BodyState::Incomplete,
        });
        check_history(&initial, std::slice::from_ref(&interrupted), &initial).unwrap();
        assert!(check_history(&initial, &[interrupted], &state(&[(0, 1)])).is_err());
    }

    #[test]
    fn rejects_malformed_retry_numbers_and_mutation_summaries() {
        let initial = state(&[(0, 0)]);
        let mut wrong_number = increment(0, 0, 1, 0);
        wrong_number.body_executions[0].body_number = 1;
        assert!(check_history(&initial, &[wrong_number], &state(&[(0, 1)])).is_err());

        let mut wrong_mutation = increment(0, 0, 1, 0);
        wrong_mutation.body_executions[0]
            .final_mutations
            .insert(0, Some(9));
        assert!(check_history(&initial, &[wrong_mutation], &state(&[(0, 1)])).is_err());
    }

    #[test]
    fn application_error_is_an_error_outcome_without_writes() {
        let initial = state(&[(0, 4), (1, 0)]);
        let error = op(
            0,
            0,
            Some(1),
            HistoryOutcome::SnapshotTransparent,
            trace(
                vec![
                    HistoryAction::Read {
                        key: 0,
                        value: Some(4),
                    },
                    HistoryAction::Write { key: 1, value: 9 },
                ],
                BodyState::ApplicationErrorOutcome,
            ),
        );
        check_history(&initial, std::slice::from_ref(&error), &initial).unwrap();
        assert!(check_history(&initial, &[error], &state(&[(0, 4), (1, 9)])).is_err());
    }

    #[test]
    fn read_your_writes_uses_the_local_overlay() {
        let initial = state(&[(0, 0)]);
        let history = vec![op(
            0,
            0,
            Some(1),
            HistoryOutcome::SnapshotTransparent,
            trace(
                vec![
                    HistoryAction::Write { key: 0, value: 7 },
                    HistoryAction::Read {
                        key: 0,
                        value: Some(7),
                    },
                ],
                BodyState::CommitOutcome,
            ),
        )];
        check_history(&initial, &history, &state(&[(0, 7)])).unwrap();
    }
}
