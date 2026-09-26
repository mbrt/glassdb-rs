//! A topology policy that also counts what other policies would decide on its
//! windows, so that one measurement of a tree can test several rules.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use glassdb::{
    LeafWindow, PairWindow, TopologyChange, TopologyPolicy, TopologyWindow, TransactionCounts,
};
use serde::Serialize;

/// The windows that the policies of one cell saw, and the changes that each
/// shadow policy would decide on them.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ShadowCounts {
    windows: u64,
    /// The sum over the windows of their active leaves.
    leaf_windows: u64,
    policies: Vec<ShadowDecisions>,
    /// Each window with its decisions, if the tally traces them.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    trace: Vec<WindowTrace>,
}

/// The changes that one shadow policy would decide.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct ShadowDecisions {
    policy: String,
    splits: u64,
    merges: u64,
}

/// The measurements of one window of one database instance, and the changes
/// that each shadow policy decided on them.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct WindowTrace {
    database: usize,
    /// The wall time from the last reset of the tally to the window.
    at_ms: f64,
    elapsed_ms: f64,
    split_ms: f64,
    merge_ms: f64,
    leaves: Vec<LeafTrace>,
    pairs: Vec<PairTrace>,
    decisions: Vec<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct LeafTrace {
    leaf: String,
    lost_cas_ms: f64,
    queue_wait_ms: f64,
    slow_cas_ms: f64,
    inline_pressure_ms: f64,
    split_key: Option<String>,
    entries: Option<usize>,
    split_recently: bool,
    merged_recently: bool,
    committed: CommitTrace,
    divided: CommitTrace,
    latency_ms: f64,
    divided_time_ms: f64,
    rounds: u64,
    round_members: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct CommitTrace {
    direct: u64,
    locked: u64,
    read_only: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct PairTrace {
    left: String,
    right: String,
    adjacent_miss_ms: f64,
    scan_crossing_ms: f64,
}

/// Collects the counts of the database instances of one cell.
#[derive(Debug)]
pub(crate) struct ShadowTally {
    trace: bool,
    state: Mutex<(Instant, ShadowCounts)>,
}

/// Decides with one policy, and counts what each shadow policy would decide
/// on the same windows. The shadow decisions do not change the tree.
pub(super) struct Shadowed {
    pub(super) policy: Box<dyn TopologyPolicy>,
    pub(super) shadows: Vec<Box<dyn TopologyPolicy>>,
    pub(super) tally: Arc<ShadowTally>,
    /// The index of the database instance in its cell.
    pub(super) database: usize,
}

impl ShadowTally {
    /// Creates a tally for the shadow policies with these labels, which also
    /// keeps each window if `trace` is set.
    pub(crate) fn new(labels: impl IntoIterator<Item = String>, trace: bool) -> Self {
        let policies = labels
            .into_iter()
            .map(|policy| ShadowDecisions {
                policy,
                splits: 0,
                merges: 0,
            })
            .collect();
        let counts = ShadowCounts {
            policies,
            ..ShadowCounts::default()
        };
        Self {
            trace,
            state: Mutex::new((Instant::now(), counts)),
        }
    }

    /// Returns and resets the counts since the last call.
    pub(crate) fn take(&self) -> ShadowCounts {
        let mut state = self.state.lock().unwrap();
        state.0 = Instant::now();
        let counts = &mut state.1;
        let taken = ShadowCounts {
            windows: std::mem::take(&mut counts.windows),
            leaf_windows: std::mem::take(&mut counts.leaf_windows),
            policies: counts.policies.clone(),
            trace: std::mem::take(&mut counts.trace),
        };
        for decisions in &mut counts.policies {
            decisions.splits = 0;
            decisions.merges = 0;
        }
        taken
    }

    fn record(&self, database: usize, window: &TopologyWindow, decided: &[Vec<TopologyChange>]) {
        let mut state = self.state.lock().unwrap();
        let at = state.0.elapsed();
        let counts = &mut state.1;
        counts.windows += 1;
        counts.leaf_windows += window.leaves.len() as u64;
        for (decisions, changes) in counts.policies.iter_mut().zip(decided) {
            for change in changes {
                match change {
                    TopologyChange::Split(_) | TopologyChange::SplitAt(..) => decisions.splits += 1,
                    TopologyChange::Merge(_) => decisions.merges += 1,
                }
            }
        }
        if self.trace {
            counts
                .trace
                .push(WindowTrace::new(database, at, window, decided));
        }
    }
}

impl TopologyPolicy for Shadowed {
    fn window(&self) -> Duration {
        self.policy.window()
    }

    fn decide(&self, window: &TopologyWindow) -> Vec<TopologyChange> {
        let decided: Vec<_> = self
            .shadows
            .iter()
            .map(|shadow| shadow.decide(window))
            .collect();
        self.tally.record(self.database, window, &decided);
        self.policy.decide(window)
    }
}

impl WindowTrace {
    fn new(
        database: usize,
        at: Duration,
        window: &TopologyWindow,
        decided: &[Vec<TopologyChange>],
    ) -> Self {
        Self {
            database,
            at_ms: millis(at),
            elapsed_ms: millis(window.elapsed),
            split_ms: millis(window.split_time),
            merge_ms: millis(window.merge_time),
            leaves: window
                .leaves
                .iter()
                .map(|(id, leaf)| LeafTrace::new(id.to_string(), leaf))
                .collect(),
            pairs: window.pairs.iter().map(PairTrace::new).collect(),
            decisions: decided
                .iter()
                .map(|changes| changes.iter().map(describe).collect())
                .collect(),
        }
    }
}

impl LeafTrace {
    fn new(leaf: String, window: &LeafWindow) -> Self {
        let avoidable = &window.avoidable;
        Self {
            leaf,
            lost_cas_ms: millis(avoidable.lost_cas),
            queue_wait_ms: millis(avoidable.queue_wait),
            slow_cas_ms: millis(avoidable.slow_cas),
            inline_pressure_ms: millis(avoidable.inline_pressure),
            split_key: window
                .split_key
                .as_deref()
                .map(|key| String::from_utf8_lossy(key).into_owned()),
            entries: window.size.map(|size| size.entries),
            split_recently: window.split_recently,
            merged_recently: window.merged_recently,
            committed: CommitTrace::new(&window.committed),
            divided: CommitTrace::new(&window.divided),
            latency_ms: millis(window.latency),
            divided_time_ms: millis(window.divided_time),
            rounds: window.rounds,
            round_members: window.round_members,
        }
    }
}

impl CommitTrace {
    fn new(counts: &TransactionCounts) -> Self {
        Self {
            direct: counts.direct,
            locked: counts.locked,
            read_only: counts.read_only,
        }
    }
}

impl PairTrace {
    fn new(pair: &PairWindow) -> Self {
        Self {
            left: pair.left.to_string(),
            right: pair.right.to_string(),
            adjacent_miss_ms: millis(pair.avoidable.adjacent_miss),
            scan_crossing_ms: millis(pair.avoidable.scan_crossing),
        }
    }
}

fn describe(change: &TopologyChange) -> String {
    match change {
        TopologyChange::Split(id) => format!("split {id}"),
        TopologyChange::SplitAt(id, key) => {
            format!("split {id} at {}", String::from_utf8_lossy(key))
        }
        TopologyChange::Merge(id) => format!("merge {id}"),
    }
}

fn millis(time: Duration) -> f64 {
    time.as_secs_f64() * 1000.0
}
