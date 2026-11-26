//! Yield requests between database instances (ADR-110).
//!
//! On a busy leaf, the next coordinator round of a database instance sends its
//! CAS right after the last one lands. A writer on another instance must read
//! the leaf before its CAS, so it can lose almost every race and starve
//! (hack/perf/investigations.md, 2026-11-26). Only the writer that starves
//! knows it. When a key change of another writer rejects
//! [`REJECTIONS_TO_REQUEST`] leaf CASes in a row of a round, the instance
//! writes its yield request for the collection, and a later such round renews
//! it. An instance whose rounds of one leaf run back to back
//! for [`POLL_EVERY`] reads the requests of the collection, at most once in
//! that time. While a request of another instance has not expired, each round
//! of this instance in the collection that follows another round of its leaf
//! waits until the requested gap has passed since that round.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use glassdb_concurr::rt::{self, Instant};
use glassdb_data::{CollectionAddress, ObjectPath};
use glassdb_storage::{StorageError, YieldRequest, YieldRequestStore};

/// The leaf CASes in a row of one round that a key change of another writer
/// rejects before the instance asks the other instances to yield. In the
/// measured loads, rounds without starvation seldom have this many, and 20
/// let two instances that both carry a load starve each other again
/// (hack/perf/investigations.md, 2026-11-26).
pub(super) const REJECTIONS_TO_REQUEST: u32 = 10;

/// How long a request lasts after the round that wrote it.
const REQUEST_LIFETIME: Duration = Duration::from_secs(5);

/// The shortest time between two writes of the request of an instance for
/// one collection.
const RENEW_AFTER: Duration = Duration::from_secs(1);

/// How long the rounds of a leaf must run back to back before the instance
/// reads the requests of the collection, and the shortest time between two
/// reads of them.
const POLL_EVERY: Duration = Duration::from_secs(1);

/// The longest time from the end of one round of a leaf to the start of the
/// next one for which the two rounds run back to back.
const BACK_TO_BACK: Duration = Duration::from_millis(5);

/// The limits of the requested gap. The gap is 3/2 of the time of the last
/// load and CAS of the round that writes the request.
const MIN_GAP: Duration = Duration::from_millis(20);
const MAX_GAP: Duration = Duration::from_millis(500);

/// The fewest leaves or collections that the state keeps before it removes
/// those that it no longer needs.
const MIN_PRUNE_LEN: usize = 64;

/// The yield state of the leaf coordinator of one database instance.
pub(super) struct Yielding {
    /// A random token of this database instance.
    instance: u64,
    store: YieldRequestStore,
    state: Mutex<State>,
}

struct State {
    leaves: HashMap<ObjectPath, LeafRounds>,
    collections: HashMap<CollectionAddress, CollectionYield>,
    /// The length of `leaves` and `collections` at which they next remove
    /// the entries that they no longer need.
    prune_at: usize,
}

/// The recent rounds of one leaf on this instance.
struct LeafRounds {
    /// The end of the last round.
    ended: Instant,
    /// The start of the run of back-to-back rounds that the last round
    /// belongs to.
    busy_since: Instant,
}

#[derive(Default)]
struct CollectionYield {
    /// When this instance last wrote its request.
    requested: Option<Instant>,
    /// When this instance last read the requests.
    polled: Option<Instant>,
    /// Until when this instance yields, and the gap.
    yields: Option<(Instant, Duration)>,
}

impl CollectionYield {
    fn is_stale(&self, now: Instant) -> bool {
        let old = |at: Option<Instant>, keep: Duration| {
            at.is_none_or(|at| now.saturating_duration_since(at) >= keep)
        };
        old(self.requested, RENEW_AFTER)
            && old(self.polled, POLL_EVERY)
            && self.yields.is_none_or(|(until, _)| until <= now)
    }
}

/// What a round of a leaf must do for the yield requests of its collection.
pub(super) struct BeforeRound {
    /// The collection whose requests the round must read while it runs.
    pub(super) poll: Option<CollectionAddress>,
    /// How long the round must wait before its first leaf load.
    pub(super) wait: Option<Duration>,
}

fn collection_of(path: &ObjectPath) -> Option<&CollectionAddress> {
    match path {
        ObjectPath::TreeRoot { collection } | ObjectPath::Node { collection, .. } => {
            Some(collection)
        }
        _ => None,
    }
}

impl Yielding {
    pub(super) fn new(store: YieldRequestStore) -> Self {
        let mut bytes = [0; 8];
        glassdb_concurr::entropy::fill_bytes(&mut bytes);
        Self {
            instance: u64::from_be_bytes(bytes),
            store,
            state: Mutex::new(State {
                leaves: HashMap::new(),
                collections: HashMap::new(),
                prune_at: MIN_PRUNE_LEN,
            }),
        }
    }

    /// Returns what a round of `path` that starts now must do for the yield
    /// requests of its collection.
    pub(super) fn before_round(&self, path: &ObjectPath) -> BeforeRound {
        let mut before = BeforeRound {
            poll: None,
            wait: None,
        };
        let Some(collection) = collection_of(path) else {
            return before;
        };
        let now = Instant::now();
        let mut guard = self.state.lock().unwrap();
        let state = &mut *guard;
        let Some(rounds) = state.leaves.get_mut(path) else {
            return before;
        };
        let since_end = now.saturating_duration_since(rounds.ended);
        if since_end > BACK_TO_BACK {
            rounds.busy_since = now;
        }
        let busy_for = now.saturating_duration_since(rounds.busy_since);
        let entry = state.collections.entry(collection.clone()).or_default();
        if busy_for >= POLL_EVERY
            && entry
                .polled
                .is_none_or(|at| now.saturating_duration_since(at) >= POLL_EVERY)
        {
            entry.polled = Some(now);
            before.poll = Some(collection.clone());
        }
        match entry.yields {
            Some((until, gap)) if until > now => before.wait = gap.checked_sub(since_end),
            Some(_) => entry.yields = None,
            None => {}
        }
        before
    }

    /// Records the end of a round of `path`.
    pub(super) fn round_ended(&self, path: &ObjectPath) {
        let now = Instant::now();
        let mut state = self.state.lock().unwrap();
        match state.leaves.get_mut(path) {
            Some(rounds) => rounds.ended = now,
            None => {
                state.leaves.insert(
                    path.clone(),
                    LeafRounds {
                        ended: now,
                        busy_since: now,
                    },
                );
            }
        }
        state.prune(now);
    }

    /// Reads the requests of `collection`, and yields it while a request of
    /// another instance has not expired. A failed read changes nothing.
    pub(super) async fn poll(&self, collection: &CollectionAddress) {
        let Ok(observed) = self.store.read(collection).await else {
            return;
        };
        let wall_now = rt::system_now();
        let now = Instant::now();
        let yields = observed
            .value()
            .into_iter()
            .flat_map(|requests| requests.requests())
            .filter(|request| request.instance != self.instance)
            .filter_map(|request| {
                let left = request.expires_at.duration_since(wall_now).ok()?;
                // A clock ahead of this one cannot make a request last
                // longer than its lifetime.
                Some((now + left.min(REQUEST_LIFETIME), request.gap))
            })
            .reduce(|(a_until, a_gap), (b_until, b_gap)| (a_until.max(b_until), a_gap.max(b_gap)));
        let mut state = self.state.lock().unwrap();
        state
            .collections
            .entry(collection.clone())
            .or_default()
            .yields = yields;
    }

    /// Writes or renews the request of this instance for the collection of
    /// `path`, after a key change of another writer rejected
    /// [`REJECTIONS_TO_REQUEST`] leaf CASes in a row of a round.
    /// The last load and CAS of the round took `attempt`. A failed write
    /// changes nothing: the next long streak writes again.
    pub(super) async fn request(&self, path: &ObjectPath, attempt: Duration) {
        let Some(collection) = collection_of(path) else {
            return;
        };
        let now = Instant::now();
        {
            let mut state = self.state.lock().unwrap();
            let entry = state.collections.entry(collection.clone()).or_default();
            if entry
                .requested
                .is_some_and(|at| now.saturating_duration_since(at) < RENEW_AFTER)
            {
                return;
            }
            entry.requested = Some(now);
        }
        let request = YieldRequest {
            instance: self.instance,
            expires_at: rt::system_now() + REQUEST_LIFETIME,
            gap: (attempt * 3 / 2).clamp(MIN_GAP, MAX_GAP),
        };
        if let Err(error) = self.write(collection, request).await {
            tracing::debug!(%error, "yield request not written");
        }
    }

    async fn write(
        &self,
        collection: &CollectionAddress,
        request: YieldRequest,
    ) -> Result<(), StorageError> {
        // The requests of other instances change rarely, so a few attempts
        // are enough.
        for _ in 0..3 {
            let observed = self.store.read(collection).await?;
            let requests = observed
                .value()
                .map(|requests| (**requests).clone())
                .unwrap_or_default()
                .with(request.clone(), rt::system_now());
            if self.store.write(&observed, requests).await? {
                return Ok(());
            }
        }
        Ok(())
    }
}

impl State {
    /// Removes the leaves and collections that no longer need state, once
    /// the state has grown since the last removal.
    fn prune(&mut self, now: Instant) {
        if self.leaves.len() + self.collections.len() < self.prune_at {
            return;
        }
        // A leaf whose last round ended longer ago than the largest gap
        // neither runs back to back nor waits.
        let keep = MAX_GAP.max(BACK_TO_BACK);
        self.leaves
            .retain(|_, rounds| now.saturating_duration_since(rounds.ended) < keep);
        self.collections.retain(|_, entry| !entry.is_stale(now));
        self.prune_at = ((self.leaves.len() + self.collections.len()) * 2).max(MIN_PRUNE_LEN);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use glassdb_backend::memory::MemoryBackend;
    use glassdb_data::NodeId;
    use glassdb_storage::{CachedStore, Timeline, YieldRequests};

    use super::*;

    fn collection() -> CollectionAddress {
        CollectionAddress::root("yield")
    }

    fn leaf() -> ObjectPath {
        ObjectPath::Node {
            collection: collection(),
            id: NodeId::from_bytes([1; 16]),
        }
    }

    fn store() -> YieldRequestStore {
        YieldRequestStore::new(CachedStore::new(
            Arc::new(MemoryBackend::new()),
            1 << 20,
            Timeline::new(),
            None,
        ))
    }

    const GAP: Duration = Duration::from_millis(100);

    async fn write_request(store: &YieldRequestStore, instance: u64, lifetime: Duration) {
        let observed = store.read(&collection()).await.unwrap();
        let request = YieldRequest {
            instance,
            expires_at: rt::system_now() + lifetime,
            gap: GAP,
        };
        let requests = YieldRequests::default().with(request, rt::system_now());
        assert!(store.write(&observed, requests).await.unwrap());
    }

    // Runs rounds of `leaf()` back to back for `busy`, and returns the
    // collection that the last one must poll.
    async fn run_back_to_back(yielding: &Yielding, busy: Duration) -> Option<CollectionAddress> {
        let start = Instant::now();
        let mut poll = None;
        while start.elapsed() < busy {
            yielding.round_ended(&leaf());
            rt::sleep(BACK_TO_BACK / 2).await;
            poll = yielding.before_round(&leaf()).poll;
        }
        poll
    }

    // The poll costs a backend read, so an instance reads the requests only
    // while it keeps a leaf of the collection busy for a whole poll interval,
    // and then at most once in each interval.
    #[tokio::test(start_paused = true)]
    async fn an_instance_polls_only_while_a_leaf_stays_busy() {
        let yielding = Yielding::new(store());
        assert_eq!(run_back_to_back(&yielding, POLL_EVERY / 2).await, None);
        // A break restarts the busy time.
        yielding.round_ended(&leaf());
        rt::sleep(BACK_TO_BACK * 2).await;
        assert_eq!(yielding.before_round(&leaf()).poll, None);
        assert_eq!(run_back_to_back(&yielding, POLL_EVERY / 2).await, None);

        assert_eq!(
            run_back_to_back(&yielding, POLL_EVERY / 2).await,
            Some(collection())
        );
        assert_eq!(run_back_to_back(&yielding, POLL_EVERY / 2).await, None);
    }

    // Regression: the next round of a busy leaf sent its CAS right
    // after the last one landed, so a writer on another instance lost almost
    // every race and starved for tens of seconds. While a request of another
    // instance lasts, a round that follows another round of its leaf must
    // wait for the requested gap.
    #[tokio::test(start_paused = true)]
    async fn a_round_waits_for_the_gap_only_while_another_instance_requests_it() {
        // (name, instance of the request, its lifetime, whether rounds wait)
        let cases = [
            ("other instance", Some(1), REQUEST_LIFETIME, true),
            ("expired", Some(1), Duration::ZERO, false),
            ("no request", None, REQUEST_LIFETIME, false),
        ];
        for (name, instance, lifetime, waits) in cases {
            let store = store();
            let yielding = Yielding::new(store.clone());
            if let Some(instance) = instance {
                write_request(&store, instance, lifetime).await;
            }
            yielding.poll(&collection()).await;

            yielding.round_ended(&leaf());
            let wait = yielding.before_round(&leaf()).wait;
            assert_eq!(wait.is_some_and(|wait| wait == GAP), waits, "{name}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_instance_ignores_its_own_request_and_yields_until_the_request_expires() {
        let store = store();
        let yielding = Yielding::new(store.clone());
        yielding.request(&leaf(), GAP).await;
        yielding.poll(&collection()).await;
        yielding.round_ended(&leaf());
        assert_eq!(yielding.before_round(&leaf()).wait, None, "own request");

        let other = Yielding::new(store);
        other.poll(&collection()).await;
        other.round_ended(&leaf());
        // The gap is 3/2 of the load and CAS time of the requesting round.
        assert_eq!(other.before_round(&leaf()).wait, Some(GAP * 3 / 2));
        rt::sleep(REQUEST_LIFETIME).await;
        other.round_ended(&leaf());
        assert_eq!(other.before_round(&leaf()).wait, None, "expired");
    }

    // A round whose CASes keep failing writes the request at most once in
    // each renewal interval, so a starving instance does not add a write to
    // each rejected CAS.
    #[tokio::test(start_paused = true)]
    async fn a_request_is_renewed_at_most_once_in_each_interval() {
        let store = store();
        let yielding = Yielding::new(store.clone());
        let expiry = async || {
            let observed = store.read(&collection()).await.unwrap();
            observed.value().unwrap().requests()[0].expires_at
        };
        yielding.request(&leaf(), GAP).await;
        let first = expiry().await;
        rt::sleep(RENEW_AFTER / 2).await;
        yielding.request(&leaf(), GAP).await;
        assert_eq!(expiry().await, first);
        rt::sleep(RENEW_AFTER / 2).await;
        yielding.request(&leaf(), GAP).await;
        assert!(expiry().await > first);
    }

    // The state keeps one entry for each recent leaf. Under rounds over many
    // leaves, it must keep only the leaves that can still run back to back
    // or wait for a gap.
    #[tokio::test(start_paused = true)]
    async fn the_state_removes_leaves_without_recent_rounds() {
        let yielding = Yielding::new(store());
        for i in 0..1000u16 {
            let mut id = [0; 16];
            id[..2].copy_from_slice(&i.to_be_bytes());
            yielding.round_ended(&ObjectPath::Node {
                collection: collection(),
                id: NodeId::from_bytes(id),
            });
            rt::sleep(Duration::from_millis(10)).await;
        }
        let kept = yielding.state.lock().unwrap().leaves.len();
        assert!(kept < 2 * MIN_PRUNE_LEN, "{kept}");
    }
}
