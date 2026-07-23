//! The causally coordinated decoded object cache (ADR-036, ADR-043).
//!
//! One database-local cached object store sits between the [`Backend`] and every
//! typed storage abstraction. All typed stores share this single byte-bounded
//! LRU, keyed by physical object path; each supplies its own encoding, decoding,
//! and decoded-size accounting through a [`Codec`]. A path has exactly one
//! decoded type, so reading it back through a different typed store is an
//! internal error.
//!
//! Requirement is a local currentness watermark, not a durable guarantee. Each
//! cache entry is `Present` (a decoded value, its [`Revision`], and a
//! current-after [`SequencePoint`]), `Absent` (a current-after watermark,
//! no revision), or uncertain (no entry: no usable discoverable knowledge).
//! successful read returns an [`Observation`]
//! that references monotonic currentness evidence shared with the current cache
//! entry; the observation stays usable even after that entry is evicted or
//! invalidated, because invalidation changes what a *new* read may use but does
//! not revoke the historical fact that the observed state was current after its
//! watermark.
//!
//! Reads take a [`Requirement`]: `Any` accepts any usable cached entry and reads
//! the backend on a miss; `AtLeast(T)` accepts an entry only when its watermark
//! is at least `T`, otherwise it checks through the backend. Actual same-path
//! backend calls are serialized, and the store allocates an invocation point
//! immediately before dispatch. Reconciliation happens before the path lane is
//! released and before the operation becomes ready.

use std::any::Any;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use glassdb_backend::{self as backend, Backend, BackendError};
use glassdb_concurr::{rt, shard::Sharded};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use crate::cache::{Cache, Weighable};
use crate::cache_stats::{CacheMetrics, CacheStats};
#[cfg(test)]
use crate::disk_cache::PersistentCacheConfig;
use crate::disk_cache::{EncodedBody, FenceGuard, PathFence, PersistentCache};
use crate::error::StorageError;
use crate::timeline::{SequencePoint, Timeline};

const PERSISTENT_CACHE_LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Encoding, decoding, and decoded-size accounting for one physical object type.
///
/// Each typed store supplies its own codec; the cache holds the decoded value
/// so an object is decoded once per changed revision rather than once per hit.
pub(crate) trait Codec: Send + Sync + 'static {
    /// The decoded, immutable value cached for this object type.
    type Value: Send + Sync + 'static;

    /// Decodes an object body into its cached value.
    fn decode(path: &str, bytes: &[u8]) -> Result<Self::Value, StorageError>;

    /// Encodes a cached value back into its object body (the CAS unit).
    fn encode(value: &Self::Value) -> Result<Vec<u8>, StorageError>;

    /// Estimates the decoded value's in-memory size in bytes, governing
    /// eviction.
    fn size(value: &Self::Value) -> usize;

    /// Reports whether `path` names an object handled by this codec.
    fn valid_path(path: &str) -> bool;

    /// Describes this physical object type in diagnostics.
    fn name() -> &'static str;
}

/// The cached store's opaque content-CAS token, wrapping the backend version.
///
/// Higher layers may retain, compare, and pass a revision (and, where recovery
/// requires it, serialize the underlying backend version), but do not interpret
/// or manufacture one.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Revision(backend::Version);

impl Revision {
    fn version(&self) -> &backend::Version {
        &self.0
    }

    /// Returns the provider token for durable recovery metadata.
    pub fn serialize(&self) -> &str {
        &self.0.token
    }
}

/// The freshness requirement a cached entry must satisfy before it is served.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Requirement {
    /// Accept any usable cached entry without a backend check; read the
    /// backend only on a miss.
    Any,
    /// Accept an entry only when its watermark is at least this time; otherwise
    /// check through the backend.
    AtLeast(SequencePoint),
}

impl Requirement {
    /// Returns the stronger of two requirements.
    pub fn stricter(self, other: Self) -> Self {
        match (self, other) {
            (Requirement::Any, requirement) | (requirement, Requirement::Any) => requirement,
            (Requirement::AtLeast(left), Requirement::AtLeast(right)) => {
                Requirement::AtLeast(left.max(right))
            }
        }
    }

    /// Builds a requirement accepting evidence no older than `max_staleness`
    /// on `timeline`.
    pub fn within(timeline: &Timeline, max_staleness: Duration) -> Self {
        if max_staleness == Duration::MAX {
            Requirement::Any
        } else {
            // An explicit bounded-staleness read has no transaction validation
            // barrier or mutation receipt to inherit, so its policy must sample
            // the database timeline here.
            Requirement::AtLeast(timeline.approximate_cutoff(max_staleness))
        }
    }
}

/// The outcome of a conditional mutation (create or compare-and-swap).
#[derive(Debug)]
pub enum CasResult<V> {
    /// The mutation landed; the installed state's observation.
    Committed(Observation<V>),
    /// The precondition failed: the starting revision or cached absence was
    /// obsolete. The exact starting entry has been invalidated.
    Conflict,
}

impl<V> CasResult<V> {
    /// Reports whether the mutation committed.
    pub fn committed(&self) -> bool {
        matches!(self, CasResult::Committed(_))
    }

    /// Returns the committed observation, or `None` on conflict.
    pub fn into_observation(self) -> Option<Observation<V>> {
        match self {
            CasResult::Committed(o) => Some(o),
            CasResult::Conflict => None,
        }
    }
}

/// The outcome of checking whether a retained observation is still current.
#[derive(Debug)]
pub enum ObservationCheck<V> {
    /// The observed state is still current after the required bound; its
    /// watermark has been advanced if a backend round-trip confirmed it.
    Current,
    /// The state changed; here is the current observation.
    Changed(Observation<V>),
}

/// A shared, monotonically-advanceable currentness watermark. Observations of one
/// state and that state's current cache entry hold clones of the same cell, so
/// checking advances the evidence every holder sees. An `Arc` held by a caller
/// outlives eviction of the corresponding cache entry.
#[derive(Debug, Clone)]
struct Evidence(Arc<AtomicU64>);

impl Evidence {
    fn new(t: SequencePoint) -> Self {
        Evidence(Arc::new(AtomicU64::new(t.raw())))
    }

    fn get(&self) -> SequencePoint {
        SequencePoint::from_raw(self.0.load(Ordering::SeqCst))
    }

    /// Advances the watermark to at least `t`, never regressing it.
    fn advance(&self, t: SequencePoint) {
        self.0.fetch_max(t.raw(), Ordering::SeqCst);
    }
}

/// An exact observed state of one object, returned by a successful read or
/// mutation. It carries the decoded value (or absence), the [`Revision`], and a
/// reference to the shared currentness evidence. It remains inspectable after the
/// state is evicted or invalidated as the current cache entry.
#[derive(Debug, Clone)]
pub struct Observation<V> {
    path: Arc<str>,
    value: Option<Arc<V>>,
    revision: Option<Revision>,
    evidence: Evidence,
    cache_hit: bool,
}

impl<V> Observation<V> {
    /// The decoded value, or `None` for an observed absence.
    pub fn value(&self) -> Option<&Arc<V>> {
        self.value.as_ref()
    }

    /// Consumes the observation, yielding the decoded value (or `None`).
    pub fn into_value(self) -> Option<Arc<V>> {
        self.value
    }

    /// Reports whether the observed state has a value (is not an absence).
    pub fn exists(&self) -> bool {
        self.value.is_some()
    }

    /// Reports whether the observed state is absent.
    pub fn is_absent(&self) -> bool {
        self.value.is_none()
    }

    /// The observed revision, or `None` for an absence.
    pub fn revision(&self) -> Option<&Revision> {
        self.revision.as_ref()
    }

    /// The watermark after which the state was known to be current.
    pub fn current_after(&self) -> SequencePoint {
        self.evidence.get()
    }

    /// The object path this observation refers to.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Reports whether the observation reused a cached decoded body.
    pub fn cache_hit(&self) -> bool {
        self.cache_hit
    }

    /// Reports whether two observations refer to the same exact state.
    ///
    /// Observations of one state normally share the same evidence cell, so
    /// pointer identity is the fast path. But a cache eviction and reload mint a
    /// fresh evidence cell for the very same committed version, so two
    /// observations of the same path and revision are still the same state.
    pub fn same_state(&self, other: &Self) -> bool {
        if Arc::ptr_eq(&self.evidence.0, &other.evidence.0) {
            return true;
        }
        match (&self.revision, &other.revision) {
            (Some(mine), Some(theirs)) => self.path == other.path && mine == theirs,
            _ => false,
        }
    }
}

/// One entry in the shared decoded LRU: either a present decoded value or a
/// confirmed absence. A missing object has no entry at all.
#[derive(Clone)]
enum EntryState {
    Present {
        value: Arc<dyn Any + Send + Sync>,
        size: usize,
        revision: Revision,
        evidence: Evidence,
    },
    Absent {
        evidence: Evidence,
    },
}

#[derive(Clone)]
struct CacheEntry {
    state: EntryState,
}

impl Weighable for CacheEntry {
    fn size(&self) -> usize {
        // A present entry weighs its decoded size plus the revision token; an
        // absent entry costs a small fixed bookkeeping amount.
        const OVERHEAD: usize = std::mem::size_of::<CacheEntry>();
        match &self.state {
            EntryState::Present { size, revision, .. } => size + revision.0.token.len() + OVERHEAD,
            EntryState::Absent { .. } => OVERHEAD,
        }
    }
}

/// The raw, type-erased result of a backend fetch, shared across coalesced
/// waiters. Cheaply cloneable so one in-flight check can serve many.
#[derive(Clone)]
struct FetchResult {
    value: Option<Arc<dyn Any + Send + Sync>>,
    revision: Option<Revision>,
    evidence: Evidence,
    cache_hit: bool,
}

#[derive(Clone)]
enum FlightOutcome {
    Success(FetchResult),
    Error(StorageError),
    Cancelled,
}

/// One in-flight backend currentness check of a path, tracked for coalescing.
struct InFlight {
    invoked: SequencePoint,
    outcome: Mutex<Option<FlightOutcome>>,
    notify: Notify,
}

impl InFlight {
    async fn wait(&self) -> FlightOutcome {
        loop {
            let notified = self.notify.notified();
            if let Some(outcome) = self.outcome.lock().unwrap().clone() {
                return outcome;
            }
            notified.await;
        }
    }

    fn finish(&self, outcome: FlightOutcome) {
        let mut slot = self.outcome.lock().unwrap();
        if slot.is_none() {
            *slot = Some(outcome);
            self.notify.notify_waiters();
        }
    }
}

// Coordination has a different lifetime from cached knowledge, so it uses the
// same sharding policy as the cache without sharing its storage or locks.
type PathMapShard = Mutex<HashMap<Arc<str>, Weak<PathState>>>;
type PathMap = Sharded<PathMapShard>;

/// Database-local admission for actual backend calls on physical paths.
#[derive(Clone)]
struct PathCoordinator {
    paths: Arc<PathMap>,
}

impl PathCoordinator {
    fn new() -> Self {
        Self {
            paths: Arc::new(Sharded::new(|_| Mutex::new(HashMap::new()))),
        }
    }

    fn state(&self, path: &Arc<str>) -> Arc<PathState> {
        let mut paths = self.paths.for_key(path.as_bytes()).lock().unwrap();
        if let Some(state) = paths.get(path.as_ref()).and_then(Weak::upgrade) {
            return state;
        }
        let state = Arc::new(PathState {
            path: path.clone(),
            coordinator: Arc::downgrade(&self.paths),
            gate: Arc::new(Semaphore::new(1)),
            flight: Mutex::new(None),
            l2_fence: Arc::new(PathFence::default()),
        });
        paths.insert(path.clone(), Arc::downgrade(&state));
        state
    }

    async fn acquire(&self, path: &Arc<str>) -> PathPermit {
        let state = self.state(path);
        let permit = state
            .gate
            .clone()
            .acquire_owned()
            .await
            .expect("path semaphores are never closed");
        PathPermit {
            state,
            permit: Some(permit),
        }
    }

    async fn admit_read(&self, path: &Arc<str>, req: Requirement) -> ReadAdmission {
        let state = self.state(path);
        let flight = state.flight.lock().unwrap().clone();
        if let Some(flight) = flight.filter(|flight| satisfies(flight.invoked, req)) {
            return ReadAdmission::Join(flight);
        }
        let permit = state
            .gate
            .clone()
            .acquire_owned()
            .await
            .expect("path semaphores are never closed");
        ReadAdmission::Lead(PathPermit {
            state,
            permit: Some(permit),
        })
    }
}

struct PathState {
    path: Arc<str>,
    coordinator: Weak<PathMap>,
    gate: Arc<Semaphore>,
    flight: Mutex<Option<Arc<InFlight>>>,
    l2_fence: Arc<PathFence>,
}

impl Drop for PathState {
    fn drop(&mut self) {
        let Some(paths) = self.coordinator.upgrade() else {
            return;
        };
        let mut paths = paths.for_key(self.path.as_bytes()).lock().unwrap();
        if paths
            .get(self.path.as_ref())
            .is_some_and(|state| state.upgrade().is_none())
        {
            paths.remove(self.path.as_ref());
        }
    }
}

struct PathPermit {
    state: Arc<PathState>,
    permit: Option<OwnedSemaphorePermit>,
}

impl PathPermit {
    fn lead_read(self, invoked: SequencePoint) -> FlightLeader {
        let flight = Arc::new(InFlight {
            invoked,
            outcome: Mutex::new(None),
            notify: Notify::new(),
        });
        let previous = self.state.flight.lock().unwrap().replace(flight.clone());
        assert!(
            previous.is_none(),
            "path permit had an existing read flight"
        );
        FlightLeader {
            permit: Some(self),
            flight,
            armed: true,
        }
    }
}

impl Drop for PathPermit {
    fn drop(&mut self) {
        self.permit.take();
    }
}

enum ReadAdmission {
    Join(Arc<InFlight>),
    Lead(PathPermit),
}

#[derive(Clone)]
struct PresentSeed {
    value: Arc<dyn Any + Send + Sync>,
    size: usize,
    revision: Revision,
    evidence: Evidence,
}

enum ExpectedPredicate {
    Absent,
    Present(Revision),
}

/// Evidence cells proven current when a mutation predicate succeeds.
///
/// A retained observation and a matching cache entry can have distinct cells
/// after eviction and reload, so both must be preserved until reconciliation.
struct ExpectedEvidence {
    observation: Option<Evidence>,
    cached: Option<Evidence>,
}

impl ExpectedEvidence {
    fn new(observation: Option<Evidence>) -> Self {
        Self {
            observation,
            cached: None,
        }
    }

    fn capture_cached(&mut self, cached: Evidence) {
        let already_captured = self
            .observation
            .as_ref()
            .is_some_and(|observation| Arc::ptr_eq(&observation.0, &cached.0))
            || self
                .cached
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(&current.0, &cached.0));
        if already_captured {
            return;
        }
        debug_assert!(
            self.cached.is_none(),
            "a mutation captures at most one matching cache entry"
        );
        self.cached = Some(cached);
    }

    fn advance(&self, invoked: SequencePoint) {
        for evidence in self.observation.iter().chain(self.cached.iter()) {
            evidence.advance(invoked);
        }
    }
}

struct ExpectedState {
    predicate: ExpectedPredicate,
    evidence: ExpectedEvidence,
}

impl ExpectedState {
    fn absent(evidence: Option<Evidence>) -> Self {
        Self {
            predicate: ExpectedPredicate::Absent,
            evidence: ExpectedEvidence::new(evidence),
        }
    }

    fn present(revision: Revision, evidence: Evidence) -> Self {
        Self {
            predicate: ExpectedPredicate::Present(revision),
            evidence: ExpectedEvidence::new(Some(evidence)),
        }
    }

    fn capture_cached(&mut self, entry: Option<CacheEntry>) {
        let cached = match (&self.predicate, entry.map(|entry| entry.state)) {
            (ExpectedPredicate::Absent, Some(EntryState::Absent { evidence })) => Some(evidence),
            (
                ExpectedPredicate::Present(revision),
                Some(EntryState::Present {
                    revision: cached_revision,
                    evidence,
                    ..
                }),
            ) if *revision == cached_revision => Some(evidence),
            _ => None,
        };
        if let Some(cached) = cached {
            self.evidence.capture_cached(cached);
        }
    }

    fn advance(&self, invoked: SequencePoint) {
        self.evidence.advance(invoked);
    }

    fn matches(&self, entry: &CacheEntry) -> bool {
        match (&self.predicate, &entry.state) {
            (ExpectedPredicate::Absent, EntryState::Absent { .. }) => true,
            (
                ExpectedPredicate::Present(revision),
                EntryState::Present {
                    revision: current, ..
                },
            ) => revision == current,
            _ => false,
        }
    }
}

struct MutationGuard {
    cache: Arc<Cache<CacheEntry>>,
    persistent: Option<PersistentCache>,
    path: Arc<str>,
    expected: ExpectedState,
    permit: Option<PathPermit>,
    l2_fence: Option<FenceGuard>,
    armed: bool,
}

impl MutationGuard {
    fn new(
        cache: Arc<Cache<CacheEntry>>,
        persistent: Option<PersistentCache>,
        path: Arc<str>,
        mut expected: ExpectedState,
        permit: PathPermit,
    ) -> Self {
        expected.capture_cached(cache.get(path.as_ref()));
        Self {
            cache,
            persistent,
            path,
            expected,
            permit: Some(permit),
            l2_fence: None,
            armed: true,
        }
    }

    fn changed<R>(mut self, current_at: Option<SequencePoint>, apply: impl FnOnce() -> R) -> R {
        self.begin_path_change();
        let result = apply();
        if let Some(current_at) = current_at {
            self.expected.advance(current_at);
        }
        self.invalidate_l2();
        self.armed = false;
        self.permit.take();
        result
    }

    fn conflict(mut self) {
        self.begin_path_change();
        self.invalidate_expected();
        self.invalidate_l2();
        self.armed = false;
        self.permit.take();
    }

    fn uncertain(mut self) {
        self.begin_path_change();
        self.make_uncertain();
        self.invalidate_l2();
        self.armed = false;
        self.permit.take();
    }

    fn unchanged(mut self) {
        self.armed = false;
        self.permit.take();
    }

    fn begin_path_change(&mut self) {
        if self.l2_fence.is_some() {
            return;
        }
        let Some(persistent) = &self.persistent else {
            return;
        };
        let permit = self
            .permit
            .as_ref()
            .expect("an active mutation retains its path permit");
        self.l2_fence = persistent.begin_fence(permit.state.l2_fence.clone(), permit.state.clone());
    }

    fn invalidate_expected(&self) {
        self.cache.update(&self.path, |old| match old {
            Some(entry) if self.expected.matches(&entry) => None,
            other => other,
        });
    }

    fn make_uncertain(&self) {
        self.cache.delete(&self.path);
    }

    fn invalidate_l2(&mut self) {
        let (Some(persistent), Some(fence)) = (&self.persistent, self.l2_fence.take()) else {
            return;
        };
        persistent.invalidate(self.path.clone(), fence);
    }
}

impl Drop for MutationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.begin_path_change();
            self.make_uncertain();
            self.invalidate_l2();
        }
        self.permit.take();
    }
}

/// The decoded object cache over a [`Backend`] (ADR-036). Reads and mutations of
/// every physical object class go through this boundary; listing is an uncached
/// pass-through. Cloning is cheap (shared `Arc`s), so every typed store holds its
/// own handle onto the one shared cache.
#[derive(Clone)]
pub struct CachedStore {
    backend: Arc<dyn Backend>,
    cache: Arc<Cache<CacheEntry>>,
    timeline: Timeline,
    // Count of object bodies transferred from the backend (a fresh `read` or a
    // conditional read that returned a changed body). A caller samples this
    // before and after a logical read to tell whether the result reused cached
    // bodies (an unchanged count, possibly after a cheap conditional check)
    // or had to fetch a body — the signal behind the transaction-layer
    // cache-hit stat.
    body_reads: Arc<AtomicU64>,
    coordinator: PathCoordinator,
    metrics: Arc<CacheMetrics>,
    persistent: Option<PersistentCache>,
}

struct FlightLeader {
    permit: Option<PathPermit>,
    flight: Arc<InFlight>,
    armed: bool,
}

impl FlightLeader {
    fn complete(mut self, outcome: FlightOutcome) {
        self.flight.finish(outcome);
        self.remove();
        self.armed = false;
        self.permit.take();
    }

    fn remove(&self) {
        let Some(permit) = &self.permit else {
            return;
        };
        let mut flight = permit.state.flight.lock().unwrap();
        if flight
            .as_ref()
            .is_some_and(|candidate| Arc::ptr_eq(candidate, &self.flight))
        {
            flight.take();
        }
    }
}

impl Drop for FlightLeader {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.flight.finish(FlightOutcome::Cancelled);
        self.remove();
        self.permit.take();
    }
}

impl CachedStore {
    /// Creates a cached store over `backend`, sharing the single byte-bounded
    /// LRU sized by `max_size`, ordering evidence on `timeline`, and optionally
    /// using an already-open persistent encoded-body tier. When that tier is
    /// present, `timeline` must start after the sequence point returned with
    /// the same opened cache.
    pub fn new(
        backend: Arc<dyn Backend>,
        max_size: usize,
        timeline: Timeline,
        persistent: Option<PersistentCache>,
    ) -> Self {
        let metrics = persistent
            .as_ref()
            .map(PersistentCache::metrics)
            .unwrap_or_else(|| Arc::new(CacheMetrics::new()));
        CachedStore {
            backend,
            cache: Arc::new(Cache::new(max_size)),
            timeline,
            body_reads: Arc::new(AtomicU64::new(0)),
            coordinator: PathCoordinator::new(),
            metrics,
            persistent,
        }
    }

    /// The running count of object bodies this store has transferred from the
    /// backend. Sampled around a logical read to detect a body-free read (the
    /// count did not move): a hit reuses cached bodies, possibly after a cheap
    /// conditional check that returned "not modified".
    pub fn body_reads(&self) -> u64 {
        self.body_reads.load(Ordering::SeqCst)
    }

    /// Returns cache activity since the previous sample.
    pub fn cache_stats_and_reset(&self) -> CacheStats {
        self.metrics.snapshot_and_reset()
    }

    /// Drains and syncs the persistent cache, when configured.
    pub async fn shutdown(&self) {
        if let Some(persistent) = &self.persistent {
            persistent.shutdown().await;
        }
    }

    pub(crate) fn typed<C: Codec>(&self) -> TypedCachedStore<C> {
        TypedCachedStore {
            store: self.clone(),
            codec: PhantomData,
        }
    }

    /// Reads the object at `path`, serving a cached entry that satisfies `req`
    /// or checking through the backend otherwise. Returns an [`Observation`],
    /// whose `value()` is `None` for an object that does not exist. A new read
    /// never returns a positively known-obsolete value from uncertain state.
    async fn read<C: Codec>(
        &self,
        path: &str,
        req: Requirement,
    ) -> Result<Observation<C::Value>, StorageError> {
        let key: Arc<str> = Arc::from(path);
        if let Some(obs) = self.try_hit::<C>(&key, req)? {
            self.metrics.l1_hit();
            return Ok(obs);
        }
        self.metrics.l1_miss();
        let fetched = self.fetch::<C>(&key, req, None).await?;
        self.to_observation::<C>(&key, fetched)
    }

    /// Returns the cached observation for `path` without contacting the backend,
    /// or `None` when it is not cached. A committed/aborted object is immutable,
    /// so its cached copy is authoritative indefinitely; callers use this to
    /// serve terminal objects without a currentness-check round-trip.
    fn peek<C: Codec>(&self, path: &str) -> Result<Option<Observation<C::Value>>, StorageError> {
        let key: Arc<str> = Arc::from(path);
        self.try_hit::<C>(&key, Requirement::Any)
    }

    /// Checks whether a previously returned observation is current under `req`.
    /// Succeeds locally when the observation's watermark already satisfies the
    /// bound (even if that state is no longer the current cache entry); otherwise
    /// uses the observation's revision in a conditional backend read (or an
    /// ordinary read for an absence, which has no revision).
    async fn check_current<C: Codec>(
        &self,
        obs: &Observation<C::Value>,
        req: Requirement,
    ) -> Result<ObservationCheck<C::Value>, StorageError> {
        if satisfies(obs.evidence.get(), req) {
            return Ok(ObservationCheck::Current);
        }
        let path = Arc::clone(&obs.path);
        if let Some(current) = self.try_hit::<C>(&path, req)? {
            if current.revision == obs.revision {
                obs.evidence.advance(current.current_after());
                return Ok(ObservationCheck::Current);
            }
            return Ok(ObservationCheck::Changed(current));
        }
        let fetched = self.fetch::<C>(&path, req, Some(obs)).await?;
        let current = self.to_observation::<C>(&path, fetched)?;
        if same_observed_state(obs, &current) {
            let merged = obs.current_after().max(current.current_after());
            obs.evidence.advance(merged);
            current.evidence.advance(merged);
            Ok(ObservationCheck::Current)
        } else {
            Ok(ObservationCheck::Changed(current))
        }
    }

    /// Creates the object only if absent. On success publishes the value; on a
    /// conflict (it already exists) invalidates the cached absence and reports
    /// [`CasResult::Conflict`]; an in-doubt outcome makes path knowledge
    /// uncertain and surfaces `Unavailable`.
    async fn create<C: Codec>(
        &self,
        path: &str,
        expected_absence: Option<&Observation<C::Value>>,
        value: Arc<C::Value>,
    ) -> Result<CasResult<C::Value>, StorageError> {
        let bytes = C::encode(&value)?;
        let size = C::size(&value);
        let path: Arc<str> = Arc::from(path);
        let expected = ExpectedState::absent(expected_absence.map(|obs| obs.evidence.clone()));
        let permit = self.coordinator.acquire(&path).await;
        let guard = MutationGuard::new(
            self.cache.clone(),
            self.persistent.clone(),
            path.clone(),
            expected,
            permit,
        );
        let invoked = self.next_invocation();
        match self.backend.write_if_not_exists(&path, bytes).await {
            Ok(v) => {
                let obs = guard.changed(Some(invoked), || {
                    self.commit_write::<C>(&path, value, size, Revision(v), invoked)
                });
                Ok(CasResult::Committed(obs))
            }
            Err(BackendError::Precondition) => {
                guard.conflict();
                Ok(CasResult::Conflict)
            }
            Err(BackendError::Unavailable(msg)) => {
                guard.uncertain();
                Err(StorageError::Unavailable(msg))
            }
            Err(e) => {
                guard.unchanged();
                Err(e.into())
            }
        }
    }

    /// Compare-and-swaps the object from `expected` to `value`. On success the
    /// expected observation is proven to have remained current right up to the
    /// swap, so its watermark is advanced, and the new value is published; a
    /// conflict invalidates the exact starting revision if still cached, while
    /// an in-doubt outcome makes all path knowledge uncertain.
    async fn cas<C: Codec>(
        &self,
        value: Arc<C::Value>,
        expected: &Observation<C::Value>,
    ) -> Result<CasResult<C::Value>, StorageError> {
        let bytes = C::encode(&value)?;
        let size = C::size(&value);
        let path = expected.path.clone();
        let revision = expected
            .revision
            .clone()
            .ok_or_else(|| StorageError::other("CAS requires a present observation"))?;
        let expected_state = ExpectedState::present(revision.clone(), expected.evidence.clone());
        let permit = self.coordinator.acquire(&path).await;
        let guard = MutationGuard::new(
            self.cache.clone(),
            self.persistent.clone(),
            path.clone(),
            expected_state,
            permit,
        );
        let invoked = self.next_invocation();
        match self
            .backend
            .write_if(&path, bytes, revision.version())
            .await
        {
            Ok(v) => {
                let obs = guard.changed(Some(invoked), || {
                    self.commit_write::<C>(&path, value, size, Revision(v), invoked)
                });
                Ok(CasResult::Committed(obs))
            }
            Err(BackendError::NotFound) => {
                guard.changed(None, || self.install_absent(&path, invoked, None));
                Ok(CasResult::Conflict)
            }
            Err(BackendError::Precondition) => {
                guard.conflict();
                Ok(CasResult::Conflict)
            }
            Err(BackendError::Unavailable(msg)) => {
                guard.uncertain();
                Err(StorageError::Unavailable(msg))
            }
            Err(e) => {
                guard.unchanged();
                Err(e.into())
            }
        }
    }

    /// Deletes the exact present observation and returns the installed absence.
    /// A missing object is successful convergence; a conflict invalidates the
    /// expected revision if still cached, while an in-doubt outcome makes all
    /// path knowledge uncertain.
    async fn delete<C: Codec>(
        &self,
        expected: &Observation<C::Value>,
    ) -> Result<Observation<C::Value>, StorageError> {
        let revision = expected
            .revision
            .clone()
            .ok_or_else(|| StorageError::other("delete requires a present observation"))?;
        let path = expected.path.clone();
        let expected_state = ExpectedState::present(revision.clone(), expected.evidence.clone());
        let permit = self.coordinator.acquire(&path).await;
        let guard = MutationGuard::new(
            self.cache.clone(),
            self.persistent.clone(),
            path.clone(),
            expected_state,
            permit,
        );
        let invoked = self.next_invocation();
        match self.backend.delete_if(&path, revision.version()).await {
            Ok(()) => {
                let observation = guard.changed(Some(invoked), || {
                    let evidence = self.install_absent(&path, invoked, None);
                    Observation {
                        path: path.clone(),
                        value: None,
                        revision: None,
                        evidence,
                        cache_hit: false,
                    }
                });
                Ok(observation)
            }
            Err(BackendError::NotFound) => {
                let observation = guard.changed(None, || {
                    let evidence = self.install_absent(&path, invoked, None);
                    Observation {
                        path: path.clone(),
                        value: None,
                        revision: None,
                        evidence,
                        cache_hit: false,
                    }
                });
                Ok(observation)
            }
            Err(BackendError::Precondition) => {
                guard.conflict();
                Err(StorageError::Precondition)
            }
            Err(BackendError::Unavailable(msg)) => {
                guard.uncertain();
                Err(StorageError::Unavailable(msg))
            }
            Err(e) => {
                guard.unchanged();
                Err(e.into())
            }
        }
    }

    /// Lists one page of object paths under `prefix`, an uncached pass-through
    /// because a prefix has no object version.
    pub async fn list(
        &self,
        prefix: &str,
        cursor: Option<&backend::ListCursor>,
        limit: backend::ListLimit,
    ) -> Result<backend::ListPage, StorageError> {
        let _invoked = self.next_invocation();
        Ok(self.backend.list(prefix, cursor, limit).await?)
    }

    /// Allocates a unique invocation watermark, ordered before the backend
    /// call it precedes.
    fn next_invocation(&self) -> SequencePoint {
        self.timeline.now()
    }

    /// Serves a cached entry that already satisfies `req`, or `None` when the
    /// path is missing or the entry is too stale for the bound.
    fn try_hit<C: Codec>(
        &self,
        path: &Arc<str>,
        req: Requirement,
    ) -> Result<Option<Observation<C::Value>>, StorageError> {
        let Some(entry) = self.cache.get(path) else {
            return Ok(None);
        };
        match entry.state {
            EntryState::Present {
                value,
                revision,
                evidence,
                ..
            } => {
                if !satisfies(evidence.get(), req) {
                    return Ok(None);
                }
                let value = downcast::<C>(path, value)?;
                if let Some(persistent) = &self.persistent {
                    let state = self.coordinator.state(path);
                    persistent.record_present_hit(path, &state.l2_fence, state.clone());
                }
                Ok(Some(Observation {
                    path: path.clone(),
                    value: Some(value),
                    revision: Some(revision),
                    evidence,
                    cache_hit: true,
                }))
            }
            EntryState::Absent { evidence } => {
                if !satisfies(evidence.get(), req) {
                    return Ok(None);
                }
                Ok(Some(Observation {
                    path: path.clone(),
                    value: None,
                    revision: None,
                    evidence,
                    cache_hit: true,
                }))
            }
        }
    }

    /// Fetches from the backend, coalescing with an in-flight check of the same
    /// path when that check's invocation satisfies `req`.
    async fn fetch<C: Codec>(
        &self,
        path: &Arc<str>,
        req: Requirement,
        fallback: Option<&Observation<C::Value>>,
    ) -> Result<FetchResult, StorageError> {
        loop {
            match self.coordinator.admit_read(path, req).await {
                ReadAdmission::Join(flight) => match flight.wait().await {
                    FlightOutcome::Success(fetched) => return Ok(fetched),
                    FlightOutcome::Error(error) => return Err(error),
                    FlightOutcome::Cancelled => {}
                },
                ReadAdmission::Lead(permit) => {
                    if let Some(observed) = fallback
                        && satisfies(observed.current_after(), req)
                    {
                        return Ok(fetch_from_observation(observed, true));
                    }
                    if let Some(observed) = self.try_hit::<C>(path, req)? {
                        return Ok(fetch_from_observation(&observed, true));
                    }
                    let state = permit.state.clone();
                    let mut seed = self.present_seed::<C>(path, fallback)?;
                    if seed.is_none()
                        && let Some(persistent_seed) = self.load_l2::<C>(path, &state).await
                    {
                        if req == Requirement::Any {
                            return Ok(FetchResult {
                                value: Some(persistent_seed.value),
                                revision: Some(persistent_seed.revision),
                                evidence: persistent_seed.evidence,
                                cache_hit: true,
                            });
                        }
                        seed = Some(persistent_seed);
                    }
                    let invoked = self.next_invocation();
                    let leader = permit.lead_read(invoked);
                    let result = self.do_fetch::<C>(path, invoked, seed, &state).await;
                    leader.complete(match &result {
                        Ok(fetched) => FlightOutcome::Success(fetched.clone()),
                        Err(error) => FlightOutcome::Error(error.clone()),
                    });
                    return result;
                }
            }
        }
    }

    fn present_seed<C: Codec>(
        &self,
        path: &Arc<str>,
        fallback: Option<&Observation<C::Value>>,
    ) -> Result<Option<PresentSeed>, StorageError> {
        if let Some(CacheEntry {
            state:
                EntryState::Present {
                    value,
                    size,
                    revision,
                    evidence,
                },
        }) = self.cache.get(path)
        {
            downcast::<C>(path, value.clone())?;
            return Ok(Some(PresentSeed {
                value,
                size,
                revision,
                evidence,
            }));
        }
        let Some(observed) = fallback else {
            return Ok(None);
        };
        let (Some(value), Some(revision)) = (&observed.value, &observed.revision) else {
            return Ok(None);
        };
        let erased: Arc<dyn Any + Send + Sync> = value.clone();
        Ok(Some(PresentSeed {
            value: erased,
            size: C::size(value),
            revision: revision.clone(),
            evidence: observed.evidence.clone(),
        }))
    }

    async fn load_l2<C: Codec>(
        &self,
        path: &Arc<str>,
        state: &Arc<PathState>,
    ) -> Option<PresentSeed> {
        let persistent = self.persistent.clone()?;
        if state.l2_fence.is_active() || !persistent.is_enabled() {
            return None;
        }
        let encoded = match rt::timeout(
            PERSISTENT_CACHE_LOOKUP_TIMEOUT,
            persistent.lookup(path.clone()),
        )
        .await
        {
            Ok(encoded) => encoded?,
            Err(_) => {
                persistent.disable_slow_lookup();
                return None;
            }
        };
        self.decode_l2::<C>(path, state, persistent, encoded)
    }

    fn decode_l2<C: Codec>(
        &self,
        path: &Arc<str>,
        state: &Arc<PathState>,
        persistent: PersistentCache,
        encoded: EncodedBody,
    ) -> Option<PresentSeed> {
        let token = match String::from_utf8(encoded.revision) {
            Ok(token) => token,
            Err(error) => {
                tracing::warn!(path = %path, %error, "discarding invalid persistent-cache revision");
                persistent.reject_corrupt_candidate(
                    path.clone(),
                    state.l2_fence.clone(),
                    state.clone(),
                );
                return None;
            }
        };
        let decoded = match C::decode(path, &encoded.body) {
            Ok(decoded) => decoded,
            Err(error) => {
                tracing::warn!(path = %path, %error, "discarding undecodable persistent-cache body");
                persistent.reject_corrupt_candidate(
                    path.clone(),
                    state.l2_fence.clone(),
                    state.clone(),
                );
                return None;
            }
        };
        let size = C::size(&decoded);
        let value: Arc<dyn Any + Send + Sync> = Arc::new(decoded);
        let revision = Revision(backend::Version::new(token));
        let evidence = self.install_present(
            path,
            value.clone(),
            size,
            revision.clone(),
            Evidence::new(encoded.current_after),
        );
        persistent.record_present_hit(path, &state.l2_fence, state.clone());
        Some(PresentSeed {
            value,
            size,
            revision,
            evidence,
        })
    }

    /// Runs one backend read for a path: a version-conditional check when
    /// a present revision is known, else an ordinary read.
    async fn do_fetch<C: Codec>(
        &self,
        path: &str,
        invoked: SequencePoint,
        seed: Option<PresentSeed>,
        state: &Arc<PathState>,
    ) -> Result<FetchResult, StorageError> {
        match seed {
            Some(seed) => match self
                .backend
                .read_if_modified(path, seed.revision.version())
                .await
            {
                Ok(reply) => {
                    self.publish_present::<C>(path, reply.contents, reply.version, invoked, state)
                }
                Err(BackendError::Precondition) => Ok(self.publish_unchanged(path, seed, invoked)),
                Err(BackendError::NotFound) => Ok(self.publish_absent(path, invoked, None, state)),
                Err(e) => Err(e.into()),
            },
            None => match self.backend.read(path).await {
                Ok(reply) => {
                    self.publish_present::<C>(path, reply.contents, reply.version, invoked, state)
                }
                Err(BackendError::NotFound) => Ok(self.publish_absent(path, invoked, None, state)),
                Err(e) => Err(e.into()),
            },
        }
    }

    /// Decodes and publishes a freshly read body as a present entry.
    fn publish_present<C: Codec>(
        &self,
        path: &str,
        bytes: Vec<u8>,
        version: backend::Version,
        invoked: SequencePoint,
        state: &Arc<PathState>,
    ) -> Result<FetchResult, StorageError> {
        self.body_reads.fetch_add(1, Ordering::SeqCst);
        let decoded = match C::decode(path, &bytes) {
            Ok(decoded) => decoded,
            Err(error) => {
                let fence = self.begin_read_fence(state);
                self.cache.delete(path);
                self.invalidate_read_l2(Arc::from(path), fence);
                return Err(error);
            }
        };
        let size = C::size(&decoded);
        let value: Arc<dyn Any + Send + Sync> = Arc::new(decoded);
        let revision = Revision(version);
        let fence = self.begin_read_fence(state);
        let evidence = self.install_present(
            path,
            value.clone(),
            size,
            revision.clone(),
            Evidence::new(invoked),
        );
        if let (Some(persistent), Some(fence)) = (&self.persistent, fence) {
            persistent.replace(
                Arc::from(path),
                revision.serialize().as_bytes().to_vec(),
                bytes,
                invoked,
                fence,
            );
        }
        Ok(FetchResult {
            value: Some(value),
            revision: Some(revision),
            evidence,
            cache_hit: false,
        })
    }

    /// Handles a "not modified" response by reusing the body retained for the
    /// conditional request.
    fn publish_unchanged(
        &self,
        path: &str,
        seed: PresentSeed,
        invoked: SequencePoint,
    ) -> FetchResult {
        seed.evidence.advance(invoked);
        let evidence = self.install_present(
            path,
            seed.value.clone(),
            seed.size,
            seed.revision.clone(),
            seed.evidence,
        );
        FetchResult {
            value: Some(seed.value),
            revision: Some(seed.revision),
            evidence,
            cache_hit: true,
        }
    }

    /// Publishes a confirmed absence.
    fn publish_absent(
        &self,
        path: &str,
        invoked: SequencePoint,
        incoming: Option<Evidence>,
        state: &Arc<PathState>,
    ) -> FetchResult {
        let fence = self.begin_read_fence(state);
        let evidence = self.install_absent(path, invoked, incoming);
        self.invalidate_read_l2(Arc::from(path), fence);
        FetchResult {
            value: None,
            revision: None,
            evidence,
            cache_hit: false,
        }
    }

    fn begin_read_fence(&self, state: &Arc<PathState>) -> Option<FenceGuard> {
        self.persistent
            .as_ref()?
            .begin_fence(state.l2_fence.clone(), state.clone())
    }

    fn invalidate_read_l2(&self, path: Arc<str>, fence: Option<FenceGuard>) {
        if let (Some(persistent), Some(fence)) = (&self.persistent, fence) {
            persistent.invalidate(path, fence);
        }
    }

    /// Publishes a mutation's submitted value as the current present entry.
    fn commit_write<C: Codec>(
        &self,
        path: &str,
        value: Arc<C::Value>,
        size: usize,
        revision: Revision,
        invoked: SequencePoint,
    ) -> Observation<C::Value> {
        let erased: Arc<dyn Any + Send + Sync> = value.clone();
        let evidence =
            self.install_present(path, erased, size, revision.clone(), Evidence::new(invoked));
        Observation {
            path: Arc::from(path),
            value: Some(value),
            revision: Some(revision),
            evidence,
            cache_hit: false,
        }
    }

    /// Installs a present entry, merging evidence when the current entry has the
    /// same revision.
    fn install_present(
        &self,
        path: &str,
        value: Arc<dyn Any + Send + Sync>,
        size: usize,
        revision: Revision,
        incoming: Evidence,
    ) -> Evidence {
        self.cache.update_with_result(path, |old| match old {
            Some(CacheEntry {
                state:
                    EntryState::Present {
                        value: old_value,
                        size: old_size,
                        revision: old_revision,
                        evidence,
                    },
            }) if old_revision == revision => {
                evidence.advance(incoming.get());
                let installed = evidence.clone();
                (
                    Some(CacheEntry {
                        state: EntryState::Present {
                            value: old_value,
                            size: old_size,
                            revision: old_revision,
                            evidence,
                        },
                    }),
                    installed,
                )
            }
            _ => {
                let installed = incoming.clone();
                (
                    Some(CacheEntry {
                        state: EntryState::Present {
                            value,
                            size,
                            revision,
                            evidence: incoming,
                        },
                    }),
                    installed,
                )
            }
        })
    }

    /// Installs confirmed absence, merging evidence with an existing absence.
    fn install_absent(
        &self,
        path: &str,
        invoked: SequencePoint,
        incoming: Option<Evidence>,
    ) -> Evidence {
        let incoming = incoming.unwrap_or_else(|| Evidence::new(invoked));
        incoming.advance(invoked);
        self.cache.update_with_result(path, |old| match old {
            Some(CacheEntry {
                state: EntryState::Absent { evidence },
            }) => {
                evidence.advance(invoked);
                let installed = evidence.clone();
                (
                    Some(CacheEntry {
                        state: EntryState::Absent { evidence },
                    }),
                    installed,
                )
            }
            _ => {
                let installed = incoming.clone();
                (
                    Some(CacheEntry {
                        state: EntryState::Absent { evidence: incoming },
                    }),
                    installed,
                )
            }
        })
    }

    /// Converts a type-erased fetch result into a typed observation.
    fn to_observation<C: Codec>(
        &self,
        path: &Arc<str>,
        f: FetchResult,
    ) -> Result<Observation<C::Value>, StorageError> {
        let value = match f.value {
            Some(any) => Some(downcast::<C>(path, any)?),
            None => None,
        };
        Ok(Observation {
            path: path.clone(),
            value,
            revision: f.revision,
            evidence: f.evidence,
            cache_hit: f.cache_hit,
        })
    }
}

/// A typed facade over the shared decoded cache.
pub(crate) struct TypedCachedStore<C: Codec> {
    store: CachedStore,
    codec: PhantomData<fn() -> C>,
}

impl<C: Codec> Clone for TypedCachedStore<C> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            codec: PhantomData,
        }
    }
}

impl<C: Codec> TypedCachedStore<C> {
    fn check_path(path: &str) -> Result<(), StorageError> {
        if C::valid_path(path) {
            Ok(())
        } else {
            Err(StorageError::other(format!(
                "path {path:?} does not name a {} object",
                C::name()
            )))
        }
    }

    /// Returns a cached observation without backend I/O.
    pub(crate) fn peek(&self, path: &str) -> Result<Option<Observation<C::Value>>, StorageError> {
        Self::check_path(path)?;
        self.store.peek::<C>(path)
    }

    /// Reads the current state with the requested freshness requirement.
    pub(crate) async fn read(
        &self,
        path: &str,
        requirement: Requirement,
    ) -> Result<Observation<C::Value>, StorageError> {
        Self::check_path(path)?;
        self.store.read::<C>(path, requirement).await
    }

    /// Lists one page of object paths belonging to this typed store.
    pub(crate) async fn list(
        &self,
        prefix: &str,
        cursor: Option<&backend::ListCursor>,
        limit: backend::ListLimit,
    ) -> Result<backend::ListPage, StorageError> {
        let page = self.store.list(prefix, cursor, limit).await?;
        for path in &page.objects {
            Self::check_path(path)?;
        }
        Ok(page)
    }

    /// Checks whether an exact retained observation is current after `bound`.
    pub(crate) async fn check_current(
        &self,
        observed: &Observation<C::Value>,
        bound: SequencePoint,
    ) -> Result<ObservationCheck<C::Value>, StorageError> {
        Self::check_path(observed.path())?;
        self.store
            .check_current::<C>(observed, Requirement::AtLeast(bound))
            .await
    }

    /// Creates a decoded object if it is absent.
    pub(crate) async fn create(
        &self,
        path: &str,
        expected_absence: Option<&Observation<C::Value>>,
        value: Arc<C::Value>,
    ) -> Result<CasResult<C::Value>, StorageError> {
        Self::check_path(path)?;
        if let Some(expected) = expected_absence
            && (!expected.is_absent() || expected.path() != path)
        {
            return Err(StorageError::other(
                "create requires an absence observation for the same path",
            ));
        }
        self.store.create::<C>(path, expected_absence, value).await
    }

    /// Conditionally replaces the exact observed revision.
    pub(crate) async fn compare_and_swap(
        &self,
        expected: &Observation<C::Value>,
        value: Arc<C::Value>,
    ) -> Result<CasResult<C::Value>, StorageError> {
        Self::check_path(expected.path())?;
        if expected.revision().is_none() {
            return Err(StorageError::other("CAS requires a present observation"));
        }
        self.store.cas::<C>(value, expected).await
    }

    /// Deletes an exact present observation and caches the resulting absence.
    pub(crate) async fn delete(
        &self,
        expected: &Observation<C::Value>,
    ) -> Result<Observation<C::Value>, StorageError> {
        Self::check_path(expected.path())?;
        if expected.is_absent() {
            return Err(StorageError::other("delete requires a present observation"));
        }
        self.store.delete::<C>(expected).await
    }
}

/// Reports whether an entry confirmed current at `evidence` satisfies `req`.
fn satisfies(evidence: SequencePoint, req: Requirement) -> bool {
    match req {
        Requirement::Any => true,
        Requirement::AtLeast(t) => evidence >= t,
    }
}

fn fetch_from_observation<V: Send + Sync + 'static>(
    observed: &Observation<V>,
    cache_hit: bool,
) -> FetchResult {
    let value = observed
        .value
        .as_ref()
        .map(|value| value.clone() as Arc<dyn Any + Send + Sync>);
    FetchResult {
        value,
        revision: observed.revision.clone(),
        evidence: observed.evidence.clone(),
        cache_hit,
    }
}

fn same_observed_state<V>(left: &Observation<V>, right: &Observation<V>) -> bool {
    left.path == right.path && left.revision == right.revision
}

/// Downcasts a type-erased cached value to the codec's decoded type. A mismatch
/// means a path was used through the wrong typed store, which is an internal
/// error.
fn downcast<C: Codec>(
    path: &str,
    value: Arc<dyn Any + Send + Sync>,
) -> Result<Arc<C::Value>, StorageError> {
    value.downcast::<C::Value>().map_err(|_| {
        StorageError::other(format!(
            "cached object at {path} has a different decoded type than {}",
            C::name()
        ))
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{
        BackendOp, HookBackend, HookFuture, OpLog, RecordingBackend,
    };
    use glassdb_data::DatabaseUuid;
    use tempfile::TempDir;

    use super::*;
    use crate::timeline::TimeSource;

    // A trivial identity codec so the concurrency layer can be exercised in
    // isolation from any real object type.
    struct Bytes;

    impl Codec for Bytes {
        type Value = Vec<u8>;
        fn decode(_path: &str, bytes: &[u8]) -> Result<Vec<u8>, StorageError> {
            Ok(bytes.to_vec())
        }
        fn encode(value: &Vec<u8>) -> Result<Vec<u8>, StorageError> {
            Ok(value.clone())
        }
        fn size(value: &Vec<u8>) -> usize {
            value.len()
        }
        fn valid_path(_: &str) -> bool {
            true
        }
        fn name() -> &'static str {
            "bytes"
        }
    }

    // Models a provider such as S3 whose revision identifies contents rather
    // than a unique mutation. Recreating equivalent bytes deliberately reuses
    // the same token.
    #[derive(Default)]
    struct ContentVersionBackend {
        objects: Mutex<HashMap<String, Vec<u8>>>,
    }

    impl ContentVersionBackend {
        fn version(value: &[u8]) -> backend::Version {
            backend::Version::new(format!("{value:?}"))
        }
    }

    #[async_trait::async_trait]
    impl Backend for ContentVersionBackend {
        async fn read(&self, path: &str) -> Result<backend::ReadReply, BackendError> {
            let objects = self.objects.lock().unwrap();
            let contents = objects.get(path).cloned().ok_or(BackendError::NotFound)?;
            Ok(backend::ReadReply {
                version: Self::version(&contents),
                contents,
            })
        }

        async fn read_if_modified(
            &self,
            path: &str,
            expected: &backend::Version,
        ) -> Result<backend::ReadReply, BackendError> {
            let reply = self.read(path).await?;
            if &reply.version == expected {
                Err(BackendError::Precondition)
            } else {
                Ok(reply)
            }
        }

        async fn write_if(
            &self,
            path: &str,
            value: Vec<u8>,
            expected: &backend::Version,
        ) -> Result<backend::Version, BackendError> {
            let mut objects = self.objects.lock().unwrap();
            let current = objects.get_mut(path).ok_or(BackendError::NotFound)?;
            if &Self::version(current) != expected {
                return Err(BackendError::Precondition);
            }
            *current = value;
            Ok(Self::version(current))
        }

        async fn write_if_not_exists(
            &self,
            path: &str,
            value: Vec<u8>,
        ) -> Result<backend::Version, BackendError> {
            let mut objects = self.objects.lock().unwrap();
            if objects.contains_key(path) {
                return Err(BackendError::Precondition);
            }
            let version = Self::version(&value);
            objects.insert(path.to_string(), value);
            Ok(version)
        }

        async fn delete_if(
            &self,
            path: &str,
            expected: &backend::Version,
        ) -> Result<(), BackendError> {
            let mut objects = self.objects.lock().unwrap();
            let current = objects.get(path).ok_or(BackendError::NotFound)?;
            if &Self::version(current) != expected {
                return Err(BackendError::Precondition);
            }
            objects.remove(path);
            Ok(())
        }

        async fn list(
            &self,
            _prefix: &str,
            _cursor: Option<&backend::ListCursor>,
            _limit: backend::ListLimit,
        ) -> Result<backend::ListPage, BackendError> {
            Ok(backend::ListPage::default())
        }
    }

    // A second codec over a different decoded type, to prove a path used through
    // the wrong typed store is an internal error.
    struct Ints;

    impl Codec for Ints {
        type Value = u64;
        fn decode(_path: &str, bytes: &[u8]) -> Result<u64, StorageError> {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| StorageError::other("bad int"))?;
            Ok(u64::from_le_bytes(arr))
        }
        fn encode(value: &u64) -> Result<Vec<u8>, StorageError> {
            Ok(value.to_le_bytes().to_vec())
        }
        fn size(_: &u64) -> usize {
            8
        }
        fn valid_path(_: &str) -> bool {
            true
        }
        fn name() -> &'static str {
            "integers"
        }
    }

    fn v(bytes: &[u8]) -> Arc<Vec<u8>> {
        Arc::new(bytes.to_vec())
    }

    fn ready(result: Result<(), BackendError>) -> HookFuture {
        Box::pin(async move { result })
    }

    // A store over a recording memory backend, plus the op log for counting
    // backend traffic.
    fn bytes_store(backend: Arc<dyn Backend>) -> TypedCachedStore<Bytes> {
        CachedStore::new(backend, 1 << 20, Timeline::new(), None).typed()
    }

    fn store_rec() -> (TypedCachedStore<Bytes>, OpLog) {
        let rec = Arc::new(RecordingBackend::new(Arc::new(MemoryBackend::new())));
        let log = rec.log();
        let backend: Arc<dyn Backend> = rec;
        (bytes_store(backend), log)
    }

    fn count(log: &OpLog, op: &str) -> usize {
        log.lock().unwrap().iter().filter(|r| r.op == op).count()
    }

    fn clear(log: &OpLog) {
        log.lock().unwrap().clear();
    }

    fn cache_uuid() -> DatabaseUuid {
        let mut bytes = [7; 16];
        bytes[6] = 0x47;
        bytes[8] = 0x87;
        DatabaseUuid::from_bytes(bytes).unwrap()
    }

    async fn persistent_store(
        directory: &TempDir,
        backend: Arc<dyn Backend>,
    ) -> (CachedStore, Timeline) {
        let opened = PersistentCache::open_with_test_geometry(
            PersistentCacheConfig {
                directory: directory.path().to_path_buf(),
                capacity_bytes: 2 * 1024 * 1024,
            },
            "db",
            cache_uuid(),
        )
        .await;
        let timeline = Timeline::starting_after(opened.last_sequence_point);
        let store = CachedStore::new(backend, 1 << 20, timeline.clone(), Some(opened.cache));
        (store, timeline)
    }

    #[cfg(sim)]
    #[test]
    fn persistent_cache_fails_open_in_deterministic_simulation() {
        let directory = TempDir::new().unwrap();
        let cache_file = directory.path().join("l2.cache");
        rt::block_on_with(rt::TapeScheduler::new(Vec::new()), 0, async move {
            let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
            let (store, _) = persistent_store(&directory, backend).await;
            assert!(
                store
                    .persistent
                    .as_ref()
                    .is_some_and(|persistent| !persistent.is_enabled())
            );
        });

        assert!(!cache_file.exists());
    }

    async fn create_value(
        store: &TypedCachedStore<Bytes>,
        path: &str,
        value: Arc<Vec<u8>>,
    ) -> Observation<Vec<u8>> {
        store
            .create(path, None, value)
            .await
            .unwrap()
            .into_observation()
            .unwrap()
    }

    async fn replace_value(
        store: &TypedCachedStore<Bytes>,
        expected: &Observation<Vec<u8>>,
        value: Arc<Vec<u8>>,
    ) -> Observation<Vec<u8>> {
        store
            .compare_and_swap(expected, value)
            .await
            .unwrap()
            .into_observation()
            .unwrap()
    }

    // Model invariant: an `Any` hit is served from cache with no backend op,
    // while `AtLeast(now())` on an older entry checks and advances (never
    // regresses) its watermark.
    #[tokio::test]
    async fn any_hit_is_local_and_at_least_checks_current_and_advances() {
        let (s, log) = store_rec();
        create_value(&s, "p", v(b"a")).await;

        let o1 = s.read("p", Requirement::Any).await.unwrap();
        assert_eq!(o1.value().unwrap().as_slice(), b"a");
        assert_eq!(count(&log, "read"), 0);
        assert_eq!(count(&log, "read_if_modified"), 0);

        let t = s.store.timeline.now();
        let o2 = s.read("p", Requirement::AtLeast(t)).await.unwrap();
        assert_eq!(count(&log, "read_if_modified"), 1, "stale entry is checked");
        assert!(o2.current_after() >= t, "watermark advanced to the bound");
        assert!(o2.current_after() >= o1.current_after(), "never regresses");
    }

    // Model invariant: `AtLeast(T)` accepts an entry whose watermark already
    // reaches `T` with no backend op.
    #[tokio::test]
    async fn at_least_served_locally_when_watermark_sufficient() {
        let (s, log) = store_rec();
        create_value(&s, "p", v(b"a")).await;
        let o = s
            .read("p", Requirement::AtLeast(s.store.timeline.now()))
            .await
            .unwrap();
        let w = o.current_after();
        clear(&log);

        let o2 = s.read("p", Requirement::AtLeast(w)).await.unwrap();
        assert_eq!(count(&log, "read"), 0);
        assert_eq!(count(&log, "read_if_modified"), 0);
        assert!(o2.current_after() >= w);
    }

    // Model invariant: `Any` never returns an entry a conflict invalidated. A
    // stale CAS makes the exact starting entry uncertain, so the next `Any`
    // re-reads the backend and observes the winner.
    #[tokio::test]
    async fn any_rereads_after_conflict_invalidates_starting_entry() {
        let mem = Arc::new(MemoryBackend::new());
        let rec = Arc::new(RecordingBackend::new(mem));
        let log = rec.log();
        let backend: Arc<dyn Backend> = rec;
        let s1 = bytes_store(backend.clone());
        let s2 = bytes_store(backend);

        let obs = s1
            .create("p", None, v(b"a"))
            .await
            .unwrap()
            .into_observation()
            .unwrap();
        // A peer overwrites the object; s1's cache is unaware.
        replace_value(&s2, &obs, v(b"b")).await;

        let r = s1.compare_and_swap(&obs, v(b"c")).await.unwrap();
        assert!(!r.committed(), "the stale CAS conflicts");
        clear(&log);

        let got = s1.read("p", Requirement::Any).await.unwrap();
        assert_eq!(
            got.value().unwrap().as_slice(),
            b"b",
            "Any must not return the obsolete value"
        );
        assert_eq!(
            count(&log, "read"),
            1,
            "the invalidated entry forces a read"
        );
    }

    // Regression: two observations of one committed revision are the same state
    // even when they hold distinct evidence cells. A cache eviction and reload
    // (modeled here by two independent caches over one backend) mints a fresh
    // cell for the unchanged version; `same_state` must still hold, otherwise a
    // lock CAS fails to certify a read taken before the reload.
    #[tokio::test]
    async fn same_state_holds_across_independent_evidence_for_one_revision() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let a = bytes_store(backend.clone());
        let b = bytes_store(backend);

        create_value(&a, "p", v(b"x")).await;
        let obs_a = a.read("p", Requirement::Any).await.unwrap();
        let obs_b = b.read("p", Requirement::Any).await.unwrap();

        assert_eq!(
            obs_a.revision(),
            obs_b.revision(),
            "both observed the same committed version"
        );
        assert!(
            obs_a.same_state(&obs_b),
            "same revision is the same state despite distinct evidence cells"
        );
    }

    // Model invariant: an observation stays usable after its current entry is
    // invalidated. Its established watermark satisfies an older bound locally,
    // while a new read cannot rediscover the obsolete value and a stricter bound
    // checks through the backend.
    #[tokio::test]
    async fn observation_outlives_invalidation() {
        let mem = Arc::new(MemoryBackend::new());
        let rec = Arc::new(RecordingBackend::new(mem));
        let log = rec.log();
        let backend: Arc<dyn Backend> = rec;
        let s1 = bytes_store(backend.clone());
        let s2 = bytes_store(backend);

        let obs = s1
            .create("p", None, v(b"a"))
            .await
            .unwrap()
            .into_observation()
            .unwrap();
        let w = obs.current_after();

        replace_value(&s2, &obs, v(b"b")).await;
        s1.compare_and_swap(&obs, v(b"c")).await.unwrap(); // conflict -> uncertain

        assert_eq!(obs.value().unwrap().as_slice(), b"a", "still inspectable");

        clear(&log);
        assert!(matches!(
            s1.check_current(&obs, w).await.unwrap(),
            ObservationCheck::Current
        ));
        assert_eq!(count(&log, "read"), 0, "older bound needs no backend op");
        assert_eq!(count(&log, "read_if_modified"), 0);

        // A stricter bound checks again and observes the winner.
        let t = s1.store.timeline.now();
        match s1.check_current(&obs, t).await.unwrap() {
            ObservationCheck::Changed(cur) => assert_eq!(cur.value().unwrap().as_slice(), b"b"),
            ObservationCheck::Current => panic!("a stricter bound must observe the changed state"),
        }

        // A brand-new read cannot rediscover the obsolete value.
        let got = s1.read("p", Requirement::Any).await.unwrap();
        assert_eq!(got.value().unwrap().as_slice(), b"b");
    }

    #[tokio::test]
    async fn newer_current_evidence_confirms_an_observation_without_io() {
        let memory = Arc::new(MemoryBackend::new());
        let recording = Arc::new(RecordingBackend::new(memory));
        let log = recording.log();
        let backend: Arc<dyn Backend> = recording;
        let local = bytes_store(backend.clone());
        let peer = bytes_store(backend);

        let observed = create_value(&local, "p", v(b"a")).await;
        replace_value(&peer, &observed, v(b"b")).await;
        let bound = local.store.timeline.now();
        let current = local.read("p", Requirement::AtLeast(bound)).await.unwrap();
        assert_eq!(current.value().unwrap().as_slice(), b"b");

        clear(&log);
        match local.check_current(&observed, bound).await.unwrap() {
            ObservationCheck::Changed(changed) => {
                assert_eq!(changed.value().unwrap().as_slice(), b"b");
            }
            ObservationCheck::Current => panic!("the retained revision changed"),
        }
        assert!(log.lock().unwrap().is_empty());
    }

    // Model invariant: a successful CAS advances both the expected observation's
    // shared evidence and installs the new value from its start time.
    #[tokio::test]
    async fn successful_cas_advances_expected_and_installs() {
        let (s, _log) = store_rec();
        let obs = s
            .create("p", None, v(b"a"))
            .await
            .unwrap()
            .into_observation()
            .unwrap();

        let before = s.store.timeline.now();
        let nb = s
            .compare_and_swap(&obs, v(b"b"))
            .await
            .unwrap()
            .into_observation()
            .unwrap();
        assert!(
            obs.current_after() >= before,
            "expected observation advanced past the CAS start"
        );
        assert!(nb.current_after() >= before);
        assert_eq!(nb.value().unwrap().as_slice(), b"b");

        let got = s.read("p", Requirement::Any).await.unwrap();
        assert_eq!(got.value().unwrap().as_slice(), b"b");
    }

    // A reload can create independent evidence for the same revision. A
    // successful CAS proves both retained observations current at invocation.
    #[tokio::test]
    async fn successful_cas_advances_observation_and_reloaded_evidence() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let store = bytes_store(backend);
        let expected = create_value(&store, "p", v(b"a")).await;

        store.store.cache.delete("p");
        let reloaded = store.read("p", Requirement::Any).await.unwrap();
        assert!(expected.same_state(&reloaded));
        assert!(!Arc::ptr_eq(&expected.evidence.0, &reloaded.evidence.0));

        let before = store.store.timeline.now();
        replace_value(&store, &expected, v(b"b")).await;

        assert!(expected.current_after() >= before);
        assert!(reloaded.current_after() >= before);
    }

    // Model invariant: a CAS conflict neither advances the expected observation
    // nor installs the proposed value.
    #[tokio::test]
    async fn cas_conflict_advances_nothing() {
        let mem = Arc::new(MemoryBackend::new());
        let backend: Arc<dyn Backend> = Arc::new(mem);
        let s1 = bytes_store(backend.clone());
        let s2 = bytes_store(backend);

        let obs = s1
            .create("p", None, v(b"a"))
            .await
            .unwrap()
            .into_observation()
            .unwrap();
        replace_value(&s2, &obs, v(b"b")).await;

        let before = s1.store.timeline.now();
        let r = s1.compare_and_swap(&obs, v(b"c")).await.unwrap();
        assert!(!r.committed());
        assert!(
            obs.current_after() < before,
            "conflict must not advance the observation"
        );
        let got = s1.read("p", Requirement::Any).await.unwrap();
        assert_eq!(
            got.value().unwrap().as_slice(),
            b"b",
            "proposed value not installed"
        );
    }

    // Model invariant: an in-doubt CAS makes all discoverable path knowledge
    // uncertain and does not advance the observation's watermark. The
    // underlying write may still have landed, which a later `Any` read discovers.
    #[tokio::test]
    async fn cas_in_doubt_makes_path_uncertain() {
        let hook = HookBackend::new(Arc::new(MemoryBackend::new()));
        let backend: Arc<dyn Backend> = hook.clone();
        let s = bytes_store(backend);

        let obs = s
            .create("p", None, v(b"a"))
            .await
            .unwrap()
            .into_observation()
            .unwrap();
        let before = s.store.timeline.now();

        // The write lands but its acknowledgement is lost.
        hook.set_after(|op, outcome| {
            ready(
                if matches!(op, BackendOp::WriteIf { .. }) && outcome.is_success() {
                    Err(BackendError::Unavailable("lost ack".into()))
                } else {
                    Ok(())
                },
            )
        });
        let err = s.compare_and_swap(&obs, v(b"b")).await.unwrap_err();
        assert!(matches!(err, StorageError::Unavailable(_)));
        hook.clear_after();

        assert!(
            obs.current_after() < before,
            "an in-doubt outcome must not advance the observation"
        );
        // The path became uncertain, so Any re-reads and finds the write
        // that actually landed.
        let got = s.read("p", Requirement::Any).await.unwrap();
        assert_eq!(got.value().unwrap().as_slice(), b"b");
    }

    // Model invariant: a failed (never-landed) mutation publishes nothing.
    #[tokio::test]
    async fn failed_mutation_is_not_published() {
        let hook = HookBackend::new(Arc::new(MemoryBackend::new()));
        let backend: Arc<dyn Backend> = hook.clone();
        let s = bytes_store(backend);

        // Cache a confirmed absence first.
        assert!(!s.read("p", Requirement::Any).await.unwrap().exists());

        hook.set_before(|op| {
            ready(if matches!(op, BackendOp::WriteIfNotExists { .. }) {
                Err(BackendError::other("boom"))
            } else {
                Ok(())
            })
        });
        assert!(matches!(
            s.create("p", None, v(b"x")).await,
            Err(StorageError::Other { .. })
        ));
        hook.clear_before();

        let got = s.read("p", Requirement::Any).await.unwrap();
        assert!(!got.exists(), "a failed create must not publish its value");
    }

    // Conditional mutations are state-based: an old absence predicate is safe
    // to execute after the path existed and became absent again.
    #[tokio::test]
    async fn create_executes_after_absence_aba() {
        let rec = Arc::new(RecordingBackend::new(Arc::new(MemoryBackend::new())));
        let log = rec.log();
        let backend: Arc<dyn Backend> = rec;
        let local = bytes_store(backend.clone());
        let peer = bytes_store(backend);

        let absent = local.read("p", Requirement::Any).await.unwrap();
        let present = create_value(&peer, "p", v(b"temporary")).await;
        peer.delete(&present).await.unwrap();
        clear(&log);

        let created = local
            .create("p", Some(&absent), v(b"final"))
            .await
            .unwrap()
            .into_observation()
            .unwrap();
        assert_eq!(created.value().unwrap().as_slice(), b"final");
        assert_eq!(count(&log, "write_if_not_exists"), 1);
    }

    // Providers may derive revisions from content. Returning to the same bytes
    // therefore restores the original CAS predicate, which remains valid.
    #[tokio::test]
    async fn cas_executes_after_revision_aba() {
        let content = Arc::new(ContentVersionBackend::default());
        let backend: Arc<dyn Backend> = content.clone();
        let store = bytes_store(backend);
        let expected = create_value(&store, "p", v(b"a")).await;
        content
            .delete_if("p", expected.revision().unwrap().version())
            .await
            .unwrap();
        content
            .write_if_not_exists("p", b"a".to_vec())
            .await
            .unwrap();

        let replacement = replace_value(&store, &expected, v(b"b")).await;
        assert_eq!(replacement.value().unwrap().as_slice(), b"b");
    }

    // Model invariant: repeated conditional checks advance but never
    // regress the watermark.
    #[tokio::test]
    async fn unchanged_conditional_reads_only_advance() {
        let (s, log) = store_rec();
        create_value(&s, "p", v(b"a")).await;

        let t1 = s.store.timeline.now();
        let w1 = s
            .read("p", Requirement::AtLeast(t1))
            .await
            .unwrap()
            .current_after();
        assert!(w1 >= t1);
        let t2 = s.store.timeline.now();
        let w2 = s
            .read("p", Requirement::AtLeast(t2))
            .await
            .unwrap()
            .current_after();
        assert!(w2 >= w1, "watermark never regresses");
        assert_eq!(count(&log, "read_if_modified"), 2);
    }

    // Model invariant: negative caching. An absence is cached and re-served
    // without a backend read; a create replaces it; a delete installs a fresh
    // confirmed absence.
    #[tokio::test]
    async fn absence_is_cached_and_transitions() {
        let (s, log) = store_rec();
        assert!(!s.read("m", Requirement::Any).await.unwrap().exists());
        assert_eq!(count(&log, "read"), 1);
        clear(&log);
        assert!(!s.read("m", Requirement::Any).await.unwrap().exists());
        assert_eq!(count(&log, "read"), 0, "absence is cached");

        let present = create_value(&s, "m", v(b"x")).await;
        let got = s.read("m", Requirement::Any).await.unwrap();
        assert_eq!(got.value().unwrap().as_slice(), b"x");

        let deleted = s.delete(&present).await.unwrap();
        assert!(deleted.is_absent());
        clear(&log);
        assert!(!s.read("m", Requirement::Any).await.unwrap().exists());
        assert_eq!(count(&log, "read"), 0, "delete leaves cached absence");
    }

    // Model invariant: a successful conditional delete advances the exact
    // expected state's evidence and publishes absence from the operation's
    // invocation.
    #[tokio::test]
    async fn successful_delete_advances_expected_and_installs_absence() {
        let (s, log) = store_rec();
        let expected = create_value(&s, "p", v(b"a")).await;
        let before = s.store.timeline.now();

        let absent = s.delete(&expected).await.unwrap();

        assert!(absent.is_absent());
        assert!(absent.current_after() >= before);
        assert!(expected.current_after() >= before);
        assert_eq!(count(&log, "delete_if"), 1);
        clear(&log);
        assert!(s.read("p", Requirement::Any).await.unwrap().is_absent());
        assert!(log.lock().unwrap().is_empty());
    }

    // Model invariant: NotFound is successful convergence on absence, but it
    // does not claim the retained present observation survived until this
    // delete's invocation.
    #[tokio::test]
    async fn delete_not_found_converges_without_advancing_expected() {
        let memory = Arc::new(MemoryBackend::new());
        let recording = Arc::new(RecordingBackend::new(memory));
        let log = recording.log();
        let backend: Arc<dyn Backend> = recording;
        let local = bytes_store(backend.clone());
        let peer = bytes_store(backend);

        let expected = create_value(&local, "p", v(b"a")).await;
        let peer_observation = peer.read("p", Requirement::Any).await.unwrap();
        peer.delete(&peer_observation).await.unwrap();
        let before = local.store.timeline.now();
        clear(&log);

        let absent = local.delete(&expected).await.unwrap();

        assert!(absent.is_absent());
        assert!(absent.current_after() >= before);
        assert!(expected.current_after() < before);
        assert_eq!(count(&log, "delete_if"), 1);
        clear(&log);
        assert!(local.read("p", Requirement::Any).await.unwrap().is_absent());
        assert!(log.lock().unwrap().is_empty());
    }

    // A writer outside this database is not ordered by the local path lane. A
    // sufficiently fresh read still discovers a recreation that happened while
    // a local NotFound response was delayed.
    #[tokio::test]
    async fn fresh_read_discovers_external_recreation_after_delayed_not_found() {
        let content = Arc::new(ContentVersionBackend::default());
        let inner: Arc<dyn Backend> = content.clone();
        let hook = HookBackend::new(inner);
        let backend: Arc<dyn Backend> = hook.clone();
        let store = bytes_store(backend);
        let expected = create_value(&store, "p", v(b"a")).await;
        content
            .delete_if("p", expected.revision().unwrap().version())
            .await
            .unwrap();

        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        hook.set_after({
            let entered = entered.clone();
            let release = release.clone();
            move |operation, outcome| {
                let gate = matches!(operation, BackendOp::DeleteIf { path, .. } if *path == "p")
                    && !outcome.is_success();
                let entered = entered.clone();
                let release = release.clone();
                Box::pin(async move {
                    if gate {
                        entered.notify_one();
                        release.notified().await;
                    }
                    Ok(())
                })
            }
        });

        let deleting = tokio::spawn({
            let store = store.clone();
            async move { store.delete(&expected).await }
        });
        entered.notified().await;
        content
            .write_if_not_exists("p", b"a".to_vec())
            .await
            .unwrap();
        release.notify_one();

        assert!(deleting.await.unwrap().unwrap().is_absent());
        let bound = store.store.timeline.now();
        let current = store.read("p", Requirement::AtLeast(bound)).await.unwrap();
        assert!(current.exists());
        assert_eq!(current.value().unwrap().as_slice(), b"a");
    }

    // Model invariant: a stale conditional delete invalidates only its exact
    // cached starting revision, forcing the next unbounded read to discover the
    // winner without deleting it.
    #[tokio::test]
    async fn delete_conflict_invalidates_expected_and_preserves_winner() {
        let memory = Arc::new(MemoryBackend::new());
        let recording = Arc::new(RecordingBackend::new(memory));
        let log = recording.log();
        let backend: Arc<dyn Backend> = recording;
        let local = bytes_store(backend.clone());
        let peer = bytes_store(backend);

        let expected = create_value(&local, "p", v(b"a")).await;
        let peer_observation = peer.read("p", Requirement::Any).await.unwrap();
        replace_value(&peer, &peer_observation, v(b"b")).await;
        let before = local.store.timeline.now();

        assert!(matches!(
            local.delete(&expected).await,
            Err(StorageError::Precondition)
        ));
        assert!(expected.current_after() < before);
        clear(&log);

        let current = local.read("p", Requirement::Any).await.unwrap();
        assert_eq!(current.value().unwrap().as_slice(), b"b");
        assert_eq!(count(&log, "read"), 1);
    }

    // Model invariant: a lost delete acknowledgement makes the path uncertain.
    // The expected cache entry is invalidated and its evidence is not advanced,
    // even when the underlying deletion actually landed.
    #[tokio::test]
    async fn delete_in_doubt_invalidates_expected_without_advancing_it() {
        let memory = Arc::new(MemoryBackend::new());
        let recording = Arc::new(RecordingBackend::new(memory));
        let log = recording.log();
        let inner: Arc<dyn Backend> = recording;
        let hook = HookBackend::new(inner);
        let backend: Arc<dyn Backend> = hook.clone();
        let store = bytes_store(backend);
        let expected = create_value(&store, "p", v(b"a")).await;
        let before = store.store.timeline.now();

        hook.set_after(|operation, outcome| {
            ready(
                if matches!(operation, BackendOp::DeleteIf { .. }) && outcome.is_success() {
                    Err(BackendError::Unavailable("lost ack".into()))
                } else {
                    Ok(())
                },
            )
        });
        assert!(matches!(
            store.delete(&expected).await,
            Err(StorageError::Unavailable(_))
        ));
        hook.clear_after();

        assert!(expected.current_after() < before);
        clear(&log);
        assert!(store.read("p", Requirement::Any).await.unwrap().is_absent());
        assert_eq!(count(&log, "read"), 1);
    }

    // Model invariant: a definitive error raised before dispatch leaves the
    // retained present entry usable because the backend knows deletion did not
    // apply.
    #[tokio::test]
    async fn definitive_delete_error_keeps_expected_cached() {
        let memory = Arc::new(MemoryBackend::new());
        let recording = Arc::new(RecordingBackend::new(memory));
        let log = recording.log();
        let inner: Arc<dyn Backend> = recording;
        let hook = HookBackend::new(inner);
        let backend: Arc<dyn Backend> = hook.clone();
        let store = bytes_store(backend);
        let expected = create_value(&store, "p", v(b"a")).await;
        let before = store.store.timeline.now();

        hook.set_before(|operation| {
            ready(if matches!(operation, BackendOp::DeleteIf { .. }) {
                Err(BackendError::other("rejected before dispatch"))
            } else {
                Ok(())
            })
        });
        assert!(matches!(
            store.delete(&expected).await,
            Err(StorageError::Other { .. })
        ));
        hook.clear_before();

        assert!(expected.current_after() < before);
        clear(&log);
        let current = store.read("p", Requirement::Any).await.unwrap();
        assert_eq!(current.value().unwrap().as_slice(), b"a");
        assert!(current.cache_hit());
        assert!(log.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn delete_rejects_an_absence_observation_without_backend_io() {
        let (store, log) = store_rec();
        let absent = store.read("p", Requirement::Any).await.unwrap();
        clear(&log);

        assert!(matches!(
            store.delete(&absent).await,
            Err(StorageError::Other { .. })
        ));
        assert!(log.lock().unwrap().is_empty());
    }

    // A path used through a mismatched typed store is an internal error.
    #[tokio::test]
    async fn wrong_decoded_type_is_internal_error() {
        let store = CachedStore::new(
            Arc::new(MemoryBackend::new()),
            1 << 20,
            Timeline::new(),
            None,
        );
        let bytes = store.typed::<Bytes>();
        let ints = store.typed::<Ints>();
        create_value(&bytes, "p", v(b"abcd")).await;
        assert!(matches!(
            ints.read("p", Requirement::Any).await,
            Err(StorageError::Other { .. })
        ));
    }

    // Regression: a read invoked after a same-path create is admitted cannot
    // race past it and publish a false absence.
    #[tokio::test]
    async fn read_invoked_after_create_cannot_publish_false_absence() {
        let recording = Arc::new(RecordingBackend::new(Arc::new(MemoryBackend::new())));
        let log = recording.log();
        let inner: Arc<dyn Backend> = recording;
        let hook = HookBackend::new(inner);
        let backend: Arc<dyn Backend> = hook.clone();
        let s = bytes_store(backend);

        let entered = Arc::new(AtomicBool::new(false));
        let released = Arc::new(AtomicBool::new(false));
        hook.set_before({
            let entered = entered.clone();
            let released = released.clone();
            move |op| {
                if !matches!(op, BackendOp::WriteIfNotExists { path, .. } if *path == "p") {
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

        let creating = tokio::spawn({
            let s = s.clone();
            async move { create_value(&s, "p", v(b"a")).await }
        });
        while !entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        let reading = tokio::spawn({
            let s = s.clone();
            async move { s.read("p", Requirement::Any).await }
        });
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert!(!reading.is_finished(), "the read waits behind the create");
        released.store(true, Ordering::SeqCst);

        creating.await.unwrap();
        let observed = reading.await.unwrap().unwrap();
        assert_eq!(observed.value().unwrap().as_slice(), b"a");
        assert_eq!(count(&log, "write_if_not_exists"), 1);
        assert_eq!(count(&log, "read"), 0, "the queued read reused the write");
    }

    // Gated race: two concurrent `Any` reads of a cold path share one in-flight
    // backend read.
    #[tokio::test]
    async fn concurrent_any_reads_coalesce() {
        let mem = Arc::new(MemoryBackend::new());
        let rec = Arc::new(RecordingBackend::new(mem));
        let log = rec.log();
        let inner: Arc<dyn Backend> = rec;
        let hook = HookBackend::new(inner);
        let backend: Arc<dyn Backend> = hook.clone();
        let seeder = bytes_store(backend.clone());
        create_value(&seeder, "p", v(b"a")).await;
        let s = bytes_store(backend);
        clear(&log);

        let entered = Arc::new(AtomicUsize::new(0));
        let released = Arc::new(AtomicBool::new(false));
        hook.set_before({
            let entered = entered.clone();
            let released = released.clone();
            move |op| {
                if !matches!(op, BackendOp::Read { .. }) {
                    return ready(Ok(()));
                }
                entered.fetch_add(1, Ordering::SeqCst);
                let released = released.clone();
                Box::pin(async move {
                    while !released.load(Ordering::SeqCst) {
                        tokio::task::yield_now().await;
                    }
                    Ok(())
                })
            }
        });

        let r1 = tokio::spawn({
            let s = s.clone();
            async move { s.read("p", Requirement::Any).await }
        });
        while entered.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        let r2 = tokio::spawn({
            let s = s.clone();
            async move { s.read("p", Requirement::Any).await }
        });
        // Give r2 a chance to (not) start its own read; it should join r1.
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        released.store(true, Ordering::SeqCst);
        assert_eq!(r1.await.unwrap().unwrap().value().unwrap().as_slice(), b"a");
        assert_eq!(r2.await.unwrap().unwrap().value().unwrap().as_slice(), b"a");
        assert_eq!(count(&log, "read"), 1, "the two reads coalesced");
    }

    // A busy path must not serialize backend work for an unrelated path.
    #[tokio::test]
    async fn different_paths_run_in_parallel() {
        let rec = Arc::new(RecordingBackend::new(Arc::new(MemoryBackend::new())));
        let log = rec.log();
        let inner: Arc<dyn Backend> = rec;
        let hook = HookBackend::new(inner);
        let backend: Arc<dyn Backend> = hook.clone();
        let seeder = bytes_store(backend.clone());
        create_value(&seeder, "p", v(b"a")).await;
        create_value(&seeder, "q", v(b"b")).await;
        let store = bytes_store(backend);
        clear(&log);

        let entered = Arc::new(AtomicBool::new(false));
        let released = Arc::new(AtomicBool::new(false));
        hook.set_before({
            let entered = entered.clone();
            let released = released.clone();
            move |operation| {
                if !matches!(operation, BackendOp::Read { path } if *path == "p") {
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

        let p = tokio::spawn({
            let store = store.clone();
            async move { store.read("p", Requirement::Any).await }
        });
        while !entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        let q = tokio::spawn({
            let store = store.clone();
            async move { store.read("q", Requirement::Any).await }
        });
        for _ in 0..64 {
            if q.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(q.is_finished(), "the q read was blocked by the p lane");
        assert_eq!(q.await.unwrap().unwrap().value().unwrap().as_slice(), b"b");
        released.store(true, Ordering::SeqCst);
        assert_eq!(p.await.unwrap().unwrap().value().unwrap().as_slice(), b"a");
        assert_eq!(count(&log, "read"), 2);
    }

    // `Any` deliberately bypasses the lane on a hit, so it may observe the old
    // cached state while a mutation is awaiting its acknowledgement.
    #[tokio::test]
    async fn any_cache_hit_during_mutation_returns_previous_state() {
        let hook = HookBackend::new(Arc::new(MemoryBackend::new()));
        let backend: Arc<dyn Backend> = hook.clone();
        let store = bytes_store(backend);
        let expected = create_value(&store, "p", v(b"a")).await;
        let entered = Arc::new(AtomicBool::new(false));
        let released = Arc::new(AtomicBool::new(false));
        hook.set_after({
            let entered = entered.clone();
            let released = released.clone();
            move |operation, outcome| {
                if !matches!(operation, BackendOp::WriteIf { path, .. } if *path == "p")
                    || !outcome.is_success()
                {
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

        let replacing = tokio::spawn({
            let store = store.clone();
            async move { replace_value(&store, &expected, v(b"b")).await }
        });
        while !entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        let old = store.read("p", Requirement::Any).await.unwrap();
        assert_eq!(old.value().unwrap().as_slice(), b"a");
        assert!(old.cache_hit());

        released.store(true, Ordering::SeqCst);
        let new = replacing.await.unwrap();
        assert_eq!(new.value().unwrap().as_slice(), b"b");
        assert_eq!(
            store
                .read("p", Requirement::Any)
                .await
                .unwrap()
                .value()
                .unwrap()
                .as_slice(),
            b"b"
        );
    }

    // Dropping a mutation after dispatch removes discoverable knowledge before
    // releasing the lane, even when the remote write already applied.
    #[tokio::test]
    async fn cancelling_invoked_mutation_makes_cache_uncertain() {
        let rec = Arc::new(RecordingBackend::new(Arc::new(MemoryBackend::new())));
        let log = rec.log();
        let inner: Arc<dyn Backend> = rec;
        let hook = HookBackend::new(inner);
        let backend: Arc<dyn Backend> = hook.clone();
        let store = bytes_store(backend);
        let expected = create_value(&store, "p", v(b"a")).await;
        clear(&log);

        let entered = Arc::new(Notify::new());
        hook.set_after({
            let entered = entered.clone();
            move |operation, outcome| {
                let gate = matches!(operation, BackendOp::WriteIf { path, .. } if *path == "p")
                    && outcome.is_success();
                let entered = entered.clone();
                Box::pin(async move {
                    if gate {
                        entered.notify_one();
                        std::future::pending::<()>().await;
                    }
                    Ok(())
                })
            }
        });
        let replacing = tokio::spawn({
            let store = store.clone();
            async move { replace_value(&store, &expected, v(b"b")).await }
        });
        entered.notified().await;
        replacing.abort();
        let _ = replacing.await;
        hook.clear_after();

        let current = store.read("p", Requirement::Any).await.unwrap();
        assert_eq!(current.value().unwrap().as_slice(), b"b");
        assert!(!current.cache_hit());
        assert_eq!(count(&log, "write_if"), 1);
        assert_eq!(count(&log, "read"), 1);
    }

    // Cancellation while queued has not invoked the mutation and therefore
    // leaves the cache knowledge established by the lane owner intact.
    #[tokio::test]
    async fn cancelling_queued_mutation_preserves_cache() {
        let rec = Arc::new(RecordingBackend::new(Arc::new(MemoryBackend::new())));
        let log = rec.log();
        let inner: Arc<dyn Backend> = rec;
        let hook = HookBackend::new(inner);
        let backend: Arc<dyn Backend> = hook.clone();
        let seeder = bytes_store(backend.clone());
        let expected = create_value(&seeder, "p", v(b"a")).await;
        let store = bytes_store(backend);
        clear(&log);

        let entered = Arc::new(AtomicBool::new(false));
        let released = Arc::new(AtomicBool::new(false));
        hook.set_after({
            let entered = entered.clone();
            let released = released.clone();
            move |operation, _| {
                if !matches!(operation, BackendOp::Read { path } if *path == "p") {
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
        let reading = tokio::spawn({
            let store = store.clone();
            async move { store.read("p", Requirement::Any).await }
        });
        while !entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        let queued = tokio::spawn({
            let store = store.clone();
            async move { replace_value(&store, &expected, v(b"b")).await }
        });
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        queued.abort();
        let _ = queued.await;
        released.store(true, Ordering::SeqCst);
        reading.await.unwrap().unwrap();
        hook.clear_after();

        assert_eq!(count(&log, "write_if"), 0);
        clear(&log);
        let current = store.read("p", Requirement::Any).await.unwrap();
        assert_eq!(current.value().unwrap().as_slice(), b"a");
        assert!(current.cache_hit());
        assert!(log.lock().unwrap().is_empty());
    }

    // Gated race: a stricter waiter whose bound is not satisfied by the in-flight
    // check's start does not coalesce; it issues its own check.
    #[tokio::test]
    async fn stricter_waiter_does_not_coalesce() {
        let mem = Arc::new(MemoryBackend::new());
        let rec = Arc::new(RecordingBackend::new(mem));
        let log = rec.log();
        let inner: Arc<dyn Backend> = rec;
        let hook = HookBackend::new(inner);
        let backend: Arc<dyn Backend> = hook.clone();
        let s = bytes_store(backend);
        // Seed a present-but-stale entry.
        create_value(&s, "p", v(b"a")).await;
        clear(&log);

        let entered = Arc::new(AtomicUsize::new(0));
        let released = Arc::new(AtomicBool::new(false));
        hook.set_before({
            let entered = entered.clone();
            let released = released.clone();
            move |op| {
                if !matches!(op, BackendOp::ReadIfModified { .. }) {
                    return ready(Ok(()));
                }
                entered.fetch_add(1, Ordering::SeqCst);
                let released = released.clone();
                Box::pin(async move {
                    while !released.load(Ordering::SeqCst) {
                        tokio::task::yield_now().await;
                    }
                    Ok(())
                })
            }
        });

        // Reader A checks at AtLeast(now()); its op start is tA.
        let a = tokio::spawn({
            let s = s.clone();
            let t = s.store.timeline.now();
            async move { s.read("p", Requirement::AtLeast(t)).await }
        });
        while entered.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        // A stricter bound than A's start: it cannot join A's in-flight op.
        let strict = s.store.timeline.now();
        let b = tokio::spawn({
            let s = s.clone();
            async move { s.read("p", Requirement::AtLeast(strict)).await }
        });
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            entered.load(Ordering::SeqCst),
            1,
            "the stricter check waits for the same-path lane"
        );
        released.store(true, Ordering::SeqCst);
        a.await.unwrap().unwrap();
        b.await.unwrap().unwrap();
        assert_eq!(
            count(&log, "read_if_modified"),
            2,
            "the stricter waiter issued its own check"
        );
    }

    #[derive(Default)]
    struct TestClock {
        elapsed: Mutex<Duration>,
    }

    impl TestClock {
        fn set(&self, duration: Duration) {
            *self.elapsed.lock().unwrap() = duration;
        }
    }

    impl TimeSource for TestClock {
        fn elapsed(&self) -> Duration {
            *self.elapsed.lock().unwrap()
        }
    }

    #[test]
    fn duration_requirement_uses_the_timeline() {
        let clock = Arc::new(TestClock::default());
        clock.set(Duration::from_secs(10));
        let timeline = Timeline::with_source(clock);
        let _store: TypedCachedStore<Bytes> = CachedStore::new(
            Arc::new(MemoryBackend::new()),
            1 << 20,
            timeline.clone(),
            None,
        )
        .typed();

        assert_eq!(
            Requirement::within(&timeline, Duration::from_secs(3)),
            Requirement::AtLeast(SequencePoint::from_raw(7_000_000_000))
        );
        assert_eq!(
            Requirement::within(&timeline, Duration::MAX),
            Requirement::Any
        );
    }

    #[tokio::test]
    async fn response_time_does_not_overstate_freshness() {
        let inner = Arc::new(MemoryBackend::new());
        inner
            .write_if_not_exists("p", b"one".to_vec())
            .await
            .unwrap();
        let hooked = HookBackend::new(inner);
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        hooked.set_after({
            let entered = entered.clone();
            let release = release.clone();
            move |operation, _| {
                let gate = matches!(operation, BackendOp::Read { path } if *path == "p");
                let entered = entered.clone();
                let release = release.clone();
                Box::pin(async move {
                    if gate {
                        entered.notify_one();
                        release.notified().await;
                    }
                    Ok(())
                })
            }
        });
        let clock = Arc::new(TestClock::default());
        clock.set(Duration::from_secs(1));
        let timeline = Timeline::with_source(clock.clone());
        let store: TypedCachedStore<Bytes> =
            CachedStore::new(hooked, 1 << 20, timeline.clone(), None).typed();
        let read_store = store.clone();
        let read =
            tokio::spawn(async move { read_store.read("p", Requirement::Any).await.unwrap() });
        entered.notified().await;

        clock.set(Duration::from_secs(100));
        let later = timeline.now();
        release.notify_one();
        let observed = read.await.unwrap();

        assert!(observed.current_after() < later);
    }

    #[tokio::test]
    async fn cancelling_a_read_leader_releases_its_waiters() {
        let inner = Arc::new(MemoryBackend::new());
        inner
            .write_if_not_exists("p", b"one".to_vec())
            .await
            .unwrap();
        let hooked = HookBackend::new(inner);
        let entered = Arc::new(Notify::new());
        let first = Arc::new(AtomicBool::new(true));
        hooked.set_before({
            let entered = entered.clone();
            let first = first.clone();
            move |operation| {
                let gate = matches!(operation, BackendOp::Read { path } if *path == "p")
                    && first.swap(false, Ordering::SeqCst);
                let entered = entered.clone();
                Box::pin(async move {
                    if gate {
                        entered.notify_one();
                        std::future::pending::<()>().await;
                    }
                    Ok(())
                })
            }
        });
        let store: TypedCachedStore<Bytes> =
            CachedStore::new(hooked, 1 << 20, Timeline::new(), None).typed();
        let leader = tokio::spawn({
            let store = store.clone();
            async move { store.read("p", Requirement::Any).await }
        });
        entered.notified().await;
        let waiter = tokio::spawn(async move { store.read("p", Requirement::Any).await });
        tokio::task::yield_now().await;
        leader.abort();

        let observed = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter remained stuck behind a cancelled leader")
            .unwrap()
            .unwrap();
        assert_eq!(observed.value().unwrap().as_slice(), b"one");
    }

    #[tokio::test]
    async fn persistent_evidence_precedes_the_reopened_session() {
        let directory = TempDir::new().unwrap();
        let backend = Arc::new(MemoryBackend::new());
        backend
            .write_if_not_exists("p", b"one".to_vec())
            .await
            .unwrap();
        let recorded = Arc::new(RecordingBackend::new(backend));
        let log = recorded.log();
        let erased: Arc<dyn Backend> = recorded;

        let (first, _) = persistent_store(&directory, erased.clone()).await;
        let first_typed: TypedCachedStore<Bytes> = first.typed();
        let loaded = first_typed.read("p", Requirement::Any).await.unwrap();
        let persisted = loaded.current_after();
        drop(first_typed);
        first.shutdown().await;
        drop(first);

        let (reopened, timeline) = persistent_store(&directory, erased).await;
        let bound = timeline.now();
        assert!(bound > persisted);

        let typed: TypedCachedStore<Bytes> = reopened.typed();
        let restored = typed.read("p", Requirement::Any).await.unwrap();
        assert_eq!(restored.value().unwrap().as_slice(), b"one");
        assert_eq!(restored.current_after(), persisted);
        assert!(restored.cache_hit());
        assert_eq!(reopened.body_reads(), 0);

        clear(&log);
        let verified = typed.read("p", Requirement::AtLeast(bound)).await.unwrap();
        assert!(verified.current_after() >= bound);
        assert_eq!(reopened.body_reads(), 0);
        assert_eq!(
            count(&log, "read_if_modified"),
            1,
            "persisted evidence should seed a conditional backend read"
        );
        let stats = reopened.cache_stats_and_reset();
        assert_eq!(stats.l2_hits, 1, "cache stats: {stats:?}");

        drop(typed);
        reopened.shutdown().await;
    }

    #[tokio::test]
    async fn mutation_invalidates_a_persisted_body_without_admitting_the_write() {
        let directory = TempDir::new().unwrap();
        let backend = Arc::new(MemoryBackend::new());
        backend
            .write_if_not_exists("p", b"one".to_vec())
            .await
            .unwrap();
        let erased: Arc<dyn Backend> = backend.clone();

        let (first, _) = persistent_store(&directory, erased.clone()).await;
        let first_typed: TypedCachedStore<Bytes> = first.typed();
        let old = first_typed.read("p", Requirement::Any).await.unwrap();
        let changed = first_typed.compare_and_swap(&old, v(b"two")).await.unwrap();
        assert!(changed.committed());
        drop(first_typed);
        first.shutdown().await;
        drop(first);

        let (reopened, _) = persistent_store(&directory, erased).await;
        let typed: TypedCachedStore<Bytes> = reopened.typed();
        let loaded = typed.read("p", Requirement::Any).await.unwrap();
        assert_eq!(loaded.value().unwrap().as_slice(), b"two");
        assert!(loaded.current_after() > SequencePoint::default());
        assert_eq!(reopened.body_reads(), 1);
        let stats = reopened.cache_stats_and_reset();
        assert_eq!(stats.l2_hits, 0, "cache stats: {stats:?}");
        assert_eq!(stats.l2_misses, 1, "cache stats: {stats:?}");

        drop(typed);
        reopened.shutdown().await;
    }
}
