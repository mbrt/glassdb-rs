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

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::cache_stats::{CacheMetrics, CacheStats};
use crate::disk_cache::PersistentCache;
use crate::error::StorageError;
use crate::timeline::{SequencePoint, Timeline};
use glassdb_backend::{self as backend, Backend, BackendError};
use glassdb_data::{ObjectPath, PathError};

mod knowledge;
mod mutation;
mod path_lane;
mod persistent_bridge;

use knowledge::{FetchResult, Knowledge, PresentSeed};
use mutation::{MutationOutcome, MutationRound};
use path_lane::{FlightOutcome, PathCoordinator, PathState, ReadAdmission};
use persistent_bridge::PersistentBridge;

/// A physical object address carried in both its semantic and backend forms.
///
/// Storage constructs keys from typed paths. Raw strings enter only through
/// backend listings, where they are parsed once before reaching typed stores.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ObjectKey {
    object: ObjectPath,
    encoded: Arc<str>,
}

impl ObjectKey {
    /// Parses one object name returned by a backend listing.
    pub(crate) fn parse(encoded: impl Into<Arc<str>>) -> Result<Self, PathError> {
        let encoded = encoded.into();
        let object = ObjectPath::try_from(encoded.as_ref())?;
        Ok(Self { object, encoded })
    }

    /// Returns the classified physical object path.
    pub(crate) fn object_path(&self) -> &ObjectPath {
        &self.object
    }

    /// Returns the exact canonical name used at backend boundaries.
    pub(crate) fn as_str(&self) -> &str {
        &self.encoded
    }

    fn encoded(&self) -> &Arc<str> {
        &self.encoded
    }
}

impl From<ObjectPath> for ObjectKey {
    fn from(object: ObjectPath) -> Self {
        let encoded = Arc::from(object.to_string());
        Self { object, encoded }
    }
}

impl From<&ObjectPath> for ObjectKey {
    fn from(object: &ObjectPath) -> Self {
        Self::from(object.clone())
    }
}

impl From<&ObjectKey> for ObjectKey {
    fn from(key: &ObjectKey) -> Self {
        key.clone()
    }
}

// Cached-store protocol tests intentionally exercise arbitrary opaque keys.
// Production callers have no raw-string conversion and must use ObjectPath.
#[cfg(test)]
impl From<&str> for ObjectKey {
    fn from(encoded: &str) -> Self {
        Self {
            object: ObjectPath::DatabaseMetadata {
                db_root: glassdb_data::DbRoot::try_from("test").unwrap(),
            },
            encoded: Arc::from(encoded),
        }
    }
}

#[cfg(test)]
impl From<&String> for ObjectKey {
    fn from(encoded: &String) -> Self {
        Self::from(encoded.as_str())
    }
}

/// Encoding, decoding, and decoded-size accounting for one physical object type.
///
/// Each typed store supplies its own codec; the cache holds the decoded value
/// so an object is decoded once per changed revision rather than once per hit.
pub(crate) trait Codec: Send + Sync + 'static {
    /// The decoded, immutable value cached for this object type.
    type Value: Send + Sync + 'static;

    /// Decodes an object body into its cached value.
    fn decode(path: &ObjectPath, bytes: &[u8]) -> Result<Self::Value, StorageError>;

    /// Encodes a cached value back into its object body (the CAS unit).
    fn encode(path: &ObjectPath, value: &Self::Value) -> Result<Vec<u8>, StorageError>;

    /// Estimates the decoded value's in-memory size in bytes, governing
    /// eviction.
    fn size(value: &Self::Value) -> usize;

    /// Reports whether `path` names an object handled by this codec.
    fn accepts(path: &ObjectPath) -> bool;

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
/// reference to shared currentness evidence. It remains inspectable after the
/// state is evicted or invalidated as the current cache entry.
#[derive(Debug, Clone)]
pub struct Observation<V> {
    key: ObjectKey,
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

    /// The parsed physical object path this observation refers to.
    pub fn path(&self) -> &ObjectPath {
        self.key.object_path()
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
            (Some(mine), Some(theirs)) => self.key == other.key && mine == theirs,
            _ => false,
        }
    }
}

/// The decoded object cache over a [`Backend`] (ADR-036). Reads and mutations of
/// every physical object class go through this boundary; listing is an uncached
/// pass-through. Cloning is cheap (shared `Arc`s), so every typed store holds its
/// own handle onto the one shared cache.
#[derive(Clone)]
pub struct CachedStore {
    backend: Arc<dyn Backend>,
    knowledge: Knowledge,
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
    persistent: PersistentBridge,
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
        let persistent = PersistentBridge::new(persistent);
        let metrics = persistent
            .metrics()
            .unwrap_or_else(|| Arc::new(CacheMetrics::new()));
        CachedStore {
            backend,
            knowledge: Knowledge::new(max_size),
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
        self.persistent.shutdown().await;
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
        key: ObjectKey,
        req: Requirement,
    ) -> Result<Observation<C::Value>, StorageError> {
        if let Some(obs) = self.try_hit::<C>(&key, req)? {
            self.metrics.l1_hit();
            return Ok(obs);
        }
        self.metrics.l1_miss();
        let fetched = self.fetch::<C>(&key, req, None).await?;
        self.knowledge.to_observation::<C>(key, fetched)
    }

    /// Returns the cached observation for `path` without contacting the backend,
    /// or `None` when it is not cached. A committed/aborted object is immutable,
    /// so its cached copy is authoritative indefinitely; callers use this to
    /// serve terminal objects without a currentness-check round-trip.
    fn peek<C: Codec>(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<Observation<C::Value>>, StorageError> {
        self.try_hit::<C>(key, Requirement::Any)
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
        let key = obs.key.clone();
        if let Some(current) = self.try_hit::<C>(&key, req)? {
            if current.revision == obs.revision {
                obs.evidence.advance(current.current_after());
                return Ok(ObservationCheck::Current);
            }
            return Ok(ObservationCheck::Changed(current));
        }
        let fetched = self.fetch::<C>(&key, req, Some(obs)).await?;
        let current = self.knowledge.to_observation::<C>(key, fetched)?;
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
        key: ObjectKey,
        expected_absence: Option<&Observation<C::Value>>,
        value: Arc<C::Value>,
    ) -> Result<CasResult<C::Value>, StorageError> {
        let bytes = C::encode(key.object_path(), &value)?;
        let size = C::size(&value);
        let path = key.encoded().clone();
        let expected = self.knowledge.expected_absent(expected_absence);
        let permit = self.coordinator.acquire(&path).await;
        let round = MutationRound::new(
            self.knowledge.clone(),
            self.persistent.clone(),
            path.clone(),
            expected,
            permit,
        );
        let invoked = self.next_invocation();
        let outcome = match self.backend.write_if_not_exists(&path, bytes).await {
            Ok(version) => MutationOutcome::success(version, Some(invoked)),
            Err(BackendError::Precondition) => MutationOutcome::conflict(),
            Err(error) => MutationOutcome::failed(error),
        };
        let committed = round.finish(outcome, |version| {
            self.knowledge
                .install_mutation::<C>(key, value, size, Revision(version), invoked)
        })?;
        Ok(committed.map_or(CasResult::Conflict, CasResult::Committed))
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
        let bytes = C::encode(expected.path(), &value)?;
        let size = C::size(&value);
        let key = expected.key.clone();
        let path = key.encoded().clone();
        let revision = expected
            .revision
            .clone()
            .ok_or_else(|| StorageError::other("CAS requires a present observation"))?;
        let expected_state = self.knowledge.expected_present(revision.clone(), expected);
        let permit = self.coordinator.acquire(&path).await;
        let round = MutationRound::new(
            self.knowledge.clone(),
            self.persistent.clone(),
            path.clone(),
            expected_state,
            permit,
        );
        let invoked = self.next_invocation();
        let outcome = match self
            .backend
            .write_if(&path, bytes, revision.version())
            .await
        {
            Ok(version) => MutationOutcome::success(Some(version), Some(invoked)),
            Err(BackendError::NotFound) => MutationOutcome::success(None, None),
            Err(BackendError::Precondition) => MutationOutcome::conflict(),
            Err(error) => MutationOutcome::failed(error),
        };
        let completed = round.finish(outcome, |version| match version {
            Some(version) => CasResult::Committed(self.knowledge.install_mutation::<C>(
                key,
                value,
                size,
                Revision(version),
                invoked,
            )),
            None => {
                self.knowledge
                    .install_absent_observation::<C::Value>(key, invoked);
                CasResult::Conflict
            }
        })?;
        Ok(completed.unwrap_or(CasResult::Conflict))
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
        let key = expected.key.clone();
        let path = key.encoded().clone();
        let expected_state = self.knowledge.expected_present(revision.clone(), expected);
        let permit = self.coordinator.acquire(&path).await;
        let round = MutationRound::new(
            self.knowledge.clone(),
            self.persistent.clone(),
            path.clone(),
            expected_state,
            permit,
        );
        let invoked = self.next_invocation();
        let outcome = match self.backend.delete_if(&path, revision.version()).await {
            Ok(()) => MutationOutcome::success((), Some(invoked)),
            Err(BackendError::NotFound) => MutationOutcome::success((), None),
            Err(BackendError::Precondition) => MutationOutcome::conflict(),
            Err(error) => MutationOutcome::failed(error),
        };
        round
            .finish(outcome, |()| {
                self.knowledge
                    .install_absent_observation::<C::Value>(key, invoked)
            })?
            .ok_or(StorageError::Precondition)
    }

    /// Lists one page of object paths.
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
        key: &ObjectKey,
        req: Requirement,
    ) -> Result<Option<Observation<C::Value>>, StorageError> {
        let observed = self.knowledge.peek::<C>(key, req)?;
        if observed.as_ref().is_some_and(Observation::exists) && self.persistent.is_configured() {
            let state = self.coordinator.state(key.encoded());
            self.persistent.record_present_hit(key.encoded(), &state);
        }
        Ok(observed)
    }

    /// Fetches from the backend, coalescing with an in-flight check of the same
    /// path when that check's invocation satisfies `req`.
    async fn fetch<C: Codec>(
        &self,
        key: &ObjectKey,
        req: Requirement,
        fallback: Option<&Observation<C::Value>>,
    ) -> Result<FetchResult, StorageError> {
        loop {
            match self.coordinator.admit_read(key.encoded(), req).await {
                ReadAdmission::Join(flight) => match flight.wait().await {
                    FlightOutcome::Success(fetched) => return Ok(fetched),
                    FlightOutcome::Error(error) => return Err(error),
                    FlightOutcome::Cancelled => {}
                },
                ReadAdmission::Lead(permit) => {
                    if let Some(observed) = fallback
                        && satisfies(observed.current_after(), req)
                    {
                        return Ok(self.knowledge.result_from_observation(observed, true));
                    }
                    if let Some(observed) = self.try_hit::<C>(key, req)? {
                        return Ok(self.knowledge.result_from_observation(&observed, true));
                    }
                    let state = permit.state().clone();
                    let mut seed = self.knowledge.present_seed::<C>(key, fallback)?;
                    if seed.is_none()
                        && let Some(persistent_seed) = self
                            .persistent
                            .load::<C>(&self.knowledge, key, &state)
                            .await
                    {
                        if req == Requirement::Any {
                            return Ok(self.knowledge.result_from_seed(persistent_seed, true));
                        }
                        seed = Some(persistent_seed);
                    }
                    let invoked = self.next_invocation();
                    let leader = permit.lead_read(invoked);
                    let result = self.do_fetch::<C>(key, invoked, seed, &state).await;
                    leader.complete(match &result {
                        Ok(fetched) => FlightOutcome::Success(fetched.clone()),
                        Err(error) => FlightOutcome::Error(error.clone()),
                    });
                    return result;
                }
            }
        }
    }

    /// Runs one backend read for a path: a version-conditional check when
    /// a present revision is known, else an ordinary read.
    async fn do_fetch<C: Codec>(
        &self,
        key: &ObjectKey,
        invoked: SequencePoint,
        seed: Option<PresentSeed>,
        state: &Arc<PathState>,
    ) -> Result<FetchResult, StorageError> {
        match seed {
            Some(seed) => match self
                .backend
                .read_if_modified(key.as_str(), seed.revision().version())
                .await
            {
                Ok(reply) => {
                    self.publish_present::<C>(key, reply.contents, reply.version, invoked, state)
                }
                Err(BackendError::Precondition) => {
                    Ok(self.publish_unchanged(key.as_str(), seed, invoked))
                }
                Err(BackendError::NotFound) => {
                    Ok(self.publish_absent(key.as_str(), invoked, state))
                }
                Err(e) => Err(e.into()),
            },
            None => match self.backend.read(key.as_str()).await {
                Ok(reply) => {
                    self.publish_present::<C>(key, reply.contents, reply.version, invoked, state)
                }
                Err(BackendError::NotFound) => {
                    Ok(self.publish_absent(key.as_str(), invoked, state))
                }
                Err(e) => Err(e.into()),
            },
        }
    }

    /// Decodes and publishes a freshly read body as a present entry.
    fn publish_present<C: Codec>(
        &self,
        key: &ObjectKey,
        bytes: Vec<u8>,
        version: backend::Version,
        invoked: SequencePoint,
        state: &Arc<PathState>,
    ) -> Result<FetchResult, StorageError> {
        self.body_reads.fetch_add(1, Ordering::SeqCst);
        let decoded = match C::decode(key.object_path(), &bytes) {
            Ok(decoded) => decoded,
            Err(error) => {
                let change = self.persistent.begin_change(state);
                self.knowledge.invalidate(key.as_str());
                change.invalidate(key.encoded().clone());
                return Err(error);
            }
        };
        let size = C::size(&decoded);
        let value = Arc::new(decoded);
        let revision = Revision(version);
        let change = self.persistent.begin_change(state);
        let fetched = self.knowledge.install_fetched::<C>(
            key.as_str(),
            value,
            size,
            revision.clone(),
            invoked,
        );
        change.replace(key.encoded().clone(), &revision, bytes, invoked);
        Ok(fetched)
    }

    /// Handles a "not modified" response by reusing the body retained for the
    /// conditional request.
    fn publish_unchanged(
        &self,
        path: &str,
        seed: PresentSeed,
        invoked: SequencePoint,
    ) -> FetchResult {
        self.knowledge.confirm_unchanged(path, seed, invoked)
    }

    /// Publishes a confirmed absence.
    fn publish_absent(
        &self,
        path: &str,
        invoked: SequencePoint,
        state: &Arc<PathState>,
    ) -> FetchResult {
        let change = self.persistent.begin_change(state);
        let fetched = self.knowledge.install_absent_result(path, invoked);
        change.invalidate(Arc::from(path));
        fetched
    }
}

/// A typed facade over the shared decoded cache.
pub(crate) struct TypedCachedStore<C: Codec> {
    store: CachedStore,
    codec: PhantomData<fn() -> C>,
}

/// One typed backend listing page with every object path parsed at ingress.
pub(crate) struct TypedListPage {
    pub(crate) objects: Vec<ObjectKey>,
    pub(crate) next: Option<backend::ListCursor>,
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
    fn check_path(key: &ObjectKey) -> Result<(), StorageError> {
        if C::accepts(key.object_path()) {
            Ok(())
        } else {
            Err(StorageError::other(format!(
                "path {:?} does not name a {} object",
                key.as_str(),
                C::name()
            )))
        }
    }

    /// Returns a cached observation without backend I/O.
    pub(crate) fn peek(
        &self,
        key: impl Into<ObjectKey>,
    ) -> Result<Option<Observation<C::Value>>, StorageError> {
        let key = key.into();
        Self::check_path(&key)?;
        self.store.peek::<C>(&key)
    }

    /// Reads the current state with the requested freshness requirement.
    pub(crate) async fn read(
        &self,
        key: impl Into<ObjectKey>,
        requirement: Requirement,
    ) -> Result<Observation<C::Value>, StorageError> {
        let key = key.into();
        Self::check_path(&key)?;
        self.store.read::<C>(key, requirement).await
    }

    /// Lists one page of object paths belonging to this typed store.
    pub(crate) async fn list(
        &self,
        prefix: &str,
        cursor: Option<&backend::ListCursor>,
        limit: backend::ListLimit,
    ) -> Result<TypedListPage, StorageError> {
        let page = self.store.list(prefix, cursor, limit).await?;
        let mut objects = Vec::with_capacity(page.objects.len());
        for encoded in page.objects {
            let key = ObjectKey::parse(Arc::<str>::from(encoded))
                .map_err(|error| StorageError::with_source("parsing listed object path", error))?;
            Self::check_path(&key)?;
            objects.push(key);
        }
        Ok(TypedListPage {
            objects,
            next: page.next,
        })
    }

    /// Checks whether an exact retained observation is current after `bound`.
    pub(crate) async fn check_current(
        &self,
        observed: &Observation<C::Value>,
        bound: SequencePoint,
    ) -> Result<ObservationCheck<C::Value>, StorageError> {
        Self::check_path(&observed.key)?;
        self.store
            .check_current::<C>(observed, Requirement::AtLeast(bound))
            .await
    }

    /// Creates a decoded object if it is absent.
    pub(crate) async fn create(
        &self,
        key: impl Into<ObjectKey>,
        expected_absence: Option<&Observation<C::Value>>,
        value: Arc<C::Value>,
    ) -> Result<CasResult<C::Value>, StorageError> {
        let key = key.into();
        Self::check_path(&key)?;
        if let Some(expected) = expected_absence
            && (!expected.is_absent() || expected.key != key)
        {
            return Err(StorageError::other(
                "create requires an absence observation for the same path",
            ));
        }
        self.store.create::<C>(key, expected_absence, value).await
    }

    /// Conditionally replaces the exact observed revision.
    pub(crate) async fn compare_and_swap(
        &self,
        expected: &Observation<C::Value>,
        value: Arc<C::Value>,
    ) -> Result<CasResult<C::Value>, StorageError> {
        Self::check_path(&expected.key)?;
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
        Self::check_path(&expected.key)?;
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

fn same_observed_state<V>(left: &Observation<V>, right: &Observation<V>) -> bool {
    left.key == right.key && left.revision == right.revision
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{
        BackendOp, HookBackend, HookFuture, OpLog, RecordingBackend,
    };
    #[cfg(sim)]
    use glassdb_concurr::exec;
    use glassdb_data::DatabaseId;
    use tempfile::TempDir;
    use tokio::sync::Notify;

    use super::*;
    #[cfg(sim)]
    use crate::disk_cache::PathFence;
    use crate::disk_cache::PersistentCacheConfig;
    use crate::disk_cache::sim_media::{MediaFaultProfile, SimMedia};
    use crate::timeline::TimeSource;

    // A trivial identity codec so the concurrency layer can be exercised in
    // isolation from any real object type.
    struct Bytes;

    impl Codec for Bytes {
        type Value = Vec<u8>;
        fn decode(_path: &ObjectPath, bytes: &[u8]) -> Result<Vec<u8>, StorageError> {
            Ok(bytes.to_vec())
        }
        fn encode(_path: &ObjectPath, value: &Vec<u8>) -> Result<Vec<u8>, StorageError> {
            Ok(value.clone())
        }
        fn size(value: &Vec<u8>) -> usize {
            value.len()
        }
        fn accepts(_: &ObjectPath) -> bool {
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
        fn decode(_path: &ObjectPath, bytes: &[u8]) -> Result<u64, StorageError> {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| StorageError::other("bad int"))?;
            Ok(u64::from_le_bytes(arr))
        }
        fn encode(_path: &ObjectPath, value: &u64) -> Result<Vec<u8>, StorageError> {
            Ok(value.to_le_bytes().to_vec())
        }
        fn size(_: &u64) -> usize {
            8
        }
        fn accepts(_: &ObjectPath) -> bool {
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

    fn cache_id() -> DatabaseId {
        DatabaseId::from_bytes([7; 16])
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
            cache_id(),
        )
        .await;
        let timeline = Timeline::starting_after(opened.last_sequence_point);
        let store = CachedStore::new(backend, 1 << 20, timeline.clone(), Some(opened.cache));
        (store, timeline)
    }

    async fn simulated_persistent_store(
        directory: &TempDir,
        backend: Arc<dyn Backend>,
        media: SimMedia,
    ) -> (CachedStore, Timeline) {
        let opened = PersistentCache::open_with_sim_media(
            PersistentCacheConfig {
                directory: directory.path().to_path_buf(),
                capacity_bytes: 2 * 1024 * 1024,
            },
            "db",
            cache_id(),
            media,
        )
        .await;
        let timeline = Timeline::starting_after(opened.last_sequence_point);
        let store = CachedStore::new(backend, 1 << 20, timeline.clone(), Some(opened.cache));
        (store, timeline)
    }

    #[cfg(sim)]
    #[test]
    fn persistent_cache_runs_with_cached_store_in_deterministic_simulation() {
        let directory = TempDir::new().unwrap();
        let media = SimMedia::new(MediaFaultProfile::Healthy, Vec::new(), 0);
        exec::block_on_with(exec::TapeScheduler::new(Vec::new()), 0, async move {
            let backend = Arc::new(MemoryBackend::new());
            backend
                .write_if_not_exists("p", b"one".to_vec())
                .await
                .unwrap();
            let erased: Arc<dyn Backend> = backend;
            let (first, _) =
                simulated_persistent_store(&directory, erased.clone(), media.clone()).await;
            let typed: TypedCachedStore<Bytes> = first.typed();
            let loaded = typed.read("p", Requirement::Any).await.unwrap();
            let persisted = loaded.current_after();
            drop(typed);
            first.shutdown().await;
            drop(first);

            let (reopened, timeline) = simulated_persistent_store(&directory, erased, media).await;
            assert!(timeline.now() > persisted);
            let typed: TypedCachedStore<Bytes> = reopened.typed();
            let restored = typed.read("p", Requirement::Any).await.unwrap();
            assert_eq!(restored.value().unwrap().as_slice(), b"one");
            assert_eq!(restored.current_after(), persisted);
            assert!(restored.cache_hit());
            assert_eq!(reopened.body_reads(), 0);
            drop(typed);
            reopened.shutdown().await;
        });
    }

    #[cfg(sim)]
    #[test]
    fn simulated_media_failure_remains_a_cached_store_performance_failure() {
        let directory = TempDir::new().unwrap();
        let media = SimMedia::new(MediaFaultProfile::Selected, vec![255], 0);
        exec::block_on_with(exec::TapeScheduler::new(Vec::new()), 0, async move {
            let backend = Arc::new(MemoryBackend::new());
            backend
                .write_if_not_exists("p", b"one".to_vec())
                .await
                .unwrap();
            let erased: Arc<dyn Backend> = backend;
            let (store, _) = simulated_persistent_store(&directory, erased, media).await;
            assert!(!store.persistent.is_enabled());

            let typed: TypedCachedStore<Bytes> = store.typed();
            let loaded = typed.read("p", Requirement::Any).await.unwrap();
            assert_eq!(loaded.value().unwrap().as_slice(), b"one");
            drop(typed);
            store.shutdown().await;
        });
    }

    #[cfg(not(sim))]
    #[tokio::test]
    async fn slow_persistent_lookup_falls_back_to_one_backend_read() {
        let directory = TempDir::new().unwrap();
        let media = SimMedia::new(MediaFaultProfile::Healthy, Vec::new(), 0);
        let backend = Arc::new(MemoryBackend::new());
        backend
            .write_if_not_exists("p", b"one".to_vec())
            .await
            .unwrap();
        let recorded = Arc::new(RecordingBackend::new(backend));
        let log = recorded.log();
        let erased: Arc<dyn Backend> = recorded;

        let (first, _) =
            simulated_persistent_store(&directory, erased.clone(), media.clone()).await;
        let first_typed: TypedCachedStore<Bytes> = first.typed();
        first_typed.read("p", Requirement::Any).await.unwrap();
        drop(first_typed);
        first.shutdown().await;
        drop(first);
        clear(&log);

        let (reopened, _) = simulated_persistent_store(&directory, erased, media.clone()).await;
        assert!(reopened.persistent.is_enabled());
        let typed: TypedCachedStore<Bytes> = reopened.typed();
        let mut pause = media.pause_next_operation();
        let mut read = tokio::spawn({
            let typed = typed.clone();
            async move { typed.read("p", Requirement::Any).await }
        });
        tokio::select! {
            () = pause.wait_until_entered() => {}
            result = &mut read => panic!("read completed before media pause: {result:?}"),
        }
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(6)).await;

        let loaded = read.await.unwrap().unwrap();
        assert_eq!(loaded.value().unwrap().as_slice(), b"one");
        assert_eq!(reopened.body_reads(), 1);
        assert_eq!(count(&log, "read"), 1);
        assert_eq!(count(&log, "read_if_modified"), 0);
        assert!(!reopened.persistent.is_enabled());
        let stats = reopened.cache_stats_and_reset();
        assert_eq!(stats.l2_errors, 1, "cache stats: {stats:?}");

        pause.resume();
        drop(typed);
        reopened.shutdown().await;
    }

    #[cfg(sim)]
    #[test]
    fn simulated_invalid_candidate_is_rejected_before_escape() {
        let directory = TempDir::new().unwrap();
        let media = SimMedia::new(MediaFaultProfile::Healthy, Vec::new(), 0);
        exec::block_on_with(exec::TapeScheduler::new(Vec::new()), 0, async move {
            let backend = Arc::new(MemoryBackend::new());
            backend
                .write_if_not_exists("p", b"backend".to_vec())
                .await
                .unwrap();
            let recorded = Arc::new(RecordingBackend::new(backend));
            let log = recorded.log();
            let opened = PersistentCache::open_with_sim_media(
                PersistentCacheConfig {
                    directory: directory.path().to_path_buf(),
                    capacity_bytes: 2 * 1024 * 1024,
                },
                "db",
                cache_id(),
                media,
            )
            .await;
            let persistent = opened.cache;
            let guard = persistent
                .begin_fence(Arc::new(PathFence::default()))
                .unwrap();
            persistent.replace(
                Arc::from("p"),
                vec![0xff],
                b"untrusted".to_vec(),
                SequencePoint::from_raw(1),
                guard,
            );

            let erased: Arc<dyn Backend> = recorded;
            let store = CachedStore::new(
                erased,
                1 << 20,
                Timeline::starting_after(opened.last_sequence_point),
                Some(persistent),
            );
            let typed: TypedCachedStore<Bytes> = store.typed();
            let loaded = typed.read("p", Requirement::Any).await.unwrap();
            assert_eq!(loaded.value().unwrap().as_slice(), b"backend");
            assert_eq!(store.body_reads(), 1);
            assert_eq!(count(&log, "read"), 1);
            assert_eq!(count(&log, "read_if_modified"), 0);
            assert!(store.cache_stats_and_reset().l2_errors >= 1);
            drop(typed);
            store.shutdown().await;
        });
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

    const OLD_VALUE: &[u8] = b"old";
    const PROPOSED_VALUE: &[u8] = b"proposed";
    const WINNER_VALUE: &[u8] = b"winner";

    struct ProtocolBackend {
        memory: Arc<MemoryBackend>,
        hook: Arc<HookBackend>,
        operations: OpLog,
        backend: Arc<dyn Backend>,
    }

    impl ProtocolBackend {
        fn new() -> Self {
            let memory = Arc::new(MemoryBackend::new());
            let inner: Arc<dyn Backend> = memory.clone();
            let hook = HookBackend::new(inner);
            // Record outside the hook so an injected error or a cancelled hook
            // still counts as one call across the CachedStore boundary.
            let hooked: Arc<dyn Backend> = hook.clone();
            let recording = Arc::new(RecordingBackend::new(hooked));
            let operations = recording.log();
            let backend: Arc<dyn Backend> = recording;
            Self {
                memory,
                hook,
                operations,
                backend,
            }
        }

        fn store(&self) -> TypedCachedStore<Bytes> {
            bytes_store(self.backend.clone())
        }

        fn clear_operations(&self) {
            clear(&self.operations);
        }

        fn assert_operations(&self, expected: &[&str], context: &str) {
            let operations = self.operations.lock().unwrap();
            let actual: Vec<_> = operations.iter().map(|operation| operation.op).collect();
            assert_eq!(actual, expected, "{context}: operations: {operations:?}");
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum ExpectedValue {
        Absent,
        Old,
        Proposed,
        Winner,
    }

    impl ExpectedValue {
        fn bytes(self) -> Option<&'static [u8]> {
            match self {
                Self::Absent => None,
                Self::Old => Some(OLD_VALUE),
                Self::Proposed => Some(PROPOSED_VALUE),
                Self::Winner => Some(WINNER_VALUE),
            }
        }
    }

    fn assert_observation(
        observed: &Observation<Vec<u8>>,
        expected: ExpectedValue,
        cache_hit: bool,
        context: &str,
    ) {
        assert_eq!(
            observed.value().map(|value| value.as_slice()),
            expected.bytes(),
            "{context}: value"
        );
        assert_eq!(observed.cache_hit(), cache_hit, "{context}: cache hit");
    }

    #[derive(Clone, Copy, Debug)]
    enum MutationKind {
        Create,
        Cas,
        Delete,
    }

    impl MutationKind {
        fn operation(self) -> &'static str {
            match self {
                Self::Create => "write_if_not_exists",
                Self::Cas => "write_if",
                Self::Delete => "delete_if",
            }
        }

        fn matches(self, operation: &BackendOp<'_>) -> bool {
            matches!(
                (self, operation),
                (Self::Create, BackendOp::WriteIfNotExists { .. })
                    | (Self::Cas, BackendOp::WriteIf { .. })
                    | (Self::Delete, BackendOp::DeleteIf { .. })
            )
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum KnowledgeCase {
        Absent,
        Matching,
        Stale,
        Missing,
        KnownWinner,
    }

    #[derive(Clone, Copy, Debug)]
    enum CompletionCase {
        Natural,
        UnavailableAfterApply,
        DefinitiveBeforeApply,
        CancelledAfterApply,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum ExpectedMutationResult {
        Committed,
        Conflict,
        Deleted,
        Precondition,
        Unavailable,
        Definitive,
        Cancelled,
    }

    #[derive(Debug)]
    struct MutationResult {
        kind: ExpectedMutationResult,
        observation: Option<Observation<Vec<u8>>>,
    }

    async fn invoke_mutation(
        store: TypedCachedStore<Bytes>,
        kind: MutationKind,
        expected: Option<Observation<Vec<u8>>>,
    ) -> MutationResult {
        match kind {
            MutationKind::Create => match store
                .create("p", expected.as_ref(), v(PROPOSED_VALUE))
                .await
            {
                Ok(CasResult::Committed(observation)) => MutationResult {
                    kind: ExpectedMutationResult::Committed,
                    observation: Some(observation),
                },
                Ok(CasResult::Conflict) => MutationResult {
                    kind: ExpectedMutationResult::Conflict,
                    observation: None,
                },
                Err(StorageError::Unavailable(_)) => MutationResult {
                    kind: ExpectedMutationResult::Unavailable,
                    observation: None,
                },
                Err(StorageError::Other { .. }) => MutationResult {
                    kind: ExpectedMutationResult::Definitive,
                    observation: None,
                },
                Err(error) => panic!("unexpected create result: {error:?}"),
            },
            MutationKind::Cas => match store
                .compare_and_swap(
                    expected.as_ref().expect("CAS case needs an observation"),
                    v(PROPOSED_VALUE),
                )
                .await
            {
                Ok(CasResult::Committed(observation)) => MutationResult {
                    kind: ExpectedMutationResult::Committed,
                    observation: Some(observation),
                },
                Ok(CasResult::Conflict) => MutationResult {
                    kind: ExpectedMutationResult::Conflict,
                    observation: None,
                },
                Err(StorageError::Unavailable(_)) => MutationResult {
                    kind: ExpectedMutationResult::Unavailable,
                    observation: None,
                },
                Err(StorageError::Other { .. }) => MutationResult {
                    kind: ExpectedMutationResult::Definitive,
                    observation: None,
                },
                Err(error) => panic!("unexpected CAS result: {error:?}"),
            },
            MutationKind::Delete => match store
                .delete(expected.as_ref().expect("delete case needs an observation"))
                .await
            {
                Ok(observation) => MutationResult {
                    kind: ExpectedMutationResult::Deleted,
                    observation: Some(observation),
                },
                Err(StorageError::Precondition) => MutationResult {
                    kind: ExpectedMutationResult::Precondition,
                    observation: None,
                },
                Err(StorageError::Unavailable(_)) => MutationResult {
                    kind: ExpectedMutationResult::Unavailable,
                    observation: None,
                },
                Err(StorageError::Other { .. }) => MutationResult {
                    kind: ExpectedMutationResult::Definitive,
                    observation: None,
                },
                Err(error) => panic!("unexpected delete result: {error:?}"),
            },
        }
    }

    struct PreparedMutation {
        expected: Observation<Vec<u8>>,
        known_winner: Option<Observation<Vec<u8>>>,
    }

    async fn prepare_mutation(
        protocol: &ProtocolBackend,
        store: &TypedCachedStore<Bytes>,
        kind: MutationKind,
        knowledge: KnowledgeCase,
    ) -> PreparedMutation {
        let expected = match (kind, knowledge) {
            (MutationKind::Create, KnowledgeCase::Absent | KnowledgeCase::Stale) => {
                store.read("p", Requirement::Any).await.unwrap()
            }
            (_, KnowledgeCase::Matching)
            | (_, KnowledgeCase::Stale)
            | (_, KnowledgeCase::Missing)
            | (_, KnowledgeCase::KnownWinner) => create_value(store, "p", v(OLD_VALUE)).await,
            (_, KnowledgeCase::Absent) => {
                panic!("only create cases use known absence")
            }
        };

        match (kind, knowledge) {
            (MutationKind::Create, KnowledgeCase::Stale) => {
                protocol
                    .memory
                    .write_if_not_exists("p", WINNER_VALUE.to_vec())
                    .await
                    .unwrap();
            }
            (_, KnowledgeCase::Stale | KnowledgeCase::KnownWinner) => {
                protocol
                    .memory
                    .write_if(
                        "p",
                        WINNER_VALUE.to_vec(),
                        expected.revision().unwrap().version(),
                    )
                    .await
                    .unwrap();
            }
            (_, KnowledgeCase::Missing) => {
                protocol
                    .memory
                    .delete_if("p", expected.revision().unwrap().version())
                    .await
                    .unwrap();
            }
            (_, KnowledgeCase::Absent | KnowledgeCase::Matching) => {}
        }

        let known_winner = if matches!(knowledge, KnowledgeCase::KnownWinner) {
            let bound = store.store.timeline.now();
            Some(store.read("p", Requirement::AtLeast(bound)).await.unwrap())
        } else {
            None
        };

        assert_eq!(
            expected.is_absent(),
            matches!(kind, MutationKind::Create),
            "the matrix uses absence only as create's predicate"
        );
        protocol.clear_operations();
        PreparedMutation {
            expected,
            known_winner,
        }
    }

    struct MutationCase {
        name: &'static str,
        kind: MutationKind,
        knowledge: KnowledgeCase,
        completion: CompletionCase,
        result: ExpectedMutationResult,
        advance_expected: bool,
        next_value: ExpectedValue,
        next_cache_hit: bool,
    }

    const MUTATION_CASES: &[MutationCase] = &[
        MutationCase {
            name: "create commits from known absence",
            kind: MutationKind::Create,
            knowledge: KnowledgeCase::Absent,
            completion: CompletionCase::Natural,
            result: ExpectedMutationResult::Committed,
            advance_expected: true,
            next_value: ExpectedValue::Proposed,
            next_cache_hit: true,
        },
        MutationCase {
            name: "create conflicts with stale absence",
            kind: MutationKind::Create,
            knowledge: KnowledgeCase::Stale,
            completion: CompletionCase::Natural,
            result: ExpectedMutationResult::Conflict,
            advance_expected: false,
            next_value: ExpectedValue::Winner,
            next_cache_hit: false,
        },
        MutationCase {
            name: "create lost acknowledgement is uncertain",
            kind: MutationKind::Create,
            knowledge: KnowledgeCase::Absent,
            completion: CompletionCase::UnavailableAfterApply,
            result: ExpectedMutationResult::Unavailable,
            advance_expected: false,
            next_value: ExpectedValue::Proposed,
            next_cache_hit: false,
        },
        MutationCase {
            name: "create definitive failure preserves absence",
            kind: MutationKind::Create,
            knowledge: KnowledgeCase::Absent,
            completion: CompletionCase::DefinitiveBeforeApply,
            result: ExpectedMutationResult::Definitive,
            advance_expected: false,
            next_value: ExpectedValue::Absent,
            next_cache_hit: true,
        },
        MutationCase {
            name: "cancelled invoked create is uncertain",
            kind: MutationKind::Create,
            knowledge: KnowledgeCase::Absent,
            completion: CompletionCase::CancelledAfterApply,
            result: ExpectedMutationResult::Cancelled,
            advance_expected: false,
            next_value: ExpectedValue::Proposed,
            next_cache_hit: false,
        },
        MutationCase {
            name: "CAS commits from matching revision",
            kind: MutationKind::Cas,
            knowledge: KnowledgeCase::Matching,
            completion: CompletionCase::Natural,
            result: ExpectedMutationResult::Committed,
            advance_expected: true,
            next_value: ExpectedValue::Proposed,
            next_cache_hit: true,
        },
        MutationCase {
            name: "CAS conflict invalidates stale revision",
            kind: MutationKind::Cas,
            knowledge: KnowledgeCase::Stale,
            completion: CompletionCase::Natural,
            result: ExpectedMutationResult::Conflict,
            advance_expected: false,
            next_value: ExpectedValue::Winner,
            next_cache_hit: false,
        },
        MutationCase {
            name: "CAS conflict preserves known winner",
            kind: MutationKind::Cas,
            knowledge: KnowledgeCase::KnownWinner,
            completion: CompletionCase::Natural,
            result: ExpectedMutationResult::Conflict,
            advance_expected: false,
            next_value: ExpectedValue::Winner,
            next_cache_hit: true,
        },
        MutationCase {
            name: "CAS missing installs absence",
            kind: MutationKind::Cas,
            knowledge: KnowledgeCase::Missing,
            completion: CompletionCase::Natural,
            result: ExpectedMutationResult::Conflict,
            advance_expected: false,
            next_value: ExpectedValue::Absent,
            next_cache_hit: true,
        },
        MutationCase {
            name: "CAS lost acknowledgement is uncertain",
            kind: MutationKind::Cas,
            knowledge: KnowledgeCase::Matching,
            completion: CompletionCase::UnavailableAfterApply,
            result: ExpectedMutationResult::Unavailable,
            advance_expected: false,
            next_value: ExpectedValue::Proposed,
            next_cache_hit: false,
        },
        MutationCase {
            name: "CAS definitive failure preserves expected revision",
            kind: MutationKind::Cas,
            knowledge: KnowledgeCase::Matching,
            completion: CompletionCase::DefinitiveBeforeApply,
            result: ExpectedMutationResult::Definitive,
            advance_expected: false,
            next_value: ExpectedValue::Old,
            next_cache_hit: true,
        },
        MutationCase {
            name: "cancelled invoked CAS is uncertain",
            kind: MutationKind::Cas,
            knowledge: KnowledgeCase::Matching,
            completion: CompletionCase::CancelledAfterApply,
            result: ExpectedMutationResult::Cancelled,
            advance_expected: false,
            next_value: ExpectedValue::Proposed,
            next_cache_hit: false,
        },
        MutationCase {
            name: "delete commits from matching revision",
            kind: MutationKind::Delete,
            knowledge: KnowledgeCase::Matching,
            completion: CompletionCase::Natural,
            result: ExpectedMutationResult::Deleted,
            advance_expected: true,
            next_value: ExpectedValue::Absent,
            next_cache_hit: true,
        },
        MutationCase {
            name: "delete conflict invalidates stale revision",
            kind: MutationKind::Delete,
            knowledge: KnowledgeCase::Stale,
            completion: CompletionCase::Natural,
            result: ExpectedMutationResult::Precondition,
            advance_expected: false,
            next_value: ExpectedValue::Winner,
            next_cache_hit: false,
        },
        MutationCase {
            name: "delete conflict preserves known winner",
            kind: MutationKind::Delete,
            knowledge: KnowledgeCase::KnownWinner,
            completion: CompletionCase::Natural,
            result: ExpectedMutationResult::Precondition,
            advance_expected: false,
            next_value: ExpectedValue::Winner,
            next_cache_hit: true,
        },
        MutationCase {
            name: "delete missing converges on absence",
            kind: MutationKind::Delete,
            knowledge: KnowledgeCase::Missing,
            completion: CompletionCase::Natural,
            result: ExpectedMutationResult::Deleted,
            advance_expected: false,
            next_value: ExpectedValue::Absent,
            next_cache_hit: true,
        },
        MutationCase {
            name: "delete lost acknowledgement is uncertain",
            kind: MutationKind::Delete,
            knowledge: KnowledgeCase::Matching,
            completion: CompletionCase::UnavailableAfterApply,
            result: ExpectedMutationResult::Unavailable,
            advance_expected: false,
            next_value: ExpectedValue::Absent,
            next_cache_hit: false,
        },
        MutationCase {
            name: "delete definitive failure preserves expected revision",
            kind: MutationKind::Delete,
            knowledge: KnowledgeCase::Matching,
            completion: CompletionCase::DefinitiveBeforeApply,
            result: ExpectedMutationResult::Definitive,
            advance_expected: false,
            next_value: ExpectedValue::Old,
            next_cache_hit: true,
        },
        MutationCase {
            name: "cancelled invoked delete is uncertain",
            kind: MutationKind::Delete,
            knowledge: KnowledgeCase::Matching,
            completion: CompletionCase::CancelledAfterApply,
            result: ExpectedMutationResult::Cancelled,
            advance_expected: false,
            next_value: ExpectedValue::Absent,
            next_cache_hit: false,
        },
    ];

    #[tokio::test]
    async fn mutation_protocol_matrix() {
        for case in MUTATION_CASES {
            let protocol = ProtocolBackend::new();
            let store = protocol.store();
            let prepared = prepare_mutation(&protocol, &store, case.kind, case.knowledge).await;
            let original_evidence = prepared.expected.current_after();
            let known_winner_evidence = prepared
                .known_winner
                .as_ref()
                .map(Observation::current_after);
            let barrier = store.store.timeline.now();

            let result = match case.completion {
                CompletionCase::Natural => {
                    invoke_mutation(store.clone(), case.kind, Some(prepared.expected.clone())).await
                }
                CompletionCase::UnavailableAfterApply => {
                    protocol.hook.set_after({
                        let kind = case.kind;
                        move |operation, outcome| {
                            ready(if kind.matches(operation) && outcome.is_success() {
                                Err(BackendError::Unavailable("lost acknowledgement".into()))
                            } else {
                                Ok(())
                            })
                        }
                    });
                    invoke_mutation(store.clone(), case.kind, Some(prepared.expected.clone())).await
                }
                CompletionCase::DefinitiveBeforeApply => {
                    protocol.hook.set_before({
                        let kind = case.kind;
                        move |operation| {
                            ready(if kind.matches(operation) {
                                Err(BackendError::other("rejected before apply"))
                            } else {
                                Ok(())
                            })
                        }
                    });
                    invoke_mutation(store.clone(), case.kind, Some(prepared.expected.clone())).await
                }
                CompletionCase::CancelledAfterApply => {
                    let entered = Arc::new(Notify::new());
                    protocol.hook.set_after({
                        let kind = case.kind;
                        let entered = entered.clone();
                        move |operation, outcome| {
                            let should_cancel = kind.matches(operation) && outcome.is_success();
                            let entered = entered.clone();
                            Box::pin(async move {
                                if should_cancel {
                                    entered.notify_one();
                                    std::future::pending::<()>().await;
                                }
                                Ok(())
                            })
                        }
                    });
                    let mutation = tokio::spawn(invoke_mutation(
                        store.clone(),
                        case.kind,
                        Some(prepared.expected.clone()),
                    ));
                    entered.notified().await;
                    mutation.abort();
                    assert!(mutation.await.unwrap_err().is_cancelled(), "{}", case.name);
                    MutationResult {
                        kind: ExpectedMutationResult::Cancelled,
                        observation: None,
                    }
                }
            };

            protocol.hook.clear_before();
            protocol.hook.clear_after();
            assert_eq!(result.kind, case.result, "{}: result", case.name);
            assert_eq!(
                prepared.expected.current_after() >= barrier,
                case.advance_expected,
                "{}: expected evidence",
                case.name
            );
            if !case.advance_expected {
                assert_eq!(
                    prepared.expected.current_after(),
                    original_evidence,
                    "{}: unchanged expected evidence",
                    case.name
                );
            }
            if let Some(known_winner) = &prepared.known_winner {
                assert_eq!(
                    known_winner.current_after(),
                    known_winner_evidence.unwrap(),
                    "{}: known winner evidence",
                    case.name
                );
            }
            if let Some(observation) = &result.observation {
                let value = if matches!(case.kind, MutationKind::Delete) {
                    ExpectedValue::Absent
                } else {
                    ExpectedValue::Proposed
                };
                assert_observation(observation, value, false, case.name);
                assert!(
                    observation.current_after() >= barrier,
                    "{}: returned evidence",
                    case.name
                );
            }
            protocol.assert_operations(&[case.kind.operation()], case.name);

            protocol.clear_operations();
            let next = store.read("p", Requirement::Any).await.unwrap();
            assert_observation(&next, case.next_value, case.next_cache_hit, case.name);
            let expected_operations: &[&str] = if case.next_cache_hit { &[] } else { &["read"] };
            protocol.assert_operations(expected_operations, case.name);
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum ReadCompletionCase {
        Present,
        Absent,
        Unavailable,
        Definitive,
    }

    #[tokio::test]
    async fn read_coalescing_protocol_matrix() {
        for completion in [
            ReadCompletionCase::Present,
            ReadCompletionCase::Absent,
            ReadCompletionCase::Unavailable,
            ReadCompletionCase::Definitive,
        ] {
            let context = format!("coalesced {completion:?} read");
            let protocol = ProtocolBackend::new();
            if matches!(completion, ReadCompletionCase::Present) {
                protocol
                    .memory
                    .write_if_not_exists("p", OLD_VALUE.to_vec())
                    .await
                    .unwrap();
            }
            let store = protocol.store();
            let barrier = store.store.timeline.now();
            let entered = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            let first = Arc::new(AtomicBool::new(true));
            protocol.hook.set_before({
                let entered = entered.clone();
                let release = release.clone();
                let first = first.clone();
                move |operation| {
                    let gate = matches!(operation, BackendOp::Read { path } if *path == "p")
                        && first.swap(false, Ordering::SeqCst);
                    let entered = entered.clone();
                    let release = release.clone();
                    Box::pin(async move {
                        if gate {
                            entered.notify_one();
                            release.notified().await;
                        }
                        match completion {
                            ReadCompletionCase::Present | ReadCompletionCase::Absent => Ok(()),
                            ReadCompletionCase::Unavailable => {
                                Err(BackendError::Unavailable("read unavailable".into()))
                            }
                            ReadCompletionCase::Definitive => {
                                Err(BackendError::other("read rejected"))
                            }
                        }
                    })
                }
            });

            let leader = tokio::spawn({
                let store = store.clone();
                async move { store.read("p", Requirement::Any).await }
            });
            entered.notified().await;
            let waiter = tokio::spawn({
                let store = store.clone();
                async move { store.read("p", Requirement::Any).await }
            });
            for _ in 0..64 {
                tokio::task::yield_now().await;
            }
            release.notify_one();

            let leader = leader.await.unwrap();
            let waiter = waiter.await.unwrap();
            match completion {
                ReadCompletionCase::Present | ReadCompletionCase::Absent => {
                    let leader = leader.unwrap();
                    let waiter = waiter.unwrap();
                    let value = if matches!(completion, ReadCompletionCase::Present) {
                        ExpectedValue::Old
                    } else {
                        ExpectedValue::Absent
                    };
                    assert_observation(&leader, value, false, &context);
                    assert_observation(&waiter, value, false, &context);
                    assert!(leader.same_state(&waiter), "{context}: shared state");
                    assert_eq!(
                        leader.current_after(),
                        waiter.current_after(),
                        "{context}: shared watermark"
                    );
                    assert!(leader.current_after() >= barrier, "{context}: evidence");
                }
                ReadCompletionCase::Unavailable => {
                    assert!(
                        matches!(leader, Err(StorageError::Unavailable(_))),
                        "{context}"
                    );
                    assert!(
                        matches!(waiter, Err(StorageError::Unavailable(_))),
                        "{context}"
                    );
                }
                ReadCompletionCase::Definitive => {
                    assert!(
                        matches!(leader, Err(StorageError::Other { .. })),
                        "{context}"
                    );
                    assert!(
                        matches!(waiter, Err(StorageError::Other { .. })),
                        "{context}"
                    );
                }
            }
            protocol.assert_operations(&["read"], &context);

            protocol.hook.clear_before();
            protocol.clear_operations();
            let next = store.read("p", Requirement::Any).await.unwrap();
            let (next_value, next_hit) = match completion {
                ReadCompletionCase::Present => (ExpectedValue::Old, true),
                ReadCompletionCase::Absent => (ExpectedValue::Absent, true),
                ReadCompletionCase::Unavailable | ReadCompletionCase::Definitive => {
                    (ExpectedValue::Absent, false)
                }
            };
            assert_observation(&next, next_value, next_hit, &context);
            let expected_operations: &[&str] = if next_hit { &[] } else { &["read"] };
            protocol.assert_operations(expected_operations, &context);
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum CancelledReader {
        Leader,
        Waiter,
    }

    #[tokio::test]
    async fn read_cancellation_protocol_matrix() {
        for cancelled in [CancelledReader::Leader, CancelledReader::Waiter] {
            let context = format!("cancelled read {cancelled:?}");
            let protocol = ProtocolBackend::new();
            protocol
                .memory
                .write_if_not_exists("p", OLD_VALUE.to_vec())
                .await
                .unwrap();
            let store = protocol.store();
            let barrier = store.store.timeline.now();
            let entered = Arc::new(Notify::new());
            let first = Arc::new(AtomicBool::new(true));

            match cancelled {
                CancelledReader::Leader => {
                    protocol.hook.set_before({
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
                    let leader = tokio::spawn({
                        let store = store.clone();
                        async move { store.read("p", Requirement::Any).await }
                    });
                    entered.notified().await;
                    let waiter = tokio::spawn({
                        let store = store.clone();
                        async move { store.read("p", Requirement::Any).await }
                    });
                    for _ in 0..64 {
                        tokio::task::yield_now().await;
                    }
                    leader.abort();
                    assert!(leader.await.unwrap_err().is_cancelled(), "{context}");
                    let observed = tokio::time::timeout(Duration::from_secs(1), waiter)
                        .await
                        .expect("waiter remained stuck behind its cancelled leader")
                        .unwrap()
                        .unwrap();
                    assert_observation(&observed, ExpectedValue::Old, false, &context);
                    assert!(observed.current_after() >= barrier, "{context}: evidence");
                    protocol.assert_operations(&["read", "read"], &context);
                }
                CancelledReader::Waiter => {
                    let release = Arc::new(Notify::new());
                    protocol.hook.set_before({
                        let entered = entered.clone();
                        let release = release.clone();
                        let first = first.clone();
                        move |operation| {
                            let gate = matches!(operation, BackendOp::Read { path } if *path == "p")
                                && first.swap(false, Ordering::SeqCst);
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
                    let leader = tokio::spawn({
                        let store = store.clone();
                        async move { store.read("p", Requirement::Any).await }
                    });
                    entered.notified().await;
                    let waiter = tokio::spawn({
                        let store = store.clone();
                        async move { store.read("p", Requirement::Any).await }
                    });
                    for _ in 0..64 {
                        tokio::task::yield_now().await;
                    }
                    waiter.abort();
                    assert!(waiter.await.unwrap_err().is_cancelled(), "{context}");
                    release.notify_one();
                    let observed = leader.await.unwrap().unwrap();
                    assert_observation(&observed, ExpectedValue::Old, false, &context);
                    assert!(observed.current_after() >= barrier, "{context}: evidence");
                    protocol.assert_operations(&["read"], &context);
                }
            }

            protocol.hook.clear_before();
            protocol.clear_operations();
            let next = store.read("p", Requirement::Any).await.unwrap();
            assert_observation(&next, ExpectedValue::Old, true, &context);
            protocol.assert_operations(&[], &context);
        }
    }

    #[tokio::test]
    async fn queued_mutation_cancellation_protocol_matrix() {
        for kind in [
            MutationKind::Create,
            MutationKind::Cas,
            MutationKind::Delete,
        ] {
            let context = format!("queued {kind:?} cancellation");
            let protocol = ProtocolBackend::new();
            let store = protocol.store();
            let knowledge = if matches!(kind, MutationKind::Create) {
                KnowledgeCase::Absent
            } else {
                KnowledgeCase::Matching
            };
            let prepared = prepare_mutation(&protocol, &store, kind, knowledge).await;
            let expected_value = if matches!(kind, MutationKind::Create) {
                ExpectedValue::Absent
            } else {
                ExpectedValue::Old
            };
            let entered = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            protocol.hook.set_after({
                let entered = entered.clone();
                let release = release.clone();
                move |operation, _| {
                    let gate = match kind {
                        MutationKind::Create => {
                            matches!(operation, BackendOp::Read { path } if *path == "p")
                        }
                        MutationKind::Cas | MutationKind::Delete => matches!(
                            operation,
                            BackendOp::ReadIfModified { path, .. } if *path == "p"
                        ),
                    };
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

            let bound = store.store.timeline.now();
            let validating = tokio::spawn({
                let store = store.clone();
                async move { store.read("p", Requirement::AtLeast(bound)).await }
            });
            entered.notified().await;
            protocol.clear_operations();
            let mutation = tokio::spawn(invoke_mutation(
                store.clone(),
                kind,
                Some(prepared.expected.clone()),
            ));
            for _ in 0..64 {
                tokio::task::yield_now().await;
            }
            mutation.abort();
            assert!(mutation.await.unwrap_err().is_cancelled(), "{context}");
            release.notify_one();
            let validated = validating.await.unwrap().unwrap();
            assert_observation(
                &validated,
                expected_value,
                !matches!(kind, MutationKind::Create),
                &context,
            );
            protocol.hook.clear_after();
            protocol.assert_operations(&[], &context);

            protocol.clear_operations();
            let next = store.read("p", Requirement::Any).await.unwrap();
            assert_observation(&next, expected_value, true, &context);
            protocol.assert_operations(&[], &context);
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum L2RemoteCase {
        Matching,
        Stale,
        Missing,
    }

    struct L2MutationCase {
        name: &'static str,
        kind: MutationKind,
        remote: L2RemoteCase,
        completion: CompletionCase,
        result: ExpectedMutationResult,
        advance_expected: bool,
        next_value: ExpectedValue,
    }

    const L2_MUTATION_CASES: &[L2MutationCase] = &[
        L2MutationCase {
            name: "L2 create commits after persisted state became missing",
            kind: MutationKind::Create,
            remote: L2RemoteCase::Missing,
            completion: CompletionCase::Natural,
            result: ExpectedMutationResult::Committed,
            advance_expected: false,
            next_value: ExpectedValue::Proposed,
        },
        L2MutationCase {
            name: "L2 create conflict invalidates persisted state",
            kind: MutationKind::Create,
            remote: L2RemoteCase::Matching,
            completion: CompletionCase::Natural,
            result: ExpectedMutationResult::Conflict,
            advance_expected: false,
            next_value: ExpectedValue::Old,
        },
        L2MutationCase {
            name: "L2 create lost acknowledgement invalidates persisted state",
            kind: MutationKind::Create,
            remote: L2RemoteCase::Missing,
            completion: CompletionCase::UnavailableAfterApply,
            result: ExpectedMutationResult::Unavailable,
            advance_expected: false,
            next_value: ExpectedValue::Proposed,
        },
        L2MutationCase {
            name: "L2 create definitive failure preserves persisted state",
            kind: MutationKind::Create,
            remote: L2RemoteCase::Matching,
            completion: CompletionCase::DefinitiveBeforeApply,
            result: ExpectedMutationResult::Definitive,
            advance_expected: false,
            next_value: ExpectedValue::Old,
        },
        L2MutationCase {
            name: "L2 cancelled create invalidates persisted state",
            kind: MutationKind::Create,
            remote: L2RemoteCase::Missing,
            completion: CompletionCase::CancelledAfterApply,
            result: ExpectedMutationResult::Cancelled,
            advance_expected: false,
            next_value: ExpectedValue::Proposed,
        },
        L2MutationCase {
            name: "L2 CAS commits from persisted revision",
            kind: MutationKind::Cas,
            remote: L2RemoteCase::Matching,
            completion: CompletionCase::Natural,
            result: ExpectedMutationResult::Committed,
            advance_expected: true,
            next_value: ExpectedValue::Proposed,
        },
        L2MutationCase {
            name: "L2 CAS conflict invalidates persisted stale revision",
            kind: MutationKind::Cas,
            remote: L2RemoteCase::Stale,
            completion: CompletionCase::Natural,
            result: ExpectedMutationResult::Conflict,
            advance_expected: false,
            next_value: ExpectedValue::Winner,
        },
        L2MutationCase {
            name: "L2 CAS missing invalidates persisted revision",
            kind: MutationKind::Cas,
            remote: L2RemoteCase::Missing,
            completion: CompletionCase::Natural,
            result: ExpectedMutationResult::Conflict,
            advance_expected: false,
            next_value: ExpectedValue::Absent,
        },
        L2MutationCase {
            name: "L2 CAS lost acknowledgement invalidates persisted revision",
            kind: MutationKind::Cas,
            remote: L2RemoteCase::Matching,
            completion: CompletionCase::UnavailableAfterApply,
            result: ExpectedMutationResult::Unavailable,
            advance_expected: false,
            next_value: ExpectedValue::Proposed,
        },
        L2MutationCase {
            name: "L2 CAS definitive failure preserves persisted revision",
            kind: MutationKind::Cas,
            remote: L2RemoteCase::Matching,
            completion: CompletionCase::DefinitiveBeforeApply,
            result: ExpectedMutationResult::Definitive,
            advance_expected: false,
            next_value: ExpectedValue::Old,
        },
        L2MutationCase {
            name: "L2 cancelled CAS invalidates persisted revision",
            kind: MutationKind::Cas,
            remote: L2RemoteCase::Matching,
            completion: CompletionCase::CancelledAfterApply,
            result: ExpectedMutationResult::Cancelled,
            advance_expected: false,
            next_value: ExpectedValue::Proposed,
        },
        L2MutationCase {
            name: "L2 delete commits from persisted revision",
            kind: MutationKind::Delete,
            remote: L2RemoteCase::Matching,
            completion: CompletionCase::Natural,
            result: ExpectedMutationResult::Deleted,
            advance_expected: true,
            next_value: ExpectedValue::Absent,
        },
        L2MutationCase {
            name: "L2 delete conflict invalidates persisted stale revision",
            kind: MutationKind::Delete,
            remote: L2RemoteCase::Stale,
            completion: CompletionCase::Natural,
            result: ExpectedMutationResult::Precondition,
            advance_expected: false,
            next_value: ExpectedValue::Winner,
        },
        L2MutationCase {
            name: "L2 delete missing invalidates persisted revision",
            kind: MutationKind::Delete,
            remote: L2RemoteCase::Missing,
            completion: CompletionCase::Natural,
            result: ExpectedMutationResult::Deleted,
            advance_expected: false,
            next_value: ExpectedValue::Absent,
        },
        L2MutationCase {
            name: "L2 delete lost acknowledgement invalidates persisted revision",
            kind: MutationKind::Delete,
            remote: L2RemoteCase::Matching,
            completion: CompletionCase::UnavailableAfterApply,
            result: ExpectedMutationResult::Unavailable,
            advance_expected: false,
            next_value: ExpectedValue::Absent,
        },
        L2MutationCase {
            name: "L2 delete definitive failure preserves persisted revision",
            kind: MutationKind::Delete,
            remote: L2RemoteCase::Matching,
            completion: CompletionCase::DefinitiveBeforeApply,
            result: ExpectedMutationResult::Definitive,
            advance_expected: false,
            next_value: ExpectedValue::Old,
        },
        L2MutationCase {
            name: "L2 cancelled delete invalidates persisted revision",
            kind: MutationKind::Delete,
            remote: L2RemoteCase::Matching,
            completion: CompletionCase::CancelledAfterApply,
            result: ExpectedMutationResult::Cancelled,
            advance_expected: false,
            next_value: ExpectedValue::Absent,
        },
    ];

    #[tokio::test]
    async fn persistent_mutation_protocol_matrix() {
        for case in L2_MUTATION_CASES {
            let directory = TempDir::new().unwrap();
            let protocol = ProtocolBackend::new();
            let old_version = protocol
                .memory
                .write_if_not_exists("p", OLD_VALUE.to_vec())
                .await
                .unwrap();

            let (first, _) = persistent_store(&directory, protocol.backend.clone()).await;
            let first_typed: TypedCachedStore<Bytes> = first.typed();
            let persisted = first_typed.read("p", Requirement::Any).await.unwrap();
            assert_observation(&persisted, ExpectedValue::Old, false, case.name);
            drop(first_typed);
            first.shutdown().await;
            drop(first);

            protocol.clear_operations();
            let (second, timeline) = persistent_store(&directory, protocol.backend.clone()).await;
            let second_typed: TypedCachedStore<Bytes> = second.typed();
            let expected = if matches!(case.kind, MutationKind::Create) {
                None
            } else {
                let restored = second_typed.read("p", Requirement::Any).await.unwrap();
                assert_observation(&restored, ExpectedValue::Old, true, case.name);
                protocol.assert_operations(&[], case.name);
                Some(restored)
            };

            match case.remote {
                L2RemoteCase::Matching => {}
                L2RemoteCase::Stale => {
                    protocol
                        .memory
                        .write_if("p", WINNER_VALUE.to_vec(), &old_version)
                        .await
                        .unwrap();
                }
                L2RemoteCase::Missing => {
                    protocol.memory.delete_if("p", &old_version).await.unwrap();
                }
            }
            protocol.clear_operations();
            let original_evidence = expected.as_ref().map(Observation::current_after);
            let barrier = timeline.now();

            let result = match case.completion {
                CompletionCase::Natural => {
                    invoke_mutation(second_typed.clone(), case.kind, expected.clone()).await
                }
                CompletionCase::UnavailableAfterApply => {
                    protocol.hook.set_after({
                        let kind = case.kind;
                        move |operation, outcome| {
                            ready(if kind.matches(operation) && outcome.is_success() {
                                Err(BackendError::Unavailable("lost acknowledgement".into()))
                            } else {
                                Ok(())
                            })
                        }
                    });
                    invoke_mutation(second_typed.clone(), case.kind, expected.clone()).await
                }
                CompletionCase::DefinitiveBeforeApply => {
                    protocol.hook.set_before({
                        let kind = case.kind;
                        move |operation| {
                            ready(if kind.matches(operation) {
                                Err(BackendError::other("rejected before apply"))
                            } else {
                                Ok(())
                            })
                        }
                    });
                    invoke_mutation(second_typed.clone(), case.kind, expected.clone()).await
                }
                CompletionCase::CancelledAfterApply => {
                    let entered = Arc::new(Notify::new());
                    protocol.hook.set_after({
                        let entered = entered.clone();
                        let kind = case.kind;
                        move |operation, outcome| {
                            let should_cancel = kind.matches(operation) && outcome.is_success();
                            let entered = entered.clone();
                            Box::pin(async move {
                                if should_cancel {
                                    entered.notify_one();
                                    std::future::pending::<()>().await;
                                }
                                Ok(())
                            })
                        }
                    });
                    let mutation = tokio::spawn(invoke_mutation(
                        second_typed.clone(),
                        case.kind,
                        expected.clone(),
                    ));
                    entered.notified().await;
                    mutation.abort();
                    assert!(mutation.await.unwrap_err().is_cancelled(), "{}", case.name);
                    MutationResult {
                        kind: ExpectedMutationResult::Cancelled,
                        observation: None,
                    }
                }
            };

            protocol.hook.clear_before();
            protocol.hook.clear_after();
            assert_eq!(result.kind, case.result, "{}: result", case.name);
            if let (Some(expected), Some(original_evidence)) = (&expected, original_evidence) {
                assert_eq!(
                    expected.current_after() >= barrier,
                    case.advance_expected,
                    "{}: expected evidence",
                    case.name
                );
                if !case.advance_expected {
                    assert_eq!(
                        expected.current_after(),
                        original_evidence,
                        "{}: unchanged expected evidence",
                        case.name
                    );
                }
            }
            if let Some(observation) = &result.observation {
                let value = if matches!(case.kind, MutationKind::Delete) {
                    ExpectedValue::Absent
                } else {
                    ExpectedValue::Proposed
                };
                assert_observation(observation, value, false, case.name);
                assert!(
                    observation.current_after() >= barrier,
                    "{}: returned evidence",
                    case.name
                );
            }
            protocol.assert_operations(&[case.kind.operation()], case.name);

            drop(second_typed);
            second.shutdown().await;
            drop(second);

            protocol.clear_operations();
            let (third, _) = persistent_store(&directory, protocol.backend.clone()).await;
            let third_typed: TypedCachedStore<Bytes> = third.typed();
            let next = third_typed.read("p", Requirement::Any).await.unwrap();
            let preserved = matches!(case.completion, CompletionCase::DefinitiveBeforeApply);
            assert_observation(&next, case.next_value, preserved, case.name);
            let expected_operations: &[&str] = if preserved { &[] } else { &["read"] };
            protocol.assert_operations(expected_operations, case.name);
            drop(third_typed);
            third.shutdown().await;
        }
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

        store.store.knowledge.invalidate("p");
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
