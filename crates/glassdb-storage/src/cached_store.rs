//! The decoded object cache with bounded freshness (ADR-036).
//!
//! One database-local cached object store sits between the [`Backend`] and every
//! typed storage abstraction. All typed stores share this single byte-bounded
//! LRU, keyed by physical object path; each supplies its own encoding, decoding,
//! and decoded-size accounting through a [`Codec`]. A path has exactly one
//! decoded type, so reading it back through a different typed store is an
//! internal error.
//!
//! Freshness is a local validation watermark, not a durable guarantee. Each
//! cache entry is `Present` (a decoded value, its [`Revision`], and a
//! validated-after [`ValidationTime`]), `Absent` (a validated-after watermark,
//! no revision), or `Missing` (no entry: a positively known-obsolete value that
//! a new lookup cannot rediscover). A successful read returns an [`Observation`]
//! that references monotonic validation evidence shared with the current cache
//! entry; the observation stays usable even after that entry is evicted or
//! invalidated, because invalidation changes what a *new* read may use but does
//! not revoke the historical fact that the observed state was current after its
//! watermark.
//!
//! Reads take a [`Requirement`]: `Any` accepts any usable cached entry and reads
//! the backend on a miss; `AtLeast(T)` accepts an entry only when its watermark
//! is at least `T`, otherwise it validates through the backend. The store
//! records `started-at` immediately before each backend call: a successful read
//! or mutation linearized at some point after `started-at`, so that is the
//! result's watermark. Watermarks never regress, and a mutation is published
//! only after backend success.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use glassdb_backend::{self as backend, Backend, BackendError};

use crate::cache::{Cache, Weighable};
use crate::error::StorageError;

/// Encoding, decoding, and decoded-size accounting for one physical object type.
///
/// Each typed store supplies its own codec; the cache holds the decoded value
/// so an object is decoded once per changed revision rather than once per hit.
pub trait Codec {
    /// The decoded, immutable value cached for this object type.
    type Value: Send + Sync + 'static;

    /// Decodes an object body into its cached value.
    fn decode(bytes: &[u8]) -> Result<Self::Value, StorageError>;

    /// Encodes a cached value back into its object body (the CAS unit).
    fn encode(value: &Self::Value) -> Result<Vec<u8>, StorageError>;

    /// Estimates the decoded value's in-memory size in bytes, governing
    /// eviction.
    fn size(value: &Self::Value) -> usize;
}

/// The cached store's opaque content-CAS token, wrapping the backend version.
///
/// Higher layers may retain, compare, and pass a revision (and, where recovery
/// requires it, serialize the underlying backend version), but do not interpret
/// or manufacture one.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Revision(backend::Version);

impl Revision {
    /// Wraps a backend version as a revision.
    pub fn from_version(v: backend::Version) -> Self {
        Revision(v)
    }

    /// Returns the underlying backend version, for a conditional backend call.
    pub fn version(&self) -> &backend::Version {
        &self.0
    }

    /// Reports whether the revision is unset (no backing version).
    pub fn is_unset(&self) -> bool {
        self.0.is_unset()
    }

    /// Consumes the revision, returning the underlying backend version.
    pub fn into_version(self) -> backend::Version {
        self.0
    }
}

/// A monotonic, opaque logical time, meaningful only within one open store.
///
/// It cannot be persisted or exchanged between databases or processes. `T0`
/// (the [`Default`]) is the genesis value, below any real operation's watermark.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ValidationTime(u64);

/// The freshness a read requires of a cached entry before serving it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Requirement {
    /// Accept any usable cached entry without backend validation; read the
    /// backend only on a miss.
    Any,
    /// Accept an entry only when its watermark is at least this time; otherwise
    /// validate through the backend.
    AtLeast(ValidationTime),
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

/// The outcome of validating a previously returned observation.
#[derive(Debug)]
pub enum Validated<V> {
    /// The observed state is still current after the required bound; its
    /// watermark has been advanced if a backend round-trip confirmed it.
    Unchanged,
    /// The state changed; here is the current observation.
    Changed(Observation<V>),
}

/// A shared, monotonically-advanceable validation watermark. Observations of one
/// state and that state's current cache entry hold clones of the same cell, so
/// validating advances the evidence every holder sees. An `Arc` held by a caller
/// outlives eviction of the corresponding cache entry.
#[derive(Debug, Clone)]
struct Evidence(Arc<AtomicU64>);

impl Evidence {
    fn new(t: ValidationTime) -> Self {
        Evidence(Arc::new(AtomicU64::new(t.0)))
    }

    fn get(&self) -> ValidationTime {
        ValidationTime(self.0.load(Ordering::SeqCst))
    }

    /// Advances the watermark to at least `t`, never regressing it.
    fn advance(&self, t: ValidationTime) {
        self.0.fetch_max(t.0, Ordering::SeqCst);
    }
}

/// An exact observed state of one object, returned by a successful read or
/// mutation. It carries the decoded value (or absence), the [`Revision`], and a
/// reference to the shared validation evidence. It remains inspectable after the
/// state is evicted or invalidated as the current cache entry.
#[derive(Debug, Clone)]
pub struct Observation<V> {
    path: Arc<str>,
    value: Option<Arc<V>>,
    revision: Option<Revision>,
    evidence: Evidence,
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

    /// The observed revision, or `None` for an absence.
    pub fn revision(&self) -> Option<&Revision> {
        self.revision.as_ref()
    }

    /// The watermark: the observed state was current at some point after this
    /// time.
    pub fn validated_after(&self) -> ValidationTime {
        self.evidence.get()
    }

    /// The object path this observation refers to.
    pub fn path(&self) -> &str {
        &self.path
    }
}

/// One entry in the shared decoded LRU: either a present decoded value or a
/// validated absence. A missing object has no entry at all.
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

impl CacheEntry {
    fn evidence(&self) -> &Evidence {
        match &self.state {
            EntryState::Present { evidence, .. } | EntryState::Absent { evidence } => evidence,
        }
    }

    fn evidence_time(&self) -> ValidationTime {
        self.evidence().get()
    }
}

impl Weighable for CacheEntry {
    fn size_b(&self) -> usize {
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
/// waiters. Cheaply cloneable so one in-flight validation can serve many.
#[derive(Clone)]
struct FetchResult {
    value: Option<Arc<dyn Any + Send + Sync>>,
    revision: Option<Revision>,
    evidence: Evidence,
}

type SharedFetch = Shared<BoxFuture<'static, Result<FetchResult, StorageError>>>;

/// One in-flight backend validation of a path, tracked for coalescing.
struct InFlight {
    started: ValidationTime,
    fut: SharedFetch,
}

/// The decoded object cache over a [`Backend`] (ADR-036). Reads and mutations of
/// every physical object class go through this boundary; listing is an uncached
/// pass-through. Cloning is cheap (shared `Arc`s), so every typed store holds its
/// own handle onto the one shared cache.
#[derive(Clone)]
pub struct CachedStore {
    backend: Arc<dyn Backend>,
    cache: Arc<Cache<CacheEntry>>,
    clock: Arc<AtomicU64>,
    // Count of object bodies transferred from the backend (a fresh `read` or a
    // conditional read that returned a changed body). A caller samples this
    // before and after a logical read to tell whether the result reused cached
    // bodies (an unchanged count, possibly after cheap conditional revalidation)
    // or had to fetch a body — the signal behind the transaction-layer
    // cache-hit stat.
    body_reads: Arc<AtomicU64>,
    inflight: Arc<Mutex<HashMap<String, InFlight>>>,
}

impl CachedStore {
    /// Creates a cached store over `backend`, sharing the single byte-bounded
    /// LRU sized by `max_size_b`.
    pub fn new(backend: Arc<dyn Backend>, max_size_b: usize) -> Self {
        CachedStore {
            backend,
            cache: Arc::new(Cache::new(max_size_b)),
            clock: Arc::new(AtomicU64::new(0)),
            body_reads: Arc::new(AtomicU64::new(0)),
            inflight: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// The running count of object bodies this store has transferred from the
    /// backend. Sampled around a logical read to detect a body-free read (the
    /// count did not move): a hit reuses cached bodies, possibly after a cheap
    /// conditional revalidation that returned "not modified".
    pub fn body_reads(&self) -> u64 {
        self.body_reads.load(Ordering::SeqCst)
    }

    /// Returns the current logical time: a bound such that any operation started
    /// from now on satisfies `AtLeast(now())`, while every already-completed
    /// operation does not. Higher layers capture this to require freshness of
    /// later validations.
    pub fn now(&self) -> ValidationTime {
        ValidationTime(self.clock.load(Ordering::SeqCst) + 1)
    }

    /// Reads the object at `path`, serving a cached entry that satisfies `req`
    /// or validating through the backend otherwise. Returns an [`Observation`],
    /// whose `value()` is `None` for an object that does not exist. A new read
    /// never returns a positively known-obsolete (`Missing`) value.
    pub async fn read<C: Codec>(
        &self,
        path: &str,
        req: Requirement,
    ) -> Result<Observation<C::Value>, StorageError> {
        let key: Arc<str> = Arc::from(path);
        if let Some(obs) = self.try_hit::<C>(&key, req)? {
            return Ok(obs);
        }
        let fetched = self.fetch::<C>(&key, req).await?;
        self.to_observation::<C>(&key, fetched)
    }

    /// Returns the cached observation for `path` without contacting the backend,
    /// or `None` when it is not cached. A committed/aborted object is immutable,
    /// so its cached copy is authoritative indefinitely; callers use this to
    /// serve terminal objects without a revalidation round-trip.
    pub fn peek<C: Codec>(
        &self,
        path: &str,
    ) -> Result<Option<Observation<C::Value>>, StorageError> {
        let key: Arc<str> = Arc::from(path);
        self.try_hit::<C>(&key, Requirement::Any)
    }

    /// Validates a previously returned observation against `req`. Succeeds
    /// locally when the observation's watermark already satisfies the bound
    /// (even if that state is no longer the current cache entry); otherwise uses
    /// the observation's revision in a conditional backend read (or an ordinary
    /// read for an absence, which has no revision).
    pub async fn validate<C: Codec>(
        &self,
        obs: &Observation<C::Value>,
        req: Requirement,
    ) -> Result<Validated<C::Value>, StorageError> {
        if satisfies(obs.evidence.get(), req) {
            return Ok(Validated::Unchanged);
        }
        let started = self.next_tick();
        match &obs.revision {
            Some(rev) => match self
                .backend
                .read_if_modified(&obs.path, rev.version())
                .await
            {
                Err(BackendError::Precondition) => {
                    // Unchanged: advance the observation's (and, if it is still
                    // current, the cache entry's) watermark without decoding.
                    obs.evidence.advance(started);
                    self.advance_present(&obs.path, rev, started);
                    Ok(Validated::Unchanged)
                }
                Ok(reply) => {
                    let f = self.publish_present::<C>(
                        &obs.path,
                        &reply.contents,
                        reply.version,
                        started,
                    )?;
                    Ok(Validated::Changed(self.to_observation::<C>(&obs.path, f)?))
                }
                Err(BackendError::NotFound) => {
                    let f = self.publish_absent(&obs.path, started);
                    Ok(Validated::Changed(self.to_observation::<C>(&obs.path, f)?))
                }
                Err(e) => Err(e.into()),
            },
            None => match self.backend.read(&obs.path).await {
                Err(BackendError::NotFound) => {
                    obs.evidence.advance(started);
                    self.install_absent(&obs.path, started);
                    Ok(Validated::Unchanged)
                }
                Ok(reply) => {
                    let f = self.publish_present::<C>(
                        &obs.path,
                        &reply.contents,
                        reply.version,
                        started,
                    )?;
                    Ok(Validated::Changed(self.to_observation::<C>(&obs.path, f)?))
                }
                Err(e) => Err(e.into()),
            },
        }
    }

    /// Unconditionally writes `value` and publishes it in the cache after the
    /// backend confirms. An in-doubt outcome invalidates the starting knowledge
    /// (it may or may not have landed) and surfaces `Unavailable`.
    pub async fn write<C: Codec>(
        &self,
        path: &str,
        value: Arc<C::Value>,
    ) -> Result<Observation<C::Value>, StorageError> {
        let bytes = C::encode(&value)?;
        let size = C::size(&value);
        let started = self.next_tick();
        match self.backend.write(path, bytes).await {
            Ok(v) => Ok(self.commit_write::<C>(path, value, size, Revision(v), started)),
            Err(BackendError::Unavailable(msg)) => {
                self.invalidate_stale(path, started);
                Err(StorageError::Unavailable(msg))
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Creates the object only if absent. On success publishes the value; on a
    /// conflict (it already exists) invalidates the cached absence and reports
    /// [`CasResult::Conflict`]; an in-doubt outcome invalidates the absence and
    /// surfaces `Unavailable`.
    pub async fn create<C: Codec>(
        &self,
        path: &str,
        value: Arc<C::Value>,
    ) -> Result<CasResult<C::Value>, StorageError> {
        let bytes = C::encode(&value)?;
        let size = C::size(&value);
        let started = self.next_tick();
        match self.backend.write_if_not_exists(path, bytes).await {
            Ok(v) => Ok(CasResult::Committed(self.commit_write::<C>(
                path,
                value,
                size,
                Revision(v),
                started,
            ))),
            Err(BackendError::Precondition) => {
                self.invalidate_absent(path);
                Ok(CasResult::Conflict)
            }
            Err(BackendError::Unavailable(msg)) => {
                self.invalidate_absent(path);
                Err(StorageError::Unavailable(msg))
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Compare-and-swaps the object from `expected` to `value`. On success the
    /// expected observation is proven to have remained current right up to the
    /// swap, so its watermark is advanced, and the new value is published; a
    /// conflict or an in-doubt outcome invalidates the exact starting revision
    /// only if it is still the current entry.
    pub async fn cas<C: Codec>(
        &self,
        path: &str,
        value: Arc<C::Value>,
        expected: &Revision,
    ) -> Result<CasResult<C::Value>, StorageError> {
        let bytes = C::encode(&value)?;
        let size = C::size(&value);
        let started = self.next_tick();
        match self.backend.write_if(path, bytes, expected.version()).await {
            Ok(v) => {
                // Capture the expected state's shared evidence before publishing
                // the new value; a successful CAS proves that state stayed
                // current up to the swap, so advance it (and every retained
                // observation of it) afterward, once the install can no longer
                // mistake it for newer knowledge.
                let expected_ev = self.evidence_if_present(path, expected);
                let obs = self.commit_write::<C>(path, value, size, Revision(v), started);
                if let Some(ev) = expected_ev {
                    ev.advance(started);
                }
                Ok(CasResult::Committed(obs))
            }
            // A CAS whose object vanished is a lost race, like a precondition
            // miss: the starting revision is obsolete.
            Err(BackendError::Precondition | BackendError::NotFound) => {
                self.invalidate_present(path, expected);
                Ok(CasResult::Conflict)
            }
            Err(BackendError::Unavailable(msg)) => {
                self.invalidate_present(path, expected);
                Err(StorageError::Unavailable(msg))
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Deletes the object and installs freshly validated absence. A missing
    /// object is treated as a successful delete; an in-doubt outcome invalidates
    /// the starting knowledge and surfaces `Unavailable`.
    pub async fn delete(&self, path: &str) -> Result<(), StorageError> {
        let started = self.next_tick();
        match self.backend.delete(path).await {
            Ok(()) | Err(BackendError::NotFound) => {
                self.install_absent(path, started);
                Ok(())
            }
            Err(BackendError::Unavailable(msg)) => {
                self.invalidate_stale(path, started);
                Err(StorageError::Unavailable(msg))
            }
            Err(e) => Err(e.into()),
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
        Ok(self.backend.list(prefix, cursor, limit).await?)
    }

    /// Allocates a unique `started-at` watermark, ordered before the backend
    /// call it precedes.
    fn next_tick(&self) -> ValidationTime {
        ValidationTime(self.clock.fetch_add(1, Ordering::SeqCst) + 1)
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
                Ok(Some(Observation {
                    path: path.clone(),
                    value: Some(value),
                    revision: Some(revision),
                    evidence,
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
                }))
            }
        }
    }

    /// Fetches from the backend, coalescing with an in-flight validation of the
    /// same path when that validation's start satisfies `req`.
    async fn fetch<C: Codec>(
        &self,
        path: &Arc<str>,
        req: Requirement,
    ) -> Result<FetchResult, StorageError> {
        let joined = {
            let map = self.inflight.lock().unwrap();
            map.get(path.as_ref())
                .filter(|f| satisfies(f.started, req))
                .map(|f| f.fut.clone())
        };
        if let Some(fut) = joined
            && let Ok(r) = fut.await
        {
            // A coalesced success serves us; a failure falls through to our own
            // attempt below.
            return Ok(r);
        }

        let started = self.next_tick();
        let expected = match self.cache.get(path.as_ref()).map(|e| e.state) {
            Some(EntryState::Present { revision, .. }) => Some(revision),
            _ => None,
        };
        let this = self.clone();
        let key = path.clone();
        let fut = async move { this.do_fetch::<C>(&key, started, expected).await }
            .boxed()
            .shared();
        {
            let mut map = self.inflight.lock().unwrap();
            map.insert(
                path.to_string(),
                InFlight {
                    started,
                    fut: fut.clone(),
                },
            );
        }
        let res = fut.await;
        {
            let mut map = self.inflight.lock().unwrap();
            if map.get(path.as_ref()).map(|f| f.started) == Some(started) {
                map.remove(path.as_ref());
            }
        }
        res
    }

    /// Runs one backend read for a path: a version-conditional revalidation when
    /// a present revision is known, else an ordinary read.
    async fn do_fetch<C: Codec>(
        &self,
        path: &str,
        started: ValidationTime,
        expected: Option<Revision>,
    ) -> Result<FetchResult, StorageError> {
        match &expected {
            Some(rev) => match self.backend.read_if_modified(path, rev.version()).await {
                Ok(reply) => {
                    self.publish_present::<C>(path, &reply.contents, reply.version, started)
                }
                Err(BackendError::Precondition) => {
                    self.publish_unchanged::<C>(path, rev.clone(), started)
                        .await
                }
                Err(BackendError::NotFound) => Ok(self.publish_absent(path, started)),
                Err(e) => Err(e.into()),
            },
            None => match self.backend.read(path).await {
                Ok(reply) => {
                    self.publish_present::<C>(path, &reply.contents, reply.version, started)
                }
                Err(BackendError::NotFound) => Ok(self.publish_absent(path, started)),
                Err(e) => Err(e.into()),
            },
        }
    }

    /// Decodes and publishes a freshly read body as a present entry.
    fn publish_present<C: Codec>(
        &self,
        path: &str,
        bytes: &[u8],
        version: backend::Version,
        started: ValidationTime,
    ) -> Result<FetchResult, StorageError> {
        self.body_reads.fetch_add(1, Ordering::SeqCst);
        let decoded = C::decode(bytes)?;
        let size = C::size(&decoded);
        let value: Arc<dyn Any + Send + Sync> = Arc::new(decoded);
        let revision = Revision(version);
        let evidence = self.install_present(path, value.clone(), size, revision.clone(), started);
        Ok(FetchResult {
            value: Some(value),
            revision: Some(revision),
            evidence,
        })
    }

    /// Handles a "not modified" response: reuses the cached body and advances
    /// its watermark, or full-reads if the entry was evicted meanwhile.
    async fn publish_unchanged<C: Codec>(
        &self,
        path: &str,
        revision: Revision,
        started: ValidationTime,
    ) -> Result<FetchResult, StorageError> {
        if let Some(entry) = self.cache.get(path)
            && let EntryState::Present {
                value,
                revision: r,
                evidence,
                ..
            } = entry.state
            && r == revision
        {
            evidence.advance(started);
            return Ok(FetchResult {
                value: Some(value),
                revision: Some(revision),
                evidence,
            });
        }
        // The cached body is gone (evicted) or changed locally, so recover it.
        match self.backend.read(path).await {
            Ok(reply) => self.publish_present::<C>(path, &reply.contents, reply.version, started),
            Err(BackendError::NotFound) => Ok(self.publish_absent(path, started)),
            Err(e) => Err(e.into()),
        }
    }

    /// Publishes a validated absence.
    fn publish_absent(&self, path: &str, started: ValidationTime) -> FetchResult {
        let evidence = self.install_absent(path, started);
        FetchResult {
            value: None,
            revision: None,
            evidence,
        }
    }

    /// Publishes a mutation's submitted value as the current present entry.
    fn commit_write<C: Codec>(
        &self,
        path: &str,
        value: Arc<C::Value>,
        size: usize,
        revision: Revision,
        started: ValidationTime,
    ) -> Observation<C::Value> {
        let erased: Arc<dyn Any + Send + Sync> = value.clone();
        let evidence = self.install_present(path, erased, size, revision.clone(), started);
        Observation {
            path: Arc::from(path),
            value: Some(value),
            revision: Some(revision),
            evidence,
        }
    }

    /// Installs a present entry under the non-regression rule: an entry already
    /// validated at least as recently is kept (the caller still gets an
    /// observation of what it read); a same-revision entry only advances its
    /// watermark; otherwise the new value is installed. Returns the evidence the
    /// caller's observation should reference.
    fn install_present(
        &self,
        path: &str,
        value: Arc<dyn Any + Send + Sync>,
        size: usize,
        revision: Revision,
        started: ValidationTime,
    ) -> Evidence {
        let mut out: Option<Evidence> = None;
        self.cache.update(path, |old| {
            match old {
                Some(CacheEntry {
                    state:
                        EntryState::Present {
                            value: old_value,
                            size: old_size,
                            revision: r,
                            evidence,
                        },
                }) if r == revision => {
                    evidence.advance(started);
                    out = Some(evidence.clone());
                    Some(CacheEntry {
                        state: EntryState::Present {
                            value: old_value,
                            size: old_size,
                            revision: r,
                            evidence,
                        },
                    })
                }
                Some(entry) if entry.evidence_time() >= started => {
                    // Newer knowledge is already cached: keep it and hand the
                    // caller a detached watermark for the state it observed.
                    out = Some(Evidence::new(started));
                    Some(entry)
                }
                _ => {
                    let ev = Evidence::new(started);
                    out = Some(ev.clone());
                    Some(CacheEntry {
                        state: EntryState::Present {
                            value,
                            size,
                            revision,
                            evidence: ev,
                        },
                    })
                }
            }
        });
        out.expect("update closure always sets the evidence")
    }

    /// Installs a validated absence under the same non-regression rule.
    fn install_absent(&self, path: &str, started: ValidationTime) -> Evidence {
        let mut out: Option<Evidence> = None;
        self.cache.update(path, |old| match old {
            Some(CacheEntry {
                state: EntryState::Absent { evidence },
            }) => {
                evidence.advance(started);
                out = Some(evidence.clone());
                Some(CacheEntry {
                    state: EntryState::Absent { evidence },
                })
            }
            Some(entry) if entry.evidence_time() >= started => {
                out = Some(Evidence::new(started));
                Some(entry)
            }
            _ => {
                let ev = Evidence::new(started);
                out = Some(ev.clone());
                Some(CacheEntry {
                    state: EntryState::Absent { evidence: ev },
                })
            }
        });
        out.expect("update closure always sets the evidence")
    }

    /// Returns a clone of the current entry's shared evidence iff it is still
    /// present at `expected`, without advancing it.
    fn evidence_if_present(&self, path: &str, expected: &Revision) -> Option<Evidence> {
        match self.cache.get(path).map(|e| e.state) {
            Some(EntryState::Present {
                revision, evidence, ..
            }) if &revision == expected => Some(evidence),
            _ => None,
        }
    }

    /// Advances the current entry's watermark iff it is still present at
    /// `expected`, proving that exact state remained current up to `started`.
    fn advance_present(&self, path: &str, expected: &Revision, started: ValidationTime) {
        self.cache.update(path, |old| {
            if let Some(CacheEntry {
                state:
                    EntryState::Present {
                        revision, evidence, ..
                    },
            }) = &old
                && revision == expected
            {
                evidence.advance(started);
            }
            old
        });
    }

    /// Invalidates the exact starting present entry (to `Missing`) only if it is
    /// still current at `expected`; never discards a different value or a later
    /// validation.
    fn invalidate_present(&self, path: &str, expected: &Revision) {
        self.cache.update(path, |old| match &old {
            Some(CacheEntry {
                state: EntryState::Present { revision, .. },
            }) if revision == expected => None,
            _ => old,
        });
    }

    /// Invalidates a cached absence (to `Missing`), leaving any concurrently
    /// installed present value untouched.
    fn invalidate_absent(&self, path: &str) {
        self.cache.update(path, |old| match &old {
            Some(CacheEntry {
                state: EntryState::Absent { .. },
            }) => None,
            _ => old,
        });
    }

    /// Invalidates the current entry (to `Missing`) unless a newer operation has
    /// already advanced it past `started`; used for in-doubt unconditional
    /// writes and deletes, which have no exact starting revision.
    fn invalidate_stale(&self, path: &str, started: ValidationTime) {
        self.cache.update(path, |old| match &old {
            Some(entry) if entry.evidence_time() < started => None,
            _ => old,
        });
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
        })
    }
}

/// Reports whether an entry validated at `evidence` satisfies `req`.
fn satisfies(evidence: ValidationTime, req: Requirement) -> bool {
    match req {
        Requirement::Any => true,
        Requirement::AtLeast(t) => evidence >= t,
    }
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
            "cached object at {path} has a different decoded type"
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

    use super::*;

    // A trivial identity codec so the concurrency layer can be exercised in
    // isolation from any real object type.
    struct Bytes;

    impl Codec for Bytes {
        type Value = Vec<u8>;
        fn decode(bytes: &[u8]) -> Result<Vec<u8>, StorageError> {
            Ok(bytes.to_vec())
        }
        fn encode(value: &Vec<u8>) -> Result<Vec<u8>, StorageError> {
            Ok(value.clone())
        }
        fn size(value: &Vec<u8>) -> usize {
            value.len()
        }
    }

    // A second codec over a different decoded type, to prove a path used through
    // the wrong typed store is an internal error.
    struct Ints;

    impl Codec for Ints {
        type Value = u64;
        fn decode(bytes: &[u8]) -> Result<u64, StorageError> {
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
    }

    fn v(bytes: &[u8]) -> Arc<Vec<u8>> {
        Arc::new(bytes.to_vec())
    }

    fn ready(result: Result<(), BackendError>) -> HookFuture {
        Box::pin(async move { result })
    }

    // A store over a recording memory backend, plus the op log for counting
    // backend traffic.
    fn store_rec() -> (CachedStore, OpLog) {
        let rec = Arc::new(RecordingBackend::new(Arc::new(MemoryBackend::new())));
        let log = rec.log();
        let backend: Arc<dyn Backend> = rec;
        (CachedStore::new(backend, 1 << 20), log)
    }

    fn count(log: &OpLog, op: &str) -> usize {
        log.lock().unwrap().iter().filter(|r| r.op == op).count()
    }

    fn clear(log: &OpLog) {
        log.lock().unwrap().clear();
    }

    // Model invariant: an `Any` hit is served from cache with no backend op,
    // while `AtLeast(now())` on an older entry revalidates and advances (never
    // regresses) its watermark.
    #[tokio::test]
    async fn any_hit_is_local_and_at_least_revalidates_and_advances() {
        let (s, log) = store_rec();
        s.write::<Bytes>("p", v(b"a")).await.unwrap();

        let o1 = s.read::<Bytes>("p", Requirement::Any).await.unwrap();
        assert_eq!(o1.value().unwrap().as_slice(), b"a");
        assert_eq!(count(&log, "read"), 0);
        assert_eq!(count(&log, "read_if_modified"), 0);

        let t = s.now();
        let o2 = s.read::<Bytes>("p", Requirement::AtLeast(t)).await.unwrap();
        assert_eq!(
            count(&log, "read_if_modified"),
            1,
            "stale entry revalidates"
        );
        assert!(o2.validated_after() >= t, "watermark advanced to the bound");
        assert!(
            o2.validated_after() >= o1.validated_after(),
            "never regresses"
        );
    }

    // Model invariant: `AtLeast(T)` accepts an entry whose watermark already
    // reaches `T` with no backend op.
    #[tokio::test]
    async fn at_least_served_locally_when_watermark_sufficient() {
        let (s, log) = store_rec();
        s.write::<Bytes>("p", v(b"a")).await.unwrap();
        let o = s
            .read::<Bytes>("p", Requirement::AtLeast(s.now()))
            .await
            .unwrap();
        let w = o.validated_after();
        clear(&log);

        let o2 = s.read::<Bytes>("p", Requirement::AtLeast(w)).await.unwrap();
        assert_eq!(count(&log, "read"), 0);
        assert_eq!(count(&log, "read_if_modified"), 0);
        assert!(o2.validated_after() >= w);
    }

    // Model invariant: `Any` never returns an entry a conflict invalidated. A
    // stale CAS turns the exact starting entry into `Missing`, so the next `Any`
    // re-reads the backend and observes the winner.
    #[tokio::test]
    async fn any_rereads_after_conflict_invalidates_starting_entry() {
        let mem = Arc::new(MemoryBackend::new());
        let rec = Arc::new(RecordingBackend::new(mem));
        let log = rec.log();
        let backend: Arc<dyn Backend> = rec;
        let s1 = CachedStore::new(backend.clone(), 1 << 20);
        let s2 = CachedStore::new(backend, 1 << 20);

        let obs = s1
            .create::<Bytes>("p", v(b"a"))
            .await
            .unwrap()
            .into_observation()
            .unwrap();
        let rev = obs.revision().unwrap().clone();
        // A peer overwrites the object; s1's cache is unaware.
        s2.write::<Bytes>("p", v(b"b")).await.unwrap();

        let r = s1.cas::<Bytes>("p", v(b"c"), &rev).await.unwrap();
        assert!(!r.committed(), "the stale CAS conflicts");
        clear(&log);

        let got = s1.read::<Bytes>("p", Requirement::Any).await.unwrap();
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

    // Model invariant: an observation stays usable after its current entry is
    // invalidated. Its established watermark satisfies an older bound locally,
    // while a new read cannot rediscover the obsolete value and a stricter bound
    // validates through the backend.
    #[tokio::test]
    async fn observation_outlives_invalidation() {
        let mem = Arc::new(MemoryBackend::new());
        let rec = Arc::new(RecordingBackend::new(mem));
        let log = rec.log();
        let backend: Arc<dyn Backend> = rec;
        let s1 = CachedStore::new(backend.clone(), 1 << 20);
        let s2 = CachedStore::new(backend, 1 << 20);

        let obs = s1
            .create::<Bytes>("p", v(b"a"))
            .await
            .unwrap()
            .into_observation()
            .unwrap();
        let w = obs.validated_after();
        let rev = obs.revision().unwrap().clone();

        s2.write::<Bytes>("p", v(b"b")).await.unwrap();
        s1.cas::<Bytes>("p", v(b"c"), &rev).await.unwrap(); // conflict -> Missing

        assert_eq!(obs.value().unwrap().as_slice(), b"a", "still inspectable");

        clear(&log);
        assert!(matches!(
            s1.validate::<Bytes>(&obs, Requirement::AtLeast(w))
                .await
                .unwrap(),
            Validated::Unchanged
        ));
        assert_eq!(count(&log, "read"), 0, "older bound needs no backend op");
        assert_eq!(count(&log, "read_if_modified"), 0);

        // A stricter bound re-validates and observes the winner.
        let t = s1.now();
        match s1
            .validate::<Bytes>(&obs, Requirement::AtLeast(t))
            .await
            .unwrap()
        {
            Validated::Changed(cur) => assert_eq!(cur.value().unwrap().as_slice(), b"b"),
            Validated::Unchanged => panic!("a stricter bound must revalidate"),
        }

        // A brand-new read cannot rediscover the obsolete value.
        let got = s1.read::<Bytes>("p", Requirement::Any).await.unwrap();
        assert_eq!(got.value().unwrap().as_slice(), b"b");
    }

    // Model invariant: a successful CAS advances both the expected observation's
    // shared evidence and installs the new value from its start time.
    #[tokio::test]
    async fn successful_cas_advances_expected_and_installs() {
        let (s, _log) = store_rec();
        let obs = s
            .create::<Bytes>("p", v(b"a"))
            .await
            .unwrap()
            .into_observation()
            .unwrap();
        let rev = obs.revision().unwrap().clone();

        let before = s.now();
        let nb = s
            .cas::<Bytes>("p", v(b"b"), &rev)
            .await
            .unwrap()
            .into_observation()
            .unwrap();
        assert!(
            obs.validated_after() >= before,
            "expected observation advanced past the CAS start"
        );
        assert!(nb.validated_after() >= before);
        assert_eq!(nb.value().unwrap().as_slice(), b"b");

        let got = s.read::<Bytes>("p", Requirement::Any).await.unwrap();
        assert_eq!(got.value().unwrap().as_slice(), b"b");
    }

    // Model invariant: a CAS conflict neither advances the expected observation
    // nor installs the proposed value.
    #[tokio::test]
    async fn cas_conflict_advances_nothing() {
        let mem = Arc::new(MemoryBackend::new());
        let backend: Arc<dyn Backend> = Arc::new(mem);
        let s1 = CachedStore::new(backend.clone(), 1 << 20);
        let s2 = CachedStore::new(backend, 1 << 20);

        let obs = s1
            .create::<Bytes>("p", v(b"a"))
            .await
            .unwrap()
            .into_observation()
            .unwrap();
        let rev = obs.revision().unwrap().clone();
        s2.write::<Bytes>("p", v(b"b")).await.unwrap();

        let before = s1.now();
        let r = s1.cas::<Bytes>("p", v(b"c"), &rev).await.unwrap();
        assert!(!r.committed());
        assert!(
            obs.validated_after() < before,
            "conflict must not advance the observation"
        );
        let got = s1.read::<Bytes>("p", Requirement::Any).await.unwrap();
        assert_eq!(
            got.value().unwrap().as_slice(),
            b"b",
            "proposed value not installed"
        );
    }

    // Model invariant: an in-doubt CAS invalidates only its exact starting entry
    // and does not advance the observation's watermark. The underlying write may
    // still have landed, which a later `Any` read discovers.
    #[tokio::test]
    async fn cas_in_doubt_invalidates_only_starting_entry() {
        let hook = HookBackend::new(Arc::new(MemoryBackend::new()));
        let backend: Arc<dyn Backend> = hook.clone();
        let s = CachedStore::new(backend, 1 << 20);

        let obs = s
            .create::<Bytes>("p", v(b"a"))
            .await
            .unwrap()
            .into_observation()
            .unwrap();
        let rev = obs.revision().unwrap().clone();
        let before = s.now();

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
        let err = s.cas::<Bytes>("p", v(b"b"), &rev).await.unwrap_err();
        assert!(matches!(err, StorageError::Unavailable(_)));
        hook.clear_after();

        assert!(
            obs.validated_after() < before,
            "an in-doubt outcome must not advance the observation"
        );
        // The starting entry became Missing, so Any re-reads and finds the write
        // that actually landed.
        let got = s.read::<Bytes>("p", Requirement::Any).await.unwrap();
        assert_eq!(got.value().unwrap().as_slice(), b"b");
    }

    // Model invariant: a failed (never-landed) mutation publishes nothing.
    #[tokio::test]
    async fn failed_mutation_is_not_published() {
        let hook = HookBackend::new(Arc::new(MemoryBackend::new()));
        let backend: Arc<dyn Backend> = hook.clone();
        let s = CachedStore::new(backend, 1 << 20);

        // Cache a validated absence first.
        assert!(
            !s.read::<Bytes>("p", Requirement::Any)
                .await
                .unwrap()
                .exists()
        );

        hook.set_before(|op| {
            ready(if matches!(op, BackendOp::WriteIfNotExists { .. }) {
                Err(BackendError::other("boom"))
            } else {
                Ok(())
            })
        });
        assert!(matches!(
            s.create::<Bytes>("p", v(b"x")).await,
            Err(StorageError::Other { .. })
        ));
        hook.clear_before();

        let got = s.read::<Bytes>("p", Requirement::Any).await.unwrap();
        assert!(!got.exists(), "a failed create must not publish its value");
    }

    // Model invariant: repeated conditional revalidations advance but never
    // regress the watermark.
    #[tokio::test]
    async fn unchanged_conditional_reads_only_advance() {
        let (s, log) = store_rec();
        s.write::<Bytes>("p", v(b"a")).await.unwrap();

        let t1 = s.now();
        let w1 = s
            .read::<Bytes>("p", Requirement::AtLeast(t1))
            .await
            .unwrap()
            .validated_after();
        assert!(w1 >= t1);
        let t2 = s.now();
        let w2 = s
            .read::<Bytes>("p", Requirement::AtLeast(t2))
            .await
            .unwrap()
            .validated_after();
        assert!(w2 >= w1, "watermark never regresses");
        assert_eq!(count(&log, "read_if_modified"), 2);
    }

    // Model invariant: negative caching. An absence is cached and re-served
    // without a backend read; a create replaces it; a delete installs a fresh
    // validated absence.
    #[tokio::test]
    async fn absence_is_cached_and_transitions() {
        let (s, log) = store_rec();
        assert!(
            !s.read::<Bytes>("m", Requirement::Any)
                .await
                .unwrap()
                .exists()
        );
        assert_eq!(count(&log, "read"), 1);
        clear(&log);
        assert!(
            !s.read::<Bytes>("m", Requirement::Any)
                .await
                .unwrap()
                .exists()
        );
        assert_eq!(count(&log, "read"), 0, "absence is cached");

        assert!(s.create::<Bytes>("m", v(b"x")).await.unwrap().committed());
        let got = s.read::<Bytes>("m", Requirement::Any).await.unwrap();
        assert_eq!(got.value().unwrap().as_slice(), b"x");

        s.delete("m").await.unwrap();
        clear(&log);
        assert!(
            !s.read::<Bytes>("m", Requirement::Any)
                .await
                .unwrap()
                .exists()
        );
        assert_eq!(count(&log, "read"), 0, "delete leaves cached absence");
    }

    // A path used through a mismatched typed store is an internal error.
    #[tokio::test]
    async fn wrong_decoded_type_is_internal_error() {
        let (s, _log) = store_rec();
        s.write::<Bytes>("p", v(b"abcd")).await.unwrap();
        assert!(matches!(
            s.read::<Ints>("p", Requirement::Any).await,
            Err(StorageError::Other { .. })
        ));
    }

    // Gated race: a delayed old read completing after a newer write cannot
    // overwrite the newer entry, and it is stamped with its own earlier start.
    #[tokio::test]
    async fn delayed_read_cannot_overwrite_newer_write() {
        let hook = HookBackend::new(Arc::new(MemoryBackend::new()));
        let backend: Arc<dyn Backend> = hook.clone();
        // Seed the backend through a separate store so the store under test
        // starts with a cold cache and must read the backend.
        let seeder = CachedStore::new(backend.clone(), 1 << 20);
        seeder.write::<Bytes>("p", v(b"a")).await.unwrap();
        let s = CachedStore::new(backend, 1 << 20);

        let entered = Arc::new(AtomicBool::new(false));
        let released = Arc::new(AtomicBool::new(false));
        // Gate *after* the backend read returns: the read has already captured
        // the old body ("a") and is parked before publishing it.
        hook.set_after({
            let entered = entered.clone();
            let released = released.clone();
            move |op, _| {
                if !matches!(op, BackendOp::Read { .. }) {
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

        let read = tokio::spawn({
            let s = s.clone();
            async move { s.read::<Bytes>("p", Requirement::Any).await }
        });
        while !entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        // A newer write lands while the read is parked before publishing.
        let wb = s.write::<Bytes>("p", v(b"b")).await.unwrap();
        released.store(true, Ordering::SeqCst);

        let o = read.await.unwrap().unwrap();
        assert_eq!(
            o.value().unwrap().as_slice(),
            b"a",
            "the read observed the old body"
        );
        assert!(
            o.validated_after() < wb.validated_after(),
            "the delayed read is stamped with its own earlier start"
        );
        let got = s.read::<Bytes>("p", Requirement::Any).await.unwrap();
        assert_eq!(
            got.value().unwrap().as_slice(),
            b"b",
            "the newer write is not overwritten by the delayed read"
        );
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
        let seeder = CachedStore::new(backend.clone(), 1 << 20);
        seeder.write::<Bytes>("p", v(b"a")).await.unwrap();
        let s = CachedStore::new(backend, 1 << 20);
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
            async move { s.read::<Bytes>("p", Requirement::Any).await }
        });
        while entered.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        let r2 = tokio::spawn({
            let s = s.clone();
            async move { s.read::<Bytes>("p", Requirement::Any).await }
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

    // Gated race: a stricter waiter whose bound is not satisfied by the in-flight
    // validation's start does not coalesce; it issues its own validation.
    #[tokio::test]
    async fn stricter_waiter_does_not_coalesce() {
        let mem = Arc::new(MemoryBackend::new());
        let rec = Arc::new(RecordingBackend::new(mem));
        let log = rec.log();
        let inner: Arc<dyn Backend> = rec;
        let hook = HookBackend::new(inner);
        let backend: Arc<dyn Backend> = hook.clone();
        let s = CachedStore::new(backend, 1 << 20);
        // Seed a present-but-stale entry.
        s.write::<Bytes>("p", v(b"a")).await.unwrap();
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

        // Reader A revalidates at AtLeast(now()); its op start is tA.
        let a = tokio::spawn({
            let s = s.clone();
            let t = s.now();
            async move { s.read::<Bytes>("p", Requirement::AtLeast(t)).await }
        });
        while entered.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
        // A stricter bound than A's start: it cannot join A's in-flight op.
        let strict = s.now();
        let b = tokio::spawn({
            let s = s.clone();
            async move { s.read::<Bytes>("p", Requirement::AtLeast(strict)).await }
        });
        while entered.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
        released.store(true, Ordering::SeqCst);
        a.await.unwrap().unwrap();
        b.await.unwrap().unwrap();
        assert_eq!(
            count(&log, "read_if_modified"),
            2,
            "the stricter waiter issued its own validation"
        );
    }
}
