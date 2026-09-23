//! Exact observations, revisions, and conditional-mutation evidence.
//!
//! Changes to this module require the review in
//! `docs/guides/storage-consistency.md` (E3, E4). Fields and evidence operations
//! use `pub(super)` only for the parent cached-store implementation. Do not
//! widen them to the storage crate or add transformations for typed stores.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use glassdb_backend as backend;
use glassdb_data::ObjectPath;

use super::ObjectKey;
use crate::timeline::{CurrentnessBarrier, Requirement, SequencePoint};

/// The cached store's opaque content revision, wrapping the backend revision.
///
/// Higher layers may retain, compare, and pass a revision (and, where recovery
/// requires it, serialize the underlying backend revision), but do not interpret
/// or manufacture one.
/// Keep the backend revision private. Do not add public constructors, `Default`,
/// conversions, or mutable backend access.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Revision(backend::Revision);

impl Revision {
    /// Returns the provider token for durable recovery metadata.
    pub fn serialize(&self) -> &str {
        &self.0.token
    }

    /// Retains the token of an exact backend state.
    ///
    /// The token must come from a backend result or that state's persistent
    /// cache entry. It supplies no currentness evidence by itself.
    pub(super) fn from_backend(revision: backend::Revision) -> Self {
        Self(revision)
    }

    /// Borrows the token for a conditional backend operation.
    pub(super) fn backend(&self) -> &backend::Revision {
        &self.0
    }
}

/// The outcome of a CAS (a conditional create or replace).
#[derive(Debug)]
pub enum CasResult<V> {
    /// The mutation definitively took effect; the receipt retains its precondition
    /// and installed state. This does not establish a transaction commit or
    /// promise that the installed state is still current.
    Applied(CasReceipt<V>),
    /// The precondition failed: the starting revision or cached absence was
    /// obsolete. The exact starting entry has been invalidated.
    Rejected,
}

impl<V> CasResult<V> {
    /// Reports whether the conditional mutation definitively took effect.
    pub fn is_applied(&self) -> bool {
        matches!(self, CasResult::Applied(_))
    }

    /// Returns the successful mutation's receipt, or `None` when the mutation was rejected.
    pub fn into_receipt(self) -> Option<CasReceipt<V>> {
        match self {
            CasResult::Applied(receipt) => Some(receipt),
            CasResult::Rejected => None,
        }
    }
}

/// Proof of one applied CAS.
///
/// Only the storage mutation implementation may construct a receipt, after a
/// definitive backend success. Reads, rejections, plans with no staged changes, and in-doubt
/// results must not be converted into receipts. Batch-member participation is
/// separate from this storage proof and belongs to the coordinator.
///
/// The expected revision identifies the precondition; `None` means absence for
/// a conditional create. It must not be paired with the installed body to make
/// an observation. The installed observation retains the exact state written by
/// this mutation, even if a peer replaces it before the reply arrives.
/// Its invocation point is fixed: advancing the installed observation's
/// watermark must never make the precondition qualify for a later barrier.
///
/// State evidence is available only through explicit accessors. Do not add
/// `Deref`, `AsRef`, `From`, payload mapping, or methods that expose a sequence
/// point. A receipt does not establish a currentness barrier after the mutation.
#[derive(Debug, Clone)]
pub struct CasReceipt<V> {
    pub(super) expected_revision: Option<Revision>,
    pub(super) installed: Observation<V>,
    pub(super) invoked: SequencePoint,
}

impl<V> CasReceipt<V> {
    /// Returns the exact state installed by the successful mutation.
    pub fn installed(&self) -> &Observation<V> {
        &self.installed
    }

    /// Consumes the receipt and retains only its installed-state observation.
    pub fn into_installed(self) -> Observation<V> {
        self.installed
    }

    /// Reports whether this mutation confirmed the observation's state at or
    /// after `barrier`. It does not prove continuous currentness since the read.
    pub fn confirms_expected(
        &self,
        observed: &Observation<V>,
        barrier: CurrentnessBarrier,
    ) -> bool {
        barrier.is_reached_by(self.invoked)
            && self.installed.key == observed.key
            && self.expected_revision == observed.revision
    }
}

/// The outcome of checking whether a retained observation is still current.
#[derive(Debug, Clone)]
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
/// Keep the counter private so updates cannot bypass monotonic advancement.
#[derive(Debug, Clone)]
pub(super) struct Evidence(Arc<AtomicU64>);

impl Evidence {
    pub(super) fn new(t: SequencePoint) -> Self {
        Evidence(Arc::new(AtomicU64::new(t.raw())))
    }

    pub(super) fn get(&self) -> SequencePoint {
        SequencePoint::from_raw(self.0.load(Ordering::SeqCst))
    }

    /// Reports whether both handles share currentness updates.
    pub(super) fn is_shared_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Advances the watermark to at least `t`, never regressing it.
    pub(super) fn advance(&self, t: SequencePoint) {
        self.0.fetch_max(t.raw(), Ordering::SeqCst);
    }
}

/// An exact observed state of one object, returned by a successful read or
/// mutation. It carries the decoded value (or absence), the [`Revision`], and a
/// reference to shared currentness evidence. It remains inspectable after the
/// state is evicted or invalidated as the current cache entry.
///
/// Keep the state, revision, path, and evidence together. Do not add public
/// constructors, payload transformations, sequence-point accessors, or
/// conversions to requirements or barriers. The invocation point cannot
/// prove that another operation follows the completed observation.
/// Raw evidence access is restricted to the parent `cached_store` implementation.
/// Other storage modules must use the currentness-check interface.
#[derive(Debug, Clone)]
pub struct Observation<V> {
    pub(super) key: ObjectKey,
    pub(super) value: Option<Arc<V>>,
    pub(super) revision: Option<Revision>,
    pub(super) evidence: Evidence,
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

    /// Reports whether this state has currentness evidence at or after `barrier`.
    pub fn is_current_after(&self, barrier: CurrentnessBarrier) -> bool {
        barrier.is_reached_by(self.evidence.get())
    }

    /// Reports whether the retained evidence satisfies the freshness requirement.
    pub fn satisfies(&self, requirement: Requirement) -> bool {
        requirement.is_satisfied_by(self.evidence.get())
    }

    /// The parsed physical object path this observation refers to.
    pub fn path(&self) -> &ObjectPath {
        self.key.object_path()
    }

    /// Reports whether two observations refer to the same exact state.
    ///
    /// Observations of one state normally share the same evidence cell, so
    /// pointer identity is the fast path. But a cache eviction and reload mint a
    /// fresh evidence cell for the very same committed revision, so two
    /// observations of the same path and revision are still the same state.
    pub fn same_state(&self, other: &Self) -> bool {
        if self.evidence.is_shared_with(&other.evidence) {
            return true;
        }
        match (&self.revision, &other.revision) {
            (Some(mine), Some(theirs)) => self.key == other.key && mine == theirs,
            _ => false,
        }
    }

    pub(super) fn current_after(&self) -> SequencePoint {
        self.evidence.get()
    }
}
