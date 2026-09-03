//! Logical key and range resolution over the B-link tree.
//!
//! Routing and scan composition live here; transaction-dependent interpretation
//! of loaded nodes and entries belongs to
//! [`KeyStateResolver`](crate::key_state_resolver::KeyStateResolver).

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;

use glassdb_concurr::map_all_bounded;
use glassdb_data::{CollectionAddress, LogicalKey, TxId};
use glassdb_storage::{LeafEntry, Requirement, RoutedLeaf, StorageError, TreeRouter};

use crate::access::{LeafCoverage, ScanAccess, ScanEvidence, ScanMutation, ScanRange};
use crate::error::{TransError, trans_to_storage};
use crate::key_state_resolver::{KeyStateResolver, WriterResolution};
use crate::monitor::KeyCommitStatus;

/// The result of a phantom-safe scan: the live keys in key order, the covered
/// leaves' membership dependencies, and the effective page frontier.
#[derive(Debug, Clone)]
pub struct ScanResult {
    evidence: ScanEvidence,
}

/// The effective writer and routed-leaf generation for one point access.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EffectivePointAccessState {
    pub(crate) writer: Option<TxId>,
    pub(crate) membership_version: u64,
}

impl ScanResult {
    /// Returns the live keys surfaced by the scan.
    pub fn keys(&self) -> &[Vec<u8>] {
        self.evidence.keys()
    }

    /// Converts this result into the access record required for validation.
    pub fn into_access(
        self,
        collection: CollectionAddress,
        range: ScanRange,
        overlay: Vec<ScanMutation>,
    ) -> ScanAccess {
        ScanAccess::new(collection, range, overlay, self.evidence)
    }

    pub(crate) fn new(evidence: ScanEvidence) -> Self {
        Self { evidence }
    }
}

/// Resolves logical keys and ranges through the B-link tree.
#[derive(Clone)]
pub(crate) struct KeyResolver {
    router: TreeRouter,
    state: KeyStateResolver,
    parallelism: NonZeroUsize,
}

impl KeyResolver {
    /// Creates key resolution over a tree router and loaded-state resolver.
    pub(crate) fn new(
        router: TreeRouter,
        state: KeyStateResolver,
        parallelism: NonZeroUsize,
    ) -> Self {
        Self {
            router,
            state,
            parallelism,
        }
    }

    /// Resolves one bounded, forward page and its membership dependencies.
    /// `cap` is an optional inclusive validation frontier that prevents a
    /// limited-page recheck from reading beyond the range already protected.
    pub(crate) async fn scan_keys(
        &self,
        collection: &CollectionAddress,
        range: &ScanRange,
        overlay: &[ScanMutation],
        own_lock_holder: Option<&TxId>,
        cap: Option<&[u8]>,
    ) -> Result<ScanResult, StorageError> {
        // A transaction scan retains every covered leaf and validates those
        // observations after its validation barrier. Requiring "now" here
        // would only duplicate work; stale execution is safe and retryable.
        self.scan_keys_at(
            collection,
            range,
            overlay,
            own_lock_holder,
            cap,
            Requirement::Any,
        )
        .await
    }

    /// Returns the committed value a resolved writer recorded for `key`.
    pub(crate) async fn committed_value(
        &self,
        key: &LogicalKey,
        writer: &TxId,
    ) -> Result<KeyCommitStatus, TransError> {
        self.state.committed_value(key, writer).await
    }

    /// Resolves a page and all dependent transaction states against one shared
    /// freshness requirement.
    pub(crate) async fn scan_keys_at(
        &self,
        collection: &CollectionAddress,
        range: &ScanRange,
        overlay: &[ScanMutation],
        own_lock_holder: Option<&TxId>,
        cap: Option<&[u8]>,
        requirement: Requirement,
    ) -> Result<ScanResult, StorageError> {
        let Some(mut loc) = self
            .router
            .first_leaf_at(collection, &range.start, requirement)
            .await
            .map_err(|error| error.classify_collection_absence(collection))?
        else {
            return Err(StorageError::NotFound.classify_collection_absence(collection));
        };

        if range.is_empty() {
            return Ok(ScanResult::new(ScanEvidence::new(
                Vec::new(),
                Vec::new(),
                Some(range.start.clone()),
            )));
        }

        let mut overlay: BTreeMap<Vec<u8>, bool> = overlay
            .iter()
            .filter(|mutation| Self::in_scan_window(range, &mutation.key, cap))
            .map(|mutation| (mutation.key.clone(), mutation.present))
            .collect();
        let mut keys = Vec::new();
        let mut covered = Vec::new();

        loop {
            let coverage = self
                .leaf_coverage(&loc, own_lock_holder, requirement)
                .await?;
            let node = loc
                .node()
                .ok_or_else(|| StorageError::other("existing leaf has no decoded node"))?;
            let leaf = node
                .as_leaf()
                .ok_or_else(|| StorageError::other("leaf scan reached a non-leaf node"))?;
            let mut candidates: BTreeSet<Vec<u8>> = leaf
                .entries()
                .filter(|entry| Self::in_scan_window(range, &entry.key, cap))
                .map(|entry| entry.key.clone())
                .collect();
            let overlay_keys: Vec<Vec<u8>> = overlay
                .keys()
                .take_while(|key| node.covers(key))
                .cloned()
                .collect();
            let leaf_overlay: BTreeMap<Vec<u8>, bool> = overlay_keys
                .into_iter()
                .map(|key| {
                    let present = overlay
                        .remove(&key)
                        .expect("overlay key was selected from the map");
                    (key, present)
                })
                .collect();
            candidates.extend(leaf_overlay.keys().cloned());

            for key in candidates {
                let present = match leaf_overlay.get(key.as_slice()) {
                    Some(present) => *present,
                    None => {
                        let logical_key = LogicalKey::new(collection.clone(), &key);
                        match leaf.lookup(&key) {
                            None => false,
                            Some(entry) => self
                                .state
                                .resolve_effective(
                                    &logical_key,
                                    Some(entry),
                                    own_lock_holder,
                                    requirement,
                                )
                                .await
                                .map_err(trans_to_storage)?
                                .exists(),
                        }
                    }
                };
                if !present {
                    continue;
                }
                keys.push(key);
                if range.limit.is_some_and(|limit| keys.len() == limit) {
                    covered.push(coverage);
                    let frontier = keys.last().cloned();
                    return Ok(ScanResult::new(ScanEvidence::new(keys, covered, frontier)));
                }
            }
            covered.push(coverage);

            let target = cap.or(range.end.as_deref());
            if target.is_some_and(|target| node.covers(target)) {
                break;
            }
            let Some(next) = self
                .router
                .next_leaf(collection, &loc, requirement)
                .await
                .map_err(|error| error.classify_collection_absence(collection))?
            else {
                break;
            };
            loc = next;
        }

        let frontier = cap.map(<[u8]>::to_vec).or_else(|| range.end.clone());
        Ok(ScanResult::new(ScanEvidence::new(keys, covered, frontier)))
    }

    /// Loads only a scan's physical validation dependencies, without resolving
    /// the leaf entries themselves.
    pub(crate) async fn scan_coverage(
        &self,
        collection: &CollectionAddress,
        range: &ScanRange,
        frontier: Option<&[u8]>,
        own_lock_holder: Option<&TxId>,
        requirement: Requirement,
    ) -> Result<Vec<LeafCoverage>, StorageError> {
        if range.is_empty() {
            if self
                .router
                .first_leaf_at(collection, &range.start, requirement)
                .await
                .map_err(|error| error.classify_collection_absence(collection))?
                .is_none()
            {
                return Err(StorageError::NotFound.classify_collection_absence(collection));
            }
            return Ok(Vec::new());
        }

        let leaves = self
            .router
            .leaves_through(collection, &range.start, frontier, requirement)
            .await
            .map_err(|error| error.classify_collection_absence(collection))?;
        let mut covered = Vec::with_capacity(leaves.len());
        for leaf in leaves {
            covered.push(
                self.leaf_coverage(&leaf, own_lock_holder, requirement)
                    .await?,
            );
        }
        Ok(covered)
    }

    async fn leaf_coverage(
        &self,
        loc: &RoutedLeaf,
        own_lock_holder: Option<&TxId>,
        requirement: Requirement,
    ) -> Result<LeafCoverage, StorageError> {
        let node = loc.node();
        if let Some(node) = node {
            self.state.ensure_collection_live(node).await?;
        }
        let pending_membership = match node {
            Some(node) => {
                self.state
                    .pending_membership(node, own_lock_holder, requirement)
                    .await?
            }
            None => Vec::new(),
        };
        Ok(LeafCoverage {
            path: loc.path.to_string().into(),
            membership_version: node.map_or(0, |node| node.membership_version()),
            pending_membership,
            observation: loc.observation.clone(),
        })
    }

    /// Resolves effective writers and routed-leaf generations against one
    /// shared freshness requirement.
    pub(crate) async fn effective_point_states(
        &self,
        keys: &[LogicalKey],
        own_lock_holder: Option<&TxId>,
        requirement: Requirement,
    ) -> Result<Vec<EffectivePointAccessState>, StorageError> {
        let items = keys
            .iter()
            .cloned()
            .enumerate()
            .map(|(ordinal, key)| (key.clone(), (ordinal, key)))
            .collect::<Vec<_>>();
        let groups = self
            .router
            .route_keys_with_requirements(items, Requirement::Any, requirement)
            .await?;

        let group_results = map_all_bounded(groups, self.parallelism, |group| async move {
            let first_ordinal = group
                .keys
                .first()
                .map(|(_, (ordinal, _))| *ordinal)
                .expect("a routed leaf group has at least one key");
            let Some(node) = group.node() else {
                return vec![(
                    first_ordinal,
                    Err(StorageError::other("routed leaf has no decoded node")),
                )];
            };
            if let Err(error) = self.state.ensure_collection_live(node).await {
                return vec![(first_ordinal, Err(error))];
            }
            let membership_version = node.membership_version();
            let leaf = match node.as_leaf() {
                Some(leaf) => leaf,
                None => {
                    return vec![(
                        first_ordinal,
                        Err(StorageError::other(
                            "descent grouped keys under a non-leaf node",
                        )),
                    )];
                }
            };

            let mut results = Vec::with_capacity(group.keys.len());
            for (raw_key, (ordinal, key)) in &group.keys {
                let resolved = self
                    .state
                    .resolve_effective(key, leaf.lookup(raw_key), own_lock_holder, requirement)
                    .await
                    .map(|resolved| resolved.into_writer())
                    .map_err(trans_to_storage);
                match resolved {
                    Ok(resolved) => results.push((
                        *ordinal,
                        Ok(EffectivePointAccessState {
                            writer: resolved.writer,
                            membership_version,
                        }),
                    )),
                    Err(error) => {
                        results.push((*ordinal, Err(error)));
                        break;
                    }
                }
            }
            results
        })
        .await;

        let mut states = std::iter::repeat_with(|| None)
            .take(keys.len())
            .collect::<Vec<_>>();
        let mut errors = Vec::new();
        for (ordinal, result) in group_results.into_iter().flatten() {
            match result {
                Ok(state) => states[ordinal] = Some(state),
                Err(error) => errors.push((ordinal, error)),
            }
        }
        if let Some((_, error)) = errors.into_iter().min_by_key(|(ordinal, _)| *ordinal) {
            return Err(error);
        }
        Ok(states
            .into_iter()
            .map(|state| state.expect("every point key has a validation state"))
            .collect())
    }

    /// Resolves `key` to its routed leaf and effective writer.
    /// An absent key resolves to no writer.
    ///
    /// `requirement` is forwarded to the descent: same-leaf direct commit passes
    /// [`Requirement::Any`] so its eligibility check reuses a leaf already
    /// cached by the transaction, without a revalidation round-trip; a stale
    /// copy is caught by the publication's version-conditional CAS (ADR-030).
    pub(crate) async fn resolve_key(
        &self,
        key: &LogicalKey,
        requirement: Requirement,
    ) -> Result<(WriterResolution, RoutedLeaf), TransError> {
        let loc = self.locate_key(key, requirement).await?;
        let writer = self
            .state
            .resolve_effective(key, Self::entry_at(&loc, key.key())?, None, requirement)
            .await?
            .into_writer();
        Ok((writer, loc))
    }

    async fn locate_key(
        &self,
        key: &LogicalKey,
        requirement: Requirement,
    ) -> Result<RoutedLeaf, TransError> {
        // Interior index nodes are served from cache (ADR-031 hot-path
        // invariant); only the terminal leaf honors the caller's `requirement`
        // (the fast path's `Any` reuse, else a current lower bound), so the root `_r`
        // is not revalidated on every commit.
        let loc = self
            .router
            .route_key_with_requirements(key.collection(), key.key(), Requirement::Any, requirement)
            .await
            .map_err(|error| error.classify_collection_absence(key.collection()))?;
        if let Some(node) = loc.node() {
            self.state
                .ensure_collection_live(node)
                .await
                .map_err(TransError::from)?;
        }
        Ok(loc)
    }

    fn entry_at<'a>(
        loc: &'a RoutedLeaf,
        raw_key: &[u8],
    ) -> Result<Option<&'a LeafEntry>, TransError> {
        let leaf = loc
            .node()
            .map(|node| {
                node.as_leaf()
                    .ok_or_else(|| TransError::other("descent resolved a non-leaf node"))
            })
            .transpose()?;
        Ok(leaf.and_then(|leaf| leaf.lookup(raw_key)))
    }

    fn in_scan_window(range: &ScanRange, key: &[u8], cap: Option<&[u8]>) -> bool {
        range.contains(key) && cap.is_none_or(|cap| key <= cap)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{OpLog, RecordingBackend};
    use glassdb_concurr::{Background, RetryConfig};
    use glassdb_data::{CollectionId, DbRoot, ObjectPath};
    use glassdb_storage::transaction::{TLogger, TxCommitStatus};
    use glassdb_storage::{
        CachedStore, CurrentState, LeafBody, LeafEntry, Node, NodeStore, Timeline, TreeRouter,
    };

    use crate::monitor::Monitor;
    use crate::reader::Reader;

    const DB: &str = "db";
    fn collection() -> CollectionAddress {
        CollectionAddress::root(DB)
    }

    fn root_path() -> ObjectPath {
        ObjectPath::TreeRoot {
            collection: collection(),
        }
    }

    fn logical_key(key: &[u8]) -> LogicalKey {
        LogicalKey::new(collection(), key)
    }

    fn missing_collection() -> CollectionAddress {
        CollectionAddress::new(DB, CollectionId::from_slice(&[1; 16]).unwrap())
    }

    // A resolver over `backend` with its own fresh cache, so it starts cold,
    // paired with the monitor backing it (a clone, sharing its caches) so a test
    // can commit holder values the resolver then help-forwards. The returned
    // `Background` must be kept alive for the monitor's lifetime.
    async fn resolver_over(
        backend: Arc<dyn Backend>,
    ) -> (KeyResolver, Monitor, Timeline, Arc<Background>) {
        let timeline = Timeline::new();
        let objects = CachedStore::new(backend, 1 << 20, timeline.clone(), None);
        let tl = TLogger::new(objects.clone(), DbRoot::try_from(DB).unwrap());
        let bg = Arc::new(Background::new());
        let mon = Monitor::with_config(
            tl,
            timeline.clone(),
            Arc::downgrade(&bg),
            RetryConfig::default(),
            crate::monitor::ProtocolTiming::default(),
        );
        let nodes = NodeStore::new(objects, std::num::NonZeroUsize::MIN);
        nodes
            .create_root(&collection(), &Node::leaf(LeafBody::new()))
            .await
            .unwrap();
        let state = KeyStateResolver::new(mon.clone());
        (
            KeyResolver::new(
                TreeRouter::new(nodes.clone(), std::num::NonZeroUsize::MIN),
                state,
                std::num::NonZeroUsize::MIN,
            ),
            mon,
            timeline,
            bg,
        )
    }

    struct TestStore {
        nodes: NodeStore,
        timeline: Timeline,
    }

    impl std::ops::Deref for TestStore {
        type Target = NodeStore;

        fn deref(&self) -> &Self::Target {
            &self.nodes
        }
    }

    async fn store_over(backend: Arc<dyn Backend>) -> TestStore {
        let timeline = Timeline::new();
        let nodes = NodeStore::new(
            CachedStore::new(backend, 1 << 20, timeline.clone(), None),
            std::num::NonZeroUsize::MIN,
        );
        nodes
            .create_root(&collection(), &Node::leaf(LeafBody::new()))
            .await
            .unwrap();
        TestStore { nodes, timeline }
    }

    async fn effective_writer(resolver: &KeyResolver, key: &LogicalKey) -> Option<TxId> {
        resolver
            .resolve_key(key, Requirement::Any)
            .await
            .unwrap()
            .0
            .writer
    }

    // Installs a committed pointer for `key` directly in the collection's leaf
    // `_r` (no lock holders), so the entry resolves to `writer` regardless of
    // whether that writer recorded a live value or tombstone.
    async fn seed_writer(store: &TestStore, key: &[u8], writer: &TxId, deleted: bool) {
        let path = root_path();
        let loaded = store
            .load_leaf(&path, Requirement::AtLeast(store.timeline.now()))
            .await
            .unwrap();
        let mut entries: BTreeMap<Vec<u8>, LeafEntry> = loaded
            .entries()
            .entries()
            .cloned()
            .map(|e| (e.key.clone(), e))
            .collect();
        let current = if deleted {
            CurrentState::Tombstone {
                writer: writer.clone(),
            }
        } else {
            CurrentState::External {
                writer: writer.clone(),
            }
        };
        entries.insert(key.to_vec(), LeafEntry::new(key).with_current(current));
        let new_leaf = LeafBody::from_entries(entries.into_values());
        let mut edit = loaded.into_edit();
        edit.set_entries(new_leaf);
        assert!(store.commit_leaf(edit).await.unwrap());
    }

    // Installs an inline committed value for `key` directly in the leaf (no lock
    // holders): the ADR-051 state a reader must serve without any other object.
    async fn seed_inline(store: &TestStore, key: &[u8], writer: &TxId, value: &[u8]) {
        seed_entry(
            store,
            key,
            LeafEntry::new(key).with_current(CurrentState::Inline {
                writer: writer.clone(),
                value: Arc::from(value),
            }),
        )
        .await;
    }

    // Adds `holder` as the entry's exclusive lock holder, keeping whatever
    // current value the entry already records.
    async fn seed_hold(store: &TestStore, key: &[u8], holder: &TxId) {
        let existing = store
            .load_leaf(&root_path(), Requirement::AtLeast(store.timeline.now()))
            .await
            .unwrap();
        let mut entry = existing.entries().lookup(key).cloned().unwrap();
        entry.replace_write_lock(holder.clone());
        seed_entry(store, key, entry).await;
    }

    // Replaces `key`'s entry in the collection's leaf `_r` with `entry`.
    async fn seed_entry(store: &TestStore, key: &[u8], entry: LeafEntry) {
        let path = root_path();
        let loaded = store
            .load_leaf(&path, Requirement::AtLeast(store.timeline.now()))
            .await
            .unwrap();
        let mut entries: BTreeMap<Vec<u8>, LeafEntry> = loaded
            .entries()
            .entries()
            .cloned()
            .map(|e| (e.key.clone(), e))
            .collect();
        entries.insert(key.to_vec(), entry);
        let new_leaf = LeafBody::from_entries(entries.into_values());
        let mut edit = loaded.into_edit();
        edit.set_entries(new_leaf);
        assert!(store.commit_leaf(edit).await.unwrap());
    }

    // Commits `writer`'s value for `key` through the monitor (a tombstone when
    // `deleted`), so a later help-forward of that holder observes it.
    async fn commit_value(mon: &Monitor, key: &[u8], writer: &TxId, deleted: bool) {
        use glassdb_storage::transaction::{TxLog, TxWrite};
        mon.begin_tx(writer);
        let mut tl = TxLog::new(writer.clone(), TxCommitStatus::Ok);
        tl.writes = vec![TxWrite {
            key: logical_key(key),
            value: Arc::from(b"v".as_slice()),
            deleted,
            prev_writer: TxId::default(),
        }];
        mon.commit_tx(tl).await.unwrap();
    }

    // Installs a write-locked entry for `key` whose only holder is `holder` and
    // whose `current_writer` pointer is not yet published — the help-forward
    // case: the effective writer must be discovered from the committed holder,
    // not the (stale, empty) pointer.
    async fn seed_locked(store: &TestStore, key: &[u8], holder: &TxId) {
        let path = root_path();
        let loaded = store
            .load_leaf(&path, Requirement::AtLeast(store.timeline.now()))
            .await
            .unwrap();
        let mut entries: BTreeMap<Vec<u8>, LeafEntry> = loaded
            .entries()
            .entries()
            .cloned()
            .map(|e| (e.key.clone(), e))
            .collect();
        let mut entry = LeafEntry::new(key);
        entry.replace_write_lock(holder.clone());
        entries.insert(key.to_vec(), entry);
        let new_leaf = LeafBody::from_entries(entries.into_values());
        let mut edit = loaded.into_edit();
        edit.set_entries(new_leaf);
        assert!(store.commit_leaf(edit).await.unwrap());
    }

    fn count_tx_reads(log: &OpLog) -> usize {
        log.lock()
            .unwrap()
            .iter()
            .filter(|r| (r.op == "read" || r.op == "read_if_modified") && r.path.contains("/_t/"))
            .count()
    }

    fn count_leaf_reads(log: &OpLog) -> usize {
        log.lock()
            .unwrap()
            .iter()
            .filter(|r| {
                (r.op == "read" || r.op == "read_if_modified")
                    && (r.path.contains("/_n/") || r.path.ends_with("/_r"))
            })
            .count()
    }

    #[tokio::test]
    async fn missing_bound_collection_is_classified_during_routing() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (resolver, _monitor, timeline, _background) = resolver_over(backend).await;
        let collection = missing_collection();
        let key = LogicalKey::new(collection.clone(), b"k");
        let range = ScanRange::all();

        assert!(matches!(
            resolver.resolve_key(&key, Requirement::Any).await,
            Err(TransError::Storage(StorageError::StaleCollection))
        ));
        assert!(matches!(
            resolver
                .scan_keys(&collection, &range, &[], None, None)
                .await,
            Err(StorageError::StaleCollection)
        ));
        assert!(matches!(
            resolver
                .scan_coverage(&collection, &range, None, None, Requirement::Any)
                .await,
            Err(StorageError::StaleCollection)
        ));
        assert!(matches!(
            resolver
                .effective_point_states(&[key], None, Requirement::AtLeast(timeline.now()),)
                .await,
            Err(StorageError::StaleCollection)
        ));
    }

    // With split deferred every key lives in the collection's single leaf `_r`
    // (ADR-031), so a batch of keys resolves against that one leaf: a live
    // pointer, a tombstone, and an absent key each resolve to the right writer.
    #[tokio::test]
    async fn effective_point_states_resolve_against_the_single_leaf() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());

        // Seed through a separate cache so the resolver-under-test starts cold.
        let seed_store = store_over(backend.clone()).await;
        let a = b"apple".to_vec();
        let b = b"mango".to_vec();
        let c = b"lonely".to_vec();
        let live = TxId::with_priority(1, b"live");
        let tomb = TxId::with_priority(2, b"tomb");

        seed_writer(&seed_store, &a, &live, false).await;
        seed_writer(&seed_store, &b, &tomb, true).await;
        // `c` is deliberately left absent.

        let (resolver, _mon, timeline, _bg) = resolver_over(backend.clone()).await;

        let pa = logical_key(&a);
        let pb = logical_key(&b);
        let pc = logical_key(&c);
        let out = resolver
            .effective_point_states(
                &[pa.clone(), pb.clone(), pc.clone()],
                None,
                Requirement::AtLeast(timeline.now()),
            )
            .await
            .unwrap();

        assert_eq!(out[0].writer, Some(live));
        assert_eq!(out[1].writer, Some(tomb), "a tombstone still has a writer");
        assert_eq!(out[2].writer, None, "an absent key resolves to no writer");
        assert!(out.iter().all(|state| state.membership_version == 0));
    }

    #[tokio::test]
    async fn effective_point_states_treats_own_exclusive_hold_as_protection() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let seed_store = store_over(backend.clone()).await;
        let key = b"held";
        let predecessor = TxId::with_priority(1, b"predecessor");
        let holder = TxId::with_priority(2, b"holder");
        seed_writer(&seed_store, key, &predecessor, false).await;
        seed_hold(&seed_store, key, &holder).await;

        let (resolver, monitor, timeline, _background) = resolver_over(backend).await;
        commit_value(&monitor, key, &holder, false).await;
        let key = logical_key(key);
        let requirement = Requirement::AtLeast(timeline.now());

        let foreign = resolver
            .effective_point_states(std::slice::from_ref(&key), None, requirement)
            .await
            .unwrap();
        assert_eq!(foreign[0].writer, Some(holder.clone()));

        let own = resolver
            .effective_point_states(std::slice::from_ref(&key), Some(&holder), requirement)
            .await
            .unwrap();
        assert_eq!(own[0].writer, Some(predecessor));
    }

    // `resolve_key` with `Any` reuses a leaf already in the resolver's
    // cache without any backend read, while a current bound revalidates it with one
    // conditional read (ADR-030). This lets a same-leaf direct candidate reuse
    // the leaf the transaction body cached, adding no leaf load at commit.
    #[tokio::test]
    async fn resolve_key_any_reuses_cached_leaf() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);

        // Seed through a separate cache so the resolver-under-test starts cold.
        let seed_store = store_over(backend.clone()).await;
        let key = b"rmw-key";
        let writer = TxId::with_priority(1, b"w");
        seed_writer(&seed_store, key, &writer, false).await;

        let (resolver, _mon, timeline, _bg) = resolver_over(backend.clone()).await;
        let key_path = logical_key(key);

        // Warm the resolver's own cache with one cold load.
        resolver
            .resolve_key(&key_path, Requirement::Any)
            .await
            .unwrap();
        log.lock().unwrap().clear();

        // `Any` serves the cached leaf: no backend read at all.
        let (resolved, _) = resolver
            .resolve_key(&key_path, Requirement::Any)
            .await
            .unwrap();
        assert_eq!(resolved.writer, Some(writer.clone()), "still resolves");
        assert_eq!(
            count_leaf_reads(&log),
            0,
            "Any reuses the cached leaf without a backend read"
        );

        // A current bound revalidates the cached leaf with one conditional read.
        log.lock().unwrap().clear();
        resolver
            .resolve_key(&key_path, Requirement::AtLeast(timeline.now()))
            .await
            .unwrap();
        assert_eq!(
            count_leaf_reads(&log),
            1,
            "a current bound revalidates the cached leaf"
        );
    }

    // The singular resolve mirrors the batched one for one key: live and
    // tombstone pointers yield their writer, while an absent key yields none.
    #[tokio::test]
    async fn effective_writer_resolves_single_key() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let seed_store = store_over(backend.clone()).await;
        let live = TxId::with_priority(1, b"live");
        let dead = TxId::with_priority(2, b"dead");
        seed_writer(&seed_store, b"live-key", &live, false).await;
        seed_writer(&seed_store, b"dead-key", &dead, true).await;

        let (resolver, _mon, _timeline, _bg) = resolver_over(backend).await;
        assert_eq!(
            effective_writer(&resolver, &logical_key(b"live-key")).await,
            Some(live)
        );
        assert_eq!(
            effective_writer(&resolver, &logical_key(b"dead-key")).await,
            Some(dead)
        );
        assert_eq!(
            effective_writer(&resolver, &logical_key(b"missing")).await,
            None
        );
    }

    // A committed exclusive holder that has not yet published its `current_writer`
    // pointer is help-forwarded: writer identity is resolved independently of
    // whether the committed value is live or a tombstone.
    #[tokio::test]
    async fn effective_writer_help_forwards_committed_holder() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let seed_store = store_over(backend.clone()).await;
        let (resolver, mon, _timeline, _bg) = resolver_over(backend).await;

        let live = TxId::with_priority(1, b"live");
        commit_value(&mon, b"live-key", &live, false).await;
        seed_locked(&seed_store, b"live-key", &live).await;

        let tomb = TxId::with_priority(2, b"tomb");
        commit_value(&mon, b"dead-key", &tomb, true).await;
        seed_locked(&seed_store, b"dead-key", &tomb).await;

        assert_eq!(
            effective_writer(&resolver, &logical_key(b"live-key")).await,
            Some(live),
            "a committed exclusive holder is help-forwarded as the writer"
        );
        assert_eq!(
            effective_writer(&resolver, &logical_key(b"dead-key")).await,
            Some(tomb),
            "a help-forwarded tombstone still resolves its writer"
        );
    }

    // ADR-051: an inline value in the leaf is the writer's own authoritative
    // evidence, so the read serves it without opening a transaction object.
    #[tokio::test]
    async fn inline_value_reads_without_a_transaction_object() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);

        let seed_store = store_over(backend.clone()).await;
        let writer = TxId::with_priority(1, b"inline");
        seed_inline(&seed_store, b"k", &writer, b"hello").await;

        let (resolver, _mon, timeline, _bg) = resolver_over(backend).await;
        let reader = Reader::new(resolver, timeline, RetryConfig::default());
        log.lock().unwrap().clear();

        let out = reader
            .read(&logical_key(b"k"), Duration::MAX)
            .await
            .unwrap();
        let value = out.value.expect("inline value is present");
        assert_eq!(value.value.as_ref(), b"hello");
        assert_eq!(value.version.writer, writer);
        assert_eq!(
            count_tx_reads(&log),
            0,
            "an inline value needs no transaction object"
        );
    }

    // A tombstone is equally authoritative: absence is decided from the leaf.
    #[tokio::test]
    async fn tombstone_reads_absent_without_a_transaction_object() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);

        let seed_store = store_over(backend.clone()).await;
        let writer = TxId::with_priority(1, b"tomb");
        seed_writer(&seed_store, b"k", &writer, true).await;

        let (resolver, _mon, timeline, _bg) = resolver_over(backend).await;
        let reader = Reader::new(resolver, timeline, RetryConfig::default());
        log.lock().unwrap().clear();

        let out = reader
            .read(&logical_key(b"k"), Duration::MAX)
            .await
            .unwrap();
        assert!(out.value.is_none());
        let (_, _, evidence) = out.into_parts();
        assert!(evidence.validates(Some(&writer), 0));
        assert_eq!(
            count_tx_reads(&log),
            0,
            "a tombstone needs no transaction object"
        );
    }

    // A committed exclusive holder is ahead of the recorded inline predecessor,
    // so the predecessor's bytes must never be served as the holder's value.
    #[tokio::test]
    async fn help_forwarded_holder_over_an_inline_predecessor_serves_its_own_value() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let seed_store = store_over(backend.clone()).await;
        let (resolver, mon, timeline, _bg) = resolver_over(backend).await;

        let old = TxId::with_priority(1, b"old");
        seed_inline(&seed_store, b"k", &old, b"old-value").await;
        let new = TxId::with_priority(2, b"new");
        commit_value(&mon, b"k", &new, false).await;
        seed_hold(&seed_store, b"k", &new).await;

        let reader = Reader::new(resolver, timeline, RetryConfig::default());
        let out = reader
            .read(&logical_key(b"k"), Duration::MAX)
            .await
            .unwrap();
        let value = out.value.expect("the holder committed a live value");
        // `commit_value` writes b"v"; the stale inline b"old-value" must not win.
        assert_eq!(value.value.as_ref(), b"v");
        assert_eq!(value.version.writer, new);
    }

    // Read validation is a writer-identity comparison, not a value comparison:
    // two versions holding identical bytes are still distinct versions.
    #[tokio::test]
    async fn equal_inline_bytes_from_different_writers_are_distinct_versions() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let seed_store = store_over(backend.clone()).await;
        let (resolver, _mon, timeline, _bg) = resolver_over(backend).await;
        let reader = Reader::new(resolver, timeline, RetryConfig::default());

        let first = TxId::with_priority(1, b"first");
        seed_inline(&seed_store, b"k", &first, b"same").await;
        let before = reader
            .read(&logical_key(b"k"), Duration::MAX)
            .await
            .unwrap();

        let second = TxId::with_priority(2, b"second");
        seed_inline(&seed_store, b"k", &second, b"same").await;
        let after = reader
            .read(&logical_key(b"k"), Duration::from_secs(0))
            .await
            .unwrap();

        assert_eq!(before.value.as_ref().unwrap().value.as_ref(), b"same");
        assert_eq!(after.value.as_ref().unwrap().value.as_ref(), b"same");
        assert_ne!(
            before.value.unwrap().version,
            after.value.unwrap().version,
            "equal bytes under different writers are different versions"
        );
    }
}
