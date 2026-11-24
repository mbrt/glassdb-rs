//! The final statuses that one database instance knows, and what it observed
//! about them.

use glassdb_data::TxId;
use glassdb_storage::Requirement;
use glassdb_storage::transaction::TxCommitStatus;
use hashlink::LinkedHashMap;

use super::MonitorStats;

/// A durable final status. Only owner acknowledgement can change it, from
/// a wound to aborted, and each of them means that the identity cannot commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FinalStatus {
    Committed,
    Wounded,
    BlindWounded,
    Aborted,
}

/// What this database instance knows about an identity with a final status.
///
/// Two observations leave the status true but let a stale leaf observation
/// still name the identity where it held a value: the committed record was
/// reclaimed, or the wound is blind. For each, this instance keeps the first
/// requirement after the observation. A leaf observation that reaches it
/// cannot name the identity as a holder of a key that the identity wrote
/// (ADR-104). The constructors allow only these two cases to have one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct KnownFinal {
    status: FinalStatus,
    no_value_after: Option<Requirement>,
}

impl KnownFinal {
    /// Returns what a durable observation of `status` shows, or `None` for a
    /// status that is not final. A blind wound needs the requirement after its
    /// observation, so it comes from [`KnownFinal::blind_wound`].
    pub(super) fn observed(status: TxCommitStatus) -> Option<Self> {
        let status = match status {
            TxCommitStatus::Committed => FinalStatus::Committed,
            TxCommitStatus::Wounded => FinalStatus::Wounded,
            TxCommitStatus::Aborted => FinalStatus::Aborted,
            TxCommitStatus::BlindWounded | TxCommitStatus::Unknown | TxCommitStatus::Pending => {
                return None;
            }
        };
        Some(Self {
            status,
            no_value_after: None,
        })
    }

    /// A blind wound that this instance observed before `after`.
    pub(super) fn blind_wound(after: Requirement) -> Self {
        Self {
            status: FinalStatus::BlindWounded,
            no_value_after: Some(after),
        }
    }

    /// A committed identity whose record this instance found reclaimed before
    /// `after`.
    pub(super) fn reclaimed(after: Requirement) -> Self {
        Self {
            status: FinalStatus::Committed,
            no_value_after: Some(after),
        }
    }

    pub(super) fn status(self) -> TxCommitStatus {
        match self.status {
            FinalStatus::Committed => TxCommitStatus::Committed,
            FinalStatus::Wounded => TxCommitStatus::Wounded,
            FinalStatus::BlindWounded => TxCommitStatus::BlindWounded,
            FinalStatus::Aborted => TxCommitStatus::Aborted,
        }
    }

    /// The first requirement at which a leaf observation shows no value of the
    /// identity, when that is not every observation.
    pub(super) fn no_value_after(self) -> Option<Requirement> {
        self.no_value_after
    }
}

/// The final statuses that this instance knows, bounded by count, with the
/// least recently used entry evicted first.
pub(super) struct FinalStatusCache {
    capacity: usize,
    entries: LinkedHashMap<TxId, KnownFinal>,
    stats: MonitorStats,
}

impl FinalStatusCache {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: LinkedHashMap::new(),
            stats: MonitorStats::default(),
        }
    }

    /// Looks up `tid`, and counts the lookup.
    pub(super) fn get(&mut self, tid: &TxId) -> Option<KnownFinal> {
        let known = self.entries.to_back(tid).copied();
        if known.is_some() {
            self.stats.hits += 1;
        } else {
            self.stats.misses += 1;
        }
        known
    }

    /// Returns the no-value requirement of `tid`, without counting a lookup.
    pub(super) fn no_value_after(&self, tid: &TxId) -> Option<Requirement> {
        self.entries
            .get(tid)
            .and_then(|known| known.no_value_after())
    }

    pub(super) fn insert(&mut self, tid: TxId, known: KnownFinal) {
        let known = match self.entries.get(&tid) {
            // An older observation of an acknowledged wound must not hide the
            // acknowledgement.
            Some(existing)
                if existing.status == FinalStatus::Aborted
                    && matches!(
                        known.status,
                        FinalStatus::Wounded | FinalStatus::BlindWounded
                    ) =>
            {
                return;
            }
            // This instance knows that the identity committed, and a blind
            // wound of it says less.
            Some(existing)
                if existing.status == FinalStatus::Committed
                    && known.status == FinalStatus::BlindWounded =>
            {
                return;
            }
            // A later observation must not drop the requirement of an earlier
            // one: the earliest requirement is the least strict that is
            // correct.
            Some(existing)
                if existing.status == known.status && existing.no_value_after.is_some() =>
            {
                *existing
            }
            _ => known,
        };
        self.entries.insert(tid, known);
        while self.entries.len() > self.capacity {
            self.entries.pop_front();
        }
    }

    /// Notes that the committed record of `tid` was reclaimed before `now`, and
    /// returns the requirement that a leaf observation must reach to show no
    /// value of it.
    pub(super) fn note_reclaimed(&mut self, tid: TxId, now: Requirement) -> Requirement {
        self.insert(tid, KnownFinal::reclaimed(now));
        self.no_value_after(&tid).unwrap_or(now)
    }

    pub(super) fn take_stats(&mut self) -> MonitorStats {
        std::mem::take(&mut self.stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx_id(prefix: &[u8]) -> TxId {
        TxId::with_priority(0, prefix)
    }

    #[test]
    fn only_final_statuses_are_known() {
        for status in [
            TxCommitStatus::Unknown,
            TxCommitStatus::Pending,
            TxCommitStatus::BlindWounded,
        ] {
            assert_eq!(KnownFinal::observed(status), None, "{status:?}");
        }
        for status in [
            TxCommitStatus::Committed,
            TxCommitStatus::Wounded,
            TxCommitStatus::Aborted,
        ] {
            let known = KnownFinal::observed(status).unwrap();
            assert_eq!(known.status(), status);
            assert_eq!(known.no_value_after(), None);
        }
        let blind = KnownFinal::blind_wound(Requirement::ANY);
        assert_eq!(blind.status(), TxCommitStatus::BlindWounded);
        assert_eq!(blind.no_value_after(), Some(Requirement::ANY));
        let reclaimed = KnownFinal::reclaimed(Requirement::ANY);
        assert_eq!(reclaimed.status(), TxCommitStatus::Committed);
        assert_eq!(reclaimed.no_value_after(), Some(Requirement::ANY));
    }

    // Owner acknowledgement changes a wound to aborted, never the other way.
    #[test]
    fn final_status_cache_keeps_an_acknowledged_wound() {
        let mut cache = FinalStatusCache::new(2);
        let tid = tx_id(b"wounded");
        let wounded = KnownFinal::observed(TxCommitStatus::Wounded).unwrap();
        let aborted = KnownFinal::observed(TxCommitStatus::Aborted).unwrap();

        cache.insert(tid, wounded);
        cache.insert(tid, aborted);
        cache.insert(tid, wounded);
        cache.insert(tid, KnownFinal::blind_wound(Requirement::ANY));

        assert_eq!(cache.get(&tid), Some(aborted));
    }

    // The first requirement is the least strict one that is correct, so a
    // later observation does not replace it.
    #[test]
    fn final_status_cache_keeps_the_first_no_value_requirement() {
        let mut cache = FinalStatusCache::new(2);
        let timeline = glassdb_storage::Timeline::new();
        let first = Requirement::after(timeline.currentness_barrier());
        let second = Requirement::after(timeline.currentness_barrier());
        assert_ne!(first, second);
        let reclaimed = tx_id(b"reclaimed");
        let committed = KnownFinal::observed(TxCommitStatus::Committed).unwrap();

        cache.insert(reclaimed, committed);
        assert_eq!(cache.no_value_after(&reclaimed), None);
        assert_eq!(cache.note_reclaimed(reclaimed, first), first);
        assert_eq!(cache.note_reclaimed(reclaimed, second), first);
        cache.insert(reclaimed, committed);
        cache.insert(reclaimed, KnownFinal::blind_wound(second));
        assert_eq!(cache.get(&reclaimed), Some(KnownFinal::reclaimed(first)));

        let blind = tx_id(b"blind");
        cache.insert(blind, KnownFinal::blind_wound(first));
        cache.insert(blind, KnownFinal::blind_wound(second));
        assert_eq!(cache.no_value_after(&blind), Some(first));
    }

    #[test]
    fn final_status_cache_is_count_bounded_and_lru() {
        let mut cache = FinalStatusCache::new(2);
        let first = tx_id(b"first");
        let second = tx_id(b"second");
        let third = tx_id(b"third");
        let known = KnownFinal::observed(TxCommitStatus::Committed).unwrap();

        cache.insert(first, known);
        cache.insert(second, known);
        assert!(cache.get(&first).is_some());
        cache.insert(third, known);

        assert!(cache.get(&second).is_none());
        assert!(cache.get(&first).is_some());
        assert!(cache.get(&third).is_some());
    }
}
