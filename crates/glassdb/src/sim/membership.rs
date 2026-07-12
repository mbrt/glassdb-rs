//! Concurrent membership, split traversal, and phantom-safe listing workload.

use std::sync::{Arc, Mutex};

use arbitrary::{Arbitrary, Unstructured};
use glassdb_backend::Backend;

use crate::{Database, Error};

use super::harness::{SimWorkload, open_det_db};
use super::{MAX_CLIENTS, MAX_OPS_PER_CLIENT, assert_valid_listing, key_name, tiny_split_policy};
// ===========================================================================
// Membership workload (ADR-031 dynamic range sharding).
//
// Clients concurrently create (put) and delete keys and list the collection,
// with a tiny split policy so a handful of keys grows the B-link tree — forcing
// leaf and root splits, right-link traversal, and cross-leaf sorted listing. The
// oracle is twofold: every committed listing must be strictly sorted and drawn
// from the key universe (a structural invariant that always holds, even under
// faults and mid-split), and the final key set must match the per-key membership
// accounting (exactly with faults off; within the in-doubt bound otherwise).
//
// To keep the per-key membership accounting sound under concurrency, each client
// owns a disjoint subset of the key universe (assigned by residue), so a key's
// operations are totally ordered by its single owning client. Keys owned by
// different clients still interleave and can share a leaf, so same-leaf
// create/delete races and phantom prevention are still exercised.
// ===========================================================================

/// Size of the membership key universe. With the tiny split policy below a
/// couple of live keys already overflow a leaf, so this is comfortably enough to
/// drive multi-level splits.
const MEMBERSHIP_KEYS: usize = 8;
/// Collection the membership keys live in.
const MEMBERSHIP_COLLECTION: &[u8] = b"members";

/// A single membership operation on one key of the universe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembOp {
    /// Create or overwrite the key (making it live).
    Put(usize),
    /// Delete the key (making it not live).
    Delete(usize),
    /// List the collection's keys and assert the listing is well-formed.
    List,
}

/// A membership workload: one op sequence per client. Each client owns a
/// disjoint subset of `0..MEMBERSHIP_KEYS` (keys `k` with `k % nclients == i`),
/// so every key is mutated by a single client and its op history is totally
/// ordered. Clients run concurrently.
#[derive(Debug, Clone, Default)]
pub struct MembershipWorkload {
    /// Per-client op sequences.
    pub clients: Vec<Vec<MembOp>>,
}

impl<'a> Arbitrary<'a> for MembershipWorkload {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        // At least two clients so there is something to interleave.
        let nclients = 2 + (u.arbitrary::<u8>()? as usize % (MAX_CLIENTS - 1));
        let mut clients = Vec::with_capacity(nclients);
        for i in 0..nclients {
            // This client's disjoint slice of the key universe. Non-empty since
            // MEMBERSHIP_KEYS >= MAX_CLIENTS >= nclients.
            let my_keys: Vec<usize> = (0..MEMBERSHIP_KEYS).filter(|k| k % nclients == i).collect();
            let nops = u.arbitrary::<u8>()? as usize % (MAX_OPS_PER_CLIENT + 1);
            let mut ops = Vec::with_capacity(nops);
            for _ in 0..nops {
                let key = |u: &mut Unstructured<'a>| -> arbitrary::Result<usize> {
                    Ok(my_keys[u.arbitrary::<u8>()? as usize % my_keys.len()])
                };
                ops.push(match u.arbitrary::<u8>()? % 3 {
                    0 => MembOp::Put(key(u)?),
                    1 => MembOp::Delete(key(u)?),
                    _ => MembOp::List,
                });
            }
            clients.push(ops);
        }
        Ok(MembershipWorkload { clients })
    }
}

/// The set of liveness outcomes an unacknowledged (in-doubt) op may have left in
/// the store since the last acknowledged op on a key. Empty means no op is
/// in-doubt. Because a key's ops are totally ordered by its single owning client
/// but a crashed/failed op may or may not have reached the backend, several
/// consecutive in-doubt ops can each independently be the last one that applied,
/// so every one of their outcomes stays possible until an ack makes the value
/// definite again.
#[derive(Clone, Copy, Default)]
struct PendingOutcomes {
    live: bool,
    dead: bool,
}

impl PendingOutcomes {
    fn is_empty(self) -> bool {
        !self.live && !self.dead
    }

    /// Whether the concrete outcome `observed` (live/not-live) is one of the
    /// possible in-doubt outcomes.
    fn allows(self, observed: bool) -> bool {
        if observed { self.live } else { self.dead }
    }
}

/// Per-key membership accounting. `committed[k]` is the definite liveness from
/// acknowledged ops; `pending[k]` is the set of ambiguous outcomes left by
/// in-flight ops since the last ack (a possibility is added before each op runs,
/// and the whole set is cleared on the next ack), i.e. the in-doubt cases a fault
/// can leave behind.
pub struct MembershipAcct {
    committed: Vec<bool>,
    pending: Vec<PendingOutcomes>,
}

impl MembershipAcct {
    fn new() -> Self {
        MembershipAcct {
            committed: vec![false; MEMBERSHIP_KEYS],
            pending: vec![PendingOutcomes::default(); MEMBERSHIP_KEYS],
        }
    }

    /// Marks key `k`'s in-flight op (outcome `live`) as started but unconfirmed.
    /// If a crash or lost ack leaves it in-doubt, this outcome joins the set
    /// [`verify`](MembershipWorkload::verify) tolerates. It *accumulates* rather
    /// than replaces: a second in-doubt op on the same key keeps the earlier
    /// op's outcome possible, since either may have been the one that applied.
    fn begin(&mut self, k: usize, live: bool) {
        if live {
            self.pending[k].live = true;
        } else {
            self.pending[k].dead = true;
        }
    }

    /// Confirms key `k`'s op committed: its outcome is now definite, which
    /// resolves every earlier in-doubt op on the key, so the pending set clears.
    fn commit(&mut self, k: usize, live: bool) {
        self.committed[k] = live;
        self.pending[k] = PendingOutcomes::default();
    }
}

impl SimWorkload for MembershipWorkload {
    type Op = MembOp;
    type State = Mutex<MembershipAcct>;

    fn clients(&self) -> &[Vec<MembOp>] {
        &self.clients
    }

    fn new_state(&self) -> Mutex<MembershipAcct> {
        Mutex::new(MembershipAcct::new())
    }

    async fn open_db(backend: &Arc<dyn Backend>) -> Result<Database, Error> {
        // A tiny split soft cap so a handful of keys forces B-link splits.
        open_det_db(backend, tiny_split_policy()).await
    }

    async fn seed(&self, db: &Database) {
        // The collection starts empty; just create it.
        db.collection(MEMBERSHIP_COLLECTION)
            .create()
            .await
            .expect("create collection");
    }

    async fn run_op(
        db: &Database,
        op: &MembOp,
        state: &Mutex<MembershipAcct>,
    ) -> Result<(), Error> {
        let coll = db.collection(MEMBERSHIP_COLLECTION);
        let coll = &coll;
        match op {
            MembOp::Put(k) => {
                // Record the intended outcome before the op so a crash mid-commit
                // leaves it correctly in-doubt (the store may or may not reflect
                // it), mirroring the increment harness's started-before/acked-after.
                state.lock().unwrap().begin(*k, true);
                let kn = &key_name(*k);
                db.tx(|tx| async move { tx.write(coll, kn, b"1") }).await?;
                state.lock().unwrap().commit(*k, true);
                Ok(())
            }
            MembOp::Delete(k) => {
                state.lock().unwrap().begin(*k, false);
                let kn = &key_name(*k);
                db.tx(|tx| async move { tx.delete(coll, kn) }).await?;
                state.lock().unwrap().commit(*k, false);
                Ok(())
            }
            MembOp::List => {
                let keys: Vec<Vec<u8>> = coll.keys().await?.collect::<Result<_, _>>()?;
                assert_valid_listing(&keys, MEMBERSHIP_KEYS);
                Ok(())
            }
        }
    }

    async fn verify(&self, db: &Database, state: &Mutex<MembershipAcct>, faults_enabled: bool) {
        let coll = db.collection(MEMBERSHIP_COLLECTION);
        let keys: Vec<Vec<u8>> = coll
            .keys()
            .await
            .expect("final listing")
            .collect::<Result<_, _>>()
            .expect("final listing");
        assert_valid_listing(&keys, MEMBERSHIP_KEYS);

        let acct = state.lock().unwrap();
        for k in 0..MEMBERSHIP_KEYS {
            let observed = keys.contains(&key_name(k));
            let base = acct.committed[k];
            let pending = acct.pending[k];
            if pending.is_empty() {
                assert_eq!(
                    observed, base,
                    "key k{k}: listed={observed} but last committed op set live={base}"
                );
            } else {
                assert!(
                    faults_enabled,
                    "key k{k}: an op was left in-doubt with faults disabled"
                );
                assert!(
                    observed == base || pending.allows(observed),
                    "key k{k}: listed={observed} outside in-doubt bound \
                     (committed={base}, pending live={}, dead={})",
                    pending.live,
                    pending.dead,
                );
            }
        }
    }
}
