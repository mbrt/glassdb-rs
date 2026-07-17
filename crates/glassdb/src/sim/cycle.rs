//! Non-commuting ring workload and concurrent snapshot isolation oracle.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arbitrary::{Arbitrary, Unstructured};
use glassdb_backend::Backend;
use glassdb_concurr::rt;

use crate::{Collection, Database, Error};

use super::harness::SimWorkload;
use super::{MAX_CLIENTS, MAX_OPS_PER_CLIENT, key_name, read_int, write_int};
// ===========================================================================
// Cycle workload (ported from FoundationDB's `Cycle.cpp`).
//
// Keys form a ring `key(i) -> (i + 1) % N`; each transaction reads three
// consecutive next-pointers and rotates their edges, which preserves a single
// N-cycle. Because the rotation does not commute, any isolation or atomicity
// break splits, shrinks, or grows the ring — a true serializability oracle that
// the commutative RMW-increment workload cannot provide. The ring stays
// valid whether a swap commits or aborts, so the invariant holds as-is under the
// crash/outage/lost-ack nemeses, with no relaxation.
//
// Alongside the swaps, an optional read-only observer snapshots the whole ring
// in one transaction (reading all N pointers concurrently) and asserts that
// snapshot is itself a valid ring. That adds a read-side serializability oracle
// — a committed read-only tx must observe a single committed state — and is the
// only workload that exercises `Transaction`'s concurrent-read path.
// ===========================================================================

/// Smallest ring size that guarantees the four nodes a swap touches (the start
/// node and its next three along the ring) are distinct, so the rotation
/// preserves a single cycle.
const MIN_NODES: usize = 4;
/// Largest ring the workload generates (keeps state small for the executor).
const MAX_NODES: usize = 12;
/// Most snapshots the concurrent read-only observer takes during a run (keeps
/// its work bounded at `MAX_SNAPSHOTS * node_count` reads).
const MAX_SNAPSHOTS: usize = MAX_OPS_PER_CLIENT;
/// Collection the ring lives in.
const CYCLE_COLLECTION: &[u8] = b"cycle";

/// A Cycle workload: `node_count` keys arranged in a ring, plus one swap
/// sequence per client. Each `usize` is a swap's start node, already reduced to
/// `0..node_count`. Clients run concurrently.
#[derive(Debug, Clone)]
pub struct CycleWorkload {
    /// Number of nodes in the ring (>= [`MIN_NODES`]).
    pub node_count: usize,
    /// Per-client swap start nodes.
    pub clients: Vec<Vec<usize>>,
    /// How many times a concurrent read-only observer snapshots the whole ring
    /// while the swaps run (`0` disables it). Each snapshot reads all `N`
    /// next-pointers concurrently in one transaction and must observe a valid
    /// ring — a read-side serializability oracle (see [`read_ring_snapshot`]).
    pub snapshot_reads: usize,
}

impl Default for CycleWorkload {
    fn default() -> Self {
        CycleWorkload {
            node_count: MIN_NODES,
            clients: Vec::new(),
            snapshot_reads: 0,
        }
    }
}

impl<'a> Arbitrary<'a> for CycleWorkload {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let node_count = MIN_NODES + (u.arbitrary::<u8>()? as usize % (MAX_NODES - MIN_NODES + 1));
        // At least two clients so there is something to interleave.
        let nclients = 2 + (u.arbitrary::<u8>()? as usize % (MAX_CLIENTS - 1));
        let mut clients = Vec::with_capacity(nclients);
        for _ in 0..nclients {
            let nswaps = u.arbitrary::<u8>()? as usize % (MAX_OPS_PER_CLIENT + 1);
            let mut swaps = Vec::with_capacity(nswaps);
            for _ in 0..nswaps {
                swaps.push(u.arbitrary::<u8>()? as usize % node_count);
            }
            clients.push(swaps);
        }
        let snapshot_reads = u.arbitrary::<u8>()? as usize % (MAX_SNAPSHOTS + 1);
        Ok(CycleWorkload {
            node_count,
            clients,
            snapshot_reads,
        })
    }
}

/// Reads node `idx`'s next-pointer within `tx`.
async fn read_next(tx: &crate::Transaction, coll: &Collection, idx: usize) -> Result<usize, Error> {
    let v = tx
        .read(coll, &key_name(idx))
        .await?
        .ok_or(Error::NotFound)?;
    Ok(read_int(&v) as usize)
}

/// Sets node `idx`'s next-pointer to `next` within `tx`.
fn write_next(
    tx: &crate::Transaction,
    coll: &Collection,
    idx: usize,
    next: usize,
) -> Result<(), Error> {
    tx.write(coll, &key_name(idx), &write_int(next as i64))
}

/// Asserts the stored next-pointers still form a single ring over every node in
/// `0..N`: walking from node 0 visits each node exactly once and returns to 0
/// after exactly `N` hops. A serializability or atomicity failure splits the
/// ring (a revisit / early return to 0), drops a node, or dangles a pointer out
/// of range.
fn assert_ring(next: &[usize]) {
    let n = next.len();
    let mut seen = vec![false; n];
    let mut cur = 0usize;
    for hop in 0..n {
        assert!(
            cur < n,
            "ring pointer {cur} out of range (n={n}) at hop {hop}"
        );
        assert!(
            !seen[cur],
            "ring revisits node {cur} at hop {hop}: cycle shorter than {n}"
        );
        seen[cur] = true;
        cur = next[cur];
    }
    assert_eq!(
        cur, 0,
        "ring does not return to node 0 after {n} hops (broken or longer cycle)"
    );
}

/// One ring edge-rotation transaction starting at node `r` (mirrors FDB's
/// Cycle). Reads three consecutive next-pointers, then rotates the edges. For a
/// single N-cycle with N >= 4 the four nodes are distinct, so the three writes
/// target distinct keys and map the ring to another single N-cycle.
async fn cycle_swap(db: &Database, coll: &Collection, r: usize) -> Result<(), Error> {
    db.tx(|tx| async move {
        let r2 = read_next(&tx, coll, r).await?;
        let r3 = read_next(&tx, coll, r2).await?;
        let r4 = read_next(&tx, coll, r3).await?;
        write_next(&tx, coll, r, r3)?;
        write_next(&tx, coll, r2, r4)?;
        write_next(&tx, coll, r3, r2)
    })
    .await
}

/// Snapshots the whole ring within a single transaction, reading all `N`
/// next-pointers *concurrently* (joined together as in-flight backend fetches),
/// and returns the snapshot. Unlike [`cycle_swap`]'s dependent pointer walk,
/// these reads have no data dependency, so this is what exercises `Transaction`'s
/// concurrent-read path (see `tx.rs`: `read` takes `&self` and never holds the
/// lock across `.await`). A committed read-only transaction observes exactly one
/// committed state under serializable isolation, so the caller can assert the
/// snapshot is itself a valid ring.
async fn read_ring_snapshot(
    db: &Database,
    coll: &Collection,
    node_count: usize,
) -> Result<Vec<usize>, Error> {
    db.tx(|tx| async move {
        futures::future::try_join_all((0..node_count).map(|k| read_next(&tx, coll, k))).await
    })
    .await
}

impl SimWorkload for CycleWorkload {
    type Op = usize;
    type State = AtomicUsize;

    fn clients(&self) -> &[Vec<usize>] {
        &self.clients
    }

    fn new_state(&self) -> AtomicUsize {
        AtomicUsize::new(0)
    }

    async fn seed(&self, db: &Database) {
        let coll = db.collection(CYCLE_COLLECTION);
        coll.create().await.expect("create collection");
        // Lay down the ring (key(i) -> (i + 1) % N).
        let node_count = self.node_count;
        let coll = &coll;
        db.tx(|tx| async move {
            for k in 0..node_count {
                write_next(&tx, coll, k, (k + 1) % node_count)?;
            }
            Ok(())
        })
        .await
        .expect("seed ring");
    }

    async fn run_op(db: &Database, op: &usize, _state: &AtomicUsize) -> Result<(), Error> {
        cycle_swap(db, &db.collection(CYCLE_COLLECTION), *op).await
    }

    async fn verify(&self, db: &Database, state: &AtomicUsize, _faults_enabled: bool) {
        if self.snapshot_reads != 0 {
            assert!(
                state.load(Ordering::SeqCst) != 0,
                "configured snapshot observer did not run"
            );
        }
        // Read every node's final next-pointer and assert the ring is still a
        // single N-cycle. The invariant holds whether or not faults occurred,
        // since each swap is atomic.
        let coll = db.collection(CYCLE_COLLECTION);
        let node_count = self.node_count;
        let mut next = vec![0usize; node_count];
        for (k, slot) in next.iter_mut().enumerate() {
            *slot = read_int(
                &coll
                    .read(&key_name(k))
                    .await
                    .expect("final read")
                    .expect("final value"),
            ) as usize;
        }
        assert_ring(&next);
    }

    fn spawn_observer(
        &self,
        backbone: &Arc<dyn Backend>,
        state: &Arc<AtomicUsize>,
    ) -> Option<rt::JoinHandle<()>> {
        // A concurrent read-only observer: while the swaps mutate the ring it
        // snapshots all N pointers in one transaction (concurrent reads, unlike
        // the swaps' dependent walk) and asserts the snapshot is itself a valid
        // ring. A committed read-only tx sees exactly one committed state under
        // serializable isolation, and every committed ring state is valid, so a
        // torn snapshot that still committed is a read-isolation bug `assert_ring`
        // will catch. Runs on the faultless backbone so it stays a reliable
        // observer regardless of the per-client transport faults. A panic here
        // propagates out of the run, just like the final verification.
        if self.snapshot_reads == 0 {
            return None;
        }
        let node_count = self.node_count;
        let snapshot_reads = self.snapshot_reads;
        let backbone = backbone.clone();
        let state = state.clone();
        Some(rt::spawn(async move {
            let Ok(db) = Self::open_db(&backbone).await else {
                return;
            };
            let coll = db.collection(CYCLE_COLLECTION);
            for _ in 0..snapshot_reads {
                rt::yield_now().await;
                state.fetch_add(1, Ordering::SeqCst);
                match read_ring_snapshot(&db, &coll, node_count).await {
                    Ok(snap) => assert_ring(&snap),
                    Err(_) => break,
                }
            }
            db.shutdown().await;
        }))
    }
}
