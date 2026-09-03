//! Shared-key RMW workload for lost-update, serializability, and in-doubt accounting.
//! Unlike the transaction API workload, clients deliberately overlap on keys and
//! increments are non-idempotent, so acknowledged/started bounds are the oracle.

use std::future::Future;
use std::sync::Mutex;

use arbitrary::{Arbitrary, Unstructured};

use crate::{Collection, CollectionPath, Database, Error, InlinePolicy, SplitPolicy};

use super::SimMedia;
use super::harness::{SimWorkload, open_det_db};
use super::{MAX_CLIENTS, MAX_OPS_PER_CLIENT, key_name, read_int, try_read_int, write_int};
/// Number of distinct keys the workload operates on.
pub const RMW_KEY_COUNT: usize = 4;
/// Collection the increment workload operates on. Each workload owns its own
/// collection; the harness never needs to know which one.
const INCREMENT_COLLECTION: &[u8] = b"fuzz";

/// A single operation performed by a client, all wrapped in their own
/// transaction (with automatic conflict retries).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RmwOp {
    /// Read-modify-write: increment a single key.
    Rmw(usize),
    /// Increment two distinct keys in the same transaction.
    MultiRmw(usize, usize),
    /// Read-only transaction over a set of keys.
    ReadOnly(Vec<usize>),
}

/// A complete workload: one op sequence per client. Clients run concurrently.
#[derive(Debug, Clone, Default)]
pub struct RmwWorkload {
    /// Per-client op sequences.
    pub clients: Vec<Vec<RmwOp>>,
}
impl<'a> Arbitrary<'a> for RmwOp {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let key = |u: &mut Unstructured<'a>| -> arbitrary::Result<usize> {
            Ok(u.arbitrary::<u8>()? as usize % RMW_KEY_COUNT)
        };
        Ok(match u.arbitrary::<u8>()? % 3 {
            0 => RmwOp::Rmw(key(u)?),
            1 => {
                let a = key(u)?;
                // Force the second key to differ so the increment count is
                // unambiguous (two writes to the same key in one tx net +1).
                let b =
                    (a + 1 + (u.arbitrary::<u8>()? as usize % (RMW_KEY_COUNT - 1))) % RMW_KEY_COUNT;
                RmwOp::MultiRmw(a, b)
            }
            _ => {
                let n = u.arbitrary::<u8>()? as usize % (RMW_KEY_COUNT + 1);
                let mut keys = Vec::with_capacity(n);
                for _ in 0..n {
                    keys.push(key(u)?);
                }
                RmwOp::ReadOnly(keys)
            }
        })
    }
}

impl<'a> Arbitrary<'a> for RmwWorkload {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        // At least two clients so there is something to interleave.
        let nclients = 2 + (u.arbitrary::<u8>()? as usize % (MAX_CLIENTS - 1));
        let mut clients = Vec::with_capacity(nclients);
        for _ in 0..nclients {
            let nops = u.arbitrary::<u8>()? as usize % (MAX_OPS_PER_CLIENT + 1);
            let mut ops = Vec::with_capacity(nops);
            for _ in 0..nops {
                ops.push(RmwOp::arbitrary(u)?);
            }
            clients.push(ops);
        }
        Ok(RmwWorkload { clients })
    }
}

async fn read_int_from_tx(tx: &crate::Transaction, c: &Collection, k: &[u8]) -> Result<i64, Error> {
    let value = tx.read(c, k).await?.ok_or(Error::NotFound)?;
    match try_read_int(&value) {
        Some(current) if current >= 0 => Ok(current),
        _ => Err(Error::internal(format!(
            "key {k:?} has invalid increment value {value:?}"
        ))),
    }
}

fn incremented_value(key: &[u8], current: i64) -> Result<Vec<u8>, Error> {
    current
        .checked_add(1)
        .map(write_int)
        .ok_or_else(|| Error::internal(format!("integer overflow for key {key:?}")))
}

/// Per-key accounting shared across client tasks. `started` counts increments
/// that entered a transaction; `acked` counts those whose commit returned `Ok`.
pub struct RmwAcct {
    started: Vec<i64>,
    acked: Vec<i64>,
}

impl RmwAcct {
    fn new() -> Self {
        RmwAcct {
            started: vec![0; RMW_KEY_COUNT],
            acked: vec![0; RMW_KEY_COUNT],
        }
    }
}

/// Attempts a single op in its own transaction, updating `acct`: `started` is
/// bumped before the attempt, `acked` only after a commit outcome. A failure leaves the op
/// counted in `started` but not `acked` (i.e. in-doubt).
async fn run_one(
    db: &Database,
    coll: &Collection,
    op: &RmwOp,
    acct: &Mutex<RmwAcct>,
) -> Result<(), Error> {
    match op {
        RmwOp::Rmw(k) => {
            acct.lock().unwrap().started[*k] += 1;
            // Bind a reference so the `async move` body captures `&Vec`
            // (`Copy`) instead of moving the key, keeping the closure
            // `FnMut` for retries.
            let kn = &key_name(*k);
            db.tx(|tx| async move {
                let cur = read_int_from_tx(&tx, coll, kn).await?;
                tx.write(coll, kn, &incremented_value(kn, cur)?)
            })
            .await?;
            acct.lock().unwrap().acked[*k] += 1;
        }
        RmwOp::MultiRmw(a, b) => {
            {
                let mut g = acct.lock().unwrap();
                g.started[*a] += 1;
                g.started[*b] += 1;
            }
            let ka = &key_name(*a);
            let kb = &key_name(*b);
            db.tx(|tx| async move {
                let va = read_int_from_tx(&tx, coll, ka).await?;
                let vb = read_int_from_tx(&tx, coll, kb).await?;
                tx.write(coll, ka, &incremented_value(ka, va)?)?;
                tx.write(coll, kb, &incremented_value(kb, vb)?)
            })
            .await?;
            {
                let mut g = acct.lock().unwrap();
                g.acked[*a] += 1;
                g.acked[*b] += 1;
            }
        }
        RmwOp::ReadOnly(keys) => {
            let names: Vec<Vec<u8>> = keys.iter().map(|k| key_name(*k)).collect();
            let names = &names;
            db.tx(|tx| async move {
                for kn in names {
                    read_int_from_tx(&tx, coll, kn).await?;
                }
                Ok(())
            })
            .await?;
        }
    }
    Ok(())
}

/// Total increments each key should receive, derived from the workload.
fn expected_increments(workload: &RmwWorkload) -> Vec<i64> {
    let mut expected = vec![0i64; RMW_KEY_COUNT];
    for ops in &workload.clients {
        for op in ops {
            match op {
                RmwOp::Rmw(k) => expected[*k] += 1,
                RmwOp::MultiRmw(a, b) => {
                    expected[*a] += 1;
                    expected[*b] += 1;
                }
                RmwOp::ReadOnly(_) => {}
            }
        }
    }
    expected
}

/// Asserts the serializability invariant for every key: an acknowledged commit
/// must be durable (`acked <= final`) and the store cannot show more than what
/// was attempted (`final <= started`). With faults disabled, `final` must equal
/// the workload's total increments exactly.
fn assert_bounds(acct: &RmwAcct, finals: &[i64], expected: &[i64], failures_enabled: bool) {
    for k in 0..RMW_KEY_COUNT {
        let a = acct.acked[k];
        let s = acct.started[k];
        let f = finals[k];
        assert!(
            a <= f && f <= s,
            "key k{k}: violated acked({a}) <= final({f}) <= started({s})"
        );
        if !failures_enabled {
            assert_eq!(
                f, expected[k],
                "key k{k}: final {f} != expected {} (no faults)",
                expected[k]
            );
        }
    }
}
impl SimWorkload for RmwWorkload {
    type Op = RmwOp;
    type State = Mutex<RmwAcct>;

    fn clients(&self) -> &[Vec<RmwOp>] {
        &self.clients
    }

    fn new_state(&self) -> Mutex<RmwAcct> {
        Mutex::new(RmwAcct::new())
    }

    fn open_db(
        backend: &std::sync::Arc<dyn glassdb_backend::Backend>,
        media: Option<SimMedia>,
    ) -> impl Future<Output = Result<Database, Error>> + Send {
        // Two distinct direct increments fill a leaf, so the existing
        // contention/fault schedules also cover pressure-driven structural
        // work without adding a second lifecycle oracle or workload.
        open_det_db(
            backend,
            SplitPolicy::default(),
            InlinePolicy {
                max_value_bytes: 8,
                max_leaf_bytes: 16,
            },
            media,
        )
    }

    async fn seed(&self, db: &Database) {
        let coll = db
            .root_collection()
            .create_collection_if_absent(INCREMENT_COLLECTION)
            .await
            .expect("create collection");
        // Seed every key to zero up front, so a read of an untouched key returns
        // 0 rather than NotFound and the increment accounting is exact.
        let coll = &coll;
        db.tx(|tx| async move {
            for k in 0..RMW_KEY_COUNT {
                tx.write(coll, &key_name(k), &write_int(0))?;
            }
            Ok(())
        })
        .await
        .expect("seed keys");
    }

    async fn run_op(db: &Database, op: &RmwOp, state: &Mutex<RmwAcct>) -> Result<(), Error> {
        let coll = db
            .open_collection(&CollectionPath::new(INCREMENT_COLLECTION)?)
            .await?;
        run_one(db, &coll, op, state).await
    }

    async fn verify(&self, db: &Database, state: &Mutex<RmwAcct>, failures_enabled: bool) {
        let coll = db
            .open_collection(&CollectionPath::new(INCREMENT_COLLECTION).unwrap())
            .await
            .expect("open increment collection");
        let expected = expected_increments(self);
        let mut finals = vec![0i64; RMW_KEY_COUNT];
        for (k, slot) in finals.iter_mut().enumerate() {
            *slot = read_int(
                &coll
                    .read(&key_name(k))
                    .await
                    .expect("final read")
                    .expect("final value"),
            );
        }
        let acct = state.lock().unwrap();
        assert_bounds(&acct, &finals, &expected, failures_enabled);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use glassdb_backend::{Backend, memory::MemoryBackend};

    #[tokio::test]
    async fn missing_preseeded_key_is_not_treated_as_zero() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let db = open_det_db(
            &backend,
            SplitPolicy::default(),
            InlinePolicy::default(),
            None,
        )
        .await
        .unwrap();
        let collection = db
            .root_collection()
            .create_collection_if_absent(INCREMENT_COLLECTION)
            .await
            .unwrap();
        let collection = &collection;

        let result = db
            .tx(|tx| async move {
                read_int_from_tx(&tx, collection, b"missing")
                    .await
                    .map(|_| ())
            })
            .await;
        assert!(matches!(result, Err(Error::NotFound)));
        db.shutdown().await;
    }
}
