//! Deterministic-simulation workload harness for the concurrency fuzzer.
//!
//! A [`Workload`] is a set of clients, each a sequence of [`Op`]s, generated
//! from fuzzer bytes via [`arbitrary`]. [`run_and_assert`] opens one [`DB`] per
//! client over a single shared in-memory backend, runs every client
//! concurrently, and checks the serializability invariant: each key's final
//! value equals the total number of read-modify-write increments applied to it.
//!
//! Under the madsim simulator (`--cfg madsim`) task scheduling, time, and
//! randomness are all functions of a single seed, so a failing run reproduces
//! exactly from its input. [`run_and_record`] additionally captures the ordered
//! stream of backend operations so two same-seed runs can be compared
//! byte-for-byte (see the `concurrent_sim` self-check and ADR-008).

use std::sync::Arc;

use arbitrary::{Arbitrary, Unstructured};
use glassdb_backend::memory::MemoryBackend;
use glassdb_backend::middleware::{OpLog, RecordingBackend};
use glassdb_backend::Backend;

use crate::{Collection, Ctx, Error, Options, DB};

/// Number of distinct keys the workload operates on.
pub const KEY_COUNT: usize = 4;
const MAX_CLIENTS: usize = 4;
const MAX_OPS_PER_CLIENT: usize = 8;
const COLLECTION: &[u8] = b"fuzz";
const DB_NAME: &str = "fuzz";

/// A single operation performed by a client, all wrapped in their own
/// transaction (with automatic conflict retries).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// Read-modify-write: increment a single key.
    Rmw(usize),
    /// Increment two distinct keys in the same transaction.
    MultiRmw(usize, usize),
    /// Read-only transaction over a set of keys.
    ReadOnly(Vec<usize>),
}

/// A complete workload: one op sequence per client. Clients run concurrently.
#[derive(Debug, Clone, Default)]
pub struct Workload {
    /// Per-client op sequences.
    pub clients: Vec<Vec<Op>>,
}

impl<'a> Arbitrary<'a> for Op {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let key = |u: &mut Unstructured<'a>| -> arbitrary::Result<usize> {
            Ok(u.arbitrary::<u8>()? as usize % KEY_COUNT)
        };
        Ok(match u.arbitrary::<u8>()? % 3 {
            0 => Op::Rmw(key(u)?),
            1 => {
                let a = key(u)?;
                // Force the second key to differ so the increment count is
                // unambiguous (two writes to the same key in one tx net +1).
                let b = (a + 1 + (u.arbitrary::<u8>()? as usize % (KEY_COUNT - 1))) % KEY_COUNT;
                Op::MultiRmw(a, b)
            }
            _ => {
                let n = u.arbitrary::<u8>()? as usize % (KEY_COUNT + 1);
                let mut keys = Vec::with_capacity(n);
                for _ in 0..n {
                    keys.push(key(u)?);
                }
                Op::ReadOnly(keys)
            }
        })
    }
}

impl<'a> Arbitrary<'a> for Workload {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        // At least two clients so there is something to interleave.
        let nclients = 2 + (u.arbitrary::<u8>()? as usize % (MAX_CLIENTS - 1));
        let mut clients = Vec::with_capacity(nclients);
        for _ in 0..nclients {
            let nops = u.arbitrary::<u8>()? as usize % (MAX_OPS_PER_CLIENT + 1);
            let mut ops = Vec::with_capacity(nops);
            for _ in 0..nops {
                ops.push(Op::arbitrary(u)?);
            }
            clients.push(ops);
        }
        Ok(Workload { clients })
    }
}

fn write_int(n: i64) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}

fn read_int(b: &[u8]) -> i64 {
    let mut arr = [0u8; 8];
    arr.copy_from_slice(b);
    i64::from_le_bytes(arr)
}

fn key_name(k: usize) -> Vec<u8> {
    format!("k{k}").into_bytes()
}

async fn read_int_from_tx(tx: &mut crate::Tx, c: &Collection, k: &[u8]) -> Result<i64, Error> {
    match tx.read(c, k).await {
        Ok(v) => Ok(read_int(&v)),
        Err(e) if e.is_not_found() => Ok(0),
        Err(e) => Err(e),
    }
}

async fn run_client(ctx: &Ctx, db: &DB, coll: &Collection, ops: &[Op]) -> Result<(), Error> {
    for op in ops {
        match op {
            Op::Rmw(k) => {
                let kn = key_name(*k);
                db.tx(ctx, async |tx| {
                    let cur = read_int_from_tx(tx, coll, &kn).await?;
                    tx.write(coll, &kn, &write_int(cur + 1))
                })
                .await?;
            }
            Op::MultiRmw(a, b) => {
                let ka = key_name(*a);
                let kb = key_name(*b);
                db.tx(ctx, async |tx| {
                    let va = read_int_from_tx(tx, coll, &ka).await?;
                    let vb = read_int_from_tx(tx, coll, &kb).await?;
                    tx.write(coll, &ka, &write_int(va + 1))?;
                    tx.write(coll, &kb, &write_int(vb + 1))
                })
                .await?;
            }
            Op::ReadOnly(keys) => {
                let names: Vec<Vec<u8>> = keys.iter().map(|k| key_name(*k)).collect();
                db.tx(ctx, async |tx| {
                    for kn in &names {
                        let v = read_int_from_tx(tx, coll, kn).await?;
                        assert!(v >= 0, "observed negative value {v} for key {kn:?}");
                    }
                    Ok(())
                })
                .await?;
            }
        }
    }
    Ok(())
}

/// Total increments each key should receive, derived from the workload.
fn expected_increments(workload: &Workload) -> Vec<i64> {
    let mut expected = vec![0i64; KEY_COUNT];
    for ops in &workload.clients {
        for op in ops {
            match op {
                Op::Rmw(k) => expected[*k] += 1,
                Op::MultiRmw(a, b) => {
                    expected[*a] += 1;
                    expected[*b] += 1;
                }
                Op::ReadOnly(_) => {}
            }
        }
    }
    expected
}

fn deterministic_options() -> Options {
    Options {
        deterministic_time: true,
        ..Default::default()
    }
}

/// Opens one DB per client over `backend`, runs all clients concurrently, then
/// asserts the serializability invariant. Panics (the fuzzer's failure signal)
/// on any transaction error or invariant violation.
async fn run_on(workload: &Workload, backend: Arc<dyn Backend>) {
    let ctx = Ctx::background();
    let opts = deterministic_options();

    // Initialize the collection and seed every key to zero up front.
    let init_db = DB::open_with(&ctx, DB_NAME, backend.clone(), opts.clone())
        .await
        .expect("open init db");
    let init_coll = init_db.collection(COLLECTION);
    init_coll.create(&ctx).await.expect("create collection");
    init_db
        .tx(&ctx, async |tx| {
            for k in 0..KEY_COUNT {
                tx.write(&init_coll, &key_name(k), &write_int(0))?;
            }
            Ok(())
        })
        .await
        .expect("seed keys");

    let expected = expected_increments(workload);

    // Run each client on its own DB instance sharing the backend, concurrently.
    // (`join_all` rather than `task::spawn` because the per-op `AsyncFnMut`
    // closures borrow locals, which does not satisfy spawn's `for<'a>` Send
    // bound; the interleaving is still driven deterministically by madsim.)
    let mut futs = Vec::with_capacity(workload.clients.len());
    for ops in &workload.clients {
        let db = DB::open_with(&ctx, DB_NAME, backend.clone(), opts.clone())
            .await
            .expect("open client db");
        let ops = ops.clone();
        let cctx = ctx.clone();
        futs.push(async move {
            let coll = db.collection(COLLECTION);
            let res = run_client(&cctx, &db, &coll, &ops).await;
            db.close().await;
            res
        });
    }
    for res in futures::future::join_all(futs).await {
        res.expect("client tx failed");
    }

    // Each key's final value must equal the increments applied to it.
    for (k, &want) in expected.iter().enumerate() {
        let got = read_int(
            &init_coll
                .read_strong(&ctx, &key_name(k))
                .await
                .expect("final read"),
        );
        assert_eq!(
            got, want,
            "serializability violation on key k{k}: final {got}, expected {want}"
        );
    }
    init_db.close().await;
}

/// Runs `workload` over a fresh in-memory backend and asserts serializability.
pub async fn run_and_assert(workload: Workload) {
    let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    run_on(&workload, backend).await;
}

/// Like [`run_and_assert`] but records the ordered stream of backend operations
/// and returns the log, for byte-for-byte determinism comparison across runs.
pub async fn run_and_record(workload: &Workload) -> OpLog {
    let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let rec = Arc::new(RecordingBackend::new(mem));
    let log = rec.log();
    let backend: Arc<dyn Backend> = rec;
    run_on(workload, backend).await;
    log
}
