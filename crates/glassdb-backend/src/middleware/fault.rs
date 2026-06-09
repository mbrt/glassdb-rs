//! A [`Backend`] decorator modelling a single client's faulty **transport** to
//! the store, in place of network/node fault injection. Faults belong to the
//! link between *one* client and the (shared) backend — not to the backend
//! itself — so the harness wraps the shared store in one `FaultBackend` *per
//! client*; one client's outage leaves the others able to reach storage and
//! recover, exactly as a real node disconnect would.
//!
//! Every operation can fault on **either side**, all preserving the harness
//! invariant `acked <= final <= started`:
//!
//! - **delay**: a virtual latency before the operation (harmless to the bound);
//! - **dropped request** (before landing): the request never reaches the store,
//!   so the op fails without landing and the engine retries / leaves it in-doubt;
//! - **lost ack** (after landing): the op *lands* at the store but the response
//!   is lost, reported as [`BackendError::Unavailable`] (outcome unknown) — the
//!   in-doubt case that drives commit-status / redrive recovery;
//! - **outage**: a sustained, all-or-nothing window ([`down`](FaultBackend::down)
//!   / [`heal`](FaultBackend::heal)) during which *every* op on this client's
//!   transport faults (either side). One down/heal pair makes a whole correlated
//!   outage deterministically reachable — the thing that drives lease expiry and
//!   lock-lease recovery, which coincident independent rolls cannot do reliably.
//!
//! All randomness comes from a seed/tape and all timing from [`rt::sleep`], so
//! under the deterministic executor the whole fault schedule is a pure function
//! of the input and the task schedule, and a failing run reproduces exactly.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use glassdb_concurr::Tape;
use glassdb_concurr::rt;

use crate::{Backend, BackendError, Metadata, ReadReply, Tags, Version, WriterId};

/// Probabilities (out of 256) governing transport faults, plus the maximum
/// injected delay. A probability of zero disables that behaviour.
#[derive(Debug, Clone, Copy)]
pub struct FaultOptions {
    /// Chance an operation is delayed before running.
    pub delay_prob: u8,
    /// Chance the transport faults an operation (drops it on one side or the
    /// other).
    pub fault_prob: u8,
    /// Given a fault, the chance it happens *after* landing (the op reaches the
    /// store but the ack is lost) rather than before. The remainder are dropped
    /// requests that never land.
    pub lost_ack_prob: u8,
    /// Upper bound on an injected delay.
    pub max_delay: Duration,
}

impl FaultOptions {
    /// Scales the fault probabilities by `intensity` (0 = none, 255 = max). The
    /// before/after split (`lost_ack_prob`) is intensity-independent: a dropped
    /// link is roughly as likely to lose the request as the ack.
    pub fn from_intensity(intensity: u8) -> Self {
        let scale = |max: u8| ((max as u16 * intensity as u16) / 255) as u8;
        FaultOptions {
            delay_prob: scale(64),
            fault_prob: scale(24),
            lost_ack_prob: 128,
            max_delay: Duration::from_millis(200),
        }
    }
}

impl Default for FaultOptions {
    fn default() -> Self {
        FaultOptions::from_intensity(64)
    }
}

/// Where in an operation's lifecycle the transport drops it.
enum Fault {
    /// The request never reached the store (nothing landed).
    Dropped,
    /// The op landed at the store but its ack was lost (outcome unknown).
    LostAck,
}

/// A per-client transport [`Backend`] decorator injecting faults while
/// [active](Self::set_active). Faults are off until `set_active(true)`, so the
/// harness can seed and verify over a perfect connection and inject faults only
/// while the clients run.
///
/// Fault decisions come from a [`Tape`]: the fuzzer can guide the *fault
/// schedule* byte-for-byte, and with an empty tape the decisions fall back to a
/// seeded PRNG (pure seed-breadth sampling, e.g. PCT runs).
pub struct FaultBackend {
    inner: Arc<dyn Backend>,
    opts: FaultOptions,
    tape: Mutex<Tape>,
    active: AtomicBool,
    /// Whether this client's transport is currently in a sustained outage
    /// (all-or-nothing: every op faults until healed).
    down: AtomicBool,
}

impl FaultBackend {
    /// Wraps `inner` with a fault injector whose decisions are a pure function of
    /// `seed`. Faults start disabled.
    pub fn new(inner: Arc<dyn Backend>, seed: u64, opts: FaultOptions) -> Arc<Self> {
        Self::with_tape(inner, Vec::new(), seed, opts)
    }

    /// Wraps `inner` with a fault injector that draws decisions from `tape`
    /// first, then from a PRNG seeded by `seed` once the tape is exhausted.
    /// Faults start disabled.
    pub fn with_tape(
        inner: Arc<dyn Backend>,
        tape: Vec<u8>,
        seed: u64,
        opts: FaultOptions,
    ) -> Arc<Self> {
        Arc::new(FaultBackend {
            inner,
            opts,
            tape: Mutex::new(Tape::new(tape, seed)),
            active: AtomicBool::new(false),
            down: AtomicBool::new(false),
        })
    }

    /// Enables or disables fault injection. While disabled, all operations pass
    /// straight through.
    pub fn set_active(&self, on: bool) {
        self.active.store(on, Ordering::SeqCst);
    }

    fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    /// Opens a sustained outage on this client's transport: every operation
    /// faults (on either side) until [`heal`](Self::heal).
    pub fn down(&self) {
        self.down.store(true, Ordering::SeqCst);
    }

    /// Heals a previously [downed](Self::down) transport.
    pub fn heal(&self) {
        self.down.store(false, Ordering::SeqCst);
    }

    /// Runs `op` through the faulty transport: an optional delay, then either the
    /// real call, a dropped request (no landing), or a landed call whose ack is
    /// lost. During an outage every op faults. The caller cancels by dropping
    /// the surrounding future.
    async fn transport<T, Fut>(&self, op: impl FnOnce() -> Fut) -> Result<T, BackendError>
    where
        Fut: std::future::Future<Output = Result<T, BackendError>>,
    {
        if !self.is_active() {
            return op().await;
        }
        // Draw delay and the fault decision from the tape in one shot, so tape
        // consumption is grouped per operation. An outage forces a fault.
        let (delay, fault) = {
            let mut t = self.tape.lock().unwrap();
            let delay = if t.roll(self.opts.delay_prob) {
                Some(Duration::from_nanos(
                    t.below(self.opts.max_delay.as_nanos() as u64 + 1),
                ))
            } else {
                None
            };
            let faulted = self.down.load(Ordering::SeqCst) || t.roll(self.opts.fault_prob);
            let fault = faulted.then(|| {
                if t.roll(self.opts.lost_ack_prob) {
                    Fault::LostAck
                } else {
                    Fault::Dropped
                }
            });
            (delay, fault)
        };
        if let Some(d) = delay {
            rt::sleep(d).await;
        }
        match fault {
            None => op().await,
            Some(Fault::Dropped) => Err(BackendError::Unavailable(
                "injected dropped request (lost before landing)".into(),
            )),
            // The op reaches the store; only a *successful* landing is reported
            // as an in-doubt lost ack. A genuine error is returned as-is.
            Some(Fault::LostAck) => match op().await {
                Ok(_) => Err(BackendError::Unavailable(
                    "injected lost ack (landed, ack lost)".into(),
                )),
                Err(e) => Err(e),
            },
        }
    }
}

#[async_trait]
impl Backend for FaultBackend {
    async fn read_if_modified(
        &self,
        path: &str,
        expected_writer: &WriterId,
    ) -> Result<ReadReply, BackendError> {
        self.transport(|| self.inner.read_if_modified(path, expected_writer))
            .await
    }

    async fn read(&self, path: &str) -> Result<ReadReply, BackendError> {
        self.transport(|| self.inner.read(path)).await
    }

    async fn get_metadata(&self, path: &str) -> Result<Metadata, BackendError> {
        self.transport(|| self.inner.get_metadata(path)).await
    }

    async fn set_tags_if(
        &self,
        path: &str,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.transport(|| self.inner.set_tags_if(path, expected, tags))
            .await
    }

    async fn write(
        &self,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.transport(|| self.inner.write(path, value, tags)).await
    }

    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.transport(|| self.inner.write_if(path, value, expected, tags))
            .await
    }

    async fn write_if_not_exists(
        &self,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.transport(|| self.inner.write_if_not_exists(path, value, tags))
            .await
    }

    async fn delete(&self, path: &str) -> Result<(), BackendError> {
        self.transport(|| self.inner.delete(path)).await
    }

    async fn delete_if(&self, path: &str, expected: &Version) -> Result<(), BackendError> {
        self.transport(|| self.inner.delete_if(path, expected))
            .await
    }

    async fn list(&self, dir_path: &str) -> Result<Vec<String>, BackendError> {
        self.transport(|| self.inner.list(dir_path)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryBackend;

    #[tokio::test]
    async fn inactive_passes_through() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let fb = FaultBackend::new(mem, 1, FaultOptions::from_intensity(255));
        // While inactive, an unconditional write/read round-trips cleanly.
        fb.write("p", b"v".to_vec(), Tags::new()).await.unwrap();
        let r = fb.read("p").await.unwrap();
        assert_eq!(r.contents, b"v");
    }

    #[tokio::test]
    async fn fault_tape_guides_injection() {
        async fn run(tape: Vec<u8>) -> Vec<bool> {
            let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
            let fb = FaultBackend::with_tape(mem, tape, 7, FaultOptions::from_intensity(255));
            fb.set_active(true);
            let mut outcomes = Vec::new();
            for i in 0..32 {
                let ok = fb
                    .write_if_not_exists(&format!("k{i}"), b"v".to_vec(), Tags::new())
                    .await
                    .is_ok();
                outcomes.push(ok);
            }
            outcomes
        }
        // A given fault tape yields a byte-for-byte reproducible outcome.
        let tape: Vec<u8> = (0..64u16).map(|b| b as u8).collect();
        assert_eq!(run(tape.clone()).await, run(tape).await);
        // A tape of low bytes keeps every roll under the threshold, so faults fire.
        let faults = run(vec![0u8; 64]).await.iter().filter(|ok| !**ok).count();
        assert!(faults > 0, "expected the low-byte tape to inject faults");
    }

    #[tokio::test]
    async fn outage_downs_whole_transport_then_heals() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        // Isolate the outage: disable the i.i.d. rolls, and force any fault to be
        // a dropped request so the empty store is never written.
        let opts = FaultOptions {
            delay_prob: 0,
            fault_prob: 0,
            lost_ack_prob: 0,
            max_delay: Duration::from_millis(0),
        };
        let fb = FaultBackend::new(mem, 1, opts);
        fb.set_active(true);

        // While down, the transport is all-or-nothing: every op fails, whatever
        // its path.
        fb.down();
        for i in 0..32 {
            assert!(
                matches!(
                    fb.read(&format!("p{i}")).await,
                    Err(BackendError::Unavailable(_))
                ),
                "op {i} should fail during a transport outage"
            );
        }

        // Healing restores the transport: reads now reach the (empty) store.
        fb.heal();
        for i in 0..32 {
            assert!(
                !matches!(
                    fb.read(&format!("p{i}")).await,
                    Err(BackendError::Unavailable(_))
                ),
                "op {i} still outaged after heal"
            );
        }
    }

    #[tokio::test]
    async fn active_eventually_injects_faults() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let fb = FaultBackend::new(mem, 7, FaultOptions::from_intensity(255));
        fb.set_active(true);
        // With max intensity, some conditional writes are faulted within a few
        // dozen attempts.
        let mut faults = 0;
        for i in 0..200 {
            let path = format!("k{i}");
            let r = fb
                .write_if_not_exists(&path, b"v".to_vec(), Tags::new())
                .await;
            if r.is_err() {
                faults += 1;
            }
        }
        assert!(faults > 0, "expected at least one injected fault");
    }
}
