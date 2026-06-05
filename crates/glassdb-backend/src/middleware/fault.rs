//! A [`Backend`] decorator that injects deterministic, seeded faults for the
//! deterministic-simulation harness, in place of network/node fault injection.
//!
//! Three fault kinds are modeled, all preserving the harness invariant
//! `acked <= final <= started`:
//!
//! - **delay**: a virtual latency before the operation (harmless to the bound);
//! - **fail-before** (drop / transient error): the operation fails *without*
//!   landing, so the engine surfaces an error and the op is left in-doubt;
//! - **lost-ack**: a conditional write *lands* but its outcome is reported as
//!   [`BackendError::Unavailable`] (unknown), modelling a lost acknowledgement.
//!
//! All randomness comes from a seed and all timing from [`rt::sleep`], so under
//! the deterministic executor the whole fault schedule is a pure function of the
//! seed and the task schedule, and a failing run reproduces exactly.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use glassdb_concurr::rt;
use glassdb_concurr::{Ctx, Rng};

use crate::{Backend, BackendError, Metadata, ReadReply, Tags, Version, WriterId};

/// Probabilities (out of 256) of each fault kind, plus the maximum injected
/// delay. A probability of zero disables that fault.
#[derive(Debug, Clone, Copy)]
pub struct FaultOptions {
    /// Chance an operation is delayed before running.
    pub delay_prob: u8,
    /// Chance an operation fails before landing (drop / transient error).
    pub fail_before_prob: u8,
    /// Chance a conditional write lands but reports its ack as lost.
    pub lost_ack_prob: u8,
    /// Upper bound on an injected delay.
    pub max_delay: Duration,
}

impl FaultOptions {
    /// Scales the fault probabilities by `intensity` (0 = none, 255 = max).
    pub fn from_intensity(intensity: u8) -> Self {
        let scale = |max: u8| ((max as u16 * intensity as u16) / 255) as u8;
        FaultOptions {
            delay_prob: scale(64),
            fail_before_prob: scale(24),
            lost_ack_prob: scale(24),
            max_delay: Duration::from_millis(200),
        }
    }
}

impl Default for FaultOptions {
    fn default() -> Self {
        FaultOptions::from_intensity(64)
    }
}

/// Returns true with probability `prob/256` (a fault-injection coin flip),
/// drawing one value from `rng`. Kept local because the `& 0xff < prob`
/// convention is specific to this middleware's `FaultOptions`.
fn roll(rng: &mut Rng, prob: u8) -> bool {
    prob != 0 && (rng.next_u64() & 0xff) < prob as u64
}

/// A [`Backend`] decorator injecting seeded faults while [active](Self::set_active).
/// Faults are off until `set_active(true)`, so the harness can seed and verify
/// without interference and inject faults only while the clients run.
pub struct FaultBackend {
    inner: Arc<dyn Backend>,
    opts: FaultOptions,
    rng: Mutex<Rng>,
    active: AtomicBool,
}

impl FaultBackend {
    /// Wraps `inner` with a fault injector seeded by `seed`. Faults start
    /// disabled.
    pub fn new(inner: Arc<dyn Backend>, seed: u64, opts: FaultOptions) -> Arc<Self> {
        Arc::new(FaultBackend {
            inner,
            opts,
            rng: Mutex::new(Rng::new(seed)),
            active: AtomicBool::new(false),
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

    /// Applies the pre-operation faults (delay then fail-before). Returns an
    /// error if the operation should fail without landing.
    async fn before(&self, ctx: &Ctx) -> Result<(), BackendError> {
        if !self.is_active() {
            return Ok(());
        }
        let delay = {
            let mut r = self.rng.lock().unwrap();
            if roll(&mut r, self.opts.delay_prob) {
                Some(Duration::from_nanos(
                    r.below(self.opts.max_delay.as_nanos() as u64 + 1),
                ))
            } else {
                None
            }
        };
        if let Some(d) = delay {
            tokio::select! {
                _ = ctx.cancelled() => return Err(BackendError::Cancelled),
                _ = rt::sleep(d) => {}
            }
        }
        if roll(&mut self.rng.lock().unwrap(), self.opts.fail_before_prob) {
            return Err(BackendError::Unavailable(
                "injected transient backend fault".into(),
            ));
        }
        Ok(())
    }

    /// Decides whether a just-landed conditional write should report a lost ack.
    fn lost_ack(&self) -> bool {
        self.is_active() && roll(&mut self.rng.lock().unwrap(), self.opts.lost_ack_prob)
    }
}

fn lost_ack_err(op: &str) -> BackendError {
    BackendError::Unavailable(format!("injected lost ack on a landed {op}"))
}

#[async_trait]
impl Backend for FaultBackend {
    async fn read_if_modified(
        &self,
        ctx: &Ctx,
        path: &str,
        expected_writer: &WriterId,
    ) -> Result<ReadReply, BackendError> {
        self.before(ctx).await?;
        self.inner
            .read_if_modified(ctx, path, expected_writer)
            .await
    }

    async fn read(&self, ctx: &Ctx, path: &str) -> Result<ReadReply, BackendError> {
        self.before(ctx).await?;
        self.inner.read(ctx, path).await
    }

    async fn get_metadata(&self, ctx: &Ctx, path: &str) -> Result<Metadata, BackendError> {
        self.before(ctx).await?;
        self.inner.get_metadata(ctx, path).await
    }

    async fn set_tags_if(
        &self,
        ctx: &Ctx,
        path: &str,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.before(ctx).await?;
        match self.inner.set_tags_if(ctx, path, expected, tags).await {
            Ok(_) if self.lost_ack() => Err(lost_ack_err("set_tags_if")),
            other => other,
        }
    }

    async fn write(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        // Unconditional overwrite: idempotent, so only delay/fail-before apply.
        self.before(ctx).await?;
        self.inner.write(ctx, path, value, tags).await
    }

    async fn write_if(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.before(ctx).await?;
        match self.inner.write_if(ctx, path, value, expected, tags).await {
            Ok(_) if self.lost_ack() => Err(lost_ack_err("write_if")),
            other => other,
        }
    }

    async fn write_if_not_exists(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.before(ctx).await?;
        match self.inner.write_if_not_exists(ctx, path, value, tags).await {
            Ok(_) if self.lost_ack() => Err(lost_ack_err("write_if_not_exists")),
            other => other,
        }
    }

    async fn delete(&self, ctx: &Ctx, path: &str) -> Result<(), BackendError> {
        self.before(ctx).await?;
        self.inner.delete(ctx, path).await
    }

    async fn delete_if(
        &self,
        ctx: &Ctx,
        path: &str,
        expected: &Version,
    ) -> Result<(), BackendError> {
        self.before(ctx).await?;
        match self.inner.delete_if(ctx, path, expected).await {
            Ok(()) if self.lost_ack() => Err(lost_ack_err("delete_if")),
            other => other,
        }
    }

    async fn list(&self, ctx: &Ctx, dir_path: &str) -> Result<Vec<String>, BackendError> {
        self.before(ctx).await?;
        self.inner.list(ctx, dir_path).await
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
        let ctx = Ctx::background();
        // While inactive, an unconditional write/read round-trips cleanly.
        fb.write(&ctx, "p", b"v".to_vec(), Tags::new())
            .await
            .unwrap();
        let r = fb.read(&ctx, "p").await.unwrap();
        assert_eq!(r.contents, b"v");
    }

    #[tokio::test]
    async fn same_seed_same_fault_sequence() {
        fn decisions(seed: u64) -> Vec<bool> {
            let mut r = Rng::new(seed);
            (0..32).map(|_| roll(&mut r, 128)).collect()
        }
        assert_eq!(decisions(42), decisions(42));
        assert_ne!(decisions(42), decisions(43));
    }

    #[tokio::test]
    async fn active_eventually_injects_faults() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let fb = FaultBackend::new(mem, 7, FaultOptions::from_intensity(255));
        let ctx = Ctx::background();
        fb.set_active(true);
        // With max intensity, some conditional writes are faulted within a few
        // dozen attempts.
        let mut faults = 0;
        for i in 0..200 {
            let path = format!("k{i}");
            let r = fb
                .write_if_not_exists(&ctx, &path, b"v".to_vec(), Tags::new())
                .await;
            if r.is_err() {
                faults += 1;
            }
        }
        assert!(faults > 0, "expected at least one injected fault");
    }
}
