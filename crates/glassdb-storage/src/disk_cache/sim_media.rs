//! Deterministic byte-level media for one persistent-cache container.

use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use glassdb_concurr::{Tape, rt};
use tokio::sync::Notify;

use super::media::{CacheFile, CacheMedia};
use super::{PersistentCacheMedia, compact};

/// Breadth of media faults selected from the independent media tape.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MediaFaultProfile {
    /// Operations yield but otherwise succeed.
    #[default]
    Healthy,
    /// Operations may be delayed or fail before taking effect.
    Selected,
    /// Operations may additionally write partially or remain pending.
    Full,
}

/// Deterministic byte-level model of the disk cache's single container.
#[derive(Clone)]
pub struct SimMedia {
    shared: Arc<Shared>,
}

impl From<SimMedia> for PersistentCacheMedia {
    fn from(media: SimMedia) -> Self {
        Self {
            media: Arc::new(media),
            geometry: compact::GEOMETRY,
        }
    }
}

struct Shared {
    state: Mutex<State>,
    tape: Mutex<Tape>,
    profile: MediaFaultProfile,
    changed: Notify,
}

struct State {
    attached: bool,
    permanently_unavailable: bool,
    generation: u64,
    next_handle: u64,
    locked_by: Option<u64>,
    working: Option<Vec<u8>>,
    durable: Vec<u8>,
    durable_exists: bool,
    out_of_bounds_accesses: u64,
}

struct SimFile {
    shared: Arc<Shared>,
    handle: u64,
    generation: u64,
}

#[derive(Clone, Copy)]
enum OperationEffect {
    Complete,
    PartialWrite,
}

// Selected faults stay sparse so cross-layer tests usually reach cache
// behavior; Full spends more decisions exploring the isolated fault domain.
const SELECTED_COMPLETE_CUTOFF: u8 = 240;
const SELECTED_DELAY_CUTOFF: u8 = 248;
const FULL_COMPLETE_CUTOFF: u8 = 224;
const FULL_DELAY_CUTOFF: u8 = 240;
const FULL_ERROR_CUTOFF: u8 = 248;
const FULL_PARTIAL_CUTOFF: u8 = 252;

impl SimMedia {
    /// Creates one simulated cache container with its own fault-decision tape.
    pub fn new(profile: MediaFaultProfile, fault_tape: Vec<u8>, seed: u64) -> Self {
        Self {
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    attached: true,
                    permanently_unavailable: false,
                    generation: 1,
                    next_handle: 1,
                    locked_by: None,
                    working: None,
                    durable: Vec::new(),
                    durable_exists: false,
                    out_of_bounds_accesses: 0,
                }),
                tape: Mutex::new(Tape::new(fault_tape, seed)),
                profile,
                changed: Notify::new(),
            }),
        }
    }

    /// Simulates abrupt process loss and resolves every unsynchronized effect
    /// independently against the last completed synchronization.
    pub fn crash(&self) {
        let mut state = self.shared.state.lock().unwrap();
        self.shared.resolve_crash(&mut state);
        state.generation = state.generation.wrapping_add(1).max(1);
        state.locked_by = None;
        drop(state);
        self.shared.changed.notify_waiters();
    }

    /// Simulates media removal. Existing handles remain invalid after reattach.
    pub fn detach(&self) {
        let mut state = self.shared.state.lock().unwrap();
        self.shared.resolve_crash(&mut state);
        state.attached = false;
        state.generation = state.generation.wrapping_add(1).max(1);
        state.locked_by = None;
        drop(state);
        self.shared.changed.notify_waiters();
    }

    /// Makes the detached media available for a new exclusive open.
    pub fn reattach(&self) {
        let mut state = self.shared.state.lock().unwrap();
        if !state.permanently_unavailable {
            state.attached = true;
        }
        drop(state);
        self.shared.changed.notify_waiters();
    }

    /// Makes all current and future operations fail permanently.
    pub fn make_permanently_unavailable(&self) {
        let mut state = self.shared.state.lock().unwrap();
        self.shared.resolve_crash(&mut state);
        state.attached = false;
        state.permanently_unavailable = true;
        state.generation = state.generation.wrapping_add(1).max(1);
        state.locked_by = None;
        drop(state);
        self.shared.changed.notify_waiters();
    }

    /// Flips `mask` in one durable byte and makes the damage immediately
    /// visible to the attached process.
    pub fn corrupt(&self, offset: u64, mask: u8) -> bool {
        let Ok(offset) = usize::try_from(offset) else {
            return false;
        };
        let mut state = self.shared.state.lock().unwrap();
        if !state.durable_exists || offset >= state.durable.len() {
            return false;
        }
        state.durable[offset] ^= mask;
        if let Some(working) = state.working.as_mut()
            && offset < working.len()
        {
            working[offset] ^= mask;
        }
        true
    }

    /// Returns the number of cache requests that exceeded the modeled file.
    pub fn out_of_bounds_accesses(&self) -> u64 {
        self.shared.state.lock().unwrap().out_of_bounds_accesses
    }

    /// Returns a snapshot of bytes at the last completed durability boundary.
    pub fn durable_bytes(&self) -> Option<Vec<u8>> {
        let state = self.shared.state.lock().unwrap();
        state.durable_exists.then(|| state.durable.clone())
    }
}

impl Shared {
    async fn before_operation(
        &self,
        expected: Option<(u64, u64)>,
        allow_partial_write: bool,
    ) -> io::Result<OperationEffect> {
        rt::yield_now().await;
        self.check_available(expected)?;

        let decision = {
            let mut tape = self.tape.lock().unwrap();
            match self.profile {
                MediaFaultProfile::Healthy => 0,
                MediaFaultProfile::Selected | MediaFaultProfile::Full => tape.below(256) as u8,
            }
        };
        match self.profile {
            MediaFaultProfile::Healthy => Ok(OperationEffect::Complete),
            MediaFaultProfile::Selected if decision < SELECTED_COMPLETE_CUTOFF => {
                Ok(OperationEffect::Complete)
            }
            MediaFaultProfile::Selected if decision < SELECTED_DELAY_CUTOFF => {
                self.delay(expected).await?;
                Ok(OperationEffect::Complete)
            }
            MediaFaultProfile::Selected => Err(injected_error()),
            MediaFaultProfile::Full if decision < FULL_COMPLETE_CUTOFF => {
                Ok(OperationEffect::Complete)
            }
            MediaFaultProfile::Full if decision < FULL_DELAY_CUTOFF => {
                self.delay(expected).await?;
                Ok(OperationEffect::Complete)
            }
            MediaFaultProfile::Full if decision < FULL_ERROR_CUTOFF => Err(injected_error()),
            MediaFaultProfile::Full if decision < FULL_PARTIAL_CUTOFF && allow_partial_write => {
                Ok(OperationEffect::PartialWrite)
            }
            MediaFaultProfile::Full if decision < FULL_PARTIAL_CUTOFF => Err(injected_error()),
            MediaFaultProfile::Full => self.pending_until_failure(expected).await,
        }
    }

    async fn delay(&self, expected: Option<(u64, u64)>) -> io::Result<()> {
        let millis = {
            let mut tape = self.tape.lock().unwrap();
            1 + tape.below(4)
        };
        rt::sleep(Duration::from_millis(millis)).await;
        self.check_available(expected)
    }

    async fn pending_until_failure(
        &self,
        expected: Option<(u64, u64)>,
    ) -> io::Result<OperationEffect> {
        loop {
            let changed = self.changed.notified();
            self.check_available(expected)?;
            changed.await;
        }
    }

    fn check_available(&self, expected: Option<(u64, u64)>) -> io::Result<()> {
        let state = self.state.lock().unwrap();
        Self::check_state_available(&state, expected)
    }

    fn check_state_available(state: &State, expected: Option<(u64, u64)>) -> io::Result<()> {
        if state.permanently_unavailable {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "simulated cache media is permanently unavailable",
            ));
        }
        if !state.attached {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "simulated cache media is detached",
            ));
        }
        if let Some((handle, generation)) = expected
            && (generation != state.generation || state.locked_by != Some(handle))
        {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "simulated cache-media handle is stale",
            ));
        }
        Ok(())
    }

    fn resolve_crash(&self, state: &mut State) {
        let Some(working) = state.working.as_ref() else {
            state.durable.clear();
            state.durable_exists = false;
            return;
        };
        let creation_survives = state.durable_exists || self.choose();
        if !creation_survives {
            state.working = None;
            state.durable.clear();
            state.durable_exists = false;
            return;
        }

        let old_len = state.durable.len();
        let new_len = working.len();
        let resolved_len = if old_len == new_len || self.choose() {
            new_len
        } else {
            old_len
        };
        let mut resolved = vec![0; resolved_len];
        for (offset, byte) in resolved.iter_mut().enumerate() {
            let old = state.durable.get(offset).copied().unwrap_or(0);
            let new = working.get(offset).copied().unwrap_or(0);
            *byte = if old == new || self.choose() {
                new
            } else {
                old
            };
        }
        state.durable = resolved;
        state.durable_exists = true;
        state.working = None;
    }

    fn choose(&self) -> bool {
        self.tape.lock().unwrap().roll(128)
    }

    fn record_out_of_bounds(state: &mut State) -> io::Error {
        state.out_of_bounds_accesses += 1;
        io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "simulated cache-media access is out of bounds",
        )
    }
}

#[async_trait]
impl CacheMedia for SimMedia {
    async fn open_exclusive(&self, _directory: &Path) -> io::Result<Arc<dyn CacheFile>> {
        self.shared.before_operation(None, false).await?;
        let mut state = self.shared.state.lock().unwrap();
        Shared::check_state_available(&state, None)?;
        if state.locked_by.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "simulated cache media is already locked",
            ));
        }
        if state.working.is_none() {
            state.working = if state.durable_exists {
                Some(state.durable.clone())
            } else {
                Some(Vec::new())
            };
        }
        let handle = state.next_handle;
        state.next_handle = state.next_handle.wrapping_add(1).max(1);
        state.locked_by = Some(handle);
        Ok(Arc::new(SimFile {
            shared: self.shared.clone(),
            handle,
            generation: state.generation,
        }))
    }
}

impl SimFile {
    async fn effect(&self, allow_partial_write: bool) -> io::Result<OperationEffect> {
        self.shared
            .before_operation(Some((self.handle, self.generation)), allow_partial_write)
            .await
    }

    fn with_working<T>(
        &self,
        operation: impl FnOnce(&mut State, &mut Vec<u8>) -> io::Result<T>,
    ) -> io::Result<T> {
        let mut state = self.shared.state.lock().unwrap();
        if self.generation != state.generation || state.locked_by != Some(self.handle) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "simulated cache-media handle is stale",
            ));
        }
        let mut working = state.working.take().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "simulated cache container does not exist",
            )
        })?;
        let result = operation(&mut state, &mut working);
        state.working = Some(working);
        result
    }
}

impl Drop for SimFile {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().unwrap();
        if self.generation == state.generation && state.locked_by == Some(self.handle) {
            state.locked_by = None;
        }
    }
}

#[async_trait]
impl CacheFile for SimFile {
    async fn len(&self) -> io::Result<u64> {
        self.effect(false).await?;
        self.with_working(|_, working| {
            u64::try_from(working.len())
                .map_err(|_| io::Error::other("simulated file is too large"))
        })
    }

    async fn set_len(&self, len: u64) -> io::Result<()> {
        self.effect(false).await?;
        let len = usize::try_from(len)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file length is too large"))?;
        self.with_working(|_, working| {
            working.resize(len, 0);
            Ok(())
        })
    }

    async fn allocate(&self, len: u64) -> io::Result<()> {
        self.effect(false).await?;
        self.with_working(|state, working| {
            if len > working.len() as u64 {
                return Err(Shared::record_out_of_bounds(state));
            }
            Ok(())
        })
    }

    async fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        self.effect(false).await?;
        self.with_working(|state, working| {
            let start = usize::try_from(offset).map_err(|_| Shared::record_out_of_bounds(state))?;
            let end = start
                .checked_add(bytes.len())
                .ok_or_else(|| Shared::record_out_of_bounds(state))?;
            let source = working
                .get(start..end)
                .ok_or_else(|| Shared::record_out_of_bounds(state))?;
            bytes.copy_from_slice(source);
            Ok(())
        })
    }

    async fn write_all_at(&self, bytes: &[u8], offset: u64) -> io::Result<()> {
        let effect = self.effect(true).await?;
        self.with_working(|state, working| {
            let start = usize::try_from(offset).map_err(|_| Shared::record_out_of_bounds(state))?;
            let end = start
                .checked_add(bytes.len())
                .ok_or_else(|| Shared::record_out_of_bounds(state))?;
            let destination = working
                .get_mut(start..end)
                .ok_or_else(|| Shared::record_out_of_bounds(state))?;
            if matches!(effect, OperationEffect::PartialWrite) {
                let written = if bytes.len() <= 1 {
                    bytes.len()
                } else {
                    let mut tape = self.shared.tape.lock().unwrap();
                    1 + tape.below((bytes.len() - 1) as u64) as usize
                };
                destination[..written].copy_from_slice(&bytes[..written]);
                return Err(io::Error::other(
                    "simulated partial cache-media write failure",
                ));
            }
            destination.copy_from_slice(bytes);
            Ok(())
        })
    }

    async fn sync_data(&self) -> io::Result<()> {
        self.effect(false).await?;
        self.with_working(|state, working| {
            state.durable.clone_from(working);
            Ok(())
        })
    }

    async fn sync_all(&self) -> io::Result<()> {
        self.effect(false).await?;
        self.with_working(|state, working| {
            state.durable.clone_from(working);
            state.durable_exists = true;
            Ok(())
        })
    }
}

fn injected_error() -> io::Error {
    io::Error::other("simulated cache-media operation failed before taking effect")
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn open(media: &SimMedia) -> Arc<dyn CacheFile> {
        media.open_exclusive(Path::new("ignored")).await.unwrap()
    }

    #[tokio::test]
    async fn sync_data_does_not_make_creation_durable() {
        let media = SimMedia::new(MediaFaultProfile::Healthy, vec![255], 1);
        let file = open(&media).await;
        file.set_len(4).await.unwrap();
        file.write_all_at(b"body", 0).await.unwrap();
        file.sync_data().await.unwrap();
        drop(file);

        media.crash();

        assert_eq!(media.durable_bytes(), None);
    }

    #[tokio::test]
    async fn sync_all_preserves_bytes_across_crash() {
        let media = SimMedia::new(MediaFaultProfile::Healthy, Vec::new(), 1);
        let file = open(&media).await;
        file.set_len(4).await.unwrap();
        file.write_all_at(b"body", 0).await.unwrap();
        file.sync_all().await.unwrap();
        drop(file);

        media.crash();
        let reopened = open(&media).await;
        let mut body = [0; 4];
        reopened.read_exact_at(&mut body, 0).await.unwrap();

        assert_eq!(&body, b"body");
        assert_eq!(media.out_of_bounds_accesses(), 0);
    }

    #[tokio::test]
    async fn crash_can_preserve_bytes_independently_after_sync() {
        let media = SimMedia::new(MediaFaultProfile::Healthy, vec![0, 255, 0, 255], 1);
        let file = open(&media).await;
        file.set_len(4).await.unwrap();
        file.write_all_at(b"old!", 0).await.unwrap();
        file.sync_all().await.unwrap();
        file.write_all_at(b"new!", 0).await.unwrap();
        drop(file);

        media.crash();
        let reopened = open(&media).await;
        let mut body = [0; 4];
        reopened.read_exact_at(&mut body, 0).await.unwrap();

        assert_eq!(&body, b"nlw!");
    }

    #[tokio::test]
    async fn detach_invalidates_handles_after_reattach() {
        let media = SimMedia::new(MediaFaultProfile::Healthy, Vec::new(), 1);
        let file = open(&media).await;
        media.detach();
        media.reattach();

        assert_eq!(
            file.len().await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert!(open(&media).await.len().await.is_ok());
    }

    #[tokio::test]
    async fn corruption_and_permanent_failure_are_independent_controls() {
        let media = SimMedia::new(MediaFaultProfile::Healthy, Vec::new(), 1);
        let file = open(&media).await;
        file.set_len(4).await.unwrap();
        file.write_all_at(b"body", 0).await.unwrap();
        file.sync_all().await.unwrap();
        drop(file);

        assert!(media.corrupt(1, 0xff));
        media.crash();
        let reopened = open(&media).await;
        let mut body = [0; 4];
        reopened.read_exact_at(&mut body, 0).await.unwrap();
        assert_ne!(&body, b"body");
        drop(reopened);

        media.make_permanently_unavailable();
        media.reattach();
        assert_eq!(
            media
                .open_exclusive(Path::new("ignored"))
                .await
                .err()
                .expect("permanently unavailable media unexpectedly opened")
                .kind(),
            io::ErrorKind::NotConnected
        );
    }

    #[tokio::test]
    async fn full_profile_can_fail_after_a_partial_write() {
        let media = SimMedia::new(MediaFaultProfile::Full, vec![0, 0, 248, 0, 0], 1);
        let file = open(&media).await;
        file.set_len(4).await.unwrap();
        let error = file.write_all_at(b"body", 0).await.unwrap_err();
        let mut actual = [0; 4];
        file.read_exact_at(&mut actual, 0).await.unwrap();

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(actual[0], b'b');
        assert_ne!(&actual, b"body");
    }

    #[tokio::test]
    async fn selected_profile_can_fail_without_an_effect() {
        let media = SimMedia::new(MediaFaultProfile::Selected, vec![0, 0, 255, 0], 1);
        let file = open(&media).await;
        file.set_len(4).await.unwrap();
        assert!(file.write_all_at(b"body", 0).await.is_err());
        let mut actual = [1; 4];
        file.read_exact_at(&mut actual, 0).await.unwrap();

        assert_eq!(actual, [0; 4]);
    }

    #[tokio::test]
    async fn pending_operation_resolves_on_detach() {
        let media = SimMedia::new(MediaFaultProfile::Full, vec![0, 0, 252], 1);
        let file = open(&media).await;
        file.set_len(4).await.unwrap();
        let operation = tokio::spawn({
            let file = file.clone();
            async move { file.len().await }
        });
        tokio::task::yield_now().await;

        media.detach();

        assert_eq!(
            operation
                .await
                .unwrap()
                .expect_err("pending operation unexpectedly succeeded")
                .kind(),
            io::ErrorKind::NotConnected
        );
    }
}
