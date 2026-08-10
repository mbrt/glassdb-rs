//! Isolated deterministic fault harness for the persistent cache.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use glassdb_concurr::rt;
use glassdb_data::DatabaseId;
use sha2::{Digest, Sha256};

use super::sim_media::{MediaFaultProfile, SimMedia};
use super::{PathFence, PersistentCache, PersistentCacheConfig, SequencePoint};

const CAPACITY_BYTES: u64 = 2 * 1024 * 1024;
const MAX_COMMANDS: usize = 48;
const PATH_COUNT: u8 = 4;
const MAX_BODY_BYTES: usize = 32 * 1024;

/// Observable trace emitted by the isolated persistent-cache simulation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiskCacheEvent {
    /// Result of opening one database identity.
    Opened {
        identity: u8,
        enabled: bool,
        last_sequence_point: Option<u64>,
    },
    /// Result of looking up one modeled path.
    Lookup {
        identity: u8,
        path: u8,
        record_digest: Option<[u8; 32]>,
    },
    /// Cache state after an operation that does not return a record.
    State { operation: u8, enabled: bool },
}

#[derive(Clone)]
struct KnownRecord {
    revision: Vec<u8>,
    body: Vec<u8>,
    sequence_point: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum CommandKind {
    Open = 0,
    Replace = 1,
    Lookup = 2,
    Invalidate = 3,
    Close = 4,
    Crash = 5,
    Detach = 6,
    Reattach = 7,
    Corrupt = 8,
    MakePermanentlyUnavailable = 9,
}

impl CommandKind {
    const fn from_byte(byte: u8) -> Self {
        match byte % 10 {
            0 => Self::Open,
            1 => Self::Replace,
            2 => Self::Lookup,
            3 => Self::Invalidate,
            4 => Self::Close,
            5 => Self::Crash,
            6 => Self::Detach,
            7 => Self::Reattach,
            8 => Self::Corrupt,
            _ => Self::MakePermanentlyUnavailable,
        }
    }

    const fn operation_code(self) -> u8 {
        self as u8
    }
}

#[derive(Clone, Copy)]
struct Command {
    kind: CommandKind,
    path: u8,
    identity: u8,
    argument: u16,
    pattern: u8,
}

struct DecodedInput {
    seed: u64,
    commands: Vec<Command>,
    schedule_tape: Vec<u8>,
    media_tape: Vec<u8>,
}

struct HarnessState {
    cache: Option<PersistentCache>,
    media: SimMedia,
    identity: u8,
    next_sequence_point: u64,
    known_records: HashMap<(u8, u8), Vec<KnownRecord>>,
    events: Vec<DiskCacheEvent>,
}

impl HarnessState {
    fn new(seed: u64, media_tape: Vec<u8>, event_capacity: usize) -> Self {
        Self {
            cache: None,
            media: SimMedia::new(MediaFaultProfile::Full, media_tape, seed ^ 0xD15C_CA4E),
            identity: 0,
            next_sequence_point: 1,
            known_records: HashMap::new(),
            events: Vec::with_capacity(event_capacity),
        }
    }

    async fn dispatch(&mut self, command: Command) {
        match command.kind {
            CommandKind::Open => self.handle_open(command).await,
            CommandKind::Replace => self.handle_replace(command),
            CommandKind::Lookup => self.handle_lookup(command).await,
            CommandKind::Invalidate => self.handle_invalidate(command),
            CommandKind::Close => self.handle_close(command).await,
            CommandKind::Crash => self.handle_crash(command).await,
            CommandKind::Detach => self.handle_detach(command),
            CommandKind::Reattach => self.handle_reattach(command),
            CommandKind::Corrupt => self.handle_corrupt(command),
            CommandKind::MakePermanentlyUnavailable => {
                self.handle_make_permanently_unavailable(command);
            }
        }
    }

    async fn handle_open(&mut self, command: Command) {
        self.close_cache().await;
        self.identity = command.identity;
        let opened = PersistentCache::open(
            config(),
            "db",
            database_id(self.identity),
            Some(self.media.clone().into()),
        )
        .await;
        let recovered = opened.last_sequence_point.map(SequencePoint::raw);
        if let Some(point) = recovered {
            assert!(
                self.known_records
                    .iter()
                    .any(|((known_identity, _), records)| {
                        *known_identity == self.identity
                            && records.iter().any(|record| record.sequence_point == point)
                    }),
                "cache recovered a sequence point that was never admitted"
            );
        }
        self.events.push(DiskCacheEvent::Opened {
            identity: self.identity,
            enabled: opened.cache.is_enabled(),
            last_sequence_point: recovered,
        });
        self.cache = Some(opened.cache);
    }

    fn handle_replace(&mut self, command: Command) {
        if let Some(current) = self.cache.as_ref()
            && let Some(guard) = current.begin_fence(Arc::new(PathFence::default()))
        {
            let sequence_point = self.next_sequence_point;
            self.next_sequence_point = self.next_sequence_point.saturating_add(1);
            let path = path(command.path);
            let revision = revision(sequence_point, command.pattern);
            let body = body(command.argument, command.pattern);
            self.known_records
                .entry((self.identity, command.path))
                .or_default()
                .push(KnownRecord {
                    revision: revision.clone(),
                    body: body.clone(),
                    sequence_point,
                });
            current.replace(
                path,
                revision,
                body,
                SequencePoint::from_raw(sequence_point),
                guard,
            );
        }
        self.push_state(command.kind);
    }

    async fn handle_lookup(&mut self, command: Command) {
        let record = if let Some(current) = self.cache.as_ref() {
            match rt::timeout(Duration::from_secs(6), current.lookup(path(command.path))).await {
                Ok(record) => record,
                Err(_) => {
                    current.disable_slow_lookup();
                    None
                }
            }
        } else {
            None
        };
        let record_digest = record.map(|record| {
            let valid = self
                .known_records
                .get(&(self.identity, command.path))
                .is_some_and(|records| {
                    records.iter().any(|known| {
                        known.revision == record.revision
                            && known.body == record.body
                            && known.sequence_point == record.current_after.raw()
                    })
                });
            assert!(valid, "cache returned a fabricated or mixed record");
            digest_record(&record.revision, &record.body, record.current_after.raw())
        });
        self.events.push(DiskCacheEvent::Lookup {
            identity: self.identity,
            path: command.path,
            record_digest,
        });
    }

    fn handle_invalidate(&mut self, command: Command) {
        if let Some(current) = self.cache.as_ref()
            && let Some(guard) = current.begin_fence(Arc::new(PathFence::default()))
        {
            current.invalidate(path(command.path), guard);
        }
        self.push_state(command.kind);
    }

    async fn handle_close(&mut self, command: Command) {
        self.close_cache().await;
        self.push_state(command.kind);
    }

    async fn handle_crash(&mut self, command: Command) {
        self.cache = None;
        self.media.crash();
        rt::yield_now().await;
        self.push_state(command.kind);
    }

    fn handle_detach(&mut self, command: Command) {
        self.media.detach();
        self.push_state(command.kind);
    }

    fn handle_reattach(&mut self, command: Command) {
        self.media.reattach();
        self.push_state(command.kind);
    }

    fn handle_corrupt(&mut self, command: Command) {
        let durable_len = self
            .media
            .durable_bytes()
            .map_or(0, |bytes| bytes.len() as u64);
        if durable_len != 0 {
            let scaled = u64::from(command.argument)
                .wrapping_mul(257)
                .wrapping_add(u64::from(command.path));
            let _ = self
                .media
                .corrupt(scaled % durable_len, command.pattern | 1);
        }
        self.push_state(command.kind);
    }

    fn handle_make_permanently_unavailable(&mut self, command: Command) {
        self.media.make_permanently_unavailable();
        self.push_state(command.kind);
    }

    async fn close_cache(&mut self) {
        if let Some(current) = self.cache.take() {
            current.shutdown().await;
            drop(current);
            rt::yield_now().await;
        }
    }

    fn push_state(&mut self, kind: CommandKind) {
        self.events.push(DiskCacheEvent::State {
            operation: kind.operation_code(),
            enabled: self.cache.as_ref().is_some_and(PersistentCache::is_enabled),
        });
    }
}

/// Runs one cache-only fuzz input and panics on a safety-oracle violation.
pub fn replay_disk_cache_input(data: &[u8]) {
    let _ = record_disk_cache_input(data);
}

/// Runs one cache-only fuzz input and returns its deterministic observable trace.
pub fn record_disk_cache_input(data: &[u8]) -> Vec<DiskCacheEvent> {
    let decoded = decode(data);
    rt::block_on_with(
        rt::TapeScheduler::new(decoded.schedule_tape),
        decoded.seed,
        run(decoded.seed, decoded.commands, decoded.media_tape),
    )
}

async fn run(seed: u64, commands: Vec<Command>, media_tape: Vec<u8>) -> Vec<DiskCacheEvent> {
    let mut state = HarnessState::new(seed, media_tape, commands.len());
    for command in commands {
        state.dispatch(command).await;
    }

    state.close_cache().await;
    assert_eq!(
        state.media.out_of_bounds_accesses(),
        0,
        "cache accessed outside its preallocated container"
    );
    state.events
}

fn decode(data: &[u8]) -> DecodedInput {
    let mut seed_bytes = [0; 8];
    let seed_len = data.len().min(seed_bytes.len());
    seed_bytes[..seed_len].copy_from_slice(&data[..seed_len]);
    let seed = u64::from_le_bytes(seed_bytes);
    let rest = data.get(seed_len..).unwrap_or_default();
    let third = rest.len() / 3;
    let command_tape = &rest[..third];
    let schedule_tape = rest[third..third * 2].to_vec();
    let media_tape = rest[third * 2..].to_vec();

    let requested = command_tape
        .first()
        .map_or(0, |count| usize::from(*count) % (MAX_COMMANDS + 1));
    let mut commands = Vec::with_capacity(requested);
    for bytes in command_tape[command_tape.len().min(1)..].chunks_exact(6) {
        if commands.len() == requested {
            break;
        }
        commands.push(Command {
            kind: CommandKind::from_byte(bytes[0]),
            path: bytes[1] % PATH_COUNT,
            identity: bytes[2],
            argument: u16::from_le_bytes([bytes[3], bytes[4]]),
            pattern: bytes[5],
        });
    }
    DecodedInput {
        seed,
        commands,
        schedule_tape,
        media_tape,
    }
}

fn config() -> PersistentCacheConfig {
    PersistentCacheConfig {
        directory: PathBuf::from("simulated-cache"),
        capacity_bytes: CAPACITY_BYTES,
    }
}

fn database_id(identity: u8) -> DatabaseId {
    DatabaseId::from_bytes([identity; 16])
}

fn path(index: u8) -> Arc<str> {
    Arc::from(format!("db/object-{index}"))
}

fn revision(sequence_point: u64, pattern: u8) -> Vec<u8> {
    let mut revision = sequence_point.to_le_bytes().to_vec();
    revision.push(pattern);
    revision
}

fn body(argument: u16, pattern: u8) -> Vec<u8> {
    let len = usize::from(argument) % (MAX_BODY_BYTES + 1);
    (0..len)
        .map(|index| pattern.wrapping_add(index as u8))
        .collect()
}

fn digest_record(revision: &[u8], body: &[u8], sequence_point: u64) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update((revision.len() as u64).to_le_bytes());
    digest.update(revision);
    digest.update((body.len() as u64).to_le_bytes());
    digest.update(body);
    digest.update(sequence_point.to_le_bytes());
    digest.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::CommandKind;

    #[test]
    fn command_kind_preserves_byte_mapping() {
        const KINDS: [CommandKind; 10] = [
            CommandKind::Open,
            CommandKind::Replace,
            CommandKind::Lookup,
            CommandKind::Invalidate,
            CommandKind::Close,
            CommandKind::Crash,
            CommandKind::Detach,
            CommandKind::Reattach,
            CommandKind::Corrupt,
            CommandKind::MakePermanentlyUnavailable,
        ];

        for byte in u8::MIN..=u8::MAX {
            let kind = CommandKind::from_byte(byte);
            assert_eq!(kind, KINDS[usize::from(byte % 10)], "byte {byte}");
            assert_eq!(kind.operation_code(), byte % 10, "byte {byte}");
        }
    }
}
