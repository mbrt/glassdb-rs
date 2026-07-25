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

#[derive(Clone, Copy)]
struct Command {
    operation: u8,
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
    let media = SimMedia::new(MediaFaultProfile::Full, media_tape, seed ^ 0xD15C_CA4E);
    let mut cache = None;
    let mut identity = 0;
    let mut next_sequence_point = 1u64;
    let mut known: HashMap<(u8, u8), Vec<KnownRecord>> = HashMap::new();
    let mut events = Vec::with_capacity(commands.len());

    for command in commands {
        match command.operation {
            0 => {
                close(&mut cache).await;
                identity = command.identity;
                let opened = PersistentCache::open(
                    config(),
                    "db",
                    database_id(identity),
                    Some(media.clone().into()),
                )
                .await;
                let recovered = opened.last_sequence_point.map(SequencePoint::raw);
                if let Some(point) = recovered {
                    assert!(
                        known.iter().any(|((known_identity, _), records)| {
                            *known_identity == identity
                                && records.iter().any(|record| record.sequence_point == point)
                        }),
                        "cache recovered a sequence point that was never admitted"
                    );
                }
                events.push(DiskCacheEvent::Opened {
                    identity,
                    enabled: opened.cache.is_enabled(),
                    last_sequence_point: recovered,
                });
                cache = Some(opened.cache);
            }
            1 => {
                if let Some(current) = cache.as_ref()
                    && let Some(guard) =
                        current.begin_fence(Arc::new(PathFence::default()), Arc::new(()))
                {
                    let sequence_point = next_sequence_point;
                    next_sequence_point = next_sequence_point.saturating_add(1);
                    let path = path(command.path);
                    let revision = revision(sequence_point, command.pattern);
                    let body = body(command.argument, command.pattern);
                    known
                        .entry((identity, command.path))
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
                events.push(state_event(command.operation, cache.as_ref()));
            }
            2 => {
                let record = if let Some(current) = cache.as_ref() {
                    match rt::timeout(Duration::from_secs(6), current.lookup(path(command.path)))
                        .await
                    {
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
                    let valid = known.get(&(identity, command.path)).is_some_and(|records| {
                        records.iter().any(|known| {
                            known.revision == record.revision
                                && known.body == record.body
                                && known.sequence_point == record.current_after.raw()
                        })
                    });
                    assert!(valid, "cache returned a fabricated or mixed record");
                    digest_record(&record.revision, &record.body, record.current_after.raw())
                });
                events.push(DiskCacheEvent::Lookup {
                    identity,
                    path: command.path,
                    record_digest,
                });
            }
            3 => {
                if let Some(current) = cache.as_ref()
                    && let Some(guard) =
                        current.begin_fence(Arc::new(PathFence::default()), Arc::new(()))
                {
                    current.invalidate(path(command.path), guard);
                }
                events.push(state_event(command.operation, cache.as_ref()));
            }
            4 => {
                close(&mut cache).await;
                events.push(state_event(command.operation, cache.as_ref()));
            }
            5 => {
                cache = None;
                media.crash();
                rt::yield_now().await;
                events.push(state_event(command.operation, cache.as_ref()));
            }
            6 => {
                media.detach();
                events.push(state_event(command.operation, cache.as_ref()));
            }
            7 => {
                media.reattach();
                events.push(state_event(command.operation, cache.as_ref()));
            }
            8 => {
                let durable_len = media.durable_bytes().map_or(0, |bytes| bytes.len() as u64);
                if durable_len != 0 {
                    let scaled = u64::from(command.argument)
                        .wrapping_mul(257)
                        .wrapping_add(u64::from(command.path));
                    let _ = media.corrupt(scaled % durable_len, command.pattern | 1);
                }
                events.push(state_event(command.operation, cache.as_ref()));
            }
            _ => {
                media.make_permanently_unavailable();
                events.push(state_event(command.operation, cache.as_ref()));
            }
        }
    }

    close(&mut cache).await;
    assert_eq!(
        media.out_of_bounds_accesses(),
        0,
        "cache accessed outside its preallocated container"
    );
    events
}

async fn close(cache: &mut Option<PersistentCache>) {
    if let Some(current) = cache.take() {
        current.shutdown().await;
        drop(current);
        rt::yield_now().await;
    }
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
            operation: bytes[0] % 10,
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

fn state_event(operation: u8, cache: Option<&PersistentCache>) -> DiskCacheEvent {
    DiskCacheEvent::State {
        operation,
        enabled: cache.is_some_and(PersistentCache::is_enabled),
    }
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
