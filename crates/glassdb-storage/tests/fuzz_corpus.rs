//! Replays the committed persistent-cache corpus through its storage-layer harness.
#![cfg(all(sim, feature = "sim"))]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use glassdb_storage::sim::disk_cache::{DiskCacheEvent, record_disk_cache_input};
use rayon::prelude::*;
use sha2::{Digest, Sha256};

const TRACE_DIGEST_DOMAIN: &[u8] = b"glassdb-storage/disk-cache-trace/v1\0";
const TRACE_DIGESTS: &str = include_str!("disk_cache_trace_digests.txt");

fn corpus_paths() -> Result<Vec<PathBuf>, String> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus/disk_cache");
    let entries = std::fs::read_dir(&dir)
        .map_err(|error| format!("read disk-cache corpus dir {}: {error}", dir.display()))?;
    let mut paths = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|error| format!("read disk-cache corpus entry: {error}"))?
            .path();
        if path.is_file() {
            paths.push(path);
        }
    }
    paths.sort_unstable();
    if paths.is_empty() {
        return Err(format!(
            "no disk-cache corpus files under {}",
            dir.display()
        ));
    }
    Ok(paths)
}

fn parse_trace_digests() -> Result<BTreeMap<&'static str, &'static str>, String> {
    let mut digests = BTreeMap::new();
    for (index, line) in TRACE_DIGESTS.lines().enumerate() {
        let line_number = index + 1;
        let mut fields = line.split_ascii_whitespace();
        let Some(name) = fields.next() else {
            return Err(format!("empty trace digest line {line_number}"));
        };
        let Some(digest) = fields.next() else {
            return Err(format!("missing digest on line {line_number}"));
        };
        if fields.next().is_some() {
            return Err(format!("extra fields on trace digest line {line_number}"));
        }
        if name.len() != 40 || !name.bytes().all(is_lower_hex_digit) {
            return Err(format!(
                "invalid corpus filename on trace digest line {line_number}: {name}"
            ));
        }
        if digest.len() != 64 || !digest.bytes().all(is_lower_hex_digit) {
            return Err(format!(
                "invalid SHA-256 digest on trace digest line {line_number}: {digest}"
            ));
        }
        if digests.insert(name, digest).is_some() {
            return Err(format!(
                "duplicate corpus filename on line {line_number}: {name}"
            ));
        }
    }
    if digests.is_empty() {
        return Err("disk-cache trace digest manifest is empty".to_owned());
    }
    Ok(digests)
}

fn is_lower_hex_digit(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
}

fn corpus_name(path: &Path) -> Result<&str, String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("corpus path has no UTF-8 filename: {}", path.display()))
}

fn replay_corpus_file(path: &Path, expected_digest: &str) -> Result<(), String> {
    let data = std::fs::read(path)
        .map_err(|error| format!("read corpus file {}: {error}", path.display()))?;
    let first = std::panic::catch_unwind(|| record_disk_cache_input(&data))
        .map_err(|_| format!("corpus replay failed for {}", path.display()))?;
    let second = std::panic::catch_unwind(|| record_disk_cache_input(&data))
        .map_err(|_| format!("second corpus replay failed for {}", path.display()))?;
    if first != second {
        return Err(format!(
            "corpus replay diverged for {}\n  run 1: {first:?}\n  run 2: {second:?}",
            path.display()
        ));
    }

    let actual_digest = trace_digest(&first);
    if actual_digest != expected_digest {
        return Err(format!(
            "corpus trace changed for {}\n  expected: {expected_digest}\n    actual: {actual_digest}",
            path.display()
        ));
    }
    Ok(())
}

fn trace_digest(events: &[DiskCacheEvent]) -> String {
    let mut digest = Sha256::new();
    digest.update(TRACE_DIGEST_DOMAIN);
    digest.update((events.len() as u64).to_le_bytes());
    for event in events {
        match event {
            DiskCacheEvent::Opened {
                identity,
                enabled,
                last_sequence_point,
            } => {
                digest.update([0, *identity, u8::from(*enabled)]);
                match last_sequence_point {
                    Some(sequence_point) => {
                        digest.update([1]);
                        digest.update(sequence_point.to_le_bytes());
                    }
                    None => digest.update([0]),
                }
            }
            DiskCacheEvent::Lookup {
                identity,
                path,
                record_digest,
            } => {
                digest.update([1, *identity, *path]);
                match record_digest {
                    Some(record_digest) => {
                        digest.update([1]);
                        digest.update(record_digest);
                    }
                    None => digest.update([0]),
                }
            }
            DiskCacheEvent::State { operation, enabled } => {
                digest.update([2, *operation, u8::from(*enabled)]);
            }
        }
    }

    hex(&digest.finalize())
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[test]
fn replays_committed_disk_cache_corpus() {
    let paths = corpus_paths().unwrap_or_else(|error| panic!("{error}"));
    let expected = parse_trace_digests().unwrap_or_else(|error| panic!("{error}"));
    let corpus_names = paths
        .iter()
        .map(|path| corpus_name(path).map(str::to_owned))
        .collect::<Result<BTreeSet<_>, _>>()
        .unwrap_or_else(|error| panic!("{error}"));
    let expected_names = expected
        .keys()
        .map(|name| (*name).to_owned())
        .collect::<BTreeSet<_>>();
    let missing = corpus_names
        .difference(&expected_names)
        .cloned()
        .collect::<Vec<_>>();
    let stale = expected_names
        .difference(&corpus_names)
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty() && stale.is_empty(),
        "disk-cache trace digest manifest does not match the corpus\n  missing: {missing:?}\n    stale: {stale:?}"
    );

    if let Err(error) = paths.par_iter().try_for_each(|path| {
        let name = corpus_name(path)?;
        replay_corpus_file(path, expected[name])
    }) {
        panic!("{error}");
    }
}
