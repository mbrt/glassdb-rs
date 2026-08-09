//! Persistent-cache binary geometry and encoding.

use std::io;

use glassdb_data::DatabaseId;
use sha2::{Digest, Sha256};

use crate::timeline::SequencePoint;

use super::{Record, Slot};

const SLOT_BYTES: u64 = 40;
const RECORD_HEADER_BYTES: u64 = 48;
const RECORD_ALIGNMENT: u64 = 8;

#[derive(Clone, Copy)]
pub(super) struct CacheGeometry {
    magic: [u8; 8],
    format_version: u64,
    block_bytes: u64,
    minimum_record_bytes: u64,
    segment_bytes: u64,
    index_divisor: u64,
    minimum_segments: u64,
    identity_domain: &'static [u8],
    header_domain: &'static [u8],
    marker_domain: &'static [u8],
    path_domain: &'static [u8],
    record_domain: &'static [u8],
}

pub(super) const PRODUCTION_GEOMETRY: CacheGeometry = CacheGeometry {
    magic: *b"GLDBL2\0\0",
    format_version: 1,
    block_bytes: 4 * 1024,
    minimum_record_bytes: 4 * 1024,
    segment_bytes: 64 * 1024 * 1024,
    index_divisor: 64,
    minimum_segments: 2,
    identity_domain: b"glassdb-l2-identity-v1",
    header_domain: b"glassdb-l2-header-v1",
    marker_domain: b"glassdb-l2-clean-tail-v1",
    path_domain: b"glassdb-l2-path-v1",
    record_domain: b"glassdb-l2-record-v1",
};

#[cfg(any(test, feature = "sim"))]
pub(super) const COMPACT_GEOMETRY: CacheGeometry = CacheGeometry {
    magic: *b"GL2TEST\0",
    format_version: 1,
    block_bytes: 4 * 1024,
    minimum_record_bytes: 4 * 1024,
    segment_bytes: 256 * 1024,
    index_divisor: 64,
    minimum_segments: 2,
    identity_domain: b"glassdb-l2-identity-test-v1",
    header_domain: b"glassdb-l2-header-test-v1",
    marker_domain: b"glassdb-l2-clean-tail-test-v1",
    path_domain: b"glassdb-l2-path-test-v1",
    record_domain: b"glassdb-l2-record-test-v1",
};

#[derive(Clone, Copy, Debug)]
struct Layout {
    capacity: u64,
    metadata_bytes: u64,
    index_bytes: u64,
    data_offset: u64,
    segment_count: usize,
    ring_end: u64,
}

impl Layout {
    fn derive(capacity: u64, geometry: CacheGeometry) -> io::Result<Self> {
        if geometry.block_bytes < 4096
            || !geometry.block_bytes.is_power_of_two()
            || geometry.minimum_record_bytes < RECORD_HEADER_BYTES
            || !geometry
                .minimum_record_bytes
                .is_multiple_of(RECORD_ALIGNMENT)
            || geometry.segment_bytes <= geometry.block_bytes
            || !geometry.segment_bytes.is_multiple_of(geometry.block_bytes)
            || geometry.index_divisor == 0
            || geometry.minimum_segments < 2
            || geometry.block_bytes < SLOT_BYTES
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid persistent-cache geometry",
            ));
        }
        let capacity = CacheFormat::floor_to(capacity, geometry.block_bytes);
        let metadata_bytes = geometry
            .block_bytes
            .checked_mul(2)
            .ok_or_else(CacheFormat::overflow)?;
        let index_bytes =
            CacheFormat::floor_to(capacity / geometry.index_divisor, geometry.block_bytes);
        if index_bytes < geometry.block_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "persistent-cache capacity leaves no index bucket",
            ));
        }
        let data_offset = metadata_bytes
            .checked_add(index_bytes)
            .ok_or_else(CacheFormat::overflow)?;
        let data_bytes = capacity.checked_sub(data_offset).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "persistent-cache capacity is smaller than its metadata",
            )
        })?;
        let segment_count_u64 = data_bytes / geometry.segment_bytes;
        if segment_count_u64 < geometry.minimum_segments {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "persistent-cache capacity must hold at least two segments",
            ));
        }
        let segment_count = usize::try_from(segment_count_u64).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "persistent-cache segment count does not fit in memory",
            )
        })?;
        let ring_end = data_offset
            .checked_add(
                segment_count_u64
                    .checked_mul(geometry.segment_bytes)
                    .ok_or_else(CacheFormat::overflow)?,
            )
            .ok_or_else(CacheFormat::overflow)?;
        Ok(Self {
            capacity,
            metadata_bytes,
            index_bytes,
            data_offset,
            segment_count,
            ring_end,
        })
    }
}

/// The complete binary-format context for one persistent-cache container.
pub(super) struct CacheFormat {
    geometry: CacheGeometry,
    layout: Layout,
    identity: [u8; 32],
}

impl CacheFormat {
    pub(super) const SLOT_BYTES: u64 = SLOT_BYTES;
    pub(super) const RECORD_ALIGNMENT: u64 = RECORD_ALIGNMENT;

    pub(super) fn new(
        geometry: CacheGeometry,
        capacity: u64,
        database_name: &str,
        database_id: DatabaseId,
    ) -> io::Result<Self> {
        Ok(Self {
            geometry,
            layout: Layout::derive(capacity, geometry)?,
            identity: Self::identity_digest(geometry, database_name, database_id)?,
        })
    }

    pub(super) fn capacity(&self) -> u64 {
        self.layout.capacity
    }

    pub(super) fn block_bytes(&self) -> u64 {
        self.geometry.block_bytes
    }

    pub(super) fn minimum_record_bytes(&self) -> u64 {
        self.geometry.minimum_record_bytes
    }

    pub(super) fn segment_bytes(&self) -> u64 {
        self.geometry.segment_bytes
    }

    pub(super) fn maximum_record_bytes(&self) -> u64 {
        self.geometry.segment_bytes - self.geometry.block_bytes
    }

    pub(super) fn metadata_bytes(&self) -> u64 {
        self.layout.metadata_bytes
    }

    pub(super) fn data_offset(&self) -> u64 {
        self.layout.data_offset
    }

    pub(super) fn segment_count(&self) -> usize {
        self.layout.segment_count
    }

    pub(super) fn ring_end(&self) -> u64 {
        self.layout.ring_end
    }

    pub(super) fn bucket_count(&self) -> u64 {
        self.layout.index_bytes / self.geometry.block_bytes
    }

    pub(super) fn segment_start(&self, index: usize) -> u64 {
        self.layout.data_offset + index as u64 * self.geometry.segment_bytes
    }

    pub(super) fn record_bytes(&self, revision_bytes: usize, body_bytes: usize) -> Option<u64> {
        u32::try_from(revision_bytes).ok()?;
        u32::try_from(body_bytes).ok()?;
        let content = RECORD_HEADER_BYTES
            .checked_add(revision_bytes as u64)?
            .checked_add(body_bytes as u64)?;
        Some(Self::align_up(content, RECORD_ALIGNMENT)?.max(self.geometry.minimum_record_bytes))
    }

    pub(super) fn path_fingerprint(&self, path: &str) -> u64 {
        let mut digest = Sha256::new();
        digest.update(self.geometry.path_domain);
        digest.update(self.identity);
        digest.update((path.len() as u32).to_le_bytes());
        digest.update(path.as_bytes());
        let digest = digest.finalize();
        u64::from_le_bytes(digest[..8].try_into().unwrap())
    }

    pub(super) fn encode_file_header(&self) -> Vec<u8> {
        let mut header = vec![0; self.geometry.block_bytes as usize];
        header[0..8].copy_from_slice(&self.geometry.magic);
        header[8..16].copy_from_slice(&self.geometry.format_version.to_le_bytes());
        header[16..48].copy_from_slice(&self.identity);
        let digest = self.header_digest(&header[..48]);
        header[48..80].copy_from_slice(&digest);
        header
    }

    pub(super) fn file_header_valid(&self, header: &[u8]) -> bool {
        if header.len() < 80
            || header[0..8] != self.geometry.magic
            || u64::from_le_bytes(header[8..16].try_into().unwrap()) != self.geometry.format_version
            || header[16..48] != self.identity
        {
            return false;
        }
        header[48..80] == self.header_digest(&header[..48])
    }

    pub(super) fn encode_segment_header(&self, generation: u64) -> Vec<u8> {
        let mut header = vec![0; self.geometry.block_bytes as usize];
        header[0..8].copy_from_slice(&generation.to_le_bytes());
        header[8..16].copy_from_slice(&(!generation).to_le_bytes());
        header
    }

    pub(super) fn decode_segment_generation(&self, header: &[u8]) -> Option<u64> {
        let generation = u64::from_le_bytes(header.get(0..8)?.try_into().ok()?);
        let complement = u64::from_le_bytes(header.get(8..16)?.try_into().ok()?);
        (generation != 0 && complement == !generation).then_some(generation)
    }

    pub(super) fn encode_clean_tail(&self, tail: Option<(u64, u64)>) -> Vec<u8> {
        let mut marker = vec![0; self.geometry.block_bytes as usize];
        if let Some((generation, append_offset)) = tail {
            marker[0..8].copy_from_slice(&generation.to_le_bytes());
            marker[8..16].copy_from_slice(&append_offset.to_le_bytes());
            let digest = self.marker_digest(&marker[..16]);
            marker[16..48].copy_from_slice(&digest);
        }
        marker
    }

    pub(super) fn decode_clean_tail(&self, marker: &[u8]) -> Option<(u64, u64)> {
        if marker.len() < 48 || marker[16..48] != self.marker_digest(&marker[..16]) {
            return None;
        }
        let generation = u64::from_le_bytes(marker[0..8].try_into().unwrap());
        let append_offset = u64::from_le_bytes(marker[8..16].try_into().unwrap());
        Some((generation, append_offset))
    }

    pub(super) fn encode_record(
        &self,
        path: &str,
        revision: &[u8],
        body: &[u8],
        current_after: SequencePoint,
    ) -> io::Result<Vec<u8>> {
        let record_bytes = self
            .record_bytes(revision.len(), body.len())
            .ok_or_else(Self::invalid_record)?;
        let mut record = vec![0; record_bytes as usize];
        record[0..4].copy_from_slice(&(revision.len() as u32).to_le_bytes());
        record[4..8].copy_from_slice(&(body.len() as u32).to_le_bytes());
        record[8..16].copy_from_slice(&current_after.raw().to_le_bytes());
        let digest = self.record_digest(path, &record[..16], revision, body)?;
        record[16..48].copy_from_slice(&digest);
        let revision_end = RECORD_HEADER_BYTES as usize + revision.len();
        record[RECORD_HEADER_BYTES as usize..revision_end].copy_from_slice(revision);
        record[revision_end..revision_end + body.len()].copy_from_slice(body);
        Ok(record)
    }

    pub(super) fn decode_record(&self, path: &str, bytes: &[u8]) -> io::Result<Record> {
        if bytes.len() < RECORD_HEADER_BYTES as usize {
            return Err(Self::invalid_record());
        }
        let revision_bytes = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let body_bytes = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
        let current_after =
            SequencePoint::from_raw(u64::from_le_bytes(bytes[8..16].try_into().unwrap()));
        let expected_record_bytes = self
            .record_bytes(revision_bytes, body_bytes)
            .ok_or_else(Self::invalid_record)?;
        if expected_record_bytes != bytes.len() as u64 {
            return Err(Self::invalid_record());
        }
        let content_end = (RECORD_HEADER_BYTES as usize)
            .checked_add(revision_bytes)
            .and_then(|end| end.checked_add(body_bytes))
            .filter(|end| *end <= bytes.len())
            .ok_or_else(Self::invalid_record)?;
        let revision_end = RECORD_HEADER_BYTES as usize + revision_bytes;
        let revision = &bytes[RECORD_HEADER_BYTES as usize..revision_end];
        let body = &bytes[revision_end..content_end];
        let expected_digest = self.record_digest(path, &bytes[..16], revision, body)?;
        if bytes[16..48] != expected_digest {
            return Err(Self::invalid_record());
        }
        Ok(Record {
            revision: revision.to_vec(),
            body: body.to_vec(),
            current_after,
        })
    }

    pub(super) fn encode_slot(&self, slot: Slot) -> [u8; SLOT_BYTES as usize] {
        let mut bytes = [0; SLOT_BYTES as usize];
        bytes[0..8].copy_from_slice(&slot.fingerprint.to_le_bytes());
        bytes[8..16].copy_from_slice(&slot.generation.to_le_bytes());
        bytes[16..24].copy_from_slice(&slot.record_offset.to_le_bytes());
        bytes[24..32].copy_from_slice(&slot.record_bytes.to_le_bytes());
        bytes[32..40].copy_from_slice(&slot.current_after.raw().to_le_bytes());
        bytes
    }

    pub(super) fn decode_slot(&self, bytes: &[u8]) -> Slot {
        Slot {
            fingerprint: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            generation: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            record_offset: u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            record_bytes: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            current_after: SequencePoint::from_raw(u64::from_le_bytes(
                bytes[32..40].try_into().unwrap(),
            )),
        }
    }

    pub(super) fn overflow() -> io::Error {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "persistent-cache geometry overflows u64",
        )
    }

    pub(super) fn invalid_record() -> io::Error {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid persistent-cache record",
        )
    }

    fn identity_digest(
        geometry: CacheGeometry,
        database_name: &str,
        database_id: DatabaseId,
    ) -> io::Result<[u8; 32]> {
        let name_len = u32::try_from(database_name.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "database name is too long for persistent-cache identity",
            )
        })?;
        let mut digest = Sha256::new();
        digest.update(geometry.identity_domain);
        digest.update(name_len.to_le_bytes());
        digest.update(database_name.as_bytes());
        digest.update(database_id.as_bytes());
        Ok(digest.finalize().into())
    }

    fn header_digest(&self, header: &[u8]) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(self.geometry.header_domain);
        digest.update(header);
        digest.update(self.layout.capacity.to_le_bytes());
        digest.finalize().into()
    }

    fn marker_digest(&self, marker: &[u8]) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(self.geometry.marker_domain);
        digest.update(self.identity);
        digest.update(self.layout.capacity.to_le_bytes());
        digest.update(marker);
        digest.finalize().into()
    }

    fn record_digest(
        &self,
        path: &str,
        header: &[u8],
        revision: &[u8],
        body: &[u8],
    ) -> io::Result<[u8; 32]> {
        let path_len = u32::try_from(path.len()).map_err(|_| Self::invalid_record())?;
        let mut digest = Sha256::new();
        digest.update(self.geometry.record_domain);
        digest.update(self.identity);
        digest.update(path_len.to_le_bytes());
        digest.update(path.as_bytes());
        digest.update(header);
        digest.update(revision);
        digest.update(body);
        Ok(digest.finalize().into())
    }

    fn align_up(value: u64, alignment: u64) -> Option<u64> {
        value
            .checked_add(alignment.checked_sub(1)?)
            .map(|value| value / alignment * alignment)
    }

    fn floor_to(value: u64, alignment: u64) -> u64 {
        value / alignment * alignment
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(byte: u8) -> DatabaseId {
        DatabaseId::from_bytes([byte; 16])
    }

    #[test]
    fn production_minimum_is_131_mib() {
        assert!(Layout::derive(130 * 1024 * 1024, PRODUCTION_GEOMETRY).is_err());
        assert!(Layout::derive(131 * 1024 * 1024, PRODUCTION_GEOMETRY).is_ok());
    }

    #[test]
    fn test_format_is_not_a_production_file() {
        assert_ne!(COMPACT_GEOMETRY.magic, PRODUCTION_GEOMETRY.magic);
        assert_ne!(
            COMPACT_GEOMETRY.header_domain,
            PRODUCTION_GEOMETRY.header_domain
        );
    }

    #[test]
    fn record_size_is_charged_and_aligned() {
        let format = CacheFormat::new(COMPACT_GEOMETRY, 2 * 1024 * 1024, "db", id(1)).unwrap();
        assert_eq!(format.record_bytes(2, 4), Some(4096));
        assert_eq!(format.record_bytes(2, 4097), Some(4152));
    }
}
