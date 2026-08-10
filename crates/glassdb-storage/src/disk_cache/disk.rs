//! Persistent-cache disk I/O and recovery mechanics.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use glassdb_data::DatabaseId;

use crate::cache_stats::CacheMetrics;
use crate::timeline::SequencePoint;

use super::admission::HitFilter;
use super::format::{CacheFormat, CacheGeometry};
use super::media::{CacheFile, CacheMedia};
use super::{CACHE_FILE, PersistentCacheConfig};

const INDEX_SCAN_BYTES: usize = 4 * 1024 * 1024;
const SYNC_BYTES: u64 = 64 * 1024 * 1024;

pub(super) struct Disk {
    pub(super) file: Arc<dyn CacheFile>,
    pub(super) format: CacheFormat,
    pub(super) segment_generations: Box<[AtomicU64]>,
    metrics: Arc<CacheMetrics>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Slot {
    pub(super) fingerprint: u64,
    pub(super) generation: u64,
    pub(super) record_offset: u64,
    pub(super) record_bytes: u64,
    pub(super) current_after: SequencePoint,
}

pub(super) struct Record {
    pub(super) revision: Vec<u8>,
    pub(super) body: Vec<u8>,
    pub(super) current_after: SequencePoint,
}

impl Disk {
    pub(super) async fn lookup(&self, path: &str) -> io::Result<Option<Record>> {
        let fingerprint = self.path_fingerprint(path);
        let mut slots = self.matching_slots(fingerprint).await?;
        slots.sort_unstable_by_key(|slot| std::cmp::Reverse(slot.generation));
        for slot in slots {
            match self.read_record(path, slot).await {
                Ok(Some(record)) => return Ok(Some(record)),
                Ok(None) => {}
                Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                    self.metrics.l2_error();
                    tracing::warn!(path, %error, "discarding corrupt persistent-cache record");
                }
                Err(error) => return Err(error),
            }
        }
        Ok(None)
    }

    pub(super) fn path_fingerprint(&self, path: &str) -> u64 {
        self.format.path_fingerprint(path)
    }

    pub(super) async fn current_slot(&self, path: &str) -> io::Result<Option<Slot>> {
        let fingerprint = self.path_fingerprint(path);
        let mut slots = self.matching_slots(fingerprint).await?;
        slots.sort_unstable_by_key(|slot| std::cmp::Reverse(slot.generation));
        for slot in slots {
            if self.read_record(path, slot).await?.is_some() {
                return Ok(Some(slot));
            }
        }
        Ok(None)
    }

    pub(super) async fn read_record(&self, path: &str, slot: Slot) -> io::Result<Option<Record>> {
        let Some(segment) = self.slot_range(slot) else {
            return Ok(None);
        };
        if self.segment_generations[segment].load(Ordering::Acquire) != slot.generation {
            return Ok(None);
        }
        let record_len =
            usize::try_from(slot.record_bytes).map_err(|_| CacheFormat::invalid_record())?;
        let mut bytes = vec![0; record_len];
        self.read_exact_at(&mut bytes, slot.record_offset).await?;
        if self.segment_generations[segment].load(Ordering::Acquire) != slot.generation {
            return Ok(None);
        }
        let record = self.format.decode_record(path, &bytes)?;
        if record.current_after != slot.current_after {
            return Err(CacheFormat::invalid_record());
        }
        Ok(Some(record))
    }

    async fn matching_slots(&self, fingerprint: u64) -> io::Result<Vec<Slot>> {
        let bucket = fingerprint % self.format.bucket_count();
        let offset = self.format.metadata_bytes() + bucket * self.format.block_bytes();
        let mut bytes = vec![0; self.format.block_bytes() as usize];
        self.read_exact_at(&mut bytes, offset).await?;
        let mut slots = Vec::new();
        for raw in bytes.chunks_exact(CacheFormat::SLOT_BYTES as usize) {
            let slot = self.format.decode_slot(raw);
            if slot.fingerprint == fingerprint
                && slot.generation != 0
                && self.slot_range(slot).is_some()
            {
                slots.push(slot);
            }
        }
        Ok(slots)
    }

    async fn last_sequence_point(&self) -> io::Result<Option<SequencePoint>> {
        let block_bytes =
            usize::try_from(self.format.block_bytes()).map_err(|_| CacheFormat::overflow())?;
        let scan_bytes = INDEX_SCAN_BYTES / block_bytes * block_bytes;
        let scan_bytes = scan_bytes.max(block_bytes);
        let mut bytes = vec![0; scan_bytes];
        let mut offset = self.format.metadata_bytes();
        let index_end = self.format.data_offset();
        let mut maximum = None;
        while offset < index_end {
            let remaining =
                usize::try_from(index_end - offset).map_err(|_| CacheFormat::overflow())?;
            let read_bytes = remaining.min(bytes.len());
            self.read_exact_at(&mut bytes[..read_bytes], offset).await?;
            for bucket in bytes[..read_bytes].chunks_exact(block_bytes) {
                for raw in bucket.chunks_exact(CacheFormat::SLOT_BYTES as usize) {
                    let slot = self.format.decode_slot(raw);
                    if slot.generation != 0 && self.slot_range(slot).is_some() {
                        maximum = Some(
                            maximum.map_or(slot.current_after, |current: SequencePoint| {
                                current.max(slot.current_after)
                            }),
                        );
                    }
                }
            }
            offset += read_bytes as u64;
        }
        if maximum.is_some_and(|point| point.raw() == u64::MAX) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "persistent-cache sequence point exhausted",
            ));
        }
        Ok(maximum)
    }

    fn slot_range(&self, slot: Slot) -> Option<usize> {
        if slot.current_after.raw() == 0
            || !slot
                .record_offset
                .is_multiple_of(CacheFormat::RECORD_ALIGNMENT)
            || slot.record_bytes < self.format.minimum_record_bytes()
            || !slot
                .record_bytes
                .is_multiple_of(CacheFormat::RECORD_ALIGNMENT)
            || slot.record_bytes > self.format.maximum_record_bytes()
            || slot.record_offset < self.format.data_offset()
        {
            return None;
        }
        let relative = slot.record_offset.checked_sub(self.format.data_offset())?;
        let segment = usize::try_from(relative / self.format.segment_bytes()).ok()?;
        if segment >= self.format.segment_count() {
            return None;
        }
        let start = self.format.segment_start(segment);
        let content_start = start.checked_add(self.format.block_bytes())?;
        let end = slot.record_offset.checked_add(slot.record_bytes)?;
        let segment_end = start.checked_add(self.format.segment_bytes())?;
        if slot.record_offset < content_start || end > segment_end || end > self.format.ring_end() {
            return None;
        }
        let generation = self.segment_generations[segment].load(Ordering::Acquire);
        (generation == slot.generation && generation != 0).then_some(segment)
    }

    async fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        self.file.read_exact_at(bytes, offset).await?;
        self.metrics.l2_read(bytes.len());
        Ok(())
    }

    async fn write_all_at(&self, bytes: &[u8], offset: u64) -> io::Result<()> {
        self.file.write_all_at(bytes, offset).await?;
        self.metrics.l2_write(bytes.len());
        Ok(())
    }
}

pub(super) struct WriterState {
    pub(super) disk: Arc<Disk>,
    pub(super) filter: Arc<HitFilter>,
    pub(super) active_segment: Option<usize>,
    append_offset: u64,
    next_generation: u64,
    dirty_bytes: u64,
    pub(super) promotion_tokens: u64,
}

impl WriterState {
    pub(super) async fn append(
        &mut self,
        path: &str,
        revision: &[u8],
        body: &[u8],
        current_after: SequencePoint,
    ) -> io::Result<Slot> {
        let record_bytes = self
            .disk
            .format
            .record_bytes(revision.len(), body.len())
            .ok_or_else(CacheFormat::invalid_record)?;
        self.ensure_space(record_bytes).await?;
        let segment = self.active_segment.expect("ensure_space selects a segment");
        let generation = self.disk.segment_generations[segment].load(Ordering::Acquire);
        let record = self
            .disk
            .format
            .encode_record(path, revision, body, current_after)?;
        debug_assert_eq!(record.len() as u64, record_bytes);
        let slot = Slot {
            fingerprint: self.disk.path_fingerprint(path),
            generation,
            record_offset: self.append_offset,
            record_bytes,
            current_after,
        };
        self.disk.write_all_at(&record, self.append_offset).await?;
        self.publish(slot).await?;
        self.append_offset += record_bytes;
        self.dirty_bytes = self
            .dirty_bytes
            .saturating_add(record_bytes + CacheFormat::SLOT_BYTES);
        Ok(slot)
    }

    pub(super) async fn invalidate(&mut self, path: &str) -> io::Result<()> {
        let fingerprint = self.disk.path_fingerprint(path);
        let bucket = fingerprint % self.disk.format.bucket_count();
        let bucket_offset =
            self.disk.format.metadata_bytes() + bucket * self.disk.format.block_bytes();
        let mut bytes = vec![0; self.disk.format.block_bytes() as usize];
        self.disk.read_exact_at(&mut bytes, bucket_offset).await?;
        let zero = [0u8; CacheFormat::SLOT_BYTES as usize];
        for (index, raw) in bytes
            .chunks_exact(CacheFormat::SLOT_BYTES as usize)
            .enumerate()
        {
            let slot = self.disk.format.decode_slot(raw);
            if slot.generation != 0 && slot.fingerprint == fingerprint {
                self.disk
                    .write_all_at(
                        &zero,
                        bucket_offset + index as u64 * CacheFormat::SLOT_BYTES,
                    )
                    .await?;
                self.dirty_bytes = self.dirty_bytes.saturating_add(CacheFormat::SLOT_BYTES);
            }
        }
        Ok(())
    }

    pub(super) async fn sync_if_needed(&mut self) -> io::Result<bool> {
        if self.dirty_bytes < SYNC_BYTES {
            return Ok(false);
        }
        self.sync().await
    }

    pub(super) async fn sync(&mut self) -> io::Result<bool> {
        if self.dirty_bytes == 0 {
            return Ok(false);
        }
        self.disk.file.sync_data().await?;
        self.dirty_bytes = 0;
        Ok(true)
    }

    pub(super) async fn clean_shutdown(&mut self) -> io::Result<()> {
        self.disk.file.sync_data().await?;
        let tail = self.active_segment.map(|segment| {
            let generation = self.disk.segment_generations[segment].load(Ordering::Acquire);
            (generation, self.append_offset)
        });
        let marker = self.disk.format.encode_clean_tail(tail);
        self.disk
            .write_all_at(&marker, self.disk.format.block_bytes())
            .await?;
        self.disk.file.sync_data().await?;
        self.dirty_bytes = 0;
        Ok(())
    }

    async fn ensure_space(&mut self, record_bytes: u64) -> io::Result<()> {
        if let Some(segment) = self.active_segment {
            let end = self.disk.format.segment_start(segment) + self.disk.format.segment_bytes();
            if self.append_offset + record_bytes <= end {
                return Ok(());
            }
        }
        self.initialize_segment().await
    }

    async fn initialize_segment(&mut self) -> io::Result<()> {
        let mut unused = None;
        let mut oldest = None;
        for (index, generation) in self.disk.segment_generations.iter().enumerate() {
            let generation = generation.load(Ordering::Acquire);
            if generation == 0 {
                unused.get_or_insert(index);
            } else if oldest.is_none_or(|(_, current)| generation < current) {
                oldest = Some((index, generation));
            }
        }
        let segment = match unused {
            Some(segment) => segment,
            None => oldest.expect("a valid layout has segments").0,
        };
        let generation = self.next_generation;
        self.next_generation = generation
            .checked_add(1)
            .ok_or_else(|| io::Error::other("persistent-cache segment generation exhausted"))?;
        let header = self.disk.format.encode_segment_header(generation);
        let start = self.disk.format.segment_start(segment);
        self.disk.write_all_at(&header, start).await?;
        self.disk.segment_generations[segment].store(generation, Ordering::Release);
        self.active_segment = Some(segment);
        self.append_offset = start + self.disk.format.block_bytes();
        self.dirty_bytes = self
            .dirty_bytes
            .saturating_add(self.disk.format.block_bytes());
        self.filter
            .note_segment_reinitialized(self.disk.format.segment_count());
        Ok(())
    }

    async fn publish(&mut self, slot: Slot) -> io::Result<()> {
        let bucket = slot.fingerprint % self.disk.format.bucket_count();
        let bucket_offset =
            self.disk.format.metadata_bytes() + bucket * self.disk.format.block_bytes();
        let mut bytes = vec![0; self.disk.format.block_bytes() as usize];
        self.disk.read_exact_at(&mut bytes, bucket_offset).await?;
        let zero = [0u8; CacheFormat::SLOT_BYTES as usize];
        for (index, raw) in bytes
            .chunks_exact_mut(CacheFormat::SLOT_BYTES as usize)
            .enumerate()
        {
            let previous = self.disk.format.decode_slot(raw);
            if previous.generation != 0 && previous.fingerprint == slot.fingerprint {
                self.disk
                    .write_all_at(
                        &zero,
                        bucket_offset + index as u64 * CacheFormat::SLOT_BYTES,
                    )
                    .await?;
                raw.fill(0);
                self.dirty_bytes = self.dirty_bytes.saturating_add(CacheFormat::SLOT_BYTES);
            }
        }
        let mut empty = None;
        let mut stale = None;
        let mut oldest = None;
        for (index, raw) in bytes
            .chunks_exact(CacheFormat::SLOT_BYTES as usize)
            .enumerate()
        {
            let candidate = self.disk.format.decode_slot(raw);
            if candidate.generation == 0 {
                empty.get_or_insert(index);
                continue;
            }
            if self.disk.slot_range(candidate).is_none() {
                stale.get_or_insert(index);
                continue;
            }
            if oldest
                .is_none_or(|(_, current): (usize, Slot)| candidate.generation < current.generation)
            {
                oldest = Some((index, candidate));
            }
        }
        let index = empty
            .or(stale)
            .unwrap_or_else(|| oldest.expect("a non-empty bucket has a replacement").0);
        self.disk
            .write_all_at(
                &self.disk.format.encode_slot(slot),
                bucket_offset + index as u64 * CacheFormat::SLOT_BYTES,
            )
            .await?;
        Ok(())
    }
}

pub(super) async fn open_disk(
    config: PersistentCacheConfig,
    database_name: &str,
    database_id: DatabaseId,
    geometry: CacheGeometry,
    metrics: Arc<CacheMetrics>,
    media: Arc<dyn CacheMedia>,
) -> io::Result<(Arc<Disk>, WriterState, Option<SequencePoint>)> {
    let format = CacheFormat::new(geometry, config.capacity_bytes, database_name, database_id)?;
    let file = media.open_exclusive(&config.directory).await?;
    let current_len = file.len().await?;
    let valid = current_len == format.capacity()
        && current_len != 0
        && header_valid(file.as_ref(), &format, &metrics).await?;
    if !valid {
        if current_len != 0 {
            tracing::info!(
                path = %config.directory.join(CACHE_FILE).display(),
                current_bytes = current_len,
                configured_bytes = format.capacity(),
                "reinitializing incompatible persistent-cache container"
            );
        }
        initialize_file(file.as_ref(), &format, &metrics).await?;
    }

    let mut generations = Vec::with_capacity(format.segment_count());
    let mut maximum = 0;
    for segment in 0..format.segment_count() {
        let mut header = [0u8; 16];
        file.read_exact_at(&mut header, format.segment_start(segment))
            .await?;
        metrics.l2_read(header.len());
        let generation = if let Some(generation) = format.decode_segment_generation(&header) {
            maximum = maximum.max(generation);
            generation
        } else {
            0
        };
        generations.push(AtomicU64::new(generation));
    }
    let next_generation = maximum
        .checked_add(1)
        .ok_or_else(|| io::Error::other("persistent-cache segment generation exhausted"))?;
    let disk = Arc::new(Disk {
        file,
        format,
        segment_generations: generations.into_boxed_slice(),
        metrics,
    });
    let last_sequence_point = if valid {
        disk.last_sequence_point().await?
    } else {
        None
    };
    let clean_tail = read_clean_tail(&disk, maximum).await?;
    let (active_segment, append_offset) = clean_tail.unwrap_or((None, 0));
    let writer = WriterState {
        disk: disk.clone(),
        filter: Arc::new(HitFilter::new()),
        active_segment,
        append_offset,
        next_generation,
        dirty_bytes: 0,
        promotion_tokens: 0,
    };
    Ok((disk, writer, last_sequence_point))
}

async fn initialize_file(
    file: &dyn CacheFile,
    format: &CacheFormat,
    metrics: &CacheMetrics,
) -> io::Result<()> {
    file.set_len(0).await?;
    file.set_len(format.capacity()).await?;
    file.allocate(format.capacity()).await?;
    let header = format.encode_file_header();
    file.write_all_at(&header, 0).await?;
    metrics.l2_write(header.len());
    file.sync_all().await?;
    Ok(())
}

async fn header_valid(
    file: &dyn CacheFile,
    format: &CacheFormat,
    metrics: &CacheMetrics,
) -> io::Result<bool> {
    let mut header = vec![0; format.block_bytes() as usize];
    file.read_exact_at(&mut header, 0).await?;
    metrics.l2_read(header.len());
    Ok(format.file_header_valid(&header))
}

async fn read_clean_tail(disk: &Disk, maximum: u64) -> io::Result<Option<(Option<usize>, u64)>> {
    if maximum == 0 {
        return Ok(None);
    }
    let mut marker = vec![0; disk.format.block_bytes() as usize];
    disk.read_exact_at(&mut marker, disk.format.block_bytes())
        .await?;
    let Some((generation, append_offset)) = disk.format.decode_clean_tail(&marker) else {
        return Ok(None);
    };
    if generation != maximum {
        return Ok(None);
    }
    for segment in 0..disk.format.segment_count() {
        if disk.segment_generations[segment].load(Ordering::Acquire) != generation {
            continue;
        }
        let start = disk.format.segment_start(segment);
        if append_offset % CacheFormat::RECORD_ALIGNMENT == 0
            && append_offset >= start + disk.format.block_bytes()
            && append_offset <= start + disk.format.segment_bytes()
        {
            return Ok(Some((Some(segment), append_offset)));
        }
    }
    Ok(None)
}
