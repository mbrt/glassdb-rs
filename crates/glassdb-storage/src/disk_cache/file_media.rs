//! Native Linux media for the persistent cache.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use glassdb_concurr::rt;
use rustix::fs::{FallocateFlags, FlockOperation};

use super::CACHE_FILE;
use super::media::{CacheFile, CacheMedia};

/// Native Linux media used by production persistent caches.
pub(super) struct FileMedia;

struct FileCacheFile {
    file: File,
}

#[async_trait]
impl CacheMedia for FileMedia {
    async fn open_exclusive(&self, directory: &Path) -> io::Result<Arc<dyn CacheFile>> {
        // Host filesystem operations would bypass the deterministic executor.
        // Simulation must inject SimMedia instead.
        if rt::in_sim() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "native cache media is unavailable in deterministic simulation",
            ));
        }
        std::fs::create_dir_all(directory)?;
        let path = directory.join(CACHE_FILE);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        rustix::fs::flock(&file, FlockOperation::NonBlockingLockExclusive)?;
        Ok(Arc::new(FileCacheFile { file }))
    }
}

#[async_trait]
impl CacheFile for FileCacheFile {
    async fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    async fn set_len(&self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }

    async fn allocate(&self, len: u64) -> io::Result<()> {
        rustix::fs::fallocate(&self.file, FallocateFlags::empty(), 0, len).map_err(io::Error::from)
    }

    async fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        self.file.read_exact_at(bytes, offset)
    }

    async fn write_all_at(&self, bytes: &[u8], offset: u64) -> io::Result<()> {
        self.file.write_all_at(bytes, offset)
    }

    async fn sync_data(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    async fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }
}
