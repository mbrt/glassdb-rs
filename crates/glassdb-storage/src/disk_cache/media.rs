use std::io;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;

/// Opens the single container owned by one persistent cache.
#[async_trait]
pub(super) trait CacheMedia: Send + Sync {
    async fn open_exclusive(&self, directory: &Path) -> io::Result<Arc<dyn CacheFile>>;
}

/// The positioned-I/O surface required by the persistent-cache format.
#[async_trait]
pub(super) trait CacheFile: Send + Sync {
    async fn len(&self) -> io::Result<u64>;
    async fn set_len(&self, len: u64) -> io::Result<()>;
    async fn allocate(&self, len: u64) -> io::Result<()>;
    async fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()>;
    async fn write_all_at(&self, bytes: &[u8], offset: u64) -> io::Result<()>;
    async fn sync_data(&self) -> io::Result<()>;
    async fn sync_all(&self) -> io::Result<()>;
}
