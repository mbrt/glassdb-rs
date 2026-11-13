//! Host filesystem media for the persistent cache.

/// Host filesystem media used by persistent caches.
pub(super) struct FileMedia;

#[cfg(sim)]
mod sim {
    use std::io;
    use std::path::Path;
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::super::media::{CacheFile, CacheMedia};
    use super::FileMedia;

    #[async_trait]
    impl CacheMedia for FileMedia {
        async fn open_exclusive(&self, directory: &Path) -> io::Result<Arc<dyn CacheFile>> {
            // Host filesystem operations would bypass the deterministic executor.
            // Simulation must inject SimMedia instead.
            let _ = directory;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "native cache media is unavailable in deterministic simulation",
            ))
        }
    }

    #[cfg(test)]
    mod sim_tests {
        use std::io;
        use std::path::Path;

        use super::{CacheMedia, FileMedia};

        #[test]
        fn native_media_is_rejected_without_an_executor() {
            let result = futures::executor::block_on(
                FileMedia.open_exclusive(Path::new("unused-native-cache-directory")),
            );
            let error = match result {
                Ok(_) => panic!("native media opened in a simulation build"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        }
    }
}

#[cfg(not(sim))]
mod native {
    use std::fs::{File, OpenOptions};
    use std::io;
    use std::os::unix::fs::FileExt;
    use std::path::Path;
    use std::sync::Arc;

    use async_trait::async_trait;
    use rustix::fs::{FallocateFlags, FlockOperation};

    use super::super::CACHE_FILE;
    use super::super::media::{CacheFile, CacheMedia};
    use super::FileMedia;

    /// Runs one blocking file operation on Tokio's blocking pool.
    ///
    /// The closure owns what it uses. If the caller drops the future, the
    /// operation still runs to its end, and so it keeps the file and its lock
    /// until then.
    async fn blocking<T, F>(operation: F) -> io::Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> io::Result<T> + Send + 'static,
    {
        tokio::task::spawn_blocking(operation)
            .await
            .map_err(io::Error::other)?
    }

    struct FileCacheFile {
        file: Arc<File>,
    }

    #[async_trait]
    impl CacheMedia for FileMedia {
        async fn open_exclusive(&self, directory: &Path) -> io::Result<Arc<dyn CacheFile>> {
            let directory = directory.to_path_buf();
            blocking(move || {
                std::fs::create_dir_all(&directory)?;
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(directory.join(CACHE_FILE))?;
                rustix::fs::flock(&file, FlockOperation::NonBlockingLockExclusive)?;
                Ok(Arc::new(FileCacheFile {
                    file: Arc::new(file),
                }) as Arc<dyn CacheFile>)
            })
            .await
        }
    }

    #[async_trait]
    impl CacheFile for FileCacheFile {
        async fn len(&self) -> io::Result<u64> {
            let file = self.file.clone();
            blocking(move || Ok(file.metadata()?.len())).await
        }

        async fn set_len(&self, len: u64) -> io::Result<()> {
            let file = self.file.clone();
            blocking(move || file.set_len(len)).await
        }

        async fn allocate(&self, len: u64) -> io::Result<()> {
            let file = self.file.clone();
            blocking(move || {
                rustix::fs::fallocate(&*file, FallocateFlags::empty(), 0, len)
                    .map_err(io::Error::from)
            })
            .await
        }

        async fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
            let file = self.file.clone();
            let len = bytes.len();
            let read = blocking(move || {
                let mut buffer = vec![0; len];
                file.read_exact_at(&mut buffer, offset)?;
                Ok(buffer)
            })
            .await?;
            bytes.copy_from_slice(&read);
            Ok(())
        }

        async fn write_all_at(&self, bytes: &[u8], offset: u64) -> io::Result<()> {
            let file = self.file.clone();
            let bytes = bytes.to_vec();
            blocking(move || file.write_all_at(&bytes, offset)).await
        }

        async fn sync_data(&self) -> io::Result<()> {
            let file = self.file.clone();
            blocking(move || file.sync_data()).await
        }

        async fn sync_all(&self) -> io::Result<()> {
            let file = self.file.clone();
            blocking(move || file.sync_all()).await
        }
    }
}
