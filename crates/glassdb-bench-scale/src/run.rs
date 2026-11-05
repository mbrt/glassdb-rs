//! Shared lifecycle helpers for the scale benchmark binaries.

use futures::stream::{FuturesUnordered, StreamExt};
use glassdb::{Database, Error};
use glassdb_concurr::rt::{self, Instant};
use tokio::task::JoinHandle;

/// Waits for benchmark workers until `deadline`.
///
/// The first worker error or panic aborts its peers. Expiry also aborts every
/// worker, so dropping a benchmark cell cannot leave detached transactions
/// changing the backend or holding `Database` clones alive.
pub async fn join_tasks_until(
    handles: Vec<JoinHandle<Result<(), Error>>>,
    deadline: Instant,
) -> Result<(), Error> {
    let mut pending: FuturesUnordered<_> = handles.into_iter().collect();
    while !pending.is_empty() {
        let left = deadline.saturating_duration_since(Instant::now());
        let joined = match rt::timeout(left, pending.next()).await {
            Ok(Some(joined)) => joined,
            Ok(None) => return Ok(()),
            Err(_) => {
                abort_all(&mut pending).await;
                return Err(Error::internal(
                    "benchmark workers exceeded the cell completion deadline",
                ));
            }
        };
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                abort_all(&mut pending).await;
                return Err(err);
            }
            Err(err) => {
                abort_all(&mut pending).await;
                return Err(Error::with_source("benchmark worker task failed", err));
            }
        }
    }
    Ok(())
}

/// Gracefully closes all databases without extending the cell deadline.
pub async fn shutdown_databases_until(
    databases: &[Database],
    deadline: Instant,
) -> Result<(), Error> {
    let shutdowns = futures::future::join_all(databases.iter().map(Database::shutdown));
    rt::timeout(
        deadline.saturating_duration_since(Instant::now()),
        shutdowns,
    )
    .await
    .map(|_| ())
    .map_err(|_| Error::internal("database shutdown exceeded the cell completion deadline"))
}

async fn abort_all(pending: &mut FuturesUnordered<JoinHandle<Result<(), Error>>>) {
    for handle in pending.iter() {
        handle.abort();
    }
    while pending.next().await.is_some() {}
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn worker_error_aborts_its_peers() {
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Relaxed);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let marker = Dropped(dropped.clone());
        let pending = tokio::spawn(async move {
            let _marker = marker;
            std::future::pending::<Result<(), Error>>().await
        });
        let failed = tokio::spawn(async { Err(Error::NotFound) });

        let err = join_tasks_until(
            vec![pending, failed],
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::NotFound));
        assert!(dropped.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn deadline_aborts_pending_workers() {
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Relaxed);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let marker = Dropped(dropped.clone());
        let pending = tokio::spawn(async move {
            let _marker = marker;
            std::future::pending::<Result<(), Error>>().await
        });

        let err = join_tasks_until(vec![pending], Instant::now())
            .await
            .unwrap_err();

        assert!(matches!(err, Error::Internal { .. }));
        assert!(dropped.load(Ordering::Relaxed));
    }
}
