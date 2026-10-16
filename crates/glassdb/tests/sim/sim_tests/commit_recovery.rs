//! Commit recovery when the owner pauses for longer than the GC safety
//! horizon. GC is real in these tests, so they show that correctness does not
//! depend on the time that the owner waits.
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use glassdb::backend::{BackendError, Revision, memory::MemoryBackend};
use glassdb::middleware::{BackendOp, HookBackend};
use glassdb::{Backend, Database, Error, InlinePolicy};
use glassdb_concurr::{exec, rt};
use glassdb_storage::transaction::TxCommitStatus;
use glassdb_trans::ProtocolTiming;

/// A transaction-record write that reaches storage only when the test
/// releases it, while its caller already received `Unavailable`.
#[derive(Clone, Default)]
struct DelayedWrite(Arc<Mutex<Option<HeldWrite>>>);

/// The path, value, and expected revision of a held write. A create has no
/// expected revision.
type HeldWrite = (String, Vec<u8>, Option<Revision>);

impl DelayedWrite {
    fn hold(&self, operation: &BackendOp<'_>) {
        let held = match operation {
            BackendOp::WriteIf {
                path,
                value,
                expected,
            } => (path.to_string(), value.to_vec(), Some((*expected).clone())),
            BackendOp::WriteIfNotExists { path, value } => (path.to_string(), value.to_vec(), None),
            _ => panic!("only writes can be held"),
        };
        *self.0.lock().unwrap() = Some(held);
    }

    fn path(&self) -> String {
        self.0
            .lock()
            .unwrap()
            .as_ref()
            .expect("a held write")
            .0
            .clone()
    }

    async fn release(&self, memory: &MemoryBackend) -> Result<(), BackendError> {
        let (path, value, expected) = self.0.lock().unwrap().take().expect("a held write");
        match expected {
            Some(expected) => memory.write_if(&path, value, &expected).await,
            None => memory.write_if_not_exists(&path, value).await,
        }
        .map(|_| ())
    }
}

fn committed_record_write(operation: &BackendOp<'_>) -> bool {
    matches!(
        operation,
        BackendOp::WriteIf { path, value, .. } | BackendOp::WriteIfNotExists { path, value }
            if path.contains("/_t/")
                && glassdb_storage::txrecord::status(value)
                    .is_ok_and(|status| status == TxCommitStatus::Committed)
    )
}

fn record_status(body: &[u8]) -> TxCommitStatus {
    glassdb_storage::txrecord::status(body).unwrap()
}

async fn wait_until_deleted(memory: &MemoryBackend, path: &str) {
    for _ in 0..600 {
        if matches!(memory.read(path).await, Err(BackendError::NotFound)) {
            return;
        }
        rt::sleep(Duration::from_millis(50)).await;
    }
    panic!("GC did not delete {path}");
}

fn open(backend: Arc<dyn Backend>) -> impl Future<Output = Result<Database, Error>> {
    Database::builder("recovery", backend)
        .protocol_timing(ProtocolTiming::simulation())
        .inline_policy(InlinePolicy::none())
        .open()
}

// Regression: the first commit write of a collection-only transaction is in
// doubt and lands after the owner read `Pending`. GC in another process then
// does the directory write-back and deletes the committed record before the
// rejected retry reads back. The owner must report the outcome as in doubt,
// because the transaction committed.
#[test]
fn a_reclaimed_commit_that_landed_after_a_pending_read_is_in_doubt() {
    exec::block_on(async {
        let memory = Arc::new(MemoryBackend::new());
        let backend = HookBackend::new(memory.clone());
        let db = open(backend.clone()).await.unwrap();
        let collector = open(memory.clone()).await.unwrap();

        let delayed = DelayedWrite::default();
        let attempts = Arc::new(AtomicUsize::new(0));
        backend.set_before({
            let delayed = delayed.clone();
            let attempts = attempts.clone();
            move |operation| {
                let attempt = committed_record_write(operation)
                    .then(|| attempts.fetch_add(1, Ordering::SeqCst));
                if attempt == Some(0) {
                    delayed.hold(operation);
                }
                let delayed = delayed.clone();
                let memory = memory.clone();
                Box::pin(async move {
                    match attempt {
                        Some(0) => Err(BackendError::Unavailable("injected delayed write".into())),
                        Some(1) => {
                            // The owner pauses before its retry reaches
                            // storage, for longer than the safety horizon.
                            let path = delayed.path();
                            delayed.release(&memory).await.unwrap();
                            wait_until_deleted(&memory, &path).await;
                            Ok(())
                        }
                        _ => Ok(()),
                    }
                })
            }
        });

        let result = db
            .root_collection()
            .create_collection(b"child")
            .await
            .map(|_| ());
        assert!(matches!(result, Err(Error::InDoubt(_))), "{result:?}");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        backend.clear_before();
        assert!(
            db.root_collection()
                .collection_exists(b"child")
                .await
                .unwrap()
        );
        db.shutdown().await;
        collector.shutdown().await;
    });
}

// Regression (ADR-059): the create of a committed record is in doubt, and an
// older peer wounds the empty path before the create lands. The owner must
// keep the wound pinned. Otherwise GC deletes the acknowledged abort and the
// delayed create commits an identity that peers already treat as aborted.
#[test]
fn a_delayed_commit_create_cannot_land_after_a_wound() {
    exec::block_on(async {
        let memory = Arc::new(MemoryBackend::new());
        let backend = HookBackend::new(memory.clone());
        let owner = open(backend.clone()).await.unwrap();
        let peer = open(memory.clone()).await.unwrap();
        owner.root_collection().write(b"key", b"old").await.unwrap();

        let peer_started = Arc::new(tokio::sync::Notify::new());
        let owner_locked = Arc::new(tokio::sync::Notify::new());
        let peer_write = rt::spawn({
            let peer_started = peer_started.clone();
            let owner_locked = owner_locked.clone();
            async move {
                peer.tx(|tx| {
                    let peer_started = peer_started.clone();
                    let owner_locked = owner_locked.clone();
                    async move {
                        peer_started.notify_one();
                        owner_locked.notified().await;
                        tx.write(&tx.root_collection(), b"key", b"peer")
                    }
                })
                .await
                .unwrap();
                peer
            }
        });
        // Wound-wait does not order identities with equal start times, so the
        // owner must start later than the peer for the peer to wound it.
        peer_started.notified().await;
        rt::sleep(Duration::from_millis(1)).await;

        let delayed = DelayedWrite::default();
        let held = Arc::new(AtomicBool::new(false));
        backend.set_before({
            let delayed = delayed.clone();
            let held = held.clone();
            let memory = memory.clone();
            move |operation| {
                let hold = matches!(operation, BackendOp::WriteIfNotExists { .. })
                    && committed_record_write(operation)
                    && !held.swap(true, Ordering::SeqCst);
                if hold {
                    delayed.hold(operation);
                }
                let path = operation.path().to_string();
                let owner_locked = owner_locked.clone();
                let memory = memory.clone();
                Box::pin(async move {
                    if !hold {
                        return Ok(());
                    }
                    // The peer finds the owner's lock and wounds the identity
                    // before the owner reads its record back.
                    owner_locked.notify_one();
                    while !memory
                        .read(&path)
                        .await
                        .is_ok_and(|r| record_status(&r.contents) == TxCommitStatus::Wounded)
                    {
                        rt::sleep(Duration::from_millis(1)).await;
                    }
                    Err(BackendError::Unavailable("injected delayed create".into()))
                })
            }
        });

        // The wound makes the owner replay the body under a new identity.
        owner.root_collection().write(b"key", b"new").await.unwrap();
        backend.clear_before();
        let peer = peer_write.await.unwrap();
        assert!(held.load(Ordering::SeqCst));
        let path = delayed.path();

        // Give GC more than the safety horizon to delete an acknowledged abort.
        rt::sleep(Duration::from_secs(5)).await;
        let wounded = memory.read(&path).await.unwrap();
        assert_eq!(record_status(&wounded.contents), TxCommitStatus::Wounded);
        assert!(matches!(
            delayed.release(&memory).await,
            Err(BackendError::Precondition)
        ));
        let current = memory.read(&path).await.unwrap();
        assert_eq!(record_status(&current.contents), TxCommitStatus::Wounded);
        assert_eq!(
            peer.root_collection().read(b"key").await.unwrap(),
            Some(b"new".to_vec())
        );
        owner.shutdown().await;
        peer.shutdown().await;
    });
}
