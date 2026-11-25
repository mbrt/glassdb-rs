//! End-of-run check that no leaf or collection record keeps a stray holder.
//!
//! A stray holder is a holder whose transaction record GC deleted (ADR-079).
//! After a full GC, each holder that a leaf or a collection record names, and
//! whose record existed, must still have it. No CAS that adds a holder can land
//! after GC deleted the record of its identity (ADR-105, ADR-106), so the check
//! runs also with transport failures and slow mutations.
//!
//! A holder whose identity never wrote a record is not a stray holder. Its owner
//! crashed before the record, and a later writer wounds it (ADR-024).

use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use glassdb_backend::middleware::OpLog;
use glassdb_backend::{Backend, BackendError, ListCursor, ListLimit};
use glassdb_concurr::rt;
use glassdb_data::{ObjectPath, TxId};
use glassdb_storage::{CollectionRecord, Node};

use crate::Database;

const LIST_PAGE: ListLimit = NonZeroUsize::new(1000).unwrap();
const GC_PARALLELISM: NonZeroUsize = NonZeroUsize::new(4).unwrap();

/// Runs full GCs on `db` until no garbage is left, then panics if a GC check
/// failed, or if a leaf in `backend` names a lock holder without a transaction
/// record. With `crashes`, the log of every operation on `backend`, a holder
/// whose identity never wrote a record is allowed.
pub(super) async fn assert_no_stray_holders(
    db: &Database,
    backend: &dyn Backend,
    db_name: &str,
    crashes: Option<&OpLog>,
) {
    collect_all_garbage(db).await;
    let (leaves, records) = stored_holders(backend, db_name).await;
    let stray = stray_holders(backend, crashes, leaves).await;
    assert!(
        stray.is_empty(),
        "leaves keep lock holders without a transaction record: {stray:?}"
    );
    let stray = stray_holders(backend, crashes, records).await;
    assert!(
        stray.is_empty(),
        "collection records name holders without a transaction record \
         (path, field, holder): {stray:?}"
    );
}

/// Returns each `(path, field, holder)` of `holders` whose holder has no
/// transaction record, except a holder that never wrote one under `crashes`.
async fn stray_holders(
    backend: &dyn Backend,
    crashes: Option<&OpLog>,
    holders: Vec<(ObjectPath, &'static str, TxId)>,
) -> Vec<(String, &'static str, TxId)> {
    let mut stray = Vec::new();
    for (path, field, holder) in holders {
        let record = ObjectPath::Transaction {
            db_prefix: path_db_prefix(&path),
            id: holder,
        }
        .to_string();
        match backend.read(&record).await {
            Ok(_) => {}
            Err(BackendError::NotFound)
                if crashes.is_some_and(|log| !was_written(log, &record)) => {}
            Err(BackendError::NotFound) => stray.push((path.to_string(), field, holder)),
            Err(error) => panic!("reading the record of holder {holder}: {error}"),
        }
    }
    stray
}

/// Reports whether `log` has a write of `path`. Every operation in `log`
/// reached the faultless backend, and a write of a record path applies unless
/// the record already exists, so the record existed.
fn was_written(log: &OpLog, path: &str) -> bool {
    log.lock()
        .unwrap()
        .iter()
        .any(|op| op.path == path && op.op.starts_with("write"))
}

/// Repeats full GCs until a GC defers nothing and makes no progress, then
/// panics if a check of that GC failed. The records of the last retention
/// period are deferred, so this waits for it. A check can fail until a later
/// record of the same GC is deleted, for example a lock release on a dropped
/// collection that the drop did not reclaim yet. So a GC that deletes records
/// is repeated too.
async fn collect_all_garbage(db: &Database) {
    loop {
        let report = db
            .collect_garbage(GC_PARALLELISM)
            .await
            .expect("full GC after the run");
        if report.deferred == 0 && report.reclaimed_effects == 0 && report.deleted == 0 {
            assert_eq!(report.failed, 0, "full GC checks failed: {report:?}");
            return;
        }
        rt::sleep(report.retry_after.unwrap_or_default()).await;
    }
}

/// Returns the holders that the stored leaves and collection records name, as
/// `(path, field, holder)`. A leaf holds entry locks and the membership lock. A
/// collection record holds the directory lock, the topology freeze, and the
/// topology participants.
async fn stored_holders(
    backend: &dyn Backend,
    db_name: &str,
) -> (
    Vec<(ObjectPath, &'static str, TxId)>,
    Vec<(ObjectPath, &'static str, TxId)>,
) {
    let mut leaves = Vec::new();
    let mut records = Vec::new();
    let mut cursor: Option<ListCursor> = None;
    loop {
        let page = backend
            .list(&format!("{db_name}/"), cursor.as_ref(), LIST_PAGE)
            .await
            .expect("listing the stored objects");
        for object in &page.objects {
            let Ok(path) = ObjectPath::try_from(object.as_str()) else {
                continue;
            };
            if !matches!(
                path,
                ObjectPath::TreeRoot { .. }
                    | ObjectPath::Node { .. }
                    | ObjectPath::CollectionRecord { .. }
            ) {
                continue;
            }
            let reply = match backend.read(object).await {
                Ok(reply) => reply,
                Err(BackendError::NotFound) => continue,
                Err(error) => panic!("reading {object}: {error}"),
            };
            if matches!(path, ObjectPath::CollectionRecord { .. }) {
                let record = CollectionRecord::decode(&reply.contents)
                    .expect("decoding a stored collection record");
                let holders = record
                    .directory_lock()
                    .holders()
                    .iter()
                    .map(|id| ("directory_lock", *id))
                    .chain(record.topology_freeze().map(|id| ("topology_freeze", *id)))
                    .chain(
                        record
                            .topology_participants()
                            .map(|id| ("topology_participants", *id)),
                    );
                records.extend(holders.map(|(field, id)| (path.clone(), field, id)));
                continue;
            }
            let node = Node::decode(&reply.contents).expect("decoding a stored node");
            let Some(leaf) = node.as_leaf() else {
                continue;
            };
            let holders: BTreeSet<TxId> = leaf
                .entries()
                .flat_map(|entry| entry.lock_holders().iter().copied())
                .chain(node.membership_lock().holders().iter().copied())
                .collect();
            leaves.extend(
                holders
                    .into_iter()
                    .map(|holder| (path.clone(), "leaf", holder)),
            );
        }
        match page.next {
            Some(next) => cursor = Some(next),
            None => return (leaves, records),
        }
    }
}

fn path_db_prefix(path: &ObjectPath) -> glassdb_data::DbPrefix {
    match path {
        ObjectPath::TreeRoot { collection }
        | ObjectPath::Node { collection, .. }
        | ObjectPath::CollectionRecord { collection } => collection.db_prefix_component().clone(),
        _ => unreachable!("only leaves and collection records have holders"),
    }
}
