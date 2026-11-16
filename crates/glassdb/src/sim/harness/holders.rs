//! End-of-run check that no leaf keeps a stray holder.
//!
//! A stray holder is a lock of a transaction whose record GC deleted
//! (ADR-079). After a full GC, each lock holder in a leaf must still have a
//! record. A lock CAS that lands after the retention period can still leave a
//! stray holder (ADR-095), so the harness runs this check only without
//! transport failures and slow mutations.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use glassdb_backend::{Backend, BackendError, ListCursor, ListLimit};
use glassdb_concurr::rt;
use glassdb_data::{ObjectPath, TxId};
use glassdb_storage::Node;

use crate::Database;

const LIST_PAGE: ListLimit = NonZeroUsize::new(1000).unwrap();
const GC_PARALLELISM: NonZeroUsize = NonZeroUsize::new(4).unwrap();

/// Runs full GCs on `db` until no garbage is left, then panics if a GC check
/// failed, or if a leaf in `backend` names a lock holder that has no
/// transaction record.
pub(super) async fn assert_no_stray_holders(db: &Database, backend: &dyn Backend, db_name: &str) {
    collect_all_garbage(db).await;
    let mut stray = Vec::new();
    for (path, holder) in leaf_holders(backend, db_name).await {
        let record = ObjectPath::Transaction {
            db_prefix: path_db_prefix(&path),
            id: holder,
        };
        match backend.read(&record.to_string()).await {
            Ok(_) => {}
            Err(BackendError::NotFound) => stray.push((path.to_string(), holder)),
            Err(error) => panic!("reading the record of holder {holder}: {error}"),
        }
    }
    assert!(
        stray.is_empty(),
        "leaves keep lock holders without a transaction record: {stray:?}"
    );
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

/// Returns each leaf path with each transaction that holds an entry lock or
/// the membership lock of the leaf.
async fn leaf_holders(backend: &dyn Backend, db_name: &str) -> Vec<(ObjectPath, TxId)> {
    let mut out = Vec::new();
    let mut cursor: Option<ListCursor> = None;
    loop {
        let page = backend
            .list(&format!("{db_name}/"), cursor.as_ref(), LIST_PAGE)
            .await
            .expect("listing the stored objects");
        for object in &page.objects {
            let Ok(path @ (ObjectPath::TreeRoot { .. } | ObjectPath::Node { .. })) =
                ObjectPath::try_from(object.as_str())
            else {
                continue;
            };
            let reply = match backend.read(object).await {
                Ok(reply) => reply,
                Err(BackendError::NotFound) => continue,
                Err(error) => panic!("reading {object}: {error}"),
            };
            let node = Node::decode(&reply.contents).expect("decoding a stored node");
            let Some(leaf) = node.as_leaf() else {
                continue;
            };
            let holders: BTreeSet<TxId> = leaf
                .entries()
                .flat_map(|entry| entry.lock_holders().iter().copied())
                .chain(node.membership_lock().holders().iter().copied())
                .collect();
            out.extend(holders.into_iter().map(|holder| (path.clone(), holder)));
        }
        match page.next {
            Some(next) => cursor = Some(next),
            None => return out,
        }
    }
}

fn path_db_prefix(path: &ObjectPath) -> glassdb_data::DbPrefix {
    match path {
        ObjectPath::TreeRoot { collection } | ObjectPath::Node { collection, .. } => {
            collection.db_prefix_component().clone()
        }
        _ => unreachable!("only leaf paths have holders"),
    }
}
