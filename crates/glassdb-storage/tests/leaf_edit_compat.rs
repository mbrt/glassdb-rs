#![allow(deprecated)]

use glassdb_storage::{
    LeafEdit, LeafObservation, LoadedLeaf, NodeLocks, Shard, ShardStore, StorageError,
};

fn legacy_fields(loaded: &LoadedLeaf) -> (&Shard, &NodeLocks, &LeafObservation) {
    (&loaded.entries, &loaded.locks, &loaded.observation)
}

fn replace_legacy_fields(
    loaded: &mut LoadedLeaf,
    entries: Shard,
    locks: NodeLocks,
    observation: LeafObservation,
) {
    loaded.entries = entries;
    loaded.locks = locks;
    loaded.observation = observation;
}

async fn legacy_commit(
    store: &ShardStore,
    path: &str,
    entries: &Shard,
    locks: &NodeLocks,
    observation: &LeafObservation,
) -> Result<bool, StorageError> {
    store.store_leaf(path, entries, locks, observation).await
}

async fn bound_commit(store: &ShardStore, loaded: LoadedLeaf) -> Result<bool, StorageError> {
    let edit: LeafEdit = loaded.into_edit();
    store.commit_leaf(edit).await
}

#[test]
fn legacy_and_bound_leaf_edit_surfaces_compile() {
    let _legacy_fields = legacy_fields;
    let _replace_legacy_fields = replace_legacy_fields;
    let _legacy_commit = legacy_commit;
    let _bound_commit = bound_commit;
}
