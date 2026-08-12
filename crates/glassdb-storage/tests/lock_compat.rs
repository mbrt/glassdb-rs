use glassdb_storage::{ExclusiveGate, NodeLock, SharedExclusiveLock};

#[test]
fn node_lock_remains_an_alias_for_the_shared_exclusive_scope() {
    let lock: NodeLock = SharedExclusiveLock::default();
    assert!(lock.holders().is_empty());

    let gate = ExclusiveGate::default();
    assert!(gate.holder().is_none());
}
