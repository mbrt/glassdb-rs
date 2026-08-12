use glassdb_data::TxId;
use glassdb_storage::{
    EntryLockState, ExclusiveGate, LockType, NodeLock, ShardEntry, SharedExclusiveLock,
};

#[test]
fn node_lock_remains_an_alias_for_the_shared_exclusive_scope() {
    let lock: NodeLock = SharedExclusiveLock::default();
    assert!(lock.holders().is_empty());

    let gate = ExclusiveGate::default();
    assert!(gate.holder().is_none());
}

#[test]
fn shard_entry_raw_lock_fields_remain_available_during_migration() {
    let holder = TxId::from_bytes(vec![1]);
    let mut entry = ShardEntry::new(b"key");
    entry.lock_type = LockType::Write;
    entry.locked_by = vec![holder.clone()];

    assert_eq!(entry.lock_type(), LockType::Write);
    assert!(entry.is_locked_by(&holder));

    entry.replace_lock(EntryLockState::create(holder.clone()));
    assert_eq!(entry.lock_type, LockType::Create);
    assert_eq!(entry.locked_by, vec![holder]);
}
