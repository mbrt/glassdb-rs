//! Transaction-log data shared by persistence and transaction processing.

mod model;

pub use model::{
    TxCollectionChange, TxCollectionOp, TxCommitStatus, TxLifecycleRelation, TxLock, TxLog,
    TxRecordState, TxWrite,
};
