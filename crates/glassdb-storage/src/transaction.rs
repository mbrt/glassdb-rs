//! Transaction-log data shared by persistence and transaction processing.

mod codec;
mod model;

pub(crate) use codec::TxLogCodec;
pub use model::{
    TxCollectionChange, TxCollectionOp, TxCommitStatus, TxLifecycleRelation, TxLock, TxLog,
    TxRecordState, TxWrite,
};
