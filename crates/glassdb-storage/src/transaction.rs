//! Transaction-record data shared by persistence and transaction processing.

mod codec;
mod model;
mod store;

pub(crate) use codec::TxRecordCodec;
pub use model::{
    TxCollectionChange, TxCollectionOp, TxCommitStatus, TxLifecycleRelation, TxLock, TxRecord,
    TxRecordState, TxWrite,
};
pub use store::{TxListPage, TxRecordStore, TxStatus};
