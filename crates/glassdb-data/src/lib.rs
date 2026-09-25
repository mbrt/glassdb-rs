//! Core data types for GlassDB: transaction identities and storage path
//! encoding. Ported from the Go `internal/data` and `internal/data/paths`
//! packages.

pub mod base64;
mod entropy;
mod ids;
mod paths;

pub use entropy::shuffle;
pub use ids::{
    CollectionId, DatabaseId, ID_BYTES, MAX_COLLECTION_NAME_BYTES, NodeId, StructuralIntentId, TxId,
};
pub use paths::{CollectionAddress, DbPrefix, LeafRef, LogicalKey, ObjectPath, PathError};
