//! Core data types for GlassDB: transaction identities and storage path
//! encoding. Ported from the Go `internal/data` and `internal/data/paths`
//! packages.

pub mod base64;
mod collection_id;
mod database_id;
mod entropy;
mod node_id;
mod paths;
mod structural_intent_id;
mod txid;

pub use collection_id::{CollectionId, MAX_COLLECTION_NAME_BYTES};
pub use database_id::DatabaseId;
pub use entropy::shuffle;
pub use node_id::NodeId;
pub use paths::{CollectionAddress, DbPrefix, LeafRef, LogicalKey, ObjectPath, PathError};
pub use structural_intent_id::StructuralIntentId;
pub use txid::TxId;

/// Number of bytes in each fixed-width GlassDB ID.
pub const ID_BYTES: usize = 16;
