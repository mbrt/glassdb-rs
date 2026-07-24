//! Core data types for GlassDB: transaction identifiers and storage path
//! encoding. Ported from the Go `internal/data` and `internal/data/paths`
//! packages.

pub mod base64;
mod collection_id;
mod database_id;
mod entropy;
pub mod paths;
mod txid;

pub use collection_id::{COLLECTION_ID_BYTES, CollectionId, MAX_COLLECTION_NAME_BYTES};
pub use database_id::{DATABASE_ID_BYTES, DatabaseId};
pub use entropy::shuffle;
pub use paths::{CollectionAddress, KeyRef, LeafRef};
pub use txid::{TxId, TxIdSet, set_diff, set_intersect, set_union};
