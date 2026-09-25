//! Immutable database identity, coordination limits, and transaction timing.

use std::time::Duration;

use glassdb_backend::Backend;
use glassdb_data::{DATABASE_ID_BYTES, DatabaseId};
use glassdb_proto as pb;
use glassdb_storage::NodeSizePolicy;
use glassdb_trans::{Engine, ProtocolTiming};
use prost::Message;

use crate::error::Error;

const DB_VERSION: &str = "v5";
const DB_META_PATH: &str = "glassdb";

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DatabaseMetadata {
    pub(crate) id: DatabaseId,
    pub(crate) timing: ProtocolTiming,
    node_max_bytes: usize,
    split_headroom_bytes: usize,
}

impl DatabaseMetadata {
    /// Combines stored hard limits with local split and merge thresholds.
    pub(crate) fn node_size_policy(&self, local: NodeSizePolicy) -> Result<NodeSizePolicy, Error> {
        let policy = NodeSizePolicy::builder()
            .leaf_max_entries(local.leaf_max_entries())
            .node_soft_max_bytes(local.node_soft_max_bytes())
            .index_max_children(local.index_max_children())
            .node_max_bytes(self.node_max_bytes)
            .split_headroom_bytes(self.split_headroom_bytes)
            .build()
            .map_err(|error| Error::with_source("invalid database coordination limits", error))?;
        validate_limits(policy)
            .map_err(|error| Error::with_source("invalid database coordination limits", error))?;
        Ok(policy)
    }
}

/// Verifies the database metadata exists with the expected version, creating it
/// if missing. Races against concurrent creators are resolved by re-checking.
pub(crate) async fn check_or_create_db_meta(
    b: &impl Backend,
    name: &str,
    policy: NodeSizePolicy,
    timing: ProtocolTiming,
) -> Result<DatabaseMetadata, Error> {
    match check_db_version(b, name).await {
        Ok(metadata) => return Ok(metadata),
        Err(Error::NotFound) => {}
        Err(e) => return Err(e),
    }
    validate_limits(policy)?;
    validate_timing(timing)?;
    Engine::prepare_permanent_collection(b, name)
        .await
        .map_err(Error::from)?;
    let proposed = DatabaseMetadata {
        id: DatabaseId::new_random(),
        timing,
        node_max_bytes: policy.node_max_bytes(),
        split_headroom_bytes: policy.split_headroom_bytes(),
    };
    match set_db_metadata(b, name, &proposed).await {
        Ok(()) => Ok(proposed),
        Err(Error::Precondition) => {
            // We raced against another instance; re-check the metadata.
            check_db_version(b, name).await
        }
        Err(e) => Err(Error::with_source("creating db metadata", e)),
    }
}

async fn check_db_version(b: &impl Backend, name: &str) -> Result<DatabaseMetadata, Error> {
    let p = format!("{name}/{DB_META_PATH}");
    let reply = b.read(&p).await?;
    let meta = pb::DatabaseMetadata::decode(reply.contents.as_slice())
        .map_err(|e| Error::with_source("decoding db metadata", e))?;
    if meta.version != DB_VERSION {
        return Err(Error::internal(format!(
            "got db version {:?}, expected {DB_VERSION:?}",
            meta.version
        )));
    }
    let bytes: [u8; DATABASE_ID_BYTES] = meta.database_id.try_into().map_err(|_| {
        Error::internal(format!(
            "database metadata ID must contain exactly {DATABASE_ID_BYTES} bytes"
        ))
    })?;
    let pending_timeout = required_duration(meta.pending_timeout_nanos, "pending_timeout_nanos")?;
    let max_clock_skew = required_duration(meta.max_clock_skew_nanos, "max_clock_skew_nanos")?;
    if pending_timeout < Duration::from_nanos(2) {
        return Err(Error::internal(
            "database pending timeout must be at least two nanoseconds",
        ));
    }
    let metadata = DatabaseMetadata {
        id: DatabaseId::from_bytes(bytes),
        timing: ProtocolTiming::new(pending_timeout, max_clock_skew),
        node_max_bytes: required_size(meta.node_max_bytes, "node_max_bytes")?,
        split_headroom_bytes: required_size(meta.split_headroom_bytes, "split_headroom_bytes")?,
    };
    metadata.node_size_policy(NodeSizePolicy::default())?;
    Ok(metadata)
}

async fn set_db_metadata(
    b: &impl Backend,
    name: &str,
    metadata: &DatabaseMetadata,
) -> Result<(), Error> {
    let p = format!("{name}/{DB_META_PATH}");
    let body = pb::DatabaseMetadata {
        version: DB_VERSION.to_string(),
        database_id: metadata.id.as_bytes().to_vec(),
        node_max_bytes: Some(metadata.node_max_bytes as u64),
        split_headroom_bytes: Some(metadata.split_headroom_bytes as u64),
        pending_timeout_nanos: Some(duration_nanos(metadata.timing.pending_timeout())?),
        max_clock_skew_nanos: Some(duration_nanos(metadata.timing.max_clock_skew())?),
    }
    .encode_to_vec();
    b.write_if_not_exists(&p, body).await?;
    Ok(())
}

fn required_size(value: Option<u64>, field: &str) -> Result<usize, Error> {
    let value =
        value.ok_or_else(|| Error::internal(format!("database metadata missing {field}")))?;
    usize::try_from(value)
        .map_err(|_| Error::internal(format!("database metadata {field} exceeds platform size")))
}

fn required_duration(value: Option<u64>, field: &str) -> Result<Duration, Error> {
    value
        .map(Duration::from_nanos)
        .ok_or_else(|| Error::internal(format!("database metadata missing {field}")))
}

fn validate_limits(policy: NodeSizePolicy) -> Result<(), Error> {
    if !policy.key_fits(b"") {
        return Err(Error::InvalidInput(
            "coordination limits cannot admit even an empty key".into(),
        ));
    }
    Ok(())
}

fn validate_timing(timing: ProtocolTiming) -> Result<(), Error> {
    if timing.pending_timeout() < Duration::from_nanos(2) {
        return Err(Error::InvalidInput(
            "pending timeout must be at least two nanoseconds".into(),
        ));
    }
    duration_nanos(timing.pending_timeout())?;
    duration_nanos(timing.max_clock_skew())?;
    Ok(())
}

fn duration_nanos(duration: Duration) -> Result<u64, Error> {
    u64::try_from(duration.as_nanos()).map_err(|_| {
        Error::InvalidInput("protocol duration exceeds unsigned 64-bit nanoseconds".into())
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{BackendOp, HookBackend};

    #[tokio::test]
    async fn create_then_validate_round_trips_through_proto() {
        let b = MemoryBackend::new();
        // First open creates the metadata object.
        let created = check_or_create_db_meta(
            &b,
            "mydb",
            NodeSizePolicy::default(),
            ProtocolTiming::new(Duration::from_nanos(123_456_789), Duration::ZERO),
        )
        .await
        .unwrap();
        // Second open decodes the freshly written proto and validates it.
        let reopened = check_or_create_db_meta(
            &b,
            "mydb",
            NodeSizePolicy::default(),
            ProtocolTiming::default(),
        )
        .await
        .unwrap();
        assert_eq!(created, reopened);
    }

    #[tokio::test]
    async fn arbitrary_16_byte_id_is_accepted() {
        let b = MemoryBackend::new();
        let id = DatabaseId::from_bytes([
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f,
        ]);
        let body = pb::DatabaseMetadata {
            version: DB_VERSION.to_string(),
            database_id: id.as_bytes().to_vec(),
            node_max_bytes: Some(1024 * 1024),
            split_headroom_bytes: Some(64 * 1024),
            pending_timeout_nanos: Some(15_000_000_000),
            max_clock_skew_nanos: Some(30_000_000_000),
        }
        .encode_to_vec();
        b.write_if_not_exists(&format!("mydb/{DB_META_PATH}"), body)
            .await
            .unwrap();

        assert_eq!(
            check_or_create_db_meta(
                &b,
                "mydb",
                NodeSizePolicy::default(),
                ProtocolTiming::default()
            )
            .await
            .unwrap()
            .id,
            id
        );
    }

    #[tokio::test]
    async fn previous_version_is_rejected() {
        let b = MemoryBackend::new();
        let p = format!("mydb/{DB_META_PATH}");
        let body = pb::DatabaseMetadata {
            version: "v4".to_string(),
            database_id: DatabaseId::new_random().as_bytes().to_vec(),
            node_max_bytes: None,
            split_headroom_bytes: None,
            pending_timeout_nanos: None,
            max_clock_skew_nanos: None,
        }
        .encode_to_vec();
        b.write_if_not_exists(&p, body).await.unwrap();

        let err = check_or_create_db_meta(
            &b,
            "mydb",
            NodeSizePolicy::default(),
            ProtocolTiming::default(),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&err, Error::Internal { msg, .. } if msg.contains("v4") && msg.contains("v5")),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn invalid_id_length_is_rejected() {
        let b = MemoryBackend::new();
        for (name, database_id) in [("missing", vec![]), ("short", vec![0; 15])] {
            let p = format!("{name}/{DB_META_PATH}");
            let body = pb::DatabaseMetadata {
                version: DB_VERSION.to_string(),
                database_id,
                node_max_bytes: None,
                split_headroom_bytes: None,
                pending_timeout_nanos: None,
                max_clock_skew_nanos: None,
            }
            .encode_to_vec();
            b.write_if_not_exists(&p, body).await.unwrap();

            let err = check_or_create_db_meta(
                &b,
                name,
                NodeSizePolicy::default(),
                ProtocolTiming::default(),
            )
            .await
            .unwrap_err();
            assert!(matches!(err, Error::Internal { .. }), "{name}: got {err:?}");
        }
    }

    #[tokio::test]
    async fn legacy_naked_string_body_is_rejected() {
        // A pre-v2 database stored the version as a naked string, which is not a
        // valid protobuf message; opening it must fail rather than misread.
        let b = MemoryBackend::new();
        let p = format!("mydb/{DB_META_PATH}");
        b.write_if_not_exists(&p, b"v0".to_vec()).await.unwrap();

        let err = check_or_create_db_meta(
            &b,
            "mydb",
            NodeSizePolicy::default(),
            ProtocolTiming::default(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, Error::Internal { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn invalid_metadata_stops_open_before_engine_initialization() {
        type MetadataEdit = fn(&mut pb::DatabaseMetadata);
        let cases: &[(&str, MetadataEdit)] = &[
            ("missing hard cap", |m| m.node_max_bytes = None),
            ("missing headroom", |m| m.split_headroom_bytes = None),
            ("missing timeout", |m| m.pending_timeout_nanos = None),
            ("missing skew", |m| m.max_clock_skew_nanos = None),
            ("zero timeout", |m| m.pending_timeout_nanos = Some(0)),
            ("zero refresh interval", |m| {
                m.pending_timeout_nanos = Some(1)
            }),
            ("zero content budget", |m| {
                m.split_headroom_bytes = m.node_max_bytes
            }),
            ("headroom above cap", |m| {
                m.split_headroom_bytes = Some(1025)
            }),
            ("unusable cap", |m| {
                m.node_max_bytes = Some(1);
                m.split_headroom_bytes = Some(0);
            }),
            ("missing version", |m| m.version.clear()),
            ("future version", |m| m.version = "future".into()),
            ("missing ID", |m| m.database_id.clear()),
        ];
        for (name, edit) in cases {
            let mut metadata = pb::DatabaseMetadata {
                version: DB_VERSION.into(),
                database_id: DatabaseId::new_random().as_bytes().to_vec(),
                node_max_bytes: Some(1024),
                split_headroom_bytes: Some(128),
                pending_timeout_nanos: Some(250_000_000),
                max_clock_skew_nanos: Some(0),
            };
            edit(&mut metadata);
            let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
            backend
                .write_if_not_exists("invalid/glassdb", metadata.encode_to_vec())
                .await
                .unwrap();
            backend.set_before(|op| {
                assert!(
                    matches!(op, BackendOp::Read { path } if *path == "invalid/glassdb"),
                    "invalid metadata must stop opening before any other access: {op:?}"
                );
                Box::pin(async { Ok(()) })
            });
            let result = crate::Database::open("invalid", backend).await;
            assert!(matches!(result, Err(Error::Internal { .. })), "{name}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_creators_load_one_complete_metadata_record() {
        let memory = Arc::new(MemoryBackend::new());
        let loser_backend = HookBackend::new(memory.clone());
        let reached = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        loser_backend.set_before({
            let reached = reached.clone();
            let release = release.clone();
            move |op| {
                let pause = matches!(op, BackendOp::WriteIfNotExists { path, .. }
                    if *path == "race/glassdb");
                let reached = reached.clone();
                let release = release.clone();
                Box::pin(async move {
                    if pause {
                        reached.notify_one();
                        release.notified().await;
                    }
                    Ok(())
                })
            }
        });
        let loser = tokio::spawn(async move {
            check_or_create_db_meta(
                &loser_backend,
                "race",
                NodeSizePolicy::default(),
                ProtocolTiming::default(),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), reached.notified())
            .await
            .unwrap();
        let policy = NodeSizePolicy::builder()
            .node_max_bytes(512)
            .split_headroom_bytes(128)
            .build()
            .unwrap();
        let winner = check_or_create_db_meta(&memory, "race", policy, ProtocolTiming::simulation())
            .await
            .unwrap();
        release.notify_one();
        let loser = loser.await.unwrap().unwrap();
        assert_eq!(
            loser, winner,
            "identity, limits, and timing must all come from the winner"
        );
        assert_eq!(winner.node_max_bytes, 512);
        assert_eq!(winner.split_headroom_bytes, 128);
        assert_eq!(winner.timing, ProtocolTiming::simulation());
    }

    // Golden vector: the metadata body must always encode to these exact bytes.
    // Changing the on-disk format must break this test.
    #[test]
    fn golden_encoding() {
        let id = [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f,
        ];
        let got = pb::DatabaseMetadata {
            version: DB_VERSION.to_string(),
            database_id: id.to_vec(),
            node_max_bytes: Some(1024 * 1024),
            split_headroom_bytes: Some(64 * 1024),
            pending_timeout_nanos: Some(15_000_000_000),
            max_clock_skew_nanos: Some(30_000_000_000),
        }
        .encode_to_vec();
        // field 1 (string), len 2, "v5"; field 2 (bytes), len 16, ID.
        let mut expected = vec![0x0a, 0x02, 0x76, 0x35, 0x12, 0x10];
        expected.extend(id);
        expected.extend([0x18, 0x80, 0x80, 0x40, 0x20, 0x80, 0x80, 0x04]);
        expected.extend([0x28, 0x80, 0xac, 0xc7, 0xf0, 0x37]);
        expected.extend([0x30, 0x80, 0xd8, 0x8e, 0xe1, 0x6f]);
        assert_eq!(got, expected, "db-metadata encoding drifted");
    }
}
