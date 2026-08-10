#![allow(deprecated)]

use glassdb_data::{CollectionAddress, TxId};
use glassdb_storage::StorageError;
use glassdb_trans::{Engine, LeafCoverage, ScanAccess, ScanMutation, ScanRange, ScanResult};

fn legacy_access(
    collection: CollectionAddress,
    range: ScanRange,
    overlay: Vec<ScanMutation>,
    keys: Vec<Vec<u8>>,
    frontier: Option<Vec<u8>>,
    covered: Vec<LeafCoverage>,
) -> ScanAccess {
    ScanAccess {
        collection,
        range,
        overlay,
        keys,
        frontier,
        covered,
    }
}

fn legacy_result(
    keys: Vec<Vec<u8>>,
    covered: Vec<LeafCoverage>,
    frontier: Option<Vec<u8>>,
) -> ScanResult {
    ScanResult {
        keys,
        covered,
        frontier,
    }
}

fn opaque_access(
    result: ScanResult,
    collection: CollectionAddress,
    range: ScanRange,
    overlay: Vec<ScanMutation>,
) -> ScanAccess {
    result.into_access(collection, range, overlay)
}

async fn engine_entry_points_compile(
    engine: &Engine,
    collection: &CollectionAddress,
    range: &ScanRange,
    overlay: &[ScanMutation],
    own_lock_holder: Option<&TxId>,
    cap: Option<&[u8]>,
) -> Result<(), StorageError> {
    engine.scan(collection, range, overlay).await?;
    engine
        .scan_keys(collection, range, overlay, own_lock_holder, cap)
        .await?;
    Ok(())
}

#[test]
fn legacy_and_opaque_scan_construction_compile() {
    let _legacy_access = legacy_access;
    let _legacy_result = legacy_result;
    let _opaque_access = opaque_access;
    let _engine_entry_points = engine_entry_points_compile;
}
