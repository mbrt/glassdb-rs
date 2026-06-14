//! Database metadata version check. Ported from the Go `version.go`.

use std::sync::Arc;

use glassdb_backend::{Backend, Tags};
use glassdb_data::gopath;

use crate::error::Error;

const DB_VERSION: &str = "v0";
const DB_META_PATH: &str = "glassdb";
const DB_VERSION_TAG: &str = "version";

/// Verifies the database metadata exists with the expected version, creating it
/// if missing. Races against concurrent creators are resolved by re-checking.
pub(crate) async fn check_or_create_db_meta(b: &Arc<dyn Backend>, name: &str) -> Result<(), Error> {
    match check_db_version(b, name).await {
        Ok(()) => return Ok(()),
        Err(e) if !e.is_not_found() => return Err(e),
        Err(_) => {}
    }
    match set_db_metadata(b, name).await {
        Ok(()) => Ok(()),
        Err(e) if e.is_precondition() => {
            // We raced against another instance; re-check the metadata.
            check_db_version(b, name).await
        }
        Err(e) => Err(Error::Internal(format!("creating db metadata: {e}"))),
    }
}

async fn check_db_version(b: &Arc<dyn Backend>, name: &str) -> Result<(), Error> {
    let p = gopath::join(&[name, DB_META_PATH]);
    let meta = b.get_metadata(&p).await?;
    let got = meta
        .tags
        .get(DB_VERSION_TAG)
        .map(String::as_str)
        .unwrap_or("");
    if got != DB_VERSION {
        return Err(Error::Internal(format!(
            "got db version {got:?}, expected {DB_VERSION:?}"
        )));
    }
    Ok(())
}

async fn set_db_metadata(b: &Arc<dyn Backend>, name: &str) -> Result<(), Error> {
    let p = gopath::join(&[name, DB_META_PATH]);
    let mut tags = Tags::new();
    tags.insert(DB_VERSION_TAG.to_string(), DB_VERSION.to_string());
    b.write_if_not_exists(&p, Vec::new(), tags).await?;
    Ok(())
}
