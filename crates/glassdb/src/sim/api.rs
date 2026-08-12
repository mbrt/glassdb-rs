//! Transaction API workload for collection lifecycle, read-your-writes, aborts,
//! and atomicity. Clients own disjoint keys and collection names so every
//! possible in-doubt state can be modeled exactly; they still contend on shared
//! B-link leaves and collection-directory metadata.

use std::sync::{Arc, Mutex};

use glassdb_backend::Backend;

use crate::{Database, Error};

mod executor;
mod generator;
mod model;
mod observation;
mod oracle;

pub use model::ApiAcct;

use super::harness::{SimWorkload, open_det_db};
use super::{SimMedia, tiny_split_policy};
use executor::StepResult;
// ===========================================================================
// Transaction API workload (inspired by FoundationDB FuzzApiCorrectness).
//
// Each operation is either a small key transaction program or one collection
// lifecycle program, optionally ending in an explicit abort. Keeping one
// lifecycle action per generated transaction avoids constructing combinations
// that the public API deliberately rejects (such as drop-after-write) while the
// action itself can still compose collection and data changes atomically.
// Clients own disjoint keys and collection names, letting the harness retain
// both outcomes of every in-doubt commit and verify an exact reachable state.
// ===========================================================================

const API_KEYS: usize = 8;
const API_COLLECTION: &[u8] = b"api";
const API_COLLECTION_SLOTS: usize = 2;
const API_NESTED_COLLECTION: &[u8] = b"nested";
const API_COLLECTION_VALUE_KEY: &[u8] = b"value";

/// One public transaction API call in an [`ApiTransaction`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiAction {
    /// Read a key and check read-your-writes and repeatable-read behavior.
    Read(usize),
    /// Stage a one-byte value for a key.
    Write(usize, u8),
    /// Stage a key deletion.
    Delete(usize),
    /// Strictly create a client-owned top-level collection.
    CreateCollection(usize),
    /// Idempotently create a client-owned top-level collection.
    CreateCollectionIfAbsent(usize),
    /// Read and cross-check a collection through direct and path APIs.
    ReadCollection(usize),
    /// Create a collection if needed and atomically write its modeled value.
    WriteCollection(usize, u8),
    /// Ensure a nested child exists using strict creation when possible.
    CreateNestedCollection(usize),
    /// Ensure a nested child exists and atomically write its modeled value.
    WriteNestedCollection(usize, u8),
    /// Drop a nested child when it exists.
    DropNestedCollection(usize),
    /// Drop a top-level collection, expecting `NotEmpty` while its child exists.
    DropCollection(usize),
    /// Cross-check every client-owned binding through existence and listing APIs.
    InspectCollections,
}

/// A sequence of API calls executed atomically by one client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiTransaction {
    /// Owning client. Its keys are the residue class `key % client_count`.
    pub client: usize,
    /// Calls made in order within one transaction.
    pub actions: Vec<ApiAction>,
    /// Whether the transaction explicitly aborts after running its calls.
    pub abort: bool,
}

/// Random transaction programs executed by concurrent clients.
#[derive(Debug, Clone)]
pub struct ApiWorkload {
    /// Per-client transaction sequences.
    pub clients: Vec<Vec<ApiTransaction>>,
}

impl Default for ApiWorkload {
    fn default() -> Self {
        ApiWorkload {
            clients: vec![Vec::new(), Vec::new()],
        }
    }
}

/// Marks an oracle failure that must first pass transaction read validation.
const API_INVARIANT_MARKER: &str = "api-invariant";

fn api_invariant_error(detail: impl std::fmt::Display) -> Error {
    Error::internal(format!("{API_INVARIANT_MARKER}: {detail}"))
}

fn api_invariant_message(error: &Error) -> Option<&str> {
    match error {
        Error::Internal { msg, .. } if msg.starts_with(API_INVARIANT_MARKER) => Some(msg),
        _ => None,
    }
}

fn check_api_invariant(condition: bool, detail: impl std::fmt::Display) -> Result<(), Error> {
    crate::ensure_tx!(condition, api_invariant_error(detail));
    Ok(())
}

async fn run_api_step(
    db: &Database,
    program: &ApiTransaction,
    state: &Mutex<ApiAcct>,
) -> Result<(), Error> {
    let (before, after) = if program.abort {
        let before = state.lock().unwrap().possible(program.client).clone();
        let after = ApiAcct::project(&before, program);
        (before, after)
    } else {
        state.lock().unwrap().begin(program)
    };

    match executor::execute_step(db, program, &before, &after).await? {
        StepResult::Committed => state.lock().unwrap().confirm(program.client, after),
        StepResult::ExplicitlyAborted => {}
    }
    Ok(())
}

impl SimWorkload for ApiWorkload {
    type Op = ApiTransaction;
    type State = Mutex<ApiAcct>;

    fn clients(&self) -> &[Vec<ApiTransaction>] {
        &self.clients
    }

    fn new_state(&self) -> Mutex<ApiAcct> {
        Mutex::new(ApiAcct::new(self.clients.len()))
    }

    async fn open_db(
        backend: &Arc<dyn Backend>,
        media: Option<SimMedia>,
    ) -> Result<Database, Error> {
        open_det_db(
            backend,
            tiny_split_policy(),
            glassdb_storage::InlinePolicy::default(),
            media,
        )
        .await
    }

    async fn seed(&self, db: &Database) {
        db.root_collection()
            .create_collection_if_absent(API_COLLECTION)
            .await
            .expect("create API collection");
    }

    async fn run_op(
        db: &Database,
        op: &ApiTransaction,
        state: &Mutex<ApiAcct>,
    ) -> Result<(), Error> {
        run_api_step(db, op, state).await
    }

    async fn verify(&self, db: &Database, state: &Mutex<ApiAcct>, _failures_enabled: bool) {
        oracle::verify_final_state(db, state, self.clients.len()).await;
    }
}
