//! Transaction API workload for read-your-writes, deletes, aborts, and atomicity.
//! Clients own disjoint keys so every possible in-doubt state can be modeled
//! exactly; shared-key lost updates remain the RMW workload's responsibility.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use arbitrary::{Arbitrary, Unstructured};
use glassdb_backend::Backend;

use crate::{Database, Error};

use super::harness::{SimWorkload, open_det_db};
use super::{MAX_CLIENTS, MAX_OPS_PER_CLIENT, assert_valid_listing, key_name, tiny_split_policy};
// ===========================================================================
// Transaction API workload (inspired by FoundationDB FuzzApiCorrectness).
//
// Each operation is a small transaction program containing arbitrary reads,
// writes, and deletes, optionally ending in an explicit abort. Clients own
// disjoint keys so the harness can maintain an exact model while their
// transactions still contend on shared B-link leaves and membership metadata.
// The model retains both outcomes of every in-doubt commit and verifies that
// the final state is one reachable sequence of atomic transactions.
// ===========================================================================

const API_KEYS: usize = 8;
const MAX_ACTIONS_PER_TX: usize = 6;
const API_COLLECTION: &[u8] = b"api";

/// One public transaction API call in an [`ApiTransaction`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiAction {
    /// Read a key and check read-your-writes and repeatable-read behavior.
    Read(usize),
    /// Stage a one-byte value for a key.
    Write(usize, u8),
    /// Stage a key deletion.
    Delete(usize),
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

impl<'a> Arbitrary<'a> for ApiWorkload {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let nclients = 2 + (u.arbitrary::<u8>()? as usize % (MAX_CLIENTS - 1));
        let mut clients = Vec::with_capacity(nclients);
        for client in 0..nclients {
            let owned: Vec<usize> = (0..API_KEYS)
                .filter(|key| key % nclients == client)
                .collect();
            let ntxs = u.arbitrary::<u8>()? as usize % (MAX_OPS_PER_CLIENT + 1);
            let mut txs = Vec::with_capacity(ntxs);
            for _ in 0..ntxs {
                let nactions = 1 + (u.arbitrary::<u8>()? as usize % MAX_ACTIONS_PER_TX);
                let mut actions = Vec::with_capacity(nactions);
                for _ in 0..nactions {
                    let key = owned[u.arbitrary::<u8>()? as usize % owned.len()];
                    actions.push(match u.arbitrary::<u8>()? % 3 {
                        0 => ApiAction::Read(key),
                        1 => ApiAction::Write(key, u.arbitrary()?),
                        _ => ApiAction::Delete(key),
                    });
                }
                txs.push(ApiTransaction {
                    client,
                    actions,
                    abort: u.arbitrary::<u8>()? % 4 == 0,
                });
            }
            clients.push(txs);
        }
        Ok(ApiWorkload { clients })
    }
}

type ApiModel = Vec<Option<u8>>;

/// Exact reachable states for each client's disjoint key slice.
pub struct ApiAcct {
    possible: Vec<BTreeSet<ApiModel>>,
}

impl ApiAcct {
    fn new(nclients: usize) -> Self {
        let initial = BTreeSet::from([vec![None; API_KEYS]]);
        ApiAcct {
            possible: vec![initial; nclients],
        }
    }

    fn apply(model: &ApiModel, program: &ApiTransaction) -> ApiModel {
        let mut next = model.clone();
        for action in &program.actions {
            match action {
                ApiAction::Read(_) => {}
                ApiAction::Write(key, value) => next[*key] = Some(*value),
                ApiAction::Delete(key) => next[*key] = None,
            }
        }
        next
    }

    fn begin(&mut self, program: &ApiTransaction) -> (BTreeSet<ApiModel>, BTreeSet<ApiModel>) {
        let before = self.possible[program.client].clone();
        let after: BTreeSet<ApiModel> = before
            .iter()
            .map(|model| Self::apply(model, program))
            .collect();
        self.possible[program.client].extend(after.iter().cloned());
        (before, after)
    }

    fn confirm(&mut self, client: usize, after: BTreeSet<ApiModel>) {
        self.possible[client] = after;
    }
}

fn possible_values(models: &BTreeSet<ApiModel>, key: usize) -> BTreeSet<Option<u8>> {
    models.iter().map(|model| model[key]).collect()
}

/// Marks the error a transaction body returns when a read observes a value
/// outside its begin-snapshot model. A stale read is expected under ADR-036
/// (execution accepts any cached state), so the body cannot assert on it
/// directly: it returns this marker instead, and the engine validates the read
/// set. A read that was merely stale fails validation and the transaction
/// retries with fresh state; a read that validates as *current* yet lies
/// outside the model is a genuine serializability violation, which surfaces as
/// this marker on the committed run and fails the test.
const OUT_OF_MODEL_MARKER: &str = "api-out-of-model";

fn out_of_model_error(key: usize, actual: Option<u8>, allowed: &BTreeSet<Option<u8>>) -> Error {
    Error::internal(format!(
        "{OUT_OF_MODEL_MARKER}: API key k{key} read {actual:?} outside modeled states {allowed:?}"
    ))
}

fn out_of_model_message(err: &Error) -> Option<&str> {
    match err {
        Error::Internal { msg, .. } if msg.starts_with(OUT_OF_MODEL_MARKER) => Some(msg),
        _ => None,
    }
}

async fn run_api_program(
    db: &Database,
    program: &ApiTransaction,
    state: &Mutex<ApiAcct>,
) -> Result<(), Error> {
    let (before, after) = if program.abort {
        (state.lock().unwrap().possible[program.client].clone(), None)
    } else {
        let (before, after) = state.lock().unwrap().begin(program);
        (before, Some(after))
    };
    let allowed: Vec<BTreeSet<Option<u8>>> = (0..API_KEYS)
        .map(|key| possible_values(&before, key))
        .collect();
    let actions = &program.actions;
    let should_abort = program.abort;
    let collection = db.collection(API_COLLECTION);
    let collection = &collection;
    let allowed = &allowed;
    let result = db
        .tx(|tx| async move {
            let mut staged = [None::<Option<u8>>; API_KEYS];
            let mut observed = [None::<Option<u8>>; API_KEYS];
            for action in actions {
                match action {
                    ApiAction::Read(key) => {
                        let actual = match tx.read(collection, &key_name(*key)).await {
                            Ok(Some(value)) => {
                                assert_eq!(
                                    value.len(),
                                    1,
                                    "API key k{key} has non-byte value {value:?}"
                                );
                                Some(value[0])
                            }
                            Ok(None) => None,
                            Err(error) => return Err(error),
                        };
                        if let Some(expected) = staged[*key] {
                            assert_eq!(
                                actual, expected,
                                "API key k{key} violated read-your-writes"
                            );
                        } else if let Some(expected) = observed[*key] {
                            assert_eq!(
                                actual, expected,
                                "API key k{key} violated repeatable reads"
                            );
                        } else if !allowed[*key].contains(&actual) {
                            // Stale reads are legal (ADR-036); let commit
                            // validation decide whether this attempt must retry
                            // rather than asserting on a possibly-stale value.
                            return Err(out_of_model_error(*key, actual, &allowed[*key]));
                        } else {
                            observed[*key] = Some(actual);
                        }
                    }
                    ApiAction::Write(key, value) => {
                        tx.write(collection, &key_name(*key), &[*value])?;
                        staged[*key] = Some(Some(*value));
                    }
                    ApiAction::Delete(key) => {
                        tx.delete(collection, &key_name(*key))?;
                        staged[*key] = Some(None);
                    }
                }
            }
            if should_abort { tx.abort() } else { Ok(()) }
        })
        .await;

    // A surfaced out-of-model marker means the engine validated the read set as
    // current yet it lies outside the begin-snapshot model: a real
    // serializability violation (a merely-stale read would have retried inside
    // the engine and never reached here).
    if let Err(error) = &result
        && let Some(message) = out_of_model_message(error)
    {
        panic!("{message}");
    }

    if program.abort {
        return match result {
            Err(Error::Aborted) => Ok(()),
            Ok(()) => panic!("explicitly aborted API transaction committed"),
            Err(error) => Err(error),
        };
    }
    result?;
    state
        .lock()
        .unwrap()
        .confirm(program.client, after.expect("commit transition"));
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

    async fn open_db(backend: &Arc<dyn Backend>) -> Result<Database, Error> {
        open_det_db(backend, tiny_split_policy()).await
    }

    async fn seed(&self, db: &Database) {
        db.collection(API_COLLECTION)
            .create()
            .await
            .expect("create API collection");
    }

    async fn run_op(
        db: &Database,
        op: &ApiTransaction,
        state: &Mutex<ApiAcct>,
    ) -> Result<(), Error> {
        run_api_program(db, op, state).await
    }

    async fn verify(&self, db: &Database, state: &Mutex<ApiAcct>, _faults_enabled: bool) {
        let collection = db.collection(API_COLLECTION);
        let listed: Vec<Vec<u8>> = collection
            .keys()
            .await
            .expect("final API listing")
            .collect::<Result<_, _>>()
            .expect("final API listing");
        assert_valid_listing(&listed, API_KEYS);

        let nclients = self.clients.len();
        let mut actual = vec![vec![None; API_KEYS]; nclients];
        for key in 0..API_KEYS {
            let name = key_name(key);
            let value = match collection.read(&name).await {
                Ok(Some(value)) => {
                    assert_eq!(
                        value.len(),
                        1,
                        "API key k{key} has non-byte value {value:?}"
                    );
                    assert!(
                        listed.contains(&name),
                        "API key k{key} readable but not listed"
                    );
                    Some(value[0])
                }
                Ok(None) => {
                    assert!(
                        !listed.contains(&name),
                        "API key k{key} listed but not readable"
                    );
                    None
                }
                Err(error) => panic!("final API read failed for k{key}: {error}"),
            };
            actual[key % nclients][key] = value;
        }

        let acct = state.lock().unwrap();
        for (client, actual) in actual.iter().enumerate() {
            assert!(
                acct.possible[client].contains(actual),
                "client {client} final API state {:?} is not reachable; expected one of {:?}",
                actual,
                acct.possible[client]
            );
        }
    }
}
