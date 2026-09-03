//! Database-backed execution for generated transaction API programs.

use std::collections::BTreeSet;

use crate::{Collection, CollectionPath, Database, Error, Transaction};

use super::model::{
    ApiModel, expected_catalog_states, expected_collection_states, possible_values,
};
use super::observation::{api_collection_name, inspect_collection, read_collection_value};
use super::{
    API_COLLECTION, API_COLLECTION_SLOTS, API_COLLECTION_VALUE_KEY, API_KEYS,
    API_NESTED_COLLECTION, ApiAction, ApiTransaction, api_invariant_error, api_invariant_message,
    check_api_invariant,
};
use crate::sim::key_name;

/// Definite outcome of one API transaction program.
pub(super) enum StepResult {
    /// The database confirmed that every action committed atomically.
    Committed,
    /// The transaction confirmed the program's explicit abort.
    ExplicitlyAborted,
}

/// Executes one generated program and distinguishes its definite outcomes.
pub(super) async fn execute_step(
    db: &Database,
    program: &ApiTransaction,
    before: &BTreeSet<ApiModel>,
    after: &BTreeSet<ApiModel>,
) -> Result<StepResult, Error> {
    let allowed: Vec<BTreeSet<Option<u8>>> = (0..API_KEYS)
        .map(|key| possible_values(before, key))
        .collect();
    let collection = db
        .open_collection(&CollectionPath::new(API_COLLECTION)?)
        .await?;
    let result = execute_program(db, &collection, program, &allowed, after).await;

    // A stale observation retries before this error can escape. A stable one is
    // a real workload invariant failure.
    if let Err(error) = &result
        && let Some(message) = api_invariant_message(error)
    {
        panic!("{message}");
    }

    if program.abort {
        return match result {
            Err(Error::Aborted) => Ok(StepResult::ExplicitlyAborted),
            Ok(()) => panic!("explicitly aborted API transaction committed"),
            Err(error) => Err(error),
        };
    }
    result?;
    Ok(StepResult::Committed)
}

async fn execute_program(
    db: &Database,
    collection: &Collection,
    program: &ApiTransaction,
    allowed: &[BTreeSet<Option<u8>>],
    expected_after: &BTreeSet<ApiModel>,
) -> Result<(), Error> {
    let actions = &program.actions;
    let client = program.client;
    let should_abort = program.abort;
    db.tx(|tx| async move {
        let mut staged = [None::<Option<u8>>; API_KEYS];
        let mut observed = [None::<Option<u8>>; API_KEYS];
        for action in actions {
            match action {
                ApiAction::Read(key) => {
                    let actual = match tx.read(collection, &key_name(*key)).await {
                        Ok(Some(value)) => {
                            check_api_invariant(
                                value.len() == 1,
                                format!("API key k{key} has non-byte value {value:?}"),
                            )?;
                            Some(value[0])
                        }
                        Ok(None) => None,
                        Err(error) => return Err(error),
                    };
                    if let Some(expected) = staged[*key] {
                        check_api_invariant(
                            actual == expected,
                            format!("API key k{key} violated read-your-writes"),
                        )?;
                    } else if let Some(expected) = observed[*key] {
                        check_api_invariant(
                            actual == expected,
                            format!("API key k{key} violated repeatable reads"),
                        )?;
                    } else if !allowed[*key].contains(&actual) {
                        return Err(api_invariant_error(format!(
                            "API key k{key} read {actual:?} outside modeled states {:?}",
                            allowed[*key]
                        )));
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
                ApiAction::CreateCollection(_)
                | ApiAction::CreateCollectionIfAbsent(_)
                | ApiAction::ReadCollection(_)
                | ApiAction::WriteCollection(_, _)
                | ApiAction::CreateNestedCollection(_)
                | ApiAction::WriteNestedCollection(_, _)
                | ApiAction::DropNestedCollection(_)
                | ApiAction::DropCollection(_)
                | ApiAction::InspectCollections => {
                    execute_collection_action(&tx, action, client, expected_after).await?;
                }
            }
        }
        if should_abort { tx.abort() } else { Ok(()) }
    })
    .await
}

async fn execute_collection_action(
    tx: &Transaction,
    action: &ApiAction,
    client: usize,
    after: &BTreeSet<ApiModel>,
) -> Result<(), Error> {
    let root = tx.root_collection();
    match action {
        ApiAction::CreateCollection(slot) => {
            let name = api_collection_name(client, *slot);
            let existed = tx.collection_exists(&root, &name).await?;
            let result = tx.create_collection(&root, &name).await;
            match (existed, result) {
                (false, Ok(_)) | (true, Err(Error::AlreadyExists)) => {}
                (false, Err(Error::AlreadyExists)) => {
                    return Err(api_invariant_error(format!(
                        "strict create rejected absent collection {name:?}"
                    )));
                }
                (true, Ok(_)) => {
                    return Err(api_invariant_error(format!(
                        "strict create replaced existing collection {name:?}"
                    )));
                }
                (_, Err(error)) => return Err(error),
            }
        }
        ApiAction::CreateCollectionIfAbsent(slot) => {
            ensure_collection(tx, client, *slot).await?;
        }
        ApiAction::ReadCollection(_) | ApiAction::InspectCollections => {}
        ApiAction::WriteCollection(slot, value) => {
            let collection = ensure_collection(tx, client, *slot).await?;
            tx.write(&collection, API_COLLECTION_VALUE_KEY, &[*value])?;
            let actual =
                read_collection_value(tx, &collection, "newly written top-level collection")
                    .await?;
            check_api_invariant(
                actual == Some(*value),
                "top-level collection violated read-your-writes",
            )?;
        }
        ApiAction::CreateNestedCollection(slot) => {
            let collection = ensure_collection(tx, client, *slot).await?;
            let existed = tx
                .collection_exists(&collection, API_NESTED_COLLECTION)
                .await?;
            let result = tx
                .create_collection(&collection, API_NESTED_COLLECTION)
                .await;
            match (existed, result) {
                (false, Ok(_)) | (true, Err(Error::AlreadyExists)) => {}
                (false, Err(Error::AlreadyExists)) => {
                    return Err(api_invariant_error(
                        "strict create rejected an absent nested collection",
                    ));
                }
                (true, Ok(_)) => {
                    return Err(api_invariant_error(
                        "strict create replaced an existing nested collection",
                    ));
                }
                (_, Err(error)) => return Err(error),
            }
        }
        ApiAction::WriteNestedCollection(slot, value) => {
            let collection = ensure_collection(tx, client, *slot).await?;
            let existed = tx
                .collection_exists(&collection, API_NESTED_COLLECTION)
                .await?;
            let (child, created) = tx
                .create_collection_if_absent(&collection, API_NESTED_COLLECTION)
                .await?;
            check_api_invariant(
                created != existed,
                "nested create-if-absent reported the wrong outcome",
            )?;
            tx.write(&child, API_COLLECTION_VALUE_KEY, &[*value])?;
            let actual =
                read_collection_value(tx, &child, "newly written nested collection").await?;
            check_api_invariant(
                actual == Some(*value),
                "nested collection violated read-your-writes",
            )?;
        }
        ApiAction::DropNestedCollection(slot) => {
            let name = api_collection_name(client, *slot);
            if tx.collection_exists(&root, &name).await? {
                let collection = tx.open_collection(&root, &name).await?;
                if tx
                    .collection_exists(&collection, API_NESTED_COLLECTION)
                    .await?
                {
                    let child = tx
                        .open_collection(&collection, API_NESTED_COLLECTION)
                        .await?;
                    tx.drop_collection(&child).await?;
                    ensure_dropped_handle_is_stale(tx, &child).await?;
                }
            }
        }
        ApiAction::DropCollection(slot) => {
            let name = api_collection_name(client, *slot);
            if tx.collection_exists(&root, &name).await? {
                let collection = tx.open_collection(&root, &name).await?;
                let child_exists = tx
                    .collection_exists(&collection, API_NESTED_COLLECTION)
                    .await?;
                match (child_exists, tx.drop_collection(&collection).await) {
                    (true, Err(Error::NotEmpty)) => {}
                    (false, Ok(())) => {
                        ensure_dropped_handle_is_stale(tx, &collection).await?;
                    }
                    (true, Ok(())) => {
                        return Err(api_invariant_error(
                            "non-recursive drop removed a non-empty collection",
                        ));
                    }
                    (false, Err(Error::NotEmpty)) => {
                        return Err(api_invariant_error(
                            "drop reported NotEmpty for a childless collection",
                        ));
                    }
                    (_, Err(error)) => return Err(error),
                }
            }
        }
        ApiAction::Read(_) | ApiAction::Write(_, _) | ApiAction::Delete(_) => {
            return Err(Error::internal(
                "key action routed to collection action executor",
            ));
        }
    }

    if let Some(slot) = collection_slot(action) {
        let actual = inspect_collection(tx, client, slot).await?;
        let allowed = expected_collection_states(after, slot);
        if !allowed.contains(&actual) {
            return Err(api_invariant_error(format!(
                "collection slot {slot} observed {actual:?} outside modeled states {allowed:?}"
            )));
        }
    } else {
        let mut actual = Vec::with_capacity(API_COLLECTION_SLOTS);
        for slot in 0..API_COLLECTION_SLOTS {
            actual.push(inspect_collection(tx, client, slot).await?);
        }
        let allowed = expected_catalog_states(after);
        if !allowed.contains(&actual) {
            return Err(api_invariant_error(format!(
                "collection catalog observed {actual:?} outside modeled states {allowed:?}"
            )));
        }
    }
    Ok(())
}

async fn ensure_collection(
    tx: &Transaction,
    client: usize,
    slot: usize,
) -> Result<Collection, Error> {
    let root = tx.root_collection();
    let name = api_collection_name(client, slot);
    let existed = tx.collection_exists(&root, &name).await?;
    let (collection, created) = tx.create_collection_if_absent(&root, &name).await?;
    check_api_invariant(
        created != existed,
        format!("create-if-absent reported the wrong outcome for {name:?}"),
    )?;
    Ok(collection)
}

async fn ensure_dropped_handle_is_stale(
    tx: &Transaction,
    collection: &Collection,
) -> Result<(), Error> {
    match tx.read(collection, API_COLLECTION_VALUE_KEY).await {
        Err(Error::StaleCollection) => Ok(()),
        Err(error) => Err(error),
        Ok(value) => Err(api_invariant_error(format!(
            "dropped collection handle read {value:?} instead of becoming stale"
        ))),
    }
}

fn collection_slot(action: &ApiAction) -> Option<usize> {
    match action {
        ApiAction::CreateCollection(slot)
        | ApiAction::CreateCollectionIfAbsent(slot)
        | ApiAction::ReadCollection(slot)
        | ApiAction::WriteCollection(slot, _)
        | ApiAction::CreateNestedCollection(slot)
        | ApiAction::WriteNestedCollection(slot, _)
        | ApiAction::DropNestedCollection(slot)
        | ApiAction::DropCollection(slot) => Some(*slot),
        ApiAction::Read(_)
        | ApiAction::Write(_, _)
        | ApiAction::Delete(_)
        | ApiAction::InspectCollections => None,
    }
}
