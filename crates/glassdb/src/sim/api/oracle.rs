//! Final-state verification for the transaction API workload.

use std::collections::BTreeSet;
use std::sync::Mutex;

use crate::{CollectionPath, Database};

use super::model::{ApiAcct, ApiModel};
use super::observation::{api_collection_name, inspect_collection, listed_collection_names};
use super::{API_COLLECTION, API_COLLECTION_SLOTS, API_KEYS, check_api_invariant};
use crate::sim::{assert_valid_listing, key_name};

/// Reads and verifies the final committed API state against every reachable model.
pub(super) async fn verify_final_state(db: &Database, state: &Mutex<ApiAcct>, nclients: usize) {
    let collection = db
        .open_collection(&CollectionPath::new(API_COLLECTION).unwrap())
        .await
        .expect("open API collection");
    let listed: Vec<Vec<u8>> = collection
        .iter_keys()
        .await
        .expect("final API listing")
        .collect();
    assert_valid_listing(&listed, API_KEYS);

    let mut actual = vec![ApiModel::new(); nclients];
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
        actual[key % nclients].set_value(key, value);
    }

    let catalogs = db
        .tx(|tx| async move {
            let root = tx.root_collection();
            let root_names = listed_collection_names(&tx, &root).await?;
            let mut known_names = BTreeSet::from([API_COLLECTION.to_vec()]);
            for client in 0..nclients {
                for slot in 0..API_COLLECTION_SLOTS {
                    known_names.insert(api_collection_name(client, slot));
                }
            }
            check_api_invariant(
                root_names.iter().all(|name| known_names.contains(name)),
                format!("root listing contains an unmodeled collection: {root_names:?}"),
            )?;

            let mut catalogs = vec![Vec::with_capacity(API_COLLECTION_SLOTS); nclients];
            for (client, client_catalog) in catalogs.iter_mut().enumerate() {
                for slot in 0..API_COLLECTION_SLOTS {
                    client_catalog.push(inspect_collection(&tx, client, slot).await?);
                }
            }
            Ok(catalogs)
        })
        .await
        .expect("verify final collection catalog");
    for (model, catalog) in actual.iter_mut().zip(catalogs) {
        model.set_collections(catalog);
    }

    assert_reachable(&actual, &state.lock().unwrap());
}

fn assert_reachable(actual: &[ApiModel], account: &ApiAcct) {
    for (client, actual) in actual.iter().enumerate() {
        assert!(
            account.contains(client, actual),
            "client {client} final API state {:?} is not reachable; expected one of {:?}",
            actual,
            account.possible(client)
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reachable_verdicts_and_diagnostics_are_stable() {
        let account = ApiAcct::new(1);
        assert_reachable(&[ApiModel::new()], &account);

        let mut corrupted = ApiModel::new();
        corrupted.set_value(0, Some(7));
        let panic = std::panic::catch_unwind(|| assert_reachable(&[corrupted], &account))
            .expect_err("corrupted model unexpectedly passed the API oracle");
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .expect("oracle panic did not contain a string diagnostic");
        assert_eq!(
            message,
            "client 0 final API state ApiModel { values: [Some(7), None, None, None, None, None, None, None], collections: [None, None] } is not reachable; expected one of {ApiModel { values: [None, None, None, None, None, None, None, None], collections: [None, None] }}"
        );
    }
}
