//! Read-only observations shared by API program execution and final verification.

use crate::{Collection, CollectionPath, Error, Transaction};

use super::model::{ApiChildModel, ApiCollectionModel};
use super::{
    API_COLLECTION_VALUE_KEY, API_NESTED_COLLECTION, api_invariant_error, check_api_invariant,
};

pub(super) fn api_collection_name(client: usize, slot: usize) -> Vec<u8> {
    format!("api-c{client}-{slot}").into_bytes()
}

fn api_collection_path(client: usize, slot: usize) -> CollectionPath {
    CollectionPath::new(api_collection_name(client, slot)).expect("valid API collection name")
}

fn api_nested_collection_path(client: usize, slot: usize) -> CollectionPath {
    api_collection_path(client, slot)
        .child(API_NESTED_COLLECTION)
        .expect("valid nested API collection name")
}

pub(super) async fn listed_collection_names(
    tx: &Transaction,
    parent: &Collection,
) -> Result<Vec<Vec<u8>>, Error> {
    Ok(tx
        .iter_collections(parent)
        .await?
        .map(|entry| entry.name)
        .collect())
}

pub(super) async fn read_collection_value(
    tx: &Transaction,
    collection: &Collection,
    context: &str,
) -> Result<Option<u8>, Error> {
    match tx.read(collection, API_COLLECTION_VALUE_KEY).await? {
        Some(value) => {
            check_api_invariant(
                value.len() == 1,
                format!("{context} has non-byte modeled value {value:?}"),
            )?;
            Ok(Some(value[0]))
        }
        None => Ok(None),
    }
}

pub(super) async fn inspect_collection(
    tx: &Transaction,
    client: usize,
    slot: usize,
) -> Result<Option<ApiCollectionModel>, Error> {
    let root = tx.root_collection();
    let name = api_collection_name(client, slot);
    let path = api_collection_path(client, slot);
    let exists = tx.collection_exists(&root, &name).await?;
    let path_exists = tx.collection_path_exists(&path).await?;
    check_api_invariant(
        path_exists == exists,
        format!("direct and path existence disagree for {name:?}"),
    )?;

    let root_names = listed_collection_names(tx, &root).await?;
    check_api_invariant(
        root_names.windows(2).all(|pair| pair[0] < pair[1]),
        format!("root collection listing is not strictly sorted: {root_names:?}"),
    )?;
    check_api_invariant(
        root_names.iter().any(|candidate| candidate == &name) == exists,
        format!("root listing disagrees with existence for {name:?}"),
    )?;

    if !exists {
        match tx.open_collection(&root, &name).await {
            Err(Error::NotFound) => {}
            Err(error) => return Err(error),
            Ok(_) => {
                return Err(api_invariant_error(format!(
                    "direct open found absent collection {name:?}"
                )));
            }
        }
        match tx.open_collection_path(&path).await {
            Err(Error::NotFound) => {}
            Err(error) => return Err(error),
            Ok(_) => {
                return Err(api_invariant_error(format!(
                    "path open found absent collection {name:?}"
                )));
            }
        }
        return Ok(None);
    }

    let collection = tx.open_collection(&root, &name).await?;
    let path_collection = tx.open_collection_path(&path).await?;
    let value = read_collection_value(tx, &collection, "top-level collection").await?;
    let path_value =
        read_collection_value(tx, &path_collection, "path-opened top-level collection").await?;
    check_api_invariant(
        path_value == value,
        format!("direct and path opens disagree for {name:?}"),
    )?;

    let nested_path = api_nested_collection_path(client, slot);
    let child_exists = tx
        .collection_exists(&collection, API_NESTED_COLLECTION)
        .await?;
    let path_child_exists = tx.collection_path_exists(&nested_path).await?;
    check_api_invariant(
        path_child_exists == child_exists,
        format!("direct and path existence disagree for nested child of {name:?}"),
    )?;
    let children = listed_collection_names(tx, &collection).await?;
    let expected_children = if child_exists {
        vec![API_NESTED_COLLECTION.to_vec()]
    } else {
        Vec::new()
    };
    check_api_invariant(
        children == expected_children,
        format!("nested listing disagrees for {name:?}"),
    )?;

    let child = if child_exists {
        let child = tx
            .open_collection(&collection, API_NESTED_COLLECTION)
            .await?;
        let path_child = tx.open_collection_path(&nested_path).await?;
        let value = read_collection_value(tx, &child, "nested collection").await?;
        let path_value =
            read_collection_value(tx, &path_child, "path-opened nested collection").await?;
        check_api_invariant(
            path_value == value,
            format!("direct and path opens disagree for nested child of {name:?}"),
        )?;
        Some(ApiChildModel::new(value))
    } else {
        match tx.open_collection(&collection, API_NESTED_COLLECTION).await {
            Err(Error::NotFound) => {}
            Err(error) => return Err(error),
            Ok(_) => {
                return Err(api_invariant_error(format!(
                    "direct open found absent nested child of {name:?}"
                )));
            }
        }
        match tx.open_collection_path(&nested_path).await {
            Err(Error::NotFound) => {}
            Err(error) => return Err(error),
            Ok(_) => {
                return Err(api_invariant_error(format!(
                    "path open found absent nested child of {name:?}"
                )));
            }
        }
        None
    };

    Ok(Some(ApiCollectionModel::new(value, child)))
}
