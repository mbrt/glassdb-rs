//! The cells of the `contention` experiment: writers whose transactions share
//! some of their keys.

use std::error::Error;

use serde::Deserialize;
use toml::Value;
use toml::value::Table;

use crate::runner::config::typed;

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct OverlapsParams {
    /// The shape whose keys and overlap the values set.
    shape: String,
    /// The largest key count of a transaction.
    max_keys: usize,
}

/// Returns one coupled value for each key count from 1 to `max-keys` and each
/// count of shared keys from 1 to that key count. A value sets the keys of the
/// shape and its `overlap` source, so that all writers share the first
/// `shared` keys and own the others.
pub(crate) fn overlaps(params: &Value) -> Result<Vec<Value>, Box<dyn Error>> {
    let params: OverlapsParams = typed(params, "contention-overlaps")?;
    if params.max_keys == 0 {
        return Err("max-keys must be at least 1".into());
    }
    let mut values = Vec::new();
    for keys in 1..=params.max_keys {
        for shared in 1..=keys {
            let mut overlap = Table::new();
            overlap.insert("shared".into(), Value::Integer(shared as i64));
            let mut source = Table::new();
            source.insert("overlap".into(), Value::Table(overlap));
            let mut set = Table::new();
            set.insert(
                format!("shapes.{}.keys", params.shape),
                Value::Integer(keys as i64),
            );
            set.insert(
                format!("shapes.{}.source", params.shape),
                Value::Table(source),
            );
            let mut value = Table::new();
            value.insert(
                "label".into(),
                Value::String(format!("keys{keys}-shared{shared}")),
            );
            value.insert("set".into(), Value::Table(set));
            values.push(Value::Table(value));
        }
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_key_count_has_every_overlap_up_to_it() {
        let params: Table = toml::from_str("shape = \"w\"\nmax-keys = 3").unwrap();
        let values = overlaps(&Value::Table(params)).unwrap();
        let labels: Vec<_> = values
            .iter()
            .map(|v| v["label"].as_str().unwrap())
            .collect();
        assert_eq!(
            labels,
            [
                "keys1-shared1",
                "keys2-shared1",
                "keys2-shared2",
                "keys3-shared1",
                "keys3-shared2",
                "keys3-shared3"
            ]
        );
        let set = &values[4]["set"];
        assert_eq!(set["shapes.w.keys"].as_integer(), Some(3));
        assert_eq!(
            set["shapes.w.source"]["overlap"]["shared"].as_integer(),
            Some(2)
        );
    }
}
