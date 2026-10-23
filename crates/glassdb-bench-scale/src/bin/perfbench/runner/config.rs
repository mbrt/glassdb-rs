//! The layered configuration of the runner: loading, merging, `--set`, and
//! the expansion of an experiment into its cells.

use std::collections::BTreeSet;
use std::error::Error;
use std::path::Path;
use std::time::Duration;

use indexmap::IndexMap;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use toml::Value;
use toml::value::Table;

/// The configuration that is always the first layer.
const BUILTIN: &str = include_str!("builtin.toml");

/// The top level of the merged configuration. Each cell checks its own
/// sections; this checks the top-level keys and the experiments.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
#[allow(dead_code)]
struct Config {
    #[serde(default)]
    pools: Table,
    #[serde(default)]
    shapes: Table,
    #[serde(default)]
    workloads: Table,
    seed: Table,
    cell: Table,
    #[serde(default)]
    experiments: IndexMap<String, Experiment>,
}

/// A named entry that one invocation runs: values and axes.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Experiment {
    /// Values that replace the defaults, by dotted path.
    #[serde(default, deserialize_with = "flat_paths")]
    pub(crate) set: IndexMap<String, Value>,
    /// The axes, from the outer loop to the inner loop.
    #[serde(default)]
    pub(crate) axes: Vec<Axis>,
}

/// An ordered list of values for one or more parameters.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Axis {
    /// The parameter of a single-parameter axis.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) path: Option<String>,
    /// The name of a coupled axis.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    #[serde(default)]
    pub(crate) values: Vec<Value>,
    /// A scenario part that gives the values, as `{ NAME = PARAMS }`, in place
    /// of `values`. The loader replaces it with the values that it gives.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) generate: Option<Value>,
    #[serde(default)]
    pub(crate) start: Start,
}

/// Where a cell starts when its axis is the outermost one that changed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum Start {
    /// A copy of the seed, with new database instances.
    #[default]
    Seed,
    /// The storage and the database instances at the end of the cell before.
    Previous,
    /// A copy of the storage after the warm-up of the cell before, with new
    /// database instances.
    PreviousWarmUp,
}

/// One value of each axis: the label of the cell, and the paths it sets.
#[derive(Debug, Clone)]
pub(crate) struct CellPoint {
    /// The label of each axis, in axis order.
    pub(crate) labels: Vec<(String, String)>,
    /// The values that the axes set, by dotted path.
    pub(crate) set: IndexMap<String, Value>,
    /// Where the cell starts.
    pub(crate) start: Start,
}

/// The selected experiment with all its values resolved.
#[derive(Debug, Clone)]
pub(crate) struct Resolved {
    pub(crate) name: String,
    /// The configuration after the values of the experiment and of
    /// `--set`, as one TOML table without `experiments`.
    pub(crate) base: Table,
    pub(crate) axes: Vec<Axis>,
}

/// Options of the command line that select and change the experiment.
#[derive(Debug, Clone, Default)]
pub(crate) struct Selection {
    pub(crate) configs: Vec<std::path::PathBuf>,
    pub(crate) experiment: String,
    pub(crate) set: Vec<String>,
    pub(crate) only: Vec<String>,
}

impl Selection {
    /// Returns the names of the experiments of the merged configuration.
    pub(crate) fn experiment_names(&self) -> Result<Vec<String>, Box<dyn Error>> {
        let (_, config) = self.load()?;
        Ok(config.experiments.keys().cloned().collect())
    }

    /// Loads the layers, selects the experiment, and applies `--set` and
    /// `--only`.
    pub(crate) fn resolve(&self) -> Result<Resolved, Box<dyn Error>> {
        let (mut merged, config) = self.load()?;
        let mut experiment = config
            .experiments
            .get(&self.experiment)
            .cloned()
            .ok_or_else(|| {
                let names: Vec<_> = config.experiments.keys().cloned().collect();
                format!(
                    "unknown experiment {:?}; known: {}",
                    self.experiment,
                    names.join(", ")
                )
            })?;
        for axis in &mut experiment.axes {
            axis.expand()?;
        }
        for assignment in &self.set {
            let (path, value) = parse_assignment(assignment)?;
            experiment.axes.retain(|axis| !axis.sets(&path));
            experiment.set.insert(path, value);
        }
        for only in &self.only {
            let (name, label) = only
                .split_once('=')
                .ok_or_else(|| format!("--only {only:?}: expected AXIS=LABEL"))?;
            let axis = experiment
                .axes
                .iter_mut()
                .find(|axis| axis.label() == name)
                .ok_or_else(|| format!("--only {only:?}: no axis {name:?}"))?;
            let kept: Vec<_> = axis
                .values
                .iter()
                .filter(|value| value_label(value).as_deref() == Some(label))
                .cloned()
                .collect();
            if kept.is_empty() {
                return Err(
                    format!("--only {only:?}: axis {name:?} has no value {label:?}").into(),
                );
            }
            axis.values = kept;
        }
        merged.remove("experiments");
        for (path, value) in &experiment.set {
            set_path(&mut merged, path, value.clone())?;
        }
        check_axes(&experiment)?;
        Ok(Resolved {
            name: self.experiment.clone(),
            base: merged,
            axes: experiment.axes,
        })
    }

    /// Merges the built-in configuration and the `--config` files in order.
    fn load(&self) -> Result<(Table, Config), Box<dyn Error>> {
        let mut merged: Table = toml::from_str(BUILTIN).map_err(|e| format!("built-in: {e}"))?;
        for path in &self.configs {
            let layer = read_layer(path)?;
            merge(&mut merged, layer);
        }
        let config: Config = Value::Table(merged.clone())
            .try_into()
            .map_err(|e| format!("configuration: {e}"))?;
        Ok((merged, config))
    }
}

impl Resolved {
    /// Returns the points of the experiment: the cartesian product of the axes,
    /// the first axis outermost, each with its start rule.
    pub(crate) fn points(&self) -> Vec<CellPoint> {
        let mut points = vec![CellPoint {
            labels: Vec::new(),
            set: IndexMap::new(),
            start: Start::Seed,
        }];
        for axis in &self.axes {
            let mut next = Vec::with_capacity(points.len() * axis.values.len());
            for point in &points {
                for (index, value) in axis.values.iter().enumerate() {
                    let mut point = point.clone();
                    point.labels.push((
                        axis.label().to_string(),
                        value_label(value).unwrap_or_default(),
                    ));
                    for (path, value) in axis.assignments(value) {
                        point.set.insert(path, value);
                    }
                    // The inner axes restart at their first value when an outer
                    // axis changes, so the first value of an axis keeps the
                    // start of the outer axis.
                    if index > 0 {
                        point.start = axis.start;
                    }
                    next.push(point);
                }
            }
            points = next;
        }
        // A cell starts by the rule of the outermost axis that changed since the
        // cell before it. The loop above gives each point the rule of the
        // innermost axis whose value is not its first, which is that axis.
        if let Some(first) = points.first_mut() {
            first.start = Start::Seed;
        }
        points
    }

    /// Returns the table of one cell: the base with the values of `point`.
    pub(crate) fn cell_table(&self, point: &CellPoint) -> Result<Table, Box<dyn Error>> {
        let mut table = self.base.clone();
        for (path, value) in &point.set {
            set_path(&mut table, path, value.clone())?;
        }
        Ok(table)
    }

    /// Returns the experiment as it ran, for the result.
    pub(crate) fn describe(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "configuration": toml_to_json(&Value::Table(self.base.clone())),
            "axes": self.axes.iter().map(|axis| serde_json::to_value(axis).unwrap_or_default()).collect::<Vec<_>>(),
        })
    }
}

impl Axis {
    /// Returns the name that labels the axis in results and `--only`.
    pub(crate) fn label(&self) -> &str {
        self.name.as_deref().or(self.path.as_deref()).unwrap_or("")
    }

    /// Replaces a `generate` part with the values that it gives.
    fn expand(&mut self) -> Result<(), Box<dyn Error>> {
        let Some(generate) = self.generate.take() else {
            return Ok(());
        };
        if !self.values.is_empty() {
            return Err(format!(
                "axis {:?}: give `values` or `generate`, not both",
                self.label()
            )
            .into());
        }
        let (name, params) = match generate
            .as_table()
            .map(|table| table.iter().collect::<Vec<_>>())
        {
            Some(entries) if entries.len() == 1 => (entries[0].0.clone(), entries[0].1.clone()),
            _ => {
                return Err(format!(
                    "axis {:?}: `generate` must be a table with one name",
                    self.label()
                )
                .into());
            }
        };
        self.values = crate::scenarios::axis_values(&name, &params)
            .map_err(|e| format!("axis {:?}: {e}", self.label()))?;
        Ok(())
    }

    /// Reports whether one value of the axis sets `path`.
    fn sets(&self, path: &str) -> bool {
        self.values
            .iter()
            .any(|value| self.assignments(value).any(|(p, _)| p == path))
    }

    /// Returns the paths that `value` sets.
    fn assignments<'a>(
        &'a self,
        value: &'a Value,
    ) -> Box<dyn Iterator<Item = (String, Value)> + 'a> {
        match &self.path {
            Some(path) => Box::new(std::iter::once((path.clone(), value.clone()))),
            None => match value.get("set").and_then(Value::as_table) {
                Some(set) => Box::new(set.iter().map(|(p, v)| (p.clone(), v.clone()))),
                None => Box::new(std::iter::empty()),
            },
        }
    }

    /// Returns the paths that any value of the axis sets.
    fn paths(&self) -> BTreeSet<String> {
        self.values
            .iter()
            .flat_map(|value| self.assignments(value).map(|(p, _)| p))
            .collect()
    }
}

/// Reads the whole duration syntax of the command line from a TOML string.
pub(crate) fn duration<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    let text = String::deserialize(deserializer)?;
    glassdb_bench_scale::parse_duration(&text).map_err(serde::de::Error::custom)
}

/// Deserializes one part of a cell table into its typed form.
pub(crate) fn typed<T: DeserializeOwned>(value: &Value, what: &str) -> Result<T, Box<dyn Error>> {
    value
        .clone()
        .try_into()
        .map_err(|e| format!("{what}: {e}").into())
}

/// Converts a TOML value into JSON for the result.
pub(crate) fn toml_to_json(value: &Value) -> serde_json::Value {
    serde_json::to_value(value).unwrap_or(serde_json::Value::Null)
}

/// Returns the label of one axis value: a scalar itself, or the `label` of a
/// coupled value.
fn value_label(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Table(table) => table
            .get("label")
            .and_then(Value::as_str)
            .map(str::to_string),
        other => Some(other.to_string()),
    }
}

/// Checks the axes for paths that two axes set, and for start rules that
/// cannot hold.
fn check_axes(experiment: &Experiment) -> Result<(), Box<dyn Error>> {
    let mut seen = BTreeSet::new();
    let mut seeds_done = false;
    for axis in &experiment.axes {
        match (&axis.path, &axis.name) {
            (Some(_), None) | (None, Some(_)) => {}
            _ => {
                return Err(format!(
                    "axis {:?}: give exactly one of `path` and `name`",
                    axis.label()
                )
                .into());
            }
        }
        if axis.values.is_empty() {
            return Err(format!("axis {:?} has no values", axis.label()).into());
        }
        if axis.name.is_some()
            && axis
                .values
                .iter()
                .any(|value| value.get("label").and_then(Value::as_str).is_none())
        {
            return Err(format!("axis {:?}: each value needs a `label`", axis.label()).into());
        }
        for path in axis.paths() {
            if experiment.set.contains_key(&path) {
                return Err(format!("{path} is in `set` and in axis {:?}", axis.label()).into());
            }
            if !seen.insert(path.clone()) {
                return Err(format!("{path} is in two axes").into());
            }
        }
        match axis.start {
            Start::Seed if seeds_done => {
                return Err(format!(
                    "axis {:?} starts from the seed, but it is inside an axis that continues",
                    axis.label()
                )
                .into());
            }
            Start::Seed => {}
            Start::Previous | Start::PreviousWarmUp => seeds_done = true,
        }
    }
    Ok(())
}

fn read_layer(path: &Path) -> Result<Table, Box<dyn Error>> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(toml::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?)
}

/// Merges `layer` into `base`: tables merge key by key, other values replace
/// the earlier value. An experiment and a part selector replace the earlier
/// value as a whole, so no axis or part parameter stays from an earlier layer.
fn merge(base: &mut Table, layer: Table) {
    merge_at(base, layer, "");
}

fn merge_at(base: &mut Table, layer: Table, prefix: &str) {
    for (key, value) in layer {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        let whole = replaced_whole(&path);
        match (base.get_mut(&key), value) {
            (Some(Value::Table(existing)), Value::Table(table)) if !whole => {
                merge_at(existing, table, &path);
            }
            (_, value) => {
                base.insert(key, value);
            }
        }
    }
}

/// Reports whether a later layer replaces the value at `path` as a whole.
fn replaced_whole(path: &str) -> bool {
    let segments: Vec<_> = path.split('.').collect();
    matches!(
        segments.as_slice(),
        ["experiments", _] | ["shapes", _, "source"] | ["cell", "measure", "until"]
    )
}

/// Sets the value at a dotted `path`, creating tables on the way.
pub(crate) fn set_path(table: &mut Table, path: &str, value: Value) -> Result<(), Box<dyn Error>> {
    let mut segments = path.split('.').peekable();
    let mut current = table;
    while let Some(segment) = segments.next() {
        if segment.is_empty() {
            return Err(format!("invalid path {path:?}").into());
        }
        if segments.peek().is_none() {
            current.insert(segment.to_string(), value);
            return Ok(());
        }
        let entry = current
            .entry(segment.to_string())
            .or_insert_with(|| Value::Table(Table::new()));
        current = match entry {
            Value::Table(next) => next,
            _ => return Err(format!("{path}: {segment} is not a table").into()),
        };
    }
    Err(format!("invalid path {path:?}").into())
}

/// Parses `PATH=VALUE`, where `VALUE` is a TOML value, or a bare string.
fn parse_assignment(text: &str) -> Result<(String, Value), Box<dyn Error>> {
    let (path, value) = text
        .split_once('=')
        .ok_or_else(|| format!("--set {text:?}: expected PATH=VALUE"))?;
    let path = path.trim().to_string();
    let parsed: Result<Table, _> = toml::from_str(&format!("v = {}", value.trim()));
    let value = match parsed {
        Ok(mut table) => table
            .remove("v")
            .unwrap_or(Value::String(value.to_string())),
        Err(_) => Value::String(value.trim().to_string()),
    };
    Ok((path, value))
}

/// Flattens the `set` table of an experiment into dotted paths, so that TOML
/// dotted keys and quoted paths mean the same.
fn flat_paths<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<IndexMap<String, Value>, D::Error> {
    let table = Table::deserialize(deserializer)?;
    let mut flat = IndexMap::new();
    flatten("", table, &mut flat);
    Ok(flat)
}

fn flatten(prefix: &str, table: Table, out: &mut IndexMap<String, Value>) {
    for (key, value) in table {
        let path = if prefix.is_empty() {
            key
        } else {
            format!("{prefix}.{key}")
        };
        match value {
            // A part selector with parameters is one value, not a path prefix.
            Value::Table(inner) if !replaced_whole(&path) => flatten(&path, inner, out),
            value => {
                out.insert(path, value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolve(
        layers: &[&str],
        experiment: &str,
        set: &[&str],
    ) -> Result<Resolved, Box<dyn Error>> {
        let dir = std::env::temp_dir().join(format!(
            "perfbench-config-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&dir)?;
        let configs = layers
            .iter()
            .enumerate()
            .map(|(index, text)| {
                let path = dir.join(format!("{index}.toml"));
                std::fs::write(&path, text).map(|()| path)
            })
            .collect::<Result<_, _>>()?;
        Selection {
            configs,
            experiment: experiment.to_string(),
            set: set.iter().map(|s| s.to_string()).collect(),
            only: Vec::new(),
        }
        .resolve()
    }

    fn get<'a>(table: &'a Table, path: &str) -> Option<&'a Value> {
        let mut segments = path.split('.');
        let mut value = table.get(segments.next()?)?;
        for segment in segments {
            value = value.get(segment)?;
        }
        Some(value)
    }

    #[test]
    fn a_later_layer_changes_one_field_and_keeps_the_others() -> Result<(), Box<dyn Error>> {
        let resolved = resolve(&["[shapes.rw-many]\nkeys = 4\n"], "mixed", &[])?;
        assert_eq!(
            get(&resolved.base, "shapes.rw-many.keys"),
            Some(&Value::Integer(4))
        );
        assert_eq!(
            get(&resolved.base, "shapes.rw-many.kind"),
            Some(&Value::String("read-modify-write".into()))
        );
        Ok(())
    }

    #[test]
    fn an_experiment_and_a_source_replace_the_earlier_value_as_a_whole()
    -> Result<(), Box<dyn Error>> {
        let first = "[experiments.probe]\n[[experiments.probe.axes]]\npath = \"cell.affinity\"\nvalues = [0, 100]\n";
        let second =
            "[experiments.probe.set]\ncell.workers = 2\n\n[shapes.rw-many]\nsource = \"pool\"\n";
        let resolved = resolve(
            &[
                "[shapes.rw-many]\nsource = { stripe = { shared-pct = 5 } }\n",
                first,
                second,
            ],
            "probe",
            &[],
        )?;
        assert!(resolved.axes.is_empty());
        assert_eq!(
            get(&resolved.base, "cell.workers"),
            Some(&Value::Integer(2))
        );
        assert_eq!(
            get(&resolved.base, "shapes.rw-many.source"),
            Some(&Value::String("pool".into()))
        );
        Ok(())
    }

    #[test]
    fn set_removes_the_axis_of_its_path_and_the_last_one_wins() -> Result<(), Box<dyn Error>> {
        let resolved = resolve(&[], "mixed", &["cell.affinity=50", "cell.affinity=25"])?;
        assert!(
            resolved
                .axes
                .iter()
                .all(|axis| axis.label() != "cell.affinity")
        );
        assert_eq!(
            get(&resolved.base, "cell.affinity"),
            Some(&Value::Integer(25))
        );
        Ok(())
    }

    #[test]
    fn the_outermost_axis_that_changed_decides_the_start() -> Result<(), Box<dyn Error>> {
        let layer = "[experiments.chain]\n\
            [[experiments.chain.axes]]\npath = \"cell.affinity\"\nvalues = [0, 100]\n\
            [[experiments.chain.axes]]\npath = \"cell.workers\"\nvalues = [1, 2, 3]\nstart = \"previous\"\n";
        let points = resolve(&[layer], "chain", &[])?.points();
        let starts: Vec<_> = points.iter().map(|point| point.start).collect();
        use Start::*;
        assert_eq!(starts, [Seed, Previous, Previous, Seed, Previous, Previous]);
        Ok(())
    }

    #[test]
    fn invalid_axes_are_rejected() {
        let cases = [
            (
                "[experiments.x]\n[[experiments.x.axes]]\npath = \"cell.workers\"\nvalues = [1, 2]\nstart = \"previous\"\n\
                 [[experiments.x.axes]]\npath = \"cell.affinity\"\nvalues = [0, 100]\n",
                "inside an axis that continues",
            ),
            (
                "[experiments.x.set]\ncell.workers = 3\n[[experiments.x.axes]]\npath = \"cell.workers\"\nvalues = [1, 2]\n",
                "is in `set` and in axis",
            ),
            (
                "[experiments.x]\n[[experiments.x.axes]]\nname = \"pair\"\nvalues = [{ set = { \"cell.workers\" = 1 } }]\n",
                "needs a `label`",
            ),
        ];
        for (layer, message) in cases {
            let error = resolve(&[layer], "x", &[]).expect_err(message).to_string();
            assert!(error.contains(message), "{error}");
        }
    }

    #[test]
    fn every_builtin_experiment_plans_its_cells_and_chains() -> Result<(), Box<dyn Error>> {
        let names = Selection::default().experiment_names()?;
        assert!(names.len() >= 9, "{names:?}");
        for name in names {
            let resolved = Selection {
                experiment: name.clone(),
                ..Selection::default()
            }
            .resolve()?;
            crate::runner::plan(&resolved).map_err(|e| format!("{name}: {e}"))?;
        }
        Ok(())
    }

    #[test]
    fn only_keeps_one_value_of_a_coupled_axis() -> Result<(), Box<dyn Error>> {
        let mut selection = Selection {
            experiment: "suite".into(),
            only: vec!["case=warm_read".into()],
            ..Selection::default()
        };
        let resolved = selection.resolve()?;
        assert_eq!(resolved.points().len(), 1);
        selection.only = vec!["case=missing".into()];
        assert!(selection.resolve().is_err());
        Ok(())
    }
}
