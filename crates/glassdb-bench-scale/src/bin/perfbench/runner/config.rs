//! The layered configuration of the runner: loading, merging, `--set`, and
//! the expansion of an experiment into its cells.

use std::collections::BTreeSet;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::time::Duration;

use indexmap::IndexMap;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use toml::Value;
use toml::value::Table;

/// The defaults that are the first layer of every experiment.
const BUILTIN: &str = include_str!("builtin.toml");

/// The top level of the configuration of one experiment, without its axes.
/// Each cell checks its own sections; this checks the top-level keys.
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

/// One selected experiment with all its values resolved.
#[derive(Debug, Clone)]
pub(crate) struct Resolved {
    /// The stem of the experiment file.
    pub(crate) name: String,
    /// The configuration after all layers and `--set`, as one TOML table
    /// without `axes`.
    pub(crate) base: Table,
    pub(crate) axes: Vec<Axis>,
}

/// Options of the command line that select and change the experiments.
#[derive(Debug, Clone, Default)]
pub(crate) struct Selection {
    /// Files, or directories of files, that apply to every experiment.
    pub(crate) configs: Vec<PathBuf>,
    /// Experiment files, or directories of experiment files.
    pub(crate) experiments: Vec<PathBuf>,
    pub(crate) set: Vec<String>,
    pub(crate) only: Vec<String>,
}

impl Selection {
    /// Resolves each selected experiment, in the order of the command line,
    /// and applies `--set` and `--only` to each one.
    pub(crate) fn resolve(&self) -> Result<Vec<Resolved>, Box<dyn Error>> {
        let files = experiment_files(&self.experiments)?;
        let mut common: Table = toml::from_str(BUILTIN).map_err(|e| format!("built-in: {e}"))?;
        for path in toml_files(&self.configs)? {
            merge(&mut common, read_layer(&path, &mut Vec::new())?);
        }
        let mut resolved = files
            .into_iter()
            .map(|(name, path)| self.resolve_one(name, &path, &common))
            .collect::<Result<Vec<_>, _>>()?;
        for only in &self.only {
            keep_only(&mut resolved, only)?;
        }
        for experiment in &resolved {
            check_axes(&experiment.axes).map_err(|e| format!("{}: {e}", experiment.name))?;
        }
        Ok(resolved)
    }

    /// Merges the experiment file at `path` over `common`, and applies `--set`.
    fn resolve_one(
        &self,
        name: String,
        path: &Path,
        common: &Table,
    ) -> Result<Resolved, Box<dyn Error>> {
        let mut base = common.clone();
        merge(&mut base, read_layer(path, &mut Vec::new())?);
        let mut axes: Vec<Axis> = match base.remove("axes") {
            Some(axes) => axes
                .try_into()
                .map_err(|e| format!("{}: axes: {e}", path.display()))?,
            None => Vec::new(),
        };
        for axis in &mut axes {
            axis.expand()
                .map_err(|e| format!("{}: {e}", path.display()))?;
        }
        for assignment in &self.set {
            let (parameter, value) = parse_assignment(assignment)?;
            axes.retain(|axis| !axis.sets(&parameter));
            set_path(&mut base, &parameter, value)?;
        }
        let _: Config = Value::Table(base.clone())
            .try_into()
            .map_err(|e| format!("{}: {e}", path.display()))?;
        Ok(Resolved { name, base, axes })
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
fn check_axes(axes: &[Axis]) -> Result<(), Box<dyn Error>> {
    let mut seen = BTreeSet::new();
    let mut seeds_done = false;
    for axis in axes {
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

/// Keeps one value of the axis that `only` names, as `AXIS=LABEL`, in each
/// experiment that has that axis.
fn keep_only(experiments: &mut [Resolved], only: &str) -> Result<(), Box<dyn Error>> {
    let (name, label) = only
        .split_once('=')
        .ok_or_else(|| format!("--only {only:?}: expected AXIS=LABEL"))?;
    let mut found = false;
    for experiment in experiments {
        let Some(axis) = experiment.axes.iter_mut().find(|axis| axis.label() == name) else {
            continue;
        };
        found = true;
        let kept: Vec<_> = axis
            .values
            .iter()
            .filter(|value| value_label(value).as_deref() == Some(label))
            .cloned()
            .collect();
        if kept.is_empty() {
            return Err(format!(
                "--only {only:?}: axis {name:?} of {} has no value {label:?}",
                experiment.name
            )
            .into());
        }
        axis.values = kept;
    }
    if !found {
        return Err(format!("--only {only:?}: no experiment has axis {name:?}").into());
    }
    Ok(())
}

/// Returns the name and the path of each experiment file, and rejects two
/// experiments with one name, because the result identifies them by name.
fn experiment_files(paths: &[PathBuf]) -> Result<Vec<(String, PathBuf)>, Box<dyn Error>> {
    if paths.is_empty() {
        return Err("give at least one --experiment".into());
    }
    let mut files: IndexMap<String, PathBuf> = IndexMap::new();
    for path in toml_files(paths)? {
        let name = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or_else(|| format!("{}: no experiment name", path.display()))?
            .to_string();
        if let Some(other) = files.get(&name) {
            return Err(format!(
                "experiment {name:?} is in {} and in {}",
                other.display(),
                path.display()
            )
            .into());
        }
        files.insert(name, path);
    }
    Ok(files.into_iter().collect())
}

/// Replaces each directory in `paths` with its `*.toml` files, in sorted order.
fn toml_files(paths: &[PathBuf]) -> Result<Vec<PathBuf>, Box<dyn Error>> {
    let mut files = Vec::new();
    for path in paths {
        if !path.is_dir() {
            files.push(path.clone());
            continue;
        }
        let mut entries = std::fs::read_dir(path)
            .and_then(|entries| {
                entries
                    .map(|entry| entry.map(|entry| entry.path()))
                    .collect::<Result<Vec<_>, _>>()
            })
            .map_err(|e| format!("{}: {e}", path.display()))?;
        entries.retain(|entry| entry.is_file() && entry.extension().is_some_and(|e| e == "toml"));
        if entries.is_empty() {
            return Err(format!("{}: no *.toml file", path.display()).into());
        }
        entries.sort();
        files.extend(entries);
    }
    Ok(files)
}

/// Reads one file, with the files of its `include` merged before its own
/// values. `including` holds the files whose includes are being read.
fn read_layer(path: &Path, including: &mut Vec<PathBuf>) -> Result<Table, Box<dyn Error>> {
    let canonical = path
        .canonicalize()
        .map_err(|e| format!("{}: {e}", path.display()))?;
    if including.contains(&canonical) {
        return Err(format!("{}: includes itself", path.display()).into());
    }
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut own: Table = toml::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
    let includes: Vec<PathBuf> = match own.remove("include") {
        Some(value) => value
            .try_into()
            .map_err(|e| format!("{}: include: {e}", path.display()))?,
        None => Vec::new(),
    };
    let directory = path.parent().unwrap_or(Path::new(""));
    including.push(canonical);
    let mut layer = Table::new();
    for include in includes {
        merge(&mut layer, read_layer(&directory.join(include), including)?);
    }
    including.pop();
    merge(&mut layer, own);
    Ok(layer)
}

/// Merges `layer` into `base`: tables merge key by key, other values replace
/// the earlier value. A part selector replaces the earlier value as a whole,
/// so no part parameter stays from an earlier layer.
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
        ["shapes" | "workloads", _, "source"] | ["cell", "measure", "until"]
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

/// The experiments of the repository.
#[cfg(test)]
pub(crate) const REPOSITORY_EXPERIMENTS: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../../hack/perf/experiments");

/// Returns the file of the repository experiment `name`.
#[cfg(test)]
pub(crate) fn repository_experiment(name: &str) -> PathBuf {
    let file = format!("{name}.toml");
    std::fs::read_dir(REPOSITORY_EXPERIMENTS)
        .unwrap()
        .map(|entry| entry.unwrap().path().join(&file))
        .find(|path| path.is_file())
        .unwrap_or_else(|| panic!("no repository experiment {name:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A temporary directory, removed when it is dropped.
    struct Dir(PathBuf);

    impl std::ops::Deref for Dir {
        type Target = Path;

        fn deref(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Writes `files`, as `(PATH, TEXT)`, in a new directory, and returns it.
    fn write(files: &[(&str, &str)]) -> Dir {
        let dir = Dir(std::env::temp_dir().join(format!(
            "perfbench-config-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        )));
        for (path, text) in files {
            let path = dir.join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, text).unwrap();
        }
        dir
    }

    fn resolve(
        dir: &Path,
        configs: &[&str],
        experiments: &[&str],
        set: &[&str],
    ) -> Result<Vec<Resolved>, Box<dyn Error>> {
        Selection {
            configs: configs.iter().map(|path| dir.join(path)).collect(),
            experiments: experiments.iter().map(|path| dir.join(path)).collect(),
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

    const SHAPE: &str = "[shapes.rw]\nkind = \"read-modify-write\"\npool = \"p\"\nkeys = 10\n";

    #[test]
    fn a_later_layer_changes_one_field_and_keeps_the_others() -> Result<(), Box<dyn Error>> {
        let dir = write(&[
            ("shape.toml", SHAPE),
            (
                "x.toml",
                "include = [\"shape.toml\"]\n[shapes.rw]\nkeys = 4\n",
            ),
        ]);
        let resolved = resolve(&dir, &[], &["x.toml"], &[])?;
        assert_eq!(
            get(&resolved[0].base, "shapes.rw.keys"),
            Some(&Value::Integer(4))
        );
        assert_eq!(
            get(&resolved[0].base, "shapes.rw.kind"),
            Some(&Value::String("read-modify-write".into()))
        );
        Ok(())
    }

    #[test]
    fn a_source_replaces_the_earlier_value_as_a_whole() -> Result<(), Box<dyn Error>> {
        let dir = write(&[
            (
                "all.toml",
                "[shapes.rw]\nsource = { stripe = { shared-pct = 5 } }\n",
            ),
            ("x.toml", "[shapes.rw]\nsource = \"pool\"\n"),
        ]);
        let resolved = resolve(&dir, &["all.toml"], &["x.toml"], &[])?;
        assert_eq!(
            get(&resolved[0].base, "shapes.rw.source"),
            Some(&Value::String("pool".into()))
        );
        Ok(())
    }

    #[test]
    fn layers_apply_in_order_and_experiments_do_not_see_each_other() -> Result<(), Box<dyn Error>> {
        let dir = write(&[
            ("all.toml", "[cell]\nworkers = 2\ndatabases = 2\n"),
            ("base.toml", "[cell]\nworkers = 3\naffinity = 0\n"),
            (
                "experiments/a.toml",
                "include = [\"../base.toml\"]\n[pools.wide]\nkeys = 10\n[cell]\nworkers = 5\n",
            ),
            ("experiments/b.toml", "[cell]\ngc = false\n"),
        ]);
        let resolved = resolve(&dir, &["all.toml"], &["experiments"], &["cell.affinity=50"])?;
        let names: Vec<_> = resolved.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, ["a", "b"]);
        let (a, b) = (&resolved[0].base, &resolved[1].base);
        // The built-in defaults, the configs, the includes, the experiment
        // file, and `--set`, in this order.
        assert_eq!(get(a, "cell.databases"), Some(&Value::Integer(2)));
        assert_eq!(get(a, "cell.workers"), Some(&Value::Integer(5)));
        assert_eq!(get(a, "cell.affinity"), Some(&Value::Integer(50)));
        assert_eq!(get(a, "cell.gc"), Some(&Value::Boolean(true)));
        assert_eq!(get(b, "cell.workers"), Some(&Value::Integer(2)));
        assert_eq!(get(b, "cell.affinity"), Some(&Value::Integer(50)));
        assert_eq!(get(b, "cell.gc"), Some(&Value::Boolean(false)));
        assert_eq!(get(b, "pools.wide"), None);
        Ok(())
    }

    /// The files of a test case, as `(PATH, TEXT)`.
    type Files<'a> = &'a [(&'a str, &'a str)];

    #[test]
    fn a_config_directory_loads_its_files_in_name_order_with_their_includes()
    -> Result<(), Box<dyn Error>> {
        let dir = write(&[
            (
                "all/b.toml",
                "include = [\"../shared/one.toml\"]\n[cell]\nworkers = 3\n",
            ),
            ("all/a.toml", "[cell]\nworkers = 2\ndatabases = 2\n"),
            // The path of an include is relative to the file that has it.
            (
                "shared/one.toml",
                "include = [\"two.toml\"]\n[cell]\ndatabases = 5\n",
            ),
            ("shared/two.toml", "[cell]\naffinity = 0\n"),
            ("x.toml", ""),
        ]);
        let base = &resolve(&dir, &["all"], &["x.toml"], &[])?[0].base;
        assert_eq!(get(base, "cell.workers"), Some(&Value::Integer(3)));
        assert_eq!(get(base, "cell.databases"), Some(&Value::Integer(5)));
        assert_eq!(get(base, "cell.affinity"), Some(&Value::Integer(0)));
        Ok(())
    }

    #[test]
    fn a_workload_source_replaces_the_earlier_value_as_a_whole() -> Result<(), Box<dyn Error>> {
        let dir = write(&[
            (
                "all.toml",
                "[workloads.w]\nsource = { stripe = { shared-pct = 5 } }\n",
            ),
            (
                "x.toml",
                "[workloads.w]\nsource = { index = { keys = [0] } }\n",
            ),
        ]);
        let base = &resolve(&dir, &["all.toml"], &["x.toml"], &[])?[0].base;
        let source = get(base, "workloads.w.source").and_then(Value::as_table);
        assert_eq!(
            source.map(|s| s.keys().map(String::as_str).collect::<Vec<_>>()),
            Some(vec!["index"])
        );
        Ok(())
    }

    #[test]
    fn invalid_files_are_rejected() {
        let cases: [(Files, &[&str], &str); 4] = [
            (
                &[("a/x.toml", ""), ("b/x.toml", "")],
                &["a", "b"],
                "experiment \"x\" is in",
            ),
            (
                &[
                    ("x.toml", "include = [\"y.toml\"]\n"),
                    ("y.toml", "include = [\"x.toml\"]\n"),
                ],
                &["x.toml"],
                "includes itself",
            ),
            (&[("x.toml", "[unknown]\n")], &["x.toml"], "unknown field"),
            (&[("empty/x.txt", "")], &["empty"], "no *.toml file"),
        ];
        for (files, experiments, message) in cases {
            let dir = write(files);
            let error = resolve(&dir, &[], experiments, &[])
                .expect_err(message)
                .to_string();
            assert!(error.contains(message), "{error}");
        }
    }

    #[test]
    fn set_removes_the_axis_of_its_path_and_the_last_one_wins() -> Result<(), Box<dyn Error>> {
        let dir = write(&[(
            "x.toml",
            "[[axes]]\npath = \"cell.affinity\"\nvalues = [0, 100]\n",
        )]);
        let resolved = resolve(
            &dir,
            &[],
            &["x.toml"],
            &["cell.affinity=50", "cell.affinity=25"],
        )?;
        assert!(resolved[0].axes.is_empty());
        assert_eq!(
            get(&resolved[0].base, "cell.affinity"),
            Some(&Value::Integer(25))
        );
        Ok(())
    }

    #[test]
    fn a_value_of_the_file_applies_to_the_axis_values_that_do_not_set_it()
    -> Result<(), Box<dyn Error>> {
        let dir = write(&[(
            "x.toml",
            "[cell]\nworkers = 3\n[[axes]]\nname = \"pair\"\nvalues = [\
             { label = \"a\", set = { \"cell.workers\" = 1 } }, { label = \"b\", set = {} }]\n",
        )]);
        let resolved = &resolve(&dir, &[], &["x.toml"], &[])?[0];
        let workers: Vec<_> = resolved
            .points()
            .iter()
            .map(|point| {
                let table = resolved.cell_table(point).unwrap();
                get(&table, "cell.workers").cloned()
            })
            .collect();
        assert_eq!(workers, [Some(Value::Integer(1)), Some(Value::Integer(3))]);
        Ok(())
    }

    #[test]
    fn the_outermost_axis_that_changed_decides_the_start() -> Result<(), Box<dyn Error>> {
        let dir = write(&[(
            "x.toml",
            "[[axes]]\npath = \"cell.affinity\"\nvalues = [0, 100]\n\
             [[axes]]\npath = \"cell.workers\"\nvalues = [1, 2, 3]\nstart = \"previous\"\n",
        )]);
        let points = resolve(&dir, &[], &["x.toml"], &[])?[0].points();
        let starts: Vec<_> = points.iter().map(|point| point.start).collect();
        use Start::*;
        assert_eq!(starts, [Seed, Previous, Previous, Seed, Previous, Previous]);
        Ok(())
    }

    #[test]
    fn invalid_axes_are_rejected() {
        let cases = [
            (
                "[[axes]]\npath = \"cell.workers\"\nvalues = [1, 2]\nstart = \"previous\"\n\
                 [[axes]]\npath = \"cell.affinity\"\nvalues = [0, 100]\n",
                "inside an axis that continues",
            ),
            (
                "[[axes]]\npath = \"cell.workers\"\nvalues = [1]\n\
                 [[axes]]\npath = \"cell.workers\"\nvalues = [2]\n",
                "is in two axes",
            ),
            (
                "[[axes]]\nname = \"pair\"\nvalues = [{ set = { \"cell.workers\" = 1 } }]\n",
                "needs a `label`",
            ),
        ];
        for (text, message) in cases {
            let dir = write(&[("x.toml", text)]);
            let error = resolve(&dir, &[], &["x.toml"], &[])
                .expect_err(message)
                .to_string();
            assert!(error.contains(message), "{error}");
        }
    }

    #[test]
    fn every_repository_experiment_plans_its_cells_and_chains() -> Result<(), Box<dyn Error>> {
        let experiments: Vec<_> = std::fs::read_dir(REPOSITORY_EXPERIMENTS)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<_, _>>()?;
        let resolved = Selection {
            experiments,
            ..Selection::default()
        }
        .resolve()?;
        assert!(resolved.len() >= 20, "{}", resolved.len());
        for experiment in &resolved {
            crate::runner::plan(experiment).map_err(|e| format!("{}: {e}", experiment.name))?;
        }
        Ok(())
    }

    #[test]
    fn only_keeps_one_value_of_an_axis_in_each_experiment_that_has_it() -> Result<(), Box<dyn Error>>
    {
        let mut selection = Selection {
            experiments: vec![
                repository_experiment("inline-pressure"),
                repository_experiment("warm_read"),
            ],
            only: vec!["phase=pressure".into()],
            ..Selection::default()
        };
        let resolved = selection.resolve()?;
        assert_eq!(resolved[0].points().len(), 1);
        assert_eq!(resolved[1].points().len(), 1);
        for (only, message) in [
            ("phase=missing", "has no value"),
            ("case=warm_read", "no experiment has axis"),
        ] {
            selection.only = vec![only.into()];
            let error = selection.resolve().expect_err(only).to_string();
            assert!(error.contains(message), "{error}");
        }
        Ok(())
    }
}
