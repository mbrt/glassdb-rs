//! The typed form of one cell: its pools, groups, shapes, and parameters.

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use glassdb::{DatabaseBuilder, InlinePolicy, NodeSizePolicy, TopologyPolicy};
use indexmap::IndexMap;
use serde::Deserialize;
use toml::Value;
use toml::value::Table;

use super::config::{duration, typed};
use super::kinds::{self, Kind};
use super::sources::{self, SourceSpec};
use super::until::Until;

/// One cell, resolved from its configuration table.
#[derive(Clone)]
pub(crate) struct CellSpec {
    /// The pools that the shapes of the cell use, with their resolved sizes.
    pub(crate) pools: IndexMap<String, PoolSpec>,
    pub(crate) groups: Vec<GroupSpec>,
    pub(crate) seed: SeedSpec,
    pub(crate) params: CellParams,
    /// The collections of each pool that the cell uses.
    pub(crate) collections: usize,
    /// The collections of each pool that the seed writes. A chain of cells
    /// that continue each other shares one seed, so the seed writes the
    /// collections of the largest cell of the chain, and each cell uses the
    /// first of them.
    pub(crate) seed_collections: usize,
    /// Whether a group takes its instances from `cell.databases`.
    pub(crate) uses_cell_databases: bool,
    /// Whether a shape takes its workers from `cell.workers`.
    pub(crate) uses_cell_workers: bool,
}

/// A set of keys that the seed writes in each collection of a cell.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub(crate) struct PoolSpec {
    /// The keys of the pool. When absent, the sources of its shapes give it.
    #[serde(default)]
    pub(crate) keys: Option<usize>,
    /// Whether the seed writes the keys. Without, the seed creates the empty
    /// collections only.
    #[serde(default = "yes")]
    pub(crate) seeded: bool,
    /// The size of each value that the seed writes.
    #[serde(default = "default_value_bytes")]
    pub(crate) value_bytes: usize,
    /// The minimum size of each key name. Long keys make the leaves larger,
    /// so that a pool splits with fewer keys.
    #[serde(default)]
    pub(crate) key_bytes: usize,
}

/// Database instances that run the same shapes.
#[derive(Clone)]
pub(crate) struct GroupSpec {
    pub(crate) name: String,
    /// The instances that the group opens. Zero when each transaction of its
    /// shapes opens its own instance.
    pub(crate) instances: usize,
    pub(crate) shapes: Vec<ShapeSpec>,
}

/// One transaction shape of one group.
#[derive(Clone)]
pub(crate) struct ShapeSpec {
    pub(crate) name: String,
    pub(crate) kind: Arc<dyn Kind>,
    pub(crate) pool: String,
    pub(crate) source: SourceSpec,
    pub(crate) workers: usize,
}

/// The parameters of the seed.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub(crate) struct SeedSpec {
    /// The model time without splits, merges, and GC progress that ends the
    /// seed.
    /// Zero ends the seed after its writes, with no wait and no full GC.
    #[serde(deserialize_with = "duration")]
    pub(crate) quiet_period: Duration,
    #[serde(deserialize_with = "duration")]
    pub(crate) timeout: Duration,
    #[serde(default)]
    pub(crate) database: DatabaseOptions,
}

/// The options of the database instances of a seed or a cell.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub(crate) struct DatabaseOptions {
    #[serde(default)]
    pub(crate) topology: Topology,
    pub(crate) leaf_max_entries: Option<usize>,
    pub(crate) leaf_min_entries: Option<usize>,
    pub(crate) index_min_children: Option<usize>,
    pub(crate) inline_max_value_bytes: Option<usize>,
    pub(crate) inline_max_leaf_bytes: Option<usize>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum Topology {
    #[default]
    Avoidable,
    Size,
}

/// The parameters of a cell that are not part of its workload.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub(crate) struct CellParams {
    pub(crate) workload: String,
    /// The collections of each pool, or `"databases"` for one for each
    /// instance of the cell.
    pub(crate) collections: Collections,
    pub(crate) databases: usize,
    pub(crate) workers: usize,
    pub(crate) affinity: u8,
    pub(crate) gc: bool,
    #[serde(default)]
    pub(crate) database: DatabaseOptions,
    pub(crate) warm_up: WarmUpParams,
    pub(crate) measure: MeasureParams,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub(crate) enum Collections {
    Count(usize),
    Named(CollectionsName),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum CollectionsName {
    Databases,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub(crate) struct WarmUpParams {
    /// Minimum model time of the warm-up.
    #[serde(deserialize_with = "duration")]
    pub(crate) minimum: Duration,
    /// The model time window of the quiet check, or none to run until the
    /// timeout.
    #[serde(deserialize_with = "quiet")]
    pub(crate) quiet: Option<Duration>,
    /// The splits and merges of all database instances in each second of
    /// model time, at or below which a full `quiet` window ends the warm-up.
    pub(crate) quiet_rate: f64,
    /// Maximum model time. It wins over `minimum`.
    #[serde(deserialize_with = "duration")]
    pub(crate) timeout: Duration,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub(crate) struct MeasureParams {
    pub(crate) until: Value,
    #[serde(deserialize_with = "duration")]
    pub(crate) duration: Duration,
    #[serde(deserialize_with = "duration")]
    pub(crate) max_duration: Duration,
    pub(crate) window_commits: u64,
}

/// The workload table: its groups, and the pool and source that replace those
/// of its shapes.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkloadTable {
    #[serde(default)]
    pool: Option<String>,
    #[serde(default)]
    source: Option<Value>,
    groups: IndexMap<String, GroupTable>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GroupTable {
    #[serde(default)]
    instances: Option<usize>,
    shapes: IndexMap<String, Workers>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Workers {
    Count(usize),
    Cell(CellWorkers),
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
enum CellWorkers {
    Cell,
}

impl CellSpec {
    /// Resolves the cell of `table`, a configuration with the values of one
    /// cell.
    pub(crate) fn from_table(table: &Table) -> Result<Self, Box<dyn Error>> {
        let section = |name: &str| {
            table
                .get(name)
                .cloned()
                .unwrap_or_else(|| Value::Table(Table::new()))
        };
        let params: CellParams = typed(&section("cell"), "cell")?;
        let seed: SeedSpec = typed(&section("seed"), "seed")?;
        let workloads = section("workloads");
        let workload_value = workloads
            .get(&params.workload)
            .ok_or_else(|| format!("unknown workload {:?}", params.workload))?;
        let workload: WorkloadTable =
            typed(workload_value, &format!("workloads.{}", params.workload))?;
        let shapes = section("shapes");
        let mut groups = Vec::new();
        let mut uses_cell_databases = false;
        let mut uses_cell_workers = false;
        for (group_name, group) in &workload.groups {
            uses_cell_databases |= group.instances.is_none();
            let mut group_shapes = Vec::new();
            for (shape_name, workers) in &group.shapes {
                let workers = match workers {
                    Workers::Count(count) => *count,
                    Workers::Cell(CellWorkers::Cell) => {
                        uses_cell_workers = true;
                        params.workers
                    }
                };
                let shape = shapes
                    .get(shape_name)
                    .and_then(Value::as_table)
                    .ok_or_else(|| format!("unknown shape {shape_name:?}"))?;
                group_shapes.push(ShapeSpec::from_table(
                    shape_name,
                    shape,
                    workload.pool.as_deref(),
                    workload.source.as_ref(),
                    workers,
                )?);
            }
            let largest = group_shapes
                .iter()
                .map(|shape| shape.workers)
                .max()
                .unwrap_or(0);
            // A shape that opens its own instance for each transaction needs
            // no instance of the group.
            let instances = if group_shapes.iter().all(|shape| shape.kind.fresh_instance()) {
                0
            } else {
                group
                    .instances
                    .unwrap_or(params.databases)
                    .min(largest)
                    .max(1)
            };
            groups.push(GroupSpec {
                name: group_name.clone(),
                instances,
                shapes: group_shapes,
            });
        }
        let instances: usize = groups.iter().map(|group| group.instances.max(1)).sum();
        let collections = match params.collections {
            Collections::Count(count) => count,
            Collections::Named(CollectionsName::Databases) => instances,
        }
        .max(1);
        let pool_tables = section("pools");
        let mut pools = IndexMap::new();
        for shape in groups.iter().flat_map(|group| &group.shapes) {
            if pools.contains_key(&shape.pool) {
                continue;
            }
            let value = pool_tables
                .get(&shape.pool)
                .ok_or_else(|| format!("shape {}: unknown pool {:?}", shape.name, shape.pool))?;
            let pool: PoolSpec = typed(value, &format!("pools.{}", shape.pool))?;
            pools.insert(shape.pool.clone(), pool);
        }
        let mut spec = Self {
            pools,
            groups,
            seed,
            params,
            collections,
            seed_collections: collections,
            uses_cell_databases,
            uses_cell_workers,
        };
        spec.resolve_pool_sizes()?;
        spec.check()?;
        Ok(spec)
    }

    /// Returns the end condition of the measurement.
    pub(crate) fn until(&self) -> Result<Until, Box<dyn Error>> {
        Until::from_value(&self.params.measure.until)
    }

    /// Returns the instances that the cell opens.
    pub(crate) fn instances(&self) -> usize {
        self.groups.iter().map(|group| group.instances).sum()
    }

    /// Returns the workers that select keys of `pool` with a source of
    /// `family`, in the order of the groups, shapes, and workers.
    pub(crate) fn source_workers(&self, pool: &str, family: sources::Family) -> usize {
        self.groups
            .iter()
            .flat_map(|group| &group.shapes)
            .filter(|shape| shape.pool == pool && shape.source.family() == family)
            .map(|shape| shape.workers)
            .sum()
    }

    fn resolve_pool_sizes(&mut self) -> Result<(), Box<dyn Error>> {
        let names: Vec<String> = self.pools.keys().cloned().collect();
        for name in names {
            if self.pools[&name].keys.is_some() {
                continue;
            }
            let mut size = None;
            for shape in self.groups.iter().flat_map(|group| &group.shapes) {
                if shape.pool != name {
                    continue;
                }
                let workers = self.source_workers(&name, shape.source.family());
                if let Some(needed) = shape.source.pool_size(shape.kind.keys(), workers) {
                    size = Some(size.unwrap_or(0usize).max(needed));
                }
            }
            let size = size.ok_or_else(|| {
                format!("pool {name:?} has no `keys`, and no source of its shapes gives a size")
            })?;
            self.pools[&name].keys = Some(size);
        }
        Ok(())
    }

    fn check(&self) -> Result<(), Box<dyn Error>> {
        let quiet_rate = self.params.warm_up.quiet_rate;
        if !(quiet_rate.is_finite() && quiet_rate >= 0.0) {
            return Err(
                format!("cell.warm-up.quiet-rate must be 0 or more, got {quiet_rate}").into(),
            );
        }
        if self.params.affinity > 100 {
            return Err("cell.affinity must be a percentage from 0 through 100".into());
        }
        if self
            .groups
            .iter()
            .all(|group| group.shapes.iter().all(|shape| shape.workers == 0))
        {
            return Err(format!("workload {:?} has no workers", self.params.workload).into());
        }
        for shape in self.groups.iter().flat_map(|group| &group.shapes) {
            let pool = self.pool_size(&shape.pool);
            let workers = self.source_workers(&shape.pool, shape.source.family());
            shape
                .source
                .check(pool, shape.kind.keys(), workers)
                .map_err(|error| format!("shape {}: {error}", shape.name))?;
        }
        Until::from_value(&self.params.measure.until)?;
        Ok(())
    }

    /// Returns the resolved size of `pool`.
    pub(crate) fn pool_size(&self, pool: &str) -> usize {
        self.pools.get(pool).and_then(|pool| pool.keys).unwrap_or(0)
    }
}

impl ShapeSpec {
    fn from_table(
        name: &str,
        table: &Table,
        workload_pool: Option<&str>,
        workload_source: Option<&Value>,
        workers: usize,
    ) -> Result<Self, Box<dyn Error>> {
        let mut params = table.clone();
        let kind_name = params
            .remove("kind")
            .and_then(|value| value.as_str().map(str::to_string))
            .ok_or_else(|| format!("shape {name}: `kind` must be a string"))?;
        let pool = params
            .remove("pool")
            .and_then(|value| value.as_str().map(str::to_string));
        let source = params.remove("source");
        let pool = workload_pool
            .map(str::to_string)
            .or(pool)
            .ok_or_else(|| format!("shape {name}: `pool` must be a string"))?;
        let source = workload_source.cloned().or(source);
        let kind = kinds::build(&kind_name, params).map_err(|e| format!("shape {name}: {e}"))?;
        let source = sources::SourceSpec::from_value(source.as_ref())
            .map_err(|e| format!("shape {name}: {e}"))?;
        Ok(Self {
            name: name.to_string(),
            kind,
            pool,
            source,
            workers,
        })
    }
}

impl DatabaseOptions {
    /// Applies the options to a builder of a database instance.
    pub(crate) fn apply(
        &self,
        mut builder: DatabaseBuilder,
    ) -> Result<DatabaseBuilder, Box<dyn Error>> {
        builder = builder
            .protocol_timing(crate::TIMING)
            .topology_policy(match self.topology {
                Topology::Avoidable => TopologyPolicy::AvoidableTime,
                Topology::Size => TopologyPolicy::SizeCauses,
            });
        if self.leaf_max_entries.is_some()
            || self.leaf_min_entries.is_some()
            || self.index_min_children.is_some()
        {
            let mut sizes = NodeSizePolicy::builder();
            if let Some(value) = self.leaf_max_entries {
                sizes = sizes.leaf_max_entries(value);
            }
            if let Some(value) = self.leaf_min_entries {
                sizes = sizes.leaf_min_entries(value);
            }
            if let Some(value) = self.index_min_children {
                sizes = sizes.index_min_children(value);
            }
            builder = builder.node_size_policy(sizes.build()?);
        }
        if self.inline_max_value_bytes.is_some() || self.inline_max_leaf_bytes.is_some() {
            let defaults = InlinePolicy::default();
            builder = builder.inline_policy(InlinePolicy {
                max_value_bytes: self
                    .inline_max_value_bytes
                    .unwrap_or(defaults.max_value_bytes),
                max_leaf_bytes: self
                    .inline_max_leaf_bytes
                    .unwrap_or(defaults.max_leaf_bytes),
            });
        }
        Ok(builder)
    }
}

impl PoolSpec {
    /// Returns the name of key `index` of the pool. Names have a fixed width,
    /// so that their byte order is their index order.
    pub(crate) fn key(&self, index: usize) -> Vec<u8> {
        let digits = self
            .keys
            .unwrap_or(1)
            .saturating_sub(1)
            .max(1)
            .to_string()
            .len();
        let width = digits.max(self.key_bytes).max(1);
        format!("{index:0width$}").into_bytes()
    }
}

fn quiet<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Option<Duration>, D::Error> {
    let text = String::deserialize(deserializer)?;
    if text == "never" {
        return Ok(None);
    }
    glassdb_bench_scale::parse_duration(&text)
        .map(Some)
        .map_err(serde::de::Error::custom)
}

fn yes() -> bool {
    true
}

fn default_value_bytes() -> usize {
    128
}
