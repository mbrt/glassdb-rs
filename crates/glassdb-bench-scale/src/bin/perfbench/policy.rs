//! Topology policies that scenarios open their measurement clients with.

use std::sync::Arc;
use std::time::Duration;

use glassdb::{AvoidableTimePolicy, DatabaseBuilder, FixedTopology, SizePolicy, TopologyPolicy};

use memory::MemoryPolicy;
use net::{AddedTime, NetTimePolicy};
use shadow::Shadowed;
pub(super) use shadow::{ShadowCounts, ShadowTally};

mod memory;
mod net;
mod shadow;

const POLICY_FORMS: &str = "engine|fixed|size|avoidable[:<split>:<merge>]|memory:<split>:<merge>|net[-passes|-conflicts]:<split>:<merge>:<weight>:<half-life>[:<crossing-weight>]";

/// What decides the leaf splits and merges of one measurement client.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum PolicySpec {
    /// No topology policy: soft caps, underfull thresholds, and inline
    /// pressure decide, as they do by default.
    Engine,
    /// [`FixedTopology`]: only the hard cap splits leaves.
    Fixed,
    /// [`SizePolicy`]: the size causes of `Engine`, but decided once in each
    /// window from the sizes and inline pressure of that window.
    Size,
    /// [`AvoidableTimePolicy`] with its default split and merge threshold
    /// multiples, or with these.
    Avoidable { thresholds: Option<(f64, f64)> },
    /// [`MemoryPolicy`] with the split and merge threshold multiples. They have
    /// no default, because the defaults of [`AvoidableTimePolicy`] are
    /// private.
    Memory { split: f64, merge: f64 },
    /// [`NetTimePolicy`] with the split and merge threshold multiples, the
    /// weight of a divided transaction, and the half-life in seconds of its
    /// moving average. The ADR-074 runs use `net:0.05:0.1:0.25:10`. Its split
    /// multiple is lower than the one of [`AvoidableTimePolicy`], because a
    /// moving average stays below the peaks of single windows. `net`
    /// estimates the time that a split adds from the divided commits,
    /// `net-passes` from the time of the divided commit passes, and
    /// `net-conflicts` from the time of those that a conflict ended. The
    /// optional crossing weight adds the conflict time of the transactions
    /// over two adjacent leaves to their merge side.
    Net {
        added: AddedTime,
        split: f64,
        merge: f64,
        weight: f64,
        half_life: f64,
        crossing: Option<f64>,
    },
}

impl PolicySpec {
    /// Parses `engine`, `fixed`, `size`, `avoidable`,
    /// `avoidable:<split>:<merge>`, `memory:<split>:<merge>`, or
    /// `net[-passes|-conflicts]:<split>:<merge>:<weight>:<half-life>[:<crossing-weight>]`.
    pub(super) fn parse(value: &str) -> Result<Self, String> {
        let multiple = |text: &str| match text.parse::<f64>() {
            Ok(multiple) if multiple.is_finite() && multiple >= 0.0 => Ok(multiple),
            _ => Err(format!(
                "invalid threshold multiple {text:?} in policy {value:?}"
            )),
        };
        match value.split(':').collect::<Vec<_>>().as_slice() {
            ["engine"] => Ok(Self::Engine),
            ["fixed"] => Ok(Self::Fixed),
            ["size"] => Ok(Self::Size),
            ["avoidable"] => Ok(Self::Avoidable { thresholds: None }),
            ["avoidable", split, merge] => Ok(Self::Avoidable {
                thresholds: Some((multiple(split)?, multiple(merge)?)),
            }),
            ["memory", split, merge] => Ok(Self::Memory {
                split: multiple(split)?,
                merge: multiple(merge)?,
            }),
            [
                name @ ("net" | "net-passes" | "net-conflicts"),
                split,
                merge,
                weight,
                half_life,
                crossing @ ..,
            ] if crossing.len() <= 1 => Ok(Self::Net {
                added: match *name {
                    "net" => AddedTime::Commits,
                    "net-passes" => AddedTime::Passes,
                    _ => AddedTime::Conflicts,
                },
                split: multiple(split)?,
                merge: multiple(merge)?,
                weight: multiple(weight)?,
                half_life: match half_life.parse::<f64>() {
                    Ok(seconds) if seconds.is_finite() && seconds > 0.0 => seconds,
                    _ => {
                        return Err(format!(
                            "invalid half-life {half_life:?} in policy {value:?}"
                        ));
                    }
                },
                crossing: crossing
                    .first()
                    .map(|weight| multiple(weight))
                    .transpose()?,
            }),
            _ => Err(format!(
                "unknown policy {value:?} (expected {POLICY_FORMS})"
            )),
        }
    }

    /// Returns the label of the policy in reports, which parses back to it.
    pub(super) fn label(self) -> String {
        match self {
            Self::Engine => "engine".into(),
            Self::Fixed => "fixed".into(),
            Self::Size => "size".into(),
            Self::Avoidable { thresholds: None } => "avoidable".into(),
            Self::Avoidable {
                thresholds: Some((split, merge)),
            } => format!("avoidable:{split}:{merge}"),
            Self::Memory { split, merge } => format!("memory:{split}:{merge}"),
            Self::Net {
                added,
                split,
                merge,
                weight,
                half_life,
                crossing,
            } => {
                let name = match added {
                    AddedTime::Commits => "net",
                    AddedTime::Passes => "net-passes",
                    AddedTime::Conflicts => "net-conflicts",
                };
                let crossing = crossing.map_or(String::new(), |weight| format!(":{weight}"));
                format!("{name}:{split}:{merge}:{weight}:{half_life}{crossing}")
            }
        }
    }

    /// Sets the policy on `builder`.
    pub(super) fn apply(self, builder: DatabaseBuilder) -> DatabaseBuilder {
        match self {
            Self::Engine => builder,
            Self::Fixed => builder.topology_policy(FixedTopology),
            Self::Size => builder.topology_policy(SizePolicy),
            Self::Avoidable { thresholds: None } => {
                builder.topology_policy(AvoidableTimePolicy::new())
            }
            Self::Avoidable {
                thresholds: Some((split, merge)),
            } => builder.topology_policy(
                AvoidableTimePolicy::new()
                    .split_threshold(split)
                    .merge_threshold(merge),
            ),
            Self::Memory { split, merge } => {
                builder.topology_policy(MemoryPolicy::new(split, merge))
            }
            Self::Net {
                added,
                split,
                merge,
                weight,
                half_life,
                crossing,
            } => builder.topology_policy(NetTimePolicy::new(
                added,
                split,
                merge,
                weight,
                crossing,
                Duration::from_secs_f64(half_life),
            )),
        }
    }

    /// Sets the policy on `builder`, and records in `tally` the windows of the
    /// database instance with index `database`, and what each of `shadows`
    /// would decide on them.
    ///
    /// # Panics
    ///
    /// Panics when this policy or a shadow is `Engine`, which has no windows.
    pub(super) fn apply_shadowed(
        self,
        builder: DatabaseBuilder,
        shadows: &[PolicySpec],
        tally: &Arc<ShadowTally>,
        database: usize,
    ) -> DatabaseBuilder {
        let policy = self.policy().expect("the engine policy has no windows");
        let shadows = shadows
            .iter()
            .map(|shadow| shadow.policy().expect("the engine policy has no windows"))
            .collect();
        builder.topology_policy(Shadowed {
            policy,
            shadows,
            tally: tally.clone(),
            database,
        })
    }

    fn policy(self) -> Option<Box<dyn TopologyPolicy>> {
        Some(match self {
            Self::Engine => return None,
            Self::Fixed => Box::new(FixedTopology),
            Self::Size => Box::new(SizePolicy),
            Self::Avoidable { thresholds: None } => Box::new(AvoidableTimePolicy::new()),
            Self::Avoidable {
                thresholds: Some((split, merge)),
            } => Box::new(
                AvoidableTimePolicy::new()
                    .split_threshold(split)
                    .merge_threshold(merge),
            ),
            Self::Memory { split, merge } => Box::new(MemoryPolicy::new(split, merge)),
            Self::Net {
                added,
                split,
                merge,
                weight,
                half_life,
                crossing,
            } => Box::new(NetTimePolicy::new(
                added,
                split,
                merge,
                weight,
                crossing,
                Duration::from_secs_f64(half_life),
            )),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_parse_back_to_their_policy() {
        for value in [
            "engine",
            "fixed",
            "size",
            "avoidable",
            "avoidable:1:1",
            "avoidable:0.5:2",
            "memory:1:1",
            "memory:0.25:0.1",
            "net:0.05:0.1:0.25:10",
            "net:1:1:0:0.5",
            "net-passes:0.05:0.1:0.5:10",
            "net-conflicts:0.05:0.1:4:10",
            "net-conflicts:0.05:0.1:4:10:1",
        ] {
            let policy = PolicySpec::parse(value).unwrap();
            assert_eq!(policy.label(), value);
            assert_eq!(PolicySpec::parse(&policy.label()), Ok(policy));
        }
    }

    #[test]
    fn invalid_policies_are_rejected() {
        let cases = [
            (
                "legacy",
                "unknown policy \"legacy\" (expected engine|fixed|size|avoidable[:<split>:<merge>]|memory:<split>:<merge>|net[-passes|-conflicts]:<split>:<merge>:<weight>:<half-life>[:<crossing-weight>])",
            ),
            (
                "avoidable:-1:1",
                "invalid threshold multiple \"-1\" in policy \"avoidable:-1:1\"",
            ),
            (
                "avoidable:1",
                "unknown policy \"avoidable:1\" (expected engine|fixed|size|avoidable[:<split>:<merge>]|memory:<split>:<merge>|net[-passes|-conflicts]:<split>:<merge>:<weight>:<half-life>[:<crossing-weight>])",
            ),
            (
                "memory",
                "unknown policy \"memory\" (expected engine|fixed|size|avoidable[:<split>:<merge>]|memory:<split>:<merge>|net[-passes|-conflicts]:<split>:<merge>:<weight>:<half-life>[:<crossing-weight>])",
            ),
            (
                "net:0.25:0.1:0.25:0",
                "invalid half-life \"0\" in policy \"net:0.25:0.1:0.25:0\"",
            ),
            (
                "net:0.25:0.1:0.25:10:1:1",
                "unknown policy \"net:0.25:0.1:0.25:10:1:1\" (expected engine|fixed|size|avoidable[:<split>:<merge>]|memory:<split>:<merge>|net[-passes|-conflicts]:<split>:<merge>:<weight>:<half-life>[:<crossing-weight>])",
            ),
        ];
        for (value, expected) in cases {
            assert_eq!(PolicySpec::parse(value), Err(expected.to_string()));
        }
    }
}
