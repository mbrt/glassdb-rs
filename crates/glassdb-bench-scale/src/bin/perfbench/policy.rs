//! Topology policies that scenarios open their measurement clients with.

use std::sync::Arc;

use glassdb::{AvoidableTimePolicy, DatabaseBuilder, FixedTopology, SizePolicy, TopologyPolicy};

use memory::MemoryPolicy;
use shadow::Shadowed;
pub(super) use shadow::{ShadowCounts, ShadowTally};

mod memory;
mod shadow;

const POLICY_FORMS: &str = "engine|fixed|size|avoidable[:<split>:<merge>]|memory:<split>:<merge>";

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
    /// no default, because the defaults of [`AvoidableTimePolicy`] apply to
    /// its moving averages.
    Memory { split: f64, merge: f64 },
}

impl PolicySpec {
    /// Parses `engine`, `fixed`, `size`, `avoidable`,
    /// `avoidable:<split>:<merge>`, or `memory:<split>:<merge>`.
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
                "unknown policy \"legacy\" (expected engine|fixed|size|avoidable[:<split>:<merge>]|memory:<split>:<merge>)",
            ),
            (
                "avoidable:-1:1",
                "invalid threshold multiple \"-1\" in policy \"avoidable:-1:1\"",
            ),
            (
                "avoidable:1",
                "unknown policy \"avoidable:1\" (expected engine|fixed|size|avoidable[:<split>:<merge>]|memory:<split>:<merge>)",
            ),
            (
                "memory",
                "unknown policy \"memory\" (expected engine|fixed|size|avoidable[:<split>:<merge>]|memory:<split>:<merge>)",
            ),
        ];
        for (value, expected) in cases {
            assert_eq!(PolicySpec::parse(value), Err(expected.to_string()));
        }
    }
}
