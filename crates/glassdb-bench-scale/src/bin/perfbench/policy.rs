//! Topology policies that scenarios open their measurement clients with.

use glassdb::{AvoidableTimePolicy, DatabaseBuilder, FixedTopology, SizePolicy};

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
    /// [`AvoidableTimePolicy`] with the split and merge threshold multiples.
    Avoidable { split: f64, merge: f64 },
}

impl PolicySpec {
    /// Parses `engine`, `fixed`, `size`, `avoidable`, or
    /// `avoidable:<split>:<merge>`.
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
            ["avoidable"] => Ok(Self::Avoidable {
                split: 1.0,
                merge: 1.0,
            }),
            ["avoidable", split, merge] => Ok(Self::Avoidable {
                split: multiple(split)?,
                merge: multiple(merge)?,
            }),
            _ => Err(format!(
                "unknown policy {value:?} (expected engine|fixed|size|avoidable[:<split>:<merge>])"
            )),
        }
    }

    /// Returns the label of the policy in reports, which parses back to it.
    pub(super) fn label(self) -> String {
        match self {
            Self::Engine => "engine".into(),
            Self::Fixed => "fixed".into(),
            Self::Size => "size".into(),
            Self::Avoidable { split, merge } if split == 1.0 && merge == 1.0 => "avoidable".into(),
            Self::Avoidable { split, merge } => format!("avoidable:{split}:{merge}"),
        }
    }

    /// Sets the policy on `builder`.
    pub(super) fn apply(self, builder: DatabaseBuilder) -> DatabaseBuilder {
        match self {
            Self::Engine => builder,
            Self::Fixed => builder.topology_policy(FixedTopology),
            Self::Size => builder.topology_policy(SizePolicy),
            Self::Avoidable { split, merge } => builder.topology_policy(
                AvoidableTimePolicy::new()
                    .split_threshold(split)
                    .merge_threshold(merge),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_parse_back_to_their_policy() {
        for value in ["engine", "fixed", "size", "avoidable", "avoidable:0.5:2"] {
            let policy = PolicySpec::parse(value).unwrap();
            assert_eq!(policy.label(), value);
            assert_eq!(PolicySpec::parse(&policy.label()), Ok(policy));
        }
        assert_eq!(
            PolicySpec::parse("avoidable:1:1"),
            PolicySpec::parse("avoidable")
        );
    }

    #[test]
    fn invalid_policies_are_rejected() {
        let cases = [
            (
                "legacy",
                "unknown policy \"legacy\" (expected engine|fixed|size|avoidable[:<split>:<merge>])",
            ),
            (
                "avoidable:-1:1",
                "invalid threshold multiple \"-1\" in policy \"avoidable:-1:1\"",
            ),
            (
                "avoidable:1",
                "unknown policy \"avoidable:1\" (expected engine|fixed|size|avoidable[:<split>:<merge>])",
            ),
        ];
        for (value, expected) in cases {
            assert_eq!(PolicySpec::parse(value), Err(expected.to_string()));
        }
    }
}
