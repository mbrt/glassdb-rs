//! The end of the pressure phase of the `inline-pressure` experiment: the
//! leaves adapt to their aggregate inline budget (ADR-056, ADR-074) when a
//! whole pass over the pressured keys commits without locks.

use glassdb::Stats;

use crate::runner::until::PassCondition;

/// Ends the measurement at the first pass that takes no lock. That pass is
/// outside the measurement, so the measurement has the passes under pressure.
pub(crate) struct NoLockedPass;

impl PassCondition for NoLockedPass {
    fn ends(&self, pass: &Stats) -> bool {
        pass.locker.calls == 0
    }

    fn excludes_last_pass(&self) -> bool {
        true
    }
}
