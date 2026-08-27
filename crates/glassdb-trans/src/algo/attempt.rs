//! Validated lifecycle transitions for one transaction attempt.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AttemptPhase {
    New,
    Engaged,
    Committed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadValidationMode {
    Optimistic,
    Locked,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AcquisitionMode {
    Parallel,
    ForcedSerial,
}

/// Correlated lifecycle state for one transaction handle.
pub(super) struct AttemptState {
    phase: AttemptPhase,
    validation_mode: ReadValidationMode,
    acquisition_mode: AcquisitionMode,
    renewals: usize,
}

impl AttemptState {
    pub(super) fn new() -> Self {
        AttemptState {
            phase: AttemptPhase::New,
            validation_mode: ReadValidationMode::Optimistic,
            acquisition_mode: AcquisitionMode::Parallel,
            renewals: 0,
        }
    }

    /// Gives an active attempt a durable identity, reporting whether this is
    /// its first engagement.
    pub(super) fn engage(&mut self) -> bool {
        match self.phase {
            AttemptPhase::New => {
                self.phase = AttemptPhase::Engaged;
                self.validation_mode = ReadValidationMode::Locked;
                true
            }
            AttemptPhase::Engaged => false,
            AttemptPhase::Committed => panic!("cannot engage a committed transaction"),
        }
    }

    /// Marks the attempt terminal after its commit point has won.
    pub(super) fn commit(&mut self) {
        match self.phase {
            AttemptPhase::New | AttemptPhase::Engaged => {
                self.phase = AttemptPhase::Committed;
            }
            AttemptPhase::Committed => panic!("cannot commit a committed transaction"),
        }
    }

    /// Escalates subsequent read validation to the locked path.
    pub(super) fn force_locked_reads(&mut self) {
        match self.phase {
            AttemptPhase::New | AttemptPhase::Engaged => {
                self.validation_mode = ReadValidationMode::Locked;
            }
            AttemptPhase::Committed => {
                panic!("cannot change read validation for a committed transaction")
            }
        }
    }

    /// Forces all later lock acquisition for this attempt and its replacements
    /// to use the sorted serial order.
    pub(super) fn force_serial_acquisition(&mut self) {
        match self.phase {
            AttemptPhase::New | AttemptPhase::Engaged => {
                self.acquisition_mode = AcquisitionMode::ForcedSerial;
            }
            AttemptPhase::Committed => {
                panic!("cannot change lock acquisition for a committed transaction")
            }
        }
    }

    /// Starts a fresh identity while preserving locked validation and lock
    /// acquisition mode.
    pub(super) fn renew(&mut self) {
        match self.phase {
            // The engine renewal boundary historically accepts any active
            // opaque handle. Wound cleanup may also discover a concurrent
            // terminal outcome before the driver consumes and renews the
            // handle, so renewal must remain valid from every phase.
            AttemptPhase::New | AttemptPhase::Engaged | AttemptPhase::Committed => {
                self.phase = AttemptPhase::New;
                self.validation_mode = ReadValidationMode::Locked;
                self.renewals += 1;
            }
        }
    }

    pub(super) fn needs_abort(&self) -> bool {
        self.phase == AttemptPhase::Engaged
    }

    pub(super) fn should_lock_reads(&self) -> bool {
        self.validation_mode == ReadValidationMode::Locked
    }

    pub(super) fn should_acquire_serially(&self) -> bool {
        self.acquisition_mode == AcquisitionMode::ForcedSerial
    }

    pub(super) fn assert_resettable(&self) {
        assert!(
            self.phase != AttemptPhase::Committed,
            "cannot reset a committed transaction"
        );
    }

    pub(super) fn renewals(&self) -> usize {
        self.renewals
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transition_table_preserves_attempt_invariants() {
        let mut direct = AttemptState::new();
        direct.commit();
        assert_eq!(direct.phase, AttemptPhase::Committed);
        assert_eq!(direct.validation_mode, ReadValidationMode::Optimistic);
        assert_eq!(direct.acquisition_mode, AcquisitionMode::Parallel);

        let mut retry = AttemptState::new();
        retry.force_locked_reads();
        assert_eq!(retry.phase, AttemptPhase::New);
        assert_eq!(retry.validation_mode, ReadValidationMode::Locked);

        let mut engaged = AttemptState::new();
        assert!(engaged.engage());
        assert!(!engaged.engage());
        assert!(engaged.needs_abort());
        assert_eq!(engaged.phase, AttemptPhase::Engaged);
        assert_eq!(engaged.validation_mode, ReadValidationMode::Locked);

        engaged.renew();
        assert_eq!(engaged.phase, AttemptPhase::New);
        assert_eq!(engaged.validation_mode, ReadValidationMode::Locked);
        assert_eq!(engaged.acquisition_mode, AcquisitionMode::Parallel);
        assert_eq!(engaged.renewals, 1);
        assert!(!engaged.needs_abort());

        let mut committed = AttemptState::new();
        committed.engage();
        committed.commit();
        assert_eq!(committed.phase, AttemptPhase::Committed);
        assert_eq!(committed.validation_mode, ReadValidationMode::Locked);
        assert!(!committed.needs_abort());

        committed.renew();
        assert_eq!(committed.phase, AttemptPhase::New);
        assert_eq!(committed.validation_mode, ReadValidationMode::Locked);
        assert_eq!(committed.renewals, 1);

        let mut serial = AttemptState::new();
        serial.engage();
        serial.force_serial_acquisition();
        serial.renew();
        assert!(serial.should_acquire_serially());
    }

    #[test]
    #[should_panic(expected = "cannot reset a committed transaction")]
    fn committed_attempt_cannot_be_reset() {
        let mut state = AttemptState::new();
        state.commit();
        state.assert_resettable();
    }
}
