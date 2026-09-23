//! Validated lifecycle transitions for one transaction handle.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IdentityPhase {
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
pub(super) struct HandleState {
    phase: IdentityPhase,
    validation_mode: ReadValidationMode,
    acquisition_mode: AcquisitionMode,
    renewals: usize,
}

impl HandleState {
    pub(super) fn new() -> Self {
        HandleState {
            phase: IdentityPhase::New,
            validation_mode: ReadValidationMode::Optimistic,
            acquisition_mode: AcquisitionMode::Parallel,
            renewals: 0,
        }
    }

    /// Makes the current transaction identity durable, reporting whether this
    /// is its first engagement.
    pub(super) fn engage(&mut self) -> bool {
        match self.phase {
            IdentityPhase::New => {
                self.phase = IdentityPhase::Engaged;
                self.validation_mode = ReadValidationMode::Locked;
                true
            }
            IdentityPhase::Engaged => false,
            IdentityPhase::Committed => panic!("cannot engage a committed transaction"),
        }
    }

    /// Marks the transaction committed after its commit point has won.
    pub(super) fn commit(&mut self) {
        match self.phase {
            IdentityPhase::New | IdentityPhase::Engaged => {
                self.phase = IdentityPhase::Committed;
            }
            IdentityPhase::Committed => panic!("cannot commit a committed transaction"),
        }
    }

    /// Escalates subsequent read validation to locked validation.
    pub(super) fn force_locked_reads(&mut self) {
        match self.phase {
            IdentityPhase::New | IdentityPhase::Engaged => {
                self.validation_mode = ReadValidationMode::Locked;
            }
            IdentityPhase::Committed => {
                panic!("cannot change read validation for a committed transaction")
            }
        }
    }

    /// Forces all later lock acquisition for this identity and its renewals
    /// to use the sorted serial order.
    pub(super) fn force_serial_acquisition(&mut self) {
        match self.phase {
            IdentityPhase::New | IdentityPhase::Engaged => {
                self.acquisition_mode = AcquisitionMode::ForcedSerial;
            }
            IdentityPhase::Committed => {
                panic!("cannot change lock acquisition for a committed transaction")
            }
        }
    }

    /// Starts a fresh identity while preserving locked validation and lock
    /// acquisition mode.
    pub(super) fn renew(&mut self) {
        match self.phase {
            // The engine renewal boundary historically accepts any active
            // opaque handle. Wound handling may also discover a concurrent
            // final status before the driver consumes and renews the
            // handle, so renewal must remain valid from every phase.
            IdentityPhase::New | IdentityPhase::Engaged | IdentityPhase::Committed => {
                self.phase = IdentityPhase::New;
                self.validation_mode = ReadValidationMode::Locked;
                self.renewals += 1;
            }
        }
    }

    pub(super) fn needs_abort(&self) -> bool {
        self.phase == IdentityPhase::Engaged
    }

    pub(super) fn should_lock_reads(&self) -> bool {
        self.validation_mode == ReadValidationMode::Locked
    }

    pub(super) fn should_acquire_serially(&self) -> bool {
        self.acquisition_mode == AcquisitionMode::ForcedSerial
    }

    pub(super) fn assert_resettable(&self) {
        assert!(
            self.phase != IdentityPhase::Committed,
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
    fn transition_table_preserves_handle_invariants() {
        let mut direct = HandleState::new();
        direct.commit();
        assert_eq!(direct.phase, IdentityPhase::Committed);
        assert_eq!(direct.validation_mode, ReadValidationMode::Optimistic);
        assert_eq!(direct.acquisition_mode, AcquisitionMode::Parallel);

        let mut replayed = HandleState::new();
        replayed.force_locked_reads();
        assert_eq!(replayed.phase, IdentityPhase::New);
        assert_eq!(replayed.validation_mode, ReadValidationMode::Locked);

        let mut engaged = HandleState::new();
        assert!(engaged.engage());
        assert!(!engaged.engage());
        assert!(engaged.needs_abort());
        assert_eq!(engaged.phase, IdentityPhase::Engaged);
        assert_eq!(engaged.validation_mode, ReadValidationMode::Locked);

        engaged.renew();
        assert_eq!(engaged.phase, IdentityPhase::New);
        assert_eq!(engaged.validation_mode, ReadValidationMode::Locked);
        assert_eq!(engaged.acquisition_mode, AcquisitionMode::Parallel);
        assert_eq!(engaged.renewals, 1);
        assert!(!engaged.needs_abort());

        let mut committed = HandleState::new();
        committed.engage();
        committed.commit();
        assert_eq!(committed.phase, IdentityPhase::Committed);
        assert_eq!(committed.validation_mode, ReadValidationMode::Locked);
        assert!(!committed.needs_abort());

        committed.renew();
        assert_eq!(committed.phase, IdentityPhase::New);
        assert_eq!(committed.validation_mode, ReadValidationMode::Locked);
        assert_eq!(committed.renewals, 1);

        let mut serial = HandleState::new();
        serial.engage();
        serial.force_serial_acquisition();
        serial.renew();
        assert!(serial.should_acquire_serially());
    }

    #[test]
    #[should_panic(expected = "cannot reset a committed transaction")]
    fn committed_handle_cannot_be_reset() {
        let mut state = HandleState::new();
        state.commit();
        state.assert_resettable();
    }
}
