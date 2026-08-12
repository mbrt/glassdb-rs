//! Lock-state representation shared across persisted coordination scopes.

use glassdb_data::TxId;
use glassdb_proto as pb;

/// The type of lock held on a storage object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LockType {
    #[default]
    Unknown,
    None,
    Read,
    Write,
    Create,
}

impl std::fmt::Display for LockType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

/// A canonical set of transactions holding one lock.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct HolderSet {
    holders: Vec<TxId>,
}

impl HolderSet {
    fn from_wire(locked_by: Vec<Vec<u8>>) -> Result<Self, LockStateError> {
        Self::from_holders(locked_by.into_iter().map(TxId::from_bytes).collect())
    }

    fn from_holders(mut holders: Vec<TxId>) -> Result<Self, LockStateError> {
        holders.sort();
        if holders.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(LockStateError::DuplicateHolder);
        }
        Ok(Self { holders })
    }

    fn as_slice(&self) -> &[TxId] {
        &self.holders
    }

    fn contains(&self, id: &TxId) -> bool {
        self.holders.binary_search(id).is_ok()
    }

    fn insert(&mut self, id: TxId) {
        if let Err(index) = self.holders.binary_search(&id) {
            self.holders.insert(index, id);
        }
    }

    fn replace(&mut self, id: TxId) {
        self.holders.clear();
        self.holders.push(id);
    }

    fn remove(&mut self, id: &TxId) -> bool {
        let Ok(index) = self.holders.binary_search(id) else {
            return false;
        };
        self.holders.remove(index);
        true
    }

    fn clear(&mut self) {
        self.holders.clear();
    }

    fn is_empty(&self) -> bool {
        self.holders.is_empty()
    }

    fn into_vec(self) -> Vec<TxId> {
        self.holders
    }

    fn to_wire(&self) -> Vec<Vec<u8>> {
        self.holders
            .iter()
            .map(|id| id.as_bytes().to_vec())
            .collect()
    }
}

/// A validated neutral lock type and its canonical holders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LockState {
    typ: LockType,
    holders: HolderSet,
}

impl Default for LockState {
    fn default() -> Self {
        Self {
            typ: LockType::None,
            holders: HolderSet::default(),
        }
    }
}

impl LockState {
    /// Decodes and validates one persisted lock state.
    pub(crate) fn from_wire(
        lock_type: i32,
        locked_by: Vec<Vec<u8>>,
    ) -> Result<Self, LockStateError> {
        let typ = lock_type_from_proto(lock_type);
        let holders = HolderSet::from_wire(locked_by)?;
        Self::from_parts(typ, holders)
    }

    /// Returns the held lock type.
    pub(crate) fn lock_type(&self) -> LockType {
        self.typ
    }

    /// Returns the transactions holding the lock in canonical order.
    pub(crate) fn holders(&self) -> &[TxId] {
        self.holders.as_slice()
    }

    /// Reports whether `id` holds this lock.
    pub(crate) fn contains(&self, id: &TxId) -> bool {
        self.holders.contains(id)
    }

    /// Adds one shared holder.
    pub(crate) fn add_reader(&mut self, id: TxId) {
        self.holders.insert(id);
        self.typ = LockType::Read;
    }

    /// Replaces the lock with one exclusive holder.
    pub(crate) fn set_writer(&mut self, id: TxId) {
        self.holders.replace(id);
        self.typ = LockType::Write;
    }

    /// Removes one holder and unlocks an empty state.
    pub(crate) fn remove(&mut self, id: &TxId) -> bool {
        let removed = self.holders.remove(id);
        if self.holders.is_empty() {
            self.typ = LockType::None;
        }
        removed
    }

    /// Removes every holder.
    pub(crate) fn clear(&mut self) {
        self.typ = LockType::None;
        self.holders.clear();
    }

    /// Reports whether the lock has no holders.
    pub(crate) fn is_empty(&self) -> bool {
        self.typ == LockType::None && self.holders.is_empty()
    }

    /// Splits the neutral state into compatibility fields.
    pub(crate) fn into_parts(self) -> (LockType, Vec<TxId>) {
        (self.typ, self.holders.into_vec())
    }

    /// Builds the canonical protobuf fields for this lock.
    pub(crate) fn to_wire(&self) -> (i32, Vec<Vec<u8>>) {
        (lock_type_to_proto(self.typ) as i32, self.holders.to_wire())
    }

    fn from_parts(mut typ: LockType, holders: HolderSet) -> Result<Self, LockStateError> {
        // Proto3 omitted the enum field in legacy empty locks, which decodes as
        // Unknown. No mutation can produce Unknown with holders.
        if typ == LockType::Unknown && holders.is_empty() {
            typ = LockType::None;
        }
        match (typ, holders.as_slice()) {
            (LockType::None, [])
            | (LockType::Read, [_, ..])
            | (LockType::Write | LockType::Create, [_]) => Ok(Self { typ, holders }),
            _ => Err(LockStateError::InvalidShape),
        }
    }
}

/// Why persisted lock fields could not form a neutral lock state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LockStateError {
    DuplicateHolder,
    InvalidShape,
}

/// Maps the storage-domain lock type to its stable protobuf value.
pub(crate) fn lock_type_to_proto(typ: LockType) -> pb::lock::LockType {
    match typ {
        LockType::None => pb::lock::LockType::None,
        LockType::Read => pb::lock::LockType::Read,
        LockType::Write => pb::lock::LockType::Write,
        LockType::Create => pb::lock::LockType::Create,
        LockType::Unknown => pb::lock::LockType::Unknown,
    }
}

/// Maps a protobuf value, including unrecognized integers, to the domain type.
pub(crate) fn lock_type_from_proto(typ: i32) -> LockType {
    match pb::lock::LockType::try_from(typ) {
        Ok(pb::lock::LockType::None) => LockType::None,
        Ok(pb::lock::LockType::Read) => LockType::Read,
        Ok(pb::lock::LockType::Write) => LockType::Write,
        Ok(pb::lock::LockType::Create) => LockType::Create,
        _ => LockType::Unknown,
    }
}

/// Encodes compatibility holders in canonical order.
pub(crate) fn holders_to_proto(holders: &[TxId]) -> Vec<Vec<u8>> {
    let mut locked_by: Vec<Vec<u8>> = holders.iter().map(|id| id.as_bytes().to_vec()).collect();
    locked_by.sort();
    locked_by
}
