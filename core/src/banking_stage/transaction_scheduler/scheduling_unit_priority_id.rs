use {
    crate::banking_stage::scheduler_messages::{BundleId, TransactionId},
    prio_graph::TopLevelId,
    std::hash::{Hash, Hasher},
};

/// A unified scheduling unit that can be either a standalone transaction or a bundle.
/// This enum is used in the priority queue to allow bundles and transactions to compete fairly.
/// Bundle variant directly stores the container_ids vector.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum SchedulingUnitId {
    Transaction(TransactionId),
    Bundle(BundleId),
}

impl SchedulingUnitId {
    pub fn is_transaction(&self) -> bool {
        matches!(self, Self::Transaction(_))
    }

    pub fn is_bundle(&self) -> bool {
        matches!(self, Self::Bundle(_))
    }
}

impl Hash for SchedulingUnitId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Transaction(id) => {
                0u8.hash(state);
                id.hash(state);
            }
            Self::Bundle(id) => {
                1u8.hash(state);
                id.hash(state);
            }
        }
    }
}

/// Priority wrapper for scheduling units that can be compared in the priority queue
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SchedulingUnitPriorityId {
    pub priority: u64,
    pub id: SchedulingUnitId,
}

impl SchedulingUnitPriorityId {
    pub fn new(priority: u64, id: SchedulingUnitId) -> Self {
        Self { priority, id }
    }

    pub fn id(&self) -> usize {
        match &self.id {
            SchedulingUnitId::Transaction(tx_id) => *tx_id as usize,
            SchedulingUnitId::Bundle(bundle_id) => *bundle_id as usize,
        }
    }
}

impl Hash for SchedulingUnitPriorityId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl TopLevelId<Self> for SchedulingUnitPriorityId {
    fn id(&self) -> Self {
        self.clone()
    }
}
