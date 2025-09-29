#[cfg(feature = "dev-context-only-utils")]
use qualifier_attr::qualifiers;
#[cfg(feature = "dev-context-only-utils")]
use {
    super::unified_scheduling_unit::UnifiedSchedulingUnit,
    prio_graph::TopLevelId,
    std::hash::{Hash, Hasher},
};

/// A unique identifier tied with priority ordering for a transaction or a bundle.
#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct UnifiedPriorityId {
    pub(crate) priority: u64,
    pub(crate) unit: UnifiedSchedulingUnit,
}

impl UnifiedPriorityId {
    pub(crate) fn new(priority: u64, unit: UnifiedSchedulingUnit) -> Self {
        Self { priority, unit }
    }
}

impl Hash for UnifiedPriorityId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.unit.hash(state);
    }
}

impl TopLevelId<Self> for UnifiedPriorityId {
    fn id(&self) -> Self {
        *self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::banking_stage::unified_schedule::unified_scheduling_unit::UnifiedSchedulingUnit;

    #[test]
    fn test_transaction_priority_id_ordering() {
        // Higher priority first
        {
            let id1 = UnifiedPriorityId::new(1, UnifiedSchedulingUnit::Transaction(1));
            let id2 = UnifiedPriorityId::new(2, UnifiedSchedulingUnit::Transaction(1));
            assert!(id1 < id2);
            assert!(id1 <= id2);
            assert!(id2 > id1);
            assert!(id2 >= id1);
        }

        // Equal priority then compare by id
        {
            let id1 = UnifiedPriorityId::new(1, UnifiedSchedulingUnit::Transaction(1));
            let id2 = UnifiedPriorityId::new(1, UnifiedSchedulingUnit::Transaction(2));
            assert!(id1 < id2);
            assert!(id1 <= id2);
            assert!(id2 > id1);
            assert!(id2 >= id1);
        }

        // Equal priority and id
        {
            let id1 = UnifiedPriorityId::new(1, UnifiedSchedulingUnit::Transaction(1));
            let id2 = UnifiedPriorityId::new(1, UnifiedSchedulingUnit::Transaction(1));
            assert_eq!(id1, id2);
            assert!(id1 >= id2);
            assert!(id1 <= id2);
            assert!(id2 >= id1);
            assert!(id2 <= id1);
        }

        // Equal priority, different unit types
        {
            let id1 = UnifiedPriorityId::new(1, UnifiedSchedulingUnit::Transaction(1));
            let id2 = UnifiedPriorityId::new(1, UnifiedSchedulingUnit::Bundle(1));
            assert!(id1 < id2); // Transaction < Bundle
        }
    }
}
