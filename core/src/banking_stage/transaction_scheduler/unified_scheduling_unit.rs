use crate::banking_stage::scheduler_messages::{TransactionId, BundleId};

/// A lightweight enum to identify the type of scheduling unit in the priority queue.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UnifiedSchedulingUnit {
    Transaction(TransactionId),
    Bundle(BundleId),
}
