use crate::banking_stage::{
    bundle_scheduler::bundle_state::BundleId, scheduler_messages::TransactionId,
};

/// A lightweight enum to identify the type of scheduling unit in the priority queue.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UnifiedSchedulingUnit {
    Transaction(TransactionId),
    Bundle(BundleId),
}