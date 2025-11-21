use {
    solana_bundle::SanitizedBundle, solana_clock::{Epoch, Slot}, std::fmt::Display
};

/// A unique identifier for a transaction batch.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct TransactionBatchId(u64);

impl TransactionBatchId {
    pub fn new(index: u64) -> Self {
        Self(index)
    }
}

impl Display for TransactionBatchId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub type TransactionId = usize;
pub type BundleId = usize;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct MaxAge {
    pub sanitized_epoch: Epoch,
    pub alt_invalidation_slot: Slot,
}

impl MaxAge {
    pub const MAX: Self = Self {
        sanitized_epoch: Epoch::MAX,
        alt_invalidation_slot: Slot::MAX,
    };
}

/// A work item that can be either a transaction or a bundle
pub enum ConsumeWorkItem<Tx> {
    Transaction {
        id: TransactionId,
        transaction: Tx,
        max_age: MaxAge,
    },
    Bundle {
        id: BundleId,
        bundle: SanitizedBundle,
        /// Max age for bundle validity.
        #[allow(dead_code)]
        max_age: MaxAge,
    },
}

/// Message: [Scheduler -> Worker]
/// Work items (transactions and/or bundles) to be consumed (i.e. executed, recorded, and committed)
pub struct ConsumeWork<Tx> {
    pub batch_id: TransactionBatchId,
    pub items: Vec<ConsumeWorkItem<Tx>>,
}

/// Message: [Worker -> Scheduler]
/// Processed work items with retry information.
/// For transactions: retryable_transaction_indexes contains indexes into the items vec
/// For bundles: retryable_bundle_ids contains the bundle IDs that should be retried
pub struct FinishedConsumeWork<Tx> {
    pub work: ConsumeWork<Tx>,
    pub retryable_transaction_indexes: Vec<usize>,
    pub retryable_bundle_ids: Vec<BundleId>,
}