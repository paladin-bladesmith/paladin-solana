use {
    crate::banking_stage::consumer::RetryableIndex,
    solana_clock::{Epoch, Slot},
    std::fmt::Display,
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

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
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

/// Message: [Scheduler -> Worker]
/// Transactions to be consumed (i.e. executed, recorded, and committed)
pub struct ConsumeWork<Tx> {
    pub batch_id: TransactionBatchId,
    pub ids: Vec<TransactionId>,
    pub transactions: Vec<Tx>,
    pub max_ages: Vec<MaxAge>,
    pub bundle_id: Option<BundleId>,
}

/// Message: [Worker -> Scheduler]
/// Processed transactions with retry information.
/// For bundle batches: if any transaction fails, the entire bundle should be retried (bundle_id will be Some).
/// For regular batches: individual transactions can be retried independently.
pub struct FinishedConsumeWork<Tx> {
    pub work: ConsumeWork<Tx>,
    /// Indexes of individual transactions that should be retried.
    /// For bundle batches: empty if bundle succeeded, contains indexes if bundle should be retried.
    /// For regular batches: contains indexes of failed transactions.
    pub retryable_indexes: Vec<RetryableIndex>,
}
