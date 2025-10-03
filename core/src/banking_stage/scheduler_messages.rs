use {
    solana_bundle::SanitizedBundle,
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

/// Message: [Scheduler -> Worker]
/// Transactions to be consumed (i.e. executed, recorded, and committed)
pub struct ConsumeWork<Tx> {
    pub batch_id: TransactionBatchId,
    pub ids: Vec<TransactionId>,
    pub transactions: Vec<Tx>,
    pub max_ages: Vec<MaxAge>,
}

/// Message: [Worker -> Scheduler]
/// Processed transactions.
pub struct FinishedConsumeWork<Tx> {
    pub work: ConsumeWork<Tx>,
    pub retryable_indexes: Vec<usize>,
}

pub type BundleId = u64;

/// Message: [Scheduler -> BundleWorker]
/// Bundle to be consumed (i.e. executed, recorded, and committed)
pub struct BundleConsumeWork {
    pub bundle_id: BundleId,
    pub bundle: SanitizedBundle,
    pub _max_age: MaxAge,
}

/// Message: [BundleWorker -> Scheduler]
/// Processed bundle.
pub struct FinishedBundleConsumeWork {
    pub work: BundleConsumeWork,
    pub retryable: bool,
}
