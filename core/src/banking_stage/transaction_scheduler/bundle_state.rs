use crate::banking_stage::scheduler_messages::MaxAge;
#[cfg(feature = "dev-context-only-utils")]
use qualifier_attr::qualifiers;
use {ahash::{HashSet, HashSetExt}, solana_bundle::SanitizedBundle, solana_pubkey::Pubkey};

/// BundleState is used to track the state of a bundle in the bundle unified schedular
/// and the new banking stage as a whole.
/// 
/// Newly received bundles initially have `Some(bundle)`.
/// When a bundle is scheduled, the bundle is taken from the Option.
/// When a bundle finishes procesing it is retried or cost buffered, in this
/// case the bundle is added back into the Option. If it is not retried or cost buffered,
/// the state is dropped.
#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
pub (crate) struct BundleState {
    /// If some, the bundle is available for consuming
    /// If none, the bundle is currently scheduled or being processed
    bundle: Option<SanitizedBundle>,
    /// Tracks information on the maximum age the bundle's pre-processing
    /// is valid for.
    max_age: MaxAge,
    /// Priority of the bundle.
    priority: u64,
    /// Estimated cost of the bundle.
    cost: u64,
    /// Pre-computed deduplicated write accounts from all transactions in bundle.
    write_accounts: Vec<Pubkey>,
    /// Pre-computed deduplicated read accounts from all transactions in bundle.
    read_accounts: Vec<Pubkey>,
}

impl BundleState {
    /// Creates a new `BundleState` in the `Unprocessed` state.
    /// Extracts and caches deduplicated account lists for efficient reuse.
    pub(crate) fn new(bundle: SanitizedBundle, max_age: MaxAge, priority: u64, cost: u64) -> Self {
        let (write_accounts, read_accounts) = Self::extract_bundle_accounts(&bundle);
        
        Self {
            bundle: Some(bundle),
            max_age,
            priority,
            cost,
            write_accounts,
            read_accounts,
        }
    }

    /// Intended to be called when a bundle is consumed. This method
    /// takes ownership of the bundle from the state.
    ///
    /// # Panics
    /// This method will panic if the bundle has already been consumed.
    pub(crate) fn take_bundle_for_consuming(&mut self) -> (SanitizedBundle, MaxAge) {
        let bundle = self
            .bundle
            .take()
            .expect("bundle not already pending");
        (bundle, self.max_age)
    }

    /// Return the priority of the bundle.
    /// This is *not* the same as the `compute_unit_price` of the bundle transactions.
    /// The priority is used to order bundles for processing.
    pub(crate) fn priority(&self) -> u64 {
        self.priority
    }

    /// Returns the cost of the bundle.
    pub(crate) fn cost(&self) -> u64 {
        self.cost
    }

    /// When the bundle is retried or cost buffered
    /// Intended to be called when a bundle is retried. This method will
    /// put the bundle back into the state.
    /// # Panics
    /// This method will panic if the bundle is already in the state.
    pub(crate) fn retry_bundle(&mut self, bundle: SanitizedBundle) {
        assert!(self.bundle.replace(bundle).is_none(), "bundle already present");
    }

    /// Returns the pre-computed deduplicated write accounts for this bundle.
    pub(crate) fn write_accounts(&self) -> &[Pubkey] {
        &self.write_accounts
    }

    /// Returns the pre-computed deduplicated read accounts for this bundle.
    pub(crate) fn read_accounts(&self) -> &[Pubkey] {
        &self.read_accounts
    }

    /// Extract unique write and read accounts from bundle in a single pass.
    /// Returns (write_accounts, read_accounts) with duplicates removed.
    fn extract_bundle_accounts(bundle: &SanitizedBundle) -> (Vec<Pubkey>, Vec<Pubkey>) {
        let mut write_set = HashSet::new();
        let mut read_set = HashSet::new();
        
        // Single pass through all transactions
        for tx in &bundle.transactions {
            let account_keys = tx.message().account_keys();
            for (idx, key) in account_keys.iter().enumerate() {
                if tx.message().is_writable(idx) {
                    write_set.insert(*key);
                    // Remove from read_set if it was there (write takes precedence)
                    read_set.remove(key);
                } else if !write_set.contains(key) {
                    read_set.insert(*key);
                }
            }
        }
        
        (
            write_set.into_iter().collect(),
            read_set.into_iter().collect(),
        )
    }
}
