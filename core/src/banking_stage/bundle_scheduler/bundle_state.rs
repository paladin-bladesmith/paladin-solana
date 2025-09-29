use crate::banking_stage::scheduler_messages::MaxAge;
#[cfg(feature = "dev-context-only-utils")]
use qualifier_attr::qualifiers;
use solana_bundle::SanitizedBundle;

/// Represents the key in the Slab for a bundle
pub(crate) type BundleId = usize;

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
}

impl BundleState {
    /// Creates a new `BundleState` in the `Unprocessed` state.
    pub(crate) fn new(bundle: SanitizedBundle, max_age: MaxAge, priority: u64, cost: u64) -> Self {
        Self {
            bundle: Some(bundle),
            max_age,
            priority,
            cost,
        }
    }

    /// Return the priority of the bundle.
    /// This is *not* the same as the `compute_unit_price` of the bundle.
    /// The priority is used to order bundles for processing.
    pub(crate) fn priority(&self) -> u64 {
        self.priority
    }

    /// Return the cost of the bundle.
    pub(crate) fn cost(&self) -> u64 {
        self.cost
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

    // When the bundle is retried or cost buffered
    /// Intended to be called when a bundle is retried. This method will
    /// put the bundle back into the state.
    /// # Panics
    /// This method will panic if the bundle is already in the state.
    pub(crate) fn retry_bundle(&mut self, bundle: SanitizedBundle) {
        assert!(self.bundle.replace(bundle).is_none(), "bundle already present");
    }

    /// Get a reference to the bundle.
    ///
    /// # Panics
    /// This method will panic if the bundle is in the `Pending` state.
    pub(crate) fn bundle(&self) -> &SanitizedBundle {
        self.bundle.as_ref().expect("bundle is pending")
    }
}