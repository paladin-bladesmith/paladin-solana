// pub trait UnifiedState {
//     pub(crate) fn new(bundle: SanitizedBundle, max_age: MaxAge, priority: u64, cost: u64) -> UnifiedState;
//     pub(crate) fn priority(&self) -> u64;
//     pub(crate) fn cost(&self) -> u64;
//     pub(crate) fn take_bundle_for_consuming(&mut self) -> (SanitizedBundle, MaxAge);
//     pub(crate) fn retry_bundle(&mut self, bundle: SanitizedBundle);

//     /// Get a reference to the bundle.
//     ///
//     /// # Panics
//     /// This method will panic if the bundle is in the `Pending` state.
//     pub(crate) fn bundle(&self) -> &SanitizedBundle {
//         self.bundle.as_ref().expect("bundle is pending")
//     }
