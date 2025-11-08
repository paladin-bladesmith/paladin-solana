#[cfg(feature = "dev-context-only-utils")]
use qualifier_attr::qualifiers;
use {
    super::{
        scheduler_common::SchedulingCommon, scheduler_error::SchedulerError,
        transaction_state::TransactionState, unified_state_container::StateContainer,
    },
    solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
    std::num::Saturating,
};

#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
pub(crate) trait Scheduler<Tx: TransactionWithMeta> {
    /// Schedule transactions from `container`.
    /// pre-graph and pre-lock filters may be passed to be applied
    /// before specific actions internally.
    fn schedule<S: StateContainer<Tx>>(
        &mut self,
        container: &mut S,
        pre_graph_filter: impl Fn(&[&Tx], &mut [bool]),
        pre_lock_filter: impl Fn(&TransactionState<Tx>) -> PreLockFilterAction,
    ) -> Result<SchedulingSummary, SchedulerError>;

    /// Receive completed batches of transactions without blocking.
    /// Returns (num_transactions, num_retryable_transactions) on success.
    fn receive_completed(
        &mut self,
        container: &mut impl StateContainer<Tx>,
    ) -> Result<(usize, usize), SchedulerError> {
        let mut total_num_transactions = Saturating::<usize>(0);
        let mut total_num_retryable = Saturating::<usize>(0);
        loop {
            let (num_transactions, num_retryable) = self
                .scheduling_common_mut()
                .try_receive_completed(container)?;
            if num_transactions == 0 {
                break;
            }
            total_num_transactions += num_transactions;
            total_num_retryable += num_retryable;
        }
        let Saturating(total_num_transactions) = total_num_transactions;
        let Saturating(total_num_retryable) = total_num_retryable;
        Ok((total_num_transactions, total_num_retryable))
    }

    // Receive completed bundles wothout blocking
    fn receive_bundles(
        &mut self,
        container: &mut impl StateContainer<Tx>,
    ) -> Result<(usize,usize),SchedulerError>{
        let mut total_bundles = Saturating::<usize>(0);
        let mut total_retryable = Saturating::<usize>(0);
        loop{
            let (num_bundles, num_retryale) = self.scheduling_common_mut().receive_bundles(container)?;
            if num_bundles==0{
                break;
            }
            total_bundles+=num_bundles;
            total_retryable+=num_retryale;
        }
        let Saturating(total_bundles) = total_bundles;
        let Saturating(total_retryable) = total_retryable;
        Ok((total_bundles, total_retryable))
    }

    /// Flush any deferred retryable bundles into the container queue.
    /// Default implementation forwards to the shared `SchedulingCommon`.
    fn flush_deferred_bundles(
        &mut self,
        container: &mut impl StateContainer<Tx>,
    ) -> Result<usize, SchedulerError> {
        Ok(self.scheduling_common_mut().flush_deferred_bundles(container))
    }

    /// All schedulers should have access to the common context for shared
    /// implementation.
    fn scheduling_common_mut(&mut self) -> &mut SchedulingCommon<Tx>;
}

/// Action to be taken by pre-lock filter.
#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
pub(crate) enum PreLockFilterAction {
    /// Attempt to schedule the transaction.
    AttemptToSchedule,
}

/// Metrics from scheduling transactions.
#[derive(Default, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
pub(crate) struct SchedulingSummary {
    /// Starting queue size
    pub starting_queue_size: usize,
    /// Starting buffer size (outstanding txs are not counted in queue)
    pub starting_buffer_size: usize,

    /// Number of transactions scheduled.
    pub num_scheduled: usize,
    /// Number of transactions that were not scheduled due to conflicts.
    pub num_unschedulable_conflicts: usize,
    /// Number of transactions that were skipped due to thread capacity.
    pub num_unschedulable_threads: usize,
    /// Number of transactions that were dropped due to filter.
    pub num_filtered_out: usize,
    /// Time spent filtering transactions
    pub filter_time_us: u64,
}
