use {
    super::{
        consumer::{Consumer, ExecuteAndCommitTransactionsOutput, ProcessTransactionBatchOutput},
        leader_slot_timing_metrics::LeaderExecuteAndCommitTimings,
        scheduler_messages::{ConsumeWork, FinishedConsumeWork},
    },
    crate::{
        banking_stage::scheduler_messages::ConsumeWorkItem,
        bundle_stage::bundle_consumer::BundleConsumer,
    },
    crossbeam_channel::{Receiver, RecvError, SendError, Sender},
    solana_measure::measure_us,
    solana_poh::poh_recorder::SharedWorkingBank,
    solana_runtime::bank::Bank,
    solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    solana_time_utils::AtomicInterval,
    std::{
        sync::{
            atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    },
    thiserror::Error,
};

#[derive(Debug, Error)]
pub enum ConsumeWorkerError<Tx> {
    #[error("Failed to receive work from scheduler: {0}")]
    Recv(#[from] RecvError),
    #[error("Failed to send finalized consume work to scheduler: {0}")]
    Send(#[from] SendError<FinishedConsumeWork<Tx>>),
}

pub(crate) struct ConsumeWorker<Tx> {
    exit: Arc<AtomicBool>,
    consume_receiver: Receiver<ConsumeWork<Tx>>,
    consumer: Consumer,
    bundle_consumer: Option<BundleConsumer>,
    consumed_sender: Sender<FinishedConsumeWork<Tx>>,

    shared_working_bank: SharedWorkingBank,
    metrics: Arc<ConsumeWorkerMetrics>,
}

impl<Tx: TransactionWithMeta> ConsumeWorker<Tx> {
    pub fn new(
        id: u32,
        exit: Arc<AtomicBool>,
        consume_receiver: Receiver<ConsumeWork<Tx>>,
        consumer: Consumer,
        bundle_consumer: Option<BundleConsumer>,
        consumed_sender: Sender<FinishedConsumeWork<Tx>>,
        shared_working_bank: SharedWorkingBank,
    ) -> Self {
        Self {
            exit,
            consume_receiver,
            consumer,
            bundle_consumer,
            consumed_sender,
            shared_working_bank,
            metrics: Arc::new(ConsumeWorkerMetrics::new(id)),
        }
    }

    pub fn metrics_handle(&self) -> Arc<ConsumeWorkerMetrics> {
        self.metrics.clone()
    }

    pub fn run(
        mut self,
        reservation_cb: impl Fn(&Bank) -> u64,
    ) -> Result<(), ConsumeWorkerError<Tx>> {
        while !self.exit.load(Ordering::Relaxed) {
            let work = self.consume_receiver.recv()?;
            self.consume_loop(work, &reservation_cb)?;
        }
        Ok(())
    }

    fn consume_loop(
        &mut self,
        work: ConsumeWork<Tx>,
        reservation_cb: &impl Fn(&Bank) -> u64,
    ) -> Result<(), ConsumeWorkerError<Tx>> {
        let (maybe_consume_bank, get_bank_us) = measure_us!(self.working_bank_with_timeout());
        let Some(mut bank) = maybe_consume_bank else {
            self.metrics
                .timing_metrics
                .wait_for_bank_failure_us
                .fetch_add(get_bank_us, Ordering::Relaxed);
            return self.retry_drain(work);
        };
        self.metrics
            .timing_metrics
            .wait_for_bank_success_us
            .fetch_add(get_bank_us, Ordering::Relaxed);

        // Collect all work items first to avoid borrow checker issues
        let work_items: Vec<_> = try_drain_iter(work, &self.consume_receiver).collect();

        for work in work_items {
            self.metrics
                .count_metrics
                .max_queue_len
                .fetch_max(self.consume_receiver.len() as u64, Ordering::Relaxed);
            if self.exit.load(Ordering::Relaxed) {
                return Ok(());
            }
            if bank.is_complete() || {
                // if working bank has changed, then try to get a new bank.
                self.working_bank()
                    .map(|working_bank| Arc::ptr_eq(&working_bank, &bank))
                    .unwrap_or(true)
            } {
                let (maybe_new_bank, get_bank_us) = measure_us!(self.working_bank_with_timeout());
                if let Some(new_bank) = maybe_new_bank {
                    self.metrics
                        .timing_metrics
                        .wait_for_bank_success_us
                        .fetch_add(get_bank_us, Ordering::Relaxed);
                    bank = new_bank;
                } else {
                    self.metrics
                        .timing_metrics
                        .wait_for_bank_failure_us
                        .fetch_add(get_bank_us, Ordering::Relaxed);
                    return self.retry_drain(work);
                }
            }
            self.metrics
                .count_metrics
                .num_messages_processed
                .fetch_add(1, Ordering::Relaxed);
            self.consume(&bank, work, reservation_cb)?;
        }

        Ok(())
    }

    /// Consume a single batch of work items (transactions and/or bundles).
    ///
    /// Process items in priority order while batching consecutive transactions for efficiency.
    /// Example: [Tx1, Tx2, B1, Tx3, Tx4] → batch[Tx1,Tx2] → B1 → batch[Tx3,Tx4]
    fn consume(
        &mut self,
        bank: &Arc<Bank>,
        mut work: ConsumeWork<Tx>,
        reservation_cb: &impl Fn(&Bank) -> u64,
    ) -> Result<(), ConsumeWorkerError<Tx>> {
        let items = std::mem::take(&mut work.items);
        let mut retryable_transaction_indexes = Vec::new();
        let mut retryable_bundle_ids = Vec::new();

        let mut current_tx_batch = Vec::new();
        let mut current_tx_ids = Vec::new();
        let mut current_tx_max_ages = Vec::new();
        let mut processed_items = Vec::new();
        let mut tx_index_offset = 0;

        // Helper to flush accumulated transaction batch
        let flush_tx_batch = |batch: &mut Vec<Tx>,
                              ids: &mut Vec<_>,
                              max_ages: &mut Vec<_>,
                              processed: &mut Vec<_>,
                              retryable: &mut Vec<usize>,
                              offset: &mut usize,
                              consumer: &Consumer,
                              metrics: &ConsumeWorkerMetrics| {
            if batch.is_empty() {
                return;
            }

            let output = consumer.process_and_record_aged_transactions(
                bank,
                batch,
                max_ages,
                reservation_cb,
            );

            metrics.update_for_consume(&output);
            metrics.has_data.store(true, Ordering::Relaxed);

            // Map batch-relative retryable indexes to global indexes
            for batch_idx in output
                .execute_and_commit_transactions_output
                .retryable_transaction_indexes
            {
                retryable.push(*offset + batch_idx);
            }

            // Reconstruct items for return
            for ((id, tx), max_age) in ids.drain(..).zip(batch.drain(..)).zip(max_ages.drain(..)) {
                processed.push(ConsumeWorkItem::Transaction {
                    id,
                    transaction: tx,
                    max_age,
                });
            }

            *offset += processed.len();
        };

        // Process items in order, batching consecutive transactions
        for item in items {
            match item {
                ConsumeWorkItem::Transaction {
                    id,
                    transaction,
                    max_age,
                } => {
                    current_tx_batch.push(transaction);
                    current_tx_ids.push(id);
                    current_tx_max_ages.push(max_age);
                }
                ConsumeWorkItem::Bundle {
                    id,
                    bundle,
                    max_age,
                } => {
                    // Flush any pending transaction batch before processing bundle
                    flush_tx_batch(
                        &mut current_tx_batch,
                        &mut current_tx_ids,
                        &mut current_tx_max_ages,
                        &mut processed_items,
                        &mut retryable_transaction_indexes,
                        &mut tx_index_offset,
                        &self.consumer,
                        &self.metrics,
                    );

                    // Execute bundle - locks already held by ThreadAwareAccountLocks
                    if let Some(bundle_consumer) = &mut self.bundle_consumer {
                        match bundle_consumer.process_single_bundle(bank, &bundle, true) {
                            Ok(_) => {
                                // Bundle executed successfully
                            }
                            Err(_) => {
                                retryable_bundle_ids.push(id);
                            }
                        }
                    } else {
                        retryable_bundle_ids.push(id);
                    }

                    processed_items.push(ConsumeWorkItem::Bundle {
                        id,
                        bundle,
                        max_age,
                    });
                }
            }
        }

        // Flush any remaining transactions
        flush_tx_batch(
            &mut current_tx_batch,
            &mut current_tx_ids,
            &mut current_tx_max_ages,
            &mut processed_items,
            &mut retryable_transaction_indexes,
            &mut tx_index_offset,
            &self.consumer,
            &self.metrics,
        );

        work.items = processed_items;

        self.consumed_sender.send(FinishedConsumeWork {
            work,
            retryable_transaction_indexes,
            retryable_bundle_ids,
        })?;

        Ok(())
    }

    /// Get the current poh working bank with a timeout - if the Bank is
    /// not available within the timeout, return None.
    fn working_bank_with_timeout(&self) -> Option<Arc<Bank>> {
        const TIMEOUT: Duration = Duration::from_millis(50);
        let now = Instant::now();
        while now.elapsed() < TIMEOUT {
            if let Some(bank) = self.working_bank() {
                return Some(bank);
            }
        }

        None
    }

    /// Get the current poh working bank without a timeout.
    fn working_bank(&self) -> Option<Arc<Bank>> {
        self.shared_working_bank.load()
    }

    /// Retry current batch and all outstanding batches.
    fn retry_drain(&mut self, work: ConsumeWork<Tx>) -> Result<(), ConsumeWorkerError<Tx>> {
        // Collect all work items first to avoid borrow checker issues
        let work_items: Vec<_> = try_drain_iter(work, &self.consume_receiver).collect();

        for work in work_items {
            if self.exit.load(Ordering::Relaxed) {
                return Ok(());
            }
            self.retry(work)?;
        }
        Ok(())
    }

    /// Send transactions and bundles back to scheduler as retryable.
    fn retry(&self, work: ConsumeWork<Tx>) -> Result<(), ConsumeWorkerError<Tx>> {
        let mut num_transactions = 0;
        let mut retryable_bundle_ids = Vec::new();

        for item in &work.items {
            match item {
                ConsumeWorkItem::Transaction { .. } => num_transactions += 1,
                ConsumeWorkItem::Bundle { id, .. } => retryable_bundle_ids.push(*id),
            }
        }

        let retryable_indexes: Vec<_> = (0..num_transactions).collect();
        let num_retryable = retryable_indexes.len();
        self.metrics
            .count_metrics
            .retryable_transaction_count
            .fetch_add(num_retryable, Ordering::Relaxed);
        self.metrics
            .count_metrics
            .retryable_expired_bank_count
            .fetch_add(num_retryable, Ordering::Relaxed);
        self.metrics.has_data.store(true, Ordering::Relaxed);
        self.consumed_sender.send(FinishedConsumeWork {
            work,
            retryable_transaction_indexes: retryable_indexes,
            retryable_bundle_ids,
        })?;
        Ok(())
    }
}

/// Helper function to create an non-blocking iterator over work in the receiver,
/// starting with the given work item.
fn try_drain_iter<T>(work: T, receiver: &Receiver<T>) -> impl Iterator<Item = T> + '_ {
    std::iter::once(work).chain(receiver.try_iter())
}

/// Metrics tracking number of packets processed by the consume worker.
/// These are atomic, and intended to be reported by the scheduling thread
/// since the consume worker thread is sleeping unless there is work to be
/// done.
pub(crate) struct ConsumeWorkerMetrics {
    id: String,
    interval: AtomicInterval,
    has_data: AtomicBool,

    count_metrics: ConsumeWorkerCountMetrics,
    error_metrics: ConsumeWorkerTransactionErrorMetrics,
    timing_metrics: ConsumeWorkerTimingMetrics,
}

impl ConsumeWorkerMetrics {
    /// Report and reset metrics iff the interval has elapsed and the worker did some work.
    pub fn maybe_report_and_reset(&self) {
        const REPORT_INTERVAL_MS: u64 = 20;
        if self.interval.should_update(REPORT_INTERVAL_MS)
            && self.has_data.swap(false, Ordering::Relaxed)
        {
            self.count_metrics.report_and_reset(&self.id);
            self.timing_metrics.report_and_reset(&self.id);
            self.error_metrics.report_and_reset(&self.id);
        }
    }

    fn new(id: u32) -> Self {
        Self {
            id: id.to_string(),
            interval: AtomicInterval::default(),
            has_data: AtomicBool::new(false),
            count_metrics: ConsumeWorkerCountMetrics::default(),
            error_metrics: ConsumeWorkerTransactionErrorMetrics::default(),
            timing_metrics: ConsumeWorkerTimingMetrics::default(),
        }
    }

    fn update_for_consume(
        &self,
        ProcessTransactionBatchOutput {
            cost_model_throttled_transactions_count,
            cost_model_us,
            execute_and_commit_transactions_output,
        }: &ProcessTransactionBatchOutput,
    ) {
        self.count_metrics
            .cost_model_throttled_transactions_count
            .fetch_add(*cost_model_throttled_transactions_count, Ordering::Relaxed);
        self.timing_metrics
            .cost_model_us
            .fetch_add(*cost_model_us, Ordering::Relaxed);
        self.update_on_execute_and_commit_transactions_output(
            execute_and_commit_transactions_output,
        );
    }

    fn update_on_execute_and_commit_transactions_output(
        &self,
        ExecuteAndCommitTransactionsOutput {
            transaction_counts,
            retryable_transaction_indexes,
            execute_and_commit_timings,
            error_counters,
            min_prioritization_fees,
            max_prioritization_fees,
            ..
        }: &ExecuteAndCommitTransactionsOutput,
    ) {
        self.count_metrics
            .transactions_attempted_processing_count
            .fetch_add(
                transaction_counts.attempted_processing_count,
                Ordering::Relaxed,
            );
        self.count_metrics
            .processed_transactions_count
            .fetch_add(transaction_counts.processed_count, Ordering::Relaxed);
        self.count_metrics
            .processed_with_successful_result_count
            .fetch_add(
                transaction_counts.processed_with_successful_result_count,
                Ordering::Relaxed,
            );
        self.count_metrics
            .retryable_transaction_count
            .fetch_add(retryable_transaction_indexes.len(), Ordering::Relaxed);
        let min_prioritization_fees = self
            .count_metrics
            .min_prioritization_fees
            .fetch_min(*min_prioritization_fees, Ordering::Relaxed);
        let max_prioritization_fees = self
            .count_metrics
            .max_prioritization_fees
            .fetch_max(*max_prioritization_fees, Ordering::Relaxed);
        self.count_metrics
            .min_prioritization_fees
            .swap(min_prioritization_fees, Ordering::Relaxed);
        self.count_metrics
            .max_prioritization_fees
            .swap(max_prioritization_fees, Ordering::Relaxed);
        self.update_on_execute_and_commit_timings(execute_and_commit_timings);
        self.update_on_error_counters(error_counters);
    }

    fn update_on_execute_and_commit_timings(
        &self,
        LeaderExecuteAndCommitTimings {
            load_execute_us,
            freeze_lock_us,
            record_us,
            commit_us,
            find_and_send_votes_us,
            ..
        }: &LeaderExecuteAndCommitTimings,
    ) {
        self.timing_metrics
            .load_execute_us_min
            .fetch_min(*load_execute_us, Ordering::Relaxed);
        self.timing_metrics
            .load_execute_us_max
            .fetch_max(*load_execute_us, Ordering::Relaxed);
        self.timing_metrics
            .load_execute_us
            .fetch_add(*load_execute_us, Ordering::Relaxed);
        self.timing_metrics
            .freeze_lock_us
            .fetch_add(*freeze_lock_us, Ordering::Relaxed);
        self.timing_metrics
            .record_us
            .fetch_add(*record_us, Ordering::Relaxed);
        self.timing_metrics
            .commit_us
            .fetch_add(*commit_us, Ordering::Relaxed);
        self.timing_metrics
            .find_and_send_votes_us
            .fetch_add(*find_and_send_votes_us, Ordering::Relaxed);
        self.timing_metrics
            .num_batches_processed
            .fetch_add(1, Ordering::Relaxed);
    }

    fn update_on_error_counters(
        &self,
        TransactionErrorMetrics {
            total,
            account_in_use,
            too_many_account_locks,
            account_loaded_twice,
            account_not_found,
            blockhash_not_found,
            blockhash_too_old,
            call_chain_too_deep,
            already_processed,
            instruction_error,
            insufficient_funds,
            invalid_account_for_fee,
            invalid_account_index,
            invalid_program_for_execution,
            invalid_compute_budget,
            not_allowed_during_cluster_maintenance,
            invalid_writable_account,
            invalid_rent_paying_account,
            would_exceed_max_block_cost_limit,
            would_exceed_max_account_cost_limit,
            would_exceed_max_vote_cost_limit,
            would_exceed_account_data_block_limit,
            max_loaded_accounts_data_size_exceeded,
            program_execution_temporarily_restricted,
        }: &TransactionErrorMetrics,
    ) {
        self.error_metrics
            .total
            .fetch_add(total.0, Ordering::Relaxed);
        self.error_metrics
            .account_in_use
            .fetch_add(account_in_use.0, Ordering::Relaxed);
        self.error_metrics
            .too_many_account_locks
            .fetch_add(too_many_account_locks.0, Ordering::Relaxed);
        self.error_metrics
            .account_loaded_twice
            .fetch_add(account_loaded_twice.0, Ordering::Relaxed);
        self.error_metrics
            .account_not_found
            .fetch_add(account_not_found.0, Ordering::Relaxed);
        self.error_metrics
            .blockhash_not_found
            .fetch_add(blockhash_not_found.0, Ordering::Relaxed);
        self.error_metrics
            .blockhash_too_old
            .fetch_add(blockhash_too_old.0, Ordering::Relaxed);
        self.error_metrics
            .call_chain_too_deep
            .fetch_add(call_chain_too_deep.0, Ordering::Relaxed);
        self.error_metrics
            .already_processed
            .fetch_add(already_processed.0, Ordering::Relaxed);
        self.error_metrics
            .instruction_error
            .fetch_add(instruction_error.0, Ordering::Relaxed);
        self.error_metrics
            .insufficient_funds
            .fetch_add(insufficient_funds.0, Ordering::Relaxed);
        self.error_metrics
            .invalid_account_for_fee
            .fetch_add(invalid_account_for_fee.0, Ordering::Relaxed);
        self.error_metrics
            .invalid_account_index
            .fetch_add(invalid_account_index.0, Ordering::Relaxed);
        self.error_metrics
            .invalid_program_for_execution
            .fetch_add(invalid_program_for_execution.0, Ordering::Relaxed);
        self.error_metrics
            .invalid_compute_budget
            .fetch_add(invalid_compute_budget.0, Ordering::Relaxed);
        self.error_metrics
            .not_allowed_during_cluster_maintenance
            .fetch_add(not_allowed_during_cluster_maintenance.0, Ordering::Relaxed);
        self.error_metrics
            .invalid_writable_account
            .fetch_add(invalid_writable_account.0, Ordering::Relaxed);
        self.error_metrics
            .invalid_rent_paying_account
            .fetch_add(invalid_rent_paying_account.0, Ordering::Relaxed);
        self.error_metrics
            .would_exceed_max_block_cost_limit
            .fetch_add(would_exceed_max_block_cost_limit.0, Ordering::Relaxed);
        self.error_metrics
            .would_exceed_max_account_cost_limit
            .fetch_add(would_exceed_max_account_cost_limit.0, Ordering::Relaxed);
        self.error_metrics
            .would_exceed_max_vote_cost_limit
            .fetch_add(would_exceed_max_vote_cost_limit.0, Ordering::Relaxed);
        self.error_metrics
            .would_exceed_account_data_block_limit
            .fetch_add(would_exceed_account_data_block_limit.0, Ordering::Relaxed);
        self.error_metrics
            .max_loaded_accounts_data_size_exceeded
            .fetch_add(max_loaded_accounts_data_size_exceeded.0, Ordering::Relaxed);
        self.error_metrics
            .program_execution_temporarily_restricted
            .fetch_add(
                program_execution_temporarily_restricted.0,
                Ordering::Relaxed,
            );
    }
}

struct ConsumeWorkerCountMetrics {
    max_queue_len: AtomicU64,
    num_messages_processed: AtomicU64,
    transactions_attempted_processing_count: AtomicU64,
    processed_transactions_count: AtomicU64,
    processed_with_successful_result_count: AtomicU64,
    retryable_transaction_count: AtomicUsize,
    retryable_expired_bank_count: AtomicUsize,
    cost_model_throttled_transactions_count: AtomicU64,
    min_prioritization_fees: AtomicU64,
    max_prioritization_fees: AtomicU64,
}

impl Default for ConsumeWorkerCountMetrics {
    fn default() -> Self {
        Self {
            max_queue_len: AtomicU64::default(),
            num_messages_processed: AtomicU64::default(),
            transactions_attempted_processing_count: AtomicU64::default(),
            processed_transactions_count: AtomicU64::default(),
            processed_with_successful_result_count: AtomicU64::default(),
            retryable_transaction_count: AtomicUsize::default(),
            retryable_expired_bank_count: AtomicUsize::default(),
            cost_model_throttled_transactions_count: AtomicU64::default(),
            min_prioritization_fees: AtomicU64::new(u64::MAX),
            max_prioritization_fees: AtomicU64::default(),
        }
    }
}

impl ConsumeWorkerCountMetrics {
    fn report_and_reset(&self, id: &str) {
        datapoint_info!(
            "banking_stage_worker_counts",
            "id" => id,
            ("max_queue_len", self.max_queue_len.swap(0, Ordering::Relaxed), i64),
            (
                "num_messages_processed",
                self.num_messages_processed.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "transactions_attempted_processing_count",
                self.transactions_attempted_processing_count
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "processed_transactions_count",
                self.processed_transactions_count.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "processed_with_successful_result_count",
                self.processed_with_successful_result_count
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "retryable_transaction_count",
                self.retryable_transaction_count.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "retryable_expired_bank_count",
                self.retryable_expired_bank_count.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "cost_model_throttled_transactions_count",
                self.cost_model_throttled_transactions_count
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "min_prioritization_fees",
                self.min_prioritization_fees
                    .swap(u64::MAX, Ordering::Relaxed),
                i64
            ),
            (
                "max_prioritization_fees",
                self.max_prioritization_fees.swap(0, Ordering::Relaxed),
                i64
            ),
        );
    }
}

#[derive(Default)]
struct ConsumeWorkerTimingMetrics {
    cost_model_us: AtomicU64,
    load_execute_us: AtomicU64,
    load_execute_us_min: AtomicU64,
    load_execute_us_max: AtomicU64,
    freeze_lock_us: AtomicU64,
    record_us: AtomicU64,
    commit_us: AtomicU64,
    find_and_send_votes_us: AtomicU64,
    wait_for_bank_success_us: AtomicU64,
    wait_for_bank_failure_us: AtomicU64,
    num_batches_processed: AtomicU64,
}

impl ConsumeWorkerTimingMetrics {
    fn report_and_reset(&self, id: &str) {
        datapoint_info!(
            "banking_stage_worker_timing",
            "id" => id,
            (
                "cost_model_us",
                self.cost_model_us.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "load_execute_us",
                self.load_execute_us.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "load_execute_us_min",
                self.load_execute_us_min.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "load_execute_us_max",
                self.load_execute_us_max.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "num_batches_processed",
                self.num_batches_processed.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "freeze_lock_us",
                self.freeze_lock_us.swap(0, Ordering::Relaxed),
                i64
            ),
            ("record_us", self.record_us.swap(0, Ordering::Relaxed), i64),
            ("commit_us", self.commit_us.swap(0, Ordering::Relaxed), i64),
            (
                "find_and_send_votes_us",
                self.find_and_send_votes_us.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "wait_for_bank_success_us",
                self.wait_for_bank_success_us.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "wait_for_bank_failure_us",
                self.wait_for_bank_failure_us.swap(0, Ordering::Relaxed),
                i64
            ),
        );
    }
}

#[derive(Default)]
struct ConsumeWorkerTransactionErrorMetrics {
    total: AtomicUsize,
    account_in_use: AtomicUsize,
    too_many_account_locks: AtomicUsize,
    account_loaded_twice: AtomicUsize,
    account_not_found: AtomicUsize,
    blockhash_not_found: AtomicUsize,
    blockhash_too_old: AtomicUsize,
    call_chain_too_deep: AtomicUsize,
    already_processed: AtomicUsize,
    instruction_error: AtomicUsize,
    insufficient_funds: AtomicUsize,
    invalid_account_for_fee: AtomicUsize,
    invalid_account_index: AtomicUsize,
    invalid_program_for_execution: AtomicUsize,
    invalid_compute_budget: AtomicUsize,
    not_allowed_during_cluster_maintenance: AtomicUsize,
    invalid_writable_account: AtomicUsize,
    invalid_rent_paying_account: AtomicUsize,
    would_exceed_max_block_cost_limit: AtomicUsize,
    would_exceed_max_account_cost_limit: AtomicUsize,
    would_exceed_max_vote_cost_limit: AtomicUsize,
    would_exceed_account_data_block_limit: AtomicUsize,
    max_loaded_accounts_data_size_exceeded: AtomicUsize,
    program_execution_temporarily_restricted: AtomicUsize,
}

impl ConsumeWorkerTransactionErrorMetrics {
    fn report_and_reset(&self, id: &str) {
        datapoint_info!(
            "banking_stage_worker_error_metrics",
            "id" => id,
            ("total", self.total.swap(0, Ordering::Relaxed), i64),
            (
                "account_in_use",
                self.account_in_use.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "too_many_account_locks",
                self.too_many_account_locks.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "account_loaded_twice",
                self.account_loaded_twice.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "account_not_found",
                self.account_not_found.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "blockhash_not_found",
                self.blockhash_not_found.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "blockhash_too_old",
                self.blockhash_too_old.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "call_chain_too_deep",
                self.call_chain_too_deep.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "already_processed",
                self.already_processed.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "instruction_error",
                self.instruction_error.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "insufficient_funds",
                self.insufficient_funds.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "invalid_account_for_fee",
                self.invalid_account_for_fee.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "invalid_account_index",
                self.invalid_account_index.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "invalid_program_for_execution",
                self.invalid_program_for_execution
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "invalid_compute_budget",
                self.invalid_compute_budget
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "not_allowed_during_cluster_maintenance",
                self.not_allowed_during_cluster_maintenance
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "invalid_writable_account",
                self.invalid_writable_account.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "invalid_rent_paying_account",
                self.invalid_rent_paying_account.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "would_exceed_max_block_cost_limit",
                self.would_exceed_max_block_cost_limit
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "would_exceed_max_account_cost_limit",
                self.would_exceed_max_account_cost_limit
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "would_exceed_max_vote_cost_limit",
                self.would_exceed_max_vote_cost_limit
                    .swap(0, Ordering::Relaxed),
                i64
            ),
        );
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            banking_stage::{
                committer::Committer,
                qos_service::QosService,
                scheduler_messages::{MaxAge, TransactionBatchId},
                tests::{create_slow_genesis_config, sanitize_transactions, simulate_poh},
            },
            bundle_stage::bundle_account_locker::BundleAccountLocker,
        },
        crossbeam_channel::unbounded,
        solana_clock::{Slot, MAX_PROCESSING_AGE},
        solana_genesis_config::GenesisConfig,
        solana_keypair::Keypair,
        solana_ledger::{
            blockstore::Blockstore, genesis_utils::GenesisConfigInfo,
            get_tmp_ledger_path_auto_delete, leader_schedule_cache::LeaderScheduleCache,
        },
        solana_message::{
            v0::{self, LoadedAddresses},
            AddressLookupTableAccount, SimpleAddressLoader, VersionedMessage,
        },
        solana_poh::{
            poh_recorder::{PohRecorder, WorkingBankEntry},
            transaction_recorder::TransactionRecorder,
        },
        solana_poh_config::PohConfig,
        solana_pubkey::Pubkey,
        solana_runtime::{
            bank_forks::BankForks, prioritization_fee_cache::PrioritizationFeeCache,
            vote_sender_types::ReplayVoteReceiver,
        },
        solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
        solana_signer::Signer,
        solana_svm_transaction::svm_message::SVMMessage,
        solana_system_interface::instruction as system_instruction,
        solana_system_transaction as system_transaction,
        solana_transaction::{
            sanitized::{MessageHash, SanitizedTransaction},
            versioned::VersionedTransaction,
        },
        solana_transaction_error::TransactionError,
        std::{
            collections::HashSet,
            sync::{atomic::AtomicBool, RwLock},
            thread::JoinHandle,
        },
        tempfile::TempDir,
        test_case::test_case,
    };

    // Helper struct to create tests that hold channels, files, etc.
    // such that our tests can be more easily set up and run.
    struct TestFrame {
        mint_keypair: Keypair,
        genesis_config: GenesisConfig,
        bank: Arc<Bank>,
        _bank_forks: Arc<RwLock<BankForks>>,
        _ledger_path: TempDir,
        _entry_receiver: Receiver<WorkingBankEntry>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        _poh_simulator: JoinHandle<()>,
        _replay_vote_receiver: ReplayVoteReceiver,

        consume_sender: Sender<ConsumeWork<RuntimeTransaction<SanitizedTransaction>>>,
        consumed_receiver: Receiver<FinishedConsumeWork<RuntimeTransaction<SanitizedTransaction>>>,
    }

    fn setup_test_frame(
        relax_intrabatch_account_locks: bool,
    ) -> (
        TestFrame,
        ConsumeWorker<RuntimeTransaction<SanitizedTransaction>>,
    ) {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(10_000);
        let (bank, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
        // Warp to next epoch for MaxAge tests.
        let mut bank = Bank::new_from_parent(
            bank.clone(),
            &Pubkey::new_unique(),
            bank.get_epoch_info().slots_in_epoch,
        );
        if !relax_intrabatch_account_locks {
            bank.deactivate_feature(&agave_feature_set::relax_intrabatch_account_locks::id());
        }
        let bank = Arc::new(bank);

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let (poh_recorder, entry_receiver) = PohRecorder::new(
            bank.tick_height(),
            bank.last_blockhash(),
            bank.clone(),
            Some((4, 4)),
            bank.ticks_per_slot(),
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );
        let (record_sender, record_receiver) = unbounded();
        let recorder = TransactionRecorder::new(record_sender, poh_recorder.is_exited.clone());
        let poh_recorder = Arc::new(RwLock::new(poh_recorder));
        let poh_simulator = simulate_poh(record_receiver, &poh_recorder);

        let (replay_vote_sender, replay_vote_receiver) = unbounded();
        let committer = Committer::new(
            None,
            replay_vote_sender,
            Arc::new(PrioritizationFeeCache::new(0u64)),
        );
        let consumer = Consumer::new(
            committer,
            recorder,
            QosService::new(1),
            None,
            BundleAccountLocker::default(),
        );

        let (consume_sender, consume_receiver) = unbounded();
        let (consumed_sender, consumed_receiver) = unbounded();
        let worker = ConsumeWorker::new(
            0,
            Arc::new(AtomicBool::new(false)),
            consume_receiver,
            consumer,
            None, // No bundle consumer for tests
            consumed_sender,
            poh_recorder.read().unwrap().shared_working_bank(),
        );

        (
            TestFrame {
                mint_keypair,
                genesis_config,
                bank,
                _bank_forks: bank_forks,
                _ledger_path: ledger_path,
                _entry_receiver: entry_receiver,
                poh_recorder,
                _poh_simulator: poh_simulator,
                _replay_vote_receiver: replay_vote_receiver,
                consume_sender,
                consumed_receiver,
            },
            worker,
        )
    }

    #[test]
    fn test_worker_consume_no_bank() {
        let (test_frame, worker) = setup_test_frame(true);
        let TestFrame {
            mint_keypair,
            genesis_config,
            bank,
            consume_sender,
            consumed_receiver,
            ..
        } = &test_frame;
        let worker_thread = std::thread::spawn(move || worker.run(|_| 0));

        let pubkey1 = Pubkey::new_unique();

        let transactions = sanitize_transactions(vec![system_transaction::transfer(
            mint_keypair,
            &pubkey1,
            1,
            genesis_config.hash(),
        )]);
        let bid = TransactionBatchId::new(0);
        let id = 0;
        let max_age = MaxAge {
            sanitized_epoch: bank.epoch(),
            alt_invalidation_slot: bank.slot(),
        };
        let work = ConsumeWork {
            batch_id: bid,
            items: transactions
                .into_iter()
                .map(|tx| ConsumeWorkItem::Transaction {
                    id,
                    transaction: tx,
                    max_age,
                })
                .collect(),
        };
        consume_sender.send(work).unwrap();
        let consumed = consumed_receiver.recv().unwrap();
        assert_eq!(consumed.work.batch_id, bid);
        assert_eq!(consumed.work.items.len(), 1);
        assert_eq!(consumed.retryable_transaction_indexes, vec![0]);

        drop(test_frame);
        let _ = worker_thread.join().unwrap();
    }

    #[test]
    fn test_worker_consume_simple() {
        let (test_frame, worker) = setup_test_frame(true);
        let TestFrame {
            mint_keypair,
            genesis_config,
            bank,
            poh_recorder,
            consume_sender,
            consumed_receiver,
            ..
        } = &test_frame;
        let worker_thread = std::thread::spawn(move || worker.run(|_| 0));
        poh_recorder
            .write()
            .unwrap()
            .set_bank_for_test(bank.clone());

        let pubkey1 = Pubkey::new_unique();

        let transactions = sanitize_transactions(vec![system_transaction::transfer(
            mint_keypair,
            &pubkey1,
            1,
            genesis_config.hash(),
        )]);
        let bid = TransactionBatchId::new(0);
        let id = 0;
        let max_age = MaxAge {
            sanitized_epoch: bank.epoch(),
            alt_invalidation_slot: bank.slot(),
        };
        let work = ConsumeWork {
            batch_id: bid,
            items: transactions
                .into_iter()
                .map(|tx| ConsumeWorkItem::Transaction {
                    id,
                    transaction: tx,
                    max_age,
                })
                .collect(),
        };
        consume_sender.send(work).unwrap();
        let consumed = consumed_receiver.recv().unwrap();
        assert_eq!(consumed.work.batch_id, bid);
        assert_eq!(consumed.work.items.len(), 1);
        assert_eq!(consumed.retryable_transaction_indexes, Vec::<usize>::new());

        drop(test_frame);
        let _ = worker_thread.join().unwrap();
    }

    #[test_case(false; "old")]
    #[test_case(true; "simd83")]
    fn test_worker_consume_self_conflicting(relax_intrabatch_account_locks: bool) {
        let (test_frame, worker) = setup_test_frame(relax_intrabatch_account_locks);
        let TestFrame {
            mint_keypair,
            genesis_config,
            bank,
            poh_recorder,
            consume_sender,
            consumed_receiver,
            ..
        } = &test_frame;
        let worker_thread = std::thread::spawn(move || worker.run(|_| 0));
        poh_recorder
            .write()
            .unwrap()
            .set_bank_for_test(bank.clone());

        let pubkey1 = Pubkey::new_unique();
        let pubkey2 = Pubkey::new_unique();

        let txs = sanitize_transactions(vec![
            system_transaction::transfer(mint_keypair, &pubkey1, 2, genesis_config.hash()),
            system_transaction::transfer(mint_keypair, &pubkey2, 2, genesis_config.hash()),
        ]);

        let bid = TransactionBatchId::new(0);
        let id1 = 1;
        let id2 = 0;
        let max_age = MaxAge {
            sanitized_epoch: bank.epoch(),
            alt_invalidation_slot: bank.slot(),
        };
        consume_sender
            .send(ConsumeWork {
                batch_id: bid,
                items: vec![
                    ConsumeWorkItem::Transaction {
                        id: id1,
                        transaction: txs[0].clone(),
                        max_age,
                    },
                    ConsumeWorkItem::Transaction {
                        id: id2,
                        transaction: txs[1].clone(),
                        max_age,
                    },
                ],
            })
            .unwrap();

        let consumed = consumed_receiver.recv().unwrap();
        assert_eq!(consumed.work.batch_id, bid);
        assert_eq!(consumed.work.items.len(), 2);

        // id2 succeeds with simd83, or is retryable due to lock conflict without simd83
        assert_eq!(
            consumed.retryable_transaction_indexes,
            if relax_intrabatch_account_locks {
                vec![]
            } else {
                vec![1]
            }
        );

        drop(test_frame);
        let _ = worker_thread.join().unwrap();
    }

    #[test]
    fn test_worker_consume_multiple_messages() {
        let (test_frame, worker) = setup_test_frame(true);
        let TestFrame {
            mint_keypair,
            genesis_config,
            bank,
            poh_recorder,
            consume_sender,
            consumed_receiver,
            ..
        } = &test_frame;
        let worker_thread = std::thread::spawn(move || worker.run(|_| 0));
        poh_recorder
            .write()
            .unwrap()
            .set_bank_for_test(bank.clone());

        let pubkey1 = Pubkey::new_unique();
        let pubkey2 = Pubkey::new_unique();

        let txs1 = sanitize_transactions(vec![system_transaction::transfer(
            mint_keypair,
            &pubkey1,
            2,
            genesis_config.hash(),
        )]);
        let txs2 = sanitize_transactions(vec![system_transaction::transfer(
            mint_keypair,
            &pubkey2,
            2,
            genesis_config.hash(),
        )]);

        let bid1 = TransactionBatchId::new(0);
        let bid2 = TransactionBatchId::new(1);
        let id1 = 1;
        let id2 = 0;
        let max_age = MaxAge {
            sanitized_epoch: bank.epoch(),
            alt_invalidation_slot: bank.slot(),
        };
        consume_sender
            .send(ConsumeWork {
                batch_id: bid1,
                items: txs1
                    .into_iter()
                    .map(|tx| ConsumeWorkItem::Transaction {
                        id: id1,
                        transaction: tx,
                        max_age,
                    })
                    .collect(),
            })
            .unwrap();

        consume_sender
            .send(ConsumeWork {
                batch_id: bid2,
                items: txs2
                    .into_iter()
                    .map(|tx| ConsumeWorkItem::Transaction {
                        id: id2,
                        transaction: tx,
                        max_age,
                    })
                    .collect(),
            })
            .unwrap();
        let consumed = consumed_receiver.recv().unwrap();
        assert_eq!(consumed.work.batch_id, bid1);
        assert_eq!(consumed.work.items.len(), 1);
        assert_eq!(consumed.retryable_transaction_indexes, Vec::<usize>::new());

        let consumed = consumed_receiver.recv().unwrap();
        assert_eq!(consumed.work.batch_id, bid2);
        assert_eq!(consumed.work.items.len(), 1);
        assert_eq!(consumed.retryable_transaction_indexes, Vec::<usize>::new());

        drop(test_frame);
        let _ = worker_thread.join().unwrap();
    }

    #[test]
    fn test_worker_ttl() {
        let (test_frame, worker) = setup_test_frame(true);
        let TestFrame {
            mint_keypair,
            genesis_config,
            bank,
            poh_recorder,
            consume_sender,
            consumed_receiver,
            ..
        } = &test_frame;
        let worker_thread = std::thread::spawn(move || worker.run(|_| 0));
        poh_recorder
            .write()
            .unwrap()
            .set_bank_for_test(bank.clone());
        assert!(bank.slot() > 0);
        assert!(bank.epoch() > 0);

        // No conflicts between transactions. Test 6 cases.
        // 1. Epoch expiration, before slot => still succeeds due to resanitizing
        // 2. Epoch expiration, on slot => succeeds normally
        // 3. Epoch expiration, after slot => succeeds normally
        // 4. ALT expiration, before slot => fails
        // 5. ALT expiration, on slot => succeeds normally
        // 6. ALT expiration, after slot => succeeds normally
        let simple_transfer = || {
            system_transaction::transfer(
                &Keypair::new(),
                &Pubkey::new_unique(),
                1,
                genesis_config.hash(),
            )
        };
        let simple_v0_transfer = || {
            let payer = Keypair::new();
            let to_pubkey = Pubkey::new_unique();
            let loaded_addresses = LoadedAddresses {
                writable: vec![to_pubkey],
                readonly: vec![],
            };
            let loader = SimpleAddressLoader::Enabled(loaded_addresses);
            RuntimeTransaction::try_create(
                VersionedTransaction::try_new(
                    VersionedMessage::V0(
                        v0::Message::try_compile(
                            &payer.pubkey(),
                            &[system_instruction::transfer(&payer.pubkey(), &to_pubkey, 1)],
                            &[AddressLookupTableAccount {
                                key: Pubkey::new_unique(), // will fail if using **bank** to lookup
                                addresses: vec![to_pubkey],
                            }],
                            genesis_config.hash(),
                        )
                        .unwrap(),
                    ),
                    &[&payer],
                )
                .unwrap(),
                MessageHash::Compute,
                None,
                loader,
                &HashSet::default(),
            )
            .unwrap()
        };

        let mut txs = sanitize_transactions(vec![
            simple_transfer(),
            simple_transfer(),
            simple_transfer(),
        ]);
        txs.push(simple_v0_transfer());
        txs.push(simple_v0_transfer());
        txs.push(simple_v0_transfer());
        let sanitized_txs = txs.clone();

        // Fund the keypairs.
        for tx in &txs {
            bank.process_transaction(&system_transaction::transfer(
                mint_keypair,
                &tx.account_keys()[0],
                2,
                genesis_config.hash(),
            ))
            .unwrap();
        }

        let max_ages = vec![
            MaxAge {
                sanitized_epoch: bank.epoch() - 1,
                alt_invalidation_slot: Slot::MAX,
            },
            MaxAge {
                sanitized_epoch: bank.epoch(),
                alt_invalidation_slot: Slot::MAX,
            },
            MaxAge {
                sanitized_epoch: bank.epoch() + 1,
                alt_invalidation_slot: Slot::MAX,
            },
            MaxAge {
                sanitized_epoch: bank.epoch(),
                alt_invalidation_slot: bank.slot() - 1,
            },
            MaxAge {
                sanitized_epoch: bank.epoch(),
                alt_invalidation_slot: bank.slot(),
            },
            MaxAge {
                sanitized_epoch: bank.epoch(),
                alt_invalidation_slot: bank.slot() + 1,
            },
        ];
        consume_sender
            .send(ConsumeWork {
                batch_id: TransactionBatchId::new(1),
                items: txs
                    .into_iter()
                    .enumerate()
                    .map(|(i, tx)| ConsumeWorkItem::Transaction {
                        id: i,
                        transaction: tx,
                        max_age: max_ages[i],
                    })
                    .collect(),
            })
            .unwrap();

        let consumed = consumed_receiver.recv().unwrap();
        assert_eq!(consumed.retryable_transaction_indexes, Vec::<usize>::new());
        // all but one succeed. 6 for initial funding
        assert_eq!(bank.transaction_count(), 6 + 5);

        let already_processed_results = bank
            .check_transactions(
                &sanitized_txs,
                &vec![Ok(()); sanitized_txs.len()],
                MAX_PROCESSING_AGE,
                &mut TransactionErrorMetrics::default(),
            )
            .into_iter()
            .map(|r| match r {
                Ok(_) => Ok(()),
                Err(err) => Err(err),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            already_processed_results,
            vec![
                Err(TransactionError::AlreadyProcessed),
                Err(TransactionError::AlreadyProcessed),
                Err(TransactionError::AlreadyProcessed),
                Ok(()), // <--- this transaction was not processed
                Err(TransactionError::AlreadyProcessed),
                Err(TransactionError::AlreadyProcessed)
            ]
        );

        drop(test_frame);
        let _ = worker_thread.join().unwrap();
    }
}
