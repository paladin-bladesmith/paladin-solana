#[cfg(feature = "dev-context-only-utils")]
use qualifier_attr::qualifiers;
use solana_perf::packet::PacketBatch;

use {
    super::{
        unified_priority_id::UnifiedPriorityId,
        unified_scheduling_unit::UnifiedSchedulingUnit,
        transaction_state::TransactionState,
        unified_state_container::{
            SharedBytes, StateContainer, TransactionViewState, TransactionViewStateContainer, UnifiedStateContainer,
            EXTRA_CAPACITY,
        },
    },
    crate::banking_stage::{
        consumer::Consumer, decision_maker::BufferedPacketsDecision,
        immutable_deserialized_packet::ImmutableDeserializedPacket,
        packet_deserializer::PacketDeserializer, scheduler_messages::MaxAge,
    },
    crate::bundle_stage::{
        bundle_packet_receiver::BundleReceiver,
        bundle_packet_deserializer::ReceiveBundleResults,
    },
    crate::immutable_deserialized_bundle::ImmutableDeserializedBundle,
    agave_banking_stage_ingress_types::{BankingPacketBatch, BankingPacketReceiver},
    agave_transaction_view::{
        resolved_transaction_view::ResolvedTransactionView,
        transaction_version::TransactionVersion, transaction_view::SanitizedTransactionView,
    },
    arrayvec::ArrayVec,
    arrayref::array_ref,
    core::time::Duration,
    crossbeam_channel::{RecvTimeoutError, TryRecvError},
    solana_accounts_db::account_locks::validate_account_locks,
    solana_address_lookup_table_interface::state::estimate_last_valid_slot,
    solana_clock::{Epoch, Slot, MAX_PROCESSING_AGE},
    solana_cost_model::cost_model::CostModel,
    solana_fee_structure::FeeBudgetLimits,
    solana_measure::measure_us,
    solana_message::compiled_instruction::CompiledInstruction,
    solana_pubkey::Pubkey,
    solana_runtime::{bank::Bank, bank_forks::BankForks},
    solana_runtime_transaction::{
        runtime_transaction::RuntimeTransaction, transaction_meta::StaticMeta,
        transaction_with_meta::TransactionWithMeta,
    },
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    solana_svm_transaction::svm_message::SVMMessage,
    solana_transaction::sanitized::{MessageHash, SanitizedTransaction},
    solana_transaction_error::TransactionError,
    std::{
        collections::HashSet,
        sync::{Arc, RwLock},
        time::Instant,
    },
};

#[derive(Debug)]
#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
pub(crate) struct DisconnectedError;

/// Stats/metrics returned by `receive_and_buffer_packets`.
#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
#[derive(Default)]
pub(crate) struct ReceivingStats {
    pub num_received: usize,
    /// Count of packets that passed sigverify but were dropped
    /// without further checks because we were outside the holding
    /// window.
    pub num_dropped_without_parsing: usize,

    pub num_dropped_on_parsing_and_sanitization: usize,
    pub num_dropped_on_lock_validation: usize,
    pub num_dropped_on_compute_budget: usize,
    pub num_dropped_on_age: usize,
    pub num_dropped_on_already_processed: usize,
    pub num_dropped_on_fee_payer: usize,
    pub num_dropped_on_capacity: usize,
    pub num_buffered: usize,
    pub num_dropped_on_blacklisted_account: usize,
    pub receive_time_us: u64,
    pub buffer_time_us: u64,
    pub num_bundles_received: usize,
    pub num_bundles_dropped_on_sanitization: usize,
    pub num_bundles_dropped_on_capacity: usize,
    pub num_bundles_buffered: usize,
}

impl ReceivingStats {
    fn accumulate(&mut self, other: ReceivingStats) {
        self.num_received += other.num_received;
        self.num_dropped_without_parsing += other.num_dropped_without_parsing;
        self.num_dropped_on_parsing_and_sanitization +=
            other.num_dropped_on_parsing_and_sanitization;
        self.num_dropped_on_lock_validation += other.num_dropped_on_lock_validation;
        self.num_dropped_on_compute_budget += other.num_dropped_on_compute_budget;
        self.num_dropped_on_age += other.num_dropped_on_age;
        self.num_dropped_on_already_processed += other.num_dropped_on_already_processed;
        self.num_dropped_on_fee_payer += other.num_dropped_on_fee_payer;
        self.num_dropped_on_capacity += other.num_dropped_on_capacity;
        self.num_buffered += other.num_buffered;
        self.num_dropped_on_blacklisted_account += other.num_dropped_on_blacklisted_account;
        self.receive_time_us += other.receive_time_us;
        self.buffer_time_us += other.buffer_time_us;
        self.num_bundles_received += other.num_bundles_received;
        self.num_bundles_dropped_on_sanitization += other.num_bundles_dropped_on_sanitization;
        self.num_bundles_dropped_on_capacity += other.num_bundles_dropped_on_capacity;
        self.num_bundles_buffered += other.num_bundles_buffered;
    }
}

#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
pub(crate) trait ReceiveAndBuffer {
    type Transaction: TransactionWithMeta + Send + Sync;
    /// Also stores SanitizedBundles
    type Container: StateContainer<Self::Transaction> + Send + Sync;

    /// Return Err if the receiver is disconnected AND no packets were
    /// received. Otherwise return Ok(num_received).
    fn receive_and_buffer_packets(
        &mut self,
        container: &mut Self::Container,
        decision: &BufferedPacketsDecision,
    ) -> Result<ReceivingStats, DisconnectedError>;

    /// Return Err if the bundle receiver is disconnected and bundle were received
    /// Otherwise return Ok(num_bundles_received)
    fn receive_and_buffer_bundles(
        &mut self,
        container: &mut Self::Container,
        decision: &BufferedPacketsDecision,
    ) -> Result<usize, DisconnectedError>; // the usize is temporary and we should look at adding stat of bundles

    fn maybe_queue_batch(
        &mut self,
        container: &mut Self::Container,
        decision: &BufferedPacketsDecision,
    ) -> BufferStats;
}

#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
pub(crate) struct SanitizedTransactionReceiveAndBuffer {
    /// Packet/Transaction ingress.
    packet_receiver: PacketDeserializer,
    /// Bundle ingress
    bundle_receiver: BundleReceiver,

    bank_forks: Arc<RwLock<BankForks>>,
    blacklisted_accounts: HashSet<Pubkey>,
    tip_accounts: HashSet<Pubkey>,

    batch: Vec<ImmutableDeserializedPacket>,
    bundle_store: Vec<ImmutableDeserializedBundle>,
    batch_start: Instant,
    batch_interval: Duration,
}

impl ReceiveAndBuffer for SanitizedTransactionReceiveAndBuffer {
    type Transaction = RuntimeTransaction<SanitizedTransaction>;
    type Container = UnifiedStateContainer<Self::Transaction>;

    /// Returns whether the packet receiver is still connected.
    fn receive_and_buffer_packets(
        &mut self,
        container: &mut Self::Container,
        decision: &BufferedPacketsDecision,
    ) -> Result<ReceivingStats, DisconnectedError> {
        const MAX_RECEIVE_PACKETS: usize = 5_000;
        const MAX_PACKET_RECEIVE_TIME: Duration = Duration::from_millis(10);
        let (recv_timeout, should_batch) = match decision {
            BufferedPacketsDecision::Consume(_) | BufferedPacketsDecision::Hold => (
                if container.is_empty() {
                    MAX_PACKET_RECEIVE_TIME
                } else {
                    Duration::ZERO
                },
                true,
            ),
            BufferedPacketsDecision::Forward => (MAX_PACKET_RECEIVE_TIME, false),
            BufferedPacketsDecision::ForwardAndHold => (MAX_PACKET_RECEIVE_TIME, true),
        };

        let (received_packet_results, receive_time_us) = measure_us!(self
            .packet_receiver
            .receive_packets(recv_timeout, MAX_RECEIVE_PACKETS));

        match received_packet_results {
            Ok(receive_packet_results) => {
                let num_received =
                    receive_packet_results.packet_stats.passed_sigverify_count.0 as usize;

                if should_batch {
                    let num_dropped_on_initial_parsing =
                        num_received - receive_packet_results.deserialized_packets.len();

                    self.batch_packets(receive_packet_results.deserialized_packets);

                    Ok(ReceivingStats {
                        num_received,
                        num_dropped_without_parsing: 0,
                        // Count only the initial parsing/sanitization drops once to avoid
                        // double-counting (deserializer already accounts for failed
                        // sanitization when filtering into deserialized_packets).
                        num_dropped_on_parsing_and_sanitization: num_dropped_on_initial_parsing,
                        num_dropped_on_lock_validation: 0,
                        num_dropped_on_compute_budget: 0,
                        num_dropped_on_age: 0,
                        num_dropped_on_already_processed: 0,
                        num_dropped_on_fee_payer: 0,
                        num_dropped_on_capacity: 0,
                        num_buffered: 0,
                        num_dropped_on_blacklisted_account: 0,
                        receive_time_us,
                        buffer_time_us: 0,
                        num_bundles_received: 0,
                        num_bundles_dropped_on_sanitization: 0,
                        num_bundles_dropped_on_capacity: 0,
                        num_bundles_buffered: 0,
                    })
                } else {
                    Ok(ReceivingStats {
                        num_received,
                        num_dropped_without_parsing: num_received,
                        num_dropped_on_parsing_and_sanitization: 0,
                        num_dropped_on_lock_validation: 0,
                        num_dropped_on_compute_budget: 0,
                        num_dropped_on_age: 0,
                        num_dropped_on_already_processed: 0,
                        num_dropped_on_fee_payer: 0,
                        num_dropped_on_capacity: 0,
                        num_buffered: 0,
                        num_dropped_on_blacklisted_account: 0,
                        receive_time_us,
                        buffer_time_us: 0,
                        num_bundles_received: 0,
                        num_bundles_dropped_on_sanitization: 0,
                        num_bundles_dropped_on_capacity: 0,
                        num_bundles_buffered: 0,
                    })
                }
            }
            Err(RecvTimeoutError::Timeout) => Ok(ReceivingStats {
                num_received: 0,
                num_dropped_without_parsing: 0,
                num_dropped_on_parsing_and_sanitization: 0,
                num_dropped_on_lock_validation: 0,
                num_dropped_on_compute_budget: 0,
                num_dropped_on_age: 0,
                num_dropped_on_already_processed: 0,
                num_dropped_on_fee_payer: 0,
                num_dropped_on_capacity: 0,
                num_buffered: 0,
                num_dropped_on_blacklisted_account: 0,
                receive_time_us,
                buffer_time_us: 0,
                num_bundles_received: 0,
                num_bundles_dropped_on_sanitization: 0,
                num_bundles_dropped_on_capacity: 0,
                num_bundles_buffered: 0,
            }),
            Err(RecvTimeoutError::Disconnected) => Err(DisconnectedError),
        }
    }

    fn receive_and_buffer_bundles(
        &mut self,
        container: &mut Self::Container,
        _decision: &BufferedPacketsDecision
    ) -> Result<usize, DisconnectedError> {
        let mut batch_bundle_results = ReceiveBundleResults::default();

        match self.bundle_receiver.receive_and_buffer_bundles(
            container,
            &mut batch_bundle_results,
        ) {
            Ok(_) | Err(RecvTimeoutError::Timeout) => (),
            Err(RecvTimeoutError::Disconnected) => return Err(DisconnectedError),
        }

        let num_bundles = batch_bundle_results.deserialized_bundles.len();
        
        // Add bundles to the store and start/update the unified timer
        if !batch_bundle_results.deserialized_bundles.is_empty() {
            // If this is the first item in the batch (both transactions and bundles),
            // set the start timestamp for the batch.
            if self.batch.is_empty() && self.bundle_store.is_empty() {
                self.batch_start = Instant::now();
            }
            self.bundle_store.extend(batch_bundle_results.deserialized_bundles);
        }
        
        Ok(num_bundles)
    }

    fn maybe_queue_batch(
        &mut self,
        container: &mut UnifiedStateContainer<RuntimeTransaction<SanitizedTransaction>>,
        _decision: &BufferedPacketsDecision,
    ) -> BufferStats {
        let mut stats = BufferStats::default();
        
        // Check unified batch timer for BOTH transactions and bundles
        if self.batch_start.elapsed() >= self.batch_interval {
            // Process transaction packets
            if !self.batch.is_empty() {
                stats.accumulate(self.buffer_packets(container));
            }
            
            // Process bundles
            if !self.bundle_store.is_empty() {
                stats.accumulate(self.buffer_bundles(container));
            }
            
            // Reset the unified batch timer after processing
            self.batch_start = Instant::now();
        }
        
        stats
    }
}

#[allow(dead_code)]
#[derive(Default)]
pub struct BufferStats {
    num_received: usize,
    num_dropped_without_parsing: usize,
    num_dropped_on_sanitization: usize,
    num_dropped_on_lock_validation: usize,
    num_dropped_on_compute_budget: usize,
    num_dropped_on_age: usize,
    num_dropped_on_already_processed: usize,
    num_dropped_on_fee_payer: usize,
    num_dropped_on_capacity: usize,
    num_buffered: usize,
    num_dropped_on_blacklisted_account: usize,
    buffer_time_us: u64,
    // Bundle-specific fields
    num_bundles_dropped_on_sanitization: usize,
    num_bundles_dropped_on_capacity: usize,
    num_bundles_buffered: usize,
}

impl BufferStats {
    fn accumulate(&mut self, other: BufferStats) {
        self.num_received += other.num_received;
        self.num_dropped_without_parsing += other.num_dropped_without_parsing;
        self.num_dropped_on_sanitization += other.num_dropped_on_sanitization;
        self.num_dropped_on_lock_validation += other.num_dropped_on_lock_validation;
        self.num_dropped_on_compute_budget += other.num_dropped_on_compute_budget;
        self.num_dropped_on_age += other.num_dropped_on_age;
        self.num_dropped_on_already_processed += other.num_dropped_on_already_processed;
        self.num_dropped_on_fee_payer += other.num_dropped_on_fee_payer;
        self.num_dropped_on_capacity += other.num_dropped_on_capacity;
        self.num_buffered += other.num_buffered;
        self.num_dropped_on_blacklisted_account += other.num_dropped_on_blacklisted_account;
        self.buffer_time_us += other.buffer_time_us;
        self.num_bundles_dropped_on_sanitization += other.num_bundles_dropped_on_sanitization;
        self.num_bundles_dropped_on_capacity += other.num_bundles_dropped_on_capacity;
        self.num_bundles_buffered += other.num_bundles_buffered;
    }
}

impl SanitizedTransactionReceiveAndBuffer {
    pub fn new(
        packet_receiver: PacketDeserializer,
        bundle_receiver: BundleReceiver,
        bank_forks: Arc<RwLock<BankForks>>,
        blacklisted_accounts: HashSet<Pubkey>,
        batch_interval: Duration,
        tip_accounts: HashSet<Pubkey>,
    ) -> Self {
        Self {
            packet_receiver,
            bundle_receiver,
            bank_forks,
            blacklisted_accounts,

            batch: Vec::default(),
            bundle_store: Vec::default(),
            batch_start: Instant::now(),
            batch_interval,
            tip_accounts,
        }
    }

    fn batch_packets(&mut self, packets: Vec<ImmutableDeserializedPacket>) {
        // If this is the first item in the batch (both transactions and bundles),
        // set the start timestamp for the batch.
        if self.batch.is_empty() && self.bundle_store.is_empty() {
            self.batch_start = Instant::now();
        }

        self.batch.extend(packets);
    }

    fn buffer_packets(
        &mut self,
        container: &mut UnifiedStateContainer<RuntimeTransaction<SanitizedTransaction>>,
    ) -> BufferStats {
        let start = Instant::now();
        // Convert to Arcs
        let packets: Vec<_> = self.batch.drain(..).map(Arc::new).collect();
        // Sanitize packets, generate IDs, and insert into the container.
        let (root_bank, working_bank) = {
            let bank_forks = self.bank_forks.read().unwrap();
            let root_bank = bank_forks.root_bank();
            let working_bank = bank_forks.working_bank();
            (root_bank, working_bank)
        };
        let alt_resolved_slot = root_bank.slot();
        let sanitized_epoch = root_bank.epoch();
        let transaction_account_lock_limit = working_bank.get_transaction_account_lock_limit();
        let vote_only = working_bank.vote_only_bank();

        const CHUNK_SIZE: usize = 128;
        let lock_results: [_; CHUNK_SIZE] = core::array::from_fn(|_| Ok(()));

        let mut transactions = ArrayVec::<_, CHUNK_SIZE>::new();
        let mut max_ages = ArrayVec::<_, CHUNK_SIZE>::new();
        let mut fee_budget_limits_vec = ArrayVec::<_, CHUNK_SIZE>::new();

        let mut num_dropped_on_sanitization = 0;
        let mut num_dropped_on_lock_validation = 0;
        let mut num_dropped_on_compute_budget = 0;
        let mut num_dropped_on_age = 0;
        let mut num_dropped_on_already_processed = 0;
        let mut num_dropped_on_fee_payer = 0;
        let mut num_dropped_on_capacity = 0;
        let mut num_buffered = 0;
        let mut num_dropped_on_blacklisted_account = 0;

        let mut error_counts = TransactionErrorMetrics::default();
        for chunk in packets.chunks(CHUNK_SIZE) {
            for packet in chunk {
                let Some((tx, deactivation_slot)) = packet.build_sanitized_transaction(
                    vote_only,
                    root_bank.as_ref(),
                    root_bank.get_reserved_account_keys(),
                ) else {
                    num_dropped_on_sanitization += 1;
                    continue;
                };

                if validate_account_locks(
                    tx.message().account_keys(),
                    transaction_account_lock_limit,
                )
                .is_err()
                {
                    num_dropped_on_lock_validation += 1;
                    continue;
                }

                if tx
                    .message()
                    .account_keys()
                    .iter()
                    .any(|account| self.blacklisted_accounts.contains(account))
                {
                    num_dropped_on_blacklisted_account += 1;
                    continue;
                }

                let Ok(fee_budget_limits) = tx
                    .compute_budget_instruction_details()
                    .sanitize_and_convert_to_compute_budget_limits(&working_bank.feature_set)
                    .map(|compute_budget| compute_budget.into())
                else {
                    num_dropped_on_compute_budget += 1;
                    continue;
                };

                transactions.push(tx);
                max_ages.push(calculate_max_age(
                    sanitized_epoch,
                    deactivation_slot,
                    alt_resolved_slot,
                ));
                fee_budget_limits_vec.push(fee_budget_limits);
            }

            let check_results = working_bank.check_transactions(
                &transactions,
                &lock_results[..transactions.len()],
                MAX_PROCESSING_AGE,
                &mut error_counts,
            );

            for (((transaction, max_age), fee_budget_limits), check_result) in transactions
                .drain(..)
                .zip(max_ages.drain(..))
                .zip(fee_budget_limits_vec.drain(..))
                .zip(check_results)
            {
                match check_result {
                    Ok(_) => {}
                    Err(err) => {
                        match err {
                            TransactionError::BlockhashNotFound => {
                                num_dropped_on_age += 1;
                            }
                            TransactionError::AlreadyProcessed => {
                                num_dropped_on_already_processed += 1;
                            }
                            _ => {}
                        }
                        continue;
                    }
                }

                if Consumer::check_fee_payer_unlocked(
                    &working_bank,
                    &transaction,
                    &mut error_counts,
                )
                .is_err()
                {
                    num_dropped_on_fee_payer += 1;
                    continue;
                }

                let (priority, cost) =
                    calculate_priority_and_cost(&transaction, &fee_budget_limits, &working_bank);
                num_buffered += 1;
                if container.insert_new_transaction(transaction, max_age, priority, cost) {
                    num_dropped_on_capacity += 1;
                }
            }
        }

        BufferStats {
            num_received: 0,
            num_dropped_without_parsing: 0,
            num_dropped_on_sanitization,
            num_dropped_on_lock_validation,
            num_dropped_on_compute_budget,
            num_dropped_on_age,
            num_dropped_on_already_processed,
            num_dropped_on_fee_payer,
            num_dropped_on_capacity,
            num_buffered,
            num_dropped_on_blacklisted_account,
            buffer_time_us: start.elapsed().as_millis() as u64,
            num_bundles_dropped_on_sanitization: 0,
            num_bundles_dropped_on_capacity: 0,
            num_bundles_buffered: 0,
        }
    }

    fn buffer_bundles(
        &mut self,
        container: &mut UnifiedStateContainer<RuntimeTransaction<SanitizedTransaction>>,
    ) -> BufferStats {
        let start = Instant::now();
        
        // Get the working bank for sanitization
        let (root_bank, working_bank) = {
            let bank_forks = self.bank_forks.read().unwrap();
            (bank_forks.root_bank(), bank_forks.working_bank())
        };

        let mut num_dropped_on_sanitization = 0;
        let mut num_buffered = 0;
        let mut num_dropped_on_capacity = 0;
        let mut transaction_error_metrics = TransactionErrorMetrics::default();
        
        // Process all bundles in the store
        let bundles_to_process = std::mem::take(&mut self.bundle_store);
        
        for bundle in bundles_to_process {
            // Sanitize the bundle - build_sanitized_bundle needs bank, blacklisted_accounts, and transaction_error_metrics
            let Ok(sanitized_bundle) = bundle.build_sanitized_bundle(
                &working_bank,
                &self.blacklisted_accounts,
                &mut transaction_error_metrics,
            ) else {
                num_dropped_on_sanitization += 1;
                continue;
            };

            // Calculate aggregate priority and cost for the bundle
            let (priority, total_cost) = calculate_bundle_priority_and_cost(
                &sanitized_bundle.transactions,
                &working_bank,
                &self.tip_accounts,
            );

            // Calculate max_age for the bundle
            // Use the current slot and epoch for bundle max_age
            let sanitized_epoch = root_bank.epoch();
            let current_slot = root_bank.slot();
            let max_age = calculate_max_age(sanitized_epoch, Slot::MAX, current_slot);

            // Try to insert the bundle
            let insertion_success = container.insert_new_bundle(
                sanitized_bundle,
                max_age,
                priority,
                total_cost,
            );
            
            if insertion_success {
                num_buffered += 1;
            } else {
                num_dropped_on_capacity += 1;
            }
        }

        BufferStats {
            num_received: 0,
            num_dropped_without_parsing: 0,
            num_dropped_on_sanitization: 0,
            num_dropped_on_lock_validation: 0,
            num_dropped_on_compute_budget: 0,
            num_dropped_on_age: 0,
            num_dropped_on_already_processed: 0,
            num_dropped_on_fee_payer: 0,
            num_dropped_on_capacity: 0,
            num_buffered: 0,
            num_dropped_on_blacklisted_account: 0,
            buffer_time_us: start.elapsed().as_millis() as u64,
            num_bundles_dropped_on_sanitization: num_dropped_on_sanitization,
            num_bundles_dropped_on_capacity: num_dropped_on_capacity,
            num_bundles_buffered: num_buffered,
        }
    }
}

#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
pub(crate) struct TransactionViewReceiveAndBuffer {
    pub receiver: BankingPacketReceiver,
    //bundle ingress
    pub bundle_receiver: BundleReceiver,
    pub bank_forks: Arc<RwLock<BankForks>>,
    pub blacklisted_accounts: HashSet<Pubkey>,
    pub tip_accounts: HashSet<Pubkey>,

    // Batching
    batch: Vec<BankingPacketBatch>,
    bundle_store: Vec<ImmutableDeserializedBundle>,
    batch_start: Instant,
    batch_interval: Duration,
}

impl ReceiveAndBuffer for TransactionViewReceiveAndBuffer {
    type Transaction = RuntimeTransaction<ResolvedTransactionView<SharedBytes>>;
    type Container = TransactionViewStateContainer;

    fn receive_and_buffer_packets(
        &mut self,
        container: &mut Self::Container,
        decision: &BufferedPacketsDecision,
    ) -> Result<ReceivingStats, DisconnectedError> {
        // Receive packet batches.
        let start = Instant::now();
        let timeout = self.get_timeout();
        const PACKET_BURST_LIMIT: usize = 1000;

        let mut stats = ReceivingStats::default();

        // If not leader/unknown, do a blocking-receive initially. This lets
        // the thread sleep until a message is received, or until the timeout.
        // Additionally, only sleep if the container is empty.
        if container.is_empty()
            && matches!(
                decision,
                BufferedPacketsDecision::Forward | BufferedPacketsDecision::ForwardAndHold
            )
        {
            // TODO: Is it better to manually sleep instead, avoiding the locking
            //       overhead for wakers? But then risk not waking up when message
            //       received - as long as sleep is somewhat short, this should be
            //       fine.
            match self.receiver.recv_timeout(timeout) {
                Ok(packet_batch_message) => {
                    stats.accumulate(self.batch_packets(packet_batch_message, decision));
                }
                Err(RecvTimeoutError::Timeout) => return Ok(stats),
                Err(RecvTimeoutError::Disconnected) => {
                    return (!self.batch.is_empty())
                        .then_some(stats)
                        .ok_or(DisconnectedError);
                }
            }
        }

        while start.elapsed() < timeout && stats.num_received < PACKET_BURST_LIMIT {
            match self.receiver.try_recv() {
                Ok(packet_batch_message) => {
                    stats.accumulate(self.batch_packets(packet_batch_message, decision));
                }
                Err(TryRecvError::Empty) => return Ok(stats),
                Err(TryRecvError::Disconnected) => {
                    return (!self.batch.is_empty())
                        .then_some(stats)
                        .ok_or(DisconnectedError);
                }
            }
        }

        Ok(stats)
    }

    fn receive_and_buffer_bundles(
        &mut self,
        container: &mut Self::Container,
        _decision: &BufferedPacketsDecision,
    ) -> Result<usize, DisconnectedError> {
        let mut batch_bundle_results = ReceiveBundleResults::default();

        match self.bundle_receiver.receive_and_buffer_bundles(
            container,
            &mut batch_bundle_results,
        ) {
            Ok(_) | Err(RecvTimeoutError::Timeout) => (),
            Err(RecvTimeoutError::Disconnected) => return Err(DisconnectedError),
        }

        let num_bundles = batch_bundle_results.deserialized_bundles.len();
        
        // Add bundles to the store and start/update the unified timer
        if !batch_bundle_results.deserialized_bundles.is_empty() {
            // If this is the first item in the batch (both transactions and bundles),
            // set the start timestamp for the batch.
            if self.batch.is_empty() && self.bundle_store.is_empty() {
                self.batch_start = Instant::now();
            }
            self.bundle_store.extend(batch_bundle_results.deserialized_bundles);
        }
        
        Ok(num_bundles)
    }

    fn maybe_queue_batch(
        &mut self,
        container: &mut Self::Container,
        decision: &BufferedPacketsDecision,
    ) -> BufferStats {
        let mut stats = BufferStats::default();
        
        // Check unified batch timer for BOTH transactions and bundles
        if self.batch_start.elapsed() >= self.batch_interval {
            // Process transaction packets
            if !self.batch.is_empty() {
                let (root_bank, working_bank) = {
                    let bank_forks = self.bank_forks.read().unwrap();
                    let root_bank = bank_forks.root_bank();
                    let working_bank = bank_forks.working_bank();
                    (root_bank, working_bank)
                };

                stats.accumulate(self.handle_batch_of_packet_batch_messages(
                    container,
                    decision,
                    &root_bank,
                    &working_bank,
                ));
            }
            
            // Process bundles
            if !self.bundle_store.is_empty() {
                stats.accumulate(self.buffer_bundles(container));
            }
            
            // Reset the unified batch timer after processing
            self.batch_start = Instant::now();
        }
        
        stats
    }
}

enum PacketHandlingError {
    Sanitization,
    LockValidation,
    ComputeBudget,
    BlacklistedAccount,
}

impl TransactionViewReceiveAndBuffer {
    pub fn new(
        receiver: BankingPacketReceiver,
        bundle_receiver: BundleReceiver,
        bank_forks: Arc<RwLock<BankForks>>,
        blacklisted_accounts: HashSet<Pubkey>,
        batch_interval: Duration,
        tip_accounts: HashSet<Pubkey>,
    ) -> Self {
        Self {
            receiver,
            bundle_receiver,
            bank_forks,
            blacklisted_accounts,
            batch: Vec::default(),
            bundle_store: Vec::default(),
            batch_start: Instant::now(),
            batch_interval,
            tip_accounts,
        }
    }

    /// If batch is not empty, and time has elapsed, return zero timeout
    ///
    /// If batch is not empty, but time has not elapsed, return remaining time
    ///
    /// If batch is empty, return default timeout
    fn get_timeout(&self) -> Duration {
        if !self.batch.is_empty() {
            if self.batch_start.elapsed() >= self.batch_interval {
                Duration::ZERO
            } else {
                self.batch_interval - self.batch_start.elapsed()
            }
        } else {
            Duration::from_millis(10)
        }
    }

    fn buffer_bundles(
        &mut self,
        container: &mut TransactionViewStateContainer,
    ) -> BufferStats {
        let start = Instant::now();
        
        // Get the working bank for sanitization
        let (root_bank, working_bank) = {
            let bank_forks = self.bank_forks.read().unwrap();
            (bank_forks.root_bank(), bank_forks.working_bank())
        };

        let mut num_dropped_on_sanitization = 0;
        let mut num_buffered = 0;
        let mut num_dropped_on_capacity = 0;
        let mut transaction_error_metrics = TransactionErrorMetrics::default();
        
        // Process all bundles in the store
        let bundles_to_process = std::mem::take(&mut self.bundle_store);
        
        for bundle in bundles_to_process {
            // Sanitize the bundle
            let Ok(sanitized_bundle) = bundle.build_sanitized_bundle(
                &working_bank,
                &self.blacklisted_accounts,
                &mut transaction_error_metrics,
            ) else {
                num_dropped_on_sanitization += 1;
                continue;
            };

            // Calculate aggregate priority and cost for the bundle
            let (priority, total_cost) = calculate_bundle_priority_and_cost(
                &sanitized_bundle.transactions,
                &working_bank,
                &self.tip_accounts,
            );

            // Calculate max_age for the bundle
            let sanitized_epoch = root_bank.epoch();
            let current_slot = root_bank.slot();
            let max_age = calculate_max_age(sanitized_epoch, Slot::MAX, current_slot);

            // Try to insert the bundle
            let insertion_success = container.insert_new_bundle(
                sanitized_bundle,
                max_age,
                priority,
                total_cost
            );
            
            if insertion_success {
                num_buffered += 1;
            } else {
                num_dropped_on_capacity += 1;
            }
        }

        BufferStats {
            num_received: 0,
            num_dropped_without_parsing: 0,
            num_dropped_on_sanitization: 0,
            num_dropped_on_lock_validation: 0,
            num_dropped_on_compute_budget: 0,
            num_dropped_on_age: 0,
            num_dropped_on_already_processed: 0,
            num_dropped_on_fee_payer: 0,
            num_dropped_on_capacity: 0,
            num_buffered: 0,
            num_dropped_on_blacklisted_account: 0,
            buffer_time_us: start.elapsed().as_millis() as u64,
            num_bundles_dropped_on_sanitization: num_dropped_on_sanitization,
            num_bundles_dropped_on_capacity: num_dropped_on_capacity,
            num_bundles_buffered: num_buffered,
        }
    }

    /// Filter and batch the TXs
    fn batch_packets(
        &mut self,
        packet_batch_message: BankingPacketBatch,
        decision: &BufferedPacketsDecision,
    ) -> ReceivingStats {
        // If not holding packets, just drop them immediately without parsing.
        if matches!(decision, BufferedPacketsDecision::Forward) {
            let mut stats = ReceivingStats::default();
            let total = packet_batch_message.iter().map(|b| b.len()).sum::<usize>();
            stats.num_received = total;
            stats.num_dropped_without_parsing = total;
            return stats;
        }

        // If this is the first item in the batch (both transactions and bundles),
        // set the start timestamp for the batch.
        if self.batch.is_empty() && self.bundle_store.is_empty() {
            self.batch_start = Instant::now();
        }

        // Count total number of packets and how many were marked discard
        let mut total_packets_len = 0;
        let non_discard = packet_batch_message
            .iter()
            .map(|b| {
                total_packets_len += b.len();
                b.iter()
                    .map(|p| usize::from(!p.meta().discard()))
                    .sum::<usize>()
            })
            .sum::<usize>();

        self.batch.push(packet_batch_message);

        ReceivingStats {
            // Mirror transaction_scheduler semantics: num_received counts all packets seen,
            // while num_dropped_without_parsing accounts for those flagged discard.
            num_received: total_packets_len,
            num_dropped_without_parsing: total_packets_len - non_discard,
            num_dropped_on_parsing_and_sanitization: 0,
            num_dropped_on_lock_validation: 0,
            num_dropped_on_compute_budget: 0,
            num_dropped_on_age: 0,
            num_dropped_on_already_processed: 0,
            num_dropped_on_fee_payer: 0,
            num_dropped_on_capacity: 0,
            num_buffered: 0,
            num_dropped_on_blacklisted_account: 0,
            receive_time_us: 0,
            buffer_time_us: 0,
            num_bundles_received: 0,
            num_bundles_dropped_on_sanitization: 0,
            num_bundles_dropped_on_capacity: 0,
            num_bundles_buffered: 0,
        }
    }

    /// Return number of received packets.
    fn handle_batch_of_packet_batch_messages(
        &mut self,
        container: &mut TransactionViewStateContainer,
        decision: &BufferedPacketsDecision,
        root_bank: &Bank,
        working_bank: &Bank,
    ) -> BufferStats {
        let packet_batch_messages: Vec<BankingPacketBatch> = self.batch.drain(..).collect();

        let start = Instant::now();
        // If outside holding window, do not parse.
        let should_parse = !matches!(decision, BufferedPacketsDecision::Forward);

        // Sanitize packets, generate IDs, and insert into the container.
        let alt_resolved_slot = root_bank.slot();
        let sanitized_epoch = root_bank.epoch();
        let transaction_account_lock_limit = working_bank.get_transaction_account_lock_limit();

        // Create temporary batches of transactions to be age-checked.
        let mut transaction_priority_ids = ArrayVec::<_, EXTRA_CAPACITY>::new();
        let lock_results: [_; EXTRA_CAPACITY] = core::array::from_fn(|_| Ok(()));
        let mut error_counters = TransactionErrorMetrics::default();
        let mut num_dropped_on_age = 0;
        let mut num_dropped_on_already_processed = 0;
        let mut num_dropped_on_fee_payer = 0;
        let mut num_dropped_on_capacity = 0;
        let mut num_buffered = 0;

        let mut check_and_push_to_queue =
            |container: &mut TransactionViewStateContainer,
             transaction_priority_ids: &mut ArrayVec<UnifiedPriorityId, 64>| {
                // Temporary scope so that transaction references are immediately
                // dropped and transactions not passing
                let mut check_results = {
                    let mut transactions = ArrayVec::<_, EXTRA_CAPACITY>::new();
                    transactions.extend(transaction_priority_ids.iter().map(|priority_id| {
                        container
                            .get_transaction(priority_id.get_id())
                            .expect("transaction must exist")
                    }));
                    working_bank.check_transactions::<RuntimeTransaction<_>>(
                        &transactions,
                        &lock_results[..transactions.len()],
                        MAX_PROCESSING_AGE,
                        &mut error_counters,
                    )
                };

                // Remove errored transactions
                for (result, priority_id) in check_results
                    .iter_mut()
                    .zip(transaction_priority_ids.iter())
                {
                    if let Err(err) = result {
                        match err {
                            TransactionError::BlockhashNotFound => {
                                num_dropped_on_age += 1;
                            }
                            TransactionError::AlreadyProcessed => {
                                num_dropped_on_already_processed += 1;
                            }
                            _ => {}
                        }
                        container.remove_by_id(priority_id.id);
                        continue;
                    }
                    let transaction = container
                        .get_transaction(priority_id.get_id())
                        .expect("transaction must exist");
                    if let Err(err) = Consumer::check_fee_payer_unlocked(
                        working_bank,
                        transaction,
                        &mut error_counters,
                    ) {
                        *result = Err(err);
                        num_dropped_on_fee_payer += 1;
                        container.remove_by_id(priority_id.id);
                        continue;
                    }

                    num_buffered += 1;
                }
                // Push non-errored transaction into queue.
                num_dropped_on_capacity += container.push_ids_into_queue(
                    check_results
                        .into_iter()
                        .zip(transaction_priority_ids.drain(..))
                        .filter(|(r, _)| r.is_ok())
                        .map(|(_, id)| id),
                );
            };

        let mut num_dropped_without_parsing = 0;
        let mut num_dropped_on_parsing_and_sanitization = 0;
        let mut num_dropped_on_lock_validation = 0;
        let mut num_dropped_on_compute_budget = 0;
        let mut num_dropped_on_blacklisted_account = 0;

        let flatted_messages: Vec<&PacketBatch> = packet_batch_messages
            .iter()
            .flat_map(|arc| arc.iter())
            .collect();
        for packet_batch in flatted_messages {
            for packet in packet_batch.iter() {
                let Some(packet_data) = packet.data(..) else {
                    continue;
                };

                if !should_parse {
                    num_dropped_without_parsing += 1;
                    continue;
                }

                // Reserve free-space to copy packet into, run sanitization checks, and insert.
                if let Some(transaction_id) =
                    container.try_insert_map_only_with_data(packet_data, |bytes| {
                        match Self::try_handle_packet(
                            bytes,
                            root_bank,
                            working_bank,
                            alt_resolved_slot,
                            sanitized_epoch,
                            transaction_account_lock_limit,
                            &self.blacklisted_accounts,
                        ) {
                            Ok(state) => Ok(state),
                            Err(PacketHandlingError::Sanitization) => {
                                num_dropped_on_parsing_and_sanitization += 1;
                                Err(())
                            }
                            Err(PacketHandlingError::LockValidation) => {
                                num_dropped_on_lock_validation += 1;
                                Err(())
                            }
                            Err(PacketHandlingError::ComputeBudget) => {
                                num_dropped_on_compute_budget += 1;
                                Err(())
                            }
                            Err(PacketHandlingError::BlacklistedAccount) => {
                                num_dropped_on_blacklisted_account += 1;
                                Err(())
                            }
                        }
                    })
                {
                    let priority = container
                        .get_mut_transaction_state(transaction_id)
                        .expect("transaction must exist")
                        .priority();
                    transaction_priority_ids
                        .push(UnifiedPriorityId::new(priority, UnifiedSchedulingUnit::Transaction(transaction_id)));

                    // If at capacity, run checks and remove invalid transactions.
                    if transaction_priority_ids.len() == EXTRA_CAPACITY {
                        check_and_push_to_queue(container, &mut transaction_priority_ids);
                    }
                }
            }
        }

        // Any remaining packets undergo status/age checks
        check_and_push_to_queue(container, &mut transaction_priority_ids);

        BufferStats {
            // Match transaction_scheduler semantics: do not populate num_received here.
            num_received: 0,
            num_dropped_without_parsing,
            num_dropped_on_sanitization: num_dropped_on_parsing_and_sanitization,
            num_dropped_on_lock_validation,
            num_dropped_on_compute_budget,
            num_dropped_on_age,
            num_dropped_on_already_processed,
            num_dropped_on_fee_payer,
            num_dropped_on_capacity,
            num_buffered,
            num_dropped_on_blacklisted_account,
            buffer_time_us: start.elapsed().as_micros() as u64,
            num_bundles_dropped_on_sanitization: 0,
            num_bundles_dropped_on_capacity: 0,
            num_bundles_buffered: 0,
        }
    }

    fn try_handle_packet(
        bytes: SharedBytes,
        root_bank: &Bank,
        working_bank: &Bank,
        alt_resolved_slot: Slot,
        sanitized_epoch: Epoch,
        transaction_account_lock_limit: usize,
        blacklisted_accounts: &HashSet<Pubkey>,
    ) -> Result<TransactionViewState, PacketHandlingError> {
        // Parsing and basic sanitization checks
        let Ok(view) = SanitizedTransactionView::try_new_sanitized(bytes) else {
            return Err(PacketHandlingError::Sanitization);
        };

        let Ok(view) = RuntimeTransaction::<SanitizedTransactionView<_>>::try_from(
            view,
            MessageHash::Compute,
            None,
        ) else {
            return Err(PacketHandlingError::Sanitization);
        };

        // Discard non-vote packets if in vote-only mode.
        if root_bank.vote_only_bank() && !view.is_simple_vote_transaction() {
            return Err(PacketHandlingError::Sanitization);
        }

        // Load addresses for transaction.
        let load_addresses_result = match view.version() {
            TransactionVersion::Legacy => Ok((None, u64::MAX)),
            TransactionVersion::V0 => root_bank
                .load_addresses_from_ref(view.address_table_lookup_iter())
                .map(|(loaded_addresses, deactivation_slot)| {
                    (Some(loaded_addresses), deactivation_slot)
                }),
        };
        let Ok((loaded_addresses, deactivation_slot)) = load_addresses_result else {
            return Err(PacketHandlingError::Sanitization);
        };

        let Ok(view) = RuntimeTransaction::<ResolvedTransactionView<_>>::try_from(
            view,
            loaded_addresses,
            root_bank.get_reserved_account_keys(),
        ) else {
            return Err(PacketHandlingError::Sanitization);
        };

        if validate_account_locks(view.account_keys(), transaction_account_lock_limit).is_err() {
            return Err(PacketHandlingError::LockValidation);
        }

        if view
            .account_keys()
            .iter()
            .any(|account| blacklisted_accounts.contains(account))
        {
            return Err(PacketHandlingError::BlacklistedAccount);
        }

        let Ok(compute_budget_limits) = view
            .compute_budget_instruction_details()
            .sanitize_and_convert_to_compute_budget_limits(&working_bank.feature_set)
        else {
            return Err(PacketHandlingError::ComputeBudget);
        };

        let max_age = calculate_max_age(sanitized_epoch, deactivation_slot, alt_resolved_slot);
        let fee_budget_limits = FeeBudgetLimits::from(compute_budget_limits);
        let (priority, cost) = calculate_priority_and_cost(&view, &fee_budget_limits, working_bank);

        Ok(TransactionState::new(view, max_age, priority, cost))
    }
}

/// Calculate aggregate priority and cost for a bundle of transactions.
///
/// For bundles, we sum the rewards and costs across all transactions,
/// then calculate a single priority for the entire bundle:
/// P = (sum of rewards * 1M) / (sum of costs + 1)
///
/// This ensures bundles are prioritized based on their total economic value
/// relative to their total resource consumption.
///
/// Returns (priority, total_cost) as (u64, u64).
fn calculate_bundle_priority_and_cost(
    transactions: &[RuntimeTransaction<SanitizedTransaction>],
    bank: &Bank,
    tip_accounts: &HashSet<Pubkey>,
) -> (u64, u64) {
    let mut total_reward = 0u128;
    let mut total_cost = 0u128;
    
    for tx in transactions {
        let compute_budget_details = tx.compute_budget_instruction_details();
        let Ok(compute_budget_limits) = compute_budget_details
            .sanitize_and_convert_to_compute_budget_limits(&bank.feature_set)
        else {
            // Skip this transaction if compute budget is invalid
            continue;
        };
        
        let fee_budget_limits = FeeBudgetLimits::from(compute_budget_limits);
        let cost = CostModel::calculate_cost(tx, &bank.feature_set).sum();
        let reward = bank.calculate_reward_for_transaction(tx, &fee_budget_limits);

        // Calculate tip rewards
        let account_keys = tx.message().static_account_keys();
        let message = tx.message();
        let tips = message.program_instructions_iter().filter_map(|(pid, ix)| extract_transfer(account_keys, pid, ix))
            .filter(|(dest, _)| tip_accounts.contains(dest))
            .map(|(_, amount)| amount)
            .sum::<u64>();
        
        total_reward += reward as u128;
        total_reward += tips as u128;
        total_cost += cost as u128;
    }

    // Calculate priority: (total_reward * 1M) / (total_cost + 1)
    const MULTIPLIER: u128 = 1_000_000;
    let priority = total_reward
        .saturating_mul(MULTIPLIER)
        .saturating_div(total_cost.saturating_add(1));
    
    (priority as u64, total_cost as u64)
}

fn extract_transfer<'a>(
        account_keys: &'a [Pubkey],
        program_id: &Pubkey,
        ix: &CompiledInstruction,
    ) -> Option<(&'a Pubkey, u64)> {
        if program_id == &solana_sdk_ids::system_program::ID
            && ix.data.len() >= 12
            && u32::from_le_bytes(*array_ref![&ix.data, 0, 4]) == 2
        {
            let destination = account_keys.get(*ix.accounts.get(1)? as usize)?;
            let amount = u64::from_le_bytes(*array_ref![ix.data, 4, 8]);

            Some((destination, amount))
        } else {
            None
        }
    }


/// Calculate priority and cost for a transaction:
///
/// Cost is calculated through the `CostModel`,
/// and priority is calculated through a formula here that attempts to sell
/// blockspace to the highest bidder.
///
/// The priority is calculated as:
/// P = R / (1 + C)
/// where P is the priority, R is the reward,
/// and C is the cost towards block-limits.
///
/// Current minimum costs are on the order of several hundred,
/// so the denominator is effectively C, and the +1 is simply
/// to avoid any division by zero due to a bug - these costs
/// are calculated by the cost-model and are not direct
/// from user input. They should never be zero.
/// Any difference in the prioritization is negligible for
/// the current transaction costs.
fn calculate_priority_and_cost(
    transaction: &impl TransactionWithMeta,
    fee_budget_limits: &FeeBudgetLimits,
    bank: &Bank,
) -> (u64, u64) {
    let cost = CostModel::calculate_cost(transaction, &bank.feature_set).sum();
    let reward = bank.calculate_reward_for_transaction(transaction, fee_budget_limits);

    // We need a multiplier here to avoid rounding down too aggressively.
    // For many transactions, the cost will be greater than the fees in terms of raw lamports.
    // For the purposes of calculating prioritization, we multiply the fees by a large number so that
    // the cost is a small fraction.
    // An offset of 1 is used in the denominator to explicitly avoid division by zero.
    const MULTIPLIER: u64 = 1_000_000;
    (
        reward
            .saturating_mul(MULTIPLIER)
            .saturating_div(cost.saturating_add(1)),
        cost,
    )
}

/// Given the epoch, the minimum deactivation slot, and the current slot,
/// return the `MaxAge` that should be used for the transaction. This is used
/// to determine the maximum slot that a transaction will be considered valid
/// for, without re-resolving addresses or resanitizing.
///
/// This function considers the deactivation period of Address Table
/// accounts. If the deactivation period runs past the end of the epoch,
/// then the transaction is considered valid until the end of the epoch.
/// Otherwise, the transaction is considered valid until the deactivation
/// period.
///
/// Since the deactivation period technically uses blocks rather than
/// slots, the value used here is the lower-bound on the deactivation
/// period, i.e. the transaction's address lookups are valid until
/// AT LEAST this slot.
fn calculate_max_age(
    sanitized_epoch: Epoch,
    deactivation_slot: Slot,
    current_slot: Slot,
) -> MaxAge {
    let alt_min_expire_slot = estimate_last_valid_slot(deactivation_slot.min(current_slot));
    MaxAge {
        sanitized_epoch,
        alt_invalidation_slot: alt_min_expire_slot,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::banking_stage::tests::create_slow_genesis_config,
        crossbeam_channel::{unbounded, Receiver},
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_ledger::genesis_utils::GenesisConfigInfo,
        solana_message::{v0, AddressLookupTableAccount, VersionedMessage},
        solana_packet::{Meta, PACKET_DATA_SIZE},
        solana_perf::packet::{to_packet_batches, Packet, PacketBatch, PinnedPacketBatch},
        solana_pubkey::Pubkey,
        solana_signer::Signer,
        solana_system_interface::instruction as system_instruction,
        solana_system_transaction::transfer,
        solana_transaction::versioned::VersionedTransaction,
        std::thread::sleep,
        test_case::test_case,
    };

    fn test_bank_forks() -> (Arc<RwLock<BankForks>>, Keypair) {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(u64::MAX);

        let (_bank, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
        (bank_forks, mint_keypair)
    }

    const TEST_CONTAINER_CAPACITY: usize = 100;
    const BATCH_PERIOD: Duration = Duration::from_millis(50);

    fn setup_sanitized_transaction_receive_and_buffer(
        receiver: Receiver<BankingPacketBatch>,
        bank_forks: Arc<RwLock<BankForks>>,
        blacklisted_accounts: HashSet<Pubkey>,
    ) -> (
        SanitizedTransactionReceiveAndBuffer,
        UnifiedStateContainer<RuntimeTransaction<SanitizedTransaction>>,
    ) {
        let (bundle_sender, bundle_receiver_ch) = crossbeam_channel::unbounded();
        drop(bundle_sender); // We won't send any bundles in tests
        
        let receive_and_buffer = SanitizedTransactionReceiveAndBuffer {
            packet_receiver: PacketDeserializer::new(receiver),
            bundle_receiver: BundleReceiver::new(10_000, bundle_receiver_ch, Some(5)),
            bank_forks,
            blacklisted_accounts,

            batch: Vec::default(),
            bundle_store: Vec::default(),
            batch_start: Instant::now(),
            batch_interval: Duration::ZERO,
            tip_accounts: HashSet::new(),
        };
        let container = UnifiedStateContainer::with_capacity(TEST_CONTAINER_CAPACITY);
        (receive_and_buffer, container)
    }

    fn setup_sanitized_transaction_receive_and_buffer_with_batching(
        receiver: Receiver<BankingPacketBatch>,
        bank_forks: Arc<RwLock<BankForks>>,
        blacklisted_accounts: HashSet<Pubkey>,
    ) -> (
        SanitizedTransactionReceiveAndBuffer,
        UnifiedStateContainer<RuntimeTransaction<SanitizedTransaction>>,
    ) {
        let (bundle_sender, bundle_receiver_ch) = crossbeam_channel::unbounded();
        drop(bundle_sender); // We won't send any bundles in tests
        
        let receive_and_buffer = SanitizedTransactionReceiveAndBuffer {
            packet_receiver: PacketDeserializer::new(receiver),
            bundle_receiver: BundleReceiver::new(10_000, bundle_receiver_ch, Some(5)),
            bank_forks,
            blacklisted_accounts,

            batch: Vec::default(),
            bundle_store: Vec::default(),
            batch_start: Instant::now(),
            batch_interval: BATCH_PERIOD,
            tip_accounts: HashSet::new(),
        };
        let container = UnifiedStateContainer::with_capacity(TEST_CONTAINER_CAPACITY);
        (receive_and_buffer, container)
    }

    fn setup_transaction_view_receive_and_buffer(
        receiver: Receiver<BankingPacketBatch>,
        bank_forks: Arc<RwLock<BankForks>>,
        blacklisted_accounts: HashSet<Pubkey>,
    ) -> (
        TransactionViewReceiveAndBuffer,
        TransactionViewStateContainer,
    ) {
        let (bundle_sender, bundle_receiver_ch) = crossbeam_channel::unbounded();
        drop(bundle_sender); // We won't send any bundles in tests
        
        let receive_and_buffer = TransactionViewReceiveAndBuffer {
            receiver,
            bundle_receiver: BundleReceiver::new(10_000, bundle_receiver_ch, Some(5)),
            bank_forks,
            blacklisted_accounts,

            batch: Vec::default(),
            bundle_store: Vec::default(),
            batch_start: Instant::now(),
            batch_interval: Duration::ZERO,
            tip_accounts: HashSet::new(),
        };
        let container = TransactionViewStateContainer::with_capacity(TEST_CONTAINER_CAPACITY);
        (receive_and_buffer, container)
    }

    fn setup_transaction_view_receive_and_buffer_with_batching(
        receiver: Receiver<BankingPacketBatch>,
        bank_forks: Arc<RwLock<BankForks>>,
        blacklisted_accounts: HashSet<Pubkey>,
    ) -> (
        TransactionViewReceiveAndBuffer,
        TransactionViewStateContainer,
    ) {
        let (bundle_sender, bundle_receiver_ch) = crossbeam_channel::unbounded();
        drop(bundle_sender); // We won't send any bundles in tests
        
        let receive_and_buffer = TransactionViewReceiveAndBuffer {
            receiver,
            bundle_receiver: BundleReceiver::new(10_000, bundle_receiver_ch, Some(5)),
            bank_forks,
            blacklisted_accounts,

            batch: Vec::default(),
            bundle_store: Vec::default(),
            batch_start: Instant::now(),
            batch_interval: BATCH_PERIOD,
            tip_accounts: HashSet::new(),
        };
        let container = TransactionViewStateContainer::with_capacity(TEST_CONTAINER_CAPACITY);
        (receive_and_buffer, container)
    }

    // verify container state makes sense:
    // 1. Number of transactions matches expectation
    // 2. All transactions IDs in priority queue exist in the map
    fn verify_container<Tx: TransactionWithMeta>(
        container: &mut impl StateContainer<Tx>,
        expected_length: usize,
    ) {
        let mut actual_length: usize = 0;
        while let Some(id) = container.pop() {
            let Some(_) = container.get_transaction(id.get_id()) else {
                panic!(
                    "transaction in queue position {} with id {} must exist.",
                    actual_length, id.get_id()
                );
            };
            actual_length += 1;
        }

        assert_eq!(actual_length, expected_length);
    }

    #[test]
    fn test_calculate_max_age() {
        let current_slot = 100;
        let sanitized_epoch = 10;

        // ALT deactivation slot is delayed
        assert_eq!(
            calculate_max_age(sanitized_epoch, current_slot - 1, current_slot),
            MaxAge {
                sanitized_epoch,
                alt_invalidation_slot: current_slot - 1 + solana_slot_hashes::get_entries() as u64,
            }
        );

        // no deactivation slot
        assert_eq!(
            calculate_max_age(sanitized_epoch, u64::MAX, current_slot),
            MaxAge {
                sanitized_epoch,
                alt_invalidation_slot: current_slot + solana_slot_hashes::get_entries() as u64,
            }
        );
    }

    #[test_case(setup_sanitized_transaction_receive_and_buffer; "testcase-sdk")]
    #[test_case(setup_transaction_view_receive_and_buffer; "testcase-view")]
    fn test_receive_and_buffer_disconnected_channel<R: ReceiveAndBuffer>(
        setup_receive_and_buffer: impl FnOnce(
            Receiver<BankingPacketBatch>,
            Arc<RwLock<BankForks>>,
            HashSet<Pubkey>,
        ) -> (R, R::Container),
    ) {
        let (sender, receiver) = unbounded();
        let (bank_forks, _mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container) =
            setup_receive_and_buffer(receiver, bank_forks, HashSet::default());

        drop(sender); // disconnect channel
        let r = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold);
        assert!(r.is_err());
    }

    #[test_case(setup_sanitized_transaction_receive_and_buffer; "testcase-sdk")]
    #[test_case(setup_transaction_view_receive_and_buffer; "testcase-view")]
    fn test_receive_and_buffer_no_hold<R: ReceiveAndBuffer>(
        setup_receive_and_buffer: impl FnOnce(
            Receiver<BankingPacketBatch>,
            Arc<RwLock<BankForks>>,
            HashSet<Pubkey>,
        ) -> (R, R::Container),
    ) {
        let (sender, receiver) = unbounded();
        let (bank_forks, mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container) =
            setup_receive_and_buffer(receiver, bank_forks.clone(), HashSet::default());

        let transaction = transfer(
            &mint_keypair,
            &Pubkey::new_unique(),
            1,
            bank_forks.read().unwrap().root_bank().last_blockhash(),
        );
        let packet_batches = Arc::new(to_packet_batches(&[transaction], 1));
        sender.send(packet_batches).unwrap();

        let ReceivingStats {
            num_received,
            num_dropped_without_parsing,
            num_dropped_on_parsing_and_sanitization,
            num_dropped_on_lock_validation,
            num_dropped_on_compute_budget,
            num_dropped_on_age,
            num_dropped_on_already_processed,
            num_dropped_on_fee_payer,
            num_dropped_on_capacity,
            num_buffered,
            ..
        } = receive_and_buffer
            .receive_and_buffer_packets(
                &mut container,
                &BufferedPacketsDecision::Forward, // no packets should be held
            )
            .unwrap();

        assert_eq!(num_received, 1);
        assert_eq!(num_dropped_without_parsing, 1);
        assert_eq!(num_dropped_on_parsing_and_sanitization, 0);
        assert_eq!(num_dropped_on_lock_validation, 0);
        assert_eq!(num_dropped_on_compute_budget, 0);
        assert_eq!(num_dropped_on_age, 0);
        assert_eq!(num_dropped_on_already_processed, 0);
        assert_eq!(num_dropped_on_fee_payer, 0);
        assert_eq!(num_dropped_on_capacity, 0);
        assert_eq!(num_buffered, 0);
        verify_container(&mut container, 0);
    }

    #[test_case(setup_sanitized_transaction_receive_and_buffer, 0, 0; "testcase-sdk")]
    #[test_case(setup_transaction_view_receive_and_buffer, 1, 1; "testcase-view")]
    fn test_receive_and_buffer_discard<R: ReceiveAndBuffer>(
        setup_receive_and_buffer: impl FnOnce(
            Receiver<BankingPacketBatch>,
            Arc<RwLock<BankForks>>,
            HashSet<Pubkey>,
        ) -> (R, R::Container),
        should_receive: usize,
        should_drop_without_parsing: usize,
    ) {
        let (sender, receiver) = unbounded();
        let (bank_forks, mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container) =
            setup_receive_and_buffer(receiver, bank_forks.clone(), HashSet::default());

        let transaction = transfer(
            &mint_keypair,
            &Pubkey::new_unique(),
            1,
            bank_forks.read().unwrap().root_bank().last_blockhash(),
        );
        let mut packet_batches = Arc::new(to_packet_batches(&[transaction], 1));
        Arc::make_mut(&mut packet_batches)[0]
            .first_mut()
            .unwrap()
            .meta_mut()
            .set_discard(true);
        sender.send(packet_batches).unwrap();

        let ReceivingStats {
            num_received,
            num_dropped_without_parsing,
            num_dropped_on_parsing_and_sanitization,
            num_dropped_on_lock_validation,
            num_dropped_on_compute_budget,
            num_dropped_on_age,
            num_dropped_on_already_processed,
            num_dropped_on_fee_payer,
            num_dropped_on_capacity,
            num_buffered,
            ..
        } = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();
        // SDK path: should_receive = 0, VIEW path: should_receive = 1
        assert_eq!(num_received, should_receive);
        assert_eq!(num_dropped_without_parsing, should_drop_without_parsing);
        assert_eq!(num_dropped_on_parsing_and_sanitization, 0);
        assert_eq!(num_dropped_on_lock_validation, 0);
        assert_eq!(num_dropped_on_compute_budget, 0);
        assert_eq!(num_dropped_on_age, 0);
        assert_eq!(num_dropped_on_already_processed, 0);
        assert_eq!(num_dropped_on_fee_payer, 0);
        assert_eq!(num_dropped_on_capacity, 0);
        assert_eq!(num_buffered, 0);

        // maybe_queue should do nothing for discarded packet
        let _ = receive_and_buffer.maybe_queue_batch(&mut container, &BufferedPacketsDecision::Hold);
        verify_container(&mut container, 0);
    }

    #[test_case(setup_sanitized_transaction_receive_and_buffer, 1, 0; "testcase-sdk")]
    #[test_case(setup_transaction_view_receive_and_buffer, 0, 1; "testcase-view")]
    fn test_receive_and_buffer_invalid_transaction_format<R: ReceiveAndBuffer>(
        setup_receive_and_buffer: impl FnOnce(
            Receiver<BankingPacketBatch>,
            Arc<RwLock<BankForks>>,
            HashSet<Pubkey>,
        ) -> (R, R::Container),
        should_drop_on_parsing_and_sanitization: usize,
        should_drop_on_sanitization: usize,
    ) {
        let (sender, receiver) = unbounded();
        let (bank_forks, _mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container) =
            setup_receive_and_buffer(receiver, bank_forks.clone(), HashSet::default());

        let packet_batches = Arc::new(vec![PacketBatch::from(PinnedPacketBatch::new(vec![
            Packet::new([1u8; PACKET_DATA_SIZE], Meta::default()),
        ]))]);
        sender.send(packet_batches).unwrap();

        let ReceivingStats {
            num_received,
            num_dropped_without_parsing,
            num_dropped_on_parsing_and_sanitization,
            num_dropped_on_lock_validation,
            num_dropped_on_compute_budget,
            num_dropped_on_age,
            num_dropped_on_already_processed,
            num_dropped_on_fee_payer,
            num_dropped_on_capacity,
            num_buffered,
            receive_time_us: _,
            buffer_time_us: _,
            num_dropped_on_blacklisted_account: _,
            ..
        } = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();

        assert_eq!(num_received, 1);
        assert_eq!(num_dropped_without_parsing, 0);
        assert_eq!(num_dropped_on_parsing_and_sanitization, should_drop_on_parsing_and_sanitization);
        assert_eq!(num_dropped_on_lock_validation, 0);
        assert_eq!(num_dropped_on_compute_budget, 0);
        assert_eq!(num_dropped_on_age, 0);
        assert_eq!(num_dropped_on_already_processed, 0);
        assert_eq!(num_dropped_on_fee_payer, 0);
        assert_eq!(num_dropped_on_capacity, 0);
        assert_eq!(num_buffered, 0);
        let BufferStats {
            num_received,
            num_buffered,
            num_dropped_without_parsing,
            num_dropped_on_sanitization,
            ..
        } = receive_and_buffer.maybe_queue_batch(&mut container, &BufferedPacketsDecision::Hold);

        assert_eq!(num_received, 0);
        assert_eq!(num_buffered, 0);
        assert_eq!(num_dropped_without_parsing, 0);
        assert_eq!(num_dropped_on_sanitization, should_drop_on_sanitization);

        verify_container(&mut container, 0);
    }

    #[test_case(setup_sanitized_transaction_receive_and_buffer; "testcase-sdk")]
    #[test_case(setup_transaction_view_receive_and_buffer; "testcase-view")]
    fn test_receive_and_buffer_invalid_blockhash<R: ReceiveAndBuffer>(
        setup_receive_and_buffer: impl FnOnce(
            Receiver<BankingPacketBatch>,
            Arc<RwLock<BankForks>>,
            HashSet<Pubkey>,
        ) -> (R, R::Container),
    ) {
        let (sender, receiver) = unbounded();
        let (bank_forks, mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container) =
            setup_receive_and_buffer(receiver, bank_forks.clone(), HashSet::default());

        let transaction = transfer(&mint_keypair, &Pubkey::new_unique(), 1, Hash::new_unique());
        let packet_batches = Arc::new(to_packet_batches(&[transaction], 1));
        sender.send(packet_batches).unwrap();

        let ReceivingStats { num_received, .. } = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();

        assert_eq!(num_received, 1);

        let BufferStats {
            num_received,
            num_buffered,
            num_dropped_on_age,
            ..
        } = receive_and_buffer.maybe_queue_batch(&mut container, &BufferedPacketsDecision::Hold);

        assert_eq!(num_received, 0);
        assert_eq!(num_buffered, 0);
        assert_eq!(num_dropped_on_age, 1);

        verify_container(&mut container, 0);
    }

    #[test_case(setup_sanitized_transaction_receive_and_buffer; "testcase-sdk")]
    #[test_case(setup_transaction_view_receive_and_buffer; "testcase-view")]
    fn test_receive_and_buffer_simple_transfer_unfunded_fee_payer<R: ReceiveAndBuffer>(
        setup_receive_and_buffer: impl FnOnce(
            Receiver<BankingPacketBatch>,
            Arc<RwLock<BankForks>>,
            HashSet<Pubkey>,
        ) -> (R, R::Container),
    ) {
        let (sender, receiver) = unbounded();
        let (bank_forks, _mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container) =
            setup_receive_and_buffer(receiver, bank_forks.clone(), HashSet::default());

        let transaction = transfer(
            &Keypair::new(),
            &Pubkey::new_unique(),
            1,
            bank_forks.read().unwrap().root_bank().last_blockhash(),
        );
        let packet_batches = Arc::new(to_packet_batches(&[transaction], 1));
        sender.send(packet_batches).unwrap();

        let ReceivingStats { num_received, .. } = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();

        assert_eq!(num_received, 1);
        let BufferStats {
            num_received,
            num_buffered,
            num_dropped_on_fee_payer,
            ..
        } = receive_and_buffer.maybe_queue_batch(&mut container, &BufferedPacketsDecision::Hold);

        assert_eq!(num_received, 0);
        assert_eq!(num_buffered, 0);
        assert_eq!(num_dropped_on_fee_payer, 1);

        verify_container(&mut container, 0);
    }

    #[test_case(setup_sanitized_transaction_receive_and_buffer; "testcase-sdk")]
    #[test_case(setup_transaction_view_receive_and_buffer; "testcase-view")]
    fn test_receive_and_buffer_failed_alt_resolve<R: ReceiveAndBuffer>(
        setup_receive_and_buffer: impl FnOnce(
            Receiver<BankingPacketBatch>,
            Arc<RwLock<BankForks>>,
            HashSet<Pubkey>,
        ) -> (R, R::Container),
    ) {
        let (sender, receiver) = unbounded();
        let (bank_forks, mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container) =
            setup_receive_and_buffer(receiver, bank_forks.clone(), HashSet::default());

        let to_pubkey = Pubkey::new_unique();
        let transaction = VersionedTransaction::try_new(
            VersionedMessage::V0(
                v0::Message::try_compile(
                    &mint_keypair.pubkey(),
                    &[system_instruction::transfer(
                        &mint_keypair.pubkey(),
                        &to_pubkey,
                        1,
                    )],
                    &[AddressLookupTableAccount {
                        key: Pubkey::new_unique(), // will fail if using **bank** to lookup
                        addresses: vec![to_pubkey],
                    }],
                    bank_forks.read().unwrap().root_bank().last_blockhash(),
                )
                .unwrap(),
            ),
            &[&mint_keypair],
        )
        .unwrap();
        let packet_batches = Arc::new(to_packet_batches(&[transaction], 1));
        sender.send(packet_batches).unwrap();

        let ReceivingStats { num_received, .. } = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();
        assert_eq!(num_received, 1);

        let BufferStats {
            num_received,
            num_buffered,
            num_dropped_on_sanitization,
            ..
        } = receive_and_buffer.maybe_queue_batch(&mut container, &BufferedPacketsDecision::Hold);
        assert_eq!(num_received, 0);
        assert_eq!(num_buffered, 0);
        assert_eq!(num_dropped_on_sanitization, 1);

        verify_container(&mut container, 0);
    }

    #[test_case(setup_sanitized_transaction_receive_and_buffer; "testcase-sdk")]
    #[test_case(setup_transaction_view_receive_and_buffer; "testcase-view")]
    fn test_receive_and_buffer_simple_transfer<R: ReceiveAndBuffer>(
        setup_receive_and_buffer: impl FnOnce(
            Receiver<BankingPacketBatch>,
            Arc<RwLock<BankForks>>,
            HashSet<Pubkey>,
        ) -> (R, R::Container),
    ) {
        let (sender, receiver) = unbounded();
        let (bank_forks, mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container) =
            setup_receive_and_buffer(receiver, bank_forks.clone(), HashSet::default());
        let transaction = transfer(
            &mint_keypair,
            &Pubkey::new_unique(),
            1,
            bank_forks.read().unwrap().root_bank().last_blockhash(),
        );
        let packet_batches = Arc::new(to_packet_batches(&[transaction], 1));
        sender.send(packet_batches).unwrap();

        let ReceivingStats { num_received, .. } = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();

        assert_eq!(num_received, 1);

        // We need to queue the batch
        receive_and_buffer.maybe_queue_batch(&mut container, &BufferedPacketsDecision::Hold);

        verify_container(&mut container, 1);
    }

    #[test_case(setup_sanitized_transaction_receive_and_buffer; "testcase-sdk")]
    #[test_case(setup_transaction_view_receive_and_buffer; "testcase-view")]
    fn test_receive_and_buffer_overfull<R: ReceiveAndBuffer>(
        setup_receive_and_buffer: impl FnOnce(
            Receiver<BankingPacketBatch>,
            Arc<RwLock<BankForks>>,
            HashSet<Pubkey>,
        ) -> (R, R::Container),
    ) {
        let (sender, receiver) = unbounded();
        let (bank_forks, mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container) =
            setup_receive_and_buffer(receiver, bank_forks.clone(), HashSet::default());

        let num_transactions = 3 * TEST_CONTAINER_CAPACITY;
        let transactions = Vec::from_iter((0..num_transactions).map(|_| {
            transfer(
                &mint_keypair,
                &Pubkey::new_unique(),
                1,
                bank_forks.read().unwrap().root_bank().last_blockhash(),
            )
        }));

        let packet_batches = Arc::new(to_packet_batches(&transactions, 17));
        sender.send(packet_batches).unwrap();

        let ReceivingStats { num_received, .. } = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();

        assert_eq!(num_received, num_transactions);

        // We need to queue the batch
        let BufferStats {
            num_dropped_on_capacity, ..
        } = receive_and_buffer.maybe_queue_batch(&mut container, &BufferedPacketsDecision::Hold);

        assert!(num_dropped_on_capacity > 0);
        verify_container(&mut container, TEST_CONTAINER_CAPACITY);
    }

    #[test_case(setup_sanitized_transaction_receive_and_buffer; "testcase-sdk")]
    #[test_case(setup_transaction_view_receive_and_buffer; "testcase-view")]
    fn test_receive_blacklisted_account<R: ReceiveAndBuffer>(
        setup_receive_and_buffer: impl FnOnce(
            Receiver<BankingPacketBatch>,
            Arc<RwLock<BankForks>>,
            HashSet<Pubkey>,
        ) -> (R, R::Container),
    ) {
        let (sender, receiver) = unbounded();
        let (bank_forks, mint_keypair) = test_bank_forks();

        let blacklisted_account = Keypair::new();

        let (mut receive_and_buffer, mut container) = setup_receive_and_buffer(
            receiver,
            bank_forks.clone(),
            HashSet::from_iter(vec![blacklisted_account.pubkey()]),
        );
        let ok_tx = transfer(
            &mint_keypair,
            &Pubkey::new_unique(),
            1,
            bank_forks.read().unwrap().root_bank().last_blockhash(),
        );
        let blacklisted_tx = transfer(
            &mint_keypair,
            &blacklisted_account.pubkey(),
            1,
            bank_forks.read().unwrap().root_bank().last_blockhash(),
        );
        let packet_batches = Arc::new(to_packet_batches(&[ok_tx, blacklisted_tx], 2));
        sender.send(packet_batches).unwrap();

        let ReceivingStats { num_received, .. } = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();

        assert_eq!(num_received, 2);

        // We need to queue the batch
        let BufferStats {
            num_dropped_on_blacklisted_account, num_buffered, ..
        } = receive_and_buffer.maybe_queue_batch(&mut container, &BufferedPacketsDecision::Hold);
        assert_eq!(num_buffered, 1);
        assert_eq!(num_dropped_on_blacklisted_account, 1);
        verify_container(&mut container, 1);
    }

    #[test_case(setup_sanitized_transaction_receive_and_buffer_with_batching; "testcase-sdk")]
    #[test_case(setup_transaction_view_receive_and_buffer_with_batching; "testcase-view")]
    fn test_receive_and_buffer_batching<R: ReceiveAndBuffer>(
        setup_receive_and_buffer: impl FnOnce(
            Receiver<BankingPacketBatch>,
            Arc<RwLock<BankForks>>,
            HashSet<Pubkey>,
        ) -> (R, R::Container),
    ) {
        let (sender, receiver) = unbounded();
        let (bank_forks, mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container) =
            setup_receive_and_buffer(receiver, bank_forks.clone(), HashSet::default());

        let transaction = transfer(
            &mint_keypair,
            &Pubkey::new_unique(),
            1,
            bank_forks.read().unwrap().root_bank().last_blockhash(),
        );
        let packet_batches = Arc::new(to_packet_batches(&[transaction], 1));
        sender.send(packet_batches).unwrap();

        let ReceivingStats { num_received, ..
        } = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();
        assert_eq!(num_received, 1);

        // Do another transfer after 2 ms
        sleep(Duration::from_millis(2));
        let transaction = transfer(
            &mint_keypair,
            &Pubkey::new_unique(),
            1,
            bank_forks.read().unwrap().root_bank().last_blockhash(),
        );
        let packet_batches = Arc::new(to_packet_batches(&[transaction], 1));
        sender.send(packet_batches).unwrap();

        let ReceivingStats { num_received, ..
        } = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();
        assert_eq!(num_received, 1);

        // Sleep the batching period
        sleep(BATCH_PERIOD);

        // We need to queue the batch for sdk
        receive_and_buffer.maybe_queue_batch(&mut container, &BufferedPacketsDecision::Hold);

        // After both received and buffered, the result should be the same, the container should have 2 transactions
        verify_container(&mut container, 2);
    }
}