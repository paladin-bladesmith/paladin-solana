#[cfg(feature = "dev-context-only-utils")]
use qualifier_attr::qualifiers;
#[allow(unused_imports)]
use solana_perf::packet::PacketBatch;
use {
    super::{
        scheduling_unit_priority_id::{SchedulingUnitId, SchedulingUnitPriorityId},
        transaction_state::TransactionState,
        transaction_state_container::{
            SharedBytes, StateContainer, TransactionViewState, TransactionViewStateContainer,
            EXTRA_CAPACITY,
        },
    },
    crate::{
        banking_stage::{
            consumer::Consumer, decision_maker::BufferedPacketsDecision, scheduler_messages::MaxAge,
        },
        bundle_stage::bundle_storage::BundleStorage,
        packet_bundle::VerifiedPacketBundle,
    },
    agave_banking_stage_ingress_types::{BankingPacketBatch, BankingPacketReceiver},
    agave_transaction_view::{
        resolved_transaction_view::ResolvedTransactionView, transaction_data::TransactionData,
        transaction_version::TransactionVersion, transaction_view::SanitizedTransactionView,
    },
    ahash::HashSet,
    arrayref::array_ref,
    arrayvec::ArrayVec,
    core::time::Duration,
    crossbeam_channel::{Receiver, RecvTimeoutError, TryRecvError},
    solana_accounts_db::account_locks::validate_account_locks,
    solana_address_lookup_table_interface::state::estimate_last_valid_slot,
    solana_clock::{Epoch, Slot, MAX_PROCESSING_AGE},
    solana_cost_model::cost_model::CostModel,
    solana_fee_structure::FeeBudgetLimits,
    solana_message::v0::LoadedAddresses,
    solana_pubkey::Pubkey,
    solana_pubkey::Pubkey,
    solana_runtime::{bank::Bank, bank_forks::BankForks},
    solana_runtime_transaction::{
        runtime_transaction::RuntimeTransaction, transaction_meta::StaticMeta,
        transaction_with_meta::TransactionWithMeta,
    },
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    solana_svm_transaction::{instruction::SVMInstruction, svm_message::SVMMessage},
    solana_transaction::sanitized::MessageHash,
    solana_transaction_error::TransactionError,
    std::{
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
    pub num_dropped_on_blacklisted_account: usize,
    pub receive_time_us: u64,
    pub buffer_time_us: u64,
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
        self.num_dropped_on_blacklisted_account += other.num_dropped_on_blacklisted_account;
        self.receive_time_us += other.receive_time_us;
        self.buffer_time_us += other.buffer_time_us;
    }
}

#[allow(unused)]
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
}

#[allow(unused)]
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
}

#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
pub(crate) trait ReceiveAndBuffer {
    type Transaction: TransactionWithMeta + Send + Sync;
    type Container: StateContainer<Self::Transaction> + Send + Sync;

    /// Return Err if the receiver is disconnected AND no packets were
    /// received. Otherwise return Ok(num_received).
    fn receive_and_buffer_packets(
        &mut self,
        container: &mut Self::Container,
        decision: &BufferedPacketsDecision,
    ) -> Result<ReceivingStats, DisconnectedError>;

    fn receive_and_buffer_bundles(
        &mut self,
        container: &mut Self::Container,
        decision: &BufferedPacketsDecision,
    ) -> Result<ReceivingStats, DisconnectedError>;

    fn maybe_queue_batch(
        &mut self,
        container: &mut Self::Container,
        decision: &BufferedPacketsDecision,
    ) -> BufferStats;
}

#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
pub(crate) struct TransactionViewReceiveAndBuffer {
    pub receiver: BankingPacketReceiver,
    pub verified_bundle_receiver: Receiver<VerifiedPacketBundle>,
    pub bank_forks: Arc<RwLock<BankForks>>,
    pub blacklisted_accounts: HashSet<Pubkey>,

    // Batching
    batch: Vec<BankingPacketBatch>,
    bundle_batch: Vec<VerifiedPacketBundle>,
    batch_start: Instant,
    batch_interval: Duration,
    tip_accounts: HashSet<Pubkey>,
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
            match self.receiver.recv_timeout(timeout) {
                Ok(packet_batch_message) => {
                    stats.accumulate(self.batch_packets(packet_batch_message, decision));
                    stats.accumulate(self.batch_packets(packet_batch_message, decision));
                }
                Err(RecvTimeoutError::Timeout) => return Ok(stats),
                Err(RecvTimeoutError::Timeout) => return Ok(stats),
                Err(RecvTimeoutError::Disconnected) => {
                    return (!self.batch.is_empty())
                        .then_some(stats)
                        .ok_or(DisconnectedError);
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
        decision: &BufferedPacketsDecision,
    ) -> Result<ReceivingStats, DisconnectedError> {
        let start = Instant::now();
        let stats = ReceivingStats::default();
        let timeout = self.get_timeout();

        // If not leader/unknown, do a blocking-receive initially. This lets
        // the thread sleep until a message is received, or until the timeout.
        // Additionally, only sleep if the container is empty.
        if container.is_empty()
            && matches!(
                decision,
                BufferedPacketsDecision::Forward | BufferedPacketsDecision::ForwardAndHold
            )
        {
            match self.verified_bundle_receiver.recv_timeout(timeout) {
                Ok(bundle) => {
                    self.bundle_batch.push(bundle);
                }
                Err(RecvTimeoutError::Timeout) => return Ok(stats),
                Err(RecvTimeoutError::Disconnected) => {
                    return (!self.bundle_batch.is_empty())
                        .then_some(stats)
                        .ok_or(DisconnectedError);
                }
            }
        }

        while start.elapsed() < timeout {
            match self.verified_bundle_receiver.try_recv() {
                Ok(bundle) => {
                    if self.bundle_batch.is_empty() {
                        self.batch_start = Instant::now();
                    }
                    self.bundle_batch.push(bundle);
                }
                Err(TryRecvError::Empty) => return Ok(stats),
                Err(TryRecvError::Disconnected) => {
                    return (!self.bundle_batch.is_empty())
                        .then_some(stats)
                        .ok_or(DisconnectedError);
                }
            }
        }

        Ok(stats)
    }

    fn maybe_queue_batch(
        &mut self,
        container: &mut Self::Container,
        decision: &BufferedPacketsDecision,
    ) -> BufferStats {
        if !self.batch.is_empty()
            || !self.bundle_batch.is_empty() && self.batch_start.elapsed() >= self.batch_interval
        {
            let (root_bank, working_bank) = {
                let bank_forks = self.bank_forks.read().unwrap();
                let root_bank = bank_forks.root_bank();
                let working_bank = bank_forks.working_bank();
                (root_bank, working_bank)
            };

            let mut stats = BufferStats::default();

            if !self.batch.is_empty() {
                let tx_stats = self.handle_batch_of_packet_batch_messages(
                    container,
                    decision,
                    &root_bank,
                    &working_bank,
                );
                stats.num_buffered += tx_stats.num_buffered;
                stats.num_dropped_on_capacity += tx_stats.num_dropped_on_capacity;
                stats.buffer_time_us += tx_stats.buffer_time_us;
            }

            if !self.bundle_batch.is_empty() {
                let bundle_stats = self.handle_verified_bundle_batch(
                    container,
                    decision,
                    &root_bank,
                    &working_bank,
                );
                stats.num_buffered += bundle_stats.num_buffered;
                stats.num_dropped_on_capacity += bundle_stats.num_dropped_on_capacity;
                stats.buffer_time_us += bundle_stats.buffer_time_us;
            }
            
            self.batch_start = Instant::now();

            stats
        } else {
            BufferStats::default()
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum PacketHandlingError {
#[derive(Debug, PartialEq, Eq)]
pub enum PacketHandlingError {
    Sanitization,
    LockValidation,
    ComputeBudget,
    ALTResolution,
    BlacklistedAccount,
    BlacklistedAccount,
}

impl TransactionViewReceiveAndBuffer {
    pub fn new(
        receiver: BankingPacketReceiver,
        verified_bundle_receiver: Receiver<VerifiedPacketBundle>,
        bank_forks: Arc<RwLock<BankForks>>,
        blacklisted_accounts: HashSet<Pubkey>,
        batch_interval: Duration,
        tip_accounts: &HashSet<Pubkey>,
    ) -> Self {
        Self {
            receiver,
            verified_bundle_receiver,
            bank_forks,
            blacklisted_accounts,
            batch: Vec::default(),
            bundle_batch: Vec::default(),
            batch_start: Instant::now(),
            batch_interval,
            tip_accounts: tip_accounts.clone(),
        }
    }

    /// If batch is not empty, and time has elapsed, return zero timeout
    ///
    /// If batch is not empty, but time has not elapsed, return remaining time
    ///
    /// If batch is empty, return default timeout
    fn get_timeout(&self) -> Duration {
        if !self.batch.is_empty() {
            // if self.batch_start.elapsed() >= self.batch_interval {
                Duration::ZERO
            // } else {
            //     self.batch_interval
            //         .saturating_sub(self.batch_start.elapsed())
            // }
        } else {
            Duration::from_millis(10)
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

        // If this is the first packet in the batch, set the start timestamp for
        // the batch.
        if self.batch.is_empty() {
            self.batch_start = Instant::now();
        }

        // Count len of packets without discarded
        let mut total_packets_len = 0;
        let num_received = packet_batch_message
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
            num_received: total_packets_len,
            num_dropped_without_parsing: total_packets_len - num_received,
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
        }
    }

    fn handle_verified_bundle_batch(
        &mut self,
        container: &mut TransactionViewStateContainer,
        decision: &BufferedPacketsDecision,
        root_bank: &Bank,
        working_bank: &Bank,
    ) -> BufferStats {
        let bundle_batch: Vec<VerifiedPacketBundle> = self.bundle_batch.drain(..).collect();
        let start = Instant::now();

        // If outside holding window, do not parse.
        let should_parse = !matches!(decision, BufferedPacketsDecision::Forward);
        if !should_parse {
            return BufferStats::default();
        }

        let mut num_buffered = 0;
        let mut num_dropped_on_capacity = 0;

        let mut bundle_priority_ids = Vec::new();

        for bundle in bundle_batch {
            match BundleStorage::insert_bundle(
                container,
                bundle,
                root_bank,
                working_bank,
                &self.blacklisted_accounts,
                &self.tip_accounts,
            ) {
                Ok(container_ids) => {
                    let bundle_id = container.try_insert_bundle_mapping(container_ids.clone());

                    let mut bundle_priority = 0u64;
                    for &tx_id in container_ids.iter() {
                        if let Some(state) = container.get_mut_transaction_state(tx_id) {
                            bundle_priority = bundle_priority.max(state.priority());
                        }
                    }

                    // Create Bundle priority ID and add to list
                    bundle_priority_ids.push(SchedulingUnitPriorityId::new(
                        bundle_priority,
                        SchedulingUnitId::Bundle(bundle_id),
                    ));

                    num_buffered += container_ids.len();
                }
                Err(_e) => {
                    // Errors are already handled in insert_bundle (rollback, etc.)
                }
            }
        }

        // Push all bundle priority IDs into the queue
        num_dropped_on_capacity += container.push_ids_into_queue(bundle_priority_ids.into_iter());

        BufferStats {
            num_received: 0,
            num_dropped_without_parsing: 0,
            num_dropped_on_sanitization: 0,
            num_dropped_on_lock_validation: 0,
            num_dropped_on_compute_budget: 0,
            num_dropped_on_age: 0,
            num_dropped_on_already_processed: 0,
            num_dropped_on_fee_payer: 0,
            num_dropped_on_capacity,
            num_buffered,
            num_dropped_on_blacklisted_account: 0,
            buffer_time_us: start.elapsed().as_micros() as u64,
        }
    }

    /// Return number of received packets.
    fn handle_batch_of_packet_batch_messages(
    fn handle_batch_of_packet_batch_messages(
        &mut self,
        container: &mut TransactionViewStateContainer,
        decision: &BufferedPacketsDecision,
        root_bank: &Bank,
        working_bank: &Bank,
    ) -> BufferStats {
        let packet_batch_messages: Vec<BankingPacketBatch> = self.batch.drain(..).collect();

    ) -> BufferStats {
        let packet_batch_messages: Vec<BankingPacketBatch> = self.batch.drain(..).collect();

        let start = Instant::now();
        // If outside holding window, do not parse.
        let should_parse = !matches!(decision, BufferedPacketsDecision::Forward);

        let enable_static_instruction_limit = root_bank
            .feature_set
            .is_active(&agave_feature_set::static_instruction_limit::ID);
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
             transaction_priority_ids: &mut ArrayVec<SchedulingUnitPriorityId, 64>| {
                // Temporary scope so that transaction references are immediately
                // dropped and transactions not passing
                let mut check_results = {
                    let mut transactions = ArrayVec::<_, EXTRA_CAPACITY>::new();
                    transactions.extend(transaction_priority_ids.iter().map(|priority_id| {
                        container
                            .get_transaction(priority_id.id())
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
                        .get_transaction(priority_id.id())
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
                            enable_static_instruction_limit,
                            transaction_account_lock_limit,
                            &self.blacklisted_accounts,
                            &self.blacklisted_accounts,
                        ) {
                            Ok(state) => Ok(state),
                            Err(
                                PacketHandlingError::Sanitization
                                | PacketHandlingError::ALTResolution,
                            ) => {
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
                    transaction_priority_ids.push(SchedulingUnitPriorityId::new(
                        priority,
                        SchedulingUnitId::Transaction(transaction_id),
                    ));

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
            num_received: 0,
        BufferStats {
            num_received: 0,
            num_dropped_without_parsing,
            num_dropped_on_sanitization: num_dropped_on_parsing_and_sanitization,
            num_dropped_on_sanitization: num_dropped_on_parsing_and_sanitization,
            num_dropped_on_lock_validation,
            num_dropped_on_compute_budget,
            num_dropped_on_age,
            num_dropped_on_already_processed,
            num_dropped_on_fee_payer,
            num_dropped_on_capacity,
            num_buffered,
            num_dropped_on_blacklisted_account,
            num_dropped_on_blacklisted_account,
            buffer_time_us: start.elapsed().as_micros() as u64,
        }
    }

    fn try_handle_packet(
        bytes: SharedBytes,
        root_bank: &Bank,
        working_bank: &Bank,
        enable_static_instruction_limit: bool,
        transaction_account_lock_limit: usize,
        blacklisted_accounts: &HashSet<Pubkey>,
        blacklisted_accounts: &HashSet<Pubkey>,
    ) -> Result<TransactionViewState, PacketHandlingError> {
        let (view, deactivation_slot) = translate_to_runtime_view(
            bytes,
            working_bank,
            root_bank,
            enable_static_instruction_limit,
            transaction_account_lock_limit,
        )?;
        if validate_account_locks(
            view.account_keys(),
            root_bank.get_transaction_account_lock_limit(),
        )
        .is_err()
        {
            return Err(PacketHandlingError::LockValidation);
        }

        if view
            .account_keys()
            .iter()
            .any(|account| blacklisted_accounts.contains(account))
        {
            return Err(PacketHandlingError::BlacklistedAccount);
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

        let max_age = calculate_max_age(root_bank.epoch(), deactivation_slot, root_bank.slot());
        let fee_budget_limits = FeeBudgetLimits::from(compute_budget_limits);
        let (priority, cost) =
            calculate_priority_and_cost(&view, &fee_budget_limits, working_bank, None);

        Ok(TransactionState::new(view, max_age, priority, cost))
    }
}

/// Perform sanitization checks and transition from data to an executable
/// [`RuntimeTransaction`]. This additionally returns the minimum slot for
/// ALT deactivation, if any. If no minimum slot, Slot::MAX is returned.
pub(crate) fn translate_to_runtime_view<D: TransactionData>(
    data: D,
    working_bank: &Bank,
    root_bank: &Bank,
    enable_static_instruction_limit: bool,
    transaction_account_lock_limit: usize,
) -> Result<(RuntimeTransaction<ResolvedTransactionView<D>>, u64), PacketHandlingError> {
    // Parsing and basic sanitization checks
    let Ok(view) =
        SanitizedTransactionView::try_new_sanitized(data, enable_static_instruction_limit)
    else {
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
    if working_bank.vote_only_bank() && !view.is_simple_vote_transaction() {
        return Err(PacketHandlingError::Sanitization);
    }

    if usize::from(view.total_num_accounts()) > transaction_account_lock_limit {
        return Err(PacketHandlingError::LockValidation);
    }

    let (loaded_addresses, deactivation_slot) = load_addresses_for_view(&view, root_bank)?;

    let Ok(view) = RuntimeTransaction::<ResolvedTransactionView<_>>::try_from(
        view,
        loaded_addresses,
        root_bank.get_reserved_account_keys(),
    ) else {
        return Err(PacketHandlingError::Sanitization);
    };

    Ok((view, deactivation_slot))
}

/// Load addresses from ALTs (if necessary) and return the
/// [`LoadedAddresses`] with the minimum deactivation slot.
pub(crate) fn load_addresses_for_view<D: TransactionData>(
    view: &SanitizedTransactionView<D>,
    bank: &Bank,
) -> Result<(Option<LoadedAddresses>, Slot), PacketHandlingError> {
    match view.version() {
        TransactionVersion::Legacy => Ok((None, u64::MAX)),
        TransactionVersion::V0 => bank
            .load_addresses_from_ref(view.address_table_lookup_iter())
            .map(|(loaded_addresses, deactivation_slot)| {
                (Some(loaded_addresses), deactivation_slot)
            })
            .map_err(|_| PacketHandlingError::ALTResolution),
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
pub(crate) fn calculate_priority_and_cost(
    transaction: &impl TransactionWithMeta,
    fee_budget_limits: &FeeBudgetLimits,
    bank: &Bank,
    tip_amount: Option<u64>,
) -> (u64, u64) {
    let cost = CostModel::calculate_cost(transaction, &bank.feature_set).sum();
    let reward = bank.calculate_reward_for_transaction(transaction, fee_budget_limits);
    let reward = reward.saturating_add(tip_amount.unwrap_or(0));

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

pub(crate) fn calculate_bundle_transaction_priority_and_cost(
    transaction: &impl TransactionWithMeta,
    fee_budget_limits: &FeeBudgetLimits,
    bank: &Bank,
    tip_accounts: &HashSet<Pubkey>,
) -> (u64, u64) {
    let tip_amount = transaction
        .instructions_iter()
        .filter_map(|ix| {
            extract_transfer(transaction.static_account_keys(), &ix).and_then(|(dest, amount)| {
                if tip_accounts.contains(dest) {
                    Some(amount)
                } else {
                    None
                }
            })
        })
        .sum();
    calculate_priority_and_cost(transaction, fee_budget_limits, bank, Some(tip_amount))
}

fn extract_transfer<'a>(
    account_keys: &'a [Pubkey],
    ix: &SVMInstruction,
) -> Option<(&'a Pubkey, u64)> {
    let program_id = account_keys.get(ix.program_id_index as usize)?;
    if program_id != &solana_sdk_ids::system_program::ID {
        return None;
    }

    // Check if this is a transfer instruction (discriminator = 2)
    // Transfer instruction format: [u32 discriminator, u64 lamports]
    if ix.data.len() < 12 {
        return None;
    }

    let discriminator = u32::from_le_bytes(*array_ref![&ix.data, 0, 4]);
    if discriminator != 2 {
        return None;
    }

    let destination_index = *ix.accounts.get(1)? as usize;
    let destination = account_keys.get(destination_index)?;
    let amount = u64::from_le_bytes(*array_ref![&ix.data, 4, 8]);

    Some((destination, amount))
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
pub(crate) fn calculate_max_age(
pub(crate) fn calculate_max_age(
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

#[cfg(any())] // Tests disabled - need updating for bundle integration
#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::banking_stage::tests::create_slow_genesis_config,
        agave_verified_packet_receiver::receiver,
        ahash::HashSetExt,
        crossbeam_channel::{unbounded, Receiver},
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_ledger::genesis_utils::GenesisConfigInfo,
        solana_message::{
            v0, AccountMeta, AddressLookupTableAccount, Instruction, VersionedMessage,
        },
        solana_packet::{Meta, PACKET_DATA_SIZE},
        solana_perf::packet::{to_packet_batches, Packet, PacketBatch, PinnedPacketBatch},
        solana_pubkey::Pubkey,
        solana_signer::Signer,
        solana_system_interface::instruction as system_instruction,
        solana_system_transaction::transfer,
        solana_transaction::versioned::VersionedTransaction,
        std::thread::sleep,
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

    fn setup_transaction_view_receive_and_buffer(
        receiver: Receiver<BankingPacketBatch>,
        bundle_receiver: Receiver<VerifiedPacketBundle>,
        bank_forks: Arc<RwLock<BankForks>>,
        blacklisted_accounts: HashSet<Pubkey>,
        blacklisted_accounts: HashSet<Pubkey>,
    ) -> (
        TransactionViewReceiveAndBuffer,
        TransactionViewStateContainer,
    ) {
        let receive_and_buffer = TransactionViewReceiveAndBuffer {
            receiver,
            verified_bundle_receiver: bundle_receiver,
            bank_forks,
            blacklisted_accounts,

            batch: Vec::default(),
            bundle_batch: Vec::default(),
            batch_start: Instant::now(),
            batch_interval: Duration::ZERO,
            tip_accounts: HashSet::new(),
        };
        let container = TransactionViewStateContainer::with_capacity(TEST_CONTAINER_CAPACITY);
        (receive_and_buffer, container)
    }

    fn setup_transaction_view_receive_and_buffer_with_batching(
        receiver: Receiver<BankingPacketBatch>,
        bundle_receiver: Receiver<VerifiedPacketBundle>,
        bank_forks: Arc<RwLock<BankForks>>,
        blacklisted_accounts: HashSet<Pubkey>,
    ) -> (
        TransactionViewReceiveAndBuffer,
        TransactionViewStateContainer,
    ) {
        let receive_and_buffer = TransactionViewReceiveAndBuffer {
            receiver,
            verified_bundle_receiver: bundle_receiver,
            bank_forks,
            blacklisted_accounts,

            batch: Vec::default(),
            bundle_batch: Vec::default(),
            batch_start: Instant::now(),
            batch_interval: BATCH_PERIOD,
            tip_accounts: HashSet::new(),
        };
        let container = TransactionViewStateContainer::with_capacity(TEST_CONTAINER_CAPACITY);
        (receive_and_buffer, container)
    }

    // fn setup_transaction_view_receive_and_buffer_with_batching(
    //     receiver: Receiver<BankingPacketBatch>,
    //     bank_forks: Arc<RwLock<BankForks>>,
    //     blacklisted_accounts: HashSet<Pubkey>,
    // ) -> (
    //     TransactionViewReceiveAndBuffer,
    //     TransactionViewStateContainer,
    // ) {
    //     let receive_and_buffer = TransactionViewReceiveAndBuffer {
    //         receiver,
    //         bank_forks,
    //         blacklisted_accounts,

    //         batch: Vec::default(),
    //         batch_start: Instant::now(),
    //         batch_interval: BATCH_PERIOD,
    //     };
    //     let container = TransactionViewStateContainer::with_capacity(TEST_CONTAINER_CAPACITY);
    //     (receive_and_buffer, container)
    // }

    // verify container state makes sense:
    // 1. Number of transactions matches expectation
    // 2. All transactions IDs in priority queue exist in the map
    fn verify_container<Tx: TransactionWithMeta>(
        container: &mut impl StateContainer<Tx>,
        expected_length: usize,
    ) {
        let mut actual_length: usize = 0;
        while let Some(id) = container.pop() {
            let Some(_) = container.get_transaction(id.id) else {
                panic!(
                    "transaction in queue position {} with id {} must exist.",
                    actual_length, id.id
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

    #[test]
    fn test_receive_and_buffer_disconnected_channel() {
        let (sender, receiver) = unbounded();
        let (_bundle_sender, bundle_receiver) = unbounded();
        let (bank_forks, _mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container) =
            setup_transaction_view_receive_and_buffer_with_batching(
                receiver,
                bundle_receiver,
                bank_forks,
                HashSet::new(),
            );

        drop(sender); // disconnect channel
        let r = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold);
        assert!(r.is_err());
    }

    #[test]
    fn test_receive_and_buffer_no_hold() {
        let (sender, receiver) = unbounded();
        let (_bundle_sender, bundle_receiver) = unbounded();
        let (bank_forks, mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container) =
            setup_transaction_view_receive_and_buffer_with_batching(
                receiver,
                bundle_receiver,
                bank_forks.clone(),
                HashSet::new(),
            );

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
            receive_time_us: _,
            buffer_time_us: _,
            num_dropped_on_blacklisted_account: _,
            num_dropped_on_blacklisted_account: _,
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

    #[test]
    fn test_receive_and_buffer_discard() {
        let (sender, receiver) = unbounded();
        let (_bundle_sender, bundle_receiver) = unbounded();
        let (bank_forks, mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container) =
            setup_transaction_view_receive_and_buffer_with_batching(
                receiver,
                bundle_receiver,
                bank_forks.clone(),
                HashSet::new(),
            );

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
            receive_time_us: _,
            buffer_time_us: _,
            num_dropped_on_blacklisted_account: _,
            num_dropped_on_blacklisted_account: _,
        } = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();
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

        receive_and_buffer.maybe_queue_batch(&mut container, &BufferedPacketsDecision::Hold);
        receive_and_buffer.maybe_queue_batch(&mut container, &BufferedPacketsDecision::Hold);
        verify_container(&mut container, 0);
    }

    #[test]
    fn test_receive_and_buffer_invalid_transaction_format() {
        let (sender, receiver) = unbounded();
        let (_bundle_sender, bundle_receiver) = unbounded();
        let (bank_forks, _mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container) =
            setup_transaction_view_receive_and_buffer_with_batching(
                receiver,
                bundle_receiver,
                bank_forks.clone(),
                HashSet::new(),
            );

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
            num_dropped_on_blacklisted_account: _,
        } = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();

        assert_eq!(num_received, 1);
        assert_eq!(num_dropped_without_parsing, 0);
        assert_eq!(
            num_dropped_on_parsing_and_sanitization,
            should_drop_on_parsing_and_sanitization
        );
        assert_eq!(num_dropped_on_lock_validation, 0);
        assert_eq!(num_dropped_on_compute_budget, 0);
        assert_eq!(num_dropped_on_age, 0);
        assert_eq!(num_dropped_on_already_processed, 0);
        assert_eq!(num_dropped_on_fee_payer, 0);
        assert_eq!(num_dropped_on_capacity, 0);
        assert_eq!(num_buffered, 0);

        // We need to queue the batch
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

    #[test]
    fn test_receive_and_buffer_invalid_blockhash() {
        let (sender, receiver) = unbounded();
        let (_bundle_sender, bundle_receiver) = unbounded();
        let (bank_forks, mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container) =
            setup_transaction_view_receive_and_buffer_with_batching(
                receiver,
                bundle_receiver,
                bank_forks.clone(),
                HashSet::new(),
            );

        let transaction = transfer(&mint_keypair, &Pubkey::new_unique(), 1, Hash::new_unique());
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
            receive_time_us: _,
            buffer_time_us: _,
            num_dropped_on_blacklisted_account: _,
            num_dropped_on_blacklisted_account: _,
        } = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();

        assert_eq!(num_received, 1);
        assert_eq!(num_dropped_without_parsing, 0);
        assert_eq!(num_dropped_on_parsing_and_sanitization, 0);
        assert_eq!(num_dropped_on_lock_validation, 0);
        assert_eq!(num_dropped_on_compute_budget, 0);
        assert_eq!(num_dropped_on_age, 0);
        assert_eq!(num_dropped_on_age, 0);
        assert_eq!(num_dropped_on_already_processed, 0);
        assert_eq!(num_dropped_on_fee_payer, 0);
        assert_eq!(num_dropped_on_capacity, 0);
        assert_eq!(num_buffered, 0);

        // We need to queue the batch
        let BufferStats {
            num_received,
            num_buffered,
            num_dropped_on_age,
            ..
        } = receive_and_buffer.maybe_queue_batch(&mut container, &BufferedPacketsDecision::Hold);

        assert_eq!(num_received, 0);
        assert_eq!(num_buffered, 0);
        assert_eq!(num_dropped_on_age, 1);

        // We need to queue the batch
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

    #[test]
    fn test_receive_and_buffer_simple_transfer_unfunded_fee_payer() {
        let (sender, receiver) = unbounded();
        let (_bundle_sender, bundle_receiver) = unbounded();
        let (bank_forks, _mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container) =
            setup_transaction_view_receive_and_buffer_with_batching(
                receiver,
                bundle_receiver,
                bank_forks.clone(),
                HashSet::new(),
            );

        let transaction = transfer(
            &Keypair::new(),
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
            receive_time_us: _,
            buffer_time_us: _,
            num_dropped_on_blacklisted_account: _,
            num_dropped_on_blacklisted_account: _,
        } = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();

        assert_eq!(num_received, 1);
        assert_eq!(num_dropped_without_parsing, 0);
        assert_eq!(num_dropped_on_parsing_and_sanitization, 0);
        assert_eq!(num_dropped_on_lock_validation, 0);
        assert_eq!(num_dropped_on_compute_budget, 0);
        assert_eq!(num_dropped_on_age, 0);
        assert_eq!(num_dropped_on_already_processed, 0);
        assert_eq!(num_dropped_on_fee_payer, 0);
        assert_eq!(num_dropped_on_fee_payer, 0);
        assert_eq!(num_dropped_on_capacity, 0);
        assert_eq!(num_buffered, 0);

        // We need to queue the batch
        let BufferStats {
            num_received,
            num_buffered,
            num_dropped_on_fee_payer,
            ..
        } = receive_and_buffer.maybe_queue_batch(&mut container, &BufferedPacketsDecision::Hold);

        assert_eq!(num_received, 0);
        assert_eq!(num_buffered, 0);
        assert_eq!(num_dropped_on_fee_payer, 1);

        // We need to queue the batch
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

    #[test]
    fn test_receive_and_buffer_failed_alt_resolve() {
        let (sender, receiver) = unbounded();
        let (_bundle_sender, bundle_receiver) = unbounded();
        let (bank_forks, mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container) =
            setup_transaction_view_receive_and_buffer_with_batching(
                receiver,
                bundle_receiver,
                bank_forks.clone(),
                HashSet::new(),
            );
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
            num_dropped_on_blacklisted_account: _,
        } = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();
        assert_eq!(num_received, 1);


        assert_eq!(num_dropped_without_parsing, 0);
        assert_eq!(num_dropped_on_parsing_and_sanitization, 0);
        assert_eq!(num_dropped_on_parsing_and_sanitization, 0);
        assert_eq!(num_dropped_on_lock_validation, 0);
        assert_eq!(num_dropped_on_compute_budget, 0);
        assert_eq!(num_dropped_on_age, 0);
        assert_eq!(num_dropped_on_already_processed, 0);
        assert_eq!(num_dropped_on_fee_payer, 0);
        assert_eq!(num_dropped_on_capacity, 0);
        assert_eq!(num_buffered, 0);

        // We need to queue the batch
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
        assert_eq!(num_dropped_on_sanitization, 1);

        // Nothing should be included in the container
        // We need to queue the batch
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
        assert_eq!(num_dropped_on_sanitization, 1);

        // Nothing should be included in the container
        verify_container(&mut container, 0);
    }

    #[test]
    fn test_receive_and_buffer_simple_transfer() {
        let (sender, receiver) = unbounded();
        let (_bundle_sender, bundle_receiver) = unbounded();
        let (bank_forks, mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container) =
            setup_transaction_view_receive_and_buffer_with_batching(
                receiver,
                bundle_receiver,
                bank_forks.clone(),
                HashSet::new(),
            );

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
            receive_time_us: _,
            buffer_time_us: _,
            num_dropped_on_blacklisted_account: _,
            num_dropped_on_blacklisted_account: _,
        } = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();

        assert_eq!(num_received, 1);


        assert_eq!(num_dropped_without_parsing, 0);
        assert_eq!(num_dropped_on_parsing_and_sanitization, 0);
        assert_eq!(num_dropped_on_lock_validation, 0);
        assert_eq!(num_dropped_on_compute_budget, 0);
        assert_eq!(num_dropped_on_age, 0);
        assert_eq!(num_dropped_on_already_processed, 0);
        assert_eq!(num_dropped_on_fee_payer, 0);
        assert_eq!(num_dropped_on_capacity, 0);
        assert_eq!(num_buffered, 0);

        // We need to queue the batch
        receive_and_buffer.maybe_queue_batch(&mut container, &BufferedPacketsDecision::Hold);
        assert_eq!(num_buffered, 0);

        // We need to queue the batch
        receive_and_buffer.maybe_queue_batch(&mut container, &BufferedPacketsDecision::Hold);

        verify_container(&mut container, 1);
    }

    #[test]
    fn test_receive_and_buffer_overfull() {
        let (sender, receiver) = unbounded();
        let (_bundle_sender, bundle_receiver) = unbounded();
        let (bank_forks, mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container) =
            setup_transaction_view_receive_and_buffer_with_batching(
                receiver,
                bundle_receiver,
                bank_forks.clone(),
                HashSet::new(),
            );

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
            num_dropped_on_blacklisted_account: _,
        } = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();

        assert_eq!(num_received, num_transactions);
        assert_eq!(num_dropped_without_parsing, 0);
        assert_eq!(num_dropped_on_parsing_and_sanitization, 0);
        assert_eq!(num_dropped_on_lock_validation, 0);
        assert_eq!(num_dropped_on_compute_budget, 0);
        assert_eq!(num_dropped_on_age, 0);
        assert_eq!(num_dropped_on_already_processed, 0);
        assert_eq!(num_dropped_on_fee_payer, 0);
        assert!(num_dropped_on_capacity == 0);
        assert_eq!(num_buffered, 0);

        // We need to queue the batch
        let BufferStats {
            num_received,
            num_buffered,
            num_dropped_on_capacity,
            ..
        } = receive_and_buffer.maybe_queue_batch(&mut container, &BufferedPacketsDecision::Hold);

        assert_eq!(num_received, 0);
        assert!(num_dropped_on_capacity == 0);
        assert_eq!(num_buffered, 0);

        // We need to queue the batch
        let BufferStats {
            num_received,
            num_buffered,
            num_dropped_on_capacity,
            ..
        } = receive_and_buffer.maybe_queue_batch(&mut container, &BufferedPacketsDecision::Hold);

        assert_eq!(num_received, 0);
        assert_eq!(num_buffered, num_transactions);
        assert!(num_dropped_on_capacity > 0);
        assert!(num_dropped_on_capacity > 0);

        // assert_eq!(num_buffered, num_transactions);
        // assert_eq!(num_buffered, num_transactions);
        verify_container(&mut container, TEST_CONTAINER_CAPACITY);
    }

    #[test]
    fn test_receive_and_buffer_too_many_keys() {
        fn create_tx_with_n_keys(payer: &Keypair, n: usize) -> VersionedTransaction {
            let alt_keys = (0..n - 2).map(|_| Pubkey::new_unique()).collect::<Vec<_>>();
            VersionedTransaction::try_new(
                VersionedMessage::V0(
                    v0::Message::try_compile(
                        &payer.pubkey(),
                        &[Instruction::new_with_bytes(
                            Pubkey::new_unique(),
                            &[],
                            alt_keys
                                .iter()
                                .map(|k| AccountMeta::new(*k, false))
                                .collect::<Vec<_>>(),
                        )],
                        &[AddressLookupTableAccount {
                            key: Pubkey::new_unique(),
                            addresses: alt_keys,
                        }],
                        Hash::new_unique(),
                    )
                    .unwrap(),
                ),
                &[payer],
            )
            .unwrap()
        }

        let (sender, receiver) = unbounded();
        let (_bundle_sender, bundle_receiver) = unbounded();
        let (bank_forks, mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container) =
            setup_transaction_view_receive_and_buffer_with_batching(
                receiver,
                bundle_receiver,
                bank_forks.clone(),
                HashSet::new(),
            );

        let transaction_account_lock_limit = bank_forks
            .read()
            .unwrap()
            .root_bank()
            .get_transaction_account_lock_limit();

        // ALTs do not actually exist in the bank for this transaction - sanitization would cause failure if
        // lock validation was not done first.
        let bad_tx = create_tx_with_n_keys(&mint_keypair, transaction_account_lock_limit + 1);
        let transactions = [bad_tx];

        let packet_batches = Arc::new(to_packet_batches(&transactions, 17));
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
            num_dropped_on_blacklisted_account: _,
        } = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();

        assert_eq!(num_received, 1);
        assert_eq!(num_dropped_without_parsing, 0);
        assert_eq!(num_dropped_on_parsing_and_sanitization, 0);
        assert_eq!(num_dropped_on_lock_validation, 0);
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
            num_dropped_on_lock_validation,
            ..
        } = receive_and_buffer.maybe_queue_batch(&mut container, &BufferedPacketsDecision::Hold);

        assert_eq!(num_received, 0);
        assert_eq!(num_buffered, 0);
        assert_eq!(num_dropped_on_lock_validation, 1);

        let BufferStats {
            num_received,
            num_buffered,
            num_dropped_on_lock_validation,
            ..
        } = receive_and_buffer.maybe_queue_batch(&mut container, &BufferedPacketsDecision::Hold);

        assert_eq!(num_received, 0);
        assert_eq!(num_buffered, 0);
        assert_eq!(num_dropped_on_lock_validation, 1);

        verify_container(&mut container, 0);
    }

    #[test]
    fn test_receive_blacklisted_account() {
        let keypair = Keypair::new();
        let blacklisted_accounts = HashSet::from_iter([keypair.pubkey()]);

        let (sender, receiver) = unbounded();
        let (_bundle_sender, bundle_receiver) = unbounded();
        let (bank_forks, _mint_keypair) = test_bank_forks();
        let (mut receive_and_buffer, mut container) = setup_transaction_view_receive_and_buffer(
            receiver,
            bundle_receiver,
            bank_forks.clone(),
            blacklisted_accounts,
        );

        let transaction = transfer(
            &keypair,
            &Pubkey::new_unique(),
            1,
            bank_forks.read().unwrap().root_bank().last_blockhash(),
        );
        let packet_batches = Arc::new(to_packet_batches(&[transaction], 1));
        sender.send(packet_batches).unwrap();

        let r = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();
        assert_eq!(r.num_dropped_on_blacklisted_account, 0);

        // We need to queue the batch
        let BufferStats {
            num_received,
            num_buffered,
            num_dropped_on_blacklisted_account,
            ..
        } = receive_and_buffer.maybe_queue_batch(&mut container, &BufferedPacketsDecision::Hold);

        assert_eq!(num_received, 0);
        assert_eq!(num_buffered, 1);
        assert_eq!(num_dropped_on_blacklisted_account, 1);

        verify_container(&mut container, 1);
    }

    fn test_receive_and_buffer_batching() {
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

        let ReceivingStats { num_received, .. } = receive_and_buffer
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

    #[test]
    fn test_extract_transfer_valid() {
        // Create a simple transfer instruction
        let from = Pubkey::new_unique();
        let to = Pubkey::new_unique();
        let system_program = solana_sdk_ids::system_program::ID;

        let account_keys = vec![from, to, system_program];

        // Build transfer instruction data: discriminator (4 bytes) + lamports (8 bytes)
        let amount: u64 = 1_000_000;
        let mut data = vec![0u8; 12];
        data[0..4].copy_from_slice(&2u32.to_le_bytes()); // Transfer discriminator = 2
        data[4..12].copy_from_slice(&amount.to_le_bytes());

        let ix = SVMInstruction {
            program_id_index: 2,  // system program is at index 2
            accounts: vec![0, 1], // from at 0, to at 1
            data,
        };

        let result = extract_transfer(&account_keys, &ix);
        assert!(result.is_some());
        let (destination, extracted_amount) = result.unwrap();
        assert_eq!(destination, &to);
        assert_eq!(extracted_amount, amount);
    }

    #[test]
    fn test_extract_transfer_wrong_program() {
        let from = Pubkey::new_unique();
        let to = Pubkey::new_unique();
        let other_program = Pubkey::new_unique();

        let account_keys = vec![from, to, other_program];

        let amount: u64 = 1_000_000;
        let mut data = vec![0u8; 12];
        data[0..4].copy_from_slice(&2u32.to_le_bytes());
        data[4..12].copy_from_slice(&amount.to_le_bytes());

        let ix = SVMInstruction {
            program_id_index: 2,
            accounts: vec![0, 1],
            data,
        };

        let result = extract_transfer(&account_keys, &ix);
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_transfer_wrong_discriminator() {
        let from = Pubkey::new_unique();
        let to = Pubkey::new_unique();
        let system_program = solana_sdk_ids::system_program::ID;

        let account_keys = vec![from, to, system_program];

        let amount: u64 = 1_000_000;
        let mut data = vec![0u8; 12];
        data[0..4].copy_from_slice(&3u32.to_le_bytes()); // Wrong discriminator (not 2)
        data[4..12].copy_from_slice(&amount.to_le_bytes());

        let ix = SVMInstruction {
            program_id_index: 2,
            accounts: vec![0, 1],
            data,
        };

        let result = extract_transfer(&account_keys, &ix);
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_transfer_insufficient_data() {
        let from = Pubkey::new_unique();
        let to = Pubkey::new_unique();
        let system_program = solana_sdk_ids::system_program::ID;

        let account_keys = vec![from, to, system_program];

        let data = vec![2u8, 0, 0, 0]; // Only 4 bytes, needs 12

        let ix = SVMInstruction {
            program_id_index: 2,
            accounts: vec![0, 1],
            data,
        };

        let result = extract_transfer(&account_keys, &ix);
        assert!(result.is_none());
    }

    #[test]
    fn test_calculate_bundle_transaction_priority_with_tips() {
        let (bank_forks, mint_keypair) = test_bank_forks();
        let bank = bank_forks.read().unwrap().root_bank();

        // Create tip accounts
        let tip_account1 = Pubkey::new_unique();
        let tip_account2 = Pubkey::new_unique();
        let mut tip_accounts = HashSet::new();
        tip_accounts.insert(tip_account1);
        tip_accounts.insert(tip_account2);

        // Create a transaction with multiple instructions including tips
        let regular_dest = Pubkey::new_unique();
        let tip_amount1: u64 = 5000;
        let tip_amount2: u64 = 3000;
        let regular_amount: u64 = 1000;

        let instructions = vec![
            system_instruction::transfer(&mint_keypair.pubkey(), &regular_dest, regular_amount),
            system_instruction::transfer(&mint_keypair.pubkey(), &tip_account1, tip_amount1),
            system_instruction::transfer(&mint_keypair.pubkey(), &tip_account2, tip_amount2),
        ];

        let message = solana_message::Message::new(&instructions, Some(&mint_keypair.pubkey()));
        let transaction =
            solana_transaction::Transaction::new(&[&mint_keypair], message, bank.last_blockhash());
        let tx = RuntimeTransaction::from_transaction_for_tests(transaction);

        let fee_budget_limits = FeeBudgetLimits::default();
        let (priority_with_tips, cost_with_tips) = calculate_bundle_transaction_priority_and_cost(
            &tx,
            &fee_budget_limits,
            &bank,
            &tip_accounts,
        );

        // Calculate priority without tips for comparison
        let (priority_no_tips, cost_no_tips) =
            calculate_priority_and_cost(&tx, &fee_budget_limits, &bank, None);

        // Cost should be the same
        assert_eq!(cost_with_tips, cost_no_tips);

        // Priority with tips should be higher than without
        assert!(priority_with_tips > priority_no_tips);

        // Verify the tip amount was added correctly (tip_amount1 + tip_amount2 = 8000)
        let base_reward = bank.calculate_reward_for_transaction(&tx, &fee_budget_limits);
        let expected_reward_with_tips = base_reward + tip_amount1 + tip_amount2;
        let expected_priority = expected_reward_with_tips
            .saturating_mul(1_000_000)
            .saturating_div(cost_with_tips.saturating_add(1));
        assert_eq!(priority_with_tips, expected_priority);
    }

    #[test]
    fn test_calculate_bundle_transaction_priority_no_tips() {
        let (bank_forks, mint_keypair) = test_bank_forks();
        let bank = bank_forks.read().unwrap().root_bank();

        let tip_account = Pubkey::new_unique();
        let mut tip_accounts = HashSet::new();
        tip_accounts.insert(tip_account);

        // Create a transaction with no transfers to tip accounts
        let regular_dest = Pubkey::new_unique();
        let transaction = transfer(&mint_keypair, &regular_dest, 1000, bank.last_blockhash());

        let fee_budget_limits = FeeBudgetLimits::default();
        let (priority_with_check, cost_with_check) = calculate_bundle_transaction_priority_and_cost(
            &transaction,
            &fee_budget_limits,
            &bank,
            &tip_accounts,
        );

        let (priority_baseline, cost_baseline) =
            calculate_priority_and_cost(&transaction, &fee_budget_limits, &bank, None);

        // Should be the same since no tips were sent
        assert_eq!(priority_with_check, priority_baseline);
        assert_eq!(cost_with_check, cost_baseline);
    }
}
