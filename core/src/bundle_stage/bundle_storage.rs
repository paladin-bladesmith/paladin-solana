use {
    crate::{
        banking_stage::immutable_deserialized_packet::ImmutableDeserializedPacket,
        bundle_stage::bundle_stage_leader_metrics::BundleStageLeaderMetrics,
        immutable_deserialized_bundle::ImmutableDeserializedBundle,
    },
    arrayref::array_ref,
    solana_bundle::{
        bundle_execution::LoadAndExecuteBundleError, BundleExecutionError, SanitizedBundle,
    },
    solana_clock::Slot,
    solana_cost_model::cost_model::CostModel,
    solana_fee_structure::FeeBudgetLimits,
    solana_message::compiled_instruction::CompiledInstruction,
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
    solana_runtime_transaction::transaction_meta::StaticMeta,
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    std::{
        collections::{HashSet, VecDeque},
        sync::Arc,
        time::Instant,
    },
};

pub struct InsertPacketBundlesSummary {
    pub num_bundles_inserted: usize,
    pub num_packets_inserted: usize,
    pub num_bundles_dropped: usize,
}

/// Bundle storage has two deques: one for unprocessed bundles and another for ones that exceeded
/// the cost model and need to get retried next slot.
#[derive(Debug)]
pub struct BundleStorage {
    last_update_slot: Slot,
    unprocessed_bundle_storage: VecDeque<ImmutableDeserializedBundle>,
    // Storage for bundles that exceeded the cost model for the slot they were last attempted
    // execution on
    cost_model_buffered_bundle_storage: VecDeque<ImmutableDeserializedBundle>,
}

impl Default for BundleStorage {
    fn default() -> Self {
        Self {
            last_update_slot: Slot::default(),
            unprocessed_bundle_storage: VecDeque::with_capacity(Self::BUNDLE_STORAGE_CAPACITY),
            cost_model_buffered_bundle_storage: VecDeque::with_capacity(
                Self::BUNDLE_STORAGE_CAPACITY,
            ),
        }
    }
}

impl BundleStorage {
    pub const BUNDLE_STORAGE_CAPACITY: usize = 1000;

    pub fn is_empty(&self) -> bool {
        self.unprocessed_bundle_storage.is_empty()
            && self.cost_model_buffered_bundle_storage.is_empty()
    }

    pub fn len(&self) -> usize {
        self.unprocessed_bundles_len() + self.cost_model_buffered_bundles_len()
    }

    pub fn unprocessed_bundles_len(&self) -> usize {
        self.unprocessed_bundle_storage.len()
    }

    pub fn unprocessed_packets_len(&self) -> usize {
        self.unprocessed_bundle_storage
            .iter()
            .map(|b| b.len())
            .sum::<usize>()
    }

    pub(crate) fn cost_model_buffered_bundles_len(&self) -> usize {
        self.cost_model_buffered_bundle_storage.len()
    }

    pub(crate) fn cost_model_buffered_packets_len(&self) -> usize {
        self.cost_model_buffered_bundle_storage
            .iter()
            .map(|b| b.len())
            .sum()
    }

    pub(crate) fn max_receive_size(&self) -> usize {
        self.unprocessed_bundle_storage.capacity() - self.unprocessed_bundle_storage.len()
    }

    /// Returns the number of unprocessed bundles + cost model buffered cleared
    pub fn reset(&mut self) -> (usize, usize) {
        let num_unprocessed_bundles = self.unprocessed_bundle_storage.len();
        let num_cost_model_buffered_bundles = self.cost_model_buffered_bundle_storage.len();
        self.unprocessed_bundle_storage.clear();
        self.cost_model_buffered_bundle_storage.clear();
        (num_unprocessed_bundles, num_cost_model_buffered_bundles)
    }

    fn insert_bundles(
        deque: &mut VecDeque<ImmutableDeserializedBundle>,
        deserialized_bundles: Vec<ImmutableDeserializedBundle>,
        push_back: bool,
    ) -> InsertPacketBundlesSummary {
        // deque should be initialized with size [Self::BUNDLE_STORAGE_CAPACITY]
        let deque_free_space = Self::BUNDLE_STORAGE_CAPACITY
            .checked_sub(deque.len())
            .unwrap();
        let num_bundles_inserted = std::cmp::min(deque_free_space, deserialized_bundles.len());
        let num_bundles_dropped = deserialized_bundles
            .len()
            .checked_sub(num_bundles_inserted)
            .unwrap();
        let num_packets_inserted = deserialized_bundles
            .iter()
            .take(num_bundles_inserted)
            .map(|b| b.len())
            .sum::<usize>();

        let to_insert = deserialized_bundles.into_iter().take(num_bundles_inserted);
        if push_back {
            deque.extend(to_insert)
        } else {
            to_insert.for_each(|b| deque.push_front(b));
        }

        InsertPacketBundlesSummary {
            num_bundles_inserted,
            num_packets_inserted,
            num_bundles_dropped,
        }
    }

    fn push_front_unprocessed_bundles(
        &mut self,
        deserialized_bundles: Vec<ImmutableDeserializedBundle>,
    ) -> InsertPacketBundlesSummary {
        Self::insert_bundles(
            &mut self.unprocessed_bundle_storage,
            deserialized_bundles,
            false,
        )
    }

    fn push_back_cost_model_buffered_bundles(
        &mut self,
        deserialized_bundles: Vec<ImmutableDeserializedBundle>,
    ) -> InsertPacketBundlesSummary {
        Self::insert_bundles(
            &mut self.cost_model_buffered_bundle_storage,
            deserialized_bundles,
            true,
        )
    }

    pub fn insert_unprocessed_bundles(
        &mut self,
        deserialized_bundles: Vec<ImmutableDeserializedBundle>,
    ) -> InsertPacketBundlesSummary {
        Self::insert_bundles(
            &mut self.unprocessed_bundle_storage,
            deserialized_bundles,
            true,
        )
    }

    /// Drains bundles from the queue, sanitizes them to prepare for execution, executes them by
    /// calling `processing_function`, then potentially rebuffer them.
    pub fn process_bundles<F>(
        &mut self,
        bank: Arc<Bank>,
        bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
        blacklisted_accounts: &HashSet<Pubkey>,
        tip_accounts: &HashSet<Pubkey>,
        mut processing_function: F,
    ) -> bool
    where
        F: FnMut(
            &[(ImmutableDeserializedBundle, SanitizedBundle)],
            &mut BundleStageLeaderMetrics,
        ) -> Vec<Result<(), BundleExecutionError>>,
    {
        let sanitized_bundles = self.drain_and_sanitize_bundles(
            bank,
            bundle_stage_leader_metrics,
            blacklisted_accounts,
            tip_accounts,
        );

        debug!("processing {} bundles", sanitized_bundles.len());
        let bundle_execution_results =
            processing_function(&sanitized_bundles, bundle_stage_leader_metrics);

        let mut is_slot_over = false;

        let mut rebuffered_bundles = Vec::new();

        sanitized_bundles
            .into_iter()
            .zip(bundle_execution_results)
            .for_each(
                |((deserialized_bundle, sanitized_bundle), result)| match result {
                    Ok(_) => {
                        debug!("bundle={} executed ok", sanitized_bundle.bundle_id);
                        // yippee
                    }
                    Err(BundleExecutionError::PohRecordError(e)) => {
                        // buffer the bundle to the front of the queue to be attempted next slot
                        debug!(
                            "bundle={} poh record error: {e:?}",
                            sanitized_bundle.bundle_id
                        );
                        rebuffered_bundles.push(deserialized_bundle);
                        is_slot_over = true;
                    }
                    Err(BundleExecutionError::BankProcessingTimeLimitReached) => {
                        // buffer the bundle to the front of the queue to be attempted next slot
                        debug!("bundle={} bank processing done", sanitized_bundle.bundle_id);
                        rebuffered_bundles.push(deserialized_bundle);
                        is_slot_over = true;
                    }
                    Err(BundleExecutionError::ExceedsCostModel) => {
                        // cost model buffered bundles contain most recent bundles at the front of the queue
                        debug!(
                            "bundle={} exceeds cost model, rebuffering",
                            sanitized_bundle.bundle_id
                        );
                        self.push_back_cost_model_buffered_bundles(vec![deserialized_bundle]);
                    }
                    Err(BundleExecutionError::TransactionFailure(
                        LoadAndExecuteBundleError::ProcessingTimeExceeded(_),
                    )) => {
                        // these are treated the same as exceeds cost model and are rebuferred to be completed
                        // at the beginning of the next slot
                        debug!(
                            "bundle={} processing time exceeded, rebuffering",
                            sanitized_bundle.bundle_id
                        );
                        self.push_back_cost_model_buffered_bundles(vec![deserialized_bundle]);
                    }
                    Err(BundleExecutionError::TransactionFailure(e)) => {
                        debug!(
                            "bundle={} execution error: {:?}",
                            sanitized_bundle.bundle_id, e
                        );
                        // do nothing
                    }
                    Err(BundleExecutionError::TipError(e)) => {
                        debug!("bundle={} tip error: {}", sanitized_bundle.bundle_id, e);
                        // Tip errors are _typically_ due to misconfiguration (except for poh record error, bank processing error, exceeds cost model)
                        // in order to prevent buffering too many bundles, we'll just drop the bundle
                    }
                    Err(BundleExecutionError::LockError) => {
                        // lock errors are irrecoverable due to malformed transactions
                        debug!("bundle={} lock error", sanitized_bundle.bundle_id);
                    }
                    // NB: Tip cutoff is static & front-runs will never succeed.
                    Err(BundleExecutionError::FrontRun) => {}
                },
            );

        // rebuffered bundles are pushed onto deque in reverse order so the first bundle is at the front
        for bundle in rebuffered_bundles.into_iter().rev() {
            self.push_front_unprocessed_bundles(vec![bundle]);
        }

        is_slot_over
    }

    /// Drains the unprocessed_bundle_storage, converting bundle packets into SanitizedBundles
    fn drain_and_sanitize_bundles(
        &mut self,
        bank: Arc<Bank>,
        bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
        blacklisted_accounts: &HashSet<Pubkey>,
        tip_accounts: &HashSet<Pubkey>,
    ) -> Vec<(ImmutableDeserializedBundle, SanitizedBundle)> {
        let mut error_metrics = TransactionErrorMetrics::default();

        let start = Instant::now();

        let mut sanitized_bundles = Vec::new();

        // on new slot, drain anything that was buffered from last slot
        if bank.slot() != self.last_update_slot {
            sanitized_bundles.extend(
                self.cost_model_buffered_bundle_storage
                    .drain(..)
                    .filter_map(|packet_bundle| {
                        let r = packet_bundle.build_sanitized_bundle(
                            &bank,
                            blacklisted_accounts,
                            &mut error_metrics,
                        );
                        bundle_stage_leader_metrics
                            .bundle_stage_metrics_tracker()
                            .increment_sanitize_transaction_result(&r);

                        match r {
                            Ok(sanitized_bundle) => Some((packet_bundle, sanitized_bundle)),
                            Err(e) => {
                                debug!(
                                    "bundle id: {} error sanitizing: {}",
                                    packet_bundle.bundle_id(),
                                    e
                                );
                                None
                            }
                        }
                    }),
            );

            self.last_update_slot = bank.slot();
        }

        sanitized_bundles.extend(self.unprocessed_bundle_storage.drain(..).filter_map(
            |packet_bundle| {
                let r = packet_bundle.build_sanitized_bundle(
                    &bank,
                    blacklisted_accounts,
                    &mut error_metrics,
                );
                bundle_stage_leader_metrics
                    .bundle_stage_metrics_tracker()
                    .increment_sanitize_transaction_result(&r);
                match r {
                    Ok(sanitized_bundle) => Some((packet_bundle, sanitized_bundle)),
                    Err(e) => {
                        debug!(
                            "bundle id: {} error sanitizing: {}",
                            packet_bundle.bundle_id(),
                            e
                        );
                        None
                    }
                }
            },
        ));

        let mut priority_counter = 0;
        sanitized_bundles.sort_by_cached_key(|(immutable_bundle, sanitized_bundle)| {
            Self::calculate_bundle_priority(
                immutable_bundle,
                sanitized_bundle,
                &bank,
                &mut priority_counter,
                tip_accounts,
            )
        });

        let elapsed = start.elapsed().as_micros();
        bundle_stage_leader_metrics
            .bundle_stage_metrics_tracker()
            .increment_sanitize_bundle_elapsed_us(elapsed as u64);
        bundle_stage_leader_metrics
            .leader_slot_metrics_tracker()
            .increment_transactions_from_packets_us(elapsed as u64);

        bundle_stage_leader_metrics
            .leader_slot_metrics_tracker()
            .accumulate_transaction_errors(&error_metrics);

        sanitized_bundles
    }

    fn calculate_bundle_priority(
        immutable_bundle: &ImmutableDeserializedBundle,
        sanitized_bundle: &SanitizedBundle,
        bank: &Bank,
        priority_counter: &mut u64,
        tip_accounts: &HashSet<Pubkey>,
    ) -> (std::cmp::Reverse<u64>, u64) {
        let total_cu_cost: u64 = sanitized_bundle
            .transactions
            .iter()
            .map(|tx| CostModel::calculate_cost(tx, &bank.feature_set).sum())
            .sum();

        let reward_from_tx: u64 = sanitized_bundle
            .transactions
            .iter()
            .map(|tx| {
                let limits = tx
                    .compute_budget_instruction_details()
                    .sanitize_and_convert_to_compute_budget_limits(&bank.feature_set)
                    .unwrap_or_default();
                bank.calculate_reward_for_transaction(tx, &FeeBudgetLimits::from(limits))
            })
            .sum();

        let reward_from_tips: u64 = immutable_bundle
            .packets()
            .iter()
            .map(|packets| Self::extract_tips_from_packet(packets, tip_accounts))
            .sum();

        let total_reward = reward_from_tx.saturating_add(reward_from_tips);
        const MULTIPLIER: u64 = 1_000_000;
        let priority = total_reward.saturating_mul(MULTIPLIER) / total_cu_cost.max(1);
        *priority_counter = priority_counter.wrapping_add(1);

        (std::cmp::Reverse(priority), *priority_counter)
    }

    pub fn extract_tips_from_packet(
        packet: &ImmutableDeserializedPacket,
        tip_accounts: &HashSet<Pubkey>,
    ) -> u64 {
        let message = packet.transaction().get_message();
        let account_keys = message.message.static_account_keys();
        message
            .program_instructions_iter()
            .filter_map(|(pid, ix)| Self::extract_transfer(account_keys, pid, ix))
            .filter(|(dest, _)| tip_accounts.contains(dest))
            .map(|(_, amount)| amount)
            .sum()
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
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::packet_bundle::PacketBundle,
        agave_feature_set::FeatureSet,
        solana_compute_budget_interface::ComputeBudgetInstruction,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_ledger::genesis_utils::GenesisConfigInfo,
        solana_message::Message,
        solana_packet::Packet,
        solana_perf::packet::{PacketBatch, PinnedPacketBatch},
        solana_runtime::genesis_utils::create_genesis_config,
        solana_seed_derivable::SeedDerivable,
        solana_signer::Signer,
        solana_transaction::Transaction,
    };

    #[test]
    fn transfer_encoding() {
        let from = Keypair::from_seed(&[1; 32]).unwrap();
        let to = Pubkey::new_from_array([2; 32]);
        let amount = 250;
        let transfer =
            solana_system_transaction::transfer(&from, &to, amount, Hash::new_from_array([3; 32]));

        let (recovered_to, recovered_amount) = BundleStorage::extract_transfer(
            &transfer.message.account_keys,
            &solana_system_program::id(),
            &transfer.message.instructions[0],
        )
        .unwrap();
        assert_eq!(recovered_to, &to);
        assert_eq!(recovered_amount, amount);
    }

    #[test]
    fn test_priority_sorting_behavior() {
        let mut priorities = vec![
            (std::cmp::Reverse(100), 1),
            (std::cmp::Reverse(200), 2),
            (std::cmp::Reverse(100), 3),
            (std::cmp::Reverse(50), 4),
        ];
        priorities.sort();
        assert_eq!(
            priorities,
            vec![
                (std::cmp::Reverse(200), 2),
                (std::cmp::Reverse(100), 1),
                (std::cmp::Reverse(100), 3),
                (std::cmp::Reverse(50), 4),
            ]
        );
    }

    #[test]
    fn test_bundle_priority_calculation() {
        let GenesisConfigInfo {
            mut genesis_config, ..
        } = create_genesis_config(1_000_000_000);
        genesis_config.fee_rate_governor.lamports_per_signature = 5000;
        let mut bank = Bank::new_for_tests(&genesis_config);
        bank.feature_set = Arc::new(FeatureSet::all_enabled());
        let (bank, _bank_forks) = bank.wrap_with_bank_forks_for_tests();
        assert!(bank
            .feature_set
            .is_active(&agave_feature_set::reward_full_priority_fee::id()));

        let blockhash = bank.last_blockhash();

        let payer1 = Keypair::new();
        let payer2 = Keypair::new();
        let payer3 = Keypair::new();
        let dest1 = Keypair::new();
        let dest2 = Keypair::new();
        let dest3 = Keypair::new();
        let dest4 = Keypair::new();
        let tip_account = Keypair::new();

        let tip_accounts = [tip_account.pubkey()]
            .iter()
            .cloned()
            .collect::<HashSet<_>>();

        let create_tx_with_priority_fee = |payer: &Keypair,
                                           dest: &Pubkey,
                                           transfer_amount: u64,
                                           priority_fee_per_cu: u64,
                                           compute_units: u32|
         -> Transaction {
            let mut instructions = vec![];
            if compute_units > 0 {
                instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(
                    compute_units,
                ));
            }
            if priority_fee_per_cu > 0 {
                instructions.push(ComputeBudgetInstruction::set_compute_unit_price(
                    priority_fee_per_cu,
                ));
            }
            instructions.push(solana_system_interface::instruction::transfer(
                &payer.pubkey(),
                dest,
                transfer_amount,
            ));

            let message = Message::new(&instructions, Some(&payer.pubkey()));
            Transaction::new(&[payer], message, blockhash)
        };

        let create_bundles_from_transactions =
            |transactions: &[Transaction]| -> ImmutableDeserializedBundle {
                ImmutableDeserializedBundle::new(
                    &mut PacketBundle {
                        batch: PacketBatch::Pinned(PinnedPacketBatch::new(
                            transactions
                                .iter()
                                .map(|tx| Packet::from_data(None, tx).unwrap())
                                .collect::<Vec<_>>(),
                        )),
                        bundle_id: format!("test_bundle_{}", rand::random::<u32>()),
                    },
                    None,
                    &Ok,
                )
                .unwrap()
            };

        // Bundle 1: 1 transaction with low priority fee
        let bundle1_txs = vec![create_tx_with_priority_fee(
            &payer1,
            &dest1.pubkey(),
            1_000,
            100,
            50_000,
        )];

        // Bundle 2: 1 transaction with high priority fee
        let bundle2_txs = vec![create_tx_with_priority_fee(
            &payer2,
            &dest2.pubkey(),
            2_000,
            5000,
            40_000,
        )];

        // Bundle 3: 3 transactions with medium priority fees + one tip transaction
        let bundle3_txs = vec![
            create_tx_with_priority_fee(&payer1, &dest3.pubkey(), 500, 1000, 30_000),
            solana_system_transaction::transfer(&payer2, &tip_account.pubkey(), 50_000, blockhash),
            create_tx_with_priority_fee(&payer3, &dest4.pubkey(), 1_500, 1200, 35_000),
        ];

        // Create immutable bundles
        let immutable_bundle1 = create_bundles_from_transactions(&bundle1_txs);
        let immutable_bundle2 = create_bundles_from_transactions(&bundle2_txs);
        let immutable_bundle3 = create_bundles_from_transactions(&bundle3_txs);

        // Create sanitized bundles (similar to drain_and_sanitize_bundles)
        let mut error_metrics = TransactionErrorMetrics::default();

        let sanitized_bundle1 = immutable_bundle1
            .build_sanitized_bundle(&bank, &HashSet::default(), &mut error_metrics)
            .expect("Bundle 1 should sanitize successfully");

        let sanitized_bundle2 = immutable_bundle2
            .build_sanitized_bundle(&bank, &HashSet::default(), &mut error_metrics)
            .expect("Bundle 2 should sanitize successfully");

        let sanitized_bundle3 = immutable_bundle3
            .build_sanitized_bundle(&bank, &HashSet::default(), &mut error_metrics)
            .expect("Bundle 3 should sanitize successfully");

        let mut priority_counter = 0;
        let bundle1_priority_key = BundleStorage::calculate_bundle_priority(
            &immutable_bundle1,
            &sanitized_bundle1,
            &bank,
            &mut priority_counter,
            &tip_accounts,
        );

        let bundle2_priority_key = BundleStorage::calculate_bundle_priority(
            &immutable_bundle2,
            &sanitized_bundle2,
            &bank,
            &mut priority_counter,
            &tip_accounts,
        );

        let bundle3_priority_key = BundleStorage::calculate_bundle_priority(
            &immutable_bundle3,
            &sanitized_bundle3,
            &bank,
            &mut priority_counter,
            &tip_accounts,
        );

        let mut bundles_with_priority = vec![
            (bundle1_priority_key, "Bundle 1"),
            (bundle2_priority_key, "Bundle 2"),
            (bundle3_priority_key, "Bundle 3"),
        ];

        bundles_with_priority.sort_by_key(|(priority_key, _)| *priority_key);

        assert_eq!(bundles_with_priority[0].1, "Bundle 3");
        assert_eq!(bundles_with_priority[1].1, "Bundle 2");
        assert_eq!(bundles_with_priority[2].1, "Bundle 1");
    }
}
