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
                    Err(BundleExecutionError::ReservationConflict) => {
                        // Reservation conflicts are retryable - buffer to try again
                        debug!(
                            "bundle={} reservation conflict, rebuffering",
                            sanitized_bundle.bundle_id
                        );
                        self.push_back_cost_model_buffered_bundles(vec![deserialized_bundle]);
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
        sha2::{Digest, Sha256},
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

    struct TestActors {
        payer1: Keypair,
        payer2: Keypair,
        payer3: Keypair,
        dest1: Keypair,
        dest2: Keypair,
        dest3: Keypair,
        dest4: Keypair,
        tip_account: Keypair,
    }

    struct TxFeeParams {
        cu_limit: Option<u32>, // None => omit CU limit ix
        cu_price: Option<u64>, // None => omit CU price ix
        tip_lamports: u64,     // Added as an extra transfer ix (0 allowed)
    }

    fn setup_bank(genesis_lamports: u64) -> (Arc<Bank>, Hash) {
        let GenesisConfigInfo {
            mut genesis_config, ..
        } = create_genesis_config(genesis_lamports);
        genesis_config.fee_rate_governor.lamports_per_signature = 5_000; // stable for tests
        let mut bank = Bank::new_for_tests(&genesis_config);
        bank.feature_set = Arc::new(FeatureSet::all_enabled());
        let (bank, _bf) = bank.wrap_with_bank_forks_for_tests();
        assert!(bank
            .feature_set
            .is_active(&agave_feature_set::reward_full_priority_fee::id()));
        let bh = bank.last_blockhash();
        (bank, bh)
    }

    fn setup_actors() -> TestActors {
        TestActors {
            payer1: Keypair::new(),
            payer2: Keypair::new(),
            payer3: Keypair::new(),
            dest1: Keypair::new(),
            dest2: Keypair::new(),
            dest3: Keypair::new(),
            dest4: Keypair::new(),
            tip_account: Keypair::new(),
        }
    }

    /// Build a standardized transaction: [CU limit?] [CU price?] [main transfer] [tip transfer (could be 0)]
    fn build_standard_tx(
        payer: &Keypair,
        dest: &Pubkey,
        lamports: u64,
        fees: &TxFeeParams,
        tip_dest: &Pubkey,
        blockhash: Hash,
    ) -> Transaction {
        let mut ixs = Vec::with_capacity(4);
        if let Some(limit) = fees.cu_limit {
            ixs.push(ComputeBudgetInstruction::set_compute_unit_limit(limit));
        }
        if let Some(price) = fees.cu_price {
            ixs.push(ComputeBudgetInstruction::set_compute_unit_price(price));
        }
        ixs.push(solana_system_interface::instruction::transfer(
            &payer.pubkey(),
            dest,
            lamports,
        ));
        // Always include tip ix (0 allowed) to normalize CU usage / instruction count
        ixs.push(solana_system_interface::instruction::transfer(
            &payer.pubkey(),
            tip_dest,
            fees.tip_lamports,
        ));
        let msg = Message::new(&ixs, Some(&payer.pubkey()));
        Transaction::new(&[payer], msg, blockhash)
    }

    fn derive_bundle_id(txs: &[Transaction]) -> String {
        let joined = txs
            .iter()
            .map(|t| t.signatures[0].to_string())
            .collect::<Vec<_>>()
            .join(",");
        let mut h = Sha256::new();
        h.update(joined.as_bytes());
        format!("{:x}", h.finalize())
    }

    fn build_immutable_bundle(
        txs: &[Transaction],
        label: Option<&str>,
    ) -> ImmutableDeserializedBundle {
        let bundle_id = label
            .map(|s| s.to_string())
            .unwrap_or_else(|| derive_bundle_id(txs));
        let mut pkt_bundle = PacketBundle {
            batch: PacketBatch::Pinned(PinnedPacketBatch::new(
                txs.iter()
                    .map(|t| Packet::from_data(None, t).unwrap())
                    .collect(),
            )),
            bundle_id,
        };
        ImmutableDeserializedBundle::new(&mut pkt_bundle, None).unwrap()
    }

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
    fn test_bundle_priority_calculation_with_cu_limits() {
        let (bank, blockhash) = setup_bank(1_000_000_000);
        let actors = setup_actors();
        let tip_accounts = [actors.tip_account.pubkey()]
            .into_iter()
            .collect::<HashSet<_>>();

        // Use standardized tx builder (tip_lamports 0 to equalize IX count)
        let fee_high = TxFeeParams {
            cu_limit: Some(10_000),
            cu_price: Some(10_000),
            tip_lamports: 0,
        };
        let fee_high_no_limit = TxFeeParams {
            cu_limit: None,
            cu_price: Some(10_000),
            tip_lamports: 0,
        };
        let fee_low = TxFeeParams {
            cu_limit: Some(10_000),
            cu_price: Some(5_000),
            tip_lamports: 0,
        };
        let fee_low_no_limit = TxFeeParams {
            cu_limit: None,
            cu_price: Some(5_000),
            tip_lamports: 0,
        };
        let fee_very_high = TxFeeParams {
            cu_limit: Some(20_000),
            cu_price: Some(100_000),
            tip_lamports: 0,
        };
        let fee_zero = TxFeeParams {
            cu_limit: None,
            cu_price: Some(0),
            tip_lamports: 0,
        };

        let bundle1_txs = vec![
            build_standard_tx(
                &actors.payer1,
                &actors.dest1.pubkey(),
                1_000,
                &fee_high,
                &actors.tip_account.pubkey(),
                blockhash,
            ),
            build_standard_tx(
                &actors.payer1,
                &actors.dest2.pubkey(),
                1_000,
                &fee_high_no_limit,
                &actors.tip_account.pubkey(),
                blockhash,
            ),
        ];
        let bundle2_txs = vec![
            build_standard_tx(
                &actors.payer2,
                &actors.dest2.pubkey(),
                1_000,
                &fee_low,
                &actors.tip_account.pubkey(),
                blockhash,
            ),
            build_standard_tx(
                &actors.payer2,
                &actors.dest3.pubkey(),
                1_000,
                &fee_low_no_limit,
                &actors.tip_account.pubkey(),
                blockhash,
            ),
        ];
        let bundle3_txs = vec![
            build_standard_tx(
                &actors.payer1,
                &actors.dest3.pubkey(),
                1_000,
                &fee_very_high,
                &actors.tip_account.pubkey(),
                blockhash,
            ),
            build_standard_tx(
                &actors.payer3,
                &actors.dest4.pubkey(),
                1_000,
                &fee_zero,
                &actors.tip_account.pubkey(),
                blockhash,
            ),
        ];

        let immutable_bundle1 = build_immutable_bundle(&bundle1_txs, Some("bundle1"));
        let immutable_bundle2 = build_immutable_bundle(&bundle2_txs, Some("bundle2"));
        let immutable_bundle3 = build_immutable_bundle(&bundle3_txs, Some("bundle3"));

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
        assert_eq!(bundles_with_priority[1].1, "Bundle 1");
        assert_eq!(bundles_with_priority[2].1, "Bundle 2");
    }

    #[test]
    fn test_nonzero_cu_price_beats_zero_price_with_limit() {
        let (bank, blockhash) = setup_bank(1_000_000_000);
        let actors = setup_actors();
        // Empty tip_accounts -> we only compare price dimension
        let tip_accounts: HashSet<Pubkey> = HashSet::new();

        let fee_zero = TxFeeParams {
            cu_limit: Some(10_000),
            cu_price: Some(0),
            tip_lamports: 0,
        };
        let fee_priced = TxFeeParams {
            cu_limit: Some(10_000),
            cu_price: Some(10_000),
            tip_lamports: 0,
        };
        let tx_zero = build_standard_tx(
            &actors.payer1,
            &actors.dest1.pubkey(),
            1_000,
            &fee_zero,
            &actors.tip_account.pubkey(),
            blockhash,
        );
        let tx_priced = build_standard_tx(
            &actors.payer2,
            &actors.dest2.pubkey(),
            1_000,
            &fee_priced,
            &actors.tip_account.pubkey(),
            blockhash,
        );

        let imm_a = build_immutable_bundle(&[tx_zero], Some("zero_price"));
        let imm_b = build_immutable_bundle(&[tx_priced], Some("priced"));

        let mut errs = TransactionErrorMetrics::default();
        let san_a = imm_a
            .build_sanitized_bundle(&bank, &HashSet::default(), &mut errs)
            .unwrap();
        let san_b = imm_b
            .build_sanitized_bundle(&bank, &HashSet::default(), &mut errs)
            .unwrap();

        let mut ctr = 0;
        let key_a = BundleStorage::calculate_bundle_priority(
            &imm_a,
            &san_a,
            &bank,
            &mut ctr,
            &tip_accounts,
        );
        let key_b = BundleStorage::calculate_bundle_priority(
            &imm_b,
            &san_b,
            &bank,
            &mut ctr,
            &tip_accounts,
        );

        // Non-zero price should outrank zero price
        assert!(
            key_b < key_a,
            "priced bundle should sort before zero-price bundle: {:?} vs {:?}",
            key_b,
            key_a
        );
    }

    #[test]
    fn test_zero_price_two_signers_can_outrank_low_price() {
        let (bank, blockhash) = setup_bank(1_000_000_000);
        let actors = setup_actors();
        let tip_accounts: HashSet<Pubkey> = HashSet::new();

        // Two-signers, zero price
        let ixs_zp2 = vec![
            solana_system_interface::instruction::transfer(
                &actors.payer1.pubkey(),
                &actors.dest1.pubkey(),
                1_000,
            ),
            solana_system_interface::instruction::transfer(
                &actors.payer2.pubkey(),
                &actors.dest2.pubkey(),
                1_000,
            ),
        ];
        let msg_zp2 = Message::new(&ixs_zp2, Some(&actors.payer1.pubkey()));
        let tx_zp2 = Transaction::new(&[&actors.payer1, &actors.payer2], msg_zp2, blockhash);

        // Single signer low price (adds CB overhead)
        let ixs_lp1 = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(10_000),
            ComputeBudgetInstruction::set_compute_unit_price(100),
            solana_system_interface::instruction::transfer(
                &actors.payer3.pubkey(),
                &actors.dest1.pubkey(),
                1_000,
            ),
        ];
        let msg_lp1 = Message::new(&ixs_lp1, Some(&actors.payer3.pubkey()));
        let tx_lp1 = Transaction::new(&[&actors.payer3], msg_lp1, blockhash);

        let imm_zp2 = build_immutable_bundle(&[tx_zp2], Some("zp2"));
        let imm_lp1 = build_immutable_bundle(&[tx_lp1], Some("lp1"));

        let mut errs = TransactionErrorMetrics::default();
        let san_zp2 = imm_zp2
            .build_sanitized_bundle(&bank, &HashSet::default(), &mut errs)
            .unwrap();
        let san_lp1 = imm_lp1
            .build_sanitized_bundle(&bank, &HashSet::default(), &mut errs)
            .unwrap();

        let mut ctr = 0;
        let key_zp2 = BundleStorage::calculate_bundle_priority(
            &imm_zp2,
            &san_zp2,
            &bank,
            &mut ctr,
            &tip_accounts,
        );
        let key_lp1 = BundleStorage::calculate_bundle_priority(
            &imm_lp1,
            &san_lp1,
            &bank,
            &mut ctr,
            &tip_accounts,
        );

        // Expect the zero-price 2-signer tx to outrank the low-priority-fee single-signer tx
        assert!(
            key_zp2 < key_lp1,
            "2-signer zero-price should sort before 1-signer low-price: {:?} vs {:?}",
            key_zp2,
            key_lp1
        );
    }

    #[test]
    fn test_bundle_priority_calculation_with_tips() {
        let (bank, blockhash) = setup_bank(1_000_000_000);
        let actors = setup_actors();
        let tip_accounts = [actors.tip_account.pubkey()]
            .into_iter()
            .collect::<HashSet<_>>();

        let tx_base = {
            let ixs = vec![
                ComputeBudgetInstruction::set_compute_unit_limit(10_000),
                ComputeBudgetInstruction::set_compute_unit_price(0),
                solana_system_interface::instruction::transfer(
                    &actors.payer1.pubkey(),
                    &actors.dest1.pubkey(),
                    1_000,
                ),
            ];
            let msg = Message::new(&ixs, Some(&actors.payer1.pubkey()));
            Transaction::new(&[&actors.payer1], msg, blockhash)
        };
        let tx_base_for_tip_bundle = {
            let ixs = vec![
                ComputeBudgetInstruction::set_compute_unit_limit(10_000),
                ComputeBudgetInstruction::set_compute_unit_price(0),
                solana_system_interface::instruction::transfer(
                    &actors.payer1.pubkey(),
                    &actors.dest2.pubkey(),
                    1_000,
                ),
            ];
            let msg = Message::new(&ixs, Some(&actors.payer1.pubkey()));
            Transaction::new(&[&actors.payer1], msg, blockhash)
        };
        let tip_tx = solana_system_transaction::transfer(
            &actors.payer2,
            &actors.tip_account.pubkey(),
            50_000,
            blockhash,
        );

        let imm_no_tip = build_immutable_bundle(&[tx_base], Some("bundle_no_tip"));
        let imm_with_tip =
            build_immutable_bundle(&[tx_base_for_tip_bundle, tip_tx], Some("bundle_with_tip"));

        let mut errs = TransactionErrorMetrics::default();
        let san_no_tip = imm_no_tip
            .build_sanitized_bundle(&bank, &HashSet::default(), &mut errs)
            .unwrap();
        let san_with_tip = imm_with_tip
            .build_sanitized_bundle(&bank, &HashSet::default(), &mut errs)
            .unwrap();

        let mut ctr = 0;
        let key_no_tip = BundleStorage::calculate_bundle_priority(
            &imm_no_tip,
            &san_no_tip,
            &bank,
            &mut ctr,
            &tip_accounts,
        );
        let key_with_tip = BundleStorage::calculate_bundle_priority(
            &imm_with_tip,
            &san_with_tip,
            &bank,
            &mut ctr,
            &tip_accounts,
        );

        assert!(
            key_with_tip < key_no_tip,
            "bundle with tip should outrank bundle without tip: {:?} vs {:?}",
            key_with_tip,
            key_no_tip
        );
    }

    #[test]
    fn test_tip_bundle_outranks_low_priority_fee_bundle() {
        let (bank, blockhash) = setup_bank(1_000_000_000);
        let actors = setup_actors();
        let tip_accounts = [actors.tip_account.pubkey()]
            .into_iter()
            .collect::<HashSet<_>>();

        let tx_low_prio = {
            let ixs = vec![
                ComputeBudgetInstruction::set_compute_unit_limit(10_000),
                ComputeBudgetInstruction::set_compute_unit_price(100),
                solana_system_interface::instruction::transfer(
                    &actors.payer1.pubkey(),
                    &actors.dest1.pubkey(),
                    1_000,
                ),
            ];
            let msg = Message::new(&ixs, Some(&actors.payer1.pubkey()));
            Transaction::new(&[&actors.payer1], msg, blockhash)
        };
        let tx_base_zero_price = {
            let ixs = vec![
                ComputeBudgetInstruction::set_compute_unit_limit(10_000),
                ComputeBudgetInstruction::set_compute_unit_price(0),
                solana_system_interface::instruction::transfer(
                    &actors.payer1.pubkey(),
                    &actors.dest2.pubkey(),
                    1_000,
                ),
            ];
            let msg = Message::new(&ixs, Some(&actors.payer1.pubkey()));
            Transaction::new(&[&actors.payer1], msg, blockhash)
        };
        let tx_tip = solana_system_transaction::transfer(
            &actors.payer2,
            &actors.tip_account.pubkey(),
            50_000,
            blockhash,
        );

        let imm_low = build_immutable_bundle(&[tx_low_prio], Some("low_prio_bundle"));
        let imm_tip = build_immutable_bundle(&[tx_base_zero_price, tx_tip], Some("tip_bundle"));

        let mut errs = TransactionErrorMetrics::default();
        let san_low = imm_low
            .build_sanitized_bundle(&bank, &HashSet::default(), &mut errs)
            .unwrap();
        let san_tip = imm_tip
            .build_sanitized_bundle(&bank, &HashSet::default(), &mut errs)
            .unwrap();

        let mut ctr = 0;
        let key_low = BundleStorage::calculate_bundle_priority(
            &imm_low,
            &san_low,
            &bank,
            &mut ctr,
            &tip_accounts,
        );
        let key_tip = BundleStorage::calculate_bundle_priority(
            &imm_tip,
            &san_tip,
            &bank,
            &mut ctr,
            &tip_accounts,
        );

        assert!(
            key_tip < key_low,
            "tip bundle should outrank low priority fee bundle: {:?} vs {:?}",
            key_tip,
            key_low
        );
    }
}
