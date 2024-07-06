use {
    crate::{
        banking_stage::{
            decision_maker::{BufferedPacketsDecision, DecisionMaker},
            immutable_deserialized_packet::ImmutableDeserializedPacket,
            qos_service::QosService,
            transaction_scheduler::scheduler_controller::SchedulerController,
            unprocessed_transaction_storage::UnprocessedTransactionStorage,
        },
        bundle_stage::{
            bundle_consumer::BundleConsumer,
            bundle_reserved_space_manager::BundleReservedSpaceManager,
            bundle_stage_leader_metrics::BundleStageLeaderMetrics, committer::Committer,
            front_run_identifier::AMM_PROGRAMS,
        },
        immutable_deserialized_bundle::ImmutableDeserializedBundle,
        packet_bundle::PacketBundle,
        tip_manager::TipManager,
    },
    crossbeam_channel::{Receiver, RecvTimeoutError},
    hashbrown::HashMap,
    ouroboros::self_referencing,
    solana_bundle::{
        bundle_account_locker::{BundleAccountLocker, LockedBundle},
        BundleExecutionError, SanitizedBundle,
    },
    solana_cost_model::block_cost_limits::MAX_BLOCK_UNITS,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::blockstore_processor::TransactionStatusSender,
    solana_measure::{measure_time, measure_us},
    solana_poh::poh_recorder::{BankStart, PohRecorder, TransactionRecorder},
    solana_runtime::{
        bank::Bank, prioritization_fee_cache::PrioritizationFeeCache,
        vote_sender_types::ReplayVoteSender,
    },
    solana_runtime_transaction::instructions_processor::process_compute_budget_instructions,
    solana_sdk::pubkey::Pubkey,
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    std::{
        cmp::Reverse,
        collections::{BTreeMap, HashSet},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        time::{Duration, Instant},
    },
};

const PALADIN_BUNDLE_STAGE_ID: u32 = 2000;
const MAX_BUNDLE_RETRY_DURATION: Duration = Duration::from_millis(40);
const MAX_PACKETS_PER_BUNDLE: usize = 1;
const BATCH_INTERVAL: Duration = Duration::from_millis(50);
const MIN_MICRO_LAMPORTS: u64 = 10u64.pow(6) * 10; // 10 lamports per CU

pub(crate) struct PaladinBundleStage {
    exit: Arc<AtomicBool>,

    paladin_rx: Receiver<Vec<PacketBundle>>,

    decision_maker: DecisionMaker,
    poh_recorder: Arc<RwLock<PohRecorder>>,

    bundles: BTreeMap<BundlePriorityId, ImmutableDeserializedBundle>,
    bundle_id: BundleIdGenerator,
    batches: [Vec<(ImmutableDeserializedBundle, SanitizedBundle)>; 2],
    batches_last: Instant,

    tip_manager: TipManager,
    bundle_account_locker: BundleAccountLocker,
    committer: Committer,
    transaction_recorder: TransactionRecorder,
    qos_service: QosService,
    reserved_space: BundleReservedSpaceManager,
    log_messages_bytes_limit: Option<usize>,
    blacklisted_accounts: HashSet<Pubkey>,

    bundle_stage_leader_metrics: BundleStageLeaderMetrics,
}

impl PaladinBundleStage {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn(
        exit: Arc<AtomicBool>,
        paladin_rx: Receiver<Vec<PacketBundle>>,
        cluster_info: Arc<ClusterInfo>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        replay_vote_sender: ReplayVoteSender,
        log_messages_bytes_limit: Option<usize>,
        tip_manager: TipManager,
        bundle_account_locker: BundleAccountLocker,
        prioritization_fee_cache: Arc<PrioritizationFeeCache>,
        preallocated_bundle_cost: u64,
    ) -> std::thread::JoinHandle<()> {
        info!("Spawning PaladinBundleStage");

        let transaction_recorder = poh_recorder.read().unwrap().new_recorder();
        let decision_maker = DecisionMaker::new(cluster_info.id(), poh_recorder.clone());
        let committer = Committer::new(
            transaction_status_sender,
            replay_vote_sender,
            prioritization_fee_cache,
        );

        let reserved_ticks = poh_recorder
            .read()
            .unwrap()
            .ticks_per_slot()
            .saturating_mul(8)
            .saturating_div(10);
        let reserved_space = BundleReservedSpaceManager::new(
            MAX_BLOCK_UNITS,
            preallocated_bundle_cost,
            reserved_ticks,
        );

        std::thread::Builder::new()
            .name("paladin-bundle-stage".to_string())
            .spawn(move || {
                PaladinBundleStage {
                    exit,

                    paladin_rx,

                    decision_maker,
                    poh_recorder,

                    bundles: BTreeMap::default(),
                    bundle_id: BundleIdGenerator::default(),
                    batches: [vec![], vec![]],
                    batches_last: Instant::now(),

                    tip_manager,
                    bundle_account_locker,
                    committer,
                    transaction_recorder,
                    qos_service: QosService::new(PALADIN_BUNDLE_STAGE_ID),
                    reserved_space,
                    log_messages_bytes_limit,
                    // TODO: Add funnel here and in jito + banking threads once that is live.
                    blacklisted_accounts: HashSet::from_iter([jito_tip_payment::ID]),

                    bundle_stage_leader_metrics: BundleStageLeaderMetrics::new(
                        PALADIN_BUNDLE_STAGE_ID,
                    ),
                }
                .run()
            })
            .unwrap()
    }

    fn run(mut self) {
        // This state represents our current locks which is intentionally kept
        // separate to our thread struct.
        let bundle_account_locker = self.bundle_account_locker.clone();
        let mut locked_bundles: HashMap<String, _> = HashMap::default();

        while !self.exit.load(Ordering::Relaxed) {
            // Wait for initial bundles.
            let timeout = match self.bundles.is_empty() {
                true => Duration::from_millis(100),
                false => Duration::from_millis(0),
            };
            match self.paladin_rx.recv_timeout(timeout) {
                Ok(bundles) => {
                    self.drain_socket(&bundle_account_locker, &mut locked_bundles, bundles)
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            };

            // Check if we should drain a batch.
            if self.batches_last.elapsed() > BATCH_INTERVAL {
                let bank = self.poh_recorder.read().unwrap().latest_bank();
                for (immutable, sanitized) in self.batches[0].drain(..) {
                    // NB: We filter duplicate bundles to ensure we always have
                    // the same number of locked and sanitized bundles.
                    if locked_bundles.contains_key(&sanitized.bundle_id) {
                        // TODO: Metrics.
                        continue;
                    }

                    Self::lock_bundle(
                        &bank,
                        &bundle_account_locker,
                        &mut locked_bundles,
                        &mut self.bundles,
                        immutable,
                        sanitized,
                        self.bundle_id.next(),
                    );
                }
                self.batches_last = Instant::now();

                // Move the non-empty batch to the front.
                let [first, second] = self.batches.each_mut();
                std::mem::swap(first, second);
            }

            let decision = self.decision_maker.make_consume_or_forward_decision();
            let (bundle_action, banking_action) = self
                .bundle_stage_leader_metrics
                .check_leader_slot_boundary(decision.bank_start(), None);
            self.bundle_stage_leader_metrics
                .apply_action(bundle_action, banking_action);

            match decision {
                BufferedPacketsDecision::Consume(bank_start) => {
                    for bundle in self.consume_buffered_bundles(&bank_start).into_keys() {
                        debug!("Included or dropped; bundle_id={bundle}");

                        assert!(locked_bundles.remove(&bundle).is_some());
                    }

                    assert_eq!(self.bundles.len(), locked_bundles.len());
                }
                BufferedPacketsDecision::Forward => {
                    for bundle in std::mem::take(&mut self.bundles).into_values() {
                        assert!(locked_bundles.remove(bundle.bundle_id()).is_some());
                    }
                }
                BufferedPacketsDecision::ForwardAndHold | BufferedPacketsDecision::Hold => {}
            }
        }
    }

    fn drain_socket<'lock>(
        &mut self,
        bundle_account_locker: &'lock BundleAccountLocker,
        locked_bundles: &mut HashMap<String, LockedSanitizedBundle<'lock>>,
        bundles: Vec<PacketBundle>,
    ) {
        // Drain the socket channel.
        let new_bundles: Vec<_> = [bundles]
            .into_iter()
            .chain(std::iter::from_fn(|| self.paladin_rx.try_recv().ok()))
            .flatten()
            .collect();

        // Take all necessary locks.
        let bank = self.poh_recorder.read().unwrap().latest_bank();
        for mut bundle in new_bundles {
            if bundle.batch.len() != 1 {
                eprintln!("ERR: Invalid P3 bundle; bundle_id={}", bundle.bundle_id);
            }

            // NB: We filter duplicate bundles to ensure we always have the same
            // number of locked and sanitized bundles.
            if locked_bundles.contains_key(&bundle.bundle_id) {
                // TODO: Metrics.
                continue;
            }

            let immutable = match ImmutableDeserializedBundle::new(
                &mut bundle,
                Some(MAX_PACKETS_PER_BUNDLE),
                &|packet: ImmutableDeserializedPacket| {
                    // see packet_receiver.rs
                    packet.check_insufficent_compute_unit_limit()?;
                    packet.check_excessive_precompiles()?;
                    Ok(packet)
                },
            ) {
                Ok(bundle) => bundle,
                Err(err) => {
                    warn!(
                        "Failed to convert bundle; bundle_id={}; err={err}",
                        bundle.bundle_id
                    );

                    continue;
                }
            };

            let sanitized = match immutable.build_sanitized_bundle(
                &bank,
                &self.blacklisted_accounts,
                &mut TransactionErrorMetrics::default(),
            ) {
                Ok(sanitized_bundle) => sanitized_bundle,
                Err(err) => {
                    warn!(
                        "Failed to deserialize paladin bundle; bundle_id={}; err={err}",
                        immutable.bundle_id()
                    );

                    continue;
                }
            };

            // If this transaction touches a DeFi program, it may carry MEV and
            // thus we want to delay + batch it to ensure a more competitive
            // auction process.
            if sanitized.transactions[0]
                .message()
                .account_keys()
                .iter()
                .any(|key| AMM_PROGRAMS.contains(key))
            {
                self.batches[1].push((immutable, sanitized));
                continue;
            }

            // Lock.
            Self::lock_bundle(
                &bank,
                bundle_account_locker,
                locked_bundles,
                &mut self.bundles,
                immutable,
                sanitized,
                self.bundle_id.next(),
            );
        }
    }

    fn lock_bundle<'lock>(
        bank: &Arc<Bank>,
        bundle_account_locker: &'lock BundleAccountLocker,
        locked_bundles: &mut HashMap<String, LockedSanitizedBundle<'lock>>,
        bundles: &mut BTreeMap<BundlePriorityId, ImmutableDeserializedBundle>,
        immutable: ImmutableDeserializedBundle,
        sanitized: SanitizedBundle,
        id: u64,
    ) {
        let priority = Self::compute_priority(bank, &sanitized);

        match (LockedSanitizedBundleTryBuilder {
            sanitized,
            locked_builder: |sanitized| {
                bundle_account_locker.prepare_locked_bundle(sanitized, bank)
            },
        }
        .try_build())
        {
            Ok(combined) => {
                debug!("Locked bundle built; bundle_id={}", immutable.bundle_id());

                // NB: Silence locked unused warning.
                let _ = combined.borrow_locked();

                assert!(bundles
                    .insert(BundlePriorityId { priority, id }, immutable,)
                    .is_none());
                assert!(locked_bundles
                    .insert(combined.borrow_sanitized().bundle_id.clone(), combined)
                    .is_none());
            }
            Err(err) => warn!(
                "Failed to lock; bundle_id={}; err={err}",
                immutable.bundle_id()
            ),
        }
    }

    /// Returns the bundles that were processed/dropped.
    #[must_use]
    fn consume_buffered_bundles(
        &mut self,
        bank_start: &BankStart,
    ) -> HashMap<String, BundlePriorityId> {
        // Drain our latest bundles.
        let mut bundles_start: HashMap<_, _> = self
            .bundles
            .iter()
            .rev()
            .map(|(priority, bundle)| (bundle.bundle_id().to_string(), *priority))
            .collect();
        let bundles: Vec<_> = std::mem::take(&mut self.bundles).into_values().collect();
        let mut unprocessed_transaction_storage =
            UnprocessedTransactionStorage::new_bundle_storage();
        unprocessed_transaction_storage.insert_bundles(bundles);

        // Process any bundles we can.
        let _reached_end_of_slot = unprocessed_transaction_storage.process_bundles(
            bank_start.working_bank.clone(),
            &mut self.bundle_stage_leader_metrics,
            &self.blacklisted_accounts,
            |bundles, bundle_stage_leader_metrics| {
                Self::do_process_bundles(
                    &self.bundle_account_locker,
                    &self.committer,
                    &self.transaction_recorder,
                    &self.qos_service,
                    &self.log_messages_bytes_limit,
                    MAX_BUNDLE_RETRY_DURATION,
                    &self.reserved_space,
                    bundles,
                    self.tip_manager.get_tip_accounts(),
                    bank_start,
                    bundle_stage_leader_metrics,
                )
            },
        );

        // Re-buffer any unprocessed bundles.
        let mut bundle_storage = match unprocessed_transaction_storage {
            UnprocessedTransactionStorage::BundleStorage(storage) => storage,
            _ => unreachable!(),
        };

        // Remove the bundles that did not get processed from `bundles_start`.
        for unprocessed in bundle_storage
            .unprocessed_bundle_storage
            .drain(..)
            .chain(bundle_storage.cost_model_buffered_bundle_storage.drain(..))
        {
            let priority = bundles_start.remove(unprocessed.bundle_id()).unwrap();
            self.bundles.insert(priority, unprocessed);
        }

        // `bundles_start` now contains the bundles that **were** processed. We must return this set
        // so we can manually remove these locks.
        bundles_start
    }

    #[allow(clippy::too_many_arguments)]
    fn do_process_bundles(
        bundle_account_locker: &BundleAccountLocker,
        committer: &Committer,
        recorder: &TransactionRecorder,
        qos_service: &QosService,
        log_messages_bytes_limit: &Option<usize>,
        max_bundle_retry_duration: Duration,
        reserved_space: &BundleReservedSpaceManager,
        bundles: &[(ImmutableDeserializedBundle, SanitizedBundle)],
        tip_accounts: &HashSet<Pubkey>,
        bank_start: &BankStart,
        bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
    ) -> Vec<Result<(), BundleExecutionError>> {
        // TODO: Can we avoid this needless step?
        #[allow(clippy::needless_collect)]
        let (locked_bundle_results, locked_bundles_elapsed) = measure_time!(
            bundles
                .iter()
                .map(|(_, sanitized_bundle)| {
                    bundle_account_locker
                        .prepare_locked_bundle(sanitized_bundle, &bank_start.working_bank)
                        .map(|locked_bundle| (locked_bundle, sanitized_bundle))
                })
                .collect::<Vec<_>>(),
            "locked_bundles_elapsed"
        );
        bundle_stage_leader_metrics
            .bundle_stage_metrics_tracker()
            .increment_locked_bundle_elapsed_us(locked_bundles_elapsed.as_us());

        let (execution_results, execute_locked_bundles_elapsed) =
            measure_time!(locked_bundle_results
                .into_iter()
                .map(|r| match r {
                    Ok((locked_bundle, sanitized_bundle)) => {
                        let (r, measure) = measure_us!(Self::process_bundle(
                            committer,
                            recorder,
                            qos_service,
                            log_messages_bytes_limit,
                            max_bundle_retry_duration,
                            reserved_space,
                            locked_bundle,
                            sanitized_bundle,
                            tip_accounts,
                            bank_start,
                            bundle_stage_leader_metrics,
                        ));
                        bundle_stage_leader_metrics
                            .leader_slot_metrics_tracker()
                            .increment_process_packets_transactions_us(measure);
                        r
                    }
                    Err(_) => {
                        Err(BundleExecutionError::LockError)
                    }
                })
                .collect::<Vec<_>>());

        bundle_stage_leader_metrics
            .bundle_stage_metrics_tracker()
            .increment_execute_locked_bundles_elapsed_us(execute_locked_bundles_elapsed.as_us());
        execution_results.iter().for_each(|result| {
            bundle_stage_leader_metrics
                .bundle_stage_metrics_tracker()
                .increment_bundle_execution_result(result);
        });

        execution_results
    }

    #[allow(clippy::too_many_arguments)]
    fn process_bundle(
        committer: &Committer,
        recorder: &TransactionRecorder,
        qos_service: &QosService,
        log_messages_bytes_limit: &Option<usize>,
        max_bundle_retry_duration: Duration,
        reserved_space: &BundleReservedSpaceManager,
        locked_bundle: LockedBundle,
        sanitized_bundle: &SanitizedBundle,
        tip_accounts: &HashSet<Pubkey>,
        bank_start: &BankStart,
        bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
    ) -> Result<(), BundleExecutionError> {
        if !Bank::should_bank_still_be_processing_txs(
            &bank_start.bank_creation_time,
            bank_start.working_bank.ns_per_slot,
        ) {
            return Err(BundleExecutionError::BankProcessingTimeLimitReached);
        }

        BundleConsumer::update_qos_and_execute_record_commit_bundle(
            committer,
            recorder,
            qos_service,
            log_messages_bytes_limit,
            max_bundle_retry_duration,
            reserved_space,
            locked_bundle,
            sanitized_bundle,
            tip_accounts,
            bank_start,
            bundle_stage_leader_metrics,
            false, // fifo
            Some(MIN_MICRO_LAMPORTS),
            true, // include_reverted
        )?;

        Ok(())
    }

    /// Computes the priority of a P3 transaction.
    ///
    /// # Panics
    ///
    /// If the caller provides a bundle len != 1.
    fn compute_priority(bank: &Bank, sanitized: &SanitizedBundle) -> u64 {
        assert_eq!(sanitized.transactions.len(), 1);
        let tx = &sanitized.transactions[0];

        let Ok(compute_budget_limits) = process_compute_budget_instructions(
            tx.message()
                .program_instructions_iter()
                .map(|(key, ix)| (key, ix.into())),
            &bank.feature_set,
        ) else {
            return 0;
        };

        SchedulerController::<Arc<ClusterInfo>>::calculate_priority_and_cost(
            tx,
            &compute_budget_limits.into(),
            bank,
        )
        .0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BundlePriorityId {
    priority: u64,
    id: u64,
}

impl Ord for BundlePriorityId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.priority.cmp(&other.priority) {
            std::cmp::Ordering::Equal => Reverse(self.id).cmp(&Reverse(other.id)),
            ord => ord,
        }
    }
}

impl PartialOrd for BundlePriorityId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[self_referencing]
struct LockedSanitizedBundle<'a> {
    sanitized: SanitizedBundle,
    #[covariant]
    #[borrows(sanitized)]
    locked: LockedBundle<'a, 'this>,
}

#[derive(Debug, Default)]
struct BundleIdGenerator {
    next: u64,
}

impl BundleIdGenerator {
    fn next(&mut self) -> u64 {
        let next = self.next;
        self.next = self.next.wrapping_add(1);

        next
    }
}

#[cfg(test)]
mod tests {
    use {super::*, std::collections::BTreeSet};

    #[test]
    fn transaction_priority_id_ordering() {
        let high_prio_high_id = BundlePriorityId {
            priority: u64::MAX,
            id: u64::MAX,
        };
        let high_prio_low_id = BundlePriorityId {
            priority: u64::MAX,
            id: 0,
        };
        let low_prio_high_id = BundlePriorityId {
            priority: 0,
            id: u64::MAX,
        };
        let low_prio_low_id = BundlePriorityId { priority: 0, id: 0 };

        // Check sort order in array.
        let mut sorted = [
            low_prio_low_id,
            low_prio_high_id,
            high_prio_low_id,
            high_prio_high_id,
        ];
        sorted.sort();
        assert_eq!(
            sorted,
            [
                low_prio_high_id,
                low_prio_low_id,
                high_prio_high_id,
                high_prio_low_id,
            ]
        );

        // Check BTreeMap iteration order.
        assert_eq!(
            BTreeSet::from_iter(sorted)
                .into_iter()
                .rev()
                .collect::<Vec<_>>(),
            vec![
                high_prio_low_id,
                high_prio_high_id,
                low_prio_low_id,
                low_prio_high_id,
            ]
        );
    }
}
