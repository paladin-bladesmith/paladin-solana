use {
    crate::{
        banking_stage::{
            decision_maker::{BufferedPacketsDecision, DecisionMaker},
            qos_service::QosService,
            unprocessed_transaction_storage::UnprocessedTransactionStorage,
        },
        bundle_stage::{
            bundle_consumer::BundleConsumer, bundle_stage_leader_metrics::BundleStageLeaderMetrics,
            committer::Committer,
        },
        consensus_cache_updater::ConsensusCacheUpdater,
        immutable_deserialized_bundle::ImmutableDeserializedBundle,
        packet_bundle::PacketBundle,
    },
    crossbeam_channel::Receiver,
    hashbrown::HashMap,
    ouroboros::self_referencing,
    solana_accounts_db::transaction_error_metrics::TransactionErrorMetrics,
    solana_bundle::{
        bundle_account_locker::{BundleAccountLocker, LockedBundle},
        BundleExecutionError,
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::blockstore_processor::TransactionStatusSender,
    solana_measure::{measure, measure_us},
    solana_poh::poh_recorder::{BankStart, PohRecorder, TransactionRecorder},
    solana_runtime::{bank::Bank, prioritization_fee_cache::PrioritizationFeeCache},
    solana_sdk::{bundle::SanitizedBundle, pubkey::Pubkey},
    solana_vote::vote_sender_types::ReplayVoteSender,
    std::{
        collections::{HashSet, VecDeque},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        time::Duration,
    },
};

const PALADIN_BUNDLE_STAGE_ID: u32 = 2000;
const MAX_BUNDLE_RETRY_DURATION: Duration = Duration::from_millis(40);
const MAX_PACKETS_PER_BUNDLE: usize = 5;

pub(crate) struct PaladinBundleStage {
    exit: Arc<AtomicBool>,

    paladin_rx: Receiver<Vec<PacketBundle>>,

    decision_maker: DecisionMaker,
    poh_recorder: Arc<RwLock<PohRecorder>>,

    bundles: Vec<ImmutableDeserializedBundle>,
    bundle_stage_leader_metrics: BundleStageLeaderMetrics,
    bundle_account_locker: BundleAccountLocker,
    committer: Committer,
    transaction_recorder: TransactionRecorder,
    qos_service: QosService,
    log_messages_bytes_limit: Option<usize>,
    consensus_cache_updater: ConsensusCacheUpdater,
    blacklisted_accounts: HashSet<Pubkey>,
}

impl PaladinBundleStage {
    pub(crate) fn spawn(
        exit: Arc<AtomicBool>,
        paladin_rx: Receiver<Vec<PacketBundle>>,
        cluster_info: Arc<ClusterInfo>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        replay_vote_sender: ReplayVoteSender,
        log_messages_bytes_limit: Option<usize>,
        bundle_account_locker: BundleAccountLocker,
        prioritization_fee_cache: Arc<PrioritizationFeeCache>,
    ) -> std::thread::JoinHandle<()> {
        info!("Spawning PaladinBundleStage");

        let transaction_recorder = poh_recorder.read().unwrap().new_recorder();
        let decision_maker = DecisionMaker::new(cluster_info.id(), poh_recorder.clone());
        let committer = Committer::new(
            transaction_status_sender,
            replay_vote_sender,
            prioritization_fee_cache,
        );

        std::thread::Builder::new()
            .name("paladin-bundle-stage".to_string())
            .spawn(move || {
                PaladinBundleStage {
                    exit,

                    paladin_rx,

                    decision_maker,
                    poh_recorder,

                    bundles: Vec::default(),
                    bundle_stage_leader_metrics: BundleStageLeaderMetrics::new(
                        PALADIN_BUNDLE_STAGE_ID,
                    ),
                    bundle_account_locker,
                    committer,
                    transaction_recorder,
                    qos_service: QosService::new(PALADIN_BUNDLE_STAGE_ID),
                    log_messages_bytes_limit,
                    consensus_cache_updater: ConsensusCacheUpdater::default(),
                    blacklisted_accounts: HashSet::default(),
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
            let bundles = match self.paladin_rx.recv_timeout(timeout) {
                Ok(bundles) => bundles,
                Err(_) => continue,
            };

            // Drain the socket channel.
            let mut arbs = None;
            let mut new_bundles = Vec::default();
            for bundles in std::iter::once(bundles)
                .chain(std::iter::from_fn(|| self.paladin_rx.try_recv().ok()))
            {
                match &bundles.first().unwrap().bundle_id.chars().next().unwrap() {
                    'R' => new_bundles.extend(bundles),
                    'A' => {
                        assert!(bundles
                            .iter()
                            .all(|bundle| bundle.bundle_id.starts_with('A')));
                        arbs = Some(bundles);
                    }
                    prefix => panic!("Unexpected bundle ID prefix; prefix={prefix}"),
                }
            }

            // Drop any arb bundles if we have a fresher set.
            if arbs.is_some() {
                self.bundles.retain(|bundle| {
                    let drop = bundle.bundle_id().starts_with('A');
                    if drop {
                        println!("DROP: {}", bundle.bundle_id());
                        assert!(locked_bundles.remove(bundle.bundle_id()).is_some());
                    }

                    drop
                });
            }

            // Take all necessary locks, processing the arbs first.
            let bank = self.poh_recorder.read().unwrap().latest_bank();
            for mut bundle in arbs.into_iter().flatten().chain(new_bundles) {
                println!("BUNDLE: {}", bundle.bundle_id);
                let immutable = match ImmutableDeserializedBundle::new(
                    &mut bundle,
                    Some(MAX_PACKETS_PER_BUNDLE),
                ) {
                    Ok(bundle) => bundle,
                    Err(err) => {
                        warn!("Failed to convert bundle; err={err}");
                        continue;
                    }
                };

                let sanitized = match immutable.build_sanitized_bundle(
                    &bank,
                    &HashSet::default(),
                    &mut TransactionErrorMetrics::default(),
                ) {
                    Ok(sanitized_bundle) => sanitized_bundle,
                    Err(err) => {
                        warn!("Failed to deserialize paladin bundle; err={err}");

                        continue;
                    }
                };

                // Lock.
                match (LockedSanitizedBundleTryBuilder {
                    sanitized,
                    locked_builder: |sanitized| {
                        bundle_account_locker.prepare_locked_bundle(sanitized, &bank)
                    },
                }
                .try_build())
                {
                    Ok(combined) => {
                        println!("LOCKED: {}", combined.borrow_sanitized().bundle_id);
                        let prev = locked_bundles
                            .insert(combined.borrow_sanitized().bundle_id.clone(), combined);
                        assert!(prev.is_none());
                    }
                    Err(err) => warn!("Failed to lock; err={err}"),
                }
            }

            // Update our locks if bank has started.
            let mut decision = self.decision_maker.make_consume_or_forward_decision();
            while self.paladin_rx.is_empty() {
                let (bundle_action, banking_action) = self
                    .bundle_stage_leader_metrics
                    .check_leader_slot_boundary(decision.bank_start());
                self.bundle_stage_leader_metrics
                    .apply_action(bundle_action, banking_action);

                match decision {
                    BufferedPacketsDecision::Consume(bank_start) => {
                        for bundle in self.consume_buffered_bundles(&bank_start) {
                            println!("REMOVE: {bundle}");
                            assert!(locked_bundles.remove(&bundle).is_some());
                        }
                    }
                    _ => break,
                }

                decision = self.decision_maker.make_consume_or_forward_decision();
            }
        }
    }

    /// Returns the bundles that were processed/dropped.
    #[must_use]
    fn consume_buffered_bundles(&mut self, bank_start: &BankStart) -> HashSet<String> {
        self.maybe_update_blacklist(bank_start);

        // Drain our latest bundles.
        let bundles: VecDeque<_> = self.bundles.drain(..).collect();
        let mut bundles_start: HashSet<_> = bundles
            .iter()
            .map(|bundle| bundle.bundle_id().to_string())
            .collect();
        let mut unprocessed_transaction_storage = UnprocessedTransactionStorage::new_bundle_storage(
            bundles,
            VecDeque::with_capacity(self.bundles.len()),
        );

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
                    bundles,
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

        // Remove the bundles that did not get processed from our bundle ID HashSet.
        for unprocessed in bundle_storage.unprocessed_bundle_storage.drain(..) {
            bundles_start.remove(unprocessed.bundle_id());
            self.bundles.push(unprocessed);
        }

        // `bundles_start` now contains the bundles that **were** processed. We must return this set so we can manually remove these locks.
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
        bundles: &[(ImmutableDeserializedBundle, SanitizedBundle)],
        bank_start: &BankStart,
        bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
    ) -> Vec<Result<(), BundleExecutionError>> {
        // BundleAccountLocker holds RW locks for ALL accounts in ALL transactions within a single bundle.
        // By pre-locking bundles before they're ready to be processed, it will prevent BankingStage from
        // grabbing those locks so BundleStage can process as fast as possible.
        // A LockedBundle is similar to TransactionBatch; once its dropped the locks are released.
        #[allow(clippy::needless_collect)]
        let (locked_bundle_results, locked_bundles_elapsed) = measure!(
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

        let (execution_results, execute_locked_bundles_elapsed) = measure!(locked_bundle_results
            .into_iter()
            .map(|r| match r {
                Ok((locked_bundle, sanitized_bundle)) => {
                    let (r, measure) = measure_us!(Self::process_bundle(
                        committer,
                        recorder,
                        qos_service,
                        log_messages_bytes_limit,
                        max_bundle_retry_duration,
                        locked_bundle,
                        sanitized_bundle,
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
        locked_bundle: LockedBundle,
        sanitized_bundle: &SanitizedBundle,
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
            None,
            locked_bundle,
            sanitized_bundle,
            bank_start,
            bundle_stage_leader_metrics,
            false,
        )?;

        Ok(())
    }

    fn maybe_update_blacklist(&mut self, bank_start: &BankStart) {
        if self
            .consensus_cache_updater
            .maybe_update(&bank_start.working_bank)
        {
            self.blacklisted_accounts = self
                .consensus_cache_updater
                .consensus_accounts_cache()
                .iter()
                .chain(std::iter::once(&jito_tip_payment::id()))
                .cloned()
                .collect();

            debug!(
                "updated blacklist with {} accounts",
                self.blacklisted_accounts.len()
            );
        }
    }
}

#[self_referencing]
struct LockedSanitizedBundle<'a> {
    sanitized: SanitizedBundle,
    #[covariant]
    #[borrows(sanitized)]
    locked: LockedBundle<'a, 'this>,
}