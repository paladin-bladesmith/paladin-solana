use {
    crate::{
        banking_stage::{
            committer::CommitTransactionDetails,
            decision_maker::{BufferedPacketsDecision, DecisionMaker},
            leader_slot_metrics::ProcessTransactionsSummary,
            leader_slot_timing_metrics::LeaderExecuteAndCommitTimings,
            qos_service::QosService,
            unprocessed_transaction_storage::UnprocessedTransactionStorage,
        },
        bundle_stage::{
            bundle_account_locker::{BundleAccountLocker, LockedBundle},
            bundle_consumer::BundleConsumer,
            bundle_stage_leader_metrics::BundleStageLeaderMetrics,
            committer::Committer,
        },
        consensus_cache_updater::ConsensusCacheUpdater,
        immutable_deserialized_bundle::ImmutableDeserializedBundle,
        packet_bundle::PacketBundle,
        proxy::block_engine_stage::BlockBuilderFeeInfo,
        tip_manager::TipManager,
    },
    crossbeam_channel::Receiver,
    solana_accounts_db::transaction_error_metrics::TransactionErrorMetrics,
    solana_bundle::{
        bundle_execution::{load_and_execute_bundle, BundleExecutionMetrics},
        BundleExecutionError, BundleExecutionResult, TipError,
    },
    solana_cost_model::transaction_cost::TransactionCost,
    solana_gossip::cluster_info::ClusterInfo,
    solana_measure::{measure, measure_us},
    solana_poh::poh_recorder::{
        BankStart, PohRecorder, RecordTransactionsSummary, TransactionRecorder,
    },
    solana_runtime::bank::Bank,
    solana_sdk::{
        bundle::SanitizedBundle,
        clock::{Slot, MAX_PROCESSING_AGE},
        feature_set,
        pubkey::Pubkey,
        transaction::{self},
    },
    std::{
        collections::{HashSet, VecDeque},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex, RwLock,
        },
        time::{Duration, Instant},
    },
};

pub(crate) struct PaladinBundleStage {
    exit: Arc<AtomicBool>,

    paladin_rx: Receiver<Vec<PacketBundle>>,

    decision_maker: DecisionMaker,

    bundles: Vec<PacketBundle>,
    bundle_stage_leader_metrics: BundleStageLeaderMetrics,
    bundle_account_locker: BundleAccountLocker,
    tip_manager: TipManager,
    cluster_info: Arc<ClusterInfo>,
    block_builder_fee_info: BlockBuilderFeeInfo,
    committer: Committer,
    transaction_recorder: TransactionRecorder,
    qos_service: QosService,
    log_messages_bytes_limit: Option<usize>,
    max_bundle_retry_duration: Duration,
}

impl PaladinBundleStage {
    pub(crate) fn spawn(
        exit: Arc<AtomicBool>,
        paladin_rx: Receiver<Vec<PacketBundle>>,
        (cluster_info, poh_recorder): (Arc<ClusterInfo>, Arc<RwLock<PohRecorder>>),
    ) -> std::thread::JoinHandle<()> {
        info!("Spawning PaladinBundleStage");

        let decision_maker = DecisionMaker::new(cluster_info.id(), poh_recorder);

        std::thread::Builder::new()
            .name("paladin-bundle-stage".to_string())
            .spawn(move || {
                PaladinBundleStage {
                    exit,

                    paladin_rx,

                    decision_maker,

                    bundles: Vec::default(),
                }
                .run()
            })
            .unwrap()
    }

    fn run(mut self) {
        while !self.exit.load(Ordering::Relaxed) {
            // Wait for initial bundles.
            let timeout = match self.bundles.is_empty() {
                true => Duration::from_millis(100),
                false => Duration::from_millis(0),
            };
            match self.paladin_rx.recv_timeout(timeout) {
                Ok(bundles) => self.bundles = bundles,
                Err(_) => continue,
            };

            // Drain the socket channel.
            while let Ok(bundles) = self.paladin_rx.try_recv() {
                self.bundles = bundles;
            }

            // Process any bundles we have.
            let decision = self.decision_maker.make_consume_or_forward_decision();
            if let BufferedPacketsDecision::Consume(bank_start) = decision {
                self.consume_buffered_bundles(&bank_start);
            }
        }
    }

    fn consume_buffered_bundles(&mut self, bank_start: &BankStart) {
        // Drain all bundles.
        let bundles = self
            .bundles
            .drain(..)
            .filter_map(|mut bundle| {
                match ImmutableDeserializedBundle::new(
                    &mut bundle,
                    Some(todo!("Find where this is set")),
                ) {
                    Ok(bundle) => Some(bundle),
                    Err(err) => {
                        warn!("Failed to convert bundle; err={err}");
                        None
                    }
                }
            })
            .collect();
        let mut unprocessed_transaction_storage = UnprocessedTransactionStorage::new_bundle_storage(
            bundles,
            VecDeque::with_capacity(self.bundles.len()),
        );

        // Process bundles.
        unprocessed_transaction_storage.process_bundles(
            bank_start.working_bank.clone(),
            &mut self.bundle_stage_leader_metrics,
            &HashSet::default(),
            |bundles, bundle_stage_leader_metrics| {
                Self::do_process_bundles(
                    &self.bundle_account_locker,
                    &self.cluster_info,
                    &self.committer,
                    &self.transaction_recorder,
                    &self.qos_service,
                    &self.log_messages_bytes_limit,
                    self.max_bundle_retry_duration,
                    bundles,
                    bank_start,
                    bundle_stage_leader_metrics,
                )
            },
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn do_process_bundles(
        bundle_account_locker: &BundleAccountLocker,
        cluster_info: &Arc<ClusterInfo>,
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
                Ok(locked_bundle) => {
                    let (r, measure) = measure_us!(Self::process_bundle(
                        bundle_account_locker,
                        cluster_info,
                        committer,
                        recorder,
                        qos_service,
                        log_messages_bytes_limit,
                        max_bundle_retry_duration,
                        &locked_bundle,
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
        bundle_account_locker: &BundleAccountLocker,
        cluster_info: &Arc<ClusterInfo>,
        committer: &Committer,
        recorder: &TransactionRecorder,
        qos_service: &QosService,
        log_messages_bytes_limit: &Option<usize>,
        max_bundle_retry_duration: Duration,
        locked_bundle: &LockedBundle,
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
            locked_bundle.sanitized_bundle(),
            bank_start,
            bundle_stage_leader_metrics,
        )?;

        Ok(())
    }
}
