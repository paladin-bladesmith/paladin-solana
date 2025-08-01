use {
    crate::{
        banking_stage::{
            committer::CommitTransactionDetails,
            leader_slot_metrics::{CommittedTransactionsCounts, ProcessTransactionsSummary},
            leader_slot_timing_metrics::LeaderExecuteAndCommitTimings,
            qos_service::QosService,
        },
        bundle_stage::{
            bundle_account_locker::{BundleAccountLocker, LockedBundle},
            bundle_stage_leader_metrics::BundleStageLeaderMetrics,
            bundle_storage::BundleStorage,
            committer::Committer,
        },
        immutable_deserialized_bundle::ImmutableDeserializedBundle,
        proxy::block_engine_stage::BlockBuilderFeeInfo,
        tip_manager::TipManager,
    },
    itertools::Itertools,
    solana_bundle::{
        bundle_execution::{load_and_execute_bundle, BundleExecutionMetrics},
        BundleExecutionError, BundleExecutionResult, SanitizedBundle, TipError,
    },
    solana_clock::{Slot, MAX_PROCESSING_AGE},
    solana_cost_model::transaction_cost::TransactionCost,
    solana_entry::entry::hash_transactions,
    solana_gossip::cluster_info::ClusterInfo,
    solana_measure::measure_us,
    solana_poh::{
        poh_recorder::BankStart,
        transaction_recorder::{
            RecordTransactionsSummary, RecordTransactionsTimings, TransactionRecorder,
        },
    },
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
    solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    solana_transaction::{sanitized::SanitizedTransaction, versioned::VersionedTransaction},
    solana_transaction_error::TransactionResult,
    std::{
        collections::HashSet,
        num::Saturating,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    },
};

type ReserveBundleBlockspaceResult<'a> = BundleExecutionResult<(
    Vec<TransactionResult<TransactionCost<'a, RuntimeTransaction<SanitizedTransaction>>>>,
    u64,
)>;

pub struct ExecuteRecordCommitResult {
    commit_transaction_details: Vec<CommitTransactionDetails>,
    result: BundleExecutionResult<()>,
    execution_metrics: BundleExecutionMetrics,
    execute_and_commit_timings: LeaderExecuteAndCommitTimings,
    transaction_error_counter: TransactionErrorMetrics,
}

pub struct BundleConsumer {
    committer: Committer,
    transaction_recorder: TransactionRecorder,
    qos_service: QosService,
    log_messages_bytes_limit: Option<usize>,

    tip_manager: TipManager,
    last_tip_update_slot: Slot,

    blacklisted_accounts: HashSet<Pubkey>,

    // Manages account locks across multiple transactions within a bundle to prevent race conditions
    // with BankingStage
    bundle_account_locker: BundleAccountLocker,

    block_builder_fee_info: Arc<Mutex<BlockBuilderFeeInfo>>,

    max_bundle_retry_duration: Duration,

    cluster_info: Arc<ClusterInfo>,
}

impl BundleConsumer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        committer: Committer,
        transaction_recorder: TransactionRecorder,
        qos_service: QosService,
        log_messages_bytes_limit: Option<usize>,
        tip_manager: TipManager,
        bundle_account_locker: BundleAccountLocker,
        block_builder_fee_info: Arc<Mutex<BlockBuilderFeeInfo>>,
        max_bundle_retry_duration: Duration,
        cluster_info: Arc<ClusterInfo>,
    ) -> Self {
        let blacklisted_accounts = HashSet::from_iter([tip_manager.tip_payment_program_id()]);
        Self {
            committer,
            transaction_recorder,
            qos_service,
            log_messages_bytes_limit,
            tip_manager,
            // MAX because sending tips during slot 0 in tests doesn't work
            last_tip_update_slot: u64::MAX,
            blacklisted_accounts,
            bundle_account_locker,
            block_builder_fee_info,
            max_bundle_retry_duration,
            cluster_info,
        }
    }

    // A bundle is a series of transactions to be executed sequentially, atomically, and all-or-nothing.
    // Sequentially:
    //  - Transactions are executed in order
    // Atomically:
    //  - All transactions in a bundle get recoded to PoH and committed to the bank in the same slot. Account locks
    //  for all accounts in all transactions in a bundle are held during the entire execution to remove POH record race conditions
    //  with transactions in BankingStage.
    // All-or-nothing:
    //  - All transactions are committed or none. Modified state for the entire bundle isn't recorded to PoH and committed to the
    //  bank until all transactions in the bundle have executed.
    //
    // Some corner cases to be aware of when working with BundleStage:
    // A bundle is not allowed to call the Tip Payment program in a bundle (or BankingStage).
    // - This is to avoid stealing of tips by malicious parties with bundles that crank the tip
    // payment program and set the tip receiver to themself.
    // A bundle is not allowed to touch consensus-related accounts
    //  - This is to avoid stalling the voting BankingStage threads.
    pub fn consume_buffered_bundles(
        &mut self,
        bank_start: &BankStart,
        bundle_storage: &mut BundleStorage,
        bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
    ) {
        let reached_end_of_slot = bundle_storage.process_bundles(
            bank_start.working_bank.clone(),
            bundle_stage_leader_metrics,
            &self.blacklisted_accounts,
            &self.tip_manager.get_tip_accounts(),
            |bundles, bundle_stage_leader_metrics| {
                Self::do_process_bundles(
                    &self.bundle_account_locker,
                    &self.tip_manager,
                    &mut self.last_tip_update_slot,
                    &self.cluster_info,
                    &self.block_builder_fee_info,
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

        if reached_end_of_slot {
            bundle_stage_leader_metrics
                .leader_slot_metrics_tracker()
                .set_end_of_slot_unprocessed_buffer_len(bundle_storage.len() as u64);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn do_process_bundles(
        bundle_account_locker: &BundleAccountLocker,
        tip_manager: &TipManager,
        last_tip_updated_slot: &mut Slot,
        cluster_info: &Arc<ClusterInfo>,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
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
        let (locked_bundle_results, locked_bundles_elapsed_us) = measure_us!(bundles
            .iter()
            .map(|(_, sanitized_bundle)| {
                bundle_account_locker
                    .prepare_locked_bundle(sanitized_bundle, &bank_start.working_bank)
            })
            .collect::<Vec<_>>());
        bundle_stage_leader_metrics
            .bundle_stage_metrics_tracker()
            .increment_locked_bundle_elapsed_us(locked_bundles_elapsed_us);

        let (execution_results, execute_locked_bundles_elapsed_us) =
            measure_us!(locked_bundle_results
                .into_iter()
                .map(|r| match r {
                    Ok(locked_bundle) => {
                        let (r, measure) = measure_us!(Self::process_bundle(
                            bundle_account_locker,
                            tip_manager,
                            last_tip_updated_slot,
                            cluster_info,
                            block_builder_fee_info,
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
            .increment_execute_locked_bundles_elapsed_us(execute_locked_bundles_elapsed_us);
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
        tip_manager: &TipManager,
        last_tip_updated_slot: &mut Slot,
        cluster_info: &Arc<ClusterInfo>,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
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

        if bank_start.working_bank.slot() != *last_tip_updated_slot
            && Self::bundle_touches_tip_pdas(
                locked_bundle.sanitized_bundle(),
                &tip_manager.get_tip_accounts(),
            )
        {
            let start = Instant::now();
            let result = Self::handle_tip_programs(
                bundle_account_locker,
                tip_manager,
                cluster_info,
                block_builder_fee_info,
                committer,
                recorder,
                qos_service,
                log_messages_bytes_limit,
                max_bundle_retry_duration,
                bank_start,
                bundle_stage_leader_metrics,
            );

            bundle_stage_leader_metrics
                .bundle_stage_metrics_tracker()
                .increment_change_tip_receiver_elapsed_us(start.elapsed().as_micros() as u64);

            result?;

            *last_tip_updated_slot = bank_start.working_bank.slot();
        }

        Self::update_qos_and_execute_record_commit_bundle(
            committer,
            recorder,
            qos_service,
            log_messages_bytes_limit,
            max_bundle_retry_duration,
            locked_bundle.sanitized_bundle(),
            bank_start,
            bundle_stage_leader_metrics,
        )?;

        Ok(())
    }

    /// The validator needs to manage state on two programs related to tips
    #[allow(clippy::too_many_arguments)]
    fn handle_tip_programs(
        bundle_account_locker: &BundleAccountLocker,
        tip_manager: &TipManager,
        cluster_info: &Arc<ClusterInfo>,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        committer: &Committer,
        recorder: &TransactionRecorder,
        qos_service: &QosService,
        log_messages_bytes_limit: &Option<usize>,
        max_bundle_retry_duration: Duration,
        bank_start: &BankStart,
        bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
    ) -> Result<(), BundleExecutionError> {
        debug!("handle_tip_programs");

        // This will setup the tip payment and tip distribution program if they haven't been
        // initialized yet, which is typically helpful for local validators. On mainnet and testnet,
        // this code should never run.
        let keypair = cluster_info.keypair().clone();
        let initialize_tip_programs_bundle =
            tip_manager.get_initialize_tip_programs_bundle(&bank_start.working_bank, &keypair);
        if let Some(bundle) = initialize_tip_programs_bundle {
            debug!(
                "initializing tip programs with {} transactions, bundle id: {}",
                bundle.transactions.len(),
                bundle.bundle_id
            );

            let locked_init_tip_programs_bundle = bundle_account_locker
                .prepare_locked_bundle(&bundle, &bank_start.working_bank)
                .map_err(|_| BundleExecutionError::TipError(TipError::LockError))?;

            Self::update_qos_and_execute_record_commit_bundle(
                committer,
                recorder,
                qos_service,
                log_messages_bytes_limit,
                max_bundle_retry_duration,
                locked_init_tip_programs_bundle.sanitized_bundle(),
                bank_start,
                bundle_stage_leader_metrics,
            )
            .map_err(|e| {
                bundle_stage_leader_metrics
                    .bundle_stage_metrics_tracker()
                    .increment_num_init_tip_account_errors(1);
                error!(
                    "bundle: {} error initializing tip programs: {:?}",
                    locked_init_tip_programs_bundle.sanitized_bundle().bundle_id,
                    e
                );
                BundleExecutionError::TipError(TipError::InitializeProgramsError)
            })?;

            bundle_stage_leader_metrics
                .bundle_stage_metrics_tracker()
                .increment_num_init_tip_account_ok(1);
        }

        // There are two frequently run internal cranks inside the jito-solana validator that have to do with managing MEV tips.
        // One is initialize the TipDistributionAccount, which is a validator's "tip piggy bank" for an epoch
        // The other is ensuring the tip_receiver is configured correctly to ensure tips are routed to the correct
        // address. The validator must drain the tip accounts to the previous tip receiver before setting the tip receiver to
        // themselves.

        let kp = cluster_info.keypair().clone();
        let tip_crank_bundle = tip_manager.get_tip_programs_crank_bundle(
            &bank_start.working_bank,
            &kp,
            &block_builder_fee_info.lock().unwrap(),
        )?;
        debug!("tip_crank_bundle is_some: {}", tip_crank_bundle.is_some());

        if let Some(bundle) = tip_crank_bundle {
            info!(
                "bundle id: {} cranking tip programs with {} transactions",
                bundle.bundle_id,
                bundle.transactions.len()
            );

            let locked_tip_crank_bundle = bundle_account_locker
                .prepare_locked_bundle(&bundle, &bank_start.working_bank)
                .map_err(|_| BundleExecutionError::TipError(TipError::LockError))?;

            Self::update_qos_and_execute_record_commit_bundle(
                committer,
                recorder,
                qos_service,
                log_messages_bytes_limit,
                max_bundle_retry_duration,
                locked_tip_crank_bundle.sanitized_bundle(),
                bank_start,
                bundle_stage_leader_metrics,
            )
            .map_err(|e| {
                bundle_stage_leader_metrics
                    .bundle_stage_metrics_tracker()
                    .increment_num_change_tip_receiver_errors(1);
                error!(
                    "bundle: {} error cranking tip programs: {:?}",
                    locked_tip_crank_bundle.sanitized_bundle().bundle_id,
                    e
                );
                BundleExecutionError::TipError(TipError::CrankTipError)
            })?;

            bundle_stage_leader_metrics
                .bundle_stage_metrics_tracker()
                .increment_num_change_tip_receiver_ok(1);
        }

        Ok(())
    }

    /// Reserves space for the entire bundle up-front to ensure the entire bundle can execute.
    /// Rolls back the reserved space if there's not enough blockspace for all transactions in the bundle.
    fn reserve_bundle_blockspace<'a>(
        qos_service: &QosService,
        sanitized_bundle: &'a SanitizedBundle,
        bank: &Arc<Bank>,
    ) -> ReserveBundleBlockspaceResult<'a> {
        let (transaction_qos_cost_results, cost_model_throttled_transactions_count) = qos_service
            .select_and_accumulate_transaction_costs(
                bank,
                &sanitized_bundle.transactions,
                std::iter::repeat(Ok(())),
                // bundle stage does not respect the cost model reservation
                &|_| 0,
            );

        // rollback all transaction costs if it can't fit and
        if transaction_qos_cost_results.iter().any(|c| c.is_err()) {
            QosService::remove_or_update_costs(transaction_qos_cost_results.iter(), None, bank);
            return Err(BundleExecutionError::ExceedsCostModel);
        }

        Ok((
            transaction_qos_cost_results,
            cost_model_throttled_transactions_count,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn update_qos_and_execute_record_commit_bundle(
        committer: &Committer,
        recorder: &TransactionRecorder,
        qos_service: &QosService,
        log_messages_bytes_limit: &Option<usize>,
        max_bundle_retry_duration: Duration,
        sanitized_bundle: &SanitizedBundle,
        bank_start: &BankStart,
        bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
    ) -> BundleExecutionResult<()> {
        debug!(
            "bundle: {} reserving blockspace for {} transactions",
            sanitized_bundle.bundle_id,
            sanitized_bundle.transactions.len()
        );

        let (
            (transaction_qos_cost_results, _cost_model_throttled_transactions_count),
            cost_model_elapsed_us,
        ) = measure_us!(Self::reserve_bundle_blockspace(
            qos_service,
            sanitized_bundle,
            &bank_start.working_bank
        )?);

        debug!(
            "bundle: {} executing, recording, and committing",
            sanitized_bundle.bundle_id
        );

        let (result, process_transactions_us) = measure_us!(Self::execute_record_commit_bundle(
            committer,
            recorder,
            log_messages_bytes_limit,
            max_bundle_retry_duration,
            sanitized_bundle,
            bank_start,
        ));

        bundle_stage_leader_metrics
            .bundle_stage_metrics_tracker()
            .increment_num_execution_retries(result.execution_metrics.num_retries);
        bundle_stage_leader_metrics
            .leader_slot_metrics_tracker()
            .accumulate_transaction_errors(&result.transaction_error_counter);
        bundle_stage_leader_metrics
            .leader_slot_metrics_tracker()
            .increment_process_transactions_us(process_transactions_us);

        let (cu, us) = result
            .execute_and_commit_timings
            .execute_timings
            .accumulate_execute_units_and_time();
        qos_service.accumulate_actual_execute_cu(cu);
        qos_service.accumulate_actual_execute_time(us);

        let num_committed = result
            .commit_transaction_details
            .iter()
            .filter(|c| matches!(c, CommitTransactionDetails::Committed { .. }))
            .count();
        bundle_stage_leader_metrics
            .leader_slot_metrics_tracker()
            .accumulate_process_transactions_summary(&ProcessTransactionsSummary {
                reached_max_poh_height: matches!(
                    result.result,
                    Err(BundleExecutionError::BankProcessingTimeLimitReached)
                        | Err(BundleExecutionError::PohRecordError(_))
                ),
                transaction_counts: CommittedTransactionsCounts {
                    attempted_processing_count: Saturating(
                        sanitized_bundle.transactions.len() as u64
                    ),
                    committed_transactions_count: Saturating(num_committed as u64),
                    // NOTE: this assumes that bundles are committed all-or-nothing
                    committed_transactions_with_successful_result_count: Saturating(
                        num_committed as u64,
                    ),
                    processed_but_failed_commit: Saturating(0),
                },
                retryable_transaction_indexes: vec![],
                cost_model_throttled_transactions_count: 0,
                cost_model_us: cost_model_elapsed_us,
                execute_and_commit_timings: result.execute_and_commit_timings,
                error_counters: result.transaction_error_counter,
            });

        match result.result {
            Ok(_) => {
                QosService::remove_or_update_costs(
                    transaction_qos_cost_results.iter(),
                    Some(&result.commit_transaction_details),
                    &bank_start.working_bank,
                );

                qos_service.report_metrics(bank_start.working_bank.slot());
                Ok(())
            }
            Err(e) => {
                // on bundle failure, none of the transactions are committed, so need to revert
                // all compute reserved
                QosService::remove_or_update_costs(
                    transaction_qos_cost_results.iter(),
                    None,
                    &bank_start.working_bank,
                );
                qos_service.report_metrics(bank_start.working_bank.slot());

                Err(e)
            }
        }
    }

    fn execute_record_commit_bundle(
        committer: &Committer,
        recorder: &TransactionRecorder,
        log_messages_bytes_limit: &Option<usize>,
        max_bundle_retry_duration: Duration,
        sanitized_bundle: &SanitizedBundle,
        bank_start: &BankStart,
    ) -> ExecuteRecordCommitResult {
        let transaction_status_sender_enabled = committer.transaction_status_sender_enabled();

        let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();

        debug!("bundle: {} executing", sanitized_bundle.bundle_id);
        let default_accounts = vec![None; sanitized_bundle.transactions.len()];
        let bundle_execution_results = load_and_execute_bundle(
            &bank_start.working_bank,
            sanitized_bundle,
            MAX_PROCESSING_AGE,
            &max_bundle_retry_duration,
            transaction_status_sender_enabled,
            log_messages_bytes_limit,
            false,
            None,
            &default_accounts,
            &default_accounts,
        );

        let execution_metrics = bundle_execution_results.metrics.clone();

        execute_and_commit_timings.load_execute_us = execution_metrics.load_execute_us.0;
        execute_and_commit_timings
            .execute_timings
            .accumulate(&execution_metrics.execute_timings);
        let transaction_error_counter = execution_metrics.errors.clone();

        debug!(
            "bundle: {} executed, is_ok: {}",
            sanitized_bundle.bundle_id,
            bundle_execution_results.result.is_ok()
        );

        // don't commit bundle if failure executing any part of the bundle
        if let Err(e) = bundle_execution_results.result {
            return ExecuteRecordCommitResult {
                commit_transaction_details: vec![],
                result: Err(e.clone().into()),
                execution_metrics,
                execute_and_commit_timings,
                transaction_error_counter,
            };
        }

        // NB: Must run before we start committing the transactions.
        if super::front_run_identifier::is_bundle_front_run(&bundle_execution_results) {
            info!(
                "Dropping front run bundle; bundle_id={}; txs=[{}]",
                sanitized_bundle.bundle_id,
                sanitized_bundle
                    .transactions
                    .iter()
                    .map(|tx| tx.signature().to_string())
                    .join(", ")
            );

            return ExecuteRecordCommitResult {
                commit_transaction_details: vec![],
                result: Err(BundleExecutionError::FrontRun),
                execution_metrics,
                execute_and_commit_timings,
                transaction_error_counter,
            };
        }

        let (executed_batches, execution_results_to_transactions_us) =
            measure_us!(bundle_execution_results
                .bundle_transaction_results
                .iter()
                .map(|br| br.executed_versioned_transactions())
                .collect::<Vec<Vec<VersionedTransaction>>>());

        debug!(
            "bundle: {} recording {} batches of {:?} transactions",
            sanitized_bundle.bundle_id,
            executed_batches.len(),
            executed_batches
                .iter()
                .map(|b| b.len())
                .collect::<Vec<usize>>()
        );

        let (freeze_lock, freeze_lock_us) = measure_us!(bank_start.working_bank.freeze_lock());
        execute_and_commit_timings.freeze_lock_us = freeze_lock_us;

        let (hashes, hash_us) = measure_us!(executed_batches
            .iter()
            .map(|txs| hash_transactions(txs))
            .collect());
        let (starting_index, poh_record_us) =
            measure_us!(recorder.record(bank_start.working_bank.slot(), hashes, executed_batches));

        let RecordTransactionsSummary {
            record_transactions_timings,
            result: record_transactions_result,
            starting_transaction_index,
        } = match starting_index {
            Ok(starting_transaction_index) => RecordTransactionsSummary {
                record_transactions_timings: RecordTransactionsTimings {
                    processing_results_to_transactions_us: Saturating(0), // TODO (LB)
                    hash_us: Saturating(hash_us),
                    poh_record_us: Saturating(poh_record_us),
                },
                result: Ok(()),
                starting_transaction_index,
            },
            Err(e) => RecordTransactionsSummary {
                record_transactions_timings: RecordTransactionsTimings {
                    processing_results_to_transactions_us: Saturating(0), // TODO (LB)
                    hash_us: Saturating(hash_us),
                    poh_record_us: Saturating(poh_record_us),
                },
                result: Err(e),
                starting_transaction_index: None,
            },
        };

        execute_and_commit_timings.record_us = record_transactions_timings.poh_record_us.0;
        execute_and_commit_timings.record_transactions_timings = record_transactions_timings;
        execute_and_commit_timings
            .record_transactions_timings
            .processing_results_to_transactions_us =
            Saturating(execution_results_to_transactions_us);

        debug!(
            "bundle: {} record result: {}",
            sanitized_bundle.bundle_id,
            record_transactions_result.is_ok()
        );

        // don't commit bundle if failed to record
        if let Err(e) = record_transactions_result {
            return ExecuteRecordCommitResult {
                commit_transaction_details: vec![],
                result: Err(e.into()),
                execution_metrics,
                execute_and_commit_timings,
                transaction_error_counter,
            };
        }

        // note: execute_and_commit_timings.commit_us handled inside this function
        let (commit_us, commit_bundle_details) = committer.commit_bundle(
            bundle_execution_results,
            starting_transaction_index,
            &bank_start.working_bank,
            &mut execute_and_commit_timings,
        );
        execute_and_commit_timings.commit_us = commit_us;

        drop(freeze_lock);

        // commit_bundle_details contains transactions that were and were not committed
        // given the current implementation only executes, records, and commits bundles
        // where all transactions executed, we can filter out the non-committed
        // TODO (LB): does this make more sense in commit_bundle for future when failing bundles are accepted?
        let commit_transaction_details = commit_bundle_details
            .commit_transaction_details
            .into_iter()
            .flat_map(|commit_details| {
                commit_details
                    .into_iter()
                    .filter(|d| matches!(d, CommitTransactionDetails::Committed { .. }))
            })
            .collect();
        debug!(
            "bundle: {} commit details: {:?}",
            sanitized_bundle.bundle_id, commit_transaction_details
        );

        ExecuteRecordCommitResult {
            commit_transaction_details,
            result: Ok(()),
            execution_metrics,
            execute_and_commit_timings,
            transaction_error_counter,
        }
    }

    /// Returns true if any of the transactions in a bundle mention one of the tip PDAs
    fn bundle_touches_tip_pdas(bundle: &SanitizedBundle, tip_pdas: &HashSet<Pubkey>) -> bool {
        bundle.transactions.iter().any(|tx| {
            tx.message()
                .account_keys()
                .iter()
                .any(|a| tip_pdas.contains(a))
        })
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            bundle_stage::{
                bundle_account_locker::BundleAccountLocker, bundle_consumer::BundleConsumer,
                bundle_packet_deserializer::BundlePacketDeserializer,
                bundle_stage_leader_metrics::BundleStageLeaderMetrics, committer::Committer,
                BundleStorage, QosService,
            },
            packet_bundle::PacketBundle,
            proxy::block_engine_stage::BlockBuilderFeeInfo,
            tip_manager::{
                tests::MockBlockstore, TipDistributionAccountConfig, TipManager, TipManagerConfig,
            },
        },
        crossbeam_channel::{unbounded, Receiver},
        rand::{thread_rng, RngCore},
        solana_bundle::SanitizedBundle,
        solana_bundle_sdk::derive_bundle_id,
        solana_clock::MAX_PROCESSING_AGE,
        solana_cost_model::cost_model::CostModel,
        solana_fee_calculator::{FeeRateGovernor, DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE},
        solana_genesis_config::ClusterType,
        solana_gossip::{cluster_info::ClusterInfo, contact_info::ContactInfo},
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_ledger::{
            blockstore::Blockstore, genesis_utils::create_genesis_config,
            get_tmp_ledger_path_auto_delete, leader_schedule_cache::LeaderScheduleCache,
        },
        solana_native_token::sol_to_lamports,
        solana_perf::packet::{BytesPacket, PacketBatch},
        solana_poh::{
            poh_recorder::{PohRecorder, Record, WorkingBankEntry},
            poh_service::PohService,
            transaction_recorder::TransactionRecorder,
        },
        solana_poh_config::PohConfig,
        solana_program_test::programs::spl_programs,
        solana_pubkey::{pubkey, Pubkey},
        solana_rent::Rent,
        solana_runtime::{
            bank::Bank,
            bank_forks::BankForks,
            genesis_utils::{create_genesis_config_with_leader_ex, GenesisConfigInfo},
            installed_scheduler_pool::BankWithScheduler,
            prioritization_fee_cache::PrioritizationFeeCache,
        },
        solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
        solana_signer::Signer,
        solana_streamer::socket::SocketAddrSpace,
        solana_svm::{
            account_loader::TransactionCheckResult,
            transaction_error_metrics::TransactionErrorMetrics,
        },
        solana_system_transaction::transfer,
        solana_transaction::versioned::VersionedTransaction,
        solana_transaction_error::TransactionError,
        solana_vote_program::vote_state::VoteState,
        std::{
            collections::HashSet,
            sync::{
                atomic::{AtomicBool, Ordering},
                Arc, Mutex, RwLock,
            },
            thread::{Builder, JoinHandle},
            time::Duration,
        },
    };

    struct TestFixture {
        genesis_config_info: GenesisConfigInfo,
        leader_keypair: Keypair,
        bank: Arc<Bank>,
        exit: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        transaction_recorder: TransactionRecorder,
        poh_simulator: JoinHandle<()>,
        entry_receiver: Receiver<WorkingBankEntry>,
        bank_forks: Arc<RwLock<BankForks>>,
    }

    pub fn simulate_poh(
        record_receiver: Receiver<Record>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
    ) -> JoinHandle<()> {
        let poh_recorder = poh_recorder.clone();
        let is_exited = poh_recorder.read().unwrap().is_exited.clone();
        let tick_producer = Builder::new()
            .name("solana-simulate_poh".to_string())
            .spawn(move || loop {
                PohService::read_record_receiver_and_process(
                    &poh_recorder,
                    &record_receiver,
                    Duration::from_millis(10),
                );
                if is_exited.load(Ordering::Relaxed) {
                    break;
                }
            });
        tick_producer.unwrap()
    }

    struct TestRecorder {
        exit: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        transaction_recorder: TransactionRecorder,
        poh_simulator: JoinHandle<()>,
        entry_receiver: Receiver<WorkingBankEntry>,
    }

    fn create_test_recorder(
        bank: &Arc<Bank>,
        blockstore: Arc<Blockstore>,
        poh_config: Option<PohConfig>,
        leader_schedule_cache: Option<Arc<LeaderScheduleCache>>,
    ) -> TestRecorder {
        let leader_schedule_cache = match leader_schedule_cache {
            Some(provided_cache) => provided_cache,
            None => Arc::new(LeaderScheduleCache::new_from_bank(bank)),
        };
        let exit = Arc::new(AtomicBool::new(false));
        let poh_config = poh_config.unwrap_or_default();

        let (mut poh_recorder, entry_receiver) = PohRecorder::new(
            bank.tick_height(),
            bank.last_blockhash(),
            bank.clone(),
            Some((4, 4)),
            bank.ticks_per_slot(),
            blockstore,
            &leader_schedule_cache,
            &poh_config,
            exit.clone(),
        );
        poh_recorder.set_bank(
            BankWithScheduler::new_without_scheduler(bank.clone()),
            false,
        );

        let (record_sender, record_receiver) = unbounded();
        let transaction_recorder = TransactionRecorder::new(record_sender, exit.clone());

        let poh_recorder = Arc::new(RwLock::new(poh_recorder));
        let poh_simulator = simulate_poh(record_receiver, &poh_recorder);

        TestRecorder {
            exit,
            poh_recorder,
            transaction_recorder,
            poh_simulator,
            entry_receiver,
        }
    }

    fn create_test_fixture(mint_sol: u64) -> TestFixture {
        let mint_keypair = Keypair::new();
        let leader_keypair = Keypair::new();
        let voting_keypair = Keypair::new();

        let rent = Rent::default();

        let mut genesis_config = create_genesis_config_with_leader_ex(
            sol_to_lamports(mint_sol as f64),
            &mint_keypair.pubkey(),
            &leader_keypair.pubkey(),
            &voting_keypair.pubkey(),
            &solana_pubkey::new_rand(),
            rent.minimum_balance(VoteState::size_of()) + sol_to_lamports(1_000_000.0),
            sol_to_lamports(1_000_000.0),
            FeeRateGovernor {
                // Initialize with a non-zero fee
                lamports_per_signature: DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE / 2,
                ..FeeRateGovernor::default()
            },
            rent.clone(), // most tests don't expect rent
            ClusterType::Development,
            spl_programs(&rent),
        );
        genesis_config.ticks_per_slot *= 8;

        // workaround for https://github.com/solana-labs/solana/issues/30085
        // the test can deploy and use spl_programs in the genensis slot without waiting for the next one
        let (bank, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);

        let bank = Arc::new(Bank::new_from_parent(bank, &Pubkey::default(), 1));

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(
            Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger"),
        );

        let TestRecorder {
            exit,
            poh_recorder,
            transaction_recorder,
            poh_simulator,
            entry_receiver,
        } = create_test_recorder(&bank, blockstore, Some(PohConfig::default()), None);

        let validator_pubkey = voting_keypair.pubkey();
        TestFixture {
            genesis_config_info: GenesisConfigInfo {
                genesis_config,
                mint_keypair,
                voting_keypair,
                validator_pubkey,
            },
            leader_keypair,
            bank,
            bank_forks,
            exit,
            poh_recorder,
            transaction_recorder,
            poh_simulator,
            entry_receiver,
        }
    }

    fn make_random_overlapping_bundles(
        mint_keypair: &Keypair,
        num_bundles: usize,
        num_packets_per_bundle: usize,
        hash: Hash,
        max_transfer_amount: u64,
    ) -> Vec<PacketBundle> {
        let mut rng = thread_rng();

        (0..num_bundles)
            .map(|_| {
                let transfers: Vec<_> = (0..num_packets_per_bundle)
                    .map(|_| {
                        VersionedTransaction::from(transfer(
                            mint_keypair,
                            &mint_keypair.pubkey(),
                            rng.next_u64() % max_transfer_amount,
                            hash,
                        ))
                    })
                    .collect();
                let bundle_id = derive_bundle_id(&transfers).unwrap();

                PacketBundle {
                    batch: PacketBatch::from(
                        transfers
                            .iter()
                            .map(|tx| BytesPacket::from_data(None, tx).unwrap())
                            .collect::<Vec<_>>(),
                    ),
                    bundle_id,
                }
            })
            .collect::<Vec<_>>()
    }

    fn get_tip_manager(
        leader_schedule_cache: Arc<LeaderScheduleCache>,
        vote_account: &Pubkey,
        funnel: Option<Pubkey>,
    ) -> TipManager {
        TipManager::new(
            Arc::new(RwLock::new(MockBlockstore(vec![]))),
            leader_schedule_cache,
            TipManagerConfig {
                funnel,
                rewards_split: None,
                tip_payment_program_id: pubkey!("T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt"),
                tip_distribution_program_id: pubkey!(
                    "4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7"
                ),
                tip_distribution_account_config: TipDistributionAccountConfig {
                    merkle_root_upload_authority: Pubkey::new_unique(),
                    vote_account: *vote_account,
                    commission_bps: 10,
                },
            },
        )
    }

    /// Happy-path bundle execution w/ no tip management
    #[test]
    fn test_bundle_no_tip_success() {
        solana_logger::setup();
        let TestFixture {
            genesis_config_info,
            leader_keypair,
            bank,
            exit,
            poh_recorder,
            transaction_recorder,
            poh_simulator,
            entry_receiver,
            bank_forks: _bank_forks,
        } = create_test_fixture(1_000_000);

        let status = poh_recorder
            .read()
            .unwrap()
            .reached_leader_slot(&leader_keypair.pubkey());
        info!("status: {:?}", status);

        let (replay_vote_sender, _replay_vote_receiver) = unbounded();
        let committer = Committer::new(
            None,
            replay_vote_sender,
            Arc::new(PrioritizationFeeCache::new(0u64)),
        );

        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::new(leader_keypair.pubkey(), 0, 0),
            Arc::new(leader_keypair),
            SocketAddrSpace::new(true),
        ));
        let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_bank(&bank));

        let tip_manager = get_tip_manager(
            leader_schedule_cache,
            &genesis_config_info.voting_keypair.pubkey(),
            None,
        );
        let block_builder_pubkey = Pubkey::new_unique();
        let block_builder_info = Arc::new(Mutex::new(BlockBuilderFeeInfo {
            block_builder: block_builder_pubkey,
            block_builder_commission: 10,
        }));

        let mut consumer = BundleConsumer::new(
            committer,
            transaction_recorder,
            QosService::new(1),
            None,
            tip_manager,
            BundleAccountLocker::default(),
            block_builder_info,
            Duration::from_secs(10),
            cluster_info,
        );

        let bank_start = poh_recorder.read().unwrap().bank_start().unwrap();

        let mut bundle_storage = BundleStorage::default();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(1);

        let mut packet_bundles = make_random_overlapping_bundles(
            &genesis_config_info.mint_keypair,
            1,
            3,
            genesis_config_info.genesis_config.hash(),
            10_000,
        );
        let deserialized_bundle = BundlePacketDeserializer::deserialize_bundle(
            packet_bundles.get_mut(0).unwrap(),
            None,
            &Ok,
        )
        .unwrap();
        let mut error_metrics = TransactionErrorMetrics::default();
        let sanitized_bundle = deserialized_bundle
            .build_sanitized_bundle(
                &bank_start.working_bank,
                &HashSet::default(),
                &mut error_metrics,
            )
            .unwrap();

        let summary = bundle_storage.insert_unprocessed_bundles(vec![deserialized_bundle]);
        assert_eq!(
            summary.num_packets_inserted,
            sanitized_bundle.transactions.len()
        );
        assert_eq!(summary.num_bundles_dropped, 0);
        assert_eq!(summary.num_bundles_inserted, 1);

        consumer.consume_buffered_bundles(
            &bank_start,
            &mut bundle_storage,
            &mut bundle_stage_leader_metrics,
        );

        let mut transactions = Vec::new();
        while let Ok((wbe_bank, (entry, _ticks))) = entry_receiver.recv() {
            assert_eq!(bank.slot(), wbe_bank.slot());
            transactions.extend(entry.transactions);
            if transactions.len() == sanitized_bundle.transactions.len() {
                break;
            }
        }

        let bundle_versioned_transactions: Vec<_> = sanitized_bundle
            .transactions
            .iter()
            .map(|tx| tx.to_versioned_transaction())
            .collect();
        assert_eq!(transactions, bundle_versioned_transactions);

        let check_results = bank.check_transactions(
            &sanitized_bundle.transactions,
            &vec![Ok(()); sanitized_bundle.transactions.len()],
            MAX_PROCESSING_AGE,
            &mut error_metrics,
        );

        let expected_result: Vec<TransactionCheckResult> =
            vec![Err(TransactionError::AlreadyProcessed); sanitized_bundle.transactions.len()];

        assert_eq!(check_results, expected_result);

        poh_recorder
            .write()
            .unwrap()
            .is_exited
            .store(true, Ordering::Relaxed);
        exit.store(true, Ordering::Relaxed);
        poh_simulator.join().unwrap();
    }

    /// Happy-path bundle execution to ensure tip management works.
    /// Tip management involves cranking setup bundles before executing the test bundle
    #[test]
    fn test_bundle_tip_program_setup_success() {
        solana_logger::setup();
        let TestFixture {
            genesis_config_info,
            leader_keypair,
            bank,
            exit,
            poh_recorder,
            transaction_recorder,
            poh_simulator,
            entry_receiver,
            bank_forks: _bank_forks,
        } = create_test_fixture(1_000_000);

        let (replay_vote_sender, _replay_vote_receiver) = unbounded();
        let committer = Committer::new(
            None,
            replay_vote_sender,
            Arc::new(PrioritizationFeeCache::new(0u64)),
        );

        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::new(leader_keypair.pubkey(), 0, 0),
            Arc::new(leader_keypair),
            SocketAddrSpace::new(true),
        ));
        let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_bank(&bank));

        let block_builder_pubkey = Pubkey::new_unique();
        let tip_manager = get_tip_manager(
            leader_schedule_cache,
            &genesis_config_info.voting_keypair.pubkey(),
            None,
        );
        let block_builder_info = Arc::new(Mutex::new(BlockBuilderFeeInfo {
            block_builder: block_builder_pubkey,
            block_builder_commission: 10,
        }));

        let mut consumer = BundleConsumer::new(
            committer,
            transaction_recorder,
            QosService::new(1),
            None,
            tip_manager.clone(),
            BundleAccountLocker::default(),
            block_builder_info,
            Duration::from_secs(10),
            cluster_info.clone(),
        );

        let bank_start = poh_recorder.read().unwrap().bank_start().unwrap();

        let mut bundle_storage = BundleStorage::default();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(1);
        // MAIN LOGIC

        // a bundle that tips the tip program
        let tip_accounts = tip_manager.get_tip_accounts();
        let tip_account = tip_accounts.iter().collect::<Vec<_>>()[0];
        let mut packet_bundle = PacketBundle {
            batch: PacketBatch::from(vec![BytesPacket::from_data(
                None,
                transfer(
                    &genesis_config_info.mint_keypair,
                    tip_account,
                    1,
                    genesis_config_info.genesis_config.hash(),
                ),
            )
            .unwrap()]),
            bundle_id: "test_transfer".to_string(),
        };

        let deserialized_bundle =
            BundlePacketDeserializer::deserialize_bundle(&mut packet_bundle, None, &Ok).unwrap();
        let mut error_metrics = TransactionErrorMetrics::default();
        let sanitized_bundle = deserialized_bundle
            .build_sanitized_bundle(
                &bank_start.working_bank,
                &HashSet::default(),
                &mut error_metrics,
            )
            .unwrap();

        let summary = bundle_storage.insert_unprocessed_bundles(vec![deserialized_bundle]);
        assert_eq!(summary.num_bundles_inserted, 1);
        assert_eq!(summary.num_packets_inserted, 1);
        assert_eq!(summary.num_bundles_dropped, 0);

        consumer.consume_buffered_bundles(
            &bank_start,
            &mut bundle_storage,
            &mut bundle_stage_leader_metrics,
        );

        // its expected there are 3 transactions. One to initialize the tip program configuration, one to change the tip receiver,
        // and another with the tip

        let mut transactions = Vec::new();
        while let Ok((wbe_bank, (entry, _ticks))) = entry_receiver.recv() {
            assert_eq!(bank.slot(), wbe_bank.slot());
            transactions.extend(entry.transactions);
            if transactions.len() == 5 {
                break;
            }
        }

        // tip management on the first bundle involves:
        // calling initialize on the tip payment and tip distribution programs
        // creating the tip distribution account for this validator's epoch (the MEV piggy bank)
        // changing the tip receiver and block builder tx
        // the original transfer that was sent
        let keypair = cluster_info.keypair().clone();

        assert_eq!(
            transactions[0],
            tip_manager
                .initialize_tip_payment_program_tx(&bank, &keypair)
                .to_versioned_transaction()
        );
        assert_eq!(
            transactions[1],
            tip_manager
                .initialize_tip_distribution_config_tx(&bank, &keypair)
                .to_versioned_transaction()
        );
        assert_eq!(
            transactions[2],
            tip_manager
                .initialize_tip_distribution_account_tx(&bank, &keypair)
                .to_versioned_transaction()
        );
        // the first tip receiver + block builder are the initializer (keypair.pubkey()) as set by the
        // TipPayment program during initialization
        assert_eq!(
            transactions[4],
            sanitized_bundle.transactions[0].to_versioned_transaction()
        );

        poh_recorder
            .write()
            .unwrap()
            .is_exited
            .store(true, Ordering::Relaxed);
        exit.store(true, Ordering::Relaxed);
        poh_simulator.join().unwrap();
    }

    #[test]
    fn test_handle_tip_programs() {
        solana_logger::setup();
        let TestFixture {
            genesis_config_info,
            leader_keypair,
            bank,
            exit,
            poh_recorder,
            transaction_recorder,
            poh_simulator,
            entry_receiver,
            bank_forks: _bank_forks,
        } = create_test_fixture(1_000_000);

        let (replay_vote_sender, _replay_vote_receiver) = unbounded();
        let committer = Committer::new(
            None,
            replay_vote_sender,
            Arc::new(PrioritizationFeeCache::new(0u64)),
        );

        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::new(leader_keypair.pubkey(), 0, 0),
            Arc::new(leader_keypair),
            SocketAddrSpace::new(true),
        ));
        let leader_schedule_cache = Arc::new(LeaderScheduleCache::new_from_bank(&bank));

        let block_builder_pubkey = Pubkey::new_unique();
        let tip_manager = get_tip_manager(
            leader_schedule_cache,
            &genesis_config_info.voting_keypair.pubkey(),
            None,
        );
        let block_builder_info = Arc::new(Mutex::new(BlockBuilderFeeInfo {
            block_builder: block_builder_pubkey,
            block_builder_commission: 10,
        }));

        let bank_start = poh_recorder.read().unwrap().bank_start().unwrap();

        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(1);
        assert_matches!(
            BundleConsumer::handle_tip_programs(
                &BundleAccountLocker::default(),
                &tip_manager,
                &cluster_info,
                &block_builder_info,
                &committer,
                &transaction_recorder,
                &QosService::new(1),
                &None,
                Duration::from_secs(10),
                &bank_start,
                &mut bundle_stage_leader_metrics
            ),
            Ok(())
        );

        let mut transactions = Vec::new();
        while let Ok((wbe_bank, (entry, _ticks))) = entry_receiver.recv() {
            assert_eq!(bank.slot(), wbe_bank.slot());
            transactions.extend(entry.transactions);
            if transactions.len() == 4 {
                break;
            }
        }

        let keypair = cluster_info.keypair().clone();
        // expect to see initialize tip payment program, tip distribution program, initialize tip distribution account, change tip receiver + change block builder
        assert_eq!(
            transactions[0],
            tip_manager
                .initialize_tip_payment_program_tx(&bank, &keypair)
                .to_versioned_transaction()
        );
        assert_eq!(
            transactions[1],
            tip_manager
                .initialize_tip_distribution_config_tx(&bank, &keypair)
                .to_versioned_transaction()
        );
        assert_eq!(
            transactions[2],
            tip_manager
                .initialize_tip_distribution_account_tx(&bank, &keypair)
                .to_versioned_transaction()
        );
        // the first tip receiver + block builder are the initializer (keypair.pubkey()) as set by the
        // TipPayment program during initialization

        poh_recorder
            .write()
            .unwrap()
            .is_exited
            .store(true, Ordering::Relaxed);
        exit.store(true, Ordering::Relaxed);
        poh_simulator.join().unwrap();
    }

    #[test]
    fn test_reserve_bundle_blockspace_success() {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));

        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();
        let transfer_tx = RuntimeTransaction::from_transaction_for_tests(transfer(
            &keypair1,
            &keypair2.pubkey(),
            1,
            bank.parent_hash(),
        ));
        let sanitized_bundle = SanitizedBundle {
            transactions: vec![transfer_tx],
            bundle_id: String::default(),
        };

        let transfer_cost =
            CostModel::calculate_cost(&sanitized_bundle.transactions[0], &bank.feature_set);

        let qos_service = QosService::new(1);
        assert!(
            BundleConsumer::reserve_bundle_blockspace(&qos_service, &sanitized_bundle, &bank)
                .is_ok()
        );
        assert_eq!(
            bank.read_cost_tracker().unwrap().block_cost(),
            transfer_cost.sum()
        );
    }

    #[test]
    fn test_reserve_bundle_blockspace_failure() {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));

        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();
        let transfer_tx1 = RuntimeTransaction::from_transaction_for_tests(transfer(
            &keypair1,
            &keypair2.pubkey(),
            1,
            bank.parent_hash(),
        ));
        let transfer_tx2 = RuntimeTransaction::from_transaction_for_tests(transfer(
            &keypair1,
            &keypair2.pubkey(),
            2,
            bank.parent_hash(),
        ));
        let sanitized_bundle = SanitizedBundle {
            transactions: vec![transfer_tx1, transfer_tx2],
            bundle_id: String::default(),
        };

        // set block cost limit to 1 transfer transaction, try to process 2, should return an error
        // and rollback block cost added
        let transfer_cost =
            CostModel::calculate_cost(&sanitized_bundle.transactions[0], &bank.feature_set);
        bank.write_cost_tracker()
            .unwrap()
            .set_limits(u64::MAX, transfer_cost.sum(), u64::MAX);

        let qos_service = QosService::new(1);

        assert!(
            BundleConsumer::reserve_bundle_blockspace(&qos_service, &sanitized_bundle, &bank)
                .is_err()
        );
        // the block cost shall not be modified
        assert_eq!(bank.read_cost_tracker().unwrap().block_cost(), 0);
    }
}
