use {
    crate::SanitizedBundle,
    itertools::izip,
    log::*,
    solana_account::{AccountSharedData, ReadableAccount},
    solana_measure::{measure::Measure, measure_us},
    solana_pubkey::Pubkey,
    solana_runtime::{
        account_saver::collect_accounts_to_store,
        bank::{Bank, LoadAndExecuteTransactionsOutput},
        transaction_batch::TransactionBatch,
    },
    solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
    solana_signature::Signature,
    solana_svm::{
        account_overrides::AccountOverrides,
        transaction_error_metrics::TransactionErrorMetrics,
        transaction_processing_callback::TransactionProcessingCallback,
        transaction_processing_result::{ProcessedTransaction, TransactionProcessingResult},
        transaction_processor::{ExecutionRecordingConfig, TransactionProcessingConfig},
    },
    solana_timings::ExecuteTimings,
    solana_transaction::{sanitized::SanitizedTransaction, versioned::VersionedTransaction},
    solana_transaction_error::{TransactionError, TransactionResult as Result},
    std::{
        cmp::{max, min},
        num::Saturating,
        ops::AddAssign,
        result,
        time::{Duration, Instant},
    },
    thiserror::Error,
};

#[derive(Clone, Default)]
pub struct BundleExecutionMetrics {
    pub num_retries: Saturating<u64>,
    pub load_execute_us: Saturating<u64>,
    pub collect_pre_post_accounts_us: Saturating<u64>,
    pub cache_accounts_us: Saturating<u64>,
    pub execute_timings: ExecuteTimings,
    pub errors: TransactionErrorMetrics,
}

/// Contains the results from executing each TransactionBatch with a final result associated with it
/// Note that if !result.is_ok(), bundle_transaction_results will not contain the output for every transaction.
pub struct LoadAndExecuteBundleOutput<'a> {
    pub bundle_transaction_results: Vec<BundleTransactionsOutput<'a>>,
    pub result: LoadAndExecuteBundleResult<()>,
    pub metrics: BundleExecutionMetrics,
}

#[derive(Clone, Debug, Error)]
pub enum LoadAndExecuteBundleError {
    #[error("Bundle execution timed out")]
    ProcessingTimeExceeded(Duration),

    #[error(
        "A transaction in the bundle encountered a lock error: [signature={:?}, transaction_error={:?}]",
        signature,
        transaction_error
    )]
    LockError {
        signature: Signature,
        transaction_error: TransactionError,
    },

    #[error(
        "A transaction in the bundle failed to execute: [signature={:?}, execution_result={:?}",
        signature,
        execution_result
    )]
    TransactionError {
        signature: Signature,
        // Box reduces the size between variants in the Error
        execution_result: Box<Result<ProcessedTransaction>>,
    },

    #[error("Invalid pre or post accounts")]
    InvalidPreOrPostAccounts,
}

pub struct BundleTransactionsOutput<'a> {
    pub transactions: &'a [RuntimeTransaction<SanitizedTransaction>],
    pub load_and_execute_transactions_output: LoadAndExecuteTransactionsOutput,
    // the length of the outer vector should be the same as transactions.len()
    // for indices that didn't get executed, expect a None.
    pre_tx_execution_accounts: Vec<Option<Vec<(Pubkey, AccountSharedData)>>>,
    post_tx_execution_accounts: Vec<Option<Vec<(Pubkey, AccountSharedData)>>>,
}

impl<'a> BundleTransactionsOutput<'a> {
    pub fn executed_versioned_transactions(&self) -> Vec<VersionedTransaction> {
        self.transactions
            .iter()
            .zip(
                self.load_and_execute_transactions_output
                    .processing_results
                    .iter(),
            )
            .filter_map(|(tx, exec_result)| {
                matches!(exec_result, Ok(ProcessedTransaction::Executed(_)))
                    .then(|| tx.to_versioned_transaction())
            })
            .collect()
    }

    pub fn executed_transactions(&self) -> Vec<&'a RuntimeTransaction<SanitizedTransaction>> {
        self.transactions
            .iter()
            .zip(
                self.load_and_execute_transactions_output
                    .processing_results
                    .iter(),
            )
            .filter_map(|(tx, exec_result)| exec_result.is_ok().then_some(tx))
            .collect()
    }

    pub fn load_and_execute_transactions_output(&self) -> &LoadAndExecuteTransactionsOutput {
        &self.load_and_execute_transactions_output
    }

    pub fn transactions(&self) -> &[RuntimeTransaction<SanitizedTransaction>] {
        self.transactions
    }

    pub fn execution_results(&self) -> &[TransactionProcessingResult] {
        &self.load_and_execute_transactions_output.processing_results
    }
    pub fn pre_tx_execution_accounts(&self) -> &Vec<Option<Vec<(Pubkey, AccountSharedData)>>> {
        &self.pre_tx_execution_accounts
    }

    pub fn post_tx_execution_accounts(&self) -> &Vec<Option<Vec<(Pubkey, AccountSharedData)>>> {
        &self.post_tx_execution_accounts
    }
}

pub type LoadAndExecuteBundleResult<T> = result::Result<T, LoadAndExecuteBundleError>;

/// Return an Error if a transaction was executed and reverted
/// NOTE: `execution_results` are zipped with `sanitized_txs` so it's expected a sanitized tx at
/// position i has a corresponding execution result at position i within the `execution_results`
/// slice
pub fn check_bundle_execution_results<'a>(
    execution_results: &'a [TransactionProcessingResult],
    sanitized_txs: &'a [RuntimeTransaction<SanitizedTransaction>],
) -> result::Result<
    (),
    (
        &'a RuntimeTransaction<SanitizedTransaction>,
        &'a TransactionProcessingResult,
    ),
> {
    for (exec_results, sanitized_tx) in execution_results.iter().zip(sanitized_txs) {
        match exec_results {
            Err(TransactionError::AccountInUse) => {
                // AccountInUse is expected and should be retried
                continue;
            }
            Err(_) => {
                return Err((sanitized_tx, exec_results));
            }
            Ok(ProcessedTransaction::FeesOnly(_)) => {
                return Err((sanitized_tx, exec_results));
            }
            Ok(ProcessedTransaction::Executed(executed_transaction)) => {
                if !executed_transaction.execution_details.was_successful() {
                    return Err((sanitized_tx, exec_results));
                }
            }
        }
    }
    Ok(())
}

/// Executing a bundle is somewhat complicated compared to executing single transactions. In order to
/// avoid duplicate logic for execution and simulation, this function can be leveraged.
///
/// Assumptions for the caller:
/// - all transactions were signed properly
/// - user has deduplicated transactions inside the bundle
///
/// TODO (LB):
/// - given a bundle with 3 transactions that write lock the following accounts: [A, B, C], on failure of B
///   we should add in the BundleTransactionsOutput of A and C and return the error for B.
#[allow(clippy::too_many_arguments)]
pub fn load_and_execute_bundle<'a>(
    bank: &Bank,
    bundle: &'a SanitizedBundle,
    // Max blockhash age
    max_age: usize,
    // Upper bound on execution time for a bundle
    max_processing_time: &Duration,
    transaction_status_sender_enabled: bool,
    log_messages_bytes_limit: &Option<usize>,
    // simulation will not use the Bank's account locks when building the TransactionBatch
    // if simulating on an unfrozen bank, this is helpful to avoid stalling replay and use whatever
    // state the accounts are in at the current time
    is_simulation: bool,
    account_overrides: Option<&mut AccountOverrides>,
    // these must be the same length as the bundle's transactions
    // allows one to read account state before and after execution of each transaction in the bundle
    // will use AccountsOverride + Bank
    pre_execution_accounts: &[Option<Vec<Pubkey>>],
    post_execution_accounts: &[Option<Vec<Pubkey>>],
) -> LoadAndExecuteBundleOutput<'a> {
    if pre_execution_accounts.len() != post_execution_accounts.len()
        || post_execution_accounts.len() != bundle.transactions.len()
    {
        return LoadAndExecuteBundleOutput {
            bundle_transaction_results: vec![],
            result: Err(LoadAndExecuteBundleError::InvalidPreOrPostAccounts),
            metrics: BundleExecutionMetrics::default(),
        };
    }

    let mut binding = AccountOverrides::default();
    let account_overrides = account_overrides.unwrap_or(&mut binding);
    if is_simulation {
        bundle
            .transactions
            .iter()
            .map(|tx| tx.message().account_keys())
            .for_each(|account_keys| {
                account_overrides.upsert_account_overrides(
                    bank.get_account_overrides_for_simulation(&account_keys),
                );

                // An unfrozen bank's state is always changing.
                // By taking a snapshot of the accounts we're mocking out grabbing their locks.
                // **Note** this does not prevent race conditions, just mocks preventing them.
                if !bank.is_frozen() {
                    for pk in account_keys.iter() {
                        // Save on a disk read.
                        if account_overrides.get(pk).is_none() {
                            account_overrides.set_account(pk, bank.get_account_shared_data(pk));
                        }
                    }
                }
            });
    }

    let mut chunk_start = 0;
    let start_time = Instant::now();

    let mut bundle_transaction_results = vec![];
    let mut metrics = BundleExecutionMetrics::default();

    while chunk_start != bundle.transactions.len() {
        if start_time.elapsed() > *max_processing_time {
            trace!("bundle: {} took too long to execute", bundle.bundle_id);
            return LoadAndExecuteBundleOutput {
                bundle_transaction_results,
                metrics,
                result: Err(LoadAndExecuteBundleError::ProcessingTimeExceeded(
                    start_time.elapsed(),
                )),
            };
        }

        let chunk_end = min(bundle.transactions.len(), chunk_start.saturating_add(128));
        let chunk = &bundle.transactions[chunk_start..chunk_end];

        // Note: these batches are dropped after execution and before record/commit, which is atypical
        // compared to BankingStage which holds account locks until record + commit to avoid race conditions with
        // other BankingStage threads. However, the caller of this method, BundleConsumer, will use BundleAccountLocks
        // to hold RW locks across all transactions in a bundle until its processed.
        let batch = if is_simulation {
            bank.prepare_sequential_sanitized_batch_with_results_for_simulation(chunk)
        } else {
            bank.prepare_sequential_sanitized_batch_with_results(chunk)
        };

        debug!(
            "bundle: {} batch num locks ok: {}",
            bundle.bundle_id,
            batch.lock_results().iter().filter(|lr| lr.is_ok()).count()
        );

        // Bundle locking failed if lock result returns something other than ok or AccountInUse
        for (sanitied_tx, lock_result) in batch
            .sanitized_transactions()
            .iter()
            .zip(batch.lock_results())
        {
            if !matches!(lock_result, Ok(()) | Err(TransactionError::AccountInUse)) {
                return LoadAndExecuteBundleOutput {
                    bundle_transaction_results,
                    metrics,
                    result: Err(LoadAndExecuteBundleError::LockError {
                        signature: *sanitied_tx.signature(),
                        transaction_error: lock_result.as_ref().unwrap_err().clone(),
                    }),
                };
            }
        }

        let end = min(
            chunk_start.saturating_add(batch.sanitized_transactions().len()),
            pre_execution_accounts.len(),
        );

        let m = Measure::start("accounts");
        let accounts_requested = &pre_execution_accounts[chunk_start..end];
        let pre_tx_execution_accounts =
            get_account_transactions(bank, account_overrides, accounts_requested, &batch);
        metrics
            .collect_pre_post_accounts_us
            .add_assign(Saturating(m.end_as_us()));

        let (load_and_execute_transactions_output, load_execute_us) = measure_us!(bank
            .load_and_execute_transactions(
                &batch,
                max_age,
                &mut metrics.execute_timings,
                &mut metrics.errors,
                TransactionProcessingConfig {
                    account_overrides: Some(account_overrides),
                    check_program_modification_slot: bank.check_program_modification_slot(),
                    log_messages_bytes_limit: *log_messages_bytes_limit,
                    limit_to_load_programs: true,
                    recording_config: ExecutionRecordingConfig::new_single_setting(
                        transaction_status_sender_enabled
                    ),
                },
            ));
        debug!(
            "bundle id: {} loaded_transactions: {:?}",
            bundle.bundle_id, load_and_execute_transactions_output.processing_results
        );
        metrics
            .load_execute_us
            .add_assign(Saturating(load_execute_us));

        // All transactions within a bundle are expected to be executable + not fail
        // If there's any transactions that executed and failed or didn't execute due to
        // unexpected failures (not locking related), bail out of bundle execution early.
        if let Err((failing_tx, exec_result)) = check_bundle_execution_results(
            load_and_execute_transactions_output
                .processing_results
                .as_slice(),
            batch.sanitized_transactions(),
        ) {
            // TODO (LB): we should try to return partial results here for successful bundles in a parallel batch.
            //  given a bundle that write locks the following accounts [[A], [B], [C]]
            //  when B fails, we could return the execution results for A and C, but leave B out.
            //  however, if we have bundle that write locks accounts [[A_1], [A_2], [B], [C]] and B fails
            //  we'll get the results for A_1 but not [A_2], [B], [C] due to the way this loop executes.
            debug!(
                "bundle: {} execution error; signature: {} error: {:?}",
                bundle.bundle_id,
                failing_tx.signature(),
                exec_result
            );
            return LoadAndExecuteBundleOutput {
                bundle_transaction_results,
                metrics,
                result: Err(LoadAndExecuteBundleError::TransactionError {
                    signature: *failing_tx.signature(),
                    execution_result: Box::new(exec_result.clone()),
                }),
            };
        }

        // If none of the transactions were executed, most likely an AccountInUse error
        // need to retry to ensure that all transactions in the bundle are executed.
        if !load_and_execute_transactions_output
            .processing_results
            .iter()
            .any(|r| r.is_ok())
        {
            metrics.num_retries.add_assign(Saturating(1));
            debug!(
                "bundle: {} no transaction executed, retrying",
                bundle.bundle_id
            );
            continue;
        }

        // Cache accounts so next iterations of loop can load cached state instead of using
        // AccountsDB, which will contain stale account state because results aren't committed
        // to the bank yet.
        // NOTE: collect_accounts_to_store does not handle any state changes related to
        // failed, non-nonce transactions.
        let m = Measure::start("cache");
        let accounts = collect_accounts_to_store(
            batch.sanitized_transactions(),
            &None::<Vec<SanitizedTransaction>>,
            &load_and_execute_transactions_output.processing_results,
        )
        .0;
        for (pubkey, data) in accounts {
            if data.lamports() == 0 {
                account_overrides.set_account(pubkey, Some(AccountSharedData::default()));
            } else {
                account_overrides.set_account(pubkey, Some(data.clone()));
            }
        }
        metrics
            .cache_accounts_us
            .add_assign(Saturating(m.end_as_us()));

        let end = max(
            chunk_start.saturating_add(batch.sanitized_transactions().len()),
            post_execution_accounts.len(),
        );

        let m = Measure::start("accounts");
        let accounts_requested = &post_execution_accounts[chunk_start..end];
        let post_tx_execution_accounts =
            get_account_transactions(bank, account_overrides, accounts_requested, &batch);
        metrics
            .collect_pre_post_accounts_us
            .add_assign(Saturating(m.end_as_us()));

        let processing_end = batch.lock_results().iter().position(|lr| lr.is_err());
        if let Some(end) = processing_end {
            chunk_start = chunk_start.saturating_add(end);
        } else {
            chunk_start = chunk_end;
        }

        bundle_transaction_results.push(BundleTransactionsOutput {
            transactions: chunk,
            load_and_execute_transactions_output,
            pre_tx_execution_accounts,
            post_tx_execution_accounts,
        });
    }

    LoadAndExecuteBundleOutput {
        bundle_transaction_results,
        metrics,
        result: Ok(()),
    }
}

fn get_account_transactions(
    bank: &Bank,
    account_overrides: &AccountOverrides,
    accounts: &[Option<Vec<Pubkey>>],
    batch: &TransactionBatch<RuntimeTransaction<SanitizedTransaction>>,
) -> Vec<Option<Vec<(Pubkey, AccountSharedData)>>> {
    let iter = izip!(batch.lock_results().iter(), accounts.iter());

    iter.map(|(lock_result, accounts_requested)| {
        if lock_result.is_ok() {
            accounts_requested.as_ref().map(|accounts_requested| {
                accounts_requested
                    .iter()
                    .map(|a| match account_overrides.get(a) {
                        None => (*a, bank.get_account(a).unwrap_or_default()),
                        Some(data) => (*a, data.clone()),
                    })
                    .collect()
            })
        } else {
            None
        }
    })
    .collect()
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            bundle_execution::{load_and_execute_bundle, LoadAndExecuteBundleError},
            derive_bundle_id_from_sanitized_transactions, SanitizedBundle,
        },
        anchor_lang::solana_program::clock::MAX_PROCESSING_AGE,
        assert_matches::assert_matches,
        solana_keypair::Keypair,
        solana_ledger::genesis_utils::create_genesis_config,
        solana_pubkey::Pubkey,
        solana_runtime::{bank::Bank, bank_forks::BankForks, genesis_utils::GenesisConfigInfo},
        solana_runtime_transaction::{
            runtime_transaction::RuntimeTransaction, transaction_with_meta::TransactionWithMeta,
        },
        solana_signer::Signer,
        solana_svm::transaction_processing_result::TransactionProcessingResultExtensions,
        solana_system_transaction::transfer,
        solana_transaction::{
            sanitized::MessageHash, versioned::VersionedTransaction, Transaction,
        },
        solana_transaction_error::TransactionError,
        std::{
            sync::{Arc, Barrier, RwLock},
            thread::{sleep, spawn},
            time::Duration,
        },
    };

    const MAX_PROCESSING_TIME: Duration = Duration::from_secs(1);
    const LOG_MESSAGE_BYTES_LIMITS: Option<usize> = Some(100_000);
    const MINT_AMOUNT_LAMPORTS: u64 = 1_000_000;

    fn create_simple_test_bank(
        lamports: u64,
    ) -> (GenesisConfigInfo, Arc<Bank>, Arc<RwLock<BankForks>>) {
        let genesis_config_info = create_genesis_config(lamports);
        let (bank, bank_forks) =
            Bank::new_with_bank_forks_for_tests(&genesis_config_info.genesis_config);
        (genesis_config_info, bank, bank_forks)
    }

    fn make_bundle(txs: &[Transaction], bank: &Bank) -> SanitizedBundle {
        let transactions: Vec<_> = txs
            .iter()
            .map(|tx| {
                let tx = VersionedTransaction::from(tx.clone());
                RuntimeTransaction::try_create(
                    tx,
                    MessageHash::Compute,
                    None,
                    bank,
                    bank.get_reserved_account_keys(),
                    false,
                )
                .unwrap()
            })
            .collect();

        let bundle_id = derive_bundle_id_from_sanitized_transactions(&transactions);

        SanitizedBundle {
            transactions,
            bundle_id,
        }
    }

    /// A single, valid bundle shall execute successfully and return the correct BundleTransactionsOutput content
    #[test]
    fn test_single_transaction_bundle_success() {
        const TRANSFER_AMOUNT: u64 = 1_000;
        let (genesis_config_info, bank, _bank_forks) =
            create_simple_test_bank(MINT_AMOUNT_LAMPORTS);

        let kp = Keypair::new();
        let transactions = vec![transfer(
            &genesis_config_info.mint_keypair,
            &kp.pubkey(),
            TRANSFER_AMOUNT,
            genesis_config_info.genesis_config.hash(),
        )];
        let bundle = make_bundle(&transactions, &bank);
        let default_accounts = vec![None; bundle.transactions.len()];

        let execution_result = load_and_execute_bundle(
            &bank,
            &bundle,
            MAX_PROCESSING_AGE,
            &MAX_PROCESSING_TIME,
            true,
            &LOG_MESSAGE_BYTES_LIMITS,
            false,
            None,
            &default_accounts,
            &default_accounts,
        );

        // make sure the bundle succeeded
        assert!(execution_result.result.is_ok());

        // check to make sure there was one batch returned with one transaction that was the same that was put in
        assert_eq!(execution_result.bundle_transaction_results.len(), 1);
        let tx_result = execution_result.bundle_transaction_results.first().unwrap();
        assert_eq!(tx_result.transactions.len(), 1);
        assert_eq!(
            tx_result.transactions[0].to_versioned_transaction(),
            bundle.transactions[0].to_versioned_transaction()
        );

        // make sure the transaction executed successfully
        assert_eq!(
            tx_result
                .load_and_execute_transactions_output
                .processing_results
                .len(),
            1
        );
        let execution_result = tx_result
            .load_and_execute_transactions_output
            .processing_results
            .first()
            .unwrap();
        assert!(execution_result.is_ok());
        let processed = execution_result.as_ref().unwrap().executed_transaction();
        assert!(processed.is_some());
        let executed = processed.unwrap();
        assert!(executed.was_successful());
    }

    /// Test a simple failure
    #[test]
    fn test_single_transaction_bundle_fail() {
        const TRANSFER_AMOUNT: u64 = 1_000;
        let (genesis_config_info, bank, _bank_forks) =
            create_simple_test_bank(MINT_AMOUNT_LAMPORTS);

        // kp has no funds, transfer will fail
        let kp = Keypair::new();
        let transactions = vec![transfer(
            &kp,
            &kp.pubkey(),
            TRANSFER_AMOUNT,
            genesis_config_info.genesis_config.hash(),
        )];
        let bundle = make_bundle(&transactions, &bank);

        let default_accounts = vec![None; bundle.transactions.len()];
        let execution_result = load_and_execute_bundle(
            &bank,
            &bundle,
            MAX_PROCESSING_AGE,
            &MAX_PROCESSING_TIME,
            true,
            &LOG_MESSAGE_BYTES_LIMITS,
            false,
            None,
            &default_accounts,
            &default_accounts,
        );

        assert_eq!(execution_result.bundle_transaction_results.len(), 0);

        assert!(execution_result.result.is_err());

        match execution_result.result.unwrap_err() {
            LoadAndExecuteBundleError::ProcessingTimeExceeded(_)
            | LoadAndExecuteBundleError::LockError { .. }
            | LoadAndExecuteBundleError::InvalidPreOrPostAccounts => {
                unreachable!();
            }
            LoadAndExecuteBundleError::TransactionError {
                signature,
                execution_result,
            } => {
                assert_eq!(signature, *bundle.transactions[0].signature());
                assert_eq!(
                    execution_result.unwrap_err(),
                    TransactionError::AccountNotFound
                );
            }
        }
    }

    /// Tests a multi-tx bundle that succeeds. Checks the returned results
    #[test]
    fn test_multi_transaction_bundle_success() {
        const TRANSFER_AMOUNT_1: u64 = 100_000;
        const TRANSFER_AMOUNT_2: u64 = 50_000;
        const TRANSFER_AMOUNT_3: u64 = 10_000;
        let (genesis_config_info, bank, _bank_forks) =
            create_simple_test_bank(MINT_AMOUNT_LAMPORTS);
        // mint transfers 100k to 1
        // 1 transfers 50k to 2
        // 2 transfers 10k to 3
        // should get executed in 3 batches [[1], [2], [3]]
        let kp1 = Keypair::new();
        let kp2 = Keypair::new();
        let kp3 = Keypair::new();
        let transactions = vec![
            transfer(
                &genesis_config_info.mint_keypair,
                &kp1.pubkey(),
                TRANSFER_AMOUNT_1,
                genesis_config_info.genesis_config.hash(),
            ),
            transfer(
                &kp1,
                &kp2.pubkey(),
                TRANSFER_AMOUNT_2,
                genesis_config_info.genesis_config.hash(),
            ),
            transfer(
                &kp2,
                &kp3.pubkey(),
                TRANSFER_AMOUNT_3,
                genesis_config_info.genesis_config.hash(),
            ),
        ];
        let bundle = make_bundle(&transactions, &bank);

        let default_accounts = vec![None; bundle.transactions.len()];
        let execution_result = load_and_execute_bundle(
            &bank,
            &bundle,
            MAX_PROCESSING_AGE,
            &MAX_PROCESSING_TIME,
            true,
            &LOG_MESSAGE_BYTES_LIMITS,
            false,
            None,
            &default_accounts,
            &default_accounts,
        );

        assert!(
            execution_result.result.is_ok(),
            "{:?}",
            execution_result.result
        );
        assert_eq!(execution_result.bundle_transaction_results.len(), 3);

        // first batch contains the first tx that was executed
        assert_eq!(
            execution_result.bundle_transaction_results[0]
                .transactions
                .iter()
                .map(|r| r.to_versioned_transaction())
                .collect::<Vec<VersionedTransaction>>(),
            bundle
                .transactions
                .iter()
                .map(|r| r.to_versioned_transaction())
                .collect::<Vec<VersionedTransaction>>()
        );
        assert_eq!(
            execution_result.bundle_transaction_results[0]
                .load_and_execute_transactions_output
                .processing_results
                .len(),
            3
        );
        assert!(execution_result
            .bundle_transaction_results
            .first()
            .unwrap()
            .load_and_execute_transactions_output
            .processing_results
            .first()
            .unwrap()
            .as_ref()
            .unwrap()
            .status()
            .is_ok());
        assert_eq!(
            execution_result.bundle_transaction_results[0]
                .load_and_execute_transactions_output
                .processing_results
                .get(1)
                .unwrap()
                .flattened_result(),
            Err(TransactionError::AccountInUse)
        );
        assert_eq!(
            execution_result.bundle_transaction_results[0]
                .load_and_execute_transactions_output
                .processing_results
                .get(2)
                .unwrap()
                .flattened_result(),
            Err(TransactionError::AccountInUse)
        );

        // in the second batch, the second transaction was executed
        assert_eq!(
            execution_result.bundle_transaction_results[1]
                .transactions
                .iter()
                .map(|r| r.to_versioned_transaction())
                .collect::<Vec<VersionedTransaction>>(),
            bundle.transactions[1..]
                .iter()
                .map(|r| r.to_versioned_transaction())
                .collect::<Vec<VersionedTransaction>>()
        );
        assert_eq!(
            execution_result.bundle_transaction_results[1]
                .load_and_execute_transactions_output
                .processing_results
                .len(),
            2
        );
        assert!(execution_result.bundle_transaction_results[1]
            .load_and_execute_transactions_output
            .processing_results
            .first()
            .unwrap()
            .as_ref()
            .unwrap()
            .status()
            .is_ok());
        assert_eq!(
            execution_result.bundle_transaction_results[1]
                .load_and_execute_transactions_output
                .processing_results
                .get(1)
                .unwrap()
                .flattened_result(),
            Err(TransactionError::AccountInUse)
        );

        // in the third batch, the third transaction was executed
        assert_eq!(
            execution_result.bundle_transaction_results[2]
                .transactions
                .iter()
                .map(|r| r.to_versioned_transaction())
                .collect::<Vec<VersionedTransaction>>(),
            bundle.transactions[2..]
                .iter()
                .map(|r| r.to_versioned_transaction())
                .collect::<Vec<VersionedTransaction>>(),
        );
        assert_eq!(
            execution_result.bundle_transaction_results[2]
                .load_and_execute_transactions_output
                .processing_results
                .len(),
            1
        );
        assert!(execution_result.bundle_transaction_results[2]
            .load_and_execute_transactions_output
            .processing_results
            .first()
            .unwrap()
            .as_ref()
            .unwrap()
            .status()
            .is_ok());
    }

    /// Tests a multi-tx bundle with the middle transaction failing.
    #[test]
    fn test_multi_transaction_bundle_fails() {
        let (genesis_config_info, bank, _bank_forks) =
            create_simple_test_bank(MINT_AMOUNT_LAMPORTS);

        let kp1 = Keypair::new();
        let kp2 = Keypair::new();
        let kp3 = Keypair::new();
        let transactions = vec![
            transfer(
                &genesis_config_info.mint_keypair,
                &kp1.pubkey(),
                100_000,
                genesis_config_info.genesis_config.hash(),
            ),
            transfer(
                &kp2,
                &kp3.pubkey(),
                100_000,
                genesis_config_info.genesis_config.hash(),
            ),
            transfer(
                &kp1,
                &kp2.pubkey(),
                100_000,
                genesis_config_info.genesis_config.hash(),
            ),
        ];
        let bundle = make_bundle(&transactions, &bank);

        let default_accounts = vec![None; bundle.transactions.len()];
        let execution_result = load_and_execute_bundle(
            &bank,
            &bundle,
            MAX_PROCESSING_AGE,
            &MAX_PROCESSING_TIME,
            true,
            &LOG_MESSAGE_BYTES_LIMITS,
            false,
            None,
            &default_accounts,
            &default_accounts,
        );
        match execution_result.result.as_ref().unwrap_err() {
            LoadAndExecuteBundleError::ProcessingTimeExceeded(_)
            | LoadAndExecuteBundleError::LockError { .. }
            | LoadAndExecuteBundleError::InvalidPreOrPostAccounts => {
                unreachable!();
            }

            LoadAndExecuteBundleError::TransactionError {
                signature,
                execution_result: tx_failure,
            } => {
                assert_eq!(signature, bundle.transactions[1].signature());
                assert_eq!(
                    tx_failure.flattened_result(),
                    Err(TransactionError::AccountNotFound)
                );
            }
        }
    }

    /// Tests that when the max processing time is exceeded, the bundle is an error
    #[test]
    fn test_bundle_max_processing_time_exceeded() {
        let (genesis_config_info, bank, _bank_forks) =
            create_simple_test_bank(MINT_AMOUNT_LAMPORTS);

        let kp = Keypair::new();
        let transactions = vec![transfer(
            &genesis_config_info.mint_keypair,
            &kp.pubkey(),
            1,
            genesis_config_info.genesis_config.hash(),
        )];
        let bundle = make_bundle(&transactions, &bank);

        let locked_transfer = vec![RuntimeTransaction::from_transaction_for_tests(transfer(
            &genesis_config_info.mint_keypair,
            &kp.pubkey(),
            2,
            genesis_config_info.genesis_config.hash(),
        ))];

        // locks it and prevents execution bc write lock on genesis_config_info.mint_keypair + kp.pubkey() held
        let _batch = bank.prepare_sanitized_batch(&locked_transfer);

        let default = vec![None; bundle.transactions.len()];
        let result = load_and_execute_bundle(
            &bank,
            &bundle,
            MAX_PROCESSING_AGE,
            &Duration::from_millis(100),
            false,
            &None,
            false,
            None,
            &default,
            &default,
        );
        assert_matches!(
            result.result,
            Err(LoadAndExecuteBundleError::ProcessingTimeExceeded(_))
        );
    }

    #[test]
    fn test_simulate_bundle_with_locked_account_works() {
        let (genesis_config_info, bank, _bank_forks) =
            create_simple_test_bank(MINT_AMOUNT_LAMPORTS);

        let kp = Keypair::new();
        let transactions = vec![transfer(
            &genesis_config_info.mint_keypair,
            &kp.pubkey(),
            1,
            genesis_config_info.genesis_config.hash(),
        )];
        let bundle = make_bundle(&transactions, &bank);

        let locked_transfer = vec![RuntimeTransaction::from_transaction_for_tests(transfer(
            &genesis_config_info.mint_keypair,
            &kp.pubkey(),
            2,
            genesis_config_info.genesis_config.hash(),
        ))];

        let _batch = bank.prepare_sanitized_batch(&locked_transfer);

        // simulation ignores account locks so you can simulate bundles on unfrozen banks
        let default = vec![None; bundle.transactions.len()];
        let result = load_and_execute_bundle(
            &bank,
            &bundle,
            MAX_PROCESSING_AGE,
            &Duration::from_millis(100),
            false,
            &None,
            true,
            None,
            &default,
            &default,
        );
        assert!(result.result.is_ok());
    }

    /// Creates a multi-tx bundle and temporarily locks the accounts for one of the transactions in a bundle.
    /// Ensures the result is what's expected
    #[test]
    fn test_bundle_works_with_released_account_locks() {
        let (genesis_config_info, bank, _bank_forks) =
            create_simple_test_bank(MINT_AMOUNT_LAMPORTS);
        let barrier = Arc::new(Barrier::new(2));

        let kp = Keypair::new();

        let transactions = vec![transfer(
            &genesis_config_info.mint_keypair,
            &kp.pubkey(),
            1,
            genesis_config_info.genesis_config.hash(),
        )];
        let bundle = make_bundle(&transactions, &bank);

        let locked_transfer = vec![RuntimeTransaction::from_transaction_for_tests(transfer(
            &genesis_config_info.mint_keypair,
            &kp.pubkey(),
            2,
            genesis_config_info.genesis_config.hash(),
        ))];

        // background thread locks the accounts for a bit then unlocks them
        let thread = {
            let barrier = barrier.clone();
            let bank = bank.clone();
            spawn(move || {
                let batch = bank.prepare_sanitized_batch(&locked_transfer);
                barrier.wait();
                sleep(Duration::from_millis(500));
                drop(batch);
            })
        };

        let _ = barrier.wait();

        // load_and_execute_bundle should spin for a bit then process after the 500ms sleep is over
        let default = vec![None; bundle.transactions.len()];
        let result = load_and_execute_bundle(
            &bank,
            &bundle,
            MAX_PROCESSING_AGE,
            &Duration::from_secs(2),
            false,
            &None,
            false,
            None,
            &default,
            &default,
        );
        assert!(result.result.is_ok());

        thread.join().unwrap();
    }

    /// Tests that when the max processing time is exceeded, the bundle is an error
    #[test]
    fn test_bundle_bad_pre_post_accounts() {
        const PRE_EXECUTION_ACCOUNTS: [Option<Vec<Pubkey>>; 2] = [None, None];
        let (genesis_config_info, bank, _bank_forks) =
            create_simple_test_bank(MINT_AMOUNT_LAMPORTS);

        let kp = Keypair::new();
        let transactions = vec![transfer(
            &genesis_config_info.mint_keypair,
            &kp.pubkey(),
            1,
            genesis_config_info.genesis_config.hash(),
        )];
        let bundle = make_bundle(&transactions, &bank);

        let result = load_and_execute_bundle(
            &bank,
            &bundle,
            MAX_PROCESSING_AGE,
            &Duration::from_millis(100),
            false,
            &None,
            false,
            None,
            &PRE_EXECUTION_ACCOUNTS,
            &vec![None; bundle.transactions.len()],
        );
        assert_matches!(
            result.result,
            Err(LoadAndExecuteBundleError::InvalidPreOrPostAccounts)
        );

        let result = load_and_execute_bundle(
            &bank,
            &bundle,
            MAX_PROCESSING_AGE,
            &Duration::from_millis(100),
            false,
            &None,
            false,
            None,
            &vec![None; bundle.transactions.len()],
            &PRE_EXECUTION_ACCOUNTS,
        );
        assert_matches!(
            result.result,
            Err(LoadAndExecuteBundleError::InvalidPreOrPostAccounts)
        );
    }
}
