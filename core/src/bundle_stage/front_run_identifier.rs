use {
    crate::{
        banking_stage::{
            committer::CommitTransactionDetails, leader_slot_metrics::ProcessTransactionsSummary,
            leader_slot_timing_metrics::LeaderExecuteAndCommitTimings, qos_service::QosService,
            unprocessed_transaction_storage::UnprocessedTransactionStorage,
        },
        bundle_stage::{
            bundle_account_locker::{BundleAccountLocker, LockedBundle},
            bundle_reserved_space_manager::BundleReservedSpaceManager,
            bundle_stage_leader_metrics::BundleStageLeaderMetrics,
            committer::Committer,
        },
        consensus_cache_updater::ConsensusCacheUpdater,
        immutable_deserialized_bundle::ImmutableDeserializedBundle,
        proxy::block_engine_stage::BlockBuilderFeeInfo,
        tip_manager::TipManager,
    },
    solana_accounts_db::transaction_error_metrics::TransactionErrorMetrics,
    solana_bundle::{
        bundle_execution::{
            load_and_execute_bundle, BundleExecutionMetrics, LoadAndExecuteBundleOutput,
        },
        BundleExecutionError, BundleExecutionResult, TipError,
    },
    solana_cost_model::transaction_cost::TransactionCost,
    solana_gossip::cluster_info::ClusterInfo,
    solana_measure::{measure, measure_us},
    solana_poh::poh_recorder::{BankStart, RecordTransactionsSummary, TransactionRecorder},
    solana_runtime::bank::Bank,
    solana_sdk::{
        account::ReadableAccount,
        bundle::SanitizedBundle,
        clock::{Slot, MAX_PROCESSING_AGE},
        feature_set,
        pubkey::Pubkey,
        transaction::{self},
    },
    std::{
        collections::{hash_map::Entry, HashMap, HashSet},
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    },
};

pub(crate) fn bundle_is_front_run(bundle_execution_results: &LoadAndExecuteBundleOutput) -> bool {
    let tx_count: usize = bundle_execution_results
        .bundle_transaction_results()
        .iter()
        .map(|batch| batch.transactions().len())
        .sum();

    // Ignore failed/single TX bundles.
    if !bundle_execution_results.executed_ok() || tx_count <= 1 {
        return false;
    }

    const AMM_PROGRAMS: &[Pubkey] = &[
        solana_sdk::pubkey!("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"), // RaydiumV4
        solana_sdk::pubkey!("9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin"), // Serum DEX V3
        solana_sdk::pubkey!("Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB"), // Meteora CPMM
        solana_sdk::pubkey!("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"),  // Whirlpool
        solana_sdk::pubkey!("srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX"),  // Serum
        solana_sdk::pubkey!("DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1"), // Orca AMM V1
        solana_sdk::pubkey!("9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP"), // Orca AMM V2
        solana_sdk::pubkey!("LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"),  // Meteora DLMM
        solana_sdk::pubkey!("SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ"),  // Saber
        solana_sdk::pubkey!("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"), // Raydium CLMM
        solana_sdk::pubkey!("PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY"),  // Phoenix
        solana_sdk::pubkey!("opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb"),  // Open Book
        solana_sdk::pubkey!("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"),  // Pump.fun
    ];

    // Track the initial signers for each pool account.
    let mut initial_pool_signers: HashMap<_, Vec<_>> = HashMap::default();

    // Check all TXs for write-locked pool accounts.
    for (load_res, sanitized_tx) in bundle_execution_results
        .bundle_transaction_results()
        .iter()
        .flat_map(|batch| {
            batch
                .load_and_execute_transactions_output()
                .loaded_transactions
                .iter()
                .enumerate()
                .map(|(i, load_res)| (&load_res.0, &batch.transactions()[i]))
        })
    {
        let loaded_tx = match load_res {
            Ok(tx) => tx,
            Err(err) => {
                error!("Unexpected failed bundle; err={err:?}");
                continue;
            }
        };

        // Get the signers for this transaction.
        let message = sanitized_tx.message();
        let current_signers: Vec<_> = message
            .account_keys()
            .iter()
            .take(sanitized_tx.message().num_signatures() as usize)
            .collect();
        println!("SIGNERS: {current_signers:?}");

        // Find all the AMM owned writeable accounts.
        let writeable_accounts = message
            .account_keys()
            .iter()
            .enumerate()
            .filter(|(i, _)| sanitized_tx.message().is_writable(*i));
        let writeable_amm_accounts = writeable_accounts
            .map(|(i, _)| &loaded_tx.accounts[i])
            .filter(|(_, account)| AMM_PROGRAMS.contains(account.owner()));

        // For each writeable AMM account, the set of signers between transactions
        // must have at least one overlapping signer.
        for (key, _) in writeable_amm_accounts {
            println!("WRITE: {key}");
            match initial_pool_signers.entry(*key) {
                Entry::Vacant(entry) => {
                    entry.insert(current_signers.iter().map(|key| **key).collect());
                }
                Entry::Occupied(entry) => {
                    if entry
                        .get()
                        .iter()
                        .all(|signer| !current_signers.contains(&signer))
                    {
                        println!("FRONT RUN");

                        return true;
                    }
                }
            }
        }
    }

    false
}
