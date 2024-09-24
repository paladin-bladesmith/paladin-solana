use std::cell::RefCell;

use hashbrown::HashMap;
use solana_bundle::bundle_execution::LoadAndExecuteBundleOutput;
use solana_sdk::{account::ReadableAccount, pubkey::Pubkey, transaction::MAX_TX_ACCOUNT_LOCKS};

use super::MAX_PACKETS_PER_BUNDLE;

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

thread_local! {
    static AMM_MAP: RefCell<HashMap<Pubkey, [bool; MAX_PACKETS_PER_BUNDLE]>> = RefCell::new(HashMap::with_capacity(MAX_TX_ACCOUNT_LOCKS * MAX_PACKETS_PER_BUNDLE));
}

pub(crate) fn bundle_is_front_run(bundle_execution_results: &LoadAndExecuteBundleOutput) -> bool {
    AMM_MAP.with(|map| map.borrow_mut().clear());

    // Early return on failed/single TX bundles.
    let tx_count: usize = bundle_execution_results
        .bundle_transaction_results()
        .iter()
        .map(|batch| batch.transactions().len())
        .sum();
    if !bundle_execution_results.executed_ok() || tx_count <= 1 {
        return false;
    }

    // First we insert flags for which TXs touch which write-lock accounts.
    map_write_lock_usage(bundle_execution_results);

    // Now we check all the write lock accounts for overlaps.
    let mut adjacency_matrix = [[false; MAX_PACKETS_PER_BUNDLE]; MAX_PACKETS_PER_BUNDLE];
    AMM_MAP.with_borrow(|map| {
        for (account, txs) in map {
            if txs.iter().filter(|tx| **tx).count() > 1 {
                todo!("Update adjacency matrix");
            }
        }
    });

    // TODO:
    //
    // Our challenge is as follows:
    //
    // 1. Identify which TXs have overlapping AMM write locks.
    //   a. An overlapping AMM write lock is when 2 TXs write lock the exact
    //      same AMM-owned account. We use write locking as a proxy for
    //      swapping.
    //   b. HashMap<Pubkey, [bool; MAX_TX_PER_BUNDLE]> shows which TXs have
    //      overlapping swaps.
    // 2. Determine if overlapping TXs have overlapping signers.
    //   a. For each overlapping TX we need to brute-force compare the signers.

    todo!()
}

fn map_write_lock_usage(bundle_execution_results: &LoadAndExecuteBundleOutput) {
    // Check all TXs for write-locked pool accounts.
    for (i, (load_res, sanitized_tx)) in bundle_execution_results
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
        .enumerate()
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
        let writeable_amm_accounts = message
            .account_keys()
            .iter()
            .enumerate()
            .filter(|(i, _)| sanitized_tx.message().is_writable(*i))
            .map(|(i, _)| &loaded_tx.accounts[i])
            .filter(|(_, account)| AMM_PROGRAMS.contains(account.owner()));

        // Record this TX's access of the writeable account.
        for (key, _) in writeable_amm_accounts {
            println!("WRITE: {key}");

            AMM_MAP.with_borrow_mut(|map| map.entry_ref(key).or_default()[i] = true);
        }
    }
}
