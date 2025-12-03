use {
    hashbrown::HashMap,
    solana_account::ReadableAccount,
    solana_pubkey::Pubkey,
    solana_sdk::transaction::VersionedTransaction,
    solana_svm::{
        account_loader::LoadedTransaction,
        transaction_processing_result::{ProcessedTransaction, TransactionProcessingResultExtensions},
    },
    solana_transaction::{TransactionError, sanitized::MAX_TX_ACCOUNT_LOCKS},
    std::cell::RefCell,
};

const MAX_PACKETS_PER_BUNDLE: usize = 5;

pub(crate) const AMM_PROGRAMS: &[Pubkey] = &[
    solana_pubkey::pubkey!("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"), // RaydiumV4
    solana_pubkey::pubkey!("9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin"), // Serum DEX V3
    solana_pubkey::pubkey!("Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB"), // Meteora CPMM
    solana_pubkey::pubkey!("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"),  // Whirlpool
    solana_pubkey::pubkey!("srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX"),  // Serum
    solana_pubkey::pubkey!("DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1"), // Orca AMM V1
    solana_pubkey::pubkey!("9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP"), // Orca AMM V2
    solana_pubkey::pubkey!("LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"),  // Meteora DLMM
    solana_pubkey::pubkey!("SSwpkEEcbUqx4vtoEByFjSkhKdCT862DNVb52nZg1UZ"),  // Saber
    solana_pubkey::pubkey!("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"), // Raydium CLMM
    solana_pubkey::pubkey!("CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"), // Raydium CPMM
    solana_pubkey::pubkey!("PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY"),  // Phoenix
    solana_pubkey::pubkey!("opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb"),  // Open Book
    solana_pubkey::pubkey!("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"),  // Pump.fun
];

thread_local! {
    static AMM_MAP: RefCell<HashMap<Pubkey, [bool; MAX_PACKETS_PER_BUNDLE]>>
        = RefCell::new(HashMap::with_capacity(MAX_TX_ACCOUNT_LOCKS * MAX_PACKETS_PER_BUNDLE));
}

#[must_use]
pub(crate) fn is_bundle_front_run<'a>(
    bundle: &Vec<VersionedTransaction>,
    processing_results: &Vec<Result<ProcessedTransaction, TransactionError>>,
) -> bool {
    AMM_MAP.with_borrow_mut(|map| map.clear());

    let tx_count = bundle.len();
    let output_count = processing_results.len();

    // Confirm output matches bundle size
    if tx_count != output_count {
        eprintln!("BUG: Invalid assumption about batch layout");
        return false;
    }

    // No reason to check if only one TX
    if tx_count <= 1 {
        return false;
    }

    // Confirm all TXs were executed ok
    if !is_executed_ok(processing_results) {
        return false;
    }

    let loaded = processing_results
        .iter()
        .filter_map(|t| {
            t.processed_transaction().and_then(|pt| match pt {
                solana_svm::transaction_processing_result::ProcessedTransaction::Executed(
                    executed_tx,
                ) => Some(&executed_tx.loaded_transaction),
                solana_svm::transaction_processing_result::ProcessedTransaction::FeesOnly(_) => {
                    None
                }
            })
        })
        .collect::<Vec<_>>();

    // Check all TXs for write-locked pool accounts.
    for (i, tx) in bundle.iter().enumerate() {
        // Find all the AMM owned writeable accounts.
        let writeable_amm_accounts = writable_accounts_owners(tx, loaded[i])
            .filter(|account| AMM_PROGRAMS.contains(&account.1));

        // Record this TX's access of the writeable account.
        for account in writeable_amm_accounts {
            // TODO: Use entry_ref once merged upstream.
            AMM_MAP.with_borrow_mut(|map| map.entry(account.0).or_default()[i] = true);
        }
    }

    // Compute which TXs overlap with each other.
    let mut overlap_matrix = [[false; MAX_PACKETS_PER_BUNDLE]; MAX_PACKETS_PER_BUNDLE];
    AMM_MAP.with_borrow(|map| {
        for txs in map.values() {
            if txs.iter().filter(|tx| **tx).count() <= 1 {
                continue;
            }

            for (i, tx1) in txs.iter().enumerate() {
                for (j, tx2) in txs.iter().enumerate() {
                    if *tx1 && *tx2 {
                        overlap_matrix[i][j] = true;
                        overlap_matrix[j][i] = true;
                    }
                }
            }
        }
    });

    // Brute-force signer checks for overlapping TXs.
    for i in 0..overlap_matrix.len() {
        'outer: for j in i..overlap_matrix.len() {
            if overlap_matrix[i][j] {
                let Some(i) = bundle.iter().nth(i) else {
                    eprintln!("BUG: i not found");
                    return false;
                };
                let Some(j) = bundle.iter().nth(j) else {
                    eprintln!("BUG: j not found");
                    return false;
                };

                // Brute force compare the signers.
                for (i, j) in signers(i).flat_map(|i| signers(j).map(move |j| (i, j))) {
                    // If any signer matches then this overlap is not a front-run.
                    if i == j {
                        continue 'outer;
                    }
                }

                // If we reach this point it means none of the signers matched and thus this is a
                // frontrun.
                return true;
            }
        }
    }

    false
}

/// Confirm if all TXs in bundle wer executed ok
fn is_executed_ok(processing_results: &Vec<Result<ProcessedTransaction, TransactionError>>) -> bool {
        processing_results
        .iter()
        .all(|t| t.was_processed_with_successful_result())
}

fn signers(tx: &VersionedTransaction) -> impl Iterator<Item = &Pubkey> {
    tx.message
        .static_account_keys()
        .iter()
        // TODO: Check what happens if a precompile is used.
        .take(tx.signatures.len() as usize)
}

fn writable_accounts_owners<'a>(
    tx: &'a VersionedTransaction,
    loaded_tx: &'a LoadedTransaction,
) -> impl Iterator<Item = (Pubkey, Pubkey)> + use<'a> {
    tx.message
        .static_account_keys()
        .iter()
        .enumerate()
        .filter(|(i, _)| tx.message.is_maybe_writable(*i, None))
        .map(|(i, key)| (key.clone(), loaded_tx.accounts[i].1.owner().clone()))
}

// pub(crate) trait BundleResult<'a> {
//     type Transaction: BundleTransaction;

//     fn executed_ok(&self) -> bool;
//     fn transactions(&'a self) -> impl Iterator<Item = Self::Transaction>;
// }

// pub(crate) trait BundleTransaction {
//     /// Iterator over each signer.
//     fn signers(&self) -> impl Iterator<Item = &Pubkey>;
//     /// Iterator over each loaded account's owner.
//     fn writable_accounts_owners(&self) -> impl Iterator<Item = AccountRef>;
// }

// pub(crate) struct AccountRef<'a> {
//     key: &'a Pubkey,
//     owner: &'a Pubkey,
// }

// impl<'a> BundleResult<'a> for LoadAndExecuteTransactionsOutput {
//     type Transaction = (
//         &'a VersionedTransaction,
//         &'a LoadedTransaction,
//     );

//     fn executed_ok(&self) -> bool {
//         self.processing_results.iter().any(|t| !t.was_processed_with_successful_result() )
//     }

//     fn transactions(&'a self) -> impl Iterator<Item = Self::Transaction> {
//         self.processing_results.into_iter().flat_map(|batch| {
//             // unwrap safe because we should check if executed ok first
//             let output = batch.unwrap().executed_transaction().unwrap();
//             if batch.transactions().len() != output.processing_results.len()
//                 || batch.transactions().len() != output.processing_results.len()
//             {
//                 eprintln!("BUG: Invalid assumption about batch layout");
//             }

//             izip!(batch.transactions(), &output.processing_results,).filter_map(
//                 |(sanitized, exec)| match exec {
//                     Ok(exec) => Some((sanitized, &exec.executed_transaction()?.loaded_transaction)),
//                     Err(_) => None,
//                 },
//             )
//         })
//     }
// }

// impl<'a> BundleResult for LoadAndExecuteTransactionsOutput {
//     type Transaction = (
//         &'a RuntimeTransaction<SanitizedTransaction>,
//         &'a LoadedTransaction,
//     );

//     fn executed_ok(&self) -> bool {
//         self.result.is_ok()
//     }

//     fn transactions(&'a self) -> impl Iterator<Item = Self::Transaction> {
//         self.bundle_transaction_results.iter().flat_map(|batch| {
//             let output = batch.load_and_execute_transactions_output();
//             if batch.transactions().len() != output.processing_results.len()
//                 || batch.transactions().len() != output.processing_results.len()
//             {
//                 eprintln!("BUG: Invalid assumption about batch layout");
//             }

//             izip!(batch.transactions(), &output.processing_results,).filter_map(
//                 |(sanitized, exec)| match exec {
//                     Ok(exec) => Some((sanitized, &exec.executed_transaction()?.loaded_transaction)),
//                     Err(_) => None,
//                 },
//             )
//         })
//     }
// }

// impl BundleTransaction for VersionedTransaction
// {
//     fn signers(&self) -> impl Iterator<Item = &Pubkey> {
//         self.message.static_account_keys()
//             .iter()
//             // TODO: Check what happens if a precompile is used.
//             .take(self.signatures.len() as usize)
//     }

//     fn writable_accounts_owners(&self) -> impl Iterator<Item = AccountRef> {
//         self
//             .message.static_account_keys()
//             .iter()
//             .enumerate()
//             .filter(|(i, _)| self.message.is_maybe_writable(*i, None))
//             .map(|(i, key)| AccountRef {
//                 key,
//                 owner: self.1.accounts[i].1.owner(),
//             })
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;

//     const NOT_AMM_0: Pubkey = Pubkey::new_from_array([1; 32]);
//     const NOT_AMM_1: Pubkey = Pubkey::new_from_array([2; 32]);
//     const AMM_0: Pubkey = AMM_PROGRAMS[0];
//     const AMM_1: Pubkey = AMM_PROGRAMS[1];
//     const AMM_2: Pubkey = AMM_PROGRAMS[2];
//     const SIGNER_0: Pubkey = Pubkey::new_from_array([3; 32]);
//     const SIGNER_1: Pubkey = Pubkey::new_from_array([4; 32]);
//     const SIGNER_2: Pubkey = Pubkey::new_from_array([5; 32]);

//     struct MockBundleResult {
//         executed_ok: bool,
//         transactions: Vec<MockTransaction>,
//     }

//     impl<'a> BundleResult<'a> for MockBundleResult {
//         type Transaction = &'a MockTransaction;

//         fn executed_ok(&self) -> bool {
//             self.executed_ok
//         }

//         fn transactions(&'a self) -> impl Iterator<Item = Self::Transaction> {
//             self.transactions.iter()
//         }
//     }

//     struct MockTransaction {
//         signers: Vec<Pubkey>,
//         accounts: Vec<MockAccount>,
//     }

//     impl<'a> BundleTransaction for &'a MockTransaction {
//         fn signers(&self) -> impl Iterator<Item = &Pubkey> {
//             self.signers.iter()
//         }

//         fn writable_accounts_owners(&self) -> impl Iterator<Item = AccountRef> {
//             self.accounts
//                 .iter()
//                 .map(|MockAccount { key, owner }| AccountRef { key, owner })
//         }
//     }

//     struct MockAccount {
//         key: Pubkey,
//         owner: Pubkey,
//     }

//     struct MockData {
//         tx: vec<VersionedTransaction>,
//         output: LoadAndExecuteTransactionsOutput,
//     }

//     fn new_with_txs(signer: Vec<Pubkey>, key: Vec<Pubkey>, owner: Vec<Pubkey>) -> MockData {
//         for (i,signer) in signer.iter().enumerate() {
//             assert_eq!(key.len(), owner.len());
//     }

//     fn get_tx(signer: Pubkey, key: Pubkey, owner: Pubkey) -> VersionedTransaction {
//         let mut v = VersionedTransaction::default();

//         v.signatures.push(solana_sdk::signature::Signature::default());

//         let p = ProcessedTransaction::Executed(ExecutedTransaction {
//             loaded_transaction: LoadedTransaction::default(),
//             execution_details: TransactionExecutionDetails::default(),
//             programs_modified_by_tx: HashMap::new(),
//         });
//         let output = LoadAndExecuteTransactionsOutput::default();
//     }

//     #[test]
//     fn two_amm_no_front_run() {
// let v = VersionedTransaction::default();
// let output = LoadAndExecuteTransactionsOutput::default();

//         // Arrange.
//         let bundle = MockBundleResult {
//             executed_ok: true,
//             transactions: vec![
//                 MockTransaction {
//                     signers: vec![SIGNER_0],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([0; 32]),
//                         owner: AMM_0,
//                     }],
//                 },
//                 MockTransaction {
//                     signers: vec![SIGNER_1],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([1; 32]),
//                         owner: AMM_1,
//                     }],
//                 },
//             ],
//         };

//         // Act & Assert.
//         assert!(!is_bundle_front_run(&bundle));
//     }

//     #[test]
//     fn two_amm_front_run() {
//         // Arrange.
//         let bundle = MockBundleResult {
//             executed_ok: true,
//             transactions: vec![
//                 MockTransaction {
//                     signers: vec![SIGNER_0],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([0; 32]),
//                         owner: AMM_0,
//                     }],
//                 },
//                 MockTransaction {
//                     signers: vec![SIGNER_1],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([0; 32]),
//                         owner: AMM_0,
//                     }],
//                 },
//             ],
//         };

//         // Act & Assert.
//         assert!(is_bundle_front_run(&bundle));
//     }

//     #[test]
//     fn first_and_third_conflict_middle_not_amm() {
//         // Arrange.
//         let bundle = MockBundleResult {
//             executed_ok: true,
//             transactions: vec![
//                 MockTransaction {
//                     signers: vec![SIGNER_0],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([0; 32]),
//                         owner: AMM_0,
//                     }],
//                 },
//                 MockTransaction {
//                     signers: vec![SIGNER_1],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([1; 32]),
//                         owner: NOT_AMM_0,
//                     }],
//                 },
//                 MockTransaction {
//                     signers: vec![SIGNER_2],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([0; 32]),
//                         owner: AMM_0,
//                     }],
//                 },
//             ],
//         };

//         // Act & Assert.
//         assert!(is_bundle_front_run(&bundle));
//     }

//     #[test]
//     fn three_txs_first_and_second_same_amm_different_key() {
//         // Arrange.
//         let bundle = MockBundleResult {
//             executed_ok: true,
//             transactions: vec![
//                 MockTransaction {
//                     signers: vec![SIGNER_0],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([0; 32]),
//                         owner: AMM_0,
//                     }],
//                 },
//                 MockTransaction {
//                     signers: vec![SIGNER_1],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([1; 32]),
//                         owner: NOT_AMM_0,
//                     }],
//                 },
//                 MockTransaction {
//                     signers: vec![SIGNER_2],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([1; 32]),
//                         owner: AMM_0,
//                     }],
//                 },
//             ],
//         };

//         // Act & Assert.
//         assert!(!is_bundle_front_run(&bundle));
//     }

//     #[test]
//     fn first_and_third_conflict_middle_amm() {
//         // Arrange.
//         let bundle = MockBundleResult {
//             executed_ok: true,
//             transactions: vec![
//                 MockTransaction {
//                     signers: vec![SIGNER_0],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([0; 32]),
//                         owner: AMM_0,
//                     }],
//                 },
//                 MockTransaction {
//                     signers: vec![SIGNER_1],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([1; 32]),
//                         owner: AMM_1,
//                     }],
//                 },
//                 MockTransaction {
//                     signers: vec![SIGNER_2],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([0; 32]),
//                         owner: AMM_0,
//                     }],
//                 },
//             ],
//         };

//         // Act & Assert.
//         assert!(is_bundle_front_run(&bundle));
//     }

//     #[test]
//     fn three_different_amms() {
//         // Arrange.
//         let bundle = MockBundleResult {
//             executed_ok: true,
//             transactions: vec![
//                 MockTransaction {
//                     signers: vec![SIGNER_0],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([0; 32]),
//                         owner: AMM_0,
//                     }],
//                 },
//                 MockTransaction {
//                     signers: vec![SIGNER_1],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([1; 32]),
//                         owner: AMM_1,
//                     }],
//                 },
//                 MockTransaction {
//                     signers: vec![SIGNER_2],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([2; 32]),
//                         owner: AMM_2,
//                     }],
//                 },
//             ],
//         };

//         // Act & Assert.
//         assert!(!is_bundle_front_run(&bundle));
//     }

//     #[test]
//     fn three_same_amms_different_accounts() {
//         // Arrange.
//         let bundle = MockBundleResult {
//             executed_ok: true,
//             transactions: vec![
//                 MockTransaction {
//                     signers: vec![SIGNER_0],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([0; 32]),
//                         owner: AMM_0,
//                     }],
//                 },
//                 MockTransaction {
//                     signers: vec![SIGNER_1],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([1; 32]),
//                         owner: AMM_0,
//                     }],
//                 },
//                 MockTransaction {
//                     signers: vec![SIGNER_2],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([2; 32]),
//                         owner: AMM_0,
//                     }],
//                 },
//             ],
//         };

//         // Act & Assert.
//         assert!(!is_bundle_front_run(&bundle));
//     }

//     #[test]
//     fn two_amms_same_signer() {
//         // Arrange.
//         let bundle = MockBundleResult {
//             executed_ok: true,
//             transactions: vec![
//                 MockTransaction {
//                     signers: vec![SIGNER_0],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([0; 32]),
//                         owner: AMM_0,
//                     }],
//                 },
//                 MockTransaction {
//                     signers: vec![SIGNER_0],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([0; 32]),
//                         owner: AMM_0,
//                     }],
//                 },
//             ],
//         };

//         // Act & Assert.
//         assert!(!is_bundle_front_run(&bundle));
//     }

//     #[test]
//     fn overlapping_non_amms_different_signers() {
//         // Arrange.
//         let bundle = MockBundleResult {
//             executed_ok: true,
//             transactions: vec![
//                 MockTransaction {
//                     signers: vec![SIGNER_0],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([0; 32]),
//                         owner: NOT_AMM_0,
//                     }],
//                 },
//                 MockTransaction {
//                     signers: vec![SIGNER_1],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([0; 32]),
//                         owner: NOT_AMM_0,
//                     }],
//                 },
//             ],
//         };

//         // Act & Assert.
//         assert!(!is_bundle_front_run(&bundle));
//     }

//     #[test]
//     fn non_overlapping_non_amms_different_signers() {
//         // Arrange.
//         let bundle = MockBundleResult {
//             executed_ok: true,
//             transactions: vec![
//                 MockTransaction {
//                     signers: vec![SIGNER_0],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([0; 32]),
//                         owner: NOT_AMM_0,
//                     }],
//                 },
//                 MockTransaction {
//                     signers: vec![SIGNER_1],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([1; 32]),
//                         owner: NOT_AMM_1,
//                     }],
//                 },
//             ],
//         };

//         // Act & Assert.
//         assert!(!is_bundle_front_run(&bundle));
//     }

//     #[test]
//     fn sandwich_bundle_simple() {
//         // Arrange.
//         let bundle = MockBundleResult {
//             executed_ok: true,
//             transactions: vec![
//                 MockTransaction {
//                     signers: vec![SIGNER_0],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([0; 32]),
//                         owner: AMM_0,
//                     }],
//                 },
//                 MockTransaction {
//                     signers: vec![SIGNER_1],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([0; 32]),
//                         owner: AMM_0,
//                     }],
//                 },
//                 MockTransaction {
//                     signers: vec![SIGNER_0],
//                     accounts: vec![MockAccount {
//                         key: Pubkey::new_from_array([0; 32]),
//                         owner: AMM_0,
//                     }],
//                 },
//             ],
//         };

//         // Act & Assert.
//         assert!(is_bundle_front_run(&bundle));
//     }

//     #[test]
//     fn sandwich_bundle_complex() {
//         // Arrange.
//         let bundle = MockBundleResult {
//             executed_ok: true,
//             transactions: vec![
//                 MockTransaction {
//                     signers: vec![SIGNER_0],
//                     accounts: vec![
//                         MockAccount {
//                             key: Pubkey::new_from_array([20; 32]),
//                             owner: NOT_AMM_0,
//                         },
//                         MockAccount {
//                             key: Pubkey::new_from_array([21; 32]),
//                             owner: NOT_AMM_0,
//                         },
//                     ],
//                 },
//                 MockTransaction {
//                     signers: vec![SIGNER_0],
//                     accounts: vec![
//                         MockAccount {
//                             key: Pubkey::new_from_array([10; 32]),
//                             owner: NOT_AMM_0,
//                         },
//                         MockAccount {
//                             key: Pubkey::new_from_array([0; 32]),
//                             owner: AMM_0,
//                         },
//                         MockAccount {
//                             key: Pubkey::new_from_array([11; 32]),
//                             owner: NOT_AMM_0,
//                         },
//                     ],
//                 },
//                 MockTransaction {
//                     signers: vec![SIGNER_1],
//                     accounts: vec![
//                         MockAccount {
//                             key: Pubkey::new_from_array([12; 32]),
//                             owner: NOT_AMM_0,
//                         },
//                         MockAccount {
//                             key: Pubkey::new_from_array([13; 32]),
//                             owner: NOT_AMM_1,
//                         },
//                         MockAccount {
//                             key: Pubkey::new_from_array([0; 32]),
//                             owner: AMM_0,
//                         },
//                     ],
//                 },
//                 MockTransaction {
//                     signers: vec![SIGNER_0],
//                     accounts: vec![
//                         MockAccount {
//                             key: Pubkey::new_from_array([0; 32]),
//                             owner: AMM_0,
//                         },
//                         MockAccount {
//                             key: Pubkey::new_from_array([14; 32]),
//                             owner: NOT_AMM_0,
//                         },
//                         MockAccount {
//                             key: Pubkey::new_from_array([15; 32]),
//                             owner: NOT_AMM_1,
//                         },``
//                     ],
//                 },
//             ],
//         };

//         // Act & Assert.
//         assert!(is_bundle_front_run(&bundle));
//     }
// }
