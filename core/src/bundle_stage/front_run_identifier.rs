use {
    solana_account::ReadableAccount,
    solana_pubkey::Pubkey,
    solana_sdk::transaction::VersionedTransaction,
    solana_svm::{
        account_loader::LoadedTransaction,
        transaction_processing_result::{
            ProcessedTransaction, TransactionProcessingResultExtensions,
        },
    },
    solana_transaction::{sanitized::MAX_TX_ACCOUNT_LOCKS, TransactionError},
    std::{cell::RefCell, collections::HashMap},
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

    // println!(
    //     "AMM Map: {}",
    //     AMM_MAP.with_borrow(|map| format!("{:#?}", map))
    // );
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
                        // println!("Found overlap between TX {} and TX {}", i, j);
                        overlap_matrix[i][j] = true;
                        overlap_matrix[j][i] = true;
                    }
                }
            }
        }
    });

    // println!("Overlap matrix: {:#?}", overlap_matrix);
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
                    // println!("Comparing TX {} and TX {}", i, j);
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
fn is_executed_ok(
    processing_results: &Vec<Result<ProcessedTransaction, TransactionError>>,
) -> bool {
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use solana_account::AccountSharedData;
    use solana_message::{Message, MessageHeader};
    use solana_svm::transaction_execution_result::{
        ExecutedTransaction, TransactionExecutionDetails,
    };

    use super::*;

    const NOT_AMM_0: Pubkey = Pubkey::new_from_array([1; 32]);
    const NOT_AMM_1: Pubkey = Pubkey::new_from_array([2; 32]);
    const AMM_0: Pubkey = AMM_PROGRAMS[0];
    const AMM_1: Pubkey = AMM_PROGRAMS[1];
    const AMM_2: Pubkey = AMM_PROGRAMS[2];
    const SIGNER_0: Pubkey = Pubkey::new_from_array([3; 32]);
    const SIGNER_1: Pubkey = Pubkey::new_from_array([4; 32]);
    const SIGNER_2: Pubkey = Pubkey::new_from_array([5; 32]);

    struct MockAccount {
        key: Pubkey,
        owner: Pubkey,
    }

    struct MockTransaction {
        signers: Vec<Pubkey>,
        accounts: Vec<MockAccount>,
    }

    struct MockData {
        txs: Vec<VersionedTransaction>,
        output: Vec<Result<ProcessedTransaction, TransactionError>>,
    }

    impl MockData {
        fn new(mock_txs: Vec<MockTransaction>) -> MockData {
            let mut txs = vec![];
            let mut output = vec![];

            for tx in mock_txs.iter() {
                let mut v_tx = VersionedTransaction::default();

                let mut num_required_signatures = 0;
                let mut account_keys = vec![];
                let mut accounts = vec![];

                for signer in tx.signers.iter() {
                    v_tx.signatures
                        .push(solana_sdk::signature::Signature::default());
                    num_required_signatures += 1;

                    account_keys.push(signer.clone());
                    accounts.push((
                        signer.clone(),
                        AccountSharedData::from(solana_account::Account::new(
                            0,
                            0,
                            &Pubkey::default(),
                        )),
                    ))
                }

                for account in tx.accounts.iter() {
                    account_keys.push(account.key.clone());
                    accounts.push((
                        account.key.clone(),
                        AccountSharedData::from(solana_account::Account::new(0, 0, &account.owner)),
                    ))
                }

                let msg = Message {
                    header: MessageHeader {
                        num_required_signatures,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 0,
                    },
                    account_keys,
                    ..Default::default()
                };
                v_tx.message = solana_sdk::message::VersionedMessage::Legacy(msg);

                let mut loaded_transaction = LoadedTransaction::default();
                loaded_transaction.accounts = accounts;

                let execution_details = TransactionExecutionDetails {
                    status: Ok(()),
                    log_messages: None,
                    inner_instructions: None,
                    return_data: None,
                    executed_units: 1,
                    accounts_data_len_delta: 1,
                };

                let p_tx = ProcessedTransaction::Executed(Box::new(ExecutedTransaction {
                    loaded_transaction,
                    execution_details,
                    programs_modified_by_tx: HashMap::new(),
                }));

                txs.push(v_tx);
                output.push(Ok(p_tx));
            }

            MockData { txs, output }
        }
    }

    #[test]
    fn two_amm_no_front_run() {
        // Arrange.
        let MockData { txs, output } = MockData::new(vec![
            MockTransaction {
                signers: vec![SIGNER_0],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([0; 32]),
                    owner: AMM_0,
                }],
            },
            MockTransaction {
                signers: vec![SIGNER_1],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([1; 32]),
                    owner: AMM_1,
                }],
            },
        ]);

        // Act & Assert.
        assert!(!is_bundle_front_run(&txs, &output));
    }

    #[test]
    fn two_amm_front_run() {
        // Arrange.
        let MockData { txs, output } = MockData::new(vec![
            MockTransaction {
                signers: vec![SIGNER_0],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([0; 32]),
                    owner: AMM_0,
                }],
            },
            MockTransaction {
                signers: vec![SIGNER_1],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([0; 32]),
                    owner: AMM_0,
                }],
            },
        ]);

        // Act & Assert.
        assert!(is_bundle_front_run(&txs, &output));
    }

    #[test]
    fn first_and_third_conflict_middle_not_amm() {
        // Arrange.
        let MockData { txs, output } = MockData::new(vec![
            MockTransaction {
                signers: vec![SIGNER_0],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([0; 32]),
                    owner: AMM_0,
                }],
            },
            MockTransaction {
                signers: vec![SIGNER_1],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([1; 32]),
                    owner: NOT_AMM_0,
                }],
            },
            MockTransaction {
                signers: vec![SIGNER_2],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([0; 32]),
                    owner: AMM_0,
                }],
            },
        ]);

        // Act & Assert.
        assert!(is_bundle_front_run(&txs, &output));
    }

    #[test]
    fn three_txs_first_and_second_same_amm_different_key() {
        // Arrange.
        let MockData { txs, output } = MockData::new(vec![
            MockTransaction {
                signers: vec![SIGNER_0],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([0; 32]),
                    owner: AMM_0,
                }],
            },
            MockTransaction {
                signers: vec![SIGNER_1],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([1; 32]),
                    owner: NOT_AMM_0,
                }],
            },
            MockTransaction {
                signers: vec![SIGNER_2],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([1; 32]),
                    owner: AMM_0,
                }],
            },
        ]);

        // Act & Assert.
        assert!(!is_bundle_front_run(&txs, &output));
    }

    #[test]
    fn first_and_third_conflict_middle_amm() {
        // Arrange.
        let MockData { txs, output } = MockData::new(vec![
            MockTransaction {
                signers: vec![SIGNER_0],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([0; 32]),
                    owner: AMM_0,
                }],
            },
            MockTransaction {
                signers: vec![SIGNER_1],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([1; 32]),
                    owner: AMM_1,
                }],
            },
            MockTransaction {
                signers: vec![SIGNER_2],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([0; 32]),
                    owner: AMM_0,
                }],
            },
        ]);

        // Act & Assert.
        assert!(is_bundle_front_run(&txs, &output));
    }

    #[test]
    fn three_different_amms() {
        // Arrange.
        let MockData { txs, output } = MockData::new(vec![
            MockTransaction {
                signers: vec![SIGNER_0],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([0; 32]),
                    owner: AMM_0,
                }],
            },
            MockTransaction {
                signers: vec![SIGNER_1],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([1; 32]),
                    owner: AMM_1,
                }],
            },
            MockTransaction {
                signers: vec![SIGNER_2],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([2; 32]),
                    owner: AMM_2,
                }],
            },
        ]);

        // Act & Assert.
        assert!(!is_bundle_front_run(&txs, &output));
    }

    #[test]
    fn three_same_amms_different_accounts() {
        // Arrange.
        let MockData { txs, output } = MockData::new(vec![
            MockTransaction {
                signers: vec![SIGNER_0],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([0; 32]),
                    owner: AMM_0,
                }],
            },
            MockTransaction {
                signers: vec![SIGNER_1],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([1; 32]),
                    owner: AMM_0,
                }],
            },
            MockTransaction {
                signers: vec![SIGNER_2],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([2; 32]),
                    owner: AMM_0,
                }],
            },
        ]);

        // Act & Assert.
        assert!(!is_bundle_front_run(&txs, &output));
    }

    #[test]
    fn two_amms_same_signer() {
        // Arrange.
        let MockData { txs, output } = MockData::new(vec![
            MockTransaction {
                signers: vec![SIGNER_0],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([0; 32]),
                    owner: AMM_0,
                }],
            },
            MockTransaction {
                signers: vec![SIGNER_0],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([0; 32]),
                    owner: AMM_0,
                }],
            },
        ]);

        // Act & Assert.
        assert!(!is_bundle_front_run(&txs, &output));
    }

    #[test]
    fn overlapping_non_amms_different_signers() {
        // Arrange.
        let MockData { txs, output } = MockData::new(vec![
            MockTransaction {
                signers: vec![SIGNER_0],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([0; 32]),
                    owner: NOT_AMM_0,
                }],
            },
            MockTransaction {
                signers: vec![SIGNER_1],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([0; 32]),
                    owner: NOT_AMM_0,
                }],
            },
        ]);

        // Act & Assert.
        assert!(!is_bundle_front_run(&txs, &output));
    }

    #[test]
    fn non_overlapping_non_amms_different_signers() {
        // Arrange.
        let MockData { txs, output } = MockData::new(vec![
            MockTransaction {
                signers: vec![SIGNER_0],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([0; 32]),
                    owner: NOT_AMM_0,
                }],
            },
            MockTransaction {
                signers: vec![SIGNER_1],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([1; 32]),
                    owner: NOT_AMM_1,
                }],
            },
        ]);

        // Act & Assert.
        assert!(!is_bundle_front_run(&txs, &output));
    }

    #[test]
    fn sandwich_bundle_simple() {
        // Arrange.
        let MockData { txs, output } = MockData::new(vec![
            MockTransaction {
                signers: vec![SIGNER_0],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([0; 32]),
                    owner: AMM_0,
                }],
            },
            MockTransaction {
                signers: vec![SIGNER_1],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([0; 32]),
                    owner: AMM_0,
                }],
            },
            MockTransaction {
                signers: vec![SIGNER_0],
                accounts: vec![MockAccount {
                    key: Pubkey::new_from_array([0; 32]),
                    owner: AMM_0,
                }],
            },
        ]);

        // Act & Assert.
        assert!(is_bundle_front_run(&txs, &output));
    }

    #[test]
    fn sandwich_bundle_complex() {
        // Arrange.
        let MockData { txs, output } = MockData::new(vec![
            MockTransaction {
                signers: vec![SIGNER_0],
                accounts: vec![
                    MockAccount {
                        key: Pubkey::new_from_array([20; 32]),
                        owner: NOT_AMM_0,
                    },
                    MockAccount {
                        key: Pubkey::new_from_array([21; 32]),
                        owner: NOT_AMM_0,
                    },
                ],
            },
            MockTransaction {
                signers: vec![SIGNER_1],
                accounts: vec![
                    MockAccount {
                        key: Pubkey::new_from_array([10; 32]),
                        owner: NOT_AMM_0,
                    },
                    MockAccount {
                        key: Pubkey::new_from_array([0; 32]),
                        owner: AMM_0,
                    },
                    MockAccount {
                        key: Pubkey::new_from_array([11; 32]),
                        owner: NOT_AMM_0,
                    },
                ],
            },
            MockTransaction {
                signers: vec![SIGNER_1],
                accounts: vec![
                    MockAccount {
                        key: Pubkey::new_from_array([12; 32]),
                        owner: NOT_AMM_0,
                    },
                    MockAccount {
                        key: Pubkey::new_from_array([13; 32]),
                        owner: NOT_AMM_1,
                    },
                    MockAccount {
                        key: Pubkey::new_from_array([0; 32]),
                        owner: AMM_0,
                    },
                ],
            },
            MockTransaction {
                signers: vec![SIGNER_0],
                accounts: vec![
                    MockAccount {
                        key: Pubkey::new_from_array([0; 32]),
                        owner: AMM_0,
                    },
                    MockAccount {
                        key: Pubkey::new_from_array([14; 32]),
                        owner: NOT_AMM_0,
                    },
                    MockAccount {
                        key: Pubkey::new_from_array([15; 32]),
                        owner: NOT_AMM_1,
                    },
                ],
            },
        ]);

        // let MockData { txs, output } = MockData::new(vec![
        //     MockTransaction {
        //         signers: vec![SIGNER_0],
        //         accounts: vec![
        //             MockAccount {
        //                 key: Pubkey::new_from_array([20; 32]),
        //                 owner: NOT_AMM_0,
        //             },
        //             MockAccount {
        //                 key: Pubkey::new_from_array([21; 32]),
        //                 owner: NOT_AMM_0,
        //             },
        //         ],
        //     },
        //     MockTransaction {
        //         signers: vec![SIGNER_0],
        //         accounts: vec![
        //             MockAccount {
        //                 key: Pubkey::new_from_array([10; 32]),
        //                 owner: NOT_AMM_0,
        //             },
        //             MockAccount {
        //                 key: Pubkey::new_from_array([0; 32]),
        //                 owner: AMM_0,
        //             },
        //             MockAccount {
        //                 key: Pubkey::new_from_array([11; 32]),
        //                 owner: NOT_AMM_0,
        //             },
        //         ],
        //     },
        //     MockTransaction {
        //         signers: vec![SIGNER_1],
        //         accounts: vec![
        //             MockAccount {
        //                 key: Pubkey::new_from_array([12; 32]),
        //                 owner: NOT_AMM_0,
        //             },
        //             MockAccount {
        //                 key: Pubkey::new_from_array([13; 32]),
        //                 owner: NOT_AMM_1,
        //             },
        //             MockAccount {
        //                 key: Pubkey::new_from_array([0; 32]),
        //                 owner: AMM_0,
        //             },
        //         ],
        //     },
        //     MockTransaction {
        //         signers: vec![SIGNER_0],
        //         accounts: vec![
        //             MockAccount {
        //                 key: Pubkey::new_from_array([0; 32]),
        //                 owner: AMM_0,
        //             },
        //             MockAccount {
        //                 key: Pubkey::new_from_array([14; 32]),
        //                 owner: NOT_AMM_0,
        //             },
        //             MockAccount {
        //                 key: Pubkey::new_from_array([15; 32]),
        //                 owner: NOT_AMM_1,
        //             },
        //         ],
        //     },
        // ]);

        // Act & Assert.
        assert!(is_bundle_front_run(&txs, &output));
    }
}
