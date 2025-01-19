use {
    super::MAX_PACKETS_PER_BUNDLE,
    hashbrown::HashMap,
    itertools::izip,
    solana_bundle::bundle_execution::LoadAndExecuteBundleOutput,
    solana_sdk::{
        account::ReadableAccount,
        pubkey::Pubkey,
        transaction::{SanitizedTransaction, MAX_TX_ACCOUNT_LOCKS},
    },
    solana_svm::account_loader::LoadedTransaction,
    std::cell::RefCell,
};

pub(crate) const AMM_PROGRAMS: &[Pubkey] = &[
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
    solana_sdk::pubkey!("CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"), // Raydium CPMM
    solana_sdk::pubkey!("PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY"),  // Phoenix
    solana_sdk::pubkey!("opnb2LAfJYbRMAHHvqjCwQxanZn7ReEHp1k81EohpZb"),  // Open Book
    solana_sdk::pubkey!("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"),  // Pump.fun
];

thread_local! {
    static AMM_MAP: RefCell<HashMap<Pubkey, [bool; MAX_PACKETS_PER_BUNDLE]>>
        = RefCell::new(HashMap::with_capacity(MAX_TX_ACCOUNT_LOCKS * MAX_PACKETS_PER_BUNDLE));
}

#[must_use]
pub(crate) fn is_bundle_front_run<'a>(bundle: &'a impl BundleResult<'a>) -> bool {
    AMM_MAP.with_borrow_mut(|map| map.clear());

    if !bundle.executed_ok() {
        eprintln!("BUG: Unexpected bundle with ERR");
        return false;
    }

    // If the bundle did not execute okay then the results will not be valid.
    let count = bundle.transactions().count();
    if count <= 1 {
        return false;
    }

    if count > MAX_PACKETS_PER_BUNDLE {
        eprintln!("BUG: Too many packets in bundle; packets={count}");
        return false;
    }

    // Check all TXs for write-locked pool accounts.
    for (i, tx) in bundle.transactions().enumerate() {
        // Find all the AMM owned writeable accounts.
        let writeable_amm_accounts = tx
            .writable_accounts_owners()
            .filter(|account| AMM_PROGRAMS.contains(account.owner));

        // Record this TX's access of the writeable account.
        for account in writeable_amm_accounts {
            AMM_MAP.with_borrow_mut(|map| map.entry_ref(account.key).or_default()[i] = true);
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
                let Some(i) = bundle.transactions().nth(i) else {
                    eprintln!("BUG: i not found");
                    return false;
                };
                let Some(j) = bundle.transactions().nth(j) else {
                    eprintln!("BUG: j not found");
                    return false;
                };

                // Brute force compare the signers.
                for (i, j) in i.signers().flat_map(|i| j.signers().map(move |j| (i, j))) {
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

pub(crate) trait BundleResult<'a> {
    type Transaction: BundleTransaction;

    fn executed_ok(&self) -> bool;
    fn transactions(&'a self) -> impl Iterator<Item = Self::Transaction>;
}

pub(crate) trait BundleTransaction {
    /// Iterator over each signer.
    fn signers(&self) -> impl Iterator<Item = &Pubkey>;
    /// Iterator over each loaded account's owner.
    fn writable_accounts_owners(&self) -> impl Iterator<Item = AccountRef>;
}

pub(crate) struct AccountRef<'a> {
    key: &'a Pubkey,
    owner: &'a Pubkey,
}

impl<'a> BundleResult<'a> for LoadAndExecuteBundleOutput<'a> {
    type Transaction = (&'a SanitizedTransaction, &'a LoadedTransaction);

    fn executed_ok(&self) -> bool {
        self.executed_ok()
    }

    fn transactions(&'a self) -> impl Iterator<Item = Self::Transaction> {
        self.bundle_transaction_results().iter().flat_map(|batch| {
            let output = batch.load_and_execute_transactions_output();
            if batch.transactions().len() != output.loaded_transactions.len()
                || batch.transactions().len() != output.execution_results.len()
            {
                eprintln!("BUG: Invalid assumption about batch layout");
            }

            izip!(
                batch.transactions(),
                &output.loaded_transactions,
                &output.execution_results,
            )
            .filter_map(|(sanitized, loaded, exec)| {
                match (loaded, exec.was_executed_successfully()) {
                    (Ok(loaded), true) => Some((sanitized, loaded)),
                    (Err(_), _) | (Ok(_), false) => None,
                }
            })
        })
    }
}

impl<'a> BundleTransaction for (&'a SanitizedTransaction, &'a LoadedTransaction) {
    fn signers(&self) -> impl Iterator<Item = &Pubkey> {
        self.0
            .message()
            .account_keys()
            .iter()
            .take(self.0.message().num_signatures() as usize)
    }

    fn writable_accounts_owners(&self) -> impl Iterator<Item = AccountRef> {
        self.0
            .message()
            .account_keys()
            .iter()
            .enumerate()
            .filter(|(i, _)| self.0.message().is_writable(*i))
            .map(|(i, key)| AccountRef {
                key,
                owner: self.1.accounts[i].1.owner(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOT_AMM_0: Pubkey = Pubkey::new_from_array([1; 32]);
    const NOT_AMM_1: Pubkey = Pubkey::new_from_array([2; 32]);
    const AMM_0: Pubkey = AMM_PROGRAMS[0];
    const AMM_1: Pubkey = AMM_PROGRAMS[1];
    const AMM_2: Pubkey = AMM_PROGRAMS[2];
    const SIGNER_0: Pubkey = Pubkey::new_from_array([3; 32]);
    const SIGNER_1: Pubkey = Pubkey::new_from_array([4; 32]);
    const SIGNER_2: Pubkey = Pubkey::new_from_array([5; 32]);

    struct MockBundleResult {
        executed_ok: bool,
        transactions: Vec<MockTransaction>,
    }

    impl<'a> BundleResult<'a> for MockBundleResult {
        type Transaction = &'a MockTransaction;

        fn executed_ok(&self) -> bool {
            self.executed_ok
        }

        fn transactions(&'a self) -> impl Iterator<Item = Self::Transaction> {
            self.transactions.iter()
        }
    }

    struct MockTransaction {
        signers: Vec<Pubkey>,
        accounts: Vec<MockAccount>,
    }

    impl<'a> BundleTransaction for &'a MockTransaction {
        fn signers(&self) -> impl Iterator<Item = &Pubkey> {
            self.signers.iter()
        }

        fn writable_accounts_owners(&self) -> impl Iterator<Item = AccountRef> {
            self.accounts
                .iter()
                .map(|MockAccount { key, owner }| AccountRef { key, owner })
        }
    }

    struct MockAccount {
        key: Pubkey,
        owner: Pubkey,
    }

    #[test]
    fn two_amm_no_front_run() {
        // Arrange.
        let bundle = MockBundleResult {
            executed_ok: true,
            transactions: vec![
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
            ],
        };

        // Act & Assert.
        assert!(!is_bundle_front_run(&bundle));
    }

    #[test]
    fn two_amm_front_run() {
        // Arrange.
        let bundle = MockBundleResult {
            executed_ok: true,
            transactions: vec![
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
            ],
        };

        // Act & Assert.
        assert!(is_bundle_front_run(&bundle));
    }

    #[test]
    fn first_and_third_conflict_middle_not_amm() {
        // Arrange.
        let bundle = MockBundleResult {
            executed_ok: true,
            transactions: vec![
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
            ],
        };

        // Act & Assert.
        assert!(is_bundle_front_run(&bundle));
    }

    #[test]
    fn three_txs_first_and_second_same_amm_different_key() {
        // Arrange.
        let bundle = MockBundleResult {
            executed_ok: true,
            transactions: vec![
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
            ],
        };

        // Act & Assert.
        assert!(!is_bundle_front_run(&bundle));
    }

    #[test]
    fn first_and_third_conflict_middle_amm() {
        // Arrange.
        let bundle = MockBundleResult {
            executed_ok: true,
            transactions: vec![
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
            ],
        };

        // Act & Assert.
        assert!(is_bundle_front_run(&bundle));
    }

    #[test]
    fn three_different_amms() {
        // Arrange.
        let bundle = MockBundleResult {
            executed_ok: true,
            transactions: vec![
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
            ],
        };

        // Act & Assert.
        assert!(!is_bundle_front_run(&bundle));
    }

    #[test]
    fn three_same_amms_different_accounts() {
        // Arrange.
        let bundle = MockBundleResult {
            executed_ok: true,
            transactions: vec![
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
            ],
        };

        // Act & Assert.
        assert!(!is_bundle_front_run(&bundle));
    }

    #[test]
    fn two_amms_same_signer() {
        // Arrange.
        let bundle = MockBundleResult {
            executed_ok: true,
            transactions: vec![
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
            ],
        };

        // Act & Assert.
        assert!(!is_bundle_front_run(&bundle));
    }

    #[test]
    fn overlapping_non_amms_different_signers() {
        // Arrange.
        let bundle = MockBundleResult {
            executed_ok: true,
            transactions: vec![
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
            ],
        };

        // Act & Assert.
        assert!(!is_bundle_front_run(&bundle));
    }

    #[test]
    fn non_overlapping_non_amms_different_signers() {
        // Arrange.
        let bundle = MockBundleResult {
            executed_ok: true,
            transactions: vec![
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
            ],
        };

        // Act & Assert.
        assert!(!is_bundle_front_run(&bundle));
    }

    #[test]
    fn sandwich_bundle_simple() {
        // Arrange.
        let bundle = MockBundleResult {
            executed_ok: true,
            transactions: vec![
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
            ],
        };

        // Act & Assert.
        assert!(is_bundle_front_run(&bundle));
    }

    #[test]
    fn sandwich_bundle_complex() {
        // Arrange.
        let bundle = MockBundleResult {
            executed_ok: true,
            transactions: vec![
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
                    signers: vec![SIGNER_0],
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
            ],
        };

        // Act & Assert.
        assert!(is_bundle_front_run(&bundle));
    }
}
