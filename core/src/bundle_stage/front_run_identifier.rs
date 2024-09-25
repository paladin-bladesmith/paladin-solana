use {
    super::MAX_PACKETS_PER_BUNDLE,
    hashbrown::HashMap,
    solana_bundle::bundle_execution::LoadAndExecuteBundleOutput,
    solana_sdk::{
        pubkey::Pubkey,
        transaction::{SanitizedTransaction, MAX_TX_ACCOUNT_LOCKS},
    },
    std::cell::RefCell,
};

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
    static AMM_MAP: RefCell<HashMap<&'static Pubkey, [bool; MAX_PACKETS_PER_BUNDLE]>> = RefCell::new(HashMap::with_capacity(MAX_TX_ACCOUNT_LOCKS * MAX_PACKETS_PER_BUNDLE));
}

#[must_use]
pub(crate) fn bundle_is_front_run(bundle: &impl BundleResult) -> bool {
    // NB: Clear just in case somehow we broke the control flow (perhaps a future patch) and forgot
    // to clear before return.
    AMM_MAP.with(|map| map.borrow_mut().clear());

    // Early return on failed/single TX bundles.
    let tx_count = bundle.transactions().count();
    if !bundle.executed_ok() || tx_count <= 1 {
        return false;
    }

    // Check all TXs for write-locked pool accounts.
    for (i, tx) in bundle.transactions().enumerate() {
        // Find all the AMM owned writeable accounts.
        let writeable_amm_accounts = tx
            .writable_account_owners()
            .filter(|owner| AMM_PROGRAMS.contains(owner));

        // Record this TX's access of the writeable account.
        for key in writeable_amm_accounts {
            let key = unsafe { std::mem::transmute::<&Pubkey, &'static Pubkey>(key) };

            AMM_MAP.with_borrow_mut(|map| map.entry(key).or_default()[i] = true);
        }
    }

    // Compute which TXs overlap with each other.
    let mut overlap_matrix = [[false; MAX_PACKETS_PER_BUNDLE]; MAX_PACKETS_PER_BUNDLE];
    AMM_MAP.with_borrow(|map| {
        for (key, txs) in map {
            println!("{key:?}: {txs:?}");

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
                    error!("BUG");
                    return false;
                };
                let Some(j) = bundle.transactions().nth(j) else {
                    error!("BUG");
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

                // NB: We MUST clear this hashmap else it will be full of pointers to
                // de-allocated memory (whenever the bundle struct gets dropped).
                AMM_MAP.with_borrow_mut(|map| map.clear());
                return true;
            }
        }
    }

    // NB: We MUST clear this hashmap else it will be full of pointers to de-allocated
    // memory (whenever the bundle struct gets dropped).
    AMM_MAP.with_borrow_mut(|map| map.clear());

    false
}

pub(crate) trait BundleResult {
    type Transaction: BundleTransaction;

    fn executed_ok(&self) -> bool;
    fn transactions(&self) -> impl Iterator<Item = &Self::Transaction>;
}

pub(crate) trait BundleTransaction {
    /// Iterator over each signer.
    fn signers(&self) -> impl Iterator<Item = &Pubkey>;
    /// Iterator over each loaded account's owner.
    fn writable_account_owners(&self) -> impl Iterator<Item = &Pubkey>;
}

impl BundleResult for LoadAndExecuteBundleOutput<'_> {
    type Transaction = SanitizedTransaction;

    fn executed_ok(&self) -> bool {
        self.executed_ok()
    }

    fn transactions(&self) -> impl Iterator<Item = &Self::Transaction> {
        self.bundle_transaction_results()
            .iter()
            .flat_map(|batch| batch.transactions())
    }
}

impl BundleTransaction for SanitizedTransaction {
    fn signers(&self) -> impl Iterator<Item = &Pubkey> {
        self.message()
            .account_keys()
            .iter()
            .take(self.message().num_signatures() as usize)
    }

    fn writable_account_owners(&self) -> impl Iterator<Item = &Pubkey> {
        self.message()
            .account_keys()
            .iter()
            .enumerate()
            .filter(|(i, _)| self.message().is_writable(*i))
            .map(|(_, key)| key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOT_AMM_0: Pubkey = Pubkey::new_from_array([1; 32]);
    const NOT_AMM_1: Pubkey = Pubkey::new_from_array([2; 32]);
    const AMM_0: Pubkey = AMM_PROGRAMS[0];
    const AMM_1: Pubkey = AMM_PROGRAMS[1];
    const SIGNER_0: Pubkey = Pubkey::new_from_array([3; 32]);
    const SIGNER_1: Pubkey = Pubkey::new_from_array([4; 32]);

    struct MockBundleResult {
        executed_ok: bool,
        transactions: Vec<MockTransaction>,
    }

    impl BundleResult for MockBundleResult {
        type Transaction = MockTransaction;

        fn executed_ok(&self) -> bool {
            self.executed_ok
        }

        fn transactions(&self) -> impl Iterator<Item = &Self::Transaction> {
            self.transactions.iter()
        }
    }

    struct MockTransaction {
        signers: Vec<Pubkey>,
        writeable_account_owners: Vec<Pubkey>,
    }

    impl BundleTransaction for MockTransaction {
        fn signers(&self) -> impl Iterator<Item = &Pubkey> {
            self.signers.iter()
        }

        fn writable_account_owners(&self) -> impl Iterator<Item = &Pubkey> {
            self.writeable_account_owners.iter()
        }
    }

    #[test]
    fn two_amm_no_front_run() {
        // Arrange.
        let bundle = MockBundleResult {
            executed_ok: true,
            transactions: vec![
                MockTransaction {
                    signers: vec![SIGNER_0],
                    writeable_account_owners: vec![AMM_0],
                },
                MockTransaction {
                    signers: vec![SIGNER_1],
                    writeable_account_owners: vec![AMM_1],
                },
            ],
        };

        // Act.
        let is_frontrun = bundle_is_front_run(&bundle);

        // Assert.
        assert_eq!(is_frontrun, false);
    }

    #[test]
    fn two_amm_front_run() {
        // Arrange.
        let bundle = MockBundleResult {
            executed_ok: true,
            transactions: vec![
                MockTransaction {
                    signers: vec![SIGNER_0],
                    writeable_account_owners: vec![AMM_0],
                },
                MockTransaction {
                    signers: vec![SIGNER_1],
                    writeable_account_owners: vec![AMM_0],
                },
            ],
        };

        // Act.
        let is_frontrun = bundle_is_front_run(&bundle);

        // Assert.
        assert_eq!(is_frontrun, true);
    }
}
