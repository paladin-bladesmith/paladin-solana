use {
    crate::{
        banking_stage::{
            immutable_deserialized_packet::{DeserializedPacketError, ImmutableDeserializedPacket},
            packet_filter::PacketFilterFailure,
        },
        packet_bundle::PacketBundle,
    },
    solana_bundle::SanitizedBundle,
    solana_perf::sigverify::verify_packet,
    solana_runtime::{bank::Bank, verify_precompiles::verify_precompiles},
    solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
    solana_sdk::{
        clock::MAX_PROCESSING_AGE, hash::Hash, pubkey::Pubkey, transaction::SanitizedTransaction,
    },
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    std::{
        collections::{hash_map::RandomState, HashSet},
        iter::repeat,
    },
    thiserror::Error,
};

#[derive(Debug, Error)]
pub enum DeserializedBundleError {
    #[error("FailedToSerializePacket: {0}")]
    FailedToSerializePacket(#[from] DeserializedPacketError),

    #[error("EmptyBatch")]
    EmptyBatch,

    #[error("TooManyPackets")]
    TooManyPackets,

    #[error("MarkedDiscard")]
    MarkedDiscard,

    #[error("SignatureVerificationFailure")]
    SignatureVerificationFailure,

    #[error("Bank is in vote-only mode")]
    VoteOnlyMode,

    #[error("Bundle mentions blacklisted account")]
    BlacklistedAccount,

    #[error("Bundle contains a transaction that failed to serialize")]
    FailedToSerializeTransaction,

    #[error("Bundle contains a duplicate transaction")]
    DuplicateTransaction,

    #[error("Bundle failed check_transactions")]
    FailedCheckTransactions,

    #[error("PacketFilterFailure: {0}")]
    PacketFilterFailure(#[from] PacketFilterFailure),

    #[error("Failed to verify precompiles")]
    FailedVerifyPrecompiles,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ImmutableDeserializedBundle {
    bundle_id: String,
    packets: Vec<ImmutableDeserializedPacket>,
}

impl ImmutableDeserializedBundle {
    pub fn new(
        bundle: &mut PacketBundle,
        max_len: Option<usize>,
        packet_filter: &impl Fn(
            ImmutableDeserializedPacket,
        ) -> Result<ImmutableDeserializedPacket, PacketFilterFailure>,
    ) -> Result<Self, DeserializedBundleError> {
        // Checks: non-zero, less than some length, marked for discard, signature verification failed, failed to sanitize to
        // ImmutableDeserializedPacket
        if bundle.batch.is_empty() {
            return Err(DeserializedBundleError::EmptyBatch);
        }
        if max_len
            .map(|max_len| bundle.batch.len() > max_len)
            .unwrap_or(false)
        {
            return Err(DeserializedBundleError::TooManyPackets);
        }
        if bundle.batch.iter().any(|p| p.meta().discard()) {
            return Err(DeserializedBundleError::MarkedDiscard);
        }
        if bundle.batch.iter_mut().any(|p| !verify_packet(p, false)) {
            return Err(DeserializedBundleError::SignatureVerificationFailure);
        }

        let mut immutable_packets = Vec::with_capacity(bundle.batch.len());
        for packet in bundle.batch.iter() {
            let immutable_packet = ImmutableDeserializedPacket::new(packet.clone())?;
            let immutable_packet = packet_filter(immutable_packet)?;
            immutable_packets.push(immutable_packet);
        }

        Ok(Self {
            bundle_id: bundle.bundle_id.clone(),
            packets: immutable_packets,
        })
    }

    pub fn packets(&self) -> &[ImmutableDeserializedPacket] {
        &self.packets
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.packets.len()
    }

    pub fn bundle_id(&self) -> &str {
        &self.bundle_id
    }

    /// A bundle has the following requirements:
    /// - all transactions must be sanitiz-able
    /// - no duplicate signatures
    /// - must not contain a blacklisted account
    /// - can't already be processed or contain a bad blockhash
    pub fn build_sanitized_bundle(
        &self,
        bank: &Bank,
        blacklisted_accounts: &HashSet<Pubkey>,
        transaction_error_metrics: &mut TransactionErrorMetrics,
        move_precompile_verification_to_svm: bool,
    ) -> Result<SanitizedBundle, DeserializedBundleError> {
        if bank.vote_only_bank() {
            return Err(DeserializedBundleError::VoteOnlyMode);
        }

        let transactions: Vec<RuntimeTransaction<SanitizedTransaction>> = self
            .packets
            .iter()
            .filter_map(|p| {
                p.build_sanitized_transaction(
                    bank.vote_only_bank(),
                    bank,
                    bank.get_reserved_account_keys(),
                )
                .map(|(tx, _)| tx)
            })
            .collect();

        if self.packets.len() != transactions.len() {
            return Err(DeserializedBundleError::FailedToSerializeTransaction);
        }

        let unique_messages: HashSet<&Hash, RandomState> =
            HashSet::from_iter(transactions.iter().map(|tx| tx.message_hash()));
        if unique_messages.len() != transactions.len() {
            return Err(DeserializedBundleError::DuplicateTransaction);
        }

        let contains_blacklisted_account = transactions.iter().any(|tx| {
            tx.message()
                .account_keys()
                .iter()
                .any(|acc| blacklisted_accounts.contains(acc))
        });

        if contains_blacklisted_account {
            return Err(DeserializedBundleError::BlacklistedAccount);
        }

        // assume everything locks okay to check for already-processed transaction or expired/invalid blockhash
        let lock_results: Vec<_> = repeat(Ok(())).take(transactions.len()).collect();
        let check_results = bank.check_transactions(
            &transactions,
            &lock_results,
            MAX_PROCESSING_AGE,
            transaction_error_metrics,
        );

        if check_results.iter().any(|r| r.is_err()) {
            return Err(DeserializedBundleError::FailedCheckTransactions);
        }

        if !move_precompile_verification_to_svm {
            for tx in &transactions {
                verify_precompiles(tx, &bank.feature_set)
                    .map_err(|_| DeserializedBundleError::FailedVerifyPrecompiles)?;
            }
        }

        Ok(SanitizedBundle {
            transactions,
            bundle_id: self.bundle_id.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            immutable_deserialized_bundle::{DeserializedBundleError, ImmutableDeserializedBundle},
            packet_bundle::PacketBundle,
        },
        solana_client::rpc_client::SerializableTransaction,
        solana_ledger::genesis_utils::create_genesis_config,
        solana_perf::packet::PacketBatch,
        solana_runtime::{
            bank::{Bank, NewBankOptions},
            genesis_utils::GenesisConfigInfo,
        },
        solana_sdk::{
            hash::Hash,
            instruction::Instruction,
            packet::Packet,
            pubkey::Pubkey,
            signature::{Keypair, Signer},
            system_transaction::transfer,
            transaction::Transaction,
        },
        solana_svm::transaction_error_metrics::TransactionErrorMetrics,
        std::{collections::HashSet, sync::Arc},
    };

    /// Happy case
    #[test]
    fn test_simple_get_sanitized_bundle() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (bank, _) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let kp = Keypair::new();

        let tx0 = transfer(&mint_keypair, &kp.pubkey(), 500, genesis_config.hash());

        let tx1 = transfer(&mint_keypair, &kp.pubkey(), 501, genesis_config.hash());

        let bundle = ImmutableDeserializedBundle::new(
            &mut PacketBundle {
                batch: PacketBatch::new(vec![
                    Packet::from_data(None, &tx0).unwrap(),
                    Packet::from_data(None, &tx1).unwrap(),
                ]),
                bundle_id: String::default(),
            },
            None,
            &Ok,
        )
        .unwrap();

        let mut transaction_errors = TransactionErrorMetrics::default();
        let sanitized_bundle = bundle
            .build_sanitized_bundle(&bank, &HashSet::default(), &mut transaction_errors, false)
            .unwrap();
        assert_eq!(sanitized_bundle.transactions.len(), 2);
        assert_eq!(
            sanitized_bundle.transactions[0].signature(),
            tx0.get_signature()
        );
        assert_eq!(
            sanitized_bundle.transactions[1].signature(),
            tx1.get_signature()
        );
    }

    #[test]
    fn test_empty_batch_fails_to_init() {
        assert_matches!(
            ImmutableDeserializedBundle::new(
                &mut PacketBundle {
                    batch: PacketBatch::new(vec![]),
                    bundle_id: String::default(),
                },
                None,
                &Ok
            ),
            Err(DeserializedBundleError::EmptyBatch)
        );
    }

    #[test]
    fn test_too_many_packets_fails_to_init() {
        let kp = Keypair::new();

        assert_matches!(
            ImmutableDeserializedBundle::new(
                &mut PacketBundle {
                    batch: PacketBatch::new(
                        (0..10)
                            .map(|i| {
                                Packet::from_data(
                                    None,
                                    transfer(&kp, &kp.pubkey(), i, Hash::default()),
                                )
                                .unwrap()
                            })
                            .collect()
                    ),
                    bundle_id: String::default(),
                },
                Some(5),
                &Ok
            ),
            Err(DeserializedBundleError::TooManyPackets)
        );
    }

    #[test]
    fn test_packets_marked_discard_fails_to_init() {
        let kp = Keypair::new();

        let mut packet =
            Packet::from_data(None, transfer(&kp, &kp.pubkey(), 100, Hash::default())).unwrap();
        packet.meta_mut().set_discard(true);

        assert_matches!(
            ImmutableDeserializedBundle::new(
                &mut PacketBundle {
                    batch: PacketBatch::new(vec![packet]),
                    bundle_id: String::default(),
                },
                Some(5),
                &Ok
            ),
            Err(DeserializedBundleError::MarkedDiscard)
        );
    }

    #[test]
    fn test_bad_signature_fails_to_init() {
        let kp0 = Keypair::new();
        let kp1 = Keypair::new();

        let mut tx0 = transfer(&kp0, &kp0.pubkey(), 100, Hash::default());
        let tx1 = transfer(&kp1, &kp0.pubkey(), 100, Hash::default());
        tx0.signatures = tx1.signatures;

        assert_matches!(
            ImmutableDeserializedBundle::new(
                &mut PacketBundle {
                    batch: PacketBatch::new(vec![Packet::from_data(None, tx0).unwrap()]),
                    bundle_id: String::default(),
                },
                None,
                &Ok
            ),
            Err(DeserializedBundleError::SignatureVerificationFailure)
        );
    }

    #[test]
    fn test_vote_only_bank_fails_to_build() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (parent, _) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
        let vote_only_bank = Arc::new(Bank::new_from_parent_with_options(
            parent,
            &Pubkey::new_unique(),
            1,
            NewBankOptions {
                vote_only_bank: true,
            },
        ));

        let kp = Keypair::new();

        let tx0 = transfer(&mint_keypair, &kp.pubkey(), 500, genesis_config.hash());

        let bundle = ImmutableDeserializedBundle::new(
            &mut PacketBundle {
                batch: PacketBatch::new(vec![Packet::from_data(None, tx0).unwrap()]),
                bundle_id: String::default(),
            },
            None,
            &Ok,
        )
        .unwrap();

        let mut transaction_errors = TransactionErrorMetrics::default();
        assert_matches!(
            bundle.build_sanitized_bundle(
                &vote_only_bank,
                &HashSet::default(),
                &mut transaction_errors,
                false
            ),
            Err(DeserializedBundleError::VoteOnlyMode)
        );
    }

    #[test]
    fn test_duplicate_signature_fails_to_build() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (bank, _) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let kp = Keypair::new();

        let tx0 = transfer(&mint_keypair, &kp.pubkey(), 500, genesis_config.hash());

        let bundle = ImmutableDeserializedBundle::new(
            &mut PacketBundle {
                batch: PacketBatch::new(vec![
                    Packet::from_data(None, &tx0).unwrap(),
                    Packet::from_data(None, &tx0).unwrap(),
                ]),
                bundle_id: String::default(),
            },
            None,
            &Ok,
        )
        .unwrap();

        let mut transaction_errors = TransactionErrorMetrics::default();
        assert_matches!(
            bundle.build_sanitized_bundle(
                &bank,
                &HashSet::default(),
                &mut transaction_errors,
                false
            ),
            Err(DeserializedBundleError::DuplicateTransaction)
        );
    }

    #[test]
    fn test_blacklisted_account_fails_to_build() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (bank, _) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let kp = Keypair::new();

        let tx0 = transfer(&mint_keypair, &kp.pubkey(), 500, genesis_config.hash());

        let bundle = ImmutableDeserializedBundle::new(
            &mut PacketBundle {
                batch: PacketBatch::new(vec![Packet::from_data(None, tx0).unwrap()]),
                bundle_id: String::default(),
            },
            None,
            &Ok,
        )
        .unwrap();

        let mut transaction_errors = TransactionErrorMetrics::default();
        assert_matches!(
            bundle.build_sanitized_bundle(
                &bank,
                &HashSet::from([kp.pubkey()]),
                &mut transaction_errors,
                false
            ),
            Err(DeserializedBundleError::BlacklistedAccount)
        );
    }

    #[test]
    fn test_already_processed_tx_fails_to_build() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (bank, _bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let kp = Keypair::new();

        let tx0 = transfer(&mint_keypair, &kp.pubkey(), 500, genesis_config.hash());

        bank.process_transaction(&tx0).unwrap();

        let bundle = ImmutableDeserializedBundle::new(
            &mut PacketBundle {
                batch: PacketBatch::new(vec![Packet::from_data(None, tx0).unwrap()]),
                bundle_id: String::default(),
            },
            None,
            &Ok,
        )
        .unwrap();

        let mut transaction_errors = TransactionErrorMetrics::default();
        assert_matches!(
            bundle.build_sanitized_bundle(
                &bank,
                &HashSet::default(),
                &mut transaction_errors,
                false
            ),
            Err(DeserializedBundleError::FailedCheckTransactions)
        );
    }

    #[test]
    fn test_bad_blockhash_fails_to_build() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (bank, _) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let kp = Keypair::new();

        let tx0 = transfer(&mint_keypair, &kp.pubkey(), 500, Hash::default());

        let bundle = ImmutableDeserializedBundle::new(
            &mut PacketBundle {
                batch: PacketBatch::new(vec![Packet::from_data(None, tx0).unwrap()]),
                bundle_id: String::default(),
            },
            None,
            &Ok,
        )
        .unwrap();

        let mut transaction_errors = TransactionErrorMetrics::default();
        assert_matches!(
            bundle.build_sanitized_bundle(
                &bank,
                &HashSet::default(),
                &mut transaction_errors,
                false
            ),
            Err(DeserializedBundleError::FailedCheckTransactions)
        );
    }

    #[test]
    fn test_bad_ed25519_program_precompile() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (bank, _) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let tx = Transaction::new_signed_with_payer(
            &[Instruction {
                program_id: solana_sdk::ed25519_program::id(),
                accounts: vec![],
                data: vec![], // empty data is invalid
            }],
            Some(&mint_keypair.pubkey()),
            &[&mint_keypair],
            genesis_config.hash(),
        );

        let bundle = ImmutableDeserializedBundle::new(
            &mut PacketBundle {
                batch: PacketBatch::new(vec![Packet::from_data(None, tx).unwrap()]),
                bundle_id: String::default(),
            },
            None,
            &Ok,
        )
        .unwrap();

        let mut transaction_errors = TransactionErrorMetrics::default();
        assert_matches!(
            bundle.build_sanitized_bundle(
                &bank,
                &HashSet::default(),
                &mut transaction_errors,
                false
            ),
            Err(DeserializedBundleError::FailedVerifyPrecompiles)
        );

        // move precompile to SVM ignores the check
        assert!(bundle
            .build_sanitized_bundle(&bank, &HashSet::default(), &mut transaction_errors, true)
            .is_ok());
    }

    #[test]
    fn test_bad_secp256k1_program_precompile() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (bank, _) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let tx = Transaction::new_signed_with_payer(
            &[Instruction {
                program_id: solana_sdk::secp256k1_program::id(),
                accounts: vec![],
                data: vec![], // empty data is invalid
            }],
            Some(&mint_keypair.pubkey()),
            &[&mint_keypair],
            genesis_config.hash(),
        );

        let bundle = ImmutableDeserializedBundle::new(
            &mut PacketBundle {
                batch: PacketBatch::new(vec![Packet::from_data(None, tx).unwrap()]),
                bundle_id: String::default(),
            },
            None,
            &Ok,
        )
        .unwrap();

        let mut transaction_errors = TransactionErrorMetrics::default();
        assert_matches!(
            bundle.build_sanitized_bundle(
                &bank,
                &HashSet::default(),
                &mut transaction_errors,
                false
            ),
            Err(DeserializedBundleError::FailedVerifyPrecompiles)
        );

        // move precompile to SVM ignores the check
        assert!(bundle
            .build_sanitized_bundle(&bank, &HashSet::default(), &mut transaction_errors, true)
            .is_ok());
    }
}
