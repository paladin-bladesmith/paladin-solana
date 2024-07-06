//! These tests compliment the implementation in the bundle crate.

#[cfg(test)]
mod tests {
    use {
        crate::{
            immutable_deserialized_bundle::ImmutableDeserializedBundle, packet_bundle::PacketBundle,
        },
        solana_bundle::bundle_account_locker::BundleAccountLocker,
        solana_ledger::genesis_utils::create_genesis_config,
        solana_perf::packet::PacketBatch,
        solana_runtime::{bank::Bank, genesis_utils::GenesisConfigInfo},
        solana_sdk::{
            packet::Packet, signature::Signer, signer::keypair::Keypair, system_program,
            system_transaction::transfer, transaction::VersionedTransaction,
        },
        solana_svm::transaction_error_metrics::TransactionErrorMetrics,
        std::collections::HashSet,
    };

    #[test]
    fn test_simple_lock_bundles() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2);
        let (bank, _) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let bundle_account_locker = BundleAccountLocker::default();

        let kp0 = Keypair::new();
        let kp1 = Keypair::new();

        let tx0 = VersionedTransaction::from(transfer(
            &mint_keypair,
            &kp0.pubkey(),
            1,
            genesis_config.hash(),
        ));
        let tx1 = VersionedTransaction::from(transfer(
            &mint_keypair,
            &kp1.pubkey(),
            1,
            genesis_config.hash(),
        ));

        let mut packet_bundle0 = PacketBundle {
            batch: PacketBatch::new(vec![Packet::from_data(None, &tx0).unwrap()]),
            bundle_id: tx0.signatures[0].to_string(),
        };
        let mut packet_bundle1 = PacketBundle {
            batch: PacketBatch::new(vec![Packet::from_data(None, &tx1).unwrap()]),
            bundle_id: tx1.signatures[0].to_string(),
        };

        let mut transaction_errors = TransactionErrorMetrics::default();

        let sanitized_bundle0 = ImmutableDeserializedBundle::new(&mut packet_bundle0, None)
            .unwrap()
            .build_sanitized_bundle(&bank, &HashSet::default(), &mut transaction_errors)
            .expect("sanitize bundle 0");
        let sanitized_bundle1 = ImmutableDeserializedBundle::new(&mut packet_bundle1, None)
            .unwrap()
            .build_sanitized_bundle(&bank, &HashSet::default(), &mut transaction_errors)
            .expect("sanitize bundle 1");

        let locked_bundle0 = bundle_account_locker
            .prepare_locked_bundle(&sanitized_bundle0, &bank)
            .unwrap();

        assert_eq!(
            bundle_account_locker.write_locks(),
            HashSet::from_iter([mint_keypair.pubkey(), kp0.pubkey()])
        );
        assert_eq!(
            bundle_account_locker.read_locks(),
            HashSet::from_iter([system_program::id()])
        );

        let locked_bundle1 = bundle_account_locker
            .prepare_locked_bundle(&sanitized_bundle1, &bank)
            .unwrap();
        assert_eq!(
            bundle_account_locker.write_locks(),
            HashSet::from_iter([mint_keypair.pubkey(), kp0.pubkey(), kp1.pubkey()])
        );
        assert_eq!(
            bundle_account_locker.read_locks(),
            HashSet::from_iter([system_program::id()])
        );

        drop(locked_bundle0);
        assert_eq!(
            bundle_account_locker.write_locks(),
            HashSet::from_iter([mint_keypair.pubkey(), kp1.pubkey()])
        );
        assert_eq!(
            bundle_account_locker.read_locks(),
            HashSet::from_iter([system_program::id()])
        );

        drop(locked_bundle1);
        assert!(bundle_account_locker.write_locks().is_empty());
        assert!(bundle_account_locker.read_locks().is_empty());
    }
}
